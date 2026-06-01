# Proposal: Node Inspect

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-inspect-05312026-004.md     -->

**Author**: Tourillon Contributors <tourillon@example.com>
**Status:** Draft
**Date:** 2026-05-31
**Sequence:** 004

---

## Summary

This proposal specifies `tourctl node inspect <address>` — a diagnostic command that
opens a direct mTLS connection to any running node's peer address and retrieves a live
snapshot of its internal state. The response carries partition ownership (computed via
`Partitioner.ranges_for()`), the full membership registry view held by the target, and
local probe state for every tracked peer (including both the gossip phi value and the
data-circuit-breaker suspect flag). The proposal also updates
`core/structure/inspect.py` to add the `data_is_suspect` field to `ProbeSummary` and
a `probe_states_total` field to `NodeInspectResponse`, and it assumes that the
`Bootstraper` introduced in proposal 001 wires the `node.inspect` handler into the peer
dispatcher from a single `TourillonCore` instance rather than scattering registration
across startup helpers. The full `token_hex` value is always present in the wire
payload; the CLI truncates it to the first 8 hex digits followed by `…` for the
human-readable table view. No amendments to this proposal are permitted; later proposals
extend it only through new `Dispatcher` registrations.

---

## Motivation

After a node joins the cluster, operators need a single command that
answers the following questions without modifying any state:

1. **Is this node healthy?** What is its current phase, generation, and epoch?
2. **Which partitions does it own?** Which vnodes does it hold, and how many partitions
   do they cover?
3. **What is its view of the cluster?** Which members does it know about, and in what
   phase are they?
4. **Are any peers suspected?** Is the suspicion driven by gossip latency (phi-accrual)
   or by data-plane failures (circuit-breaker)? The dual-detector model makes this
   distinction visible; `tourctl node inspect` is the first operator surface that exposes it.

Without this proposal, the only way to diagnose a running node is to grep its logs —
a technique that scales poorly and provides no structured output for automation.

---

## CLI contract

All commands print to stdout on success and to stderr on failure.
Exit code `0` = success; `1` = user or config error; `2` = transport / internal error.

### `tourctl node inspect <address>` — retrieve a live snapshot from a running node

```
$ tourctl node inspect ADDRESS [OPTIONS]

  Connect directly to ADDRESS (the node's peer address, host:port) via mTLS and
  retrieve a full live snapshot of the node's in-memory state. No state is
  modified. The node can be in any phase; the command always succeeds as long as
  the peer socket is reachable.

Arguments:
  ADDRESS               Peer address of the target node (host:port)  [required]

Options:
  --context TEXT        Context name from contexts.toml
  --contexts-file PATH  Path to contexts.toml  [default: ~/.tourillon/contexts.toml]
  --timeout TEXT        Request timeout        [default: 30s]
  --partitions          Force display of every partition range individually, even
                        when the list exceeds PARTITION_DISPLAY_THRESHOLD (64).
  --json                Emit machine-readable JSON to stdout instead of the
                        human-readable table.
  --help                Show this message and exit.
```

Either `ADDRESS` (positional) or a context with a `peer` endpoint must be supplied.
If both are provided, `ADDRESS` takes precedence.

---

#### Human-readable output (default, `--json` absent)

The output is split into four labelled sections separated by blank lines.

**Happy path — READY node (stdout):**

```
Node: node-1
Phase: ready  |  Generation: 1  |  Seq: 7  |  Epoch: 2
Peer: 192.168.1.1:7001  |  KV: 192.168.1.1:7000
Size: M

Partitions (1024 total, shift=10):
  Owned: 512 partitions across 2 range(s)
  token 0xaf3c12b8…  →  pids [   0– 511]  (512 partitions)
  token 0xd1047e9c…  →  pids [ 512–1023]  (512 partitions)

Members (3 total):
  node_id    phase      gen  seq  peer_address
  node-1     ready        1    7  192.168.1.1:7001
  node-2     ready        1    3  192.168.1.2:7001
  node-3     joining      1    0  192.168.1.3:7001

Probe states (2 tracked):
  node_id    state    phi    data_suspect
  node-2     live     0.12   false
  node-3     live     0.05   false
```

**Happy path — JOINING node (stdout):**

```
Node: node-3
Phase: joining  |  Generation: 1  |  Seq: 0  |  Epoch: 2
Peer: 192.168.1.3:7001  |  KV: (none — not yet READY)
Size: S

Partitions (1024 total, shift=10):
  Owned: 256 partitions across 1 range(s)
  token 0x5e872af0…  →  pids [ 256– 511]  (256 partitions)

Members (3 total):
  node_id    phase      gen  seq  peer_address
  node-1     ready        1    7  192.168.1.1:7001
  node-2     ready        1    3  192.168.1.2:7001
  node-3     joining      1    0  192.168.1.3:7001

Probe states (2 tracked):
  node_id    state      phi    data_suspect
  node-1     live       0.09   false
  node-2     suspect    9.81   false
```

When `kv_address` is the empty string (the node is not yet serving KV traffic),
the CLI renders `(none — not yet READY)` in the KV line.

When `members_truncated` is `True` the Members section footer reads:

```
  [truncated — showing 256 of 300 members]
```

When `probe_states_truncated` is `True` the Probe states section footer reads:

```
  [truncated — showing 256 of 300 probe states]
```

**Node with a data-suspect peer (stdout — probe states section):**

```
Probe states (2 tracked):
  node_id    state    phi    data_suspect
  node-2     live     0.12   false
  node-3     suspect  0.00   true
```

`phi` is rendered to 2 decimal places; `0.00` when no gossip observations exist yet.
`data_suspect` is `true` when `ProbeSummary.data_is_suspect` is `True`, `false` otherwise.
`state` is the combined `MemberState` value (`live`, `suspect`, or `unknown`).

#### Partition range display — `PARTITION_DISPLAY_THRESHOLD`

`PARTITION_DISPLAY_THRESHOLD = 64` (constant in `tourctl/infra/cli/node.py`).

- When `len(partition_ranges) ≤ PARTITION_DISPLAY_THRESHOLD`, every range is always
  rendered individually using the `token 0x…  →  pids  N–M  (K partitions)` line.
- When `len(partition_ranges) > PARTITION_DISPLAY_THRESHOLD` **and** `--partitions`
  is absent, the range list is collapsed to a single hint line:
  ```
  Partitions (1024 total, shift=10):
    Owned: 16384 partitions across 32 range(s)  (use --partitions to list all)
  ```
- When `--partitions` is present, every range is listed regardless of count:
  ```
  $ tourctl node inspect 192.168.1.1:7001 --partitions
  ...
  Partitions (65536 total, shift=16):
    Owned: 16384 partitions across 32 range(s)
    token 0x03f2a1b4…  →  pids [    0– 2047]  (2048 partitions)
    token 0x11cc8e72…  →  pids [ 2048– 4095]  (2048 partitions)
    ...
    token 0xfbd14a09…  →  pids [63488–65535]  (2048 partitions)
  ```

---

#### JSON output (`--json`)

The `--json` flag causes the full `NodeInspectResponse` to be serialised as JSON to
stdout. Every field in the dataclass is present. `tokens` and partition integer fields
are rendered as JSON numbers. `token_hex` fields carry the full untruncated value
(`"0x"` + 32 hex digits for a 128-bit hash space). Booleans are JSON `true`/`false`.

**Example (stdout, partial):**

```json
{
  "node_id": "node-1",
  "phase": "ready",
  "peer_address": "192.168.1.1:7001",
  "kv_address": "192.168.1.1:7000",
  "size": "M",
  "generation": 1,
  "seq": 7,
  "epoch": 2,
  "tokens": [2938471029384710293, 9384710293847102938],
  "total_partitions": 1024,
  "partition_shift": 10,
  "owned_partitions": 512,
  "partition_ranges": [
    {
      "start_pid": 0,
      "end_pid": 511,
      "count": 512,
      "token_hex": "0xaf3c12b8deadbeef0011223344556677"
    }
  ],
  "members": [
    {
      "node_id": "node-1",
      "phase": "ready",
      "generation": 1,
      "seq": 7,
      "peer_address": "192.168.1.1:7001"
    }
  ],
  "members_truncated": false,
  "members_total": 3,
  "probe_states": [
    {
      "node_id": "node-2",
      "state": "live",
      "phi": 0.12,
      "data_is_suspect": false
    }
  ],
  "probe_states_truncated": false,
  "probe_states_total": 2
}
```

---

#### Error cases

**Error — connection refused (stderr, exit 2):**

```
Error: connection refused: 192.168.1.1:7001
The node may not be running. Start it with 'tourillon node start'.
```

**Error — request timeout (stderr, exit 2):**

```
Error: no response from 192.168.1.1:7001 within 30s.
The node may be unreachable or not running. Check peer_server.bind in config.toml.
```

**Error — no address and no context peer endpoint (stderr, exit 1):**

```
Error: peer address required: supply ADDRESS or a context with a peer endpoint.
```

**Error — TLS handshake failure (stderr, exit 2):**

```
Error: TLS handshake failed: [SSL] certificate verify failed (_ssl.c:1007)
Ensure the client certificate was issued by the cluster CA and has not expired.
```

---

## Design

### Data model

#### `INSPECT_MEMBER_LIMIT` (constant, `core/structure/inspect.py`)

```python
INSPECT_MEMBER_LIMIT: int = 256
```

Both the `members` and `probe_states` tuples in `NodeInspectResponse` are truncated to
at most `INSPECT_MEMBER_LIMIT` entries. When the registry or probe manager contains more
entries than this limit, the corresponding `*_truncated` flag is set to `True` and the
matching `*_total` field records the true count before truncation. Entries are sorted by
`node_id` before truncation so the result is deterministic.

#### `PartitionRange` modification (`core/structure/inspect.py`)

`PartitionRange` in `inspect.py` is the **wire-level** representation of a partition
range; it is distinct from `PartitionRange` in `core/ring/partitioner.py`, which is the
**ring-level** representation (it carries an `owner: VNode` reference and is never
serialised). The inspect-level `PartitionRange` is derived from the ring-level one:

```
ring_range.start_pid  →  inspect_range.start_pid
ring_range.end_pid    →  inspect_range.end_pid
ring_range.count      →  inspect_range.count
"0x" + format(ring_range.owner.token, "032x")  →  inspect_range.token_hex
```

The `token_hex` field carries the full 32-hex-digit representation (for a 128-bit hash
space) prefixed with `"0x"`. The CLI renders only the first 8 hex digits after the prefix,
followed by `…`:

```
"0xaf3c12b8deadbeef0011223344556677"  →  CLI displays  "0xaf3c12b8…"
```

Full value is always present in the wire payload and in `--json` output.

#### `ProbeSummary` modification (`core/structure/inspect.py`)

`ProbeManager.all_states_with_phi()` returns 4-tuples
`(node_id, MemberState, gossip_phi, data_is_suspect)`. `ProbeSummary` is updated to
expose both detector outputs explicitly:

```python
@dataclass(frozen=True)
class ProbeSummary:
    node_id: str
    state: str        # combined MemberState: "live" | "suspect" | "unknown"
    phi: float        # gossip_phi from the phi-accrual FailureDetector; 0.0 when no observations
    data_is_suspect: bool  # DataCircuitBreaker.is_suspect for this peer
```

`state` is the combined state (gossip OR data). `phi` is the raw gossip phi value so
operators can distinguish "gossip-live but data-dead" (`state="suspect"`, `phi=0.2`,
`data_is_suspect=True`) from "gossip-dead but data-live" (`state="suspect"`, `phi=12.4`,
`data_is_suspect=False`).

#### `NodeInspectResponse` modification (`core/structure/inspect.py`)

`probe_states_total: int` is added as a new field (mirroring `members_total`) so that
callers can determine the true probe-manager size even when the payload is truncated:

```python
@dataclass(frozen=True)
class NodeInspectResponse:
    node_id: str
    phase: str              # MemberPhase value
    peer_address: str
    kv_address: str         # empty string when node is not yet serving KV traffic
    size: str               # NodeSize value (e.g. "M")
    generation: int
    seq: int
    epoch: int
    tokens: tuple[int, ...]
    total_partitions: int
    partition_shift: int
    owned_partitions: int
    partition_ranges: tuple[PartitionRange, ...]
    members: tuple[MemberSummary, ...]
    members_truncated: bool
    members_total: int
    probe_states: tuple[ProbeSummary, ...]
    probe_states_truncated: bool
    probe_states_total: int  # NEW — true probe manager size before truncation
```

#### Handler construction (`core/lifecycle/handlers.py`)

The `node.inspect` handler is a plain `async def` decorated with
`@dispatcher.on("node.inspect")` inside a `register()` function that captures all
dependencies via closure:

```python
def register(
    dispatcher: Dispatcher,
    node_id: str,
    cfg: TourillonConfig,
    topology_mgr: TopologyManager,
    probe_mgr: ProbeManager,
    partitioner: Partitioner,
    state_port: StatePort,
    serializer: SerializerPort,
) -> None:
    @dispatcher.on("node.inspect")
    async def handle_node_inspect(
        receive: ReceiveEnvelope, send: SendEnvelope
    ) -> None:
        ...
```

The handler does not require the request envelope payload (it is ignored); it uses only
the `correlation_id` from the incoming envelope to tag the response.

#### Response construction algorithm

```
1.  _ = await receive()           # consume the request (payload ignored)
2.  node_state = await state_port.load()
    # node_state: NodeState — current phase, generation, seq, epoch, tokens
3.  topo = await topology_mgr.snapshot()
    # topo: Topology — ring, registry
4.  ring = topo.ring
5.  registry_members = sorted(topo.registry.all(), key=lambda m: m.node_id)

6.  # Build partition ranges for this node
    ring_ranges = partitioner.ranges_for(node_id, ring)
    # ring_ranges: list[ring.PartitionRange]  (sorted by start_pid)
    inspect_ranges = tuple(
        inspect.PartitionRange(
            start_pid=r.start_pid,
            end_pid=r.end_pid,
            count=r.count,
            token_hex="0x" + format(r.owner.token, "032x"),
        )
        for r in ring_ranges
    )
    owned_partitions = sum(r.count for r in ring_ranges)

7.  # Truncate members
    members_total = len(registry_members)
    members_truncated = members_total > INSPECT_MEMBER_LIMIT
    members_slice = registry_members[:INSPECT_MEMBER_LIMIT]
    member_summaries = tuple(
        MemberSummary(
            node_id=m.node_id,
            phase=str(m.phase),
            generation=m.generation,
            seq=m.seq,
            peer_address=m.peer_address,
        )
        for m in members_slice
    )

8.  # Build probe states
    raw_probes = await probe_mgr.all_states_with_phi()
    # raw_probes: list[tuple[str, MemberState, float, bool]]
    sorted_probes = sorted(raw_probes, key=lambda t: t[0])
    probe_states_total = len(sorted_probes)
    probe_states_truncated = probe_states_total > INSPECT_MEMBER_LIMIT
    probes_slice = sorted_probes[:INSPECT_MEMBER_LIMIT]
    probe_summaries = tuple(
        ProbeSummary(
            node_id=nid,
            state=str(state),
            phi=phi,
            data_is_suspect=data_is_suspect,
        )
        for nid, state, phi, data_is_suspect in probes_slice
    )

9.  response = NodeInspectResponse(
        node_id=node_id,
        phase=str(node_state.phase),
        peer_address=cfg.peer_server.advertise,
        kv_address=(
            cfg.kv_server.advertise
            if node_state.phase in {MemberPhase.READY, MemberPhase.DRAINING}
            else ""
        ),
        size=str(cfg.node.size),
        generation=node_state.generation,
        seq=node_state.seq,
        epoch=topo.epoch,
        tokens=node_state.tokens,
        total_partitions=partitioner.total_partitions,
        partition_shift=partitioner.partition_shift,
        owned_partitions=owned_partitions,
        partition_ranges=inspect_ranges,
        members=member_summaries,
        members_truncated=members_truncated,
        members_total=members_total,
        probe_states=probe_summaries,
        probe_states_truncated=probe_states_truncated,
        probe_states_total=probe_states_total,
    )

10. payload = serializer.encode(_response_to_dict(response))
11. await send(
        Envelope.create(
            payload,
            kind="node.inspect.response",
            schema_id=serializer.schema_id,
            correlation_id=request_envelope.correlation_id,
        )
    )
```

The helper `_response_to_dict(r: NodeInspectResponse) -> dict` performs a recursive
conversion of the frozen dataclass tree to a plain `dict` / `list` / primitive tree
that `SerializerPort.encode` can serialise. It lives in `core/lifecycle/handlers.py`
and never imports `msgpack`.

#### `tourctl node inspect` CLI flow

```
1.  Parse CLI arguments (address, --context, --contexts-file, --timeout, --json).
2.  Resolve peer_address:
      a. If ADDRESS positional argument supplied → use it.
      b. Else resolve from context's peer endpoint.
      c. If neither → stderr error; exit 1.
3.  load_contexts(contexts_file) → ContextsFile; select context if --context given.
4.  Build ssl_ctx = build_client_ssl_context(
            cert_data, key_data, ca_data
        ) from selected context credentials.
5.  TcpClient.connect(peer_address, ssl_ctx).
6.  request_env = Envelope.create(
            payload=serializer.encode({}),
            kind="node.inspect",
            schema_id=serializer.schema_id,
        )
7.  response_env = await client.request(
            request_env, timeout=parse_duration(options.timeout)
        )
8.  If response_env.kind != "node.inspect.response":
        stderr "Error: unexpected response kind: {response_env.kind}"; exit 2.
9.  raw = serializer.decode(response_env.payload)
    response = _dict_to_response(raw)   # inverse of _response_to_dict
10. If --json:
        print(json.dumps(_response_to_json_dict(response), indent=2))
    Else:
        print(_render_human(response))
11. exit 0.
```

Errors from steps 5–7 are mapped to exit code 2 with descriptive stderr messages.

#### Token truncation helper

```python
def _truncate_token(token_hex: str) -> str:
    """Return token_hex with only the first 8 hex digits after the 0x prefix.

    Input:  "0xaf3c12b8deadbeef0011223344556677"
    Output: "0xaf3c12b8…"
    """
    return token_hex[:10] + "…"
```

`token_hex[:10]` slices `"0x"` (2 chars) + the first 8 hex digits.

#### Partition range display format

Each `PartitionRange` in the human-readable output is rendered as:

```
  token <truncated_token>  →  pids [<start_pid>–<end_pid>]  (<count> partitions)
```

Left-pad `start_pid` and `end_pid` to the width of `total_partitions - 1` in decimal so
columns align. Example for `total_partitions=1024` (max pid = 1023, width 4):

```
  token 0xaf3c12b8…  →  pids [   0– 511]  (512 partitions)
  token 0xd1047e9c…  →  pids [ 512–1023]  (512 partitions)
```

#### Wrapping ranges

When `start_pid > end_pid` (the vnode with the minimum token; its arc crosses the zero
boundary), the CLI renders both bounds literally as they appear in `PartitionRange`:

```
  token 0x001a3cb4…  →  pids [ 900–  63]  (188 partitions)  [w]
```

The `[w]` tag makes it clear that the range is `[900, 1023] ∪ [0, 63]`.

### Core invariants

1. **No state mutation.** The `node.inspect` handler is strictly read-only. It never
   calls `state_port.save()`, `topology_mgr.apply_member()`, or any write method on
   `ProbeManager`. A bug in the handler must not be able to corrupt live state.

2. **Handler always responds.** Even if an error occurs during response construction,
   the handler must catch the exception, log it at ERROR level, and close the
   connection rather than leaving the client waiting indefinitely.

3. **Truncation is deterministic.** Both the `members` and `probe_states` slices are
   sorted by `node_id` before truncation. Given the same registry state, two calls to
   the handler always produce identical lists.

4. **`token_hex` truncation is CLI-only.** The wire payload and `--json` output always
   carry the full `token_hex`. Truncation is applied only in the human-readable render
   path. No handler code ever truncates `token_hex`.

5. **`kv_address` is empty for non-serving phases.** The handler sets `kv_address = ""`
   when `node_state.phase not in {READY, DRAINING}`. The CLI renders `(none — not yet
   READY)` in place of the empty string.

6. **`node.inspect` does not feed `data_fd`.** This is a control-plane operation.
   No call to `probe_mgr.record_data_failure()` or `probe_mgr.record_data_success()`
   is made as a result of handling `node.inspect`.

7. **`probe_states_total` always reflects pre-truncation count.** Even when
   `probe_states_truncated == False` (no truncation occurred), `probe_states_total`
   is set to the actual count of tracked peers, never to `INSPECT_MEMBER_LIMIT`.

### Sequence / flow — `tourctl node inspect`

```
tourctl side:
  1. Parse CLI options; resolve peer address.
  2. Build mTLS client ssl_ctx from contexts.toml credentials.
  3. Connect TcpClient to peer address.
  4. Send "node.inspect" envelope with empty payload.
  5. Await "node.inspect.response" with configured timeout.
  6. Decode payload → NodeInspectResponse.
  7. Render and print.

Daemon side (`Bootstraper`-wired `handle_node_inspect`):
  1. Receive "node.inspect" envelope; note correlation_id.
  2. Load NodeState from state_port (in-memory; no disk I/O on hot path).
  3. Snapshot topology (ring + registry) from TopologyManager.
  4. Compute partition ranges via Partitioner.ranges_for(node_id, ring).
  5. Build MemberSummary list (sorted, truncated).
  6. Fetch all_states_with_phi() from ProbeManager (under lock).
  7. Build ProbeSummary list (sorted, truncated).
  8. Construct NodeInspectResponse.
  9. Encode with SerializerPort.
 10. Send "node.inspect.response" envelope.
```

### Error paths

| Scenario | Behaviour |
|---|---|
| `tourctl node inspect` — no address, no context peer | stderr "Error: peer address required …"; exit 1 |
| `tourctl node inspect` — connection refused | stderr "Error: connection refused: …"; exit 2 |
| `tourctl node inspect` — request timeout | stderr "Error: no response from … within …"; exit 2 |
| `tourctl node inspect` — TLS handshake failure | stderr "Error: TLS handshake failed: …"; exit 2 |
| `tourctl node inspect` — unexpected response kind | stderr "Error: unexpected response kind: …"; exit 2 |
| Handler — `state_port.load()` returns `None` | Use zero-value defaults: `generation=0`, `seq=0`, `epoch=0`, `tokens=()`; phase from config default |
| Handler — `partitioner.ranges_for()` returns empty list (empty ring) | `owned_partitions=0`; `partition_ranges=()`; response still sent normally |
| Handler — `topology_mgr.snapshot()` raises unexpectedly | Log at ERROR; close connection without sending a response |
| Response — `members_total > INSPECT_MEMBER_LIMIT` | `members_truncated=True`; list capped; `members_total` reflects true count |
| Response — `probe_states_total > INSPECT_MEMBER_LIMIT` | `probe_states_truncated=True`; list capped; `probe_states_total` reflects true count |

---

## Design decisions

### Decision: direct mTLS connection to target's peer address (no forwarding)

**Alternatives considered:**
- (A) Route inspect requests through a well-known "leader" or seed node that forwards
  to the target.
- (B) Connect directly to the target's peer address from `tourctl`.

**Chosen because:** (B) is simpler, has lower latency, and is more useful for diagnosis
— the most common reason to inspect a node is to check whether it is reachable at all.
Forwarding (A) would obscure network partitions between the CLI client and the target
node. The `contexts.toml` already carries the peer address for every cluster endpoint,
and proposal 001 established that `tourctl` constructs its own mTLS client context from
`CredentialsConfig`. No additional routing infrastructure is needed.

### Decision: `data_is_suspect` is a separate field in `ProbeSummary`, not folded into `state`

**Alternatives considered:**
- (A) Add a new `MemberState` value such as `data_suspect` to represent the
  data-plane failure mode.
- (B) Expose only the combined `state`; omit per-detector detail.
- (C) Keep `state` as the combined value; add `data_is_suspect: bool` as a separate
  field.

**Chosen because:** (C) preserves backward compatibility with code that pattern-matches
on `state` while giving operators the additional diagnostic resolution they need.
(A) expands `MemberState` to a 4-value enum whose combined semantics are harder to
reason about (gossip-live + data-dead is a valid state, not a new state name).
(B) makes the dual-detector model invisible at the operator surface, defeating the purpose
of proposal 003's `all_states_with_phi()` extension.

### Decision: `INSPECT_MEMBER_LIMIT = 256` for both members and probe states

**Alternatives considered:**
- Separate limits for members and probe states.
- A configurable `[inspect]` section in `config.toml`.
- No limit (return all entries unconditionally).

**Chosen because:** A single constant avoids configuration sprawl. 256 is large enough
to cover production clusters of meaningful size (typical clusters are 5–50 nodes). The
`members_total` and `probe_states_total` fields allow operators to detect truncation and
use other tooling for full registry inspection when needed. A configurable limit is
deferred until a clear operator need exists.

### Decision: handler lives in `core/lifecycle/handlers.py`, not `core/gossip/handlers.py`

**Alternatives considered:**
- Register the `node.inspect` handler inside `core/gossip/handlers.py` alongside
  `gossip.push`, `gossip.ping`, etc.

**Chosen because:** `node.inspect` is a lifecycle diagnostic operation that reads
`NodeState`, the `TopologyManager`, and the `ProbeManager`. Gossip handlers deal
exclusively with membership propagation. Placing the inspect handler in
`core/lifecycle/handlers.py` keeps a clean separation of concerns and avoids adding
a `StatePort` dependency to `core/gossip/handlers.py`. Both files share the same
`register(dispatcher, ...)` convention; there is no functional difference.

### Decision: `token_hex` is a `str` in the wire payload, not the raw `int` token

**Alternatives considered:**
- Send `token: int` (128-bit) in each `PartitionRange`; let the CLI convert to hex.

**Chosen because:** `NodeInspectResponse` is a pure display/diagnostics structure.
Carrying the token as a pre-formatted hex string avoids duplicating the
`"0x" + format(token, "032x")` logic in every consumer (CLI, JSON renderer, tests).
The `MsgpackSerializerAdapter` would encode a 128-bit integer as `ExtType(1)`; a hex
string is more portable in JSON output and avoids integer overflow in languages that do
not natively support 128-bit integers.

### Decision: empty string for `kv_address` when node is not serving KV traffic

**Alternatives considered:**
- Use `None` / `null`.
- Omit the field entirely.

**Chosen because:** Frozen dataclasses with `None` fields complicate type annotations and
require `Optional[str]` throughout the stack. An empty string is a valid sentinel that
every serialiser handles identically. The CLI checks `if not response.kv_address` to
decide whether to render `(none — not yet READY)`, which is a simple, idiomatic guard.

---

## Interfaces (informative)

### `tourillon/core/structure/inspect.py` — full updated file

```python
INSPECT_MEMBER_LIMIT: int = 256

@dataclass(frozen=True)
class PartitionRange:
    """Wire-level partition range for node inspect payloads.

    Distinct from core/ring/partitioner.py PartitionRange (which carries owner VNode).
    token_hex is the full "0x<32 hex digits>" string; CLI truncates to first 8 digits.
    """
    start_pid: int
    end_pid: int
    count: int
    token_hex: str  # "0x" + 32 hex digits (128-bit space); full, never truncated here

@dataclass(frozen=True)
class MemberSummary:
    """Condensed Member record for NodeInspectResponse.members."""
    node_id: str
    phase: str          # MemberPhase value
    generation: int
    seq: int
    peer_address: str

@dataclass(frozen=True)
class ProbeSummary:
    """Local probe state for one peer, as seen by the inspected node."""
    node_id: str
    state: str           # combined MemberState: "live" | "suspect" | "unknown"
    phi: float           # gossip_phi; 0.0 when no observations
    data_is_suspect: bool  # DataCircuitBreaker.is_suspect for this peer

@dataclass(frozen=True)
class NodeInspectResponse:
    """Full live snapshot returned by the target node in response to node.inspect."""
    node_id: str
    phase: str
    peer_address: str
    kv_address: str      # empty string when not serving KV traffic
    size: str
    generation: int
    seq: int
    epoch: int
    tokens: tuple[int, ...]
    total_partitions: int
    partition_shift: int
    owned_partitions: int
    partition_ranges: tuple[PartitionRange, ...]
    members: tuple[MemberSummary, ...]
    members_truncated: bool
    members_total: int
    probe_states: tuple[ProbeSummary, ...]
    probe_states_truncated: bool
    probe_states_total: int  # true count before truncation
```

### `tourillon/core/lifecycle/handlers.py`

```python
def register(
    dispatcher: Dispatcher,
    node_id: str,
    cfg: TourillonConfig,
    topology_mgr: TopologyManager,
    probe_mgr: ProbeManager,
    partitioner: Partitioner,
    state_port: StatePort,
    serializer: SerializerPort,
) -> None:
    """Register the node.inspect handler on dispatcher.

    All dependencies are captured via closure. Called once at daemon startup.
    """

    @dispatcher.on("node.inspect")
    async def handle_node_inspect(
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle a node.inspect request; respond with node.inspect.response."""
        ...


def _response_to_dict(response: NodeInspectResponse) -> dict[str, object]:
    """Convert NodeInspectResponse to a plain dict tree for SerializerPort.encode."""
    ...


def _dict_to_response(raw: dict[str, object]) -> NodeInspectResponse:
    """Reconstruct NodeInspectResponse from the decoded dict (CLI side)."""
    ...
```

### `tourctl/infra/cli/node.py` (additions)

```python
@node_app.command("inspect")
def inspect_node(
    address: str = typer.Argument(..., help="Peer address (host:port)"),
    context: str | None = typer.Option(None, "--context"),
    contexts_file: Path = typer.Option(
        Path.home() / ".tourillon" / "contexts.toml", "--contexts-file"
    ),
    timeout: str = typer.Option("30s", "--timeout"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Connect to ADDRESS and display a live snapshot of the node's state."""
    ...


def _truncate_token(token_hex: str) -> str:
    """Return "0x<8 hex digits>…" from the full token_hex string."""
    return token_hex[:10] + "…"


def _render_human(response: NodeInspectResponse) -> str:
    """Format NodeInspectResponse as a multi-section human-readable string."""
    ...


def _render_json(response: NodeInspectResponse) -> str:
    """Serialise NodeInspectResponse to a pretty-printed JSON string."""
    ...
```

---


## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.
E2e tests use `tmp_path` (pytest fixture) and a real running daemon process.

| # | Mark | Fixture | Action | Expected |
|---|------|---------|--------|----------|
| 1 | unit | `handle_node_inspect` handler wired with `InMemoryStateAdapter(NodeState(phase=READY, gen=1, seq=3, epoch=2, tokens=(100,200,300,400)))`, single-node ring (4 vnodes), `Partitioner(shift=10)`, empty `ProbeManager()`, 1-member `TopologyManager` | Deliver `node.inspect` envelope | Handler calls `send()` once with `kind="node.inspect.response"`; decoded `NodeInspectResponse.phase == "ready"` and `NodeInspectResponse.total_partitions == 1024` |
| 2 | unit | Same handler as scenario 1; single-node ring owns all partitions | Decode `partition_ranges` from the response | `len(partition_ranges) == 4`; `sum(r.count for r in partition_ranges) == 1024`; each `token_hex` starts with `"0x"` and is 34 chars long |
| 3 | unit | Handler wired with `TopologyManager` containing 3 members (`"n1"`, `"n2"`, `"n3"`) | Deliver `node.inspect` envelope; decode response | `len(members) == 3`; `[m.node_id for m in members] == ["n1", "n2", "n3"]` (sorted); `members_total == 3`; `members_truncated == False` |
| 4 | unit | `ProbeManager` (after proposal 003 rewrite) with `"n2"` tracked: 1 heartbeat + 1 `record_data_failure` call | Deliver `node.inspect` envelope; decode `probe_states` | `len(probe_states) == 1`; `probe_states[0].node_id == "n2"`; `probe_states[0].data_is_suspect == True`; `probe_states[0].state == "suspect"` |
| 5 | unit | `TopologyManager` containing `INSPECT_MEMBER_LIMIT + 1` members (257 total) | Deliver `node.inspect` envelope; decode response | `members_truncated == True`; `members_total == 257`; `len(members) == 256` |
| 6 | unit | `ProbeManager` with `INSPECT_MEMBER_LIMIT + 1` probe entries (257 total) | Deliver `node.inspect` envelope; decode response | `probe_states_truncated == True`; `probe_states_total == 257`; `len(probe_states) == 256` |
| 7 | unit | `ProbeSummary(node_id="n2", state="live", phi=0.12, data_is_suspect=False)` and `ProbeSummary(node_id="n3", state="suspect", phi=0.0, data_is_suspect=True)` both present in response | Decode `probe_states` | `"n3"` entry has `data_is_suspect=True` and `phi=0.0`; `"n2"` entry has `data_is_suspect=False`; demonstrates distinct "gossip-live data-dead" vs "gossip-dead data-live" cases |
| 8 | unit | `_truncate_token("0xaf3c12b8deadbeef0011223344556677")` | Call the function directly | Returns `"0xaf3c12b8…"` (exactly 10 chars + ellipsis = 11 chars total) |
| 9 | unit | `_truncate_token("0x001a3cb4f200000000000000000000001a")` (longer than standard) | Call the function directly | Returns `"0x001a3cb4…"` (always slices at position 10, regardless of full length) |
| 10 | unit | `NodeInspectResponse` built from in-memory state with `kv_address=""` (JOINING node) | Call `_render_human(response)` | Output contains `"(none — not yet READY)"` in the KV address line; `"joining"` in the Phase line |
| 11 | unit | `NodeInspectResponse` with 2 partition ranges (`token_hex` known) | Call `_render_human(response)` | Output contains `"0x"` + exactly 8 hex chars + `"…"` for each range; pids are padded to consistent width |
| 12 | unit | `NodeInspectResponse` with a wrapping range (`start_pid=900`, `end_pid=63`) | Call `_render_human(response)` | Output contains `[w]` tag for that range row |
| 13 | unit | `NodeInspectResponse` with `members_truncated=True`, `members_total=300` | Call `_render_human(response)` | Output contains `"[truncated — showing 256 of 300 members]"` |
| 14 | unit | `NodeInspectResponse` with `probe_states_truncated=True`, `probe_states_total=300` | Call `_render_human(response)` | Output contains `"[truncated — showing 256 of 300 probe states]"` |
| 15 | unit | `NodeInspectResponse` with known field values | Call `_render_json(response)` | Output is valid JSON; `json.loads(output)["node_id"]` matches `response.node_id`; all `token_hex` values are present untruncated |
| 16 | unit | `NodeInspectResponse` with `partition_ranges` containing `token_hex="0xaf3c12b8deadbeef0011223344556677"` | Call `_render_json(response)` | JSON output contains the full untruncated `token_hex` string; no `…` present in JSON output |
| 17 | e2e | Running READY first-node daemon (proposal 002 bootstrap); valid `contexts.toml` with peer endpoint | `tourctl node inspect <peer_addr> --contexts-file tmp/contexts.toml` | Exit code 0; stdout contains node_id; stdout contains `"Phase: ready"`; stdout contains `"Partitions"` section |
| 18 | e2e | Running READY first-node daemon | `tourctl node inspect <peer_addr> --contexts-file tmp/contexts.toml --json` | Exit code 0; stdout is valid JSON; `json.loads(stdout)["phase"] == "ready"`; all `token_hex` values start with `"0x"` and are untruncated |

---

## Exit criteria

- [ ] All 18 test scenarios pass (`uv run pytest -m "unit or e2e" -x`).
- [ ] `uv run pytest --cov=tourillon --cov=tourctl --cov-fail-under=90` passes.
- [ ] `uv run ruff check tourillon/ tourctl/ tests/` passes with zero violations.
- [ ] `uv run black --check tourillon/ tourctl/ tests/` passes.
- [ ] `core/structure/inspect.py` — `ProbeSummary` has a `data_is_suspect: bool` field;
  `NodeInspectResponse` has a `probe_states_total: int` field; `INSPECT_MEMBER_LIMIT = 256`
  is defined.
- [ ] `core/lifecycle/handlers.py` — `register(dispatcher, ...)` is present and
  registers exactly one handler for kind `"node.inspect"` using `@dispatcher.on(...)`.
- [ ] `handle_node_inspect` never calls `state_port.save()`, `topology_mgr.apply_member()`,
  `probe_mgr.record_data_failure()`, or `probe_mgr.record_data_success()` (read-only
  invariant enforced by code review).
- [ ] `NodeInspectResponse.kv_address` is the empty string when
  `node_state.phase not in {READY, DRAINING}`.
- [ ] CLI `_truncate_token` returns `token_hex[:10] + "…"` regardless of the total
  length of `token_hex`.
- [ ] `_render_human` renders wrapping ranges (where `start_pid > end_pid`) with a
  `[w]` annotation.
- [ ] `_render_json` emits the full untruncated `token_hex` value for every
  `PartitionRange` entry.
- [ ] `members` and `probe_states` are sorted by `node_id` before truncation, making
  the output deterministic.
- [ ] `tourctl node inspect` exits with code 1 when no peer address is resolvable.
- [ ] `tourctl node inspect` exits with code 2 on connection refused, TLS failure,
  or timeout.
- [ ] No module under `tourillon/core/` imports `infra/`, `msgpack`, `ssl`, or
  `tomllib`/`tomli_w` directly.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

- `tourctl node pause` / `tourctl node resume` commands and `node.pause` /
  `node.resume` envelope kinds — covered by a later proposal.
- `tourctl gc status` and `gc.status` / `gc.status.response` envelope kinds —
  covered by a later proposal.
- `tourctl rebalance status` and rebalance envelope kinds — covered by a later proposal.
- Per-partition inspect (drill-down to individual pid ownership and transfer state) —
  covered by a later proposal.
- KV data inspection (reading keys/values from a partition) — out of scope for operator
  tooling; the `kv.get` path is the correct channel for data access.
- Watch / streaming mode for inspect (continuously re-poll the node and diff output) —
  deferred to a future observability proposal.
- Cluster-wide sweep (`tourctl node inspect --all`) that contacts every member and
  aggregates results — deferred; the single-node form is the primitive from which
  a sweep can be scripted.
- Certificate expiry display in inspect output — TLS certificates are static for the
  node lifetime in this proposal; rotation is out of scope.
- `node.inspect` request payload fields (e.g. scope filters to request only members
  or only probe states) — the request payload is currently ignored; scoping is deferred
  until a demonstrated need exists.
