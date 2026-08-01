# Proposal: Node Inspect (`tourctl node inspect`)

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Accepted
**Date:** 2026-05-10
**Sequence:** 003
**Revision:** 2

---

## Summary

`tourctl node inspect <address>` retrieves a rich, live snapshot of a cluster
node's identity, ring position, partition ownership, cluster membership view,
and failure-detector state. The operator supplies the target node's **peer
address** (e.g. `peer-3.prod.example.com:7701`) directly. `tourctl` opens a
mTLS connection straight to that address and sends a self-inspect request. The
target node builds and returns the response entirely from its own state —
no intermediary, no forwarding.

---

## Motivation

Operators of a leaderless distributed system rely heavily on per-node
introspection to diagnose imbalances, verify ring topology convergence, detect
stale gossip, and understand partition ownership. Without a dedicated inspect
command the only available signal is the daemon log, which requires direct
machine access and provides no structured overview.

The primary information need is **live node state** — what the node itself
reports: its full lifecycle phase, exact token placement on the ring, every
partition range it owns, its complete view of cluster membership, and the local
phi-accrual probe state for every peer. `tourctl` connects directly to the
target's peer address over mTLS; no intermediary contact node is involved.

---

## CLI contract

### `tourctl node inspect <address>`

Opens a direct mTLS connection to `<address>` (the target's peer address, e.g.
`peer-3.prod.example.com:7701`), sends a `node.inspect` self-inspect request,
and renders a structured summary.

```
$ tourctl node inspect peer-3.prod.example.com:7701
Inspecting peer-3.prod.example.com:7701 (node-3) ...

  Identity
    Node ID:      node-3
    Phase:        READY
    Size:         M  (4 vnodes)
    Generation:   1
    Seq:          7
    Epoch:        3

  Addresses
    Peer:   peer-3.prod.example.com:7701
    KV:     kv-3.prod.example.com:7700

  Ring position  (partition_shift=10, 1 024 total partitions)
    Owned partitions:  256  (25.0 % of total)
    Vnode ranges:      4
      token 0x1a3cb4f2…  →  pids  0–63     (64 partitions)
      token 0x4f7b3200…  →  pids  256–319  (64 partitions)
      token 0x8d220100…  →  pids  512–575  (64 partitions)
      token 0xc901ff00…  →  pids  768–831  (64 partitions)

  Cluster membership  (as seen by node-3)
    Members:  3 total
      node-1   READY     gen=1  seq=12
      node-2   READY     gen=1  seq=9
      node-3   READY     gen=1  seq=7   ← this node

  Probe state  (node-3's local failure detector)
    node-1   LIVE
    node-2   LIVE
```

The header line reads `(node-id)` using the `node_id` field returned in the
response payload (self-reported by the target).

#### `--timeout <duration>`

Override the default response timeout for this request. Accepts values in the
same format as duration config fields (`10s`, `5.5s`, …). Default:
`RESPONSE_TIMEOUT` (30 s).

#### `--json`

Emit the raw `NodeInspectResponse` as pretty-printed JSON to stdout instead of
the Rich table. All fields defined in the response payload schema are included;
truncated sections report `members_truncated=true` and `members_total=N` (see
§ Payload size budget). Designed for programmatic consumers and shell pipelines.

```
$ tourctl node inspect peer-3.prod.example.com:7701 --json
{
  "node_id": "node-3",
  "phase": "ready",
  "peer_address": "peer-3.prod.example.com:7701",
  "kv_address": "kv-3.prod.example.com:7700",
  "size": "M",
  "generation": 1,
  "seq": 7,
  "epoch": 3,
  "tokens": [439804651110,...],
  "total_partitions": 1024,
  "partition_shift": 10,
  "owned_partitions": 256,
  "partition_ranges": [
    {"start_pid": 0,   "end_pid": 63,  "count": 64, "token_hex": "0x1a3cb4f200000000000000000000001a"},
    {"start_pid": 256, "end_pid": 319, "count": 64, "token_hex": "0x4f7b320000000000000000000000004f"},
    {"start_pid": 512, "end_pid": 575, "count": 64, "token_hex": "0x8d2201000000000000000000000000a1"},
    {"start_pid": 768, "end_pid": 831, "count": 64, "token_hex": "0xc901ff000000000000000000000000c9"}
  ],
  "members": [...],
  "members_truncated": false,
  "members_total": 3,
  "probe_states": [...],
  "probe_states_truncated": false
}
```

Error responses are always written to stderr and still exit with code 1,
regardless of `--json`.

#### `--partitions`

Force the CLI to render every partition range individually, even when
`partition_shift` is large. Each range uses the standard vnode line format:

```
$ tourctl node inspect peer-3.prod.example.com:7701 --partitions
...
  Ring position  (partition_shift=16, 65 536 total partitions)
    Owned partitions:  16 384  (25.0 % of total)
    Vnode ranges:      32
      token 0x03f2a1b4…  →  pids      0– 2047   (2 048 partitions)
      token 0x11cc8e72…  →  pids   2048– 4095   (2 048 partitions)
      token 0x2a09f301…  →  pids   4096– 6143   (2 048 partitions)
      ...
      token 0xfbd14a09…  →  pids  63488–65535   (2 048 partitions)
```

Without this flag, the output collapses the range list to a single hint line
when `len(partition_ranges) > PARTITION_DISPLAY_THRESHOLD` (default: 64):

```
  Ring position  (partition_shift=16, 65 536 total partitions)
    Owned partitions:  16 384  (25.0 % of total)
    Vnode ranges:      32  (use --partitions to list all ranges)
```

When `len(partition_ranges) ≤ PARTITION_DISPLAY_THRESHOLD`, every range is
always rendered using the same `token 0x…  →  pids  N–M  (K partitions)` line,
even without `--partitions`.

---

## Design

### Data model

#### Request payload

```
kind = node.inspect
payload = {}
```

#### Response payload

```
kind = node.inspect.response
payload = {
  "node_id":               str,
  "phase":                 str,          # MemberPhase value
  "peer_address":          str,
  "kv_address":            str,
  "size":                  str,          # NodeSize value (e.g. "M")
  "generation":            int,
  "seq":                   int,
  "epoch":                 int,
  "tokens":                list[int],    # sorted ascending
  "total_partitions":      int,
  "partition_shift":       int,
  "owned_partitions":      int,
  "partition_ranges": [                  # one entry per vnode, sorted by start_pid
    {"start_pid": int, "end_pid": int, "count": int, "token_hex": str},
    ...
  ],
  # token_hex is the full lowercase hex representation of the vnode token
  # prefixed with "0x" (e.g. "0x1a3cb4f200000000000000000000000a").
  # The CLI renders it truncated to the first 8 hex digits followed by "…":
  #   token 0x1a3cb4f2…  →  pids  0–63  (64 partitions)
  "members": [                           # may be truncated; see members_truncated
    {"node_id": str, "phase": str, "generation": int, "seq": int, "peer_address": str},
    ...
  ],
  "members_truncated":     bool,         # true when registry was cut to fit the payload budget
  "members_total":         int,          # total registry size before any truncation
  "probe_states": [                      # may be truncated; see probe_states_truncated
    {"node_id": str, "state": str, "phi": float},
    ...
  ],
  "probe_states_truncated": bool         # true when probe table was cut to fit the payload budget
}
```

Error kinds returned on the peer endpoint (after the standard error response
flow defined in proposal 001):

| `kind` | When |
|--------|------|
| `error.node_unreachable` | tourctl could not connect to `<address>` within the attempt timeout |
| `error.node_not_inspectable` | Target node received the request but its phase does not allow self-inspection (e.g. `IDLE` with no topology yet; the node falls back to returning a minimal response rather than this error — see below) |

### Core invariants

- `tourctl` opens a direct mTLS connection to the address supplied on the
  command line. No intermediary node is involved; no request forwarding occurs.
- The response to `node.inspect` is **always built by the target node itself**
  from its own in-memory state. No other node constructs or modifies it.
- `partition_ranges` are computed purely from the target's own `Partitioner`
  and `Ring` state; they are never read from `state.toml`. They reflect the
  in-memory ring at the time of the request.
- `probe_states` reflect the target's `ProbeManager` at the time of the
  request.
- An `IDLE` node that receives a `node.inspect` request (possible during a
  brief window before sockets are closed or during a crash-recovery restart)
  returns a minimal response: phase, generation, seq, epoch from its current
  `NodeState`, and empty `tokens`, `partition_ranges`, `members`, and
  `probe_states`. The `node.inspect.response` kind is reused; no error is
  emitted.
- The handler for `node.inspect` is registered on the **peer `Dispatcher`
  only**. The KV dispatcher must not expose this handler.

### Sequence / flow

```
tourctl                          node-3
  │                                 │
  │── node.inspect ───────────────► │
  │   {}                            │ build response:
  │                                 │   NodeState, ring,
  │                                 │   partitioner, members,
  │                                 │   probe_states
  │◄── node.inspect.response ───────│
```


### Partition range computation

The target node computes `partition_ranges` as follows, iterating over its
own sorted token list and leveraging the `Partitioner`:

```
For each token t in sorted(self.tokens):
    placement = partitioner.placement_for_token(t, ring)
    arc = arc of the hash space owned by the vnode at t
    start_pid = partitioner.pid_for_hash(arc.start + 1)
    end_pid   = partitioner.pid_for_hash(arc.end)
    count     = end_pid - start_pid + 1
    ranges.append(PartitionRange(start_pid, end_pid, count, hex(t)))
```

The first node (single-node cluster) owns all partitions: one range per vnode,
collectively spanning `[0, total_partitions − 1]`. The CLI renders each range
on one line.

### Payload size budget

`MAX_PAYLOAD_DEFAULT = 4 MiB` (from proposal 001) applies to every `Envelope`
payload, including `node.inspect.response`. The fixed fields (`node_id`, tokens,
`partition_ranges`, addresses, numeric scalars) are bounded by the node size: at
most 32 tokens × ~18 bytes (msgpack 128-bit integer) plus at most 32
`PartitionRange` entries × ~60 bytes ≈ ~2.5 KB — negligible.

The variable-size contributors are `members` and `probe_states`, both of which
grow linearly with the number of nodes in the cluster:

| Cluster size | `members` (est.) | `probe_states` (est.) | Total (est.) |
|-------------:|----------------:|---------------------:|-------------:|
| 1 000        | ~100 KB         | ~48 KB               | ~150 KB      |
| 5 000        | ~500 KB         | ~240 KB              | ~740 KB      |
| 15 000       | ~1.5 MiB        | ~720 KB              | ~2.2 MiB     |
| 25 000       | ~2.5 MiB        | ~1.2 MiB             | ~3.7 MiB     |
| 30 000       | ~3.0 MiB        | ~1.4 MiB             | ~4.4 MiB     |

*(Estimates: `MemberSummary` ~105 bytes msgpack; `ProbeSummary` ~49 bytes
msgpack; `node_id` ~30 bytes, `peer_address` ~50 bytes.)*

The handler must not send a payload that exceeds `MAX_PAYLOAD_DEFAULT`
(the server-side transport would reject it when reading the response back).
The truncation algorithm is applied **before serialization** using the constant
`INSPECT_MEMBER_LIMIT = 10_000`:

```
1. Collect the full members list (sorted by node_id).
2. If len(members) > INSPECT_MEMBER_LIMIT:
       members = members[:INSPECT_MEMBER_LIMIT]
       members_truncated = True
3. Collect the full probe_states list (sorted by node_id).
4. If len(probe_states) > INSPECT_MEMBER_LIMIT:
       probe_states = probe_states[:INSPECT_MEMBER_LIMIT]
       probe_states_truncated = True
5. Serialize the full response.
6. If len(serialized) > MAX_PAYLOAD_DEFAULT:
       # Binary-search for the largest members prefix that fits, then
       # try with probe_states=[] first:
       probe_states = []
       probe_states_truncated = True
       Re-serialize.
       If still > MAX_PAYLOAD_DEFAULT:
           Binary-search members prefix that fits with probe_states=[].
           members_truncated = True
7. Send the (possibly truncated) response.
```

Step 6 is a safety fallback for adversarial or misconfigured clusters with
unusually long `node_id` or `peer_address` strings. It is not expected to
trigger under any realistic Tourillon deployment below `INSPECT_MEMBER_LIMIT`.

Both `members_truncated` and `probe_states_truncated` are always present in the
payload (defaulting to `false`). `members_total` always reports the true
registry size before truncation. The CLI warns the operator when either flag is
`true`:

```
  ⚠ Membership list truncated: showing 10 000 of 31 427 members.
    Use --json to pipe the full JSON payload for offline analysis when available.
```

### Error paths

| Condition | Response |
|-----------|----------|
| tourctl cannot connect to `<address>` (timeout / refused) | `error.node_unreachable` response; tourctl prints `✗ peer-3.prod.example.com:7701 is unreachable (connection refused).` exit 1 |
| Target node in a phase that has no peer socket (e.g. shut down mid-transition) | TCP connection refused |
| Response timeout | `ResponseTimeoutError` on the TcpClient; tourctl prints `✗ Inspect timed out after <N>s.` exit 1 |

---

## Design decisions

### Decision: direct connection — no contact-node proxy

**Alternatives considered:** route through a well-known contact node that
forwards to the target (original design), or redirect (return target address
for tourctl to reconnect).

**Chosen because:** since `tourctl` already knows the target's peer address
(the operator supplies it explicitly), adding an intermediary hop adds latency,
complexity, and a second point of failure with no benefit. Direct mTLS gives
the operator an immediate, unambiguous view of the exact node they addressed.
The operator is responsible for having network access to the target address
(VPN, bastion, etc.) — the same requirement that applies to `tourctl node join`.

### Decision: address-based routing

**Alternatives considered:** pass `node_id` and require a registry-owning
contact to route the request.

**Chosen because:** `tourctl` already supplies the target's peer address
directly, so no registry lookup is needed and routing is deterministic
regardless of gossip convergence state. The node self-identifies in the
response (`node_id` field), giving the operator its logical name after the
fact.

### Decision: partition range display threshold

**Alternatives considered:** always display every range; always display a
summary; display per-token rather than per-range.

**Chosen because:** with `partition_shift=10` (1 024 partitions) and a 4-vnode
node the output is 4 lines — entirely readable. With `partition_shift=16`
(65 536 partitions) and 32 vnodes (XXL node) the output is 32 lines of ranges,
each covering ~2 048 partitions — still readable but long. The threshold
(`PARTITION_DISPLAY_THRESHOLD = 64 ranges`) covers every realistic setup at
≤ `partition_shift=15` without truncation. Operators who genuinely need the
full list on large spaces use `--partitions`. The response payload always
carries the complete data; truncation is a CLI rendering decision only.

### Decision: partition range display threshold

**Alternatives considered:** return `error.node_not_inspectable`; refuse the
request entirely.

**Chosen because:** an `IDLE` node with a bound peer socket (in a transient
window during startup) has valid lifecycle state. Returning a minimal response
allows the operator to still observe phase, generation, and epoch even before
the ring is populated. An error response would make it impossible to distinguish
"node exists but not yet ready" from "node unreachable".

### Decision: handler registered on peer dispatcher only

**Alternatives considered:** expose inspect on both KV and peer dispatchers.

**Chosen because:** the KV endpoint is the data-plane and is scoped to KV
operations only (put, get, delete, replicate, hint). Exposing introspection
there would blur the boundary between the two planes and require client
certificates that are normally scoped to application clients only.

### Decision: `--json` outputs raw response bytes, not a reformatted dict

**Alternatives considered:** reconstruct a custom JSON schema for human
readability; emit YAML.

**Chosen because:** the response payload schema is already well-structured and
self-documenting. Reflecting it verbatim as JSON gives programmatic consumers
a stable, predictable schema that mirrors the wire format exactly. Any
post-processing (filtering, joining outputs across nodes) can then be done
with standard tools (`jq`, Python dicts). Reformatting into a different schema
would introduce a second surface to maintain and risk divergence from the
proposal.

### Decision: `INSPECT_MEMBER_LIMIT` constant rather than serialisation probe

**Alternatives considered:** always serialize the full response and check
byte length; reject the request with an error if the limit is exceeded.

**Chosen because:** a constant limit of 10 000 members is deterministic,
O(1) to check, and covers all realistic Tourillon deployments (a 10 000-node
cluster would require `partition_shift ≥ 17`, which is a dedicated large-scale
configuration). The binary-search fallback in step 6 of the truncation algorithm
handles pathological edge cases without introducing a protocol error that would
force the operator to use a different tool. Rejecting large responses would make
`node inspect` useless precisely when the cluster is under strain and visibility
matters most.

---

## Interfaces (informative)

```python
# tourillion/core/structure/inspect.py

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PartitionRange:
    """A contiguous range of partition IDs owned by one vnode.

    start_pid and end_pid are both inclusive. token_hex is the full lowercase
    hex representation of the vnode token, prefixed with "0x"
    (e.g. "0x1a3cb4f200000000000000000000000a"). The CLI truncates it to the
    first 8 hex digits followed by the ellipsis character "…" when rendering
    the standard vnode line:

        token 0x1a3cb4f2…  →  pids  0–63  (64 partitions)

    All partition range output — whether from a direct inspect or from
    --partitions — uses this exact format. The response payload always carries
    the untruncated token_hex; truncation is a CLI rendering decision only.
    """

    start_pid: int
    end_pid: int
    count: int
    token_hex: str  # full "0x<hex>" string; CLI truncates to "0x<8 chars>…" for display


@dataclass(frozen=True)
class MemberSummary:
    """Condensed Member record for inclusion in NodeInspectResponse.members."""

    node_id: str
    phase: str        # MemberPhase value
    generation: int
    seq: int
    peer_address: str


@dataclass(frozen=True)
class ProbeSummary:
    """Local probe state for one peer, as seen by the inspected node."""

    node_id: str
    state: str    # MemberState value: "live" | "suspect" | "unknown"
    phi: float    # current φ value; 0.0 when no observations yet


@dataclass(frozen=True)
class NodeInspectResponse:
    """Full live snapshot returned by the target node in response to node.inspect.

    partition_ranges contains one entry per owned vnode, sorted by start_pid.
    members contains entries from the target's MemberRegistry, sorted by
    node_id, truncated to INSPECT_MEMBER_LIMIT when the registry is large.
    members_total always reflects the true registry size before truncation.
    probe_states contains entries from the target's ProbeManager, sorted by
    node_id, subject to the same limit.

    Both members_truncated and probe_states_truncated default to False and are
    always present in the serialised payload.
    """

    node_id: str
    phase: str            # MemberPhase value
    peer_address: str
    kv_address: str
    size: str             # NodeSize value
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


# tourillon/bootstrap/handlers/inspect.py  (registered on peer Dispatcher)

class NodeInspectHandler:
    """ConnectionHandler for node.inspect on the peer endpoint.

    Receives an empty payload. Builds NodeInspectResponse from the node's own
    in-memory state (NodeState, Ring, Partitioner, MemberRegistry,
    ProbeManager) and sends it back. No outbound connection is made.
    """

    def __init__(
        self,
        node_id: str,
        peer_address: str,
        node_state: "NodeStateAccessor",
        topology_manager: "TopologyManager",
        probe_manager: "ProbeManager",
        partitioner: "Partitioner",
        kv_address: str,
    ) -> None: ...

    async def __call__(
        self,
        receive: "ReceiveEnvelope",
        send: "SendEnvelope",
    ) -> None: ...


# tourctl/core/commands/inspect.py

class InspectCommand:
    """Opens a direct mTLS connection to target_address and renders the result.

    Sends node.inspect with an empty payload, awaits NodeInspectResponse, and
    renders the structured output. The standard format for every vnode
    partition range line is:

        token 0x<8 hex digits>…  →  pids  <start>–<end>  (<count> partitions)

    This format is used unconditionally whenever partition ranges are rendered,
    whether from a direct inspect or --partitions. When json_output=True the
    raw response is serialised to JSON and written to stdout (token_hex in the
    JSON carries the full untruncated hex string); otherwise Rich is used for
    human-readable tabular output. CLI rendering collapses partition_ranges to
    a hint line when len(partition_ranges) > PARTITION_DISPLAY_THRESHOLD unless
    show_all_partitions=True. A truncation warning is printed when
    members_truncated or probe_states_truncated is True.
    """

    PARTITION_DISPLAY_THRESHOLD: int = 64
    INSPECT_MEMBER_LIMIT: int = 10_000

    async def run(
        self,
        target_address: str,
        show_all_partitions: bool,
        json_output: bool,
        timeout: float,
    ) -> None: ...
```

---

## Proposed code organisation

```
tourillon/
  core/
    handlers/
      inspect.py     # NodeInspectHandler
    structure/
      inspect.py     # NodeInspectResponse, PartitionRange,
                     # MemberSummary, ProbeSummary

tourctl/
  core/
    commands/
      inspect.py     # InspectCommand — serialises request, deserialises response
  infra/
    cli/
      node.py        # Typer command: `tourctl node inspect` (registers InspectCommand)
```

No new files are added to `tourillon/infra/cli/` — `NodeInspectHandler` is
instantiated at server startup and registered on the peer `Dispatcher`. No
changes to `tourillon/infra/cli/node.py` beyond wiring the new handler.

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | Single in-memory node (node-1, peer `127.0.0.1:7701`), `READY`, 4 vnodes, `partition_shift=10` | `InspectCommand.run(target_address="127.0.0.1:7701")` — direct mTLS connection | Receives `node.inspect.response`; `node_id="node-1"`, `owned_partitions=1024`, `len(partition_ranges)==4` |
| 2 | In-memory node (node-2, peer `127.0.0.2:7701`), `READY`, 4 vnodes | `InspectCommand.run(target_address="127.0.0.2:7701")` — direct mTLS connection | Receives `node.inspect.response` with `node_id="node-2"`; no intermediary involved |
| 3 | No node listening at `127.0.0.9:7701` | `InspectCommand.run(target_address="127.0.0.9:7701")` | Direct connection refused; tourctl prints `✗ 127.0.0.9:7701 is unreachable (connection refused).`; exit 1 |
| 4 | node-2's peer server has been stopped; address `127.0.0.2:7701` reports connection refused | `InspectCommand.run(target_address="127.0.0.2:7701")` | Connection refused; tourctl prints unreachable message; exit 1 |
| 5 | node-1 in phase `IDLE` (no topology built yet), peer `127.0.0.1:7701` | `InspectCommand.run(target_address="127.0.0.1:7701")` | Returns `node.inspect.response` with `phase="idle"`, empty `tokens`, `partition_ranges`, `members`, `probe_states` |
| 6 | `NodeInspectResponse` serialisation round-trip | Encode with `MsgpackSerializerAdapter`, decode | All fields preserved exactly including nested `PartitionRange` tuples |
| 7 | In-memory target at `127.0.0.1:7701` delays response beyond `RESPONSE_TIMEOUT` | `InspectCommand.run(target_address="127.0.0.1:7701")` | `ResponseTimeoutError` raised in `TcpClient`; tourctl prints `✗ Inspect timed out after <N>s.`; exit 1 |
| 8 | Single node, `partition_shift=4`, 8 vnodes (size L), peer `127.0.0.1:7701` | `NodeInspectHandler._build_partition_ranges()` | Returns 8 `PartitionRange` entries; `sum(r.count for r in ranges) == 16` (total partitions); all `[start_pid, end_pid]` non-overlapping |
| 9 | `partition_ranges` with `len > PARTITION_DISPLAY_THRESHOLD` | `InspectCommand.run(show_all_partitions=False)` | CLI prints summary line `N ranges (use --partitions to list all)`; no individual range lines |
| 10 | Same as 9 | `InspectCommand.run(show_all_partitions=True)` | CLI prints all individual range lines |
| 11 | Single node `READY`; `InspectCommand.run(json_output=True)` | Execute command | stdout contains valid JSON; `members_truncated=false`; `probe_states_truncated=false`; all numeric fields present |
| 12 | In-memory node with `INSPECT_MEMBER_LIMIT + 1` registry entries | Build `NodeInspectResponse` via handler | `members` contains exactly `INSPECT_MEMBER_LIMIT` entries; `members_truncated=True`; `members_total == INSPECT_MEMBER_LIMIT + 1` |
| 13 | In-memory node with `INSPECT_MEMBER_LIMIT + 1` probe entries | Build `NodeInspectResponse` via handler | `probe_states` contains exactly `INSPECT_MEMBER_LIMIT` entries; `probe_states_truncated=True` |
| 14 | `InspectCommand.run(json_output=False)` with `members_truncated=True` | Render rich output | CLI prints warning line `⚠ Membership list truncated: showing N of M members.` |
| 15 | Serialized response with all fields at maximum realistic size (10 000 members × max-length strings) | Serialize with `MsgpackSerializerAdapter` | `len(serialized) <= MAX_PAYLOAD_DEFAULT` (4 MiB); no truncation step needed at this limit |
| 16 | Simulated response pre-truncation that would exceed `MAX_PAYLOAD_DEFAULT` (step-6 fallback) | Handler clears `probe_states`, re-serializes | Final payload ≤ 4 MiB; `probe_states_truncated=True`; `members` untouched if it now fits |
| 17 `[e2e]` | Two real nodes (node-1, node-2) with mTLS; node-2 running | `tourctl node inspect <node-2-peer-addr>` — direct connection | Full CLI output rendered; data self-reported by node-2 |
| 18 `[e2e]` | Two real nodes; node-2 stopped | `tourctl node inspect <node-2-peer-addr>` | CLI prints `✗ <addr> is unreachable` |
| 19 `[e2e]` | Two real nodes | `tourctl node inspect <node-2-peer-addr> --json` | stdout is valid JSON parseable by `json.loads()`; all payload fields defined in the response schema are present |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `tourctl node inspect <address>` opens a direct mTLS connection to `<address>`; no intermediary node is involved and no forwarding occurs.
- [ ] The target node builds the inspect response entirely from its own in-memory state; no other node constructs or modifies it.
- [ ] `node.inspect` handler is registered on the **peer** `Dispatcher` only; the KV `Dispatcher` closes the connection on unknown kind (verified by scenario from proposal 001).
- [ ] Partition ranges sum to correct total for every `NodeSize` × `partition_shift` combination tested.
- [ ] CLI truncates partition output above `PARTITION_DISPLAY_THRESHOLD` unless `--partitions` is passed.
- [ ] `error.node_unreachable` returned when the connection to `target_address` fails.
- [ ] `--json` emits valid JSON to stdout; output is parseable by `json.loads()` and matches the payload schema exactly.
- [ ] `members_truncated=True` and `members_total=N` are emitted when the registry exceeds `INSPECT_MEMBER_LIMIT`; CLI warns the operator.
- [ ] `probe_states_truncated=True` is emitted when probe table exceeds `INSPECT_MEMBER_LIMIT`.
- [ ] Serialized `NodeInspectResponse` with exactly `INSPECT_MEMBER_LIMIT` members fits within `MAX_PAYLOAD_DEFAULT` (4 MiB) — verified by a dedicated size assertion in the test suite.
- [ ] Step-6 fallback (clear `probe_states`, re-serialize) produces a payload ≤ `MAX_PAYLOAD_DEFAULT`.
- [ ] `uv run pytest -m inspect` passes with zero failures.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Streaming inspect (following partition migration in real time), cluster-wide
broadcast inspect (aggregate view from all nodes simultaneously), health-check
endpoint (lighter-weight liveness probe — separate proposal), KV data-plane
metrics (key counts, bytes stored per partition), historical gossip log replay,
and gossip-view queries (asking a live node for its cached record of a
potentially unreachable peer — see `docs/known-issues-and-todos.md`). These
are left to dedicated proposals.
