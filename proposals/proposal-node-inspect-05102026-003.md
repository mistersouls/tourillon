# Proposal: Node Inspect (`tourctl node inspect`)

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Accepted
**Date:** 2026-05-10
**Sequence:** 003
**Schema version:** 1

---

## Summary

`tourctl node inspect <node-id>` retrieves a rich, live snapshot of a cluster
node's identity, ring position, partition ownership, cluster membership view,
and failure-detector state. The request is sent to the contact node's peer
endpoint over mTLS; if the contact is not the target, it transparently proxies
the request to the target's peer address and relays the response. The target
node always builds and returns the response itself — no other node constructs
it on the target's behalf. A separate `--peer-view` flag lets the operator
query the contact node's gossip record for the target instead of reaching the
target directly, which is useful for debugging an unreachable node.

---

## Motivation

Operators of a leaderless distributed system rely heavily on per-node
introspection to diagnose imbalances, verify ring topology convergence, detect
stale gossip, and understand partition ownership. Without a dedicated inspect
command the only available signal is the daemon log, which requires direct
machine access and provides no structured overview.

Two distinct information needs exist:

1. **Live node state** — what the node itself reports: its full lifecycle phase,
   exact token placement on the ring, every partition range it owns, its
   complete view of cluster membership, and the local phi-accrual probe state
   for every peer. This is the primary mode and is always answered by the
   target node.

2. **Gossip view** — what a peer reports about the target: its cached `Member`
   record (phase, generation, seq, tokens) and its local probe state for the
   target. This mode is used when the target is unreachable and its peer
   endpoint refuses connections, or when an operator wants to compare two
   nodes' views of a third.

The forwarding mechanism is necessary because `tourctl` typically connects to a
single well-known peer endpoint (from `contexts.toml`). It cannot be expected
to know or have firewall access to every node's peer address. The contact node
handles the routing transparently.

---

## CLI contract

### `tourctl node inspect <node-id>`

Contacts the peer endpoint in the active context, sends a `node.inspect`
request for `<node-id>`, waits for the response (from the target, proxied
through the contact if needed), and renders a structured summary.

```
$ tourctl node inspect node-3
Inspecting node-3 (via node-1) ...

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

When the contact node (node-1) is also the target (`node-id == contact node`),
the request is answered locally without any forwarding.

#### `--timeout <duration>`

Override the default response timeout for this request. Accepts values in the
same format as duration config fields (`10s`, `5.5s`, …). Default:
`RESPONSE_TIMEOUT` (30 s).

#### `--json`

Emit the raw `NodeInspectResponse` (or `NodePeerViewResponse` when combined
with `--peer-view`) as pretty-printed JSON to stdout instead of the Rich table.
All fields defined in the response payload schema are included; truncated
sections report `members_truncated=true` and `members_total=N` (see
§ Payload size budget). Designed for programmatic consumers and shell pipelines.

```
$ tourctl node inspect node-3 --json
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
  "probe_states_truncated": false,
  "forwarded_by": "node-1"
}
```

When combined with `--peer-view`:

```
$ tourctl node inspect node-3 --peer-view --json
{
  "target_node_id": "node-3",
  "observed_by": "node-1",
  "phase": "ready",
  "generation": 1,
  "seq": 7,
  "peer_address": "peer-3.prod.example.com:7701",
  "tokens": [439804651110,...],
  "probe_state": "live",
  "phi": 1.24
}
```

Error responses are always written to stderr and still exit with code 1,
regardless of `--json`.

#### `--partitions`

Force the CLI to render every partition range individually, even when
`partition_shift` is large. Each range uses the standard vnode line format:

```
$ tourctl node inspect node-3 --partitions
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

#### `--peer-view`

Asks the contact node to return its own gossip record for `<node-id>` without
contacting the target. Does **not** perform any forwarding: the contact node
answers from its `MemberRegistry` alone.

```
$ tourctl node inspect node-3 --peer-view
Gossip view of node-3  (observed by node-1) ...

  ⚠ This is node-1's cached gossip record, not a live response from node-3.

  Phase:       READY
  Generation:  1
  Seq:         7
  Peer:        peer-3.prod.example.com:7701
  Tokens:      4 tokens
  Probe state: LIVE  (φ = 1.24)
```

If the contact node has no entry for the given `node-id` in its registry, it
returns an error:

```
✗ node-1 has no gossip record for node-3.
  exit code 1
```

---

## Design

### Data model

#### Request payloads

```
kind = node.inspect
payload = {
  "target_node_id": str   # node to inspect
}

kind = node.inspect.peer_view
payload = {
  "target_node_id": str   # node whose gossip record is requested
}
```

#### Response payloads

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
  "probe_states_truncated": bool,        # true when probe table was cut to fit the payload budget
  "forwarded_by":          str | None    # node_id of the contact that proxied this, if any
}

kind = node.inspect.peer_view.response
payload = {
  "target_node_id": str,
  "observed_by":    str,         # node_id of the contact node answering
  "phase":          str,
  "generation":     int,
  "seq":            int,
  "peer_address":   str,
  "tokens":         list[int],
  "probe_state":    str,         # MemberState: "live" | "suspect" | "unknown"
  "phi":            float        # current φ value; 0.0 when no observations
}
```

Error kinds returned on the peer endpoint (after the standard error response
flow defined in proposal 001):

| `kind` | When |
|--------|------|
| `error.node_not_found` | Contact node has no registry entry for `target_node_id` (forwarding path), or registry is empty |
| `error.node_unreachable` | Contact node could not connect to target's peer address within the attempt timeout |
| `error.node_not_inspectable` | Target node received the request but its phase does not allow self-inspection (e.g. `IDLE` with no topology yet; the node falls back to returning a minimal response rather than this error — see below) |
| `error.forward_loop` | A node detecting that `target_node_id` equals its own node_id refuses to forward and returns an error if it cannot answer locally (safety guard against misconfiguration) |

### Core invariants

- The response to `node.inspect` is **always built by the target node itself**.
  A contact node that proxies the request does not modify, supplement, or
  re-interpret the payload. It copies the target's response envelope unchanged
  back to the tourctl client.
- `node.inspect.peer_view` is **always answered by the contact node from its
  own registry** without any forwarding. The contact never relays this request
  to the target.
- The contact node **performs at most one forwarding hop**. If the contact node
  receives `node.inspect` and is not the target, it opens one TcpClient
  connection to the target, forwards the request, and proxies the response.
  The target never forwards further, even if it is also not the intended
  destination (misconfiguration error → `error.forward_loop`).
- Forwarding uses a fresh `TcpClient` connection and a new `correlation_id` for
  the sub-request. The outer `correlation_id` (from tourctl) is preserved in
  the proxied response envelope returned to tourctl.
- The contact node closes its TcpClient connection to the target immediately
  after receiving the response (one-shot pattern per proposal 001).
- `partition_ranges` are computed purely from the target's own `Partitioner`
  and `Ring` state; they are never read from `state.toml`. They reflect the
  in-memory ring at the time of the request.
- `probe_states` reflect the target's `ProbeManager` at the time of the
  request. For the `--peer-view` path, the `phi` value is read from the
  contact's `ProbeManager` for the target.
- An `IDLE` node that receives a `node.inspect` request (possible during a
  brief window before sockets are closed or during a crash-recovery restart)
  returns a minimal response: phase, generation, seq, epoch from its current
  `NodeState`, and empty `tokens`, `partition_ranges`, `members`, and
  `probe_states`. The `node.inspect.response` kind is reused; no error is
  emitted.
- The handler for `node.inspect` is registered on the **peer `Dispatcher`
  only**. The KV dispatcher must not expose this handler.

### Sequence / flow — direct inspect (with forwarding)

```
tourctl                          node-1 (contact)              node-3 (target)
  │                                    │                              │
  │── node.inspect ──────────────────► │                              │
  │   {target: "node-3"}               │                              │
  │                                    │  lookup "node-3" in registry │
  │                                    │  found: peer = peer-3:7701   │
  │                                    │── node.inspect ─────────────►│
  │                                    │   {target: "node-3"}         │
  │                                    │   (new correlation_id)       │
  │                                    │                              │ build response:
  │                                    │                              │   NodeState, ring,
  │                                    │                              │   partitioner, members,
  │                                    │                              │   probe_states
  │                                    │◄─ node.inspect.response ─────│
  │                                    │   forwarded_by=None          │
  │                                    │                              │
  │                             set forwarded_by="node-1"             │
  │◄── node.inspect.response ──────────│                              │
  │    forwarded_by="node-1"           │                              │
  │    (original correlation_id)       │                              │
```

When the contact IS the target (`target_node_id == self.node_id`):

```
tourctl                          node-3 (contact == target)
  │                                    │
  │── node.inspect ──────────────────► │
  │   {target: "node-3"}               │ build response locally
  │◄── node.inspect.response ──────────│ forwarded_by=None
```

### Sequence / flow — `--peer-view`

```
tourctl                          node-1 (contact)
  │                                    │
  │── node.inspect.peer_view ────────► │
  │   {target: "node-3"}               │ lookup "node-3" in own registry
  │                                    │ read own ProbeManager state for "node-3"
  │◄── node.inspect.peer_view.response─│ no connection to node-3 at any point
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

`NodePeerViewResponse` is small and fixed-size (no member list); it is never
subject to truncation.

### Error paths

| Condition | Response |
|-----------|----------|
| `target_node_id` not found in contact node's registry | `error.node_not_found` response; tourctl prints `✗ node-1 has no routing entry for node-3.` exit 1 |
| Contact node cannot connect to target peer address (timeout / refused) | `error.node_unreachable` response; tourctl prints `✗ node-3 is unreachable (peer-3:7701: connection refused). Try --peer-view to query node-1's gossip record.` exit 1 |
| `--peer-view` and no registry entry for target | `error.node_not_found` response; tourctl prints `✗ node-1 has no gossip record for node-3.` exit 1 |
| Target node in a phase that has no peer socket (e.g. shut down mid-transition) | TCP connection refused → `error.node_unreachable` on the contact |
| `node.inspect` received by a node that is itself not the target and has no entry for the target | `error.node_not_found` |
| Response timeout (either tourctl ↔ contact or contact ↔ target) | `ResponseTimeoutError` on the affected TcpClient; tourctl prints `✗ Inspect timed out after <N>s.` exit 1 |

---

## Design decisions

### Decision: single forwarding hop, no multi-hop chaining

**Alternatives considered:** allow cascading forwarding (node-1 → node-2 →
node-3 if node-2 knows node-3 but node-1 does not).

**Chosen because:** multi-hop forwarding introduces unbounded latency,
amplifies the blast radius of a misconfiguration (forwarding loops), and is
unnecessary in a well-converged gossip cluster — every node's registry should
contain all live peers within a few gossip rounds. Enforcing exactly one hop
makes the failure surface predictable: if the contact cannot reach the target
directly, it surfaces `error.node_unreachable` and the operator uses
`--peer-view` for offline diagnostics.

### Decision: contact node proxies response, not redirects

**Alternatives considered:** contact node replies with `node.inspect.redirect`
carrying the target's peer address, then tourctl opens a second connection
directly to the target.

**Chosen because:** tourctl operators configure a single peer endpoint in
`contexts.toml`. Firewalls, NAT, or network segmentation may prevent tourctl
from reaching internal peer addresses directly. Proxying through the contact
node preserves the single-entry-point model from proposal 001. The latency
overhead of one extra hop is negligible for an infrequent operator command.

### Decision: `--peer-view` is a separate envelope kind, not a flag in the payload

**Alternatives considered:** a single `node.inspect` payload with a boolean
`peer_view` field that the contact node interprets.

**Chosen because:** the two modes have fundamentally different semantics and
response schemas. A boolean field would force the contact node to branch on a
flag deep in the handler and return two incompatible payload shapes under the
same `kind`. Separate envelope kinds make the handler table explicit,
self-documenting, and consistent with the `<domain>.<verb>` naming convention.

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

### Decision: `forwarded_by` field in the response payload

**Alternatives considered:** omit it; convey it as a log line in the CLI.

**Chosen because:** the `forwarded_by` field makes the routing hop explicit in
the structured response data, which is useful for programmatic consumers
(scripts, monitoring) and for debugging. The CLI displays it in the header
line (`via node-1`) only when the value is non-null, keeping the happy-path
output clean.

### Decision: IDLE nodes return a minimal response rather than an error

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
    node_id, subject to the same limit. forwarded_by is None when the contact
    node and the target node are the same; otherwise it is the node_id that
    proxied the request.

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
    forwarded_by: str | None


@dataclass(frozen=True)
class NodePeerViewResponse:
    """Gossip record returned by the contact node for node.inspect.peer_view.

    This response is constructed from the contact node's own MemberRegistry
    and ProbeManager. It never involves the target node. phi is 0.0 when the
    contact has never recorded a probe observation for the target.
    """

    target_node_id: str
    observed_by: str
    phase: str        # MemberPhase value
    generation: int
    seq: int
    peer_address: str
    tokens: tuple[int, ...]
    probe_state: str  # MemberState value
    phi: float


# tourillon/core/handlers/inspect.py  (registered on peer Dispatcher)

class NodeInspectHandler:
    """ConnectionHandler for node.inspect on the peer endpoint.

    Reads the target_node_id from the request payload. If the target equals
    self.node_id, builds and returns NodeInspectResponse locally. Otherwise,
    looks up the target in the TopologyManager registry, opens a TcpClient
    connection to the target's peer_address, forwards the request, awaits the
    response, sets forwarded_by=self.node_id in the response payload, and
    returns it to the caller. The TcpClient connection is closed after the
    first response is received.

    If the target is not found in the registry, sends error.node_not_found.
    If the target is unreachable, sends error.node_unreachable.
    """

    def __init__(
        self,
        node_id: str,
        node_state: "NodeStateAccessor",
        topology_manager: "TopologyManager",
        probe_manager: "ProbeManager",
        partitioner: "Partitioner",
        peer_address: str,
        kv_address: str,
        tls_ctx: "ssl.SSLContext",
        attempt_timeout: float,
    ) -> None: ...

    async def __call__(
        self,
        receive: "ReceiveEnvelope",
        send: "SendEnvelope",
    ) -> None: ...


class NodeInspectPeerViewHandler:
    """ConnectionHandler for node.inspect.peer_view on the peer endpoint.

    Reads the target_node_id from the payload. Returns NodePeerViewResponse
    built from self's MemberRegistry and ProbeManager without contacting the
    target. Sends error.node_not_found if the target is absent from the
    registry.
    """

    def __init__(
        self,
        node_id: str,
        topology_manager: "TopologyManager",
        probe_manager: "ProbeManager",
    ) -> None: ...

    async def __call__(
        self,
        receive: "ReceiveEnvelope",
        send: "SendEnvelope",
    ) -> None: ...


# tourctl/core/commands/inspect.py

class InspectCommand:
    """Sends node.inspect (or node.inspect.peer_view) and renders the result.

    Connects to the peer endpoint in the active context, sends the request,
    awaits NodeInspectResponse (or NodePeerViewResponse), and renders the
    structured output. The standard format for every vnode partition range line
    is:

        token 0x<8 hex digits>…  →  pids  <start>–<end>  (<count> partitions)

    This format is used unconditionally whenever partition ranges are rendered,
    whether from a self-inspect, a forwarded inspect, or --partitions. When
    json_output=True the raw response is serialised to JSON and written to
    stdout (token_hex in the JSON carries the full untruncated hex string);
    otherwise Rich is used for human-readable tabular output. CLI rendering
    collapses partition_ranges to a hint line when
    len(partition_ranges) > PARTITION_DISPLAY_THRESHOLD unless
    show_all_partitions=True. A truncation warning is printed when
    members_truncated or probe_states_truncated is True.
    """

    PARTITION_DISPLAY_THRESHOLD: int = 64
    INSPECT_MEMBER_LIMIT: int = 10_000

    async def run(
        self,
        target_node_id: str,
        peer_view: bool,
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
      inspect.py     # NodeInspectHandler, NodeInspectPeerViewHandler
    structure/
      inspect.py     # NodeInspectResponse, NodePeerViewResponse, PartitionRange,
                     # MemberSummary, ProbeSummary

tourctl/
  core/
    commands/
      inspect.py     # InspectCommand — serialises request, deserialises response
  infra/
    cli/
      node.py        # Typer command: `tourctl node inspect` (registers InspectCommand)
```

No new files are added to `tourillon/infra/cli/` — `NodeInspectHandler` and
`NodeInspectPeerViewHandler` are instantiated at server startup and registered
on the peer `Dispatcher`. No changes to `tourillon/infra/cli/node.py` beyond
wiring the two new handlers.

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | Single in-memory node (node-1), `READY`, 4 vnodes, `partition_shift=10` | Send `node.inspect` with `target="node-1"` | Receives `node.inspect.response`; `node_id="node-1"`, `owned_partitions=1024`, `len(partition_ranges)==4`, `forwarded_by=None` |
| 2 | Two in-memory nodes (node-1, node-2), both `READY`; node-1 is contact | Send `node.inspect` with `target="node-2"` to node-1 | node-1 forwards to node-2; receives response with `node_id="node-2"`, `forwarded_by="node-1"` |
| 3 | node-1 as contact; `target="node-1"` (self-inspect) | Send `node.inspect` | No forwarding occurs; `forwarded_by=None`; response identical to scenario 1 |
| 4 | node-1 as contact; `target="node-99"` (unknown) | Send `node.inspect` | node-1 returns `error.node_not_found`; no TcpClient connection attempt |
| 5 | node-1 as contact; node-2 registered but peer socket refused | Send `node.inspect` for node-2 | node-1 returns `error.node_unreachable`; connection attempt made to node-2:7701 |
| 6 | node-1 as contact; `MemberRegistry` has node-2 with phase=DRAINING | Send `node.inspect.peer_view` for node-2 | Returns `node.inspect.peer_view.response`; `observed_by="node-1"`, `phase="draining"`, no connection to node-2 |
| 7 | node-1 as contact; `target="node-99"` | Send `node.inspect.peer_view` | Returns `error.node_not_found`; no connection attempt |
| 8 | node-1 in phase `IDLE` (no topology built yet) | Send `node.inspect` with `target="node-1"` | Returns `node.inspect.response` with `phase="idle"`, empty `tokens`, `partition_ranges`, `members`, `probe_states` |
| 9 | `NodeInspectResponse` serialisation round-trip | Encode with `MsgpackSerializerAdapter`, decode | All fields preserved exactly including nested `PartitionRange` tuples |
| 10 | node-1 contact; node-2 target with `probe_state=SUSPECT` | Send `node.inspect.peer_view` for node-2 | `probe_state="suspect"`, `phi > 8.0` in response |
| 11 | node-1 contact; in-memory target holding a `node.inspect` response late by `RESPONSE_TIMEOUT` | Send `node.inspect` | node-1 client raises `ResponseTimeoutError`; node-1 returns `error.node_unreachable` to tourctl; both connections closed |
| 12 | Single node, `partition_shift=4`, 8 vnodes (size L) | `NodeInspectHandler._build_partition_ranges()` | Returns 8 `PartitionRange` entries; `sum(r.count for r in ranges) == 16` (total partitions); all `[start_pid, end_pid]` non-overlapping |
| 13 | `partition_ranges` with `len > PARTITION_DISPLAY_THRESHOLD` | `InspectCommand.run(show_all_partitions=False)` | CLI prints summary line `N ranges (use --partitions to list all)`; no individual range lines |
| 14 | Same as 13 | `InspectCommand.run(show_all_partitions=True)` | CLI prints all individual range lines |
| 15 | `node.inspect` forwarded; target modifies `forwarded_by` to its own node_id | Validate response | Response is rejected; `forwarded_by` is set by the contact node, never by the target (this is a deserialization validation scenario ensuring target returns `forwarded_by=None`) |
| 16 | Contact node receives `node.inspect` for a target it would need to forward to, but target's `peer_address` is empty string in registry | Send `node.inspect` | Returns `error.node_not_found` (no usable address for routing); logged as a warning |
| 17 | Single node `READY`; `InspectCommand.run(json_output=True)` | Execute command | stdout contains valid JSON; `members_truncated=false`; `probe_states_truncated=false`; all numeric fields present |
| 18 | Single node `READY`; `InspectCommand.run(json_output=True, peer_view=True)` | Execute command | stdout contains valid JSON with `target_node_id`, `observed_by`, `probe_state`, `phi` fields |
| 19 | In-memory node with `INSPECT_MEMBER_LIMIT + 1` registry entries | Build `NodeInspectResponse` via handler | `members` contains exactly `INSPECT_MEMBER_LIMIT` entries; `members_truncated=True`; `members_total == INSPECT_MEMBER_LIMIT + 1` |
| 20 | In-memory node with `INSPECT_MEMBER_LIMIT + 1` probe entries | Build `NodeInspectResponse` via handler | `probe_states` contains exactly `INSPECT_MEMBER_LIMIT` entries; `probe_states_truncated=True` |
| 21 | `InspectCommand.run(json_output=False)` with `members_truncated=True` | Render rich output | CLI prints warning line `⚠ Membership list truncated: showing N of M members.` |
| 22 | Serialized response with all fields at maximum realistic size (10 000 members × max-length strings) | Serialize with `MsgpackSerializerAdapter` | `len(serialized) <= MAX_PAYLOAD_DEFAULT` (4 MiB); no truncation step needed at this limit |
| 23 | Simulated response pre-truncation that would exceed `MAX_PAYLOAD_DEFAULT` (step-6 fallback) | Handler clears `probe_states`, re-serializes | Final payload ≤ 4 MiB; `probe_states_truncated=True`; `members` untouched if it now fits |
| 24 `[e2e]` | Two real nodes, mTLS, node-1 contact, node-2 target | `tourctl node inspect node-2` | Full CLI output rendered; `forwarded_by="node-1"` visible |
| 25 `[e2e]` | Two real nodes; node-2 stopped | `tourctl node inspect node-2` | CLI prints `✗ node-2 is unreachable`; suggestion to use `--peer-view` |
| 26 `[e2e]` | Two real nodes; node-2 stopped | `tourctl node inspect node-2 --peer-view` | CLI renders node-1's gossip record for node-2 with last known phase and phi value |
| 27 `[e2e]` | Two real nodes | `tourctl node inspect node-2 --json` | stdout is valid JSON parseable by `json.loads()`; `forwarded_by` field present |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `tourctl node inspect <self>` (self-inspect) returns `forwarded_by=None` and correct partition ranges.
- [ ] `tourctl node inspect <other>` (forwarded) returns `forwarded_by=<contact>` and data built by the target.
- [ ] `node.inspect.peer_view` never opens a connection to the target node; verified by in-memory adapter asserting zero TcpClient calls.
- [ ] `node.inspect` handler is registered on the **peer** `Dispatcher` only; the KV `Dispatcher` closes the connection on unknown kind (verified by scenario from proposal 001).
- [ ] Partition ranges sum to correct total for every `NodeSize` × `partition_shift` combination tested.
- [ ] CLI truncates partition output above `PARTITION_DISPLAY_THRESHOLD` unless `--partitions` is passed.
- [ ] `error.node_not_found` returned when target is absent from registry; `error.node_unreachable` when target is registered but connection fails.
- [ ] `--json` emits valid JSON to stdout; output is parseable by `json.loads()` and matches the payload schema exactly.
- [ ] `--json --peer-view` emits `NodePeerViewResponse` as valid JSON.
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
metrics (key counts, bytes stored per partition), and historical gossip log
replay. These are left to dedicated proposals.
