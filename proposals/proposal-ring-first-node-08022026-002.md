
# Proposal: Ring & First-Node Bootstrap

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Accepted
**Date:** 2026-05-02
**Sequence:** 002
**Schema version:** 2

---

## Summary

This proposal defines the consistent-hashing ring layer — `HashSpace`, `VNode`,
`Ring`, `Partitioner`, `PlacementStrategy`, and `TopologyManager` — and the
first-node bootstrap path that brings a single node from `IDLE` directly to
`READY` without seed contact, token-selection ceremony, or data transfer. It is
the foundational layer on which all subsequent proposals depend. After
implementing this proposal, the ring can be created, queried, and verified in
isolation, and a single node can reach `READY` by running `tourillon node start`
with no seeds configured.

---

## Motivation

Every routing, replication, and placement decision in Tourillon is a pure
function of the consistent-hashing ring. Before any node can join a cluster,
serve traffic, or exchange data, the ring primitives must be correct and
independently verifiable.

Isolating the ring layer into its own proposal means it can be proven correct
without a running cluster, gives every subsequent proposal a stable dependency,
and keeps each proposal self-contained and testable.

The first-node bootstrap is the simplest possible join scenario: no peers, no
data to transfer, no concurrency. It validates the `IDLE → READY` direct
transition and the epoch machinery against the ring layer before the seeded join
and gossip are introduced. The `JOINING` phase is reserved for the seeded-join
proposal where an incoming node must wait for partition data transfer before
serving traffic.

---

## CLI contract

### `tourillon node start` — first-node bootstrap

`tourillon node start` reads `config.toml`, acquires the exclusive process lock
(`pid.lock`), then drives the node lifecycle. The command accepts only these
phases at startup: `IDLE` and `READY`. Any other persisted phase is an error
and the process exits with code 1 before binding any socket.

#### Output conventions

`tourillon` is a daemon — it emits **zero terminal output** outside of the
Python `logging` subsystem. Every line the operator sees (progress, errors,
ready signal, shutdown) is a structured `logging` record routed through the
root logger. `setup_logging()` (`tourillon/bootstrap/log.py`) is the first call
in every Typer command and configures the root logger with a level-aware format:

```
# INFO and above (operator view — module only, no internal function names)
%(asctime)s %(levelname)-8s [%(name)s] %(message)s

# DEBUG (developer view — includes the emitting function for tracing)
%(asctime)s %(levelname)-8s [%(name)s:%(funcName)s] %(message)s
```

with `datefmt="%Y-%m-%dT%H:%M:%S"`. The log level defaults to `INFO` and can
be overridden via `--log-level DEBUG|INFO|WARNING|ERROR`.

`Console.print()` and `print()` are forbidden everywhere in `tourillon/`.

Log messages use **narrative, concise sentences** (e.g. `"Node 'node-1' is ready
(epoch 1, generation 1)."`) rather than machine-readable key-value tokens. This
makes the output immediately legible to an operator without special tooling.

#### Accepted phases and their startup behaviour

`data_dir` (the `[node].data_dir` value from `config.toml`) **must already
exist** when `tourillon node start` is called. The daemon never creates it. If
the directory is absent the process logs an error with the resolved absolute
path and exits with code 1 before acquiring any lock or binding any socket.

| Persisted phase | Startup action |
|-----------------|----------------|
| `IDLE` (no seeds) | First-node bootstrap: `IDLE → READY`. Both `TcpServer` sockets are bound after `READY` is persisted. |
| `READY` | Crash-recovery restart: skip transition, rebuild topology from persisted state, bind both sockets. |
| Any other | `logger.error` + exit 1 before binding any socket. |

#### No seeds — first-node bootstrap

```
$ tourillon node start --config config.toml
2026-05-10T14:23:45 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-1.
2026-05-10T14:23:45 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-1' starting from phase 'idle'.
2026-05-10T14:23:45 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-1' bootstrapped as first node: 1024 partitions, 4 vnode(s).
2026-05-10T14:23:45 INFO     [tourillon.infra.store.state] State persisted to disk (phase: ready, epoch: 1, generation: 1).
2026-05-10T14:23:45 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:23:45 INFO     [tourillon.core.transport.server] KV server listening on 0.0.0.0:7700.
2026-05-10T14:23:45 INFO     [tourillon.infra.cli.node] Node 'node-1' is ready (epoch 1, generation 1).
```

#### READY restart (crash-recovery)

```
$ tourillon node start --config config.toml
2026-05-10T14:23:45 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-1.
2026-05-10T14:23:45 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-1' starting from phase 'ready'.
2026-05-10T14:23:45 INFO     [tourillon.core.lifecycle.bootstrap] Topology rebuilt for node 'node-1': 4 vnode(s), epoch 1.
2026-05-10T14:23:45 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:23:45 INFO     [tourillon.core.transport.server] KV server listening on 0.0.0.0:7700.
2026-05-10T14:23:45 INFO     [tourillon.infra.cli.node] Node 'node-1' is ready (epoch 1, generation 1).
```

#### Graceful shutdown (Ctrl-C / SIGTERM)

```
2026-05-10T14:23:50 INFO     [tourillon.infra.cli.node] Shutdown signal received; stopping node 'node-1'.
2026-05-10T14:23:50 INFO     [tourillon.core.transport.server] Peer server stopped.
2026-05-10T14:23:50 INFO     [tourillon.core.transport.server] KV server stopped.
2026-05-10T14:23:50 INFO     [tourillon.infra.cli.node] Node 'node-1' stopped cleanly.
```

#### Seeds configured (out of scope for this proposal)

```
$ tourillon node start --config config.toml   # seeds set
2026-05-10T14:23:45 ERROR    [tourillon.infra.cli.node] Node 'node-1' has seeds configured; seeded join is not yet supported by this command.
```

Exit code 1. `setup_logging()` is called before config validation so every
error reaches the log handler.

> **Note — `tourctl` architecture:** `tourctl` is a pure client that always
> communicates with a running node over mTLS (KV or peer endpoint). It never
> accesses `state.toml` or `pid.lock` directly. The `tourctl node join` command
> — which will connect to the peer endpoint over mTLS and trigger the seeded
> join protocol — is **out of scope** for this proposal and will be defined in
> the seeded-join proposal.

> **Note — KV socket lifecycle:** Both the peer socket (ring / gossip traffic) and
> the KV socket (data-plane traffic) are bound **only when `phase == READY`** and
> closed when the node stops. Neither socket is opened during intermediate
> transitions. KV application handlers (`kv.put`, `kv.get`, `kv.delete`) are out
> of scope for this proposal and will be registered by the data-plane layer; until
> then the KV `Dispatcher` has no handlers and closes any incoming connection
> immediately.

---

## Design

### Hash space

`HashSpace(bits: int = 128)` defines the circular integer domain `[0, 2**bits)`.

- In production: `bits=128`, giving 2^128 ≈ 3.4 × 10^38 positions.
- In tests: `bits=8` (256 positions) preserves every structural invariant while
  making property-based tests tractable.

**Hash function:** MD5, used exclusively for its 128-bit deterministic output.
Not a cryptographic primitive — all security is provided by mTLS. Output is
truncated to `bits` significant bits for `bits < 128`.

**Circular arithmetic:** all position arithmetic is `mod 2**bits`. Intervals are
half-open `(a, b]` with wrap-around: when `a ≥ b`, the interval crosses the
zero boundary (`h > a or h ≤ b`).

`HashSpace` is never a global singleton. It is instantiated once at cluster
bootstrap with a fixed `bits` value and injected into every component. Mixing
instances with different `bits` values in the same cluster is a configuration
error caught at startup.

### Virtual nodes

```python
@dataclass(frozen=True)
class VNode:
    node_id: str
    token:   int   # ∈ [0, 2**bits)
```

A physical node contributes `node_size.token_count` vnodes to the ring
(`[node].size` in config: `XS`=1, `S`=2, `M`=4, `L`=8, `XL`=16, `XXL`=32;
fixed at cluster join time and immutable thereafter). `[node].size` expresses
intentional load capacity: a larger node owns more partitions and absorbs more
traffic proportionally, enabling mixed hardware tiers in the same cluster.

**Token selection:** tokens are chosen **randomly** at the start of the join
transition: `IDLE → READY` for the first-node path; `IDLE → JOINING` for the
seeded path (future proposal). They are persisted immediately and never
regenerated.

```
Before persisting the target phase:
  tokens = [secrets.randbelow(2**bits) for _ in range(token_count)]
  Persist tokens alongside the new phase in state.toml.
```

Tokens are generated exactly once, persisted immediately, and broadcast later
(seeded join proposal 007). Random placement gives a statistically uniform distribution
across the hash space with no correlation to node identity.

**Token uniqueness:** with `bits=128` the probability of any two tokens
colliding across an entire cluster is ~10⁻²⁸ — effectively impossible. No
collision-handling logic is specified or required. Tests use deterministic
tokens (not `secrets.randbelow`) to keep fixtures stable on the small
`bits=8` space.

### Ring

`Ring` is an **immutable sorted sequence** of `VNode` instances ordered by
ascending token. A coroutine holding a ring snapshot across an `await` can
never have it change underneath.

**Successor lookup — O(log n):**

```python
def successor(self, token: int) -> VNode:
    idx = bisect_right(self._vnodes, token, key=lambda v: v.token)
    if idx == len(self._vnodes):
        idx = 0   # wrap-around
    return self._vnodes[idx]
```

**Mutations return new Ring instances:**

```python
ring2 = ring.add_vnodes(new_vnodes)   # merge-sort, O(k log k + n)
ring3 = ring.drop_nodes({"node-3"})  # linear filter, O(n)
```

`ring.iter_from(vnode)` yields all vnodes in clockwise order from a given
position, wrapping at the end. Used by `PlacementStrategy` and rebalance.

### Partitioner and logical partitions

`Partitioner(hash_space, partition_shift)` imposes a **static, fixed-size grid**
of `2**partition_shift` logical partitions over the ring. This grid is
independent of physical nodes and never changes when topology changes.

```
total_partitions = 2 ** partition_shift
step             = hash_space.max >> partition_shift
pid              = h >> (bits - partition_shift)   # O(1)
```

`partition_shift` is fixed for the lifetime of the cluster. Choose
conservatively:

| Max node count | Min `partition_shift` | Total partitions |
|---------------:|---------------------:|-----------------:|
| 100            | 10                   | 1 024            |
| 1 000          | 14                   | 16 384           |
| 10 000         | 17                   | 131 072          |

For tests with `bits=8`, `partition_shift` must be strictly less than 8.
Enforced at construction time.

`LogicalPartition` is a contiguous half-open arc `(start, end]` of the circular
hash space. Its `pid` is a stable storage-key prefix that survives ring
mutations.

`PartitionPlacement` is an **ephemeral** binding between a `LogicalPartition`
and its current owner `VNode`. It is never persisted. After any ring mutation,
placements for affected partitions must be recomputed.

`Partitioner.placement_for_token(token, ring)` is the primary routing entry
point: O(1) partition lookup followed by O(log n) ring successor lookup.

### Placement strategy and preference list

```python
class PlacementStrategy(Protocol):
    async def preference_list(
        self,
        placement: PartitionPlacement,
        topology: Topology,
        probe_manager: "ProbeManager",
    ) -> list[PreferenceEntry]: ...
```

`SimplePreferenceStrategy(rf: int)` — default implementation:

1. Start from `placement.vnode`.
2. Walk clockwise via `topology.ring.iter_from`, deduplicating by `node_id`.
3. Stop when `rf` canonical positions are collected or the full ring is exhausted.
4. Record the last vnode token reached (`last_token`) — this is the start point for the handoff walk in step 6.
5. For each collected member, build a `PreferenceEntry` based on phase and probe state:
   - `READY`,    probe LIVE    → `PreferenceEntry(readable=True,  suspect=False, handoff=None)`
   - `READY`,    probe SUSPECT → `PreferenceEntry(readable=True,  suspect=True,  handoff=other.node_id)`
   - `DRAINING`, probe LIVE    → `PreferenceEntry(readable=True,  suspect=False, handoff=other.node_id)`
   - `DRAINING`, probe SUSPECT → `PreferenceEntry(readable=True,  suspect=True,  handoff=other.node_id)`
   - `PAUSED`,   probe LIVE    → `PreferenceEntry(readable=False, suspect=False, handoff=other.node_id)`
   - `PAUSED`,   probe SUSPECT → `PreferenceEntry(readable=False, suspect=True,  handoff=other.node_id)`
6. For each entry that requires a handoff, continue the clockwise walk from `last_token`, advancing `last_token` after each selection. Skip any `node_id` already used as a primary or handoff target. The walk is bounded to one full circle; if no eligible candidate remains, set `handoff=None`. The caller decides how to handle a `None` handoff (degrade, error, etc.) — this behaviour is defined in the seeded join proposal.

Every `node_id` — whether it appears as `PreferenceEntry.node_id` (primary
position) or as `PreferenceEntry.handoff` (temporary handoff target) — must
appear **at most once** across the entire preference list. A node that is
already a primary replica must not also be a handoff target, and vice versa.
This uniqueness invariant ensures that every write fan-out reaches a distinct
physical node, preserving the replication guarantee.


Phase eligibility:

| Phase | Readable (reads) | Handoff for writes | Reason |
|-------|:---:|:------------------:|--------|
| `IDLE` | No | No | No data; not participating. |
| `JOINING` | No | No | Incomplete snapshot; excluded from routing. |
| `READY` | Yes | When SUSPECT | Fully operational; hinted handoff reserved when the failure detector suspects this node. |
| `PAUSED` | No | Yes | Remains in ring to allow resynchronisation on resume; does not serve reads. |
| `DRAINING` | Yes | Yes | Counts toward reads; writes redirected to handoff target. |
| `FAILED` | No | No | Inert. |

The preference list is a **deterministic function** of `(placement, topology, probe_manager)`:
identical inputs always produce identical outputs. No wall-clock time is permitted
while constructing the list.

`SimplePreferenceStrategy` receives a `ProbeManager` instance and awaits its query
methods (`is_suspect`, `state_of`, etc.) while walking the ring. It must not
trigger probes; probes are started externally by the operation layer when a peer fails.

`ProbeManager` is a **domain class** that manages one `FailureDetector` instance per
actively observed peer. It is the single source of truth on local reachability for
the rest of the domain. Each `FailureDetector` tracks heartbeat inter-arrival times
for exactly one peer; `ProbeManager` maps `node_id → FailureDetector` and translates
`phi()` into a `MemberState` value. `MemberState` has three values: `LIVE`,
`SUSPECT`, `UNKNOWN`.

### Member registry

**`MemberRegistry`** is a pure key-value store (`node_id → Member`). It handles
insertion and lookup only. It carries no eviction policy and no gossip
knowledge. `snapshot()` returns a shallow copy that is safe to iterate outside
the `TopologyManager` lock. Tombstone eviction (removing `FAILED` entries) is
out of scope for this proposal and belongs to the gossip proposal.

### Topology

`Topology` is an **immutable** point-in-time snapshot of the cluster topology.
It is safe to hold across `await` points. Obtained via
`TopologyManager.snapshot()`.

`TopologyManager` is the stateful manager. It owns a `MemberRegistry` held
under a single `asyncio.Lock` so that ring and registry are always mutated
atomically. The active routing ring contains the
vnodes of `READY`, `DRAINING`, `PAUSED`, and `FAILED` nodes. A `JOINING` node's
vnodes are stored in the registry but not incorporated into the routing ring
until the node reaches `READY`. A `PAUSED` node's vnodes remain in the ring so
that preference-list construction can include the node and reserve a handoff
target for resynchronisation on resume. A `FAILED` node's vnodes remain in the
ring but are skipped during preference-list construction — they are neither
readable nor eligible as handoff targets.

### Topology epoch

The **topology epoch** is a monotonically increasing integer stored as
`[topology].epoch` in `state.toml`. It is incremented on those phase
transitions that change routing eligibility or modify the active routing ring.
This makes the epoch a compact version identifier for the topology used by
routing and placement.

| Transition | Epoch advances? | Ring changes? |
|-----------|:---------------:|:-------------:|
| `IDLE → READY` (first node, no seeds) |       Yes       | Yes |
| `IDLE → JOINING` (seeded join — future proposal) |       Yes       |      No       |
| `JOINING → READY` |       Yes       |      Yes      |
| `JOINING → FAILED` |       Yes       |      No       |
| `READY → PAUSED` |       Yes       |      No       |
| `READY → DRAINING` |       Yes       |      No       |
| `DRAINING → IDLE` |       Yes       |      Yes      |
| `DRAINING → FAILED` |       Yes       |      No       |

`state.toml` is the single source of truth for the node's persistent lifecycle
state. It is always written atomically via `StatePort.save()`. The canonical
TOML format and the two reference snapshots are defined in `### Node state`
above.

`tokens` is written once at the start of the join transition (`IDLE → READY`
for the first node; `IDLE → JOINING` for seeded join) and never changes for the
lifetime of this join. `seq` advances by one on each phase transition. `epoch`
tracks the topology version and advances as specified in the epoch table above.

### First-node bootstrap sequence

Precondition: no seeds in `[cluster].seeds` and no `--seed` flags. The
persisted phase is `IDLE`.

```
1. IDLE → READY (single atomic transition — no intermediate JOINING):
   - Generate tokens: [secrets.randbelow(2**bits) for _ in range(token_count)].
   - Increment generation (→ 1 for a fresh node), set seq = 0.
   - Assign all total_partitions partitions to this node.
     (Pure function of a single-node ring — no seed contact, no transfer.)
   - Build NodeState(phase=READY, generation=1, seq=0, tokens, epoch=1).
   - Call StatePort.save(state) — atomic write + fsync before any socket is
     opened. No peer socket, no KV socket may be opened before save() returns.

2. TopologyManager: incorporate this node's vnodes into the routing ring.

3. Bind the peer TcpServer on [node].peer_address.
4. Bind the KV TcpServer on [node].kv_address.
   KV handlers (kv.put / kv.get / kv.delete) are registered by the data-plane
   layer; they are out of scope for this proposal. The KV socket is bound here
   so that the lifecycle invariant (KV socket ↔ phase READY) is established
   from the very first bootstrap.
```

No seed contact. No partition data transfer. No rebalance.

The `JOINING` phase is entirely absent from the first-node path. It exists
exclusively for the seeded-join flow (future proposal) where an incoming node
must wait for an authoritative partition snapshot before serving traffic.

`Member.generation` is incremented **exactly once** here. Any subsequent
crash-restart reuses the persisted generation unchanged.

### READY restart (crash-recovery)

When the node restarts with a persisted phase of `READY`, the bootstrap
sequence is:

```
1. Load NodeState from state.toml — phase=READY, epoch and tokens intact.
2. Rebuild TopologyManager from the persisted state (single-node: self only).
3. Bind the peer TcpServer on [node].peer_address.
4. Bind the KV TcpServer on [node].kv_address.
```

No state transition is executed. `epoch` and `generation` are unchanged.
This is a pure READY → READY path: the node resumes exactly where it stopped.

### Error paths

**Node already in a non-IDLE / non-READY phase:** `tourillon node start` reads
the current phase from `state.toml`. If the phase is not `IDLE` and not `READY`
(e.g. `JOINING`, `DRAINING`, or `PAUSED` left over from a previous run), the
process logs an error and exits with code 1 before binding any socket. Recovery
from those phases is defined in their respective proposals.

**Seeds configured:** If `[cluster].seeds` is non-empty, the process exits with
code 1 and an explanatory message. Seeded join is out of scope for this
proposal.

### Crash recovery semantics

The authoritative restart state is always read from `state.toml`. The node
never infers its phase from network observations or in-memory state.

| Phase at crash | Tokens persisted? | Restart behaviour |
|---|:---:|---|
| `IDLE` | No | Restart cleanly as `IDLE`. No generation is incremented; the node runs first-node bootstrap on the next `tourillon node start`. |
| `IDLE → READY` (crash before `state.toml` written) | No | `state.toml` was never written (or still holds the initial `IDLE` state). Restart as `IDLE`; re-execute first-node bootstrap from the top. Generation may be re-incremented on the next start. |
| `READY` | Yes | Restart in `READY`. Rebuild the `TopologyManager` from persisted state and resume responding on the peer socket. No state transition is executed. |
| `JOINING` (seeded join — future proposal) | Yes | Restart in `JOINING`. Reuse persisted `generation` and `tokens` unchanged — never re-increment generation or regenerate tokens. Re-attempt the `JOINING → READY` transition. |
| `DRAINING` | Yes | Restart in `DRAINING`. Resume the drain protocol from where it was interrupted (future proposal). |
| `PAUSED` | Yes | Restart in `PAUSED`. Remain paused until `tourctl node resume` is issued (future proposal). |

The critical invariant is the **write-before-announce** rule: because
`StatePort.save()` completes (including fsync) before any gossip or topology
announcement is emitted, a crash between the write and the announcement is
safe — the node restarts with the correct phase and re-emits the announcement
on the next boot. A crash *before* `save()` leaves disk in its previous state,
so the node restarts one phase earlier and re-executes the transition from
scratch.

---

## Core invariants

- The ring is a **pure function** of the persisted registry. Two nodes with the
  same registry always derive the same ring, preference lists, and partition
  assignments.
- `partition_shift` is **immutable** after cluster bootstrap.
- `[node].size` is **immutable** after the node joins.
- **Vnode tokens are chosen randomly once** at the start of the join transition
  (`IDLE → READY` for the first node; `IDLE → JOINING` for seeded join) and then
  persisted. They are never regenerated on retry, crash-restart, or recovery.
- The **routing ring** incorporates a node's vnodes when that node reaches
  `READY`, and retains them as long as the node exists — regardless of whether
  it subsequently becomes `DRAINING`, `PAUSED`, or `FAILED`. Vnodes are removed
  only on `DRAINING → IDLE` (graceful leave). `JOINING` and `IDLE` nodes are
  never in the ring.
- Topology epoch **never regresses** and advances on every phase transition that
  changes routing eligibility or the active routing ring. `IDLE → JOINING` does
  not advance the epoch; `IDLE → READY` (first node) does.
- `TopologyManager` holds the ring and `MemberRegistry` under a single
  `asyncio.Lock`. They are always mutated atomically. Ring mutations are derived
  internally from the phase transition observed in `apply_member`; callers never
  touch the ring directly.
- `apply_member` returns `True` if the registry or ring was modified, `False`
  for no-ops.
- `PartitionPlacement` is **ephemeral**: never persisted; always recomputed
  after ring mutation.
- `Member.generation` is incremented **exactly once** per join lifecycle, at
  the start of the join transition. Never on crash-restart or retry.
- The node state file is **always written before any socket is opened** for any
  phase transition. The peer `TcpServer` is bound only after `StatePort.save()`
  returns successfully.
- **`data_dir` must be created by the operator** before starting the node. The
  daemon never creates it. If the directory is absent the process logs an error
  with the absolute path and exits with code 1 before acquiring any lock.
  returns successfully.

---

## Design decisions

**MD5 as the hash function.**
MD5 is chosen exclusively for its 128-bit deterministic output and universal
availability. It is not used as a security primitive — mTLS provides all
security guarantees. Its speed and fixed output width make it a straightforward
choice for key distribution. Output is truncated to `bits` significant bits for
test instances with `bits < 128`.


**Random token placement.**
Tokens are generated with `secrets.randbelow(2**bits)` at the start of the join
transition (`IDLE → READY` for the first node; `IDLE → JOINING` for seeded
join). Random placement achieves a statistically uniform distribution across the
hash space with no correlation to node identity, avoiding hot-spots and natural
clustering that deterministic schemes (e.g. based on node name hash) introduce.

**Immutable `Ring`.**
`add_vnodes` and `drop_nodes` return new `Ring` instances and never mutate the
receiver. This makes it safe for a coroutine to hold a ring reference across
`await` points: the ring it operates on cannot change underneath it, eliminating
an entire class of race conditions without requiring additional locks.

**Fixed, topology-independent partition grid.**
`Partitioner` imposes a static grid of `2**partition_shift` partitions whose
boundaries are fixed for the lifetime of the cluster. Partition IDs become
stable storage-key prefixes that survive ring mutations entirely. When a node
leaves and another takes over a partition, the underlying keys do not need to be
renamed; only ownership changes.

**`partition_shift < bits` enforced at construction.**
If `partition_shift == bits` every hash would map to a different partition for
each possible bit value, producing a meaningless grid. More practically, the
constraint prevents the degenerate case where all positions collapse. Validation
at construction time surfaces misconfiguration at startup rather than silently
producing incorrect routing.

**Epoch does not advance on `IDLE → JOINING`.**
A `JOINING` node does not participate in the active routing ring and serves no
traffic. Incrementing the epoch at this transition would cause peers that observe
the epoch to believe the active topology has changed and trigger unnecessary
reconfigurations. The epoch advances only when routing eligibility or the active
ring actually changes, keeping the epoch a meaningful compacted version of the
routing state. For the first-node path (`IDLE → READY`), the epoch advances to 1
immediately because the node enters the ring directly.

**`IDLE → READY` directly for the first-node path.**
A node joining a cluster for the first time via the seeded path must pause in
`JOINING` until it has received and applied a complete partition snapshot from
the donor node. The first-node path has no donor and no data to receive, so the
pause is unnecessary and the node can transition directly from `IDLE` to `READY`.
Keeping `JOINING` absent from the seedless path eliminates a transient state that
has no observable meaning, simplifies the bootstrap sequence, and makes the CLI
output reflect only states the operator needs to act on.

**`FailureDetector` and `ProbeManager` are domain classes, not infra ports.**
The phi-accrual algorithm, probe lifecycle rules, and threshold computation are
business logic — they determine how the cluster reasons about peer health and
influence routing decisions directly. Placing them in `core/` makes them
testable in isolation: tests call `record_heartbeat` and `record_miss` directly
without any real network calls.

**`FailureDetector` is node-id-agnostic.**
A `FailureDetector` instance models the heartbeat inter-arrival distribution for
exactly one peer and knows nothing about that peer's identity. This keeps the
statistical model pure and makes it trivially unit-testable. `ProbeManager` is
the only place where node identities are associated with detector instances; it
creates one `FailureDetector` per `node_id` on first observation and maps
`phi()` to a `MemberState` value.

**`MemberState` is distinct from `MemberPhase`.**
`MemberState` (`LIVE`, `SUSPECT`, `UNKNOWN`) is a private, per-observer
judgement derived locally from `FailureDetector.phi()` and is never gossiped.
`MemberPhase` is the node's own self-declared state broadcast via gossip. Keeping
them as separate types prevents callers from confusing local suspicion with
cluster-wide membership state.

**`TopologyManager` exposes only member-centric mutations.**
Exposing ring-level operations (`add_vnode`, `drop_from_ring`, …) would force
callers to know which ring mutation corresponds to each phase transition,
distributing that mapping logic across the codebase. `apply_member` and
`merge_registry` centralise it: callers hand in a `Member` record and the
manager derives the ring delta internally by diffing old vs. new phase.

**`MemberRegistry` is a pure key-value store.**
Mixing storage and eviction policy in the same type would embed gossip-engine
semantics inside a data structure, making it impossible to test the storage layer
in isolation. `MemberRegistry` is a pure `node_id → Member` store with no
knowledge of gossip rounds or eviction, keeping it trivially unit-testable.
Tombstone eviction belongs to the gossip proposal.

**`apply_member` returns `bool`.**
Returning `True` when the registry or ring changed allows the gossip engine to
decide in O(1) whether to queue a re-broadcast for this record, without taking a
second snapshot and comparing topologies. A `False` return means the record was
a no-op and can be silently dropped.

**Single `asyncio.Lock` in `TopologyManager`.**
Ring and registry must be mutated atomically. A dedicated lock per field would
allow a snapshot to observe a ring that already contains a new node's vnodes
while the registry still shows that node as `JOINING` — an inconsistent state
that would corrupt preference-list computation. A single lock eliminates this
class of inconsistency.

**`PartitionPlacement` is ephemeral.**
Persisting placements would introduce a second source of truth that could diverge
from the ring after a crash or an unclean shutdown. Always recomputing placement
from the current ring guarantees that the result is consistent with the persisted
registry, which is the single authoritative source.

**`Member.generation` increments exactly once per join lifecycle.**
Generation identifies a logical join attempt. If generation were incremented on
retry or crash-restart, peers that have already observed `generation=1` for a
node would receive a `generation=2` gossip record for what is logically the same
join, breaking convergence: they would treat it as a newer incarnation and accept
stale data as fresh. Reusing the persisted generation preserves the invariant
that a higher generation always means a newer join lifecycle. For the first-node
path, generation is incremented at the single `IDLE → READY` transition;
for seeded join it is incremented at `IDLE → JOINING`.

**`preference_list` is `async`.**
`ProbeManager` query methods are protected by an async lock because the
detector's per-peer state can be updated concurrently by the transport layer.
Making `preference_list` async allows the strategy to read the most current
`MemberState` atomically without requiring a separate snapshot step that could
introduce a TOCTOU gap.

**`StatePort` abstracts durable state I/O.**
The bootstrap sequence and phase-transition code call `StatePort.save()` rather
than writing TOML directly. This keeps all filesystem and encoding details inside
`FileStateAdapter` in `infra/`, preserving the hexagonal boundary: `core/` never
imports `tomllib`, `pathlib`, or `os`. Tests inject an in-memory `StatePort` stub
that records `save()` calls and returns canned `load()` results, making the full
bootstrap sequence testable without touching the filesystem.

**`FileStateAdapter` is the sole writer of `state.toml`.**
Designating a single component as the writer guarantees that writes are always
atomic (temp file + `os.replace()`) and eliminates the risk of concurrent writers
corrupting the file.

**`NodeState` is the single in-memory view of `state.toml`.**
Decoding the entire file into one frozen dataclass rather than reading individual
fields from a raw dict makes the state an explicit value that can be passed,
logged, tested, and compared without re-reading the file.

**`TopologyManager.snapshot()` acquires the lock, constructs an immutable
`Topology`, then releases.**
The returned `Topology` dataclass is frozen and captures both
`MemberRegistry.snapshot()` and the current `Ring` at the same logical instant.
Callers may hold it across `await` points without re-acquiring the lock. Any
subsequent mutation of the `TopologyManager` produces a new `Topology` and never
modifies the one already returned.

---

## Interfaces (informative)

```python
# tourillon/core/lifecycle/state.py
@dataclass(frozen=True)
class NodeState:
    """Durable snapshot of a node's lifecycle state, mapped to state.toml.

    Maps directly to the [node] and [topology] sections. Constructed
    exclusively by StatePort implementors and the bootstrap sequence; never
    built ad-hoc in domain code.

    tokens is empty for a fresh IDLE node and populated exactly once at the
    start of the join transition (IDLE → READY for the first node;
    IDLE → JOINING for seeded join). epoch is 0 before the first transition
    that advances it (IDLE → READY for the first node; JOINING → READY for
    seeded join).
    """

    phase:      MemberPhase
    generation: int
    seq:        int
    tokens:     tuple[int, ...]  # empty for IDLE; populated at join transition
    epoch:      int              # topology version from [topology].epoch


# tourillon/core/ports/state.py
class StateError(Exception):
    """Raised by StatePort implementations on any I/O or decode failure."""


class StatePort(Protocol):
    """Read and write the node's persistent lifecycle state.

    load() returns None when no state file exists (first boot, fresh IDLE
    node). save() is always atomic and durable: it writes to a temporary
    file in the same directory, calls os.replace(), then fsyncs the
    containing directory when the platform supports it. No partial writes
    are ever visible on disk.

    Callers must ensure save() completes before emitting any gossip or
    topology announcement (write-before-announce invariant).
    """

    async def load(self) -> NodeState | None:
        """Return the persisted NodeState, or None if no state file exists."""
        ...

    async def save(self, state: NodeState) -> None:
        """Atomically persist state to disk.

        Raise StateError on any I/O failure (permissions, filesystem full,
        decode error). Never raises on a clean write.
        """
        ...


# tourillon/infra/store/state.py
class FileStateAdapter:
    """StatePort implementation that reads/writes state.toml in data_dir.

    state.toml canonical format:

        [node]
        phase      = "joining"
        generation = 1
        seq        = 0
        tokens     = [14, 87, 142, 201]

        [topology]
        epoch = 0

    Writes are atomic: the new content is written to a sibling temp file,
    os.replace() is called, then the containing directory is fsynced when the
    platform supports it. This guarantees that a crash mid-write leaves either
    the old state or the new state visible on disk, never a partial file.

    Injected at startup by the bootstrap sequence. No other component may
    open or write state.toml directly.
    """

    def __init__(self, path: Path) -> None:
        """Initialise with the absolute path to state.toml."""
        ...

    async def load(self) -> NodeState | None:
        """Read and parse state.toml.

        Return None if the file does not exist. Raise StateError on any
        parse or I/O failure.
        """
        ...

    async def save(self, state: NodeState) -> None:
        """Write state atomically.

        Raise StateError on any I/O failure.
        """
        ...


# tourillon/core/lifecycle/member.py
class MemberPhase(StrEnum):
    IDLE     = "idle"
    JOINING  = "joining"
    READY    = "ready"
    PAUSED   = "paused"
    DRAINING = "draining"
    FAILED   = "failed"


@dataclass(frozen=True)
class Member:
    """Immutable gossip-exchange record describing a single cluster member.

    tokens holds the ordered sequence of hash-space positions claimed
    by this node. It is populated exactly once at the start of the join
    transition (IDLE → READY for the first node; IDLE → JOINING for seeded
    join) and never changes afterwards. An IDLE node that has not yet started
    carries an empty tuple.
    """

    node_id:      str
    peer_address: str
    generation:   int
    seq:          int
    phase:        MemberPhase
    tokens: tuple[int, ...]  # empty for IDLE; populated at join transition

    def supersedes(self, other: "Member") -> bool:
        """Return True when self is strictly newer than other.

        Precedence is the lexicographic pair (generation, seq). A self with
        a higher generation always supersedes regardless of seq. Within the
        same generation, higher seq wins.
        """
        return (self.generation, self.seq) > (other.generation, other.seq)


# tourillon/core/lifecycle/registry.py
class MemberRegistry:
    """Pure key-value store for Member records indexed by node_id.

    This class has exactly one responsibility: storing and retrieving Member
    values. It carries no gossip knowledge, no propagation counters, and no
    eviction policy. Tombstone eviction is out of scope for this proposal.

    None of the methods here are thread-safe. All callers must hold the
    TopologyManager lock before mutating the registry.
    """

    def upsert(self, member: Member) -> bool:
        """Insert or replace the entry for member.node_id if member supersedes current.

        Return True if the registry was modified, False if the incoming record
        is equal to or older than the one already stored.
        """
        ...

    def get(self, node_id: str) -> Member | None: ...

    def members_in_phase(self, *phases: MemberPhase) -> dict[str, Member]:
        """Return a node_id → Member mapping for all members in any of the given phases."""
        ...

    def snapshot(self) -> "MemberRegistry":
        """Return a shallow copy that is safe to read outside the lock.

        The copy captures the registry at this instant. Subsequent mutations
        of the original do not affect the copy.
        """
        ...

    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[Member]: ...


# tourillon/core/ring/hashspace.py
class HashSpace:
    def __init__(self, bits: int = 128) -> None: ...

    @property
    def bits(self) -> int: ...

    @property
    def max(self) -> int:
        """Return 2**bits — one past the last valid position."""
        ...

    def hash(self, value: bytes) -> int:
        """Return the MD5 hash of value truncated to bits significant bits."""
        ...


# tourillon/core/ring/vnode.py
@dataclass(frozen=True)
class VNode:
    node_id: str
    token:   int


# tourillon/core/ring/ring.py
class Ring:
    """Immutable sorted sequence of VNode instances.

    Mutations (add_vnodes, drop_nodes) return new Ring instances; the
    original is never modified. Safe to hold across await points.
    """

    @classmethod
    def empty(cls) -> "Ring":
        """Return an empty Ring."""
        ...

    def successor(self, token: int) -> VNode:
        """Return the first VNode clockwise at or after token.

        Wraps around to the first vnode when token exceeds the highest token
        in the ring.
        """
        idx = bisect_right(self._vnodes, token, key=lambda v: v.token)
        if idx == len(self._vnodes):
            idx = 0
        return self._vnodes[idx]

    def add_vnodes(self, vnodes: list[VNode]) -> "Ring": ...    # merge-sort, O(k log k + n)
    def drop_nodes(self, node_ids: set[str]) -> "Ring": ...    # linear filter, O(n)
    def iter_from(self, vnode: VNode) -> Iterator[VNode]: ...  # bisect_left to find start index
    def __len__(self) -> int: ...


# tourillon/core/ring/partitioner.py
class Partitioner:
    def __init__(self, hash_space: HashSpace, partition_shift: int) -> None:
        """Raise ValueError if partition_shift >= hash_space.bits."""
        ...

    @property
    def total_partitions(self) -> int: ...

    def pid_for_hash(self, h: int) -> int: ...
    def segment_for_pid(self, pid: int) -> "LogicalPartition": ...
    def placement_for_token(self, token: int, ring: Ring) -> "PartitionPlacement": ...


@dataclass(frozen=True)
class LogicalPartition:
    pid:   int
    start: int
    end:   int  # half-open (start, end]

    def contains(self, h: int) -> bool: ...


@dataclass(frozen=True)
class PartitionPlacement:
    partition: LogicalPartition
    vnode:     VNode

    @property
    def address(self) -> str:
        """Return str(pid) — stable storage-key prefix independent of the owner."""
        return str(self.partition.pid)


# tourillon/core/ring/placement.py
@dataclass(frozen=True)
class PreferenceEntry:
    node_id:  str
    readable: bool       # True if reads are permitted (READY or DRAINING)
    suspect:  bool       # True if the failure detector suspects this node
    handoff:  str | None # node_id to receive writes temporarily; None for healthy primaries


class PlacementStrategy(Protocol):
    async def preference_list(
        self,
        placement: PartitionPlacement,
        topology: "Topology",
        probe_manager: "ProbeManager",
    ) -> list[PreferenceEntry]: ...


class SimplePreferenceStrategy:
    def __init__(self, rf: int) -> None: ...


# tourillon/core/ring/topology.py
@dataclass(frozen=True)
class Topology:
    """Immutable point-in-time snapshot of the cluster topology.

    Safe to hold across await points. Obtained via TopologyManager.snapshot().
    The active ring contains the vnodes of READY, DRAINING, PAUSED, and FAILED
    nodes. JOINING and IDLE vnodes are absent from the ring.

    registry holds a shallow copy of MemberRegistry produced by
    MemberRegistry.snapshot() at the time of the TopologyManager lock
    acquisition. It is safe to iterate and query without holding any lock.
    Mutations on registry are not visible from any subsequently captured
    Topology; each snapshot is an independent value.
    """

    epoch:    int
    registry: MemberRegistry
    ring:     Ring

    def members_in_phase(self, *phases: MemberPhase) -> dict[str, Member]:
        """Return a node_id → Member mapping for members matching any of the given phases."""
        return self.registry.members_in_phase(*phases)

    @property
    def active_node_ids(self) -> frozenset[str]:
        """Return node_ids of READY, DRAINING, and PAUSED members."""
        return frozenset(
            m.node_id for m in self.registry
            if m.phase in (MemberPhase.READY, MemberPhase.DRAINING, MemberPhase.PAUSED)
        )


class TopologyManager:
    """Stateful manager for the active ring and MemberRegistry.

    All mutations go through apply_member or merge_registry. Both methods compare
    the incoming Member against the current registry entry using
    member.supersedes() and, when the record is newer, update the registry and
    derive the necessary ring mutation from the observed phase transition entirely
    internally. Callers never manipulate the ring directly.

    Ring mutation rules applied automatically by apply_member:
      IDLE → JOINING       : store vnodes in registry; ring unchanged; epoch unchanged.
      JOINING → READY      : ring.add_vnodes(member.vnodes); advance epoch.
      DRAINING → IDLE      : ring.drop_nodes({node_id}); advance epoch.
      Any → FAILED         : ring unchanged (vnodes retained); advance epoch.
      Any other eligibility change : advance epoch only.

    Snapshot contract: snapshot() acquires the lock, builds an immutable Topology
    from MemberRegistry.snapshot() and the current Ring and epoch, releases the
    lock, then returns. The returned Topology is safe to hold across await points
    without re-acquiring the lock.
    """

    async def snapshot(self) -> Topology: ...

    async def apply_member(self, member: Member) -> bool:
        """Apply a single gossip record to the registry and ring.

        Return True if the registry or ring was modified. Return False if the
        record is equal to or older than the current entry (idempotent no-op).
        """
        ...

    async def merge_registry(self, registry: MemberRegistry) -> None:
        """Merge an externally received MemberRegistry atomically.

        Equivalent to calling apply_member for every entry in registry, but
        performed under a single lock acquisition so that snapshots always
        reflect a fully merged state and never observe a partial merge.
        """
        ...



# tourillon/core/lifecycle/probe.py
class MemberState(StrEnum):
    """Local reachability classification for a peer, derived from its FailureDetector.

    This is a private, per-observer value and is never gossiped. It is
    distinct from MemberPhase, which is the node's own self-declared state
    broadcast via gossip.
    """

    LIVE    = "live"     # phi below threshold; heartbeats arriving normally
    SUSPECT = "suspect"  # phi above threshold; peer may be failing
    UNKNOWN = "unknown"  # no observations yet for this peer


class ProbeManager:
    """Manages one FailureDetector per actively observed peer.

    The ProbeManager is the single source of truth on local reachability.
    It maps each node_id to its own FailureDetector instance. Query methods
    (state_of, is_suspect, …) are awaited by PlacementStrategy while walking
    the ring. Mutation methods (record_heartbeat, record_miss) are called by
    the transport layer and must not be called from placement logic.

    A peer absent from the internal registry returns MemberState.UNKNOWN.
    """

    async def state_of(self, node_id: str) -> MemberState: ...
    async def is_suspect(self, node_id: str) -> bool: ...
    async def is_live(self, node_id: str) -> bool: ...
    async def is_unknown(self, node_id: str) -> bool: ...
    async def snapshot(self) -> dict[str, MemberState]: ...

    # Called by the transport layer — never from placement logic.
    async def record_heartbeat(self, node_id: str) -> None:
        """Record a successful heartbeat arrival for node_id.

        Forwards the arrival time to the corresponding FailureDetector,
        creating a new detector instance if this is the first observation.
        """
        ...

    async def record_miss(self, node_id: str) -> None:
        """Signal that an expected heartbeat or response was not received.

        Marks the peer as at least SUSPECT so that subsequent phi queries
        reflect the missing interval. Creates a detector if necessary.
        """
        ...


# tourillon/core/lifecycle/phi.py
class FailureDetector:
    """Phi-accrual failure detector (Hayashibara et al., 2004).

    A single FailureDetector instance monitors exactly one peer and knows
    nothing about the peer's identity — identity management is the sole
    responsibility of ProbeManager. The detector models the distribution of
    heartbeat inter-arrival intervals and computes a phi value that grows
    the longer the peer has been silent.

    record_heartbeat() must be called each time a heartbeat arrives from the
    monitored peer. phi() and is_available() may be called at any time.
    """

    def record_heartbeat(self) -> None:
        """Record an instantaneous heartbeat arrival.

        Updates the inter-arrival time distribution used to compute phi.
        """
        ...

    def phi(self) -> float:
        """Return the current suspicion level.

        Return 0.0 if no heartbeat has been recorded yet. The value grows
        unboundedly as the time since the last heartbeat increases relative
        to the historical inter-arrival distribution.
        """
        ...

    def is_available(self, threshold: float = 8.0) -> bool:
        """Return True when phi() < threshold.

        The default threshold of 8.0 corresponds to a false-positive
        probability of ~0.003 % under a normal inter-arrival distribution.
        """
        ...
```

---

## Proposed code organisation

```
tourillon/
  bootstrap/
    log.py          # setup_logging(), _LevelAwareFormatter — level-aware log format
  core/
    lifecycle/
      member.py       # MemberPhase StrEnum, Member dataclass
      state.py        # NodeState dataclass — in-memory view of state.toml
      registry.py     # MemberRegistry — pure node_id → Member store
      probe.py        # MemberState StrEnum + ProbeManager (one FailureDetector per peer)
      phi.py          # FailureDetector — phi-accrual (Hayashibara et al., 2004); node-id-agnostic
      bootstrap.py    # run_first_node_bootstrap() — IDLE → READY (no seeds) domain logic
    ports/
      state.py        # StatePort Protocol, StateError
    ring/
      __init__.py
      hashspace.py    # HashSpace
      vnode.py        # VNode dataclass
      ring.py         # Ring (immutable sorted sequence, bisect_right)
      partitioner.py  # Partitioner, LogicalPartition, PartitionPlacement
      placement.py    # PlacementStrategy Protocol, PreferenceEntry, SimplePreferenceStrategy
      topology.py     # Topology (frozen snapshot), TopologyManager (apply_member, merge_registry, snapshot)
  infra/
    store/
      state.py        # FileStateAdapter — StatePort impl; temp-file + os.replace() + fsync
    cli/
      main.py         # Root Typer app — assembles node, pki, and config sub-apps
      node.py         # tourillon node start command + async serve loop
      pki.py          # tourillon pki ca command
      config.py       # tourillon config generate and generate-context commands
```

No `tourctl` code is added in this proposal. `tourctl node join` will be defined
in the seeded-join proposal once the peer mTLS listener is in place.

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | `Ring(bits=8)`, 3 nodes 2 vnodes each | `add_vnodes(new_vnodes)` | Returns new `Ring`; original unchanged; result contains all former + new vnodes sorted ascending |
| 2 | `Ring(bits=8)`, 3 nodes | `drop_nodes({"node-3"})` | New ring without node-3 vnodes; original unchanged |
| 3 | `Partitioner(bits=8, ps=4)`, 3 independent ring instances with same state | `placement_for_token` for 100 tokens | All 3 return identical `PartitionPlacement` for every token |
| 4 | `bits=8` | `Partitioner(hs, 8)` | Raises `ValueError` (partition_shift must be < bits) |
| 5 | 5-node topology: A=READY, B=JOINING, C=READY, D=DRAINING, E=PAUSED; all reachable; `rf=3` | `preference_list` | A, C, D readable; E present in PL with `readable=False`; B absent |
| 6 | Same topology; probe marks C as SUSPECT | `preference_list` | A and D readable; one `PreferenceEntry(handoff="C")` emitted; C marked suspect; no node_id appears more than once across the full list; SUSPECT nodes excluded from handoff candidates |
| 7 | `TopologyManager`, node B JOINING | `apply_member(member_b_ready)` | Returns `True`; snapshot ring includes B's vnodes; `epoch` advanced by 1 |
| 8 | `TopologyManager`, node B READY | `apply_member(member_b_ready)` again (same record) | Returns `False`; snapshot unchanged |
| 9 | In-memory node, `IDLE`, no seeds | `run_first_node_bootstrap()` | Phase transitions directly to `READY` (no intermediate `JOINING`); `epoch==1`; `generation==1`; all partitions assigned; peer `TcpServer` started |
| 10 | In-memory node, `READY` | `run_first_node_bootstrap()` | Returns `NodeState(phase=READY)` unchanged; topology rebuilt from persisted state; `StatePort.save()` not called; generation and epoch unchanged |
| 11 | In-memory node, `JOINING` | `run_first_node_bootstrap()` | Raises `BootstrapError(exit_code=1)`; error "unexpected phase joining — cannot start from this state" |
| 12 | (Hypothesis) arbitrary `add_vnodes` + `drop_nodes` on `bits=8` | Any sequence | `ring.successor(t)` always returns a vnode present in the ring; `len(ring)` = total surviving vnodes |
| 13 | `MemberRegistry.snapshot()` | Mutate original registry after taking snapshot | Snapshot is unaffected |
| 14 | `MemberRegistry` with A=READY, B=DRAINING | `members_in_phase(READY, DRAINING)` | Returns `{"A": member_a, "B": member_b}`; JOINING/IDLE/FAILED members absent |
| 15 | `merge_registry` atomicity | `merge_registry(registry_with_5_new_members)` | A `snapshot()` taken immediately after contains all 5 new members; no intermediate snapshot can observe a partial merge |
| 16 | `FileStateAdapter`, no file on disk | `load()` | Returns `None` |
| 17 | `FileStateAdapter`, valid `state.toml` with `phase=ready` | `load()` | Returns `NodeState(phase=READY, generation=1, seq=1, tokens=(14,87), epoch=1)` |
| 18 | `FileStateAdapter` | `save(NodeState(...))` then `load()` | Round-trip: loaded state equals saved state |
| 19 | In-memory `StatePort` stub | `save()` called from bootstrap sequence | Stub records exactly **one** `save()` call: with `phase=READY`, `epoch=1`, `generation=1` |
| 20 | `FileStateAdapter`, simulate crash mid-write (temp file exists, `os.replace()` not called) | Open adapter fresh and call `load()` | Old state returned; temp file is ignored; no StateError |
| 21 | `PidLock`, `data_dir` does not exist | `acquire()` | Raises `PidLockError` |
| 22 | `PidLock`, `data_dir` exists | `acquire()` + `release()` | Lock file created and released; double `release()` is a no-op |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `uv run pytest -m ring` passes with zero failures.
- [ ] `FileStateAdapter` round-trip test passes (`save` → `load` returns identical `NodeState`).
- [ ] Bootstrap sequence uses `StatePort` exclusively; no direct file I/O in `core/`.
- [ ] `tourillon node start` (no seeds) → `READY` in one terminal session with a single `StatePort.save()` call; no `JOINING` state is ever persisted.
- [ ] `READY` restart path covered: node starts in `READY`, peer `TcpServer` is bound, no state transition is executed.
- [ ] `generation==1` after first-node bootstrap; calling `run_first_node_bootstrap()` a second time raises `BootstrapError` without mutating state.
- [ ] Property-based tests pass for arbitrary ring mutation sequences on `bits=8`.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Seeded join, gossip protocol, membership fetch, partition data transfer,
rebalance, retry/deadline model, `FAILED → JOINING` recovery, and
`tourctl node join` (requires a running peer mTLS listener — deferred to the
seeded-join proposal). All these belong to dedicated proposals.
