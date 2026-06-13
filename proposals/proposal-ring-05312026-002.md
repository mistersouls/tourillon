# Proposal: Ring & First-Node Bootstrap

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-ring-05312026-002.md     -->

**Author**: Tourillon Contributors <tourillon@example.com>
**Status:** Draft
**Date:** 2026-05-31
**Sequence:** 002

---

## Summary

This proposal specifies the complete ring data layer and first-node bootstrap path for
Tourillon. It covers the circular consistent-hash ring (`HashSpace`, `VNode`, `Ring`), the
static partition grid laid over that ring (`Partitioner`, `LogicalPartition`,
`PartitionPlacement`, `PartitionRange`), the replication-preference strategy
(`PlacementStrategy`, `SimplePreferenceStrategy`, `PreferenceEntry`), topology management
(`TopologyManager`, `Topology`), the full node-lifecycle FSM (`MemberPhase`, `Member`,
`MemberRegistry`, `NodeState`), local failure detection (`FailureDetector`, `MemberState`,
`ProbeManager`), the `StatePersistence` protocol, and the `FileStatePersistence`
adapter backed by `state.toml`.
It then assembles these pieces into the `tourillon node start` command, which executes the
`IDLE → READY` transition for a seedless first-node cluster bootstrap or recovers a node
that crashed while `READY`. `PartitionRange` (computed by `Partitioner.ranges_for()`) is
used in startup log lines to show which ring arcs the new node owns. No amendments to this
proposal are permitted; later proposals extend it only through new gossip paths and new
TOML sections.

---

## Motivation

Proposal 001 delivers identity, transport, and configuration parsing. Before any KV traffic
can flow, two further foundations must exist:

1. **Ring** — Every routing decision in Tourillon — which replicas own a key, what the
   preference list for a write looks like, how a rebalance plan is computed — derives from
   the consistent-hash ring. Without a formally specified ring there is no stable address
   space, and every downstream proposal would need to invent their own.

2. **First-node bootstrap** — A cluster cannot grow until its first node is operational.
   The `IDLE → READY` transition is the simplest possible membership change (no peers to
   coordinate with), yet it exercises every invariant that the gossip-based join path
   will later rely on: token generation, write-before-announce state persistence, `Member`
   record construction, and `TopologyManager` mutation. Specifying it here ensures those
   invariants are defined and tested before multi-node coordination is introduced.

---

## CLI contract

All commands print to stdout on success and to stderr on failure.
Exit code `0` = success; `1` = user or config error; `2` = internal error.

### Logging convention

`tourillon node start` is a daemon — it emits **zero terminal output** outside of the
Python `logging` subsystem. Every line the operator sees (progress, errors, ready
signal, shutdown) is a structured `logging` record routed through the root logger.
`setup_logging()` (`tourillon/bootstrap/deps.py`) is the **first call** in every
Typer command and configures the root logger with a level-aware format:

```
# INFO and above — operator view (module name only)
%(asctime)s %(levelname)-8s [%(name)s] %(message)s

# DEBUG — developer view (includes emitting function for tracing)
%(asctime)s %(levelname)-8s [%(name)s:%(funcName)s] %(message)s
```

`datefmt="%Y-%m-%dT%H:%M:%S"`. The log level defaults to `INFO` and can be
overridden via `--log-level DEBUG|INFO|WARNING|ERROR`.

`Console.print()` and `print()` are **forbidden** everywhere under `tourillon/`.
Log messages use narrative, concise sentences (e.g. `"Node 'node-1' is ready
(epoch 1, generation 1)."`) rather than machine-readable key-value tokens.

---

### `tourillon node start` — start the node as the first (seedless) node

```
$ tourillon node start [OPTIONS]

  Start this node as the sole member of a new cluster.
  If no state.toml exists the node executes IDLE → READY: generates tokens,
  persists state.toml, then begins listening. If state.toml records phase=ready
  the node performs a crash-recovery restart: topology is rebuilt from disk
  without re-generating tokens and without incrementing the epoch.

  In both cases the peer listener is bound immediately. The KV listener is
  bound only once the node reaches READY phase.

Options:
  --config PATH   Path to config.toml   [default: ./config.toml]
  --help          Show this message and exit.
```

**Happy path — fresh first-node bootstrap (stdout):**

```
Node node-1 starting (phase: idle).
Generated 4 token(s) for node size M.
Partition ranges owned (1024 total partitions):
  token 0xaf3c12b8… → pids [  0– 255]  (256 partitions)
  token 0x3e9d7fa1… → pids [256– 511]  (256 partitions)
  token 0x8ab21c44… → pids [512– 767]  (256 partitions)
  token 0xd1047e9c… → pids [768–1023]  (256 partitions)
State persisted (phase: ready, epoch: 1, generation: 1).
Node node-1 is READY.
Peer listener: 192.168.1.1:7001
KV  listener: 192.168.1.1:7000
```

The token hex values are the first 8 hex characters of the full integer (128-bit for
production rings) followed by `…`. Partition range rows are sorted by `start_pid`
ascending. The display is produced by `Partitioner.ranges_for(node_id, ring)`.

**Happy path — crash-recovery restart (stdout):**

```
Node node-1 starting (phase: ready).
Topology rebuilt from state.toml: 4 vnode(s), epoch 1.
Node node-1 is READY.
Peer listener: 192.168.1.1:7001
KV  listener: 192.168.1.1:7000
```

**Error — config file not found (stderr, exit 1):**

```
Error: config file not found: ./config.toml
```

**Error — state.toml belongs to a different node (stderr, exit 1):**

```
Error: node_id mismatch: config=node-1 state=node-2
This data_dir belongs to a different node. Check your config.toml
or point data_dir at the correct directory.
```

**Error — unexpected persisted phase (stderr, exit 1):**

```
Error: unexpected phase joining — cannot start from this state via 'node start'.
Use 'tourctl node join' to resume a join in progress.
```

**Error — token count does not match node size (stderr, exit 1):**

```
Error: token count mismatch: state has 2 token(s) but node size M requires 4.
The node was likely reconfigured after joining. Resolve manually or wipe data_dir.
```

---

## Design

### Data model

#### `HashSpace`

`HashSpace(bits: int = 128)` defines the circular integer domain `[0, 2**bits)`.
`bits` must be ≥ 1; smaller values (e.g. `bits=8`) are used in tests.
`hash(value: bytes) → int` computes the MD5 digest of `value` and right-shifts the
result to `bits` significant bits. MD5 is used exclusively for its deterministic
128-bit output width; mTLS provides all security. `HashSpace` is never a global
singleton; it is instantiated once at bootstrap with a fixed `bits` value and injected
into every component that needs it.

```
bits=128 → domain [0, 2**128)   production default
bits=8   → domain [0, 256)      test default
```

#### `VNode`

```python
@dataclass(frozen=True)
class VNode:
    node_id: str
    token: int  # ∈ [0, 2**bits)
```

A virtual-node token owned by a physical node. A physical node contributes
`NodeSize.token_count` VNode instances to the ring. Tokens are generated randomly once,
at the beginning of the join transition, and never change. Heterogeneous clusters are
fully supported: larger `NodeSize` values yield more tokens and therefore more ring
partitions.

#### `Ring`

An immutable, sorted sequence of `VNode` instances ordered by ascending token. All
mutation methods (`add_vnodes`, `drop_nodes`) return a new `Ring`; the original is
never modified. A coroutine holding a ring reference across an `await` point can never
observe the ring changing underneath it.

Key operations:

| Method | Complexity | Description |
|---|---|---|
| `successor(token)` | O(log n) | First VNode clockwise at or after `token`; wraps at ring end |
| `predecessor(token)` | O(log n) | First VNode counter-clockwise strictly before `token` |
| `add_vnodes(vnodes)` | O(n log n) | Return new ring with added vnodes |
| `drop_nodes(node_ids)` | O(n) | Return new ring without any vnode in `node_ids` |
| `iter_from(vnode)` | O(n) | Clockwise iterator starting at `vnode.token` |

Both `successor` and `predecessor` raise `ValueError` on an empty ring.

#### `LogicalPartition`

```python
@dataclass(frozen=True)
class LogicalPartition:
    pid: int
    start: int   # exclusive lower bound
    end: int     # inclusive upper bound — half-open arc (start, end]
```

A contiguous half-open arc `(start, end]` of the circular hash space. `pid` is a
stable, integer storage-key prefix that is invariant under ring mutations; only
ownership changes, not key names. The arc wraps around the zero boundary when
`start >= end`.

`contains(h: int) → bool` handles the wrap-around case:

```
if start < end:  return start < h <= end
else:            return h > start or h <= end
```

#### `PartitionPlacement`

```python
@dataclass(frozen=True)
class PartitionPlacement:
    segment: int
    partition: LogicalPartition
    vnode: VNode
```

An ephemeral binding between a `LogicalPartition` and its current owner VNode, derived
from a ring snapshot. Never persisted. Always recomputed after any ring mutation.
`segment` is the coarse-grained segment id computed by `Partitioner.segment_for(pid)`.

#### `PartitionRange`

```python
@dataclass(frozen=True)
class PartitionRange:
    owner: VNode
    start_pid: int   # inclusive
    end_pid: int     # inclusive
    count: int

    @property
    def wraps(self) -> bool: ...
```

A contiguous run of partition IDs all owned by the same VNode, as returned by
`Partitioner.ranges_for(node_id, ring)`. Used exclusively as a **display and
payload-compaction primitive** — never for routing or storage decisions.

`wraps` is `True` when `start_pid > end_pid` (the range crosses the zero boundary).

Startup log output uses `PartitionRange` to show which arcs the newly-READY node owns
(see CLI contract above). Later proposals reuse `PartitionRange` for inspect output and
transfer planning.

#### `Partitioner`

`Partitioner(hash_space, partition_shift, segment_shift)` imposes a static grid of
`2**partition_shift` logical partitions over the ring. The grid is fixed for the
lifetime of the cluster.

Partitions are grouped into `2**segment_shift` coarser segments, each covering
`2**(partition_shift - segment_shift)` contiguous partitions. Segments are the unit
at which `BackendStorage` instances are opened.

Invariants enforced at construction time:
- `partition_shift < hash_space.bits`
- `segment_shift < partition_shift`

Key methods:

| Method | Complexity | Description |
|---|---|---|
| `pid_for_hash(h)` | O(1) | `h >> (bits - partition_shift)` |
| `segment_for(pid)` | O(1) | `pid >> (partition_shift - segment_shift)` |
| `partition_for(pid)` | O(1) | `LogicalPartition` arc for `pid` |
| `placement_for_token(token, ring)` | O(log n) | `PartitionPlacement` for a hash token |
| `ranges_for(node_id, ring)` | O(n log n) | `list[PartitionRange]` for `node_id` on `ring` |

`ranges_for` iterates every vnode in the ring (ascending token order), finds each vnode
belonging to `node_id`, computes the predecessor's token, and maps both bounds to pids.
The result is in ascending `start_pid` order because `pid_for_hash` is monotone with
ascending token.

`TourillonConfig` exposes `segment_shift` as a derived immutable property from
`node_size` (power-of-two token count). Startup validation still enforces
`segment_shift < partition_shift < hash_space.bits`.

#### `MemberPhase`

```python
class MemberPhase(StrEnum):
    IDLE     = "idle"
    JOINING  = "joining"
    READY    = "ready"
    PAUSED   = "paused"
    DRAINING = "draining"
    FAILED   = "failed"
```

Legal phase transitions in scope for this proposal:

```
IDLE → READY        first-node bootstrap (this proposal)
READY → READY       crash-recovery restart (this proposal, no topology change)
```

All other transitions are defined by later proposals. `MemberPhase` is the single source of truth for every phase check in the
codebase.

#### `Member`

```python
@dataclass(frozen=True)
class Member:
    node_id: str
    peer_address: str
    generation: int
    seq: int
    phase: MemberPhase
    tokens: tuple[int, ...]      # empty for IDLE; populated at join transition
    partition_shift: int         # cluster-wide constant; mismatch is a fatal error
```

An immutable gossip-exchange record. `supersedes(other: Member) → bool` compares on the
lexicographic pair `(generation, seq)`: higher generation always wins regardless of seq;
within the same generation higher seq wins. This comparator is the canonical merge
function used by both `MemberRegistry.upsert()` and the gossip delta handler.

`tokens` is populated exactly once — at the start of the first join transition — and
never changes afterwards. An IDLE node that has not yet started carries an empty tuple.

`partition_shift` is echoed into every `Member` record so that any peer receiving a
gossip record with a mismatched `partition_shift` can detect the configuration error
immediately.

#### `MemberRegistry`

A pure `dict[str, Member]` store with no gossip knowledge, no propagation counters, and
no eviction policy. All methods are intentionally not thread-safe: callers must hold the
`TopologyManager` lock before mutating.

| Method | Description |
|---|---|
| `upsert(member)` | Insert or replace if `member.supersedes(current)`; return `True` if modified |
| `get(node_id)` | Return `Member` or `None` |
| `members_in_phase(*phases)` | `dict[str, Member]` filtered by phase |
| `snapshot()` | Shallow copy safe to read outside the lock |

#### `NodeState`

```python
@dataclass(frozen=True)
class NodeState:
    node_id: str
    phase: MemberPhase
    generation: int
    seq: int
    tokens: tuple[int, ...]
    epoch: int
    committed_pids: tuple[int, ...] = ()
    staging_pids: tuple[int, ...] = ()
```

The durable snapshot of a node's lifecycle state, mapped directly to `state.toml`.
`committed_pids` and `staging_pids` track partition ownership during rebalance; in this
proposal they are always empty tuples.

#### `state.toml` format

`state.toml` lives in `TourillonConfig.data_dir`. Its canonical on-disk format is:

```toml
[node]
node_id    = "node-1"
phase      = "ready"
generation = 1
seq        = 0
tokens     = [14929832, 876342918, 2345871022, 3987612345]

[topology]
epoch = 1

[rebalance]
committed_pids = []
staging_pids   = []
```

Fields:

| Section | Key | Type | Notes |
|---|---|---|---|
| `[node]` | `node_id` | string | Must match `TourillonConfig.node_id` on restart |
| `[node]` | `phase` | string | `MemberPhase` value |
| `[node]` | `generation` | int | Monotonically increasing; increments on every new join |
| `[node]` | `seq` | int | Gossip sequence counter for this generation |
| `[node]` | `tokens` | int[] | Hash-space positions; populated at join; empty for IDLE |
| `[topology]` | `epoch` | int | Topology version; `0` before first transition |
| `[rebalance]` | `committed_pids` | int[] | PIDs fully committed during rebalance |
| `[rebalance]` | `staging_pids` | int[] | PIDs with in-progress staging |

The `[rebalance]` section is always written even when both lists are empty, so that
`_parse_state` never encounters a missing key.

#### `StatePersistence`

```python
class StatePersistence(Protocol):
    async def load(self) -> NodeState | None: ...
    async def save(self, state: NodeState) -> None: ...
```

`load()` returns `None` when no `state.toml` exists (first boot). `save()` is always
atomic and durable (write to temp file → `os.replace()` → fsync containing directory).
The **write-before-announce** invariant: `save()` must complete before any socket is
opened or any gossip record is emitted.

`StateError(Exception)` is raised by any `StatePersistence` implementor on I/O or
decode failure. `StateError` is defined in `tourillon/core/exceptions.py`.

#### `FileStatePersistence`

`FileStatePersistence(path: Path, config_rw: ConfigReadWriter)` implements
`StatePersistence` for `state.toml` on local disk.

- All `load()` and `save()` calls are serialised through a per-instance `asyncio.Lock`
  (`_io_lock`) to prevent Windows `FILE_SHARE_DELETE` races between a concurrent
  thread-pool read (holding `state.toml` open) and an `os.replace()` rename in `save()`.
- `save()` dispatches to `asyncio.to_thread(_save_sync)` for the blocking I/O.
- `_save_sync` encodes `NodeState` and writes through `ConfigReadWriter.write`.
- `_load_sync` reads through `ConfigReadWriter.read`; a missing file returns `None`;
  parse or I/O errors are wrapped as `StateError`.

Higher-level callers that need an atomic load-modify-save cycle (e.g. the rebalance
applicator) must additionally hold their own lock between `load()` and
`save()`; `_io_lock` is only for I/O serialisation.

#### `MemberState` and `FailureDetector`

`FailureDetector` monitors exactly one peer using the phi-accrual algorithm
(Hayashibara et al., 2004):

- `record_heartbeat()` records the wall-clock arrival time. The first call establishes
  the baseline (`_last_arrival`); no inter-arrival interval is recorded on the first
  call.
- `phi() → float` returns `elapsed / (mean * log(10))` where `elapsed` is time since
  the last arrival and `mean` is the arithmetic mean of up to 1000 stored intervals.
  Returns `0.0` when fewer than one interval has been recorded.
- `is_available(threshold=8.0) → bool` returns `phi() < threshold`. The default
  threshold of 8.0 corresponds to a false-positive probability of ≈ 0.003% under a
  normal inter-arrival distribution.
- `has_observations: bool` — `True` once the first heartbeat has been recorded.

`MemberState(StrEnum)` classifies a peer from the local observer's perspective:

```python
class MemberState(StrEnum):
    LIVE    = "live"     # phi < threshold; heartbeats arriving normally
    SUSPECT = "suspect"  # phi >= threshold; peer may be failing
    UNKNOWN = "unknown"  # no observations yet
```

`MemberState` is private and never gossiped. It is distinct from `MemberPhase`, which
is the node's own self-declared state broadcast via gossip.

#### `ProbeManager`

`ProbeManager` manages one `FailureDetector` per actively observed peer, protected
by a single `asyncio.Lock`. Absent nodes return `MemberState.UNKNOWN`.

| Method | Description |
|---|---|
| `state_of(node_id)` | Return `MemberState` |
| `is_suspect(node_id)` | `True` if `SUSPECT` |
| `is_live(node_id)` | `True` if `LIVE` |
| `is_unknown(node_id)` | `True` if `UNKNOWN` |
| `record_heartbeat(node_id)` | Forward heartbeat to detector; create if absent |
| `record_miss(node_id)` | Create detector without recording an interval |
| `phi_of(node_id)` | Return `Ï†` or `0.0` |
| `snapshot()` | `dict[str, MemberState]` snapshot |
| `all_states_with_phi()` | `list[tuple[str, MemberState, float]]` |

Note: `ProbeManager` in this proposal has a single `FailureDetector` per peer (gossip
heartbeats only). The API is intentionally minimal; support for data-plane failure
detection will be added when the data plane is introduced.

#### `Topology`

```python
@dataclass(frozen=True)
class Topology:
    epoch: int
    registry: MemberRegistry   # shallow copy at snapshot time
    ring: Ring
```

An immutable point-in-time snapshot of cluster topology, safe to hold across `await`
points. Obtained via `TopologyManager.snapshot()`. The active ring contains the vnodes
of `READY`, `DRAINING`, `PAUSED`, and `FAILED` nodes; `JOINING` and `IDLE` vnodes are
absent.

`active_node_ids → frozenset[str]` — node_ids of `READY`, `DRAINING`, and `PAUSED`
members.

`members_in_phase(*phases) → dict[str, Member]` — delegates to `registry.members_in_phase`.

#### `TopologyManager`

Stateful manager for the active ring and `MemberRegistry`. All mutations go through
`apply_member` or `merge_registry`; callers never manipulate the ring directly.

**Ring mutation rules** (applied automatically by `_apply`):

| Transition | Ring change | Epoch |
|---|---|---|
| `IDLE/JOINING → READY` | `ring.add_vnodes(member.tokens)` | `+1` |
| `DRAINING → IDLE` | `ring.drop_nodes({member.node_id})` | `+1` |
| `IDLE → JOINING` | none | unchanged |
| any other accepted mutation | none | `+1` |

`snapshot()` acquires the lock, builds an immutable `Topology` from
`MemberRegistry.snapshot()` and the current `Ring`, releases the lock, and returns
the frozen `Topology`.

`member_fingerprint() → str` — SHA-256 of the sorted concatenation of per-member
tuples `(uint32_be(len(node_id_utf8)) || node_id_utf8 || uint64_be(generation) ||
uint64_be(seq))`. The 4-byte length prefix for `node_id` prevents hash collisions
between node IDs of different lengths that would otherwise produce identical byte
sequences (e.g. `"node-1x"` + short counters vs `"node-1"` + shifted counters).
Tuples are sorted lexicographically by `node_id` before concatenation. The fingerprint
is computed lazily and cached; the cache is invalidated on every accepted mutation.
`epoch` is intentionally excluded from the fingerprint so ring-only mutations
(e.g. DRAINING→IDLE) do not trigger spurious full-member syncs in the gossip engine.

`merge_registry(members)` performs all upserts under a single lock acquisition so that
a `snapshot()` concurrent with a batch merge never observes a partial state.

#### `PlacementStrategy` and `SimplePreferenceStrategy`

```python
class PlacementStrategy(Protocol):
    async def preference_list(
        self,
        placement: PartitionPlacement,
        topology: Topology,
        probe_manager: ProbeManager,
    ) -> list[PreferenceEntry]: ...
```

`PreferenceEntry` describes one replica slot in the replication preference list:

```python
@dataclass(frozen=True)
class PreferenceEntry:
    node_id: str
    readable: bool          # True when phase ∈ {READY, DRAINING}
    suspect: bool           # True when ProbeManager suspects this node
    handoff: str | None     # handoff target node_id; None for healthy replicas
```

`SimplePreferenceStrategy(rf: int)` is the default implementation. It walks the ring
clockwise from `placement.vnode`, collecting up to `rf` unique physical nodes that are
not `IDLE`, `JOINING`, or `FAILED` (`_EXCLUDED_PHASES`). For each collected node it
determines:

- `readable`: phase ∈ `{READY, DRAINING}`
- `needs_handoff`: phase ∈ `_ALWAYS_HANDOFF_PHASES = {DRAINING, PAUSED}` **or**
  (phase == `READY` and `probe_manager.is_suspect(node_id)`)
- `handoff`: first node from a second clockwise walk starting after the last primary,
  skipping `_EXCLUDED_PHASES` and any already-used node_id (primary or prior handoff),
  skipping nodes where `probe_manager.is_suspect` is `True`

Every `node_id` across the entire preference list appears **at most once** — a node
already listed as a primary is never also listed as a handoff target.

#### Startup integrity checks

`check_node_id_consistency(config_node_id, state_node_id)`:
- Raises `NodeIdMismatchError` if the two values differ. The daemon must exit with
  code 1 immediately.

`check_tokens_coherence(phase, tokens, node_size) → bool`:
- Returns `False` (caller must transition to `FAILED`) when
  `phase ∈ {JOINING, READY, DRAINING}` and `len(tokens) != node_size.token_count`.
- Always returns `True` for `IDLE`, `PAUSED`, and `FAILED` (exempt).

### Core invariants

1. **Write-before-announce.** `state_port.save(state)` must complete successfully
   before `topology_mgr.apply_member(member)` is called and before any TCP socket
   is bound. A crash after save but before the socket opens leaves the node in a
   safely-restartable READY state.

2. **Immutable tokens.** A node's `tokens` tuple is generated once, at the beginning
   of its first join transition, and never changes. The same tuple is written to
   `state.toml` and to the `Member` gossip record.

3. **Immutable `partition_shift` and `segment_shift`.** Both are cluster-wide constants
   fixed at `tourillon config generate` time. Changing either requires a full node
   decommission. The startup check compares the value in `config.toml` against any
   received gossip record and aborts on mismatch.

4. **Ring immutability.** `Ring`, `LogicalPartition`, `PartitionPlacement`, and
   `PartitionRange` are all frozen dataclasses or produce new instances on mutation.
   No coroutine can observe a ring changing under it across an `await` point.

5. **`TopologyManager` as the single ring mutator.** No component other than
   `TopologyManager._apply` may call `ring.add_vnodes` or `ring.drop_nodes`. The ring
   is a private attribute of `TopologyManager`.

6. **`partition_shift` in every `Member` record.** A receiving node that observes a
   `partition_shift` mismatch in a gossip record must reject the record and, if the
   source is the local node, transition to `FAILED`.

7. **Segment-shift constraint.** `segment_shift < partition_shift < hash_space.bits` is
   enforced by `Partitioner.__init__` and by `load_config`. Violating either raises
   `ConfigError` before any socket is opened.

8. **`_io_lock` serialises FileStatePersistence I/O.** No concurrent `load()` and `save()`
   calls can interleave on Windows (no `FILE_SHARE_DELETE` races).

### Sequence / flow — `tourillon node start` (fresh bootstrap)

```
1. `NodeConfigService.load_config(config_path)` → `TourillonConfig` (from proposal 001)
2. Build HashSpace(bits=128).
3. Validate segment_shift < partition_shift < 128 → ConfigError on violation.
4. Construct `FileStatePersistence(data_dir / "state.toml", config_rw)`.
5. persisted = await state_port.load()
   a. If persisted is not None:
      - check_node_id_consistency(cfg.node_id, persisted.node_id) → NodeIdMismatchError on mismatch; exit 1.
      - check_tokens_coherence(persisted.phase, persisted.tokens, cfg.node_size) → if False, log error; exit 1.
6. Dispatch to the first-node bootstrap path in `NodeStarter`.

--- INSIDE NodeStarter first-node bootstrap (phase == IDLE path) ---
7.  tokens = tuple(secrets.randbelow(hash_space.max) for _ in range(cfg.node_size.token_count))
8.  state = NodeState(node_id=cfg.node_id, phase=READY, generation=1, seq=0,
                     tokens=tokens, epoch=1)
9.  await state_port.save(state)          # write-before-announce
10. Construct Partitioner(hash_space, cfg.partition_shift, cfg.segment_shift).
11. member = Member(node_id, peer_address, generation=1, seq=0, phase=READY,
                   tokens=tokens, partition_shift=cfg.partition_shift)
12. await topology_mgr.apply_member(member)   # adds vnodes to ring; epoch → 1
--- END ---

13. Compute ranges = partitioner.ranges_for(cfg.node_id, ring)
14. Print startup log lines (see CLI contract).
15. Build ssl_ctx_peer and ssl_ctx_kv from TlsConfig (see proposal 001).
16. Register all peer-plane handlers on Dispatcher.
17. Start TcpServer("peer", ssl_ctx_peer) on `servers.peer.bind` → peer listener up.
18. Start TcpServer("kv", ssl_ctx_kv) on `servers.kv.bind` → KV listener up (READY).
19. Run event loop until SIGINT/SIGTERM.
20. On shutdown: stop KV server; stop peer server; close listeners.
```

### Sequence / flow — `tourillon node start` (crash-recovery restart)

```
Steps 1–6 identical to above; dispatch to NodeStarter crash-recovery path with phase == READY.

--- INSIDE NodeStarter crash-recovery bootstrap (phase == READY path) ---
7. No new tokens generated; no state written to disk.
8. member = Member(node_id, peer_address, generation=state.generation,
                  seq=state.seq, phase=READY, tokens=state.tokens,
                  partition_shift=cfg.partition_shift)
9. await topology_mgr.apply_member(member)   # adds vnodes to ring; epoch unchanged
--- END ---

10. Continue from step 13 (ranges, listeners, loop).
```

### Error paths

| Scenario | Behaviour |
|---|---|
| `--config` file absent | `ConfigError` → stderr "Error: config file not found: …"; exit 1 |
| `data_dir` not writable | `StateError` on `save()` → stderr "Error: Cannot write state.toml: …"; exit 1 |
| `state.toml` parse error | `StateError` on `load()` → stderr "Error: Malformed state.toml: …"; exit 1 |
| `node_id` mismatch between config and state | `NodeIdMismatchError` → stderr message (see CLI contract); exit 1 |
| Token count mismatch at restart | `check_tokens_coherence` returns `False` → stderr message; exit 1 |
| Persisted phase ∈ {JOINING, DRAINING, PAUSED, FAILED} | `BootstrapError(exit_code=1)` → stderr message; exit 1 |
| `partition_shift >= hash_space.bits` | `ConfigError` → stderr; exit 1 |
| `segment_shift >= partition_shift` | `ConfigError` → stderr; exit 1 |
| Peer port already in use | `OSError` on `TcpServer.start()` → stderr "Error: cannot bind peer listener on …"; exit 1 |
| KV port already in use | `OSError` on `TcpServer.start()` → stderr "Error: cannot bind KV listener on …"; exit 1 |

---

## Design decisions

### Decision: `PartitionRange` is display-only; routing uses individual pids

**Alternatives considered:**
- Route writes to a range address rather than individual partition ids.
- Use `PartitionRange` as the storage key prefix.

**Chosen because:** Contiguous ranges are an emergent property of the ring at a given
moment and can change shape every time a node joins or leaves. Using individual `pid`
values as the stable storage-key prefix means no renaming is ever required on topology
change. `PartitionRange` is a compact display primitive that groups consecutive pids for
human consumption and wire-payload compaction, but never appears in the
routing or storage path.

### Decision: Immutable `Ring` with copy-on-write mutations

**Alternatives considered:**
- Mutable ring protected by a lock.
- Persistent data structure (e.g. balanced BST with structural sharing).

**Chosen because:** The ring changes infrequently (only on join/leave). Copy-on-write is
O(n log n) per mutation but O(1) for the common case of a stable cluster. Because a ring
snapshot is immutable, any coroutine can hold it across `await` points without risk of
observing a mid-mutation state. The sorted-list representation has excellent cache
locality for the O(log n) `bisect_right` lookups that dominate placement and ring-walk
operations.

### Decision: `HashSpace` injects `bits` instead of a global constant

**Alternatives considered:**
- Hard-code `bits=128` everywhere.

**Chosen because:** Tests with `bits=128` generate tokens in [0, 2^128), making
property-based and regression tests computationally expensive. Injecting `HashSpace` as a
dependency and using `bits=8` in tests preserves every structural invariant while keeping
tests tractable. Mixing instances with different `bits` values in a real cluster is a
fatal configuration error caught at startup.

### Decision: Single phi-accrual `FailureDetector` per peer (this proposal)

**Alternatives considered:**
- Implement the dual-detector model (gossip FD + data-plane circuit breaker) immediately.

**Chosen because:** Introducing a `DataCircuitBreaker` before the data plane
exists would require forward references and premature abstraction. The single-detector
`ProbeManager` API is intentionally a strict subset of the eventual dual-detector API
so that adding a second detector later will not break existing call sites.

### Decision: Write-before-announce enforced in first-node bootstrap orchestration

**Alternatives considered:**
- Apply the `TopologyManager` mutation first, then persist state.

**Chosen because:** If the process crashes after `apply_member` but before `save()`, the
topology manager has a `READY` member but the node's disk state is still `IDLE`. On
restart, the node would re-enter the bootstrap path and generate different tokens,
creating a split-brain situation. Writing to disk first means a crash at any point leaves
the node in a consistent state that can be fully recovered.

### Decision: `segment_shift` exposed as a derived `TourillonConfig` property

**Alternatives considered:**
- Keep `segment_shift` hidden inside `Partitioner`.
- Add `segment_shift` as a user-set TOML field.

**Chosen because:** the current repository model already treats node sizing as the
single operator knob (`node_size`). Deriving `segment_shift` from that immutable size
keeps one source of truth, avoids an extra persisted setting, and still guarantees
`segment_shift < partition_shift` via startup validation.

### Decision: `state.toml` always includes `[rebalance]` section

**Alternatives considered:**
- Omit `[rebalance]` when both lists are empty.

**Chosen because:** A fixed schema makes `_parse_state` simpler and avoids key-absence
checks. Both `committed_pids` and `staging_pids` default to empty lists in `_parse_state`
if the section is absent for backward compatibility, but `_encode_state` always writes
the section explicitly so that the on-disk format is predictable.

---

## Interfaces (informative)

Repository alignment for this proposal follows the current package layout:
`core/structure/*` for domain records and pure ring logic,
`core/services/*` for orchestration/managers,
`core/machinery/*` for state persistence adapters,
`bootstrap/*` for CLI wiring.

### `tourillon/core/structure/ring.py` *(new)*

```python
class HashSpace: ...

@dataclass(frozen=True)
class VNode: ...

class Ring: ...
```

### `tourillon/core/structure/partition.py` *(new)*

```python
@dataclass(frozen=True)
class LogicalPartition: ...

@dataclass(frozen=True)
class PartitionPlacement: ...

@dataclass(frozen=True)
class PartitionRange: ...

class Partitioner: ...
```

### `tourillon/core/services/topology.py` *(new)*

```python
class MemberRegistry: ...

@dataclass(frozen=True)
class Topology: ...

class TopologyManager: ...
```

### `tourillon/core/services/placement.py` *(new)*

```python
@dataclass(frozen=True)
class PreferenceEntry: ...

class PlacementStrategy(Protocol): ...

class SimplePreferenceStrategy: ...
```

### `tourillon/core/structure/member.py`

```python
class MemberPhase(StrEnum):
    IDLE     = "idle"
    JOINING  = "joining"
    READY    = "ready"
    PAUSED   = "paused"
    DRAINING = "draining"
    FAILED   = "failed"

@dataclass(frozen=True)
class Member:
    node_id: str
    peer_address: str
    generation: int
    seq: int
    phase: MemberPhase
    tokens: tuple[int, ...]
    partition_shift: int
    def supersedes(self, other: Member) -> bool: ...

@dataclass(frozen=True)
class NodeState:
    node_id: str
    phase: MemberPhase
    generation: int
    seq: int
    tokens: tuple[int, ...]
    epoch: int
    committed_pids: tuple[int, ...] = ()
    staging_pids: tuple[int, ...] = ()
```

### `tourillon/core/services/probe.py` *(new)*

```python
class FailureDetector:
    def record_heartbeat(self) -> None: ...
    def phi(self) -> float: ...
    @property
    def has_observations(self) -> bool: ...
    def is_available(self, threshold: float = 8.0) -> bool: ...

class MemberState(StrEnum):
    LIVE    = "live"
    SUSPECT = "suspect"
    UNKNOWN = "unknown"

class ProbeManager:
    async def state_of(self, node_id: str) -> MemberState: ...
    async def is_suspect(self, node_id: str) -> bool: ...
    async def is_live(self, node_id: str) -> bool: ...
    async def is_unknown(self, node_id: str) -> bool: ...
    async def record_heartbeat(self, node_id: str) -> None: ...
    async def record_miss(self, node_id: str) -> None: ...
    async def phi_of(self, node_id: str) -> float: ...
    async def snapshot(self) -> dict[str, MemberState]: ...
    async def all_states_with_phi(self) -> list[tuple[str, MemberState, float]]: ...
```

### `tourillon/core/services/starter.py` + `tourillon/core/exceptions.py`

```python
class BootstrapError(Exception): ...  # defined in core/exceptions.py

class NodeStarter:
    @staticmethod
    def check_node_id_consistency(config_node_id: str, state_node_id: str) -> None: ...
    @staticmethod
    def check_tokens_coherence(
        phase: MemberPhase,
        tokens: tuple[int, ...],
        node_size: NodeSize,
    ) -> bool: ...
```

### `tourillon/core/machinery/state.py` + `tourillon/core/exceptions.py`

```python
class StateError(Exception): ...  # defined in core/exceptions.py

class StatePersistence(Protocol):
    async def load(self) -> NodeState | None: ...
    async def save(self, state: NodeState) -> None: ...

class FileStatePersistence:
    def __init__(self, path: Path, config_rw: ConfigReadWriter) -> None: ...
    async def load(self) -> NodeState | None: ...
    async def save(self, state: NodeState) -> None: ...

def _parse_state(raw: dict[str, Any]) -> NodeState: ...
def _encode_state(state: NodeState) -> dict[str, Any]: ...
```

### `tourillon/core/structure/config.py` — `segment_shift` property

```python
@dataclass(frozen=True)
class TourillonConfig:
    # ... all fields from the previous proposal ...
    @property
    def segment_shift(self) -> int: ...
    # ... remaining unchanged fields ...
```

---

## Proposed code organisation

Files **created or modified** by this proposal (in mandatory creation order):

```
tourillon/core/structure/config.py               MODIFIED — keeps derived segment_shift
tourillon/core/structure/ring.py                 NEW — HashSpace, VNode, Ring
tourillon/core/structure/partition.py            NEW — Partitioner, LogicalPartition,
                                                            PartitionPlacement, PartitionRange
tourillon/core/structure/member.py               MODIFIED — MemberPhase, Member, NodeState
tourillon/core/services/topology.py              NEW — MemberRegistry, Topology,
                                                            TopologyManager
tourillon/core/services/placement.py             NEW — PlacementStrategy,
                                                            SimplePreferenceStrategy,
                                                            PreferenceEntry
tourillon/core/services/probe.py                 NEW — FailureDetector, MemberState,
                                                            ProbeManager
tourillon/core/services/starter.py               MODIFIED — startup checks and
                                                            first-node bootstrap orchestration
tourillon/core/machinery/state.py                MODIFIED — StatePersistence,
                                                            FileStatePersistence
tourillon/core/services/config.py                MODIFIED — config validation for
                                                            segment_shift constraint
tourillon/bootstrap/cli/node.py                  MODIFIED — `tourillon node start`
tourillon/bootstrap/deps.py                      MODIFIED — setup_logging and runtime wiring
tests/__init__.py                                CREATE (package marker)
tests/unit/__init__.py                           CREATE (package marker)
tests/unit/test_hashspace.py                     NEW — scenarios 1–3
tests/unit/test_ring.py                          NEW — scenarios 4–7
tests/unit/test_partitioner.py                   NEW — scenarios 8–17
tests/unit/test_member.py                        NEW — scenarios 18–20
tests/unit/test_registry.py                      NEW — scenarios 21–24
tests/unit/test_topology.py                      NEW — scenarios 25–32
tests/unit/test_placement.py                     NEW — scenarios 33–35
tests/unit/test_phi.py                           NEW — scenarios 36–37
tests/unit/test_probe.py                         NEW — scenarios 38–40
tests/unit/test_state_adapter.py                 NEW — scenarios 41–44
tests/unit/test_bootstrap.py                     NEW — scenarios 45–48
tests/e2e/__init__.py                            CREATE (package marker)
tests/e2e/test_node_start.py                     NEW — scenarios 49–50
```

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.
E2e tests use `tmp_path` (pytest fixture) and real filesystem / subprocess.

| # | Mark | Fixture | Action | Expected |
|---|------|---------|--------|----------|
| 1 | unit | `HashSpace(bits=8)` | `h = hs.hash(b"hello")` | `0 <= h < 256` |
| 2 | unit | — | `HashSpace(bits=8).hash(b"x")` called twice | Both calls return the same integer (deterministic) |
| 3 | unit | — | `HashSpace(bits=0)` | Raises `ValueError` with message containing "bits must be >= 1" |
| 4 | unit | `Ring.empty()` | `ring.successor(0)` | Raises `ValueError` |
| 5 | unit | `Ring([VNode("a", 10), VNode("a", 200)])` | `ring.successor(300)` | Returns `VNode("a", 10)` (wraps around) |
| 6 | unit | `Ring([VNode("a", 10)])` | `new_ring = ring.add_vnodes([VNode("b", 5)])`; check `ring` unchanged | Original ring has 1 vnode; new ring has 2 vnodes sorted ascending by token |
| 7 | unit | `Ring([VNode("a", 10), VNode("b", 20)])` | `ring.drop_nodes({"a"})` | Returns new ring with only `VNode("b", 20)` |
| 8 | unit | `HashSpace(bits=8)`, `Partitioner(hs, 4, 1)` | `p.pid_for_hash(0xFF)` | Returns `15` (`0xFF >> 4`) |
| 9 | unit | `HashSpace(bits=8)` | `Partitioner(hs, 8, 1)` | Raises `ValueError` (partition_shift not strictly less than bits) |
| 10 | unit | `HashSpace(bits=8)` | `Partitioner(hs, 4, 4)` | Raises `ValueError` (segment_shift not strictly less than partition_shift) |
| 11 | unit | `Partitioner(HashSpace(8), 4, 1)` | `p.partition_for(0)` | Returns `LogicalPartition(pid=0, start=0, end=16)` |
| 12 | unit | `LogicalPartition(pid=0, start=0, end=16)` | `lp.contains(8)`, `lp.contains(0)`, `lp.contains(16)` | `True`, `False`, `True` |
| 13 | unit | `LogicalPartition(pid=15, start=240, end=0)` (wrap-around) | `lp.contains(255)`, `lp.contains(0)`, `lp.contains(128)` | `True`, `True`, `False` |
| 14 | unit | `Partitioner(HashSpace(8), 4, 1)`, ring with single `VNode("a", 128)` | `p.ranges_for("a", ring)` | Returns one `PartitionRange` with `count == 16`; `start_pid=0`; `end_pid=15` |
| 15 | unit | — | `PartitionRange(owner=VNode("a",0), start_pid=10, end_pid=3, count=9).wraps` | Returns `True` |
| 16 | unit | — | `PartitionRange(owner=VNode("a",0), start_pid=0, end_pid=9, count=10).wraps` | Returns `False` |
| 17 | unit | `Partitioner(HashSpace(8), 4, 1)` | `p.total_partitions == 16` and `p.total_segments == 2` | Both assertions hold |
| 18 | unit | — | `Member("n","a",2,0,READY,(1,),10).supersedes(Member("n","a",1,99,READY,(1,),10))` | Returns `True` (higher generation wins regardless of seq) |
| 19 | unit | — | `Member("n","a",1,5,READY,(1,),10).supersedes(Member("n","a",1,3,READY,(1,),10))` | Returns `True` (same generation, higher seq wins) |
| 20 | unit | — | `m.supersedes(m)` for any `Member` `m` | Returns `False` (equal records do not supersede) |
| 21 | unit | `MemberRegistry()` | `upsert(member_gen1_seq0)` | Returns `True`; `get(node_id)` returns the member |
| 22 | unit | `MemberRegistry()` with member `(gen=1, seq=5)` | `upsert(member with gen=1, seq=3)` | Returns `False`; stored member unchanged |
| 23 | unit | `MemberRegistry()` with READY and IDLE members | `members_in_phase(MemberPhase.READY)` | Returns dict with only the READY member |
| 24 | unit | `MemberRegistry()` with one member | `snap = registry.snapshot(); registry.upsert(newer_member)` | `snap.get(node_id)` still returns the old member (snapshot is independent) |
| 25 | unit | `TopologyManager()` | `await tm.apply_member(member_READY_2_tokens)` | Returns `True`; `snapshot().epoch == 1`; `len(snapshot().ring) == 2` |
| 26 | unit | `TopologyManager()` with READY member | `await tm.apply_member(same_member)` | Returns `False`; epoch unchanged; ring size unchanged |
| 27 | unit | `TopologyManager()` | `await tm.apply_member(member_IDLE_to_JOINING)` | Returns `True`; epoch is `0` (IDLE→JOINING does not advance epoch); ring is empty |
| 28 | unit | `TopologyManager()` with READY member (gen=1,seq=0) | `apply_member(DRAINING gen=1,seq=1)` then `apply_member(IDLE gen=1,seq=2)` | After DRAINING: epoch=2; after IDLE: epoch=3; ring is empty |
| 29 | unit | `TopologyManager()` | `await tm.merge_registry([m1_new, m2_new])` | Returns `2`; both members retrievable from snapshot registry |
| 30 | unit | `TopologyManager()` with one accepted member | `fp1 = await tm.member_fingerprint(); await tm.apply_member(newer_member); fp2 = await tm.member_fingerprint()` | `fp1 != fp2` |
| 31 | unit | `TopologyManager()` | `fp1 = await tm.member_fingerprint(); await tm.apply_member(same_member_no_op); fp2 = await tm.member_fingerprint()` | `fp1 == fp2` (no-op does not invalidate cache) |
| 32 | unit | `TopologyManager()` | `snapshot = await tm.snapshot()` | Returns `Topology` with `epoch=0`, empty registry, empty ring |
| 33 | unit | `SimplePreferenceStrategy(rf=1)` with topology containing one READY node; `ProbeManager()` | `await strategy.preference_list(placement, topology, pm)` | Returns one entry: `readable=True`, `suspect=False`, `handoff=None` |
| 34 | unit | `SimplePreferenceStrategy(rf=2)` with topology where one node is JOINING | `await strategy.preference_list(placement, topology, pm)` | JOINING node absent from preference list (`_EXCLUDED_PHASES`) |
| 35 | unit | `SimplePreferenceStrategy(rf=1)` with topology containing one DRAINING node and one READY handoff candidate | `await strategy.preference_list(placement, topology, pm)` | Entry has `readable=True`; `handoff` is the READY candidate's `node_id` |
| 36 | unit | `FailureDetector()` | `fd.phi()` before any heartbeat | Returns `0.0` |
| 37 | unit | `FailureDetector()` | Two `record_heartbeat()` calls with ≥ 0.05s gap; `fd.phi()` immediately after second call | Returns a value close to `0.0` (elapsed ≪ mean) and `fd.is_available()` is `True` |
| 38 | unit | `ProbeManager()` | `await pm.state_of("node1")` | Returns `MemberState.UNKNOWN` (no observations) |
| 39 | unit | `ProbeManager()` | `await pm.record_heartbeat("node1"); await pm.state_of("node1")` | Returns `MemberState.LIVE` (phi = 0.0 < threshold after first heartbeat) |
| 40 | unit | `ProbeManager()` | `await pm.all_states_with_phi()` on an empty manager | Returns `[]` |
| 41 | unit | `FileStatePersistence(tmp_path / "state.toml", config_rw)` | `await adapter.load()` when file absent | Returns `None` |
| 42 | unit | `FileStatePersistence(tmp_path / "state.toml", config_rw)` | `await adapter.save(state); result = await adapter.load()` | `result` equals `state` on all fields: `node_id`, `phase`, `generation`, `seq`, `tokens`, `epoch`, `committed_pids`, `staging_pids` |
| 43 | unit | `FileStatePersistence(tmp_path / "state.toml", config_rw)` | `await adapter.save(state)` | No stray temp file remains in `tmp_path` after save completes |
| 44 | unit | `FileStatePersistence(tmp_path / "state.toml", config_rw)` with malformed TOML | `await adapter.load()` | Raises `StateError` |
| 45 | unit | `InMemoryStatePersistence()` returning `None`; `TopologyManager()`; `TourillonConfig(node_size=M, ...)` | first-node bootstrap path in `NodeStarter` | Returns `NodeState(phase=READY, generation=1, epoch=1)`; `len(state.tokens) == 4`; topology snapshot has 4 vnodes in ring |
| 46 | unit | `InMemoryStatePersistence()` pre-seeded with READY `NodeState(tokens=(5,10,15,20), epoch=1)`; `TopologyManager()` | crash-recovery bootstrap path in `NodeStarter` | Returns the same `NodeState`; no new state written; topology has 4 vnodes; `save()` call count is `0` |
| 47 | unit | `InMemoryStatePersistence()` pre-seeded with `NodeState(phase=JOINING)` | first-node bootstrap dispatch in `NodeStarter` | Raises `BootstrapError` with `exit_code == 1` |
| 48 | unit | — | `NodeStarter.check_node_id_consistency("node-a", "node-b")` | Raises `NodeIdMismatchError` |
| 49 | e2e | `tmp_path`; valid `config.toml` (from proposal 001 generate); node not yet started | `subprocess tourillon node start --config config.toml` (start then SIGINT) | Process exits cleanly; `state.toml` exists in `data_dir`; `phase = "ready"`; `len(tokens) == cfg.node_size.token_count` |
| 50 | e2e | `tmp_path`; valid `config.toml`; pre-written READY `state.toml` with tokens `T` | `subprocess tourillon node start --config config.toml` (start then SIGINT) | Process exits cleanly; tokens in `state.toml` are unchanged (`T`); no new `state.toml` written during restart |

---

## Exit criteria

- [ ] All 50 test scenarios pass (`uv run pytest -m "unit or e2e" -x`).
- [ ] `uv run pytest --cov=tourillon --cov-fail-under=90` passes.
- [ ] `uv run ruff check tourillon/ tests/` passes with zero violations.
- [ ] `uv run black --check tourillon/ tests/` passes.
- [ ] `TourillonConfig.segment_shift` is available as a derived immutable property; config loading validates `segment_shift < partition_shift`.
- [ ] `Partitioner.__init__` raises `ValueError` when `partition_shift >= hash_space.bits` or `segment_shift >= partition_shift`.
- [ ] First-node bootstrap orchestration in `NodeStarter` calls `state.save()` before topology mutation on the `IDLE` path (write-before-announce).
- [ ] Crash-recovery `READY` startup path does not persist a new state snapshot.
- [ ] Invalid persisted phases still raise `BootstrapError(exit_code=1)`.
- [ ] `FileStatePersistence.save()` leaves no temp file on disk after a successful write.
- [ ] `FileStatePersistence.load()` returns `None` when `state.toml` is absent.
- [ ] `state.toml` round-trip via `_encode_state` / `_parse_state` preserves all eight `NodeState` fields.
- [ ] `TopologyManager.apply_member` adds vnodes to ring on `IDLE/JOINING → READY` transition.
- [ ] `TopologyManager.apply_member` removes vnodes from ring on `DRAINING → IDLE` transition.
- [ ] `TopologyManager.apply_member` returns `False` and leaves epoch unchanged for a no-op (same or older record).
- [ ] `TopologyManager.apply_member` with `IDLE → JOINING` leaves epoch unchanged.
- [ ] `member_fingerprint` is invalidated and recomputed after every accepted mutation.
- [ ] `SimplePreferenceStrategy` never includes `IDLE`, `JOINING`, or `FAILED` nodes in the preference list.
- [ ] Startup log output for a fresh bootstrap includes one `PartitionRange` line per vnode, formatted as specified in the CLI contract.
- [ ] No module under `tourillon/core/` imports `infra/`, `msgpack`, `ssl`, or `tomllib`/`tomli_w` directly.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

- Gossip engine and seeded join (`IDLE → JOINING → READY`) — covered by a later proposal.
- `tourctl node join` command — covered by a later proposal.
- Dual-detector `ProbeManager` (gossip FD + `DataCircuitBreaker`) — covered by a later proposal.
- `DRAINING`, `PAUSED`, and `FAILED` phase transitions — covered by later proposals.
- `tourctl node inspect` command and `node.inspect` envelope kind — covered by a later proposal.
- Partition rebalance and the `[rebalance]` config section — covered by a later proposal.
- KV operations (`kv.put`, `kv.get`, `kv.delete`) and the `[kv]` config section — covered by a later proposal.
- `[gc]` config section and tombstone eviction — covered by a later proposal.
- `BackendStorage` port and `PartitionStore` implementation — covered by a later proposal.
- TLS certificate rotation — static for the node lifetime; requires a restart.
- Heterogeneous `HashSpace.bits` across nodes — detected at gossip time; not reachable via `node start`.
- `TcpClient` connection pooling — deferred to proposals that introduce pooled client wiring.
- Process supervision, systemd unit files, or container entry points.
