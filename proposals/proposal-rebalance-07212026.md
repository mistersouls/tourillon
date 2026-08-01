# Proposal: Partition Rebalance

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Accepted
**Date:** 2026-05-11
**Sequence:** 005
**Revision:** 4
**Continues:** proposal-gossip-seeded-join-05102026-004 (`JOINING → READY` deferred)
**Updated:** 2026-07-21 — aligned with implemented storage model (Namespace.TAGS/LOG, TagKind, Address, RangeTransfer, rebalance.resume two-stage protocol)
**Updated:** 2026-07-22 — CLI redesigned: `tourctl rebalance status` shows one line per range; new `tourctl rebalance range --id <range-id> <addr>` shows one line per PID within a range

---

> **Continuation of proposal 004.** Proposal 004 (Gossip Engine & Seeded Node
> Join) defined the `IDLE → JOINING` transition and explicitly deferred the
> `JOINING → READY` transition to a dedicated rebalance proposal. This proposal
> fulfils that commitment: it specifies and implements the full partition
> rebalance protocol that drives a node from `JOINING → READY` (receiving
> partitions) and from `DRAINING → IDLE` (sending partitions).

---

## Summary

This proposal is the direct continuation of proposal 004, which left a node
in the `JOINING` phase after gossip bootstrap with no mechanism to advance to
`READY`. The missing piece is partition data transfer: a `JOINING` node must
receive all partitions it owns in the new ring before it can serve KV traffic;
a `DRAINING` node must send all its partitions before it can leave.

This proposal defines how Tourillon performs that redistribution: a
deterministic planner, a stateful applicator, a streaming wire protocol with
crash-safe staging, and two operator-visible commands —
`tourctl rebalance status` (one line per range, overview) and
`tourctl rebalance range` (one line per PID, drill-down into a range).
Completion of all transfers triggers the `JOINING → READY` or `DRAINING → IDLE`
phase transition that proposal 004 explicitly deferred.

The storage design in this proposal uses two logical **namespaces** —
`Namespace.TAGS` (chronological ordering within a partition, HLC cursor) and
`Namespace.LOG` (per-key multi-version storage, latest-HLC lookup) — backed by
LMDB as the **reference implementation**, chosen for its ACID guarantees,
transactional writes, and cursor-based iteration. The domain layer depends
exclusively on the `Storage` / `PartitionStore` / `PartitionStaging` **Protocol
interfaces** defined in `core/ports/storage.py`. LMDB-specific concepts
(transaction semantics, cursor positioning) are confined to `tourillon/infra/`
and never leak into `core/`. Keys are encoded using the domain types `Pid`,
`Address` (keyspace + key bytes), `HLCTimestamp`, and `Key`
(`core/machinery/namespace.py`); tags are expressed as `TagKind` enum values
combined with `TagPayload`.

---

## Motivation

Proposal 004 established that a node completing gossip bootstrap enters the
`JOINING` phase with its vnodes in the ring but without any partition data.
Without a rebalance mechanism, that node would remain stuck in `JOINING`
indefinitely — owning ring partitions it has no data for, causing read misses,
and never opening its KV socket. A leaving node faces the symmetric problem: it
holds data that should belong to its successors but has no way to transfer it.
This proposal closes that gap while guaranteeing no data loss on crash, no
duplicate work on restart, and clean cancellation when topology changes again
mid-transfer.

---

## CLI contract

The rebalance CLI exposes **two complementary commands**:

| Command | Granularity | Error level |
|---------|-------------|-------------|
| `tourctl rebalance status <addr>` | One line per **range** | `LAST_ERROR` = first failed PID's error for that range |
| `tourctl rebalance range --id <range-id> <addr>` | One line per **PID** in a range | `LAST_ERROR` = that PID's own transfer error |

Both commands connect directly to `<addr>` (the target's peer address, e.g.
`10.0.0.2:7701`) over mTLS. No contact node, no forwarding.

A **range** is a `RangeTransfer` entry: a contiguous span of partition IDs
sharing the same `(src, dst)`. The range identifier has the form
`{src}→{dst}:{start_pid}–{end_pid}`, e.g. `node-1→node-3:0–42999`.
This matches `RangeTransfer.id` in the code.

---

### `tourctl rebalance status <addr> [--blocked] [--json]`

Displays a summary header followed by **one line per range** enrolled in the
current rebalance plan. Each line aggregates the PID-level state for that range.

**Range aggregate state** — derived from the terminal/non-terminal state of
all `TransferHandle`s within the range:

| Aggregate | Rule |
|-----------|------|
| `OK`      | All PIDs `COMMITTED` |
| `BLOCKED` | At least one PID `FAILED` |
| `RUNNING` | At least one PID `RUNNING` or `PENDING`, none `FAILED` |
| `DONE`    | All PIDs `CANCELLED` (range superseded by new epoch) |

**Range-level `LAST_ERROR`** — the `last_error` of the first FAILED PID in
the range (sorted by pid ascending). `—` when no PID has failed. Both views
surface errors at their respective granularity: the status table shows one
error per range, the range detail table shows one error per PID.

**Node in JOINING (destination — receiving):**

```
$ tourctl rebalance status 10.0.0.3:7701

Rebalance status — node-3  (epoch 4, JOINING → READY)
Role: receiving
Ranges: 3 total  (1 OK, 1 running, 1 blocked)

 RANGE                        FROM     TO       PIDS   COMMITTED  RUNNING  FAILED  STATE    LAST_ERROR
────────────────────────────────────────────────────────────────────────────────────────────────────────
 node-1→node-3:0–999          node-1   node-3   1000      998        1       1     BLOCKED  source unreachable after 10 retries
 node-2→node-3:1000–1999      node-2   node-3   1000     1000        0       0     OK       —
 node-2→node-3:2000–2499      node-2   node-3    500      498        2       0     RUNNING  —
```

**Node in DRAINING (source — sending):**

```
$ tourctl rebalance status 10.0.0.2:7701

Rebalance status — node-2  (epoch 4, DRAINING → IDLE)
Role: sending
Ranges: 2 total  (1 OK, 1 running, 0 blocked)

 RANGE                        FROM     TO       PIDS   COMMITTED  RUNNING  FAILED  STATE    LAST_ERROR
────────────────────────────────────────────────────────────────────────────────────────────────────────
 node-2→node-3:0–999          node-2   node-3   1000     1000        0       0     OK       —
 node-2→node-4:1000–1499      node-2   node-4    500      498        2       0     RUNNING  —
```

`LAST_ERROR` is truncated at 60 characters in terminal output; `--json` returns
the full string. `—` when no PID in the range has a recorded error.

**`--blocked` — only blocked ranges + operator hint:**

```
$ tourctl rebalance status 10.0.0.3:7701 --blocked

Rebalance status — node-3  (epoch 4, JOINING → READY)
State: BLOCKED

Blocked ranges:
 RANGE                        FROM     TO       PIDS   FAILED  LAST_ERROR
──────────────────────────────────────────────────────────────────────────────────────────────
 node-1→node-3:0–999          node-1   node-3   1000       1   source unreachable after 10 retries

Operator hint:
  - Use `tourctl rebalance range --id node-1→node-3:0–999 10.0.0.3:7701` to see which PIDs failed.
  - Check node-1 state (FAILED/PAUSED?).
  - Either:
      recover node-1 (FAILED → JOINING / READY), or
      change topology to trigger a new epoch and plan.
```

Exit code 2 when `--blocked` and at least one range is BLOCKED.

**No active rebalance:**

```
$ tourctl rebalance status 10.0.0.1:7701
No active rebalance on node-1.
```

Exit codes: 0 on success, 1 on any error, 2 when `--blocked` and BLOCKED.

---

### `tourctl rebalance range --id <range-id> <addr> [--json]`

Displays a range header followed by **one line per PID** within that range.
`<range-id>` is the full identifier returned in the status table:
`{src}→{dst}:{start_pid}–{end_pid}`.

**Example — partially failed range:**

```
$ tourctl rebalance range --id "node-1→node-3:0–999" 10.0.0.3:7701

Range node-1→node-3:0–999  (BLOCKED)
  From:   node-1
  To:     node-3
  Epoch:  4
  PIDs:   1000 total  (998 committed, 1 running, 1 failed)

 PID   STATE       CHUNKS    BYTES      AGE    LAST_ERROR
─────────────────────────────────────────────────────────
   0   COMMITTED    128       4.2 MiB   12s    —
   1   COMMITTED    128       4.2 MiB   12s    —
   ...
 998   RUNNING       61/128   2.0 MiB    4s    —
 999   FAILED        45/128   1.5 MiB    8s    source unreachable after 10 retries
```

The `...` notation collapses identical consecutive COMMITTED rows in the
terminal; `--json` always emits every row explicitly.

**Example — fully committed range:**

```
$ tourctl rebalance range --id "node-2→node-3:1000–1999" 10.0.0.3:7701

Range node-2→node-3:1000–1999  (OK)
  From:   node-2
  To:     node-3
  Epoch:  4
  PIDs:   1000 total  (1000 committed)

 PID    STATE       CHUNKS    BYTES      AGE    LAST_ERROR
───────────────────────────────────────────────────────────
 1000   COMMITTED    128       4.2 MiB   12s    —
 1001   COMMITTED    128       4.2 MiB   12s    —
 ...
 1999   COMMITTED    128       4.2 MiB   12s    —
```

**Unknown range:**

```
$ tourctl rebalance range --id "node-x→node-3:9999–9999" 10.0.0.3:7701
Error: range 'node-x→node-3:9999–9999' not found on node-3.
```

Exit codes: 0 on success, 1 on any error.

**`--json`** emits the raw `rebalance.range.response` payload as pretty-printed
JSON, including every PID entry without collapse.

---

### Wire protocol extensions

Two request/response pairs support the CLI commands.

#### `rebalance.status` → `rebalance.status.response` (per-range)

Request:
```json
{ "epoch": null }
```

Response:
```json
{
  "node_id":  "node-3",
  "epoch":    4,
  "trigger":  "joining",
  "role":     "receiving",
  "blocked":  true,
  "ranges": [
    {
      "range_id":   "node-1→node-3:0–999",
      "src":        "node-1",
      "dst":        "node-3",
      "start_pid":  0,
      "end_pid":    999,
      "total_pids": 1000,
      "committed":  998,
      "running":    1,
      "pending":    0,
      "failed":     1,
      "cancelled":  0,
      "state":      "blocked",
      "last_error": "source unreachable after 10 retries"
    },
    {
      "range_id":   "node-2→node-3:1000–1999",
      "src":        "node-2",
      "dst":        "node-3",
      "start_pid":  1000,
      "end_pid":    1999,
      "total_pids": 1000,
      "committed":  1000,
      "running":    0,
      "pending":    0,
      "failed":     0,
      "cancelled":  0,
      "state":      "ok",
      "last_error": null
    }
  ]
}
```

- `trigger` is `"joining"` or `"draining"`.
- `role` is `"receiving"` (JOINING node) or `"sending"` (DRAINING node).
- `blocked` is `true` when at least one range has a FAILED PID.
- **Range-level `last_error`**: `last_error` of the first FAILED PID in the
  range (sorted by pid ascending). `null` when no PID is FAILED.
- **PID-level `last_error`** (in `rebalance.range.response`): the per-`TransferHandle`
  `last_error` string. `null` when no error has been recorded for that pid.
- Both levels always carry `last_error`; `null` means no error.

#### `rebalance.range` → `rebalance.range.response` (new — per-PID)

Request:
```json
{ "range_id": "node-1→node-3:0–999" }
```

Response:
```json
{
  "node_id":    "node-3",
  "epoch":      4,
  "range_id":   "node-1→node-3:0–999",
  "src":        "node-1",
  "dst":        "node-3",
  "start_pid":  0,
  "end_pid":    999,
  "state":      "blocked",
  "transfers": [
    {
      "pid":          0,
      "state":        "committed",
      "chunks_done":  128,
      "chunks_total": 128,
      "bytes_done":   4404019,
      "started_at":   "2026-05-10T14:23:33Z",
      "finished_at":  "2026-05-10T14:23:45Z",
      "last_error":   null
    },
    {
      "pid":          999,
      "state":        "failed",
      "chunks_done":  45,
      "chunks_total": null,
      "bytes_done":   1572864,
      "started_at":   "2026-05-10T14:23:37Z",
      "finished_at":  "2026-05-10T14:23:45Z",
      "last_error":   "source unreachable after 10 retries"
    }
  ]
}
```

Error when `range_id` not found:
```json
{ "error": "range_not_found", "range_id": "node-x→node-3:9999–9999" }
```

`rebalance.range.response` is paginated when the range contains more PIDs
than fit in `MAX_PAYLOAD_DEFAULT`; use `after_pid`/`limit`/`has_more`/`next_pid`.


---

## Design

### Data model

#### `HLCTimestamp` and `HLCClock` (`core/structure/clock.py`)

Immutable hybrid logical clock timestamp `(wall_ms, counter, node_id)` with
`tick()` and `update()` methods. Used as the version key (`metadata`) in every
storage record. `HLCClock` is the mutable stateful wrapper that advances the
timestamp monotonically for a single node.

#### LMDB storage layout

LMDB supports multiple **named databases** within a single environment — each
database is accessed via a `DBI` (database instance) handle. Tourillon opens
**two DBIs** per environment:

| DBI name | Handle constant | Purpose |
|----------|-----------------|---------|
| `"index"` | `Namespace.TAGS` | Chronological ordering within a partition (HLC cursor) |
| `"data"`  | `Namespace.LOG`  | Per-key multi-version storage (latest-HLC lookup) |

Both DBIs are opened with `MDB_CREATE` at environment startup and are written
**within the same LMDB write transaction**. This guarantees that every mutation
is atomic across INDEX and DATA: a crash cannot leave one DBI updated and the
other not. All reads also use a single read transaction that spans both DBIs,
providing a consistent snapshot of the full record at any instant.

```
Namespace.TAGS  key: pid (4B BE) | hlc (12B) | keyspace | key
           val: b""                   — committed, visible to kv.get
              | b"\x01" + epoch (4B)  — rebalance staging
              | b"\x02" + node_id     — hinted handoff

Namespace.LOG   key: pid (4B BE) | keyspace | key | hlc (12B)
           val: <user bytes>  (empty for Tombstone)
```

`Namespace.TAGS` sorts entries **chronologically within a partition** (pid then HLC).
`Namespace.LOG` sorts entries **per-key within a partition** (pid | keyspace | key
then HLC ascending), so all versions of a key are contiguous and the latest HLC
is last.

`kv.get(pid, keyspace, key)` positions a `Namespace.LOG` cursor at the last entry
whose key starts with `(pid, keyspace, key)` and returns the value — **O(1)**.
No `Namespace.TAGS` lookup is required. The phase invariant guarantees that on any
node serving KV traffic (`phase == READY`), every entry for its owned partitions
carries the committed tag `b""`: a node in JOINING (the only phase where
`\x01` staging entries exist) never serves KV traffic (invariant §3), and by
the time a node reaches READY, `commit()` has already flipped all staging tags
atomically in a single LMDB transaction. The `Namespace.TAGS` tag therefore plays no
role in the read path; it exists exclusively for `commit()`, `cleanup()`,
`exists()`, `last_staged_key()`, and `scan()` (rebalance and anti-entropy
paths).

The store is **log-structured**: each write to a user key appends a new
entry to both `Namespace.TAGS` and `Namespace.LOG` because the HLC — encoding
`(wall_ms, counter, node_id)` — is strictly monotonically increasing per write
and globally unique (node_id disambiguates concurrent writes from different
nodes). No existing key is ever overwritten in normal operation (staging cleanup
and compaction are the only exceptions). For a key written N times, N `Namespace.LOG`
entries exist; `kv.get` reads only the last one.

#### `Address`, `Version`, `Tombstone`, `KvMetadata` (`core/structure/record.py`)

```python
@dataclass(frozen=True)
class Address:
    """Canonical addressing unit: logical keyspace + record key, both raw bytes."""
    keyspace: bytes
    key: bytes

@dataclass(frozen=True)
class KvMetadata:
    """HLC timestamp plus write-quorum stored with every KV record."""
    hlc: HLCTimestamp
    quorum_write: int = 1

@dataclass(frozen=True)
class Version:
    """Immutable snapshot of a key's value at a specific causal instant."""
    address:      Address
    metadata:     HLCTimestamp   # ordering handle — never compare by value bytes
    value:        bytes
    quorum_write: int

@dataclass(frozen=True)
class Tombstone:
    """Deletion marker that causally supersedes earlier Versions."""
    address:      Address
    metadata:     HLCTimestamp   # no value field
    quorum_write: int
```

`Version` and `Tombstone` are serialised as kind-discriminated dicts and are
fully round-trippable through msgpack via `.to_dict()` / `.from_dict()`.

`keyspace` and `key` are raw `bytes` throughout — callers that work with
human-readable names are responsible for encoding before constructing an
`Address`.

#### `RangeTransfer`, `PartitionTransfer`, `RebalancePlan`, `TransferState`, `TransferHandle`

`RangeTransfer` is the **wire unit** for `rebalance.plan`: it groups
contiguous partition IDs sharing the same `(src, dst)` into a single range entry.
With `pshift=17` (131 072 partitions) this reduces the plan payload from
O(partitions) entries to O(nodes) entries. `PartitionTransfer` is the **internal
unit** used by the applicator for per-pid tracking, staging, and commit; it is
never serialised on the wire.

```python
@dataclass(frozen=True)
class RangeTransfer:
    start_pid: int   # inclusive lower bound
    end_pid:   int   # inclusive upper bound
    src: str         # node_id of the source
    dst: str         # node_id of the destination
    count: int       # range size hint (informational)
    # pids() → yields each pid in [start_pid, end_pid], wrapping at total_partitions

@dataclass(frozen=True)
class PartitionTransfer:
    pid: int
    src: str   # node_id of the source
    dst: str   # node_id of the destination (entering node)

@dataclass
class RebalancePlan:
    epoch:  int
    ranges: tuple[RangeTransfer, ...]   # wire form; call .pids(total) for pid-level iteration

class TransferState(StrEnum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMMITTED = "committed"
    CANCELLED = "cancelled"
    FAILED    = "failed"

@dataclass
class TransferHandle:
    transfer:     PartitionTransfer
    state:        TransferState
    epoch:        int
    queue:        asyncio.Queue[TransferMessage]
    cancel_event: asyncio.Event
    chunks_done:  int = 0
    chunks_total: int | None = None
    bytes_done:   int = 0
    started_at:   datetime | None = None
    finished_at:  datetime | None = None
    last_error:   str | None = None   # human-readable last network error; None when no error
```

#### `Storage`, `PartitionStore`, `PartitionStaging`, `PartitionHint` (`core/ports/storage.py`)

Three-level scoped hierarchy. `Storage` is a factory keyed by `pid`.
`PartitionStore` is the per-partition handle — `pid` only appears once at
`open_by_pid()`. `PartitionStaging` is a sub-context scoped to one `(pid, epoch)`
pair for rebalance staging. `PartitionHint` handles hinted-handoff puts.

```python
class Storage(Protocol):
    async def open_by_pid(self, pid: int) -> PartitionStore: ...
    async def close(self) -> None: ...

class PartitionStore(Protocol):
    """Per-partition handle. KV operations (get/put/tombstone) live here."""

    def scan(
        self, resume_from: Key | None = None, batch_size: int = 1024
    ) -> AsyncIterator[TaggedRecord]: ...
    def staging(self, epoch: int) -> PartitionStaging: ...
    def hint(self, node_id: str) -> PartitionHint: ...

class PartitionStaging(Protocol):
    """Rebalance staging context scoped to one (pid, epoch) pair."""

    async def stage(self, tagged: TaggedRecord) -> None: ...
    async def commit(self) -> None: ...
    async def cleanup(self) -> None: ...
    async def exists(self) -> bool: ...
    async def last_staged_key(self) -> Key | None: ...
```

`scan(resume_from)` walks `Namespace.TAGS` using `resume_from` as a `Key` cursor.
When `resume_from` is a `Key`, the implementation positions strictly **after** it.
When `resume_from` is `None`, the scan starts from `(pid, HLC=0)` for a full
transfer. The scan yields every committed (`TagKind.LIVE` or `TagKind.TOMBSTONE`
tagged) entry in **HLC order** as `TaggedRecord` objects. A key written N times
appears N times in `scan()` output.

Transferring the full history from HLC=0 is necessary so the destination node's
`Namespace.TAGS` watermark is accurate for future KV anti-entropy.

`stage(tagged)` writes one record to `Namespace.TAGS` with `TagKind.STAGING`
tag (sub-kind = LIVE or TOMBSTONE, payload = epoch bytes) and to `Namespace.LOG`
atomically. Must be called only after `pid` has been added to `staging_pids`
in `state.toml` (invariant §2).

`commit()` atomically promotes all staged entries for this `(pid, epoch)`:
each `Namespace.TAGS` entry with `TagKind.STAGING` has its tag updated to
`TagKind.LIVE` or `TagKind.TOMBSTONE` (from the staging sub-kind). Called after
`rebalance.commit.ok` is received; `state.toml` is updated to move `pid` from
`staging_pids` to `committed_pids` **after** this commit (storage-first
invariant §1).

`cleanup()` deletes all `TagKind.STAGING` entries for this `(pid, epoch)`.
Called on: (1) explicit cancel (superseded plan); (2) startup cleanup when the
stored epoch is older than the current gossip epoch.

`exists()` returns `True` if any `TagKind.STAGING` entries exist for this
`(pid, epoch)`. Used during crash recovery.

`last_staged_key()` returns the `Key` with the maximum HLC among all `STAGING`
entries for this `(pid, epoch)`, or `None` when nothing has been staged yet.
The returned `Key` is passed as `resume_from` in the `rebalance.resume` message.

#### `NodeState` extensions

`committed_pids` and `staging_pids` are added to `NodeState` and persisted in
a new `[rebalance]` section of `state.toml`:

```toml
[rebalance]
committed_pids = [42, 67]
staging_pids   = [43, 44]
```

Both are reset to `[]` whenever `epoch` advances.

#### Rebalance tuning (constructor params on `RebalanceApplicator`)

Rebalance concurrency and chunk size are constructor parameters on
`RebalanceApplicator`, not a separate config section:

```python
RebalanceApplicator(
    ...,
    max_concurrency: int = 4,        # max parallel in-flight transfers
    max_chunk_bytes: int = 2_097_152,  # 2 MiB per transfer chunk
)
```

### Visibility tags in `Namespace.TAGS`

Every `Namespace.TAGS` entry carries a `Tag` composed of a `TagKind` byte
and an optional `TagPayload`, written atomically with the `Namespace.LOG` entry.

```python
class TagKind(Enum):
    LIVE      = b"\x00"   # committed version   — visible to kv.get
    TOMBSTONE = b"\x01"   # committed deletion  — visible to kv.get
    STAGING   = b"\x02"   # rebalance staging   — NOT visible to kv.get
    HINT      = b"\x03"   # hinted handoff      — NOT visible to kv.get
    STALE     = b"\x04"   # superseded version  — NOT visible to kv.get
    PHANTOM   = b"\xff"   # internal/reserved
```

`STAGING` payload: `sub (1B: 0x00=LIVE | 0x01=TOMBSTONE) + epoch (4B BE)`.
`HINT` payload: `sub (1B: 0x00=LIVE | 0x01=TOMBSTONE) + node_id (UTF-8 bytes)`.

| Kind       | Meaning                   | Visible to `kv.get` |
|------------|---------------------------|:-------------------:|
| `LIVE`     | Committed version         | ✅                  |
| `TOMBSTONE`| Committed deletion        | ✅                  |
| `STAGING`  | Rebalance in-progress     | ❌                  |
| `HINT`     | Hinted handoff pending    | ❌                  |
| `STALE`    | Superseded (compaction)   | ❌                  |

### Core invariants

1. **Storage-first commit**: `commit()` (promoting `STAGING` → `LIVE`/`TOMBSTONE`)
   is performed before `state.toml` is updated to move the pid from `staging_pids`
   to `committed_pids`. A crash between the two is auto-healed on restart.
2. **state.toml before first write**: `staging_pids` is updated to include a
   pid in `state.toml` *before* the first storage write for that pid.
3. **Staging invisible**: a node in JOINING does not serve KV traffic; staging
   entries cannot be returned by `kv.get`.
4. **Idempotent restart**: `committed_pids` prevents re-transferring a pid
   that already succeeded. `staging_pids` enables targeted cleanup.
5. **Cancel-safe**: `cancel_event.set()` causes the coroutine to delete staging
   entries and remove the pid from `staging_pids` before exiting.
6. **Topology-only planner**: `_replica_set()` is derived solely from the ring
   (vnode positions). FAILED nodes are included because excluding them would
   create plan divergence when their FAILED gossip message has not yet reached
   all nodes. If a FAILED node is assigned as source, the transfer exhausts its
   retries, blocks the phase, and waits for a new epoch. Operator intervention
   is required to resolve the deadlock (FAILED → JOINING or FAILED → DRAINING).
7. **No phase advance on failed pids**: the `JOINING → READY` or
   `DRAINING → IDLE` transition is gated on `WaitGroup.wait()` returning with an
   empty `failed_list`. Even a single permanently-failed pid blocks the phase
   transition until a new plan resolves it.
8. **Network errors retried; process errors abort immediately**: only transport-level
   failures (connection refused, timeout, mTLS error, stream drop) trigger the
   exponential-backoff retry loop (`GossipBootstrapConfig` parameters). Protocol
   rejections (`epoch_mismatch`, `src_mismatch`, `malformed_transfers`, digest
   mismatch) are process errors — retrying would produce the same result — and
   cause the transfer to be marked FAILED immediately without consuming retries.
   The retry loop checks `cancel_event` between attempts. After `max_retries`
   exhausted on network errors → transfer marked FAILED. No local source
   substitution is performed.
9. **DRAINING source reads store directly**: the rebalance protocol always reads
   committed data from the `PartitionStore` via `scan()`; it never goes through
   the KV preference list or consults the handoff target.

### MemberPhase FSM

All valid phase transitions are listed below. The rebalance planner and
applicator must reason about these phases consistently.

```
IDLE    → JOINING   — node initiates join
JOINING → READY     — all partition transfers committed
JOINING → FAILED    — join aborted (e.g. transfers permanently fail after max retries)
JOINING → PAUSED    — operator pause mid-join

READY   → DRAINING  — operator-initiated leave (future proposal)
READY   → PAUSED    — operator pause

DRAINING → IDLE     — all partition transfers committed (leave complete)
DRAINING → FAILED   — drain aborted (e.g. transfers permanently fail)
DRAINING → PAUSED   — operator pause mid-drain

FAILED  → JOINING   — operator-initiated re-join after failure
FAILED  → DRAINING  — operator-initiated forced drain after failure

PAUSED  → (previous phase resumed by operator)
```

Key properties used by the planner and applicator:

| Phase    | Vnodes in ring | Serves KV | Gossips | Included in replica-set computation |
|----------|:--------------:|:---------:|:-------:|:-----------------------------------:|
| IDLE     | ❌             | ❌        | ❌      | ❌ (never in ring)                  |
| JOINING  | ✅             | ❌        | ✅      | ✅                                  |
| READY    | ✅             | ✅        | ✅      | ✅                                  |
| DRAINING | ✅             | reads only| ✅      | ✅                                  |
| FAILED   | ✅ (inert)     | ❌        | ❌      | ✅ (see below)                      |
| PAUSED   | ✅             | ❌        | ✅      | ✅                                  |

FAILED nodes keep their vnodes in the ring and are **included** in replica-set
computation. Excluding them would introduce plan divergence: a FAILED node
emits its state transition exactly once via gossip; if that message has not yet
reached all nodes when they each compute the plan, nodes with different registry
views would produce different assignments for the same epoch — breaking the
determinism invariant. Including FAILED nodes keeps the plan derivable from the
ring topology alone (no registry dependency). The consequence is that any
transfer assigned to a FAILED source will exhaust its retries, block the phase,
and wait for a new epoch. Resolution requires operator intervention (FAILED →
JOINING or FAILED → DRAINING to re-enter the cluster). Vnode expiration is
explicitly **out of scope**.

### Replica-aware planner

`RebalancePlanner` is constructed with a `Partitioner` and the cluster
replication factor `rf`. For each partition `pid`, it computes the **replica
sets** — the first `rf` distinct nodes clockwise in each ring (all phases
included; topology-only, no registry consulted) — rather than a single owner.

```
old_replicas = first rf distinct node_ids clockwise in old_ring   # all phases included
new_replicas = first rf distinct node_ids clockwise in new_ring   # all phases included

leaving  = sorted(old_replicas - new_replicas)   # nodes exiting the replica set
entering = sorted(new_replicas - old_replicas)   # nodes joining the replica set
```

The replica set computation uses the same clockwise-walk algorithm as
`SimplePreferenceStrategy` (collect `rf` distinct `node_id`s from
`ring.iter_from(vnode)`), but applied **independently to each ring** as a
**pure synchronous helper**. It deliberately does not reuse
`SimplePreferenceStrategy` for three reasons:

1. `SimplePreferenceStrategy` is `async` and consults `ProbeManager` (suspect
   state). The planner must be **synchronous and deterministic**: two nodes
   computing the plan independently from the same ring snapshots must reach
   identical assignments. Probe state varies per node per instant.
2. `SimplePreferenceStrategy` excludes `_EXCLUDED_PHASES = {IDLE, JOINING,
   FAILED}`. For `new_ring` in the JOIN scenario, the arriving node's tokens
   are explicitly added; excluding it by phase would produce an empty
   `entering` set and a no-op plan.
3. `SimplePreferenceStrategy` operates on a single live `topology.ring`. The
   planner receives two explicit ring objects (`old_ring`, `new_ring`) and must
   query each independently.

The internal helper `_replica_set(ring, vnode, rf) -> frozenset[str]` mirrors
the walk core without probe check or phase filter. IDLE nodes never appear in
the ring (they never add vnodes). FAILED, PAUSED, JOINING, READY, and DRAINING
nodes are all included if their vnodes are present — the planner is
**topology-only** and never consults the registry. This preserves full
determinism: two nodes computing the plan independently from the same ring
snapshot always reach identical assignments, regardless of their local registry
state at that instant.

`plan()` requires only the two ring objects and the epoch — no `registry`
parameter.

**Source assignment — leaving-first pairing:**

Leaving nodes are paired 1-to-1 with entering nodes in sorted order using
`zip(leaving, entering)`, which stops at the shorter list:

```
pairs    = zip(leaving, entering)              # D→B, E→C, …
unpaired = entering[len(leaving):]             # entering nodes with no leaving pair
→ PartitionTransfer(pid, src=min(old_replicas), dst=dst) for dst in unpaired
```

`min(old_replicas)` is used for unpaired destinations — it is the
lexicographically smallest node_id among the non-FAILED **and non-PAUSED** old
replica set. FAILED and PAUSED nodes are excluded because neither can reliably
serve as a transfer source: FAILED nodes are inert, and PAUSED nodes are
temporarily unreachable. Selecting either would immediately trigger the
retry-with-backoff policy and risk blocking the phase. The result is always a
live READY or DRAINING node.

Asymmetry only occurs near the `rf` boundary:

| Case | Condition | `zip` result | `unpaired` |
|---|---|---|---|
| Cluster growing through rf | `\|entering\| > \|leaving\|` | leaving-first pairs consumed first | remaining entering nodes use `min(old_replicas)` |
| Cluster shrinking through rf | `\|leaving\| > \|entering\|` | `zip` stops at `len(entering)` | `entering[len(leaving):]` → `[]` in Python (no IndexError) |

In the shrinking case, leftover leaving nodes have no valid destination: the
cluster is under-replicated and no new node needs their data. Transferring to
an already-covered node is forbidden by the replica exclusion invariant (those
nodes already hold their own version, possibly newer). Any data exclusive to the
leftover leaving nodes is accepted as lost — documented trade-off of draining
below rf.

**Why leaving-first pairing matters (LWW + replication lag):**

Tourillon uses last-write-wins (LWW): the `HLCTimestamp` total order `(wall_ms,
counter, node_id)` determines a single authoritative version per key on each
replica. There are never two coexisting versions of the same key on the same
replica. The highest HLC always wins — even if a write could not be confirmed
on all replicas at write time (partial quorum), hinted handoff (`\x02<node_id>`
tag) ensures the missing replica is eventually brought up to date. Quorum
read/write semantics are defined in a future KV proposal.

However, in a leaderless AP system with asynchronous replication, a leaving node
D may have received writes **after** the last replication cycle to stable node A.
Under LWW, D's copy IS the authoritative newest version for those keys as far as
the cluster is concerned. If the rebalance only transferred from A:

- B would start with A's older LWW version for keys that D updated more recently.
- Keys would silently regress to stale data until KV anti-entropy corrected them.

Pairing D→B directly transfers D's most-recent versions to B immediately.
The new replica set `{A, B, C}` thus contains the union of the freshest
versions that were spread across `{A, D, E}`, with no replication-lag regression.
Using only `min(old_replicas)` as the universal source would transfer A's potentially
stale versions and depend on eventual anti-entropy to correct the regression.

**Replica exclusion invariant**: `entering` only contains nodes absent from
`old_replicas`. A node already in the replica set is never a transfer destination:
it already holds committed data (possibly newer than the source's copy under LWW),
and overwriting it with a bulk transfer could regress it to an older version.

**No-op cases**:
- `old_ring` is empty → `old_replicas = ∅` → no source → empty plan.
- `|cluster| < rf` and source draining → `entering = ∅` → empty plan; the
  surviving nodes already cover all available replication slots.


### Concrete rf scenarios

| rf | Cluster             | Event           | Transfer outcome                                                        |
|----|---------------------|-----------------|-------------------------------------------------------------------------|
| 3  | 2 nodes (A, B)      | B draining      | `entering=∅` → **no transfer**; A already covers all slots             |
| 3  | 4 nodes (A,B,C,D)   | D draining      | pid `old={A,B,D}` → `leaving=[D]`, `entering=[C]` → **D→C**            |
| 3  | 5 nodes (A,B,C,D,E) | D+E draining    | pid `old={A,D,E}` → `leaving=[D,E]`, `entering=[B,C]` → **D→B, E→C**  |
| 3  | E joining (rf=3/3)  | E joins         | pid `old={A,B,C}` → `entering=[E]`, `leaving=[]` → **min(old_replicas)=A→E** |
| 3  | 4 nodes, D FAILED   | E joins         | pid `old={A,B,C,D}` (D included, inert) → `entering=[E]`; if D paired with E → transfer fails; operator must recover D |

In the rf=3 / 2-node case the cluster is already under-replicated;
the surviving node has all the data and no transfer is needed. Accepted
data-availability trade-off documented in design decisions.

### Preference list interaction and edge cases

`SimplePreferenceStrategy` (in `tourillon/core/ring/placement.py`) and the
rebalance planner both operate on ring topology but serve different purposes.

#### Phase exclusion divergence

`SimplePreferenceStrategy` excludes `{IDLE, JOINING, FAILED}` nodes from KV
preference lists; DRAINING nodes are included but have `handoff != None` — new
writes are redirected to a live handoff target while the node continues to serve
reads. The rebalance planner includes **all phases** (FAILED, PAUSED, JOINING,
READY, DRAINING) in replica-set computation — it is topology-only and never
excludes by phase. FAILED and PAUSED nodes may therefore appear in `leaving` or
`entering`; the applicator handles them via the retry policy (FAILED: exhausts
retries and blocks the phase; PAUSED: retries until the node resumes).

#### DRAINING source and handoff

A DRAINING source has `PreferenceEntry.handoff != None` — new KV writes for its
partitions are redirected by the KV layer to the handoff target. The rebalance
protocol **bypasses the preference list entirely**: it reads committed LMDB data
directly from the source via `PartitionStore.scan()`. Writes that reached the
handoff target after the DRAINING phase started are reconciled by KV
anti-entropy (out of scope). The handoff target will be in the final replica set
(`entering`), ensuring those writes survive.

#### JOINING destination and preference list exclusion

The JOINING node is in `_EXCLUDED_PHASES` for `SimplePreferenceStrategy` — it
does not appear in any KV preference list while transferring. All KV writes
during the transfer window continue to flow to the old replica set. The JOINING
node becomes visible to `SimplePreferenceStrategy` only after `JOINING → READY`
and `TopologyManager` adds its vnodes to the ring.

#### PAUSED source

A PAUSED node has its vnodes in the ring, data intact, but is temporarily not
serving traffic. When the planned source is PAUSED and unreachable, the
applicator applies the **same retry-with-backoff policy** as for any unreachable
peer: wait for it to resume (PAUSED → previous phase). No source substitution
is performed — the PAUSED node holds the correct data and a substitution could
produce a stale transfer from a node with older LWW versions. If retries are
exhausted before the source resumes, the transfer is marked FAILED and the phase
is blocked until a new gossip epoch provides a new plan.

#### Unreachable peer policy (source or destination)

When the applicator cannot reach a transfer peer (connection refused, timeout,
mTLS handshake error), it applies the following policy:

1. Log WARNING with peer `node_id` and error.
2. Retry with exponential backoff using `GossipBootstrapConfig` parameters
   (`initial_delay_s`, `max_delay_s`, `multiplier`, `jitter`, `max_retries`).
3. Between each retry, check `cancel_event.is_set()` — if the transfer has been
   superseded by a new plan, abort without consuming further retries.
4. If all retries are exhausted: call `staging.cleanup()` (destination side),
   mark the handle `TransferState.FAILED`, call `WaitGroup.done(pid, False)`.
5. `WaitGroup.wait()` returns with that pid in `failed_list`. The applicator
   does **not** advance the phase while any pid is in `failed_list`.
6. When `GossipEngine` propagates a new topology epoch, `TopologyManager`
   triggers `applicator.apply(new_plan)`. The new plan re-assigns failed pids
   and the old `TransferHandle` is cancelled.

#### Edge-case summary

| Case | Planner behaviour | Applicator behaviour |
|---|---|---|
| Source PAUSED | Included in `old_replicas` (data intact) | Retry with backoff waiting for resume; FAILED after `max_retries` |
| Source unreachable (transient) | Plan unchanged | Retry with backoff; FAILED after `max_retries`; wait for new epoch |
| Destination unreachable at start | Plan unchanged | Retry with backoff; FAILED after `max_retries` |
| Destination unreachable mid-stream | Plan unchanged | Stream exception → retry with backoff from `resume_from` cursor |
| New epoch received while retrying | `apply(new_plan)` called | `cancel_event.set()`; retry loop aborts; new handle started |
| FAILED node assigned as source | Included in `old_replicas` (topology-only planner) | Retry with backoff; FAILED after `max_retries`; phase blocked; operator must recover node |
| DRAINING source with handoff | Appears in `leaving` | Reads committed LMDB directly; ignores handoff target |
| JOINING destination excluded from pf | JOINING in `entering` | Transfer proceeds; pf unaffected until READY |
### Initiator rules

| Phase      | This node is | Sends plan to | Then                         |
|------------|-------------|---------------|------------------------------|
| JOINING    | destination | sources       | sources stream chunks back   |
| DRAINING   | source      | destinations  | this node streams to each    |

### Happy path — JOIN

See the [sequence diagrams](#sequence-diagrams) section below.

1. Node transitions `IDLE → JOINING` (already implemented in proposal 004).
2. `TopologyManager` produces `old_ring` (before self) and `new_ring` (with self).
3. `RebalancePlanner.plan(old_ring, new_ring, epoch)` → `RebalancePlan`.
4. `RebalanceApplicator.apply(plan)` — for each transfer where `dst == self`:
   a. Source node is identified; `pool.acquire(src_id, src_addr)` opens TLS conn.
   b. Destination calls `staging.last_staged_key()` → `cursor`
      (`null` on first attempt; base64 INDEX key bytes on crash recovery).
   c. Send `rebalance.plan {epoch, ranges filtered to src=source,
      resume_from: cursor}` to source; await `rebalance.plan.ok`.
   d. Source calls `scan(resume_from=cursor)` and **pushes**
      `rebalance.transfer` chunks to the destination.
   e. Destination writes records via `staging(epoch).stage(tagged)` and
      accumulates SHA-256 digest over received records in memory.
   f. After `is_last = true`, send `rebalance.commit {epoch, pid, digest}`.
   g. Await `rebalance.commit.ok`; call `staging(epoch).commit()`; update
      `state.toml` (LMDB-first invariant).
5. `WaitGroup.wait()` — when all pids are COMMITTED → `JOINING → READY`.

### Happy path — DRAIN

See the [sequence diagrams](#sequence-diagrams) section below.

1. Node transitions `READY → DRAINING` (future proposal).
2. `planner.plan(ring_with_self, ring_without_self, epoch)` → plan.
3. `applicator.apply(plan)` — for each transfer where `src == self`:
   a. Source sends `rebalance.plan {epoch, transfers}` to destination; awaits
      `rebalance.plan.ok {epoch}`.
   b. Destination sends `rebalance.resume {epoch, transfer_id, pid,
      resume_from: Key.to_dict() | null}` per outgoing partition.
   c. Source calls `scan(resume_from)` and **pushes**
      `rebalance.transfer` chunks to the destination.
   d. Destination accumulates digest; sends `rebalance.commit {epoch, pid, digest}`.
   e. Source validates delta digest; replies `rebalance.commit.ok`; marks pid as sent.
4. `WaitGroup.wait()` → `DRAINING → IDLE`.

### Phase transitions driven by the rebalance

#### `JOINING → READY`

This is the primary deliverable of this proposal with respect to proposal 004.
The sequence after `WaitGroup.wait()` returns with an empty `failed_list`:

1. **State persistence first**: write `state.toml` with `phase = READY`,
   `committed_pids = [...]`, `staging_pids = []`. Phase is always persisted
   before any network action (phase-persistence-before-gossip invariant).
2. **KV socket open**: the bootstrap layer binds the KV TCP socket. The socket
   is only opened at this point — never while the node is in `JOINING`
   (KV-socket-lifecycle invariant).
3. **Gossip**: the updated `MemberPhase.READY` record is emitted to peers so
   they update their ring views and start routing KV traffic to this node.

If `WaitGroup.wait()` returns with a non-empty `failed_list`, the node remains
in `JOINING` and does **not** advance. No KV socket is opened. The operator
must resolve the blocking transfers (see `tourctl rebalance status <addr> --blocked`)
or a new gossip epoch must supersede the plan.

#### `DRAINING → IDLE`

The symmetric outcome of the DRAIN happy path. Once `WaitGroup.wait()` returns
with an empty `failed_list` on a `DRAINING` node:

1. **State persistence**: write `state.toml` with `phase = IDLE`.
2. **KV socket close**: the KV socket (which was kept open for reads during
   `DRAINING`) is closed.
3. **Gossip**: `MemberPhase.IDLE` is emitted; peers remove this node's vnodes
   from their active ring views.

The operator action that triggers `READY → DRAINING` is defined in future proposal.
This proposal only specifies the completion side: what happens when all
DRAIN transfers are committed.

### Concurrent topology changes

If a new gossip epoch is received while transfers are in-flight:

```
pids_old  = set(handles.keys())
pids_new  = {pid for r in new_plan.ranges for pid in r}   # expand RangeTransfer
cancel    = pids_old - pids_new      → cancel_event.set() for each
start     = pids_new - pids_old      → new TransferHandle + coroutine
```

Pids in the intersection continue without interruption.

### Crash recovery

On startup the node reloads `state.toml` and then reconciles its persisted
rebalance state with the current gossip topology before opening any KV socket.

#### Step 1 — Epoch comparison

```
stored_epoch  = state.toml → epoch
gossip_epoch  = TopologyManager.snapshot().epoch
```

**If `stored_epoch < gossip_epoch`** (topology advanced while the node was
down): all staging entries under `stored_epoch` are now stale and will never be
committed under the new epoch.

```
for pid in staging_pids:
    staging(stored_epoch).cleanup()   # delete STAGING tag entries for this epoch
staging_pids = []
state.toml   ← epoch=gossip_epoch, staging_pids=[]
```

After cleanup, continue to step 2 with `staging_pids = []`.

**If `stored_epoch == gossip_epoch`**: no epoch drift; proceed directly to
step 2.

#### Step 2 — Reconcile staging_pids under current epoch

For each pid in `staging_pids`:

- `staging(epoch).exists()` is `True` (staging entries present, transfer was
  interrupted mid-stream) → call `staging.last_staged_key()` to get the
  exact LMDB cursor and **resume the transfer from that position** by passing the
  cursor as `resume_from` in the negotiation message. No
  cleanup. Already-staged records survive the restart; LMDB idempotency absorbs
  any overlap at the boundary.
- `exists()` is `False` (storage already committed `STAGING → LIVE/TOMBSTONE`,
  but state.toml not yet updated before crash) → treat as committed: move pid
  to `committed_pids`, rewrite `state.toml`. Auto-healed.

#### Step 3 — Recalculate plan and check if rebalance is still needed

```
new_plan = planner.plan(old_ring, current_ring, gossip_epoch)
```

- `pid in committed_pids` → skip (idempotent).
- `pid in staging_pids` and resumed in step 2 → `Key` cursor is passed as
  `resume_from` in `rebalance.resume` (both JOIN and DRAIN destinations).
- `pid` not in `new_plan` (partition is no longer being transferred
  under the current topology — e.g., another node filled the role during the
  outage, or the destination changed) → **no action required**. Staging entries
  for this pid are cleaned up via `staging(epoch).cleanup()` and the pid is
  removed from `staging_pids`. The node does not need to complete a transfer
  that the current plan no longer includes.
- Otherwise → start fresh transfer with `resume_from=None`.

#### Step 4 — No rebalance needed at all

If `new_plan.ranges` is empty after recalculation (e.g., the cluster
re-stabilised while the node was down and all partitions are already placed
correctly), `staging_pids` and `committed_pids` are both reset to `[]`,
`state.toml` is rewritten, and the node proceeds directly to its target phase
(`READY` if JOINING, `IDLE` if DRAINING). No transfers are started, no cleanup
is needed beyond removing any residual staging entries from the aborted
previous attempt.

### Wire protocol

All wire payloads are msgpack-encoded dicts.

#### `rebalance.plan` → `rebalance.plan.ok` / `rebalance.plan.reject`

The `rebalance.plan` message serves two purposes: it communicates the transfer
assignment to the peer **and** carries the `resume_from` cursor when the sender
is the destination (JOIN scenario — see initiator rules).

The `transfers` field uses `RangeTransfer` entries — contiguous pid
ranges grouped by `(src, dst)` — instead of one entry per pid. With
`pshift=17` (131 072 partitions) a 3-node cluster produces O(nodes) range
entries, keeping the payload well below `MAX_PAYLOAD_DEFAULT` (4 MiB) regardless
of the partition shift value.

**JOIN** — sent by the JOINING destination to each source:

```json
{
  "epoch": 4,
  "transfers": [{"start_pid": 0, "end_pid": 42999, "src": "node-1", "dst": "node-3"}],
  "resume_from": null
}
```

`resume_from` is `null` on the first attempt. On crash recovery the destination
calls `staging.last_staged_key()` and base64-encodes the raw INDEX key
bytes `pid(4B BE) | hlc(12B) | keyspace | key`:

```json
{
  "epoch": 4,
  "transfers": [{"start_pid": 0, "end_pid": 42999, "src": "node-1", "dst": "node-3"}],
  "resume_from": "<base64-encoded INDEX key bytes>"
}
```

**DRAIN** — sent by the DRAINING source to each destination:

```json
{
  "epoch": 4,
  "transfers": [{"start_pid": 0, "end_pid": 42999, "src": "node-1", "dst": "node-3"}]
}
```

No `resume_from` field; the destination carries the resume cursor and returns
it in `rebalance.plan.ok`.

**Serialisation invariant — mandatory merge**: the planner MUST group contiguous
pids sharing the same `(src, dst)` into a single range entry. Two entries with
identical `(src, dst)` MUST NOT satisfy `entry_a.end_pid + 1 == entry_b.start_pid`.
This invariant is enforced at deserialisation; a violation causes a
`rebalance.plan.reject` with reason `"malformed_transfers"`. A single-partition
transfer is represented as `{"start_pid": N, "end_pid": N, ...}`.

`rebalance.plan.ok` — acknowledged by the peer:

**JOIN** (source acknowledges):
```json
{ "epoch": 4 }
```

**DRAIN** (destination acknowledges, including its resume cursor):
```json
{ "epoch": 4, "resume_from": null }
```

`resume_from` in `rebalance.plan.ok` follows the same rules as above: `null`
for a fresh start, base64 INDEX key bytes on crash recovery.

Reject reasons: `"epoch_mismatch"` or `"src_mismatch"`.

#### `rebalance.transfer` — push stream (source → destination)

`rebalance.transfer` is always a **push** from the source to the destination.
The source calls `scan(resume_from)` triggered by the `rebalance.resume` message
and streams chunks on the same `correlation_id`:

```json
{
  "epoch": 4, "transfer_id": "node-1->node-3:42", "chunk_seq": 0, "is_last": false,
  "records": [
    {"key": {"pid": 42, "ts": {...}, "addr": {...}}, "value": {...}, "tag": {...}}
  ]
}
```

When `resume_from` is a `Key`, the source positions strictly **after** it.
With `resume_from = null` the scan starts from `(pid, HLC=0)` (full transfer).
The destination stages each received `TaggedRecord` via `staging.stage(tagged)`;
write idempotency safely absorbs any overlap at the boundary. Chunks are streamed
without per-chunk ack; `is_last = true` terminates the stream.

Each source must only process transfers where `src == self.node_id` and must
validate `epoch` before starting the scan.


#### `rebalance.commit` → `rebalance.commit.ok` / `rebalance.commit.reject`

```json
{ "epoch": 4, "pid": 42, "digest": "<sha256_hex>" }
```

The digest covers the **same delta** that the source streamed: records yielded
by `scan(resume_from)` where `resume_from` is the cursor from the negotiation
(`rebalance.plan` for JOIN, `rebalance.plan.ok` for DRAIN). Both sides walk the
same sequence and hash the same bytes, so the digests converge deterministically.

Digest algorithm: SHA-256 fed record-by-record in `Namespace.TAGS` key order.
For each `TaggedRecord` yielded by `scan()`:

```
sha256.update(tagged.to_bytes(Namespace.TAGS))
```

where `tagged.to_bytes(ns)` = `key.to_bytes(ns) + value.to_bytes() + tag.to_bytes()`.
This encodes both the ordering key (TAGS layout) and the record payload in a
single deterministic bytes sequence. A mismatch triggers `rebalance.commit.reject`.

#### `rebalance.status` / `rebalance.range` wire formats

The wire schemas for both requests and responses are defined in the
[CLI contract](#cli-contract) section above. Key semantic notes:

- `trigger` is `"joining"` or `"draining"`.
- `blocked` is `true` when at least one range has a FAILED PID.
- Range-level `last_error`: first FAILED PID's error, `null` when none.
- PID-level `last_error`: per-`TransferHandle` error string, `null` when none.
- `chunks_total` is `null` on the destination side until `is_last` is received.
- `rebalance.range.response` is paginated when the range is large;
  use `after_pid`/`limit`/`has_more`/`next_pid` fields.


### Envelope-level retry semantics

Only **network-level errors** are retried (connection refused, timeout, mTLS
handshake failure, stream drop). **Process-level errors** — responses that
indicate a logical or protocol violation — are never retried because repeating
the same request would produce the same rejection. They require operator
intervention or a new gossip epoch to resolve.

| Kind sent | Expected response | Retryable? | Error class | Action |
|---|---|---|---|---|
| `rebalance.plan` | `rebalance.plan.ok` | ✅ network | timeout / conn error | Retry with backoff; `FAILED` after `max_retries` |
| `rebalance.plan` | `rebalance.plan.reject` `epoch_mismatch` | ❌ process | stale epoch | Abort immediately; wait for new gossip epoch |
| `rebalance.plan` | `rebalance.plan.reject` `src_mismatch` | ❌ process | config/logic error | Abort immediately; log ERROR |
| `rebalance.plan` | `rebalance.plan.reject` `malformed_transfers` | ❌ process | programming error | Abort immediately; log ERROR (should never occur in production) |
| `rebalance.transfer` (stream) | No per-chunk ack | ✅ network (via cursor) | stream exception / conn drop | Resume from `last_staged_key()` on next `rebalance.plan` negotiation |
| `rebalance.commit` | `rebalance.commit.ok` | ✅ network | timeout / conn error | Retry `rebalance.commit` (digest is deterministic) |
| `rebalance.commit` | `rebalance.commit.reject` | ❌ process | digest mismatch (data integrity) | Abort; log ERROR; mark FAILED; wait for new epoch |

Key rules:

- All retry loops check `cancel_event.is_set()` between attempts and abort
  immediately when set.
- `rebalance.transfer` has no per-chunk ack. Resilience is handled at the
  session level via the `resume_from` cursor: on the next `rebalance.plan`
  negotiation the destination embeds `last_staged_key()` and the source
  resumes from there. No inner retry loop runs at the chunk level.
- `rebalance.commit.reject` (digest mismatch) is a **process error**: the
  source computed a different digest from what it streamed, which indicates a
  bug in scan ordering or serialisation. Retrying would reproduce the same
  mismatch. The transfer is marked FAILED; operator investigation is required.

### Transport layer

`_ConnectionSession` is enhanced to support server-side streaming receive.
When the dispatch loop reads an envelope whose `correlation_id` is already
in-flight (same cid as a running handler), the envelope is routed to that
handler's receive queue instead of spawning a new handler. This enables
streaming handlers to call `receive()` in a loop to consume successive chunks.
Existing single-shot handlers are unaffected — they call `receive()` once and
return.

---

## Sequence diagrams

### JOIN — first transfer (no prior crash)

```mermaid
sequenceDiagram
    participant DST as JOINING node (destination)
    participant SRC as Source node (READY/DRAINING)

    Note over DST: last_staged_key() → null (no staging entries)

    DST->>SRC: rebalance.plan {epoch, ranges, resume_from: null}
    SRC-->>DST: rebalance.plan.ok {epoch}

    Note over SRC: scan(resume_from=None)<br/>yields all committed records HLC=0…N

    loop push stream
        SRC-->>DST: rebalance.transfer {chunk_seq, records, is_last: false}
        Note over DST: staging.stage(tagged)<br/>accumulate SHA-256 digest
    end
    SRC-->>DST: rebalance.transfer {records, is_last: true}

    DST->>SRC: rebalance.commit {epoch, pid=42, digest}
    Note over SRC: scan(None) → recompute digest — must match

    SRC-->>DST: rebalance.commit.ok
    Note over DST: staging.commit() → Namespace.TAGS \x01→b""<br/>state.toml: staging_pids→committed_pids
```

### DRAIN — first transfer

```mermaid
sequenceDiagram
    participant SRC as DRAINING node (source)
    participant DST as Destination node

    SRC->>DST: rebalance.plan {epoch, ranges}
    Note over DST: last_staged_key() → null (no staging entries)
    DST-->>SRC: rebalance.plan.ok {epoch, resume_from: null}

    Note over SRC: scan(resume_from=None)

    loop push stream
        SRC-->>DST: rebalance.transfer {chunk_seq, records, is_last: false}
        Note over DST: staging.stage(tagged)<br/>accumulate SHA-256 digest
    end
    SRC-->>DST: rebalance.transfer {records, is_last: true}

    DST->>SRC: rebalance.commit {epoch, pid=42, digest}
    Note over SRC: verifies delta digest

    SRC-->>DST: rebalance.commit.ok
    Note over DST: staging.commit()<br/>state.toml update
```

### Crash recovery — destination crashes mid-stream, same epoch

```mermaid
sequenceDiagram
    participant DST as JOINING node (restarted)
    participant SRC as Source node

    Note over DST: Startup: state.toml epoch=4, gossip_epoch=4<br/>→ same epoch, resume path
    Note over DST: staging(4).exists(pid=42) → True<br/>last_staged_key() → cursor C

    DST->>SRC: rebalance.plan {epoch=4, ranges, resume_from: base64(C)}
    SRC-->>DST: rebalance.plan.ok {epoch=4}

    Note over SRC: scan(resume_from=C)<br/>MDB_SET_RANGE(C) → MDB_NEXT<br/>streams only records strictly after C

    loop remaining chunks
        SRC-->>DST: rebalance.transfer {records after C, is_last: false}
        Note over DST: stage() — LMDB idempotent at boundary<br/>accumulate digest (delta only)
    end
    SRC-->>DST: rebalance.transfer {is_last: true}

    DST->>SRC: rebalance.commit {epoch=4, pid=42, digest}
    Note over SRC: scan(resume_from=C) → same delta → digest matches

    SRC-->>DST: rebalance.commit.ok
    Note over DST: staging.commit() + state.toml
```

### Crash recovery — epoch advanced while node was down

```mermaid
sequenceDiagram
    participant DST as JOINING node (restarted)
    participant SRC as Source node (new epoch)

    Note over DST: state.toml epoch=4, staging_pids=[42]<br/>gossip_epoch=5 → epoch drift

    Note over DST: staging(epoch=4).cleanup() for pid=42<br/>delete \x01\x04 entries from Namespace.TAGS+Namespace.LOG
    Note over DST: state.toml ← epoch=5, staging_pids=[]
    Note over DST: planner.plan(old_ring, ring_epoch5) → new_plan

    alt pid=42 still in new_plan
        Note over DST: last_staged_key() → null (just cleaned up)
        DST->>SRC: rebalance.plan {epoch=5, ranges, resume_from: null}
        SRC-->>DST: rebalance.plan.ok {epoch=5}
        Note over SRC: full transfer from HLC=0
        SRC-->>DST: rebalance.transfer chunks…
        DST->>SRC: rebalance.commit {epoch=5, pid=42, digest}
        SRC-->>DST: rebalance.commit.ok
    else pid=42 no longer in new_plan
        Note over DST: no transfer needed — proceed to target phase
    end
```

---

## Design decisions

### Decision: replica-set targeting — existing replicas are never transfer destinations

**Alternatives considered:** transfer to the new primary owner only (rf=1 logic).
**Chosen because:** Tourillon uses LWW: the HLC total order determines a single
authoritative version per key per replica. A node already in the old replica set
holds committed data that may be **newer** than the source's copy (it received
writes more recently, or its LWW winner is a higher HLC). Overwriting it with a
bulk transfer could regress it to older data. The planner computes
`entering = new_replicas - old_replicas` to target exclusively nodes that
genuinely lack the partition's data.

### Decision: `scan()` transfers the full `Namespace.TAGS` history, not just the latest version per key

**Alternatives considered:** transfer only the highest-HLC record per key
(LWW snapshot); or transfer only records newer than a watermark the destination
already holds.
**Chosen because:** the store is log-structured — every write appends a new
`Namespace.TAGS` entry with a unique HLC key. Transferring only the latest
version would leave the destination with an incomplete `Namespace.TAGS` history. After the rebalance, the new node participates in
incremental KV anti-entropy using its INDEX watermark as a cursor. If its
history has gaps (missing intermediate versions or intermediate tombstones), it
would re-request records that the cluster considers already-propagated, or fail
to answer peer requests correctly. Transferring the full history from HLC=0 is
the safe baseline; delta-watermark optimisation (send only records newer than
destination's HLC) is a future optimisation out of scope here.

### Decision: under-replicated cluster (rf > cluster size) — no transfer on drain

**Alternatives considered:** refuse to drain, or synthesise a transfer to an
already-covered node.
**Chosen because:** when `cluster_size < rf`, every remaining node already holds
all replicas. Adding a redundant transfer to the surviving node would either be
a no-op (data already present) or corrupt it (overwrite newer versions). The
planner emits an empty plan; the operator accepts the reduction in replication
level as a known trade-off of draining below rf.

### Decision: leaving-first pairing for source selection

**Alternatives considered:** always use `min(old_replicas)` as the single source for
all destinations.
**Chosen because:** in a leaderless AP system with async replication and LWW,
a leaving node D may have received writes **after** the last replication cycle
to stable node A. D therefore holds the **most recent LWW version** for those
keys. Transferring from A instead of D would silently regress the new node B to
an older version for those keys — recoverable only by KV anti-entropy, but not
immediately available. Pairing D directly with its replacement B ensures B
starts with D's freshest data. The pairing is deterministic —
`zip(sorted(leaving), sorted(entering))` — so every node computing the plan
independently reaches identical assignments.

### Decision: deterministic source selection — `min(old_replicas)` for unpaired destinations

**Alternatives considered:** round-robin or random source.
**Chosen because:** entering nodes that have no leaving node to pair with (pure
join, or when more nodes enter than leave) must still have a deterministic
source. `min(old_replicas)` — the lexicographically smallest non-FAILED,
non-PAUSED node_id in the old replica set — is a total order that requires no
shared state and is identical regardless of which node computes the plan. PAUSED
nodes are excluded from the `min()` selection (though they remain in
`old_replicas` for plan correctness) because selecting a PAUSED node as the
deterministic source would immediately trigger retries and risk blocking the phase
transition.

### Decision: `Version` and `Tombstone` as distinct types, not `value: bytes | None`

**Alternatives considered:** single `KVEntry` with `value: bytes | None` where
`None` signals a tombstone.
**Chosen because:** `Version` and `Tombstone` are semantically different records
with different storage layouts. A distinct `Tombstone` type prevents callers
from accidentally ignoring deleted records (the type system forces handling both
cases), enables clean pattern matching, and avoids ambiguity in the wire encoding
(`kind` discriminator rather than null-check on value). It also maps directly to
the separate LMDB operations performed for each kind during staging and compaction.

### Decision: `keyspace` and `key` as `bytes` in `Address`

**Alternatives considered:** `keyspace: str`, `key: bytes`.
**Chosen because:** the storage and transport layers are encoding-agnostic.
Callers that work with human-readable names encode to bytes before constructing
`Address`, keeping the encoding decision at the boundary where it belongs and
eliminating implicit UTF-8 assumptions inside the storage layer.

### Decision: storage-first commit order

**Alternatives considered:** write `state.toml` first.
**Chosen because:** if state.toml is written first and the process crashes before
the LMDB transaction commits, the pid appears committed but data is invisible —
silent data loss. Because both `Namespace.TAGS` and `Namespace.LOG` are written atomically
in one transaction, the LMDB-first order enables auto-healing on restart
(§ crash recovery): either both DBIs are committed and `state.toml` can safely
be updated, or neither is, and cleanup + restart is safe.

### Decision: epoch in staging tag (`STAGING` + epoch bytes)

**Alternatives considered:** UUID plan identifier.
**Chosen because:** epoch is already the topology versioning coordinate. Using
it eliminates a separate ID, links every staging entry in both `Namespace.TAGS` and
`Namespace.LOG` unambiguously to the topology event that created it, and allows
efficient orphan cleanup targeting only the entries for that epoch.

### Decision: destination always initiates plan request (JOIN), source initiates (DRAIN)

**Chosen because:** in JOIN, the joining node is the only one that knows it
wants to receive data. In DRAIN, the draining node is the only one that knows
it wants to leave, and it has the data. Letting the initiator own the plan
prevents race conditions where multiple nodes independently start the same
transfer.

### Decision: pull-based transfer with `resume_from` watermark

**Alternatives considered:** push-based streaming with cleanup-and-restart on
crash; separate pull-request message from destination to source.
**Chosen because:** the `Namespace.TAGS` layout was designed with HLC as a natural
resume cursor. Discarding partially-transferred data on crash wastes bandwidth
proportional to partition size and transfer progress. By having the destination
embed `resume_from = staging.last_staged_key()` in the negotiation message
(`rebalance.plan` for JOIN, `rebalance.plan.ok` for DRAIN), the source restarts
from the exact INDEX cursor where the crash happened without a separate round-trip.
`rebalance.transfer` remains a semantically unambiguous **push** from source to
destination in both initiator scenarios. LMDB write idempotency (`Namespace.LOG` key
includes HLC — same key, same value) absorbs any overlap at the boundary
safely. `cleanup()` is reserved exclusively for cancellations (superseded plan),
not for recoverable crashes.

### Decision: digest covers delta from `resume_from`, not the full partition

**Alternatives considered:** always digest the full partition (requires
re-reading all staged records on resume, negating the watermark benefit);
skip digest on resume (weaker integrity guarantee).
**Chosen because:** the delta digest gives an integrity guarantee over the
exact bytes exchanged in this transfer session. Records staged in a prior
session were already verified by that session's digest. The two guarantees
compose transitively. Full-partition integrity is verifiable out-of-band by
the KV anti-entropy layer after rebalance completes.

### Decision: `committed_pids` / `staging_pids` in `NodeState` / `state.toml`

**Alternatives considered:** separate rebalance manifest file.
**Chosen because:** reusing the existing `StatePersistence` / `FileStateAdapter`
path gives atomicity and fsync-on-write for free, without introducing a new
port or storage file.

### Decision: `max_concurrency` and `max_chunk_bytes` as `RebalanceApplicator` constructor params

**Chosen because:** these are operator-tunable parameters that depend on
available bandwidth and memory. Defaults (4 concurrent, 2 MiB) are safe for
typical hardware. They are wired at bootstrap time rather than read from a
config section to keep the config schema minimal.

### Decision: unreachable-destination retries before FAILED marking

**Alternatives considered:** fail the transfer immediately on first connection
error; or redirect to a substitute destination.
**Chosen because:** a transient network blip (a few seconds of packet loss,
slow TLS context startup) should not abort a multi-minute rebalance and block a
node in the JOINING phase indefinitely. Exponential backoff reuses the
`GossipBootstrapConfig` parameters already defined in proposal 004, avoiding a
separate backoff config. Redirection to a substitute destination (the way hinted
handoff works for KV writes) would be incorrect for rebalance: the destination
is not interchangeable — it is the specific node that needs to own the partition
data after the topology change. Only an epoch change (a new plan from gossip) can
legitimately change the destination; the applicator responds to that via
`cancel_event`.

### Decision: FAILED nodes included in replica-set computation (topology-only planner)

**Alternatives considered:** exclude FAILED nodes by consulting the registry in
`_replica_set()`; local runtime source substitution.
**Chosen because:** excluding FAILED nodes from `_replica_set()` introduces a
registry dependency in the planner. A FAILED node declares its state exactly
once via gossip and then goes silent. If that message has not yet reached all
nodes when they independently compute the plan for the same epoch, they would
consult different registry snapshots and produce different assignments —
breaking the determinism invariant. Keeping `_replica_set()` topology-only
(ring walk, no registry) guarantees that two nodes with identical ring snapshots
always produce identical plans. The consequence is intentional: a transfer
assigned to a FAILED source exhausts its retries, blocks the phase, and signals
that operator intervention is needed (FAILED → JOINING or FAILED → DRAINING).
Vnode expiration is out of scope.

---

## Implemented code organisation

```
tourillon/core/structure/clock.py         — HLCTimestamp, HLCClock
tourillon/core/structure/record.py        — Address, KvMetadata, Version, Tombstone
tourillon/core/machinery/namespace.py     — Namespace, Key, Pid, Address, Tag, TagKind,
                                            TagPayload, Value, TaggedRecord, KeyPartition
tourillon/core/ports/storage.py           — Storage, PartitionStore,
                                            PartitionStaging, PartitionHint Protocols
tourillon/core/storage/store.py           — PartitionStore implementation
tourillon/core/storage/staging.py         — PartitionStaging implementation
tourillon/core/storage/hint.py            — PartitionHint implementation
tourillon/core/rebalance/__init__.py
tourillon/core/rebalance/transfer.py      — RangeTransfer, PartitionTransfer,
                                            RebalancePlan, TransferState, TransferHandle
tourillon/core/rebalance/planner.py       — RebalancePlanner
tourillon/core/rebalance/applicator.py    — RebalanceApplicator
tourillon/core/rebalance/rebalancer.py    — Rebalancer (top-level wiring)
tourillon/core/services/rebalancer.py     — NodeRebalancer (server-side handler logic)
tourillon/bootstrap/handlers/rebalance.py — rebalance.plan / resume / commit / status / range
tourillon/core/ring/topology.py           — TopologyManager (ring mutations + epoch)
tourillon/core/machinery/state.py         — FileStatePersistence (committed/staging_pids)
tourillon/core/helpers/waitgroup.py       — WaitGroup[T]

tests/unit/test_planner.py
tests/unit/test_rebalancer.py
```


---

## Interfaces (informative — matches implemented code)

```python
# core/structure/record.py
type Record = Version | Tombstone

# core/machinery/namespace.py
class Namespace(StrEnum):
    LOG  = "log"    # per-key multi-version: pid | addr | hlc
    TAGS = "tags"   # chronological: pid | hlc | addr

class TagKind(Enum):
    LIVE      = b"\x00"
    TOMBSTONE = b"\x01"
    STAGING   = b"\x02"
    HINT      = b"\x03"
    STALE     = b"\x04"
    PHANTOM   = b"\xff"

@dataclass(frozen=True, order=True)
class Key:
    pid:  Pid
    ts:   HLCTimestamp
    addr: Address

    def to_bytes(self, ns: Namespace) -> bytes: ...
    def to_dict(self) -> dict[str, Any]: ...

    @classmethod
    def from_bytes(cls, data: bytes, ns: Namespace) -> "Key": ...
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Key": ...

# core/ports/storage.py
class Storage(Protocol):
    async def open_by_pid(self, pid: int) -> PartitionStore: ...
    async def close(self) -> None: ...

class PartitionStore(Protocol):
    def scan(
        self, resume_from: Key | None = None, batch_size: int = 1024
    ) -> AsyncIterator[TaggedRecord]: ...
    def staging(self, epoch: int) -> PartitionStaging: ...
    def hint(self, node_id: str) -> PartitionHint: ...

class PartitionStaging(Protocol):
    async def stage(self, tagged: TaggedRecord) -> None: ...
    async def commit(self) -> None: ...
    async def cleanup(self) -> None: ...
    async def exists(self) -> bool: ...
    async def last_staged_key(self) -> Key | None:
        """Return the Key with the maximum HLC among STAGING entries for (pid, epoch).

        Passed as resume_from in rebalance.resume — source positions strictly
        after it. Returns None when no staging entries exist.
        """
        ...

# core/rebalance/planner.py
class RebalancePlanner:
    def __init__(self, partitioner: Partitioner, node_id: str, replication_factor: int) -> None: ...
    def plan(self, old_ring: Ring, new_ring: Ring) -> list[RangeTransfer]: ...

# core/rebalance/applicator.py
class RebalanceApplicator:
    def __init__(
        self,
        node_id: str,
        total_partitions: int,
        pool: PeerClientPool,
        topology_mgr: TopologyManager,
        serializer: Serializer,
        storage: Storage,
        backoff: Backoff | None = None,
        max_concurrency: int = 4,
        max_chunk_bytes: int = 2_097_152,
    ) -> None: ...

    async def apply(self, plan: RebalancePlan) -> None: ...
    async def wait(self) -> tuple[list[str], list[str]]: ...

# core/helpers/waitgroup.py
class WaitGroup(Generic[T]):
    async def add(self, n: int) -> None: ...
    async def done(self, key: T, success: bool) -> None: ...
    async def wait(self) -> tuple[list[T], list[T]]: ...
```

---

## Test scenarios

All scenarios use in-memory adapters unless marked `[e2e]`.

| #  | Fixture                                 | Action                                               | Expected                                                          |
|----|-----------------------------------------|------------------------------------------------------|-------------------------------------------------------------------|
| 1  | Empty old ring, 1-node new ring         | `planner.plan(empty, ring_1, epoch=1)`               | `RebalancePlan` with no transfers (no source)                     |
| 2  | rf=1, 1-node old ring, 2-node new ring  | `planner.plan(ring_1, ring_2, epoch=2)`              | Each pid owned by new node generates a transfer from old node     |
| 3  | rf=1, 2-node ring, add 3rd node         | `planner.plan(ring_2, ring_3, epoch=3)`              | Only pids whose owner changes appear in plan                      |
| 4  | rf=1, 3-node ring, drop node            | `planner.plan(ring_3, ring_2, epoch=4)`              | Pids formerly owned by dropped node reassigned; others unchanged  |
| 5  | Same old and new ring                   | `planner.plan(ring, ring, epoch=1)`                  | Empty `RebalancePlan` — `ranges` is an empty tuple                |
| 6  | rf=3, 2 nodes (A,B), B draining         | `planner.plan(ring_AB, ring_A, epoch=5)`             | Empty plan — A already has full replica set; no transfer needed   |
| 7  | rf=3, 4 nodes (A,B,C,D), D draining     | `planner.plan(ring_ABCD, ring_ABC, epoch=6)`         | pid `old={A,B,D}` → `leaving=[D]`, `entering=[C]` → **D→C**      |
| 8  | rf=3, 5 nodes (A,B,C,D,E), D+E draining | `planner.plan(ring_ABCDE, ring_ABC, epoch=7)`        | pid `old={A,D,E}` → `leaving=[D,E]`, `entering=[B,C]` → **D→B, E→C** |
| 9  | Existing replica as potential dst        | D draining; A is in both old and new replicas        | A is in `stable`, NOT in `entering`; never a transfer destination |
| 10 | Join only, rf=3, stable cluster          | E joins; pid `old={A,B,C}`, `new` includes E         | `leaving=[]`, `entering=[E]` → **min(old_replicas)=A→E**                 |
| 11 | `scan()` — log-structured, full history | Key `x` written 3 times (v1, v2, tombstone) on one partition | `scan()` yields 3 records in HLC order: `Version@hlc1`, `Version@hlc2`, `Tombstone@hlc3`; `kv.get(x)` returns only `Tombstone@hlc3`; all 3 entries are visible in `Namespace.TAGS` (committed tag `b""`) and their payloads are in `Namespace.LOG` |
| 11 | Applicator, 1 transfer, mock storage    | `applicator.apply(plan)` — succeeds                  | `WaitGroup` completes; `committed_pids` includes pid              |
| 12 | Applicator with active handles          | Second `apply()` with no overlapping pids            | Old pids cancelled (`cancel_event` set), new ones started         |
| 13 | Applicator, pid in `committed_pids`     | `apply()` with that pid in plan                      | Transfer coroutine exits immediately (idempotent skip)            |
| 14 | Transfer in-flight, `cancel_event` set  | Coroutine checks cancel between chunks               | `staging.cleanup()` called; WaitGroup done(False)                 |
| 15 | Digest mismatch from source             | Source sends `rebalance.commit.reject`               | Transfer marked FAILED immediately (process error — no retry); `last_error` set; `WaitGroup.done(pid, False)` |
| 16 | Source returns `rebalance.plan.reject` with `epoch_mismatch` | Applicator calls apply | `RebalancePlan.reject` logged; transfer not started              |
| 17 | Digest determinism — ordered | Feed one `Version` then one `Tombstone` in HLC order | SHA-256 deterministic via `tagged.to_bytes(Namespace.TAGS)`; same order → same digest |
| 18 | Digest determinism — reverse | Same records in reverse order | Different digest from #17 |
| 19 | Crash recovery: same epoch, staging entries exist | `exists()` True; `last_staged_key()` → cursor bytes C | `resume_from=base64(C)` sent in `rebalance.plan` (JOIN) or `rebalance.plan.ok` (DRAIN); source calls `MDB_SET_RANGE(C)` then `MDB_NEXT`; no record at C re-sent; `cleanup()` not called |
| 20 | Crash recovery: same epoch, no staging entries   | `exists()` returns `False` | Treated as committed; moved to `committed_pids`; auto-healed |
| 21 | `rebalance.plan` handler, epoch mismatch | Source node has epoch 3; plan has epoch 4           | Responds `rebalance.plan.reject` with `reason: epoch_mismatch`    |
| 22 | `rebalance.plan` handler, src mismatch   | Plan lists `src = "other-node"` but handler is self  | Responds `rebalance.plan.reject` with `reason: src_mismatch`     |
| 23 | `rebalance.transfer` handler, full stream | Source streams N chunks with `Version`/`Tombstone`, last `is_last=true` | All records written via `staging.stage()`; `rebalance.commit` sent |
| 24 | `rebalance.status` handler — range aggregation | 1 range with 3 pids: COMMITTED, RUNNING, FAILED | 1 range entry: `state="blocked"`, `committed=1`, `running=1`, `failed=1`, non-null `last_error` |
| 25 | `rebalance.status` handler, no plan      | Applicator has no handles                            | Response with empty `ranges`, `null` epoch, `blocked=false` |
| 26 | `rebalance.range` handler — pid-level detail | Range with 998 COMMITTED + 1 RUNNING + 1 FAILED | Lists all PIDs; FAILED entry has non-null `last_error`; state `"blocked"` |
| 26 | FAILED node assigned as source | Node D is FAILED; plan assigns D→E | Applicator retries connection to D; after `max_retries` → transfer FAILED; phase blocked; waits for new epoch (operator must recover D or issue new plan) |
| 27 | PAUSED source | Source node is PAUSED; destination initiates transfer | Applicator retries with backoff; transfer proceeds when source resumes; no substitution |
| 28 | Destination unreachable, succeeds on retry | Destination refuses first 2 attempts, accepts 3rd  | Transfer completes after 2 retries; WARNING x2 then INFO success |
| 29 | Destination permanently unreachable      | All retries exhausted (`max_retries=3`)              | Transfer marked FAILED; `staging.cleanup()` called; phase NOT advanced; `WaitGroup.done(pid, False)` |
| 30 | New epoch while retrying unreachable dst | `cancel_event` set between retry 1 and retry 2      | Retry loop exits without consuming retry 2; `staging.cleanup()`; new plan's handle starts |
| 31 | DRAINING source, has handoff | Source has `PreferenceEntry.handoff != None` | Applicator connects directly to source; `scan()` reads committed store entries; handoff target never contacted |
| 32 | JOINING destination excluded from pf    | JOINING node is `entering` in plan                   | Transfer proceeds normally; `SimplePreferenceStrategy` omits JOINING from pf throughout; pf updated only after `JOINING → READY` |
| 33 | `scan(resume_from=Key C)` — exact cursor | Partition has 5 TaggedRecords; Key C points to 3rd | Source positions strictly after C; yields only records 4 and 5; record 3 is not re-sent |
| 34 | Delta digest on resume | Source streams records 4–5 after Key C | `rebalance.commit` digest matches source's `scan(Key C)` digest; `rebalance.commit.ok` sent |
| 36 | Crash recovery: epoch advanced while node was down | `state.toml epoch=4`; gossip delivers `epoch=5`; `staging_pids=[42]` | `staging(4).cleanup()` called for pid 42; `state.toml` rewritten with `epoch=5, staging_pids=[]`; transfer restarted fresh under epoch 5 with `resume_from=null` |
| 37 | Crash recovery: rebalance no longer needed after restart | Node restarts; `planner.plan(old, current)` returns empty plan | No transfers started; any residual staging entries cleaned up; node transitions directly to target phase |
| 38 | `kv.get` on READY node — O(1), no tag check | READY node with 3 committed entries for key `x` | Last `Namespace.LOG` entry returned directly; no `Namespace.TAGS` lookup performed; phase invariant guarantees tag is `b""` |
| 39 | `pshift=17`, 3-node cluster, plan payload size | `planner.plan(ring_2, ring_3, epoch=1)` with `partition_shift=17` | `len(plan.ranges)` is O(nodes) (≤ 2×vnodes_per_node entries); msgpack-serialised plan payload < 4 MiB (`MAX_PAYLOAD_DEFAULT`); `plan.expand()` yields exactly the pids whose owner changed |
| 40 | `rebalance.status` — `blocked=true`, `last_error` populated | 1 RUNNING + 1 FAILED handle | `blocked=true`; `summary={"failed":1,"running":1,...}`; FAILED entry has non-null `last_error`; RUNNING entry has `last_error=null` |
| 41 | `rebalance.status` — inactive partitions count | Node owns 131072 partitions total; 50 have `TransferHandle` | `active_partitions=50`; `inactive_partitions=131022`; `active+inactive==131072` |
| 42 | `rebalance.commit.reject` — process error, no retry | Source returns `rebalance.commit.reject` (digest mismatch) | Transfer marked FAILED immediately; `last_error` set; no retry attempted; `WaitGroup.done(pid, False)` |

---

## Exit criteria

- [ ] All test scenarios pass (`uv run pytest -m rebalance -x`).
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.
- [ ] `tourctl rebalance status <addr>` renders one line per range with correct aggregate state (OK/RUNNING/BLOCKED/DONE) for both JOINING and DRAINING nodes; `LAST_ERROR` column is populated for BLOCKED ranges.
- [ ] `tourctl rebalance range --id <range-id> <addr>` renders one line per PID; correctly shows per-pid state, chunks, bytes, age, and `last_error`.
- [ ] `tourctl rebalance range --id <range-id> <addr>` exits with code 1 when `range_id` is not found.
- [ ] `tourctl rebalance status --blocked` filters to blocked ranges only and appends the operator hint with a `tourctl rebalance range --id ...` suggestion.
- [ ] `NodeState.committed_pids` / `staging_pids` survive a round-trip through `FileStatePersistence`.
- [ ] `Version` and `Tombstone` round-trip losslessly through `.to_dict()` / `.from_dict()`.
- [ ] Digest produces identical output for identical `TaggedRecord` sequences using `tagged.to_bytes(Namespace.TAGS)`.
- [ ] `RebalancePlanner.plan()` is deterministic: calling it twice with identical rings and rf produces identical plans.
- [ ] Planner pairs leaving nodes with entering nodes before falling back to `min(old_replicas)` (leaving-first pairing invariant).
- [ ] Planner produces an empty plan when `cluster_size ≤ rf` and the source is draining.
- [ ] `scan(resume_from=Key)` positions strictly after the given Key and yields only records after it; `scan(None)` yields all committed records from HLC=0; a key written N times produces N TaggedRecords.
- [ ] `last_staged_key()` returns the raw `Namespace.TAGS` key bytes (`pid|hlc|ks|key`) of the highest-HLC staging entry, or `None` when none exist; the bytes are valid as a verbatim LMDB cursor for `MDB_SET_RANGE`.
- [ ] Crash recovery with same epoch: `last_staged_key()` is passed as `resume_from`; `cleanup()` is not called; source streams only records strictly after the cursor; boundary record is not re-sent.
- [ ] Crash recovery with epoch drift (`stored_epoch < gossip_epoch`): `cleanup()` is called for all pids in `staging_pids` under the old epoch; `state.toml` is rewritten with the new epoch; transfers restart with `resume_from=null`.
- [ ] Crash recovery when no rebalance needed: empty new plan causes any residual staging entries to be cleaned up; node transitions directly to target phase without starting any transfer.
- [ ] storage-first commit invariant is enforced: the LMDB write transaction committing both `Namespace.TAGS` (tag `b""`) and `Namespace.LOG` precedes `state.toml` update.
- [ ] Two-stage protocol: applicator sends `rebalance.plan` → awaits `rebalance.plan.ok` → destination sends `rebalance.resume` per transfer (with `resume_from: Key.to_dict() | null`); `rebalance.transfer` is always a push from source to destination; digest in `rebalance.commit` covers the delta streamed from the Key cursor.
- [ ] `_replica_set()` is topology-only (no registry): FAILED, PAUSED, READY, DRAINING, JOINING nodes with vnodes in the ring are all included; two calls with identical ring snapshots always return identical results regardless of local registry state.
- [ ] FAILED source assigned by plan: transfer exhausts retries, blocks the phase, does NOT perform local source substitution; resolution waits for operator intervention and a new epoch.
- [ ] Applicator retries unreachable destinations with exponential backoff before marking FAILED; retry loop aborts immediately on `cancel_event`; process errors (`epoch_mismatch`, `src_mismatch`, `malformed_transfers`, digest mismatch) mark the transfer FAILED immediately without consuming retries.
- [ ] `min(old_replicas)` for unpaired destinations selects the lexicographically smallest non-FAILED **and non-PAUSED** node_id; a PAUSED or FAILED node is never selected as the deterministic source.
- [ ] Phase transition (`JOINING → READY`, `DRAINING → IDLE`) is never executed while any pid remains in `WaitGroup.failed_list`.
- [ ] DRAINING source transfers: `PartitionStore.scan()` is called on the source directly; handoff target is never contacted.
- [ ] JOINING destination is absent from `SimplePreferenceStrategy` preference lists throughout the transfer window; pf is updated only after `JOINING → READY`.
- [ ] `kv.get` reads the last `Namespace.LOG` entry for `(pid, keyspace, key)` in O(1) without consulting `Namespace.TAGS`; correctness is guaranteed by the phase invariant (READY nodes have no staging entries) documented and enforced by invariant §3.

---

## Out of scope

- DRAIN trigger (`READY → DRAINING` command) — defined in a dedicated leave proposal.
- KV read/write paths (`kv.get`, `kv.put`, `kv.delete`) — separate KV proposal.
- Quorum read/write semantics and coordinator-free write acknowledgement — KV proposal.
- Hinted handoff replay (`\x02<node_id>` hints) — KV proposal.
- Vnode expiration for FAILED nodes — operator must explicitly recover a FAILED node
  (`FAILED → JOINING` or `FAILED → DRAINING`) to unblock rebalance. Automatic
  expiration of vnodes is not implemented.
- Periodic compaction of orphaned `\x01<old_epoch>` entries.
- `mdb_copy` defragmentation.
- Bandwidth throttling beyond `max_concurrent_transfers`.
- 