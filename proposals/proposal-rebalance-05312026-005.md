# Proposal: Partition Rebalance

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-rebalance-05312026-005.md     -->

**Author**: Tourillon Contributors <tourillon@example.com>
**Status:** Draft
**Date:** 2026-05-31
**Sequence:** 005

---

## Summary

This proposal specifies the full partition-rebalance protocol for Tourillon: the
wire-level exchange that moves partition data between nodes during `JOINING → READY`
(receiver) and `DRAINING → IDLE` (sender) transitions. It defines the implementation of
implementations in `core/kv/store.py` (`PartitionStaging`, `PartitionHint`,
`PartitionStore`) so that domain code can stage, commit, scan, put, and tombstone
records via `BackendStorage`. It introduces `TransferHandle` and its five-state FSM
(`PENDING → RUNNING → COMMITTED / FAILED / CANCELLED`), six envelope kinds
(`node.leave`, `rebalance.plan`, `rebalance.transfer.init`, `rebalance.transfer.chunk`,
`rebalance.transfer.done`, `rebalance.commit`), crash-safety write ordering enforced by
two invariants (commit-before-announce and pid-in-staging-before-stage), and two
operator CLI commands:
- `tourctl node leave` — instructs a `READY` node to enter `DRAINING` and begin
  handing off its partitions.
- `tourctl rebalance status [--range <ID>]` — provides both range-level and pid-level
  observability from a single command: without `--range` it renders one row per
  `PartitionRange`; with `--range <N>` it renders one row per `TransferHandle` inside
  that range.

`PartitionRange` (from `core/ring/partitioner.py`) is used for both CLI display and
transfer planning to minimise wire round-trips. Data-plane transfer outcomes feed
`ProbeManager.data_fd` so that chronically unreachable rebalance targets are suspected
by the local failure detector. The proposal assumes the startup `Bootstraper` and
`TourillonCore` from proposal 001 are already present: they provide the storage facade,
dispatchers, and handler wiring that the rebalance engine closes over. No amendments to
this proposal are permitted; later proposals extend it only through new `Dispatcher`
registrations and new TOML sections.

---

## Motivation

Proposal 003 delivers gossip-based membership propagation and the `IDLE → JOINING`
transition. Before a joining node can advance to `READY` and serve KV traffic, three
further problems must be solved:

1. **Data ownership transfer** — When a node joins or drains, one or more partitions
   must be physically shipped from the current owner to the new owner. Without a
   structured handover protocol, the new node serves stale or empty reads.

2. **Crash safety** — If a node crashes mid-transfer, it must be able to resume or
   roll back on restart without inconsistency. The staging area must be durable before
   the state file is updated (write-before-announce), and the pid must appear in
   `staging_pids` before the first data byte is written (pid-before-stage).

3. **Operator visibility** — Operators need to know which partitions are being
   transferred and whether any are stuck in `FAILED` state. The ring's `PartitionRange`
   grouping makes the output scannable (O(vnodes) rows) without exposing the O(partitions)
   raw-pid list by default; a drill-down command exposes pid-level detail on demand.

Without this proposal, a multi-node Tourillon cluster cannot progress past the initial
`JOINING` phase on new nodes, and `DRAINING` nodes cannot safely decommission.

---

## CLI contract

### `tourctl node leave`

```
$ tourctl node leave <addr> [--context TEXT] [--contexts-file PATH] [--timeout TEXT]
```

Connects to the node at `<addr>` over mTLS and sends a `node.leave` envelope,
instructing the node to transition from `READY` to `DRAINING` and begin handing off its
partitions to the remaining cluster members.

The command returns immediately after the node acknowledges the leave request (i.e.
after the `READY → DRAINING` state transition has been persisted). The actual drain
(partition transfer) proceeds asynchronously on the node. Use
`tourctl rebalance status <addr>` to monitor drain progress.

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--context TEXT` | _(default context)_ | Named context from the contexts file. |
| `--contexts-file PATH` | `~/.tourillon/contexts.toml` | Contexts file override. |
| `--timeout TEXT` | `10s` | Dial + round-trip timeout (parsed by `parse_duration()`). |

**Human output:**

```
$ tourctl node leave 10.0.0.2:7100
Node node-b is now DRAINING. Monitor with:
  tourctl rebalance status 10.0.0.2:7100
```

**Error cases:**

```
$ tourctl node leave 10.0.0.2:7100
Error: connection refused (10.0.0.2:7100)
[exit 2]

$ tourctl node leave 10.0.0.2:7100
Error: node is not in READY phase (current phase: JOINING); cannot leave
[exit 3]

$ tourctl node leave 10.0.0.2:7100
Error: TLS handshake failed: certificate signed by unknown authority
[exit 2]
```

---

### `tourctl rebalance status`

```
$ tourctl rebalance status <addr> [--range <ID>] [--limit N] [--blocked] [--json]
```

Connects to the peer at `<addr>` over mTLS and sends a `rebalance.plan` request.

- **Without `--range`**: renders one output row per `PartitionRange` (range-level
  summary, O(vnodes) rows). Each row has an `ID` column for use with `--range`.
- **With `--range <ID>`**: renders one output row per `TransferHandle` inside the
  specified range (pid-level drill-down, O(partitions/vnodes) rows). `<ID>` is the
  0-based range index as shown in the `ID` column of the summary output.

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--range <ID>` | _(none)_ | Drill into the range with that `ID` (0-based, as shown in the `ID` column of the summary output). Shows one pid-level row per `TransferHandle` inside that range. |
| `--after-pid <N>` | _(none)_ | Pagination cursor: return only handles with `pid > N`. Use the last `PID` from the previous page as the next `--after-pid` value. |
| `--limit N` | 50 (range mode) / 100 (pid mode) | Maximum number of rows to return. |
| `--blocked` | false | Filter to ranges/handles that have at least one `FAILED` transfer and print an operator hint block. Exit code `2` when the node is blocked. |
| `--state` | _(all)_ | Pid mode only (requires `--range`): filter handles by state. One of `pending`, `running`, `failed`. |
| `--json` | false | Emit machine-readable JSON to stdout. |

**Range-level output (no `--range`):**

A one-line summary header is printed before the table:

```
$ tourctl rebalance status 10.0.0.1:7100

Rebalance status — node-a  (epoch 4, JOINING → READY)
Role: receiving
Summary: 1024 active partitions (888 committed, 4 running, 132 pending, 0 failed)

ID  RANGE              PENDING  RUNNING  COMMITTED  FAILED  CANCELLED
────────────────────────────────────────────────────────────────────────
 0   0-511 (512)            0        0        512       0          0
 1   512-1023 (512)       128        4        376       4          0
 2   1024-1535 (512)      512        0          0       0          0
...
```

`ID` is the 0-based range index matching the order emitted by
`Partitioner.ranges_for()` on the server side. `RANGE` is formatted as
`<start_pid>-<end_pid> (<count>)`. Wrapping ranges (where `start_pid >
end_pid`) are annotated with `[w]`:

```
ID  RANGE             PENDING  RUNNING  COMMITTED  FAILED  CANCELLED
────────────────────────────────────────────────────────────────────────
 7   1008-15 (24)[w]        0        0         24       0          0
```

**Pid-level output (`--range <ID>`):**

The same summary header is printed, scoped to the selected range:

```
$ tourctl rebalance status 10.0.0.1:7100 --range 1

Rebalance status — node-b  (epoch 4, JOINING → READY)
Role: receiving  ·  Range 1: 512-1023 (512)
Summary: 4 active partitions (1 committed, 1 running, 1 failed, 1 pending)

 PID   FROM     TO       STATE       CHUNKS    BYTES       AGE   LAST_ERROR
─────────────────────────────────────────────────────────────────────────────
 512   node-a   node-b   RUNNING     61/?      2.0 MiB      4s   —
 513   node-a   node-b   FAILED      45/?      1.5 MiB      8s   connection reset by peer
 514   node-a   node-b   PENDING     —         —            —    —
 515   node-a   node-b   COMMITTED   128       4.2 MiB     12s   —
 516   node-a   node-b   CANCELLED   —         —            8s   —
```

- `CHUNKS` — for `RUNNING`/`FAILED`: `<done>/<total>` when total is known, `<done>/?`
  while the transfer is still in progress (total is only known on receipt of
  `rebalance.transfer.done`); for `COMMITTED`: `<total>` (no slash, transfer complete);
  for `PENDING`/`CANCELLED`: `—`.
- `BYTES` — human-readable (`B`, `KiB`, `MiB`, `GiB`); `—` for `PENDING`/`CANCELLED`.
- `AGE` — wall time since the handle entered its current state; `—` for `PENDING`.

**JSON output — range mode (`--json`, no `--range`):**

```json
{
  "node_id": "node-a",
  "epoch": 4,
  "phase": "JOINING",
  "role": "receiving",
  "summary": {
    "total": 1024,
    "committed": 888,
    "running": 4,
    "pending": 132,
    "failed": 0,
    "cancelled": 0
  },
  "ranges": [
    {
      "range_index": 0,
      "owner": {
        "node_id": "node-a",
        "token": "0x000000000000000000000000000000000000000000000000000000000000000a"
      },
      "start_pid": 0,
      "end_pid": 511,
      "count": 512,
      "wraps": false,
      "pending": 0,
      "running": 0,
      "committed": 512,
      "failed": 0,
      "cancelled": 0
    }
  ],
  "limit": 50,
  "total_ranges": 8
}
```

**JSON output — pid mode (`--json --range <ID>`):**

```json
{
  "node_id": "node-b",
  "epoch": 4,
  "phase": "JOINING",
  "role": "receiving",
  "range_index": 1,
  "range_start_pid": 512,
  "range_end_pid": 1023,
  "summary": {
    "total": 4,
    "committed": 1,
    "running": 1,
    "pending": 1,
    "failed": 1,
    "cancelled": 0
  },
  "handles": [
    {
      "pid": 512,
      "from_node_id": "node-a",
      "to_node_id": "node-b",
      "state": "RUNNING",
      "chunks_done": 61,
      "chunks_total": null,
      "bytes_transferred": 2097152,
      "age_seconds": 4,
      "last_error": null
    }
  ],
  "limit": 100
}
```

**`--blocked` output:**

When `--blocked` is passed and the node has at least one `FAILED` transfer, the
table is filtered to failing handles and an operator hint block is appended:

```
$ tourctl rebalance status 10.0.0.1:7100 --blocked

Rebalance status — node-b  (epoch 4, JOINING → READY)
Role: receiving  ·  State: BLOCKED

Blocking transfers:
 PID   FROM     TO       STATE    LAST_ERROR
──────────────────────────────────────────────────────────────────────────────
 513   node-a   node-b   FAILED   connection reset by peer
 517   node-c   node-b   FAILED   source unreachable after 10 retries

Operator hint:
  - Check node-a and node-c states with 'tourctl node inspect'.
  - Either: recover the source node (FAILED → JOINING / READY),
    or change topology to trigger a new epoch and rebalance plan.
```

Exit code `2` when `--blocked` and the node has at least one FAILED transfer.
Exit code `0` when `--blocked` and no transfers are blocked (prints
`"No blocked transfers on node-b."`).

**Pagination with `--after-pid`:**

For clusters with a large `partition_shift` (e.g. `pshift=17` → up to ~43 000
pids per node), use `--after-pid` to page through results:

```
$ tourctl rebalance status 10.0.0.1:7100 --range 1 --limit 100
 PID   FROM   TO   STATE   ...
 512   ...
 ...
 611   ...    ← last row on this page

$ tourctl rebalance status 10.0.0.1:7100 --range 1 --limit 100 --after-pid 611
 PID   FROM   TO   STATE   ...
 612   ...
```

`--after-pid` is server-side: the `rebalance.plan` response only includes handles
with `pid > after_pid`, keeping each response well below `MAX_PAYLOAD_DEFAULT`.

**Error cases:**

```
$ tourctl rebalance status 10.0.0.1:7100
Error: connection refused (10.0.0.1:7100)
[exit 2]

$ tourctl rebalance status 10.0.0.1:7100
Error: node is not in JOINING or DRAINING phase; rebalance not active
[exit 3]

$ tourctl rebalance status 10.0.0.1:7100 --range 99
Error: range index 99 out of bounds (0–7)
[exit 3]

$ tourctl rebalance status 10.0.0.1:7100 --range 1 --state unknown
Error: unknown state filter "unknown"; valid values: pending, running, failed
[exit 1]
```

---

## Design

### Data model

#### `TransferState` (enum)

```
PENDING    — allocated, awaiting init handshake
RUNNING    — rebalance.transfer.init received; chunks flowing
COMMITTED  — rebalance.transfer.done received; PartitionStaging.commit() called;
             pid moved from staging_pids to committed_pids in state.toml
FAILED     — unrecoverable error; cleanup() called; operator intervention required
CANCELLED  — cleanup() called after receiving a stale-epoch signal or explicit cancel
```

#### `TransferHandle` (frozen dataclass)

Represents a single partition's transfer lifecycle on the receiver side.

```
pid               int
epoch             int               — rebalance epoch from NodeState
from_node_id      str               — sender's node_id
to_node_id        str               — receiver's node_id
state             TransferState
chunks_done       int               — number of rebalance.transfer.chunk envelopes received
chunks_total      int | None        — total chunks expected; None until rebalance.transfer.done received
bytes_transferred int               — cumulative bytes received across all chunks
started_at        float | None      — monotonic timestamp when handle left PENDING; None if still PENDING
last_error        str | None        — last exception message, populated on FAILED
```

`TransferHandle` is immutable; callers replace it atomically in `RebalanceEngine`'s
in-memory registry. `AGE` displayed in the CLI is `time.monotonic() - started_at`
(formatted as `Xs`, `Xm Ys`, etc.); `—` when `started_at is None`.

`chunks_total` is `None` for the entire duration of the transfer and is only set when
the final `rebalance.transfer.done` envelope is received (which carries the
authoritative chunk count). Pre-scanning the partition up-front would require a full
LMDB cursor scan per pid before streaming starts — O(records) — which is unacceptable
for large partitions. Displaying `61/?` in the `CHUNKS` column is therefore normal
during an active transfer.

#### `RebalancePlan` (frozen dataclass)

Computed by the receiver when it enters `JOINING` (or by an operator-initiated drain).
Contains:

```
epoch           int
node_id         str             — this node
pids            tuple[int, ...]  — full list of pids to receive (or send)
ranges          list[PartitionRange]   — grouped view for display
source_node_id  str             — peer that will send
```

#### `PartitionRange` (`core/ring/partitioner.py`)

No new fields. Used here for two purposes:
1. **Display** — `tourctl rebalance status` (without `--range`) groups per-pid state
   counters by range, rendering one row per `PartitionRange`.
2. **Transfer planning** — the sender sends one `rebalance.transfer.init` per pid but
   groups contiguous pids into the plan so the receiver can allocate all
   `TransferHandle` objects up-front before the first chunk arrives.

#### `PartitionStore` (`core/kv/store.py`)

The concrete domain implementation that wraps `BackendStorage`. Stores records using
`NamespaceKey`-encoded keys in `Namespace.LOG` (ordered by address then HLC) and
`Namespace.TAGS` (ordered by HLC then address).

Key encoding:

| Namespace | Layout |
|---|---|
| `LOG` | `pid (4B BE) \| len_ks (2B) \| keyspace \| len_key (2B) \| key \| hlc` |
| `TAGS` | `pid (4B BE) \| hlc \| len_ks (2B) \| keyspace \| len_key (2B) \| key` |

`get(addr)` scans `Namespace.TAGS` with a pid-scoped prefix and returns the record at
the highest HLC for `addr`, or `None`. `put` and `tombstone` delegate to
`BackendStorage.put()` with an appropriate `Tag` (`TagKind.LIVE` or
`TagKind.TOMBSTONE`). `scan(resume_from)` iterates `Namespace.LOG` within the pid
prefix to enumerate all committed records for the partition.

#### `PartitionStaging` (`core/kv/store.py`)

Wraps `BackendStorage` for one `(pid, epoch)` pair. Uses `TagKind.STAGING` with an
epoch-bearing payload to tag staged records, keeping them invisible to normal reads.

Key layout in staging:

```
LOG  key: same as committed LOG key
TAG  tag: TagKind.STAGING | epoch (4B BE)
```

`stage(record)` calls `BackendStorage.put()` with a `STAGING` tag.
`commit()` re-tags every staged entry to `TagKind.LIVE` or `TagKind.TOMBSTONE` using
`BackendStorage.tag()` in a single sweep, then issues a final `BackendStorage.put()` to
persist the epoch watermark. **`commit()` must complete before the caller updates
`state.toml`** (invariant §1).
`cleanup()` iterates staging entries for this `(pid, epoch)` and calls
`BackendStorage.delete()` on each.
`exists()` returns `True` if the pid-prefix scan over `TAGS` finds any `STAGING` entry.
`last_staged_key()` returns the highest-HLC `STAGING` entry key, used as a chunk
resume cursor after a crash.

#### `PartitionHint` (`core/kv/store.py`)

Wraps `BackendStorage` for one `(pid, node_id)` pair. Uses `TagKind.HINT` with a
sub-kind byte (`TagKind.LIVE.value` or `TagKind.TOMBSTONE.value`) in the payload.

`put(addr, value, meta)` calls `BackendStorage.put()` with tag
`Tag(kind=TagKind.HINT, payload=bytes([TagKind.LIVE.value[0]]))` and value bytes.
`tombstone(addr, meta)` calls `BackendStorage.put()` with tag
`Tag(kind=TagKind.HINT, payload=bytes([TagKind.TOMBSTONE.value[0]]))`.
`iter(resume_from)` iterates `Namespace.TAGS` for the pid prefix, yielding only entries
whose tag kind is `TagKind.HINT` and whose tag payload starts with the encoded node_id.

#### `RebalanceEngine` (`core/rebalance/engine.py`)

In-memory coordinator that owns the `TransferHandle` registry and drives the FSM
transitions. Injected with `Storage`, `StatePort`, and `ProbeManager`.

```python
@dataclass
class RebalanceEngine:
    storage: Storage
    state_port: StatePort
    probe_mgr: ProbeManager
    _handles: dict[int, TransferHandle]  # keyed by pid
```

Public API used by handlers and the CLI:

```python
async def allocate(self, plan: RebalancePlan) -> None: ...
async def on_init(self, pid: int, epoch: int, sender: str) -> PartitionStaging: ...
async def on_chunk(self, pid: int, records: list[Record]) -> None: ...
async def on_done(self, pid: int) -> None: ...
async def cancel(self, pid: int) -> None: ...
async def status(
    self,
    range_index: int | None = None,
    limit: int = 50,
    blocked_only: bool = False,
    state_filter: TransferState | None = None,
) -> list[RangeSummary] | list[TransferHandle]:
    """
    Without range_index: return range-level summaries (list[RangeSummary]).
    With range_index:    return pid-level handles for that range (list[TransferHandle]).
    Raises IndexError if range_index is out of bounds.
    """
    ...
```

#### `RangeSummary` (frozen dataclass)

```
range_index  int
owner        VNode
start_pid    int
end_pid      int
count        int
wraps        bool
pending      int
running      int
committed    int
failed       int
cancelled    int
```

---

### Envelope kinds

All envelope payloads are msgpack-serialised dicts.

#### `node.leave`

Sent by `tourctl node leave` to the target node. Instructs the node to transition
`READY → DRAINING` and initiate the drain plan.

**Request payload:** _(empty dict `{}`)_

**Response kind:** `node.leave.ack`
```json
{
  "node_id": "node-b",
  "phase": "DRAINING",
  "epoch": 5
}
```

If the node is not in `READY` phase the handler returns a `node.leave.error` envelope:
```json
{
  "node_id": "node-b",
  "phase": "JOINING",
  "error": "node is not in READY phase"
}
```

The CLI maps `node.leave.error` to exit code 3.

#### `rebalance.plan`

Sent by CLI (`tourctl rebalance status`) or peer-to-peer when a new node enters
`JOINING` to kick off the transfer.

**Request payload:**
```json
{
  "range_index": null,
  "limit": 50,
  "state_filter": null,
  "blocked_only": false
}
```

`range_index` is `null` when requesting a range-level summary (no `--range` flag).
When `--range <ID>` is provided, `range_index` is the integer ID and the server
returns pid-level `TransferHandle` data for that range instead of `RangeSummary` data.

**Response kind:** `rebalance.plan.response`
```json
{
  "epoch": 4,
  "node_id": "node-b",
  "source_node_id": "node-a",
  "pids": [0, 1, 2, ...],
  "ranges": [
    {"range_index": 0, "start_pid": 0, "end_pid": 511, "count": 512, ...}
  ],
  "handles": []
}
```

`ranges` is populated when `range_index` is `null`; `handles` is populated when
`range_index` is set. The peer-to-peer use case (join initiation) always sends
`range_index: null`.

#### `rebalance.transfer.init`

Sent by the **sender** to signal the start of transfer for a single pid.

```json
{
  "pid": 42,
  "epoch": 4,
  "sender_node_id": "node-a",
  "estimated_records": 1024
}
```

#### `rebalance.transfer.chunk`

Sent by the **sender** carrying a batch of records.

```json
{
  "pid": 42,
  "epoch": 4,
  "chunk_seq": 0,
  "records": [ <Record.to_dict()>, ... ],
  "resume_key": null
}
```

`resume_key` is `null` on the first chunk. For subsequent chunks it is the
`Key.to_bytes(Namespace.LOG).hex()` of the last record in the previous chunk, enabling
receiver-side crash recovery via `last_staged_key()`.

#### `rebalance.transfer.done`

Sent by the **sender** after all chunks for a pid have been transmitted.

```json
{
  "pid": 42,
  "epoch": 4,
  "total_records": 1024
}
```

On receiving this, the receiver calls `PartitionStaging.commit()` (invariant §1) then
advances the `TransferHandle` to `COMMITTED` and persists `committed_pids` in
`state.toml`.

#### `rebalance.commit`

Sent by the **coordinator** (the joining/draining node itself, or the sender peer) once
all pids have reached `COMMITTED`. Triggers the final phase transition:
- Receiver (`JOINING`): `JOINING → READY`.
- Sender (`DRAINING`): advances the drain epoch; when all pids are committed → `DRAINING → IDLE`.

```json
{
  "epoch": 4,
  "committed_pids": [0, 1, 2, ...]
}
```

---

### Core invariants

**Invariant §1 — Commit before announce:**
`PartitionStaging.commit()` **must** complete successfully before `state.toml` is
updated to move the pid from `staging_pids` to `committed_pids`. If the node crashes
between `commit()` returning and the `state.toml` write, the committed staging data
survives and the next restart can detect the discrepancy by scanning `TagKind.STAGING`
entries that are epoch-consistent with the current epoch — they are already committed
at the storage level; the `state.toml` write is idempotent.

**Invariant §2 — Pid in `staging_pids` before first `stage()` call:**
Before calling `PartitionStaging.stage()` for the first record in a pid,
`RebalanceEngine.on_init()` must persist the pid into `state.toml`'s `staging_pids`
list via `StatePort.save()`. If the node crashes before the first `stage()`, the
restart knows to clean up the staging area for this pid. If it crashes after the first
`stage()`, `last_staged_key()` provides a resume cursor.

**Invariant §3 — Epoch monotonicity:**
Transfers with an epoch older than the current `NodeState.epoch` are rejected with a
`CANCELLED` transition and an error log. Stale epoch re-attempts after a topology
change are silently dropped.

**Invariant §4 — No direct `BackendStorage` access from handlers:**
Handlers in `core/rebalance/handlers.py` **never** call `BackendStorage` directly.
All storage access is routed through `PartitionStore` (via `Storage.open_by_pid()`)
or through `PartitionStaging` and `PartitionHint` sub-contexts.

---

### Sequence / flow

#### Receiver path (`JOINING → READY`)

```
[new node N, state = JOINING]

1.  N contacts source node S (seed or designated owner) and sends rebalance.plan.
2.  S computes pids to transfer using Partitioner.ranges_for(N.node_id, ring).
3.  S sends rebalance.plan.response with the full pid list and PartitionRange grouping.
4.  N calls RebalanceEngine.allocate(plan):
      — Creates TransferHandle(pid, PENDING) for every pid in plan.pids.
      — No state.toml write yet.
5.  For each pid p (S iterates in ascending pid order):
    a. S sends rebalance.transfer.init(pid=p, epoch=E, sender_node_id=S.node_id).
    b. N.on_init(p, E, S.node_id):
         — Adds p to state.toml staging_pids (invariant §2).
         — Advances TransferHandle(p) → RUNNING.
         — Returns PartitionStaging(p, E, backend).
    c. S calls PartitionStore.scan() to iterate all committed records for p.
    d. S sends rebalance.transfer.chunk batches (each ≤ max_chunk_bytes).
    e. N.on_chunk(p, records):
         — Calls PartitionStaging.stage(record) for each record.
         — Updates chunks_received and records_transferred on the handle.
    f. S sends rebalance.transfer.done(pid=p, epoch=E, total_records=T).
    g. N.on_done(p):
         — Calls PartitionStaging.commit() (invariant §1).
         — Moves p from staging_pids to committed_pids in state.toml.
         — Advances TransferHandle(p) → COMMITTED.
         — Calls probe_mgr.record_data_success(S.node_id).
6.  Once all pids are COMMITTED, S sends rebalance.commit(epoch=E, committed_pids=[...]).
7.  N validates the committed_pids set matches its own.
8.  N transitions NodeState.phase JOINING → READY via StatePort.save().
```

#### Sender path (`DRAINING → IDLE`)

```
[draining node D, state = DRAINING]
  (entered via tourctl node leave → node.leave → READY → DRAINING transition)

1.  D computes pids to send: all pids where D is the current owner and the new owner
    is a JOINING peer (resolved via Partitioner.ranges_for(D.node_id, ring) vs
    gossip membership state).
2.  D sends rebalance.plan.response to receiver R.
3.  D iterates pids, sending init / chunks / done envelopes as in steps 5a–5f above.
4.  On successful transfer of all pids, D sends rebalance.commit.
5.  D transitions NodeState.phase DRAINING → IDLE via StatePort.save().
```

### Répliquage — politiques opérationnelles (JOINING vs DRAINING)

La politique suivante s'applique lors du calcul et de l'exécution des transferts :

- Conditions de déclenchement
  - Un pid est transféré uniquement lorsqu'il existe une différence entre `owners(pid, ring_old, RF)` et `owners(pid, ring_new, RF)` telle que :
    - le nœud qui exécute la transition se trouve dans `owners_new` mais pas dans `owners_old` → le nœud doit recevoir le pid (JOINING case), ou
    - le nœud se trouvait dans `owners_old` mais pas dans `owners_new` → le nœud doit envoyer le pid (sender case).
  - Si `owners_new` est déjà satisfait par des copies committées (i.e. au moins RF copies présentes), aucun transfert n'est planifié pour ce pid.

- Politique par défaut pour DRAINING sans remplaçant
  - Par défaut, l'entrée en `DRAINING` n'entraîne pas la réplique automatique des pids vers d'autres nœuds existants si aucun remplaçant n'a été ajouté au ring. L'opérateur peut demander explicitement une re‑réplication (`force_replicate`) pour copier les pids vers cibles choisies parmi les nœuds restants.

- Comportement JOINING et sources
  - JOINING est receiver-driven : le joining node demande un plan à une source et initie des pulls (`rebalance.transfer.init` → `chunk` → `done`) vers la source choisie. Pour chaque pid, le plan contient un `preferred_source` (un owner committé si possible). Si aucune source committée n'est disponible, le pid est marqué `missing` et nécessite intervention opérateur.

- Sélection de source
  - Les candidats sont d'abord les owners committés. Parmi eux, on préfère les nœuds sains selon `ProbeManager` et ceux ayant la plus faible charge de sortie.
  - Si aucun owner committé n'est trouvé dans la set attendue, la sélection cherchera dans `owners_old` avant de déclarer `missing`.

#### Crash recovery on restart

```
1.  On startup, the Bootstraper loads `NodeState` from `state.toml` before the
    rebalance engine resumes.
2.  If staging_pids is non-empty:
    a. For each pid in staging_pids, check PartitionStaging.exists().
    b. If exists → TransferHandle(pid, RUNNING) (resume from last_staged_key()).
    c. If not exists → TransferHandle(pid, PENDING) (re-request init from peer).
3.  committed_pids are already visible in the live storage; no action needed.
4.  If phase == JOINING and all pids are now COMMITTED, advance → READY immediately
    without re-contacting the sender.
```

---

### Error paths

| Condition | Outcome |
|---|---|
| `tourctl node leave` — node not in `READY` phase | Server returns `node.leave.error`; CLI exits 3 |
| `tourctl node leave` — connection refused / TLS failure | Exit 2 |
| `rebalance.transfer.chunk` arrives for unknown pid | Log warning; send error envelope; no state change |
| `rebalance.transfer.chunk` epoch mismatch | TransferHandle → CANCELLED; call cleanup() |
| `PartitionStaging.stage()` raises | TransferHandle → FAILED; call cleanup(); call `probe_mgr.record_data_failure(sender_node_id)` |
| `PartitionStaging.commit()` raises | TransferHandle → FAILED; staging data left intact for manual recovery; call `probe_mgr.record_data_failure(sender_node_id)` |
| Sender closes connection before `done` | TransferHandle → FAILED; `probe_mgr.record_data_failure(sender_node_id)` |
| All retries exhausted for a pid | TransferHandle → FAILED; overall rebalance stalls; operator uses `tourctl rebalance status --range <N>` to identify failed pids |
| CLI `--range <N>` out of bounds | Exit 3; message: `range index N out of bounds (0–M)` |
| Node not in JOINING or DRAINING | Exit 3; message: `node is not in JOINING or DRAINING phase; rebalance not active` |
| Connection refused / TLS failure | Exit 2 |
| Invalid `--state` filter value | Exit 1 |

---

### Transfer chunk sizing

A configurable `[rebalance]` section is added to `config.toml`:

```toml
[rebalance]
max_chunk_bytes   = "1Mi"    # max msgpack-encoded bytes per chunk envelope
chunk_concurrency = 1        # number of pids transferred in parallel (default: sequential)
transfer_timeout  = "5m"     # per-pid transfer deadline
```

`max_chunk_bytes` is parsed by `parse_bytes()` from `bootstrap/config.py`.
`transfer_timeout` is parsed by `parse_duration()`.

---

### `PartitionRange` used for transfer planning

`RebalanceEngine.allocate(plan)` receives a `RebalancePlan` that contains both `pids`
(the authoritative list) and `ranges` (the `PartitionRange` grouping). The ranges are
used exclusively for:

1. **CLI display** — `tourctl rebalance status` (no `--range`) renders one row per range
   with aggregated per-state counters; the `ID` column identifies each range.
2. **Pid scoping** — `tourctl rebalance status --range <ID>` uses that range's
   `(start_pid, end_pid)` to scope `TransferHandle` results server-side.

The sender iterates pids in pid-ascending order regardless of range boundaries;
`PartitionRange` never influences data routing or storage decisions.

---

### One `BackendStorage` per segment

The infra adapter `tourillon/infra/store/storage_adapter.py` maintains a
`dict[int, BackendStorage]` keyed by segment ID. `open_by_pid(pid)` calls
`Partitioner.segment_for(pid)` to resolve the segment, opens (or retrieves from cache)
the `BackendStorage` for that segment, and returns a `PartitionStore(pid, backend)`.
Multiple pids sharing the same segment share one `BackendStorage` instance.

The domain layer (`core/kv/store.py`) never imports `infra/` and never knows which
backend engine backs its `BackendStorage` reference. `BackendStorage` is the only
storage abstraction the domain ever touches.

---

## Design decisions

### Decision: Sequential pid transfer (chunk_concurrency = 1 default)

**Alternatives considered:** Transfer all pids for a range concurrently using
`asyncio.TaskGroup`.
**Chosen because:** Sequential transfer simplifies crash recovery (one pid in-flight at
a time; `last_staged_key()` is unambiguous) and reduces peak memory pressure on both
sender and receiver. Operators can increase `chunk_concurrency` once they are confident
in the infra adapter's durability guarantees.

---

### Decision: `commit()` sweep re-tags rather than copy-on-promote

**Alternatives considered:** Write records directly to the live namespace at `stage()`,
using a pid-epoch lock to block reads.
**Chosen because:** Staging via `TagKind.STAGING` keeps staged records invisible to
reads without a lock. The `commit()` sweep uses `BackendStorage.tag()` (tag-only
update, no value copy), which is O(staged entries) but does not require copying value
bytes. This matches the proposal 001 `BackendStorage` API surface.

---

### Decision: `rebalance.plan` doubles as both initiation and observability request

**Alternatives considered:** Separate `rebalance.status` and `rebalance.pids` envelope
kinds for the CLI; a `range_index` discriminator on the response to differentiate
range-level summaries from pid-level handles.
**Chosen because:** A single `rebalance.plan` kind with a `range_index` discriminator
(`null` → range summary, integer → pid detail) keeps the handler count low. The CLI
never participates in the data-plane transfer; it only reads state. The unified
`tourctl rebalance status [--range <ID>]` command surface mirrors the wire discriminator
exactly, removing conceptual distance between the CLI and the protocol.

---

### Decision: `data_fd` fed on transfer outcomes

**Alternatives considered:** Ignore transfer failures in `data_fd`; only feed it from
KV operations.
**Chosen because:** A rebalance sender that consistently fails to deliver chunks is
exhibiting a data-plane failure, exactly the signal `DataCircuitBreaker` is designed to
detect. Feeding `record_data_failure` on transfer errors and `record_data_success` on
`COMMITTED` transitions enables the gossip layer to route around the failing node
earlier, consistent with the intent of the dual-detector model.

---


## Interfaces (informative)

```python
# core/rebalance/engine.py

from __future__ import annotations

import enum
from dataclasses import dataclass, field

from tourillon.core.ports.storage import PartitionStaging, Storage
from tourillon.core.ports.state import StatePort
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.ring.partitioner import PartitionRange
from tourillon.core.ring.vnode import VNode
from tourillon.core.structure.record import Record


class TransferState(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class TransferHandle:
    pid: int
    epoch: int
    peer_node_id: str
    state: TransferState
    chunks_received: int = 0
    records_transferred: int = 0
    last_error: str | None = None


@dataclass(frozen=True)
class RebalancePlan:
    epoch: int
    node_id: str
    source_node_id: str
    pids: tuple[int, ...]
    ranges: list[PartitionRange]


@dataclass(frozen=True)
class RangeSummary:
    range_index: int
    owner: VNode
    start_pid: int
    end_pid: int
    count: int
    wraps: bool
    pending: int
    running: int
    committed: int
    failed: int
    cancelled: int


@dataclass
class RebalanceEngine:
    storage: Storage
    state_port: StatePort
    probe_mgr: ProbeManager
    _handles: dict[int, TransferHandle] = field(default_factory=dict)
    _plan: RebalancePlan | None = None

    async def allocate(self, plan: RebalancePlan) -> None:
        """Initialise TransferHandle(PENDING) for every pid in plan."""
        ...

    async def on_init(
        self, pid: int, epoch: int, sender_node_id: str
    ) -> PartitionStaging:
        """Persist pid to staging_pids, advance handle PENDING→RUNNING, return staging context."""
        ...

    async def on_chunk(self, pid: int, records: list[Record]) -> None:
        """Stage records; update chunks_received and records_transferred."""
        ...

    async def on_done(self, pid: int) -> None:
        """commit(); move pid to committed_pids; advance handle RUNNING→COMMITTED."""
        ...

    async def cancel(self, pid: int) -> None:
        """cleanup(); advance handle →CANCELLED."""
        ...

    async def status(
        self,
        range_index: int | None = None,
        limit: int = 50,
        blocked_only: bool = False,
        state_filter: TransferState | None = None,
    ) -> list[RangeSummary] | list[TransferHandle]:
        """
        Without range_index: return range-level summaries (list[RangeSummary]).
        With range_index: return pid-level handles for that range (list[TransferHandle]).
        Raises IndexError if range_index is out of bounds.
        """
        ...
```

```python
# core/kv/store.py  (fleshed-out PartitionStore)

from __future__ import annotations

from collections.abc import AsyncIterator

from tourillon.core.ports.storage import BackendStorage
from tourillon.core.structure.machinery import (
    Key,
    Namespace,
    Pid,
    Tag,
    TagKind,
    TaggedRecord,
    Value,
)
from tourillon.core.structure.record import Address, KvMetadata, Record


class PartitionStore:
    def __init__(self, pid: int, backend: BackendStorage) -> None:
        self._pid = pid
        self._backend = backend

    def scan(self, resume_from: Key | None = None) -> AsyncIterator[Record]:
        """Iterate Namespace.LOG within pid prefix; yield committed Records."""
        ...

    def staging(self, epoch: int) -> PartitionStaging:
        return PartitionStaging(self._pid, epoch, self._backend)

    def hint(self, node_id: str) -> PartitionHint:
        return PartitionHint(self._pid, node_id, self._backend)

    async def get(self, addr: Address) -> Record | None:
        """Scan Namespace.TAGS for addr; return record at highest HLC."""
        ...

    async def put(self, addr: Address, value: bytes | None, meta: KvMetadata) -> None:
        """Write Version(TagKind.LIVE) via BackendStorage.put()."""
        ...

    async def tombstone(self, addr: Address, meta: KvMetadata) -> None:
        """Write Tombstone(TagKind.TOMBSTONE) via BackendStorage.put()."""
        ...
```

```python
# core/rebalance/handlers.py

from __future__ import annotations

from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.types import ReceiveEnvelope, SendEnvelope
from tourillon.core.rebalance.engine import RebalanceEngine


def register(dispatcher: Dispatcher, engine: RebalanceEngine) -> None:
    @dispatcher.on("node.leave")
    async def handle_node_leave(
        receive: ReceiveEnvelope, send: SendEnvelope
    ) -> None:
        ...

    @dispatcher.on("rebalance.plan")
    async def handle_rebalance_plan(
        receive: ReceiveEnvelope, send: SendEnvelope
    ) -> None:
        ...

    @dispatcher.on("rebalance.transfer.init")
    async def handle_transfer_init(
        receive: ReceiveEnvelope, send: SendEnvelope
    ) -> None:
        ...

    @dispatcher.on("rebalance.transfer.chunk")
    async def handle_transfer_chunk(
        receive: ReceiveEnvelope, send: SendEnvelope
    ) -> None:
        ...

    @dispatcher.on("rebalance.transfer.done")
    async def handle_transfer_done(
        receive: ReceiveEnvelope, send: SendEnvelope
    ) -> None:
        ...

    @dispatcher.on("rebalance.commit")
    async def handle_rebalance_commit(
        receive: ReceiveEnvelope, send: SendEnvelope
    ) -> None:
        ...
```

```python
# infra/store/storage_adapter.py

from __future__ import annotations

from tourillon.core.ports.storage import BackendStorage, PartitionStore, Storage
from tourillon.core.kv.store import PartitionStore as ConcretePartitionStore
from tourillon.core.ring.partitioner import Partitioner


class StorageAdapter:
    """Implements Storage. One BackendStorage per segment, cached."""

    def __init__(
        self,
        partitioner: Partitioner,
        backend_factory: Callable[[int], BackendStorage],
    ) -> None:
        self._partitioner = partitioner
        self._factory = backend_factory
        self._cache: dict[int, BackendStorage] = {}

    def open_by_pid(self, pid: int) -> PartitionStore:
        segment = self._partitioner.segment_for(pid)
        if segment not in self._cache:
            self._cache[segment] = self._factory(segment)
        return ConcretePartitionStore(pid, self._cache[segment])
```

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.

| # | Mark | Fixture | Action | Expected |
|---|------|---------|--------|----------|
| 1 | unit | `PartitionStore` backed by `InMemoryBackend`; `pid=0` | `put(addr, b"hello", meta)` then `get(addr)` | Returns `Version` with `value=b"hello"` and matching HLC. |
| 2 | unit | `PartitionStore` backed by `InMemoryBackend`; `pid=0` | `tombstone(addr, meta)` then `get(addr)` | Returns `Tombstone` with matching HLC. |
| 3 | unit | `PartitionStore`; two records at different HLC for same addr | `get(addr)` | Returns the record with the higher HLC. |
| 4 | unit | `PartitionStore`; 5 records across `pid=0` | `scan()` | Yields exactly 5 committed `Record` objects in key order. |
| 5 | unit | `PartitionStaging`; `pid=1`, `epoch=3` | `stage(record)` then `exists()` | `exists()` returns `True`. |
| 6 | unit | `PartitionStaging`; one staged record | `commit()` then `InMemoryBackend` tag scan | Staged entry tag is `TagKind.LIVE` (no longer `STAGING`) after commit. |
| 7 | unit | `PartitionStaging`; one staged record | `cleanup()` then `exists()` | `exists()` returns `False` after cleanup. |
| 8 | unit | `PartitionStaging`; 3 staged records | `last_staged_key()` | Returns key of record with highest HLC among staged entries. |
| 9 | unit | `PartitionHint`; `pid=2`, `node_id="node-b"` | `put(addr, b"v", meta)` then `iter()` | Yields one `TaggedRecord` with `tag.kind == TagKind.HINT`. |
| 10 | unit | `PartitionHint`; hint for tombstone | `tombstone(addr, meta)` then `iter()` | Yields one `TaggedRecord` whose hint sub-kind byte is `TagKind.TOMBSTONE.value[0]`. |
| 11 | unit | `RebalanceEngine`; `plan` with 3 pids | `allocate(plan)` | All 3 pids have `TransferHandle.state == PENDING`. |
| 12 | unit | `RebalanceEngine`; pid=0 in PENDING | `on_init(0, epoch=1, "node-a")` | `TransferHandle.state == RUNNING`; pid 0 in `state.toml staging_pids`. |
| 13 | unit | `RebalanceEngine`; pid=0 in RUNNING | `on_chunk(0, [record1, record2])` | `TransferHandle.records_transferred == 2`; records visible in staging. |
| 14 | unit | `RebalanceEngine`; pid=0 in RUNNING | `on_done(0)` | `TransferHandle.state == COMMITTED`; pid 0 in `committed_pids`; not in `staging_pids`. |
| 15 | unit | `RebalanceEngine`; pid=1 in RUNNING | `cancel(1)` | `TransferHandle.state == CANCELLED`; staging entries for pid=1 are absent. |
| 16 | unit | `RebalanceEngine`; stale epoch chunk (epoch=0, current=2) | `on_init(0, epoch=0, "node-a")` | Raises `ValueError`; `TransferHandle.state == CANCELLED`. |
| 17 | unit | `RebalanceEngine`; 4 pids across 2 ranges; 2 COMMITTED, 1 FAILED, 1 PENDING | `status(range_index=None, limit=10)` | Returns 2 `RangeSummary` objects; per-state counters match. |
| 18 | unit | `RebalanceEngine`; pids 0–3 across range 0; pids 4–7 across range 1 | `status(range_index=1, limit=2)` | Returns exactly 2 `TransferHandle` objects with pids in range 1. |
| 19 | unit | `StorageAdapter`; `Partitioner` with `partition_shift=4`, `segment_shift=2` | `open_by_pid(0)` and `open_by_pid(1)` (same segment) | Both calls return `PartitionStore` objects backed by the **same** `BackendStorage` instance. |
| 20 | unit | `StorageAdapter`; `open_by_pid(0)` (segment 0) and `open_by_pid(4)` (segment 1) | Two calls | Each call returns a `PartitionStore` backed by a **different** `BackendStorage` instance. |
| 21 | unit | `RebalanceEngine`; on_done raises `StorageError` from `commit()` | `on_done(pid=0)` | `TransferHandle.state == FAILED`; `probe_mgr.record_data_failure("node-a")` called once. |
| 22 | unit | `RebalanceEngine`; pid=0 transitions RUNNING→COMMITTED | `on_done(0)` | `probe_mgr.record_data_success("node-a")` called once. |
| 23 | unit | `RebalancePlan` with `ranges`; `status()` with `range_index=99` out of bounds | `status(range_index=99)` | Raises `IndexError` with message containing `"out of bounds"`. |
| 24 | unit | `PartitionStaging.commit()` called then `state.toml` written (invariant §1 check) | Mock `StatePort.save` records call order | `commit()` observed before `save()` in call log. |
| 25 | unit | `RebalanceEngine.on_init()` called; mock `StatePort.save` records call order | `on_init(pid=0, ...)` | `StatePort.save()` (adding pid to `staging_pids`) called before first `stage()` invocation (invariant §2). |
| 26 | unit | `handle_node_leave` handler; node in `READY` phase | Dispatch `node.leave` envelope | Node transitions to `DRAINING`; response kind is `node.leave.ack` with `phase: "DRAINING"`. |
| 27 | unit | `handle_node_leave` handler; node in `JOINING` phase | Dispatch `node.leave` envelope | Response kind is `node.leave.error` with `error` field; node phase unchanged. |
| 28 | e2e | Two in-process nodes; node B in JOINING, node A in READY with 256 pids | Full rebalance.plan → init → chunk → done → commit exchange | Node B transitions to READY; all 256 pids COMMITTED; node A sends no further chunks. |
| 29 | e2e | Node B JOINING; connection drops after 50% of chunks; node B restarts | Resume from `last_staged_key()` | Transfer completes; duplicate records are not double-committed; final record count matches source. |
| 30 | e2e | Node A in READY; `tourctl node leave` sent | `node.leave` → DRAINING; then full drain rebalance | Node A transitions DRAINING → IDLE after all pids committed; `tourctl rebalance status` shows 100% COMMITTED. |

| 31 | unit | RF=3; nodes A,B,C; operator requests `tourctl node leave C` (no replacement) | Dispatch `node.leave` on C | No transfers scheduled by default; cluster RF temporarily reduced to 2; `tourctl rebalance status` shows zero active transfers. |
| 32 | unit | RF=2; nodes A,B; node C joins (JOINING) and requests rebalance | Full rebalance.plan → init → chunk → done → commit | C pulls missing pids from A or B; after completion, new_owners including C are COMMITTED where source copies existed. |
| 33 | unit | RF=3; nodes A,B,C; node D joins (JOINING) | Full rebalance plan and selective transfer | D pulls only the subset of pids for which it becomes owner; transfers sourced from A/B/C; `tourctl rebalance status` reflects COMMITTED pids on D. |

---

## Exit criteria

- [ ] All 30 test scenarios pass (`uv run pytest -m "unit or e2e" -x`).
- [ ] `uv run pytest --cov=tourillon --cov=tourctl --cov-fail-under=90` passes.
- [ ] `uv run ruff check tourillon/ tourctl/ tests/` passes with zero violations.
- [ ] `uv run black --check tourillon/ tourctl/ tests/` passes.
- [ ] `core/kv/store.py` — `PartitionStore`, `PartitionStaging`, and `PartitionHint`
  all methods are fully implemented.
- [ ] `core/rebalance/engine.py` — `RebalanceEngine` enforces invariant §1: `commit()` is
  called and awaited before `StatePort.save()` is called to move the pid to `committed_pids`.
- [ ] `core/rebalance/engine.py` — `RebalanceEngine` enforces invariant §2: `StatePort.save()`
  is called to add the pid to `staging_pids` before the first `PartitionStaging.stage()` call.
- [ ] `core/rebalance/engine.py` — `RebalanceEngine.on_init()` rejects transfers with
  `epoch < NodeState.epoch` by advancing the handle to `CANCELLED` and raising `ValueError`.
- [ ] `core/rebalance/handlers.py` — `register(dispatcher, engine)` registers exactly **six**
  handlers using `@dispatcher.on(kind)` for the six envelope kinds specified above
  (`node.leave`, `rebalance.plan`, `rebalance.transfer.init`, `rebalance.transfer.chunk`,
  `rebalance.transfer.done`, `rebalance.commit`).
- [ ] `infra/store/storage_adapter.py` — `StorageAdapter.open_by_pid()` calls
  `Partitioner.segment_for(pid)` and caches one `BackendStorage` per unique segment;
  pids sharing a segment return a `PartitionStore` backed by the same instance.
- [ ] No module under `tourillon/core/` imports `infra/`, `msgpack`, `ssl`, or
  `tomllib`/`tomli_w` directly.
- [ ] `tourctl node leave` exits with code 3 when the target node is not in `READY` phase.
- [ ] `tourctl node leave` exits with code 2 on connection refused, TLS failure, or timeout.
- [ ] `tourctl rebalance status` exits with code 3 when the target node is not in JOINING or
  DRAINING phase.
- [ ] `tourctl rebalance status` exits with code 2 on connection refused, TLS failure, or
  timeout.
- [ ] `tourctl rebalance status --range <N>` exits with code 3 when N is out of bounds.
- [ ] `tourctl rebalance status --range <N> --state <invalid>` exits with code 1.
- [ ] `tourctl rebalance status --json` (no `--range`) emits valid JSON with the `ranges`
  top-level key.
- [ ] `tourctl rebalance status --range <N> --json` emits valid JSON with the `handles`
  top-level key and a `range_index` field.
- [ ] Wrapping ranges (`start_pid > end_pid`) rendered with `[w]` annotation in human
  output of `tourctl rebalance status`.
- [ ] `record_data_failure(peer_node_id)` called on `ProbeManager` when `commit()` raises or
  sender closes connection before `done`; `record_data_success(peer_node_id)` called on
  every `COMMITTED` transition.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

- KV read / write operations and hinted-handoff replay — covered by a later proposal.
- Garbage collection of stale staging entries — covered by a later proposal.
- `PAUSED` interaction with in-progress rebalance transfers — covered by a later proposal.
- Concurrent multi-pid transfer (`chunk_concurrency > 1`) crash-recovery — deferred;
  the default is sequential (1).
- Automatic retry of `FAILED` transfers — the operator is expected to drain and rejoin
  the node; automatic retry logic is deferred to a future operations proposal.
- Cluster-wide rebalance coordination (triggering rebalance across all nodes) — this
  proposal covers only the two-node handover protocol; a future cluster-manager proposal
  will orchestrate multi-hop rebalances.
- Storage backend selection and configuration (e.g. LMDB vs RocksDB) — the infra adapter
  is specified here but the choice of concrete `BackendStorage` implementation is left to
  the deployment configuration.
- Cross-datacenter rebalance throttling — out of scope; future network-policy proposal.
