# Tourillon architecture

Deep reference for contributors and AI agents. Complements [`AGENTS.md`](../AGENTS.md), which holds the working rules.

> Tourillon draws on general distributed-systems literature. It is not a reimplementation of any specific product, and no such product is named anywhere in this repository. Mechanisms are described here in neutral terms: ring, partition, group, preference list, quorum, handoff, anti-entropy, transfer.

---

## 1. Packages and dependency direction

```
            ┌───────────┐
            │  tourlib  │  envelope framing, transport, wire models,
            └─────┬─────┘  ports: Serializer / Persistence / MtlsCrypto
                  │
      ┌───────────┼─────────────┐
      │                         │
┌─────▼──────┐          ┌───────▼──────┐
│ tourillon  │          │  tourclient  │  async client + service registry
│ (node)     │          └───────┬──────┘
└────────────┘                  │
                          ┌─────▼─────┐
                          │  tourctl  │  operator CLI
                          └───────────┘
```

`tourlib` never imports `tourillon`, `tourclient` or `tourctl`. Inside `tourillon`:

```
bootstrap/   composition root: deps wiring, CLI, envelope handlers
   │ depends on
core/        domain logic — talks to protocols only
   ▲ implemented by
infra/       LMDB storage, OpenTelemetry telemetry
```

`core/` must never import `infra/` or a third-party backend. Implementation choice happens exclusively in `bootstrap/deps.py`.

---

## 2. Process startup

`tourillon node start` →

1. `get_app()` builds the singleton `Application` with the default adapters: `CryptographyMtlsAdapter`, `TomlPersistence`, `MsgPackSerializer`, `LMDBStorageProvider`, `OpenTelemetryProvider`, plus two `Dispatcher` instances (peer and client).
2. `Application.load_config()` reads the node TOML and initialises logging.
3. `Application.create_manager()` derives the server and client `ssl.SSLContext` from the embedded mTLS material and constructs the `Manager`.
4. `Manager` builds the node services (section 3).
5. `Manager.serve_forever()` installs `SIGINT`/`SIGTERM` handlers, starts the node, and waits.

Shutdown is ordered: `NodeLifecycle.stop()` (client server → drain task → hint replayer → join task → gossip → peer server), then peer pool, storage, telemetry. A failing step is logged and the remaining steps still run.

Shutdown signals do not initiate a drain. Only an explicit `tourctl node drain`
request transitions a `READY` or `PAUSED` node to `DRAINING`. The command acknowledges
initiation, not completion: use `tourctl node inspect` on the peer endpoint to
observe the final `IDLE` phase. New writes (including replicated writes) are refused,
but reads and their background read repairs remain available throughout
`DRAINING`. Admitted mutations and a snapshot of detached writes and read repairs
settle before owned ranges are pushed to their replacement replicas.
The barrier does not include reads or keep expanding for newly scheduled repairs,
so continuous reads cannot prevent transfers from starting.
Hint replay and read repair continue during transfers. After transfer success,
new read repairs are disabled and existing background writes settle. Hint replay
continues until every persisted obligation is satisfied, then stops.
The node remains `DRAINING` until transfers, background writes and hints settle.
Ring changes trigger transfer replanning.

Read routing treats `readable` and `handoff` independently: a readable owner
and its handoff are both queried, with duplicate destinations queried only once.
They represent one logical replica for the read quorum. Before that preference
can satisfy an early quorum, both requests must settle; at the response deadline,
one successful response is sufficient. Version selection includes both sources,
including hinted tombstones. Write routing continues to prefer the handoff.

A failed drain stays `DRAINING` and can be retried with `node drain`; a restart
resumes a persisted drain. After successful transfers the client listener closes,
then completion publishes `IDLE`, removing the node from the ring without
terminating the process or deleting
its local data. The peer
listener and gossip remain available. On startup, any `IDLE` node with persisted
or configured seeds waits for an explicit join; without seeds it bootstraps.
No previous-phase field is used to distinguish a drained node.
Draining the last node on the ring is rejected
because there is no destination for its ranges.
At departure, at most five eligible peer addresses are persisted as restart
seeds; selection stops as soon as this bound is reached.

### Two servers, two dispatchers

| Server | Bind | Dispatcher | Traffic |
| --- | --- | --- | --- |
| Peer | `servers.peer.bind` | `peer_dispatcher()` | `gossip.*`, `replication.*`, `transfer.*`, `node.*`, `probe.*`, `inspect` |
| Client | `servers.client.bind` | `client_dispatcher()` | `kv.*`, `probe.*` |

The peer server starts first; the client server only opens once the node has joined and reached `READY`, so clients are never routed to a node that cannot serve them.

**mTLS is mandatory.** `EnvelopeServer` refuses any SSL context whose `verify_mode` is not `CERT_REQUIRED`, for both peer and client listeners.

---

## 3. Manager composition

`Manager` owns and injects:

| Component | Responsibility |
| --- | --- |
| `Partitioner` + `HashSpace(bits=128)` | Hash keys, map to partitions and groups |
| `TopologyManager` | Member registry + the ring, rebuilt on accepted updates |
| `PeerPool` | Pooled mTLS `EnvelopeClient` connections to peers |
| `SimplePlacementStrategy` | Preference lists honouring the replication factor |
| `StoreService` | KV coordination: placement, replication, quorum, conflicts |
| `NodeLifecycle` | Startup, join, transfers, servers, shutdown |
| `GossipService` / `Gossiper` | Membership dissemination and anti-entropy |
| `AntiEntropyService` | Data anti-entropy: background digest comparison between replicas |
| `TransferService` | Peer-facing partition transfer endpoint |
| `NodeInspector` | Paginated introspection for `node.inspect` |
| `HLCClock` | Hybrid logical clock, restored from storage at boot |
| `FailureDetectorProvider` | Per-peer phi-accrual detectors |

---

## 4. Key space, partitions and groups

- The hash space is 128-bit.
- The partitioner derives, from a key hash: a **partition id**, the **partition range**, and the **group** that owns it on disk.
- `pshift` controls the number of partitions (`2**pshift`), `gshift` the number of storage groups (`2**gshift`). The running defaults are `pshift=17`, `gshift=2`.
- Partitions are the unit of **placement and transfer**; groups are the unit of **storage isolation** (one LMDB environment and one session provider per group).

A node contributes **vnodes** (tokens) to the ring according to its size class:

| Size | XS | S | M | L | XL | XXL |
| --- | --- | --- | --- | --- | --- | --- |
| Tokens | 1 | 2 | 4 | 8 | 16 | 32 |

> Changing `pshift`, `gshift`, the hash function or the token derivation relocates every key. Treat it as a data-layout migration, never a routine tweak.

---

## 5. Placement and preference lists

1. Hash the `Address` (namespace + key, length-prefixed) into the hash space.
2. Map the hash to its partition.
3. Find the ring **successor vnode** of the partition's end token — that vnode's member is the natural placement.
4. Walk the ring forward collecting **distinct members** until the replication factor is satisfied.

Eligibility and roles:

- Only members in `READY`, `PAUSED` or `DRAINING` are on the ring. `IDLE`, `JOINING` and `FAILED` members are skipped.
- Each preference entry carries independent `readable` and `writable` owner
  capabilities, plus an optional `handoff` node id.
- Failure detection is **local and advisory**. Two nodes may legitimately disagree about a peer's liveness; the protocol must tolerate it.

| Owner phase | `readable` | `writable` | Seek a handoff |
| --- | --- | --- | --- |
| `READY` | true | true | Only when suspected by the local failure detector |
| `DRAINING` | true | false | Always |
| `PAUSED` | false | false | Always |

An absent failure detector is treated as alive. Suspicion does not change
`readable` or `writable`: it only requests a substitute. Handoff candidates must
be `READY`, considered alive, outside the selected owners, and distinct from
handoffs already assigned in this preference list.

Consumers must use these fields independently:

- Reads query the owner when `readable`, plus the handoff when present. The pair
  counts as one logical replica for the read quorum.
- Client writes (`kv.put` and `kv.delete`) prefer the handoff, carrying a
  delivery obligation for the owner. Without a handoff, they target the owner only when
  `writable`. Otherwise this preference has no write target.

Repair routing is unchanged for now: explicit write repair retains its
handoff-or-readable-owner selection, and background read repair targets
readable owners without a handoff. Applying `writable` to repairs is a
separate decision.

`handoff=None` can mean no substitute was needed or none was available; never
infer writability from it or from `readable`. In particular, a known `DRAINING`
owner without a substitute remains readable but is not targeted for writes.
Receivers must still enforce their actual admission state because gossip can
lag behind phase changes.

---

## 6. Read and write paths

### Write (`kv.put`, `kv.delete`)

1. The receiving node coordinates — there is no leader to forward to.
2. A `Version` record is stamped with an HLC timestamp from the local `HLCClock`.
3. The preference list is resolved for the key.
4. `replication.write` is fanned out to the targets in parallel; the local replica is applied directly.
5. A handoff receives the version and an independent delivery obligation whose
   `intent` identifies the owner. The version retains its `LIVE` or `TOMBSTONE`
   tag; the obligation is persisted in `META` atomically with the version.
6. The coordinator verifies `quorum_write` acknowledgements before accepting; request models default omitted values to 1, including deletes and write repair.

Deletes are **tombstones**, never physical removals, so a delete can win over a concurrent write by timestamp.

### Read (`kv.get`)

1. The preference list is resolved and `replication.read` is collected from the readable targets plus the local replica.
2. Visible versions are filtered; delivery obligations do not change a version's
   `LIVE` or `TOMBSTONE` nature.
3. A winner is selected by a **total order**: replica count, then HLC timestamp, then namespace, key, data, and quorum marker as deterministic tie-breaks.
4. The `quorum_read` response count is verified; request models default omitted values to 1. An empty replica response counts toward this quorum.
5. A tombstone winner is returned to the caller as "absent".
6. Replicas that answered with a missing or older version are healed in the background (**read repair**), bounded by a semaphore and deduplicated per address and node. Repair never blocks the answer.

> The ordering function must be a total order and identical on every node. If two nodes order the same candidate set differently, replicas diverge permanently.

### Resuming an under-replicated write (`kv.repair`)

A write that misses its requested `quorum_write` is **not rolled back**: the version exists wherever it landed. Rather than hide that, the failure carries an opaque **handle** designating the exact version.

1. The client hands the handle back through `kv.repair`.
2. The receiving node decodes it, recomputes the partition id from the address and refuses a mismatch or an implausibly distant timestamp — all local, before any RPC.
3. `replication.lookup` asks each replica whether it holds that exact version; a copy surviving only as a hint can serve as the source but never counts as its owner's durability.
4. Missing owners receive the version, and the requested quorum is verified again — so a repair may itself report a quorum failure, with the same handle, and be retried.

The handle makes a partial write resumable instead of pushing the operator towards a second write under a new timestamp.

### Hint persistence (write path)

New coordinated writes persist hints as separate delivery obligations:

```text
META key   := MetaKind.HINT | length(intent) | intent | hlc_hint
META value := Key.to_bytes(Keyspace.TAGS)
```

`intent` names the historical intended owner according to the emitter's local
placement view; it is not a claim that ownership can never change. The actual
network target is resolved from the current placement when replaying.

The emitter sends a record tagged `HINT` through `replication.hint`, with
required `record` and `intent` request fields, and no obligation HLC.
Responses use `replication.hint.accept` or `replication.hint.reject`.
`replication.write` carries only a normal record, without an intent.
The hint tag retains only the original
kind and payload, without a node id. The storing node generates
`hlc_hint` from its own clock and constructs the local `Hint(record, intent, hlc)`.
Incoming writes remain refused during `DRAINING`. The version's own HLC never
changes. Distinct obligations may reference the same version, even for the same
intent; receiving the record again creates a new local obligation.
Startup clock restoration includes persisted obligation timestamps, preventing
their identities from being reused after a restart with a regressed wall clock.

`PartitionGroupHint.put()` commits `TAGS`, `LOGS` and the `META`
reference in one write transaction, preserving `TagKind.HINT` in `TAGS`.
`PartitionGroupStore.put()` does not modify obligations. Transfer streams select only `LIVE`
and `TOMBSTONE` records and skip hints.

Recording a new obligation preserves an existing `LIVE` or `TOMBSTONE` tag.
The existing tag alone determines whether the version is already owned, relying
on atomic TAGS and LOGS persistence. An owned version's value is neither checked
nor rewritten. The META obligation is always recorded.

Replay scans bounded `META` pages and resolves the current owners for each
version. Each replay window contains at most `batch_size` records and is further
capped using the maximum value size against a 64 MiB value budget by default
(four maximum-size values). This conservative cap also applies to small values.
It bounds payloads per window, not total process memory: decoding, the scan page
and network serialization require additional memory.
Metadata pages and their referenced TAGS/LOGS reads use batches rather than one
executor job per entry.

Within a window, up to four destination workers run concurrently by default.
Each destination is assigned to one worker, which processes its records
sequentially and rechecks backoff before each attempt. The number of tasks is
bounded by the worker limit, not the number of obligations or replicas.
A slow destination does not prevent other workers from delivering to healthy
destinations in the window; subsequent windows and groups still await the
current window. Cancellation awaits all workers and leaves unconfirmed debt.

A missing referenced TAGS or LOGS entry is logged and skipped so later
obligations can progress; its obligation remains pending and still blocks IDLE.
Malformed obligation keys, references, tags and values are likewise logged and
skipped individually without deleting their obligations. Storage access failures
still propagate to the replay loop.
If the recorded intent is still an owner, replay writes only to that
node. If it is no longer an owner, replay asks every current owner for
`replication.exists`, carrying the exact version key. The response is only
`{"owned": bool}`: an owned copy requires both a `LIVE` or `TOMBSTONE` tag and
the exact `LOGS` key. The check does not retrieve the value; a `HINT` or `STAGING`
copy does not count. `replication.exists.accept` and `.reject` are peer-only
responses, like the other internal replication operations.

Only owners missing the version receive its original `LIVE` or `TOMBSTONE`
record, with its unchanged timestamp. Handoff destinations are not used. An
owner that is not writable may confirm an existing copy but receives no write.
If the hint holder is itself a current owner, replay uses
`PartitionGroupHint.promote()` without a dependency on `StoreService` or
`PartitionGroupStore`. Like staging activation, promotion checks existing copies
and batches missing versions in one short write transaction, without modifying
META obligations. Read-only confirmation uses a read transaction and never loads
LOGS values. `PartitionGroupStore` retains only ordinary version operations and
does not access META. No session
is held across network
calls. The lifecycle pauses
local replay writes during drain and waits for admitted local deliveries before
transfers; existing owned copies may still be confirmed while draining.

Replay deletes the exact obligation only after all required owners confirm
their copies and a fresh placement lookup returns the same owner set as before
delivery. Empty owner sets, failed checks, failed writes and changed owner sets
leave the obligation pending. Retries retain the original intent and identity;
backoff is tracked per actual destination. This stability check uses the local
view, not a globally synchronized topology.

Satisfaction batches idempotent deletes of exact obligation keys without reading
their references first. It does not modify the stored tag or `LOGS` value. A local normal
write can promote a hinted copy when the hint holder is now a writable owner;
successful delivery alone does not confer ownership.

During drain, replay runs alongside transfers. Once transfers succeed and
background writes settle, `HintReplayer.wait()` waits until no persisted hint
obligations remain, before replay stops and the node becomes `IDLE`. An
required owner that remains unavailable keeps the node `DRAINING`.
Moving pending obligations to a different hint holder is not implemented.
Staging, repair and read repair policies are unchanged.

---

## 7. Membership and failure detection

Gossip kinds:

| Kind | Purpose |
| --- | --- |
| `gossip.push` → `gossip.push.accept` | Hot-path dissemination of fresh member state |
| `gossip.ping` → `gossip.pong` | Liveness and round-trip sampling |
| `gossip.digest` → streamed `gossip.delta` | Anti-entropy reconciliation of divergent views |

`Gossiper` runs two loops: a **hot loop** for recent changes and an **anti-entropy loop** for convergence. `TopologyManager` applies accepted updates to the registry, rebuilds the ring, and signals waiters so join-time transfer planning can replan on topology change.

Every `gossip.push` carries a `hop_limit` (`gossip.hop_limit`, 3 by default). A responder that accepts at least one *newer* member relays exactly those members once more through `Gossiper.relay` with `hop_limit - 1`, skipping the sender and the members themselves; a budget of zero ends the epidemic. Relays run as background tasks (the push handler never waits on them), at most 64 are in flight, and a member is only relayed when it was newer locally, so duplicates die out on their own. `wanted` members of a `gossip.delta` (known by the initiator with a newer epoch, or unknown to the responder) are pulled back with `hop_limit = 1`. All gossip requests hold their pooled connection through `PeerPool.lease`.

Membership anti-entropy (the gossip `ae_interval` loop) and data anti-entropy (§9) are separate loops: membership state is tiny and must converge in seconds, data comparison is large and is paced over minutes.

`PhiAccrual` turns inter-arrival times of successful probes into a suspicion value; `PeerFailureDetector` drives probes per peer and exposes a status consumed by placement.

Invariants:

- Member updates are **monotonic** — a stale update must never regress a member's phase.
- Applying the same update twice is a no-op.
- A node's own record is authoritative for its own phase.

---

## 8. Partition transfers

When the ring changes, data must move. `TransferService` exposes the peer-facing endpoint:

| Operation | Meaning |
| --- | --- |
| `transfer.init` | Open or resume a session; returns staged resume keys |
| `transfer.claim` | Stream a bounded window of records to the puller |
| `transfer.chunk` | Stage a pushed chunk |
| `transfer.commit` | Atomically activate staged data |

The `core/transfer/` package splits this into a planner (what must move), a runner (drive sessions to completion), a streamer/receiver (move bytes), and an applicator (write into storage).

Required properties: **resumable** (a serial/window protocol lets an interrupted transfer continue), **idempotent** (a retried chunk neither duplicates nor drops records), and **isolated** (staged data is invisible to reads until commit).

During join, `NodeLifecycle` watches for ring changes and replans transfers until the topology settles, then promotes the node to `READY` and opens the client server.

---

## 9. Data anti-entropy

Writes, hinted handoff and transfers each leave narrow gaps where a replica
can miss a version for good: a write landing behind a transfer cursor, a hint
delivered while the ring moves, a node applying a topology update late and
accepting a write for a partition it no longer replicates. Read repair only
heals what is read. `AntiEntropyService` (`tourillon/core/services/antientropy.py`,
with its implementation components in `tourillon/core/ae/`)
closes these gaps proactively by comparing digests between replicas, whatever
caused the divergence.

### Digests

Every group maintains digest cells in its `META` keyspace, updated in the same
transaction as the data (`GroupSession.update_tags` / `_flush`):

| Key | Cell |
| --- | --- |
| `DIGEST · 0x00 · pid` (`ROOT_PREFIX`) | fold of every owned version of the partition |
| `DIGEST · 0x01 · pid · bucket` (`BUCKET_PREFIX`) | fold of the owned versions whose `wall_ms >> 22` is `bucket` (~70 min) |

A cell is `(count, xor of blake2b(tags key))`: order-independent, cheap to
update incrementally, and two equal cells mean the same set of versions with
overwhelming probability. Only owned versions (`LIVE`, `TOMBSTONE`) are
counted; staged transfer copies and hint copies are not. Since a version is
immutable and identified by its TAGS key (pid, address, HLC), comparing *sets
of keys* is enough: a value never needs comparing.

### The descent

One session compares a slice of partitions with one peer
(`core/ae/sync.py`, `PeerSync`):

1. `antientropy.roots` — the initiator sends its root cell for up to
   `pids_per_cycle` pids; the responder answers only those that differ.
2. `antientropy.buckets` — for each differing pid, page through the peer's
   bucket cells and compare them with the local ones.
3. `antientropy.range` — the bucket straddling the grace cutoff also counts
   versions too young to judge, so both sides recompute the digest of its
   eligible part only by scanning.
4. `antientropy.keys` — for each differing bucket, list version keys page by
   page and merge them against the local keys of the same window.
5. `antientropy.push` the versions the peer lacks; `antientropy.fetch` and
   apply the ones this node lacks (`put_many`, which also promotes a local
   staged or hint copy into an owned one).

The cost follows the size of the difference, not the size of the data: in
the steady state a session is one `roots` round-trip that returns nothing.

**Grace.** Versions younger than `grace` seconds (wall clock of the HLC) are
never compared: they are still travelling through the write path and the hint
replayer, and copying them would only race with those.

**Repairs are direct copies.** Missing versions are pushed or fetched directly,
not turned into hint obligations. The copy is idempotent (an existing version
is never rewritten), adds no durable state to replay, and bounded per session;
the data and its digests *are* the progress state, so a session stopped half
way loses nothing — the next sweep finds the rest because digests still differ.

### Replica mode and handoff mode

- **Replica mode** — for every co-replica `peer`, the pids both replicate
  (`ReplicaView.shared[peer]`) are compared two ways. Only one node of each
  pair initiates (`initiates()`: a hash of the sorted pair), so a pair is
  compared once per sweep, not twice.
- **Handoff mode** — this node scans its own non-empty roots
  (`root_page`), keeps the pids it does **not** replicate in the current view,
  and pushes their versions one way to every current replica (`push_only`). It
  never pulls and never deletes: the local copy stays until the GC reclaims it.
  When every replica was reached and the pid held no version still inside the
  grace window, the pid is marked with `(view hash, local root)` and skipped
  until either changes (a young version would otherwise never be pushed: the
  root does not change when it ages). Marks are in
  memory: after a restart each orphan pid is re-checked once (cheap: a `roots`
  exchange).

A version written on a pid it does not hash to is logged as foreign by the
responder; it is still accepted, since refusing it would lose data.

### Agreeing on replicas without agreeing on the ring

`ViewTracker` rebuilds a `ReplicaView` (the replica tuple of every pid, built
off the hot path and yielding every 4096 pids) whenever the ring changes,
after `settle_delay` so a burst of updates causes one rebuild. Every request
carries a `Scope`: its pids plus a 16-byte hash of the replica tuples the
sender computed for them. The responder recomputes it and refuses with
`stale_view` on mismatch, so both sides always reason about the same
replicas without needing identical rings — only the scoped pids must agree.

A ring change sets the view's *stale* signal (`wait_for_ring_change`): the
running session stops at its next request (`ViewChangedError`), the sweep is
abandoned, and a new plan is drawn on the rebuilt view.

### Scheduling at scale

Once per `sweep_period` (600 s by default) a node walks a shuffled plan of
units — replica slices of at most `pids_per_cycle` pids per initiated peer,
plus handoff slices covering the pid space — spaced by
`sweep_period / len(plan)` (at least `min_interval`, ±`jitter`), with a random
first delay. The initiator runs **one session at a time**.

With `1 << 17` partitions, RF 3 and 10,000 nodes a node replicates ~39 pids
and shares them with a few dozen peers: its plan is a few dozen replica units
(half of its pairs, since only one side initiates) plus 32 handoff units that
only read local roots unless orphans exist. That is one `roots` request every
~10–20 s per node, flat in the cluster size. Cluster-wide this is ~1,000
small requests per second spread uniformly over 10,000 nodes — each node
serves about as many as it sends, which is noise next to client traffic.
On a small cluster each unit covers more pids instead, and `min_interval`
caps the rate.

### Storm safety

- The responder serves at most `concurrency` requests at once and refuses the
  rest immediately with `busy` + `retry_after`, instead of queueing.
- Rejects carrying `retry_after` (`busy`, `not_ready`, `stale_view`) defer the
  peer for that long; transport failures and other rejects defer it with
  exponential backoff. Peers that are not `READY` or are suspected by the
  failure detector are skipped.
- Per-session budgets: `max_keys_per_cycle` keys listed and
  `max_repairs_per_cycle` versions copied (`BudgetSpentError`).
- Writes go through small transactions (`APPLY_BATCH` = 64 records, 8 MiB) and
  every scan yields to the event loop, so client `kv.put/get/delete` are never
  blocked behind a long anti-entropy transaction.
- The service starts only once the node is `READY` and stops before drain and
  shutdown; a stop waits for the requests already admitted.

### Envelope bounds and pagination

An envelope carries at most 18 MiB. Anti-entropy never refuses work for size:
senders paginate and receivers clamp.

| Request | Bound |
| --- | --- |
| `roots` | split into chunks of at most `1 << 17` pids (~4 MiB) |
| `buckets`, `keys` | `limit` clamped to 16,384 entries per page; the cursor continues |
| `fetch` | at most 16,384 keys; answers at most `min(max_bytes, 17 MiB)` of records but always at least one, and returns how many keys it `consumed` so the requester resumes |
| `push` | batches of at most `min(max_batch_bytes, 17 MiB)`, at least one record |

A maximal record (16 MiB value) always fits one batch on its own.

### Configuration

`[placement.anti_entropy]` in the node configuration (`AntiEntropyConfig`):

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | run the loop and serve requests |
| `sweep_period` | `600` | seconds to compare every unit once |
| `min_interval` | `1` | floor between two units |
| `jitter` | `0.2` | relative spread of every delay |
| `grace` | `120` | seconds before a version is compared |
| `settle_delay` | `5` | wait after a ring change before rebuilding the view |
| `concurrency` | `4` | requests served at once as responder |
| `pids_per_cycle` | `4096` | pids per unit |
| `keys_per_request` | `1024` | keys per page while descending |
| `max_batch_bytes` | `4 MiB` | payload of one push or fetch |
| `max_repairs_per_cycle` | `10000` | versions copied by one session |
| `max_keys_per_cycle` | `1000000` | keys listed by one session |

Values are only bounded below; anything an envelope cannot carry is clamped
and paginated.

### Known limits

- **GC interplay.** The GC keeps the most recent versions of each address. It
  must never collect a version that a replica could still push back, or
  anti-entropy would resurrect it; the GC must therefore collect consistently
  across replicas (for example only versions older than a horizon well beyond
  `grace` plus a sweep). The GC is not implemented yet.
- **Handoff restart.** A budget stop on a very large orphan partition
  restarts its descent from the beginning on the next sweep (copies already
  made are not redone, only rescanned).
- **Digest migration.** Data written before digests were maintained has no
  roots and is invisible to anti-entropy until rewritten.
- The responder's `range` scan is bounded by the bucket size, not by a budget.

---

## 10. Wire protocol

```
Envelope := header(32 bytes) || kind(UTF-8) || payload
header   := >B H B 16s I B 7s
            proto_version | schema_id | mode | correlation_id | payload_len | kind_len | reserved
mode     := 0 RPC | 1 fire-and-forget
```

- `kind` is lowercase `domain.action`, regex-validated; responses are `<kind>.accept` / `<kind>.reject`; transport-level problems use `transport.error`.
- Payloads are serialized by the injected `Serializer`; MsgPack is the default and declares its own `schema_id`.
- `replay_envelope()` builds a response that preserves `proto_version`, `schema_id`, `mode` and `correlation_id`.
- `EnvelopeClient` multiplexes concurrent requests over one connection, routing replies by `correlation_id` to pending futures (RPC) or queues (streams).
- Limits come from `tourlib/constant.py`: 16 MiB user payload, 18 MiB envelope ceiling, 30 s response timeout, 5 s connection timeout, 255-byte kind, 63-byte node id and namespace, 128-byte key.

Handlers are async generators registered once per kind on the peer or client dispatcher; registering a duplicate kind raises.

---

## 11. Client and CLI

`tourclient` exposes a **service registry** mapping a service name to a factory and the endpoints it is valid on:

| Service | Endpoints | Kinds |
| --- | --- | --- |
| `probe` | client, peer | `probe.ping` → `probe.pong` |
| `kv` | client, peer | `kv.put` / `kv.get` / `kv.delete` / `kv.repair` |
| `node` | peer | `node.join` / `node.drain` |
| `inspect` | peer | `node.inspect` → paginated `node.info` |

`gossip.*`, `replication.*`, `transfer.*` and `antientropy.*` are internal to the cluster and deliberately have no client service.

`tourctl` builds on those services with cyclopts commands (`probe ping`, `node inspect`, `node join`, `node drain`, `put`, `get`, `delete`, `repair`). Commands stay thin; rendering and error mapping live in `tourctl/core/`, which translates configuration, TLS/connectivity and protocol failures into actionable `SystemExit` messages instead of tracebacks.

`tourillon config ca | client | node` generates the mTLS material and the TOML configuration files that both sides consume.

---

## 12. Storage model

```
Storage ──session_provider_for(group)──▶ SessionProvider ──session(write=)──▶ Session ──batch()──▶ Batch
```

- One session provider per **group**; sessions are short-lived and scoped to read or write.
- `Session` offers `get`, `put`, `delete`, `last` and `iter`; `Batch` offers the same scheduling API resolved by one `await batch.result()`.
- Keyspaces and record layout live in `tourillon/core/models/storageschema.py`. Keys are length-prefixed byte compositions — never ad-hoc strings.
- `Storage.latest_hlc(total_groups)` is read at boot to restore the hybrid logical clock so timestamps never move backwards across restarts.

`core/storage/session.py` provides a typed `GroupSession` through
`GroupSessionManager.session(write=...)`. Its `read` and `get_many` methods
read exact versions; `get` reads the latest version and `tags` loads only tags.
`exists_many` checks owned tags and value presence in a single batch without
loading values. `stage_many` writes transfer versions without hiding active
copies; retries may replace the staging source.
`put_many` inserts versions and promotes non-owned copies without rewriting
existing values. `update_tags` maintains the per-bucket count/XOR digest
across ownership changes, including duplicate keys and repeated flushes.
Tag updates reject incomplete versions before changing any tag or digest.
The caller retains responsibility for placement, transfer authorization and
hint obligations. The manager flushes digests before committing the underlying
transaction; errors roll back records and digests together.
Store operations use managed sessions directly, without a separate store facade.
`PartitionGroupStaging` and `PartitionGroupHint` remain separate workflows:
the former owns transfer bounds, resumes and source authorization; the latter
owns delivery obligations and acknowledgements. Both receive a
`GroupSessionManager` and delegate record writes to its typed session.
Each staging activation page selects its tags and restores exactly one wrapper
with `update_tags` in the same write transaction, so another writer cannot
change its source or ownership between selection and activation.
`GroupSession` does not expose its underlying raw session. `tag_page` returns
typed tags and a physical continuation cursor, while `load_records` fetches
only selected values. `last_staged_keys` reads durable transfer progress.
`put_hint_obligation`, `has_hints`, `hint_page` and `remove_hint_obligations`
encapsulate delivery metadata without allowing arbitrary META writes.
Paged scans log and skip unreadable entries while still advancing their
cursor; no transaction stays open while the caller consumes a page.

Configuration and node state are persisted through the `Persistence` port, TOML by default, with per-path locking and atomic replace.

---

## 13. Observability

Telemetry goes through the `Telemetry` / `TelemetryProvider` ports, implemented by the OpenTelemetry adapter in `tourillon/infra/adapter/telemetry/`. The peer pool, transport and gossip paths are instrumented. Logging uses `tourillon.core.log.logger` with lazy `%s` formatting and node/correlation context on request paths.

---

## 14. Where the design is deliberately open

The following are design decisions that have **not** been settled yet; see [`backlog.md`](backlog.md) for the actionable list.

- Namespaces exist on the wire and in addressing but have no registry, quota, or access-control semantics.
- Consistency is expressed as raw per-request quorum integers; there is no named consistency-level API.
- Data anti-entropy heals replicas proactively (§9), but the version GC that bounds storage is not implemented, and its consistency constraint with anti-entropy is not enforced yet.
- Placement has no failure-domain (anti-affinity) awareness.
- Steady-state rebalancing is not implemented; transfers are driven by joins and explicit departures.
