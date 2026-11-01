# Tourillon architecture

Deep reference for contributors and AI agents. Complements [`AGENTS.md`](../AGENTS.md), which holds the working rules.

> Tourillon draws on general distributed-systems literature. It is not a reimplementation of any specific product, and no such product is named anywhere in this repository. Mechanisms are described here in neutral terms: ring, partition, group, preference list, quorum, handoff, anti-entropy, replication obligation.

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

Shutdown is ordered: `NodeLifecycle.stop()` (client server → drain and join tasks → anti-entropy → gossip → peer server), then peer pool, storage, telemetry. A failing step is logged and the remaining steps still run.

Shutdown signals do not initiate a drain. Only an explicit `tourctl node drain`
request transitions a `READY` or `PAUSED` node to `DRAINING`. The command acknowledges
initiation, not completion: use `tourctl node inspect` on the peer endpoint to
observe the final `IDLE` phase. New writes (including replicated writes) are refused,
but reads and their background read repairs remain available throughout
`DRAINING`. Admitted mutations and a snapshot of detached writes and read repairs
settle before anti-entropy hands the stored partitions to their replacement
replicas (§8). The barrier does not include reads or keep expanding for newly
scheduled repairs, so continuous reads cannot prevent the handoff from starting.
Obligation delivery and read repair continue meanwhile. Once the handoff goal
is reached, new read repairs are disabled and existing background writes settle.
The node remains `DRAINING` until then; ring changes make anti-entropy redo the
affected partitions on the new view.

Read routing treats `readable` and `handoff` independently: a readable owner
and its handoff are both queried, with duplicate destinations queried only once.
They represent one logical replica for the read quorum. Before that preference
can satisfy an early quorum, both requests must settle; at the response deadline,
one successful response is sufficient. Version selection includes both sources,
including tombstones held by the handoff. Write routing continues to prefer the handoff.

A failed drain stays `DRAINING` and can be retried with `node drain`; a restart
resumes a persisted drain. After a successful handoff the client listener closes,
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
| Peer | `servers.peer.bind` | `peer_dispatcher()` | `gossip.*`, `replication.*`, `antientropy.*`, `node.*`, `probe.*`, `inspect` |
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
| `NodeLifecycle` | Startup, join and drain goals, servers, shutdown |
| `GossipService` / `Gossiper` | Membership dissemination and anti-entropy |
| `AntiEntropyService` | The only mechanism copying versions between nodes: obligation delivery, sweep, join and drain goals |
| `ObligationQueue` | Bounded in-memory hotqueue of durable replication obligations, shared by the store and anti-entropy |
| `NodeInspector` | Paginated introspection for `node.inspect` |
| `HLCClock` | Hybrid logical clock, restored from storage at boot |
| `FailureDetectorProvider` | Per-peer phi-accrual detectors |

---

## 4. Key space, partitions and groups

- The hash space is 128-bit.
- The partitioner derives, from a key hash: a **partition id**, the **partition range**, and the **group** that owns it on disk.
- `pshift` controls the number of partitions (`2**pshift`), `gshift` the number of storage groups (`2**gshift`). The running defaults are `pshift=17`, `gshift=2`.
- Partitions are the unit of **placement and anti-entropy**; groups are the unit of **storage isolation** (one LMDB environment and one session provider per group).

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

- Members in `JOINING`, `READY`, `PAUSED` or `DRAINING` are on the ring. `IDLE` and `FAILED` members are skipped.
- Each preference entry carries independent `readable` and `writable` owner
  capabilities, plus an optional `handoff` node id.
- Failure detection is **local and advisory**. Two nodes may legitimately disagree about a peer's liveness; the protocol must tolerate it.

| Owner phase | `readable` | `writable` | Seek a handoff |
| --- | --- | --- | --- |
| `READY` | true | true | Only when suspected by the local failure detector |
| `JOINING` | false | false | Always |
| `DRAINING` | true | false | Always |
| `PAUSED` | false | false | Always |

An absent failure detector is treated as alive. Suspicion does not change
`readable` or `writable`: it only requests a substitute. Handoff candidates must
be `READY`, considered alive, outside the selected owners, and distinct from
handoffs already assigned in this preference list. A `JOINING` owner is the
target owner of its partitions but serves no ordinary request until its
lifecycle reaches `READY`: reads go to its handoff (usually the owner it
displaces) and writes reach it through an obligation held by that handoff.

Consumers must use these fields independently:

- Reads query the owner when `readable`, plus the handoff when present. The pair
  counts as one logical replica for the read quorum.
- Client writes (`kv.put` and `kv.delete`) prefer the handoff, carrying a
  delivery obligation for the owner. Without a handoff, they target the owner only when
  `writable`. Otherwise this preference has no write target.

Explicit write repair retains its handoff-or-readable-owner selection.
Background read repair never writes to a laggard itself: it asks a node that
holds the winning version to record an obligation for the laggard (see §6).

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
5. A handoff receives the version through `replication.hint` with an `intent`
   naming the owner. It stores the plain `LIVE` or `TOMBSTONE` version and a
   replication obligation for the owner in `META`, in one transaction.
   When a direct replica write fails, the coordinator records the same
   obligation locally (see *Replication obligations* below).
6. The coordinator verifies `quorum_write` acknowledgements before accepting; request models default omitted values to 1, including deletes and write repair.

Deletes are **tombstones**, never physical removals, so a delete can win over a concurrent write by timestamp.

### Read (`kv.get`)

1. The preference list is resolved and `replication.read` is collected from the readable targets plus the local replica.
2. Only `LIVE` and `TOMBSTONE` versions are visible; obligations never change
   a version's tag.
3. A winner is selected by a **total order**: replica count, then HLC timestamp, then namespace, key, data, and quorum marker as deterministic tie-breaks.
4. The `quorum_read` response count is verified; request models default omitted values to 1. An empty replica response counts toward this quorum.
5. A tombstone winner is returned to the caller as "absent".
6. Replicas that answered with a missing or older version are healed in the background (**read repair**), bounded by a semaphore and deduplicated per address and node. Read repair is a producer of obligations: the coordinator records one locally when it holds the winner, otherwise it sends `replication.hint` to a node that answered with the winner, with the laggard as `intent`. Anti-entropy delivers it. Repair never blocks the answer.

> The ordering function must be a total order and identical on every node. If two nodes order the same candidate set differently, replicas diverge permanently.

### Resuming an under-replicated write (`kv.repair`)

A write that misses its requested `quorum_write` is **not rolled back**: the version exists wherever it landed. Rather than hide that, the failure carries an opaque **handle** designating the exact version.

1. The client hands the handle back through `kv.repair`.
2. The receiving node decodes it, recomputes the partition id from the address and refuses a mismatch or an implausibly distant timestamp — all local, before any RPC.
3. `replication.lookup` returns one snapshot containing the exact source, up to X retained version identities and whether X newer versions exclude the requested one. A copy held by a stand-in can serve as the source but never counts as its owner's durability.
4. Missing owners receive the version, and the requested quorum is verified again — so a repair may itself report a quorum failure, with the same handle, and be retried.

Repair remains exact: it never substitutes a newer version. If an exact source
still exists, repair may restore that history even on replicas holding X newer
versions; subsequent GC may collect it again. When no source is found and
observed retention evidence establishes that the requested quorum is
unavailable, it rejects with `outdated`, the token and newer identities with
observed owner counts. Unknown or unreachable replicas alone do not establish
that diagnostic. A collected historical token is not guaranteed repairable.

### Replication obligations

An obligation states that one target must receive a version whose HLC is at least `required_ts`.
It is the single form of replication debt: the write path (handoff and failed
replica writes) and read repair produce it, anti-entropy consumes it.

```text
META key   := MetaKind.OBLIGATION | u16 len(target) | target | Key.to_bytes(Keyspace.TAGS)
META value := empty
```

- The version itself is stored as a plain `LIVE` or `TOMBSTONE` record. There
  is no hint tag, no staging and no activation: a version is either present
  or absent, and the digests count it as soon as it is stored.
- `GroupSession.put_obligations` writes the obligation in the same
  transaction as the version (`StoreService._oblige`). Recording the same
  obligation twice is a no-op. An obligation never targets the local node.
- Only then is it submitted to the `ObligationQueue`, a bounded in-memory
  hotqueue (`hotqueue_size`, 4096 by default). An obligation that does not
  fit stays in `META`; the queue raises its `backlog` flag, and delivery
  reloads `META` once the queue drains. The flag starts raised, so
  obligations left by a previous run are reloaded at startup.
- Incoming `replication.hint` requests are refused while the node is
  draining or paused, like other replica writes.

Delivery (`AntiEntropyService.deliver`) takes bounded batches from the queue
and pushes the versions through `antientropy.push`, grouped per target and
partition. The immutable timestamped identity is unchanged; delivery loads the latest local version once per address and groups covered debts into one send per target/key, even across byte-batch boundaries. Only captured, covered identities are removed after a validated acknowledgement; concurrent new debts remain. `applied`, `already_present` and `ignored_retention` account explicitly for every submitted record. The last disposition satisfies delivery, not proof that the exact version is stored, and never counts as an exact write or repair quorum acknowledgement.

Missing covering data is logged and its debt remains durable. A debt is dropped without delivery only when its target no longer replicates the partition in the current target view. Obligations of a target that is
suspected, unknown or refusing stay in `META`, the target is backed off and
the backlog is reloaded later. Obligations take priority over the periodic
sweep: a sweep unit waits until the queue is idle.

---

## 7. Membership and failure detection

Gossip kinds:

| Kind | Purpose |
| --- | --- |
| `gossip.push` → `gossip.push.accept` | Hot-path dissemination of fresh member state |
| `gossip.ping` → `gossip.pong` | Liveness and round-trip sampling |
| `gossip.digest` → streamed `gossip.delta` | Anti-entropy reconciliation of divergent views |

`Gossiper` runs two loops: a **hot loop** for recent changes and an **anti-entropy loop** for convergence. `TopologyManager` applies accepted updates to the registry, rebuilds the ring, and bumps a generation counter that wakes anti-entropy's view tracker. A phase change between two ring phases (for example `JOINING` → `READY`) changes ownership without changing the vnodes, and counts as a change too.

Every `gossip.push` carries a `hop_limit` (`gossip.hop_limit`, 3 by default). A responder that accepts at least one *newer* member relays exactly those members once more through `Gossiper.relay` with `hop_limit - 1`, skipping the sender and the members themselves; a budget of zero ends the epidemic. Relays run as background tasks (the push handler never waits on them), at most 64 are in flight, and a member is only relayed when it was newer locally, so duplicates die out on their own. `wanted` members of a `gossip.delta` (known by the initiator with a newer epoch, or unknown to the responder) are pulled back with `hop_limit = 1`. All gossip requests hold their pooled connection through `PeerPool.lease`.

Membership anti-entropy (the gossip `ae_interval` loop) and data anti-entropy (§9) are separate loops: membership state is tiny and must converge in seconds, data comparison is large and is paced over minutes.

`PhiAccrual` turns inter-arrival times of successful probes into a suspicion value; `PeerFailureDetector` drives probes per peer and exposes a status consumed by placement.

Invariants:

- Member updates are **monotonic** — a stale update must never regress a member's phase.
- Applying the same update twice is a no-op.
- A node's own record is authoritative for its own phase.

---

## 8. Join and drain

There is no dedicated transfer protocol. Placement states the target owner of
every partition and anti-entropy converges the data towards it.

**Join.** The node publishes `JOINING`: its vnodes enter the ring, so it is a
target replica of the partitions it gains, but it is neither readable nor
writable. Writes for those partitions go to a stand-in that records an
obligation for it. The lifecycle starts anti-entropy and waits for
`AntiEntropyService.wait_joined()`: every partition the node will own is
copied from a single **primary source** (`ReplicaView.primary_source`), among
the nodes serving it today (`ReplicaView.sources`, which skips `JOINING`
members). Entering and leaving replicas of a partition are paired in sorted
order, so joiners replacing as many nodes each copy from a distinct one; an
unpaired joiner picks a current source by hash to spread the load. Only when
the primary is down does the partition fall back to its other sources, so a
join cannot hang on a dead node. A version held only by a staying replica is
left to the sweep. Refused or interrupted sessions are retried on the current
view.
Only then does the node move to `READY` and open its client server. If
membership sync or the goal fails, the node becomes `FAILED`.

**Drain.** The node publishes `DRAINING`: it leaves the target placement
(`ReplicaView.replicas` skips `DRAINING` members) while still serving reads.
Once admitted writes settle, `AntiEntropyService.drain()` pushes every stored
partition to the replicas replacing it; a partition is done when every live
target replica completed a session for it. Read repairs then stop, the node
becomes unreadable, closes its client server and publishes `IDLE`. A failure
leaves the node `DRAINING`; the drain can be requested again, and resumes on
restart.

---

## 9. Data anti-entropy

`AntiEntropyService` (`tourillon/core/services/antientropy.py`, with its
components in `tourillon/core/ae/`) is the only mechanism that copies
versions between nodes. It delivers obligations (§6), fills joining nodes and
empties draining ones (§8), and its periodic sweep compares digests between
replicas to find whatever the obligations missed, such as a node applying a
topology update late and accepting a write for a partition it no longer
replicates.

### Digests

Every group maintains digest cells in its `META` keyspace, updated in the same
transaction as the data (`GroupSession.put_many` / `flush`):

| Key | Cell |
| --- | --- |
| `DIGEST · 0x00 · pid` (`ROOT_PREFIX`) | fold of every owned version of the partition |
| `DIGEST · 0x01 · pid · bucket` (`BUCKET_PREFIX`) | fold of the owned versions whose `wall_ms >> 22` is `bucket` (~70 min) |

A cell is `(count, xor of blake2b(tags key))`: order-independent, cheap to
update incrementally, and two equal cells mean the same set of versions with
overwhelming probability. Only owned versions (`LIVE`, `TOMBSTONE`) are
counted; obligations live under their own `META` prefix and are never
folded into a digest. Since a version is
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
   apply the ones this node lacks (`put_many`, which never rewrites an
   existing version).

The cost follows the size of the difference, not the size of the data: in
the steady state a session is one `roots` round-trip that returns nothing.

**Grace.** Versions younger than `grace` seconds (wall clock of the HLC) are
never compared by the sweep: they are still travelling through the write
path and obligation delivery, and copying them would only race with those.
The join and drain goals use the current time as cutoff instead.

**Sweep repairs are direct copies.** Missing versions are pushed or fetched
directly, not turned into obligations. The copy is idempotent (an existing version
is never rewritten), adds no durable state to replay, and bounded per session;
the data and its digests *are* the progress state, so a session stopped half
way loses nothing — the next sweep finds the rest because digests still differ.

### Replica mode and handoff mode

The direction of every copy follows ownership; callers never choose between
push, pull or both.

- **Replica mode** — for every co-replica `peer`, the pids both replicate
  (`ReplicaView.shared[peer]`) are compared two ways. Only one node of each
  pair initiates (`initiates()`: a hash of the sorted pair), so a pair is
  compared once per sweep, not twice.
- **Handoff mode** — this node scans its own non-empty roots
  (`root_page`), keeps the pids it does **not** replicate in the current view,
  and pushes their versions one way to every current replica. It
  never pulls and never deletes: the local copy stays until the GC reclaims it.
  When every replica was reached and the pid held no version still inside the
  grace window, the pid is marked with `(view hash, local root)` and skipped
  until either changes (a young version would otherwise never be pushed: the
  root does not change when it ages). Marks are in
  memory: after a restart each orphan pid is re-checked once (cheap: a `roots`
  exchange).

A version written on a pid it does not hash to is logged as foreign by the
responder; it is still accepted, since refusing it would lose data. A
responder serves reads of any partition (a joining node pulls from a node it
displaces) but refuses an `antientropy.push` for a partition it does not
replicate with `not_replica`.

### Agreeing on replicas without agreeing on the ring

`ViewTracker` rebuilds a `ReplicaView` (the replica tuple of every pid, built
off the hot path and yielding every 4096 pids) whenever the ring or a ring member's phase changes,
once membership stays quiet for `settle_quiet` (each further change restarts
that wait) and at the latest `settle_delay` after the first change, so a burst
of updates causes one rebuild while an isolated change is picked up at once. Every request
carries a `Scope`: its pids plus a 16-byte hash of the replica tuples the
sender computed for them. The responder recomputes it and refuses with
`stale_view` on mismatch, so both sides always reason about the same
replicas without needing identical rings — only the scoped pids must agree.

A topology change sets the view's *stale* signal (`TopologyManager.wait_for_change`): the
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
  exponential backoff. Peers that are not `JOINING`, `READY` or `DRAINING`,
  or are suspected by the failure detector, are skipped.
- Per-session budgets: `max_keys_per_cycle` keys listed and
  `max_repairs_per_cycle` versions copied (`BudgetSpentError`).
- Merge descent saves exclusive key/bucket checkpoints per peer and view, including local-only windows. Completed chunks advance the checkpoint; failed chunks are retried. Partition and root-page order rotates so early differing buckets cannot indefinitely starve later work. Checkpoints reset after a completed traversal or view change and are not persisted across restarts.
- Writes go through small transactions (`APPLY_BATCH` = 64 records, 8 MiB) and
  every scan yields to the event loop, so client `kv.put/get/delete` are never
  blocked behind a long anti-entropy transaction.
- The service runs while the node is `JOINING`, `READY` or `DRAINING`, and
  stops on shutdown; a stop waits for the requests already admitted.

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

Anti-entropy is mandatory: delivery and the periodic sweep always start.

| Key | Default | Meaning |
| --- | --- | --- |
| `sweep_period` | `600` | seconds to compare every unit once |
| `min_interval` | `1` | floor between two units |
| `jitter` | `0.2` | relative spread of every delay |
| `grace` | `120` | seconds before a version is compared |
| `settle_delay` | `5` | longest wait after a ring change before rebuilding the view |
| `settle_quiet` | `0.5` | quiet period that ends that wait early; retry hint of `not_ready` / `stale_view` |
| `concurrency` | `4` | requests served at once as responder |
| `pids_per_cycle` | `4096` | pids per unit |
| `keys_per_request` | `1024` | keys per page while descending |
| `max_batch_bytes` | `4 MiB` | payload of one push or fetch |
| `max_repairs_per_cycle` | `10000` | versions copied by one session |
| `max_keys_per_cycle` | `1000000` | keys listed by one session; at least 2 for a local/remote merge page |
| `hotqueue_size` | `4096` | obligations scheduled in memory at once; the rest wait in `META` |

Values are only bounded below; anything an envelope cannot carry is clamped
and paginated.

### Known limits

- **Retention policy.** Replicas must use the same X (§12). Every AE scope carries it and a mismatch is explicitly rejected. Historical arrivals outside local top-X are ignored only after X newer versions are durably present in the same local snapshot. Digests represent retained records, not a retirement floor.
- **Unreachable joins and drains.** A join skips sources that are down, so a
  version held only by a dead node arrives later through the sweep. A drain
  cannot finish while no live replica replaces the node.
- **Orphaned obligations.** A read-repair obligation recorded on a node after
  its drain finished stays in `META` until delivery drops it as obsolete.
- **Handoff restart.** Budget stops resume in memory; a process restart rechecks data and handoff marks from the beginning.
- **Digest migration.** Data written before digests were maintained has no
  roots and is invisible to anti-entropy until rewritten.
- The responder's `range` scan is bounded by the bucket size, not by a budget.

---

## 10. Wire protocol

```
Envelope := header(32 bytes) || kind(UTF-8) || context || payload
header   := >B H B 16s I B B 6s
            proto_version | schema_id | mode | correlation_id | payload_len | kind_len | context_len | reserved
mode     := 0 RPC | 1 fire-and-forget
```

- `context` is an opaque byte string of at most 32 bytes (`MAX_ENVELOPE_CONTEXT`). The transport never interprets it: the sender's `EnvelopeClient` fills it from a provider, and the receiver hands it to the telemetry port. An empty context costs nothing, so the frame is the same as without one. `replay_envelope()` does not echo the request context.

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
| `node` | peer | `node.join` / `node.drain` / `node.gc` / `node.gc.status` |
| `inspect` | peer | `node.inspect` → paginated `node.info` |

`gossip.*`, `replication.*` and `antientropy.*` are internal to the cluster and deliberately have no client service.

`tourctl` builds on those services with cyclopts commands (`probe ping`, `node inspect`, `node join`, `node drain`, `node gc`, `put`, `get`, `delete`, `repair`). Commands stay thin; rendering and error mapping live in `tourctl/core/`, which translates configuration, TLS/connectivity and protocol failures into actionable `SystemExit` messages instead of tracebacks.

`tourillon config ca | client | node` generates the mTLS material and the TOML configuration files that both sides consume.

---

## 12. Storage model

```
Storage ──session_provider_for(group)──▶ SessionProvider ──session(write=)──▶ Session ──batch()──▶ Batch
```

- One session provider per **group**; sessions are short-lived and scoped to read or write.
- `Session` offers `get`, `put`, `delete`, `last`, `iter` and bounded `keys` pages of `KVRef` (key and keyspace); `Batch` offers the scheduling API resolved by one `await batch.result()`. Both key-only APIs use the native cursor with `values=False`, without materializing values or intermediate `KVPair` objects.
- Keyspaces and record layout live in `tourillon/core/models/storageschema.py`. Keys are length-prefixed byte compositions — never ad-hoc strings.
- `Storage.latest_hlc(total_groups)` is read at boot to restore the hybrid logical clock so timestamps never move backwards across restarts.

`core/storage/session.py` provides a typed `GroupSession` through
`GroupSessionManager.session(write=...)`. Its `read` and `get_many` methods
read exact versions and `get` reads the latest version. `put` and `put_many`
insert `LIVE` or `TOMBSTONE` versions without ever rewriting an existing one,
and fold only newly inserted versions into the per-bucket count/XOR digest.
`put_obligations`, `has_obligations`, `obligation_page` and
`remove_obligations` encapsulate replication obligations without allowing
arbitrary `META` writes. `roots`, `root_page`, `bucket_page` and `owned_keys`
serve anti-entropy. The caller retains responsibility for placement. The
manager flushes digests before committing the underlying transaction; errors
roll back records, obligations and digests together. `GroupSession` does not
expose its underlying raw session. Paged scans log and skip unreadable
entries while still advancing their cursor; no transaction stays open while
the caller consumes a page.

`GroupSession` and `GroupSessionManager` have no retention policy. They expose
bounded `latest_identities`/`latest_identities_many` reads and atomic `remove`
operations; the caller selects what may be removed.

`core/storage/retention.py` provides `RetentionPolicy`, an explicit collaborator
that neither opens transactions nor wraps storage. Callers open
`GroupSessionManager.session(write=...)` and pass that session to the policy
only when their use case needs it:

- Store inserts exact writes and hints with `session.put`, without a retention
  preflight. An acknowledgement means the exact version was stored, not that
  it will be retained forever. Records, obligations and digests remain atomic.
- AE calls `policy.apply(session, records)` to prevent repeated reintroduction
  of collected history. It reports `applied`, `already_present` or
  `ignored_retention` per input and carries X in scopes to detect mismatches.
- GC calls `policy.collect(session, keys)` to select versions covered by X
  newer durable local versions, then removes them atomically.
- Exact repair lookup calls `policy.lookup(session, key)` for the source and
  retention evidence. Ordinary reads and writes do not incur these reads.

Policy decisions and mutations use the same caller-owned transaction.
Admission reads are grouped by distinct address into two metadata batches,
without loading their payloads. There is one rule implementation, but no
mandatory wrapper or retention-aware raw session.

Configuration and node state are persisted through the `Persistence` port, TOML by default, with per-path locking and atomic replace.

### Local historical GC

`GarbageCollector` retains the X greatest HLC versions per `(namespace, key)` within each partition, including tombstones. X is at least 1: the current local state is never collected, even on outgoing or orphan partitions. Historical obligations remain useful through a newer retained version; retained outgoing copies remain available to AE until transfers finish. Entire orphan-partition removal, current tombstone removal and file compaction are not implemented.

Collection deletes `LOGS` and `TAGS` and subtracts root/bucket count-XOR contributions in the same write transaction. Empty digest cells are removed; rollback preserves all three surfaces. No `STALE` tag, time floor, remote vote or pre-collection quorum check is used. Reads of the newest X identities use bounded, key-only storage pages without materializing their payloads.

One mandatory worker owns periodic passes, a deduplicated bounded hot queue and internally requested passes. `submit(PartitionAddress)` is a generic notification API with no AE types or network dependency; AE is its first producer. `Manager` owns its start and stop; `NodeLifecycle` requires the GC collaborator to wire notifications to AE. Overflow falls back to the periodic scan. Each transaction handles at most `batch_size` candidates and yields between pages. The manager starts the worker after clock restoration and stops it before closing storage.

AE and GC are mandatory, and their retention configuration is validated.
This does not impose admission filtering on exact writes: history can exceed X
until GC runs. The GC notification callback is required.

```toml
[placement.retention]
versions = 2
interval = 600.0
batch_size = 64
hotqueue_size = 4096
```

Bounds: `versions` 1..64, `interval` 0.01..86400 seconds, `batch_size` 1..1024 and `hotqueue_size` 1..65536. **The default retains two versions**; replicas must be configured together. Increasing X cannot recover already collected history.

`manager.gc.request(dry_run=True)` queues a non-mutating estimate; `request()`
queues collection. An optional `partition` restricts the scan to that pid,
including storage-page bounds, with no interleaved hot work. Both use the same
retention checks. Status exposes job ID, state, scope, current group, counts,
logical record bytes, start/finish timestamps and errors. Estimates are local
snapshots, not reservations or predicted filesystem savings.

`tourctl node gc ADDRESS [--partition PID] [--dry-run] [--wait] [--timeout 60]
[--json]` targets the peer endpoint. `node.gc` acknowledges submission only;
`--wait` polls `node.gc.status` for that exact job, with a bounded wait that
never cancels server work. Already queued/running requests are rejected.
Failures and cancellation produce nonzero CLI exits; JSON failure output is
structured on stderr. `node inspect` includes the worker and latest pass in
text and JSON, without a separate CLI status command. The worker retains at
most 64 completed job results in memory; expired or unknown IDs are rejected
explicitly. The status describes scan progress, not a precomputed percentage
or interleaved hot work. Human output uses binary byte units; JSON keeps exact
integers. There is no force, compaction, policy mutation or cluster-wide
collection option.

The contract preserves current state under the replication requested at write time; it does not retain history indefinitely. Free backend pages can be reused, but file size need not decrease after GC.

GC logs `gc.started`/`gc.stopped` at INFO, including the node and configured
retention/scan interval at startup. The first periodic scan runs after that
interval; subsequent scans wait the interval after the previous pass finishes.
Completed passes log mode (`periodic`, `requested` or `estimate`), scanned
versions, `collected` (or `collectible` for estimates), logical LOGS/TAGS record
bytes and duration. Job IDs remain in the internal status and failure logs,
not successful pass summaries. Transaction failures include
node, group and source; successful hot transactions do not emit INFO per key.
The internal pass status describes the scan only, not interleaved hot work.
Metrics cover both paths and count removal work only after successful commit.
An estimate records `collectible`, not `collected`, and never increments removed
bytes. Queue size tracks pending addresses, not total collectible history.

---

## 13. Observability

Telemetry goes through the `Telemetry` / `TelemetryProvider` ports, implemented by the OpenTelemetry adapter in `tourillon/infra/adapter/telemetry/`; the no-op implementation is used when telemetry is disabled.

### Logs

Logging uses `tourillon.core.log.logger` with lazy `%s` formatting. Messages are prefixed by subsystem (`node.phase`, `gossip.`, `topology.`, `ae.`, `store.`, `replication.`, `probe`) and carry `key=value` context; exceptions are rendered with `describe(exc)` so that message-less errors (timeouts) still show their type. Levels follow one rule:

| Level | Used for |
| --- | --- |
| `ERROR` | The node cannot do its job: startup or bind failure, a failed drain, a handler crash (with traceback). |
| `WARNING` | A degradation an operator should look at: a replica or quorum failure, a peer unreachable (first failure of a streak only), a saturated pool, an obligation that cannot be recorded. |
| `INFO` | State changes: listeners bound, phase transitions, owned tokens, ring and replica-view changes, join/drain progress, a sweep that copied data, a peer that recovered. |
| `DEBUG` | Steady-state and per-request detail: each envelope, refusals returned to the caller, idle sweeps, repeated probe failures, pool churn. |

A healthy idle node therefore stays quiet at `INFO` after startup.

### Metrics

All names carry the `tourillon_` prefix. Labels are low-cardinality on purpose: no key, peer or correlation id is ever a label.

| Area | Metrics (labels) |
| --- | --- |
| Transport | `connections_accepted_total`, `connections_closed_total`, `dispatched_total`, `unknown_kind_total`, `protocol_errors_total`, `in_flight_current` (`endpoint`, `kind`, `mode`) |
| Peer pool | `pool_connections_total`, `pool_evictions_total`, `pool_promotions_total`, `pool_acquire_errors_total` (`reason`: `timeout`/`connect`), `pool_size_permanent`, `pool_size_overflow` |
| Store | `kv_requests_total`, `kv_request_duration_seconds` (`op`, `outcome`: `ok` or the error code), `replication_failures_total` (`op`), `read_repairs_total` (`outcome`), `obligations_total` (`event`: `recorded`/`delivered`/`deferred`/`dropped`) |
| Anti-entropy | `ae_sessions_total` (`goal`, `outcome`), `ae_versions_total`, `ae_bytes_total` (`direction`: `pull`/`push`), `ae_sweeps_total` (`outcome`: `complete`/`interrupted`) |
| GC | `gc_passes_total`, `gc_pass_duration_seconds` (`source`: `periodic`/`requested`, `mode`: `collect`/`estimate`, `outcome`: `completed`/`failed`/`cancelled`); `gc_versions_total` (`source`: `scan`/`hot`, `mode`, `event`: `scanned`/`collected`/`collectible`); `gc_record_bytes_removed_total`, `gc_errors_total` (`source`: `scan`/`hot`); `gc_transaction_duration_seconds` (`source`, `mode`, `outcome`); `gc_hot_queue_size` |
| Gossip | `gossip_push_attempt_total`, `gossip_push_error_total`, `gossip_ae_cycle_total`, `gossip_ae_diverged`, `gossip_failed_announcements_total`, `gossip_hot_queue_size`, `gossip_last_ae_at` |

### Traces

GC emits logs and metrics only, with no GC spans. Its duration histograms include
transaction acquisition and commit. Removed bytes include both LOGS/TAGS keys, the value and tag, excluding digest changes,
not physical disk reclamation. GC metric labels contain no keys, job IDs or
partition IDs.

Write traces add `peer.request` around peer acquisition and request/response round-trips, and `storage.write` around local record persistence, including obligation persistence on handoff. Its children split transaction lifetime (`storage.transaction`), version insertion (`storage.record.put`) and handoff obligations (`storage.obligations.put`). The transaction span includes begin, digest updates and commit, so its uncovered time after accounting for child spans helps identify their combined overhead. Span attributes identify the peer, storage group and request kind, never the key or value.

Request-boundary spans include: `transport.envelope` (a handler failure is recorded on this span), `gossip.push` and `gossip.ae_ping_pong`. Trace context crosses nodes through the envelope `context`: peer clients ask `Telemetry.inject_context()` for the current span, and the receiving session wraps `transport.envelope` in `Telemetry.extract_context()`, so a replica's spans join the coordinator's trace. The OpenTelemetry adapter writes 26 bytes (format `0x01`, trace id, span id, trace flags) and only for a sampled span; with tracing off nothing is sent. There is deliberately no connection span (a pooled connection lives for hours; connections are covered by metrics), and an envelope without readable context starts a new trace rather than nesting under whatever span is current. Join and drain run in a fresh context so the loops they start (gossip, anti-entropy, listeners) never inherit the triggering request's span. Client connections (`tourctl`) send no context.

---

## 14. Where the design is deliberately open

The following are design decisions that have **not** been settled yet; see [`backlog.md`](backlog.md) for the actionable list.

- Namespaces exist on the wire and in addressing but have no registry, quota, or access-control semantics.
- Consistency is expressed as raw per-request quorum integers; there is no named consistency-level API.
- Data anti-entropy heals retained state proactively (§9); historical GC is local top-X (§12). Final current-tombstone removal and whole-orphan reclamation remain out of scope.
- Placement has no failure-domain (anti-affinity) awareness.
- Ownership only changes through joins and explicit drains; there is no automatic rebalancing of vnodes.
