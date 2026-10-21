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

Shutdown is ordered: `NodeLifecycle.stop()` (client server → leave task → hint replayer → join task → gossip → peer server), then peer pool, storage, telemetry. A failing step is logged and the remaining steps still run.

Shutdown signals do not initiate a departure. Only an explicit `tourctl node leave`
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

A failed drain stays `DRAINING` and can be retried with `node leave`; a restart
resumes a persisted drain. After successful transfers the client listener closes,
then completion publishes `IDLE`, removing the node from the ring without
terminating the process or deleting
its local data. The peer
listener and gossip remain available. On startup, any `IDLE` node with persisted
or configured seeds waits for an explicit join; without seeds it bootstraps.
No previous-phase field is used to distinguish a drained node.
Leaving the last node on the ring is rejected
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
local replay writes on leave and waits for admitted local deliveries before
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

During leave, replay runs alongside transfers. Once transfers succeed and
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

## 9. Wire protocol

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

## 10. Client and CLI

`tourclient` exposes a **service registry** mapping a service name to a factory and the endpoints it is valid on:

| Service | Endpoints | Kinds |
| --- | --- | --- |
| `probe` | client, peer | `probe.ping` → `probe.pong` |
| `kv` | client, peer | `kv.put` / `kv.get` / `kv.delete` / `kv.repair` |
| `node` | peer | `node.join` / `node.leave` |
| `inspect` | peer | `node.inspect` → paginated `node.info` |

`gossip.*`, `replication.*` and `transfer.*` are internal to the cluster and deliberately have no client service.

`tourctl` builds on those services with cyclopts commands (`probe ping`, `node inspect`, `node join`, `node leave`, `put`, `get`, `delete`, `repair`). Commands stay thin; rendering and error mapping live in `tourctl/core/`, which translates configuration, TLS/connectivity and protocol failures into actionable `SystemExit` messages instead of tracebacks.

`tourillon config ca | client | node` generates the mTLS material and the TOML configuration files that both sides consume.

---

## 11. Storage model

```
Storage ──session_provider_for(group)──▶ SessionProvider ──session(write=)──▶ Session ──batch()──▶ Batch
```

- One session provider per **group**; sessions are short-lived and scoped to read or write.
- `Session` offers `get`, `put`, `delete`, `last` and `iter`; `Batch` offers the same scheduling API resolved by one `await batch.result()`.
- Keyspaces and record layout live in `tourillon/core/models/storageschema.py`. Keys are length-prefixed byte compositions — never ad-hoc strings.
- `Storage.latest_hlc(total_groups)` is read at boot to restore the hybrid logical clock so timestamps never move backwards across restarts.

Configuration and node state are persisted through the `Persistence` port, TOML by default, with per-path locking and atomic replace.

---

## 12. Observability

Telemetry goes through the `Telemetry` / `TelemetryProvider` ports, implemented by the OpenTelemetry adapter in `tourillon/infra/adapter/telemetry/`. The peer pool, transport and gossip paths are instrumented. Logging uses `tourillon.core.log.logger` with lazy `%s` formatting and node/correlation context on request paths.

---

## 13. Where the design is deliberately open

The following are design decisions that have **not** been settled yet; see [`backlog.md`](backlog.md) for the actionable list.

- Namespaces exist on the wire and in addressing but have no registry, quota, or access-control semantics.
- Consistency is expressed as raw per-request quorum integers; there is no named consistency-level API.
- There is no anti-entropy between replicas outside the read path: divergence is healed opportunistically by read repair, or on demand through `kv.repair`, and never proactively.
- Placement has no failure-domain (anti-affinity) awareness.
- Steady-state rebalancing is not implemented; transfers are driven by joins and explicit departures.
