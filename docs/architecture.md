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
hint replay stops, new read repairs are disabled and existing repairs settle.
Pending hints retain their `HINT` tags in transferred records; the destination
replays them to their intended owners. Departure does not wait for the source's
hints to be satisfied.
The node remains `DRAINING` until the transfer runner succeeds.
Ring changes trigger transfer replanning.

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
- Each preference entry is annotated as readable, writable, or needing **handoff**, based on the member phase plus the local failure detector.
- Failure detection is **local and advisory**. Two nodes may legitimately disagree about a peer's liveness; the protocol must tolerate it.

---

## 6. Read and write paths

### Write (`kv.put`, `kv.delete`)

1. The receiving node coordinates — there is no leader to forward to.
2. A `Version` record is stamped with an HLC timestamp from the local `HLCClock`.
3. The preference list is resolved for the key.
4. `replication.write` is fanned out to the targets in parallel; the local replica is applied directly.
5. Targets that are not writable receive a **hinted** record, tagged with the intended owner.
6. The coordinator verifies `quorum_write` acknowledgements before accepting; request models default omitted values to 1, including deletes and write repair.

Deletes are **tombstones**, never physical removals, so a delete can win over a concurrent write by timestamp.

### Read (`kv.get`)

1. The preference list is resolved and `replication.read` is collected from the readable targets plus the local replica.
2. Visible versions are filtered (tombstones and hints have special handling).
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

### Hinted handoff replay

A background replayer scans hint keyspaces per group and delivers stored hints to their intended owner once it is reachable again, then clears them. Replay must be idempotent: redelivering a hint may not resurrect a deleted key or lose a newer write.

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
