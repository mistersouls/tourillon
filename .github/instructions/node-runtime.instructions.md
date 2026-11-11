---
applyTo: "tourillon/core/**/*.py,tourillon/infra/**/*.py,tourillon/bootstrap/deps.py"
---

# Node runtime — core domain and adapters

## Layering (non-negotiable)

```
tourillon/core/      domain logic — depends ONLY on protocols
tourillon/infra/     concrete adapters (LMDB storage, OpenTelemetry)
tourillon/bootstrap/ composition root — the only place that picks implementations
```

- `tourillon/core/` must never import `lmdb`, `opentelemetry`, or anything from `tourillon/infra/`.
- New capability → extend the protocol in `tourillon/core/ports/`, implement in `tourillon/infra/`, wire in `tourillon/bootstrap/deps.py`.
- `deps.py` uses `functools.lru_cache(maxsize=1)` for process singletons (`get_app`, `peer_dispatcher`, `client_dispatcher`). Keep that pattern; do not introduce new global mutable state.

## Composition

`Application` holds adapters and the context; `Manager` constructs and owns the node services:
`Partitioner` + `HashSpace`, `TopologyManager`, `PeerPool`, `SimplePlacementStrategy`, `StoreService`, `NodeLifecycle`, `NodeInspector`, `GossipService`, `AntiEntropyService`, `ObligationQueue`, `HLCClock`, `FailureDetectorProvider`.

Services receive their collaborators by constructor injection. Do not reach for globals or import `get_manager()` from inside `core/` — that helper exists for `bootstrap/handlers/` only.

## Storage access

Services receive the same `core/storage/store.py` `Store` from `Manager`.
Use its data operations; never open a session or transaction in a service.

- `Store` owns partition-group routing, exact reads and writes, AE application, obligations, digests, retention and clock restoration.
- Its internal `SessionProvider` opens a typed `Session` through `Storage.transactional_for(group)` and `Transactional.transaction(write=...)`.
- Raw operations return deferred results, resolved by `await result.get()` or `await transaction.results()`. Keep related record, obligation and digest mutations in one transaction.
- Key and value encodings come from `tourillon/core/models/storageschema.py` (`Keyspace`, `KVPair`, `KVRef`). Never invent an ad-hoc byte layout.
- Write sessions must be short-lived; do not hold one across a network call.

## Ring, partitions and placement

- The hash space is `HashSpace(bits=128)`; the `Partitioner` splits it into `2**pshift` partitions grouped into `2**gshift` groups.
- A key maps to a partition; the partition maps to its successor vnode on the ring; the preference list is built by walking the ring from there, skipping members that are not eligible.
- Only `READY`, `PAUSED` and `DRAINING` members are present on the ring. `IDLE`, `JOINING` and `FAILED` members are excluded from placement.
- Ring lookups must stay O(log n); the ring is a sorted token array. Any change here needs a property-style test over random token sets to prove placement stays deterministic and wrap-around is correct.
- Changing `pshift`, `gshift`, the hash function, the token layout or the preference-list walk is a **data-layout change**: it moves every key. Call that out explicitly and add migration/compatibility notes.

## Replication, quorums and conflicts

- `StoreService` coordinates: it resolves the preference list, fans out `replication.read` / `replication.write` to peers, and applies locally.
- Request models use non-optional integer quorums (`quorum_read`, `quorum_write`) defaulting to 1, including write repair. Explicit values remain validated; handlers treat legacy explicit `null` values as omitted. Configurable defaults tied to replication factor are deferred.
- Conflict resolution is HLC-based last-writer-wins with deterministic tie-breaks. Any change to the comparison order must keep it a **total order** and be identical on every node, otherwise replicas diverge. Test with shuffled input.
- Deletes are tombstones, not removals. Preserve tombstone visibility semantics when touching read paths.
- Missed replicas (handoff, failed replica write, read repair) become durable replication obligations: persisted in `META` with the version, then queued in the bounded `ObligationQueue`. Anti-entropy is the only consumer. Versions are always stored as plain `LIVE` or `TOMBSTONE`; there is no hint or staging tag.

## Gossip, membership and failure detection

- `GossipService`/`Gossiper` run a hot push loop plus an anti-entropy loop (`gossip.push`, `gossip.ping`/`gossip.pong`, `gossip.digest` → streamed `gossip.delta`).
- `TopologyManager` owns the member registry and rebuilds the ring on accepted updates; its `generation` counter (ring and ownership changes, including a JOINING → READY phase change) wakes anti-entropy's view tracker.
- `PhiAccrual` computes local suspicion; `PeerFailureDetector` drives probes. Failure detection is **local and advisory** — never treat it as cluster-wide truth.
- Membership state changes must be idempotent and monotonic: re-applying an older update must not regress a member's phase.

## Join, drain and anti-entropy

There is no transfer protocol. Placement states target ownership; `AntiEntropyService` converges data to it.
- A `JOINING` node is on the ring with `readable=False` and `writable=False`; the lifecycle waits for `AntiEntropyService.wait_joined()` before publishing `READY`.
- A `DRAINING` node is skipped by the target view; the lifecycle waits for `AntiEntropyService.drain()` before publishing `IDLE`.
- Copy direction follows ownership; never add a push/pull mode switch. Copies must stay **idempotent**: an existing version is never rewritten.

## Concurrency

- Long-running loops live in tasks owned by a service; they must be cancelled in `stop()` and awaited.
- Shutdown order matters: `NodeLifecycle.stop()` then peer pool, storage, telemetry. A failing step must not abort the remaining steps.
- Avoid unbounded task creation per inbound message or per peer; prefer bounded concurrency.
