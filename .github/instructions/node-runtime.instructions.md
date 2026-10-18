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
`Partitioner` + `HashSpace`, `TopologyManager`, `PeerPool`, `SimplePlacementStrategy`, `StoreService`, `NodeLifecycle`, `NodeInspector`, `GossipService`, `TransferService`, `HLCClock`, `FailureDetectorProvider`.

Services receive their collaborators by constructor injection. Do not reach for globals or import `get_manager()` from inside `core/` — that helper exists for `bootstrap/handlers/` only.

## Storage access

```python
provider = await storage.session_provider_for(group)
async with provider.session(write=True) as session:
    async with session.batch() as batch:
        batch.put(kv)
        batch.get(ref)
        results = await batch.result()
```

- Every operation is scoped to a **partition group**. Never bypass `session_provider_for`.
- Use `batch()` when multiple operations belong to the same logical unit; a single `await batch.result()` resolves them.
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
- Quorums are optional per-request integers (`quorum_read`, `quorum_write`). Absent means "no explicit quorum requirement" — do not silently invent a default.
- Conflict resolution is HLC-based last-writer-wins with deterministic tie-breaks. Any change to the comparison order must keep it a **total order** and be identical on every node, otherwise replicas diverge. Test with shuffled input.
- Deletes are tombstones, not removals. Preserve tombstone visibility semantics when touching read paths.
- Unwritable replicas get hinted writes, replayed later by the hint replayer. Keep hint payload shape and its tombstone detection in sync.

## Gossip, membership and failure detection

- `GossipService`/`Gossiper` run a hot push loop plus an anti-entropy loop (`gossip.push`, `gossip.ping`/`gossip.pong`, `gossip.digest` → streamed `gossip.delta`).
- `TopologyManager` owns the member registry and rebuilds the ring on accepted updates; it notifies waiters so join-time transfer planning can replan.
- `PhiAccrual` computes local suspicion; `PeerFailureDetector` drives probes. Failure detection is **local and advisory** — never treat it as cluster-wide truth.
- Membership state changes must be idempotent and monotonic: re-applying an older update must not regress a member's phase.

## Transfers

`TransferService` is the peer-facing coordinator: `init` (open/resume a session, return staged resume keys), `claim` (stream a pull window), `chunk` (stage pushed data), `commit` (activate staged data).
Transfers must be **resumable and idempotent** — a retried chunk must not duplicate or lose data, and staged data must never be readable before commit.

## Concurrency

- Long-running loops live in tasks owned by a service; they must be cancelled in `stop()` and awaited.
- Shutdown order matters: `NodeLifecycle.stop()` then peer pool, storage, telemetry. A failing step must not abort the remaining steps.
- Avoid unbounded task creation per inbound message or per peer; prefer bounded concurrency.
