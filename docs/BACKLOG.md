# Backlog — known gaps and defects

Working list for contributors and AI agents. Every entry was verified against the code at the time of writing and cites `file:line`.
Use [`.github/prompts/fix-bug.prompt.md`](../.github/prompts/fix-bug.prompt.md) for defects and [`.github/prompts/raise-coverage.prompt.md`](../.github/prompts/raise-coverage.prompt.md) for coverage work.

**Rules for this file**

- Keep entries factual and cited. If a claim cannot be traced to code, it does not belong here.
- When you fix an item, delete it in the same PR.
- When you leave a `# todo(<handle>):` marker in code, add a matching entry here.

---

## A. Missing features promised by the README

| # | Item | Evidence | Notes |
| --- | --- | --- | --- |
| A1 | **Namespaces are addressing-only.** `namespace` is carried in `Address`, requests and keyspaces, but there is no namespace registry, no isolation policy, no per-namespace replication settings. | `tourlib/models/version.py` (`Address`), `tourlib/models/kv.py` | README advertises multi-tenant isolation and namespace-level replication. |
| A2 | **No ACLs.** Authorization is limited to mTLS peer authentication; any trusted certificate may read or write any namespace. | `tourillon/core/models/config.py` (TLS contexts), `tourillon/bootstrap/handlers/kv.py` | README advertises namespace ACLs. |
| A3 | **No quotas.** Nothing bounds per-namespace key count or stored bytes. | no implementation found | README advertises quota management. |
| A4 | **No named consistency levels.** Consistency is expressed as raw per-request `quorum_read` / `quorum_write` integers with no default policy. | `tourlib/models/kv.py`, `tourillon/bootstrap/handlers/kv.py:26-96` | README advertises "eventual, strong, tunable". Needs a design decision before implementation. |
| A5 | **No failure-domain aware placement.** Placement walks the ring and takes distinct members; there is no label model (rack / zone / region / provider) and no anti-affinity constraint. | `tourillon/core/partition/placement.py`, `tourillon/core/models/member.py` | README advertises label-based anti-affinity. Requires member labels on the gossip wire first. |
| A6 | **No steady-state rebalancing.** Transfers are planned during join and replanned on ring change while joining; there is no periodic or operator-triggered rebalance controller. | `tourillon/core/services/lifecycle.py` (`_run_transfers`) | README advertises automatic rebalance on topology change. |
| A7 | **Read repair is not observable.** Background read repair now writes the winning version back to stale or missing owners, but its counters live on `StoreService.stats` and are not exported through the telemetry port. | `tourillon/core/services/store.py` (`_schedule_read_repair`, `ReplicationStats`) | Wire `ReplicationStats` to `Telemetry` so repairs, skips and failures are visible in metrics. |

---

## B. Unimplemented code paths

| # | Item | Evidence |
| --- | --- | --- |
| B1 | **`NodeLifecycle.leave()` is an empty stub** (`...`). A node cannot be drained or removed gracefully; there is also no `tourctl node leave` command. Decommissioning currently means killing the process and letting failure detection react. See C4 for the immediate hazard. | `tourillon/core/services/lifecycle.py:176-177` |
| B2 | **Local write only re-checks the partition id, not the preference list.** A replica applying `replication.write` recomputes the pid from the address and traces a mismatch, but it does not confirm it is still a valid replication target for that key. The full check is deliberately deferred: `preference_list` snapshots every known member on each call (B4), so calling it per replicated write would multiply that cost by the replication factor. | `tourillon/core/services/store.py` (`StoreService.write`), `tourillon/core/gossip/registry.py:50-56` |
| B3 | **Ring rebuild is O(n) per accepted membership update**, with an O(n²) bootstrap merge, flagged in code as a scalability concern at the 10k-node target. | `tourillon/core/gossip/topology.py:170` |
| B4 | **Every placement lookup copies the whole membership table.** `TopologyManager.snapshot()` calls `registry.snapshot()`, which returns `dict(self._members)` under the manager lock, so each `preference_list` — one per client read and per client write — is O(number of known members). This is the cost that keeps B2 partial. | `tourillon/core/gossip/topology.py:75-81`, `tourillon/core/gossip/registry.py:50-56` |

---

## C. Defects

| # | Item | Evidence |
| --- | --- | --- |
| C1 | **Gossip never releases the peer connections it acquires.** `_send_push` and `_ae_ping_pong` call `peer_pool.acquire()` with no matching `release()` and no `lease()` context, unlike every other caller (`StoreService._remote_request`, `HintReplayer._replicate_to`, `TransferApplicator`). Gossip runs on a timer, so each push and each anti-entropy cycle permanently increments the refcount of the peer it talked to. | `tourillon/core/gossip/gossiper.py:343`, `tourillon/core/gossip/gossiper.py:429`, versus `tourillon/core/services/store.py:655-664` |
| C2 | **A leaked refcount makes a pooled connection unreclaimable, and the pool then evicts connections that are in use.** `_remove_expired` only reaps entries whose `refcount == 0`, and an overflow connection is only closed when a `release()` brings it to zero — neither can happen for a peer that gossip has talked to. Once `max_size` and `max_overflow` are both saturated, every new acquisition takes the LRU branch, which closes a permanent connection **without checking its refcount**, aborting any request in flight on it. | `tourillon/core/peer/pool.py:273`, `tourillon/core/peer/pool.py:279`, `tourillon/core/peer/pool.py:183` (`refcount == 0` conditions), `tourillon/core/peer/pool.py:154-156` (LRU eviction ignores refcount) |
| C3 | **A peer that changes its bind address resets the refcount of concurrent holders.** `_may_refresh_peer` reconnects and sets `peer.refcount = 1`, discarding the count of callers already holding the old client. Their `release()` calls then drive the counter negative, so `refcount == 0` never holds again and the entry is pinned for the life of the process. The same reconnection also closes the old client under those in-flight requests. | `tourillon/core/peer/pool.py:245-251` |
| C4 | **`NodeLifecycle.leave()` silently succeeds.** The body is `...`, so the coroutine returns `None` instead of signalling that draining is unimplemented. Any future caller — including a `tourctl node leave` — would report a successful departure that never happened. Prefer raising until B1 is implemented. | `tourillon/core/services/lifecycle.py:176-177` |
| C5 | **Dead placeholder branch on the gossip error path.** `_handle_push_error` compares the peer's error code against the literal string `"todo: critical error should trigger stopping program"`, which no handler ever emits, and the branch only logs. There is no protocol-level way for a peer to signal a fatal condition, although `ProcessError` already plays that role for transfers. | `tourillon/core/gossip/gossiper.py:399-407`, versus `tourillon/bootstrap/handlers/transfer.py:33-34` |

---

## D. Tooling, tests and process

| # | Item | Evidence |
| --- | --- | --- |
| D1 | **Repo-wide lint and format debt.** `uv run ruff check .` reports 109 errors (mostly `I001` import ordering) and `uv run black --check .` would rewrite 168 files. Until this is paid down, contributors and CI must lint only changed files. Pay it down deliberately, in a dedicated formatting-only PR, never mixed with behaviour changes. | `uv run ruff check .`, `uv run black --check .` |
| D2 | **`tourclient` is not in the coverage gate.** `--cov` covers `tourillon`, `tourctl` and `tourlib` only, so the entire client library's coverage is unmeasured. | `pyproject.toml` (`addopts`) |
| D3 | **Under-tested runtime internals.** The weakest modules are the peer pool, storage group, node lifecycle, transfer planner, transport session, phi-accrual detector and gossip bootstrapper — exactly the code paths where distributed bugs hide. | `uv run pytest --cov-report=term-missing` |
| D4 | **E2E relies on bounded polling.** `Journey.wait_for` polls with short sleeps and fixed budgets (`COMMAND_TIMEOUT`, `CONVERGENCE_TIMEOUT`, `SCENARIO_TIMEOUT`); these may be tight on loaded CI runners. Prefer waiting on observable state and keep the budgets reviewed. | `tests/e2e/journey.py` |
| D5 | **Shallow licence-header check.** `check_license_header.py` only looks for two substrings in the first 20 lines, so a mangled or duplicated header passes unnoticed — one such header was found and fixed in `tourillon/bootstrap/usecases/config.py`. Consider matching the full block. | `scripts/check_license_header.py` |
| D6 | **No type checker.** There is no mypy/pyright configuration or CI step, although the codebase is fully annotated and `.gitignore` already anticipates `.mypy_cache/`. | `pyproject.toml`, `.gitignore` |

---

## E. Design questions to settle before implementing

These are not defects; they are decisions Tourillon has not made yet. Resolve them deliberately — do not copy another system's answer.

1. **Consistency API.** Do quorums stay raw integers, or does Tourillon expose named levels mapped to a replication factor? What is the default when a request specifies nothing?
2. **Conflict resolution.** Last-writer-wins by HLC is simple but loses concurrent updates. Does Tourillon need causality tracking, sibling values, or application-supplied merge?
3. **Repair strategy.** Background read repair is implemented and enabled by default, and an under-replicated write can be resumed on demand through `kv.repair` from the handle its failure returned; anti-entropy between replicas outside the read path is still undecided.
4. **Namespace lifecycle.** How is a namespace created, configured, and deleted, and what happens to data on deletion?
5. **Failure-domain model.** What labels does a node advertise, how do they reach other nodes through gossip, and how strictly does placement enforce them when constraints cannot be satisfied?
6. **Decommissioning.** What does a graceful leave guarantee — hand off all owned ranges before exiting, or just stop accepting writes?
7. **Tombstone lifetime.** When may a tombstone be collected without risking resurrection of deleted data?

---

## F. Robustness gaps (not defects, but load-bearing assumptions)

| # | Item | Evidence |
| --- | --- | --- |
| F1 | **No bound on accepted connections.** `max_in_flight` caps concurrent requests *per connection*, but nothing caps the number of connections, and it is not exposed in the node configuration — the default is always used. A single authorized client can therefore multiply the effective in-flight limit by opening more sockets. | `tourillon/core/transports/server.py:74-80`, `tourillon/core/transports/session.py:57` |
| F2 | **Read repair creates one task per stale replica with no admission control.** The semaphore bounds how many repairs run at once, not how many tasks are created and queued; a scan-like read over many distinct stale keys can queue one task per key. `sample_rate` is the only valve. | `tourillon/core/services/store.py` (`_start_repair`, `_repair_slots`) |
| F3 | **A requested read quorum narrows read repair.** Once the quorum is met the remaining reads are cancelled, so replicas that had not yet answered are absent from the answer set and are not repaired even when stale. Repair coverage is widest when no read quorum is requested. | `tourillon/core/services/store.py` (`_collect_reads`, `_repair_targets`) |
