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
| A7 | **No read repair.** A read selects a winning version but never writes it back to stale or missing replicas, so divergence is only healed by anti-entropy and hint replay. | `tourillon/core/services/store.py` (`get`, `_collect_reads`) | Decide explicitly whether Tourillon wants synchronous or background repair. |

---

## B. Unimplemented code paths

| # | Item | Evidence |
| --- | --- | --- |
| B1 | **`NodeLifecycle.leave()` is an empty stub** (`...`). A node cannot be drained or removed gracefully; there is also no `tourctl node leave` command. Decommissioning currently means killing the process and letting failure detection react. | `tourillon/core/services/lifecycle.py:177-178` |
| B2 | **Seed bootstrap has a missing pre-check.** `# todo(souls) some pre check condition` guards an empty branch in the seed attempt path. | `tourillon/core/gossip/bootstrapper.py:123` |
| B3 | **Hard-coded retry policy.** `BackoffRetry()` is constructed inline with a `# todo(souls): make configurable` marker; backoff is not exposed in configuration. | `tourillon/core/services/lifecycle.py:432` |
| B4 | **Local write does not re-check placement.** `# todo(souls): probably should check again if node is in the preference list` — a replica applies a `replication.write` without confirming it is still a valid target for that key. | `tourillon/core/services/store.py:122` |
| B5 | **Ring rebuild is O(n) per accepted membership update**, with an O(n²) bootstrap merge, flagged in code as a scalability concern at the 10k-node target. | `tourillon/core/gossip/topology.py:170` |

---

## C. Defects

| # | Severity | Item | Evidence |
| --- | --- | --- | --- |
| C1 | Medium | **Client transport has no connection timeout.** The node runtime wraps `connect()` with `ENVELOPE_CONNECTION_TIMEOUT` (`asyncio.timeout` in the bootstrapper, `asyncio.wait_for` in the peer pool), but `tourclient` calls `transport.connect()` unguarded. `tourctl` can therefore hang indefinitely against a blackholed address instead of failing with the documented bounded error. | `tourclient/core/transport.py:48-49`, `tourclient/client.py:141` vs `tourillon/core/peer/pool.py:219,249` and `tourillon/core/gossip/bootstrapper.py:118` |
| C2 | Low | **Dead duplicate assignment.** `PeerFailureDetector.__init__` assigns `self._lock = asyncio.Lock()` twice. Harmless today, but it indicates the constructor was edited without review and the second lock silently replaces the first. | `tourillon/core/probe/detector.py:48,52` |
| C3 | Low | **`__all__` outside `__init__.py`.** Several modules declare `__all__` although the project convention forbids it outside package initialisers. | e.g. `tourlib/port/persistence.py` |

---

## D. Tooling, tests and process

| # | Item | Evidence |
| --- | --- | --- |
| D1 | **Repo-wide lint and format debt.** `uv run ruff check .` reports 113 errors (mostly `I001` import ordering, plus `C901` complexity) and `uv run black --check .` would rewrite 168 files. Until this is paid down, contributors and CI must lint only changed files. Pay it down deliberately, in a dedicated formatting-only PR, never mixed with behaviour changes. | `uv run ruff check .`, `uv run black --check .` |
| D2 | **`C901` violations in shipped code**, e.g. `EnvelopeClient._read_loop` at complexity 9 against the limit of 8. | `tourlib/transport.py:200` |
| D3 | **`tourclient` is not in the coverage gate.** `--cov` covers `tourillon`, `tourctl` and `tourlib` only, so the entire client library's coverage is unmeasured. | `pyproject.toml` (`addopts`) |
| D4 | **`--journey-artifacts` is unusable from the default invocation.** `pytest_addoption` lives in `tests/e2e/conftest.py`, a non-initial conftest, so the flag is only accepted when `tests/e2e` is passed explicitly; `uv run pytest --journey-artifacts ...` fails with "unrecognized arguments". Moving the option to the root `tests/conftest.py` would fix it. | `tests/e2e/conftest.py:21-36` |
| D5 | **Under-tested runtime internals.** The weakest modules are the peer pool, storage group, node lifecycle, transfer planner, transport session, phi-accrual detector and gossip bootstrapper — exactly the code paths where distributed bugs hide. | `uv run pytest --cov-report=term-missing` |
| D6 | **E2E relies on bounded polling.** `Journey.wait_for` polls with short sleeps and fixed budgets (`COMMAND_TIMEOUT`, `CONVERGENCE_TIMEOUT`, `SCENARIO_TIMEOUT`); these may be tight on loaded CI runners. Prefer waiting on observable state and keep the budgets reviewed. | `tests/e2e/journey.py` |
| D7 | **Shallow licence-header check.** `check_license_header.py` only looks for two substrings in the first 20 lines, so a mangled or duplicated header passes unnoticed — one such header was found and fixed in `tourillon/bootstrap/usecases/config.py`. Consider matching the full block. | `scripts/check_license_header.py` |
| D8 | **No type checker.** There is no mypy/pyright configuration or CI step, although the codebase is fully annotated and `.gitignore` already anticipates `.mypy_cache/`. | `pyproject.toml`, `.gitignore` |

---

## E. Design questions to settle before implementing

These are not defects; they are decisions Tourillon has not made yet. Resolve them deliberately — do not copy another system's answer.

1. **Consistency API.** Do quorums stay raw integers, or does Tourillon expose named levels mapped to a replication factor? What is the default when a request specifies nothing?
2. **Conflict resolution.** Last-writer-wins by HLC is simple but loses concurrent updates. Does Tourillon need causality tracking, sibling values, or application-supplied merge?
3. **Repair strategy.** Synchronous read repair, background repair, or anti-entropy only?
4. **Namespace lifecycle.** How is a namespace created, configured, and deleted, and what happens to data on deletion?
5. **Failure-domain model.** What labels does a node advertise, how do they reach other nodes through gossip, and how strictly does placement enforce them when constraints cannot be satisfied?
6. **Decommissioning.** What does a graceful leave guarantee — hand off all owned ranges before exiting, or just stop accepting writes?
7. **Tombstone lifetime.** When may a tombstone be collected without risking resurrection of deleted data?
