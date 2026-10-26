# AGENTS.md — Tourillon

Authoritative context for AI coding agents working in this repository.
Human contributors should read this too: it is the shortest accurate description of how the project fits together.

> This file is the single source of truth for agent behaviour.
> `.github/copilot-instructions.md` restates only the non-negotiable rules; path-scoped rules live in `.github/instructions/*.instructions.md`.

---

## 1. What Tourillon is

Tourillon is a **leaderless, peer-to-peer distributed key-value database** written in Python.
There is no controller node, no coordinator election and no primary replica: every node runs the same code and any node can coordinate any request.

Design pillars actually implemented today:

- **Consistent-hash ring** of virtual nodes (vnodes), sized per node via a size class.
- **Fixed partition grid**: the hash space is cut into `2**pshift` partitions grouped into `2**gshift` storage groups.
- **Preference lists** computed by walking the ring from a partition's successor vnode.
- **Per-request quorums** (`quorum_read` / `quorum_write` integers), not named consistency levels.
- **Hybrid logical clocks (HLC)** for last-writer-wins conflict resolution with deterministic tie-breaks.
- **Hinted handoff** when a preferred replica is not writable, replayed later by a background replayer.
- **Gossip** membership with a phi-accrual failure detector.
- **Streaming partition transfers** when the ring changes.
- **Mandatory mTLS** on every socket, peer and client alike.

> **Originality rule — important.**
> Tourillon takes inspiration from the general literature on leaderless replicated stores, but it is **not** a clone of any existing product.
> Never name, cite, or compare the project to specific commercial or open-source databases in code, comments, docstrings, tests, docs, commit messages or PR descriptions.
> Describe mechanisms in neutral, first-principles vocabulary (ring, partition, preference list, quorum, handoff, anti-entropy) and design what is right for Tourillon rather than copying another system's semantics.

---

## 2. Repository layout

| Path | Role |
| --- | --- |
| `tourillon/` | Node runtime (the server). Ships the `tourillon` CLI. |
| `tourillon/core/` | Domain logic: ring, partitioning, placement, gossip, probe, store, transfer, transports, models, ports. **No third-party backend code here.** |
| `tourillon/infra/` | Concrete adapters: LMDB storage, OpenTelemetry telemetry. |
| `tourillon/bootstrap/` | Composition root: `deps.py` wiring, `main.py` CLI, `usecases/` commands, `handlers/` envelope handlers. |
| `tourlib/` | Shared library: envelope framing, transport, wire models, ports (`Serializer`, `Persistence`, `MtlsCrypto`) and their adapters. Used by **all** other packages. |
| `tourclient/` | Async client library with a service registry. Installable dependency of `tourctl`. |
| `tourctl/` | Operator / user CLI built on `tourclient`. Ships the `tourctl` CLI. |
| `tests/` | Mirrors the package layout; `tests/e2e/` drives real processes. |
| `scripts/` | License-header check and the native bundle builder / installers. |
| `docs/` | `architecture.md` (deep reference) and `backlog.md` (known gaps and bugs). |

uv workspace members are `tourillon`, `tourctl`, `tourlib`, `tourclient`.
Each member builds its own wheel and is installed editably during development.
`tourillon` depends on `tourlib`; `tourctl` depends on `tourclient` and `tourlib`.
Do not force-include package sources: copied files shadow editable imports.

---

## 3. Environment and commands

Python **3.14+** is required. All commands run through `uv`.

```bash
uv sync --extra dev            # install workspace + dev tools
uv run pytest                  # full suite, coverage gate (>= 90%)
uv run pytest -m "not e2e" --no-cov -q              # fast loop, skips e2e
uv run pytest tests/tourillon/test_store.py          # one file
uv run pytest tests/tourillon/test_store.py::test_x  # one test
uv run ruff check .            # lint
uv run black --check .         # format check
uv run pre-commit run --all-files                    # all hooks
uv build                       # build distributions
```

The end-to-end tests spawn real node processes and dominate the runtime (~60 s without them,
~230 s with). Iterate with `-m "not e2e" --no-cov`, then run the full `uv run pytest` once before
you hand the change over — `--no-cov` is required on a partial run because the coverage gate is
computed over the whole suite.

Baseline you can rely on: **the test suite is green** (705 tests, ~93.9% coverage).

### Formatting debt — read before running a formatter

`ruff check .` and `black --check .` currently fail **repo-wide on pre-existing code**, not because of your change.
Therefore:

- **Never run `black .` or `ruff check --fix .` across the whole repository.** That produces a multi-thousand-line diff that buries your actual change and will be rejected.
- Format and lint **only the files you touched**:
  ```bash
  uv run black tourillon/core/services/store.py
  uv run ruff check tourillon/core/services/store.py
  ```
- Treat pre-existing violations in files you did not touch as out of scope.

---

## 4. Non-negotiable rules

1. **Never read `.draft/` or `.ai-chat.md`.** They are excluded on purpose.
2. **Never add `__all__` outside `__init__.py`.**
3. **Every `.py` file must start with the Apache 2.0 header** (see §5). CI and a pre-commit hook enforce it.
4. **Cyclomatic complexity per function must stay `<= 8`** (`ruff` rule `C901`). Extract helpers instead of nesting.
5. **E2E tests must exercise the real runtime.** No monkeypatching, stubbing or faking of the application under test in `tests/e2e/`.
6. **Never name other database products** anywhere in the repository (see §1).
7. **Never commit secrets, certificates or key material.** `.certs/`, `.config/`, `.data/` are git-ignored; keep generated material there or in `tmp_path`.
8. **Do not change `uv.lock` by hand.** It is committed deliberately for reproducible builds.
9. **Add tests for both success and failure paths** whenever you change runtime behaviour.

### Development compatibility policy

Tourillon is pre-alpha. Unless explicitly requested for a task, do not add
backward-compatibility layers, legacy-format detection, migration machinery or
dedicated legacy-data errors. Update producers, consumers, tests and documentation
to the new format together. Development data may be recreated, but never delete
existing data automatically.

---

## 5. Required file header

Every Python file begins with exactly this block, before any import or docstring:

```python
# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
```

`scripts/check_license_header.py` only looks for `# Copyright` and `# Licensed under the Apache License, Version 2.0` within the first 20 lines — but always paste the full block.

---

## 6. Architecture rules that agents get wrong

### Ports and adapters

Domain code depends on **protocols**, never on concrete backends.

- Protocols live in `tourillon/core/ports/` (`Storage`, `SessionProvider`, `Session`, `Batch`, `StorageProvider`, `Telemetry`, `TelemetryProvider`) and in `tourlib/port/` (`Serializer`, `Persistence`, `MtlsCrypto`).
- Implementations live in `tourillon/infra/` and `tourlib/adapter/`.
- Selection happens **only** in `tourillon/bootstrap/deps.py`.

If you need a new backend capability: extend the protocol, implement it in `infra`/`adapter`, wire it in `deps.py`. Never `import lmdb` or `import opentelemetry` from `tourillon/core/`.

### The envelope wire protocol

All network traffic is a single `Envelope` frame (`tourlib/envelope.py`):

```
32-byte header | UTF-8 kind | payload bytes
header := >B H B 16s I B 7s
          proto_version, schema_id, mode, correlation_id(uuid16), payload_len, kind_len, reserved
mode := 0 (RPC) | 1 (fire-and-forget)
```

Rules:

- `kind` is lowercase dotted `domain.action` and is regex-validated. Responses conventionally use `<kind>.accept` and `<kind>.reject`.
- Build responses with `replay_envelope(kind=..., request=..., payload=...)` so `proto_version`, `schema_id`, `mode` and `correlation_id` are preserved. Never hand-build a response envelope.
- Payloads are encoded through the injected `Serializer` (MsgPack by default) — never `json.dumps`, never `pickle`.
- Handlers are async generators registered by kind:

  ```python
  dispatcher = client_dispatcher()      # or peer_dispatcher()

  @dispatcher.on("kv.get")
  async def get(request: Envelope) -> AsyncIterator[Envelope]:
      ...
      yield replay_envelope(kind="kv.get.accept", request=request, payload=...)
  ```

- `Dispatcher.register` **raises** on a duplicate kind. One handler per kind, globally.
- There are **two dispatchers**: `peer_dispatcher()` (node-to-node) and `client_dispatcher()` (user-facing). Put a handler on the right one.

**When you add or change a kind, update all four places:**

1. the server handler in `tourillon/bootstrap/handlers/`,
2. the wire model in `tourlib/models/`,
3. the client service in `tourclient/core/services/`,
4. the registry entry in `tourclient/core/services/registry.py` (including `supported_endpoints`).

Current endpoint matrix: `probe` → client+peer, `kv` → client+peer, `node` → peer, `inspect` → peer.
Purely internal kinds (`gossip.*`, `replication.*`, `transfer.*`) intentionally have **no** client service.

### Storage

- Every operation is scoped to a **partition group**: get a `SessionProvider` with `storage.session_provider_for(group)`, then `async with provider.session(write=True) as session:`.
- Use `session.batch()` when several operations belong together; a single `await batch.result()` resolves them.
- Keyspaces and key/value schema types live in `tourillon/core/models/storageschema.py`. Do not invent ad-hoc key encodings.

### Configuration and node state

- Everything persisted goes through the `Persistence` port (TOML by default), including mutable node state.
- Parsing, defaulting and validation belong in the config/model types (`tourillon/core/config/`, `tourillon/core/models/config.py`), **not** in CLI handlers.
- mTLS is mandatory: `EnvelopeServer` rejects any SSL context whose `verify_mode` is not `CERT_REQUIRED`.

### CLI

Both CLIs use **cyclopts** (not click/typer/argparse).

- Command functions live in `*/bootstrap/usecases/` and stay thin: parse options, delegate to `core/`, return a string.
- The framework prints the returned string; use `return`, not `print`.
- User-facing failures exit via `SystemExit(message)` — no tracebacks.
- JSON output is consistently `json.dumps(..., ensure_ascii=False, indent=2, sort_keys=True)`.

---

## 7. Testing expectations

- Tests mirror packages: `tests/<package>/test_<module>.py`.
- Mark every test file with its package marker: `@pytest.mark.tourillon` / `tourctl` / `tourlib`, and `@pytest.mark.e2e` for E2E.
- `asyncio_mode = "auto"` is enabled; async tests need no decorator boilerplate beyond the marker conventions already used.
- Use the `mtls_contexts` fixture (`tests/conftest.py`) for TLS material; it generates a real CA plus server/client leaf certs under `tmp_path`.
- Helpers live in `tests/helpers/` (`create_mtls_contexts`, `generate_ca`, `make_envelope`, `start_envelope_server`). Reuse them instead of re-rolling servers.
- Unit tests may use fakes for **ports** (that is what ports are for). E2E tests may not fake anything.
- E2E is pytest-bdd: `.feature` files in `tests/e2e/features/`, step definitions in `tests/e2e/test_*_e2e_bdd.py`, process orchestration in `tests/e2e/journey.py`.
- E2E requires real `tourillon` / `tourctl` executables on `PATH` or passed via `--tourillon-bin` / `--tourctl-bin`; otherwise the session fails fast. Locally, `uv sync --extra dev` puts them in `.venv`.
- Coverage gate is **90%** over `tourillon`, `tourctl`, `tourlib`. Note `tourclient` is **not** measured today.

---

## 8. Working agreement for agents

**Before changing code**

- Read `docs/architecture.md` for the subsystem you are touching, and `docs/backlog.md` to check the item is not already characterised.
- Locate the existing pattern and follow it. This codebase is highly consistent; consistency beats cleverness.

**While changing code**

- Keep changes surgical. Do not reformat, re-sort imports, or "tidy" files you were not asked to change.
- Respect the layering: `core` → ports only; `infra`/`adapter` → implementations; `bootstrap` → wiring.
- Keep functions under complexity 8 and keep the license header intact.
- Prefer `logger` from `tourillon.core.log` over `print` in runtime code.

**Before declaring done**

1. `uv run pytest tests/<touched area>` — targeted tests pass.
2. `uv run pytest` — full suite and the 90% gate still pass.
3. `uv run black <changed files>` and `uv run ruff check <changed files>` — clean.
4. New behaviour has both a success test and a failure test.
5. No product names from §1, no secrets, no `.draft/` content leaked.

**Commits and PRs**

- Imperative, scoped subject lines: `store: enforce write quorum before acking`.
- Explain *why* in the body, reference the `docs/backlog.md` item when applicable.
- Include the trailer when the change was authored with an assistant:
  ```
  Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
  ```

---

## 9. Where to start on common tasks

| Task | Entry points |
| --- | --- |
| Add a client-visible operation | `tourlib/models/` → `tourillon/bootstrap/handlers/` → `tourclient/core/services/` → `registry.py` → `tourctl/bootstrap/usecases/` |
| Change read/write/replication semantics | `tourillon/core/services/store.py`, `tourillon/core/partition/placement.py` |
| Change membership or failure detection | `tourillon/core/gossip/`, `tourillon/core/probe/` |
| Change ring / partition math | `tourillon/core/spaces/`, `tourillon/core/partition/partitioner.py` |
| Change startup / shutdown / join | `tourillon/core/services/lifecycle.py`, `tourillon/core/manager.py` |
| Change rebalancing | `tourillon/core/transfer/`, `tourillon/core/services/transfer.py` |
| Add a storage capability | `tourillon/core/ports/storage.py` → `tourillon/infra/storage/` → `bootstrap/deps.py` |
| Add a CLI command | `tourctl/bootstrap/usecases/` + `tourctl/core/` (or `tourillon/bootstrap/usecases/`) |
