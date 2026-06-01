# GitHub Copilot Instructions — Tourillon

## Project Overview

Tourillon is a **leaderless, peer-to-peer distributed key-value database** written in
Python 3.14.  Every feature is specified as a numbered *proposal* in `proposals/` before
any code is written.  The implementation must satisfy the proposal's **exit criteria**
exactly — no more, no less.

Package manager: **uv**.  Virtual-env: `.venv/` (managed by uv).
Entry points: `tourillon` (node daemon / operator CLI) and `tourctl` (cluster-admin CLI).

---

## Repository Layout

```
tourillon/          # Node daemon — core library + infra adapters
  core/             #   Pure domain logic (no external I/O, no third-party imports)
    ports/          #     Protocol definitions (Ports in hexagonal arch)
    structure/      #     Frozen dataclasses: config, envelope, contexts, ring types
    transport/      #     Dispatcher, framing, TcpServer, TcpClient, connection pool
  bootstrap/        #   Startup wiring: config loading, process lock, Bootstraper
  infra/            #   Adapters: TLS, PKI (cryptography), serializer (msgpack), CLI (Typer)
    cli/            #     tourillon CLI commands (Typer apps)
    pki/            #     CryptographyCaAdapter, CryptographyCertIssuerAdapter
    serializer/     #     MsgpackSerializerAdapter
    tls/            #     build_server/client_ssl_context, TlsValidationError
tourctl/            # Operator CLI — cluster-admin commands (Typer)
  infra/cli/        #     tourctl CLI commands
tests/
  unit/             # In-memory adapters, no sockets, no real filesystem I/O
  e2e/              # Real filesystem via pytest tmp_path; no real network sockets
scripts/            # Dev tooling (check_license_header.py)
proposals/          # Feature proposals (source of truth for every implementation)
```

---

## Architecture Rules (non-negotiable)

1. **`tourillon/core/` must never import from `tourillon/infra/`, `tourillon/bootstrap/`,
   `tourctl/`, or any third-party library** (`msgpack`, `cryptography`, `ssl`, `typer`,
   `rich`, …).  It may only use the Python standard library.

2. **Ports (`tourillon/core/ports/`) are `Protocol` classes** — structural sub-typing.
   Adapters in `tourillon/infra/` implement them; the core never names the concrete type.

3. **All domain dataclasses are `frozen=True`.**  No subsystem mutates a config, envelope,
   ring token, or node-state object after construction.

4. **No plaintext TCP.**  Every listener uses `ssl.CERT_REQUIRED`.  No fallback mode exists.

5. **No `*_file` indirections in config.**  TLS material is always base64-encoded PEM
   stored inline in `config.toml` / `contexts.toml`.

6. **Duration fields are unit-embedded strings** (`"10s"`, `"2m"`, `"1h"`, `"500ms"`).
   Size fields are unit-embedded strings (`"512Ki"`, `"1Mi"`, `"4Gi"`).
   `parse_duration` and `parse_bytes` live in `tourillon/bootstrap/config.py`.

7. **`contexts.toml` writes are atomic** (`tempfile.mkstemp` + `os.replace`, mode 0600).

8. **`config.toml` is written at mode 0600** — it embeds the node private key.

9. **Handler registration is startup-time only.**  `Dispatcher.register` / `.on` are
   called during bootstrap; at request time only `lookup` is called.

10. **`pid.lock` is acquired before any socket binding or state-file access.**

---

## Key Types Quick Reference

| Type | Module | Notes |
|---|---|---|
| `TourillonConfig` | `core/structure/config.py` | frozen; duration/size fields are `str` |
| `Envelope` | `core/structure/envelope.py` | frozen; `encode()` / `decode()` / `create()` |
| `Dispatcher` | `core/transport/dispatcher.py` | `@dispatcher.on(kind)` or `.register(kind, fn)` |
| `TcpServer` | `core/transport/server.py` | wraps `asyncio.start_server`; mTLS |
| `TcpClient` | `core/transport/client.py` | `.request()` / `.stream()` / `.send()` |
| `SerializerPort` | `core/ports/serializer.py` | `Protocol`; `schema_id`, `encode`, `decode` |
| `MsgpackSerializerAdapter` | `infra/serializer/msgpack.py` | uint128 → `ExtType(1)` |
| `TourillonCore` | `bootstrap/core.py` | composition root; injected into dispatchers |
| `Bootstraper` | `bootstrap/bootstraper.py` | orchestrates startup (`start_node()`) |
| `ConfigError` | `bootstrap/config.py` | fatal config error; caught by bootstrap entry point |
| `ProcessLockPort` | `core/ports/state.py` | `Protocol`; `acquire()` / `release()` |
| `FileProcessLockAdapter` | `infra/process_lock.py` | `fcntl` (POSIX) / `msvcrt` (Windows) |
| `ContextsFile` | `core/structure/contexts.py` | `get()` / `upsert()` |
| `NodeState` | `core/structure/ring.py` | `MemberPhase` FSM |
| `TopologyManager` | `core/ring/topology.py` | maintains live ring |
| `Partitioner` | `core/ring/partitioner.py` | `ranges_for()`, `placement_for()` |

---

## Envelope Wire Format

```
offset  bytes  field
0       1      proto_version  uint8   (must be 1)
1       2      schema_id      uint16  (1 = msgpack, 0 = raw)
3       16     correlation_id bytes   (UUID v4, 16 bytes)
19      4      payload_len    uint32
23      1      kind_len       uint8   (1–64)
24      N      kind           utf-8
24+N    M      payload
```

`kind` is dot-separated ASCII (`kv.put`, `gossip.push`, `error.proto_version_unsupported`).
Max payload: 4 MiB (`MAX_PAYLOAD_DEFAULT`).  Max in-flight per connection: 128.

---

## CLI Conventions

- All commands use **Typer**.  Apps are composed (sub-commands via `app.add_typer`).
- **`tourillon/` commands**: `tourillon pki ca`, `tourillon pki issue`,
  `tourillon config generate`, `tourillon config generate-context`, `tourillon node start`.
- **`tourctl/` commands**: `tourctl config use-context`, `tourctl node inspect`, etc.
- Exit codes: `0` = success, `1` = user/config error, `2` = crypto/internal error.
- Success lines go to **stdout**; errors go to **stderr**.
- The daemon (`tourillon node start`) uses Python `logging` only — **no `print()` or
  `Console.print()`** anywhere under `tourillon/`.

---

## Testing Conventions

- **Unit tests** (`tests/unit/`): in-memory adapters, `tmp_path` for filesystem, no sockets.
- **E2e tests** (`tests/e2e/`): real filesystem via `pytest tmp_path`; no real network sockets.
- **Asyncio**: `asyncio_mode = "auto"` in `pyproject.toml` — use `async def test_*` freely.
- **Markers**: each proposal has its own pytest marker (see `[tool.pytest.ini_options]` in
  `pyproject.toml`).  Tag new tests with the relevant marker.
- **Coverage gate**: `--cov-fail-under=90` — every PR must maintain ≥ 90 % coverage.

Run the full suite:
```bash
uv run pytest
```
Run a single proposal's tests:
```bash
uv run pytest -m bootstrap
```

---

## CI Gates (all must pass before merge)

| Gate | Command |
|---|---|
| Formatting | `uv run black --check .` |
| Linting + import sort | `uv run ruff check .` |
| Cyclomatic complexity ≤ 10 | `uv run radon cc tourillon -n C -s` |
| Maintainability index ≠ F | `uv run radon mi tourillon -s` |
| Tests + coverage ≥ 90 % | `uv run pytest` |
| Pre-commit all hooks | `uv run pre-commit run --all-files` |

---

## Pre-commit Hooks (`.pre-commit-config.yaml`)

All hooks use `language: system` (the uv venv).  Run `uv sync --extra dev` first.

| Hook | Tool | What it does |
|---|---|---|
| `black` | black | Auto-format Python |
| `ruff` | ruff check --fix | Lint + auto-fix + isort |
| `autoflake` | autoflake | Remove unused imports/variables |
| `trailing-whitespace` | pre-commit-hooks | Strip trailing whitespace |
| `end-of-file-fixer` | pre-commit-hooks | Ensure files end with newline |
| `check-yaml` | pre-commit-hooks | Validate YAML syntax |
| `check-toml` | pre-commit-hooks | Validate TOML syntax |
| `check-license-header` | scripts/check_license_header.py | Apache 2.0 header required |

---

## License Header

**Every Python source file must start with:**

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

The pre-commit `check-license-header` hook enforces this.

---

## Dependency Management

| Task | Command |
|---|---|
| Install all dev deps | `uv sync --extra dev` |
| Add a runtime dep | `uv add <package>` |
| Add a dev dep | `uv add --dev <package>` |
| Lock file update | `uv lock` |
| Run any tool in venv | `uv run <tool>` |

Runtime dependencies (from `pyproject.toml`):
`msgpack`, `rich`, `tomli-w`, `typer`, `cryptography==46.0.3`.

---

## Proposal-Driven Development

All new features originate from a proposal in `proposals/`.
The proposal defines:
- **CLI contract** (commands, flags, stdout/stderr output, exit codes)
- **Data model** (dataclasses, wire formats, TOML schemas)
- **Core invariants** (rules that must never be violated)
- **Sequence / flow** (step-by-step happy path)
- **Error paths** (what the user sees for each failure)
- **Interfaces** (informative Python snippets — not prescriptive)
- **Test scenarios** (numbered table — each scenario becomes a test)
- **Exit criteria** (checklist that must be fully ticked before the proposal is `Implemented`)
- **Proposed code organisation** (file creation order)

When implementing a proposal:
1. Read the full proposal before writing any code.
2. Create/modify files in the **mandatory creation order** listed under
   "Proposed code organisation".
3. Write tests matching every numbered scenario.
4. Tick every exit criterion.
5. Update the proposal `**Status:**` to `Implemented`.
