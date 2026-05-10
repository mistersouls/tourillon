# GitHub Copilot Instructions — Tourillon

## Project identity

Tourillon is a **leaderless, peer-to-peer distributed key-value database**
written in Python 3.14. It guarantees deterministic convergence, ordered
hinted handoff, consistent-hashing ring partitioning, and mandatory mTLS for
all traffic. There is no coordinator, no leader-election, and no plaintext
fallback.

The project is split across two CLI entry points:

| Package | Binary | Role |
|---------|--------|------|
| `tourillon/` | `tourillon` | Node daemon — starts, joins, leaves, serves KV traffic |
| `tourctl/` | `tourctl` | Operator CLI — issues commands to a running node over mTLS |

---

## Proposals — authoritative specifications

All design decisions, invariants, interfaces, and test scenarios are defined in
`proposals/`. **Consult the relevant proposal before writing or modifying any
code.** Never contradict a proposal's design decisions in code or comments.

| File | Covers |
|------|--------|
| `proposals/proposal-bootstrap-05022026-001.md` | PKI (`tourillon pki ca`), config format (`config.toml`, `contexts.toml`), `tourillon config generate`, `tourctl config generate-context`, `Envelope` wire format, `Dispatcher`, `TcpClient`, mTLS transport layer |
| `proposals/proposal-ring-first-node-08022026-002.md` | `tourillon node start` (first-node bootstrap), consistent-hashing ring, `HashSpace`, `VNode`, `Ring`, `Partitioner`, `PlacementStrategy`, `TopologyManager`, `MemberPhase` FSM, `NodeState`, `state.toml`, `FailureDetector`, `ProbeManager` |
| `proposals/proposal-node-inspect-05102026-003.md` | `tourctl node inspect`, `node.inspect` / `node.inspect.peer_view` envelope kinds, single-hop forwarding, `NodeInspectResponse`, `NodePeerViewResponse`, partition range display |


When implementing a feature, read the full proposal first: summary → design → interfaces → test scenarios → exit criteria.

---

## Language and runtime

- Target **Python 3.14** exclusively. Never suggest syntax or stdlib APIs
  deprecated or removed in 3.14, and never shim for older versions.
- Use modern Python idioms: `X | Y` union types, `match` statements,
  `TypeAlias`, `Self`, `ParamSpec`, `TypeVarTuple`, `@override`, PEP 695
  type-parameter syntax where applicable.
- All public functions, methods, and class attributes **must** carry PEP 484
  type annotations. Never omit `-> None` on procedures.
- Prefer `dataclass(frozen=True)` for value objects; use `StrEnum` for
  string-valued enumerations (e.g. `NodeSize`, `MemberPhase`).

---

## Asyncio rules — mandatory

Tourillon is **fully asynchronous**. All I/O, networking, replication,
handoff, repair, and coordination paths MUST use `asyncio`.

- All new functions MUST be `async def` unless purely CPU-bound.
- Use **`asyncio.TaskGroup`** for structured concurrency; never create
  orphan tasks.
- Use **`asyncio.timeout()`** instead of `wait_for()`.
- Use **`asyncio.get_running_loop()`**; never `get_event_loop()`.
- Never block the event loop: no `time.sleep()`, no sync file I/O,
  no sync network calls. Use `await asyncio.to_thread(...)` for blocking ops.
- Never use callbacks-based APIs; always prefer awaitable forms.
- Preferred concurrency primitives: `TaskGroup`, `asyncio.Semaphore`,
  `asyncio.Event`, `asyncio.Queue`, Streams API
  (`asyncio.open_connection`, `asyncio.start_server`).
- Never use deprecated asyncio APIs.

---

## File header — mandatory

Every `.py` file **must** start with this exact Apache 2.0 header block,
followed by a blank line, then the module docstring:

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
"""<one-line module summary ending with a period>."""
```

---

## Code style

- **Formatter:** Black, `line-length = 88`. Emit code that Black would not
  reformat.
- **Linter:** Ruff rule sets `E W F I N UP B SIM C90`; fix all violations.
- **Imports:** one import per line; stdlib → third-party → first-party
  (`tourillon`, `tourctl`), separated by blank lines (isort/ruff-I order).
- **Docstrings:** PEP 257. One-line summary ending with `.`; blank line before
  extended description; imperative mood (`Return …`, `Raise …`).
- No wildcard imports (`from x import *`). No unused imports or variables.
- No decorative separator comments (`# ----`, `# ====`).
- Max cyclomatic complexity: **8** (Ruff `mccabe` C90 rule, `max-complexity = 8` in `pyproject.toml`). Refactor into smaller helpers rather than raising complexity above 8.
- **Prefer `logging` over any print-like output** for all operational and progress messages. Use `logger.info`, `logger.debug`, `logger.warning`, `logger.error`. Never use `print()`.
  - **`tourillon` (daemon):** zero `print()` and zero `Console.print()` — every output line goes through `logging`. The process calls `setup_logging()` at startup (`tourillon/bootstrap/log.py`) before any other code runs.
  - **`tourctl` (operator CLI):** Rich `Console.print()` is permitted for explicit user-facing terminal output in Typer commands. Internal helpers still use `logging`.

---

## Architecture — hexagonal (ports & adapters)

Tourillon uses a strict **hexagonal architecture**:

```
tourillon/
  bootstrap/   # startup sequence: config loading, pid.lock, TLS context wiring
  core/        # domain logic — zero infrastructure imports
    ring/      # HashSpace, VNode, Ring, Partitioner, PlacementStrategy
    gossip/    # GossipEngine, GossipPayload
    kv/        # StoreKey, HLC, Version, Tombstone, store logic
    lifecycle/ # MemberPhase FSM, Member, state.toml reader
    handlers/  # ConnectionHandler implementations (envelope routing)
    ports/     # Protocol interfaces (storage, network, serializer, clock, pki)
    structure/ # Pure dataclasses shared across layers
  infra/       # adapters — imports third-party libraries
    cli/       # Typer CLI commands (tourillon binary)
    tls/       # ssl.SSLContext factory, mTLS helpers
    pki/       # cryptography library adapter (CertificateAuthorityPort, etc.)
    store/     # on-disk KV storage adapter
    transport/ # asyncio TCP server/client, Envelope read/write, Dispatcher

tourctl/
  core/
    commands/  # tourctl subcommands (node join, node leave, kv get, …)
  infra/
    cli/       # Typer entry point for tourctl
```

Rules:
- **`core/` never imports from `infra/`** or from any third-party library
  except `msgpack` indirectly via `SerializerPort`.
- The core layer depends only on **`ports/` Protocols**. Infra adapters
  implement those protocols and are injected at startup.
- `SerializerPort`, `StoragePort`, `ClockPort`, `CertificateAuthorityPort`,
  `CertificateIssuerPort` live in `core/ports/`. Their implementations live in
  `infra/`.

---

## Architecture invariants — non-negotiable

| Invariant | Rule |
|-----------|------|
| Leaderless | Never introduce a single-node coordination point or leader election. |
| Deterministic routing | Route/replicate via the consistent-hashing ring; never hardcode node addresses or use random tiebreaks for routing. |
| mTLS everywhere | All inter-node and client-node sockets must enforce mutual TLS. Never allow a plaintext fallback. |
| Serializer abstraction | The core layer never imports `msgpack` directly. All encode/decode goes through `SerializerPort`. |
| Immutable Ring | `Ring` mutations (`add_vnodes`, `drop_nodes`) return new instances. Never mutate in place. |
| Phase guard — writes | Code serving `kv.put` / `kv.delete` must guard on `phase == READY`. |
| Phase guard — reads | Code serving `kv.get` or secondary replication must guard on `phase in (READY, DRAINING)`. |
| Phase persistence before gossip | The updated `MemberPhase` is written to `state.toml` **before** the corresponding gossip record is emitted. Never reverse this order. |
| Generation increment | `Member.generation` is incremented **exactly once** per `IDLE → JOINING` transition. Never on retry, crash-restart, or any other event. |
| PartitionPlacement is ephemeral | Never persist `PartitionPlacement`. Always recompute it after a ring mutation. |
| Quarantined staging | Received partition data is invisible to `kv.get` until `rebalance.commit` succeeds. |
| KV socket lifecycle | The KV TCP socket is bound **only when `phase == READY`**; closed otherwise. There is no application-layer redirect. |
| Inline PEM | All TLS material is stored as base64-encoded PEM inline in TOML files. No `*_file` path variants anywhere. |
| No print for logging | Use `logging` (structured `LogRecord` extra dict). Never `print()`. |
| Atomic config writes | `contexts.toml` is written via temp-file + `os.replace()`. Never write directly. |

---

## Envelope kinds — naming convention

All `Envelope.kind` strings follow `<domain>.<verb>` or
`<domain>.<noun>.<verb>` patterns. Examples from proposals:

```
kv.put             kv.get             kv.delete
kv.replicate       kv.hint
node.joined        node.inspect
ring.fetch         ring.propose_join
rebalance.plan.request  rebalance.plan  rebalance.transfer  rebalance.commit
error.proto_version_unsupported  error.payload_too_large  error.kind_len_invalid
```

Unknown `kind`s close the connection without a response envelope.

---

## Toolchain

The project uses [`uv`](https://docs.astral.sh/uv/) for all environment and
dependency management. Never suggest `pip install`, `python -m venv`, or `pip
freeze`.

```bash
# Install / refresh dev environment
uv sync --extra dev

# Install git hooks (once per checkout)
uv run pre-commit install

# Run all quality checks
uv run pre-commit run --all-files

# Run tests with coverage
uv run pytest

# Run only a specific proposal area
uv run pytest -m lifecycle
uv run pytest -m ring
uv run pytest -m kv
```

---

## Dependency rules

- Runtime → `[project.dependencies]` in `pyproject.toml`.
- Dev/tooling → `[project.optional-dependencies] dev`.
- Prefer stdlib. Add third-party only when it meaningfully reduces complexity.
- Pin with `>=<min>` lower bounds only; avoid upper bounds unless a known
  incompatibility exists. Exception: `cryptography` is pinned exactly
  (`==46.0.7`) — minimum version that fixes CVE-2026-26007/34073/39892.
  Do not downgrade without a security review.

---

## Testing

- Test files live in `tests/` and mirror package structure
  (`tests/core/ring/test_ring.py` mirrors `tourillon/core/ring/ring.py`).
- Every test file must carry the Apache 2.0 header.
- Test functions: `async def test_<what>_<condition>_<expected>() -> None`.
- **Never** use `unittest.TestCase`; use plain `pytest` functions.
- Use `pytest.raises`, `pytest.mark`, `pytest.fixture`. Use `pytest-asyncio`
  with `asyncio_mode = "auto"` (already configured in `pyproject.toml`).
- Do not hardcode ports or addresses; pass them as fixtures.
- Distributed-behaviour tests use **in-memory adapters**; real sockets are
  confined to `tests/e2e/`.
- Mark tests with proposal-aligned pytest marks so they can be run selectively:

  ```python
  @pytest.mark.bootstrap    # proposal 001
  @pytest.mark.ring         # proposal 002
  @pytest.mark.lifecycle    # proposal 003
  @pytest.mark.leave        # proposal 004
  @pytest.mark.pause        # proposal 005
  @pytest.mark.kv           # proposal 006
  ```

- Coverage threshold: **90 %** (`--cov-fail-under=90`).
- Each proposal's "Test scenarios" table is the authoritative source for what
  must be tested.

---

## Documentation rules

- Keep inline comments minimal. Prefer self-documenting names and docstrings.
- Public Protocols, classes, and methods must document: purpose, invariants,
  expected call sequence, ordering guarantees, and caller constraints.
- Write narrative sentences; avoid pure bullet lists in docstrings.
- Do not contradict the proposals in any code comment or docstring.

---

## What Copilot must never do

- `asyncio.get_event_loop()` — use `asyncio.get_running_loop()` or
  `asyncio.run()`.
- `time.sleep()` — use `await asyncio.sleep()`.
- `print()` or `Console.print()` anywhere in `tourillon/` — the daemon emits zero terminal output outside of `logging`. Use `logger.info/debug/warning/error`. `tourctl/` may use `Console.print()` in Typer commands only.
- Exceed cyclomatic complexity **8** — refactor into helpers instead.
- Hardcode IP addresses, ports, or certificate paths.
- Bypass type annotations with `Any` unless truly unavoidable; if required, add
  `# noqa: ANN401` with a comment explaining why.
- Import `msgpack` in `core/` — go through `SerializerPort`.
- Import `ssl`, `asyncio.start_server`, socket, or filesystem APIs in `core/`.
- Suggest `pip`, `virtualenv`, or `conda` commands.
- Generate code that violates any architecture invariant listed above.
