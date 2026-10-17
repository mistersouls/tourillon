# Copilot Instructions

## Repository-specific constraints

- Never read files under `.draft/` or `.ai-chat.md`.
- NEVER add `__all__` declarations outside `__init__.py` files.
- End-to-end tests must exercise the real runtime; do not monkeypatch, stub, or fake the application under test.

## Build, test, and lint

- Requires Python 3.14 or newer. Install the project and development tools with `uv sync --extra dev`.
- Build the distributions with `uv build`.
- Run the full test suite with coverage enforcement using `uv run pytest`. Pytest defaults to `tests/`, writes `coverage.xml`, and requires at least 90% coverage for `tourillon`, `tourctl`, and `tourlib`.
- Run one test file with `uv run pytest tests/tourillon/test_store.py`, or one test with `uv run pytest tests/tourillon/test_store.py::test_put_replicates_to_handoff_and_readable_targets`.
- Run Ruff with `uv run ruff check .`; check formatting with `uv run black --check .`.
- Run all configured pre-commit hooks with `uv run pre-commit run --all-files`. The hooks also apply formatting, Ruff fixes, unused-import cleanup, file hygiene, and the Apache license-header check.

## Architecture

- `tourillon` is the node runtime. `tourillon.core` contains node behavior and abstractions; `tourillon.infra` provides concrete storage and telemetry adapters; `tourillon.bootstrap` assembles the application and exposes the `tourillon` CLI.
- `Application` is constructed with ports/adapters, and `Manager` composes node services such as storage, partitioning, topology, peer pooling, lifecycle, gossip, and transfers. `NodeLifecycle` owns startup/shutdown and the separate peer and client envelope servers.
- `tourclient` is the async client library. Its service registry maps service names to factories and declares which client/peer endpoint each service supports. `tourctl` is the operator CLI built on those client services.
- `tourlib` holds shared wire models and envelope framing, serialization, persistence, and mTLS abstractions/adapters used across the runtime, client, and CLI.
- Tests are grouped by package under `tests/`; `tests/e2e/` exercises command-line entry points and real node processes.

## Codebase conventions

- Keep node/domain behavior behind protocols in `tourillon.core.ports`; put backend implementations in `tourillon.infra`. Default implementations are wired in `tourillon.bootstrap.deps`, rather than being selected inside domain services.
- Network calls use `tourlib.envelope.Envelope`: `kind` values are lowercase dotted names (`domain.action`), payloads are encoded through the shared `Serializer`, and `Dispatcher` handlers are registered by kind. Keep service endpoint declarations in `tourclient.core.services.registry` aligned with server handler registration.
- Storage operations are scoped to a partition group and use async sessions; use a session's batch API when multiple operations belong together. Keyspace and key/value schema types live in `tourillon.core.models.storageschema`.
- Configuration and node state are persisted through the `Persistence` port (TOML by default). Keep parsing/validation in the configuration or model types rather than duplicating it in CLI handlers.
- Keep individual functions at cyclomatic complexity `<= 8`. Add tests for both success and failure behavior when changing runtime behavior.
