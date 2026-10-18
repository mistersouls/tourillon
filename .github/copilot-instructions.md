# Copilot instructions — Tourillon

**Read [`AGENTS.md`](../AGENTS.md) first.** It is the authoritative context for this repository: architecture, layering, wire protocol, testing and the working agreement.
This file repeats only the rules that must never be violated, plus Copilot-specific guidance.

Path-scoped rules live in [`.github/instructions/`](instructions/). Reusable task runbooks live in [`.github/prompts/`](prompts/).

## Project in one paragraph

Tourillon is a leaderless, peer-to-peer distributed key-value database in Python 3.14+. Nodes are identical peers arranged on a consistent-hash ring of virtual nodes; keys map to partitions, partitions to preference lists, and requests are coordinated by whichever node receives them. Replication uses per-request quorums, hybrid logical clocks for conflict resolution, hinted handoff for unavailable replicas, gossip for membership, and streaming transfers for rebalancing. Every socket is mTLS-authenticated.

## Hard rules

- Never read `.draft/` or `.ai-chat.md`.
- Never name other database products (commercial or open-source) anywhere in the repo — code, comments, docstrings, tests, docs, commit messages, PR text. Describe mechanisms from first principles using neutral vocabulary. Tourillon is its own design, not a reimplementation of anything.
- Never add `__all__` outside `__init__.py`.
- Every `.py` file starts with the Apache 2.0 header block (copy it from any existing file).
- Keep every function at cyclomatic complexity `<= 8`.
- E2E tests (`tests/e2e/`) must drive the real executables — never monkeypatch, stub or fake the app under test.
- Never commit secrets or certificate material; never hand-edit `uv.lock`.

## Commands

```bash
uv sync --extra dev   # setup
uv run pytest         # full suite + 90% coverage gate
uv run black <files>  # format only what you touched
uv run ruff check <files>
```

**Do not run `black .` or `ruff check --fix .`.** The repository has pre-existing, repo-wide formatting debt; a global run creates a huge unrelated diff. Lint and format only the files you changed, and ignore pre-existing violations elsewhere.

## Layering (the most common source of mistakes)

- `tourillon/core/` — domain logic, depends only on protocols in `tourillon/core/ports/` and `tourlib/port/`. No `lmdb`, no `opentelemetry`, no concrete adapters.
- `tourillon/infra/`, `tourlib/adapter/` — concrete implementations.
- `tourillon/bootstrap/deps.py` — the only place that chooses implementations.

## Wire protocol essentials

One frame type, `tourlib.envelope.Envelope`: 32-byte header + UTF-8 `kind` + serialized payload. `kind` is lowercase `domain.action`; responses are `<kind>.accept` / `<kind>.reject`.

- Always build responses with `replay_envelope(kind=..., request=..., payload=...)`.
- Always encode payloads through the injected `Serializer`.
- Handlers are async generators registered with `@dispatcher.on("domain.action")` on either `peer_dispatcher()` or `client_dispatcher()`; a kind may be registered only once.
- Adding or changing a kind means touching all four of: server handler, `tourlib/models/`, `tourclient/core/services/`, and `tourclient/core/services/registry.py`.

## Definition of done

1. Targeted tests pass, then the full `uv run pytest` passes with the coverage gate.
2. Both a success test and a failure test exist for new runtime behaviour.
3. `black` and `ruff` are clean on the files you changed.
4. The change is surgical — no drive-by reformatting or unrelated refactors.
