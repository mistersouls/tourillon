# Copilot instructions for Tourillon

## Default principles

- Prefer extending the existing design over replacing it.
- Preserve the current public contracts unless the user explicitly asks for a breaking change.
- If a change would be radical, ask before doing it.
- Use the proposal files in `proposals/` as the source of truth for feature work.

## Architecture

- Follow hexagonal / DDD boundaries.
- Keep domain rules in the core, not in CLI or infrastructure code.
- Use ports for dependencies and adapters for concrete implementations.
- Keep bootstrap code thin: wiring only.
- Do not let transport, persistence, or CLI details leak into domain objects.

## Python style

- Target Python 3.14.
- Write typed, explicit, straightforward code.
- Prefer `pathlib`, `dataclasses`, `Enum`, `Protocol`, and small pure helpers where they fit.
- Keep functions short and focused.
- Avoid broad `try` / `except` blocks and silent fallbacks.
- Prefer existing repository style over personal preference when they differ.
- `ssl` is part of the Python standard library and is allowed in domain/core code when it improves type safety or clarity.

## Formatting and linting

- Use Black formatting.
- Follow Ruff rules as the repository's flake8 / isort / pyupgrade equivalent.
- Keep cyclomatic complexity at or below 8.
- Keep Radon maintainability in good shape.
- Respect PEP 8 / PEP 20 / PEP 257 where they fit the codebase.

## Working rules

- Read nearby code before editing.
- Reuse existing helpers and patterns first.
- Add or update tests for observable behavior changes.
- Do not introduce new dependencies unless strictly necessary and justified by the proposal.

## Validation

- Format with `black`.
- Lint with `ruff check`.
- Check complexity with `radon cc`.
- Check maintainability with `radon mi`.
- Run the relevant `pytest` scope for the changed area.
