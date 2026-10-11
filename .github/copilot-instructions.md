# Copilot Instructions

## NEVER READ

- NEVER READ `.draft/` file. Read it is PROHIBITED.
- NEVER READ `.ai-chat.md` file. Read it is PROHIBITED.
- NEVER add `__all__` declarations outside `__init__.py` files.
- End-to-end tests must exercise the real runtime; do not monkeypatch, stub, or fake the application under test.

## Quality gates

- Target cyclomatic complexity: `<= 8` per function/method.
- Coverage target: `>= 90%`.
- New behavior requires tests for nominal and failure paths.

## Prohibited pattern

- Do not use ge