# Proposal: [Short title]

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-ring-05312026-002.md -->

**Author**: [Team or name] <[email]>
**Status:** Draft | Accepted | Implemented
**Date:** YYYY-MM-DD
**Sequence:** NNN

---

## Summary

One concise paragraph describing the capability, bounded scope, and intended
operator impact.

---

## Motivation

Explain what is missing today, what risk or friction exists without this
change, and why now.

---

## CLI contract

Define all user-visible command/flag/output/error behavior first.

- Cover new or modified commands for `tourillon` and/or `tourctl`.
- Provide happy-path and failure-path examples.
- Specify stdout vs stderr and exit codes.

```bash
$ tourillon <command> [OPTIONS]
$ tourctl <command> [OPTIONS]
```

---

## Design

### Data model

Describe new or changed domain records (dataclasses/enums/value objects), wire
messages, and config/state schema fragments.

### Core invariants

List non-negotiable rules that must always hold.

### Sequence / flow

Document the main orchestration path(s) step-by-step.

### Error paths

Map concrete failure scenarios to operator-visible behavior.

---

## Design decisions

### Decision: [title]

**Alternatives considered:**

- [option A]
- [option B]

**Chosen because:** [short rationale]

---

## Proposed code organisation

List files created/modified, with concise intent per file.

```text
tourillon/...   NEW/MODIFIED — [reason]
tourlib/...     NEW/MODIFIED — [reason]
tourctl/...     NEW/MODIFIED — [reason]
tests/...       NEW/MODIFIED — [reason]
```

---

## Interfaces (informative)

Show minimal illustrative signatures for protocols/services/models.

```python
# illustrative only
class ExamplePort(Protocol):
    async def call(self, ...) -> ...: ...
```

---

## Test scenarios

State assumptions (in-memory vs `[e2e]`) and enumerate observable scenarios.

| # | Mark | Fixture | Action | Expected |
|---|------|---------|--------|----------|
| 1 | unit | ... | ... | ... |

---

## Exit criteria

- [ ] CLI contract examples are implemented as specified.
- [ ] Listed test scenarios pass.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run ruff check .` passes.
- [ ] `uv run black --check .` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Explicitly list what this proposal does not change.
