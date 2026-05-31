# Proposal: [Short title]

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-bootstrap-05022026-001.md     -->

**Author**: Firstname Lastname <Email>
**Status:** Draft | Accepted | Implemented
**Date:** YYYY-MM-DD
**Sequence:** NNN

---

## Summary

One paragraph. What problem does this solve, and why does it matter?

---

## Motivation

Why is this being proposed? What breaks or is missing without it?

---

## CLI contract

Start here. Describe every user-visible command this proposal introduces or
modifies. Use realistic examples including flags, output, and error cases.
The implementation follows from the CLI contract — never the other way around.

```
$ tourillon <command> [flags]
$ tourctl <command> [flags]
```

---

## Design

### Data model

Key types, interfaces, or dataclasses.

### Core invariants

Rules that must never be violated.

### Sequence / flow

Step-by-step description of the happy path.

### Error paths

What can go wrong and what the user sees.

---

## Design decisions

### Decision: [title]

**Alternatives considered:** ...
**Chosen because:** ...

---

## Interfaces (informative)

Relevant Python Protocols, dataclasses, or enum snippets.
Illustrative, not prescriptive.

```python
```

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | ... | ... | ... |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

What this proposal explicitly does not cover.
