---
mode: agent
description: Diagnose and fix a runtime bug with a regression test that fails first.
---

# Fix a bug

Fix: `${input:bug:describe the bug, or reference a docs/backlog.md item}`

Read `AGENTS.md` and the `.github/instructions/` file matching the area you are touching.

## Method

1. **Reproduce first.** Write a failing test that captures the incorrect behaviour before changing any production code. Put it next to the existing tests for that module. If you cannot make it fail, you have not understood the bug yet — keep investigating.
2. **Find the root cause.** Trace the real call path: coordinator → placement → peer RPC → storage, or handler → service → port. State the root cause explicitly before fixing it. Do not patch the symptom at the call site if the defect is deeper.
3. **Fix minimally.** Change the smallest amount of code that addresses the cause. No drive-by refactors, no reformatting of untouched lines.
4. **Prove it.** The regression test now passes; add a companion test for the adjacent failure path (timeout, rejection, unavailable replica, cancellation) if one is missing.
5. **Check for siblings.** The same pattern often repeats across services — search for it and note any further occurrences in `docs/backlog.md` rather than silently expanding scope.

## Distributed-systems review checklist

When the fix touches the replication, membership or transfer paths, confirm:

- Conflict comparison remains a **total order** and is identical on every node.
- Operations are **idempotent** on retry; a replayed write or chunk cannot duplicate or lose data.
- Membership updates are **monotonic** — applying a stale update must not regress a member's phase.
- Every network call has a timeout; every background task is cancelled and awaited on shutdown.
- Failures are logged with context (node id, correlation id) and never silently swallowed.
- Tombstones and hinted writes keep their visibility semantics.

## Done when

The regression test fails before the fix and passes after; `uv run pytest` is green with the coverage gate; `black`/`ruff` are clean on changed files; and `docs/backlog.md` is updated if the item was listed there.
