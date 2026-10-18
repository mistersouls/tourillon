---
mode: agent
description: Raise meaningful test coverage for an under-tested module.
---

# Raise coverage for a module

Target module: `${input:module:e.g. tourillon/core/peer/pool.py}`

Read `.github/instructions/tests.instructions.md` first.

## Method

1. Measure the current state:
   ```bash
   uv run pytest --cov=<package> --cov-report=term-missing tests/<area>
   ```
   Note the specific uncovered line ranges — do not guess.
2. Read the module and list its **behaviours**, not its lines: success path, each error branch, retry/backoff, cancellation, shutdown, boundary conditions.
3. Write tests for the behaviours that are missing, preferring the order: error branches → cancellation/shutdown → boundaries → happy path (usually already covered).
4. Inject fakes at the **port** boundary (`tourillon/core/ports/`, `tourlib/port/`) rather than monkeypatching internals. If a module is untestable without monkeypatching, that is a design finding — report it.
5. Re-measure and confirm the new lines are genuinely exercised.

## Rules

- No assertion-free tests, no tests that only call a function to move the number.
- Every test asserts observable behaviour: a return value, a raised exception, an emitted envelope kind, a stored record, a cancelled task.
- No fixed sleeps; bind to ephemeral ports; clean up tasks and temp dirs.
- Keep the full suite green and order-independent.

## Priority areas

Per `docs/backlog.md`, the weakest areas are the peer pool, storage group, lifecycle, transfer planner, transport session, phi-accrual detector, gossip bootstrapper, and the whole `tourclient` package (which is not in the coverage gate at all).

## Done when

`uv run pytest` passes, the targeted module's coverage improved measurably, and every new test maps to a named behaviour.
