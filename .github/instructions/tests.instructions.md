---
applyTo: "tests/**/*.py"
---

# Testing conventions

## Layout and markers

- `tests/<package>/test_<module>.py` mirrors the source tree.
- Mark each file with its package marker: `@pytest.mark.tourillon`, `@pytest.mark.tourctl`, `@pytest.mark.tourlib`; E2E files add `@pytest.mark.e2e`.
- `asyncio_mode = "auto"` is on, so `async def test_...` runs without extra plumbing.
- Test names describe behaviour, not implementation: `test_put_replicates_to_handoff_and_readable_targets`.

## What good tests look like here

- **Always test both paths.** Every behaviour change needs a success test and a failure test (rejection, timeout, invalid input, unavailable peer).
- Use `tmp_path` for every file, database and certificate. Never write into the repository tree.
- Use the shared fixtures and helpers instead of re-rolling infrastructure:
  - `mtls_contexts` (from `tests/conftest.py`) → real CA + server/client leaf certs,
  - `tests/helpers/` → `create_mtls_contexts`, `generate_ca`, `make_envelope`, `start_envelope_server`.
- Assert on observable behaviour (returned values, emitted envelope kinds, stored records), not on private attributes.
- Prefer deterministic inputs. If you need randomness (ring tokens, key distribution), seed it and assert an invariant rather than a snapshot.

## Fakes

- Faking a **port** is correct and encouraged in unit tests: implement the `Protocol` from `tourillon/core/ports/` or `tourlib/port/` and inject it.
- Monkeypatching internals of the code under test is a smell — restructure via injection instead.
- Never fake `Envelope` framing: build real envelopes with the helpers so the test exercises the real codec.

## Async and concurrency

- Never use bare `asyncio.sleep` to "wait for" a condition. Await the real signal (event, future, queue) or poll a predicate with a bounded deadline.
- Always cancel and await background tasks at the end of a test; leaked tasks make unrelated tests flaky.
- Use `async with` for servers, transports, sessions and batches so teardown happens even on failure.
- Tests must pass when run alone and in the full suite. No shared global state, no ordering dependencies, no fixed ports — bind to port 0 or use the helpers.

## E2E / BDD (`tests/e2e/`)

- **No mocking, monkeypatching, stubbing or faking of the application under test.** E2E spawns the real `tourillon` and `tourctl` executables as subprocesses.
- Scenarios are declared in `tests/e2e/features/*.feature` and bound in `tests/e2e/test_*_e2e_bdd.py` via `scenarios(...)`/`scenario(...)`.
- Process orchestration, timeouts and polling belong in `tests/e2e/journey.py`; reporting in `tests/e2e/reporting.py`. Extend those rather than inlining `subprocess` calls into step definitions.
- Wait with the bounded `Journey.wait_for(...)` helper and a meaningful predicate (node phase is `READY`, value converged) — never a fixed sleep.
- Executables resolve from `PATH` or `--tourillon-bin` / `--tourctl-bin`; missing binaries fail the session loudly on purpose. Do not add skips to work around that.
- Write Gherkin in user language: what the operator does and observes, not which function is called.

## Coverage

- Gate is **90%** across `tourillon`, `tourctl` and `tourlib` (`--cov-fail-under=90`). `tourclient` is not measured yet — still test it.
- Chase meaningful coverage: error branches, retry paths, cancellation and shutdown. Do not add assertion-free tests to move the number.
- Run targeted tests while iterating (`-m "not e2e" --no-cov` for a full-but-fast pass), then the full `uv run pytest` before declaring done.
