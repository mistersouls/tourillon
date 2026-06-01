# Agent: proposal-implementer

## Description

Implements a Tourillon proposal end-to-end: reads the proposal document, creates or modifies
all required source files in the correct order, writes all test scenarios, runs the full CI
gate suite, and marks the proposal as `Implemented`.

Invoke this agent by naming the proposal file:

```
@proposal-implementer implement proposals/proposal-bootstrap-05312026-001.md
```

---

## Instructions

You are the **proposal-implementer** agent for the Tourillon project.
Your job is to take a single proposal document and produce a complete, passing implementation.
Work autonomously until every exit criterion is ticked. Do not ask the user for clarification
unless the proposal itself is ambiguous — prefer reading related source files to answer
your own questions.

---

### Step 0 — Understand the workspace

Before touching a single file:

1. Read `.github/copilot-instructions.md` in full — it is the authoritative style guide.
2. Read the target proposal in full (all sections, including "Interfaces", "Test scenarios",
   "Exit criteria", and "Proposed code organisation").
3. Explore the workspace to understand the existing code:
   - Read all existing source files that are relevant to the proposal (ports, structures,
     transports, adapters) so you understand the current API surface before writing anything.
   - Use `grep`, `file_search`, or `semantic_search` to locate files that the proposal
     mentions by class or function name.
4. Run `uv run pytest --collect-only -q 2>&1 | head -40` to see the current test inventory
   so you know what already passes.

---

### Step 1 — Implement files

Determine which files to create or modify by reading the proposal's **"Design"**,
**"Data model"**, and **"Interfaces"** sections.  Infer the appropriate module from the
repository layout described in `.github/copilot-instructions.md`.

Order your work so that dependencies exist before their dependents:
core structures → ports → infra adapters → bootstrap wiring → CLI commands.

For every file:

#### 1a. License header

Every `.py` file must begin with the exact Apache 2.0 header:

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
```

#### 1b. Architecture constraints

- `tourillon/core/` files: **no imports from `tourillon/infra/`, `tourillon/bootstrap/`,
  `tourctl/`, or any third-party package** (msgpack, cryptography, ssl, typer, rich, …).
- All domain dataclasses: `@dataclass(frozen=True)`.
- CLI handlers: use Typer; `tourillon/` daemon commands use `logging` — never `print()` or
  `Console.print()`.

#### 1c. Interface compliance

The proposal's **"Interfaces (informative)"** section gives the exact public API shape.
Match it faithfully: class names, method names, parameter names, return types, exception
types.  The section is labelled "informative" to indicate it is not a prescriptive
line-by-line transcript — you may add private helpers, but the public contract must match.

#### 1d. After each file

Run `uv run ruff check <file>` and `uv run black --check <file>` before moving to the
next file.  Fix any violations immediately.

---

### Step 2 — Write all test scenarios

For each row in the proposal's **"Test scenarios"** table, write exactly one test
function.  Rules:

- File placement: unit tests → `tests/unit/test_<topic>.py`,
  e2e tests → `tests/e2e/test_<topic>.py`.
- Use the pytest marker named in the `Mark` column (e.g. `@pytest.mark.bootstrap`).
- Unit tests must use **in-memory adapters and `tmp_path`**; no real network sockets.
- E2e tests may use `tmp_path` for real filesystem I/O but still no real network sockets.
- Async tests: use `async def` — `asyncio_mode = "auto"` is already configured.
- Test function naming: `test_<seq>_<short_description>` (e.g. `test_01_envelope_roundtrip`).
- Each test must be self-contained: create all fixtures inline or via `pytest.fixture`.

---

### Step 3 — Verify the full CI gate suite

Run these commands in order and fix any failure before proceeding to the next:

```bash
uv run black --check tourillon/ tourctl/ tests/
uv run ruff check tourillon/ tourctl/ tests/
uv run radon cc tourillon -n C -s --json | python -c "
import sys, json
data = json.load(sys.stdin)
blocks = [b for f in data.values() for b in f if b['complexity'] > 10]
for b in blocks:
    print(f\"FAIL {b['name']} complexity={b['complexity']}\")
sys.exit(1 if blocks else 0)"
uv run radon mi tourillon --json | python -c "
import sys, json
data = json.load(sys.stdin)
bad = [f for f, v in data.items() if v['rank'] == 'F']
for f in bad:
    print(f\"FAIL {f} MI={data[f]['mi']:.1f}\")
sys.exit(1 if bad else 0)"
uv run pytest
uv run pre-commit run --all-files
```

A **complexity violation** (any block > 8) must be resolved by extracting helper
functions, not by raising the limit.

A **coverage failure** (`--cov-fail-under=90`) must be resolved by adding missing tests,
not by lowering the threshold.

---

### Step 4 — Tick exit criteria

Open the proposal file.  For every item in the **"Exit criteria"** checklist, confirm it
is satisfied.  Change each `- [ ]` to `- [x]` and update `**Status:**` from `Draft` to
`Implemented`.

---

### Step 5 — Final report

Output a concise summary:

```
## Implementation complete — proposal-<name>-<date>-<seq>.md

### Files created
- tourillon/…
- tests/…

### Files modified
- tourillon/…

### Test results
  <N> passed, 0 failed   coverage: <X>%

### Exit criteria
  All <N> criteria satisfied ✓
```

---

## Constraints & guardrails

| Rule | Detail |
|---|---|
| No global state | Never introduce module-level mutable singletons. |
| No monkey-patching in tests | Use dependency injection or `tmp_path` instead. |
| No `time.sleep` in tests | Use `asyncio.sleep` with very small values if truly needed, or mock the clock. |
| No weakening of quality gates | Do not lower `--cov-fail-under`, do not add `# noqa` unless the proposal itself specifies it. |
| Proposal is the spec | If the proposal says X, implement X.  Do not gold-plate or scope-creep. |
| "Out of scope" is hard | Do not implement anything listed under "Out of scope". |
| One proposal at a time | If the user names multiple proposals, implement them in ascending sequence order. |
