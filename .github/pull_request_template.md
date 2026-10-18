<!--
Keep this short. The goal is to make the change reviewable, not to fill a form.
See AGENTS.md for the working agreement.
-->

## What and why

<!-- What changes, and what problem it solves. Link the docs/BACKLOG.md item if there is one. -->

## How

<!-- The approach, and any design decision a reviewer should challenge. -->

## Checklist

- [ ] `uv run pytest` passes, including the 90% coverage gate
- [ ] New runtime behaviour has both a success test and a failure test
- [ ] `uv run black <changed files>` and `uv run ruff check <changed files>` are clean (changed files only — no repo-wide reformat)
- [ ] Apache 2.0 header present on every new `.py` file
- [ ] Layering respected: `core/` depends on ports only; adapters in `infra/`; wiring in `bootstrap/deps.py`
- [ ] Envelope changes updated in all four places: handler, `tourlib/models/`, client service, service registry
- [ ] No other database product is named anywhere in the change
- [ ] No secrets, certificates or key material committed
- [ ] `docs/BACKLOG.md` updated if this fixes or introduces a known gap

## Distributed-systems impact

<!-- Delete if not applicable. -->

- [ ] Data layout unchanged (hash function, `pshift`/`gshift`, token derivation, key encodings)
- [ ] Wire protocol unchanged (header struct, protocol version, schema id), or compatibility is handled and tested
- [ ] Conflict ordering remains a total order, identical on every node
- [ ] New or changed operations are idempotent on retry
- [ ] Every new network call has a timeout; every new background task is cancelled and awaited on shutdown
