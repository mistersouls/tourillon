---
mode: agent
description: Add a new envelope kind end-to-end (model, handler, client service, registry, tests).
---

# Add an envelope kind end-to-end

Add the operation `${input:kind:envelope kind, e.g. kv.scan}` to Tourillon.

Read `AGENTS.md` and `.github/instructions/wire-protocol.instructions.md` before writing code.

## Steps

1. **Decide the surface.** Is this user-facing (`client_dispatcher`) or internal node-to-node (`peer_dispatcher`)? Internal kinds get no client service.
2. **Model** — add frozen request/response dataclasses in `tourlib/models/`, validating in `__post_init__` against the limits in `tourlib/constant.py`.
3. **Domain logic** — implement the behaviour in the appropriate `tourillon/core/services/` service. The handler must stay a thin adapter. Keep each function at complexity `<= 8`.
4. **Handler** — add an async-generator handler in `tourillon/bootstrap/handlers/` registered with `@dispatcher.on("<kind>")`. Decode with the injected serializer, delegate, and `yield replay_envelope(...)` for both `<kind>.accept` and `<kind>.reject`.
5. **Client service** (skip for internal kinds) — add the method in `tourclient/core/services/`, validate the response kind, map `.reject` to `RequestRejectedError` and anything unexpected to `ResponseError`.
6. **Registry** — add or update the entry in `tourclient/core/services/registry.py` with an accurate `supported_endpoints` frozenset.
7. **CLI** (only if an operator needs it) — thin command in `tourctl/bootstrap/usecases/`, rendering in `tourctl/core/`, with `--json` support.
8. **Tests** — at minimum: handler accept path, handler reject path, model validation failure, client decode success, client error mapping, and an envelope round-trip.

## Constraints

- Do not alter the envelope header struct, `PROTOCOL_VERSION` or the schema id.
- Every `.py` file gets the Apache 2.0 header.
- No product names from other databases anywhere.

## Done when

`uv run pytest` passes with the coverage gate, and `uv run black` + `uv run ruff check` are clean **on the changed files only**.
