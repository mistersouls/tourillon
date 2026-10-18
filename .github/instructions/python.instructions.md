---
applyTo: "**/*.py"
---

# Python conventions

Target runtime is **Python 3.14+**. Modern syntax is expected and already used throughout.

## File skeleton

1. Apache 2.0 header block (13 comment lines) — always first.
2. Module docstring, if any.
3. Imports: stdlib, third-party, first-party (`tourillon`, `tourctl`, `tourlib`, `tourclient`), separated by blank lines.
4. Code.

Never add `__all__` outside `__init__.py`.

## Style

- Line length 88 (black). Format only the files you touched; never the whole repo.
- Full type annotations on every public function, method and dataclass field.
- Use modern typing: `X | None`, `list[str]`, `dict[str, Any]`, `collections.abc` for `Callable`/`Iterator`/`AsyncIterator`/`Mapping`. Do not import `Optional`, `Union`, `List`, `Dict`.
- Use `Protocol` for ports, `@dataclass(frozen=True)` for value objects and wire models, `enum.Enum` for closed sets.
- Keep cyclomatic complexity `<= 8` per function (`C901`). Extract a private helper rather than adding another branch.
- Private module/class members are `_`-prefixed; public surface stays intentionally small.

## Docstrings

Follow the style already present in the file you are editing. The codebase uses both Sphinx-style (`:param x:`) and Google-style (`Args:` / `Returns:` / `Raises:`) — match the neighbours, do not convert existing ones.

Document the *contract*: what it guarantees, what it raises, and any concurrency or ordering requirement. Skip docstrings that only restate the signature.

## Async

- Everything on the request path is `async`. Never call blocking I/O from a coroutine; use the existing thread-pool helper in `tourlib/contrib/threadpool.py` for blocking backends.
- Never leave a coroutine unawaited. Background tasks must be stored, cancelled on shutdown, and awaited with `contextlib.suppress(asyncio.CancelledError)`.
- Use `async with` for sessions, batches, transports and servers — they all implement the protocol.

## Errors and logging

- Raise the domain exceptions defined in `tourillon/core/exceptions.py` and `tourlib/exceptions.py`. Do not raise bare `Exception`.
- Never swallow exceptions silently. If a loop must survive a failure, log it with `logger.exception(...)` and record telemetry.
- Use `logger` from `tourillon.core.log` with `%s` lazy formatting — never f-strings in log calls, never `print` in runtime code.
- Include the node id or correlation id in log messages on the request path; it is what makes distributed debugging possible.

## Comments

Comment only what is non-obvious: protocol invariants, ordering guarantees, why a branch exists. Do not narrate code.
Mark deliberate follow-ups as `# todo(<handle>): <what and why>` — matching the existing convention — and add a corresponding entry to `docs/BACKLOG.md`.
