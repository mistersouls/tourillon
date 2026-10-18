---
applyTo: "tourctl/**/*.py,tourillon/bootstrap/usecases/**/*.py,tourillon/bootstrap/main.py"
---

# CLI conventions (`tourillon` and `tourctl`)

Both CLIs use **cyclopts**. Do not introduce click, typer or argparse.

## Structure

```
<pkg>/bootstrap/main.py        App() + app.command(...) registration
<pkg>/bootstrap/usecases/*.py  thin command functions (option parsing only)
<pkg>/core/*.py                the actual work: client calls, rendering, error mapping
```

- Command functions must stay thin: validate/normalise options, call into `core/`, return a string.
- Business logic, rendering and error translation belong in `core/` so they are unit-testable without the CLI.

## Command functions

```python
@some_app.command(name="thing", alias="do-thing")
async def do_thing(
    required_arg: str,
    optional: Path = Path("default.toml"),
    *,
    opts: GlobalOptions | None = None,
) -> str:
    """One-line summary.

    :param required_arg: What it identifies.
    :param optional: What it points at.
    """
    ...
    return f"Did the thing:\nresult={value}"
```

- **Return** the output string; cyclopts prints it. Do not `print` from a command.
- Document every parameter in the docstring — cyclopts surfaces it as help text.
- Commands may be `async`; cyclopts runs them.
- `tourctl` commands take the shared `GlobalOptions` bundle (`tourconf`, `context`) rather than redeclaring those flags.
- Defaults must be explicit and safe. Destructive behaviour requires an opt-in flag (`overwrite: bool = False`).

## Output

- Human output: short labelled `key=value` lines or a compact report. Keep it greppable.
- JSON output: always `json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)`.
- Offer `--json` wherever a human-readable report exists, and keep the two renderings driven by the same data structure.
- Never print secrets, private keys or raw certificate material.

## Errors

- User-facing failures exit via `SystemExit(message)` with an actionable message — never a traceback.
- Map exceptions through `tourctl/core/error.py` and its per-command `ErrorProfile`, so config, TLS/connectivity and protocol failures each get a distinct, useful hint.
- Do not catch bare `Exception` and emit a generic message: that hides real bugs. Catch the known error families; let genuinely unexpected errors surface.
- Keep exit codes consistent: success returns a string, failure raises `SystemExit`.

## Client usage

```python
async with client(service="kv", endpoint="client", tourconf=..., context=...) as kv:
    version = await kv.get(namespace=..., key=...)
```

Use the `client(...)` context manager from `tourclient.client`; it validates the service/endpoint pair, loads config, builds the TLS context and closes the transport. Never build a `Transport` by hand in CLI code.

## Tests

Every new command needs:

- a unit test in `tests/tourctl/` (or `tests/tourillon/`) covering success, rejection and connectivity-failure rendering;
- a pytest-bdd scenario in `tests/e2e/` when the command is part of an operator journey.
