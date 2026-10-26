---
applyTo: ".github/workflows/**,scripts/**,**/pyproject.toml,.pre-commit-config.yaml"
---

# Build, packaging and CI

## Dependencies

- `uv` is the only supported workflow. Add dependencies to the owning package's `pyproject.toml`, then run `uv sync --extra dev` to refresh `uv.lock`.
- **Never hand-edit `uv.lock`.** It is committed deliberately so builds and bundles are reproducible.
- Keep the runtime dependency surface minimal — the node ships as a self-contained native bundle, and every dependency has to be vendored and licensed.
- Each workspace member builds its own wheel and is installed editably during development. `tourillon` depends on `tourlib`; `tourctl` depends on `tourclient` and `tourlib`; `tourclient` depends on `tourlib`. Do not force-include package sources or create dependencies from `tourlib` onto its consumers.

## Workflows

- `ci.yml` runs on pushes and pull requests: tests with the coverage gate, plus lint/format **restricted to changed files** (the repository has pre-existing formatting debt, so a global check would always fail).
- `release-dist.yml` builds native bundles for six platform targets, verifies the installers, runs the real-process E2E journey against the installed executable, and publishes archives on tags.
- `copilot-setup-steps.yml` pre-installs the toolchain for the Copilot coding agent. Keep its setup steps in sync with `ci.yml`.
- Pin actions to a major version, keep `uv sync --locked` in CI so a stale lockfile fails loudly, and always upload E2E artifacts with `if: always()`.

## Bundling (`scripts/bundle/`)

`build.py` produces a self-contained distribution: it copies a CPython runtime, compiles the native launcher, installs locked dependencies, copies the application packages, compiles bytecode and **strips all `.py`/`.pyi` sources**, bundles application bytecode into the runtime zip, writes licences and metadata, adds the platform installer, audits the result, and smoke-tests `--help`.

If you touch packaging:

- Anything imported at runtime must survive source-stripping — no reliance on `__file__`, source introspection, or reading adjacent `.py` files.
- New runtime data files must be explicitly included by the builder and asserted by the bundle audit.
- Products differ: `tourillon` ships `tourillon` + `tourlib`; `tourctl` ships `tourctl` + `tourclient` + `tourlib`.
- Keep `install.sh` and `install.ps1` behaviourally in step, including `--update` / `-Update`.
- Update `scripts/bundle/README.md` when the archive layout or installer contract changes.

## Pre-commit

Hooks run with `language: system` against the uv-managed `.venv`, so `uv sync --extra dev` must come first. They apply black, `ruff check --fix`, autoflake, file hygiene, YAML/TOML syntax checks and the Apache licence-header check.

`scripts/check_license_header.py` requires `# Copyright` and `# Licensed under the Apache License, Version 2.0` within the first 20 lines of every Python file. New scripts, tests and generated files all need the full header block.
