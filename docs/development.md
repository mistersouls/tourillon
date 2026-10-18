# Development

This guide covers repository setup, contribution checks, and preparing
Tourillon releases. End-user instructions are in the
[installation guide](installation.md) and [project README](../README.md).
Bundle construction and release packaging are covered by the
[bundle build guide](../scripts/bundle/README.md).

## Repository setup

The project requires Python 3.14 or newer and `uv`.

```bash
uv sync --extra dev
```

Run the test suite with its configured coverage threshold:

```bash
uv run pytest
```

Before submitting changes, run the checks on the files you changed. The
repository has some pre-existing repository-wide lint and formatting debt
(see [Backlog item D1](backlog.md)).

```bash
uv run pre-commit run --files path/to/changed_file.py
uv run ruff check path/to/changed_file.py
uv run black --check path/to/changed_file.py
```

## Recording the README animation

From the repository root, generate the GIF using the real Docker Compose
example:

```sh
uv run scripts/record_example.py
```

The script declares its own rendering dependencies and writes
`docs/assets/tourillon.gif`. It builds both product images, starts an isolated
single-node stack without publishing host ports, records real `put` and `get`
output inside a single container shell opened with `exec`, and removes its
containers and volume afterward. Startup output is shown progressively, and
commands are animated character by character. Existing example
containers and volumes are not modified. The build refreshes the local product
image tags selected by the example.

On Windows it uses the Ubuntu WSL distribution by default. For Compose v1 in
WSL, run:

```sh
uv run scripts/record_example.py --wsl Ubuntu --compose v1
```

Use `--font PATH` to select a monospace TrueType font and `--output PATH` to
write the GIF elsewhere. The animation shortens waiting times, not command
results.

## Preparing a release

Merge release documentation and code changes to `main`, update local `main`,
and run the release helper from that checkout. It publishes the selected
products' current manifest versions, then prepares the next-version branch:

```bash
python scripts/release.py --dry-run
python scripts/release.py
```

By default, both products are released and bumped. Select only one product
when needed:

```bash
python scripts/release.py --products tourillon
python scripts/release.py --products tourctl
```

The helper requires a clean, up-to-date local `main`. For each selected
product, it fetches the existing release tag from `origin`, or creates a
missing annotated tag at the current `main` commit. It runs pre-commit and
pytest in a detached worktree at each unique tagged commit. Only after the
tagged tests pass does it atomically push missing tags; pushing tags starts the
release workflow.

It then creates a version-specific branch, bumps only the selected product
manifest(s), updates the root version to the maximum of both product versions,
regenerates `uv.lock`, runs the checks, commits the bump, and pushes the branch
to `origin`. It does not push a commit directly to `main`; merge the pushed
branch through the repository's normal review process. If the release workflow
fails after the tag push, the tags remain immutable. Rerun/fix the workflow for
infrastructure problems, or make a corrected release with a new version for a
code defect. Review the already-pushed version-bump branch before merging.

Use `--bump x`, `--bump y`, or `--bump z` to increment major, minor, or patch
and start a `.dev1` series; `--bump tag` (the default) increments the current
pre-release number. Add `--stage dev`, `--stage a`, `--stage b`, `--stage rc`,
or `--stage stable` to choose a stage. For example,
`--bump tag --stage stable` promotes `0.1.0rc2` to `0.1.0`.

There are four project manifests, but `tourlib/pyproject.toml` deliberately
stays at `0.0.0`: it is an internal virtual workspace dependency, not a
separately released product. The root metadata project (`tourproject`) tracks
the greatest of the `tourillon` and `tourctl` versions. `--yes` skips the
confirmation; `--dry-run` previews the release without running tests or
changing files.
