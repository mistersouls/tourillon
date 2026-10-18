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
"""Validate, tag, and publish the Tourillon product bundles."""

import argparse
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

PRODUCTS = ("tourillon", "tourctl")
VERSION_PATTERN = re.compile(
    r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"
    r"(?:\.(?P<dev>dev)(?P<dev_number>\d+)|(?P<stage>a|b|rc)(?P<stage_number>\d+))?"
)
PRERELEASE_STAGES = ("dev", "a", "b", "rc")
VERSION_STAGES = (*PRERELEASE_STAGES, "stable")
VERSION_LINE = re.compile(r'(?m)^(version\s*=\s*")[^"]+(")')
VERSION_FILES = (
    "pyproject.toml",
    "tourillon/pyproject.toml",
    "tourctl/pyproject.toml",
)
RELEASE_FILES = (*VERSION_FILES, "uv.lock")


def bump_base(
    major: int, minor: int, patch: int, component: str
) -> tuple[int, int, int]:
    if component == "x":
        return major + 1, 0, 0
    elif component == "y":
        return major, minor + 1, 0
    if component == "z":
        return major, minor, patch + 1
    raise ValueError(f"Unsupported version component: {component!r}")


def bump_version(version: str, component: str, stage: str | None = None) -> str:
    """Return a bumped pre-release or stable version using PEP 440 stages."""
    match = VERSION_PATTERN.fullmatch(version)
    if match is None:
        raise ValueError(f"Unsupported version format: {version!r}")
    if stage is not None and stage not in VERSION_STAGES:
        raise ValueError(f"Unsupported version stage: {stage!r}")

    major, minor, patch = (int(match[name]) for name in ("major", "minor", "patch"))
    if component != "tag":
        major, minor, patch = bump_base(major, minor, patch, component)
        stage = stage or "dev"
        number = 1
    elif stage == "stable":
        if match["dev"] is None and match["stage"] is None:
            raise ValueError(f"Version {version!r} is already stable.")
        return f"{major}.{minor}.{patch}"
    else:
        current_stage = match["dev"] or match["stage"]
        if current_stage is None:
            major, minor, patch = bump_base(major, minor, patch, "z")
        stage = stage or current_stage or "dev"
        current_number = int(match["dev_number"] or match["stage_number"] or 0)
        number = current_number + 1 if stage == current_stage else 1
    if stage == "stable":
        return f"{major}.{minor}.{patch}"
    suffix = f".dev{number}" if stage == "dev" else f"{stage}{number}"
    return f"{major}.{minor}.{patch}{suffix}"


def version_key(version: str) -> tuple[int, int, int, int, int]:
    """Return a sort key matching the supported PEP 440 version ordering."""
    match = VERSION_PATTERN.fullmatch(version)
    if match is None:
        raise ValueError(f"Unsupported version format: {version!r}")
    stage = match["dev"] or match["stage"]
    rank = {"dev": 0, "a": 1, "b": 2, "rc": 3, None: 4}[stage]
    number = int(match["dev_number"] or match["stage_number"] or 0)
    return (
        int(match["major"]),
        int(match["minor"]),
        int(match["patch"]),
        rank,
        number,
    )


def load_versions(root: Path) -> dict[str, str]:
    """Read product versions and ensure root metadata is their maximum."""
    versions = {}
    for product in PRODUCTS:
        path = root / product / "pyproject.toml"
        with path.open("rb") as manifest:
            versions[product] = tomllib.load(manifest)["project"]["version"]
    for version in versions.values():
        if VERSION_PATTERN.fullmatch(version) is None:
            raise ValueError(f"Unsupported version format: {version!r}")
    root_path = root / "pyproject.toml"
    with root_path.open("rb") as manifest:
        root_version = tomllib.load(manifest)["project"]["version"]
    maximum = max(versions.values(), key=version_key)
    if root_version != maximum:
        raise ValueError(
            f"Root project version {root_version} must be the maximum product "
            f"version ({maximum}): {versions}"
        )
    versions["root"] = root_version
    return versions


def bump_products(
    versions: dict[str, str],
    products: tuple[str, ...],
    component: str,
    stage: str | None,
) -> dict[str, str]:
    """Bump only selected products and set root metadata to the new maximum."""
    updated = {product: versions[product] for product in PRODUCTS}
    for product in products:
        updated[product] = bump_version(updated[product], component, stage)
    updated["root"] = max(updated.values(), key=version_key)
    return updated


def write_versions(root: Path, versions: dict[str, str]) -> None:
    """Update root metadata and product versions supplied by the caller."""
    for project, relative_path in (("root", ""), *[(name, name) for name in PRODUCTS]):
        version = versions[project]
        path = root / relative_path / "pyproject.toml"
        text = path.read_text(encoding="utf-8")
        updated, count = VERSION_LINE.subn(rf"\g<1>{version}\g<2>", text, count=1)
        if count != 1:
            raise ValueError(f"Could not find one project version in {path}")
        path.write_text(updated, encoding="utf-8", newline="\n")


def run(root: Path, command: list[str], dry_run: bool = False) -> None:
    """Run a command in the repository, or print it in dry-run mode."""
    print("+ " + subprocess.list2cmdline(command))
    if not dry_run:
        subprocess.run(command, cwd=root, check=True)


def git_output(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def git_status_lines(root: Path) -> list[str]:
    """Read porcelain status without stripping its significant leading spaces."""
    result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.splitlines()


def check_clean_worktree(root: Path) -> None:
    status = git_status_lines(root)
    if status:
        raise ValueError(
            "Tracked files have uncommitted changes. Commit the release "
            "documentation and code before running this script."
        )


def require_current_main(root: Path) -> None:
    """Require a clean checkout of main exactly matching origin/main."""
    branch = git_output(root, "branch", "--show-current")
    if branch != "main":
        raise ValueError(
            f"Release from main only; currently on {branch or 'detached HEAD'}."
        )
    if git_output(root, "rev-parse", "HEAD") != git_output(
        root, "rev-parse", "origin/main"
    ):
        raise ValueError("Local main is not up to date with origin/main; pull first.")


def local_tag_exists(root: Path, tag: str) -> bool:
    result = subprocess.run(
        ["git", "show-ref", "--verify", "--quiet", f"refs/tags/{tag}"],
        cwd=root,
    )
    if result.returncode not in {0, 1}:
        raise subprocess.CalledProcessError(result.returncode, result.args)
    return result.returncode == 0


def check_release_changes(root: Path, expected: set[str]) -> None:
    status = git_status_lines(root)
    changed = {line[3:] for line in status}
    unexpected = changed - expected
    if unexpected:
        paths = ", ".join(sorted(unexpected))
        raise ValueError(
            "Validation changed files outside the release manifests: "
            f"{paths}. Review and commit those changes before releasing."
        )


def confirm(action: str, version: str, tags: list[str], assume_yes: bool) -> bool:
    print(f"Action: {action}")
    print(f"Version: {version}")
    if tags:
        print("Tags: " + ", ".join(tags))
    if assume_yes:
        return True
    try:
        answer = input("Continue? [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in {"y", "yes"}


def remote_tag_commit(root: Path, tag: str) -> str | None:
    result = subprocess.run(
        [
            "git",
            "ls-remote",
            "--tags",
            "origin",
            f"refs/tags/{tag}",
            f"refs/tags/{tag}^{{}}",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    refs = [line.split("\t", 1) for line in result.stdout.splitlines()]
    peeled = next((sha for sha, ref in refs if ref.endswith("^{}")), None)
    return peeled or (refs[0][0] if refs else None)


def prepare_release_tags(
    root: Path, versions: dict[str, str], products: tuple[str, ...], dry_run: bool
) -> list[str]:
    """Fetch existing product tags or create missing ones at current main."""
    push_tags = []
    head = git_output(root, "rev-parse", "HEAD")
    for product in products:
        tag = f"{product}-v{versions[product]}"
        remote_commit = remote_tag_commit(root, tag)
        if remote_commit is not None:
            if not local_tag_exists(root, tag):
                run(
                    root,
                    ["git", "fetch", "origin", f"refs/tags/{tag}:refs/tags/{tag}"],
                    dry_run,
                )
            local_commit = (
                git_output(root, "rev-parse", f"{tag}^{{}}")
                if not dry_run
                else remote_commit
            )
            if local_commit != remote_commit:
                raise ValueError(
                    f"Local tag {tag} differs from origin; refusing to move it."
                )
            print(f"Using existing tag {tag} ({remote_commit[:12]}).")
            continue

        if local_tag_exists(root, tag):
            local_commit = git_output(root, "rev-parse", f"{tag}^{{}}")
            if local_commit != head:
                raise ValueError(
                    f"Local tag {tag} exists on another commit, but not on origin."
                )
        else:
            run(
                root,
                ["git", "tag", "-a", tag, "-m", f"{product} {versions[product]}"],
                dry_run,
            )
        push_tags.append(tag)

    return push_tags


def run_tag_checks(root: Path, tags: list[str], dry_run: bool) -> None:
    """Run checks in a detached worktree for each unique tagged commit."""
    commits = set()
    for tag in tags:
        commit = git_output(root, "rev-parse", f"{tag}^{{}}") if not dry_run else tag
        if commit in commits:
            continue
        commits.add(commit)
        if dry_run:
            print(f"Would run validation in a detached worktree at {tag} ({commit}).")
            continue
        with tempfile.TemporaryDirectory(prefix="tourillon-release-") as temporary:
            worktree = Path(temporary) / "source"
            run(root, ["git", "worktree", "add", "--detach", str(worktree), tag])
            try:
                run_checks(worktree, dry_run=False)
            finally:
                run(root, ["git", "worktree", "remove", "--force", str(worktree)])


def create_bump_branch(
    root: Path,
    versions: dict[str, str],
    products: tuple[str, ...],
    dry_run: bool,
) -> str:
    branch_versions = "-".join(f"{product}-{versions[product]}" for product in products)
    branch = f"chore/version-bump-{branch_versions}"
    remote_branch = subprocess.run(
        ["git", "ls-remote", "--heads", "origin", f"refs/heads/{branch}"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    if remote_branch.stdout.strip():
        print(f"Version-bump branch {branch} already exists on origin; skipping it.")
        return ""
    local_branch = subprocess.run(
        ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=root,
    )
    if local_branch.returncode == 0:
        raise ValueError(
            f"Local branch {branch} already exists; inspect it before retrying."
        )
    if local_branch.returncode != 1:
        raise subprocess.CalledProcessError(local_branch.returncode, local_branch.args)
    run(root, ["git", "switch", "-c", branch], dry_run)
    return branch


def run_checks(root: Path, dry_run: bool) -> None:
    run(root, ["uv", "sync", "--extra", "dev"], dry_run)
    run(root, ["uv", "run", "pre-commit", "run", "--all-files"], dry_run)
    run(root, ["uv", "run", "pytest"], dry_run)


def commit_bump(
    root: Path,
    versions: dict[str, str],
    products: tuple[str, ...],
    dry_run: bool,
) -> None:
    changed = [path for path in RELEASE_FILES if (root / path).exists()]
    if not dry_run:
        changed = [
            path
            for path in changed
            if path in git_output(root, "diff", "--name-only").splitlines()
        ]
    if changed:
        run(root, ["git", "add", "--", *changed], dry_run)
    run(
        root,
        [
            "git",
            "commit",
            "-m",
            "chore: bump "
            + ", ".join(f"{product} to {versions[product]}" for product in products),
        ],
        dry_run,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bump",
        choices=("x", "y", "z", "tag"),
        default="tag",
        help="bump selected product(s) after publishing tags (default: tag)",
    )
    parser.add_argument(
        "--products",
        choices=("tourillon", "tourctl", "both"),
        default="both",
        help="product version(s) to bump after release (default: both)",
    )
    parser.add_argument(
        "--stage",
        choices=VERSION_STAGES,
        help="version stage for the bump (default: retain the existing stage)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show the planned release without changing files or running commands",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="skip the interactive confirmation",
    )
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    """Reject incompatible version-stage arguments."""
    if args.stage == "stable" and args.bump != "tag":
        raise ValueError("--stage stable can only be used with --bump tag.")


def selected_products(selection: str) -> tuple[str, ...]:
    return PRODUCTS if selection == "both" else (selection,)


def publish_and_bump(
    root: Path,
    versions: dict[str, str],
    products: tuple[str, ...],
    component: str,
    stage: str | None,
    dry_run: bool,
    assume_yes: bool,
) -> None:
    release_tags = [f"{product}-v{versions[product]}" for product in products]
    next_versions = bump_products(versions, products, component, stage)
    print("Products: " + ", ".join(products))
    print("Release tags: " + ", ".join(release_tags))
    print(
        "Next versions: "
        + ", ".join(f"{product}={next_versions[product]}" for product in products)
        + f", tourproject={next_versions['root']}"
    )
    if not confirm(
        "test tags, publish missing tags, and push version-bump branch",
        next_versions["root"],
        release_tags,
        assume_yes,
    ):
        raise ValueError("Release cancelled.")

    require_current_main(root)
    tags_to_push = prepare_release_tags(root, versions, products, dry_run)
    run_tag_checks(root, release_tags, dry_run)
    if tags_to_push:
        run(root, ["git", "push", "--atomic", "origin", *tags_to_push], dry_run)
    else:
        print("All selected release tags already exist on origin.")

    branch = create_bump_branch(root, next_versions, products, dry_run)
    if not branch:
        return
    if not dry_run:
        write_versions(root, next_versions)
    run(root, ["uv", "sync", "--extra", "dev"], dry_run)
    run(root, ["uv", "run", "pre-commit", "run", "--all-files"], dry_run)
    run(root, ["uv", "run", "pytest"], dry_run)
    if not dry_run:
        check_release_changes(root, set(RELEASE_FILES))
    commit_bump(root, next_versions, products, dry_run)
    run(root, ["git", "push", "--set-upstream", "origin", branch], dry_run)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    try:
        validate_args(args)
        check_clean_worktree(root)
        versions = load_versions(root)
        publish_and_bump(
            root,
            versions,
            selected_products(args.products),
            args.bump,
            args.stage,
            args.dry_run,
            args.yes or args.dry_run,
        )
        return 0
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Release failed: {exc}", file=sys.stderr)
        return getattr(exc, "returncode", 1) or 1


if __name__ == "__main__":
    raise SystemExit(main())
