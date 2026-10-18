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

import subprocess
import tomllib

import pytest

from scripts.release import (
    bump_products,
    bump_version,
    check_release_changes,
    load_versions,
    local_tag_exists,
    prepare_release_tags,
    remote_tag_commit,
    write_versions,
)


def git(root, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def make_release_repo(tmp_path):
    origin = tmp_path / "origin.git"
    repo = tmp_path / "repo"
    repo.mkdir()
    git(tmp_path, "init", "--bare", str(origin))
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.name", "Release Test")
    git(repo, "config", "user.email", "release@example.invalid")
    (repo / "sentinel.txt").write_text("release commit\n", encoding="utf-8")
    git(repo, "add", "sentinel.txt")
    git(repo, "commit", "-m", "release commit")
    git(repo, "remote", "add", "origin", str(origin))
    git(repo, "push", "--set-upstream", "origin", "main")
    return repo


@pytest.mark.parametrize(
    ("component", "expected"),
    [
        ("x", "1.0.0.dev1"),
        ("y", "0.2.0.dev1"),
        ("z", "0.1.1.dev1"),
        ("tag", "0.1.0.dev2"),
    ],
)
def test_bump_version(component: str, expected: str) -> None:
    assert bump_version("0.1.0.dev1", component) == expected


def test_bump_tag_starts_development_sequence_from_stable_version() -> None:
    assert bump_version("2.4.6", "tag") == "2.4.7.dev1"


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("0.1.0a1", "0.1.0a2"),
        ("0.1.0b3", "0.1.0b4"),
        ("0.1.0rc2", "0.1.0rc3"),
        ("0.1.0.dev4", "0.1.0.dev5"),
    ],
)
def test_bump_tag_increments_existing_prerelease_stage(
    version: str, expected: str
) -> None:
    assert bump_version(version, "tag") == expected


@pytest.mark.parametrize(
    ("version", "stage", "expected"),
    [
        ("0.1.0.dev4", "a", "0.1.0a1"),
        ("0.1.0a2", "b", "0.1.0b1"),
        ("0.1.0b3", "rc", "0.1.0rc1"),
        ("0.1.0rc1", "dev", "0.1.0.dev1"),
        ("0.1.0.dev4", "rc", "0.1.1rc1"),
    ],
)
def test_stage_can_be_selected_for_tag_or_patch_bump(
    version: str, stage: str, expected: str
) -> None:
    component = "z" if expected.startswith("0.1.1") else "tag"
    assert bump_version(version, component, stage) == expected


@pytest.mark.parametrize(
    ("version", "component", "expected"),
    [
        ("0.1.0rc2", "tag", "0.1.0"),
        ("0.1.0.dev3", "tag", "0.1.0"),
        ("0.1.0.dev3", "z", "0.1.1"),
    ],
)
def test_stable_stage_has_no_prerelease_suffix(
    version: str, component: str, expected: str
) -> None:
    assert bump_version(version, component, "stable") == expected


def test_bump_version_rejects_unsupported_versions() -> None:
    with pytest.raises(ValueError, match="Unsupported version format"):
        bump_version("v0.1.0", "tag")


def test_bump_version_rejects_unknown_stage_and_component() -> None:
    with pytest.raises(ValueError, match="Unsupported version stage"):
        bump_version("0.1.0.dev1", "tag", "preview")
    with pytest.raises(ValueError, match="Unsupported version component"):
        bump_version("0.1.0.dev1", "minor")
    with pytest.raises(ValueError, match="already stable"):
        bump_version("0.1.0", "tag", "stable")


def test_version_bump_tracks_max_product_and_leaves_tourlib_unpublished(
    tmp_path,
) -> None:
    manifests = {
        "pyproject.toml": ("tourproject", "0.1.0.dev1"),
        "tourillon/pyproject.toml": ("tourillon", "0.1.0.dev1"),
        "tourctl/pyproject.toml": ("tourctl", "0.0.9"),
        "tourlib/pyproject.toml": ("tourlib", "0.0.0"),
    }
    for relative_path, (name, version) in manifests.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            f'[project]\nname = "{name}"\nversion = "{version}"\n',
            encoding="utf-8",
        )

    assert load_versions(tmp_path) == {
        "root": "0.1.0.dev1",
        "tourillon": "0.1.0.dev1",
        "tourctl": "0.0.9",
    }
    versions = bump_products(load_versions(tmp_path), ("tourctl",), "tag", None)
    assert versions == {
        "root": "0.1.0.dev1",
        "tourillon": "0.1.0.dev1",
        "tourctl": "0.0.10.dev1",
    }
    write_versions(tmp_path, versions)

    with (tmp_path / "pyproject.toml").open("rb") as manifest:
        assert tomllib.load(manifest)["project"]["version"] == "0.1.0.dev1"
    with (tmp_path / "tourillon/pyproject.toml").open("rb") as manifest:
        assert tomllib.load(manifest)["project"]["version"] == "0.1.0.dev1"
    with (tmp_path / "tourctl/pyproject.toml").open("rb") as manifest:
        assert tomllib.load(manifest)["project"]["version"] == "0.0.10.dev1"
    with (tmp_path / "tourlib/pyproject.toml").open("rb") as manifest:
        assert tomllib.load(manifest)["project"]["version"] == "0.0.0"


def test_root_version_is_updated_to_highest_bumped_product() -> None:
    versions = {"tourillon": "0.1.0.dev1", "tourctl": "0.1.0.dev1"}
    updated = bump_products(versions, ("tourillon",), "tag", None)
    assert updated == {
        "root": "0.1.0.dev2",
        "tourillon": "0.1.0.dev2",
        "tourctl": "0.1.0.dev1",
    }


def test_root_version_uses_pep440_order_across_products(tmp_path) -> None:
    manifests = {
        "pyproject.toml": ("tourproject", "1.2.0rc1"),
        "tourillon/pyproject.toml": ("tourillon", "1.2.0.dev8"),
        "tourctl/pyproject.toml": ("tourctl", "1.2.0rc1"),
    }
    for relative_path, (name, version) in manifests.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            f'[project]\nname = "{name}"\nversion = "{version}"\n',
            encoding="utf-8",
        )

    assert load_versions(tmp_path)["root"] == "1.2.0rc1"
    with (tmp_path / "pyproject.toml").open("w", encoding="utf-8") as manifest:
        manifest.write('[project]\nname = "tourproject"\nversion = "1.2.0.dev8"\n')
    with pytest.raises(ValueError, match="must be the maximum"):
        load_versions(tmp_path)


def test_existing_remote_release_tag_is_fetched_and_reused(tmp_path) -> None:
    repo = make_release_repo(tmp_path)
    tag = "tourillon-v0.1.0.dev1"
    git(repo, "tag", "-a", tag, "-m", "release")
    git(repo, "push", "origin", tag)
    expected_commit = git(repo, "rev-parse", f"{tag}^{{}}")
    git(repo, "tag", "-d", tag)

    pushed = prepare_release_tags(
        repo, {"tourillon": "0.1.0.dev1"}, ("tourillon",), dry_run=False
    )

    assert pushed == []
    assert local_tag_exists(repo, tag)
    assert remote_tag_commit(repo, tag) == expected_commit
    assert git(repo, "rev-parse", f"{tag}^{{}}") == expected_commit


def test_release_change_check_preserves_first_path_character(tmp_path) -> None:
    repo = make_release_repo(tmp_path)
    manifest = repo / "pyproject.toml"
    manifest.write_text('[project]\nversion = "0.1.0.dev2"\n', encoding="utf-8")
    git(repo, "add", "pyproject.toml")
    git(repo, "commit", "-m", "add root manifest")
    manifest.write_text('[project]\nversion = "0.1.0.dev3"\n', encoding="utf-8")

    check_release_changes(repo, {"pyproject.toml"})
    with pytest.raises(ValueError, match="pyproject.toml"):
        check_release_changes(repo, set())


def test_missing_release_tag_is_created_and_second_run_reuses_it(tmp_path) -> None:
    repo = make_release_repo(tmp_path)
    tag = "tourctl-v0.1.0.dev1"
    versions = {"tourctl": "0.1.0.dev1"}

    pushed = prepare_release_tags(repo, versions, ("tourctl",), dry_run=False)
    assert pushed == [tag]
    assert local_tag_exists(repo, tag)
    git(repo, "push", "--atomic", "origin", *pushed)

    assert prepare_release_tags(repo, versions, ("tourctl",), dry_run=False) == []
