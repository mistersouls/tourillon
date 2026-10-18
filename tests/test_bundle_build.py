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

import dataclasses
import json
import marshal
import os
import re
import shutil
import subprocess
import tomllib
import zipfile
from typing import get_type_hints

import pytest

import scripts.bundle.build as bundle
from scripts.bundle.build import (
    _copy_native_files,
    _copy_unix_runtime,
    build_stdlib_archive,
    bundle_application_packages,
    copy_application_packages,
    normalized_architecture,
    strip_sources,
    validate_application_packages,
)


def test_build_stdlib_archive_contains_only_optimized_bytecode(tmp_path):
    stdlib = tmp_path / "Lib"
    package = stdlib / "example"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("value = 42\n", encoding="utf-8")
    (package / "types.pyi").write_text("value: int\n", encoding="utf-8")
    tests = stdlib / "test"
    tests.mkdir()
    (tests / "not_shipped.py").write_text("raise RuntimeError\n", encoding="utf-8")

    archive_path = tmp_path / "python314.zip"
    build_stdlib_archive(stdlib, archive_path)

    with zipfile.ZipFile(archive_path) as archive:
        assert archive.namelist() == ["example/__init__.pyc"]
        code = marshal.loads(archive.read("example/__init__.pyc")[16:])
        assert code.co_consts == (42, None)


def test_bundled_dataclasses_preserve_resolvable_constructor_annotations(tmp_path):
    stdlib = tmp_path / "Lib"
    stdlib.mkdir()
    shutil.copyfile(dataclasses.__file__, stdlib / "dataclasses.py")
    archive_path = tmp_path / "python314.zip"

    build_stdlib_archive(stdlib, archive_path)

    with zipfile.ZipFile(archive_path) as archive:
        code = marshal.loads(archive.read("dataclasses.pyc")[16:])
    namespace = {"__name__": "dataclasses"}
    exec(code, namespace)

    @namespace["dataclass"]
    class Options:
        tourconf: str | None = None

    assert get_type_hints(Options.__init__) == {
        "tourconf": str | None,
        "return": type(None),
    }
    assert Options(tourconf="config.toml").tourconf == "config.toml"


def test_copy_native_files_drops_gui_and_test_extensions(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for name in ("_socket.pyd", "_testcapi.pyd", "_tkinter.pyd", "tcl86t.dll"):
        (source / name).write_bytes(b"native")
    destination = tmp_path / "runtime"

    _copy_native_files(source, destination, {".dll", ".pyd"})

    assert [path.name for path in destination.iterdir()] == ["_socket.pyd"]


def test_windows_bundle_installer_has_no_command_shim(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle.sys, "platform", "win32")

    bundle.write_platform_installer(tmp_path)

    assert (tmp_path / "install.ps1").is_file()
    assert not (tmp_path / "bin").exists()


def test_bundle_support_files_live_beside_dist(tmp_path, monkeypatch):
    monkeypatch.setattr(bundle.sys, "platform", "win32")
    dist = tmp_path / "dist"
    dist.mkdir()
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "LICENSE").write_text("Project license\n", encoding="utf-8")
    (dist / "PYTHON_LICENSE.txt").write_text("Python license\n", encoding="utf-8")

    bundle.write_licenses(tmp_path, dist, project_root)
    bundle.write_bundle_metadata(
        dist, tmp_path, "1.2.3", dist / "tourillon.exe", "tourillon"
    )
    bundle.write_platform_installer(tmp_path)
    bundle.validate_bundle_metadata(dist, tmp_path, windows=True, product="tourillon")

    assert (tmp_path / "README.txt").is_file()
    assert (tmp_path / "THIRD_PARTY_LICENSES").is_file()
    assert (tmp_path / "install.ps1").is_file()
    assert (dist / "PRODUCT_RUNTIME_INFO.json").is_file()
    assert not (dist / "README.txt").exists()
    assert not (dist / "THIRD_PARTY_LICENSES").exists()
    assert not (dist / "install.ps1").exists()
    assert (
        json.loads((dist / "PRODUCT_RUNTIME_INFO.json").read_text(encoding="utf-8"))[
            "launcher"
        ]
        == "tourillon.exe"
    )
    assert (
        json.loads((dist / "PRODUCT_RUNTIME_INFO.json").read_text(encoding="utf-8"))[
            "version"
        ]
        == "1.2.3"
    )


def test_application_packages_are_bundled_in_runtime_zip(tmp_path):
    project_root = tmp_path / "project"
    dist = tmp_path / "dist"
    tourillon = project_root / "tourillon"
    tourlib = project_root / "tourlib"
    tourillon.mkdir(parents=True)
    tourlib.mkdir(parents=True)
    (tourillon / "__init__.py").write_text("value = 42\n", encoding="utf-8")
    (tourillon / "worker.py").write_text("answer = 84\n", encoding="utf-8")
    (tourillon / "resource.txt").write_text("bundled\n", encoding="utf-8")
    (tourlib / "__init__.py").write_text("name = 'tourlib'\n", encoding="utf-8")
    dist.mkdir()
    executable = dist / "tourillon"
    executable.write_bytes(b"native executable")
    stdlib = tmp_path / "stdlib"
    stdlib.mkdir()
    (stdlib / "support.py").write_text("stdlib_value = 7\n", encoding="utf-8")
    archive_path = dist / "python314.zip"
    build_stdlib_archive(stdlib, archive_path)

    copy_application_packages(project_root, dist, "tourillon")
    validate_application_packages(dist, "tourillon")
    strip_sources(dist)
    bundle_application_packages(dist, "tourillon")

    assert executable.is_file()
    assert not (dist / "app").exists()
    with zipfile.ZipFile(archive_path) as archive:
        entries = set(archive.namelist())
        assert "support.pyc" in entries
        assert "tourillon/__init__.pyc" in entries
        assert "tourillon/worker.pyc" in entries
        assert "tourillon/resource.txt" in entries
        assert "tourlib/__init__.pyc" in entries

    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(archive_path)
    result = subprocess.run(
        [
            bundle.sys.executable,
            "-c",
            "import tourillon, tourillon.worker, tourlib; "
            "print(tourillon.value, tourillon.worker.answer, tourlib.name)",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "42 84 tourlib"


def test_tourctl_packages_are_bundled_without_tourillon(tmp_path):
    project_root = tmp_path / "project"
    dist = tmp_path / "dist"
    packages = bundle.PRODUCT_PACKAGES["tourctl"]
    for name in packages:
        package = project_root / name
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(f"name = {name!r}\n", encoding="utf-8")
    (project_root / "tourctl" / "pyproject.toml").write_text(
        "[project]\nname = 'tourctl'\n", encoding="utf-8"
    )
    dist.mkdir()
    stdlib = tmp_path / "stdlib"
    stdlib.mkdir()
    archive_path = dist / "python314.zip"
    build_stdlib_archive(stdlib, archive_path)

    copy_application_packages(project_root, dist, "tourctl")
    validate_application_packages(dist, "tourctl")
    strip_sources(dist)
    bundle_application_packages(dist, "tourctl")

    with zipfile.ZipFile(archive_path) as archive:
        entries = set(archive.namelist())
        assert all(f"{name}/__init__.pyc" in entries for name in packages)
        assert "tourillon/__init__.pyc" not in entries
        assert "tourctl/pyproject.toml" not in entries
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(archive_path)
    result = subprocess.run(
        [
            bundle.sys.executable,
            "-c",
            "import importlib; print(','.join(importlib.import_module(name).name "
            "for name in ('tourctl', 'tourclient', 'tourlib')))",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "tourctl,tourclient,tourlib"


def test_products_have_independent_package_sets():
    assert bundle.PRODUCT_PACKAGES["tourillon"] == ("tourillon", "tourlib")
    assert bundle.PRODUCT_PACKAGES["tourctl"] == ("tourctl", "tourclient", "tourlib")


def test_workspace_products_inherit_shared_runtime_dependencies():
    project_root = bundle.Path.cwd()

    def dependencies(path):
        with (project_root / path).open("rb") as project_file:
            return tomllib.load(project_file)["project"].get("dependencies", [])

    tourlib = set(dependencies("tourlib/pyproject.toml"))
    assert tourlib == {
        "cryptography==46.0.3",
        "cyclopts>=4.22.3",
        "msgpack>=1.1",
        "tomli-w>=1.2",
    }
    assert "tourlib" in dependencies("tourillon/pyproject.toml")
    assert dependencies("tourctl/pyproject.toml") == ["tourlib"]
    assert dependencies("pyproject.toml") == []

    with (project_root / "tourillon" / "pyproject.toml").open("rb") as project_file:
        tourillon_project = tomllib.load(project_file)
    with (project_root / "tourctl" / "pyproject.toml").open("rb") as project_file:
        tourctl_project = tomllib.load(project_file)
    with (project_root / "pyproject.toml").open("rb") as project_file:
        root_project = tomllib.load(project_file)
    assert root_project["project"]["name"] == "tourproject"
    assert tourillon_project["project"]["name"] == "tourillon"
    assert tourillon_project["tool"]["uv"]["package"] is True
    assert tourctl_project["tool"]["uv"]["package"] is True
    assert "tourillon" in tourillon_project["project"]["scripts"]
    assert "tourctl" in tourctl_project["project"]["scripts"]


@pytest.mark.parametrize(
    ("product", "tag", "version", "expected"),
    [
        ("tourillon", "tourillon-v1.2.3", None, "1.2.3"),
        ("tourillon", "tourillon-v0.0.1.dev0", None, "0.0.1.dev0"),
        ("tourctl", "tourctl-v0.1.0.dev1", None, "0.1.0.dev1"),
        ("tourctl", "tourctl-v2.0.0-rc.1", None, "2.0.0-rc.1"),
        ("tourctl", None, "0.1.0.dev12+local", "0.1.0.dev12+local"),
        ("tourctl", None, "3.4.5", "3.4.5"),
        ("tourctl", None, None, "0.0.0+local"),
    ],
)
def test_bundle_version_comes_from_tag_or_build_input(product, tag, version, expected):
    assert bundle.bundle_version(product, tag, version) == expected


@pytest.mark.parametrize(
    ("product", "tag", "version", "message"),
    [
        ("tourctl", "tourillon-v1.2.3", None, "selects tourillon"),
        ("tourillon", "tourillon-v1.2.3", "2.0.0", "does not match"),
        ("tourctl", "tourctl-release", None, "must use"),
        ("tourctl", None, "not-a-version", "not a supported release version"),
    ],
)
def test_bundle_version_rejects_invalid_sources(product, tag, version, message):
    with pytest.raises(RuntimeError, match=message):
        bundle.bundle_version(product, tag, version)


@pytest.mark.parametrize(
    ("version", "valid"),
    [
        ("0.0.1.dev0", True),
        ("0.1.0.dev12", True),
        ("0.1.0.dev1+local", True),
        ("1.2.3", True),
        ("2.0.0-rc.1", True),
        ("0.0.0+local", True),
        ("0.0.1.dev", False),
        ("0.0.1.dev-1", False),
        ("0.0.1.dev0/unsafe", False),
        ("0.0.1.DEV0", False),
        ("../0.0.1.dev0", False),
        ("not-a-version", False),
    ],
)
def test_bundle_and_installers_agree_on_supported_versions(version, valid):
    root = bundle.Path.cwd() / "scripts" / "bundle"
    powershell = (root / "install.ps1").read_text(encoding="utf-8")
    shell = (root / "install.sh").read_text(encoding="utf-8")
    powershell_pattern = re.search(r"-cnotmatch '([^']+)'", powershell).group(1)
    shell_pattern = re.search(r"grep -Eq '([^']+)'", shell).group(1)

    assert bool(re.fullmatch(powershell_pattern, version)) == valid
    assert bool(re.fullmatch(shell_pattern, version)) == valid
    if valid:
        assert bundle.bundle_version("tourillon", None, version) == version
    else:
        with pytest.raises(RuntimeError, match="not a supported release version"):
            bundle.bundle_version("tourillon", None, version)


@pytest.mark.parametrize("system", ["linux", "darwin"])
def test_unix_bundle_contains_executable_install_script(tmp_path, monkeypatch, system):
    monkeypatch.setattr(bundle.sys, "platform", system)
    chmod_modes = []
    original_chmod = bundle.Path.chmod

    def record_chmod(path, mode, *args, **kwargs):
        chmod_modes.append(mode)
        return original_chmod(path, mode, *args, **kwargs)

    monkeypatch.setattr(bundle.Path, "chmod", record_chmod)

    bundle.write_platform_installer(tmp_path)

    installer = tmp_path / "install.sh"
    assert installer.is_file()
    assert len(chmod_modes) == 1
    assert chmod_modes[0] & 0o111 == 0o111
    if bundle.os.name != "nt":
        assert installer.stat().st_mode & 0o111


def test_archive_audit_requires_executable_posix_installer(tmp_path):
    archive_path = tmp_path / "bundle.zip"
    installer = zipfile.ZipInfo("tourillon/install.sh")
    installer.external_attr = 0o100755 << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(installer, "#!/bin/sh\n")
        archive.writestr("tourillon/README.txt", "instructions\n")
        archive.writestr("tourillon/THIRD_PARTY_LICENSES", "licenses\n")

    bundle._validate_output_archive(archive_path)


def test_archive_audit_rejects_non_executable_posix_installer(tmp_path):
    archive_path = tmp_path / "bundle.zip"
    installer = zipfile.ZipInfo("tourillon/install.sh")
    installer.external_attr = 0o100644 << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(installer, "#!/bin/sh\n")
        archive.writestr("tourillon/README.txt", "instructions\n")
        archive.writestr("tourillon/THIRD_PARTY_LICENSES", "licenses\n")

    with pytest.raises(RuntimeError, match="not executable"):
        bundle._validate_output_archive(archive_path)


def test_archive_audit_rejects_bundle_documents_inside_dist(tmp_path):
    archive_path = tmp_path / "bundle.zip"
    installer = zipfile.ZipInfo("tourillon/install.sh")
    installer.external_attr = 0o100755 << 16
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(installer, "#!/bin/sh\n")
        archive.writestr("tourillon/dist/README.txt", "instructions\n")
        archive.writestr("tourillon/dist/THIRD_PARTY_LICENSES", "licenses\n")

    with pytest.raises(RuntimeError, match="missing bundle-root"):
        bundle._validate_output_archive(archive_path)


def test_copy_unix_runtime_preserves_library_and_extension_layout(
    tmp_path, monkeypatch
):
    home = tmp_path / "python"
    library_dir = home / "lib"
    extension_dir = library_dir / "python3.14" / "lib-dynload"
    extension_dir.mkdir(parents=True)
    (library_dir / "libpython3.14.so.1.0").write_bytes(b"python")
    (library_dir / "libssl.so.3").write_bytes(b"ssl")
    (extension_dir / "_socket.cpython-314-x86_64-linux-gnu.so").write_bytes(b"socket")
    destination = tmp_path / "dist"
    monkeypatch.setattr(bundle.sys, "platform", "linux")
    monkeypatch.setattr(
        bundle.sysconfig,
        "get_config_var",
        lambda name: {
            "LIBDIR": str(library_dir),
            "LDLIBRARY": "libpython3.14.so.1.0",
            "DESTSHARED": str(extension_dir),
        }.get(name),
    )
    monkeypatch.setattr(
        bundle.sysconfig,
        "get_path",
        lambda name: str(library_dir / "python3.14") if name == "stdlib" else "",
    )

    _copy_unix_runtime(home, destination)

    assert (destination / "lib" / "libpython3.14.so.1.0").is_file()
    assert (destination / "lib" / "libssl.so.3").is_file()
    assert (
        destination
        / "lib"
        / "python3.14"
        / "lib-dynload"
        / "_socket.cpython-314-x86_64-linux-gnu.so"
    ).is_file()


def test_architecture_normalizes_windows_and_linux_aliases(monkeypatch):
    monkeypatch.setattr("scripts.bundle.build.platform.machine", lambda: "AMD64")

    assert normalized_architecture() == "x86_64"

    monkeypatch.setattr("scripts.bundle.build.platform.machine", lambda: "aarch64")

    assert normalized_architecture() == "arm64"


@pytest.mark.parametrize(
    ("libc", "version", "message"),
    [
        ("", "", "glibc runtime is required"),
        ("musl", "1.2.5", "musl-based"),
        ("glibc", "2.34", "too old"),
    ],
)
def test_linux_bundle_rejects_unsupported_libc(monkeypatch, libc, version, message):
    monkeypatch.setattr(bundle.platform, "libc_ver", lambda: (libc, version))

    with pytest.raises(RuntimeError, match=message):
        bundle.validate_linux_glibc()


def test_linux_bundle_warns_when_glibc_is_newer_than_portability_baseline(
    monkeypatch, capsys
):
    monkeypatch.setattr(bundle.platform, "libc_ver", lambda: ("glibc", "2.38"))

    assert bundle.validate_linux_glibc() == (2, 38)
    assert "Build on Ubuntu 22.04" in capsys.readouterr().out


def test_build_step_error_includes_recommendation():
    def fail():
        raise OSError("disk full")

    with pytest.raises(RuntimeError, match="Recommendation: free disk space"):
        bundle.run_step("Creating archive", "free disk space", fail, lambda _: None)


def test_failed_publication_restores_previous_bundle_and_archive(tmp_path):
    staged_bundle = tmp_path / "staging" / "tourillon"
    staged_bundle.mkdir(parents=True)
    (staged_bundle / "version.txt").write_text("new", encoding="utf-8")
    final_bundle = tmp_path / "tourillon"
    final_bundle.mkdir()
    (final_bundle / "version.txt").write_text("old", encoding="utf-8")
    final_archive = tmp_path / "tourillon-test.zip"
    final_archive.write_text("previous archive", encoding="utf-8")
    missing_archive = tmp_path / "staging" / "missing.zip"

    with pytest.raises(FileNotFoundError):
        bundle._publish_bundle(
            staged_bundle, final_bundle, missing_archive, final_archive
        )

    assert (final_bundle / "version.txt").read_text(encoding="utf-8") == "old"
    assert final_archive.read_text(encoding="utf-8") == "previous archive"
