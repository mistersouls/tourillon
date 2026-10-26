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

import argparse
import compileall
import importlib._bootstrap_external
import importlib.util
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import tokenize
import zipfile
from collections.abc import Callable
from pathlib import Path

EXCLUDED_STDLIB_DIRS = {
    "__pycache__",
    "ensurepip",
    "idlelib",
    "site-packages",
    "test",
    "tests",
    "tkinter",
    "turtledemo",
    "venv",
}
PRODUCT_PACKAGES = {
    "tourillon": ("tourillon", "tourlib"),
    "tourctl": ("tourctl", "tourclient", "tourlib"),
}


def normalized_platform() -> str:
    if sys.platform == "win32":
        return "windows"
    if sys.platform == "darwin":
        return "macos"
    if sys.platform.startswith("linux"):
        return "linux"
    raise RuntimeError(f"Unsupported operating system: {sys.platform}")


def normalized_architecture() -> str:
    architecture = platform.machine().lower()
    aliases = {
        "amd64": "x86_64",
        "aarch64": "arm64",
        "arm64": "arm64",
        "x64": "x86_64",
        "x86_64": "x86_64",
    }
    try:
        return aliases[architecture]
    except KeyError as error:
        raise RuntimeError(
            f"Unsupported processor architecture: {architecture}"
        ) from error


def bundle_tag() -> str:
    return f"{normalized_platform()}-{normalized_architecture()}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build, validate, and package a self-contained Tourillon product bundle "
            "for the current OS and architecture."
        ),
        epilog=(
            "Requires CPython 3.14, uv, and a native C compiler. "
            "The resulting ZIP is installed offline with install.ps1 "
            "(Windows) or install.sh (Linux/macOS)."
        ),
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--python-home", type=Path, default=Path(sys.base_prefix))
    parser.add_argument(
        "--product", choices=tuple(PRODUCT_PACKAGES), default="tourillon"
    )
    parser.add_argument("--release-tag")
    parser.add_argument("--version")
    parser.add_argument("--strict-no-py", action="store_true")
    parser.add_argument("--zip-output", action="store_true")
    return parser.parse_args()


def command(arguments: list[str], *, cwd: Path | None = None) -> None:
    print("+", subprocess.list2cmdline(arguments))
    subprocess.run(arguments, cwd=cwd, check=True)


def validate_linux_glibc() -> tuple[int, int]:
    libc, version = platform.libc_ver()
    if libc.lower() != "glibc" or not version:
        raise RuntimeError(
            "A detectable glibc runtime is required to build Linux bundles; "
            "musl-based environments such as Alpine are not supported."
        )
    try:
        version_parts = re.match(r"^(\d+)\.(\d+)", version)
        if version_parts is None:
            raise ValueError(version)
        glibc_version = (int(version_parts.group(1)), int(version_parts.group(2)))
    except ValueError as error:
        raise RuntimeError(f"Could not parse glibc version: {version}") from error
    if glibc_version < (2, 35):
        raise RuntimeError(
            f"glibc {version} is too old; build on Ubuntu 22.04 or newer "
            "(glibc 2.35+) to produce supported Linux bundles."
        )
    if glibc_version > (2, 35):
        print(
            f"WARNING: this Linux bundle is built against glibc {version}; "
            "it may not run on Ubuntu 22.04. Build on Ubuntu 22.04 to target "
            "the documented glibc 2.35 baseline."
        )
    return int(glibc_version[0]), int(glibc_version[1])


def _validate_project_inputs(project_root: Path, product: str) -> None:
    if not (project_root / "pyproject.toml").is_file():
        raise FileNotFoundError(
            f"Required project input is missing: {project_root / 'pyproject.toml'}"
        )
    required_paths = [project_root / "uv.lock", project_root / "LICENSE"]
    required_paths.extend(project_root / name for name in PRODUCT_PACKAGES[product])
    required_paths.append(project_root / product / "pyproject.toml")
    for required in required_paths:
        if not required.exists():
            raise FileNotFoundError(f"Required project input is missing: {required}")
    if not (project_root / "scripts" / "bundle" / "launcher.c").is_file():
        raise FileNotFoundError(
            "Native launcher source scripts/bundle/launcher.c is missing."
        )


def _validate_python_install(python_home: Path) -> None:
    if not python_home.is_dir():
        raise FileNotFoundError(f"CPython home does not exist: {python_home}")
    include = Path(str(sysconfig.get_path("include") or ""))
    if not (include / "Python.h").is_file():
        raise FileNotFoundError(
            f"Python.h was not found in {include}; install the CPython development headers."
        )
    if shutil.which("uv") is None:
        raise RuntimeError("uv was not found on PATH; install uv and retry.")


def _validate_windows_toolchain(python_home: Path) -> None:
    if shutil.which("cl.exe") is None and _windows_compiler_environment() is None:
        raise RuntimeError(
            "MSVC C compiler was not found; install Visual Studio Build Tools "
            "with the C++ workload and the target architecture toolchain."
        )
    if not (python_home / "python314.dll").is_file():
        raise FileNotFoundError(f"CPython runtime DLL is missing from {python_home}.")
    if not (python_home / "libs" / "python314.lib").is_file():
        raise FileNotFoundError(
            f"CPython import library is missing from {python_home / 'libs'}."
        )


def _validate_unix_toolchain(python_home: Path) -> None:
    if shutil.which("cc") is None and shutil.which("clang") is None:
        raise RuntimeError("No C compiler was found; install GCC or Clang and retry.")
    if not _python_shared_libraries(python_home):
        raise FileNotFoundError(
            f"CPython shared library was not found under {python_home}; "
            "install a shared-library CPython build."
        )


def _validate_python_build_inputs(python_home: Path) -> None:
    if sys.version_info[:2] != (3, 14):
        raise RuntimeError(
            f"Python 3.14 is required; current interpreter is {platform.python_version()}."
        )
    _validate_python_install(python_home)
    if sys.platform == "win32":
        _validate_windows_toolchain(python_home)
    else:
        _validate_unix_toolchain(python_home)


def validate_build_environment(
    project_root: Path, python_home: Path, product: str
) -> None:
    normalized_platform()
    normalized_architecture()
    _validate_project_inputs(project_root, product)
    _validate_python_build_inputs(python_home)
    if sys.platform.startswith("linux"):
        validate_linux_glibc()


def run_step(
    label: str,
    recommendation: str,
    action: Callable,
    validate: Callable,
):
    print(f"\n==> {label}")
    try:
        result = action()
        validate(result)
    except Exception as error:
        raise RuntimeError(
            f"Step failed: {label}\n"
            f"  Cause: {error}\n"
            f"  Recommendation: {recommendation}"
        ) from error
    print(f"OK: {label}")
    return result


def python_version_tag() -> str:
    return f"python{sys.version_info.major}{sys.version_info.minor}"


def _stdlib_paths(stdlib: Path) -> list[Path]:
    paths: list[Path] = []
    for parent, directories, filenames in os.walk(stdlib):
        directories[:] = sorted(
            directory
            for directory in directories
            if directory.lower() not in EXCLUDED_STDLIB_DIRS
        )
        paths.extend(Path(parent) / filename for filename in filenames)
    return sorted(paths)


def build_stdlib_archive(stdlib: Path, archive_path: Path) -> None:
    print(f"Building optimized CPython standard library: {stdlib}")
    with zipfile.ZipFile(
        archive_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6
    ) as archive:
        for source in _stdlib_paths(stdlib):
            relative = source.relative_to(stdlib)
            if relative.parts[0] == "lib-dynload":
                continue
            if source.suffix == ".py":
                if source.name.endswith(".pyi"):
                    continue
                with tokenize.open(source) as stream:
                    source_text = stream.read()
                code = compile(
                    source_text,
                    f"<stdlib>/{relative.as_posix()}",
                    "exec",
                    dont_inherit=True,
                    optimize=2,
                )
                payload = importlib._bootstrap_external._code_to_hash_pyc(
                    code,
                    importlib.util.source_hash(source.read_bytes()),
                    checked=False,
                )
                archive.writestr(relative.with_suffix(".pyc").as_posix(), payload)
            elif source.suffix not in {".pyc", ".pyo", ".pyi"}:
                archive.write(source, relative.as_posix())


def _native_files(source: Path, suffixes: set[str]) -> list[Path]:
    if not source.is_dir():
        return []
    return sorted(
        file
        for file in source.iterdir()
        if file.is_file()
        and file.suffix.lower() in suffixes
        and not file.stem.lower().startswith("_test")
    )


def _copy_native_files(source: Path, destination: Path, suffixes: set[str]) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for native_file in _native_files(source, suffixes):
        name = native_file.name.lower()
        if name.startswith(("tcl", "tk", "_test", "_tkinter")) or name.endswith(".ico"):
            continue
        shutil.copy2(native_file, destination / native_file.name)


def _copy_windows_runtime(home: Path, destination: Path) -> None:
    for pattern in ("python*.dll", "vcruntime*.dll"):
        for library in home.glob(pattern):
            shutil.copy2(library, destination / library.name)
    _copy_native_files(home / "DLLs", destination / "DLLs", {".dll", ".pyd"})
    stdlib_dlls = Path(sysconfig.get_path("stdlib")) / "DLLs"
    if stdlib_dlls != home / "DLLs":
        _copy_native_files(stdlib_dlls, destination / "DLLs", {".dll", ".pyd"})


def _python_shared_libraries(home: Path) -> list[Path]:
    library_dir = Path(str(sysconfig.get_config_var("LIBDIR") or home / "lib"))
    library_name = str(sysconfig.get_config_var("LDLIBRARY") or "")
    if not library_name:
        return []
    library_names = {library_name}
    libraries = [library_dir / name for name in sorted(library_names)]
    return [library for library in libraries if library.is_file()]


def _relative_runtime_path(home: Path, path: Path) -> Path:
    try:
        return path.relative_to(home)
    except ValueError:
        return Path("lib")


def _copy_unix_shared_libraries(
    home: Path, destination: Path, library_dir: Path, libraries: list[Path]
) -> None:
    relative_library_dir = _relative_runtime_path(home, library_dir)
    runtime_library_dir = destination / relative_library_dir
    runtime_library_dir.mkdir(parents=True, exist_ok=True)
    for library in libraries:
        shutil.copy2(library, runtime_library_dir / library.name)

    source_library_dirs = [home / "lib", home / "lib64"]
    if library_dir not in source_library_dirs:
        source_library_dirs.append(library_dir)
    for source_library_dir in source_library_dirs:
        if not source_library_dir.is_dir():
            continue
        relative_dir = _relative_runtime_path(home, source_library_dir)
        target_dir = destination / relative_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        for library in source_library_dir.iterdir():
            if library.is_file() and (
                ".so" in library.name or library.suffix == ".dylib"
            ):
                shutil.copy2(library, target_dir / library.name)


def _copy_unix_extensions(home: Path, destination: Path) -> None:
    stdlib = Path(sysconfig.get_path("stdlib"))
    suffixes = {".so"} if sys.platform == "linux" else {".so", ".dylib"}
    extension_dir = Path(str(sysconfig.get_config_var("DESTSHARED") or ""))
    if not extension_dir.is_dir():
        extension_dir = stdlib / "lib-dynload"
    try:
        relative_extension_dir = extension_dir.relative_to(home)
    except ValueError:
        relative_extension_dir = Path("lib") / python_version_tag() / "lib-dynload"
    _copy_native_files(
        extension_dir,
        destination / relative_extension_dir,
        suffixes,
    )


def _copy_unix_runtime(home: Path, destination: Path) -> None:
    libraries = _python_shared_libraries(home)
    if not libraries:
        raise FileNotFoundError(
            "CPython shared library is missing; build Python with shared-library support."
        )
    _copy_unix_shared_libraries(home, destination, libraries[0].parent, libraries)
    _copy_unix_extensions(home, destination)


def _copy_runtime(home: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    stdlib = Path(sysconfig.get_path("stdlib"))
    if not stdlib.is_dir():
        raise FileNotFoundError(f"CPython standard library not found: {stdlib}")
    if sys.platform == "win32":
        _copy_windows_runtime(home, destination)
    else:
        _copy_unix_runtime(home, destination)

    python_license = next(
        (
            candidate
            for candidate in (
                home / "LICENSE.txt",
                home / "LICENSE",
                Path(sys.prefix) / "LICENSE.txt",
                Path(sys.prefix) / "LICENSE",
                stdlib / "LICENSE.txt",
            )
            if candidate.is_file()
        ),
        None,
    )
    if python_license is None:
        raise FileNotFoundError(
            "CPython license was not found; install the official CPython runtime "
            "including its license file."
        )
    shutil.copy2(python_license, destination / "PYTHON_LICENSE.txt")

    archive = destination / f"{python_version_tag()}.zip"
    build_stdlib_archive(stdlib, archive)
    return stdlib


def _validate_runtime_archive(dist: Path) -> None:
    archive = dist / f"{python_version_tag()}.zip"
    if not archive.is_file() or archive.stat().st_size == 0:
        raise RuntimeError(f"Standard library archive was not created: {archive}")
    with zipfile.ZipFile(archive) as standard_library:
        if standard_library.testzip() is not None or not standard_library.namelist():
            raise RuntimeError(f"Standard library archive is invalid: {archive}")


def _validate_windows_runtime(dist: Path) -> None:
    required = (dist / "PYTHON_LICENSE.txt", dist / "python314.dll")
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise RuntimeError("CPython runtime files are missing: " + ", ".join(missing))


def _validate_unix_runtime(dist: Path, stdlib: Path, home: Path) -> None:
    libraries = _python_shared_libraries(home)
    if not libraries:
        raise RuntimeError("The CPython shared library was not found.")
    relative = _relative_runtime_path(home, libraries[0])
    required = [dist / "PYTHON_LICENSE.txt", dist / relative]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise RuntimeError("CPython runtime files are missing: " + ", ".join(missing))
    extension_dir = Path(str(sysconfig.get_config_var("DESTSHARED") or ""))
    if not extension_dir.is_dir():
        extension_dir = stdlib / "lib-dynload"
    if not _native_files(extension_dir, {".so", ".dylib"}):
        raise RuntimeError(f"No CPython native extensions found in {extension_dir}.")
    if not any(
        path.is_file()
        for suffix in (".so", ".dylib")
        for path in dist.rglob(f"*{suffix}")
    ):
        raise RuntimeError("No native CPython extensions were copied into the bundle.")


def validate_runtime(dist: Path, stdlib: Path, home: Path) -> None:
    _validate_runtime_archive(dist)
    if sys.platform == "win32":
        _validate_windows_runtime(dist)
    else:
        _validate_unix_runtime(dist, stdlib, home)


def _windows_compiler_environment() -> tuple[Path, str] | None:
    program_files = [
        Path(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")),
        Path(os.environ.get("PROGRAMFILES", r"C:\Program Files")),
    ]
    target_arch = "arm64" if normalized_architecture() == "arm64" else "x64"
    host_arch = "arm64" if normalized_architecture() == "arm64" else "x64"
    for base in program_files:
        vswhere = base / "Microsoft Visual Studio" / "Installer" / "vswhere.exe"
        if not vswhere.is_file():
            continue
        result = subprocess.run(
            [
                str(vswhere),
                "-all",
                "-products",
                "*",
                "-property",
                "installationPath",
            ],
            capture_output=True,
            check=True,
            text=True,
        )
        for line in result.stdout.splitlines():
            installation = Path(line.strip())
            compiler = installation / "VC" / "Tools" / "MSVC"
            candidates = list(
                compiler.glob(f"*/bin/Host{host_arch}/{target_arch}/cl.exe")
            )
            if candidates:
                devcmd = installation / "Common7" / "Tools" / "VsDevCmd.bat"
                if devcmd.is_file():
                    return devcmd, host_arch
    return None


def _run_msvc(arguments: list[str], cwd: Path) -> None:
    compiler = _windows_compiler_environment()
    if compiler is None:
        command(arguments, cwd=cwd)
        return
    devcmd, host_arch = compiler
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".cmd",
        encoding="utf-8-sig",
        newline="\r\n",
        delete=False,
    ) as batch:
        batch_path = Path(batch.name)
        batch.write("@echo off\r\n")
        batch.write(f'call "{devcmd}" -arch={host_arch} -host_arch={host_arch}\r\n')
        batch.write("if errorlevel 1 exit /b %errorlevel%\r\n")
        batch.write(f"{subprocess.list2cmdline(arguments)}\r\n")
        batch.write("exit /b %errorlevel%\r\n")
    try:
        command(
            [os.environ.get("COMSPEC", "cmd.exe"), "/d", "/c", str(batch_path)],
            cwd=cwd,
        )
    finally:
        batch_path.unlink(missing_ok=True)


def build_launcher(home: Path, runtime: Path, project_root: Path, product: str) -> Path:
    launcher_source = project_root / "scripts" / "bundle" / "launcher.c"
    if not launcher_source.is_file():
        raise FileNotFoundError(f"Launcher source not found: {launcher_source}")
    executable = runtime / (f"{product}.exe" if sys.platform == "win32" else product)
    include = Path(str(sysconfig.get_path("include")))

    if sys.platform == "win32":
        library_dir = home / "libs"
        python_lib = library_dir / f"{python_version_tag()}.lib"
        if not python_lib.is_file():
            raise FileNotFoundError(f"CPython import library not found: {python_lib}")
        object_file = runtime / "launcher.obj"
        arguments = [
            "cl.exe",
            "/nologo",
            "/O2",
            "/MD",
            f"/I{include}",
            f"/Fo{object_file}",
            *(["/DTOURCTL_LAUNCHER"] if product == "tourctl" else []),
            str(launcher_source),
            "/link",
            f"/LIBPATH:{library_dir}",
            python_lib.name,
            f"/OUT:{executable}",
            "/SUBSYSTEM:CONSOLE",
        ]
        try:
            _run_msvc(arguments, project_root)
        finally:
            object_file.unlink(missing_ok=True)
        return executable

    compiler = shutil.which("cc") or shutil.which("clang")
    if compiler is None:
        raise RuntimeError("A C compiler is required to build the CPython launcher.")

    libraries = _python_shared_libraries(home)
    library_dir = libraries[0].parent
    library_name = libraries[0].name
    relative_library_dir = _relative_runtime_path(home, library_dir)
    match = re.match(r"lib(.+?)(?:\.so.*|\.dylib|\.a)$", library_name)
    if match is None:
        raise RuntimeError(f"Unsupported CPython library name: {library_name}")
    link_flags = [
        compiler,
        "-O2",
        f"-I{include}",
        *(["-DTOURCTL_LAUNCHER"] if product == "tourctl" else []),
        str(launcher_source),
        f"-L{library_dir}",
        f"-l{match.group(1)}",
    ]
    if sys.platform == "darwin":
        link_flags.extend(
            ["-Wl,-rpath,@executable_path/" + relative_library_dir.as_posix()]
        )
    else:
        link_flags.extend(["-Wl,-rpath,$ORIGIN/" + relative_library_dir.as_posix()])
    for variable in ("LIBS", "SYSLIBS", "LINKFORSHARED"):
        link_flags.extend(shlex.split(str(sysconfig.get_config_var(variable) or "")))
    link_flags.extend(["-o", str(executable)])
    command(link_flags, cwd=project_root)
    return executable


def export_locked_requirements(project_root: Path, product: str) -> Path:
    descriptor, path = tempfile.mkstemp(prefix=f"{product}-", suffix=".txt")
    os.close(descriptor)
    requirement_file = Path(path)
    try:
        arguments = [
            "uv",
            "export",
            "--project",
            str(project_root),
            "--package",
            product,
            "--locked",
            "--no-dev",
            "--no-default-groups",
        ]
        arguments.extend(
            [
                "--no-emit-workspace",
                "--no-annotate",
                "--no-header",
                "--format",
                "requirements.txt",
                "--output-file",
                str(requirement_file),
            ]
        )
        command(arguments, cwd=project_root)
    except Exception:
        requirement_file.unlink(missing_ok=True)
        raise
    return requirement_file


def install_dependencies(dist: Path, project_root: Path, product: str) -> None:
    requirements = export_locked_requirements(project_root, product)
    try:
        command(
            [
                "uv",
                "pip",
                "install",
                "--python",
                sys.executable,
                "--target",
                str(dist),
                "--compile-bytecode",
                "--requirement",
                str(requirements),
            ],
            cwd=project_root,
        )
    finally:
        requirements.unlink(missing_ok=True)


def validate_dependencies(dist: Path) -> None:
    metadata = list(dist.glob("*.dist-info"))
    if not metadata:
        raise RuntimeError(
            "Dependency installation produced no package metadata; check uv.lock "
            "and the platform-specific wheels."
        )


def copy_application_packages(project_root: Path, dist: Path, product: str) -> None:
    names = PRODUCT_PACKAGES[product]
    application_root = dist / "app"
    for name in names:
        source = project_root / name
        if not source.is_dir():
            raise FileNotFoundError(f"Application package not found: {source}")
        shutil.copytree(
            source,
            application_root / name,
            ignore=shutil.ignore_patterns(
                "__pycache__", ".ruff_cache", "pyproject.toml"
            ),
        )


def validate_application_packages(dist: Path, product: str) -> None:
    names = PRODUCT_PACKAGES[product]
    missing = [
        str(dist / "app" / name / "__init__.py")
        for name in names
        if not (dist / "app" / name / "__init__.py").is_file()
    ]
    if missing:
        raise RuntimeError("Application packages are incomplete: " + ", ".join(missing))


def bundle_application_packages(dist: Path, product: str) -> None:
    application_root = dist / "app"
    archive_path = dist / f"{python_version_tag()}.zip"
    if not application_root.is_dir():
        raise FileNotFoundError(
            f"Application package staging directory is missing: {application_root}"
        )
    if not archive_path.is_file():
        raise FileNotFoundError(f"CPython runtime archive is missing: {archive_path}")

    names = PRODUCT_PACKAGES[product]
    package_files = [
        file
        for name in names
        for file in sorted((application_root / name).rglob("*"))
        if file.is_file()
    ]
    if not package_files or any(
        file.suffix in {".py", ".pyi"} for file in package_files
    ):
        raise RuntimeError(
            "Application packages must be compiled and have no Python sources "
            "before being added to the runtime archive."
        )

    with zipfile.ZipFile(
        archive_path, "a", zipfile.ZIP_DEFLATED, compresslevel=6
    ) as archive:
        existing = set(archive.namelist())
        entries = [
            (file, file.relative_to(application_root).as_posix())
            for file in package_files
        ]
        duplicates = [name for _, name in entries if name in existing]
        if duplicates:
            raise RuntimeError(
                "Application package paths conflict with CPython runtime entries: "
                + ", ".join(duplicates)
            )
        for file, name in entries:
            archive.write(file, name)

    shutil.rmtree(application_root)
    validate_application_archive(archive_path, product)


def validate_application_archive(archive_path: Path, product: str) -> None:
    names = PRODUCT_PACKAGES[product]
    with zipfile.ZipFile(archive_path) as archive:
        if archive.testzip() is not None:
            raise RuntimeError(f"CPython runtime archive is corrupt: {archive_path}")
        entries = set(archive.namelist())
    missing = [
        f"{name}/__init__.pyc"
        for name in names
        if f"{name}/__init__.pyc" not in entries
    ]
    if missing:
        raise RuntimeError(
            "Runtime archive is missing compiled application packages: "
            + ", ".join(missing)
        )
    source_files = [
        entry
        for entry in entries
        if any(entry.startswith(f"{name}/") for name in names)
        and entry.endswith((".py", ".pyi"))
    ]
    if source_files:
        raise RuntimeError(
            "Runtime archive contains application Python sources or stubs: "
            + ", ".join(sorted(source_files))
        )


def collect_license_files(dist_info: Path) -> list[Path]:
    candidates = list((dist_info / "licenses").rglob("*"))
    candidates.extend(path for path in dist_info.iterdir() if path.is_file())
    return sorted(
        {
            path
            for path in candidates
            if path.is_file()
            and (
                path.parent.name.lower() == "licenses"
                or path.name.lower().startswith(("license", "copying", "notice"))
            )
        }
    )


def _package_license_sections(dist: Path) -> list[str]:
    sections: list[str] = []
    for metadata in sorted(dist.glob("*.dist-info")):
        name = metadata.name.removesuffix(".dist-info")
        package_metadata = metadata / "METADATA"
        if package_metadata.is_file():
            for line in package_metadata.read_text(errors="replace").splitlines():
                if line.startswith("Name:"):
                    name = line.partition(":")[2].strip()
                    break
        for license_file in collect_license_files(metadata):
            sections.extend(
                [
                    f"{name} - {license_file.relative_to(metadata).as_posix()}",
                    license_file.read_text(errors="replace"),
                ]
            )
    return sections


def write_licenses(bundle: Path, dist: Path, project_root: Path) -> None:
    if not (project_root / "LICENSE").is_file():
        raise FileNotFoundError(
            f"Project license is missing: {project_root / 'LICENSE'}"
        )
    if not (dist / "PYTHON_LICENSE.txt").is_file():
        raise FileNotFoundError(
            f"CPython license is missing: {dist / 'PYTHON_LICENSE.txt'}"
        )
    sections: list[str] = []
    for label, path in (
        ("Tourillon", project_root / "LICENSE"),
        ("CPython", dist / "PYTHON_LICENSE.txt"),
    ):
        if path.is_file():
            sections.extend([label, "=" * len(label), path.read_text(errors="replace")])
    sections.extend(_package_license_sections(dist))
    license_file = bundle / "THIRD_PARTY_LICENSES"
    license_file.write_text("\n\n".join(sections).rstrip() + "\n", encoding="utf-8")
    if license_file.stat().st_size == 0:
        raise RuntimeError("Third-party license notices were not written.")


def strip_sources(dist: Path) -> None:
    compiled = compileall.compile_dir(
        str(dist),
        force=True,
        quiet=1,
        legacy=True,
        optimize=2,
        stripdir=str(dist),
        prependdir="<tourillon>",
    )
    if not compiled:
        raise RuntimeError(
            "At least one Python module could not be compiled; inspect the build log."
        )
    for pattern in ("*.py", "*.pyi"):
        for source in dist.rglob(pattern):
            source.unlink()
    for cache in dist.rglob("__pycache__"):
        shutil.rmtree(cache)
    if not any(dist.rglob("*.pyc")):
        raise RuntimeError("Compilation produced no Python bytecode.")
    if any(dist.rglob("*.py")) or any(dist.rglob("*.pyi")):
        raise RuntimeError("Python source or stub files remain after source stripping.")


def write_bundle_metadata(
    runtime: Path, bundle: Path, version: str, executable: Path, product: str
) -> None:
    metadata = {
        "product": product,
        "version": version,
        "platform": normalized_platform(),
        "architecture": normalized_architecture(),
        "python": platform.python_version(),
        "launcher": executable.name,
        "runtime_dist": ".",
    }
    (runtime / "PRODUCT_RUNTIME_INFO.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    install_instructions = (
        "Offline installation (current user):\n"
        "  powershell -ExecutionPolicy Bypass -File .\\install.ps1\n"
        "The installer adds dist to your user PATH. Use -Update to replace it.\n"
        if sys.platform == "win32"
        else "Offline installation (current user):\n"
        "  ./install.sh\n"
        f"Creates ~/.local/bin/{product}; add ~/.local/bin to PATH if needed.\n"
        "Use --update to replace the installed version.\n"
    )
    (bundle / "README.txt").write_text(
        f"{product} {version} ({bundle_tag()})\n"
        "Self-contained CPython runtime and locked application dependencies.\n"
        f"{install_instructions}"
        f"Direct executable: dist/{product}.exe (Windows) or dist/{product} (Unix).\n"
        "See THIRD_PARTY_LICENSES for bundled license notices.\n",
        encoding="utf-8",
    )


def write_platform_installer(bundle: Path) -> None:
    installer_name = "install.ps1" if sys.platform == "win32" else "install.sh"
    installer = Path(__file__).with_name(installer_name)
    if not installer.is_file():
        raise FileNotFoundError(f"Platform installer script is missing: {installer}")
    destination = bundle / installer_name
    shutil.copy2(installer, destination)
    if sys.platform != "win32":
        destination.chmod(destination.stat().st_mode | 0o111)


def _validate_bundle_documents(runtime: Path, bundle: Path) -> None:
    for required in (
        bundle / "README.txt",
        bundle / "THIRD_PARTY_LICENSES",
    ):
        if not required.is_file() or required.stat().st_size == 0:
            raise RuntimeError(
                f"Required bundle metadata is missing or empty: {required}"
            )
    if (runtime / "README.txt").exists() or (runtime / "THIRD_PARTY_LICENSES").exists():
        raise RuntimeError(
            "Bundle instructions and licenses must live beside dist/ in the bundle root."
        )


def validate_bundle_metadata(
    runtime: Path, bundle: Path, windows: bool, product: str
) -> None:
    metadata_file = runtime / "PRODUCT_RUNTIME_INFO.json"
    if not metadata_file.is_file() or metadata_file.stat().st_size == 0:
        raise RuntimeError(
            f"Required bundle metadata is missing or empty: {metadata_file}"
        )
    _validate_bundle_documents(runtime, bundle)
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    if metadata.get("product") != product:
        raise RuntimeError(f"Unexpected product in runtime metadata: {metadata}")
    _validate_version(metadata.get("version"))
    if metadata.get("launcher") != (f"{product}.exe" if windows else product):
        raise RuntimeError(f"Unexpected launcher in runtime metadata: {metadata}")
    installer = bundle / ("install.ps1" if windows else "install.sh")
    if not installer.is_file() or installer.stat().st_size == 0:
        raise RuntimeError(f"Platform installer script was not generated: {installer}")
    if windows and (bundle / "bin").exists():
        raise RuntimeError("The Windows bundle must not contain a bin command shim.")
    if not windows and not os.access(installer, os.X_OK):
        raise RuntimeError(f"Unix installer is not executable: {installer}")


def main() -> None:
    try:
        args = parse_args()
        bundle = build(args)
    except (
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        zipfile.BadZipFile,
    ) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print(f"Portable bundle ready: {bundle}")


def validate_launcher_file(executable: Path) -> None:
    if not executable.is_file():
        raise FileNotFoundError(f"Native launcher was not built: {executable}")
    if executable.stat().st_size == 0:
        raise RuntimeError(f"Native launcher is empty: {executable}")
    if sys.platform != "win32" and not os.access(executable, os.X_OK):
        raise RuntimeError(f"Native launcher is not executable: {executable}")


def validate_linux_shared_dependencies(dist: Path, executable: Path) -> None:
    ldd = shutil.which("ldd")
    if ldd is None:
        raise RuntimeError(
            "ldd is required to audit Linux shared-library dependencies."
        )
    binaries = [executable, *dist.rglob("*.so*")]
    for binary in binaries:
        result = subprocess.run(
            [ldd, str(binary)],
            capture_output=True,
            check=False,
            text=True,
        )
        output = result.stdout + result.stderr
        if result.returncode != 0 or "not found" in output:
            raise RuntimeError(
                f"Unresolved shared-library dependency for {binary}:\n{output.strip()}"
            )


def _audit_bundle_contents(dist: Path, strict_no_py: bool) -> int:
    source_count = sum(1 for _ in dist.rglob("*.py"))
    stub_count = sum(1 for _ in dist.rglob("*.pyi"))
    if strict_no_py and source_count:
        raise RuntimeError(f"Bundle contains {source_count} Python source files.")
    if stub_count:
        raise RuntimeError(f"Bundle contains {stub_count} non-runtime type stubs.")
    if not any(dist.rglob("*.pyc")):
        raise RuntimeError("Bundle has no compiled Python bytecode.")
    return source_count


def _audit_windows_bundle(dist: Path) -> None:
    required = (
        dist / "python314.dll",
        dist / "python314.zip",
        dist / "PYTHON_LICENSE.txt",
        dist / "vcruntime140.dll",
    )
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise RuntimeError("Windows runtime is incomplete: " + ", ".join(missing))
    if not any(dist.rglob("*.pyd")):
        raise RuntimeError("Bundle has no native CPython extension modules (.pyd).")


def _audit_unix_bundle(dist: Path, python_home: Path) -> None:
    libraries = _python_shared_libraries(python_home)
    if not libraries:
        raise RuntimeError("The source CPython shared library is unavailable.")
    relative_library = _relative_runtime_path(python_home, libraries[0])
    required = (
        dist / f"{python_version_tag()}.zip",
        dist / "PYTHON_LICENSE.txt",
        dist / relative_library,
    )
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise RuntimeError("Unix runtime is incomplete: " + ", ".join(missing))
    if not any(dist.rglob("*.so")):
        raise RuntimeError("Bundle has no native CPython extension modules (.so).")


def audit_bundle(
    dist: Path,
    executable: Path,
    strict_no_py: bool,
    python_home: Path,
) -> None:
    validate_launcher_file(executable)
    source_count = _audit_bundle_contents(dist, strict_no_py)
    if sys.platform == "win32":
        _audit_windows_bundle(dist)
    else:
        _audit_unix_bundle(dist, python_home)
        if sys.platform.startswith("linux"):
            validate_linux_shared_dependencies(dist, executable)
    print(f"Bundle audit: .py={source_count}")


def smoke_test(executable: Path, product: str) -> None:
    print(f"Smoke-testing {executable.name} --help outside the repository")
    result = subprocess.run(
        [str(executable), "--help"],
        cwd=tempfile.gettempdir(),
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
        timeout=60,
    )
    output = result.stdout + result.stderr
    if result.returncode or f"Usage: {product}" not in output:
        raise RuntimeError(
            f"Launcher smoke test failed ({result.returncode}):\n{output}\n"
            "Check that the runtime files are complete and that this runner "
            "matches the bundle architecture."
        )


def create_archive(bundle: Path, output_root: Path, version: str) -> Path:
    archive = output_root / f"{bundle.name}-{version}-{bundle_tag()}"
    print(f"Creating {archive}.zip")
    result = Path(shutil.make_archive(str(archive), "zip", output_root, bundle.name))
    _validate_output_archive(result, bundle.name)
    return result


def _validate_output_archive(archive: Path, bundle_name: str = "tourillon") -> None:
    if not archive.is_file() or archive.stat().st_size == 0:
        raise RuntimeError(f"ZIP archive is missing or empty: {archive}")
    with zipfile.ZipFile(archive) as packaged:
        entries = packaged.infolist()
        if packaged.testzip() is not None or not entries:
            raise RuntimeError(f"ZIP archive is corrupt or empty: {archive}")
        names = [entry.filename for entry in entries]
        if any(name.endswith((".py", ".pyi")) for name in names):
            raise RuntimeError(
                f"ZIP archive contains Python source or stubs: {archive}"
            )
        _validate_archive_bundle_layout(entries, names, bundle_name)


def _validate_archive_bundle_layout(
    entries: list[zipfile.ZipInfo], names: list[str], bundle_name: str
) -> None:
    required_documents = {
        f"{bundle_name}/README.txt",
        f"{bundle_name}/THIRD_PARTY_LICENSES",
    }
    if not required_documents.issubset(names):
        missing = ", ".join(sorted(required_documents.difference(names)))
        raise RuntimeError(
            f"ZIP is missing bundle-root documentation or licenses: {missing}."
        )
    if any(
        name
        in {
            f"{bundle_name}/dist/README.txt",
            f"{bundle_name}/dist/THIRD_PARTY_LICENSES",
        }
        for name in names
    ):
        raise RuntimeError(
            "ZIP documentation and licenses must live beside dist/ in the bundle root."
        )
    installers = [
        entry
        for entry in entries
        if entry.filename in (f"{bundle_name}/install.ps1", f"{bundle_name}/install.sh")
    ]
    if len(installers) != 1:
        raise RuntimeError(
            "ZIP must contain exactly one native installer beside dist/, "
            f"found {len(installers)}."
        )
    installer = installers[0]
    if installer.filename.endswith("/install.sh"):
        permissions = installer.external_attr >> 16
        if not permissions & 0o111:
            raise RuntimeError("POSIX installer is not executable in the ZIP archive.")


def _prepare_output(output_root: Path) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=output_root, prefix=".tourillon-write-test-"):
        pass
    return output_root


def _prepare_staging_root(output_root: Path) -> Path:
    return Path(tempfile.mkdtemp(prefix=".tourillon-build-", dir=output_root))


def _validate_output(output_root: Path) -> None:
    if not output_root.is_dir():
        raise NotADirectoryError(f"Output location is not a directory: {output_root}")


def _publish_bundle(
    staged_bundle: Path,
    final_bundle: Path,
    staged_archive: Path | None,
    final_archive: Path | None,
) -> None:
    bundle_backup = final_bundle.with_name(f".{final_bundle.name}.previous")
    archive_backup = (
        final_archive.with_name(f".{final_archive.name}.previous")
        if final_archive is not None
        else None
    )
    if bundle_backup.exists() or (archive_backup and archive_backup.exists()):
        raise FileExistsError(
            "A previous bundle recovery file exists; inspect it before rebuilding."
        )
    moved_bundle = False
    moved_archive = False
    installed_bundle = False
    try:
        moved_bundle = _backup_path(final_bundle, bundle_backup)
        if final_archive is not None and archive_backup is not None:
            moved_archive = _backup_path(final_archive, archive_backup)
        staged_bundle.replace(final_bundle)
        installed_bundle = True
        if staged_archive is not None and final_archive is not None:
            staged_archive.replace(final_archive)
    except OSError:
        _rollback_publication(
            final_bundle,
            bundle_backup,
            installed_bundle,
            moved_bundle,
            final_archive,
            archive_backup,
            moved_archive,
        )
        raise
    if bundle_backup.exists():
        shutil.rmtree(bundle_backup)
    if archive_backup and archive_backup.exists():
        archive_backup.unlink()


def _rollback_publication(
    final_bundle: Path,
    bundle_backup: Path,
    installed_bundle: bool,
    moved_bundle: bool,
    final_archive: Path | None,
    archive_backup: Path | None,
    moved_archive: bool,
) -> None:
    if installed_bundle and final_bundle.exists():
        shutil.rmtree(final_bundle)
    _restore_path(bundle_backup, final_bundle, moved_bundle)
    if final_archive is not None and archive_backup is not None:
        _restore_path(archive_backup, final_archive, moved_archive)


def _backup_path(path: Path, backup: Path) -> bool:
    if not path.exists():
        return False
    path.replace(backup)
    return True


def _restore_path(backup: Path, original: Path, moved: bool) -> None:
    if moved and backup.exists():
        backup.replace(original)


def _validate_published_bundle(
    bundle: Path, archive: Path | None, windows: bool, product: str
) -> None:
    dist = bundle / "dist"
    executable = dist / (f"{product}.exe" if windows else product)
    validate_launcher_file(executable)
    validate_bundle_metadata(dist, bundle, windows, product)
    validate_application_archive(dist / f"{python_version_tag()}.zip", product)
    if (dist / "app").exists():
        raise RuntimeError(
            "Application packages must be stored in the CPython runtime archive."
        )
    if archive is not None:
        _validate_output_archive(archive, product)


def _validate_version(version: str) -> None:
    if not isinstance(version, str) or not re.fullmatch(
        r"[0-9]+\.[0-9]+\.[0-9]+(?:\.dev[0-9]+)?(?:[-+][0-9A-Za-z.-]+)?", version
    ):
        raise RuntimeError(
            f"Bundle version is not a supported release version: {version!r}"
        )


def bundle_version(product: str, release_tag: str | None, version: str | None) -> str:
    version = version or None
    if release_tag:
        tag_match = re.fullmatch(r"(tourillon|tourctl)-v(.+)", release_tag)
        if tag_match is None:
            raise RuntimeError(
                f"Release tag {release_tag!r} must use <product>-v<version>."
            )
        if tag_match.group(1) != product:
            raise RuntimeError(
                f"Release tag {release_tag!r} selects {tag_match.group(1)}, "
                f"not {product}."
            )
        release_version = tag_match.group(2)
        _validate_version(release_version)
        if version is not None and version != release_version:
            raise RuntimeError(
                f"Explicit version {version!r} does not match release tag "
                f"{release_tag!r}."
            )
        return release_version
    resolved_version = version or "0.0.0+local"
    _validate_version(resolved_version)
    return resolved_version


def build(args: argparse.Namespace) -> Path:
    project_root = args.project_root.resolve()
    python_home = args.python_home.resolve()
    product = args.product

    run_step(
        "Checking host and project prerequisites",
        "Use CPython 3.14, install uv and a native C compiler, and build Linux "
        "bundles on Ubuntu 22.04 (glibc 2.35).",
        lambda: validate_build_environment(project_root, python_home, product),
        lambda _: None,
    )
    version = bundle_version(product, args.release_tag, args.version)
    output_root = (
        args.output_root or project_root / "dist" / f"{product}-exe-{bundle_tag()}"
    ).resolve()
    final_bundle = output_root / product
    run_step(
        "Checking output directory",
        "Choose a writable output directory with enough free disk space.",
        lambda: _prepare_output(output_root),
        _validate_output,
    )
    staging_root = run_step(
        "Creating temporary build directory",
        "Check output-directory write access and free disk space.",
        lambda: _prepare_staging_root(output_root),
        _validate_output,
    )
    bundle = staging_root / product
    dist = bundle / "dist"
    print(f"Target: {bundle_tag()} with CPython {platform.python_version()}")
    staged_archive: Path | None = None
    try:
        run_step(
            "Preparing staging directory",
            "Check temporary-directory permissions and free disk space.",
            lambda: dist.mkdir(parents=True),
            lambda _: _validate_output(dist),
        )
        run_step(
            "Copying CPython runtime and building standard library",
            "Install a complete CPython 3.14 distribution with shared libraries "
            "and license files.",
            lambda: _copy_runtime(python_home, dist),
            lambda result: validate_runtime(dist, result, python_home),
        )
        executable = run_step(
            "Compiling native launcher",
            "Install MSVC Build Tools on Windows or GCC/Clang development tools "
            "on Linux/macOS, including Python.h.",
            lambda: build_launcher(python_home, dist, project_root, product),
            validate_launcher_file,
        )
        run_step(
            "Installing locked runtime dependencies",
            "Check network access, uv.lock, and availability of wheels for this "
            "OS/architecture; no unlocked dependency fallback is used.",
            lambda: install_dependencies(dist, project_root, product),
            lambda _: validate_dependencies(dist),
        )
        run_step(
            f"Copying {product} application packages",
            f"Ensure {', '.join(PRODUCT_PACKAGES[product])}/ are present in the project checkout.",
            lambda: copy_application_packages(project_root, dist, product),
            lambda _: validate_application_packages(dist, product),
        )
        run_step(
            "Compiling optimized bytecode and removing sources",
            "Inspect Python compile errors in the preceding log and ensure the "
            "output filesystem permits deleting generated source files.",
            lambda: strip_sources(dist),
            lambda _: None,
        )
        run_step(
            f"Bundling {product} Python packages into the runtime archive",
            "Check compiled application bytecode and resolve any archive path "
            "conflicts with the CPython standard library.",
            lambda: bundle_application_packages(dist, product),
            lambda _: None,
        )

        def write_bundle_files() -> None:
            write_licenses(bundle, dist, project_root)
            write_bundle_metadata(
                dist, bundle, version, dist / executable.name, product
            )
            write_platform_installer(bundle)

        run_step(
            "Writing licenses, metadata, and client installer",
            "Restore the project and CPython license files and scripts/bundle/install.ps1.",
            write_bundle_files,
            lambda _: validate_bundle_metadata(
                dist, bundle, sys.platform == "win32", product
            ),
        )
        run_step(
            "Auditing bundle contents and native dependencies",
            "Inspect missing runtime files reported above; on Linux, rebuild on "
            "Ubuntu 22.04 and install any missing shared-library packages.",
            lambda: audit_bundle(
                dist, dist / executable.name, args.strict_no_py, python_home
            ),
            lambda _: None,
        )
        run_step(
            f"Running {product} --help outside the repository",
            "Check launcher output, runtime library paths, and application "
            "dependencies; run the same command manually from a temporary directory.",
            lambda: smoke_test(dist / executable.name, product),
            lambda _: None,
        )
        if args.zip_output:
            staged_archive = run_step(
                "Creating and validating ZIP archive",
                "Check free disk space and permissions in the output directory.",
                lambda: create_archive(bundle, staging_root, version),
                lambda archive: _validate_output_archive(archive, product),
            )
        final_archive = (
            output_root / f"{product}-{version}-{bundle_tag()}.zip"
            if staged_archive is not None
            else None
        )
        run_step(
            "Publishing verified bundle",
            "Close processes using the existing bundle and check output-directory "
            "permissions; the previous verified bundle is preserved on failure.",
            lambda: _publish_bundle(
                bundle, final_bundle, staged_archive, final_archive
            ),
            lambda _: _validate_published_bundle(
                final_bundle,
                final_archive,
                sys.platform == "win32",
                product,
            ),
        )
    finally:
        try:
            shutil.rmtree(staging_root)
        except OSError as error:
            print(
                f"WARNING: could not remove temporary build directory {staging_root}: "
                f"{error}. Remove it manually after checking its contents.",
                file=sys.stderr,
            )
    return final_bundle


if __name__ == "__main__":
    main()
