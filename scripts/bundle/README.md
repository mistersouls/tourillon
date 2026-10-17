# Portable Tourillon and Tourctl bundles

The repository is a uv workspace: the root project is for development and
testing, while `tourillon/pyproject.toml` and `tourctl/pyproject.toml` define
the two installable products and their runtime dependencies. Shared runtime
dependencies, including Cyclopts, are declared once in
`tourlib/pyproject.toml` and inherited by both products. The root project is
non-installable and includes both products in its workspace dependency group.
One `uv sync --extra dev` installs the development tools and both product
packages in the same environment. `tourclient` remains source-only and is
included in the Tourctl distribution without a separate project manifest.

Each product has its own native bundle and release lifecycle. `tourillon`
ships `tourillon` and `tourlib`; `tourctl` ships `tourctl`, `tourclient`, and
`tourlib`. The C launcher initializes CPython through `Python.h` and runs the
matching product module. The CPython runtime, standard library, native
extensions, and locked dependencies are shipped together. Python sources
(`.py`) and type stubs (`.pyi`) are removed after compilation to optimized
bytecode.

## Local build

Requirements: `uv` and a native C compiler (MSVC on Windows, Clang/GCC on
Linux and macOS). `uv` installs Python 3.14 for the host architecture.

```powershell
uv python install 3.14
uv sync --locked --extra dev --python 3.14
uv run --no-sync python scripts/bundle/build.py --product tourillon --strict-no-py --zip-output
uv run --no-sync python scripts/bundle/build.py --product tourctl --strict-no-py --zip-output
```

Pass `--version X.Y.Z` for a manually versioned build; local builds default to
`0.0.0+local`. A release tag such as `tourctl-v1.2.3` is the authoritative
version for a release build. The product manifests carry development metadata
versions; `tourlib` and the root development project use workspace metadata
versions only. The bundle builder does not use these metadata versions for
releases.

Development release tags such as `tourillon-v0.0.1.dev0` and
`tourctl-v0.1.0.dev1` are also supported by the builder and both installers.

The output is separate by product and native platform:

```text
dist/tourillon-exe-windows-arm64/tourillon/
dist/tourctl-exe-windows-arm64/tourctl/
dist/tourillon-exe-linux-x86_64/tourillon/
dist/tourctl-exe-linux-x86_64/tourctl/
```

Each bundle keeps its documentation, license notices, and installer beside
`dist/`; the native executable and runtime are inside `dist/`:

```text
tourillon/
├── README.txt
├── THIRD_PARTY_LICENSES
├── install.ps1 (Windows) or install.sh (Linux/macOS)
└── dist/
    ├── tourillon.exe (Windows) or tourillon (Linux/macOS)
    ├── PRODUCT_RUNTIME_INFO.json (product and version metadata)
    ├── python314.zip (standard library and that product's application packages)
    └── native dependencies and extension modules
```

Application packages are compiled to optimized bytecode and stored in
`python314.zip`. Third-party packages remain extracted under `dist/`, since
native extensions such as LMDB and cryptography must be loaded from files.

## Release matrix

`.github/workflows/release-dist.yml` builds the selected product on all six
native GitHub Actions runners with CPython 3.14. In GitHub Actions, run
**Release distributions** manually to test one or both products. Push
`tourillon-vX.Y.Z` or `tourctl-vX.Y.Z` to build only that product and attach
its bundles to a GitHub release.

The product selection is resolved in the matrix before expanding the jobs:
a product tag or a single-product manual run creates six build jobs; a manual
run with `all` creates twelve. Manual runs on branches upload artifacts only;
tag runs also publish release assets.

| Target | Runner |
| --- | --- |
| Windows ARM64 | `windows-11-arm` |
| Windows x86_64 | `windows-2025` |
| Linux ARM64 | `ubuntu-22.04-arm` |
| Linux x86_64 | `ubuntu-22.04` |
| macOS ARM64 | `macos-15` |
| macOS x86_64 | `macos-15-intel` |

Each job creates a self-contained ZIP. The native executable is under `dist/`
on all platforms (`tourillon.exe`/`tourctl.exe` on Windows).
There is no command shim: invoke the native executable in `dist` directly.
Every ZIP includes an offline native installer: `install.ps1` on Windows,
`install.sh` on Linux and macOS. The installers validate the platform and
architecture, test the executable, install into a per-user versioned
directory, and update a `current` link. Unix installers also create a symbolic
link in the command directory; Windows adds `current\dist` to the user's PATH.
Windows jobs test ZIP installation and update. MSI packaging is currently
disabled while the WiX 7 EULA and OSMF terms are under review.

Both installers are fully offline: they use only files inside the ZIP and
operating-system utilities; they do not download runtimes or dependencies.
They check the bundle metadata, OS and architecture, CPython version, launcher
smoke test, and destination write access. The bundle is copied to a staging
directory and tested before activation. Failures include a recommendation.

Windows installs by default under
`%LOCALAPPDATA%\Programs\Tourillon` or `%LOCALAPPDATA%\Programs\Tourctl`;
Linux and macOS install under `~/.local/share/<product>`, with a command link
under `~/.local/bin`. Override the location if needed:

```powershell
.\install.ps1 `
  -InstallDir "$env:LOCALAPPDATA\Programs\Tourctl"
```

```sh
./install.sh \
  --install-dir "$HOME/.local/share/tourctl" \
  --bin-dir "$HOME/.local/bin"
```

On Unix, ensure `~/.local/bin` is on `PATH`; the installer prints a shell
configuration recommendation when it is not. On Windows, the installer adds
the executable directory to the current user's PATH. Use `-Update` or
`--update` to replace the installed version.

To run directly without changing PATH, invoke the executable in the installed
`current/dist` directory. For example, after installing on Windows:

```powershell
& "$env:LOCALAPPDATA\Programs\Tourctl\v2\current\dist\tourctl.exe" --help
```

Native artifacts must be built on their target platform: cryptographic
extensions, LMDB, CPython libraries, and the C launcher are not interchangeable
across operating systems or architectures. Linux bundles are built on Ubuntu
22.04 and require glibc 2.35 or newer; older Linux distributions are not
supported. The builder rejects missing glibc, musl, and glibc versions below
2.35, and warns when building against a newer glibc because that can reduce
compatibility with older Linux distributions.

## Verification

After extracting the ZIP:

```text
# Windows
.\tourctl\dist\tourctl.exe --help

# Linux/macOS
./tourctl/dist/tourctl --help
```

macOS bundles are not signed or notarized by default. Distribution to end users
may require Apple Developer signing and notarization.
