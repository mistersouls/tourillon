# Installation

Install the native Tourillon applications on Windows, Linux, or macOS.
For a local container-based trial instead, use the
[Docker Compose example](../examples/docker/README.md).

## Choose a download

Download a ZIP from [GitHub Releases](https://github.com/mistersouls/tourillon/releases):

| Product | Purpose |
| --- | --- |
| `tourillon` | Configure and run a node. |
| `tourctl` | Inspect nodes and write, read, or delete values. |

Products have separate releases. Choose the product, operating system, and
architecture you need; for example,
`tourctl-0.1.0.dev2-windows-x86_64.zip`.

Use `arm64` for ARM systems and `x86_64` for Intel/AMD systems. Inside WSL,
choose a **Linux** bundle, not a Windows bundle. Run `uname -m` in Linux:
`aarch64` means ARM64; `x86_64` means x86_64.

Linux bundles target glibc 2.35 or newer, such as Ubuntu 22.04 or newer.
Alpine's musl runtime is not supported. macOS bundles are not signed or
notarized by default; local security policy may prevent them from starting.

## Install

Extract the **complete** ZIP. In the extracted product directory, beside
`dist/`, run the included installer:

**Windows, PowerShell 5.1 or newer:**

```powershell
.\install.ps1
```

**Linux and macOS:**

```sh
sh ./install.sh
```

The installer works offline, validates the bundle against the host, and tests
the executable before activating it. No additional application runtime needs
to be installed. Linux still requires the compatible system libraries noted
above; the bundle is not a standalone operating-system image.

Default locations:

| Platform | Installation directory | Command location |
| --- | --- | --- |
| Windows | `%LOCALAPPDATA%\Programs\Tourillon` or `Tourctl` | `current\dist` under the installation directory, added to the user PATH |
| Linux/macOS | `~/.local/share/tourillon` or `tourctl` | A link under `~/.local/bin` |

On Windows, open a new terminal if the command is not found. On Linux/macOS,
ensure `~/.local/bin` is on PATH:

```sh
export PATH="$HOME/.local/bin:$PATH"
```

Add that line to your shell profile to retain it for new terminals.
Verify each product you installed:

```text
tourillon --help
tourctl --help
```

## Custom directories and updates

Set a custom installation directory:

```powershell
.\install.ps1 -InstallDir "$env:LOCALAPPDATA\Programs\Tourctl"
```

```sh
sh ./install.sh \
  --install-dir "$HOME/.local/share/tourctl" \
  --bin-dir "$HOME/.local/bin"
```

To activate a new version or replace an installed version, extract its ZIP
and run `.\install.ps1 -Update` or `sh ./install.sh --update`.
Repeat any custom directory options used for the existing installation.
The installers manage versioned directories and a `current` link.

## Troubleshooting

- **Platform or architecture mismatch:** download the bundle for the operating
  system where it will execute, including inside WSL or a container.
- **Command not found:** check PATH or run the installed executable directly:
  `~/.local/share/tourctl/current/dist/tourctl --help` on Unix, or
  `& "$env:LOCALAPPDATA\Programs\Tourctl\current\dist\tourctl.exe" --help`
  in PowerShell.
- **Executable self-test fails:** retain the error output, check the host's
  system-library compatibility, and re-extract the complete ZIP. Do not move
  the executable out of its `dist` directory.
- **Permission or command-link conflict:** follow the installer's recommendation
  or select writable installation and command directories. Do not overwrite
  an unrelated command.

Continue with [your first node and value](../README.md#your-first-node-and-first-value).
Read the [current capabilities and limitations](status.md) before evaluating
Tourillon beyond a local trial.
