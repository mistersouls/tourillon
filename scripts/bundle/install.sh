#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Usage: ./install.sh [--install-dir DIR] [--bin-dir DIR] [--update]

Install this offline application bundle for the current user.
Options:
  --install-dir DIR  Installation root (default: ~/.local/share/<product>)
  --bin-dir DIR      Command-link directory (default: ~/.local/bin)
  --update           Replace the installed version
  -h, --help         Show this help
EOF
}

fail() {
    printf 'ERROR: %s\nRecommendation: %s\n' "$1" "$2" >&2
    exit 1
}

install_dir=${PRODUCT_INSTALL_DIR:-${TOURILLON_INSTALL_DIR:-}}
bin_dir=${PRODUCT_BIN_DIR:-${TOURILLON_BIN_DIR:-"$HOME/.local/bin"}}
update=false

while [ "$#" -gt 0 ]; do
    case "$1" in
        --install-dir)
            [ "$#" -ge 2 ] || fail "--install-dir requires a path." "Pass a writable directory path."
            install_dir=$2
            shift 2
            ;;
        --bin-dir)
            [ "$#" -ge 2 ] || fail "--bin-dir requires a path." "Pass a writable directory path."
            bin_dir=$2
            shift 2
            ;;
        --update)
            update=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "Unknown option: $1" "Run ./install.sh --help to see supported options."
            ;;
    esac
done

case "$(uname -s)" in
    Linux) host_platform=linux ;;
    Darwin) host_platform=macos ;;
    *)
        fail "Unsupported operating system: $(uname -s)" \
            "Use the Windows PowerShell installer on Windows."
        ;;
esac

case "$(uname -m)" in
    x86_64|amd64) host_arch=x86_64 ;;
    arm64|aarch64) host_arch=arm64 ;;
    *)
        fail "Unsupported processor architecture: $(uname -m)" \
            "Download a Tourillon bundle built for a supported architecture."
        ;;
esac

if [ "$host_platform" = linux ]; then
    if ! command -v getconf >/dev/null 2>&1; then
        fail "Cannot determine the system glibc version." \
            "Use a glibc-based Linux distribution such as Ubuntu 22.04 or newer."
    fi
    libc_version=$(getconf GNU_LIBC_VERSION 2>/dev/null || true)
    libc_is_supported=$(printf '%s\n' "$libc_version" | awk '
        $1 == "glibc" {
            split($2, version, ".")
            if (version[1] > 2 || (version[1] == 2 && version[2] >= 35)) {
                print "yes"
            }
        }')
    [ "$libc_is_supported" = yes ] \
        || fail "The host must provide glibc 2.35 or newer (detected: ${libc_version:-unknown})." \
            "Use Ubuntu 22.04 or a newer glibc-based Linux distribution; musl/Alpine is not supported."
fi

bundle_dir=$(CDPATH= cd "$(dirname "$0")" && pwd -P)
dist_dir=$bundle_dir/dist
license_file=$bundle_dir/THIRD_PARTY_LICENSES
metadata_file=$dist_dir/PRODUCT_RUNTIME_INFO.json

printf '[1/5] Checking bundle and host compatibility...\n'
for required in "$metadata_file" "$license_file"; do
    [ -f "$required" ] || fail "Required bundle file is missing: $required" \
        "Re-extract the complete product ZIP and run its install.sh."
done
version=$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$metadata_file")
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(\.dev[0-9]+)?([-+][0-9A-Za-z.-]+)?$' \
    || fail "Bundle metadata has an invalid product version." \
        "Use an unmodified release ZIP downloaded from the official release."
grep -Eq '"platform"[[:space:]]*:[[:space:]]*"'$host_platform'"' "$metadata_file" \
    || fail "This bundle is not built for $host_platform." \
        "Download the $host_platform-$host_arch bundle."
grep -Eq '"architecture"[[:space:]]*:[[:space:]]*"'$host_arch'"' "$metadata_file" \
    || fail "This bundle is not built for $host_arch." \
        "Download the $host_platform-$host_arch bundle."
grep -Eq '"python"[[:space:]]*:[[:space:]]*"3\.14\.' "$metadata_file" \
    || fail "Bundle does not contain the supported CPython 3.14 runtime." \
        "Download the current product bundle."
product=$(sed -n 's/.*"product"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$metadata_file")
launcher=$(sed -n 's/.*"launcher"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$metadata_file")
printf '%s\n' "$product" | grep -Eq '^[a-z][a-z0-9-]*$' \
    || fail "Bundle metadata has an invalid product name." "Use an unmodified product ZIP."
case "$product" in
    tourillon|tourctl) ;;
    *) fail "Bundle metadata references an unsupported product: $product" \
        "Use an unmodified product ZIP." ;;
esac
[ "$launcher" = "$product" ] \
    || fail "Bundle metadata does not reference the expected native executable." \
        "Re-extract the complete product ZIP."
[ -f "$dist_dir/$launcher" ] \
    || fail "Required bundle executable is missing: $dist_dir/$launcher" \
        "Re-extract the complete product ZIP."
if [ -z "$install_dir" ]; then
    install_dir="${HOME:?HOME is not set}/.local/share/$product"
fi

printf '[2/5] Checking executable and installation permissions...\n'
chmod +x "$dist_dir/$launcher" || fail "Could not mark the bundled executable as runnable." \
    "Re-extract the ZIP on a filesystem that allows executable files."
if [ -e "$install_dir" ] && [ ! -d "$install_dir" ]; then
    fail "Installation path is not a directory: $install_dir" \
        "Choose a writable directory with --install-dir."
fi
mkdir -p "$install_dir" "$bin_dir" 2>/dev/null \
    || fail "Cannot create installation or command directory." \
        "Choose writable paths with --install-dir and --bin-dir."
install_dir=$(CDPATH= cd "$install_dir" && pwd -P)
bin_dir=$(CDPATH= cd "$bin_dir" && pwd -P)
write_probe=$install_dir/.$product-write-test-$$
if ! (umask 077 && : > "$write_probe") 2>/dev/null; then
    fail "Installation directory is not writable: $install_dir" \
        "Choose a directory owned by your user."
fi
rm -f "$write_probe"
write_probe=$bin_dir/.$product-write-test-$$
if ! (umask 077 && : > "$write_probe") 2>/dev/null; then
    fail "Command directory is not writable: $bin_dir" \
        "Choose a directory owned by your user with --bin-dir."
fi
rm -f "$write_probe"

printf '[3/5] Testing the downloaded executable offline...\n'
if ! help_output=$("$dist_dir/$launcher" --help 2>&1) ||
    ! printf '%s\n' "$help_output" | grep -q "Usage: $product"; then
    fail "Bundled executable failed its --help self-test." \
        "Re-extract the ZIP and check that it matches this OS and architecture."
fi

version_dir=$install_dir/$version
current_link=$install_dir/current
command_link=$bin_dir/$product
staging_dir=$install_dir/.staging-$version-$$
staging_bundle=$staging_dir/$(basename "$bundle_dir")
backup_dir=$install_dir/.backup-$version-$$
previous_current=$install_dir/.current-previous-$$
temporary_command=$bin_dir/.$product-$$
temporary_current=

if [ -e "$version_dir" ] && [ "$update" != true ]; then
    fail "$product $version is already installed." \
        "Re-run ./install.sh --update to replace the installed version."
fi
if [ -L "$current_link" ] && [ "$update" != true ]; then
    fail "A $product installation is already active." \
        "Re-run ./install.sh --update to switch the current installation."
fi
if [ -e "$current_link" ] && [ ! -L "$current_link" ]; then
    fail "Current installation path exists but is not a symbolic link: $current_link" \
        "Inspect or move that path before running the installer again."
fi
if [ -e "$command_link" ] || [ -L "$command_link" ]; then
    if [ ! -L "$command_link" ] ||
        [ "$(readlink "$command_link")" != "$current_link/dist/$launcher" ]; then
        fail "A different command already exists at $command_link." \
            "Choose another command directory with --bin-dir or move that exact conflicting path."
    fi
fi
mkdir -p "$install_dir" || fail "Cannot create installation dir: $install_dir" \
    "Choose a writable path with --install-dir."

rollback_needed=false
new_version_installed=false
current_switched=false
current_was_present=false
cleanup() {
    rm -rf "$staging_dir"
    rm -f "$temporary_command"
    if [ -n "$temporary_current" ]; then
        rm -f "$temporary_current"
    fi
    if [ "$rollback_needed" = true ]; then
        if [ "$new_version_installed" = true ] && [ -e "$version_dir" ]; then
            rm -rf "$version_dir"
        fi
        if [ -e "$backup_dir" ]; then
            mv "$backup_dir" "$version_dir"
        fi
        if [ "$current_switched" = true ] && [ -L "$current_link" ]; then
            rm -f "$current_link"
        fi
        if [ "$current_was_present" = true ] && [ -L "$previous_current" ]; then
            mv "$previous_current" "$current_link"
        fi
    else
        rm -f "$previous_current"
        rm -rf "$backup_dir"
    fi
}
trap cleanup EXIT HUP INT TERM

printf '[4/5] Staging and validating the installation...\n'
mkdir "$staging_dir" || fail "Cannot create staging directory: $staging_dir" \
    "Check free disk space and write permissions."
cp -R "$bundle_dir" "$staging_dir/" \
    || fail "Could not copy the bundle into the installation directory." \
        "Check free disk space and permissions, then retry."
chmod +x "$staging_bundle/dist/$launcher" \
    || fail "Could not set executable permissions on the staged launcher." \
        "Use a filesystem that supports executable permissions."
if ! help_output=$("$staging_bundle/dist/$launcher" --help 2>&1) ||
    ! printf '%s\n' "$help_output" | grep -q "Usage: $product"; then
    fail "The staged executable failed its --help self-test." \
        "Check the copied files and available disk space, then retry."
fi

printf '[5/5] Activating %s and creating the command link...\n' "$product"
if [ -e "$version_dir" ]; then
    mv "$version_dir" "$backup_dir" \
        || fail "Could not preserve the previous version before update." \
            "Check whether files are in use and verify directory permissions."
    rollback_needed=true
fi
mv "$staging_bundle" "$version_dir" \
    || fail "Could not activate the staged version." \
        "Check filesystem permissions and available disk space."
new_version_installed=true
rollback_needed=true
temporary_current=$install_dir/.current-$$
ln -s "$version_dir" "$temporary_current" \
    || fail "Could not create the current-version symbolic link." \
        "Use a filesystem that supports symbolic links."
if [ -L "$current_link" ]; then
    mv "$current_link" "$previous_current" \
        || fail "Could not preserve the current-version link." \
            "Close programs using the application and retry."
    rollback_needed=true
    current_was_present=true
fi
mv "$temporary_current" "$current_link" \
    || fail "Could not switch the current-version link." \
        "Check installation directory permissions."
current_switched=true
ln -s "$current_link/dist/$launcher" "$temporary_command" \
    || fail "Could not create the command link." \
        "Choose a writable command directory with --bin-dir."
mv -f "$temporary_command" "$command_link" \
    || fail "Could not install the $product command link at $command_link." \
        "Choose a writable command directory with --bin-dir."
rollback_needed=false
rm -f "$previous_current"
rm -rf "$backup_dir"
trap - EXIT HUP INT TERM
cleanup

printf 'Installed %s %s.\n' "$product" "$version"
case ":${PATH:-}:" in
    *":$bin_dir:"*) ;;
    *)
        printf "Add this directory to your PATH to run '%s' in new terminals:\n  %s\n" "$product" "$bin_dir"
        case "$(basename "${SHELL:-sh}")" in
            zsh) printf 'For zsh, add to ~/.zshrc:\n  export PATH="%s:$PATH"\n' "$bin_dir" ;;
            bash) printf 'For bash, add to ~/.bashrc or ~/.profile:\n  export PATH="%s:$PATH"\n' "$bin_dir" ;;
        esac
        ;;
esac
printf 'Test with: "%s" --help\n' "$command_link"
