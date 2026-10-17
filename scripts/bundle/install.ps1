[CmdletBinding()]
param(
    [string]$InstallDir,
    [switch]$Update
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Stop-WithRecommendation([string]$Message, [string]$Recommendation) {
    throw "$Message`nRecommendation: $Recommendation"
}

function Test-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-DirectoryWritable([string]$Path) {
    if (Test-Path -LiteralPath $Path -PathType Leaf) {
        Stop-WithRecommendation "An installation directory path is a file: $Path" `
            "Choose a directory path with -InstallDir."
    }
    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
    $probe = Join-Path $Path (".tourillon-write-test-" + [Guid]::NewGuid())
    try {
        [IO.File]::WriteAllText($probe, "test")
    } finally {
        if (Test-Path -LiteralPath $probe) {
            Remove-Item -LiteralPath $probe -Force
        }
    }
}

function Remove-Junction([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    & $env:ComSpec /d /c "rmdir `"$Path`"" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Stop-WithRecommendation "Could not remove junction: $Path" `
            "Close programs using the application and check installation-directory permissions."
    }
}

function Add-UserPathEntry([string]$Path) {
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $entries = @($userPath -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $present = $false
    foreach ($entry in $entries) {
        if ([string]::Equals(
            $entry.TrimEnd("\"),
            $Path.TrimEnd("\"),
            [StringComparison]::OrdinalIgnoreCase
        )) {
            $present = $true
            break
        }
    }
    if (-not $present) {
        $updatedPath = (@($entries) + $Path) -join ";"
        if ($updatedPath.Length -gt 30000) {
            Write-Warning "The user PATH is too long to update automatically. Add this directory manually: $Path"
            return
        }
        try {
            [Environment]::SetEnvironmentVariable("Path", $updatedPath, "User")
        } catch {
            Write-Warning "Could not update the user PATH. Add this directory manually: $Path ($($_.Exception.Message))"
            return
        }
    }
    $processEntries = @($env:PATH -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if (-not ($processEntries | Where-Object {
        [string]::Equals($_.TrimEnd("\"), $Path.TrimEnd("\"), [StringComparison]::OrdinalIgnoreCase)
    })) {
        $env:PATH = (@($processEntries) + $Path) -join ";"
    }
}

try {
    $phase = "preflight"
    if ($env:OS -ne "Windows_NT") {
        Stop-WithRecommendation "This installer only supports Windows." `
            "Run install.ps1 from Windows PowerShell or PowerShell 7 on Windows."
    }
    if ($PSVersionTable.PSVersion -lt [Version]"5.1") {
        Stop-WithRecommendation "PowerShell 5.1 or newer is required." `
            "Upgrade Windows PowerShell or install PowerShell 7."
    }

    $bundleDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $distDir = Join-Path $bundleDir "dist"
    $metadataPath = Join-Path $distDir "PRODUCT_RUNTIME_INFO.json"
    foreach ($required in @($metadataPath, (Join-Path $bundleDir "THIRD_PARTY_LICENSES"))) {
        if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
            Stop-WithRecommendation "Required bundle file is missing: $required" `
                "Re-download and extract the complete product ZIP before installing."
        }
    }

    try {
        $runtime = Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json
    } catch {
        Stop-WithRecommendation "Runtime metadata is not valid JSON: $($_.Exception.Message)" `
            "Re-download and extract the complete product ZIP."
    }
    $version = [string]$runtime.version
    if ($version -cnotmatch '^[0-9]+\.[0-9]+\.[0-9]+(\.dev[0-9]+)?([-+][0-9A-Za-z.-]+)?$') {
        Stop-WithRecommendation "Invalid product version in $metadataPath." `
            "Use an unmodified release ZIP from the official release."
    }
    if ($runtime.platform -ne "windows" -or $runtime.architecture -notin @("arm64", "x86_64")) {
        Stop-WithRecommendation "The bundle has unsupported platform metadata." `
            "Download the Windows bundle matching your computer architecture."
    }
    $product = [string]$runtime.product
    if ($product -notin @("tourillon", "tourctl")) {
        Stop-WithRecommendation "Runtime metadata references an unsupported product: $product" `
            "Re-download and extract the complete product ZIP."
    }
    $commandName = $product
    $displayName = if ($product -eq "tourillon") { "Tourillon" } else { "Tourctl" }
    if ($runtime.launcher -ne "$product.exe" -or $runtime.python -notmatch '^3\.14\.') {
        Stop-WithRecommendation "Runtime metadata references an unexpected launcher: $($runtime.launcher)" `
            "Re-download and extract the complete CPython 3.14 product ZIP."
    }
    if ([string]::IsNullOrWhiteSpace($InstallDir)) {
        if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
            Stop-WithRecommendation "The per-user application directory could not be determined." `
                "Pass a writable -InstallDir explicitly."
        }
        $InstallDir = Join-Path $env:LOCALAPPDATA "Programs\$displayName"
    }
    $sourceLauncher = Join-Path $distDir $runtime.launcher
    if (-not (Test-Path -LiteralPath $sourceLauncher -PathType Leaf)) {
        Stop-WithRecommendation "The native launcher is missing: $sourceLauncher" `
            "Re-download and extract the complete product ZIP."
    }
    $nativeArchitecture = $env:PROCESSOR_ARCHITEW6432
    if ([string]::IsNullOrWhiteSpace($nativeArchitecture)) {
        $nativeArchitecture = $env:PROCESSOR_ARCHITECTURE
    }
    if ($runtime.architecture -eq "arm64" -and $nativeArchitecture -ne "ARM64") {
        Stop-WithRecommendation "This ARM64 bundle does not match this Windows architecture." `
            "Download the x86_64 bundle for this computer."
    }
    if ($runtime.architecture -eq "x86_64" -and $nativeArchitecture -eq "ARM64") {
        Write-Warning "The x86_64 bundle will run under emulation on Windows ARM64; use the ARM64 bundle for native performance."
    }
    if (-not (Test-Administrator) -and
        -not [string]::IsNullOrWhiteSpace($env:ProgramFiles) -and
        $InstallDir.StartsWith($env:ProgramFiles, [StringComparison]::OrdinalIgnoreCase)) {
        Stop-WithRecommendation "Administrator permissions are required to install under $env:ProgramFiles." `
            "Use the default per-user installation under LOCALAPPDATA or pass a user-writable -InstallDir."
    }

    Write-Host "[1/5] Checking the downloaded bundle..."
    $help = & $sourceLauncher --help 2>&1
    if ($LASTEXITCODE -ne 0 -or ($help -join "`n") -notmatch "Usage: $commandName") {
        Stop-WithRecommendation "The bundled launcher failed its --help self-test." `
            "Extract the ZIP again and check Windows security software or missing files."
    }

    Write-Host "[2/5] Checking installation directory permissions..."
    Test-DirectoryWritable $InstallDir
    $versionDir = Join-Path $installDir $version
    $currentDir = Join-Path $installDir "current"
    if ((Test-Path -LiteralPath $versionDir) -and -not $Update) {
        Stop-WithRecommendation "$displayName $version is already installed." `
            "Rerun with -Update to replace this installation."
    }
    if ((Test-Path -LiteralPath $currentDir) -and -not $Update) {
        Stop-WithRecommendation "A $displayName installation already exists." `
            "Rerun with -Update to switch the current installation."
    }
    if ((Test-Path -LiteralPath $currentDir) -and
        -not ((Get-Item -LiteralPath $currentDir -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        Stop-WithRecommendation "The current-version path exists but is not a junction: $currentDir" `
            "Move that path aside manually, then rerun the installer."
    }

    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
    $stagingDir = Join-Path $installDir (".staging-$version-$PID")
    $stagingBundle = Join-Path $stagingDir (Split-Path -Leaf $bundleDir)
    $backupDir = Join-Path $installDir (".backup-$version-$PID")
    if (Test-Path -LiteralPath $stagingDir) {
        Stop-WithRecommendation "A staging directory already exists: $stagingDir" `
            "Remove that exact stale staging directory and rerun the installer."
    }
    $movedOldVersion = $false
    try {
        $phase = "staging"
        Write-Host "[3/5] Copying and validating the versioned installation..."
        New-Item -ItemType Directory -Path $stagingDir -Force | Out-Null
        Copy-Item -LiteralPath $bundleDir -Destination $stagingDir -Recurse
        $stagedLauncher = Join-Path (Join-Path $stagingBundle "dist") $runtime.launcher
        $stagedHelp = & $stagedLauncher --help 2>&1
        if ($LASTEXITCODE -ne 0 -or ($stagedHelp -join "`n") -notmatch "Usage: $commandName") {
            Stop-WithRecommendation "The copied installation failed its --help self-test." `
                "Check available disk space and permissions, then rerun the installer."
        }
        if (Test-Path -LiteralPath $versionDir) {
            Move-Item -LiteralPath $versionDir -Destination $backupDir
            $movedOldVersion = $true
        }
        Move-Item -LiteralPath $stagingBundle -Destination $versionDir

        $phase = "activation"
        Write-Host "[4/5] Activating the installation..."
        $temporaryCurrent = Join-Path $installDir "current-$PID"
        Write-Host "  Creating temporary version junction..."
        cmd /d /c "mklink /J `"$temporaryCurrent`" `"$versionDir`"" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Stop-WithRecommendation "Could not create the current-version junction." `
                "Check permissions on $installDir and close any process using the installation."
        }
        $previousCurrent = Join-Path $installDir "current-previous-$PID"
        $movedCurrent = $false
        try {
            if (Test-Path -LiteralPath $currentDir) {
                Write-Host "  Replacing current version junction..."
                Move-Item -LiteralPath $currentDir -Destination $previousCurrent
                $movedCurrent = $true
            }
            Move-Item -LiteralPath $temporaryCurrent -Destination $currentDir
        } catch {
            if ($movedCurrent -and (Test-Path -LiteralPath $previousCurrent)) {
                Move-Item -LiteralPath $previousCurrent -Destination $currentDir -Force
            }
            throw
        }
        if (Test-Path -LiteralPath $previousCurrent) {
            Write-Host "  Removing previous version junction..."
            Remove-Junction $previousCurrent
        }
        $phase = "cleanup"
        if ($movedOldVersion) { Remove-Item -LiteralPath $backupDir -Recurse -Force }

        Write-Host "[5/5] Installed $displayName $version."
        $executableDir = Join-Path $currentDir "dist"
        Add-UserPathEntry $executableDir
        Write-Host "Test with: `"$executableDir\$($runtime.launcher)`" --help"
    } catch {
        if ($movedOldVersion -and (Test-Path -LiteralPath $backupDir) -and
            -not (Test-Path -LiteralPath $versionDir)) {
            Move-Item -LiteralPath $backupDir -Destination $versionDir
        }
        throw
    } finally {
        if (Test-Path -LiteralPath (Join-Path $installDir "current-$PID")) {
            Remove-Junction (Join-Path $installDir "current-$PID")
        }
        if (Test-Path -LiteralPath $stagingDir) {
            Remove-Item -LiteralPath $stagingDir -Recurse -Force
        }
    }
} catch {
    $message = $_.Exception.Message
    $message = "Installer failed during $phase. $message"
    if ($message -notmatch "Recommendation:") {
        $message += "`nRecommendation: Check free disk space and access permissions, "
        $message += "then rerun the installer from the extracted release ZIP."
    }
    Write-Error $message
    exit 1
}
