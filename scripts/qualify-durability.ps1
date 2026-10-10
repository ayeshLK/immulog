[CmdletBinding()]
param(
	[string]$OutputDirectory,
	[switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Show-Usage {
	@"
Usage: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\qualify-durability.ps1 [-OutputDirectory PATH]

Run the native durability evidence suite on Windows and create an upload-ready
.zip archive. No administrator privileges are required.

Options:
  -OutputDirectory PATH  New or empty evidence directory. It should be on the
                         filesystem being evaluated. The default is a
                         timestamped directory under the current user's home.
  -Help                  Show this help.

The runner refuses a dirty Git worktree. It uses an isolated temporary
directory beneath the evidence directory and never opens an application data
directory. The tests cover deterministic fault injection and process crashes,
not physical power loss.
"@
}

if ($Help) {
	Show-Usage
	exit 0
}

foreach ($Command in @("git", "go")) {
	if ($null -eq (Get-Command $Command -ErrorAction SilentlyContinue)) {
		throw "Required command is not available: $Command"
	}
}

$RepositoryRoot = (& git rev-parse --show-toplevel 2>$null)
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($RepositoryRoot)) {
	throw "Run this script from an immulog clone."
}
$RepositoryRoot = $RepositoryRoot.Trim()
Set-Location $RepositoryRoot

$WorktreeStatus = (& git status --porcelain)
if ($LASTEXITCODE -ne 0) {
	throw "Could not inspect the Git worktree."
}
if ($WorktreeStatus) {
	[Console]::Error.WriteLine("The worktree must be clean before qualification.")
	& git status --short
	exit 2
}

$GoVersion = (& go env GOVERSION)
if ($LASTEXITCODE -ne 0 -or $GoVersion -notmatch '^go1\.(25|2[6-9]|[3-9][0-9])(\..*)?$') {
	throw "Go 1.25 or newer is required; detected: $GoVersion"
}

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
	$OutputDirectory = Join-Path $HOME ("immulog-qualification-" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss"))
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path $OutputDirectory) {
	if (@(Get-ChildItem -Force $OutputDirectory).Count -ne 0) {
		throw "Output directory is not empty: $OutputDirectory"
	}
} else {
	New-Item -ItemType Directory -Path $OutputDirectory | Out-Null
}

$TestTemp = Join-Path $OutputDirectory "test-tmp"
New-Item -ItemType Directory -Force -Path $TestTemp | Out-Null
$env:TEMP = $TestTemp
$env:TMP = $TestTemp

$EnvironmentFile = Join-Path $OutputDirectory "environment.txt"
$CommandsFile = Join-Path $OutputDirectory "commands.txt"
$SummaryFile = Join-Path $OutputDirectory "summary.tsv"
"step`tstatus`tlog" | Set-Content -Encoding utf8 $SummaryFile
New-Item -ItemType File -Force -Path $CommandsFile | Out-Null

$EnvironmentLines = @(
	"captured_at_utc=$((Get-Date).ToUniversalTime().ToString('o'))",
	"repository=$RepositoryRoot",
	"commit=$(& git rev-parse HEAD)",
	"branch=$(& git branch --show-current)",
	"worktree_status=clean",
	"evidence_directory=$OutputDirectory",
	"test_temporary_directory=$TestTemp",
	"",
	"go_version:",
	((& go version) -join [Environment]::NewLine),
	"",
	"go_environment:",
	((& go env GOOS GOARCH CGO_ENABLED) -join [Environment]::NewLine),
	"",
	"windows:",
	((Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion, OsBuildNumber, OsArchitecture | Format-List | Out-String).Trim()),
	"",
	"host:",
	((Get-CimInstance Win32_ComputerSystem | Select-Object Manufacturer, Model, HypervisorPresent | Format-List | Out-String).Trim())
)
try {
	$DriveLetter = (Get-Item $TestTemp).PSDrive.Name
	$EnvironmentLines += ""
	$EnvironmentLines += "volume:"
	$EnvironmentLines += (Get-Volume -DriveLetter $DriveLetter | Select-Object DriveLetter, FileSystem, FileSystemLabel, DriveType, HealthStatus, Size, SizeRemaining | Format-List | Out-String).Trim()
} catch {
	$EnvironmentLines += "volume_capture_error=$($_.Exception.Message)"
}
$EnvironmentLines | Set-Content -Encoding utf8 $EnvironmentFile

$AnyFailed = $false
function Invoke-NativeStep {
	param(
		[string]$Name,
		[string]$Command,
		[string[]]$Arguments
	)
	$LogFile = Join-Path $OutputDirectory "$Name.log"
	("{0}: {1} {2}" -f $Name, $Command, ($Arguments -join " ")) | Add-Content -Encoding utf8 $CommandsFile
	Write-Host "`n=== $Name ==="
	& $Command @Arguments 2>&1 | Tee-Object -FilePath $LogFile
	$Status = $LASTEXITCODE
	("{0}`t{1}`t{2}" -f $Name, $Status, (Split-Path -Leaf $LogFile)) | Add-Content -Encoding utf8 $SummaryFile
	if ($Status -ne 0) {
		$script:AnyFailed = $true
	}
}

Invoke-NativeStep "storage-shuffled" "go" @("test", "./storage", "-shuffle=on", "-count=1")
Invoke-NativeStep "storage-race" "go" @("test", "-race", "./storage", "-count=1")
Invoke-NativeStep "crash-recovery" "go" @("test", "./storage", "-run", "Test(ProcessCrashRecovery|PersistenceBoundaryCrashRecovery)$", "-count=10", "-v")
Invoke-NativeStep "all-packages" "go" @("test", "./...", "-count=1")
Invoke-NativeStep "vet" "go" @("vet", "./...")

"completed_at_utc=$((Get-Date).ToUniversalTime().ToString('o'))" | Add-Content -Encoding utf8 $EnvironmentFile
$ArchivePath = "$($OutputDirectory.TrimEnd([System.IO.Path]::DirectorySeparatorChar)).zip"
if (Test-Path $ArchivePath) {
	Remove-Item -Force $ArchivePath
}
Compress-Archive -Path (Join-Path $OutputDirectory "*") -DestinationPath $ArchivePath
(Get-FileHash -Algorithm SHA256 $ArchivePath | Format-List | Out-String).Trim() | Set-Content -Encoding utf8 "$ArchivePath.sha256"

Write-Host "`nEvidence directory: $OutputDirectory"
Write-Host "Upload-ready archive: $ArchivePath"
Write-Host "Review the archive for private information before attaching it to issue #67."
if ($AnyFailed) {
	[Console]::Error.WriteLine("Qualification completed with one or more failures; preserve the archive and report the first failure.")
	exit 1
}
Write-Host "Qualification commands passed. Maintainer review is still required before changing platform claims."
