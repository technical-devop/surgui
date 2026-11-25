# Build pinned Surge v0.13.0 for Windows x64 with the loopback-only bind patch,
# then stage it as a Tauri resource.
#
# Groundwork only: fetch + verify + patch + compile + bundle. No runtime
# lifecycle, no GUI code, no daemon management, no network download at
# runtime.
#
# Rules enforced here:
# - Source comes ONLY from the official codeload archive for the pinned tag.
#   No `git clone` of Surge upstream.
# - SHA-256 of the archive is verified before extraction; any mismatch aborts.
# - The patch is applied with `git apply --check` first (strict, no fuzz);
#   any mismatch aborts (fail closed).
# - The patch must use traditional `--- a/...` / `+++ b/...` headers WITHOUT
#   `diff --git` lines: when this script runs `git apply` from the gitignored
#   staging subdir, hunks carrying `diff --git` headers are silently SKIPPED
#   (exit 0, nothing applied). The no-diff-git gate below fails the build if
#   that header ever reappears.
# - Post-apply content assertions verify the managed section is really
#   present (TLS-only, env-configured, no token persistence) and the managed
#   tests are present; then the relevant Go tests run. Any failure aborts.
# - A separate Windows startup diagnostic test runs after those tests. It is
#   nonfatal evidence only: stdout/stderr are discarded without buffering and
#   one allowlisted startup CI_DIAG marker plus one allowlisted preflight
#   CI_DIAG marker are read from small bounded marker files (go_unknown /
#   go_preflight_unknown on failure/timeout). Requires PowerShell 7 for Kill(true),
#   and that PS7 requirement applies only while the diagnostic is enabled.
#   The diagnostic is opt-in gated: it runs only when BOTH
#   SURGUI_CI_STARTUP_DIAGNOSTIC=1 and GITHUB_ACTIONS=true are set (set by the
#   GitHub workflow build step) and is intended for a GitHub-hosted workflow.
#   Without both variables the diagnostic is skipped entirely and this script
#   behaves exactly as it did before the probe existed. The probe is
#   comparison evidence only, not a claim of full isolation: process-tree
#   cleanup after a timeout is not independently verified, so cleanup remains
#   unverified until runner teardown.
# - All temporary files stay inside a unique, exclusively created per-run
#   directory under .\.surge-stage\ (gitignored), e.g.
#   .\.surge-stage\run-<guid>\. The script never deletes or overwrites any
#   preexisting file or directory under .\.surge-stage\; source, archive,
#   TMPDIR/TMP/TEMP, GOTMPDIR, GOCACHE, GOMODCACHE, and GOPATH all live
#   inside that per-run directory. Nothing is written to host default temp
#   or cache locations. The only intended write outside the per-run
#   directory is the final Tauri resource binary at $Out below.
# - Upstream MIT notice is verified byte-for-byte against the committed copy.
$ErrorActionPreference = 'Stop'

# Startup diagnostic opt-in: the probe is gated only by the explicit opt-in
# SURGUI_CI_STARTUP_DIAGNOSTIC=1 AND GITHUB_ACTIONS=true, both required, and
# is intended for a GitHub-hosted workflow. When either is absent the whole
# diagnostic (setup, copy, run, marker handling) is skipped below and the
# build keeps its original, probe-free behavior.
$DiagnosticEnabled = ([string]$env:SURGUI_CI_STARTUP_DIAGNOSTIC -ceq '1') -and ([string]$env:GITHUB_ACTIONS -ceq 'true')

# PowerShell 7 is required only when the diagnostic actually runs:
# process-tree termination uses Kill($true), which is unavailable on Windows
# PowerShell 5. The guard is conditional on $DiagnosticEnabled, so a
# diagnostic-disabled build never depends on it. Fail closed with a static
# message before any staging state is created.
if ($DiagnosticEnabled -and $PSVersionTable.PSVersion.Major -lt 7) {
  throw 'This script requires PowerShell 7 (pwsh) when the CI startup diagnostic is enabled. Refusing to build.'
}

$Version = (Get-Content "$PSScriptRoot\..\third-party\surge\SURGE_VERSION" -Raw).Trim()
$ExpectedSha = (Get-Content "$PSScriptRoot\..\third-party\surge\SURGE_SHA256" -Raw).Trim().ToLowerInvariant()
$CodeloadUrl = "https://codeload.github.com/SurgeDM/Surge/tar.gz/refs/tags/$Version"

$RepoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$Stage = Join-Path $RepoRoot '.surge-stage'
$Patch = Join-Path $RepoRoot 'third-party\surge\surge-bind-loopback.patch'
$DiagnosticTest = Join-Path $RepoRoot 'third-party\surge\windows-startup-diagnostic_test.go'
$Out = Join-Path $RepoRoot 'src-tauri\resources\surge-x86_64-pc-windows-msvc.exe'
$CommittedLicense = Join-Path $RepoRoot 'src-tauri\resources\Surge-LICENSE'

Write-Host "Surge pinned build: version=$Version"
Write-Host "Surge pinned build: codeload=$CodeloadUrl"

New-Item -ItemType Directory -Force -Path $Stage | Out-Null

# Narrow regression guard: fixed legacy staging paths must never be deleted
# or reused. This script contains no Remove-Item of staging content;
# per-run state lives only under a fresh, exclusively created $RunDir.
$ExpectedStage = Join-Path $RepoRoot '.surge-stage'
if ($Stage -ne $ExpectedStage) {
  throw "Staging dir escaped repo confinement: $Stage. Refusing to build."
}
# $Version is interpolated into a filename: fail closed on path separators.
if ($Version -notmatch '^[A-Za-z0-9._-]+$') {
  throw "Unsafe version for staging filename: $Version. Refusing to build."
}

# Exclusively create a unique per-run directory; never reuse or overwrite.
$RunDir = $null
$RunCreated = $false
for ($i = 0; $i -lt 10 -and -not $RunCreated; $i++) {
  $Candidate = Join-Path $Stage ('run-' + [System.Guid]::NewGuid().ToString('N'))
  if (Test-Path $Candidate) { continue }
  try {
    # No -Force: New-Item fails if the path already exists (exclusive).
    New-Item -ItemType Directory -Path $Candidate -ErrorAction Stop | Out-Null
    $RunDir = $Candidate
    $RunCreated = $true
  } catch {
    if (Test-Path $Candidate) { continue }
    throw
  }
}
if (-not $RunCreated) {
  throw 'Failed to create exclusive per-run staging directory. Refusing to build.'
}
# Confinement regression check: $RunDir must stay under $Stage.
$StagePrefix = $Stage + [System.IO.Path]::DirectorySeparatorChar
if (-not $RunDir.StartsWith($StagePrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "Per-run dir escaped staging confinement: $RunDir. Refusing to build."
}

$Archive = Join-Path $RunDir "surge-$Version.tar.gz"
$SrcParent = Join-Path $RunDir 'src'
$SrcDir = Join-Path $SrcParent ('Surge-' + $Version.TrimStart('v'))
$SandboxTmp = Join-Path $RunDir 'sandbox-tmp'
$GoTmp = Join-Path $RunDir 'gotmp'
$GoCache = Join-Path $RunDir 'gocache'
$GoModCache = Join-Path $RunDir 'gomodcache'
$GoPathDir = Join-Path $RunDir 'gopath'
$DiagnosticTestDest = Join-Path $SrcDir 'cmd\windows-startup-diagnostic_test.go'
$DiagnosticState = Join-Path $RunDir 'diagnostic-state'
$DiagAppData = Join-Path $DiagnosticState 'appdata'
$DiagLocalAppData = Join-Path $DiagnosticState 'localappdata'
$DiagUserProfile = Join-Path $DiagnosticState 'userprofile'
$DiagHome = Join-Path $DiagnosticState 'home'
$DiagTemp = Join-Path $DiagnosticState 'temp'
$DiagXdgConfig = Join-Path $DiagnosticState 'xdg-config'
$DiagXdgState = Join-Path $DiagnosticState 'xdg-state'
$DiagXdgData = Join-Path $DiagnosticState 'xdg-data'
$DiagXdgCache = Join-Path $DiagnosticState 'xdg-cache'
$DiagXdgRuntime = Join-Path $DiagnosticState 'xdg-runtime'
$DiagXdgConfigDirs = Join-Path $DiagnosticState 'xdg-config-dirs'
$DiagXdgDataDirs = Join-Path $DiagnosticState 'xdg-data-dirs'
$DiagnosticMarkerFile = Join-Path $RunDir 'diagnostic-marker.txt'
$DiagnosticPreflightFile = $DiagnosticMarkerFile + '.preflight'
# Separate sanitized numeric errno marker derived from the existing preflight
# marker path. Comparison only; the value is bounded to the fixed 16-bit range
# 0..65535. No raw error text is read or emitted.
$DiagnosticPreflightErrnoFile = $DiagnosticPreflightFile + '.errno'
$RunPrefix = $RunDir + [System.IO.Path]::DirectorySeparatorChar

# Never overwrite preexisting data: every per-run path must be absent
# (a fresh GUID dir guarantees this; presence means a bug or collision).
foreach ($p in @($Archive, $SrcParent, $SrcDir, $SandboxTmp, $GoTmp, $GoCache, $GoModCache, $GoPathDir, $DiagnosticTestDest, $DiagnosticState, $DiagAppData, $DiagLocalAppData, $DiagUserProfile, $DiagHome, $DiagTemp, $DiagXdgConfig, $DiagXdgState, $DiagXdgData, $DiagXdgCache, $DiagXdgRuntime, $DiagXdgConfigDirs, $DiagXdgDataDirs, $DiagnosticMarkerFile, $DiagnosticPreflightFile, $DiagnosticPreflightErrnoFile)) {
  if (-not $p.StartsWith($RunPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Per-run path escaped confinement: $p. Refusing to build."
  }
  if (Test-Path $p) {
    throw "Refusing to overwrite preexisting path: $p. Refusing to build."
  }
}

New-Item -ItemType Directory -Path $SrcParent | Out-Null
New-Item -ItemType Directory -Path $SandboxTmp | Out-Null
New-Item -ItemType Directory -Path $GoTmp | Out-Null
New-Item -ItemType Directory -Path $GoCache | Out-Null
New-Item -ItemType Directory -Path $GoModCache | Out-Null
New-Item -ItemType Directory -Path $GoPathDir | Out-Null
# Diagnostic-only setup: created only when the opt-in gate is open.
if ($DiagnosticEnabled) {
  New-Item -ItemType Directory -Path $DiagnosticState | Out-Null
  foreach ($d in @($DiagAppData, $DiagLocalAppData, $DiagUserProfile, $DiagHome, $DiagTemp, $DiagXdgConfig, $DiagXdgState, $DiagXdgData, $DiagXdgCache, $DiagXdgRuntime, $DiagXdgConfigDirs, $DiagXdgDataDirs)) {
    New-Item -ItemType Directory -Path $d | Out-Null
  }
}
# Confine child-process temp (curl/tar) and Go caches to the per-run dir so
# no host default temp/cache is touched.
$env:TMPDIR = $SandboxTmp
$env:TMP = $SandboxTmp
$env:TEMP = $SandboxTmp
$env:GOTMPDIR = $GoTmp
$env:GOCACHE = $GoCache
$env:GOMODCACHE = $GoModCache
$env:GOPATH = $GoPathDir

Write-Host 'Downloading pinned Surge source archive (in-repo staging only)...'
& curl.exe -sSL --fail --retry 3 --max-time 300 -o $Archive $CodeloadUrl
if ($LASTEXITCODE -ne 0) { throw "curl failed with exit code $LASTEXITCODE" }

Write-Host 'Verifying SHA-256 (fail closed on mismatch)...'
$ActualSha = (Get-FileHash -Path $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ActualSha -ne $ExpectedSha) {
  throw "SHA-256 mismatch for $Archive. Expected $ExpectedSha, got $ActualSha. Refusing to build."
}
Write-Host "SHA-256 OK: $ActualSha"

Write-Host 'Extracting archive...'
& tar -xzf $Archive -C $SrcParent
if ($LASTEXITCODE -ne 0) { throw "tar extract failed with exit code $LASTEXITCODE" }

if (-not (Test-Path (Join-Path $SrcDir 'go.mod'))) {
  throw "Expected source root not found: $SrcDir\go.mod"
}

Write-Host 'Verifying upstream MIT notice matches committed copy...'
$UpstreamLicense = Join-Path $SrcDir 'LICENSE'
$UpstreamBytes = [System.IO.File]::ReadAllBytes($UpstreamLicense)
$CommittedBytes = [System.IO.File]::ReadAllBytes($CommittedLicense)
if ($UpstreamBytes.Length -ne $CommittedBytes.Length) {
  throw 'src-tauri/resources/Surge-LICENSE differs from upstream LICENSE for ' + $Version + '. Refusing to build.'
}
for ($i = 0; $i -lt $UpstreamBytes.Length; $i++) {
  if ($UpstreamBytes[$i] -ne $CommittedBytes[$i]) {
    throw 'src-tauri/resources/Surge-LICENSE differs from upstream LICENSE for ' + $Version + ' (byte ' + $i + '). Refusing to build.'
  }
}

Write-Host 'Verifying patch has no diff --git headers (silently skipped under staging)...'
if (Select-String -Path $Patch -Pattern '^diff --git' -Quiet) {
  throw "Patch contains 'diff --git' headers, which git apply silently skips when run from the gitignored staging subdir. Use traditional headers only."
}

Write-Host 'Applying loopback patch strictly (fail closed on mismatch)...'
Push-Location $SrcDir
try {
  & git -c core.autocrlf=false apply --check --verbose $Patch
  if ($LASTEXITCODE -ne 0) { throw "git apply --check failed with exit code $LASTEXITCODE" }
  & git -c core.autocrlf=false apply --verbose $Patch
  if ($LASTEXITCODE -ne 0) { throw "git apply failed with exit code $LASTEXITCODE" }
} finally {
  Pop-Location
}

Write-Host 'Verifying patched bind host defaults to loopback...'
$HttpServerFile = Join-Path $SrcDir 'cmd\root_http_server.go'
$SettingsFile = Join-Path $SrcDir 'internal\config\settings.go'
$PatchedLine = 'DefaultBindHost = "127.0.0.1"'
$OldLine = 'DefaultBindHost = "0.0.0.0"'
$PatchedMatches = @(Select-String -Path $SettingsFile -Pattern $PatchedLine -SimpleMatch)
if ($PatchedMatches.Count -ne 1) {
  throw "Expected exactly 1 occurrence of the loopback DefaultBindHost line in $SettingsFile, found $($PatchedMatches.Count). Refusing to build."
}
$OldMatches = @(Select-String -Path $SettingsFile -Pattern $OldLine -SimpleMatch)
if ($OldMatches.Count -ne 0) {
  throw "Expected 0 occurrences of the upstream 0.0.0.0 bind default in $SettingsFile, found $($OldMatches.Count). Refusing to build."
}
if (-not (Select-String -Path $SettingsFile -Pattern 'if strings.TrimSpace\(sVal\) == LoopbackBindHost' -Quiet)) {
  throw "bind_host validator does not reject non-loopback addresses. Refusing to build."
}

Write-Host 'Verifying Surgui-managed section (TLS-only, env-configured)...'
$HttpLines = Get-Content $HttpServerFile
$MarkerIdx = -1
for ($i = 0; $i -lt $HttpLines.Count; $i++) {
  if ($HttpLines[$i] -match 'Surgui-managed Surge server \(TLS-only loopback') { $MarkerIdx = $i; break }
}
if ($MarkerIdx -lt 0) {
  throw "Managed section marker not found in $HttpServerFile (patch skipped?). Refusing to build."
}
$ManagedSection = $HttpLines[$MarkerIdx..($HttpLines.Count - 1)] -join "`n"
$ManagedCode = ($ManagedSection -split "`n" | Where-Object { $_ -notmatch '^\s*//' }) -join "`n"
$Required = @(
  'surguiManagedHost = "127.0.0.1"',
  'func surguiManagedConfigFromEnv',
  'func bindSurguiManagedListener',
  'func loadSurguiManagedCert',
  'func newSurguiManagedServer',
  'func runSurguiManagedServer',
  'surgui-managed-server',
  'ServeTLS',
  'X509KeyPair',
  'TLSConfig',
  'MinVersion',
  'SURGUI_PORT',
  'SURGUI_TLS_CERT_PEM',
  'SURGUI_TLS_KEY_PEM',
  'SURGUI_AUTH_TOKEN'
)
foreach ($r in $Required) {
  if ($ManagedSection -notmatch [regex]::Escape($r)) {
    throw "Managed section lacks required marker: $r. Refusing to build."
  }
}
# Fail closed on insecure fallbacks, file-based TLS, or token persistence:
# no plaintext Serve/ListenAndServe, no port scan, no token-file writes,
# no legacy token inputs, no TLS file paths, no file-based cert loader,
# no raw cert/key error details.
$Forbidden = @(
  '.Serve(',
  'ListenAndServe',
  'findAvailablePort',
  'bindServerListener',
  'persistAuthToken',
  'ensureAuthToken',
  'writeTokenToFile',
  'resolveServerToken',
  'resolveTokenPath',
  'WriteFile',
  'ReadFile',
  'LoadX509KeyPair',
  'SURGUI_TLS_CERT_FILE',
  'SURGUI_TLS_KEY_FILE',
  'certFile',
  'keyFile',
  'CERT_FILE',
  'KEY_FILE',
  'cert/key: %v',
  'SURGE_TOKEN'
)
foreach ($f in $Forbidden) {
  if ($ManagedCode -match [regex]::Escape($f)) {
    throw "Managed section contains forbidden marker: $f. Refusing to build."
  }
}

Write-Host 'Verifying managed tests are present in cmd_test.go...'
$CmdTestFile = Join-Path $SrcDir 'cmd\cmd_test.go'
$TestLines = Get-Content $CmdTestFile
$TestIdx = -1
for ($i = 0; $i -lt $TestLines.Count; $i++) {
  if ($TestLines[$i] -match 'Tests for the Surgui-managed server') { $TestIdx = $i; break }
}
if ($TestIdx -lt 0) {
  throw "Managed test section marker not found in $CmdTestFile (patch skipped?). Refusing to build."
}
$TestSection = $TestLines[$TestIdx..($TestLines.Count - 1)] -join "`n"
$RequiredTests = @(
  'func TestSurguiManaged_ConfigMissingFailsClosed',
  'func TestSurguiManaged_ConfigInvalidPortFailsClosed',
  'func TestSurguiManaged_ConfigBadTokenFailsClosed',
  'func TestSurguiManaged_ConfigValid',
  'func TestSurguiManaged_BindsLoopbackOnly',
  'func TestSurguiManaged_TLSListenerServesHTTPS',
  'func TestSurguiManaged_NoPlaintextFallback',
  'func TestSurguiManaged_PortCollisionFailsClosed',
  'func TestSurguiManaged_WrongCertFailsClosed',
  'func TestSurguiManaged_TokenNotPersisted',
  'func TestSurguiManaged_BindHostDefaultsToLoopback',
  'func TestSurguiManaged_DefaultServerListenerBindsLoopback',
  'func TestSurguiManaged_NonLoopbackBindHostFailsBeforeListen'
)
foreach ($t in $RequiredTests) {
  if ($TestSection -notmatch [regex]::Escape($t)) {
    throw "Managed test missing: $t. Refusing to build."
  }
}

Write-Host 'Verifying managed tests use in-memory PEM contract (no TLS files)...'
$RequiredPemTests = @(
  'SURGUI_TLS_CERT_PEM',
  'SURGUI_TLS_KEY_PEM',
  'makeSurguiTestCertPEM',
  'ServeTLS(ln, "", "")',
  'TLSConfig'
)
foreach ($r in $RequiredPemTests) {
  if ($TestSection -notmatch [regex]::Escape($r)) {
    throw "Managed tests lack required PEM marker: $r. Refusing to build."
  }
}
$ForbiddenPemTests = @(
  'SURGUI_TLS_CERT_FILE',
  'SURGUI_TLS_KEY_FILE',
  'LoadX509KeyPair',
  'writeSurguiTestCertFiles',
  'CERT_FILE',
  'KEY_FILE'
)
foreach ($f in $ForbiddenPemTests) {
  if ($TestSection -match [regex]::Escape($f)) {
    throw "Managed tests contain forbidden file-based TLS marker: $f. Refusing to build."
  }
}

Write-Host 'Checking gofmt on patched files...'
$GofmtOut = & gofmt -l $HttpServerFile $CmdTestFile $SettingsFile 2>&1
$GofmtCode = $LASTEXITCODE
if ($GofmtCode -ne 0) {
  throw "gofmt execution failed with exit code $GofmtCode : $GofmtOut. Refusing to build."
}
$GofmtText = (($GofmtOut | ForEach-Object { "$_".Trim() } | Where-Object { $_ -ne '' }) -join "`n").Trim()
if ($GofmtText -ne '') {
  throw "Patched file not gofmt-clean: $GofmtText. Refusing to build."
}

Write-Host 'Running managed + related Go tests (fail closed)...'
Push-Location $SrcDir
try {
  $env:CGO_ENABLED = '0'
  $env:GOTOOLCHAIN = 'local'
  $env:GOMAXPROCS = '2'
  $env:GOMEMLIMIT = '2GiB'
  $env:GOFLAGS = '-trimpath'
  $env:TMPDIR = $SandboxTmp
  $env:TMP = $SandboxTmp
  $env:TEMP = $SandboxTmp
  $env:GOTMPDIR = $GoTmp
  $env:GOCACHE = $GoCache
  $env:GOMODCACHE = $GoModCache
  $env:GOPATH = $GoPathDir
  & go test -p 1 ./cmd/ -run 'TestSurguiManaged|TestFindAvailablePort|TestStartHTTPServer|TestBindServerListener' -count=1 -v
  if ($LASTEXITCODE -ne 0) { throw "go test failed with exit code $LASTEXITCODE" }
} finally {
  Pop-Location
}

# ---------------------------------------------------------------------------
# Windows startup diagnostic (test-only, nonfatal evidence).
#
# Opt-in gate: the entire diagnostic below (state setup, test copy, process
# start, marker handling and its output) runs ONLY when the gate is open,
# which requires BOTH SURGUI_CI_STARTUP_DIAGNOSTIC=1 (set explicitly by the
# GitHub workflow build step) AND GITHUB_ACTIONS=true (set by the runner);
# the combination is intended for a GitHub-hosted workflow. When the gate is
# closed the block is skipped cleanly - no state dirs, no file copy, no child
# process,
# no marker line - and the build proceeds exactly as it did before the probe
# existed. The PowerShell 7 guard above is likewise conditional on this gate.
#
# This copies the standalone diagnostic test into the already patched source
# tree, runs it as a separate sequential go test invocation, discards all
# stdout/stderr without buffering or logging, and reads one allowlisted startup
# CI marker plus one allowlisted preflight CI marker from separate small
# bounded marker files. On any failure, timeout, missing marker, or ambiguous
# marker the wrapper prints only CI_DIAG:go_unknown (startup) or
# CI_DIAG:go_preflight_unknown (preflight). No raw test output, paths,
# environment, token, cert, or error text is ever emitted. Requires PowerShell
# 7 for Kill($true).
#
# Comparison evidence only, not a claim of full isolation: the synthetic
# per-run environment is a comparison baseline against production paths, not
# proof that nothing is written outside it. Process-tree cleanup after a
# timeout (Kill($true) plus bounded WaitForExit) is attempted but is NOT
# independently verified here, and runner teardown is the final cleanup
# boundary for anything the probe leaves behind.
#
# The diagnostic is intentionally nonfatal for now (evidence only). Existing
# build/test/packaging failures above and below remain fail-closed.
# ---------------------------------------------------------------------------
if ($DiagnosticEnabled) {
  $DiagnosticMarker = 'CI_DIAG:go_unknown'
  $DiagnosticAllowed = @(
    'CI_DIAG:go_startup_ok',
    'CI_DIAG:go_startup_config',
    'CI_DIAG:go_startup_cert',
    'CI_DIAG:go_startup_lock',
    'CI_DIAG:go_startup_engine',
    'CI_DIAG:go_startup_lifecycle',
    'CI_DIAG:go_startup_bind',
    'CI_DIAG:go_startup_bind_invalid',
    'CI_DIAG:go_startup_bind_addr_in_use',
    'CI_DIAG:go_startup_bind_access_denied',
    'CI_DIAG:go_startup_bind_other',
    'CI_DIAG:go_startup_bind_address_unavailable',
    'CI_DIAG:go_startup_bind_network_down',
    'CI_DIAG:go_startup_bind_address_family_unsupported',
    'CI_DIAG:go_startup_bind_invalid_argument',
    'CI_DIAG:go_startup_bind_socket_limit',
    'CI_DIAG:go_startup_bind_no_buffer_space',
    'CI_DIAG:go_startup_bind_winsock_not_ready',
    'CI_DIAG:go_startup_bind_winsock_version',
    'CI_DIAG:go_startup_bind_winsock_uninitialized',
    'CI_DIAG:go_startup_bind_other_syscall',
    'CI_DIAG:go_startup_bind_addr_error',
    'CI_DIAG:go_startup_bind_dns_error',
    'CI_DIAG:go_startup_bind_other_non_syscall',
    'CI_DIAG:go_startup_serve',
    'CI_DIAG:go_startup_other',
    'CI_DIAG:go_startup_not_ready'
  )
  $PreflightMarker = 'CI_DIAG:go_preflight_unknown'
  $DiagnosticPreflightAllowed = @(
    'CI_DIAG:go_preflight_free',
    'CI_DIAG:go_preflight_address_in_use',
    'CI_DIAG:go_preflight_access_denied',
    'CI_DIAG:go_preflight_address_unavailable',
    'CI_DIAG:go_preflight_network_down',
    'CI_DIAG:go_preflight_address_family_unsupported',
    'CI_DIAG:go_preflight_invalid_argument',
    'CI_DIAG:go_preflight_socket_limit',
    'CI_DIAG:go_preflight_no_buffer_space',
    'CI_DIAG:go_preflight_winsock_not_ready',
    'CI_DIAG:go_preflight_winsock_version',
    'CI_DIAG:go_preflight_winsock_uninitialized',
    'CI_DIAG:go_preflight_other_syscall',
    'CI_DIAG:go_preflight_addr_error',
    'CI_DIAG:go_preflight_dns_error',
    'CI_DIAG:go_preflight_other_non_syscall',
    'CI_DIAG:go_preflight_other'
  )
  # Sanitized bounded numeric OS errno evidence, comparison only. The value is
  # a canonical decimal within the fixed 16-bit range 0..65535; no raw error
  # text, paths, identity, auth, stdout, or stderr is emitted. This numeric
  # category describes the synthetic CI preflight only. Missing, invalid, or
  # failed reads report the fixed unknown.
  $PreflightWinErr = 'CI_DIAG:preflight_winerr_unknown'
  $proc = $null
  try {
    if (-not (Test-Path -LiteralPath $DiagnosticTest -PathType Leaf)) {
      throw 'diagnostic test source missing'
    }
    if (-not $DiagnosticTestDest.StartsWith($RunPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
      throw 'diagnostic test destination escaped per-run confinement'
    }
    if (Test-Path -LiteralPath $DiagnosticTestDest) {
      throw 'diagnostic test destination already exists'
    }
    if (-not $DiagnosticMarkerFile.StartsWith($RunPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
      throw 'diagnostic marker escaped per-run confinement'
    }
    if (Test-Path -LiteralPath $DiagnosticMarkerFile) {
      throw 'diagnostic marker already exists'
    }
    if (-not $DiagnosticPreflightFile.StartsWith($RunPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
      throw 'diagnostic preflight marker escaped per-run confinement'
    }
    if (Test-Path -LiteralPath $DiagnosticPreflightFile) {
      throw 'diagnostic preflight marker already exists'
    }
    if (-not $DiagnosticPreflightErrnoFile.StartsWith($RunPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
      throw 'diagnostic preflight errno marker escaped per-run confinement'
    }
    if (Test-Path -LiteralPath $DiagnosticPreflightErrnoFile) {
      throw 'diagnostic preflight errno marker already exists'
    }
    Copy-Item -LiteralPath $DiagnosticTest -Destination $DiagnosticTestDest -ErrorAction Stop

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'go'
    # No quoting is needed inside this argument string: none of these arguments
    # contain spaces, and ProcessStartInfo passes it directly without a shell.
    $psi.Arguments = 'test -p=1 -run=^(TestWindowsStartupDiagnostic|TestSurguiWindowsSocketErrorClassification)$ -count=1 -timeout=45s -v ./cmd/'
    $psi.WorkingDirectory = $SrcDir
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true

    # Start from an empty child environment so CI secrets are not inherited.
    # Only the explicit non-secret allowlist below is copied in.
    $UseEnvironmentDict = ($psi.PSObject.Properties.Name -contains 'Environment')
    if ($UseEnvironmentDict) { $psi.Environment.Clear() } else { $psi.EnvironmentVariables.Clear() }

    foreach ($n in @('PATH', 'SystemRoot', 'SystemDrive', 'windir', 'ComSpec', 'PATHEXT', 'GOROOT')) {
      $v = [System.Environment]::GetEnvironmentVariable($n, [System.EnvironmentVariableTarget]::Process)
      if (-not [string]::IsNullOrEmpty($v)) {
        if ($UseEnvironmentDict) { $psi.Environment[$n] = $v } else { $psi.EnvironmentVariables[$n] = $v }
      }
    }

    # Diagnostic-only confinement via env indirection: all XDG user-dir
    # variables plus the cached XDG_RUNTIME_DIR are set to isolated per-run
    # dirs BEFORE the test process starts, so the cached xdg.RuntimeDir value
    # is confined at startup. The Go test clears XDG_RUNTIME_DIR AFTER process
    # start to exercise fallback to that cached isolated value. Honest
    # limitation: on Windows the fallback may still resolve via the
    # KnownFolder API, which can touch the runner profile outside any
    # isolated tree. This diagnostic therefore does not claim exact
    # reproduction of production paths and cannot guarantee zero writes
    # outside synthetic state; it only confines what env indirection can.
    # This is a comparison baseline only; see the gate note above: cleanup
    # remains unverified until runner teardown. No full isolation is claimed.
    $DiagnosticEnv = @{
      'APPDATA'                                = $DiagAppData
      'LOCALAPPDATA'                           = $DiagLocalAppData
      'USERPROFILE'                            = $DiagUserProfile
      'HOME'                                   = $DiagHome
      'TEMP'                                   = $DiagTemp
      'TMP'                                    = $DiagTemp
      'TMPDIR'                                 = $DiagTemp
      'XDG_CONFIG_HOME'                        = $DiagXdgConfig
      'XDG_STATE_HOME'                         = $DiagXdgState
      'XDG_DATA_HOME'                          = $DiagXdgData
      'XDG_CACHE_HOME'                         = $DiagXdgCache
      'XDG_RUNTIME_DIR'                        = $DiagXdgRuntime
      'XDG_CONFIG_DIRS'                        = $DiagXdgConfigDirs
      'XDG_DATA_DIRS'                          = $DiagXdgDataDirs
      'SURGUI_WINDOWS_DIAGNOSTIC_MARKER_FILE'  = $DiagnosticMarkerFile
      'GOTMPDIR'                               = $GoTmp
      'GOCACHE'                                = $GoCache
      'GOMODCACHE'                             = $GoModCache
      'GOPATH'                                 = $GoPathDir
      'GOFLAGS'                                = '-trimpath'
      'GOTOOLCHAIN'                            = 'local'
      'GOPROXY'                                = 'off'
      'GOSUMDB'                                = 'off'
      'CGO_ENABLED'                            = '0'
      'GOMAXPROCS'                             = '2'
    }
    foreach ($k in $DiagnosticEnv.Keys) {
      $v = [string]$DiagnosticEnv[$k]
      if ($UseEnvironmentDict) { $psi.Environment[$k] = $v } else { $psi.EnvironmentVariables[$k] = $v }
    }
    # Diagnostic-only SystemRoot restore: TestMain replaces SystemRoot with a
    # temporary directory, and this test needs the correct Windows system root
    # for comparison. Populate the isolated child env from the actual
    # parent-process value via the .NET process env getter. The value is never
    # logged. If empty, the key stays unset and the Go test returns without a
    # marker, so the wrapper reports the fixed unknown literals below.
    $DiagnosticSystemRootValue = [System.Environment]::GetEnvironmentVariable('SystemRoot', [System.EnvironmentVariableTarget]::Process)
    if (-not [string]::IsNullOrEmpty($DiagnosticSystemRootValue)) {
      if ($UseEnvironmentDict) { $psi.Environment['SURGUI_DIAGNOSTIC_SYSTEM_ROOT'] = $DiagnosticSystemRootValue } else { $psi.EnvironmentVariables['SURGUI_DIAGNOSTIC_SYSTEM_ROOT'] = $DiagnosticSystemRootValue }
    }

    $proc = [System.Diagnostics.Process]::Start($psi)
    # Discard output without buffering: never retain or log raw bytes.
    $nullStream = [System.IO.Stream]::Null
    $stdoutDiscard = $proc.StandardOutput.BaseStream.CopyToAsync($nullStream)
    $stderrDiscard = $proc.StandardError.BaseStream.CopyToAsync($nullStream)
    $Exited = $proc.WaitForExit(60000)
    if (-not $Exited) {
      # Timeout: terminate the entire process tree (PowerShell 7). The Go test
      # binary spawns a child; Kill() without the tree flag would orphan it.
      # WaitForExit is bounded and its result is required; a failure to
      # terminate fails the build and never silently continues. This cleanup
      # path is not independently verified; runner teardown is the final
      # cleanup boundary.
      $Terminated = $false
      try {
        try {
          $proc.Kill($true)
        } catch {
          if (-not $proc.HasExited) { throw }
        }
        $Terminated = $proc.WaitForExit(5000)
      } catch {
        $Terminated = $false
      }
      if (-not $Terminated) {
        throw 'Failed to terminate diagnostic test process tree after timeout. Refusing to build.'
      }
    } else {
      # Bounded drain of the discard pumps; no output retained.
      try { $stdoutDiscard.Wait(5000) | Out-Null } catch { }
      try { $stderrDiscard.Wait(5000) | Out-Null } catch { }
    }

    # Small bounded preflight marker read: independent of the main marker
    # because it is written before engine startup. Accept one exact
    # allowlisted literal only; emit the canonical literal, never raw text.
    $FoundPreflightMarkers = @()
    if (Test-Path -LiteralPath $DiagnosticPreflightFile) {
      $preflightInfo = Get-Item -LiteralPath $DiagnosticPreflightFile
      if ($preflightInfo.Length -gt 0 -and $preflightInfo.Length -lt 256) {
        $preflightCandidate = ([System.IO.File]::ReadAllText($preflightInfo.FullName)).Trim()
        foreach ($allowedPreflightMarker in $DiagnosticPreflightAllowed) {
          if ([string]::Equals($allowedPreflightMarker, $preflightCandidate, [System.StringComparison]::Ordinal)) {
            $FoundPreflightMarkers += $allowedPreflightMarker
            break
          }
        }
      }
    }
    if ($FoundPreflightMarkers.Count -eq 1) {
      $PreflightMarker = $FoundPreflightMarkers[0]
    } else {
      $PreflightMarker = 'CI_DIAG:go_preflight_unknown'
    }

    # Small bounded numeric errno read, comparison only. Accept the separate
    # "<existing preflight marker>.errno" file ONLY on successful process
    # completion, and ONLY 1..5 ASCII digit bytes matching the fixed canonical
    # form. Re-derive the decimal output from the parsed invariant uint; never
    # echo candidate or arbitrary bytes. Fixed 16-bit bound: 0..65535.
    if ($Exited -and $proc.ExitCode -eq 0) {
      if (Test-Path -LiteralPath $DiagnosticPreflightErrnoFile -PathType Leaf) {
        $errnoInfo = Get-Item -LiteralPath $DiagnosticPreflightErrnoFile
        if ($errnoInfo.Length -ge 1 -and $errnoInfo.Length -le 5) {
          $errnoBytes = [System.IO.File]::ReadAllBytes($errnoInfo.FullName)
          $errnoDigitsOnly = $true
          foreach ($errnoByte in $errnoBytes) {
            if ($errnoByte -lt 0x30 -or $errnoByte -gt 0x39) {
              $errnoDigitsOnly = $false
              break
            }
          }
          if ($errnoDigitsOnly) {
            $errnoText = [System.Text.Encoding]::ASCII.GetString($errnoBytes)
            if ($errnoText -match '^(0|[1-9][0-9]{0,4})$') {
              $errnoValue = [uint32]0
              if ([uint32]::TryParse($errnoText, [System.Globalization.NumberStyles]::None, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$errnoValue)) {
                if ($errnoValue -le 65535) {
                  $PreflightWinErr = 'CI_DIAG:preflight_winerr_' + $errnoValue.ToString([System.Globalization.CultureInfo]::InvariantCulture)
                }
              }
            }
          }
        }
      }
    }

    $FoundMarkers = @()
    if ($Exited -and $proc.ExitCode -eq 0) {
      # Small bounded marker read: accept one exact allowlisted literal only.
      if (Test-Path -LiteralPath $DiagnosticMarkerFile) {
        $markerInfo = Get-Item -LiteralPath $DiagnosticMarkerFile
        if ($markerInfo.Length -gt 0 -and $markerInfo.Length -lt 256) {
          $candidate = ([System.IO.File]::ReadAllText($markerInfo.FullName)).Trim()
          # Ordinal, case-sensitive comparison against the allowlist. The
          # emitted value is always the canonical allowlist literal, never
          # the untrusted candidate text and never a raw error.
          foreach ($allowedMarker in $DiagnosticAllowed) {
            if ([string]::Equals($allowedMarker, $candidate, [System.StringComparison]::Ordinal)) {
              $FoundMarkers += $allowedMarker
              break
            }
          }
        }
      }
    }
    if ($FoundMarkers.Count -eq 1) {
      $DiagnosticMarker = $FoundMarkers[0]
    } else {
      $DiagnosticMarker = 'CI_DIAG:go_unknown'
    }
  } catch {
    $TerminateFailure = 'Failed to terminate diagnostic test process tree after timeout. Refusing to build.'
    # Never mask a termination failure as unknown evidence; fail the build.
    # Never emit marker paths or raw errors here.
    if ($_.Exception.Message -eq $TerminateFailure) {
      throw
    }
    if ($null -ne $proc) {
      try {
        if (-not $proc.HasExited) {
          try {
            $proc.Kill($true)
          } catch {
            if (-not $proc.HasExited) { throw $TerminateFailure }
          }
          if (-not $proc.WaitForExit(5000)) {
            throw $TerminateFailure
          }
        }
      } catch {
        # Cleanup failure must fail the build, never silently continue as unknown.
        throw $TerminateFailure
      }
    }
    $DiagnosticMarker = 'CI_DIAG:go_unknown'
    $PreflightWinErr = 'CI_DIAG:preflight_winerr_unknown'
  }
  Write-Host $DiagnosticMarker
  Write-Host $PreflightMarker
  Write-Host $PreflightWinErr
}

Write-Host 'Compiling Surge for windows/amd64 (reproducible flags)...'
Push-Location $SrcDir
try {
  $env:CGO_ENABLED = '0'
  $env:GOTOOLCHAIN = 'local'
  $env:GOMAXPROCS = '2'
  $env:GOMEMLIMIT = '2GiB'
  $env:GOFLAGS = '-trimpath'
  $env:GOOS = 'windows'
  $env:GOARCH = 'amd64'
  $env:TMPDIR = $SandboxTmp
  $env:TMP = $SandboxTmp
  $env:TEMP = $SandboxTmp
  $env:GOTMPDIR = $GoTmp
  $env:GOCACHE = $GoCache
  $env:GOMODCACHE = $GoModCache
  $env:GOPATH = $GoPathDir
  & go build -trimpath -ldflags="-s -w" -o $Out .
  if ($LASTEXITCODE -ne 0) { throw "go build failed with exit code $LASTEXITCODE" }
} finally {
  Pop-Location
}

Write-Host 'Checking Tauri resource destination...'
if (-not (Test-Path $Out)) { throw "Expected resource not found: $Out" }
if (-not (Test-Path $CommittedLicense)) { throw "Expected resource not found: $CommittedLicense" }
Get-Item $Out, $CommittedLicense | Format-Table Name, Length, FullName | Out-String | Write-Host

Write-Host 'OK: staged Tauri resource: src-tauri/resources/surge-x86_64-pc-windows-msvc.exe'
