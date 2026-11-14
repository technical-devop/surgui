# Build pinned Surge v0.12.4 for Windows x64 with the loopback-only bind patch,
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

$Version = (Get-Content "$PSScriptRoot\..\third-party\surge\SURGE_VERSION" -Raw).Trim()
$ExpectedSha = (Get-Content "$PSScriptRoot\..\third-party\surge\SURGE_SHA256" -Raw).Trim().ToLowerInvariant()
$CodeloadUrl = "https://codeload.github.com/SurgeDM/Surge/tar.gz/refs/tags/$Version"

$RepoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$Stage = Join-Path $RepoRoot '.surge-stage'
$Patch = Join-Path $RepoRoot 'third-party\surge\surge-bind-loopback.patch'
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
$SrcDir = Join-Path $SrcParent 'Surge-0.12.4'
$SandboxTmp = Join-Path $RunDir 'sandbox-tmp'
$GoTmp = Join-Path $RunDir 'gotmp'
$GoCache = Join-Path $RunDir 'gocache'
$GoModCache = Join-Path $RunDir 'gomodcache'
$GoPathDir = Join-Path $RunDir 'gopath'
$RunPrefix = $RunDir + [System.IO.Path]::DirectorySeparatorChar

# Never overwrite preexisting data: every per-run path must be absent
# (a fresh GUID dir guarantees this; presence means a bug or collision).
foreach ($p in @($Archive, $SrcParent, $SrcDir, $SandboxTmp, $GoTmp, $GoCache, $GoModCache, $GoPathDir)) {
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
  & git apply --check --verbose $Patch
  if ($LASTEXITCODE -ne 0) { throw "git apply --check failed with exit code $LASTEXITCODE" }
  & git apply --verbose $Patch
  if ($LASTEXITCODE -ne 0) { throw "git apply failed with exit code $LASTEXITCODE" }
} finally {
  Pop-Location
}

Write-Host 'Verifying patched bind host...'
$HttpServerFile = Join-Path $SrcDir 'cmd\root_http_server.go'
$PatchedLine = 'const serverBindHost = "127.0.0.1"'
$OldLine = 'const serverBindHost = "0.0.0.0"'
$PatchedMatches = @(Select-String -Path $HttpServerFile -Pattern $PatchedLine -SimpleMatch)
if ($PatchedMatches.Count -ne 1) {
  throw "Expected exactly 1 occurrence of the patched bind host line in $HttpServerFile, found $($PatchedMatches.Count). Refusing to build."
}
$OldMatches = @(Select-String -Path $HttpServerFile -Pattern $OldLine -SimpleMatch)
if ($OldMatches.Count -ne 0) {
  throw "Expected 0 occurrences of the unpatched bind host line in $HttpServerFile, found $($OldMatches.Count). Refusing to build."
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
  'func TestSurguiManaged_TokenNotPersisted'
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
$GofmtOut = & gofmt -l $HttpServerFile $CmdTestFile 2>&1
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
  $env:GOFLAGS = '-trimpath'
  $env:TMPDIR = $SandboxTmp
  $env:TMP = $SandboxTmp
  $env:TEMP = $SandboxTmp
  $env:GOTMPDIR = $GoTmp
  $env:GOCACHE = $GoCache
  $env:GOMODCACHE = $GoModCache
  $env:GOPATH = $GoPathDir
  & go test -p 1 ./cmd/ -run 'TestSurguiManaged|TestFindAvailablePort|TestStartHTTPServer' -count=1 -v
  if ($LASTEXITCODE -ne 0) { throw "go test failed with exit code $LASTEXITCODE" }
} finally {
  Pop-Location
}

Write-Host 'Compiling Surge for windows/amd64 (reproducible flags)...'
Push-Location $SrcDir
try {
  $env:CGO_ENABLED = '0'
  $env:GOTOOLCHAIN = 'local'
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
