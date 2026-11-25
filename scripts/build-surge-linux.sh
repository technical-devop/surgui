#!/usr/bin/env bash
#
# Build pinned Surge v0.13.0 for Linux x64 with the loopback-only bind patch,
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
#   directory under ./.surge-stage/ (gitignored), e.g.
#   ./.surge-stage/run-XXXXXX/. The script never deletes or overwrites any
#   preexisting file or directory under ./.surge-stage/; source, archive,
#   TMPDIR/TMP/TEMP, GOTMPDIR, GOCACHE, GOMODCACHE, and GOPATH all live
#   inside that per-run directory. Nothing is written to /tmp or other host
#   default cache locations. The only intended write outside the per-run
#   directory is the final Tauri resource binary at OUT below.
# - Upstream MIT notice is verified byte-for-byte against the committed copy.
set -euo pipefail

VERSION="$(tr -d ' \t\r\n' < "$(dirname "$0")/../third-party/surge/SURGE_VERSION")"
EXPECTED_SHA="$(tr -d ' \t\r\n' < "$(dirname "$0")/../third-party/surge/SURGE_SHA256")"
CODELOAD_URL="https://codeload.github.com/SurgeDM/Surge/tar.gz/refs/tags/${VERSION}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGE="$REPO_ROOT/.surge-stage"
PATCH="$REPO_ROOT/third-party/surge/surge-bind-loopback.patch"
OUT="$REPO_ROOT/src-tauri/resources/surge-x86_64-unknown-linux-gnu"
UPSTREAM_LICENSE_NAME="LICENSE"

echo "Surge pinned build: version=${VERSION}"
echo "Surge pinned build: codeload=${CODELOAD_URL}"

mkdir -p "$STAGE"

# Narrow regression guard: fixed legacy staging paths must never be deleted
# or reused. This script contains no `rm -rf`/`rm -f` of staging content;
# per-run state lives only under a fresh, exclusively created $RUN_DIR.
if [ "$STAGE" != "$REPO_ROOT/.surge-stage" ]; then
  echo "ERROR: staging dir escaped repo confinement: $STAGE" >&2
  exit 1
fi
# VERSION is interpolated into a filename: fail closed on path separators.
if [[ "$VERSION" =~ [^A-Za-z0-9._-] ]]; then
  echo "ERROR: unsafe VERSION for staging filename: $VERSION" >&2
  exit 1
fi

# Exclusively create a unique per-run directory; never reuse or overwrite.
RUN_DIR="$(mktemp -d "$STAGE/run-XXXXXX")"
# Confinement regression check: RUN_DIR must stay under $STAGE.
case "$RUN_DIR" in
  "$STAGE"/run-*) ;;
  *) echo "ERROR: per-run dir escaped staging confinement: $RUN_DIR" >&2; exit 1 ;;
esac

ARCHIVE="$RUN_DIR/surge-${VERSION}.tar.gz"
SRC_PARENT="$RUN_DIR/src"
SRC_DIR="$SRC_PARENT/Surge-${VERSION#v}"
RUN_TMP="$RUN_DIR/sandbox-tmp"
RUN_GOTMP="$RUN_DIR/gotmp"
RUN_GOCACHE="$RUN_DIR/gocache"
RUN_GOMODCACHE="$RUN_DIR/gomodcache"
RUN_GOPATH="$RUN_DIR/gopath"

# Never overwrite preexisting data: every per-run path must be absent
# (mktemp guarantees a fresh $RUN_DIR, so presence means a bug or collision).
for p in "$ARCHIVE" "$SRC_PARENT" "$SRC_DIR" "$RUN_TMP" "$RUN_GOTMP" "$RUN_GOCACHE" "$RUN_GOMODCACHE" "$RUN_GOPATH"; do
  case "$p" in
    "$RUN_DIR"/*) ;;
    *) echo "ERROR: per-run path escaped confinement: $p" >&2; exit 1 ;;
  esac
  if [ -e "$p" ]; then
    echo "ERROR: refusing to overwrite preexisting path: $p" >&2
    exit 1
  fi
done

mkdir -p "$SRC_PARENT" "$RUN_TMP" "$RUN_GOTMP" "$RUN_GOCACHE" "$RUN_GOMODCACHE" "$RUN_GOPATH"
# Confine child-process temp (curl/tar/sha256sum) to the per-run dir.
export TMPDIR="$RUN_TMP" TMP="$RUN_TMP" TEMP="$RUN_TMP" GOTMPDIR="$RUN_GOTMP"
# Confine Go caches to the per-run dir so no host default cache is touched.
export GOCACHE="$RUN_GOCACHE" GOMODCACHE="$RUN_GOMODCACHE" GOPATH="$RUN_GOPATH"

echo "Downloading pinned Surge source archive (in-repo staging only)..."
curl -sSL --fail --retry 3 --max-time 300 -o "$ARCHIVE" "$CODELOAD_URL"

echo "Verifying SHA-256 (fail closed on mismatch)..."
echo "${EXPECTED_SHA}  ${ARCHIVE}" | sha256sum -c -

echo "Extracting archive..."
tar -xzf "$ARCHIVE" -C "$SRC_PARENT"

if [ ! -f "$SRC_DIR/go.mod" ]; then
  echo "ERROR: expected source root not found: $SRC_DIR/go.mod" >&2
  exit 1
fi

echo "Verifying upstream MIT notice matches committed copy..."
if ! cmp -s "$SRC_DIR/$UPSTREAM_LICENSE_NAME" "$REPO_ROOT/src-tauri/resources/Surge-LICENSE"; then
  echo "ERROR: src-tauri/resources/Surge-LICENSE differs from upstream $UPSTREAM_LICENSE_NAME for ${VERSION}." >&2
  echo "Refusing to build: re-copy the upstream file verbatim instead of editing it." >&2
  exit 1
fi

echo "Verifying patch has no diff --git headers (silently skipped under staging)..."
if grep -q '^diff --git' "$PATCH"; then
  echo "ERROR: patch contains 'diff --git' headers, which git apply silently skips" >&2
  echo "when run from the gitignored staging subdir. Use traditional headers only." >&2
  exit 1
fi

echo "Applying loopback patch strictly (fail closed on mismatch)..."
(
  cd "$SRC_DIR"
  git apply --check --verbose "$PATCH"
  git apply --verbose "$PATCH"
)

echo "Verifying patched bind host defaults to loopback..."
HTTP_SERVER_GO="$SRC_DIR/cmd/root_http_server.go"
SETTINGS_GO="$SRC_DIR/internal/config/settings.go"
DEFAULT_COUNT="$(grep -Fc 'DefaultBindHost = "127.0.0.1"' "$SETTINGS_GO" || true)"
DEFAULT_COUNT="${DEFAULT_COUNT:-0}"
if [ "$DEFAULT_COUNT" -ne 1 ]; then
  echo "ERROR: expected exactly 1 loopback DefaultBindHost line, found ${DEFAULT_COUNT}." >&2
  exit 1
fi
OLD_COUNT="$(grep -Fc 'DefaultBindHost = "0.0.0.0"' "$SETTINGS_GO" || true)"
OLD_COUNT="${OLD_COUNT:-0}"
if [ "$OLD_COUNT" -ne 0 ]; then
  echo "ERROR: upstream 0.0.0.0 bind default still present after patch (${OLD_COUNT} occurrence(s))." >&2
  exit 1
fi
if ! grep -Fq 'if strings.TrimSpace(sVal) == LoopbackBindHost' "$SETTINGS_GO"; then
  echo "ERROR: bind_host validator does not reject non-loopback addresses." >&2
  exit 1
fi

echo "Verifying Surgui-managed section (TLS-only, env-configured)..."
MANAGED_SECTION="$(sed -n '/Surgui-managed Surge server (TLS-only loopback/,$p' "$HTTP_SERVER_GO")"
if [ -z "$MANAGED_SECTION" ]; then
  echo "ERROR: managed section marker not found in $HTTP_SERVER_GO (patch skipped?)." >&2
  exit 1
fi
# Code lines only: strip full-line comments so doc mentions do not trip guards.
MANAGED_CODE="$(printf '%s\n' "$MANAGED_SECTION" | grep -v '^[[:space:]]*//' || true)"
for required in \
  'surguiManagedHost = "127.0.0.1"' \
  'func surguiManagedConfigFromEnv' \
  'func bindSurguiManagedListener' \
  'func loadSurguiManagedCert' \
  'func newSurguiManagedServer' \
  'func runSurguiManagedServer' \
  'surgui-managed-server' \
  'ServeTLS' \
  'X509KeyPair' \
  'TLSConfig' \
  'MinVersion' \
  'SURGUI_PORT' \
  'SURGUI_TLS_CERT_PEM' \
  'SURGUI_TLS_KEY_PEM' \
  'SURGUI_AUTH_TOKEN'; do
  if ! printf '%s\n' "$MANAGED_SECTION" | grep -Fq "$required"; then
    echo "ERROR: managed section lacks required marker: $required" >&2
    exit 1
  fi
done
# Fail closed on insecure fallbacks, file-based TLS, or token persistence:
# no plaintext Serve/ListenAndServe, no port scan, no token-file writes,
# no legacy token inputs, no TLS file paths, no file-based cert loader,
# no raw cert/key error details.
for forbidden in \
  '.Serve(' \
  'ListenAndServe' \
  'findAvailablePort' \
  'bindServerListener' \
  'persistAuthToken' \
  'ensureAuthToken' \
  'writeTokenToFile' \
  'resolveServerToken' \
  'resolveTokenPath' \
  'WriteFile' \
  'ReadFile' \
  'LoadX509KeyPair' \
  'SURGUI_TLS_CERT_FILE' \
  'SURGUI_TLS_KEY_FILE' \
  'certFile' \
  'keyFile' \
  'CERT_FILE' \
  'KEY_FILE' \
  'cert/key: %v' \
  'SURGE_TOKEN'; do
  if printf '%s\n' "$MANAGED_CODE" | grep -Fq "$forbidden"; then
    echo "ERROR: managed section contains forbidden marker: $forbidden" >&2
    exit 1
  fi
done

echo "Verifying managed tests are present in cmd_test.go..."
CMD_TEST_GO="$SRC_DIR/cmd/cmd_test.go"
TEST_SECTION="$(sed -n '/Tests for the Surgui-managed server/,$p' "$CMD_TEST_GO")"
if [ -z "$TEST_SECTION" ]; then
  echo "ERROR: managed test section marker not found in $CMD_TEST_GO (patch skipped?)." >&2
  exit 1
fi
for t in \
  'func TestSurguiManaged_ConfigMissingFailsClosed' \
  'func TestSurguiManaged_ConfigInvalidPortFailsClosed' \
  'func TestSurguiManaged_ConfigBadTokenFailsClosed' \
  'func TestSurguiManaged_ConfigValid' \
  'func TestSurguiManaged_BindsLoopbackOnly' \
  'func TestSurguiManaged_TLSListenerServesHTTPS' \
  'func TestSurguiManaged_NoPlaintextFallback' \
  'func TestSurguiManaged_PortCollisionFailsClosed' \
  'func TestSurguiManaged_WrongCertFailsClosed' \
  'func TestSurguiManaged_TokenNotPersisted' \
  'func TestSurguiManaged_BindHostDefaultsToLoopback' \
  'func TestSurguiManaged_DefaultServerListenerBindsLoopback' \
  'func TestSurguiManaged_NonLoopbackBindHostFailsBeforeListen'; do
  if ! printf '%s\n' "$TEST_SECTION" | grep -Fq "$t"; then
    echo "ERROR: managed test missing: $t" >&2
    exit 1
  fi
done

echo "Verifying managed tests use in-memory PEM contract (no TLS files)..."
for required in \
  'SURGUI_TLS_CERT_PEM' \
  'SURGUI_TLS_KEY_PEM' \
  'makeSurguiTestCertPEM' \
  'ServeTLS(ln, "", "")' \
  'TLSConfig'; do
  if ! printf '%s\n' "$TEST_SECTION" | grep -Fq "$required"; then
    echo "ERROR: managed tests lack required PEM marker: $required" >&2
    exit 1
  fi
done
for forbidden in \
  'SURGUI_TLS_CERT_FILE' \
  'SURGUI_TLS_KEY_FILE' \
  'LoadX509KeyPair' \
  'writeSurguiTestCertFiles' \
  'CERT_FILE' \
  'KEY_FILE'; do
  if printf '%s\n' "$TEST_SECTION" | grep -Fq "$forbidden"; then
    echo "ERROR: managed tests contain forbidden file-based TLS marker: $forbidden" >&2
    exit 1
  fi
done

echo "Checking gofmt on patched files..."
GOFMT_OUT=""
GOFMT_STATUS=0
GOFMT_OUT="$(gofmt -l "$HTTP_SERVER_GO" "$CMD_TEST_GO" "$SETTINGS_GO")" || GOFMT_STATUS=$?
if [ "$GOFMT_STATUS" -ne 0 ]; then
  echo "ERROR: gofmt execution failed with exit code ${GOFMT_STATUS}." >&2
  exit 1
fi
if [ -n "$GOFMT_OUT" ]; then
  echo "ERROR: patched files not gofmt-clean: $GOFMT_OUT" >&2
  exit 1
fi

echo "Running managed + related Go tests (fail closed)..."
(
  cd "$SRC_DIR"
  CGO_ENABLED=0 GOTOOLCHAIN=local GOMAXPROCS=2 GOMEMLIMIT=2GiB GOFLAGS=-trimpath \
    TMPDIR="$RUN_TMP" TMP="$RUN_TMP" TEMP="$RUN_TMP" GOTMPDIR="$RUN_GOTMP" \
    GOCACHE="$RUN_GOCACHE" GOMODCACHE="$RUN_GOMODCACHE" GOPATH="$RUN_GOPATH" \
    go test -p 1 ./cmd/ -run 'TestSurguiManaged|TestFindAvailablePort|TestStartHTTPServer|TestBindServerListener' -count=1 -v
)

echo "Compiling Surge for linux/amd64 (reproducible flags)..."
(
  cd "$SRC_DIR"
  CGO_ENABLED=0 GOTOOLCHAIN=local GOMAXPROCS=2 GOMEMLIMIT=2GiB GOFLAGS=-trimpath GOOS=linux GOARCH=amd64 \
    TMPDIR="$RUN_TMP" TMP="$RUN_TMP" TEMP="$RUN_TMP" GOTMPDIR="$RUN_GOTMP" \
    GOCACHE="$RUN_GOCACHE" GOMODCACHE="$RUN_GOMODCACHE" GOPATH="$RUN_GOPATH" \
    go build -trimpath -ldflags="-s -w" -o "$OUT" .
)

chmod 0755 "$OUT"

echo "Checking Tauri resource destination and executability..."
test -f "$OUT"
test -x "$OUT"
test -f "$REPO_ROOT/src-tauri/resources/Surge-LICENSE"
ls -l "$OUT" "$REPO_ROOT/src-tauri/resources/Surge-LICENSE"

echo "OK: staged Tauri resource: src-tauri/resources/surge-x86_64-unknown-linux-gnu"
