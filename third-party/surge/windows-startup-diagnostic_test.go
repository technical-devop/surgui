//go:build windows

package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

const (
	surguiDiagnosticPort         = 47531
	surguiDiagnosticToken        = "surgui-windows-startup-diagnostic-token"
	surguiDiagnosticStartupWait  = 10 * time.Second
	surguiDiagnosticShutdownWait = 10 * time.Second
	surguiDiagnosticProbeTimeout = 750 * time.Millisecond
)

// Dedicated diagnostic-only env var supplying the absolute marker file path.
// Set by the PowerShell wrapper to a unique per-run file under $RunDir.
const surguiDiagnosticMarkerFileEnv = "SURGUI_WINDOWS_DIAGNOSTIC_MARKER_FILE"

// Fixed suffix for the separate preflight marker, derived from the diagnostic
// marker path. It is exclusively created before the managed engine starts.
const surguiDiagnosticPreflightMarkerSuffix = ".preflight"

// Exact predeclared category markers. The test writes ONLY one of these
// literals to the marker file; no raw output, paths, or credentials.
const (
	surguiDiagnosticMarkerOk            = "CI_DIAG:go_startup_ok"
	surguiDiagnosticMarkerConfig        = "CI_DIAG:go_startup_config"
	surguiDiagnosticMarkerCert          = "CI_DIAG:go_startup_cert"
	surguiDiagnosticMarkerLock          = "CI_DIAG:go_startup_lock"
	surguiDiagnosticMarkerEngine        = "CI_DIAG:go_startup_engine"
	surguiDiagnosticMarkerLifecycle     = "CI_DIAG:go_startup_lifecycle"
	surguiDiagnosticMarkerBind          = "CI_DIAG:go_startup_bind"
	surguiDiagnosticMarkerBindInvalid   = "CI_DIAG:go_startup_bind_invalid"
	surguiDiagnosticMarkerBindAddrInUse = "CI_DIAG:go_startup_bind_addr_in_use"
	surguiDiagnosticMarkerBindDenied    = "CI_DIAG:go_startup_bind_access_denied"
	surguiDiagnosticMarkerBindOther     = "CI_DIAG:go_startup_bind_other"
	surguiDiagnosticMarkerServe         = "CI_DIAG:go_startup_serve"
	surguiDiagnosticMarkerOther         = "CI_DIAG:go_startup_other"
	surguiDiagnosticMarkerNotReady      = "CI_DIAG:go_startup_not_ready"
)

// Exact predeclared preflight category markers. These are written to the
// separate exclusively created ".preflight" marker before engine startup.
const (
	surguiDiagnosticMarkerPreflightFree         = "CI_DIAG:go_preflight_free"
	surguiDiagnosticMarkerPreflightAddrInUse    = "CI_DIAG:go_preflight_address_in_use"
	surguiDiagnosticMarkerPreflightAccessDenied = "CI_DIAG:go_preflight_access_denied"
	surguiDiagnosticMarkerPreflightOther        = "CI_DIAG:go_preflight_other"
)

// emitSurguiWindowsDiagnosticMarker selects one fixed literal via an explicit
// category switch and writes ONLY that literal to the marker file. All
// unknown categories map to the fixed other literal. All file errors are
// swallowed (outer wrapper falls back to go_unknown). It never prints the
// marker path or raw errors.
func emitSurguiWindowsDiagnosticMarker(category string) {
	var marker string
	switch category {
	case "ok":
		marker = surguiDiagnosticMarkerOk
	case "config":
		marker = surguiDiagnosticMarkerConfig
	case "cert":
		marker = surguiDiagnosticMarkerCert
	case "lock":
		marker = surguiDiagnosticMarkerLock
	case "engine":
		marker = surguiDiagnosticMarkerEngine
	case "lifecycle":
		marker = surguiDiagnosticMarkerLifecycle
	case "bind":
		marker = surguiDiagnosticMarkerBind
	case "bind_invalid":
		marker = surguiDiagnosticMarkerBindInvalid
	case "bind_addr_in_use":
		marker = surguiDiagnosticMarkerBindAddrInUse
	case "bind_access_denied":
		marker = surguiDiagnosticMarkerBindDenied
	case "bind_other":
		marker = surguiDiagnosticMarkerBindOther
	case "serve":
		marker = surguiDiagnosticMarkerServe
	case "not_ready":
		marker = surguiDiagnosticMarkerNotReady
	case "other":
		marker = surguiDiagnosticMarkerOther
	default:
		marker = surguiDiagnosticMarkerOther
	}
	path := os.Getenv(surguiDiagnosticMarkerFileEnv)
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(marker)
}

// emitSurguiWindowsPreflightMarker selects one fixed literal via an explicit
// category switch and writes ONLY that literal to the separate exclusively
// created preflight marker derived from the diagnostic marker path. All file
// errors are swallowed (the outer wrapper then emits go_preflight_unknown).
// It never prints the marker path or raw errors.
func emitSurguiWindowsPreflightMarker(category string) {
	var marker string
	switch category {
	case "free":
		marker = surguiDiagnosticMarkerPreflightFree
	case "address_in_use":
		marker = surguiDiagnosticMarkerPreflightAddrInUse
	case "access_denied":
		marker = surguiDiagnosticMarkerPreflightAccessDenied
	case "other":
		marker = surguiDiagnosticMarkerPreflightOther
	default:
		marker = surguiDiagnosticMarkerPreflightOther
	}
	path := os.Getenv(surguiDiagnosticMarkerFileEnv)
	if path == "" {
		return
	}
	f, err := os.OpenFile(path+surguiDiagnosticPreflightMarkerSuffix, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(marker)
}

// classifySurguiWindowsPreflightError maps only fixed Windows socket errors to
// a static category using errors.Is. It never returns or prints raw values.
func classifySurguiWindowsPreflightError(err error) string {
	switch {
	case err == nil:
		return "free"
	case errors.Is(err, syscall.Errno(10048)):
		return "address_in_use"
	case errors.Is(err, syscall.Errno(10013)):
		return "access_denied"
	default:
		return "other"
	}
}

// runSurguiWindowsStartupPreflight directly probes the fixed diagnostic port
// before the managed engine starts, then immediately closes the listener on
// success. It never inspects or signals any socket owner and never touches
// any other port.
func runSurguiWindowsStartupPreflight() {
	ln, err := net.Listen("tcp", "127.0.0.1:47531")
	if err != nil {
		emitSurguiWindowsPreflightMarker(classifySurguiWindowsPreflightError(err))
		return
	}
	_ = ln.Close()
	emitSurguiWindowsPreflightMarker(classifySurguiWindowsPreflightError(nil))
}

// classifySurguiWindowsBindError maps a managed-server bind error to a fixed
// category using errors.Is. It never returns or prints the original error.
func classifySurguiWindowsBindError(err error) string {
	switch {
	case errors.Is(err, syscall.Errno(10048)):
		return "bind_addr_in_use"
	case errors.Is(err, syscall.Errno(10013)):
		return "bind_access_denied"
	default:
		return "bind_other"
	}
}

// classifySurguiWindowsStartupError maps only known, fixed error prefixes to a
// static category. It never returns or prints the original error text. The
// bind prefix is preserved only to identify a bind error; the underlying
// wrapped errno is then classified separately.
func classifySurguiWindowsStartupError(err error) string {
	if err == nil {
		return "ok"
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "surgui managed server: SURGUI_"):
		return "config"
	case strings.HasPrefix(msg, "surgui managed server: TLS cert and key are required"),
		strings.HasPrefix(msg, "surgui managed server: invalid TLS cert/key"):
		return "cert"
	case strings.HasPrefix(msg, "surgui managed server: error acquiring lock"),
		strings.HasPrefix(msg, "surgui managed server: surge server is already running"):
		return "lock"
	case strings.HasPrefix(msg, "stop previous local service"),
		strings.HasPrefix(msg, "failed to create surge directories"):
		return "engine"
	case strings.HasPrefix(msg, "surgui managed server: error creating lifecycle event stream"):
		return "lifecycle"
	case strings.HasPrefix(msg, "surgui managed server: port must be in 1-65535"):
		return "bind_invalid"
	case strings.HasPrefix(msg, "surgui managed server: could not bind"):
		return classifySurguiWindowsBindError(err)
	case strings.HasPrefix(msg, "surgui managed server: HTTPS server error"):
		return "serve"
	default:
		return "other"
	}
}

func surguiWindowsDiagnosticHealthy(client *http.Client, token string) bool {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://127.0.0.1:%d/health", surguiDiagnosticPort), nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// TestWindowsStartupDiagnostic exercises the patched managed-server entrypoint
// in-process on loopback. Before engine startup it writes one exact predeclared
// preflight literal to a separate ".preflight" marker derived from the
// diagnostic-only env var, then writes at most one exact predeclared startup
// literal to the main marker file. It never prints raw output, paths, or
// credentials, and never calls t.Fatal with a dynamic error. The wrapper
// discards stdout/stderr and accepts only exact allowlisted literals from the
// small bounded marker files; a missing startup marker becomes
// CI_DIAG:go_unknown and a missing preflight marker becomes
// CI_DIAG:go_preflight_unknown.
func TestWindowsStartupDiagnostic(t *testing.T) {
	// Baseline evidence: probe the fixed fixture port BEFORE any engine
	// initialization. On success the listener is closed immediately.
	runSurguiWindowsStartupPreflight()

	// Diagnostic-only XDG handling; differs from production.
	// The PowerShell wrapper sets all relevant XDG user-dir variables
	// (XDG_CONFIG_HOME, XDG_STATE_HOME, XDG_DATA_HOME, XDG_CACHE_HOME,
	// XDG_RUNTIME_DIR, XDG_CONFIG_DIRS, XDG_DATA_DIRS) plus APPDATA,
	// LOCALAPPDATA, USERPROFILE, HOME, TEMP/TMP/TMPDIR to isolated per-run
	// dirs BEFORE the test process starts, so the cached xdg.RuntimeDir value
	// is confined at startup. Clearing XDG_RUNTIME_DIR here (AFTER process
	// start) exercises fallback to that cached isolated value. This differs
	// from the managed Windows app in production, where XDG variables are
	// absent at process initialization and the fallback may resolve via the
	// Windows KnownFolder API, which prioritizes the real local profile
	// outside any isolated tree. This test therefore does not claim exact
	// reproduction of production startup paths and cannot guarantee zero
	// writes outside synthetic state.
	t.Setenv("XDG_RUNTIME_DIR", "")

	certPEM, keyPEM := makeSurguiTestCertPEM(t)
	setSurguiTestEnv(t, surguiDiagnosticPort, certPEM, keyPEM, surguiDiagnosticToken)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := &cobra.Command{Use: "surge-windows-diagnostic"}
	root.SetContext(ctx)
	cmd := &cobra.Command{Use: "surgui-managed-server"}
	root.AddCommand(cmd)

	errCh := make(chan error, 1)
	go func() {
		var runErr error
		defer func() {
			if recover() != nil {
				runErr = fmt.Errorf("surgui managed server: recovered panic")
			}
			errCh <- runErr
		}()
		runErr = runSurguiManagedServer(cmd)
	}()

	probePool := x509.NewCertPool()
	if !probePool.AppendCertsFromPEM([]byte(certPEM)) {
		emitSurguiWindowsDiagnosticMarker("other")
		return
	}
	probeClient := &http.Client{
		Timeout: surguiDiagnosticProbeTimeout,
		Transport: &http.Transport{
			// Pinned to the synthetic cert: the TLS handshake must verify
			// against this pool before any HTTP bytes (including the
			// Authorization header) are sent, so the synthetic bearer is
			// never sent to an unrelated listener. No InsecureSkipVerify.
			TLSClientConfig: &tls.Config{
				RootCAs:    probePool,
				ServerName: "127.0.0.1",
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	defer probeClient.CloseIdleConnections()

	sawHealthy := false
	deadline := time.Now().Add(surguiDiagnosticStartupWait)
	for time.Now().Before(deadline) {
		select {
		case runErr := <-errCh:
			cancel()
			// Before health: a nil exit is not ok. Health never succeeded,
			// so report the dedicated not-ready marker; only the
			// post-health path below may report ok after an authenticated
			// trusted TLS probe succeeded.
			if runErr == nil {
				emitSurguiWindowsDiagnosticMarker("not_ready")
				return
			}
			emitSurguiWindowsDiagnosticMarker(classifySurguiWindowsStartupError(runErr))
			return
		default:
		}
		if surguiWindowsDiagnosticHealthy(probeClient, surguiDiagnosticToken) {
			sawHealthy = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancel()

	if !sawHealthy {
		// Health never succeeded: never report ok just because context
		// cancel returned nil. Drain shutdown with a bounded wait, then
		// report the dedicated not-ready marker.
		select {
		case <-errCh:
		case <-time.After(surguiDiagnosticShutdownWait):
		}
		emitSurguiWindowsDiagnosticMarker("not_ready")
		return
	}

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(surguiDiagnosticShutdownWait):
		// No marker: the outer CI wrapper emits CI_DIAG:go_unknown.
		return
	}
	emitSurguiWindowsDiagnosticMarker(classifySurguiWindowsStartupError(runErr))
}
