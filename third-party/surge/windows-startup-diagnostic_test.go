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
	"strconv"
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

// Fixed suffix appended to the existing preflight marker path for the
// sanitized numeric OS errno evidence. Comparison only; the value is bounded
// to the fixed 16-bit range 0..65535. No raw error text is ever written.
const surguiDiagnosticPreflightErrnoSuffix = ".errno"

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

// Additional fixed startup bind markers for the refined shared socket
// classifier. Each marker is a static literal selected explicitly.
const (
	surguiDiagnosticMarkerBindAddressUnavailable       = "CI_DIAG:go_startup_bind_address_unavailable"
	surguiDiagnosticMarkerBindNetworkDown              = "CI_DIAG:go_startup_bind_network_down"
	surguiDiagnosticMarkerBindAddressFamilyUnsupported = "CI_DIAG:go_startup_bind_address_family_unsupported"
	surguiDiagnosticMarkerBindInvalidArgument          = "CI_DIAG:go_startup_bind_invalid_argument"
	surguiDiagnosticMarkerBindSocketLimit              = "CI_DIAG:go_startup_bind_socket_limit"
	surguiDiagnosticMarkerBindNoBufferSpace            = "CI_DIAG:go_startup_bind_no_buffer_space"
	surguiDiagnosticMarkerBindWinsockNotReady          = "CI_DIAG:go_startup_bind_winsock_not_ready"
	surguiDiagnosticMarkerBindWinsockVersion           = "CI_DIAG:go_startup_bind_winsock_version"
	surguiDiagnosticMarkerBindWinsockUninitialized     = "CI_DIAG:go_startup_bind_winsock_uninitialized"
	surguiDiagnosticMarkerBindOtherSyscall             = "CI_DIAG:go_startup_bind_other_syscall"
	surguiDiagnosticMarkerBindAddrError                = "CI_DIAG:go_startup_bind_addr_error"
	surguiDiagnosticMarkerBindDNSError                 = "CI_DIAG:go_startup_bind_dns_error"
	surguiDiagnosticMarkerBindOtherNonSyscall          = "CI_DIAG:go_startup_bind_other_non_syscall"
)

// Additional fixed preflight markers for the refined shared socket classifier.
const (
	surguiDiagnosticMarkerPreflightAddressUnavailable       = "CI_DIAG:go_preflight_address_unavailable"
	surguiDiagnosticMarkerPreflightNetworkDown              = "CI_DIAG:go_preflight_network_down"
	surguiDiagnosticMarkerPreflightAddressFamilyUnsupported = "CI_DIAG:go_preflight_address_family_unsupported"
	surguiDiagnosticMarkerPreflightInvalidArgument          = "CI_DIAG:go_preflight_invalid_argument"
	surguiDiagnosticMarkerPreflightSocketLimit              = "CI_DIAG:go_preflight_socket_limit"
	surguiDiagnosticMarkerPreflightNoBufferSpace            = "CI_DIAG:go_preflight_no_buffer_space"
	surguiDiagnosticMarkerPreflightWinsockNotReady          = "CI_DIAG:go_preflight_winsock_not_ready"
	surguiDiagnosticMarkerPreflightWinsockVersion           = "CI_DIAG:go_preflight_winsock_version"
	surguiDiagnosticMarkerPreflightWinsockUninitialized     = "CI_DIAG:go_preflight_winsock_uninitialized"
	surguiDiagnosticMarkerPreflightOtherSyscall             = "CI_DIAG:go_preflight_other_syscall"
	surguiDiagnosticMarkerPreflightAddrError                = "CI_DIAG:go_preflight_addr_error"
	surguiDiagnosticMarkerPreflightDNSError                 = "CI_DIAG:go_preflight_dns_error"
	surguiDiagnosticMarkerPreflightOtherNonSyscall          = "CI_DIAG:go_preflight_other_non_syscall"
)

// Fixed Windows socket errno values classified by the shared function.
const (
	surguiWindowsSocketErrnoAddressInUse             = syscall.Errno(10048)
	surguiWindowsSocketErrnoAccessDenied             = syscall.Errno(10013)
	surguiWindowsSocketErrnoAddressUnavailable       = syscall.Errno(10049)
	surguiWindowsSocketErrnoNetworkDown              = syscall.Errno(10050)
	surguiWindowsSocketErrnoAddressFamilyUnsupported = syscall.Errno(10047)
	surguiWindowsSocketErrnoInvalidArgument          = syscall.Errno(10022)
	surguiWindowsSocketErrnoSocketLimit              = syscall.Errno(10024)
	surguiWindowsSocketErrnoNoBufferSpace            = syscall.Errno(10055)
	surguiWindowsSocketErrnoWinsockNotReady          = syscall.Errno(10091)
	surguiWindowsSocketErrnoWinsockVersion           = syscall.Errno(10092)
	surguiWindowsSocketErrnoWinsockUninitialized     = syscall.Errno(10093)
)

// Fixed socket-error category literals returned by the shared classifier.
// These are the only category values; emitters map them to fixed markers.
const (
	surguiSocketCategoryFree                     = "free"
	surguiSocketCategoryAddressInUse             = "address_in_use"
	surguiSocketCategoryAccessDenied             = "access_denied"
	surguiSocketCategoryAddressUnavailable       = "address_unavailable"
	surguiSocketCategoryNetworkDown              = "network_down"
	surguiSocketCategoryAddressFamilyUnsupported = "address_family_unsupported"
	surguiSocketCategoryInvalidArgument          = "invalid_argument"
	surguiSocketCategorySocketLimit              = "socket_limit"
	surguiSocketCategoryNoBufferSpace            = "no_buffer_space"
	surguiSocketCategoryWinsockNotReady          = "winsock_not_ready"
	surguiSocketCategoryWinsockVersion           = "winsock_version"
	surguiSocketCategoryWinsockUninitialized     = "winsock_uninitialized"
	surguiSocketCategoryOtherSyscall             = "other_syscall"
	surguiSocketCategoryAddrError                = "addr_error"
	surguiSocketCategoryDNSError                 = "dns_error"
	surguiSocketCategoryOtherNonSyscall          = "other_non_syscall"
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
	case surguiSocketCategoryAddressInUse:
		marker = surguiDiagnosticMarkerBindAddrInUse
	case surguiSocketCategoryAccessDenied:
		marker = surguiDiagnosticMarkerBindDenied
	case surguiSocketCategoryAddressUnavailable:
		marker = surguiDiagnosticMarkerBindAddressUnavailable
	case surguiSocketCategoryNetworkDown:
		marker = surguiDiagnosticMarkerBindNetworkDown
	case surguiSocketCategoryAddressFamilyUnsupported:
		marker = surguiDiagnosticMarkerBindAddressFamilyUnsupported
	case surguiSocketCategoryInvalidArgument:
		marker = surguiDiagnosticMarkerBindInvalidArgument
	case surguiSocketCategorySocketLimit:
		marker = surguiDiagnosticMarkerBindSocketLimit
	case surguiSocketCategoryNoBufferSpace:
		marker = surguiDiagnosticMarkerBindNoBufferSpace
	case surguiSocketCategoryWinsockNotReady:
		marker = surguiDiagnosticMarkerBindWinsockNotReady
	case surguiSocketCategoryWinsockVersion:
		marker = surguiDiagnosticMarkerBindWinsockVersion
	case surguiSocketCategoryWinsockUninitialized:
		marker = surguiDiagnosticMarkerBindWinsockUninitialized
	case surguiSocketCategoryOtherSyscall:
		marker = surguiDiagnosticMarkerBindOtherSyscall
	case surguiSocketCategoryAddrError:
		marker = surguiDiagnosticMarkerBindAddrError
	case surguiSocketCategoryDNSError:
		marker = surguiDiagnosticMarkerBindDNSError
	case surguiSocketCategoryOtherNonSyscall:
		marker = surguiDiagnosticMarkerBindOtherNonSyscall
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
	case surguiSocketCategoryFree:
		marker = surguiDiagnosticMarkerPreflightFree
	case surguiSocketCategoryAddressInUse:
		marker = surguiDiagnosticMarkerPreflightAddrInUse
	case surguiSocketCategoryAccessDenied:
		marker = surguiDiagnosticMarkerPreflightAccessDenied
	case surguiSocketCategoryAddressUnavailable:
		marker = surguiDiagnosticMarkerPreflightAddressUnavailable
	case surguiSocketCategoryNetworkDown:
		marker = surguiDiagnosticMarkerPreflightNetworkDown
	case surguiSocketCategoryAddressFamilyUnsupported:
		marker = surguiDiagnosticMarkerPreflightAddressFamilyUnsupported
	case surguiSocketCategoryInvalidArgument:
		marker = surguiDiagnosticMarkerPreflightInvalidArgument
	case surguiSocketCategorySocketLimit:
		marker = surguiDiagnosticMarkerPreflightSocketLimit
	case surguiSocketCategoryNoBufferSpace:
		marker = surguiDiagnosticMarkerPreflightNoBufferSpace
	case surguiSocketCategoryWinsockNotReady:
		marker = surguiDiagnosticMarkerPreflightWinsockNotReady
	case surguiSocketCategoryWinsockVersion:
		marker = surguiDiagnosticMarkerPreflightWinsockVersion
	case surguiSocketCategoryWinsockUninitialized:
		marker = surguiDiagnosticMarkerPreflightWinsockUninitialized
	case surguiSocketCategoryOtherSyscall:
		marker = surguiDiagnosticMarkerPreflightOtherSyscall
	case surguiSocketCategoryAddrError:
		marker = surguiDiagnosticMarkerPreflightAddrError
	case surguiSocketCategoryDNSError:
		marker = surguiDiagnosticMarkerPreflightDNSError
	case surguiSocketCategoryOtherNonSyscall:
		marker = surguiDiagnosticMarkerPreflightOtherNonSyscall
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

// surguiWindowsPreflightErrno extracts ONLY a sanitized numeric OS errno from
// a failed preflight error. errors.As must find a wrapped syscall.Errno and
// the value must be within the fixed 16-bit bound 0..65535; otherwise it
// reports false so no errno file is created. It never returns error text,
// paths, identity, auth, stdout, or stderr. This numeric category describes
// the synthetic CI preflight only.
func surguiWindowsPreflightErrno(err error) (uint16, bool) {
	if err == nil {
		return 0, false
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return 0, false
	}
	value := uint64(errno)
	if value > 65535 {
		return 0, false
	}
	return uint16(value), true
}

// emitSurguiWindowsPreflightErrno writes ONLY the canonical decimal ASCII
// digits of a sanitized bounded errno to the exclusive separate
// "<existing preflight marker>.errno" file. It creates no file when
// extraction fails, and never writes candidate or arbitrary bytes. All file
// errors are swallowed; the wrapper then reports the fixed unknown literal.
func emitSurguiWindowsPreflightErrno(err error) {
	value, ok := surguiWindowsPreflightErrno(err)
	if !ok {
		return
	}
	path := os.Getenv(surguiDiagnosticMarkerFileEnv)
	if path == "" {
		return
	}
	f, openErr := os.OpenFile(path+surguiDiagnosticPreflightMarkerSuffix+surguiDiagnosticPreflightErrnoSuffix, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if openErr != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(strconv.FormatUint(uint64(value), 10))
}

// classifySurguiWindowsSocketError maps an error to exactly one fixed socket
// category. Known Windows syscall errnos map to fixed literals; any other
// syscall errno maps to other_syscall via errors.As. Address and DNS errors
// map to addr_error and dns_error; all remaining errors map to
// other_non_syscall. It never returns or prints dynamic error text.
func classifySurguiWindowsSocketError(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case surguiWindowsSocketErrnoAddressInUse:
			return surguiSocketCategoryAddressInUse
		case surguiWindowsSocketErrnoAccessDenied:
			return surguiSocketCategoryAccessDenied
		case surguiWindowsSocketErrnoAddressUnavailable:
			return surguiSocketCategoryAddressUnavailable
		case surguiWindowsSocketErrnoNetworkDown:
			return surguiSocketCategoryNetworkDown
		case surguiWindowsSocketErrnoAddressFamilyUnsupported:
			return surguiSocketCategoryAddressFamilyUnsupported
		case surguiWindowsSocketErrnoInvalidArgument:
			return surguiSocketCategoryInvalidArgument
		case surguiWindowsSocketErrnoSocketLimit:
			return surguiSocketCategorySocketLimit
		case surguiWindowsSocketErrnoNoBufferSpace:
			return surguiSocketCategoryNoBufferSpace
		case surguiWindowsSocketErrnoWinsockNotReady:
			return surguiSocketCategoryWinsockNotReady
		case surguiWindowsSocketErrnoWinsockVersion:
			return surguiSocketCategoryWinsockVersion
		case surguiWindowsSocketErrnoWinsockUninitialized:
			return surguiSocketCategoryWinsockUninitialized
		default:
			return surguiSocketCategoryOtherSyscall
		}
	}
	var addrErr *net.AddrError
	if errors.As(err, &addrErr) {
		return surguiSocketCategoryAddrError
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return surguiSocketCategoryDNSError
	}
	return surguiSocketCategoryOtherNonSyscall
}

// classifySurguiWindowsPreflightError maps the preflight listen error to the
// shared fixed socket category, preserving the dedicated free category. It
// never returns or prints raw values.
func classifySurguiWindowsPreflightError(err error) string {
	if err == nil {
		return surguiSocketCategoryFree
	}
	return classifySurguiWindowsSocketError(err)
}

// runSurguiWindowsStartupPreflight directly probes the fixed diagnostic port
// before the managed engine starts, then immediately closes the listener on
// success. It never inspects or signals any socket owner and never touches
// any other port.
func runSurguiWindowsStartupPreflight() {
	ln, err := net.Listen("tcp", "127.0.0.1:47531")
	if err != nil {
		emitSurguiWindowsPreflightMarker(classifySurguiWindowsPreflightError(err))
		emitSurguiWindowsPreflightErrno(err)
		return
	}
	_ = ln.Close()
	emitSurguiWindowsPreflightMarker(classifySurguiWindowsPreflightError(nil))
}

// classifySurguiWindowsBindError maps a managed-server bind error to the
// shared fixed socket category. It never returns or prints the original error.
func classifySurguiWindowsBindError(err error) string {
	return classifySurguiWindowsSocketError(err)
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

// TestSurguiWindowsSocketErrorClassification verifies the shared classifier on
// wrapped representative errnos and non-syscall errors, plus focused
// extraction of a sanitized bounded preflight errno. Assertions contain only
// fixed category literals and bounded numeric values; no error details are
// included.
func TestSurguiWindowsSocketErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"wrapped_address_in_use", fmt.Errorf("wrapped: %w", syscall.Errno(10048)), surguiSocketCategoryAddressInUse},
		{"wrapped_access_denied", fmt.Errorf("wrapped: %w", syscall.Errno(10013)), surguiSocketCategoryAccessDenied},
		{"wrapped_address_unavailable", fmt.Errorf("wrapped: %w", syscall.Errno(10049)), surguiSocketCategoryAddressUnavailable},
		{"wrapped_winsock_not_ready", fmt.Errorf("wrapped: %w", syscall.Errno(10091)), surguiSocketCategoryWinsockNotReady},
		{"wrapped_unknown_errno", fmt.Errorf("wrapped: %w", syscall.Errno(10099)), surguiSocketCategoryOtherSyscall},
		{"wrapped_addr_error", fmt.Errorf("wrapped: %w", &net.AddrError{Err: "test", Addr: "test"}), surguiSocketCategoryAddrError},
		{"wrapped_dns_error", fmt.Errorf("wrapped: %w", &net.DNSError{Err: "test", Name: "test"}), surguiSocketCategoryDNSError},
		{"wrapped_non_syscall", fmt.Errorf("wrapped: %w", errors.New("plain test error")), surguiSocketCategoryOtherNonSyscall},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := classifySurguiWindowsSocketError(tt.err)
			if got != tt.want {
				t.Fatalf("unexpected category: got %q want %q", got, tt.want)
			}
		})
	}

	// Focused extraction tests: only a wrapped syscall.Errno within the fixed
	// 16-bit bound 0..65535 is accepted. Non-errno and out-of-range values must
	// report false so no errno marker file is created. Comparison only; no raw
	// error text is asserted or emitted.
	extractTests := []struct {
		name   string
		err    error
		want   uint16
		wantOK bool
	}{
		{"extract_wrapped_errno", fmt.Errorf("wrapped: %w", syscall.Errno(10099)), 10099, true},
		{"extract_non_errno", fmt.Errorf("wrapped: %w", errors.New("plain test error")), 0, false},
		{"extract_errno_above_65535", fmt.Errorf("wrapped: %w", syscall.Errno(65536)), 0, false},
	}
	for _, tt := range extractTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, ok := surguiWindowsPreflightErrno(tt.err)
			if ok != tt.wantOK || (ok && got != tt.want) {
				t.Fatalf("unexpected errno extraction: got=%d ok=%t want=%d wantOK=%t", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}
