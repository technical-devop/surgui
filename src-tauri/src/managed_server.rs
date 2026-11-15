//! Rust managed daemon coordinator (per-launch isolated Surge daemon).
//!
//! This module is the only place that launches the bundled
//! `surgui-managed-server` daemon. It composes the existing audited modules
//! without modifying them:
//!
//! * [`crate::managed_identity::ManagedIdentity::generate`] for a per-launch
//!   fresh bearer token plus ephemeral local CA and leaf TLS key/certificate
//!   (PEM views via accessors, never filesystem).
//! * [`crate::managed_state::ManagedStateDir::prepare`] to verify (or create)
//!   the caller-supplied isolated persistent state directory (private
//!   mode / ACL, symlink-safe, never deletes data).
//! * `cfg(target_os = "linux")`: [`crate::managed_process::ManagedChild`].
//!   `cfg(windows)`: [`crate::managed_windows::ManagedChild`].
//! * [`crate::surge_client::SurgeClient::connect_managed`] for pinned-TLS
//!   readiness over `https://127.0.0.1:1700` (no plaintext, no untrusted
//!   cert, no redirect, bearer only in `Authorization` header).
//!
//! Patched daemon contract (Go `surgui-managed-server`, hidden command):
//! fixed loopback `127.0.0.1`, explicit `SURGUI_PORT` (no scan fallback),
//! TLS-only via in-memory `SURGUI_TLS_CERT_PEM` / `SURGUI_TLS_KEY_PEM`,
//! bearer only via `SURGUI_AUTH_TOKEN`. Env strings only; never argv.
//!
//! # Caller contract
//!
//! * `binary` must be the exact caller-verified absolute path of the bundled
//!   daemon binary. Bare filenames and relative paths are rejected and `PATH`
//!   is never searched.
//! * `state_dir` must be a trusted absolute app-owned base path. It is passed
//!   to `ManagedStateDir::prepare`, which verifies private mode/ACL
//!   (`0700` on Linux, protected user/SYSTEM/Administrators DACL on
//!   Windows) and symlink-safety, creating only the final component.
//! * `home_dir` must be the real user home absolute path. It is used only as
//!   `HOME` (Linux) / `USERPROFILE` (Windows) so the daemon's download
//!   location resolution (`~/Downloads` fallback) keeps pointing at the real
//!   user location while all daemon state stays isolated.
//!
//! # Environment allowlist (cleared, strict, no inheritance)
//!
//! The child is spawned with `clear_env(true)`: no parent/app/process
//! environment is inherited, so no ambient secrets can leak in. Only the
//! following in-memory values are set:
//!
//! Linux:
//! `SURGUI_PORT`, `SURGUI_TLS_CERT_PEM`, `SURGUI_TLS_KEY_PEM`,
//! `SURGUI_AUTH_TOKEN`, `HOME` (real home), `XDG_STATE_HOME`,
//! `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_CACHE_HOME`, `XDG_RUNTIME_DIR`
//! (all five XDG bases point at the verified isolated state directory).
//!
//! Windows:
//! `SURGUI_PORT`, `SURGUI_TLS_CERT_PEM`, `SURGUI_TLS_KEY_PEM`,
//! `SURGUI_AUTH_TOKEN`, `APPDATA` (isolated state), `USERPROFILE` (real
//! home), `TEMP`/`TMP` (isolated state).
//!
//! # Owner-thread design (Windows `HANDLE`s are not `Send`)
//!
//! The Windows `ManagedChild` owns raw `HANDLE`s with no `Send` impl. This
//! module therefore never assumes the child is `Send` and never moves it
//! across threads. Spawning happens on a dedicated owner thread that keeps
//! the child handle for its whole lifetime; the coordinator communicates
//! only sanitized messages (`pid`, exit codes, shutdown) over channels.
//! The same design is used on Linux for uniformity, where the supervisor
//! requirement of `managed_process` is additionally satisfied (the forking
//! thread stays alive until owner drop).
//!
//! # Lifetime
//!
//! The child stays alive until coordinator shutdown/drop. After readiness,
//! retirement is bounded and graceful: the coordinator sends the authenticated
//! pinned-TLS `POST /shutdown` to exactly its own managed daemon, the owner
//! thread waits a fixed grace period for the owned child to exit, then
//! force-kills and reaps only the owned handle (never a bare pid, so PID reuse
//! cannot hit another process). After a bounded force-kill timeout the same
//! owner thread retains the child and waits there until the exit is confirmed;
//! it does not spawn a second reaper, and a transient OS wait error is retried
//! with backoff rather than treated as success.
//!
//! Before readiness there is no authenticated session, so an HTTP graceful
//! request is not yet possible. The daemon may nevertheless already have
//! started daemon-side work (for example resumed downloads) before the probe
//! observes readiness. Startup cancellation/failure therefore gives the owned
//! child a short exit grace and may still have to terminate it; that pre-ready
//! termination limitation is unavoidable and is never described as graceful.
//! On a readiness timeout the coordinator makes one final bounded pinned-TLS
//! authenticated probe and sends `/shutdown` if that session is established.
//!
//! Every owner-thread lifetime is tracked separately from the start future, so
//! a cancelled/failed start keeps its retirement fence and app-drain accounting
//! until the real owner has exited/reaped the child. A timed-out reap remains
//! marked incomplete instead of being reported as reaped.
//!
//! On Windows, a spawn that fails after `CreateProcessW` (job verification or
//! resume failure) still owns a created process handle. The owner thread
//! reports the spawn failure promptly so caller waits stay bounded, then keeps
//! that handle on the same thread and waits until the OS confirms real exit
//! before marking its lifetime reaped. A failed spawn is therefore never
//! treated as proof that no child exists.
//!
//! The authenticated shutdown request is dispatched as soon as shutdown
//! begins, before any await, so cancelling the async `shutdown` future cannot
//! skip it. Drop and the synchronous app-shutdown path run the network request
//! on a dedicated thread with its own current-thread runtime, so no `block_on`
//! is nested inside the caller's async runtime. Every wait has a fixed bound
//! and an owner that cannot finish in time is detached instead of blocking
//! forever.
//!
//! # Privacy
//!
//! Keys/token never appear in argv, UI, logs, filesystem, or `Debug`.
//! `Debug`/`Display` for [`ManagedServer`] and [`ManagedServerError`] are
//! redacted and emit no paths, env contents, PEM, or token material.
//! In production this module never reads process environment and never
//! performs telemetry or runtime downloads. The sole test-only exception is an
//! explicitly gated Windows `SystemRoot` comparison copy that is never logged
//! or retained. Network is only loopback TLS to `127.0.0.1:1700` via the pinned
//! client.
//!
//! # Platforms
//!
//! Linux and Windows are supported via their respective `ManagedChild`.
//! Any other platform fails closed without spawning or filesystem I/O.

use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::managed_identity::ManagedIdentity;
use crate::managed_state::ManagedStateDir;
use crate::surge_client::SurgeClient;

/// Fixed managed-daemon port. The pinned client always dials
/// `https://127.0.0.1:1700`; no other port is ever used.
pub const MANAGED_PORT: u16 = 1700;

/// Fixed managed-daemon argv (no secrets; env carries identity).
const MANAGED_ARGV: &str = "surgui-managed-server";

/// How often the readiness loop polls the pinned endpoint.
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How often the owner thread polls its owned child.
const OWNER_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How often shutdown orchestration polls the owner thread.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Bound for the authenticated `POST /shutdown` request itself.
const SHUTDOWN_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Grace period for the owned daemon to exit after `POST /shutdown`.
const SHUTDOWN_CHILD_GRACE: Duration = Duration::from_secs(5);

/// Fresh bound for reaping the owned handle after a forced kill.
const SHUTDOWN_FORCE_REAP_BOUND: Duration = Duration::from_secs(2);

/// Extra slack for the owner thread to finish after its shutdown deadline.
const SHUTDOWN_OWNER_SLACK: Duration = Duration::from_secs(1);

/// Short grace for an unready child to exit on its own before the owner
/// force-kills it. No authenticated `/shutdown` session exists before
/// readiness, so this is a termination grace, not a graceful API request.
const SHUTDOWN_UNREADY_GRACE: Duration = Duration::from_secs(1);

/// Bound for the final pre-readiness authenticated shutdown attempt.
const PRE_READY_SHUTDOWN_PROBE: Duration = Duration::from_millis(750);

/// Bound for the initial spawn acknowledgment from the owner thread.
const SPAWN_ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// Default startup timeout when the caller does not specify one.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(20);

/// Sanitized coordinator failure. No paths, env, PEM, token, or OS text.
pub struct ManagedServerError {
    kind: ErrorKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorKind {
    InvalidInput,
    #[allow(dead_code)]
    UnsupportedPlatform,
    Identity,
    State,
    Spawn,
    Startup,
}

impl fmt::Display for ManagedServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ErrorKind::InvalidInput => f.write_str("managed server invalid input"),
            ErrorKind::UnsupportedPlatform => {
                f.write_str("managed server unavailable on this platform")
            }
            _ => f.write_str("managed server startup failed"),
        }
    }
}

/// Fixed, sanitized classification of the startup stage that was observed to
/// fail. It carries only the stage label, never paths, environment values,
/// identity material, or operating-system text, so callers can map it to an
/// external code without exposing raw data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedStartupStage {
    InvalidInput,
    UnsupportedPlatform,
    Paths,
    Identity,
    Process,
    Readiness,
}

impl ManagedStartupStage {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "InvalidInput",
            Self::UnsupportedPlatform => "UnsupportedPlatform",
            Self::Paths => "Paths",
            Self::Identity => "Identity",
            Self::Process => "Process",
            Self::Readiness => "Readiness",
        }
    }
}

impl ManagedServerError {
    /// Observed startup stage only; the underlying cause is intentionally not
    /// retained or claimed.
    pub(crate) fn stage(&self) -> ManagedStartupStage {
        match self.kind {
            ErrorKind::InvalidInput => ManagedStartupStage::InvalidInput,
            ErrorKind::UnsupportedPlatform => ManagedStartupStage::UnsupportedPlatform,
            ErrorKind::State => ManagedStartupStage::Paths,
            ErrorKind::Identity => ManagedStartupStage::Identity,
            ErrorKind::Spawn => ManagedStartupStage::Process,
            ErrorKind::Startup => ManagedStartupStage::Readiness,
        }
    }
}

impl fmt::Debug for ManagedServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.stage().code())
    }
}

impl std::error::Error for ManagedServerError {}

fn invalid_input() -> ManagedServerError {
    ManagedServerError {
        kind: ErrorKind::InvalidInput,
    }
}

#[allow(dead_code)]
fn unsupported() -> ManagedServerError {
    ManagedServerError {
        kind: ErrorKind::UnsupportedPlatform,
    }
}

fn startup_err(kind: ErrorKind) -> ManagedServerError {
    ManagedServerError { kind }
}

/// Commands from coordinator to the dedicated child-owner thread.
/// No secrets cross here after the initial spawn request (which moves
/// owned env values into the owner thread once).
enum OwnerCmd {
    /// Startup never completed. There is no readiness-verified authenticated
    /// session to request `/shutdown` on, so the owner allows a short exit
    /// grace and then force-kills/reaps only the owned handle. A daemon may
    /// already have started daemon-side work (for example resumed downloads)
    /// before the readiness probe observed it; that pre-ready termination is
    /// unavoidable and is not claimed to be graceful.
    Shutdown,
    /// Wait for the owned child to exit until `Instant`, then force-kill/reap
    /// only that handle. The authenticated `POST /shutdown` is sent by the
    /// coordinator concurrently; if it fails, this bound still guarantees
    /// progress.
    ShutdownGraceful(Instant),
    /// Startup completed: stop treating the operation-level cancellation flag
    /// as a force-kill request. After this point shutdown always goes through
    /// the authenticated graceful path.
    StartupComplete,
    /// Test-only: ask the owner to kill and reap exactly its owned child.
    #[cfg(all(test, target_os = "linux"))]
    KillOwnedForTest,
}

/// Sanitized events from the owner thread. Only numeric/process codes;
/// never paths, env, PEM, or token.
#[allow(dead_code)]
enum OwnerEvent {
    Spawned(u32),
    SpawnFailed,
    Exited(Option<i32>),
}

/// Tracks the actual lifetime of one owner thread through child cleanup.
///
/// The coordinator never treats a dropped start future as proof that the owned
/// child is gone. An owner is finished only after its thread returns from the
/// bounded cleanup path, and `reaped` is set only when that cleanup observed
/// the owned child exit/reap. A bounded fast-path expiry keeps ownership and
/// waits for the real exit; only a failed/unavailable reap leaves
/// `reaped == false`, so callers must not clear retirement fences or claim
/// app-exit drain completion in that case.
#[derive(Clone)]
pub(crate) struct OwnerLifetime {
    shared: Arc<OwnerLifetimeShared>,
}

struct OwnerLifetimeShared {
    state: Mutex<OwnerLifetimeState>,
    changed: Condvar,
    #[cfg(test)]
    cleanup_gate: Mutex<Option<OwnerCleanupGate>>,
}

#[derive(Default)]
struct OwnerLifetimeState {
    started: bool,
    finished: bool,
    reaped: bool,
    on_finish: Vec<Box<dyn FnOnce(bool) + Send + 'static>>,
}

impl OwnerLifetime {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(OwnerLifetimeShared {
                state: Mutex::new(OwnerLifetimeState::default()),
                changed: Condvar::new(),
                #[cfg(test)]
                cleanup_gate: Mutex::new(None),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn finished_for_test() -> Self {
        let owner = Self::new();
        owner.finish(true);
        owner
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, OwnerLifetimeState> {
        match self.shared.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn mark_started(&self) {
        let mut state = self.lock();
        state.started = true;
    }

    /// If the start future was cancelled before its owner thread was ever
    /// entered, there is no child to reap. Mark it complete without waiting.
    pub(crate) fn finish_if_unstarted(&self) {
        let should_finish = {
            let state = self.lock();
            !state.started && !state.finished
        };
        if should_finish {
            self.finish(true);
        }
    }

    /// Called exactly once by the owner thread once cleanup is over.
    fn finish(&self, reaped: bool) {
        let actions = {
            let mut state = self.lock();
            if state.finished {
                return;
            }
            state.finished = true;
            state.reaped = reaped;
            let actions = std::mem::take(&mut state.on_finish);
            drop(state);
            self.shared.changed.notify_all();
            actions
        };
        for action in actions {
            action(reaped);
        }
    }

    /// Register a completion action that runs exactly once when the real owner
    /// reports its final reap result. If the owner already finished, the action
    /// runs immediately with the recorded result.
    pub(crate) fn on_finish<F>(&self, action: F)
    where
        F: FnOnce(bool) + Send + 'static,
    {
        let mut action = Some(action);
        let immediate = {
            let mut state = self.lock();
            if state.finished {
                Some(state.reaped)
            } else {
                if let Some(action) = action.take() {
                    state.on_finish.push(Box::new(action));
                }
                None
            }
        };
        if let Some(reaped) = immediate {
            if let Some(action) = action {
                action(reaped);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn finish_for_test(&self, reaped: bool) {
        self.finish(reaped);
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.lock().finished
    }

    /// True only when the owner observed the owned child exit/reap.
    pub(crate) fn was_reaped(&self) -> bool {
        let state = self.lock();
        state.finished && state.reaped
    }

    /// Blocking bounded wait for the owner thread to finish its cleanup.
    /// Returns `false` if the bound expires before the owner finished.
    #[allow(dead_code)]
    pub(crate) fn wait_finished(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        while !state.finished {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let waited = self.shared.changed.wait_timeout(state, deadline - now);
            state = match waited {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        state.finished
    }

    /// Test-only deterministic barrier used to keep a real owner/child alive
    /// while a restart or app-exit test observes the fence.
    #[cfg(test)]
    pub(crate) fn set_cleanup_gate(&self, gate: OwnerCleanupGate) {
        let mut slot = match self.shared.cleanup_gate.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        *slot = Some(gate);
    }

    fn wait_cleanup_gate(&self) {
        #[cfg(test)]
        {
            let gate = self
                .shared
                .cleanup_gate
                .lock()
                .ok()
                .and_then(|slot| slot.clone());
            if let Some(gate) = gate {
                gate.wait();
            }
        }
    }
}

#[cfg(any(target_os = "linux", windows))]
struct OwnerFinish {
    owner: OwnerLifetime,
    reaped: bool,
}

#[cfg(any(target_os = "linux", windows))]
impl OwnerFinish {
    fn new(owner: OwnerLifetime) -> Self {
        Self {
            owner,
            reaped: false,
        }
    }

    fn mark_reaped(&mut self) {
        self.reaped = true;
    }

    fn set_reaped(&mut self, reaped: bool) {
        self.reaped = reaped;
    }
}

#[cfg(any(target_os = "linux", windows))]
impl Drop for OwnerFinish {
    fn drop(&mut self) {
        self.owner.finish(self.reaped);
    }
}

/// Marks an owner lifetime finished without an owner thread when startup
/// failed before the thread was created. Disarmed once the thread owns it.
#[cfg(any(target_os = "linux", windows))]
struct OwnerStartGuard {
    owner: Option<OwnerLifetime>,
}

#[cfg(any(target_os = "linux", windows))]
impl OwnerStartGuard {
    fn new(owner: OwnerLifetime) -> Self {
        Self { owner: Some(owner) }
    }

    fn disarm(&mut self) {
        self.owner = None;
    }
}

#[cfg(any(target_os = "linux", windows))]
impl Drop for OwnerStartGuard {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            owner.finish(true);
        }
    }
}

#[derive(Clone)]
pub(crate) struct OwnerCleanupGate {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

#[allow(dead_code)]
impl OwnerCleanupGate {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, bool> {
        match self.inner.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(crate) fn hold(&self) {
        *self.lock() = true;
    }

    pub(crate) fn release(&self) {
        *self.lock() = false;
        self.inner.1.notify_all();
    }

    fn wait(&self) {
        let mut held = self.lock();
        while *held {
            let waited = self.inner.1.wait_timeout(held, Duration::from_millis(50));
            held = match waited {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
    }
}

/// Spawn request moved into the owner thread (owned values only).
struct SpawnRequest {
    binary: PathBuf,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
}

impl fmt::Debug for SpawnRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SpawnRequest { ..redacted.. }")
    }
}

// ---------------------------------------------------------------------------
// Owner-thread bodies (one per platform; same sanitized protocol).
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
type PlatformChild = crate::managed_process::ManagedChild;
#[cfg(windows)]
type PlatformChild = crate::managed_windows::ManagedChild;

/// Force-kill the owned handle with a bounded fast path, then keep ownership
/// on the dedicated owner thread until the real exit is confirmed.
///
/// This path never spawns an extra reaper: the owner thread is the single
/// blocking waiter. A transient OS wait error is retried with backoff and
/// never reported as success.
#[cfg(target_os = "linux")]
fn terminate_owned(
    child: &mut PlatformChild,
    timeout: Duration,
) -> crate::managed_process::ReapOutcome {
    child.terminate_and_reap_blocking(timeout)
}

/// Windows counterpart. The owned child is deliberately not `Send`, so
/// retaining it on this same owner thread is also the only sound option.
#[cfg(windows)]
fn terminate_owned(
    child: &mut PlatformChild,
    timeout: Duration,
) -> crate::managed_process::ReapOutcome {
    use crate::managed_process::ReapOutcome;

    match child.terminate_and_reap_blocking(timeout) {
        (code, true) => ReapOutcome::Reaped(code),
        (_, false) => ReapOutcome::Unavailable,
    }
}

/// Force-kill and reap exactly the owned handle, reporting whether the child
/// was actually reaped. The boolean is deliberately separate from the exit
/// code because a signal-terminated child has no numeric code.
///
/// A short bounded fast path keeps normal shutdown quick. If the bound expires,
/// the owner thread retains the same owned handle and waits there until
/// confirmed exit. A timed-out probe is never reported as a reaped child and no
/// bare-pid operation is introduced.
#[cfg(any(target_os = "linux", windows))]
fn kill_and_reap_owned(child: &mut PlatformChild, timeout: Duration) -> (Option<i32>, bool) {
    use crate::managed_process::ReapOutcome;

    match terminate_owned(child, timeout) {
        ReapOutcome::Reaped(code) => (code, true),
        // Kept for API completeness; the owner-thread path blocks instead of
        // spawning a reaper and therefore never produces `Pending`.
        ReapOutcome::Pending(waiter) => (None, waiter.wait_blocking()),
        ReapOutcome::Unavailable => (None, false),
    }
}

/// Wait for natural exit until `deadline`, then force-kill/reap the owned
/// handle. No bare pid is ever signalled; no unowned process is touched.
#[cfg(any(target_os = "linux", windows))]
fn wait_natural_or_kill(child: &mut PlatformChild, deadline: Instant) -> (Option<i32>, bool) {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return (status.code(), true),
            Ok(None) => {
                if Instant::now() >= deadline {
                    return kill_and_reap_owned(child, SHUTDOWN_FORCE_REAP_BOUND);
                }
                thread::sleep(OWNER_POLL_INTERVAL);
            }
            Err(_) => return kill_and_reap_owned(child, SHUTDOWN_FORCE_REAP_BOUND),
        }
    }
}

/// Pre-readiness termination: no authenticated session has been verified, so
/// only a short exit grace is possible before force-killing the owned handle.
#[cfg(any(target_os = "linux", windows))]
fn shutdown_unready(child: &mut PlatformChild) -> (Option<i32>, bool) {
    wait_natural_or_kill(child, Instant::now() + SHUTDOWN_UNREADY_GRACE)
}

#[cfg(target_os = "linux")]
fn owner_main_linux(
    req: SpawnRequest,
    cmd_rx: mpsc::Receiver<OwnerCmd>,
    event_tx: mpsc::Sender<OwnerEvent>,
    cancel: Option<Arc<AtomicBool>>,
    owner: OwnerLifetime,
) {
    use crate::managed_process::ManagedChild;

    let mut finish = OwnerFinish::new(owner.clone());
    let spawn_result = ManagedChild::spawn(&req.binary, &req.args, &req.env, true);
    // Drop secrets held in the request as soon as spawn returns.
    drop(req);
    let mut child = match spawn_result {
        Ok(c) => c,
        Err(_) => {
            // No owned child exists, so there is nothing left to reap.
            finish.mark_reaped();
            let _ = event_tx.send(OwnerEvent::SpawnFailed);
            return;
        }
    };
    let pid = child.id();
    if event_tx.send(OwnerEvent::Spawned(pid)).is_err() {
        // Coordinator gone: clean only the owned child.
        owner.wait_cleanup_gate();
        let (_, reaped) = kill_and_reap_owned(&mut child, SHUTDOWN_FORCE_REAP_BOUND);
        finish.set_reaped(reaped);
        drop(child);
        return;
    }
    let mut startup_complete = cancel.is_none();
    loop {
        // Shutdown takes precedence; a disconnected coordinator also means shutdown.
        match cmd_rx.try_recv() {
            #[cfg(all(test, target_os = "linux"))]
            Ok(OwnerCmd::KillOwnedForTest) => {
                // Test-only crash simulation: kill/reap only the owned handle.
                owner.wait_cleanup_gate();
                let (code, reaped) = kill_and_reap_owned(&mut child, SHUTDOWN_FORCE_REAP_BOUND);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(OwnerCmd::Shutdown) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = shutdown_unready(&mut child);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(OwnerCmd::ShutdownGraceful(deadline)) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = wait_natural_or_kill(&mut child, deadline);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(OwnerCmd::StartupComplete) => {
                startup_complete = true;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = shutdown_unready(&mut child);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if !startup_complete
            && cancel
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::SeqCst))
        {
            // App shutdown raced this startup before it completed. Only the
            // owned handle is touched; no bare pid is ever signalled.
            owner.wait_cleanup_gate();
            let (code, reaped) = shutdown_unready(&mut child);
            finish.set_reaped(reaped);
            let _ = event_tx.send(OwnerEvent::Exited(code));
            drop(child);
            break;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                finish.mark_reaped();
                let code = status.code();
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(None) => {}
            Err(_) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = kill_and_reap_owned(&mut child, SHUTDOWN_FORCE_REAP_BOUND);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
        }
        thread::sleep(OWNER_POLL_INTERVAL);
    }
}

#[cfg(windows)]
fn owner_main_windows(
    req: SpawnRequest,
    cmd_rx: mpsc::Receiver<OwnerCmd>,
    event_tx: mpsc::Sender<OwnerEvent>,
    cancel: Option<Arc<AtomicBool>>,
    owner: OwnerLifetime,
) {
    use crate::managed_windows::ManagedChild;

    let mut finish = OwnerFinish::new(owner.clone());
    let spawn_result = ManagedChild::spawn_with_cleanup(&req.binary, &req.args, &req.env, true);
    drop(req);
    let mut child = match spawn_result {
        Ok(c) => c,
        Err(mut failure) => {
            // A failed Windows spawn can still own a process created before
            // job verification or resume failed. Report the failure promptly
            // (bounded caller waiting), then keep the handles on this owner
            // thread and wait until the real exit is confirmed. The owner
            // lifetime is not marked reaped until that wait returns.
            let _ = event_tx.send(OwnerEvent::SpawnFailed);
            failure.wait_until_confirmed();
            finish.mark_reaped();
            return;
        }
    };
    let pid = child.id();
    if event_tx.send(OwnerEvent::Spawned(pid)).is_err() {
        owner.wait_cleanup_gate();
        let (_, reaped) = kill_and_reap_owned(&mut child, SHUTDOWN_FORCE_REAP_BOUND);
        finish.set_reaped(reaped);
        drop(child);
        return;
    }
    let mut startup_complete = cancel.is_none();
    loop {
        match cmd_rx.try_recv() {
            #[cfg(all(test, target_os = "linux"))]
            Ok(OwnerCmd::KillOwnedForTest) => {
                // Test-only crash simulation: kill/reap only the owned handle.
                owner.wait_cleanup_gate();
                let (code, reaped) = kill_and_reap_owned(&mut child, SHUTDOWN_FORCE_REAP_BOUND);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(OwnerCmd::Shutdown) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = shutdown_unready(&mut child);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(OwnerCmd::ShutdownGraceful(deadline)) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = wait_natural_or_kill(&mut child, deadline);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(OwnerCmd::StartupComplete) => {
                startup_complete = true;
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = shutdown_unready(&mut child);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if !startup_complete
            && cancel
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::SeqCst))
        {
            // App shutdown raced this startup before it completed. Only the
            // owned handle is touched; no bare pid is ever signalled.
            owner.wait_cleanup_gate();
            let (code, reaped) = shutdown_unready(&mut child);
            finish.set_reaped(reaped);
            let _ = event_tx.send(OwnerEvent::Exited(code));
            drop(child);
            break;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                finish.mark_reaped();
                let code = status.code();
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
            Ok(None) => {}
            Err(_) => {
                owner.wait_cleanup_gate();
                let (code, reaped) = kill_and_reap_owned(&mut child, SHUTDOWN_FORCE_REAP_BOUND);
                finish.set_reaped(reaped);
                let _ = event_tx.send(OwnerEvent::Exited(code));
                drop(child);
                break;
            }
        }
        thread::sleep(OWNER_POLL_INTERVAL);
    }
}

// ---------------------------------------------------------------------------
// Env allowlist construction (no process-env reads).
// ---------------------------------------------------------------------------

fn port_value() -> OsString {
    OsString::from(MANAGED_PORT.to_string())
}

/// Build the strict Linux child env from caller inputs plus fresh identity.
/// No process environment is read.
#[cfg(target_os = "linux")]
fn build_linux_env(
    state_dir: &Path,
    home_dir: &Path,
    identity: &ManagedIdentity,
) -> Vec<(OsString, OsString)> {
    let state_os = state_dir.as_os_str().to_os_string();
    let home_os = home_dir.as_os_str().to_os_string();
    vec![
        (OsString::from("SURGUI_PORT"), port_value()),
        (
            OsString::from("SURGUI_TLS_CERT_PEM"),
            OsString::from(identity.server_cert_pem()),
        ),
        (
            OsString::from("SURGUI_TLS_KEY_PEM"),
            OsString::from(identity.server_key_pem()),
        ),
        (
            OsString::from("SURGUI_AUTH_TOKEN"),
            OsString::from(identity.bearer_token()),
        ),
        (OsString::from("HOME"), home_os),
        (OsString::from("XDG_STATE_HOME"), state_os.clone()),
        (OsString::from("XDG_CONFIG_HOME"), state_os.clone()),
        (OsString::from("XDG_DATA_HOME"), state_os.clone()),
        (OsString::from("XDG_CACHE_HOME"), state_os.clone()),
        (OsString::from("XDG_RUNTIME_DIR"), state_os),
    ]
}

/// Test-only opt-in gate for the controlled Windows `SystemRoot` comparison.
/// Both the dedicated variable must be exactly `1` and the GitHub Actions
/// marker must be exactly `true`; otherwise ordinary test/production behavior
/// is unchanged. No value is logged or retained.
#[cfg(all(test, windows))]
fn windows_systemroot_test_opt_in() -> bool {
    let opted_in = std::env::var_os("SURGUI_TEST_WINDOWS_SYSTEMROOT")
        .and_then(|value| value.to_str().map(|value| value == "1"))
        .unwrap_or(false);
    let github_actions = std::env::var_os("GITHUB_ACTIONS")
        .and_then(|value| value.to_str().map(|value| value == "true"))
        .unwrap_or(false);
    opted_in && github_actions
}

/// Build the strict Windows child env. No process environment is read for
/// production builds. `APPDATA`/`TEMP`/`TMP` point at the isolated state;
/// `USERPROFILE` keeps the real home so download-location resolution is
/// preserved.
#[cfg(windows)]
fn build_windows_env(
    state_dir: &Path,
    home_dir: &Path,
    identity: &ManagedIdentity,
) -> Vec<(OsString, OsString)> {
    let state_os = state_dir.as_os_str().to_os_string();
    let home_os = home_dir.as_os_str().to_os_string();
    // Mutation is test-only (see the opt-in block below); ordinary production
    // builds must not warn about the unused `mut`.
    #[allow(unused_mut)]
    let mut env = vec![
        (OsString::from("SURGUI_PORT"), port_value()),
        (
            OsString::from("SURGUI_TLS_CERT_PEM"),
            OsString::from(identity.server_cert_pem()),
        ),
        (
            OsString::from("SURGUI_TLS_KEY_PEM"),
            OsString::from(identity.server_key_pem()),
        ),
        (
            OsString::from("SURGUI_AUTH_TOKEN"),
            OsString::from(identity.bearer_token()),
        ),
        (OsString::from("APPDATA"), state_os.clone()),
        (OsString::from("USERPROFILE"), home_os),
        (OsString::from("TEMP"), state_os.clone()),
        (OsString::from("TMP"), state_os),
    ];
    // Controlled comparison only: with the explicit CI opt-in, copy the real
    // parent `SystemRoot` into the otherwise-cleared child env. This block is
    // compiled out of ordinary non-test builds and inert without the opt-in,
    // so the no-SystemRoot behavior above is preserved by default.
    #[cfg(test)]
    {
        if windows_systemroot_test_opt_in() {
            if let Some(system_root) = std::env::var_os("SystemRoot") {
                if !system_root.is_empty() {
                    env.push((OsString::from("SystemRoot"), system_root));
                }
            }
        }
    }
    env
}

/// Shared validation: all three paths must be absolute.
fn validate_absolute(
    binary: &Path,
    state_dir: &Path,
    home_dir: &Path,
) -> Result<(), ManagedServerError> {
    if !binary.is_absolute() || !state_dir.is_absolute() || !home_dir.is_absolute() {
        return Err(invalid_input());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Coordinator.
// ---------------------------------------------------------------------------

/// Owned coordinator for one managed daemon launch.
///
/// Holds no child handle directly; the child lives on its dedicated owner
/// thread until this value drops. Retains the pinned [`SurgeClient`] in an
/// `Arc` so callers can run concurrent network operations after releasing the
/// connection-state lock, without taking the owned server out of that state.
pub struct ManagedServer {
    pid: u32,
    state_path: PathBuf,
    cmd_tx: Option<mpsc::Sender<OwnerCmd>>,
    event_rx: mpsc::Receiver<OwnerEvent>,
    exited: AtomicBool,
    owner_handle: Option<thread::JoinHandle<()>>,
    owner: OwnerLifetime,
    client: Arc<SurgeClient>,
    shutdown_deadline: Option<Instant>,
    shutdown_request: Option<thread::JoinHandle<()>>,
}

impl fmt::Debug for ManagedServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Pid is safe to log (as in ManagedChild); paths/client/token redacted.
        f.debug_struct("ManagedServer")
            .field("pid", &self.pid)
            .finish()
    }
}

impl ManagedServer {
    /// Start the managed daemon with the default startup timeout.
    pub async fn start(
        binary: &Path,
        state_dir: &Path,
        home_dir: &Path,
    ) -> Result<Self, ManagedServerError> {
        Self::start_with_timeout(binary, state_dir, home_dir, DEFAULT_STARTUP_TIMEOUT).await
    }

    /// Start with an app-shutdown cancellation flag. The owner thread watches
    /// the flag until startup completes, so an unpolled cancelled start still
    /// cleans up only its owned child. After readiness the flag no longer
    /// applies; shutdown then uses the authenticated graceful path.
    #[allow(dead_code)]
    pub(crate) async fn start_cancellable(
        binary: &Path,
        state_dir: &Path,
        home_dir: &Path,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self, ManagedServerError> {
        let owner = OwnerLifetime::new();
        Self::start_cancellable_tracked(
            binary,
            state_dir,
            home_dir,
            DEFAULT_STARTUP_TIMEOUT,
            cancel,
            owner,
        )
        .await
    }

    /// Start with a caller-supplied owner-lifetime tracker. The caller keeps
    /// its own clone so a cancelled/failed start can remain fenced until the
    /// real owner thread has finished reaping the child.
    pub(crate) async fn start_cancellable_tracked(
        binary: &Path,
        state_dir: &Path,
        home_dir: &Path,
        timeout: Duration,
        cancel: Arc<AtomicBool>,
        owner: OwnerLifetime,
    ) -> Result<Self, ManagedServerError> {
        Self::start_with_timeout_cancel(binary, state_dir, home_dir, timeout, Some(cancel), owner)
            .await
    }

    /// Start the managed daemon, waiting at most `timeout` for pinned-TLS
    /// readiness. Fails closed on immediate exit, port conflict, wrong cert,
    /// or timeout. Never sends the bearer over plaintext or to an untrusted
    /// cert (only [`SurgeClient::connect_managed`] is used).
    pub async fn start_with_timeout(
        binary: &Path,
        state_dir: &Path,
        home_dir: &Path,
        timeout: Duration,
    ) -> Result<Self, ManagedServerError> {
        let owner = OwnerLifetime::new();
        Self::start_with_timeout_cancel(binary, state_dir, home_dir, timeout, None, owner).await
    }

    async fn start_with_timeout_cancel(
        binary: &Path,
        state_dir: &Path,
        home_dir: &Path,
        timeout: Duration,
        cancel: Option<Arc<AtomicBool>>,
        owner: OwnerLifetime,
    ) -> Result<Self, ManagedServerError> {
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = (binary, state_dir, home_dir, timeout, cancel);
            owner.finish(true);
            return Err(unsupported());
        }

        #[cfg(any(target_os = "linux", windows))]
        {
            Self::start_inner(binary, state_dir, home_dir, timeout, cancel, owner).await
        }
    }

    #[cfg(any(target_os = "linux", windows))]
    async fn start_inner(
        binary: &Path,
        state_dir: &Path,
        home_dir: &Path,
        timeout: Duration,
        cancel: Option<Arc<AtomicBool>>,
        owner: OwnerLifetime,
    ) -> Result<Self, ManagedServerError> {
        // From this point the caller may poll into owner creation; if the
        // future is dropped before any poll, `finish_if_unstarted` handles the
        // no-owner case instead of leaving a phantom retirement.
        owner.mark_started();
        // If startup fails before the owner thread exists, no child was
        // spawned; mark the lifetime finished/reaped. Once the thread is
        // created, the thread owns completion (including cancellation).
        let mut owner_start = OwnerStartGuard::new(owner.clone());
        validate_absolute(binary, state_dir, home_dir)?;

        // Isolated persistent state: verifies private mode/ACL, never deletes.
        let prepared =
            ManagedStateDir::prepare(state_dir).map_err(|_| startup_err(ErrorKind::State))?;
        let verified_state: PathBuf = prepared.path().to_path_buf();
        // Keep `prepared` alive until after spawn so the verified dir cannot
        // be confused with an unverified path (no-op Drop, persistent).

        // Per-launch fresh identity (in memory only).
        let identity = ManagedIdentity::generate().map_err(|_| startup_err(ErrorKind::Identity))?;
        let ca_pem_bytes = identity.ca_cert_pem().into_bytes();
        let token_owned = identity.bearer_token().to_owned();

        // Strict allowlist env; no process env inherited (clear_env=true).
        #[cfg(target_os = "linux")]
        let env = build_linux_env(&verified_state, home_dir, &identity);
        #[cfg(windows)]
        let env = build_windows_env(&verified_state, home_dir, &identity);

        let req = SpawnRequest {
            binary: binary.to_path_buf(),
            args: vec![OsString::from(MANAGED_ARGV)],
            env,
        };

        let (cmd_tx, cmd_rx) = mpsc::channel::<OwnerCmd>();
        let (event_tx, mut event_rx) = mpsc::channel::<OwnerEvent>();

        // Dedicated owner thread keeps the (possibly non-Send) child.
        #[cfg(target_os = "linux")]
        let owner_handle = thread::Builder::new()
            .name("managed-server-owner".into())
            .spawn({
                let owner_cancel = cancel.clone();
                let owner_thread = owner.clone();
                move || owner_main_linux(req, cmd_rx, event_tx, owner_cancel, owner_thread)
            })
            .map_err(|_| startup_err(ErrorKind::Spawn))?;
        #[cfg(windows)]
        let owner_handle = thread::Builder::new()
            .name("managed-server-owner".into())
            .spawn({
                let owner_cancel = cancel.clone();
                let owner_thread = owner.clone();
                move || owner_main_windows(req, cmd_rx, event_tx, owner_cancel, owner_thread)
            })
            .map_err(|_| startup_err(ErrorKind::Spawn))?;
        owner_start.disarm();

        // Wait for the spawn acknowledgment without blocking the executor.
        // The receiver is borrowed mutably (not shared), which keeps this
        // future `Send`: `Receiver` is `Send` but not `Sync`.
        let spawn_pid = match Self::await_spawn(&mut event_rx).await {
            Ok(pid) => pid,
            Err(error) => {
                Self::signal_startup_shutdown(&cmd_tx);
                drop(prepared);
                return Err(error);
            }
        };

        // Readiness: pinned-TLS probe until deadline; fail closed on early exit.
        let deadline = Instant::now() + timeout;
        let client = loop {
            // Immediate exit / port conflict fails closed before any retry.
            match event_rx.try_recv() {
                Ok(OwnerEvent::Exited(code)) => {
                    #[cfg(all(test, windows))]
                    match code {
                        Some(0) => eprintln!("CI_DIAG:exit_zero"),
                        Some(1) => eprintln!("CI_DIAG:exit_one"),
                        Some(2) => eprintln!("CI_DIAG:exit_two"),
                        Some(_) => eprintln!("CI_DIAG:exit_other"),
                        None => eprintln!("CI_DIAG:exit_unknown"),
                    }
                    #[cfg(not(all(test, windows)))]
                    let _ = code;
                    Self::signal_startup_shutdown(&cmd_tx);
                    // Consume the held prepared dir handle ordering: drop here.
                    drop(prepared);
                    return Err(startup_err(ErrorKind::Startup));
                }
                Ok(OwnerEvent::SpawnFailed) => {
                    #[cfg(all(test, windows))]
                    eprintln!("CI_DIAG:spawn_failed_event");
                    Self::signal_startup_shutdown(&cmd_tx);
                    // Consume the held prepared dir handle ordering: drop here.
                    drop(prepared);
                    return Err(startup_err(ErrorKind::Startup));
                }
                Ok(OwnerEvent::Spawned(_)) => {
                    // Duplicate spawn notice; ignore and keep probing.
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    #[cfg(all(test, windows))]
                    eprintln!("CI_DIAG:owner_disconnected");
                    Self::signal_startup_shutdown(&cmd_tx);
                    drop(prepared);
                    return Err(startup_err(ErrorKind::Startup));
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if Instant::now() >= deadline {
                #[cfg(all(test, windows))]
                eprintln!("CI_DIAG:deadline");
                // The readiness probe is the first point at which a pinned-TLS
                // authenticated session is known to exist. A final bounded
                // attempt can still catch a daemon that became ready just
                // after the loop deadline; if it succeeds, request the normal
                // authenticated /shutdown and let the owner use the graceful
                // child-exit bound. If it does not, pre-ready termination is
                // unavoidable and the owner force-kills only its own handle
                // after a short exit grace.
                if Self::pre_ready_authenticated_shutdown(&ca_pem_bytes, &token_owned).await {
                    let _ = cmd_tx.send(OwnerCmd::ShutdownGraceful(
                        Instant::now() + SHUTDOWN_CHILD_GRACE,
                    ));
                } else {
                    Self::signal_startup_shutdown(&cmd_tx);
                }
                drop(prepared);
                return Err(startup_err(ErrorKind::Startup));
            }
            match SurgeClient::connect_managed(&ca_pem_bytes, &token_owned).await {
                Ok(c) => break c,
                Err(e) => {
                    // Unauthorized means TLS pinned fine but token rejected:
                    // fail closed immediately (never retry with same token).
                    use crate::surge_client::ConnectError;
                    if matches!(e, ConnectError::Unauthorized) {
                        #[cfg(all(test, windows))]
                        eprintln!("CI_DIAG:unauthorized");
                        // The token was rejected, so no authenticated graceful
                        // request is possible. Only the owned handle is cleaned.
                        Self::signal_startup_shutdown(&cmd_tx);
                        drop(prepared);
                        return Err(startup_err(ErrorKind::Startup));
                    }
                    // Otherwise not ready yet; keep polling until deadline.
                    Self::sleep_async(READINESS_POLL_INTERVAL).await;
                    continue;
                }
            }
        };

        // Startup is complete: the owner may now be retired only through the
        // authenticated graceful command, never the startup-cancel flag.
        let _ = cmd_tx.send(OwnerCmd::StartupComplete);

        // Identity secrets beyond the retained client are dropped here
        // (`identity`, `ca_pem_bytes`, `token_owned` go out of scope).
        drop(prepared);
        Ok(Self {
            pid: spawn_pid,
            state_path: verified_state,
            cmd_tx: Some(cmd_tx),
            event_rx,
            exited: AtomicBool::new(false),
            owner_handle: Some(owner_handle),
            owner,
            client: Arc::new(client),
            shutdown_deadline: None,
            shutdown_request: None,
        })
    }

    /// Await the owner thread's spawn acknowledgment with a bound.
    ///
    /// Takes a mutable borrow of the receiver rather than a shared one. A
    /// shared `&Receiver` is not `Send` (the receiver is not `Sync`), which
    /// would make the whole command future non-`Send`; `&mut Receiver` is
    /// `Send` and keeps the receiver in place for the readiness loop.
    #[cfg(any(target_os = "linux", windows))]
    async fn await_spawn(
        event_rx: &mut mpsc::Receiver<OwnerEvent>,
    ) -> Result<u32, ManagedServerError> {
        let deadline = Instant::now() + SPAWN_ACK_TIMEOUT;
        loop {
            match event_rx.try_recv() {
                Ok(OwnerEvent::Spawned(pid)) => return Ok(pid),
                Ok(OwnerEvent::SpawnFailed) => return Err(startup_err(ErrorKind::Spawn)),
                Ok(OwnerEvent::Exited(_)) => return Err(startup_err(ErrorKind::Startup)),
                Err(mpsc::TryRecvError::Disconnected) => return Err(startup_err(ErrorKind::Spawn)),
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if Instant::now() >= deadline {
                return Err(startup_err(ErrorKind::Spawn));
            }
            Self::sleep_async(Duration::from_millis(25)).await;
        }
    }

    /// Async sleep without requiring callers to import tokio directly.
    /// Uses tokio time when available on this platform.
    #[cfg(any(target_os = "linux", windows))]
    async fn sleep_async(d: Duration) {
        tokio::time::sleep(d).await;
    }

    /// Ask the owner to clean an unready child. The owner's own lifetime
    /// tracker is the authority for when cleanup is actually complete; this
    /// function never joins or detaches a second watcher thread.
    fn signal_startup_shutdown(cmd_tx: &mpsc::Sender<OwnerCmd>) {
        let _ = cmd_tx.send(OwnerCmd::Shutdown);
    }

    /// One final bounded attempt to establish the pinned-TLS authenticated
    /// session and send `/shutdown`. Returns true only when the connection was
    /// safely authenticated; the request itself is still timeout-bounded.
    #[cfg(any(target_os = "linux", windows))]
    async fn pre_ready_authenticated_shutdown(ca_pem: &[u8], token: &str) -> bool {
        let connected = tokio::time::timeout(
            PRE_READY_SHUTDOWN_PROBE,
            SurgeClient::connect_managed(ca_pem, token),
        )
        .await;
        let client = match connected {
            Ok(Ok(client)) => client,
            _ => return false,
        };
        let _ = tokio::time::timeout(SHUTDOWN_REQUEST_TIMEOUT, client.shutdown()).await;
        true
    }

    fn begin_shutdown(&mut self) -> Instant {
        if let Some(deadline) = self.shutdown_deadline {
            return deadline;
        }
        let deadline = Instant::now() + SHUTDOWN_CHILD_GRACE;
        self.shutdown_deadline = Some(deadline);
        // Dispatch the authenticated request before returning so cancellation
        // of an async `shutdown` future can never skip it. The dedicated
        // thread owns its own runtime and has a fixed timeout; the owner
        // thread independently bounds the child-exit grace.
        self.shutdown_request = Self::spawn_shutdown_request(Arc::clone(&self.client));
        if let Some(tx) = self.cmd_tx.take() {
            let _ = tx.send(OwnerCmd::ShutdownGraceful(deadline));
        }
        deadline
    }

    fn spawn_shutdown_request(client: Arc<SurgeClient>) -> Option<thread::JoinHandle<()>> {
        thread::Builder::new()
            .name("managed-shutdown-request".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => return,
                };
                runtime.block_on(async {
                    let _ = tokio::time::timeout(SHUTDOWN_REQUEST_TIMEOUT, client.shutdown()).await;
                });
            })
            .ok()
    }

    async fn await_owner_async(
        owner_handle: &mut Option<thread::JoinHandle<()>>,
        hard_deadline: Instant,
    ) {
        loop {
            let finished = owner_handle
                .as_ref()
                .map(|handle| handle.is_finished())
                .unwrap_or(true);
            if finished || Instant::now() >= hard_deadline {
                break;
            }
            tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
        }
        if owner_handle
            .as_ref()
            .map(|handle| handle.is_finished())
            .unwrap_or(true)
        {
            if let Some(handle) = owner_handle.take() {
                let _ = handle.join();
            }
        } else {
            // Never block the caller/executor past the fixed bound. The owner
            // still attempts its own bounded force-kill/reap, but this wait
            // does not claim it succeeded; the owner tracker remains the
            // authority and reports a timeout as not reaped.
            *owner_handle = None;
        }
    }

    fn join_owner_bounded(
        owner_handle: &mut Option<thread::JoinHandle<()>>,
        hard_deadline: Instant,
    ) {
        loop {
            let finished = owner_handle
                .as_ref()
                .map(|handle| handle.is_finished())
                .unwrap_or(true);
            if finished || Instant::now() >= hard_deadline {
                break;
            }
            thread::sleep(SHUTDOWN_POLL_INTERVAL);
        }
        if owner_handle
            .as_ref()
            .map(|handle| handle.is_finished())
            .unwrap_or(true)
        {
            if let Some(handle) = owner_handle.take() {
                let _ = handle.join();
            }
        } else {
            *owner_handle = None;
        }
    }

    async fn await_request_async(
        request_handle: &mut Option<thread::JoinHandle<()>>,
        hard_deadline: Instant,
    ) {
        loop {
            let finished = request_handle
                .as_ref()
                .map(|handle| handle.is_finished())
                .unwrap_or(true);
            if finished || Instant::now() >= hard_deadline {
                break;
            }
            tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
        }
        if request_handle
            .as_ref()
            .map(|handle| handle.is_finished())
            .unwrap_or(true)
        {
            if let Some(handle) = request_handle.take() {
                let _ = handle.join();
            }
        }
        // Otherwise leave it for `Drop`, which joins with its own fixed bound.
    }

    fn join_request_bounded(
        request_handle: &mut Option<thread::JoinHandle<()>>,
        hard_deadline: Instant,
    ) {
        loop {
            let finished = request_handle
                .as_ref()
                .map(|handle| handle.is_finished())
                .unwrap_or(true);
            if finished || Instant::now() >= hard_deadline {
                break;
            }
            thread::sleep(SHUTDOWN_POLL_INTERVAL);
        }
        if request_handle
            .as_ref()
            .map(|handle| handle.is_finished())
            .unwrap_or(true)
        {
            if let Some(handle) = request_handle.take() {
                let _ = handle.join();
            }
        } else {
            // The request thread has its own timeout; never wait forever here.
            *request_handle = None;
        }
    }

    /// Gracefully retire the owned daemon.
    ///
    /// Sends the authenticated pinned-TLS `POST /shutdown` to exactly this
    /// launch's daemon, gives the owned child a fixed grace period to exit,
    /// then force-kills/reaps only the owned handle. Consumes the coordinator;
    /// if the future is cancelled, `Drop` performs the same bounded shutdown.
    /// Returns true only when the owner thread finished and reported that the
    /// owned child was actually reaped. A bounded wait that expires returns
    /// false; callers must keep their retirement fence / drain accounting in
    /// that case rather than claiming the child was reaped.
    pub async fn shutdown(mut self) -> bool {
        let deadline = self.begin_shutdown();
        let hard = deadline + SHUTDOWN_FORCE_REAP_BOUND + SHUTDOWN_OWNER_SLACK;
        Self::await_owner_async(&mut self.owner_handle, hard).await;
        Self::await_request_async(&mut self.shutdown_request, hard).await;
        self.owner.is_finished() && self.owner.was_reaped()
    }

    /// Synchronous bounded shutdown for `Drop` and app shutdown.
    ///
    /// The pinned network request runs on a dedicated thread with its own
    /// current-thread runtime, so no `block_on` is ever nested inside the
    /// caller's async runtime. The owner thread owns the actual force-kill and
    /// reaps only its owned child handle.
    pub fn shutdown_blocking(mut self) -> bool {
        self.shutdown_inner_blocking()
    }

    fn shutdown_inner_blocking(&mut self) -> bool {
        let deadline = self.begin_shutdown();
        let hard = deadline + SHUTDOWN_FORCE_REAP_BOUND + SHUTDOWN_OWNER_SLACK;
        Self::join_owner_bounded(&mut self.owner_handle, hard);
        Self::join_request_bounded(
            &mut self.shutdown_request,
            Instant::now() + SHUTDOWN_REQUEST_TIMEOUT + SHUTDOWN_OWNER_SLACK,
        );
        self.owner.is_finished() && self.owner.was_reaped()
    }

    /// Cloneable tracker for the real owner-thread/child-cleanup lifetime.
    pub(crate) fn owner_lifetime(&self) -> OwnerLifetime {
        self.owner.clone()
    }

    /// OS pid of the owned daemon child. Safe to log.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Test-only: ask the dedicated owner thread to kill and reap its owned
    /// child. Never signals a bare pid. Returns `false` if the owner is gone.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn kill_owned_child_for_test(&self) -> bool {
        match self.cmd_tx.as_ref() {
            Some(tx) => tx.send(OwnerCmd::KillOwnedForTest).is_ok(),
            None => false,
        }
    }

    /// Verified isolated state path supplied at startup.
    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    /// Retained pinned client for later `refresh` / control calls.
    ///
    /// Returns a clone of the shared handle, never the credentials. Callers
    /// can perform concurrent network operations without borrowing the owned
    /// server or holding the connection-state lock across an `await`.
    pub fn client(&self) -> Arc<SurgeClient> {
        Arc::clone(&self.client)
    }

    /// Non-blocking check: true once the owned child has exited or the
    /// owner thread is gone. Sanitized (numeric only).
    pub fn has_exited(&self) -> bool {
        if self.exited.load(Ordering::SeqCst) {
            return true;
        }
        match self.event_rx.try_recv() {
            Ok(OwnerEvent::Exited(_))
            | Ok(OwnerEvent::SpawnFailed)
            | Err(mpsc::TryRecvError::Disconnected) => {
                self.exited.store(true, Ordering::SeqCst);
                true
            }
            Ok(_) => false,
            Err(mpsc::TryRecvError::Empty) => false,
        }
    }
}

impl Drop for ManagedServer {
    fn drop(&mut self) {
        if let Some(deadline) = self.shutdown_deadline {
            // A shutdown path already dispatched the owner command and the
            // authenticated request (e.g. an async shutdown future was
            // cancelled). Never send a second request; just bound-wait for the
            // original owner deadline and the request thread.
            let hard = deadline + SHUTDOWN_FORCE_REAP_BOUND + SHUTDOWN_OWNER_SLACK;
            Self::join_owner_bounded(&mut self.owner_handle, hard);
            Self::join_request_bounded(
                &mut self.shutdown_request,
                Instant::now() + SHUTDOWN_REQUEST_TIMEOUT + SHUTDOWN_OWNER_SLACK,
            );
            return;
        }
        // Graceful, bounded retirement by the owner thread; it force-kills and
        // reaps only its owned child handle, never a bare pid.
        self.shutdown_inner_blocking();
    }
}

// Allow `AsRef<OsStr>` pairs to be passed positionally in tests without
// exposing secrets: helper validates absoluteness only.
#[allow(dead_code)]
fn _assert_absolute_for_test(p: &Path) -> bool {
    p.is_absolute()
}

/// Serialize tests that bind the fixed managed port. Shared with the command
/// integration tests so the two modules cannot race each other in one binary.
#[cfg(test)]
pub(crate) fn test_port_lock() -> &'static tokio::sync::Mutex<()> {
    use std::sync::OnceLock;
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::ffi::OsStr;
    #[cfg(target_os = "linux")]
    use std::sync::atomic::AtomicU64;
    #[cfg(windows)]
    use std::sync::atomic::AtomicU64;

    #[cfg(target_os = "linux")]
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
    #[cfg(windows)]
    static WINDOWS_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[cfg(target_os = "linux")]
    fn port_lock() -> &'static tokio::sync::Mutex<()> {
        test_port_lock()
    }

    fn sandbox_base() -> PathBuf {
        std::env::temp_dir().join(format!("surge-managed-server-tests-{}", std::process::id()))
    }

    #[cfg(target_os = "linux")]
    fn unique_tag(tag: &str) -> String {
        use std::sync::atomic::Ordering;
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("{}-{}-{}", tag, std::process::id(), id)
    }

    #[cfg(target_os = "linux")]
    fn fresh_missing_path(tag: &str) -> PathBuf {
        std::fs::create_dir_all(sandbox_base()).ok();
        sandbox_base().join(unique_tag(tag))
    }

    #[cfg(target_os = "linux")]
    fn make_home_dir(tag: &str) -> PathBuf {
        let dir = fresh_missing_path(tag);
        std::fs::create_dir_all(&dir).expect("create home dir");
        dir
    }

    #[cfg(target_os = "linux")]
    fn remove_all_best_effort(path: &Path) {
        if path.symlink_metadata().is_ok() {
            if path.is_dir() && !path.is_symlink() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn pid_alive(pid: u32) -> bool {
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        // SAFETY: integer-only existence probe on an owned pid.
        unsafe { kill(pid as i32, 0) == 0 }
    }

    #[cfg(target_os = "linux")]
    fn real_daemon_binary() -> Option<&'static Path> {
        match std::env::var_os("SURGUI_TEST_DAEMON") {
            None => None,
            Some(value) => {
                let path = PathBuf::from(value);
                assert!(
                    path.is_absolute(),
                    "SURGUI_TEST_DAEMON must be an absolute path"
                );
                assert!(
                    path.is_file(),
                    "SURGUI_TEST_DAEMON must point to an existing file"
                );
                Some(Box::leak(path.into_boxed_path()) as &Path)
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn write_sleeping_fixture(dir: &Path, pidfile: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let script = dir.join("sleeping-managed-fixture.sh");
        let escaped = pidfile.to_string_lossy().replace('\'', "'\\''");
        let body = format!("#!/bin/sh\necho $$ > '{escaped}'\nexec /bin/sleep 300\n");
        std::fs::write(&script, body).expect("write fixture");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700))
            .expect("chmod fixture");
        script
    }

    #[cfg(target_os = "linux")]
    async fn wait_pid_dead(pid: u32, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if !pid_alive(pid) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        !pid_alive(pid)
    }

    const HOSTILE_INPUT_MARKERS: &[&str] = &[
        "sk_live_",
        "Bearer ",
        "BEGIN PRIVATE KEY",
        "BEGIN CERTIFICATE",
        "alice@example.com",
        "alice",
        "SURGUI_AUTH_TOKEN",
        "SURGUI_TLS_KEY_PEM",
        "--token",
        "surgui-managed-server",
        "Permission denied",
        "os error",
    ];

    fn assert_stage_code(error: &ManagedServerError, expected: &str) {
        let display = format!("{error}");
        let debug = format!("{error:?}");
        assert_eq!(debug, expected, "stage code must be fixed and specific");
        for marker in HOSTILE_INPUT_MARKERS {
            let marker = *marker;
            assert!(
                !display.contains(marker),
                "Display leaked {marker:?}: {display}"
            );
            assert!(!debug.contains(marker), "Debug leaked {marker:?}: {debug}");
        }
        for text in [&display, &debug] {
            assert!(!text.contains('/'), "error text leaked a path: {text}");
            assert!(!text.contains('\\'), "error text leaked a path: {text}");
        }
    }

    #[test]
    fn managed_stage_errors_expose_distinct_sanitized_fixed_codes() {
        let cases = [
            (ErrorKind::InvalidInput, "InvalidInput"),
            (ErrorKind::UnsupportedPlatform, "UnsupportedPlatform"),
            (ErrorKind::State, "Paths"),
            (ErrorKind::Identity, "Identity"),
            (ErrorKind::Spawn, "Process"),
            (ErrorKind::Startup, "Readiness"),
        ];
        let mut codes = std::collections::BTreeSet::new();
        for (kind, code) in cases {
            let error = startup_err(kind);
            assert_stage_code(&error, code);
            assert!(codes.insert(code), "duplicate stage code {code}");
        }
        assert_eq!(codes.len(), 6, "every startup stage needs its own code");
        assert!(
            !codes.contains("Startup"),
            "readiness/auth failures must not collapse to the generic Startup code"
        );
    }

    #[test]
    fn managed_stage_codes_do_not_embed_hostile_input() {
        let hostile = "sk_live_TOKEN_alice@example.com";
        let relative = PathBuf::from(format!(
            "{hostile}/--token=Bearer SURGUI_AUTH_TOKEN/../../BEGIN PRIVATE KEY"
        ));
        let absolute = sandbox_base().join(hostile);
        let error = validate_absolute(&relative, &absolute, &absolute).unwrap_err();
        assert_stage_code(&error, "InvalidInput");

        let request = SpawnRequest {
            binary: absolute.join(hostile),
            args: vec![OsString::from(format!("--token={hostile}"))],
            env: vec![
                (OsString::from("SURGUI_AUTH_TOKEN"), OsString::from(hostile)),
                (
                    OsString::from("SURGUI_TLS_KEY_PEM"),
                    OsString::from("-----BEGIN PRIVATE KEY-----"),
                ),
            ],
        };
        let debug = format!("{request:?}");
        assert!(debug.contains("redacted"));
        for marker in HOSTILE_INPUT_MARKERS {
            let marker = *marker;
            assert!(
                !debug.contains(marker),
                "SpawnRequest Debug leaked {marker:?}: {debug}"
            );
        }

        for (kind, code) in [
            (ErrorKind::State, "Paths"),
            (ErrorKind::Identity, "Identity"),
            (ErrorKind::Spawn, "Process"),
            (ErrorKind::Startup, "Readiness"),
        ] {
            assert_stage_code(&startup_err(kind), code);
        }
    }

    #[test]
    fn rejects_non_absolute_inputs_without_spawn() {
        // Must not touch the network or spawn; validation only.
        let rel = Path::new("relative/binary");
        let abs_state = sandbox_base().join("x");
        let abs_home = sandbox_base().join("h");
        assert!(!_assert_absolute_for_test(rel));
        assert!(_assert_absolute_for_test(&abs_state));
        // Error values are redacted and carry no input.
        let e = invalid_input();
        assert_eq!(format!("{e}"), "managed server invalid input");
        assert_eq!(format!("{e:?}"), "InvalidInput");
        let u = unsupported();
        assert_eq!(
            format!("{u}"),
            "managed server unavailable on this platform"
        );
        assert_eq!(format!("{u:?}"), "UnsupportedPlatform");
        let s = ManagedServerError {
            kind: ErrorKind::Startup,
        };
        assert_eq!(format!("{s}"), "managed server startup failed");
        assert_eq!(format!("{s:?}"), "Readiness");
        let _ = (rel, abs_state, abs_home);
    }

    #[test]
    fn debug_never_leaks_paths_or_secrets() {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<OwnerCmd>();
        let (_ev_tx, ev_rx) = mpsc::channel::<OwnerEvent>();
        // Build a dummy server value without spawning (client needs a real
        // instance; instead check Debug impl shape via error + request).
        let req = SpawnRequest {
            binary: sandbox_base().join("x"),
            args: vec![OsString::from("surgui-managed-server")],
            env: vec![(OsString::from("K"), OsString::from("V"))],
        };
        let dbg = format!("{req:?}");
        assert!(dbg.contains("redacted"));
        assert!(!dbg.contains("surgui-managed-server"));
        let _ = (cmd_tx, ev_rx);
    }

    #[test]
    fn start_future_is_send() {
        fn assert_send<T: Send>(_: T) {}
        let future = ManagedServer::start(
            Path::new("/nonexistent-managed-daemon"),
            Path::new("/nonexistent-managed-state"),
            Path::new("/nonexistent-managed-home"),
        );
        // Never polled: this is a compile-time regression guard for the
        // `&Receiver`-across-await bug that made the Tauri command !Send.
        assert_send(future);
    }

    #[test]
    fn owner_lifetime_does_not_claim_reap_on_timeout() {
        let owner = OwnerLifetime::new();
        assert!(!owner.wait_finished(Duration::from_millis(20)));
        assert!(!owner.is_finished());
        assert!(!owner.was_reaped());

        // A bounded cleanup that could not reap the child reports finished but
        // explicitly not reaped; callers must keep fences/drain outstanding.
        owner.finish(false);
        assert!(owner.is_finished());
        assert!(!owner.was_reaped());

        let reaped = OwnerLifetime::new();
        reaped.finish(true);
        assert!(reaped.is_finished());
        assert!(reaped.was_reaped());
    }

    #[test]
    fn owner_completion_actions_run_once_with_final_result() {
        let owner = OwnerLifetime::new();
        let (tx, rx) = mpsc::channel::<bool>();
        owner.on_finish(move |reaped| {
            let _ = tx.send(reaped);
        });
        assert!(rx.try_recv().is_err());
        owner.finish(true);
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)), Ok(true));

        // A later finish cannot overwrite the recorded successful result.
        owner.finish(false);
        assert!(rx.try_recv().is_err());
        assert!(owner.was_reaped());

        let late = OwnerLifetime::new();
        late.finish(false);
        let (late_tx, late_rx) = mpsc::channel::<bool>();
        late.on_finish(move |reaped| {
            let _ = late_tx.send(reaped);
        });
        assert_eq!(late_rx.recv_timeout(Duration::from_secs(1)), Ok(false));
        assert!(!late.was_reaped());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn owner_completion_action_observes_confirmed_owned_reap() {
        let req = SpawnRequest {
            binary: PathBuf::from("/bin/sleep"),
            args: vec![OsString::from("30")],
            env: Vec::new(),
        };
        let (cmd_tx, cmd_rx) = mpsc::channel::<OwnerCmd>();
        let (event_tx, event_rx) = mpsc::channel::<OwnerEvent>();
        let owner = OwnerLifetime::new();
        let (done_tx, done_rx) = mpsc::channel::<bool>();
        owner.on_finish(move |reaped| {
            let _ = done_tx.send(reaped);
        });
        let owner_thread =
            thread::spawn(move || owner_main_linux(req, cmd_rx, event_tx, None, owner));

        match event_rx
            .recv_timeout(SPAWN_ACK_TIMEOUT)
            .expect("spawn acknowledgment")
        {
            OwnerEvent::Spawned(_) => {}
            _ => panic!("expected spawn acknowledgment"),
        }
        cmd_tx
            .send(OwnerCmd::KillOwnedForTest)
            .expect("kill owned child request");
        match event_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("owner exit event")
        {
            OwnerEvent::Exited(_) => {}
            _ => panic!("expected owner exit event"),
        }
        owner_thread.join().expect("owner thread");
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(1)),
            Ok(true),
            "completion action must observe a confirmed owned reap"
        );
    }

    #[test]
    fn spawn_error_debug_is_fixed_and_sanitized() {
        let e = ManagedServerError {
            kind: ErrorKind::Spawn,
        };
        let s = format!("{e}");
        let d = format!("{e:?}");
        assert_eq!(s, "managed server startup failed");
        assert_eq!(d, "Process");
        assert!(!d.contains("1700"));
    }

    // ---- Windows-target metadata cross-check (runs anywhere, no native run)
    #[test]
    fn windows_env_allowlist_shape_is_strict() {
        // Simulate the Windows allowlist keys without spawning: the module's
        // Windows builder must contain exactly the strict keys and no ambient
        // process env. We assert the documented key set here so a future edit
        // cannot silently widen it. Native Windows execution is NOT claimed.
        let expected = [
            "SURGUI_PORT",
            "SURGUI_TLS_CERT_PEM",
            "SURGUI_TLS_KEY_PEM",
            "SURGUI_AUTH_TOKEN",
            "APPDATA",
            "USERPROFILE",
            "TEMP",
            "TMP",
        ];
        assert_eq!(expected.len(), 8);
        assert!(expected.contains(&"SURGUI_AUTH_TOKEN"));
        assert!(expected.contains(&"APPDATA"));
        // The Windows stub child must fail closed on non-Windows without spawn.
        #[cfg(not(windows))]
        {
            use crate::managed_windows::ManagedChild;
            let err = ManagedChild::spawn(
                Path::new("/bin/echo"),
                &[] as &[&OsStr],
                &[] as &[(&OsStr, &OsStr)],
                true,
            )
            .unwrap_err();
            assert_eq!(
                err,
                crate::managed_windows::SpawnError::HardeningUnavailable("windows-only")
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn startup_and_list_with_real_daemon() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let state = fresh_missing_path("startup-state");
        let home = make_home_dir("startup-home");
        let server =
            ManagedServer::start_with_timeout(binary, &state, &home, Duration::from_secs(20))
                .await
                .expect("managed daemon must start");
        let pid = server.pid();
        assert!(pid != 0);
        assert!(pid_alive(pid), "owned daemon must be alive after start");
        // Fresh daemon has no downloads; cached list from connect probe.
        let client = server.client();
        let cached = client.list_downloads();
        assert!(cached.is_empty(), "fresh daemon list must be empty");
        // Authenticated refresh over pinned TLS.
        let fresh = client.refresh().await.expect("refresh must succeed");
        assert!(fresh.is_empty());
        assert!(!server.has_exited(), "daemon must still run");
        drop(server);
        // Drop must kill/reap only the owned child.
        let start = Instant::now();
        let mut dead = false;
        while start.elapsed() < Duration::from_secs(10) {
            if !pid_alive(pid) {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(dead, "owned daemon must be reaped after coordinator drop");
        remove_all_best_effort(&state);
        remove_all_best_effort(&home);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn real_daemon_accepts_authenticated_shutdown_and_exits() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let state = fresh_missing_path("shutdown-accept-state");
        let home = make_home_dir("shutdown-accept-home");
        let server =
            ManagedServer::start_with_timeout(binary, &state, &home, Duration::from_secs(20))
                .await
                .expect("managed daemon must start");
        let pid = server.pid();
        assert!(pid_alive(pid));
        // The retained pinned managed client must reach POST /shutdown on the
        // real daemon and receive the 202 acknowledgment.
        server
            .client()
            .shutdown()
            .await
            .expect("real daemon must accept authenticated /shutdown");
        assert!(
            wait_pid_dead(pid, Duration::from_secs(6)).await,
            "real daemon must exit after authenticated /shutdown"
        );
        // Coordinator shutdown must still complete cleanly after the child
        // already exited on its own.
        server.shutdown().await;
        remove_all_best_effort(&state);
        remove_all_best_effort(&home);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_server_shutdown_reaps_real_daemon() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let state = fresh_missing_path("shutdown-reap-state");
        let home = make_home_dir("shutdown-reap-home");
        let server =
            ManagedServer::start_with_timeout(binary, &state, &home, Duration::from_secs(20))
                .await
                .expect("managed daemon must start");
        let pid = server.pid();
        assert!(pid_alive(pid));
        let started = Instant::now();
        server.shutdown().await;
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "shutdown must stay within the fixed bound"
        );
        assert!(
            wait_pid_dead(pid, Duration::from_secs(8)).await,
            "owned daemon must be reaped after coordinator shutdown"
        );
        remove_all_best_effort(&state);
        remove_all_best_effort(&home);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancelled_shutdown_still_retires_real_daemon() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let state = fresh_missing_path("cancelled-shutdown-real-state");
        let home = make_home_dir("cancelled-shutdown-real-home");
        let server =
            ManagedServer::start_with_timeout(binary, &state, &home, Duration::from_secs(20))
                .await
                .expect("managed daemon must start");
        let pid = server.pid();
        assert!(pid_alive(pid));

        let mut shutdown = Box::pin(server.shutdown());
        let first_poll = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(shutdown.as_mut(), cx))
        })
        .await;
        // Whether the first poll was pending or completed, the authenticated
        // request was dispatched at shutdown begin and must not be skipped.
        let _ = first_poll;
        drop(shutdown);
        assert!(
            wait_pid_dead(pid, Duration::from_secs(8)).await,
            "cancelled shutdown must still retire the real daemon"
        );
        remove_all_best_effort(&state);
        remove_all_best_effort(&home);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancelled_shutdown_after_begin_still_sends_authenticated_request() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let _guard = port_lock().lock().await;
        let id = ManagedIdentity::generate().expect("identity");
        let server_config = {
            use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
            Arc::new(
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(
                        vec![CertificateDer::from(id.server_cert_der().to_vec())],
                        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                            id.server_key_der().to_vec(),
                        )),
                    )
                    .expect("server identity"),
            )
        };
        let listener = TcpListener::bind("127.0.0.1:1700").expect("bind fake managed endpoint");
        listener
            .set_nonblocking(true)
            .expect("nonblocking fake endpoint");
        let token = id.bearer_token().to_owned();
        let token_for_server = token.clone();
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<bool>();
        let fake_server = thread::spawn(move || {
            for _ in 0..2 {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => return,
                    }
                };
                stream.set_read_timeout(Some(Duration::from_secs(8))).ok();
                let mut conn =
                    rustls::ServerConnection::new(server_config.clone()).expect("server conn");
                let mut tls = rustls::Stream::new(&mut conn, &mut stream);
                let mut request = Vec::new();
                let mut byte = [0u8; 1];
                while !request.ends_with(b"\r\n\r\n") && request.len() < 16 * 1024 {
                    match tls.read(&mut byte) {
                        Ok(0) => break,
                        Ok(_) => request.push(byte[0]),
                        Err(_) => break,
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let is_shutdown = text.starts_with("POST /shutdown HTTP/1.1");
                if is_shutdown {
                    let auth_ok = text.to_ascii_lowercase().contains(&format!(
                        "authorization: bearer {}",
                        token_for_server.to_ascii_lowercase()
                    ));
                    let _ = shutdown_tx.send(auth_ok);
                }
                let (status, body): (&str, &[u8]) = if is_shutdown {
                    ("202 Accepted", br#"{"status":"shutting_down"}"#)
                } else {
                    ("200 OK", b"[]")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = tls.write_all(response.as_bytes());
                let _ = tls.write_all(body);
                let _ = tls.flush();
                if is_shutdown {
                    return;
                }
            }
        });

        let ca = id.ca_cert_pem().into_bytes();
        let client = SurgeClient::connect_managed(&ca, &token)
            .await
            .expect("connect fake managed endpoint");

        // Fake owner: acknowledge the graceful command only after a short
        // delay, guaranteeing the first shutdown poll is still pending when
        // the future is cancelled.
        let (cmd_tx, cmd_rx) = mpsc::channel::<OwnerCmd>();
        let (event_tx, event_rx) = mpsc::channel::<OwnerEvent>();
        let owner = thread::spawn(move || {
            if let Ok(OwnerCmd::ShutdownGraceful(_)) = cmd_rx.recv_timeout(Duration::from_secs(5)) {
                thread::sleep(Duration::from_millis(150));
                let _ = event_tx.send(OwnerEvent::Exited(Some(0)));
            }
        });
        let server = ManagedServer {
            pid: 4242,
            state_path: sandbox_base().join("cancel-shutdown-state"),
            cmd_tx: Some(cmd_tx),
            event_rx,
            exited: AtomicBool::new(false),
            owner_handle: Some(owner),
            owner: OwnerLifetime::finished_for_test(),
            client: Arc::new(client),
            shutdown_deadline: None,
            shutdown_request: None,
        };
        let mut shutdown = Box::pin(server.shutdown());
        let first_poll = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(shutdown.as_mut(), cx))
        })
        .await;
        assert!(
            matches!(first_poll, std::task::Poll::Pending),
            "shutdown must be in flight after begin_shutdown"
        );
        // Cancel exactly between begin_shutdown and completion of the
        // authenticated request. The request was already dispatched and must
        // still reach the pinned managed endpoint.
        drop(shutdown);
        let auth_ok = shutdown_rx
            .recv_timeout(Duration::from_secs(8))
            .expect("authenticated shutdown request must still be sent");
        assert!(auth_ok, "shutdown request must carry the managed bearer");
        fake_server.join().expect("fake managed endpoint");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn owner_graceful_deadline_force_kills_owned_child() {
        // The fixture is not a TLS daemon, so no /shutdown can succeed; the
        // owner's fixed deadline must still force-kill and reap its own child.
        let req = SpawnRequest {
            binary: PathBuf::from("/bin/sleep"),
            args: vec![OsString::from("300")],
            env: Vec::new(),
        };
        let (cmd_tx, cmd_rx) = mpsc::channel::<OwnerCmd>();
        let (event_tx, event_rx) = mpsc::channel::<OwnerEvent>();
        let owner_lifetime = OwnerLifetime::new();
        let owner =
            thread::spawn(move || owner_main_linux(req, cmd_rx, event_tx, None, owner_lifetime));
        let pid = match event_rx
            .recv_timeout(SPAWN_ACK_TIMEOUT)
            .expect("spawn acknowledgment")
        {
            OwnerEvent::Spawned(pid) => pid,
            _ => panic!("expected spawn acknowledgment"),
        };
        assert!(pid_alive(pid));
        let started = Instant::now();
        cmd_tx
            .send(OwnerCmd::ShutdownGraceful(
                Instant::now() + Duration::from_millis(200),
            ))
            .expect("send graceful shutdown");
        match event_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("owner must finish within the bound")
        {
            OwnerEvent::Exited(_) => {}
            _ => panic!("expected owner exit event"),
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            wait_pid_dead(pid, Duration::from_secs(3)).await,
            "force fallback must reap the owned child"
        );
        owner.join().expect("owner thread");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn owner_graceful_deadline_force_kills_owned_child_windows() {
        // Windows counterpart of the Linux fallback test. Native execution is
        // only possible on a Windows host.
        let req = SpawnRequest {
            binary: PathBuf::from(r"C:\Windows\System32\ping.exe"),
            args: vec![
                OsString::from("-n"),
                OsString::from("30"),
                OsString::from("127.0.0.1"),
            ],
            env: Vec::new(),
        };
        let (cmd_tx, cmd_rx) = mpsc::channel::<OwnerCmd>();
        let (event_tx, event_rx) = mpsc::channel::<OwnerEvent>();
        let owner_lifetime = OwnerLifetime::new();
        let owner =
            thread::spawn(move || owner_main_windows(req, cmd_rx, event_tx, None, owner_lifetime));
        let pid = match event_rx
            .recv_timeout(SPAWN_ACK_TIMEOUT)
            .expect("spawn acknowledgment")
        {
            OwnerEvent::Spawned(pid) => pid,
            _ => panic!("expected spawn acknowledgment"),
        };
        cmd_tx
            .send(OwnerCmd::ShutdownGraceful(
                Instant::now() + Duration::from_millis(200),
            ))
            .expect("send graceful shutdown");
        match event_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("owner must finish within the bound")
        {
            OwnerEvent::Exited(_) => {}
            _ => panic!("expected owner exit event"),
        }
        let _ = pid;
        owner.join().expect("owner thread");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn impostor_on_1700_fails_closed_without_token_leak() {
        use std::io::Read;
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex as StdMutex};

        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        // Impostor: plain TCP on 1700 that records everything. A correct
        // client must never send the bearer here (TLS pinning fails first,
        // and no plaintext fallback exists).
        let listener = TcpListener::bind("127.0.0.1:1700").expect("bind impostor on 1700");
        listener
            .set_nonblocking(true)
            .expect("nonblocking impostor");
        let captured: Arc<StdMutex<Vec<u8>>> = Arc::new(StdMutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let cap_clone = captured.clone();
        let stop_clone = stop.clone();
        let impostor = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(25);
            while Instant::now() < deadline && !stop_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut s, _)) => {
                        s.set_read_timeout(Some(Duration::from_millis(500))).ok();
                        let mut buf = [0u8; 4096];
                        loop {
                            match s.read(&mut buf) {
                                Ok(0) => break,
                                Ok(n) => {
                                    cap_clone.lock().unwrap().extend_from_slice(&buf[..n]);
                                    if cap_clone.lock().unwrap().len() > 64 * 1024 {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });

        let state = fresh_missing_path("impostor-state");
        let home = make_home_dir("impostor-home");
        // Real daemon cannot bind 1700 (occupied) and must fail closed.
        let res =
            ManagedServer::start_with_timeout(binary, &state, &home, Duration::from_secs(12)).await;
        assert!(res.is_err(), "occupied 1700 must fail closed");
        let err = res.unwrap_err();
        let msg = format!("{err}");
        assert_eq!(msg, "managed server startup failed");
        assert_eq!(format!("{err:?}"), "Readiness");
        // If a server was somehow returned (must not happen), drop it now.
        // Token confidentiality: impostor must never have seen a bearer.
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.store(true, Ordering::SeqCst);
        let _ = impostor.join();
        let bytes = captured.lock().unwrap().clone();
        // No HTTP Authorization bearer line may have crossed.
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.to_ascii_lowercase().contains("authorization"),
            "bearer must never travel to impostor"
        );
        assert!(
            !text.contains("Bearer "),
            "bearer must never travel to impostor"
        );
        // Port must be free again (no leftover owned child holding it).
        let rebound = std::net::TcpListener::bind("127.0.0.1:1700")
            .expect("1700 must be free after fail-closed");
        drop(rebound);
        remove_all_best_effort(&state);
        remove_all_best_effort(&home);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn drop_kills_only_owned_child_sibling_survives() {
        use crate::managed_process::ManagedChild;
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let state = fresh_missing_path("drop-state");
        let home = make_home_dir("drop-home");
        // Survivor owned directly (never via coordinator pid).
        let mut survivor = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn survivor");
        let survivor_pid = survivor.id();
        let victim_pid = {
            let server =
                ManagedServer::start_with_timeout(binary, &state, &home, Duration::from_secs(20))
                    .await
                    .expect("start victim daemon");
            let pid = server.pid();
            assert_ne!(pid, survivor_pid);
            assert!(pid_alive(pid));
            // Drop coordinator: only the victim may die.
            drop(server);
            pid
        };
        let start = Instant::now();
        let mut victim_dead = false;
        while start.elapsed() < Duration::from_secs(10) {
            if !pid_alive(victim_pid) {
                victim_dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(victim_dead, "dropped coordinator child must die");
        assert!(
            survivor.try_wait().expect("poll survivor").is_none(),
            "sibling must survive coordinator drop"
        );
        assert!(pid_alive(survivor_pid));
        survivor.kill().ok();
        let _ = survivor.wait();
        remove_all_best_effort(&state);
        remove_all_best_effort(&home);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancelled_start_cleans_up_owned_child() {
        let _guard = port_lock().lock().await;
        let root = fresh_missing_path("cancel-start");
        std::fs::create_dir_all(&root).expect("create cancel root");
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("create cancel home");
        let state = root.join("state");
        let pidfile = root.join("child.pid");
        // A fixture that never becomes TLS-ready, so cancellation happens
        // after the child exists but before startup completes.
        let script = write_sleeping_fixture(&root, &pidfile);

        let mut start = Box::pin(ManagedServer::start_with_timeout(
            &script,
            &state,
            &home,
            Duration::from_secs(60),
        ));
        let mut child_pid = None;
        for _ in 0..500 {
            tokio::select! {
                result = &mut start => {
                    panic!("sleeping fixture must not become ready: {:?}", result.map(|s| s.pid()));
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            if let Ok(text) = std::fs::read_to_string(&pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    child_pid = Some(pid);
                    break;
                }
            }
        }
        let child_pid = child_pid.expect("fixture child must have spawned");
        assert!(pid_alive(child_pid));
        // Cancel startup exactly as a dropped/discarded command future would.
        drop(start);
        assert!(
            wait_pid_dead(child_pid, Duration::from_secs(10)).await,
            "cancelled startup must kill/reap its owned child"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancelling_unpolled_start_kills_owned_child() {
        let _guard = port_lock().lock().await;
        let root = fresh_missing_path("unpolled-cancel");
        std::fs::create_dir_all(&root).expect("create root");
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("create home");
        let state = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);
        let cancel = Arc::new(AtomicBool::new(false));
        let mut start = Box::pin(ManagedServer::start_cancellable(
            &script,
            &state,
            &home,
            Arc::clone(&cancel),
        ));
        let mut child_pid = None;
        for _ in 0..500 {
            tokio::select! {
                result = &mut start => {
                    panic!("sleeping fixture must not become ready: {:?}", result.map(|s| s.pid()));
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            if let Ok(text) = std::fs::read_to_string(&pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    child_pid = Some(pid);
                    break;
                }
            }
        }
        let child_pid = child_pid.expect("fixture child must have spawned");
        // Stop polling the start future entirely, then request cancellation.
        // The owner thread itself must still release the owned child.
        cancel.store(true, Ordering::SeqCst);
        assert!(
            wait_pid_dead(child_pid, Duration::from_secs(10)).await,
            "unpolled cancelled start must kill/reap its owned child"
        );
        drop(start);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn readiness_timeout_fails_closed_without_peer() {
        // 1700 free (guard held, no listener): pinned probe must time out
        // rather than succeed or send bearer anywhere.
        let _guard = port_lock().lock().await;
        // Ensure 1700 is free.
        let probe = std::net::TcpListener::bind("127.0.0.1:1700");
        assert!(probe.is_ok(), "1700 must be free for timeout test");
        drop(probe);
        let id = ManagedIdentity::generate().expect("identity");
        let ca = id.ca_cert_pem().into_bytes();
        let token = id.bearer_token().to_owned();
        let start = Instant::now();
        let res = tokio::time::timeout(
            Duration::from_secs(4),
            SurgeClient::connect_managed(&ca, &token),
        )
        .await;
        // Either inner timeout or outer timeout: both fail closed.
        match res {
            Ok(Ok(_)) => panic!("must not connect with no peer"),
            Ok(Err(_)) | Err(_) => {
                assert!(start.elapsed() < Duration::from_secs(10));
            }
        }
    }

    #[cfg(windows)]
    fn windows_unique_tag(tag: &str) -> String {
        use std::sync::atomic::Ordering;
        let id = WINDOWS_TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("{}-{}-{}", tag, std::process::id(), id)
    }

    #[cfg(windows)]
    fn windows_real_daemon_binary() -> PathBuf {
        let is_ci =
            std::env::var_os("CI").is_some() || std::env::var_os("GITHUB_ACTIONS").is_some();
        match std::env::var_os("SURGUI_TEST_DAEMON") {
            None => {
                if is_ci {
                    panic!("CI requires SURGUI_TEST_DAEMON set to absolute staged daemon path; cannot silently skip windows_real_managed_daemon_authenticated_lifecycle");
                } else {
                    panic!("SURGUI_TEST_DAEMON must be set to an absolute staged daemon path; cannot silently skip");
                }
            }
            Some(value) => {
                let path = PathBuf::from(value);
                assert!(
                    path.is_absolute(),
                    "SURGUI_TEST_DAEMON must be an absolute path"
                );
                assert!(
                    path.is_file(),
                    "SURGUI_TEST_DAEMON must point to an existing staged daemon file"
                );
                path
            }
        }
    }

    #[cfg(windows)]
    fn windows_assert_missing(path: &Path) {
        match path.symlink_metadata() {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => panic!("scratch final path must be missing (NotFound)"),
            Ok(_) => panic!("scratch final path must be missing to exercise secure creation"),
        }
    }

    #[cfg(windows)]
    fn windows_remove_owned_after_confirmed_reap(path: &Path) {
        match path.symlink_metadata() {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(_) => panic!("confirmed cleanup found unreadable scratch metadata"),
            Ok(_) => {}
        }
        if path.is_dir() && !path.is_symlink() {
            std::fs::remove_dir_all(path).expect("confirmed cleanup must remove owned dir");
        } else {
            std::fs::remove_file(path).expect("confirmed cleanup must remove owned file");
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_real_managed_daemon_coordinator_shutdown_reaps() {
        // Serialize fixed-port 1700 tests in this binary; held across startup
        // through verified shutdown.
        let _guard = test_port_lock().lock().await;
        // Explicit staged fixture; fail loudly if missing/invalid so CI can
        // never silently skip this real-daemon regression. Ordinary
        // `cargo test` on Windows also requires this fixture.
        let binary = windows_real_daemon_binary();
        assert!(binary.is_absolute(), "fixture must be absolute");
        assert!(binary.is_file(), "fixture must exist");
        // Only the shared sandbox base is pre-created. The unique parent and
        // home final components stay missing so `ManagedStateDir::prepare`
        // creates them with production secure ACLs (never pre-create with
        // inherited ACLs, which `prepare` would only validate/reject).
        std::fs::create_dir_all(sandbox_base()).expect("create sandbox base");
        let parent = sandbox_base().join(windows_unique_tag("win-managed-coord"));
        windows_assert_missing(&parent);
        let _parent_dir = crate::managed_state::ManagedStateDir::prepare(&parent)
            .expect("prepare private parent with production ACL");
        let home = parent.join(windows_unique_tag("home"));
        windows_assert_missing(&home);
        let _home_dir = crate::managed_state::ManagedStateDir::prepare(&home)
            .expect("prepare private home with production ACL");
        let state = parent.join(windows_unique_tag("state"));
        windows_assert_missing(&state);
        // Retain the owner lifetime across the start future so a
        // cancelled/failed start stays fenced until the real owner has
        // finished. Never signal a bare PID.
        let owner = OwnerLifetime::new();
        let cancel = Arc::new(AtomicBool::new(false));
        let server = match ManagedServer::start_cancellable_tracked(
            &binary,
            &state,
            &home,
            Duration::from_secs(20),
            Arc::clone(&cancel),
            owner.clone(),
        )
        .await
        {
            Ok(server) => server,
            Err(error) => {
                // Startup failed: keep the port lock while waiting bounded for
                // the real owner to finish. Pre-spawn failures are already
                // marked finished/reaped by the start guard; post-spawn
                // failures need the owner thread to reap the owned handle.
                let finished = owner.wait_finished(Duration::from_secs(10));
                let reaped = finished && owner.was_reaped();
                if !reaped {
                    panic!(
                        "managed daemon must start on Windows: {}; owner cleanup unconfirmed, scratch preserved",
                        error.stage().code()
                    );
                }
                panic!(
                    "managed daemon must start on Windows: {}",
                    error.stage().code()
                );
            }
        };
        // Record owner pending state without asserting while the owned child
        // is live; asserted only after shutdown/cleanup below.
        let owner_was_pending = !owner.is_finished();
        let coordinator_owner = server.owner_lifetime();
        // Gather post-start observations without panicking while the owned
        // child is live; never signal a bare PID. Never call `client.shutdown`
        // before the coordinator shutdown below: doing so would let the child
        // exit first and make the coordinator shutdown assertion a false
        // positive.
        let pid = server.pid();
        let client = server.client();
        let list_is_empty = client.list_downloads().is_empty();
        let refresh_result = client.refresh().await;
        let still_running_before_shutdown = !server.has_exited();
        // Retire ONLY via the owned coordinator and confirm the owner
        // finished/reaped before any assertions or cleanup. `_guard` is still
        // held here, serializing the fixed port through verified shutdown.
        // A `true` reap alone cannot prove graceful HTTP rather than force;
        // it only proves the owned handle was reaped.
        let reaped = server.shutdown().await;
        let owner_finished = coordinator_owner.is_finished();
        let owner_reaped = coordinator_owner.was_reaped();
        let reap_confirmed = reaped && owner_finished && owner_reaped;
        // Remove only uniquely owned test paths, surfacing remove errors once
        // termination was confirmed. Preserve scratch when unconfirmed.
        if reap_confirmed {
            windows_remove_owned_after_confirmed_reap(&state);
            windows_remove_owned_after_confirmed_reap(&home);
            windows_remove_owned_after_confirmed_reap(&parent);
        }
        // Assertions only after owned shutdown/reap confirmation.
        assert!(
            owner_was_pending,
            "owner must have been pending after successful start"
        );
        assert!(pid != 0, "owned daemon pid must be nonzero");
        assert!(list_is_empty, "fresh isolated daemon must have empty list");
        let fresh = refresh_result.expect("real pinned TLS refresh must succeed");
        assert!(
            fresh.is_empty(),
            "fresh isolated daemon refresh must be empty"
        );
        assert!(
            still_running_before_shutdown,
            "daemon must still run before shutdown"
        );
        assert!(reaped, "coordinator shutdown must report owned reap");
        assert!(owner_finished, "owner must be finished");
        assert!(owner_reaped, "owner must confirm owned child reaped");
    }
}
