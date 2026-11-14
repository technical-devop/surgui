//! Owned child-process lifetime (Linux supervisor / Windows fail-closed).
//!
//! Standalone file. Wiring requires `pub mod managed_process;` in `lib.rs`
//! (out of scope). No `Cargo.toml` change: Linux uses only `std` plus two
//! manually declared syscalls (`prctl`, `getppid`). Standalone `rustc
//! --test` execution is supported (no Cargo needed).
//!
//! # Guarantees (Linux)
//! - Spawns exactly the absolute `binary` verbatim; bare/relative paths are
//!   rejected, `PATH` never searched.
//! - Holds the real `Child` handle; `kill`/`wait` act on that handle only,
//!   never on a bare pid, so PID reuse cannot hit another process.
//! - Normal drop force-kills (SIGKILL) the owned child and waits at most a
//!   fixed bound; if the bound expires, exactly one background blocking reaper
//!   retains the owned handle until confirmed exit. If that reaper cannot be
//!   created or rejects the ownership handoff, the child stays owned and the
//!   drop waits on it (retrying OS wait errors with backoff) instead of
//!   discarding it as reaped. Drop never panics and never signals a bare pid.
//! - A missing owned handle is never treated as a confirmed reap. Shared
//!   state records whether the child is still owned, handed to the single
//!   reaper, confirmed exited, or failed to confirm. Repeated
//!   `terminate_and_reap` calls while a handoff is pending stay `Pending`, and
//!   the blocking owner path waits on that same real completion. `Reaped` is
//!   returned only after a real owned wait confirms exit.
//! - Forced parent death (SIGKILL of owner process, destructors skipped) must
//!   not orphan: child carries `PR_SET_PDEATHSIG(SIGKILL)`.
//!
//! # Linux thread semantics
//! `PR_SET_PDEATHSIG` fires when the creating parent *thread* dies, not
//! necessarily the whole process. Spawning from a short-lived worker thread
//! would therefore kill the child spuriously. This file spawns only on a
//! dedicated supervisor thread that blocks until the owner drops, so the
//! creating thread stays alive as long as the child. `pre_exec` (runs
//! post-`fork`, pre-`exec`, single-threaded, async-signal-safe only) does
//! `prctl(PR_SET_PDEATHSIG, SIGKILL)` then a `getppid() == expected` race
//! check; mismatch aborts before `exec`. Whole-process SIGKILL kills the
//! supervisor thread too, so the kernel still delivers SIGKILL to the child.
//!
//! # Non-guarantees
//! No assurance for setuid/setgid/capability `exec` (kernel clears
//! pdeathsig), for grandchildren the child creates itself, or for untested
//! configurations. Verified paths are absolute-path spawns covered by the
//! tests below.
//!
//! # Windows / non-Linux Unix
//! Fail closed with [`SpawnError::HardeningUnavailable`]. Correct Windows
//! orphan prevention needs suspended launch plus a non-inheritable
//! kill-on-close Job, unavailable via `std::process::Command` alone.
//!
//! # Privacy
//! No path, arg, or env value is stored or printed. `Debug`/`Display` emit
//! only codes, pid, and `ErrorKind`.

use std::ffi::OsStr;
#[cfg(any(target_os = "linux", test))]
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::Path;
use std::process::{Child, ExitStatus};
#[cfg(any(target_os = "linux", test))]
use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Upper bound for reaping an owned child after a forced kill during drop.
const FORCED_REAP_TIMEOUT: Duration = Duration::from_secs(5);

/// Sanitized spawn failure. No paths, args, env, or OS text.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    /// Path was not absolute. `PATH` never probed.
    NotAbsolute,
    /// `Command::spawn` or supervisor setup failed. Kind only.
    SpawnFailed { kind: io::ErrorKind },
    /// `pre_exec` saw `getppid() != expected`: parent died in fork-to-`prctl`
    /// window, child aborted before `exec`.
    ParentExitedDuringSpawn,
    /// Orphan prevention unavailable; spawn refused. Payload names the gap
    /// (`"windows-job"` or `"unix-pdeathsig"`).
    HardeningUnavailable(&'static str),
}

impl fmt::Debug for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAbsolute => write!(f, "NotAbsolute"),
            Self::SpawnFailed { kind } => {
                f.debug_struct("SpawnFailed").field("kind", kind).finish()
            }
            Self::ParentExitedDuringSpawn => write!(f, "ParentExitedDuringSpawn"),
            Self::HardeningUnavailable(what) => {
                f.debug_tuple("HardeningUnavailable").field(what).finish()
            }
        }
    }
}

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAbsolute => write!(f, "binary path must be absolute"),
            Self::SpawnFailed { .. } => write!(f, "process spawn failed"),
            Self::ParentExitedDuringSpawn => write!(f, "parent exited during spawn"),
            Self::HardeningUnavailable(_) => {
                write!(f, "orphan prevention unavailable on this platform")
            }
        }
    }
}

impl std::error::Error for SpawnError {}

impl SpawnError {
    fn spawn_failed(err: io::Error) -> Self {
        Self::SpawnFailed { kind: err.kind() }
    }
}

#[cfg(target_os = "linux")]
mod linux_raw {
    use std::os::raw::{c_int, c_ulong};

    // SAFETY: signatures match Linux `prctl(2)` / `getppid(2)`.
    extern "C" {
        pub fn prctl(
            option: c_int,
            arg2: c_ulong,
            arg3: c_ulong,
            arg4: c_ulong,
            arg5: c_ulong,
        ) -> c_int;
        pub fn getppid() -> c_int;
    }

    pub const PR_SET_PDEATHSIG: c_int = 1;
    pub const SIGKILL: c_ulong = 9;
}

/// Owned handle to exactly one child plus its supervisor thread.
///
/// Main thread owns the `Child` handle directly (`kill`/`wait` never use a
/// bare pid). The supervisor thread is the thread that forked the child and
/// stays blocked on a channel until owner drop, keeping pdeathsig armed
/// against spurious parent-thread death. On whole-process SIGKILL both
/// threads die and the kernel kills the child.
pub struct ManagedChild {
    child: Option<Child>,
    shutdown: Option<mpsc::Sender<()>>,
    supervisor: Option<thread::JoinHandle<()>>,
    reap: Arc<ReapShared>,
}

/// Outcome of a bounded force-kill/reap attempt.
pub enum ReapOutcome {
    /// The owned child was reaped within the bound.
    Reaped(Option<i32>),
    /// No reap has been confirmed yet. Either one background reaper now owns
    /// the child, or a previous bounded probe left it with the owner. A
    /// repeated call observes the same real completion.
    Pending(ReapWaiter),
    /// A background reaper could not be created (or rejected the ownership
    /// handoff), and no exit has been confirmed. The child/guard remain
    /// retained by the owning `ManagedChild` when still owned; success is never
    /// claimed and a later call may confirm the reap.
    Unavailable,
}

/// Shared lifecycle for a child that may outlive a bounded reap probe.
///
/// `child == None` in the owner is not proof of reaping. Only a real owned
/// wait may move this state to `Confirmed`; a repeated call observes the same
/// state and the same single reaper instead of inventing a result.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReapStatus {
    /// No reaper has taken ownership; the owner still holds the child.
    Owned,
    /// Exactly one background reaper owns the child and is waiting.
    Pending,
    /// A real owned wait (owner or reaper) confirmed exit.
    Confirmed(Option<i32>),
    /// Ownership was handed off but confirmation failed (for example a panicked
    /// waiter). Never treated as success.
    Failed,
}

struct ReapShared {
    status: Mutex<ReapStatus>,
    changed: Condvar,
    /// `cfg(test)`-only handshake: notified exactly when the actual blocking
    /// owner path enters its real wait (shared-state wait or retained-handle
    /// blocking wait). Production builds have no such field and no behavior
    /// change; the hook never holds the status lock across the wait and its
    /// `send` never blocks, preserving thread safety and cancellation.
    #[cfg(test)]
    test_blocking_entered: Mutex<Option<mpsc::Sender<()>>>,
}

impl ReapShared {
    fn new() -> Self {
        Self {
            status: Mutex::new(ReapStatus::Owned),
            changed: Condvar::new(),
            #[cfg(test)]
            test_blocking_entered: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_test_blocking_entered(&self, tx: Option<mpsc::Sender<()>>) {
        match self.test_blocking_entered.lock() {
            Ok(mut guard) => *guard = tx,
            Err(poisoned) => *poisoned.into_inner() = tx,
        }
    }

    #[cfg(test)]
    fn notify_test_blocking_entered(&self) {
        // Best-effort, non-blocking handshake only. Never poisons status,
        // never blocks the owner thread, never affects the Condvar protocol.
        let guard = match self.test_blocking_entered.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(tx) = guard.as_ref() {
            let _ = tx.send(());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ReapStatus> {
        match self.status.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn status(&self) -> ReapStatus {
        *self.lock()
    }

    /// Move `Owned` -> `Pending` before any handoff. Returns false if the
    /// state was not `Owned` (never start a second reaper blindly).
    fn mark_pending(&self) -> bool {
        let mut status = self.lock();
        if matches!(*status, ReapStatus::Owned) {
            *status = ReapStatus::Pending;
            true
        } else {
            false
        }
    }

    /// Undo an unaccepted handoff. Never overwrites a confirmed result.
    fn mark_owned(&self) {
        let mut status = self.lock();
        if matches!(*status, ReapStatus::Pending | ReapStatus::Failed) {
            *status = ReapStatus::Owned;
            self.changed.notify_all();
        }
    }

    /// Record the only success transition: a real owned wait returned.
    fn confirm(&self, code: Option<i32>) {
        let mut status = self.lock();
        if !matches!(*status, ReapStatus::Confirmed(_)) {
            *status = ReapStatus::Confirmed(code);
            self.changed.notify_all();
        }
    }

    fn fail(&self) {
        let mut status = self.lock();
        if !matches!(*status, ReapStatus::Confirmed(_)) {
            *status = ReapStatus::Failed;
            self.changed.notify_all();
        }
    }

    /// Wait for a non-pending state. `None` timeout waits until completion.
    /// A timeout returns the still-`Pending` state; success is not implied.
    fn wait(&self, timeout: Option<Duration>) -> ReapStatus {
        let mut status = self.lock();
        match timeout {
            None => {
                while matches!(*status, ReapStatus::Pending) {
                    status = match self.changed.wait(status) {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                }
            }
            Some(timeout) => {
                let deadline = Instant::now() + timeout;
                while matches!(*status, ReapStatus::Pending) {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    let waited = self.changed.wait_timeout(status, deadline - now);
                    status = match waited {
                        Ok((guard, _)) => guard,
                        Err(poisoned) => poisoned.into_inner().0,
                    };
                }
            }
        }
        *status
    }
}

/// Completion signal for the single owned child/reaper state.
pub struct ReapWaiter {
    shared: Arc<ReapShared>,
}

impl ReapWaiter {
    fn new(shared: Arc<ReapShared>) -> Self {
        Self { shared }
    }

    /// Wait at most `timeout` for the real owned wait to confirm exit.
    /// `None` means the bound expired; success is not claimed.
    pub fn wait(&self, timeout: Duration) -> Option<bool> {
        match self.shared.wait(Some(timeout)) {
            ReapStatus::Confirmed(_) => Some(true),
            ReapStatus::Failed => Some(false),
            _ => None,
        }
    }

    /// Wait for the single completion report. No CPU polling.
    pub fn wait_blocking(&self) -> bool {
        matches!(self.shared.wait(None), ReapStatus::Confirmed(_))
    }

    fn wait_blocking_status(&self) -> ReapStatus {
        self.shared.wait(None)
    }
}

/// Ownership moved to a background reaper only after its thread was created
/// and is ready to receive it.
struct ReaperOwned {
    child: Child,
    shutdown: Option<mpsc::Sender<()>>,
    supervisor: Option<thread::JoinHandle<()>>,
}

type ReaperWait = Arc<dyn Fn(&mut Child) -> io::Result<ExitStatus> + Send + Sync>;

fn real_child_wait(child: &mut Child) -> io::Result<ExitStatus> {
    child.wait()
}

/// Retry a blocking wait on the same owned handle until the OS confirms exit.
///
/// A transient wait error is never treated as success and the handle is never
/// discarded. The backoff prevents a busy loop; the caller retains ownership
/// for the whole loop.
fn wait_until_confirmed<T, F>(mut wait: F) -> T
where
    F: FnMut() -> io::Result<T>,
{
    let mut delay = Duration::from_millis(10);
    loop {
        match wait() {
            Ok(value) => return value,
            Err(_) => {
                thread::sleep(delay);
                delay = delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
    }
}

fn spawn_child_reaper(
    handoff_rx: mpsc::Receiver<ReaperOwned>,
    shared: Arc<ReapShared>,
    wait: ReaperWait,
) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("managed-child-reaper".into())
        .spawn(move || {
            let owned = match handoff_rx.recv() {
                Ok(owned) => owned,
                Err(_) => {
                    shared.fail();
                    return;
                }
            };
            let ReaperOwned {
                mut child,
                shutdown,
                supervisor,
            } = owned;
            // If the waiter itself panics, mark failure rather than leaving a
            // pending state that could be mistaken for an eventual success.
            let confirm = ReapConfirmGuard {
                shared: Arc::clone(&shared),
            };
            // Wait on the same owned handle until the exit is confirmed. A
            // wait error is retried with backoff; the handle is never dropped
            // as if reaped.
            let status = wait_until_confirmed(|| wait(&mut child));
            drop(child);
            drop(shutdown);
            if let Some(handle) = supervisor {
                let _ = handle.join();
            }
            // Only a real owned wait gets here with exit confirmed.
            shared.confirm(status.code());
            drop(confirm);
        })
}

/// Marks failure only if no real confirmation was recorded first.
struct ReapConfirmGuard {
    shared: Arc<ReapShared>,
}

impl Drop for ReapConfirmGuard {
    fn drop(&mut self) {
        self.shared.fail();
    }
}

impl fmt::Debug for ManagedChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedChild")
            .field("pid", &self.id())
            .finish()
    }
}

impl ManagedChild {
    /// Spawn absolute `binary` with `args` and `env_overrides`.
    ///
    /// `clear_env(true)` clears inherited env first. stdio is null. On
    /// Windows / non-Linux Unix returns `HardeningUnavailable` unspawned.
    pub fn spawn(
        binary: &Path,
        args: &[impl AsRef<OsStr>],
        env_overrides: &[(impl AsRef<OsStr>, impl AsRef<OsStr>)],
        clear_env: bool,
    ) -> Result<Self, SpawnError> {
        if !binary.is_absolute() {
            return Err(SpawnError::NotAbsolute);
        }

        #[cfg(windows)]
        {
            let _ = (args, env_overrides, clear_env);
            return Err(SpawnError::HardeningUnavailable("windows-job"));
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = (args, env_overrides, clear_env);
            return Err(SpawnError::HardeningUnavailable("unix-pdeathsig"));
        }

        #[cfg(target_os = "linux")]
        {
            Self::spawn_supervised(binary, args, env_overrides, clear_env)
        }
    }

    #[cfg(target_os = "linux")]
    fn spawn_supervised(
        binary: &Path,
        args: &[impl AsRef<OsStr>],
        env_overrides: &[(impl AsRef<OsStr>, impl AsRef<OsStr>)],
        clear_env: bool,
    ) -> Result<Self, SpawnError> {
        use std::os::unix::process::CommandExt;

        let binary_buf = binary.to_path_buf();
        let args_owned: Vec<OsString> = args.iter().map(|a| a.as_ref().to_os_string()).collect();
        let env_owned: Vec<(OsString, OsString)> = env_overrides
            .iter()
            .map(|(k, v)| (k.as_ref().to_os_string(), v.as_ref().to_os_string()))
            .collect();

        let (result_tx, result_rx) = mpsc::channel::<Result<Child, SpawnError>>();
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();

        let supervisor = thread::Builder::new()
            .name("managed-proc-supv".into())
            .spawn(move || {
                let mut cmd = Command::new(&binary_buf);
                cmd.args(&args_owned);
                if clear_env {
                    cmd.env_clear();
                }
                for (k, v) in &env_owned {
                    cmd.env(k, v);
                }
                cmd.stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());

                let expected_ppid = std::process::id();
                // SAFETY: runs post-fork, pre-exec, single-threaded; only
                // `prctl`/`getppid`, no allocation or locking.
                unsafe {
                    cmd.pre_exec(move || {
                        let rc = linux_raw::prctl(
                            linux_raw::PR_SET_PDEATHSIG,
                            linux_raw::SIGKILL,
                            0,
                            0,
                            0,
                        );
                        if rc != 0 {
                            return Err(io::Error::last_os_error());
                        }
                        if linux_raw::getppid() as u32 != expected_ppid {
                            return Err(io::Error::from_raw_os_error(10));
                        }
                        Ok(())
                    });
                }

                let spawned: Result<Child, SpawnError> = cmd.spawn().map_err(|e| {
                    if e.raw_os_error() == Some(10) {
                        SpawnError::ParentExitedDuringSpawn
                    } else {
                        SpawnError::spawn_failed(e)
                    }
                });
                match spawned {
                    Err(e) => {
                        let _ = result_tx.send(Err(e));
                    }
                    Ok(child) => {
                        match result_tx.send(Ok(child)) {
                            Ok(()) => {
                                // Stay alive as the pdeathsig parent until
                                // owner drop closes `shutdown_tx`.
                                let _ = shutdown_rx.recv();
                            }
                            Err(mpsc::SendError(Ok(mut orphan))) => {
                                // Owner vanished before handoff: clean only
                                // the just-spawned owned handle.
                                let _ = orphan.kill();
                                let _ = orphan.wait();
                            }
                            Err(_) => {}
                        }
                    }
                }
            })
            .map_err(SpawnError::spawn_failed)?;

        let child = result_rx.recv().map_err(|_| SpawnError::SpawnFailed {
            kind: io::ErrorKind::Other,
        })??;
        Ok(Self {
            child: Some(child),
            shutdown: Some(shutdown_tx),
            supervisor: Some(supervisor),
            reap: Arc::new(ReapShared::new()),
        })
    }

    /// OS pid of the owned child.
    pub fn id(&self) -> u32 {
        self.child.as_ref().map(Child::id).unwrap_or(0)
    }

    /// Non-blocking reap check on the owned handle only.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match self.child.as_mut() {
            Some(child) => child.try_wait(),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "owned child is no longer held",
            )),
        }
    }

    /// Blocking reap of the owned handle only.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        match self.child.as_mut() {
            Some(child) => child.wait(),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "owned child is no longer held",
            )),
        }
    }

    /// Kill the owned handle only (SIGKILL).
    pub fn kill(&mut self) -> io::Result<()> {
        match self.child.as_mut() {
            Some(child) => child.kill(),
            None => Ok(()),
        }
    }

    /// Force-kill and reap only the owned handle, waiting at most `timeout`.
    ///
    /// Never signals a bare pid. Returns the exit code when the child is
    /// reaped before the bound; `None` means the bound elapsed. This bounded
    /// probe does not transfer ownership; use [`Self::terminate_and_reap`]
    /// when a timed-out reap must remain tracked to confirmed exit.
    pub fn force_kill_and_reap(&mut self, timeout: Duration) -> Option<i32> {
        let deadline = Instant::now() + timeout;
        let mut killed = false;
        loop {
            match self.try_wait() {
                Ok(Some(status)) => return status.code(),
                Ok(None) => {
                    if !killed {
                        let _ = self.kill();
                        killed = true;
                    }
                    if Instant::now() >= deadline {
                        return None;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => {
                    if !killed {
                        let _ = self.kill();
                    }
                    return None;
                }
            }
        }
    }

    /// Force-kill the owned child, then keep ownership until its exit is
    /// confirmed.
    ///
    /// A fast bounded reap keeps shutdown latency low. If the bound expires,
    /// exactly one background blocking reaper takes over the same owned child
    /// (and its supervisor guard) and reports the real result. Ownership is
    /// moved only after the reaper thread exists; if that thread cannot be
    /// created (or rejects the handoff), the child, supervisor guard, and
    /// shutdown sender stay retained here and `Unavailable` is returned. No
    /// bare pid is ever signalled and no polling loop continues after the
    /// bound.
    pub fn terminate_and_reap(&mut self, timeout: Duration) -> ReapOutcome {
        self.terminate_and_reap_with(timeout, spawn_child_reaper, real_child_wait)
    }

    fn terminate_and_reap_with<S, W>(
        &mut self,
        timeout: Duration,
        spawner: S,
        wait: W,
    ) -> ReapOutcome
    where
        S: FnOnce(
            mpsc::Receiver<ReaperOwned>,
            Arc<ReapShared>,
            ReaperWait,
        ) -> io::Result<thread::JoinHandle<()>>,
        W: Fn(&mut Child) -> io::Result<ExitStatus> + Send + Sync + 'static,
    {
        // A missing owned handle is not evidence of reap. Observe the shared
        // lifecycle first so a repeated call cannot invent `Reaped`.
        if let Some(outcome) = self.existing_reap_outcome() {
            return outcome;
        }
        let deadline = Instant::now() + timeout;
        let mut killed = false;
        loop {
            match self.try_wait() {
                Ok(Some(status)) => {
                    self.reap.confirm(status.code());
                    self.finish_ownership();
                    return ReapOutcome::Reaped(status.code());
                }
                Ok(None) => {
                    if !killed {
                        let _ = self.kill();
                        killed = true;
                    }
                    if Instant::now() >= deadline {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => {
                    if !killed {
                        let _ = self.kill();
                    }
                    break;
                }
            }
        }
        self.start_background_reaper_with(spawner, wait)
    }

    /// Force-kill the owned child and wait for confirmed exit on the calling
    /// thread after a short bounded fast path.
    ///
    /// This is intended for the dedicated owner thread: once the bound
    /// expires, that same thread retains the handle and blocks (retrying OS
    /// wait errors with backoff) instead of spawning another reaper. If a
    /// previous call already handed the child to one background reaper, this
    /// path waits on that same real completion rather than reporting a missing
    /// handle as reaped. App-exit and async callers still wait only within
    /// their own fixed bounds and observe the real result through the owner
    /// lifetime.
    pub fn terminate_and_reap_blocking(&mut self, timeout: Duration) -> ReapOutcome {
        self.terminate_and_reap_blocking_with(timeout, real_child_wait)
    }

    fn terminate_and_reap_blocking_with<W>(&mut self, timeout: Duration, wait: W) -> ReapOutcome
    where
        W: Fn(&mut Child) -> io::Result<ExitStatus>,
    {
        if let Some(outcome) = self.existing_reap_outcome_blocking() {
            return outcome;
        }
        let deadline = Instant::now() + timeout;
        let mut killed = false;
        loop {
            match self.try_wait() {
                Ok(Some(status)) => {
                    self.reap.confirm(status.code());
                    self.finish_ownership();
                    return ReapOutcome::Reaped(status.code());
                }
                Ok(None) => {
                    if !killed {
                        let _ = self.kill();
                        killed = true;
                    }
                    if Instant::now() >= deadline {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => {
                    if !killed {
                        let _ = self.kill();
                    }
                    break;
                }
            }
        }
        if self.child.is_none() {
            // Never convert a missing handle into a confirmed reap. Only an
            // observed confirmation (or the shared reaper's completion) may
            // return `Reaped`.
            return self
                .existing_reap_outcome_blocking()
                .unwrap_or(ReapOutcome::Unavailable);
        }
        #[cfg(test)]
        self.reap.notify_test_blocking_entered();
        let status = wait_until_confirmed(|| match self.child.as_mut() {
            Some(child) => wait(child),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "owned child is no longer held",
            )),
        });
        self.reap.confirm(status.code());
        self.finish_ownership();
        ReapOutcome::Reaped(status.code())
    }

    fn existing_reap_outcome(&mut self) -> Option<ReapOutcome> {
        match self.reap.status() {
            ReapStatus::Owned => None,
            ReapStatus::Pending => Some(ReapOutcome::Pending(ReapWaiter::new(Arc::clone(
                &self.reap,
            )))),
            ReapStatus::Confirmed(code) => {
                self.finish_ownership();
                Some(ReapOutcome::Reaped(code))
            }
            ReapStatus::Failed => Some(ReapOutcome::Unavailable),
        }
    }

    fn existing_reap_outcome_blocking(&mut self) -> Option<ReapOutcome> {
        match self.reap.status() {
            ReapStatus::Owned => None,
            ReapStatus::Pending => {
                // `cfg(test)`-only deterministic handshake: signal INSIDE the
                // actual blocking wait path (same shared-state wait the owner
                // thread blocks on) before parking. Production has no hook.
                #[cfg(test)]
                self.reap.notify_test_blocking_entered();
                let waiter = ReapWaiter::new(Arc::clone(&self.reap));
                match waiter.wait_blocking_status() {
                    ReapStatus::Confirmed(code) => {
                        self.finish_ownership();
                        Some(ReapOutcome::Reaped(code))
                    }
                    _ => Some(ReapOutcome::Unavailable),
                }
            }
            ReapStatus::Confirmed(code) => {
                self.finish_ownership();
                Some(ReapOutcome::Reaped(code))
            }
            ReapStatus::Failed => Some(ReapOutcome::Unavailable),
        }
    }

    fn finish_ownership(&mut self) {
        self.child = None;
        self.shutdown.take();
        if let Some(handle) = self.supervisor.take() {
            let _ = handle.join();
        }
    }

    fn reap_retained_until_confirmed(&mut self) {
        if self.child.is_none() {
            self.finish_ownership();
            return;
        }
        let status = wait_until_confirmed(|| self.wait());
        self.reap.confirm(status.code());
        self.finish_ownership();
    }

    fn start_background_reaper_with<S, W>(&mut self, spawner: S, wait: W) -> ReapOutcome
    where
        S: FnOnce(
            mpsc::Receiver<ReaperOwned>,
            Arc<ReapShared>,
            ReaperWait,
        ) -> io::Result<thread::JoinHandle<()>>,
        W: Fn(&mut Child) -> io::Result<ExitStatus> + Send + Sync + 'static,
    {
        if let Some(outcome) = self.existing_reap_outcome() {
            return outcome;
        }
        if self.child.is_none() {
            // No owned handle and no recorded confirmation: fail closed.
            return ReapOutcome::Unavailable;
        }

        // Publish `Pending` before the thread can observe completion, so even
        // a very fast real wait cannot be overwritten by the handoff path.
        if !self.reap.mark_pending() {
            return self
                .existing_reap_outcome()
                .unwrap_or(ReapOutcome::Unavailable);
        }

        let wait: ReaperWait = Arc::new(wait);
        let (handoff_tx, handoff_rx) = mpsc::channel::<ReaperOwned>();

        // The closure cannot capture the child. Create the reaper thread
        // first; only a successful spawn is allowed to take ownership.
        let spawned = spawner(handoff_rx, Arc::clone(&self.reap), wait);
        let handle = match spawned {
            Ok(handle) => handle,
            Err(_) => {
                self.reap.mark_owned();
                return ReapOutcome::Unavailable;
            }
        };

        let child = match self.child.take() {
            Some(child) => child,
            None => {
                // The reaper will observe a closed handoff and mark failure;
                // dropping the sender guarantees it cannot block forever.
                drop(handoff_tx);
                let _ = handle.join();
                self.reap.fail();
                let confirmed = matches!(self.reap.status(), ReapStatus::Confirmed(_));
                self.finish_ownership();
                return if confirmed {
                    match self.reap.status() {
                        ReapStatus::Confirmed(code) => ReapOutcome::Reaped(code),
                        _ => ReapOutcome::Unavailable,
                    }
                } else {
                    ReapOutcome::Unavailable
                };
            }
        };
        let owned = ReaperOwned {
            child,
            shutdown: self.shutdown.take(),
            supervisor: self.supervisor.take(),
        };

        match handoff_tx.send(owned) {
            Ok(()) => ReapOutcome::Pending(ReapWaiter::new(Arc::clone(&self.reap))),
            Err(mpsc::SendError(owned)) => {
                // The reaper exited before accepting ownership. Restore the
                // exact owned handle and guard instead of dropping them.
                self.child = Some(owned.child);
                self.shutdown = owned.shutdown;
                self.supervisor = owned.supervisor;
                self.reap.mark_owned();
                let _ = handle.join();
                ReapOutcome::Unavailable
            }
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if self.child.is_none() {
            return;
        }
        // A timed-out bounded reap is handed to one background blocking
        // reaper; the owned child handle is never discarded while its exit is
        // unconfirmed. If no reaper can be created, retain the still-owned
        // handle here and wait (with error backoff) rather than dropping it
        // as if reaped.
        match self.terminate_and_reap(FORCED_REAP_TIMEOUT) {
            ReapOutcome::Reaped(_) | ReapOutcome::Pending(_) => {}
            ReapOutcome::Unavailable => self.reap_retained_until_confirmed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    const SECRET: &str = "s3cret-t0ken-value-xyz";

    #[cfg(target_os = "linux")]
    fn take_injected_failure(counter: &std::sync::atomic::AtomicUsize) -> bool {
        use std::sync::atomic::Ordering;
        let mut current = counter.load(Ordering::SeqCst);
        while current > 0 {
            match counter.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    fn wait_for_exit(child: &mut ManagedChild, timeout: Duration) -> Option<ExitStatus> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            match child.try_wait() {
                Ok(Some(s)) => return Some(s),
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => return None,
            }
        }
        None
    }

    #[test]
    fn rejects_non_absolute_without_touching_path() {
        for rel in ["", "sleep", "./sleep", "usr/bin/sleep"] {
            let err = ManagedChild::spawn(
                Path::new(rel),
                &[] as &[&OsStr],
                &[] as &[(&OsStr, &OsStr)],
                true,
            )
            .unwrap_err();
            assert_eq!(err, SpawnError::NotAbsolute, "input: {rel:?}");
        }
    }

    #[test]
    fn errors_carry_no_private_data() {
        let err = ManagedChild::spawn(
            Path::new("relative-with-secret"),
            &[SECRET],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .unwrap_err();
        for s in [format!("{err:?}"), format!("{err}")] {
            assert!(!s.contains(SECRET), "leaked: {s}");
            assert!(!s.contains("relative-with-secret"), "leaked: {s}");
        }
        let disp = format!("{}", SpawnError::HardeningUnavailable("windows-job"));
        assert!(disp.contains("orphan prevention"));
        assert!(!disp.contains(SECRET));
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
    #[test]
    fn supervisor_thread_stays_alive_while_child_runs() {
        let mut child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn sleep");
        let pid = child.id();
        // Supervisor (creating) thread must not have disappeared.
        let sup_alive = child.supervisor.as_ref().map(|h| !h.is_finished());
        assert_eq!(
            sup_alive,
            Some(true),
            "supervisor thread exited while child runs"
        );
        // Child persists while owner alive.
        std::thread::sleep(Duration::from_millis(200));
        assert!(child.try_wait().unwrap().is_none());
        assert!(pid_alive(pid), "owned child died spuriously");
        child.kill().ok();
        let _ = wait_for_exit(&mut child, Duration::from_secs(10));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn caller_thread_may_exit_child_survives() {
        // Spawn from a short-lived thread; the returned owner moves to this
        // thread. pdeathsig parent must be the supervisor, not the caller.
        let child = std::thread::spawn(|| {
            ManagedChild::spawn(
                Path::new("/usr/bin/sleep"),
                &["30"],
                &[] as &[(&OsStr, &OsStr)],
                true,
            )
            .expect("spawn sleep")
        })
        .join()
        .expect("spawner thread");
        let mut child = child;
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(200));
        assert!(child.try_wait().unwrap().is_none());
        assert!(pid_alive(pid), "child died with short-lived caller thread");
        child.kill().ok();
        let _ = wait_for_exit(&mut child, Duration::from_secs(10));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn owned_child_dies_on_explicit_kill_sibling_untouched() {
        // NOTE: this exercises explicit kill + reap of the owned handle
        // (Drop-equivalent path). It does NOT kill the parent process and
        // therefore does NOT verify forced-parent-death (pdeathsig); see
        // `forced_parent_death_kills_managed_child` for that.
        let mut survivor = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn survivor");
        let survivor_pid = survivor.id();
        let victim_pid: u32 = {
            let mut victim = ManagedChild::spawn(
                Path::new("/usr/bin/sleep"),
                &["30"],
                &[] as &[(&OsStr, &OsStr)],
                true,
            )
            .expect("spawn victim");
            let pid = victim.id();
            assert_ne!(pid, survivor_pid);
            // Owner kill: explicit kill + reap of owned handle only.
            victim.kill().expect("kill owned victim");
            let status = wait_for_exit(&mut victim, Duration::from_secs(10)).expect("reap victim");
            assert!(!status.success());
            pid
        };
        // Only probes/kills of owned handles above; no unrelated pid touched.
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !pid_alive(victim_pid),
            "victim still alive after owner kill"
        );
        assert!(
            survivor.try_wait().unwrap().is_none(),
            "sibling harmed by victim kill"
        );
        assert!(pid_alive(survivor_pid));
        survivor.kill().ok();
        let _ = wait_for_exit(&mut survivor, Duration::from_secs(10));
    }

    #[cfg(target_os = "linux")]
    fn proc_state(pid: u32) -> Option<char> {
        // `/proc/<pid>/stat` format: `pid (comm) state ...`. `comm` may
        // contain spaces/parens, so split after the last `)`.
        let content = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let close = content.rfind(')')?;
        content[close + 1..]
            .split_whitespace()
            .next()?
            .chars()
            .next()
    }

    #[cfg(target_os = "linux")]
    fn is_dead_or_zombie(pid: u32) -> bool {
        // Zombie (or already-reaped, i.e. `/proc` entry gone) counts as dead:
        // SIGKILL was delivered; only init-side reaping may still be pending.
        // The grandchild is reparented away from us so we cannot `wait()` it.
        match proc_state(pid) {
            None => true,
            Some('Z') | Some('X') | Some('x') => true,
            Some(_) => false,
        }
    }

    #[cfg(target_os = "linux")]
    fn raw_kill(pid: u32, sig: i32) -> i32 {
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        // SAFETY: integer-only signal to a test-owned pid; no pointer use.
        unsafe { kill(pid as i32, sig) }
    }

    /// Disposable helper-parent entry point.
    ///
    /// Runs only when `MANAGED_PROC_HELPER_MODE=1` (set by
    /// `forced_parent_death_kills_managed_child` when it re-executes this
    /// test binary in an isolated process). Spawns one owned `sleep` child,
    /// prints `HELPER_READY pid=<child>` on stdout (the private pipe), then
    /// blocks until SIGKILLed. Normal test runs (env unset) return at once.
    #[cfg(target_os = "linux")]
    #[test]
    fn __helper_parent_blocks_with_managed_child() {
        if std::env::var_os("MANAGED_PROC_HELPER_MODE").as_deref() != Some(OsStr::new("1")) {
            return;
        }
        let child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["120"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("helper spawn sleep");
        let pid = child.id();
        println!("HELPER_READY pid={pid}");
        {
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
        }
        // Keep the owner (and its supervisor thread, the pdeathsig parent)
        // alive until SIGKILL arrives. SIGKILL skips `Drop`, so only kernel
        // pdeathsig delivery can stop the grandchild. Intentionally never
        // kills/reaps here; the outer test owns cleanup.
        std::mem::forget(child);
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }

    /// Forced-parent-death regression (Linux only).
    ///
    /// Spawns a disposable helper parent as an isolated process (re-executed
    /// test binary running `__helper_parent_blocks_with_managed_child`),
    /// reads the managed grandchild pid over the piped-stdout private pipe,
    /// SIGKILLs the helper PARENT (skipping destructors), reaps the helper,
    /// then polls the grandchild until gone/zombie within a bound.
    /// Only the two test-owned pids are ever signalled. No PID-reuse
    /// guarantee is asserted: the check is only that the just-reported
    /// test-owned pid is gone/zombie shortly after its parent's SIGKILL.
    #[cfg(target_os = "linux")]
    #[test]
    fn forced_parent_death_kills_managed_child() {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};

        const SIGKILL: i32 = 9;

        let exe = std::env::current_exe().expect("current exe");
        let mut helper = Command::new(&exe)
            .arg("__helper_parent_blocks_with_managed_child")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env("MANAGED_PROC_HELPER_MODE", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn helper parent");
        let helper_pid = helper.id();

        // Blocking line scan on a side thread so the READY wait is bounded.
        let (ready_tx, ready_rx) = mpsc::channel::<Option<u32>>();
        let stdout = helper.stdout.take().expect("helper stdout piped");
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let mut found: Option<u32> = None;
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        if let Some(pos) = line.find("HELPER_READY pid=") {
                            let rest = &line[pos + "HELPER_READY pid=".len()..];
                            let digits: String =
                                rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                            if let Ok(pid) = digits.parse::<u32>() {
                                found = Some(pid);
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = ready_tx.send(found);
        });

        let grandchild_pid = match ready_rx.recv_timeout(Duration::from_secs(15)) {
            Ok(Some(pid)) => pid,
            Ok(None) => {
                let _ = raw_kill(helper_pid, SIGKILL);
                let _ = helper.wait();
                panic!("helper did not report grandchild pid (EOF/parse)");
            }
            Err(_) => {
                let _ = raw_kill(helper_pid, SIGKILL);
                let _ = helper.wait();
                panic!("timeout waiting for helper READY line");
            }
        };
        assert_ne!(grandchild_pid, helper_pid, "pid aliasing");
        // Precondition: managed grandchild alive (not gone/zombie) while its
        // helper parent still runs.
        assert!(
            !is_dead_or_zombie(grandchild_pid),
            "grandchild not alive before parent SIGKILL"
        );

        // Kill the helper PARENT only; destructors are skipped by SIGKILL.
        assert_eq!(raw_kill(helper_pid, SIGKILL), 0, "SIGKILL helper parent");

        // Reap the direct helper child (zombie) within a bound.
        let start = Instant::now();
        let helper_status = loop {
            match helper.try_wait() {
                Ok(Some(s)) => break Some(s),
                Ok(None) => {
                    if start.elapsed() > Duration::from_secs(10) {
                        break None;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break None,
            }
        };
        assert!(
            helper_status.is_some(),
            "helper parent not reaped after SIGKILL"
        );
        // Death-by-signal is not `success`; no finer exit semantics asserted.
        assert!(!helper_status.unwrap().success());

        // The grandchild reparents away from us, so polling (not `wait()`)
        // observes it; zombies count as dead (SIGKILL already delivered).
        let start = Instant::now();
        let died = loop {
            if is_dead_or_zombie(grandchild_pid) {
                break true;
            }
            if start.elapsed() > Duration::from_secs(10) {
                break is_dead_or_zombie(grandchild_pid);
            }
            thread::sleep(Duration::from_millis(20));
        };
        if !died {
            // Cleanup only the test-owned grandchild before failing.
            let _ = raw_kill(grandchild_pid, SIGKILL);
            thread::sleep(Duration::from_millis(500));
            let gone = is_dead_or_zombie(grandchild_pid);
            panic!("managed child survived forced parent death (post-cleanup gone={gone})");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn timed_out_reap_keeps_owned_child_until_confirmed() {
        let mut child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn sleep");

        // A zero bound forces the delayed path deterministically: the child is
        // force-killed, handed to one background blocking reaper, and only that
        // reaper may report the confirmed exit.
        let outcome = child.terminate_and_reap(Duration::ZERO);
        let reaped = match outcome {
            ReapOutcome::Pending(waiter) => waiter.wait_blocking(),
            ReapOutcome::Reaped(_) => true,
            ReapOutcome::Unavailable => false,
        };
        assert!(reaped, "bounded timeout must preserve ownership until reap");
        assert_eq!(child.id(), 0, "confirmed reap releases the owned handle");

        // Repeating after confirmed cleanup must be inert, never a second
        // signal to an unrelated process.
        assert!(matches!(
            child.terminate_and_reap(Duration::ZERO),
            ReapOutcome::Reaped(None)
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn repeated_reap_before_confirmation_never_reports_reaped() {
        use std::sync::{Condvar, Mutex};

        struct Gate {
            entered: Mutex<bool>,
            entered_cv: Condvar,
            release: Mutex<bool>,
            release_cv: Condvar,
        }

        let gate = Arc::new(Gate {
            entered: Mutex::new(false),
            entered_cv: Condvar::new(),
            release: Mutex::new(false),
            release_cv: Condvar::new(),
        });
        let gate_for_wait = Arc::clone(&gate);
        let wait = move |child: &mut Child| -> io::Result<ExitStatus> {
            {
                let mut entered = gate_for_wait.entered.lock().unwrap();
                *entered = true;
                gate_for_wait.entered_cv.notify_all();
            }
            {
                let mut release = gate_for_wait.release.lock().unwrap();
                while !*release {
                    release = gate_for_wait.release_cv.wait(release).unwrap();
                }
            }
            child.wait()
        };

        let mut child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn sleep");
        let pid = child.id();

        // Force the delayed path and hold the injected wait before any real
        // owned wait is allowed to confirm the exit.
        let first = child.terminate_and_reap_with(Duration::ZERO, spawn_child_reaper, wait);
        assert!(matches!(first, ReapOutcome::Pending(_)));
        {
            let mut entered = gate.entered.lock().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !*entered {
                let now = Instant::now();
                assert!(now < deadline, "reaper never entered the injected wait");
                let waited = gate
                    .entered_cv
                    .wait_timeout(entered, deadline - now)
                    .unwrap();
                entered = waited.0;
            }
        }

        // Repeat the bounded operation BEFORE the owned child can reap. A
        // missing handle must not be interpreted as `Reaped`.
        let second = child.terminate_and_reap(Duration::ZERO);
        assert!(
            matches!(second, ReapOutcome::Pending(_)),
            "repeated terminate must not report Reaped before real confirmation"
        );
        drop(second);

        // The blocking owner path must wait on that same real completion.
        // Deterministic handshake INSIDE the actual blocking wait path: the
        // `cfg(test)` hook in `existing_reap_outcome_blocking` signals when
        // the owner thread enters the real shared-state wait, before any
        // assertion. No sleep, no production behavior change.
        let (blocking_entered_tx, blocking_entered_rx) = mpsc::channel::<()>();
        child
            .reap
            .set_test_blocking_entered(Some(blocking_entered_tx));
        let blocking = thread::spawn(move || {
            let outcome = child.terminate_and_reap_blocking(Duration::ZERO);
            (child, outcome)
        });
        blocking_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("blocking owner entered actual shared-state wait");
        assert!(
            !blocking.is_finished(),
            "blocking owner path reported completion before the real owned wait"
        );

        // Now allow the single owned wait to run.
        {
            let mut release = gate.release.lock().unwrap();
            *release = true;
            gate.release_cv.notify_all();
        }
        let (child, third) = blocking.join().expect("blocking owner thread");
        assert!(
            matches!(third, ReapOutcome::Reaped(_)),
            "blocking owner path must report only the confirmed real reap"
        );
        assert_eq!(child.id(), 0, "confirmed reap releases the owned handle");
        assert!(
            !pid_alive(pid),
            "owned child must be gone after confirmation"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reaper_thread_creation_failure_retains_owned_child_for_later_reap() {
        let mut child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn sleep");
        let pid = child.id();

        // Injected thread-creation failure must not consume the owned handle.
        let outcome = child.terminate_and_reap_with(
            Duration::ZERO,
            |_rx: mpsc::Receiver<ReaperOwned>,
             _shared: Arc<ReapShared>,
             _wait: ReaperWait|
             -> io::Result<thread::JoinHandle<()>> {
                Err(io::Error::from(io::ErrorKind::Other))
            },
            real_child_wait,
        );
        assert!(matches!(outcome, ReapOutcome::Unavailable));
        assert_eq!(child.id(), pid, "failed reaper spawn must retain the child");

        // A later retry on the same owned handle must confirm the reap.
        let start = Instant::now();
        let later = child.terminate_and_reap_blocking(Duration::from_secs(5));
        assert!(matches!(later, ReapOutcome::Reaped(_)));
        assert_eq!(child.id(), 0, "later confirmed reap releases the handle");
        assert!(start.elapsed() < Duration::from_secs(5));
        assert!(!pid_alive(pid), "owned child must be gone after later reap");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reaper_handoff_failure_retains_owned_child_for_later_reap() {
        let mut child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn sleep");
        let pid = child.id();

        // The spawned receiver is closed before ownership is sent. The sender
        // must return the exact child/shutdown/supervisor to this owner.
        let outcome = child.terminate_and_reap_with(
            Duration::ZERO,
            |rx: mpsc::Receiver<ReaperOwned>,
             _shared: Arc<ReapShared>,
             _wait: ReaperWait|
             -> io::Result<thread::JoinHandle<()>> {
                drop(rx);
                thread::Builder::new()
                    .name("test-handoff-drop".into())
                    .spawn(|| {})
                    .map_err(|_| io::Error::from(io::ErrorKind::Other))
            },
            real_child_wait,
        );
        assert!(matches!(outcome, ReapOutcome::Unavailable));
        assert_eq!(
            child.id(),
            pid,
            "failed ownership handoff must retain the child"
        );

        let start = Instant::now();
        let later = child.terminate_and_reap_blocking(Duration::from_secs(5));
        assert!(matches!(later, ReapOutcome::Reaped(_)));
        assert_eq!(child.id(), 0);
        assert!(start.elapsed() < Duration::from_secs(5));
        assert!(!pid_alive(pid));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reaper_wait_error_is_retried_and_later_confirmed() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let failures = Arc::new(AtomicUsize::new(1));
        let remaining = Arc::clone(&failures);
        let wait = move |child: &mut Child| -> io::Result<ExitStatus> {
            if take_injected_failure(&remaining) {
                Err(io::Error::other("injected transient wait failure"))
            } else {
                child.wait()
            }
        };

        let mut child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn sleep");
        let pid = child.id();
        let start = Instant::now();
        let outcome = child.terminate_and_reap_with(Duration::ZERO, spawn_child_reaper, wait);
        let reaped = match outcome {
            ReapOutcome::Pending(waiter) => waiter.wait_blocking(),
            ReapOutcome::Reaped(_) => true,
            ReapOutcome::Unavailable => false,
        };
        assert!(
            reaped,
            "transient wait failure must not be treated as success"
        );
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(failures.load(Ordering::SeqCst), 0);
        assert_eq!(child.id(), 0);
        assert!(!pid_alive(pid));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn blocking_owner_wait_error_is_retried_and_later_confirmed() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let failures = Arc::new(AtomicUsize::new(1));
        let remaining = Arc::clone(&failures);
        let wait = move |child: &mut Child| -> io::Result<ExitStatus> {
            if take_injected_failure(&remaining) {
                Err(io::Error::other("injected transient wait failure"))
            } else {
                child.wait()
            }
        };

        let mut child = ManagedChild::spawn(
            Path::new("/usr/bin/sleep"),
            &["30"],
            &[] as &[(&OsStr, &OsStr)],
            true,
        )
        .expect("spawn sleep");
        let pid = child.id();
        let start = Instant::now();
        let outcome = child.terminate_and_reap_blocking_with(Duration::ZERO, wait);
        assert!(matches!(outcome, ReapOutcome::Reaped(_)));
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(failures.load(Ordering::SeqCst), 0);
        assert_eq!(child.id(), 0);
        assert!(!pid_alive(pid));
    }

    #[allow(dead_code)]
    fn _absolute_path_helper_mirrors_spawn_rule(p: &Path) -> PathBuf {
        assert!(p.is_absolute());
        p.to_path_buf()
    }
}
