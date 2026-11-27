//! Isolated durable persistence owner for automatic-hash state.
//!
//! One managed state directory has at most one owner process. The owner holds
//! an OS advisory lock for its whole lifetime, performs every namespace/state
//! filesystem operation, and reports an explicit outcome for each commit:
//! before-rename failure, published-but-directory-sync-failed, conflict, or
//! an externally changed file. The UI process never touches those files while
//! holding a lifecycle, state, or slot lock.
//!
//! The owner is a child process rather than a thread because a thread blocked
//! in `fsync`/`rename` cannot be cancelled or bounded at process exit. Killing
//! a child is bounded on Linux outside uninterruptible kernel waits; such a
//! wait (for example a hung network filesystem) can delay reaping until the
//! kernel returns. The advisory lock remains held until the child really dies,
//! so no replacement owner can start while an old operation is unresolved.
//!
//! Protocol messages carry only a state-directory path, canonical state
//! bytes, and fixed status codes. They never carry URLs, tokens, filenames, or
//! OS error text.
//!
//! The advisory lock serializes only owners that honor it. An older build or
//! another writer that ignores the lock can interleave with this owner; a
//! check-then-rename cannot close that window. This implementation never
//! claims that mixed old/new versions are safe together.
//!
//! Publication is a two-phase durable marker protocol. A pending marker is
//! synced before the rename. Once the renamed state and its directory entry
//! are synced, the marker becomes `durable`; a later lifecycle-authorized
//! caller turns it into `acked`, then removes it. A crash or a stale lifecycle
//! before `acked` leaves an unacknowledged marker and the next owner fails
//! closed. A crash after `acked` (including a lost reply) may expose the
//! already-authorized durable state; this protocol makes no exactly-once or
//! reply-derived outcome promise.

use std::collections::{HashSet, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::hash_state;

pub(crate) const HASH_STATE_FILE: &str = "automatic-hash.json";
pub(crate) const HASH_LOCK_FILE: &str = "automatic-hash.lock";
pub(crate) const HASH_PENDING_FILE: &str = "automatic-hash.pending";
const MARKER_HEADER: &[u8] = b"SURGUI-HASH-MARKER-2";
const LEGACY_PENDING_MARKER: &[u8] = b"SURGUI-HASH-PENDING-1\n";
const MAX_MARKER_BYTES: usize = 512;
const HELPER_ARG: &str = "--automatic-hash-persist-helper";
const MAX_STATE_BYTES: usize = 32 * 1024 * 1024;
const MAX_PROTOCOL_LINE: usize = MAX_STATE_BYTES * 2;
const MAX_TEMP_CLEANUP: usize = 64;
const MAX_UNRESOLVED_DIRECTORIES: usize = 32;
const MAX_LIVE_HELPERS: usize = 64;
const QUEUE_CAPACITY: usize = 4;
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const COMMIT_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const KILL_WAIT: Duration = Duration::from_secs(2);
const FAULT_WAIT: Duration = Duration::from_secs(30);
const REAPER_SPAWN_ATTEMPTS: usize = 3;
const REAPER_SPAWN_BACKOFF: Duration = Duration::from_millis(2);
const REAPER_WAIT_BACKOFF_MIN: Duration = Duration::from_millis(1);
const REAPER_WAIT_BACKOFF_MAX: Duration = Duration::from_millis(50);

/// Test-only deterministic fault configuration. Production callers never set
/// it; the helper only acts on it when it arrives in the init request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FaultConfig {
    pub(crate) pause: Option<String>,
    pub(crate) fail: Option<String>,
    pub(crate) cleanup: Option<String>,
    pub(crate) dir: Option<PathBuf>,
}

#[cfg(test)]
std::thread_local! {
    static LAST_PERSIST_DIAGNOSTIC: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn record_persist_diagnostic(message: String) {
    eprintln!("{message}");
    LAST_PERSIST_DIAGNOSTIC.with(|slot| {
        *slot.borrow_mut() = Some(message);
    });
}

/// Test-only access to the last sanitized persistence diagnostic observed on
/// this thread. It never carries paths, credentials, state bytes, or payloads.
#[cfg(test)]
pub(crate) fn last_persist_diagnostic() -> Option<String> {
    LAST_PERSIST_DIAGNOSTIC.with(|slot| slot.borrow().clone())
}

#[cfg(test)]
pub(crate) fn clear_persist_diagnostic() {
    LAST_PERSIST_DIAGNOSTIC.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[cfg(test)]
fn wire_status_label(status: &str) -> &'static str {
    match status {
        "prepared" => "prepared",
        "before_rename" => "before_rename",
        "conflict" => "conflict",
        "external_change" => "external_change",
        "unconfirmed" => "unconfirmed",
        "invalid" => "invalid",
        "committed" => "committed",
        "ready" => "ready",
        "busy" => "busy",
        "malformed" => "malformed",
        "unresolved" => "unresolved",
        "no_reply" => "no_reply",
        "error" => "error",
        _ => "other",
    }
}

#[cfg(test)]
macro_rules! persist_diag {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        record_persist_diagnostic(format!($($arg)*));
    }};
}

macro_rules! prepare_diag {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        record_prepare_diagnostic($($arg)*);
    }};
}

macro_rules! ack_diag {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        record_ack_diagnostic($($arg)*);
    }};
}

macro_rules! open_diag {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        record_open_diagnostic($($arg)*);
    }};
}

#[cfg(test)]
fn record_open_diagnostic(
    phase: &str,
    outcome: &str,
    elapsed: Duration,
    status_after: Option<DomainStatus>,
    wire_status: Option<&str>,
) {
    let status_after = match status_after {
        Some(status) => format!("{status:?}"),
        None => "none".into(),
    };
    let response_status = wire_status.map(wire_status_label).unwrap_or("none");
    persist_diag!(
        "hash-persist open phase={phase} outcome={outcome} elapsed_ms={} status_after={status_after} response_status={response_status}",
        elapsed.as_millis()
    );
}

#[cfg(test)]
fn record_prepare_diagnostic(
    domain: &PersistenceDomain,
    phase: &str,
    outcome: &str,
    elapsed: Duration,
    base: u64,
    status_before: DomainStatus,
    response_revision: Option<u64>,
) {
    let revision = domain.lock_state().revision;
    let status_after = domain.status();
    let response_revision = response_revision
        .map(|revision| revision.to_string())
        .unwrap_or_else(|| "none".into());
    persist_diag!(
        "hash-persist prepare phase={phase} outcome={outcome} elapsed_ms={} status_before={status_before:?} status_after={status_after:?} revision={revision} base={base} response_revision={response_revision}",
        elapsed.as_millis()
    );
}

#[cfg(test)]
fn record_ack_diagnostic(
    inner: &Arc<Inner>,
    phase: &str,
    outcome: &str,
    elapsed: Duration,
    expected_revision: u64,
    status_before: DomainStatus,
    response_revision: Option<u64>,
) {
    let state = lock_inner_state(inner);
    let revision = state.revision;
    let prepared = state.prepared;
    let status_after = state.status;
    drop(state);
    let prepared = prepared
        .map(|revision| revision.to_string())
        .unwrap_or_else(|| "none".into());
    let response_revision = response_revision
        .map(|revision| revision.to_string())
        .unwrap_or_else(|| "none".into());
    persist_diag!(
        "hash-persist ack phase={phase} outcome={outcome} elapsed_ms={} status_before={status_before:?} status_after={status_after:?} revision={revision} expected_revision={expected_revision} prepared={prepared} response_revision={response_revision}",
        elapsed.as_millis()
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PersistenceError {
    Busy,
    Malformed,
    Unavailable,
    Unresolved,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CommitOutcome {
    /// Published and the directory entry is synced.
    Committed(u64),
    /// The operation definitely failed before the rename, so the previous
    /// published state is still authoritative.
    BeforeRename,
    /// The rename may have been published but durability/ownership is
    /// unresolved. The domain is quarantined and readers must report the
    /// state as unconfirmed.
    Unconfirmed,
    /// The supplied base revision no longer matches the owner's published
    /// revision. Nothing was written.
    Conflict,
    /// The file was changed by another (older/foreign) writer. Nothing was
    /// written and the domain is quarantined.
    ExternalChange,
}

/// Result of the durable preparation phase. The state is durable but not yet
/// lifecycle-authorized; callers must confirm or abandon it.
pub(crate) enum PrepareOutcome {
    Prepared(PreparedCommit),
    BeforeRename,
    Unconfirmed,
    Conflict,
    ExternalChange,
}

pub(crate) struct OpenState {
    pub namespace: Option<String>,
    pub bytes: Option<Vec<u8>>,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DomainStatus {
    Ready,
    Quarantined,
    Closed,
}

struct DomainState {
    status: DomainStatus,
    revision: u64,
    prepared: Option<u64>,
}

enum Command {
    Init {
        dir: PathBuf,
        fault: Option<FaultConfig>,
        reply: SyncSender<WireResponse>,
    },
    Commit {
        base: u64,
        data: Vec<u8>,
        reply: SyncSender<WireResponse>,
    },
    Ack {
        revision: u64,
        reply: SyncSender<WireResponse>,
    },
    #[cfg(test)]
    SetFault {
        fault: Option<FaultConfig>,
        reply: SyncSender<WireResponse>,
    },
    Shutdown {
        reply: SyncSender<WireResponse>,
    },
}

/// A publication whose state and directory entry are already durable but whose
/// lifecycle authorization has not yet been recorded. It must be confirmed or
/// explicitly abandoned; dropping it quarantines the owner and leaves the
/// unacknowledged marker for the next owner to reject.
pub(crate) struct PreparedCommit {
    inner: Arc<Inner>,
    revision: u64,
    resolved: bool,
}

/// A live helper process plus the global capacity reservation that must remain
/// held until the OS confirms the child was reaped. The lease travels with the
/// handle from the owning domain into the shared reaper, so a timeout or wait
/// error can never release capacity while the child may still exist.
struct ReapJob {
    child: Child,
    #[allow(dead_code)]
    permit: HelperPermit,
}

/// Bounded process-wide reservation for helper owners. A helper is counted
/// from immediately before its spawn attempt until a blocking wait/try-wait
/// confirms it exited, including while it is queued or in-flight in the shared
/// reaper. Exhaustion rejects a new owner before any child is created.
struct HelperSlots {
    live: AtomicUsize,
    limit: usize,
}

impl HelperSlots {
    fn new(limit: usize) -> Self {
        Self {
            live: AtomicUsize::new(0),
            limit,
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<HelperPermit> {
        let mut live = self.live.load(Ordering::SeqCst);
        loop {
            if live >= self.limit {
                return None;
            }
            match self.live.compare_exchange_weak(
                live,
                live + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(HelperPermit {
                        slots: Arc::clone(self),
                    })
                }
                Err(observed) => live = observed,
            }
        }
    }
}

struct HelperPermit {
    slots: Arc<HelperSlots>,
}

impl Drop for HelperPermit {
    fn drop(&mut self) {
        self.slots.live.fetch_sub(1, Ordering::SeqCst);
    }
}

fn helper_slots() -> &'static Arc<HelperSlots> {
    static SLOTS: OnceLock<Arc<HelperSlots>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(HelperSlots::new(MAX_LIVE_HELPERS)))
}

struct Inner {
    tx: SyncSender<Command>,
    child: Mutex<Option<ReapJob>>,
    state: Mutex<DomainState>,
}

pub(crate) struct PersistenceDomain {
    inner: Arc<Inner>,
}

impl PersistenceDomain {
    /// Spawn the owner, acquire the directory lock, and load canonical state.
    /// A busy lock fails closed; this never starts a replacement writer.
    pub(crate) fn open(
        directory: &Path,
        fault: Option<FaultConfig>,
    ) -> Result<(Arc<Self>, OpenState), PersistenceError> {
        Self::open_with(directory, fault, helper_slots(), || {
            reaper().ensure_started()
        })
    }

    fn open_with<F>(
        directory: &Path,
        fault: Option<FaultConfig>,
        slots: &Arc<HelperSlots>,
        mut ensure_reaper: F,
    ) -> Result<(Arc<Self>, OpenState), PersistenceError>
    where
        F: FnMut() -> bool,
    {
        #[cfg(test)]
        let started = Instant::now();
        let key = attempt_key(directory);
        if !begin_attempt(&key) {
            open_diag!("guard", "unavailable", started.elapsed(), None, None);
            return Err(PersistenceError::Unavailable);
        }
        // Reserve process-wide helper capacity before any spawn attempt. The
        // permit is moved into the child handle and is only released after a
        // wait confirms the OS reaped it.
        let permit = match slots.try_acquire() {
            Some(permit) => permit,
            None => {
                end_attempt(&key);
                open_diag!("slots", "unavailable", started.elapsed(), None, None);
                return Err(PersistenceError::Unavailable);
            }
        };
        // A helper may only be created once the shared reaper is alive. If the
        // worker cannot be created, refusing the owner is safer than creating
        // a child that might later have no reaper able to collect it.
        if !ensure_reaper() {
            end_attempt(&key);
            drop(permit);
            open_diag!("reaper", "unavailable", started.elapsed(), None, None);
            return Err(PersistenceError::Unavailable);
        }
        let mut keep_unresolved = false;
        let result = Self::open_registered(directory, fault, permit, &mut keep_unresolved);
        if result.is_ok() || !keep_unresolved {
            end_attempt(&key);
        }
        result
    }

    fn open_registered(
        directory: &Path,
        fault: Option<FaultConfig>,
        permit: HelperPermit,
        keep_unresolved: &mut bool,
    ) -> Result<(Arc<Self>, OpenState), PersistenceError> {
        #[cfg(test)]
        let started = Instant::now();
        let candidates = match helper_candidates() {
            Ok(candidates) => candidates,
            Err(error) => {
                open_diag!("spawn", "unavailable", started.elapsed(), None, None);
                return Err(error);
            }
        };
        let mut spawned = None;
        let mut last_error = None;
        for (program, arg) in candidates {
            let mut command = ProcessCommand::new(program);
            if let Some(arg) = arg {
                command.arg(arg);
            }
            match command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
            {
                Ok(child) => {
                    spawned = Some(child);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let mut child = match spawned {
            Some(child) => child,
            None => {
                let _ = last_error;
                open_diag!("spawn", "spawn_failed", started.elapsed(), None, None);
                return Err(PersistenceError::Unavailable);
            }
        };
        let Some(stdin) = child.stdin.take() else {
            if !reap_owned_child(ReapJob { child, permit }) {
                *keep_unresolved = true;
            }
            open_diag!("spawn", "stdin_missing", started.elapsed(), None, None);
            return Err(PersistenceError::Unavailable);
        };
        let Some(stdout) = child.stdout.take() else {
            if !reap_owned_child(ReapJob { child, permit }) {
                *keep_unresolved = true;
            }
            open_diag!("spawn", "stdout_missing", started.elapsed(), None, None);
            return Err(PersistenceError::Unavailable);
        };
        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        spawn_session(stdin, stdout, rx);
        let inner = Arc::new(Inner {
            tx,
            child: Mutex::new(Some(ReapJob { child, permit })),
            state: Mutex::new(DomainState {
                status: DomainStatus::Ready,
                revision: 0,
                prepared: None,
            }),
        });
        let domain = Arc::new(Self {
            inner: Arc::clone(&inner),
        });
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        if inner
            .tx
            .try_send(Command::Init {
                dir: directory.to_path_buf(),
                fault,
                reply: reply_tx,
            })
            .is_err()
        {
            if !domain.close_nonblocking() {
                *keep_unresolved = true;
            }
            open_diag!(
                "init_send",
                "send_failed",
                started.elapsed(),
                Some(domain.status()),
                None
            );
            return Err(PersistenceError::Unavailable);
        }
        let response = match reply_rx.recv_timeout(OPEN_TIMEOUT) {
            Ok(response) => response,
            Err(_error) => {
                #[cfg(test)]
                let outcome = match _error {
                    mpsc::RecvTimeoutError::Timeout => "timeout",
                    mpsc::RecvTimeoutError::Disconnected => "disconnected",
                };
                // A timeout or a helper that exited without a reply leaves the
                // attempt unresolved even if the child happens to be reaped.
                // One directory gets one attempt for the lifetime of this
                // process. This is ambiguity, not a definite failure.
                *keep_unresolved = true;
                domain.quarantine();
                open_diag!(
                    "receive",
                    outcome,
                    started.elapsed(),
                    Some(domain.status()),
                    None
                );
                return Err(PersistenceError::Unresolved);
            }
        };
        match response.status.as_str() {
            "ready" => {
                let bytes = match decode_payload(response.state) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        if !domain.close_nonblocking() {
                            *keep_unresolved = true;
                        }
                        open_diag!(
                            "decode",
                            "error",
                            started.elapsed(),
                            Some(domain.status()),
                            Some(response.status.as_str())
                        );
                        return Err(error);
                    }
                };
                let state = {
                    let mut state = domain.lock_state();
                    if response.revision != state.revision {
                        state.revision = response.revision;
                    }
                    OpenState {
                        namespace: response.namespace,
                        bytes,
                        revision: response.revision,
                    }
                };
                Ok((domain, state))
            }
            "busy" => {
                if !domain.close_nonblocking() {
                    *keep_unresolved = true;
                }
                open_diag!(
                    "wire",
                    wire_status_label(response.status.as_str()),
                    started.elapsed(),
                    Some(domain.status()),
                    Some(response.status.as_str())
                );
                Err(PersistenceError::Busy)
            }
            "malformed" => {
                if !domain.close_nonblocking() {
                    *keep_unresolved = true;
                }
                open_diag!(
                    "wire",
                    wire_status_label(response.status.as_str()),
                    started.elapsed(),
                    Some(domain.status()),
                    Some(response.status.as_str())
                );
                Err(PersistenceError::Malformed)
            }
            "unresolved" | "unconfirmed" | "external_change" | "no_reply" => {
                if !domain.close_nonblocking() {
                    *keep_unresolved = true;
                }
                open_diag!(
                    "wire",
                    wire_status_label(response.status.as_str()),
                    started.elapsed(),
                    Some(domain.status()),
                    Some(response.status.as_str())
                );
                Err(PersistenceError::Unresolved)
            }
            // The helper only reports a generic error for definitive
            // pre-publication/setup failures. An unrecognized status after a
            // possible publication is treated as ambiguity.
            "error" => {
                if !domain.close_nonblocking() {
                    *keep_unresolved = true;
                }
                open_diag!(
                    "wire",
                    wire_status_label(response.status.as_str()),
                    started.elapsed(),
                    Some(domain.status()),
                    Some(response.status.as_str())
                );
                Err(PersistenceError::Unavailable)
            }
            _ => {
                if !domain.close_nonblocking() {
                    *keep_unresolved = true;
                }
                open_diag!(
                    "wire",
                    wire_status_label(response.status.as_str()),
                    started.elapsed(),
                    Some(domain.status()),
                    Some(response.status.as_str())
                );
                Err(PersistenceError::Unresolved)
            }
        }
    }

    pub(crate) fn status(&self) -> DomainStatus {
        self.lock_state().status
    }

    pub(crate) fn is_confirmed(&self) -> bool {
        self.status() == DomainStatus::Ready
    }

    /// Durably publish state without yet recording lifecycle authorization.
    /// Timeout or ambiguity quarantines the domain immediately; a late
    /// response can never be mistaken for the current operation.
    pub(crate) fn prepare(
        &self,
        base: u64,
        bytes: Vec<u8>,
    ) -> Result<PrepareOutcome, PersistenceError> {
        #[cfg(test)]
        let started = Instant::now();
        {
            let state = self.lock_state();
            if state.status != DomainStatus::Ready {
                #[cfg(test)]
                let status_before = state.status;
                drop(state);
                prepare_diag!(
                    self,
                    "precheck",
                    "typed:Unavailable",
                    started.elapsed(),
                    base,
                    status_before,
                    None
                );
                return Err(PersistenceError::Unavailable);
            }
            if state.prepared.is_some() {
                #[cfg(test)]
                let status_before = state.status;
                drop(state);
                prepare_diag!(
                    self,
                    "precheck",
                    "typed:Busy",
                    started.elapsed(),
                    base,
                    status_before,
                    None
                );
                return Err(PersistenceError::Busy);
            }
            if state.revision != base {
                #[cfg(test)]
                let status_before = state.status;
                drop(state);
                prepare_diag!(
                    self,
                    "precheck",
                    "conflict",
                    started.elapsed(),
                    base,
                    status_before,
                    None
                );
                return Ok(PrepareOutcome::Conflict);
            }
        }
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        match self.inner.tx.try_send(Command::Commit {
            base,
            data: bytes,
            reply: reply_tx,
        }) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                #[cfg(test)]
                let status_before = self.status();
                prepare_diag!(
                    self,
                    "send",
                    "queue_full",
                    started.elapsed(),
                    base,
                    status_before,
                    None
                );
                return Err(PersistenceError::Busy);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                #[cfg(test)]
                let status_before = self.status();
                #[cfg(test)]
                let elapsed = started.elapsed();
                self.quarantine();
                prepare_diag!(
                    self,
                    "send",
                    "disconnected",
                    elapsed,
                    base,
                    status_before,
                    None
                );
                return Err(PersistenceError::Unavailable);
            }
        }
        let response = match reply_rx.recv_timeout(COMMIT_TIMEOUT) {
            Ok(response) => response,
            Err(_error) => {
                #[cfg(test)]
                let outcome = match _error {
                    mpsc::RecvTimeoutError::Timeout => "timeout",
                    mpsc::RecvTimeoutError::Disconnected => "disconnected",
                };
                #[cfg(test)]
                let status_before = self.status();
                #[cfg(test)]
                let elapsed = started.elapsed();
                self.quarantine();
                prepare_diag!(self, "recv", outcome, elapsed, base, status_before, None);
                return Ok(PrepareOutcome::Unconfirmed);
            }
        };
        match response.status.as_str() {
            "prepared" => {
                let mut state = self.lock_state();
                if state.status != DomainStatus::Ready {
                    #[cfg(test)]
                    let status_before = state.status;
                    drop(state);
                    self.quarantine();
                    prepare_diag!(
                        self,
                        "post_prepare",
                        "status_changed",
                        started.elapsed(),
                        base,
                        status_before,
                        Some(response.revision)
                    );
                    return Ok(PrepareOutcome::Unconfirmed);
                }
                if state.revision != base {
                    #[cfg(test)]
                    let status_before = state.status;
                    drop(state);
                    self.quarantine();
                    prepare_diag!(
                        self,
                        "post_prepare",
                        "revision_changed",
                        started.elapsed(),
                        base,
                        status_before,
                        Some(response.revision)
                    );
                    return Ok(PrepareOutcome::Conflict);
                }
                state.prepared = Some(response.revision);
                Ok(PrepareOutcome::Prepared(PreparedCommit {
                    inner: Arc::clone(&self.inner),
                    revision: response.revision,
                    resolved: false,
                }))
            }
            "before_rename" => {
                #[cfg(test)]
                let status_before = self.status();
                prepare_diag!(
                    self,
                    "wire",
                    "before_rename",
                    started.elapsed(),
                    base,
                    status_before,
                    Some(response.revision)
                );
                Ok(PrepareOutcome::BeforeRename)
            }
            "conflict" => {
                #[cfg(test)]
                let status_before = self.status();
                #[cfg(test)]
                let elapsed = started.elapsed();
                self.quarantine();
                prepare_diag!(
                    self,
                    "wire",
                    "conflict",
                    elapsed,
                    base,
                    status_before,
                    Some(response.revision)
                );
                Ok(PrepareOutcome::Conflict)
            }
            "external_change" => {
                #[cfg(test)]
                let status_before = self.status();
                #[cfg(test)]
                let elapsed = started.elapsed();
                self.quarantine();
                prepare_diag!(
                    self,
                    "wire",
                    "external_change",
                    elapsed,
                    base,
                    status_before,
                    Some(response.revision)
                );
                Ok(PrepareOutcome::ExternalChange)
            }
            "unconfirmed" => {
                #[cfg(test)]
                let status_before = self.status();
                #[cfg(test)]
                let elapsed = started.elapsed();
                self.quarantine();
                prepare_diag!(
                    self,
                    "wire",
                    "unconfirmed",
                    elapsed,
                    base,
                    status_before,
                    Some(response.revision)
                );
                Ok(PrepareOutcome::Unconfirmed)
            }
            "invalid" => {
                #[cfg(test)]
                let status_before = self.status();
                #[cfg(test)]
                let elapsed = started.elapsed();
                self.quarantine();
                prepare_diag!(
                    self,
                    "wire",
                    "invalid",
                    elapsed,
                    base,
                    status_before,
                    Some(response.revision)
                );
                Err(PersistenceError::Malformed)
            }
            _ => {
                #[cfg(test)]
                let status_before = self.status();
                #[cfg(test)]
                let elapsed = started.elapsed();
                self.quarantine();
                prepare_diag!(
                    self,
                    "wire",
                    wire_status_label(response.status.as_str()),
                    elapsed,
                    base,
                    status_before,
                    Some(response.revision)
                );
                Ok(PrepareOutcome::Unconfirmed)
            }
        }
    }

    /// Prepare and immediately acknowledge a commit that has no external
    /// lifecycle authorization step. Ticketed callers must use
    /// [`Self::prepare`] and confirm only while the ticket is still current.
    #[allow(dead_code)]
    pub(crate) fn commit(
        &self,
        base: u64,
        bytes: Vec<u8>,
    ) -> Result<CommitOutcome, PersistenceError> {
        match self.prepare(base, bytes)? {
            PrepareOutcome::Prepared(prepared) => prepared.confirm(),
            PrepareOutcome::BeforeRename => Ok(CommitOutcome::BeforeRename),
            PrepareOutcome::Unconfirmed => Ok(CommitOutcome::Unconfirmed),
            PrepareOutcome::Conflict => Ok(CommitOutcome::Conflict),
            PrepareOutcome::ExternalChange => Ok(CommitOutcome::ExternalChange),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_fault(&self, fault: Option<FaultConfig>) -> Result<(), PersistenceError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        match self.inner.tx.try_send(Command::SetFault {
            fault,
            reply: reply_tx,
        }) {
            Ok(()) => {}
            Err(_) => return Err(PersistenceError::Busy),
        }
        match reply_rx.recv_timeout(COMMIT_TIMEOUT) {
            Ok(response) if response.status == "fault_set" => Ok(()),
            _ => {
                self.quarantine();
                Err(PersistenceError::Unavailable)
            }
        }
    }

    /// Ask the owner to stop, bounded. If it does not acknowledge, kill it and
    /// keep the domain closed. The OS lock remains held until the child dies.
    pub(crate) fn shutdown(&self) {
        {
            let mut state = self.lock_state();
            if state.status == DomainStatus::Closed {
                return;
            }
            state.status = DomainStatus::Closed;
        }
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let sent = self
            .inner
            .tx
            .try_send(Command::Shutdown { reply: reply_tx })
            .is_ok();
        if sent {
            let _ = reply_rx.recv_timeout(SHUTDOWN_TIMEOUT);
        }
        self.kill_and_reap();
    }

    fn quarantine(&self) -> bool {
        {
            let mut state = self.lock_state();
            if state.status == DomainStatus::Ready {
                state.status = DomainStatus::Quarantined;
            }
            state.prepared = None;
        }
        self.kill_and_reap()
    }

    /// A commit was physically published but its lifecycle authorization was
    /// no longer current. Keep the owner from ever serving as the writer for
    /// this process again; readers must report an unconfirmed state.
    pub(crate) fn abandon_unconfirmed(&self) {
        self.quarantine();
    }

    fn close_nonblocking(&self) -> bool {
        {
            let mut state = self.lock_state();
            state.status = DomainStatus::Closed;
            state.prepared = None;
        }
        self.kill_and_reap()
    }

    fn kill_and_reap(&self) -> bool {
        let child = {
            let mut guard = match self.inner.child.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.take()
        };
        match child {
            Some(child) => reap_owned_child(child),
            None => true,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, DomainState> {
        match self.inner.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Drop for PersistenceDomain {
    fn drop(&mut self) {
        if self.status() != DomainStatus::Closed {
            self.shutdown();
        }
    }
}

impl PreparedCommit {
    /// Record that the current lifecycle authorized this already-durable
    /// publication. The marker becomes `acked` before it is unlinked so a
    /// crash or a lost reply cannot turn an unauthorized publication into a
    /// confirmed one.
    pub(crate) fn confirm(mut self) -> Result<CommitOutcome, PersistenceError> {
        self.resolved = true;
        #[cfg(test)]
        let started = Instant::now();
        {
            let state = lock_inner_state(&self.inner);
            if state.status != DomainStatus::Ready || state.prepared != Some(self.revision) {
                #[cfg(test)]
                let status_before = state.status;
                drop(state);
                quarantine_inner(&self.inner);
                ack_diag!(
                    &self.inner,
                    "precheck",
                    "status_or_ticket",
                    started.elapsed(),
                    self.revision,
                    status_before,
                    None
                );
                return Ok(CommitOutcome::Unconfirmed);
            }
        }
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        match self.inner.tx.try_send(Command::Ack {
            revision: self.revision,
            reply: reply_tx,
        }) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                #[cfg(test)]
                let status_before = lock_inner_state(&self.inner).status;
                #[cfg(test)]
                let elapsed = started.elapsed();
                quarantine_inner(&self.inner);
                ack_diag!(
                    &self.inner,
                    "send",
                    "queue_full",
                    elapsed,
                    self.revision,
                    status_before,
                    None
                );
                return Ok(CommitOutcome::Unconfirmed);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                #[cfg(test)]
                let status_before = lock_inner_state(&self.inner).status;
                #[cfg(test)]
                let elapsed = started.elapsed();
                quarantine_inner(&self.inner);
                ack_diag!(
                    &self.inner,
                    "send",
                    "disconnected",
                    elapsed,
                    self.revision,
                    status_before,
                    None
                );
                return Ok(CommitOutcome::Unconfirmed);
            }
        }
        let response = match reply_rx.recv_timeout(COMMIT_TIMEOUT) {
            Ok(response) => response,
            Err(_error) => {
                #[cfg(test)]
                let outcome = match _error {
                    mpsc::RecvTimeoutError::Timeout => "timeout",
                    mpsc::RecvTimeoutError::Disconnected => "disconnected",
                };
                #[cfg(test)]
                let status_before = lock_inner_state(&self.inner).status;
                #[cfg(test)]
                let elapsed = started.elapsed();
                quarantine_inner(&self.inner);
                ack_diag!(
                    &self.inner,
                    "recv",
                    outcome,
                    elapsed,
                    self.revision,
                    status_before,
                    None
                );
                return Ok(CommitOutcome::Unconfirmed);
            }
        };
        match response.status.as_str() {
            "committed" => {
                let mut state = lock_inner_state(&self.inner);
                if state.status != DomainStatus::Ready {
                    #[cfg(test)]
                    let status_before = state.status;
                    drop(state);
                    quarantine_inner(&self.inner);
                    ack_diag!(
                        &self.inner,
                        "post_ack",
                        "status_changed",
                        started.elapsed(),
                        self.revision,
                        status_before,
                        Some(response.revision)
                    );
                    return Ok(CommitOutcome::Unconfirmed);
                }
                state.revision = response.revision;
                state.prepared = None;
                Ok(CommitOutcome::Committed(response.revision))
            }
            "unconfirmed" => {
                #[cfg(test)]
                let status_before = lock_inner_state(&self.inner).status;
                #[cfg(test)]
                let elapsed = started.elapsed();
                quarantine_inner(&self.inner);
                ack_diag!(
                    &self.inner,
                    "wire",
                    "unconfirmed",
                    elapsed,
                    self.revision,
                    status_before,
                    Some(response.revision)
                );
                Ok(CommitOutcome::Unconfirmed)
            }
            "invalid" => {
                #[cfg(test)]
                let status_before = lock_inner_state(&self.inner).status;
                #[cfg(test)]
                let elapsed = started.elapsed();
                quarantine_inner(&self.inner);
                ack_diag!(
                    &self.inner,
                    "wire",
                    "invalid",
                    elapsed,
                    self.revision,
                    status_before,
                    Some(response.revision)
                );
                Err(PersistenceError::Malformed)
            }
            _ => {
                #[cfg(test)]
                let status_before = lock_inner_state(&self.inner).status;
                #[cfg(test)]
                let elapsed = started.elapsed();
                quarantine_inner(&self.inner);
                ack_diag!(
                    &self.inner,
                    "wire",
                    wire_status_label(response.status.as_str()),
                    elapsed,
                    self.revision,
                    status_before,
                    Some(response.revision)
                );
                Ok(CommitOutcome::Unconfirmed)
            }
        }
    }

    /// Abandon an authorized-but-unconfirmed publication. The durable marker
    /// is deliberately left in its unacknowledged phase; a successor fails
    /// closed instead of accepting obsolete or unauthorized state.
    pub(crate) fn abort(mut self) {
        self.resolved = true;
        quarantine_inner(&self.inner);
    }
}

impl Drop for PreparedCommit {
    fn drop(&mut self) {
        if !self.resolved {
            quarantine_inner(&self.inner);
        }
    }
}

fn lock_inner_state(inner: &Arc<Inner>) -> std::sync::MutexGuard<'_, DomainState> {
    match inner.state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn quarantine_inner(inner: &Arc<Inner>) -> bool {
    {
        let mut state = lock_inner_state(inner);
        if state.status == DomainStatus::Ready {
            state.status = DomainStatus::Quarantined;
        }
        state.prepared = None;
    }
    kill_inner(inner)
}

fn kill_inner(inner: &Arc<Inner>) -> bool {
    let child = {
        let mut guard = match inner.child.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.take()
    };
    match child {
        Some(child) => reap_owned_child(child),
        None => true,
    }
}

fn attempt_key(directory: &Path) -> PathBuf {
    // Deliberately no filesystem call here: the parent must not block in
    // `canonicalize` before the helper timeout. The supplied absolute app-data
    // path is the identity. Alias spellings are serialized only by the
    // directory advisory lock after the helper opens it, never by this key.
    directory.to_path_buf()
}

fn unresolved_directories() -> &'static Mutex<HashSet<PathBuf>> {
    static UNRESOLVED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    UNRESOLVED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn begin_attempt(key: &Path) -> bool {
    let mut unresolved = match unresolved_directories().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if unresolved.contains(key) || unresolved.len() >= MAX_UNRESOLVED_DIRECTORIES {
        return false;
    }
    unresolved.insert(key.to_path_buf());
    true
}

fn end_attempt(key: &Path) {
    let mut unresolved = match unresolved_directories().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    unresolved.remove(key);
}

/// Queue, run/start flags, and generation live under the same mutex. Once a
/// worker has been started it is persistent: it only waits for work, so a
/// later thread-spawn failure cannot strand a child that was already spawned.
/// At most one worker is active at a time.
///
/// Starting a worker is not assumed to succeed. A bounded set of spawn
/// attempts is made. If a concurrent handoff arrived while those attempts were
/// failing, one bounded retry round is performed so that interleaving does not
/// leave the queue ownerless. If every attempt still fails (for example under
/// persistent thread exhaustion) the handles and their capacity permits remain
/// queued, the start flag is cleared, and a later handoff can retry. Handles
/// are never dropped: ownership stays explicit instead of being silently lost.
/// Callers do not block on spawning or on a child wait.
struct Reaper {
    inner: Mutex<ReaperInner>,
    ready: Condvar,
}

struct ReaperInner {
    queue: VecDeque<ReapJob>,
    running: bool,
    starting: bool,
    poisoned: bool,
    generation: u64,
}

impl Reaper {
    fn new() -> Self {
        Self {
            inner: Mutex::new(ReaperInner {
                queue: VecDeque::new(),
                running: false,
                starting: false,
                poisoned: false,
                generation: 0,
            }),
            ready: Condvar::new(),
        }
    }

    fn enqueue_with<F>(self: &Arc<Self>, job: ReapJob, mut spawn: F)
    where
        F: FnMut() -> std::io::Result<()>,
    {
        let start = {
            let mut inner = match self.inner.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            inner.queue.push_back(job);
            inner.generation = inner.generation.wrapping_add(1);
            if inner.poisoned || inner.running || inner.starting {
                false
            } else {
                inner.starting = true;
                true
            }
        };
        // Wake an already-running persistent worker. A newly started worker
        // will observe the queued job on its first pass.
        self.ready.notify_all();
        if start {
            self.start_worker(&mut spawn);
        }
    }

    fn ensure_started_with<F>(self: &Arc<Self>, mut spawn: F) -> bool
    where
        F: FnMut() -> std::io::Result<()>,
    {
        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        loop {
            if inner.poisoned {
                return false;
            }
            if inner.running {
                return true;
            }
            if !inner.starting {
                inner.starting = true;
                drop(inner);
                return self.start_worker(&mut spawn);
            }
            // Another caller owns the start attempt. Wait for its outcome; if
            // it fails, loop and try with this caller's spawn closure.
            inner = match self.ready.wait(inner) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    fn ensure_started(self: &Arc<Self>) -> bool {
        self.ensure_started_with(|| {
            let reaper = Arc::clone(self);
            std::thread::Builder::new()
                .name("hash-persist-reaper".into())
                .spawn(move || reaper_loop(reaper))
                .map(|_| ())
        })
    }

    fn start_worker<F>(self: &Arc<Self>, spawn: &mut F) -> bool
    where
        F: FnMut() -> std::io::Result<()>,
    {
        let mut rounds = 0;
        let mut observed_generation = {
            let inner = match self.inner.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            inner.generation
        };
        loop {
            let mut delay = REAPER_SPAWN_BACKOFF;
            for attempt in 0..REAPER_SPAWN_ATTEMPTS {
                let poisoned = {
                    let inner = match self.inner.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    inner.poisoned
                };
                if poisoned {
                    let mut inner = match self.inner.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    inner.starting = false;
                    drop(inner);
                    self.ready.notify_all();
                    return false;
                }
                if spawn().is_ok() {
                    let mut inner = match self.inner.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    // A worker may have panicked while this one was being
                    // created. Let the spawned thread observe the poison and
                    // exit instead of marking a dead/poisoned reaper running.
                    if inner.poisoned {
                        inner.starting = false;
                        drop(inner);
                        self.ready.notify_all();
                        return false;
                    }
                    inner.starting = false;
                    inner.running = true;
                    drop(inner);
                    self.ready.notify_all();
                    return true;
                }
                if attempt + 1 < REAPER_SPAWN_ATTEMPTS {
                    std::thread::sleep(delay);
                    delay = delay.saturating_mul(2);
                }
            }
            let mut inner = match self.inner.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if inner.poisoned {
                inner.starting = false;
                drop(inner);
                self.ready.notify_all();
                return false;
            }
            // If a handoff arrived while every attempt was failing, retry once
            // more before giving up. The check and the failure transition are
            // under one lock, so an enqueue cannot fall into a gap where it
            // sees `starting` and no later worker retries it.
            if rounds < 1 && !inner.queue.is_empty() && inner.generation != observed_generation {
                observed_generation = inner.generation;
                rounds += 1;
                drop(inner);
                continue;
            }
            // No worker was created. Keep every queued handle and permit so a
            // later handoff can retry. Dropping handles here would falsely
            // report them as reaped; waiting here would delay shutdown.
            inner.starting = false;
            drop(inner);
            self.ready.notify_all();
            return false;
        }
    }

    fn enqueue(self: &Arc<Self>, job: ReapJob) {
        self.enqueue_with(job, || {
            let reaper = Arc::clone(self);
            std::thread::Builder::new()
                .name("hash-persist-reaper".into())
                .spawn(move || reaper_loop(reaper))
                .map(|_| ())
        });
    }

    #[cfg(test)]
    fn take_next(&self) -> Option<ReapJob> {
        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        inner.queue.pop_front()
    }

    fn next_blocking(&self) -> Option<ReapJob> {
        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        loop {
            if inner.poisoned {
                return None;
            }
            if let Some(job) = inner.queue.pop_front() {
                return Some(job);
            }
            inner = match self.ready.wait(inner) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    /// A wait panicked while it still owned `job`. Fail closed: retain the
    /// exact child handle and its capacity permit in the queue, stop every
    /// future worker, and let the already-running worker exit. New helpers are
    /// rejected by `ensure_started`/`enqueue_with`; capacity stays reserved
    /// rather than being released for a child that was never confirmed reaped.
    fn poison_with(self: &Arc<Self>, job: ReapJob) {
        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        inner.poisoned = true;
        inner.running = false;
        inner.starting = false;
        inner.queue.push_front(job);
        drop(inner);
        self.ready.notify_all();
    }
}

fn reaper() -> &'static Arc<Reaper> {
    static REAPER: OnceLock<Arc<Reaper>> = OnceLock::new();
    REAPER.get_or_init(|| Arc::new(Reaper::new()))
}

fn handoff_reap(job: ReapJob) {
    reaper().enqueue(job);
}

fn reaper_loop(reaper: Arc<Reaper>) {
    reaper_loop_with(reaper, wait_confirmed);
}

fn reaper_loop_with<W>(reaper: Arc<Reaper>, mut wait: W)
where
    W: FnMut(&mut Child),
{
    loop {
        let Some(mut job) = reaper.next_blocking() else {
            return;
        };
        // `job` is owned outside the closure so a panic cannot drop the child
        // handle or release its capacity permit. On panic the job is retained
        // and the reaper poisons itself instead of silently losing the child
        // or starting another unbounded worker.
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wait(&mut job.child)));
        if outcome.is_err() {
            reaper.poison_with(job);
            return;
        }
        // The job (and its capacity permit) is released only after the wait
        // above confirms exit.
    }
}

/// Retry a blocking wait on the owned handle until the OS confirms exit.
/// `Interrupted` is transparent and retried; any other error also leaves the
/// handle owned by this worker. The capped exponential backoff prevents a
/// persistent wait error from becoming a busy loop, and the handle is never
/// dropped as if it had been reaped.
fn wait_confirmed(child: &mut Child) {
    let mut delay = REAPER_WAIT_BACKOFF_MIN;
    loop {
        match child.wait() {
            Ok(_) => return,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::Interrupted {
                    // A spurious interruption should be retried promptly, but
                    // keep a small backoff for repeated interruptions.
                    std::thread::yield_now();
                }
                std::thread::sleep(delay);
                delay = delay.saturating_mul(2).min(REAPER_WAIT_BACKOFF_MAX);
            }
        }
    }
}

#[cfg(test)]
fn drain_reaper<W>(reaper: &Reaper, mut wait: W)
where
    W: FnMut(&mut Child) -> std::io::Result<std::process::ExitStatus>,
{
    while let Some(mut job) = reaper.take_next() {
        let mut delay = REAPER_WAIT_BACKOFF_MIN;
        loop {
            match wait(&mut job.child) {
                Ok(_) => break,
                Err(error) => {
                    if error.kind() == std::io::ErrorKind::Interrupted {
                        // A spurious interruption should be retried promptly,
                        // but keep a small backoff for repeated interruptions.
                        std::thread::yield_now();
                    }
                    std::thread::sleep(delay);
                    delay = delay.saturating_mul(2).min(REAPER_WAIT_BACKOFF_MAX);
                }
            }
        }
        // Dropping the job here releases its capacity permit only after the
        // injected wait confirmed the child.
    }
}

fn reap_owned_child(mut job: ReapJob) -> bool {
    let _ = job.child.kill();
    let deadline = Instant::now() + KILL_WAIT;
    loop {
        match job.child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => {
                handoff_reap(job);
                return false;
            }
        }
        if Instant::now() >= deadline {
            // The child may be in an uninterruptible filesystem wait. A single
            // shared reaper owns all such children so retries cannot grow an
            // unbounded number of helper or reaper threads.
            handoff_reap(job);
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Locate the dedicated helper binary, falling back to re-executing the main
/// binary in helper mode when packaging did not include the separate helper.
/// Detection is by attempting the spawn, not by a blocking metadata probe in
/// the parent. That fallback is not packaging-validated; a package that omits
/// both fails closed rather than silently persisting through a different path.
fn helper_candidates() -> Result<Vec<(PathBuf, Option<std::ffi::OsString>)>, PersistenceError> {
    #[cfg(debug_assertions)]
    if let Some(path) = std::env::var_os("SURGUI_HASH_HELPER_EXE") {
        return Ok(vec![(PathBuf::from(path), None)]);
    }
    let current = std::env::current_exe().map_err(|_| PersistenceError::Unavailable)?;
    let mut candidates = Vec::with_capacity(2);
    if let Some(parent) = current.parent() {
        let target = if parent.file_name().is_some_and(|name| name == "deps") {
            parent.parent().unwrap_or(parent)
        } else {
            parent
        };
        candidates.push((
            target.join(format!("hash-state-helper{}", std::env::consts::EXE_SUFFIX)),
            None,
        ));
    }
    candidates.push((current, Some(HELPER_ARG.into())));
    Ok(candidates)
}

fn spawn_session(stdin: ChildStdin, stdout: ChildStdout, rx: Receiver<Command>) {
    let _ = std::thread::Builder::new()
        .name("hash-persist-session".into())
        .spawn(move || {
            let mut stdin = stdin;
            let mut stdout = BufReader::new(stdout);
            while let Ok(command) = rx.recv() {
                let (request, reply) = match command {
                    Command::Init { dir, fault, reply } => (
                        WireRequest {
                            kind: "init".into(),
                            dir: Some(dir.to_string_lossy().into_owned()),
                            base: 0,
                            data: None,
                            fault,
                        },
                        reply,
                    ),
                    Command::Commit { base, data, reply } => (
                        WireRequest {
                            kind: "commit".into(),
                            dir: None,
                            base,
                            data: Some(encode_payload(&data)),
                            fault: None,
                        },
                        reply,
                    ),
                    Command::Ack { revision, reply } => (
                        WireRequest {
                            kind: "ack".into(),
                            dir: None,
                            base: revision,
                            data: None,
                            fault: None,
                        },
                        reply,
                    ),
                    #[cfg(test)]
                    Command::SetFault { fault, reply } => (
                        WireRequest {
                            kind: "fault".into(),
                            dir: None,
                            base: 0,
                            data: None,
                            fault,
                        },
                        reply,
                    ),
                    Command::Shutdown { reply } => (
                        WireRequest {
                            kind: "shutdown".into(),
                            dir: None,
                            base: 0,
                            data: None,
                            fault: None,
                        },
                        reply,
                    ),
                };
                let shutdown = request.kind == "shutdown";
                // A helper that exits or emits no parseable reply may have
                // acted before dying; callers must treat this as ambiguous
                // rather than as a definitive helper error.
                let response = round_trip(&mut stdin, &mut stdout, &request)
                    .unwrap_or_else(|| WireResponse::status("no_reply"));
                let terminal = matches!(
                    response.status.as_str(),
                    "error" | "busy" | "invalid" | "conflict" | "external_change" | "unconfirmed"
                );
                let _ = reply.send(response);
                if shutdown || terminal {
                    break;
                }
            }
        });
}

fn round_trip(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
    request: &WireRequest,
) -> Option<WireResponse> {
    let mut line = serde_json::to_vec(request).ok()?;
    line.push(b'\n');
    stdin.write_all(&line).ok()?;
    stdin.flush().ok()?;
    let mut response = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match stdout.read(&mut byte) {
            Ok(0) | Err(_) => return None,
            Ok(_) => {
                if byte[0] == b'\n' {
                    break;
                }
                response.push(byte[0]);
                if response.len() > MAX_PROTOCOL_LINE {
                    return None;
                }
            }
        }
    }
    serde_json::from_slice(&response).ok()
}

#[derive(Serialize, Deserialize)]
struct WireRequest {
    kind: String,
    #[serde(default)]
    dir: Option<String>,
    #[serde(default)]
    base: u64,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    fault: Option<FaultConfig>,
}

#[derive(Serialize, Deserialize)]
struct WireResponse {
    status: String,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    revision: u64,
}

impl WireResponse {
    fn status(status: &str) -> Self {
        Self {
            status: status.into(),
            namespace: None,
            state: None,
            revision: 0,
        }
    }

    fn error() -> Self {
        Self::status("error")
    }

    fn with_revision(status: &str, revision: u64) -> Self {
        Self {
            status: status.into(),
            namespace: None,
            state: None,
            revision,
        }
    }
}

fn encode_payload(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode_payload(value: Option<String>) -> Result<Option<Vec<u8>>, PersistenceError> {
    match value {
        None => Ok(None),
        Some(value) => base64::engine::general_purpose::STANDARD
            .decode(value)
            .map(Some)
            .map_err(|_| PersistenceError::Unavailable),
    }
}

/// Entry point used by the real app when it is re-executed in helper mode.
pub fn run_helper_from_env() -> Option<i32> {
    let requested = std::env::args_os().any(|arg| arg == HELPER_ARG);
    requested.then(helper_standalone)
}

/// Entry point used by the dedicated `hash-state-helper` binary.
pub fn helper_standalone() -> i32 {
    helper_loop().unwrap_or(1)
}

fn helper_loop() -> Option<i32> {
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let init = read_request(&mut reader)?;
    if init.kind != "init" {
        return Some(1);
    }
    let directory = PathBuf::from(init.dir?);
    let fault = init.fault;
    // Acquire the single OS-level initialization guard before the helper
    // performs any filesystem lookup. In particular, the guard must not be
    // keyed by a canonicalized path: canonicalization itself can hang, and two
    // alias spellings would then resolve the key concurrently before either
    // one took a guard. Holding this one guard across preparation and path
    // resolution makes a successor fail closed instead of accumulating behind
    // a stuck filesystem call.
    let guard = match acquire_initialization_guard() {
        Ok(guard) => guard,
        Err(HelperGuardError::Busy) => {
            respond(WireResponse::status("busy"));
            return Some(2);
        }
        Err(HelperGuardError::Unavailable) => {
            respond(WireResponse::error());
            return Some(1);
        }
    };
    // Deterministic test barrier after the guard is held but before the first
    // path lookup. Production callers never set it.
    if fault_point(&fault, "prelock").is_err() {
        respond(WireResponse::error());
        return Some(1);
    }
    if crate::managed_state::ManagedStateDir::prepare(&directory).is_err() {
        respond(WireResponse::error());
        return Some(1);
    }
    if fault_point(&fault, "load").is_err() {
        respond(WireResponse::error());
        return Some(1);
    }
    let lock = match acquire_directory_lock(&directory) {
        Ok(lock) => lock,
        Err(LockError::Busy) => {
            respond(WireResponse::status("busy"));
            return Some(2);
        }
        Err(LockError::Unavailable) => {
            respond(WireResponse::error());
            return Some(1);
        }
    };
    // The per-directory cooperating lock now owns serialization for this
    // directory, so independent directories no longer need to wait behind this
    // helper's initialization.
    drop(guard);
    match recover_pending_marker(&directory) {
        MarkerRecovery::Absent | MarkerRecovery::Acked => {}
        MarkerRecovery::Unresolved => {
            respond(WireResponse::status("unresolved"));
            return Some(1);
        }
    }
    if fault_point(&fault, "namespace").is_err() {
        respond(WireResponse::error());
        return Some(1);
    }
    let namespace = match hash_state::ensure_namespace(&directory) {
        hash_state::NamespaceLoad::Ready(namespace) => namespace,
        hash_state::NamespaceLoad::Malformed => {
            respond(WireResponse::status("malformed"));
            return Some(1);
        }
        hash_state::NamespaceLoad::Unavailable => {
            respond(WireResponse::error());
            return Some(1);
        }
    };
    cleanup_stale_temps(&directory);
    let state_path = directory.join(HASH_STATE_FILE);
    let original = match read_bounded(&state_path) {
        Ok(bytes) => bytes,
        Err(ReadStateError::Unavailable) => {
            respond(WireResponse::error());
            return Some(1);
        }
    };
    let canonical = match original.as_deref().map(hash_state::normalize_state) {
        None => None,
        Some(Ok(bytes)) => Some(bytes),
        Some(Err(_)) => {
            respond(WireResponse::status("malformed"));
            return Some(1);
        }
    };
    let mut session = HelperSession {
        directory: directory.clone(),
        namespace: namespace.clone(),
        state_path,
        last_state: original.clone(),
        revision: 0,
        prepared: None,
        fault,
    };
    if let (Some(original), Some(canonical)) = (original.as_deref(), canonical.as_deref()) {
        if original != canonical {
            // Persist the legacy migration before claiming the state is ready.
            // A definitive pre-rename failure stays a generic error, while an
            // ambiguous publication is reported as unconfirmed so the parent
            // can surface HashUnconfirmed instead of a generic State.
            let migrated = session
                .commit_prepare(0, canonical.to_vec())
                .and_then(|revision| session.commit_ack(revision));
            if let Err(failure) = migrated {
                match failure {
                    CommitFailure::BeforeRename => {
                        respond(WireResponse::error());
                    }
                    CommitFailure::ExternalChange => {
                        respond(WireResponse::status("external_change"));
                    }
                    CommitFailure::Unconfirmed(_) => {
                        respond(WireResponse::status("unconfirmed"));
                    }
                    CommitFailure::Crash => return Some(70),
                }
                return Some(1);
            }
        }
    }
    respond(WireResponse {
        status: "ready".into(),
        namespace: Some(namespace),
        state: session.last_state.as_deref().map(encode_payload),
        revision: session.revision,
    });
    while let Some(request) = read_request(&mut reader) {
        match request.kind.as_str() {
            "commit" => {
                let response = match request.base == session.revision {
                    false => WireResponse::with_revision("conflict", session.revision),
                    true => {
                        let Some(data) = request.data else {
                            respond(WireResponse::status("invalid"));
                            continue;
                        };
                        let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data)
                        else {
                            respond(WireResponse::status("invalid"));
                            continue;
                        };
                        if bytes.len() > MAX_STATE_BYTES {
                            respond(WireResponse::status("invalid"));
                            continue;
                        }
                        let Ok(normalized) = hash_state::normalize_state(&bytes) else {
                            respond(WireResponse::status("invalid"));
                            continue;
                        };
                        match session.commit_prepare(request.base, normalized) {
                            Ok(revision) => WireResponse::with_revision("prepared", revision),
                            Err(CommitFailure::BeforeRename) => {
                                WireResponse::with_revision("before_rename", session.revision)
                            }
                            Err(CommitFailure::ExternalChange) => {
                                WireResponse::with_revision("external_change", session.revision)
                            }
                            Err(CommitFailure::Unconfirmed(revision)) => {
                                WireResponse::with_revision("unconfirmed", revision)
                            }
                            Err(CommitFailure::Crash) => return Some(70),
                        }
                    }
                };
                respond(response);
            }
            "ack" => {
                let response = match session.commit_ack(request.base) {
                    Ok(revision) => WireResponse::with_revision("committed", revision),
                    Err(CommitFailure::BeforeRename) => {
                        WireResponse::with_revision("before_rename", session.revision)
                    }
                    Err(CommitFailure::ExternalChange) => {
                        WireResponse::with_revision("external_change", session.revision)
                    }
                    Err(CommitFailure::Unconfirmed(revision)) => {
                        WireResponse::with_revision("unconfirmed", revision)
                    }
                    Err(CommitFailure::Crash) => return Some(70),
                };
                respond(response);
            }
            "fault" => {
                session.fault = request.fault;
                respond(WireResponse::status("fault_set"));
            }
            "shutdown" => {
                let _ = fault_point(&session.fault, "shutdown");
                respond(WireResponse::status("bye"));
                break;
            }
            _ => respond(WireResponse::status("invalid")),
        }
    }
    drop(lock);
    Some(0)
}

fn respond(response: WireResponse) {
    let mut out = std::io::stdout().lock();
    if let Ok(mut line) = serde_json::to_vec(&response) {
        line.push(b'\n');
        let _ = out.write_all(&line);
        let _ = out.flush();
    }
}

fn read_request(reader: &mut impl BufRead) -> Option<WireRequest> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match reader.read(&mut byte) {
            Ok(0) | Err(_) => return None,
            Ok(_) => {
                if byte[0] == b'\n' {
                    break;
                }
                line.push(byte[0]);
                if line.len() > MAX_PROTOCOL_LINE {
                    return None;
                }
            }
        }
    }
    serde_json::from_slice(&line).ok()
}

#[derive(Debug)]
enum HelperGuardError {
    Busy,
    Unavailable,
}

#[cfg(target_os = "linux")]
struct HelperGuard {
    fd: libc::c_int,
}

#[cfg(target_os = "linux")]
impl Drop for HelperGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::close(self.fd) };
    }
}

#[cfg(windows)]
struct HelperGuard {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl Drop for HelperGuard {
    fn drop(&mut self) {
        let _ = unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
struct HelperGuard;

/// A single OS-level initialization guard acquired before the helper performs
/// any filesystem lookup. It is deliberately not keyed by the requested path:
/// resolving a path (even canonicalizing it) is itself a filesystem operation
/// that can block indefinitely, so a path-derived guard would let lexical and
/// symlink aliases accumulate behind a stuck resolution. This guard serializes
/// helper initialization globally, then is released as soon as the concrete
/// per-directory cooperating lock has been acquired.
///
/// Linux uses an abstract Unix socket. Abstract names live in the current
/// network namespace and are released by the kernel on process exit; the name
/// includes the effective uid, so different Unix users do not block each
/// other. A helper in a different network namespace or running as a different
/// user is therefore outside this guard's scope; the per-directory file lock
/// still serializes owners that share the directory.
///
/// Windows uses a named kernel mutex in the `Local\` namespace. That namespace
/// is scoped to the current logon session, so another session is not excluded
/// by this guard; the per-directory lock still applies to cooperating owners in
/// that session.
///
/// Other platforms have no evaluated guard and fail closed.
#[cfg(target_os = "linux")]
fn acquire_initialization_guard() -> Result<HelperGuard, HelperGuardError> {
    let name = format!("\0surgui-hash-init-{}", unsafe { libc::geteuid() });
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(HelperGuardError::Unavailable);
    }
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let bytes = name.as_bytes();
    if bytes.len() > address.sun_path.len() {
        let _ = unsafe { libc::close(fd) };
        return Err(HelperGuardError::Unavailable);
    }
    for (index, byte) in bytes.iter().enumerate() {
        address.sun_path[index] = *byte as libc::c_char;
    }
    let length = (std::mem::size_of::<libc::sa_family_t>() + bytes.len()) as libc::socklen_t;
    let result = unsafe {
        libc::bind(
            fd,
            &address as *const libc::sockaddr_un as *const libc::sockaddr,
            length,
        )
    };
    if result != 0 {
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default();
        let _ = unsafe { libc::close(fd) };
        return Err(if errno == libc::EADDRINUSE {
            HelperGuardError::Busy
        } else {
            HelperGuardError::Unavailable
        });
    }
    Ok(HelperGuard { fd })
}

#[cfg(windows)]
fn acquire_initialization_guard() -> Result<HelperGuard, HelperGuardError> {
    use windows_sys::Win32::Foundation::{GetLastError, SetLastError, ERROR_ALREADY_EXISTS};
    use windows_sys::Win32::System::Threading::CreateMutexW;
    // `Local\` is logon-session-scoped. The default mutex security descriptor
    // is sufficient for the same-user desktop process model this app supports;
    // a helper in a different session is outside this guard's scope.
    let name: Vec<u16> = "Local\\SURGUI-hash-initialization"
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    unsafe { SetLastError(0) };
    let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
    if handle.is_null() {
        return Err(HelperGuardError::Unavailable);
    }
    if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
        let _ = unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
        return Err(HelperGuardError::Busy);
    }
    Ok(HelperGuard { handle })
}

#[cfg(not(any(target_os = "linux", windows)))]
fn acquire_initialization_guard() -> Result<HelperGuard, HelperGuardError> {
    Err(HelperGuardError::Unavailable)
}

enum LockError {
    Busy,
    Unavailable,
}

struct DirectoryLock {
    #[cfg(unix)]
    file: std::fs::File,
    #[cfg(windows)]
    file: std::fs::File,
    #[cfg(not(any(unix, windows)))]
    _private: (),
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
        #[cfg(windows)]
        {
            let _ = unlock_windows(&self.file);
        }
    }
}

fn acquire_directory_lock(directory: &Path) -> Result<DirectoryLock, LockError> {
    let path = directory.join(HASH_LOCK_FILE);
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|_| LockError::Unavailable)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let errno = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or_default();
            return Err(if errno == libc::EWOULDBLOCK || errno == libc::EAGAIN {
                LockError::Busy
            } else {
                LockError::Unavailable
            });
        }
        Ok(DirectoryLock { file })
    }
    #[cfg(windows)]
    {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|_| LockError::Unavailable)?;
        lock_windows(&file).map_err(|busy| {
            if busy {
                LockError::Busy
            } else {
                LockError::Unavailable
            }
        })?;
        Ok(DirectoryLock { file })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (directory, path);
        Err(LockError::Unavailable)
    }
}

#[cfg(windows)]
fn lock_windows(file: &std::fs::File) -> Result<(), bool> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;
    let mut overlapped = unsafe { std::mem::zeroed::<OVERLAPPED>() };
    let result = unsafe {
        LockFileEx(
            file.as_raw_handle() as HANDLE,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    if result == 0 {
        let error = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default() as u32;
        return Err(error == ERROR_LOCK_VIOLATION);
    }
    Ok(())
}

#[cfg(windows)]
fn unlock_windows(file: &std::fs::File) -> Result<(), ()> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::UnlockFileEx;
    use windows_sys::Win32::System::IO::OVERLAPPED;
    let mut overlapped = unsafe { std::mem::zeroed::<OVERLAPPED>() };
    let result = unsafe {
        UnlockFileEx(
            file.as_raw_handle() as HANDLE,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    if result == 0 {
        Err(())
    } else {
        Ok(())
    }
}

struct HelperSession {
    directory: PathBuf,
    namespace: String,
    state_path: PathBuf,
    last_state: Option<Vec<u8>>,
    revision: u64,
    prepared: Option<PendingCommit>,
    fault: Option<FaultConfig>,
}

struct PendingCommit {
    revision: u64,
    hash: String,
    desired: Vec<u8>,
}

enum CommitFailure {
    BeforeRename,
    ExternalChange,
    Unconfirmed(u64),
    Crash,
}

enum ReadStateError {
    Unavailable,
}

fn read_bounded(path: &Path) -> Result<Option<Vec<u8>>, ReadStateError> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(_) => return Err(ReadStateError::Unavailable),
    };
    let mut bytes = Vec::new();
    if file
        .take((MAX_STATE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Err(ReadStateError::Unavailable);
    }
    if bytes.len() > MAX_STATE_BYTES {
        return Err(ReadStateError::Unavailable);
    }
    Ok(Some(bytes))
}

impl HelperSession {
    /// Durably write and rename the new state, then record the durable marker
    /// phase. The session revision is not advanced until [`Self::commit_ack`].
    fn commit_prepare(&mut self, base: u64, desired: Vec<u8>) -> Result<u64, CommitFailure> {
        if self.prepared.is_some() {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if base != self.revision {
            return Err(CommitFailure::ExternalChange);
        }
        if self.external_change() {
            return Err(CommitFailure::ExternalChange);
        }
        fault_point(&self.fault, "write").map_err(|_| CommitFailure::BeforeRename)?;
        let temp = match self.write_temp(&desired) {
            Ok(temp) => temp,
            Err(_) => return Err(CommitFailure::BeforeRename),
        };
        if self.external_change() {
            let _ = std::fs::remove_file(&temp);
            return Err(CommitFailure::ExternalChange);
        }
        fault_point(&self.fault, "rename").map_err(|_| {
            let _ = std::fs::remove_file(&temp);
            CommitFailure::BeforeRename
        })?;
        let next_revision = self.revision.wrapping_add(1);
        let hash = state_hash(&desired);
        let reuse_acked = self.marker_is_acked_for_last_state();
        match write_pending_marker(
            &self.directory,
            next_revision,
            &hash,
            reuse_acked,
            &self.fault,
        ) {
            MarkerWrite::Written => {}
            MarkerWrite::AlreadyExists => {
                let _ = std::fs::remove_file(&temp);
                return Err(CommitFailure::Unconfirmed(self.revision));
            }
            MarkerWrite::FailedRolledBack => {
                let _ = std::fs::remove_file(&temp);
                return Err(CommitFailure::BeforeRename);
            }
            MarkerWrite::FailedUnresolved => {
                return Err(CommitFailure::Unconfirmed(self.revision));
            }
        }
        if rename_temp(&self.fault, &temp, &self.state_path).is_err() {
            // A rename syscall error is not proof that the rename did not take
            // effect (distributed filesystems can return an error after
            // applying the operation). Leave the pending marker and the temp,
            // and quarantine instead of claiming a rollback.
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if let Some(fault) = self.fault.as_ref() {
            if fault.fail.as_deref() == Some("crash_after_rename") {
                return Err(CommitFailure::Crash);
            }
        }
        if fault_point(&self.fault, "rename_in_progress").is_err() {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if fault_point(&self.fault, "dir_sync").is_err() {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if hash_state::sync_parent(&self.directory).is_err() {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if marker_set_phase(
            &self.directory,
            MarkerPhase::Durable,
            next_revision,
            &hash,
            &self.fault,
        )
        .is_err()
        {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if fault_point(&self.fault, "marker_durable_ready").is_err() {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        self.prepared = Some(PendingCommit {
            revision: next_revision,
            hash,
            desired,
        });
        Ok(next_revision)
    }

    /// Record the lifecycle authorization durably before removing the marker.
    /// If the unlink (or its directory sync) fails, the `acked` marker remains
    /// and a successor may accept this already-authorized state. A lost reply
    /// after this point has the same property; no exactly-once promise is made.
    fn commit_ack(&mut self, revision: u64) -> Result<u64, CommitFailure> {
        let Some(pending) = self.prepared.take() else {
            return Err(CommitFailure::Unconfirmed(self.revision));
        };
        if pending.revision != revision {
            self.prepared = Some(pending);
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if marker_set_phase(
            &self.directory,
            MarkerPhase::Acked,
            revision,
            &pending.hash,
            &self.fault,
        )
        .is_err()
        {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        if fault_point(&self.fault, "marker_acked_ready").is_err() {
            return Err(CommitFailure::Unconfirmed(self.revision));
        }
        // A failed unlink/sync leaves the durable `acked` marker; it is not an
        // ambiguity about whether the state or authorization exists.
        let _ = remove_pending_marker(&self.directory, &self.fault);
        self.last_state = Some(pending.desired);
        self.revision = revision;
        Ok(revision)
    }

    fn marker_is_acked_for_last_state(&self) -> bool {
        let Some(last_state) = self.last_state.as_deref() else {
            return false;
        };
        let expected = state_hash(last_state);
        matches!(
            read_marker(&self.directory),
            MarkerRead::Record(record)
                if record.phase == MarkerPhase::Acked && record.hash == expected
        )
    }

    /// Detect a file/namespace change made by an older app or another writer
    /// that does not honor the advisory lock. A check-then-rename still has a
    /// residual race with such a writer; we never reset or delete its data and
    /// fail closed on the next observed change instead of pretending the window
    /// is closed.
    fn external_change(&self) -> bool {
        match hash_state::read_namespace_only(&self.directory) {
            hash_state::NamespaceLoad::Ready(namespace) if namespace == self.namespace => {}
            _ => return true,
        }
        match read_bounded(&self.state_path) {
            Ok(current) => current != self.last_state,
            Err(ReadStateError::Unavailable) => true,
        }
    }

    fn write_temp(&self, bytes: &[u8]) -> std::io::Result<PathBuf> {
        use std::io::Write as _;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        for _ in 0..16 {
            let sequence = COUNTER.fetch_add(1, Ordering::SeqCst);
            let temp = self.directory.join(format!(
                ".automatic-hash-persist-{}-{sequence}.tmp",
                std::process::id()
            ));
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
            }
            let mut file = match options.open(&temp) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            };
            let result = file
                .write_all(bytes)
                .and_then(|()| {
                    fault_point(&self.fault, "file_sync")
                        .map_err(|()| std::io::Error::other("injected file sync fault"))
                })
                .and_then(|()| file.sync_all());
            drop(file);
            if let Err(error) = result {
                let _ = std::fs::remove_file(&temp);
                return Err(error);
            }
            return Ok(temp);
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "persistence temp path is busy",
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkerPhase {
    Pending,
    Durable,
    Acked,
}

impl MarkerPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Durable => "durable",
            Self::Acked => "acked",
        }
    }
}

struct MarkerRecord {
    phase: MarkerPhase,
    hash: String,
}

enum MarkerRead {
    Missing,
    Record(MarkerRecord),
    Invalid,
}

enum MarkerRecovery {
    Absent,
    Acked,
    Unresolved,
}

enum MarkerWrite {
    Written,
    AlreadyExists,
    FailedRolledBack,
    FailedUnresolved,
}

fn state_hash(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn marker_line(phase: MarkerPhase, revision: u64, hash: &str) -> Vec<u8> {
    format!(
        "{} {} {} {}\n",
        String::from_utf8_lossy(MARKER_HEADER),
        phase.as_str(),
        revision,
        hash
    )
    .into_bytes()
}

fn parse_marker(bytes: &[u8]) -> Option<MarkerRecord> {
    if bytes.starts_with(LEGACY_PENDING_MARKER) {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let mut parts = text.trim_end_matches(['\r', '\n']).split(' ');
    let header = parts.next()?;
    if header.as_bytes() != MARKER_HEADER {
        return None;
    }
    let phase = match parts.next()? {
        "pending" => MarkerPhase::Pending,
        "durable" => MarkerPhase::Durable,
        "acked" => MarkerPhase::Acked,
        _ => return None,
    };
    let revision = parts.next()?.parse::<u64>().ok()?;
    if revision == 0 {
        return None;
    }
    let hash = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    Some(MarkerRecord {
        phase,
        hash: hash.to_owned(),
    })
}

fn read_marker(directory: &Path) -> MarkerRead {
    let path = directory.join(HASH_PENDING_FILE);
    match std::fs::symlink_metadata(&path) {
        Ok(meta) => {
            if !meta.file_type().is_file() || meta.len() > MAX_MARKER_BYTES as u64 {
                return MarkerRead::Invalid;
            }
            match std::fs::read(&path) {
                Ok(bytes) => match parse_marker(&bytes) {
                    Some(record) => MarkerRead::Record(record),
                    None => MarkerRead::Invalid,
                },
                Err(_) => MarkerRead::Invalid,
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => MarkerRead::Missing,
        // An unreadable marker is unresolved, not absent.
        Err(_) => MarkerRead::Invalid,
    }
}

fn recover_pending_marker(directory: &Path) -> MarkerRecovery {
    match read_marker(directory) {
        MarkerRead::Missing => MarkerRecovery::Absent,
        MarkerRead::Invalid => MarkerRecovery::Unresolved,
        MarkerRead::Record(record) => match record.phase {
            MarkerPhase::Pending | MarkerPhase::Durable => MarkerRecovery::Unresolved,
            MarkerPhase::Acked => {
                let state_path = directory.join(HASH_STATE_FILE);
                match read_bounded(&state_path) {
                    Ok(Some(bytes)) if state_hash(&bytes) == record.hash => {
                        // The acked marker is durable but its unlink was not.
                        // The authorized state is already durable, so proceed;
                        // a later failure to clean up remains harmless.
                        let _ = remove_pending_marker(directory, &None);
                        MarkerRecovery::Acked
                    }
                    _ => MarkerRecovery::Unresolved,
                }
            }
        },
    }
}

fn cleanup_failed_marker(directory: &Path, fault: &Option<FaultConfig>) -> bool {
    if fault.as_ref().and_then(|fault| fault.cleanup.as_deref()) == Some("marker_cleanup")
        || fault_point(fault, "marker_cleanup").is_err()
    {
        return false;
    }
    match std::fs::remove_file(directory.join(HASH_PENDING_FILE)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return false,
    }
    hash_state::sync_parent(directory).is_ok()
}

fn write_pending_marker(
    directory: &Path,
    revision: u64,
    hash: &str,
    allow_existing_acked: bool,
    fault: &Option<FaultConfig>,
) -> MarkerWrite {
    if fault_point(fault, "marker_create").is_err() {
        return MarkerWrite::FailedRolledBack;
    }
    let path = directory.join(HASH_PENDING_FILE);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !allow_existing_acked {
                return MarkerWrite::AlreadyExists;
            }
            let mut options = std::fs::OpenOptions::new();
            options.write(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
            }
            match options.open(&path) {
                Ok(file) => file,
                Err(_) => return MarkerWrite::FailedUnresolved,
            }
        }
        Err(_) => return MarkerWrite::FailedRolledBack,
    };
    let line = marker_line(MarkerPhase::Pending, revision, hash);
    let written = fault_point(fault, "marker_pending_write")
        .map_err(|()| std::io::Error::other("injected marker write fault"))
        .and_then(|()| file.write_all(&line))
        .and_then(|()| {
            fault_point(fault, "marker_pending_file_sync")
                .map_err(|()| std::io::Error::other("injected marker file sync fault"))
        })
        .and_then(|()| file.sync_all());
    drop(file);
    if written.is_err() {
        return if cleanup_failed_marker(directory, fault) {
            MarkerWrite::FailedRolledBack
        } else {
            MarkerWrite::FailedUnresolved
        };
    }
    if fault_point(fault, "marker_pending_dir_sync").is_err()
        || hash_state::sync_parent(directory).is_err()
    {
        return if cleanup_failed_marker(directory, fault) {
            MarkerWrite::FailedRolledBack
        } else {
            MarkerWrite::FailedUnresolved
        };
    }
    MarkerWrite::Written
}

fn marker_set_phase(
    directory: &Path,
    phase: MarkerPhase,
    revision: u64,
    hash: &str,
    fault: &Option<FaultConfig>,
) -> Result<(), ()> {
    let prefix = match phase {
        MarkerPhase::Pending => "marker_pending",
        MarkerPhase::Durable => "marker_durable",
        MarkerPhase::Acked => "marker_ack",
    };
    if fault_point(fault, &format!("{prefix}_phase_write")).is_err() {
        return Err(());
    }
    let path = directory.join(HASH_PENDING_FILE);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(&path).map_err(|_| ())?;
    let line = marker_line(phase, revision, hash);
    file.write_all(&line).map_err(|_| ())?;
    if fault_point(fault, &format!("{prefix}_file_sync")).is_err() {
        return Err(());
    }
    file.sync_all().map_err(|_| ())
}

fn remove_pending_marker(directory: &Path, fault: &Option<FaultConfig>) -> Result<(), ()> {
    if fault_point(fault, "marker_unlink").is_err() {
        return Err(());
    }
    std::fs::remove_file(directory.join(HASH_PENDING_FILE)).map_err(|_| ())?;
    if fault_point(fault, "marker_unlink_sync").is_err() {
        return Err(());
    }
    hash_state::sync_parent(directory).map_err(|_| ())
}

fn rename_temp(fault: &Option<FaultConfig>, temp: &Path, state: &Path) -> std::io::Result<()> {
    match fault.as_ref().and_then(|fault| fault.fail.as_deref()) {
        Some("rename_error") => Err(std::io::Error::other("injected rename error")),
        Some("rename_after_effect") => {
            std::fs::rename(temp, state)?;
            Err(std::io::Error::other("injected rename error after effect"))
        }
        _ => std::fs::rename(temp, state),
    }
}

fn cleanup_stale_temps(directory: &Path) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        if removed >= MAX_TEMP_CLEANUP {
            break;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(".automatic-hash-persist-") || !name.ends_with(".tmp") {
            continue;
        }
        if entry.file_type().is_ok_and(|kind| kind.is_file())
            && std::fs::remove_file(entry.path()).is_ok()
        {
            removed += 1;
        }
    }
}

fn fault_point(fault: &Option<FaultConfig>, point: &str) -> Result<(), ()> {
    let Some(fault) = fault else {
        return Ok(());
    };
    if fault.fail.as_deref() == Some(point) {
        return Err(());
    }
    if fault.pause.as_deref() != Some(point) {
        return Ok(());
    }
    let Some(directory) = fault.dir.as_deref() else {
        return Ok(());
    };
    let entered = directory.join(format!("entered-{point}"));
    let release = directory.join(format!("release-{point}"));
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&entered);
    let attempt = directory.join(format!("entered-{point}-{}", std::process::id()));
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&attempt);
    let deadline = Instant::now() + FAULT_WAIT;
    while Instant::now() < deadline {
        if release.exists() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize_helper_process_tests() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        match LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[test]
    fn helper_mode_is_not_selected_by_ordinary_arguments() {
        assert!(!std::env::args_os().any(|arg| arg == HELPER_ARG));
    }

    #[test]
    fn request_line_bound_is_finite() {
        const {
            assert!(MAX_PROTOCOL_LINE >= MAX_STATE_BYTES);
        }
    }

    #[test]
    fn a_full_bounded_queue_fails_closed_without_blocking() {
        let (tx, rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        for _ in 0..QUEUE_CAPACITY {
            let (reply, _reply_rx) = mpsc::sync_channel(1);
            tx.try_send(Command::SetFault { fault: None, reply })
                .expect("bounded queue accepts its capacity");
        }
        let domain = PersistenceDomain {
            inner: Arc::new(Inner {
                tx,
                child: Mutex::new(None),
                state: Mutex::new(DomainState {
                    status: DomainStatus::Ready,
                    revision: 0,
                    prepared: None,
                }),
            }),
        };
        assert_eq!(domain.commit(0, vec![1]), Err(PersistenceError::Busy));
        assert!(domain.is_confirmed(), "a busy queue is not ambiguity");
        drop(rx);
    }

    #[test]
    fn temp_cleanup_only_targets_owner_temp_names() {
        let root = std::env::temp_dir().join(format!(
            "hash-persist-cleanup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("temp dir");
        let ours = root.join(".automatic-hash-persist-1-1.tmp");
        let foreign = root.join("user-file.tmp");
        std::fs::write(&ours, b"x").expect("ours");
        std::fs::write(&foreign, b"x").expect("foreign");
        cleanup_stale_temps(&root);
        assert!(!ours.exists());
        assert!(foreign.exists(), "unrelated user files are never deleted");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    fn true_child() -> Child {
        ProcessCommand::new("/bin/true")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn true")
    }

    #[cfg(unix)]
    fn true_job() -> ReapJob {
        let permit = Arc::new(HelperSlots::new(1))
            .try_acquire()
            .expect("test permit");
        ReapJob {
            child: true_child(),
            permit,
        }
    }

    #[cfg(unix)]
    #[test]
    fn reaper_persistent_worker_does_not_spawn_twice_or_miss_a_wakeup() {
        let reaper = Arc::new(Reaper::new());
        let mut starts = 0;
        reaper.enqueue_with(true_job(), || {
            starts += 1;
            Ok(())
        });
        assert_eq!(starts, 1);
        let mut first = reaper.take_next().expect("queued child");
        let _ = first.child.wait();
        assert!(reaper.take_next().is_none());
        {
            let inner = reaper.inner.lock().expect("reaper state");
            assert!(inner.running, "the worker stays alive between handoffs");
            assert!(!inner.starting);
            assert!(inner.queue.is_empty());
        }
        reaper.enqueue_with(true_job(), || {
            starts += 1;
            Ok(())
        });
        assert_eq!(
            starts, 1,
            "a persistent worker already owns the queue and must not respawn"
        );
        let mut second = reaper.take_next().expect("second child");
        let _ = second.child.wait();
        assert!(reaper.take_next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn helper_capacity_saturation_rejects_before_spawn() {
        let root = std::env::temp_dir().join(format!(
            "hash-persist-capacity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let slots = Arc::new(HelperSlots::new(1));
        let held = slots.try_acquire().expect("reserve the only slot");
        let result = PersistenceDomain::open_with(&root, None, &slots, || true);
        assert!(
            matches!(result, Err(PersistenceError::Unavailable)),
            "saturated capacity must reject the owner"
        );
        drop(held);
        assert!(
            slots.try_acquire().is_some(),
            "dropping the reservation releases capacity"
        );
        assert!(
            std::fs::read_dir(&root)
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(true),
            "no helper process or file may be created once capacity is exhausted"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reaper_start_failure_prevents_any_helper_spawn() {
        let root = std::env::temp_dir().join(format!(
            "hash-persist-reaper-start-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let slots = Arc::new(HelperSlots::new(1));
        let result = PersistenceDomain::open_with(&root, None, &slots, || false);
        assert!(
            matches!(result, Err(PersistenceError::Unavailable)),
            "a missing reaper must reject initialization"
        );
        assert!(
            std::fs::read_dir(&root)
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(true),
            "no helper may be spawned when the reaper cannot start"
        );
        assert!(
            slots.try_acquire().is_some(),
            "the rejected owner must not retain capacity"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reaper_spawn_failure_retains_capacity_and_a_later_handoff_recovers() {
        let reaper = Arc::new(Reaper::new());
        let slots = Arc::new(HelperSlots::new(1));
        let permit = slots.try_acquire().expect("reserve the only slot");
        let mut failures = 0;
        let started = Instant::now();
        reaper.enqueue_with(
            ReapJob {
                child: true_child(),
                permit,
            },
            || {
                failures += 1;
                Err(std::io::Error::other("injected spawn failure"))
            },
        );
        assert_eq!(
            failures, REAPER_SPAWN_ATTEMPTS,
            "spawn failure retries are bounded"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "spawn failure must not block shutdown indefinitely"
        );
        {
            let inner = reaper.inner.lock().expect("reaper state");
            assert!(!inner.running, "spawn failure must not stick running true");
            assert!(!inner.starting);
            assert_eq!(inner.queue.len(), 1, "the child is retained for retry");
        }
        assert!(
            slots.try_acquire().is_none(),
            "capacity stays held while the unreaped child is queued"
        );
        let mut starts = 0;
        reaper.enqueue_with(true_job(), || {
            starts += 1;
            Ok(())
        });
        assert_eq!(starts, 1, "the next handoff retries the reaper");
        let mut first = reaper.take_next().expect("retained child");
        let _ = first.child.wait();
        drop(first);
        assert!(
            slots.try_acquire().is_some(),
            "capacity is released only when the retained child is reaped"
        );
        let mut second = reaper.take_next().expect("new child");
        let _ = second.child.wait();
        assert!(reaper.take_next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn reaper_all_spawn_failures_during_concurrent_handoff_retry_and_recover() {
        use std::sync::Barrier;

        let reaper = Arc::new(Reaper::new());
        let entered = Arc::new(Barrier::new(2));
        let enqueued = Arc::new(Barrier::new(2));
        let handoff = {
            let reaper = Arc::clone(&reaper);
            let entered = Arc::clone(&entered);
            let enqueued = Arc::clone(&enqueued);
            std::thread::spawn(move || {
                let mut attempts = 0;
                reaper.enqueue_with(true_job(), || {
                    attempts += 1;
                    if attempts == 1 {
                        entered.wait();
                        enqueued.wait();
                    }
                    if attempts <= REAPER_SPAWN_ATTEMPTS {
                        Err(std::io::Error::other("injected spawn failure"))
                    } else {
                        Ok(())
                    }
                });
                attempts
            })
        };

        entered.wait();
        let mut concurrent_starts = 0;
        reaper.enqueue_with(true_job(), || {
            concurrent_starts += 1;
            Ok(())
        });
        assert_eq!(
            concurrent_starts, 0,
            "an enqueue while the first handoff is starting must not start a second worker"
        );
        enqueued.wait();

        let attempts = handoff.join().expect("handoff thread");
        assert_eq!(
            attempts,
            REAPER_SPAWN_ATTEMPTS + 1,
            "an all-fail round must retry after a concurrent handoff"
        );
        {
            let inner = reaper.inner.lock().expect("reaper state");
            assert!(inner.running, "the retried worker owns the queue");
            assert!(!inner.starting);
            assert_eq!(
                inner.queue.len(),
                2,
                "both concurrent children are handed to the retried worker"
            );
        }
        let mut first = reaper.take_next().expect("first child");
        let _ = first.child.wait();
        let mut second = reaper.take_next().expect("second child");
        let _ = second.child.wait();
        assert!(reaper.take_next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn reaper_wait_error_is_retried_and_never_drops_the_child() {
        let reaper = Arc::new(Reaper::new());
        let mut starts = 0;
        reaper.enqueue_with(true_job(), || {
            starts += 1;
            Ok(())
        });
        reaper.enqueue_with(true_job(), || {
            starts += 1;
            Ok(())
        });
        assert_eq!(starts, 1, "one worker owns both queued children");

        let mut waits = 0;
        drain_reaper(&reaper, |child: &mut Child| {
            waits += 1;
            match waits {
                1 => Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "injected interruption",
                )),
                2 => Err(std::io::Error::other("injected unresolved wait error")),
                _ => child.wait(),
            }
        });
        assert_eq!(
            waits, 4,
            "both children are waited after the errors; neither handle is dropped"
        );
        assert!(reaper.take_next().is_none());
        let inner = reaper.inner.lock().expect("reaper state");
        assert!(inner.running);
        assert!(!inner.starting);
        assert!(inner.queue.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn reaper_panic_retains_child_and_capacity_and_stops_new_workers() {
        let slots = Arc::new(HelperSlots::new(1));
        let permit = slots.try_acquire().expect("reserve the only slot");
        let reaper = Arc::new(Reaper::new());
        let mut starts = 0;
        let first_pid = {
            let child = true_child();
            let pid = child.id();
            reaper.enqueue_with(ReapJob { child, permit }, || {
                starts += 1;
                Ok(())
            });
            pid
        };
        assert_eq!(starts, 1, "the first handoff is accepted");

        let worker = {
            let reaper = Arc::clone(&reaper);
            std::thread::spawn(move || {
                reaper_loop_with(reaper, |_child: &mut Child| {
                    panic!("injected reaper wait panic")
                })
            })
        };
        let started = Instant::now();
        worker
            .join()
            .expect("the panic is contained by the reaper worker");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a panicking wait must not leave the worker hanging"
        );

        assert!(
            slots.try_acquire().is_none(),
            "capacity stays reserved for the child whose reaping panicked"
        );
        {
            let inner = reaper.inner.lock().expect("reaper state");
            assert!(inner.poisoned, "the reaper must fail closed after a panic");
            assert!(!inner.running);
            assert!(!inner.starting);
            assert_eq!(
                inner.queue.len(),
                1,
                "the panicked child handle is retained instead of silently dropped"
            );
        }

        let mut later_starts = 0;
        reaper.enqueue_with(true_job(), || {
            later_starts += 1;
            Ok(())
        });
        assert_eq!(
            later_starts, 0,
            "a poisoned reaper must not start another worker or helper"
        );
        assert!(
            !reaper.ensure_started_with(|| {
                panic!("a poisoned reaper must not attempt another spawn")
            }),
            "ensure_started must reject a poisoned reaper"
        );
        {
            let inner = reaper.inner.lock().expect("reaper state");
            assert_eq!(
                inner.queue.len(),
                2,
                "jobs queued after the panic are retained, not dropped"
            );
        }

        let mut first = reaper.take_next().expect("panicked child retained");
        assert_eq!(first.child.id(), first_pid);
        let _ = first.child.wait();
        drop(first);
        assert!(
            slots.try_acquire().is_some(),
            "capacity is released only after the retained child is confirmed reaped"
        );
        let mut second = reaper.take_next().expect("later child retained");
        let _ = second.child.wait();
        assert!(reaper.take_next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn hung_wait_holds_capacity_until_the_eventual_reap_releases_it() {
        use std::sync::mpsc;

        let slots = Arc::new(HelperSlots::new(1));
        let permit = slots.try_acquire().expect("reserve the only slot");
        let reaper = Arc::new(Reaper::new());
        reaper.enqueue_with(
            ReapJob {
                child: true_child(),
                permit,
            },
            || Ok(()),
        );
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let drain = {
            let reaper = Arc::clone(&reaper);
            let mut first = true;
            std::thread::spawn(move || {
                drain_reaper(&reaper, move |child: &mut Child| {
                    if first {
                        first = false;
                        entered_tx.send(()).expect("announce child wait");
                        release_rx.recv().expect("release child wait");
                    }
                    child.wait()
                });
            })
        };

        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the worker is waiting on the first child");
        assert!(
            slots.try_acquire().is_none(),
            "a hung wait must not release capacity"
        );
        let started = Instant::now();
        reaper.enqueue_with(true_job(), || {
            panic!("the active worker must own the new handoff")
        });
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a handoff must not wait for a child that is still running"
        );
        release_tx.send(()).expect("release the child wait");
        drain.join().expect("drain thread");
        assert!(
            slots.try_acquire().is_some(),
            "capacity is released once the wait confirms the reap"
        );
        while let Some(mut job) = reaper.take_next() {
            let _ = job.child.wait();
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn helper_prelock_guard_is_system_wide_and_released_on_drop() {
        let _serial = serialize_helper_process_tests();
        let first = acquire_initialization_guard().expect("first guard");
        assert!(matches!(
            acquire_initialization_guard(),
            Err(HelperGuardError::Busy)
        ));
        drop(first);
        // A concurrently forked helper can briefly retain the inherited
        // close-on-exec descriptor between fork and exec, so a single bind
        // immediately after drop may observe transient EADDRINUSE. Retry
        // within a bound to assert the guard is actually released.
        let deadline = Instant::now() + Duration::from_secs(2);
        let _third = loop {
            match acquire_initialization_guard() {
                Ok(guard) => break guard,
                Err(HelperGuardError::Busy) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => panic!("guard released"),
            }
        };
    }

    #[cfg(target_os = "linux")]
    fn spawn_hash_helper(
        program: &Path,
        arg: Option<std::ffi::OsString>,
        root: &Path,
        fault: Option<FaultConfig>,
    ) -> (Child, BufReader<ChildStdout>) {
        let mut command = ProcessCommand::new(program);
        if let Some(arg) = arg {
            command.arg(arg);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn helper");
        let request = WireRequest {
            kind: "init".into(),
            dir: Some(root.to_string_lossy().into_owned()),
            base: 0,
            data: None,
            fault,
        };
        let mut line = serde_json::to_vec(&request).expect("wire request");
        line.push(b'\n');
        child
            .stdin
            .as_mut()
            .expect("helper stdin")
            .write_all(&line)
            .expect("write init");
        child
            .stdin
            .as_mut()
            .expect("helper stdin")
            .flush()
            .expect("flush init");
        let stdout = BufReader::new(child.stdout.take().expect("helper stdout"));
        (child, stdout)
    }

    #[cfg(target_os = "linux")]
    fn read_wire_response(stdout: &mut impl BufRead) -> WireResponse {
        let mut line = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            stdout.read_exact(&mut byte).expect("helper response");
            if byte[0] == b'\n' {
                break;
            }
            line.push(byte[0]);
        }
        serde_json::from_slice(&line).expect("wire response")
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_second_process_cannot_join_a_prelock_blocked_helper() {
        let _serial = serialize_helper_process_tests();
        let root = std::env::temp_dir().join(format!(
            "hash-persist-guard-process-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("temp dir");
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                .expect("private temp dir");
        }
        let (program, program_arg) = helper_candidates()
            .expect("helper candidates")
            .into_iter()
            .next()
            .expect("helper candidate");
        let (mut first, mut first_out) = spawn_hash_helper(
            &program,
            program_arg.clone(),
            &root,
            Some(FaultConfig {
                pause: Some("prelock".into()),
                fail: None,
                cleanup: None,
                dir: Some(root.clone()),
            }),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !root.join("entered-prelock").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            root.join("entered-prelock").exists(),
            "the first helper must hold the initialization guard before path resolution"
        );

        let (mut second, mut second_out) = spawn_hash_helper(&program, program_arg, &root, None);
        let busy = read_wire_response(&mut second_out);
        assert_eq!(
            busy.status, "busy",
            "a different process must be rejected before preparation"
        );
        let _ = second.kill();
        let _ = second.wait();

        std::fs::write(root.join("release-prelock"), b"go").expect("release first helper");
        let ready = read_wire_response(&mut first_out);
        assert_eq!(ready.status, "ready", "the first helper completes normally");
        let _ = first.kill();
        let _ = first.wait();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn prelock_guard_rejects_lexical_and_symlink_aliases_before_preparation() {
        let _serial = serialize_helper_process_tests();
        let root = std::env::temp_dir().join(format!(
            "hash-persist-guard-alias-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("temp dir");
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                .expect("private temp dir");
        }
        let symlink = root.with_file_name(format!(
            "{}-symlink",
            root.file_name().expect("temp dir name").to_string_lossy()
        ));
        std::os::unix::fs::symlink(&root, &symlink).expect("symlink alias");
        let (program, program_arg) = helper_candidates()
            .expect("helper candidates")
            .into_iter()
            .next()
            .expect("helper candidate");
        let paused_prelock = || FaultConfig {
            pause: Some("prelock".into()),
            fail: None,
            cleanup: None,
            dir: Some(root.clone()),
        };
        let (mut first, mut first_out) =
            spawn_hash_helper(&program, program_arg.clone(), &root, Some(paused_prelock()));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !root.join("entered-prelock").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            root.join("entered-prelock").exists(),
            "the first helper must hold the initialization guard before path resolution"
        );

        for alias in [root.join("."), symlink.clone()] {
            let (mut child, mut out) = spawn_hash_helper(
                &program,
                program_arg.clone(),
                &alias,
                Some(paused_prelock()),
            );
            let pid = child.id();
            let started = Instant::now();
            let exited = loop {
                match child.try_wait().expect("poll alias helper") {
                    Some(status) => break Some(status),
                    None if started.elapsed() < Duration::from_secs(2) => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    None => break None,
                }
            };
            if exited.is_none() {
                let _ = std::fs::write(root.join("release-prelock"), b"go");
                let _ = child.kill();
                let _ = child.wait();
                let _ = first.kill();
                let _ = first.wait();
                let _ = std::fs::remove_file(&symlink);
                let _ = std::fs::remove_dir_all(&root);
                panic!(
                    "alias {alias:?} entered preparation instead of being rejected before the directory lock"
                );
            }
            let response = read_wire_response(&mut out);
            assert_eq!(
                response.status, "busy",
                "alias {alias:?} must be rejected by the global initialization guard"
            );
            assert!(
                !root.join(format!("entered-prelock-{pid}")).exists(),
                "alias {alias:?} must not reach the pre-resolution barrier"
            );
        }

        std::fs::write(root.join("release-prelock"), b"go").expect("release first helper");
        let ready = read_wire_response(&mut first_out);
        assert_eq!(ready.status, "ready", "the first helper completes normally");
        let _ = first.kill();
        let _ = first.wait();
        let _ = std::fs::remove_file(&symlink);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn initialization_guard_release_allows_independent_directory() {
        let _serial = serialize_helper_process_tests();
        let unique = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let first_root = std::env::temp_dir().join(format!("hash-persist-guard-a-{unique}"));
        let other_root = std::env::temp_dir().join(format!("hash-persist-guard-b-{unique}"));
        for root in [&first_root, &other_root] {
            std::fs::create_dir_all(root).expect("temp dir");
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))
                .expect("private temp dir");
        }
        let (program, program_arg) = helper_candidates()
            .expect("helper candidates")
            .into_iter()
            .next()
            .expect("helper candidate");
        let (mut first, mut first_out) = spawn_hash_helper(
            &program,
            program_arg.clone(),
            &first_root,
            Some(FaultConfig {
                pause: Some("prelock".into()),
                fail: None,
                cleanup: None,
                dir: Some(first_root.clone()),
            }),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !first_root.join("entered-prelock").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            first_root.join("entered-prelock").exists(),
            "the first helper must hold the initialization guard before path resolution"
        );

        // The guard is intentionally global during initialization: even a
        // disjoint directory is rejected until the first helper has taken its
        // own cooperating directory lock and released this guard.
        let (mut blocked, mut blocked_out) =
            spawn_hash_helper(&program, program_arg.clone(), &other_root, None);
        let blocked_response = read_wire_response(&mut blocked_out);
        assert_eq!(
            blocked_response.status, "busy",
            "one helper globally must serialize initialization before release"
        );
        let _ = blocked.kill();
        let _ = blocked.wait();

        std::fs::write(first_root.join("release-prelock"), b"go").expect("release first helper");
        let ready = read_wire_response(&mut first_out);
        assert_eq!(ready.status, "ready", "the first helper completes normally");

        // The first helper still holds its own per-directory lock, but it no
        // longer holds the initialization guard, so an independent directory
        // must be able to initialize concurrently.
        let (mut independent, mut independent_out) =
            spawn_hash_helper(&program, program_arg, &other_root, None);
        let independent_response = read_wire_response(&mut independent_out);
        assert_eq!(
            independent_response.status, "ready",
            "an independent directory must proceed after initialization guard release"
        );
        let _ = independent.kill();
        let _ = independent.wait();
        let _ = first.kill();
        let _ = first.wait();
        let _ = std::fs::remove_dir_all(&first_root);
        let _ = std::fs::remove_dir_all(&other_root);
    }
    #[cfg(target_os = "linux")]
    fn diagnostic_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "hash-persist-diag-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("temp dir");
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                .expect("private temp dir");
        }
        root
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn prepare_timeout_diagnostic_distinguishes_timeout_and_quarantines() {
        let _serial = serialize_helper_process_tests();
        let root = diagnostic_root("prepare-timeout");
        let bytes = br#"{"version":2,"defaultEnabled":false,"records":[]}"#.to_vec();
        let (domain, open) = PersistenceDomain::open(
            &root,
            Some(FaultConfig {
                pause: Some("dir_sync".into()),
                fail: None,
                cleanup: None,
                dir: Some(root.clone()),
            }),
        )
        .expect("open owner");
        assert_eq!(open.revision, 0);
        clear_persist_diagnostic();
        let started = Instant::now();
        assert!(matches!(
            domain.prepare(0, bytes),
            Ok(PrepareOutcome::Unconfirmed)
        ));
        let elapsed = started.elapsed();
        assert!(
            elapsed >= COMMIT_TIMEOUT,
            "prepare must wait for the full commit deadline: {elapsed:?}"
        );
        assert!(
            root.join("entered-dir_sync").exists(),
            "the helper must actually reach the injected prepare pause"
        );
        assert_eq!(domain.status(), DomainStatus::Quarantined);
        let diag = last_persist_diagnostic().expect("prepare diagnostic");
        assert!(
            diag.contains("hash-persist prepare phase=recv outcome=timeout"),
            "{diag}"
        );
        assert!(diag.contains("status_before=Ready"), "{diag}");
        assert!(diag.contains("status_after=Quarantined"), "{diag}");
        assert!(diag.contains("revision=0"), "{diag}");
        assert!(diag.contains("base=0"), "{diag}");
        let root_text = root.to_string_lossy().into_owned();
        assert!(
            !diag.contains(&root_text),
            "diagnostic must not leak paths: {diag}"
        );
        assert!(
            !diag.contains("defaultEnabled"),
            "diagnostic must not leak state bytes: {diag}"
        );
        assert!(
            matches!(
                domain.prepare(
                    0,
                    br#"{"version":2,"defaultEnabled":false,"records":[]}"#.to_vec()
                ),
                Err(PersistenceError::Unavailable)
            ),
            "a timed-out prepare must leave the owner quarantined"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn ack_timeout_diagnostic_distinguishes_timeout_and_quarantines() {
        let _serial = serialize_helper_process_tests();
        let root = diagnostic_root("ack-timeout");
        let bytes = br#"{"version":2,"defaultEnabled":false,"records":[]}"#.to_vec();
        let (domain, open) = PersistenceDomain::open(
            &root,
            Some(FaultConfig {
                pause: Some("marker_acked_ready".into()),
                fail: None,
                cleanup: None,
                dir: Some(root.clone()),
            }),
        )
        .expect("open owner");
        assert_eq!(open.revision, 0);
        let prepared = match domain.prepare(0, bytes).expect("prepare durable state") {
            PrepareOutcome::Prepared(prepared) => prepared,
            _ => panic!("prepare must succeed before the ack pause"),
        };
        clear_persist_diagnostic();
        let started = Instant::now();
        assert!(matches!(prepared.confirm(), Ok(CommitOutcome::Unconfirmed)));
        let elapsed = started.elapsed();
        assert!(
            elapsed >= COMMIT_TIMEOUT,
            "ack must wait for the full commit deadline: {elapsed:?}"
        );
        assert!(
            root.join("entered-marker_acked_ready").exists(),
            "the helper must actually reach the injected ack pause"
        );
        assert_eq!(domain.status(), DomainStatus::Quarantined);
        let diag = last_persist_diagnostic().expect("ack diagnostic");
        assert!(
            diag.contains("hash-persist ack phase=recv outcome=timeout"),
            "{diag}"
        );
        assert!(diag.contains("status_before=Ready"), "{diag}");
        assert!(diag.contains("status_after=Quarantined"), "{diag}");
        assert!(diag.contains("revision=0"), "{diag}");
        assert!(diag.contains("expected_revision=1"), "{diag}");
        let root_text = root.to_string_lossy().into_owned();
        assert!(
            !diag.contains(&root_text),
            "diagnostic must not leak paths: {diag}"
        );
        assert!(
            !diag.contains("defaultEnabled"),
            "diagnostic must not leak state bytes: {diag}"
        );
        assert!(
            matches!(
                domain.prepare(
                    0,
                    br#"{"version":2,"defaultEnabled":false,"records":[]}"#.to_vec()
                ),
                Err(PersistenceError::Unavailable)
            ),
            "a timed-out ack must leave the owner quarantined"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn open_timeout_diagnostic_distinguishes_timeout_and_quarantines() {
        let _serial = serialize_helper_process_tests();
        let root = diagnostic_root("open-timeout");
        clear_persist_diagnostic();
        let started = Instant::now();
        let result = PersistenceDomain::open(
            &root,
            Some(FaultConfig {
                pause: Some("load".into()),
                fail: None,
                cleanup: None,
                dir: Some(root.clone()),
            }),
        );
        let elapsed = started.elapsed();
        assert!(
            matches!(result, Err(PersistenceError::Unresolved)),
            "the timed-out open must fail closed"
        );
        assert!(
            elapsed >= OPEN_TIMEOUT,
            "open must wait for the full open deadline: {elapsed:?}"
        );
        assert!(
            root.join("entered-load").exists(),
            "the helper must actually reach the injected open pause"
        );
        let diag = last_persist_diagnostic().expect("open diagnostic");
        assert!(
            diag.contains("hash-persist open phase=receive outcome=timeout"),
            "{diag}"
        );
        assert!(diag.contains("status_after=Quarantined"), "{diag}");
        assert!(diag.contains("response_status=none"), "{diag}");
        let root_text = root.to_string_lossy().into_owned();
        assert!(
            !diag.contains(&root_text),
            "diagnostic must not leak paths: {diag}"
        );
        assert!(
            !diag.contains("defaultEnabled"),
            "diagnostic must not leak state bytes: {diag}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
