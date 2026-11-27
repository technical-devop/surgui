use std::ffi::OsStr;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tauri::path::BaseDirectory;
use tauri::Emitter as _;
use tauri::Manager as _;

use crate::automatic_hash::{self, HashWorkError};
use crate::hash_state::{DownloadHashDto, HashIntent, HashStore};
use crate::managed_server::{ManagedServer, OwnerLifetime, DEFAULT_STARTUP_TIMEOUT};
use crate::open_location::{
    self, LocationError, LocationLaunch, LocationLauncher, SystemLocationLauncher,
};
use crate::progress_stream::ProgressStream;
use crate::surge_client::{
    AddOutcome, BulkControlAction, BulkControlOutcome, ConnectError, ControlAction, Download,
    SurgeClient, MAX_CONCURRENT_DOWNLOADS_MAX, MAX_CONCURRENT_DOWNLOADS_MIN, MAX_RATE_LIMIT_BPS,
};

/// How often an async lifecycle operation checks the app-shutdown flag.
const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Bound for `shutdown_managed` to wait for detached start/retirement work.
/// A single graceful retirement is itself bounded at roughly eight seconds.
const LIFECYCLE_DRAIN_TIMEOUT: Duration = Duration::from_secs(9);

/// How often a new managed start polls for prior detached retirements.
const RETIREMENT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Bound for a new managed start to wait for a prior detached retirement.
const RETIREMENT_WAIT_TIMEOUT: Duration = Duration::from_secs(9);

/// Largest integer exactly representable as a JavaScript `Number`. Byte counts
/// and timestamps from Surge are signed `int64`; values outside this range are
/// clamped so they survive the JSON/JS boundary without precision loss.
const JS_MAX_SAFE_INTEGER: i64 = MAX_RATE_LIMIT_BPS;

/// Upper bound for a download id accepted by the location command.
const MAX_DOWNLOAD_ID_BYTES: usize = 4 * 1024;

/// Sanitized queue row sent to the frontend.
///
/// Only the download id, display filename, normalized status, sanitized
/// lifecycle state, and numeric byte/speed metrics leave the process. The
/// Surge `url` (may carry signed secrets), `dest_path` (private filesystem
/// layout), `error` (server text), and `hook_error` (may contain a shell
/// command or secret) are never projected. Field names cross the IPC boundary
/// as camelCase.
#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueItemDto {
    pub id: String,
    pub name: String,
    pub status: String,
    /// Sanitized daemon phase (`queued`, `retrying`, `downloading`,
    /// `verifying`, `finalizing`, or `unknown`); falls back to the normalized
    /// status for older daemons that do not emit `phase`.
    pub phase: String,
    /// A post-download hook is running.
    pub processing: bool,
    /// A post-download hook has finished (successfully or not).
    pub processed: bool,
    /// The hook failed. Only this bit is exposed; its error text is not.
    pub hook_failed: bool,
    pub bytes_done: i64,
    pub bytes_total: i64,
    pub speed_bps: f64,
    pub added_at: i64,
}

impl fmt::Debug for QueueItemDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // id and name can be user-controlled and status/phase are
        // server-controlled, so only booleans and numeric metrics are safe.
        f.debug_struct("QueueItemDto")
            .field("processing", &self.processing)
            .field("processed", &self.processed)
            .field("hook_failed", &self.hook_failed)
            .field("bytes_done", &self.bytes_done)
            .field("bytes_total", &self.bytes_total)
            .field("speed_bps", &self.speed_bps)
            .field("added_at", &self.added_at)
            .finish()
    }
}

/// Authoritative `/list` snapshot plus the stream cursor it corresponds to.
///
/// `stream_seq` is the progress-stream receipt sequence captured immediately
/// before the `/list` request starts. Events received at or before the cursor
/// are reflected by the authoritative snapshot; later events carry a higher
/// receipt sequence and are merged per row (byte-monotonically) so a stale
/// snapshot can never overwrite newer progress and a queued pre-read sample
/// can never overwrite the newer list.
#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSnapshotDto {
    pub epoch: u64,
    pub stream_seq: u64,
    pub items: Vec<QueueItemDto>,
}

impl fmt::Debug for QueueSnapshotDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueSnapshotDto")
            .field("epoch", &self.epoch)
            .field("stream_seq", &self.stream_seq)
            .field("item_count", &self.items.len())
            .finish()
    }
}

/// App-wide automatic-hash preference. The first-run value is `false`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutomaticHashSettingsDto {
    pub default_enabled: bool,
}

/// One UI-supplied hash binding for a positively acknowledged download id.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HashIntentInput {
    pub id: String,
    pub enabled: bool,
}

/// Result of one `POST /download` with the confirmed queued id preserved.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddReceiptDto {
    pub status: AddOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// `Some(true)` only when a durable hash intent was confirmed written for
    /// the accepted queued id. `Some(false)` means the daemon accepted the add
    /// but the intent write failed, so the file will not be auto-hashed unless
    /// the UI explicitly rebinds the id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_recorded: Option<bool>,
}

impl fmt::Debug for AddReceiptDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AddReceiptDto")
            .field("status", &self.status)
            .field("has_id", &self.id.is_some())
            .finish()
    }
}

/// Bridge failure surfaced to the frontend as a stable, sanitized code.
///
/// No variant carries the endpoint, bearer token, response body, filename, or
/// error text, so `Serialize` and `Debug` are both safe to log or return.
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BridgeError {
    InvalidEndpoint,
    EmptyToken,
    Unauthorized,
    Network,
    Timeout,
    BodyTooLarge,
    InvalidResponse,
    RateUnconfirmed,
    SettingsConflict,
    SettingsUnconfirmed,
    BulkUnconfirmed,
    NotConnected,
    Superseded,
    State,
    HashUnconfirmed,
    InvalidDownloadId,
    DownloadNotFound,
    DownloadAmbiguous,
    DownloadNotCompleted,
    DownloadMoving,
    DownloadFileMissing,
    ManagedConnectionRequired,
    LocationUnavailable,
    LocationUnsupported,
    LocationLaunchFailed,
}

impl fmt::Debug for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::InvalidEndpoint => "InvalidEndpoint",
            Self::EmptyToken => "EmptyToken",
            Self::Unauthorized => "Unauthorized",
            Self::Network => "Network",
            Self::Timeout => "Timeout",
            Self::BodyTooLarge => "BodyTooLarge",
            Self::InvalidResponse => "InvalidResponse",
            Self::RateUnconfirmed => "RateUnconfirmed",
            Self::SettingsConflict => "SettingsConflict",
            Self::SettingsUnconfirmed => "SettingsUnconfirmed",
            Self::BulkUnconfirmed => "BulkUnconfirmed",
            Self::NotConnected => "NotConnected",
            Self::Superseded => "Superseded",
            Self::State => "State",
            Self::HashUnconfirmed => "HashUnconfirmed",
            Self::InvalidDownloadId => "InvalidDownloadId",
            Self::DownloadNotFound => "DownloadNotFound",
            Self::DownloadAmbiguous => "DownloadAmbiguous",
            Self::DownloadNotCompleted => "DownloadNotCompleted",
            Self::DownloadMoving => "DownloadMoving",
            Self::DownloadFileMissing => "DownloadFileMissing",
            Self::ManagedConnectionRequired => "ManagedConnectionRequired",
            Self::LocationUnavailable => "LocationUnavailable",
            Self::LocationUnsupported => "LocationUnsupported",
            Self::LocationLaunchFailed => "LocationLaunchFailed",
        };
        f.write_str(code)
    }
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidEndpoint => "invalid Surge endpoint",
            Self::EmptyToken => "missing bearer token",
            Self::Unauthorized => "unauthorized: invalid bearer token",
            Self::Network => "connection failed",
            Self::Timeout => "connection timed out",
            Self::BodyTooLarge => "response body too large",
            Self::InvalidResponse => "invalid response from server",
            Self::RateUnconfirmed => "rate limit state could not be confirmed",
            Self::SettingsConflict => "settings changed since editing began",
            Self::SettingsUnconfirmed => "settings change could not be confirmed",
            Self::BulkUnconfirmed => "bulk queue action could not be confirmed",
            Self::NotConnected => "not connected to a Surge instance",
            Self::Superseded => "connection attempt was superseded",
            Self::State => "connection state unavailable",
            Self::HashUnconfirmed => "automatic hash state could not be confirmed",
            Self::InvalidDownloadId => "invalid download id",
            Self::DownloadNotFound => "download not found",
            Self::DownloadAmbiguous => "download id is ambiguous",
            Self::DownloadNotCompleted => "download is not completed",
            Self::DownloadMoving => "download file is moving",
            Self::DownloadFileMissing => "download file is missing",
            Self::ManagedConnectionRequired => "managed connection required",
            Self::LocationUnavailable => "download location is unavailable",
            Self::LocationUnsupported => "download location is unsupported",
            Self::LocationLaunchFailed => "download location could not be opened",
        };
        f.write_str(message)
    }
}

impl std::error::Error for BridgeError {}

/// Sanitized failure for the managed-daemon initialization command.
///
/// No variant carries a path, resource name, process id, environment value,
/// certificate, token, OS error text, or underlying library message. The
/// serialized code is the entire payload that may cross the IPC boundary.
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ManagedError {
    UnsupportedPlatform,
    Resource,
    Paths,
    InvalidInput,
    Identity,
    Process,
    Readiness,
    AlreadyStarted,
    Superseded,
    Retry,
    Startup,
    State,
    ShuttingDown,
}

impl fmt::Debug for ManagedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl fmt::Display for ManagedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::UnsupportedPlatform => "managed daemon unavailable on this platform",
            Self::Resource => "managed daemon resource unavailable",
            Self::Paths => "managed daemon state path unavailable",
            Self::InvalidInput => "managed daemon invalid input",
            Self::Identity => "managed daemon identity stage failed",
            Self::Process => "managed daemon process stage failed",
            Self::Readiness => "managed daemon readiness stage failed",
            Self::AlreadyStarted => "managed daemon already started",
            Self::Superseded => "managed daemon start was superseded",
            Self::Retry => "managed daemon is retiring; retry the start",
            Self::Startup => "managed daemon startup failed",
            Self::State => "managed daemon state unavailable",
            Self::ShuttingDown => "managed daemon is shutting down",
        };
        f.write_str(message)
    }
}

impl ManagedError {
    fn code(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "UnsupportedPlatform",
            Self::Resource => "Resource",
            Self::Paths => "Paths",
            Self::InvalidInput => "InvalidInput",
            Self::Identity => "Identity",
            Self::Process => "Process",
            Self::Readiness => "Readiness",
            Self::AlreadyStarted => "AlreadyStarted",
            Self::Superseded => "Superseded",
            Self::Retry => "Retry",
            Self::Startup => "Startup",
            Self::State => "State",
            Self::ShuttingDown => "ShuttingDown",
        }
    }
}

impl std::error::Error for ManagedError {}

impl From<crate::managed_server::ManagedStartupStage> for ManagedError {
    fn from(stage: crate::managed_server::ManagedStartupStage) -> Self {
        use crate::managed_server::ManagedStartupStage;
        match stage {
            ManagedStartupStage::InvalidInput => Self::InvalidInput,
            ManagedStartupStage::UnsupportedPlatform => Self::UnsupportedPlatform,
            ManagedStartupStage::Paths => Self::Paths,
            ManagedStartupStage::Identity => Self::Identity,
            ManagedStartupStage::Process => Self::Process,
            ManagedStartupStage::Readiness => Self::Readiness,
        }
    }
}

impl From<LocationError> for BridgeError {
    fn from(err: LocationError) -> Self {
        // LocationError never carries path or OS detail; map only the stable
        // class so no private filesystem information can cross IPC or logs.
        match err {
            LocationError::Missing => Self::DownloadFileMissing,
            #[cfg(not(any(target_os = "linux", windows)))]
            LocationError::UnsupportedPlatform => Self::LocationUnsupported,
            LocationError::LaunchFailed => Self::LocationLaunchFailed,
            LocationError::InvalidPath
            | LocationError::NonLocal
            | LocationError::Symlink
            | LocationError::Unavailable => Self::LocationUnavailable,
        }
    }
}

impl From<ConnectError> for BridgeError {
    fn from(err: ConnectError) -> Self {
        // Map known codes only; the `InvalidEndpoint` reason string is dropped
        // so no raw input can ride along with the error.
        match err {
            ConnectError::InvalidEndpoint(_) => Self::InvalidEndpoint,
            ConnectError::EmptyToken => Self::EmptyToken,
            ConnectError::Unauthorized => Self::Unauthorized,
            ConnectError::Network => Self::Network,
            ConnectError::Timeout => Self::Timeout,
            ConnectError::BodyTooLarge => Self::BodyTooLarge,
            ConnectError::InvalidResponse => Self::InvalidResponse,
            ConnectError::RateUnconfirmed => Self::RateUnconfirmed,
            ConnectError::SettingsConflict => Self::SettingsConflict,
            ConnectError::SettingsUnconfirmed => Self::SettingsUnconfirmed,
            ConnectError::BulkUnconfirmed => Self::BulkUnconfirmed,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum ActiveConnection {
    #[default]
    None,
    Manual,
    Managed,
}

/// One managed-start claim. The generation fences lifecycle attempts; the
/// epoch is the connection epoch reserved when the claim was made.
#[derive(Clone, Copy, Debug)]
struct ManagedStartTicket {
    generation: u64,
    epoch: u64,
}

/// Tracks detached lifecycle work (start, retirement) so app shutdown can wait
/// for it without holding the connection-state mutex across network or awaits.
#[derive(Default)]
struct ManagedLifecycle {
    state: Mutex<LifecycleState>,
    drained: Condvar,
    cancel: Arc<AtomicBool>,
}

#[derive(Default)]
struct LifecycleState {
    active: usize,
    shutting_down: bool,
}

impl ManagedLifecycle {
    fn lock(&self) -> std::sync::MutexGuard<'_, LifecycleState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn begin_operation(self: &Arc<Self>) -> Option<LifecycleOperation> {
        let mut state = self.lock();
        if state.shutting_down {
            return None;
        }
        state.active = state.active.saturating_add(1);
        Some(LifecycleOperation {
            lifecycle: Arc::clone(self),
        })
    }

    /// Cleanup that must still be drained when app shutdown is already in
    /// progress. Never rejected by the shutdown fence.
    fn begin_cleanup(self: &Arc<Self>) -> LifecycleOperation {
        let mut state = self.lock();
        state.active = state.active.saturating_add(1);
        LifecycleOperation {
            lifecycle: Arc::clone(self),
        }
    }

    fn begin_shutdown(&self) {
        self.cancel.store(true, Ordering::SeqCst);
        let mut state = self.lock();
        state.shutting_down = true;
        self.drained.notify_all();
    }

    fn is_shutting_down(&self) -> bool {
        self.lock().shutting_down
    }

    fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancel)
    }

    fn wait_for_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        while state.active > 0 {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let waited = self.drained.wait_timeout(state, deadline - now);
            state = match waited {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        state.active == 0
    }
}

struct LifecycleOperation {
    lifecycle: Arc<ManagedLifecycle>,
}

impl LifecycleOperation {
    fn is_shutting_down(&self) -> bool {
        self.lifecycle.is_shutting_down()
    }
}

impl Drop for LifecycleOperation {
    fn drop(&mut self) {
        let mut state = self.lifecycle.lock();
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.lifecycle.drained.notify_all();
        }
    }
}

fn finish_retirement_state(state: &Arc<Mutex<ConnectionState>>) {
    let mut state = match state.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    };
    state.finish_retirement();
}

/// Wait for the real owner thread and only then clear the retirement fence and
/// drain operation. If the owner reports a failed/unconfirmed reap, both stay
/// outstanding. Registration is completion-driven rather than tied to a
/// watcher timeout, so a late confirmed reap still clears the fence and drain.
fn spawn_owner_cleanup(
    state: Arc<Mutex<ConnectionState>>,
    owner: OwnerLifetime,
    operation: LifecycleOperation,
    finish_retirement: bool,
) {
    owner.on_finish(move |reaped| {
        if reaped {
            if finish_retirement {
                finish_retirement_state(&state);
            }
            drop(operation);
        } else {
            // Never claim success for an unconfirmed/failed reap. Keep both
            // the retirement fence and lifecycle operation outstanding.
            std::mem::forget(operation);
        }
    });
}

/// Keeps a detached retirement visible to new managed starts until the owned
/// server's real owner thread has finished reaping the child.
struct RetirementGuard {
    state: Arc<Mutex<ConnectionState>>,
    lifecycle: Arc<ManagedLifecycle>,
    owner: Option<OwnerLifetime>,
    armed: bool,
}

impl Drop for RetirementGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        match self.owner.take() {
            None => finish_retirement_state(&self.state),
            Some(owner) => {
                let operation = self.lifecycle.begin_cleanup();
                spawn_owner_cleanup(Arc::clone(&self.state), owner, operation, true);
            }
        }
    }
}

/// Serializes every global-rate mutation for this app lifetime.
///
/// The async gate is acquired before the HTTP operation starts and moved into
/// a managed background task, so dropping the Tauri command future cannot
/// release it while a write is in flight. `poisoned` is deliberately never
/// cleared by disconnect/reconnect or daemon retirement: an ambiguous write
/// may still land later, so only a process restart (which also retires the
/// daemon) may re-enable writes.
#[derive(Default)]
struct RateWriter {
    gate: Arc<tokio::sync::Mutex<()>>,
    poisoned: AtomicBool,
}

/// Atomically captured managed writer target. The epoch and pid must still
/// match when the operation finishes (success or failure) before any result is
/// allowed to surface as current.
#[derive(Clone)]
struct ManagedRateSnapshot {
    client: Arc<SurgeClient>,
    epoch: u64,
    pid: u32,
}

/// Fail-closed guard for one in-flight rate write.
///
/// Disarmed only for a definitive result (confirmed success or a definitive
/// pre-request/local rejection). Any panic, abort, or ambiguous outcome drops
/// the guard while armed and permanently poisons the writer.
struct RateWritePoison {
    writer: Arc<RateWriter>,
    armed: bool,
}

impl RateWritePoison {
    fn new(writer: Arc<RateWriter>) -> Self {
        Self {
            writer,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn poison_now(&mut self) {
        self.writer.poisoned.store(true, Ordering::SeqCst);
        self.armed = false;
    }
}

impl Drop for RateWritePoison {
    fn drop(&mut self) {
        if self.armed {
            self.writer.poisoned.store(true, Ordering::SeqCst);
        }
    }
}

/// Serializes daemon settings mutations for this app lifetime.
///
/// Kept separate from [`RateWriter`] so an ambiguous rate write never blocks
/// or poisons the concurrency control (and vice versa). `poisoned` is only set
/// by an ambiguous concurrency write and is never cleared by reconnect or
/// retirement; only an app restart starts fresh.
#[derive(Default)]
struct SettingsWriteGate {
    gate: Arc<tokio::sync::Mutex<()>>,
    poisoned: AtomicBool,
}

/// Fail-closed guard for one in-flight settings write. Disarmed only for a
/// definitive result (confirmed success or a definite pre-request/rejection).
struct SettingsWritePoison {
    writer: Arc<SettingsWriteGate>,
    armed: bool,
}

impl SettingsWritePoison {
    fn new(writer: Arc<SettingsWriteGate>) -> Self {
        Self {
            writer,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn poison_now(&mut self) {
        self.writer.poisoned.store(true, Ordering::SeqCst);
        self.armed = false;
    }
}

impl Drop for SettingsWritePoison {
    fn drop(&mut self) {
        if self.armed {
            self.writer.poisoned.store(true, Ordering::SeqCst);
        }
    }
}

/// Serializes synchronous location validation for this manager.
///
/// The permit is acquired before dispatch and moved into the blocking closure,
/// so a cancelled caller cannot release it while the filesystem walk is still
/// running. A busy permit fails immediately rather than dispatching another
/// blocking worker, which bounds worker use when a filesystem call hangs.
#[derive(Default)]
struct LocationValidationGate {
    busy: AtomicBool,
}

struct LocationValidationPermit {
    gate: Arc<LocationValidationGate>,
}

impl LocationValidationGate {
    fn try_acquire(self: &Arc<Self>) -> Option<LocationValidationPermit> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| LocationValidationPermit {
                gate: Arc::clone(self),
            })
    }
}

impl Drop for LocationValidationPermit {
    fn drop(&mut self) {
        self.gate.busy.store(false, Ordering::Release);
    }
}

/// Serializes the actually blocking file-manager launch for this manager.
///
/// The permit is acquired with the final shutdown/ownership reservation and
/// moved into the blocking worker, so a cancelled caller cannot release it
/// while a filesystem or process creation call is still running. A busy permit
/// rejects immediately instead of dispatching another blocking worker.
#[derive(Default)]
struct LocationLaunchGate {
    busy: AtomicBool,
}

struct LocationLaunchPermit {
    gate: Arc<LocationLaunchGate>,
}

struct LocationLaunchReservation {
    _permit: LocationLaunchPermit,
}

impl LocationLaunchGate {
    fn try_acquire(self: &Arc<Self>) -> Option<LocationLaunchPermit> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| LocationLaunchPermit {
                gate: Arc::clone(self),
            })
    }
}

impl Drop for LocationLaunchPermit {
    fn drop(&mut self) {
        self.gate.busy.store(false, Ordering::Release);
    }
}

const AUTOMATIC_HASH_FILE: &str = crate::hash_persistence::HASH_STATE_FILE;

fn new_hash_connection_id() -> String {
    let mut raw = [0u8; 16];
    if getrandom::fill(&mut raw).is_ok() {
        let mut out = String::with_capacity(32);
        for byte in raw {
            out.push_str(&format!("{byte:02x}"));
        }
        return out;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("fallback-{:x}-{:x}", now.as_nanos(), std::process::id())
}

struct HashSlot {
    path: Option<PathBuf>,
    loaded: bool,
    namespace: Option<String>,
    store: HashStore,
}

impl Default for HashSlot {
    fn default() -> Self {
        Self {
            path: None,
            loaded: false,
            namespace: None,
            store: HashStore::load(None),
        }
    }
}

#[derive(Default)]
struct HashCoordinator {
    slot: Mutex<HashSlot>,
    worker: Mutex<HashWorkerState>,
    /// Serializes lifecycle transitions against hash-write validation and
    /// publication.
    ///
    /// Lock order is lifecycle gate -> connection state -> hash slot. A
    /// transition holds the gate while it advances the epoch and fence. A
    /// write holds it only while validating and while publishing an
    /// already-synced temporary file, never across durable storage sync. That
    /// makes the (epoch, fence) pair change atomically without letting a hung
    /// filesystem block shutdown.
    lifecycle_gate: Mutex<()>,
    /// Serializes hash-state mutations across their off-lock persistence
    /// phase. Lifecycle transitions never take this lock, so a hung filesystem
    /// cannot prevent shutdown from fencing the connection.
    write_gate: Mutex<()>,
    /// Set as soon as app shutdown begins, before the lifecycle gate is
    /// acquired. A ticketed write must not publish even while shutdown is
    /// still waiting for an in-flight transition.
    shutting_down: AtomicBool,
    /// Serializes opening/replacing the isolated persistence owner. It never
    /// spans a lifecycle, state, or slot lock.
    open_gate: Mutex<()>,
    /// Serializes the final domain installation against the shutdown fence.
    /// It is never held across opening the owner.
    install_gate: Mutex<()>,
    /// Set under `install_gate` before shutdown releases it. An initialization
    /// that already passed its final check can never install afterwards.
    installation_closed: AtomicBool,
    /// The single owner for the current managed state directory. Once it is
    /// quarantined, no replacement is started in this process: an unresolved
    /// rename must never be hidden behind a new writer.
    domain: Mutex<Option<Arc<crate::hash_persistence::PersistenceDomain>>>,
    /// Last terminal initialization failure, recorded so managed readers can
    /// surface it without reopening the same unresolved directory.
    initialization_failure: Mutex<Option<BridgeError>>,
    #[cfg(test)]
    persistence_fault: Mutex<Option<crate::hash_persistence::FaultConfig>>,
    #[cfg(test)]
    hash_write_pause: VerifiedPause,
    #[cfg(test)]
    hash_commit_pause: VerifiedPause,
}

/// Atomic connection/hash lifecycle fence for one hash-state mutation.
///
/// Captured under both the connection-state lock and the hash slot lock, then
/// revalidated under the same ordering immediately before the mutation. Every
/// connection transition increments both the epoch and the hash fence before
/// releasing the state lock, so a captured ticket can never authorize a write
/// after a newer lifecycle has taken over.
#[derive(Clone, Debug, PartialEq, Eq)]
struct HashWriteTicket {
    epoch: u64,
    binding: crate::hash_state::DaemonBinding,
    fence: u64,
    namespace: Option<String>,
}

#[derive(Default)]
struct HashWorkerState {
    running: bool,
    dirty: bool,
    token: Option<Arc<AtomicBool>>,
}

impl HashCoordinator {
    fn lock_slot(&self) -> std::sync::MutexGuard<'_, HashSlot> {
        match self.slot.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_worker(&self) -> std::sync::MutexGuard<'_, HashWorkerState> {
        match self.worker.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_lifecycle(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.lifecycle_gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_write(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.write_gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_open(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.open_gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_install(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.install_gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn domain(&self) -> Option<Arc<crate::hash_persistence::PersistenceDomain>> {
        match self.domain.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn persistence_confirmed(&self) -> bool {
        self.domain().is_some_and(|domain| domain.is_confirmed())
    }

    fn initialization_failure(&self) -> Option<BridgeError> {
        match self.initialization_failure.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    fn set_initialization_failure(&self, failure: Option<BridgeError>) {
        let mut guard = match self.initialization_failure.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = failure;
    }

    fn abandon_domain(&self) {
        if let Some(domain) = self.domain() {
            domain.abandon_unconfirmed();
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    fn set_persistence_fault(&self, fault: Option<crate::hash_persistence::FaultConfig>) {
        let mut guard = match self.persistence_fault.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = fault;
    }

    fn begin_shutdown(&self) {
        {
            // Publish the shutdown fence under the same lock used to install a
            // newly opened owner. Initialization cannot cross this point and
            // then install afterwards.
            let _install = self.lock_install();
            self.shutting_down.store(true, Ordering::SeqCst);
            self.installation_closed.store(true, Ordering::SeqCst);
        }
        // Bounded owner shutdown: never waits on a filesystem syscall, and an
        // unresolved owner stays locked/quarantined instead of being replaced.
        if let Some(domain) = self.domain() {
            domain.shutdown();
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    fn ensure_loaded(&self, directory: &Path) -> Result<(), BridgeError> {
        // An unresolved pending marker/quarantined owner can never be
        // confirmed in this process. Keep the recorded failure observable and
        // avoid opening the same directory again on every reader.
        if self.initialization_failure() == Some(BridgeError::HashUnconfirmed) {
            return Err(BridgeError::HashUnconfirmed);
        }
        let result = self.ensure_loaded_inner(directory);
        match result {
            Ok(()) => {
                self.set_initialization_failure(None);
                Ok(())
            }
            Err(error) => {
                if error != BridgeError::Superseded {
                    self.set_initialization_failure(Some(error));
                }
                Err(error)
            }
        }
    }

    fn ensure_loaded_inner(&self, directory: &Path) -> Result<(), BridgeError> {
        let path = directory.join(AUTOMATIC_HASH_FILE);
        let _open = self.lock_open();
        if self.is_shutting_down() {
            return Err(BridgeError::Superseded);
        }
        {
            let slot = self.lock_slot();
            if let Some(loaded_path) = slot.path.as_ref() {
                if loaded_path == &path {
                    return if self.persistence_confirmed() {
                        Ok(())
                    } else {
                        Err(BridgeError::HashUnconfirmed)
                    };
                }
                // A coordinator owns one state directory. Refuse to silently
                // forget the old owner (which may still be unresolved).
                return Err(BridgeError::State);
            }
        }
        if self.domain().is_some() {
            return Err(BridgeError::State);
        }
        #[cfg(test)]
        let fault = match self.persistence_fault.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        #[cfg(not(test))]
        let fault = None;
        let (domain, open) = crate::hash_persistence::PersistenceDomain::open(directory, fault)
            .map_err(|error| match error {
                crate::hash_persistence::PersistenceError::Unresolved => {
                    BridgeError::HashUnconfirmed
                }
                crate::hash_persistence::PersistenceError::Busy
                | crate::hash_persistence::PersistenceError::Malformed
                | crate::hash_persistence::PersistenceError::Unavailable => BridgeError::State,
            })?;
        if self.is_shutting_down() {
            domain.shutdown();
            return Err(BridgeError::Superseded);
        }
        let namespace = open.namespace.clone();
        let mut store = match open.bytes.as_deref() {
            Some(bytes) => match HashStore::from_bytes(None, bytes) {
                Ok(store) => store,
                Err(_) => {
                    domain.shutdown();
                    return Err(BridgeError::State);
                }
            },
            None => HashStore::load(None),
        };
        store.align_revision(open.revision);
        // Records from a different (or missing/malformed) namespace can never
        // be proven to belong to this owned queue, so fail closed before any
        // result can be served or resumed. This mutation is in memory only;
        // the staged bytes are published without any lock held.
        let base = store.revision();
        let _ = store.invalidate_unprovable(namespace.as_deref());
        if store.revision() != base {
            if self.is_shutting_down() {
                domain.shutdown();
                return Err(BridgeError::Superseded);
            }
            let staged = match store.stage_write() {
                Ok(staged) => staged,
                Err(_) => {
                    domain.shutdown();
                    return Err(BridgeError::State);
                }
            };
            let prepared = match domain.prepare(base, staged.bytes().to_vec()) {
                Ok(crate::hash_persistence::PrepareOutcome::Prepared(prepared)) => prepared,
                Ok(crate::hash_persistence::PrepareOutcome::BeforeRename) => {
                    domain.shutdown();
                    return Err(if self.is_shutting_down() {
                        BridgeError::Superseded
                    } else {
                        BridgeError::State
                    });
                }
                Ok(crate::hash_persistence::PrepareOutcome::Conflict) => {
                    domain.shutdown();
                    return Err(BridgeError::Superseded);
                }
                Ok(crate::hash_persistence::PrepareOutcome::ExternalChange)
                | Ok(crate::hash_persistence::PrepareOutcome::Unconfirmed)
                | Err(crate::hash_persistence::PersistenceError::Unavailable)
                | Err(crate::hash_persistence::PersistenceError::Unresolved) => {
                    domain.shutdown();
                    return Err(if self.is_shutting_down() {
                        BridgeError::Superseded
                    } else {
                        BridgeError::HashUnconfirmed
                    });
                }
                Err(crate::hash_persistence::PersistenceError::Busy)
                | Err(crate::hash_persistence::PersistenceError::Malformed) => {
                    domain.shutdown();
                    return Err(if self.is_shutting_down() {
                        BridgeError::Superseded
                    } else {
                        BridgeError::State
                    });
                }
            };
            if self.is_shutting_down() {
                prepared.abort();
                return Err(BridgeError::Superseded);
            }
            match prepared.confirm() {
                Ok(crate::hash_persistence::CommitOutcome::Committed(revision))
                    if !self.is_shutting_down() =>
                {
                    staged.install(&mut store, revision);
                }
                Ok(crate::hash_persistence::CommitOutcome::BeforeRename) => {
                    domain.shutdown();
                    return Err(if self.is_shutting_down() {
                        BridgeError::Superseded
                    } else {
                        BridgeError::State
                    });
                }
                Ok(crate::hash_persistence::CommitOutcome::Conflict) => {
                    domain.shutdown();
                    return Err(BridgeError::Superseded);
                }
                Ok(crate::hash_persistence::CommitOutcome::Committed(_))
                | Ok(crate::hash_persistence::CommitOutcome::ExternalChange)
                | Ok(crate::hash_persistence::CommitOutcome::Unconfirmed)
                | Err(crate::hash_persistence::PersistenceError::Unavailable)
                | Err(crate::hash_persistence::PersistenceError::Unresolved) => {
                    domain.shutdown();
                    return Err(if self.is_shutting_down() {
                        BridgeError::Superseded
                    } else {
                        BridgeError::HashUnconfirmed
                    });
                }
                Err(crate::hash_persistence::PersistenceError::Busy)
                | Err(crate::hash_persistence::PersistenceError::Malformed) => {
                    domain.shutdown();
                    return Err(if self.is_shutting_down() {
                        BridgeError::Superseded
                    } else {
                        BridgeError::State
                    });
                }
            }
        }
        // Final installation is serialized with `begin_shutdown`, which sets
        // both flags under the same lock. An owner that started opening before
        // shutdown can never cross this point and install afterwards.
        let _install = self.lock_install();
        if self.is_shutting_down() || self.installation_closed.load(Ordering::SeqCst) {
            drop(_install);
            domain.shutdown();
            return Err(BridgeError::Superseded);
        }
        let mut slot = self.lock_slot();
        let fence = slot.store.fence().wrapping_add(1);
        store.set_fence(fence);
        slot.namespace = namespace;
        slot.store = store;
        slot.path = Some(path);
        slot.loaded = true;
        let mut domain_guard = match self.domain.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *domain_guard = Some(domain);
        Ok(())
    }

    fn namespace(&self) -> Option<String> {
        let slot = self.lock_slot();
        slot.namespace.clone()
    }

    fn fence(&self) -> u64 {
        let slot = self.lock_slot();
        slot.store.fence()
    }

    fn default_enabled(&self) -> Result<bool, BridgeError> {
        if !self.persistence_confirmed() {
            return Err(BridgeError::HashUnconfirmed);
        }
        let slot = self.lock_slot();
        if !slot.loaded {
            return Err(BridgeError::State);
        }
        Ok(slot.store.default_enabled())
    }

    fn set_default_enabled(&self, enabled: bool) -> Result<bool, BridgeError> {
        let _write = self.lock_write();
        let (mut fork, base) = {
            let slot = self.lock_slot();
            if !slot.loaded {
                return Err(BridgeError::State);
            }
            (slot.store.fork_for_staging(), slot.store.revision())
        };
        fork.set_default_enabled(enabled)
            .map_err(|_| BridgeError::State)?;
        let staged = fork.stage_write().map_err(|_| BridgeError::State)?;
        let revision = self.commit_staged(base, staged)?;
        let slot = self.lock_slot();
        if slot.store.revision() != revision {
            self.abandon_domain();
            return Err(BridgeError::HashUnconfirmed);
        }
        Ok(slot.store.default_enabled())
    }

    /// Publish an already-serialized in-memory mutation through the isolated
    /// owner and install it only if the owner confirms and no other writer
    /// changed the slot while the commit was in flight.
    fn commit_staged(
        &self,
        base: u64,
        staged: crate::hash_state::StagedHashWrite,
    ) -> Result<u64, BridgeError> {
        let Some(domain) = self.domain() else {
            return Err(BridgeError::State);
        };
        let prepared = match domain.prepare(base, staged.bytes().to_vec()) {
            Ok(crate::hash_persistence::PrepareOutcome::Prepared(prepared)) => prepared,
            Ok(crate::hash_persistence::PrepareOutcome::BeforeRename) => {
                return Err(BridgeError::State);
            }
            Ok(crate::hash_persistence::PrepareOutcome::Conflict) => {
                self.abandon_domain();
                return Err(BridgeError::Superseded);
            }
            Ok(crate::hash_persistence::PrepareOutcome::ExternalChange)
            | Ok(crate::hash_persistence::PrepareOutcome::Unconfirmed)
            | Err(_) => {
                return Err(BridgeError::HashUnconfirmed);
            }
        };
        if self.is_shutting_down() || !domain.is_confirmed() {
            prepared.abort();
            return Err(if self.is_shutting_down() {
                BridgeError::Superseded
            } else {
                BridgeError::HashUnconfirmed
            });
        }
        let revision = match prepared.confirm() {
            Ok(crate::hash_persistence::CommitOutcome::Committed(revision))
                if !self.is_shutting_down() && domain.is_confirmed() =>
            {
                revision
            }
            Ok(crate::hash_persistence::CommitOutcome::Committed(_)) => {
                return Err(if self.is_shutting_down() {
                    BridgeError::Superseded
                } else {
                    BridgeError::HashUnconfirmed
                });
            }
            Ok(_) | Err(_) => {
                return Err(BridgeError::HashUnconfirmed);
            }
        };
        let mut slot = self.lock_slot();
        if slot.store.revision() != base {
            drop(slot);
            self.abandon_domain();
            return Err(BridgeError::Superseded);
        }
        staged.install(&mut slot.store, revision);
        Ok(revision)
    }

    /// Run a non-ticketed in-memory mutation through the isolated owner. Used
    /// for state that is not tied to a managed connection (the app-wide
    /// default) and by test-only completion seeding.
    fn mutate_persisted<R>(
        &self,
        mutate: impl FnOnce(&mut HashStore) -> Result<R, BridgeError>,
    ) -> Result<R, BridgeError> {
        let _write = self.lock_write();
        let (mut fork, base) = {
            let slot = self.lock_slot();
            if !slot.loaded {
                return Err(BridgeError::State);
            }
            (slot.store.fork_for_staging(), slot.store.revision())
        };
        let result = mutate(&mut fork)?;
        if fork.revision() != base {
            let staged = fork.stage_write().map_err(|_| BridgeError::State)?;
            self.commit_staged(base, staged)?;
        }
        Ok(result)
    }

    #[cfg(all(test, target_os = "linux"))]
    fn record_intents(&self, intents: &[HashIntent]) -> Result<(), BridgeError> {
        self.record_intents_with(intents, &[]).map(|_| ())
    }

    #[cfg(all(test, target_os = "linux"))]
    fn record_intents_with(
        &self,
        intents: &[HashIntent],
        provenance: &[(String, crate::hash_state::HashProvenance)],
    ) -> Result<crate::hash_state::ReconcileCohort, BridgeError> {
        self.mutate_persisted(|store| {
            store
                .record_intents_with(intents, provenance)
                .map_err(|_| BridgeError::State)
        })
    }

    fn visible(&self, namespace: Option<&str>) -> Vec<DownloadHashDto> {
        let slot = self.lock_slot();
        namespace
            .map(|value| slot.store.visible(value))
            .unwrap_or_default()
    }

    fn visible_unconfirmed(&self, namespace: Option<&str>) -> Vec<DownloadHashDto> {
        let slot = self.lock_slot();
        slot.store.visible_unconfirmed(namespace)
    }

    fn candidates(&self, namespace: &str) -> Vec<crate::hash_state::HashCandidate> {
        let slot = self.lock_slot();
        slot.store.candidates(namespace)
    }

    fn reconcile_cohort(&self) -> crate::hash_state::ReconcileCohort {
        let slot = self.lock_slot();
        slot.store.reconcile_cohort()
    }

    fn completed_for(&self, namespace: &str) -> Vec<crate::hash_state::CompletedHash> {
        let slot = self.lock_slot();
        slot.store.completed_for(namespace)
    }

    fn visible_verified(
        &self,
        namespace: &str,
        verified: &[crate::hash_state::CompletedHash],
        fence: u64,
    ) -> Option<Vec<DownloadHashDto>> {
        let slot = self.lock_slot();
        slot.store.visible_verified(namespace, verified, fence)
    }

    #[allow(dead_code)]
    fn mark_complete(
        &self,
        id: &str,
        generation: u64,
        fence: u64,
        completed: crate::hash_state::HashCompleted,
    ) -> Result<bool, BridgeError> {
        self.mutate_persisted(|store| {
            store
                .mark_complete(id, generation, fence, completed)
                .map_err(|_| BridgeError::State)
        })
    }

    #[allow(dead_code)]
    fn mark_failed(&self, id: &str, generation: u64, fence: u64) -> Result<bool, BridgeError> {
        self.mutate_persisted(|store| {
            store
                .mark_failed(id, generation, fence)
                .map_err(|_| BridgeError::State)
        })
    }

    #[allow(dead_code)]
    fn mark_unavailable(&self, id: &str, generation: u64, fence: u64) -> Result<bool, BridgeError> {
        self.mutate_persisted(|store| {
            store
                .mark_unavailable(id, generation, fence)
                .map_err(|_| BridgeError::State)
        })
    }

    #[allow(dead_code)]
    fn mark_stale(&self, id: &str, generation: u64, fence: u64) -> Result<bool, BridgeError> {
        self.mutate_persisted(|store| {
            store
                .mark_stale(id, generation, fence)
                .map_err(|_| BridgeError::State)
        })
    }

    fn invalidate_completed(
        &self,
        expected: &crate::hash_state::CompletedHash,
        fence: u64,
    ) -> Result<bool, BridgeError> {
        self.mutate_persisted(|store| {
            store
                .invalidate_completed(expected, fence)
                .map_err(|_| BridgeError::State)
        })
    }

    /// Signal the current worker without changing the lifecycle fence. The
    /// epoch and fence are advanced together by the owning transition under
    /// the lifecycle gate and connection-state lock.
    fn cancel_worker(&self) {
        let worker = self.lock_worker();
        if let Some(token) = worker.token.as_ref() {
            token.store(true, Ordering::SeqCst);
        }
    }

    /// Test-only helper that simulates a lifecycle change without an epoch
    /// transition. Production transitions must use the atomic path instead.
    #[cfg(all(test, target_os = "linux"))]
    fn cancel_current(&self) {
        let _lifecycle = self.lock_lifecycle();
        self.cancel_worker();
        // A lifecycle change must immediately fence results from the previous
        // managed connection, even if the old worker has not observed its
        // cancellation token yet. This fence is intentionally not persisted.
        self.lock_slot().store.fence_in_flight();
    }

    /// Start the single worker or coalesce with the running one. A new token is
    /// created only when no worker owns the slot.
    ///
    /// The lifecycle gate is held while the token is created so a transition
    /// that is cancelling and fencing the old lifecycle cannot race a new
    /// uncancelled worker into the gap between cancellation and the fence.
    fn start_worker(&self) -> Option<Arc<AtomicBool>> {
        let _lifecycle = self.lock_lifecycle();
        if self.is_shutting_down() {
            return None;
        }
        let mut worker = self.lock_worker();
        if worker.running {
            worker.dirty = true;
            return None;
        }
        worker.running = true;
        worker.dirty = false;
        let token = Arc::new(AtomicBool::new(false));
        worker.token = Some(Arc::clone(&token));
        Some(token)
    }

    /// Release the worker slot, replacing a cancelled token with a fresh one
    /// when a trigger raced the exit. This is what lets pending work survive a
    /// connection switch instead of being dropped with the old token.
    fn finish_worker(
        &self,
        token: &Arc<AtomicBool>,
        allow_respawn: bool,
    ) -> Option<Arc<AtomicBool>> {
        // Respawn also takes the lifecycle gate. A worker that exits while a
        // transition is cancelling/fencing therefore waits and can only start
        // against the new epoch, never between the cancel and the fence.
        let _lifecycle = self.lock_lifecycle();
        let mut worker = self.lock_worker();
        worker.running = false;
        if worker
            .token
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, token))
        {
            worker.token = None;
        }
        if !allow_respawn || !worker.dirty || self.is_shutting_down() {
            worker.dirty = false;
            return None;
        }
        worker.dirty = false;
        worker.running = true;
        let next = Arc::new(AtomicBool::new(false));
        worker.token = Some(Arc::clone(&next));
        Some(next)
    }

    /// Consume the dirty flag set by a concurrent trigger.
    fn take_dirty(&self) -> bool {
        let mut worker = self.lock_worker();
        std::mem::take(&mut worker.dirty)
    }
}

/// Revalidate a captured lifecycle ticket and run one hash-state mutation.
///
/// The lifecycle/state/slot locks are held only to snapshot the fork and to
/// install confirmed memory. The durable write is submitted to the isolated
/// persistence owner with no lock held, so a hung `fsync`/`rename` can never
/// block shutdown. If the lifecycle wins the race after the owner published,
/// the result is not installed as current and the domain is quarantined; we do
/// not claim the already-authorized rename could not physically occur.
fn mutate_hash_at_ticket<T>(
    state: &Mutex<ConnectionState>,
    hashes: &HashCoordinator,
    ticket: &HashWriteTicket,
    mutate: impl FnOnce(&mut HashSlot) -> Result<T, BridgeError>,
) -> Result<T, BridgeError> {
    let _write = hashes.lock_write();
    #[cfg(test)]
    hashes.hash_write_pause.wait();

    // Snapshot and validate under the lifecycle gate, but do not mutate or
    // persist while holding it.
    let (mut staged_slot, base_revision) = {
        let _lifecycle = hashes.lock_lifecycle();
        let state = state.lock().map_err(|_| BridgeError::State)?;
        if hashes.is_shutting_down() || state.shutting_down {
            return Err(BridgeError::Superseded);
        }
        if state.epoch != ticket.epoch
            || state.hash_daemon_binding().as_ref() != Some(&ticket.binding)
        {
            return Err(BridgeError::Superseded);
        }
        let slot = hashes.lock_slot();
        if slot.store.fence() != ticket.fence
            || slot.namespace.as_deref() != ticket.namespace.as_deref()
        {
            return Err(BridgeError::Superseded);
        }
        let staged = HashSlot {
            path: slot.path.clone(),
            loaded: slot.loaded,
            namespace: slot.namespace.clone(),
            store: slot.store.fork_for_staging(),
        };
        (staged, slot.store.revision())
    };

    let result = mutate(&mut staged_slot)?;
    if staged_slot.store.revision() == base_revision {
        return Ok(result);
    }
    let staged = staged_slot
        .store
        .stage_write()
        .map_err(|_| BridgeError::State)?;
    #[cfg(test)]
    hashes.hash_commit_pause.wait();
    if hashes.is_shutting_down() {
        return Err(BridgeError::Superseded);
    }

    let Some(domain) = hashes.domain() else {
        return Err(BridgeError::State);
    };
    let prepared = match domain.prepare(base_revision, staged.bytes().to_vec()) {
        Ok(crate::hash_persistence::PrepareOutcome::Prepared(prepared)) => prepared,
        Ok(crate::hash_persistence::PrepareOutcome::BeforeRename) => {
            return Err(BridgeError::State);
        }
        Ok(crate::hash_persistence::PrepareOutcome::Conflict) => {
            hashes.abandon_domain();
            return Err(BridgeError::Superseded);
        }
        Ok(crate::hash_persistence::PrepareOutcome::ExternalChange)
        | Ok(crate::hash_persistence::PrepareOutcome::Unconfirmed)
        | Err(_) => {
            return Err(if hashes.is_shutting_down() {
                BridgeError::Superseded
            } else {
                BridgeError::HashUnconfirmed
            });
        }
    };

    // The state is already durable, but authorization is decided only while
    // the lifecycle gate is held. A stale ticket aborts before the marker can
    // become `acked`, so a successor fails closed instead of accepting an
    // obsolete publication. A transition cannot slip between this validation
    // and the durable acknowledgement.
    let _lifecycle = hashes.lock_lifecycle();
    let state = state.lock().map_err(|_| BridgeError::State)?;
    let mut slot = hashes.lock_slot();
    if hashes.is_shutting_down()
        || state.shutting_down
        || state.epoch != ticket.epoch
        || state.hash_daemon_binding().as_ref() != Some(&ticket.binding)
    {
        drop(slot);
        drop(state);
        drop(_lifecycle);
        prepared.abort();
        hashes.abandon_domain();
        return Err(BridgeError::Superseded);
    }
    if slot.store.fence() != ticket.fence
        || slot.namespace.as_deref() != ticket.namespace.as_deref()
        || slot.store.revision() != base_revision
    {
        drop(slot);
        drop(state);
        drop(_lifecycle);
        prepared.abort();
        hashes.abandon_domain();
        return Err(BridgeError::HashUnconfirmed);
    }
    let committed_revision = match prepared.confirm() {
        Ok(crate::hash_persistence::CommitOutcome::Committed(revision)) => revision,
        Ok(_) | Err(_) => {
            drop(slot);
            drop(state);
            drop(_lifecycle);
            return Err(BridgeError::HashUnconfirmed);
        }
    };
    if hashes.is_shutting_down() || state.shutting_down {
        drop(slot);
        drop(state);
        drop(_lifecycle);
        hashes.abandon_domain();
        return Err(BridgeError::Superseded);
    }
    drop(state);
    staged.install(&mut slot.store, committed_revision);
    Ok(result)
}

fn spawn_hash_worker(
    state: Arc<Mutex<ConnectionState>>,
    lifecycle: Arc<ManagedLifecycle>,
    hashes: Arc<HashCoordinator>,
    token: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        'worker: loop {
            // Check cancellation before consuming the dirty flag. A trigger
            // that raced cancellation must survive into `finish_worker`, which
            // restarts with a fresh token.
            if token.load(Ordering::SeqCst) || lifecycle.is_shutting_down() {
                break 'worker;
            }
            let _ = hashes.take_dirty();
            // Capture the daemon target and the hash lifecycle ticket under
            // one state lock, then the namespace/fence under the slot lock.
            // This binds the eventual reconcile mutation to the exact
            // connection epoch that produced the `/list` request.
            let (snapshot, ticket) = {
                let state = match state.lock() {
                    Ok(state) => state,
                    Err(poisoned) => poisoned.into_inner(),
                };
                match (state.managed_rate_snapshot(), state.hash_daemon_binding()) {
                    (Ok(snapshot), Some(binding)) => {
                        let slot = hashes.lock_slot();
                        let Some(namespace) = slot.namespace.clone() else {
                            break 'worker;
                        };
                        (
                            snapshot,
                            HashWriteTicket {
                                epoch: state.epoch,
                                binding,
                                fence: slot.store.fence(),
                                namespace: Some(namespace),
                            },
                        )
                    }
                    _ => break 'worker,
                }
            };
            let Some(namespace) = ticket.namespace.clone() else {
                break 'worker;
            };
            // A transition may have cancelled the token after the capture
            // above; do not start a superseded `/list` request.
            if token.load(Ordering::SeqCst) || lifecycle.is_shutting_down() {
                break 'worker;
            }
            // Capture the exact cohort before the snapshot request starts.
            // A record accepted or modified while `/list` is in flight must
            // never be invalidated by that older response.
            let cohort = hashes.reconcile_cohort();
            let candidates = if !cohort.is_empty() {
                // Revalidate only that cohort against the fresh authenticated
                // owned-daemon queue before retaining/resuming it.
                let Ok(downloads) = snapshot.client.refresh().await else {
                    break 'worker;
                };
                if token.load(Ordering::SeqCst) || lifecycle.is_shutting_down() {
                    break 'worker;
                }
                let items: Vec<_> = downloads
                    .iter()
                    .map(crate::hash_state::QueueItemIdentity::from_download)
                    .collect();
                match mutate_hash_at_ticket(&state, &hashes, &ticket, |slot| {
                    if !slot.loaded {
                        return Err(BridgeError::State);
                    }
                    slot.store
                        .reconcile_cohort_and_candidates(&namespace, &items, &cohort)
                        .map_err(|_| BridgeError::State)
                }) {
                    Ok(candidates) => candidates,
                    Err(_) => break 'worker,
                }
            } else {
                hashes.candidates(&namespace)
            };
            if candidates.is_empty() {
                if hashes.take_dirty() {
                    continue 'worker;
                }
                break 'worker;
            }
            for candidate in candidates {
                if token.load(Ordering::SeqCst) || lifecycle.is_shutting_down() {
                    break 'worker;
                }
                let check_state = Arc::clone(&state);
                let check_token = Arc::clone(&token);
                let check_snapshot = snapshot.clone();
                let check_lifecycle = Arc::clone(&lifecycle);
                let current = move || {
                    if check_token.load(Ordering::SeqCst) || check_lifecycle.is_shutting_down() {
                        return false;
                    }
                    match check_state.lock() {
                        Ok(state) => {
                            !state.shutting_down && state.rate_write_is_current(&check_snapshot)
                        }
                        Err(_) => false,
                    }
                };
                match automatic_hash::hash_one(
                    &snapshot.client,
                    &candidate.id,
                    candidate.added_at,
                    &candidate.fingerprint,
                    &token,
                    current,
                )
                .await
                {
                    Ok(automatic_hash::HashOneOutcome::Completed(completed)) => {
                        if mutate_hash_at_ticket(&state, &hashes, &ticket, |slot| {
                            if !slot.loaded {
                                return Err(BridgeError::State);
                            }
                            slot.store
                                .mark_complete(
                                    &candidate.id,
                                    candidate.generation,
                                    candidate.fence,
                                    completed,
                                )
                                .map(|_| ())
                                .map_err(|_| BridgeError::State)
                        })
                        .is_err()
                        {
                            break 'worker;
                        }
                    }
                    Ok(automatic_hash::HashOneOutcome::Pending) => {}
                    Ok(automatic_hash::HashOneOutcome::Failed) => {
                        if mutate_hash_at_ticket(&state, &hashes, &ticket, |slot| {
                            if !slot.loaded {
                                return Err(BridgeError::State);
                            }
                            slot.store
                                .mark_failed(&candidate.id, candidate.generation, candidate.fence)
                                .map(|_| ())
                                .map_err(|_| BridgeError::State)
                        })
                        .is_err()
                        {
                            break 'worker;
                        }
                    }
                    Ok(automatic_hash::HashOneOutcome::Unavailable) => {
                        if mutate_hash_at_ticket(&state, &hashes, &ticket, |slot| {
                            if !slot.loaded {
                                return Err(BridgeError::State);
                            }
                            slot.store
                                .mark_unavailable(
                                    &candidate.id,
                                    candidate.generation,
                                    candidate.fence,
                                )
                                .map(|_| ())
                                .map_err(|_| BridgeError::State)
                        })
                        .is_err()
                        {
                            break 'worker;
                        }
                    }
                    Ok(automatic_hash::HashOneOutcome::Stale) => {
                        if mutate_hash_at_ticket(&state, &hashes, &ticket, |slot| {
                            if !slot.loaded {
                                return Err(BridgeError::State);
                            }
                            slot.store
                                .mark_stale(&candidate.id, candidate.generation, candidate.fence)
                                .map(|_| ())
                                .map_err(|_| BridgeError::State)
                        })
                        .is_err()
                        {
                            break 'worker;
                        }
                    }
                    Ok(automatic_hash::HashOneOutcome::Cancelled)
                    | Err(HashWorkError::Daemon)
                    | Err(HashWorkError::Internal) => break 'worker,
                }
            }
            if hashes.take_dirty() {
                continue 'worker;
            }
            break 'worker;
        }

        if let Some(next) = hashes.finish_worker(&token, !lifecycle.is_shutting_down()) {
            spawn_hash_worker(state, lifecycle, hashes, next);
        }
    });
}

#[cfg(test)]
#[derive(Clone)]
struct TestManagedConnection {
    client: Arc<SurgeClient>,
    pid: u32,
    exited: bool,
}

#[derive(Default)]
struct ConnectionState {
    epoch: u64,
    start_generation: u64,
    starting: bool,
    shutting_down: bool,
    retiring: usize,
    manual: Option<Arc<SurgeClient>>,
    managed: Option<ManagedServer>,
    #[cfg(test)]
    test_managed: Option<TestManagedConnection>,
    hash_connection_id: Option<String>,
    active: ActiveConnection,
}

impl ConnectionState {
    fn active_snapshot(&self) -> Result<(Arc<SurgeClient>, u64), BridgeError> {
        match self.active {
            ActiveConnection::None => Err(BridgeError::NotConnected),
            ActiveConnection::Manual => {
                let client = self.manual.clone().ok_or(BridgeError::NotConnected)?;
                Ok((client, self.epoch))
            }
            ActiveConnection::Managed => {
                #[cfg(test)]
                if let Some(test) = self.test_managed.as_ref() {
                    if test.exited {
                        return Err(BridgeError::NotConnected);
                    }
                    return Ok((Arc::clone(&test.client), self.epoch));
                }
                let server = self.managed.as_ref().ok_or(BridgeError::NotConnected)?;
                if server.has_exited() {
                    return Err(BridgeError::NotConnected);
                }
                Ok((server.client(), self.epoch))
            }
        }
    }

    fn managed_rate_snapshot(&self) -> Result<ManagedRateSnapshot, BridgeError> {
        if self.active != ActiveConnection::Managed {
            return Err(BridgeError::NotConnected);
        }
        #[cfg(test)]
        if let Some(test) = self.test_managed.as_ref() {
            if test.exited {
                return Err(BridgeError::NotConnected);
            }
            return Ok(ManagedRateSnapshot {
                client: Arc::clone(&test.client),
                epoch: self.epoch,
                pid: test.pid,
            });
        }
        let server = self.managed.as_ref().ok_or(BridgeError::NotConnected)?;
        if server.has_exited() {
            return Err(BridgeError::NotConnected);
        }
        Ok(ManagedRateSnapshot {
            client: server.client(),
            epoch: self.epoch,
            pid: server.pid(),
        })
    }

    fn hash_daemon_binding(&self) -> Option<crate::hash_state::DaemonBinding> {
        if self.active != ActiveConnection::Managed {
            return None;
        }
        let connection_id = self.hash_connection_id.clone()?;
        #[cfg(test)]
        if let Some(test) = self.test_managed.as_ref() {
            if test.exited {
                return None;
            }
            return Some(crate::hash_state::DaemonBinding {
                connection_id,
                daemon_pid: test.pid,
            });
        }
        let server = self.managed.as_ref()?;
        if server.has_exited() {
            return None;
        }
        Some(crate::hash_state::DaemonBinding {
            connection_id,
            daemon_pid: server.pid(),
        })
    }

    fn managed_location_snapshot(&self) -> Result<ManagedRateSnapshot, BridgeError> {
        if self.active == ActiveConnection::Manual {
            return Err(BridgeError::ManagedConnectionRequired);
        }
        self.managed_rate_snapshot()
    }

    /// Same managed-only target as a location read, used for settings writes.
    fn managed_settings_snapshot(&self) -> Result<ManagedRateSnapshot, BridgeError> {
        if self.active == ActiveConnection::Manual {
            return Err(BridgeError::ManagedConnectionRequired);
        }
        self.managed_rate_snapshot()
    }

    fn rate_write_is_current(&self, snapshot: &ManagedRateSnapshot) -> bool {
        if self.epoch != snapshot.epoch || self.active != ActiveConnection::Managed {
            return false;
        }
        #[cfg(test)]
        if let Some(test) = self.test_managed.as_ref() {
            return !test.exited && test.pid == snapshot.pid;
        }
        self.managed
            .as_ref()
            .is_some_and(|server| !server.has_exited() && server.pid() == snapshot.pid)
    }

    fn clear_active(&mut self) {
        self.manual = None;
        self.hash_connection_id = None;
        self.active = ActiveConnection::None;
        #[cfg(test)]
        {
            self.test_managed = None;
        }
    }

    fn mark_retirement(&mut self) {
        self.retiring = self.retiring.saturating_add(1);
    }

    fn finish_retirement(&mut self) {
        self.retiring = self.retiring.saturating_sub(1);
    }

    fn can_begin_managed_start(&self) -> Result<(), ManagedError> {
        if self.shutting_down {
            return Err(ManagedError::ShuttingDown);
        }
        if self.starting {
            return Err(ManagedError::AlreadyStarted);
        }
        if let Some(server) = self.managed.as_ref() {
            // A live daemon that is still the active managed connection keeps
            // the single-start invariant. Anything else (a crashed child or a
            // daemon that is no longer reachable because another connection
            // superseded it) must not block an explicit restart: retire it
            // outside the lock and free the slot.
            if self.active == ActiveConnection::Managed && !server.has_exited() {
                return Err(ManagedError::AlreadyStarted);
            }
        }
        Ok(())
    }

    fn begin_managed_start(
        &mut self,
    ) -> Result<(ManagedStartTicket, Option<ManagedServer>), ManagedError> {
        self.can_begin_managed_start()?;
        // Remove the old daemon under the lock, but never shut it down while
        // holding the lock: the caller performs the bounded network/process
        // retirement after the guard is released.
        let retired = self.managed.take();
        self.start_generation = self.start_generation.wrapping_add(1);
        self.starting = true;
        self.epoch = self.epoch.wrapping_add(1);
        self.clear_active();
        Ok((
            ManagedStartTicket {
                generation: self.start_generation,
                epoch: self.epoch,
            },
            retired,
        ))
    }

    fn abort_managed_start(&mut self, ticket: ManagedStartTicket) {
        if self.starting && self.start_generation == ticket.generation {
            self.starting = false;
        }
    }

    /// Decide whether a completed startup may become the active connection.
    ///
    /// A `false` result means the startup lost the connection-epoch race. The
    /// caller must drop the newly started server (the reclaimed pid is never
    /// trusted) so a superseded startup cannot leave an unreachable daemon
    /// behind. An error means the claim was invalidated by shutdown and the
    /// server must also drop.
    fn decide_managed_finish(&mut self, ticket: ManagedStartTicket) -> Result<bool, ManagedError> {
        if self.shutting_down {
            return Err(ManagedError::ShuttingDown);
        }
        if self.start_generation != ticket.generation || !self.starting {
            return Err(ManagedError::Superseded);
        }
        self.starting = false;
        Ok(self.epoch == ticket.epoch
            && self.active == ActiveConnection::None
            && self.manual.is_none())
    }
}

/// RAII claim for one managed startup.
///
/// If the command future is cancelled (frontend drop, timeout, runtime
/// shutdown) before disarming, `Drop` releases the claim and replaces it with
/// a retirement fence that remains until the real owner thread exits/reaps the
/// child. The lifecycle operation is handed directly to that cleanup watcher,
/// so app-exit drain cannot observe a zero gap on mere future cancellation.
struct ManagedStartGuard {
    state: Arc<Mutex<ConnectionState>>,
    ticket: ManagedStartTicket,
    owner: OwnerLifetime,
    operation: Option<LifecycleOperation>,
    finished: bool,
}

impl ManagedStartGuard {
    fn new(
        manager: &ConnectionManager,
        ticket: ManagedStartTicket,
        owner: OwnerLifetime,
        operation: LifecycleOperation,
    ) -> Self {
        Self {
            state: Arc::clone(&manager.state),
            ticket,
            owner,
            operation: Some(operation),
            finished: false,
        }
    }

    fn disarm(mut self) {
        self.finished = true;
    }
}

impl Drop for ManagedStartGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let operation = match self.operation.take() {
            Some(operation) => operation,
            None => return,
        };
        self.owner.finish_if_unstarted();
        {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.abort_managed_start(self.ticket);
            state.mark_retirement();
        }
        spawn_owner_cleanup(Arc::clone(&self.state), self.owner.clone(), operation, true);
    }
}

#[cfg(test)]
#[derive(Default)]
struct VerifiedPause {
    inner: std::sync::Mutex<Option<VerifiedPauseArmed>>,
}

#[cfg(test)]
struct VerifiedPauseArmed {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
impl VerifiedPause {
    #[cfg(target_os = "linux")]
    fn arm(&self) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let mut inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        *inner = Some(VerifiedPauseArmed {
            entered,
            release: release_rx,
        });
        (entered_rx, release)
    }

    #[cfg(target_os = "linux")]
    fn clear(&self) {
        let mut inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        *inner = None;
    }

    fn wait(&self) {
        let inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(pause) = inner.as_ref() {
            let _ = pause.entered.send(());
            let _ = pause
                .release
                .recv_timeout(std::time::Duration::from_secs(5));
        }
    }
}

/// Owns the single active Surge connection and, once started, the single
/// owned managed daemon.
///
/// `epoch` is a monotonic fence: every `connect`/`disconnect`/managed start
/// bumps it, and a completed network call is discarded unless its epoch is
/// still current. That stops a slow earlier operation from overwriting a
/// newer connection (or a `disconnect`). The mutex is never held across an
/// `await`; managed calls clone the server's `Arc<SurgeClient>` and leave the
/// owned server in place, so cancellation cannot drop the child or strand a
/// lease. Concurrent polling and control calls share the same pinned client.
#[derive(Default)]
pub struct ConnectionManager {
    state: Arc<Mutex<ConnectionState>>,
    lifecycle: Arc<ManagedLifecycle>,
    progress: Arc<ProgressStream>,
    rate_writer: Arc<RateWriter>,
    settings_writer: Arc<SettingsWriteGate>,
    location_validation: Arc<LocationValidationGate>,
    location_launch: Arc<LocationLaunchGate>,
    hashes: Arc<HashCoordinator>,
    #[cfg(test)]
    verified_pause: VerifiedPause,
    #[cfg(test)]
    verified_list_pause: VerifiedPause,
    #[cfg(test)]
    record_intents_pause: VerifiedPause,
    #[cfg(test)]
    lifecycle_fence_pause: VerifiedPause,
}

impl fmt::Debug for ConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let connected = match self.state.lock() {
            Ok(state) => state.active != ActiveConnection::None,
            Err(poisoned) => poisoned.into_inner().active != ActiveConnection::None,
        };
        f.debug_struct("ConnectionManager")
            .field("connected", &connected)
            .finish()
    }
}

impl ConnectionManager {
    /// Authenticate against the Surge instance and store the connection.
    ///
    /// Any owned managed daemon is detached under the lock and retired outside
    /// it before the manual network round-trip, so a manual switch never leaves
    /// the superseded daemon alive but unreachable. A failed or superseded
    /// manual attempt cannot orphan it either: the retired server is consumed
    /// by the bounded shutdown below on every path (cancellation falls back to
    /// `ManagedServer::drop`). The manual listener is never sent `/shutdown`;
    /// only the retired server's own pinned managed client is used for that.
    pub async fn connect(
        &self,
        endpoint: &str,
        token: &str,
    ) -> Result<Vec<QueueItemDto>, BridgeError> {
        // A replacement connection must never leave the previous stream or
        // hash worker reading from the superseded daemon.
        self.progress.stop();
        // Hold the lifecycle operation only across taking and retiring the old
        // daemon. If app shutdown has already started, the shutdown path owns
        // any remaining server and this attempt must not cancel/fence it.
        let lifecycle = match self.lifecycle.begin_operation() {
            Some(guard) => guard,
            None => return Err(BridgeError::Superseded),
        };
        let (epoch, retired, retirement_registered) = {
            // Cancel and fence under one gate so a trigger cannot start a new
            // uncancelled worker between the two operations.
            let _lifecycle = self.hashes.lock_lifecycle();
            self.hashes.cancel_worker();
            #[cfg(test)]
            self.lifecycle_fence_pause.wait();
            let mut state = self.state.lock().map_err(|_| BridgeError::State)?;
            let mut slot = self.hashes.lock_slot();
            slot.store.fence_in_flight();
            state.epoch = state.epoch.wrapping_add(1);
            state.clear_active();
            let retired = state.managed.take();
            let registered = if retired.is_some() {
                state.mark_retirement();
                true
            } else {
                false
            };
            (state.epoch, retired, registered)
        };

        // Keep the retirement visible until the owned child has actually been
        // retired, so a new managed start cannot bind the fixed port while the
        // old daemon is still draining. No lock is held across the await.
        let retirement = if retirement_registered {
            let owner = retired
                .as_ref()
                .map(|server| server.owner_lifetime())
                .expect("registered retirement must own a server");
            Some(self.retirement_guard_for(owner))
        } else {
            None
        };
        // Retire the old owned daemon outside the lock. No lock is held while
        // the authenticated /shutdown request or the child-exit wait runs.
        if let Some(server) = retired {
            let _ = server.shutdown().await;
        }
        drop(retirement);
        drop(lifecycle);

        // Network runs without the lock held.
        let client = SurgeClient::connect(endpoint, token).await?;
        let items = project(client.list_downloads());

        {
            let mut state = self.state.lock().map_err(|_| BridgeError::State)?;
            if state.epoch != epoch {
                return Err(BridgeError::Superseded);
            }
            state.manual = Some(Arc::new(client));
            state.active = ActiveConnection::Manual;
        }

        Ok(items)
    }

    /// Claim the single managed-start slot and reserve a connection epoch.
    ///
    /// Returns any superseded/crashed server that was removed from state. The
    /// caller must retire it outside the state lock (bounded graceful
    /// shutdown), so network work never runs while the lock is held.
    fn begin_managed_start(
        &self,
    ) -> Result<(ManagedStartTicket, Option<ManagedServer>), ManagedError> {
        let _lifecycle = self.hashes.lock_lifecycle();
        // Reject an invalid start before touching the progress stream, worker,
        // or fence. A concurrent valid claim or shutdown must not disturb the
        // work that already belongs to the current lifecycle.
        {
            let state = self.state.lock().map_err(|_| ManagedError::State)?;
            state.can_begin_managed_start()?;
        }
        self.progress.stop();
        self.hashes.cancel_worker();
        #[cfg(test)]
        self.lifecycle_fence_pause.wait();
        let mut state = self.state.lock().map_err(|_| ManagedError::State)?;
        let mut slot = self.hashes.lock_slot();
        slot.store.fence_in_flight();
        state.begin_managed_start()
    }

    fn register_retirement(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.mark_retirement();
    }

    #[cfg(test)]
    fn retirement_guard(&self) -> RetirementGuard {
        self.retirement_guard_owner(None)
    }

    fn retirement_guard_for(&self, owner: OwnerLifetime) -> RetirementGuard {
        self.retirement_guard_owner(Some(owner))
    }

    fn retirement_guard_owner(&self, owner: Option<OwnerLifetime>) -> RetirementGuard {
        RetirementGuard {
            state: Arc::clone(&self.state),
            lifecycle: Arc::clone(&self.lifecycle),
            owner,
            armed: true,
        }
    }

    /// Wait, within a fixed bound, for retirements that were detached before
    /// this start claimed the slot. `own` counts the retirement registered by
    /// this start itself, which has not begun yet.
    async fn wait_for_prior_retirements(
        &self,
        own: usize,
        timeout: Duration,
    ) -> Result<(), ManagedError> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.lifecycle.is_shutting_down() {
                return Err(ManagedError::ShuttingDown);
            }
            {
                let state = self.state.lock().map_err(|_| ManagedError::State)?;
                if state.retiring <= own {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(ManagedError::Retry);
            }
            tokio::time::sleep(RETIREMENT_POLL_INTERVAL).await;
        }
    }

    /// Release a failed managed-start claim without touching a newer start.
    #[cfg(test)]
    fn abort_managed_start(&self, ticket: ManagedStartTicket) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.abort_managed_start(ticket);
    }

    /// Install a successfully started owned server.
    ///
    /// The server is retained only when it is still the current epoch's active
    /// managed connection. If a `connect`/`disconnect`/newer start won the
    /// race, the just-started server is retired here with the same bounded
    /// graceful shutdown used everywhere else, instead of being left alive but
    /// unreachable and blocking the next explicit restart. `Superseded` is
    /// returned without exposing any detail.
    fn finish_managed_start(
        &self,
        ticket: ManagedStartTicket,
        server: ManagedServer,
    ) -> Result<Vec<QueueItemDto>, ManagedError> {
        let cached = project(server.client().list_downloads());
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.mark_retirement();
                drop(state);
                let owner = server.owner_lifetime();
                let _retirement = self.retirement_guard_for(owner);
                let _ = server.shutdown_blocking();
                return Err(ManagedError::State);
            }
        };
        let activate = match state.decide_managed_finish(ticket) {
            Ok(activate) => activate,
            Err(error) => {
                // This just-started daemon was never installed. Keep its
                // retirement visible to any concurrent/new start until the
                // bounded shutdown below has actually reaped it.
                state.mark_retirement();
                drop(state);
                let owner = server.owner_lifetime();
                let _retirement = self.retirement_guard_for(owner);
                let _ = server.shutdown_blocking();
                return Err(error);
            }
        };
        if !activate {
            state.mark_retirement();
            drop(state);
            let owner = server.owner_lifetime();
            let _retirement = self.retirement_guard_for(owner);
            let _ = server.shutdown_blocking();
            return Err(ManagedError::Superseded);
        }
        state.managed = Some(server);
        state.manual = None;
        #[cfg(test)]
        {
            state.test_managed = None;
        }
        state.hash_connection_id = Some(new_hash_connection_id());
        state.active = ActiveConnection::Managed;
        drop(state);
        Ok(cached)
    }

    /// Stop and reap the owned managed daemon at app shutdown.
    ///
    /// In-flight operations hold only an `Arc<SurgeClient>` clone, so taking
    /// the owned server out here is safe and never strands a lease. Detached
    /// starts and retirements are cancelled through the lifecycle flag and
    /// waited for within a fixed bound before this returns. A location launch
    /// that already reserved before the fence is accepted and may finish after
    /// shutdown begins; shutdown waits for it only up to the same bound and
    /// does not promise that an accepted launch cannot physically spawn a
    /// process after shutdown starts.
    pub fn shutdown_managed(&self) -> bool {
        // Fence new lifecycle work first, then invalidate the connection state.
        // This ordering means an operation that already claimed a slot either
        // sees `shutting_down` before finishing or gets cancelled below.
        self.lifecycle.begin_shutdown();
        self.hashes.begin_shutdown();
        self.progress.stop();
        let server = {
            let _lifecycle = self.hashes.lock_lifecycle();
            self.hashes.cancel_worker();
            #[cfg(test)]
            self.lifecycle_fence_pause.wait();
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            let mut slot = self.hashes.lock_slot();
            slot.store.fence_in_flight();
            state.shutting_down = true;
            state.starting = false;
            state.start_generation = state.start_generation.wrapping_add(1);
            state.epoch = state.epoch.wrapping_add(1);
            state.clear_active();
            state.managed.take()
        };
        if let Some(server) = server {
            let owner = server.owner_lifetime();
            // Count the active server shutdown itself so app drain cannot
            // complete before its owner is confirmed reaped.
            let operation = self.lifecycle.begin_cleanup();
            if server.shutdown_blocking() && owner.is_finished() && owner.was_reaped() {
                drop(operation);
            } else {
                spawn_owner_cleanup(Arc::clone(&self.state), owner, operation, false);
            }
        }
        // Wait for detached starts/retirements and registered owner cleanup
        // completion to observe the flag and finish reaping. All paths are
        // bounded; a timeout returns false instead of claiming the drain was
        // complete.
        self.lifecycle.wait_for_drain(LIFECYCLE_DRAIN_TIMEOUT)
    }

    /// Clone the active client without holding the state lock across awaits.
    ///
    /// Managed mode clones the pinned client out of the still-owned server;
    /// the server itself never leaves the state, so cancellation of the caller
    /// cannot kill the child or leave a "leased" gap that rejects concurrent
    /// polling/control calls with `NotConnected`.
    fn acquire_active(&self) -> Result<(Arc<SurgeClient>, u64), BridgeError> {
        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        state.active_snapshot()
    }

    /// Re-query `/list` on the active connection.
    ///
    /// The client handle is cloned under the lock; the network round trip and
    /// any cancellation run with no state lock held and without taking the
    /// owned managed server out of the state.
    pub async fn refresh(&self) -> Result<Vec<QueueItemDto>, BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        let downloads = client.refresh().await?;

        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        drop(state);
        Ok(project(&downloads))
    }

    /// Fresh queue listing; each call performs an authenticated `GET /list`.
    ///
    /// Delegates to [`Self::refresh`] so the existing epoch fence applies and
    /// no lock is held across the await.
    pub async fn list_downloads(&self) -> Result<Vec<QueueItemDto>, BridgeError> {
        self.refresh().await
    }

    /// Authoritative queue snapshot with the progress-stream cursor captured
    /// before the `/list` request. Events received during the read therefore
    /// carry a newer receipt sequence and can be merged over the snapshot,
    /// while queued pre-read samples remain at or below the cursor.
    pub async fn list_downloads_snapshot(&self) -> Result<QueueSnapshotDto, BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        let stream_seq = self.progress.seq();
        let downloads = client.refresh().await?;

        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        drop(state);
        Ok(QueueSnapshotDto {
            epoch,
            stream_seq,
            items: project(&downloads),
        })
    }

    /// Start the single progress stream for the current connection.
    ///
    /// `subscription` is the frontend's monotonic connection epoch. The stream
    /// task is additionally bound to the Rust connection epoch and to a live
    /// managed/manual connection, so it exits after any replacement,
    /// disconnect, disposal or shutdown even if this start raced a stop.
    pub fn start_progress_stream(
        &self,
        subscription: u64,
        app: tauri::AppHandle,
    ) -> Result<(), BridgeError> {
        if subscription == 0 {
            return Err(BridgeError::InvalidResponse);
        }
        let (client, epoch) = self.acquire_active()?;
        let state = Arc::clone(&self.state);
        let is_current = move || match state.lock() {
            Ok(state) => state.epoch == epoch && state.active != ActiveConnection::None,
            Err(poisoned) => {
                let state = poisoned.into_inner();
                state.epoch == epoch && state.active != ActiveConnection::None
            }
        };
        let emit: Arc<dyn Fn(&'static str, serde_json::Value) + Send + Sync> =
            Arc::new(move |event, payload| {
                let _ = app.emit(event, payload);
            });
        self.progress
            .start(subscription, epoch, client, emit, is_current);
        Ok(())
    }

    /// Stop and abort the progress stream (best-effort, non-blocking).
    pub fn stop_progress_stream(&self) {
        self.progress.stop();
    }

    /// Open the containing folder of a completed download on the owned managed
    /// daemon.
    ///
    /// The IPC caller supplies only an id. This captures the owned daemon's
    /// client, epoch and pid, performs a fresh authenticated `/list`, requires
    /// exactly one matching completed item that is neither moving nor already
    /// reported missing, validates the server-provided destination and its
    /// actual file, then rechecks the same epoch/pid/active-managed state
    /// immediately before launching the platform file manager. Manual/remote
    /// connections, stale daemons, duplicate ids and invalid locations fail
    /// closed. The destination path is never returned, logged or forwarded.
    pub async fn open_download_location(&self, id: &str) -> Result<(), BridgeError> {
        self.open_download_location_with(id, Arc::new(SystemLocationLauncher))
            .await
    }

    async fn open_download_location_with<L>(
        &self,
        id: &str,
        launcher: Arc<L>,
    ) -> Result<(), BridgeError>
    where
        L: LocationLauncher + 'static,
    {
        if id.is_empty() || id.len() > MAX_DOWNLOAD_ID_BYTES || id.chars().any(char::is_control) {
            return Err(BridgeError::InvalidDownloadId);
        }

        let snapshot = {
            let state = self.state.lock().map_err(|_| BridgeError::State)?;
            if state.shutting_down {
                return Err(BridgeError::Superseded);
            }
            state.managed_location_snapshot()?
        };

        // Keep app shutdown from draining while the refresh, validation and
        // launch are still running; the final state check below also observes
        // a shutdown that began in the meantime.
        let operation = self
            .lifecycle
            .begin_operation()
            .ok_or(BridgeError::Superseded)?;
        if operation.is_shutting_down() {
            return Err(BridgeError::Superseded);
        }

        let downloads = snapshot.client.refresh().await.map_err(BridgeError::from)?;
        let mut matches = downloads.iter().filter(|download| download.id == id);
        let download = match (matches.next(), matches.next()) {
            (Some(download), None) => download,
            (None, _) => return Err(BridgeError::DownloadNotFound),
            (Some(_), Some(_)) => return Err(BridgeError::DownloadAmbiguous),
        };
        if download.status != "completed" {
            return Err(BridgeError::DownloadNotCompleted);
        }
        if download.moving {
            return Err(BridgeError::DownloadMoving);
        }
        if download.file_missing {
            return Err(BridgeError::DownloadFileMissing);
        }
        let destination = download
            .dest_path
            .as_deref()
            .filter(|path| !path.is_empty())
            .ok_or(BridgeError::LocationUnavailable)?;
        let destination = destination.to_owned();
        let directory = self
            .validate_location_blocking(move || {
                open_location::containing_directory_for_completed_file(&destination)
            })
            .await?;

        if operation.is_shutting_down() {
            return Err(BridgeError::Superseded);
        }
        let (launch, operation) = self
            .launch_location_if_current(operation, &snapshot, launcher, directory)
            .await?;
        // Confirm bounded immediate status off the async worker; the lifecycle
        // ticket retained by the accepted reservation keeps shutdown waiting
        // for this bounded step too.
        tokio::task::spawn_blocking(move || {
            let _operation = operation;
            launch.wait_bounded(open_location::LAUNCH_STATUS_TIMEOUT)
        })
        .await
        .map_err(|_| BridgeError::LocationLaunchFailed)??;
        Ok(())
    }

    /// Run the synchronous filesystem validation on the blocking pool under a
    /// single manager-wide permit. The permit is acquired before dispatch and
    /// moved into the closure, so it remains held until validation finishes
    /// even if this future is cancelled. A busy permit returns a fixed
    /// sanitized failure without dispatching more blocking work, so a hung
    /// filesystem cannot accumulate blocked workers.
    async fn validate_location_blocking<F>(&self, validate: F) -> Result<PathBuf, BridgeError>
    where
        F: FnOnce() -> Result<PathBuf, LocationError> + Send + 'static,
    {
        let permit = self
            .location_validation
            .try_acquire()
            .ok_or(BridgeError::LocationUnavailable)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            validate()
        })
        .await
        .map_err(|_| BridgeError::LocationUnavailable)?
        .map_err(BridgeError::from)
    }

    /// Atomically reserve a launch before shutdown and then run it off the
    /// async worker.
    ///
    /// The state and lifecycle locks are held only for the final ownership
    /// check and the explicit reservation. A click that reserves before the
    /// shutdown fence is accepted: it owns the reservation and may finish after
    /// shutdown begins. The reservation's lifecycle ticket is moved into the
    /// single-permit `spawn_blocking` worker and retained until that worker
    /// exits, even if the caller is cancelled. After the fence no new
    /// reservation is made. Shutdown therefore stays bounded even if the
    /// blocking launch stalls; this does not promise that an already accepted
    /// launch cannot physically create a process after shutdown starts.
    async fn launch_location_if_current<L>(
        &self,
        operation: LifecycleOperation,
        snapshot: &ManagedRateSnapshot,
        launcher: Arc<L>,
        directory: PathBuf,
    ) -> Result<(LocationLaunch, LifecycleOperation), BridgeError>
    where
        L: LocationLauncher + 'static,
    {
        let reservation = self.reserve_launch_if_current(snapshot)?;
        // Resolve and spawn only after both locks are released. The reservation
        // and lifecycle ticket stay with the worker until it exits.
        tokio::task::spawn_blocking(move || {
            let _reservation = reservation;
            let result = launcher.launch_directory(&directory);
            (result, operation)
        })
        .await
        .map_err(|_| BridgeError::LocationLaunchFailed)
        .and_then(|(result, operation)| {
            result
                .map(|launch| (launch, operation))
                .map_err(BridgeError::from)
        })
    }

    /// Final atomic check and reservation. Locks are released before the
    /// potentially blocking launch work starts.
    fn reserve_launch_if_current(
        &self,
        snapshot: &ManagedRateSnapshot,
    ) -> Result<LocationLaunchReservation, BridgeError> {
        // Preserve the state -> lifecycle lock order. `begin_shutdown` sets the
        // fence under the lifecycle lock and releases it before taking state.
        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        let lifecycle = self.lifecycle.lock();
        if state.shutting_down || lifecycle.shutting_down || !state.rate_write_is_current(snapshot)
        {
            return Err(BridgeError::Superseded);
        }
        let permit = self
            .location_launch
            .try_acquire()
            .ok_or(BridgeError::LocationLaunchFailed)?;
        Ok(LocationLaunchReservation { _permit: permit })
    }

    /// Apply a released control action to the active connection.
    ///
    /// Mirrors [`Self::refresh`]: the client and epoch are cloned/taken under
    /// the lock, the network round trip runs without it, and the epoch is
    /// re-checked after a successful POST so a superseding
    /// `connect`/`disconnect` cannot let a stale acknowledgment surface as
    /// success. The cached queue snapshot is never mutated here; this only
    /// forwards the action and reports that Surge acknowledged it.
    pub async fn control_download(
        &self,
        action: ControlAction,
        id: &str,
    ) -> Result<(), BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        client.control_download(action, id).await?;

        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        drop(state);
        Ok(())
    }

    /// Apply a released bulk queue action to every download on the daemon.
    ///
    /// The client and epoch are cloned under the lock and the network round
    /// trip runs without it. The epoch is re-checked after both success and
    /// failure, so a superseding `connect`/`disconnect` can never attribute a
    /// prior daemon's bulk result or error to the replacement connection.
    /// Partial and unconfirmed outcomes are still surfaced (as
    /// [`BridgeError::BulkUnconfirmed`]) so the frontend can reconcile; nothing
    /// is retried here and no queue snapshot is cached or mutated.
    pub async fn bulk_control(
        &self,
        action: BulkControlAction,
    ) -> Result<BulkControlOutcome, BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        let result = client.bulk_control(action).await;

        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        drop(state);
        result.map_err(BridgeError::from)
    }

    /// Enqueue a download on the active connection.
    ///
    /// Mirrors [`Self::control_download`]: the client and epoch are cloned/
    /// taken under the lock, the network round trip runs without it, and the
    /// epoch is re-checked after a successful POST so a superseding
    /// `connect`/`disconnect` cannot let a stale acknowledgment surface as
    /// success. The cached queue snapshot is never mutated here; this only
    /// forwards the request and reports Surge's sanitized outcome.
    pub async fn add_download(&self, url: &str) -> Result<AddOutcome, BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        let outcome = client.add_download(url).await?;

        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        drop(state);
        Ok(outcome)
    }

    /// Load the persistent automatic-hash state from the app-private managed
    /// state directory. Missing state starts with the default disabled.
    fn ensure_hash_state(&self, app: &tauri::AppHandle) -> Result<(), BridgeError> {
        let directory = resolve_managed_state_dir(app).map_err(|_| BridgeError::State)?;
        self.hashes.ensure_loaded(&directory)
    }

    /// Start or coalesce the single bounded hashing worker.
    fn trigger_hashing(&self) {
        let hashes = Arc::clone(&self.hashes);
        let Some(token) = hashes.start_worker() else {
            return;
        };
        spawn_hash_worker(
            Arc::clone(&self.state),
            Arc::clone(&self.lifecycle),
            hashes,
            token,
        );
    }

    fn current_hash_namespace(&self) -> Option<String> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.active != ActiveConnection::Managed {
            return None;
        }
        self.hashes.namespace()
    }

    fn is_active_managed(&self) -> bool {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.active == ActiveConnection::Managed
    }

    /// Capture the connection mode and epoch together so a hash command can
    /// attribute an initialization failure to the connection it actually
    /// observed, then fence its result against a later transition.
    fn connection_mode_epoch(&self) -> Result<(ActiveConnection, u64), BridgeError> {
        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        Ok((state.active, state.epoch))
    }

    fn connection_mode_epoch_is_current(&self, mode: ActiveConnection, epoch: u64) -> bool {
        match self.state.lock() {
            Ok(state) => state.active == mode && state.epoch == epoch,
            Err(poisoned) => {
                let state = poisoned.into_inner();
                state.active == mode && state.epoch == epoch
            }
        }
    }

    /// Owned managed readers must fail closed when the persistence owner is
    /// unresolved or quarantined. Manual and disconnected states keep their
    /// existing empty-row behavior because they never own automatic hashes.
    fn require_managed_hash_confirmed(&self) -> Result<(), BridgeError> {
        if !self.is_active_managed() || self.hashes.persistence_confirmed() {
            return Ok(());
        }
        Err(self
            .hashes
            .initialization_failure()
            .filter(|error| *error != BridgeError::Superseded)
            .unwrap_or(BridgeError::HashUnconfirmed))
    }

    /// Command body for `get_download_hashes` after managed initialization.
    ///
    /// The mode/epoch observed before the asynchronous verification is fenced
    /// again afterwards. A transition during the snapshot must surface as
    /// `Superseded`, never as a stale empty success (manual/offline) or a stale
    /// error from the previous connection. Manual and offline connections keep
    /// their existing empty-row behavior as long as no transition intervened.
    pub async fn get_download_hashes_checked(&self) -> Result<Vec<DownloadHashDto>, BridgeError> {
        let (mode, epoch) = self.connection_mode_epoch()?;
        self.require_managed_hash_confirmed()?;
        let result = self.verified_download_hashes().await;
        let fence_current = self.connection_mode_epoch_is_current(mode, epoch);
        let confirmed = self.require_managed_hash_confirmed();
        if !fence_current {
            return Err(BridgeError::Superseded);
        }
        confirmed?;
        result
    }

    /// Get with an initialization step whose failure is attributed to the
    /// connection mode/epoch observed before it ran.
    ///
    /// The mode/epoch is captured before `ensure`, then fenced again after the
    /// synchronous initialization and after the asynchronous verified snapshot.
    /// A transition during either phase surfaces as `Superseded`; a managed
    /// failure from the captured connection is never leaked as a replacement
    /// connection's error. Manual connections keep their existing empty-row
    /// behavior and skip the managed initialization.
    pub(crate) async fn get_download_hashes_fenced<F>(
        &self,
        ensure: F,
    ) -> Result<Vec<DownloadHashDto>, BridgeError>
    where
        F: FnOnce() -> Result<(), BridgeError>,
    {
        let (mode, epoch) = self.connection_mode_epoch()?;
        let managed = mode == ActiveConnection::Managed;
        let init = if managed { ensure() } else { Ok(()) };
        let fence_current = self.connection_mode_epoch_is_current(mode, epoch);
        match init {
            Ok(()) => {
                if !fence_current {
                    return Err(BridgeError::Superseded);
                }
                let result = self.get_download_hashes_checked().await;
                if !self.connection_mode_epoch_is_current(mode, epoch) {
                    return Err(BridgeError::Superseded);
                }
                result
            }
            Err(error) => {
                if managed && !fence_current {
                    Err(BridgeError::Superseded)
                } else if managed || self.is_active_managed() {
                    Err(error)
                } else if !fence_current {
                    Err(BridgeError::Superseded)
                } else {
                    Ok(Vec::new())
                }
            }
        }
    }

    /// Command body for `refresh_download_hashes` after managed initialization.
    pub async fn refresh_download_hashes_checked(
        &self,
    ) -> Result<Vec<DownloadHashDto>, BridgeError> {
        let (mode, epoch) = self.connection_mode_epoch()?;
        self.require_managed_hash_confirmed()?;
        self.trigger_hashing();
        let result = self.verified_download_hashes().await;
        let fence_current = self.connection_mode_epoch_is_current(mode, epoch);
        let confirmed = self.require_managed_hash_confirmed();
        if !fence_current {
            return Err(BridgeError::Superseded);
        }
        confirmed?;
        result
    }

    /// Refresh with an initialization step whose failure is attributed to the
    /// connection mode/epoch observed before it ran. A stale manual snapshot
    /// can never swallow a managed initialization failure into an empty
    /// success, and the result of the asynchronous verification is only
    /// returned while its fence is still current. A transition that happens
    /// while the snapshot is in flight must surface as `Superseded`, never as
    /// a stale empty success or a stale error from the previous connection.
    pub(crate) async fn refresh_download_hashes_fenced<F>(
        &self,
        ensure: F,
    ) -> Result<Vec<DownloadHashDto>, BridgeError>
    where
        F: FnOnce() -> Result<(), BridgeError>,
    {
        let (mode, epoch) = self.connection_mode_epoch()?;
        let managed = mode == ActiveConnection::Managed;
        let init = ensure();
        let fence_current = self.connection_mode_epoch_is_current(mode, epoch);
        match init {
            Ok(()) => {
                if !fence_current {
                    return Err(BridgeError::Superseded);
                }
                let result = self.refresh_download_hashes_checked().await;
                if !self.connection_mode_epoch_is_current(mode, epoch) {
                    return Err(BridgeError::Superseded);
                }
                result
            }
            Err(error) => {
                // Manual connections keep their best-effort empty rows, but
                // only while neither the captured nor the current connection
                // is managed. A managed failure is always surfaced.
                if self.is_active_managed() || (managed && fence_current) {
                    Err(error)
                } else if !fence_current {
                    Err(BridgeError::Superseded)
                } else {
                    Ok(Vec::new())
                }
            }
        }
    }

    /// Capture the lifecycle fence for the connection currently held by
    /// `state`. The caller must already hold the connection-state lock; the
    /// hash slot is nested inside it to preserve the state -> slot lock order.
    fn hash_write_ticket_locked(
        &self,
        state: &ConnectionState,
    ) -> Result<HashWriteTicket, BridgeError> {
        let binding = state
            .hash_daemon_binding()
            .ok_or(BridgeError::NotConnected)?;
        let slot = self.hashes.lock_slot();
        Ok(HashWriteTicket {
            epoch: state.epoch,
            binding,
            fence: slot.store.fence(),
            namespace: slot.namespace.clone(),
        })
    }

    /// Capture a ticket for an operation already pinned to `epoch`. A manual
    /// or missing daemon binding yields `None`, so the caller leaves automatic
    /// hashing disabled rather than writing an unbound record.
    fn capture_hash_write_ticket(
        &self,
        epoch: u64,
    ) -> Result<Option<HashWriteTicket>, BridgeError> {
        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        match self.hash_write_ticket_locked(&state) {
            Ok(ticket) => Ok(Some(ticket)),
            Err(BridgeError::NotConnected) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Enqueue a download and, only for a confirmed accepted id, persist the
    /// per-download hash intent before triggering the worker.
    ///
    /// The intent is bound to the stable per-owned-queue namespace and to the
    /// authoritative `added_at`/queue-fingerprint provenance read back for the
    /// acknowledged id. If that association cannot be proven, nothing is
    /// recorded and `hash_recorded` is false rather than guessing.
    /// A crash between acceptance and the intent write can lose the intent; no
    /// retry or exactly-once claim is made.
    pub async fn add_download_with_options(
        &self,
        url: &str,
        hash: bool,
    ) -> Result<AddReceiptDto, BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        let receipt = client.add_download_receipt(url).await?;

        // Bind the eventual intent write to the exact daemon lifecycle that
        // acknowledged the add. The ticket is captured before the status read
        // and revalidated atomically with the mutation, so a reconnect during
        // that read can never install an old intent into the new lifecycle.
        let ticket = self.capture_hash_write_ticket(epoch)?;

        let mut hash_recorded = None;
        if hash {
            let Some(id) = receipt.id.as_deref() else {
                self.trigger_hashing();
                return Ok(AddReceiptDto {
                    status: receipt.outcome,
                    id: receipt.id,
                    hash_recorded,
                });
            };
            let recorded = match ticket {
                Some(ticket) => match client.get_download(id).await {
                    Ok(download) => match ticket.namespace.clone() {
                        Some(namespace) => {
                            let provenance = crate::hash_state::HashProvenance {
                                namespace,
                                added_at: download.added_at,
                                fingerprint: crate::hash_state::queue_fingerprint(
                                    &download.id,
                                    download.added_at,
                                    &download.url,
                                ),
                            };
                            mutate_hash_at_ticket(&self.state, &self.hashes, &ticket, |slot| {
                                if !slot.loaded {
                                    return Err(BridgeError::State);
                                }
                                slot.store
                                    .record_intents_with(
                                        &[HashIntent {
                                            id: id.to_owned(),
                                            enabled: true,
                                        }],
                                        &[(id.to_owned(), provenance)],
                                    )
                                    .map(|_| ())
                                    .map_err(|_| BridgeError::State)
                            })
                            .is_ok()
                        }
                        None => false,
                    },
                    Err(_) => false,
                },
                None => false,
            };
            hash_recorded = Some(recorded);
        }
        self.trigger_hashing();
        Ok(AddReceiptDto {
            status: receipt.outcome,
            id: receipt.id,
            hash_recorded,
        })
    }

    pub fn automatic_hash_default(&self) -> Result<bool, BridgeError> {
        self.hashes.default_enabled()
    }

    pub fn set_automatic_hash_default(&self, enabled: bool) -> Result<bool, BridgeError> {
        self.hashes.set_default_enabled(enabled)
    }

    /// Bind UI toggles to the current daemon and the authoritative queue
    /// records. Enabled ids that cannot be matched uniquely are retained only
    /// as fail-closed rows (the worker reconciles them to stale), never guessed.
    pub(crate) async fn record_hash_intents(
        &self,
        intents: &[HashIntent],
    ) -> Result<Vec<DownloadHashDto>, BridgeError> {
        // Capture the lifecycle ticket before any cohort or network read so
        // the whole transaction is bound to one connection. Only records that
        // existed before this snapshot request may be reconciled by its
        // response. Intents recorded by this call are added to the cohort
        // explicitly below so they can still fail closed.
        let (snapshot, ticket) = {
            let state = self.state.lock().map_err(|_| BridgeError::State)?;
            if state.shutting_down {
                return Err(BridgeError::Superseded);
            }
            let snapshot = state.managed_rate_snapshot()?;
            let ticket = self.hash_write_ticket_locked(&state)?;
            (snapshot, ticket)
        };
        let preexisting = self.hashes.reconcile_cohort();
        let downloads = snapshot.client.refresh().await?;
        let namespace = ticket.namespace.clone();
        let written = mutate_hash_at_ticket(&self.state, &self.hashes, &ticket, |slot| {
            if !slot.loaded {
                return Err(BridgeError::State);
            }
            let mut provenance = Vec::new();
            if let Some(namespace) = namespace.as_deref() {
                for intent in intents.iter().filter(|intent| intent.enabled) {
                    let mut matches = downloads.iter().filter(|download| download.id == intent.id);
                    let Some(download) = matches.next() else {
                        continue;
                    };
                    if matches.next().is_some() {
                        continue;
                    }
                    provenance.push((
                        intent.id.clone(),
                        crate::hash_state::HashProvenance {
                            namespace: namespace.to_owned(),
                            added_at: download.added_at,
                            fingerprint: crate::hash_state::queue_fingerprint(
                                &download.id,
                                download.added_at,
                                &download.url,
                            ),
                        },
                    ));
                }
            }
            slot.store
                .record_intents_with(intents, &provenance)
                .map_err(|_| BridgeError::State)
        })?;
        #[cfg(test)]
        self.record_intents_pause.wait();
        // Unprovable enabled intents must not remain pending forever, while a
        // concurrent record must not be staled by this older snapshot. The
        // write returned its exact identities+generations, so a replacement
        // recorded after the write is never included in this cohort. The
        // reconcile is fenced by the same lifecycle ticket: if a reconnect
        // happened while the snapshot was in flight, it is discarded instead
        // of mutating the replacement connection's records.
        if let Some(namespace) = namespace {
            let mut cohort = preexisting;
            cohort.extend(written);
            let items: Vec<_> = downloads
                .iter()
                .map(crate::hash_state::QueueItemIdentity::from_download)
                .collect();
            let _ = mutate_hash_at_ticket(&self.state, &self.hashes, &ticket, |slot| {
                if !slot.loaded {
                    return Err(BridgeError::State);
                }
                slot.store
                    .reconcile_cohort_and_candidates(&namespace, &items, &cohort)
                    .map(|_| ())
                    .map_err(|_| BridgeError::State)
            });
        }
        self.trigger_hashing();
        // Never return persisted complete digests without the verified serve
        // path; this also covers existing rows the caller did not mention.
        self.verified_download_hashes().await
    }

    pub fn download_hashes(&self) -> Vec<DownloadHashDto> {
        let namespace = self.current_hash_namespace();
        if !self.hashes.persistence_confirmed() {
            return self.hashes.visible_unconfirmed(namespace.as_deref());
        }
        self.hashes.visible(namespace.as_deref())
    }

    /// Serve only digests whose authoritative queue record and exact file
    /// identity still match. Any missing, replaced, modified or ambiguous
    /// record is invalidated before the rows are returned. If the daemon
    /// cannot be queried, the previous digests are withheld and a sanitized
    /// error is returned instead of a misleading empty success. A transition
    /// that races the verification surfaces as `Superseded` for both success
    /// and failure paths. Manual/offline connections keep their positive
    /// empty-row behavior because they never own automatic hashes.
    pub async fn verified_download_hashes(&self) -> Result<Vec<DownloadHashDto>, BridgeError> {
        let early_namespace = self.current_hash_namespace();
        if !self.hashes.persistence_confirmed() {
            return Ok(self.hashes.visible_unconfirmed(early_namespace.as_deref()));
        }
        let (mode, epoch) = self.connection_mode_epoch()?;
        let snapshot = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.managed_rate_snapshot()
        };
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if !self.connection_mode_epoch_is_current(mode, epoch) {
                    return Err(BridgeError::Superseded);
                }
                // Manual and disconnected readers retain the existing
                // best-effort empty rows; only an owned managed reader must
                // surface the failed verification.
                if mode == ActiveConnection::Managed {
                    return Err(error);
                }
                return Ok(Vec::new());
            }
        };
        // Capture the exact completed cohort and fence before the network
        // request. A completion persisted while the older `/list` is still in
        // flight must not be reconciled against (or invalidated by) it.
        let Some(namespace) = self.hashes.namespace() else {
            return Ok(Vec::new());
        };
        let fence = self.hashes.fence();
        let completed = self.hashes.completed_for(&namespace);

        let downloads = match snapshot.client.refresh().await {
            Ok(downloads) => downloads,
            Err(error) => {
                if !self.connection_mode_epoch_is_current(mode, epoch) {
                    return Err(BridgeError::Superseded);
                }
                return Err(BridgeError::from(error));
            }
        };
        #[cfg(test)]
        self.verified_list_pause.wait();
        let current = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.rate_write_is_current(&snapshot)
        };
        if !current {
            if !self.connection_mode_epoch_is_current(mode, epoch) {
                return Err(BridgeError::Superseded);
            }
            // The mode/epoch is still the one this read observed, so the
            // owned managed process exited (or its snapshot is no longer
            // valid) after the successful `/list`. An empty success here
            // would falsely assert that no hash intent exists.
            return Err(BridgeError::NotConnected);
        }
        let mut invalid = Vec::new();
        let mut verified = Vec::new();
        for record in completed {
            let mut matches = downloads.iter().filter(|download| download.id == record.id);
            let Some(download) = matches.next() else {
                invalid.push(record);
                continue;
            };
            if matches.next().is_some() {
                invalid.push(record);
                continue;
            }
            let fingerprint = crate::hash_state::queue_fingerprint(
                &download.id,
                download.added_at,
                &download.url,
            );
            let record_is_current =
                download.added_at == record.added_at && fingerprint == record.fingerprint;
            if !record_is_current {
                invalid.push(record);
                continue;
            }
            let decision = match automatic_hash::completion_state(&snapshot.client, download).await
            {
                Ok(decision) => decision,
                Err(_) => {
                    if !self.connection_mode_epoch_is_current(mode, epoch) {
                        return Err(BridgeError::Superseded);
                    }
                    return Err(BridgeError::InvalidResponse);
                }
            };
            match decision {
                automatic_hash::CompletionDecision::Ready => {}
                automatic_hash::CompletionDecision::Pending => continue,
                automatic_hash::CompletionDecision::Failed
                | automatic_hash::CompletionDecision::Unavailable => {
                    invalid.push(record);
                    continue;
                }
            }
            let Some(path) = download.dest_path.clone().filter(|path| !path.is_empty()) else {
                invalid.push(record);
                continue;
            };
            let expected = crate::file_hash::FileIdentity {
                dev: record.file_dev,
                ino: record.file_ino,
                size: record.size,
                mtime_sec: record.mtime_sec,
                mtime_nsec: record.mtime_nsec,
                ctime_sec: record.ctime_sec,
                ctime_nsec: record.ctime_nsec,
            };
            let verified_file = tokio::task::spawn_blocking(move || {
                crate::file_hash::verify_file_identity(&path, &expected)
            })
            .await;
            if matches!(verified_file, Ok(Ok(()))) {
                verified.push(record);
            } else {
                invalid.push(record);
            }
        }
        for record in invalid {
            match self.hashes.invalidate_completed(&record, fence) {
                Ok(true) => {}
                Ok(false) => {
                    // `false` is not proof that there was no hash intent.
                    // Either the lifecycle fence moved while this response
                    // was in flight, or the exact record changed/was already
                    // invalidated. A moved lifecycle is superseded; anything
                    // else is revalidated by the final exact-set projection,
                    // so a stale digest still cannot be served.
                    if self.hashes.fence() != fence
                        || !self.connection_mode_epoch_is_current(mode, epoch)
                    {
                        return Err(BridgeError::Superseded);
                    }
                }
                Err(error) => {
                    if self.hashes.fence() != fence
                        || !self.connection_mode_epoch_is_current(mode, epoch)
                    {
                        return Err(BridgeError::Superseded);
                    }
                    return Err(error);
                }
            }
        }
        #[cfg(test)]
        self.verified_pause.wait();
        // Publish under the connection-state lock so the epoch cannot change
        // between the final check and the exact verified-set projection.
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.shutting_down {
            return Err(BridgeError::Superseded);
        }
        if !state.rate_write_is_current(&snapshot) {
            if state.epoch != epoch || state.active != mode {
                return Err(BridgeError::Superseded);
            }
            // The connection is still this read's lifecycle, but its owned
            // process exited before publication. Never publish an empty
            // success for an unverifiable managed snapshot.
            return Err(BridgeError::NotConnected);
        }
        let rows = self.hashes.visible_verified(&namespace, &verified, fence);
        drop(state);
        rows.ok_or(BridgeError::Superseded)
    }

    pub fn refresh_download_hashes(&self) -> Vec<DownloadHashDto> {
        self.trigger_hashing();
        self.download_hashes()
    }

    /// Read the active connection's global download limit in bytes/sec.
    ///
    /// Read-only access is allowed for both owned-managed and manual
    /// connections. The pinned client and epoch are cloned under the lock, the
    /// network round trip runs without it, and the epoch is re-checked after
    /// both success and failure so an old connection's error can never surface
    /// as the replacement connection's state.
    pub async fn get_global_rate_limit(&self) -> Result<i64, BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        let result = client.get_global_rate_limit().await;

        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        drop(state);
        result.map_err(BridgeError::from)
    }

    /// Apply and confirm an owned managed daemon's global download limit.
    ///
    /// Only a currently owned managed daemon may be written. A manually
    /// connected server cannot be fenced against a delayed request, so writes
    /// to it are refused with [`BridgeError::NotConnected`] (reads still work).
    ///
    /// Every setter shares one manager-level async gate, acquired before the
    /// request starts. The intended managed client and epoch are captured
    /// before waiting on that gate, so a queued caller can never retarget a
    /// replacement daemon that becomes active while it waits. After acquiring
    /// the gate, the same epoch, managed identity, and shutdown state are
    /// revalidated immediately before dispatch.
    ///
    /// A lifecycle operation ticket is registered before the gate wait and
    /// moved into the background task, so app shutdown cannot finish draining
    /// while a rate write (or its queued predecessor) is still outstanding.
    /// The task keeps the gate and ticket until the HTTP request, epoch
    /// recheck, and poison decision all finish.
    ///
    /// Invalid input is rejected locally (definitively pre-request). Any
    /// ambiguous write outcome returns [`BridgeError::RateUnconfirmed`] and
    /// permanently poisons this manager's writer for the rest of the process
    /// lifetime; later writes (including after disconnect/reconnect) return
    /// `RateUnconfirmed` without sending anything. Only an app restart, which
    /// also retires the owned daemon, starts fresh.
    pub async fn set_global_rate_limit(&self, rate: i64) -> Result<i64, BridgeError> {
        if !(0..=MAX_RATE_LIMIT_BPS).contains(&rate) {
            return Err(BridgeError::InvalidResponse);
        }

        if self.rate_writer.poisoned.load(Ordering::SeqCst) {
            return Err(BridgeError::RateUnconfirmed);
        }

        // Capture the intended target before waiting on the writer gate. A
        // queued caller must keep operating on the daemon it observed, not on
        // a replacement that becomes active while it is queued.
        let snapshot = {
            let state = self.state.lock().map_err(|_| BridgeError::State)?;
            if state.shutting_down {
                return Err(BridgeError::Superseded);
            }
            state.managed_rate_snapshot()?
        };

        // Register before any async work so app shutdown cannot finish
        // draining while this write is still queued or in flight.
        let lifecycle = self
            .lifecycle
            .begin_operation()
            .ok_or(BridgeError::Superseded)?;

        let writer = Arc::clone(&self.rate_writer);
        let gate = Arc::clone(&writer.gate).lock_owned().await;
        if writer.poisoned.load(Ordering::SeqCst) {
            return Err(BridgeError::RateUnconfirmed);
        }

        let state = Arc::clone(&self.state);
        let client = Arc::clone(&snapshot.client);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        // The background task owns the gate, lifecycle ticket, and a
        // fail-closed poison guard. Caller cancellation only drops
        // `result_rx`; the revalidation, HTTP request, epoch recheck, and
        // poison decision still finish, and shutdown keeps waiting for the
        // lifecycle ticket.
        tokio::spawn(async move {
            let _gate = gate;
            let operation_lifecycle = lifecycle;
            let mut poison = RateWritePoison::new(Arc::clone(&writer));

            // Revalidate immediately before dispatch. The target may have
            // been retired, replaced, or the app may have begun shutting down
            // while this caller waited for the gate; in that case this write
            // must never be sent.
            let dispatch = if operation_lifecycle.is_shutting_down() {
                Err(BridgeError::Superseded)
            } else {
                match state.lock() {
                    Ok(state) if !state.shutting_down && state.rate_write_is_current(&snapshot) => {
                        Ok(())
                    }
                    Ok(_) => Err(BridgeError::Superseded),
                    Err(_) => Err(BridgeError::State),
                }
            };
            if let Err(error) = dispatch {
                poison.disarm();
                let _ = result_tx.send(Err(error));
                return;
            }

            let operation = client.set_global_rate_limit(rate).await;
            if matches!(&operation, Err(ConnectError::RateUnconfirmed)) {
                poison.poison_now();
            } else {
                poison.disarm();
            }

            let result = {
                let state = match state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        let result = if matches!(&operation, Err(ConnectError::RateUnconfirmed)) {
                            Err(BridgeError::RateUnconfirmed)
                        } else {
                            Err(BridgeError::State)
                        };
                        let _ = result_tx.send(result);
                        return;
                    }
                };
                // Recheck after both Ok and Err. A stale operation must not
                // leak its old success or old error as the new connection's
                // state, but its poison decision above still stands. A stale
                // ambiguous outcome is global current state (the writer is
                // already poisoned), not a stale connection error, so it
                // surfaces as RateUnconfirmed without any server detail.
                if !state.rate_write_is_current(&snapshot) {
                    if matches!(&operation, Err(ConnectError::RateUnconfirmed)) {
                        Err(BridgeError::RateUnconfirmed)
                    } else {
                        Err(BridgeError::Superseded)
                    }
                } else {
                    operation.map_err(BridgeError::from)
                }
            };
            let _ = result_tx.send(result);
        });

        match result_rx.await {
            Ok(result) => result,
            Err(_) => {
                if self.rate_writer.poisoned.load(Ordering::SeqCst) {
                    Err(BridgeError::RateUnconfirmed)
                } else {
                    Err(BridgeError::State)
                }
            }
        }
    }

    /// Read the daemon's saved max-concurrent-downloads value. Reads are
    /// allowed on manual and managed connections; writes are managed-only.
    pub async fn get_max_concurrent_downloads(&self) -> Result<i64, BridgeError> {
        let (client, epoch) = self.acquire_active()?;
        let result = client.get_max_concurrent_downloads().await;

        let state = self.state.lock().map_err(|_| BridgeError::State)?;
        if state.epoch != epoch {
            return Err(BridgeError::Superseded);
        }
        drop(state);
        result.map_err(BridgeError::from)
    }

    /// Save the daemon's `network.max_concurrent_downloads` on the owned
    /// bundled daemon only.
    ///
    /// `before` is the value the UI loaded; the daemon rejects a stale edit
    /// with 409 instead of overwriting a concurrent change. Even a same-value
    /// write is sent so an externally changed daemon value surfaces as a
    /// conflict. The app never restarts the daemon. Only a confirmed
    /// 200/readback is success; an
    /// ambiguous outcome returns [`BridgeError::SettingsUnconfirmed`] and
    /// poisons this settings writer until the app restarts, so no later write
    /// is sent and nothing is retried silently. This gate is independent of
    /// the global-rate writer.
    pub async fn set_max_concurrent_downloads(
        &self,
        before: i64,
        value: i64,
    ) -> Result<i64, BridgeError> {
        let range = MAX_CONCURRENT_DOWNLOADS_MIN..=MAX_CONCURRENT_DOWNLOADS_MAX;
        if !range.contains(&before) || !range.contains(&value) {
            return Err(BridgeError::InvalidResponse);
        }
        if self.settings_writer.poisoned.load(Ordering::SeqCst) {
            return Err(BridgeError::SettingsUnconfirmed);
        }

        let snapshot = {
            let state = self.state.lock().map_err(|_| BridgeError::State)?;
            if state.shutting_down {
                return Err(BridgeError::Superseded);
            }
            state.managed_settings_snapshot()?
        };

        let lifecycle = self
            .lifecycle
            .begin_operation()
            .ok_or(BridgeError::Superseded)?;

        let writer = Arc::clone(&self.settings_writer);
        let gate = Arc::clone(&writer.gate).lock_owned().await;
        if writer.poisoned.load(Ordering::SeqCst) {
            return Err(BridgeError::SettingsUnconfirmed);
        }

        let state = Arc::clone(&self.state);
        let client = Arc::clone(&snapshot.client);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let _gate = gate;
            let operation_lifecycle = lifecycle;
            let mut poison = SettingsWritePoison::new(Arc::clone(&writer));

            let dispatch = if operation_lifecycle.is_shutting_down() {
                Err(BridgeError::Superseded)
            } else {
                match state.lock() {
                    Ok(state) if !state.shutting_down && state.rate_write_is_current(&snapshot) => {
                        Ok(())
                    }
                    Ok(_) => Err(BridgeError::Superseded),
                    Err(_) => Err(BridgeError::State),
                }
            };
            if let Err(error) = dispatch {
                poison.disarm();
                let _ = result_tx.send(Err(error));
                return;
            }

            let operation = client.set_max_concurrent_downloads(before, value).await;
            if matches!(&operation, Err(ConnectError::SettingsUnconfirmed)) {
                poison.poison_now();
            } else {
                poison.disarm();
            }

            let result = {
                let state = match state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        let result = if matches!(&operation, Err(ConnectError::SettingsUnconfirmed))
                        {
                            Err(BridgeError::SettingsUnconfirmed)
                        } else {
                            Err(BridgeError::State)
                        };
                        let _ = result_tx.send(result);
                        return;
                    }
                };
                if !state.rate_write_is_current(&snapshot) {
                    if matches!(&operation, Err(ConnectError::SettingsUnconfirmed)) {
                        Err(BridgeError::SettingsUnconfirmed)
                    } else {
                        Err(BridgeError::Superseded)
                    }
                } else {
                    operation.map_err(BridgeError::from)
                }
            };
            let _ = result_tx.send(result);
        });

        match result_rx.await {
            Ok(result) => result,
            Err(_) => {
                if self.settings_writer.poisoned.load(Ordering::SeqCst) {
                    Err(BridgeError::SettingsUnconfirmed)
                } else {
                    Err(BridgeError::State)
                }
            }
        }
    }

    #[cfg(test)]
    fn install_test_managed(&self, client: Arc<SurgeClient>, pid: u32) -> u64 {
        let _lifecycle = self.hashes.lock_lifecycle();
        self.hashes.cancel_worker();
        let mut state = self.state.lock().expect("test state lock");
        let mut slot = self.hashes.lock_slot();
        slot.store.fence_in_flight();
        state.epoch = state.epoch.wrapping_add(1);
        state.manual = None;
        state.test_managed = Some(TestManagedConnection {
            client,
            pid,
            exited: false,
        });
        state.hash_connection_id = Some(new_hash_connection_id());
        state.active = ActiveConnection::Managed;
        state.epoch
    }

    /// Simulate the owned managed child exiting without a connection
    /// transition. The epoch and hash fence deliberately stay unchanged, so
    /// readers must not mistake the loss for a lifecycle supersession.
    #[cfg(all(test, target_os = "linux"))]
    fn mark_test_managed_exited(&self) {
        let mut state = self.state.lock().expect("test state lock");
        if let Some(test) = state.test_managed.as_mut() {
            test.exited = true;
        }
    }

    /// Drop the active connection and fence off any in-flight `connect`.
    ///
    /// Any owned managed daemon is retired as well, outside the lock, with the
    /// bounded graceful shutdown. Keeping it alive but unreachable would leak a
    /// live child and block every later explicit restart. In-flight operations
    /// only hold an `Arc<SurgeClient>` clone, so they observe the new epoch (or
    /// a network error) without stranding state.
    pub fn disconnect(&self) {
        // Abort the stream before any state change so no read survives the
        // disconnect. Hash worker cancellation happens atomically with the
        // fence below.
        self.progress.stop();
        // The shutdown path owns lifecycle fencing once it starts; it also
        // takes any remaining managed server itself. Registering here keeps a
        // concurrent new start from binding the fixed port mid-retirement.
        let lifecycle = match self.lifecycle.begin_operation() {
            Some(guard) => guard,
            None => return,
        };
        // Recover from a poisoned lock: the only mutation is a full reset, so a
        // half-written prior state is safely overwritten.
        let (server, registered) = {
            let _lifecycle = self.hashes.lock_lifecycle();
            self.hashes.cancel_worker();
            #[cfg(test)]
            self.lifecycle_fence_pause.wait();
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            let mut slot = self.hashes.lock_slot();
            slot.store.fence_in_flight();
            state.epoch = state.epoch.wrapping_add(1);
            state.clear_active();
            let server = state.managed.take();
            let registered = if server.is_some() {
                state.mark_retirement();
                true
            } else {
                false
            };
            (server, registered)
        };
        let retirement = if registered {
            let owner = server
                .as_ref()
                .map(|server| server.owner_lifetime())
                .expect("registered retirement must own a server");
            Some(self.retirement_guard_for(owner))
        } else {
            None
        };
        if let Some(server) = server {
            let _ = server.shutdown_blocking();
        }
        drop(retirement);
        drop(lifecycle);
    }
}

fn project(downloads: &[Download]) -> Vec<QueueItemDto> {
    downloads.iter().map(project_item).collect()
}

fn project_item(download: &Download) -> QueueItemDto {
    let bytes_total = clamp_index(download.total_size);
    let mut bytes_done = clamp_index(download.downloaded);
    if bytes_total > 0 && bytes_done > bytes_total {
        bytes_done = bytes_total;
    }
    QueueItemDto {
        id: download.id.clone(),
        name: download.filename.clone(),
        status: map_status(&download.status).to_string(),
        phase: map_phase(download.phase.as_deref(), &download.status),
        processing: download.processing,
        processed: download.processed,
        hook_failed: download
            .hook_error
            .as_deref()
            .is_some_and(|error| !error.trim().is_empty()),
        bytes_done,
        bytes_total,
        speed_bps: sanitize_speed(download.speed),
        added_at: clamp_index(download.added_at),
    }
}

/// Squeeze a signed `int64` metric into `[0, JS_MAX_SAFE_INTEGER]`.
fn clamp_index(value: i64) -> i64 {
    value.clamp(0, JS_MAX_SAFE_INTEGER)
}

/// Non-finite rates collapse to zero; finite rates are clamped into
/// `[0, JS_MAX_SAFE_INTEGER]`.
fn sanitize_speed(speed: f64) -> f64 {
    if speed.is_finite() {
        speed.clamp(0.0, JS_MAX_SAFE_INTEGER as f64)
    } else {
        0.0
    }
}

/// Explicit Surge status projection; anything unrecognized becomes `unknown`.
fn map_status(status: &str) -> &'static str {
    match status {
        "downloading" | "verifying" => "active",
        "queued" => "queued",
        "paused" => "paused",
        "completed" => "completed",
        "error" => "failed",
        "pausing" => "pausing",
        _ => "unknown",
    }
}

/// Sanitize the v0.13.0 `phase` field. Older daemons omit it, so fall back to
/// the already-normalized status (matching the released CLI). Unknown values
/// become `unknown` rather than crossing IPC verbatim.
fn map_phase(phase: Option<&str>, status: &str) -> String {
    match phase.map(str::trim).filter(|phase| !phase.is_empty()) {
        Some("queued") => "queued".to_string(),
        Some("retrying") => "retrying".to_string(),
        Some("downloading") => "downloading".to_string(),
        Some("verifying") => "verifying".to_string(),
        Some("finalizing") => "finalizing".to_string(),
        Some(_) => "unknown".to_string(),
        None => map_status(status).to_string(),
    }
}

/// Bundled daemon resource name for this exact target.
///
/// Only the platform-specific binary is ever selected. Unknown target
/// platforms and architectures fail closed before any resolver, spawn, or
/// filesystem activity.
fn managed_resource_file_name() -> Result<&'static str, ManagedError> {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        return Ok("surge-x86_64-unknown-linux-gnu");
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        return Ok("surge-aarch64-unknown-linux-gnu");
    }
    #[cfg(all(windows, target_arch = "x86_64"))]
    {
        return Ok("surge-x86_64-pc-windows-msvc.exe");
    }
    #[cfg(all(windows, target_arch = "aarch64"))]
    {
        return Ok("surge-aarch64-pc-windows-msvc.exe");
    }
    #[allow(unreachable_code)]
    Err(ManagedError::UnsupportedPlatform)
}

/// Resolve only the platform-specific bundled daemon through Tauri's resource
/// resolver and validate its identity before it is ever spawned.
fn resolve_managed_binary(app: &tauri::AppHandle) -> Result<PathBuf, ManagedError> {
    let name = managed_resource_file_name()?;
    let relative = format!("resources/{name}");
    let root = app
        .path()
        .resource_dir()
        .map_err(|_| ManagedError::Resource)?;
    let resolved = app
        .path()
        .resolve(&relative, BaseDirectory::Resource)
        .map_err(|_| ManagedError::Resource)?;
    validate_managed_binary(&resolved, &root, name)
}

fn validate_managed_binary(
    resolved: &Path,
    resource_root: &Path,
    expected_name: &str,
) -> Result<PathBuf, ManagedError> {
    if !resolved.is_absolute() || !resource_root.is_absolute() {
        return Err(ManagedError::Resource);
    }
    if resolved.file_name() != Some(OsStr::new(expected_name)) {
        return Err(ManagedError::Resource);
    }

    let metadata = std::fs::symlink_metadata(resolved).map_err(|_| ManagedError::Resource)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(ManagedError::Resource);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(ManagedError::Resource);
        }
    }

    // The resource must stay inside the resolver's resource root even after
    // symlink resolution (the final component itself was already rejected as
    // a symlink above).
    let canonical_root = resource_root
        .canonicalize()
        .map_err(|_| ManagedError::Resource)?;
    let canonical_binary = resolved
        .canonicalize()
        .map_err(|_| ManagedError::Resource)?;
    if !canonical_binary.starts_with(&canonical_root) {
        return Err(ManagedError::Resource);
    }

    Ok(resolved.to_path_buf())
}

/// Derive the app-owned persistent state directory from Tauri app paths.
fn resolve_managed_state_dir(app: &tauri::AppHandle) -> Result<PathBuf, ManagedError> {
    let local_data = app
        .path()
        .app_local_data_dir()
        .map_err(|_| ManagedError::Paths)?;
    if !local_data.is_absolute() {
        return Err(ManagedError::Paths);
    }
    ensure_app_data_dir(&local_data)?;
    let state_dir = local_data.join("managed-surge");
    if !state_dir.is_absolute() {
        return Err(ManagedError::Paths);
    }
    Ok(state_dir)
}

/// Ensure the Tauri app-local-data directory exists without recursively
/// creating unverified ancestors. The final daemon state child is created and
/// verified by `ManagedStateDir::prepare`.
fn ensure_app_data_dir(dir: &Path) -> Result<(), ManagedError> {
    match std::fs::symlink_metadata(dir) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ManagedError::Paths);
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(dir).map_err(|_| ManagedError::Paths)?;
            let metadata = std::fs::symlink_metadata(dir).map_err(|_| ManagedError::Paths)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ManagedError::Paths);
            }
            Ok(())
        }
        Err(_) => Err(ManagedError::Paths),
    }
}

/// Poll an async lifecycle step while remaining responsive to app shutdown.
/// Dropping the returned future (on `None`) is what releases its owned child.
async fn await_lifecycle<F>(lifecycle: &ManagedLifecycle, future: F) -> Option<F::Output>
where
    F: Future,
{
    let mut future = Box::pin(future);
    loop {
        if lifecycle.is_shutting_down() {
            return None;
        }
        if let Ok(output) = tokio::time::timeout(LIFECYCLE_POLL_INTERVAL, &mut future).await {
            return Some(output);
        }
    }
}

/// Claim the single managed-start slot, start the owned daemon, and install
/// it only if the reserved connection epoch is still current.
async fn start_managed_at(
    manager: &ConnectionManager,
    binary: &Path,
    state_dir: &Path,
    home_dir: &Path,
) -> Result<Vec<QueueItemDto>, ManagedError> {
    start_managed_at_with_owner(
        manager,
        binary,
        state_dir,
        home_dir,
        DEFAULT_STARTUP_TIMEOUT,
        None,
    )
    .await
}

async fn start_managed_at_with_owner(
    manager: &ConnectionManager,
    binary: &Path,
    state_dir: &Path,
    home_dir: &Path,
    timeout: Duration,
    cleanup_gate: Option<crate::managed_server::OwnerCleanupGate>,
) -> Result<Vec<QueueItemDto>, ManagedError> {
    // Holds app-shutdown fencing for the whole start/retire window so
    // `shutdown_managed` can wait for this detached operation. This same
    // operation is handed to the owner-cleanup watcher if the start future is
    // cancelled, so app drain never observes a gap on mere cancellation.
    let lifecycle = match manager.lifecycle.begin_operation() {
        Some(guard) => guard,
        None => return Err(ManagedError::ShuttingDown),
    };
    let (ticket, retired) = manager.begin_managed_start()?;
    let owner = OwnerLifetime::new();
    #[cfg(test)]
    if let Some(gate) = cleanup_gate.clone() {
        owner.set_cleanup_gate(gate);
    }
    #[cfg(not(test))]
    let _ = cleanup_gate;
    // The guard replaces the claim with a retirement fence that remains until
    // the real owner thread exits/reaps the child. Install it before retiring
    // any old daemon so cancellation cannot leave `starting` stuck or clear
    // lifecycle accounting early.
    let claim = ManagedStartGuard::new(manager, ticket, owner.clone(), lifecycle);
    let own_retirement = if retired.is_some() {
        manager.register_retirement();
        1
    } else {
        0
    };
    let retirement_owner = retired.as_ref().map(|server| server.owner_lifetime());
    let retirement = match retirement_owner {
        Some(owner) if own_retirement > 0 => Some(manager.retirement_guard_for(owner)),
        _ => None,
    };
    let old = retired;
    // A prior detached retirement (connect/disconnect/newer start) may still
    // own the fixed port. Wait within a fixed bound, or return an explicit
    // retry, before spawning a competing child.
    manager
        .wait_for_prior_retirements(own_retirement, RETIREMENT_WAIT_TIMEOUT)
        .await?;
    if let Some(old) = old {
        // Retire the superseded/crashed daemon outside the lock before a new
        // child can race for the same fixed port.
        let retired = await_lifecycle(&manager.lifecycle, old.shutdown()).await;
        if manager.lifecycle.is_shutting_down() || retired.is_none() {
            return Err(ManagedError::ShuttingDown);
        }
        if retired == Some(false) {
            // Bounded shutdown did not confirm the old child was reaped. Do
            // not spawn a competing child; the still-armed retirement guard
            // keeps the fence and app-drain accounting visible.
            return Err(ManagedError::Retry);
        }
    }
    drop(retirement);
    // The started server owns its child, so dropping it (on cancellation or
    // supersession) retires exactly that child and never a bare/unpinned pid.
    let started = await_lifecycle(
        &manager.lifecycle,
        ManagedServer::start_cancellable_tracked(
            binary,
            state_dir,
            home_dir,
            timeout,
            manager.lifecycle.cancel_flag(),
            owner.clone(),
        ),
    )
    .await;
    // Prefer the explicit shutdown result even if the child failed
    // concurrently because the owner observed the cancellation flag.
    if manager.lifecycle.is_shutting_down() {
        return Err(ManagedError::ShuttingDown);
    }
    let server = match started {
        Some(Ok(server)) => server,
        Some(Err(error)) => return Err(ManagedError::from(error.stage())),
        None => return Err(ManagedError::ShuttingDown),
    };
    let result = manager.finish_managed_start(ticket, server);
    claim.disarm();
    result
}

/// Start the single bundled managed Surge daemon and bind the existing
/// `ConnectionManager` to its pinned client.
///
/// The frontend supplies no endpoint, token, certificate, path, or process
/// id. Resource and state paths are derived exclusively from Tauri's resource
/// resolver, app-local-data directory, and real user home. The returned queue
/// rows are the same sanitized DTO as the manual connect command; credentials
/// never cross IPC.
#[tauri::command]
pub async fn start_managed_surge(
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<Vec<QueueItemDto>, ManagedError> {
    let binary = resolve_managed_binary(&app)?;
    let home_dir = app.path().home_dir().map_err(|_| ManagedError::Paths)?;
    if !home_dir.is_absolute() || !home_dir.is_dir() {
        return Err(ManagedError::Paths);
    }
    let state_dir = resolve_managed_state_dir(&app)?;
    let result = start_managed_at(&state, &binary, &state_dir, &home_dir).await;
    if result.is_ok() && state.ensure_hash_state(&app).is_ok() {
        state.trigger_hashing();
    }
    result
}

#[tauri::command]
pub async fn connect_surge(
    endpoint: String,
    token: String,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<Vec<QueueItemDto>, BridgeError> {
    state.connect(&endpoint, &token).await
}

#[tauri::command]
pub async fn list_downloads(
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<Vec<QueueItemDto>, BridgeError> {
    let result = state.list_downloads().await;
    if result.is_ok() && state.ensure_hash_state(&app).is_ok() {
        state.trigger_hashing();
    }
    result
}

/// Authoritative `/list` snapshot plus the stream cursor for merge fencing.
#[tauri::command]
pub async fn list_downloads_snapshot(
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<QueueSnapshotDto, BridgeError> {
    let result = state.list_downloads_snapshot().await;
    if result.is_ok() && state.ensure_hash_state(&app).is_ok() {
        state.trigger_hashing();
    }
    result
}

/// Start the authenticated `/events` progress stream for the current
/// connection. `subscription` is the frontend's monotonic epoch; stale starts
/// are ignored by the stream manager.
#[tauri::command]
pub async fn start_progress_stream(
    subscription: u64,
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<(), BridgeError> {
    state.start_progress_stream(subscription, app)
}

/// Open the containing folder of a completed download on the owned managed
/// daemon. Accepts only the server-owned download id; no path crosses IPC.
#[tauri::command]
pub async fn open_download_location(
    id: String,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<(), BridgeError> {
    state.open_download_location(&id).await
}

/// Read the active global download limit in integer bytes/sec.
#[tauri::command]
pub async fn get_global_rate_limit(
    state: tauri::State<'_, ConnectionManager>,
) -> Result<i64, BridgeError> {
    state.get_global_rate_limit().await
}

/// Apply a nonnegative global download limit in integer bytes/sec and return
/// the confirmed readback. `0` means unlimited. Invalid or unsafe-sized input
/// is rejected as [`BridgeError::InvalidResponse`]; an unreadable/unparseable
/// readback is [`BridgeError::RateUnconfirmed`].
#[tauri::command]
pub async fn set_global_rate_limit(
    rate: i64,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<i64, BridgeError> {
    state.set_global_rate_limit(rate).await
}

/// Read the daemon's saved max-concurrent-downloads value (1–10).
#[tauri::command]
pub async fn get_max_concurrent_downloads(
    state: tauri::State<'_, ConnectionManager>,
) -> Result<i64, BridgeError> {
    state.get_max_concurrent_downloads().await
}

/// Save the daemon's max-concurrent-downloads setting on the owned bundled
/// daemon. `before` is the loaded value for the daemon's conflict check. The
/// daemon is not restarted; the frontend must present the change as pending a
/// restart. An unconfirmed write is never retried automatically.
#[tauri::command]
pub async fn set_max_concurrent_downloads(
    before: i64,
    value: i64,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<i64, BridgeError> {
    state.set_max_concurrent_downloads(before, value).await
}

#[tauri::command]
pub fn disconnect_surge(state: tauri::State<'_, ConnectionManager>) {
    state.disconnect();
}

/// Forward a queue control action over the active connection.
///
/// `action` is matched against a strict, case-sensitive allowlist; anything
/// unrecognized becomes a sanitized [`BridgeError::InvalidResponse`]. There is
/// no mapping to Surge's `/purge` route, so this command never explicitly
/// purges completed final files (released `/delete` cancels active transfers
/// and may clean partial bytes). Returns only an acknowledgment — the
/// frontend owns any queue refresh.
#[tauri::command]
pub async fn control_download(
    action: String,
    id: String,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<(), BridgeError> {
    let action = match action.as_str() {
        "pause" => ControlAction::Pause,
        "resume" => ControlAction::Resume,
        "removeRecord" => ControlAction::RemoveRecord,
        _ => return Err(BridgeError::InvalidResponse),
    };
    state.control_download(action, &id).await
}

/// Apply Pause all or Resume all to the daemon's entire queue.
///
/// `action` is matched against a strict, case-sensitive allowlist (`pauseAll`
/// or `resumeAll`); anything else becomes a sanitized
/// [`BridgeError::InvalidResponse`] without a request. The applied set is the
/// daemon's whole queue, not the rows currently filtered in the UI. Returns
/// only the validated count and per-item results — the frontend owns the fresh
/// queue reconciliation and never retries automatically.
#[tauri::command]
pub async fn bulk_control(
    action: String,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<BulkControlOutcome, BridgeError> {
    let action = match action.as_str() {
        "pauseAll" => BulkControlAction::PauseAll,
        "resumeAll" => BulkControlAction::ResumeAll,
        _ => return Err(BridgeError::InvalidResponse),
    };
    state.bulk_control(action).await
}

/// Enqueue a download on the active connection.
///
/// The URL is validated locally by the client before any network request; only
/// the sanitized [`AddOutcome`] crosses back to the frontend, and no response
/// field (`id`, `filename`, `status`, `message`) is projected. A stale
/// connection epoch surfaces as [`BridgeError::Superseded`] rather than a
/// stale success.
#[tauri::command]
pub async fn add_download(
    url: String,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<AddOutcome, BridgeError> {
    state.add_download(&url).await
}

/// App-wide automatic-hash preference. Defaults to false on first run and is
/// persisted atomically in the app-private managed state directory.
#[tauri::command]
pub fn get_automatic_hash_settings(
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<AutomaticHashSettingsDto, BridgeError> {
    state.ensure_hash_state(&app)?;
    Ok(AutomaticHashSettingsDto {
        default_enabled: state.automatic_hash_default()?,
    })
}

/// Persist the app-wide automatic-hash default. It affects the initial state
/// of the next batch checkbox; it does not retroactively hash existing rows.
#[tauri::command]
pub fn set_automatic_hash_default(
    enabled: bool,
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<AutomaticHashSettingsDto, BridgeError> {
    state.ensure_hash_state(&app)?;
    Ok(AutomaticHashSettingsDto {
        default_enabled: state.set_automatic_hash_default(enabled)?,
    })
}

/// Bind automatic hashing to positively acknowledged download ids.
///
/// The IPC payload contains only daemon ids and booleans, never a filesystem
/// path. Duplicate ids in one call keep the last value, invalid ids fail
/// without changing state, and the list is bounded.
#[tauri::command]
pub async fn set_download_hash_intents(
    intents: Vec<HashIntentInput>,
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<Vec<DownloadHashDto>, BridgeError> {
    state.ensure_hash_state(&app)?;
    if intents.len() > crate::hash_state::MAX_HASH_INTENTS_PER_CALL {
        return Err(BridgeError::InvalidResponse);
    }
    let intents: Vec<HashIntent> = intents
        .into_iter()
        .map(|intent| HashIntent {
            id: intent.id,
            enabled: intent.enabled,
        })
        .collect();
    state.record_hash_intents(&intents).await
}

/// Current sanitized automatic-hash rows for the owned managed connection.
/// Results associated with another daemon process are never returned, and a
/// completed digest is only served after its authoritative queue record and
/// exact file identity are re-verified. An owned managed connection with
/// unresolved/quarantined hash persistence reports `HashUnconfirmed` instead
/// of an empty success. Manual connections never initialize or own this state.
#[tauri::command]
pub async fn get_download_hashes(
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<Vec<DownloadHashDto>, BridgeError> {
    state
        .get_download_hashes_fenced(|| state.ensure_hash_state(&app))
        .await
}

/// Ask the single bounded worker to make another pass, then return the
/// re-verified sanitized rows. Hashing itself continues in the background and
/// never blocks this command. An owned managed connection with unresolved or
/// quarantined persistence reports `HashUnconfirmed`; a manual connection keeps
/// the existing best-effort empty-row behavior.
#[tauri::command]
pub async fn refresh_download_hashes(
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<Vec<DownloadHashDto>, BridgeError> {
    state
        .refresh_download_hashes_fenced(|| state.ensure_hash_state(&app))
        .await
}

/// Enqueue a download with the batch checkbox value.
///
/// The id is returned only for a positively acknowledged queued add and is the
/// same id used for the durable hash intent. Pending/duplicate/ambiguous adds
/// carry no id and are never retried automatically.
#[tauri::command]
pub async fn add_download_with_options(
    url: String,
    hash: bool,
    app: tauri::AppHandle,
    state: tauri::State<'_, ConnectionManager>,
) -> Result<AddReceiptDto, BridgeError> {
    state.ensure_hash_state(&app)?;
    state.add_download_with_options(&url, hash).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(target_os = "linux")]
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    #[cfg(target_os = "linux")]
    fn sandbox_base() -> PathBuf {
        std::env::temp_dir().join(format!(
            "surge-managed-integration-tests-{}",
            std::process::id()
        ))
    }

    #[cfg(target_os = "linux")]
    fn fresh_dir(tag: &str) -> PathBuf {
        std::fs::create_dir_all(sandbox_base()).expect("create sandbox base");
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = sandbox_base().join(format!("{tag}-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create sandbox child");
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .expect("private sandbox child");
        }
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
    fn port_lock() -> &'static tokio::sync::Mutex<()> {
        crate::managed_server::test_port_lock()
    }

    #[cfg(target_os = "linux")]
    fn real_daemon_binary() -> Option<PathBuf> {
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
                Some(path)
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn pid_alive(pid: u32) -> bool {
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        // SAFETY: integer-only existence probe on a pid observed from our child.
        unsafe { kill(pid as i32, 0) == 0 }
    }

    #[cfg(target_os = "linux")]
    fn kill_managed_child_for_test(manager: &ConnectionManager) -> bool {
        let state = manager.state.lock().unwrap();
        state
            .managed
            .as_ref()
            .map(|server| server.kill_owned_child_for_test())
            .unwrap_or(false)
    }

    #[cfg(target_os = "linux")]
    async fn wait_pid_dead(pid: u32, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if !pid_alive(pid) {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        !pid_alive(pid)
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
    fn managed_pid(manager: &ConnectionManager) -> Option<u32> {
        manager
            .state
            .lock()
            .ok()
            .and_then(|state| state.managed.as_ref().map(|server| server.pid()))
    }

    #[cfg(target_os = "linux")]
    fn read_pidfile(pidfile: &Path) -> Option<u32> {
        std::fs::read_to_string(pidfile)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
    }

    fn claim(manager: &ConnectionManager) -> ManagedStartTicket {
        let (ticket, retired) = manager.begin_managed_start().expect("managed start claim");
        assert!(
            retired.is_none(),
            "a fresh manager must not retire a server"
        );
        ticket
    }

    #[cfg(unix)]
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

    #[cfg(unix)]
    fn assert_managed_stage_code(
        error: &ManagedError,
        expected_debug: &str,
        expected_json: &str,
        extra_forbidden: &[&str],
    ) {
        let display = format!("{error}");
        let debug = format!("{error:?}");
        let json = serde_json::to_string(error).expect("serialize managed error");
        assert_eq!(
            debug, expected_debug,
            "stage code must be fixed and specific"
        );
        assert_eq!(
            json,
            format!("\"{expected_json}\""),
            "serialized code must stay fixed"
        );
        for marker in extra_forbidden.iter().chain(HOSTILE_INPUT_MARKERS.iter()) {
            let marker = *marker;
            assert!(
                !display.contains(marker),
                "Display leaked {marker:?}: {display}"
            );
            assert!(!debug.contains(marker), "Debug leaked {marker:?}: {debug}");
            assert!(
                !json.contains(marker),
                "serialized code leaked {marker:?}: {json}"
            );
        }
        for text in [&display, &debug, &json] {
            assert!(!text.contains('/'), "error text leaked a path: {text}");
            assert!(!text.contains('\\'), "error text leaked a path: {text}");
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_managed_failure_closed(manager: &ConnectionManager) {
        let state = manager.state.lock().unwrap();
        assert!(!state.starting, "failed start must release the claim");
        assert!(
            state.managed.is_none(),
            "failed start must not install a server"
        );
    }

    #[test]
    fn managed_error_codes_are_sanitized() {
        let error = ManagedError::Startup;
        assert_eq!(format!("{error}"), "managed daemon startup failed");
        assert_eq!(format!("{error:?}"), "Startup");
        assert!(!format!("{error:?}").contains("/"));
        assert_eq!(
            serde_json::to_string(&ManagedError::AlreadyStarted).unwrap(),
            "\"alreadyStarted\""
        );
    }

    #[test]
    fn managed_resource_name_is_target_specific() {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        assert_eq!(
            managed_resource_file_name().unwrap(),
            "surge-x86_64-unknown-linux-gnu"
        );
        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        assert_eq!(
            managed_resource_file_name().unwrap(),
            "surge-aarch64-unknown-linux-gnu"
        );
        #[cfg(all(windows, target_arch = "x86_64"))]
        assert_eq!(
            managed_resource_file_name().unwrap(),
            "surge-x86_64-pc-windows-msvc.exe"
        );
        #[cfg(all(windows, target_arch = "aarch64"))]
        assert_eq!(
            managed_resource_file_name().unwrap(),
            "surge-aarch64-pc-windows-msvc.exe"
        );
        #[cfg(not(any(
            all(target_os = "linux", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "aarch64"),
            all(windows, target_arch = "x86_64"),
            all(windows, target_arch = "aarch64"),
        )))]
        assert_eq!(
            managed_resource_file_name().unwrap_err(),
            ManagedError::UnsupportedPlatform
        );
    }

    #[cfg(unix)]
    #[test]
    fn managed_binary_validation_rejects_unsafe_paths() {
        use std::os::unix::fs::PermissionsExt;

        let root = fresh_dir("resource-root");
        let name = "surge-test-fixture";
        let binary = root.join(name);
        std::fs::write(&binary, b"fixture").expect("write fixture");
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
            .expect("chmod fixture");

        assert_eq!(
            validate_managed_binary(&binary, &root, name).unwrap(),
            binary
        );

        let link = root.join("surge-link");
        std::os::unix::fs::symlink(&binary, &link).expect("create symlink");
        assert_eq!(
            validate_managed_binary(&link, &root, "surge-link").unwrap_err(),
            ManagedError::Resource
        );

        let noexec = root.join("surge-noexec");
        std::fs::write(&noexec, b"fixture").expect("write noexec");
        std::fs::set_permissions(&noexec, std::fs::Permissions::from_mode(0o600))
            .expect("chmod noexec");
        assert_eq!(
            validate_managed_binary(&noexec, &root, "surge-noexec").unwrap_err(),
            ManagedError::Resource
        );

        let directory = root.join("surge-dir");
        std::fs::create_dir(&directory).expect("create directory");
        assert_eq!(
            validate_managed_binary(&directory, &root, "surge-dir").unwrap_err(),
            ManagedError::Resource
        );

        let outside = sandbox_base().join("surge-outside");
        std::fs::write(&outside, b"fixture").expect("write outside fixture");
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o700))
            .expect("chmod outside fixture");
        assert_eq!(
            validate_managed_binary(&outside, &root, "surge-outside").unwrap_err(),
            ManagedError::Resource
        );

        assert_eq!(
            validate_managed_binary(&root.join("missing"), &root, "missing").unwrap_err(),
            ManagedError::Resource
        );

        let _ = std::fs::remove_file(&outside);
        remove_all_best_effort(&root);
    }

    #[cfg(unix)]
    #[test]
    fn managed_stage_resource_resolution_is_sanitized_and_fixed() {
        use std::os::unix::fs::PermissionsExt;

        let root = fresh_dir("resource-resolution-secret");
        let hostile = "sk_live_TOKEN_alice@example.com-Bearer BEGIN PRIVATE KEY";
        let target = root.join(hostile);
        std::fs::write(&target, b"fixture").expect("write hostile fixture");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700))
            .expect("chmod hostile fixture");
        let link = root.join("surge-link");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");
        let root_text = root.to_string_lossy().into_owned();
        let target_text = target.to_string_lossy().into_owned();

        let error = validate_managed_binary(&link, &root, "surge-link").unwrap_err();
        assert_managed_stage_code(&error, "Resource", "resource", &[&root_text, &target_text]);

        remove_all_best_effort(&root);
    }

    #[cfg(unix)]
    #[test]
    fn app_data_dir_creation_is_non_recursive_and_rejects_symlinks() {
        let root = fresh_dir("app-data");
        let child = root.join("child");
        ensure_app_data_dir(&child).expect("create final component");
        assert!(child.is_dir());
        assert_eq!(
            ensure_app_data_dir(&root.join("missing-parent/child")).unwrap_err(),
            ManagedError::Paths
        );

        let target = root.join("target");
        std::fs::create_dir(&target).expect("create target");
        let link = root.join("link");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");
        assert_eq!(ensure_app_data_dir(&link).unwrap_err(), ManagedError::Paths);

        remove_all_best_effort(&root);
    }

    #[test]
    fn managed_start_claim_is_not_reentrant_and_abort_allows_retry() {
        let manager = ConnectionManager::default();
        let first = claim(&manager);
        assert_eq!(
            manager.begin_managed_start().unwrap_err(),
            ManagedError::AlreadyStarted
        );

        manager.abort_managed_start(first);
        let second = claim(&manager);
        manager.abort_managed_start(second);

        let state = manager.state.lock().unwrap();
        assert!(!state.starting);
        assert!(state.managed.is_none());
    }

    #[test]
    fn disconnect_during_start_prevents_activation_and_allows_retry() {
        let manager = ConnectionManager::default();
        let ticket = claim(&manager);

        manager.disconnect();

        let (activate, starting) = {
            let mut state = manager.state.lock().unwrap();
            let activate = state
                .decide_managed_finish(ticket)
                .expect("finish decision");
            (activate, state.starting)
        };
        assert!(!activate, "a newer epoch must not be overwritten");
        assert!(!starting);

        let retry = claim(&manager);
        manager.abort_managed_start(retry);
    }

    #[test]
    fn stale_claim_cannot_finish_a_newer_start() {
        let manager = ConnectionManager::default();
        let stale = claim(&manager);
        manager.abort_managed_start(stale);
        let current = claim(&manager);

        let mut state = manager.state.lock().unwrap();
        assert_eq!(
            state.decide_managed_finish(stale).unwrap_err(),
            ManagedError::Superseded
        );
        assert!(
            state.starting,
            "stale finish must not clear the newer claim"
        );
        drop(state);

        manager.abort_managed_start(current);
    }

    #[test]
    fn shutdown_invalidates_start_and_fences_new_claims() {
        let manager = ConnectionManager::default();
        let ticket = claim(&manager);
        manager.shutdown_managed();

        let mut state = manager.state.lock().unwrap();
        assert_eq!(
            state.decide_managed_finish(ticket).unwrap_err(),
            ManagedError::ShuttingDown
        );
        drop(state);

        assert_eq!(
            manager.begin_managed_start().unwrap_err(),
            ManagedError::ShuttingDown
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_stage_process_launch_failure_is_sanitized_and_fails_closed() {
        let root = fresh_dir("process-launch-failure");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let binary = root.join("missing-managed-daemon-sk_live_TOKEN_alice@example.com");
        let state_dir = root.join("state");
        let binary_text = binary.to_string_lossy().into_owned();
        let root_text = root.to_string_lossy().into_owned();

        let manager = ConnectionManager::default();
        let error = start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .unwrap_err();
        assert_managed_stage_code(&error, "Process", "process", &[&binary_text, &root_text]);

        let retry = claim(&manager);
        manager.abort_managed_start(retry);
        assert_managed_failure_closed(&manager);

        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_stage_state_preparation_failure_is_sanitized_and_fails_closed() {
        let root = fresh_dir("state-preparation-failure");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let binary = root.join("missing-managed-daemon-sk_live_TOKEN_alice@example.com");
        let state_dir = root.join("state-sk_live_TOKEN_alice@example.com");
        std::fs::write(&state_dir, b"not a directory").expect("write hostile state fixture");
        let binary_text = binary.to_string_lossy().into_owned();
        let state_text = state_dir.to_string_lossy().into_owned();
        let root_text = root.to_string_lossy().into_owned();

        let manager = ConnectionManager::default();
        let error = start_managed_at_with_owner(
            &manager,
            &binary,
            &state_dir,
            &home,
            Duration::from_millis(200),
            None,
        )
        .await
        .unwrap_err();
        assert_managed_stage_code(
            &error,
            "Paths",
            "paths",
            &[&binary_text, &state_text, &root_text],
        );

        let retry = claim(&manager);
        manager.abort_managed_start(retry);
        assert_managed_failure_closed(&manager);

        remove_all_best_effort(&root);
    }

    #[test]
    fn managed_start_future_and_guard_are_send() {
        fn assert_send<T: Send>(_: T) {}

        let manager = ConnectionManager::default();
        let future = start_managed_at(
            &manager,
            Path::new("/nonexistent-managed-daemon"),
            Path::new("/nonexistent-managed-state"),
            Path::new("/nonexistent-managed-home"),
        );
        // Never polled: compile-time regression guard for the Tauri command
        // future being !Send (the original `&Receiver`-across-await defect).
        assert_send(future);

        let ticket = claim(&manager);
        let lifecycle = manager
            .lifecycle
            .begin_operation()
            .expect("lifecycle operation");
        assert_send(ManagedStartGuard::new(
            &manager,
            ticket,
            OwnerLifetime::finished_for_test(),
            lifecycle,
        ));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancelled_startup_releases_claim_and_cleans_child() {
        let _guard = port_lock().lock().await;
        let root = fresh_dir("cancel-startup");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);

        let manager = ConnectionManager::default();
        let mut start = Box::pin(start_managed_at(&manager, &script, &state_dir, &home));
        let mut child_pid = None;
        for _ in 0..500 {
            tokio::select! {
                result = &mut start => {
                    panic!("sleeping fixture must not become ready: {:?}", result);
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
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

        // Cancel the command future. The RAII guard must release the claim and
        // the owned child must be killed/reaped without a stranded lease.
        drop(start);
        assert!(
            wait_pid_dead(child_pid, std::time::Duration::from_secs(10)).await,
            "cancelled startup must clean up its owned child"
        );

        let retry = claim(&manager);
        manager.abort_managed_start(retry);
        let state = manager.state.lock().unwrap();
        assert!(!state.starting);
        assert!(state.managed.is_none());
        drop(state);

        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_retirement(manager: &ConnectionManager, minimum: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if manager.state.lock().unwrap().retiring >= minimum {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("retirement fence was not registered");
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_new_pid(pidfile: &Path, old: u32, timeout: std::time::Duration) -> u32 {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if let Ok(text) = std::fs::read_to_string(pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    if pid != old {
                        return pid;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("restart did not spawn a new child");
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_retirement_release(manager: &ConnectionManager) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if manager.state.lock().unwrap().retiring == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("retirement fence was not released after confirmed reap");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_start_blocks_restart_until_real_owner_reaped() {
        let _guard = port_lock().lock().await;
        let root = fresh_dir("cancel-owner-fence");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);

        let manager = Arc::new(ConnectionManager::default());
        let gate = crate::managed_server::OwnerCleanupGate::new();
        gate.hold();

        let start_manager = Arc::clone(&manager);
        let start_script = script.clone();
        let start_home = home.clone();
        let start_state = state_dir.clone();
        let mut start = Box::pin(start_managed_at_with_owner(
            &start_manager,
            &start_script,
            &start_state,
            &start_home,
            std::time::Duration::from_secs(60),
            Some(gate.clone()),
        ));
        let mut child_pid = None;
        for _ in 0..500 {
            tokio::select! {
                result = &mut start => {
                    panic!("sleeping fixture must not become ready: {:?}", result);
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
            if let Ok(text) = std::fs::read_to_string(&pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    child_pid = Some(pid);
                    break;
                }
            }
        }
        let old_pid = child_pid.expect("fixture child must have spawned");
        assert!(pid_alive(old_pid));

        // Cancel the start future exactly as a dropped command would. The
        // owner is gated in its cleanup path, so the child is deterministically
        // still alive while the restart is attempted.
        drop(start);
        wait_for_retirement(&manager, 1).await;
        assert!(pid_alive(old_pid), "gated owner child must still be alive");

        let restart_manager = Arc::clone(&manager);
        let restart_script = script.clone();
        let restart_home = home.clone();
        let restart_state = state_dir.clone();
        let restart = tokio::spawn(async move {
            start_managed_at(
                &restart_manager,
                &restart_script,
                &restart_state,
                &restart_home,
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(
            !restart.is_finished(),
            "restart must wait for the real owner retirement"
        );
        let still_old = std::fs::read_to_string(&pidfile)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok());
        assert_eq!(
            still_old,
            Some(old_pid),
            "restart must not spawn while the old child still lives"
        );
        assert!(pid_alive(old_pid));

        gate.release();
        assert!(
            wait_pid_dead(old_pid, std::time::Duration::from_secs(10)).await,
            "released owner must kill/reap the old child"
        );
        wait_for_retirement_release(&manager).await;
        let new_pid = wait_for_new_pid(&pidfile, old_pid, std::time::Duration::from_secs(10)).await;
        assert!(pid_alive(new_pid));

        restart.abort();
        let _ = restart.await;
        assert!(
            wait_pid_dead(new_pid, std::time::Duration::from_secs(10)).await,
            "aborted restart must clean up its child"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn app_shutdown_waits_for_cancelled_owner_reap() {
        let _guard = port_lock().lock().await;
        let root = fresh_dir("cancel-owner-shutdown");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);

        let manager = Arc::new(ConnectionManager::default());
        let gate = crate::managed_server::OwnerCleanupGate::new();
        gate.hold();

        let start_manager = Arc::clone(&manager);
        let start_script = script.clone();
        let start_home = home.clone();
        let start_state = state_dir.clone();
        let mut start = Box::pin(start_managed_at_with_owner(
            &start_manager,
            &start_script,
            &start_state,
            &start_home,
            std::time::Duration::from_secs(60),
            Some(gate.clone()),
        ));
        let mut child_pid = None;
        for _ in 0..500 {
            tokio::select! {
                result = &mut start => {
                    panic!("sleeping fixture must not become ready: {:?}", result);
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
            if let Ok(text) = std::fs::read_to_string(&pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    child_pid = Some(pid);
                    break;
                }
            }
        }
        let child_pid = child_pid.expect("fixture child must have spawned");
        drop(start);
        wait_for_retirement(&manager, 1).await;
        assert!(pid_alive(child_pid));

        let shutdown_manager = Arc::clone(&manager);
        let shutdown = tokio::spawn(async move { shutdown_manager.shutdown_managed() });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(
            !shutdown.is_finished(),
            "app drain must not complete on mere future cancellation"
        );
        assert!(pid_alive(child_pid));

        gate.release();
        assert!(
            wait_pid_dead(child_pid, std::time::Duration::from_secs(10)).await,
            "cancelled owner must kill/reap its child"
        );
        let drained = tokio::time::timeout(std::time::Duration::from_secs(10), shutdown)
            .await
            .expect("app shutdown must finish after reaping")
            .expect("shutdown task");
        assert!(drained, "app drain must only complete after reaping");
        assert_eq!(manager.state.lock().unwrap().retiring, 0);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_stage_readiness_failure_is_sanitized_and_reaps_owned_child() {
        let _guard = port_lock().lock().await;
        let root = fresh_dir("readiness-failure");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);
        let script_text = script.to_string_lossy().into_owned();
        let root_text = root.to_string_lossy().into_owned();

        let manager = ConnectionManager::default();
        let error = start_managed_at_with_owner(
            &manager,
            &script,
            &state_dir,
            &home,
            std::time::Duration::from_millis(250),
            None,
        )
        .await
        .unwrap_err();
        assert_managed_stage_code(
            &error,
            "Readiness",
            "readiness",
            &[&script_text, &root_text],
        );

        let pid = read_pidfile(&pidfile).expect("fixture child must have spawned");
        assert!(
            wait_pid_dead(pid, std::time::Duration::from_secs(10)).await,
            "failed readiness must reap only the owned child"
        );
        assert_managed_failure_closed(&manager);

        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_start_blocks_restart_and_app_drain_until_owner_reaped() {
        let _guard = port_lock().lock().await;
        let root = fresh_dir("failed-owner-fence");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);

        let manager = Arc::new(ConnectionManager::default());
        let gate = crate::managed_server::OwnerCleanupGate::new();
        gate.hold();

        // Short readiness timeout makes this a genuine failed start while the
        // real owner still has a live child blocked in cleanup.
        let error = start_managed_at_with_owner(
            &manager,
            &script,
            &state_dir,
            &home,
            std::time::Duration::from_millis(150),
            Some(gate.clone()),
        )
        .await
        .unwrap_err();
        assert_managed_stage_code(&error, "Readiness", "readiness", &[]);
        wait_for_retirement(&manager, 1).await;
        let old_pid = std::fs::read_to_string(&pidfile)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
            .expect("fixture child must have spawned");
        assert!(
            pid_alive(old_pid),
            "gated failed-start owner child must still be alive"
        );

        let restart_manager = Arc::clone(&manager);
        let restart_script = script.clone();
        let restart_home = home.clone();
        let restart_state = state_dir.clone();
        let restart = tokio::spawn(async move {
            start_managed_at(
                &restart_manager,
                &restart_script,
                &restart_state,
                &restart_home,
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(
            !restart.is_finished(),
            "failed-start retry must wait for owner reaping"
        );
        let still_old = std::fs::read_to_string(&pidfile)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok());
        assert_eq!(still_old, Some(old_pid));

        let shutdown_manager = Arc::clone(&manager);
        let shutdown = tokio::spawn(async move { shutdown_manager.shutdown_managed() });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(
            !shutdown.is_finished(),
            "app drain must not complete before the owner is reaped"
        );
        assert!(pid_alive(old_pid));

        gate.release();
        assert!(
            wait_pid_dead(old_pid, std::time::Duration::from_secs(10)).await,
            "released owner must kill/reap the failed-start child"
        );
        let drained = tokio::time::timeout(std::time::Duration::from_secs(10), shutdown)
            .await
            .expect("app shutdown must finish after reaping")
            .expect("shutdown task");
        assert!(drained, "app drain must only complete after reaping");
        assert_eq!(manager.state.lock().unwrap().retiring, 0);

        let _ = restart.await;
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn concurrent_refresh_and_control_share_managed_server() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let root = fresh_dir("concurrent-managed");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");

        let manager = ConnectionManager::default();
        let initial = start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("managed daemon must start");
        assert!(initial.is_empty());
        let pid = managed_pid(&manager).expect("owned server retained");

        // Cancel an in-flight network operation after one poll. It holds only
        // an Arc client clone, so the owned server and child are untouched.
        let mut in_flight = Box::pin(manager.refresh());
        let mut first_poll = None;
        std::future::poll_fn(|cx| {
            first_poll = Some(std::future::Future::poll(in_flight.as_mut(), cx));
            std::task::Poll::Ready(())
        })
        .await;
        assert!(
            matches!(first_poll, Some(std::task::Poll::Pending)),
            "network refresh must be in flight after its first poll"
        );
        drop(in_flight);
        assert_eq!(managed_pid(&manager), Some(pid));
        assert!(pid_alive(pid));

        let (r1, r2, r3, control) = tokio::join!(
            manager.refresh(),
            manager.list_downloads(),
            manager.refresh(),
            manager.control_download(ControlAction::RemoveRecord, "missing-test-id"),
        );
        assert!(r1.is_ok(), "concurrent refresh 1 failed: {r1:?}");
        assert!(r2.is_ok(), "concurrent list failed: {r2:?}");
        assert!(r3.is_ok(), "concurrent refresh 2 failed: {r3:?}");
        assert!(
            !matches!(&control, Err(BridgeError::NotConnected)),
            "concurrent control must not see NotConnected: {control:?}"
        );
        assert_eq!(managed_pid(&manager), Some(pid), "server must stay owned");
        assert!(pid_alive(pid));
        assert!(manager.refresh().await.is_ok());

        manager.shutdown_managed();
        assert!(wait_pid_dead(pid, std::time::Duration::from_secs(10)).await);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn crashed_managed_child_allows_explicit_restart() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let root = fresh_dir("crashed-managed");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");

        let manager = ConnectionManager::default();
        start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("managed daemon must start");
        let first_pid = managed_pid(&manager).expect("owned server retained");
        assert!(
            kill_managed_child_for_test(&manager),
            "owned child kill request must reach the owner thread"
        );
        assert!(wait_pid_dead(first_pid, std::time::Duration::from_secs(10)).await);

        let start = std::time::Instant::now();
        while start.elapsed() < std::time::Duration::from_secs(10) {
            let exited = {
                let state = manager.state.lock().unwrap();
                state
                    .managed
                    .as_ref()
                    .map(|server| server.has_exited())
                    .unwrap_or(true)
            };
            if exited {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        let restarted = start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("explicit restart after crash must succeed");
        assert!(restarted.is_empty());
        let second_pid = managed_pid(&manager).expect("restarted server retained");
        assert_ne!(second_pid, first_pid);
        assert!(pid_alive(second_pid));
        assert!(manager.refresh().await.is_ok());

        manager.shutdown_managed();
        assert!(wait_pid_dead(second_pid, std::time::Duration::from_secs(10)).await);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn disconnect_stops_managed_child_and_allows_restart() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let root = fresh_dir("disconnect-managed");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");

        let manager = ConnectionManager::default();
        start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("managed daemon must start");
        let first_pid = managed_pid(&manager).expect("owned server retained");

        manager.disconnect();
        assert!(managed_pid(&manager).is_none());
        assert!(wait_pid_dead(first_pid, std::time::Duration::from_secs(10)).await);
        assert_eq!(
            manager.refresh().await.unwrap_err(),
            BridgeError::NotConnected
        );

        let restarted = start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("restart after disconnect must succeed");
        assert!(restarted.is_empty());
        let second_pid = managed_pid(&manager).expect("restarted server retained");
        assert_ne!(second_pid, first_pid);
        assert!(pid_alive(second_pid));

        manager.shutdown_managed();
        assert!(wait_pid_dead(second_pid, std::time::Duration::from_secs(10)).await);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn inactive_live_managed_child_does_not_block_restart() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let root = fresh_dir("inactive-managed");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");

        let manager = ConnectionManager::default();
        start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("managed daemon must start");
        let old_pid = managed_pid(&manager).expect("owned server retained");

        // Simulate the old failure mode: a live daemon retained in state but
        // no longer reachable as the active connection.
        {
            let mut state = manager.state.lock().unwrap();
            state.active = ActiveConnection::None;
        }
        let (claim, retired) = manager
            .begin_managed_start()
            .expect("unreachable live daemon must not block an explicit restart");
        let retired = retired.expect("old live daemon must be removed from state");
        retired.shutdown().await;
        assert!(wait_pid_dead(old_pid, std::time::Duration::from_secs(10)).await);
        manager.abort_managed_start(claim);

        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    fn read_http_headers(stream: &mut impl std::io::Read) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") && buf.len() < 16 * 1024 {
            match stream.read(&mut byte) {
                Ok(0) => break,
                Ok(_) => buf.push(byte[0]),
                Err(_) => break,
            }
        }
        buf
    }

    #[cfg(target_os = "linux")]
    fn manual_plain_listener(
        response: &'static [u8],
        status_line: &'static [u8],
    ) -> (
        String,
        std::sync::mpsc::Receiver<Vec<u8>>,
        std::thread::JoinHandle<()>,
    ) {
        use std::io::Write;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind manual listener");
        let addr = listener.local_addr().expect("manual listener addr");
        let (capture_tx, capture_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept manual client");
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .expect("manual read timeout");
            let request = read_http_headers(&mut stream);
            let response_head = format!(
                "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                String::from_utf8_lossy(status_line),
                response.len()
            );
            stream
                .write_all(response_head.as_bytes())
                .expect("manual response head");
            stream.write_all(response).expect("manual response body");
            stream.flush().expect("manual response flush");
            let _ = capture_tx.send(request);
        });
        (format!("http://{addr}"), capture_rx, server)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn manual_connect_retires_managed_daemon_and_never_shuts_down_manual() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let root = fresh_dir("manual-switch");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");

        let manager = ConnectionManager::default();
        start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("managed daemon must start");
        let old_pid = managed_pid(&manager).expect("owned server retained");

        let (endpoint, captured, server) = manual_plain_listener(b"[]", b"200 OK");
        let items = manager
            .connect(&endpoint, "manual-token")
            .await
            .expect("manual connect must succeed");
        assert!(items.is_empty());
        assert!(managed_pid(&manager).is_none(), "managed binding retired");
        assert!(
            wait_pid_dead(old_pid, std::time::Duration::from_secs(10)).await,
            "manual switch must retire the superseded managed daemon"
        );
        {
            let state = manager.state.lock().unwrap();
            assert!(state.active == ActiveConnection::Manual);
            assert!(state.manual.is_some());
            assert!(state.managed.is_none());
        }

        let request = captured
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("captured manual request");
        server.join().expect("manual server thread");
        let text = String::from_utf8_lossy(&request);
        assert!(text.starts_with("GET /list HTTP/1.1"));
        assert!(
            !text.to_ascii_lowercase().contains("/shutdown"),
            "the manual listener must never receive a shutdown request"
        );

        manager.disconnect();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn failed_manual_connect_retires_managed_daemon_without_orphan() {
        let _guard = port_lock().lock().await;
        let Some(binary) = real_daemon_binary() else {
            eprintln!("skipping: real daemon fixture not available");
            return;
        };
        let root = fresh_dir("manual-failure");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");

        let manager = ConnectionManager::default();
        start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .expect("managed daemon must start");
        let old_pid = managed_pid(&manager).expect("owned server retained");

        let (endpoint, captured, server) =
            manual_plain_listener(b"boom", b"500 Internal Server Error");
        let error = manager
            .connect(&endpoint, "manual-token")
            .await
            .expect_err("manual connect must fail");
        assert_eq!(error, BridgeError::InvalidResponse);
        assert!(managed_pid(&manager).is_none(), "managed binding retired");
        assert!(
            wait_pid_dead(old_pid, std::time::Duration::from_secs(10)).await,
            "failed manual attempt must not orphan the managed daemon"
        );

        let request = captured
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("captured manual request");
        server.join().expect("manual server thread");
        let text = String::from_utf8_lossy(&request);
        assert!(!text.to_ascii_lowercase().contains("/shutdown"));

        manager.disconnect();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn app_shutdown_accounts_for_detached_start_and_reaps_child() {
        let _guard = port_lock().lock().await;
        let root = fresh_dir("app-shutdown-start");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);

        let manager = Arc::new(ConnectionManager::default());
        let start_manager = Arc::clone(&manager);
        let start_script = script.clone();
        let start_home = home.clone();
        let start_state = state_dir.clone();
        let task = tokio::spawn(async move {
            start_managed_at(&start_manager, &start_script, &start_state, &start_home).await
        });

        let mut child_pid = None;
        let wait_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < wait_deadline {
            if let Ok(text) = std::fs::read_to_string(&pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    child_pid = Some(pid);
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let child_pid = child_pid.expect("fixture child must have spawned");
        assert!(pid_alive(child_pid));
        assert!(!task.is_finished(), "start must still be detached");

        // App Exit while the start owns a spawned daemon that is not yet in
        // ConnectionState. Shutdown must cancel and account for it.
        let started = std::time::Instant::now();
        manager.shutdown_managed();
        let elapsed = started.elapsed();
        let result = task.await.expect("detached start task must join");
        assert_eq!(result.unwrap_err(), ManagedError::ShuttingDown);
        assert!(
            elapsed < std::time::Duration::from_secs(12),
            "app shutdown must stay bounded"
        );
        assert!(
            wait_pid_dead(child_pid, std::time::Duration::from_secs(10)).await,
            "detached start child must be killed/reaped"
        );
        {
            let state = manager.state.lock().unwrap();
            assert!(state.managed.is_none());
            assert!(!state.starting);
            assert!(state.shutting_down);
            assert_eq!(state.retiring, 0);
        }
        assert_eq!(
            start_managed_at(&manager, &script, &state_dir, &home)
                .await
                .unwrap_err(),
            ManagedError::ShuttingDown
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_start_waits_for_detached_retirement_before_spawning() {
        let _guard = port_lock().lock().await;
        let root = fresh_dir("retirement-gate");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let state_dir = root.join("state");
        let pidfile = root.join("child.pid");
        let script = write_sleeping_fixture(&root, &pidfile);

        let manager = Arc::new(ConnectionManager::default());
        // Gate: a prior detached retirement is registered but has not finished.
        {
            let mut state = manager.state.lock().unwrap();
            state.mark_retirement();
        }
        let retirement_manager = Arc::clone(&manager);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let retirement = std::thread::spawn(move || {
            let _guard = retirement_manager.retirement_guard();
            let _ = ready_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("retirement gate ready");

        let start_manager = Arc::clone(&manager);
        let start_script = script.clone();
        let start_home = home.clone();
        let start_state = state_dir.clone();
        let task = tokio::spawn(async move {
            start_managed_at(&start_manager, &start_script, &start_state, &start_home).await
        });

        let claim_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < claim_deadline {
            let starting = manager.state.lock().unwrap().starting;
            if starting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            manager.state.lock().unwrap().starting,
            "start must claim first"
        );
        // While the retirement gate is held, the new start must not spawn a
        // competing child for the fixed port.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !pidfile.exists(),
            "new start must wait for the detached retirement"
        );
        assert!(!task.is_finished());

        release_tx.send(()).expect("release retirement gate");
        retirement.join().expect("retirement gate thread");
        let mut child_pid = None;
        let spawn_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < spawn_deadline {
            if let Ok(text) = std::fs::read_to_string(&pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    child_pid = Some(pid);
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let child_pid = child_pid.expect("start must proceed after retirement completes");
        assert!(
            manager.state.lock().unwrap().retiring == 0,
            "retirement gate must be released before spawning"
        );

        task.abort();
        let _ = task.await;
        assert!(
            wait_pid_dead(child_pid, std::time::Duration::from_secs(10)).await,
            "aborted start must clean up its owned child"
        );
        remove_all_best_effort(&root);
    }

    #[test]
    fn late_confirmed_reap_clears_retirement_and_drain() {
        let manager = ConnectionManager::default();
        {
            let mut state = manager.state.lock().unwrap();
            state.mark_retirement();
        }
        let owner = OwnerLifetime::new();
        let operation = manager.lifecycle.begin_cleanup();
        spawn_owner_cleanup(Arc::clone(&manager.state), owner.clone(), operation, true);

        assert_eq!(manager.state.lock().unwrap().retiring, 1);
        assert!(
            !manager
                .lifecycle
                .wait_for_drain(std::time::Duration::from_millis(25)),
            "a pending owner must keep the drain incomplete"
        );

        // Confirmation arrives after the bounded observation window. It must
        // still release the restart fence and the drain marker.
        owner.finish_for_test(true);
        assert_eq!(manager.state.lock().unwrap().retiring, 0);
        assert!(
            manager
                .lifecycle
                .wait_for_drain(std::time::Duration::from_millis(500)),
            "late confirmed reap must complete the drain"
        );
    }

    #[test]
    fn failed_double_reap_keeps_fence_and_drain_outstanding() {
        let manager = ConnectionManager::default();
        {
            let mut state = manager.state.lock().unwrap();
            state.mark_retirement();
        }
        let owner = OwnerLifetime::new();
        let operation = manager.lifecycle.begin_cleanup();
        spawn_owner_cleanup(Arc::clone(&manager.state), owner.clone(), operation, true);

        // Both the bounded probe and the retained reaper failed to confirm an
        // exit. Success must not be claimed and both markers stay outstanding.
        owner.finish_for_test(false);
        assert!(!owner.was_reaped());
        assert_eq!(manager.state.lock().unwrap().retiring, 1);
        assert!(
            !manager
                .lifecycle
                .wait_for_drain(std::time::Duration::from_millis(25)),
            "unconfirmed reap must never complete the drain"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn app_shutdown_waits_for_detached_retirement() {
        let manager = Arc::new(ConnectionManager::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.mark_retirement();
        }
        let retirement_manager = Arc::clone(&manager);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let retirement = std::thread::spawn(move || {
            let _operation = retirement_manager
                .lifecycle
                .begin_operation()
                .expect("retirement lifecycle operation");
            let _guard = retirement_manager.retirement_guard();
            let _ = ready_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("retirement gate ready");

        let shutdown_manager = Arc::clone(&manager);
        let shutdown = tokio::spawn(async move {
            shutdown_manager.shutdown_managed();
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(
            !shutdown.is_finished(),
            "app shutdown must wait for a detached retirement"
        );

        release_tx.send(()).expect("release retirement gate");
        retirement.join().expect("retirement gate thread");
        tokio::time::timeout(std::time::Duration::from_secs(2), shutdown)
            .await
            .expect("app shutdown must drain within its bound")
            .expect("shutdown task");
        assert_eq!(manager.state.lock().unwrap().retiring, 0);
        assert!(manager.state.lock().unwrap().shutting_down);
    }

    #[tokio::test]
    async fn prior_retirement_wait_is_bounded_and_retryable() {
        let manager = ConnectionManager::default();
        {
            let mut state = manager.state.lock().unwrap();
            state.mark_retirement();
        }
        let started = std::time::Instant::now();
        let error = manager
            .wait_for_prior_retirements(0, std::time::Duration::from_millis(100))
            .await
            .unwrap_err();
        assert_eq!(error, ManagedError::Retry);
        assert!(started.elapsed() >= std::time::Duration::from_millis(90));
        assert!(started.elapsed() < std::time::Duration::from_secs(2));

        {
            let mut state = manager.state.lock().unwrap();
            state.finish_retirement();
        }
        manager
            .wait_for_prior_retirements(0, std::time::Duration::from_millis(100))
            .await
            .expect("retry succeeds once retirement is complete");
    }

    // -----------------------------------------------------------------------
    // Global rate writer ordering/fencing against a loopback managed target.
    // -----------------------------------------------------------------------

    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;

    struct RateTestCaptured {
        request_line: String,
    }

    const RATE_TEST_LIST: &str = r#"[{"id":"r-001","url":"https://dl.example.test/rate.bin","filename":"rate.bin","total_size":10,"downloaded":1,"progress":10.0,"speed":0.0,"status":"downloading","eta":1}]"#;

    fn rate_test_write_response(stream: &mut TcpStream, status_line: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            status_line,
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    fn rate_test_accept(listener: &TcpListener) -> TcpStream {
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).expect("blocking stream");
                    return stream;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "no loopback connection accepted within deadline"
                    );
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(error) => panic!("accept failed: {error}"),
            }
        }
    }

    fn rate_test_read_head(stream: &mut TcpStream) -> String {
        let mut buf = [0u8; 4096];
        let mut head = Vec::new();
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(count) => head.extend_from_slice(&buf[..count]),
            }
        }
        String::from_utf8_lossy(&head).to_string()
    }

    fn rate_test_capture(stream: &mut TcpStream) -> RateTestCaptured {
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .expect("set read timeout");
        let head = rate_test_read_head(stream);
        let head_end = head
            .find("\r\n\r\n")
            .map(|index| index + 4)
            .unwrap_or(head.len());
        let content_length: usize = head
            .get(..head_end)
            .unwrap_or_default()
            .split("\r\n")
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse().ok())
            .unwrap_or(0);
        let mut remaining = content_length.saturating_sub(head.len().saturating_sub(head_end));
        let mut buffer = [0u8; 4096];
        while remaining > 0 {
            match stream.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(count) => remaining = remaining.saturating_sub(count),
            }
        }
        let request_line = head
            .get(..head_end)
            .unwrap_or_default()
            .split("\r\n")
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        RateTestCaptured { request_line }
    }

    fn rate_test_settings(rate: i64) -> String {
        format!(r#"{{"network":{{"global_rate_limit":"{rate} B/s"}}}}"#)
    }

    fn rate_test_ack(rate: i64) -> String {
        let status = if rate == 0 {
            "global_rate_unlimited"
        } else {
            "global_rate_limited"
        };
        format!(r#"{{"status":"{status}","rate":"{rate}"}}"#)
    }

    fn rate_test_spawn_scripted(
        responses: Vec<(&'static str, String)>,
    ) -> (String, thread::JoinHandle<Vec<RateTestCaptured>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let server = thread::spawn(move || {
            let mut captured = Vec::with_capacity(responses.len());
            for (status_line, body) in responses {
                let mut stream = rate_test_accept(&listener);
                captured.push(rate_test_capture(&mut stream));
                if status_line == "DROP" {
                    // Close without a response to model a network failure after
                    // the authenticated request was accepted.
                    continue;
                }
                rate_test_write_response(&mut stream, status_line, &body);
            }
            captured
        });
        (format!("http://{addr}"), server)
    }

    async fn install_rate_test_managed(
        manager: &ConnectionManager,
        base: &str,
        pid: u32,
    ) -> (Arc<SurgeClient>, u64) {
        let client = Arc::new(
            SurgeClient::connect(base, "test-token")
                .await
                .expect("loopback managed client"),
        );
        let epoch = manager.install_test_managed(Arc::clone(&client), pid);
        (client, epoch)
    }

    fn rate_test_spawn_gated_post(
        status_line: &'static str,
        body: String,
        readback: String,
    ) -> (
        String,
        thread::JoinHandle<Vec<RateTestCaptured>>,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut captured = Vec::with_capacity(3);
            let mut first = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut first));
            rate_test_write_response(&mut first, "200 OK", RATE_TEST_LIST);

            let mut second = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut second));
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            rate_test_write_response(&mut second, status_line, &body);

            if status_line == "200 OK" {
                let mut third = rate_test_accept(&listener);
                captured.push(rate_test_capture(&mut third));
                rate_test_write_response(&mut third, "200 OK", &readback);
            }

            // Watch briefly for any request that must have been fenced off.
            listener
                .set_nonblocking(true)
                .expect("nonblocking listener");
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(300);
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).expect("blocking stream");
                        captured.push(rate_test_capture(&mut stream));
                        rate_test_write_response(&mut stream, "500 Internal Server Error", "{}");
                    }
                    Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= deadline {
                            break;
                        }
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
            captured
        });
        (format!("http://{addr}"), server, started_rx, release_tx)
    }

    fn rate_test_spawn_two_writes(
        first_rate: i64,
        second_rate: i64,
    ) -> (
        String,
        thread::JoinHandle<Vec<RateTestCaptured>>,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut captured = Vec::with_capacity(5);
            let mut first = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut first));
            rate_test_write_response(&mut first, "200 OK", RATE_TEST_LIST);

            let mut post1 = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut post1));
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            rate_test_write_response(&mut post1, "200 OK", &rate_test_ack(first_rate));

            let mut read1 = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut read1));
            rate_test_write_response(&mut read1, "200 OK", &rate_test_settings(first_rate));

            let mut post2 = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut post2));
            rate_test_write_response(&mut post2, "200 OK", &rate_test_ack(second_rate));

            let mut read2 = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut read2));
            rate_test_write_response(&mut read2, "200 OK", &rate_test_settings(second_rate));
            captured
        });
        (format!("http://{addr}"), server, started_rx, release_tx)
    }

    /// Serve two same-origin managed clients, gate the first write, and watch
    /// briefly for a queued write that must never be sent to the replacement.
    fn rate_test_spawn_same_origin_replacement(
        first_rate: i64,
        queued_rate: i64,
    ) -> (
        String,
        thread::JoinHandle<Vec<RateTestCaptured>>,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut captured = Vec::with_capacity(5);
            for _ in 0..2 {
                let mut connect = rate_test_accept(&listener);
                captured.push(rate_test_capture(&mut connect));
                rate_test_write_response(&mut connect, "200 OK", RATE_TEST_LIST);
            }

            let mut post1 = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut post1));
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            rate_test_write_response(&mut post1, "200 OK", &rate_test_ack(first_rate));

            let mut read1 = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut read1));
            rate_test_write_response(&mut read1, "200 OK", &rate_test_settings(first_rate));

            listener
                .set_nonblocking(true)
                .expect("nonblocking listener");
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(300);
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).expect("blocking stream");
                        captured.push(rate_test_capture(&mut stream));
                        let is_post = captured
                            .last()
                            .is_some_and(|request| request.request_line.starts_with("POST "));
                        if is_post {
                            rate_test_write_response(
                                &mut stream,
                                "200 OK",
                                &rate_test_ack(queued_rate),
                            );
                        } else {
                            rate_test_write_response(
                                &mut stream,
                                "200 OK",
                                &rate_test_settings(queued_rate),
                            );
                        }
                    }
                    Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= deadline {
                            break;
                        }
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
            captured
        });
        (format!("http://{addr}"), server, started_rx, release_tx)
    }

    fn rate_test_path(captured: &RateTestCaptured) -> &str {
        captured
            .request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_rate_writes_are_serialized_in_first_acquired_order() {
        let first_rate = 1_000_000;
        let second_rate = 2_000_000;
        let (base, server, started, release) = rate_test_spawn_two_writes(first_rate, second_rate);
        let manager = Arc::new(ConnectionManager::default());
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;

        let first_manager = Arc::clone(&manager);
        let first =
            tokio::spawn(async move { first_manager.set_global_rate_limit(first_rate).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("first POST must reach the gated server");

        let second_manager = Arc::clone(&manager);
        let second =
            tokio::spawn(async move { second_manager.set_global_rate_limit(second_rate).await });
        let _ = release.send(());

        assert_eq!(first.await.expect("first set task"), Ok(first_rate));
        assert_eq!(second.await.expect("second set task"), Ok(second_rate));
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 5);
        assert_eq!(
            rate_test_path(&captured[1]),
            format!("/rate-limit/global?rate={first_rate}")
        );
        assert!(captured[2].request_line.starts_with("GET /settings "));
        assert_eq!(
            rate_test_path(&captured[3]),
            format!("/rate-limit/global?rate={second_rate}")
        );
        assert!(captured[4].request_line.starts_with("GET /settings "));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_rate_writer_cannot_retarget_same_origin_replacement_daemon() {
        const FIRST_RATE: i64 = 7_000_000;
        const QUEUED_RATE: i64 = 8_000_000;
        let (base, server, started, release) =
            rate_test_spawn_same_origin_replacement(FIRST_RATE, QUEUED_RATE);
        let manager = Arc::new(ConnectionManager::default());
        let (_client_a, _epoch) = install_rate_test_managed(&manager, &base, 8101).await;
        let replacement = Arc::new(
            SurgeClient::connect(&base, "test-token-b")
                .await
                .expect("same-origin replacement client"),
        );

        let first_manager = Arc::clone(&manager);
        let first =
            tokio::spawn(async move { first_manager.set_global_rate_limit(FIRST_RATE).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("first POST must reach the gated server");

        // This caller snapshots the original daemon while the first write
        // holds the writer gate, then remains queued.
        let second_manager = Arc::clone(&manager);
        let queued =
            tokio::spawn(async move { second_manager.set_global_rate_limit(QUEUED_RATE).await });
        let queued_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let active = manager.lifecycle.lock().active;
            if active >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < queued_deadline,
                "queued caller must snapshot and register before replacement"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Replace the active managed daemon at the same origin while the
        // queued caller is still waiting for the gate.
        manager.disconnect();
        manager.install_test_managed(Arc::clone(&replacement), 8102);
        let _ = release.send(());

        assert_eq!(
            first.await.expect("first set task"),
            Err(BridgeError::Superseded),
            "the old operation must not surface after replacement"
        );
        assert_eq!(
            queued.await.expect("queued set task"),
            Err(BridgeError::Superseded),
            "a queued caller must not mutate the replacement daemon"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            4,
            "the queued writer must not send any request to the replacement"
        );
        assert_eq!(
            rate_test_path(&captured[2]),
            format!("/rate-limit/global?rate={FIRST_RATE}")
        );
        assert!(captured[3].request_line.starts_with("GET /settings "));
        let queued_query = format!("rate={QUEUED_RATE}");
        assert!(
            captured
                .iter()
                .all(|request| !rate_test_path(request).contains(&queued_query)),
            "the queued rate must never reach the daemon"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ambiguous_managed_write_poisons_writer_across_same_daemon_reconnect() {
        const RATE: i64 = 3_000_000;
        let sentinel = "ambiguous-managed-write-sentinel";
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            (
                "500 Internal Server Error",
                format!(r#"{{"error":"{sentinel}"}}"#),
            ),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        let (client, epoch) = install_rate_test_managed(&manager, &base, 9001).await;

        let err = manager
            .set_global_rate_limit(RATE)
            .await
            .expect_err("ambiguous write must fail closed");
        assert_eq!(err, BridgeError::RateUnconfirmed);
        assert!(manager.rate_writer.poisoned.load(Ordering::SeqCst));

        manager.disconnect();
        let replacement_epoch = manager.install_test_managed(Arc::clone(&client), 9001);
        assert_ne!(replacement_epoch, epoch, "reconnect must change the epoch");
        assert_eq!(
            manager.set_global_rate_limit(0).await,
            Err(BridgeError::RateUnconfirmed),
            "poison must survive a same-daemon reconnect"
        );
        assert!(manager.rate_writer.poisoned.load(Ordering::SeqCst));

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "poisoned writer must not send another request"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_managed_write_is_superseded_after_epoch_change() {
        const RATE: i64 = 4_000_000;
        let (base, server, started, release) =
            rate_test_spawn_gated_post("200 OK", rate_test_ack(RATE), rate_test_settings(RATE));
        let manager = Arc::new(ConnectionManager::default());
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 7001).await;

        let set_manager = Arc::clone(&manager);
        let set = tokio::spawn(async move { set_manager.set_global_rate_limit(RATE).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("POST must reach the gated server");

        manager.disconnect();
        manager.install_test_managed(Arc::clone(&client), 7001);
        let _ = release.send(());

        assert_eq!(
            set.await.expect("set task"),
            Err(BridgeError::Superseded),
            "an old confirmed write must not surface after reconnect"
        );
        assert!(
            !manager.rate_writer.poisoned.load(Ordering::SeqCst),
            "a definitive old success must not poison the writer"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 3);
        assert_eq!(
            rate_test_path(&captured[1]),
            format!("/rate-limit/global?rate={RATE}")
        );
        assert!(captured[2].request_line.starts_with("GET /settings "));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_ambiguous_write_still_reports_rate_unconfirmed() {
        const RATE: i64 = 4_100_000;
        let (base, server, started, release) = rate_test_spawn_gated_post(
            "500 Internal Server Error",
            "{}".to_string(),
            String::new(),
        );
        let manager = Arc::new(ConnectionManager::default());
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 7002).await;

        let set_manager = Arc::clone(&manager);
        let set = tokio::spawn(async move { set_manager.set_global_rate_limit(RATE).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("POST must reach the gated server");

        manager.disconnect();
        manager.install_test_managed(Arc::clone(&client), 7002);
        let _ = release.send(());

        assert_eq!(
            set.await.expect("set task"),
            Err(BridgeError::RateUnconfirmed),
            "a stale ambiguous write poisons globally and must stay visible"
        );
        assert!(
            manager.rate_writer.poisoned.load(Ordering::SeqCst),
            "an ambiguous old response must poison the writer"
        );
        assert_eq!(
            manager.set_global_rate_limit(0).await,
            Err(BridgeError::RateUnconfirmed),
            "poison must block any write to the replacement"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "poisoned writer must not send another request to the replacement"
        );
        assert_eq!(
            rate_test_path(&captured[1]),
            format!("/rate-limit/global?rate={RATE}")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_caller_cannot_release_gate_or_bypass_poison() {
        const RATE: i64 = 5_000_000;
        let (base, server, started, release) = rate_test_spawn_gated_post(
            "500 Internal Server Error",
            "{}".to_string(),
            String::new(),
        );
        let manager = Arc::new(ConnectionManager::default());
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 6001).await;

        let first_manager = Arc::clone(&manager);
        let first = tokio::spawn(async move { first_manager.set_global_rate_limit(RATE).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("POST must reach the gated server");
        first.abort();
        let _ = first.await;
        let _ = release.send(());

        // The background task still owns the gate and finishes the ambiguous
        // write before this second mutation can start.
        assert_eq!(
            manager.set_global_rate_limit(0).await,
            Err(BridgeError::RateUnconfirmed),
            "cancelling the caller must not bypass the poison"
        );
        assert!(manager.rate_writer.poisoned.load(Ordering::SeqCst));
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "the next mutation must be refused before any request"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_for_cancelled_rate_post_and_fences_queued_write() {
        const RATE: i64 = 9_000_000;
        const QUEUED_RATE: i64 = 9_500_000;
        let (base, server, started, release) =
            rate_test_spawn_gated_post("200 OK", rate_test_ack(RATE), rate_test_settings(RATE));
        let manager = Arc::new(ConnectionManager::default());
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 6101).await;

        let first_manager = Arc::clone(&manager);
        let first = tokio::spawn(async move { first_manager.set_global_rate_limit(RATE).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("POST must reach the gated server");

        let queued_manager = Arc::clone(&manager);
        let queued =
            tokio::spawn(async move { queued_manager.set_global_rate_limit(QUEUED_RATE).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let active = manager.lifecycle.lock().active;
            if active >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "queued caller must register its lifecycle operation"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Cancel the first caller, but its detached task still owns the gate,
        // lifecycle ticket, and in-flight POST.
        first.abort();
        let _ = first.await;

        let shutdown_manager = Arc::clone(&manager);
        let (drain_tx, drain_rx) = mpsc::channel();
        let shutdown = thread::spawn(move || {
            let drained = shutdown_manager.shutdown_managed();
            let _ = drain_tx.send(drained);
        });
        let shutdown_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if manager.lifecycle.is_shutting_down() {
                break;
            }
            assert!(
                std::time::Instant::now() < shutdown_deadline,
                "app shutdown did not start in time"
            );
            assert!(
                drain_rx.try_recv().is_err(),
                "app shutdown must wait for the detached rate POST"
            );
            thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            drain_rx.try_recv().is_err(),
            "app shutdown must wait for the detached rate POST"
        );

        let _ = release.send(());
        assert_eq!(
            queued.await.expect("queued set task"),
            Err(BridgeError::Superseded),
            "shutdown must fence the queued write before dispatch"
        );
        let drained = drain_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("shutdown must drain after the detached POST finishes");
        assert!(drained, "registered rate work must drain on shutdown");
        shutdown.join().expect("shutdown thread");
        assert_eq!(
            manager.set_global_rate_limit(0).await,
            Err(BridgeError::Superseded),
            "writes must be rejected after the shutdown fence"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            3,
            "no queued request may be sent after the shutdown fence"
        );
        assert_eq!(
            rate_test_path(&captured[1]),
            format!("/rate-limit/global?rate={RATE}")
        );
        assert!(captured[2].request_line.starts_with("GET /settings "));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn readback_failure_poisons_managed_writer() {
        const RATE: i64 = 6_000_000;
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", rate_test_ack(RATE)),
            ("500 Internal Server Error", "{}".to_string()),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 5001).await;

        assert_eq!(
            manager.set_global_rate_limit(RATE).await,
            Err(BridgeError::RateUnconfirmed)
        );
        assert!(manager.rate_writer.poisoned.load(Ordering::SeqCst));
        assert_eq!(
            manager.set_global_rate_limit(0).await,
            Err(BridgeError::RateUnconfirmed)
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 3, "poison must block a fourth request");
    }

    // -----------------------------------------------------------------------
    // Open-containing-folder command: id-only lookup, fresh list, validation
    // and final ownership fence against a loopback managed target.
    // -----------------------------------------------------------------------

    #[cfg(target_os = "linux")]
    struct RecordingLocationLauncher {
        calls: Mutex<Vec<PathBuf>>,
        result: Result<(), LocationError>,
    }

    #[cfg(target_os = "linux")]
    impl RecordingLocationLauncher {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                result: Ok(()),
            }
        }

        fn failing() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                result: Err(LocationError::LaunchFailed),
            }
        }

        fn calls(&self) -> Vec<PathBuf> {
            self.calls.lock().expect("launcher calls lock").clone()
        }
    }

    #[cfg(target_os = "linux")]
    impl LocationLauncher for RecordingLocationLauncher {
        fn launch_directory(&self, directory: &Path) -> Result<LocationLaunch, LocationError> {
            self.calls
                .lock()
                .expect("launcher calls lock")
                .push(directory.to_path_buf());
            match self.result {
                Ok(()) => Ok(LocationLaunch::test_result(Ok(()))),
                Err(error) => Err(error),
            }
        }
    }

    #[cfg(target_os = "linux")]
    struct GatedLocationLauncher {
        calls: Mutex<Vec<PathBuf>>,
        entered: Mutex<mpsc::Sender<()>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    #[cfg(target_os = "linux")]
    impl GatedLocationLauncher {
        fn calls(&self) -> Vec<PathBuf> {
            self.calls.lock().expect("gated calls lock").clone()
        }
    }

    #[cfg(target_os = "linux")]
    impl LocationLauncher for GatedLocationLauncher {
        fn launch_directory(&self, directory: &Path) -> Result<LocationLaunch, LocationError> {
            self.calls
                .lock()
                .expect("gated calls lock")
                .push(directory.to_path_buf());
            let _ = self.entered.lock().expect("gated entered lock").send(());
            let _ = self
                .release
                .lock()
                .expect("gated release lock")
                .recv_timeout(std::time::Duration::from_secs(30));
            Ok(LocationLaunch::test_result(Ok(())))
        }
    }

    #[cfg(target_os = "linux")]
    fn open_location_test_item(
        id: &str,
        destination: Option<&str>,
        status: &str,
        moving: bool,
        file_missing: bool,
    ) -> serde_json::Value {
        let mut item = serde_json::json!({
            "id": id,
            "url": "https://dl.example.test/file.bin",
            "filename": "file.bin",
            "total_size": 9,
            "downloaded": 9,
            "progress": 100.0,
            "speed": 0.0,
            "status": status,
            "eta": 0,
            "moving": moving,
            "file_missing": file_missing,
        });
        if let Some(destination) = destination {
            item["dest_path"] = serde_json::Value::String(destination.to_string());
        }
        item
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hash_coordinator_persists_app_default_and_intents() {
        let root = fresh_dir("hash-coordinator");
        let coordinator = HashCoordinator::default();
        coordinator.ensure_loaded(&root).expect("load state");
        assert!(!coordinator.default_enabled().expect("default"));
        assert!(coordinator.set_default_enabled(true).expect("set default"));

        let intents = [
            HashIntent {
                id: "d-1".into(),
                enabled: true,
            },
            HashIntent {
                id: "d-1".into(),
                enabled: false,
            },
        ];
        coordinator
            .record_intents(&intents)
            .expect("record intents");
        assert!(coordinator.candidates("conn-a").is_empty());
        let visible = coordinator.visible(Some("conn-a"));
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].state, "disabled");

        drop(coordinator);
        let reloaded = HashCoordinator::default();
        reloaded.ensure_loaded(&root).expect("reload state");
        assert!(reloaded.default_enabled().expect("persisted default"));
        assert_eq!(reloaded.visible(Some("conn-a"))[0].state, "disabled");
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    fn persistence_fault(
        dir: &Path,
        pause: Option<&str>,
        fail: Option<&str>,
    ) -> crate::hash_persistence::FaultConfig {
        crate::hash_persistence::FaultConfig {
            pause: pause.map(str::to_owned),
            fail: fail.map(str::to_owned),
            cleanup: None,
            dir: Some(dir.to_path_buf()),
        }
    }

    #[cfg(target_os = "linux")]
    fn wait_for_path(path: &Path, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if path.exists() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        path.exists()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn before_rename_failures_are_definitive_and_leave_the_domain_usable() {
        let root = fresh_dir("hash-before-rename");
        let state_path = root.join(AUTOMATIC_HASH_FILE);
        let coordinator = HashCoordinator::default();
        coordinator.set_persistence_fault(Some(persistence_fault(&root, None, Some("write"))));
        coordinator.ensure_loaded(&root).expect("load state");

        for point in ["write", "file_sync", "rename"] {
            coordinator
                .domain()
                .expect("owner")
                .set_fault(Some(persistence_fault(&root, None, Some(point))))
                .expect("arm fault");
            assert_eq!(
                coordinator.set_default_enabled(true),
                Err(BridgeError::State),
                "failure before rename is definitive ({point})"
            );
            assert!(!state_path.exists(), "nothing may be published ({point})");
            assert!(
                coordinator.persistence_confirmed(),
                "a before-rename failure must not quarantine ({point})"
            );
            assert!(!coordinator.default_enabled().expect("default still known"));
        }
        coordinator
            .domain()
            .expect("owner")
            .set_fault(None)
            .expect("clear fault");
        assert!(coordinator
            .set_default_enabled(true)
            .expect("retry succeeds"));
        drop(coordinator);
        let reloaded = HashCoordinator::default();
        reloaded.ensure_loaded(&root).expect("reload");
        assert!(reloaded.default_enabled().expect("persisted"));
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn directory_sync_failure_is_published_but_reported_unconfirmed() {
        let root = fresh_dir("hash-dir-sync");
        let state_path = root.join(AUTOMATIC_HASH_FILE);
        let coordinator = HashCoordinator::default();
        coordinator.set_persistence_fault(Some(persistence_fault(&root, None, Some("dir_sync"))));
        coordinator.ensure_loaded(&root).expect("load state");

        assert_eq!(
            coordinator.set_default_enabled(true),
            Err(BridgeError::HashUnconfirmed)
        );
        assert!(!coordinator.persistence_confirmed());
        assert_eq!(
            coordinator.set_default_enabled(false),
            Err(BridgeError::HashUnconfirmed),
            "a quarantined owner is never replaced by a new writer"
        );
        let bytes = std::fs::read(&state_path).expect("published file");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
        assert_eq!(json["defaultEnabled"], true, "the rename really published");
        drop(coordinator);

        let reloaded = HashCoordinator::default();
        assert_eq!(
            reloaded.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "a new owner must not confirm a publication whose directory sync failed"
        );
        assert!(root.join("automatic-hash.pending").exists());
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn crash_after_rename_leaves_publication_unconfirmed_for_the_next_owner() {
        let root = fresh_dir("hash-crash-rename");
        let coordinator = HashCoordinator::default();
        coordinator.set_persistence_fault(Some(persistence_fault(
            &root,
            None,
            Some("crash_after_rename"),
        )));
        coordinator.ensure_loaded(&root).expect("load state");
        assert_eq!(
            coordinator.set_default_enabled(true),
            Err(BridgeError::HashUnconfirmed)
        );
        assert!(!coordinator.persistence_confirmed());
        drop(coordinator);

        let reloaded = HashCoordinator::default();
        assert_eq!(
            reloaded.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "a crash after rename must not be silently confirmed by a successor"
        );
        assert!(root.join("automatic-hash.pending").exists());
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_hash_reads_fail_unconfirmed_on_a_pending_marker_while_the_queue_continues() {
        let root = fresh_dir("hash-command-pending");
        let marker = root.join(crate::hash_persistence::HASH_PENDING_FILE);
        std::fs::write(
            &marker,
            b"SURGUI-HASH-MARKER-2 pending 1 0000000000000000000000000000000000000000000000000000000000000000\n",
        )
        .expect("write real pending marker");
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", RATE_TEST_LIST.to_string()),
        ]);

        let manager = ConnectionManager::default();
        assert_eq!(
            manager.hashes.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "a real pending marker must not be silently confirmed"
        );
        assert_eq!(
            manager.hashes.initialization_failure(),
            Some(BridgeError::HashUnconfirmed),
            "the initialization failure must be recorded for later readers"
        );
        install_rate_test_managed(&manager, &base, 4242).await;

        // The download queue is independent of automatic-hash persistence.
        let queue = manager
            .refresh()
            .await
            .expect("managed queue must continue with unresolved hash state");
        assert_eq!(queue.len(), 1);

        assert_eq!(
            manager
                .get_download_hashes_checked()
                .await
                .expect_err("get must not return an empty success"),
            BridgeError::HashUnconfirmed,
            "get must not turn unresolved persistence into an empty success"
        );
        assert_eq!(
            manager
                .refresh_download_hashes_checked()
                .await
                .expect_err("refresh must not return an empty success"),
            BridgeError::HashUnconfirmed,
            "refresh must not turn unresolved persistence into an empty success"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "only connect and the queue refresh may reach the daemon"
        );

        // A recorded terminal failure is observable without reopening the same
        // unresolved directory on every read.
        std::fs::remove_file(&marker).expect("remove marker");
        assert_eq!(
            manager.hashes.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "the recorded failure must remain observable without a new attempt"
        );

        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_hash_reads_return_normal_empty_success_when_confirmed() {
        let root = fresh_dir("hash-command-empty");
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", "[]".to_string()),
            ("200 OK", "[]".to_string()),
            ("200 OK", "[]".to_string()),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load confirmed hash state");
        install_rate_test_managed(&manager, &base, 4242).await;

        assert!(
            manager
                .get_download_hashes_fenced(|| Ok(()))
                .await
                .expect("confirmed empty get")
                .is_empty(),
            "confirmed persistence with no records is a real empty success"
        );
        assert!(
            manager
                .refresh_download_hashes_fenced(|| Ok(()))
                .await
                .expect("confirmed empty refresh")
                .is_empty(),
            "confirmed persistence with no records is a real empty success"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            3,
            "connect, get, and refresh must each contact the daemon exactly once"
        );
        for request in &captured {
            assert_eq!(
                rate_test_path(request),
                "/list",
                "every hash read must use the authenticated queue probe"
            );
        }
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_hash_reads_fail_closed_when_the_daemon_list_fails() {
        for (status, expected) in [
            ("500 Internal Server Error", BridgeError::InvalidResponse),
            ("DROP", BridgeError::Network),
        ] {
            let root = fresh_dir("hash-command-daemon-failure");
            let file = root.join("failed.bin");
            std::fs::write(&file, b"abc").expect("write fixture");
            let (base, server) = rate_test_spawn_scripted(vec![
                ("200 OK", RATE_TEST_LIST.to_string()),
                (status, "{}".to_string()),
                (status, "{}".to_string()),
            ]);
            let manager = ConnectionManager::default();
            manager
                .hashes
                .ensure_loaded(&root)
                .expect("load confirmed hash state");
            install_rate_test_managed(&manager, &base, 4242).await;
            seed_completed_hash(&manager, &root, "h-failed-read", 42, &file);
            // The refresh fixture triggers the worker, but this test is about
            // the synchronous command body. Stop the worker before it can race
            // the scripted responses.
            manager.lifecycle.begin_shutdown();

            assert_eq!(
                manager.get_download_hashes_fenced(|| Ok(())).await.err(),
                Some(expected),
                "get must surface the daemon verification failure"
            );
            assert_eq!(
                manager.refresh_download_hashes_checked().await.err(),
                Some(expected),
                "refresh must surface the daemon verification failure"
            );
            let stored = manager.download_hashes();
            let row = hash_row(&stored, "h-failed-read");
            assert_eq!(
                row.state, "complete",
                "a failed verification must not mutate the persisted digest"
            );
            assert!(
                row.md5.is_some(),
                "the withheld digest must remain persisted for a later retry"
            );

            let captured = server.join().expect("server thread panicked");
            assert_eq!(
                captured.len(),
                3,
                "connect, get, and refresh must each contact the daemon exactly once"
            );
            for request in &captured {
                assert_eq!(
                    rate_test_path(request),
                    "/list",
                    "a failed read must still be an authenticated queue probe"
                );
            }
            manager.hashes.cancel_current();
            remove_all_best_effort(&root);
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_hash_reads_recover_after_a_daemon_list_failure() {
        let root = fresh_dir("hash-command-daemon-recovery");
        let file = root.join("recovered.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let recovery_list = serde_json::json!([hash_test_item(
            "h-recovery",
            &destination,
            "completed",
            None,
            false,
            true,
            42,
            3,
            3
        )])
        .to_string();

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("500 Internal Server Error", "{}".to_string()),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load confirmed hash state");
        install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-recovery", 42, &file);
        manager.lifecycle.begin_shutdown();
        assert_eq!(
            manager.get_download_hashes_fenced(|| Ok(())).await.err(),
            Some(BridgeError::InvalidResponse)
        );
        server.join().expect("server thread panicked");

        // A later, successful authoritative read on the same owned persistence
        // serves the same digest without requiring a new hash computation.
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", recovery_list),
        ]);
        install_rate_test_managed(&manager, &base, 4343).await;
        let rows = manager
            .get_download_hashes_fenced(|| Ok(()))
            .await
            .expect("normal recovery succeeds");
        let row = hash_row(&rows, "h-recovery");
        assert_eq!(row.state, "complete");
        assert_eq!(row.md5.as_deref(), Some("900150983cd24fb0d6963f7d28e17f72"));
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "recovery is one connect probe and one authoritative /list"
        );
        for request in &captured {
            assert_eq!(rate_test_path(request), "/list");
        }
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn manual_and_offline_hash_reads_keep_a_positive_empty_result() {
        let root = fresh_dir("hash-command-manual-offline");
        let file = root.join("manual.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load confirmed hash state");
        seed_completed_hash(&manager, &root, "h-manual", 42, &file);
        manager
            .connect(&base, "manual-token")
            .await
            .expect("manual connection");
        assert!(
            manager
                .get_download_hashes_checked()
                .await
                .expect("manual reads stay positive")
                .is_empty(),
            "manual connections never surface managed digests"
        );
        assert!(
            manager
                .refresh_download_hashes_checked()
                .await
                .expect("manual refresh stays positive")
                .is_empty(),
            "manual refresh never surfaces managed digests"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            1,
            "only the manual connect may use the daemon"
        );
        manager.disconnect();
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);

        // Offline/disconnected readers keep the same positive empty result and
        // never probe a daemon they do not own.
        let root = fresh_dir("hash-command-offline");
        let offline = ConnectionManager::default();
        offline
            .hashes
            .ensure_loaded(&root)
            .expect("load confirmed hash state");
        assert!(offline
            .get_download_hashes_checked()
            .await
            .expect("offline reads stay positive")
            .is_empty());
        assert!(offline
            .refresh_download_hashes_checked()
            .await
            .expect("offline refresh stays positive")
            .is_empty());
        offline.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn daemon_list_failure_after_a_lifecycle_transition_is_superseded() {
        let root = fresh_dir("hash-command-daemon-superseded");
        let file = root.join("superseded.bin");
        std::fs::write(&file, b"abc").expect("write fixture");

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut captured = Vec::new();
            let mut connect = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut connect));
            rate_test_write_response(&mut connect, "200 OK", RATE_TEST_LIST);

            let mut list = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut list));
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            rate_test_write_response(&mut list, "500 Internal Server Error", "{}");
            captured
        });
        let base = format!("http://{addr}");

        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load confirmed hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-1", 42, &file);
        manager.lifecycle.begin_shutdown();

        let read = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get_download_hashes_fenced(|| Ok(())).await }
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("verification started its /list request");
        manager.install_test_managed(Arc::clone(&client), 4343);
        release_tx
            .send(())
            .expect("release the stale failure response");

        assert_eq!(
            read.await.expect("read task").err(),
            Some(BridgeError::Superseded),
            "a failure from a replaced lifecycle must not surface as its error"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 2, "connect and the superseded /list only");
        for request in &captured {
            assert_eq!(rate_test_path(request), "/list");
        }
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn manual_connections_keep_empty_hash_rows_with_unresolved_persistence() {
        let root = fresh_dir("hash-command-manual");
        std::fs::write(
            root.join(crate::hash_persistence::HASH_PENDING_FILE),
            b"SURGUI-HASH-MARKER-2 pending 1 0000000000000000000000000000000000000000000000000000000000000000\n",
        )
        .expect("write real pending marker");
        let (endpoint, captured, server) = manual_plain_listener(b"[]", b"200 OK");

        let manager = ConnectionManager::default();
        assert_eq!(
            manager.hashes.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed)
        );
        manager
            .connect(&endpoint, "manual-token")
            .await
            .expect("manual connect must succeed independently");

        assert!(manager
            .get_download_hashes_checked()
            .await
            .expect("manual get keeps existing behavior")
            .is_empty());
        assert!(manager
            .refresh_download_hashes_checked()
            .await
            .expect("manual refresh keeps existing behavior")
            .is_empty());
        assert_eq!(
            manager.hashes.initialization_failure(),
            Some(BridgeError::HashUnconfirmed),
            "manual behavior must not erase the recorded managed failure"
        );

        let request = captured
            .recv_timeout(Duration::from_secs(5))
            .expect("captured manual request");
        server.join().expect("manual server thread");
        let text = String::from_utf8_lossy(&request);
        assert!(text.starts_with("GET /list HTTP/1.1"));

        manager.disconnect();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn foreign_state_change_is_detected_without_overwriting_it() {
        let root = fresh_dir("hash-external-change");
        let state_path = root.join(AUTOMATIC_HASH_FILE);
        let coordinator = HashCoordinator::default();
        coordinator.ensure_loaded(&root).expect("load state");
        let foreign = br#"{"version":2,"defaultEnabled":true,"records":[]}"#;
        std::fs::write(&state_path, foreign).expect("foreign write");

        assert_eq!(
            coordinator.set_default_enabled(false),
            Err(BridgeError::HashUnconfirmed)
        );
        assert_eq!(
            std::fs::read(&state_path).expect("foreign state preserved"),
            foreign
        );
        assert!(!coordinator.persistence_confirmed());
        drop(coordinator);
        let reloaded = HashCoordinator::default();
        reloaded.ensure_loaded(&root).expect("load foreign state");
        assert!(reloaded.default_enabled().expect("foreign value retained"));
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn malformed_state_and_namespace_are_never_regenerated() {
        let root = fresh_dir("hash-malformed-state");
        let state_path = root.join(AUTOMATIC_HASH_FILE);
        std::fs::write(&state_path, b"{not json").expect("malformed state");
        let coordinator = HashCoordinator::default();
        assert_eq!(coordinator.ensure_loaded(&root), Err(BridgeError::State));
        assert_eq!(
            std::fs::read(&state_path).expect("state preserved"),
            b"{not json"
        );
        drop(coordinator);
        remove_all_best_effort(&root);

        let root = fresh_dir("hash-malformed-namespace");
        let namespace_path = root.join(crate::hash_state::HASH_NAMESPACE_FILE);
        std::fs::write(&namespace_path, b"bad-namespace").expect("malformed namespace");
        let coordinator = HashCoordinator::default();
        assert_eq!(coordinator.ensure_loaded(&root), Err(BridgeError::State));
        assert_eq!(
            std::fs::read(&namespace_path).expect("namespace preserved"),
            b"bad-namespace"
        );
        assert!(!root.join(AUTOMATIC_HASH_FILE).exists());
        drop(coordinator);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn second_owner_instance_fails_closed_until_the_first_really_dies() {
        let root = fresh_dir("hash-second-instance");
        let first = ConnectionManager::default();
        first.hashes.ensure_loaded(&root).expect("first owner");
        assert!(first.hashes.set_default_enabled(true).expect("write"));

        let second = ConnectionManager::default();
        assert_eq!(
            second.hashes.ensure_loaded(&root),
            Err(BridgeError::State),
            "a live owner makes the directory busy"
        );
        assert_eq!(
            second.hashes.set_default_enabled(false),
            Err(BridgeError::State),
            "the busy instance must not write or reset state"
        );

        first.shutdown_managed();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match second.hashes.ensure_loaded(&root) {
                Ok(()) => break,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(error) => panic!("successor never acquired the released lock: {error:?}"),
            }
        }
        assert!(
            second.hashes.default_enabled().expect("successor default"),
            "the successor validates the winner's published state"
        );
        drop(second);
        drop(first);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn concurrent_first_run_has_a_single_exclusive_namespace_winner() {
        let root = fresh_dir("hash-namespace-race");
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let root = root.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                let manager = ConnectionManager::default();
                barrier.wait();
                let result = manager.hashes.ensure_loaded(&root);
                let namespace = manager.hashes.namespace();
                (manager, result, namespace)
            }));
        }
        let outcomes: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().expect("instance"))
            .collect();
        let ready: Vec<_> = outcomes
            .iter()
            .filter(|(_, result, _)| result.is_ok())
            .collect();
        assert_eq!(ready.len(), 1, "exactly one first-run owner wins");
        let winner_namespace = ready[0].2.clone().expect("winner namespace");
        assert!(outcomes
            .iter()
            .filter(|(_, result, _)| result.is_err())
            .all(|(_, _, namespace)| namespace.is_none()));
        // Drop both managers; the loser never created or replaced metadata.
        drop(outcomes);
        let reader = ConnectionManager::default();
        reader.hashes.ensure_loaded(&root).expect("successor loads");
        assert_eq!(
            reader.hashes.namespace().as_deref(),
            Some(winner_namespace.as_str())
        );
        drop(reader);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn load_and_namespace_barriers_are_deterministic() {
        for point in ["load", "namespace"] {
            let root = fresh_dir("hash-barrier");
            let manager = Arc::new(ConnectionManager::default());
            manager
                .hashes
                .set_persistence_fault(Some(persistence_fault(&root, Some(point), None)));
            let load = {
                let manager = Arc::clone(&manager);
                let root = root.clone();
                std::thread::spawn(move || manager.hashes.ensure_loaded(&root))
            };
            assert!(
                wait_for_path(
                    &root.join(format!("entered-{point}")),
                    Duration::from_secs(5)
                ),
                "helper must reach the {point} barrier"
            );
            std::fs::write(root.join(format!("release-{point}")), b"go").expect("release");
            assert_eq!(load.join().expect("load thread"), Ok(()));
            manager.hashes.begin_shutdown();
            drop(manager);
            remove_all_best_effort(&root);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn in_flight_publish_cannot_block_shutdown_or_surface_as_current() {
        let root = fresh_dir("hash-in-flight-publish");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(
                &root,
                Some("rename_in_progress"),
                None,
            )))
            .expect("arm rename barrier");
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let write = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let result = manager.hashes.set_default_enabled(true);
                let _ = done_tx.send(result);
            })
        };
        assert!(
            wait_for_path(
                &root.join("entered-rename_in_progress"),
                Duration::from_secs(5)
            ),
            "the rename must be genuinely in progress"
        );
        let started = Instant::now();
        let drained = manager.shutdown_managed();
        let elapsed = started.elapsed();
        assert!(drained, "shutdown must complete within its bound");
        assert!(
            elapsed < Duration::from_secs(4),
            "shutdown waited on the in-flight filesystem operation: {elapsed:?}"
        );
        let result = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the commit thread must observe cancellation");
        assert!(
            matches!(
                result,
                Err(BridgeError::Superseded) | Err(BridgeError::HashUnconfirmed)
            ),
            "the old lifecycle must not report success: {result:?}"
        );
        assert!(!manager.hashes.persistence_confirmed());
        assert_eq!(
            manager.hashes.default_enabled(),
            Err(BridgeError::HashUnconfirmed),
            "the published-but-unconfirmed value must not surface from stale memory"
        );
        write.join().expect("write thread");
        let _ = std::fs::write(root.join("release-rename_in_progress"), b"go");
        drop(manager);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn ambiguous_rename_timeout_quarantines_and_discards_late_completion() {
        let root = fresh_dir("hash-rename-timeout");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(
                &root,
                Some("rename_in_progress"),
                None,
            )))
            .expect("arm rename barrier");
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let write = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let result = manager.hashes.set_default_enabled(true);
                let _ = done_tx.send(result);
            })
        };
        assert!(wait_for_path(
            &root.join("entered-rename_in_progress"),
            Duration::from_secs(5)
        ));
        let result = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("commit must time out and quarantine");
        assert!(
            matches!(
                result,
                Err(BridgeError::HashUnconfirmed) | Err(BridgeError::Superseded)
            ),
            "an unresolved rename is not a success: {result:?}"
        );
        assert!(!manager.hashes.persistence_confirmed());
        assert_eq!(
            manager.hashes.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "no replacement writer starts while the old operation is unresolved"
        );
        assert_eq!(
            manager.hashes.default_enabled(),
            Err(BridgeError::HashUnconfirmed)
        );
        // A late helper completion cannot be observed after the domain was
        // quarantined; the release file only exists to stop test threads.
        let _ = std::fs::write(root.join("release-rename_in_progress"), b"go");
        write.join().expect("write thread");
        drop(manager);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rename_errors_are_ambiguous_and_pending_marker_survives_restart() {
        for fault in ["rename_error", "rename_after_effect"] {
            let root = fresh_dir("hash-rename-ambiguous");
            let state_path = root.join(AUTOMATIC_HASH_FILE);
            let coordinator = HashCoordinator::default();
            coordinator.set_persistence_fault(Some(persistence_fault(&root, None, Some(fault))));
            coordinator.ensure_loaded(&root).expect("load state");
            assert_eq!(
                coordinator.set_default_enabled(true),
                Err(BridgeError::HashUnconfirmed),
                "a rename syscall error is ambiguous ({fault})"
            );
            assert!(
                !coordinator.persistence_confirmed(),
                "an ambiguous rename must quarantine, not report rollback ({fault})"
            );
            if fault == "rename_after_effect" {
                let bytes = std::fs::read(&state_path).expect("published file");
                let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
                assert_eq!(json["defaultEnabled"], true, "the rename really published");
            }
            drop(coordinator);

            // The pending marker is durable before the rename. A new owner must
            // not silently confirm either outcome after the process exits.
            let reloaded = HashCoordinator::default();
            assert_eq!(
                reloaded.ensure_loaded(&root),
                Err(BridgeError::HashUnconfirmed),
                "restart must not lose the unresolved publication ({fault})"
            );
            assert!(root.join("automatic-hash.pending").exists());
            remove_all_best_effort(&root);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_migration_or_invalid_marker_fails_closed_on_the_next_owner() {
        let root = fresh_dir("hash-migration-pending");
        let state_path = root.join(AUTOMATIC_HASH_FILE);
        std::fs::write(
            &state_path,
            br#"{ "version": 2, "defaultEnabled": true, "records": [] }"#,
        )
        .expect("legacy spacing needs normalization");
        let coordinator = HashCoordinator::default();
        coordinator.set_persistence_fault(Some(persistence_fault(&root, None, Some("dir_sync"))));
        assert_eq!(
            coordinator.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "an ambiguous initialization publication must not become generic State"
        );
        drop(coordinator);
        assert!(root.join("automatic-hash.pending").exists());

        let reloaded = HashCoordinator::default();
        assert_eq!(
            reloaded.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "a failed migration publication is still unresolved"
        );

        let invalid = fresh_dir("hash-invalid-pending");
        std::fs::write(
            invalid.join("automatic-hash.pending"),
            b"not a pending marker",
        )
        .expect("invalid marker");
        let invalid_owner = HashCoordinator::default();
        assert_eq!(
            invalid_owner.ensure_loaded(&invalid),
            Err(BridgeError::HashUnconfirmed),
            "an invalid marker is never treated as confirmation"
        );
        assert_eq!(
            std::fs::read(invalid.join("automatic-hash.pending")).expect("marker preserved"),
            b"not a pending marker"
        );
        remove_all_best_effort(&root);
        remove_all_best_effort(&invalid);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn initialization_definite_pre_rename_failure_stays_distinct_from_ambiguity() {
        let root = fresh_dir("hash-init-definite");
        let state_path = root.join(AUTOMATIC_HASH_FILE);
        std::fs::write(
            &state_path,
            br#"{ "version": 2, "defaultEnabled": true, "records": [] }"#,
        )
        .expect("legacy spacing needs normalization");
        let coordinator = HashCoordinator::default();
        coordinator.set_persistence_fault(Some(persistence_fault(&root, None, Some("write"))));
        assert_eq!(
            coordinator.ensure_loaded(&root),
            Err(BridgeError::State),
            "a definite failure before rename is not an ambiguous publication"
        );
        assert!(!root.join("automatic-hash.pending").exists());
        assert!(
            std::fs::read(&state_path)
                .expect("previous state remains")
                .windows(b"\"version\": 2".len())
                .any(|window| window == b"\"version\": 2"),
            "the pre-existing state must not be rewritten"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn initialization_helper_exit_after_publication_maps_to_unconfirmed() {
        let root = fresh_dir("hash-init-crash");
        std::fs::write(
            root.join(AUTOMATIC_HASH_FILE),
            br#"{ "version": 2, "defaultEnabled": true, "records": [] }"#,
        )
        .expect("legacy spacing needs normalization");
        let coordinator = HashCoordinator::default();
        coordinator.set_persistence_fault(Some(persistence_fault(
            &root,
            None,
            Some("crash_after_rename"),
        )));
        assert_eq!(
            coordinator.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "a helper that exits after the rename is ambiguous, not generic State"
        );
        assert!(root
            .join(crate::hash_persistence::HASH_PENDING_FILE)
            .exists());
        drop(coordinator);

        let reloaded = HashCoordinator::default();
        assert_eq!(
            reloaded.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "the successor must still fail closed on the pending marker"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_fences_a_stale_manual_snapshot_before_managed_failure() {
        use std::sync::Barrier;

        let manager = Arc::new(ConnectionManager::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.active = ActiveConnection::Manual;
            state.epoch = 11;
        }
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let switcher = {
            let manager = Arc::clone(&manager);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                entered.wait();
                {
                    let mut state = manager.state.lock().unwrap();
                    state.active = ActiveConnection::Managed;
                    state.epoch = state.epoch.wrapping_add(1);
                }
                release.wait();
            })
        };
        let ensure = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                entered.wait();
                release.wait();
                Err(BridgeError::HashUnconfirmed)
            }
        };

        let result = manager.refresh_download_hashes_fenced(ensure).await;
        switcher.join().expect("switcher thread");
        assert!(
            matches!(result, Err(BridgeError::HashUnconfirmed)),
            "a stale manual snapshot must not turn a managed failure into empty success: {result:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_success_is_fenced_against_a_concurrent_mode_change() {
        use std::sync::Barrier;

        let manager = Arc::new(ConnectionManager::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.active = ActiveConnection::Manual;
            state.epoch = 12;
        }
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let switcher = {
            let manager = Arc::clone(&manager);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                entered.wait();
                {
                    let mut state = manager.state.lock().unwrap();
                    state.active = ActiveConnection::Managed;
                    state.epoch = state.epoch.wrapping_add(1);
                }
                release.wait();
            })
        };
        let ensure = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                entered.wait();
                release.wait();
                Ok(())
            }
        };

        let result = manager.refresh_download_hashes_fenced(ensure).await;
        switcher.join().expect("switcher thread");
        assert!(
            matches!(result, Err(BridgeError::Superseded)),
            "rows initialized for the old mode must not be returned after a transition: {result:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_fences_a_managed_initialization_failure_across_a_transition() {
        use std::sync::Barrier;

        for (next_mode, label) in [
            (ActiveConnection::Manual, "managed/manual"),
            (ActiveConnection::Managed, "managed/reconnect"),
        ] {
            let manager = Arc::new(ConnectionManager::default());
            {
                let mut state = manager.state.lock().unwrap();
                state.active = ActiveConnection::Managed;
                state.epoch = 21;
            }
            let entered = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            let switcher = {
                let manager = Arc::clone(&manager);
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                std::thread::spawn(move || {
                    entered.wait();
                    {
                        let mut state = manager.state.lock().unwrap();
                        state.active = next_mode;
                        state.epoch = state.epoch.wrapping_add(1);
                    }
                    release.wait();
                })
            };
            let ensure = {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move || {
                    entered.wait();
                    release.wait();
                    Err(BridgeError::HashUnconfirmed)
                }
            };

            let result = manager.get_download_hashes_fenced(ensure).await;
            switcher.join().expect("switcher thread");
            assert!(
                matches!(result, Err(BridgeError::Superseded)),
                "a {label} transition during managed initialization must supersede its stale error: {result:?}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_success_is_fenced_when_the_mode_changes_during_ensure() {
        use std::sync::Barrier;

        let manager = Arc::new(ConnectionManager::default());
        {
            let mut state = manager.state.lock().unwrap();
            state.active = ActiveConnection::Managed;
            state.epoch = 22;
        }
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let switcher = {
            let manager = Arc::clone(&manager);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                entered.wait();
                {
                    let mut state = manager.state.lock().unwrap();
                    state.active = ActiveConnection::Manual;
                    state.epoch = state.epoch.wrapping_add(1);
                }
                release.wait();
            })
        };
        let ensure = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                entered.wait();
                release.wait();
                Ok(())
            }
        };

        let result = manager.get_download_hashes_fenced(ensure).await;
        switcher.join().expect("switcher thread");
        assert!(
            matches!(result, Err(BridgeError::Superseded)),
            "a transition during ensure must not return the old lifecycle's rows: {result:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn get_manual_connections_skip_managed_initialization() {
        let manager = ConnectionManager::default();
        {
            let mut state = manager.state.lock().unwrap();
            state.active = ActiveConnection::Manual;
            state.epoch = 23;
        }

        let rows = manager
            .get_download_hashes_fenced(|| panic!("manual get must not initialize managed state"))
            .await
            .expect("manual get keeps its empty-row behavior");
        assert!(rows.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_is_fenced_after_the_verified_snapshot_awaits() {
        let root = fresh_dir("hash-refresh-post-await");
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", "[]".to_string()),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load confirmed hash state");
        install_rate_test_managed(&manager, &base, 4242).await;

        let (entered_rx, release_tx) = manager.verified_pause.arm();
        let refresh = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.refresh_download_hashes_fenced(|| Ok(())).await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("verification reached the publication barrier");

        // Change the connection while the verified snapshot is paused after
        // its filesystem/network awaits. The production refresh path must not
        // return the old connection's now-stale empty snapshot.
        {
            let mut state = manager.state.lock().expect("state lock");
            state.active = ActiveConnection::Manual;
            state.epoch = state.epoch.wrapping_add(1);
        }
        release_tx.send(()).expect("release publication");
        let result = refresh.await.expect("refresh task");
        manager.verified_pause.clear();
        server.join().expect("server thread panicked");

        assert!(
            matches!(result, Err(BridgeError::Superseded)),
            "a transition after the snapshot await must not surface a stale empty success: {result:?}"
        );
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_is_fenced_after_the_verified_snapshot_awaits() {
        let root = fresh_dir("hash-get-post-await");
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", "[]".to_string()),
            ("200 OK", "[]".to_string()),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load confirmed hash state");
        install_rate_test_managed(&manager, &base, 4242).await;

        // A get that is not overtaken by a transition still returns the
        // verified snapshot as a normal success.
        let (entered_rx, release_tx) = manager.verified_pause.arm();
        let get = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get_download_hashes_fenced(|| Ok(())).await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("valid get reached the publication barrier");
        release_tx.send(()).expect("release valid publication");
        let result = get.await.expect("valid get task");
        manager.verified_pause.clear();
        assert!(
            matches!(result, Ok(ref rows) if rows.is_empty()),
            "a current verified snapshot must remain a normal empty success: {result:?}"
        );

        // Change the connection while a second get is paused after its
        // filesystem/network awaits. The production get path must not return
        // the old connection's now-stale empty snapshot.
        let (entered_rx, release_tx) = manager.verified_pause.arm();
        let get = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get_download_hashes_fenced(|| Ok(())).await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("stale get reached the publication barrier");
        {
            let mut state = manager.state.lock().expect("state lock");
            state.active = ActiveConnection::Manual;
            state.epoch = state.epoch.wrapping_add(1);
        }
        release_tx.send(()).expect("release stale publication");
        let result = get.await.expect("stale get task");
        manager.verified_pause.clear();
        server.join().expect("server thread panicked");

        assert!(
            matches!(result, Err(BridgeError::Superseded)),
            "a transition after the snapshot await must not surface a stale empty get: {result:?}"
        );
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn shutdown_during_load_or_namespace_never_installs_an_owner() {
        for point in ["load", "namespace"] {
            let root = fresh_dir("hash-shutdown-init");
            let manager = Arc::new(ConnectionManager::default());
            manager
                .hashes
                .set_persistence_fault(Some(persistence_fault(&root, Some(point), None)));
            let load = {
                let manager = Arc::clone(&manager);
                let root = root.clone();
                std::thread::spawn(move || manager.hashes.ensure_loaded(&root))
            };
            assert!(
                wait_for_path(
                    &root.join(format!("entered-{point}")),
                    Duration::from_secs(5)
                ),
                "helper must reach the {point} barrier"
            );
            manager.hashes.begin_shutdown();
            std::fs::write(root.join(format!("release-{point}")), b"go").expect("release");
            let result = load.join().expect("load thread");
            assert!(
                result.is_err(),
                "shutdown during {point} must reject initialization: {result:?}"
            );
            assert!(
                manager.hashes.domain().is_none(),
                "no owner may be installed after shutdown ({point})"
            );
            assert!(!manager.hashes.persistence_confirmed());
            assert!(
                manager.hashes.ensure_loaded(&root).is_err(),
                "shutdown must prevent launching a replacement owner ({point})"
            );
            assert!(manager.hashes.domain().is_none());
            drop(manager);
            remove_all_best_effort(&root);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn timed_out_prelock_open_is_one_attempt_per_directory_and_process() {
        use std::time::Instant;

        let root = fresh_dir("hash-open-timeout");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .set_persistence_fault(Some(persistence_fault(&root, Some("load"), None)));
        let first = {
            let manager = Arc::clone(&manager);
            let root = root.clone();
            std::thread::spawn(move || manager.hashes.ensure_loaded(&root))
        };
        assert!(
            wait_for_path(&root.join("entered-load"), Duration::from_secs(5)),
            "helper must hang before acquiring the lock"
        );
        assert!(
            first.join().expect("first open thread").is_err(),
            "the timed-out open must fail closed"
        );
        let attempts = |root: &Path| {
            std::fs::read_dir(root)
                .expect("attempt dir")
                .flatten()
                .filter(|entry| {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    name.starts_with("entered-load-")
                })
                .count()
        };
        assert_eq!(attempts(&root), 1, "exactly one helper attempt was made");

        let started = Instant::now();
        let second = {
            let manager = Arc::clone(&manager);
            let root = root.clone();
            std::thread::spawn(move || manager.hashes.ensure_loaded(&root))
        };
        std::thread::sleep(Duration::from_millis(750));
        let finished = second.is_finished();
        if !finished {
            // Red path: release the unintended replacement helper so the test
            // can report the failure without leaving a paused process behind.
            let _ = std::fs::write(root.join("release-load"), b"go");
        }
        let result = second.join().expect("second open thread");
        assert!(
            finished,
            "a retry after an unresolved pre-lock attempt must not spawn another helper: {:?}",
            started.elapsed()
        );
        assert!(result.is_err());
        assert_eq!(
            attempts(&root),
            1,
            "no replacement helper may be spawned for the unresolved directory"
        );
        drop(manager);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn marker_prepublication_failure_rolls_back_and_keeps_the_domain_usable() {
        let root = fresh_dir("hash-marker-rollback");
        let coordinator = HashCoordinator::default();
        coordinator.ensure_loaded(&root).expect("load state");
        assert!(coordinator
            .set_default_enabled(true)
            .expect("initial write"));
        coordinator
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(
                &root,
                None,
                Some("marker_pending_dir_sync"),
            )))
            .expect("arm marker fault");

        assert_eq!(
            coordinator.set_default_enabled(false),
            Err(BridgeError::State),
            "a marker failure before rename is a definitive pre-publication failure"
        );
        assert!(
            !root.join("automatic-hash.pending").exists(),
            "the failed pending marker must be rolled back when cleanup succeeds"
        );
        assert!(coordinator.persistence_confirmed());
        assert!(
            coordinator
                .default_enabled()
                .expect("old state remains known"),
            "the healthy previous state is not stranded"
        );

        coordinator
            .domain()
            .expect("owner")
            .set_fault(None)
            .expect("clear fault");
        assert!(!coordinator.set_default_enabled(false).expect("retry"));
        drop(coordinator);
        let reloaded = HashCoordinator::default();
        reloaded.ensure_loaded(&root).expect("reload healthy state");
        assert!(!reloaded.default_enabled().expect("persisted retry"));
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_marker_cleanup_reports_unresolved_instead_of_claiming_rollback() {
        let root = fresh_dir("hash-marker-cleanup-failed");
        let coordinator = HashCoordinator::default();
        coordinator.ensure_loaded(&root).expect("load state");
        assert!(coordinator
            .set_default_enabled(true)
            .expect("initial write"));
        let mut fault = persistence_fault(&root, None, Some("marker_pending_dir_sync"));
        fault.cleanup = Some("marker_cleanup".into());
        coordinator
            .domain()
            .expect("owner")
            .set_fault(Some(fault))
            .expect("arm marker and cleanup fault");

        assert_eq!(
            coordinator.set_default_enabled(false),
            Err(BridgeError::HashUnconfirmed),
            "a marker whose cleanup also failed is unresolved, not rolled back"
        );
        assert!(root.join("automatic-hash.pending").exists());
        assert!(!coordinator.persistence_confirmed());
        drop(coordinator);

        let reloaded = HashCoordinator::default();
        assert_eq!(
            reloaded.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "the next owner must fail closed on the unresolved marker"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn acked_marker_cleanup_failures_remain_durable_known_across_restart() {
        for point in ["marker_unlink", "marker_unlink_sync"] {
            let root = fresh_dir("hash-marker-cleanup");
            let coordinator = HashCoordinator::default();
            coordinator.ensure_loaded(&root).expect("load state");
            coordinator
                .domain()
                .expect("owner")
                .set_fault(Some(persistence_fault(&root, None, Some(point))))
                .expect("arm cleanup fault");

            assert!(
                coordinator
                    .set_default_enabled(true)
                    .expect("authorized write"),
                "an already-acked durable write is confirmed even if cleanup fails ({point})"
            );
            if point == "marker_unlink" {
                let marker = std::fs::read_to_string(root.join("automatic-hash.pending"))
                    .expect("acked marker retained");
                assert!(
                    marker.contains("acked"),
                    "the retained marker records durable authorization ({point})"
                );
            }
            coordinator
                .domain()
                .expect("owner")
                .set_fault(None)
                .expect("clear fault");
            drop(coordinator);

            let reloaded = HashCoordinator::default();
            reloaded
                .ensure_loaded(&root)
                .expect("a durably acked state is accepted, not unconfirmed");
            assert!(reloaded.default_enabled().expect("persisted default"));
            assert!(
                !root.join("automatic-hash.pending").exists(),
                "recovery cleans up the acked marker ({point})"
            );
            remove_all_best_effort(&root);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn lost_ack_reply_is_reported_unconfirmed_but_the_authorized_state_is_durable() {
        let root = fresh_dir("hash-lost-ack");
        let coordinator = Arc::new(HashCoordinator::default());
        coordinator.ensure_loaded(&root).expect("load state");
        coordinator
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(
                &root,
                Some("marker_acked_ready"),
                None,
            )))
            .expect("arm ack barrier");
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let write = {
            let coordinator = Arc::clone(&coordinator);
            std::thread::spawn(move || {
                let result = coordinator.set_default_enabled(true);
                let _ = done_tx.send(result);
            })
        };
        assert!(
            wait_for_path(
                &root.join("entered-marker_acked_ready"),
                Duration::from_secs(5)
            ),
            "the helper must pause after the durable ack and before its reply"
        );
        let result = done_rx
            .recv_timeout(Duration::from_secs(6))
            .expect("the lost reply must time out");
        assert_eq!(
            result,
            Err(BridgeError::HashUnconfirmed),
            "a lost reply is not reported as success"
        );
        assert!(!coordinator.persistence_confirmed());
        let _ = std::fs::write(root.join("release-marker_acked_ready"), b"go");
        write.join().expect("write thread");
        drop(coordinator);

        let reloaded = HashCoordinator::default();
        reloaded
            .ensure_loaded(&root)
            .expect("the durably authorized state may be visible after a lost reply");
        assert!(
            reloaded.default_enabled().expect("authorized default"),
            "this is not an exactly-once or reply-derived outcome promise"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_lifecycle_commit_never_becomes_acked_and_fails_closed_after_restart() {
        let root = fresh_dir("hash-stale-epoch-restart");
        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let ticket = {
            let state = manager.state.lock().unwrap();
            manager
                .hash_write_ticket_locked(&state)
                .expect("managed hash ticket")
        };
        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(
                &root,
                Some("marker_durable_ready"),
                None,
            )))
            .expect("arm durable barrier");
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let write = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let result =
                    mutate_hash_at_ticket(&manager.state, &manager.hashes, &ticket, |slot| {
                        if !slot.loaded {
                            return Err(BridgeError::State);
                        }
                        slot.store
                            .record_intents(&[HashIntent {
                                id: "stale-epoch".to_owned(),
                                enabled: true,
                            }])
                            .map_err(|_| BridgeError::State)
                    });
                let _ = done_tx.send(result);
            })
        };
        assert!(
            wait_for_path(
                &root.join("entered-marker_durable_ready"),
                Duration::from_secs(5)
            ),
            "the state must be durable but not yet acknowledged"
        );
        manager.install_test_managed(Arc::clone(&client), 4343);
        let _ = std::fs::write(root.join("release-marker_durable_ready"), b"go");
        let result = done_rx
            .recv_timeout(Duration::from_secs(6))
            .expect("stale write must finish");
        assert!(
            matches!(
                result,
                Err(BridgeError::Superseded) | Err(BridgeError::HashUnconfirmed)
            ),
            "the stale lifecycle must not publish authority: {result:?}"
        );
        write.join().expect("write thread");
        let captured = server.join().expect("server thread");
        assert_eq!(captured.len(), 1);
        assert!(root.join("automatic-hash.pending").exists());
        drop(manager);

        let reloaded = HashCoordinator::default();
        assert_eq!(
            reloaded.ensure_loaded(&root),
            Err(BridgeError::HashUnconfirmed),
            "an unacknowledged stale publication must fail closed after restart"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn migration_is_normalized_by_the_isolated_owner() {
        let root = fresh_dir("hash-owner-migration");
        let state_path = root.join(AUTOMATIC_HASH_FILE);
        let md5 = "900150983cd24fb0d6963f7d28e17f72";
        let sha256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let fingerprint = crate::hash_state::queue_fingerprint("old-1", 1, "https://example.test");
        let legacy = format!(
            r#"{{"version":2,"defaultEnabled":true,"records":[{{"id":"old-1","enabled":true,"status":"complete","generation":4,"namespace":"00112233445566778899aabbccddeeff","addedAt":1,"fingerprint":"{fingerprint}","fileSize":3,"mtimeSec":1,"mtimeNsec":2,"fileDev":10,"fileIno":20,"md5":"{md5}","sha256":"{sha256}","resultVersion":2}}]}}"#
        );
        std::fs::write(&state_path, legacy.as_bytes()).expect("legacy state");
        let manager = ConnectionManager::default();
        manager.hashes.ensure_loaded(&root).expect("migrate load");
        assert!(
            manager.hashes.default_enabled().expect("default preserved"),
            "the v2 app default survives migration"
        );
        assert!(
            manager
                .hashes
                .candidates("00112233445566778899aabbccddeeff")
                .is_empty(),
            "legacy completed results are not served as current"
        );
        drop(manager);
        let bytes = std::fs::read(&state_path).expect("normalized state");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(json["records"][0]["status"], "stale");
        assert_eq!(json["records"][0]["generation"], 5);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[allow(clippy::too_many_arguments)]
    fn hash_test_item(
        id: &str,
        destination: &str,
        status: &str,
        phase: Option<&str>,
        processing: bool,
        processed: bool,
        added_at: i64,
        total: i64,
        downloaded: i64,
    ) -> serde_json::Value {
        let mut item = serde_json::json!({
            "id": id,
            "url": "https://dl.example.test/private-url",
            "filename": "abc.bin",
            "total_size": total,
            "downloaded": downloaded,
            "progress": 100.0,
            "speed": 0.0,
            "status": status,
            "eta": 0,
            "processing": processing,
            "processed": processed,
            "added_at": added_at,
            "dest_path": destination,
        });
        if let Some(phase) = phase {
            item["phase"] = serde_json::Value::String(phase.to_owned());
        }
        item
    }

    #[cfg(target_os = "linux")]
    fn hash_test_fingerprint(id: &str, added_at: i64) -> String {
        crate::hash_state::queue_fingerprint(id, added_at, "https://dl.example.test/private-url")
    }

    #[cfg(target_os = "linux")]
    const HASH_TEST_NO_HOOK: &str = r#"{"categories":{"category_enabled":false,"categories":[]}}"#;

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_hashes_completed_download_with_known_digests() {
        let root = fresh_dir("hash-known");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        let result = automatic_hash::hash_one(
            &client,
            "h-1",
            42,
            &hash_test_fingerprint("h-1", 42),
            &cancel,
            || true,
        )
        .await
        .expect("hash one");
        match result {
            automatic_hash::HashOneOutcome::Completed(completed) => {
                assert_eq!(completed.added_at, 42);
                assert_eq!(completed.md5, "900150983cd24fb0d6963f7d28e17f72");
                assert_eq!(
                    completed.sha256,
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                );
                assert_eq!(completed.size, 3);
            }
            other => panic!("expected completed hashes, got {other:?}"),
        }
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 5);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn automatic_hash_worker_completes_persisted_intent_in_background() {
        let root = fresh_dir("hash-worker");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
        ]);
        let manager = ConnectionManager::default();
        crate::hash_persistence::clear_persist_diagnostic();
        manager.hashes.ensure_loaded(&root).unwrap_or_else(|error| {
            panic!(
                "load hash state failed: {error:?}; diagnostic={:?}",
                crate::hash_persistence::last_persist_diagnostic()
            )
        });
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let _ = client;
        let namespace = manager.hashes.namespace().expect("queue namespace");
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-1".into(),
                    enabled: true,
                }],
                &[(
                    "h-1".into(),
                    crate::hash_state::HashProvenance {
                        namespace,
                        added_at: 42,
                        fingerprint: hash_test_fingerprint("h-1", 42),
                    },
                )],
            )
            .expect("record intent");
        manager.trigger_hashing();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let rows = manager.download_hashes();
            if let Some(row) = rows.iter().find(|row| row.id == "h-1") {
                if row.state == "complete" {
                    assert_eq!(row.md5.as_deref(), Some("900150983cd24fb0d6963f7d28e17f72"));
                    assert_eq!(
                        row.sha256.as_deref(),
                        Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                    );
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "hash worker did not publish in time"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 6);
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_pending_intent_resumes_once_on_the_same_owned_queue() {
        let root = fresh_dir("hash-restart-pending");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();

        // First app run saves a pending intent, then exits before hashing.
        let first = HashCoordinator::default();
        first.ensure_loaded(&root).expect("load state");
        let namespace = first.namespace().expect("queue namespace");
        first
            .record_intents_with(
                &[HashIntent {
                    id: "h-1".into(),
                    enabled: true,
                }],
                &[(
                    "h-1".into(),
                    crate::hash_state::HashProvenance {
                        namespace: namespace.clone(),
                        added_at: 42,
                        fingerprint: hash_test_fingerprint("h-1", 42),
                    },
                )],
            )
            .expect("record pending intent");
        drop(first);

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("reload hash state");
        assert_eq!(
            manager.hashes.namespace().as_deref(),
            Some(namespace.as_str()),
            "the same app-private queue must keep its namespace across restart"
        );
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 5151).await;
        manager.trigger_hashing();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let rows = manager.download_hashes();
            if rows
                .iter()
                .any(|row| row.id == "h-1" && row.state == "complete")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the saved pending intent did not resume after restart"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            6,
            "restart must hash the pending item exactly once"
        );
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_result_is_retained_after_restart_only_after_queue_checks() {
        let root = fresh_dir("hash-restart-complete");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
        ]);
        let first = ConnectionManager::default();
        first.hashes.ensure_loaded(&root).expect("load state");
        let (_client, _epoch) = install_rate_test_managed(&first, &base, 6161).await;
        let namespace = first.hashes.namespace().expect("queue namespace");
        first
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-1".into(),
                    enabled: true,
                }],
                &[(
                    "h-1".into(),
                    crate::hash_state::HashProvenance {
                        namespace,
                        added_at: 42,
                        fingerprint: hash_test_fingerprint("h-1", 42),
                    },
                )],
            )
            .expect("record intent");
        first.trigger_hashing();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while first
            .download_hashes()
            .iter()
            .find(|row| row.id == "h-1")
            .is_none_or(|row| row.state != "complete")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fixture did not complete hashing"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        first.hashes.cancel_current();
        server.join().expect("server thread panicked");
        drop(first);

        // Restart: a fresh manager/connection/pid sees the completed result
        // only after the same authoritative queue record is revalidated.
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
        ]);
        let second = ConnectionManager::default();
        second.hashes.ensure_loaded(&root).expect("reload state");
        let (_client, _epoch) = install_rate_test_managed(&second, &base, 7171).await;
        let rows = second
            .verified_download_hashes()
            .await
            .expect("verified restart rows");
        let row = rows.iter().find(|row| row.id == "h-1").expect("row");
        assert_eq!(row.state, "complete", "matching queue retains the result");
        assert!(row.md5.is_some());
        server.join().expect("server thread panicked");
        drop(second);

        // Restart again with a replaced/removed queue: the saved digest must
        // fail closed as stale before it is served.
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", "[]".to_string()),
        ]);
        let third = ConnectionManager::default();
        third.hashes.ensure_loaded(&root).expect("reload state");
        let (_client, _epoch) = install_rate_test_managed(&third, &base, 8181).await;
        let rows = third
            .verified_download_hashes()
            .await
            .expect("verified restart rows");
        let row = rows.iter().find(|row| row.id == "h-1").expect("row");
        assert_eq!(row.state, "stale");
        assert!(row.md5.is_none());
        assert!(row.sha256.is_none());
        server.join().expect("server thread panicked");
        third.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_cancelled_before_read_does_not_contact_daemon() {
        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(true));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Cancelled
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 1, "cancelled work must not fetch /list");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_blocks_completed_finalizing_and_hook_running() {
        let list = serde_json::json!([hash_test_item(
            "h-1",
            "/tmp/does-not-matter.bin",
            "completed",
            Some("finalizing"),
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Pending
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "finalizing must not open or hash the file"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_waits_for_configured_hook_completion() {
        let list = serde_json::json!([hash_test_item(
            "h-1",
            "/tmp/does-not-matter.bin",
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let hook = serde_json::json!({
            "categories": {
                "category_enabled": true,
                "categories": [{
                    "name": "All",
                    "pattern": ".*",
                    "path": "/tmp",
                    "post_download_command": "true"
                }]
            }
        })
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
            ("200 OK", hook),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Pending
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 3);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_treats_duplicate_and_missing_ids_safely() {
        let duplicate = serde_json::json!([
            hash_test_item("h-1", "/tmp/a", "completed", None, false, false, 42, 3, 3),
            hash_test_item("h-1", "/tmp/a", "completed", None, false, false, 42, 3, 3)
        ])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", duplicate),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Unavailable
        );
        server.join().expect("server thread panicked");

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", "[]".to_string()),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4243).await;
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Stale
        );
        server.join().expect("server thread panicked");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_does_not_publish_after_generation_change() {
        let root = fresh_dir("hash-generation");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let first = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let second = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            43,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", first),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", second),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Stale
        );
        server.join().expect("server thread panicked");
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn automatic_hash_worker_processes_every_pending_candidate() {
        let root = fresh_dir("hash-multiple-pending");
        let first_file = root.join("first.bin");
        let second_file = root.join("second.bin");
        std::fs::write(&first_file, b"abc").expect("write first fixture");
        std::fs::write(&second_file, b"abc").expect("write second fixture");
        let list = serde_json::json!([
            hash_test_item(
                "h-1",
                &first_file.to_string_lossy(),
                "completed",
                None,
                false,
                false,
                42,
                3,
                3
            ),
            hash_test_item(
                "h-2",
                &second_file.to_string_lossy(),
                "completed",
                None,
                false,
                false,
                43,
                3,
                3
            )
        ])
        .to_string();
        let no_hook = HASH_TEST_NO_HOOK.to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", list.clone()),
            ("200 OK", no_hook.clone()),
            ("200 OK", list.clone()),
            ("200 OK", no_hook.clone()),
            ("200 OK", list.clone()),
            ("200 OK", no_hook.clone()),
            ("200 OK", list.clone()),
            ("200 OK", no_hook),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let namespace = manager.hashes.namespace().expect("queue namespace");
        let provenance = |id: &str, added_at| {
            (
                id.to_owned(),
                crate::hash_state::HashProvenance {
                    namespace: namespace.clone(),
                    added_at,
                    fingerprint: hash_test_fingerprint(id, added_at),
                },
            )
        };
        manager
            .hashes
            .record_intents_with(
                &[
                    HashIntent {
                        id: "h-1".into(),
                        enabled: true,
                    },
                    HashIntent {
                        id: "h-2".into(),
                        enabled: true,
                    },
                ],
                &[provenance("h-1", 42), provenance("h-2", 43)],
            )
            .expect("record intents");
        manager.trigger_hashing();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let rows = manager.download_hashes();
            let done = ["h-1", "h-2"].iter().all(|id| {
                rows.iter()
                    .any(|row| row.id == *id && row.state == "complete")
            });
            if done {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the single worker must finish every pending candidate"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 10);
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hash_worker_only_reconciles_the_cohort_captured_before_list() {
        let root = fresh_dir("hash-cohort-worker");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let old_list = serde_json::json!([hash_test_item(
            "h-old",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (third_tx, third_rx) = mpsc::channel();
        let (release_third_tx, release_third_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut captured = Vec::new();
            let mut first = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut first));
            rate_test_write_response(&mut first, "200 OK", RATE_TEST_LIST);

            let mut second = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut second));
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            rate_test_write_response(&mut second, "200 OK", &old_list);

            let mut third = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut third));
            let _ = third_tx.send(());
            let _ = release_third_rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(third);
            captured
        });
        let base = format!("http://{addr}");

        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let namespace = manager.hashes.namespace().expect("queue namespace");
        let provenance = |id: &str, added_at| {
            (
                id.to_owned(),
                crate::hash_state::HashProvenance {
                    namespace: namespace.clone(),
                    added_at,
                    fingerprint: hash_test_fingerprint(id, added_at),
                },
            )
        };
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-old".into(),
                    enabled: true,
                }],
                &[provenance("h-old", 42)],
            )
            .expect("record existing intent");
        manager.trigger_hashing();

        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("worker started its /list request");
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-new".into(),
                    enabled: true,
                }],
                &[provenance("h-new", 43)],
            )
            .expect("record a newer intent while /list is in flight");
        release_tx
            .send(())
            .expect("release the older /list snapshot");

        third_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("worker finished reconciling and started the next request");
        let new_state = manager
            .download_hashes()
            .into_iter()
            .find(|row| row.id == "h-new")
            .map(|row| row.state);
        manager.hashes.cancel_current();
        release_third_tx.send(()).expect("release the worker");
        server.join().expect("server thread panicked");

        assert_eq!(
            new_state.as_deref(),
            Some("pending"),
            "an intent added after the /list snapshot must not be invalidated by that snapshot"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hash_worker_slot_is_single_and_restarts_with_a_fresh_token() {
        let coordinator = HashCoordinator::default();
        let first = coordinator.start_worker().expect("single worker starts");
        assert!(
            coordinator.start_worker().is_none(),
            "a concurrent trigger must coalesce instead of starting another worker"
        );
        coordinator.cancel_current();
        let second = coordinator
            .finish_worker(&first, true)
            .expect("dirty work must survive a cancelled worker");
        assert!(
            !second.load(Ordering::SeqCst),
            "the restarted worker must not inherit the cancelled token"
        );
        assert!(coordinator.start_worker().is_none());
        assert!(coordinator.take_dirty());
        assert!(coordinator.finish_worker(&second, true).is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_rejects_reused_id_with_different_provenance_before_hashing() {
        let list = serde_json::json!([hash_test_item(
            "h-1",
            "/no/such/file-must-not-be-opened.bin",
            "completed",
            None,
            false,
            false,
            99,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Stale,
            "a reused id with different provenance must never be hashed"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "no file or settings work after a mismatch"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_rechecks_hook_configuration_after_reading() {
        let root = fresh_dir("hash-hook-recheck");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let hook = serde_json::json!({
            "categories": {
                "category_enabled": true,
                "categories": [{
                    "name": "All",
                    "pattern": ".*",
                    "path": "/tmp",
                    "post_download_command": "true"
                }]
            }
        })
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list),
            ("200 OK", hook),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await
            .expect("hash one"),
            automatic_hash::HashOneOutcome::Pending,
            "a hook that became applicable must block publication"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 5);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn automatic_hash_fails_closed_when_the_post_hash_hook_config_is_malformed() {
        let root = fresh_dir("hash-hook-malformed");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list),
            ("200 OK", r#"{"categories":{}}"#.to_string()),
        ]);
        let (client, _epoch) =
            install_rate_test_managed(&ConnectionManager::default(), &base, 4242).await;
        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(
            automatic_hash::hash_one(
                &client,
                "h-1",
                42,
                &hash_test_fingerprint("h-1", 42),
                &cancel,
                || true
            )
            .await,
            Err(automatic_hash::HashWorkError::Daemon),
            "malformed hook settings must not publish a digest"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 5);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn add_download_with_hash_binds_fast_completion_to_the_acknowledged_provenance() {
        let root = fresh_dir("hash-add-fast");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let item = hash_test_item(
            "fast-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3,
        );
        let status_object = item.to_string();
        let list = serde_json::json!([item]).to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            (
                "200 OK",
                r#"{"status":"queued","id":"fast-1","filename":"abc.bin"}"#.to_string(),
            ),
            ("200 OK", status_object),
            ("200 OK", list.clone()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let receipt = manager
            .add_download_with_options("https://dl.example.test/abc.bin", true)
            .await
            .expect("add");
        assert_eq!(receipt.status, AddOutcome::Queued);
        assert_eq!(receipt.id.as_deref(), Some("fast-1"));
        assert_eq!(receipt.hash_recorded, Some(true));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let rows = manager.download_hashes();
            if let Some(row) = rows.iter().find(|row| row.id == "fast-1") {
                if row.state == "complete" {
                    assert_eq!(row.md5.as_deref(), Some("900150983cd24fb0d6963f7d28e17f72"));
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "fast completion was not bound to the acknowledged id"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 8);
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    fn lifecycle_marker(manager: &ConnectionManager) -> (u64, u64) {
        let epoch = manager.state.lock().unwrap().epoch;
        (epoch, manager.hashes.fence())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn disconnect_advances_epoch_and_fence_atomically() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let root = fresh_dir("lifecycle-atomic-disconnect");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let before = lifecycle_marker(&manager);
        let (entered_rx, release_tx) = manager.lifecycle_fence_pause.arm();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                manager.disconnect();
                let _ = done_tx.send(());
            })
        };
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("disconnect reached the lifecycle fence");
        assert_eq!(
            lifecycle_marker(&manager),
            before,
            "fence and epoch must not be published separately"
        );
        release_tx.send(()).expect("release disconnect");
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("disconnect completed");
        worker.join().expect("disconnect thread");
        let after = lifecycle_marker(&manager);
        assert_eq!(after.0, before.0.wrapping_add(1));
        assert_eq!(after.1, before.1.wrapping_add(1));
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn shutdown_advances_epoch_and_fence_atomically() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let root = fresh_dir("lifecycle-atomic-shutdown");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let before = lifecycle_marker(&manager);
        let (entered_rx, release_tx) = manager.lifecycle_fence_pause.arm();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let drained = manager.shutdown_managed();
                let _ = done_tx.send(drained);
            })
        };
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("shutdown reached the lifecycle fence");
        assert_eq!(
            lifecycle_marker(&manager),
            before,
            "shutdown must not publish the fence before the epoch"
        );
        release_tx.send(()).expect("release shutdown");
        let drained = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("shutdown completed");
        assert!(drained);
        worker.join().expect("shutdown thread");
        let after = lifecycle_marker(&manager);
        assert_eq!(after.0, before.0.wrapping_add(1));
        assert_eq!(after.1, before.1.wrapping_add(1));
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn managed_start_advances_epoch_and_fence_atomically() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let root = fresh_dir("lifecycle-atomic-managed");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let before = lifecycle_marker(&manager);
        let (entered_rx, release_tx) = manager.lifecycle_fence_pause.arm();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let claim = manager.begin_managed_start();
                let _ = done_tx.send(claim.is_ok());
            })
        };
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("managed start reached the lifecycle fence");
        assert_eq!(
            lifecycle_marker(&manager),
            before,
            "managed start must not publish the fence before the epoch"
        );
        release_tx.send(()).expect("release managed start");
        assert!(done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("managed start completed"));
        worker.join().expect("managed start thread");
        let after = lifecycle_marker(&manager);
        assert_eq!(after.0, before.0.wrapping_add(1));
        assert_eq!(after.1, before.1.wrapping_add(1));
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_start_and_respawn_wait_for_the_lifecycle_fence() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let root = fresh_dir("hash-worker-lifecycle-gap");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let first = manager.hashes.start_worker().expect("start worker");
        assert!(
            manager.hashes.start_worker().is_none(),
            "second trigger marks pending work"
        );

        let (entered_rx, release_tx) = manager.lifecycle_fence_pause.arm();
        let (disconnect_tx, disconnect_rx) = mpsc::channel();
        let disconnect = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                manager.disconnect();
                let _ = disconnect_tx.send(());
            })
        };
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("disconnect paused after cancelling but before fencing");

        // In the gap between cancellation and the fence, neither a fresh
        // trigger nor a finishing worker may install an uncancelled worker
        // against the old lifecycle.
        let start_hashes = Arc::clone(&manager.hashes);
        let (start_tx, start_rx) = mpsc::channel();
        let starter = std::thread::spawn(move || {
            let started = start_hashes.start_worker().is_some();
            let _ = start_tx.send(started);
        });
        assert!(
            start_rx.recv_timeout(Duration::from_millis(250)).is_err(),
            "start_worker must wait for the lifecycle fence"
        );

        let finish_hashes = Arc::clone(&manager.hashes);
        let (finish_tx, finish_rx) = mpsc::channel();
        let finisher = std::thread::spawn(move || {
            let respawned = finish_hashes.finish_worker(&first, true).is_some();
            let _ = finish_tx.send(respawned);
        });
        assert!(
            finish_rx.recv_timeout(Duration::from_millis(250)).is_err(),
            "finish_worker must wait for the lifecycle fence before respawning"
        );

        release_tx.send(()).expect("release disconnect");
        disconnect_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("disconnect completed");
        start_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("start_worker completed after the fence");
        assert!(
            finish_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("finish_worker completed after the fence"),
            "pending dirty work must respawn with a fresh uncancelled token"
        );
        disconnect.join().expect("disconnect thread");
        starter.join().expect("starter thread");
        finisher.join().expect("finisher thread");
        manager.hashes.cancel_worker();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rejected_managed_start_does_not_cancel_or_fence_live_work() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let root = fresh_dir("managed-start-reject");
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let token = manager.hashes.start_worker().expect("start worker");
        {
            let mut state = manager.state.lock().unwrap();
            state.starting = true;
            state.start_generation = 7;
        }
        let before = lifecycle_marker(&manager);
        let (_entered_rx, release_tx) = manager.lifecycle_fence_pause.arm();
        let (done_tx, done_rx) = mpsc::channel();
        let begin = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let already_started = matches!(
                    manager.begin_managed_start(),
                    Err(ManagedError::AlreadyStarted)
                );
                let _ = done_tx.send(already_started);
            })
        };
        let result = done_rx.recv_timeout(Duration::from_millis(300));
        if result.is_err() {
            // Old behavior: the rejected start entered the fence pause first.
            let _ = release_tx.send(());
        }
        let already_started = result
            .or_else(|_| done_rx.recv_timeout(Duration::from_secs(5)).map_err(|_| ()))
            .expect("rejected managed start must return without waiting on the fence");
        begin.join().expect("begin thread");
        assert!(already_started);
        assert!(
            !token.load(Ordering::SeqCst),
            "a rejected start must not cancel the live worker"
        );
        assert_eq!(
            lifecycle_marker(&manager),
            before,
            "a rejected start must not advance the epoch or fence"
        );
        assert!(
            manager.state.lock().unwrap().starting,
            "a rejected start must not clear the existing claim"
        );
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_connect_advances_epoch_and_fence_atomically() {
        use std::sync::Arc;
        use std::time::Duration;

        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let manager = Arc::new(ConnectionManager::default());
        let before = lifecycle_marker(&manager);
        let (entered_rx, release_tx) = manager.lifecycle_fence_pause.arm();
        let connect = tokio::spawn({
            let manager = Arc::clone(&manager);
            let base = base.clone();
            async move { manager.connect(&base, "test-token").await }
        });
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("manual connect reached the lifecycle fence");
        assert_eq!(
            lifecycle_marker(&manager),
            before,
            "manual connect must not publish the fence before the epoch"
        );
        release_tx.send(()).expect("release manual connect");
        connect
            .await
            .expect("connect task")
            .expect("manual connect succeeds");
        server.join().expect("server thread");
        let after = lifecycle_marker(&manager);
        assert_eq!(after.0, before.0.wrapping_add(1));
        assert_eq!(after.1, before.1.wrapping_add(1));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_hash_persistence_does_not_block_shutdown_and_supersedes_the_write() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let root = fresh_dir("hash-persist-shutdown");
        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        install_rate_test_managed(&manager, &base, 4242).await;
        let ticket = {
            let state = manager.state.lock().unwrap();
            manager
                .hash_write_ticket_locked(&state)
                .expect("managed hash ticket")
        };
        let (write_entered_rx, write_release_tx) = manager.hashes.hash_commit_pause.arm();
        let (write_done_tx, write_done_rx) = mpsc::channel();
        let write = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let result =
                    mutate_hash_at_ticket(&manager.state, &manager.hashes, &ticket, |slot| {
                        if !slot.loaded {
                            return Err(BridgeError::State);
                        }
                        slot.store
                            .record_intents(&[HashIntent {
                                id: "blocked-persist".to_owned(),
                                enabled: true,
                            }])
                            .map_err(|_| BridgeError::State)
                    });
                let _ = write_done_tx.send(matches!(result, Err(BridgeError::Superseded)));
            })
        };
        write_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("hash write staged and paused before publishing");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::channel();
        let shutdown = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let drained = manager.shutdown_managed();
                let _ = shutdown_done_tx.send(drained);
            })
        };
        let shutdown_result = shutdown_done_rx.recv_timeout(Duration::from_secs(2));

        write_release_tx
            .send(())
            .expect("release staged hash write");
        let superseded = write_done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("hash write completed");
        write.join().expect("hash write thread");
        shutdown.join().expect("shutdown thread");
        assert!(
            shutdown_result
                .expect("shutdown must not wait for a hash write blocked in durable persistence"),
            "shutdown must drain within its bound"
        );
        assert!(
            superseded,
            "a write staged before shutdown must not publish after the fence"
        );
        let captured = server.join().expect("server thread");
        assert_eq!(captured.len(), 1);
        assert!(
            manager
                .download_hashes()
                .iter()
                .all(|row| row.id != "blocked-persist"),
            "the superseded write must not install a record"
        );
        manager.hashes.hash_commit_pause.clear();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_flag_supersedes_a_ticket_write_before_the_fence() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let root = fresh_dir("hash-shutdown-flag");
        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        install_rate_test_managed(&manager, &base, 4242).await;
        let ticket = {
            let state = manager.state.lock().unwrap();
            manager
                .hash_write_ticket_locked(&state)
                .expect("managed hash ticket")
        };
        let (write_entered_rx, write_release_tx) = manager.hashes.hash_write_pause.arm();
        let (write_done_tx, write_done_rx) = mpsc::channel();
        let write = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || {
                let result =
                    mutate_hash_at_ticket(&manager.state, &manager.hashes, &ticket, |slot| {
                        if !slot.loaded {
                            return Err(BridgeError::State);
                        }
                        slot.store
                            .record_intents(&[HashIntent {
                                id: "shutdown-flag".to_owned(),
                                enabled: true,
                            }])
                            .map_err(|_| BridgeError::State)
                    });
                let _ = write_done_tx.send(matches!(result, Err(BridgeError::Superseded)));
            })
        };
        write_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("hash write paused before validation");

        // Simulate the earliest part of shutdown: the flag is set before the
        // epoch/fence transition can acquire the lifecycle gate.
        manager.hashes.begin_shutdown();
        write_release_tx.send(()).expect("release paused write");
        assert!(
            write_done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("hash write completed"),
            "shutdown begin must reject a ticket captured before the fence moved"
        );
        write.join().expect("hash write thread");
        assert!(
            manager
                .download_hashes()
                .iter()
                .all(|row| row.id != "shutdown-flag"),
            "a shutdown-flagged write must not install a record"
        );
        manager.hashes.hash_write_pause.clear();
        server.join().expect("server thread");
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn add_hash_intent_is_fenced_against_a_reconnect_during_the_write() {
        let root = fresh_dir("hash-add-fence");
        let item = hash_test_item(
            "fast-race",
            "/tmp/not-opened.bin",
            "downloading",
            None,
            false,
            false,
            42,
            3,
            1,
        );
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            (
                "200 OK",
                r#"{"status":"queued","id":"fast-race","filename":"abc.bin"}"#.to_string(),
            ),
            ("200 OK", item.to_string()),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;

        let (entered_rx, release_tx) = manager.hashes.hash_write_pause.arm();
        let add = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move {
                manager
                    .add_download_with_options("https://dl.example.test/abc.bin", true)
                    .await
            }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("add reached the fenced intent write");

        // Replace the connection after the old add pinned its epoch but before
        // it commits the intent. The old ticket must not write into the new
        // lifecycle's state.
        manager.install_test_managed(Arc::clone(&client), 4343);
        manager.lifecycle.begin_shutdown();
        release_tx.send(()).expect("release the fenced write");

        let receipt = add
            .await
            .expect("add task")
            .expect("add receipt remains a success");
        manager.hashes.hash_write_pause.clear();
        let captured = server.join().expect("server thread panicked");

        assert_eq!(captured.len(), 3);
        assert_eq!(receipt.id.as_deref(), Some("fast-race"));
        assert_eq!(
            receipt.hash_recorded,
            Some(false),
            "a superseded add must not claim a persisted intent"
        );
        assert!(
            manager
                .download_hashes()
                .iter()
                .all(|row| row.id != "fast-race"),
            "the old add must not install a record into the replacement lifecycle"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn published_intent_after_reconnect_is_not_surfaced_as_current() {
        let root = fresh_dir("hash-publish-reconnect");
        let item = hash_test_item(
            "late-race",
            "/tmp/not-opened.bin",
            "downloading",
            None,
            false,
            false,
            42,
            3,
            1,
        );
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            (
                "200 OK",
                r#"{"status":"queued","id":"late-race","filename":"abc.bin"}"#.to_string(),
            ),
            ("200 OK", item.to_string()),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(
                &root,
                Some("rename_in_progress"),
                None,
            )))
            .expect("arm rename barrier");

        let add = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move {
                manager
                    .add_download_with_options("https://dl.example.test/abc.bin", true)
                    .await
            }
        });
        assert!(
            wait_for_path(
                &root.join("entered-rename_in_progress"),
                Duration::from_secs(15)
            ),
            "the intent rename must be in progress"
        );
        // Reconnect while the already-authorized rename is physically in
        // flight. It may still publish, but it must not become current.
        manager.install_test_managed(Arc::clone(&client), 4343);
        std::fs::write(root.join("release-rename_in_progress"), b"go").expect("release");
        let receipt = add
            .await
            .expect("add task")
            .expect("add receipt remains a success");
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 3);
        assert_eq!(receipt.id.as_deref(), Some("late-race"));
        assert_eq!(
            receipt.hash_recorded,
            Some(false),
            "a rename published after reconnect is not a confirmed intent"
        );
        assert!(!manager.hashes.persistence_confirmed());
        assert!(
            manager
                .download_hashes()
                .iter()
                .all(|row| row.id != "late-race"),
            "the published old-lifecycle record must not surface as current"
        );
        drop(manager);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[derive(Clone, Copy)]
    enum HashVerification {
        FileRemoved,
        FileReplaced,
        QueueRemoved,
    }

    #[cfg(target_os = "linux")]
    async fn completed_hash_fixture(
        tag: &str,
        verification: HashVerification,
    ) -> (
        PathBuf,
        PathBuf,
        thread::JoinHandle<Vec<RateTestCaptured>>,
        ConnectionManager,
    ) {
        let root = fresh_dir(tag);
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let verification_body = match verification {
            HashVerification::QueueRemoved => "[]".to_string(),
            HashVerification::FileRemoved | HashVerification::FileReplaced => list.clone(),
        };
        let mut responses = vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", list),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
            ("200 OK", verification_body),
        ];
        if !matches!(verification, HashVerification::QueueRemoved) {
            responses.push(("200 OK", HASH_TEST_NO_HOOK.to_string()));
        }
        let (base, server) = rate_test_spawn_scripted(responses);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let namespace = manager.hashes.namespace().expect("queue namespace");
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-1".into(),
                    enabled: true,
                }],
                &[(
                    "h-1".into(),
                    crate::hash_state::HashProvenance {
                        namespace,
                        added_at: 42,
                        fingerprint: hash_test_fingerprint("h-1", 42),
                    },
                )],
            )
            .expect("record intent");
        manager.trigger_hashing();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while manager
            .download_hashes()
            .iter()
            .find(|row| row.id == "h-1")
            .is_none_or(|row| row.state != "complete")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fixture did not complete hashing"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        (root, file, server, manager)
    }

    #[cfg(target_os = "linux")]
    fn seed_completed_hash(
        manager: &ConnectionManager,
        root: &Path,
        id: &str,
        added_at: i64,
        destination: &Path,
    ) -> String {
        crate::hash_persistence::clear_persist_diagnostic();
        manager.hashes.ensure_loaded(root).expect("load hash state");
        let namespace = manager.hashes.namespace().expect("queue namespace");
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: id.into(),
                    enabled: true,
                }],
                &[(
                    id.into(),
                    crate::hash_state::HashProvenance {
                        namespace: namespace.clone(),
                        added_at,
                        fingerprint: hash_test_fingerprint(id, added_at),
                    },
                )],
            )
            .expect("record bound intent");
        let candidate = manager
            .hashes
            .candidates(&namespace)
            .into_iter()
            .find(|candidate| candidate.id == id)
            .expect("candidate");
        let cancel = Arc::new(AtomicBool::new(false));
        let size = std::fs::metadata(destination)
            .expect("fixture metadata")
            .len();
        let digests =
            crate::file_hash::hash_file(&destination.to_string_lossy(), Some(size), &cancel)
                .expect("hash fixture");
        assert!(
            manager
                .hashes
                .mark_complete(
                    id,
                    candidate.generation,
                    candidate.fence,
                    crate::hash_state::HashCompleted {
                        added_at,
                        md5: digests.md5,
                        sha256: digests.sha256,
                        size: digests.size,
                        mtime_sec: digests.mtime_sec,
                        mtime_nsec: digests.mtime_nsec,
                        ctime_sec: digests.ctime_sec,
                        ctime_nsec: digests.ctime_nsec,
                        file_dev: digests.file_dev,
                        file_ino: digests.file_ino,
                    },
                )
                .unwrap_or_else(|error| {
                    let diagnostic = crate::hash_persistence::last_persist_diagnostic()
                        .unwrap_or_else(|| "<none>".into());
                    panic!(
                        "complete fixture failed: {error:?}; last persistence diagnostic: {diagnostic}"
                    )
                }),
            "fixture completion must publish"
        );
        namespace
    }

    #[cfg(target_os = "linux")]
    fn hash_row<'a>(rows: &'a [DownloadHashDto], id: &str) -> &'a DownloadHashDto {
        rows.iter().find(|row| row.id == id).expect("row remains")
    }

    #[cfg(target_os = "linux")]
    fn assert_not_served_complete(row: &DownloadHashDto) {
        assert_ne!(row.state, "complete");
        assert!(row.md5.is_none());
        assert!(row.sha256.is_none());
        assert!(row.bytes.is_none());
        assert!(row.result_version.is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn verified_hashes_do_not_serve_completed_rows_still_finalizing_or_verifying() {
        for phase in ["finalizing", "verifying"] {
            let root = fresh_dir("hash-verify-phase");
            let file = root.join("abc.bin");
            std::fs::write(&file, b"abc").expect("write fixture");
            let destination = file.to_string_lossy().into_owned();
            let list = serde_json::json!([hash_test_item(
                "h-1",
                &destination,
                "completed",
                Some(phase),
                false,
                false,
                42,
                3,
                3
            )])
            .to_string();
            let (base, server) = rate_test_spawn_scripted(vec![
                ("200 OK", RATE_TEST_LIST.to_string()),
                ("200 OK", list),
            ]);
            let manager = ConnectionManager::default();
            manager
                .hashes
                .ensure_loaded(&root)
                .expect("load hash state");
            let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
            seed_completed_hash(&manager, &root, "h-1", 42, &file);

            let rows = manager
                .verified_download_hashes()
                .await
                .expect("verified phase rows");
            assert_not_served_complete(hash_row(&rows, "h-1"));
            let captured = server.join().expect("server thread panicked");
            assert_eq!(captured.len(), 2, "{phase} must not fetch hook settings");
            manager.hashes.cancel_current();
            remove_all_best_effort(&root);
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn verified_hashes_respect_current_hook_applicability() {
        let root = fresh_dir("hash-verify-hook");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let hook = serde_json::json!({
            "categories": {
                "category_enabled": true,
                "categories": [{
                    "name": "All",
                    "pattern": ".*",
                    "path": "/tmp",
                    "post_download_command": "true"
                }]
            }
        })
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
            ("200 OK", hook),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-1", 42, &file);

        let rows = manager
            .verified_download_hashes()
            .await
            .expect("verified hook rows");
        assert_not_served_complete(hash_row(&rows, "h-1"));
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 3);
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn verified_hashes_recheck_changed_hook_configuration() {
        let root = fresh_dir("hash-verify-hook-change");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            false,
            42,
            3,
            3
        )])
        .to_string();
        let hook = serde_json::json!({
            "categories": {
                "category_enabled": true,
                "categories": [{
                    "name": "All",
                    "pattern": ".*",
                    "path": "/tmp",
                    "post_download_command": "true"
                }]
            }
        })
        .to_string();

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", HASH_TEST_NO_HOOK.to_string()),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-1", 42, &file);
        let rows = manager
            .verified_download_hashes()
            .await
            .expect("verified hook-change rows");
        let first = hash_row(&rows, "h-1");
        assert_eq!(first.state, "complete");
        assert!(first.md5.is_some());
        server.join().expect("server thread panicked");

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
            ("200 OK", hook),
        ]);
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4343).await;
        let rows = manager
            .verified_download_hashes()
            .await
            .expect("verified hook rows");
        assert_not_served_complete(hash_row(&rows, "h-1"));
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 3);
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn verified_hashes_fail_closed_on_malformed_or_failed_hook_settings() {
        for (status, body) in [
            ("200 OK", r#"{"categories":{}}"#.to_string()),
            ("500 Internal Server Error", "{}".to_string()),
        ] {
            let root = fresh_dir("hash-verify-hook-failure");
            let file = root.join("abc.bin");
            std::fs::write(&file, b"abc").expect("write fixture");
            let destination = file.to_string_lossy().into_owned();
            let list = serde_json::json!([hash_test_item(
                "h-1",
                &destination,
                "completed",
                None,
                false,
                false,
                42,
                3,
                3
            )])
            .to_string();
            let (base, server) = rate_test_spawn_scripted(vec![
                ("200 OK", RATE_TEST_LIST.to_string()),
                ("200 OK", list),
                (status, body),
            ]);
            let manager = ConnectionManager::default();
            manager
                .hashes
                .ensure_loaded(&root)
                .expect("load hash state");
            let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
            seed_completed_hash(&manager, &root, "h-1", 42, &file);

            let result = manager.verified_download_hashes().await;
            assert_eq!(
                result.err(),
                Some(BridgeError::InvalidResponse),
                "unvalidated settings must not become an empty success"
            );
            let captured = server.join().expect("server thread panicked");
            assert_eq!(captured.len(), 3);
            manager.hashes.cancel_current();
            remove_all_best_effort(&root);
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_strip_a_record_completed_after_the_snapshot() {
        let root = fresh_dir("hash-verify-concurrent");
        let first_file = root.join("first.bin");
        let second_file = root.join("second.bin");
        std::fs::write(&first_file, b"abc").expect("write first fixture");
        std::fs::write(&second_file, b"xyz").expect("write second fixture");
        let first_destination = first_file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &first_destination,
            "completed",
            None,
            false,
            true,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let namespace = seed_completed_hash(&manager, &root, "h-1", 42, &first_file);
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-2".into(),
                    enabled: true,
                }],
                &[(
                    "h-2".into(),
                    crate::hash_state::HashProvenance {
                        namespace,
                        added_at: 43,
                        fingerprint: hash_test_fingerprint("h-2", 43),
                    },
                )],
            )
            .expect("record later intent");
        let later = manager
            .hashes
            .candidates(&manager.hashes.namespace().expect("namespace"))
            .into_iter()
            .find(|candidate| candidate.id == "h-2")
            .expect("later candidate");
        let (entered_rx, release_tx) = manager.verified_pause.arm();
        let serve = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.verified_download_hashes().await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("validation reached the publication barrier");

        let cancel = Arc::new(AtomicBool::new(false));
        let size = std::fs::metadata(&second_file)
            .expect("fixture metadata")
            .len();
        let digests =
            crate::file_hash::hash_file(&second_file.to_string_lossy(), Some(size), &cancel)
                .expect("hash later fixture");
        assert!(manager
            .hashes
            .mark_complete(
                "h-2",
                later.generation,
                later.fence,
                crate::hash_state::HashCompleted {
                    added_at: 43,
                    md5: digests.md5,
                    sha256: digests.sha256,
                    size: digests.size,
                    mtime_sec: digests.mtime_sec,
                    mtime_nsec: digests.mtime_nsec,
                    ctime_sec: digests.ctime_sec,
                    ctime_nsec: digests.ctime_nsec,
                    file_dev: digests.file_dev,
                    file_ino: digests.file_ino,
                },
            )
            .expect("complete later record"));
        release_tx.send(()).expect("release publication");
        let rows = serve.await.expect("serve task").expect("verified rows");
        manager.verified_pause.clear();
        server.join().expect("server thread panicked");

        let first = hash_row(&rows, "h-1");
        assert_eq!(first.state, "complete");
        assert!(first.md5.is_some());
        assert_not_served_complete(hash_row(&rows, "h-2"));
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_do_not_invalidate_completions_after_the_list_snapshot() {
        let root = fresh_dir("hash-verify-list-snapshot");
        let old_file = root.join("old.bin");
        let new_file = root.join("new.bin");
        std::fs::write(&old_file, b"abc").expect("write old fixture");
        std::fs::write(&new_file, b"xyz").expect("write new fixture");
        let old_destination = old_file.to_string_lossy().into_owned();
        // `processed: true` skips the hook settings request so the only gated
        // request is the authoritative `/list` read itself.
        let old_list = serde_json::json!([hash_test_item(
            "h-old",
            &old_destination,
            "completed",
            None,
            false,
            true,
            42,
            3,
            3
        )])
        .to_string();

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut captured = Vec::new();
            let mut first = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut first));
            rate_test_write_response(&mut first, "200 OK", RATE_TEST_LIST);

            // Hold the authoritative `/list` response while a newer completion
            // is persisted, so the response is genuinely older than that record.
            let mut second = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut second));
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            rate_test_write_response(&mut second, "200 OK", &old_list);
            captured
        });
        let base = format!("http://{addr}");

        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-old", 42, &old_file);

        let serve = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.verified_download_hashes().await }
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("validation started its /list request");

        // A new add can complete (and record its digest) while the older
        // `/list` response is still in flight. It must not be part of the
        // snapshot that this response is allowed to invalidate.
        seed_completed_hash(&manager, &root, "h-new", 43, &new_file);
        release_tx
            .send(())
            .expect("release the older /list snapshot");

        let rows = serve.await.expect("serve task").expect("verified rows");
        server.join().expect("server thread panicked");
        manager.hashes.cancel_current();

        let old = hash_row(&rows, "h-old");
        assert_eq!(old.state, "complete");
        assert!(old.md5.is_some());
        let new = hash_row(&rows, "h-new");
        assert_ne!(
            new.state, "stale",
            "an older /list must not invalidate a completion created after the snapshot"
        );
        assert_not_served_complete(new);
        let stored = manager
            .download_hashes()
            .into_iter()
            .find(|row| row.id == "h-new")
            .expect("new record remains");
        assert_eq!(
            stored.state, "complete",
            "the fresh digest must remain stored until its own verification pass"
        );
        assert!(stored.md5.is_some());
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_are_superseded_by_an_epoch_change_during_validation() {
        let root = fresh_dir("hash-verify-epoch");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            true,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-1", 42, &file);
        let (entered_rx, release_tx) = manager.verified_pause.arm();
        let serve = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.verified_download_hashes().await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("validation reached the publication barrier");

        manager.install_test_managed(Arc::clone(&client), 4343);
        release_tx.send(()).expect("release publication");
        let result = serve.await.expect("serve task");
        manager.verified_pause.clear();
        server.join().expect("server thread panicked");

        assert_eq!(
            result.err(),
            Some(BridgeError::Superseded),
            "an epoch change during validation must never fabricate an empty success"
        );
        let stored = manager.download_hashes();
        let row = hash_row(&stored, "h-1");
        assert_eq!(
            row.state, "complete",
            "the superseded read must not publish or clear the persisted digest"
        );
        assert!(row.md5.is_some());
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_fail_closed_when_the_managed_process_exits_after_the_list() {
        let root = fresh_dir("hash-verify-exit-after-list");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            true,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-1", 42, &file);
        let (entered_rx, release_tx) = manager.verified_list_pause.arm();
        let read = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get_download_hashes_fenced(|| Ok(())).await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("verification reached the post-list barrier");

        manager.mark_test_managed_exited();
        release_tx.send(()).expect("release the /list snapshot");
        let result = read.await.expect("read task");
        manager.verified_list_pause.clear();
        let captured = server.join().expect("server thread panicked");

        assert_eq!(
            result.err(),
            Some(BridgeError::NotConnected),
            "a process exit after /list must not fabricate an empty success"
        );
        assert_eq!(captured.len(), 2);
        let stored = manager.download_hashes();
        let row = hash_row(&stored, "h-1");
        assert_eq!(row.state, "complete");
        assert!(
            row.md5.is_some(),
            "the exited process must not clear the persisted digest"
        );
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_fail_closed_when_the_managed_process_exits_at_the_final_barrier() {
        let root = fresh_dir("hash-verify-exit-final");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            true,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-1", 42, &file);
        // Stop the refresh-triggered worker so only the command body reaches
        // the scripted daemon.
        manager.lifecycle.begin_shutdown();
        let (entered_rx, release_tx) = manager.verified_pause.arm();
        let read = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.refresh_download_hashes_fenced(|| Ok(())).await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("verification reached the publication barrier");

        manager.mark_test_managed_exited();
        release_tx.send(()).expect("release publication");
        let result = read.await.expect("read task");
        manager.verified_pause.clear();
        let captured = server.join().expect("server thread panicked");

        assert_eq!(
            result.err(),
            Some(BridgeError::NotConnected),
            "a process exit at the final barrier must not fabricate an empty success"
        );
        assert_eq!(captured.len(), 2);
        let stored = manager.download_hashes();
        let row = hash_row(&stored, "h-1");
        assert_eq!(row.state, "complete");
        assert!(
            row.md5.is_some(),
            "the final barrier must not clear the persisted digest"
        );
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_propagate_a_definite_invalidation_failure() {
        let (root, file, server, manager) = completed_hash_fixture(
            "hash-verify-invalidate-before-rename",
            HashVerification::FileRemoved,
        )
        .await;
        std::fs::remove_file(&file).expect("remove completed file");
        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(&root, None, Some("rename"))))
            .expect("arm definitive failure");

        let result = manager.get_download_hashes_fenced(|| Ok(())).await;
        assert_eq!(
            result.err(),
            Some(BridgeError::State),
            "a before-rename invalidation failure must surface its exact error"
        );

        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(None)
            .expect("clear definitive failure");
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 8);
        let stored = manager.download_hashes();
        let row = hash_row(&stored, "h-1");
        assert_eq!(
            row.state, "complete",
            "a definitive failure must not stale or serve the unverified digest"
        );
        assert!(row.md5.is_some());
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_preserve_an_ambiguous_invalidation_as_unconfirmed() {
        let (root, file, server, manager) = completed_hash_fixture(
            "hash-verify-invalidate-unconfirmed",
            HashVerification::FileRemoved,
        )
        .await;
        std::fs::remove_file(&file).expect("remove completed file");
        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(&root, None, Some("dir_sync"))))
            .expect("arm ambiguous failure");

        let result = manager.get_download_hashes_fenced(|| Ok(())).await;
        assert_eq!(
            result.err(),
            Some(BridgeError::HashUnconfirmed),
            "an ambiguous invalidation must stay HashUnconfirmed, not become State or empty"
        );
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 8);
        let stored = manager.download_hashes();
        let row = hash_row(&stored, "h-1");
        assert_eq!(
            row.state, "unavailable",
            "no stale digest may be served after an ambiguous invalidation"
        );
        assert!(row.md5.is_none());
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_supersede_an_invalidation_error_after_an_epoch_change() {
        let (root, file, server, manager) = completed_hash_fixture(
            "hash-verify-invalidate-epoch",
            HashVerification::FileRemoved,
        )
        .await;
        let manager = Arc::new(manager);
        std::fs::remove_file(&file).expect("remove completed file");
        manager
            .hashes
            .domain()
            .expect("owner")
            .set_fault(Some(persistence_fault(&root, Some("write"), None)))
            .expect("arm write barrier");

        let read = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get_download_hashes_fenced(|| Ok(())).await }
        });
        assert!(
            wait_for_path(
                &root.join("entered-write"),
                std::time::Duration::from_secs(15)
            ),
            "invalidation reached the persistence write barrier"
        );

        let client = {
            let state = manager.state.lock().expect("test state lock");
            Arc::clone(&state.test_managed.as_ref().expect("test managed").client)
        };
        manager.install_test_managed(client, 4343);
        std::fs::write(
            root.join(AUTOMATIC_HASH_FILE),
            br#"{"version":2,"defaultEnabled":true,"records":[]}"#,
        )
        .expect("inject foreign state");
        std::fs::write(root.join("release-write"), b"go").expect("release write barrier");

        let result = read.await.expect("read task");
        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            result.err(),
            Some(BridgeError::Superseded),
            "an old invalidation error must not surface after a lifecycle change"
        );
        assert_eq!(captured.len(), 8);
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_supersede_a_stale_invalidation_fence() {
        let (root, file, server, manager) = completed_hash_fixture(
            "hash-verify-invalidate-fence",
            HashVerification::FileRemoved,
        )
        .await;
        let manager = Arc::new(manager);
        std::fs::remove_file(&file).expect("remove completed file");
        let (entered_rx, release_tx) = manager.verified_list_pause.arm();
        let read = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get_download_hashes_fenced(|| Ok(())).await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("verification reached the post-list barrier");

        manager.hashes.cancel_current();
        release_tx.send(()).expect("release the /list snapshot");
        let result = read.await.expect("read task");
        manager.verified_list_pause.clear();
        let captured = server.join().expect("server thread panicked");

        assert_eq!(
            result.err(),
            Some(BridgeError::Superseded),
            "a moved hash fence is a lifecycle change, not an empty hash result"
        );
        assert_eq!(captured.len(), 8);
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_do_not_treat_an_already_invalidated_record_as_empty() {
        let (root, file, server, manager) = completed_hash_fixture(
            "hash-verify-invalidate-already",
            HashVerification::FileRemoved,
        )
        .await;
        let manager = Arc::new(manager);
        std::fs::remove_file(&file).expect("remove completed file");
        let (entered_rx, release_tx) = manager.verified_list_pause.arm();
        let read = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get_download_hashes_fenced(|| Ok(())).await }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("verification reached the post-list barrier");

        let namespace = manager.hashes.namespace().expect("queue namespace");
        let fence = manager.hashes.fence();
        let record = manager
            .hashes
            .completed_for(&namespace)
            .into_iter()
            .find(|record| record.id == "h-1")
            .expect("completed record");
        assert!(
            manager
                .hashes
                .invalidate_completed(&record, fence)
                .expect("concurrent invalidation"),
            "the exact record must invalidate under the current fence"
        );
        release_tx.send(()).expect("release the /list snapshot");
        let rows = read
            .await
            .expect("read task")
            .expect("a concurrent invalidation is not a false empty success");
        manager.verified_list_pause.clear();
        let captured = server.join().expect("server thread panicked");

        assert_eq!(captured.len(), 8);
        let row = hash_row(&rows, "h-1");
        assert_eq!(row.state, "stale");
        assert!(row.md5.is_none());
        assert!(row.sha256.is_none());
        manager.hashes.cancel_current();
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn record_hash_intents_never_returns_unverified_completed_digests() {
        let root = fresh_dir("hash-intents-verified");
        let file = root.join("abc.bin");
        std::fs::write(&file, b"abc").expect("write fixture");
        let destination = file.to_string_lossy().into_owned();
        let list = serde_json::json!([hash_test_item(
            "h-1",
            &destination,
            "completed",
            None,
            false,
            true,
            42,
            3,
            3
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", list),
        ]);
        let manager = ConnectionManager::default();
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        seed_completed_hash(&manager, &root, "h-1", 42, &file);
        std::fs::remove_file(&file).expect("remove completed file");
        manager.lifecycle.begin_shutdown();

        let rows = manager
            .record_hash_intents(&[])
            .await
            .expect("record and serve verified rows");
        assert_stale_after_verification(rows);
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 3);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn record_hash_intents_reconciles_only_the_transaction_cohort() {
        let root = fresh_dir("hash-intents-cohort");
        let list = serde_json::json!([hash_test_item(
            "h-1",
            "/tmp/not-opened.bin",
            "downloading",
            None,
            false,
            false,
            42,
            3,
            1
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list.clone()),
            ("200 OK", list),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        // Stop the background worker from fetching `/list` or hashing; this
        // test only exercises the synchronous intent transaction.
        manager.lifecycle.begin_shutdown();

        let (entered_rx, release_tx) = manager.record_intents_pause.arm();
        let rebind = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move {
                manager
                    .record_hash_intents(&[HashIntent {
                        id: "h-1".into(),
                        enabled: true,
                    }])
                    .await
            }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("record_hash_intents entered the cohort barrier");

        // Replace the same id with a newer generation while the older list
        // snapshot is still held. The replacement must not enter the cohort
        // this call is allowed to reconcile against that older response.
        let namespace = manager.hashes.namespace().expect("queue namespace");
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-1".into(),
                    enabled: true,
                }],
                &[(
                    "h-1".into(),
                    crate::hash_state::HashProvenance {
                        namespace: namespace.clone(),
                        added_at: 99,
                        fingerprint: crate::hash_state::queue_fingerprint(
                            "h-1",
                            99,
                            "https://dl.example.test/private-url",
                        ),
                    },
                )],
            )
            .expect("concurrent rebind");
        release_tx
            .send(())
            .expect("release the older /list snapshot");

        let rows = rebind
            .await
            .expect("record task")
            .expect("record and serve verified rows");
        manager.record_intents_pause.clear();
        server.join().expect("server thread panicked");

        let candidate = manager
            .hashes
            .candidates(&namespace)
            .into_iter()
            .find(|candidate| candidate.id == "h-1");
        let candidate = candidate.expect("the newer rebind must remain eligible for hashing");
        assert_eq!(
            candidate.added_at, 99,
            "the concurrently rebound provenance must survive the older list"
        );
        let row = rows
            .iter()
            .find(|row| row.id == "h-1")
            .expect("row remains");
        assert_ne!(
            row.state, "stale",
            "an intervening rebind must not be invalidated by the older list"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn record_hash_intents_are_fenced_against_a_reconnect_during_the_write() {
        let root = fresh_dir("hash-record-write-fence");
        let list = serde_json::json!([hash_test_item(
            "h-race",
            "/tmp/not-opened.bin",
            "downloading",
            None,
            false,
            false,
            42,
            3,
            1
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;

        let (entered_rx, release_tx) = manager.hashes.hash_write_pause.arm();
        let record = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move {
                manager
                    .record_hash_intents(&[HashIntent {
                        id: "h-race".into(),
                        enabled: true,
                    }])
                    .await
            }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("record reached the fenced intent write");

        manager.install_test_managed(Arc::clone(&client), 4343);
        release_tx.send(()).expect("release the fenced write");

        let result = record.await.expect("record task");
        manager.hashes.hash_write_pause.clear();
        let captured = server.join().expect("server thread panicked");

        assert_eq!(captured.len(), 2);
        assert!(
            matches!(result, Err(BridgeError::Superseded)),
            "an older record snapshot must not commit after a reconnect"
        );
        assert!(
            manager
                .download_hashes()
                .iter()
                .all(|row| row.id != "h-race"),
            "the old record must not install a record into the replacement lifecycle"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn record_hash_intents_reconcile_is_fenced_against_a_reconnect() {
        let root = fresh_dir("hash-record-reconcile-fence");
        let list = serde_json::json!([hash_test_item(
            "h-new",
            "/tmp/not-opened.bin",
            "downloading",
            None,
            false,
            false,
            99,
            3,
            1
        )])
        .to_string();
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", list),
            ("200 OK", "[]".to_string()),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let namespace = manager.hashes.namespace().expect("queue namespace");
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-old".into(),
                    enabled: true,
                }],
                &[(
                    "h-old".into(),
                    crate::hash_state::HashProvenance {
                        namespace,
                        added_at: 42,
                        fingerprint: hash_test_fingerprint("h-old", 42),
                    },
                )],
            )
            .expect("seed preexisting intent");

        let (entered_rx, release_tx) = manager.record_intents_pause.arm();
        let record = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move {
                manager
                    .record_hash_intents(&[HashIntent {
                        id: "h-new".into(),
                        enabled: true,
                    }])
                    .await
            }
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("record reached the reconcile barrier");

        manager.install_test_managed(Arc::clone(&client), 4343);
        manager.lifecycle.begin_shutdown();
        release_tx
            .send(())
            .expect("release the older list snapshot");

        record
            .await
            .expect("record task")
            .expect("record and serve verified rows");
        manager.record_intents_pause.clear();
        server.join().expect("server thread panicked");

        let old = manager
            .download_hashes()
            .into_iter()
            .find(|row| row.id == "h-old")
            .expect("preexisting row remains");
        assert_ne!(
            old.state, "stale",
            "an older /list must not stale a record across a reconnect"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hash_worker_reconcile_is_fenced_against_a_reconnect() {
        let root = fresh_dir("hash-worker-reconcile-fence");
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", "[]".to_string()),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        manager
            .hashes
            .ensure_loaded(&root)
            .expect("load hash state");
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let namespace = manager.hashes.namespace().expect("queue namespace");
        manager
            .hashes
            .record_intents_with(
                &[HashIntent {
                    id: "h-old".into(),
                    enabled: true,
                }],
                &[(
                    "h-old".into(),
                    crate::hash_state::HashProvenance {
                        namespace: namespace.clone(),
                        added_at: 42,
                        fingerprint: hash_test_fingerprint("h-old", 42),
                    },
                )],
            )
            .expect("seed preexisting intent");

        let (entered_rx, release_tx) = manager.hashes.hash_write_pause.arm();
        manager.trigger_hashing();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("worker reached the fenced reconcile");

        manager.install_test_managed(Arc::clone(&client), 4343);
        manager.lifecycle.begin_shutdown();
        release_tx
            .send(())
            .expect("release the older list snapshot");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while manager.hashes.lock_worker().running {
            assert!(
                std::time::Instant::now() < deadline,
                "superseded worker did not exit"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        manager.hashes.hash_write_pause.clear();
        server.join().expect("server thread panicked");

        let old = manager
            .download_hashes()
            .into_iter()
            .find(|row| row.id == "h-old")
            .expect("preexisting row remains");
        assert_ne!(
            old.state, "stale",
            "an older worker /list must not stale a record across a reconnect"
        );
        assert!(
            manager
                .hashes
                .candidates(&namespace)
                .iter()
                .any(|candidate| candidate.id == "h-old" && candidate.added_at == 42),
            "the superseded reconcile must leave the record eligible for the new lifecycle"
        );
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    fn assert_stale_after_verification(rows: Vec<DownloadHashDto>) {
        let row = rows
            .iter()
            .find(|row| row.id == "h-1")
            .expect("row remains");
        assert_eq!(row.state, "stale");
        assert!(row.md5.is_none());
        assert!(row.sha256.is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_invalidate_a_removed_file() {
        let (root, file, server, manager) =
            completed_hash_fixture("hash-verify-removed", HashVerification::FileRemoved).await;
        std::fs::remove_file(&file).expect("remove completed file");
        let rows = manager
            .verified_download_hashes()
            .await
            .expect("verified rows");
        assert_stale_after_verification(rows);
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 8);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_hashes_invalidate_a_replaced_file_and_removed_queue_record() {
        let (root, file, server, manager) =
            completed_hash_fixture("hash-verify-replaced", HashVerification::FileReplaced).await;
        std::fs::write(&file, b"xyz").expect("replace file contents");
        let rows = manager
            .verified_download_hashes()
            .await
            .expect("verified rows");
        assert_stale_after_verification(rows);
        server.join().expect("server thread panicked");
        remove_all_best_effort(&root);

        let (root, _file, server, manager) =
            completed_hash_fixture("hash-verify-queue", HashVerification::QueueRemoved).await;
        let rows = manager
            .verified_download_hashes()
            .await
            .expect("verified rows");
        assert_stale_after_verification(rows);
        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 7);
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn add_receipt_preserves_queued_id_and_withholds_approval_id() {
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            (
                "200 OK",
                r#"{"status":"queued","id":"download-id-1","filename":"new.bin"}"#.to_string(),
            ),
        ]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let receipt = client
            .add_download_receipt("https://dl.example.test/new.bin")
            .await
            .expect("accepted add");
        assert_eq!(receipt.outcome, AddOutcome::Queued);
        assert_eq!(receipt.id.as_deref(), Some("download-id-1"));
        server.join().expect("server thread panicked");

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            (
                "202 Accepted",
                r#"{"status":"pending_approval","id":"approval-request-1"}"#.to_string(),
            ),
        ]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let receipt = client
            .add_download_receipt("https://dl.example.test/pending.bin")
            .await
            .expect("pending add");
        assert_eq!(receipt.outcome, AddOutcome::PendingApproval);
        assert_eq!(receipt.id, None, "approval ids must never bind an intent");
        server.join().expect("server thread panicked");
    }

    #[cfg(target_os = "linux")]
    fn open_location_test_body(
        id: &str,
        destination: Option<&str>,
        status: &str,
        moving: bool,
        file_missing: bool,
    ) -> String {
        serde_json::json!([open_location_test_item(
            id,
            destination,
            status,
            moving,
            file_missing
        )])
        .to_string()
    }

    #[cfg(target_os = "linux")]
    async fn assert_open_location_error(body: String, id: &str, expected: BridgeError) {
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", body),
        ]);
        let manager = ConnectionManager::default();
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let launcher = Arc::new(RecordingLocationLauncher::new());
        let result = manager
            .open_download_location_with(id, Arc::clone(&launcher))
            .await;
        let _captured = server.join().expect("server thread panicked");
        assert_eq!(result, Err(expected));
        assert!(
            launcher.calls().is_empty(),
            "rejected locations must not launch"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_location_rejects_invalid_ids_without_network() {
        let manager = ConnectionManager::default();
        let launcher = Arc::new(RecordingLocationLauncher::new());
        let long_id = "x".repeat(MAX_DOWNLOAD_ID_BYTES + 1);
        for id in ["", "\0", long_id.as_str()] {
            assert_eq!(
                manager
                    .open_download_location_with(id, Arc::clone(&launcher))
                    .await,
                Err(BridgeError::InvalidDownloadId)
            );
        }
        assert!(launcher.calls().is_empty());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_location_rejects_manual_connections() {
        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let manager = ConnectionManager::default();
        manager
            .connect(&base, "test-token")
            .await
            .expect("manual connection");
        let launcher = Arc::new(RecordingLocationLauncher::new());
        assert_eq!(
            manager
                .open_download_location_with("d-1", Arc::clone(&launcher))
                .await,
            Err(BridgeError::ManagedConnectionRequired)
        );
        assert!(launcher.calls().is_empty());
        let _captured = server.join().expect("server thread panicked");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_location_launches_valid_completed_download_folder() {
        let root = fresh_dir("open-location-valid");
        let parent = root.join("dir ; $(echo pwn) `id` & 'quote'");
        std::fs::create_dir_all(&parent).expect("create parent fixture");
        let file = parent.join("-file \"double\" 'single' ; $HOME `touch pwned` & !.bin");
        std::fs::write(&file, b"completed").expect("write file fixture");
        let destination = file.to_string_lossy().into_owned();
        let body = open_location_test_body("d-1", Some(&destination), "completed", false, false);

        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", body),
        ]);
        let manager = ConnectionManager::default();
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let launcher = Arc::new(RecordingLocationLauncher::new());
        assert_eq!(
            manager
                .open_download_location_with("d-1", Arc::clone(&launcher))
                .await,
            Ok(())
        );
        assert_eq!(launcher.calls(), vec![parent.clone()]);
        assert!(
            !parent.join("pwned").exists(),
            "metacharacters must stay literal"
        );
        let _captured = server.join().expect("server thread panicked");
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_location_rejects_non_unique_and_incomplete_items() {
        let duplicate = serde_json::json!([
            open_location_test_item("d-1", Some("/no/such/file.bin"), "completed", false, false),
            open_location_test_item("d-1", Some("/no/such/file.bin"), "completed", false, false),
        ])
        .to_string();
        assert_open_location_error(duplicate, "d-1", BridgeError::DownloadAmbiguous).await;
        assert_open_location_error(
            open_location_test_body(
                "other",
                Some("/no/such/file.bin"),
                "completed",
                false,
                false,
            ),
            "d-1",
            BridgeError::DownloadNotFound,
        )
        .await;
        assert_open_location_error(
            open_location_test_body(
                "d-1",
                Some("/no/such/file.bin"),
                "downloading",
                false,
                false,
            ),
            "d-1",
            BridgeError::DownloadNotCompleted,
        )
        .await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_location_rejects_moving_missing_and_invalid_destinations() {
        assert_open_location_error(
            open_location_test_body("d-1", Some("/no/such/file.bin"), "completed", true, false),
            "d-1",
            BridgeError::DownloadMoving,
        )
        .await;
        assert_open_location_error(
            open_location_test_body("d-1", Some("/no/such/file.bin"), "completed", false, true),
            "d-1",
            BridgeError::DownloadFileMissing,
        )
        .await;
        assert_open_location_error(
            open_location_test_body("d-1", Some("relative/file.bin"), "completed", false, false),
            "d-1",
            BridgeError::LocationUnavailable,
        )
        .await;
        assert_open_location_error(
            open_location_test_body("d-1", None, "completed", false, false),
            "d-1",
            BridgeError::LocationUnavailable,
        )
        .await;

        let root = fresh_dir("open-location-vanished");
        let missing = root.join("vanished.bin");
        let destination = missing.to_string_lossy().into_owned();
        assert_open_location_error(
            open_location_test_body("d-1", Some(&destination), "completed", false, false),
            "d-1",
            BridgeError::DownloadFileMissing,
        )
        .await;
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_location_reports_sanitized_launch_failure() {
        let root = fresh_dir("open-location-launch-failure");
        let file = root.join("completed.bin");
        std::fs::write(&file, b"completed").expect("write file fixture");
        let destination = file.to_string_lossy().into_owned();
        let body = open_location_test_body("d-1", Some(&destination), "completed", false, false);
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", body),
        ]);
        let manager = ConnectionManager::default();
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let launcher = Arc::new(RecordingLocationLauncher::failing());
        assert_eq!(
            manager
                .open_download_location_with("d-1", Arc::clone(&launcher))
                .await,
            Err(BridgeError::LocationLaunchFailed)
        );
        assert_eq!(launcher.calls(), vec![root.clone()]);
        let _captured = server.join().expect("server thread panicked");
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    fn open_location_test_spawn_gated_refresh(
        body: String,
    ) -> (
        String,
        thread::JoinHandle<Vec<RateTestCaptured>>,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
        let addr = listener.local_addr().expect("local addr");
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut captured = Vec::with_capacity(2);
            let mut first = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut first));
            rate_test_write_response(&mut first, "200 OK", RATE_TEST_LIST);

            let mut second = rate_test_accept(&listener);
            captured.push(rate_test_capture(&mut second));
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
            rate_test_write_response(&mut second, "200 OK", &body);
            captured
        });
        (format!("http://{addr}"), server, started_rx, release_tx)
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_location_rejects_daemon_replacement_during_refresh() {
        let root = fresh_dir("open-location-stale");
        let file = root.join("completed.bin");
        std::fs::write(&file, b"completed").expect("write file fixture");
        let destination = file.to_string_lossy().into_owned();
        let body = open_location_test_body("d-1", Some(&destination), "completed", false, false);
        let (base, server, started_rx, release_tx) = open_location_test_spawn_gated_refresh(body);
        let manager = Arc::new(ConnectionManager::default());
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;
        let launcher = Arc::new(RecordingLocationLauncher::new());

        let manager_task = Arc::clone(&manager);
        let launcher_task = Arc::clone(&launcher);
        let task = tokio::spawn(async move {
            manager_task
                .open_download_location_with("d-1", Arc::clone(&launcher_task))
                .await
        });

        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("refresh must start");
        manager.install_test_managed(Arc::clone(&client), 9999);
        release_tx.send(()).expect("release refresh");

        assert_eq!(task.await.expect("open task"), Err(BridgeError::Superseded));
        assert!(
            launcher.calls().is_empty(),
            "a replaced daemon must never launch"
        );
        let _captured = server.join().expect("server thread panicked");
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn open_location_rejects_launch_after_shutdown_fence() {
        let (base, server) = rate_test_spawn_scripted(vec![("200 OK", RATE_TEST_LIST.to_string())]);
        let manager = ConnectionManager::default();
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;

        let operation = manager
            .lifecycle
            .begin_operation()
            .expect("reserve before shutdown");
        let snapshot = {
            let state = manager.state.lock().expect("state lock");
            state
                .managed_location_snapshot()
                .expect("managed location snapshot")
        };

        // Interleaving under test: the lifecycle fence is set, but shutdown has
        // not yet acquired the state lock to publish `state.shutting_down`.
        manager.lifecycle.begin_shutdown();
        assert!(!manager.state.lock().expect("state lock").shutting_down);

        let launcher = Arc::new(RecordingLocationLauncher::new());
        let result = manager
            .launch_location_if_current(
                operation,
                &snapshot,
                Arc::clone(&launcher),
                PathBuf::from("/tmp"),
            )
            .await;
        assert_eq!(result.err(), Some(BridgeError::Superseded));
        assert!(
            launcher.calls().is_empty(),
            "a launch after the shutdown fence must not start"
        );
        assert!(
            manager.lifecycle.begin_operation().is_none(),
            "no action may be reserved after the shutdown fence"
        );

        manager.shutdown_managed();
        let _captured = server.join().expect("server thread panicked");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_location_gated_launch_keeps_shutdown_bounded() {
        let root = fresh_dir("open-location-gated-stall");
        let file = root.join("completed.bin");
        std::fs::write(&file, b"completed").expect("write file fixture");
        let destination = file.to_string_lossy().into_owned();
        let body = open_location_test_body("d-1", Some(&destination), "completed", false, false);
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("200 OK", body),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 4242).await;

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let launcher = Arc::new(GatedLocationLauncher {
            calls: Mutex::new(Vec::new()),
            entered: Mutex::new(entered_tx),
            release: Mutex::new(release_rx),
        });
        let task_manager = Arc::clone(&manager);
        let task_launcher = Arc::clone(&launcher);
        let task = tokio::spawn(async move {
            task_manager
                .open_download_location_with("d-1", task_launcher)
                .await
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the launch worker must enter the gated spawn");

        let shutdown_manager = Arc::clone(&manager);
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let shutdown = thread::spawn(move || {
            let started = Instant::now();
            let drained = shutdown_manager.shutdown_managed();
            let _ = shutdown_tx.send((drained, started.elapsed()));
        });
        let (drained, elapsed) = shutdown_rx
            .recv_timeout(LIFECYCLE_DRAIN_TIMEOUT + std::time::Duration::from_secs(5))
            .expect("shutdown must return within its fixed drain bound");
        assert!(
            !drained,
            "a stalled accepted launch must not be reported as drained"
        );
        assert!(
            elapsed <= LIFECYCLE_DRAIN_TIMEOUT + std::time::Duration::from_secs(3),
            "shutdown must not wait indefinitely for the blocking launch"
        );
        shutdown.join().expect("shutdown thread");

        // The reservation was accepted before the fence, so the launch is
        // allowed to finish after shutdown began.
        release_tx.send(()).expect("release the gated launcher");
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("an accepted launch must finish after release")
            .expect("launch task");
        assert_eq!(result, Ok(()));
        assert_eq!(launcher.calls().len(), 1);
        assert!(
            manager.lifecycle.begin_operation().is_none(),
            "no action may be reserved after the shutdown fence"
        );

        let _captured = server.join().expect("server thread panicked");
        remove_all_best_effort(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn location_validation_permit_caps_blocking_work() {
        let manager = ConnectionManager::default();
        let gate = Arc::clone(&manager.location_validation);
        let held = gate.try_acquire().expect("hold the validation permit");

        let called = Arc::new(AtomicBool::new(false));
        let called_in_closure = Arc::clone(&called);
        let result = manager
            .validate_location_blocking(move || {
                called_in_closure.store(true, Ordering::SeqCst);
                Ok(PathBuf::from("/tmp"))
            })
            .await;
        assert_eq!(result.err(), Some(BridgeError::LocationUnavailable));
        assert!(
            !called.load(Ordering::SeqCst),
            "a busy permit must reject without running more validation work"
        );

        drop(held);
        assert_eq!(
            manager
                .validate_location_blocking(|| Ok(PathBuf::from("/tmp")))
                .await,
            Ok(PathBuf::from("/tmp"))
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn location_validation_permit_survives_cancelled_caller() {
        let manager = Arc::new(ConnectionManager::default());
        let gate = Arc::clone(&manager.location_validation);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let task_manager = Arc::clone(&manager);
        let task = tokio::spawn(async move {
            task_manager
                .validate_location_blocking(move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
                    Ok(PathBuf::from("/tmp"))
                })
                .await
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("validation must start");
        assert!(
            gate.try_acquire().is_none(),
            "the running closure must hold the permit"
        );

        task.abort();
        let _ = task.await;
        assert!(
            gate.try_acquire().is_none(),
            "cancelling the caller must not release the validation permit"
        );

        release_tx.send(()).expect("release validation");
        let deadline = Instant::now() + std::time::Duration::from_secs(3);
        loop {
            if let Some(permit) = gate.try_acquire() {
                drop(permit);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the permit must be released after validation completes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ambiguous_settings_write_poisons_settings_but_not_rate_writer() {
        const RATE: i64 = 1_234_000;
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("500 Internal Server Error", "{}".to_string()),
            ("200 OK", rate_test_ack(RATE)),
            ("200 OK", rate_test_settings(RATE)),
        ]);
        let manager = Arc::new(ConnectionManager::default());
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 5151).await;

        assert_eq!(
            manager.set_max_concurrent_downloads(3, 5).await,
            Err(BridgeError::SettingsUnconfirmed),
            "an ambiguous settings write must fail closed"
        );
        assert!(
            manager.settings_writer.poisoned.load(Ordering::SeqCst),
            "an ambiguous settings write must poison settings writes"
        );
        assert!(
            !manager.rate_writer.poisoned.load(Ordering::SeqCst),
            "settings poisoning must not touch rate control"
        );

        assert_eq!(
            manager.set_global_rate_limit(RATE).await,
            Ok(RATE),
            "rate control must still work after a settings ambiguity"
        );
        assert_eq!(
            manager.set_max_concurrent_downloads(3, 5).await,
            Err(BridgeError::SettingsUnconfirmed),
            "the poisoned settings writer must refuse later writes"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            4,
            "the poisoned settings writer must not send another request"
        );
        assert!(
            captured[1].request_line.starts_with("PATCH /settings "),
            "settings write must use PATCH, got {}",
            captured[1].request_line
        );
        assert!(captured[2]
            .request_line
            .starts_with("POST /rate-limit/global"));
        assert!(captured[3].request_line.starts_with("GET /settings "));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_ambiguous_settings_write_stays_unconfirmed_after_epoch_change() {
        let (base, server, started, release) = rate_test_spawn_gated_post(
            "500 Internal Server Error",
            "{}".to_string(),
            String::new(),
        );
        let manager = Arc::new(ConnectionManager::default());
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 5152).await;

        let set_manager = Arc::clone(&manager);
        let set = tokio::spawn(async move { set_manager.set_max_concurrent_downloads(3, 5).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("PATCH must reach the gated server");

        manager.disconnect();
        manager.install_test_managed(Arc::clone(&client), 5152);
        let _ = release.send(());

        assert_eq!(
            set.await.expect("set task"),
            Err(BridgeError::SettingsUnconfirmed),
            "a stale ambiguous settings write poisons globally and must stay visible"
        );
        assert!(
            manager.settings_writer.poisoned.load(Ordering::SeqCst),
            "the stale ambiguous response must poison the settings writer"
        );
        assert_eq!(
            manager.set_max_concurrent_downloads(3, 5).await,
            Err(BridgeError::SettingsUnconfirmed),
            "poison must block any write to the replacement"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "poisoned settings writer must not send another request"
        );
        assert!(
            captured[1].request_line.starts_with("PATCH /settings "),
            "settings write must use PATCH, got {}",
            captured[1].request_line
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_value_settings_write_confirms_via_daemon_patch() {
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            (
                "200 OK",
                r#"{"network":{"max_concurrent_downloads":3}}"#.to_string(),
            ),
        ]);
        let manager = ConnectionManager::default();
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 5160).await;

        assert_eq!(manager.set_max_concurrent_downloads(3, 3).await, Ok(3));

        let captured = server.join().expect("server thread panicked");
        assert_eq!(
            captured.len(),
            2,
            "same-value writes must still reach the daemon"
        );
        assert!(
            captured[1].request_line.starts_with("PATCH /settings "),
            "same-value writes must not bypass the daemon conflict check"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_value_settings_write_rejects_external_change() {
        let (base, server) = rate_test_spawn_scripted(vec![
            ("200 OK", RATE_TEST_LIST.to_string()),
            ("409 Conflict", "{}".to_string()),
        ]);
        let manager = ConnectionManager::default();
        let (_client, _epoch) = install_rate_test_managed(&manager, &base, 5161).await;

        assert_eq!(
            manager.set_max_concurrent_downloads(3, 3).await,
            Err(BridgeError::SettingsConflict),
            "a same-value write must report an externally changed daemon setting"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 2);
        assert!(
            captured[1].request_line.starts_with("PATCH /settings "),
            "same-value writes must not bypass the daemon conflict check"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_same_value_settings_write_is_superseded() {
        let (base, server, started, release) =
            rate_test_spawn_gated_post("409 Conflict", "{}".to_string(), String::new());
        let manager = Arc::new(ConnectionManager::default());
        let (client, _epoch) = install_rate_test_managed(&manager, &base, 5162).await;

        let set_manager = Arc::clone(&manager);
        let set = tokio::spawn(async move { set_manager.set_max_concurrent_downloads(3, 3).await });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("same-value PATCH must reach the gated server");

        manager.disconnect();
        manager.install_test_managed(Arc::clone(&client), 5162);
        let _ = release.send(());

        assert_eq!(
            set.await.expect("set task"),
            Err(BridgeError::Superseded),
            "a stale same-value write must not report a local success"
        );
        assert!(
            !manager.settings_writer.poisoned.load(Ordering::SeqCst),
            "a definitive conflict on a stale connection must not poison the writer"
        );

        let captured = server.join().expect("server thread panicked");
        assert_eq!(captured.len(), 2);
        assert!(
            captured[1].request_line.starts_with("PATCH /settings "),
            "settings write must use PATCH, got {}",
            captured[1].request_line
        );
    }
}
