use std::ffi::OsStr;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tauri::path::BaseDirectory;
use tauri::Emitter as _;
use tauri::Manager as _;

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
/// Only the download id, display filename, normalized status, and numeric
/// byte/speed metrics leave the process. The Surge `url` (may carry signed
/// secrets), `dest_path` (private filesystem layout), and `error` (server
/// text) are never projected. Field names cross the IPC boundary as camelCase.
#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueItemDto {
    pub id: String,
    pub name: String,
    pub status: String,
    pub bytes_done: i64,
    pub bytes_total: i64,
    pub speed_bps: f64,
    pub added_at: i64,
}

impl fmt::Debug for QueueItemDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // id and name can be user-controlled and status is server-controlled,
        // so only the numeric metrics are safe to print.
        f.debug_struct("QueueItemDto")
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

#[cfg(test)]
#[derive(Clone)]
struct TestManagedConnection {
    client: Arc<SurgeClient>,
    pid: u32,
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
            return test.pid == snapshot.pid;
        }
        self.managed
            .as_ref()
            .is_some_and(|server| !server.has_exited() && server.pid() == snapshot.pid)
    }

    fn clear_active(&mut self) {
        self.manual = None;
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

    fn begin_managed_start(
        &mut self,
    ) -> Result<(ManagedStartTicket, Option<ManagedServer>), ManagedError> {
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
        // A replacement connection must never leave the previous stream task
        // reading (or emitting) in the background.
        self.progress.stop();
        // Hold the lifecycle operation only across taking and retiring the old
        // daemon. If app shutdown has already started, the shutdown path owns
        // any remaining server.
        let lifecycle = match self.lifecycle.begin_operation() {
            Some(guard) => guard,
            None => return Err(BridgeError::Superseded),
        };
        let (epoch, retired, retirement_registered) = {
            let mut state = self.state.lock().map_err(|_| BridgeError::State)?;
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
        self.progress.stop();
        let mut state = self.state.lock().map_err(|_| ManagedError::State)?;
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
        self.progress.stop();
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.shutting_down = true;
        state.starting = false;
        state.start_generation = state.start_generation.wrapping_add(1);
        state.epoch = state.epoch.wrapping_add(1);
        state.clear_active();
        let server = state.managed.take();
        drop(state);
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
        let mut state = self.state.lock().expect("test state lock");
        state.epoch = state.epoch.wrapping_add(1);
        state.manual = None;
        state.test_managed = Some(TestManagedConnection { client, pid });
        state.active = ActiveConnection::Managed;
        state.epoch
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
        // disconnect.
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
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.epoch = state.epoch.wrapping_add(1);
        state.clear_active();
        let server = state.managed.take();
        let registered = if server.is_some() {
            state.mark_retirement();
            true
        } else {
            false
        };
        drop(state);
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
        "downloading" => "active",
        "queued" => "queued",
        "paused" => "paused",
        "completed" => "completed",
        "error" => "failed",
        "pausing" => "pausing",
        _ => "unknown",
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
    start_managed_at(&state, &binary, &state_dir, &home_dir).await
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
    state: tauri::State<'_, ConnectionManager>,
) -> Result<Vec<QueueItemDto>, BridgeError> {
    state.list_downloads().await
}

/// Authoritative `/list` snapshot plus the stream cursor for merge fencing.
#[tauri::command]
pub async fn list_downloads_snapshot(
    state: tauri::State<'_, ConnectionManager>,
) -> Result<QueueSnapshotDto, BridgeError> {
    state.list_downloads_snapshot().await
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
