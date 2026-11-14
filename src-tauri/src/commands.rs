use std::ffi::OsStr;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tauri::path::BaseDirectory;
use tauri::Manager as _;

use crate::managed_server::{ManagedServer, OwnerLifetime, DEFAULT_STARTUP_TIMEOUT};
use crate::surge_client::{AddOutcome, ConnectError, ControlAction, Download, SurgeClient};

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
const JS_MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

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
    NotConnected,
    Superseded,
    State,
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
            Self::NotConnected => "NotConnected",
            Self::Superseded => "Superseded",
            Self::State => "State",
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
            Self::NotConnected => "not connected to a Surge instance",
            Self::Superseded => "connection attempt was superseded",
            Self::State => "connection state unavailable",
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

#[derive(Default)]
struct ConnectionState {
    epoch: u64,
    start_generation: u64,
    starting: bool,
    shutting_down: bool,
    retiring: usize,
    manual: Option<Arc<SurgeClient>>,
    managed: Option<ManagedServer>,
    active: ActiveConnection,
}

impl ConnectionState {
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
        self.manual = None;
        self.active = ActiveConnection::None;
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
            state.manual = None;
            state.active = ActiveConnection::None;
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
        state.active = ActiveConnection::Managed;
        drop(state);
        Ok(cached)
    }

    /// Stop and reap the owned managed daemon at app shutdown.
    ///
    /// In-flight operations hold only an `Arc<SurgeClient>` clone, so taking
    /// the owned server out here is safe and never strands a lease. Detached
    /// starts and retirements are cancelled through the lifecycle flag and
    /// waited for within a fixed bound before this returns.
    pub fn shutdown_managed(&self) -> bool {
        // Fence new lifecycle work first, then invalidate the connection state.
        // This ordering means an operation that already claimed a slot either
        // sees `shutting_down` before finishing or gets cancelled below.
        self.lifecycle.begin_shutdown();
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.shutting_down = true;
        state.starting = false;
        state.start_generation = state.start_generation.wrapping_add(1);
        state.epoch = state.epoch.wrapping_add(1);
        state.manual = None;
        state.active = ActiveConnection::None;
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
        match state.active {
            ActiveConnection::None => Err(BridgeError::NotConnected),
            ActiveConnection::Manual => {
                let client = state.manual.clone().ok_or(BridgeError::NotConnected)?;
                Ok((client, state.epoch))
            }
            ActiveConnection::Managed => {
                let server = state.managed.as_ref().ok_or(BridgeError::NotConnected)?;
                if server.has_exited() {
                    return Err(BridgeError::NotConnected);
                }
                Ok((server.client(), state.epoch))
            }
        }
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

    /// Drop the active connection and fence off any in-flight `connect`.
    ///
    /// Any owned managed daemon is retired as well, outside the lock, with the
    /// bounded graceful shutdown. Keeping it alive but unreachable would leak a
    /// live child and block every later explicit restart. In-flight operations
    /// only hold an `Arc<SurgeClient>` clone, so they observe the new epoch (or
    /// a network error) without stranding state.
    pub fn disconnect(&self) {
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
        state.manual = None;
        state.active = ActiveConnection::None;
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
        Some(Err(_)) => return Err(ManagedError::Startup),
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

    fn claim(manager: &ConnectionManager) -> ManagedStartTicket {
        let (ticket, retired) = manager.begin_managed_start().expect("managed start claim");
        assert!(
            retired.is_none(),
            "a fresh manager must not retire a server"
        );
        ticket
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
    async fn startup_failure_releases_claim_without_spawning() {
        let root = fresh_dir("startup-failure");
        let home = root.join("home");
        std::fs::create_dir(&home).expect("create home");
        let binary = root.join("missing-managed-daemon");
        let state_dir = root.join("state");

        let manager = ConnectionManager::default();
        let error = start_managed_at(&manager, &binary, &state_dir, &home)
            .await
            .unwrap_err();
        assert_eq!(error, ManagedError::Startup);

        let retry = claim(&manager);
        manager.abort_managed_start(retry);

        let state = manager.state.lock().unwrap();
        assert!(!state.starting);
        assert!(state.managed.is_none());
        drop(state);

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
        assert_eq!(error, ManagedError::Startup);
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
}
