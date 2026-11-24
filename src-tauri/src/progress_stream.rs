//! Truthful near-real-time queue progress from the pinned Surge `/events` SSE
//! stream.
//!
//! The released Surge v0.12.4 daemon exposes an authenticated
//! `GET /events` endpoint returning `text/event-stream`. Progress frames arrive
//! roughly every 150 ms carrying a flat JSON object with a numeric `type`
//! (`1` = progress), `download_id`, and optional `downloaded`, `total` and
//! `speed`. Zero values are omitted by Go's `omitempty`, so an absent numeric
//! field means zero, not "keep the previous value". The stream has no replay,
//! no heartbeat and no full queue metadata; an authoritative `/list` snapshot
//! is still required for lifecycle/reconnect resynchronisation.
//!
//! This module only projects validated, sanitized values. Raw frames, headers,
//! credentials and transport errors never leave it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::surge_client::{ConnectError, SurgeClient};

/// Renderer event carrying one coalesced batch of validated progress updates.
pub const PROGRESS_EVENT: &str = "surge://progress";
/// Renderer event asking the frontend to reconcile against an authoritative
/// `/list` snapshot (lifecycle transition or daemon-requested resync).
pub const LIFECYCLE_EVENT: &str = "surge://lifecycle";
/// Renderer event reporting stream readiness/fallback. The existing periodic
/// polling remains active regardless of this state.
pub const STREAM_STATE_EVENT: &str = "surge://stream";

/// Largest line accepted inside an SSE frame.
const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
/// Largest accumulated `data:` payload for one event.
const MAX_SSE_FRAME_BYTES: usize = 256 * 1024;
/// Largest pending unparsed buffer.
const MAX_SSE_BUFFER_BYTES: usize = MAX_SSE_FRAME_BYTES + MAX_SSE_LINE_BYTES;
/// Largest event name accepted from the SSE `event:` field.
const MAX_SSE_EVENT_BYTES: usize = 1024;
/// Largest download id projected to the renderer.
const MAX_STREAM_ID_BYTES: usize = 4 * 1024;
/// Bounded number of distinct rows buffered between coalesced emissions.
const MAX_PROGRESS_IDS: usize = 4096;
/// How often coalesced progress/lifecycle notices are emitted.
const COALESCE_INTERVAL: Duration = Duration::from_millis(100);
/// First reconnect delay after a stream/transport failure.
const RECONNECT_MIN: Duration = Duration::from_millis(500);
/// Maximum reconnect backoff. Idle streams are never treated as failures.
const RECONNECT_MAX: Duration = Duration::from_secs(8);
/// Consecutive protocol/validation failures before falling back to polling.
const MAX_PROTOCOL_FAILURES: u32 = 5;
/// An established stream must produce a valid frame or stay open this long
/// before a reconnect resets the protocol-failure count and backoff.
const HEALTHY_STREAM_DURATION: Duration = Duration::from_secs(3);
/// Largest integer exactly representable as a JavaScript `Number`.
const JS_MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// One validated progress projection. Numeric fields are already clamped and
/// missing/omitted daemon fields are zero.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressUpdate {
    pub id: String,
    /// Receipt sequence assigned before coalescing. Snapshot cursors use the
    /// same clock so per-row merge order survives a batched flush.
    pub seq: u64,
    pub bytes_done: i64,
    pub bytes_total: i64,
    pub speed_bps: f64,
}

impl ProgressUpdate {
    fn sanitized(id: String, downloaded: i64, total: i64, speed: f64) -> Self {
        let bytes_total = clamp_metric(total);
        let mut bytes_done = clamp_metric(downloaded);
        if bytes_total > 0 && bytes_done > bytes_total {
            bytes_done = bytes_total;
        }
        let speed_bps = if speed.is_finite() {
            speed.clamp(0.0, JS_MAX_SAFE_INTEGER as f64)
        } else {
            0.0
        };
        Self {
            id,
            seq: 0,
            bytes_done,
            bytes_total,
            speed_bps,
        }
    }

    fn with_seq(mut self, seq: u64) -> Self {
        self.seq = seq;
        self
    }
}

/// Coalesced batch emitted to the renderer.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgressBatch {
    subscription: u64,
    epoch: u64,
    seq: u64,
    updates: Vec<ProgressUpdate>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LifecycleNotice {
    subscription: u64,
    epoch: u64,
    seq: u64,
    resync: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamStateNotice {
    subscription: u64,
    epoch: u64,
    seq: u64,
    state: &'static str,
}

fn clamp_metric(value: i64) -> i64 {
    value.clamp(0, JS_MAX_SAFE_INTEGER)
}

/// Parsed SSE frame. `event` is the optional SSE event name; `data` is the
/// joined (newline-separated) payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseFrame {
    pub event: String,
    pub data: Vec<u8>,
}

/// Bounded parser failure. No payload content is carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseError {
    Oversized,
    Malformed,
}

/// Incremental, bounded SSE parser.
///
/// Line endings (`CRLF`, `LF`, `CR`) and UTF-8 multi-byte sequences may be
/// split across arbitrary chunk boundaries. Parsing is byte-oriented so a
/// split code point is never decoded early; only the final `data:` payload is
/// JSON-decoded. Comments are ignored, `data:` lines are joined with `\n`, and
/// unknown fields are ignored per the SSE contract.
pub struct SseParser {
    buffer: Vec<u8>,
    event: Option<String>,
    data: Vec<u8>,
    has_data: bool,
}

impl Default for SseParser {
    fn default() -> Self {
        Self::new()
    }
}

impl SseParser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            event: None,
            data: Vec::new(),
            has_data: false,
        }
    }

    /// Feed one transport chunk. `emit` returns `false` to stop consuming the
    /// current chunk (used to fail closed on a projection error).
    pub fn push<F>(&mut self, chunk: &[u8], mut emit: F) -> Result<(), SseError>
    where
        F: FnMut(SseFrame) -> bool,
    {
        // Feed a large transport chunk in bounded slices so a batch of valid
        // frames is never rejected just because hyper delivered it in one
        // multi-hundred-kilobyte buffer.
        let mut offset = 0;
        while offset < chunk.len() {
            let room = MAX_SSE_BUFFER_BYTES.saturating_sub(self.buffer.len());
            if room == 0 {
                return Err(SseError::Oversized);
            }
            let take = std::cmp::min(room, chunk.len() - offset);
            self.buffer.extend_from_slice(&chunk[offset..offset + take]);
            offset += take;
            if !self.process_buffer(&mut emit)? {
                return Ok(());
            }
            if self.buffer.len() > MAX_SSE_LINE_BYTES {
                return Err(SseError::Oversized);
            }
        }
        Ok(())
    }

    fn process_buffer<F>(&mut self, emit: &mut F) -> Result<bool, SseError>
    where
        F: FnMut(SseFrame) -> bool,
    {
        loop {
            let Some((line_end, consumed)) = find_line_end(&self.buffer) else {
                break;
            };
            let line: Vec<u8> = self.buffer.drain(..line_end).collect();
            self.buffer.drain(..consumed - line_end);
            if !self.process_line(&line, emit)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Flush a trailing unterminated line at EOF and dispatch any pending
    /// frame. Upstream always sends a blank line, but a truncated close should
    /// not silently lose an already-complete bounded frame.
    pub fn finish<F>(&mut self, mut emit: F) -> Result<(), SseError>
    where
        F: FnMut(SseFrame) -> bool,
    {
        if !self.buffer.is_empty() {
            let mut line = std::mem::take(&mut self.buffer);
            if line.ends_with(b"\r") {
                line.pop();
            }
            if !self.process_line(&line, &mut emit)? {
                return Ok(());
            }
        }
        let _ = self.dispatch(&mut emit)?;
        Ok(())
    }

    fn process_line<F>(&mut self, line: &[u8], emit: &mut F) -> Result<bool, SseError>
    where
        F: FnMut(SseFrame) -> bool,
    {
        if line.len() > MAX_SSE_LINE_BYTES {
            return Err(SseError::Oversized);
        }
        if line.is_empty() {
            return self.dispatch(emit);
        }
        if line[0] == b':' {
            return Ok(true);
        }
        let (field, value) = match line.iter().position(|byte| *byte == b':') {
            Some(index) => (&line[..index], &line[index + 1..]),
            None => (line, &[][..]),
        };
        match field {
            b"data" => {
                let value = value.strip_prefix(b" ").unwrap_or(value);
                if self.has_data {
                    if self.data.len().saturating_add(1) > MAX_SSE_FRAME_BYTES {
                        return Err(SseError::Oversized);
                    }
                    self.data.push(b'\n');
                }
                if self.data.len().saturating_add(value.len()) > MAX_SSE_FRAME_BYTES {
                    return Err(SseError::Oversized);
                }
                self.data.extend_from_slice(value);
                self.has_data = true;
            }
            b"event" => {
                let value = value.strip_prefix(b" ").unwrap_or(value);
                if value.len() > MAX_SSE_EVENT_BYTES {
                    return Err(SseError::Oversized);
                }
                let text = std::str::from_utf8(value).map_err(|_| SseError::Malformed)?;
                self.event = Some(text.to_string());
            }
            b"id" | b"retry" => {
                if value.len() > MAX_SSE_LINE_BYTES {
                    return Err(SseError::Oversized);
                }
            }
            _ => {}
        }
        Ok(true)
    }

    fn dispatch<F>(&mut self, emit: &mut F) -> Result<bool, SseError>
    where
        F: FnMut(SseFrame) -> bool,
    {
        if !self.has_data {
            self.event = None;
            self.data.clear();
            return Ok(true);
        }
        let data = std::mem::take(&mut self.data);
        let event = self.event.take().unwrap_or_default();
        self.has_data = false;
        if data.is_empty() {
            return Ok(true);
        }
        Ok(emit(SseFrame { event, data }))
    }
}

fn find_line_end(buffer: &[u8]) -> Option<(usize, usize)> {
    for (index, byte) in buffer.iter().enumerate() {
        match byte {
            b'\n' => return Some((index, index + 1)),
            b'\r' => {
                if index + 1 < buffer.len() {
                    let consumed = if buffer[index + 1] == b'\n' {
                        index + 2
                    } else {
                        index + 1
                    };
                    return Some((index, consumed));
                }
                // A trailing CR may be the first half of a split CRLF; wait
                // for the next chunk (or EOF).
                return None;
            }
            _ => {}
        }
    }
    None
}

/// Flattened subset of the released `types.DownloadEvent` JSON. Only the fields
/// this controller trusts are modeled; unknown fields are ignored for forward
/// compatibility.
#[derive(Deserialize)]
struct EventEnvelope {
    #[serde(rename = "type")]
    kind: i64,
    #[serde(default)]
    download_id: Option<String>,
    #[serde(default)]
    downloaded: Option<i64>,
    #[serde(default)]
    total: Option<i64>,
    #[serde(default)]
    speed: Option<f64>,
    #[serde(default)]
    resync_required: Option<bool>,
}

/// Validated action derived from one frame.
#[derive(Debug, PartialEq)]
pub enum FrameAction {
    Progress(ProgressUpdate),
    Lifecycle,
    Resync,
    Ignore,
}

/// Project one bounded frame into a sanitized action.
///
/// Numeric `type` values match pinned `types.EventType`: `1` is progress,
/// `11` is system, and the remaining lifecycle values trigger reconciliation.
/// Unknown future types are ignored rather than treated as errors.
pub fn project_frame(frame: &SseFrame) -> Result<FrameAction, SseError> {
    let envelope: EventEnvelope =
        serde_json::from_slice(&frame.data).map_err(|_| SseError::Malformed)?;
    match envelope.kind {
        1 => {
            let id = envelope.download_id.ok_or(SseError::Malformed)?;
            if id.is_empty() || id.len() > MAX_STREAM_ID_BYTES || id.chars().any(char::is_control) {
                return Err(SseError::Malformed);
            }
            Ok(FrameAction::Progress(ProgressUpdate::sanitized(
                id,
                envelope.downloaded.unwrap_or(0),
                envelope.total.unwrap_or(0),
                envelope.speed.unwrap_or(0.0),
            )))
        }
        11 => {
            if envelope.resync_required.unwrap_or(false) {
                Ok(FrameAction::Resync)
            } else {
                Ok(FrameAction::Ignore)
            }
        }
        0 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 => Ok(FrameAction::Lifecycle),
        _ => Ok(FrameAction::Ignore),
    }
}

/// Bounded latest-value-per-id coalescer.
pub struct ProgressCoalescer {
    max: usize,
    updates: HashMap<String, ProgressUpdate>,
}

impl ProgressCoalescer {
    pub fn new(max: usize) -> Self {
        Self {
            max: max.max(1),
            updates: HashMap::new(),
        }
    }

    /// Keep only the latest sample per id. New ids beyond the bound are dropped
    /// (the periodic `/list` poll still reconciles them).
    pub fn insert(&mut self, update: ProgressUpdate) {
        if self.updates.len() < self.max || self.updates.contains_key(&update.id) {
            self.updates.insert(update.id.clone(), update);
        }
    }

    pub fn drain(&mut self) -> Vec<ProgressUpdate> {
        let mut updates: Vec<ProgressUpdate> =
            self.updates.drain().map(|(_, value)| value).collect();
        updates.sort_by(|left, right| left.id.cmp(&right.id));
        updates
    }

    pub fn len(&self) -> usize {
        self.updates.len()
    }
}

type EmitFn = dyn Fn(&'static str, serde_json::Value) + Send + Sync;
type CurrentFn = dyn Fn() -> bool + Send + Sync;

#[derive(Default)]
struct StreamState {
    active: bool,
    subscription: u64,
    epoch: u64,
    handle: Option<tokio::task::JoinHandle<()>>,
}

/// Owns the single active progress-stream task. Every start is identified by
/// the frontend subscription epoch and the Rust connection epoch; stale starts
/// are rejected and a replaced/stopped task is aborted so no read survives a
/// disconnect.
pub struct ProgressStream {
    state: Mutex<StreamState>,
    seq: AtomicU64,
}

impl Default for ProgressStream {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ProgressStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let active = match self.state.lock() {
            Ok(state) => state.active,
            Err(poisoned) => poisoned.into_inner().active,
        };
        f.debug_struct("ProgressStream")
            .field("active", &active)
            .finish()
    }
}

impl ProgressStream {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(StreamState::default()),
            seq: AtomicU64::new(0),
        }
    }

    /// Current monotonic emission sequence. Snapshot consumers use this to
    /// fence progress frames that predate the authoritative list response.
    pub fn seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst).wrapping_add(1)
    }

    pub fn is_current(&self, subscription: u64, epoch: u64) -> bool {
        match self.state.lock() {
            Ok(state) => state.active && state.subscription == subscription && state.epoch == epoch,
            Err(poisoned) => {
                let state = poisoned.into_inner();
                state.active && state.subscription == subscription && state.epoch == epoch
            }
        }
    }

    /// Start (or replace) the stream task for a newer subscription. A start
    /// whose subscription is not newer than the active one is ignored, which
    /// fences reordered IPC on reconnect.
    pub fn start<F>(
        self: &Arc<Self>,
        subscription: u64,
        epoch: u64,
        client: Arc<SurgeClient>,
        emit: Arc<EmitFn>,
        is_connection_current: F,
    ) where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            if state.active && state.subscription >= subscription {
                return;
            }
            if let Some(handle) = state.handle.take() {
                handle.abort();
            }
            state.active = true;
            state.subscription = subscription;
            state.epoch = epoch;
        }

        let stream = Arc::clone(self);
        let current = Arc::new(is_connection_current);
        let handle = tokio::spawn(async move {
            run_stream(stream, subscription, epoch, client, emit, current).await;
        });

        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.active && state.subscription == subscription && state.epoch == epoch {
            state.handle = Some(handle);
        } else {
            handle.abort();
        }
    }

    /// Stop and abort the stream task. Safe to call from disconnect, connection
    /// replacement, disposal and app shutdown.
    pub fn stop(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.active = false;
        if let Some(handle) = state.handle.take() {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamFailure {
    Protocol,
    Network,
    Stopped,
}

#[derive(Debug, Default)]
struct ConsumeStats {
    /// At least one valid progress/lifecycle/resync frame was received.
    saw_valid_frame: bool,
}

/// A closed stream counts as healthy only after staying open for the sustained
/// health window. A single valid frame followed by an immediate EOF is not
/// enough: otherwise a server that always delivers one frame then closes would
/// reset the backoff to the minimum forever. Such a session is neither healthy
/// nor a protocol failure, so retries keep their exponential backoff (bounded
/// at `RECONNECT_MAX`). Immediate EOF with no valid frame remains a protocol
/// failure so bare-header sessions eventually stop.
fn classify_stream_end(saw_valid_frame: bool, elapsed: Duration) -> (bool, bool) {
    let healthy = elapsed >= HEALTHY_STREAM_DURATION;
    let protocol = !healthy && !saw_valid_frame;
    (healthy, protocol)
}

/// Bounded reconnect policy. Only a sustained stream resets the failure count
/// and backoff. A stream that delivers a valid frame but immediately closes
/// therefore keeps backing off instead of retrying forever at the minimum
/// delay.
struct ReconnectState {
    backoff: Duration,
    protocol_failures: u32,
}

impl ReconnectState {
    fn new() -> Self {
        Self {
            backoff: RECONNECT_MIN,
            protocol_failures: 0,
        }
    }

    /// Record one failed/closed attempt. Returns the delay before the next
    /// attempt, or `None` when protocol failures reached the cutoff.
    fn after(&mut self, healthy: bool, protocol: bool) -> Option<Duration> {
        if healthy {
            self.protocol_failures = 0;
            self.backoff = RECONNECT_MIN;
        } else if protocol {
            self.protocol_failures = self.protocol_failures.saturating_add(1);
            if self.protocol_failures >= MAX_PROTOCOL_FAILURES {
                return None;
            }
        }
        let delay = self.backoff;
        self.backoff = std::cmp::min(self.backoff.saturating_mul(2), RECONNECT_MAX);
        Some(delay)
    }
}

async fn run_stream(
    stream: Arc<ProgressStream>,
    subscription: u64,
    epoch: u64,
    client: Arc<SurgeClient>,
    emit: Arc<EmitFn>,
    is_connection_current: Arc<CurrentFn>,
) {
    let mut reconnect = ReconnectState::new();
    let mut last_state: Option<&'static str> = None;

    loop {
        if !stream.is_current(subscription, epoch) || !is_connection_current() {
            return;
        }

        let mut healthy = false;
        let mut protocol = false;
        match client.open_event_stream().await {
            Ok(response) => {
                emit_state(
                    &stream,
                    &emit,
                    subscription,
                    epoch,
                    "ready",
                    &mut last_state,
                );
                let started = std::time::Instant::now();
                match consume_stream(
                    &stream,
                    &emit,
                    subscription,
                    epoch,
                    response,
                    &is_connection_current,
                )
                .await
                {
                    Ok(stats) => {
                        let (next_healthy, next_protocol) =
                            classify_stream_end(stats.saw_valid_frame, started.elapsed());
                        healthy = next_healthy;
                        protocol = next_protocol;
                    }
                    Err(StreamFailure::Protocol) => {
                        let (next_healthy, next_protocol) =
                            classify_stream_end(false, started.elapsed());
                        healthy = next_healthy;
                        protocol = next_protocol;
                    }
                    Err(StreamFailure::Network) => {
                        healthy = started.elapsed() >= HEALTHY_STREAM_DURATION;
                    }
                    Err(StreamFailure::Stopped) => return,
                }
                if !stream.is_current(subscription, epoch) || !is_connection_current() {
                    return;
                }
            }
            Err(ConnectError::Unauthorized) => {
                emit_state(
                    &stream,
                    &emit,
                    subscription,
                    epoch,
                    "unavailable",
                    &mut last_state,
                );
                return;
            }
            Err(ConnectError::InvalidResponse) => {
                protocol = true;
            }
            Err(_) => {}
        }

        emit_state(
            &stream,
            &emit,
            subscription,
            epoch,
            "unavailable",
            &mut last_state,
        );
        let Some(delay) = reconnect.after(healthy, protocol) else {
            return;
        };
        tokio::time::sleep(delay).await;
        if !stream.is_current(subscription, epoch) || !is_connection_current() {
            return;
        }
    }
}

async fn consume_stream(
    stream: &Arc<ProgressStream>,
    emit: &Arc<EmitFn>,
    subscription: u64,
    epoch: u64,
    mut response: reqwest::Response,
    is_connection_current: &Arc<CurrentFn>,
) -> Result<ConsumeStats, StreamFailure> {
    let mut parser = SseParser::new();
    let mut coalescer = ProgressCoalescer::new(MAX_PROGRESS_IDS);
    let mut flush = tokio::time::interval(COALESCE_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut lifecycle_pending = false;
    let mut resync_pending = false;
    let mut lifecycle_seq = 0u64;
    let mut resync_seq = 0u64;
    let mut stats = ConsumeStats::default();

    loop {
        tokio::select! {
            _ = flush.tick() => {
                if !stream.is_current(subscription, epoch) || !is_connection_current() {
                    return Err(StreamFailure::Stopped);
                }
                flush_pending(stream, emit, subscription, epoch, &mut coalescer, &mut lifecycle_pending, &mut resync_pending, lifecycle_seq, resync_seq)?;
            }
            chunk = response.chunk() => {
                let chunk = match chunk {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => {
                        let mut rejected = false;
                        parser.finish(|frame| {
                            match project_frame(&frame) {
                                Ok(FrameAction::Progress(update)) => {
                                    let seq = stream.next_seq();
                                    coalescer.insert(update.with_seq(seq));
                                    stats.saw_valid_frame = true;
                                }
                                Ok(FrameAction::Lifecycle) => {
                                    let seq = stream.next_seq();
                                    lifecycle_seq = lifecycle_seq.max(seq);
                                    lifecycle_pending = true;
                                    stats.saw_valid_frame = true;
                                }
                                Ok(FrameAction::Resync) => {
                                    let seq = stream.next_seq();
                                    resync_seq = resync_seq.max(seq);
                                    resync_pending = true;
                                    stats.saw_valid_frame = true;
                                }
                                Ok(FrameAction::Ignore) => {}
                                Err(_) => {
                                    rejected = true;
                                    return false;
                                }
                            }
                            true
                        }).map_err(|_| StreamFailure::Protocol)?;
                        if rejected {
                            return Err(StreamFailure::Protocol);
                        }
                        flush_pending(stream, emit, subscription, epoch, &mut coalescer, &mut lifecycle_pending, &mut resync_pending, lifecycle_seq, resync_seq)?;
                        return Ok(stats);
                    }
                    Err(_) => return Err(StreamFailure::Network),
                };
                let mut rejected = false;
                parser.push(&chunk, |frame| {
                    match project_frame(&frame) {
                        Ok(FrameAction::Progress(update)) => {
                            let seq = stream.next_seq();
                            coalescer.insert(update.with_seq(seq));
                            stats.saw_valid_frame = true;
                        }
                        Ok(FrameAction::Lifecycle) => {
                            let seq = stream.next_seq();
                            lifecycle_seq = lifecycle_seq.max(seq);
                            lifecycle_pending = true;
                            stats.saw_valid_frame = true;
                        }
                        Ok(FrameAction::Resync) => {
                            let seq = stream.next_seq();
                            resync_seq = resync_seq.max(seq);
                            resync_pending = true;
                            stats.saw_valid_frame = true;
                        }
                        Ok(FrameAction::Ignore) => {}
                        Err(_) => {
                            rejected = true;
                            return false;
                        }
                    }
                    true
                }).map_err(|_| StreamFailure::Protocol)?;
                if rejected {
                    return Err(StreamFailure::Protocol);
                }
            }
        }
    }
}

fn flush_pending(
    stream: &Arc<ProgressStream>,
    emit: &Arc<EmitFn>,
    subscription: u64,
    epoch: u64,
    coalescer: &mut ProgressCoalescer,
    lifecycle_pending: &mut bool,
    resync_pending: &mut bool,
    lifecycle_seq: u64,
    resync_seq: u64,
) -> Result<(), StreamFailure> {
    if !stream.is_current(subscription, epoch) {
        return Err(StreamFailure::Stopped);
    }
    let updates = coalescer.drain();
    // Emit the group whose last receipt sequence is older first so the other
    // group's newer sequence is not swallowed by the receiver's fence.
    let update_seq = updates.iter().map(|update| update.seq).max().unwrap_or(0);
    let notice_seq = if *lifecycle_pending || *resync_pending {
        resync_seq.max(lifecycle_seq)
    } else {
        0
    };
    let progress_first = notice_seq == 0 || update_seq <= notice_seq;
    if progress_first {
        emit_progress(emit, subscription, epoch, updates);
        emit_lifecycle(
            emit,
            subscription,
            epoch,
            lifecycle_pending,
            resync_pending,
            notice_seq,
        );
    } else {
        emit_lifecycle(
            emit,
            subscription,
            epoch,
            lifecycle_pending,
            resync_pending,
            notice_seq,
        );
        emit_progress(emit, subscription, epoch, updates);
    }
    Ok(())
}

fn emit_progress(emit: &Arc<EmitFn>, subscription: u64, epoch: u64, updates: Vec<ProgressUpdate>) {
    if updates.is_empty() {
        return;
    }
    let seq = updates.iter().map(|update| update.seq).max().unwrap_or(0);
    emit_payload(
        emit,
        PROGRESS_EVENT,
        ProgressBatch {
            subscription,
            epoch,
            seq,
            updates,
        },
    );
}

fn emit_lifecycle(
    emit: &Arc<EmitFn>,
    subscription: u64,
    epoch: u64,
    lifecycle_pending: &mut bool,
    resync_pending: &mut bool,
    seq: u64,
) {
    if !*lifecycle_pending && !*resync_pending {
        return;
    }
    let resync = *resync_pending;
    *lifecycle_pending = false;
    *resync_pending = false;
    emit_payload(
        emit,
        LIFECYCLE_EVENT,
        LifecycleNotice {
            subscription,
            epoch,
            seq,
            resync,
        },
    );
}

fn emit_state(
    stream: &Arc<ProgressStream>,
    emit: &Arc<EmitFn>,
    subscription: u64,
    epoch: u64,
    state: &'static str,
    last_state: &mut Option<&'static str>,
) {
    if *last_state == Some(state) {
        return;
    }
    *last_state = Some(state);
    let seq = stream.next_seq();
    emit_payload(
        emit,
        STREAM_STATE_EVENT,
        StreamStateNotice {
            subscription,
            epoch,
            seq,
            state,
        },
    );
}

fn emit_payload<T: Serialize>(emit: &Arc<EmitFn>, event: &'static str, payload: T) {
    if let Ok(value) = serde_json::to_value(payload) {
        emit(event, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(chunks: &[&[u8]]) -> Result<Vec<SseFrame>, SseError> {
        let mut parser = SseParser::new();
        let mut frames = Vec::new();
        for chunk in chunks {
            parser.push(chunk, |frame| {
                frames.push(frame);
                true
            })?;
        }
        parser.finish(|frame| {
            frames.push(frame);
            true
        })?;
        Ok(frames)
    }

    #[test]
    fn parser_handles_fragmented_utf8_and_crlf() {
        let payload = "{\"type\":1,\"download_id\":\"dl-é😀\",\"downloaded\":10,\"total\":20}";
        let frame = format!("event: progress\r\ndata: {}\r\n\r\n", payload);
        let bytes = frame.as_bytes();
        let mut chunks = Vec::new();
        for index in 0..bytes.len() {
            chunks.push(&bytes[index..index + 1]);
        }
        let frames = collect(&chunks).expect("fragmented frame");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event, "progress");
        assert_eq!(frames[0].data, payload.as_bytes());
    }

    #[test]
    fn parser_handles_multiline_data_comments_and_unknown_fields() {
        let frames = collect(&[
            b": keep-alive\r\n",
            b"id: 1\r\nretry: 1000\r\n",
            b"data: {\"type\":1,\r\n",
            b"data: \"download_id\":\"dl-1\",\r\n",
            b"data: \"total\":5}\r\n\r\n",
            b"unknown: ignored\r\n\r\n",
        ])
        .expect("frames");
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].data,
            b"{\"type\":1,\n\"download_id\":\"dl-1\",\n\"total\":5}"
        );
    }

    #[test]
    fn parser_accepts_large_chunk_of_bounded_frames() {
        let mut body = String::new();
        for index in 0..12_000 {
            body.push_str(&format!(
                "event: progress\ndata: {{\"type\":1,\"download_id\":\"dl-{index}\"}}\n\n"
            ));
        }
        assert!(body.len() > MAX_SSE_BUFFER_BYTES);
        let frames = collect(&[body.as_bytes()]).expect("large chunk");
        assert_eq!(frames.len(), 12_000);
        assert_eq!(frames[0].event, "progress");
    }

    #[test]
    fn parser_rejects_oversized_line_and_frame() {
        let too_long_line = vec![b'a'; MAX_SSE_LINE_BYTES + 1];
        let mut parser = SseParser::new();
        let result = parser.push(&too_long_line, |_| true);
        assert_eq!(result, Err(SseError::Oversized));

        let mut parser = SseParser::new();
        let huge = vec![b'a'; MAX_SSE_BUFFER_BYTES + 1];
        assert_eq!(parser.push(&huge, |_| true), Err(SseError::Oversized));

        let data = vec![b'x'; MAX_SSE_FRAME_BYTES + 1];
        let mut line = b"data: ".to_vec();
        line.extend_from_slice(&data);
        line.extend_from_slice(b"\n\n");
        assert!(line.len() > MAX_SSE_LINE_BYTES);
        let mut parser = SseParser::new();
        assert_eq!(parser.push(&line, |_| true), Err(SseError::Oversized));
    }

    fn frame(data: &str) -> SseFrame {
        SseFrame {
            event: "progress".to_string(),
            data: data.as_bytes().to_vec(),
        }
    }

    #[test]
    fn projection_treats_omitted_zero_fields_as_zero() {
        match project_frame(&frame(r#"{"type":1,"download_id":"dl-1"}"#)).unwrap() {
            FrameAction::Progress(update) => {
                assert_eq!(update.bytes_done, 0);
                assert_eq!(update.bytes_total, 0);
                assert_eq!(update.speed_bps, 0.0);
            }
            other => panic!("expected progress, got {other:?}"),
        }
        match project_frame(&frame(
            r#"{"type":1,"download_id":"dl-2","downloaded":500,"total":1000,"speed":0}"#,
        ))
        .unwrap()
        {
            FrameAction::Progress(update) => {
                assert_eq!(update.bytes_done, 500);
                assert_eq!(update.bytes_total, 1000);
                assert_eq!(update.speed_bps, 0.0);
            }
            other => panic!("expected progress, got {other:?}"),
        }
    }

    #[test]
    fn projection_clamps_negative_and_oversized_metrics() {
        match project_frame(&frame(
            r#"{"type":1,"download_id":"dl-1","downloaded":-5,"total":-7,"speed":-2}"#,
        ))
        .unwrap()
        {
            FrameAction::Progress(update) => {
                assert_eq!(update.bytes_done, 0);
                assert_eq!(update.bytes_total, 0);
                assert_eq!(update.speed_bps, 0.0);
            }
            other => panic!("expected progress, got {other:?}"),
        }
        let huge = format!(
            r#"{{"type":1,"download_id":"dl-1","downloaded":{},"total":{},"speed":1e300}}"#,
            i64::MAX,
            i64::MAX
        );
        match project_frame(&frame(&huge)).unwrap() {
            FrameAction::Progress(update) => {
                assert_eq!(update.bytes_done, JS_MAX_SAFE_INTEGER);
                assert_eq!(update.bytes_total, JS_MAX_SAFE_INTEGER);
                assert_eq!(update.speed_bps, JS_MAX_SAFE_INTEGER as f64);
            }
            other => panic!("expected progress, got {other:?}"),
        }
    }

    #[test]
    fn projection_rejects_malformed_and_classes_events() {
        assert_eq!(project_frame(&frame("{")), Err(SseError::Malformed));
        assert_eq!(
            project_frame(&frame(r#"{"download_id":"dl-1"}"#)),
            Err(SseError::Malformed)
        );
        assert_eq!(
            project_frame(&frame(r#"{"type":1}"#)),
            Err(SseError::Malformed)
        );
        assert_eq!(
            project_frame(&frame(r#"{"type":1,"download_id":""}"#)),
            Err(SseError::Malformed)
        );
        assert_eq!(
            project_frame(&frame(r#"{"type":2,"download_id":"dl-1"}"#)).unwrap(),
            FrameAction::Lifecycle
        );
        assert_eq!(
            project_frame(&frame(r#"{"type":11,"resync_required":true}"#)).unwrap(),
            FrameAction::Resync
        );
        assert_eq!(
            project_frame(&frame(r#"{"type":11,"message":"verifying"}"#)).unwrap(),
            FrameAction::Ignore
        );
        assert_eq!(
            project_frame(&frame(r#"{"type":99}"#)).unwrap(),
            FrameAction::Ignore
        );
    }

    #[test]
    fn coalescer_keeps_latest_per_id_and_bounds_new_ids() {
        let mut coalescer = ProgressCoalescer::new(2);
        coalescer.insert(ProgressUpdate::sanitized("a".into(), 1, 10, 1.0));
        coalescer.insert(ProgressUpdate::sanitized("a".into(), 2, 10, 2.0));
        coalescer.insert(ProgressUpdate::sanitized("b".into(), 3, 10, 3.0));
        coalescer.insert(ProgressUpdate::sanitized("c".into(), 4, 10, 4.0));
        let updates = coalescer.drain();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].id, "a");
        assert_eq!(updates[0].bytes_done, 2);
        assert_eq!(updates[1].id, "b");
        assert_eq!(coalescer.len(), 0);
    }

    #[test]
    fn stream_rejects_stale_subscription_and_stop_clears_current() {
        let stream = Arc::new(ProgressStream::new());
        {
            let mut state = stream.state.lock().expect("state");
            state.active = true;
            state.subscription = 4;
            state.epoch = 9;
        }
        assert!(stream.is_current(4, 9));
        assert!(!stream.is_current(3, 9));
        stream.stop();
        assert!(!stream.is_current(4, 9));
    }

    #[test]
    fn seq_is_monotonic() {
        let stream = ProgressStream::new();
        assert_eq!(stream.seq(), 0);
        assert_eq!(stream.next_seq(), 1);
        assert_eq!(stream.next_seq(), 2);
        assert_eq!(stream.seq(), 2);
    }

    #[test]
    fn coalescer_preserves_the_receipt_sequence_of_the_latest_sample() {
        let mut coalescer = ProgressCoalescer::new(4);
        coalescer.insert(ProgressUpdate::sanitized("a".into(), 1, 10, 1.0).with_seq(3));
        coalescer.insert(ProgressUpdate::sanitized("a".into(), 9, 10, 2.0).with_seq(8));
        coalescer.insert(ProgressUpdate::sanitized("b".into(), 2, 10, 2.0).with_seq(4));
        let updates = coalescer.drain();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].id, "a");
        assert_eq!(updates[0].seq, 8);
        assert_eq!(updates[0].bytes_done, 9);
        assert_eq!(updates[1].seq, 4);
    }

    #[test]
    fn immediate_eof_and_malformed_frames_are_not_healthy() {
        let (healthy, protocol) = classify_stream_end(false, Duration::from_millis(0));
        assert!(!healthy);
        assert!(
            protocol,
            "bare headers + immediate EOF must count as a protocol failure"
        );

        let (healthy, protocol) =
            classify_stream_end(false, HEALTHY_STREAM_DURATION + Duration::from_millis(1));
        assert!(healthy, "a sustained open body is healthy");
        assert!(!protocol);

        let (healthy, protocol) = classify_stream_end(true, Duration::from_millis(0));
        assert!(
            !healthy,
            "a single valid frame followed by immediate EOF is not sustained stability"
        );
        assert!(
            !protocol,
            "a valid frame is not a protocol failure; it still backs off"
        );

        let (healthy, protocol) =
            classify_stream_end(true, HEALTHY_STREAM_DURATION + Duration::from_millis(1));
        assert!(healthy, "a valid frame in a sustained stream is healthy");
        assert!(!protocol);
    }

    #[test]
    fn repeated_valid_frame_then_immediate_eof_keeps_exponential_backoff() {
        let mut state = ReconnectState::new();
        let (healthy, protocol) = classify_stream_end(true, Duration::from_millis(0));
        assert!(!healthy);
        assert!(!protocol);
        let mut delays = Vec::new();
        for _ in 0..8 {
            let delay = state
                .after(healthy, protocol)
                .expect("valid-frame sessions keep retrying with bounded backoff");
            delays.push(delay);
        }
        assert_eq!(delays[0], RECONNECT_MIN);
        for window in delays.windows(2) {
            assert!(
                window[1] >= window[0],
                "backoff never resets to the minimum after a valid frame + EOF"
            );
        }
        assert!(
            delays.iter().all(|delay| *delay <= RECONNECT_MAX),
            "retry stays bounded"
        );
        assert!(
            delays.last().copied() == Some(RECONNECT_MAX),
            "backoff reaches the bounded maximum instead of restarting at 500ms"
        );
    }

    #[test]
    fn repeated_malformed_frames_and_immediate_eof_back_off_then_stop() {
        // A malformed JSON frame is a protocol error ...
        assert!(project_frame(&frame("{")).is_err());
        // ... and an immediate EOF with no valid frame classifies the same way.
        let (_, immediate_protocol) = classify_stream_end(false, Duration::from_millis(0));
        assert!(immediate_protocol);

        let mut malformed = ReconnectState::new();
        let mut immediate = ReconnectState::new();
        let mut delays = Vec::new();
        for _ in 0..MAX_PROTOCOL_FAILURES - 1 {
            let malformed_delay = malformed.after(false, true).expect("still retrying");
            let immediate_delay = immediate
                .after(false, immediate_protocol)
                .expect("still retrying");
            assert_eq!(malformed_delay, immediate_delay);
            delays.push(malformed_delay);
        }
        assert_eq!(delays[0], RECONNECT_MIN);
        assert!(delays[1] > delays[0], "retry is exponential");
        assert!(
            delays.iter().all(|delay| *delay <= RECONNECT_MAX),
            "retry stays bounded"
        );
        assert_eq!(
            malformed.after(false, true),
            None,
            "malformed protocol failures are cut off after the bound"
        );
        assert_eq!(
            immediate.after(false, immediate_protocol),
            None,
            "repeated immediate EOF is cut off after the bound"
        );
    }

    #[test]
    fn healthy_stream_resets_backoff_and_protocol_failures() {
        let mut state = ReconnectState::new();
        assert!(state.after(false, true).is_some());
        assert!(state.after(false, true).is_some());
        let delay = state.after(true, false).expect("healthy retry");
        assert_eq!(delay, RECONNECT_MIN);
        assert_eq!(state.protocol_failures, 0);
    }
}
