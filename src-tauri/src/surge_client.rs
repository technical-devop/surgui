use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use reqwest::{redirect::Policy, Certificate, Client, StatusCode};
use serde::{Deserialize, Serialize};
use url::{Host, Url};

/// Upper bound for an untrusted `/list` JSON body (1 MiB).
const MAX_BODY_BYTES: u64 = 1024 * 1024;

/// Upper bound for a user-supplied download URL (8 KiB).
///
/// The exact query string is forwarded verbatim, so a bounded cap keeps a
/// hostile or malformed URL from being relayed to the daemon.
const MAX_URL_BYTES: usize = 8 * 1024;

/// Upper bound for per-item ids returned by a bulk control route. The `/list`
/// body is already capped at `MAX_BODY_BYTES`, so this is a second, explicit
/// guard against pathological ids.
const MAX_BULK_ID_BYTES: usize = 1024;

/// Fixed, display-safe replacement for every daemon per-item error string.
///
/// The daemon's raw text can contain a bearer token, signed URL, or local
/// path, so it never crosses IPC or reaches a log. Only the validated item id
/// and the success/failure bit are preserved.
const BULK_ITEM_ERROR_MESSAGE: &str = "Daemon reported an error for this item.";

/// Upper bound for the number of per-item results accepted from a bulk route.
const MAX_BULK_RESULT_ITEMS: usize = 100_000;

/// Largest integer exactly representable as a JavaScript `Number`.
///
/// Rate values are kept within `[0, MAX_RATE_LIMIT_BPS]` so a global limit can
/// cross the Tauri/JS boundary without precision loss.
pub const MAX_RATE_LIMIT_BPS: i64 = 9_007_199_254_740_991;

/// Pinned `network.max_concurrent_downloads` contract from Surge v0.12.4.
pub const MAX_CONCURRENT_DOWNLOADS_MIN: i64 = 1;
pub const MAX_CONCURRENT_DOWNLOADS_MAX: i64 = 10;
pub const MAX_CONCURRENT_DOWNLOADS_DEFAULT: i64 = 3;

/// Exact duplicate-rejection message emitted by released Surge v0.12.4 when
/// headless mode refuses an already-queued download (`cmd/root_downloads.go`,
/// unchanged from v0.12.2). The 409 status alone does not prove a duplicate,
/// so the message is matched case-insensitively in addition to
/// `status == "error"`.
const DUPLICATE_MESSAGE: &str = "Download rejected: Duplicate download detected (Headless mode)";

/// Bounded round-trip for the authenticated `/list` probe.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounded wait for the `/events` TCP+TLS+response-header handshake. Once the
/// headers arrive the established body may stay idle indefinitely.
const EVENT_STREAM_HANDSHAKE_TIMEOUT: Duration = CONNECT_TIMEOUT;

/// Pinned managed-mode origin.
///
/// Managed mode always talks to the locally spawned Surge daemon over TLS at
/// this exact origin. It takes no endpoint argument and never falls back to
/// plain HTTP or any other host/port.
pub const MANAGED_BASE_URL: &str = "https://127.0.0.1:1700";

/// Upper bound for the per-launch PEM CA bundle (64 KiB).
///
/// Rejects empty/oversized input before parsing so a hostile caller cannot
/// force a large allocation; the bytes themselves are never logged.
const MAX_CA_PEM_BYTES: usize = 64 * 1024;

/// One entry of the Surge `/list` payload (released Surge v0.12.4
/// `internal/types/models.go` `DownloadStatus`).
///
/// `total_size`, `downloaded` and `eta` are Go `int64` (`eta == -1` means
/// unknown), so they are `i64` here. `connections` is Go `int` (widened to
/// `i64` for 32/64-bit portability), `added_at`/`time_taken` are Go `int64`,
/// `avg_speed` is Go `float64`. `dest_path`/`error` are `omitempty` in Go and
/// `rate_limit`/`rate_limit_set` are only present when a cap is set, so all
/// four are `Option`. v0.12.4 adds optional forward-compatible fields
/// (`phase`, `max_connections`, `artifact`, `error_code`, `moving`,
/// `file_missing`, `category_override`, timestamps); unknown fields are
/// ignored so a server-side schema addition stays forward compatible.
/// `moving` and `file_missing` are Go `omitempty` booleans, so absent means
/// false.
#[derive(Clone, Deserialize)]
pub struct Download {
    pub id: String,
    pub url: String,
    pub filename: String,
    pub total_size: i64,
    pub downloaded: i64,
    pub progress: f64,
    pub speed: f64,
    pub status: String,
    pub eta: i64,
    #[serde(default)]
    pub moving: bool,
    #[serde(default)]
    pub file_missing: bool,
    #[serde(default)]
    pub connections: i64,
    #[serde(default)]
    pub added_at: i64,
    #[serde(default)]
    pub time_taken: i64,
    #[serde(default)]
    pub avg_speed: f64,
    #[serde(default)]
    pub dest_path: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub rate_limit: Option<i64>,
    #[serde(default)]
    pub rate_limit_set: Option<bool>,
}

impl fmt::Debug for Download {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never emit id, url, filename, status, dest_path, error, or
        // response content (id can be user-provided and status is
        // server-controlled; the rest may carry signed secrets or
        // private paths). Numeric/bool metrics only.
        f.debug_struct("Download")
            .field("total_size", &self.total_size)
            .field("downloaded", &self.downloaded)
            .field("progress", &self.progress)
            .field("speed", &self.speed)
            .field("eta", &self.eta)
            .field("moving", &self.moving)
            .field("file_missing", &self.file_missing)
            .field("connections", &self.connections)
            .field("added_at", &self.added_at)
            .field("time_taken", &self.time_taken)
            .field("avg_speed", &self.avg_speed)
            .field("rate_limit", &self.rate_limit)
            .field("rate_limit_set", &self.rate_limit_set)
            .finish()
    }
}

/// Authenticated handle to a Surge instance.
///
/// Holds the bearer token in memory only (never persisted, never logged) so
/// future Tauri commands can re-query the instance.
pub struct SurgeClient {
    client: Client,
    /// Same security posture as `client`, but with no whole-request timeout so
    /// the indefinite `/events` SSE response is never cut at 10 seconds. Only
    /// connect/handshake is bounded.
    stream_client: Client,
    base_url: String,
    token: String,
    downloads: Vec<Download>,
}

impl fmt::Debug for SurgeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print base_url (private tailnet hostname), download URLs
        // (they may carry signed secrets), the bearer token, or response
        // bodies. Only the count is safe to log.
        f.debug_struct("SurgeClient")
            .field("download_count", &self.downloads.len())
            .finish()
    }
}

/// Connection failure. Carries no bearer token and no response body, so both
/// `Display` and `Debug` are safe to log.
#[derive(Clone, PartialEq, Eq)]
pub enum ConnectError {
    InvalidEndpoint(String),
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
}

impl fmt::Debug for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint(reason) => {
                f.debug_tuple("InvalidEndpoint").field(reason).finish()
            }
            Self::EmptyToken => write!(f, "EmptyToken"),
            Self::Unauthorized => write!(f, "Unauthorized"),
            Self::Network => write!(f, "Network"),
            Self::Timeout => write!(f, "Timeout"),
            Self::BodyTooLarge => write!(f, "BodyTooLarge"),
            Self::InvalidResponse => write!(f, "InvalidResponse"),
            Self::RateUnconfirmed => write!(f, "RateUnconfirmed"),
            Self::SettingsConflict => write!(f, "SettingsConflict"),
            Self::SettingsUnconfirmed => write!(f, "SettingsUnconfirmed"),
            Self::BulkUnconfirmed => write!(f, "BulkUnconfirmed"),
        }
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint(reason) => write!(f, "invalid endpoint: {}", reason),
            Self::EmptyToken => write!(f, "missing bearer token"),
            Self::Unauthorized => write!(f, "unauthorized: invalid bearer token"),
            Self::Network => write!(f, "connection failed"),
            Self::Timeout => write!(f, "connection timed out"),
            Self::BodyTooLarge => write!(f, "response body too large"),
            Self::InvalidResponse => write!(f, "invalid response from server"),
            Self::RateUnconfirmed => write!(f, "rate limit state could not be confirmed"),
            Self::SettingsConflict => write!(f, "settings changed since editing began"),
            Self::SettingsUnconfirmed => write!(f, "settings change could not be confirmed"),
            Self::BulkUnconfirmed => write!(f, "bulk queue action could not be confirmed"),
        }
    }
}

impl std::error::Error for ConnectError {}

/// Released Surge v0.12.4 control actions (`cmd/http_api.go`).
///
/// Each action maps to one authenticated `POST` route. `RemoveRecord` uses
/// the distinct `/delete` route (drop the queue record only); the separate
/// `/purge` route that also removes the payload from disk is intentionally
/// never reachable through this type. v0.12.4 acknowledges pause/resume as
/// `pause_requested`/`resume_requested` (v0.12.2 used `paused`/`resumed`);
/// `/delete` still acknowledges `deleted`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlAction {
    Pause,
    Resume,
    RemoveRecord,
}

impl ControlAction {
    /// Exact origin-form path for the released control route.
    fn route(self) -> &'static str {
        match self {
            Self::Pause => "/pause",
            Self::Resume => "/resume",
            Self::RemoveRecord => "/delete",
        }
    }

    /// Canonical acknowledgment `status` the daemon echoes for this action.
    ///
    /// Released Surge v0.12.4 `cmd/http_api.go`: `pause_requested`,
    /// `resume_requested`, `deleted`. The v0.12.2 `paused`/`resumed` strings
    /// remain accepted via [`Self::is_acknowledged`] for backward
    /// compatibility; `/delete` never had a legacy alias.
    fn acknowledged_status(self) -> &'static str {
        match self {
            Self::Pause => "pause_requested",
            Self::Resume => "resume_requested",
            Self::RemoveRecord => "deleted",
        }
    }

    /// True for the canonical v0.12.4 acknowledgment plus the legacy v0.12.2
    /// `paused`/`resumed` strings (pause/resume only).
    fn is_acknowledged(self, status: &str) -> bool {
        if status == self.acknowledged_status() {
            return true;
        }
        match self {
            Self::Pause => status == "paused",
            Self::Resume => status == "resumed",
            Self::RemoveRecord => false,
        }
    }
}

/// Released Surge v0.12.4 bulk queue actions (`cmd/http_api.go`).
///
/// Only the two all-queue routes exist here. The enum is the allowlist: there
/// is no arbitrary route string or user-supplied path, so `/purge` and every
/// other daemon route stay unreachable through this type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkControlAction {
    PauseAll,
    ResumeAll,
}

/// Exact origin-form routes the bulk client is allowed to POST to.
const BULK_CONTROL_ROUTES: [&str; 2] = ["/pause-all", "/resume-all"];

impl BulkControlAction {
    /// Exact origin-form path for the released bulk route.
    fn route(self) -> &'static str {
        match self {
            Self::PauseAll => "/pause-all",
            Self::ResumeAll => "/resume-all",
        }
    }

    /// Count field the daemon echoes for this action.
    fn count_field(self) -> &'static str {
        match self {
            Self::PauseAll => "paused",
            Self::ResumeAll => "resumed",
        }
    }

    /// Route allowlist check. Kept explicit so a future variant cannot
    /// silently reach an unreviewed daemon route.
    fn is_allowlisted(self) -> bool {
        BULK_CONTROL_ROUTES.contains(&self.route())
    }
}

/// One sanitized per-item result from a bulk queue action.
///
/// `id` is the validated daemon row id. `error`, when present, is always the
/// fixed `BULK_ITEM_ERROR_MESSAGE`; the daemon's raw text is discarded so a
/// token, URL, or local path can never cross IPC. `Debug` never prints either
/// value because both are server-controlled.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkControlItemResult {
    pub id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl fmt::Debug for BulkControlItemResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BulkControlItemResult")
            .field("ok", &self.ok)
            .field("has_error", &self.error.is_some())
            .finish()
    }
}

/// Validated `POST /pause-all` or `POST /resume-all` outcome.
///
/// `count` is the daemon's `paused`/`resumed` success count. It is verified to
/// equal the number of per-item results without an error, so the frontend can
/// show accepted/failed counts without trusting an inconsistent number.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkControlOutcome {
    pub status: String,
    pub count: u64,
    pub results: Vec<BulkControlItemResult>,
}

impl fmt::Debug for BulkControlOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BulkControlOutcome")
            .field("count", &self.count)
            .field("result_count", &self.results.len())
            .finish()
    }
}

/// Result of a released `POST /download` request.
///
/// `serde(rename_all = "camelCase")` makes Tauri return exactly `"queued"`,
/// `"pendingApproval"` and `"duplicate"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AddOutcome {
    /// HTTP 200: accepted straight into the queue.
    Queued,
    /// HTTP 202: held for interactive approval (not queued yet).
    PendingApproval,
    /// HTTP 409: rejected as an already-known duplicate.
    Duplicate,
}

/// Released `POST /download` request body.
///
/// Exactly two fields: the URL verbatim and the approval check. No destination
/// path, headers or credentials are ever sent. `Debug` is intentionally not
/// derived so the URL (which may carry signed secrets) cannot leak.
#[derive(Serialize)]
struct AddRequest<'a> {
    url: &'a str,
    skip_approval: bool,
}

/// One conflict-safe `/settings` edit. `before` is the value the UI loaded;
/// the daemon rejects the patch with 409 if the current value differs.
#[derive(Serialize)]
struct SettingsPatchBody<'a> {
    changes: Vec<SettingsChangeBody<'a>>,
}

#[derive(Serialize)]
struct SettingsChangeBody<'a> {
    category: &'a str,
    key: &'a str,
    before: i64,
    value: i64,
}

/// Released Surge v0.12.4 `POST /download` response body (unchanged from
/// v0.12.2).
///
/// Only the trusted routing fields are modeled: `status` (outcome
/// discriminator), `id` (queue identifier) and `message` (duplicate
/// rejection). `filename` and any unknown fields are ignored. `Debug` is
/// intentionally not derived so a server-controlled `id`, `status` or
/// `message` can never surface through this type.
#[derive(Deserialize)]
struct AddResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Released control acknowledgment body: `{"status": ..., "id": ...}`.
///
/// Only the two fields whose values are verified are modeled; unknown fields
/// are ignored for forward compatibility. `Debug` is never derived so a
/// server-controlled `id` or `status` can never surface through this type.
#[derive(Deserialize)]
struct ControlAck {
    status: String,
    id: String,
}

fn invalid(reason: &str) -> ConnectError {
    ConnectError::InvalidEndpoint(reason.to_string())
}

/// Strict endpoint validation.
///
/// Accepts only a bare origin: http(s) scheme, no userinfo, no query, no
/// fragment, and no path beyond root. Plain HTTP is restricted to IP loopback
/// or `localhost`; anything else must use HTTPS so the bearer credential is
/// never sent in cleartext off-host.
fn validate_endpoint(endpoint: &str) -> Result<String, ConnectError> {
    let url = Url::parse(endpoint).map_err(|_| invalid("unparseable endpoint"))?;
    match url.scheme() {
        "https" => {}
        "http" => match url.host() {
            Some(Host::Ipv4(addr)) if addr.is_loopback() => {}
            Some(Host::Ipv6(addr)) if addr.is_loopback() => {}
            Some(Host::Domain(domain)) if domain.eq_ignore_ascii_case("localhost") => {}
            _ => {
                return Err(invalid(
                    "plain http is only allowed for IP loopback or localhost",
                ))
            }
        },
        _ => return Err(invalid("unsupported scheme")),
    }
    if url.host_str().is_none() {
        return Err(invalid("missing host"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid("userinfo is not allowed in endpoint"));
    }
    if url.query().is_some() {
        return Err(invalid("query is not allowed in endpoint"));
    }
    if url.fragment().is_some() {
        return Err(invalid("fragment is not allowed in endpoint"));
    }
    if !matches!(url.path(), "" | "/") {
        return Err(invalid("path beyond root is not allowed in endpoint"));
    }
    Ok(url.origin().ascii_serialization())
}

/// Strict local validation of a `POST /download` URL.
///
/// Only an absolute `http`/`https` URL with a host and no userinfo is accepted.
/// URL parsing is used solely to validate; the caller forwards the original
/// string verbatim so a signed query string is never normalized or dropped.
/// Empty, whitespace/control-bearing, oversized, userinfo-bearing and
/// non-HTTP(S) inputs are all rejected locally as a sanitized
/// [`ConnectError::InvalidResponse`] with no network request.
fn validate_download_url(url: &str) -> Result<(), ConnectError> {
    if url.is_empty() || url.len() > MAX_URL_BYTES {
        return Err(ConnectError::InvalidResponse);
    }
    if url
        .bytes()
        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(ConnectError::InvalidResponse);
    }
    let parsed = Url::parse(url).map_err(|_| ConnectError::InvalidResponse)?;
    match parsed.scheme() {
        "http" | "https" => {}
        _ => return Err(ConnectError::InvalidResponse),
    }
    if parsed.host_str().is_none() {
        return Err(ConnectError::InvalidResponse);
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ConnectError::InvalidResponse);
    }
    Ok(())
}

/// True only for a present, non-empty queue identifier.
fn has_nonempty_id(id: &Option<String>) -> bool {
    id.as_deref().is_some_and(|value| !value.is_empty())
}

/// Parse a Surge rate string with the pinned `utils.ParseRateLimit` `float64`
/// semantics.
///
/// Go does `strconv.ParseFloat(num, 64)` then `value * multiplier` (`/ 8` for
/// bits) then `math.Round`. That behavior is matched for daemon strings in the
/// JavaScript-safe range: e.g. `0.49999999999999999 B/s` parses to `0.5` in
/// `float64` and rounds to `1`, while long all-zero fractions parse to zero
/// without denominator overflow. The canonical daemon persistence `"<N> B/s"`
/// keeps an exact integer fast path so every safe integer round-trips without
/// `float64` loss. Invalid, non-finite, overflowing, and sub-byte inputs
/// (`0 < bytes < 0.5` rounding to zero, matching Go's "too small" error)
/// return `None`.
fn parse_rate_limit_string(input: &str) -> Option<i64> {
    let trimmed = input.trim();
    let lower = trimmed.to_ascii_lowercase();
    if trimmed.is_empty() || lower == "0" || lower == "\u{221e}" || lower == "unlimited" {
        return Some(0);
    }

    let compact: String = trimmed.chars().filter(|ch| *ch != ' ').collect();
    let numeric_end = compact
        .bytes()
        .take_while(|byte| byte.is_ascii_digit() || *byte == b'.')
        .count();
    if numeric_end == 0 {
        return None;
    }
    let numeric = &compact[..numeric_end];
    let supplied_unit = &compact[numeric_end..];
    let unit = if supplied_unit.is_empty() {
        "MB"
    } else {
        supplied_unit
    };
    let (multiplier, is_bits) = parse_rate_limit_unit(unit)?;

    // Exact integer fast path for the canonical daemon output `"<N> B/s"`.
    // `fmt.Sprintf("%d B/s", rate)` never emits a fraction, so every
    // JavaScript-safe integer round-trips without `float64` loss. Longer digit
    // strings that overflow `u128` fall through to the `float64` path below so
    // Go's `ParseFloat` overflow (`+Inf` with `ErrRange`) still maps to `None`.
    if supplied_unit == "B/s"
        && !numeric.contains('.')
        && !numeric.is_empty()
        && numeric.bytes().all(|byte| byte.is_ascii_digit())
    {
        if let Ok(exact) = numeric.parse::<u128>() {
            if exact == 0 {
                return Some(0);
            }
            if exact > MAX_RATE_LIMIT_BPS as u128 {
                return None;
            }
            return Some(exact as i64);
        }
        // Overflowed `u128`: fall through to `float64` handling.
    }

    let value: f64 = numeric.parse().ok()?;
    if !value.is_finite() {
        return None;
    }
    // Every supported multiplier is below 2^53, so the `u128 as f64` cast is
    // exact.
    let mut bytes = value * (multiplier as f64);
    if !bytes.is_finite() {
        return None;
    }
    if is_bits {
        bytes /= 8.0;
        if !bytes.is_finite() {
            return None;
        }
    }
    if bytes <= 0.0 {
        return Some(0);
    }
    if bytes > MAX_RATE_LIMIT_BPS as f64 {
        return None;
    }
    let rounded = bytes.round();
    if rounded == 0.0 {
        return None;
    }
    if rounded > MAX_RATE_LIMIT_BPS as f64 {
        return None;
    }
    Some(rounded as i64)
}

fn parse_rate_limit_unit(unit: &str) -> Option<(u128, bool)> {
    let lower = unit.to_ascii_lowercase();
    let key = lower.strip_suffix("/s").unwrap_or(lower.as_str());
    let (multiplier, mut is_bits) = match key {
        "b" | "byte" | "bytes" => (1, false),
        "kb" => (1_000, false),
        "mb" => (1_000_000, false),
        "gb" => (1_000_000_000, false),
        "tb" => (1_000_000_000_000, false),
        "kib" => (1024, false),
        "mib" => (1024 * 1024, false),
        "gib" => (1024 * 1024 * 1024, false),
        "tib" => (1024 * 1024 * 1024 * 1024, false),
        "bps" => (1, true),
        "kbps" => (1_000, true),
        "mbps" => (1_000_000, true),
        "gbps" => (1_000_000_000, true),
        "tbps" => (1_000_000_000_000, true),
        "kbit" => (1_000, true),
        "mbit" => (1_000_000, true),
        "gbit" => (1_000_000_000, true),
        "tbit" => (1_000_000_000_000, true),
        _ => return None,
    };

    if unit.ends_with("Bps") && !unit.contains('/') {
        is_bits = false;
    }
    if unit.ends_with("bit/s")
        || unit.ends_with("bits/s")
        || (unit.ends_with("b/s") && !unit.ends_with("B/s"))
        || (lower == "b/s" && !unit.starts_with('B'))
    {
        is_bits = true;
    }

    Some((multiplier, is_bits))
}

/// Convert the JSON value exposed at `network.global_rate_limit` into bytes/s.
///
/// The pinned `Setting.MarshalJSON` emits only the primitive value. Matching
/// `utils.ParseRateLimitValue`, JSON `null` is unlimited (`0`), a nonnegative
/// float is rounded half away from zero, strings use the string parser, and
/// any other type is rejected. Nonfinite values and results outside the
/// JavaScript-safe integer range are rejected rather than clamped.
fn parse_rate_limit_json(value: &serde_json::Value) -> Result<i64, ConnectError> {
    use serde_json::Value;

    let rate = match value {
        Value::Null => return Ok(0),
        Value::String(text) => {
            parse_rate_limit_string(text).ok_or(ConnectError::InvalidResponse)?
        }
        Value::Number(number) => {
            if number.is_f64() {
                let float = number.as_f64().ok_or(ConnectError::InvalidResponse)?;
                if !float.is_finite() || float < 0.0 {
                    return Err(ConnectError::InvalidResponse);
                }
                // Fail closed for large JSON floats whose decimal text cannot
                // be recovered from `serde_json::Value` (stores only `f64`).
                // Observed `serde_json` parses `"9007199254740991.0"` as
                // `9007199254740990.0`, while the pinned Go `float64` decoder
                // keeps the exact value; returning the rounded `f64` would
                // silently report a wrong byte rate. At magnitudes `>= 2^52`
                // the `f64` spacing is `>= 1`, so integer `.0` and fractional
                // forms at `MAX_RATE_LIMIT_BPS` cannot be distinguished
                // exactly. Reject rather than report a wrong value; exact
                // integers and canonical `"N B/s"` strings still round-trip
                // via the integer/string paths.
                if float >= 4_503_599_627_370_496.0 {
                    return Err(ConnectError::InvalidResponse);
                }
                // `f64::round` rounds half away from zero, matching Go's
                // `math.Round`. The safe-range check below also covers values
                // beyond `i64::MAX`, so the cast is always in range.
                let rounded = float.round();
                if rounded > MAX_RATE_LIMIT_BPS as f64 {
                    return Err(ConnectError::InvalidResponse);
                }
                rounded as i64
            } else if let Some(integer) = number.as_i64() {
                integer
            } else if let Some(unsigned) = number.as_u64() {
                if unsigned > MAX_RATE_LIMIT_BPS as u64 {
                    return Err(ConnectError::InvalidResponse);
                }
                unsigned as i64
            } else {
                return Err(ConnectError::InvalidResponse);
            }
        }
        _ => return Err(ConnectError::InvalidResponse),
    };

    if !(0..=MAX_RATE_LIMIT_BPS).contains(&rate) {
        return Err(ConnectError::InvalidResponse);
    }
    Ok(rate)
}

/// Parse the primitive `network.max_concurrent_downloads` value. The pinned
/// `Setting.MarshalJSON` emits only the value, so a number is expected.
/// Whole-valued floats are accepted for forward compatibility; anything
/// outside 1–10 is rejected rather than clamped.
fn parse_max_concurrent_json(value: &serde_json::Value) -> Result<i64, ConnectError> {
    use serde_json::Value;

    let parsed = match value {
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                integer
            } else if let Some(unsigned) = number.as_u64() {
                i64::try_from(unsigned).map_err(|_| ConnectError::InvalidResponse)?
            } else {
                let float = number.as_f64().ok_or(ConnectError::InvalidResponse)?;
                if !float.is_finite() || float.fract() != 0.0 {
                    return Err(ConnectError::InvalidResponse);
                }
                float as i64
            }
        }
        _ => return Err(ConnectError::InvalidResponse),
    };
    if !(MAX_CONCURRENT_DOWNLOADS_MIN..=MAX_CONCURRENT_DOWNLOADS_MAX).contains(&parsed) {
        return Err(ConnectError::InvalidResponse);
    }
    Ok(parsed)
}

async fn get_settings_max_concurrent(
    client: &Client,
    base_url: &str,
    token: &str,
) -> Result<i64, ConnectError> {
    let response = client
        .get(format!("{}/settings", base_url))
        .bearer_auth(token)
        .send()
        .await
        .map_err(map_send_error)?;
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(ConnectError::Unauthorized);
    }
    if status != StatusCode::OK {
        return Err(ConnectError::InvalidResponse);
    }
    let bytes = read_bounded_body(response).await?;
    let settings: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| ConnectError::InvalidResponse)?;
    let value = settings
        .get("network")
        .and_then(|network| network.get("max_concurrent_downloads"))
        .ok_or(ConnectError::InvalidResponse)?;
    parse_max_concurrent_json(value)
}

async fn get_settings_rate_limit(
    client: &Client,
    base_url: &str,
    token: &str,
) -> Result<i64, ConnectError> {
    let response = client
        .get(format!("{}/settings", base_url))
        .bearer_auth(token)
        .send()
        .await
        .map_err(map_send_error)?;
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(ConnectError::Unauthorized);
    }
    if status != StatusCode::OK {
        return Err(ConnectError::InvalidResponse);
    }
    let bytes = read_bounded_body(response).await?;
    let settings: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| ConnectError::InvalidResponse)?;
    let value = settings
        .get("network")
        .and_then(|network| network.get("global_rate_limit"))
        .ok_or(ConnectError::InvalidResponse)?;
    parse_rate_limit_json(value)
}

/// Case-insensitive match against the exact released duplicate-rejection
/// message.
fn is_duplicate_message(message: &str) -> bool {
    message.trim().eq_ignore_ascii_case(DUPLICATE_MESSAGE)
}

fn map_send_error(err: reqwest::Error) -> ConnectError {
    if err.is_timeout() {
        ConnectError::Timeout
    } else {
        ConnectError::Network
    }
}

/// Build the manual-mode client. `streaming` keeps the same redirect policy but
/// replaces the whole-request timeout with a bounded connect/handshake timeout,
/// because an idle SSE response must be allowed to stay open indefinitely.
fn build_manual_client(streaming: bool) -> Result<Client, ConnectError> {
    let mut builder = Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .connect_timeout(CONNECT_TIMEOUT);
    if !streaming {
        builder = builder.timeout(CONNECT_TIMEOUT);
    }
    builder.build().map_err(|_| ConnectError::Network)
}

/// Validate the released SSE response before any body is read.
fn validate_event_response(response: &reqwest::Response) -> Result<(), ConnectError> {
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(ConnectError::Unauthorized);
    }
    if status != StatusCode::OK {
        return Err(ConnectError::InvalidResponse);
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if !content_type.is_some_and(|value| value.eq_ignore_ascii_case("text/event-stream")) {
        return Err(ConnectError::InvalidResponse);
    }
    Ok(())
}

/// Read a `/list` body in chunks, enforcing `MAX_BODY_BYTES` even when the
/// server omits `Content-Length` (chunked transfer). `bytes().await` would
/// buffer an unbounded body first, so chunks are accumulated with a cap.
async fn read_bounded_body(mut response: reqwest::Response) -> Result<Vec<u8>, ConnectError> {
    if let Some(len) = response.content_length() {
        if len > MAX_BODY_BYTES {
            return Err(ConnectError::BodyTooLarge);
        }
    }
    let mut buf = Vec::new();
    loop {
        match response.chunk().await.map_err(map_send_error)? {
            None => break,
            Some(chunk) => {
                let used = buf.len() as u64;
                if used.saturating_add(chunk.len() as u64) > MAX_BODY_BYTES {
                    return Err(ConnectError::BodyTooLarge);
                }
                buf.extend_from_slice(&chunk);
            }
        }
    }
    Ok(buf)
}

/// Strictly validate a released bulk control body.
///
/// The released v0.12.4 shape is
/// `{"status":"done","paused"|"resumed":N,"results":[{"id":...,"error"?:...}]}`.
/// Anything unexpected is [`ConnectError::BulkUnconfirmed`] because the daemon
/// may already have applied part of the action: unknown/malformed outcomes are
/// never reported as a clean failure. Ids must be present, bounded, unique and
/// control-free; `error` must be a string when present, but its content is
/// discarded and replaced with [`BULK_ITEM_ERROR_MESSAGE`]; and the echoed
/// count must exactly match the per-item successes.
fn parse_bulk_response(
    action: BulkControlAction,
    bytes: &[u8],
) -> Result<BulkControlOutcome, ConnectError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| ConnectError::BulkUnconfirmed)?;
    let object = value.as_object().ok_or(ConnectError::BulkUnconfirmed)?;
    if object.get("status").and_then(serde_json::Value::as_str) != Some("done") {
        return Err(ConnectError::BulkUnconfirmed);
    }
    let count = object
        .get(action.count_field())
        .and_then(serde_json::Value::as_u64)
        .ok_or(ConnectError::BulkUnconfirmed)?;
    let raw_results = object
        .get("results")
        .and_then(serde_json::Value::as_array)
        .ok_or(ConnectError::BulkUnconfirmed)?;
    if raw_results.len() > MAX_BULK_RESULT_ITEMS {
        return Err(ConnectError::BulkUnconfirmed);
    }
    let mut seen = HashSet::with_capacity(raw_results.len());
    let mut results = Vec::with_capacity(raw_results.len());
    for raw in raw_results {
        let item = raw.as_object().ok_or(ConnectError::BulkUnconfirmed)?;
        let id = item
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or(ConnectError::BulkUnconfirmed)?;
        if id.is_empty() || id.len() > MAX_BULK_ID_BYTES || id.chars().any(char::is_control) {
            return Err(ConnectError::BulkUnconfirmed);
        }
        if !seen.insert(id) {
            return Err(ConnectError::BulkUnconfirmed);
        }
        let error = match item.get("error") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(_)) => Some(BULK_ITEM_ERROR_MESSAGE.to_string()),
            Some(_) => return Err(ConnectError::BulkUnconfirmed),
        };
        results.push(BulkControlItemResult {
            id: id.to_string(),
            ok: error.is_none(),
            error,
        });
    }
    let successes = results.iter().filter(|result| result.ok).count() as u64;
    if count != successes {
        return Err(ConnectError::BulkUnconfirmed);
    }
    Ok(BulkControlOutcome {
        status: "done".to_string(),
        count,
        results,
    })
}

/// Shared `GET <base>/list` probe used by both `connect` and `refresh`.
async fn get_list(
    client: &Client,
    base_url: &str,
    token: &str,
) -> Result<Vec<Download>, ConnectError> {
    let list_url = format!("{}/list", base_url);
    let response = client
        .get(list_url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(map_send_error)?;
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(ConnectError::Unauthorized);
    }
    if status != StatusCode::OK {
        return Err(ConnectError::InvalidResponse);
    }
    let bytes = read_bounded_body(response).await?;
    // Fresh/empty daemon encodes an uninitialized Go slice as JSON `null`
    // (upstream v0.12.4 `List()` leaves `var statuses` nil). Accept ONLY
    // `null` or an array; any other JSON shape stays `InvalidResponse`.
    let downloads: Option<Vec<Download>> =
        serde_json::from_slice(&bytes).map_err(|_| ConnectError::InvalidResponse)?;
    Ok(downloads.unwrap_or_default())
}

impl SurgeClient {
    /// Verify bearer auth against `GET <endpoint>/list` and retain the client.
    ///
    /// `/health` is unauthenticated on Surge and proves nothing, so auth is
    /// verified with `/list`. Redirects are disabled so the `Authorization`
    /// header never follows a redirect off the validated origin.
    pub async fn connect(endpoint: &str, token: &str) -> Result<Self, ConnectError> {
        if token.is_empty() || token.trim().is_empty() {
            return Err(ConnectError::EmptyToken);
        }
        if token
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        {
            return Err(invalid("token contains invalid characters"));
        }
        let base = validate_endpoint(endpoint)?;
        let client = build_manual_client(false)?;
        let stream_client = build_manual_client(true)?;
        let downloads = get_list(&client, &base, token).await?;
        Ok(Self {
            client,
            stream_client,
            base_url: base,
            token: token.to_string(),
            downloads,
        })
    }

    /// Downloads captured by the authenticated `/list` probe.
    pub fn list_downloads(&self) -> &[Download] {
        &self.downloads
    }

    /// Validated base origin (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Re-query `/list` with the retained bearer token.
    pub async fn refresh(&self) -> Result<Vec<Download>, ConnectError> {
        get_list(&self.client, &self.base_url, &self.token).await
    }

    /// Open the authenticated, indefinite `GET /events` SSE response.
    ///
    /// Uses the retained streaming client, which keeps the same pinned
    /// TLS/no-redirect/no-proxy posture as the normal client but has no
    /// whole-request timeout. Only the TCP/TLS/header handshake is bounded;
    /// once headers arrive the established body may stay idle indefinitely.
    /// The response status and `Content-Type: text/event-stream` are validated
    /// before the caller reads any body. The bearer token travels only in the
    /// `Authorization` header; no request URL, token or response content is
    /// carried in an error.
    pub async fn open_event_stream(&self) -> Result<reqwest::Response, ConnectError> {
        self.open_event_stream_with_timeout(EVENT_STREAM_HANDSHAKE_TIMEOUT)
            .await
    }

    async fn open_event_stream_with_timeout(
        &self,
        handshake_timeout: Duration,
    ) -> Result<reqwest::Response, ConnectError> {
        let request = self
            .stream_client
            .get(format!("{}/events", self.base_url))
            .bearer_auth(&self.token)
            .header(ACCEPT, "text/event-stream")
            .send();
        let response = match tokio::time::timeout(handshake_timeout, request).await {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => return Err(map_send_error(err)),
            Err(_) => return Err(ConnectError::Timeout),
        };
        validate_event_response(&response)?;
        Ok(response)
    }

    /// Apply a released control action to one queue record.
    ///
    /// The `id` is preserved verbatim as the URL query value via
    /// `query_pairs_mut` (never path interpolation or normalization), and an
    /// empty `id` is rejected locally without sending a request. The retained
    /// client already disables redirects and bounds the round trip. The bearer
    /// token travels in the `Authorization` header only, the response body is
    /// bounded before the acknowledgment is verified, and the request URL,
    /// token, id and body never appear in an error.
    pub async fn control_download(
        &self,
        action: ControlAction,
        id: &str,
    ) -> Result<(), ConnectError> {
        if id.is_empty() {
            return Err(ConnectError::InvalidResponse);
        }
        let mut url = Url::parse(&self.base_url).map_err(|_| ConnectError::InvalidResponse)?;
        url.set_path(action.route());
        url.query_pairs_mut().append_pair("id", id);
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(map_send_error)?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(ConnectError::Unauthorized);
        }
        if status != StatusCode::OK {
            return Err(ConnectError::InvalidResponse);
        }
        let bytes = read_bounded_body(response).await?;
        let ack: ControlAck =
            serde_json::from_slice(&bytes).map_err(|_| ConnectError::InvalidResponse)?;
        if !action.is_acknowledged(&ack.status) || ack.id != id {
            return Err(ConnectError::InvalidResponse);
        }
        Ok(())
    }

    /// Apply a released bulk control action to the whole daemon queue.
    ///
    /// The route comes only from [`BulkControlAction`] and is checked against
    /// the explicit allowlist before any request is built. The retained client
    /// already disables redirects and bounds the round trip; the bearer token
    /// travels in the `Authorization` header only, the response body is bounded
    /// before validation, and no request URL, token, id or body appears in an
    /// error.
    ///
    /// A 401/403 response is a definitive rejection. A 500 is the released
    /// `service.List()` failure that returns before any item is touched, so it
    /// is a definitive non-application. Every other transport, status, body or
    /// validation failure is [`ConnectError::BulkUnconfirmed`] because some of
    /// the bulk action may already have taken effect.
    pub async fn bulk_control(
        &self,
        action: BulkControlAction,
    ) -> Result<BulkControlOutcome, ConnectError> {
        if !action.is_allowlisted() {
            return Err(ConnectError::InvalidResponse);
        }
        let mut url = Url::parse(&self.base_url).map_err(|_| ConnectError::InvalidResponse)?;
        url.set_path(action.route());
        let response = match self.client.post(url).bearer_auth(&self.token).send().await {
            Ok(response) => response,
            Err(_) => return Err(ConnectError::BulkUnconfirmed),
        };
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(ConnectError::Unauthorized);
        }
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            // `handlePauseAll`/`handleResumeAll` call `service.List()` first
            // and return 500 before iterating, so nothing was applied.
            return Err(ConnectError::InvalidResponse);
        }
        if status != StatusCode::OK {
            return Err(ConnectError::BulkUnconfirmed);
        }
        let bytes = match read_bounded_body(response).await {
            Ok(bytes) => bytes,
            Err(_) => return Err(ConnectError::BulkUnconfirmed),
        };
        parse_bulk_response(action, &bytes)
    }

    /// Request a graceful shutdown of this managed daemon.
    ///
    /// Uses the same retained pinned-TLS `reqwest::Client` as every other
    /// managed call, so the bearer token travels only in the `Authorization`
    /// header to the already-verified managed origin. The daemon's released
    /// contract is `POST /shutdown` -> `202 Accepted` with
    /// `{"status":"shutting_down"}`. Redirects are already disabled and the
    /// response body is bounded before it is discarded. Request URL, token and
    /// body never appear in an error.
    pub async fn shutdown(&self) -> Result<(), ConnectError> {
        let response = self
            .client
            .post(format!("{}/shutdown", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(map_send_error)?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(ConnectError::Unauthorized);
        }
        if status != StatusCode::ACCEPTED {
            return Err(ConnectError::InvalidResponse);
        }
        let _ = read_bounded_body(response).await?;
        Ok(())
    }

    /// Enqueue a download through the released `POST /download` route.
    ///
    /// The URL is validated locally (no network on rejection) and forwarded
    /// verbatim as the JSON `url` field together with `skip_approval: false`;
    /// no path, headers or credentials are ever sent. The retained client
    /// already disables redirects and bounds the round trip, the bearer token
    /// travels in the `Authorization` header only, and the response body is
    /// bounded before the outcome is verified. Request URL, token, response
    /// body, `id`, `filename` and reqwest errors never appear in an error.
    pub async fn add_download(&self, url: &str) -> Result<AddOutcome, ConnectError> {
        validate_download_url(url)?;
        let body = serde_json::to_vec(&AddRequest {
            url,
            skip_approval: false,
        })
        .map_err(|_| ConnectError::InvalidResponse)?;
        let response = self
            .client
            .post(format!("{}/download", self.base_url))
            .bearer_auth(&self.token)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(map_send_error)?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(ConnectError::Unauthorized);
        }
        match status {
            StatusCode::OK | StatusCode::ACCEPTED | StatusCode::CONFLICT => {}
            _ => return Err(ConnectError::InvalidResponse),
        }
        let bytes = read_bounded_body(response).await?;
        let parsed: AddResponse =
            serde_json::from_slice(&bytes).map_err(|_| ConnectError::InvalidResponse)?;
        match status {
            StatusCode::OK => {
                if parsed.status.as_deref() == Some("queued") && has_nonempty_id(&parsed.id) {
                    Ok(AddOutcome::Queued)
                } else {
                    Err(ConnectError::InvalidResponse)
                }
            }
            StatusCode::ACCEPTED => {
                if parsed.status.as_deref() == Some("pending_approval")
                    && has_nonempty_id(&parsed.id)
                {
                    Ok(AddOutcome::PendingApproval)
                } else {
                    Err(ConnectError::InvalidResponse)
                }
            }
            _ => {
                if parsed.status.as_deref() == Some("error")
                    && parsed.message.as_deref().is_some_and(is_duplicate_message)
                {
                    Ok(AddOutcome::Duplicate)
                } else {
                    Err(ConnectError::InvalidResponse)
                }
            }
        }
    }

    /// Read the live global download limit from the pinned `/settings` route.
    ///
    /// Only the authenticated origin is contacted, the response body is
    /// bounded, and the returned value is the nonnegative integer bytes/sec in
    /// `[0, MAX_RATE_LIMIT_BPS]`. No response body, path, or credential is
    /// included in an error.
    pub async fn get_global_rate_limit(&self) -> Result<i64, ConnectError> {
        get_settings_rate_limit(&self.client, &self.base_url, &self.token).await
    }

    /// POST the exact integer rate. Every failure after the request is handed
    /// to the HTTP stack is ambiguous: the daemon may already have applied the
    /// setting even when the response is an error, malformed, or missing.
    async fn post_global_rate_limit(&self, rate: i64) -> Result<(), ConnectError> {
        let mut url = Url::parse(&self.base_url).map_err(|_| ConnectError::InvalidResponse)?;
        url.set_path("/rate-limit/global");
        url.query_pairs_mut().append_pair("rate", &rate.to_string());
        let response = match self.client.post(url).bearer_auth(&self.token).send().await {
            Ok(response) => response,
            Err(_) => return Err(ConnectError::RateUnconfirmed),
        };
        let status = response.status();
        if status != StatusCode::OK {
            return Err(ConnectError::RateUnconfirmed);
        }
        let bytes = match read_bounded_body(response).await {
            Ok(bytes) => bytes,
            Err(_) => return Err(ConnectError::RateUnconfirmed),
        };
        let ack: serde_json::Value = match serde_json::from_slice(&bytes) {
            Ok(ack) => ack,
            Err(_) => return Err(ConnectError::RateUnconfirmed),
        };
        let expected_status = if rate == 0 {
            "global_rate_unlimited"
        } else {
            "global_rate_limited"
        };
        let expected_rate = rate.to_string();
        if ack.get("status").and_then(serde_json::Value::as_str) != Some(expected_status)
            || ack.get("rate").and_then(serde_json::Value::as_str) != Some(expected_rate.as_str())
        {
            return Err(ConnectError::RateUnconfirmed);
        }
        Ok(())
    }

    /// Apply a global download limit and reconcile it with a fresh readback.
    ///
    /// Validation is local and bounded. Any `POST` failure after the request
    /// starts is reported as [`ConnectError::RateUnconfirmed`] immediately,
    /// without trusting a readback: a delayed request may still apply later.
    /// The connection manager must permanently fence its writer afterwards.
    /// Only a definitive success acknowledgement is read back; success is
    /// returned only when the live `/settings` value exactly matches `rate`.
    /// An unreadable/unparseable readback is also `RateUnconfirmed`; a
    /// definitively different value is `InvalidResponse`. Returns the
    /// confirmed bytes/sec.
    pub async fn set_global_rate_limit(&self, rate: i64) -> Result<i64, ConnectError> {
        if !(0..=MAX_RATE_LIMIT_BPS).contains(&rate) {
            return Err(ConnectError::InvalidResponse);
        }

        self.post_global_rate_limit(rate).await?;
        match get_settings_rate_limit(&self.client, &self.base_url, &self.token).await {
            Ok(confirmed) if confirmed == rate => Ok(confirmed),
            Ok(_) => Err(ConnectError::InvalidResponse),
            Err(_) => Err(ConnectError::RateUnconfirmed),
        }
    }

    /// Read the saved daemon `network.max_concurrent_downloads` value.
    ///
    /// This is the persisted settings value, not a claim about the running
    /// daemon's active concurrency; Surge applies it only after a restart.
    pub async fn get_max_concurrent_downloads(&self) -> Result<i64, ConnectError> {
        get_settings_max_concurrent(&self.client, &self.base_url, &self.token).await
    }

    /// Save `network.max_concurrent_downloads` through the conflict-safe
    /// `PATCH /settings` route.
    ///
    /// `before` is the value the caller loaded; the daemon returns 409 if its
    /// current value differs, so a concurrent change is never overwritten.
    /// Even when `before == value` the patch is sent, so an externally changed
    /// daemon value is reported as a conflict instead of being assumed
    /// unchanged. Only a 200 response whose body confirms the requested value is a
    /// success. A 400/409 is a definitive non-application; auth failures are
    /// definitive. Every other transport/status/body outcome is ambiguous:
    /// the daemon may have persisted the change, so returning
    /// [`ConnectError::SettingsUnconfirmed`] lets the manager fence later
    /// writes instead of retrying silently. The app never restarts the daemon.
    pub async fn set_max_concurrent_downloads(
        &self,
        before: i64,
        value: i64,
    ) -> Result<i64, ConnectError> {
        let range = MAX_CONCURRENT_DOWNLOADS_MIN..=MAX_CONCURRENT_DOWNLOADS_MAX;
        if !range.contains(&before) || !range.contains(&value) {
            return Err(ConnectError::InvalidResponse);
        }

        let body = serde_json::to_vec(&SettingsPatchBody {
            changes: vec![SettingsChangeBody {
                category: "Network",
                key: "max_concurrent_downloads",
                before,
                value,
            }],
        })
        .map_err(|_| ConnectError::InvalidResponse)?;
        let response = match self
            .client
            .patch(format!("{}/settings", self.base_url))
            .bearer_auth(&self.token)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => return Err(ConnectError::SettingsUnconfirmed),
        };
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(ConnectError::Unauthorized);
        }
        if status == StatusCode::CONFLICT {
            return Err(ConnectError::SettingsConflict);
        }
        if status == StatusCode::BAD_REQUEST {
            return Err(ConnectError::InvalidResponse);
        }
        if status != StatusCode::OK {
            return Err(ConnectError::SettingsUnconfirmed);
        }
        let bytes = match read_bounded_body(response).await {
            Ok(bytes) => bytes,
            Err(_) => return Err(ConnectError::SettingsUnconfirmed),
        };
        let settings: serde_json::Value = match serde_json::from_slice(&bytes) {
            Ok(settings) => settings,
            Err(_) => return Err(ConnectError::SettingsUnconfirmed),
        };
        let confirmed = settings
            .get("network")
            .and_then(|network| network.get("max_concurrent_downloads"))
            .ok_or(ConnectError::SettingsUnconfirmed)
            .and_then(|value| {
                parse_max_concurrent_json(value).map_err(|_| ConnectError::SettingsUnconfirmed)
            })?;
        if confirmed != value {
            return Err(ConnectError::SettingsUnconfirmed);
        }
        Ok(confirmed)
    }
}

/// Validate the managed-mode bearer token (same rules as manual `connect`).
///
/// Empty/blank or whitespace/control-bearing tokens are rejected locally with
/// sanitized errors; the token value never appears in an error.
fn validate_managed_token(token: &str) -> Result<(), ConnectError> {
    if token.is_empty() || token.trim().is_empty() {
        return Err(ConnectError::EmptyToken);
    }
    if token
        .bytes()
        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(invalid("token contains invalid characters"));
    }
    Ok(())
}

/// Parse the per-launch PEM CA bundle for managed mode.
///
/// Accepts one or more PEM certificates via `from_pem_bundle`. Empty,
/// oversized, or unparseable input (including private-key PEM masquerading as
/// a CA) is rejected as a sanitized `InvalidEndpoint`; the bytes are never
/// echoed.
fn parse_managed_ca(ca_pem: &[u8]) -> Result<Vec<Certificate>, ConnectError> {
    if ca_pem.is_empty() || ca_pem.len() > MAX_CA_PEM_BYTES {
        return Err(invalid("invalid managed CA certificate"));
    }
    let certs = Certificate::from_pem_bundle(ca_pem)
        .map_err(|_| invalid("invalid managed CA certificate"))?;
    if certs.is_empty() {
        return Err(invalid("invalid managed CA certificate"));
    }
    Ok(certs)
}

/// Build the managed-mode `reqwest` client with the pinned security posture.
///
/// - Rustls backend (`use_rustls_tls`); default/system roots disabled via
///   `tls_built_in_root_certs(false)` so only the per-launch CA is trusted.
///   Hostname/IP SAN validation stays enabled: `danger_accept_invalid_certs`
///   / `danger_accept_invalid_hostnames` are never set.
/// - `https_only(true)`: no plain-HTTP request can ever be built.
/// - `no_proxy()`: environment/system proxies are never used.
/// - `redirect(Policy::none())`: the `Authorization` header never follows a
///   redirect.
/// - Bounded round trip via `CONNECT_TIMEOUT`.
/// - No idle connection pool: shutdown/host-exit uses the retained client from
///   a dedicated current-thread runtime, and reusing an idle socket bound to a
///   different (possibly blocked) runtime could stall the graceful request.
/// - TLS early data is never enabled: this crate does not enable reqwest's
///   `http3`/`reqwest_unstable` early-data API, so no early data is sent.
fn build_managed_client(certs: Vec<Certificate>) -> Result<Client, ConnectError> {
    build_managed_client_with_timeout(certs, true)
}

fn build_managed_stream_client(certs: Vec<Certificate>) -> Result<Client, ConnectError> {
    build_managed_client_with_timeout(certs, false)
}

fn build_managed_client_with_timeout(
    certs: Vec<Certificate>,
    whole_request_timeout: bool,
) -> Result<Client, ConnectError> {
    let mut builder = Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .https_only(true)
        .redirect(Policy::none())
        .no_proxy()
        .pool_max_idle_per_host(0)
        .connect_timeout(CONNECT_TIMEOUT);
    if whole_request_timeout {
        builder = builder.timeout(CONNECT_TIMEOUT);
    }
    for cert in certs {
        builder = builder.add_root_certificate(cert);
    }
    builder.build().map_err(|_| ConnectError::Network)
}

impl SurgeClient {
    /// Internal Rust-only managed-mode constructor (no Tauri IPC).
    ///
    /// `ca_pem` is the per-launch PEM CA bundle bytes and `token` the
    /// per-launch secret bearer token; both are held in memory only, never
    /// persisted, never logged, and never sent over IPC (this method takes
    /// Rust values, not a Tauri command). The origin is always pinned to
    /// [`MANAGED_BASE_URL`] (`https://127.0.0.1:1700`); there is no endpoint
    /// argument and no HTTP fallback.
    ///
    /// All later requests on the returned client (`refresh`,
    /// `control_download`, `add_download`) reuse the same hardened
    /// `reqwest::Client`, so the rustls / pinned-CA / no-proxy / no-redirect /
    /// https-only posture applies to every request, not just the `/list`
    /// probe.
    pub async fn connect_managed(ca_pem: &[u8], token: &str) -> Result<Self, ConnectError> {
        validate_managed_token(token)?;
        let certs = parse_managed_ca(ca_pem)?;
        let client = build_managed_client(certs)?;
        let stream_certs = parse_managed_ca(ca_pem)?;
        let stream_client = build_managed_stream_client(stream_certs)?;
        let base = MANAGED_BASE_URL.to_string();
        let downloads = get_list(&client, &base, token).await?;
        Ok(Self {
            client,
            stream_client,
            base_url: base,
            token: token.to_string(),
            downloads,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed_identity::ManagedIdentity;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn bulk_control_routes_are_explicitly_allowlisted() {
        assert_eq!(BulkControlAction::PauseAll.route(), "/pause-all");
        assert_eq!(BulkControlAction::ResumeAll.route(), "/resume-all");
        for action in [BulkControlAction::PauseAll, BulkControlAction::ResumeAll] {
            assert!(action.is_allowlisted(), "bulk route must be allowlisted");
        }
        assert!(
            !BULK_CONTROL_ROUTES.contains(&"/purge"),
            "purge must never be an allowed bulk route"
        );
    }

    fn server_config_for(id: &ManagedIdentity) -> Arc<rustls::ServerConfig> {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
        let leaf_der = CertificateDer::from(id.server_cert_der().to_vec());
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(id.server_key_der().to_vec()));
        Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![leaf_der], key_der)
                .expect("server identity"),
        )
    }

    fn managed_client_for(id: &ManagedIdentity) -> SurgeClient {
        let certs = parse_managed_ca(id.ca_cert_pem().as_bytes()).expect("managed CA");
        let client = build_managed_client(certs).expect("managed client");
        let stream_certs =
            parse_managed_ca(id.ca_cert_pem().as_bytes()).expect("managed stream CA");
        let stream_client =
            build_managed_stream_client(stream_certs).expect("managed stream client");
        SurgeClient {
            client,
            stream_client,
            base_url: MANAGED_BASE_URL.to_string(),
            token: id.bearer_token().to_owned(),
            downloads: Vec::new(),
        }
    }

    fn read_headers(stream: &mut impl Read) -> Vec<u8> {
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

    fn accept_with_deadline(listener: &TcpListener) -> std::net::TcpStream {
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    // Windows: sockets accepted from a nonblocking listener
                    // can inherit nonblocking mode (WSAEWOULDBLOCK 10035 in
                    // rustls). Force blocking before any rustls read/write.
                    stream
                        .set_nonblocking(false)
                        .expect("accepted stream must be blocking");
                    return stream;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "accept timed out");
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept failed: {error:?}"),
            }
        }
    }

    #[tokio::test]
    async fn shutdown_posts_authenticated_request_over_pinned_tls() {
        let _guard = crate::managed_server::test_port_lock().lock().await;
        let id = ManagedIdentity::generate().expect("identity");
        let surge = managed_client_for(&id);
        let token = id.bearer_token().to_owned();
        let server_config = server_config_for(&id);
        let listener = TcpListener::bind("127.0.0.1:1700").expect("bind 1700");
        let (capture_tx, capture_rx) = mpsc::channel::<(bool, bool)>();
        let server = thread::spawn(move || {
            let mut stream = accept_with_deadline(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .expect("write timeout");
            let mut conn = rustls::ServerConnection::new(server_config).expect("server conn");
            let mut tls = rustls::Stream::new(&mut conn, &mut stream);
            let request = read_headers(&mut tls);
            let text = String::from_utf8_lossy(&request);
            let route_ok = text.starts_with("POST /shutdown HTTP/1.1");
            let auth_ok = text.to_ascii_lowercase().contains(&format!(
                "authorization: bearer {}",
                token.to_ascii_lowercase()
            ));
            let body = br#"{"status":"shutting_down"}"#;
            let response = format!(
                "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            tls.write_all(response.as_bytes())
                .expect("response headers");
            tls.write_all(body).expect("response body");
            tls.flush().expect("flush");
            let _ = capture_tx.send((route_ok, auth_ok));
        });

        surge.shutdown().await.expect("202 must be accepted");
        let (route_ok, auth_ok) = capture_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("captured request");
        server.join().expect("server thread");
        assert!(route_ok, "shutdown must use POST /shutdown");
        assert!(auth_ok, "bearer must travel only in Authorization header");
    }

    #[tokio::test]
    async fn event_stream_never_reaches_untrusted_tls_identity() {
        let _guard = crate::managed_server::test_port_lock().lock().await;
        let client_id = ManagedIdentity::generate().expect("client identity");
        let server_id = ManagedIdentity::generate().expect("server identity");
        let surge = managed_client_for(&client_id);
        let server_config = server_config_for(&server_id);
        let listener = TcpListener::bind("127.0.0.1:1700").expect("bind 1700");
        let (capture_tx, capture_rx) = mpsc::channel::<bool>();
        let server = thread::spawn(move || {
            let mut stream = accept_with_deadline(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            let mut conn = rustls::ServerConnection::new(server_config).expect("server conn");
            let mut tls = rustls::Stream::new(&mut conn, &mut stream);
            // Handshake fails before any HTTP bytes can be read with the wrong
            // trust anchor.
            let request = read_headers(&mut tls);
            let _ = capture_tx.send(!request.is_empty());
        });

        let result = surge.open_event_stream().await;
        assert!(
            result.is_err(),
            "event stream must not succeed against an untrusted TLS identity"
        );
        let request_reached = capture_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("captured handshake result");
        server.join().expect("server thread");
        assert!(
            !request_reached,
            "no HTTP request may reach an untrusted listener"
        );
    }

    #[tokio::test]
    async fn event_stream_handshake_timeout_bounds_a_silent_server() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let server = thread::spawn(move || {
            let mut stream = accept_with_deadline(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout");
            let _ = read_headers(&mut stream);
            // Accept the request, then never send response headers.
            thread::sleep(Duration::from_millis(200));
        });
        let surge = SurgeClient {
            client: build_manual_client(false).expect("manual client"),
            stream_client: build_manual_client(true).expect("manual stream client"),
            base_url: format!("http://{}", addr),
            token: "test-token".to_string(),
            downloads: Vec::new(),
        };
        let started = Instant::now();
        let error = surge
            .open_event_stream_with_timeout(Duration::from_millis(40))
            .await
            .expect_err("silent server must time out");
        assert_eq!(error, ConnectError::Timeout);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "handshake wait must be bounded"
        );
        server.join().expect("server thread");
    }

    #[tokio::test]
    async fn event_stream_does_not_time_out_an_idle_established_body() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let server = thread::spawn(move || {
            let mut stream = accept_with_deadline(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout");
            let _ = read_headers(&mut stream);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
                )
                .expect("headers");
            stream.flush().expect("flush");
            // Stay idle longer than the handshake timeout, then send a frame.
            thread::sleep(Duration::from_millis(150));
            stream.write_all(b"data: ok\n\n").expect("body");
            stream.flush().expect("flush");
        });
        let surge = SurgeClient {
            client: build_manual_client(false).expect("manual client"),
            stream_client: build_manual_client(true).expect("manual stream client"),
            base_url: format!("http://{}", addr),
            token: "test-token".to_string(),
            downloads: Vec::new(),
        };
        let response = surge
            .open_event_stream_with_timeout(Duration::from_millis(30))
            .await
            .expect("headers must establish the stream");
        let body = tokio::time::timeout(Duration::from_secs(2), response.text())
            .await
            .expect("idle body must not be cut by the handshake timeout")
            .expect("body");
        assert_eq!(body, "data: ok\n\n");
        server.join().expect("server thread");
    }

    #[tokio::test]
    async fn shutdown_never_reaches_untrusted_tls_identity() {
        let _guard = crate::managed_server::test_port_lock().lock().await;
        let client_id = ManagedIdentity::generate().expect("client identity");
        let server_id = ManagedIdentity::generate().expect("server identity");
        let surge = managed_client_for(&client_id);
        let server_config = server_config_for(&server_id);
        let listener = TcpListener::bind("127.0.0.1:1700").expect("bind 1700");
        let (capture_tx, capture_rx) = mpsc::channel::<bool>();
        let server = thread::spawn(move || {
            let mut stream = accept_with_deadline(&listener);
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .expect("write timeout");
            let mut conn = rustls::ServerConnection::new(server_config).expect("server conn");
            let mut tls = rustls::Stream::new(&mut conn, &mut stream);
            // With the wrong trust anchor the handshake fails before any HTTP
            // request bytes can be read; `read_headers` returns empty/failed.
            let request = read_headers(&mut tls);
            let _ = capture_tx.send(!request.is_empty());
        });

        let result = surge.shutdown().await;
        assert!(
            result.is_err(),
            "shutdown must not succeed against an untrusted TLS identity"
        );
        let request_reached = capture_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("captured handshake result");
        server.join().expect("server thread");
        assert!(
            !request_reached,
            "no HTTP request may reach an untrusted listener"
        );
    }
}
