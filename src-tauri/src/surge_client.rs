use std::fmt;
use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
use reqwest::{redirect::Policy, Client, StatusCode};
use serde::{Deserialize, Serialize};
use url::{Host, Url};

/// Upper bound for an untrusted `/list` JSON body (1 MiB).
const MAX_BODY_BYTES: u64 = 1024 * 1024;

/// Upper bound for a user-supplied download URL (8 KiB).
///
/// The exact query string is forwarded verbatim, so a bounded cap keeps a
/// hostile or malformed URL from being relayed to the daemon.
const MAX_URL_BYTES: usize = 8 * 1024;

/// Exact duplicate-rejection message emitted by released Surge v0.12.4 when
/// headless mode refuses an already-queued download (`cmd/root_downloads.go`,
/// unchanged from v0.12.2). The 409 status alone does not prove a duplicate,
/// so the message is matched case-insensitively in addition to
/// `status == "error"`.
const DUPLICATE_MESSAGE: &str = "Download rejected: Duplicate download detected (Headless mode)";

/// Bounded round-trip for the authenticated `/list` probe.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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
        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|_| ConnectError::Network)?;
        let downloads = get_list(&client, &base, token).await?;
        Ok(Self {
            client,
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
}
