use std::fmt;
use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
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

/// Exact duplicate-rejection message emitted by released Surge v0.12.4 when
/// headless mode refuses an already-queued download (`cmd/root_downloads.go`,
/// unchanged from v0.12.2). The 409 status alone does not prove a duplicate,
/// so the message is matched case-insensitively in addition to
/// `status == "error"`.
const DUPLICATE_MESSAGE: &str = "Download rejected: Duplicate download detected (Headless mode)";

/// Bounded round-trip for the authenticated `/list` probe.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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
    let mut builder = Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .https_only(true)
        .redirect(Policy::none())
        .no_proxy()
        .pool_max_idle_per_host(0)
        .timeout(CONNECT_TIMEOUT);
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
        let base = MANAGED_BASE_URL.to_string();
        let downloads = get_list(&client, &base, token).await?;
        Ok(Self {
            client,
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
        SurgeClient {
            client,
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
                Ok((stream, _)) => return stream,
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
