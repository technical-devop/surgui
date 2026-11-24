use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use surge_desktop_lib::surge_client::{ConnectError, SurgeClient};

const ACCEPT_DEADLINE: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(2);

const LIST_BODY: &str = r#"[{"id":"f-001","url":"https://dl.example.test/alpha.bin","filename":"alpha.bin","total_size":10485760,"downloaded":4194304,"progress":40.0,"speed":2097152.0,"status":"downloading","eta":300},{"id":"f-002","url":"https://dl.example.test/beta.iso","filename":"beta.iso","total_size":20971520,"downloaded":20971520,"progress":100.0,"speed":0.0,"status":"completed","eta":0}]"#;

const UNAUTHORIZED_BODY: &str =
    r#"{"error":"unauthorized","message":"invalid or missing bearer token"}"#;

struct Captured {
    request_line: String,
    authorization: Option<String>,
}

fn spawn_server(status_line: &'static str, body: &'static str) -> (String, JoinHandle<Captured>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || serve_once(listener, status_line, body));
    (format!("http://{}", addr), server)
}

fn serve_once(listener: TcpListener, status_line: &str, body: &str) -> Captured {
    let mut stream = accept_one(&listener);
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("set read timeout");
    let captured = parse_head(&read_head(&mut stream));
    let response = format!(
        "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        status_line,
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    captured
}

fn accept_one(listener: &TcpListener) -> TcpStream {
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let deadline = Instant::now() + ACCEPT_DEADLINE;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).expect("blocking stream");
                return stream;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "no loopback connection accepted within {:?}",
                    ACCEPT_DEADLINE
                );
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("accept failed: {}", e),
        }
    }
}

fn read_head(stream: &mut TcpStream) -> String {
    let mut buf = [0u8; 4096];
    let mut head = Vec::new();
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => head.extend_from_slice(&buf[..n]),
        }
    }
    String::from_utf8_lossy(&head).to_string()
}

fn parse_head(head: &str) -> Captured {
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default().trim().to_string();
    let authorization = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.trim().to_string());
    Captured {
        request_line,
        authorization,
    }
}

#[tokio::test]
async fn connect_success_lists_downloads() {
    let (base, server) = spawn_server("200 OK", LIST_BODY);
    let outcome = SurgeClient::connect(&base, "test-token").await;
    let captured = server.join().expect("server thread panicked");
    assert!(
        captured.request_line.starts_with("GET /list "),
        "unexpected request line: {}",
        captured.request_line
    );
    assert!(
        captured.authorization.as_deref() == Some("Bearer test-token"),
        "authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    let _client = outcome.expect("connect");
}

#[tokio::test]
async fn connect_rejects_bad_token() {
    let (base, server) = spawn_server("401 Unauthorized", UNAUTHORIZED_BODY);
    let outcome = SurgeClient::connect(&base, "wrong-token").await;
    let captured = server.join().expect("server thread panicked");
    assert!(
        captured.request_line.starts_with("GET /list "),
        "unexpected request line: {}",
        captured.request_line
    );
    assert!(
        captured.authorization.as_deref() == Some("Bearer wrong-token"),
        "authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    let err = outcome.expect_err("401 must not authenticate");
    assert!(matches!(err, ConnectError::Unauthorized), "got {:?}", err);
}

#[tokio::test]
async fn connect_rejects_empty_token() {
    let outcome = SurgeClient::connect("http://127.0.0.1:9", "").await;
    let err = outcome.expect_err("empty token must be rejected");
    assert!(matches!(err, ConnectError::EmptyToken), "got {:?}", err);
}

#[tokio::test]
async fn connect_rejects_blank_token() {
    let outcome = SurgeClient::connect("http://127.0.0.1:9", "   ").await;
    let err = outcome.expect_err("blank token must be rejected");
    assert!(matches!(err, ConnectError::EmptyToken), "got {:?}", err);
}

#[tokio::test]
async fn connect_rejects_invalid_endpoints() {
    let bad = [
        "http://user:pass@127.0.0.1:9",
        "http://127.0.0.1:9/?token=x",
        "http://127.0.0.1:9/#frag",
        "http://127.0.0.1:9/api",
        "http://127.0.0.1:9/list",
        "ftp://127.0.0.1:9",
        "http://example.com",
        "http://93.184.216.34",
        "not a url",
        "",
    ];
    for endpoint in bad {
        let outcome = SurgeClient::connect(endpoint, "test-token").await;
        let err = outcome.expect_err("endpoint must be rejected");
        assert!(
            matches!(err, ConnectError::InvalidEndpoint(_)),
            "endpoint {:?} got {:?}",
            endpoint,
            err
        );
    }
}

#[tokio::test]
async fn connect_lists_downloads_parses_fields() {
    let (base, server) = spawn_server("200 OK", LIST_BODY);
    let outcome = SurgeClient::connect(&base, "test-token").await;
    let captured = server.join().expect("server thread panicked");
    assert!(
        captured.request_line.starts_with("GET /list "),
        "unexpected request line: {}",
        captured.request_line
    );
    let client = outcome.expect("connect");
    let downloads = client.list_downloads();
    assert_eq!(downloads.len(), 2, "expected two downloads");
    assert_eq!(downloads[0].id, "f-001");
    assert_eq!(downloads[0].filename, "alpha.bin");
    assert_eq!(downloads[0].total_size, 10485760);
    assert_eq!(downloads[0].downloaded, 4194304);
    assert_eq!(downloads[0].eta, 300);
    assert_eq!(downloads[1].id, "f-002");
    assert_eq!(downloads[1].status, "completed");
    assert_eq!(downloads[1].eta, 0);
    let debug = format!("{:?}", client);
    assert!(
        !debug.contains("test-token"),
        "client Debug must redact bearer token"
    );
}

#[tokio::test]
async fn connect_rejects_malformed_list() {
    let (base, server) = spawn_server("200 OK", r#"{"not":"a list"}"#);
    let outcome = SurgeClient::connect(&base, "test-token").await;
    let _ = server.join().expect("server thread panicked");
    let err = outcome.expect_err("malformed list must not authenticate");
    assert!(
        matches!(err, ConnectError::InvalidResponse),
        "got {:?}",
        err
    );
    let debug = format!("{:?}", err);
    assert!(
        !debug.contains("a list"),
        "error Debug must not echo response body"
    );
}

#[tokio::test]
async fn errors_never_reveal_token_or_body() {
    let secret = "super-secret-token-xyz";
    let (base, server) = spawn_server("401 Unauthorized", UNAUTHORIZED_BODY);
    let outcome = SurgeClient::connect(&base, secret).await;
    let err = outcome.expect_err("401 must fail");
    let _ = server.join().expect("server thread panicked");
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(
        !debug.contains(secret),
        "error Debug must redact bearer token"
    );
    assert!(
        !display.contains(secret),
        "error Display must redact bearer token"
    );
    assert!(
        !debug.contains("invalid or missing bearer token"),
        "error Debug must not echo response body"
    );
    assert!(
        !display.contains("invalid or missing bearer token"),
        "error Display must not echo response body"
    );
}

// ---------------------------------------------------------------------------
// Managed-mode TLS client foundation (pinned https://127.0.0.1:1700).
//
// All fixtures are inline bytes only: no external files, no temp files, no
// network TLS server. These tests cover the negative and pinning surface
// that does not require a TLS handshake.
// ---------------------------------------------------------------------------

#[test]
fn managed_base_url_is_pinned_https_loopback_never_http() {
    use surge_desktop_lib::surge_client::MANAGED_BASE_URL;
    assert_eq!(
        MANAGED_BASE_URL, "https://127.0.0.1:1700",
        "managed origin must stay pinned"
    );
    assert!(
        MANAGED_BASE_URL.starts_with("https://"),
        "managed origin must never fall back to http"
    );
    assert!(
        !MANAGED_BASE_URL.starts_with("http://"),
        "plain http must never be used in managed mode"
    );
    assert!(
        MANAGED_BASE_URL.contains("127.0.0.1:1700"),
        "managed origin must stay on loopback 1700"
    );
}

#[tokio::test]
async fn managed_rejects_invalid_ca_cert_without_leaking() {
    use surge_desktop_lib::surge_client::SurgeClient;
    // Fixture only: garbage bytes presented as a CA bundle.
    const BAD_CA: &[u8] = b"not a pem certificate fixture-only";
    let err = SurgeClient::connect_managed(BAD_CA, "test-token")
        .await
        .expect_err("invalid CA must be rejected");
    assert!(
        matches!(err, ConnectError::InvalidEndpoint(_)),
        "invalid CA must map to InvalidEndpoint, got {:?}",
        err
    );
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(
        !debug.contains("not a pem"),
        "error Debug must not echo CA bytes"
    );
    assert!(
        !display.contains("not a pem"),
        "error Display must not echo CA bytes"
    );
    assert!(
        !debug.contains("test-token") && !display.contains("test-token"),
        "managed error must redact bearer token"
    );
}

#[tokio::test]
async fn managed_rejects_private_key_pem_as_ca_without_leaking() {
    use surge_desktop_lib::surge_client::SurgeClient;
    // Fixture only: private-key PEM bytes passed in place of a CA bundle.
    const KEY_AS_CA: &[u8] =
        b"-----BEGIN PRIVATE KEY-----\nZmFrZS1rZXktZml4dHVyZS1vbmx5\n-----END PRIVATE KEY-----\n";
    let err = SurgeClient::connect_managed(KEY_AS_CA, "test-token")
        .await
        .expect_err("private key bytes passed as CA must be rejected");
    assert!(
        matches!(err, ConnectError::InvalidEndpoint(_)),
        "key-as-CA must map to InvalidEndpoint, got {:?}",
        err
    );
    let debug = format!("{:?}", err);
    assert!(
        !debug.contains("PRIVATE KEY") && !debug.contains("ZmFrZS"),
        "error must not echo the private-key bytes passed as CA"
    );
}

#[tokio::test]
async fn managed_rejects_empty_token_without_network() {
    use surge_desktop_lib::surge_client::SurgeClient;
    const BAD_CA: &[u8] = b"not a pem certificate fixture-only";
    for token in ["", "   "] {
        let err = SurgeClient::connect_managed(BAD_CA, token)
            .await
            .expect_err("empty token must be rejected");
        assert!(
            matches!(err, ConnectError::EmptyToken),
            "empty managed token must map to EmptyToken, got {:?}",
            err
        );
    }
    // Whitespace/control-bearing token is rejected locally as well.
    let err = SurgeClient::connect_managed(BAD_CA, "bad token")
        .await
        .expect_err("whitespace token must be rejected");
    assert!(
        matches!(err, ConnectError::InvalidEndpoint(_)),
        "whitespace token must map to InvalidEndpoint, got {:?}",
        err
    );
}

#[tokio::test]
async fn managed_malformed_ca_fails_closed_and_redacts_secrets() {
    use surge_desktop_lib::surge_client::{SurgeClient, MANAGED_BASE_URL};
    // The managed constructor takes no endpoint argument: with malformed CA
    // bytes it fails closed with a sanitized InvalidEndpoint error instead of
    // returning a usable client, and the error never echoes the CA bytes or
    // the bearer token.
    assert_eq!(MANAGED_BASE_URL, "https://127.0.0.1:1700");
    const SECRET: &str = "managed-secret-token-fixture";
    const BAD_CA: &[u8] = b"managed-fixture-bad-ca-bytes";
    let err = SurgeClient::connect_managed(BAD_CA, SECRET)
        .await
        .expect_err("managed connect with bad CA must fail");
    assert!(
        matches!(err, ConnectError::InvalidEndpoint(_)),
        "must fail closed, got {:?}",
        err
    );
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    for leaked in [SECRET, "managed-fixture-bad-ca-bytes"] {
        assert!(
            !debug.contains(leaked),
            "managed error Debug must redact secrets"
        );
        assert!(
            !display.contains(leaked),
            "managed error Display must redact secrets"
        );
    }
}

/// Upstream v0.12.4 regression: `/pause` acknowledges `pause_requested` and
/// `/resume` acknowledges `resume_requested` (`cmd/http_api.go` at
/// v0.12.4; v0.12.2 used `paused`/`resumed`). The client must accept the
/// canonical v0.12.4 strings.
#[tokio::test]
async fn control_download_accepts_v12_4_requested_acks() {
    use surge_desktop_lib::surge_client::ControlAction;

    const FIXTURE_ID: &str = "v124-fixture";
    for (action, status) in [
        (ControlAction::Pause, "pause_requested"),
        (ControlAction::Resume, "resume_requested"),
    ] {
        let control_body = format!(r#"{{"status":"{status}","id":"{FIXTURE_ID}"}}"#);
        let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), control_body);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        client
            .control_download(action, FIXTURE_ID)
            .await
            .expect("v0.12.4 requested ack must succeed");
        let _ = server.join().expect("server thread panicked");
    }
}

/// Backward compatibility: the legacy v0.12.2 `paused`/`resumed`
/// acknowledgments remain accepted so a mixed-version fleet does not break
/// during the v0.12.4 upgrade window.
#[tokio::test]
async fn control_download_accepts_legacy_v12_2_acks() {
    use surge_desktop_lib::surge_client::ControlAction;

    const FIXTURE_ID: &str = "v122-fixture";
    for (action, status) in [
        (ControlAction::Pause, "paused"),
        (ControlAction::Resume, "resumed"),
    ] {
        let control_body = format!(r#"{{"status":"{status}","id":"{FIXTURE_ID}"}}"#);
        let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), control_body);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        client
            .control_download(action, FIXTURE_ID)
            .await
            .expect("legacy v0.12.2 ack must still succeed");
        let _ = server.join().expect("server thread panicked");
    }
}

/// A mismatched acknowledgment must not surface as success: the legacy
/// `paused` string on a resume action (and vice versa) is rejected without
/// leaking the response body.
#[tokio::test]
async fn control_download_rejects_mismatched_ack_without_leaking_body() {
    use surge_desktop_lib::surge_client::ControlAction;

    const SENTINEL_ID: &str = "mismatch-fixture";
    for (action, status) in [
        (ControlAction::Pause, "resume_requested"),
        (ControlAction::Resume, "pause_requested"),
        (ControlAction::Pause, "paused-extra"),
        (ControlAction::RemoveRecord, "pause_requested"),
    ] {
        let control_body = format!(r#"{{"status":"{status}","id":"{SENTINEL_ID}"}}"#);
        let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), control_body);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let err = client
            .control_download(action, SENTINEL_ID)
            .await
            .expect_err("mismatched ack must fail");
        let _ = server.join().expect("server thread panicked");
        assert!(
            matches!(err, ConnectError::InvalidResponse),
            "mismatched ack must map to InvalidResponse, got {:?}",
            err
        );
        let debug = format!("{:?}", err);
        let display = format!("{}", err);
        assert!(
            !debug.contains(status),
            "error Debug must not echo ack status"
        );
        assert!(
            !display.contains(status),
            "error Display must not echo ack status"
        );
    }
}

/// Full Surge v0.12.4 `DownloadStatus` shape: signed eta (-1 = unknown),
/// all carried fields, optional dest_path/rate_limit set, `error` omitted,
/// plus unknown future fields that must be ignored.
const FULL_SCHEMA_BODY: &str = r#"[{"id":"f-003","url":"https://dl.example.test/gamma.bin?sig=signed-secret-abc","filename":"gamma.bin","dest_path":"/downloads/gamma.bin","total_size":5242880,"downloaded":1048576,"progress":20.0,"speed":1048576.0,"status":"downloading","eta":-1,"connections":4,"added_at":1750000000,"time_taken":0,"avg_speed":0.0,"rate_limit":1048576,"rate_limit_set":true,"some_future_field":"future-value","another_future":123}]"#;

const SECRET_DEBUG_BODY: &str = r#"[{"id":"f-003","url":"https://dl.example.test/gamma.bin?sig=signed-secret-abc","filename":"gamma-private.bin","dest_path":"/private/downloads/gamma-private.bin","total_size":5242880,"downloaded":1048576,"progress":20.0,"speed":1048576.0,"status":"downloading","eta":-1,"connections":4,"added_at":1750000000,"time_taken":0,"avg_speed":0.0,"rate_limit":1048576,"rate_limit_set":true,"error":"disk quota exceeded on private volume"}]"#;

#[tokio::test]
async fn connect_parses_full_v12_schema_with_unknown_fields() {
    let parsed: Vec<surge_desktop_lib::surge_client::Download> =
        serde_json::from_str(FULL_SCHEMA_BODY).expect("full schema must parse");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].eta, -1);
    assert_eq!(parsed[0].connections, 4);
    assert_eq!(parsed[0].added_at, 1750000000);
    assert_eq!(parsed[0].dest_path.as_deref(), Some("/downloads/gamma.bin"));
    assert_eq!(parsed[0].rate_limit, Some(1048576));
    assert_eq!(parsed[0].rate_limit_set, Some(true));
    assert!(parsed[0].error.is_none());

    let (base, server) = spawn_server("200 OK", FULL_SCHEMA_BODY);
    let outcome = SurgeClient::connect(&base, "test-token").await;
    let _ = server.join().expect("server thread panicked");
    let client = outcome.expect("full-schema /list must connect");
    assert_eq!(client.list_downloads().len(), 1);
    assert_eq!(client.list_downloads()[0].eta, -1);
}

fn spawn_chunked_server(
    status_line: &'static str,
    total_bytes: usize,
) -> (String, JoinHandle<Captured>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut stream = accept_one(&listener);
        stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let captured = parse_head(&read_head(&mut stream));
        let head = format!(
            "HTTP/1.1 {}\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
            status_line
        );
        let _ = stream.write_all(head.as_bytes());
        // No Content-Length: stream 64 KiB chunks of filler until total_bytes.
        let chunk = vec![b'a'; 65536];
        let mut remaining = total_bytes;
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            let _ = stream.write_all(format!("{:X}\r\n", n).as_bytes());
            let _ = stream.write_all(&chunk[..n]);
            let _ = stream.write_all(b"\r\n");
            remaining -= n;
        }
        let _ = stream.write_all(b"0\r\n\r\n");
        let _ = stream.flush();
        captured
    });
    (format!("http://{}", addr), server)
}

/// First request serves a valid `/list`; second serves a chunked oversized
/// body. Used to exercise `refresh` without a second server.
fn spawn_two_step_server(
    valid_body: &'static str,
    oversized_bytes: usize,
) -> (String, JoinHandle<(Captured, Captured)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            valid_body.len(),
            valid_body
        );
        let _ = first_stream.write_all(response.as_bytes());
        let _ = first_stream.flush();
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = parse_head(&read_head(&mut second_stream));
        let head = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
        let _ = second_stream.write_all(head.as_bytes());
        let chunk = vec![b'a'; 65536];
        let mut remaining = oversized_bytes;
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            let _ = second_stream.write_all(format!("{:X}\r\n", n).as_bytes());
            let _ = second_stream.write_all(&chunk[..n]);
            let _ = second_stream.write_all(b"\r\n");
            remaining -= n;
        }
        let _ = second_stream.write_all(b"0\r\n\r\n");
        let _ = second_stream.flush();
        (first, second)
    });
    (format!("http://{}", addr), server)
}

#[tokio::test]
async fn connect_rejects_chunked_oversized_body() {
    let (base, server) = spawn_chunked_server("200 OK", 1024 * 1024 + 1);
    let outcome = SurgeClient::connect(&base, "test-token").await;
    let _ = server.join().expect("server thread panicked");
    let err = outcome.expect_err("chunked oversized body must be rejected");
    assert!(matches!(err, ConnectError::BodyTooLarge), "got {:?}", err);
}

#[tokio::test]
async fn refresh_rejects_chunked_oversized_body() {
    let (base, server) = spawn_two_step_server(LIST_BODY, 1024 * 1024 + 1);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    assert_eq!(client.list_downloads().len(), 2);
    let err = client
        .refresh()
        .await
        .expect_err("refresh must reject oversized");
    let _ = server.join().expect("server thread panicked");
    assert!(matches!(err, ConnectError::BodyTooLarge), "got {:?}", err);
}

#[tokio::test]
async fn client_debug_hides_urls_token_and_body() {
    let (base, server) = spawn_server("200 OK", LIST_BODY);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let _ = server.join().expect("server thread panicked");
    let debug = format!("{:?}", client);
    assert!(
        debug.contains("download_count"),
        "client Debug must report download_count, got: {}",
        debug
    );
    assert!(
        !debug.contains("dl.example.test"),
        "client Debug must not print download URLs"
    );
    assert!(
        !debug.contains("alpha.bin"),
        "client Debug must not print download filenames/URLs"
    );
    assert!(
        !debug.contains("test-token"),
        "client Debug must not print bearer token"
    );
    assert!(
        !debug.contains("downloads"),
        "client Debug must not dump downloads vec"
    );
}

fn spawn_sequential_server(
    first_body: &'static str,
    second_body: &'static str,
) -> (String, JoinHandle<(Captured, Captured)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            first_body.len(),
            first_body
        );
        let _ = first_stream.write_all(response.as_bytes());
        let _ = first_stream.flush();
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = parse_head(&read_head(&mut second_stream));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            second_body.len(),
            second_body
        );
        let _ = second_stream.write_all(response.as_bytes());
        let _ = second_stream.flush();
        (first, second)
    });
    (format!("http://{}", addr), server)
}

#[tokio::test]
async fn connect_accepts_null_list_as_empty() {
    // Empty fresh daemon: uninitialized Go slice encodes as JSON `null`.
    let (base, server) = spawn_server("200 OK", "null");
    let outcome = SurgeClient::connect(&base, "test-token").await;
    let captured = server.join().expect("server thread panicked");
    assert!(
        captured.request_line.starts_with("GET /list "),
        "unexpected request line: {}",
        captured.request_line
    );
    let client = outcome.expect("null /list must connect with zero downloads");
    assert_eq!(client.list_downloads().len(), 0);
}

#[tokio::test]
async fn refresh_accepts_null_list_as_empty() {
    let (base, server) = spawn_sequential_server(LIST_BODY, "null");
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    assert_eq!(client.list_downloads().len(), 2);
    let refreshed = client.refresh().await.expect("null refresh must succeed");
    let _ = server.join().expect("server thread panicked");
    assert_eq!(refreshed.len(), 0, "null refresh must return empty vec");
}

#[tokio::test]
async fn connect_accepts_empty_array_list() {
    let (base, server) = spawn_server("200 OK", "[]");
    let outcome = SurgeClient::connect(&base, "test-token").await;
    let _ = server.join().expect("server thread panicked");
    let client = outcome.expect("[] /list must connect with zero downloads");
    assert_eq!(client.list_downloads().len(), 0);
}

#[tokio::test]
async fn download_debug_redacts_signed_url_dest_path_and_error() {
    let parsed: Vec<surge_desktop_lib::surge_client::Download> =
        serde_json::from_str(SECRET_DEBUG_BODY).expect("full schema with secrets must parse");
    assert_eq!(parsed.len(), 1);
    let debug = format!("{:?}", parsed[0]);
    assert!(
        !debug.contains("signed-secret-abc"),
        "Download Debug must redact signed URL secret"
    );
    assert!(
        !debug.contains("dl.example.test"),
        "Download Debug must not print download URL"
    );
    assert!(
        !debug.contains("/private/downloads/gamma-private.bin"),
        "Download Debug must redact dest_path"
    );
    assert!(
        !debug.contains("gamma-private.bin"),
        "Download Debug must not print filename"
    );
    assert!(
        !debug.contains("disk quota exceeded"),
        "Download Debug must redact error"
    );
    assert!(
        !debug.contains("f-003"),
        "Download Debug must not print server-controlled id"
    );
    assert!(
        !debug.contains("downloading"),
        "Download Debug must not print server-controlled status"
    );
}

#[tokio::test]
async fn connect_trims_trailing_space_from_endpoint() {
    let (base, server) = spawn_server("200 OK", LIST_BODY);
    let spaced = format!("{} ", base);
    let outcome = SurgeClient::connect(&spaced, "test-token").await;
    let captured = server.join();
    let client = outcome.expect("trailing-space endpoint must connect");
    let captured = captured.expect("server thread panicked");
    assert!(
        captured.request_line.starts_with("GET /list "),
        "unexpected request line: {}",
        captured.request_line
    );
    assert_eq!(client.base_url(), base);
}

#[tokio::test]
async fn connection_manager_connect_maps_queue_and_redacts_secrets() {
    let (base, server) = spawn_server(
        "200 OK",
        r#"[{"id":"x-1","url":"https://dl.example.test/file.iso?sig=signed-secret","filename":"file.iso","dest_path":"/downloads/private-path/file.iso","total_size":1000,"downloaded":250,"progress":25.0,"speed":50.0,"status":"downloading","eta":15,"added_at":42,"error":"private-error disk full"}]"#,
    );
    let items: Vec<surge_desktop_lib::commands::QueueItemDto> =
        surge_desktop_lib::commands::ConnectionManager::default()
            .connect(&base, "fixture-token")
            .await
            .expect("connect through manager");
    let captured = server.join().expect("server thread panicked");
    assert!(
        captured.authorization.as_deref() == Some("Bearer fixture-token"),
        "authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(items.len() == 1, "expected exactly one queue item");
    assert!(items[0].name == "file.iso", "queue item name mismatch");
    assert!(items[0].status == "active", "queue item status mismatch");
    assert!(items[0].bytes_done == 250, "queue item bytes_done mismatch");
    assert!(
        items[0].bytes_total == 1000,
        "queue item bytes_total mismatch"
    );
    assert!(items[0].speed_bps == 50.0, "queue item speed_bps mismatch");
    assert!(items[0].added_at == 42, "queue item added_at mismatch");
    let serialized = serde_json::to_string(&items).expect("queue items must serialize");
    assert!(
        serialized.contains("\"bytesDone\""),
        "queue payload must expose bytesDone"
    );
    assert!(
        serialized.contains("\"speedBps\""),
        "queue payload must expose speedBps"
    );
    assert!(
        !serialized.contains("signed-secret"),
        "queue payload must redact signed URL secret"
    );
    assert!(
        !serialized.contains("private-path"),
        "queue payload must redact dest_path"
    );
    assert!(
        !serialized.contains("private-error"),
        "queue payload must redact error"
    );
    assert!(
        !serialized.contains("fixture-token"),
        "queue payload must redact bearer token"
    );
    assert!(
        !serialized.contains(base.as_str()),
        "queue payload must not leak endpoint"
    );
}

#[tokio::test]
async fn connection_manager_list_downloads_requeries_daemon() {
    let (base, server) = spawn_sequential_server(LIST_BODY, "[]");
    let manager = surge_desktop_lib::commands::ConnectionManager::default();
    let initial = manager.connect(&base, "test-token").await.expect("connect");
    assert_eq!(
        initial.len(),
        2,
        "expected two downloads from initial connect"
    );
    let second = manager
        .list_downloads()
        .await
        .expect("list_downloads must requery daemon");
    let (first, second_req) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "unexpected first request line: {}",
        first.request_line
    );
    assert!(
        second_req.request_line.starts_with("GET /list "),
        "unexpected second request line: {}",
        second_req.request_line
    );
    assert_eq!(
        second.len(),
        0,
        "list_downloads must return fresh empty list"
    );
}

/// Sequential loopback helper for released v0.12.4 control routes.
/// Serves `GET /list` then one authenticated control `POST`.
/// Owns both `String` bodies; each accept is bounded by `accept_one`'s
/// existing 5s deadline.
fn spawn_connect_then_control(
    list_body: String,
    control_body: String,
) -> (String, JoinHandle<(Captured, Captured)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            list_body.len(),
            list_body
        );
        let _ = first_stream.write_all(response.as_bytes());
        let _ = first_stream.flush();
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = parse_head(&read_head(&mut second_stream));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            control_body.len(),
            control_body
        );
        let _ = second_stream.write_all(response.as_bytes());
        let _ = second_stream.flush();
        (first, second)
    });
    (format!("http://{}", addr), server)
}

#[tokio::test]
async fn control_download_posts_exact_authenticated_path() {
    use surge_desktop_lib::surge_client::ControlAction;

    const FIXTURE_ID: &str = "file 1 & 2";
    let routes = [
        ("/pause", "pause_requested"),
        ("/resume", "resume_requested"),
        ("/delete", "deleted"),
    ];
    for (index, (path, status)) in routes.iter().enumerate() {
        let action = match index {
            0 => ControlAction::Pause,
            1 => ControlAction::Resume,
            _ => ControlAction::RemoveRecord,
        };
        let control_body = format!(r#"{{"status":"{status}","id":"{FIXTURE_ID}"}}"#);
        let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), control_body);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        client
            .control_download(action, FIXTURE_ID)
            .await
            .expect("control must succeed");
        let (first, second) = server.join().expect("server thread panicked");
        assert!(
            first.request_line.starts_with("GET /list "),
            "first request must be an authenticated GET /list"
        );
        assert!(
            first.authorization.as_deref() == Some("Bearer test-token"),
            "first authorization header did not exactly match the expected Bearer credential (captured value withheld)"
        );
        assert!(
            second.request_line.starts_with("POST "),
            "control request must use POST"
        );
        let target = second
            .request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_string();
        assert!(
            !target.is_empty(),
            "control request must include an origin-form target"
        );
        assert!(
            !target.contains("/purge"),
            "remove-record must use exact delete path, never purge"
        );
        assert!(
            !target.contains(FIXTURE_ID),
            "id query must be percent-encoded"
        );
        let parsed = url::Url::parse(&format!("http://127.0.0.1{target}"))
            .expect("control target must parse as URL");
        assert!(
            parsed.path() == *path,
            "control request used an unexpected path"
        );
        let id = parsed
            .query_pairs()
            .find(|(key, _)| key == "id")
            .map(|(_, value)| value.into_owned());
        assert!(
            id.as_deref() == Some(FIXTURE_ID),
            "control id query must round-trip the original id"
        );
        assert!(
            second.authorization.as_deref() == Some("Bearer test-token"),
            "control authorization header did not exactly match the expected Bearer credential (captured value withheld)"
        );
    }
}

#[tokio::test]
async fn control_download_rejects_unauthorized_without_leaking_body() {
    use surge_desktop_lib::surge_client::ControlAction;

    const SENTINEL: &str = "control-sentinel-7f3a9c-raw-detail";
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            LIST_BODY.len(),
            LIST_BODY
        );
        let _ = first_stream.write_all(response.as_bytes());
        let _ = first_stream.flush();
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = parse_head(&read_head(&mut second_stream));
        let body = format!(r#"{{"error":"unauthorized","message":"{SENTINEL}"}}"#);
        let response = format!(
            "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = second_stream.write_all(response.as_bytes());
        let _ = second_stream.flush();
        (first, second)
    });
    let base = format!("http://{}", addr);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .control_download(ControlAction::Pause, "file 1 & 2")
        .await
        .expect_err("control 401 must fail");
    let (first, second) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "first request must be an authenticated GET /list"
    );
    assert!(
        second.request_line.starts_with("POST /pause?id="),
        "control request must target the pause path with an encoded id query"
    );
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "control authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        matches!(err, ConnectError::Unauthorized),
        "control 401 must map to Unauthorized"
    );
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(
        !debug.contains(SENTINEL),
        "error Debug must not echo response body"
    );
    assert!(
        !display.contains(SENTINEL),
        "error Display must not echo response body"
    );
}

#[tokio::test]
async fn connection_manager_control_download_forwards_authenticated_pause() {
    use surge_desktop_lib::surge_client::ControlAction;

    const FIXTURE_ID: &str = "file 1 & 2";
    let control_body = format!(r#"{{"status":"pause_requested","id":"{FIXTURE_ID}"}}"#);
    let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), control_body);
    let manager = surge_desktop_lib::commands::ConnectionManager::default();
    manager.connect(&base, "test-token").await.expect("connect");
    manager
        .control_download(ControlAction::Pause, FIXTURE_ID)
        .await
        .expect("control must succeed");
    let (first, second) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "first request must be an authenticated GET /list"
    );
    assert!(
        first.authorization.as_deref() == Some("Bearer test-token"),
        "first authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        second.request_line.starts_with("POST "),
        "control request must use POST"
    );
    let target = second
        .request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    assert!(
        !target.is_empty(),
        "control request must include an origin-form target"
    );
    let parsed = url::Url::parse(&format!("http://127.0.0.1{target}"))
        .expect("control target must parse as URL");
    assert!(
        parsed.path() == "/pause",
        "control request used an unexpected path"
    );
    let id = parsed
        .query_pairs()
        .find(|(key, _)| key == "id")
        .map(|(_, value)| value.into_owned());
    assert!(
        id.as_deref() == Some(FIXTURE_ID),
        "control id query must round-trip the original id"
    );
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "control authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
}

/// Captured authenticated `POST /download` request with its JSON body.
struct CapturedDownload {
    request_line: String,
    authorization: Option<String>,
    body: String,
}

/// Read one HTTP request head plus its `Content-Length` body.
///
/// `read_head` alone is insufficient for a POST: a single `read` may overread
/// past `\r\n\r\n` into the body, so any bytes already buffered after the
/// header terminator are retained and only the remaining `Content-Length`
/// bytes are read. Bounded by the existing `READ_TIMEOUT`.
fn read_request_with_body(stream: &mut TcpStream) -> CapturedDownload {
    let mut buf = [0u8; 4096];
    let mut raw = Vec::new();
    while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
        }
    }
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| pos + 4)
        .unwrap_or(raw.len());
    let head_str = String::from_utf8_lossy(&raw[..head_end]).to_string();
    let captured = parse_head(&head_str);
    let content_length: usize = head_str
        .split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0);
    let mut body = raw[head_end..].to_vec();
    while body.len() < content_length {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&buf[..n]),
        }
    }
    body.truncate(content_length);
    CapturedDownload {
        request_line: captured.request_line,
        authorization: captured.authorization,
        body: String::from_utf8_lossy(&body).to_string(),
    }
}

/// Sequential loopback helper for released Surge v0.12.4 `POST /download`.
/// Serves `GET /list` then one authenticated `POST /download`.
/// Each accept is bounded by `accept_one`'s existing 5s deadline.
fn spawn_connect_then_download(
    list_body: String,
    download_body: String,
) -> (String, JoinHandle<(Captured, CapturedDownload)>) {
    spawn_connect_then_download_with_status(list_body, "200 OK", download_body)
}

/// Status-parameterized `POST /download` loopback helper.
///
/// `spawn_connect_then_download` preserves the released `200 OK` default;
/// contract tests for `202`/`409` outcomes pass their status explicitly.
fn spawn_connect_then_download_with_status(
    list_body: String,
    download_status: &'static str,
    download_body: String,
) -> (String, JoinHandle<(Captured, CapturedDownload)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            list_body.len(),
            list_body
        );
        let _ = first_stream.write_all(response.as_bytes());
        let _ = first_stream.flush();
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = read_request_with_body(&mut second_stream);
        let response = format!(
            "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            download_status,
            download_body.len(),
            download_body
        );
        let _ = second_stream.write_all(response.as_bytes());
        let _ = second_stream.flush();
        (first, second)
    });
    (format!("http://{}", addr), server)
}

/// Sequential loopback helper for the pinned `PATCH /settings` route.
/// Serves `GET /list`, then one body-captured settings patch.
fn spawn_connect_then_settings_patch(
    patch_status: &'static str,
    patch_body: String,
) -> (String, JoinHandle<(Captured, CapturedDownload)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        write_json_response(&mut first_stream, "200 OK", LIST_BODY);
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = read_request_with_body(&mut second_stream);
        write_json_response(&mut second_stream, patch_status, &patch_body);
        (first, second)
    });
    (format!("http://{}", addr), server)
}

/// Sequential loopback helper that accepts the settings PATCH and closes the
/// connection without a response, simulating an ambiguous network failure.
fn spawn_connect_then_settings_patch_without_response(
) -> (String, JoinHandle<(Captured, CapturedDownload)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        write_json_response(&mut first_stream, "200 OK", LIST_BODY);
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = read_request_with_body(&mut second_stream);
        drop(second_stream);
        (first, second)
    });
    (format!("http://{}", addr), server)
}

fn settings_direct_max_concurrent(value: &str) -> String {
    format!(r#"{{"network":{{"max_concurrent_downloads":{value}}}}}"#)
}

#[tokio::test]
async fn get_max_concurrent_downloads_reads_authenticated_settings() {
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", settings_direct_max_concurrent("7")),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let value = client
        .get_max_concurrent_downloads()
        .await
        .expect("max concurrent readback must succeed");
    assert_eq!(value, 7);
    let captured = server.join().expect("server thread panicked");
    assert!(captured[0].request_line.starts_with("GET /list "));
    assert!(captured[1].request_line.starts_with("GET /settings "));
    assert!(
        captured[1].authorization.as_deref() == Some("Bearer test-token"),
        "settings read must use the bearer token (value withheld)"
    );
}

#[tokio::test]
async fn get_max_concurrent_downloads_accepts_contract_boundaries() {
    for value in ["1", "3", "10"] {
        let (base, server) = spawn_scripted_server(vec![
            ("200 OK", LIST_BODY.to_string()),
            ("200 OK", settings_direct_max_concurrent(value)),
        ]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let parsed = client
            .get_max_concurrent_downloads()
            .await
            .expect("boundary value must parse");
        assert_eq!(parsed, value.parse::<i64>().expect("fixture integer"));
        let _ = server.join().expect("server thread panicked");
    }
}

#[tokio::test]
async fn get_max_concurrent_downloads_rejects_out_of_range_values() {
    for value in ["0", "11", "-1", "2.5", "\"3\"", "null"] {
        let (base, server) = spawn_scripted_server(vec![
            ("200 OK", LIST_BODY.to_string()),
            ("200 OK", settings_direct_max_concurrent(value)),
        ]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        assert!(
            client.get_max_concurrent_downloads().await.is_err(),
            "out-of-contract value {value} must be rejected"
        );
        let _ = server.join().expect("server thread panicked");
    }
}

#[tokio::test]
async fn set_max_concurrent_downloads_posts_conflict_safe_patch() {
    let (base, server) =
        spawn_connect_then_settings_patch("200 OK", settings_direct_max_concurrent("5"));
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let confirmed = client
        .set_max_concurrent_downloads(3, 5)
        .await
        .expect("settings patch must succeed");
    assert_eq!(confirmed, 5);
    let (first, second) = server.join().expect("server thread panicked");
    assert!(first.request_line.starts_with("GET /list "));
    assert!(
        second.request_line.starts_with("PATCH /settings "),
        "settings write must use the conflict-safe PATCH route"
    );
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "settings write authorization header did not match (value withheld)"
    );
    let body: serde_json::Value =
        serde_json::from_str(&second.body).expect("settings patch body must be JSON");
    let changes = body
        .get("changes")
        .and_then(|value| value.as_array())
        .expect("settings patch must carry a changes array");
    assert_eq!(changes.len(), 1);
    let change = &changes[0];
    assert_eq!(
        change.get("category").and_then(|value| value.as_str()),
        Some("Network")
    );
    assert_eq!(
        change.get("key").and_then(|value| value.as_str()),
        Some("max_concurrent_downloads")
    );
    assert_eq!(
        change.get("before").and_then(|value| value.as_i64()),
        Some(3)
    );
    assert_eq!(
        change.get("value").and_then(|value| value.as_i64()),
        Some(5)
    );
    assert!(
        body.get("network").is_none(),
        "the patch must not overwrite unrelated settings"
    );
}

#[tokio::test]
async fn set_max_concurrent_downloads_maps_conflict_definitively() {
    let (base, server) = spawn_connect_then_settings_patch("409 Conflict", "{}".to_string());
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .set_max_concurrent_downloads(3, 5)
        .await
        .expect_err("conflict must fail");
    assert!(matches!(err, ConnectError::SettingsConflict));
    let (_, second) = server.join().expect("server thread panicked");
    assert!(second.request_line.starts_with("PATCH /settings "));
}

#[tokio::test]
async fn set_max_concurrent_downloads_treats_500_and_bad_200_as_unconfirmed() {
    for (status, body) in [
        ("500 Internal Server Error", "{}".to_string()),
        ("200 OK", "not-json".to_string()),
        ("200 OK", settings_direct_max_concurrent("4")),
    ] {
        let (base, server) = spawn_connect_then_settings_patch(status, body);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let err = client
            .set_max_concurrent_downloads(3, 5)
            .await
            .expect_err("ambiguous outcome must fail closed");
        assert!(
            matches!(err, ConnectError::SettingsUnconfirmed),
            "status {status} must be unconfirmed, got {:?}",
            err
        );
        let _ = server.join().expect("server thread panicked");
    }
}

#[tokio::test]
async fn set_max_concurrent_downloads_rejects_out_of_range_without_network() {
    for (before, value) in [(0, 5), (3, 11), (11, 1)] {
        let (base, server) = spawn_server("200 OK", LIST_BODY);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        assert!(
            matches!(
                client.set_max_concurrent_downloads(before, value).await,
                Err(ConnectError::InvalidResponse)
            ),
            "out-of-range values must be rejected locally"
        );
        let captured = server.join().expect("server thread panicked");
        assert!(
            captured.request_line.starts_with("GET /list "),
            "invalid values must not send a settings request"
        );
    }
}

#[tokio::test]
async fn set_max_concurrent_downloads_same_value_confirms_unchanged_daemon() {
    let (base, server) =
        spawn_connect_then_settings_patch("200 OK", settings_direct_max_concurrent("3"));
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let confirmed = client
        .set_max_concurrent_downloads(3, 3)
        .await
        .expect("unchanged same-value write must be confirmed");
    assert_eq!(confirmed, 3);
    let (_, second) = server.join().expect("server thread panicked");
    assert!(
        second.request_line.starts_with("PATCH /settings "),
        "same-value writes must still check the daemon via PATCH"
    );
    let body: serde_json::Value =
        serde_json::from_str(&second.body).expect("settings patch body must be JSON");
    let change = &body["changes"][0];
    assert_eq!(change["before"].as_i64(), Some(3));
    assert_eq!(change["value"].as_i64(), Some(3));
}

#[tokio::test]
async fn set_max_concurrent_downloads_same_value_rejects_external_change() {
    let (base, server) = spawn_connect_then_settings_patch("409 Conflict", "{}".to_string());
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .set_max_concurrent_downloads(3, 3)
        .await
        .expect_err("an externally changed daemon value must conflict");
    assert!(matches!(err, ConnectError::SettingsConflict));
    let (_, second) = server.join().expect("server thread panicked");
    assert!(
        second.request_line.starts_with("PATCH /settings "),
        "same-value writes must not bypass the daemon conflict check"
    );
    let body: serde_json::Value =
        serde_json::from_str(&second.body).expect("settings patch body must be JSON");
    let change = &body["changes"][0];
    assert_eq!(change["before"].as_i64(), Some(3));
    assert_eq!(change["value"].as_i64(), Some(3));
}

#[tokio::test]
async fn set_max_concurrent_downloads_same_value_treats_network_failure_as_unconfirmed() {
    let (base, server) = spawn_connect_then_settings_patch_without_response();
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .set_max_concurrent_downloads(3, 3)
        .await
        .expect_err("a dropped same-value write must fail closed");
    assert!(matches!(err, ConnectError::SettingsUnconfirmed));
    let (_, second) = server.join().expect("server thread panicked");
    assert!(
        second.request_line.starts_with("PATCH /settings "),
        "same-value writes must still reach the daemon"
    );
}

#[tokio::test]
async fn connection_manager_reads_manual_max_concurrent_but_refuses_manual_write() {
    use surge_desktop_lib::commands::{BridgeError, ConnectionManager};

    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", settings_direct_max_concurrent("4")),
    ]);
    let manager = ConnectionManager::default();
    manager.connect(&base, "test-token").await.expect("connect");
    assert_eq!(
        manager
            .get_max_concurrent_downloads()
            .await
            .expect("manager readback"),
        4
    );
    assert!(
        matches!(
            manager.set_max_concurrent_downloads(3, 5).await,
            Err(BridgeError::ManagedConnectionRequired)
        ),
        "manual connections cannot be written"
    );
    let captured = server.join().expect("server thread panicked");
    assert_eq!(
        captured.len(),
        2,
        "refused manual write must not reach the daemon"
    );
    assert!(captured[1].request_line.starts_with("GET /settings "));
}

#[tokio::test]
async fn add_download_posts_exact_authenticated_json() {
    use surge_desktop_lib::surge_client::AddOutcome;

    const FIXTURE_URL: &str = "https://dl.example.test/new.bin?sig=benign-fixture-abc123";
    let (base, server) = spawn_connect_then_download(
        LIST_BODY.to_string(),
        r#"{"status":"queued","id":"q-001","filename":"new.bin"}"#.to_string(),
    );
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let outcome = client
        .add_download(FIXTURE_URL)
        .await
        .expect("add_download must succeed");
    let (first, second) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "first request must be an authenticated GET /list"
    );
    assert!(
        first.authorization.as_deref() == Some("Bearer test-token"),
        "first authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        second.request_line.starts_with("POST /download "),
        "add request must use exact origin-form POST path"
    );
    let target = second
        .request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    assert!(target == "/download", "add request used an unexpected path");
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "add authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    let body: serde_json::Value =
        serde_json::from_str(&second.body).expect("add request body must be JSON");
    assert!(
        body.get("url").and_then(|v| v.as_str()) == Some(FIXTURE_URL),
        "add request body must carry the URL verbatim (value withheld)"
    );
    assert!(
        body.get("skip_approval") == Some(&serde_json::Value::Bool(false)),
        "add request body must preserve Surge approval checks (false)"
    );
    assert!(
        body.get("path").is_none(),
        "add request body must omit the destination path"
    );
    assert!(
        body.get("headers").is_none(),
        "add request body must omit headers"
    );
    assert!(
        body.get("credentials").is_none(),
        "add request body must omit credentials"
    );
    assert!(
        matches!(outcome, AddOutcome::Queued),
        "add_download must report a queued outcome"
    );
}

#[tokio::test]
async fn connection_manager_add_download_forwards_authenticated_json() {
    use surge_desktop_lib::surge_client::AddOutcome;

    const FIXTURE_URL: &str = "https://dl.example.test/new.bin?sig=benign-fixture-abc123";
    let (base, server) = spawn_connect_then_download(
        LIST_BODY.to_string(),
        r#"{"status":"queued","id":"q-001","filename":"new.bin"}"#.to_string(),
    );
    let manager = surge_desktop_lib::commands::ConnectionManager::default();
    manager.connect(&base, "test-token").await.expect("connect");
    let outcome = manager
        .add_download(FIXTURE_URL)
        .await
        .expect("add_download must succeed through the manager");
    let (first, second) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "first request must be an authenticated GET /list"
    );
    assert!(
        first.authorization.as_deref() == Some("Bearer test-token"),
        "first authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        second.request_line.starts_with("POST /download "),
        "add request must use exact origin-form POST path"
    );
    let target = second
        .request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    assert!(target == "/download", "add request used an unexpected path");
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "add authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    let body: serde_json::Value =
        serde_json::from_str(&second.body).expect("add request body must be JSON");
    assert!(
        body.get("url").and_then(|v| v.as_str()) == Some(FIXTURE_URL),
        "add request body must carry the URL verbatim (value withheld)"
    );
    assert!(
        body.get("skip_approval") == Some(&serde_json::Value::Bool(false)),
        "add request body must preserve Surge approval checks (false)"
    );
    assert!(
        body.get("path").is_none(),
        "add request body must omit the destination path"
    );
    assert!(
        body.get("headers").is_none(),
        "add request body must omit headers"
    );
    assert!(
        body.get("credentials").is_none(),
        "add request body must omit credentials"
    );
    assert!(
        matches!(outcome, AddOutcome::Queued),
        "add_download must report a queued outcome"
    );
    let serialized = serde_json::to_string(&outcome).expect("outcome must serialize");
    assert!(
        serialized == "\"queued\"",
        "serialized outcome variant must be queued"
    );
}

#[tokio::test]
async fn add_download_maps_pending_approval() {
    use surge_desktop_lib::surge_client::AddOutcome;

    const FIXTURE_URL: &str = "https://dl.example.test/new.bin?sig=benign-fixture-abc123";
    let (base, server) = spawn_connect_then_download_with_status(
        LIST_BODY.to_string(),
        "202 Accepted",
        r#"{"status":"pending_approval","id":"approval-fixture"}"#.to_string(),
    );
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let outcome = client
        .add_download(FIXTURE_URL)
        .await
        .expect("add_download must succeed");
    let (first, second) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "first request must be an authenticated GET /list"
    );
    assert!(
        first.authorization.as_deref() == Some("Bearer test-token"),
        "first authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        second.request_line.starts_with("POST /download "),
        "add request must use exact origin-form POST path"
    );
    let target = second
        .request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    assert!(target == "/download", "add request used an unexpected path");
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "add authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    let body: serde_json::Value =
        serde_json::from_str(&second.body).expect("add request body must be JSON");
    assert!(
        body.get("url").and_then(|v| v.as_str()) == Some(FIXTURE_URL),
        "add request body must carry the URL verbatim (value withheld)"
    );
    assert!(
        matches!(outcome, AddOutcome::PendingApproval),
        "202 pending_approval must map to PendingApproval"
    );
    assert!(
        !matches!(outcome, AddOutcome::Queued),
        "202 must not report a queued claim"
    );
}

#[tokio::test]
async fn add_download_maps_duplicate() {
    use surge_desktop_lib::surge_client::AddOutcome;

    const FIXTURE_URL: &str = "https://dl.example.test/new.bin?sig=benign-fixture-abc123";
    let (base, server) = spawn_connect_then_download_with_status(
        LIST_BODY.to_string(),
        "409 Conflict",
        r#"{"status":"error","message":"Download rejected: Duplicate download detected (Headless mode)"}"#
            .to_string(),
    );
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let outcome = client
        .add_download(FIXTURE_URL)
        .await
        .expect("add_download must succeed");
    let (first, second) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "first request must be an authenticated GET /list"
    );
    assert!(
        first.authorization.as_deref() == Some("Bearer test-token"),
        "first authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        second.request_line.starts_with("POST /download "),
        "add request must use exact origin-form POST path"
    );
    let target = second
        .request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    assert!(target == "/download", "add request used an unexpected path");
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "add authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    let body: serde_json::Value =
        serde_json::from_str(&second.body).expect("add request body must be JSON");
    assert!(
        body.get("url").and_then(|v| v.as_str()) == Some(FIXTURE_URL),
        "add request body must carry the URL verbatim (value withheld)"
    );
    assert!(
        matches!(outcome, AddOutcome::Duplicate),
        "409 duplicate rejection must map to Duplicate"
    );
}

#[tokio::test]
async fn add_download_rejects_unexpected_conflict_without_leaking_body() {
    const FIXTURE_URL: &str = "https://dl.example.test/new.bin?sig=benign-fixture-abc123";
    const SENTINEL: &str = "add-sentinel-9e4b2a-private-detail";
    let conflict_body = format!(r#"{{"status":"error","message":"quota exceeded on {SENTINEL}"}}"#);
    let (base, server) = spawn_connect_then_download_with_status(
        LIST_BODY.to_string(),
        "409 Conflict",
        conflict_body,
    );
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .add_download(FIXTURE_URL)
        .await
        .expect_err("unexpected 409 body must fail");
    let (first, second) = server.join().expect("server thread panicked");
    assert!(
        first.request_line.starts_with("GET /list "),
        "first request must be an authenticated GET /list"
    );
    assert!(
        first.authorization.as_deref() == Some("Bearer test-token"),
        "first authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        second.request_line.starts_with("POST /download "),
        "add request must use exact origin-form POST path"
    );
    let target = second
        .request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    assert!(target == "/download", "add request used an unexpected path");
    assert!(
        second.authorization.as_deref() == Some("Bearer test-token"),
        "add authorization header did not exactly match the expected Bearer credential (captured value withheld)"
    );
    assert!(
        matches!(err, ConnectError::InvalidResponse),
        "unexpected 409 body must map to InvalidResponse"
    );
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(
        !debug.contains(SENTINEL),
        "error Debug must not echo response body"
    );
    assert!(
        !display.contains(SENTINEL),
        "error Display must not echo response body"
    );
}

// ---------------------------------------------------------------------------
// Global download rate limit (pinned `/settings` + `/rate-limit/global`).
// ---------------------------------------------------------------------------

fn write_json_response(stream: &mut TcpStream, status_line: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        status_line,
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn spawn_scripted_server(
    responses: Vec<(&'static str, String)>,
) -> (String, JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut captured = Vec::with_capacity(responses.len());
        for (status_line, body) in responses {
            let mut stream = accept_one(&listener);
            stream
                .set_read_timeout(Some(READ_TIMEOUT))
                .expect("set read timeout");
            captured.push(parse_head(&read_head(&mut stream)));
            write_json_response(&mut stream, status_line, &body);
        }
        captured
    });
    (format!("http://{}", addr), server)
}

/// Serve LIST, answer the POST ambiguously, then watch briefly for any readback.
/// The watcher proves the client does not trust a later matching readback.
fn spawn_ambiguous_post_server(
    status_line: &'static str,
    body: String,
    tempting_readback: String,
) -> (String, JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut captured = Vec::with_capacity(3);
        let mut first = accept_one(&listener);
        first
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        captured.push(parse_head(&read_head(&mut first)));
        write_json_response(&mut first, "200 OK", LIST_BODY);

        let mut second = accept_one(&listener);
        second
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        captured.push(parse_head(&read_head(&mut second)));
        write_json_response(&mut second, status_line, &body);

        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let deadline = Instant::now() + Duration::from_millis(300);
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).expect("blocking stream");
                    stream
                        .set_read_timeout(Some(READ_TIMEOUT))
                        .expect("set read timeout");
                    captured.push(parse_head(&read_head(&mut stream)));
                    write_json_response(&mut stream, "200 OK", &tempting_readback);
                    break;
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
        captured
    });
    (format!("http://{}", addr), server)
}

fn settings_direct_rate(rate: &str) -> String {
    format!(r#"{{"network":{{"global_rate_limit":"{rate}"}}}}"#)
}

#[tokio::test]
async fn get_global_rate_limit_reads_authenticated_settings() {
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", settings_direct_rate("1048576 B/s")),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let rate = client
        .get_global_rate_limit()
        .await
        .expect("global rate readback must succeed");
    let captured = server.join().expect("server thread panicked");
    assert_eq!(rate, 1_048_576);
    assert!(captured[0].request_line.starts_with("GET /list "));
    assert!(captured[1].request_line.starts_with("GET /settings "));
    for request in &captured {
        assert!(
            request.authorization.as_deref() == Some("Bearer test-token"),
            "every settings request must carry the expected Bearer credential (value withheld)"
        );
    }
}

#[tokio::test]
async fn get_global_rate_limit_accepts_zero_value() {
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", settings_direct_rate("0")),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let rate = client
        .get_global_rate_limit()
        .await
        .expect("zero setting must parse");
    let _ = server.join().expect("server thread panicked");
    assert_eq!(rate, 0, "zero means unlimited");
}

#[tokio::test]
async fn get_global_rate_limit_matches_pinned_null_and_float_rounding() {
    for (value, expected) in [
        ("null", 0_i64),
        ("0", 0),
        ("0.4", 0),
        ("0.5", 1),
        ("1.4", 1),
        ("1.5", 2),
        ("2.5", 3),
        ("1048576.49", 1_048_576),
        ("1048576.5", 1_048_577),
    ] {
        let body = format!(r#"{{"network":{{"global_rate_limit":{value}}}}}"#);
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let rate = client
            .get_global_rate_limit()
            .await
            .expect("pinned primitive value must parse");
        let _ = server.join().expect("server thread panicked");
        assert_eq!(rate, expected, "unexpected parsed rate for {value}");
    }
}

#[tokio::test]
async fn get_global_rate_limit_rejects_unsafe_large_float_without_false_unlimited() {
    use surge_desktop_lib::surge_client::MAX_RATE_LIMIT_BPS;

    // Close boundary: exact integer and canonical string forms at the
    // JavaScript-safe maximum must still round-trip exactly.
    for (value, expected) in [
        ("9007199254740990", 9_007_199_254_740_990_i64),
        ("9007199254740991", MAX_RATE_LIMIT_BPS),
        (r#""9007199254740991 B/s""#, MAX_RATE_LIMIT_BPS),
    ] {
        let body = format!(r#"{{"network":{{"global_rate_limit":{value}}}}}"#);
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let rate = client
            .get_global_rate_limit()
            .await
            .expect("exact boundary value must parse");
        let _ = server.join().expect("server thread panicked");
        assert_eq!(rate, expected, "unexpected parsed rate for {value}");
    }

    // Numerically unsafe float forms cannot be recovered exactly from
    // `serde_json::Value` (`"9007199254740991.0"` parses as
    // `9007199254740990.0`): fail closed as `InvalidResponse`, never as
    // `Ok(0)` (false unlimited) nor as a silently wrong byte rate.
    for value in [
        "9007199254740991.0",
        "9007199254740990.0",
        "4503599627370496.0",
    ] {
        let body = format!(r#"{{"network":{{"global_rate_limit":{value}}}}}"#);
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let err = client
            .get_global_rate_limit()
            .await
            .expect_err("unsafe large float must fail closed");
        let _ = server.join().expect("server thread panicked");
        assert!(
            matches!(err, ConnectError::InvalidResponse),
            "unsafe float {value} must map to InvalidResponse, got {err:?}"
        );
    }
}

#[tokio::test]
async fn get_global_rate_limit_parses_exact_numeric_and_iec_values() {
    use surge_desktop_lib::surge_client::MAX_RATE_LIMIT_BPS;

    for (value, expected) in [
        ("1048576", 1_048_576_i64),
        (r#""1 MiB/s""#, 1_048_576),
        (r#""1.5 MiB/s""#, 1_572_864),
        (r#""1 B/s""#, 1),
        (r#""1.5 B/s""#, 2),
        ("9007199254740991", MAX_RATE_LIMIT_BPS),
        (r#""9007199254740991 B/s""#, MAX_RATE_LIMIT_BPS),
    ] {
        let body = format!(r#"{{"network":{{"global_rate_limit":{value}}}}}"#);
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let rate = client
            .get_global_rate_limit()
            .await
            .expect("valid rate value must parse");
        let _ = server.join().expect("server thread panicked");
        assert_eq!(rate, expected, "unexpected parsed rate for {value}");
    }
}

/// Pinned Go `ParseRateLimit` uses `strconv.ParseFloat` (`float64`) then
/// `math.Round`: `0.49999999999999999 B/s` rounds to `0.5` in `float64` and
/// then to `1`, while a long all-zero fraction parses to zero without
/// denominator overflow.
#[tokio::test]
async fn get_global_rate_limit_matches_go_float64_edge_strings() {
    for (value, expected) in [
        (r#""0.49999999999999999 B/s""#, 1_i64),
        (
            r#""0.00000000000000000000000000000000000000000000000000 B/s""#,
            0,
        ),
    ] {
        let body = format!(r#"{{"network":{{"global_rate_limit":{value}}}}}"#);
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let rate = client
            .get_global_rate_limit()
            .await
            .expect("pinned float64 edge string must parse");
        let _ = server.join().expect("server thread panicked");
        assert_eq!(rate, expected, "unexpected parsed rate for {value}");
    }
}

/// Negative and malformed daemon strings must fail closed as `InvalidResponse`,
/// never as `Ok(0)` (false unlimited).
#[tokio::test]
async fn get_global_rate_limit_rejects_negative_and_malformed_strings() {
    let bodies = [
        r#"{"network":{"global_rate_limit":"-1 B/s"}}"#.to_string(),
        r#"{"network":{"global_rate_limit":"-.5 B/s"}}"#.to_string(),
        r#"{"network":{"global_rate_limit":"1.2.3 B/s"}}"#.to_string(),
        r#"{"network":{"global_rate_limit":". B/s"}}"#.to_string(),
        r#"{"network":{"global_rate_limit":"abc"}}"#.to_string(),
    ];
    for body in bodies {
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let err = client
            .get_global_rate_limit()
            .await
            .expect_err("negative/malformed string must fail");
        let _ = server.join().expect("server thread panicked");
        assert!(
            matches!(err, ConnectError::InvalidResponse),
            "negative/malformed string must map to InvalidResponse, got {:?}",
            err
        );
    }
}

#[tokio::test]
async fn get_global_rate_limit_rejects_out_of_range_and_nonfinite_values() {
    let bodies = [
        r#"{"network":{"global_rate_limit":-1}}"#.to_string(),
        r#"{"network":{"global_rate_limit":-0.5}}"#.to_string(),
        r#"{"network":{"global_rate_limit":9007199254740992}}"#.to_string(),
        r#"{"network":{"global_rate_limit":9007199254740991.5}}"#.to_string(),
        r#"{"network":{"global_rate_limit":1e309}}"#.to_string(),
        r#"{"network":{"global_rate_limit":"9007199254740992 B/s"}}"#.to_string(),
        r#"{"network":{"global_rate_limit":"-1 B/s"}}"#.to_string(),
        r#"{"network":{"global_rate_limit":"0.1 B/s"}}"#.to_string(),
    ];
    for body in bodies {
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let err = client
            .get_global_rate_limit()
            .await
            .expect_err("out-of-range settings must fail");
        let _ = server.join().expect("server thread panicked");
        assert!(
            matches!(err, ConnectError::InvalidResponse),
            "out-of-range settings must map to InvalidResponse, got {:?}",
            err
        );
    }
}

#[tokio::test]
async fn get_global_rate_limit_rejects_malformed_settings_without_leaking_body() {
    const SENTINEL: &str = "settings-sentinel-c41f8a-private-detail";
    let bodies = [
        "{}".to_string(),
        r#"{"network":{}}"#.to_string(),
        format!(r#"{{"network":{{"global_rate_limit":"{SENTINEL}"}}}}"#),
        r#"{"network":{"global_rate_limit":{"value":0}}}"#.to_string(),
        r#"{"network":{"global_rate_limit":true}}"#.to_string(),
    ];
    for body in bodies {
        let (base, server) =
            spawn_scripted_server(vec![("200 OK", LIST_BODY.to_string()), ("200 OK", body)]);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let err = client
            .get_global_rate_limit()
            .await
            .expect_err("malformed settings must fail");
        let _ = server.join().expect("server thread panicked");
        assert!(
            matches!(err, ConnectError::InvalidResponse),
            "malformed settings must map to InvalidResponse, got {:?}",
            err
        );
        let debug = format!("{:?}", err);
        let display = format!("{}", err);
        assert!(
            !debug.contains(SENTINEL),
            "error Debug leaked settings body"
        );
        assert!(
            !display.contains(SENTINEL),
            "error Display leaked settings body"
        );
    }
}

#[tokio::test]
async fn get_global_rate_limit_rejects_unauthorized_without_leaking_body() {
    const SENTINEL: &str = "settings-auth-sentinel-b81d9e-raw-detail";
    let body = format!(r#"{{"error":"unauthorized","message":"{SENTINEL}"}}"#);
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("401 Unauthorized", body),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .get_global_rate_limit()
        .await
        .expect_err("unauthorized settings must fail");
    let _ = server.join().expect("server thread panicked");
    assert!(matches!(err, ConnectError::Unauthorized));
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(
        !debug.contains(SENTINEL),
        "error Debug leaked settings body"
    );
    assert!(
        !display.contains(SENTINEL),
        "error Display leaked settings body"
    );
}

#[tokio::test]
async fn get_global_rate_limit_rejects_oversized_settings_body() {
    let (base, server) = spawn_two_step_server(LIST_BODY, 1024 * 1024 + 1);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .get_global_rate_limit()
        .await
        .expect_err("oversized settings body must fail");
    let _ = server.join().expect("server thread panicked");
    assert!(matches!(err, ConnectError::BodyTooLarge));
}

#[tokio::test]
async fn set_global_rate_limit_posts_exact_request_and_confirms_readback() {
    const RATE: i64 = 1_048_576;
    let ack = format!(r#"{{"status":"global_rate_limited","rate":"{RATE}"}}"#);
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", ack),
        ("200 OK", settings_direct_rate("1048576 B/s")),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let confirmed = client
        .set_global_rate_limit(RATE)
        .await
        .expect("confirmed write must succeed");
    let captured = server.join().expect("server thread panicked");
    assert_eq!(confirmed, RATE);
    assert!(captured[0].request_line.starts_with("GET /list "));
    assert_eq!(
        captured[1]
            .request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default(),
        "/rate-limit/global?rate=1048576"
    );
    assert!(captured[2].request_line.starts_with("GET /settings "));
    for request in &captured[1..] {
        assert!(
            request.authorization.as_deref() == Some("Bearer test-token"),
            "write and readback must carry the expected Bearer credential (value withheld)"
        );
    }
}

#[tokio::test]
async fn set_global_rate_limit_preserves_max_safe_integer_exactly() {
    use surge_desktop_lib::surge_client::MAX_RATE_LIMIT_BPS;

    let ack = format!(r#"{{"status":"global_rate_limited","rate":"{MAX_RATE_LIMIT_BPS}"}}"#);
    let readback = settings_direct_rate(&format!("{MAX_RATE_LIMIT_BPS} B/s"));
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", ack),
        ("200 OK", readback),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let confirmed = client
        .set_global_rate_limit(MAX_RATE_LIMIT_BPS)
        .await
        .expect("max safe integer must round-trip");
    let captured = server.join().expect("server thread panicked");
    assert_eq!(confirmed, MAX_RATE_LIMIT_BPS);
    assert_eq!(
        captured[1]
            .request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default(),
        format!("/rate-limit/global?rate={MAX_RATE_LIMIT_BPS}")
    );
}

#[tokio::test]
async fn set_global_rate_limit_zero_confirms_unlimited() {
    let ack = r#"{"status":"global_rate_unlimited","rate":"0"}"#.to_string();
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", ack),
        ("200 OK", settings_direct_rate("0")),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let confirmed = client
        .set_global_rate_limit(0)
        .await
        .expect("zero must confirm as unlimited");
    let captured = server.join().expect("server thread panicked");
    assert_eq!(confirmed, 0);
    assert_eq!(
        captured[1]
            .request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default(),
        "/rate-limit/global?rate=0"
    );
}

#[tokio::test]
async fn set_global_rate_limit_ambiguous_write_error_never_uses_late_readback() {
    const RATE: i64 = 2_097_152;
    const SENTINEL: &str = "write-error-sentinel-77ac40-raw-detail";
    let body = format!(r#"{{"error":"{SENTINEL}"}}"#);
    let (base, server) = spawn_ambiguous_post_server(
        "500 Internal Server Error",
        body,
        settings_direct_rate("2097152 B/s"),
    );
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .set_global_rate_limit(RATE)
        .await
        .expect_err("ambiguous write must not claim success");
    let captured = server.join().expect("server thread panicked");
    assert!(
        matches!(err, ConnectError::RateUnconfirmed),
        "ambiguous write must be explicitly unconfirmed, got {:?}",
        err
    );
    assert_eq!(
        captured.len(),
        2,
        "an ambiguous POST must not be followed by a readback"
    );
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(!debug.contains(SENTINEL), "error Debug leaked server body");
    assert!(
        !display.contains(SENTINEL),
        "error Display leaked server body"
    );
}

#[tokio::test]
async fn set_global_rate_limit_malformed_ack_is_ambiguous_even_with_matching_readback() {
    const RATE: i64 = 4_194_304;
    const SENTINEL: &str = "ack-sentinel-9d2e61-raw-detail";
    let ack = format!(r#"{{"status":"{SENTINEL}","rate":"{RATE}"}}"#);
    let (base, server) =
        spawn_ambiguous_post_server("200 OK", ack, settings_direct_rate("4194304 B/s"));
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .set_global_rate_limit(RATE)
        .await
        .expect_err("malformed ack must not claim success");
    let captured = server.join().expect("server thread panicked");
    assert!(matches!(err, ConnectError::RateUnconfirmed));
    assert_eq!(captured.len(), 2, "malformed ack must not be read back");
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(!debug.contains(SENTINEL), "error Debug leaked ack body");
    assert!(!display.contains(SENTINEL), "error Display leaked ack body");
}

#[tokio::test]
async fn set_global_rate_limit_rejects_readback_mismatch() {
    const RATE: i64 = 8_388_608;
    let ack = format!(r#"{{"status":"global_rate_limited","rate":"{RATE}"}}"#);
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", ack),
        ("200 OK", settings_direct_rate("1048576 B/s")),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .set_global_rate_limit(RATE)
        .await
        .expect_err("mismatched readback must fail");
    let _ = server.join().expect("server thread panicked");
    assert!(
        matches!(err, ConnectError::InvalidResponse),
        "ack without matching readback must not claim success, got {:?}",
        err
    );
}

#[tokio::test]
async fn set_global_rate_limit_reports_unknown_when_readback_fails() {
    const RATE: i64 = 33_554_432;
    const SENTINEL: &str = "readback-sentinel-e5a170-raw-detail";
    let ack = format!(r#"{{"status":"global_rate_limited","rate":"{RATE}"}}"#);
    let body = format!(r#"{{"error":"{SENTINEL}"}}"#);
    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", ack),
        ("500 Internal Server Error", body),
    ]);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .set_global_rate_limit(RATE)
        .await
        .expect_err("unreadable readback must not report success");
    let _ = server.join().expect("server thread panicked");
    assert!(
        matches!(err, ConnectError::RateUnconfirmed),
        "unavailable readback must be explicitly unknown, got {:?}",
        err
    );
    let debug = format!("{:?}", err);
    let display = format!("{}", err);
    assert!(!debug.contains(SENTINEL), "error Debug leaked server body");
    assert!(
        !display.contains(SENTINEL),
        "error Display leaked server body"
    );
}

#[tokio::test]
async fn set_global_rate_limit_rejects_invalid_input_without_network() {
    use surge_desktop_lib::surge_client::MAX_RATE_LIMIT_BPS;

    let (base, server) = spawn_server("200 OK", LIST_BODY);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    for rate in [-1, MAX_RATE_LIMIT_BPS + 1] {
        let err = client
            .set_global_rate_limit(rate)
            .await
            .expect_err("invalid rate must be rejected locally");
        assert!(
            matches!(err, ConnectError::InvalidResponse),
            "invalid rate must map to InvalidResponse, got {:?}",
            err
        );
    }
    let captured = server.join().expect("server thread panicked");
    assert!(
        captured.request_line.starts_with("GET /list "),
        "invalid rates must never reach the daemon"
    );
}

#[tokio::test]
async fn connection_manager_rate_limit_requires_connection_and_validates_input() {
    use surge_desktop_lib::commands::{BridgeError, ConnectionManager};
    use surge_desktop_lib::surge_client::MAX_RATE_LIMIT_BPS;

    let manager = ConnectionManager::default();
    assert!(matches!(
        manager.get_global_rate_limit().await,
        Err(BridgeError::NotConnected)
    ));
    assert!(matches!(
        manager.set_global_rate_limit(0).await,
        Err(BridgeError::NotConnected)
    ));
    for rate in [-1, MAX_RATE_LIMIT_BPS + 1] {
        assert!(matches!(
            manager.set_global_rate_limit(rate).await,
            Err(BridgeError::InvalidResponse)
        ));
    }
}

#[tokio::test]
async fn connection_manager_reads_manual_rate_but_refuses_manual_write() {
    use surge_desktop_lib::commands::{BridgeError, ConnectionManager};

    let (base, server) = spawn_scripted_server(vec![
        ("200 OK", LIST_BODY.to_string()),
        ("200 OK", settings_direct_rate("2048 B/s")),
    ]);
    let manager = ConnectionManager::default();
    manager.connect(&base, "test-token").await.expect("connect");
    assert_eq!(
        manager
            .get_global_rate_limit()
            .await
            .expect("manager readback"),
        2048
    );
    assert!(
        matches!(
            manager.set_global_rate_limit(4096).await,
            Err(BridgeError::NotConnected)
        ),
        "manual connections cannot be fenced for writes"
    );
    let captured = server.join().expect("server thread panicked");
    assert_eq!(
        captured.len(),
        2,
        "refused manual write must not reach the daemon"
    );
    assert!(captured[1].request_line.starts_with("GET /settings "));
}

fn spawn_gated_settings_server(
    status_line: &'static str,
    body: String,
) -> (
    String,
    JoinHandle<Vec<Captured>>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut captured = Vec::with_capacity(2);

        let mut first = accept_one(&listener);
        first
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        captured.push(parse_head(&read_head(&mut first)));
        write_json_response(&mut first, "200 OK", LIST_BODY);

        let mut second = accept_one(&listener);
        second
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        captured.push(parse_head(&read_head(&mut second)));
        let _ = started_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(5));
        write_json_response(&mut second, status_line, &body);
        captured
    });
    (format!("http://{}", addr), server, started_rx, release_tx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_get_rate_limit_is_superseded_without_touching_replacement() {
    use surge_desktop_lib::commands::{BridgeError, ConnectionManager};

    let (base_a, server_a, started, release) =
        spawn_gated_settings_server("200 OK", settings_direct_rate("7000 B/s"));
    let manager = std::sync::Arc::new(ConnectionManager::default());
    manager
        .connect(&base_a, "test-token-a")
        .await
        .expect("initial connect");

    let get_manager = std::sync::Arc::clone(&manager);
    let get_task = tokio::spawn(async move { get_manager.get_global_rate_limit().await });
    started
        .recv_timeout(Duration::from_secs(5))
        .expect("settings request must reach the gated server");

    manager.disconnect();
    let (base_b, server_b) = spawn_server("200 OK", LIST_BODY);
    manager
        .connect(&base_b, "test-token-b")
        .await
        .expect("replacement connect");
    let _ = release.send(());

    let result = get_task.await.expect("get task must finish");
    assert!(
        matches!(result, Err(BridgeError::Superseded)),
        "stale read must be superseded, got {:?}",
        result
    );

    let captured_a = server_a.join().expect("gated server thread panicked");
    assert!(
        captured_a[1].request_line.starts_with("GET /settings "),
        "stale request must use the old client only"
    );
    assert!(
        captured_a[1].authorization.as_deref() == Some("Bearer test-token-a"),
        "stale request must keep its original credential (value withheld)"
    );

    let captured_b = server_b.join().expect("replacement server thread panicked");
    assert!(
        captured_b.request_line.starts_with("GET /list "),
        "replacement connection must only see its own connect probe"
    );
    assert!(
        captured_b.authorization.as_deref() == Some("Bearer test-token-b"),
        "replacement request must use only its own credential (value withheld)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_get_error_is_superseded_not_leaked_as_new_state() {
    use surge_desktop_lib::commands::{BridgeError, ConnectionManager};

    let (base_a, server_a, started, release) =
        spawn_gated_settings_server("500 Internal Server Error", "{}".to_string());
    let manager = std::sync::Arc::new(ConnectionManager::default());
    manager
        .connect(&base_a, "test-token-a")
        .await
        .expect("initial connect");

    let get_manager = std::sync::Arc::clone(&manager);
    let get_task = tokio::spawn(async move { get_manager.get_global_rate_limit().await });
    started
        .recv_timeout(Duration::from_secs(5))
        .expect("settings request must reach the gated server");

    manager.disconnect();
    let (base_b, server_b) = spawn_server("200 OK", LIST_BODY);
    manager
        .connect(&base_b, "test-token-b")
        .await
        .expect("replacement connect");
    let _ = release.send(());

    let result = get_task.await.expect("get task must finish");
    assert!(
        matches!(result, Err(BridgeError::Superseded)),
        "an old connection's error must not surface as the new state, got {:?}",
        result
    );
    let _ = server_a.join().expect("gated server thread panicked");
    let _ = server_b.join().expect("replacement server thread panicked");
}

/// Sequential helper that serves `GET /list` then a bulk `POST` with an
/// explicit status line. Used for definitive non-200 outcomes.
fn spawn_connect_then_status(
    list_body: &'static str,
    status_line: &'static str,
    post_body: &'static str,
) -> (String, JoinHandle<(Captured, Captured)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let first = parse_head(&read_head(&mut first_stream));
        write_json_response(&mut first_stream, "200 OK", list_body);
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let second = parse_head(&read_head(&mut second_stream));
        let response = format!(
            "HTTP/1.1 {}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            status_line,
            post_body.len(),
            post_body
        );
        let _ = second_stream.write_all(response.as_bytes());
        let _ = second_stream.flush();
        (first, second)
    });
    (format!("http://{}", addr), server)
}

/// Sequential helper that serves `GET /list` then accepts the bulk request and
/// drops the socket without any response (transport failure after dispatch).
fn spawn_connect_then_drop(list_body: &'static str) -> (String, JoinHandle<Captured>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let mut first_stream = accept_one(&listener);
        first_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let _first = parse_head(&read_head(&mut first_stream));
        write_json_response(&mut first_stream, "200 OK", list_body);
        drop(first_stream);

        let mut second_stream = accept_one(&listener);
        second_stream
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("set read timeout");
        let captured = parse_head(&read_head(&mut second_stream));
        // Dropping without writing a response makes the client observe a
        // transport failure after the request may have reached the daemon.
        drop(second_stream);
        captured
    });
    (format!("http://{}", addr), server)
}

#[tokio::test]
async fn bulk_control_posts_allowlisted_routes_and_parses_partial_results() {
    use surge_desktop_lib::surge_client::{BulkControlAction, SurgeClient};

    let cases = [
        (BulkControlAction::PauseAll, "/pause-all", "paused"),
        (BulkControlAction::ResumeAll, "/resume-all", "resumed"),
    ];
    for (action, route, count_key) in cases {
        let body = format!(
            r#"{{"status":"done","{count_key}":1,"results":[{{"id":"a-1"}},{{"id":"b-2","error":"daemon said no"}}]}}"#
        );
        let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), body);
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let outcome = client
            .bulk_control(action)
            .await
            .expect("partial 200 must parse");
        let (_list, post) = server.join().expect("server thread panicked");
        assert!(
            post.request_line.starts_with(&format!("POST {route} ")),
            "unexpected bulk request line: {}",
            post.request_line
        );
        assert_eq!(
            post.authorization.as_deref(),
            Some("Bearer test-token"),
            "bearer must travel only in Authorization header"
        );
        assert_eq!(outcome.status, "done");
        assert_eq!(outcome.count, 1);
        assert_eq!(outcome.results.len(), 2);
        assert!(outcome.results[0].ok);
        assert_eq!(outcome.results[0].error, None);
        assert!(!outcome.results[1].ok);
        assert_eq!(
            outcome.results[1].error.as_deref(),
            Some("Daemon reported an error for this item.")
        );
    }
}

#[tokio::test]
async fn bulk_control_treats_malformed_or_inconsistent_200_as_unconfirmed() {
    use surge_desktop_lib::surge_client::{BulkControlAction, SurgeClient};

    let cases = [
        "{not json",
        r#"{"status":"error","paused":0,"results":[]}"#,
        r#"{"status":"done","paused":"0","results":[]}"#,
        r#"{"status":"done","paused":-1,"results":[]}"#,
        r#"{"status":"done","paused":2,"results":[{"id":"a"}]}"#,
        r#"{"status":"done","paused":1,"results":[{"id":"a"},{"id":"a"}]}"#,
        r#"{"status":"done","paused":0,"results":[{"id":""}]}"#,
        r#"{"status":"done","paused":1,"results":[{"id":"a","error":7}]}"#,
        r#"{"status":"done","paused":0,"results":{}}"#,
        r#"{"status":"done","results":[]}"#,
    ];
    for body in cases {
        let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), body.to_string());
        let client = SurgeClient::connect(&base, "test-token")
            .await
            .expect("connect");
        let err = client
            .bulk_control(BulkControlAction::PauseAll)
            .await
            .expect_err("malformed 200 must not report a clean outcome");
        let _ = server.join().expect("server thread panicked");
        assert!(
            matches!(err, ConnectError::BulkUnconfirmed),
            "body {body} must be BulkUnconfirmed, got {err:?}"
        );
        let debug = format!("{err:?}");
        assert!(
            !debug.contains(body),
            "error Debug must not echo the response body"
        );
    }
}

#[tokio::test]
async fn bulk_control_transport_failure_is_unconfirmed() {
    use surge_desktop_lib::surge_client::{BulkControlAction, SurgeClient};

    let (base, server) = spawn_connect_then_drop(LIST_BODY);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .bulk_control(BulkControlAction::ResumeAll)
        .await
        .expect_err("dropped transport must be unconfirmed");
    let post = server.join().expect("server thread panicked");
    assert!(
        post.request_line.starts_with("POST /resume-all "),
        "unexpected request line: {}",
        post.request_line
    );
    assert!(
        matches!(err, ConnectError::BulkUnconfirmed),
        "transport failure must be BulkUnconfirmed, got {err:?}"
    );
}

#[tokio::test]
async fn bulk_control_500_is_definitive_but_401_is_unauthorized() {
    use surge_desktop_lib::surge_client::{BulkControlAction, SurgeClient};

    let (base, server) = spawn_connect_then_status(
        LIST_BODY,
        "500 Internal Server Error",
        "list failed before any action",
    );
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .bulk_control(BulkControlAction::PauseAll)
        .await
        .expect_err("500 must fail");
    let _ = server.join().expect("server thread panicked");
    assert!(
        matches!(err, ConnectError::InvalidResponse),
        "list-stage 500 is a definitive non-application, got {err:?}"
    );

    let (base, server) =
        spawn_connect_then_status(LIST_BODY, "401 Unauthorized", r#"{"error":"no"}"#);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let err = client
        .bulk_control(BulkControlAction::ResumeAll)
        .await
        .expect_err("401 must fail");
    let _ = server.join().expect("server thread panicked");
    assert!(
        matches!(err, ConnectError::Unauthorized),
        "401 must map to Unauthorized, got {err:?}"
    );
}

#[tokio::test]
async fn bulk_control_replaces_per_item_error_with_fixed_safe_text() {
    use surge_desktop_lib::surge_client::{BulkControlAction, SurgeClient};

    // A daemon error can echo the bearer token, a signed URL, or a private
    // path. None of it may survive the sanitizer or appear in the IPC payload.
    let secret = "super-secret-token-xyz";
    let signed_url = "https://dl.example.test/file.bin?sig=signed-secret-abc";
    let private_path = "/home/alice/private/downloads/file.bin";
    let long_error = format!(
        "Authorization: Bearer {secret} {signed_url} {private_path}\\u0000{}",
        "x".repeat(4096)
    );
    let body = format!(
        r#"{{"status":"done","paused":0,"results":[{{"id":"server-id-7f3a","error":"{long_error}"}}]}}"#
    );
    let (base, server) = spawn_connect_then_control(LIST_BODY.to_string(), body);
    let client = SurgeClient::connect(&base, "test-token")
        .await
        .expect("connect");
    let outcome = client
        .bulk_control(BulkControlAction::PauseAll)
        .await
        .expect("partial 200 must parse");
    let _ = server.join().expect("server thread panicked");
    assert_eq!(outcome.count, 0);
    assert_eq!(outcome.results.len(), 1);
    let sanitized = outcome.results[0]
        .error
        .as_deref()
        .expect("failed item must carry a safe error");
    assert_eq!(sanitized, "Daemon reported an error for this item.");
    let serialized = serde_json::to_string(&outcome).expect("outcome must serialize");
    let debug_outcome = format!("{outcome:?}");
    let debug_item = format!("{:?}", outcome.results[0]);
    for leaked in [
        secret,
        signed_url,
        private_path,
        "Authorization",
        "private-daemon-error",
    ] {
        assert!(
            !serialized.contains(leaked),
            "IPC payload must not echo server-controlled error content"
        );
        assert!(
            !debug_outcome.contains(leaked),
            "BulkControlOutcome Debug must hide server-controlled values"
        );
        assert!(
            !debug_item.contains(leaked),
            "BulkControlItemResult Debug must hide server-controlled values"
        );
    }
    assert!(
        !debug_outcome.contains("server-id-7f3a"),
        "BulkControlOutcome Debug must hide the server-controlled id"
    );
    assert!(
        !debug_item.contains("server-id-7f3a"),
        "BulkControlItemResult Debug must hide the server-controlled id"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_bulk_control_success_is_superseded_after_reconnect() {
    use std::sync::Arc;

    use surge_desktop_lib::commands::{BridgeError, ConnectionManager};
    use surge_desktop_lib::surge_client::BulkControlAction;

    let body = r#"{"status":"done","paused":0,"results":[]}"#.to_string();
    let (base_a, server_a, started, release) = spawn_gated_settings_server("200 OK", body);
    let manager = Arc::new(ConnectionManager::default());
    manager
        .connect(&base_a, "test-token-a")
        .await
        .expect("initial connect");

    let bulk_manager = Arc::clone(&manager);
    let task =
        tokio::spawn(async move { bulk_manager.bulk_control(BulkControlAction::PauseAll).await });
    started
        .recv_timeout(Duration::from_secs(5))
        .expect("bulk request must reach the gated server");

    manager.disconnect();
    let (base_b, server_b) = spawn_server("200 OK", LIST_BODY);
    manager
        .connect(&base_b, "test-token-b")
        .await
        .expect("replacement connect");
    let _ = release.send(());

    let result = task.await.expect("bulk task must finish");
    assert!(
        matches!(result, Err(BridgeError::Superseded)),
        "a prior daemon's bulk success must not surface on the replacement, got {result:?}"
    );

    let captured_a = server_a.join().expect("gated server thread panicked");
    assert!(
        captured_a[1].request_line.starts_with("POST /pause-all "),
        "stale request must use the old client only"
    );
    assert_eq!(
        captured_a[1].authorization.as_deref(),
        Some("Bearer test-token-a"),
        "stale request must keep its original credential (value withheld)"
    );

    let captured_b = server_b.join().expect("replacement server thread panicked");
    assert!(
        captured_b.request_line.starts_with("GET /list "),
        "replacement connection must only see its own connect probe"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_bulk_control_error_is_superseded_after_reconnect() {
    use std::sync::Arc;

    use surge_desktop_lib::commands::{BridgeError, ConnectionManager};
    use surge_desktop_lib::surge_client::BulkControlAction;

    let (base_a, server_a, started, release) =
        spawn_gated_settings_server("500 Internal Server Error", "boom".to_string());
    let manager = Arc::new(ConnectionManager::default());
    manager
        .connect(&base_a, "test-token-a")
        .await
        .expect("initial connect");

    let bulk_manager = Arc::clone(&manager);
    let task = tokio::spawn(async move {
        bulk_manager
            .bulk_control(BulkControlAction::ResumeAll)
            .await
    });
    started
        .recv_timeout(Duration::from_secs(5))
        .expect("bulk request must reach the gated server");

    manager.disconnect();
    let (base_b, server_b) = spawn_server("200 OK", LIST_BODY);
    manager
        .connect(&base_b, "test-token-b")
        .await
        .expect("replacement connect");
    let _ = release.send(());

    let result = task.await.expect("bulk task must finish");
    assert!(
        matches!(result, Err(BridgeError::Superseded)),
        "a prior daemon's bulk error must not surface on the replacement, got {result:?}"
    );
    let captured_a = server_a.join().expect("gated server thread panicked");
    assert!(captured_a[1].request_line.starts_with("POST /resume-all "));
    let captured_b = server_b.join().expect("replacement server thread panicked");
    assert!(captured_b.request_line.starts_with("GET /list "));
}

fn serve_with_content_type(
    listener: &TcpListener,
    status_line: &str,
    content_type: &str,
    body: &str,
) -> Captured {
    let mut stream = accept_one(listener);
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("set read timeout");
    let captured = parse_head(&read_head(&mut stream));
    let response = format!(
        "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        status_line,
        content_type,
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    captured
}

fn spawn_event_server(
    list_status: &'static str,
    list_content_type: &'static str,
    list_body: &'static str,
    events_status: &'static str,
    events_content_type: &'static str,
    events_body: &'static str,
) -> (String, JoinHandle<(Captured, Captured)>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback test server");
    let addr = listener.local_addr().expect("local addr");
    let server = thread::spawn(move || {
        let list = serve_with_content_type(&listener, list_status, list_content_type, list_body);
        let events =
            serve_with_content_type(&listener, events_status, events_content_type, events_body);
        (list, events)
    });
    (format!("http://{}", addr), server)
}

const SSE_BODY: &str = "event: progress\ndata: {\"type\":1,\"download_id\":\"dl-1\",\"downloaded\":5,\"total\":10}\n\n";

#[tokio::test]
async fn event_stream_uses_authenticated_get_and_validates_content_type() {
    let (base, server) = spawn_event_server(
        "200 OK",
        "application/json",
        LIST_BODY,
        "200 OK",
        "text/event-stream; charset=utf-8",
        SSE_BODY,
    );
    let client = SurgeClient::connect(&base, "stream-token")
        .await
        .expect("connect");
    let response = client.open_event_stream().await.expect("event stream");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let (list, events) = server.join().expect("server thread");
    assert!(list.request_line.starts_with("GET /list "));
    assert!(
        events.request_line.starts_with("GET /events "),
        "unexpected request line: {}",
        events.request_line
    );
    assert_eq!(
        events.authorization.as_deref(),
        Some("Bearer stream-token"),
        "bearer must travel only in Authorization"
    );
}

#[tokio::test]
async fn event_stream_rejects_wrong_content_type() {
    let (base, server) = spawn_event_server(
        "200 OK",
        "application/json",
        LIST_BODY,
        "200 OK",
        "application/json",
        "{}",
    );
    let client = SurgeClient::connect(&base, "stream-token")
        .await
        .expect("connect");
    assert_eq!(
        client.open_event_stream().await.unwrap_err(),
        ConnectError::InvalidResponse
    );
    server.join().expect("server thread");
}

#[tokio::test]
async fn event_stream_rejects_unauthorized_status() {
    let (base, server) = spawn_event_server(
        "200 OK",
        "application/json",
        LIST_BODY,
        "401 Unauthorized",
        "text/event-stream",
        UNAUTHORIZED_BODY,
    );
    let client = SurgeClient::connect(&base, "stream-token")
        .await
        .expect("connect");
    assert_eq!(
        client.open_event_stream().await.unwrap_err(),
        ConnectError::Unauthorized
    );
    server.join().expect("server thread");
}
