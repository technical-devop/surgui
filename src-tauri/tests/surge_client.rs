use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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
