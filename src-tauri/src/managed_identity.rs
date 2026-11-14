//! In-memory per-launch managed identity for the local Surge controller.
//!
//! This module generates, entirely in memory:
//!
//! * a cryptographically random bearer token (256-bit, base64url, no padding), and
//! * an ephemeral local CA plus a leaf TLS server certificate signed by that CA,
//!   with an IP SAN for `127.0.0.1` and a fresh ECDSA P-256 key.
//!
//! # Security properties
//!
//! * Everything is generated per call to [`ManagedIdentity::generate`] with fresh
//!   randomness/keys. Callers are expected to generate one identity per process
//!   launch and keep it in memory.
//! * All fields of [`ManagedIdentity`] are private. Callers only receive borrowed
//!   views via explicit accessors.
//! * [`std::fmt::Debug`] for both [`ManagedIdentity`] and [`ManagedIdentityError`]
//!   is manually implemented and never emits token, key, or certificate bytes.
//! * [`ManagedIdentityError`] messages are generic/sanitized: they never include
//!   secret material, certificate contents, filesystem paths, or underlying
//!   library error strings.
//! * This module never writes to the filesystem and never spawns processes.
//!   It only uses in-memory cryptographic operations (OS RNG + `rcgen`).
//!
//! # Encoding
//!
//! Certificates/keys are stored as DER (`Vec<u8>`). PEM views are derived
//! on demand in memory via [`ManagedIdentity::ca_cert_pem`],
//! [`ManagedIdentity::server_cert_pem`], and [`ManagedIdentity::server_key_pem`].

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use base64::{
    engine::general_purpose::STANDARD, engine::general_purpose::URL_SAFE_NO_PAD, Engine as _,
};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};

/// Number of random bytes for the bearer token (256 bits).
const TOKEN_BYTES: usize = 32;

/// Per-launch in-memory identity: bearer token + local CA + leaf server cert/key.
///
/// All fields are private; use the accessor methods. `Debug` is redacted.
pub struct ManagedIdentity {
    bearer_token: String,
    ca_cert_der: Vec<u8>,
    server_cert_der: Vec<u8>,
    server_key_der: Vec<u8>,
}

/// Sanitized error for identity generation.
///
/// Both `Display` and `Debug` emit only a generic message so no secret,
/// path, or underlying-library detail can leak.
pub struct ManagedIdentityError {
    kind: ErrorKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorKind {
    Randomness,
    Certificate,
}

impl ManagedIdentity {
    /// Generate a fresh in-memory identity (new token, new CA, new leaf key/cert).
    ///
    /// No filesystem I/O and no child processes are performed.
    pub fn generate() -> Result<Self, ManagedIdentityError> {
        // 1. Cryptographically random bearer token via OS RNG.
        let mut raw = [0u8; TOKEN_BYTES];
        getrandom::fill(&mut raw).map_err(|_| ManagedIdentityError {
            kind: ErrorKind::Randomness,
        })?;
        let bearer_token = URL_SAFE_NO_PAD.encode(raw);

        // 2. Ephemeral local CA (self-signed, ECDSA P-256).
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let mut ca_dn = DistinguishedName::new();
        ca_dn.push(DnType::CommonName, "surge-desktop local CA (ephemeral)");
        ca_params.distinguished_name = ca_dn;

        let ca_key = KeyPair::generate().map_err(|_| ManagedIdentityError {
            kind: ErrorKind::Certificate,
        })?;
        let ca_cert = ca_params
            .self_signed(&ca_key)
            .map_err(|_| ManagedIdentityError {
                kind: ErrorKind::Certificate,
            })?;
        let ca_cert_der = ca_cert.der().as_ref().to_vec();

        // Issuer owns the CA params + key; used to sign the leaf.
        let issuer = rcgen::Issuer::new(ca_params, ca_key);

        // 3. Ephemeral leaf server key + cert signed by the CA.
        //    Must contain IP SAN 127.0.0.1. We also include ::1 and
        //    DNS "localhost" for loopback robustness.
        let leaf_key = KeyPair::generate().map_err(|_| ManagedIdentityError {
            kind: ErrorKind::Certificate,
        })?;
        let mut leaf_params = CertificateParams::default();
        leaf_params.is_ca = IsCa::NoCa;
        leaf_params.subject_alt_names = vec![
            SanType::IpAddress(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            SanType::IpAddress(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            SanType::DnsName("localhost".try_into().map_err(|_| ManagedIdentityError {
                kind: ErrorKind::Certificate,
            })?),
        ];
        leaf_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        leaf_params.use_authority_key_identifier_extension = true;
        let mut leaf_dn = DistinguishedName::new();
        leaf_dn.push(DnType::CommonName, "surge-desktop local server (ephemeral)");
        leaf_params.distinguished_name = leaf_dn;

        let leaf_cert =
            leaf_params
                .signed_by(&leaf_key, &issuer)
                .map_err(|_| ManagedIdentityError {
                    kind: ErrorKind::Certificate,
                })?;
        let server_cert_der = leaf_cert.der().as_ref().to_vec();
        let server_key_der = leaf_key.serialize_der();

        Ok(Self {
            bearer_token,
            ca_cert_der,
            server_cert_der,
            server_key_der,
        })
    }

    /// Bearer token value (without the `Bearer ` prefix).
    pub fn bearer_token(&self) -> &str {
        &self.bearer_token
    }

    /// Value suitable for an HTTP `Authorization` header.
    pub fn authorization_header_value(&self) -> String {
        format!("Bearer {}", self.bearer_token)
    }

    /// DER-encoded local CA certificate.
    pub fn ca_cert_der(&self) -> &[u8] {
        &self.ca_cert_der
    }

    /// DER-encoded leaf server certificate (signed by [`Self::ca_cert_der`]).
    pub fn server_cert_der(&self) -> &[u8] {
        &self.server_cert_der
    }

    /// DER-encoded leaf server private key (PKCS#8).
    pub fn server_key_der(&self) -> &[u8] {
        &self.server_key_der
    }

    /// PEM-encoded local CA certificate (derived in memory).
    pub fn ca_cert_pem(&self) -> String {
        der_to_pem("CERTIFICATE", &self.ca_cert_der)
    }

    /// PEM-encoded leaf server certificate (derived in memory).
    pub fn server_cert_pem(&self) -> String {
        der_to_pem("CERTIFICATE", &self.server_cert_der)
    }

    /// PEM-encoded leaf server private key, PKCS#8 (derived in memory).
    pub fn server_key_pem(&self) -> String {
        der_to_pem("PRIVATE KEY", &self.server_key_der)
    }
}

fn der_to_pem(label: &str, der: &[u8]) -> String {
    let b64 = STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

impl fmt::Debug for ManagedIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ManagedIdentity { ..redacted.. }")
    }
}

impl fmt::Display for ManagedIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Single generic message for all failure modes: sanitized.
        // Reference `kind` so the field is read, without leaking which
        // variant occurred.
        let _ = self.kind;
        f.write_str("managed identity generation failed")
    }
}

impl fmt::Debug for ManagedIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = self.kind;
        f.write_str("ManagedIdentityError { ..redacted.. }")
    }
}

impl std::error::Error for ManagedIdentityError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn is_base64url_nopad(s: &str) -> bool {
        !s.is_empty()
            && s.bytes().all(|b| {
                matches!(
                    b,
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_'
                )
            })
    }

    #[test]
    fn generates_distinct_identities() {
        let a = ManagedIdentity::generate().expect("generate a");
        let b = ManagedIdentity::generate().expect("generate b");

        // Tokens: 32 bytes -> 43 base64url chars, distinct.
        assert_eq!(
            a.bearer_token().len(),
            43,
            "token must be 32 bytes base64url"
        );
        assert_eq!(b.bearer_token().len(), 43);
        assert!(is_base64url_nopad(a.bearer_token()));
        assert!(is_base64url_nopad(b.bearer_token()));
        assert_ne!(
            a.bearer_token(),
            b.bearer_token(),
            "each identity must have a fresh token"
        );

        // Certs/keys: non-empty and distinct (fresh keys per launch).
        assert!(!a.ca_cert_der().is_empty());
        assert!(!a.server_cert_der().is_empty());
        assert!(!a.server_key_der().is_empty());
        assert_ne!(a.ca_cert_der(), b.ca_cert_der(), "fresh CA per identity");
        assert_ne!(
            a.server_cert_der(),
            b.server_cert_der(),
            "fresh leaf cert per identity"
        );
        assert_ne!(
            a.server_key_der(),
            b.server_key_der(),
            "fresh leaf key per identity"
        );

        // Authorization header helper must embed the token.
        assert_eq!(
            a.authorization_header_value(),
            format!("Bearer {}", a.bearer_token())
        );

        // PEM views must round-trip the stored DER.
        assert!(a.ca_cert_pem().contains("-----BEGIN CERTIFICATE-----"));
        assert!(a.server_cert_pem().contains("-----BEGIN CERTIFICATE-----"));
        assert!(a.server_key_pem().contains("-----BEGIN PRIVATE KEY-----"));
    }

    fn parse_cert<'a>(der: &'a [u8]) -> x509_parser::certificate::X509Certificate<'a> {
        let (_, cert) = x509_parser::parse_x509_certificate(der).expect("parse DER");
        cert
    }

    #[test]
    fn certificate_chain_and_ip_san_are_correct() {
        let id = ManagedIdentity::generate().expect("generate");

        let ca = parse_cert(id.ca_cert_der());
        let leaf = parse_cert(id.server_cert_der());

        // Leaf issuer must equal CA subject (signed by CA).
        assert_eq!(
            leaf.issuer().to_string(),
            ca.subject().to_string(),
            "leaf must be issued by the CA"
        );

        // CA must be a CA, leaf must not be a CA.
        let ca_constraints = ca
            .basic_constraints()
            .expect("read CA extensions")
            .expect("CA must have basicConstraints");
        assert!(ca_constraints.value.ca, "CA must have CA:true");
        let leaf_constraints_opt = leaf.basic_constraints().expect("read leaf extensions");
        if let Some(ext) = leaf_constraints_opt {
            assert!(!ext.value.ca, "leaf must not be a CA");
        }

        // Leaf EKU must contain serverAuth.
        let eku = leaf
            .extended_key_usage()
            .expect("read EKU")
            .expect("leaf must have EKU");
        assert!(
            eku.value.server_auth,
            "leaf must have serverAuth EKU, got {:?}",
            eku.value
        );

        // SAN must contain IP 127.0.0.1.
        let san = leaf
            .subject_alternative_name()
            .expect("read SAN")
            .expect("leaf must have SAN");
        let mut found_v4_loopback = false;
        for name in san.value.general_names.iter() {
            if let x509_parser::extensions::GeneralName::IPAddress(bytes) = name {
                if bytes.len() == 4
                    && bytes[0] == 127
                    && bytes[1] == 0
                    && bytes[2] == 0
                    && bytes[3] == 1
                {
                    found_v4_loopback = true;
                }
            }
        }
        assert!(
            found_v4_loopback,
            "leaf SAN must contain IP 127.0.0.1, got {:?}",
            san.value.general_names
        );
    }

    fn build_rustls_configs(
        id: &ManagedIdentity,
    ) -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let ca_der = CertificateDer::from(id.ca_cert_der().to_vec());
        let leaf_der = CertificateDer::from(id.server_cert_der().to_vec());
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(id.server_key_der().to_vec()));

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(ca_der)
            .expect("CA DER must be a valid trust anchor");

        let server_config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![leaf_der], key_der)
                .expect("leaf cert/key must form a valid server identity"),
        );
        let client_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        (server_config, client_config)
    }

    fn loopback_tls_roundtrip(id: &ManagedIdentity) {
        let (server_config, client_config) = build_rustls_configs(id);

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");

        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let stream = loop {
                match listener.accept() {
                    Ok((s, _)) => break s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "TLS test server accept timed out"
                        );
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("accept failed: {e:?}"),
                }
            };
            let mut stream = stream;
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            let mut conn = rustls::ServerConnection::new(server_config).expect("server conn");
            let mut tls = rustls::Stream::new(&mut conn, &mut stream);
            let mut buf = [0u8; 5];
            tls.read_exact(&mut buf).expect("server read");
            assert_eq!(&buf, b"hello");
            tls.write_all(b"world").expect("server write");
            tls.flush().expect("server flush");
        });

        {
            let mut stream = std::net::TcpStream::connect(addr).expect("client connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("client timeout");
            let server_name =
                rustls::pki_types::ServerName::IpAddress(rustls::pki_types::IpAddr::from(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                ));
            let mut conn =
                rustls::ClientConnection::new(client_config, server_name).expect("client conn");
            let mut tls = rustls::Stream::new(&mut conn, &mut stream);
            tls.write_all(b"hello").expect("client write");
            tls.flush().expect("client flush");
            let mut buf = [0u8; 5];
            tls.read_exact(&mut buf).expect("client read");
            assert_eq!(&buf, b"world");
        }

        server.join().expect("server thread");
    }

    #[test]
    fn real_tls_validation_against_ca_succeeds() {
        let id = ManagedIdentity::generate().expect("generate");
        // Full rustls handshake over loopback to 127.0.0.1, trusting only our
        // in-memory CA. This validates the chain and the IP SAN.
        loopback_tls_roundtrip(&id);
    }

    #[test]
    fn real_tls_validation_rejects_wrong_ca() {
        let server_id = ManagedIdentity::generate().expect("server id");
        let other_ca = ManagedIdentity::generate().expect("other ca");

        // Build a client trusting the *wrong* CA; handshake must fail.
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
        let leaf_der = CertificateDer::from(server_id.server_cert_der().to_vec());
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            server_id.server_key_der().to_vec(),
        ));
        let server_config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![leaf_der], key_der)
                .expect("server identity"),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(other_ca.ca_cert_der().to_vec()))
            .expect("other CA anchor");
        let client_config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = std::thread::spawn(move || {
            // Server will fail the handshake once the client rejects the cert,
            // or vice versa; either way we just attempt one exchange and exit.
            let (mut stream, _) = listener.accept().expect("accept");
            stream.set_read_timeout(Some(Duration::from_secs(3))).ok();
            let mut conn = rustls::ServerConnection::new(server_config).expect("conn");
            let mut tls = rustls::Stream::new(&mut conn, &mut stream);
            let mut buf = [0u8; 5];
            let _ = tls.read_exact(&mut buf);
        });

        let mut stream = std::net::TcpStream::connect(addr).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(3))).ok();
        let server_name =
            rustls::pki_types::ServerName::IpAddress(rustls::pki_types::IpAddr::from(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            ));
        let mut conn =
            rustls::ClientConnection::new(client_config, server_name).expect("client conn");
        let mut tls = rustls::Stream::new(&mut conn, &mut stream);
        let write_ok = tls.write_all(b"hello").is_ok() && tls.flush().is_ok();
        let mut buf = [0u8; 5];
        let read_ok = tls.read_exact(&mut buf).is_ok();
        // With the wrong trust anchor the handshake/data transfer must not
        // complete successfully in both directions.
        assert!(
            !(write_ok && read_ok && &buf == b"world"),
            "handshake with wrong CA must not succeed"
        );
        let _ = server.join();
    }

    #[test]
    fn debug_and_errors_never_leak_secrets() {
        let id = ManagedIdentity::generate().expect("generate");
        let dbg = format!("{:?}", id);
        assert!(
            dbg.contains("redacted"),
            "Debug must indicate redaction, got {dbg:?}"
        );
        assert!(
            !dbg.contains(id.bearer_token()),
            "Debug must not leak the bearer token"
        );
        // DER bytes must not appear (checked via PEM fragments which are
        // stable ASCII encodings of the secrets).
        for pem in [id.ca_cert_pem(), id.server_cert_pem(), id.server_key_pem()] {
            let fragment = pem
                .lines()
                .find(|l| !l.starts_with("-----") && l.len() >= 16)
                .expect("pem body");
            let short = &fragment[..16];
            assert!(
                !dbg.contains(short),
                "Debug must not leak cert/key material"
            );
        }
        assert!(!dbg.contains("BEGIN"), "Debug must not contain PEM markers");

        let err = ManagedIdentityError {
            kind: ErrorKind::Certificate,
        };
        let err_dbg = format!("{:?}", err);
        let err_display = format!("{err}");
        assert!(err_dbg.contains("redacted"), "error Debug must be redacted");
        assert_eq!(
            err_display, "managed identity generation failed",
            "error Display must be generic/sanitized"
        );
        assert!(!err_dbg.contains("BEGIN"));
        assert!(!err_display.contains(id.bearer_token()));
    }
}
