//! Optional TLS support for the SQL wire listeners (PostgreSQL + MySQL).
//!
//! Both protocols negotiate TLS *after* the TCP connection is established —
//! PostgreSQL via the `SSLRequest` preamble and MySQL via the `CLIENT_SSL`
//! capability flag — so the wire handlers operate on [`MaybeTlsStream`] and
//! call [`MaybeTlsStream::upgrade_to_tls`] in place once the client asks for
//! encryption. When no certificate is configured the listeners keep their
//! existing plaintext behavior (PG answers `SSLRequest` with `N`, MySQL does
//! not advertise `CLIENT_SSL`).

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{anyhow, Context as _};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{server::TlsStream, TlsAcceptor};

/// A TCP stream that may be plaintext or upgraded to TLS mid-connection.
pub enum MaybeTlsStream {
    /// Plaintext TCP, the initial state of every accepted connection.
    Plain(TcpStream),
    /// TLS-encrypted stream after a successful handshake.
    Tls(Box<TlsStream<TcpStream>>),
    /// Transient state held only while a TLS handshake is in flight; the
    /// underlying socket has already been consumed by the acceptor.
    Upgrading,
}

impl MaybeTlsStream {
    /// Wrap a freshly accepted plaintext socket.
    pub fn new(stream: TcpStream) -> Self {
        MaybeTlsStream::Plain(stream)
    }

    /// Whether the stream has been upgraded to TLS.
    pub fn is_tls(&self) -> bool {
        matches!(self, MaybeTlsStream::Tls(_))
    }

    /// Perform the server side of a TLS handshake, replacing the plaintext
    /// socket with the encrypted stream in place. Fails if the stream is not
    /// currently plaintext.
    pub async fn upgrade_to_tls(&mut self, acceptor: &TlsAcceptor) -> anyhow::Result<()> {
        let plain = match std::mem::replace(self, MaybeTlsStream::Upgrading) {
            MaybeTlsStream::Plain(stream) => stream,
            other => {
                *self = other;
                return Err(anyhow!("TLS upgrade requested on a non-plaintext stream"));
            }
        };
        // `accept` consumes the socket; on failure the connection is dead and
        // the stream stays in `Upgrading`, which fails all later I/O.
        let tls = acceptor
            .accept(plain)
            .await
            .context("TLS handshake failed")?;
        *self = MaybeTlsStream::Tls(Box::new(tls));
        Ok(())
    }
}

fn upgrading_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "stream is mid-TLS-upgrade",
    )
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            MaybeTlsStream::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            MaybeTlsStream::Upgrading => Poll::Ready(Err(upgrading_error())),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            MaybeTlsStream::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            MaybeTlsStream::Upgrading => Poll::Ready(Err(upgrading_error())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            MaybeTlsStream::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            MaybeTlsStream::Upgrading => Poll::Ready(Err(upgrading_error())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            MaybeTlsStream::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            MaybeTlsStream::Upgrading => Poll::Ready(Err(upgrading_error())),
        }
    }
}

/// Build a [`TlsAcceptor`] from PEM certificate-chain and private-key files.
///
/// The certificate file may contain a full chain (leaf first). The key file
/// must hold a single PKCS#8, PKCS#1, or SEC1 private key.
pub fn load_tls_acceptor(cert_path: &Path, key_path: &Path) -> anyhow::Result<TlsAcceptor> {
    // Idempotent: returns Err if a provider is already installed (e.g. by the
    // QUIC listener), which we intentionally ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let certs = CertificateDer::pem_file_iter(cert_path)
        .with_context(|| format!("open TLS cert: {}", cert_path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .context("read TLS certs")?;
    if certs.is_empty() {
        return Err(anyhow!("no certificates found in {}", cert_path.display()));
    }

    let key = PrivateKeyDer::from_pem_file(key_path)
        .with_context(|| format!("open/read TLS key: {}", key_path.display()))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("build TLS server config")?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}
