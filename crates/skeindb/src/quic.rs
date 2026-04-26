use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context};
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use skeindb_skeinql::RpcRequest;

use crate::server::{handle_rpc, AppState, RpcPolicy};

#[derive(Debug, Default, serde::Deserialize)]
struct QuicMeta {
    #[serde(default)]
    rtt: Option<String>,
    #[serde(default)]
    read_only: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
struct QuicEnvelope {
    request: RpcRequest,
    #[serde(default)]
    meta: QuicMeta,
}

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

fn ensure_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[derive(Debug, Clone)]
pub struct QuicServeOpts {
    pub bind: String,
    pub port: u16,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

pub async fn serve_quic(state: AppState, opts: QuicServeOpts) -> anyhow::Result<()> {
    ensure_rustls_provider();
    let addr: SocketAddr = format!("{}:{}", opts.bind, opts.port).parse()?;
    let (certs, key) = load_cert_chain(&opts.cert_path, &opts.key_path)?;

    let mut server_config = ServerConfig::with_single_cert(certs, key)?;
    server_config.transport = Arc::new(quinn::TransportConfig::default());

    let endpoint = Endpoint::server(server_config, addr)?;
    tracing::info!(%addr, "QUIC listening");

    while let Some(connecting) = endpoint.accept().await {
        let state = state.clone();
        tokio::spawn(async move {
            match connecting.await {
                Ok(connection) => loop {
                    match connection.accept_bi().await {
                        Ok((send, recv)) => {
                            let state = state.clone();
                            tokio::spawn(async move {
                                if let Err(err) = handle_stream(state, send, recv).await {
                                    tracing::debug!(?err, "QUIC stream error");
                                }
                            });
                        }
                        Err(err) => {
                            tracing::debug!(?err, "QUIC connection closed");
                            break;
                        }
                    }
                },
                Err(err) => {
                    tracing::warn!(?err, "QUIC connection failed");
                }
            }
        });
    }

    Ok(())
}

async fn handle_stream(
    state: AppState,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) -> anyhow::Result<()> {
    let bytes = read_frame(&mut recv).await?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).with_context(|| "failed to decode QUIC RPC request")?;
    let (req, meta) = if value.get("request").is_some() {
        let envelope: QuicEnvelope = serde_json::from_value(value)
            .with_context(|| "failed to decode QUIC request envelope")?;
        (envelope.request, envelope.meta)
    } else {
        let req: RpcRequest =
            serde_json::from_value(value).with_context(|| "failed to decode QUIC RPC request")?;
        (req, QuicMeta::default())
    };

    if let Some(rtt) = meta.rtt.as_deref() {
        if rtt != "0rtt" && rtt != "1rtt" {
            let resp: skeindb_skeinql::RpcResponse = skeindb_skeinql::RpcResponse::err(
                req.id.clone(),
                skeindb_skeinql::RpcError::new("invalid_request", "rtt must be '0rtt' or '1rtt'"),
            );
            let resp_bytes =
                serde_json::to_vec(&resp).with_context(|| "failed to encode QUIC RPC response")?;
            write_frame(&mut send, &resp_bytes).await?;
            send.finish()?;
            return Ok(());
        }
    }

    let read_only = meta.read_only.unwrap_or(false) || meta.rtt.as_deref() == Some("0rtt");
    let policy = RpcPolicy { read_only };
    let outcome = handle_rpc(&state, None, req, policy).await;

    if let Some(resp) = outcome.response {
        let resp_bytes =
            serde_json::to_vec(&resp).with_context(|| "failed to encode QUIC RPC response")?;
        write_frame(&mut send, &resp_bytes).await?;
    }

    send.finish()?;
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> anyhow::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(anyhow!("frame too large: {} bytes", len));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, data: &[u8]) -> anyhow::Result<()> {
    if data.len() > u32::MAX as usize {
        return Err(anyhow!("frame too large to send: {} bytes", data.len()));
    }
    let len = (data.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

fn load_cert_chain(
    cert_path: &PathBuf,
    key_path: &PathBuf,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs = CertificateDer::pem_file_iter(cert_path)
        .with_context(|| format!("open QUIC cert: {}", cert_path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .context("read QUIC certs")?;
    if certs.is_empty() {
        return Err(anyhow!("no certificates found in {}", cert_path.display()));
    }

    let key = PrivateKeyDer::from_pem_file(key_path)
        .with_context(|| format!("open/read QUIC key: {}", key_path.display()))?;

    Ok((certs, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_roundtrip() -> anyhow::Result<()> {
        let (mut client, mut server) = tokio::io::duplex(256);
        let payload = b"{\"skeinql\":\"1.0\"}".to_vec();

        let payload_clone = payload.clone();
        let write = tokio::spawn(async move { write_frame(&mut client, &payload_clone).await });
        let read = read_frame(&mut server).await?;
        write.await??;

        assert_eq!(payload, read);
        Ok(())
    }
}
