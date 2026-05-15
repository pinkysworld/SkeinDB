use std::collections::BTreeSet;
use std::io::BufReader;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use std::time::Instant;

use anyhow::{anyhow, Context};
use quinn::{ClientConfig, Endpoint};
use reqwest::Version;
use rustls::{pki_types::CertificateDer, RootCertStore};
use serde::Serialize;
use serde_json::json;
use skeindb_skeinql::{RpcId, RpcRequest, RpcResponse, SKEINQL_VERSION};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

static QUIC_RUSTLS_PROVIDER: OnceLock<()> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct TransportBenchOptions {
    pub http_url: String,
    pub mysql_port: u16,
    pub quic_port: u16,
    pub quic_cert: PathBuf,
    pub quic_server_name: String,
    pub concurrency: usize,
    pub requests: u64,
    pub sql: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportBenchLatencyStats {
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportBenchRun {
    pub transport: String,
    pub request_shape: String,
    pub samples: u64,
    pub latency: TransportBenchLatencyStats,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportBenchReport {
    pub sql: String,
    pub concurrency: usize,
    pub requests_per_transport: u64,
    pub results: Vec<TransportBenchRun>,
}

#[derive(Default)]
struct RunState {
    latencies: Vec<u64>,
    versions: Vec<String>,
}

pub async fn run(opts: TransportBenchOptions) -> anyhow::Result<TransportBenchReport> {
    if opts.concurrency == 0 {
        anyhow::bail!("concurrency must be greater than 0");
    }
    if opts.requests == 0 {
        anyhow::bail!("requests must be greater than 0");
    }

    let host = benchmark_host(&opts.http_url)?;
    let http2 = benchmark_http2(&opts).await?;
    let quic = benchmark_quic(&opts, &host).await?;
    let mysql = benchmark_mysql(&opts, &host).await?;

    Ok(TransportBenchReport {
        sql: opts.sql,
        concurrency: opts.concurrency,
        requests_per_transport: opts.requests,
        results: vec![http2, quic, mysql],
    })
}

pub fn print_human(report: &TransportBenchReport) {
    println!("Transport benchmark: {}", report.sql);
    println!(
        "concurrency={} requests_per_transport={}",
        report.concurrency, report.requests_per_transport
    );
    for run in &report.results {
        let version = run
            .protocol_version
            .as_deref()
            .map(|value| format!(" [{}]", value))
            .unwrap_or_default();
        println!(
            "{:10}{} p50={} p95={} p99={} mean={} (samples={})",
            run.transport,
            version,
            format_ns_ms(run.latency.p50_ns),
            format_ns_ms(run.latency.p95_ns),
            format_ns_ms(run.latency.p99_ns),
            format_ns_ms(run.latency.mean_ns as u64),
            run.samples,
        );
    }
}

fn format_ns_ms(ns: u64) -> String {
    format!("{:.3} ms", ns as f64 / 1_000_000.0)
}

fn benchmark_host(http_url: &str) -> anyhow::Result<String> {
    let url = reqwest::Url::parse(http_url).context("parse --http-url")?;
    url.host_str()
        .map(|host| host.to_string())
        .ok_or_else(|| anyhow!("--http-url must include a host"))
}

fn rpc_http_url(base: &str) -> String {
    format!("{}/api/v1/rpc", base.trim_end_matches('/'))
}

async fn benchmark_http2(opts: &TransportBenchOptions) -> anyhow::Result<TransportBenchRun> {
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .context("build HTTP/2 client")?;
    let url = rpc_http_url(&opts.http_url);
    let request_body = Arc::new(sql_exec_rpc_request_bytes(&opts.sql, "http2")?);
    let next = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(opts.concurrency);

    for _ in 0..opts.concurrency {
        let client = client.clone();
        let url = url.clone();
        let request_body = request_body.clone();
        let next = next.clone();
        let requests = opts.requests;
        handles.push(tokio::spawn(async move {
            let mut state = RunState::default();
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= requests {
                    break;
                }
                let started = Instant::now();
                let response = client
                    .post(&url)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body((*request_body).clone())
                    .send()
                    .await
                    .context("send HTTP/2 sql.exec")?;
                let version = http_version_name(response.version()).to_string();
                let status = response.status();
                let bytes = response
                    .bytes()
                    .await
                    .context("read HTTP/2 response body")?;
                if !status.is_success() {
                    anyhow::bail!("HTTP/2 sql.exec failed with status {}", status);
                }
                let parsed: RpcResponse =
                    serde_json::from_slice(&bytes).context("decode HTTP/2 rpc response")?;
                if !parsed.ok {
                    anyhow::bail!("HTTP/2 sql.exec returned rpc error: {:?}", parsed.error);
                }
                state.latencies.push(started.elapsed().as_nanos() as u64);
                state.versions.push(version);
            }
            anyhow::Ok(state)
        }));
    }

    let state = collect_states(handles).await?;
    let versions: BTreeSet<String> = state.versions.into_iter().collect();
    if versions.len() != 1 {
        anyhow::bail!(
            "HTTP/2 benchmark observed mixed protocol versions: {:?}",
            versions
        );
    }
    let version = versions
        .iter()
        .next()
        .cloned()
        .ok_or_else(|| anyhow!("HTTP/2 benchmark recorded no response versions"))?;
    if version != "HTTP/2" {
        anyhow::bail!(
            "expected HTTP/2 benchmark to negotiate HTTP/2, got {}",
            version
        );
    }

    Ok(TransportBenchRun {
        transport: "http2".to_string(),
        request_shape: "rpc:sql.exec".to_string(),
        samples: opts.requests,
        latency: latency_stats(state.latencies)?,
        protocol_version: Some(version),
    })
}

async fn benchmark_quic(
    opts: &TransportBenchOptions,
    host: &str,
) -> anyhow::Result<TransportBenchRun> {
    let (endpoint, connection) = connect_quic(opts, host).await?;
    let _endpoint = endpoint;
    let connection = Arc::new(connection);
    let request_body = Arc::new(sql_exec_rpc_request_bytes(&opts.sql, "quic")?);
    let next = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(opts.concurrency);

    for _ in 0..opts.concurrency {
        let connection = connection.clone();
        let request_body = request_body.clone();
        let next = next.clone();
        let requests = opts.requests;
        handles.push(tokio::spawn(async move {
            let mut state = RunState::default();
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= requests {
                    break;
                }
                let started = Instant::now();
                let (mut send, mut recv) =
                    connection.open_bi().await.context("open QUIC stream")?;
                write_frame(&mut send, &request_body)
                    .await
                    .context("write QUIC request frame")?;
                send.finish().context("finish QUIC stream")?;
                let bytes = read_frame(&mut recv)
                    .await
                    .context("read QUIC response frame")?;
                let parsed: RpcResponse =
                    serde_json::from_slice(&bytes).context("decode QUIC rpc response")?;
                if !parsed.ok {
                    anyhow::bail!("QUIC sql.exec returned rpc error: {:?}", parsed.error);
                }
                state.latencies.push(started.elapsed().as_nanos() as u64);
            }
            anyhow::Ok(state)
        }));
    }

    let state = collect_states(handles).await?;
    Ok(TransportBenchRun {
        transport: "quic".to_string(),
        request_shape: "rpc:sql.exec".to_string(),
        samples: opts.requests,
        latency: latency_stats(state.latencies)?,
        protocol_version: Some("QUIC".to_string()),
    })
}

async fn benchmark_mysql(
    opts: &TransportBenchOptions,
    host: &str,
) -> anyhow::Result<TransportBenchRun> {
    let next = Arc::new(AtomicU64::new(0));
    let sql = Arc::new(opts.sql.clone());
    let host = Arc::new(host.to_string());
    let mut handles = Vec::with_capacity(opts.concurrency);

    for _ in 0..opts.concurrency {
        let next = next.clone();
        let sql = sql.clone();
        let host = host.clone();
        let port = opts.mysql_port;
        let requests = opts.requests;
        handles.push(tokio::spawn(async move {
            let mut stream = mysql_connect_and_auth(host.as_str(), port)
                .await
                .context("connect MySQL benchmark worker")?;
            let mut state = RunState::default();
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= requests {
                    break;
                }
                let started = Instant::now();
                send_com_query(&mut stream, &sql)
                    .await
                    .context("send COM_QUERY")?;
                consume_mysql_response(&mut stream)
                    .await
                    .context("read COM_QUERY result")?;
                state.latencies.push(started.elapsed().as_nanos() as u64);
            }
            anyhow::Ok(state)
        }));
    }

    let state = collect_states(handles).await?;
    Ok(TransportBenchRun {
        transport: "mysql_tcp".to_string(),
        request_shape: "com_query".to_string(),
        samples: opts.requests,
        latency: latency_stats(state.latencies)?,
        protocol_version: Some("MySQL/TCP".to_string()),
    })
}

async fn collect_states(
    handles: Vec<tokio::task::JoinHandle<anyhow::Result<RunState>>>,
) -> anyhow::Result<RunState> {
    let mut state = RunState::default();
    for handle in handles {
        let mut partial = handle.await.context("join benchmark worker")??;
        state.latencies.append(&mut partial.latencies);
        state.versions.append(&mut partial.versions);
    }
    Ok(state)
}

fn latency_stats(mut samples: Vec<u64>) -> anyhow::Result<TransportBenchLatencyStats> {
    if samples.is_empty() {
        anyhow::bail!("benchmark produced no samples");
    }
    samples.sort_unstable();
    let len = samples.len();
    let sum: u128 = samples.iter().map(|value| *value as u128).sum();
    Ok(TransportBenchLatencyStats {
        min_ns: samples[0],
        p50_ns: percentile(&samples, 0.50),
        p95_ns: percentile(&samples, 0.95),
        p99_ns: percentile(&samples, 0.99),
        max_ns: samples[len - 1],
        mean_ns: sum as f64 / len as f64,
    })
}

fn percentile(samples: &[u64], percentile: f64) -> u64 {
    let last = samples.len().saturating_sub(1);
    let idx = ((last as f64) * percentile).ceil() as usize;
    samples[idx.min(last)]
}

fn ensure_rustls_provider() {
    QUIC_RUSTLS_PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

async fn connect_quic(
    opts: &TransportBenchOptions,
    host: &str,
) -> anyhow::Result<(Endpoint, quinn::Connection)> {
    ensure_rustls_provider();
    let roots = load_root_store(&opts.quic_cert)?;
    let crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .context("build QUIC client config")?,
    ));

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().context("bind QUIC client")?)?;
    endpoint.set_default_client_config(client_config);

    let addr = resolve_socket_addr(host, opts.quic_port)?;
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match endpoint.connect(addr, &opts.quic_server_name) {
            Ok(connecting) => match connecting.await {
                Ok(connection) => return Ok((endpoint, connection)),
                Err(err) if Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    let _ = err;
                }
                Err(err) => return Err(err).context("await QUIC handshake"),
            },
            Err(err) if Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let _ = err;
            }
            Err(err) => return Err(err).context("connect QUIC client"),
        }
    }
}

fn load_root_store(cert_path: &Path) -> anyhow::Result<RootCertStore> {
    let file = std::fs::File::open(cert_path).context("open QUIC cert PEM")?;
    let mut reader = BufReader::new(file);
    let mut roots = RootCertStore::empty();
    let mut loaded = 0usize;
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert: CertificateDer<'static> = cert.context("read cert from PEM")?;
        roots
            .add(cert)
            .map_err(|_| anyhow!("failed to add QUIC trust anchor from PEM"))?;
        loaded += 1;
    }
    if loaded == 0 {
        anyhow::bail!("QUIC cert PEM did not contain any certificates");
    }
    Ok(roots)
}

fn resolve_socket_addr(host: &str, port: u16) -> anyhow::Result<SocketAddr> {
    format!("{}:{}", host, port)
        .to_socket_addrs()
        .context("resolve socket address")?
        .next()
        .ok_or_else(|| anyhow!("no socket addresses resolved for {}:{}", host, port))
}

fn sql_exec_rpc_request_bytes(sql: &str, label: &str) -> anyhow::Result<Vec<u8>> {
    let request = RpcRequest {
        skeinql: SKEINQL_VERSION.to_string(),
        id: Some(RpcId::Str(format!("{}-bench", label))),
        method: "sql.exec".to_string(),
        params: Some(json!({ "sql": sql })),
    };
    serde_json::to_vec(&request).context("encode sql.exec benchmark request")
}

fn http_version_name(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/unknown",
    }
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

async fn mysql_connect_and_auth(host: &str, port: u16) -> anyhow::Result<TcpStream> {
    let mut stream = TcpStream::connect((host, port))
        .await
        .context("connect to MySQL listener")?;
    let (_seq, greeting) = read_mysql_packet(&mut stream).await?;
    if greeting.first().copied() != Some(0x0a) {
        return Err(anyhow!("unexpected MySQL greeting packet"));
    }
    write_mysql_packet(&mut stream, 1, &mysql_handshake_response_packet()).await?;
    let (_seq, auth_result) = read_mysql_packet(&mut stream).await?;
    if let Some(err) = decode_mysql_err_packet(&auth_result) {
        return Err(anyhow!("MySQL auth failed: {}", err));
    }
    if auth_result.first().copied() != Some(0x00) {
        return Err(anyhow!("unexpected MySQL auth response"));
    }
    Ok(stream)
}

async fn read_mysql_packet(stream: &mut TcpStream) -> anyhow::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok((seq, payload))
}

async fn write_mysql_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> anyhow::Result<()> {
    if payload.len() > 0x00ff_ffff {
        return Err(anyhow!("payload too large"));
    }
    let len = payload.len();
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ];
    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

fn mysql_handshake_response_packet() -> Vec<u8> {
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

    let flags =
        CLIENT_LONG_PASSWORD | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    let mut payload = Vec::new();
    payload.extend_from_slice(&flags.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.push(0x21);
    payload.extend_from_slice(&[0u8; 23]);
    payload.extend_from_slice(b"root");
    payload.push(0);
    payload.push(0);
    payload.extend_from_slice(b"mysql_native_password");
    payload.push(0);
    payload
}

async fn send_com_query(stream: &mut TcpStream, sql: &str) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_mysql_packet(stream, 0, &payload).await
}

async fn consume_mysql_response(stream: &mut TcpStream) -> anyhow::Result<()> {
    let (_seq, first_payload) = read_mysql_packet(stream).await?;
    if let Some(err) = decode_mysql_err_packet(&first_payload) {
        return Err(anyhow!("MySQL error packet: {}", err));
    }
    if first_payload.first().copied() == Some(0x00) {
        return Ok(());
    }

    let mut cursor = 0usize;
    let column_count = decode_lenenc_int(&first_payload, &mut cursor)?;
    for _ in 0..column_count {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        if let Some(err) = decode_mysql_err_packet(&payload) {
            return Err(anyhow!("MySQL column definition error: {}", err));
        }
    }

    let (_seq, eof1) = read_mysql_packet(stream).await?;
    if decode_mysql_err_packet(&eof1).is_some() {
        return Err(anyhow!("MySQL error before row stream"));
    }

    loop {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        if let Some(err) = decode_mysql_err_packet(&payload) {
            return Err(anyhow!("MySQL row error: {}", err));
        }
        if payload.first().copied() == Some(0xfe) && payload.len() < 9 {
            return Ok(());
        }
    }
}

fn decode_mysql_err_packet(payload: &[u8]) -> Option<String> {
    if payload.first().copied() != Some(0xff) || payload.len() < 3 {
        return None;
    }
    let mut cursor = 3usize;
    let mut state = None::<String>;
    if payload.get(3).copied() == Some(b'#') && payload.len() >= 9 {
        state = Some(String::from_utf8_lossy(&payload[4..9]).to_string());
        cursor = 9;
    }
    let message = String::from_utf8_lossy(payload.get(cursor..).unwrap_or_default()).to_string();
    Some(match state {
        Some(code) => format!("[{}] {}", code, message),
        None => message,
    })
}

fn decode_lenenc_int(payload: &[u8], cursor: &mut usize) -> anyhow::Result<usize> {
    if *cursor >= payload.len() {
        return Err(anyhow!("truncated lenenc int"));
    }
    let first = payload[*cursor];
    *cursor += 1;
    match first {
        0x00..=0xfa => Ok(first as usize),
        0xfc => {
            if *cursor + 2 > payload.len() {
                return Err(anyhow!("truncated lenenc int"));
            }
            let n = u16::from_le_bytes([payload[*cursor], payload[*cursor + 1]]) as usize;
            *cursor += 2;
            Ok(n)
        }
        0xfd => {
            if *cursor + 3 > payload.len() {
                return Err(anyhow!("truncated lenenc int"));
            }
            let n = (payload[*cursor] as usize)
                | ((payload[*cursor + 1] as usize) << 8)
                | ((payload[*cursor + 2] as usize) << 16);
            *cursor += 3;
            Ok(n)
        }
        0xfe => {
            if *cursor + 8 > payload.len() {
                return Err(anyhow!("truncated lenenc int"));
            }
            let n = u64::from_le_bytes([
                payload[*cursor],
                payload[*cursor + 1],
                payload[*cursor + 2],
                payload[*cursor + 3],
                payload[*cursor + 4],
                payload[*cursor + 5],
                payload[*cursor + 6],
                payload[*cursor + 7],
            ]);
            *cursor += 8;
            if n > usize::MAX as u64 {
                return Err(anyhow!("lenenc too large"));
            }
            Ok(n as usize)
        }
        _ => Err(anyhow!("invalid lenenc marker")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_stats_computes_percentiles() {
        let stats = latency_stats(vec![10, 20, 30, 40, 50]).expect("latency stats");
        assert_eq!(stats.min_ns, 10);
        assert_eq!(stats.p50_ns, 30);
        assert_eq!(stats.p95_ns, 50);
        assert_eq!(stats.p99_ns, 50);
        assert_eq!(stats.max_ns, 50);
        assert_eq!(stats.mean_ns, 30.0);
    }
}
