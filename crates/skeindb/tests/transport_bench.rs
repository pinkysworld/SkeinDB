use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use rcgen::generate_simple_self_signed;
use serde_json::Value;

#[test]
fn transport_bench_reports_http2_quic_and_mysql() -> anyhow::Result<()> {
    let dir = temp_dir("transport_bench");
    let cert =
        generate_simple_self_signed(vec!["localhost".to_string()]).context("generate QUIC cert")?;
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).context("write cert pem")?;
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).context("write key pem")?;

    let http_port = free_tcp_port();
    let mysql_port = free_tcp_port();
    let cluster_port = free_tcp_port();
    let quic_port = free_udp_port();

    let child = spawn_server(
        &dir,
        http_port,
        mysql_port,
        cluster_port,
        quic_port,
        &cert_path,
        &key_path,
    )?;
    let _guard = ChildGuard::new(child);

    wait_for_health(http_port)?;
    wait_for_tcp(mysql_port)?;

    let output = Command::new(env!("CARGO_BIN_EXE_skeindb"))
        .arg("transport-bench")
        .arg("--http-url")
        .arg(format!("http://127.0.0.1:{}", http_port))
        .arg("--mysql-port")
        .arg(mysql_port.to_string())
        .arg("--quic-port")
        .arg(quic_port.to_string())
        .arg("--quic-cert")
        .arg(&cert_path)
        .arg("--quic-server-name")
        .arg("localhost")
        .arg("--concurrency")
        .arg("2")
        .arg("--requests")
        .arg("6")
        .arg("--sql")
        .arg("SELECT 1 AS one")
        .arg("--json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("run transport-bench CLI")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("transport-bench CLI failed: {}", stderr.trim());
    }

    let report: Value = serde_json::from_slice(&output.stdout).context("decode benchmark json")?;
    let results = report["results"]
        .as_array()
        .ok_or_else(|| anyhow!("benchmark report missing results array"))?;
    assert_eq!(results.len(), 3);

    let http2 = find_result(results, "http2")?;
    assert_eq!(http2["protocol_version"].as_str(), Some("HTTP/2"));
    assert_eq!(http2["samples"].as_u64(), Some(6));
    assert!(http2["latency"]["p99_ns"].as_u64().unwrap_or(0) > 0);

    let quic = find_result(results, "quic")?;
    assert_eq!(quic["protocol_version"].as_str(), Some("QUIC"));
    assert_eq!(quic["samples"].as_u64(), Some(6));
    assert!(quic["latency"]["p99_ns"].as_u64().unwrap_or(0) > 0);

    let mysql = find_result(results, "mysql_tcp")?;
    assert_eq!(mysql["protocol_version"].as_str(), Some("MySQL/TCP"));
    assert_eq!(mysql["samples"].as_u64(), Some(6));
    assert!(mysql["latency"]["p99_ns"].as_u64().unwrap_or(0) > 0);

    Ok(())
}

fn find_result<'a>(results: &'a [Value], transport: &str) -> anyhow::Result<&'a Value> {
    results
        .iter()
        .find(|value| value["transport"].as_str() == Some(transport))
        .ok_or_else(|| anyhow!("missing {} benchmark result", transport))
}

fn wait_for_health(port: u16) -> anyhow::Result<()> {
    let url = format!("http://127.0.0.1:{}/health", port);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        let ok = Command::new("curl")
            .arg("-sSf")
            .arg(&url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if ok {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(anyhow!("server did not become healthy on {}", url))
}

fn wait_for_tcp(port: u16) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(anyhow!("tcp listener did not open on {}", port))
}

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind tcp")
        .local_addr()
        .expect("tcp local addr")
        .port()
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .expect("bind udp")
        .local_addr()
        .expect("udp local addr")
        .port()
}

fn spawn_server(
    dir: &Path,
    http_port: u16,
    mysql_port: u16,
    cluster_port: u16,
    quic_port: u16,
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<Child> {
    Command::new(env!("CARGO_BIN_EXE_skeindb"))
        .arg("serve")
        .arg("--data")
        .arg(dir)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--http")
        .arg(http_port.to_string())
        .arg("--mysql")
        .arg(mysql_port.to_string())
        .arg("--pg")
        .arg("0")
        .arg("--cluster-port")
        .arg(cluster_port.to_string())
        .arg("--quic")
        .arg(quic_port.to_string())
        .arg("--quic-cert")
        .arg(cert_path)
        .arg("--quic-key")
        .arg(key_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn skeindb server")
}

fn temp_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let unique = format!(
        "skeindb_transport_bench_{}_{}_{}",
        label,
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    dir.push(unique);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
