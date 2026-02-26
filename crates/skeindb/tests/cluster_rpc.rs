use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use serde_json::json;
use skeindb_skeinql::{RpcId, RpcRequest, RpcResponse, SKEINQL_VERSION};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

static CLUSTER_TEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

async fn cluster_test_guard() -> OwnedSemaphorePermit {
    let sem = CLUSTER_TEST_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone();
    sem.acquire_owned()
        .await
        .expect("acquire cluster test permit")
}

#[tokio::test]
async fn cluster_replication_ships_schema_and_rows() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let primary = HttpHarness::start("cluster_primary")?;
    let replica = HttpHarness::start("cluster_replica")?;

    let primary_client = RpcHttpClient::new(primary.base_url());
    let replica_client = RpcHttpClient::new(replica.base_url());

    let token_resp = primary_client
        .rpc("cluster.join_token.create", json!({}))
        .await?;
    assert!(token_resp.ok);
    let token = token_resp.result.expect("missing result")["token"]
        .as_str()
        .ok_or_else(|| anyhow!("missing token"))?
        .to_string();

    let join_resp = primary_client
        .rpc(
            "cluster.node.join",
            json!({
                "token": token,
                "node_id": "replica-http",
                "rpc_url": replica.base_url(),
                "role": "replica"
            }),
        )
        .await?;
    assert!(join_resp.ok);

    let resp = primary_client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    assert!(resp.ok);

    let resp = primary_client
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "users",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "name", "type": {"kind": "str"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;
    assert!(resp.ok);

    let resp = primary_client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 7}, "name": {"t": "str", "v": "Nora"}}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut replicated = false;
    while Instant::now() < deadline {
        let resp = replica_client
            .rpc(
                "data.get",
                json!({
                    "table": {"db": "app", "table": "users"},
                    "pk": [{"t": "u64", "v": 7}]
                }),
            )
            .await?;
        if resp.ok {
            if let Some(row_name) = resp
                .result
                .as_ref()
                .and_then(|v| v.get("row"))
                .and_then(|v| v.get("name"))
                .and_then(|v| v.get("v"))
                .and_then(|v| v.as_str())
            {
                if row_name == "Nora" {
                    replicated = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(replicated, "replica did not receive replicated row");

    let resp = primary_client
        .rpc(
            "schema.drop_table",
            json!({
                "db": "app",
                "table": "users"
            }),
        )
        .await?;
    assert!(resp.ok);

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut dropped = false;
    while Instant::now() < deadline {
        let resp = replica_client
            .rpc(
                "schema.list_tables",
                json!({
                    "db": "app"
                }),
            )
            .await?;
        if resp.ok {
            let tables = resp
                .result
                .as_ref()
                .and_then(|v| v.get("tables"))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if !tables.iter().any(|t| t.as_str() == Some("users")) {
                dropped = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(dropped, "replica did not apply replicated drop_table");
    Ok(())
}

#[tokio::test]
async fn sql_http_exec_endpoint_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("sql_http_exec")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({"sql":"CREATE DATABASE app"}))
        .await?;
    assert!(resp.ok);

    let resp = client
        .sql_exec(json!({
            "sql":"CREATE TABLE app.users (id BIGINT UNSIGNED NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))"
        }))
        .await?;
    assert!(resp.ok);

    let resp = client
        .sql_exec(json!({
            "sql":"INSERT INTO app.users (id, name) VALUES (7, 'Mia')"
        }))
        .await?;
    assert!(resp.ok);

    let resp = client
        .sql_exec(json!({
            "sql":"SELECT id, name FROM app.users WHERE id = 7"
        }))
        .await?;
    assert!(resp.ok);
    let rows = resp
        .result
        .as_ref()
        .and_then(|v| v.get("result"))
        .and_then(|v| v.get("data"))
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0]["v"].as_u64(), Some(7));
    assert_eq!(rows[0][1]["v"].as_str(), Some("Mia"));

    let resp = client
        .sql_exec(json!({
            "sql":"SELECT column_name FROM information_schema.columns WHERE table_schema = 'app' AND table_name = 'users' ORDER BY ordinal_position ASC"
        }))
        .await?;
    assert!(resp.ok);
    let col_rows = resp
        .result
        .as_ref()
        .and_then(|v| v.get("result"))
        .and_then(|v| v.get("data"))
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(col_rows.len(), 2);
    assert_eq!(col_rows[0][0]["v"].as_str(), Some("id"));
    assert_eq!(col_rows[1][0]["v"].as_str(), Some("name"));
    Ok(())
}

#[tokio::test]
async fn tx_rpc_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("tx_rpc_roundtrip")?;
    let client = RpcHttpClient::new(server.base_url());

    let begin = client.rpc("tx.begin", json!({"read_only": true})).await?;
    assert!(begin.ok);
    let tx_id = begin.result.expect("missing tx.begin result")["tx_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing tx_id"))?
        .to_string();

    let commit = client
        .rpc("tx.commit", json!({"tx_id": tx_id.clone()}))
        .await?;
    assert!(commit.ok);
    assert_eq!(
        commit.result.expect("missing tx.commit result")["status"].as_str(),
        Some("committed")
    );

    let rollback = client.rpc("tx.rollback", json!({"tx_id": tx_id})).await?;
    assert!(!rollback.ok);
    assert_eq!(
        rollback.error.as_ref().map(|e| e.code.as_str()),
        Some("not_found")
    );
    Ok(())
}

#[tokio::test]
async fn mysql_handshake_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_handshake_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;

    let mut stream = TcpStream::connect(("127.0.0.1", server.mysql_port()))
        .await
        .context("connect mysql port")?;
    let (seq, handshake) = read_mysql_packet(&mut stream).await?;
    assert_eq!(seq, 0);
    assert_eq!(handshake.first().copied(), Some(0x0a));
    assert!(handshake
        .windows(b"mysql_native_password".len())
        .any(|w| w == b"mysql_native_password"));

    let response = mysql_handshake_response_packet();
    write_mysql_packet(&mut stream, 1, &response).await?;

    let (_seq, auth_result) = read_mysql_packet(&mut stream).await?;
    assert_eq!(auth_result.first().copied(), Some(0x00));
    Ok(())
}

struct HttpHarness {
    _guard: ChildGuard,
    http_port: u16,
    mysql_port: u16,
}

impl HttpHarness {
    fn start(label: &str) -> anyhow::Result<Self> {
        Self::start_with_mysql_port(label, 0)
    }

    fn start_with_mysql(label: &str) -> anyhow::Result<Self> {
        let mysql_port = free_tcp_port();
        Self::start_with_mysql_port(label, mysql_port)
    }

    fn start_with_mysql_port(label: &str, mysql_port: u16) -> anyhow::Result<Self> {
        let dir = temp_dir(label);
        let http_port = free_tcp_port();
        let cluster_port = free_tcp_port();
        let child = spawn_server(&dir, http_port, cluster_port, mysql_port)?;
        let _guard = ChildGuard::new(child);

        wait_for_health(http_port)?;

        Ok(Self {
            _guard,
            http_port,
            mysql_port,
        })
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    fn mysql_port(&self) -> u16 {
        self.mysql_port
    }
}

struct RpcHttpClient {
    base_url: String,
    client: reqwest::Client,
}

impl RpcHttpClient {
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    async fn rpc(&self, method: &str, params: serde_json::Value) -> anyhow::Result<RpcResponse> {
        let req = RpcRequest {
            skeinql: SKEINQL_VERSION.to_string(),
            id: Some(RpcId::Str(format!("http-{}", method))),
            method: method.to_string(),
            params: Some(params),
        };
        let url = format!("{}/api/v1/rpc", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(url)
            .json(&req)
            .send()
            .await
            .context("send http rpc")?;
        let status = resp.status();
        let bytes = resp.bytes().await.context("read rpc response")?;
        if !status.is_success() {
            return Err(anyhow!("http rpc failed: {}", status));
        }
        let parsed: RpcResponse = serde_json::from_slice(&bytes).context("decode rpc response")?;
        Ok(parsed)
    }

    async fn sql_exec(&self, params: serde_json::Value) -> anyhow::Result<RpcResponse> {
        let url = format!("{}/api/v1/sql/exec", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(url)
            .json(&params)
            .send()
            .await
            .context("send sql exec request")?;
        let status = resp.status();
        let bytes = resp.bytes().await.context("read sql exec response")?;
        if !status.is_success() {
            return Err(anyhow!("http sql exec failed: {}", status));
        }
        let parsed: RpcResponse = serde_json::from_slice(&bytes).context("decode sql response")?;
        Ok(parsed)
    }
}

fn wait_for_health(port: u16) -> anyhow::Result<()> {
    let url = format!("http://127.0.0.1:{}/health", port);
    // CI and heavily loaded dev machines can take longer to bring up the embedded
    // HTTP server process; keep this generous to avoid flaky startup failures.
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let out = std::process::Command::new("curl")
            .arg("-sSf")
            .arg(&url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if out.map(|s| s.success()).unwrap_or(false) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(anyhow!("server did not become healthy on {}", url))
}

fn wait_for_tcp(port: u16) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(anyhow!("tcp listener did not open on {}", port))
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

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind tcp")
        .local_addr()
        .expect("tcp local addr")
        .port()
}

fn spawn_server(
    dir: &PathBuf,
    http_port: u16,
    cluster_port: u16,
    mysql_port: u16,
) -> anyhow::Result<Child> {
    let bin = env!("CARGO_BIN_EXE_skeindb");
    Command::new(bin)
        .arg("serve")
        .arg("--data")
        .arg(dir)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--http")
        .arg(http_port.to_string())
        .arg("--mysql")
        .arg(mysql_port.to_string())
        .arg("--cluster-port")
        .arg(cluster_port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn skeindb server")
}

fn temp_dir(label: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let unique = format!(
        "skeindb_cluster_test_{}_{}_{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
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
