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
    Ok(())
}

struct HttpHarness {
    _guard: ChildGuard,
    http_port: u16,
}

impl HttpHarness {
    fn start(label: &str) -> anyhow::Result<Self> {
        let dir = temp_dir(label);
        let http_port = free_tcp_port();
        let cluster_port = free_tcp_port();
        let child = spawn_server(&dir, http_port, cluster_port)?;
        let _guard = ChildGuard::new(child);

        wait_for_health(http_port)?;

        Ok(Self { _guard, http_port })
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
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
}

fn wait_for_health(port: u16) -> anyhow::Result<()> {
    let url = format!("http://127.0.0.1:{}/health", port);
    let deadline = Instant::now() + Duration::from_secs(5);
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

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind tcp")
        .local_addr()
        .expect("tcp local addr")
        .port()
}

fn spawn_server(dir: &PathBuf, http_port: u16, cluster_port: u16) -> anyhow::Result<Child> {
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
        .arg("0")
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
