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

    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    let mut query = Vec::new();
    query.push(0x03);
    query.extend_from_slice(b"SELECT 1 AS one, 'x' AS two");
    write_mysql_packet(&mut stream, 0, &query).await?;

    let (_seq, column_count_payload) = read_mysql_packet(&mut stream).await?;
    assert_eq!(column_count_payload.first().copied(), Some(2));

    let (_seq, _col1) = read_mysql_packet(&mut stream).await?;
    let (_seq, _col2) = read_mysql_packet(&mut stream).await?;
    let (_seq, eof1) = read_mysql_packet(&mut stream).await?;
    assert_eq!(eof1.first().copied(), Some(0xfe));

    let (_seq, row_payload) = read_mysql_packet(&mut stream).await?;
    let row = decode_mysql_text_row(&row_payload, 2)?;
    assert_eq!(row[0].as_deref(), Some("1"));
    assert_eq!(row[1].as_deref(), Some("x"));

    let (_seq, eof2) = read_mysql_packet(&mut stream).await?;
    assert_eq!(eof2.first().copied(), Some(0xfe));
    Ok(())
}

#[tokio::test]
async fn mysql_com_query_sql_exec_subset_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_com_query_sql_exec_subset")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    send_com_query(&mut stream, "CREATE DATABASE IF NOT EXISTS skein_test").await?;
    let (_seq, ok_create_db) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_db)?.0, 0);

    send_com_query(&mut stream, "USE skein_test").await?;
    let (_seq, ok_use) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_use)?.0, 0);

    send_com_query(&mut stream, "DROP TABLE IF EXISTS wp_options").await?;
    let (_seq, ok_drop) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE wp_options (option_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, option_name VARCHAR(191) NOT NULL, option_value LONGTEXT NOT NULL, autoload VARCHAR(20) NOT NULL DEFAULT 'yes', PRIMARY KEY (option_id), UNIQUE KEY option_name (option_name)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
    )
    .await?;
    let (_seq, ok_create_table) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_table)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('siteurl', 'https://example.com', 'yes')",
    )
    .await?;
    let (_seq, ok_insert) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert)?.0, 1);

    send_com_query(
        &mut stream,
        "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('siteurl', 'https://example.net', 'yes') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value), autoload = VALUES(autoload)",
    )
    .await?;
    let (_seq, ok_upsert) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_upsert)?.0, 1);

    send_com_query(
        &mut stream,
        "SELECT option_value FROM wp_options WHERE option_name = 'siteurl'",
    )
    .await?;
    let rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("https://example.net"));

    Ok(())
}

#[tokio::test]
async fn mysql_sql_calc_found_rows_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_sql_calc_found_rows_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    send_com_query(&mut stream, "CREATE DATABASE IF NOT EXISTS skein_test").await?;
    let (_seq, ok_create_db) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_db)?.0, 0);

    send_com_query(&mut stream, "USE skein_test").await?;
    let (_seq, ok_use) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_use)?.0, 0);

    send_com_query(&mut stream, "DROP TABLE IF EXISTS comments").await?;
    let (_seq, ok_drop) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE comments (id BIGINT NOT NULL, comment_approved VARCHAR(20) NOT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_table) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_table)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO comments (id, comment_approved) VALUES (1, '1'), (2, '0'), (3, '1'), (4, '1')",
    )
    .await?;
    let (_seq, ok_insert) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert)?.0, 4);

    send_com_query(
        &mut stream,
        "SELECT SQL_CALC_FOUND_ROWS id FROM comments WHERE comment_approved = '1' ORDER BY id DESC LIMIT 0, 2",
    )
    .await?;
    let rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("4"));
    assert_eq!(rows[1][0].as_deref(), Some("3"));

    send_com_query(&mut stream, "SELECT FOUND_ROWS()").await?;
    let found_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(found_rows.len(), 1);
    assert_eq!(found_rows[0][0].as_deref(), Some("3"));

    Ok(())
}

#[tokio::test]
async fn mysql_compat_corpus_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_compat_corpus_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;
    let mut txn_select_index = 0usize;

    for statement in compat_corpus_statements() {
        send_com_query(&mut stream, &statement).await?;
        let response = read_mysql_response(&mut stream)
            .await
            .with_context(|| format!("execute statement: {}", statement))?;
        let normalized = statement
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();

        match normalized.as_str() {
            "select 1" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("1"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select version()" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows[0][0].as_deref(), Some("8.0.0-skeindb"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select database()" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0], None);
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show tables from skein_test like 'wp_%'" => match response {
                MysqlResponse::Rows(rows) => assert_eq!(rows.len(), 2),
                other => panic!("expected result set, got {:?}", other),
            },
            "show table status from skein_test like 'wp_posts'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("wp_posts"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show full columns from wp_options" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 4);
                    assert_eq!(rows[0].len(), 9);
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show index from wp_posts" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert_eq!(rows[0][2].as_deref(), Some("PRIMARY"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show create table wp_posts" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    let ddl = rows[0][1].as_deref().unwrap_or_default();
                    assert!(ddl.contains("CREATE TABLE"));
                    assert!(ddl.contains("PRIMARY KEY"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select option_value from wp_options where option_name = 'siteurl'" => match response {
                MysqlResponse::Rows(rows) => {
                    let value = rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(|v| v.as_deref());
                    assert!(matches!(
                        value,
                        Some("https://example.com")
                            | Some("https://example.org")
                            | Some("https://example.net")
                    ));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select found_rows()" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows[0][0].as_deref(), Some("4"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select option_value from wp_options where option_name='txn_test'" => match response {
                MysqlResponse::Rows(rows) => {
                    let value = rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(|v| v.as_deref());
                    txn_select_index += 1;
                    let expected = match txn_select_index {
                        1 => Some("1"),
                        2 => None,
                        3 => Some("2"),
                        _ => panic!("unexpected txn_test select count"),
                    };
                    assert_eq!(value, expected);
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show status like 'threads_connected'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows[0][1].as_deref(), Some("1"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show engines" => match response {
                MysqlResponse::Rows(rows) => assert!(!rows.is_empty()),
                other => panic!("expected result set, got {:?}", other),
            },
            "show grants" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(
                        rows[0][0].as_deref(),
                        Some("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'")
                    );
                }
                other => panic!("expected result set, got {:?}", other),
            },
            _ => match response {
                MysqlResponse::Ok {
                    affected_rows,
                    last_insert_id,
                } => {
                    let _ = (affected_rows, last_insert_id);
                }
                MysqlResponse::Rows(_) => {}
            },
        }
    }
    assert_eq!(txn_select_index, 3);

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

async fn mysql_connect_and_auth(port: u16) -> anyhow::Result<TcpStream> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
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
    Ok(stream)
}

async fn send_com_query(stream: &mut TcpStream, sql: &str) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x03);
    payload.extend_from_slice(sql.as_bytes());
    write_mysql_packet(stream, 0, &payload).await
}

async fn read_mysql_text_result_rows(
    stream: &mut TcpStream,
) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    let (_seq, column_count_payload) = read_mysql_packet(stream).await?;
    if let Some(err) = decode_mysql_err_packet(&column_count_payload) {
        return Err(anyhow!("mysql error packet: {}", err));
    }
    let mut cur = 0usize;
    let column_count = decode_lenenc_int(&column_count_payload, &mut cur)?;
    for _ in 0..column_count {
        let _ = read_mysql_packet(stream).await?;
    }
    let (_seq, eof1) = read_mysql_packet(stream).await?;
    assert_eq!(eof1.first().copied(), Some(0xfe));

    let mut rows = Vec::new();
    loop {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        if payload.first().copied() == Some(0xfe) && payload.len() < 9 {
            break;
        }
        rows.push(decode_mysql_text_row(&payload, column_count)?);
    }
    Ok(rows)
}

#[derive(Debug)]
enum MysqlResponse {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    Rows(Vec<Vec<Option<String>>>),
}

async fn read_mysql_text_result_rows_after_first_packet(
    stream: &mut TcpStream,
    column_count_payload: Vec<u8>,
) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    let mut cur = 0usize;
    let column_count = decode_lenenc_int(&column_count_payload, &mut cur)?;
    for _ in 0..column_count {
        let _ = read_mysql_packet(stream).await?;
    }
    let (_seq, eof1) = read_mysql_packet(stream).await?;
    assert_eq!(eof1.first().copied(), Some(0xfe));

    let mut rows = Vec::new();
    loop {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        if payload.first().copied() == Some(0xfe) && payload.len() < 9 {
            break;
        }
        rows.push(decode_mysql_text_row(&payload, column_count)?);
    }
    Ok(rows)
}

async fn read_mysql_response(stream: &mut TcpStream) -> anyhow::Result<MysqlResponse> {
    let (_seq, first_payload) = read_mysql_packet(stream).await?;
    if let Some(err) = decode_mysql_err_packet(&first_payload) {
        return Err(anyhow!("mysql error packet: {}", err));
    }
    if first_payload.first().copied() == Some(0x00) {
        let (affected_rows, last_insert_id) = decode_mysql_ok_packet(&first_payload)?;
        return Ok(MysqlResponse::Ok {
            affected_rows,
            last_insert_id,
        });
    }
    let rows = read_mysql_text_result_rows_after_first_packet(stream, first_payload).await?;
    Ok(MysqlResponse::Rows(rows))
}

fn compat_corpus_statements() -> Vec<String> {
    let corpus = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/compat/corpus.sql"
    ));
    let mut cleaned = String::new();
    for line in corpus.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("--") {
            continue;
        }
        cleaned.push_str(line);
        cleaned.push('\n');
    }

    let mut statements = Vec::new();
    let mut current = String::new();
    let mut quote = None::<char>;
    let mut chars = cleaned.chars().peekable();
    while let Some(ch) = chars.next() {
        current.push(ch);
        match quote {
            Some(q) if ch == q => {
                if q == '\'' && chars.peek().copied() == Some('\'') {
                    current.push('\'');
                    chars.next();
                } else {
                    quote = None;
                }
            }
            Some(_) => {}
            None if ch == '\'' || ch == '"' => quote = Some(ch),
            None if ch == ';' => {
                let stmt = current.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                current.clear();
            }
            None => {}
        }
    }
    let tail = current.trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }
    statements
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

fn decode_mysql_ok_packet(payload: &[u8]) -> anyhow::Result<(u64, u64)> {
    if payload.first().copied() != Some(0x00) {
        return Err(anyhow!("not an OK packet"));
    }
    let mut cursor = 1usize;
    let affected = decode_lenenc_int(payload, &mut cursor)? as u64;
    let last_insert_id = decode_lenenc_int(payload, &mut cursor)? as u64;
    Ok((affected, last_insert_id))
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

fn decode_mysql_text_row(payload: &[u8], cols: usize) -> anyhow::Result<Vec<Option<String>>> {
    let mut out = Vec::with_capacity(cols);
    let mut cursor = 0usize;
    for _ in 0..cols {
        if cursor >= payload.len() {
            return Err(anyhow!("truncated row payload"));
        }
        if payload[cursor] == 0xfb {
            out.push(None);
            cursor += 1;
            continue;
        }
        let len = decode_lenenc_int(payload, &mut cursor)?;
        if cursor + len > payload.len() {
            return Err(anyhow!("truncated row value"));
        }
        let v = String::from_utf8(payload[cursor..cursor + len].to_vec())
            .context("decode row field utf8")?;
        cursor += len;
        out.push(Some(v));
    }
    Ok(out)
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
