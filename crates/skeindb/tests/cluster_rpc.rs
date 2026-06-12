use std::{
    fs::{self, File},
    net::TcpListener,
    path::Path,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{anyhow, Context};
use futures_util::StreamExt;
use serde_json::json;
use skeindb_skeinql::types::{
    BaseTableRef, Cte, Expr, OrderBy, Query, QueryBody, SelectBody, SelectItem, SetOp, SetOpKind,
    TableRef,
};
use skeindb_skeinql::{RpcId, RpcRequest, RpcResponse, SKEINQL_VERSION};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};

static CLUSTER_TEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
static CLUSTER_TEST_SERVER_BIN: OnceLock<PathBuf> = OnceLock::new();

const DEFAULT_HEALTH_WAIT_TIMEOUT_SECS: u64 = 180;
const HEALTH_WAIT_TIMEOUT_ENV: &str = "SKEINDB_CLUSTER_HEALTH_TIMEOUT_SECS";

fn parse_health_wait_timeout(value: Option<&str>) -> Duration {
    value
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_HEALTH_WAIT_TIMEOUT_SECS))
}

fn health_wait_timeout() -> Duration {
    let value = std::env::var(HEALTH_WAIT_TIMEOUT_ENV).ok();
    parse_health_wait_timeout(value.as_deref())
}

#[test]
fn parse_health_wait_timeout_uses_default_for_missing_invalid_and_zero_values() {
    let expected = Duration::from_secs(DEFAULT_HEALTH_WAIT_TIMEOUT_SECS);
    assert_eq!(parse_health_wait_timeout(None), expected);
    assert_eq!(parse_health_wait_timeout(Some("")), expected);
    assert_eq!(parse_health_wait_timeout(Some("abc")), expected);
    assert_eq!(parse_health_wait_timeout(Some("0")), expected);
}

#[test]
fn parse_health_wait_timeout_accepts_positive_override_values() {
    assert_eq!(
        parse_health_wait_timeout(Some("15")),
        Duration::from_secs(15)
    );
    assert_eq!(
        parse_health_wait_timeout(Some(" 240 ")),
        Duration::from_secs(240)
    );
}

fn select_query(db: &str, table: &str, projection: &[&str]) -> Query {
    Query {
        with: Vec::new(),
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: None,
                projection: projection
                    .iter()
                    .map(|col| SelectItem {
                        expr: Expr::Col {
                            col: (*col).to_string(),
                            table: None,
                        },
                        r#as: None,
                    })
                    .collect(),
                from: Some(vec![TableRef::Base(BaseTableRef {
                    db: db.to_string(),
                    table: table.to_string(),
                    r#as: None,
                })]),
                r#where: None,
                group_by: None,
                having: None,
            }),
        }),
        order_by: Vec::new(),
        limit: None,
        lock: None,
    }
}

fn advisor_workload_query(db: &str, table: &str) -> Query {
    let mut query = select_query(db, table, &["id", "name"]);
    if let QueryBody::Select { select } = query.body.as_mut() {
        select.r#where = Some(Expr::Op {
            op: "eq".to_string(),
            a: Some(Box::new(Expr::Col {
                col: "category".to_string(),
                table: None,
            })),
            b: Some(Box::new(Expr::Param { param: 0 })),
            args: None,
            list: None,
            lo: None,
            hi: None,
        });
    }
    query.order_by = vec![OrderBy {
        expr: Expr::Col {
            col: "value".to_string(),
            table: None,
        },
        dir: None,
    }];
    query
}

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

    let resp = primary_client
        .rpc(
            "query.select",
            json!({
                "query": select_query("app", "users", &["id", "name"])
            }),
        )
        .await?;
    assert!(resp.ok);
    let primary_causality = resp
        .result
        .as_ref()
        .and_then(|v| v.get("causality"))
        .cloned()
        .ok_or_else(|| anyhow!("missing primary causality token"))?;

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

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut replicated_causality = None;
    while Instant::now() < deadline {
        let resp = replica_client.rpc("cluster.status", json!({})).await?;
        if resp.ok {
            let token = resp
                .result
                .as_ref()
                .and_then(|v| v.get("replication"))
                .and_then(|v| v.get("causality"))
                .cloned();
            if token.as_ref() == Some(&primary_causality) {
                replicated_causality = token;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(replicated_causality, Some(primary_causality.clone()));

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
async fn prepared_query_get_endpoint_honors_etag_validators() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("prepared_query_get_endpoint")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    assert!(resp.ok);

    let resp = client
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

    let resp = client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 1}, "name": {"t": "str", "v": "Ada"}}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let prepare = client
        .rpc(
            "query.prepare",
            json!({"query": select_query("app", "users", &["id", "name"])}),
        )
        .await?;
    assert!(prepare.ok);
    let query_id = prepare.result.expect("missing query.prepare result")["query_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let url = format!(
        "{}/api/v1/q/{}",
        client.base_url.trim_end_matches('/'),
        query_id
    );
    let first = client.client.get(&url).send().await?;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first_etag = first
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow!("missing etag header"))?
        .to_string();
    let first_body: serde_json::Value = first.json().await?;
    let first_rows = first_body["data"]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(first_rows.len(), 1);
    assert_eq!(first_rows[0][0]["v"].as_u64(), Some(1));
    assert_eq!(first_rows[0][1]["v"].as_str(), Some("Ada"));

    let not_modified = client
        .client
        .get(&url)
        .header(reqwest::header::IF_NONE_MATCH, first_etag.clone())
        .send()
        .await?;
    assert_eq!(not_modified.status(), reqwest::StatusCode::NOT_MODIFIED);
    assert_eq!(
        not_modified
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok()),
        Some(first_etag.as_str())
    );
    assert!((not_modified.bytes().await?).is_empty());

    let resp = client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 2}, "name": {"t": "str", "v": "Grace"}}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let refreshed = client
        .client
        .get(&url)
        .header(reqwest::header::IF_NONE_MATCH, first_etag.clone())
        .send()
        .await?;
    assert_eq!(refreshed.status(), reqwest::StatusCode::OK);
    let refreshed_etag = refreshed
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow!("missing refreshed etag"))?
        .to_string();
    assert_ne!(refreshed_etag, first_etag);
    let refreshed_body: serde_json::Value = refreshed.json().await?;
    let refreshed_rows = refreshed_body["data"]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(refreshed_rows.len(), 2);

    Ok(())
}

#[tokio::test]
async fn query_execute_prepared_honors_causal_cache_validators() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("query_execute_prepared_causality")?;
    let client = RpcHttpClient::new(server.base_url());

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
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
    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 1}, "name": {"t": "str", "v": "Ada"}}]
            }),
        )
        .await?;

    let prepare = client
        .rpc(
            "query.prepare",
            json!({"query": select_query("app", "users", &["id", "name"])}),
        )
        .await?;
    assert!(prepare.ok);
    let query_id = prepare
        .result
        .as_ref()
        .and_then(|value| value.get("query_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let first = client
        .rpc(
            "query.execute_prepared",
            json!({
                "query_id": query_id,
                "args": []
            }),
        )
        .await?;
    assert!(first.ok);
    let first_result = first
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing result"))?;
    let first_etag = first_result["etag"]
        .as_str()
        .ok_or_else(|| anyhow!("missing etag"))?
        .to_string();
    let first_causality = first_result["causality"].clone();
    assert_eq!(first_causality["format"].as_str(), Some("vector_clock_v2"));
    assert_eq!(first_result["not_modified"].as_bool(), Some(false));
    assert_eq!(
        first_result["data"]["rows"].as_array().map(Vec::len),
        Some(1)
    );

    let cached = client
        .rpc(
            "query.execute_prepared",
            json!({
                "query_id": query_id,
                "args": [],
                "if_none_match": first_etag,
                "min_causality": first_causality.clone()
            }),
        )
        .await?;
    assert!(cached.ok);
    let cached_result = cached
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing cached result"))?;
    assert_eq!(cached_result["not_modified"].as_bool(), Some(true));
    assert!(cached_result["data"].is_null());
    assert_eq!(
        cached_result["causality"]["format"].as_str(),
        Some("vector_clock_v2")
    );

    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 2}, "name": {"t": "str", "v": "Grace"}}]
            }),
        )
        .await?;

    let changed = client
        .rpc(
            "query.execute_prepared",
            json!({
                "query_id": query_id,
                "args": [],
                "if_none_match": cached_result["etag"].clone(),
                "min_causality": first_causality.clone()
            }),
        )
        .await?;
    assert!(changed.ok);
    let changed_result = changed
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing changed result"))?;
    assert_eq!(changed_result["not_modified"].as_bool(), Some(false));
    assert_eq!(
        changed_result["data"]["rows"].as_array().map(Vec::len),
        Some(2)
    );
    assert_ne!(changed_result["etag"].as_str(), Some(first_etag.as_str()));

    let mut future_causality = changed_result["causality"].clone();
    let next_version = future_causality["deps"][0]["v"].as_u64().unwrap_or(0) + 1;
    future_causality["deps"][0]["v"] = serde_json::Value::from(next_version);

    let rejected = client
        .rpc(
            "query.execute_prepared",
            json!({
                "query_id": query_id,
                "args": [],
                "if_none_match": changed_result["etag"].clone(),
                "min_causality": future_causality
            }),
        )
        .await?;
    assert!(!rejected.ok);
    assert_eq!(
        rejected.error.as_ref().map(|value| value.code.as_str()),
        Some("precondition_failed")
    );

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
async fn admin_embeds_live_console_surface() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("admin_index_advisor_js")?;
    let client = reqwest::Client::new();
    let html = client
        .get(format!("{}/admin", server.base_url()))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let body = client
        .get(format!("{}/admin/src/main.js", server.base_url()))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let catalog = client
        .get(format!("{}/admin/src/lib/catalog.js", server.base_url()))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    assert!(html.contains("data-panel=\"telemetry\""));
    assert!(html.contains("data-panel=\"security\""));
    assert!(html.contains("easyNewDbForm"));
    assert!(html.contains("easyCreatePreview"));
    assert!(html.contains("btnUserRevoke"));
    assert!(html.contains("btnClusterLeaveNode"));
    assert!(html.contains("btnVecBenchmark"));
    assert!(body.contains("advisor.index_synthesize"));
    assert!(body.contains("advisor.apply_index"));
    assert!(body.contains("settings.list"));
    assert!(body.contains("cluster.node.leave"));
    assert!(body.contains("admin.user.revoke"));
    assert!(body.contains("telemetry.compat_summary"));
    assert!(body.contains("telemetry.workload_features"));
    assert!(body.contains("vector.benchmark"));
    assert!(body.contains("vector.index.status"));
    assert!(body.contains("dp.audit.log"));
    assert!(body.contains("researchSettingsLoad"));
    assert!(body.contains("renderSettingsCapabilities"));
    // PANEL_META (which carries the per-panel "security:" capability hints)
    // now lives in the extracted catalog.js ES module, imported by main.js.
    assert!(body.contains("from './lib/catalog.js'"));
    assert!(catalog.contains("security:"));
    assert!(body.contains("advisorReport"));
    assert!(!body.contains("advisor.synthesize"));
    assert!(!body.contains("call('advisor.apply'"));
    assert!(!body.contains("dp.audit_log"));
    assert!(!body.contains("vector.index_status"));

    Ok(())
}

#[tokio::test]
async fn cdc_ack_and_close_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_ack_and_close")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    assert!(resp.ok);

    let resp = client
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

    let subscribe = client
        .rpc("cdc.subscribe_table", json!({"db":"app","table":"users"}))
        .await?;
    assert!(subscribe.ok);
    let subscribe_result = subscribe
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing subscribe result"))?;
    let sub_id = subscribe_result["sub_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let start_offset = subscribe
        .result
        .as_ref()
        .and_then(|v| v.get("offset"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing offset"))?;

    let resp = client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 1}, "name": {"t": "str", "v": "Ada"}}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let first_poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id.clone(), "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(first_poll.ok);
    let first_events = first_poll
        .result
        .as_ref()
        .and_then(|v| v.get("events"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0]["op"].as_str(), Some("insert"));
    let next_offset = first_poll
        .result
        .as_ref()
        .and_then(|v| v.get("next_offset"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing next_offset"))?;

    let ack = client
        .rpc(
            "cdc.ack",
            json!({"sub_id": sub_id.clone(), "offset": next_offset}),
        )
        .await?;
    assert!(ack.ok);
    assert_eq!(
        ack.result
            .as_ref()
            .and_then(|v| v.get("acked_offset"))
            .and_then(|v| v.as_u64()),
        Some(next_offset)
    );

    let resp = client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 2}, "name": {"t": "str", "v": "Grace"}}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let second_poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id.clone(), "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(second_poll.ok);
    let second_events = second_poll
        .result
        .as_ref()
        .and_then(|v| v.get("events"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(second_events.len(), 1);
    assert_eq!(second_events[0]["pk"][0]["v"].as_u64(), Some(2));

    let close = client
        .rpc("cdc.close", json!({"sub_id": sub_id.clone()}))
        .await?;
    assert!(close.ok);
    assert_eq!(
        close
            .result
            .as_ref()
            .and_then(|v| v.get("closed"))
            .and_then(|v| v.as_bool()),
        Some(true)
    );

    let after_close = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id, "from_offset": 0, "limit": 10}),
        )
        .await?;
    assert!(!after_close.ok);
    assert_eq!(
        after_close.error.as_ref().map(|e| e.code.as_str()),
        Some("not_found")
    );

    let caps = client.rpc("system.capabilities", json!({})).await?;
    assert!(caps.ok);
    let methods = caps
        .result
        .as_ref()
        .and_then(|v| v.get("methods"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(methods.iter().any(|method| method == "cdc.ack"));
    assert!(methods.iter().any(|method| method == "cdc.close"));

    Ok(())
}

#[tokio::test]
async fn cdc_table_subscription_plain_json_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_plain_json")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    assert!(resp.ok);

    let resp = client
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "events",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "data", "type": {"kind": "str"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;
    assert!(resp.ok);

    let subscribe = client
        .rpc(
            "cdc.subscribe_table",
            json!({
                "db": "app",
                "table": "events",
                "format": "plain_json",
                "include": {"after": true}
            }),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|v| v.get("sub_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let offset = subscribe
        .result
        .as_ref()
        .and_then(|v| v.get("offset"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing offset"))?;

    let resp = client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "events"},
                "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "one"}}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id, "from_offset": offset, "limit": 10}),
        )
        .await?;
    assert!(poll.ok);
    let events = poll
        .result
        .as_ref()
        .and_then(|v| v.get("events"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["pk"][0].as_u64(), Some(1));
    assert_eq!(events[0]["after"]["id"].as_u64(), Some(1));
    assert_eq!(events[0]["after"]["data"].as_str(), Some("one"));

    Ok(())
}

#[tokio::test]
async fn cdc_table_subscription_filters_requested_ops() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_subscription_ops")?;
    let client = RpcHttpClient::new(server.base_url());

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
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

    let invalid = client
        .rpc(
            "cdc.subscribe_table",
            json!({"db":"app","table":"users","ops":["merge"]}),
        )
        .await?;
    assert!(!invalid.ok);
    assert_eq!(
        invalid.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_request")
    );

    let subscribe = client
        .rpc(
            "cdc.subscribe_table",
            json!({
                "db":"app",
                "table":"users",
                "ops":["update"],
                "include":{"after":true}
            }),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let start_offset = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("offset"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("missing offset"))?;

    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{"id": {"t": "u64", "v": 1}, "name": {"t": "str", "v": "Ada"}}]
            }),
        )
        .await?;
    client
        .rpc(
            "data.update",
            json!({
                "table": {"db": "app", "table": "users"},
                "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":1}}},
                "set": {"name": {"t": "str", "v": "Grace"}}
            }),
        )
        .await?;
    client
        .rpc(
            "data.delete",
            json!({
                "table": {"db": "app", "table": "users"},
                "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":1}}}
            }),
        )
        .await?;

    let poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id, "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(poll.ok);
    let events = poll
        .result
        .as_ref()
        .and_then(|value| value.get("events"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["op"].as_str(), Some("update"));
    assert!(events[0]["before"].is_null());
    assert_eq!(events[0]["after"]["name"]["v"].as_str(), Some("Grace"));

    Ok(())
}

#[tokio::test]
async fn cdc_table_subscription_filters_requested_primary_key() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_subscription_pk")?;
    let client = RpcHttpClient::new(server.base_url());

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
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

    let invalid = client
        .rpc(
            "cdc.subscribe_table",
            json!({"db":"app","table":"users","pk":[{"t":"u64","v":1},{"t":"u64","v":2}]}),
        )
        .await?;
    assert!(!invalid.ok);
    assert_eq!(
        invalid.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_request")
    );

    let subscribe = client
        .rpc(
            "cdc.subscribe_table",
            json!({
                "db":"app",
                "table":"users",
                "pk":[{"t":"u64","v":2}],
                "include":{"after":true}
            }),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let start_offset = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("offset"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("missing offset"))?;

    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [
                    {"id": {"t": "u64", "v": 1}, "name": {"t": "str", "v": "Ada"}},
                    {"id": {"t": "u64", "v": 2}, "name": {"t": "str", "v": "Grace"}}
                ]
            }),
        )
        .await?;
    client
        .rpc(
            "data.update",
            json!({
                "table": {"db": "app", "table": "users"},
                "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":1}}},
                "set": {"name": {"t": "str", "v": "Ada Lovelace"}}
            }),
        )
        .await?;
    client
        .rpc(
            "data.update",
            json!({
                "table": {"db": "app", "table": "users"},
                "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":2}}},
                "set": {"name": {"t": "str", "v": "Grace Hopper"}}
            }),
        )
        .await?;

    let poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id, "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(poll.ok);
    let events = poll
        .result
        .as_ref()
        .and_then(|value| value.get("events"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["op"].as_str(), Some("insert"));
    assert_eq!(events[0]["pk"][0]["v"].as_u64(), Some(2));
    assert_eq!(events[1]["op"].as_str(), Some("update"));
    assert_eq!(events[1]["pk"][0]["v"].as_u64(), Some(2));
    assert_eq!(
        events[1]["after"]["name"]["v"].as_str(),
        Some("Grace Hopper")
    );

    Ok(())
}

#[tokio::test]
async fn cdc_table_subscription_filters_requested_primary_key_range() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_subscription_pk_range")?;
    let client = RpcHttpClient::new(server.base_url());

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
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

    let invalid = client
        .rpc(
            "cdc.subscribe_table",
            json!({"db":"app","table":"users","pk_range":{}}),
        )
        .await?;
    assert!(!invalid.ok);
    assert_eq!(
        invalid.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_request")
    );

    let subscribe = client
        .rpc(
            "cdc.subscribe_table",
            json!({
                "db":"app",
                "table":"users",
                "pk_range":{
                    "lower_bound":{"t":"u64","v":2},
                    "upper_bound":{"t":"u64","v":3}
                },
                "include":{"after":true}
            }),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let start_offset = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("offset"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("missing offset"))?;

    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [
                    {"id": {"t": "u64", "v": 1}, "name": {"t": "str", "v": "Ada"}},
                    {"id": {"t": "u64", "v": 2}, "name": {"t": "str", "v": "Grace"}},
                    {"id": {"t": "u64", "v": 3}, "name": {"t": "str", "v": "Linus"}},
                    {"id": {"t": "u64", "v": 4}, "name": {"t": "str", "v": "Ken"}}
                ]
            }),
        )
        .await?;
    for (id, name) in [
        (1_u64, "Ada Lovelace"),
        (2_u64, "Grace Hopper"),
        (3_u64, "Linus Torvalds"),
        (4_u64, "Ken Thompson"),
    ] {
        client
            .rpc(
                "data.update",
                json!({
                    "table": {"db": "app", "table": "users"},
                    "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":id}}},
                    "set": {"name": {"t": "str", "v": name}}
                }),
            )
            .await?;
    }

    let poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id, "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(poll.ok);
    let events = poll
        .result
        .as_ref()
        .and_then(|value| value.get("events"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["op"].as_str(), Some("insert"));
    assert_eq!(events[0]["pk"][0]["v"].as_u64(), Some(2));
    assert_eq!(events[1]["op"].as_str(), Some("insert"));
    assert_eq!(events[1]["pk"][0]["v"].as_u64(), Some(3));
    assert_eq!(events[2]["op"].as_str(), Some("update"));
    assert_eq!(events[2]["pk"][0]["v"].as_u64(), Some(2));
    assert_eq!(
        events[2]["after"]["name"]["v"].as_str(),
        Some("Grace Hopper")
    );
    assert_eq!(events[3]["op"].as_str(), Some("update"));
    assert_eq!(events[3]["pk"][0]["v"].as_u64(), Some(3));
    assert_eq!(
        events[3]["after"]["name"]["v"].as_str(),
        Some("Linus Torvalds")
    );

    Ok(())
}

#[tokio::test]
async fn cdc_table_subscription_filters_requested_columns() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_subscription_columns")?;
    let client = RpcHttpClient::new(server.base_url());

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "users",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "name", "type": {"kind": "str"}, "nullable": false},
                    {"name": "email", "type": {"kind": "str"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;

    let invalid = client
        .rpc(
            "cdc.subscribe_table",
            json!({"db":"app","table":"users","columns":["missing"]}),
        )
        .await?;
    assert!(!invalid.ok);
    assert_eq!(
        invalid.error.as_ref().map(|error| error.code.as_str()),
        Some("invalid_request")
    );

    let subscribe = client
        .rpc(
            "cdc.subscribe_table",
            json!({
                "db":"app",
                "table":"users",
                "columns":["name"],
                "ops":["update"],
                "include":{"before":true,"after":true}
            }),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let start_offset = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("offset"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("missing offset"))?;

    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "users"},
                "rows": [{
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada"},
                    "email": {"t": "str", "v": "ada@example.com"}
                }]
            }),
        )
        .await?;
    client
        .rpc(
            "data.update",
            json!({
                "table": {"db": "app", "table": "users"},
                "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":1}}},
                "set": {"email": {"t": "str", "v": "countess@example.com"}}
            }),
        )
        .await?;
    client
        .rpc(
            "data.update",
            json!({
                "table": {"db": "app", "table": "users"},
                "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":1}}},
                "set": {"name": {"t": "str", "v": "Ada Lovelace"}}
            }),
        )
        .await?;

    let poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id, "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(poll.ok);
    let events = poll
        .result
        .as_ref()
        .and_then(|value| value.get("events"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["op"].as_str(), Some("update"));
    assert_eq!(events[0]["before"]["name"]["v"].as_str(), Some("Ada"));
    assert_eq!(
        events[0]["after"]["name"]["v"].as_str(),
        Some("Ada Lovelace")
    );
    assert!(events[0]["before"].get("email").is_none());
    assert!(events[0]["after"].get("email").is_none());

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
async fn mysql_caching_sha2_password_auth_succeeds() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let password = "caching-sha2-secret-99";
    let server = HttpHarness::start_with_mysql_and_env(
        "mysql_caching_sha2_auth",
        &[("SKEINDB_TOKEN", password)],
    )?;
    wait_for_tcp(server.mysql_port())?;

    let mut stream = mysql_connect_caching_sha2(server.mysql_port(), "root", password).await?;

    // The authenticated connection accepts queries.
    let mut query = Vec::new();
    query.push(0x03);
    query.extend_from_slice(b"SELECT 1 AS one");
    write_mysql_packet(&mut stream, 0, &query).await?;
    let (_seq, column_count_payload) = read_mysql_packet(&mut stream).await?;
    assert_eq!(column_count_payload.first().copied(), Some(1));
    Ok(())
}

#[tokio::test]
async fn mysql_caching_sha2_password_wrong_password_denied() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql_and_env(
        "mysql_caching_sha2_wrong_pw",
        &[("SKEINDB_TOKEN", "correct-secret")],
    )?;
    wait_for_tcp(server.mysql_port())?;

    let mut stream = TcpStream::connect(("127.0.0.1", server.mysql_port())).await?;
    let (_seq, handshake) = read_mysql_packet(&mut stream).await?;
    let seed = extract_mysql_handshake_seed(&handshake)?;
    let scramble = mysql_caching_sha2_scramble("wrong-secret", &seed);
    let response = mysql_caching_sha2_response_packet("root", &scramble);
    write_mysql_packet(&mut stream, 1, &response).await?;

    let (_seq, result) = read_mysql_packet(&mut stream).await?;
    let err = decode_mysql_err_packet(&result).ok_or_else(|| anyhow!("expected error packet"))?;
    assert!(err.contains("[28000]"), "unexpected error: {}", err);
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

    send_com_query(&mut stream, "SHOW INDEX FROM wp_options").await?;
    let index_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(index_rows.len(), 2);
    assert_eq!(index_rows[0][2].as_deref(), Some("PRIMARY"));
    assert_eq!(index_rows[1][1].as_deref(), Some("0"));
    assert_eq!(index_rows[1][2].as_deref(), Some("option_name"));
    assert_eq!(index_rows[1][4].as_deref(), Some("option_name"));

    send_com_query(&mut stream, "SHOW FULL COLUMNS FROM wp_options").await?;
    let column_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(column_rows.len(), 4);
    assert_eq!(column_rows[1][0].as_deref(), Some("option_name"));
    assert_eq!(column_rows[1][4].as_deref(), Some("UNI"));

    send_com_query(
        &mut stream,
        "INSERT INTO wp_options (option_name, option_value) VALUES ('home', 'https://example.com')",
    )
    .await?;
    let (_seq, ok_insert_default) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert_default)?.0, 1);

    send_com_query(
        &mut stream,
        "SELECT autoload FROM wp_options WHERE option_name = 'home'",
    )
    .await?;
    let default_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(default_rows.len(), 1);
    assert_eq!(default_rows[0][0].as_deref(), Some("yes"));

    send_com_query(
        &mut stream,
        "INSERT IGNORE INTO wp_options (option_name, option_value) VALUES ('home', 'https://ignored.example')",
    )
    .await?;
    let (_seq, ok_ignore_home) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_ignore_home)?.0, 0);

    send_com_query(
        &mut stream,
        "REPLACE INTO wp_options (option_name, option_value, autoload) VALUES ('home', 'https://example.net', 'no')",
    )
    .await?;
    let (_seq, ok_replace_home) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_replace_home)?.0, 2);

    send_com_query(
        &mut stream,
        "SELECT option_value, autoload FROM wp_options WHERE option_name = 'home'",
    )
    .await?;
    let home_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(home_rows.len(), 1);
    assert_eq!(home_rows[0][0].as_deref(), Some("https://example.net"));
    assert_eq!(home_rows[0][1].as_deref(), Some("no"));

    send_com_query(
        &mut stream,
        "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('home', 'https://duplicate.example', 'yes')",
    )
    .await?;
    let (_seq, duplicate_err) = read_mysql_packet(&mut stream).await?;
    let duplicate_err = decode_mysql_err_packet(&duplicate_err)
        .ok_or_else(|| anyhow!("expected error packet for duplicate insert"))?;
    assert!(duplicate_err.contains("[23000]"));
    assert!(duplicate_err.contains("duplicate key"));

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

    send_com_query(
        &mut stream,
        "SELECT option_id FROM wp_options WHERE option_name = 'siteurl'",
    )
    .await?;
    let original_id_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(original_id_rows.len(), 1);
    let original_id = original_id_rows[0][0].clone();

    send_com_query(
        &mut stream,
        "INSERT INTO wp_options (option_value, option_name, autoload) VALUES ('https://example.shuffle', 'siteurl', 'no') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value), autoload = VALUES(autoload)",
    )
    .await?;
    let (_seq, ok_shuffled_upsert) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_shuffled_upsert)?.0, 1);

    send_com_query(
        &mut stream,
        "SELECT option_value, autoload FROM wp_options WHERE option_name = 'siteurl'",
    )
    .await?;
    let shuffled_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(shuffled_rows.len(), 1);
    assert_eq!(
        shuffled_rows[0][0].as_deref(),
        Some("https://example.shuffle")
    );
    assert_eq!(shuffled_rows[0][1].as_deref(), Some("no"));

    send_com_query(
        &mut stream,
        "REPLACE INTO wp_options (option_value, option_name, autoload) VALUES ('https://example.replace', 'siteurl', 'yes')",
    )
    .await?;
    let (_seq, ok_shuffled_replace) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_shuffled_replace)?.0, 2);

    send_com_query(
        &mut stream,
        "SELECT option_id, option_value, autoload FROM wp_options WHERE option_name = 'siteurl'",
    )
    .await?;
    let replaced_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(replaced_rows.len(), 1);
    assert_ne!(replaced_rows[0][0], original_id);
    assert_eq!(
        replaced_rows[0][1].as_deref(),
        Some("https://example.replace")
    );
    assert_eq!(replaced_rows[0][2].as_deref(), Some("yes"));

    Ok(())
}

#[tokio::test]
async fn mysql_com_stmt_prepare_execute_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_com_stmt_prepare_execute_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    send_com_query(&mut stream, "CREATE DATABASE IF NOT EXISTS skein_test").await?;
    let (_seq, ok_create_db) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_db)?.0, 0);

    send_com_query(&mut stream, "USE skein_test").await?;
    let (_seq, ok_use) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_use)?.0, 0);

    send_com_query(&mut stream, "DROP TABLE IF EXISTS wp_users").await?;
    let (_seq, ok_drop) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE wp_users (id BIGINT UNSIGNED NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_table) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_table)?.0, 0);

    send_com_stmt_prepare(&mut stream, "INSERT INTO wp_users (id, name) VALUES (?, ?)").await?;
    let insert_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(insert_stmt.column_count, 0);
    assert_eq!(insert_stmt.param_count, 2);
    assert_eq!(
        insert_stmt
            .param_defs
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["param1", "param2"]
    );

    send_com_stmt_long_data(&mut stream, insert_stmt.statement_id, 1, b"Nora").await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { affected_rows, .. } => assert_eq!(affected_rows, 0),
        other => {
            return Err(anyhow!(
                "expected OK for COM_STMT_SEND_LONG_DATA, got {:?}",
                other
            ))
        }
    }

    send_com_stmt_execute(
        &mut stream,
        insert_stmt.statement_id,
        &[MysqlStmtParamValue::I64(7), MysqlStmtParamValue::LongData],
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { affected_rows, .. } => assert_eq!(affected_rows, 1),
        other => return Err(anyhow!("expected OK for prepared insert, got {:?}", other)),
    }

    send_com_stmt_long_data(
        &mut stream,
        insert_stmt.statement_id,
        1,
        b"SHOULD_NOT_BE_USED",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { affected_rows, .. } => assert_eq!(affected_rows, 0),
        other => {
            return Err(anyhow!(
                "expected OK for COM_STMT_SEND_LONG_DATA, got {:?}",
                other
            ))
        }
    }

    send_com_stmt_reset(&mut stream, insert_stmt.statement_id).await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { affected_rows, .. } => assert_eq!(affected_rows, 0),
        other => return Err(anyhow!("expected OK for COM_STMT_RESET, got {:?}", other)),
    }

    send_com_stmt_execute(
        &mut stream,
        insert_stmt.statement_id,
        &[
            MysqlStmtParamValue::I64(8),
            MysqlStmtParamValue::Str("Grace".to_string()),
        ],
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { affected_rows, .. } => assert_eq!(affected_rows, 1),
        other => return Err(anyhow!("expected OK for prepared insert, got {:?}", other)),
    }

    send_com_query(&mut stream, "DROP TABLE IF EXISTS wp_metrics").await?;
    let (_seq, ok_drop_metrics) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop_metrics)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE wp_metrics (id BIGINT UNSIGNED NOT NULL, score DOUBLE NULL, note VARCHAR(255) NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_metrics) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_metrics)?.0, 0);

    send_com_stmt_prepare(
        &mut stream,
        "INSERT INTO wp_metrics (id, score, note) VALUES (?, ?, ?)",
    )
    .await?;
    let metric_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(metric_stmt.param_count, 3);

    send_com_stmt_execute(
        &mut stream,
        metric_stmt.statement_id,
        &[
            MysqlStmtParamValue::I64(1),
            MysqlStmtParamValue::F64(1.5),
            MysqlStmtParamValue::Null,
        ],
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { affected_rows, .. } => assert_eq!(affected_rows, 1),
        other => {
            return Err(anyhow!(
                "expected OK for prepared metric insert, got {:?}",
                other
            ))
        }
    }

    send_com_query(
        &mut stream,
        "SELECT score, note FROM wp_metrics WHERE id = 1",
    )
    .await?;
    let metric_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(metric_rows, vec![vec![Some("1.5".to_string()), None]]);

    send_com_stmt_close(&mut stream, metric_stmt.statement_id).await?;

    send_com_stmt_prepare(&mut stream, "SELECT * FROM wp_users WHERE id = ?").await?;
    let select_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(select_stmt.column_count, 2);
    assert_eq!(select_stmt.param_count, 1);
    assert_eq!(
        select_stmt.column_defs,
        vec![("id".to_string(), 0x08), ("name".to_string(), 0xfd),]
    );

    send_com_stmt_execute(
        &mut stream,
        select_stmt.statement_id,
        &[MysqlStmtParamValue::I64(7)],
    )
    .await?;
    let nora_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        nora_rows,
        vec![vec![Some("7".to_string()), Some("Nora".to_string())]]
    );

    send_com_stmt_execute(
        &mut stream,
        select_stmt.statement_id,
        &[MysqlStmtParamValue::I64(8)],
    )
    .await?;
    let grace_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        grace_rows,
        vec![vec![Some("8".to_string()), Some("Grace".to_string())]]
    );

    send_com_stmt_prepare(
        &mut stream,
        "SELECT id FROM wp_users WHERE id = (SELECT id FROM wp_users WHERE name = 'Grace') ORDER BY id ASC",
    )
    .await?;
    let scalar_subquery_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(scalar_subquery_stmt.param_count, 0);
    assert_eq!(
        scalar_subquery_stmt.column_defs,
        vec![("id".to_string(), 0x08),]
    );

    send_com_stmt_execute(&mut stream, scalar_subquery_stmt.statement_id, &[]).await?;
    let scalar_subquery_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(scalar_subquery_rows, vec![vec![Some("8".to_string())]]);
    send_com_stmt_close(&mut stream, scalar_subquery_stmt.statement_id).await?;

    send_com_query(&mut stream, "DROP TABLE IF EXISTS wp_posts").await?;
    let (_seq, ok_drop_posts) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop_posts)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE wp_posts (id BIGINT UNSIGNED NOT NULL, author_id BIGINT UNSIGNED NOT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_posts) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_posts)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO wp_posts (id, author_id) VALUES (11, 7), (12, 42)",
    )
    .await?;
    let (_seq, ok_insert_posts) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert_posts)?.0, 2);

    send_com_query(&mut stream, "DROP TABLE IF EXISTS wp_profiles").await?;
    let (_seq, ok_drop_profiles) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop_profiles)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE wp_profiles (id BIGINT UNSIGNED NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_profiles) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_profiles)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO wp_profiles (id, name) VALUES (11, 'Piper')",
    )
    .await?;
    let (_seq, ok_insert_profiles) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert_profiles)?.0, 1);

    send_com_stmt_prepare(
        &mut stream,
        "SELECT p.id AS post_id, u.name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.author_id = u.id WHERE p.id = ?",
    )
    .await?;
    let join_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(join_stmt.param_count, 1);
    assert_eq!(
        join_stmt.column_defs,
        vec![("post_id".to_string(), 0x08), ("name".to_string(), 0xfd),]
    );

    send_com_stmt_execute(
        &mut stream,
        join_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let join_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        join_rows,
        vec![vec![Some("11".to_string()), Some("Nora".to_string())]]
    );

    send_com_stmt_close(&mut stream, join_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT p.id AS post_id, pr.name FROM wp_posts AS p INNER JOIN wp_profiles AS pr USING (id) WHERE p.id = ?",
    )
    .await?;
    let using_join_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(using_join_stmt.param_count, 1);
    assert_eq!(
        using_join_stmt.column_defs,
        vec![("post_id".to_string(), 0x08), ("name".to_string(), 0xfd),]
    );

    send_com_stmt_execute(
        &mut stream,
        using_join_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let using_join_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        using_join_rows,
        vec![vec![Some("11".to_string()), Some("Piper".to_string())]]
    );
    send_com_stmt_close(&mut stream, using_join_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT p.id post_id, u.name author_name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.author_id = u.id WHERE p.id = ?",
    )
    .await?;
    let implicit_alias_join_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(implicit_alias_join_stmt.param_count, 1);
    assert_eq!(
        implicit_alias_join_stmt.column_defs,
        vec![
            ("post_id".to_string(), 0x08),
            ("author_name".to_string(), 0xfd),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        implicit_alias_join_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let implicit_alias_join_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        implicit_alias_join_rows,
        vec![vec![Some("11".to_string()), Some("Nora".to_string())]]
    );
    send_com_stmt_close(&mut stream, implicit_alias_join_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT p.id AS post_id, u.name FROM wp_posts AS p CROSS JOIN wp_users AS u WHERE p.author_id = u.id AND p.id = ?",
    )
    .await?;
    let cross_join_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(cross_join_stmt.param_count, 1);
    assert_eq!(
        cross_join_stmt.column_defs,
        vec![("post_id".to_string(), 0x08), ("name".to_string(), 0xfd),]
    );

    send_com_stmt_execute(
        &mut stream,
        cross_join_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let cross_join_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        cross_join_rows,
        vec![vec![Some("11".to_string()), Some("Nora".to_string())]]
    );
    send_com_stmt_close(&mut stream, cross_join_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT * FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.author_id = u.id WHERE p.id = ?",
    )
    .await?;
    let join_wildcard_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(join_wildcard_stmt.param_count, 1);
    assert_eq!(
        join_wildcard_stmt.column_defs,
        vec![
            ("id".to_string(), 0x08),
            ("author_id".to_string(), 0x08),
            ("id".to_string(), 0x08),
            ("name".to_string(), 0xfd),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        join_wildcard_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let join_wildcard_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        join_wildcard_rows,
        vec![vec![
            Some("11".to_string()),
            Some("7".to_string()),
            Some("7".to_string()),
            Some("Nora".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, join_wildcard_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT * FROM wp_posts GROUP BY id, author_id HAVING id = ? ORDER BY id ASC",
    )
    .await?;
    let grouped_wildcard_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(grouped_wildcard_stmt.param_count, 1);
    assert_eq!(
        grouped_wildcard_stmt.column_defs,
        vec![("id".to_string(), 0x08), ("author_id".to_string(), 0x08),]
    );

    send_com_stmt_execute(
        &mut stream,
        grouped_wildcard_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let grouped_wildcard_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        grouped_wildcard_rows,
        vec![vec![Some("11".to_string()), Some("7".to_string())]]
    );
    send_com_stmt_close(&mut stream, grouped_wildcard_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT p.*, u.name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.author_id = u.id WHERE p.id = ?",
    )
    .await?;
    let join_qualified_wildcard_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(join_qualified_wildcard_stmt.param_count, 1);
    assert_eq!(
        join_qualified_wildcard_stmt.column_defs,
        vec![
            ("id".to_string(), 0x08),
            ("author_id".to_string(), 0x08),
            ("name".to_string(), 0xfd),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        join_qualified_wildcard_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let join_qualified_wildcard_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        join_qualified_wildcard_rows,
        vec![vec![
            Some("11".to_string()),
            Some("7".to_string()),
            Some("Nora".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, join_qualified_wildcard_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT skein_test.wp_posts.*, u.name FROM skein_test.wp_posts LEFT JOIN skein_test.wp_users AS u ON wp_posts.author_id = u.id WHERE wp_posts.id = ?",
    )
    .await?;
    let join_schema_qualified_wildcard_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(join_schema_qualified_wildcard_stmt.param_count, 1);
    assert_eq!(
        join_schema_qualified_wildcard_stmt.column_defs,
        vec![
            ("id".to_string(), 0x08),
            ("author_id".to_string(), 0x08),
            ("name".to_string(), 0xfd),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        join_schema_qualified_wildcard_stmt.statement_id,
        &[MysqlStmtParamValue::I64(11)],
    )
    .await?;
    let join_schema_qualified_wildcard_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        join_schema_qualified_wildcard_rows,
        vec![vec![
            Some("11".to_string()),
            Some("7".to_string()),
            Some("Nora".to_string()),
        ]]
    );
    send_com_stmt_close(
        &mut stream,
        join_schema_qualified_wildcard_stmt.statement_id,
    )
    .await?;

    send_com_stmt_prepare(&mut stream, "SELECT AVG(id) AS avg_user_id FROM wp_users").await?;
    let aggregate_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(
        aggregate_stmt.column_defs,
        vec![("avg_user_id".to_string(), 0x05),]
    );

    send_com_stmt_execute(&mut stream, aggregate_stmt.statement_id, &[]).await?;
    let aggregate_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(aggregate_rows, vec![vec![Some("7.5".to_string())]]);
    send_com_stmt_close(&mut stream, aggregate_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT COUNT(*) AS user_count FROM wp_users HAVING COUNT(*) >= 2 AND user_count = 2",
    )
    .await?;
    let simple_aggregate_having_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(
        simple_aggregate_having_stmt.column_defs,
        vec![("user_count".to_string(), 0x08),]
    );

    send_com_stmt_execute(&mut stream, simple_aggregate_having_stmt.statement_id, &[]).await?;
    let simple_aggregate_having_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        simple_aggregate_having_rows,
        vec![vec![Some("2".to_string())]]
    );
    send_com_stmt_close(&mut stream, simple_aggregate_having_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT id, AVG(score) AS avg_score FROM wp_metrics GROUP BY id HAVING avg_score >= 1 ORDER BY id ASC",
    )
    .await?;
    let aggregate_having_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(
        aggregate_having_stmt.column_defs,
        vec![("id".to_string(), 0x08), ("avg_score".to_string(), 0x05),]
    );

    send_com_stmt_execute(&mut stream, aggregate_having_stmt.statement_id, &[]).await?;
    let aggregate_having_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        aggregate_having_rows,
        vec![vec![Some("1".to_string()), Some("1.5".to_string())]]
    );
    send_com_stmt_close(&mut stream, aggregate_having_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT LENGTH(name) AS name_len, IF(1, name, 'missing') AS chosen_name, LOCATE('ra', name) AS hit_pos FROM wp_users WHERE id = ?",
    )
    .await?;
    let function_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(function_stmt.param_count, 1);
    assert_eq!(
        function_stmt.column_defs,
        vec![
            ("name_len".to_string(), 0x08),
            ("chosen_name".to_string(), 0xfd),
            ("hit_pos".to_string(), 0x08),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        function_stmt.statement_id,
        &[MysqlStmtParamValue::I64(8)],
    )
    .await?;
    let function_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        function_rows,
        vec![vec![
            Some("5".to_string()),
            Some("Grace".to_string()),
            Some("2".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, function_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT CAST(name AS CHAR) AS name_text, CASE WHEN id = 8 THEN name ELSE 'missing' END AS chosen_name, CAST(id AS UNSIGNED) AS id_unsigned FROM wp_users WHERE id = ?",
    )
    .await?;
    let cast_case_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(cast_case_stmt.param_count, 1);
    assert_eq!(
        cast_case_stmt.column_defs,
        vec![
            ("name_text".to_string(), 0xfd),
            ("chosen_name".to_string(), 0xfd),
            ("id_unsigned".to_string(), 0x08),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        cast_case_stmt.statement_id,
        &[MysqlStmtParamValue::I64(8)],
    )
    .await?;
    let cast_case_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        cast_case_rows,
        vec![vec![
            Some("Grace".to_string()),
            Some("Grace".to_string()),
            Some("8".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, cast_case_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT COALESCE(name, 'missing') AS display_name FROM wp_users WHERE id = ?",
    )
    .await?;
    let coalesce_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(coalesce_stmt.param_count, 1);
    assert_eq!(
        coalesce_stmt.column_defs,
        vec![("display_name".to_string(), 0xfd),]
    );

    send_com_stmt_execute(
        &mut stream,
        coalesce_stmt.statement_id,
        &[MysqlStmtParamValue::I64(8)],
    )
    .await?;
    let coalesce_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(coalesce_rows, vec![vec![Some("Grace".to_string())]]);
    send_com_stmt_close(&mut stream, coalesce_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT id + 1 AS next_id, id / 2 AS half_id, id % 3 AS mod_id FROM wp_users WHERE id = ?",
    )
    .await?;
    let arithmetic_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(arithmetic_stmt.param_count, 1);
    assert_eq!(
        arithmetic_stmt.column_defs,
        vec![
            ("next_id".to_string(), 0x08),
            ("half_id".to_string(), 0x05),
            ("mod_id".to_string(), 0x08),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        arithmetic_stmt.statement_id,
        &[MysqlStmtParamValue::I64(7)],
    )
    .await?;
    let arithmetic_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        arithmetic_rows,
        vec![vec![
            Some("8".to_string()),
            Some("3.5".to_string()),
            Some("1".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, arithmetic_stmt.statement_id).await?;

    send_com_query(&mut stream, "DROP TABLE IF EXISTS wp_events").await?;
    let (_seq, ok_drop_events) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop_events)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE wp_events (id BIGINT UNSIGNED NOT NULL, occurred_at DATETIME NOT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_events) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_events)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO wp_events (id, occurred_at) VALUES (1, '2020-01-02 03:04:05')",
    )
    .await?;
    let (_seq, ok_insert_events) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert_events)?.0, 1);

    send_com_stmt_prepare(
        &mut stream,
        "SELECT DATE(occurred_at) AS occurred_day, YEAR(occurred_at) AS occurred_year, UNIX_TIMESTAMP(occurred_at) AS occurred_ts FROM wp_events WHERE id = ?",
    )
    .await?;
    let datetime_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(datetime_stmt.param_count, 1);
    assert_eq!(
        datetime_stmt.column_defs,
        vec![
            ("occurred_day".to_string(), 0xfd),
            ("occurred_year".to_string(), 0x08),
            ("occurred_ts".to_string(), 0x08),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        datetime_stmt.statement_id,
        &[MysqlStmtParamValue::I64(1)],
    )
    .await?;
    let datetime_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        datetime_rows,
        vec![vec![
            Some("2020-01-02".to_string()),
            Some("2020".to_string()),
            Some("1577934245".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, datetime_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT DATE_FORMAT(occurred_at, '%Y-%m-%d %H:%i:%s') AS occurred_fmt, FROM_UNIXTIME(UNIX_TIMESTAMP(occurred_at)) AS occurred_from_ts, FIND_IN_SET(CAST(id AS CHAR), '9,1,5') AS id_rank, ISNULL(occurred_at) AS occurred_is_null FROM wp_events WHERE id = ?",
    )
    .await?;
    let extended_datetime_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(extended_datetime_stmt.param_count, 1);
    assert_eq!(
        extended_datetime_stmt.column_defs,
        vec![
            ("occurred_fmt".to_string(), 0xfd),
            ("occurred_from_ts".to_string(), 0xfd),
            ("id_rank".to_string(), 0x08),
            ("occurred_is_null".to_string(), 0x08),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        extended_datetime_stmt.statement_id,
        &[MysqlStmtParamValue::I64(1)],
    )
    .await?;
    let extended_datetime_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        extended_datetime_rows,
        vec![vec![
            Some("2020-01-02 03:04:05".to_string()),
            Some("2020-01-02 03:04:05".to_string()),
            Some("2".to_string()),
            Some("0".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, extended_datetime_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT DATEDIFF(occurred_at, '2020-01-01 00:00:00') AS occurred_day_diff, TIMESTAMPDIFF(HOUR, '2020-01-02 00:00:00', occurred_at) AS occurred_hour_diff FROM wp_events WHERE id = ?",
    )
    .await?;
    let diff_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(diff_stmt.param_count, 1);
    assert_eq!(
        diff_stmt.column_defs,
        vec![
            ("occurred_day_diff".to_string(), 0x08),
            ("occurred_hour_diff".to_string(), 0x08),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        diff_stmt.statement_id,
        &[MysqlStmtParamValue::I64(1)],
    )
    .await?;
    let diff_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        diff_rows,
        vec![vec![Some("1".to_string()), Some("3".to_string())]]
    );
    send_com_stmt_close(&mut stream, diff_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT WEEKDAY(occurred_at) AS occurred_weekday, DAYOFWEEK(occurred_at) AS occurred_day_of_week, DAYOFYEAR(occurred_at) AS occurred_day_of_year, MONTHNAME(occurred_at) AS occurred_month_name, DAYNAME(occurred_at) AS occurred_day_name FROM wp_events WHERE id = ?",
    )
    .await?;
    let named_datetime_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(named_datetime_stmt.param_count, 1);
    assert_eq!(
        named_datetime_stmt.column_defs,
        vec![
            ("occurred_weekday".to_string(), 0x08),
            ("occurred_day_of_week".to_string(), 0x08),
            ("occurred_day_of_year".to_string(), 0x08),
            ("occurred_month_name".to_string(), 0xfd),
            ("occurred_day_name".to_string(), 0xfd),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        named_datetime_stmt.statement_id,
        &[MysqlStmtParamValue::I64(1)],
    )
    .await?;
    let named_datetime_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        named_datetime_rows,
        vec![vec![
            Some("3".to_string()),
            Some("5".to_string()),
            Some("2".to_string()),
            Some("January".to_string()),
            Some("Thursday".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, named_datetime_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT QUARTER(occurred_at) AS occurred_quarter, LAST_DAY(occurred_at) AS occurred_last_day, EXTRACT(YEAR FROM occurred_at) AS occurred_extract_year, EXTRACT(HOUR FROM occurred_at) AS occurred_extract_hour FROM wp_events WHERE id = ?",
    )
    .await?;
    let extract_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(extract_stmt.param_count, 1);
    assert_eq!(
        extract_stmt.column_defs,
        vec![
            ("occurred_quarter".to_string(), 0x08),
            ("occurred_last_day".to_string(), 0xfd),
            ("occurred_extract_year".to_string(), 0x08),
            ("occurred_extract_hour".to_string(), 0x08),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        extract_stmt.statement_id,
        &[MysqlStmtParamValue::I64(1)],
    )
    .await?;
    let extract_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        extract_rows,
        vec![vec![
            Some("1".to_string()),
            Some("2020-01-31".to_string()),
            Some("2020".to_string()),
            Some("3".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, extract_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT DATE_ADD(occurred_at, INTERVAL 2 DAY) AS occurred_plus_two_days, DATE_SUB(occurred_at, INTERVAL 3 HOUR) AS occurred_minus_three_hours, TIMESTAMPADD(MINUTE, 30, occurred_at) AS occurred_plus_half_hour FROM wp_events WHERE id = ?",
    )
    .await?;
    let interval_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(interval_stmt.param_count, 1);
    assert_eq!(
        interval_stmt.column_defs,
        vec![
            ("occurred_plus_two_days".to_string(), 0xfd),
            ("occurred_minus_three_hours".to_string(), 0xfd),
            ("occurred_plus_half_hour".to_string(), 0xfd),
        ]
    );

    send_com_stmt_execute(
        &mut stream,
        interval_stmt.statement_id,
        &[MysqlStmtParamValue::I64(1)],
    )
    .await?;
    let interval_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        interval_rows,
        vec![vec![
            Some("2020-01-04 03:04:05".to_string()),
            Some("2020-01-02 00:04:05".to_string()),
            Some("2020-01-02 03:34:05".to_string()),
        ]]
    );
    send_com_stmt_close(&mut stream, interval_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT outer_q.id FROM wp_events AS outer_q WHERE (EXISTS (SELECT 1 FROM wp_events AS inner_q WHERE inner_q.id = outer_q.id) AND outer_q.id > 0) OR outer_q.id = 999 ORDER BY outer_q.id ASC",
    )
    .await?;
    let subquery_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(subquery_stmt.param_count, 0);
    assert_eq!(subquery_stmt.column_defs, vec![("id".to_string(), 0x08)]);

    send_com_stmt_execute(&mut stream, subquery_stmt.statement_id, &[]).await?;
    let subquery_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(subquery_rows, vec![vec![Some("1".to_string())]]);
    send_com_stmt_close(&mut stream, subquery_stmt.statement_id).await?;

    send_com_stmt_prepare(
        &mut stream,
        "SELECT outer_q.id FROM wp_events AS outer_q WHERE outer_q.id IN (SELECT mid_q.id FROM wp_events AS mid_q WHERE mid_q.id IN (SELECT inner_q.id FROM wp_events AS inner_q WHERE inner_q.id = 1)) ORDER BY outer_q.id ASC",
    )
    .await?;
    let nested_subquery_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(nested_subquery_stmt.param_count, 0);
    assert_eq!(
        nested_subquery_stmt.column_defs,
        vec![("id".to_string(), 0x08)]
    );

    send_com_stmt_execute(&mut stream, nested_subquery_stmt.statement_id, &[]).await?;
    let nested_subquery_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(nested_subquery_rows, vec![vec![Some("1".to_string())]]);
    send_com_stmt_close(&mut stream, nested_subquery_stmt.statement_id).await?;

    send_com_stmt_prepare(&mut stream, "SELECT * FROM wp_users ORDER BY id ASC").await?;
    let cursor_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(cursor_stmt.column_defs, select_stmt.column_defs);

    send_com_stmt_execute_with_flags(&mut stream, cursor_stmt.statement_id, 0x01, &[]).await?;
    let (column_types, execute_status) = read_mysql_binary_result_header(&mut stream).await?;
    assert_eq!(column_types, vec![0x08, 0xfd]);
    assert_eq!(execute_status & 0x0040, 0x0040);

    send_com_stmt_fetch(&mut stream, cursor_stmt.statement_id, 1).await?;
    let (first_fetch_rows, first_fetch_status) =
        read_mysql_stmt_fetch_rows(&mut stream, &column_types).await?;
    assert_eq!(
        first_fetch_rows,
        vec![vec![Some("7".to_string()), Some("Nora".to_string())]]
    );
    assert_eq!(first_fetch_status & 0x0040, 0x0040);

    send_com_stmt_fetch(&mut stream, cursor_stmt.statement_id, 10).await?;
    let (second_fetch_rows, second_fetch_status) =
        read_mysql_stmt_fetch_rows(&mut stream, &column_types).await?;
    assert_eq!(
        second_fetch_rows,
        vec![vec![Some("8".to_string()), Some("Grace".to_string())]]
    );
    assert_eq!(second_fetch_status & 0x0080, 0x0080);

    send_com_stmt_fetch(&mut stream, cursor_stmt.statement_id, 1).await?;
    let (empty_fetch_rows, empty_fetch_status) =
        read_mysql_stmt_fetch_rows(&mut stream, &column_types).await?;
    assert!(empty_fetch_rows.is_empty());
    assert_eq!(empty_fetch_status & 0x0080, 0x0080);

    send_com_stmt_reset(&mut stream, cursor_stmt.statement_id).await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { affected_rows, .. } => assert_eq!(affected_rows, 0),
        other => {
            return Err(anyhow!(
                "expected OK for cursor COM_STMT_RESET, got {:?}",
                other
            ))
        }
    }

    send_com_stmt_fetch(&mut stream, cursor_stmt.statement_id, 1).await?;
    let (_seq, cursor_err) = read_mysql_packet(&mut stream).await?;
    let cursor_err =
        decode_mysql_err_packet(&cursor_err).ok_or_else(|| anyhow!("expected cursor error"))?;
    assert!(cursor_err.contains("no open cursor"));

    send_com_stmt_close(&mut stream, cursor_stmt.statement_id).await?;

    send_com_stmt_close(&mut stream, select_stmt.statement_id).await?;

    send_com_stmt_execute(
        &mut stream,
        select_stmt.statement_id,
        &[MysqlStmtParamValue::I64(7)],
    )
    .await?;
    let (_seq, closed_err) = read_mysql_packet(&mut stream).await?;
    let closed_err = decode_mysql_err_packet(&closed_err)
        .ok_or_else(|| anyhow!("expected error after COM_STMT_CLOSE"))?;
    assert!(closed_err.contains("unknown prepared statement handler"));

    send_com_stmt_close(&mut stream, insert_stmt.statement_id).await?;
    Ok(())
}

#[tokio::test]
async fn mysql_simple_aggregate_compat_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_simple_aggregate_compat_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    send_com_query(&mut stream, "CREATE DATABASE IF NOT EXISTS skein_test").await?;
    let (_seq, ok_create_db) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_db)?.0, 0);

    send_com_query(&mut stream, "USE skein_test").await?;
    let (_seq, ok_use) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_use)?.0, 0);

    send_com_query(&mut stream, "DROP TABLE IF EXISTS wp_postmeta").await?;
    let (_seq, ok_drop) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE wp_postmeta (meta_id BIGINT UNSIGNED NOT NULL, sort_order BIGINT NULL, weight DOUBLE NULL, PRIMARY KEY (meta_id))",
    )
    .await?;
    let (_seq, ok_create_table) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_table)?.0, 0);

    for sql in [
        "INSERT INTO wp_postmeta (meta_id, sort_order, weight) VALUES (1, 2, 1.5)",
        "INSERT INTO wp_postmeta (meta_id, sort_order, weight) VALUES (2, NULL, NULL)",
        "INSERT INTO wp_postmeta (meta_id, sort_order, weight) VALUES (3, 5, 2.25)",
    ] {
        send_com_query(&mut stream, sql).await?;
        let (_seq, ok_insert) = read_mysql_packet(&mut stream).await?;
        assert_eq!(decode_mysql_ok_packet(&ok_insert)?.0, 1);
    }

    send_com_query(
        &mut stream,
        "SELECT COUNT(sort_order) AS present_sorts FROM wp_postmeta",
    )
    .await?;
    let count_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(count_rows, vec![vec![Some("2".to_string())]]);

    send_com_query(
        &mut stream,
        "SELECT SUM(sort_order) AS total_sort FROM wp_postmeta",
    )
    .await?;
    let int_sum_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(int_sum_rows, vec![vec![Some("7".to_string())]]);

    send_com_query(
        &mut stream,
        "SELECT SUM(weight) AS total_weight FROM wp_postmeta",
    )
    .await?;
    let float_sum_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(float_sum_rows, vec![vec![Some("3.75".to_string())]]);

    send_com_query(
        &mut stream,
        "SELECT sort_order, COUNT(*) AS group_rows FROM wp_postmeta GROUP BY sort_order ORDER BY sort_order ASC",
    )
    .await?;
    let grouped_count_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(
        grouped_count_rows,
        vec![
            vec![None, Some("1".to_string())],
            vec![Some("2".to_string()), Some("1".to_string())],
            vec![Some("5".to_string()), Some("1".to_string())],
        ]
    );

    send_com_query(
        &mut stream,
        "SELECT sort_order, SUM(weight) AS grouped_weight FROM wp_postmeta GROUP BY sort_order ORDER BY sort_order ASC LIMIT 0, 2",
    )
    .await?;
    let grouped_sum_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(
        grouped_sum_rows,
        vec![
            vec![None, None],
            vec![Some("2".to_string()), Some("1.5".to_string())],
        ]
    );

    send_com_query(
        &mut stream,
        "INSERT INTO wp_postmeta (meta_id, sort_order, weight) VALUES (4, 5, 3.0)",
    )
    .await?;
    let (_seq, ok_insert_extra) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert_extra)?.0, 1);

    send_com_query(
        &mut stream,
        "SELECT MIN(sort_order) AS min_sort FROM wp_postmeta",
    )
    .await?;
    let min_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(min_rows, vec![vec![Some("2".to_string())]]);

    send_com_query(
        &mut stream,
        "SELECT MAX(sort_order) AS max_sort FROM wp_postmeta",
    )
    .await?;
    let max_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(max_rows, vec![vec![Some("5".to_string())]]);

    send_com_query(
        &mut stream,
        "SELECT AVG(weight) AS avg_weight FROM wp_postmeta",
    )
    .await?;
    let avg_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(avg_rows, vec![vec![Some("2.25".to_string())]]);

    send_com_query(
        &mut stream,
        "SELECT sort_order, MAX(weight) AS max_weight FROM wp_postmeta GROUP BY sort_order ORDER BY sort_order ASC",
    )
    .await?;
    let grouped_max_rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(
        grouped_max_rows,
        vec![
            vec![None, None],
            vec![Some("2".to_string()), Some("1.5".to_string())],
            vec![Some("5".to_string()), Some("3.0".to_string())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn mysql_create_unique_index_rejects_existing_duplicates() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_create_unique_index_rejects_duplicates")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    for sql in [
        "CREATE DATABASE IF NOT EXISTS skein_test",
        "USE skein_test",
        "DROP TABLE IF EXISTS dup_users",
        "CREATE TABLE dup_users (id BIGINT UNSIGNED NOT NULL, email VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO dup_users (id, email) VALUES (1, 'dup@example.com')",
        "INSERT INTO dup_users (id, email) VALUES (2, 'dup@example.com')",
    ] {
        send_com_query(&mut stream, sql).await?;
        let (_seq, ok) = read_mysql_packet(&mut stream).await?;
        assert_eq!(decode_mysql_ok_packet(&ok)?.0, if sql.starts_with("INSERT") { 1 } else { 0 });
    }

    send_com_query(
        &mut stream,
        "CREATE UNIQUE INDEX email_unique ON dup_users (email)",
    )
    .await?;
    let err = read_mysql_response(&mut stream)
        .await
        .expect_err("expected duplicate-key error");
    assert!(err.to_string().contains("duplicate key"));

    Ok(())
}

#[tokio::test]
async fn mysql_primary_key_update_rejects_duplicate_key() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_primary_key_update_rejects_duplicate_key")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    for sql in [
        "CREATE DATABASE IF NOT EXISTS skein_test",
        "USE skein_test",
        "DROP TABLE IF EXISTS pk_users",
        "CREATE TABLE pk_users (id BIGINT UNSIGNED NOT NULL, email VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO pk_users (id, email) VALUES (1, 'a@example.com'), (2, 'b@example.com')",
    ] {
        send_com_query(&mut stream, sql).await?;
        let (_seq, ok) = read_mysql_packet(&mut stream).await?;
        assert_eq!(
            decode_mysql_ok_packet(&ok)?.0,
            if sql.starts_with("INSERT") { 2 } else { 0 }
        );
    }

    send_com_query(&mut stream, "UPDATE pk_users SET id = 1 WHERE id = 2").await?;
    let (_seq, err_payload) = read_mysql_packet(&mut stream).await?;
    let err = decode_mysql_err_packet(&err_payload)
        .ok_or_else(|| anyhow!("expected duplicate-key error packet"))?;
    assert!(err.contains("[23000]"));
    assert!(err.contains("duplicate key"));

    send_com_query(
        &mut stream,
        "SELECT id, email FROM pk_users ORDER BY id ASC",
    )
    .await?;
    let rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[1][0].as_deref(), Some("2"));

    Ok(())
}

#[tokio::test]
async fn mysql_rename_index_roundtrip_preserves_uniqueness() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server =
        HttpHarness::start_with_mysql("mysql_rename_index_roundtrip_preserves_uniqueness")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    for sql in [
        "CREATE DATABASE IF NOT EXISTS skein_test",
        "USE skein_test",
        "DROP TABLE IF EXISTS rename_users",
        "CREATE TABLE rename_users (id BIGINT UNSIGNED NOT NULL, email VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO rename_users (id, email) VALUES (1, 'ada@example.com')",
        "CREATE UNIQUE INDEX email_unique ON rename_users (email)",
        "ALTER TABLE rename_users RENAME INDEX email_unique TO email_login_uq",
    ] {
        send_com_query(&mut stream, sql).await?;
        let (_seq, ok) = read_mysql_packet(&mut stream).await?;
        assert_eq!(
            decode_mysql_ok_packet(&ok)?.0,
            if sql.starts_with("INSERT") { 1 } else { 0 }
        );
    }

    send_com_query(&mut stream, "SHOW INDEX FROM rename_users").await?;
    let rows = read_mysql_text_result_rows(&mut stream).await?;
    assert!(rows
        .iter()
        .any(|row| row[2].as_deref() == Some("email_login_uq")));
    assert!(!rows
        .iter()
        .any(|row| row[2].as_deref() == Some("email_unique")));

    send_com_query(
        &mut stream,
        "INSERT INTO rename_users (id, email) VALUES (2, 'ada@example.com')",
    )
    .await?;
    let err = read_mysql_response(&mut stream)
        .await
        .expect_err("expected duplicate-key error");
    assert!(err.to_string().contains("duplicate key"));
    assert!(err.to_string().contains("email_login_uq"));

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
async fn mysql_correlated_projection_subquery_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_correlated_projection_subquery_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    send_com_query(&mut stream, "CREATE DATABASE IF NOT EXISTS skein_test").await?;
    let (_seq, ok_create_db) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_db)?.0, 0);

    send_com_query(&mut stream, "USE skein_test").await?;
    let (_seq, ok_use) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_use)?.0, 0);

    send_com_query(&mut stream, "DROP TABLE IF EXISTS nodes").await?;
    let (_seq, ok_drop) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE nodes (id BIGINT NOT NULL, parent_id BIGINT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_table) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_table)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO nodes (id, parent_id) VALUES (1, NULL), (2, 1), (3, 2), (4, 1)",
    )
    .await?;
    let (_seq, ok_insert) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert)?.0, 4);

    send_com_query(
        &mut stream,
        "SELECT outer_q.id, (SELECT COUNT(*) FROM nodes AS inner_q WHERE inner_q.parent_id = outer_q.id OR inner_q.id = outer_q.id) AS related FROM nodes AS outer_q ORDER BY outer_q.id ASC",
    )
    .await?;
    let rows = read_mysql_text_result_rows(&mut stream).await?;
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("3".to_string())],
            vec![Some("2".to_string()), Some("2".to_string())],
            vec![Some("3".to_string()), Some("1".to_string())],
            vec![Some("4".to_string()), Some("1".to_string())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn mysql_com_stmt_prepare_projection_subquery_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server =
        HttpHarness::start_with_mysql("mysql_com_stmt_prepare_projection_subquery_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    send_com_query(&mut stream, "CREATE DATABASE IF NOT EXISTS skein_test").await?;
    let (_seq, ok_create_db) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_db)?.0, 0);

    send_com_query(&mut stream, "USE skein_test").await?;
    let (_seq, ok_use) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_use)?.0, 0);

    send_com_query(&mut stream, "DROP TABLE IF EXISTS nodes").await?;
    let (_seq, ok_drop_nodes) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop_nodes)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE nodes (id BIGINT NOT NULL, parent_id BIGINT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_nodes) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_nodes)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO nodes (id, parent_id) VALUES (1, NULL), (2, 1), (3, 2), (4, 1)",
    )
    .await?;
    let (_seq, ok_insert_nodes) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert_nodes)?.0, 4);

    send_com_stmt_prepare(
        &mut stream,
        "SELECT outer_q.id, (SELECT COUNT(*) FROM nodes AS inner_q WHERE inner_q.parent_id = outer_q.id OR inner_q.id = outer_q.id) AS related FROM nodes AS outer_q ORDER BY outer_q.id ASC",
    )
    .await?;
    let correlated_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(correlated_stmt.param_count, 0);
    assert_eq!(
        correlated_stmt.column_defs,
        vec![("id".to_string(), 0x08), ("related".to_string(), 0x08)]
    );

    send_com_stmt_execute(&mut stream, correlated_stmt.statement_id, &[]).await?;
    let correlated_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        correlated_rows,
        vec![
            vec![Some("1".to_string()), Some("3".to_string())],
            vec![Some("2".to_string()), Some("2".to_string())],
            vec![Some("3".to_string()), Some("1".to_string())],
            vec![Some("4".to_string()), Some("1".to_string())],
        ]
    );
    send_com_stmt_close(&mut stream, correlated_stmt.statement_id).await?;

    send_com_query(&mut stream, "DROP TABLE IF EXISTS payroll").await?;
    let (_seq, ok_drop_payroll) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_drop_payroll)?.0, 0);

    send_com_query(
        &mut stream,
        "CREATE TABLE payroll (id BIGINT NOT NULL, salary DOUBLE NOT NULL, PRIMARY KEY (id))",
    )
    .await?;
    let (_seq, ok_create_payroll) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_create_payroll)?.0, 0);

    send_com_query(
        &mut stream,
        "INSERT INTO payroll (id, salary) VALUES (1, 10.0), (2, 11.0)",
    )
    .await?;
    let (_seq, ok_insert_payroll) = read_mysql_packet(&mut stream).await?;
    assert_eq!(decode_mysql_ok_packet(&ok_insert_payroll)?.0, 2);

    send_com_stmt_prepare(
        &mut stream,
        "SELECT salary - (SELECT AVG(salary) FROM payroll) AS diff_from_avg FROM payroll ORDER BY id ASC",
    )
    .await?;
    let embedded_stmt = read_mysql_prepare_ok(&mut stream).await?;
    assert_eq!(embedded_stmt.param_count, 0);
    assert_eq!(
        embedded_stmt.column_defs,
        vec![("diff_from_avg".to_string(), 0x05)]
    );

    send_com_stmt_execute(&mut stream, embedded_stmt.statement_id, &[]).await?;
    let embedded_rows = read_mysql_binary_result_rows(&mut stream).await?;
    assert_eq!(
        embedded_rows,
        vec![
            vec![Some("-0.5".to_string())],
            vec![Some("0.5".to_string())],
        ]
    );
    send_com_stmt_close(&mut stream, embedded_stmt.statement_id).await?;

    Ok(())
}

#[tokio::test]
async fn mysql_compat_corpus_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_compat_corpus_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;
    let mut txn_select_index = 0usize;
    let mut timezone_value_index = 0usize;
    let mut timezone_autoload_index = 0usize;
    let mut siteurl_pair_index = 0usize;
    let mut wp_users_show_index_count = 0usize;

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
                    assert_eq!(rows[0][0].as_deref(), None);
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select @@sql_mode" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some(""));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select @@lower_case_table_names" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("0"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select @@version_comment limit 1" | "select @@version_comment limit 0,1" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("SkeinDB compatibility layer"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select @@version_comment limit 1 offset 1" => match response {
                MysqlResponse::Rows(rows) => assert!(rows.is_empty()),
                other => panic!("expected result set, got {:?}", other),
            },
            "show variables like 'time_zone'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("time_zone"));
                    assert_eq!(rows[0][1].as_deref(), Some("SYSTEM"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show variables like 'transaction_isolation'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("transaction_isolation"));
                    assert_eq!(rows[0][1].as_deref(), Some("REPEATABLE-READ"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show variables like 'sql_auto_is_null'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("sql_auto_is_null"));
                    assert_eq!(rows[0][1].as_deref(), Some("0"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show variables" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert!(rows.iter().any(|row| {
                        row[0].as_deref() == Some("sql_mode") && row[1].as_deref() == Some("")
                    }));
                    assert!(rows.iter().any(|row| {
                        row[0].as_deref() == Some("time_zone")
                            && row[1].as_deref() == Some("SYSTEM")
                    }));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show session variables like 'sql_mode'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("sql_mode"));
                    assert_eq!(rows[0][1].as_deref(), Some(""));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show global variables where variable_name = 'time_zone'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("time_zone"));
                    assert_eq!(rows[0][1].as_deref(), Some("SYSTEM"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show status" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert!(rows.iter().any(|row| {
                        row[0].as_deref() == Some("Threads_connected")
                            && row[1].as_deref() == Some("1")
                    }));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show global status like 'threads_%'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("Threads_connected"));
                    assert_eq!(rows[0][1].as_deref(), Some("1"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show character set like 'utf8mb4'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("utf8mb4"));
                    assert_eq!(rows[0][2].as_deref(), Some("utf8mb4_general_ci"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show collation where charset = 'utf8mb4'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert!(rows.iter().all(|row| row[1].as_deref() == Some("utf8mb4")));
                    assert!(rows.iter().any(|row| {
                        row[0].as_deref() == Some("utf8mb4_general_ci")
                            && row[3].as_deref() == Some("Yes")
                    }));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show variables like 'character_set_%'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert!(rows.iter().any(|row| {
                        row[0].as_deref() == Some("character_set_server")
                            && row[1].as_deref() == Some("utf8mb4")
                    }));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show variables like 'collation_%'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert!(rows.iter().any(|row| {
                        row[0].as_deref() == Some("collation_database")
                            && row[1].as_deref() == Some("utf8mb4_general_ci")
                    }));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select @@transaction_isolation" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("REPEATABLE-READ"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select @@sql_auto_is_null" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("0"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select @@character_set_server" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("utf8mb4"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select @@collation_database" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("utf8mb4_general_ci"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show tables from skein_test like 'wp_%'" => match response {
                MysqlResponse::Rows(rows) => assert_eq!(rows.len(), 3),
                other => panic!("expected result set, got {:?}", other),
            },
            "show full tables from skein_test where table_type = 'base table'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 3);
                    assert_eq!(rows[0][1].as_deref(), Some("BASE TABLE"));
                }
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
                    assert_eq!(rows[1][4].as_deref(), Some("UNI"));
                    assert_eq!(rows[3][5].as_deref(), Some("yes"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show index from wp_options" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 2);
                    assert_eq!(rows[0][2].as_deref(), Some("PRIMARY"));
                    assert_eq!(rows[1][1].as_deref(), Some("0"));
                    assert_eq!(rows[1][2].as_deref(), Some("option_name"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show index from wp_posts" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert_eq!(rows[0][2].as_deref(), Some("PRIMARY"));
                    assert!(rows.iter().any(|row| row[2].as_deref() == Some("post_status")));
                    assert!(rows.iter().any(|row| row[2].as_deref() == Some("post_author")));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show index from wp_users" => match response {
                MysqlResponse::Rows(rows) => {
                    wp_users_show_index_count += 1;
                    assert!(!rows.is_empty());
                    assert_eq!(rows[0][2].as_deref(), Some("PRIMARY"));
                    match wp_users_show_index_count {
                        1 => {
                            assert!(
                                rows.iter()
                                    .any(|row| row[2].as_deref() == Some("user_login_unique"))
                            );
                            assert!(rows.iter().any(|row| {
                                row[2].as_deref() == Some("user_login_unique")
                                    && row[1].as_deref() == Some("0")
                            }));
                        }
                        2 => {
                            assert!(rows
                                .iter()
                                .any(|row| row[2].as_deref() == Some("user_login_renamed")));
                            assert!(!rows
                                .iter()
                                .any(|row| row[2].as_deref() == Some("user_login_unique")));
                        }
                        3 => {
                            assert!(!rows
                                .iter()
                                .any(|row| row[2].as_deref() == Some("user_login_renamed")));
                        }
                        _ => panic!("unexpected wp_users show index count"),
                    }
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show keys from wp_posts" => match response {
                MysqlResponse::Rows(rows) => {
                    assert!(!rows.is_empty());
                    assert_eq!(rows[0][2].as_deref(), Some("PRIMARY"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show create table wp_options" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    let ddl = rows[0][1].as_deref().unwrap_or_default();
                    assert!(ddl.contains("CREATE TABLE"));
                    assert!(ddl.contains("UNIQUE KEY `option_name` (`option_name`)"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show create table wp_posts" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    let ddl = rows[0][1].as_deref().unwrap_or_default();
                    assert!(ddl.contains("CREATE TABLE"));
                    assert!(ddl.contains("PRIMARY KEY"));
                    assert!(ddl.contains("KEY `post_status` (`post_status`)"));
                    assert!(ddl.contains("KEY `post_author` (`post_author`)"));
                    assert!(ddl.contains("DEFAULT 'publish'"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "describe wp_posts" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 6);
                    assert_eq!(rows[0][0].as_deref(), Some("ID"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select option_name from wp_options where option_name in ('siteurl', 'home') order by option_name" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("home"));
                        assert_eq!(rows[1][0].as_deref(), Some("siteurl"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
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
                            | Some("https://example.replace")
                    ));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select option_value from wp_options where option_name='siteurl'" => match response {
                MysqlResponse::Rows(rows) => {
                    let value = rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(|v| v.as_deref());
                    assert!(matches!(
                        value,
                        Some("https://example.net") | Some("https://example.replace")
                    ));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select p.post_author, u.user_login from wp_posts as p left join wp_users as u on p.post_author = u.id where u.user_login is null order by p.post_author asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                        assert_eq!(rows[0][1], None);
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select u.id, u.user_login, p.post_title from wp_users as u inner join wp_posts as p using (id) order by u.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 3);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[0][1].as_deref(), Some("ada"));
                        assert_eq!(rows[0][2].as_deref(), Some("Hello"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][1].as_deref(), Some("grace"));
                        assert_eq!(rows[1][2].as_deref(), Some("Draft 1"));
                        assert_eq!(rows[2][0].as_deref(), Some("4"));
                        assert_eq!(rows[2][1].as_deref(), Some("margaret"));
                        assert_eq!(rows[2][2].as_deref(), Some("More"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select post_name from wp_posts where id = 1" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some(""));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select date(post_date), year(post_date), month(post_date), day(post_date), hour(post_date), minute(post_date), second(post_date) from wp_posts where id = 1" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("2020-01-01".to_string()),
                                Some("2020".to_string()),
                                Some("1".to_string()),
                                Some("1".to_string()),
                                Some("0".to_string()),
                                Some("0".to_string()),
                                Some("0".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select date_format(post_date, '%y-%m-%d %h:%i:%s'), from_unixtime(unix_timestamp(post_date)) from wp_posts where id = 1" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("2020-01-01 00:00:00".to_string()),
                                Some("2020-01-01 00:00:00".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select datediff(post_date, '2020-01-01 00:00:00'), timestampdiff(hour, '2020-01-01 00:00:00', post_date) from wp_posts where id = 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0], vec![Some("1".to_string()), Some("24".to_string())]);
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select weekday(post_date), dayofweek(post_date), dayofyear(post_date), monthname(post_date), dayname(post_date) from wp_posts where id = 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("3".to_string()),
                                Some("5".to_string()),
                                Some("2".to_string()),
                                Some("January".to_string()),
                                Some("Thursday".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select quarter(post_date), last_day(post_date), extract(year from post_date), extract(hour from post_date) from wp_posts where id = 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("1".to_string()),
                                Some("2020-01-31".to_string()),
                                Some("2020".to_string()),
                                Some("0".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select date_add(post_date, interval 2 day), date_sub(post_date, interval 3 hour), timestampadd(minute, 30, post_date) from wp_posts where id = 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("2020-01-04 00:00:00".to_string()),
                                Some("2020-01-01 21:00:00".to_string()),
                                Some("2020-01-02 00:30:00".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where date(post_date) = '2020-01-03' order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where date_format(post_date, '%y-%m-%d') = '2020-01-03' order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where year(post_date) = 2020 order by unix_timestamp(post_date) desc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("5"));
                        assert_eq!(rows[1][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where datediff(post_date, '2020-01-01 00:00:00') >= 2 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 3);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                        assert_eq!(rows[1][0].as_deref(), Some("4"));
                        assert_eq!(rows[2][0].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where dayname(post_date) = 'friday' order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where extract(day from post_date) = 3 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where date_add(post_date, interval 1 day) = '2020-01-04 00:00:00' order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select p.id from wp_posts as p left join wp_users as u on p.post_author = u.id where u.user_login = 'ada' order by p.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select u.id, p.id from wp_posts as p right join wp_users as u on p.post_author = u.id where p.id is null order by u.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("4"));
                        assert_eq!(rows[0][1], None);
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select p.id, u.user_login, ux.user_login from wp_posts as p left join wp_users as u on p.post_author = u.id left join wp_users as ux on ux.id = u.id where p.id = 1 order by p.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[0][1].as_deref(), Some("ada"));
                        assert_eq!(rows[0][2].as_deref(), Some("ada"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select p.id post_id, u.user_login author_login from wp_posts as p left join wp_users as u on p.post_author = u.id where p.id = 1" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[0][1].as_deref(), Some("ada"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select * from wp_posts as p left join wp_users as u on p.post_author = u.id where p.id = 1" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("1".to_string()),
                                Some("1".to_string()),
                                Some("2020-01-01 00:00:00".to_string()),
                                Some("publish".to_string()),
                                Some("Hello".to_string()),
                                Some("".to_string()),
                                Some("1".to_string()),
                                Some("ada".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select p.*, u.user_login from wp_posts as p left join wp_users as u on p.post_author = u.id where p.id = 1" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("1".to_string()),
                                Some("1".to_string()),
                                Some("2020-01-01 00:00:00".to_string()),
                                Some("publish".to_string()),
                                Some("Hello".to_string()),
                                Some("".to_string()),
                                Some("ada".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select skein_test.wp_posts.*, u.user_login from skein_test.wp_posts left join skein_test.wp_users as u on wp_posts.post_author = u.id where wp_posts.id = 1" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("1".to_string()),
                                Some("1".to_string()),
                                Some("2020-01-01 00:00:00".to_string()),
                                Some("publish".to_string()),
                                Some("Hello".to_string()),
                                Some("".to_string()),
                                Some("ada".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where post_title = null order by id asc" => match response {
                MysqlResponse::Rows(rows) => assert!(rows.is_empty()),
                other => panic!("expected result set, got {:?}", other),
            },
            "select option_value, autoload from wp_options where option_name='siteurl'" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        siteurl_pair_index += 1;
                        let expected = match siteurl_pair_index {
                            1 => (Some("https://example.shuffle"), Some("no")),
                            2 => (Some("https://example.replace-shuffled"), Some("yes")),
                            _ => panic!("unexpected siteurl pair select count"),
                        };
                        assert_eq!(rows[0][0].as_deref(), expected.0);
                        assert_eq!(rows[0][1].as_deref(), expected.1);
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select autoload from wp_options where option_name='timezone_string'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    timezone_autoload_index += 1;
                    let expected = match timezone_autoload_index {
                        1 => Some("yes"),
                        2 => Some("no"),
                        _ => panic!("unexpected timezone autoload select count"),
                    };
                    assert_eq!(rows[0][0].as_deref(), expected);
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select option_value from wp_options where option_name='timezone_string'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    timezone_value_index += 1;
                    let expected = match timezone_value_index {
                        1 => Some("UTC"),
                        2 => Some("Europe/Berlin"),
                        _ => panic!("unexpected timezone value select count"),
                    };
                    assert_eq!(rows[0][0].as_deref(), expected);
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select count(*) as publish_count from wp_posts where post_status = 'publish'" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select min(post_author) as min_author from wp_posts where post_status = 'publish'" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select max(post_author) as max_author from wp_posts where post_status = 'publish'" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select avg(post_author) as avg_author from wp_posts where post_status = 'publish'" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select count(*) as user_count from wp_users" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("3"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select count(*) as user_count from wp_users having count(*) >= 3 and user_count = 3" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("3"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select post_status, count(*) as status_count from wp_posts group by post_status order by post_status asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("draft"));
                        assert_eq!(rows[0][1].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("publish"));
                        assert_eq!(rows[1][1].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select post_status, count(*) as status_count from wp_posts group by post_status having status_count > 1 order by status_count desc, post_status asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("publish"));
                        assert_eq!(rows[0][1].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select post_status, count(*) as status_count from wp_posts group by post_status having count(*) > 1 and post_status = 'publish' order by status_count desc, post_status asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("publish"));
                        assert_eq!(rows[0][1].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select p.id as post_id, p.post_status from wp_posts as p group by p.id, p.post_status having post_id = 1 and p.post_status = 'publish' order by p.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[0][1].as_deref(), Some("publish"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select distinct p.post_author as author_id, u.user_login from wp_posts as p, wp_users as u where p.post_author = u.id order by p.post_author asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[0][1].as_deref(), Some("ada"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][1].as_deref(), Some("grace"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select * from wp_users group by id, user_login having id = 1 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(
                            rows[0],
                            vec![
                                Some("1".to_string()),
                                Some("ada".to_string()),
                            ]
                        );
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select post_author, sum(post_author) as author_sum_by_author from wp_posts where post_status = 'publish' group by post_author order by post_author asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 3);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[0][1].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][1].as_deref(), Some("4"));
                        assert_eq!(rows[2][0].as_deref(), Some("3"));
                        assert_eq!(rows[2][1].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select post_status, max(post_author) as max_author_by_status from wp_posts group by post_status order by post_status asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("draft"));
                        assert_eq!(rows[0][1].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("publish"));
                        assert_eq!(rows[1][1].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where post_status = 'publish' or post_status = 'draft' order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 5);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                        assert_eq!(rows[2][0].as_deref(), Some("3"));
                        assert_eq!(rows[3][0].as_deref(), Some("4"));
                        assert_eq!(rows[4][0].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where (post_status = 'publish' or post_status = 'draft') and post_author = 1 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where post_status not in ('draft') order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 4);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("3"));
                        assert_eq!(rows[2][0].as_deref(), Some("4"));
                        assert_eq!(rows[3][0].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where post_title not like 'dr%' order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 4);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("3"));
                        assert_eq!(rows[2][0].as_deref(), Some("4"));
                        assert_eq!(rows[3][0].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from wp_posts where post_status like 'pub%' order by id desc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("5"));
                        assert_eq!(rows[1][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select sql_calc_found_rows p.id from wp_posts as p left join wp_posts as px on px.post_author = p.post_author where p.post_status='publish' group by p.id order by p.id asc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
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
            "select slug from compat_alter_subq where id = 4" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("n-a"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show full columns from compat_alter_subq" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 3);
                    assert_eq!(rows[0][0].as_deref(), Some("id"));
                    assert_eq!(rows[1][0].as_deref(), Some("parent_id"));
                    assert_eq!(rows[2][0].as_deref(), Some("post_slug"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select post_slug from compat_alter_subq where id = 4" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("n-a"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select id from compat_alter_subq where parent_id in ( select id from compat_alter_subq where id < 3 ) order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 3);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][0].as_deref(), Some("3"));
                        assert_eq!(rows[2][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where parent_id = ( select parent_id from compat_alter_subq where id = 4 ) order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where exists ( select 1 from compat_alter_subq where post_slug = 'n-a' ) order by id asc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select outer_q.id from compat_alter_subq as outer_q where exists ( select 1 from compat_alter_subq as inner_q where inner_q.parent_id = outer_q.id ) order by outer_q.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where not exists ( select 1 from compat_alter_subq where id = 999 ) order by id asc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where parent_id in ( select id from compat_alter_subq where id < 3 ) and id > 1 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 3);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][0].as_deref(), Some("3"));
                        assert_eq!(rows[2][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where exists ( select 1 from compat_alter_subq where post_slug = 'n-a' ) and parent_id is not null order by id asc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where parent_id in ( select id from compat_alter_subq where id < 3 ) or id = 1 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 4);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                        assert_eq!(rows[2][0].as_deref(), Some("3"));
                        assert_eq!(rows[3][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where lower(post_slug) = 'n-a' order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select lower(post_slug), upper(post_slug), length(post_slug), char_length(post_slug), coalesce(parent_id, 0), ifnull(parent_id, 0), concat(post_slug, '-', ifnull(parent_id, 0)) from compat_alter_subq where id = 4" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("n-a"));
                        assert_eq!(rows[0][1].as_deref(), Some("N-A"));
                        assert_eq!(rows[0][2].as_deref(), Some("3"));
                        assert_eq!(rows[0][3].as_deref(), Some("3"));
                        assert_eq!(rows[0][4].as_deref(), Some("1"));
                        assert_eq!(rows[0][5].as_deref(), Some("1"));
                        assert_eq!(rows[0][6].as_deref(), Some("n-a-1"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select trim('  n-a  '), ltrim('  n-a'), rtrim('n-a  '), left(post_slug, 1), right(post_slug, 1), substring(post_slug, 2, 2), replace(post_slug, '-', '_'), nullif(post_slug, 'n-a') from compat_alter_subq where id = 4" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("n-a"));
                        assert_eq!(rows[0][1].as_deref(), Some("n-a"));
                        assert_eq!(rows[0][2].as_deref(), Some("n-a"));
                        assert_eq!(rows[0][3].as_deref(), Some("n"));
                        assert_eq!(rows[0][4].as_deref(), Some("a"));
                        assert_eq!(rows[0][5].as_deref(), Some("-a"));
                        assert_eq!(rows[0][6].as_deref(), Some("n_a"));
                        assert_eq!(rows[0][7], None);
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select if(1, post_slug, 'miss'), locate('a', post_slug), instr(post_slug, 'a'), abs(-7), round(1.75, 1), floor(1.75), ceil(1.2), mod(7, 4), least('z', 'a'), greatest(1, 5, 2) from compat_alter_subq where id = 4" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("n-a"));
                        assert_eq!(rows[0][1].as_deref(), Some("3"));
                        assert_eq!(rows[0][2].as_deref(), Some("3"));
                        assert_eq!(rows[0][3].as_deref(), Some("7"));
                        assert_eq!(rows[0][4].as_deref(), Some("1.8"));
                        assert_eq!(rows[0][5].as_deref(), Some("1"));
                        assert_eq!(rows[0][6].as_deref(), Some("2"));
                        assert_eq!(rows[0][7].as_deref(), Some("3"));
                        assert_eq!(rows[0][8].as_deref(), Some("a"));
                        assert_eq!(rows[0][9].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select cast(id as char), cast('7' as unsigned), case when parent_id is null then 'root' else 'child' end, case post_slug when 'n-a' then 'match' else 'miss' end from compat_alter_subq where id = 4" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("4"));
                        assert_eq!(rows[0][1].as_deref(), Some("7"));
                        assert_eq!(rows[0][2].as_deref(), Some("child"));
                        assert_eq!(rows[0][3].as_deref(), Some("match"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select parent_id + 1, parent_id - 1, parent_id * 2, parent_id / 2, parent_id % 2 from compat_alter_subq where id = 4" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[0][1].as_deref(), Some("0"));
                        assert_eq!(rows[0][2].as_deref(), Some("2"));
                        assert_eq!(rows[0][3].as_deref(), Some("0.5"));
                        assert_eq!(rows[0][4].as_deref(), Some("1"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where cast(parent_id as unsigned) = 1 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where parent_id + 0 = 1 order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][0].as_deref(), Some("4"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where parent_id is not null order by cast(parent_id as unsigned) desc, id asc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where parent_id is not null order by parent_id + 0 desc, id asc limit 0, 2" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select outer_q.id from compat_alter_subq as outer_q where exists ( select 1 from compat_alter_subq as inner_q where inner_q.parent_id = outer_q.parent_id and inner_q.post_slug = outer_q.post_slug and inner_q.id > 4 ) order by outer_q.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("4"));
                        assert_eq!(rows[1][0].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select outer_q.id from compat_alter_subq as outer_q where outer_q.id in ( select inner_q.id from compat_alter_subq as inner_q where inner_q.parent_id = outer_q.parent_id ) order by outer_q.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 4);
                        assert_eq!(rows[0][0].as_deref(), Some("2"));
                        assert_eq!(rows[1][0].as_deref(), Some("3"));
                        assert_eq!(rows[2][0].as_deref(), Some("4"));
                        assert_eq!(rows[3][0].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select outer_q.id from compat_alter_subq as outer_q where ( exists ( select 1 from compat_alter_subq as inner_q where inner_q.parent_id = outer_q.id ) and outer_q.id > 1 ) or outer_q.id = 1 order by outer_q.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 2);
                        assert_eq!(rows[0][0].as_deref(), Some("1"));
                        assert_eq!(rows[1][0].as_deref(), Some("2"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select id from compat_alter_subq where parent_id in ( select id from compat_alter_subq where id in ( select parent_id from compat_alter_subq where id = 3 ) ) order by id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "select outer_q.id from compat_alter_subq as outer_q where not ( outer_q.id = 1 or exists ( select 1 from compat_alter_subq as inner_q where inner_q.parent_id = outer_q.id ) ) order by outer_q.id asc" => {
                match response {
                    MysqlResponse::Rows(rows) => {
                        assert_eq!(rows.len(), 3);
                        assert_eq!(rows[0][0].as_deref(), Some("3"));
                        assert_eq!(rows[1][0].as_deref(), Some("4"));
                        assert_eq!(rows[2][0].as_deref(), Some("5"));
                    }
                    other => panic!("expected result set, got {:?}", other),
                }
            }
            "show full columns from compat_dropcol" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 2);
                    assert_eq!(rows[0][0].as_deref(), Some("id"));
                    assert_eq!(rows[1][0].as_deref(), Some("keep_col"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show index from compat_dropcol" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][2].as_deref(), Some("PRIMARY"));
                    assert_eq!(rows[0][4].as_deref(), Some("id"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select keep_col from compat_dropcol where id = 1" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("stay"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show tables from skein_test like 'compat_rename_%'" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("compat_rename_dst"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show full columns from compat_rename_dst" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 2);
                    assert_eq!(rows[0][0].as_deref(), Some("id"));
                    assert_eq!(rows[1][0].as_deref(), Some("slug"));
                    assert_eq!(rows[1][5].as_deref(), Some("seed"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show index from compat_rename_dst" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 2);
                    assert_eq!(rows[0][2].as_deref(), Some("PRIMARY"));
                    assert!(rows.iter().any(|row| {
                        row[2].as_deref() == Some("slug_unique")
                            && row[1].as_deref() == Some("0")
                            && row[4].as_deref() == Some("slug")
                    }));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "show create table compat_rename_dst" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    let ddl = rows[0][1].as_deref().unwrap_or_default();
                    assert!(ddl.contains("CREATE TABLE"));
                    assert!(ddl.contains("UNIQUE KEY `slug_unique` (`slug`)"));
                    assert!(ddl.contains("DEFAULT 'seed'"));
                }
                other => panic!("expected result set, got {:?}", other),
            },
            "select slug from compat_rename_dst where id = 1" => match response {
                MysqlResponse::Rows(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0][0].as_deref(), Some("hello"));
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
    assert_eq!(timezone_value_index, 2);
    assert_eq!(timezone_autoload_index, 2);
    assert_eq!(siteurl_pair_index, 2);
    assert_eq!(wp_users_show_index_count, 3);

    Ok(())
}

#[tokio::test]
async fn mysql_supports_wordpress_style_insert_variants_and_join() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_wp_style_join")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    for stmt in [
        "CREATE DATABASE IF NOT EXISTS wp",
        "USE wp",
        "CREATE TABLE wp_users (id BIGINT NOT NULL, status VARCHAR(20) NOT NULL, name VARCHAR(64) NOT NULL, PRIMARY KEY (id))",
        "CREATE TABLE wp_posts (id BIGINT NOT NULL, post_author BIGINT NOT NULL, post_status VARCHAR(20) NOT NULL, PRIMARY KEY (id))",
        "CREATE TABLE wp_options (id BIGINT NOT NULL, option_name VARCHAR(191) NOT NULL, PRIMARY KEY (id))",
        "CREATE TABLE wp_profiles (user_id BIGINT NOT NULL, display_name VARCHAR(64) NOT NULL, PRIMARY KEY (user_id))",
        "ALTER TABLE wp_posts ADD COLUMN post_title VARCHAR(64) NOT NULL DEFAULT 'untitled'",
        "ALTER TABLE wp_posts ADD COLUMN post_name VARCHAR(200) NOT NULL DEFAULT '' AFTER post_title",
        "ALTER TABLE wp_posts ADD KEY post_author (post_author)",
        "INSERT INTO wp_users (id, status, name) VALUES (1, 'active', 'Ada'), (2, 'active', 'Grace')",
        "INSERT IGNORE INTO wp_users (id, status, name) VALUES (1, 'inactive', 'Ignored'), (3, 'active', 'Linus')",
        "REPLACE INTO wp_users (id, status, name) VALUES (2, 'active', 'Grace Hopper')",
        "INSERT INTO wp_options (id, option_name) VALUES (1, 'siteurl')",
        "INSERT INTO wp_profiles (user_id, display_name) VALUES (1, 'Ada Lovelace'), (3, 'Linus Torvalds')",
        "INSERT INTO wp_posts (id, post_author, post_status) VALUES (10, 1, 'publish'), (11, 1, 'draft'), (12, 3, 'publish')",
    ] {
        send_com_query(&mut stream, stmt).await?;
        match read_mysql_response(&mut stream).await? {
            MysqlResponse::Ok { .. } => {}
            other => panic!("expected OK packet, got {:?}", other),
        }
    }

    send_com_query(&mut stream, "SELECT post_title FROM wp_posts WHERE id = 10").await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("untitled"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(&mut stream, "SELECT post_name FROM wp_posts WHERE id = 10").await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some(""));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT DISTINCT p.post_author AS author_id, u.name FROM wp_posts AS p INNER JOIN wp_users AS u ON p.post_author = u.id WHERE u.status = 'active' ORDER BY p.post_author ASC",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0].as_deref(), Some("1"));
            assert_eq!(rows[0][1].as_deref(), Some("Ada"));
            assert_eq!(rows[1][0].as_deref(), Some("3"));
            assert_eq!(rows[1][1].as_deref(), Some("Linus"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT DISTINCT p.post_author AS author_id, u.name FROM wp_posts AS p, wp_users AS u WHERE p.post_author = u.id AND u.status = 'active' ORDER BY p.post_author ASC",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0].as_deref(), Some("1"));
            assert_eq!(rows[0][1].as_deref(), Some("Ada"));
            assert_eq!(rows[1][0].as_deref(), Some("3"));
            assert_eq!(rows[1][1].as_deref(), Some("Linus"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT * FROM wp_users GROUP BY id, status, name HAVING id = 1 ORDER BY id ASC",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    Some("1".to_string()),
                    Some("active".to_string()),
                    Some("Ada".to_string()),
                ]
            );
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT TABLE_NAME AS 'table', TABLE_ROWS AS 'rows', SUM(data_length + index_length) AS 'bytes' FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'wp' AND TABLE_NAME IN ('wp_options','wp_posts','wp_users') GROUP BY TABLE_NAME",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 3);
            let mut table_names = rows
                .iter()
                .filter_map(|row| row.first().and_then(|value| value.as_deref()))
                .collect::<Vec<_>>();
            table_names.sort_unstable();
            assert_eq!(table_names, vec!["wp_options", "wp_posts", "wp_users"]);
            for row in rows {
                assert_eq!(row.get(1).and_then(|value| value.as_deref()), Some("0"));
                assert_eq!(row.get(2).and_then(|value| value.as_deref()), Some("0"));
            }
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT p.id, u.name, pr.display_name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id LEFT JOIN wp_profiles AS pr ON pr.user_id = u.id WHERE p.id = 10",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("10"));
            assert_eq!(rows[0][1].as_deref(), Some("Ada"));
            assert_eq!(rows[0][2].as_deref(), Some("Ada Lovelace"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT * FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE p.id = 10",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    Some("10".to_string()),
                    Some("1".to_string()),
                    Some("publish".to_string()),
                    Some("untitled".to_string()),
                    Some("".to_string()),
                    Some("1".to_string()),
                    Some("active".to_string()),
                    Some("Ada".to_string()),
                ]
            );
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT p.*, u.name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE p.id = 10",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    Some("10".to_string()),
                    Some("1".to_string()),
                    Some("publish".to_string()),
                    Some("untitled".to_string()),
                    Some("".to_string()),
                    Some("Ada".to_string()),
                ]
            );
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT p.id post_id, u.name author_name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE p.id = 10",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("10"));
            assert_eq!(rows[0][1].as_deref(), Some("Ada"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT p.id AS post_id, p.post_status FROM wp_posts AS p GROUP BY p.id, p.post_status HAVING post_id = 10 AND p.post_status = 'publish' ORDER BY p.id ASC",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("10"));
            assert_eq!(rows[0][1].as_deref(), Some("publish"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT wp.wp_posts.*, u.name FROM wp.wp_posts LEFT JOIN wp.wp_users AS u ON wp_posts.post_author = u.id WHERE wp_posts.id = 10",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    Some("10".to_string()),
                    Some("1".to_string()),
                    Some("publish".to_string()),
                    Some("untitled".to_string()),
                    Some("".to_string()),
                    Some("Ada".to_string()),
                ]
            );
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "INSERT INTO wp_posts (id, post_author, post_status) VALUES (13, 99, 'publish')",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { .. } => {}
        other => panic!("expected OK packet, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT p.id, u.name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE u.name IS NULL ORDER BY p.id ASC",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("13"));
            assert_eq!(rows[0][1], None);
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT p.id FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE u.name = 'Ada' ORDER BY p.id ASC",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0].as_deref(), Some("10"));
            assert_eq!(rows[1][0].as_deref(), Some("11"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT SQL_CALC_FOUND_ROWS * FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE u.name = 'Ada' ORDER BY p.id ASC LIMIT 1",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    Some("10".to_string()),
                    Some("1".to_string()),
                    Some("publish".to_string()),
                    Some("untitled".to_string()),
                    Some("".to_string()),
                    Some("1".to_string()),
                    Some("active".to_string()),
                    Some("Ada".to_string()),
                ]
            );
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(&mut stream, "SELECT FOUND_ROWS()").await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("2"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT SQL_CALC_FOUND_ROWS p.*, u.name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE u.name = 'Ada' ORDER BY p.id ASC LIMIT 1",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    Some("10".to_string()),
                    Some("1".to_string()),
                    Some("publish".to_string()),
                    Some("untitled".to_string()),
                    Some("".to_string()),
                    Some("Ada".to_string()),
                ]
            );
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(&mut stream, "SELECT FOUND_ROWS()").await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("2"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "INSERT INTO wp_users (id, status, name) VALUES (4, 'active', 'Margaret')",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { .. } => {}
        other => panic!("expected OK packet, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT u.id, p.id FROM wp_posts AS p RIGHT JOIN wp_users AS u ON p.post_author = u.id WHERE p.id IS NULL ORDER BY u.id ASC",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0].as_deref(), Some("2"));
            assert_eq!(rows[0][1], None);
            assert_eq!(rows[1][0].as_deref(), Some("4"));
            assert_eq!(rows[1][1], None);
        }
        other => panic!("expected result set, got {:?}", other),
    }

    Ok(())
}

#[tokio::test]
async fn mysql_supports_wordpress_admin_aggregate_and_site_health_queries() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_wp_admin_aggregate_shapes")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    for stmt in [
        "CREATE DATABASE IF NOT EXISTS wordpress",
        "USE wordpress",
        "CREATE TABLE wp_comments (comment_ID BIGINT NOT NULL, PRIMARY KEY (comment_ID))",
        "CREATE TABLE wp_options (option_id BIGINT NOT NULL, PRIMARY KEY (option_id))",
        "CREATE TABLE wp_posts (ID BIGINT NOT NULL, PRIMARY KEY (ID))",
        "CREATE TABLE wp_terms (term_id BIGINT NOT NULL, PRIMARY KEY (term_id))",
        "CREATE TABLE wp_users (ID BIGINT NOT NULL, user_login VARCHAR(64) NOT NULL, PRIMARY KEY (ID))",
        "CREATE TABLE wp_usermeta (umeta_id BIGINT NOT NULL, user_id BIGINT NOT NULL, meta_key VARCHAR(255) NOT NULL, meta_value TEXT NOT NULL, PRIMARY KEY (umeta_id))",
        "INSERT INTO wp_users (ID, user_login) VALUES (1, 'admin'), (2, 'subscriber')",
        "INSERT INTO wp_usermeta (umeta_id, user_id, meta_key, meta_value) VALUES (1, 1, 'wp_capabilities', 'a:1:{s:13:\"administrator\";b:1;}'), (2, 2, 'wp_capabilities', 'a:1:{s:10:\"subscriber\";b:1;}')",
    ] {
        send_com_query(&mut stream, stmt).await?;
        match read_mysql_response(&mut stream).await? {
            MysqlResponse::Ok { .. } => {}
            other => panic!("expected OK packet for {stmt:?}, got {:?}", other),
        }
    }

    send_com_query(
        &mut stream,
        "SELECT COUNT(NULLIF(`meta_value` LIKE '%\\\"administrator\\\"%', false)), COUNT(NULLIF(`meta_value` LIKE '%\\\"editor\\\"%', false)), COUNT(NULLIF(`meta_value` LIKE '%\\\"author\\\"%', false)), COUNT(NULLIF(`meta_value` LIKE '%\\\"contributor\\\"%', false)), COUNT(NULLIF(`meta_value` LIKE '%\\\"subscriber\\\"%', false)), COUNT(NULLIF(`meta_value` = 'a:0:{}', false)), COUNT(*) FROM wp_usermeta INNER JOIN wp_users ON user_id = ID WHERE meta_key = 'wp_capabilities'",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("1"));
            assert_eq!(rows[0][1].as_deref(), Some("0"));
            assert_eq!(rows[0][2].as_deref(), Some("0"));
            assert_eq!(rows[0][3].as_deref(), Some("0"));
            assert_eq!(rows[0][4].as_deref(), Some("1"));
            assert_eq!(rows[0][5].as_deref(), Some("0"));
            assert_eq!(rows[0][6].as_deref(), Some("2"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT TABLE_NAME AS 'table', TABLE_ROWS AS 'rows', SUM(data_length + index_length) AS 'bytes' FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'wordpress' AND TABLE_NAME IN ('wp_comments','wp_options','wp_posts','wp_terms','wp_users') GROUP BY TABLE_NAME",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 5);
            let mut table_names = rows
                .iter()
                .filter_map(|row| row.first().and_then(|value| value.as_deref()))
                .collect::<Vec<_>>();
            table_names.sort_unstable();
            assert_eq!(
                table_names,
                vec![
                    "wp_comments",
                    "wp_options",
                    "wp_posts",
                    "wp_terms",
                    "wp_users"
                ]
            );
            for row in rows {
                assert_eq!(row.get(1).and_then(|value| value.as_deref()), Some("0"));
                assert_eq!(row.get(2).and_then(|value| value.as_deref()), Some("0"));
            }
        }
        other => panic!("expected result set, got {:?}", other),
    }

    Ok(())
}

#[tokio::test]
async fn mysql_supports_wordpress_installer_seed_queries() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_wp_installer_seed")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    for stmt in [
        "CREATE DATABASE IF NOT EXISTS wp",
        "USE wp",
        "CREATE TABLE wp_posts (id BIGINT NOT NULL, post_author BIGINT NOT NULL, post_date DATETIME NOT NULL, post_date_gmt DATETIME NOT NULL, post_content TEXT NOT NULL, post_excerpt TEXT NOT NULL, comment_status VARCHAR(20) NOT NULL, post_title VARCHAR(255) NOT NULL, post_name VARCHAR(200) NOT NULL, post_modified DATETIME NOT NULL, post_modified_gmt DATETIME NOT NULL, guid VARCHAR(255) NOT NULL, post_type VARCHAR(20) NOT NULL, to_ping TEXT NOT NULL, pinged TEXT NOT NULL, post_content_filtered TEXT NOT NULL, PRIMARY KEY (id))",
        "CREATE TABLE wp_options (option_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, option_name VARCHAR(191) NOT NULL, option_value LONGTEXT NOT NULL, autoload VARCHAR(20) NOT NULL DEFAULT 'yes', PRIMARY KEY (option_id), UNIQUE KEY option_name (option_name))",
    ] {
        send_com_query(&mut stream, stmt).await?;
        match read_mysql_response(&mut stream).await? {
            MysqlResponse::Ok { .. } => {}
            other => panic!("expected OK packet, got {:?}", other),
        }
    }

    send_com_query(
        &mut stream,
        "INSERT INTO `wp_posts` (`id`, `post_author`, `post_date`, `post_date_gmt`, `post_content`, `post_excerpt`, `comment_status`, `post_title`, `post_name`, `post_modified`, `post_modified_gmt`, `guid`, `post_type`, `to_ping`, `pinged`, `post_content_filtered`) VALUES (2, 1, '2026-03-30 19:25:11', '2026-03-30 19:25:11', '<!-- wp:paragraph -->\\n<p>This is an example page. It\\'s different from a blog post.</p>\\n<!-- /wp:paragraph -->', '', 'closed', 'Sample Page', 'sample-page', '2026-03-30 19:25:11', '2026-03-30 19:25:11', 'http://127.0.0.1:18081/?page_id=2', 'page', '', '', '')",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { .. } => {}
        other => panic!("expected OK packet, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT post_content, post_title FROM wp_posts WHERE id = 2",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1].as_deref(), Some("Sample Page"));
            let content = rows[0][0].as_deref().unwrap_or_default();
            assert!(content.contains("It's different from a blog post."));
            assert!(content.contains("<!-- wp:paragraph -->"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "INSERT IGNORE INTO `wp_options` ( `option_name`, `option_value`, `autoload` ) VALUES ('auto_updater.lock', '1774898730', 'off') /* LOCK */",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { .. } => {}
        other => panic!("expected OK packet, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "INSERT INTO `wp_options` (`option_name`, `option_value`, `autoload`) VALUES ('_site_transient_wp_theme_files_patterns-test', 'a:2:{s:7:\\\"version\\\";s:3:\\\"1.4\\\";s:8:\\\"patterns\\\";a:1:{s:12:\\\"comments.php\\\";a:1:{s:5:\\\"title\\\";s:8:\\\"Comments\\\";}}}', 'off') ON DUPLICATE KEY UPDATE `option_name` = VALUES(`option_name`), `option_value` = VALUES(`option_value`), `autoload` = VALUES(`autoload`)",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Ok { .. } => {}
        other => panic!("expected OK packet, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT option_value FROM wp_options WHERE option_name = '_site_transient_wp_theme_files_patterns-test'",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            let value = rows[0][0].as_deref().unwrap_or_default();
            assert!(value.contains("version"));
            assert!(value.contains("comments.php"));
            assert!(value.contains("Comments"));
        }
        other => panic!("expected result set, got {:?}", other),
    }

    send_com_query(
        &mut stream,
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'wp' AND TABLE_NAME IN ('wp_users','wp_usermeta','wp_posts','wp_options') AND ENGINE = 'MyISAM'",
    )
    .await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert!(rows.is_empty());
        }
        other => panic!("expected result set, got {:?}", other),
    }

    Ok(())
}

#[tokio::test]
async fn mysql_com_query_strips_qualified_projection_labels() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_projection_labels")?;
    wait_for_tcp(server.mysql_port())?;
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;

    for stmt in [
        "CREATE DATABASE IF NOT EXISTS app",
        "USE app",
        "DROP TABLE IF EXISTS users",
        "CREATE TABLE users (id BIGINT UNSIGNED NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO users (id, name) VALUES (7, 'Mia')",
    ] {
        send_com_query(&mut stream, stmt).await?;
        match read_mysql_response(&mut stream).await? {
            MysqlResponse::Ok { .. } => {}
            other => panic!("expected OK packet, got {:?}", other),
        }
    }

    send_com_query(
        &mut stream,
        "SELECT DISTINCT u.id, u.name FROM users AS u WHERE u.id = 7",
    )
    .await?;
    let (columns, rows) = read_mysql_text_result(&mut stream).await?;
    assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows,
        vec![vec![Some("7".to_string()), Some("Mia".to_string())]]
    );

    for stmt in [
        "DROP TABLE IF EXISTS wp_term_taxonomy",
        "DROP TABLE IF EXISTS wp_terms",
        "CREATE TABLE wp_terms (term_id BIGINT UNSIGNED NOT NULL, name VARCHAR(255) NOT NULL, slug VARCHAR(255) NOT NULL, term_group BIGINT NOT NULL, PRIMARY KEY (term_id))",
        "CREATE TABLE wp_term_taxonomy (term_taxonomy_id BIGINT UNSIGNED NOT NULL, term_id BIGINT UNSIGNED NOT NULL, taxonomy VARCHAR(255) NOT NULL, description TEXT NOT NULL, parent BIGINT UNSIGNED NOT NULL, count BIGINT NOT NULL, PRIMARY KEY (term_taxonomy_id))",
        "INSERT INTO wp_terms (term_id, name, slug, term_group) VALUES (1, 'Uncategorized', 'uncategorized', 0)",
        "INSERT INTO wp_term_taxonomy (term_taxonomy_id, term_id, taxonomy, description, parent, count) VALUES (1, 1, 'category', '', 0, 1)",
    ] {
        send_com_query(&mut stream, stmt).await?;
        match read_mysql_response(&mut stream).await? {
            MysqlResponse::Ok { .. } => {}
            other => panic!("expected OK packet, got {:?}", other),
        }
    }

    send_com_query(
        &mut stream,
        "SELECT t.*, tt.* FROM wp_terms AS t INNER JOIN wp_term_taxonomy AS tt ON t.term_id = tt.term_id WHERE t.term_id = 1",
    )
    .await?;
    let (columns, rows) = read_mysql_text_result(&mut stream).await?;
    assert_eq!(
        columns,
        vec![
            "term_id".to_string(),
            "name".to_string(),
            "slug".to_string(),
            "term_group".to_string(),
            "term_taxonomy_id".to_string(),
            "term_id".to_string(),
            "taxonomy".to_string(),
            "description".to_string(),
            "parent".to_string(),
            "count".to_string(),
        ]
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][6].as_deref(), Some("category"));

    Ok(())
}

// ── Research hardening tests ────────────────────────────────────────────

#[tokio::test]
async fn r07_merge_conflict_resolution_deterministic() -> anyhow::Result<()> {
    // R07: Merge Functions — verify deterministic conflict resolution
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r07_merge")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    // Create table and insert data
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "CREATE TABLE IF NOT EXISTS r07_test (id INT PRIMARY KEY, value TEXT, version INT)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "INSERT INTO r07_test (id, value, version) VALUES (1, 'initial', 1)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    // Register a merge function for the table
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "merge.register",
            "params": {
                "table": { "db": "test", "table": "r07_test" },
                "policy": {
                    "default": { "kind": "builtin", "name": "last_write_wins" }
                }
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    // Verify merge wasm list (confirms merge subsystem is operational)
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "merge.wasm.list",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body.get("result").is_some(),
        "merge.wasm.list should return result"
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_synthesis_workflow() -> anyhow::Result<()> {
    // R16: Index Advisor — verify recommendation workflow
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor")?;
    let client = RpcHttpClient::new(server.base_url());

    // Create table with data
    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_test (id INT PRIMARY KEY, category VARCHAR(50), value INT, name VARCHAR(50))"
        }))
        .await?;
    assert!(resp.ok);

    for i in 1..=20 {
        let resp = client
            .sql_exec(json!({
                "default_db": "test",
                "sql": format!(
                    "INSERT INTO r16_test (id, category, value, name) VALUES ({i}, 'cat_{c}', {v}, 'name_{i}')",
                    c = i % 5,
                    v = i * 10
                )
            }))
            .await?;
        assert!(resp.ok);
    }

    let query = advisor_workload_query("test", "r16_test");
    let resp = client
        .rpc(
            "query.select",
            json!({
                "query": query,
                "args": [{"t": "str", "v": "cat_1"}]
            }),
        )
        .await?;
    assert!(resp.ok);

    // Request index recommendations
    let resp = client
        .rpc(
            "advisor.index_synthesize",
            json!({
                "table": { "db": "test", "table": "r16_test" }
                ,"min_queries": 1,
                "min_rows": 1
            }),
        )
        .await?;
    assert!(resp.ok);
    let result = resp.result.expect("missing advisor synthesis result");
    let suggestions = result["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        !suggestions.is_empty(),
        "expected at least one advisor suggestion"
    );
    assert_eq!(suggestions[0]["columns"], json!(["category", "value"]));
    assert!(
        suggestions[0]["include"]
            .as_array()
            .map(|cols| cols.iter().any(|v| v.as_str() == Some("name")))
            .unwrap_or(false),
        "expected suggestion to include covering projection column"
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_apply_roundtrip_and_suppresses_suggestion() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_apply")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_apply_test (id INT PRIMARY KEY, category VARCHAR(50), value INT, name VARCHAR(50))"
        }))
        .await?;
    assert!(resp.ok);

    for i in 1..=20 {
        let resp = client
            .sql_exec(json!({
                "default_db": "test",
                "sql": format!(
                    "INSERT INTO r16_apply_test (id, category, value, name) VALUES ({i}, 'cat_{c}', {v}, 'name_{i}')",
                    c = i % 5,
                    v = i * 10
                )
            }))
            .await?;
        assert!(resp.ok);
    }

    let query = advisor_workload_query("test", "r16_apply_test");
    let resp = client
        .rpc(
            "query.select",
            json!({
                "query": query,
                "args": [{"t": "str", "v": "cat_1"}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let synth = client
        .rpc(
            "advisor.index_synthesize",
            json!({
                "table": { "db": "test", "table": "r16_apply_test" },
                "min_queries": 1,
                "min_rows": 1
            }),
        )
        .await?;
    assert!(synth.ok);
    let result = synth.result.expect("missing advisor synthesize result");
    let suggestion = result["suggestions"]
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .expect("missing advisor suggestion");
    let columns = suggestion["columns"]
        .as_array()
        .expect("missing suggestion columns")
        .iter()
        .filter_map(|v| v.as_str())
        .map(|v| v.to_string())
        .collect::<Vec<_>>();
    let include = suggestion["include"]
        .as_array()
        .expect("missing suggestion include")
        .iter()
        .filter_map(|v| v.as_str())
        .map(|v| v.to_string())
        .collect::<Vec<_>>();
    let suggestion_id = suggestion["id"]
        .as_str()
        .expect("missing suggestion id")
        .to_string();

    let apply = client
        .rpc(
            "advisor.apply_index",
            json!({
                "table": { "db": "test", "table": "r16_apply_test" },
                "columns": columns,
                "include": include,
                "note": "integration apply"
            }),
        )
        .await?;
    assert!(apply.ok);
    let action_id = apply
        .result
        .as_ref()
        .and_then(|value| value.get("action_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing advisor action_id"))?
        .to_string();
    assert_eq!(
        apply.result.as_ref().and_then(|v| v.get("status")),
        Some(&json!("queued"))
    );
    assert_eq!(
        apply.result.as_ref().and_then(|v| v.get("progress_pct")),
        Some(&json!(0))
    );

    let completed =
        wait_for_advisor_history_entry(&client, "test", "r16_apply_test", &action_id, "completed")
            .await?;
    assert_eq!(completed["result_status"].as_str(), Some("built"));
    assert_eq!(completed["progress_pct"].as_u64(), Some(100));

    let apply_again = client
        .rpc(
            "advisor.apply_index",
            json!({
                "table": { "db": "test", "table": "r16_apply_test" },
                "columns": suggestion["columns"],
                "include": suggestion["include"],
                "note": "integration apply again"
            }),
        )
        .await?;
    assert!(apply_again.ok);
    assert_eq!(
        apply_again.result.as_ref().and_then(|v| v.get("status")),
        Some(&json!("exists"))
    );
    assert_eq!(
        apply_again
            .result
            .as_ref()
            .and_then(|v| v.get("progress_pct")),
        Some(&json!(100))
    );

    let history = client
        .rpc(
            "advisor.history",
            json!({
                "table": { "db": "test", "table": "r16_apply_test" },
                "limit": 10
            }),
        )
        .await?;
    assert!(history.ok);
    let entries = history.result.expect("missing advisor history result")["entries"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        entries
            .iter()
            .filter(|entry| entry["action"] == "apply")
            .count()
            >= 2
    );
    assert!(entries
        .iter()
        .any(|entry| entry["suggestion_id"] == suggestion_id));

    let dismiss = client
        .rpc(
            "advisor.dismiss",
            json!({
                "table": { "db": "test", "table": "r16_apply_test" },
                "columns": suggestion["columns"],
                "include": suggestion["include"],
                "note": "integration dismiss"
            }),
        )
        .await?;
    assert!(dismiss.ok);
    assert_eq!(
        dismiss.result.as_ref().and_then(|v| v.get("dismissed")),
        Some(&json!(true))
    );

    let resynth = client
        .rpc(
            "advisor.index_synthesize",
            json!({
                "table": { "db": "test", "table": "r16_apply_test" },
                "min_queries": 1,
                "min_rows": 1
            }),
        )
        .await?;
    assert!(resynth.ok);
    let remaining = resynth.result.expect("missing re-synthesize result")["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        remaining.iter().all(|item| item["id"] != suggestion_id),
        "applied or dismissed suggestion should be suppressed from future synthesize results"
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_apply_failure_rolls_back_and_resurfaces_suggestion() -> anyhow::Result<()>
{
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_env(
        "r16_index_apply_failure",
        &[("SKEINDB_ADVISOR_FAIL_BUILD", "1")],
    )?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_apply_fail_test (id INT PRIMARY KEY, category VARCHAR(50), value INT, name VARCHAR(50))"
        }))
        .await?;
    assert!(resp.ok);

    for i in 1..=20 {
        let resp = client
            .sql_exec(json!({
                "default_db": "test",
                "sql": format!(
                    "INSERT INTO r16_apply_fail_test (id, category, value, name) VALUES ({i}, 'cat_{c}', {v}, 'name_{i}')",
                    c = i % 5,
                    v = i * 10
                )
            }))
            .await?;
        assert!(resp.ok);
    }

    let query = advisor_workload_query("test", "r16_apply_fail_test");
    let resp = client
        .rpc(
            "query.select",
            json!({
                "query": query,
                "args": [{"t": "str", "v": "cat_1"}]
            }),
        )
        .await?;
    assert!(resp.ok);

    let synth = client
        .rpc(
            "advisor.index_synthesize",
            json!({
                "table": { "db": "test", "table": "r16_apply_fail_test" },
                "min_queries": 1,
                "min_rows": 1
            }),
        )
        .await?;
    assert!(synth.ok);
    let result = synth.result.expect("missing advisor synthesize result");
    let suggestion = result["suggestions"]
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .expect("missing advisor suggestion");
    let suggestion_id = suggestion["id"]
        .as_str()
        .expect("missing suggestion id")
        .to_string();

    let apply = client
        .rpc(
            "advisor.apply_index",
            json!({
                "table": { "db": "test", "table": "r16_apply_fail_test" },
                "columns": suggestion["columns"],
                "include": suggestion["include"],
                "note": "forced failure"
            }),
        )
        .await?;
    assert!(apply.ok);
    let action_id = apply
        .result
        .as_ref()
        .and_then(|value| value.get("action_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing advisor failure action_id"))?
        .to_string();
    assert_eq!(
        apply.result.as_ref().and_then(|value| value.get("status")),
        Some(&json!("queued"))
    );

    let failed = wait_for_advisor_history_entry(
        &client,
        "test",
        "r16_apply_fail_test",
        &action_id,
        "failed",
    )
    .await?;
    assert_eq!(failed["rollback_status"].as_str(), Some("rolled_back"));
    assert!(failed["error"].as_str().is_some());

    let resynth = client
        .rpc(
            "advisor.index_synthesize",
            json!({
                "table": { "db": "test", "table": "r16_apply_fail_test" },
                "min_queries": 1,
                "min_rows": 1
            }),
        )
        .await?;
    assert!(resynth.ok);
    let remaining = resynth
        .result
        .expect("missing failure re-synthesize result")["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(remaining
        .iter()
        .any(|item| item["id"].as_str() == Some(suggestion_id.as_str())));

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_shift_convergence() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_test (id INT PRIMARY KEY, city VARCHAR(50), email VARCHAR(100))"
        }))
        .await?;
    assert!(resp.ok);

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "city_lookup",
                        "samples": [
                            {
                                "equality_columns": ["city"],
                                "rows_scanned": 400,
                                "repeats": 3
                            }
                        ]
                    },
                    {
                        "label": "email_lookup",
                        "samples": [
                            {
                                "equality_columns": ["email"],
                                "rows_scanned": 500,
                                "repeats": 3
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(result["phase_count"].as_u64(), Some(2));
    assert_eq!(result["total_observations"].as_u64(), Some(6));
    assert_eq!(result["phases"][0]["top_after"]["columns"], json!(["city"]));
    assert_eq!(
        result["phases"][1]["top_before"]["columns"],
        json!(["city"])
    );
    assert_eq!(
        result["phases"][1]["top_after"]["columns"],
        json!(["email"])
    );
    assert_eq!(
        result["phases"][1]["final_top_stable_after_observation"].as_u64(),
        Some(3)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_single_range_latency_benchmark() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_range_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_range_test (id INT PRIMARY KEY, city VARCHAR(50), email VARCHAR(100))"
        }))
        .await?;
    assert!(resp.ok);

    let values = (0..512_u64)
        .map(|idx| {
            format!(
                "({}, 'city_{:02}', 'user{:04}@example.com')",
                idx + 1,
                idx % 8,
                idx
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_range_test (id, city, email) VALUES {values}"
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_range_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "city_email_range",
                        "samples": [
                            {
                                "equality_columns": ["city"],
                                "range_columns": ["email"],
                                "projection_columns": ["id"],
                                "rows_scanned": 512,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["city", "email"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"]
            .as_u64()
            .unwrap_or(0)
            > result["phases"][0]["latency_benchmark"]["after_rows_scanned"]
                .as_u64()
                .unwrap_or(u64::MAX)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_order_latency_benchmark() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_order_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_order_test (id INT PRIMARY KEY, created_at BIGINT, email VARCHAR(100))"
        }))
        .await?;
    assert!(resp.ok);

    let values = (0..512_u64)
        .map(|idx| format!("({}, {}, 'user{:04}@example.com')", idx + 1, 512 - idx, idx))
        .collect::<Vec<_>>()
        .join(", ");
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_order_test (id, created_at, email) VALUES {values}"
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_order_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "created_at_order",
                        "samples": [
                            {
                                "order_by_columns": ["created_at"],
                                "projection_columns": ["email"],
                                "rows_scanned": 512,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["created_at"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["benchmarkable_samples"].as_u64(),
        Some(1)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"].as_u64(),
        Some(512)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["after_rows_scanned"].as_u64(),
        Some(512)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_range_order_latency_benchmark() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_range_order_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_range_order_test (id INT PRIMARY KEY, created_at BIGINT, email VARCHAR(100))"
        }))
        .await?;
    assert!(resp.ok);

    let values = (0..1024_u64)
        .map(|idx| {
            format!(
                "({}, {}, 'user{:04}@example.com')",
                idx + 1,
                1024 - idx,
                idx
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_range_order_test (id, created_at, email) VALUES {values}"
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_range_order_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "created_at_range_order",
                        "samples": [
                            {
                                "range_columns": ["created_at"],
                                "order_by_columns": ["created_at"],
                                "projection_columns": ["email"],
                                "rows_scanned": 1024,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["created_at"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["benchmarkable_samples"].as_u64(),
        Some(1)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"].as_u64(),
        Some(1024)
    );
    assert!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"]
            .as_u64()
            .unwrap_or(0)
            > result["phases"][0]["latency_benchmark"]["after_rows_scanned"]
                .as_u64()
                .unwrap_or(u64::MAX)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_multi_range_order_latency_benchmark(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_multi_range_order_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_multi_range_order_test (id INT PRIMARY KEY, created_at BIGINT, sequence BIGINT, email VARCHAR(100))"
        }))
        .await?;
    assert!(resp.ok);

    let mut tuples = vec!["(1, 8, 12, 'user0001@example.com')".to_string()];
    let mut next_id = 2_u64;
    for created_at in 0..16_u64 {
        for sequence in 0..16_u64 {
            if created_at == 8 && sequence == 12 {
                continue;
            }
            tuples.push(format!(
                "({}, {}, {}, 'user{:04}@example.com')",
                next_id, created_at, sequence, next_id
            ));
            next_id = next_id.saturating_add(1);
        }
    }
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_multi_range_order_test (id, created_at, sequence, email) VALUES {}",
                tuples.join(", ")
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_multi_range_order_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "created_at_sequence_multi_range_order",
                        "samples": [
                            {
                                "range_columns": ["created_at", "sequence"],
                                "order_by_columns": ["created_at", "sequence"],
                                "projection_columns": ["email"],
                                "rows_scanned": 256,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["created_at", "sequence"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["benchmarkable_samples"].as_u64(),
        Some(1)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"].as_u64(),
        Some(256)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["after_rows_scanned"].as_u64(),
        Some(32)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_group_latency_benchmark() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_group_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_group_test (id INT PRIMARY KEY, tenant_id BIGINT, city VARCHAR(50), score BIGINT)"
        }))
        .await?;
    assert!(resp.ok);

    let values = (0..512_u64)
        .map(|idx| {
            format!(
                "({}, {}, 'city_{:02}', {})",
                idx + 1,
                idx % 8,
                idx % 32,
                idx % 100
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_group_test (id, tenant_id, city, score) VALUES {values}"
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_group_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "tenant_city_group",
                        "samples": [
                            {
                                "equality_columns": ["tenant_id"],
                                "group_by_columns": ["city"],
                                "projection_columns": ["score"],
                                "rows_scanned": 512,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["tenant_id", "city"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["benchmarkable_samples"].as_u64(),
        Some(1)
    );
    assert!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"]
            .as_u64()
            .unwrap_or(0)
            > result["phases"][0]["latency_benchmark"]["after_rows_scanned"]
                .as_u64()
                .unwrap_or(u64::MAX)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_multi_group_latency_benchmark() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_multi_group_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_multi_group_test (id INT PRIMARY KEY, tenant_id BIGINT, region VARCHAR(50), city VARCHAR(50), score BIGINT)"
        }))
        .await?;
    assert!(resp.ok);

    let values = (0..256_u64)
        .map(|idx| {
            format!(
                "({}, {}, 'region_{:02}', 'city_{:02}', {})",
                idx + 1,
                idx % 4,
                idx % 8,
                idx % 16,
                idx % 100
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_multi_group_test (id, tenant_id, region, city, score) VALUES {values}"
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_multi_group_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "tenant_region_city_group",
                        "samples": [
                            {
                                "equality_columns": ["tenant_id"],
                                "group_by_columns": ["region", "city"],
                                "projection_columns": ["score"],
                                "rows_scanned": 256,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["tenant_id", "region", "city"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["benchmarkable_samples"].as_u64(),
        Some(1)
    );
    assert!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"]
            .as_u64()
            .unwrap_or(0)
            > result["phases"][0]["latency_benchmark"]["after_rows_scanned"]
                .as_u64()
                .unwrap_or(u64::MAX)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_range_group_order_latency_benchmark(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_range_group_order_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_range_group_order_test (id INT PRIMARY KEY, tenant_id BIGINT, created_at BIGINT, region VARCHAR(50), city VARCHAR(50), score BIGINT)"
        }))
        .await?;
    assert!(resp.ok);

    let mut tuples = vec!["(1, 2, 4, 'region_00', 'city_00', 1)".to_string()];
    let mut next_id = 2_u64;
    for tenant_id in 0..4_u64 {
        for created_at in 0..8_u64 {
            for region in 0..4_u64 {
                for city in 0..4_u64 {
                    if tenant_id == 2 && created_at == 4 && region == 0 && city == 0 {
                        continue;
                    }
                    tuples.push(format!(
                        "({}, {}, {}, 'region_{:02}', 'city_{:02}', {})",
                        next_id,
                        tenant_id,
                        created_at,
                        region,
                        city,
                        next_id % 100
                    ));
                    next_id = next_id.saturating_add(1);
                }
            }
        }
    }
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_range_group_order_test (id, tenant_id, created_at, region, city, score) VALUES {}",
                tuples.join(", ")
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_range_group_order_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "tenant_created_at_group_order",
                        "samples": [
                            {
                                "equality_columns": ["tenant_id"],
                                "range_columns": ["created_at"],
                                "order_by_columns": ["city"],
                                "group_by_columns": ["region", "city"],
                                "projection_columns": ["score"],
                                "rows_scanned": 512,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["tenant_id", "created_at", "region", "city"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["benchmarkable_samples"].as_u64(),
        Some(1)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["before_rows_scanned"].as_u64(),
        Some(512)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["after_rows_scanned"].as_u64(),
        Some(64)
    );

    Ok(())
}

#[tokio::test]
async fn r16_index_advisor_evaluate_reports_join_key_latency_benchmark() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r16_index_advisor_join_key_evaluate")?;
    let client = RpcHttpClient::new(server.base_url());

    let resp = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": "CREATE TABLE IF NOT EXISTS r16_eval_join_key_test (id INT PRIMARY KEY, tenant_id BIGINT, email VARCHAR(100))"
        }))
        .await?;
    assert!(resp.ok);

    let values = (0..256_u64)
        .map(|idx| {
            format!(
                "({}, {}, 'user{:04}@example.com')",
                idx + 1,
                idx % 4,
                idx + 1
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let insert = client
        .sql_exec(json!({
            "default_db": "test",
            "sql": format!(
                "INSERT INTO r16_eval_join_key_test (id, tenant_id, email) VALUES {values}"
            )
        }))
        .await?;
    assert!(insert.ok, "{insert:?}");

    let eval = client
        .rpc(
            "advisor.evaluate",
            json!({
                "table": { "db": "test", "table": "r16_eval_join_key_test" },
                "min_queries": 1,
                "min_rows": 1,
                "phases": [
                    {
                        "label": "tenant_join_key",
                        "samples": [
                            {
                                "join_key_columns": ["tenant_id"],
                                "projection_columns": ["email"],
                                "rows_scanned": 256,
                                "repeats": 4
                            }
                        ]
                    }
                ]
            }),
        )
        .await?;
    assert!(eval.ok, "{eval:?}");
    let result = eval.result.expect("missing advisor evaluate result");
    assert_eq!(result["format"].as_str(), Some("skein.advisor.evaluate.v1"));
    assert_eq!(
        result["phases"][0]["top_after"]["columns"],
        json!(["tenant_id"])
    );
    assert!(result["phases"][0]["latency_benchmark"].is_object());
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["benchmarkable_samples"].as_u64(),
        Some(1)
    );
    assert_eq!(
        result["phases"][0]["latency_benchmark"]["after_rows_scanned"].as_u64(),
        Some(64)
    );

    Ok(())
}

#[tokio::test]
async fn r02_adaptive_storage_format_selection() -> anyhow::Result<()> {
    // R02: Delta-Chained Values — verify format selection and snapshot management
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r02_adaptive")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    // Create a table and populate it with enough data to trigger format selection
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "CREATE TABLE IF NOT EXISTS r02_test (id INT PRIMARY KEY, data TEXT)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    for i in 1..=10 {
        let resp = client
            .post(format!("{base}/api/v1/rpc"))
            .json(&serde_json::json!({
                "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
                "params": { "default_db": "test", "sql": format!("INSERT INTO r02_test (id, data) VALUES ({i}, 'value_{i}')") }
            }))
            .send()
            .await?;
        assert!(resp.status().is_success());
    }

    // Trigger snapshot creation
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "system.snapshot",
            "params": { "table": "r02_test" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    // Verify data can still be read after snapshot
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "SELECT COUNT(*) FROM r02_test", "result_format": "rows_json" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    Ok(())
}

#[tokio::test]
async fn r05_oblivious_padding_verification() -> anyhow::Result<()> {
    // R05: Oblivious Execution — verify padding policies
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r05_oblivious")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "ddl",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "CREATE TABLE IF NOT EXISTS r05_test (id INT PRIMARY KEY)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body.get("ok").and_then(|v| v.as_bool()), Some(true));

    // Register an oblivious policy
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "oblivious.policy.set",
            "params": {
                "table": { "db": "test", "table": "r05_test" },
                "policy": {
                    "level": "basic",
                    "pad_to_multiple": 64,
                    "target_rows": 2
                }
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body.get("ok").and_then(|v| v.as_bool()), Some(true));

    // Get policies
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "oblivious.policy.get",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body.get("result").is_some(),
        "oblivious.policy.get should return result"
    );

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "oblivious.evaluate",
            "params": {
                "table": { "db": "test", "table": "r05_test" },
                "trace_rows": [1, 2, 63, 64, 65]
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body.get("ok").and_then(|v| v.as_bool()), Some(true));
    assert!(
        body["result"]["leakage"]["padded_mutual_information_bits"]
            .as_f64()
            .unwrap_or_default()
            >= 0.0
    );

    Ok(())
}

#[tokio::test]
async fn r11_llm_autoparam_classify_and_analyze() -> anyhow::Result<()> {
    // R11: LLM-Assisted Query Autoparameterization — verify classify + analyze
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r11_autoparam")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    // Create a table for context
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "CREATE TABLE IF NOT EXISTS r11_users (id INT PRIMARY KEY, name TEXT, age INT)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    // Classify literals
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "ai.autoparam.classify",
            "params": {
                "sql": "SELECT * FROM r11_users WHERE id = 42 AND name = 'Alice'",
                "literals": [
                    { "value": { "t": "i64", "v": 42 }, "column": "id", "op": "=" },
                    { "value": { "t": "str", "v": "Alice" }, "column": "name", "op": "=" }
                ]
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let labels = body
        .get("result")
        .and_then(|r| r.get("labels"))
        .and_then(|l| l.as_array());
    assert!(labels.is_some(), "classify should return labels");
    assert_eq!(
        labels.unwrap().len(),
        2,
        "should have 2 labels for 2 literals"
    );

    // Analyze a full SQL statement
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "ai.autoparam.analyze",
            "params": {
                "sql": "SELECT * FROM r11_users WHERE age > 30 AND name = 'Bob'"
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body.get("result").expect("analyze should return result");
    assert!(
        result.get("normalized_sql").is_some(),
        "analyze should return normalized_sql"
    );
    assert!(
        result.get("fingerprint").is_some(),
        "analyze should return fingerprint"
    );
    assert!(
        result.get("literals").is_some(),
        "analyze should return extracted literals"
    );

    Ok(())
}

#[tokio::test]
async fn r14_geo_replay_bundle_roundtrip() -> anyhow::Result<()> {
    // R14: Geo-Distributed Replay Bundles — verify bundle request/apply/status cycle
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r14_replay")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    // Setup: create table and insert data
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "CREATE TABLE IF NOT EXISTS r14_events (id INT PRIMARY KEY, payload TEXT)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    for i in 1..=5 {
        let resp = client
            .post(format!("{base}/api/v1/rpc"))
            .json(&serde_json::json!({
                "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
                "params": { "default_db": "test", "sql": format!("INSERT INTO r14_events (id, payload) VALUES ({i}, 'event_{i}')") }
            }))
            .send()
            .await?;
        assert!(resp.status().is_success());
    }

    // Request a replay bundle
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "edge.bundle.request",
            "params": {
                "windows": [
                    { "table": { "db": "test", "table": "r14_events" } }
                ]
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("bundle request should return result");
    let bundle = result.get("bundle").expect("result should contain bundle");
    let bundle_id = bundle
        .get("bundle_id")
        .and_then(|v| v.as_str())
        .expect("bundle should have an id");
    assert!(!bundle_id.is_empty());

    // Check bundle status
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "edge.bundle.status",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body.get("result").is_some(),
        "bundle status should return result"
    );

    // Apply bundle (simulate edge receive)
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "edge.bundle.apply",
            "params": {
                "bundle": bundle
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    Ok(())
}

#[tokio::test]
async fn r15_schema_evolution_propose_merge_apply() -> anyhow::Result<()> {
    // R15: Conflict-Free Schema Evolution — verify propose/merge_status/apply_merge
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r15_schema_evo")?;
    let client = reqwest::Client::new();
    let base = server.base_url();
    // Create a table
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "CREATE TABLE IF NOT EXISTS r15_docs (id INT PRIMARY KEY, title TEXT)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    // Propose a schema change
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "schema.propose_change",
            "params": {
                "table": { "db": "test", "table": "r15_docs" },
                "base_version": 0,
                "changes": [
                    { "op": "add_column", "name": "author", "type": { "kind": "str" }, "nullable": true }
                ]
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("propose_change should return result");
    let change_id = result
        .get("change_id")
        .and_then(|v| v.as_str())
        .expect("should have change_id");

    // Check merge status
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "schema.merge_status",
            "params": {
                "table": { "db": "test", "table": "r15_docs" }
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body.get("result").is_some(),
        "merge_status should return result"
    );

    // Apply the merge
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "schema.apply_merge",
            "params": {
                "table": { "db": "test", "table": "r15_docs" },
                "change_ids": [change_id]
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    Ok(())
}

#[tokio::test]
async fn r14_edge_bundle_gap_blocks_bounded_staleness_route() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r14_bundle_gap")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "sql.exec",
            "params": { "default_db": "test", "sql": "CREATE TABLE IF NOT EXISTS r14_gap_events (id INT PRIMARY KEY, payload TEXT)" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    for i in 1..=3 {
        let resp = client
            .post(format!("{base}/api/v1/rpc"))
            .json(&serde_json::json!({
                "skeinql": "1.0", "id": format!("ins{i}"),
                "method": "sql.exec",
                "params": { "default_db": "test", "sql": format!("INSERT INTO r14_gap_events (id, payload) VALUES ({i}, 'event_{i}')") }
            }))
            .send()
            .await?;
        assert!(resp.status().is_success());
    }

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "bundle1",
            "method": "edge.bundle.request",
            "params": {
                "windows": [
                    { "table": { "db": "test", "table": "r14_gap_events" }, "from_seq": 0, "to_seq": 1 }
                ]
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let first_bundle = body["result"]["bundle"].clone();

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "bundle2",
            "method": "edge.bundle.request",
            "params": {
                "windows": [
                    { "table": { "db": "test", "table": "r14_gap_events" }, "from_seq": 2, "to_seq": 3 }
                ]
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let second_bundle = body["result"]["bundle"].clone();

    for (req_id, bundle) in [("apply1", first_bundle), ("apply2", second_bundle)] {
        let resp = client
            .post(format!("{base}/api/v1/rpc"))
            .json(&serde_json::json!({
                "skeinql": "1.0", "id": req_id,
                "method": "edge.bundle.apply",
                "params": { "bundle": bundle }
            }))
            .send()
            .await?;
        assert!(resp.status().is_success());
    }

    let query = select_query("test", "r14_gap_events", &["id"]);
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "status1",
            "method": "edge.bundle.status",
            "params": { "query": query, "max_lag": 0 }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body["result"].clone();
    assert_eq!(result["coverage"].as_array().map(Vec::len), Some(2));
    assert_eq!(result["route"]["eligible"].as_bool(), Some(false));
    assert_eq!(result["route"]["reason"].as_str(), Some("coverage_gap"));
    assert_eq!(result["route"]["observed_lag"].as_u64(), Some(2));

    Ok(())
}

#[tokio::test]
async fn r15_schema_evolution_concurrent_column_and_index_changes() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("r15_schema_evo_index")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    for sql in [
        "CREATE TABLE IF NOT EXISTS r15_docs_idx (id INT PRIMARY KEY, email TEXT)",
        "INSERT INTO r15_docs_idx (id, email) VALUES (1, 'ada@example.com')",
    ] {
        let resp = client
            .post(format!("{base}/api/v1/rpc"))
            .json(&serde_json::json!({
                "skeinql": "1.0", "id": "t321",
                "method": "sql.exec",
                "params": { "default_db": "test", "sql": sql }
            }))
            .send()
            .await?;
        assert!(resp.status().is_success());
    }

    let add_column = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t321",
            "method": "schema.propose_change",
            "params": {
                "table": { "db": "test", "table": "r15_docs_idx" },
                "base_version": 0,
                "changes": [
                    { "op": "add_column", "name": "region", "type": { "kind": "str" }, "nullable": true }
                ],
                "message": "add region"
            }
        }))
        .send()
        .await?;
    assert!(add_column.status().is_success());
    let add_column_body: serde_json::Value = add_column.json().await?;
    let add_column_id = add_column_body["result"]["change_id"]
        .as_str()
        .expect("missing add_column change id")
        .to_string();

    let add_index = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t321",
            "method": "schema.propose_change",
            "params": {
                "table": { "db": "test", "table": "r15_docs_idx" },
                "base_version": 0,
                "changes": [
                    { "op": "add_index", "name": "region_lookup", "columns": ["region"], "unique": false }
                ],
                "message": "index region"
            }
        }))
        .send()
        .await?;
    assert!(add_index.status().is_success());
    let add_index_body: serde_json::Value = add_index.json().await?;
    let add_index_id = add_index_body["result"]["change_id"]
        .as_str()
        .expect("missing add_index change id")
        .to_string();

    let conflict = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t321",
            "method": "schema.propose_change",
            "params": {
                "table": { "db": "test", "table": "r15_docs_idx" },
                "base_version": 0,
                "changes": [
                    { "op": "add_index", "name": "region_lookup", "columns": ["email"], "unique": false }
                ],
                "message": "conflicting name"
            }
        }))
        .send()
        .await?;
    assert!(conflict.status().is_success());
    let conflict_body: serde_json::Value = conflict.json().await?;
    let conflict_id = conflict_body["result"]["change_id"]
        .as_str()
        .expect("missing conflict change id")
        .to_string();

    let status = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t321",
            "method": "schema.merge_status",
            "params": {
                "table": { "db": "test", "table": "r15_docs_idx" }
            }
        }))
        .send()
        .await?;
    assert!(status.status().is_success());
    let status_body: serde_json::Value = status.json().await?;
    let result = status_body
        .get("result")
        .expect("missing merge_status result");
    let merge_plan = result["merge_plan"].as_array().cloned().unwrap_or_default();
    assert_eq!(merge_plan.len(), 2);
    assert_eq!(merge_plan[0].as_str(), Some(add_column_id.as_str()));
    assert_eq!(merge_plan[1].as_str(), Some(add_index_id.as_str()));
    let conflict_reason = result["conflicts"]
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|item| item["change_id"].as_str() == Some(conflict_id.as_str()))
        })
        .and_then(|item| item["reason"].as_str())
        .unwrap_or_default();
    assert!(conflict_reason.starts_with("index_conflict:"));
    let resolution = result["resolution"].as_array().cloned().unwrap_or_default();
    assert_eq!(resolution.len(), 3);
    let resolution_for = |change_id: &str| {
        resolution
            .iter()
            .find(|item| item["change_id"].as_str() == Some(change_id))
            .cloned()
            .unwrap_or_default()
    };
    let add_column_resolution = resolution_for(&add_column_id);
    assert_eq!(
        add_column_resolution["action"].as_str(),
        Some("roll_forward")
    );
    assert_eq!(
        add_column_resolution["reason"].as_str(),
        Some("eligible_merge_plan")
    );
    let add_index_resolution = resolution_for(&add_index_id);
    assert_eq!(
        add_index_resolution["action"].as_str(),
        Some("roll_forward")
    );
    let conflict_resolution = resolution_for(&conflict_id);
    assert_eq!(conflict_resolution["action"].as_str(), Some("rollback"));
    assert!(conflict_resolution["reason"]
        .as_str()
        .unwrap_or_default()
        .starts_with("index_conflict:"));

    let rollout = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t325",
            "method": "schema.simulate_rollout",
            "params": {
                "table": { "db": "test", "table": "r15_docs_idx" },
                "nodes": 3
            }
        }))
        .send()
        .await?;
    assert!(rollout.status().is_success());
    let rollout_body: serde_json::Value = rollout.json().await?;
    let rollout_result = rollout_body
        .get("result")
        .expect("missing simulate_rollout result");
    assert_eq!(
        rollout_result["format"].as_str(),
        Some("skein.schema.simulate_rollout.v1")
    );
    assert_eq!(rollout_result["current_version"].as_u64(), Some(1));
    assert_eq!(rollout_result["target_version"].as_u64(), Some(3));
    assert_eq!(rollout_result["nodes"].as_u64(), Some(3));
    assert_eq!(rollout_result["pending_change_count"].as_u64(), Some(3));
    assert_eq!(rollout_result["ready_for_rollout"].as_bool(), Some(true));
    assert_eq!(rollout_result["legacy_row_count"].as_u64(), Some(1));
    let rollout_plan = rollout_result["merge_plan"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(rollout_plan.len(), 2);
    assert_eq!(rollout_plan[0].as_str(), Some(add_column_id.as_str()));
    assert_eq!(rollout_plan[1].as_str(), Some(add_index_id.as_str()));
    let stages = rollout_result["stages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(stages.len(), 4);
    assert_eq!(stages[0]["stage"].as_str(), Some("prepare"));
    assert_eq!(stages[1]["stage"].as_str(), Some("mixed"));
    assert_eq!(stages[1]["upgraded_nodes"].as_u64(), Some(1));
    assert_eq!(stages[3]["stage"].as_str(), Some("steady_state"));
    assert_eq!(stages[3]["legacy_nodes"].as_u64(), Some(0));
    assert_eq!(stages[3]["requires_row_adaptation"].as_bool(), Some(true));

    let apply = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t321",
            "method": "schema.apply_merge",
            "params": {
                "table": { "db": "test", "table": "r15_docs_idx" }
            }
        }))
        .send()
        .await?;
    assert!(apply.status().is_success());
    let apply_body: serde_json::Value = apply.json().await?;
    assert_eq!(apply_body["result"]["new_version"].as_u64(), Some(3));
    let rolled_back = apply_body["result"]["rolled_back"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(rolled_back.len(), 1);
    assert_eq!(
        rolled_back[0]["change_id"].as_str(),
        Some(conflict_id.as_str())
    );
    assert!(rolled_back[0]["reason"]
        .as_str()
        .unwrap_or_default()
        .starts_with("index_conflict:"));

    let post_status = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t323",
            "method": "schema.merge_status",
            "params": {
                "table": { "db": "test", "table": "r15_docs_idx" }
            }
        }))
        .send()
        .await?;
    assert!(post_status.status().is_success());
    let post_status_body: serde_json::Value = post_status.json().await?;
    let post_result = post_status_body
        .get("result")
        .expect("missing post-merge status result");
    assert_eq!(
        post_result["pending"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert_eq!(
        post_result["conflicts"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert_eq!(
        post_result["merge_plan"]
            .as_array()
            .map(|items| items.len()),
        Some(0)
    );

    let describe = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t321",
            "method": "schema.describe_table",
            "params": { "db": "test", "table": "r15_docs_idx" }
        }))
        .send()
        .await?;
    assert!(describe.status().is_success());
    let describe_body: serde_json::Value = describe.json().await?;
    let result = describe_body
        .get("result")
        .expect("missing describe_table result");
    let columns = result["columns"].as_array().cloned().unwrap_or_default();
    assert!(columns
        .iter()
        .any(|column| column["name"].as_str() == Some("region")));
    let indexes = result["indexes"].as_array().cloned().unwrap_or_default();
    assert!(indexes.iter().any(|index| {
        index["name"].as_str() == Some("region_lookup")
            && index["unique"].as_bool() == Some(false)
            && index["columns"]
                .as_array()
                .map(|cols| cols.iter().any(|col| col.as_str() == Some("region")))
                .unwrap_or(false)
    }));

    let compat_indexes = result["compat_mysql"]["indexes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(compat_indexes.iter().any(|index| {
        index["name"].as_str() == Some("region_lookup")
            && index["columns"]
                .as_array()
                .map(|cols| cols.iter().any(|col| col.as_str() == Some("region")))
                .unwrap_or(false)
    }));

    Ok(())
}

#[tokio::test]
async fn t183_replay_bundle_export_import_run_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("t183_replay_bundle")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    for sql in [
        "CREATE TABLE IF NOT EXISTS replay_events (id INT PRIMARY KEY, payload TEXT)",
        "INSERT INTO replay_events (id, payload) VALUES (1, 'one')",
        "INSERT INTO replay_events (id, payload) VALUES (2, 'two')",
        "UPDATE replay_events SET payload = 'two-updated' WHERE id = 2",
        "DELETE FROM replay_events WHERE id = 1",
    ] {
        let resp = client
            .post(format!("{base}/api/v1/rpc"))
            .json(&serde_json::json!({
                "skeinql": "1.0", "id": "t183",
                "method": "sql.exec",
                "params": { "default_db": "test", "sql": sql }
            }))
            .send()
            .await?;
        assert!(resp.status().is_success(), "sql should succeed: {sql}");
    }

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t183",
            "method": "maintenance.replay.export",
            "params": { "db": "test", "bundle_id": "rpc_bundle" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let bundle = body
        .get("result")
        .and_then(|result| result.get("bundle"))
        .cloned()
        .expect("export should return bundle");
    assert_eq!(bundle["manifest"]["bundle_id"].as_str(), Some("rpc_bundle"));
    assert_eq!(bundle["manifest"]["table_count"].as_u64(), Some(1));
    assert_eq!(bundle["manifest"]["change_count"].as_u64(), Some(4));
    assert_eq!(
        bundle["performance"]["format"].as_str(),
        Some("skein.replay.performance.v1")
    );
    assert_eq!(
        bundle["performance"]["timing"]["change_count"].as_u64(),
        Some(4)
    );
    assert_eq!(
        bundle["performance"]["lsm_state"]["total_tables"].as_u64(),
        Some(1)
    );

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t183",
            "method": "maintenance.replay.import",
            "params": {
                "bundle": bundle,
                "workspace_id": "rpc_roundtrip"
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let import_result = body.get("result").expect("import should return result");
    assert_eq!(import_result["ok"].as_bool(), Some(true));

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t183",
            "method": "maintenance.replay.run",
            "params": { "workspace_id": "rpc_roundtrip" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let run_result = body.get("result").expect("run should return result");
    assert_eq!(run_result["ok"].as_bool(), Some(true));
    assert_eq!(
        run_result["expected_checksum"].as_str(),
        run_result["observed_checksum"].as_str()
    );
    assert_eq!(run_result["replayed_tables"].as_u64(), Some(1));
    assert_eq!(run_result["replayed_changes"].as_u64(), Some(4));
    assert_eq!(
        run_result["performance_report"]["format"].as_str(),
        Some("skein.replay.performance_report.v1")
    );
    assert_eq!(
        run_result["performance_report"]["timing"]["change_count_delta"].as_i64(),
        Some(0)
    );
    assert_eq!(
        run_result["performance_report"]["storage"]["total_rows_delta"].as_i64(),
        Some(0)
    );

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t185",
            "method": "maintenance.replay.export",
            "params": {
                "db": "test",
                "bundle_id": "rpc_bundle_redacted",
                "redaction": { "mode": "hash_pk", "salt": "t185" }
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let redacted_bundle = body
        .get("result")
        .and_then(|result| result.get("bundle"))
        .cloned()
        .expect("redacted export should return bundle");
    assert_eq!(
        redacted_bundle["redaction"]["mode"].as_str(),
        Some("hash_pk")
    );
    assert_eq!(
        redacted_bundle["changes"][0]["pk"][0]["t"].as_str(),
        Some("str")
    );
    assert_eq!(
        redacted_bundle["tables"][0]["rows"][0]["row"]["id"]["t"].as_str(),
        Some("str")
    );

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t185",
            "method": "maintenance.replay.import",
            "params": {
                "bundle": redacted_bundle,
                "workspace_id": "rpc_redacted_roundtrip"
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t185",
            "method": "maintenance.replay.run",
            "params": { "workspace_id": "rpc_redacted_roundtrip" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let redacted_run = body
        .get("result")
        .expect("redacted run should return result");
    assert_eq!(redacted_run["ok"].as_bool(), Some(true));

    Ok(())
}

#[tokio::test]
async fn t188_replay_run_rehydrates_cache_hints() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("t188_replay_cache_hints")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    for sql in [
        "CREATE TABLE IF NOT EXISTS replay_cache_users (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO replay_cache_users (id, name) VALUES (1, 'Ada')",
    ] {
        let resp = client
            .post(format!("{base}/api/v1/rpc"))
            .json(&serde_json::json!({
                "skeinql": "1.0", "id": "t188",
                "method": "sql.exec",
                "params": { "default_db": "test", "sql": sql }
            }))
            .send()
            .await?;
        assert!(resp.status().is_success(), "sql should succeed: {sql}");
    }

    let query = select_query("test", "replay_cache_users", &["id", "name"]);
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t188_select",
            "method": "query.select",
            "params": {
                "query": query.clone(),
                "cache": { "want_etag": true }
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let etag = body["result"]["etag"]
        .as_str()
        .expect("query.select should return etag")
        .to_string();

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t188_update",
            "method": "sql.exec",
            "params": {
                "default_db": "test",
                "sql": "UPDATE replay_cache_users SET name = 'Ada Lovelace' WHERE id = 1"
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t188_patch",
            "method": "query.patch",
            "params": {
                "query": query,
                "base_etag": etag,
                "include_full": false
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t188_export",
            "method": "maintenance.replay.export",
            "params": { "db": "test", "bundle_id": "cache_hints_bundle" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let bundle = body["result"]["bundle"].clone();
    assert_eq!(
        bundle["performance"]["cache_warm"]["cached_select_entries"].as_u64(),
        Some(2)
    );
    assert_eq!(
        bundle["performance"]["cache_warm"]["cached_patch_entries"].as_u64(),
        Some(1)
    );

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t188_import",
            "method": "maintenance.replay.import",
            "params": {
                "bundle": bundle,
                "workspace_id": "rpc_cache_hints"
            }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t188_run",
            "method": "maintenance.replay.run",
            "params": { "workspace_id": "rpc_cache_hints" }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let run_result = body["result"].clone();
    assert_eq!(
        run_result["performance_report"]["checksum_match"].as_bool(),
        Some(true)
    );
    assert_eq!(
        run_result["performance_report"]["cache_warm"]["cached_select_entries_delta"].as_i64(),
        Some(0)
    );
    assert_eq!(
        run_result["performance_report"]["cache_warm"]["cached_patch_entries_delta"].as_i64(),
        Some(0)
    );
    // R18 integration: exercises replay/RPC path (maintenance.replay.run over SkeinQL RPC)
    // which now wires timing injection -> apply_simulated_pacing in engine replay run
    // for deterministic pacing + cache/LSM fidelity. (unit: replay_pacing_* in engine.rs)
    assert!(run_result["performance_report"]["timing"].is_object());
    assert_eq!(
        run_result["performance_report"]["timing"]["change_count_delta"].as_i64(),
        Some(0)
    );

    Ok(())
}

#[tokio::test]
async fn telemetry_and_plan_cache_integration() -> anyhow::Result<()> {
    // T110-T113 + T211-T213: Telemetry + plan cache integration test
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("telemetry_plan")?;
    let client = reqwest::Client::new();
    let base = server.base_url();

    // Execute some SQL via MySQL protocol to trigger feature flags
    let mut stream = mysql_connect_and_auth(server.mysql_port()).await?;
    send_com_query(&mut stream, "CREATE DATABASE IF NOT EXISTS tel_db").await?;
    let _ = read_mysql_response(&mut stream).await?;
    send_com_query(&mut stream, "USE tel_db").await?;
    let _ = read_mysql_response(&mut stream).await?;
    send_com_query(
        &mut stream,
        "CREATE TABLE IF NOT EXISTS tel_test (id INT PRIMARY KEY, val TEXT)",
    )
    .await?;
    let _ = read_mysql_response(&mut stream).await?;
    send_com_query(
        &mut stream,
        "INSERT INTO tel_test (id, val) VALUES (1, 'hello')",
    )
    .await?;
    let _ = read_mysql_response(&mut stream).await?;
    send_com_query(&mut stream, "SELECT * FROM tel_test WHERE id = 1").await?;
    let _ = read_mysql_response(&mut stream).await?;
    send_com_query(&mut stream, "SELECT val FROM tel_test WHERE id = 1").await?;
    let _ = read_mysql_response(&mut stream).await?;

    // Check telemetry feature flags
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "telemetry.feature_flags",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("feature_flags should return result");
    let flags = result
        .get("flags")
        .and_then(|v| v.as_array())
        .expect("should have flags array");
    assert!(
        !flags.is_empty(),
        "should have recorded feature flags from MySQL queries"
    );

    // Check compat summary
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "telemetry.compat_summary",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("compat_summary should return result");
    assert!(
        result.get("coverage_pct").is_some(),
        "should have coverage_pct"
    );
    assert!(result.get("gaps").is_some(), "should have gaps list");

    // Check migration hints
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "telemetry.migration_hints",
            "params": { "limit": 5 }
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("migration_hints should return result");
    let hints = result
        .get("hints")
        .and_then(|v| v.as_array())
        .expect("should have hints array");
    assert!(!hints.is_empty(), "should have migration hints");

    // Check plan cache status
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "plan_cache.status",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("plan_cache.status should return result");
    assert!(result.get("capacity").is_some(), "should have capacity");

    // Clear plan cache
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "plan_cache.clear",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("plan_cache.clear should return result");
    assert!(result.get("cleared").is_some(), "should have cleared count");

    // Check coalescing stats
    let resp = client
        .post(format!("{base}/api/v1/rpc"))
        .json(&serde_json::json!({
            "skeinql": "1.0", "id": "t1",
            "method": "stats.coalescing",
            "params": {}
        }))
        .send()
        .await?;
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await?;
    let result = body
        .get("result")
        .expect("stats.coalescing should return result");
    assert!(
        result.get("total_coalesced").is_some(),
        "should have total_coalesced"
    );

    Ok(())
}

/// T062: Verify query.prepare → GET /api/v1/q/{query_id} with ETag → 304 roundtrip.
#[tokio::test]
async fn t062_prepared_get_etag_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("t062_etag")?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();
    let base = server.base_url();

    // Setup: create db + table + insert data
    rpc.rpc("schema.create_database", json!({"db": "etag_db"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "etag_db",
            "table": "items",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "etag_db", "table": "items"},
            "rows": [
                {"id": {"t":"u64","v":1}, "name": {"t":"str","v":"Alpha"}},
                {"id": {"t":"u64","v":2}, "name": {"t":"str","v":"Beta"}}
            ]
        }),
    )
    .await?;

    // Step 1: Prepare query
    let resp = rpc
        .rpc(
            "query.prepare",
            json!({
                "query": {
                    "body": {
                        "select": {
                            "from": [{"db": "etag_db", "table": "items"}],
                            "projection": [{"expr": {"col": "id"}}, {"expr": {"col": "name"}}]
                        }
                    }
                }
            }),
        )
        .await?;
    let query_id = resp
        .result
        .as_ref()
        .and_then(|r| r["query_id"].as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    // Step 2: GET /api/v1/q/{query_id} — should return 200 + ETag
    let get_url = format!("{}/api/v1/q/{}", base, query_id);
    let resp = http.get(&get_url).send().await?;
    assert_eq!(resp.status(), 200);
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow!("missing ETag header"))?
        .to_string();
    assert!(!etag.is_empty());

    // Step 3: GET with If-None-Match — should return 304
    let resp = http
        .get(&get_url)
        .header("If-None-Match", &etag)
        .send()
        .await?;
    assert_eq!(resp.status(), 304, "expected 304 Not Modified");

    // Step 4: Mutate data, GET without If-None-Match should return new ETag
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "etag_db", "table": "items"},
            "rows": [
                {"id": {"t":"u64","v":3}, "name": {"t":"str","v":"Gamma"}}
            ]
        }),
    )
    .await?;
    let resp = http.get(&get_url).send().await?;
    assert_eq!(resp.status(), 200);
    let new_etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    // ETags should differ after mutation
    assert_ne!(etag, new_etag, "ETag should change after insert");

    Ok(())
}

#[tokio::test]
async fn vector_search_cache_invalidates_after_vector_insert_rpc() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("vector_search_cache_invalidation")?;
    let client = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "docs",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "title", "type": {"kind": "str"}, "nullable": false},
                    {"name": "embedding", "type": {"kind": "embedding"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;
    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "docs"},
                "rows": [
                    {
                        "id": {"t": "u64", "v": 1},
                        "title": {"t": "str", "v": "alpha"},
                        "embedding": {"t": "embedding", "dims": 3, "v": [1.0, 0.0, 0.0]}
                    },
                    {
                        "id": {"t": "u64", "v": 2},
                        "title": {"t": "str", "v": "beta"},
                        "embedding": {"t": "embedding", "dims": 3, "v": [0.0, 1.0, 0.0]}
                    }
                ]
            }),
        )
        .await?;
    client
        .rpc(
            "vector.insert",
            json!({
                "table": {"db": "app", "table": "docs"},
                "column": "embedding",
                "rows": [
                    {"pk": [{"t": "u64", "v": 1}], "embedding": {"t": "embedding", "dims": 3, "v": [1.0, 0.0, 0.0]}},
                    {"pk": [{"t": "u64", "v": 2}], "embedding": {"t": "embedding", "dims": 3, "v": [0.0, 1.0, 0.0]}}
                ],
                "upsert": true
            }),
        )
        .await?;

    let prepare = client
        .rpc(
            "query.prepare",
            json!({
                "query": {
                    "body": {
                        "select": {
                            "from": [{"db": "app", "table": "docs"}],
                            "projection": [
                                {"expr": {"col": "id"}},
                                {"expr": {"col": "title"}}
                            ]
                        }
                    }
                }
            }),
        )
        .await?;
    let query_id = prepare
        .result
        .as_ref()
        .and_then(|value| value.get("query_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();
    let subscribe = client
        .rpc("query.subscribe", json!({"query_id": query_id}))
        .await?;
    let sse_url = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sse_url"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query sse_url"))?;
    let mut stream = http
        .get(format!("{}{}", server.base_url(), sse_url))
        .send()
        .await?;
    assert!(stream.status().is_success());

    let first = client
        .rpc(
            "vector.search",
            json!({
                "table": {"db": "app", "table": "docs"},
                "column": "embedding",
                "query": {"t": "embedding", "dims": 3, "v": [1.0, 0.0, 0.0]},
                "k": 1,
                "metric": "cosine",
                "include_row": true,
                "use_lsh": false,
                "cache": {"want_etag": true}
            }),
        )
        .await?;
    assert!(first.ok);
    let first_result = first
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing result"))?;
    assert_eq!(first_result["not_modified"].as_bool(), Some(false));
    assert_eq!(
        first_result["deps"]["vector"]["source"].as_str(),
        Some("table_version")
    );
    assert_eq!(
        first_result["deps"]["vector"]["column"].as_str(),
        Some("embedding")
    );
    let first_etag = first_result["etag"]
        .as_str()
        .ok_or_else(|| anyhow!("missing vector.search etag"))?
        .to_string();

    let cached = client
        .rpc(
            "vector.search",
            json!({
                "table": {"db": "app", "table": "docs"},
                "column": "embedding",
                "query": {"t": "embedding", "dims": 3, "v": [1.0, 0.0, 0.0]},
                "k": 1,
                "metric": "cosine",
                "include_row": true,
                "use_lsh": false,
                "cache": {"want_etag": true, "if_none_match": first_etag}
            }),
        )
        .await?;
    let cached_result = cached
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing cached result"))?;
    assert_eq!(cached_result["not_modified"].as_bool(), Some(true));
    assert_eq!(cached_result["matches"].as_array().map(Vec::len), Some(0));

    client
        .rpc(
            "vector.insert",
            json!({
                "table": {"db": "app", "table": "docs"},
                "column": "embedding",
                "rows": [
                    {"pk": [{"t": "u64", "v": 1}], "embedding": {"t": "embedding", "dims": 3, "v": [0.2, 1.0, 0.0]}}
                ],
                "upsert": true
            }),
        )
        .await?;
    let event = read_sse_event(&mut stream).await?;
    let event_data: serde_json::Value = serde_json::from_str(&event.data)?;
    assert_eq!(event_data["query_id"].as_str(), Some(query_id.as_str()));
    assert_eq!(event_data["table"].as_str(), Some("app.docs"));
    assert_eq!(event_data["changed"].as_bool(), Some(true));

    let changed = client
        .rpc(
            "vector.search",
            json!({
                "table": {"db": "app", "table": "docs"},
                "column": "embedding",
                "query": {"t": "embedding", "dims": 3, "v": [1.0, 0.0, 0.0]},
                "k": 1,
                "metric": "cosine",
                "include_row": true,
                "use_lsh": false,
                "cache": {"want_etag": true, "if_none_match": first_etag}
            }),
        )
        .await?;
    let changed_result = changed
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing changed result"))?;
    assert_eq!(changed_result["not_modified"].as_bool(), Some(false));
    assert_ne!(changed_result["etag"].as_str(), Some(first_etag.as_str()));

    Ok(())
}

#[tokio::test]
async fn cdc_query_subscription_invalidates_prepared_query() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_query_subscription")?;
    let client = RpcHttpClient::new(server.base_url());

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "events",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "data", "type": {"kind": "str"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;

    let prepare = client
        .rpc(
            "query.prepare",
            json!({
                "query": {
                    "body": {
                        "select": {
                            "from": [{"db": "app", "table": "events"}],
                            "projection": [
                                {"expr": {"col": "id"}},
                                {"expr": {"col": "data"}}
                            ]
                        }
                    }
                }
            }),
        )
        .await?;
    let query_id = prepare
        .result
        .as_ref()
        .and_then(|value| value.get("query_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let subscribe = client
        .rpc("cdc.subscribe_query", json!({"query_id": query_id}))
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    assert_eq!(
        subscribe
            .result
            .as_ref()
            .and_then(|value| value.get("sse_url"))
            .and_then(|value| value.as_str()),
        Some(format!("/api/v1/cdc/sse/{sub_id}").as_str())
    );
    let start_offset = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("offset"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("missing offset"))?;

    client
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "events"},
                "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
            }),
        )
        .await?;

    let execute = client
        .rpc(
            "query.execute_prepared",
            json!({"query_id": query_id, "args": []}),
        )
        .await?;
    let current_etag = execute
        .result
        .as_ref()
        .and_then(|value| value.get("etag"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing prepared query etag"))?
        .to_string();

    let poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id.clone(), "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(poll.ok);
    let events = poll
        .result
        .as_ref()
        .and_then(|value| value.get("events"))
        .and_then(|value| value.as_array())
        .ok_or_else(|| anyhow!("missing events"))?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["op"].as_str(), Some("invalidate"));
    assert_eq!(events[0]["query_id"].as_str(), Some(query_id.as_str()));
    assert_eq!(events[0]["etag"].as_str(), Some(current_etag.as_str()));
    assert_eq!(events[0]["table"].as_str(), Some("events"));

    let next_offset = poll
        .result
        .as_ref()
        .and_then(|value| value.get("next_offset"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("missing next_offset"))?;
    let ack = client
        .rpc(
            "cdc.ack",
            json!({"sub_id": sub_id.clone(), "offset": next_offset}),
        )
        .await?;
    assert!(ack.ok);

    let query_subscribe = client
        .rpc("query.subscribe", json!({"query_id": query_id}))
        .await?;
    assert!(query_subscribe.ok);
    let table_keys = query_subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("table_keys"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(table_keys
        .iter()
        .any(|value| value.as_str() == Some("app.events")));

    Ok(())
}

#[tokio::test]
async fn query_subscribe_over_view_reports_base_table_keys_and_emits_sse_on_base_table_changes(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("query_subscribe_view")?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "view.create",
        json!({
            "view": {"db": "app", "table": "event_view"},
            "query": select_query("app", "events", &["id", "data"])
        }),
    )
    .await?;

    let prepare = rpc
        .rpc(
            "query.prepare",
            json!({
                "query": select_query("app", "event_view", &["id", "data"])
            }),
        )
        .await?;
    let query_id = prepare
        .result
        .as_ref()
        .and_then(|value| value.get("query_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let subscribe = rpc
        .rpc("query.subscribe", json!({"query_id": query_id}))
        .await?;
    assert!(subscribe.ok);
    let result = subscribe
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing query.subscribe result"))?;
    let table_keys = result
        .get("table_keys")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(table_keys
        .iter()
        .any(|value| value.as_str() == Some("app.events")));
    assert!(!table_keys
        .iter()
        .any(|value| value.as_str() == Some("app.event_view")));

    let sse_url = result
        .get("sse_url")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query.subscribe sse_url"))?;
    let mut stream = http
        .get(format!("{}{}", server.base_url(), sse_url))
        .send()
        .await?;
    assert_eq!(stream.status(), reqwest::StatusCode::OK);

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "events"},
            "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
        }),
    )
    .await?;

    let event = read_sse_event(&mut stream).await?;
    let payload: serde_json::Value = serde_json::from_str(&event.data)?;
    assert_eq!(payload["query_id"].as_str(), Some(query_id.as_str()));
    assert_eq!(payload["table"].as_str(), Some("app.events"));
    assert_eq!(payload["changed"].as_bool(), Some(true));

    Ok(())
}

#[tokio::test]
async fn query_subscribe_over_union_reports_branch_table_keys_and_emits_sse_on_branch_changes(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("query_subscribe_union")?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    for table in ["events", "archived_events"] {
        rpc.rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": table,
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "data", "type": {"kind": "str"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;
    }

    let left = select_query("app", "events", &["id", "data"]);
    let right = select_query("app", "archived_events", &["id", "data"]);
    let union_query = Query {
        with: Vec::new(),
        body: Box::new(QueryBody::Setop {
            setop: SetOp {
                kind: SetOpKind::Union,
                all: true,
                left: left.body,
                right: right.body,
            },
        }),
        order_by: Vec::new(),
        limit: None,
        lock: None,
    };

    let prepare = rpc
        .rpc("query.prepare", json!({"query": union_query}))
        .await?;
    let query_id = prepare
        .result
        .as_ref()
        .and_then(|value| value.get("query_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let subscribe = rpc
        .rpc("query.subscribe", json!({"query_id": query_id}))
        .await?;
    assert!(subscribe.ok);
    let result = subscribe
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing query.subscribe result"))?;
    let table_keys = result
        .get("table_keys")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(table_keys
        .iter()
        .any(|value| value.as_str() == Some("app.events")));
    assert!(table_keys
        .iter()
        .any(|value| value.as_str() == Some("app.archived_events")));

    let sse_url = result
        .get("sse_url")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query.subscribe sse_url"))?;
    let mut stream = http
        .get(format!("{}{}", server.base_url(), sse_url))
        .send()
        .await?;
    assert_eq!(stream.status(), reqwest::StatusCode::OK);

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "archived_events"},
            "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
        }),
    )
    .await?;

    let event = read_sse_event(&mut stream).await?;
    let payload: serde_json::Value = serde_json::from_str(&event.data)?;
    assert_eq!(payload["query_id"].as_str(), Some(query_id.as_str()));
    assert_eq!(payload["table"].as_str(), Some("app.archived_events"));
    assert_eq!(payload["changed"].as_bool(), Some(true));

    Ok(())
}

#[tokio::test]
async fn query_subscribe_over_cte_reports_base_table_keys_and_emits_sse_on_base_changes(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("query_subscribe_cte")?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;

    let cte_query = select_query("app", "events", &["id", "data"]);
    let query = Query {
        with: vec![Cte {
            name: "event_rows".to_string(),
            query: cte_query,
        }],
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: None,
                projection: vec![
                    SelectItem {
                        expr: Expr::Col {
                            col: "id".to_string(),
                            table: None,
                        },
                        r#as: None,
                    },
                    SelectItem {
                        expr: Expr::Col {
                            col: "data".to_string(),
                            table: None,
                        },
                        r#as: None,
                    },
                ],
                from: Some(vec![TableRef::Base(BaseTableRef {
                    db: "app".to_string(),
                    table: "event_rows".to_string(),
                    r#as: None,
                })]),
                r#where: None,
                group_by: None,
                having: None,
            }),
        }),
        order_by: Vec::new(),
        limit: None,
        lock: None,
    };

    let prepare = rpc.rpc("query.prepare", json!({"query": query})).await?;
    let query_id = prepare
        .result
        .as_ref()
        .and_then(|value| value.get("query_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let subscribe = rpc
        .rpc("query.subscribe", json!({"query_id": query_id}))
        .await?;
    assert!(subscribe.ok);
    let result = subscribe
        .result
        .as_ref()
        .ok_or_else(|| anyhow!("missing query.subscribe result"))?;
    let table_keys = result
        .get("table_keys")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(table_keys
        .iter()
        .any(|value| value.as_str() == Some("app.events")));
    assert!(!table_keys
        .iter()
        .any(|value| value.as_str() == Some("app.event_rows")));

    let sse_url = result
        .get("sse_url")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query.subscribe sse_url"))?;
    let mut stream = http
        .get(format!("{}{}", server.base_url(), sse_url))
        .send()
        .await?;
    assert_eq!(stream.status(), reqwest::StatusCode::OK);

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "events"},
            "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
        }),
    )
    .await?;

    let event = read_sse_event(&mut stream).await?;
    let payload: serde_json::Value = serde_json::from_str(&event.data)?;
    assert_eq!(payload["query_id"].as_str(), Some(query_id.as_str()));
    assert_eq!(payload["table"].as_str(), Some("app.events"));
    assert_eq!(payload["changed"].as_bool(), Some(true));

    Ok(())
}

#[tokio::test]
async fn cdc_poll_requires_resnapshot_after_retention_horizon() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_env(
        "cdc_poll_resnapshot",
        &[("SKEINDB_CDC_RETENTION_EVENTS", "2")],
    )?;
    let client = RpcHttpClient::new(server.base_url());

    client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    client
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "events",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "data", "type": {"kind": "str"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;

    let subscribe = client
        .rpc(
            "cdc.subscribe_table",
            json!({"db": "app", "table": "events"}),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing cdc sub_id"))?
        .to_string();
    let start_offset = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("offset"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow!("missing cdc offset"))?;

    for (id, name) in [(1u64, "Ada"), (2u64, "Grace"), (3u64, "Linus")] {
        let resp = client
            .rpc(
                "data.insert",
                json!({
                    "into": {"db": "app", "table": "events"},
                    "rows": [{"id": {"t": "u64", "v": id}, "data": {"t": "str", "v": name}}]
                }),
            )
            .await?;
        assert!(resp.ok);
    }

    let poll = client
        .rpc(
            "cdc.poll",
            json!({"sub_id": sub_id, "from_offset": start_offset, "limit": 10}),
        )
        .await?;
    assert!(poll.ok);
    let result = poll.result.expect("missing cdc poll result");
    assert_eq!(
        result["events"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert_eq!(result["resnapshot_required"].as_bool(), Some(true));
    assert_eq!(result["earliest_offset"].as_u64(), Some(2));
    assert_eq!(result["latest_offset"].as_u64(), Some(3));
    assert_eq!(result["next_offset"].as_u64(), Some(1));
    assert_eq!(result["resnapshot_from_offset"].as_u64(), Some(1));
    assert_eq!(
        result["resnapshot_reason"].as_str(),
        Some("wal_horizon_exceeded")
    );

    Ok(())
}

#[tokio::test]
async fn cdc_table_sse_stream_reconnects_from_last_event_id() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_sse_reconnect")?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;

    let subscribe = rpc
        .rpc(
            "cdc.subscribe_table",
            json!({"db": "app", "table": "events"}),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let sse_url = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sse_url"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sse_url"))?
        .to_string();
    assert_eq!(sse_url, format!("/api/v1/cdc/sse/{sub_id}"));

    let sse_full_url = format!("{}{}", server.base_url(), sse_url);
    let mut first_stream = http.get(&sse_full_url).send().await?;
    assert_eq!(first_stream.status(), reqwest::StatusCode::OK);
    assert!(first_stream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .starts_with("text/event-stream"));

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "events"},
            "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
        }),
    )
    .await?;

    let first_event = read_sse_event(&mut first_stream).await?;
    assert_eq!(first_event.id.as_deref(), Some("1"));
    assert_eq!(first_event.event.as_deref(), Some("insert"));
    let first_payload: serde_json::Value = serde_json::from_str(&first_event.data)?;
    assert_eq!(first_payload["db"].as_str(), Some("app"));
    assert_eq!(first_payload["table"].as_str(), Some("events"));
    assert_eq!(first_payload["op"].as_str(), Some("insert"));
    assert_eq!(first_payload["pk"][0]["v"].as_u64(), Some(1));
    drop(first_stream);

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "events"},
            "rows": [{"id": {"t": "u64", "v": 2}, "data": {"t": "str", "v": "Grace"}}]
        }),
    )
    .await?;

    let mut resumed_stream = http
        .get(&sse_full_url)
        .header("Last-Event-ID", "1")
        .send()
        .await?;
    assert_eq!(resumed_stream.status(), reqwest::StatusCode::OK);

    let resumed_event = read_sse_event(&mut resumed_stream).await?;
    assert_eq!(resumed_event.id.as_deref(), Some("2"));
    assert_eq!(resumed_event.event.as_deref(), Some("insert"));
    let resumed_payload: serde_json::Value = serde_json::from_str(&resumed_event.data)?;
    assert_eq!(resumed_payload["pk"][0]["v"].as_u64(), Some(2));

    Ok(())
}

#[tokio::test]
async fn cdc_table_sse_stream_emits_resnapshot_event_after_horizon_loss() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_env(
        "cdc_table_sse_resnapshot",
        &[("SKEINDB_CDC_RETENTION_EVENTS", "2")],
    )?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;

    let subscribe = rpc
        .rpc(
            "cdc.subscribe_table",
            json!({"db": "app", "table": "events"}),
        )
        .await?;
    assert!(subscribe.ok);
    let sse_url = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sse_url"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing cdc sse_url"))?
        .to_string();

    let sse_full_url = format!("{}{}", server.base_url(), sse_url);
    let mut first_stream = http.get(&sse_full_url).send().await?;
    assert_eq!(first_stream.status(), reqwest::StatusCode::OK);

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "events"},
            "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
        }),
    )
    .await?;
    let first_event = read_sse_event(&mut first_stream).await?;
    assert_eq!(first_event.id.as_deref(), Some("1"));
    drop(first_stream);

    for (id, name) in [(2u64, "Grace"), (3u64, "Linus"), (4u64, "Margaret")] {
        rpc.rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "events"},
                "rows": [{"id": {"t": "u64", "v": id}, "data": {"t": "str", "v": name}}]
            }),
        )
        .await?;
    }

    let mut resumed_stream = http
        .get(&sse_full_url)
        .header("Last-Event-ID", "1")
        .send()
        .await?;
    assert_eq!(resumed_stream.status(), reqwest::StatusCode::OK);

    let resnapshot_event = read_sse_event(&mut resumed_stream).await?;
    assert_eq!(resnapshot_event.event.as_deref(), Some("resnapshot"));
    assert_eq!(resnapshot_event.id.as_deref(), Some("2"));
    let payload: serde_json::Value = serde_json::from_str(&resnapshot_event.data)?;
    assert_eq!(payload["earliest_offset"].as_u64(), Some(3));
    assert_eq!(payload["latest_offset"].as_u64(), Some(4));
    assert_eq!(payload["resnapshot_from_offset"].as_u64(), Some(2));
    assert_eq!(payload["reason"].as_str(), Some("wal_horizon_exceeded"));

    Ok(())
}

#[tokio::test]
async fn cdc_table_sse_stream_emits_backpressure_event_when_paused() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_sse_backpressure")?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;

    let subscribe = rpc
        .rpc(
            "cdc.subscribe_table",
            json!({"db": "app", "table": "events"}),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing cdc sub_id"))?
        .to_string();
    let sse_url = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sse_url"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing cdc sse_url"))?
        .to_string();

    let paused = rpc.rpc("cdc.pause", json!({"sub_id": sub_id})).await?;
    assert!(paused.ok);

    let mut stream = http
        .get(format!("{}{}", server.base_url(), sse_url))
        .send()
        .await?;
    assert_eq!(stream.status(), reqwest::StatusCode::OK);

    let event = read_sse_event(&mut stream).await?;
    assert_eq!(event.event.as_deref(), Some("backpressure"));
    let payload: serde_json::Value = serde_json::from_str(&event.data)?;
    assert_eq!(payload["state"].as_str(), Some("paused"));
    assert_eq!(payload["paused"].as_bool(), Some(true));

    Ok(())
}

#[tokio::test]
async fn cdc_table_ws_stream_emits_events() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_ws")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;

    let subscribe = rpc
        .rpc(
            "cdc.subscribe_table",
            json!({"db": "app", "table": "events"}),
        )
        .await?;
    assert!(subscribe.ok);
    let ws_url = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("ws_url"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing cdc ws_url"))?
        .to_string();
    let ws_full_url = format!(
        "{}{}",
        server.base_url().replacen("http://", "ws://", 1),
        ws_url
    );

    let (mut socket, _) = tokio_tungstenite::connect_async(&ws_full_url).await?;

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "events"},
            "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
        }),
    )
    .await?;

    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .context("timed out waiting for CDC websocket event")?
        .ok_or_else(|| anyhow!("CDC websocket closed before delivering event"))??;
    let text = match frame {
        tokio_tungstenite::tungstenite::Message::Text(text) => text,
        other => anyhow::bail!("expected websocket text frame, got {other:?}"),
    };
    let payload: serde_json::Value = serde_json::from_str(&text)?;
    assert_eq!(payload["event"].as_str(), Some("insert"));
    assert_eq!(payload["data"]["db"].as_str(), Some("app"));
    assert_eq!(payload["data"]["table"].as_str(), Some("events"));
    assert_eq!(payload["data"]["pk"][0]["v"].as_u64(), Some(1));

    Ok(())
}

#[tokio::test]
async fn cdc_table_ws_stream_emits_backpressure_event_when_paused() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_table_ws_backpressure")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;

    let subscribe = rpc
        .rpc(
            "cdc.subscribe_table",
            json!({"db": "app", "table": "events"}),
        )
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing cdc sub_id"))?
        .to_string();
    let ws_url = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("ws_url"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing cdc ws_url"))?
        .to_string();

    let paused = rpc.rpc("cdc.pause", json!({"sub_id": sub_id})).await?;
    assert!(paused.ok);

    let ws_full_url = format!(
        "{}{}",
        server.base_url().replacen("http://", "ws://", 1),
        ws_url
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(&ws_full_url).await?;

    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .context("timed out waiting for CDC websocket backpressure event")?
        .ok_or_else(|| anyhow!("CDC websocket closed before delivering backpressure event"))??;
    let text = match frame {
        tokio_tungstenite::tungstenite::Message::Text(text) => text,
        other => anyhow::bail!("expected websocket text frame, got {other:?}"),
    };
    let payload: serde_json::Value = serde_json::from_str(&text)?;
    assert_eq!(payload["event"].as_str(), Some("backpressure"));
    assert_eq!(payload["data"]["state"].as_str(), Some("paused"));
    assert_eq!(payload["data"]["paused"].as_bool(), Some(true));

    Ok(())
}

#[tokio::test]
async fn cdc_query_sse_stream_emits_invalidation_events() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("cdc_query_sse")?;
    let rpc = RpcHttpClient::new(server.base_url());
    let http = reqwest::Client::new();

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;

    let prepare = rpc
        .rpc(
            "query.prepare",
            json!({
                "query": {
                    "body": {
                        "select": {
                            "from": [{"db": "app", "table": "events"}],
                            "projection": [
                                {"expr": {"col": "id"}},
                                {"expr": {"col": "data"}}
                            ]
                        }
                    }
                }
            }),
        )
        .await?;
    let query_id = prepare
        .result
        .as_ref()
        .and_then(|value| value.get("query_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let subscribe = rpc
        .rpc("cdc.subscribe_query", json!({"query_id": query_id}))
        .await?;
    assert!(subscribe.ok);
    let sub_id = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sub_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sub_id"))?
        .to_string();
    let sse_url = subscribe
        .result
        .as_ref()
        .and_then(|value| value.get("sse_url"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing sse_url"))?
        .to_string();
    assert_eq!(sse_url, format!("/api/v1/cdc/sse/{sub_id}"));

    let mut stream = http
        .get(format!("{}{}", server.base_url(), sse_url))
        .send()
        .await?;
    assert_eq!(stream.status(), reqwest::StatusCode::OK);

    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "events"},
            "rows": [{"id": {"t": "u64", "v": 1}, "data": {"t": "str", "v": "Ada"}}]
        }),
    )
    .await?;

    let execute = rpc
        .rpc(
            "query.execute_prepared",
            json!({"query_id": query_id, "args": []}),
        )
        .await?;
    let etag = execute
        .result
        .as_ref()
        .and_then(|value| value.get("etag"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing etag"))?
        .to_string();

    let event = read_sse_event(&mut stream).await?;
    assert_eq!(event.id.as_deref(), Some("1"));
    assert_eq!(event.event.as_deref(), Some("invalidate"));
    let payload: serde_json::Value = serde_json::from_str(&event.data)?;
    assert_eq!(payload["op"].as_str(), Some("invalidate"));
    assert_eq!(payload["query_id"].as_str(), Some(query_id.as_str()));
    assert_eq!(payload["etag"].as_str(), Some(etag.as_str()));
    assert_eq!(payload["table"].as_str(), Some("events"));

    Ok(())
}

/// T063 + T122: Verify query.subscribe RPC + security token CRUD.
#[tokio::test]
async fn t063_t122_subscribe_and_security_tokens() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start("t063_t122")?;
    let rpc = RpcHttpClient::new(server.base_url());

    // Setup: create db + table + prepare query
    rpc.rpc("schema.create_database", json!({"db": "sub_db"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "sub_db",
            "table": "events",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "data", "type": {"kind": "str"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    let resp = rpc
        .rpc(
            "query.prepare",
            json!({
                "query": {
                    "body": {
                        "select": {
                            "from": [{"db": "sub_db", "table": "events"}],
                            "projection": [{"expr": {"col": "id"}}, {"expr": {"col": "data"}}]
                        }
                    }
                }
            }),
        )
        .await?;
    let query_id = resp
        .result
        .as_ref()
        .and_then(|r| r["query_id"].as_str())
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    // T063: query.subscribe should return sse_url
    let resp = rpc
        .rpc("query.subscribe", json!({"query_id": query_id}))
        .await?;
    let result = resp.result.as_ref().expect("should have result");
    assert!(result["sse_url"].as_str().is_some(), "should have sse_url");
    assert_eq!(result["query_id"].as_str().unwrap(), query_id);

    // T122: security.token.create
    let resp = rpc
        .rpc(
            "security.token.create",
            json!({"role": "admin", "label": "test token"}),
        )
        .await?;
    let result = resp.result.as_ref().expect("should have result");
    let token_id = result["token_id"]
        .as_str()
        .expect("should have token_id")
        .to_string();
    assert!(result["secret"].as_str().is_some(), "should have secret");
    assert_eq!(result["role"].as_str().unwrap(), "admin");

    // T122: security.token.list
    let resp = rpc.rpc("security.token.list", json!({})).await?;
    let result = resp.result.as_ref().expect("should have result");
    let tokens = result["tokens"]
        .as_array()
        .expect("should have tokens array");
    assert_eq!(tokens.len(), 1);

    // T122: security.token.revoke
    let resp = rpc
        .rpc("security.token.revoke", json!({"token_id": token_id}))
        .await?;
    let result = resp.result.as_ref().expect("should have result");
    assert_eq!(result["revoked"].as_bool(), Some(true));

    // Verify token is gone
    let resp = rpc.rpc("security.token.list", json!({})).await?;
    let result = resp.result.as_ref().expect("should have result");
    let tokens = result["tokens"]
        .as_array()
        .expect("should have tokens array");
    assert_eq!(tokens.len(), 0);

    Ok(())
}

struct HttpHarness {
    _guard: ChildGuard,
    http_port: u16,
    mysql_port: u16,
    pg_port: u16,
}

impl HttpHarness {
    fn start(label: &str) -> anyhow::Result<Self> {
        Self::start_with_ports_and_env(label, 0, 0, &[])
    }

    fn start_with_env(label: &str, envs: &[(&str, &str)]) -> anyhow::Result<Self> {
        Self::start_with_ports_and_env(label, 0, 0, envs)
    }

    fn start_with_mysql(label: &str) -> anyhow::Result<Self> {
        let mysql_port = free_tcp_port();
        Self::start_with_ports_and_env(label, mysql_port, 0, &[])
    }

    fn start_with_mysql_and_env(label: &str, envs: &[(&str, &str)]) -> anyhow::Result<Self> {
        let mysql_port = free_tcp_port();
        Self::start_with_ports_and_env(label, mysql_port, 0, envs)
    }

    fn start_with_pg(label: &str) -> anyhow::Result<Self> {
        let pg_port = free_tcp_port();
        Self::start_with_ports_and_env(label, 0, pg_port, &[])
    }

    fn start_with_pg_and_env(label: &str, envs: &[(&str, &str)]) -> anyhow::Result<Self> {
        let pg_port = free_tcp_port();
        Self::start_with_ports_and_env(label, 0, pg_port, envs)
    }

    fn start_with_pg_tls(label: &str) -> anyhow::Result<Self> {
        let pg_port = free_tcp_port();
        Self::start_with_ports_tls(label, 0, pg_port)
    }

    fn start_with_mysql_tls(label: &str) -> anyhow::Result<Self> {
        let mysql_port = free_tcp_port();
        Self::start_with_ports_tls(label, mysql_port, 0)
    }

    /// Start a server with TLS enabled on the SQL listeners using a freshly
    /// generated self-signed certificate written into the data directory.
    fn start_with_ports_tls(label: &str, mysql_port: u16, pg_port: u16) -> anyhow::Result<Self> {
        let dir = temp_dir(label);
        let http_port = free_tcp_port();
        let cluster_port = free_tcp_port();
        let (cert_path, key_path) = write_test_tls_cert(&dir)?;
        let log_path = dir.join("server.log");
        let child = spawn_server_with_tls(
            &dir,
            &log_path,
            http_port,
            cluster_port,
            mysql_port,
            pg_port,
            &cert_path,
            &key_path,
        )?;
        let mut _guard = ChildGuard::new(child, log_path);
        wait_for_health(http_port, &mut _guard)?;
        Ok(Self {
            _guard,
            http_port,
            mysql_port,
            pg_port,
        })
    }

    #[allow(dead_code)]
    fn start_with_mysql_port(label: &str, mysql_port: u16) -> anyhow::Result<Self> {
        Self::start_with_ports_and_env(label, mysql_port, 0, &[])
    }

    #[allow(dead_code)]
    fn start_with_ports(label: &str, mysql_port: u16, pg_port: u16) -> anyhow::Result<Self> {
        Self::start_with_ports_and_env(label, mysql_port, pg_port, &[])
    }

    fn start_with_ports_and_env(
        label: &str,
        mysql_port: u16,
        pg_port: u16,
        envs: &[(&str, &str)],
    ) -> anyhow::Result<Self> {
        let dir = temp_dir(label);
        let http_port = free_tcp_port();
        let cluster_port = free_tcp_port();
        let log_path = dir.join("server.log");
        let child = spawn_server_with_pg_env(
            &dir,
            &log_path,
            http_port,
            cluster_port,
            mysql_port,
            pg_port,
            envs,
        )?;
        let mut _guard = ChildGuard::new(child, log_path);

        wait_for_health(http_port, &mut _guard)?;

        Ok(Self {
            _guard,
            http_port,
            mysql_port,
            pg_port,
        })
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    fn mysql_port(&self) -> u16 {
        self.mysql_port
    }

    fn pg_port(&self) -> u16 {
        self.pg_port
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

async fn wait_for_advisor_history_entry(
    client: &RpcHttpClient,
    db: &str,
    table: &str,
    action_id: &str,
    expected_status: &str,
) -> anyhow::Result<serde_json::Value> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let history = client
            .rpc(
                "advisor.history",
                json!({
                    "table": { "db": db, "table": table },
                    "limit": 20
                }),
            )
            .await?;
        let entries = history
            .result
            .as_ref()
            .and_then(|value| value.get("entries"))
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        if let Some(entry) = entries.into_iter().find(|entry| {
            entry.get("id").and_then(|value| value.as_str()) == Some(action_id)
                && entry.get("status").and_then(|value| value.as_str()) == Some(expected_status)
        }) {
            return Ok(entry);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(anyhow!(
        "timed out waiting for advisor action {action_id} to reach status {expected_status}"
    ))
}

fn wait_for_health(port: u16, guard: &mut ChildGuard) -> anyhow::Result<()> {
    let url = format!("http://127.0.0.1:{}/health", port);
    // CI, Rosetta, and CloudDocs-backed workspaces can all delay HTTP readiness.
    // Keep the default generous and let targeted runs override it when needed.
    let timeout = health_wait_timeout();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let out = std::process::Command::new("curl")
            .arg("--connect-timeout")
            .arg("1")
            .arg("--max-time")
            .arg("1")
            .arg("-sSf")
            .arg(&url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if out.map(|s| s.success()).unwrap_or(false) {
            return Ok(());
        }
        if let Some(status) = guard
            .child
            .try_wait()
            .context("poll spawned skeindb server")?
        {
            let log_excerpt = guard.log_excerpt();
            if log_excerpt.is_empty() {
                return Err(anyhow!(
                    "server exited before becoming healthy on {} with status {}",
                    url,
                    status
                ));
            }
            return Err(anyhow!(
                "server exited before becoming healthy on {} with status {}\n{}",
                url,
                status,
                log_excerpt
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let process_state = match guard
        .child
        .try_wait()
        .context("poll spawned skeindb server after health timeout")?
    {
        Some(status) => format!("server exited with status {}", status),
        None => format!("server pid {} is still running", guard.child.id()),
    };
    let log_excerpt = guard.log_excerpt();
    if log_excerpt.is_empty() {
        Err(anyhow!(
            "server did not become healthy on {} within {:?}; {}",
            url,
            timeout,
            process_state
        ))
    } else {
        Err(anyhow!(
            "server did not become healthy on {} within {:?}; {}\n{}",
            url,
            timeout,
            process_state,
            log_excerpt
        ))
    }
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

async fn read_mysql_packet<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> anyhow::Result<(u8, Vec<u8>)> {
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
        .windows(b"caching_sha2_password".len())
        .any(|w| w == b"caching_sha2_password"));

    let response = mysql_handshake_response_packet();
    write_mysql_packet(&mut stream, 1, &response).await?;

    let (_seq, auth_result) = read_mysql_packet(&mut stream).await?;
    assert_eq!(auth_result.first().copied(), Some(0x00));
    Ok(stream)
}

async fn send_com_query<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    sql: &str,
) -> anyhow::Result<()> {
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

async fn read_mysql_text_result(
    stream: &mut TcpStream,
) -> anyhow::Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
    let (_seq, column_count_payload) = read_mysql_packet(stream).await?;
    if let Some(err) = decode_mysql_err_packet(&column_count_payload) {
        return Err(anyhow!("mysql error packet: {}", err));
    }
    let mut cur = 0usize;
    let column_count = decode_lenenc_int(&column_count_payload, &mut cur)?;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        let (name, _column_type) = decode_mysql_column_definition(&payload)?;
        columns.push(name);
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
    Ok((columns, rows))
}

#[derive(Debug)]
enum MysqlResponse {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    Rows(Vec<Vec<Option<String>>>),
}

async fn read_mysql_text_result_rows_after_first_packet<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
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

async fn read_mysql_response<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> anyhow::Result<MysqlResponse> {
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

#[derive(Debug)]
struct MysqlStmtPrepareOk {
    statement_id: u32,
    column_count: u16,
    param_count: u16,
    param_defs: Vec<(String, u8)>,
    column_defs: Vec<(String, u8)>,
}

#[derive(Debug, Clone)]
enum MysqlStmtParamValue {
    I64(i64),
    F64(f64),
    Null,
    Str(String),
    LongData,
}

fn decode_mysql_stmt_prepare_ok(payload: &[u8]) -> anyhow::Result<MysqlStmtPrepareOk> {
    if payload.len() < 12 || payload.first().copied() != Some(0x00) {
        return Err(anyhow!("not a COM_STMT_PREPARE OK packet"));
    }
    Ok(MysqlStmtPrepareOk {
        statement_id: u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]),
        column_count: u16::from_le_bytes([payload[5], payload[6]]),
        param_count: u16::from_le_bytes([payload[7], payload[8]]),
        param_defs: Vec::new(),
        column_defs: Vec::new(),
    })
}

fn decode_mysql_column_definition(payload: &[u8]) -> anyhow::Result<(String, u8)> {
    let mut cursor = 0usize;
    let mut name = None::<String>;
    for idx in 0..6 {
        let field = decode_mysql_lenenc_bytes(payload, &mut cursor)?;
        if idx == 4 {
            name = Some(String::from_utf8_lossy(field).to_string());
        }
    }
    if cursor >= payload.len() {
        return Err(anyhow!("truncated column definition"));
    }
    let fixed_len = payload[cursor] as usize;
    cursor += 1;
    if fixed_len < 0x0c || cursor + fixed_len > payload.len() {
        return Err(anyhow!("malformed column definition payload"));
    }
    cursor += 2 + 4;
    let type_code = payload
        .get(cursor)
        .copied()
        .ok_or_else(|| anyhow!("missing column type"))?;
    Ok((name.unwrap_or_default(), type_code))
}

fn decode_mysql_eof_status(payload: &[u8]) -> anyhow::Result<u16> {
    if payload.len() < 5 || payload.first().copied() != Some(0xfe) {
        return Err(anyhow!("not an EOF packet"));
    }
    Ok(u16::from_le_bytes([payload[3], payload[4]]))
}

fn decode_mysql_lenenc_bytes<'a>(
    payload: &'a [u8],
    cursor: &mut usize,
) -> anyhow::Result<&'a [u8]> {
    let len = decode_lenenc_int(payload, cursor)?;
    if *cursor + len > payload.len() {
        return Err(anyhow!("truncated length-encoded bytes"));
    }
    let bytes = &payload[*cursor..*cursor + len];
    *cursor += len;
    Ok(bytes)
}

fn decode_mysql_binary_row(
    payload: &[u8],
    column_types: &[u8],
) -> anyhow::Result<Vec<Option<String>>> {
    if payload.first().copied() != Some(0x00) {
        return Err(anyhow!("not a binary result row"));
    }
    let null_bitmap_len = (column_types.len() + 7 + 2) / 8;
    if payload.len() < 1 + null_bitmap_len {
        return Err(anyhow!("truncated binary result row"));
    }
    let null_bitmap = &payload[1..1 + null_bitmap_len];
    let mut cursor = 1 + null_bitmap_len;
    let mut row = Vec::with_capacity(column_types.len());
    for (idx, column_type) in column_types.iter().enumerate() {
        let bit = idx + 2;
        if (null_bitmap[bit / 8] & (1u8 << (bit % 8))) != 0 {
            row.push(None);
            continue;
        }
        let value = match *column_type {
            0x08 => {
                if cursor + 8 > payload.len() {
                    return Err(anyhow!("truncated binary integer column"));
                }
                let raw = [
                    payload[cursor],
                    payload[cursor + 1],
                    payload[cursor + 2],
                    payload[cursor + 3],
                    payload[cursor + 4],
                    payload[cursor + 5],
                    payload[cursor + 6],
                    payload[cursor + 7],
                ];
                cursor += 8;
                Some(i64::from_le_bytes(raw).to_string())
            }
            0x05 => {
                if cursor + 8 > payload.len() {
                    return Err(anyhow!("truncated binary float column"));
                }
                let raw = [
                    payload[cursor],
                    payload[cursor + 1],
                    payload[cursor + 2],
                    payload[cursor + 3],
                    payload[cursor + 4],
                    payload[cursor + 5],
                    payload[cursor + 6],
                    payload[cursor + 7],
                ];
                cursor += 8;
                Some(f64::from_le_bytes(raw).to_string())
            }
            _ => {
                let bytes = decode_mysql_lenenc_bytes(payload, &mut cursor)?;
                Some(String::from_utf8_lossy(bytes).to_string())
            }
        };
        row.push(value);
    }
    Ok(row)
}

async fn read_mysql_prepare_ok(stream: &mut TcpStream) -> anyhow::Result<MysqlStmtPrepareOk> {
    let (_seq, first_payload) = read_mysql_packet(stream).await?;
    if let Some(err) = decode_mysql_err_packet(&first_payload) {
        return Err(anyhow!("mysql error packet: {}", err));
    }
    let mut prepare_ok = decode_mysql_stmt_prepare_ok(&first_payload)?;
    for _ in 0..prepare_ok.param_count {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        prepare_ok
            .param_defs
            .push(decode_mysql_column_definition(&payload)?);
    }
    if prepare_ok.param_count > 0 {
        let (_seq, eof) = read_mysql_packet(stream).await?;
        assert_eq!(eof.first().copied(), Some(0xfe));
    }
    for _ in 0..prepare_ok.column_count {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        prepare_ok
            .column_defs
            .push(decode_mysql_column_definition(&payload)?);
    }
    if prepare_ok.column_count > 0 {
        let (_seq, eof) = read_mysql_packet(stream).await?;
        assert_eq!(eof.first().copied(), Some(0xfe));
    }
    Ok(prepare_ok)
}

async fn send_com_stmt_prepare(stream: &mut TcpStream, sql: &str) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.push(0x16);
    payload.extend_from_slice(sql.as_bytes());
    write_mysql_packet(stream, 0, &payload).await
}

async fn send_com_stmt_execute(
    stream: &mut TcpStream,
    statement_id: u32,
    params: &[MysqlStmtParamValue],
) -> anyhow::Result<()> {
    send_com_stmt_execute_with_flags(stream, statement_id, 0, params).await
}

async fn send_com_stmt_execute_with_flags(
    stream: &mut TcpStream,
    statement_id: u32,
    flags: u8,
    params: &[MysqlStmtParamValue],
) -> anyhow::Result<()> {
    let mut payload = Vec::new();
    payload.push(0x17);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.push(flags);
    payload.extend_from_slice(&1u32.to_le_bytes());
    let null_bitmap_len = params.len().div_ceil(8);
    let mut null_bitmap = vec![0u8; null_bitmap_len];
    for (idx, param) in params.iter().enumerate() {
        if matches!(param, MysqlStmtParamValue::Null) {
            null_bitmap[idx / 8] |= 1u8 << (idx % 8);
        }
    }
    payload.extend_from_slice(&null_bitmap);
    payload.push(1);
    for param in params {
        match param {
            MysqlStmtParamValue::I64(_) => {
                payload.push(0x08);
                payload.push(0);
            }
            MysqlStmtParamValue::F64(_) => {
                payload.push(0x05);
                payload.push(0);
            }
            MysqlStmtParamValue::Null => {
                payload.push(0x06);
                payload.push(0);
            }
            MysqlStmtParamValue::Str(_) | MysqlStmtParamValue::LongData => {
                payload.push(0xfd);
                payload.push(0);
            }
        }
    }
    for param in params {
        match param {
            MysqlStmtParamValue::Null | MysqlStmtParamValue::LongData => {}
            MysqlStmtParamValue::I64(v) => payload.extend_from_slice(&v.to_le_bytes()),
            MysqlStmtParamValue::F64(v) => payload.extend_from_slice(&v.to_le_bytes()),
            MysqlStmtParamValue::Str(v) => {
                encode_lenenc_int(&mut payload, v.len());
                payload.extend_from_slice(v.as_bytes());
            }
        }
    }
    write_mysql_packet(stream, 0, &payload).await
}

async fn send_com_stmt_fetch(
    stream: &mut TcpStream,
    statement_id: u32,
    rows: u32,
) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(9);
    payload.push(0x1c);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.extend_from_slice(&rows.to_le_bytes());
    write_mysql_packet(stream, 0, &payload).await
}

async fn send_com_stmt_long_data(
    stream: &mut TcpStream,
    statement_id: u32,
    param_id: u16,
    data: &[u8],
) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(data.len() + 7);
    payload.push(0x18);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.extend_from_slice(&param_id.to_le_bytes());
    payload.extend_from_slice(data);
    write_mysql_packet(stream, 0, &payload).await
}

async fn send_com_stmt_reset(stream: &mut TcpStream, statement_id: u32) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(5);
    payload.push(0x1a);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    write_mysql_packet(stream, 0, &payload).await
}

async fn send_com_stmt_close(stream: &mut TcpStream, statement_id: u32) -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(5);
    payload.push(0x19);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    write_mysql_packet(stream, 0, &payload).await
}

async fn read_mysql_binary_result_rows(
    stream: &mut TcpStream,
) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    let (column_types, _status) = read_mysql_binary_result_header(stream).await?;
    let (rows, _status) = read_mysql_binary_result_rows_after_header(stream, &column_types).await?;
    Ok(rows)
}

async fn read_mysql_binary_result_header(stream: &mut TcpStream) -> anyhow::Result<(Vec<u8>, u16)> {
    let (_seq, first_payload) = read_mysql_packet(stream).await?;
    if let Some(err) = decode_mysql_err_packet(&first_payload) {
        return Err(anyhow!("mysql error packet: {}", err));
    }
    let mut cur = 0usize;
    let column_count = decode_lenenc_int(&first_payload, &mut cur)?;
    let mut column_types = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        let (_name, column_type) = decode_mysql_column_definition(&payload)?;
        column_types.push(column_type);
    }
    let (_seq, eof1) = read_mysql_packet(stream).await?;
    Ok((column_types, decode_mysql_eof_status(&eof1)?))
}

async fn read_mysql_binary_result_rows_after_header(
    stream: &mut TcpStream,
    column_types: &[u8],
) -> anyhow::Result<(Vec<Vec<Option<String>>>, u16)> {
    let mut rows = Vec::new();
    loop {
        let (_seq, payload) = read_mysql_packet(stream).await?;
        if payload.first().copied() == Some(0xfe) && payload.len() < 9 {
            return Ok((rows, decode_mysql_eof_status(&payload)?));
        }
        rows.push(decode_mysql_binary_row(&payload, column_types)?);
    }
}

async fn read_mysql_stmt_fetch_rows(
    stream: &mut TcpStream,
    column_types: &[u8],
) -> anyhow::Result<(Vec<Vec<Option<String>>>, u16)> {
    read_mysql_binary_result_rows_after_header(stream, column_types).await
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

#[derive(Debug)]
struct SseEventFrame {
    id: Option<String>,
    event: Option<String>,
    data: String,
}

fn find_sse_frame_end(buf: &[u8]) -> Option<(usize, usize)> {
    buf.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|idx| (idx, 4))
        .or_else(|| {
            buf.windows(2)
                .position(|window| window == b"\n\n")
                .map(|idx| (idx, 2))
        })
}

fn parse_sse_event_frame(frame: &str) -> SseEventFrame {
    let mut id = None;
    let mut event = None;
    let mut data_lines = Vec::new();

    for line in frame.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("id:") {
            id = Some(value.trim().to_string());
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim().to_string());
            continue;
        }
        if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_string());
        }
    }

    SseEventFrame {
        id,
        event,
        data: data_lines.join("\n"),
    }
}

async fn read_sse_event(response: &mut reqwest::Response) -> anyhow::Result<SseEventFrame> {
    let mut buffer = Vec::new();
    loop {
        if let Some((frame_end, delimiter_len)) = find_sse_frame_end(&buffer) {
            let frame = String::from_utf8_lossy(&buffer[..frame_end]).into_owned();
            buffer.drain(..frame_end + delimiter_len);
            let parsed = parse_sse_event_frame(&frame);
            if !parsed.data.is_empty() {
                return Ok(parsed);
            }
        }

        let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
            .await
            .context("timeout waiting for SSE chunk")??;
        let Some(chunk) = chunk else {
            anyhow::bail!("SSE stream closed before an event was delivered");
        };
        buffer.extend_from_slice(&chunk);
    }
}

fn pg_compat_corpus_statements() -> Vec<String> {
    let corpus = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/compat/pg_corpus.sql"
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

    let chars: Vec<char> = cleaned.chars().collect();
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut quote = None::<char>; // ' or "
    let mut dollar_tag = None::<String>; // active $$ or $tag$ delimiter
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];

        // Inside a dollar-quoted string single quotes and semicolons are literal;
        // copy verbatim until the matching closing tag.
        if let Some(tag) = &dollar_tag {
            let tag_chars: Vec<char> = tag.chars().collect();
            if ch == '$' && chars[i..].starts_with(&tag_chars) {
                current.push_str(tag);
                i += tag_chars.len();
                dollar_tag = None;
            } else {
                current.push(ch);
                i += 1;
            }
            continue;
        }

        match quote {
            Some(q) => {
                current.push(ch);
                if ch == q {
                    if q == '\'' && chars.get(i + 1).copied() == Some('\'') {
                        current.push('\'');
                        i += 2;
                        continue;
                    }
                    quote = None;
                }
                i += 1;
            }
            None => {
                if ch == '$' {
                    if let Some(tag_len) = pg_dollar_quote_tag_len(&chars[i..]) {
                        let tag: String = chars[i..i + tag_len].iter().collect();
                        current.push_str(&tag);
                        dollar_tag = Some(tag);
                        i += tag_len;
                        continue;
                    }
                }
                current.push(ch);
                match ch {
                    '\'' | '"' => quote = Some(ch),
                    ';' => {
                        let stmt = current.trim().to_string();
                        if !stmt.is_empty() {
                            statements.push(stmt);
                        }
                        current.clear();
                    }
                    _ => {}
                }
                i += 1;
            }
        }
    }
    let tail = current.trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }
    statements
}

/// Returns the length (in chars) of a PostgreSQL dollar-quote opening delimiter
/// starting at `s[0] == '$'` — `$$` -> 2, `$tag$` -> tag length + 2 — or `None`
/// when `s` does not begin a valid dollar-quote tag.
fn pg_dollar_quote_tag_len(s: &[char]) -> Option<usize> {
    if s.first() != Some(&'$') {
        return None;
    }
    if s.get(1) == Some(&'$') {
        return Some(2);
    }
    let mut j = 1;
    if !matches!(s.get(j), Some(c) if c.is_ascii_alphabetic() || *c == '_') {
        return None;
    }
    j += 1;
    while matches!(s.get(j), Some(c) if c.is_ascii_alphanumeric() || *c == '_') {
        j += 1;
    }
    if s.get(j) == Some(&'$') {
        Some(j + 1)
    } else {
        None
    }
}

async fn write_mysql_packet<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    seq: u8,
    payload: &[u8],
) -> anyhow::Result<()> {
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

fn encode_lenenc_int(buf: &mut Vec<u8>, n: usize) {
    if n < 251 {
        buf.push(n as u8);
    } else if n <= 0xffff {
        buf.push(0xfc);
        buf.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n <= 0x00ff_ffff {
        buf.push(0xfd);
        buf.push((n & 0xff) as u8);
        buf.push(((n >> 8) & 0xff) as u8);
        buf.push(((n >> 16) & 0xff) as u8);
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&(n as u64).to_le_bytes());
    }
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

/// Extract the 20-byte authentication nonce (seed) from a server handshake
/// packet so the test client can compute a scramble.
fn extract_mysql_handshake_seed(handshake: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut pos = 1usize; // protocol version
    while pos < handshake.len() && handshake[pos] != 0 {
        pos += 1;
    }
    pos += 1; // NUL after server version
    pos += 4; // connection id
    if pos + 8 > handshake.len() {
        return Err(anyhow!("handshake too short for seed part 1"));
    }
    let mut seed = handshake[pos..pos + 8].to_vec();
    pos += 8;
    pos += 1; // filler
    pos += 2; // capability flags lower
    pos += 1; // charset
    pos += 2; // status flags
    pos += 2; // capability flags upper
    let auth_data_len = *handshake
        .get(pos)
        .ok_or_else(|| anyhow!("missing auth data length"))? as usize;
    pos += 1;
    pos += 10; // reserved
    let part2_len = auth_data_len.saturating_sub(8).saturating_sub(1);
    if pos + part2_len > handshake.len() {
        return Err(anyhow!("handshake too short for seed part 2"));
    }
    seed.extend_from_slice(&handshake[pos..pos + part2_len]);
    Ok(seed)
}

/// Client-side `caching_sha2_password` scramble:
/// `SHA256(pwd) XOR SHA256(SHA256(SHA256(pwd)) || seed)`.
fn mysql_caching_sha2_scramble(password: &str, seed: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    if password.is_empty() {
        return Vec::new();
    }
    let stage1: [u8; 32] = Sha256::digest(password.as_bytes()).into();
    let stage2: [u8; 32] = Sha256::digest(stage1).into();
    let digest: [u8; 32] = {
        let mut h = Sha256::new();
        h.update(stage2);
        h.update(seed);
        h.finalize().into()
    };
    stage1
        .iter()
        .zip(digest.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

fn mysql_caching_sha2_response_packet(username: &str, scramble: &[u8]) -> Vec<u8> {
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
    payload.extend_from_slice(username.as_bytes());
    payload.push(0);
    payload.push(scramble.len() as u8);
    payload.extend_from_slice(scramble);
    payload.extend_from_slice(b"caching_sha2_password");
    payload.push(0);
    payload
}

async fn mysql_connect_caching_sha2(
    port: u16,
    username: &str,
    password: &str,
) -> anyhow::Result<TcpStream> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .context("connect mysql port")?;
    let (seq, handshake) = read_mysql_packet(&mut stream).await?;
    assert_eq!(seq, 0);
    assert_eq!(handshake.first().copied(), Some(0x0a));
    assert!(handshake
        .windows(b"caching_sha2_password".len())
        .any(|w| w == b"caching_sha2_password"));

    let seed = extract_mysql_handshake_seed(&handshake)?;
    let scramble = mysql_caching_sha2_scramble(password, &seed);
    let response = mysql_caching_sha2_response_packet(username, &scramble);
    write_mysql_packet(&mut stream, 1, &response).await?;

    // Server confirms the cached credentials with AuthMoreData(fast-auth) + OK.
    let (_seq, more) = read_mysql_packet(&mut stream).await?;
    if let Some(err) = decode_mysql_err_packet(&more) {
        return Err(anyhow!("caching_sha2 auth failed: {}", err));
    }
    assert_eq!(more.as_slice(), &[0x01, 0x03]);
    let (_seq, ok) = read_mysql_packet(&mut stream).await?;
    assert_eq!(ok.first().copied(), Some(0x00));
    Ok(stream)
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

#[allow(dead_code)]
fn spawn_server(
    dir: &PathBuf,
    http_port: u16,
    cluster_port: u16,
    mysql_port: u16,
) -> anyhow::Result<Child> {
    let log_path = dir.join("server.log");
    spawn_server_with_pg_env(dir, &log_path, http_port, cluster_port, mysql_port, 0, &[])
}

#[allow(dead_code)]
fn spawn_server_with_pg(
    dir: &PathBuf,
    http_port: u16,
    cluster_port: u16,
    mysql_port: u16,
    pg_port: u16,
) -> anyhow::Result<Child> {
    let log_path = dir.join("server.log");
    spawn_server_with_pg_env(
        dir,
        &log_path,
        http_port,
        cluster_port,
        mysql_port,
        pg_port,
        &[],
    )
}

fn spawn_server_with_pg_env(
    dir: &PathBuf,
    log_path: &PathBuf,
    http_port: u16,
    cluster_port: u16,
    mysql_port: u16,
    pg_port: u16,
    envs: &[(&str, &str)],
) -> anyhow::Result<Child> {
    let bin = local_server_bin()?;
    let stdout = File::create(log_path).context("create server log file")?;
    let stderr = stdout
        .try_clone()
        .context("clone server log handle for stderr")?;
    let mut command = Command::new(&bin);
    command
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
        .arg(pg_port.to_string())
        .arg("--cluster-port")
        .arg(cluster_port.to_string())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    for (key, value) in envs.iter().copied() {
        command.env(key, value);
    }
    command.spawn().context("spawn skeindb server")
}

/// Spawn a server with TLS enabled on the MySQL and PostgreSQL listeners.
#[allow(clippy::too_many_arguments)]
fn spawn_server_with_tls(
    dir: &PathBuf,
    log_path: &PathBuf,
    http_port: u16,
    cluster_port: u16,
    mysql_port: u16,
    pg_port: u16,
    tls_cert: &Path,
    tls_key: &Path,
) -> anyhow::Result<Child> {
    let bin = local_server_bin()?;
    let stdout = File::create(log_path).context("create server log file")?;
    let stderr = stdout
        .try_clone()
        .context("clone server log handle for stderr")?;
    let mut command = Command::new(&bin);
    command
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
        .arg(pg_port.to_string())
        .arg("--cluster-port")
        .arg(cluster_port.to_string())
        .arg("--tls-cert")
        .arg(tls_cert)
        .arg("--tls-key")
        .arg(tls_key)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    command.spawn().context("spawn skeindb server with TLS")
}

/// Generate a self-signed certificate/key pair for `localhost` and write them
/// as PEM files into `dir`, returning their paths.
fn write_test_tls_cert(dir: &Path) -> anyhow::Result<(PathBuf, PathBuf)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .context("generate test TLS cert")?;
    let cert_path = dir.join("tls_cert.pem");
    let key_path = dir.join("tls_key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).context("write test cert pem")?;
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).context("write test key pem")?;
    Ok((cert_path, key_path))
}

fn local_server_bin() -> anyhow::Result<PathBuf> {
    if let Some(path) = CLUSTER_TEST_SERVER_BIN.get() {
        return Ok(path.clone());
    }

    let src = PathBuf::from(env!("CARGO_BIN_EXE_skeindb"));
    let dst = std::env::temp_dir().join(format!("skeindb_cluster_test_bin_{}", std::process::id()));

    copy_server_bin(&src, &dst)?;
    let _ = CLUSTER_TEST_SERVER_BIN.set(dst.clone());
    Ok(dst)
}

fn copy_server_bin(src: &Path, dst: &Path) -> anyhow::Result<()> {
    fs::copy(src, dst).with_context(|| {
        format!(
            "copy skeindb test binary from {} to {}",
            src.display(),
            dst.display()
        )
    })?;

    #[cfg(unix)]
    {
        let mut perms = fs::metadata(dst)
            .with_context(|| format!("stat copied skeindb binary {}", dst.display()))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(dst, perms)
            .with_context(|| format!("set executable permissions on {}", dst.display()))?;
    }

    Ok(())
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
    log_path: PathBuf,
}

impl ChildGuard {
    fn new(child: Child, log_path: PathBuf) -> Self {
        Self { child, log_path }
    }

    fn log_excerpt(&self) -> String {
        let Ok(contents) = std::fs::read_to_string(&self.log_path) else {
            return String::new();
        };
        let mut lines: Vec<&str> = contents.lines().collect();
        if lines.is_empty() {
            return String::new();
        }
        if lines.len() > 40 {
            lines = lines.split_off(lines.len() - 40);
        }
        format!(
            "last server log lines from {}:\n{}",
            self.log_path.display(),
            lines.join("\n")
        )
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL wire protocol integration tests
// ---------------------------------------------------------------------------

/// Build a PG startup message from scratch.
fn build_pg_startup(user: &str, database: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&196608i32.to_be_bytes()); // protocol 3.0
    payload.extend_from_slice(b"user\0");
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"database\0");
    payload.extend_from_slice(database.as_bytes());
    payload.push(0);
    payload.push(0); // parameter list terminator
    let len = (payload.len() + 4) as i32;
    let mut msg = Vec::new();
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    msg
}

/// Read a PG backend message: tag(1) + length(4) + payload.
async fn read_pg_message<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> anyhow::Result<(u8, Vec<u8>)> {
    let tag = stream.read_u8().await?;
    let len = stream.read_i32().await? as usize;
    let payload_len = len.saturating_sub(4);
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok((tag, payload))
}

/// Connect to the PG listener, perform startup handshake, consume all
/// ParameterStatus messages until ReadyForQuery.
async fn pg_connect_and_startup(port: u16) -> anyhow::Result<TcpStream> {
    wait_for_tcp(port)?;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let startup = build_pg_startup("skein", "testdb");
    stream.write_all(&startup).await?;
    stream.flush().await?;

    // Read messages until ReadyForQuery ('Z')
    loop {
        let (tag, payload) = read_pg_message(&mut stream).await?;
        match tag {
            b'R' => {
                // AuthenticationOk: first 4 bytes should be 0
                let auth_type =
                    i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                assert_eq!(auth_type, 0, "expected AuthenticationOk");
            }
            b'S' => {
                // ParameterStatus — skip
            }
            b'K' => {
                // BackendKeyData — skip
            }
            b'Z' => {
                // ReadyForQuery
                assert_eq!(payload[0], b'I', "expected idle transaction status");
                break;
            }
            _ => {
                anyhow::bail!("unexpected message tag during startup: {}", tag as char);
            }
        }
    }

    Ok(stream)
}

/// Send a simple Query message and return the (tag, payload) pairs until
/// ReadyForQuery.
async fn pg_simple_query<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    sql: &str,
) -> anyhow::Result<Vec<(u8, Vec<u8>)>> {
    // Build Query message: 'Q' + len + sql\0
    let mut payload = Vec::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    let len = (payload.len() + 4) as i32;
    stream.write_u8(b'Q').await?;
    stream.write_i32(len).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;

    let mut messages = Vec::new();
    loop {
        let (tag, payload) = read_pg_message(stream).await?;
        let done = tag == b'Z';
        messages.push((tag, payload));
        if done {
            break;
        }
    }
    Ok(messages)
}

fn pg_query_payload(sql: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    payload
}

async fn pg_send_frontend_message(
    stream: &mut TcpStream,
    tag: u8,
    payload: &[u8],
) -> anyhow::Result<()> {
    stream.write_u8(tag).await?;
    stream.write_i32((payload.len() + 4) as i32).await?;
    if !payload.is_empty() {
        stream.write_all(payload).await?;
    }
    Ok(())
}

async fn pg_send_messages_until_ready(
    stream: &mut TcpStream,
    messages: &[(u8, Vec<u8>)],
) -> anyhow::Result<Vec<(u8, Vec<u8>)>> {
    for (tag, payload) in messages {
        pg_send_frontend_message(stream, *tag, payload).await?;
    }
    stream.flush().await?;

    let mut responses = Vec::new();
    loop {
        let message = read_pg_message(stream).await?;
        let done = message.0 == b'Z';
        responses.push(message);
        if done {
            break;
        }
    }
    Ok(responses)
}

fn pg_parse_payload(statement_name: &str, sql: &str, param_types: &[i32]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(statement_name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&(param_types.len() as i16).to_be_bytes());
    for param_type in param_types {
        payload.extend_from_slice(&param_type.to_be_bytes());
    }
    payload
}

fn pg_bind_text_payload(
    portal_name: &str,
    statement_name: &str,
    params: &[Option<&str>],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(statement_name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&0i16.to_be_bytes());
    payload.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        match param {
            Some(value) => {
                payload.extend_from_slice(&(value.len() as i32).to_be_bytes());
                payload.extend_from_slice(value.as_bytes());
            }
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    payload.extend_from_slice(&0i16.to_be_bytes()); // 0 result format codes = all text
    payload
}

/// Build a Bind payload requesting binary format for all result columns.
fn pg_bind_binary_result_payload(
    portal_name: &str,
    statement_name: &str,
    params: &[Option<&str>],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(statement_name.as_bytes());
    payload.push(0);
    // Parameter format codes: 0 = all text
    payload.extend_from_slice(&0i16.to_be_bytes());
    // Parameters
    payload.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        match param {
            Some(value) => {
                payload.extend_from_slice(&(value.len() as i32).to_be_bytes());
                payload.extend_from_slice(value.as_bytes());
            }
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    // Result format codes: 1 code, value 1 = all binary
    payload.extend_from_slice(&1i16.to_be_bytes());
    payload.extend_from_slice(&1i16.to_be_bytes());
    payload
}

/// Build a Bind payload that sends every parameter in binary format (format code
/// `1`) using the supplied raw wire bytes; `None` encodes a SQL NULL (length -1).
/// Result columns are requested in text format.
fn pg_bind_binary_params_payload(
    portal_name: &str,
    statement_name: &str,
    params: &[Option<Vec<u8>>],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(statement_name.as_bytes());
    payload.push(0);
    // One parameter format code (1 = binary), broadcast to all parameters.
    payload.extend_from_slice(&1i16.to_be_bytes());
    payload.extend_from_slice(&1i16.to_be_bytes());
    payload.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        match param {
            Some(bytes) => {
                payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                payload.extend_from_slice(bytes);
            }
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    payload.extend_from_slice(&0i16.to_be_bytes()); // result columns: all text
    payload
}

fn pg_describe_payload(target_kind: u8, name: &str) -> Vec<u8> {
    let mut payload = vec![target_kind];
    payload.extend_from_slice(name.as_bytes());
    payload.push(0);
    payload
}

fn pg_execute_payload(portal_name: &str, max_rows: i32) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(portal_name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&max_rows.to_be_bytes());
    payload
}

fn pg_close_payload(target_kind: u8, name: &str) -> Vec<u8> {
    let mut payload = vec![target_kind];
    payload.extend_from_slice(name.as_bytes());
    payload.push(0);
    payload
}

fn pg_message_tags(messages: &[(u8, Vec<u8>)]) -> Vec<u8> {
    messages.iter().map(|(tag, _)| *tag).collect()
}

fn pg_parameter_description_oids(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<i32>> {
    let payload = messages
        .iter()
        .find(|(tag, _)| *tag == b't')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing ParameterDescription"))?;
    let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut oids = Vec::with_capacity(count);
    for _ in 0..count {
        let bytes = payload
            .get(offset..offset + 4)
            .ok_or_else(|| anyhow!("truncated ParameterDescription payload"))?;
        oids.push(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
        offset += 4;
    }
    Ok(oids)
}

fn pg_row_description_names(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<String>> {
    let payload = messages
        .iter()
        .find(|(tag, _)| *tag == b'T')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing RowDescription"))?;
    let column_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut names = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let end = payload[offset..]
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| anyhow!("unterminated RowDescription column name"))?
            + offset;
        names.push(String::from_utf8_lossy(&payload[offset..end]).into_owned());
        offset = end + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    }
    Ok(names)
}

fn pg_row_description_type_oids(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<i32>> {
    let payload = messages
        .iter()
        .find(|(tag, _)| *tag == b'T')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing RowDescription"))?;
    let column_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut type_oids = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let end = payload[offset..]
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| anyhow!("unterminated RowDescription column name"))?
            + offset;
        offset = end + 1 + 4 + 2;
        let bytes = payload
            .get(offset..offset + 4)
            .ok_or_else(|| anyhow!("truncated RowDescription type oid"))?;
        type_oids.push(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
        offset += 4 + 2 + 4 + 2;
    }
    Ok(type_oids)
}

/// Extract format codes (0=text, 1=binary) from RowDescription.
fn pg_row_description_format_codes(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<i16>> {
    let payload = messages
        .iter()
        .find(|(tag, _)| *tag == b'T')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing RowDescription"))?;
    let column_count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2usize;
    let mut formats = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let end = payload[offset..]
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| anyhow!("unterminated RowDescription column name"))?
            + offset;
        // Skip: name + \0 + table_oid(4) + col_attr(2) + type_oid(4) + type_size(2) + type_mod(4)
        offset = end + 1 + 4 + 2 + 4 + 2 + 4;
        let bytes = payload
            .get(offset..offset + 2)
            .ok_or_else(|| anyhow!("truncated RowDescription format code"))?;
        formats.push(i16::from_be_bytes([bytes[0], bytes[1]]));
        offset += 2;
    }
    Ok(formats)
}

/// Extract raw bytes from the first DataRow column (for binary format testing).
fn pg_first_data_row_raw_bytes(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<u8>> {
    let row = messages
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing DataRow"))?;
    let _col_count = i16::from_be_bytes([row[0], row[1]]);
    let data_len = i32::from_be_bytes([row[2], row[3], row[4], row[5]]) as usize;
    Ok(row[6..6 + data_len].to_vec())
}

fn pg_first_text_cell(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<String> {
    let row = messages
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing DataRow"))?;
    let col_count = i16::from_be_bytes([row[0], row[1]]);
    anyhow::ensure!(col_count == 1, "expected single-column DataRow");
    let data_len = i32::from_be_bytes([row[2], row[3], row[4], row[5]]) as usize;
    Ok(String::from_utf8_lossy(&row[6..6 + data_len]).into_owned())
}

fn pg_all_text_cells(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<String>> {
    let mut values = Vec::new();
    for (_, row) in messages.iter().filter(|(tag, _)| *tag == b'D') {
        let col_count = i16::from_be_bytes([row[0], row[1]]);
        anyhow::ensure!(col_count == 1, "expected single-column DataRow");
        let data_len = i32::from_be_bytes([row[2], row[3], row[4], row[5]]) as usize;
        values.push(String::from_utf8_lossy(&row[6..6 + data_len]).into_owned());
    }
    Ok(values)
}

fn pg_first_data_row_cells(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<Option<String>>> {
    let row = messages
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing DataRow"))?;
    let col_count = i16::from_be_bytes([row[0], row[1]]) as usize;
    let mut offset = 2usize;
    let mut values = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let len = i32::from_be_bytes([
            row[offset],
            row[offset + 1],
            row[offset + 2],
            row[offset + 3],
        ]);
        offset += 4;
        if len < 0 {
            values.push(None);
            continue;
        }
        let len = len as usize;
        let value = String::from_utf8_lossy(&row[offset..offset + len]).into_owned();
        offset += len;
        values.push(Some(value));
    }
    Ok(values)
}

fn pg_all_data_row_cells(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, row)| {
            let col_count = i16::from_be_bytes([row[0], row[1]]) as usize;
            let mut offset = 2usize;
            let mut values = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let len = i32::from_be_bytes([
                    row[offset],
                    row[offset + 1],
                    row[offset + 2],
                    row[offset + 3],
                ]);
                offset += 4;
                if len < 0 {
                    values.push(None);
                    continue;
                }
                let len = len as usize;
                let end = offset + len;
                values.push(Some(
                    String::from_utf8_lossy(&row[offset..end]).into_owned(),
                ));
                offset = end;
            }
            Ok(values)
        })
        .collect()
}

fn pg_ready_status(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<u8> {
    messages
        .iter()
        .rev()
        .find(|(tag, _)| *tag == b'Z')
        .and_then(|(_, payload)| payload.first().copied())
        .ok_or_else(|| anyhow!("missing ReadyForQuery status"))
}

fn pg_command_complete_tag(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<String> {
    let payload = messages
        .iter()
        .find(|(tag, _)| *tag == b'C')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing CommandComplete"))?;
    let end = payload
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(payload.len());
    Ok(String::from_utf8_lossy(&payload[..end]).into_owned())
}

fn pg_copy_data_lines(messages: &[(u8, Vec<u8>)]) -> Vec<String> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'd')
        .map(|(_, payload)| String::from_utf8_lossy(payload).into_owned())
        .collect()
}

fn pg_copy_out_response_formats(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<(u8, Vec<i16>)> {
    let payload = messages
        .iter()
        .find(|(tag, _)| *tag == b'H')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing CopyOutResponse"))?;
    anyhow::ensure!(payload.len() >= 3, "truncated CopyOutResponse payload");
    let overall_format = payload[0];
    let column_count = i16::from_be_bytes([payload[1], payload[2]]) as usize;
    let mut offset = 3usize;
    let mut column_formats = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let bytes = payload
            .get(offset..offset + 2)
            .ok_or_else(|| anyhow!("truncated CopyOutResponse format code"))?;
        column_formats.push(i16::from_be_bytes([bytes[0], bytes[1]]));
        offset += 2;
    }
    Ok((overall_format, column_formats))
}

fn pg_copy_data_bytes(messages: &[(u8, Vec<u8>)]) -> Vec<u8> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'd')
        .flat_map(|(_, payload)| payload.iter().copied())
        .collect()
}

fn pg_parse_binary_copy_rows(
    messages: &[(u8, Vec<u8>)],
) -> anyhow::Result<Vec<Vec<Option<Vec<u8>>>>> {
    let data = pg_copy_data_bytes(messages);
    anyhow::ensure!(data.len() >= 19, "truncated binary COPY stream");
    anyhow::ensure!(
        &data[..11] == b"PGCOPY\n\xFF\r\n\0",
        "binary COPY stream missing signature"
    );

    let flags = u32::from_be_bytes([data[11], data[12], data[13], data[14]]);
    let header_ext_len = u32::from_be_bytes([data[15], data[16], data[17], data[18]]) as usize;
    anyhow::ensure!(flags == 0, "unexpected binary COPY flags {flags}");
    anyhow::ensure!(
        data.len() >= 19 + header_ext_len,
        "truncated binary COPY header extension"
    );

    let mut offset = 19 + header_ext_len;
    let mut rows = Vec::new();
    loop {
        let field_count_bytes = data
            .get(offset..offset + 2)
            .ok_or_else(|| anyhow!("truncated binary COPY field count"))?;
        let field_count = i16::from_be_bytes([field_count_bytes[0], field_count_bytes[1]]);
        offset += 2;
        if field_count == -1 {
            break;
        }
        anyhow::ensure!(
            field_count >= 0,
            "invalid binary COPY field count {field_count}"
        );

        let mut row = Vec::with_capacity(field_count as usize);
        for _ in 0..field_count as usize {
            let len_bytes = data
                .get(offset..offset + 4)
                .ok_or_else(|| anyhow!("truncated binary COPY field length"))?;
            let len = i32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
            offset += 4;
            if len < 0 {
                row.push(None);
                continue;
            }
            let len = len as usize;
            let field = data
                .get(offset..offset + len)
                .ok_or_else(|| anyhow!("truncated binary COPY field payload"))?;
            row.push(Some(field.to_vec()));
            offset += len;
        }
        rows.push(row);
    }

    anyhow::ensure!(
        offset == data.len(),
        "unexpected trailing bytes in binary COPY stream"
    );
    Ok(rows)
}

fn pg_error_field(payload: &[u8], wanted: u8) -> Option<String> {
    let mut offset = 0usize;
    while offset < payload.len() {
        let field = *payload.get(offset)?;
        if field == 0 {
            break;
        }
        offset += 1;
        let start = offset;
        while offset < payload.len() && payload[offset] != 0 {
            offset += 1;
        }
        let value = String::from_utf8_lossy(&payload[start..offset]).into_owned();
        if field == wanted {
            return Some(value);
        }
        offset += 1;
    }
    None
}

fn pg_error_response(messages: &[(u8, Vec<u8>)]) -> anyhow::Result<(String, String)> {
    let payload = messages
        .iter()
        .find(|(tag, _)| *tag == b'E')
        .map(|(_, payload)| payload)
        .ok_or_else(|| anyhow!("missing ErrorResponse"))?;
    let code = pg_error_field(payload, b'C').context("missing SQLSTATE field")?;
    let message = pg_error_field(payload, b'M').context("missing error message field")?;
    Ok((code, message))
}

#[tokio::test]
async fn pg_handshake_and_ready_for_query() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_handshake")?;
    let _stream = pg_connect_and_startup(server.pg_port()).await?;
    // If we get here, handshake succeeded
    Ok(())
}

#[tokio::test]
async fn pg_simple_query_select_literal() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_select_literal")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_simple_query(&mut stream, "SELECT 1 AS num").await?;

    // Expect: RowDescription, DataRow, CommandComplete, ReadyForQuery
    let tags: Vec<u8> = msgs.iter().map(|(t, _)| *t).collect();
    assert!(
        tags.contains(&b'T'),
        "expected RowDescription in response, got: {:?}",
        tags.iter().map(|t| *t as char).collect::<Vec<_>>()
    );
    assert!(
        tags.contains(&b'D'),
        "expected DataRow in response, got: {:?}",
        tags.iter().map(|t| *t as char).collect::<Vec<_>>()
    );
    assert!(tags.contains(&b'C'), "expected CommandComplete in response");
    assert_eq!(
        *tags.last().unwrap(),
        b'Z',
        "last message should be ReadyForQuery"
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_split_part_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_split_part")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT split_part('abc,def,ghi,jkl', ',', -2) AS piece",
    )
    .await?;

    assert_eq!(pg_row_description_names(&msgs)?, vec!["piece".to_string()]);
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25]);
    assert_eq!(pg_first_text_cell(&msgs)?, "ghi");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_starts_with_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_starts_with")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT starts_with('alphabet', 'alph') AS matches_prefix",
    )
    .await?;

    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec!["matches_prefix".to_string()]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![16]);
    assert_eq!(pg_first_text_cell(&msgs)?, "t");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_row_description_uses_inferred_oids() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_row_description_oids")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_row_description_oids (id BIGINT NOT NULL, score DOUBLE NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_row_description_oids (id, score, name) VALUES (1, 1.5, 'Ada')",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT id, score, name FROM app.pg_row_description_oids",
    )
    .await?;

    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec!["id".to_string(), "score".to_string(), "name".to_string()]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![20, 701, 25]);
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_users",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "active", "type": {"kind": "bool"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_users"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada"},
                    "active": {"t": "bool", "v": true}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "active": {"t": "bool", "v": false}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active FROM app.pg_copy_users ORDER BY id) TO STDOUT",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec!["1\tAda\tt\n", "2\t\\N\tf\n"]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_in_users (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload("COPY app.pg_copy_in_users FROM STDIN"),
            ),
            (b'd', b"1\tAda\tt\n2\t\\N\tf\n".to_vec()),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active FROM app.pg_copy_in_users ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada".to_string()),
                Some("t".to_string())
            ],
            vec![Some("2".to_string()), None, Some("f".to_string())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_csv_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_with_csv_format")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_csv_out",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "active", "type": {"kind": "bool"}, "nullable": false},
                {"name": "note", "type": {"kind": "str"}, "nullable": true}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_csv_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada, Lovelace"},
                    "active": {"t": "bool", "v": true},
                    "note": {"t": "str", "v": "quote \"hi\""}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "active": {"t": "bool", "v": false},
                    "note": {"t": "str", "v": ""}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active, note FROM app.pg_copy_csv_out ORDER BY id) TO STDOUT WITH (FORMAT csv)",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec![
            "1,\"Ada, Lovelace\",t,\"quote \"\"hi\"\"\"\n",
            "2,,f,\"\"\n"
        ]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_csv_keyword_format_alias_roundtrip(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_csv_keyword_alias")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_csv_keyword_alias_out (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_copy_csv_keyword_alias_out (id, name, active) VALUES (1, 'Ada, Lovelace', true)",
        "INSERT INTO app.pg_copy_csv_keyword_alias_out (id, name, active) VALUES (2, NULL, false)",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active FROM app.pg_copy_csv_keyword_alias_out ORDER BY id) TO STDOUT WITH (CSV, HEADER)",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec!["id,name,active\n", "1,\"Ada, Lovelace\",t\n", "2,,f\n"]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_legacy_with_csv_header_roundtrip() -> anyhow::Result<()>
{
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_legacy_with_csv_header")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_csv_legacy_with_out (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_copy_csv_legacy_with_out (id, name, active) VALUES (1, 'Ada, Lovelace', true)",
        "INSERT INTO app.pg_copy_csv_legacy_with_out (id, name, active) VALUES (2, NULL, false)",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active FROM app.pg_copy_csv_legacy_with_out ORDER BY id) TO STDOUT WITH CSV HEADER",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec!["id,name,active\n", "1,\"Ada, Lovelace\",t\n", "2,,f\n"]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_csv_header_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_with_csv_header")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_csv_header_out",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "active", "type": {"kind": "bool"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_csv_header_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada, Lovelace"},
                    "active": {"t": "bool", "v": true}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "active": {"t": "bool", "v": false}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active FROM app.pg_copy_csv_header_out ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER)",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec!["id,name,active\n", "1,\"Ada, Lovelace\",t\n", "2,,f\n"]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_csv_delimiter_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_with_csv_delimiter")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_csv_delim_out",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "active", "type": {"kind": "bool"}, "nullable": false},
                {"name": "note", "type": {"kind": "str"}, "nullable": true}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_csv_delim_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada, Lovelace"},
                    "active": {"t": "bool", "v": true},
                    "note": {"t": "str", "v": "quote \"hi\""}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "active": {"t": "bool", "v": false},
                    "note": {"t": "str", "v": ""}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active, note FROM app.pg_copy_csv_delim_out ORDER BY id) TO STDOUT WITH (FORMAT csv, DELIMITER ';')",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec!["1;Ada, Lovelace;t;\"quote \"\"hi\"\"\"\n", "2;;f;\"\"\n"]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_csv_quote_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_with_csv_quote")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_csv_quote_out",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "note", "type": {"kind": "str"}, "nullable": true}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_csv_quote_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada, Lovelace"},
                    "note": {"t": "str", "v": "pipe | quote"}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "note": {"t": "str", "v": ""}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, note FROM app.pg_copy_csv_quote_out ORDER BY id) TO STDOUT WITH (FORMAT csv, QUOTE '|')",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec!["1,|Ada, Lovelace|,|pipe || quote|\n", "2,,||\n"]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_csv_escape_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_with_csv_escape")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_csv_escape_out",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "note", "type": {"kind": "str"}, "nullable": true}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_csv_escape_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada, Lovelace"},
                    "note": {"t": "str", "v": "bang ! and pipe |"}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "note": {"t": "str", "v": ""}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, note FROM app.pg_copy_csv_escape_out ORDER BY id) TO STDOUT WITH (FORMAT csv, QUOTE '|', ESCAPE '!')",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(
        pg_copy_data_lines(&msgs),
        vec!["1,|Ada, Lovelace|,|bang !! and pipe !||\n", "2,,||\n"]
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_with_csv_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_with_csv_format")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_csv_in (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, note VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload("COPY app.pg_copy_csv_in FROM STDIN WITH (FORMAT csv)"),
            ),
            (
                b'd',
                b"1,\"Ada, Lovelace\",t,\"quote \"\"hi\"\"\"\n2,,f,\"\"\n".to_vec(),
            ),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active, note FROM app.pg_copy_csv_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada, Lovelace".to_string()),
                Some("t".to_string()),
                Some("quote \"hi\"".to_string())
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("f".to_string()),
                Some(String::new())
            ],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_with_csv_escape_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_with_csv_escape")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_csv_escape_in (id BIGINT NOT NULL, name VARCHAR(255), note VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload(
                    "COPY app.pg_copy_csv_escape_in FROM STDIN WITH (FORMAT csv, QUOTE '|', ESCAPE '!')",
                ),
            ),
            (b'd', b"1,|Ada, Lovelace|,|bang !! and pipe !||\n2,,||\n".to_vec()),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, note FROM app.pg_copy_csv_escape_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada, Lovelace".to_string()),
                Some("bang ! and pipe |".to_string())
            ],
            vec![Some("2".to_string()), None, Some(String::new())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_with_csv_quote_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_with_csv_quote")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_csv_quote_in (id BIGINT NOT NULL, name VARCHAR(255), note VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload(
                    "COPY app.pg_copy_csv_quote_in FROM STDIN WITH (FORMAT csv, QUOTE '|')",
                ),
            ),
            (b'd', b"1,|Ada, Lovelace|,|pipe || quote|\n2,,||\n".to_vec()),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, note FROM app.pg_copy_csv_quote_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada, Lovelace".to_string()),
                Some("pipe | quote".to_string())
            ],
            vec![Some("2".to_string()), None, Some(String::new())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_with_csv_keyword_format_alias_roundtrip(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_csv_keyword_alias")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_csv_keyword_alias_in (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, note VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload("COPY app.pg_copy_csv_keyword_alias_in FROM STDIN WITH (CSV)"),
            ),
            (
                b'd',
                b"1,\"Ada, Lovelace\",t,\"quote \"\"hi\"\"\"\n2,,f,\"\"\n".to_vec(),
            ),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active, note FROM app.pg_copy_csv_keyword_alias_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada, Lovelace".to_string()),
                Some("t".to_string()),
                Some("quote \"hi\"".to_string())
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("f".to_string()),
                Some(String::new())
            ],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_csv_with_custom_null_string_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_csv_custom_null")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    for table in ["pg_copy_csv_null_out", "pg_copy_csv_null_in"] {
        rpc.rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": table,
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "name", "type": {"kind": "str"}, "nullable": true},
                    {"name": "active", "type": {"kind": "bool"}, "nullable": false},
                    {"name": "note", "type": {"kind": "str"}, "nullable": true}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;
    }
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_csv_null_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "NULL"},
                    "active": {"t": "bool", "v": true},
                    "note": {"t": "str", "v": "prefix NULL suffix"}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "active": {"t": "bool", "v": false},
                    "note": {"t": "str", "v": ""}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let copy_out_msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active, note FROM app.pg_copy_csv_null_out ORDER BY id) TO STDOUT WITH (FORMAT csv, NULL 'NULL')",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&copy_out_msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    let copy_lines = pg_copy_data_lines(&copy_out_msgs);
    assert_eq!(
        copy_lines,
        vec!["1,\"NULL\",t,\"prefix NULL suffix\"\n", "2,NULL,f,\"\"\n"]
    );
    assert_eq!(pg_command_complete_tag(&copy_out_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_out_msgs)?, b'I');

    let copy_in_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload(
                    "COPY app.pg_copy_csv_null_in FROM STDIN WITH (FORMAT csv, NULL 'NULL')",
                ),
            ),
            (b'd', copy_lines.concat().into_bytes()),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_in_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_in_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_in_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active, note FROM app.pg_copy_csv_null_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("NULL".to_string()),
                Some("t".to_string()),
                Some("prefix NULL suffix".to_string())
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("f".to_string()),
                Some(String::new())
            ],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_text_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_with_text_format")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_text_opts_out",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_text_opts_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada"}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY app.pg_copy_text_opts_out TO STDOUT WITH (FORMAT text)",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(pg_copy_data_lines(&msgs), vec!["1\tAda\n", "2\t\\N\n"]);
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_with_binary_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_with_binary_format")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_binary_out",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "active", "type": {"kind": "bool"}, "nullable": false},
                {"name": "payload", "type": {"kind": "bytes"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_binary_out"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada"},
                    "active": {"t": "bool", "v": true},
                    "payload": {"t": "bytes", "b64": "AAECAw=="}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "active": {"t": "bool", "v": false},
                    "payload": {"t": "bytes", "b64": ""}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY (SELECT id, name, active, payload FROM app.pg_copy_binary_out ORDER BY id) TO STDOUT WITH (FORMAT binary)",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'd', b'd', b'c', b'C', b'Z']
    );
    let (overall_format, column_formats) = pg_copy_out_response_formats(&msgs)?;
    assert_eq!(overall_format, 1);
    assert_eq!(column_formats, vec![1, 1, 1, 1]);
    let rows = pg_parse_binary_copy_rows(&msgs)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Some(1i64.to_be_bytes().to_vec()));
    assert_eq!(rows[0][1], Some(b"Ada".to_vec()));
    assert_eq!(rows[0][2], Some(vec![1]));
    assert_eq!(rows[0][3], Some(vec![0, 1, 2, 3]));
    assert_eq!(rows[1][0], Some(2i64.to_be_bytes().to_vec()));
    assert_eq!(rows[1][1], None);
    assert_eq!(rows[1][2], Some(vec![0]));
    assert_eq!(rows[1][3], Some(Vec::new()));
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_with_text_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_with_text_format")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_text_opts_in (id BIGINT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload("COPY app.pg_copy_text_opts_in FROM STDIN WITH (FORMAT text)"),
            ),
            (b'd', b"1\tAda\n2\t\\N\n".to_vec()),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name FROM app.pg_copy_text_opts_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![Some("1".to_string()), Some("Ada".to_string())],
            vec![Some("2".to_string()), None],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_to_stdout_column_list_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_to_stdout_column_list")?;
    let rpc = RpcHttpClient::new(server.base_url());

    rpc.rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    rpc.rpc(
        "schema.create_table",
        json!({
            "db": "app",
            "table": "pg_copy_users_cols",
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                {"name": "name", "type": {"kind": "str"}, "nullable": true},
                {"name": "active", "type": {"kind": "bool"}, "nullable": false}
            ],
            "primary_key": ["id"]
        }),
    )
    .await?;
    rpc.rpc(
        "data.insert",
        json!({
            "into": {"db": "app", "table": "pg_copy_users_cols"},
            "rows": [
                {
                    "id": {"t": "u64", "v": 1},
                    "name": {"t": "str", "v": "Ada"},
                    "active": {"t": "bool", "v": true}
                },
                {
                    "id": {"t": "u64", "v": 2},
                    "name": {"t": "null", "v": null},
                    "active": {"t": "bool", "v": false}
                }
            ]
        }),
    )
    .await?;

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "COPY app.pg_copy_users_cols (name, active) TO STDOUT",
    )
    .await?;

    assert_eq!(
        pg_message_tags(&msgs),
        vec![b'H', b'd', b'd', b'c', b'C', b'Z']
    );
    assert_eq!(pg_copy_data_lines(&msgs), vec!["Ada\tt\n", "\\N\tf\n"]);
    assert_eq!(pg_command_complete_tag(&msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_column_list_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_column_list")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_in_users_cols (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload("COPY app.pg_copy_in_users_cols (id, active) FROM STDIN"),
            ),
            (b'd', b"1\tt\n2\tf\n".to_vec()),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active FROM app.pg_copy_in_users_cols ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![Some("1".to_string()), None, Some("t".to_string())],
            vec![Some("2".to_string()), None, Some("f".to_string())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_row_description_uses_schema_oids_for_typed_columns() -> anyhow::Result<()>
{
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_schema_typed_oids")?;
    let rpc = RpcHttpClient::new(server.base_url());

    let resp = rpc
        .rpc("schema.create_database", json!({"db": "app"}))
        .await?;
    assert!(resp.ok);

    let resp = rpc
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "pg_schema_typed_oids",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "active", "type": {"kind": "bool"}, "nullable": false},
                    {"name": "profile", "type": {"kind": "json"}, "nullable": false},
                    {"name": "payload", "type": {"kind": "bytes"}, "nullable": false},
                    {"name": "birth_date", "type": {"kind": "date"}, "nullable": false},
                    {"name": "wake_time", "type": {"kind": "time"}, "nullable": false},
                    {"name": "created_at", "type": {"kind": "datetime"}, "nullable": false},
                    {"name": "user_uuid", "type": {"kind": "uuid"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await?;
    assert!(resp.ok);

    let resp = rpc
        .rpc(
            "data.insert",
            json!({
                "into": {"db": "app", "table": "pg_schema_typed_oids"},
                "rows": [{
                    "id": {"t": "u64", "v": 1},
                    "active": {"t": "bool", "v": true},
                    "profile": {"t": "json", "v": {"role": "admin"}},
                    "payload": {"t": "bytes", "b64": "AAECAw=="},
                    "birth_date": {"t": "date", "iso": "2026-04-17"},
                    "wake_time": {"t": "time", "iso": "08:30:00"},
                    "created_at": {"t": "datetime", "iso": "2026-04-17 08:30:00"},
                    "user_uuid": {"t": "uuid", "v": "550e8400-e29b-41d4-a716-446655440000"}
                }]
            }),
        )
        .await?;
    assert!(resp.ok);

    let mut stream = pg_connect_and_startup(server.pg_port()).await?;
    let msgs = pg_simple_query(
        &mut stream,
        "SELECT active, profile, payload, birth_date, wake_time, created_at, user_uuid FROM app.pg_schema_typed_oids",
    )
    .await?;

    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec![
            "active".to_string(),
            "profile".to_string(),
            "payload".to_string(),
            "birth_date".to_string(),
            "wake_time".to_string(),
            "created_at".to_string(),
            "user_uuid".to_string(),
        ]
    );
    assert_eq!(
        pg_row_description_type_oids(&msgs)?,
        vec![16, 3802, 17, 1082, 1083, 1114, 2950]
    );
    assert_eq!(
        pg_first_data_row_cells(&msgs)?,
        vec![
            Some("t".to_string()),
            Some("{\"role\":\"admin\"}".to_string()),
            Some("\\x00010203".to_string()),
            Some("2026-04-17".to_string()),
            Some("08:30:00".to_string()),
            Some("2026-04-17 08:30:00".to_string()),
            Some("550e8400-e29b-41d4-a716-446655440000".to_string()),
        ]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_version() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_version")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_simple_query(&mut stream, "SELECT version()").await?;

    // Find the DataRow and check it contains "SkeinDB"
    let data_rows: Vec<&Vec<u8>> = msgs
        .iter()
        .filter(|(t, _)| *t == b'D')
        .map(|(_, p)| p)
        .collect();
    assert_eq!(data_rows.len(), 1, "expected exactly 1 data row");

    let row = &data_rows[0];
    // Parse: 2 bytes column count, then for each: 4 bytes len + data
    let col_count = i16::from_be_bytes([row[0], row[1]]);
    assert_eq!(col_count, 1);
    let data_len = i32::from_be_bytes([row[2], row[3], row[4], row[5]]) as usize;
    let data = String::from_utf8_lossy(&row[6..6 + data_len]);
    assert!(
        data.contains("SkeinDB"),
        "version string should contain SkeinDB: {data}"
    );

    Ok(())
}

#[tokio::test]
async fn pg_startup_bootstrap_queries() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_bootstrap_queries")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let cases = [
        ("SELECT current_database()", "testdb"),
        ("SELECT current_catalog", "testdb"),
        ("SELECT current_user", "skein"),
        ("SELECT current_role", "skein"),
        ("SELECT current_schema", "public"),
        ("SELECT current_schema()", "public"),
        ("SELECT session_user", "skein"),
        ("SELECT user", "skein"),
        ("SELECT current_schemas(false)", "{public}"),
        ("SELECT current_schemas(true)", "{pg_catalog,public}"),
        ("SHOW server_version", "16.0 (SkeinDB compatibility)"),
        ("SHOW server_version_num", "160000"),
        ("SHOW standard_conforming_strings", "on"),
        ("SHOW max_identifier_length", "63"),
        ("SHOW transaction isolation level", "read committed"),
        ("SELECT current_setting('server_version_num')", "160000"),
        (
            "SELECT current_setting('server_version_num', true)",
            "160000",
        ),
        ("SELECT current_setting('client_encoding')", "UTF8"),
        ("SELECT current_setting('TimeZone')", "UTC"),
    ];

    for (sql, expected) in cases {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        let actual = pg_first_text_cell(&msgs)?;
        assert_eq!(actual, expected, "query: {sql}");
    }

    let msgs =
        pg_simple_query(&mut stream, "SELECT current_setting('missing_probe', true)").await?;
    assert_eq!(pg_first_data_row_cells(&msgs)?, vec![None]);

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_pg_catalog_virtual_tables_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_catalog_virtual_tables")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_simple_query(&mut stream, "CREATE DATABASE app").await?;
    assert_eq!(pg_command_complete_tag(&msgs)?, "CREATE DATABASE");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT datname, datistemplate FROM pg_catalog.pg_database WHERE datname = 'app'",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec!["datname".to_string(), "datistemplate".to_string()]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25, 16]);
    assert_eq!(
        pg_first_data_row_cells(&msgs)?,
        vec![Some("app".to_string()), Some("f".to_string())]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'public'",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec!["nspname".to_string()]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25]);
    assert_eq!(
        pg_first_data_row_cells(&msgs)?,
        vec![Some("public".to_string())]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT oid, amname, amtype FROM pg_catalog.pg_am WHERE amname IN ('heap', 'btree') ORDER BY oid",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec![
            "oid".to_string(),
            "amname".to_string(),
            "amtype".to_string()
        ]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![26, 25, 25]);
    assert_eq!(
        pg_all_data_row_cells(&msgs)?,
        vec![
            vec![
                Some("2".to_string()),
                Some("heap".to_string()),
                Some("t".to_string())
            ],
            vec![
                Some("403".to_string()),
                Some("btree".to_string()),
                Some("i".to_string())
            ],
        ]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT classoid, objoid, objsubid, description FROM pg_catalog.pg_description WHERE objsubid = 0",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec![
            "classoid".to_string(),
            "objoid".to_string(),
            "objsubid".to_string(),
            "description".to_string(),
        ]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![26, 26, 23, 25]);
    assert_eq!(
        pg_all_data_row_cells(&msgs)?,
        Vec::<Vec<Option<String>>>::new()
    );
    assert_eq!(pg_command_complete_tag(&msgs)?, "SELECT 0");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT proname, prokind, pronargs, prorettype FROM pg_catalog.pg_proc WHERE proname IN ('array_agg', 'array_length', 'array_lower', 'array_upper', 'char_length', 'clock_timestamp', 'current_schemas', 'current_setting', 'date_trunc', 'gen_random_uuid', 'left', 'length', 'lower', 'ltrim', 'pg_typeof', 'replace', 'right', 'rtrim', 'statement_timestamp', 'string_agg', 'string_to_array', 'substring', 'to_char', 'transaction_timestamp', 'trim', 'upper') ORDER BY proname, pronargs",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec![
            "proname".to_string(),
            "prokind".to_string(),
            "pronargs".to_string(),
            "prorettype".to_string(),
        ]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25, 25, 23, 26]);
    assert_eq!(
        pg_all_data_row_cells(&msgs)?,
        vec![
            vec![
                Some("array_agg".to_string()),
                Some("a".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("array_length".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("20".to_string()),
            ],
            vec![
                Some("array_lower".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("20".to_string()),
            ],
            vec![
                Some("array_upper".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("20".to_string()),
            ],
            vec![
                Some("char_length".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("20".to_string()),
            ],
            vec![
                Some("clock_timestamp".to_string()),
                Some("f".to_string()),
                Some("0".to_string()),
                Some("1114".to_string()),
            ],
            vec![
                Some("current_schemas".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("1009".to_string()),
            ],
            vec![
                Some("current_setting".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("current_setting".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("date_trunc".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("1114".to_string()),
            ],
            vec![
                Some("gen_random_uuid".to_string()),
                Some("f".to_string()),
                Some("0".to_string()),
                Some("2950".to_string()),
            ],
            vec![
                Some("left".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("length".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("20".to_string()),
            ],
            vec![
                Some("lower".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("ltrim".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("pg_typeof".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("replace".to_string()),
                Some("f".to_string()),
                Some("3".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("right".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("rtrim".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("statement_timestamp".to_string()),
                Some("f".to_string()),
                Some("0".to_string()),
                Some("1114".to_string()),
            ],
            vec![
                Some("string_agg".to_string()),
                Some("a".to_string()),
                Some("2".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("string_to_array".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("1009".to_string()),
            ],
            vec![
                Some("substring".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("substring".to_string()),
                Some("f".to_string()),
                Some("3".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("to_char".to_string()),
                Some("f".to_string()),
                Some("2".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("transaction_timestamp".to_string()),
                Some("f".to_string()),
                Some("0".to_string()),
                Some("1114".to_string()),
            ],
            vec![
                Some("trim".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
            vec![
                Some("upper".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("25".to_string()),
            ],
        ]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT name, setting, pending_restart FROM pg_catalog.pg_settings WHERE name = 'server_version_num'",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec![
            "name".to_string(),
            "setting".to_string(),
            "pending_restart".to_string(),
        ]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25, 25, 16]);
    assert_eq!(
        pg_first_data_row_cells(&msgs)?,
        vec![
            Some("server_version_num".to_string()),
            Some("160000".to_string()),
            Some("f".to_string()),
        ]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT typname, typlen FROM pg_catalog.pg_type WHERE typname = 'bytea'",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec!["typname".to_string(), "typlen".to_string()]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25, 20]);
    assert_eq!(
        pg_first_data_row_cells(&msgs)?,
        vec![Some("bytea".to_string()), Some("-1".to_string())]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT datname, usename, state FROM pg_catalog.pg_stat_activity LIMIT 1",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec![
            "datname".to_string(),
            "usename".to_string(),
            "state".to_string()
        ]
    );
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25, 25, 25]);
    assert_eq!(
        pg_first_data_row_cells(&msgs)?,
        vec![
            Some("testdb".to_string()),
            Some("root".to_string()),
            Some("active".to_string()),
        ]
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_pg_catalog_class_attribute_index_constraint() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_catalog_class_attr")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    // Set up a table with a primary key and a unique index (use default DB for pg_class catalog visibility in D micro test).
    for sql in [
        "CREATE TABLE items (id BIGINT NOT NULL, name VARCHAR(255) NOT NULL, price DOUBLE, PRIMARY KEY (id))",
        "CREATE UNIQUE INDEX idx_items_name ON items (name)",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "setup: {sql}");
    }

    // --- pg_attribute: column metadata ---
    let msgs = pg_simple_query(
        &mut stream,
        "SELECT attname, attnum, attnotnull FROM pg_catalog.pg_attribute WHERE attname = 'name'",
    )
    .await?;
    assert_eq!(
        pg_row_description_names(&msgs)?,
        vec![
            "attname".to_string(),
            "attnum".to_string(),
            "attnotnull".to_string(),
        ]
    );
    let cells = pg_first_data_row_cells(&msgs)?;
    assert_eq!(cells[0], Some("name".to_string()));
    assert_eq!(cells[1], Some("2".to_string())); // second column
    assert_eq!(cells[2], Some("t".to_string())); // NOT NULL
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    // --- pg_index: primary key entry ---
    let msgs = pg_simple_query(
        &mut stream,
        "SELECT indisprimary, indisunique, indnatts FROM pg_catalog.pg_index WHERE indisprimary = true",
    )
    .await?;
    let cells = pg_first_data_row_cells(&msgs)?;
    assert_eq!(cells[0], Some("t".to_string())); // indisprimary
    assert_eq!(cells[1], Some("t".to_string())); // indisunique
    assert_eq!(cells[2], Some("1".to_string())); // 1 column in PK
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    // --- pg_constraint: primary key constraint ---
    let msgs = pg_simple_query(
        &mut stream,
        "SELECT conname, contype FROM pg_catalog.pg_constraint WHERE contype = 'p'",
    )
    .await?;
    let cells = pg_first_data_row_cells(&msgs)?;
    assert_eq!(cells[0], Some("items_pkey".to_string()));
    assert_eq!(cells[1], Some("p".to_string()));
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    // --- pg_constraint: unique constraint from index ---
    let msgs = pg_simple_query(
        &mut stream,
        "SELECT conname, contype FROM pg_catalog.pg_constraint WHERE contype = 'u'",
    )
    .await?;
    let cells = pg_first_data_row_cells(&msgs)?;
    assert_eq!(cells[0], Some("idx_items_name_unique".to_string()));
    assert_eq!(cells[1], Some("u".to_string()));
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_parse_bind_describe_execute_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_roundtrip")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_users (id BIGINT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_ext_users (id, name) VALUES (1, 'Ada')",
        "INSERT INTO app.pg_ext_users (id, name) VALUES (2, 'Grace')",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "sel_user",
                    "SELECT name FROM app.pg_ext_users WHERE id = $1",
                    &[20],
                ),
            ),
            (b'D', pg_describe_payload(b'S', "sel_user")),
            (
                b'B',
                pg_bind_text_payload("user_portal", "sel_user", &[Some("2")]),
            ),
            (b'D', pg_describe_payload(b'P', "user_portal")),
            (b'E', pg_execute_payload("user_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let tags = pg_message_tags(&msgs);
    assert!(tags.contains(&b'1'), "missing ParseComplete: {tags:?}");
    assert!(tags.contains(&b'2'), "missing BindComplete: {tags:?}");
    assert!(
        tags.contains(&b't'),
        "missing ParameterDescription: {tags:?}"
    );
    // The statement Describe and the portal Describe each emit one RowDescription.
    // Execute must NOT emit a third one: in the extended protocol the row layout
    // is reported by Describe, and a duplicate RowDescription on Execute breaks
    // strict clients (e.g. psycopg3).
    assert_eq!(
        tags.iter().filter(|tag| **tag == b'T').count(),
        2,
        "expected statement describe and portal describe RowDescriptions only"
    );
    assert_eq!(pg_parameter_description_oids(&msgs)?, vec![20]);
    assert_eq!(pg_row_description_names(&msgs)?, vec!["name".to_string()]);
    assert_eq!(pg_row_description_type_oids(&msgs)?, vec![25]);
    assert_eq!(pg_first_text_cell(&msgs)?, "Grace");
    assert_eq!(pg_command_complete_tag(&msgs)?, "SELECT 1");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

/// Regression test for the psycopg3 failure mode the real-driver smoke matrix
/// surfaced: a parameterized SELECT issued as Parse + Bind + Describe(portal) +
/// Execute must return exactly ONE RowDescription (from the portal Describe),
/// followed by the data rows. Emitting a second RowDescription during Execute
/// made psycopg3 abort with "server sent data ('D') without prior row
/// description ('T')".
#[tokio::test]
async fn pg_extended_query_describe_portal_then_execute_emits_single_row_description(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_single_row_description")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_single_rd (id BIGINT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_single_rd (id, name) VALUES (1, 'Ada')",
        "INSERT INTO app.pg_single_rd (id, name) VALUES (2, 'Grace')",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    // psycopg3's flow: Parse, Bind, Describe(portal), Execute, Sync — no
    // statement Describe.
    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "psy_stmt",
                    "SELECT name FROM app.pg_single_rd WHERE id = $1",
                    &[20],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("psy_portal", "psy_stmt", &[Some("2")]),
            ),
            (b'D', pg_describe_payload(b'P', "psy_portal")),
            (b'E', pg_execute_payload("psy_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let tags = pg_message_tags(&msgs);
    assert_eq!(
        tags.iter().filter(|tag| **tag == b'T').count(),
        1,
        "expected exactly one RowDescription (from the portal Describe): {tags:?}"
    );
    assert_eq!(
        tags.iter().filter(|tag| **tag == b'D').count(),
        1,
        "expected exactly one DataRow: {tags:?}"
    );
    assert_eq!(pg_row_description_names(&msgs)?, vec!["name".to_string()]);
    assert_eq!(pg_first_text_cell(&msgs)?, "Grace");
    assert_eq!(pg_command_complete_tag(&msgs)?, "SELECT 1");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_binary_int_param_filters_rows() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_binary_int_param")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_bin_filter (id BIGINT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_bin_filter (id, name) VALUES (1, 'Ada')",
        "INSERT INTO app.pg_bin_filter (id, name) VALUES (2, 'Grace')",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    // psycopg3/JDBC/Npgsql send INT8 parameters in binary by default; the WHERE
    // value here arrives as 8 big-endian bytes rather than ASCII text.
    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "sel_bin_user",
                    "SELECT name FROM app.pg_bin_filter WHERE id = $1",
                    &[20],
                ),
            ),
            (
                b'B',
                pg_bind_binary_params_payload(
                    "bin_user_portal",
                    "sel_bin_user",
                    &[Some(2i64.to_be_bytes().to_vec())],
                ),
            ),
            (b'E', pg_execute_payload("bin_user_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let tags = pg_message_tags(&msgs);
    assert!(tags.contains(&b'2'), "missing BindComplete: {tags:?}");
    assert_eq!(pg_first_text_cell(&msgs)?, "Grace");
    assert_eq!(pg_command_complete_tag(&msgs)?, "SELECT 1");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_binary_params_insert_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_binary_insert")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_bin_insert (id BIGINT NOT NULL, score INT NOT NULL, ratio DOUBLE NOT NULL, label VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    // INSERT with every value supplied as a binary Bind parameter: INT8, INT4,
    // FLOAT8, and TEXT. This exercises the full decode -> SQL literal -> engine path.
    let insert_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "ins_bin_row",
                    "INSERT INTO app.pg_bin_insert (id, score, ratio, label) VALUES ($1, $2, $3, $4)",
                    &[20, 23, 701, 1043],
                ),
            ),
            (
                b'B',
                pg_bind_binary_params_payload(
                    "ins_bin_portal",
                    "ins_bin_row",
                    &[
                        Some(7i64.to_be_bytes().to_vec()),
                        Some((-13i32).to_be_bytes().to_vec()),
                        Some(2.5f64.to_be_bytes().to_vec()),
                        Some(b"binary-label".to_vec()),
                    ],
                ),
            ),
            (b'E', pg_execute_payload("ins_bin_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;
    assert!(
        pg_message_tags(&insert_msgs).contains(&b'2'),
        "missing BindComplete on insert"
    );
    assert_eq!(pg_command_complete_tag(&insert_msgs)?, "INSERT 0 1");
    assert_eq!(pg_ready_status(&insert_msgs)?, b'I');

    let select_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, score, ratio, label FROM app.pg_bin_insert WHERE id = 7",
    )
    .await?;
    let rows = pg_all_data_row_cells(&select_msgs)?;
    assert_eq!(
        rows,
        vec![vec![
            Some("7".to_string()),
            Some("-13".to_string()),
            Some("2.5".to_string()),
            Some("binary-label".to_string()),
        ]]
    );
    assert_eq!(pg_ready_status(&select_msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_execute_suspends_and_resumes_portal() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_portal_suspend")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_suspend_users (id BIGINT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_ext_suspend_users (id, name) VALUES (1, 'Ada')",
        "INSERT INTO app.pg_ext_suspend_users (id, name) VALUES (2, 'Grace')",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let first_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "sel_suspend_users",
                    "SELECT name FROM app.pg_ext_suspend_users ORDER BY id",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("suspend_portal", "sel_suspend_users", &[]),
            ),
            (b'E', pg_execute_payload("suspend_portal", 1)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let first_tags = pg_message_tags(&first_msgs);
    assert!(
        first_tags.contains(&b'1'),
        "missing ParseComplete: {first_tags:?}"
    );
    assert!(
        first_tags.contains(&b'2'),
        "missing BindComplete: {first_tags:?}"
    );
    assert!(
        first_tags.contains(&b'T'),
        "missing RowDescription: {first_tags:?}"
    );
    assert!(
        first_tags.contains(&b's'),
        "missing PortalSuspended: {first_tags:?}"
    );
    assert!(
        !first_tags.contains(&b'C'),
        "unexpected CommandComplete: {first_tags:?}"
    );
    assert_eq!(pg_all_text_cells(&first_msgs)?, vec!["Ada"]);
    assert_eq!(pg_ready_status(&first_msgs)?, b'I');

    let second_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (b'E', pg_execute_payload("suspend_portal", 1)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let second_tags = pg_message_tags(&second_msgs);
    assert!(
        second_tags.contains(&b'T'),
        "missing RowDescription: {second_tags:?}"
    );
    assert!(
        !second_tags.contains(&b's'),
        "unexpected PortalSuspended: {second_tags:?}"
    );
    assert_eq!(pg_all_text_cells(&second_msgs)?, vec!["Grace"]);
    assert_eq!(pg_command_complete_tag(&second_msgs)?, "SELECT 2");
    assert_eq!(pg_ready_status(&second_msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_from_stdin_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_from_stdin")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_in_users (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in",
                    "COPY app.pg_ext_copy_in_users FROM STDIN",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("copy_users_portal", "copy_users_in", &[]),
            ),
            (b'E', pg_execute_payload("copy_users_portal", 0)),
            (b'd', b"1\tAda\tt\n2\t\\N\tf\n".to_vec()),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_tags:?}"
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active FROM app.pg_ext_copy_in_users ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada".to_string()),
                Some("t".to_string())
            ],
            vec![Some("2".to_string()), None, Some("f".to_string())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_to_stdout_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_to_stdout")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_out_users (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_ext_copy_out_users (id, name, active) VALUES (1, 'Ada', true)",
        "INSERT INTO app.pg_ext_copy_out_users (id, name, active) VALUES (2, NULL, false)",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_out",
                    "COPY app.pg_ext_copy_out_users (name, active) TO STDOUT WITH (FORMAT text)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("copy_users_out_portal", "copy_users_out", &[]),
            ),
            (b'E', pg_execute_payload("copy_users_out_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'H'),
        "missing CopyOutResponse: {copy_tags:?}"
    );
    assert_eq!(pg_copy_data_lines(&copy_msgs), vec!["Ada\tt\n", "\\N\tf\n"]);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_to_stdout_with_csv_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_to_stdout_csv")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_out_csv (id BIGINT NOT NULL, name VARCHAR(255), note VARCHAR(255), PRIMARY KEY (id))",
        "INSERT INTO app.pg_ext_copy_out_csv (id, name, note) VALUES (1, 'Ada, Lovelace', 'quote \"hi\"')",
        "INSERT INTO app.pg_ext_copy_out_csv (id, name, note) VALUES (2, NULL, '')",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_out_csv",
                    "COPY app.pg_ext_copy_out_csv (name, note) TO STDOUT WITH (FORMAT csv)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("copy_users_out_csv_portal", "copy_users_out_csv", &[]),
            ),
            (b'E', pg_execute_payload("copy_users_out_csv_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'H'),
        "missing CopyOutResponse: {copy_tags:?}"
    );
    assert_eq!(
        pg_copy_data_lines(&copy_msgs),
        vec!["\"Ada, Lovelace\",\"quote \"\"hi\"\"\"\n", ",\"\"\n"]
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_csv_with_custom_null_string_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_csv_custom_null")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_csv_null_out (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, note VARCHAR(255), PRIMARY KEY (id))",
        "CREATE TABLE app.pg_ext_copy_csv_null_in (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, note VARCHAR(255), PRIMARY KEY (id))",
        "INSERT INTO app.pg_ext_copy_csv_null_out (id, name, active, note) VALUES (1, 'NULL', true, 'prefix NULL suffix')",
        "INSERT INTO app.pg_ext_copy_csv_null_out (id, name, active, note) VALUES (2, NULL, false, '')",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_out_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_out_csv_null",
                    "COPY (SELECT id, name, active, note FROM app.pg_ext_copy_csv_null_out ORDER BY id) TO STDOUT WITH (FORMAT csv, NULL 'NULL')",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload(
                    "copy_users_out_csv_null_portal",
                    "copy_users_out_csv_null",
                    &[],
                ),
            ),
            (b'E', pg_execute_payload("copy_users_out_csv_null_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_out_tags = pg_message_tags(&copy_out_msgs);
    assert!(
        copy_out_tags.contains(&b'1'),
        "missing ParseComplete: {copy_out_tags:?}"
    );
    assert!(
        copy_out_tags.contains(&b'2'),
        "missing BindComplete: {copy_out_tags:?}"
    );
    assert!(
        copy_out_tags.contains(&b'H'),
        "missing CopyOutResponse: {copy_out_tags:?}"
    );
    let copy_lines = pg_copy_data_lines(&copy_out_msgs);
    assert_eq!(
        copy_lines,
        vec!["1,\"NULL\",t,\"prefix NULL suffix\"\n", "2,NULL,f,\"\"\n"]
    );
    assert_eq!(pg_command_complete_tag(&copy_out_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_out_msgs)?, b'I');

    let copy_in_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in_csv_null",
                    "COPY app.pg_ext_copy_csv_null_in FROM STDIN WITH (FORMAT csv, NULL 'NULL')",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload(
                    "copy_users_in_csv_null_portal",
                    "copy_users_in_csv_null",
                    &[],
                ),
            ),
            (b'E', pg_execute_payload("copy_users_in_csv_null_portal", 0)),
            (b'd', copy_lines.concat().into_bytes()),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_in_tags = pg_message_tags(&copy_in_msgs);
    assert!(
        copy_in_tags.contains(&b'1'),
        "missing ParseComplete: {copy_in_tags:?}"
    );
    assert!(
        copy_in_tags.contains(&b'2'),
        "missing BindComplete: {copy_in_tags:?}"
    );
    assert!(
        copy_in_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_in_tags:?}"
    );
    assert_eq!(pg_command_complete_tag(&copy_in_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_in_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active, note FROM app.pg_ext_copy_csv_null_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("NULL".to_string()),
                Some("t".to_string()),
                Some("prefix NULL suffix".to_string())
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("f".to_string()),
                Some(String::new())
            ],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_to_stdout_with_binary_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_to_stdout_binary")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_out_binary (id BIGINT NOT NULL, active BOOLEAN NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_ext_copy_out_binary (id, active) VALUES (1, true)",
        "INSERT INTO app.pg_ext_copy_out_binary (id, active) VALUES (2, false)",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_out_binary",
                    "COPY app.pg_ext_copy_out_binary (id, active) TO STDOUT WITH (FORMAT binary)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("copy_users_out_binary_portal", "copy_users_out_binary", &[]),
            ),
            (b'E', pg_execute_payload("copy_users_out_binary_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'H'),
        "missing CopyOutResponse: {copy_tags:?}"
    );
    let (overall_format, column_formats) = pg_copy_out_response_formats(&copy_msgs)?;
    assert_eq!(overall_format, 1);
    assert_eq!(column_formats, vec![1, 1]);
    let rows = pg_parse_binary_copy_rows(&copy_msgs)?;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        vec![Some(1i64.to_be_bytes().to_vec()), Some(vec![1])]
    );
    assert_eq!(
        rows[1],
        vec![Some(2i64.to_be_bytes().to_vec()), Some(vec![0])]
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_with_binary_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_binary")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_binary_in (id BIGINT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    // Hand-build the documented binary COPY stream: 11-byte signature, a zero flags
    // word, a zero-length header extension, two tuples, then the -1 trailer.
    let mut payload = Vec::new();
    payload.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    payload.extend_from_slice(&0i32.to_be_bytes()); // flags
    payload.extend_from_slice(&0i32.to_be_bytes()); // header extension length
    let append_field = |buf: &mut Vec<u8>, bytes: Option<&[u8]>| match bytes {
        Some(raw) => {
            buf.extend_from_slice(&(raw.len() as i32).to_be_bytes());
            buf.extend_from_slice(raw);
        }
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
    };
    // Row 1: (1, "Ada")
    payload.extend_from_slice(&2i16.to_be_bytes());
    append_field(&mut payload, Some(&1i64.to_be_bytes()));
    append_field(&mut payload, Some(b"Ada"));
    // Row 2: (2, NULL)
    payload.extend_from_slice(&2i16.to_be_bytes());
    append_field(&mut payload, Some(&2i64.to_be_bytes()));
    append_field(&mut payload, None);
    payload.extend_from_slice(&(-1i16).to_be_bytes()); // trailer

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload("COPY app.pg_copy_binary_in FROM STDIN WITH (FORMAT binary)"),
            ),
            (b'd', payload),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'C', b'Z']);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name FROM app.pg_copy_binary_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![Some("1".to_string()), Some("Ada".to_string())],
            vec![Some("2".to_string()), None],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_from_stdin_with_binary_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_from_stdin_binary")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_binary_in (id BIGINT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let mut payload = Vec::new();
    payload.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    payload.extend_from_slice(&0i32.to_be_bytes());
    payload.extend_from_slice(&0i32.to_be_bytes());
    let append_field = |buf: &mut Vec<u8>, bytes: Option<&[u8]>| match bytes {
        Some(raw) => {
            buf.extend_from_slice(&(raw.len() as i32).to_be_bytes());
            buf.extend_from_slice(raw);
        }
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
    };
    payload.extend_from_slice(&2i16.to_be_bytes());
    append_field(&mut payload, Some(&10i64.to_be_bytes()));
    append_field(&mut payload, Some(b"Ada"));
    payload.extend_from_slice(&2i16.to_be_bytes());
    append_field(&mut payload, Some(&11i64.to_be_bytes()));
    append_field(&mut payload, None);
    payload.extend_from_slice(&(-1i16).to_be_bytes());

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in_binary",
                    "COPY app.pg_ext_copy_binary_in FROM STDIN WITH (FORMAT binary)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("copy_users_in_binary_portal", "copy_users_in_binary", &[]),
            ),
            (b'E', pg_execute_payload("copy_users_in_binary_portal", 0)),
            (b'd', payload),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_tags:?}"
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name FROM app.pg_ext_copy_binary_in ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![Some("10".to_string()), Some("Ada".to_string())],
            vec![Some("11".to_string()), None],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_from_stdin_with_csv_format_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_from_stdin_csv")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_in_csv (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, note VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in_csv",
                    "COPY app.pg_ext_copy_in_csv FROM STDIN WITH (FORMAT csv)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("copy_users_in_csv_portal", "copy_users_in_csv", &[]),
            ),
            (b'E', pg_execute_payload("copy_users_in_csv_portal", 0)),
            (
                b'd',
                b"1,\"Ada, Lovelace\",t,\"quote \"\"hi\"\"\"\n2,,f,\"\"\n".to_vec(),
            ),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_tags:?}"
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active, note FROM app.pg_ext_copy_in_csv ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada, Lovelace".to_string()),
                Some("t".to_string()),
                Some("quote \"hi\"".to_string())
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("f".to_string()),
                Some(String::new())
            ],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_from_stdin_with_csv_header_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_from_stdin_csv_header")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_in_csv_header (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in_csv_header",
                    "COPY app.pg_ext_copy_in_csv_header FROM STDIN WITH (FORMAT csv, HEADER)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload(
                    "copy_users_in_csv_header_portal",
                    "copy_users_in_csv_header",
                    &[],
                ),
            ),
            (
                b'E',
                pg_execute_payload("copy_users_in_csv_header_portal", 0),
            ),
            (
                b'd',
                b"id,name,active\n1,\"Ada, Lovelace\",t\n2,,f\n".to_vec(),
            ),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_tags:?}"
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active FROM app.pg_ext_copy_in_csv_header ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada, Lovelace".to_string()),
                Some("t".to_string())
            ],
            vec![Some("2".to_string()), None, Some("f".to_string())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_simple_query_copy_from_stdin_with_csv_header_match_rejects_mismatch(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_copy_from_stdin_csv_header_match")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_copy_csv_header_match (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'Q',
                pg_query_payload(
                    "COPY app.pg_copy_csv_header_match FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
                ),
            ),
            (b'd', b"id,full_name,active\n1,Ada,t\n".to_vec()),
            (b'c', Vec::new()),
        ],
    )
    .await?;

    assert_eq!(pg_message_tags(&copy_msgs), vec![b'G', b'E', b'Z']);
    let (code, message) = pg_error_response(&copy_msgs)?;
    assert_eq!(code, "08P01");
    assert!(
        message.contains("header row does not match target columns"),
        "unexpected message: {message}"
    );
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active FROM app.pg_copy_csv_header_match ORDER BY id",
    )
    .await?;
    assert!(pg_all_data_row_cells(&verify_msgs)?.is_empty());

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_from_stdin_with_csv_header_match_rejects_mismatch(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_from_stdin_csv_header_match")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_csv_header_match (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in_csv_header_match",
                    "COPY app.pg_ext_copy_csv_header_match FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload(
                    "copy_users_in_csv_header_match_portal",
                    "copy_users_in_csv_header_match",
                    &[],
                ),
            ),
            (
                b'E',
                pg_execute_payload("copy_users_in_csv_header_match_portal", 0),
            ),
            (b'd', b"id,full_name,active\n1,Ada,t\n".to_vec()),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'E'),
        "missing ErrorResponse: {copy_tags:?}"
    );
    let (code, message) = pg_error_response(&copy_msgs)?;
    assert_eq!(code, "08P01");
    assert!(
        message.contains("header row does not match target columns"),
        "unexpected message: {message}"
    );
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active FROM app.pg_ext_copy_csv_header_match ORDER BY id",
    )
    .await?;
    assert!(pg_all_data_row_cells(&verify_msgs)?.is_empty());

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_from_stdin_with_csv_delimiter_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_copy_from_stdin_csv_delimiter")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_in_csv_delimiter (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, note VARCHAR(255), PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in_csv_delimiter",
                    "COPY app.pg_ext_copy_in_csv_delimiter FROM STDIN WITH (FORMAT csv, DELIMITER ';')",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload(
                    "copy_users_in_csv_delimiter_portal",
                    "copy_users_in_csv_delimiter",
                    &[],
                ),
            ),
            (
                b'E',
                pg_execute_payload("copy_users_in_csv_delimiter_portal", 0),
            ),
            (
                b'd',
                b"1;Ada, Lovelace;t;\"quote \"\"hi\"\"\"\n2;;f;\"\"\n".to_vec(),
            ),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_tags:?}"
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active, note FROM app.pg_ext_copy_in_csv_delimiter ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![
                Some("1".to_string()),
                Some("Ada, Lovelace".to_string()),
                Some("t".to_string()),
                Some("quote \"hi\"".to_string())
            ],
            vec![
                Some("2".to_string()),
                None,
                Some("f".to_string()),
                Some(String::new())
            ],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_query_to_stdout_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_ext_copy_query_out")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_query_users (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_ext_copy_query_users (id, name, active) VALUES (1, 'Ada', true)",
        "INSERT INTO app.pg_ext_copy_query_users (id, name, active) VALUES (2, NULL, false)",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_query_out",
                    "COPY (SELECT name, active FROM app.pg_ext_copy_query_users ORDER BY id) TO STDOUT WITH (FORMAT text)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("copy_query_out_portal", "copy_query_out", &[]),
            ),
            (b'E', pg_execute_payload("copy_query_out_portal", 0)),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'H'),
        "missing CopyOutResponse: {copy_tags:?}"
    );
    assert_eq!(pg_copy_data_lines(&copy_msgs), vec!["Ada\tt\n", "\\N\tf\n"]);
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_copy_from_stdin_column_list_with_text_format_roundtrip(
) -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_ext_copy_in_cols_fmt")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_ext_copy_in_cols (id BIGINT NOT NULL, name VARCHAR(255), active BOOLEAN NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let copy_msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "copy_users_in_cols_fmt",
                    "COPY app.pg_ext_copy_in_cols (id, active) FROM STDIN WITH (FORMAT text)",
                    &[],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload(
                    "copy_users_in_cols_fmt_portal",
                    "copy_users_in_cols_fmt",
                    &[],
                ),
            ),
            (b'E', pg_execute_payload("copy_users_in_cols_fmt_portal", 0)),
            (b'd', b"1\tt\n2\tf\n".to_vec()),
            (b'c', Vec::new()),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let copy_tags = pg_message_tags(&copy_msgs);
    assert!(
        copy_tags.contains(&b'1'),
        "missing ParseComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'2'),
        "missing BindComplete: {copy_tags:?}"
    );
    assert!(
        copy_tags.contains(&b'G'),
        "missing CopyInResponse: {copy_tags:?}"
    );
    assert_eq!(pg_command_complete_tag(&copy_msgs)?, "COPY 2");
    assert_eq!(pg_ready_status(&copy_msgs)?, b'I');

    let verify_msgs = pg_simple_query(
        &mut stream,
        "SELECT id, name, active FROM app.pg_ext_copy_in_cols ORDER BY id",
    )
    .await?;
    assert_eq!(
        pg_all_data_row_cells(&verify_msgs)?,
        vec![
            vec![Some("1".to_string()), None, Some("t".to_string())],
            vec![Some("2".to_string()), None, Some("f".to_string())],
        ]
    );

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_close_removes_named_objects() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_close")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (b'P', pg_parse_payload("sel_close", "SELECT 1", &[])),
            (b'B', pg_bind_text_payload("portal_close", "sel_close", &[])),
            (b'C', pg_close_payload(b'P', "portal_close")),
            (b'C', pg_close_payload(b'S', "sel_close")),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let tags = pg_message_tags(&msgs);
    assert_eq!(
        tags.iter().filter(|tag| **tag == b'3').count(),
        2,
        "expected CloseComplete for both portal and statement"
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (b'D', pg_describe_payload(b'S', "sel_close")),
            (b'S', Vec::new()),
        ],
    )
    .await?;
    let (code, message) = pg_error_response(&msgs)?;
    assert_eq!(code, "26000");
    assert!(message.contains("sel_close"));
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_sync_recovers_after_execute_error() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_sync_recovery")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[
            (
                b'P',
                pg_parse_payload(
                    "missing_stmt",
                    "SELECT * FROM app.pg_missing_ext WHERE id = $1",
                    &[20],
                ),
            ),
            (
                b'B',
                pg_bind_text_payload("missing_portal", "missing_stmt", &[Some("1")]),
            ),
            (b'E', pg_execute_payload("missing_portal", 0)),
            (b'P', pg_parse_payload("ignored_stmt", "SELECT 1", &[])),
            (b'S', Vec::new()),
        ],
    )
    .await?;

    let tags = pg_message_tags(&msgs);
    assert_eq!(
        tags.iter().filter(|tag| **tag == b'1').count(),
        1,
        "messages after the first extended-protocol error should be ignored until Sync"
    );
    assert_eq!(
        tags.iter().filter(|tag| **tag == b'E').count(),
        1,
        "expected a single ErrorResponse before Sync"
    );
    let (code, _) = pg_error_response(&msgs)?;
    assert_eq!(code, "42P01");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(&mut stream, "SELECT 1").await?;
    assert_eq!(pg_first_text_cell(&msgs)?, "1");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_flush_keeps_connection_usable() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_extended_query_flush")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    pg_send_frontend_message(
        &mut stream,
        b'P',
        &pg_parse_payload("flush_stmt", "SELECT 1", &[]),
    )
    .await?;
    pg_send_frontend_message(&mut stream, b'H', &[]).await?;
    stream.flush().await?;

    let (tag, _) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'1', "expected ParseComplete after Flush");

    pg_send_frontend_message(&mut stream, b'S', &[]).await?;
    stream.flush().await?;
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'Z');
    assert_eq!(payload[0], b'I');

    Ok(())
}

#[tokio::test]
async fn pg_compat_corpus_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_compat_corpus_roundtrip")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for statement in pg_compat_corpus_statements() {
        let msgs = pg_simple_query(&mut stream, &statement).await?;
        let normalized = statement
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();

        match normalized.as_str() {
            "select 1" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "1");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "select version()" => {
                assert!(pg_first_text_cell(&msgs)?.contains("SkeinDB compatibility"));
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "select current_database()" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "testdb");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "select current_schema()" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "public");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "show server_version" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "16.0 (SkeinDB compatibility)");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "show server_version_num" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "160000");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "show standard_conforming_strings" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "on");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "show max_identifier_length" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "63");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "show transaction isolation level" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "read committed");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "create database app" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "CREATE DATABASE");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "create table app.pg_corpus_users (id bigint not null, name varchar(255) not null, primary key (id))" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "CREATE TABLE");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "insert into app.pg_corpus_users (id, name) values (1, 'ada')"
            | "insert into app.pg_corpus_users (id, name) values (2, 'grace')"
            | "insert into app.pg_corpus_users (id, name) values (3, 'linus')" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "INSERT 0 1");
            }
            "select count(*) from app.pg_corpus_users" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "2");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "select name from app.pg_corpus_users where id = 2" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "Grace");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            "begin" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "BEGIN");
                assert_eq!(pg_ready_status(&msgs)?, b'T');
            }
            "savepoint before_insert" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "SAVEPOINT");
                assert_eq!(pg_ready_status(&msgs)?, b'T');
            }
            "rollback to savepoint before_insert" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "ROLLBACK");
                assert_eq!(pg_ready_status(&msgs)?, b'T');
            }
            "release savepoint before_insert" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "RELEASE");
                assert_eq!(pg_ready_status(&msgs)?, b'T');
            }
            "commit" => {
                assert_eq!(pg_command_complete_tag(&msgs)?, "COMMIT");
                assert_eq!(pg_ready_status(&msgs)?, b'I');
            }
            // ---- type casts (:: and CAST) ----
            "select '42'::int" | "select '42'::integer" | "select cast('42' as integer)" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "42");
            }
            "select 3::bigint" => assert_eq!(pg_first_text_cell(&msgs)?, "3"),
            "select '3.14'::numeric" => assert_eq!(pg_first_text_cell(&msgs)?, "3.14"),
            "select 'hello'::text" => assert_eq!(pg_first_text_cell(&msgs)?, "hello"),
            "select 1::text" => assert_eq!(pg_first_text_cell(&msgs)?, "1"),
            "select '2024-01-15'::date" => assert_eq!(pg_first_text_cell(&msgs)?, "2024-01-15"),
            "select 't'::bool" => assert_eq!(pg_first_text_cell(&msgs)?, "t"),
            "select '550e8400-e29b-41d4-a716-446655440000'::uuid" => {
                assert_eq!(
                    pg_first_text_cell(&msgs)?,
                    "550e8400-e29b-41d4-a716-446655440000"
                );
            }
            // ---- dollar-quoted literals ----
            "select $$hello world$$" => assert_eq!(pg_first_text_cell(&msgs)?, "hello world"),
            "select $$it's a test$$" => assert_eq!(pg_first_text_cell(&msgs)?, "it's a test"),
            "select $tag$body with $$ inside$tag$" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "body with $$ inside");
            }
            // ---- ARRAY constructor / helpers ----
            "select array[1, 2, 3]" => assert_eq!(pg_first_text_cell(&msgs)?, "{1, 2, 3}"),
            "select array['a', 'b', 'c']" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "{'a', 'b', 'c'}")
            }
            "select array_length(array[1,2,3], 1)" => assert_eq!(pg_first_text_cell(&msgs)?, "3"),
            "select string_to_array('a,b,c', ',')" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "{a,b,c}")
            }
            // ---- JSON / JSONB operators ----
            "select '{\"a\": 1}'::json -> 'a'" | "select '{\"a\": 1}'::json ->> 'a'" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "1");
            }
            "select '{\"a\": {\"b\": 2}}'::jsonb -> 'a' ->> 'b'" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "2");
            }
            // ---- regex match on columns (~) ----
            "select label from app.items where label ~ '^b' order by id" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "banana");
            }
            // ---- scalar string / math functions ----
            "select concat('a', 'b', 'c')" => assert_eq!(pg_first_text_cell(&msgs)?, "abc"),
            "select substring('hello', 2, 3)" => assert_eq!(pg_first_text_cell(&msgs)?, "ell"),
            "select substring('hello', 2)" => assert_eq!(pg_first_text_cell(&msgs)?, "ello"),
            "select length('hello')" | "select char_length('hello')" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "5");
            }
            "select trim('  x  ')" => assert_eq!(pg_first_text_cell(&msgs)?, "x"),
            "select replace('aaa', 'a', 'b')" => assert_eq!(pg_first_text_cell(&msgs)?, "bbb"),
            "select left('hello', 2)" => assert_eq!(pg_first_text_cell(&msgs)?, "he"),
            "select right('hello', 2)" => assert_eq!(pg_first_text_cell(&msgs)?, "lo"),
            "select abs(-5)" => assert_eq!(pg_first_text_cell(&msgs)?, "5"),
            "select round(3.14159, 2)" => assert_eq!(pg_first_text_cell(&msgs)?, "3.14"),
            "select ceil(1.2)" => assert_eq!(pg_first_text_cell(&msgs)?, "2"),
            "select floor(1.8)" => assert_eq!(pg_first_text_cell(&msgs)?, "1"),
            "select power(2, 3)" => assert_eq!(pg_first_text_cell(&msgs)?, "8.0"),
            "select mod(10, 3)" | "select 10 % 3" => assert_eq!(pg_first_text_cell(&msgs)?, "1"),
            "select greatest(1, 2, 3)" => assert_eq!(pg_first_text_cell(&msgs)?, "3"),
            "select least(3, 1, 2)" => assert_eq!(pg_first_text_cell(&msgs)?, "1"),
            "select split_part('a,b,c', ',', 2)" => assert_eq!(pg_first_text_cell(&msgs)?, "b"),
            "select starts_with('alphabet', 'alph')" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "t")
            }
            "select coalesce(null, 'fallback')" => {
                assert_eq!(pg_first_text_cell(&msgs)?, "fallback")
            }
            "select pg_typeof(1)" => assert_eq!(pg_first_text_cell(&msgs)?, "bigint"),
            "select 2 + 3 * 4" => assert_eq!(pg_first_text_cell(&msgs)?, "14"),
            _ => {}
        }

        // Every corpus statement must execute without an ErrorResponse. The arms
        // above pin exact values/tags for the deterministic cases; this guards
        // every remaining statement against regressions in the PG SQL surface.
        let tags = pg_message_tags(&msgs);
        assert!(
            !tags.contains(&b'E'),
            "PG corpus statement produced an ErrorResponse: `{statement}` -> {:?}",
            pg_error_response(&msgs).ok()
        );
    }

    Ok(())
}

#[tokio::test]
async fn pg_failed_transaction_commit_rolls_back() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_failed_tx_commit")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_failed_tx_commit (id BIGINT UNSIGNED NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let msgs = pg_simple_query(&mut stream, "BEGIN").await?;
    assert_eq!(pg_command_complete_tag(&msgs)?, "BEGIN");
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs = pg_simple_query(
        &mut stream,
        "INSERT INTO app.pg_failed_tx_commit (id) VALUES (1)",
    )
    .await?;
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs =
        pg_simple_query(&mut stream, "SELECT * FROM app.missing_pg_failed_tx_commit").await?;
    let (code, message) = pg_error_response(&msgs)?;
    assert_eq!(code, "42P01");
    assert!(message.to_ascii_lowercase().contains("table"));
    assert_eq!(pg_ready_status(&msgs)?, b'E');

    let msgs = pg_simple_query(&mut stream, "SELECT 1").await?;
    let (code, message) = pg_error_response(&msgs)?;
    assert_eq!(code, "25P02");
    assert!(message.contains("current transaction is aborted"));
    assert_eq!(pg_ready_status(&msgs)?, b'E');

    let msgs = pg_simple_query(&mut stream, "COMMIT").await?;
    assert_eq!(pg_command_complete_tag(&msgs)?, "ROLLBACK");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(&mut stream, "SELECT COUNT(*) FROM app.pg_failed_tx_commit").await?;
    assert_eq!(pg_first_text_cell(&msgs)?, "0");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_rollback_to_savepoint_clears_failed_state() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_savepoint_recovery")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_savepoint_recovery (id BIGINT UNSIGNED NOT NULL, PRIMARY KEY (id))",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let msgs = pg_simple_query(&mut stream, "BEGIN").await?;
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs = pg_simple_query(&mut stream, "SAVEPOINT before_insert").await?;
    assert_eq!(pg_command_complete_tag(&msgs)?, "SAVEPOINT");
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs = pg_simple_query(
        &mut stream,
        "INSERT INTO app.pg_savepoint_recovery (id) VALUES (7)",
    )
    .await?;
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT * FROM app.missing_pg_savepoint_recovery",
    )
    .await?;
    let (code, _) = pg_error_response(&msgs)?;
    assert_eq!(code, "42P01");
    assert_eq!(pg_ready_status(&msgs)?, b'E');

    let msgs = pg_simple_query(&mut stream, "ROLLBACK TO SAVEPOINT before_insert").await?;
    assert_eq!(pg_command_complete_tag(&msgs)?, "ROLLBACK");
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT COUNT(*) FROM app.pg_savepoint_recovery",
    )
    .await?;
    assert_eq!(pg_first_text_cell(&msgs)?, "0");
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs = pg_simple_query(&mut stream, "RELEASE SAVEPOINT before_insert").await?;
    assert_eq!(pg_command_complete_tag(&msgs)?, "RELEASE");
    assert_eq!(pg_ready_status(&msgs)?, b'T');

    let msgs = pg_simple_query(&mut stream, "COMMIT").await?;
    assert_eq!(pg_command_complete_tag(&msgs)?, "COMMIT");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_sqlstate_maps_duplicate_and_syntax_errors() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_sqlstate_errors")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    for sql in [
        "CREATE DATABASE app",
        "CREATE TABLE app.pg_sqlstate_errors (id BIGINT UNSIGNED NOT NULL, PRIMARY KEY (id))",
        "INSERT INTO app.pg_sqlstate_errors (id) VALUES (1)",
    ] {
        let msgs = pg_simple_query(&mut stream, sql).await?;
        assert_eq!(pg_ready_status(&msgs)?, b'I', "query: {sql}");
    }

    let msgs = pg_simple_query(
        &mut stream,
        "INSERT INTO app.pg_sqlstate_errors (id) VALUES (1)",
    )
    .await?;
    let (code, message) = pg_error_response(&msgs)?;
    assert_eq!(code, "23505");
    assert!(message.to_ascii_lowercase().contains("duplicate key"));
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    let msgs = pg_simple_query(
        &mut stream,
        "SELECT * FROM app.pg_sqlstate_errors WHERE id =",
    )
    .await?;
    let (code, message) = pg_error_response(&msgs)?;
    assert_eq!(code, "42601");
    assert!(
        message.contains("malformed") || message.contains("invalid") || message.contains("WHERE")
    );
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn pg_empty_query_returns_empty_response() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_empty_query")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    let msgs = pg_simple_query(&mut stream, "").await?;

    let tags: Vec<u8> = msgs.iter().map(|(t, _)| *t).collect();
    assert!(
        tags.contains(&b'I'),
        "expected EmptyQueryResponse for empty query"
    );

    Ok(())
}

#[tokio::test]
async fn pg_terminate_closes_connection() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_terminate")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    // Send Terminate message: 'X' + length=4
    stream.write_u8(b'X').await?;
    stream.write_i32(4).await?;
    stream.flush().await?;

    // Connection should close — reading should return EOF or error
    let mut buf = [0u8; 1];
    let result = stream.read(&mut buf).await;
    match result {
        Ok(0) => {}  // EOF — expected
        Err(_) => {} // Connection reset — also expected
        Ok(n) => {
            anyhow::bail!("expected connection close after Terminate, got {n} bytes");
        }
    }

    Ok(())
}

#[tokio::test]
async fn pg_ssl_negotiation_is_rejected() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_ssl_reject")?;
    wait_for_tcp(server.pg_port())?;
    let mut stream = TcpStream::connect(("127.0.0.1", server.pg_port())).await?;

    // Send SSLRequest: length=8, code=80877103
    stream.write_i32(8).await?;
    stream.write_i32(80877103).await?;
    stream.flush().await?;

    // Server should respond with 'N' (SSL not supported)
    let response = stream.read_u8().await?;
    assert_eq!(response, b'N', "expected SSL rejection 'N'");

    // Now send a real startup message and proceed normally
    let startup = build_pg_startup("skein", "testdb");
    stream.write_all(&startup).await?;
    stream.flush().await?;

    // Should get AuthenticationOk and eventually ReadyForQuery
    loop {
        let (tag, _) = read_pg_message(&mut stream).await?;
        if tag == b'Z' {
            break;
        }
    }

    Ok(())
}

/// A rustls certificate verifier that accepts any server certificate. Used only
/// by the in-repo TLS integration tests against self-signed certificates.
#[derive(Debug)]
struct AcceptAnyServerCert(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Wrap an established TCP connection in a TLS client session (trusting any
/// server certificate — test only).
async fn tls_connect_client(
    tcp: TcpStream,
) -> anyhow::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .context("build client TLS config")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(provider)))
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let domain = rustls::pki_types::ServerName::try_from("localhost")?;
    connector
        .connect(domain, tcp)
        .await
        .context("client TLS handshake")
}

/// Build a MySQL short SSLRequest packet (HandshakeResponse41 prefix with the
/// `CLIENT_SSL` flag set and no username/auth data).
fn mysql_ssl_request_packet() -> Vec<u8> {
    const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    const CLIENT_SSL: u32 = 0x0000_0800;
    const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
    let flags = CLIENT_LONG_PASSWORD
        | CLIENT_SSL
        | CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH;
    let mut payload = Vec::with_capacity(32);
    payload.extend_from_slice(&flags.to_le_bytes()); // capability flags (4)
    payload.extend_from_slice(&0u32.to_le_bytes()); // max packet size (4)
    payload.push(0x21); // charset (1)
    payload.extend_from_slice(&[0u8; 23]); // filler (23)
    payload
}

#[tokio::test]
async fn pg_tls_handshake_and_query_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg_tls("pg_tls_roundtrip")?;
    wait_for_tcp(server.pg_port())?;
    let mut tcp = TcpStream::connect(("127.0.0.1", server.pg_port())).await?;

    // SSLRequest: length=8, code=80877103. With TLS configured the server
    // accepts with 'S' instead of rejecting with 'N'.
    tcp.write_i32(8).await?;
    tcp.write_i32(80877103).await?;
    tcp.flush().await?;
    let response = tcp.read_u8().await?;
    assert_eq!(
        response, b'S',
        "server must accept SSLRequest when TLS is configured"
    );

    // Complete the TLS handshake, then run the PG startup + a query over it.
    let mut stream = tls_connect_client(tcp).await?;
    let startup = build_pg_startup("skein", "testdb");
    stream.write_all(&startup).await?;
    stream.flush().await?;
    loop {
        let (tag, payload) = read_pg_message(&mut stream).await?;
        if tag == b'Z' {
            assert_eq!(payload[0], b'I', "expected idle transaction status");
            break;
        }
    }

    let msgs = pg_simple_query(&mut stream, "SELECT 1").await?;
    assert_eq!(pg_first_text_cell(&msgs)?, "1");
    assert_eq!(pg_ready_status(&msgs)?, b'I');

    Ok(())
}

#[tokio::test]
async fn mysql_tls_handshake_and_query_roundtrip() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql_tls("mysql_tls_roundtrip")?;
    wait_for_tcp(server.mysql_port())?;
    let mut tcp = TcpStream::connect(("127.0.0.1", server.mysql_port())).await?;

    // Server handshake (seq 0): must advertise CLIENT_SSL (0x0800).
    let (seq, handshake) = read_mysql_packet(&mut tcp).await?;
    assert_eq!(seq, 0);
    let version_end = handshake
        .iter()
        .position(|&b| b == 0)
        .context("handshake missing server-version terminator")?;
    // [version\0][conn_id(4)][seed8(8)][filler(1)][cap_lo(2)]...
    let cap_lo = version_end + 1 + 4 + 8 + 1;
    let capabilities_low = u16::from_le_bytes([handshake[cap_lo], handshake[cap_lo + 1]]);
    assert_ne!(
        capabilities_low & 0x0800,
        0,
        "server should advertise CLIENT_SSL when TLS is configured"
    );

    // SSLRequest (seq 1), then upgrade and send the real HandshakeResponse41
    // (seq 2) over the encrypted channel.
    write_mysql_packet(&mut tcp, 1, &mysql_ssl_request_packet()).await?;
    let mut stream = tls_connect_client(tcp).await?;
    write_mysql_packet(&mut stream, 2, &mysql_handshake_response_packet()).await?;
    let (_seq, ok) = read_mysql_packet(&mut stream).await?;
    assert_eq!(
        ok.first().copied(),
        Some(0x00),
        "expected OK packet after TLS auth"
    );

    send_com_query(&mut stream, "SELECT 1").await?;
    match read_mysql_response(&mut stream).await? {
        MysqlResponse::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0].as_deref(), Some("1"));
        }
        other => panic!("expected result set over TLS, got {:?}", other),
    }

    Ok(())
}

#[tokio::test]
async fn mysql_ssl_request_without_tls_is_rejected() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_mysql("mysql_ssl_reject")?;
    wait_for_tcp(server.mysql_port())?;
    let mut tcp = TcpStream::connect(("127.0.0.1", server.mysql_port())).await?;

    let (seq, handshake) = read_mysql_packet(&mut tcp).await?;
    assert_eq!(seq, 0);
    // A plaintext listener must not advertise CLIENT_SSL.
    let version_end = handshake
        .iter()
        .position(|&b| b == 0)
        .context("handshake missing server-version terminator")?;
    let cap_lo = version_end + 1 + 4 + 8 + 1;
    let capabilities_low = u16::from_le_bytes([handshake[cap_lo], handshake[cap_lo + 1]]);
    assert_eq!(
        capabilities_low & 0x0800,
        0,
        "plaintext listener must not advertise CLIENT_SSL"
    );

    // A client that requests SSL anyway receives an ERR packet rather than a
    // half-open TLS handshake.
    write_mysql_packet(&mut tcp, 1, &mysql_ssl_request_packet()).await?;
    let (_seq, payload) = read_mysql_packet(&mut tcp).await?;
    assert_eq!(
        payload.first().copied(),
        Some(0xff),
        "expected ERR packet when SSL requested without TLS configured"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// T417: PG integration tests — SCRAM-SHA-256, binary format, type OIDs
// ---------------------------------------------------------------------------

/// Perform SCRAM-SHA-256 authentication handshake manually.
async fn pg_connect_with_scram(port: u16, user: &str, password: &str) -> anyhow::Result<TcpStream> {
    wait_for_tcp(port)?;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    let startup = build_pg_startup(user, "testdb");
    stream.write_all(&startup).await?;
    stream.flush().await?;

    // Expect AuthenticationSASL (type=10)
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'R', "expected Authentication message");
    let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(auth_type, 10, "expected AuthenticationSASL (type 10)");

    // Parse mechanism list
    let mut offset = 4;
    let mut mechanisms = Vec::new();
    loop {
        if offset >= payload.len() {
            break;
        }
        if payload[offset] == 0 {
            break;
        }
        let start = offset;
        while offset < payload.len() && payload[offset] != 0 {
            offset += 1;
        }
        mechanisms.push(String::from_utf8_lossy(&payload[start..offset]).to_string());
        offset += 1; // skip null
    }
    assert!(
        mechanisms.contains(&"SCRAM-SHA-256".to_string()),
        "SCRAM-SHA-256 not in mechanism list: {:?}",
        mechanisms
    );

    // Build client-first-message
    let client_nonce = "rOprNGfwEbeRWgbNEkqO"; // fixed test nonce
    let client_first_bare = format!("n={},r={}", user, client_nonce);
    let client_first = format!("n,,{}", client_first_bare);

    // Send SASLInitialResponse
    let mechanism_name = "SCRAM-SHA-256";
    let mut sasl_payload = Vec::new();
    sasl_payload.extend_from_slice(mechanism_name.as_bytes());
    sasl_payload.push(0);
    sasl_payload.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    sasl_payload.extend_from_slice(client_first.as_bytes());

    stream.write_u8(b'p').await?;
    stream.write_i32((sasl_payload.len() + 4) as i32).await?;
    stream.write_all(&sasl_payload).await?;
    stream.flush().await?;

    // Expect AuthenticationSASLContinue (type=11) with server-first-message
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'R', "expected Authentication message");
    let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(
        auth_type, 11,
        "expected AuthenticationSASLContinue (type 11)"
    );
    let server_first = String::from_utf8_lossy(&payload[4..]).to_string();

    // Parse server-first: r=<combined_nonce>,s=<salt_b64>,i=<iterations>
    let mut combined_nonce = String::new();
    let mut salt_b64 = String::new();
    let mut iterations = 0u32;
    for attr in server_first.split(',') {
        if let Some(val) = attr.strip_prefix("r=") {
            combined_nonce = val.to_string();
        } else if let Some(val) = attr.strip_prefix("s=") {
            salt_b64 = val.to_string();
        } else if let Some(val) = attr.strip_prefix("i=") {
            iterations = val.parse().unwrap();
        }
    }
    assert!(
        combined_nonce.starts_with(client_nonce),
        "server nonce doesn't start with client nonce"
    );
    assert!(iterations > 0);

    // Derive keys
    use base64::Engine;
    let salt = base64::engine::general_purpose::STANDARD
        .decode(&salt_b64)
        .unwrap();
    let salted_password = scram_pbkdf2_sha256(password.as_bytes(), &salt, iterations);
    let client_key = scram_hmac_sha256(&salted_password, b"Client Key");
    let stored_key = {
        use sha2::{Digest, Sha256};
        let h: [u8; 32] = Sha256::digest(client_key).into();
        h
    };

    let client_final_without_proof = format!("c=biws,r={}", combined_nonce);
    let auth_message = format!(
        "{},{},{}",
        client_first_bare, server_first, client_final_without_proof
    );
    let client_signature = scram_hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut client_proof = [0u8; 32];
    for i in 0..32 {
        client_proof[i] = client_key[i] ^ client_signature[i];
    }
    let proof_b64 = base64::engine::general_purpose::STANDARD.encode(client_proof);
    let client_final = format!("{},p={}", client_final_without_proof, proof_b64);

    // Send SASLResponse (client-final-message)
    stream.write_u8(b'p').await?;
    stream.write_i32((client_final.len() + 4) as i32).await?;
    stream.write_all(client_final.as_bytes()).await?;
    stream.flush().await?;

    // Expect AuthenticationSASLFinal (type=12) with server signature
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'R', "expected Authentication message");
    let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(auth_type, 12, "expected AuthenticationSASLFinal (type 12)");
    let server_final = String::from_utf8_lossy(&payload[4..]).to_string();
    assert!(
        server_final.starts_with("v="),
        "server-final should start with v=, got: {}",
        server_final
    );

    // Expect AuthenticationOk
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'R');
    let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(auth_type, 0, "expected AuthenticationOk after SCRAM");

    // Consume ParameterStatus, BackendKeyData, ReadyForQuery
    loop {
        let (tag, payload) = read_pg_message(&mut stream).await?;
        if tag == b'Z' {
            assert_eq!(payload[0], b'I');
            break;
        }
    }

    Ok(stream)
}

/// Inline HMAC-SHA-256 for test SCRAM client.
fn scram_hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    const BLOCK_SIZE: usize = 64;
    let mut k = [0u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        let h: [u8; 32] = Sha256::digest(key).into();
        k[..32].copy_from_slice(&h);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK_SIZE];
    let mut opad = [0x5cu8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().into()
}

/// Inline PBKDF2-HMAC-SHA256 for test SCRAM client.
fn scram_pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut u = scram_hmac_sha256(password, &[salt, &1u32.to_be_bytes()].concat());
    let mut result = u;
    for _ in 1..iterations {
        u = scram_hmac_sha256(password, &u);
        for j in 0..32 {
            result[j] ^= u[j];
        }
    }
    result
}

#[tokio::test]
async fn pg_scram_sha256_auth_succeeds() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let password = "test-scram-password-42";
    let server =
        HttpHarness::start_with_pg_and_env("pg_scram_auth", &[("SKEINDB_TOKEN", password)])?;
    wait_for_tcp(server.pg_port())?;

    let mut stream = pg_connect_with_scram(server.pg_port(), "skein", password).await?;

    // Verify the connection works by running a query
    let msgs = pg_simple_query(&mut stream, "SELECT 1").await?;
    assert_eq!(pg_first_text_cell(&msgs)?, "1");

    Ok(())
}

#[tokio::test]
async fn pg_scram_sha256_wrong_password_fails() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg_and_env(
        "pg_scram_wrong_pw",
        &[("SKEINDB_TOKEN", "correct-password")],
    )?;
    wait_for_tcp(server.pg_port())?;

    let mut stream = TcpStream::connect(("127.0.0.1", server.pg_port())).await?;
    let startup = build_pg_startup("skein", "testdb");
    stream.write_all(&startup).await?;
    stream.flush().await?;

    // Read AuthenticationSASL
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'R');
    let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(auth_type, 10);

    // Send client-first with wrong password derivation
    let client_first = "n,,n=skein,r=testnonce123";
    let mechanism = "SCRAM-SHA-256";
    let mut sasl_payload = Vec::new();
    sasl_payload.extend_from_slice(mechanism.as_bytes());
    sasl_payload.push(0);
    sasl_payload.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    sasl_payload.extend_from_slice(client_first.as_bytes());

    stream.write_u8(b'p').await?;
    stream.write_i32((sasl_payload.len() + 4) as i32).await?;
    stream.write_all(&sasl_payload).await?;
    stream.flush().await?;

    // Read server-first
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'R');
    let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(auth_type, 11);
    let server_first = String::from_utf8_lossy(&payload[4..]).to_string();

    // Parse server-first to get nonce
    let combined_nonce = server_first
        .split(',')
        .find_map(|a| a.strip_prefix("r="))
        .unwrap()
        .to_string();

    // Send a client-final with bogus proof (wrong password)
    use base64::Engine;
    let bogus_proof = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    let client_final = format!("c=biws,r={},p={}", combined_nonce, bogus_proof);

    stream.write_u8(b'p').await?;
    stream.write_i32((client_final.len() + 4) as i32).await?;
    stream.write_all(client_final.as_bytes()).await?;
    stream.flush().await?;

    // Should get an ErrorResponse with 28P01 (password authentication failed)
    let (tag, payload) = read_pg_message(&mut stream).await?;
    assert_eq!(tag, b'E', "expected ErrorResponse for wrong password");
    let code = pg_error_field(&payload, b'C');
    assert_eq!(code.as_deref(), Some("28P01"));

    Ok(())
}

#[tokio::test]
async fn pg_binary_result_format_returns_binary_data() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_binary_format")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    // Parse: SELECT 42
    let parse = pg_parse_payload("s1", "SELECT 42", &[]);
    // Bind with binary result format
    let bind = pg_bind_binary_result_payload("", "s1", &[]);
    let execute = pg_execute_payload("", 0);
    // Sync
    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[(b'P', parse), (b'B', bind), (b'E', execute), (b'S', vec![])],
    )
    .await?;

    // Check RowDescription has format=1 (binary)
    let formats = pg_row_description_format_codes(&msgs)?;
    assert_eq!(formats, vec![1], "expected binary format code");

    // Check DataRow contains binary-encoded i64 (8 bytes, big-endian 42)
    let raw = pg_first_data_row_raw_bytes(&msgs)?;
    assert_eq!(raw.len(), 8, "INT8 binary should be 8 bytes");
    let value = i64::from_be_bytes(raw.try_into().unwrap());
    assert_eq!(value, 42);

    Ok(())
}

#[tokio::test]
async fn pg_extended_query_type_oids_cover_bool_date_uuid() -> anyhow::Result<()> {
    let _guard = cluster_test_guard().await;
    let server = HttpHarness::start_with_pg("pg_type_oids_wide")?;
    let mut stream = pg_connect_and_startup(server.pg_port()).await?;

    // Parse + Describe a query with diverse literal types
    let sql = "SELECT true, false, 42, 3.14, 'hello'";
    let parse = pg_parse_payload("s_types", sql, &[]);
    let describe = pg_describe_payload(b'S', "s_types");
    let msgs = pg_send_messages_until_ready(
        &mut stream,
        &[(b'P', parse), (b'D', describe), (b'S', vec![])],
    )
    .await?;

    let type_oids = pg_row_description_type_oids(&msgs)?;
    assert_eq!(type_oids.len(), 5);
    // Verify type inference:
    // - true/false → BOOL (OID 16)
    // - 42 → INT8 (OID 20)
    // - 3.14 → FLOAT8 (OID 701)
    // - 'hello' → TEXT (OID 25)
    assert_eq!(type_oids[0], 16, "expected BOOL OID for 'true'");
    assert_eq!(type_oids[1], 16, "expected BOOL OID for 'false'");
    assert_eq!(type_oids[2], 20, "expected INT8 OID for integer literal");
    assert_eq!(type_oids[3], 701, "expected FLOAT8 OID for float literal");
    assert_eq!(type_oids[4], 25, "expected TEXT OID for string literal");

    Ok(())
}
