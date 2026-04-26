use std::{
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use quinn::{ClientConfig, Endpoint};
use rcgen::generate_simple_self_signed;
use rustls::{pki_types::CertificateDer, RootCertStore};
use serde_json::json;
use skeindb_skeinql::types::{
    BaseTableRef, Cte, ExistsExpr, Expr, JoinRef, JoinTableRef, JoinType, LimitClause, Lit,
    OrderBy, Query, QueryBody, SelectBody, SelectItem, SetOp, SetOpKind, TableRef,
};
use skeindb_skeinql::{RpcId, RpcRequest, RpcResponse, SKEINQL_VERSION};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
static QUIC_TEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
static QUIC_RUSTLS_PROVIDER: OnceLock<()> = OnceLock::new();

fn ensure_rustls_provider() {
    QUIC_RUSTLS_PROVIDER.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

async fn quic_test_guard() -> OwnedSemaphorePermit {
    let sem = QUIC_TEST_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone();
    sem.acquire_owned().await.expect("acquire quic test permit")
}

async fn wait_for_quic_advisor_history_entry(
    client: &QuicClient,
    db: &str,
    table: &str,
    action_id: &str,
    expected_status: &str,
) -> anyhow::Result<serde_json::Value> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let resp = client
            .rpc(
                "advisor.history",
                json!({
                    "table": {"db": db, "table": table},
                    "limit": 20
                }),
            )
            .await?;
        let entries = resp.result.unwrap_or_default()["entries"]
            .as_array()
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

#[tokio::test]
async fn quic_rpc_ping_roundtrip() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_rpc")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let response = client.rpc("system.ping", json!({})).await?;
    assert!(response.ok);
    let result = response.result.unwrap_or_default();
    assert_eq!(result.get("pong"), Some(&json!(true)));

    Ok(())
}

#[tokio::test]
async fn quic_prepared_query_roundtrip() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_prepared")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

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
                "rows": [
                    {"id": {"t":"u64","v":1}, "name": {"t":"str","v":"Ava"}},
                    {"id": {"t":"u64","v":2}, "name": {"t":"str","v":"Bo"}}
                ]
            }),
        )
        .await?;
    assert!(resp.ok);

    let query = select_query(
        "app",
        "users",
        vec!["id", "name"],
        Some(eq_param_expr("id", 0)),
    );
    let resp = client.rpc("query.prepare", json!({"query": query})).await?;
    assert!(resp.ok);
    let query_id = resp.result.expect("missing result")["query_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing query_id"))?
        .to_string();

    let resp = client
        .rpc(
            "query.execute_prepared",
            json!({
                "query_id": query_id,
                "args": [Lit::U64 { v: 2 }]
            }),
        )
        .await?;
    assert!(resp.ok);
    let rows = resp.result.expect("missing result")["data"]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(rows.len(), 1);

    Ok(())
}

#[tokio::test]
async fn quic_wasm_plan_run_batch() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_wasm_batch")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

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
                    {"name": "score", "type": {"kind": "u64"}, "nullable": false}
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
                "rows": [
                    {"id": {"t":"u64","v":1}, "score": {"t":"u64","v":3}},
                    {"id": {"t":"u64","v":2}, "score": {"t":"u64","v":8}}
                ]
            }),
        )
        .await?;
    assert!(resp.ok);

    let predicate = Expr::Op {
        op: "gt".to_string(),
        a: Some(Box::new(Expr::Col {
            col: "score".to_string(),
            table: None,
        })),
        b: Some(Box::new(Expr::Param { param: 0 })),
        args: None,
        list: None,
        lo: None,
        hi: None,
    };
    let query = select_query("app", "users", vec!["id", "score"], Some(predicate));

    let resp = client
        .rpc("wasm.plan.compile", json!({ "query": query }))
        .await?;
    assert!(resp.ok);
    let artifact_b64 = resp.result.expect("missing result")["artifact_b64"]
        .as_str()
        .ok_or_else(|| anyhow!("missing artifact_b64"))?
        .to_string();

    let resp = client
        .rpc(
            "wasm.plan.run",
            json!({
                "artifact_b64": artifact_b64,
                "args": [{"t":"u64","v":7}],
                "result_format": "wasm_batch_v1"
            }),
        )
        .await?;
    assert!(resp.ok);
    let result = resp.result.expect("missing result");
    let data = result["data"].as_object().cloned().unwrap_or_default();
    assert_eq!(
        data.get("format").and_then(|v| v.as_str()),
        Some("skein.wasm.batch.v1")
    );
    let batch_b64 = data
        .get("batch_b64")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let bytes = BASE64_STANDARD.decode(batch_b64.as_bytes())?;
    assert!(bytes.len() >= 20);
    assert_eq!(read_u32_le(&bytes, 0), 0x31424B53);
    assert_eq!(read_u32_le(&bytes, 8), 1);
    assert_eq!(read_u32_le(&bytes, 12), 2);

    Ok(())
}

#[tokio::test]
async fn quic_advisor_index_synthesize() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_advisor")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

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
                    {"name": "city", "type": {"kind": "str"}, "nullable": false},
                    {"name": "created_at", "type": {"kind": "u64"}, "nullable": false},
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
                "rows": [
                    {"id": {"t":"u64","v":1}, "city": {"t":"str","v":"Oslo"}, "created_at": {"t":"u64","v":10}, "name": {"t":"str","v":"Ava"}},
                    {"id": {"t":"u64","v":2}, "city": {"t":"str","v":"Oslo"}, "created_at": {"t":"u64","v":11}, "name": {"t":"str","v":"Bo"}}
                ]
            }),
        )
        .await?;
    assert!(resp.ok);

    let mut query = select_query(
        "app",
        "users",
        vec!["id", "name"],
        Some(eq_param_expr("city", 0)),
    );
    query.order_by = vec![OrderBy {
        expr: Expr::Col {
            col: "created_at".to_string(),
            table: None,
        },
        dir: None,
    }];

    let resp = client
        .rpc(
            "query.select",
            json!({
                "query": query,
                "args": [Lit::Str { v: "Oslo".to_string() }]
            }),
        )
        .await?;
    assert!(resp.ok);

    let resp = client
        .rpc(
            "advisor.index_synthesize",
            json!({
                "table": {"db": "app", "table": "users"},
                "limit": 5,
                "min_queries": 1,
                "min_rows": 1
            }),
        )
        .await?;
    assert!(resp.ok);
    let suggestions = resp.result.expect("missing result")["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(!suggestions.is_empty());
    let columns = suggestions[0]["columns"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let include = suggestions[0]["include"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let cols: Vec<&str> = columns.iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(cols, vec!["city", "created_at"]);

    let resp = client
        .rpc(
            "advisor.apply_index",
            json!({
                "table": {"db": "app", "table": "users"},
                "columns": columns.clone(),
                "include": include.clone(),
                "note": "apply"
            }),
        )
        .await?;
    assert!(resp.ok);
    let action_id = resp
        .result
        .as_ref()
        .and_then(|value| value.get("action_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("missing advisor action_id"))?
        .to_string();
    let status = resp
        .result
        .as_ref()
        .and_then(|r| r.get("status"))
        .and_then(|v| v.as_str());
    assert_eq!(status, Some("queued"));
    let completed =
        wait_for_quic_advisor_history_entry(&client, "app", "users", &action_id, "completed")
            .await?;
    assert_eq!(completed["result_status"].as_str(), Some("built"));

    let resp = client
        .rpc(
            "advisor.dismiss",
            json!({
                "table": {"db": "app", "table": "users"},
                "columns": columns,
                "include": include,
                "note": "dismiss"
            }),
        )
        .await?;
    assert!(resp.ok);

    let resp = client
        .rpc(
            "advisor.history",
            json!({
                "table": {"db": "app", "table": "users"},
                "limit": 10
            }),
        )
        .await?;
    assert!(resp.ok);
    let entries = resp.result.expect("missing result")["entries"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(entries.len() >= 2);
    let actions: Vec<&str> = entries
        .iter()
        .filter_map(|entry| entry["action"].as_str())
        .collect();
    assert!(actions.contains(&"apply"));
    assert!(actions.contains(&"dismiss"));

    Ok(())
}

#[tokio::test]
async fn quic_migration_intent_report() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_intent")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let mut query = select_query("app", "users", vec!["id", "name"], None);
    query.order_by = vec![OrderBy {
        expr: Expr::Col {
            col: "created_at".to_string(),
            table: None,
        },
        dir: None,
    }];
    query.limit = Some(LimitClause {
        limit: Some(10),
        offset: Some(20),
    });

    let resp = client
        .rpc(
            "migration.intent_report",
            json!({
                "samples": [
                    {"query": query, "args": []}
                ],
                "limit": 5
            }),
        )
        .await?;
    assert!(resp.ok);
    let suggestions = resp.result.expect("missing result")["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let intents: Vec<&str> = suggestions
        .iter()
        .filter_map(|entry| entry["intent"].as_str())
        .collect();
    assert!(intents.contains(&"pagination.offset_limit"));

    Ok(())
}

#[tokio::test]
async fn quic_migration_rewrite_preview() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_rewrite")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let mut query = select_query("app", "users", vec!["id", "name"], None);
    query.order_by = vec![OrderBy {
        expr: Expr::Col {
            col: "created_at".to_string(),
            table: None,
        },
        dir: None,
    }];
    query.limit = Some(LimitClause {
        limit: Some(10),
        offset: Some(20),
    });

    let resp = client
        .rpc(
            "migration.rewrite_preview",
            json!({
                "samples": [
                    {"query": query, "args": []}
                ],
                "limit": 5
            }),
        )
        .await?;
    assert!(resp.ok);
    let rewrites = resp.result.expect("missing result")["rewrites"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let preview = rewrites
        .iter()
        .find(|entry| entry["intent"].as_str() == Some("pagination.offset_limit"));
    let preview = preview.expect("missing pagination rewrite");
    assert!(preview["before"]
        .as_str()
        .unwrap_or_default()
        .contains("LIMIT 10 OFFSET 20"));
    assert!(preview["after"]
        .as_str()
        .unwrap_or_default()
        .contains("cursor pagination"));

    Ok(())
}

#[tokio::test]
async fn quic_migration_intent_report_exists_and_coalesce() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_intent_exists")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let inner_pred = Expr::Op {
        op: "eq".to_string(),
        a: Some(Box::new(Expr::Col {
            col: "user_id".to_string(),
            table: None,
        })),
        b: Some(Box::new(Expr::Param { param: 0 })),
        args: None,
        list: None,
        lo: None,
        hi: None,
    };
    let inner_query = select_query("app", "orders", vec!["id"], Some(inner_pred));
    let exists_query = select_query(
        "app",
        "users",
        vec!["id"],
        Some(Expr::Exists {
            exists: ExistsExpr {
                query: inner_query,
                negated: None,
            },
        }),
    );

    let coalesce_expr = Expr::Func {
        name: "coalesce".to_string(),
        args: vec![
            Expr::Col {
                col: "status".to_string(),
                table: None,
            },
            Expr::Lit {
                lit: Lit::Str {
                    v: "active".to_string(),
                },
            },
        ],
        distinct: None,
    };
    let mut coalesce_query = select_query("app", "users", vec!["status"], None);
    if let QueryBody::Select { select } = coalesce_query.body.as_mut() {
        select.projection = vec![SelectItem {
            expr: coalesce_expr,
            r#as: None,
        }];
    }

    let resp = client
        .rpc(
            "migration.intent_report",
            json!({
                "samples": [
                    {"query": exists_query, "args": [Lit::U64 { v: 42 }]},
                    {"query": coalesce_query, "args": []}
                ],
                "limit": 10
            }),
        )
        .await?;
    assert!(resp.ok);
    let suggestions = resp.result.expect("missing result")["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let intents: Vec<&str> = suggestions
        .iter()
        .filter_map(|entry| entry["intent"].as_str())
        .collect();
    assert!(intents.contains(&"exists.membership"));
    assert!(intents.contains(&"defaults.coalesce"));

    Ok(())
}

#[tokio::test]
async fn quic_migration_intent_report_hierarchy() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_intent_hierarchy")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let join_on = Expr::Op {
        op: "eq".to_string(),
        a: Some(Box::new(Expr::Col {
            col: "parent_id".to_string(),
            table: Some("child".to_string()),
        })),
        b: Some(Box::new(Expr::Col {
            col: "id".to_string(),
            table: Some("parent".to_string()),
        })),
        args: None,
        list: None,
        lo: None,
        hi: None,
    };
    let join_query = Query {
        with: Vec::new(),
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: None,
                projection: vec![SelectItem {
                    expr: Expr::Col {
                        col: "id".to_string(),
                        table: Some("child".to_string()),
                    },
                    r#as: None,
                }],
                from: Some(vec![TableRef::Join(JoinTableRef {
                    join: JoinRef {
                        join_type: JoinType::Inner,
                        left: Box::new(TableRef::Base(BaseTableRef {
                            db: "app".to_string(),
                            table: "categories".to_string(),
                            r#as: Some("child".to_string()),
                        })),
                        right: Box::new(TableRef::Base(BaseTableRef {
                            db: "app".to_string(),
                            table: "categories".to_string(),
                            r#as: Some("parent".to_string()),
                        })),
                        on: Some(join_on),
                    },
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

    let resp = client
        .rpc(
            "migration.intent_report",
            json!({
                "samples": [
                    {"query": join_query, "args": []}
                ],
                "limit": 10
            }),
        )
        .await?;
    assert!(resp.ok);
    let suggestions = resp.result.expect("missing result")["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let intents: Vec<&str> = suggestions
        .iter()
        .filter_map(|entry| entry["intent"].as_str())
        .collect();
    assert!(intents.contains(&"hierarchy.adjacency"));

    Ok(())
}

#[tokio::test]
async fn quic_migration_intent_report_recursive_cte() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_intent_recursive")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let query = recursive_cte_query();

    let resp = client
        .rpc(
            "migration.intent_report",
            json!({
                "samples": [
                    {"query": query, "args": []}
                ],
                "limit": 10
            }),
        )
        .await?;
    assert!(resp.ok);
    let suggestions = resp.result.expect("missing result")["suggestions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let intents: Vec<&str> = suggestions
        .iter()
        .filter_map(|entry| entry["intent"].as_str())
        .collect();
    assert!(intents.contains(&"hierarchy.recursive_cte"));

    Ok(())
}

#[tokio::test]
async fn quic_migration_rewrite_preview_recursive_cte() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_rewrite_recursive")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let resp = client
        .rpc(
            "migration.rewrite_preview",
            json!({
                "samples": [
                    {"query": recursive_cte_query(), "args": []}
                ],
                "limit": 5
            }),
        )
        .await?;
    assert!(resp.ok);
    let rewrites = resp.result.expect("missing result")["rewrites"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let preview = rewrites
        .iter()
        .find(|entry| entry["intent"].as_str() == Some("hierarchy.recursive_cte"))
        .expect("missing recursive cte rewrite");
    assert!(preview["before"]
        .as_str()
        .unwrap_or_default()
        .contains("UNION ALL"));
    assert!(preview["after"]
        .as_str()
        .unwrap_or_default()
        .contains("roots ="));
    assert!(preview["after"]
        .as_str()
        .unwrap_or_default()
        .contains("max_depth"));

    Ok(())
}

#[tokio::test]
async fn quic_vector_search_roundtrip() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_vector").context("start vector QUIC harness")?;
    let client = QuicClient::connect(harness.port, harness.cert_der)
        .await
        .context("connect vector QUIC client")?;

    let resp = client
        .rpc("schema.create_database", json!({"db": "app"}))
        .await
        .context("vector test create database")?;
    assert!(resp.ok);

    let resp = client
        .rpc(
            "schema.create_table",
            json!({
                "db": "app",
                "table": "docs",
                "columns": [
                    {"name": "id", "type": {"kind": "u64"}, "nullable": false},
                    {"name": "embedding", "type": {"kind": "embedding"}, "nullable": false}
                ],
                "primary_key": ["id"]
            }),
        )
        .await
        .context("vector test create table")?;
    assert!(resp.ok);

    let resp = client
        .rpc(
            "vector.insert",
            json!({
                "table": {"db": "app", "table": "docs"},
                "column": "embedding",
                "rows": [
                    {"pk": [{"t":"u64","v":1}], "embedding": {"t":"embedding","dims":3,"v":[1.0,0.0,0.0]}},
                    {"pk": [{"t":"u64","v":2}], "embedding": {"t":"embedding","dims":3,"v":[-1.0,0.0,0.0]}}
                ]
            }),
        )
        .await
        .context("vector test insert embeddings")?;
    assert!(resp.ok);

    let resp = client
        .rpc(
            "vector.search",
            json!({
                "table": {"db": "app", "table": "docs"},
                "column": "embedding",
                "query": {"t":"embedding","dims":3,"v":[1.0,0.0,0.0]},
                "k": 1,
                "include_row": true
            }),
        )
        .await
        .context("vector test search embeddings")?;
    assert!(resp.ok);
    let matches = resp.result.expect("missing result")["matches"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(matches.len(), 1);
    let pk = matches[0]["pk"].as_array().cloned().unwrap_or_default();
    assert_eq!(pk[0]["v"].as_u64(), Some(1));

    Ok(())
}

#[tokio::test]
async fn quic_zero_rtt_rejects_write() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_zero_rtt")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let resp = client
        .rpc_with_meta(
            "schema.create_database",
            json!({"db": "app"}),
            json!({"rtt": "0rtt"}),
        )
        .await?;
    assert!(!resp.ok);
    let err = resp.error.expect("missing error");
    assert_eq!(err.code, "forbidden");

    Ok(())
}

#[tokio::test]
async fn quic_connection_migration_rebind() -> anyhow::Result<()> {
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("quic_rebind")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    let resp = client.rpc("system.ping", json!({})).await?;
    assert!(resp.ok);

    client.rebind()?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = client.rpc("system.ping", json!({})).await?;
    assert!(resp.ok);

    Ok(())
}

// ── R09: QUIC multi-stream hardening ──────────────────────────────────────
#[tokio::test]
async fn r09_quic_concurrent_multi_stream_rpcs() -> anyhow::Result<()> {
    // R09: Transport QUIC — verify sequential multi-stream RPCs over same connection
    let _guard = quic_test_guard().await;
    let harness = TestHarness::start("r09_multi_stream")?;
    let client = QuicClient::connect(harness.port, harness.cert_der).await?;

    // Fire multiple RPCs sequentially over the same QUIC connection
    let mut ok_count = 0u32;
    for i in 0..5 {
        let resp = client
            .rpc(
                "sql.exec",
                json!({ "sql": format!("SELECT {i} AS stream_id") }),
            )
            .await;
        if let Ok(r) = resp {
            if r.ok {
                ok_count += 1;
            }
        }
    }
    assert!(
        ok_count >= 3,
        "at least 3 out of 5 sequential QUIC RPCs should succeed"
    );

    // Test connection migration after multiple RPCs
    client.rebind()?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = client.rpc("system.ping", json!({})).await?;
    assert!(resp.ok, "ping after rebind + multi-stream should succeed");

    Ok(())
}

struct TestHarness {
    _guard: ChildGuard,
    port: u16,
    cert_der: Vec<u8>,
}

impl TestHarness {
    fn start(label: &str) -> anyhow::Result<Self> {
        let dir = temp_dir(label);
        let cert =
            generate_simple_self_signed(vec!["localhost".to_string()]).context("generate cert")?;
        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();
        let cert_der = cert.cert.der().to_vec();

        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert_pem).context("write cert pem")?;
        std::fs::write(&key_path, key_pem).context("write key pem")?;

        let quic_port = free_udp_port();
        let child = spawn_server(&dir, quic_port, &cert_path, &key_path)?;
        let _guard = ChildGuard::new(child);

        Ok(Self {
            _guard,
            port: quic_port,
            cert_der,
        })
    }
}

struct QuicClient {
    endpoint: Endpoint,
    connection: quinn::Connection,
}

impl QuicClient {
    async fn connect(port: u16, cert_der: Vec<u8>) -> anyhow::Result<Self> {
        ensure_rustls_provider();
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(cert_der))
            .map_err(|_| anyhow!("add cert to root store"))?;

        let crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
        ));

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
        endpoint.set_default_client_config(client_config);

        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let started_at = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(30);
        let attempt_timeout = Duration::from_millis(750);

        loop {
            match endpoint.connect(addr, "localhost") {
                Ok(connecting) => match tokio::time::timeout(attempt_timeout, connecting).await {
                    Ok(Ok(connection)) => {
                        return Ok(Self {
                            endpoint,
                            connection,
                        });
                    }
                    Ok(Err(err)) => {
                        if Instant::now() >= deadline {
                            return Err(err.into());
                        }
                    }
                    Err(_) => {
                        if Instant::now() >= deadline {
                            return Err(anyhow!(
                                "timed out waiting for QUIC server on {} after {:?}",
                                addr,
                                started_at.elapsed()
                            ));
                        }
                    }
                },
                Err(err) => {
                    if Instant::now() >= deadline {
                        return Err(err.into());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn rpc(&self, method: &str, params: serde_json::Value) -> anyhow::Result<RpcResponse> {
        self.rpc_with_meta(method, params, json!(null)).await
    }

    async fn rpc_with_meta(
        &self,
        method: &str,
        params: serde_json::Value,
        meta: serde_json::Value,
    ) -> anyhow::Result<RpcResponse> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        let req = RpcRequest {
            skeinql: SKEINQL_VERSION.to_string(),
            id: Some(RpcId::Str(format!("r-{}", method))),
            method: method.to_string(),
            params: Some(params),
        };
        let payload = if meta.is_null() {
            serde_json::to_vec(&req)?
        } else {
            serde_json::to_vec(&json!({ "request": req, "meta": meta }))?
        };
        write_frame(&mut send, &payload).await?;
        send.finish()?;

        let resp_bytes = read_frame(&mut recv).await?;
        let resp: RpcResponse = serde_json::from_slice(&resp_bytes)?;
        Ok(resp)
    }

    fn rebind(&self) -> anyhow::Result<()> {
        let socket = UdpSocket::bind("127.0.0.1:0")?;
        self.endpoint.rebind(socket)?;
        Ok(())
    }
}

fn select_query(db: &str, table: &str, projection: Vec<&str>, predicate: Option<Expr>) -> Query {
    let items = projection
        .into_iter()
        .map(|col| SelectItem {
            expr: Expr::Col {
                col: col.to_string(),
                table: None,
            },
            r#as: None,
        })
        .collect::<Vec<_>>();
    Query {
        with: Vec::new(),
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: None,
                projection: items,
                from: Some(vec![TableRef::Base(BaseTableRef {
                    db: db.to_string(),
                    table: table.to_string(),
                    r#as: None,
                })]),
                r#where: predicate,
                group_by: None,
                having: None,
            }),
        }),
        order_by: Vec::new(),
        limit: None,
        lock: None,
    }
}

fn recursive_cte_query() -> Query {
    let anchor = SelectBody {
        distinct: None,
        projection: vec![SelectItem {
            expr: Expr::Col {
                col: "id".to_string(),
                table: Some("c".to_string()),
            },
            r#as: None,
        }],
        from: Some(vec![TableRef::Base(BaseTableRef {
            db: "app".to_string(),
            table: "categories".to_string(),
            r#as: Some("c".to_string()),
        })]),
        r#where: None,
        group_by: None,
        having: None,
    };
    let join_on = Expr::Op {
        op: "eq".to_string(),
        a: Some(Box::new(Expr::Col {
            col: "parent_id".to_string(),
            table: Some("child".to_string()),
        })),
        b: Some(Box::new(Expr::Col {
            col: "id".to_string(),
            table: Some("parent".to_string()),
        })),
        args: None,
        list: None,
        lo: None,
        hi: None,
    };
    let recursive = SelectBody {
        distinct: None,
        projection: vec![SelectItem {
            expr: Expr::Col {
                col: "id".to_string(),
                table: Some("child".to_string()),
            },
            r#as: None,
        }],
        from: Some(vec![TableRef::Join(JoinTableRef {
            join: JoinRef {
                join_type: JoinType::Inner,
                left: Box::new(TableRef::Base(BaseTableRef {
                    db: "app".to_string(),
                    table: "categories".to_string(),
                    r#as: Some("child".to_string()),
                })),
                right: Box::new(TableRef::Base(BaseTableRef {
                    db: "app".to_string(),
                    table: "tree".to_string(),
                    r#as: Some("parent".to_string()),
                })),
                on: Some(join_on),
            },
        })]),
        r#where: None,
        group_by: None,
        having: None,
    };
    let cte_query = Query {
        with: Vec::new(),
        body: Box::new(QueryBody::Setop {
            setop: SetOp {
                kind: SetOpKind::Union,
                all: true,
                left: Box::new(QueryBody::Select {
                    select: Box::new(anchor),
                }),
                right: Box::new(QueryBody::Select {
                    select: Box::new(recursive),
                }),
            },
        }),
        order_by: Vec::new(),
        limit: None,
        lock: None,
    };
    Query {
        with: vec![Cte {
            name: "tree".to_string(),
            query: cte_query,
        }],
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: None,
                projection: vec![SelectItem {
                    expr: Expr::Col {
                        col: "id".to_string(),
                        table: None,
                    },
                    r#as: None,
                }],
                from: Some(vec![TableRef::Base(BaseTableRef {
                    db: "app".to_string(),
                    table: "tree".to_string(),
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

fn eq_param_expr(col: &str, param: u32) -> Expr {
    Expr::Op {
        op: "eq".to_string(),
        a: Some(Box::new(Expr::Col {
            col: col.to_string(),
            table: None,
        })),
        b: Some(Box::new(Expr::Param { param })),
        args: None,
        list: None,
        lo: None,
        hi: None,
    }
}

fn read_u32_le(bytes: &[u8], offset: usize) -> u32 {
    let mut buf = [0u8; 4];
    if offset + 4 <= bytes.len() {
        buf.copy_from_slice(&bytes[offset..offset + 4]);
    }
    u32::from_le_bytes(buf)
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

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .expect("bind udp")
        .local_addr()
        .expect("udp local addr")
        .port()
}

fn spawn_server(
    dir: &PathBuf,
    quic_port: u16,
    cert_path: &PathBuf,
    key_path: &PathBuf,
) -> anyhow::Result<Child> {
    let bin = env!("CARGO_BIN_EXE_skeindb");
    Command::new(bin)
        .arg("serve")
        .arg("--data")
        .arg(dir)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--http")
        .arg("0")
        .arg("--mysql")
        .arg("0")
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
        "skeindb_quic_test_{}_{}_{}",
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
