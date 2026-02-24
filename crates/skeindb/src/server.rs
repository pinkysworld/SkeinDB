use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use axum::{
    extract::Path,
    extract::State,
    http::{header, HeaderMap, Method, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sysinfo::{Pid, System};

use skeindb_skeinql::{
    methods::{
        AdvisorHistoryParams, AdvisorIndexApplyParams, AdvisorIndexDismissParams,
        AdvisorIndexSynthesizeParams, AiAutoparamAnalyzeParams, AiAutoparamClassifyParams,
        AiNlExecuteParams, AiNlExplainParams, AiNlTranslateParams, CdcPollParams,
        CdcSubscribeTableParams, ClusterJoinTokenCreateParams, ClusterNodeJoinParams,
        ClusterNodeRemoveParams, ClusterNodesParams, ClusterReplicaPromoteParams,
        ClusterShardCreateParams, ClusterShardMoveParams, ClusterShardRebalanceParams,
        DataDeleteParams, DataGetParams, DataInsertParams, DataUpdateParams, DpAggregateParams,
        DpAuditLogParams, DpBudgetGetParams, DpBudgetSetParams, EdgeBundleApplyParams,
        EdgeBundleRequestParams, EdgeBundleStatusParams, ForensicExportParams, ForensicQueryParams,
        ForensicVerifyParams, MergeApplyParams, MergeRegisterParams, MergeSimulateParams,
        MergeWasmDropParams, MergeWasmRegisterParams, MigrationIntentReportParams,
        MigrationRewritePreviewParams, ObliviousExplainParams, ObliviousPolicyGetParams,
        ObliviousPolicySetParams, QueryExecutePreparedParams, QueryPatchParams, QueryPrepareParams,
        SchemaApplyMergeParams, SchemaColumnInfo, SchemaMergeStatusParams,
        SchemaProposeChangeParams, VectorIndexStatusParams, VectorInsertParams, VectorSearchParams,
        ViewCreateParams, ViewDropParams, ViewExplainDepsParams, ViewRefreshParams,
        ViewStatusParams, WasmPlanCompileParams, WasmPlanRunParams,
    },
    types::{
        BaseTableRef, Expr, LimitClause, Lit, OrderBy, OrderDir, Query, QueryBody, QueryCache,
        ResultFormat, SelectBody, SelectItem, TableRef, TypeDesc, WireHints,
    },
    RpcError, RpcRequest, RpcResponse, SKEINQL_VERSION,
};

use crate::engine::{ColumnSchema, Engine, Subscriptions};
use crate::quic;

use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};

const REPLICATION_HEADER: &str = "x-skeindb-replication";
const CLUSTER_STATE_KEY: &str = "cluster.state.v1";
const CLUSTER_DEFAULT_JOIN_TTL_MS: u64 = 10 * 60 * 1000;
static CLUSTER_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);

const ADMIN_INDEX_HTML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/skeinadmin/index.html"
));
const ADMIN_MAIN_JS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/skeinadmin/src/main.js"
));

fn admin_index_html() -> &'static str {
    ADMIN_INDEX_HTML
}

fn admin_main_js() -> &'static str {
    ADMIN_MAIN_JS
}

#[derive(Debug, Clone)]
pub struct ServeOpts {
    pub data_dir: String,
    pub bind: String,
    pub mysql_port: u16,
    pub http_port: u16,
    pub cluster_port: u16,
    pub quic_port: Option<u16>,
    pub quic_cert: Option<PathBuf>,
    pub quic_key: Option<PathBuf>,
}

#[derive(Clone, Copy)]
struct TransportCapabilities {
    http: bool,
    quic: bool,
}

#[derive(Clone)]
pub(crate) struct AppState {
    started: Instant,
    data_dir: PathBuf,
    local_rpc_url: String,
    settings: Arc<Mutex<serde_json::Map<String, Value>>>,
    cluster: Arc<Mutex<ClusterStateModel>>,
    counters: Arc<Mutex<Counters>>,

    engine: Arc<RwLock<Engine>>,
    subs: Arc<Mutex<Subscriptions>>,
    coalesce: Arc<QueryCoalescer>,
    transport: TransportCapabilities,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterNode {
    node_id: String,
    rpc_url: String,
    role: String,
    status: String,
    joined_at_ms: u64,
    last_seen_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterJoinToken {
    token: String,
    role: String,
    expires_at_ms: u64,
    max_uses: u32,
    used: u32,
    created_at_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterShard {
    shard_id: String,
    db: String,
    table: Option<String>,
    primary_node_id: String,
    replicas: Vec<String>,
    slots: u32,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterReplicationState {
    shipped_ops: u64,
    applied_ops: u64,
    failed_ops: u64,
    last_error: Option<String>,
    last_updated_ms: u64,
}

impl Default for ClusterReplicationState {
    fn default() -> Self {
        Self {
            shipped_ops: 0,
            applied_ops: 0,
            failed_ops: 0,
            last_error: None,
            last_updated_ms: now_unix_ms_u64(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClusterStateModel {
    enabled: bool,
    cluster_id: String,
    local_node_id: String,
    primary_node_id: String,
    nodes: Vec<ClusterNode>,
    join_tokens: Vec<ClusterJoinToken>,
    shards: Vec<ClusterShard>,
    replication: ClusterReplicationState,
}

impl ClusterStateModel {
    fn bootstrap(local_node_id: String, local_rpc_url: String) -> Self {
        let ts = now_unix_ms_u64();
        let cluster_id = format!("cluster-{}", ts);
        let local = ClusterNode {
            node_id: local_node_id.clone(),
            rpc_url: local_rpc_url,
            role: "primary".to_string(),
            status: "online".to_string(),
            joined_at_ms: ts,
            last_seen_ms: ts,
        };
        Self {
            enabled: false,
            cluster_id,
            local_node_id: local_node_id.clone(),
            primary_node_id: local_node_id,
            nodes: vec![local],
            join_tokens: Vec::new(),
            shards: Vec::new(),
            replication: ClusterReplicationState::default(),
        }
    }

    fn local_role(&self) -> String {
        self.nodes
            .iter()
            .find(|n| n.node_id == self.local_node_id)
            .map(|n| n.role.clone())
            .unwrap_or_else(|| {
                if self.local_node_id == self.primary_node_id {
                    "primary".to_string()
                } else {
                    "replica".to_string()
                }
            })
    }

    fn primary_rpc_url(&self) -> Option<String> {
        self.nodes
            .iter()
            .find(|n| n.node_id == self.primary_node_id)
            .map(|n| n.rpc_url.clone())
    }

    fn cleanup_join_tokens(&mut self, now_ms: u64) {
        self.join_tokens.retain(|t| t.expires_at_ms > now_ms);
    }

    fn nodes_for_replication(&self, db: Option<&str>, table: Option<&str>) -> Vec<ClusterNode> {
        let shard_match = match (db, table) {
            (Some(db), Some(table)) => self
                .shards
                .iter()
                .find(|s| s.db == db && s.table.as_deref() == Some(table)),
            _ => None,
        };

        let mut node_ids: HashSet<String> = HashSet::new();
        if let Some(shard) = shard_match {
            for id in shard.replicas.iter() {
                node_ids.insert(id.clone());
            }
        } else {
            for node in self.nodes.iter() {
                if node.role == "replica" && node.status == "online" {
                    node_ids.insert(node.node_id.clone());
                }
            }
        }

        self.nodes
            .iter()
            .filter(|node| {
                node_ids.contains(&node.node_id)
                    && node.node_id != self.local_node_id
                    && node.status == "online"
            })
            .cloned()
            .collect()
    }

    fn shard_primary_for(&self, db: Option<&str>, table: Option<&str>) -> String {
        if let (Some(db), Some(table)) = (db, table) {
            if let Some(shard) = self
                .shards
                .iter()
                .find(|s| s.db == db && s.table.as_deref() == Some(table))
            {
                return shard.primary_node_id.clone();
            }
        }
        self.primary_node_id.clone()
    }
}

#[derive(Default)]
struct Counters {
    total_rpc: u64,
    per_method: HashMap<String, u64>,
}

#[derive(Default)]
struct QueryCoalescer {
    inflight: Mutex<HashMap<String, Arc<InFlightQuery>>>,
}

struct InFlightQuery {
    notify: tokio::sync::Notify,
    result: Mutex<Option<Result<crate::engine::QuerySelectResult, String>>>,
}

impl InFlightQuery {
    fn new() -> Self {
        Self {
            notify: tokio::sync::Notify::new(),
            result: Mutex::new(None),
        }
    }
}

impl QueryCoalescer {
    /// Returns (inflight, is_leader)
    fn get_or_create(&self, key: &str) -> (Arc<InFlightQuery>, bool) {
        let mut map = self.inflight.lock().unwrap();
        if let Some(existing) = map.get(key) {
            return (existing.clone(), false);
        }
        let q = Arc::new(InFlightQuery::new());
        map.insert(key.to_string(), q.clone());
        (q, true)
    }

    fn finish(&self, key: &str) {
        let mut map = self.inflight.lock().unwrap();
        map.remove(key);
    }
}

pub async fn serve(opts: ServeOpts) -> anyhow::Result<()> {
    let http_addr: SocketAddr = format!("{}:{}", opts.bind, opts.http_port).parse()?;

    // Ensure data dir exists.
    let data_dir = PathBuf::from(&opts.data_dir);
    std::fs::create_dir_all(&data_dir)?;

    let engine = Engine::open(&data_dir)?;
    let advertised_host = if opts.bind == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        opts.bind.clone()
    };
    let local_rpc_url = format!("http://{}:{}", advertised_host, opts.http_port);
    let local_node_id = format!(
        "node-{}-{}",
        advertised_host.replace('.', "-"),
        opts.cluster_port
    );
    let transport = TransportCapabilities {
        http: true,
        quic: opts.quic_port.is_some(),
    };

    let state = AppState {
        started: Instant::now(),
        data_dir,
        local_rpc_url: local_rpc_url.clone(),
        settings: Arc::new(Mutex::new(serde_json::Map::new())),
        cluster: Arc::new(Mutex::new(ClusterStateModel::bootstrap(
            local_node_id,
            local_rpc_url,
        ))),
        counters: Arc::new(Mutex::new(Counters::default())),
        engine: Arc::new(RwLock::new(engine)),
        subs: Arc::new(Mutex::new(Subscriptions::default())),
        coalesce: Arc::new(QueryCoalescer::default()),
        transport,
    };

    // Load persisted settings if present.
    load_settings(&state).ok();
    load_cluster_state(&state).ok();

    let app_state = state.clone();
    let app = Router::new()
        .route("/api/v1/rpc", post(rpc_handler))
        .route("/api/v1/q/:query_id", get(prepared_get_handler))
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/console", get(console_handler))
        .route("/console/", get(console_handler))
        .route("/console/src/main.js", get(console_main_js_handler))
        .route("/admin", get(admin_handler))
        .route("/admin/", get(admin_handler))
        .route("/admin/src/main.js", get(admin_main_js_handler))
        .route("/src/main.js", get(admin_main_js_handler))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers(Any),
        );

    let quic_handle = if let Some(quic_port) = opts.quic_port {
        let cert_path = opts
            .quic_cert
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--quic-cert is required when --quic is set"))?;
        let key_path = opts
            .quic_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--quic-key is required when --quic is set"))?;
        let quic_opts = quic::QuicServeOpts {
            bind: opts.bind.clone(),
            port: quic_port,
            cert_path,
            key_path,
        };
        let state = app_state.clone();
        Some(tokio::spawn(async move {
            if let Err(err) = quic::serve_quic(state, quic_opts).await {
                tracing::error!(?err, "QUIC server failed");
            }
        }))
    } else {
        None
    };

    tracing::info!(
        bind = %opts.bind,
        http_port = %opts.http_port,
        mysql_port = %opts.mysql_port,
        cluster_port = %opts.cluster_port,
        "SkeinDB server starting"
    );
    tracing::warn!(
        "MySQL protocol is still a placeholder in this scaffold. SkeinQL HTTP is live at /api/v1/rpc"
    );

    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!(%http_addr, "HTTP listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    if let Some(handle) = quic_handle {
        handle.abort();
    }

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn console_handler() -> impl IntoResponse {
    Html(admin_index_html())
}

async fn admin_handler() -> impl IntoResponse {
    Html(admin_index_html())
}

async fn admin_main_js_handler() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        admin_main_js(),
    )
}

async fn console_main_js_handler() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        admin_main_js(),
    )
}

/// Execute a prepared query via a cacheable HTTP GET.
///
/// - Uses standard HTTP validators (ETag + If-None-Match).
/// - Returns `304 Not Modified` when unchanged.
///
/// Notes:
/// - This prototype does not accept query args in the GET path.
///   Use `query.select` over RPC for parameterized queries.
async fn prepared_get_handler(
    Path(query_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> axum::response::Response {
    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok());

    let query = {
        let eng = state.engine.read().await;
        let Some(pq) = eng.get_prepared(&query_id) else {
            return (StatusCode::NOT_FOUND, "unknown query_id").into_response();
        };
        pq.query.clone()
    };

    // If the client already has an ETag, do the cheap validation path directly.
    if let Some(inm) = if_none_match {
        let eng = state.engine.read().await;
        let res = eng.query_select(
            &query,
            &[],
            ResultFormat::RowsJson,
            true,
            Some(inm),
            None,
            None,
            false,
        );
        return match res {
            Ok(r) => {
                if r.not_modified {
                    let mut resp = StatusCode::NOT_MODIFIED.into_response();
                    if let Some(etag) = r.etag.as_ref() {
                        if let Ok(v) = etag.parse() {
                            resp.headers_mut().insert(header::ETAG, v);
                        }
                    }
                    resp
                } else {
                    let etag = r.etag.clone();
                    let mut resp = (StatusCode::OK, Json(r)).into_response();
                    resp.headers_mut().insert(
                        header::CACHE_CONTROL,
                        header::HeaderValue::from_static("private, max-age=0"),
                    );
                    // Set ETag from result.
                    if let Some(etag) = etag.as_ref() {
                        if let Ok(v) = etag.parse() {
                            resp.headers_mut().insert(header::ETAG, v);
                        }
                    }
                    resp
                }
            }
            Err(e) => {
                let err = to_rpc_error(e);
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"ok": false, "error": err})),
                )
                    .into_response()
            }
        };
    }

    // Coalesce unconditional GETs (no If-None-Match) by query_id to avoid the
    // thundering herd on first-load.
    let (inflight, is_leader) = state.coalesce.get_or_create(&query_id);

    let res: Result<crate::engine::QuerySelectResult, String> = if is_leader {
        let eng = state.engine.read().await;
        let out = eng
            .query_select(
                &query,
                &[],
                ResultFormat::RowsJson,
                true,
                None,
                None,
                None,
                false,
            )
            .map_err(|e| e.to_string());
        *inflight.result.lock().unwrap() = Some(out.clone());
        inflight.notify.notify_waiters();
        state.coalesce.finish(&query_id);
        out
    } else {
        // Joiner: wait for leader (or return immediately if already completed).
        loop {
            if let Some(r) = inflight.result.lock().unwrap().as_ref() {
                break r.clone();
            }
            inflight.notify.notified().await;
        }
    };

    match res {
        Ok(r) => {
            if r.not_modified {
                let mut resp = StatusCode::NOT_MODIFIED.into_response();
                if let Some(etag) = r.etag.as_ref() {
                    if let Ok(v) = etag.parse() {
                        resp.headers_mut().insert(header::ETAG, v);
                    }
                }
                return resp;
            }

            let etag = r.etag.clone();
            let mut resp = (StatusCode::OK, Json(r)).into_response();
            resp.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("private, max-age=0"),
            );
            // r.etag is available; use it.
            if let Some(etag) = etag.as_ref() {
                if let Ok(v) = etag.parse() {
                    resp.headers_mut().insert(header::ETAG, v);
                }
            }
            resp
        }
        Err(msg) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": {"code":"internal","message": msg}})),
        )
            .into_response(),
    }
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let uptime = state.started.elapsed().as_secs_f64();
    let advisor = state.engine.read().await.advisor_metrics_snapshot();
    let counters = state.counters.lock().unwrap();

    let mut body = String::new();
    body.push_str("# HELP skeindb_uptime_seconds Process uptime in seconds\n");
    body.push_str("# TYPE skeindb_uptime_seconds gauge\n");
    body.push_str(&format!("skeindb_uptime_seconds {}\n", uptime));

    body.push_str("# HELP skeindb_rpc_total Total RPC calls\n");
    body.push_str("# TYPE skeindb_rpc_total counter\n");
    body.push_str(&format!("skeindb_rpc_total {}\n", counters.total_rpc));

    body.push_str("# HELP skeindb_rpc_method_total Total RPC calls by method\n");
    body.push_str("# TYPE skeindb_rpc_method_total counter\n");
    for (m, c) in counters.per_method.iter() {
        body.push_str(&format!(
            "skeindb_rpc_method_total{{method=\"{}\"}} {}\n",
            escape_label(m),
            c
        ));
    }

    body.push_str("# HELP skeindb_advisor_suggestions_total Index advisor suggestions generated\n");
    body.push_str("# TYPE skeindb_advisor_suggestions_total counter\n");
    body.push_str(&format!(
        "skeindb_advisor_suggestions_total {}\n",
        advisor.suggestions_total
    ));

    body.push_str("# HELP skeindb_advisor_applied_total Index advisor apply actions\n");
    body.push_str("# TYPE skeindb_advisor_applied_total counter\n");
    body.push_str(&format!(
        "skeindb_advisor_applied_total {}\n",
        advisor.applied_total
    ));

    body.push_str("# HELP skeindb_advisor_rejected_total Index advisor dismiss actions\n");
    body.push_str("# TYPE skeindb_advisor_rejected_total counter\n");
    body.push_str(&format!(
        "skeindb_advisor_rejected_total {}\n",
        advisor.rejected_total
    ));

    body.push_str(
        "# HELP skeindb_advisor_estimated_saved_ms_total Advisor estimated saved milliseconds\n",
    );
    body.push_str("# TYPE skeindb_advisor_estimated_saved_ms_total counter\n");
    body.push_str(&format!(
        "skeindb_advisor_estimated_saved_ms_total {}\n",
        advisor.estimated_saved_ms_total
    ));

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[derive(Clone, Copy, Default)]
pub(crate) struct RpcPolicy {
    pub(crate) read_only: bool,
}

pub(crate) struct RpcOutcome {
    pub(crate) status: StatusCode,
    pub(crate) response: Option<RpcResponse>,
}

impl RpcOutcome {
    fn into_response(self) -> axum::response::Response {
        match self.response {
            Some(resp) => (self.status, Json(resp)).into_response(),
            None => self.status.into_response(),
        }
    }
}

pub(crate) async fn handle_rpc(
    state: &AppState,
    headers: Option<&HeaderMap>,
    req: RpcRequest,
    policy: RpcPolicy,
) -> RpcOutcome {
    // Bump counters
    {
        let mut c = state.counters.lock().unwrap();
        c.total_rpc += 1;
        *c.per_method.entry(req.method.clone()).or_insert(0) += 1;
    }

    // Very small auth placeholder: if SKEINDB_TOKEN is set, require matching bearer.
    if let Ok(expected) = std::env::var("SKEINDB_TOKEN") {
        let auth_ok = headers
            .and_then(|map| map.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()))
            .map(|v| v == format!("Bearer {}", expected))
            .unwrap_or(false);
        if !auth_ok {
            let resp: RpcResponse = RpcResponse::err(
                req.id.clone(),
                RpcError::new("unauthorized", "Missing/invalid bearer token"),
            );
            return RpcOutcome {
                status: StatusCode::UNAUTHORIZED,
                response: Some(resp),
            };
        }
    }

    // Version check
    if req.skeinql != SKEINQL_VERSION {
        let resp: RpcResponse = RpcResponse::err(
            req.id.clone(),
            RpcError::new(
                "unsupported_version",
                format!(
                    "Unsupported skeinql version '{}'. This server supports '{}'",
                    req.skeinql, SKEINQL_VERSION
                ),
            ),
        );
        return RpcOutcome {
            status: StatusCode::OK,
            response: Some(resp),
        };
    }

    let method = req.method.clone();
    let params = req.params.clone();
    let sql_read_only = method.as_str() == "sql.exec" && sql_exec_is_read_only(params.as_ref());
    let is_replication_request = headers
        .and_then(|map| map.get(REPLICATION_HEADER).and_then(|v| v.to_str().ok()))
        .map(|v| v == "1")
        .unwrap_or(false);
    if policy.read_only && !is_read_only_method(&method) && !sql_read_only {
        if req.id.is_none() {
            return RpcOutcome {
                status: StatusCode::NO_CONTENT,
                response: None,
            };
        }
        let resp: RpcResponse = RpcResponse::err(
            req.id.clone(),
            RpcError::new("forbidden", "read-only requests cannot perform writes"),
        );
        return RpcOutcome {
            status: StatusCode::OK,
            response: Some(resp),
        };
    }
    if let Err(err) =
        enforce_cluster_write_guard(state, &method, params.as_ref(), is_replication_request)
    {
        if req.id.is_none() {
            return RpcOutcome {
                status: StatusCode::NO_CONTENT,
                response: None,
            };
        }
        let resp: RpcResponse = RpcResponse::err(req.id.clone(), err);
        return RpcOutcome {
            status: StatusCode::OK,
            response: Some(resp),
        };
    }

    let result: Result<Value, RpcError> =
        (async {
            match method.as_str() {
                "system.ping" => Ok(serde_json::json!({
                    "pong": true,
                    "time_unix_ms": now_unix_ms(),
                })),
                "system.version" => Ok(serde_json::json!({
                    "name": "skeindb",
                    "version": env!("CARGO_PKG_VERSION"),
                    "skeinql": SKEINQL_VERSION,
                })),
                "system.capabilities" => Ok(system_capabilities(state)),
                "transport.capabilities" => Ok(transport_capabilities(state)),
                "stats.snapshot" => Ok(stats_snapshot(state).await),
                "settings.get" => handle_settings_get(state, params.clone()),
                "settings.set" => handle_settings_set(state, params.clone()),
                // --------------------
                // cluster.*
                // --------------------
                "cluster.status" => cluster_status(state),
                "cluster.nodes" => {
                    let p = if params.is_some() {
                        Some(parse_params::<ClusterNodesParams>(params.clone())?)
                    } else {
                        None
                    };
                    cluster_nodes(state, p)
                }
                "cluster.join_token.create" => {
                    let p = if params.is_some() {
                        parse_params::<ClusterJoinTokenCreateParams>(params.clone())?
                    } else {
                        ClusterJoinTokenCreateParams {
                            ttl_ms: None,
                            role: None,
                            max_uses: None,
                        }
                    };
                    cluster_join_token_create(state, p)
                }
                "cluster.node.join" => {
                    let p: ClusterNodeJoinParams = parse_params(params.clone())?;
                    cluster_node_join(state, p)
                }
                "cluster.node.remove" => {
                    let p: ClusterNodeRemoveParams = parse_params(params.clone())?;
                    cluster_node_remove(state, p)
                }
                "cluster.replica.promote" => {
                    let p: ClusterReplicaPromoteParams = parse_params(params.clone())?;
                    cluster_replica_promote(state, p)
                }
                "cluster.shard.create" => {
                    let p: ClusterShardCreateParams = parse_params(params.clone())?;
                    cluster_shard_create(state, p)
                }
                "cluster.shard.move" => {
                    let p: ClusterShardMoveParams = parse_params(params.clone())?;
                    cluster_shard_move(state, p)
                }
                "cluster.shard.rebalance" => {
                    let p = if params.is_some() {
                        parse_params::<ClusterShardRebalanceParams>(params.clone())?
                    } else {
                        ClusterShardRebalanceParams {
                            max_moves: None,
                            dry_run: None,
                        }
                    };
                    cluster_shard_rebalance(state, p)
                }
                // --------------------
                // schema.*
                // --------------------
                "schema.list_databases" => {
                    let eng = state.engine.read().await;
                    Ok(serde_json::json!({"databases": eng.list_databases()}))
                }
                "schema.create_database" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        db: String,
                    }
                    let p: P = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    eng.create_database(&p.db).map_err(to_rpc_error)?;
                    Ok(serde_json::json!({"ok": true}))
                }
                "schema.list_tables" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        db: String,
                    }
                    let p: P = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let tables = eng.list_tables(&p.db).map_err(to_rpc_error)?;
                    Ok(serde_json::json!({"tables": tables}))
                }
                "schema.create_table" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        db: String,
                        table: String,
                        columns: Vec<SchemaColumnInfo>,
                        #[serde(default)]
                        primary_key: Vec<String>,
                        #[serde(default)]
                        if_not_exists: bool,
                        #[serde(default)]
                        compat_mysql: Option<Value>,
                    }
                    let p: P = parse_params(params.clone())?;
                    let cols: Vec<ColumnSchema> = p
                        .columns
                        .iter()
                        .map(|c| ColumnSchema {
                            name: c.name.clone(),
                            r#type: c.r#type.clone(),
                            nullable: c.nullable,
                            auto_increment: c.auto_increment,
                        })
                        .collect();
                    let mut eng = state.engine.write().await;
                    eng.create_table(
                        &p.db,
                        &p.table,
                        cols,
                        p.primary_key,
                        p.if_not_exists,
                        p.compat_mysql,
                    )
                    .map_err(to_rpc_error)?;
                    Ok(serde_json::json!({"ok": true}))
                }
                "schema.describe_table" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        db: String,
                        table: String,
                    }
                    let p: P = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let v = eng.describe_table(&p.db, &p.table).map_err(to_rpc_error)?;
                    Ok(v)
                }
                "schema.propose_change" => {
                    let p: SchemaProposeChangeParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.schema_propose_change(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "schema.merge_status" => {
                    let p: SchemaMergeStatusParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.schema_merge_status(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "schema.apply_merge" => {
                    let p: SchemaApplyMergeParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.schema_apply_merge(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "advisor.index_synthesize" => {
                    let p: AdvisorIndexSynthesizeParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.advisor_index_synthesize(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "advisor.apply_index" => {
                    let p: AdvisorIndexApplyParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.advisor_index_apply(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "advisor.dismiss" => {
                    let p: AdvisorIndexDismissParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.advisor_index_dismiss(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "advisor.history" => {
                    let p: AdvisorHistoryParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.advisor_history(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "migration.intent_report" => {
                    let p: MigrationIntentReportParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.migration_intent_report(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "migration.rewrite_preview" => {
                    let p: MigrationRewritePreviewParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.migration_rewrite_preview(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // data.*
                // --------------------
                "data.get" => {
                    let p: DataGetParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.data_get(&p.table, p.pk).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "data.insert" => {
                    let p: DataInsertParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng
                        .data_insert(&p.into, p.rows, p.returning)
                        .map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "data.update" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        #[serde(flatten)]
                        inner: DataUpdateParams,
                        #[serde(default)]
                        args: Vec<Lit>,
                    }
                    let p: P = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng
                        .data_update(
                            &p.inner.table,
                            &p.inner.r#where,
                            &p.inner.set,
                            p.inner.limit,
                            p.inner.if_match.as_deref(),
                            &p.args,
                        )
                        .map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "data.delete" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        #[serde(flatten)]
                        inner: DataDeleteParams,
                        #[serde(default)]
                        args: Vec<Lit>,
                    }
                    let p: P = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng
                        .data_delete(&p.inner.table, &p.inner.r#where, p.inner.limit, &p.args)
                        .map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // query.*
                // --------------------
                "query.prepare" => {
                    let p: QueryPrepareParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let pq = eng.query_prepare(p.query).map_err(to_rpc_error)?;
                    Ok(serde_json::json!({"query_id": pq.id, "canonical": pq.canonical_json}))
                }
                "query.execute_prepared" => {
                    let p: QueryExecutePreparedParams = parse_params(params.clone())?;

                    let query = {
                        let eng = state.engine.read().await;
                        let Some(pq) = eng.get_prepared(&p.query_id) else {
                            return Err(RpcError::new("not_found", "unknown query_id"));
                        };
                        pq.query.clone()
                    };

                    let mut known: HashSet<String> = HashSet::new();
                    let mut use_skeinpack = false;

                    if let Some(rf) = p.result_format.as_ref() {
                        if matches!(rf, ResultFormat::SkeinpackV1) {
                            use_skeinpack = true;
                        }
                    }

                    if let Some(w) = p.wire.as_ref() {
                        if let Some(fmt) = w.format.as_deref() {
                            if fmt == "skeinpack_v1" {
                                use_skeinpack = true;
                            }
                        }
                        if let Some(kv) = w.known_valueids.as_ref() {
                            if let Some(arr) = kv.as_array() {
                                for s in arr.iter().filter_map(|v| v.as_str()) {
                                    known.insert(s.to_string());
                                }
                            }
                        }
                    }

                    let eng = state.engine.read().await;
                    let r = eng
                        .query_select(
                            &query,
                            &p.args,
                            p.result_format.unwrap_or(ResultFormat::RowsJson),
                            true,
                            p.if_none_match.as_deref(),
                            p.min_causality.as_ref(),
                            if known.is_empty() { None } else { Some(&known) },
                            use_skeinpack,
                        )
                        .map_err(to_rpc_error)?;

                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "query.select" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        query: Query,
                        #[serde(default)]
                        args: Vec<Lit>,
                        #[serde(default)]
                        result_format: Option<ResultFormat>,
                        #[serde(default)]
                        cache: Option<QueryCache>,
                        #[serde(default)]
                        wire: Option<WireHints>,
                    }
                    let p: P = parse_params(params.clone())?;
                    let want_etag = p.cache.as_ref().and_then(|c| c.want_etag).unwrap_or(true);
                    let if_none_match = p.cache.as_ref().and_then(|c| c.if_none_match.as_deref());
                    let min_causality = p.cache.as_ref().and_then(|c| c.min_causality.as_ref());

                    let mut known: HashSet<String> = HashSet::new();
                    let mut use_skeinpack = false;
                    if let Some(rf) = p.result_format.as_ref() {
                        if matches!(rf, ResultFormat::SkeinpackV1) {
                            use_skeinpack = true;
                        }
                    }
                    if let Some(w) = p.wire.as_ref() {
                        if let Some(fmt) = w.format.as_deref() {
                            if fmt == "skeinpack_v1" {
                                use_skeinpack = true;
                            }
                        }
                        if let Some(kv) = w.known_valueids.as_ref() {
                            if let Some(arr) = kv.as_array() {
                                for s in arr.iter().filter_map(|v| v.as_str()) {
                                    known.insert(s.to_string());
                                }
                            }
                        }
                    }

                    let eng = state.engine.read().await;
                    let r = eng
                        .query_select(
                            &p.query,
                            &p.args,
                            p.result_format.unwrap_or(ResultFormat::RowsJson),
                            want_etag,
                            if_none_match,
                            min_causality,
                            if known.is_empty() { None } else { Some(&known) },
                            use_skeinpack,
                        )
                        .map_err(to_rpc_error)?;

                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "query.patch" => {
                    let p: QueryPatchParams = parse_params(params.clone())?;

                    let mut known: HashSet<String> = HashSet::new();
                    let mut use_skeinpack = false;

                    if let Some(rf) = p.result_format.as_ref() {
                        if matches!(rf, ResultFormat::SkeinpackV1) {
                            use_skeinpack = true;
                        }
                    }

                    if let Some(w) = p.wire.as_ref() {
                        if let Some(fmt) = w.format.as_deref() {
                            if fmt == "skeinpack_v1" {
                                use_skeinpack = true;
                            }
                        }
                        if let Some(kv) = w.known_valueids.as_ref() {
                            if let Some(arr) = kv.as_array() {
                                for s in arr.iter().filter_map(|v| v.as_str()) {
                                    known.insert(s.to_string());
                                }
                            }
                        }
                    }

                    let include_full = p.include_full.unwrap_or(true);
                    let fmt = p.result_format.unwrap_or(ResultFormat::RowsJson);

                    // Patch coalescing: for high fan-out read workloads, many clients often ask
                    // for the same (base_etag -> current) patch at the same time.
                    //
                    // We only coalesce strict-mode JSON patches (no per-client dict state).
                    let can_coalesce = p.base_etag.is_some()
                        && p.client_state.is_none()
                        && !(matches!(fmt, ResultFormat::SkeinpackV1) && use_skeinpack);

                    if can_coalesce {
                        let key = format!(
                            "patch:{}:{:?}:{}",
                            p.base_etag.as_deref().unwrap_or(""),
                            fmt,
                            include_full
                        );
                        let (in_flight, is_leader) = state.coalesce.get_or_create(&key);

                        if !is_leader {
                            let res = loop {
                                if let Some(res) = in_flight.result.lock().unwrap().clone() {
                                    break res;
                                }
                                in_flight.notify.notified().await;
                            };
                            let r = res.map_err(|s| RpcError::new("internal", s))?;
                            Ok(serde_json::to_value(r)
                                .map_err(|e| RpcError::new("internal", e.to_string()))?)
                        } else {
                            let res: Result<crate::engine::QuerySelectResult, String> = {
                                let eng = state.engine.read().await;
                                eng.query_patch(
                                    &p.query,
                                    &p.args,
                                    fmt,
                                    p.base_etag.as_deref(),
                                    p.client_state.as_ref(),
                                    if known.is_empty() { None } else { Some(&known) },
                                    use_skeinpack,
                                    include_full,
                                )
                                .map_err(|e| e.to_string())
                            };

                            *in_flight.result.lock().unwrap() = Some(res.clone());
                            in_flight.notify.notify_waiters();
                            state.coalesce.finish(&key);

                            let r = res.map_err(|s| RpcError::new("internal", s))?;
                            Ok(serde_json::to_value(r)
                                .map_err(|e| RpcError::new("internal", e.to_string()))?)
                        }
                    } else {
                        let eng = state.engine.read().await;
                        let r = eng
                            .query_patch(
                                &p.query,
                                &p.args,
                                fmt,
                                p.base_etag.as_deref(),
                                p.client_state.as_ref(),
                                if known.is_empty() { None } else { Some(&known) },
                                use_skeinpack,
                                include_full,
                            )
                            .map_err(to_rpc_error)?;

                        Ok(serde_json::to_value(r)
                            .map_err(|e| RpcError::new("internal", e.to_string()))?)
                    }
                }

                // --------------------
                // vector.* (research)
                // --------------------
                "vector.insert" => {
                    let p: VectorInsertParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.vector_insert(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "vector.search" => {
                    let p: VectorSearchParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.vector_search(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "vector.index.status" => {
                    let p: VectorIndexStatusParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.vector_index_status(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // ai.* (research)
                // --------------------
                "ai.autoparam.classify" => {
                    let p: AiAutoparamClassifyParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.ai_autoparam_classify(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "ai.autoparam.analyze" => {
                    let p: AiAutoparamAnalyzeParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.ai_autoparam_analyze(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "ai.nl.translate" => {
                    let p: AiNlTranslateParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.ai_nl_translate(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "ai.nl.explain" => {
                    let p: AiNlExplainParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.ai_nl_explain(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "ai.nl.execute" => {
                    let p: AiNlExecuteParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.ai_nl_execute(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // dp.* (research)
                // --------------------
                "dp.aggregate" => {
                    let p: DpAggregateParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.dp_aggregate(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "dp.budget.set" => {
                    let p: DpBudgetSetParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.dp_budget_set(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "dp.budget.get" => {
                    let p: DpBudgetGetParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.dp_budget_get(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "dp.audit.log" => {
                    let p: DpAuditLogParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.dp_audit_log(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // oblivious.* (research)
                // --------------------
                "oblivious.policy.set" => {
                    let p: ObliviousPolicySetParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.oblivious_policy_set(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "oblivious.policy.get" => {
                    let p: ObliviousPolicyGetParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.oblivious_policy_get(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "oblivious.explain" => {
                    let p: ObliviousExplainParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.oblivious_explain(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // forensic.* (research)
                // --------------------
                "forensic.query" => {
                    let p: ForensicQueryParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.forensic_query(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "forensic.verify" => {
                    let p: ForensicVerifyParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.forensic_verify(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "forensic.export" => {
                    let p: ForensicExportParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.forensic_export(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // edge.* (research)
                // --------------------
                "edge.bundle.request" => {
                    let p: EdgeBundleRequestParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.edge_bundle_request(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "edge.bundle.apply" => {
                    let p: EdgeBundleApplyParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.edge_bundle_apply(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "edge.bundle.status" => {
                    let p: EdgeBundleStatusParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.edge_bundle_status(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // merge.* (research)
                // --------------------
                "merge.register" => {
                    let p: MergeRegisterParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.merge_register(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "merge.apply" => {
                    let p: MergeApplyParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.merge_apply(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "merge.simulate" => {
                    let p: MergeSimulateParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.merge_simulate(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "merge.wasm.register" => {
                    let p: MergeWasmRegisterParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.merge_wasm_register(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "merge.wasm.list" => {
                    let eng = state.engine.read().await;
                    let r = eng.merge_wasm_list().map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "merge.wasm.drop" => {
                    let p: MergeWasmDropParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.merge_wasm_drop(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "wasm.plan.compile" => {
                    let p: WasmPlanCompileParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.wasm_plan_compile(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "wasm.plan.run" => {
                    let p: WasmPlanRunParams = parse_params(params.clone())?;
                    let want_etag = p.cache.as_ref().and_then(|c| c.want_etag).unwrap_or(true);
                    let if_none_match = p.cache.as_ref().and_then(|c| c.if_none_match.as_deref());
                    let min_causality = p.cache.as_ref().and_then(|c| c.min_causality.as_ref());

                    let mut known: HashSet<String> = HashSet::new();
                    let mut use_skeinpack = false;
                    if let Some(rf) = p.result_format.as_ref() {
                        if matches!(rf, ResultFormat::SkeinpackV1) {
                            use_skeinpack = true;
                        }
                    }
                    if let Some(w) = p.wire.as_ref() {
                        if let Some(fmt) = w.format.as_deref() {
                            if fmt == "skeinpack_v1" {
                                use_skeinpack = true;
                            }
                        }
                        if let Some(kv) = w.known_valueids.as_ref() {
                            if let Some(arr) = kv.as_array() {
                                for s in arr.iter().filter_map(|v| v.as_str()) {
                                    known.insert(s.to_string());
                                }
                            }
                        }
                    }

                    let eng = state.engine.read().await;
                    let r = eng
                        .wasm_plan_run(
                            &p.artifact_b64,
                            &p.args,
                            p.result_format.clone().unwrap_or(ResultFormat::RowsJson),
                            want_etag,
                            if_none_match,
                            min_causality,
                            if known.is_empty() { None } else { Some(&known) },
                            use_skeinpack,
                        )
                        .map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // view.* (research)
                // --------------------
                "view.create" => {
                    let p: ViewCreateParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.view_create(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "view.drop" => {
                    let p: ViewDropParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.view_drop(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "view.refresh" => {
                    let p: ViewRefreshParams = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    let r = eng.view_refresh(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "view.status" => {
                    let p: ViewStatusParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.view_status(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "view.explain_deps" => {
                    let p: ViewExplainDepsParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let r = eng.view_explain_deps(p).map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // cdc.* (polling)
                // --------------------
                "cdc.subscribe_table" => {
                    let p: CdcSubscribeTableParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let mut subs = state.subs.lock().unwrap();
                    let r = eng
                        .cdc_subscribe_table(&mut subs, &p.db, &p.table)
                        .map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }
                "cdc.poll" => {
                    let p: CdcPollParams = parse_params(params.clone())?;
                    let eng = state.engine.read().await;
                    let subs = state.subs.lock().unwrap();
                    let lim = p.limit.unwrap_or(200);
                    let r = eng
                        .cdc_poll(&subs, &p.sub_id, p.from_offset, lim)
                        .map_err(to_rpc_error)?;
                    Ok(serde_json::to_value(r)
                        .map_err(|e| RpcError::new("internal", e.to_string()))?)
                }

                // --------------------
                // sql.* (compatibility helpers)
                // --------------------
                "sql.exec" => {
                    let p: SqlExecParams = parse_params(params.clone())?;
                    sql_exec(state, p).await
                }
                _ => Err(RpcError::new(
                    "not_supported",
                    format!("Method '{}' is not supported in this build", method),
                )),
            }
        })
        .await;

    if result.is_ok()
        && should_replicate_method(&method, params.as_ref())
        && !is_replication_request
    {
        if let Some(params_obj) = params.clone() {
            if let Err(err) = replicate_write_to_cluster(state, &method, params_obj).await {
                tracing::warn!(method = %method, error = %err, "cluster replication fanout failed");
            }
        }
    }

    // Notification: no response body.
    if req.id.is_none() {
        return RpcOutcome {
            status: StatusCode::NO_CONTENT,
            response: None,
        };
    }

    let resp: RpcResponse = match result {
        Ok(v) => RpcResponse::ok(req.id.clone(), v),
        Err(e) => RpcResponse::err(req.id.clone(), e),
    };

    RpcOutcome {
        status: StatusCode::OK,
        response: Some(resp),
    }
}

async fn rpc_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RpcRequest>,
) -> axum::response::Response {
    handle_rpc(&state, Some(&headers), req, RpcPolicy::default())
        .await
        .into_response()
}

fn should_guard_cluster_write(method: &str) -> bool {
    if matches!(method, "cluster.status" | "cluster.nodes") {
        return false;
    }
    method.starts_with("cluster.") || !is_read_only_method(method)
}

fn should_replicate_method(method: &str, params: Option<&Value>) -> bool {
    if method == "sql.exec" {
        return !sql_exec_is_read_only(params);
    }
    matches!(
        method,
        "schema.create_database"
            | "schema.create_table"
            | "schema.apply_merge"
            | "data.insert"
            | "data.update"
            | "data.delete"
            | "vector.insert"
            | "merge.register"
            | "merge.apply"
            | "merge.wasm.register"
            | "merge.wasm.drop"
            | "view.create"
            | "view.drop"
            | "view.refresh"
            | "edge.bundle.apply"
    )
}

fn write_target_from_params(
    method: &str,
    params: Option<&Value>,
) -> (Option<String>, Option<String>) {
    let Some(params) = params else {
        return (None, None);
    };
    let s = |path: &[&str]| -> Option<String> {
        let mut cur = params;
        for part in path {
            cur = cur.get(*part)?;
        }
        cur.as_str().map(|v| v.to_string())
    };
    match method {
        "schema.create_database" => (s(&["db"]), None),
        "schema.create_table" => (s(&["db"]), s(&["table"])),
        "schema.apply_merge" => (s(&["table", "db"]), s(&["table", "table"])),
        "data.insert" => (s(&["into", "db"]), s(&["into", "table"])),
        "data.update" | "data.delete" => (s(&["table", "db"]), s(&["table", "table"])),
        "vector.insert" => (s(&["table", "db"]), s(&["table", "table"])),
        "merge.apply" => (s(&["table", "db"]), s(&["table", "table"])),
        "view.create" | "view.drop" | "view.refresh" => (s(&["view", "db"]), s(&["view", "table"])),
        "sql.exec" => sql_exec_write_target(params),
        _ => (None, None),
    }
}

fn enforce_cluster_write_guard(
    state: &AppState,
    method: &str,
    params: Option<&Value>,
    is_replication_request: bool,
) -> Result<(), RpcError> {
    if is_replication_request || !should_guard_cluster_write(method) {
        return Ok(());
    }
    if method == "sql.exec" && sql_exec_is_read_only(params) {
        return Ok(());
    }
    let (db, table) = write_target_from_params(method, params);
    let cluster = state.cluster.lock().unwrap();
    if !cluster.enabled {
        return Ok(());
    }
    let target_primary = cluster.shard_primary_for(db.as_deref(), table.as_deref());
    if target_primary == cluster.local_node_id {
        return Ok(());
    }
    let primary_url = cluster
        .nodes
        .iter()
        .find(|n| n.node_id == target_primary)
        .map(|n| n.rpc_url.clone())
        .or_else(|| cluster.primary_rpc_url())
        .unwrap_or_else(|| "unknown".to_string());
    Err(RpcError::new(
        "forbidden",
        format!(
            "write routed to primary node '{}' at {} for this shard",
            target_primary, primary_url
        ),
    ))
}

async fn replicate_write_to_cluster(
    state: &AppState,
    method: &str,
    params: Value,
) -> anyhow::Result<()> {
    let now = now_unix_ms_u64();
    let (db, table) = write_target_from_params(method, Some(&params));
    let (targets, enabled) = {
        let mut cluster = state.cluster.lock().unwrap();
        let enabled = cluster.enabled;
        cluster.replication.last_updated_ms = now;
        (
            cluster.nodes_for_replication(db.as_deref(), table.as_deref()),
            enabled,
        )
    };
    if !enabled || targets.is_empty() {
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?;
    let auth_token = std::env::var("SKEINDB_TOKEN").ok();
    let payload = serde_json::json!({
        "skeinql": SKEINQL_VERSION,
        "method": method,
        "params": params,
    });

    let mut shipped = 0u64;
    let mut failed = 0u64;
    let mut last_error: Option<String> = None;
    for node in targets {
        let url = format!("{}/api/v1/rpc", node.rpc_url.trim_end_matches('/'));
        let mut req = client
            .post(&url)
            .header(header::CONTENT_TYPE.as_str(), "application/json")
            .header(REPLICATION_HEADER, "1")
            .json(&payload);
        if let Some(token) = auth_token.as_ref() {
            req = req.bearer_auth(token);
        }
        let res = req.send().await;
        match res {
            Ok(resp) if resp.status().is_success() => {
                shipped += 1;
            }
            Ok(resp) => {
                failed += 1;
                let msg = format!("{} => {}", url, resp.status());
                last_error = Some(msg.clone());
                tracing::warn!(method = %method, peer = %url, status = %resp.status(), "replication request failed");
            }
            Err(err) => {
                failed += 1;
                let msg = format!("{} => {}", url, err);
                last_error = Some(msg.clone());
                tracing::warn!(method = %method, peer = %url, error = %err, "replication transport error");
            }
        }
    }

    {
        let mut cluster = state.cluster.lock().unwrap();
        cluster.replication.shipped_ops = cluster.replication.shipped_ops.saturating_add(shipped);
        cluster.replication.failed_ops = cluster.replication.failed_ops.saturating_add(failed);
        if failed > 0 {
            cluster.replication.last_error = last_error;
        }
        cluster.replication.last_updated_ms = now_unix_ms_u64();
    }
    persist_cluster_state(state).ok();
    Ok(())
}

fn cluster_status(state: &AppState) -> Result<Value, RpcError> {
    let cluster = state.cluster.lock().unwrap().clone();
    let methods = vec![
        "cluster.status",
        "cluster.nodes",
        "cluster.join_token.create",
        "cluster.node.join",
        "cluster.node.remove",
        "cluster.replica.promote",
        "cluster.shard.create",
        "cluster.shard.move",
        "cluster.shard.rebalance",
    ];
    Ok(serde_json::json!({
        "enabled": cluster.enabled,
        "cluster_id": cluster.cluster_id,
        "local_node_id": cluster.local_node_id,
        "primary_node_id": cluster.primary_node_id,
        "local_role": cluster.local_role(),
        "nodes": cluster.nodes,
        "shards": cluster.shards,
        "replication": cluster.replication,
        "methods": methods,
    }))
}

fn cluster_nodes(state: &AppState, params: Option<ClusterNodesParams>) -> Result<Value, RpcError> {
    let cluster = state.cluster.lock().unwrap();
    let role = params.and_then(|p| p.role);
    let nodes: Vec<_> = cluster
        .nodes
        .iter()
        .filter(|n| {
            if let Some(ref r) = role {
                &n.role == r
            } else {
                true
            }
        })
        .cloned()
        .collect();
    Ok(serde_json::json!({ "nodes": nodes }))
}

fn cluster_join_token_create(
    state: &AppState,
    params: ClusterJoinTokenCreateParams,
) -> Result<Value, RpcError> {
    let now = now_unix_ms_u64();
    let ttl = params
        .ttl_ms
        .unwrap_or(CLUSTER_DEFAULT_JOIN_TTL_MS)
        .max(1000);
    let role = params.role.unwrap_or_else(|| "replica".to_string());
    let max_uses = params.max_uses.unwrap_or(1).max(1);
    let seq = CLUSTER_TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let token = format!("join_{}_{}", now, seq);

    {
        let mut cluster = state.cluster.lock().unwrap();
        cluster.enabled = true;
        cluster.cleanup_join_tokens(now);
        cluster.join_tokens.push(ClusterJoinToken {
            token: token.clone(),
            role: role.clone(),
            expires_at_ms: now.saturating_add(ttl),
            max_uses,
            used: 0,
            created_at_ms: now,
        });
    }
    persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    Ok(serde_json::json!({
        "token": token,
        "expires_at_ms": now.saturating_add(ttl),
        "role": role,
        "max_uses": max_uses
    }))
}

fn cluster_node_join(state: &AppState, params: ClusterNodeJoinParams) -> Result<Value, RpcError> {
    let now = now_unix_ms_u64();
    let node = {
        let mut cluster = state.cluster.lock().unwrap();
        cluster.enabled = true;
        cluster.cleanup_join_tokens(now);

        let token = cluster
            .join_tokens
            .iter_mut()
            .find(|t| t.token == params.token)
            .ok_or_else(|| RpcError::new("forbidden", "invalid or expired join token"))?;
        if token.expires_at_ms <= now {
            return Err(RpcError::new("forbidden", "join token expired"));
        }
        if token.used >= token.max_uses {
            return Err(RpcError::new("forbidden", "join token exhausted"));
        }
        token.used += 1;

        let role = params.role.unwrap_or_else(|| token.role.clone());
        if let Some(existing) = cluster
            .nodes
            .iter_mut()
            .find(|n| n.node_id == params.node_id)
        {
            existing.rpc_url = params.rpc_url.clone();
            existing.role = role.clone();
            existing.status = "online".to_string();
            existing.last_seen_ms = now;
            existing.clone()
        } else {
            let node = ClusterNode {
                node_id: params.node_id.clone(),
                rpc_url: params.rpc_url.clone(),
                role: role.clone(),
                status: "online".to_string(),
                joined_at_ms: now,
                last_seen_ms: now,
            };
            cluster.nodes.push(node.clone());
            node
        }
    };
    persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    let cluster = state.cluster.lock().unwrap();
    Ok(serde_json::json!({
        "ok": true,
        "cluster_id": cluster.cluster_id,
        "node": node
    }))
}

fn cluster_node_remove(
    state: &AppState,
    params: ClusterNodeRemoveParams,
) -> Result<Value, RpcError> {
    let mut new_primary = None;
    {
        let mut cluster = state.cluster.lock().unwrap();
        let local_node_id = cluster.local_node_id.clone();
        if params.node_id == cluster.local_node_id && !params.force.unwrap_or(false) {
            return Err(RpcError::new(
                "forbidden",
                "cannot remove local node without force=true",
            ));
        }
        if !cluster.nodes.iter().any(|n| n.node_id == params.node_id) {
            return Err(RpcError::new("not_found", "node not found"));
        }
        cluster.nodes.retain(|n| n.node_id != params.node_id);
        for shard in cluster.shards.iter_mut() {
            shard.replicas.retain(|id| id != &params.node_id);
            if shard.primary_node_id == params.node_id {
                if let Some(next) = shard.replicas.first().cloned() {
                    shard.primary_node_id = next;
                    shard.replicas.remove(0);
                } else {
                    shard.primary_node_id = local_node_id.clone();
                }
                shard.updated_at_ms = now_unix_ms_u64();
            }
        }

        if cluster.primary_node_id == params.node_id {
            if let Some(next_id) = cluster.nodes.first().map(|n| n.node_id.clone()) {
                cluster.primary_node_id = next_id.clone();
                new_primary = Some(next_id);
            } else {
                cluster.primary_node_id = local_node_id.clone();
                new_primary = Some(local_node_id.clone());
                cluster.nodes.push(ClusterNode {
                    node_id: local_node_id,
                    rpc_url: state.local_rpc_url.clone(),
                    role: "primary".to_string(),
                    status: "online".to_string(),
                    joined_at_ms: now_unix_ms_u64(),
                    last_seen_ms: now_unix_ms_u64(),
                });
            }
        }
    }
    persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    Ok(serde_json::json!({
        "ok": true,
        "removed": params.node_id,
        "new_primary": new_primary,
    }))
}

fn cluster_replica_promote(
    state: &AppState,
    params: ClusterReplicaPromoteParams,
) -> Result<Value, RpcError> {
    let now = now_unix_ms_u64();
    {
        let mut cluster = state.cluster.lock().unwrap();
        if !cluster.nodes.iter().any(|n| n.node_id == params.node_id) {
            return Err(RpcError::new("not_found", "node not found"));
        }
        if let Some(shard_id) = params.shard_id.as_ref() {
            let shard = cluster
                .shards
                .iter_mut()
                .find(|s| s.shard_id == *shard_id)
                .ok_or_else(|| RpcError::new("not_found", "shard not found"))?;
            let old_primary = shard.primary_node_id.clone();
            shard.primary_node_id = params.node_id.clone();
            shard.replicas.retain(|n| n != &params.node_id);
            if old_primary != params.node_id && !shard.replicas.contains(&old_primary) {
                shard.replicas.push(old_primary);
            }
            shard.updated_at_ms = now;
        } else {
            let old_primary = cluster.primary_node_id.clone();
            cluster.primary_node_id = params.node_id.clone();
            for node in cluster.nodes.iter_mut() {
                if node.node_id == params.node_id {
                    node.role = "primary".to_string();
                } else if node.node_id == old_primary {
                    node.role = "replica".to_string();
                }
                node.last_seen_ms = now;
            }
        }
    }
    persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    Ok(serde_json::json!({
        "ok": true,
        "primary_node_id": params.node_id,
        "shard_id": params.shard_id
    }))
}

fn cluster_shard_create(
    state: &AppState,
    params: ClusterShardCreateParams,
) -> Result<Value, RpcError> {
    let now = now_unix_ms_u64();
    let shard = {
        let mut cluster = state.cluster.lock().unwrap();
        cluster.enabled = true;
        let shard_id = params.shard_id.clone().unwrap_or_else(|| {
            format!(
                "shard_{}_{}",
                params.db,
                CLUSTER_TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed)
            )
        });
        if cluster.shards.iter().any(|s| s.shard_id == shard_id) {
            return Err(RpcError::new("conflict", "shard already exists"));
        }
        let primary = params
            .primary_node_id
            .clone()
            .unwrap_or_else(|| cluster.primary_node_id.clone());
        if !cluster.nodes.iter().any(|n| n.node_id == primary) {
            return Err(RpcError::new("not_found", "primary node not found"));
        }
        let mut replicas = params.replicas.unwrap_or_else(|| {
            cluster
                .nodes
                .iter()
                .filter(|n| n.role == "replica")
                .map(|n| n.node_id.clone())
                .collect::<Vec<_>>()
        });
        replicas.retain(|n| n != &primary);
        let shard = ClusterShard {
            shard_id,
            db: params.db,
            table: params.table,
            primary_node_id: primary,
            replicas,
            slots: params.slots.unwrap_or(128).max(1),
            updated_at_ms: now,
        };
        cluster.shards.push(shard.clone());
        shard
    };
    persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    Ok(serde_json::json!({"ok": true, "shard": shard}))
}

fn cluster_shard_move(state: &AppState, params: ClusterShardMoveParams) -> Result<Value, RpcError> {
    let now = now_unix_ms_u64();
    let moved = {
        let mut cluster = state.cluster.lock().unwrap();
        if !cluster.nodes.iter().any(|n| n.node_id == params.to_node_id) {
            return Err(RpcError::new("not_found", "destination node not found"));
        }
        let shard = cluster
            .shards
            .iter_mut()
            .find(|s| s.shard_id == params.shard_id)
            .ok_or_else(|| RpcError::new("not_found", "shard not found"))?;
        let mut preview = shard.clone();
        let old_primary = preview.primary_node_id.clone();
        preview.primary_node_id = params.to_node_id.clone();
        preview.replicas.retain(|n| n != &params.to_node_id);
        if old_primary != params.to_node_id && !preview.replicas.contains(&old_primary) {
            preview.replicas.push(old_primary);
        }
        preview.updated_at_ms = now;
        if !params.dry_run.unwrap_or(false) {
            *shard = preview.clone();
        }
        preview
    };
    if !params.dry_run.unwrap_or(false) {
        persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    }
    Ok(serde_json::json!({
        "ok": true,
        "dry_run": params.dry_run.unwrap_or(false),
        "shard": moved
    }))
}

fn cluster_shard_rebalance(
    state: &AppState,
    params: ClusterShardRebalanceParams,
) -> Result<Value, RpcError> {
    let max_moves = params.max_moves.unwrap_or(8).max(1) as usize;
    let dry_run = params.dry_run.unwrap_or(false);
    let mut plans = Vec::new();
    {
        let cluster = state.cluster.lock().unwrap();
        let active_nodes: Vec<String> = cluster
            .nodes
            .iter()
            .filter(|n| n.status == "online")
            .map(|n| n.node_id.clone())
            .collect();
        if active_nodes.len() < 2 || cluster.shards.len() < 2 {
            return Ok(serde_json::json!({"ok": true, "dry_run": dry_run, "moves": plans}));
        }

        let mut loads: HashMap<String, usize> =
            active_nodes.iter().map(|n| (n.clone(), 0usize)).collect();
        for shard in cluster.shards.iter() {
            *loads.entry(shard.primary_node_id.clone()).or_insert(0) += 1;
        }

        for _ in 0..max_moves {
            let mut order = loads.iter().collect::<Vec<_>>();
            order.sort_by_key(|(_, c)| **c);
            let (min_node, min_load) = order.first().map(|(n, c)| ((*n).clone(), **c)).unwrap();
            let (max_node, max_load) = order.last().map(|(n, c)| ((*n).clone(), **c)).unwrap();
            if max_load <= min_load + 1 {
                break;
            }
            if let Some(shard) = cluster
                .shards
                .iter()
                .find(|s| s.primary_node_id == max_node)
            {
                plans.push(serde_json::json!({
                    "shard_id": shard.shard_id,
                    "from_node_id": max_node,
                    "to_node_id": min_node,
                }));
                if let Some(v) = loads.get_mut(&max_node) {
                    *v = v.saturating_sub(1);
                }
                if let Some(v) = loads.get_mut(&min_node) {
                    *v += 1;
                }
            } else {
                break;
            }
        }
    }

    if !dry_run && !plans.is_empty() {
        let mut cluster = state.cluster.lock().unwrap();
        for plan in plans.iter() {
            let shard_id = plan
                .get("shard_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let to_node = plan
                .get("to_node_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if let Some(shard) = cluster.shards.iter_mut().find(|s| s.shard_id == shard_id) {
                let old = shard.primary_node_id.clone();
                shard.primary_node_id = to_node.to_string();
                shard.replicas.retain(|n| n != to_node);
                if !shard.replicas.contains(&old) {
                    shard.replicas.push(old);
                }
                shard.updated_at_ms = now_unix_ms_u64();
            }
        }
        drop(cluster);
        persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    }

    Ok(serde_json::json!({
        "ok": true,
        "dry_run": dry_run,
        "moves": plans,
    }))
}

fn now_unix_ms() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn now_unix_ms_u64() -> u64 {
    now_unix_ms() as u64
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SqlExecParams {
    sql: String,
    #[serde(default)]
    explain: bool,
    #[serde(default)]
    default_db: Option<String>,
    #[serde(default)]
    result_format: Option<ResultFormat>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlVerb {
    Select,
    ShowDatabases,
    ShowTables,
    ShowColumns,
    Use,
    CreateDatabase,
    CreateTable,
    Insert,
    Update,
    Delete,
    Unsupported,
}

#[derive(Debug, Clone)]
enum SqlPlan {
    Select {
        table: BaseTableRef,
        projection: Vec<SelectItem>,
        where_expr: Option<Expr>,
        order_by: Vec<OrderBy>,
        limit: Option<LimitClause>,
    },
    ShowDatabases,
    ShowTables {
        db: String,
    },
    ShowColumns {
        table: BaseTableRef,
    },
    UseDb {
        db: String,
    },
    CreateDatabase {
        db: String,
        if_not_exists: bool,
    },
    CreateTable {
        table: BaseTableRef,
        columns: Vec<SchemaColumnInfo>,
        primary_key: Vec<String>,
        if_not_exists: bool,
    },
    Insert {
        table: BaseTableRef,
        rows: Vec<BTreeMap<String, Lit>>,
    },
    Update {
        table: BaseTableRef,
        set: BTreeMap<String, Lit>,
        where_expr: Expr,
        limit: Option<u64>,
    },
    Delete {
        table: BaseTableRef,
        where_expr: Expr,
        limit: Option<u64>,
    },
}

fn parse_params<T: DeserializeOwned>(params: Option<Value>) -> Result<T, RpcError> {
    let v = params.ok_or_else(|| RpcError::new("invalid_request", "missing params"))?;
    serde_json::from_value(v).map_err(|e| RpcError::new("invalid_request", e.to_string()))
}

fn to_rpc_error(e: anyhow::Error) -> RpcError {
    let msg = e.to_string();
    // A few common shorthands used by the prototype engine.
    if let Some(rest) = msg.strip_prefix("not_supported:") {
        return RpcError::new("not_supported", rest.trim());
    }
    if let Some(rest) = msg.strip_prefix("invalid_request:") {
        return RpcError::new("invalid_request", rest.trim());
    }
    match msg.as_str() {
        "not_found" => RpcError::new("not_found", "not found"),
        "conflict" => RpcError::new("conflict", "conflict"),
        "causality_not_satisfied" => {
            RpcError::new("precondition_failed", "min_causality not satisfied")
        }
        "dp_budget_missing" => RpcError::new("not_found", "dp budget not found"),
        "dp_budget_exhausted" => RpcError::new("forbidden", "privacy budget exhausted"),
        "not_patchable" => RpcError::new(
            "not_supported",
            "query.patch requires a single-table SELECT with a primary key",
        ),
        _ => {
            if msg.contains("not found") {
                RpcError::new("not_found", msg)
            } else if msg.contains("conflict") {
                RpcError::new("conflict", msg)
            } else {
                RpcError::new("internal", msg)
            }
        }
    }
}

fn sql_exec_is_read_only(params: Option<&Value>) -> bool {
    let Some(params) = params.and_then(|v| v.as_object()) else {
        return false;
    };
    if params
        .get("explain")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return true;
    }
    let sql = params
        .get("sql")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    matches!(
        sql_detect_verb(sql),
        SqlVerb::Select
            | SqlVerb::ShowDatabases
            | SqlVerb::ShowTables
            | SqlVerb::ShowColumns
            | SqlVerb::Use
    )
}

fn sql_detect_verb(sql: &str) -> SqlVerb {
    let s = sql.trim().trim_end_matches(';').trim_start();
    let lower = s.to_ascii_lowercase();
    if lower.starts_with("select ") {
        SqlVerb::Select
    } else if lower.starts_with("show databases") {
        SqlVerb::ShowDatabases
    } else if lower.starts_with("show tables") {
        SqlVerb::ShowTables
    } else if lower.starts_with("show columns ") || lower.starts_with("show full columns ") {
        SqlVerb::ShowColumns
    } else if lower.starts_with("use ") {
        SqlVerb::Use
    } else if lower.starts_with("create database ") {
        SqlVerb::CreateDatabase
    } else if lower.starts_with("create table ") {
        SqlVerb::CreateTable
    } else if lower.starts_with("insert into ") {
        SqlVerb::Insert
    } else if lower.starts_with("update ") {
        SqlVerb::Update
    } else if lower.starts_with("delete from ") {
        SqlVerb::Delete
    } else {
        SqlVerb::Unsupported
    }
}

fn clean_sql_ident(raw: &str) -> String {
    raw.trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_string()
}

fn is_sql_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn find_keyword_top_level(haystack: &str, keyword: &str) -> Option<usize> {
    let needle = keyword.as_bytes();
    if needle.is_empty() {
        return None;
    }
    let bytes = haystack.as_bytes();
    let lower = haystack.to_ascii_lowercase().into_bytes();
    let mut i = 0usize;
    let mut depth = 0u32;
    let mut quote = 0u8;
    while i < bytes.len() {
        let b = bytes[i];
        if quote != 0 {
            if b == quote {
                if quote == b'\'' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                quote = 0;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' | b'"' | b'`' => {
                quote = b;
                i += 1;
                continue;
            }
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0
            && i + needle.len() <= lower.len()
            && &lower[i..i + needle.len()] == needle
            && (i == 0 || !is_sql_ident_char(lower[i - 1]))
            && (i + needle.len() == lower.len() || !is_sql_ident_char(lower[i + needle.len()]))
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn split_csv_top_level(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut depth = 0u32;
    let mut quote = 0u8;
    while i < bytes.len() {
        let b = bytes[i];
        if quote != 0 {
            if b == quote {
                if quote == b'\'' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                quote = 0;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' | b'"' | b'`' => quote = b,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                let part = input[start..i].trim();
                if !part.is_empty() {
                    out.push(part.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

fn split_top_level_and(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = input.trim();
    while let Some(idx) = find_keyword_top_level(rest, "and") {
        let left = rest[..idx].trim();
        if !left.is_empty() {
            parts.push(left.to_string());
        }
        rest = rest[idx + 3..].trim();
    }
    if !rest.is_empty() {
        parts.push(rest.to_string());
    }
    parts
}

fn parse_table_ref(name: &str, default_db: Option<&str>) -> Result<BaseTableRef, RpcError> {
    let cleaned = clean_sql_ident(name);
    if cleaned.is_empty() {
        return Err(RpcError::new("invalid_request", "missing table name"));
    }
    if let Some((db, table)) = cleaned.split_once('.') {
        return Ok(BaseTableRef {
            db: clean_sql_ident(db),
            table: clean_sql_ident(table),
            r#as: None,
        });
    }
    let db = default_db
        .map(clean_sql_ident)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            RpcError::new(
                "invalid_request",
                "table name must include db.table or provide default_db",
            )
        })?;
    Ok(BaseTableRef {
        db,
        table: cleaned,
        r#as: None,
    })
}

fn parse_sql_lit(raw: &str) -> Result<Lit, RpcError> {
    let s = raw.trim();
    if s.eq_ignore_ascii_case("null") {
        return Ok(Lit::Null);
    }
    if s.eq_ignore_ascii_case("true") {
        return Ok(Lit::Bool { v: true });
    }
    if s.eq_ignore_ascii_case("false") {
        return Ok(Lit::Bool { v: false });
    }
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        return Ok(Lit::Str {
            v: inner.replace("''", "'"),
        });
    }
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Ok(Lit::Str {
            v: s[1..s.len() - 1].to_string(),
        });
    }
    if let Ok(v) = s.parse::<i64>() {
        return Ok(Lit::I64 { v });
    }
    if let Ok(v) = s.parse::<f64>() {
        return Ok(Lit::F64 { v });
    }
    Ok(Lit::Str { v: s.to_string() })
}

fn parse_condition_expr(clause: &str) -> Result<Expr, RpcError> {
    let bytes = clause.as_bytes();
    let mut i = 0usize;
    let mut depth = 0u32;
    let mut quote = 0u8;
    while i < bytes.len() {
        let b = bytes[i];
        if quote != 0 {
            if b == quote {
                if quote == b'\'' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                quote = 0;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' | b'"' | b'`' => {
                quote = b;
                i += 1;
                continue;
            }
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 {
            for (token, op) in [
                (">=", "ge"),
                ("<=", "le"),
                ("<>", "ne"),
                ("!=", "ne"),
                ("=", "eq"),
                (">", "gt"),
                ("<", "lt"),
            ] {
                if clause[i..].starts_with(token) {
                    let left = clause[..i].trim();
                    let right = clause[i + token.len()..].trim();
                    if left.is_empty() || right.is_empty() {
                        return Err(RpcError::new(
                            "invalid_request",
                            format!("invalid predicate '{}'", clause),
                        ));
                    }
                    let col = clean_sql_ident(left.rsplit('.').next().unwrap_or(left));
                    return Ok(Expr::Op {
                        op: op.to_string(),
                        a: Some(Box::new(Expr::Col { col, table: None })),
                        b: Some(Box::new(Expr::Lit {
                            lit: parse_sql_lit(right)?,
                        })),
                        args: None,
                        list: None,
                        lo: None,
                        hi: None,
                    });
                }
            }
        }
        i += 1;
    }
    Err(RpcError::new(
        "not_supported",
        format!(
            "only simple comparisons are supported in WHERE: '{}'",
            clause
        ),
    ))
}

fn parse_where_expr(where_sql: &str) -> Result<Option<Expr>, RpcError> {
    let where_sql = where_sql.trim();
    if where_sql.is_empty() {
        return Ok(None);
    }
    let parts = split_top_level_and(where_sql);
    if parts.is_empty() {
        return Ok(None);
    }
    let mut expr = parse_condition_expr(&parts[0])?;
    for part in parts.iter().skip(1) {
        let rhs = parse_condition_expr(part)?;
        expr = Expr::Op {
            op: "and".to_string(),
            a: Some(Box::new(expr)),
            b: Some(Box::new(rhs)),
            args: None,
            list: None,
            lo: None,
            hi: None,
        };
    }
    Ok(Some(expr))
}

fn parse_order_by(order_sql: &str) -> Result<Vec<OrderBy>, RpcError> {
    let mut out = Vec::new();
    for part in split_csv_top_level(order_sql) {
        let mut toks = part.split_whitespace();
        let Some(col_tok) = toks.next() else {
            continue;
        };
        let col = clean_sql_ident(col_tok.rsplit('.').next().unwrap_or(col_tok));
        if col.is_empty() {
            continue;
        }
        let dir = match toks.next().map(|t| t.to_ascii_lowercase()) {
            Some(d) if d == "desc" => Some(OrderDir::Desc),
            Some(d) if d == "asc" => Some(OrderDir::Asc),
            Some(other) => {
                return Err(RpcError::new(
                    "not_supported",
                    format!("unsupported ORDER BY direction '{}'", other),
                ))
            }
            None => Some(OrderDir::Asc),
        };
        out.push(OrderBy {
            expr: Expr::Col { col, table: None },
            dir,
        });
    }
    Ok(out)
}

fn parse_limit_clause(
    limit_sql: Option<&str>,
    offset_sql: Option<&str>,
) -> Result<Option<LimitClause>, RpcError> {
    let limit =
        match limit_sql {
            Some(raw) => Some(raw.trim().parse::<u64>().map_err(|_| {
                RpcError::new("invalid_request", "LIMIT must be an unsigned integer")
            })?),
            None => None,
        };
    let offset =
        match offset_sql {
            Some(raw) => Some(raw.trim().parse::<u64>().map_err(|_| {
                RpcError::new("invalid_request", "OFFSET must be an unsigned integer")
            })?),
            None => None,
        };
    if limit.is_none() && offset.is_none() {
        Ok(None)
    } else {
        Ok(Some(LimitClause { limit, offset }))
    }
}

fn parse_select_projection_item(raw: &str) -> Result<SelectItem, RpcError> {
    let mut expr_raw = raw.trim();
    let mut alias = None;
    if let Some(idx) = find_keyword_top_level(expr_raw, "as") {
        let left = expr_raw[..idx].trim();
        let right = expr_raw[idx + 2..].trim();
        if !left.is_empty() && !right.is_empty() {
            expr_raw = left;
            alias = Some(clean_sql_ident(right));
        }
    }
    let expr = if expr_raw == "*" {
        return Err(RpcError::new(
            "not_supported",
            "wildcard projection is resolved separately",
        ));
    } else if let Ok(lit) = parse_sql_lit(expr_raw) {
        // parse_sql_lit returns Str for bare identifiers, so handle identifiers first.
        if expr_raw.starts_with('\'')
            || expr_raw.starts_with('"')
            || expr_raw.eq_ignore_ascii_case("null")
            || expr_raw.eq_ignore_ascii_case("true")
            || expr_raw.eq_ignore_ascii_case("false")
            || expr_raw.parse::<i64>().is_ok()
            || expr_raw.parse::<f64>().is_ok()
        {
            Expr::Lit { lit }
        } else {
            let col = clean_sql_ident(expr_raw.rsplit('.').next().unwrap_or(expr_raw));
            if col.is_empty() {
                return Err(RpcError::new(
                    "invalid_request",
                    format!("invalid SELECT projection '{}'", raw),
                ));
            }
            Expr::Col { col, table: None }
        }
    } else {
        return Err(RpcError::new(
            "not_supported",
            format!("unsupported SELECT projection '{}'", raw),
        ));
    };
    Ok(SelectItem { expr, r#as: alias })
}

fn parse_select_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let mut rest = sql.trim();
    rest = rest
        .strip_prefix("SELECT ")
        .or_else(|| rest.strip_prefix("select "))
        .ok_or_else(|| RpcError::new("invalid_request", "invalid SELECT statement"))?;
    let from_idx = find_keyword_top_level(rest, "from");
    if from_idx.is_none() {
        let mut projection = Vec::new();
        for part in split_csv_top_level(rest) {
            projection.push(parse_select_projection_item(&part)?);
        }
        return Ok(SqlPlan::Select {
            table: BaseTableRef {
                db: String::new(),
                table: String::new(),
                r#as: None,
            },
            projection,
            where_expr: None,
            order_by: Vec::new(),
            limit: None,
        });
    }
    let from_idx = from_idx.unwrap_or_default();
    let projection_sql = rest[..from_idx].trim();
    let mut rem = rest[from_idx + 4..].trim();

    let next_idx = ["where", "order by", "limit", "offset"]
        .iter()
        .filter_map(|k| find_keyword_top_level(rem, k))
        .min()
        .unwrap_or(rem.len());
    let table_sql = rem[..next_idx].trim();
    rem = rem[next_idx..].trim();
    let table = parse_table_ref(table_sql, default_db)?;

    let mut where_sql = None::<String>;
    let mut order_sql = None::<String>;
    let mut limit_sql = None::<String>;
    let mut offset_sql = None::<String>;

    while !rem.is_empty() {
        if rem.to_ascii_lowercase().starts_with("where ") {
            let tail = rem[5..].trim_start();
            let next = ["order by", "limit", "offset"]
                .iter()
                .filter_map(|k| find_keyword_top_level(tail, k))
                .min()
                .unwrap_or(tail.len());
            where_sql = Some(tail[..next].trim().to_string());
            rem = tail[next..].trim();
            continue;
        }
        if rem.to_ascii_lowercase().starts_with("order by ") {
            let tail = rem[8..].trim_start();
            let next = ["limit", "offset"]
                .iter()
                .filter_map(|k| find_keyword_top_level(tail, k))
                .min()
                .unwrap_or(tail.len());
            order_sql = Some(tail[..next].trim().to_string());
            rem = tail[next..].trim();
            continue;
        }
        if rem.to_ascii_lowercase().starts_with("limit ") {
            let tail = rem[5..].trim_start();
            let next = ["offset"]
                .iter()
                .filter_map(|k| find_keyword_top_level(tail, k))
                .min()
                .unwrap_or(tail.len());
            limit_sql = Some(tail[..next].trim().to_string());
            rem = tail[next..].trim();
            continue;
        }
        if rem.to_ascii_lowercase().starts_with("offset ") {
            let tail = rem[6..].trim_start();
            offset_sql = Some(tail.trim().to_string());
            rem = "";
            continue;
        }
        return Err(RpcError::new(
            "not_supported",
            format!("unsupported SELECT clause '{}'", rem),
        ));
    }

    let projection = if projection_sql == "*" {
        Vec::new()
    } else {
        let mut out = Vec::new();
        for part in split_csv_top_level(projection_sql) {
            out.push(parse_select_projection_item(&part)?);
        }
        out
    };

    Ok(SqlPlan::Select {
        table,
        projection,
        where_expr: parse_where_expr(where_sql.as_deref().unwrap_or_default())?,
        order_by: parse_order_by(order_sql.as_deref().unwrap_or_default())?,
        limit: parse_limit_clause(limit_sql.as_deref(), offset_sql.as_deref())?,
    })
}

fn parse_show_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let lower = sql.to_ascii_lowercase();
    if lower.starts_with("show databases") {
        return Ok(SqlPlan::ShowDatabases);
    }
    if lower.starts_with("show tables") {
        let tail = sql[11..].trim();
        if tail.is_empty() {
            let db = default_db
                .map(clean_sql_ident)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    RpcError::new(
                        "invalid_request",
                        "SHOW TABLES requires FROM <db> or default_db",
                    )
                })?;
            return Ok(SqlPlan::ShowTables { db });
        }
        let tail_l = tail.to_ascii_lowercase();
        if tail_l.starts_with("from ") || tail_l.starts_with("in ") {
            let db = clean_sql_ident(tail[4..].trim());
            return Ok(SqlPlan::ShowTables { db });
        }
        return Err(RpcError::new(
            "not_supported",
            "SHOW TABLES supports only SHOW TABLES [FROM|IN] <db>",
        ));
    }
    if lower.starts_with("show columns from ") || lower.starts_with("show full columns from ") {
        let start = if lower.starts_with("show full columns from ") {
            22
        } else {
            18
        };
        let tail = sql[start..].trim();
        let from_idx = find_keyword_top_level(tail, "from");
        let (table_name, db_name) = if let Some(idx) = from_idx {
            (
                tail[..idx].trim(),
                Some(clean_sql_ident(tail[idx + 4..].trim())),
            )
        } else {
            (tail, default_db.map(clean_sql_ident))
        };
        let table_ref = parse_table_ref(table_name, db_name.as_deref())?;
        return Ok(SqlPlan::ShowColumns { table: table_ref });
    }
    Err(RpcError::new("not_supported", "unsupported SHOW statement"))
}

fn parse_use_plan(sql: &str) -> Result<SqlPlan, RpcError> {
    let db = clean_sql_ident(sql[3..].trim());
    if db.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "USE requires a database name",
        ));
    }
    Ok(SqlPlan::UseDb { db })
}

fn parse_create_database_plan(sql: &str) -> Result<SqlPlan, RpcError> {
    let mut tail = sql[15..].trim();
    let mut if_not_exists = false;
    let lower = tail.to_ascii_lowercase();
    if lower.starts_with("if not exists ") {
        if_not_exists = true;
        tail = tail[14..].trim();
    }
    let db = clean_sql_ident(tail);
    if db.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "CREATE DATABASE requires a name",
        ));
    }
    Ok(SqlPlan::CreateDatabase { db, if_not_exists })
}

fn sql_type_to_desc(token: &str, unsigned: bool) -> TypeDesc {
    let base = token
        .split('(')
        .next()
        .unwrap_or(token)
        .trim()
        .to_ascii_lowercase();
    let max = token
        .split_once('(')
        .and_then(|(_, rhs)| rhs.split_once(')'))
        .and_then(|(inner, _)| inner.trim().parse::<u64>().ok());
    let kind = match base.as_str() {
        "bigint" | "int" | "integer" | "smallint" | "tinyint" => {
            if unsigned {
                "u64"
            } else {
                "i64"
            }
        }
        "double" | "float" | "real" | "decimal" => "f64",
        "datetime" | "timestamp" => "datetime",
        "date" => "date",
        "time" => "time",
        "json" => "json",
        "blob" | "binary" | "varbinary" => "bytes",
        "bool" | "boolean" => "bool",
        _ => "string",
    };
    TypeDesc {
        kind: kind.to_string(),
        max,
        precision: None,
        scale: None,
        charset: None,
        collation: None,
        unsigned: if unsigned { Some(true) } else { None },
    }
}

fn parse_create_table_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let mut tail = sql[12..].trim();
    let mut if_not_exists = false;
    let lower = tail.to_ascii_lowercase();
    if lower.starts_with("if not exists ") {
        if_not_exists = true;
        tail = tail[14..].trim();
    }
    let Some(open_idx) = tail.find('(') else {
        return Err(RpcError::new(
            "invalid_request",
            "CREATE TABLE requires column definitions",
        ));
    };
    let table_name = tail[..open_idx].trim();
    let table = parse_table_ref(table_name, default_db)?;
    let close_idx = tail.rfind(')').ok_or_else(|| {
        RpcError::new(
            "invalid_request",
            "CREATE TABLE missing closing ')' for column definitions",
        )
    })?;
    let defs = &tail[open_idx + 1..close_idx];
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    for part in split_csv_top_level(defs) {
        let p = part.trim();
        let p_lower = p.to_ascii_lowercase();
        if p_lower.starts_with("primary key") {
            if let Some(start) = p.find('(') {
                if let Some(end) = p.rfind(')') {
                    for key in split_csv_top_level(&p[start + 1..end]) {
                        let col = clean_sql_ident(&key);
                        if !col.is_empty() {
                            primary_key.push(col);
                        }
                    }
                }
            }
            continue;
        }
        if p_lower.starts_with("key ")
            || p_lower.starts_with("unique key")
            || p_lower.starts_with("index ")
            || p_lower.starts_with("constraint ")
        {
            continue;
        }
        let toks: Vec<&str> = p.split_whitespace().collect();
        if toks.len() < 2 {
            return Err(RpcError::new(
                "invalid_request",
                format!("invalid column definition '{}'", p),
            ));
        }
        let name = clean_sql_ident(toks[0]);
        let type_tok = toks[1];
        let unsigned = toks.iter().any(|t| t.eq_ignore_ascii_case("unsigned"));
        let nullable = !p_lower.contains("not null");
        let auto_increment = p_lower.contains("auto_increment");
        columns.push(SchemaColumnInfo {
            name,
            r#type: sql_type_to_desc(type_tok, unsigned),
            nullable,
            auto_increment,
        });
    }
    if columns.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "CREATE TABLE must define at least one column",
        ));
    }
    Ok(SqlPlan::CreateTable {
        table,
        columns,
        primary_key,
        if_not_exists,
    })
}

fn parse_insert_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let lower = sql.to_ascii_lowercase();
    if find_keyword_top_level(&lower, "on duplicate key").is_some() {
        return Err(RpcError::new(
            "not_supported",
            "ON DUPLICATE KEY UPDATE is not supported in sql.exec yet",
        ));
    }
    let tail = sql[11..].trim();
    let values_idx = find_keyword_top_level(tail, "values").ok_or_else(|| {
        RpcError::new("invalid_request", "INSERT currently requires VALUES syntax")
    })?;
    let head = tail[..values_idx].trim();
    let values_sql = tail[values_idx + 6..].trim();

    let open_idx = head.find('(').ok_or_else(|| {
        RpcError::new(
            "invalid_request",
            "INSERT requires explicit column list, e.g. INSERT INTO t (c) VALUES (...)",
        )
    })?;
    let close_idx = head
        .rfind(')')
        .ok_or_else(|| RpcError::new("invalid_request", "INSERT column list is malformed"))?;
    let table_name = head[..open_idx].trim();
    let table = parse_table_ref(table_name, default_db)?;
    let cols: Vec<String> = split_csv_top_level(&head[open_idx + 1..close_idx])
        .into_iter()
        .map(|c| clean_sql_ident(&c))
        .filter(|c| !c.is_empty())
        .collect();
    if cols.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "INSERT column list is empty",
        ));
    }

    let mut tuples = Vec::new();
    let bytes = values_sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if bytes[i] != b'(' {
            return Err(RpcError::new(
                "invalid_request",
                "INSERT VALUES must contain tuples like (...), (...)",
            ));
        }
        let start = i + 1;
        let mut depth = 1u32;
        i += 1;
        let mut quote = 0u8;
        while i < bytes.len() {
            let b = bytes[i];
            if quote != 0 {
                if b == quote {
                    if quote == b'\'' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    quote = 0;
                }
                i += 1;
                continue;
            }
            match b {
                b'\'' | b'"' => quote = b,
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        if i >= bytes.len() {
            return Err(RpcError::new(
                "invalid_request",
                "INSERT tuple is not closed",
            ));
        }
        tuples.push(values_sql[start..i].to_string());
        i += 1;
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
            i += 1;
        }
    }
    let mut rows = Vec::new();
    for tuple in tuples {
        let values = split_csv_top_level(&tuple);
        if values.len() != cols.len() {
            return Err(RpcError::new(
                "invalid_request",
                "INSERT values count does not match column list",
            ));
        }
        let mut row = BTreeMap::new();
        for (col, val) in cols.iter().zip(values.iter()) {
            row.insert(col.clone(), parse_sql_lit(val)?);
        }
        rows.push(row);
    }
    Ok(SqlPlan::Insert { table, rows })
}

fn parse_update_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let tail = sql[6..].trim();
    let set_idx = find_keyword_top_level(tail, "set")
        .ok_or_else(|| RpcError::new("invalid_request", "UPDATE requires SET clause"))?;
    let table = parse_table_ref(tail[..set_idx].trim(), default_db)?;
    let mut rem = tail[set_idx + 3..].trim();

    let where_idx = find_keyword_top_level(rem, "where");
    let limit_idx = find_keyword_top_level(rem, "limit");
    let set_end = [where_idx, limit_idx]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(rem.len());
    let set_sql = rem[..set_end].trim();
    rem = rem[set_end..].trim();

    let mut set = BTreeMap::new();
    for assign in split_csv_top_level(set_sql) {
        let Some(eq_idx) = assign.find('=') else {
            return Err(RpcError::new(
                "invalid_request",
                format!("invalid assignment '{}'", assign),
            ));
        };
        let col = clean_sql_ident(assign[..eq_idx].trim());
        let val = assign[eq_idx + 1..].trim();
        set.insert(col, parse_sql_lit(val)?);
    }
    let mut where_sql = None::<String>;
    let mut limit = None::<u64>;
    while !rem.is_empty() {
        if rem.to_ascii_lowercase().starts_with("where ") {
            let tail = rem[5..].trim_start();
            let next = find_keyword_top_level(tail, "limit").unwrap_or(tail.len());
            where_sql = Some(tail[..next].trim().to_string());
            rem = tail[next..].trim();
            continue;
        }
        if rem.to_ascii_lowercase().starts_with("limit ") {
            let tail = rem[5..].trim_start();
            limit = Some(tail.trim().parse::<u64>().map_err(|_| {
                RpcError::new("invalid_request", "LIMIT must be an unsigned integer")
            })?);
            rem = "";
            continue;
        }
        return Err(RpcError::new(
            "not_supported",
            format!("unsupported UPDATE clause '{}'", rem),
        ));
    }
    let where_expr =
        parse_where_expr(where_sql.as_deref().unwrap_or_default())?.unwrap_or(Expr::Lit {
            lit: Lit::Bool { v: true },
        });
    Ok(SqlPlan::Update {
        table,
        set,
        where_expr,
        limit,
    })
}

fn parse_delete_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let mut rem = sql[11..].trim();
    let next = [
        find_keyword_top_level(rem, "where"),
        find_keyword_top_level(rem, "limit"),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(rem.len());
    let table = parse_table_ref(rem[..next].trim(), default_db)?;
    rem = rem[next..].trim();
    let mut where_sql = None::<String>;
    let mut limit = None::<u64>;
    while !rem.is_empty() {
        if rem.to_ascii_lowercase().starts_with("where ") {
            let tail = rem[5..].trim_start();
            let next = find_keyword_top_level(tail, "limit").unwrap_or(tail.len());
            where_sql = Some(tail[..next].trim().to_string());
            rem = tail[next..].trim();
            continue;
        }
        if rem.to_ascii_lowercase().starts_with("limit ") {
            let tail = rem[5..].trim_start();
            limit = Some(tail.trim().parse::<u64>().map_err(|_| {
                RpcError::new("invalid_request", "LIMIT must be an unsigned integer")
            })?);
            rem = "";
            continue;
        }
        return Err(RpcError::new(
            "not_supported",
            format!("unsupported DELETE clause '{}'", rem),
        ));
    }
    let where_expr =
        parse_where_expr(where_sql.as_deref().unwrap_or_default())?.unwrap_or(Expr::Lit {
            lit: Lit::Bool { v: true },
        });
    Ok(SqlPlan::Delete {
        table,
        where_expr,
        limit,
    })
}

fn parse_sql_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let normalized = sql.trim().trim_end_matches(';').trim();
    if normalized.is_empty() {
        return Err(RpcError::new("invalid_request", "sql is empty"));
    }
    match sql_detect_verb(normalized) {
        SqlVerb::Select => parse_select_plan(normalized, default_db),
        SqlVerb::ShowDatabases | SqlVerb::ShowTables | SqlVerb::ShowColumns => {
            parse_show_plan(normalized, default_db)
        }
        SqlVerb::Use => parse_use_plan(normalized),
        SqlVerb::CreateDatabase => parse_create_database_plan(normalized),
        SqlVerb::CreateTable => parse_create_table_plan(normalized, default_db),
        SqlVerb::Insert => parse_insert_plan(normalized, default_db),
        SqlVerb::Update => parse_update_plan(normalized, default_db),
        SqlVerb::Delete => parse_delete_plan(normalized, default_db),
        SqlVerb::Unsupported => Err(RpcError::new(
            "not_supported",
            "sql.exec supports SELECT/SHOW/USE/CREATE DATABASE/CREATE TABLE/INSERT/UPDATE/DELETE",
        )),
    }
}

fn sql_plan_name(plan: &SqlPlan) -> &'static str {
    match plan {
        SqlPlan::Select { .. } => "select",
        SqlPlan::ShowDatabases => "show_databases",
        SqlPlan::ShowTables { .. } => "show_tables",
        SqlPlan::ShowColumns { .. } => "show_columns",
        SqlPlan::UseDb { .. } => "use",
        SqlPlan::CreateDatabase { .. } => "create_database",
        SqlPlan::CreateTable { .. } => "create_table",
        SqlPlan::Insert { .. } => "insert",
        SqlPlan::Update { .. } => "update",
        SqlPlan::Delete { .. } => "delete",
    }
}

fn sql_plan_read_only(plan: &SqlPlan) -> bool {
    matches!(
        plan,
        SqlPlan::Select { .. }
            | SqlPlan::ShowDatabases
            | SqlPlan::ShowTables { .. }
            | SqlPlan::ShowColumns { .. }
            | SqlPlan::UseDb { .. }
    )
}

fn sql_exec_write_target(params: &Value) -> (Option<String>, Option<String>) {
    let Some(obj) = params.as_object() else {
        return (None, None);
    };
    let sql = obj.get("sql").and_then(|v| v.as_str()).unwrap_or_default();
    let default_db = obj.get("default_db").and_then(|v| v.as_str());
    match parse_sql_plan(sql, default_db) {
        Ok(SqlPlan::CreateDatabase { db, .. }) => (Some(db), None),
        Ok(SqlPlan::CreateTable { table, .. })
        | Ok(SqlPlan::Insert { table, .. })
        | Ok(SqlPlan::Update { table, .. })
        | Ok(SqlPlan::Delete { table, .. }) => (Some(table.db), Some(table.table)),
        _ => (None, None),
    }
}

async fn sql_exec(state: &AppState, params: SqlExecParams) -> Result<Value, RpcError> {
    let plan = parse_sql_plan(&params.sql, params.default_db.as_deref())?;
    if params.explain {
        return Ok(serde_json::json!({
            "statement": sql_plan_name(&plan),
            "read_only": sql_plan_read_only(&plan),
            "plan": sql_plan_name(&plan)
        }));
    }
    match plan {
        SqlPlan::Select {
            table,
            mut projection,
            where_expr,
            order_by,
            limit,
        } => {
            // SELECT without FROM for simple literals (e.g. SELECT 1)
            if table.db.is_empty() && table.table.is_empty() {
                let mut columns = Vec::new();
                let mut row = Vec::new();
                for (idx, item) in projection.iter().enumerate() {
                    let name = item
                        .r#as
                        .clone()
                        .unwrap_or_else(|| format!("expr{}", idx + 1));
                    columns.push(serde_json::json!({ "name": name }));
                    let lit = match &item.expr {
                        Expr::Lit { lit } => lit.clone(),
                        _ => {
                            return Err(RpcError::new(
                                "not_supported",
                                "SELECT without FROM supports only literal expressions",
                            ))
                        }
                    };
                    row.push(serde_json::to_value(lit).unwrap_or(Value::Null));
                }
                return Ok(serde_json::json!({
                    "statement": "select",
                    "read_only": true,
                    "result": {
                        "data": {
                            "columns": columns,
                            "rows": [row]
                        }
                    }
                }));
            }

            let eng = state.engine.read().await;
            if projection.is_empty() {
                let desc = eng
                    .describe_table(&table.db, &table.table)
                    .map_err(to_rpc_error)?;
                let names: Vec<String> = desc
                    .get("columns")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
                    .filter_map(|c| c.get("name").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect();
                if names.is_empty() {
                    return Err(RpcError::new("invalid_request", "table has no columns"));
                }
                projection = names
                    .into_iter()
                    .map(|name| SelectItem {
                        expr: Expr::Col {
                            col: name,
                            table: None,
                        },
                        r#as: None,
                    })
                    .collect();
            }
            let query = Query {
                with: Vec::new(),
                body: Box::new(QueryBody::Select {
                    select: Box::new(SelectBody {
                        distinct: None,
                        projection,
                        from: Some(vec![TableRef::Base(table.clone())]),
                        r#where: where_expr,
                        group_by: None,
                        having: None,
                    }),
                }),
                order_by,
                limit,
                lock: None,
            };
            let fmt = params.result_format.unwrap_or(ResultFormat::RowsJson);
            let result = eng
                .query_select(&query, &[], fmt, false, None, None, None, false)
                .map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "select",
                "read_only": true,
                "query": query,
                "result": result
            }))
        }
        SqlPlan::ShowDatabases => {
            let eng = state.engine.read().await;
            let dbs = eng.list_databases();
            let rows: Vec<Vec<Value>> = dbs.into_iter().map(|db| vec![Value::String(db)]).collect();
            Ok(serde_json::json!({
                "statement": "show_databases",
                "read_only": true,
                "result": {
                    "data": {
                        "columns": [{"name":"Database"}],
                        "rows": rows
                    }
                }
            }))
        }
        SqlPlan::ShowTables { db } => {
            let eng = state.engine.read().await;
            let tables = eng.list_tables(&db).map_err(to_rpc_error)?;
            let rows: Vec<Vec<Value>> = tables
                .into_iter()
                .map(|table| vec![Value::String(table)])
                .collect();
            Ok(serde_json::json!({
                "statement": "show_tables",
                "read_only": true,
                "db": db,
                "result": {
                    "data": {
                        "columns": [{"name":"Table"}],
                        "rows": rows
                    }
                }
            }))
        }
        SqlPlan::ShowColumns { table } => {
            let eng = state.engine.read().await;
            let desc = eng
                .describe_table(&table.db, &table.table)
                .map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "show_columns",
                "read_only": true,
                "table": table,
                "result": desc
            }))
        }
        SqlPlan::UseDb { db } => Ok(serde_json::json!({
            "statement": "use",
            "read_only": true,
            "default_db": db,
            "ok": true,
            "hint": "sql.exec is stateless; client should remember default_db for later calls"
        })),
        SqlPlan::CreateDatabase { db, if_not_exists } => {
            let mut eng = state.engine.write().await;
            if if_not_exists && eng.list_databases().iter().any(|d| d == &db) {
                return Ok(serde_json::json!({
                    "statement": "create_database",
                    "ok": true,
                    "db": db,
                    "if_not_exists": true,
                    "skipped": true
                }));
            }
            eng.create_database(&db).map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "create_database",
                "ok": true,
                "db": db,
                "if_not_exists": if_not_exists
            }))
        }
        SqlPlan::CreateTable {
            table,
            columns,
            primary_key,
            if_not_exists,
        } => {
            let cols: Vec<ColumnSchema> = columns
                .iter()
                .map(|c| ColumnSchema {
                    name: c.name.clone(),
                    r#type: c.r#type.clone(),
                    nullable: c.nullable,
                    auto_increment: c.auto_increment,
                })
                .collect();
            let mut eng = state.engine.write().await;
            eng.create_table(
                &table.db,
                &table.table,
                cols,
                primary_key,
                if_not_exists,
                None,
            )
            .map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "create_table",
                "ok": true,
                "table": table,
                "if_not_exists": if_not_exists
            }))
        }
        SqlPlan::Insert { table, rows } => {
            let mut eng = state.engine.write().await;
            let r = eng.data_insert(&table, rows, None).map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "insert",
                "ok": true,
                "table": table,
                "write": r
            }))
        }
        SqlPlan::Update {
            table,
            set,
            where_expr,
            limit,
        } => {
            let mut eng = state.engine.write().await;
            let r = eng
                .data_update(&table, &where_expr, &set, limit, None, &[])
                .map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "update",
                "ok": true,
                "table": table,
                "write": r
            }))
        }
        SqlPlan::Delete {
            table,
            where_expr,
            limit,
        } => {
            let mut eng = state.engine.write().await;
            let r = eng
                .data_delete(&table, &where_expr, limit, &[])
                .map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "delete",
                "ok": true,
                "table": table,
                "write": r
            }))
        }
    }
}

fn is_read_only_method(method: &str) -> bool {
    matches!(
        method,
        "system.ping"
            | "system.version"
            | "system.capabilities"
            | "transport.capabilities"
            | "stats.snapshot"
            | "settings.get"
            | "cluster.status"
            | "cluster.nodes"
            | "schema.list_databases"
            | "schema.list_tables"
            | "schema.describe_table"
            | "schema.merge_status"
            | "advisor.index_synthesize"
            | "advisor.history"
            | "migration.intent_report"
            | "migration.rewrite_preview"
            | "data.get"
            | "vector.search"
            | "vector.index.status"
            | "ai.autoparam.classify"
            | "ai.autoparam.analyze"
            | "ai.nl.translate"
            | "ai.nl.explain"
            | "ai.nl.execute"
            | "query.select"
            | "query.patch"
            | "query.execute_prepared"
            | "oblivious.policy.get"
            | "oblivious.explain"
            | "forensic.query"
            | "forensic.verify"
            | "forensic.export"
            | "edge.bundle.request"
            | "edge.bundle.status"
            | "wasm.plan.compile"
            | "wasm.plan.run"
            | "view.status"
            | "view.explain_deps"
            | "cdc.poll"
    )
}

fn transport_capabilities(state: &AppState) -> Value {
    serde_json::json!({
        "http": state.transport.http,
        "quic": state.transport.quic,
    })
}

fn system_capabilities(state: &AppState) -> Value {
    let methods = vec![
        "system.ping",
        "system.version",
        "system.capabilities",
        "transport.capabilities",
        "stats.snapshot",
        "settings.get",
        "settings.set",
        "cluster.status",
        "cluster.nodes",
        "cluster.join_token.create",
        "cluster.node.join",
        "cluster.node.remove",
        "cluster.replica.promote",
        "cluster.shard.create",
        "cluster.shard.move",
        "cluster.shard.rebalance",
        "schema.list_databases",
        "schema.create_database",
        "schema.list_tables",
        "schema.create_table",
        "schema.describe_table",
        "schema.propose_change",
        "schema.merge_status",
        "schema.apply_merge",
        "advisor.index_synthesize",
        "advisor.apply_index",
        "advisor.dismiss",
        "advisor.history",
        "migration.intent_report",
        "migration.rewrite_preview",
        "sql.exec",
        "data.get",
        "data.insert",
        "data.update",
        "data.delete",
        "vector.insert",
        "vector.search",
        "vector.index.status",
        "ai.autoparam.classify",
        "ai.autoparam.analyze",
        "ai.nl.translate",
        "ai.nl.explain",
        "ai.nl.execute",
        "query.prepare",
        "query.execute_prepared",
        "query.select",
        "query.patch",
        "dp.aggregate",
        "dp.budget.set",
        "dp.budget.get",
        "dp.audit.log",
        "oblivious.policy.set",
        "oblivious.policy.get",
        "oblivious.explain",
        "forensic.query",
        "forensic.verify",
        "forensic.export",
        "edge.bundle.request",
        "edge.bundle.apply",
        "edge.bundle.status",
        "merge.register",
        "merge.apply",
        "merge.simulate",
        "merge.wasm.register",
        "merge.wasm.list",
        "merge.wasm.drop",
        "wasm.plan.compile",
        "wasm.plan.run",
        "view.create",
        "view.drop",
        "view.refresh",
        "view.status",
        "view.explain_deps",
        "cdc.subscribe_table",
        "cdc.poll",
    ];
    serde_json::json!({
        "mysql_compat": false,
        "skeinql": true,
        "etag_queries": true,
        "causal_etags": true,
        "wasm_udf": false,
        "wasm_operators": true,
        "merge_wasm_registry": true,
        "column_snapshots": true,
        "audit_wal": false,
        "cluster": true,
        "dp": true,
        "oblivious": true,
        "forensic": true,
        "merge": true,
        "views": true,
        "wire": {"skeinpack_v1": true},
        "transport": transport_capabilities(state),
        "methods": methods
    })
}

async fn stats_snapshot(state: &AppState) -> Value {
    // sysinfo is cross-platform; we keep fields minimal and optional.
    let mut sys = System::new();
    sys.refresh_processes();
    sys.refresh_memory();
    sys.refresh_cpu();

    let pid = Pid::from_u32(std::process::id());
    let proc = sys.process(pid);

    let (cpu_pct, rss_bytes) = match proc {
        Some(p) => {
            // `cpu_usage` is a percentage of a single core in sysinfo.
            let cpu = p.cpu_usage() as f64;
            let rss = p.memory() as u64 * 1024;
            (cpu, rss)
        }
        None => (0.0, 0),
    };

    let uptime_s = state.started.elapsed().as_secs();
    let storage = {
        let eng = state.engine.read().await;
        eng.storage_stats_snapshot()
    };
    let cluster = state.cluster.lock().unwrap();

    serde_json::json!({
        "uptime_s": uptime_s,
        "sessions": {"active": 0, "total": 0},
        "qps": 0,
        "tps": 0,
        "process": {"cpu_pct": cpu_pct, "rss_bytes": rss_bytes},
        "storage": {
            "wal_bytes": storage.wal_bytes,
            "dedup_ratio": storage.dedup_ratio,
            "logical_bytes": storage.logical_bytes,
            "unique_bytes": storage.unique_bytes,
            "duplicate_bytes": storage.duplicate_bytes,
            "unique_values": storage.unique_values,
            "interned_values": storage.interned_values
        },
        "background": {"compaction": "idle", "snapshots": "idle"},
        "cluster": {
            "enabled": cluster.enabled,
            "local_node_id": cluster.local_node_id,
            "primary_node_id": cluster.primary_node_id,
            "nodes": cluster.nodes.len(),
            "shards": cluster.shards.len(),
            "replication": cluster.replication
        }
    })
}

fn handle_settings_get(state: &AppState, params: Option<Value>) -> Result<Value, RpcError> {
    let keys = params
        .as_ref()
        .and_then(|v| v.get("keys"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| RpcError::new("invalid_request", "settings.get requires params.keys[]"))?;

    let settings = state.settings.lock().unwrap();
    let mut out = serde_json::Map::new();
    for k in keys {
        if let Some(ks) = k.as_str() {
            if let Some(v) = settings.get(ks) {
                out.insert(ks.to_string(), v.clone());
            }
        }
    }
    Ok(Value::Object(out))
}

fn handle_settings_set(state: &AppState, params: Option<Value>) -> Result<Value, RpcError> {
    let obj = params.and_then(|v| v.as_object().cloned()).ok_or_else(|| {
        RpcError::new("invalid_request", "settings.set requires an object params")
    })?;

    {
        let mut settings = state.settings.lock().unwrap();
        for (k, v) in obj.iter() {
            settings.insert(k.clone(), v.clone());
        }
    }

    if let Some(v) = obj.get(CLUSTER_STATE_KEY) {
        let parsed: ClusterStateModel = serde_json::from_value(v.clone()).map_err(|e| {
            RpcError::new("invalid_request", format!("invalid cluster state: {}", e))
        })?;
        {
            let mut cluster = state.cluster.lock().unwrap();
            *cluster = parsed;
        }
    } else if let Some(v) = obj.get("cluster.enabled").and_then(|v| v.as_bool()) {
        let mut cluster = state.cluster.lock().unwrap();
        cluster.enabled = v;
    }

    if let Err(e) = persist_cluster_state(state) {
        tracing::warn!(error = %e, "failed to persist cluster state");
    }

    // Persist best-effort.
    if let Err(e) = save_settings(state) {
        tracing::warn!(error = %e, "failed to persist settings");
    }

    Ok(serde_json::json!({"ok": true}))
}

fn settings_path(state: &AppState) -> PathBuf {
    state.data_dir.join("settings.json")
}

fn load_settings(state: &AppState) -> anyhow::Result<()> {
    let path = settings_path(state);
    if !path.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(&path)?;
    let json: Value = serde_json::from_slice(&bytes)?;
    if let Some(obj) = json.as_object() {
        let mut settings = state.settings.lock().unwrap();
        *settings = obj.clone();
    }
    Ok(())
}

fn load_cluster_state(state: &AppState) -> anyhow::Result<()> {
    let loaded = {
        let settings = state.settings.lock().unwrap();
        settings.get(CLUSTER_STATE_KEY).cloned()
    };
    if let Some(v) = loaded {
        if let Ok(parsed) = serde_json::from_value::<ClusterStateModel>(v) {
            let mut cluster = state.cluster.lock().unwrap();
            *cluster = parsed;
        }
    }

    let now = now_unix_ms_u64();
    {
        let mut cluster = state.cluster.lock().unwrap();
        cluster.cleanup_join_tokens(now);
        let local_node_id = cluster.local_node_id.clone();
        let primary_node_id = cluster.primary_node_id.clone();
        if let Some(local) = cluster
            .nodes
            .iter_mut()
            .find(|n| n.node_id == local_node_id)
        {
            local.rpc_url = state.local_rpc_url.clone();
            local.status = "online".to_string();
            local.last_seen_ms = now;
        } else {
            cluster.nodes.push(ClusterNode {
                node_id: local_node_id.clone(),
                rpc_url: state.local_rpc_url.clone(),
                role: if local_node_id == primary_node_id {
                    "primary".to_string()
                } else {
                    "replica".to_string()
                },
                status: "online".to_string(),
                joined_at_ms: now,
                last_seen_ms: now,
            });
        }
    }
    persist_cluster_state(state)?;
    Ok(())
}

fn persist_cluster_state(state: &AppState) -> anyhow::Result<()> {
    let (cluster_value, enabled) = {
        let cluster = state.cluster.lock().unwrap();
        (serde_json::to_value(&*cluster)?, cluster.enabled)
    };
    {
        let mut settings = state.settings.lock().unwrap();
        settings.insert(CLUSTER_STATE_KEY.to_string(), cluster_value);
        settings.insert("cluster.enabled".to_string(), Value::Bool(enabled));
    }
    save_settings(state)?;
    Ok(())
}

fn save_settings(state: &AppState) -> anyhow::Result<()> {
    let path = settings_path(state);
    let settings = state.settings.lock().unwrap();
    let bytes = serde_json::to_vec_pretty(&Value::Object(settings.clone()))?;
    std::fs::write(&path, bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine as _;
    use http_body_util::BodyExt;
    use serde_json::json;
    use skeindb_skeinql::methods::RowObject;
    use skeindb_skeinql::types::{
        BaseTableRef, Expr, Query, QueryBody, SelectBody, SelectItem, TableRef,
    };
    use skeindb_skeinql::{RpcId, RpcRequest, RpcResponse};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn embedded_admin_assets_present() {
        let html = admin_index_html();
        assert!(html.contains("SkeinAdmin"));
        assert!(html.contains("Feature Center"));
        assert!(html.contains("RPC Explorer"));
        assert!(html.contains("Easy Viewer"));
        assert!(html.contains("Click-First Database Control"));
        assert!(html.contains("Visual Row Editor (Optional)"));
        assert!(html.contains("Inline grid edit mode"));
        assert!(!html.to_lowercase().contains("phpmyadmin"));
        assert!(html.contains("src/main.js"));
        let js = admin_main_js();
        assert!(js.contains("system.capabilities"));
        assert!(js.contains("easyCreateTable"));
        assert!(js.contains("easyVisualSaveChanges"));
        assert!(js.contains("easyGridSaveRow"));
    }

    fn type_desc(kind: &str) -> skeindb_skeinql::types::TypeDesc {
        skeindb_skeinql::types::TypeDesc {
            kind: kind.to_string(),
            max: None,
            precision: None,
            scale: None,
            charset: None,
            collation: None,
            unsigned: None,
        }
    }

    fn row(entries: &[(&str, Lit)]) -> RowObject {
        let mut out = RowObject::new();
        for (k, v) in entries.iter() {
            out.insert((*k).to_string(), v.clone());
        }
        out
    }

    fn select_query(
        db: &str,
        table: &str,
        projection: Vec<&str>,
        predicate: Option<Expr>,
    ) -> Query {
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

    fn read_u32_le(bytes: &[u8], offset: usize) -> u32 {
        let mut buf = [0u8; 4];
        if offset + 4 <= bytes.len() {
            buf.copy_from_slice(&bytes[offset..offset + 4]);
        }
        u32::from_le_bytes(buf)
    }

    fn eq_expr(col: &str, value: Lit) -> Expr {
        Expr::Op {
            op: "eq".to_string(),
            a: Some(Box::new(Expr::Col {
                col: col.to_string(),
                table: None,
            })),
            b: Some(Box::new(Expr::Lit { lit: value })),
            args: None,
            list: None,
            lo: None,
            hi: None,
        }
    }

    fn gt_expr(col: &str, value: Lit) -> Expr {
        Expr::Op {
            op: "gt".to_string(),
            a: Some(Box::new(Expr::Col {
                col: col.to_string(),
                table: None,
            })),
            b: Some(Box::new(Expr::Lit { lit: value })),
            args: None,
            list: None,
            lo: None,
            hi: None,
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "skeindb_server_test_{}_{}_{}",
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

    fn build_state(dir: PathBuf, engine: Engine) -> AppState {
        let local_rpc_url = "http://127.0.0.1:8080".to_string();
        let local_node_id = "node-test".to_string();
        AppState {
            started: Instant::now(),
            data_dir: dir,
            local_rpc_url: local_rpc_url.clone(),
            settings: Arc::new(Mutex::new(serde_json::Map::new())),
            cluster: Arc::new(Mutex::new(ClusterStateModel::bootstrap(
                local_node_id,
                local_rpc_url,
            ))),
            counters: Arc::new(Mutex::new(Counters::default())),
            engine: Arc::new(RwLock::new(engine)),
            subs: Arc::new(Mutex::new(Subscriptions::default())),
            coalesce: Arc::new(QueryCoalescer::default()),
            transport: TransportCapabilities {
                http: true,
                quic: false,
            },
        }
    }

    async fn call_rpc(state: &AppState, method: &str, params: Value) -> RpcResponse {
        let req = RpcRequest {
            skeinql: SKEINQL_VERSION.to_string(),
            id: Some(RpcId::Str("t1".to_string())),
            method: method.to_string(),
            params: Some(params),
        };
        let resp = rpc_handler(State(state.clone()), HeaderMap::new(), Json(req)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("parse rpc response")
    }

    #[tokio::test]
    async fn dp_rpc_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("dp_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "events",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "value".to_string(),
                    r#type: type_desc("f64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;

        let rows = vec![
            row(&[("id", Lit::U64 { v: 1 }), ("value", Lit::F64 { v: 2.0 })]),
            row(&[("id", Lit::U64 { v: 2 }), ("value", Lit::F64 { v: 3.0 })]),
        ];
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "events".to_string(),
                r#as: None,
            },
            rows,
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "dp.budget.set",
            json!({
                "principal": "analyst",
                "total_epsilon": 1.0,
                "total_delta": 1e-6
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "dp.aggregate",
            json!({
                "table": {"db":"app","table":"events"},
                "aggregates": [{"op":"count"}],
                "epsilon": 0.5,
                "delta": 1e-6,
                "principal": "analyst",
                "seed": 7
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        assert_eq!(
            result
                .get("columns")
                .and_then(|v| v.as_array())
                .map(|v| v.len()),
            Some(1)
        );
        assert_eq!(
            result
                .get("rows")
                .and_then(|v| v.as_array())
                .map(|v| v.len()),
            Some(1)
        );
        let remaining = result["privacy"]["budget"]["remaining_epsilon"]
            .as_f64()
            .unwrap_or_default();
        assert!(remaining < 1.0);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn ai_autoparam_classify_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("ai_autoparam");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "ai.autoparam.classify",
            json!({
                "sql": "select * from users where status = 'active' and id = 42",
                "literals": [
                    {"value": {"t":"str","v":"active"}, "column":"status", "table":"users", "op":"eq"},
                    {"value": {"t":"u64","v":42}, "column":"id", "table":"users", "op":"eq"}
                ]
            }),
        )
        .await;
        assert!(resp.ok);
        let labels = resp
            .result
            .unwrap_or_default()
            .get("labels")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0]["decision"], "semantic_constant");
        assert_eq!(labels[1]["decision"], "parameterize");

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn ai_autoparam_analyze_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("ai_autoparam_analyze");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "status".to_string(),
                    r#type: type_desc("str"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;
        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "ai.autoparam.analyze",
            json!({
                "db": "app",
                "sql": "select * from users where status = 'active' and id = 42"
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.unwrap_or_default();
        assert_eq!(
            result["normalized_sql"],
            "select * from users where status = ? and id = ?"
        );
        let labels = result
            .get("labels")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0]["decision"], "semantic_constant");
        assert_eq!(labels[1]["decision"], "parameterize");

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn ai_nl_translate_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("ai_nl_translate");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![ColumnSchema {
                name: "id".to_string(),
                r#type: type_desc("u64"),
                nullable: false,
                auto_increment: false,
            }],
            vec!["id".to_string()],
            false,
            None,
        )?;
        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "ai.nl.translate",
            json!({
                "db": "app",
                "request": "list users"
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.unwrap_or_default();
        let package = result.get("package").unwrap();
        assert_eq!(package.get("db"), Some(&json!("app")));
        assert_eq!(
            package
                .get("tables")
                .and_then(|v| v.as_array())
                .map(|v| v.len()),
            Some(1)
        );
        assert!(result.get("query").is_some());

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn ai_nl_execute_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("ai_nl_execute");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![ColumnSchema {
                name: "id".to_string(),
                r#type: type_desc("u64"),
                nullable: false,
                auto_increment: false,
            }],
            vec!["id".to_string()],
            false,
            None,
        )?;
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![row(&[("id", Lit::U64 { v: 7 })])],
            None,
        )?;
        let state = build_state(dir.clone(), engine);

        let explain_resp = call_rpc(
            &state,
            "ai.nl.explain",
            json!({
                "query": {
                    "body": {"select": {"projection":[{"expr":{"col":"id"}}],"from":[{"db":"app","table":"users"}]}}
                },
                "preview_limit": 1,
                "preview_format": "objects_json"
            }),
        )
        .await;
        assert!(explain_resp.ok);
        let approval = explain_resp
            .result
            .clone()
            .unwrap_or_default()
            .get("approval_token")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        assert!(!approval.is_empty());

        let exec_resp = call_rpc(
            &state,
            "ai.nl.execute",
            json!({
                "query": {
                    "body": {"select": {"projection":[{"expr":{"col":"id"}}],"from":[{"db":"app","table":"users"}]}}
                },
                "approval_token": approval,
                "result_format": "objects_json"
            }),
        )
        .await;
        assert!(exec_resp.ok);
        let data = exec_resp
            .result
            .unwrap_or_default()
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["id"]["v"], 7);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn oblivious_policy_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("oblivious_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![ColumnSchema {
                name: "id".to_string(),
                r#type: type_desc("u64"),
                nullable: false,
                auto_increment: false,
            }],
            vec!["id".to_string()],
            false,
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "oblivious.policy.set",
            json!({
                "table": {"db":"app","table":"users"},
                "policy": {"level":"basic","pad_to_multiple":4,"dummy_value_lookups":2}
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "oblivious.explain",
            json!({
                "table": {"db":"app","table":"users"}
            }),
        )
        .await;
        assert!(resp.ok);
        let plan = resp.result.expect("missing result")["plan"].clone();
        assert_eq!(plan.get("level").and_then(|v| v.as_str()), Some("basic"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn forensic_query_and_verify_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("forensic_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "events",
            vec![ColumnSchema {
                name: "id".to_string(),
                r#type: type_desc("u64"),
                nullable: false,
                auto_increment: false,
            }],
            vec!["id".to_string()],
            false,
            None,
        )?;

        let rows = vec![
            row(&[("id", Lit::U64 { v: 1 })]),
            row(&[("id", Lit::U64 { v: 2 })]),
        ];
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "events".to_string(),
                r#as: None,
            },
            rows,
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(&state, "forensic.query", json!({})).await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        let records = result["records"].clone();
        let start_hash = result["proof"]["preceding_hash"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "genesis".to_string());

        let resp = call_rpc(
            &state,
            "forensic.verify",
            json!({
                "records": records,
                "start_hash": start_hash
            }),
        )
        .await;
        assert!(resp.ok);
        assert!(resp.result.expect("missing result")["ok"]
            .as_bool()
            .unwrap_or(false));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn merge_policy_and_apply_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("merge_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "counters",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "count".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "counters".to_string(),
                r#as: None,
            },
            vec![row(&[
                ("id", Lit::U64 { v: 1 }),
                ("count", Lit::U64 { v: 5 }),
            ])],
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "merge.register",
            json!({
                "table": {"db":"app","table":"counters"},
                "policy": {
                    "default": {"kind":"builtin","name":"last_write_wins"},
                    "per_column": {"count": {"kind":"builtin","name":"sum"}}
                }
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "data.get",
            json!({
                "table": {"db":"app","table":"counters"},
                "pk": [{"t":"u64","v":1}]
            }),
        )
        .await;
        assert!(resp.ok);
        let etag = resp.result.expect("missing result")["etag"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let resp = call_rpc(
            &state,
            "data.update",
            json!({
                "table": {"db":"app","table":"counters"},
                "set": {"count": {"t":"u64","v":10}},
                "where": eq_expr("id", Lit::U64 { v: 1 })
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "merge.apply",
            json!({
                "table": {"db":"app","table":"counters"},
                "pk": [{"t":"u64","v":1}],
                "incoming": {"count": {"t":"u64","v":3}},
                "expected_etag": etag
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        assert_eq!(result["conflict"].as_bool(), Some(true));
        assert_eq!(result["applied"].as_bool(), Some(true));
        assert_eq!(result["merged"]["count"]["v"].as_u64(), Some(13));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn merge_apply_min_causality_conflict() -> anyhow::Result<()> {
        let dir = temp_dir("merge_rpc_causality");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "counters",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "count".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;

        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "counters".to_string(),
                r#as: None,
            },
            vec![row(&[
                ("id", Lit::U64 { v: 1 }),
                ("count", Lit::U64 { v: 5 }),
            ])],
            None,
        )?;

        let state = build_state(dir.clone(), engine);
        let stale = json!({
            "format": "etag_chain_v1",
            "deps": [{
                "table": "app.counters",
                "v": 9_999_999
            }]
        });

        let resp = call_rpc(
            &state,
            "merge.apply",
            json!({
                "table": {"db":"app","table":"counters"},
                "pk": [{"t":"u64","v":1}],
                "incoming": {"count": {"t":"u64","v":2}},
                "min_causality": stale
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        assert_eq!(result["applied"].as_bool(), Some(false));
        assert_eq!(result["conflict"].as_bool(), Some(true));
        assert!(result["conflicts"]
            .as_array()
            .map(|arr| arr.iter().any(|v| v.as_str() == Some("dependency")))
            .unwrap_or(false));

        let resp = call_rpc(
            &state,
            "data.get",
            json!({
                "table": {"db":"app","table":"counters"},
                "pk": [{"t":"u64","v":1}]
            }),
        )
        .await;
        assert!(resp.ok);
        let count = resp.result.expect("missing result")["row"]["count"]["v"].as_u64();
        assert_eq!(count, Some(5));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn merge_apply_constraint_conflict_rpc() -> anyhow::Result<()> {
        let dir = temp_dir("merge_rpc_constraint");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "counters",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "count".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "merge.apply",
            json!({
                "table": {"db":"app","table":"counters"},
                "pk": [{"t":"u64","v":1}],
                "incoming": {}
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        assert_eq!(result["applied"].as_bool(), Some(false));
        assert_eq!(result["conflict"].as_bool(), Some(true));
        assert!(result["conflicts"]
            .as_array()
            .map(|arr| arr.iter().any(|v| v.as_str() == Some("constraint")))
            .unwrap_or(false));

        let resp = call_rpc(
            &state,
            "data.get",
            json!({
                "table": {"db":"app","table":"counters"},
                "pk": [{"t":"u64","v":1}]
            }),
        )
        .await;
        assert!(!resp.ok);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn merge_wasm_registry_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("merge_wasm_registry");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "merge.wasm.register",
            json!({
                "module_id": "merge_sum",
                "wasm_b64": "AA==",
                "capabilities": {
                    "values_only": true,
                    "deterministic": true,
                    "max_fuel": 1000,
                    "max_memory_bytes": 65536,
                    "max_output_bytes": 4096
                }
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(&state, "merge.wasm.list", json!({})).await;
        assert!(resp.ok);
        let modules = resp.result.expect("missing result")["modules"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0]["module_id"].as_str(), Some("merge_sum"));

        let resp = call_rpc(
            &state,
            "merge.wasm.drop",
            json!({
                "module_id": "merge_sum"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(&state, "merge.wasm.list", json!({})).await;
        assert!(resp.ok);
        let modules = resp.result.expect("missing result")["modules"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert!(modules.is_empty());

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn merge_apply_wasm_policy_not_supported_rpc() -> anyhow::Result<()> {
        let dir = temp_dir("merge_apply_wasm_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "counters",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "count".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "counters".to_string(),
                r#as: None,
            },
            vec![row(&[
                ("id", Lit::U64 { v: 1 }),
                ("count", Lit::U64 { v: 5 }),
            ])],
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "merge.wasm.register",
            json!({
                "module_id": "merge_sum",
                "wasm_b64": "AA==",
                "capabilities": {
                    "values_only": true,
                    "deterministic": true,
                    "max_fuel": 1000,
                    "max_memory_bytes": 65536,
                    "max_output_bytes": 4096
                }
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "merge.register",
            json!({
                "table": {"db":"app","table":"counters"},
                "policy": {
                    "default": {"kind":"wasm","name":"merge_sum","module":"merge_sum"}
                }
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "merge.apply",
            json!({
                "table": {"db":"app","table":"counters"},
                "pk": [{"t":"u64","v":1}],
                "incoming": {"count": {"t":"u64","v":2}}
            }),
        )
        .await;
        assert!(!resp.ok);
        assert_eq!(
            resp.error.as_ref().map(|err| err.code.as_str()),
            Some("not_supported")
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn wasm_plan_compile_run_rpc() -> anyhow::Result<()> {
        let dir = temp_dir("wasm_plan_compile_run_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "score".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;

        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![
                row(&[("id", Lit::U64 { v: 1 }), ("score", Lit::U64 { v: 3 })]),
                row(&[("id", Lit::U64 { v: 2 }), ("score", Lit::U64 { v: 8 })]),
            ],
            None,
        )?;

        let state = build_state(dir.clone(), engine);

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

        let resp = call_rpc(
            &state,
            "wasm.plan.compile",
            json!({
                "query": query
            }),
        )
        .await;
        assert!(resp.ok);
        let artifact_b64 = resp.result.expect("missing result")["artifact_b64"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let resp = call_rpc(
            &state,
            "wasm.plan.run",
            json!({
                "artifact_b64": artifact_b64,
                "args": [{"t":"u64","v":7}],
                "result_format": "objects_json"
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        let rows = result["data"].as_array().cloned().unwrap_or_default();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"]["v"].as_u64(), Some(2));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn wasm_plan_run_batch_rpc() -> anyhow::Result<()> {
        let dir = temp_dir("wasm_plan_run_batch_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "score".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;

        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![
                row(&[("id", Lit::U64 { v: 1 }), ("score", Lit::U64 { v: 3 })]),
                row(&[("id", Lit::U64 { v: 2 }), ("score", Lit::U64 { v: 8 })]),
            ],
            None,
        )?;

        let state = build_state(dir.clone(), engine);

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

        let resp = call_rpc(
            &state,
            "wasm.plan.compile",
            json!({
                "query": query
            }),
        )
        .await;
        assert!(resp.ok);
        let artifact_b64 = resp.result.expect("missing result")["artifact_b64"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let resp = call_rpc(
            &state,
            "wasm.plan.run",
            json!({
                "artifact_b64": artifact_b64,
                "args": [{"t":"u64","v":7}],
                "result_format": "wasm_batch_v1"
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        let data = result["data"].as_object().cloned().unwrap_or_default();
        assert_eq!(
            data.get("format").and_then(|v| v.as_str()),
            Some("skein.wasm.batch.v1")
        );
        let columns = data
            .get("columns")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0]["name"].as_str(), Some("id"));
        assert_eq!(columns[1]["name"].as_str(), Some("score"));

        let batch_b64 = data
            .get("batch_b64")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let bytes = BASE64_STANDARD.decode(batch_b64.as_bytes())?;
        assert!(bytes.len() >= 20);
        let magic = read_u32_le(&bytes, 0);
        assert_eq!(magic, 0x31424B53);
        let row_count = read_u32_le(&bytes, 8);
        let column_count = read_u32_le(&bytes, 12);
        assert_eq!(row_count, 1);
        assert_eq!(column_count, 2);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn query_select_min_causality_enforced() -> anyhow::Result<()> {
        let dir = temp_dir("query_select_min_causality");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![ColumnSchema {
                name: "id".to_string(),
                r#type: type_desc("u64"),
                nullable: false,
                auto_increment: false,
            }],
            vec!["id".to_string()],
            false,
            None,
        )?;

        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![row(&[("id", Lit::U64 { v: 1 })])],
            None,
        )?;

        let state = build_state(dir.clone(), engine);
        let query = select_query("app", "users", vec!["id"], None);

        let resp = call_rpc(
            &state,
            "query.select",
            json!({
                "query": query.clone(),
                "cache": {"want_etag": true}
            }),
        )
        .await;
        assert!(resp.ok);

        let result = resp.result.expect("missing result");
        let token = result["causality"].clone();
        let etag = result["etag"].as_str().unwrap_or_default().to_string();

        let resp = call_rpc(
            &state,
            "query.select",
            json!({
                "query": query.clone(),
                "cache": {"min_causality": token.clone(), "if_none_match": etag}
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        assert_eq!(result["not_modified"].as_bool(), Some(true));

        let mut bad = token;
        let next = bad["deps"][0]["v"].as_u64().unwrap_or(0) + 1;
        bad["deps"][0]["v"] = serde_json::Value::from(next);

        let resp = call_rpc(
            &state,
            "query.select",
            json!({
                "query": query,
                "cache": {"min_causality": bad, "if_none_match": etag}
            }),
        )
        .await;
        assert!(!resp.ok);
        assert_eq!(
            resp.error.as_ref().map(|e| e.code.as_str()),
            Some("precondition_failed")
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn edge_bundle_roundtrip_and_routing() -> anyhow::Result<()> {
        let dir = temp_dir("edge_bundle_roundtrip");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![ColumnSchema {
                name: "id".to_string(),
                r#type: type_desc("u64"),
                nullable: false,
                auto_increment: false,
            }],
            vec!["id".to_string()],
            false,
            None,
        )?;

        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![row(&[("id", Lit::U64 { v: 1 })])],
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "edge.bundle.request",
            json!({
                "windows": [{"table": {"db":"app","table":"users"}, "from_seq": 0}],
                "redaction": {"mode":"hash_pk"}
            }),
        )
        .await;
        assert!(resp.ok);
        let bundle = resp.result.expect("missing result")["bundle"].clone();

        let resp = call_rpc(&state, "edge.bundle.apply", json!({ "bundle": bundle })).await;
        assert!(resp.ok);

        let query = select_query("app", "users", vec!["id"], None);
        let resp = call_rpc(
            &state,
            "edge.bundle.status",
            json!({ "query": query.clone(), "max_lag": 0 }),
        )
        .await;
        assert!(resp.ok);
        let route = resp.result.expect("missing result")["route"].clone();
        assert_eq!(route["eligible"].as_bool(), Some(true));

        let resp = call_rpc(
            &state,
            "data.insert",
            json!({
                "into": {"db":"app","table":"users"},
                "rows": [{"id": {"t":"u64","v":2}}]
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "edge.bundle.status",
            json!({ "query": query, "max_lag": 0 }),
        )
        .await;
        assert!(resp.ok);
        let route = resp.result.expect("missing result")["route"].clone();
        assert_eq!(route["eligible"].as_bool(), Some(false));
        assert_eq!(route["reason"].as_str(), Some("stale"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn schema_evolution_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("schema_evolution_roundtrip");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![ColumnSchema {
                name: "id".to_string(),
                r#type: type_desc("u64"),
                nullable: false,
                auto_increment: false,
            }],
            vec!["id".to_string()],
            false,
            None,
        )?;

        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![row(&[("id", Lit::U64 { v: 1 })])],
            None,
        )?;

        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "schema.propose_change",
            json!({
                "table": {"db":"app","table":"users"},
                "base_version": 1,
                "changes": [{
                    "op": "add_column",
                    "name": "region",
                    "type": {"kind":"str"},
                    "nullable": true,
                    "auto_increment": false,
                    "default": {"t":"str","v":"eu"}
                }]
            }),
        )
        .await;
        assert!(resp.ok);
        let change_id = resp.result.expect("missing result")["change_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let resp = call_rpc(
            &state,
            "schema.merge_status",
            json!({"table": {"db":"app","table":"users"}}),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        let has_plan = result["merge_plan"]
            .as_array()
            .map(|arr| arr.iter().any(|v| v.as_str() == Some(change_id.as_str())))
            .unwrap_or(false);
        assert!(has_plan);

        let resp = call_rpc(
            &state,
            "schema.apply_merge",
            json!({
                "table": {"db":"app","table":"users"},
                "change_ids": [change_id]
            }),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        assert_eq!(result["new_version"].as_u64(), Some(2));

        let resp = call_rpc(
            &state,
            "schema.describe_table",
            json!({"db":"app","table":"users"}),
        )
        .await;
        assert!(resp.ok);
        let result = resp.result.expect("missing result");
        let has_column = result["columns"]
            .as_array()
            .map(|arr| arr.iter().any(|col| col["name"].as_str() == Some("region")))
            .unwrap_or(false);
        assert!(has_column);

        let resp = call_rpc(
            &state,
            "data.get",
            json!({
                "table": {"db":"app","table":"users"},
                "pk": [{"t":"u64","v":1}]
            }),
        )
        .await;
        assert!(resp.ok);
        let row = resp.result.expect("missing result")["row"].clone();
        assert_eq!(row["region"]["v"].as_str(), Some("eu"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn query_execute_prepared_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("query_execute_prepared");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    r#type: type_desc("str"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;

        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![
                row(&[
                    ("id", Lit::U64 { v: 1 }),
                    (
                        "name",
                        Lit::Str {
                            v: "Ava".to_string(),
                        },
                    ),
                ]),
                row(&[
                    ("id", Lit::U64 { v: 2 }),
                    (
                        "name",
                        Lit::Str {
                            v: "Bo".to_string(),
                        },
                    ),
                ]),
            ],
            None,
        )?;

        let state = build_state(dir.clone(), engine);
        let query = select_query(
            "app",
            "users",
            vec!["id", "name"],
            Some(Expr::Op {
                op: "eq".to_string(),
                a: Some(Box::new(Expr::Col {
                    col: "id".to_string(),
                    table: None,
                })),
                b: Some(Box::new(Expr::Param { param: 0 })),
                args: None,
                list: None,
                lo: None,
                hi: None,
            }),
        );

        let resp = call_rpc(
            &state,
            "query.prepare",
            json!({
                "query": query
            }),
        )
        .await;
        assert!(resp.ok);
        let query_id = resp.result.expect("missing result")["query_id"]
            .as_str()
            .unwrap()
            .to_string();

        let resp = call_rpc(
            &state,
            "query.execute_prepared",
            json!({
                "query_id": query_id,
                "args": [ {"t":"u64","v":2} ]
            }),
        )
        .await;
        assert!(resp.ok);
        let rows = resp.result.expect("missing result")["data"]["rows"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(rows.len(), 1);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn view_create_refresh_and_query_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("view_rpc");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "users",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "city".to_string(),
                    r#type: type_desc("string"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "score".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "users".to_string(),
                r#as: None,
            },
            vec![
                row(&[
                    ("id", Lit::U64 { v: 1 }),
                    (
                        "city",
                        Lit::Str {
                            v: "Oslo".to_string(),
                        },
                    ),
                    ("score", Lit::U64 { v: 5 }),
                ]),
                row(&[
                    ("id", Lit::U64 { v: 2 }),
                    (
                        "city",
                        Lit::Str {
                            v: "Tokyo".to_string(),
                        },
                    ),
                    ("score", Lit::U64 { v: 20 }),
                ]),
            ],
            None,
        )?;

        let state = build_state(dir.clone(), engine);
        let view_query = select_query(
            "app",
            "users",
            vec!["id", "city"],
            Some(gt_expr("score", Lit::U64 { v: 10 })),
        );
        let resp = call_rpc(
            &state,
            "view.create",
            json!({
                "view": {"db":"app","table":"top_users"},
                "query": view_query
            }),
        )
        .await;
        assert!(resp.ok);

        let select_view = select_query("app", "top_users", vec!["id", "city"], None);
        let resp = call_rpc(
            &state,
            "query.select",
            json!({
                "query": select_view
            }),
        )
        .await;
        assert!(resp.ok);
        let rows = resp.result.expect("missing result")["data"]["rows"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(rows.len(), 1);

        let resp = call_rpc(
            &state,
            "data.update",
            json!({
                "table": {"db":"app","table":"users"},
                "set": {"score": {"t":"u64","v":15}},
                "where": eq_expr("id", Lit::U64 { v: 1 })
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "view.refresh",
            json!({
                "view": {"db":"app","table":"top_users"},
                "mode": "incremental"
            }),
        )
        .await;
        assert!(resp.ok);

        let select_view = select_query("app", "top_users", vec!["id", "city"], None);
        let resp = call_rpc(
            &state,
            "query.select",
            json!({
                "query": select_view
            }),
        )
        .await;
        assert!(resp.ok);
        let rows = resp.result.expect("missing result")["data"]["rows"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(rows.len(), 2);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn admin_toolbar_methods_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("toolbar_methods");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let ping = call_rpc(&state, "system.ping", json!({})).await;
        assert!(ping.ok);
        assert_eq!(
            ping.result
                .as_ref()
                .and_then(|v| v.get("pong"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );

        let version = call_rpc(&state, "system.version", json!({})).await;
        assert!(version.ok);
        assert!(version
            .result
            .as_ref()
            .and_then(|v| v.get("version"))
            .and_then(|v| v.as_str())
            .is_some());

        let stats = call_rpc(&state, "stats.snapshot", json!({})).await;
        assert!(stats.ok);
        assert!(stats
            .result
            .as_ref()
            .and_then(|v| v.get("process"))
            .is_some());

        let caps = call_rpc(&state, "system.capabilities", json!({})).await;
        assert!(caps.ok);
        assert!(caps
            .result
            .as_ref()
            .and_then(|v| v.get("methods"))
            .and_then(|v| v.as_array())
            .map(|m| m.iter().any(|method| method == "sql.exec"))
            .unwrap_or(false));

        let transport = call_rpc(&state, "transport.capabilities", json!({})).await;
        assert!(transport.ok);
        assert_eq!(
            transport
                .result
                .as_ref()
                .and_then(|v| v.get("http"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stats_snapshot_reports_dedup_metrics() -> anyhow::Result<()> {
        let dir = temp_dir("stats_dedup_metrics");
        let mut engine = Engine::open(&dir)?;
        engine.create_table(
            "app",
            "events",
            vec![
                ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                ColumnSchema {
                    name: "payload".to_string(),
                    r#type: type_desc("str"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "events".to_string(),
                r#as: None,
            },
            vec![
                row(&[
                    ("id", Lit::U64 { v: 1 }),
                    (
                        "payload",
                        Lit::Str {
                            v: "same-payload".to_string(),
                        },
                    ),
                ]),
                row(&[
                    ("id", Lit::U64 { v: 2 }),
                    (
                        "payload",
                        Lit::Str {
                            v: "same-payload".to_string(),
                        },
                    ),
                ]),
            ],
            None,
        )?;
        let state = build_state(dir.clone(), engine);

        let stats = call_rpc(&state, "stats.snapshot", json!({})).await;
        assert!(stats.ok);
        let storage = stats
            .result
            .as_ref()
            .and_then(|v| v.get("storage"))
            .cloned()
            .unwrap_or_default();
        let logical = storage
            .get("logical_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let unique = storage
            .get("unique_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let duplicate = storage
            .get("duplicate_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let ratio = storage
            .get("dedup_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let interned = storage
            .get("interned_values")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        assert!(logical > unique);
        assert!(duplicate > 0);
        assert!(ratio > 1.0);
        assert!(interned > 0);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn sql_exec_roundtrip_crud_show_and_use() -> anyhow::Result<()> {
        let dir = temp_dir("sql_exec_roundtrip");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE DATABASE app"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "USE app"
            }),
        )
        .await;
        assert!(resp.ok);
        assert_eq!(
            resp.result
                .as_ref()
                .and_then(|v| v.get("default_db"))
                .and_then(|v| v.as_str()),
            Some("app")
        );

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE TABLE users (id bigint, name text, PRIMARY KEY (id))",
                "default_db": "app"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Grace')",
                "default_db": "app"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id, name FROM users WHERE id = 1",
                "default_db": "app"
            }),
        )
        .await;
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

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "UPDATE users SET name = 'Ada Lovelace' WHERE id = 1 LIMIT 1",
                "default_db": "app"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "DELETE FROM users WHERE id = 2 LIMIT 1",
                "default_db": "app"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SHOW TABLES FROM app"
            }),
        )
        .await;
        assert!(resp.ok);
        let table_rows = resp
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(table_rows.iter().any(|row| row
            .as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            == Some("users")));

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SHOW COLUMNS FROM users FROM app"
            }),
        )
        .await;
        assert!(resp.ok);
        let columns = resp
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("columns"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(columns
            .iter()
            .any(|c| c.get("name").and_then(|v| v.as_str()) == Some("id")));

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id FROM users LIMIT 1",
                "default_db": "app",
                "explain": true
            }),
        )
        .await;
        assert!(resp.ok);
        assert_eq!(
            resp.result
                .as_ref()
                .and_then(|v| v.get("statement"))
                .and_then(|v| v.as_str()),
            Some("select")
        );
        assert_eq!(
            resp.result
                .as_ref()
                .and_then(|v| v.get("read_only"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn cluster_control_plane_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("cluster_control");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(&state, "cluster.status", json!({})).await;
        assert!(resp.ok);
        assert_eq!(
            resp.result
                .as_ref()
                .and_then(|v| v.get("enabled"))
                .and_then(|v| v.as_bool()),
            Some(false)
        );

        let resp = call_rpc(&state, "cluster.join_token.create", json!({})).await;
        assert!(resp.ok);
        let token = resp.result.expect("missing result")["token"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(!token.is_empty());

        let resp = call_rpc(
            &state,
            "cluster.node.join",
            json!({
                "token": token,
                "node_id": "replica-a",
                "rpc_url": "http://127.0.0.1:19081",
                "role": "replica"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "cluster.nodes",
            json!({
                "role": "replica"
            }),
        )
        .await;
        assert!(resp.ok);
        let replicas = resp.result.expect("missing result")["nodes"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(replicas.len(), 1);

        let resp = call_rpc(
            &state,
            "cluster.shard.create",
            json!({
                "db": "app",
                "table": "users",
                "replicas": ["replica-a"]
            }),
        )
        .await;
        assert!(resp.ok);
        let shard_id = resp.result.expect("missing result")["shard"]["shard_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(!shard_id.is_empty());

        let resp = call_rpc(
            &state,
            "cluster.shard.move",
            json!({
                "shard_id": shard_id,
                "to_node_id": "replica-a",
                "dry_run": true
            }),
        )
        .await;
        assert!(resp.ok);
        assert_eq!(
            resp.result
                .as_ref()
                .and_then(|v| v.get("dry_run"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );

        let resp = call_rpc(
            &state,
            "cluster.replica.promote",
            json!({
                "node_id": "replica-a"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "cluster.node.remove",
            json!({
                "node_id": "replica-a"
            }),
        )
        .await;
        assert!(!resp.ok);
        assert_eq!(
            resp.error.as_ref().map(|e| e.code.as_str()),
            Some("forbidden")
        );

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT 1"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE DATABASE blocked_via_sql"
            }),
        )
        .await;
        assert!(!resp.ok);
        assert_eq!(
            resp.error.as_ref().map(|e| e.code.as_str()),
            Some("forbidden")
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn cluster_write_guard_blocks_non_primary() -> anyhow::Result<()> {
        let dir = temp_dir("cluster_guard");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let token = call_rpc(&state, "cluster.join_token.create", json!({}))
            .await
            .result
            .expect("join token result")["token"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(!token.is_empty());

        let resp = call_rpc(
            &state,
            "cluster.node.join",
            json!({
                "token": token,
                "node_id": "primary-2",
                "rpc_url": "http://127.0.0.1:19082",
                "role": "primary"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "cluster.replica.promote",
            json!({
                "node_id": "primary-2"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "schema.create_database",
            json!({
                "db": "blocked"
            }),
        )
        .await;
        assert!(!resp.ok);
        assert_eq!(
            resp.error.as_ref().map(|e| e.code.as_str()),
            Some("forbidden")
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn cluster_state_persists_in_settings_file() -> anyhow::Result<()> {
        let dir = temp_dir("cluster_persist");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let resp = call_rpc(&state, "cluster.join_token.create", json!({})).await;
        assert!(resp.ok);

        let settings_bytes = std::fs::read(settings_path(&state))?;
        let settings_json: Value = serde_json::from_slice(&settings_bytes)?;
        assert!(settings_json.get(CLUSTER_STATE_KEY).is_some());
        assert_eq!(
            settings_json
                .get("cluster.enabled")
                .and_then(|v| v.as_bool()),
            Some(true)
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }
}
