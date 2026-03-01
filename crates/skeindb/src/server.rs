use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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
use sha1::{Digest, Sha1};
use sysinfo::{Pid, System};

use skeindb_skeinql::{
    methods::{
        AdvisorHistoryParams, AdvisorIndexApplyParams, AdvisorIndexDismissParams,
        AdvisorIndexSynthesizeParams, AiAutoparamAnalyzeParams, AiAutoparamClassifyParams,
        AiNlExecuteParams, AiNlExplainParams, AiNlTranslateParams, CdcPollParams,
        CdcSubscribeTableParams, ClusterJoinTokenCreateParams, ClusterNodeJoinParams,
        ClusterNodeLeaveParams, ClusterNodeRemoveParams, ClusterNodesParams,
        ClusterReplicaPromoteParams, ClusterShardCreateParams, ClusterShardMoveParams,
        ClusterShardRebalanceParams, DataDeleteParams, DataGetParams, DataInsertParams,
        DataUpdateParams, DpAggregateParams, DpAuditLogParams, DpBudgetGetParams,
        DpBudgetSetParams, EdgeBundleApplyParams, EdgeBundleRequestParams, EdgeBundleStatusParams,
        ForensicExportParams, ForensicQueryParams, ForensicVerifyParams, MergeApplyParams,
        MergeRegisterParams, MergeSimulateParams, MergeWasmDropParams, MergeWasmRegisterParams,
        MigrationIntentReportParams, MigrationRewritePreviewParams, ObliviousExplainParams,
        ObliviousPolicyGetParams, ObliviousPolicySetParams, QueryExecutePreparedParams,
        QueryPatchParams, QueryPrepareParams, SchemaApplyMergeParams, SchemaColumnInfo,
        SchemaMergeStatusParams, SchemaProposeChangeParams, VectorIndexStatusParams,
        VectorInsertParams, VectorSearchParams, ViewCreateParams, ViewDropParams,
        ViewExplainDepsParams, ViewRefreshParams, ViewStatusParams, WasmPlanCompileParams,
        WasmPlanRunParams,
    },
    types::{
        BaseTableRef, Expr, JoinRef, JoinTableRef, JoinType, LimitClause, Lit, OrderBy, OrderDir,
        Query, QueryBody, QueryCache, ResultFormat, SelectBody, SelectItem, TableRef, TypeDesc,
        WireHints,
    },
    RpcError, RpcId, RpcRequest, RpcResponse, SKEINQL_VERSION,
};

use crate::engine::{ColumnSchema, Engine, Subscriptions};
use crate::quic;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{watch, RwLock},
};
use tower_http::cors::{Any, CorsLayer};

const REPLICATION_HEADER: &str = "x-skeindb-replication";
const CLUSTER_STATE_KEY: &str = "cluster.state.v1";
const CLUSTER_DEFAULT_JOIN_TTL_MS: u64 = 10 * 60 * 1000;
static CLUSTER_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);
static TX_COUNTER: AtomicU64 = AtomicU64::new(1);

const ADMIN_INDEX_HTML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/skeinadmin/index.html"
));
const ADMIN_MAIN_JS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/skeinadmin/src/main.js"
));
const MYSQL_PROTOCOL_VERSION: u8 = 0x0a;
const MYSQL_SERVER_VERSION: &str = "8.0.0-skeindb";
const MYSQL_AUTH_PLUGIN: &str = "mysql_native_password";
const MYSQL_STATUS_AUTOCOMMIT: u16 = 0x0002;
const MYSQL_CAP_LONG_PASSWORD: u32 = 0x0000_0001;
const MYSQL_CAP_CONNECT_WITH_DB: u32 = 0x0000_0008;
const MYSQL_CAP_PROTOCOL_41: u32 = 0x0000_0200;
const MYSQL_CAP_SSL: u32 = 0x0000_0800;
const MYSQL_CAP_SECURE_CONNECTION: u32 = 0x0000_8000;
const MYSQL_CAP_PLUGIN_AUTH: u32 = 0x0008_0000;
const MYSQL_CAP_CONNECT_ATTRS: u32 = 0x0010_0000;
const MYSQL_CAP_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
const MYSQL_CAP_DEPRECATE_EOF: u32 = 0x0100_0000;
const MYSQL_SERVER_CAPABILITIES: u32 = MYSQL_CAP_LONG_PASSWORD
    | MYSQL_CAP_CONNECT_WITH_DB
    | MYSQL_CAP_PROTOCOL_41
    | MYSQL_CAP_SECURE_CONNECTION
    | MYSQL_CAP_PLUGIN_AUTH
    | MYSQL_CAP_CONNECT_ATTRS
    | MYSQL_CAP_PLUGIN_AUTH_LENENC_CLIENT_DATA
    | MYSQL_CAP_DEPRECATE_EOF;

fn admin_index_html() -> &'static str {
    ADMIN_INDEX_HTML
}

fn admin_main_js() -> &'static str {
    ADMIN_MAIN_JS
}

#[derive(Debug, Clone)]
pub struct ServeOpts {
    pub data_dir: String,
    pub storage_mode: String,
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
    txns: Arc<Mutex<HashMap<String, TxSession>>>,

    engine: Arc<RwLock<Engine>>,
    subs: Arc<Mutex<Subscriptions>>,
    coalesce: Arc<QueryCoalescer>,
    transport: TransportCapabilities,
    shutdown_tx: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
struct TxSession {
    id: String,
    read_only: bool,
    started_at_ms: u64,
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
    query_stats: HashMap<String, QueryStatsAgg>,
    query_log: VecDeque<QuerySample>,
}

#[derive(Debug, Clone)]
struct QueryStatsAgg {
    method: String,
    count: u64,
    error_count: u64,
    total_ms: u64,
    max_ms: u64,
    rows_returned: u64,
    last_status: u16,
    last_seen_ms: u64,
    latency_samples_ms: VecDeque<u64>,
}

impl QueryStatsAgg {
    fn new(method: String) -> Self {
        Self {
            method,
            count: 0,
            error_count: 0,
            total_ms: 0,
            max_ms: 0,
            rows_returned: 0,
            last_status: 200,
            last_seen_ms: 0,
            latency_samples_ms: VecDeque::new(),
        }
    }

    fn record(&mut self, duration_ms: u64, status: u16, ok: bool, rows_returned: u64, now_ms: u64) {
        self.count = self.count.saturating_add(1);
        self.total_ms = self.total_ms.saturating_add(duration_ms);
        self.max_ms = self.max_ms.max(duration_ms);
        self.rows_returned = self.rows_returned.saturating_add(rows_returned);
        self.last_status = status;
        self.last_seen_ms = now_ms;
        if !ok {
            self.error_count = self.error_count.saturating_add(1);
        }
        self.latency_samples_ms.push_back(duration_ms);
        while self.latency_samples_ms.len() > QUERY_LATENCY_SAMPLE_CAPACITY {
            self.latency_samples_ms.pop_front();
        }
    }

    fn avg_ms(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.total_ms as f64 / self.count as f64
    }
}

#[derive(Debug, Clone)]
struct QuerySample {
    ts_ms: u64,
    method: String,
    fingerprint: String,
    duration_ms: u64,
    status: u16,
    ok: bool,
    rows_returned: u64,
}

const QUERY_LATENCY_SAMPLE_CAPACITY: usize = 256;
const QUERY_LOG_CAPACITY: usize = 1024;

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

#[derive(Debug)]
struct MySqlHandshakeResponse {
    capabilities: u32,
    username: String,
    auth_response: Vec<u8>,
    database: Option<String>,
    auth_plugin: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MySqlLiteral {
    Int(i64),
    Str(String),
    Null,
}

#[derive(Debug, Clone)]
enum MySqlQueryOutcome {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    ResultSet {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MySqlStmtColumnType {
    LongLong,
    Double,
    VarString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MySqlStmtParamType {
    type_code: u8,
    unsigned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MySqlStmtPrepareColumn {
    name: String,
    column_type: MySqlStmtColumnType,
}

#[derive(Debug, Clone)]
struct MySqlPreparedStatement {
    sql: String,
    param_count: u16,
    result_columns: Vec<MySqlStmtPrepareColumn>,
    param_types: Vec<MySqlStmtParamType>,
    long_data: HashMap<u16, Vec<u8>>,
}

#[derive(Debug)]
struct MySqlSessionState {
    default_db: Option<String>,
    last_found_rows: u64,
    autocommit: bool,
    tx_active: bool,
    tx_undo_sql: Vec<String>,
}

type MySqlWireError = (u16, &'static str, String);

impl MySqlPreparedStatement {
    fn new(sql: String, param_count: u16, result_columns: Vec<MySqlStmtPrepareColumn>) -> Self {
        Self {
            sql,
            param_count,
            result_columns,
            param_types: Vec::new(),
            long_data: HashMap::new(),
        }
    }
}

impl MySqlSessionState {
    fn new(default_db: Option<String>) -> Self {
        Self {
            default_db,
            last_found_rows: 0,
            autocommit: true,
            tx_active: false,
            tx_undo_sql: Vec::new(),
        }
    }
}

fn mysql_seed(conn_id: u32) -> [u8; 20] {
    let mut seed = [0u8; 20];
    let mut x = now_unix_ms_u64()
        .wrapping_add((conn_id as u64) << 17)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for b in &mut seed {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *b = (x.wrapping_mul(0x2545_F491_4F6C_DD1D) & 0x7f) as u8;
    }
    seed
}

fn mysql_hash(input: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(input);
    let digest = h.finalize();
    let mut out = [0u8; 20];
    out.copy_from_slice(&digest[..20]);
    out
}

fn mysql_native_password_scramble(password: &str, seed: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = mysql_hash(password.as_bytes());
    let stage2 = mysql_hash(&stage1);
    let mut combined = Vec::with_capacity(seed.len() + stage2.len());
    combined.extend_from_slice(seed);
    combined.extend_from_slice(&stage2);
    let digest = mysql_hash(&combined);
    let mut out = vec![0u8; stage1.len()];
    for i in 0..stage1.len() {
        out[i] = stage1[i] ^ digest[i];
    }
    out
}

fn mysql_validate_native_password(password: &str, seed: &[u8], auth_response: &[u8]) -> bool {
    if password.is_empty() {
        return auth_response.is_empty();
    }
    let expected = mysql_native_password_scramble(password, seed);
    expected == auth_response
}

fn parse_lenenc_int(payload: &[u8], cursor: &mut usize) -> Result<usize, String> {
    if *cursor >= payload.len() {
        return Err("missing length-encoded integer".to_string());
    }
    let first = payload[*cursor];
    *cursor += 1;
    match first {
        0x00..=0xfa => Ok(first as usize),
        0xfc => {
            if *cursor + 2 > payload.len() {
                return Err("truncated length-encoded integer".to_string());
            }
            let n = u16::from_le_bytes([payload[*cursor], payload[*cursor + 1]]) as usize;
            *cursor += 2;
            Ok(n)
        }
        0xfd => {
            if *cursor + 3 > payload.len() {
                return Err("truncated length-encoded integer".to_string());
            }
            let n = (payload[*cursor] as usize)
                | ((payload[*cursor + 1] as usize) << 8)
                | ((payload[*cursor + 2] as usize) << 16);
            *cursor += 3;
            Ok(n)
        }
        0xfe => {
            if *cursor + 8 > payload.len() {
                return Err("truncated length-encoded integer".to_string());
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
                return Err("length-encoded integer too large".to_string());
            }
            Ok(n as usize)
        }
        _ => Err("unsupported length-encoded integer marker".to_string()),
    }
}

fn parse_lenenc_bytes<'a>(payload: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], String> {
    let len = parse_lenenc_int(payload, cursor)?;
    if *cursor + len > payload.len() {
        return Err("truncated length-encoded bytes".to_string());
    }
    let bytes = &payload[*cursor..*cursor + len];
    *cursor += len;
    Ok(bytes)
}

fn mysql_count_placeholders(sql: &str) -> u16 {
    let bytes = sql.as_bytes();
    let mut quote = 0u8;
    let mut count = 0u16;
    let mut i = 0usize;
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
            b'?' => count = count.saturating_add(1),
            _ => {}
        }
        i += 1;
    }
    count
}

fn mysql_stmt_param_is_null(null_bitmap: &[u8], idx: usize) -> bool {
    let byte = idx / 8;
    let bit = idx % 8;
    null_bitmap
        .get(byte)
        .map(|b| (b & (1u8 << bit)) != 0)
        .unwrap_or(false)
}

fn mysql_decode_time_lit(payload: &[u8], cursor: &mut usize) -> Result<Lit, String> {
    if *cursor >= payload.len() {
        return Err("truncated TIME parameter".to_string());
    }
    let len = payload[*cursor] as usize;
    *cursor += 1;
    if len == 0 {
        return Ok(Lit::Time {
            iso: "00:00:00".to_string(),
        });
    }
    if !matches!(len, 8 | 12) || *cursor + len > payload.len() {
        return Err("unsupported TIME parameter payload".to_string());
    }
    let negative = payload[*cursor] != 0;
    let days = u32::from_le_bytes([
        payload[*cursor + 1],
        payload[*cursor + 2],
        payload[*cursor + 3],
        payload[*cursor + 4],
    ]);
    let hours = payload[*cursor + 5] as u32 + days.saturating_mul(24);
    let minutes = payload[*cursor + 6];
    let seconds = payload[*cursor + 7];
    let micros = if len == 12 {
        u32::from_le_bytes([
            payload[*cursor + 8],
            payload[*cursor + 9],
            payload[*cursor + 10],
            payload[*cursor + 11],
        ])
    } else {
        0
    };
    *cursor += len;
    let mut iso = format!(
        "{}{:02}:{:02}:{:02}",
        if negative { "-" } else { "" },
        hours,
        minutes,
        seconds
    );
    if micros > 0 {
        iso.push_str(&format!(".{:06}", micros));
    }
    Ok(Lit::Time { iso })
}

fn mysql_decode_dateish_lit(
    payload: &[u8],
    cursor: &mut usize,
    datetime_like: bool,
) -> Result<Lit, String> {
    if *cursor >= payload.len() {
        return Err("truncated date-like parameter".to_string());
    }
    let len = payload[*cursor] as usize;
    *cursor += 1;
    if len == 0 {
        return Ok(if datetime_like {
            Lit::Datetime {
                iso: "0000-00-00 00:00:00".to_string(),
            }
        } else {
            Lit::Date {
                iso: "0000-00-00".to_string(),
            }
        });
    }
    let valid = if datetime_like {
        matches!(len, 4 | 7 | 11)
    } else {
        len == 4
    };
    if !valid || *cursor + len > payload.len() {
        return Err("unsupported date-like parameter payload".to_string());
    }
    let year = u16::from_le_bytes([payload[*cursor], payload[*cursor + 1]]);
    let month = payload[*cursor + 2];
    let day = payload[*cursor + 3];
    let hour = if len >= 7 { payload[*cursor + 4] } else { 0 };
    let minute = if len >= 7 { payload[*cursor + 5] } else { 0 };
    let second = if len >= 7 { payload[*cursor + 6] } else { 0 };
    let micros = if len == 11 {
        u32::from_le_bytes([
            payload[*cursor + 7],
            payload[*cursor + 8],
            payload[*cursor + 9],
            payload[*cursor + 10],
        ])
    } else {
        0
    };
    *cursor += len;
    if datetime_like {
        let mut iso = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
        if micros > 0 {
            iso.push_str(&format!(".{:06}", micros));
        }
        Ok(Lit::Datetime { iso })
    } else {
        Ok(Lit::Date {
            iso: format!("{year:04}-{month:02}-{day:02}"),
        })
    }
}

fn mysql_decode_stmt_param_lit(
    param_type: MySqlStmtParamType,
    payload: &[u8],
    cursor: &mut usize,
    long_data: Option<Vec<u8>>,
) -> Result<Lit, String> {
    if let Some(bytes) = long_data {
        return Ok(Lit::Str {
            v: String::from_utf8_lossy(&bytes).to_string(),
        });
    }

    match param_type.type_code {
        0x00 | 0x06 => Ok(Lit::Null),
        0x01 => {
            if *cursor + 1 > payload.len() {
                return Err("truncated TINY parameter".to_string());
            }
            let raw = payload[*cursor];
            *cursor += 1;
            Ok(if param_type.unsigned {
                Lit::U64 { v: raw as u64 }
            } else {
                Lit::I64 {
                    v: i8::from_le_bytes([raw]) as i64,
                }
            })
        }
        0x02 | 0x0d => {
            if *cursor + 2 > payload.len() {
                return Err("truncated SHORT parameter".to_string());
            }
            let raw = [payload[*cursor], payload[*cursor + 1]];
            *cursor += 2;
            Ok(if param_type.unsigned {
                Lit::U64 {
                    v: u16::from_le_bytes(raw) as u64,
                }
            } else {
                Lit::I64 {
                    v: i16::from_le_bytes(raw) as i64,
                }
            })
        }
        0x03 | 0x09 => {
            if *cursor + 4 > payload.len() {
                return Err("truncated LONG parameter".to_string());
            }
            let raw = [
                payload[*cursor],
                payload[*cursor + 1],
                payload[*cursor + 2],
                payload[*cursor + 3],
            ];
            *cursor += 4;
            Ok(if param_type.unsigned {
                Lit::U64 {
                    v: u32::from_le_bytes(raw) as u64,
                }
            } else {
                Lit::I64 {
                    v: i32::from_le_bytes(raw) as i64,
                }
            })
        }
        0x08 => {
            if *cursor + 8 > payload.len() {
                return Err("truncated LONGLONG parameter".to_string());
            }
            let raw = [
                payload[*cursor],
                payload[*cursor + 1],
                payload[*cursor + 2],
                payload[*cursor + 3],
                payload[*cursor + 4],
                payload[*cursor + 5],
                payload[*cursor + 6],
                payload[*cursor + 7],
            ];
            *cursor += 8;
            Ok(if param_type.unsigned {
                Lit::U64 {
                    v: u64::from_le_bytes(raw),
                }
            } else {
                Lit::I64 {
                    v: i64::from_le_bytes(raw),
                }
            })
        }
        0x04 => {
            if *cursor + 4 > payload.len() {
                return Err("truncated FLOAT parameter".to_string());
            }
            let raw = [
                payload[*cursor],
                payload[*cursor + 1],
                payload[*cursor + 2],
                payload[*cursor + 3],
            ];
            *cursor += 4;
            Ok(Lit::F64 {
                v: f32::from_le_bytes(raw) as f64,
            })
        }
        0x05 => {
            if *cursor + 8 > payload.len() {
                return Err("truncated DOUBLE parameter".to_string());
            }
            let raw = [
                payload[*cursor],
                payload[*cursor + 1],
                payload[*cursor + 2],
                payload[*cursor + 3],
                payload[*cursor + 4],
                payload[*cursor + 5],
                payload[*cursor + 6],
                payload[*cursor + 7],
            ];
            *cursor += 8;
            Ok(Lit::F64 {
                v: f64::from_le_bytes(raw),
            })
        }
        0x07 | 0x0c => mysql_decode_dateish_lit(payload, cursor, true),
        0x0a => mysql_decode_dateish_lit(payload, cursor, false),
        0x0b => mysql_decode_time_lit(payload, cursor),
        0x0f | 0xf5 | 0xfd | 0xfe | 0xf9 | 0xfa | 0xfb | 0xfc | 0xf6 | 0x10 => {
            let bytes = parse_lenenc_bytes(payload, cursor)?;
            Ok(Lit::Str {
                v: String::from_utf8_lossy(bytes).to_string(),
            })
        }
        other => Err(format!("unsupported prepared parameter type 0x{other:02x}")),
    }
}

fn mysql_substitute_stmt_sql(sql: &str, params: &[Lit]) -> Result<String, String> {
    let bytes = sql.as_bytes();
    let mut quote = 0u8;
    let mut out = String::with_capacity(sql.len().saturating_add(params.len() * 4));
    let mut i = 0usize;
    let mut param_idx = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if quote != 0 {
            out.push(b as char);
            if b == quote {
                if quote == b'\'' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
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
                out.push(b as char);
            }
            b'?' => {
                let lit = params
                    .get(param_idx)
                    .ok_or_else(|| "not enough prepared parameters".to_string())?;
                out.push_str(&mysql_render_default_lit(lit));
                param_idx += 1;
            }
            _ => out.push(b as char),
        }
        i += 1;
    }
    if param_idx != params.len() {
        return Err("too many prepared parameters".to_string());
    }
    Ok(out)
}

fn parse_mysql_handshake_response(payload: &[u8]) -> Result<MySqlHandshakeResponse, String> {
    if payload.len() < 32 {
        return Err("handshake response too short".to_string());
    }
    let capabilities = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut cursor = 4 + 4 + 1 + 23;

    let username_end = payload[cursor..]
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| "missing username terminator".to_string())?;
    let username_bytes = &payload[cursor..cursor + username_end];
    let username = String::from_utf8(username_bytes.to_vec())
        .map_err(|_| "username must be utf-8".to_string())?;
    cursor += username_end + 1;

    let auth_response = if capabilities & MYSQL_CAP_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let len = parse_lenenc_int(payload, &mut cursor)?;
        if cursor + len > payload.len() {
            return Err("truncated auth response".to_string());
        }
        let bytes = payload[cursor..cursor + len].to_vec();
        cursor += len;
        bytes
    } else if capabilities & MYSQL_CAP_SECURE_CONNECTION != 0 {
        if cursor >= payload.len() {
            return Err("missing auth response length".to_string());
        }
        let len = payload[cursor] as usize;
        cursor += 1;
        if cursor + len > payload.len() {
            return Err("truncated auth response".to_string());
        }
        let bytes = payload[cursor..cursor + len].to_vec();
        cursor += len;
        bytes
    } else {
        let end = payload[cursor..]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| "missing auth response terminator".to_string())?;
        let bytes = payload[cursor..cursor + end].to_vec();
        cursor += end + 1;
        bytes
    };

    let database = if capabilities & MYSQL_CAP_CONNECT_WITH_DB != 0 {
        let db_end = payload[cursor..]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| "missing db terminator".to_string())?;
        let db_bytes = &payload[cursor..cursor + db_end];
        cursor += db_end + 1;
        let db =
            String::from_utf8(db_bytes.to_vec()).map_err(|_| "db must be utf-8".to_string())?;
        (!db.is_empty()).then_some(db)
    } else {
        None
    };

    let auth_plugin = if capabilities & MYSQL_CAP_PLUGIN_AUTH != 0 {
        let plugin_end = payload[cursor..]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| "missing auth plugin terminator".to_string())?;
        let plugin_bytes = &payload[cursor..cursor + plugin_end];
        Some(
            String::from_utf8(plugin_bytes.to_vec())
                .map_err(|_| "auth plugin must be utf-8".to_string())?,
        )
    } else {
        None
    };

    Ok(MySqlHandshakeResponse {
        capabilities,
        username,
        auth_response,
        database,
        auth_plugin,
    })
}

fn find_ascii_ci_outside_quotes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    let mut in_string = false;
    let mut i = 0usize;
    while i + needle.len() <= haystack.len() {
        let ch = haystack[i];
        if ch == b'\'' {
            if in_string && i + 1 < haystack.len() && haystack[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if !in_string
            && haystack[i..i + needle.len()]
                .iter()
                .zip(needle.iter())
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn split_select_expressions(input: &str) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            cur.push(ch);
            if in_string && chars.peek().copied() == Some('\'') {
                cur.push('\'');
                chars.next();
                continue;
            }
            in_string = !in_string;
            continue;
        }
        if ch == ',' && !in_string {
            let item = cur.trim();
            if item.is_empty() {
                return None;
            }
            out.push(item.to_string());
            cur.clear();
            continue;
        }
        cur.push(ch);
    }
    if in_string {
        return None;
    }
    let tail = cur.trim();
    if tail.is_empty() {
        return None;
    }
    out.push(tail.to_string());
    Some(out)
}

fn parse_sql_string_literal(input: &str) -> Option<String> {
    if input.len() < 2 || !input.starts_with('\'') || !input.ends_with('\'') {
        return None;
    }
    let mut out = String::new();
    let mut chars = input[1..input.len() - 1].chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' && chars.peek().copied() == Some('\'') {
            out.push('\'');
            chars.next();
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

fn mysql_normalize_session_var_name(raw: &str) -> String {
    raw.trim()
        .trim_start_matches('@')
        .trim_start_matches('@')
        .trim()
        .trim_start_matches("global.")
        .trim_start_matches("GLOBAL.")
        .trim_start_matches("session.")
        .trim_start_matches("SESSION.")
        .trim_start_matches("local.")
        .trim_start_matches("LOCAL.")
        .trim()
        .to_ascii_lowercase()
}

fn mysql_session_var_value(raw: &str) -> Option<MySqlLiteral> {
    match mysql_normalize_session_var_name(raw).as_str() {
        "sql_mode" => Some(MySqlLiteral::Str(String::new())),
        "lower_case_table_names" => Some(MySqlLiteral::Int(0)),
        "version_comment" => Some(MySqlLiteral::Str("SkeinDB compatibility layer".to_string())),
        "tx_isolation" | "transaction_isolation" => {
            Some(MySqlLiteral::Str("REPEATABLE-READ".to_string()))
        }
        "character_set_client" | "character_set_connection" | "character_set_results" => {
            Some(MySqlLiteral::Str("utf8mb4".to_string()))
        }
        "collation_connection" => Some(MySqlLiteral::Str("utf8mb4_general_ci".to_string())),
        "autocommit" => Some(MySqlLiteral::Int(1)),
        _ => None,
    }
}

fn parse_select_literal_query(
    sql: &str,
    default_db: Option<&str>,
) -> Option<Vec<(String, MySqlLiteral)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed.is_ascii() {
        return None;
    }
    if trimmed.len() < 7 || !trimmed[..6].eq_ignore_ascii_case("select") {
        return None;
    }
    let rest = trimmed[6..].trim();
    if rest.is_empty() {
        return None;
    }
    if find_ascii_ci_outside_quotes(rest.as_bytes(), b" from ").is_some() {
        return None;
    }

    let exprs = split_select_expressions(rest)?;
    let mut cols = Vec::with_capacity(exprs.len());
    for (idx, expr) in exprs.iter().enumerate() {
        let bytes = expr.as_bytes();
        let (value_src, alias_src) =
            if let Some(as_pos) = find_ascii_ci_outside_quotes(bytes, b" as ") {
                (
                    expr[..as_pos].trim(),
                    Some(expr[as_pos + 4..].trim().to_string()),
                )
            } else {
                (expr.trim(), None)
            };
        let alias = alias_src
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| format!("col{}", idx + 1));

        let lit = if value_src.eq_ignore_ascii_case("null") {
            MySqlLiteral::Null
        } else if value_src.eq_ignore_ascii_case("version()") {
            MySqlLiteral::Str(MYSQL_SERVER_VERSION.to_string())
        } else if value_src.eq_ignore_ascii_case("database()") {
            match default_db {
                Some(db) if !db.trim().is_empty() => MySqlLiteral::Str(db.trim().to_string()),
                _ => MySqlLiteral::Null,
            }
        } else if value_src.starts_with("@@") {
            mysql_session_var_value(value_src)?
        } else if let Some(v) = parse_sql_string_literal(value_src) {
            MySqlLiteral::Str(v)
        } else if let Ok(v) = value_src.parse::<i64>() {
            MySqlLiteral::Int(v)
        } else {
            return None;
        };
        cols.push((alias, lit));
    }
    Some(cols)
}

fn mysql_parse_set_autocommit(sql: &str) -> Option<bool> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed.is_ascii() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("set ") {
        return None;
    }
    let rest = trimmed[4..].trim();
    let rest_lower = rest.to_ascii_lowercase();
    let key = "autocommit";
    if !rest_lower.starts_with(key) {
        return None;
    }
    let tail = rest[key.len()..].trim_start();
    let tail = tail.strip_prefix('=').unwrap_or(tail).trim();
    match tail {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

fn mysql_is_session_compat_set(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed.is_ascii() {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("set ") {
        return false;
    }
    let rest = trimmed[4..].trim();
    let rest_lower = rest.to_ascii_lowercase();
    if rest_lower.starts_with("names ") {
        return true;
    }

    let normalized = rest_lower
        .strip_prefix("@@session.")
        .or_else(|| rest_lower.strip_prefix("@@local."))
        .or_else(|| rest_lower.strip_prefix("@@"))
        .or_else(|| rest_lower.strip_prefix("session "))
        .or_else(|| rest_lower.strip_prefix("local "))
        .unwrap_or(rest_lower.as_str())
        .trim_start();

    [
        "sql_mode",
        "character_set_client",
        "character_set_connection",
        "character_set_results",
        "collation_connection",
        "wait_timeout",
    ]
    .iter()
    .any(|name| normalized.starts_with(name))
}

fn mysql_is_begin(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    trimmed.eq_ignore_ascii_case("begin")
        || trimmed.eq_ignore_ascii_case("begin work")
        || trimmed.eq_ignore_ascii_case("start transaction")
}

fn mysql_is_commit(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    trimmed.eq_ignore_ascii_case("commit") || trimmed.eq_ignore_ascii_case("commit work")
}

fn mysql_is_rollback(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    trimmed.eq_ignore_ascii_case("rollback") || trimmed.eq_ignore_ascii_case("rollback work")
}

fn mysql_quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn mysql_like_matches(value: &str, pattern: &str) -> bool {
    let value = value.to_ascii_lowercase().into_bytes();
    let pattern = pattern.to_ascii_lowercase().into_bytes();
    let (mut vi, mut pi) = (0usize, 0usize);
    let (mut star_pi, mut star_vi) = (None::<usize>, 0usize);

    while vi < value.len() {
        if pi < pattern.len() && (pattern[pi] == b'_' || pattern[pi] == value[vi]) {
            vi += 1;
            pi += 1;
            continue;
        }
        if pi < pattern.len() && pattern[pi] == b'%' {
            star_pi = Some(pi);
            pi += 1;
            star_vi = vi;
            continue;
        }
        if let Some(saved_pi) = star_pi {
            pi = saved_pi + 1;
            star_vi += 1;
            vi = star_vi;
            continue;
        }
        return false;
    }

    while pi < pattern.len() && pattern[pi] == b'%' {
        pi += 1;
    }
    pi == pattern.len()
}

fn mysql_type_desc_display(desc: &Value) -> String {
    let kind = desc
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("string")
        .to_ascii_lowercase();
    let max = desc.get("max").and_then(|v| v.as_u64());
    match kind.as_str() {
        "u64" => "bigint unsigned".to_string(),
        "i64" => "bigint".to_string(),
        "f64" => "double".to_string(),
        "datetime" => "datetime".to_string(),
        "date" => "date".to_string(),
        "time" => "time".to_string(),
        "json" => "json".to_string(),
        "bytes" => "blob".to_string(),
        "bool" => "tinyint(1)".to_string(),
        "string" => match max {
            Some(len) => format!("varchar({len})"),
            None => "longtext".to_string(),
        },
        other => other.to_string(),
    }
}

fn mysql_desc_column_default(desc: &Value, name: &str) -> Option<Lit> {
    desc.get("compat_mysql")
        .and_then(|v| v.get("column_defaults"))
        .and_then(|v| v.get(name))
        .cloned()
        .and_then(|v| serde_json::from_value(v).ok())
}

fn mysql_default_cell_value(lit: &Lit) -> Option<String> {
    match lit {
        Lit::Null => None,
        Lit::Bool { v } => Some(if *v { "1" } else { "0" }.to_string()),
        Lit::I64 { v } => Some(v.to_string()),
        Lit::U64 { v } => Some(v.to_string()),
        Lit::F64 { v } => Some(v.to_string()),
        Lit::Dec { v } => Some(v.clone()),
        Lit::Str { v } => Some(v.clone()),
        Lit::Date { iso } => Some(iso.clone()),
        Lit::Time { iso } => Some(iso.clone()),
        Lit::Datetime { iso } => Some(iso.clone()),
        Lit::Uuid { v } => Some(v.clone()),
        Lit::Bytes { .. } | Lit::Json { .. } | Lit::Embedding { .. } => None,
    }
}

fn mysql_render_default_lit(lit: &Lit) -> String {
    match lit {
        Lit::Null => "NULL".to_string(),
        Lit::Bool { v } => {
            if *v {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Lit::I64 { v } => v.to_string(),
        Lit::U64 { v } => v.to_string(),
        Lit::F64 { v } => v.to_string(),
        Lit::Dec { v } => v.clone(),
        Lit::Str { v } => format!("'{}'", v.replace('\'', "''")),
        Lit::Date { iso } => format!("'{}'", iso.replace('\'', "''")),
        Lit::Time { iso } => format!("'{}'", iso.replace('\'', "''")),
        Lit::Datetime { iso } => format!("'{}'", iso.replace('\'', "''")),
        Lit::Uuid { v } => format!("'{}'", v.replace('\'', "''")),
        Lit::Bytes { .. } | Lit::Json { .. } | Lit::Embedding { .. } => "NULL".to_string(),
    }
}

fn mysql_desc_indexes(desc: &Value) -> Vec<(String, Vec<String>, bool)> {
    desc.get("indexes")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|index| {
            let name = index.get("name").and_then(|v| v.as_str())?.to_string();
            let columns = index
                .get("columns")
                .and_then(|v| v.as_array())?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            if columns.is_empty() {
                return None;
            }
            let unique = index
                .get("unique")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Some((name, columns, unique))
        })
        .collect()
}

fn mysql_desc_primary_key(desc: &Value) -> Vec<String> {
    desc.get("primary_key")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect()
}

fn mysql_row_lookup_expr(row: &BTreeMap<String, Lit>, columns: &[String]) -> Option<Expr> {
    let mut expr = None::<Expr>;
    for col in columns {
        let lit = row.get(col)?.clone();
        if matches!(lit, Lit::Null) {
            return None;
        }
        let next = eq_expr(col.clone(), lit);
        expr = Some(match expr {
            Some(prev) => and_expr(prev, next),
            None => next,
        });
    }
    expr
}

fn mysql_conflict_predicates_for_row(desc: &Value, row: &BTreeMap<String, Lit>) -> Vec<Expr> {
    let mut predicates = Vec::new();
    if let Some(expr) = mysql_row_lookup_expr(row, &mysql_desc_primary_key(desc)) {
        predicates.push(expr);
    }
    for (_index_name, columns, unique) in mysql_desc_indexes(desc) {
        if !unique {
            continue;
        }
        if let Some(expr) = mysql_row_lookup_expr(row, &columns) {
            predicates.push(expr);
        }
    }
    predicates
}

fn mysql_show_columns_outcome(desc: &Value, full: bool) -> MySqlQueryOutcome {
    let columns = if full {
        vec![
            "Field".to_string(),
            "Type".to_string(),
            "Collation".to_string(),
            "Null".to_string(),
            "Key".to_string(),
            "Default".to_string(),
            "Extra".to_string(),
            "Privileges".to_string(),
            "Comment".to_string(),
        ]
    } else {
        vec![
            "Field".to_string(),
            "Type".to_string(),
            "Null".to_string(),
            "Key".to_string(),
            "Default".to_string(),
            "Extra".to_string(),
        ]
    };
    let primary_key: HashSet<String> = desc
        .get("primary_key")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .map(|v| v.to_string())
        .collect();
    let index_defs = mysql_desc_indexes(desc);
    let rows = desc
        .get("columns")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .map(|col| {
            let name = col
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let data_type = mysql_type_desc_display(col.get("type").unwrap_or(&Value::Null));
            let is_nullable = if col
                .get("nullable")
                .and_then(|v| v.as_bool())
                .unwrap_or(true)
            {
                "YES"
            } else {
                "NO"
            }
            .to_string();
            let key = if primary_key.contains(&name) {
                "PRI".to_string()
            } else if index_defs
                .iter()
                .any(|(_, columns, unique)| *unique && columns.iter().any(|col| col == &name))
            {
                "UNI".to_string()
            } else if index_defs
                .iter()
                .any(|(_, columns, _)| columns.iter().any(|col| col == &name))
            {
                "MUL".to_string()
            } else {
                String::new()
            };
            let extra = if col
                .get("auto_increment")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                "auto_increment".to_string()
            } else {
                String::new()
            };
            let default = mysql_desc_column_default(desc, &name)
                .and_then(|lit| mysql_default_cell_value(&lit));
            if full {
                vec![
                    Some(name),
                    Some(data_type),
                    Some("utf8mb4_general_ci".to_string()),
                    Some(is_nullable),
                    Some(key),
                    default,
                    Some(extra),
                    Some("select,insert,update,references".to_string()),
                    Some(String::new()),
                ]
            } else {
                vec![
                    Some(name),
                    Some(data_type),
                    Some(is_nullable),
                    Some(key),
                    default,
                    Some(extra),
                ]
            }
        })
        .collect();
    MySqlQueryOutcome::ResultSet { columns, rows }
}

fn mysql_show_index_outcome(table_name: &str, desc: &Value) -> MySqlQueryOutcome {
    let columns = vec![
        "Table".to_string(),
        "Non_unique".to_string(),
        "Key_name".to_string(),
        "Seq_in_index".to_string(),
        "Column_name".to_string(),
        "Collation".to_string(),
        "Cardinality".to_string(),
        "Sub_part".to_string(),
        "Packed".to_string(),
        "Null".to_string(),
        "Index_type".to_string(),
        "Comment".to_string(),
        "Index_comment".to_string(),
    ];
    let mut rows = Vec::new();
    let pk = desc
        .get("primary_key")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let columns_desc = desc
        .get("columns")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let compat_indexes = mysql_desc_indexes(desc);
    for (idx, column_name) in pk.iter().filter_map(|v| v.as_str()).enumerate() {
        let nullable = columns_desc
            .iter()
            .find(|col| col.get("name").and_then(|v| v.as_str()) == Some(column_name))
            .and_then(|col| col.get("nullable").and_then(|v| v.as_bool()))
            .unwrap_or(false);
        rows.push(vec![
            Some(table_name.to_string()),
            Some("0".to_string()),
            Some("PRIMARY".to_string()),
            Some((idx + 1).to_string()),
            Some(column_name.to_string()),
            Some("A".to_string()),
            Some("0".to_string()),
            None,
            None,
            Some(if nullable { "YES" } else { "NO" }.to_string()),
            Some("BTREE".to_string()),
            Some(String::new()),
            Some(String::new()),
        ]);
    }
    for (index_name, index_columns, unique) in compat_indexes {
        for (idx, column_name) in index_columns.iter().enumerate() {
            let nullable = columns_desc
                .iter()
                .find(|col| col.get("name").and_then(|v| v.as_str()) == Some(column_name.as_str()))
                .and_then(|col| col.get("nullable").and_then(|v| v.as_bool()))
                .unwrap_or(false);
            rows.push(vec![
                Some(table_name.to_string()),
                Some(if unique { "0" } else { "1" }.to_string()),
                Some(index_name.clone()),
                Some((idx + 1).to_string()),
                Some(column_name.clone()),
                Some("A".to_string()),
                Some("0".to_string()),
                None,
                None,
                Some(if nullable { "YES" } else { "NO" }.to_string()),
                Some("BTREE".to_string()),
                Some(String::new()),
                Some(String::new()),
            ]);
        }
    }
    MySqlQueryOutcome::ResultSet { columns, rows }
}

fn mysql_parse_count_query(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed.is_ascii() || trimmed.len() < 6 || !trimmed[..6].eq_ignore_ascii_case("select") {
        return None;
    }
    let rest = trimmed[6..].trim();
    let from_idx = find_keyword_top_level(rest, "from")?;
    let projection = rest[..from_idx].trim();
    let projection_lower = projection.to_ascii_lowercase();
    let (count_expr, alias_raw) = if let Some(idx) = find_keyword_top_level(projection, "as") {
        (projection[..idx].trim(), Some(projection[idx + 2..].trim()))
    } else {
        (projection, None)
    };
    let count_lower = count_expr
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    if !matches!(count_lower.as_str(), "count(*)" | "count(1)") {
        return None;
    }
    let alias = alias_raw
        .map(clean_sql_ident)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            if projection_lower.contains("count(1)") {
                "COUNT(1)".to_string()
            } else {
                "COUNT(*)".to_string()
            }
        });
    let mut from_tail = rest[from_idx..].trim().to_string();
    let truncate_at = ["order by", "limit", "offset"]
        .iter()
        .filter_map(|keyword| find_keyword_top_level(&from_tail, keyword))
        .min()
        .unwrap_or(from_tail.len());
    from_tail.truncate(truncate_at);
    let from_tail = from_tail.trim().to_string();
    (!from_tail.is_empty()).then_some((alias, format!("SELECT * {from_tail}")))
}

async fn mysql_try_count_query_outcome(
    state: &AppState,
    sql: &str,
    default_db: Option<&str>,
) -> Result<Option<MySqlQueryOutcome>, RpcError> {
    let Some((alias, transformed_sql)) = mysql_parse_count_query(sql) else {
        return Ok(None);
    };
    let params = SqlExecParams {
        sql: transformed_sql,
        explain: false,
        default_db: default_db.map(|db| db.to_string()),
        result_format: Some(ResultFormat::RowsJson),
    };
    let result = sql_exec(state, params).await?;
    let (_, rows) =
        mysql_extract_result_data(&result).map_err(|msg| RpcError::new("internal", msg))?;
    Ok(Some(MySqlQueryOutcome::ResultSet {
        columns: vec![alias],
        rows: vec![vec![Some(rows.len().to_string())]],
    }))
}

fn mysql_render_create_table(table_name: &str, desc: &Value) -> String {
    let mut defs = Vec::new();
    let columns = desc
        .get("columns")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for col in columns {
        let name = col.get("name").and_then(|v| v.as_str()).unwrap_or_default();
        let mut line = format!(
            "  {} {}",
            mysql_quote_ident(name),
            mysql_type_desc_display(col.get("type").unwrap_or(&Value::Null))
        );
        if !col
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
        {
            line.push_str(" NOT NULL");
        }
        if let Some(default) = mysql_desc_column_default(desc, name) {
            line.push_str(" DEFAULT ");
            line.push_str(&mysql_render_default_lit(&default));
        }
        if col
            .get("auto_increment")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            line.push_str(" AUTO_INCREMENT");
        }
        defs.push(line);
    }
    let pk = desc
        .get("primary_key")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if !pk.is_empty() {
        let keys = pk
            .iter()
            .filter_map(|v| v.as_str())
            .map(mysql_quote_ident)
            .collect::<Vec<_>>()
            .join(", ");
        defs.push(format!("  PRIMARY KEY ({keys})"));
    }
    for (index_name, columns, unique) in mysql_desc_indexes(desc) {
        let cols = columns
            .iter()
            .map(|col| mysql_quote_ident(col))
            .collect::<Vec<_>>()
            .join(", ");
        let prefix = if unique { "UNIQUE KEY" } else { "KEY" };
        defs.push(format!(
            "  {prefix} {} ({cols})",
            mysql_quote_ident(&index_name)
        ));
    }
    format!(
        "CREATE TABLE {} (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        mysql_quote_ident(table_name),
        defs.join(",\n")
    )
}

async fn mysql_build_insert_undo_sql(
    state: &AppState,
    sql: &str,
    default_db: Option<&str>,
    result: &Value,
) -> Option<String> {
    let affected = result
        .get("write")
        .and_then(|v| v.get("affected"))
        .and_then(|v| v.as_u64())?;
    if affected != 1 {
        return None;
    }
    let last_insert_id = result
        .get("write")
        .and_then(|v| v.get("last_insert_id"))
        .and_then(|v| v.as_u64())?;
    if last_insert_id == 0 {
        return None;
    }
    let SqlPlan::Insert { table, .. } = parse_sql_plan(sql, default_db).ok()? else {
        return None;
    };
    let eng = state.engine.read().await;
    let desc = eng.describe_table(&table.db, &table.table).ok()?;
    let pk = desc.get("primary_key").and_then(|v| v.as_array())?;
    if pk.len() != 1 {
        return None;
    }
    let pk_name = pk.first().and_then(|v| v.as_str())?;
    let auto_increment = desc
        .get("columns")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .any(|col| {
            col.get("name").and_then(|v| v.as_str()) == Some(pk_name)
                && col
                    .get("auto_increment")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
        });
    if !auto_increment {
        return None;
    }
    Some(format!(
        "DELETE FROM {}.{} WHERE {} = {} LIMIT 1",
        mysql_quote_ident(&table.db),
        mysql_quote_ident(&table.table),
        mysql_quote_ident(pk_name),
        last_insert_id
    ))
}

async fn mysql_rollback_transaction(state: &AppState, undo_sql: &[String]) -> Result<(), RpcError> {
    for sql in undo_sql.iter().rev() {
        let params = SqlExecParams {
            sql: sql.clone(),
            explain: false,
            default_db: None,
            result_format: Some(ResultFormat::RowsJson),
        };
        sql_exec(state, params).await?;
    }
    Ok(())
}

async fn mysql_try_compat_query_outcome(
    state: &AppState,
    sql: &str,
    default_db: Option<&str>,
) -> Result<Option<MySqlQueryOutcome>, RpcError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let lower = trimmed.to_ascii_lowercase();

    if let Some(result) = mysql_try_count_query_outcome(state, trimmed, default_db).await? {
        return Ok(Some(result));
    }

    if lower.starts_with("show variables like ") {
        let name = parse_sql_string_literal(trimmed[20..].trim())
            .unwrap_or_else(|| clean_sql_ident(&trimmed[20..]));
        let value = match name.to_ascii_lowercase().as_str() {
            "sql_mode" => Some(String::new()),
            "lower_case_table_names" => Some("0".to_string()),
            _ => None,
        };
        let rows = value
            .into_iter()
            .map(|v| vec![Some(name.clone()), Some(v)])
            .collect();
        return Ok(Some(MySqlQueryOutcome::ResultSet {
            columns: vec!["Variable_name".to_string(), "Value".to_string()],
            rows,
        }));
    }

    if lower.starts_with("show status like ") {
        let name = parse_sql_string_literal(trimmed[17..].trim())
            .unwrap_or_else(|| clean_sql_ident(&trimmed[17..]));
        let value = match name.to_ascii_lowercase().as_str() {
            "threads_connected" => Some("1".to_string()),
            _ => None,
        };
        let rows = value
            .into_iter()
            .map(|v| vec![Some(name.clone()), Some(v)])
            .collect();
        return Ok(Some(MySqlQueryOutcome::ResultSet {
            columns: vec!["Variable_name".to_string(), "Value".to_string()],
            rows,
        }));
    }

    if lower == "show engines" {
        return Ok(Some(MySqlQueryOutcome::ResultSet {
            columns: vec![
                "Engine".to_string(),
                "Support".to_string(),
                "Comment".to_string(),
                "Transactions".to_string(),
                "XA".to_string(),
                "Savepoints".to_string(),
            ],
            rows: vec![
                vec![
                    Some("InnoDB".to_string()),
                    Some("DEFAULT".to_string()),
                    Some("SkeinDB compatibility engine".to_string()),
                    Some("YES".to_string()),
                    Some("NO".to_string()),
                    Some("NO".to_string()),
                ],
                vec![
                    Some("SKEIN".to_string()),
                    Some("YES".to_string()),
                    Some("Native SkeinDB execution".to_string()),
                    Some("YES".to_string()),
                    Some("NO".to_string()),
                    Some("NO".to_string()),
                ],
            ],
        }));
    }

    if lower == "show grants" {
        return Ok(Some(MySqlQueryOutcome::ResultSet {
            columns: vec!["Grants for root@%".to_string()],
            rows: vec![vec![Some(
                "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%'".to_string(),
            )]],
        }));
    }

    if lower.starts_with("show full tables") || lower.starts_with("show tables") {
        let full = lower.starts_with("show full tables");
        let prefix_len = if full { 16 } else { 11 };
        let tail = trimmed[prefix_len..].trim();
        let like_idx = find_keyword_top_level(tail, "like");
        let where_idx = find_keyword_top_level(tail, "where");
        let stop_idx = [like_idx, where_idx]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(tail.len());
        let scope_sql = tail[..stop_idx].trim();
        let like_pattern =
            like_idx.and_then(|idx| parse_sql_string_literal(tail[idx + 4..].trim()));
        let where_sql = where_idx.map(|idx| tail[idx + 5..].trim());

        let db = if scope_sql.is_empty() {
            default_db.map(clean_sql_ident).filter(|db| !db.is_empty())
        } else {
            let scope_lower = scope_sql.to_ascii_lowercase();
            if scope_lower.starts_with("from ") || scope_lower.starts_with("in ") {
                let name = clean_sql_ident(scope_sql[4..].trim());
                (!name.is_empty()).then_some(name)
            } else {
                return Err(RpcError::new(
                    "not_supported",
                    "SHOW TABLES supports only optional FROM/IN and LIKE clauses",
                ));
            }
        }
        .ok_or_else(|| {
            RpcError::new(
                "invalid_request",
                "SHOW TABLES requires FROM <db> or a selected database",
            )
        })?;

        if let Some(where_sql) = where_sql {
            let normalized = where_sql
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase();
            let accepted = normalized == "table_type = 'base table'"
                || normalized == "table_type='base table'";
            if !full || !accepted {
                return Err(RpcError::new(
                    "not_supported",
                    "SHOW FULL TABLES WHERE currently supports only Table_type = 'BASE TABLE'",
                ));
            }
        }

        let eng = state.engine.read().await;
        let tables = eng.list_tables(&db).map_err(to_rpc_error)?;
        let rows = tables
            .into_iter()
            .filter(|table| {
                like_pattern
                    .as_deref()
                    .map(|pattern| mysql_like_matches(table, pattern))
                    .unwrap_or(true)
            })
            .map(|table| {
                if full {
                    vec![Some(table), Some("BASE TABLE".to_string())]
                } else {
                    vec![Some(table)]
                }
            })
            .collect();
        let mut columns = vec![format!("Tables_in_{db}")];
        if full {
            columns.push("Table_type".to_string());
        }
        return Ok(Some(MySqlQueryOutcome::ResultSet { columns, rows }));
    }

    if lower.starts_with("show table status") {
        let tail = trimmed[17..].trim();
        let like_idx = find_keyword_top_level(tail, "like");
        let scope_sql = like_idx.map(|idx| tail[..idx].trim()).unwrap_or(tail);
        let like_pattern =
            like_idx.and_then(|idx| parse_sql_string_literal(tail[idx + 4..].trim()));

        let db = if scope_sql.is_empty() {
            default_db.map(clean_sql_ident).filter(|db| !db.is_empty())
        } else {
            let scope_lower = scope_sql.to_ascii_lowercase();
            if scope_lower.starts_with("from ") || scope_lower.starts_with("in ") {
                let name = clean_sql_ident(scope_sql[4..].trim());
                (!name.is_empty()).then_some(name)
            } else {
                return Err(RpcError::new(
                    "not_supported",
                    "SHOW TABLE STATUS supports only optional FROM/IN and LIKE clauses",
                ));
            }
        }
        .ok_or_else(|| {
            RpcError::new(
                "invalid_request",
                "SHOW TABLE STATUS requires FROM <db> or a selected database",
            )
        })?;

        let eng = state.engine.read().await;
        let tables = eng.list_tables(&db).map_err(to_rpc_error)?;
        let rows = tables
            .into_iter()
            .filter(|table| {
                like_pattern
                    .as_deref()
                    .map(|pattern| mysql_like_matches(table, pattern))
                    .unwrap_or(true)
            })
            .map(|table| {
                vec![
                    Some(table),
                    Some("InnoDB".to_string()),
                    Some("10".to_string()),
                    Some("Dynamic".to_string()),
                    Some("0".to_string()),
                    Some("0".to_string()),
                    Some("0".to_string()),
                    Some("0".to_string()),
                    Some("0".to_string()),
                    Some("0".to_string()),
                    None,
                    None,
                    None,
                    None,
                    Some("utf8mb4_general_ci".to_string()),
                    None,
                    Some(String::new()),
                    Some("compatibility metadata".to_string()),
                ]
            })
            .collect();
        return Ok(Some(MySqlQueryOutcome::ResultSet {
            columns: vec![
                "Name".to_string(),
                "Engine".to_string(),
                "Version".to_string(),
                "Row_format".to_string(),
                "Rows".to_string(),
                "Avg_row_length".to_string(),
                "Data_length".to_string(),
                "Max_data_length".to_string(),
                "Index_length".to_string(),
                "Data_free".to_string(),
                "Auto_increment".to_string(),
                "Create_time".to_string(),
                "Update_time".to_string(),
                "Check_time".to_string(),
                "Collation".to_string(),
                "Checksum".to_string(),
                "Create_options".to_string(),
                "Comment".to_string(),
            ],
            rows,
        }));
    }

    if lower.starts_with("show columns from ") || lower.starts_with("show full columns from ") {
        let full = lower.starts_with("show full columns from ");
        let SqlPlan::ShowColumns { table } = parse_show_plan(trimmed, default_db)? else {
            return Ok(None);
        };
        let eng = state.engine.read().await;
        let desc = eng
            .describe_table(&table.db, &table.table)
            .map_err(to_rpc_error)?;
        return Ok(Some(mysql_show_columns_outcome(&desc, full)));
    }

    if lower.starts_with("describe ") || lower.starts_with("desc ") {
        let prefix_len = if lower.starts_with("describe ") { 9 } else { 5 };
        let tail = trimmed[prefix_len..].trim();
        let table = parse_table_ref(tail, default_db)?;
        let eng = state.engine.read().await;
        let desc = eng
            .describe_table(&table.db, &table.table)
            .map_err(to_rpc_error)?;
        return Ok(Some(mysql_show_columns_outcome(&desc, false)));
    }

    if lower.starts_with("show index from ") || lower.starts_with("show indexes from ") {
        let prefix_len = if lower.starts_with("show indexes from ") {
            18
        } else {
            15
        };
        let tail = trimmed[prefix_len..].trim();
        let table = parse_table_ref(tail, default_db)?;
        let eng = state.engine.read().await;
        let desc = eng
            .describe_table(&table.db, &table.table)
            .map_err(to_rpc_error)?;
        return Ok(Some(mysql_show_index_outcome(&table.table, &desc)));
    }

    if lower.starts_with("show keys from ") {
        let table = parse_table_ref(trimmed[14..].trim(), default_db)?;
        let eng = state.engine.read().await;
        let desc = eng
            .describe_table(&table.db, &table.table)
            .map_err(to_rpc_error)?;
        return Ok(Some(mysql_show_index_outcome(&table.table, &desc)));
    }

    if lower.starts_with("show create table ") {
        let tail = trimmed[18..].trim();
        let table = parse_table_ref(tail, default_db)?;
        let eng = state.engine.read().await;
        let desc = eng
            .describe_table(&table.db, &table.table)
            .map_err(to_rpc_error)?;
        return Ok(Some(MySqlQueryOutcome::ResultSet {
            columns: vec!["Table".to_string(), "Create Table".to_string()],
            rows: vec![vec![
                Some(table.table.clone()),
                Some(mysql_render_create_table(&table.table, &desc)),
            ]],
        }));
    }

    Ok(None)
}

fn mysql_parse_select_found_rows_query(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed.is_ascii() || trimmed.len() < 6 || !trimmed[..6].eq_ignore_ascii_case("select") {
        return None;
    }
    let rest = trimmed[6..].trim();
    if rest.len() < "found_rows()".len() {
        return None;
    }
    if !rest[..12].eq_ignore_ascii_case("found_rows()") {
        return None;
    }
    let tail = rest[12..].trim();
    if tail.is_empty() {
        return Some("FOUND_ROWS()".to_string());
    }
    let tail_lower = tail.to_ascii_lowercase();
    let alias = if tail_lower.starts_with("as ") {
        clean_sql_ident(tail[3..].trim())
    } else {
        clean_sql_ident(tail)
    };
    if alias.is_empty() || alias.contains(char::is_whitespace) || alias.contains(',') {
        return None;
    }
    Some(alias)
}

fn mysql_rewrite_sql_calc_found_rows(sql: &str) -> Option<String> {
    const TOKEN: &str = "sql_calc_found_rows";
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed.is_ascii() || trimmed.len() < 6 || !trimmed[..6].eq_ignore_ascii_case("select") {
        return None;
    }
    let rest = trimmed[6..].trim_start();
    if rest.len() < TOKEN.len() || !rest[..TOKEN.len()].eq_ignore_ascii_case(TOKEN) {
        return None;
    }
    if rest.len() > TOKEN.len() && !rest.as_bytes()[TOKEN.len()].is_ascii_whitespace() {
        return None;
    }
    let tail = rest[TOKEN.len()..].trim_start();
    if tail.is_empty() {
        return None;
    }
    Some(format!("SELECT {}", tail))
}

fn mysql_push_lenenc_int(buf: &mut Vec<u8>, n: usize) {
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

fn mysql_push_lenenc_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    mysql_push_lenenc_int(buf, bytes.len());
    buf.extend_from_slice(bytes);
}

fn mysql_column_type(lit: &MySqlLiteral) -> u8 {
    match lit {
        MySqlLiteral::Int(_) => 0x08,
        MySqlLiteral::Str(_) => 0xfd,
        MySqlLiteral::Null => 0x06,
    }
}

fn mysql_literal_text(lit: &MySqlLiteral) -> Option<String> {
    match lit {
        MySqlLiteral::Int(v) => Some(v.to_string()),
        MySqlLiteral::Str(v) => Some(v.clone()),
        MySqlLiteral::Null => None,
    }
}

fn mysql_column_definition_packet_with_type(name: &str, column_type: u8, len: u32) -> Vec<u8> {
    let mut payload = Vec::new();
    mysql_push_lenenc_bytes(&mut payload, b"def");
    mysql_push_lenenc_bytes(&mut payload, b"");
    mysql_push_lenenc_bytes(&mut payload, b"");
    mysql_push_lenenc_bytes(&mut payload, b"");
    mysql_push_lenenc_bytes(&mut payload, name.as_bytes());
    mysql_push_lenenc_bytes(&mut payload, name.as_bytes());
    payload.push(0x0c);
    payload.extend_from_slice(&0x21u16.to_le_bytes());
    payload.extend_from_slice(&len.to_le_bytes());
    payload.push(column_type);
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&[0u8; 2]);
    payload
}

fn mysql_column_definition_packet(name: &str, lit: &MySqlLiteral) -> Vec<u8> {
    let len = match lit {
        MySqlLiteral::Int(_) => 20u32,
        MySqlLiteral::Str(v) => v.len().max(1) as u32,
        MySqlLiteral::Null => 4u32,
    };
    mysql_column_definition_packet_with_type(name, mysql_column_type(lit), len)
}

fn mysql_eof_packet() -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0xfe);
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.extend_from_slice(&MYSQL_STATUS_AUTOCOMMIT.to_le_bytes());
    payload
}

fn mysql_text_row_packet(row: &[Option<String>]) -> Vec<u8> {
    let mut payload = Vec::new();
    for cell in row {
        match cell {
            Some(text) => mysql_push_lenenc_bytes(&mut payload, text.as_bytes()),
            None => payload.push(0xfb),
        }
    }
    payload
}

fn mysql_stmt_column_type_code(kind: MySqlStmtColumnType) -> u8 {
    match kind {
        MySqlStmtColumnType::LongLong => 0x08,
        MySqlStmtColumnType::Double => 0x05,
        MySqlStmtColumnType::VarString => 0xfd,
    }
}

fn mysql_stmt_column_type_for_mysql_literal(lit: &MySqlLiteral) -> MySqlStmtColumnType {
    match lit {
        MySqlLiteral::Int(_) => MySqlStmtColumnType::LongLong,
        MySqlLiteral::Str(_) | MySqlLiteral::Null => MySqlStmtColumnType::VarString,
    }
}

fn mysql_stmt_column_type_for_lit(lit: &Lit) -> MySqlStmtColumnType {
    match lit {
        Lit::Bool { .. } | Lit::I64 { .. } | Lit::U64 { .. } => MySqlStmtColumnType::LongLong,
        Lit::F64 { .. } => MySqlStmtColumnType::Double,
        Lit::Null
        | Lit::Dec { .. }
        | Lit::Str { .. }
        | Lit::Date { .. }
        | Lit::Time { .. }
        | Lit::Datetime { .. }
        | Lit::Uuid { .. }
        | Lit::Bytes { .. }
        | Lit::Json { .. }
        | Lit::Embedding { .. } => MySqlStmtColumnType::VarString,
    }
}

fn mysql_stmt_column_type_from_desc_column(column: &Value) -> MySqlStmtColumnType {
    match column
        .get("type")
        .and_then(|v| v.get("kind"))
        .and_then(|v| v.as_str())
        .unwrap_or("string")
    {
        "bool" | "i64" | "u64" => MySqlStmtColumnType::LongLong,
        "f64" => MySqlStmtColumnType::Double,
        _ => MySqlStmtColumnType::VarString,
    }
}

fn mysql_stmt_base_table_alias(base: &BaseTableRef) -> &str {
    base.r#as.as_deref().unwrap_or(&base.table)
}

fn mysql_stmt_prepare_columns_from_select(
    from: Option<&TableRef>,
    projection: &[SelectItem],
    base_desc: Option<&Value>,
) -> Vec<MySqlStmtPrepareColumn> {
    if let Some(cols) = from
        .and_then(|from| match from {
            TableRef::Base(base) => Some(base),
            _ => None,
        })
        .zip(base_desc)
        .filter(|_| projection.is_empty())
        .map(|(_, desc)| {
            desc.get("columns")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        })
    {
        return cols
            .into_iter()
            .filter_map(|column| {
                let name = column.get("name").and_then(|v| v.as_str())?.to_string();
                Some(MySqlStmtPrepareColumn {
                    name,
                    column_type: mysql_stmt_column_type_from_desc_column(&column),
                })
            })
            .collect();
    }

    let base = from.and_then(|from| match from {
        TableRef::Base(base) => Some(base),
        _ => None,
    });
    projection
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let column_type = match &item.expr {
                Expr::Lit { lit } => mysql_stmt_column_type_for_lit(lit),
                Expr::Col { col, table } => {
                    if let Some((base, desc)) = base.zip(base_desc) {
                        let matches_base = table
                            .as_deref()
                            .map(|table_name| {
                                table_name.eq_ignore_ascii_case(&base.table)
                                    || table_name
                                        .eq_ignore_ascii_case(mysql_stmt_base_table_alias(base))
                            })
                            .unwrap_or(true);
                        if matches_base {
                            desc.get("columns")
                                .and_then(|v| v.as_array())
                                .into_iter()
                                .flatten()
                                .find(|column| {
                                    column
                                        .get("name")
                                        .and_then(|v| v.as_str())
                                        .map(|name| name.eq_ignore_ascii_case(col))
                                        .unwrap_or(false)
                                })
                                .map(mysql_stmt_column_type_from_desc_column)
                                .unwrap_or(MySqlStmtColumnType::VarString)
                        } else {
                            MySqlStmtColumnType::VarString
                        }
                    } else {
                        MySqlStmtColumnType::VarString
                    }
                }
                _ => MySqlStmtColumnType::VarString,
            };
            MySqlStmtPrepareColumn {
                name: projection_label(item, idx),
                column_type,
            }
        })
        .collect()
}

async fn mysql_stmt_prepare_columns(
    state: &AppState,
    sql: &str,
    default_db: Option<&str>,
) -> Vec<MySqlStmtPrepareColumn> {
    if let Some(cols) = parse_select_literal_query(sql, default_db) {
        return cols
            .into_iter()
            .map(|(name, lit)| MySqlStmtPrepareColumn {
                name,
                column_type: mysql_stmt_column_type_for_mysql_literal(&lit),
            })
            .collect();
    }

    let Ok(SqlPlan::Select {
        from, projection, ..
    }) = parse_sql_plan(sql, default_db)
    else {
        return Vec::new();
    };

    let base_desc = match from.as_ref() {
        Some(TableRef::Base(base)) => {
            let eng = state.engine.read().await;
            eng.describe_table(&base.db, &base.table).ok()
        }
        _ => None,
    };
    mysql_stmt_prepare_columns_from_select(from.as_ref(), &projection, base_desc.as_ref())
}

fn mysql_stmt_infer_column_types(
    rows: &[Vec<Option<String>>],
    column_count: usize,
) -> Vec<MySqlStmtColumnType> {
    let mut kinds = vec![MySqlStmtColumnType::LongLong; column_count];
    for (col_idx, kind) in kinds.iter_mut().enumerate().take(column_count) {
        let mut saw_value = false;
        let mut all_i64 = true;
        let mut all_f64 = true;
        for row in rows {
            let Some(cell) = row.get(col_idx).and_then(|v| v.as_deref()) else {
                continue;
            };
            saw_value = true;
            if cell.parse::<i64>().is_err() {
                all_i64 = false;
            }
            if cell.parse::<f64>().is_err() {
                all_f64 = false;
            }
            if !all_i64 && !all_f64 {
                break;
            }
        }
        *kind = if !saw_value {
            MySqlStmtColumnType::VarString
        } else if all_i64 {
            MySqlStmtColumnType::LongLong
        } else if all_f64 {
            MySqlStmtColumnType::Double
        } else {
            MySqlStmtColumnType::VarString
        };
    }
    kinds
}

fn mysql_binary_row_packet(
    row: &[Option<String>],
    column_types: &[MySqlStmtColumnType],
) -> Result<Vec<u8>, String> {
    let null_bitmap_len = (column_types.len() + 7 + 2) / 8;
    let mut payload = Vec::new();
    payload.push(0x00);
    payload.resize(1 + null_bitmap_len, 0);
    for (idx, kind) in column_types.iter().enumerate() {
        let Some(value) = row.get(idx).and_then(|v| v.as_deref()) else {
            let bit = idx + 2;
            payload[1 + (bit / 8)] |= 1u8 << (bit % 8);
            continue;
        };
        match kind {
            MySqlStmtColumnType::LongLong => {
                let parsed = value
                    .parse::<i64>()
                    .map_err(|_| format!("invalid prepared integer result '{value}'"))?;
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            MySqlStmtColumnType::Double => {
                let parsed = value
                    .parse::<f64>()
                    .map_err(|_| format!("invalid prepared float result '{value}'"))?;
                payload.extend_from_slice(&parsed.to_le_bytes());
            }
            MySqlStmtColumnType::VarString => {
                mysql_push_lenenc_bytes(&mut payload, value.as_bytes())
            }
        }
    }
    Ok(payload)
}

fn mysql_json_value_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => {
            if *v {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn mysql_value_to_text(value: &Value) -> Option<String> {
    if value.is_null() {
        return None;
    }
    if let Some(obj) = value.as_object() {
        if let Some(kind) = obj.get("t").and_then(|v| v.as_str()) {
            if kind == "null" {
                return None;
            }
            if kind == "bool" {
                return obj.get("v").and_then(|v| v.as_bool()).map(|v| {
                    if v {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                });
            }
            if let Some(inner) = obj.get("v") {
                return Some(mysql_json_value_text(inner));
            }
            if let Some(iso) = obj.get("iso").and_then(|v| v.as_str()) {
                return Some(iso.to_string());
            }
            if let Some(b64) = obj.get("b64").and_then(|v| v.as_str()) {
                return Some(b64.to_string());
            }
            return Some(value.to_string());
        }
    }
    Some(mysql_json_value_text(value))
}

fn mysql_extract_result_data(
    result: &Value,
) -> Result<(Vec<String>, Vec<Vec<Option<String>>>), String> {
    let data = result
        .get("result")
        .and_then(|v| v.get("data"))
        .ok_or_else(|| "missing result.data".to_string())?;
    let columns_json = data
        .get("columns")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing result.data.columns".to_string())?;
    let rows_json = data
        .get("rows")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing result.data.rows".to_string())?;

    let columns = columns_json
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            col.get("name")
                .and_then(|v| v.as_str())
                .or_else(|| col.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("col{}", idx + 1))
        })
        .collect::<Vec<_>>();

    let mut rows = Vec::with_capacity(rows_json.len());
    for row in rows_json {
        let arr = row
            .as_array()
            .ok_or_else(|| "result.data.rows entry must be an array".to_string())?;
        let mut out = arr.iter().map(mysql_value_to_text).collect::<Vec<_>>();
        while out.len() < columns.len() {
            out.push(None);
        }
        rows.push(out);
    }

    Ok((columns, rows))
}

fn mysql_extract_show_columns_result(
    result: &Value,
) -> Result<(Vec<String>, Vec<Vec<Option<String>>>), String> {
    let desc = result
        .get("result")
        .ok_or_else(|| "missing show_columns result".to_string())?;
    let cols = desc
        .get("columns")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing show_columns columns".to_string())?;
    let primary_key: HashSet<String> = desc
        .get("primary_key")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .map(|v| v.to_string())
        .collect();

    let mut rows = Vec::with_capacity(cols.len());
    for col in cols {
        let name = col
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let data_type = col
            .get("type")
            .and_then(|v| v.get("kind"))
            .and_then(|v| v.as_str())
            .unwrap_or("string")
            .to_string();
        let is_nullable = if col
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
        {
            "YES"
        } else {
            "NO"
        }
        .to_string();
        let key = if primary_key.contains(&name) {
            "PRI".to_string()
        } else {
            "".to_string()
        };
        rows.push(vec![
            Some(name),
            Some(data_type),
            Some(is_nullable),
            Some(key),
        ]);
    }

    Ok((
        vec![
            "Field".to_string(),
            "Type".to_string(),
            "Null".to_string(),
            "Key".to_string(),
        ],
        rows,
    ))
}

fn mysql_query_outcome_from_sql_exec(result: &Value) -> Result<MySqlQueryOutcome, String> {
    let statement = result
        .get("statement")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    match statement {
        "select" | "show_databases" | "show_tables" => {
            let (columns, rows) = mysql_extract_result_data(result)?;
            Ok(MySqlQueryOutcome::ResultSet { columns, rows })
        }
        "show_columns" => {
            let (columns, rows) = mysql_extract_show_columns_result(result)?;
            Ok(MySqlQueryOutcome::ResultSet { columns, rows })
        }
        "use" | "create_database" | "create_table" | "alter_table" | "drop_table" => {
            Ok(MySqlQueryOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            })
        }
        "insert" | "replace" | "update" | "delete" => Ok(MySqlQueryOutcome::Ok {
            affected_rows: result
                .get("write")
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            last_insert_id: result
                .get("write")
                .and_then(|v| v.get("last_insert_id"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        }),
        _ => {
            if let Ok((columns, rows)) = mysql_extract_result_data(result) {
                return Ok(MySqlQueryOutcome::ResultSet { columns, rows });
            }
            if result.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                return Ok(MySqlQueryOutcome::Ok {
                    affected_rows: 0,
                    last_insert_id: 0,
                });
            }
            Err(format!("unsupported sql.exec statement '{}'", statement))
        }
    }
}

fn mysql_parse_stmt_execute_params(
    payload: &[u8],
    statement: &mut MySqlPreparedStatement,
) -> Result<Vec<Lit>, String> {
    if payload.len() < 10 {
        return Err("COM_STMT_EXECUTE payload too short".to_string());
    }
    let flags = payload[5];
    if flags & 0xfe != 0 {
        return Err("cursor and bulk-execute flags are not supported yet".to_string());
    }
    let mut cursor = 10usize;
    let param_count = statement.param_count as usize;
    if param_count == 0 {
        statement.long_data.clear();
        return Ok(Vec::new());
    }

    let null_bitmap_len = param_count.div_ceil(8);
    if cursor + null_bitmap_len > payload.len() {
        return Err("truncated COM_STMT_EXECUTE null bitmap".to_string());
    }
    let null_bitmap = &payload[cursor..cursor + null_bitmap_len];
    cursor += null_bitmap_len;
    if cursor >= payload.len() {
        return Err("missing COM_STMT_EXECUTE new-params flag".to_string());
    }
    let new_params_bound = payload[cursor];
    cursor += 1;

    let param_types = if new_params_bound != 0 {
        let needed = param_count
            .checked_mul(2)
            .ok_or_else(|| "parameter metadata too large".to_string())?;
        if cursor + needed > payload.len() {
            return Err("truncated COM_STMT_EXECUTE parameter types".to_string());
        }
        let mut types = Vec::with_capacity(param_count);
        for _ in 0..param_count {
            let type_code = payload[cursor];
            let unsigned = payload[cursor + 1] & 0x80 != 0;
            cursor += 2;
            types.push(MySqlStmtParamType {
                type_code,
                unsigned,
            });
        }
        statement.param_types = types.clone();
        types
    } else if statement.param_types.len() == param_count {
        statement.param_types.clone()
    } else {
        return Err("COM_STMT_EXECUTE requires parameter types on first execution".to_string());
    };

    let mut params = Vec::with_capacity(param_count);
    for (idx, param_type) in param_types.into_iter().enumerate() {
        let long_data = statement.long_data.remove(&(idx as u16));
        if mysql_stmt_param_is_null(null_bitmap, idx) {
            params.push(Lit::Null);
            continue;
        }
        let lit = mysql_decode_stmt_param_lit(param_type, payload, &mut cursor, long_data)?;
        params.push(lit);
    }
    statement.long_data.clear();
    Ok(params)
}

async fn mysql_execute_sql(
    state: &AppState,
    sql: &str,
    session: &mut MySqlSessionState,
) -> Result<MySqlQueryOutcome, MySqlWireError> {
    if let Some(enabled) = mysql_parse_set_autocommit(sql) {
        session.autocommit = enabled;
        if session.autocommit {
            session.tx_active = false;
            session.tx_undo_sql.clear();
        }
        return Ok(MySqlQueryOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        });
    }
    if mysql_is_session_compat_set(sql) {
        return Ok(MySqlQueryOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        });
    }
    if mysql_is_begin(sql) {
        session.tx_active = true;
        session.tx_undo_sql.clear();
        return Ok(MySqlQueryOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        });
    }
    if mysql_is_commit(sql) {
        session.tx_active = false;
        session.tx_undo_sql.clear();
        return Ok(MySqlQueryOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        });
    }
    if mysql_is_rollback(sql) {
        match mysql_rollback_transaction(state, &session.tx_undo_sql).await {
            Ok(()) => {
                session.tx_active = false;
                session.tx_undo_sql.clear();
                return Ok(MySqlQueryOutcome::Ok {
                    affected_rows: 0,
                    last_insert_id: 0,
                });
            }
            Err(err) => return Err(mysql_error_from_rpc(&err)),
        }
    }
    if let Some(column_name) = mysql_parse_select_found_rows_query(sql) {
        let found_rows = i64::try_from(session.last_found_rows).unwrap_or(i64::MAX);
        return Ok(MySqlQueryOutcome::ResultSet {
            columns: vec![column_name],
            rows: vec![vec![Some(found_rows.to_string())]],
        });
    }
    match mysql_try_compat_query_outcome(state, sql, session.default_db.as_deref()).await {
        Ok(Some(outcome)) => return Ok(outcome),
        Ok(None) => {}
        Err(err) => return Err(mysql_error_from_rpc(&err)),
    }

    let rewritten = mysql_rewrite_sql_calc_found_rows(sql);
    let exec_sql = rewritten.clone().unwrap_or_else(|| sql.to_string());
    let calc_found_rows = rewritten.is_some();

    if !calc_found_rows {
        if let Some(cols) = parse_select_literal_query(&exec_sql, session.default_db.as_deref()) {
            let columns = cols
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            let rows = vec![cols
                .iter()
                .map(|(_, lit)| mysql_literal_text(lit))
                .collect::<Vec<_>>()];
            return Ok(MySqlQueryOutcome::ResultSet { columns, rows });
        }
    }

    let params = SqlExecParams {
        sql: exec_sql.clone(),
        explain: false,
        default_db: session.default_db.clone(),
        result_format: Some(ResultFormat::RowsJson),
    };
    let result = sql_exec(state, params)
        .await
        .map_err(|err| mysql_error_from_rpc(&err))?;

    if result.get("statement").and_then(|v| v.as_str()) == Some("use") {
        session.default_db = result
            .get("default_db")
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());
    }
    if calc_found_rows {
        match mysql_select_total_rows_without_limit(state, &exec_sql, session.default_db.as_deref())
            .await
        {
            Ok(total) => session.last_found_rows = total,
            Err(err) => return Err(mysql_error_from_rpc(&err)),
        }
    }

    let statement = result
        .get("statement")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let transactional_write = matches!(statement, "insert" | "update" | "delete")
        && (!session.autocommit || session.tx_active);
    if transactional_write {
        session.tx_active = true;
        if statement == "insert" {
            if let Some(undo_sql) = mysql_build_insert_undo_sql(
                state,
                &exec_sql,
                session.default_db.as_deref(),
                &result,
            )
            .await
            {
                session.tx_undo_sql.push(undo_sql);
            }
        }
    }

    mysql_query_outcome_from_sql_exec(&result).map_err(|msg| (1105, "HY000", msg))
}

fn mysql_handshake_packet(connection_id: u32, seed: &[u8; 20]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(96);
    payload.push(MYSQL_PROTOCOL_VERSION);
    payload.extend_from_slice(MYSQL_SERVER_VERSION.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&connection_id.to_le_bytes());
    payload.extend_from_slice(&seed[..8]);
    payload.push(0);
    payload.extend_from_slice(&(MYSQL_SERVER_CAPABILITIES as u16).to_le_bytes());
    payload.push(0x21);
    payload.extend_from_slice(&MYSQL_STATUS_AUTOCOMMIT.to_le_bytes());
    payload.extend_from_slice(&((MYSQL_SERVER_CAPABILITIES >> 16) as u16).to_le_bytes());
    payload.push((seed.len() + 1) as u8);
    payload.extend_from_slice(&[0u8; 10]);
    let mut part2 = seed[8..].to_vec();
    if part2.len() < 13 {
        part2.resize(13, 0);
    }
    payload.extend_from_slice(&part2);
    payload.extend_from_slice(MYSQL_AUTH_PLUGIN.as_bytes());
    payload.push(0);
    payload
}

fn mysql_ok_packet() -> Vec<u8> {
    mysql_ok_packet_with(0, 0)
}

fn mysql_ok_packet_with(affected_rows: u64, last_insert_id: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0x00);
    mysql_push_lenenc_int(&mut payload, affected_rows as usize);
    mysql_push_lenenc_int(&mut payload, last_insert_id as usize);
    payload.extend_from_slice(&MYSQL_STATUS_AUTOCOMMIT.to_le_bytes());
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload
}

fn mysql_err_packet(code: u16, sql_state: &str, message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0xff);
    payload.extend_from_slice(&code.to_le_bytes());
    payload.push(b'#');
    let mut state = [b'H', b'Y', b'0', b'0', b'0'];
    for (idx, byte) in sql_state.as_bytes().iter().take(5).enumerate() {
        state[idx] = *byte;
    }
    payload.extend_from_slice(&state);
    payload.extend_from_slice(message.as_bytes());
    payload
}

async fn mysql_write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) -> anyhow::Result<()> {
    if payload.len() > 0x00ff_ffff {
        return Err(anyhow::anyhow!("mysql payload too large"));
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

async fn mysql_read_packet(stream: &mut TcpStream) -> anyhow::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok((seq, payload))
}

async fn mysql_send_text_result(
    stream: &mut TcpStream,
    start_seq: u8,
    columns: &[String],
    rows: &[Vec<Option<String>>],
) -> anyhow::Result<()> {
    let mut seq = start_seq;
    let mut column_count = Vec::new();
    mysql_push_lenenc_int(&mut column_count, columns.len());
    mysql_write_packet(stream, seq, &column_count).await?;
    seq = seq.wrapping_add(1);

    for name in columns {
        let packet = mysql_column_definition_packet(name, &MySqlLiteral::Str(String::new()));
        mysql_write_packet(stream, seq, &packet).await?;
        seq = seq.wrapping_add(1);
    }

    mysql_write_packet(stream, seq, &mysql_eof_packet()).await?;
    seq = seq.wrapping_add(1);
    for row in rows {
        let packet = mysql_text_row_packet(row);
        mysql_write_packet(stream, seq, &packet).await?;
        seq = seq.wrapping_add(1);
    }
    mysql_write_packet(stream, seq, &mysql_eof_packet()).await?;
    Ok(())
}

fn mysql_prepare_ok_packet(statement_id: u32, column_count: u16, param_count: u16) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0x00);
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.extend_from_slice(&column_count.to_le_bytes());
    payload.extend_from_slice(&param_count.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload
}

async fn mysql_send_prepare_ok(
    stream: &mut TcpStream,
    start_seq: u8,
    statement_id: u32,
    param_count: u16,
    result_columns: &[MySqlStmtPrepareColumn],
) -> anyhow::Result<()> {
    let mut seq = start_seq;
    let packet = mysql_prepare_ok_packet(statement_id, result_columns.len() as u16, param_count);
    mysql_write_packet(stream, seq, &packet).await?;
    seq = seq.wrapping_add(1);
    if param_count > 0 {
        for idx in 0..param_count {
            let name = format!("param{}", idx + 1);
            let packet = mysql_column_definition_packet_with_type(&name, 0xfd, 255);
            mysql_write_packet(stream, seq, &packet).await?;
            seq = seq.wrapping_add(1);
        }
        mysql_write_packet(stream, seq, &mysql_eof_packet()).await?;
        seq = seq.wrapping_add(1);
    }
    if !result_columns.is_empty() {
        for column in result_columns {
            let len = match column.column_type {
                MySqlStmtColumnType::LongLong => 20,
                MySqlStmtColumnType::Double => 24,
                MySqlStmtColumnType::VarString => 255,
            };
            let packet = mysql_column_definition_packet_with_type(
                &column.name,
                mysql_stmt_column_type_code(column.column_type),
                len,
            );
            mysql_write_packet(stream, seq, &packet).await?;
            seq = seq.wrapping_add(1);
        }
        mysql_write_packet(stream, seq, &mysql_eof_packet()).await?;
    }
    Ok(())
}

async fn mysql_send_binary_result(
    stream: &mut TcpStream,
    start_seq: u8,
    columns: &[String],
    rows: &[Vec<Option<String>>],
    prepared_columns: Option<&[MySqlStmtPrepareColumn]>,
) -> anyhow::Result<()> {
    let mut seq = start_seq;
    let mut column_count = Vec::new();
    mysql_push_lenenc_int(&mut column_count, columns.len());
    mysql_write_packet(stream, seq, &column_count).await?;
    seq = seq.wrapping_add(1);

    let mut column_types = mysql_stmt_infer_column_types(rows, columns.len());
    if rows.is_empty() {
        if let Some(prepared_columns) = prepared_columns {
            if prepared_columns.len() == columns.len()
                && prepared_columns
                    .iter()
                    .zip(columns.iter())
                    .all(|(prepared, actual)| prepared.name.eq_ignore_ascii_case(actual))
            {
                column_types = prepared_columns
                    .iter()
                    .map(|prepared| prepared.column_type)
                    .collect();
            }
        }
    }
    for (idx, name) in columns.iter().enumerate() {
        let len = match column_types[idx] {
            MySqlStmtColumnType::LongLong => 20,
            MySqlStmtColumnType::Double => 24,
            MySqlStmtColumnType::VarString => rows
                .iter()
                .filter_map(|row| row.get(idx).and_then(|v| v.as_ref()))
                .map(|value| value.len() as u32)
                .max()
                .unwrap_or(1),
        };
        let packet = mysql_column_definition_packet_with_type(
            name,
            mysql_stmt_column_type_code(column_types[idx]),
            len,
        );
        mysql_write_packet(stream, seq, &packet).await?;
        seq = seq.wrapping_add(1);
    }

    mysql_write_packet(stream, seq, &mysql_eof_packet()).await?;
    seq = seq.wrapping_add(1);
    for row in rows {
        let packet =
            mysql_binary_row_packet(row, &column_types).map_err(|msg| anyhow::anyhow!(msg))?;
        mysql_write_packet(stream, seq, &packet).await?;
        seq = seq.wrapping_add(1);
    }
    mysql_write_packet(stream, seq, &mysql_eof_packet()).await?;
    Ok(())
}

async fn handle_mysql_connection(
    state: AppState,
    mut stream: TcpStream,
    connection_id: u32,
) -> anyhow::Result<()> {
    let seed = mysql_seed(connection_id);
    let handshake = mysql_handshake_packet(connection_id, &seed);
    mysql_write_packet(&mut stream, 0, &handshake).await?;

    let (seq, response_payload) = mysql_read_packet(&mut stream).await?;
    let response = match parse_mysql_handshake_response(&response_payload) {
        Ok(parsed) => parsed,
        Err(reason) => {
            let packet = mysql_err_packet(1047, "08S01", &format!("malformed handshake: {reason}"));
            mysql_write_packet(&mut stream, seq.wrapping_add(1), &packet).await?;
            return Ok(());
        }
    };

    if response.capabilities & MYSQL_CAP_SSL != 0 {
        let packet = mysql_err_packet(1047, "08S01", "TLS is not supported on this listener");
        mysql_write_packet(&mut stream, seq.wrapping_add(1), &packet).await?;
        return Ok(());
    }

    if let Some(plugin) = response.auth_plugin.as_deref() {
        if plugin != MYSQL_AUTH_PLUGIN {
            let packet = mysql_err_packet(1251, "08004", "unsupported auth plugin");
            mysql_write_packet(&mut stream, seq.wrapping_add(1), &packet).await?;
            return Ok(());
        }
    }

    if let Ok(expected_password) = std::env::var("SKEINDB_TOKEN") {
        if !mysql_validate_native_password(&expected_password, &seed, &response.auth_response) {
            let packet = mysql_err_packet(1045, "28000", "access denied");
            mysql_write_packet(&mut stream, seq.wrapping_add(1), &packet).await?;
            return Ok(());
        }
    }

    let username = response.username;
    let mut session = MySqlSessionState::new(response.database);
    let mut prepared_statements = HashMap::<u32, MySqlPreparedStatement>::new();
    let mut next_statement_id = 1u32;
    let ok = mysql_ok_packet();
    mysql_write_packet(&mut stream, seq.wrapping_add(1), &ok).await?;

    loop {
        let (cmd_seq, command_payload) = match mysql_read_packet(&mut stream).await {
            Ok(packet) => packet,
            Err(err) => {
                let disconnect = err
                    .downcast_ref::<std::io::Error>()
                    .map(|io| {
                        matches!(
                            io.kind(),
                            std::io::ErrorKind::UnexpectedEof
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::BrokenPipe
                        )
                    })
                    .unwrap_or(false);
                if disconnect {
                    return Ok(());
                }
                return Err(err);
            }
        };
        if command_payload.is_empty() {
            continue;
        }
        match command_payload[0] {
            0x01 => {
                return Ok(());
            }
            0x0e => {
                mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &mysql_ok_packet())
                    .await?;
            }
            0x03 => {
                let sql = String::from_utf8_lossy(&command_payload[1..]).to_string();
                match mysql_execute_sql(&state, &sql, &mut session).await {
                    Ok(MySqlQueryOutcome::ResultSet { columns, rows }) => {
                        mysql_send_text_result(
                            &mut stream,
                            cmd_seq.wrapping_add(1),
                            &columns,
                            &rows,
                        )
                        .await?;
                    }
                    Ok(MySqlQueryOutcome::Ok {
                        affected_rows,
                        last_insert_id,
                    }) => {
                        let packet = mysql_ok_packet_with(affected_rows, last_insert_id);
                        mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    }
                    Err((code, state_code, message)) => {
                        let packet = mysql_err_packet(code, state_code, &message);
                        mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    }
                }
            }
            0x16 => {
                let sql = String::from_utf8_lossy(&command_payload[1..]).to_string();
                let statement_id = next_statement_id;
                next_statement_id = next_statement_id.wrapping_add(1);
                let param_count = mysql_count_placeholders(&sql);
                let result_columns =
                    mysql_stmt_prepare_columns(&state, &sql, session.default_db.as_deref()).await;
                prepared_statements.insert(
                    statement_id,
                    MySqlPreparedStatement::new(sql, param_count, result_columns.clone()),
                );
                mysql_send_prepare_ok(
                    &mut stream,
                    cmd_seq.wrapping_add(1),
                    statement_id,
                    param_count,
                    &result_columns,
                )
                .await?;
            }
            0x17 => {
                if command_payload.len() < 5 {
                    let packet = mysql_err_packet(1064, "42000", "malformed COM_STMT_EXECUTE");
                    mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    continue;
                }
                let statement_id = u32::from_le_bytes([
                    command_payload[1],
                    command_payload[2],
                    command_payload[3],
                    command_payload[4],
                ]);
                let (sql, prepared_columns) = match prepared_statements.get_mut(&statement_id) {
                    Some(statement) => {
                        match mysql_parse_stmt_execute_params(&command_payload, statement) {
                            Ok(params) => {
                                match mysql_substitute_stmt_sql(&statement.sql, &params) {
                                    Ok(sql) => (sql, statement.result_columns.clone()),
                                    Err(message) => {
                                        let packet = mysql_err_packet(1064, "42000", &message);
                                        mysql_write_packet(
                                            &mut stream,
                                            cmd_seq.wrapping_add(1),
                                            &packet,
                                        )
                                        .await?;
                                        continue;
                                    }
                                }
                            }
                            Err(message) => {
                                let code = if message.contains("unsupported") {
                                    1235
                                } else {
                                    1064
                                };
                                let state_code = "42000";
                                let packet = mysql_err_packet(code, state_code, &message);
                                mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet)
                                    .await?;
                                continue;
                            }
                        }
                    }
                    None => {
                        let packet =
                            mysql_err_packet(1243, "HY000", "unknown prepared statement handler");
                        mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                        continue;
                    }
                };
                match mysql_execute_sql(&state, &sql, &mut session).await {
                    Ok(MySqlQueryOutcome::ResultSet { columns, rows }) => {
                        mysql_send_binary_result(
                            &mut stream,
                            cmd_seq.wrapping_add(1),
                            &columns,
                            &rows,
                            Some(&prepared_columns),
                        )
                        .await?;
                    }
                    Ok(MySqlQueryOutcome::Ok {
                        affected_rows,
                        last_insert_id,
                    }) => {
                        let packet = mysql_ok_packet_with(affected_rows, last_insert_id);
                        mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    }
                    Err((code, state_code, message)) => {
                        let packet = mysql_err_packet(code, state_code, &message);
                        mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    }
                }
            }
            0x18 => {
                if command_payload.len() < 7 {
                    let packet =
                        mysql_err_packet(1064, "42000", "malformed COM_STMT_SEND_LONG_DATA");
                    mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    continue;
                }
                let statement_id = u32::from_le_bytes([
                    command_payload[1],
                    command_payload[2],
                    command_payload[3],
                    command_payload[4],
                ]);
                let param_id = u16::from_le_bytes([command_payload[5], command_payload[6]]);
                let Some(statement) = prepared_statements.get_mut(&statement_id) else {
                    let packet =
                        mysql_err_packet(1243, "HY000", "unknown prepared statement handler");
                    mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    continue;
                };
                statement
                    .long_data
                    .entry(param_id)
                    .or_default()
                    .extend_from_slice(&command_payload[7..]);
                mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &mysql_ok_packet())
                    .await?;
            }
            0x19 => {
                if command_payload.len() >= 5 {
                    let statement_id = u32::from_le_bytes([
                        command_payload[1],
                        command_payload[2],
                        command_payload[3],
                        command_payload[4],
                    ]);
                    prepared_statements.remove(&statement_id);
                }
                continue;
            }
            0x1a => {
                if command_payload.len() < 5 {
                    let packet = mysql_err_packet(1064, "42000", "malformed COM_STMT_RESET");
                    mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    continue;
                }
                let statement_id = u32::from_le_bytes([
                    command_payload[1],
                    command_payload[2],
                    command_payload[3],
                    command_payload[4],
                ]);
                let Some(statement) = prepared_statements.get_mut(&statement_id) else {
                    let packet =
                        mysql_err_packet(1243, "HY000", "unknown prepared statement handler");
                    mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
                    continue;
                };
                statement.long_data.clear();
                mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &mysql_ok_packet())
                    .await?;
            }
            _ => {
                let packet = mysql_err_packet(1047, "08S01", "unsupported command");
                mysql_write_packet(&mut stream, cmd_seq.wrapping_add(1), &packet).await?;
            }
        }
        tracing::debug!(user = %username, "processed MySQL command");
    }
}

async fn run_mysql_listener(
    state: AppState,
    bind: String,
    mysql_port: u16,
    mut shutdown_rx: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    if mysql_port == 0 {
        return Ok(());
    }
    let addr = format!("{}:{}", bind, mysql_port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(mysql_addr = %addr, "MySQL listening");
    let mut connection_id: u32 = 1;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = match accepted {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::warn!(?err, "MySQL accept failed");
                        continue;
                    }
                };
                let cid = connection_id;
                connection_id = connection_id.wrapping_add(1).max(1);
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_mysql_connection(state, stream, cid).await {
                        tracing::debug!(%peer_addr, ?err, "MySQL handshake failed");
                    }
                });
            }
        }
    }
    Ok(())
}

pub async fn serve(opts: ServeOpts) -> anyhow::Result<()> {
    let http_addr: SocketAddr = format!("{}:{}", opts.bind, opts.http_port).parse()?;

    // Ensure data dir exists.
    let data_dir = PathBuf::from(&opts.data_dir);
    std::fs::create_dir_all(&data_dir)?;

    let engine = Engine::open_with_storage_mode_name(&data_dir, &opts.storage_mode)?;
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
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

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
        txns: Arc::new(Mutex::new(HashMap::new())),
        engine: Arc::new(RwLock::new(engine)),
        subs: Arc::new(Mutex::new(Subscriptions::default())),
        coalesce: Arc::new(QueryCoalescer::default()),
        transport,
        shutdown_tx,
    };

    // Load persisted settings if present.
    load_settings(&state).ok();
    load_cluster_state(&state).ok();

    let app_state = state.clone();
    let app = Router::new()
        .route("/api/v1/rpc", post(rpc_handler))
        .route("/api/v1/sql/exec", post(sql_exec_http_handler))
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
    let mysql_handle = if opts.mysql_port == 0 {
        None
    } else {
        let state = app_state.clone();
        let bind = opts.bind.clone();
        let mysql_port = opts.mysql_port;
        let shutdown_rx = app_state.shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            if let Err(err) = run_mysql_listener(state, bind, mysql_port, shutdown_rx).await {
                tracing::error!(?err, "MySQL listener failed");
            }
        }))
    };

    tracing::info!(
        bind = %opts.bind,
        http_port = %opts.http_port,
        mysql_port = %opts.mysql_port,
        cluster_port = %opts.cluster_port,
        storage_mode = %opts.storage_mode,
        "SkeinDB server starting"
    );
    if opts.mysql_port == 0 {
        tracing::info!("MySQL listener disabled (--mysql 0)");
    } else {
        tracing::info!(
            "MySQL listener enabled (handshake/auth + COM_QUERY plus baseline COM_STMT_* compatibility via sql.exec translator)"
        );
    }

    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    tracing::info!(%http_addr, "HTTP listening");

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_flag = shutdown_requested.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = shutdown_signal() => {},
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() {
                        tracing::info!("shutdown requested via system.shutdown");
                    }
                }
            }
            shutdown_flag.store(true, Ordering::SeqCst);
        })
        .await?;

    if let Some(handle) = quic_handle {
        handle.abort();
    }
    if let Some(handle) = mysql_handle {
        handle.abort();
    }

    if shutdown_requested.load(Ordering::SeqCst) {
        run_shutdown_tasks(&app_state).await;
    }

    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut sigterm) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        } else {
            let _ = tokio::signal::ctrl_c().await;
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    tracing::info!("shutdown signal received");
}

async fn run_shutdown_tasks(state: &AppState) {
    if let Err(err) = checkpoint_engine_for_shutdown(state).await {
        tracing::warn!(error = %err, "shutdown checkpoint failed");
    }
    if let Err(err) = mark_local_node_offline(state) {
        tracing::warn!(error = %err, "failed to mark local node offline");
    }
    if let Err(err) = notify_cluster_node_leave(state).await {
        tracing::warn!(error = %err, "failed to notify peers about node leave");
    }
}

async fn checkpoint_engine_for_shutdown(state: &AppState) -> anyhow::Result<()> {
    let mut engine = state.engine.write().await;
    engine.checkpoint_for_shutdown()
}

fn mark_local_node_offline(state: &AppState) -> anyhow::Result<()> {
    let now = now_unix_ms_u64();
    {
        let mut cluster = state.cluster.lock().unwrap();
        let local_node_id = cluster.local_node_id.clone();
        let Some(local_node) = cluster
            .nodes
            .iter_mut()
            .find(|n| n.node_id == local_node_id)
        else {
            return Ok(());
        };
        local_node.status = "offline".to_string();
        local_node.last_seen_ms = now;
    }
    persist_cluster_state(state)?;
    Ok(())
}

async fn notify_cluster_node_leave(state: &AppState) -> anyhow::Result<()> {
    let (enabled, local_node_id, targets) = {
        let cluster = state.cluster.lock().unwrap();
        (
            cluster.enabled,
            cluster.local_node_id.clone(),
            cluster
                .nodes
                .iter()
                .filter(|node| node.node_id != cluster.local_node_id && node.status == "online")
                .map(|node| node.rpc_url.clone())
                .collect::<Vec<_>>(),
        )
    };

    if !enabled || targets.is_empty() {
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?;
    let auth_token = std::env::var("SKEINDB_TOKEN").ok();
    let payload = serde_json::json!({
        "skeinql": SKEINQL_VERSION,
        "method": "cluster.node.leave",
        "params": {
            "node_id": local_node_id,
        },
    });

    for rpc_url in targets {
        let url = format!("{}/api/v1/rpc", rpc_url.trim_end_matches('/'));
        let mut req = client
            .post(&url)
            .header(header::CONTENT_TYPE.as_str(), "application/json")
            .header(REPLICATION_HEADER, "1")
            .json(&payload);
        if let Some(token) = auth_token.as_ref() {
            req = req.bearer_auth(token);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                tracing::warn!(peer = %url, status = %resp.status(), "cluster leave notify failed");
            }
            Err(err) => {
                tracing::warn!(peer = %url, error = %err, "cluster leave transport error");
            }
        }
    }

    Ok(())
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

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                if let Some(v) = map.get(&key) {
                    out.insert(key, canonicalize_json(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        _ => value.clone(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

fn query_fingerprint(method: &str, params: Option<&Value>) -> String {
    let canonical = serde_json::json!({
        "method": method,
        "params": params.map(canonicalize_json).unwrap_or(Value::Null),
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    let digest = skeindb_core::audit_hash256(&bytes);
    hex_encode(&digest)
}

fn query_rows_returned(result: Option<&Value>) -> u64 {
    let Some(value) = result else {
        return 0;
    };

    if let Some(rows) = value
        .get("result")
        .and_then(|v| v.get("data"))
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_array())
    {
        return rows.len() as u64;
    }

    if let Some(rows) = value
        .get("data")
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_array())
    {
        return rows.len() as u64;
    }

    if let Some(rows) = value
        .get("result")
        .and_then(|v| v.get("rows"))
        .and_then(|v| v.as_array())
    {
        return rows.len() as u64;
    }

    0
}

fn percentile_ms(samples: &VecDeque<u64>, percentile: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted: Vec<u64> = samples.iter().copied().collect();
    sorted.sort_unstable();
    let max_idx = sorted.len().saturating_sub(1);
    let rank = ((percentile.clamp(0.0, 100.0) / 100.0) * max_idx as f64).round() as usize;
    sorted[rank.min(max_idx)]
}

fn observe_rpc_call(
    state: &AppState,
    method: &str,
    params: Option<&Value>,
    status: StatusCode,
    ok: bool,
    result: Option<&Value>,
    elapsed: std::time::Duration,
) {
    let now_ms = now_unix_ms_u64();
    let duration_ms = elapsed.as_millis().max(1) as u64;
    let fingerprint = query_fingerprint(method, params);
    let rows_returned = query_rows_returned(result);

    let mut counters = state.counters.lock().unwrap();
    let agg = counters
        .query_stats
        .entry(fingerprint.clone())
        .or_insert_with(|| QueryStatsAgg::new(method.to_string()));
    agg.record(duration_ms, status.as_u16(), ok, rows_returned, now_ms);

    counters.query_log.push_back(QuerySample {
        ts_ms: now_ms,
        method: method.to_string(),
        fingerprint,
        duration_ms,
        status: status.as_u16(),
        ok,
        rows_returned,
    });
    while counters.query_log.len() > QUERY_LOG_CAPACITY {
        counters.query_log.pop_front();
    }
}

fn stats_top_queries(state: &AppState, params: Option<Value>) -> Result<Value, RpcError> {
    #[derive(Clone)]
    struct Row {
        method: String,
        fingerprint: String,
        count: u64,
        error_count: u64,
        total_ms: u64,
        avg_ms: f64,
        p95_ms: u64,
        max_ms: u64,
        rows_returned: u64,
        last_status: u16,
        last_seen_ms: u64,
    }

    let limit = params
        .as_ref()
        .and_then(|v| v.get("limit"))
        .and_then(|v| v.as_u64())
        .unwrap_or(10)
        .clamp(1, 100) as usize;

    let sort_by = params
        .as_ref()
        .and_then(|v| v.get("sort_by"))
        .and_then(|v| v.as_str())
        .unwrap_or("total_ms");

    let mut rows = {
        let counters = state.counters.lock().unwrap();
        counters
            .query_stats
            .iter()
            .map(|(fingerprint, agg)| Row {
                method: agg.method.clone(),
                fingerprint: fingerprint.clone(),
                count: agg.count,
                error_count: agg.error_count,
                total_ms: agg.total_ms,
                avg_ms: agg.avg_ms(),
                p95_ms: percentile_ms(&agg.latency_samples_ms, 95.0),
                max_ms: agg.max_ms,
                rows_returned: agg.rows_returned,
                last_status: agg.last_status,
                last_seen_ms: agg.last_seen_ms,
            })
            .collect::<Vec<_>>()
    };

    rows.sort_by(|a, b| {
        let ord = match sort_by {
            "count" => b.count.cmp(&a.count),
            "avg_ms" => b
                .avg_ms
                .partial_cmp(&a.avg_ms)
                .unwrap_or(std::cmp::Ordering::Equal),
            "p95_ms" => b.p95_ms.cmp(&a.p95_ms),
            "max_ms" => b.max_ms.cmp(&a.max_ms),
            _ => b.total_ms.cmp(&a.total_ms),
        };
        ord.then_with(|| b.count.cmp(&a.count))
            .then_with(|| b.last_seen_ms.cmp(&a.last_seen_ms))
    });

    let queries: Vec<Value> = rows
        .into_iter()
        .take(limit)
        .map(|row| {
            serde_json::json!({
                "method": row.method,
                "fingerprint": row.fingerprint,
                "count": row.count,
                "error_count": row.error_count,
                "total_ms": row.total_ms,
                "avg_ms": row.avg_ms,
                "p95_ms": row.p95_ms,
                "max_ms": row.max_ms,
                "rows_returned": row.rows_returned,
                "last_status": row.last_status,
                "last_seen_ms": row.last_seen_ms,
            })
        })
        .collect();

    Ok(serde_json::json!({
        "limit": limit,
        "sort_by": sort_by,
        "queries": queries,
    }))
}

fn stats_slow_queries(state: &AppState, params: Option<Value>) -> Result<Value, RpcError> {
    let limit = params
        .as_ref()
        .and_then(|v| v.get("limit"))
        .and_then(|v| v.as_u64())
        .unwrap_or(20)
        .clamp(1, 200) as usize;
    let min_ms = params
        .as_ref()
        .and_then(|v| v.get("min_ms"))
        .and_then(|v| v.as_u64())
        .unwrap_or(200);

    let queries = {
        let counters = state.counters.lock().unwrap();
        counters
            .query_log
            .iter()
            .rev()
            .filter(|sample| sample.duration_ms >= min_ms)
            .take(limit)
            .map(|sample| {
                serde_json::json!({
                    "ts_ms": sample.ts_ms,
                    "method": sample.method,
                    "fingerprint": sample.fingerprint,
                    "duration_ms": sample.duration_ms,
                    "status": sample.status,
                    "ok": sample.ok,
                    "rows_returned": sample.rows_returned,
                })
            })
            .collect::<Vec<_>>()
    };

    Ok(serde_json::json!({
        "limit": limit,
        "min_ms": min_ms,
        "queries": queries,
    }))
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
    let started_at = Instant::now();

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
            observe_rpc_call(
                state,
                &req.method,
                req.params.as_ref(),
                StatusCode::UNAUTHORIZED,
                false,
                None,
                started_at.elapsed(),
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
        observe_rpc_call(
            state,
            &req.method,
            req.params.as_ref(),
            StatusCode::OK,
            false,
            None,
            started_at.elapsed(),
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
            observe_rpc_call(
                state,
                &method,
                params.as_ref(),
                StatusCode::NO_CONTENT,
                false,
                None,
                started_at.elapsed(),
            );
            return RpcOutcome {
                status: StatusCode::NO_CONTENT,
                response: None,
            };
        }
        let resp: RpcResponse = RpcResponse::err(
            req.id.clone(),
            RpcError::new("forbidden", "read-only requests cannot perform writes"),
        );
        observe_rpc_call(
            state,
            &method,
            params.as_ref(),
            StatusCode::OK,
            false,
            None,
            started_at.elapsed(),
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
            observe_rpc_call(
                state,
                &method,
                params.as_ref(),
                StatusCode::NO_CONTENT,
                false,
                None,
                started_at.elapsed(),
            );
            return RpcOutcome {
                status: StatusCode::NO_CONTENT,
                response: None,
            };
        }
        let resp: RpcResponse = RpcResponse::err(req.id.clone(), err);
        observe_rpc_call(
            state,
            &method,
            params.as_ref(),
            StatusCode::OK,
            false,
            None,
            started_at.elapsed(),
        );
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
                "system.shutdown" => request_server_shutdown(state),
                "system.capabilities" => Ok(system_capabilities(state)),
                "transport.capabilities" => Ok(transport_capabilities(state)),
                "tx.begin" => {
                    let p = if params.is_some() {
                        parse_params::<TxBeginParams>(params.clone())?
                    } else {
                        TxBeginParams { read_only: false }
                    };
                    tx_begin(state, p)
                }
                "tx.commit" => {
                    let p: TxFinishParams = parse_params(params.clone())?;
                    tx_finish(state, p, "committed")
                }
                "tx.rollback" => {
                    let p: TxFinishParams = parse_params(params.clone())?;
                    tx_finish(state, p, "rolled_back")
                }
                "stats.snapshot" => Ok(stats_snapshot(state).await),
                "stats.top_queries" => stats_top_queries(state, params.clone()),
                "stats.slow_queries" => stats_slow_queries(state, params.clone()),
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
                "cluster.node.leave" => {
                    let p: ClusterNodeLeaveParams = parse_params(params.clone())?;
                    cluster_node_leave(state, p)
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
                "schema.drop_database" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        db: String,
                        #[serde(default)]
                        if_exists: bool,
                    }
                    let p: P = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    eng.drop_database(&p.db, p.if_exists)
                        .map_err(to_rpc_error)?;
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
                "schema.drop_table" => {
                    #[derive(serde::Deserialize)]
                    struct P {
                        db: String,
                        table: String,
                        #[serde(default)]
                        if_exists: bool,
                    }
                    let p: P = parse_params(params.clone())?;
                    let mut eng = state.engine.write().await;
                    eng.drop_table(&p.db, &p.table, p.if_exists)
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

    let status = if req.id.is_none() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::OK
    };
    observe_rpc_call(
        state,
        &method,
        params.as_ref(),
        status,
        result.is_ok(),
        result.as_ref().ok(),
        started_at.elapsed(),
    );

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

async fn sql_exec_http_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(params): Json<SqlExecParams>,
) -> axum::response::Response {
    let params = match serde_json::to_value(params) {
        Ok(v) => v,
        Err(err) => {
            let resp: RpcResponse = RpcResponse::err(
                None,
                RpcError::new("invalid_request", format!("invalid sql payload: {err}")),
            );
            return (StatusCode::BAD_REQUEST, Json(resp)).into_response();
        }
    };
    let req = RpcRequest {
        skeinql: SKEINQL_VERSION.to_string(),
        // HTTP SQL endpoint expects a response body, so use a synthetic id
        // instead of notification semantics (id = None).
        id: Some(RpcId::Str("sql.exec.http".to_string())),
        method: "sql.exec".to_string(),
        params: Some(params),
    };
    handle_rpc(&state, Some(&headers), req, RpcPolicy::default())
        .await
        .into_response()
}

fn should_guard_cluster_write(method: &str) -> bool {
    if matches!(
        method,
        "cluster.status" | "cluster.nodes" | "system.shutdown"
    ) {
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
            | "schema.drop_database"
            | "schema.create_table"
            | "schema.drop_table"
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
        "schema.drop_database" => (s(&["db"]), None),
        "schema.create_table" => (s(&["db"]), s(&["table"])),
        "schema.drop_table" => (s(&["db"]), s(&["table"])),
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
        "cluster.node.leave",
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

fn cluster_node_leave(state: &AppState, params: ClusterNodeLeaveParams) -> Result<Value, RpcError> {
    let now = now_unix_ms_u64();
    let mut new_primary = None;
    {
        let mut cluster = state.cluster.lock().unwrap();
        let mut found = false;
        for node in cluster.nodes.iter_mut() {
            if node.node_id == params.node_id {
                node.status = "offline".to_string();
                node.last_seen_ms = now;
                found = true;
                break;
            }
        }
        if !found {
            return Err(RpcError::new("not_found", "node not found"));
        }

        if cluster.primary_node_id == params.node_id {
            if let Some(next_id) = cluster
                .nodes
                .iter()
                .find(|n| n.node_id != params.node_id && n.status == "online")
                .map(|n| n.node_id.clone())
            {
                cluster.primary_node_id = next_id.clone();
                new_primary = Some(next_id.clone());
                for node in cluster.nodes.iter_mut() {
                    if node.node_id == next_id {
                        node.role = "primary".to_string();
                    } else if node.role == "primary" {
                        node.role = "replica".to_string();
                    }
                }
            }
        }

        let online_nodes: HashSet<String> = cluster
            .nodes
            .iter()
            .filter(|n| n.status == "online")
            .map(|n| n.node_id.clone())
            .collect();
        for shard in cluster.shards.iter_mut() {
            shard.replicas.retain(|id| id != &params.node_id);
            if shard.primary_node_id == params.node_id {
                if let Some(next) = shard
                    .replicas
                    .iter()
                    .find(|id| online_nodes.contains(*id))
                    .cloned()
                    .or_else(|| new_primary.clone())
                {
                    shard.primary_node_id = next.clone();
                    shard.replicas.retain(|id| id != &next);
                }
                shard.updated_at_ms = now;
            }
        }
    }
    persist_cluster_state(state).map_err(|e| RpcError::new("internal", e.to_string()))?;
    Ok(serde_json::json!({
        "ok": true,
        "node_id": params.node_id,
        "status": "offline",
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

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct SqlExecParams {
    sql: String,
    #[serde(default)]
    explain: bool,
    #[serde(default)]
    default_db: Option<String>,
    #[serde(default)]
    result_format: Option<ResultFormat>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TxBeginParams {
    #[serde(default)]
    read_only: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TxFinishParams {
    tx_id: String,
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
    AlterTable,
    DropTable,
    Insert,
    InsertIgnore,
    Replace,
    Update,
    Delete,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InsertMode {
    Insert,
    Ignore,
    Replace,
}

#[derive(Debug, Clone)]
enum InsertDupValue {
    Literal(Lit),
    ValuesRef(String),
}

#[derive(Debug, Clone)]
struct InsertDupAssign {
    target_col: String,
    value: InsertDupValue,
}

#[derive(Debug, Clone)]
enum SqlPlan {
    Select {
        from: Option<TableRef>,
        distinct: bool,
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
        compat_mysql: Option<Value>,
    },
    AlterTableAddColumn {
        table: BaseTableRef,
        column: SchemaColumnInfo,
        default: Option<Lit>,
    },
    DropTable {
        table: BaseTableRef,
        if_exists: bool,
    },
    Insert {
        mode: InsertMode,
        table: BaseTableRef,
        columns: Vec<String>,
        rows: Vec<BTreeMap<String, Lit>>,
        on_duplicate: Option<Vec<InsertDupAssign>>,
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

fn mysql_error_from_rpc(err: &RpcError) -> (u16, &'static str, String) {
    match err.code.as_str() {
        "invalid_request" => (1064, "42000", err.message.clone()),
        "not_supported" => (1235, "42000", err.message.clone()),
        "not_found" => (1146, "42S02", err.message.clone()),
        "conflict" => (1213, "40001", err.message.clone()),
        "forbidden" => (1044, "42000", err.message.clone()),
        "unauthorized" => (1045, "28000", err.message.clone()),
        _ => (1105, "HY000", err.message.clone()),
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
    } else if lower.starts_with("alter table ") {
        SqlVerb::AlterTable
    } else if lower.starts_with("drop table ") {
        SqlVerb::DropTable
    } else if lower.starts_with("insert ignore into ") {
        SqlVerb::InsertIgnore
    } else if lower.starts_with("insert into ") {
        SqlVerb::Insert
    } else if lower.starts_with("replace into ") {
        SqlVerb::Replace
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

fn parse_base_table_ref_with_alias(
    input: &str,
    default_db: Option<&str>,
    allow_alias: bool,
) -> Result<BaseTableRef, RpcError> {
    let tokens: Vec<&str> = input.split_whitespace().collect();
    let (name_raw, alias) = match tokens.as_slice() {
        [] => ("", None),
        [name] => (*name, None),
        [name, alias] if allow_alias => (*name, Some(clean_sql_ident(alias))),
        [name, as_kw, alias] if allow_alias && as_kw.eq_ignore_ascii_case("as") => {
            (*name, Some(clean_sql_ident(alias)))
        }
        _ => {
            return Err(RpcError::new(
                "not_supported",
                format!("unsupported table reference '{}'", input.trim()),
            ))
        }
    };
    let cleaned = clean_sql_ident(name_raw);
    if cleaned.is_empty() {
        return Err(RpcError::new("invalid_request", "missing table name"));
    }
    if let Some((db, table)) = cleaned.split_once('.') {
        return Ok(BaseTableRef {
            db: clean_sql_ident(db),
            table: clean_sql_ident(table),
            r#as: alias.filter(|s| !s.is_empty()),
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
        r#as: alias.filter(|s| !s.is_empty()),
    })
}

fn parse_table_ref(name: &str, default_db: Option<&str>) -> Result<BaseTableRef, RpcError> {
    let mut table = parse_base_table_ref_with_alias(name, default_db, false)?;
    table.r#as = None;
    Ok(table)
}

fn parse_from_table_ref(input: &str, default_db: Option<&str>) -> Result<TableRef, RpcError> {
    let input = input.trim();
    for (token, join_type) in [
        (" left outer join ", JoinType::Left),
        (" right outer join ", JoinType::Right),
        (" left join ", JoinType::Left),
        (" right join ", JoinType::Right),
        (" inner join ", JoinType::Inner),
        (" join ", JoinType::Inner),
    ] {
        if let Some(idx) = find_ascii_ci_outside_quotes(input.as_bytes(), token.as_bytes()) {
            let left_sql = input[..idx].trim();
            let right_tail = input[idx + token.len()..].trim();
            let on_idx = find_keyword_top_level(right_tail, "on").ok_or_else(|| {
                RpcError::new("not_supported", "JOIN currently requires an ON predicate")
            })?;
            let right_sql = right_tail[..on_idx].trim();
            let on_sql = right_tail[on_idx + 2..].trim();
            let on = parse_where_expr(on_sql)?;
            return Ok(TableRef::Join(JoinTableRef {
                join: JoinRef {
                    join_type,
                    left: Box::new(TableRef::Base(parse_base_table_ref_with_alias(
                        left_sql, default_db, true,
                    )?)),
                    right: Box::new(TableRef::Base(parse_base_table_ref_with_alias(
                        right_sql, default_db, true,
                    )?)),
                    on,
                },
            }));
        }
    }
    Ok(TableRef::Base(parse_base_table_ref_with_alias(
        input, default_db, true,
    )?))
}

fn parse_sql_column_ref(raw: &str) -> Option<(String, Option<String>)> {
    let cleaned = clean_sql_ident(raw);
    if cleaned.is_empty() || cleaned == "*" {
        return None;
    }
    if let Some((table, col)) = cleaned.rsplit_once('.') {
        let col = clean_sql_ident(col);
        if col.is_empty() {
            return None;
        }
        let table = clean_sql_ident(table);
        return Some((col, (!table.is_empty()).then_some(table)));
    }
    Some((cleaned, None))
}

fn parse_sql_scalar_expr(raw: &str) -> Result<Expr, RpcError> {
    let s = raw.trim();
    let is_lit = s.starts_with('\'')
        || s.starts_with('"')
        || s.eq_ignore_ascii_case("null")
        || s.eq_ignore_ascii_case("true")
        || s.eq_ignore_ascii_case("false")
        || s.parse::<i64>().is_ok()
        || s.parse::<f64>().is_ok();
    if is_lit {
        return Ok(Expr::Lit {
            lit: parse_sql_lit(s)?,
        });
    }
    if let Some((col, table)) = parse_sql_column_ref(s) {
        return Ok(Expr::Col { col, table });
    }
    Err(RpcError::new(
        "not_supported",
        format!("unsupported SQL expression '{}'", raw.trim()),
    ))
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

fn parse_sql_leading_lit(raw: &str) -> Result<Lit, RpcError> {
    let s = raw.trim_start();
    if s.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "DEFAULT requires a literal value",
        ));
    }
    let bytes = s.as_bytes();
    if matches!(bytes[0], b'\'' | b'"') {
        let quote = bytes[0];
        let mut idx = 1usize;
        while idx < bytes.len() {
            if bytes[idx] == quote {
                if quote == b'\'' && idx + 1 < bytes.len() && bytes[idx + 1] == quote {
                    idx += 2;
                    continue;
                }
                return parse_sql_lit(&s[..=idx]);
            }
            idx += 1;
        }
        return Err(RpcError::new(
            "invalid_request",
            "unterminated quoted DEFAULT literal",
        ));
    }
    let token_end = s.find(char::is_whitespace).unwrap_or(s.len());
    parse_sql_lit(&s[..token_end])
}

fn parse_column_default_clause(definition: &str) -> Result<Option<Lit>, RpcError> {
    let Some(idx) = find_keyword_top_level(definition, "default") else {
        return Ok(None);
    };
    let tail = definition[idx + 7..].trim_start();
    if tail.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "DEFAULT requires a literal value",
        ));
    }
    Ok(Some(parse_sql_leading_lit(tail)?))
}

fn parse_condition_expr(clause: &str) -> Result<Expr, RpcError> {
    let clause = clause.trim();
    if let Some(idx) = find_keyword_top_level(clause, "is") {
        let left = clause[..idx].trim();
        let right = clause[idx + 2..].trim();
        let left_expr = parse_sql_scalar_expr(left)?;
        if matches!(left_expr, Expr::Col { .. }) {
            if right.eq_ignore_ascii_case("null") {
                return Ok(Expr::Op {
                    op: "is_null".to_string(),
                    a: Some(Box::new(left_expr)),
                    b: None,
                    args: None,
                    list: None,
                    lo: None,
                    hi: None,
                });
            }
            if right.eq_ignore_ascii_case("not null") {
                let is_null = Expr::Op {
                    op: "is_null".to_string(),
                    a: Some(Box::new(left_expr)),
                    b: None,
                    args: None,
                    list: None,
                    lo: None,
                    hi: None,
                };
                return Ok(Expr::Op {
                    op: "not".to_string(),
                    a: Some(Box::new(is_null)),
                    b: None,
                    args: None,
                    list: None,
                    lo: None,
                    hi: None,
                });
            }
        }
    }
    if let Some(idx) = find_keyword_top_level(clause, "in") {
        let left = clause[..idx].trim();
        let right = clause[idx + 2..].trim();
        let left_expr = parse_sql_scalar_expr(left)?;
        if matches!(left_expr, Expr::Col { .. }) && right.starts_with('(') && right.ends_with(')') {
            let values = split_csv_top_level(&right[1..right.len() - 1])
                .into_iter()
                .map(|part| parse_sql_scalar_expr(&part))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Expr::Op {
                op: "in".to_string(),
                a: Some(Box::new(left_expr)),
                b: None,
                args: None,
                list: Some(values),
                lo: None,
                hi: None,
            });
        }
    }
    if let Some((idx, op)) = [("like", "like"), ("ilike", "ilike")]
        .into_iter()
        .find_map(|(token, op)| find_keyword_top_level(clause, token).map(|idx| (idx, op)))
    {
        let left = clause[..idx].trim();
        let right = clause[idx + op.len()..].trim();
        if !left.is_empty() && !right.is_empty() {
            return Ok(Expr::Op {
                op: op.to_string(),
                a: Some(Box::new(parse_sql_scalar_expr(left)?)),
                b: Some(Box::new(parse_sql_scalar_expr(right)?)),
                args: None,
                list: None,
                lo: None,
                hi: None,
            });
        }
    }

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
                    return Ok(Expr::Op {
                        op: op.to_string(),
                        a: Some(Box::new(parse_sql_scalar_expr(left)?)),
                        b: Some(Box::new(parse_sql_scalar_expr(right)?)),
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
        let Some((col, table)) = parse_sql_column_ref(col_tok) else {
            continue;
        };
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
            expr: Expr::Col { col, table },
            dir,
        });
    }
    Ok(out)
}

fn parse_limit_clause(
    limit_sql: Option<&str>,
    offset_sql: Option<&str>,
) -> Result<Option<LimitClause>, RpcError> {
    let mut limit = None::<u64>;
    let mut offset =
        match offset_sql {
            Some(raw) => Some(raw.trim().parse::<u64>().map_err(|_| {
                RpcError::new("invalid_request", "OFFSET must be an unsigned integer")
            })?),
            None => None,
        };
    if let Some(raw) = limit_sql {
        let raw = raw.trim();
        if let Some((off_raw, lim_raw)) = raw.split_once(',') {
            if offset.is_some() {
                return Err(RpcError::new(
                    "invalid_request",
                    "use either LIMIT offset,count or LIMIT ... OFFSET ..., not both",
                ));
            }
            offset = Some(off_raw.trim().parse::<u64>().map_err(|_| {
                RpcError::new(
                    "invalid_request",
                    "LIMIT offset must be an unsigned integer",
                )
            })?);
            limit = Some(lim_raw.trim().parse::<u64>().map_err(|_| {
                RpcError::new("invalid_request", "LIMIT count must be an unsigned integer")
            })?);
        } else {
            limit = Some(raw.parse::<u64>().map_err(|_| {
                RpcError::new("invalid_request", "LIMIT must be an unsigned integer")
            })?);
        }
    }
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
    } else if expr_raw.ends_with(".*") {
        return Err(RpcError::new(
            "not_supported",
            "qualified wildcard projection is not supported yet",
        ));
    } else if let Ok(expr) = parse_sql_scalar_expr(expr_raw) {
        expr
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
    let mut distinct = false;
    if rest.len() >= 8 && rest[..8].eq_ignore_ascii_case("distinct") {
        let tail = rest[8..].trim_start();
        if tail.len() != rest.len() {
            distinct = true;
            rest = tail;
        }
    }
    let from_idx = find_keyword_top_level(rest, "from");
    if from_idx.is_none() {
        let mut projection = Vec::new();
        for part in split_csv_top_level(rest) {
            projection.push(parse_select_projection_item(&part)?);
        }
        return Ok(SqlPlan::Select {
            from: None,
            distinct,
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
    let from = parse_from_table_ref(table_sql, default_db)?;

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
        from: Some(from),
        distinct,
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

fn parse_create_table_index_def(definition: &str) -> Result<Option<Value>, RpcError> {
    let mut rest = definition.trim();
    let mut lower = rest.to_ascii_lowercase();
    if lower.starts_with("constraint ") {
        let Some(first_ws) = rest.find(char::is_whitespace) else {
            return Err(RpcError::new(
                "invalid_request",
                format!("invalid index definition '{}'", definition),
            ));
        };
        let tail = rest[first_ws..].trim_start();
        let second_ws = tail.find(char::is_whitespace).ok_or_else(|| {
            RpcError::new(
                "invalid_request",
                format!("invalid index definition '{}'", definition),
            )
        })?;
        rest = tail[second_ws..].trim_start();
        lower = rest.to_ascii_lowercase();
    }

    let unique = if lower.starts_with("unique key ") {
        rest = rest[10..].trim_start();
        true
    } else if lower.starts_with("unique index ") {
        rest = rest[12..].trim_start();
        true
    } else if lower.starts_with("unique ") {
        rest = rest[6..].trim_start();
        true
    } else if lower.starts_with("key ") {
        rest = rest[3..].trim_start();
        false
    } else if lower.starts_with("index ") {
        rest = rest[5..].trim_start();
        false
    } else {
        return Ok(None);
    };

    let Some(open_idx) = rest.find('(') else {
        return Err(RpcError::new(
            "invalid_request",
            format!("index definition missing columns '{}'", definition),
        ));
    };
    let Some(close_idx) = rest.rfind(')') else {
        return Err(RpcError::new(
            "invalid_request",
            format!("index definition missing closing ')' '{}'", definition),
        ));
    };
    let name_raw = rest[..open_idx].trim();
    let columns = split_csv_top_level(&rest[open_idx + 1..close_idx])
        .into_iter()
        .map(|col| clean_sql_ident(&col))
        .filter(|col| !col.is_empty())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            format!(
                "index definition requires at least one column '{}'",
                definition
            ),
        ));
    }
    let name = if name_raw.is_empty() {
        columns.join("_")
    } else {
        clean_sql_ident(name_raw)
    };
    if name.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            format!("index definition requires a name '{}'", definition),
        ));
    }
    Ok(Some(serde_json::json!({
        "name": name,
        "columns": columns,
        "unique": unique,
    })))
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
    let mut mysql_defaults = serde_json::Map::new();
    let mut mysql_indexes = Vec::new();
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
        if let Some(index) = parse_create_table_index_def(p)? {
            mysql_indexes.push(index);
            continue;
        }
        if p_lower.starts_with("constraint ") {
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
        let default = parse_column_default_clause(p)?;
        columns.push(SchemaColumnInfo {
            name: name.clone(),
            r#type: sql_type_to_desc(type_tok, unsigned),
            nullable,
            auto_increment,
        });
        if let Some(default) = default {
            mysql_defaults.insert(name, serde_json::to_value(default).unwrap_or(Value::Null));
        }
    }
    if columns.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "CREATE TABLE must define at least one column",
        ));
    }
    let mut compat_mysql = serde_json::Map::new();
    if !mysql_defaults.is_empty() {
        compat_mysql.insert("column_defaults".to_string(), Value::Object(mysql_defaults));
    }
    if !mysql_indexes.is_empty() {
        compat_mysql.insert("indexes".to_string(), Value::Array(mysql_indexes));
    }
    let compat_mysql = if compat_mysql.is_empty() {
        None
    } else {
        Some(Value::Object(compat_mysql))
    };
    Ok(SqlPlan::CreateTable {
        table,
        columns,
        primary_key,
        if_not_exists,
        compat_mysql,
    })
}

fn parse_alter_table_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let tail = sql[11..].trim();
    let add_idx = find_keyword_top_level(tail, "add").ok_or_else(|| {
        RpcError::new(
            "not_supported",
            "ALTER TABLE currently supports only ADD COLUMN",
        )
    })?;
    let table = parse_table_ref(tail[..add_idx].trim(), default_db)?;
    let mut clause = tail[add_idx + 3..].trim();
    if clause.len() >= 6 && clause[..6].eq_ignore_ascii_case("column") {
        clause = clause[6..].trim_start();
    }
    let parts: Vec<&str> = clause.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(RpcError::new(
            "invalid_request",
            "ALTER TABLE ADD COLUMN requires a name and type",
        ));
    }
    let name = clean_sql_ident(parts[0]);
    let type_tok = parts[1];
    let clause_lower = clause.to_ascii_lowercase();
    if clause_lower.contains(" after ") || clause_lower.ends_with(" first") {
        return Err(RpcError::new(
            "not_supported",
            "ALTER TABLE ADD COLUMN position clauses are not supported yet",
        ));
    }
    let unsigned = parts.iter().any(|t| t.eq_ignore_ascii_case("unsigned"));
    let nullable = !clause_lower.contains("not null");
    let auto_increment = clause_lower.contains("auto_increment");
    let default = find_keyword_top_level(clause, "default")
        .map(|idx| clause[idx + 7..].trim())
        .filter(|raw| !raw.is_empty())
        .map(parse_sql_lit)
        .transpose()?;
    Ok(SqlPlan::AlterTableAddColumn {
        table,
        column: SchemaColumnInfo {
            name,
            r#type: sql_type_to_desc(type_tok, unsigned),
            nullable,
            auto_increment,
        },
        default,
    })
}

fn parse_drop_table_plan(sql: &str, default_db: Option<&str>) -> Result<SqlPlan, RpcError> {
    let mut tail = sql[10..].trim();
    let mut if_exists = false;
    if tail.to_ascii_lowercase().starts_with("if exists ") {
        if_exists = true;
        tail = tail[10..].trim();
    }
    let table = parse_table_ref(tail, default_db)?;
    Ok(SqlPlan::DropTable { table, if_exists })
}

fn parse_insert_plan(
    sql: &str,
    default_db: Option<&str>,
    mode: InsertMode,
) -> Result<SqlPlan, RpcError> {
    let prefix_len = match mode {
        InsertMode::Insert => 11,
        InsertMode::Ignore => 18,
        InsertMode::Replace => 12,
    };
    let tail = sql[prefix_len..].trim();
    let values_idx = find_keyword_top_level(tail, "values").ok_or_else(|| {
        RpcError::new("invalid_request", "INSERT currently requires VALUES syntax")
    })?;
    let head = tail[..values_idx].trim();
    let mut values_sql = tail[values_idx + 6..].trim();
    let mut on_duplicate = None::<Vec<InsertDupAssign>>;
    if let Some(idx) = find_keyword_top_level(values_sql, "on duplicate key update") {
        let update_clause = values_sql[idx + "on duplicate key update".len()..].trim();
        values_sql = values_sql[..idx].trim();
        let mut assigns = Vec::new();
        for assign in split_csv_top_level(update_clause) {
            let Some(eq_idx) = assign.find('=') else {
                return Err(RpcError::new(
                    "invalid_request",
                    format!("invalid ON DUPLICATE assignment '{}'", assign),
                ));
            };
            let target_col = clean_sql_ident(assign[..eq_idx].trim());
            let rhs = assign[eq_idx + 1..].trim();
            let value = if rhs.len() > 8
                && rhs[..7].eq_ignore_ascii_case("values(")
                && rhs.ends_with(')')
            {
                let src = clean_sql_ident(&rhs[7..rhs.len() - 1]);
                InsertDupValue::ValuesRef(src)
            } else {
                InsertDupValue::Literal(parse_sql_lit(rhs)?)
            };
            assigns.push(InsertDupAssign { target_col, value });
        }
        on_duplicate = Some(assigns);
    }

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
    Ok(SqlPlan::Insert {
        mode,
        table,
        columns: cols,
        rows,
        on_duplicate,
    })
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
        SqlVerb::AlterTable => parse_alter_table_plan(normalized, default_db),
        SqlVerb::DropTable => parse_drop_table_plan(normalized, default_db),
        SqlVerb::Insert => parse_insert_plan(normalized, default_db, InsertMode::Insert),
        SqlVerb::InsertIgnore => parse_insert_plan(normalized, default_db, InsertMode::Ignore),
        SqlVerb::Replace => parse_insert_plan(normalized, default_db, InsertMode::Replace),
        SqlVerb::Update => parse_update_plan(normalized, default_db),
        SqlVerb::Delete => parse_delete_plan(normalized, default_db),
        SqlVerb::Unsupported => Err(RpcError::new(
            "not_supported",
            "sql.exec supports SELECT/SHOW/USE/CREATE DATABASE/CREATE TABLE/DROP TABLE/INSERT/UPDATE/DELETE",
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
        SqlPlan::AlterTableAddColumn { .. } => "alter_table",
        SqlPlan::DropTable { .. } => "drop_table",
        SqlPlan::Insert { mode, .. } => match mode {
            InsertMode::Replace => "replace",
            _ => "insert",
        },
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
        | Ok(SqlPlan::AlterTableAddColumn { table, .. })
        | Ok(SqlPlan::DropTable { table, .. })
        | Ok(SqlPlan::Insert { table, .. })
        | Ok(SqlPlan::Update { table, .. })
        | Ok(SqlPlan::Delete { table, .. }) => (Some(table.db), Some(table.table)),
        _ => (None, None),
    }
}

fn lit_to_f64(lit: &Lit) -> Option<f64> {
    match lit {
        Lit::I64 { v } => Some(*v as f64),
        Lit::U64 { v } => Some(*v as f64),
        Lit::F64 { v } => Some(*v),
        _ => None,
    }
}

fn lit_cmp(a: &Lit, b: &Lit) -> Option<std::cmp::Ordering> {
    if let (Some(af), Some(bf)) = (lit_to_f64(a), lit_to_f64(b)) {
        return af.partial_cmp(&bf);
    }
    match (a, b) {
        (Lit::Str { v: av }, Lit::Str { v: bv }) => Some(av.cmp(bv)),
        (Lit::Bool { v: av }, Lit::Bool { v: bv }) => Some(av.cmp(bv)),
        (Lit::Null, Lit::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

fn lit_eq(a: &Lit, b: &Lit) -> bool {
    lit_cmp(a, b)
        .map(|ord| ord == std::cmp::Ordering::Equal)
        .unwrap_or(false)
}

fn row_get_lit(row: &BTreeMap<String, Lit>, col: &str) -> Option<Lit> {
    if let Some(v) = row.get(col) {
        return Some(v.clone());
    }
    row.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(col))
        .map(|(_, v)| v.clone())
}

fn eval_info_schema_expr(expr: &Expr, row: &BTreeMap<String, Lit>) -> Result<bool, RpcError> {
    match expr {
        Expr::Op {
            op,
            a,
            b,
            args: _,
            list: _,
            lo: _,
            hi: _,
        } => {
            if op == "and" {
                let left = a.as_ref().ok_or_else(|| {
                    RpcError::new("invalid_request", "malformed WHERE expression")
                })?;
                let right = b.as_ref().ok_or_else(|| {
                    RpcError::new("invalid_request", "malformed WHERE expression")
                })?;
                return Ok(eval_info_schema_expr(left, row)? && eval_info_schema_expr(right, row)?);
            }

            let left = match a.as_deref() {
                Some(Expr::Col { col, .. }) => row_get_lit(row, col).unwrap_or(Lit::Null),
                Some(Expr::Lit { lit }) => lit.clone(),
                _ => {
                    return Err(RpcError::new(
                        "not_supported",
                        "information_schema WHERE supports only column/literal comparisons",
                    ))
                }
            };
            let right = match b.as_deref() {
                Some(Expr::Col { col, .. }) => row_get_lit(row, col).unwrap_or(Lit::Null),
                Some(Expr::Lit { lit }) => lit.clone(),
                _ => {
                    return Err(RpcError::new(
                        "not_supported",
                        "information_schema WHERE supports only column/literal comparisons",
                    ))
                }
            };

            let ord = lit_cmp(&left, &right);
            let out = match op.as_str() {
                "eq" => lit_eq(&left, &right),
                "ne" => !lit_eq(&left, &right),
                "gt" => ord
                    .map(|o| o == std::cmp::Ordering::Greater)
                    .unwrap_or(false),
                "ge" => ord
                    .map(|o| o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal)
                    .unwrap_or(false),
                "lt" => ord.map(|o| o == std::cmp::Ordering::Less).unwrap_or(false),
                "le" => ord
                    .map(|o| o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal)
                    .unwrap_or(false),
                _ => {
                    return Err(RpcError::new(
                        "not_supported",
                        format!("unsupported WHERE operator '{}'", op),
                    ))
                }
            };
            Ok(out)
        }
        Expr::Lit {
            lit: Lit::Bool { v },
        } => Ok(*v),
        _ => Err(RpcError::new(
            "not_supported",
            "information_schema WHERE supports only simple predicates",
        )),
    }
}

fn projection_label(item: &SelectItem, idx: usize) -> String {
    if let Some(alias) = item.r#as.as_ref() {
        return alias.clone();
    }
    match &item.expr {
        Expr::Col { col, .. } => col.clone(),
        _ => format!("expr{}", idx + 1),
    }
}

fn project_virtual_row(
    row: &BTreeMap<String, Lit>,
    projection: &[SelectItem],
    fallback_cols: &[&str],
) -> Result<(Vec<String>, Vec<Value>), RpcError> {
    if projection.is_empty() {
        let names = fallback_cols
            .iter()
            .map(|c| (*c).to_string())
            .collect::<Vec<_>>();
        let values = fallback_cols
            .iter()
            .map(|col| {
                row_get_lit(row, col)
                    .map(|lit| serde_json::to_value(lit).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null)
            })
            .collect::<Vec<_>>();
        return Ok((names, values));
    }

    let mut names = Vec::with_capacity(projection.len());
    let mut values = Vec::with_capacity(projection.len());
    for (idx, item) in projection.iter().enumerate() {
        names.push(projection_label(item, idx));
        let value = match &item.expr {
            Expr::Col { col, .. } => row_get_lit(row, col)
                .map(|lit| serde_json::to_value(lit).unwrap_or(Value::Null))
                .unwrap_or(Value::Null),
            Expr::Lit { lit } => serde_json::to_value(lit).unwrap_or(Value::Null),
            _ => {
                return Err(RpcError::new(
                    "not_supported",
                    "information_schema projection supports only columns and literals",
                ))
            }
        };
        values.push(value);
    }
    Ok((names, values))
}

fn sort_virtual_rows(
    rows: &mut [BTreeMap<String, Lit>],
    order_by: &[OrderBy],
) -> Result<(), RpcError> {
    if order_by.is_empty() {
        return Ok(());
    }
    rows.sort_by(|a, b| {
        for rule in order_by {
            let Expr::Col { col, .. } = &rule.expr else {
                continue;
            };
            let av = row_get_lit(a, col).unwrap_or(Lit::Null);
            let bv = row_get_lit(b, col).unwrap_or(Lit::Null);
            let ord = lit_cmp(&av, &bv).unwrap_or(std::cmp::Ordering::Equal);
            let ord = match rule.dir.clone().unwrap_or(OrderDir::Asc) {
                OrderDir::Asc => ord,
                OrderDir::Desc => ord.reverse(),
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });

    for rule in order_by {
        if !matches!(rule.expr, Expr::Col { .. }) {
            return Err(RpcError::new(
                "not_supported",
                "information_schema ORDER BY supports only column references",
            ));
        }
    }
    Ok(())
}

fn information_schema_select_result(
    eng: &Engine,
    table: &BaseTableRef,
    projection: &[SelectItem],
    where_expr: &Option<Expr>,
    order_by: &[OrderBy],
    limit: &Option<LimitClause>,
) -> Result<Option<Value>, RpcError> {
    if !table.db.eq_ignore_ascii_case("information_schema") {
        return Ok(None);
    }

    let mut rows: Vec<BTreeMap<String, Lit>> = Vec::new();
    let all_cols: Vec<&str> = if table.table.eq_ignore_ascii_case("tables") {
        for db in eng.list_databases() {
            let tables = eng.list_tables(&db).map_err(to_rpc_error)?;
            for t in tables {
                let mut row = BTreeMap::new();
                row.insert(
                    "TABLE_CATALOG".to_string(),
                    Lit::Str {
                        v: "def".to_string(),
                    },
                );
                row.insert("TABLE_SCHEMA".to_string(), Lit::Str { v: db.clone() });
                row.insert("TABLE_NAME".to_string(), Lit::Str { v: t });
                row.insert(
                    "TABLE_TYPE".to_string(),
                    Lit::Str {
                        v: "BASE TABLE".to_string(),
                    },
                );
                row.insert(
                    "ENGINE".to_string(),
                    Lit::Str {
                        v: "SkeinDB".to_string(),
                    },
                );
                row.insert("TABLE_ROWS".to_string(), Lit::U64 { v: 0 });
                rows.push(row);
            }
        }
        vec![
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "TABLE_TYPE",
            "ENGINE",
            "TABLE_ROWS",
        ]
    } else if table.table.eq_ignore_ascii_case("columns") {
        for db in eng.list_databases() {
            let tables = eng.list_tables(&db).map_err(to_rpc_error)?;
            for t in tables {
                let desc = eng.describe_table(&db, &t).map_err(to_rpc_error)?;
                let pk: HashSet<String> = desc
                    .get("primary_key")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect();
                let cols = desc
                    .get("columns")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                for (idx, col) in cols.iter().enumerate() {
                    let name = col
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let nullable = col
                        .get("nullable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    let data_type = col
                        .get("type")
                        .and_then(|v| v.get("kind"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("string")
                        .to_string();
                    let mut row = BTreeMap::new();
                    row.insert(
                        "TABLE_CATALOG".to_string(),
                        Lit::Str {
                            v: "def".to_string(),
                        },
                    );
                    row.insert("TABLE_SCHEMA".to_string(), Lit::Str { v: db.clone() });
                    row.insert("TABLE_NAME".to_string(), Lit::Str { v: t.clone() });
                    row.insert("COLUMN_NAME".to_string(), Lit::Str { v: name.clone() });
                    row.insert(
                        "ORDINAL_POSITION".to_string(),
                        Lit::U64 {
                            v: (idx + 1) as u64,
                        },
                    );
                    row.insert(
                        "IS_NULLABLE".to_string(),
                        Lit::Str {
                            v: if nullable { "YES" } else { "NO" }.to_string(),
                        },
                    );
                    row.insert("DATA_TYPE".to_string(), Lit::Str { v: data_type });
                    row.insert(
                        "COLUMN_KEY".to_string(),
                        Lit::Str {
                            v: if pk.contains(&name) { "PRI" } else { "" }.to_string(),
                        },
                    );
                    rows.push(row);
                }
            }
        }
        vec![
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "IS_NULLABLE",
            "DATA_TYPE",
            "COLUMN_KEY",
        ]
    } else {
        return Err(RpcError::new(
            "not_supported",
            format!(
                "information_schema table '{}' is not supported yet",
                table.table
            ),
        ));
    };

    if let Some(expr) = where_expr.as_ref() {
        let mut filtered = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            if eval_info_schema_expr(expr, &row)? {
                filtered.push(row);
            }
        }
        rows = filtered;
    }

    sort_virtual_rows(&mut rows, order_by)?;

    if let Some(lim) = limit.as_ref() {
        let offset = lim.offset.unwrap_or(0) as usize;
        let take = lim.limit.unwrap_or(u64::MAX) as usize;
        rows = rows.into_iter().skip(offset).take(take).collect();
    }

    let mut out_rows = Vec::new();
    let mut out_cols: Option<Vec<String>> = None;
    for row in rows.iter() {
        let (cols, vals) = project_virtual_row(row, projection, &all_cols)?;
        if out_cols.is_none() {
            out_cols = Some(cols);
        }
        out_rows.push(vals);
    }
    if out_cols.is_none() {
        let (cols, _) = project_virtual_row(&BTreeMap::new(), projection, &all_cols)?;
        out_cols = Some(cols);
    }
    let columns = out_cols
        .unwrap_or_default()
        .into_iter()
        .map(|name| serde_json::json!({ "name": name }))
        .collect::<Vec<_>>();

    Ok(Some(serde_json::json!({
        "data": {
            "columns": columns,
            "rows": out_rows,
        }
    })))
}

fn insert_dup_value_from_row(row: &BTreeMap<String, Lit>, value: &InsertDupValue) -> Lit {
    match value {
        InsertDupValue::Literal(lit) => lit.clone(),
        InsertDupValue::ValuesRef(src_col) => row.get(src_col).cloned().unwrap_or(Lit::Null),
    }
}

async fn sql_exec_insert_on_duplicate(
    state: &AppState,
    table: BaseTableRef,
    columns: Vec<String>,
    rows: Vec<BTreeMap<String, Lit>>,
    assigns: Vec<InsertDupAssign>,
) -> Result<crate::engine::WriteResult, RpcError> {
    if columns.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "INSERT column list cannot be empty",
        ));
    }
    if assigns.is_empty() {
        return Err(RpcError::new(
            "invalid_request",
            "ON DUPLICATE KEY UPDATE requires at least one assignment",
        ));
    }

    let mut eng = state.engine.write().await;
    let desc = eng
        .describe_table(&table.db, &table.table)
        .map_err(to_rpc_error)?;
    let mut affected = 0u64;
    let mut last_insert_id = 0u64;

    for row in rows {
        let mut set = BTreeMap::new();
        for assign in &assigns {
            if assign.target_col.is_empty() {
                continue;
            }
            set.insert(
                assign.target_col.clone(),
                insert_dup_value_from_row(&row, &assign.value),
            );
        }

        match eng.data_insert(&table, vec![row.clone()], None) {
            Ok(inserted) => {
                affected = affected.saturating_add(inserted.affected);
                if inserted.last_insert_id != 0 {
                    last_insert_id = inserted.last_insert_id;
                }
            }
            Err(err) if err.to_string() == "conflict" => {
                let mut updated = false;
                for where_expr in mysql_conflict_predicates_for_row(&desc, &row) {
                    let result = eng
                        .data_update(&table, &where_expr, &set, Some(1), None, &[])
                        .map_err(to_rpc_error)?;
                    if result.affected > 0 {
                        affected = affected.saturating_add(result.affected);
                        updated = true;
                        break;
                    }
                }
                if !updated {
                    return Err(RpcError::new(
                        "conflict",
                        "ON DUPLICATE KEY UPDATE could not locate the conflicting row",
                    ));
                }
            }
            Err(err) => return Err(to_rpc_error(err)),
        }
    }

    Ok(crate::engine::WriteResult {
        affected,
        last_insert_id,
        returning: None,
        etag: None,
    })
}

fn and_expr(left: Expr, right: Expr) -> Expr {
    Expr::Op {
        op: "and".to_string(),
        a: Some(Box::new(left)),
        b: Some(Box::new(right)),
        args: None,
        list: None,
        lo: None,
        hi: None,
    }
}

fn eq_expr(col: String, lit: Lit) -> Expr {
    Expr::Op {
        op: "eq".to_string(),
        a: Some(Box::new(Expr::Col { col, table: None })),
        b: Some(Box::new(Expr::Lit { lit })),
        args: None,
        list: None,
        lo: None,
        hi: None,
    }
}

fn sql_exec_row_exists(
    eng: &Engine,
    table: &BaseTableRef,
    probe_col: &str,
    where_expr: &Expr,
) -> Result<bool, RpcError> {
    let query = Query {
        with: Vec::new(),
        body: Box::new(QueryBody::Select {
            select: Box::new(SelectBody {
                distinct: None,
                projection: vec![SelectItem {
                    expr: Expr::Col {
                        col: probe_col.to_string(),
                        table: None,
                    },
                    r#as: None,
                }],
                from: Some(vec![TableRef::Base(table.clone())]),
                r#where: Some(where_expr.clone()),
                group_by: None,
                having: None,
            }),
        }),
        order_by: Vec::new(),
        limit: Some(LimitClause {
            limit: Some(1),
            offset: None,
        }),
        lock: None,
    };
    let result = eng
        .query_select(
            &query,
            &[],
            ResultFormat::RowsJson,
            false,
            None,
            None,
            None,
            false,
        )
        .map_err(to_rpc_error)?;
    let data = result
        .data
        .as_ref()
        .ok_or_else(|| RpcError::new("internal", "query result missing data"))?;
    Ok(rows_json_result_len(data)? > 0)
}

async fn sql_exec_insert_ignore(
    state: &AppState,
    table: BaseTableRef,
    columns: Vec<String>,
    rows: Vec<BTreeMap<String, Lit>>,
) -> Result<crate::engine::WriteResult, RpcError> {
    let mut eng = state.engine.write().await;
    let mut affected = 0u64;
    let mut last_insert_id = 0u64;
    let key_col = columns.first().cloned();

    for row in rows {
        if let Some(key_col) = key_col.as_ref() {
            if let Some(key_lit) = row.get(key_col).cloned() {
                let where_expr = eq_expr(key_col.clone(), key_lit);
                if sql_exec_row_exists(&eng, &table, key_col, &where_expr)? {
                    continue;
                }
            }
        }
        match eng.data_insert(&table, vec![row], None) {
            Ok(inserted) => {
                affected = affected.saturating_add(inserted.affected);
                if inserted.last_insert_id != 0 {
                    last_insert_id = inserted.last_insert_id;
                }
            }
            Err(err) if err.to_string() == "conflict" => {}
            Err(err) => return Err(to_rpc_error(err)),
        }
    }

    Ok(crate::engine::WriteResult {
        affected,
        last_insert_id,
        returning: None,
        etag: None,
    })
}

async fn sql_exec_replace_into(
    state: &AppState,
    table: BaseTableRef,
    _columns: Vec<String>,
    rows: Vec<BTreeMap<String, Lit>>,
) -> Result<crate::engine::WriteResult, RpcError> {
    let mut eng = state.engine.write().await;
    let desc = eng
        .describe_table(&table.db, &table.table)
        .map_err(to_rpc_error)?;
    let mut affected = 0u64;
    let mut last_insert_id = 0u64;

    for row in rows {
        match eng.data_insert(&table, vec![row.clone()], None) {
            Ok(inserted) => {
                affected = affected.saturating_add(inserted.affected);
                if inserted.last_insert_id != 0 {
                    last_insert_id = inserted.last_insert_id;
                }
            }
            Err(err) if err.to_string() == "conflict" => {
                let conflict_predicates = mysql_conflict_predicates_for_row(&desc, &row);
                if conflict_predicates.is_empty() {
                    return Err(RpcError::new(
                        "conflict",
                        "REPLACE could not locate the conflicting row",
                    ));
                }

                let mut deleted = 0u64;
                for where_expr in conflict_predicates {
                    let removed = eng
                        .data_delete(&table, &where_expr, None, &[])
                        .map_err(to_rpc_error)?;
                    deleted = deleted.saturating_add(removed.affected);
                }

                let inserted = eng
                    .data_insert(&table, vec![row], None)
                    .map_err(to_rpc_error)?;
                affected = affected.saturating_add(deleted.saturating_add(inserted.affected));
                if inserted.last_insert_id != 0 {
                    last_insert_id = inserted.last_insert_id;
                }
            }
            Err(err) => return Err(to_rpc_error(err)),
        }
    }

    Ok(crate::engine::WriteResult {
        affected,
        last_insert_id,
        returning: None,
        etag: None,
    })
}

async fn sql_exec_alter_table_add_column(
    state: &AppState,
    table: BaseTableRef,
    column: SchemaColumnInfo,
    default: Option<Lit>,
) -> Result<(), RpcError> {
    let mut eng = state.engine.write().await;
    let status = eng
        .schema_merge_status(SchemaMergeStatusParams {
            table: table.clone(),
        })
        .map_err(to_rpc_error)?;
    let proposed = eng
        .schema_propose_change(SchemaProposeChangeParams {
            table: table.clone(),
            base_version: status.current_version,
            changes: vec![skeindb_skeinql::methods::SchemaChangeOp::AddColumn {
                name: column.name,
                r#type: column.r#type,
                nullable: column.nullable,
                auto_increment: column.auto_increment,
                default,
            }],
            message: Some("sql.exec ALTER TABLE ADD COLUMN".to_string()),
        })
        .map_err(to_rpc_error)?;
    eng.schema_apply_merge(SchemaApplyMergeParams {
        table,
        change_ids: Some(vec![proposed.change_id]),
    })
    .map_err(to_rpc_error)?;
    Ok(())
}

fn rows_json_result_len(result: &Value) -> Result<u64, RpcError> {
    let rows = result
        .get("rows")
        .or_else(|| result.get("data").and_then(|v| v.get("rows")))
        .and_then(|v| v.as_array())
        .ok_or_else(|| RpcError::new("internal", "query result missing rows"))?;
    Ok(rows.len() as u64)
}

async fn mysql_select_total_rows_without_limit(
    state: &AppState,
    sql: &str,
    default_db: Option<&str>,
) -> Result<u64, RpcError> {
    let plan = parse_sql_plan(sql, default_db)?;
    let SqlPlan::Select {
        from,
        distinct,
        mut projection,
        where_expr,
        order_by,
        ..
    } = plan
    else {
        return Err(RpcError::new(
            "invalid_request",
            "SQL_CALC_FOUND_ROWS requires a SELECT statement",
        ));
    };

    // SELECT literal expressions always produce exactly one row.
    if from.is_none() {
        return Ok(1);
    }
    let from = from.expect("checked above");

    let eng = state.engine.read().await;
    let no_limit = None;
    if let TableRef::Base(table) = &from {
        if let Some(result) = information_schema_select_result(
            &eng,
            table,
            &projection,
            &where_expr,
            &order_by,
            &no_limit,
        )? {
            return rows_json_result_len(&result);
        }
    }

    if projection.is_empty() {
        let TableRef::Base(table) = &from else {
            return Err(RpcError::new(
                "not_supported",
                "SQL_CALC_FOUND_ROWS with wildcard joins is not supported yet",
            ));
        };
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
                distinct: distinct.then_some(true),
                projection,
                from: Some(vec![from]),
                r#where: where_expr,
                group_by: None,
                having: None,
            }),
        }),
        order_by,
        limit: None,
        lock: None,
    };
    let result = eng
        .query_select(
            &query,
            &[],
            ResultFormat::RowsJson,
            false,
            None,
            None,
            None,
            false,
        )
        .map_err(to_rpc_error)?;
    let data = result
        .data
        .as_ref()
        .ok_or_else(|| RpcError::new("internal", "query result missing data"))?;
    rows_json_result_len(data)
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
            from,
            distinct,
            mut projection,
            where_expr,
            order_by,
            limit,
        } => {
            // SELECT without FROM for simple literals (e.g. SELECT 1)
            if from.is_none() {
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
            let from = from.expect("checked Some above");
            if let TableRef::Base(table) = &from {
                if let Some(result) = information_schema_select_result(
                    &eng,
                    table,
                    &projection,
                    &where_expr,
                    &order_by,
                    &limit,
                )? {
                    return Ok(serde_json::json!({
                        "statement": "select",
                        "read_only": true,
                        "result": result
                    }));
                }
            }
            if projection.is_empty() {
                if let TableRef::Base(table) = &from {
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
                } else {
                    return Err(RpcError::new(
                        "not_supported",
                        "wildcard projection over joins is not supported yet",
                    ));
                }
            }
            let query = Query {
                with: Vec::new(),
                body: Box::new(QueryBody::Select {
                    select: Box::new(SelectBody {
                        distinct: distinct.then_some(true),
                        projection,
                        from: Some(vec![from.clone()]),
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
            compat_mysql,
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
                compat_mysql,
            )
            .map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "create_table",
                "ok": true,
                "table": table,
                "if_not_exists": if_not_exists
            }))
        }
        SqlPlan::AlterTableAddColumn {
            table,
            column,
            default,
        } => {
            sql_exec_alter_table_add_column(state, table.clone(), column.clone(), default).await?;
            Ok(serde_json::json!({
                "statement": "alter_table",
                "ok": true,
                "table": table,
                "operation": "add_column",
                "column": column.name
            }))
        }
        SqlPlan::DropTable { table, if_exists } => {
            let mut eng = state.engine.write().await;
            eng.drop_table(&table.db, &table.table, if_exists)
                .map_err(to_rpc_error)?;
            Ok(serde_json::json!({
                "statement": "drop_table",
                "ok": true,
                "table": table,
                "if_exists": if_exists
            }))
        }
        SqlPlan::Insert {
            mode,
            table,
            columns,
            rows,
            on_duplicate,
        } => {
            let r = if let Some(assigns) = on_duplicate {
                sql_exec_insert_on_duplicate(state, table.clone(), columns, rows, assigns).await?
            } else {
                match mode {
                    InsertMode::Insert => {
                        let mut eng = state.engine.write().await;
                        eng.data_insert(&table, rows, None).map_err(to_rpc_error)?
                    }
                    InsertMode::Ignore => {
                        sql_exec_insert_ignore(state, table.clone(), columns.clone(), rows).await?
                    }
                    InsertMode::Replace => {
                        sql_exec_replace_into(state, table.clone(), columns.clone(), rows).await?
                    }
                }
            };
            Ok(serde_json::json!({
                "statement": match mode {
                    InsertMode::Replace => "replace",
                    _ => "insert",
                },
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
            | "tx.begin"
            | "tx.commit"
            | "tx.rollback"
            | "stats.snapshot"
            | "stats.top_queries"
            | "stats.slow_queries"
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

fn request_server_shutdown(state: &AppState) -> Result<Value, RpcError> {
    state
        .shutdown_tx
        .send(true)
        .map_err(|_| RpcError::new("internal", "shutdown channel unavailable"))?;
    Ok(serde_json::json!({
        "ok": true,
        "message": "shutdown initiated"
    }))
}

fn tx_begin(state: &AppState, params: TxBeginParams) -> Result<Value, RpcError> {
    let now = now_unix_ms_u64();
    let tx_id = format!("tx_{:016x}", TX_COUNTER.fetch_add(1, Ordering::Relaxed));
    let session = TxSession {
        id: tx_id.clone(),
        read_only: params.read_only,
        started_at_ms: now,
    };
    state
        .txns
        .lock()
        .unwrap()
        .insert(tx_id.clone(), session.clone());
    Ok(serde_json::json!({
        "tx_id": tx_id,
        "status": "open",
        "read_only": session.read_only,
        "started_at_ms": session.started_at_ms
    }))
}

fn tx_finish(state: &AppState, params: TxFinishParams, mode: &str) -> Result<Value, RpcError> {
    let tx_id = params.tx_id.trim().to_string();
    if tx_id.is_empty() {
        return Err(RpcError::new("invalid_request", "tx_id is required"));
    }
    let session = state
        .txns
        .lock()
        .unwrap()
        .remove(&tx_id)
        .ok_or_else(|| RpcError::new("not_found", "unknown tx_id"))?;
    Ok(serde_json::json!({
        "tx_id": session.id,
        "status": mode,
        "read_only": session.read_only,
        "started_at_ms": session.started_at_ms,
        "finished_at_ms": now_unix_ms_u64()
    }))
}

fn system_capabilities(state: &AppState) -> Value {
    let methods = vec![
        "system.ping",
        "system.version",
        "system.shutdown",
        "system.capabilities",
        "transport.capabilities",
        "tx.begin",
        "tx.commit",
        "tx.rollback",
        "stats.snapshot",
        "stats.top_queries",
        "stats.slow_queries",
        "settings.get",
        "settings.set",
        "cluster.status",
        "cluster.nodes",
        "cluster.join_token.create",
        "cluster.node.join",
        "cluster.node.remove",
        "cluster.node.leave",
        "cluster.replica.promote",
        "cluster.shard.create",
        "cluster.shard.move",
        "cluster.shard.rebalance",
        "schema.list_databases",
        "schema.create_database",
        "schema.drop_database",
        "schema.list_tables",
        "schema.create_table",
        "schema.drop_table",
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
    let (total_rpc, fingerprint_count, query_samples, qps) = {
        let c = state.counters.lock().unwrap();
        let total = c.total_rpc;
        let qps = if uptime_s == 0 {
            total as f64
        } else {
            total as f64 / uptime_s as f64
        };
        (
            total,
            c.query_stats.len() as u64,
            c.query_log.len() as u64,
            qps,
        )
    };

    serde_json::json!({
        "uptime_s": uptime_s,
        "sessions": {"active": 0, "total": 0},
        "qps": qps,
        "tps": 0,
        "process": {"cpu_pct": cpu_pct, "rss_bytes": rss_bytes},
        "query": {
            "tracked_calls": total_rpc,
            "fingerprints": fingerprint_count,
            "recent_samples": query_samples
        },
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
    use axum::{routing::post, Json};
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
        assert!(html.contains("data-etab=\"browse\""));
        assert!(html.contains("easyDataGrid"));
        assert!(html.contains("easyCreateTableName"));
        assert!(html.contains("btnShutdown"));
        assert!(!html.to_lowercase().contains("phpmyadmin"));
        assert!(html.contains("src/main.js"));
        let js = admin_main_js();
        assert!(js.contains("system.shutdown"));
        assert!(js.contains("system.capabilities"));
        assert!(js.contains("easyDoCreateTable"));
        assert!(js.contains("easyRenderDataGrid"));
        assert!(js.contains("easyDeleteCheckedRows"));
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
        let (shutdown_tx, _) = watch::channel(false);
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
            txns: Arc::new(Mutex::new(HashMap::new())),
            engine: Arc::new(RwLock::new(engine)),
            subs: Arc::new(Mutex::new(Subscriptions::default())),
            coalesce: Arc::new(QueryCoalescer::default()),
            transport: TransportCapabilities {
                http: true,
                quic: false,
            },
            shutdown_tx,
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

    async fn call_sql_exec_http(state: &AppState, payload: Value) -> RpcResponse {
        let params: SqlExecParams =
            serde_json::from_value(payload).expect("decode sql exec payload");
        let resp =
            sql_exec_http_handler(State(state.clone()), HeaderMap::new(), Json(params)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("parse sql response")
    }

    #[tokio::test]
    async fn sql_exec_http_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("sql_exec_http_roundtrip");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let resp = call_sql_exec_http(&state, json!({"sql":"CREATE DATABASE app"})).await;
        assert!(resp.ok);

        let resp = call_sql_exec_http(
            &state,
            json!({
                "sql":"CREATE TABLE app.users (id BIGINT UNSIGNED NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_sql_exec_http(
            &state,
            json!({
                "sql":"INSERT INTO app.users (id, name) VALUES (1, 'Nora')"
            }),
        )
        .await;
        assert!(resp.ok);

        let resp = call_sql_exec_http(
            &state,
            json!({
                "sql":"SELECT id, name FROM app.users WHERE id = 1"
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
        assert_eq!(rows[0][0]["v"].as_u64(), Some(1));
        assert_eq!(rows[0][1]["v"].as_str(), Some("Nora"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn sql_exec_information_schema_tables_and_columns_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("sql_exec_information_schema");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let resp = call_sql_exec_http(&state, json!({"sql":"CREATE DATABASE app"})).await;
        assert!(resp.ok);

        let resp = call_sql_exec_http(
            &state,
            json!({
                "sql":"CREATE TABLE app.users (id BIGINT UNSIGNED NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id))"
            }),
        )
        .await;
        assert!(resp.ok);

        let tables = call_sql_exec_http(
            &state,
            json!({
                "sql":"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'users'"
            }),
        )
        .await;
        assert!(tables.ok);
        let table_rows = tables
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(table_rows.len(), 1);
        assert_eq!(table_rows[0][0]["v"].as_str(), Some("app"));
        assert_eq!(table_rows[0][1]["v"].as_str(), Some("users"));

        let columns = call_sql_exec_http(
            &state,
            json!({
                "sql":"SELECT column_name FROM information_schema.columns WHERE table_schema = 'app' AND table_name = 'users' ORDER BY ordinal_position ASC"
            }),
        )
        .await;
        assert!(columns.ok);
        let column_rows = columns
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(column_rows.len(), 2);
        assert_eq!(column_rows[0][0]["v"].as_str(), Some("id"));
        assert_eq!(column_rows[1][0]["v"].as_str(), Some("name"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
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
    async fn schema_drop_methods_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("schema_drop_roundtrip");
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
            "schema.drop_table",
            json!({"db":"app","table":"users"}),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(&state, "schema.list_tables", json!({"db":"app"})).await;
        assert!(resp.ok);
        assert_eq!(
            resp.result
                .as_ref()
                .and_then(|v| v.get("tables"))
                .and_then(|v| v.as_array())
                .map(|tables| tables.len()),
            Some(0)
        );

        let resp = call_rpc(
            &state,
            "schema.drop_table",
            json!({"db":"app","table":"users","if_exists": true}),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(&state, "schema.drop_database", json!({"db":"app"})).await;
        assert!(resp.ok);

        let resp = call_rpc(
            &state,
            "schema.drop_database",
            json!({"db":"app","if_exists": true}),
        )
        .await;
        assert!(resp.ok);

        let resp = call_rpc(&state, "schema.list_databases", json!({})).await;
        assert!(resp.ok);
        let databases = resp.result.expect("missing result")["databases"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert!(!databases.iter().any(|db| db.as_str() == Some("app")));

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
        assert!(caps
            .result
            .as_ref()
            .and_then(|v| v.get("methods"))
            .and_then(|v| v.as_array())
            .map(|m| m.iter().any(|method| method == "system.shutdown"))
            .unwrap_or(false));
        assert!(caps
            .result
            .as_ref()
            .and_then(|v| v.get("methods"))
            .and_then(|v| v.as_array())
            .map(|m| m.iter().any(|method| method == "stats.top_queries"))
            .unwrap_or(false));
        assert!(caps
            .result
            .as_ref()
            .and_then(|v| v.get("methods"))
            .and_then(|v| v.as_array())
            .map(|m| m.iter().any(|method| method == "tx.begin"))
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
    async fn system_shutdown_sets_shutdown_channel() -> anyhow::Result<()> {
        let dir = temp_dir("system_shutdown");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);
        let mut shutdown_rx = state.shutdown_tx.subscribe();

        let resp = call_rpc(&state, "system.shutdown", json!({})).await;
        assert!(resp.ok);
        assert_eq!(
            resp.result
                .as_ref()
                .and_then(|v| v.get("ok"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), shutdown_rx.changed()).await??;
        assert!(*shutdown_rx.borrow());

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn tx_begin_commit_and_rollback_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("tx_roundtrip");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let begin = call_rpc(&state, "tx.begin", json!({"read_only": true})).await;
        assert!(begin.ok);
        let tx_id = begin
            .result
            .as_ref()
            .and_then(|v| v.get("tx_id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        assert!(!tx_id.is_empty());

        let commit = call_rpc(&state, "tx.commit", json!({"tx_id": tx_id.clone()})).await;
        assert!(commit.ok);
        assert_eq!(
            commit
                .result
                .as_ref()
                .and_then(|v| v.get("status"))
                .and_then(|v| v.as_str()),
            Some("committed")
        );

        let unknown = call_rpc(&state, "tx.rollback", json!({"tx_id": tx_id})).await;
        assert!(!unknown.ok);
        assert_eq!(
            unknown
                .error
                .as_ref()
                .map(|e| e.code.as_str())
                .unwrap_or_default(),
            "not_found"
        );

        let begin2 = call_rpc(&state, "tx.begin", json!({})).await;
        assert!(begin2.ok);
        let tx_id2 = begin2
            .result
            .as_ref()
            .and_then(|v| v.get("tx_id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let rollback = call_rpc(&state, "tx.rollback", json!({"tx_id": tx_id2})).await;
        assert!(rollback.ok);
        assert_eq!(
            rollback
                .result
                .as_ref()
                .and_then(|v| v.get("status"))
                .and_then(|v| v.as_str()),
            Some("rolled_back")
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn mysql_handshake_packet_advertises_native_auth() {
        let seed = [42u8; 20];
        let packet = mysql_handshake_packet(7, &seed);
        assert_eq!(packet.first().copied(), Some(MYSQL_PROTOCOL_VERSION));
        assert!(packet
            .windows(MYSQL_AUTH_PLUGIN.len())
            .any(|w| w == MYSQL_AUTH_PLUGIN.as_bytes()));
    }

    #[test]
    fn mysql_native_password_validation_roundtrip() {
        let seed = [9u8; 20];
        let scramble = mysql_native_password_scramble("secret", &seed);
        assert!(mysql_validate_native_password("secret", &seed, &scramble));
        assert!(!mysql_validate_native_password("wrong", &seed, &scramble));
    }

    #[test]
    fn parse_mysql_handshake_response_secure_connection() {
        let mut payload = Vec::new();
        let caps = MYSQL_CAP_PROTOCOL_41 | MYSQL_CAP_SECURE_CONNECTION | MYSQL_CAP_PLUGIN_AUTH;
        payload.extend_from_slice(&caps.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.push(0x21);
        payload.extend_from_slice(&[0u8; 23]);
        payload.extend_from_slice(b"root");
        payload.push(0);
        payload.push(3);
        payload.extend_from_slice(b"abc");
        payload.extend_from_slice(MYSQL_AUTH_PLUGIN.as_bytes());
        payload.push(0);

        let parsed = parse_mysql_handshake_response(&payload).expect("parse handshake response");
        assert_eq!(parsed.username, "root");
        assert_eq!(parsed.auth_response, b"abc");
        assert_eq!(parsed.auth_plugin.as_deref(), Some(MYSQL_AUTH_PLUGIN));
    }

    #[test]
    fn parse_select_literal_query_roundtrip() {
        let parsed = parse_select_literal_query(
            "SELECT 1 AS one, 'x' AS two, NULL, VERSION() AS version, DATABASE() AS db, @@sql_mode AS mode",
            Some("app"),
        )
        .expect("parse select literal");
        assert_eq!(parsed.len(), 6);
        assert_eq!(parsed[0].0, "one");
        assert_eq!(parsed[0].1, MySqlLiteral::Int(1));
        assert_eq!(parsed[1].0, "two");
        assert_eq!(parsed[1].1, MySqlLiteral::Str("x".to_string()));
        assert_eq!(parsed[2].0, "col3");
        assert_eq!(parsed[2].1, MySqlLiteral::Null);
        assert_eq!(parsed[3].0, "version");
        assert_eq!(
            parsed[3].1,
            MySqlLiteral::Str(MYSQL_SERVER_VERSION.to_string())
        );
        assert_eq!(parsed[4].0, "db");
        assert_eq!(parsed[4].1, MySqlLiteral::Str("app".to_string()));
        assert_eq!(parsed[5].0, "mode");
        assert_eq!(parsed[5].1, MySqlLiteral::Str(String::new()));
    }

    #[test]
    fn parse_select_literal_query_rejects_from_clause() {
        assert!(parse_select_literal_query("SELECT 1 FROM app.users", None).is_none());
    }

    #[test]
    fn mysql_stmt_prepare_columns_resolve_schema_and_projection_labels() {
        let SqlPlan::Select {
            from, projection, ..
        } = parse_sql_plan(
            "SELECT u.id AS user_id, u.name FROM app.users AS u WHERE u.id = 7",
            Some("app"),
        )
        .expect("parse select plan")
        else {
            panic!("expected SELECT plan");
        };
        let desc = json!({
            "columns": [
                {"name": "id", "type": {"kind": "u64"}},
                {"name": "name", "type": {"kind": "string"}}
            ]
        });
        let explicit =
            mysql_stmt_prepare_columns_from_select(from.as_ref(), &projection, Some(&desc));
        assert_eq!(
            explicit,
            vec![
                MySqlStmtPrepareColumn {
                    name: "user_id".to_string(),
                    column_type: MySqlStmtColumnType::LongLong,
                },
                MySqlStmtPrepareColumn {
                    name: "name".to_string(),
                    column_type: MySqlStmtColumnType::VarString,
                },
            ]
        );

        let SqlPlan::Select {
            from, projection, ..
        } = parse_sql_plan("SELECT * FROM app.users AS u", Some("app")).expect("parse wildcard")
        else {
            panic!("expected SELECT plan");
        };
        let wildcard =
            mysql_stmt_prepare_columns_from_select(from.as_ref(), &projection, Some(&desc));
        assert_eq!(
            wildcard,
            vec![
                MySqlStmtPrepareColumn {
                    name: "id".to_string(),
                    column_type: MySqlStmtColumnType::LongLong,
                },
                MySqlStmtPrepareColumn {
                    name: "name".to_string(),
                    column_type: MySqlStmtColumnType::VarString,
                },
            ]
        );
    }

    #[test]
    fn mysql_parse_set_autocommit_roundtrip() {
        assert_eq!(mysql_parse_set_autocommit("SET autocommit=0"), Some(false));
        assert_eq!(
            mysql_parse_set_autocommit("SET autocommit = 1;"),
            Some(true)
        );
        assert_eq!(mysql_parse_set_autocommit("SET sql_mode=''"), None);
    }

    #[test]
    fn mysql_is_session_compat_set_accepts_wordpress_bootstrap_sets() {
        assert!(mysql_is_session_compat_set("SET NAMES utf8mb4"));
        assert!(mysql_is_session_compat_set("SET SESSION sql_mode = ''"));
        assert!(mysql_is_session_compat_set(
            "SET @@session.character_set_results = 'utf8mb4'"
        ));
        assert!(!mysql_is_session_compat_set("SET time_zone = '+00:00'"));
    }

    #[test]
    fn mysql_like_matches_supports_percent_and_underscore() {
        assert!(mysql_like_matches("wp_posts", "wp_%"));
        assert!(mysql_like_matches("wp_posts", "wp_post_"));
        assert!(!mysql_like_matches("wp_posts", "wp_option_"));
    }

    #[test]
    fn mysql_parse_count_query_roundtrip() {
        let parsed = mysql_parse_count_query(
            "SELECT COUNT(*) AS publish_count FROM wp_posts WHERE post_status = 'publish' ORDER BY id DESC LIMIT 10",
        )
        .expect("parse count query");
        assert_eq!(parsed.0, "publish_count");
        assert_eq!(
            parsed.1,
            "SELECT * FROM wp_posts WHERE post_status = 'publish'"
        );
        assert!(mysql_parse_count_query("SELECT id FROM wp_posts").is_none());
    }

    #[test]
    fn mysql_parse_select_found_rows_query_roundtrip() {
        assert_eq!(
            mysql_parse_select_found_rows_query("SELECT FOUND_ROWS();").as_deref(),
            Some("FOUND_ROWS()")
        );
        assert_eq!(
            mysql_parse_select_found_rows_query("SELECT FOUND_ROWS() AS total;").as_deref(),
            Some("total")
        );
        assert!(mysql_parse_select_found_rows_query("SELECT FOUND_ROWS(), 1").is_none());
    }

    #[test]
    fn mysql_rewrite_sql_calc_found_rows_roundtrip() {
        assert_eq!(
            mysql_rewrite_sql_calc_found_rows(
                "SELECT SQL_CALC_FOUND_ROWS id FROM comments ORDER BY id DESC LIMIT 0, 2;"
            )
            .as_deref(),
            Some("SELECT id FROM comments ORDER BY id DESC LIMIT 0, 2")
        );
        assert!(mysql_rewrite_sql_calc_found_rows("SELECT id FROM comments").is_none());
    }

    #[test]
    fn parse_select_plan_supports_mysql_limit_offset_count() {
        let plan = parse_sql_plan("SELECT id FROM comments LIMIT 0, 2", Some("app"))
            .expect("parse sql plan");
        let SqlPlan::Select { limit, .. } = plan else {
            panic!("expected select plan");
        };
        let limit = limit.expect("expected limit clause");
        assert_eq!(limit.offset, Some(0));
        assert_eq!(limit.limit, Some(2));
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
    async fn stats_top_and_slow_queries_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("stats_top_slow_queries");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        for _ in 0..3 {
            let resp = call_rpc(&state, "system.ping", json!({})).await;
            assert!(resp.ok);
        }
        let resp = call_rpc(&state, "system.version", json!({})).await;
        assert!(resp.ok);

        let top = call_rpc(
            &state,
            "stats.top_queries",
            json!({"limit": 5, "sort_by": "count"}),
        )
        .await;
        assert!(top.ok);
        let top_queries = top
            .result
            .as_ref()
            .and_then(|v| v.get("queries"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(!top_queries.is_empty());
        let ping = top_queries
            .iter()
            .find(|q| q.get("method").and_then(|v| v.as_str()) == Some("system.ping"))
            .expect("system.ping should be tracked");
        assert!(ping.get("count").and_then(|v| v.as_u64()).unwrap_or(0) >= 3);
        assert_eq!(
            ping.get("fingerprint")
                .and_then(|v| v.as_str())
                .map(|s| s.len()),
            Some(64)
        );

        let slow = call_rpc(
            &state,
            "stats.slow_queries",
            json!({"limit": 20, "min_ms": 0}),
        )
        .await;
        assert!(slow.ok);
        let slow_queries = slow
            .result
            .as_ref()
            .and_then(|v| v.get("queries"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(slow_queries.iter().any(|q| {
            q.get("method")
                .and_then(|v| v.as_str())
                .map(|m| m == "system.ping")
                .unwrap_or(false)
        }));

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
    async fn sql_exec_corpus_ddl_dml_subset_roundtrip() -> anyhow::Result<()> {
        let dir = temp_dir("sql_exec_corpus_subset");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let create_db = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE DATABASE IF NOT EXISTS skein_test"
            }),
        )
        .await;
        assert!(create_db.ok);

        let drop_existing = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "DROP TABLE IF EXISTS wp_options",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(drop_existing.ok);

        let create_table = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE TABLE wp_options (option_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, option_name VARCHAR(191) NOT NULL, option_value LONGTEXT NOT NULL, autoload VARCHAR(20) NOT NULL DEFAULT 'yes', PRIMARY KEY (option_id), UNIQUE KEY option_name (option_name)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(create_table.ok);

        let insert = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('siteurl', 'https://example.com', 'yes')",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(insert.ok);

        let upsert = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('siteurl', 'https://example.net', 'yes') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value), autoload = VALUES(autoload)",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(upsert.ok);
        assert_eq!(
            upsert
                .result
                .as_ref()
                .and_then(|v| v.get("write"))
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64()),
            Some(1)
        );

        let select = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT option_value FROM wp_options WHERE option_name = 'siteurl'",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(select.ok);
        let rows = select
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            rows.first()
                .and_then(|r| r.as_array())
                .and_then(|r| r.first())
                .and_then(|v| v.get("v"))
                .and_then(|v| v.as_str()),
            Some("https://example.net")
        );

        let original_id = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT option_id FROM wp_options WHERE option_name = 'siteurl'",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(original_id.ok);
        let original_id = original_id
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.as_array())
            .and_then(|row| row.first())
            .and_then(|v| v.get("v"))
            .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|n| n as u64)))
            .unwrap_or_default();

        let shuffled_upsert = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_options (option_value, option_name, autoload) VALUES ('https://example.shuffle', 'siteurl', 'no') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value), autoload = VALUES(autoload)",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(shuffled_upsert.ok);
        assert_eq!(
            shuffled_upsert
                .result
                .as_ref()
                .and_then(|v| v.get("write"))
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64()),
            Some(1)
        );

        let shuffled_select = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT option_value, autoload FROM wp_options WHERE option_name = 'siteurl'",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(shuffled_select.ok);
        let shuffled_rows = shuffled_select
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            shuffled_rows[0][0]["v"].as_str(),
            Some("https://example.shuffle")
        );
        assert_eq!(shuffled_rows[0][1]["v"].as_str(), Some("no"));

        let shuffled_replace = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "REPLACE INTO wp_options (option_value, option_name, autoload) VALUES ('https://example.replace', 'siteurl', 'yes')",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(shuffled_replace.ok);
        assert_eq!(
            shuffled_replace
                .result
                .as_ref()
                .and_then(|v| v.get("write"))
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64()),
            Some(2)
        );

        let replaced_select = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT option_id, option_value, autoload FROM wp_options WHERE option_name = 'siteurl'",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(replaced_select.ok);
        let replaced_rows = replaced_select
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let replaced_id = replaced_rows[0][0]["v"]
            .as_u64()
            .or_else(|| replaced_rows[0][0]["v"].as_i64().map(|n| n as u64))
            .unwrap_or_default();
        assert_ne!(replaced_id, original_id);
        assert_eq!(
            replaced_rows[0][1]["v"].as_str(),
            Some("https://example.replace")
        );
        assert_eq!(replaced_rows[0][2]["v"].as_str(), Some("yes"));

        let drop_table = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "DROP TABLE IF EXISTS wp_options",
                "default_db": "skein_test"
            }),
        )
        .await;
        assert!(drop_table.ok);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn sql_exec_supports_in_like_and_is_null_predicates() -> anyhow::Result<()> {
        let dir = temp_dir("sql_exec_wordpress_predicates");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let create_db = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE DATABASE IF NOT EXISTS wp"
            }),
        )
        .await;
        assert!(create_db.ok);

        let create_table = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE TABLE wp_posts (id BIGINT NOT NULL, post_status VARCHAR(20) NOT NULL, post_title TEXT NOT NULL, post_excerpt TEXT, PRIMARY KEY (id))",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(create_table.ok);

        let insert = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_posts (id, post_status, post_title, post_excerpt) VALUES (1, 'publish', 'Hello', NULL), (2, 'draft', 'Draft', 'Preview'), (3, 'publish', 'World', NULL)",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(insert.ok);

        let in_query = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id FROM wp_posts WHERE post_status IN ('publish', 'private') ORDER BY id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(in_query.ok);
        let in_rows = in_query
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(in_rows.len(), 2);

        let like_query = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id FROM wp_posts WHERE post_status LIKE 'pub%' ORDER BY id DESC LIMIT 1",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(like_query.ok);
        let like_rows = like_query
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(like_rows.len(), 1);
        assert_eq!(like_rows[0][0]["v"].as_i64(), Some(3));

        let null_query = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id FROM wp_posts WHERE post_excerpt IS NULL ORDER BY id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(null_query.ok);
        let null_rows = null_query
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(null_rows.len(), 2);

        let eq_null_query = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id FROM wp_posts WHERE post_excerpt = NULL ORDER BY id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(eq_null_query.ok);
        let eq_null_rows = eq_null_query
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(eq_null_rows.is_empty());

        let nullable_like_query = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id FROM wp_posts WHERE post_excerpt LIKE 'P%' ORDER BY id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(nullable_like_query.ok);
        let nullable_like_rows = nullable_like_query
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(nullable_like_rows.len(), 1);
        assert_eq!(nullable_like_rows[0][0]["v"].as_i64(), Some(2));

        let nullable_in_query = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT id FROM wp_posts WHERE post_excerpt IN ('Preview', NULL) ORDER BY id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(nullable_in_query.ok);
        let nullable_in_rows = nullable_in_query
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(nullable_in_rows.len(), 1);
        assert_eq!(nullable_in_rows[0][0]["v"].as_i64(), Some(2));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn sql_exec_supports_insert_ignore_replace_distinct_join_and_alter_table(
    ) -> anyhow::Result<()> {
        let dir = temp_dir("sql_exec_wordpress_shapes");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        assert!(
            call_rpc(
                &state,
                "sql.exec",
                json!({"sql":"CREATE DATABASE IF NOT EXISTS wp"})
            )
            .await
            .ok
        );

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE TABLE wp_users (id BIGINT NOT NULL, status VARCHAR(20) NOT NULL, name VARCHAR(64) NOT NULL, PRIMARY KEY (id))",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE TABLE wp_posts (id BIGINT NOT NULL, post_author BIGINT NOT NULL, post_status VARCHAR(20) NOT NULL, PRIMARY KEY (id))",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE TABLE wp_options (option_id BIGINT NOT NULL AUTO_INCREMENT, option_name VARCHAR(64) NOT NULL, option_value VARCHAR(64) NOT NULL, autoload VARCHAR(20) NOT NULL DEFAULT 'yes', PRIMARY KEY (option_id), UNIQUE KEY option_name (option_name))",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        let create_unique = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "CREATE TABLE wp_unique_options (option_id BIGINT NOT NULL AUTO_INCREMENT, option_name VARCHAR(64) NOT NULL, option_value VARCHAR(64) NOT NULL, PRIMARY KEY (option_id), UNIQUE KEY option_name (option_name))",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(create_unique.ok);

        let unique_desc = call_rpc(
            &state,
            "schema.describe_table",
            json!({
                "db": "wp",
                "table": "wp_unique_options"
            }),
        )
        .await;
        assert!(unique_desc.ok);
        let indexes = unique_desc
            .result
            .as_ref()
            .and_then(|v| v.get("indexes"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(indexes.len(), 1);
        assert_eq!(
            indexes[0].get("name").and_then(|v| v.as_str()),
            Some("option_name")
        );
        assert_eq!(
            indexes[0].get("unique").and_then(|v| v.as_bool()),
            Some(true)
        );

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_unique_options (option_name, option_value) VALUES ('siteurl', 'https://example.com')",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        let duplicate_unique = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_unique_options (option_name, option_value) VALUES ('siteurl', 'https://duplicate.example')",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(!duplicate_unique.ok);
        assert_eq!(
            duplicate_unique.error.as_ref().map(|e| e.code.as_str()),
            Some("conflict")
        );

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_options (option_name, option_value) VALUES ('timezone_string', 'UTC')",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        let created_default = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT autoload FROM wp_options WHERE option_name = 'timezone_string'",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(created_default.ok);
        let created_default_rows = created_default
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(created_default_rows[0][0]["v"].as_str(), Some("yes"));

        let ignored_option = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT IGNORE INTO wp_options (option_name, option_value) VALUES ('timezone_string', 'Europe/Berlin')",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(ignored_option.ok);
        assert_eq!(
            ignored_option
                .result
                .as_ref()
                .and_then(|v| v.get("write"))
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64()),
            Some(0)
        );

        let replaced_option = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "REPLACE INTO wp_options (option_name, option_value, autoload) VALUES ('timezone_string', 'Europe/Berlin', 'no')",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(replaced_option.ok);
        assert_eq!(
            replaced_option
                .result
                .as_ref()
                .and_then(|v| v.get("write"))
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64()),
            Some(2)
        );

        let replaced_option_rows = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT option_value, autoload FROM wp_options WHERE option_name = 'timezone_string'",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(replaced_option_rows.ok);
        let replaced_option_rows = replaced_option_rows
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            replaced_option_rows[0][0]["v"].as_str(),
            Some("Europe/Berlin")
        );
        assert_eq!(replaced_option_rows[0][1]["v"].as_str(), Some("no"));

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_users (id, status, name) VALUES (1, 'active', 'Ada'), (2, 'active', 'Grace')",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        let ignored = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT IGNORE INTO wp_users (id, status, name) VALUES (1, 'inactive', 'Ignored'), (3, 'active', 'Linus')",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(ignored.ok);
        assert_eq!(
            ignored
                .result
                .as_ref()
                .and_then(|v| v.get("write"))
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64()),
            Some(1)
        );

        let replaced = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "REPLACE INTO wp_users (id, status, name) VALUES (2, 'active', 'Grace Hopper')",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(replaced.ok);
        assert_eq!(
            replaced
                .result
                .as_ref()
                .and_then(|v| v.get("write"))
                .and_then(|v| v.get("affected"))
                .and_then(|v| v.as_u64()),
            Some(2)
        );

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "ALTER TABLE wp_posts ADD COLUMN post_title VARCHAR(64) NOT NULL DEFAULT 'untitled'",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_posts (id, post_author, post_status) VALUES (10, 1, 'publish'), (11, 1, 'draft'), (12, 3, 'publish')",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        let distinct = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT DISTINCT post_author FROM wp_posts ORDER BY post_author ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(distinct.ok);
        let distinct_rows = distinct
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(distinct_rows.len(), 2);

        let join = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT DISTINCT p.post_author AS author_id, u.name FROM wp_posts AS p INNER JOIN wp_users AS u ON p.post_author = u.id WHERE u.status = 'active' ORDER BY p.post_author ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(join.ok);
        let join_rows = join
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(join_rows.len(), 2);
        assert_eq!(join_rows[0][0]["v"].as_i64(), Some(1));
        assert_eq!(join_rows[0][1]["v"].as_str(), Some("Ada"));
        assert_eq!(join_rows[1][0]["v"].as_i64(), Some(3));
        assert_eq!(join_rows[1][1]["v"].as_str(), Some("Linus"));

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_posts (id, post_author, post_status) VALUES (13, 99, 'publish')",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        let left_join = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT p.id, u.name FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE u.name IS NULL ORDER BY p.id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(left_join.ok);
        let left_join_rows = left_join
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(left_join_rows.len(), 1);
        assert_eq!(left_join_rows[0][0]["v"].as_i64(), Some(13));
        assert_eq!(left_join_rows[0][1]["t"].as_str(), Some("null"));

        let left_join_eq = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT p.id FROM wp_posts AS p LEFT JOIN wp_users AS u ON p.post_author = u.id WHERE u.name = 'Ada' ORDER BY p.id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(left_join_eq.ok);
        let left_join_eq_rows = left_join_eq
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(left_join_eq_rows.len(), 2);
        assert_eq!(left_join_eq_rows[0][0]["v"].as_i64(), Some(10));
        assert_eq!(left_join_eq_rows[1][0]["v"].as_i64(), Some(11));

        assert!(call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "INSERT INTO wp_users (id, status, name) VALUES (4, 'active', 'Margaret')",
                "default_db": "wp"
            }),
        )
        .await
        .ok);

        let right_join = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT u.id, p.id FROM wp_posts AS p RIGHT JOIN wp_users AS u ON p.post_author = u.id WHERE p.id IS NULL ORDER BY u.id ASC",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(right_join.ok);
        let right_join_rows = right_join
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(right_join_rows.len(), 2);
        assert_eq!(right_join_rows[0][0]["v"].as_i64(), Some(2));
        assert_eq!(right_join_rows[0][1]["t"].as_str(), Some("null"));
        assert_eq!(right_join_rows[1][0]["v"].as_i64(), Some(4));
        assert_eq!(right_join_rows[1][1]["t"].as_str(), Some("null"));

        let altered = call_rpc(
            &state,
            "sql.exec",
            json!({
                "sql": "SELECT post_title FROM wp_posts WHERE id = 10",
                "default_db": "wp"
            }),
        )
        .await;
        assert!(altered.ok);
        let altered_rows = altered
            .result
            .as_ref()
            .and_then(|v| v.get("result"))
            .and_then(|v| v.get("data"))
            .and_then(|v| v.get("rows"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(altered_rows[0][0]["v"].as_str(), Some("untitled"));

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

    #[tokio::test]
    async fn cluster_node_leave_marks_node_offline() -> anyhow::Result<()> {
        let dir = temp_dir("cluster_leave");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let token = call_rpc(&state, "cluster.join_token.create", json!({}))
            .await
            .result
            .expect("missing join token result")["token"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let joined = call_rpc(
            &state,
            "cluster.node.join",
            json!({
                "token": token,
                "node_id": "replica-offline",
                "rpc_url": "http://127.0.0.1:19091",
                "role": "replica"
            }),
        )
        .await;
        assert!(joined.ok);

        let leave = call_rpc(
            &state,
            "cluster.node.leave",
            json!({
                "node_id": "replica-offline"
            }),
        )
        .await;
        assert!(leave.ok);
        assert_eq!(
            leave
                .result
                .as_ref()
                .and_then(|v| v.get("status"))
                .and_then(|v| v.as_str()),
            Some("offline")
        );

        let nodes = call_rpc(
            &state,
            "cluster.nodes",
            json!({
                "role": "replica"
            }),
        )
        .await;
        assert!(nodes.ok);
        assert_eq!(
            nodes
                .result
                .as_ref()
                .and_then(|v| v.get("nodes"))
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.get("status"))
                .and_then(|v| v.as_str()),
            Some("offline")
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn cluster_node_leave_reassigns_shard_primary() -> anyhow::Result<()> {
        let dir = temp_dir("cluster_leave_shard");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let token = call_rpc(&state, "cluster.join_token.create", json!({}))
            .await
            .result
            .expect("missing join token result")["token"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let joined = call_rpc(
            &state,
            "cluster.node.join",
            json!({
                "token": token,
                "node_id": "replica-shard",
                "rpc_url": "http://127.0.0.1:19092",
                "role": "replica"
            }),
        )
        .await;
        assert!(joined.ok);

        let created = call_rpc(
            &state,
            "cluster.shard.create",
            json!({
                "db": "app",
                "table": "events",
                "replicas": ["replica-shard"]
            }),
        )
        .await;
        assert!(created.ok);
        let shard_id = created
            .result
            .as_ref()
            .and_then(|v| v.get("shard"))
            .and_then(|v| v.get("shard_id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        assert!(!shard_id.is_empty());

        let moved = call_rpc(
            &state,
            "cluster.shard.move",
            json!({
                "shard_id": shard_id,
                "to_node_id": "replica-shard",
                "dry_run": false
            }),
        )
        .await;
        assert!(moved.ok);

        let leave = call_rpc(
            &state,
            "cluster.node.leave",
            json!({
                "node_id": "replica-shard"
            }),
        )
        .await;
        assert!(leave.ok);

        let status = call_rpc(&state, "cluster.status", json!({})).await;
        assert!(status.ok);
        let shard_primary = status
            .result
            .as_ref()
            .and_then(|v| v.get("shards"))
            .and_then(|v| v.as_array())
            .and_then(|v| v.first())
            .and_then(|v| v.get("primary_node_id"))
            .and_then(|v| v.as_str());
        assert_eq!(shard_primary, Some("node-test"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_tasks_checkpoint_and_mark_local_offline() -> anyhow::Result<()> {
        let dir = temp_dir("shutdown_tasks");
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
                            v: "shutdown-a".to_string(),
                        },
                    ),
                ]),
                row(&[
                    ("id", Lit::U64 { v: 2 }),
                    (
                        "payload",
                        Lit::Str {
                            v: "shutdown-b".to_string(),
                        },
                    ),
                ]),
            ],
            None,
        )?;
        let state = build_state(dir.clone(), engine);

        run_shutdown_tasks(&state).await;

        let settings_bytes = std::fs::read(settings_path(&state))?;
        let settings_json: Value = serde_json::from_slice(&settings_bytes)?;
        let local_status = settings_json
            .get(CLUSTER_STATE_KEY)
            .and_then(|v| v.get("nodes"))
            .and_then(|v| v.as_array())
            .and_then(|nodes| {
                nodes
                    .iter()
                    .find(|n| n.get("node_id").and_then(|v| v.as_str()) == Some("node-test"))
            })
            .and_then(|n| n.get("status"))
            .and_then(|v| v.as_str());
        assert_eq!(local_status, Some("offline"));

        assert!(dir.join("catalog.json").exists());
        assert!(dir.join("changes.json").exists());
        assert!(dir.join("prepared.json").exists());
        assert!(dir.join("snapshots.json").exists());

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_notifies_peers_with_cluster_node_leave() -> anyhow::Result<()> {
        let dir = temp_dir("shutdown_notify");
        let engine = Engine::open(&dir)?;
        let state = build_state(dir.clone(), engine);

        let seen_methods: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_nodes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_header: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));

        let methods_ref = seen_methods.clone();
        let nodes_ref = seen_nodes.clone();
        let header_ref = seen_header.clone();
        let app = Router::new().route(
            "/api/v1/rpc",
            post(move |headers: HeaderMap, Json(payload): Json<Value>| {
                let methods_ref = methods_ref.clone();
                let nodes_ref = nodes_ref.clone();
                let header_ref = header_ref.clone();
                async move {
                    let method = payload
                        .get("method")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let node_id = payload
                        .get("params")
                        .and_then(|v| v.get("node_id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let replication = headers
                        .get(REPLICATION_HEADER)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| v.to_string());
                    methods_ref.lock().unwrap().push(method);
                    nodes_ref.lock().unwrap().push(node_id);
                    header_ref.lock().unwrap().push(replication);
                    Json(serde_json::json!({
                        "skeinql": SKEINQL_VERSION,
                        "ok": true,
                        "result": {"ok": true}
                    }))
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let peer_addr = listener.local_addr()?;
        let mock = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        {
            let mut cluster = state.cluster.lock().unwrap();
            cluster.enabled = true;
            cluster.nodes.push(ClusterNode {
                node_id: "peer-a".to_string(),
                rpc_url: format!("http://{}", peer_addr),
                role: "replica".to_string(),
                status: "online".to_string(),
                joined_at_ms: now_unix_ms_u64(),
                last_seen_ms: now_unix_ms_u64(),
            });
        }

        notify_cluster_node_leave(&state).await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        mock.abort();

        assert_eq!(
            seen_methods.lock().unwrap().as_slice(),
            &["cluster.node.leave"]
        );
        assert_eq!(seen_nodes.lock().unwrap().as_slice(), &["node-test"]);
        assert_eq!(
            seen_header.lock().unwrap().as_slice(),
            &[Some("1".to_string())]
        );

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }
}
