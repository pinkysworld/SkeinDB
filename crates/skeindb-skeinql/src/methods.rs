//! SkeinQL v1.0 method parameter/result types.
//!
//! Note: SkeinDB is designed to implement SkeinQL as a stable API contract.
//! Implementations may not support every method initially; unsupported methods
//! should return `not_supported`.

use serde::{Deserialize, Serialize};

use crate::types::{
    BaseTableRef, CausalityToken, Expr, Lit, Query, QueryCache, ResultFormat, TypeDesc, WireHints,
};

// --------------------------------
// system.*
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemPingResult {
    pub pong: bool,
    pub time_unix_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemVersionResult {
    pub name: String,
    pub version: String,
    pub skeinql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemCapabilitiesResult {
    pub mysql_compat: bool,
    pub skeinql: bool,

    #[serde(default)]
    pub etag_queries: bool,
    #[serde(default)]
    pub causal_etags: bool,
    #[serde(default)]
    pub wasm_udf: bool,
    #[serde(default)]
    pub wasm_operators: bool,
    #[serde(default)]
    pub column_snapshots: bool,
    #[serde(default)]
    pub audit_wal: bool,
    #[serde(default)]
    pub cluster: bool,

    /// Optional: list of methods supported in this build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub methods: Option<Vec<String>>,

    /// Optional hashing configuration (canonicalization + algorithm).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<serde_json::Value>,
}

// --------------------------------
// settings.*
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsGetParams {
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsSetParams {
    /// Arbitrary settings object; keys are setting names.
    #[serde(flatten)]
    pub values: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsSetResult {
    pub ok: bool,
}

// --------------------------------
// schema.* (selected)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListDatabasesResult {
    pub databases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListTablesParams {
    pub db: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaListTablesResult {
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDescribeTableParams {
    pub db: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumnInfo {
    pub name: String,
    pub r#type: TypeDesc,
    pub nullable: bool,
    #[serde(default)]
    pub auto_increment: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDescribeTableResult {
    pub db: String,
    pub table: String,
    pub columns: Vec<SchemaColumnInfo>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub indexes: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compat_mysql: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaCreateDatabaseParams {
    pub db: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaCreateTableParams {
    pub db: String,
    pub table: String,
    pub columns: Vec<SchemaColumnInfo>,

    #[serde(default)]
    pub primary_key: Vec<String>,

    #[serde(default)]
    pub if_not_exists: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compat_mysql: Option<serde_json::Value>,
}

// --------------------------------
// schema.* (experimental)
// --------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SchemaChangeOp {
    AddColumn {
        name: String,
        r#type: TypeDesc,
        nullable: bool,
        #[serde(default)]
        auto_increment: bool,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<Lit>,
    },
    AddIndex {
        name: String,
        columns: Vec<String>,
        #[serde(default)]
        unique: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaProposeChangeParams {
    pub table: BaseTableRef,
    pub base_version: u64,
    pub changes: Vec<SchemaChangeOp>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaProposeChangeResult {
    pub change_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeSummary {
    pub change_id: String,
    pub base_version: u64,
    pub changes: Vec<SchemaChangeOp>,
    pub status: String,
    pub created_at_ms: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeConflict {
    pub change_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMergeStatusParams {
    pub table: BaseTableRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeResolution {
    pub change_id: String,
    pub action: String,
    pub reason: String,
    pub suggestion: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMergeStatusResult {
    pub table: BaseTableRef,
    pub current_version: u64,
    pub pending: Vec<SchemaChangeSummary>,
    pub conflicts: Vec<SchemaChangeConflict>,
    pub merge_plan: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolution: Vec<SchemaChangeResolution>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSimulateRolloutParams {
    pub table: BaseTableRef,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSimulateRolloutStage {
    pub step: u64,
    pub stage: String,
    pub upgraded_nodes: u64,
    pub legacy_nodes: u64,
    pub old_schema_version: u64,
    pub new_schema_version: u64,
    pub mixed_versions: bool,
    pub requires_query_adaptation: bool,
    pub requires_row_adaptation: bool,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSimulateRolloutResult {
    pub format: String,
    pub table: BaseTableRef,
    pub current_version: u64,
    pub target_version: u64,
    pub nodes: u64,
    pub pending_change_count: u64,
    pub ready_for_rollout: bool,
    pub legacy_row_count: u64,
    pub requires_query_adaptation: bool,
    pub merge_plan: Vec<String>,
    pub conflicts: Vec<SchemaChangeConflict>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolution: Vec<SchemaChangeResolution>,

    pub stages: Vec<SchemaSimulateRolloutStage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaApplyMergeParams {
    pub table: BaseTableRef,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaApplyMergeResult {
    pub applied: Vec<String>,
    pub conflicts: Vec<SchemaChangeConflict>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rolled_back: Vec<SchemaChangeConflict>,
    pub new_version: u64,
}

// --------------------------------
// tx.*
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxBeginParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxBeginResult {
    pub tx: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxCommitParams {
    pub tx: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxRollbackParams {
    pub tx: String,
}

// --------------------------------
// data.*
// --------------------------------

/// Row object mapping column name to typed literal.
pub type RowObject = std::collections::BTreeMap<String, Lit>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataGetParams {
    pub table: BaseTableRef,
    pub pk: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<ResultFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataInsertParams {
    pub into: BaseTableRef,
    pub rows: Vec<RowObject>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returning: Option<Vec<String>>,

    /// Engine-specific hints (dedup, delta policies, chunking, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hints: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataUpdateParams {
    pub table: BaseTableRef,
    pub r#where: Expr,
    pub set: RowObject,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    /// Optional ETag for optimistic concurrency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_match: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataDeleteParams {
    pub table: BaseTableRef,
    pub r#where: Expr,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataUpsertParams {
    pub into: BaseTableRef,
    pub rows: Vec<RowObject>,

    /// Conflict target (e.g., ["id"]). If omitted, defaults to primary key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_conflict: Option<Vec<String>>,

    /// Column assignments to apply on conflict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_set: Option<RowObject>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returning: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hints: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    pub affected: u64,
    #[serde(default)]
    pub last_insert_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returning: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

// --------------------------------
// query.*
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecParams {
    pub query: Query,

    /// Positional parameter bindings for `expr.param`.
    ///
    /// Example: param 0 reads args[0].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_format: Option<ResultFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<QueryCache>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire: Option<WireHints>,

    /// Optional transaction id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,

    /// Optional time-travel read timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<Lit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,

    #[serde(default)]
    pub not_modified: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deps: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causality: Option<CausalityToken>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire: Option<serde_json::Value>,
}

// --------------------------------
// query.patch client-state summaries
// --------------------------------

/// Client-provided summary of a row in the *current client view* of a query result.
///
/// This is used as a fallback when the server cannot find the `base_etag` snapshot
/// in its cache (e.g., restart/eviction) but still wants to compute an exact patch
/// without forcing a full refetch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRowState {
    /// Primary key values.
    pub pk: Vec<Lit>,

    /// Optional row fingerprint (hex) computed over the projected row.
    ///
    /// If provided, the server can detect which rows changed and send only
    /// the updated rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fp: Option<String>,
}

/// Optional client summary for patch fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientStateSummary {
    /// Exact list of rows in the client's current view (window) of the query result.
    ///
    /// Recommended for correctness: includes `pk` and (optionally) `fp`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<ClientRowState>>,

    /// Optional Bloom filter summary of client-known row keys.
    ///
    /// This is an **optimization-only** hint intended for very large result windows.
    /// Implementations may treat this as best-effort and return a partial patch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys_bloom: Option<serde_json::Value>,

    /// Patch strictness mode.
    ///
    /// - "strict" (default): requires exact base (etag cache or client rows+fp)
    /// - "add_only": server may return only additions (and mark removals/updates unknown)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}
/// Compute a delta patch between a previously seen query result (identified by `base_etag`)
/// and the current query result.
///
/// The response envelope is the same as `query.select` (`QueryExecResult`).
/// The patch payload is returned in `data`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPatchParams {
    pub query: Query,

    /// Positional parameter bindings for `expr.param`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Lit>,

    /// The ETag of the query result the client currently has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_etag: Option<String>,

    /// Optional client-provided summary of its current view.
    ///
    /// This enables exact patches even if the server lost the cached base snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_state: Option<ClientStateSummary>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_format: Option<ResultFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire: Option<WireHints>,

    /// If true (default), the server may include a full result under `data.full`
    /// when it cannot compute a patch and needs the client to reset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_full: Option<bool>,

    /// Optional transaction id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,

    /// Optional time-travel read timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<Lit>,
}

/// Result type for `query.patch`.
///
/// The patch is returned inside `QueryExecResult.data`.
pub type QueryPatchResult = QueryExecResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPrepareParams {
    pub query: Query,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPrepareResult {
    pub query_id: String,
    pub canonical: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutePreparedParams {
    pub query_id: String,

    /// Positional parameter bindings for `expr.param`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Lit>,

    /// Optional ETag validator (If-None-Match semantics).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_none_match: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_causality: Option<CausalityToken>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_format: Option<ResultFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire: Option<WireHints>,
}

/// Result type for `query.execute_prepared`.
pub type QueryExecutePreparedResult = QueryExecResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplainParams {
    pub query: Query,
}

// --------------------------------
// vector.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorInsertRow {
    pub pk: Vec<Lit>,
    pub embedding: Lit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorInsertParams {
    pub table: BaseTableRef,
    pub column: String,
    pub rows: Vec<VectorInsertRow>,

    #[serde(default)]
    pub upsert: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorInsertResult {
    pub inserted: u64,
    pub updated: u64,
    pub embedding_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchParams {
    pub table: BaseTableRef,
    pub column: String,
    pub query: Lit,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Expr>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_row: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_lsh: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<QueryCache>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchMatch {
    pub pk: Vec<Lit>,
    pub score: f64,
    pub embedding_id: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<RowObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    pub matches: Vec<VectorSearchMatch>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,

    #[serde(default)]
    pub not_modified: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deps: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causality: Option<CausalityToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorBenchmarkParams {
    pub table: BaseTableRef,
    pub column: String,
    pub queries: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_index: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorBenchmarkLatencyStats {
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorBenchmarkRunStats {
    pub strategy: String,
    pub total_matches: u64,
    pub latency: VectorBenchmarkLatencyStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorBenchmarkResult {
    pub table: BaseTableRef,
    pub column: String,
    pub metric: String,
    pub k: u64,
    pub queries: u64,
    pub vectors: u64,
    pub exact: VectorBenchmarkRunStats,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed: Option<VectorBenchmarkRunStats>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recall_at_k: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexStatusParams {
    pub table: BaseTableRef,
    pub column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexStatusResult {
    pub table: BaseTableRef,
    pub column: String,
    pub vectors: u64,
    pub buckets: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_bucket: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bucket: Option<u64>,
}

// --------------------------------
// ai.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamLiteral {
    pub value: Lit,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamRules {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub semantic_constant_columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parameterize_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamLabelSchemaParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamLabelFieldSchema {
    pub field: String,
    pub required: bool,
    pub meaning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamDecisionSchema {
    pub decision: String,
    pub parameterized: bool,
    pub cache_key_policy: String,
    pub meaning: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamLabelSchemaResult {
    pub format: String,
    pub taxonomy_version: u64,
    pub default_decision: String,
    pub confidence_min: f64,
    pub confidence_max: f64,
    pub literal_fields: Vec<AiAutoparamLabelFieldSchema>,
    pub label_fields: Vec<AiAutoparamLabelFieldSchema>,
    pub decisions: Vec<AiAutoparamDecisionSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamClassifierSpec {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamClassifiersParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamClassifierInfo {
    pub name: String,
    pub kind: String,
    pub default: bool,
    pub supports_rules: bool,
    pub supports_schema: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamClassifiersResult {
    pub format: String,
    pub default_classifier: String,
    pub classifiers: Vec<AiAutoparamClassifierInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamClassifyParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,

    pub literals: Vec<AiAutoparamLiteral>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<AiAutoparamRules>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classifier: Option<AiAutoparamClassifierSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamLabel {
    pub idx: u64,
    pub decision: String,
    pub confidence: f64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamClassifyResult {
    pub labels: Vec<AiAutoparamLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamAnalyzeParams {
    pub sql: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<AiAutoparamRules>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classifier: Option<AiAutoparamClassifierSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamAnalyzeResult {
    pub normalized_sql: String,
    pub fingerprint: String,
    pub literals: Vec<AiAutoparamLiteral>,
    pub labels: Vec<AiAutoparamLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamFeedbackParams {
    pub sql: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<AiAutoparamRules>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classifier: Option<AiAutoparamClassifierSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_event: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub miss_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamFeedbackResult {
    pub format: String,
    pub fingerprint: String,
    pub normalized_sql: String,
    pub cache_event: String,
    pub classifier: String,
    pub cached_before: bool,
    pub reclassified: bool,
    pub cache_miss_count: u64,
    pub reclassification_count: u64,
    pub literals: Vec<AiAutoparamLiteral>,
    pub labels: Vec<AiAutoparamLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamMetricsParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAutoparamMetricsResult {
    pub format: String,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub plan_cache_hit_rate: f64,
    pub classifier_invocations: u64,
    pub classifier_total_ns: u64,
    pub classifier_mean_ns: f64,
    pub feedback_cache_entries: u64,
    pub feedback_cache_miss_count: u64,
    pub feedback_reclassifications: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlTranslateParams {
    pub db: String,
    pub request: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_schema: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tables: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlPromptTable {
    pub table: String,
    pub columns: Vec<SchemaColumnInfo>,
    #[serde(default)]
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlPromptPackage {
    pub version: String,
    pub db: String,
    pub request: String,
    pub read_only: bool,
    pub tables: Vec<AiNlPromptTable>,
    pub prompt: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlTranslateResult {
    pub package: AiNlPromptPackage,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<Query>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlExplainParams {
    pub query: Query,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<Lit>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_format: Option<ResultFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlExplainResult {
    pub summary: String,
    pub tables: Vec<String>,
    pub projection: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    pub deps: serde_json::Value,

    pub approval_token: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_format: Option<ResultFormat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlExecuteParams {
    pub query: Query,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<Lit>>,

    pub approval_token: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_format: Option<ResultFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub want_etag: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_none_match: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_causality: Option<CausalityToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiNlExecuteResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,

    #[serde(default)]
    pub not_modified: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deps: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causality: Option<CausalityToken>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire: Option<serde_json::Value>,
}

// --------------------------------
// dp.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpBounds {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpAggregateSpec {
    pub op: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentile: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bounds: Option<DpBounds>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#as: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpAggregateParams {
    pub table: BaseTableRef,
    pub aggregates: Vec<DpAggregateSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#where: Option<Expr>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,

    pub epsilon: f64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mechanism: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpAggregateResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privacy: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpBudgetSetParams {
    pub principal: String,
    pub total_epsilon: f64,
    pub total_delta: f64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_window_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpBudgetGetParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_usage: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpBudgetGetResult {
    pub budgets: Vec<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpAuditLogParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpAuditLogResult {
    pub events: Vec<serde_json::Value>,
    pub next_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpEvaluateParams {
    pub table: BaseTableRef,
    pub aggregates: Vec<DpAggregateSpec>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#where: Option<Expr>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub epsilons: Vec<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mechanism: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trials: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpEvaluatePoint {
    pub epsilon: f64,
    pub trials: u64,
    pub samples: u64,
    pub mean_abs_error: f64,
    pub p95_abs_error: f64,
    pub max_abs_error: f64,
    pub mean_relative_error: f64,
    pub avg_latency_ms: f64,
    pub overhead_vs_exact: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpEvaluateResult {
    pub table: BaseTableRef,
    pub columns: Vec<String>,
    pub exact_rows: Vec<Vec<serde_json::Value>>,
    pub rows_examined: u64,
    pub rows_matched: u64,
    pub exact_latency_ms: f64,
    pub mechanism: String,
    pub delta: f64,
    pub trials: u64,
    pub report: Vec<DpEvaluatePoint>,
}

// --------------------------------
// advisor.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexSynthesizeParams {
    pub table: BaseTableRef,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_queries: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexSuggestion {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    pub columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    pub score: f64,
    pub count: u64,
    pub rows_scanned: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency: Option<AdvisorIndexDependencyCapture>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<AdvisorIndexCostEstimate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexDependencyCapture {
    pub predicate_columns: Vec<String>,
    pub equality_columns: Vec<String>,
    pub range_columns: Vec<String>,
    pub range_shape: String,
    pub order_by_columns: Vec<String>,
    pub group_by_columns: Vec<String>,
    pub join_key_columns: Vec<String>,
    pub projection_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexCostEstimate {
    pub read_benefit: f64,
    pub write_overhead: f64,
    pub compaction_overhead: f64,
    pub net_score: f64,
    pub write_pressure: u64,
    pub key_columns: u64,
    pub include_columns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexSynthesizeResult {
    pub table: BaseTableRef,
    pub suggestions: Vec<AdvisorIndexSuggestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexApplyParams {
    pub table: BaseTableRef,
    pub columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexApplyResult {
    pub accepted: bool,
    pub action_id: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_pct: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexDismissParams {
    pub table: BaseTableRef,
    pub columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexDismissResult {
    pub dismissed: bool,
    pub action_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexRetireUnusedParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<BaseTableRef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_idle_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexRetirementEntry {
    pub suggestion_id: String,
    pub table: BaseTableRef,
    pub columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    pub retired: bool,
    pub reason: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_micros: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorIndexRetireUnusedResult {
    pub dry_run: bool,
    pub evaluated: u64,
    pub retired: u64,
    pub candidates: Vec<AdvisorIndexRetirementEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorEvaluateParams {
    pub table: BaseTableRef,
    pub phases: Vec<AdvisorEvaluatePhase>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_queries: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorEvaluatePhase {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    pub samples: Vec<AdvisorEvaluateSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorEvaluateSample {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub equality_columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub range_columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by_columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by_columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub join_key_columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projection_columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_scanned: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeats: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorEvaluatePhaseResult {
    pub label: String,
    pub observations: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_before: Option<AdvisorIndexSuggestion>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_after: Option<AdvisorIndexSuggestion>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_top_stable_after_observation: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_benchmark: Option<AdvisorEvaluateLatencyReport>,

    pub top_changes: u64,
    pub distinct_top_suggestions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorEvaluateLatencyStats {
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorEvaluateLatencyReport {
    pub benchmarkable_samples: u64,
    pub benchmark_runs: u64,
    pub observed_rows_scanned: u64,
    pub before_rows_scanned: u64,
    pub after_rows_scanned: u64,
    pub before: AdvisorEvaluateLatencyStats,
    pub after: AdvisorEvaluateLatencyStats,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speedup: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorEvaluateResult {
    pub format: String,
    pub table: BaseTableRef,
    pub phase_count: u64,
    pub total_observations: u64,
    pub min_queries: u64,
    pub min_rows: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_top: Option<AdvisorIndexSuggestion>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_top: Option<AdvisorIndexSuggestion>,

    pub phases: Vec<AdvisorEvaluatePhaseResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorHistoryParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<BaseTableRef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorHistoryEntry {
    pub id: String,
    pub suggestion_id: String,
    pub table: BaseTableRef,
    pub columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    pub action: String,
    pub created_at_ms: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_pct: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_status: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_status: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorHistoryResult {
    pub entries: Vec<AdvisorHistoryEntry>,
}

// --------------------------------
// telemetry.* (Phase 11)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryFeatureFlagsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlagEntry {
    pub name: String,
    pub category: String,
    pub hit_count: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryFeatureFlagsResult {
    pub flags: Vec<FeatureFlagEntry>,
    pub total_queries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryCompatSummaryParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatGap {
    pub feature: String,
    pub category: String,
    pub severity: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryCompatSummaryResult {
    pub supported_features: u64,
    pub total_features: u64,
    pub coverage_pct: f64,
    pub gaps: Vec<CompatGap>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryMigrationHintsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationHint {
    pub pattern: String,
    pub mysql_form: String,
    pub skeinql_form: String,
    pub confidence: f64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryMigrationHintsResult {
    pub hints: Vec<MigrationHint>,
}

// --------------------------------
// plan_cache.* (Phase 22)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCacheStatusParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCacheEntry {
    pub fingerprint: String,
    pub normalized_sql: String,
    pub hit_count: u64,
    pub created_ms: u64,
    pub last_hit_ms: u64,
    pub schema_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCacheStatusResult {
    pub entries: Vec<PlanCacheEntry>,
    pub total_hits: u64,
    pub total_misses: u64,
    pub evictions: u64,
    pub capacity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCacheClearParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCacheClearResult {
    pub cleared: u64,
}

// --------------------------------
// stats.coalescing (Phase 16)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsCoalescingParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsCoalescingResult {
    pub total_coalesced: u64,
    pub total_leader: u64,
    pub total_follower: u64,
    pub in_flight_now: u64,
    pub saved_executions: u64,
}

// --------------------------------
// migration.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationIntentSample {
    pub query: Query,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationIntentReportParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<MigrationIntentSample>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationIntentEvidence {
    pub query_index: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<BaseTableRef>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationIntentSuggestion {
    pub intent: String,
    pub confidence: f64,
    pub title: String,
    pub recommendation: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skeinql_snippet: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<MigrationIntentEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationIntentReportResult {
    pub suggestions: Vec<MigrationIntentSuggestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRewritePreviewParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<MigrationIntentSample>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRewritePreview {
    pub intent: String,
    pub confidence: f64,
    pub title: String,
    pub before: String,
    pub after: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<MigrationIntentEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRewritePreviewResult {
    pub rewrites: Vec<MigrationRewritePreview>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReportExportParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<MigrationIntentSample>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReportExportResult {
    pub generated_at_ms: u64,
    pub title: String,
    pub report_json: serde_json::Value,
    pub markdown: String,
}

// --------------------------------
// oblivious.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousPolicy {
    pub level: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pad_to_multiple: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_rows: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dummy_value_lookups: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shuffle: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousPolicySetParams {
    pub table: BaseTableRef,
    pub policy: ObliviousPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousPolicyGetParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<BaseTableRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousPolicyGetResult {
    pub policies: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousExplainParams {
    pub table: BaseTableRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousExplainResult {
    pub plan: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousEvaluateParams {
    pub table: BaseTableRef,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trace_rows: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousTracePoint {
    pub actual_rows: u64,
    pub unpadded_accesses: u64,
    pub target_rows: u64,
    pub dummy_rows: u64,
    pub dummy_value_lookups: u64,
    pub observed_accesses: u64,
    pub overhead_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObliviousEvaluateResult {
    pub table: BaseTableRef,
    pub policy: ObliviousPolicy,
    pub actual_rows: u64,
    pub trace: Vec<ObliviousTracePoint>,
    pub leakage: serde_json::Value,
    pub performance: serde_json::Value,
}

// --------------------------------
// forensic.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForensicQueryParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<BaseTableRef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForensicQueryResult {
    pub records: Vec<serde_json::Value>,
    pub proof: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForensicVerifyParams {
    pub records: Vec<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForensicVerifyResult {
    pub ok: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bad_index: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForensicExportParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<BaseTableRef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForensicExportResult {
    pub bundle: serde_json::Value,
}

// --------------------------------
// edge.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleRedaction {
    /// Redaction mode: "none", "hash_pk", "drop_pk".
    pub mode: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub salt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleWindow {
    pub table: BaseTableRef,

    /// Exclusive lower bound (only records with seq > from_seq are included).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_seq: Option<u64>,

    /// Inclusive upper bound (defaults to latest known seq for the table).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_seq: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleRecord {
    pub seq: u64,
    pub db: String,
    pub table: String,
    pub op: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk: Option<Vec<Lit>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleCoverage {
    pub table: BaseTableRef,
    pub start_seq: u64,
    pub end_seq: u64,
    pub updated_at_ms: u64,
    pub bundle_id: String,
    pub redaction_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundle {
    pub bundle_id: String,
    pub generated_at_ms: u64,
    pub redaction: EdgeBundleRedaction,
    pub coverage: Vec<EdgeBundleCoverage>,
    pub records: Vec<EdgeBundleRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleRequestParams {
    pub windows: Vec<EdgeBundleWindow>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redaction: Option<EdgeBundleRedaction>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleRequestResult {
    pub bundle: EdgeBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleApplyParams {
    pub bundle: EdgeBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleApplyResult {
    pub applied: bool,
    pub coverage: Vec<EdgeBundleCoverage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleRoute {
    pub eligible: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    pub max_lag: u64,
    pub observed_lag: u64,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleStatusParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<Query>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_lag: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBundleStatusResult {
    pub coverage: Vec<EdgeBundleCoverage>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<EdgeBundleRoute>,
}

// --------------------------------
// maintenance.replay.*
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundleTableSchema {
    pub columns: Vec<SchemaColumnInfo>,

    #[serde(default)]
    pub primary_key: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compat_mysql: Option<serde_json::Value>,

    #[serde(default)]
    pub table_version: u64,

    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub auto_inc_next: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundleRowEntry {
    pub row: RowObject,
    pub version: u64,

    #[serde(default)]
    pub schema_version: u64,

    #[serde(default)]
    pub deleted: bool,

    #[serde(default)]
    pub commit_ts_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundleChangeEvent {
    pub seq: u64,
    pub db: String,
    pub table: String,
    pub op: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk: Option<Vec<Lit>>,

    #[serde(default)]
    pub commit_ts_ms: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayBundleTableChecksum {
    pub table: BaseTableRef,
    pub checksum: String,
    pub row_count: u64,
    pub live_row_count: u64,
    pub tombstone_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundleTable {
    pub table: BaseTableRef,
    pub schema: ReplayBundleTableSchema,
    pub rows: Vec<ReplayBundleRowEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceTableState {
    pub table: BaseTableRef,
    pub row_count: u64,
    pub live_row_count: u64,
    pub tombstone_count: u64,
    pub mvcc_versions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceLsmState {
    pub storage_mode: String,
    pub disk_bytes: u64,
    pub wal_bytes: u64,
    pub total_tables: u64,
    pub total_rows: u64,
    pub mvcc_versions: u64,
    pub delta_chains: u64,
    pub tables: Vec<ReplayBundlePerformanceTableState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceHotTable {
    pub table: BaseTableRef,
    pub change_count: u64,
    pub row_count: u64,
    pub live_row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceCacheWarmHints {
    pub cached_select_entries: u64,
    pub cached_patch_entries: u64,
    pub hot_tables: Vec<ReplayBundlePerformanceHotTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceTimingProfile {
    pub change_count: u64,
    pub span_ms: u64,
    pub inter_event_delta_count: u64,
    pub p50_inter_event_ms: u64,
    pub p95_inter_event_ms: u64,
    pub p99_inter_event_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceProfile {
    pub format: String,
    pub captured_at_ms: u64,
    pub lsm_state: ReplayBundlePerformanceLsmState,
    pub cache_warm: ReplayBundlePerformanceCacheWarmHints,
    pub timing: ReplayBundlePerformanceTimingProfile,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceStorageVariance {
    pub disk_bytes_delta: i64,
    pub wal_bytes_delta: i64,
    pub total_tables_delta: i64,
    pub total_rows_delta: i64,
    pub mvcc_versions_delta: i64,
    pub delta_chains_delta: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceCacheVariance {
    pub cached_select_entries_delta: i64,
    pub cached_patch_entries_delta: i64,
    pub hot_table_match_count: u64,
    pub missing_hot_table_count: u64,
    pub extra_hot_table_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceTimingVariance {
    pub change_count_delta: i64,
    pub span_ms_delta: i64,
    pub p50_inter_event_ms_delta: i64,
    pub p95_inter_event_ms_delta: i64,
    pub p99_inter_event_ms_delta: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundlePerformanceRunReport {
    pub format: String,
    pub baseline_checksum: String,
    pub observed_checksum: String,
    pub checksum_match: bool,
    pub storage: ReplayBundlePerformanceStorageVariance,
    pub cache_warm: ReplayBundlePerformanceCacheVariance,
    pub timing: ReplayBundlePerformanceTimingVariance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundleManifest {
    pub format_version: u32,
    pub bundle_id: String,
    pub generated_at_ms: u64,
    pub engine_version: String,
    pub storage_mode: String,
    pub checksum: String,
    pub table_checksums: Vec<ReplayBundleTableChecksum>,
    pub table_count: u64,
    pub row_count: u64,
    pub live_row_count: u64,
    pub tombstone_count: u64,
    pub change_count: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_lsn: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_lsn: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_commit_ts_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_commit_ts_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundle {
    pub manifest: ReplayBundleManifest,
    pub tables: Vec<ReplayBundleTable>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<ReplayBundleChangeEvent>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redaction: Option<EdgeBundleRedaction>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performance: Option<ReplayBundlePerformanceProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReplayExportParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_lsn: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_lsn: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redaction: Option<EdgeBundleRedaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReplayExportResult {
    pub bundle: ReplayBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReplayImportParams {
    pub bundle: ReplayBundle,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReplayImportResult {
    pub ok: bool,
    pub workspace_id: String,
    pub bundle_id: String,
    pub workspace_path: String,
    pub checksum: String,
    pub imported_tables: u64,
    pub imported_rows: u64,
    pub imported_changes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReplayRunParams {
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReplayRunResult {
    pub ok: bool,
    pub workspace_id: String,
    pub bundle_id: String,
    pub workspace_path: String,
    pub expected_checksum: String,
    pub observed_checksum: String,
    pub table_checksums: Vec<ReplayBundleTableChecksum>,
    pub replayed_tables: u64,
    pub replayed_rows: u64,
    pub replayed_changes: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performance_report: Option<ReplayBundlePerformanceRunReport>,
}

// --------------------------------
// merge.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeFunctionRef {
    pub kind: String,
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergePolicySpec {
    pub default: MergeFunctionRef,

    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub per_column: std::collections::HashMap<String, MergeFunctionRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRegisterParams {
    pub table: BaseTableRef,
    pub policy: MergePolicySpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRegisterResult {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeApplyParams {
    pub table: BaseTableRef,
    pub pk: Vec<Lit>,
    pub incoming: RowObject,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_etag: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_causality: Option<CausalityToken>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<MergePolicySpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeApplyResult {
    pub applied: bool,
    pub conflict: bool,
    pub merged: RowObject,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeSimulateParams {
    pub current: RowObject,
    pub incoming: RowObject,
    pub policy: MergePolicySpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeSimulateResult {
    pub merged: RowObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeEvaluateCase {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    pub current: RowObject,
    pub incoming: RowObject,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_etag_match: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_causality_satisfied: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraint_ok: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeEvaluateParams {
    pub policy: MergePolicySpec,
    pub cases: Vec<MergeEvaluateCase>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeEvaluateCaseResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    pub conflict: bool,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,

    pub resolved: bool,
    pub merged: RowObject,
    pub mean_merge_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeEvaluateResult {
    pub format: String,
    pub cases: u64,
    pub iterations: u64,
    pub conflict_count: u64,
    pub resolved_count: u64,
    pub conflict_rate: f64,
    pub resolution_success_rate: f64,
    pub mean_merge_ns: u64,
    pub p95_merge_ns: u64,
    pub results: Vec<MergeEvaluateCaseResult>,
}

// --------------------------------
// merge.wasm.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWasmCapabilities {
    pub values_only: bool,

    #[serde(default)]
    pub deterministic: bool,

    #[serde(default)]
    pub max_fuel: u64,

    #[serde(default)]
    pub max_memory_bytes: u64,

    #[serde(default)]
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWasmRegisterParams {
    pub module_id: String,
    pub wasm_b64: String,
    pub capabilities: MergeWasmCapabilities,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overwrite: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWasmRegisterResult {
    pub ok: bool,
    pub value_id: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWasmModuleInfo {
    pub module_id: String,
    pub value_id: String,
    pub size_bytes: u64,
    pub capabilities: MergeWasmCapabilities,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWasmListResult {
    pub modules: Vec<MergeWasmModuleInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWasmDropParams {
    pub module_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWasmDropResult {
    pub ok: bool,
}

// --------------------------------
// wasm.plan.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanCompileParams {
    pub query: Query,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abi: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanCompileResult {
    pub format: String,
    pub abi: String,
    pub artifact_b64: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,

    pub execution: String,
    pub artifact_bytes: usize,
    pub operator_count: usize,
    pub operators: Vec<String>,
    pub supports_edge_package: bool,
    pub supports_simd: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanInspectParams {
    pub artifact_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanInspectResult {
    pub format: String,
    pub abi: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,

    pub execution: String,
    pub artifact_bytes: usize,
    pub operator_count: usize,
    pub operators: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<BaseTableRef>,

    pub has_filter: bool,
    pub projection_count: usize,
    pub supports_edge_package: bool,
    pub supports_simd: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanPerfReportParams {
    pub artifact_b64: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup_iterations: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanPerfLatencyStats {
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanPerfRunStats {
    pub rows: usize,
    pub columns: usize,
    pub latency: WasmPlanPerfLatencyStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanSimdExploration {
    pub candidate: bool,
    pub enabled: bool,
    pub strategy: String,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanPerfReportResult {
    pub format: String,
    pub execution: String,
    pub iterations: u32,
    pub warmup_iterations: u32,
    pub operator_count: usize,
    pub operators: Vec<String>,
    pub outputs_match: bool,
    pub simd: WasmPlanSimdExploration,
    pub host: WasmPlanPerfRunStats,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated: Option<WasmPlanPerfRunStats>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_speedup_vs_host: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanEdgePackageParams {
    pub artifact_b64: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanEdgePackageResult {
    pub format: String,
    pub package_name: String,
    pub artifact_b64: String,
    pub artifact_bytes: usize,
    pub artifact_sha256: String,
    pub manifest_json: String,
    pub runner_js: String,
    pub instructions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPlanRunParams {
    pub artifact_b64: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_format: Option<ResultFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<QueryCache>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire: Option<WireHints>,
}

pub type WasmPlanRunResult = QueryExecResult;

// --------------------------------
// view.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewCreateParams {
    pub view: BaseTableRef,
    pub query: Query,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_not_exists: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewCreateResult {
    pub ok: bool,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDropParams {
    pub view: BaseTableRef,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_exists: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDropResult {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRefreshParams {
    pub view: BaseTableRef,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRefreshResult {
    pub mode: String,
    pub rows: u64,
    pub last_change_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewEvaluateParams {
    pub view: BaseTableRef,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterations: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewEvaluateResult {
    pub format: String,
    pub view: BaseTableRef,
    pub iterations: u64,
    pub pending_changes: u64,
    pub touched_pks: u64,
    pub stale_before: bool,
    pub incremental_ok: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incremental_error: Option<String>,

    pub correct: bool,
    pub rows_incremental: u64,
    pub rows_full: u64,
    pub mean_incremental_ns: u64,
    pub mean_full_ns: u64,
    pub speedup_vs_full: f64,
    pub recommended_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewStatusParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<BaseTableRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewStatusResult {
    pub views: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewExplainDepsParams {
    pub view: BaseTableRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewExplainDepsResult {
    pub deps: Vec<serde_json::Value>,
}

// --------------------------------
// cluster.* (research / experimental)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatusResult {
    pub enabled: bool,
    pub cluster_id: String,
    pub local_node_id: String,
    pub primary_node_id: String,
    pub local_role: String,
    pub nodes: Vec<ClusterNodeInfo>,
    pub shards: Vec<ClusterShardInfo>,
    pub replication: ClusterReplicationInfo,
    pub methods: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodesParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodesResult {
    pub nodes: Vec<ClusterNodeInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    pub node_id: String,
    pub rpc_url: String,
    pub role: String,
    pub status: String,
    pub joined_at_ms: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterJoinTokenCreateParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterJoinTokenCreateResult {
    pub token: String,
    pub expires_at_ms: u64,
    pub role: String,
    pub max_uses: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeJoinParams {
    pub token: String,
    pub node_id: String,
    pub rpc_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeJoinResult {
    pub ok: bool,
    pub cluster_id: String,
    pub node: ClusterNodeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeRemoveParams {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeRemoveResult {
    pub ok: bool,
    pub removed: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_primary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeLeaveParams {
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeLeaveResult {
    pub ok: bool,
    pub node_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_primary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterReplicaPromoteParams {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterReplicaPromoteResult {
    pub ok: bool,
    pub primary_node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardCreateParams {
    pub db: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardMoveParams {
    pub shard_id: String,
    pub to_node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardRebalanceParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_moves: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardInfo {
    pub shard_id: String,
    pub db: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub primary_node_id: String,
    #[serde(default)]
    pub replicas: Vec<String>,
    pub slots: u32,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardCreateResult {
    pub ok: bool,
    pub shard: ClusterShardInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardMoveResult {
    pub ok: bool,
    pub dry_run: bool,
    pub shard: ClusterShardInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ClusterShardManifestInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<ClusterShardMoveProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardRebalanceResult {
    pub ok: bool,
    pub dry_run: bool,
    pub moves: Vec<ClusterShardMovePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardMovePlan {
    pub shard_id: String,
    pub from_node_id: String,
    pub to_node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ClusterShardManifestInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<ClusterShardMoveProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardManifestInfo {
    pub object_count: u64,
    pub total_bytes: u64,
    pub missing_object_count: u64,
    pub missing_bytes: u64,
    pub already_present_object_count: u64,
    pub already_present_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterShardMoveProgress {
    pub stage: String,
    pub transfer_required: bool,
    pub source_node_id: String,
    pub destination_node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_rpc_url: Option<String>,
    pub requested_object_count: u64,
    pub pulled_object_count: u64,
    pub stored_object_count: u64,
    pub batches: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub invalid_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remote_missing: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verification_failed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterReplicationInfo {
    pub shipped_ops: u64,
    pub applied_ops: u64,
    pub failed_ops: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub last_updated_ms: u64,
}

// --------------------------------
// objects.* (CAS-aware replication)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPullParams {
    pub source_rpc_url: String,
    pub ids: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPullResult {
    pub ok: bool,
    pub source_rpc_url: String,
    pub requested: usize,
    pub batch_size: usize,
    pub batches: usize,
    pub already_present: usize,
    pub fetched_objects: usize,
    pub stored: usize,
    #[serde(default)]
    pub invalid_ids: Vec<String>,
    #[serde(default)]
    pub remote_missing: Vec<String>,
    #[serde(default)]
    pub verification_failed: Vec<String>,
}

// --------------------------------
// cdc.* (selected)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CdcPrimaryKeyRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<Lit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSubscribeTableParams {
    pub db: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pk: Vec<Lit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk_range: Option<CdcPrimaryKeyRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ops: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSubscribeQueryParams {
    pub query_id: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pk: Vec<Lit>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk_range: Option<CdcPrimaryKeyRange>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include: Option<serde_json::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ops: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSubscribeResult {
    pub sub_id: String,
    pub offset: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws_url: Option<String>,
}

pub type CdcRowObject = std::collections::BTreeMap<String, serde_json::Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    pub seq: u64,
    pub db: String,
    pub table: String,
    pub op: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk: Option<Vec<serde_json::Value>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<CdcRowObject>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<CdcRowObject>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,

    #[serde(default)]
    pub commit_ts_ms: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcPollParams {
    pub sub_id: String,
    pub from_offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcAckParams {
    pub sub_id: String,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcPauseParams {
    pub sub_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcResumeParams {
    pub sub_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcBackpressureStatus {
    pub state: String,

    #[serde(default)]
    pub lag: u64,

    #[serde(default)]
    pub remaining_until_resnapshot: u64,

    #[serde(default)]
    pub paused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcPollResult {
    pub events: Vec<CdcEvent>,
    pub next_offset: u64,

    #[serde(default)]
    pub earliest_offset: u64,

    #[serde(default)]
    pub latest_offset: u64,

    #[serde(default)]
    pub resnapshot_required: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resnapshot_from_offset: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resnapshot_reason: Option<String>,

    #[serde(default)]
    pub backpressure: CdcBackpressureStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcAckResult {
    pub sub_id: String,
    pub acked_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSubscriptionControlResult {
    pub sub_id: String,
    pub paused: bool,
    pub acked_offset: u64,

    #[serde(default)]
    pub backpressure: CdcBackpressureStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcCloseParams {
    pub sub_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcCloseResult {
    pub sub_id: String,
    pub closed: bool,
}

// --------------------------------
// stats.* (selected)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSnapshotResult {
    pub uptime_s: u64,
    pub sessions: SessionsStats,
    pub qps: f64,
    pub tps: f64,
    pub process: ProcessStats,
    pub storage: StorageStats,
    pub background: BackgroundStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionsStats {
    pub active: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessStats {
    pub cpu_pct: f64,
    pub rss_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub wal_bytes: u64,
    pub dedup_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundStats {
    pub compaction: String,
    pub snapshots: String,
}

// --------------------------------
// settings.encryption.* (T193)
// --------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SettingsEncryptionStatusParams {
    /// Optional database filter; when omitted, all known databases are returned.
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionDatabaseStatus {
    pub db: String,
    pub mode: String,
    pub active_key_id: Option<String>,
    pub registered_key_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionStatusResult {
    pub databases: Vec<SettingsEncryptionDatabaseStatus>,
    /// Recent audit entries (most recent first), capped to a small window.
    pub recent_audit: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionSetModeParams {
    pub db: String,
    /// One of `off`, `enc_random`, `enc_mle_db`.
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionSetModeResult {
    pub ok: bool,
    pub db: String,
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionRegisterKeyParams {
    pub db: String,
    pub key_id: String,
    /// 32-byte master key, base64-encoded (URL-safe or standard).
    pub master_key_b64: String,
    /// When true, also marks this key as the active key for new writes.
    #[serde(default)]
    pub make_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionRegisterKeyResult {
    pub ok: bool,
    pub db: String,
    pub key_id: String,
    pub active_key_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionSetActiveKeyParams {
    pub db: String,
    pub key_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionSetActiveKeyResult {
    pub ok: bool,
    pub db: String,
    pub active_key_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionRotateKeyParams {
    pub db: String,
    pub new_key_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsEncryptionRotateKeyResult {
    pub ok: bool,
    pub db: String,
    pub previous_key_id: Option<String>,
    pub new_key_id: String,
    pub mode: String,
}
