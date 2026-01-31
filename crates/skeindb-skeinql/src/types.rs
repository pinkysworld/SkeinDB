//! Core SkeinQL v1.0 types.
//!
//! The protocol spec is defined in `docs/SKEINQL.md`.
//! These types are intended for both client SDKs and server implementations.

use serde::{Deserialize, Serialize};

/// A type descriptor used for schema and result metadata.
///
/// SkeinQL keeps this intentionally simple in v1; implementations may add
/// additional fields for compatibility (charset/collation, unsigned, etc.).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeDesc {
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<u16>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<u16>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsigned: Option<bool>,
}

/// Typed literal value.
///
/// JSON shape examples:
/// - {"t":"u64","v":1}
/// - {"t":"bytes","b64":"AAECAw=="}
/// - {"t":"datetime","iso":"2020-01-01T00:00:00Z"}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Lit {
    Null,
    Bool {
        v: bool,
    },
    I64 {
        v: i64,
    },
    U64 {
        v: u64,
    },
    F64 {
        v: f64,
    },

    /// Decimal encoded as a string to avoid JSON float rounding.
    Dec {
        v: String,
    },

    Str {
        v: String,
    },

    /// Base64-encoded bytes.
    Bytes {
        b64: String,
    },

    /// Embedded JSON (object/array/number/string/etc.).
    Json {
        v: serde_json::Value,
    },

    /// Embedding vector (float32).
    Embedding {
        /// Declared dimensions (must match `v.len()`).
        dims: u32,
        /// Vector values.
        v: Vec<f32>,
        /// Optional model identifier.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
    },

    /// ISO-8601 date.
    Date {
        iso: String,
    },

    /// ISO-8601 time.
    Time {
        iso: String,
    },

    /// ISO-8601 UTC datetime.
    Datetime {
        iso: String,
    },

    /// UUID string.
    Uuid {
        v: String,
    },
}

/// Query execution result format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultFormat {
    RowsJson,
    ObjectsJson,

    /// Optional: dictionary-transfer encoding (see docs/TRAFFIC_REDUCTION.md).
    #[serde(rename = "skeinpack_v1")]
    SkeinpackV1,

    /// Experimental: columnar batch encoding (see docs/WASM_OPERATORS.md).
    #[serde(rename = "wasm_batch_v1")]
    WasmBatchV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBy {
    pub expr: Expr,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<OrderDir>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderDir {
    Asc,
    Desc,
}

/// Causal dependency describing a table or view version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CausalityDependency {
    pub table: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub v: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_seq: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale: Option<bool>,
}

/// Causality token for session-consistent reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CausalityToken {
    pub format: String,
    pub deps: Vec<CausalityDependency>,
}

/// Cache validation hints for ETag-aware queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryCache {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub want_etag: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub if_none_match: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_causality: Option<CausalityToken>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persist: Option<bool>,
}

/// Optional wire-format hints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireHints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Optional client-side value dictionary summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub known_valueids: Option<serde_json::Value>,
}

// -----------------------------
// Expressions
// -----------------------------

/// An expression.
///
/// SkeinQL uses a compact JSON object representation rather than SQL text.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Expr {
    /// Column reference: {"col":"name"} or {"col":"id","table":"u"}
    Col {
        col: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        table: Option<String>,
    },

    /// Typed literal: {"lit": {"t":"u64","v":1}}
    Lit { lit: Lit },

    /// Parameter placeholder: {"param":0}
    Param { param: u32 },

    /// Operator invocation.
    ///
    /// Supports unary/binary (`a`/`b`) and variadic (`args`) forms.
    Op {
        op: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        a: Option<Box<Expr>>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        b: Option<Box<Expr>>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        args: Option<Vec<Expr>>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        list: Option<Vec<Expr>>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        lo: Option<Box<Expr>>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        hi: Option<Box<Expr>>,
    },

    /// Function call: {"fn":"lower","args":[...]}
    Func {
        #[serde(rename = "fn")]
        name: String,
        #[serde(default)]
        args: Vec<Expr>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        distinct: Option<bool>,
    },

    /// Cast: {"cast":{"expr":...,"to":{...}}}
    Cast { cast: CastExpr },

    /// Case: {"case": {"when":[...],"else":...}}
    #[serde(rename_all = "lowercase")]
    Case {
        #[serde(rename = "case")]
        case_: CaseExpr,
    },

    /// Scalar subquery: {"subquery":{"query":{...}}}
    Subquery { subquery: SubqueryExpr },

    /// Exists: {"exists":{"query":{...},"negated":false}}
    Exists { exists: ExistsExpr },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CastExpr {
    pub expr: Box<Expr>,
    pub to: TypeDesc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseExpr {
    #[serde(default)]
    pub when: Vec<CaseWhen>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#else: Option<Box<Expr>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseWhen {
    #[serde(rename = "if")]
    pub r#if: Expr,
    pub then: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubqueryExpr {
    pub query: Query,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExistsExpr {
    pub query: Query,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub negated: Option<bool>,
}

// -----------------------------
// Query AST
// -----------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    pub name: String,
    pub query: Query,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitClause {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockMode {
    None,
    ForUpdate,
    ForShare,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub with: Vec<Cte>,

    pub body: Box<QueryBody>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<OrderBy>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<LimitClause>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock: Option<LockMode>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryBody {
    Select { select: Box<SelectBody> },
    Setop { setop: SetOp },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectItem {
    pub expr: Expr,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#as: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct: Option<bool>,

    pub projection: Vec<SelectItem>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<Vec<TableRef>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#where: Option<Expr>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_by: Option<Vec<Expr>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetOp {
    pub kind: SetOpKind,

    #[serde(default)]
    pub all: bool,

    pub left: Box<QueryBody>,
    pub right: Box<QueryBody>,
}

// -----------------------------
// Table references
// -----------------------------

/// Table reference for FROM clauses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TableRef {
    Base(BaseTableRef),
    Join(JoinTableRef),
    Subquery(SubqueryTableRef),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseTableRef {
    pub db: String,
    pub table: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#as: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinTableRef {
    pub join: JoinRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubqueryTableRef {
    pub subquery: SubqueryRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinRef {
    #[serde(rename = "type")]
    pub join_type: JoinType,
    pub left: Box<TableRef>,
    pub right: Box<TableRef>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubqueryRef {
    pub query: Query,
    pub r#as: String,
}

// -----------------------------
// Result metadata
// -----------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub r#type: TypeDesc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    pub id: String,
}
