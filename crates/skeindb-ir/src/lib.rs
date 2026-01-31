//! SkeinIR v1 - Rust types (scaffold)
//!
//! SkeinIR is the canonical intermediate representation used by SkeinDB.
//! All frontends (MySQL SQL, SkeinQL, web console) should translate into SkeinIR.
//!
//! The reference specification is docs/SKEINIR.md.

use serde::{Deserialize, Serialize};

pub type DbId = u32;
pub type TableId = u32;
pub type ColId = u32;
pub type IndexId = u32;
pub type PlanId = u32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ident {
    pub original: String,
    pub normalized: String,
}

impl Ident {
    pub fn new<S: Into<String>>(s: S) -> Self {
        let original = s.into();
        let normalized = original.to_lowercase();
        Self {
            original,
            normalized,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName {
    pub parts: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Decimal { precision: u16, scale: u16 },
    Varchar { len: u32 },
    Text,
    Longtext,
    Binary { len: u32 },
    Varbinary { len: u32 },
    Blob,
    Longblob,
    Json,
    Date,
    Time { fsp: u8 },
    Datetime { fsp: u8 },
    Timestamp { fsp: u8 },
    Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnType {
    pub ty: DataType,
    pub nullable: bool,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub unsigned: bool,
    pub zerofill: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub col_id: ColId,
    pub name: Ident,
    pub col_type: ColumnType,
    pub default_expr: Option<Expr>,
    pub auto_increment: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexType {
    Btree,
    Hash,
    Fulltext,
    Spatial,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexCol {
    pub col_id: ColId,
    pub prefix_len: Option<u32>,
    pub asc: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDef {
    pub index_id: IndexId,
    pub name: Ident,
    pub unique: bool,
    pub index_type: IndexType,
    pub columns: Vec<IndexCol>,
    pub comment: Option<String>,
    pub visible: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
    pub table_id: TableId,
    pub db_id: DbId,
    pub name: Ident,
    pub schema_ver: u32,

    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColId>,
    pub indexes: Vec<IndexDef>,

    // MySQL compatibility surface
    pub mysql_engine: Option<String>,
    pub default_charset: Option<String>,
    pub default_collation: Option<String>,
}

// -------------------------
// Expressions
// -------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Literal {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Decimal(String),
    String(String),
    Bytes(Vec<u8>),
    Json(String),
    Date { y: u16, m: u8, d: u8 },
    Time { micros: i64 },
    Datetime { micros: i64 },
    Timestamp { micros: i64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOp {
    Not,
    Negate,
    BitNot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    BitAnd,
    BitOr,
    BitXor,
    Shl,
    Shr,
    Concat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table_alias: Option<Ident>,
    pub col_name: Ident,
    pub col_id: Option<ColId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Expr {
    Lit(Literal),
    Col(ColumnRef),
    Param {
        index: u32,
    },

    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    Func {
        name: Ident,
        args: Vec<Expr>,
        distinct: bool,
    },

    Cast {
        expr: Box<Expr>,
        to_type: DataType,
    },

    Case {
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },

    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },

    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },

    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    Subquery {
        query: Box<Query>,
    },
    Exists {
        query: Box<Query>,
        negated: bool,
    },
}

// -------------------------
// Query IR
// -------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderKey {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockMode {
    None,
    ForUpdate,
    ForShare,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitClause {
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseTableRef {
    pub name: ObjectName,
    pub alias: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinRef {
    pub join_type: JoinType,
    pub left: Box<TableRef>,
    pub right: Box<TableRef>,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubqueryRef {
    pub query: Box<Query>,
    pub alias: Ident,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableRef {
    Base(BaseTableRef),
    Join(JoinRef),
    Subquery(SubqueryRef),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Select {
    pub distinct: bool,
    pub calc_found_rows: bool,
    pub projection: Vec<SelectItem>,
    pub from: Vec<TableRef>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetOp {
    pub kind: SetOpKind,
    pub all: bool,
    pub left: Box<QueryBody>,
    pub right: Box<QueryBody>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryBody {
    Select(Box<Select>),
    Setop(SetOp),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    pub name: Ident,
    pub query: Query,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    #[serde(default)]
    pub with: Vec<Cte>,
    pub body: QueryBody,
    #[serde(default)]
    pub order_by: Vec<OrderKey>,
    pub limit: Option<LimitClause>,
    pub lock: LockMode,
}

// -------------------------
// Statement IR
// -------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxnStmt {
    Begin,
    Commit,
    Rollback,
    SetAutocommit { on: bool },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: Ident,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStmt {
    pub table: ObjectName,
    pub columns: Option<Vec<Ident>>,
    pub values: Vec<Vec<Expr>>,
    pub on_duplicate_key: Option<Vec<Assignment>>,
    pub ignore: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStmt {
    pub table: ObjectName,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStmt {
    pub table: ObjectName,
    pub where_clause: Option<Expr>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStmt {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<Vec<Ident>>,
    pub indexes: Vec<IndexDef>,
    pub options: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlterOp {
    AddColumn(ColumnDef),
    DropColumn { name: Ident },
    AddIndex(IndexDef),
    DropIndex { name: Ident },
    RenameTo { new_name: ObjectName },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStmt {
    pub name: ObjectName,
    pub ops: Vec<AlterOp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShowKind {
    Databases,
    Tables {
        db: Option<Ident>,
        full: bool,
    },
    Columns {
        table: ObjectName,
        full: bool,
    },
    Index {
        table: ObjectName,
    },
    CreateTable {
        table: ObjectName,
    },
    Variables {
        scope: Option<String>,
        like: Option<String>,
    },
    Status {
        scope: Option<String>,
        like: Option<String>,
    },
    TableStatus {
        db: Option<Ident>,
        like: Option<String>,
    },
    Engines,
    Grants {
        user: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShowStmt {
    pub kind: ShowKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetScope {
    Session,
    Global,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetVarStmt {
    pub scope: SetScope,
    pub assignments: std::collections::BTreeMap<String, Expr>,
    pub set_names: Option<String>,
    pub set_charset: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UseDbStmt {
    pub db: Ident,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateUserStmt {
    pub user: String,
    pub auth_plugin: Option<String>,
    pub auth_data: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropUserStmt {
    pub user: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GrantStmt {
    pub privileges: Vec<String>,
    pub on: ObjectName,
    pub to_user: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RevokeStmt {
    pub privileges: Vec<String>,
    pub on: ObjectName,
    pub from_user: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainStmt {
    pub stmt: Box<Statement>,
    pub format: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Statement {
    Txn(TxnStmt),
    UseDb(UseDbStmt),
    Set(SetVarStmt),

    Query(Box<Query>),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),

    CreateTable(CreateTableStmt),
    AlterTable(AlterTableStmt),
    DropTable { name: ObjectName, if_exists: bool },

    CreateUser(CreateUserStmt),
    DropUser(DropUserStmt),
    Grant(GrantStmt),
    Revoke(RevokeStmt),

    Show(ShowStmt),
    Explain(ExplainStmt),
}

// -------------------------
// Plan IR (v1)
// -------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessPath {
    SeqScan,
    PkLookup,
    IndexRange { index_id: IndexId },
    IndexFull { index_id: IndexId },
    ColumnSnapshot { snapshot_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanNode {
    pub table_id: TableId,
    pub alias: Option<Ident>,
    pub projection: Option<Vec<ColId>>,
    pub predicate: Option<Expr>,
    pub access_path: AccessPath,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValuesNode {
    pub rows: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterNode {
    pub input: PlanId,
    pub predicate: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectNode {
    pub input: PlanId,
    pub items: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinAlg {
    NestedLoop,
    HashJoin,
    MergeJoin,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinNode {
    pub join_type: JoinType,
    pub left: PlanId,
    pub right: PlanId,
    pub on: Expr,
    pub algorithm: JoinAlg,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggFunc {
    pub name: Ident,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub alias: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateNode {
    pub input: PlanId,
    pub group_by: Vec<Expr>,
    pub aggs: Vec<AggFunc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortNode {
    pub input: PlanId,
    pub keys: Vec<OrderKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitNode {
    pub input: PlanId,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertExec {
    pub table_id: TableId,
    pub input: PlanId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateExec {
    pub table_id: TableId,
    pub assignments: Vec<Assignment>,
    pub input: PlanId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteExec {
    pub table_id: TableId,
    pub input: PlanId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanNode {
    Scan(ScanNode),
    Values(ValuesNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Join(JoinNode),
    Agg(AggregateNode),
    Sort(SortNode),
    Limit(LimitNode),

    Insert(InsertExec),
    Update(UpdateExec),
    Delete(DeleteExec),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanGraph {
    pub nodes: std::collections::BTreeMap<PlanId, PlanNode>,
    pub root: PlanId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ident_roundtrip_json() {
        let id = Ident::new("Wp_Options");
        let json = serde_json::to_string(&id).unwrap();
        let back: Ident = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
        assert_eq!(back.normalized, "wp_options");
    }

    #[test]
    fn query_roundtrip_json() {
        let q = Query {
            with: vec![],
            body: QueryBody::Select(Box::new(Select {
                distinct: false,
                calc_found_rows: true,
                projection: vec![SelectItem {
                    expr: Expr::Col(ColumnRef {
                        table_alias: None,
                        col_name: Ident::new("id"),
                        col_id: None,
                    }),
                    alias: None,
                }],
                from: vec![TableRef::Base(BaseTableRef {
                    name: ObjectName {
                        parts: vec![Ident::new("t")],
                    },
                    alias: None,
                })],
                where_clause: Some(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Col(ColumnRef {
                        table_alias: None,
                        col_name: Ident::new("status"),
                        col_id: None,
                    })),
                    right: Box::new(Expr::Lit(Literal::String("publish".to_string()))),
                }),
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: Some(LimitClause {
                limit: Some(10),
                offset: Some(0),
            }),
            lock: LockMode::None,
        };

        let json = serde_json::to_string(&q).unwrap();
        let back: Query = serde_json::from_str(&json).unwrap();
        assert_eq!(q, back);
    }

    #[test]
    fn statement_query_roundtrip_json() {
        let q = Query {
            with: vec![],
            body: QueryBody::Select(Box::new(Select {
                distinct: false,
                calc_found_rows: false,
                projection: vec![SelectItem {
                    expr: Expr::Lit(Literal::I64(1)),
                    alias: Some(Ident::new("one")),
                }],
                from: vec![TableRef::Base(BaseTableRef {
                    name: ObjectName {
                        parts: vec![Ident::new("t")],
                    },
                    alias: Some(Ident::new("t")),
                })],
                where_clause: None,
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            lock: LockMode::None,
        };

        let stmt = Statement::Explain(ExplainStmt {
            stmt: Box::new(Statement::Query(Box::new(q))),
            format: Some("text".to_string()),
        });

        let json = serde_json::to_string(&stmt).unwrap();
        let back: Statement = serde_json::from_str(&json).unwrap();
        assert_eq!(stmt, back);
    }
}
