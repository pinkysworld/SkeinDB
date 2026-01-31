# SkeinIR v1 (Canonical IR)

Status: Draft v1.0 (stable enough to implement)
Last updated: 2026-01-17

SkeinIR is the *canonical* intermediate representation used by SkeinDB.
All external frontends (MySQL SQL text, SkeinQL HTTP API, web console) MUST translate into SkeinIR.

Goals:
- Provide a stable target for multiple frontends (MySQL today, others later).
- Normalize “SQL AST weirdness” into engine-friendly relational operators.
- Make behavior explicit (compat flags, session-dependent functions).
- Keep schema/catalog and statement types in one place.

---

## 1. Invariants

1) Identifiers:
- Store both `original` and `normalized` forms for MySQL compatibility.
- Storage uses numeric IDs (db_id/table_id/col_id/index_id) after binding.

2) Binding:
- Translators may emit unbound ColumnRefs (by name/alias).
- Binder resolves names to IDs and populates `col_id` where possible.
- Planner/executor should not rely on unresolved names except for error messages.

3) Determinism:
- Expression evaluation must be deterministic under a given session state.
- “Compat functions” like FOUND_ROWS() and LAST_INSERT_ID() read session state.

4) MySQL compatibility knobs:
- `calc_found_rows` must be preserved from translation to execution.

---

## 2. Identifiers

```text
struct Ident {
  original: string
  normalized: string
}

struct ObjectName {
  parts: list<Ident>  // 1..3 components
}
```

---

## 3. Types

```text
enum DataType {
  BOOL,
  I8, I16, I32, I64,
  U8, U16, U32, U64,
  F32, F64,
  DECIMAL { precision: u16, scale: u16 },
  VARCHAR { len: u32 },
  TEXT, LONGTEXT,
  BINARY { len: u32 },
  VARBINARY { len: u32 },
  BLOB, LONGBLOB,
  JSON,
  DATE,
  TIME { fsp: u8 },
  DATETIME { fsp: u8 },
  TIMESTAMP { fsp: u8 },
  UUID
}

struct ColumnType {
  ty: DataType
  nullable: bool
  charset: optional<string>
  collation: optional<string>
  unsigned: bool
  zerofill: bool
}
```

---

## 4. Catalog objects

```text
type DbId = u32
type TableId = u32
type ColId = u32
type IndexId = u32

struct ColumnDef {
  col_id: ColId
  name: Ident
  col_type: ColumnType
  default_expr: optional<Expr>
  auto_increment: bool
  comment: optional<string>
}

enum IndexType { BTREE, HASH, FULLTEXT, SPATIAL }

struct IndexCol {
  col_id: ColId
  prefix_len: optional<u32>
  asc: bool
}

struct IndexDef {
  index_id: IndexId
  name: Ident
  unique: bool
  index_type: IndexType
  columns: list<IndexCol>
  comment: optional<string>
  visible: bool
}

struct TableDef {
  table_id: TableId
  db_id: DbId
  name: Ident
  schema_ver: u32

  columns: list<ColumnDef>
  primary_key: list<ColId>
  indexes: list<IndexDef>

  mysql_engine: optional<string>
  default_charset: optional<string>
  default_collation: optional<string>
}
```

---

## 5. Expressions

```text
enum Literal {
  NULL,
  BOOL(bool),
  I64(i64),
  U64(u64),
  F64(f64),
  DECIMAL(string),
  STRING(string),
  BYTES(bytes),
  JSON(string),
  DATE { y:u16, m:u8, d:u8 },
  TIME { micros:i64 },
  DATETIME { micros:i64 },
  TIMESTAMP { micros:i64 }
}

enum UnaryOp { NOT, NEGATE, BIT_NOT }

enum BinaryOp {
  ADD, SUB, MUL, DIV, MOD,
  EQ, NE, LT, LE, GT, GE,
  AND, OR,
  BIT_AND, BIT_OR, BIT_XOR,
  SHL, SHR,
  CONCAT
}

struct ColumnRef {
  table_alias: optional<Ident>
  col_name: Ident
  col_id: optional<ColId>
}

enum Expr {
  LIT(Literal),
  COL(ColumnRef),
  PARAM { index: u32 },

  UNARY { op: UnaryOp, expr: Box<Expr> },
  BINARY { op: BinaryOp, left: Box<Expr>, right: Box<Expr> },

  FUNC { name: Ident, args: list<Expr>, distinct: bool },

  CAST { expr: Box<Expr>, to_type: DataType },

  CASE { when_then: list<(Expr, Expr)>, else_expr: optional<Box<Expr>> },

  IS_NULL { expr: Box<Expr>, negated: bool },

  LIKE { expr: Box<Expr>, pattern: Box<Expr>, escape: optional<Box<Expr>>, negated: bool },

  IN_LIST { expr: Box<Expr>, list: list<Expr>, negated: bool },

  SUBQUERY { query: Box<Query> },
  EXISTS { query: Box<Query>, negated: bool }
}
```

---

## 6. Query IR

```text
enum JoinType { INNER, LEFT, RIGHT, FULL, CROSS }

struct OrderKey { expr: Expr, asc: bool, nulls_first: optional<bool> }

enum LockMode { NONE, FOR_UPDATE, FOR_SHARE }

struct LimitClause { limit: optional<u64>, offset: optional<u64> }

struct SelectItem { expr: Expr, alias: optional<Ident> }

struct BaseTableRef { name: ObjectName, alias: optional<Ident> }

struct JoinRef {
  join_type: JoinType
  left: Box<TableRef>
  right: Box<TableRef>
  on: optional<Expr>
}

struct SubqueryRef { query: Box<Query>, alias: Ident }

struct TableRef {
  base_table: optional<BaseTableRef>
  join: optional<JoinRef>
  subquery: optional<SubqueryRef>
}

struct Select {
  distinct: bool
  calc_found_rows: bool
  projection: list<SelectItem>
  from: list<TableRef>
  where: optional<Expr>
  group_by: list<Expr>
  having: optional<Expr>
}

enum SetOpKind { UNION, INTERSECT, EXCEPT }

struct SetOp {
  kind: SetOpKind
  all: bool
  left: Box<QueryBody>
  right: Box<QueryBody>
}

enum QueryBody { SELECT(Select), SETOP(SetOp) }

struct Cte { name: Ident, query: Query }

struct Query {
  with: list<Cte>
  body: QueryBody
  order_by: list<OrderKey>
  limit: optional<LimitClause>
  lock: LockMode
}
```

---

## 7. Plan IR (v1)

See docs for the full PlanGraph shape.
### 7.1 AccessPath

Scan nodes may specify an access_path hint. v1 supports:
- SeqScan
- PkLookup
- IndexRange { index_id }
- IndexFull { index_id }
- ColumnSnapshot { snapshot_id }  (optional, for hybrid row+column snapshots)


---

## 8. Versioning

- Minor additions are allowed in v1 if they have sensible defaults.
- Breaking changes require SkeinIR v2 and dual-support in translators.
