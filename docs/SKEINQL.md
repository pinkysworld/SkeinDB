# SkeinQL v1.0 — Full Protocol & Query Language Specification

Status: Draft v1.0 (implementable; v0.3.16 runtime sync)
Last updated: 2026-05-11

SkeinQL is SkeinDB's native **non-SQL** API: a versioned, structured query and control protocol.
It is designed to coexist with MySQL/SQL compatibility mode:

- Legacy applications continue using the MySQL protocol (adoption layer).
- New applications use SkeinQL for safer primitives and proprietary features.
- Tooling may translate a subset of SQL into SkeinQL (`sql.*`), but SkeinQL is not a SQL dialect.

SkeinQL is intended to be:
- **Complete** as a query language (covers core relational algebra constructs).
- **Stable** as a protocol contract (versioned, backwards-compatible).
- **Web-native** (ETags, conditional requests, streaming changefeeds).
- **Portable** (works over HTTP/1.1 and HTTP/2; QUIC/HTTP/3 is an optional research extension).

Non-goals (v1):
- Perfect reimplementation of every SQL dialect edge case.
- Mandating one storage engine design.

---

## 1) Transport

### 1.1 RPC endpoint

Normative endpoint:
- `POST /api/v1/rpc` with `Content-Type: application/json`

Optional endpoints:
- `GET /api/v1/q/{query_id}` — cacheable prepared-query execution (ETag-aware)
- `GET /api/v1/cdc/{sub_id}` — `text/event-stream` (SSE) changefeed

### 1.2 Request envelope

Request shape:

```json
{
  "skeinql": "1.0",
  "id": "req-123",
  "method": "query.select",
  "params": {}
}
```

Field rules:
- `skeinql` (required): protocol version string. MUST be `"1.0"` for this spec.
- `id` (optional): request identifier (string or integer). If omitted, the request is a **notification** and MUST NOT produce a response body.
- `method` (required): method name (see method catalog).
- `params` (optional): method parameters object.

### 1.3 Response envelope

Success response:

```json
{
  "id": "req-123",
  "ok": true,
  "result": {}
}
```

Error response:

```json
{
  "id": "req-123",
  "ok": false,
  "error": {
    "code": "invalid_request",
    "message": "Missing field: query",
    "details": {"field": "query"}
  }
}
```

### 1.4 HTTP status codes

SkeinQL uses HTTP status codes only as a coarse signal:
- `200 OK`: request parsed and a response envelope is returned (`ok:true` or `ok:false`).
- `204 No Content`: notification (no `id`) was accepted.
- `401/403`: authorization failures MAY use HTTP codes.

Clients MUST inspect the response envelope.

### 1.5 Authentication

Recommended header:
- `Authorization: Bearer <token>`

Token issuance is implementation-defined. Minimal single-binary deployments may use:
- an environment-configured bearer token
- persisted API bearer tokens created through `security.token.create`
- loopback-only mode

SkeinQL defines optional `auth.*` methods for deployments that want built-in authentication, but `auth.*` is not required to implement core query semantics.

---

## 2) Versioning and compatibility

- Requests MUST include `skeinql:"1.0"`.
- Unknown versions MUST fail with `unsupported_version`.

Servers MAY add new methods at any time. Breaking changes require SkeinQL v2.

Servers SHOULD expose supported method names in `system.capabilities` so clients can feature-detect.

For a compact client-facing API map, see `docs/API_REFERENCE.md`. This SkeinQL document remains the language and protocol specification; the API reference is the operational catalog for current runtime method families, result formats, and client checklists.

---

## 3) Core data model

### 3.1 Identifiers

Identifiers are UTF-8 strings and are case-sensitive in SkeinQL.
(Compatibility layers may apply SQL rules.)

### 3.2 Table references

Base table reference:

```json
{ "db": "mydb", "table": "users", "as": "u" }
```

### 3.3 Typed literals

SkeinQL represents scalar values as typed objects.

```json
{"t":"null"}
{"t":"bool","v":true}
{"t":"i64","v":-1}
{"t":"u64","v":42}
{"t":"f64","v":3.14}
{"t":"dec","v":"12.3400"}
{"t":"str","v":"hello"}
{"t":"bytes","b64":"AAECAw=="}
{"t":"json","v": {"a":1}}
{"t":"embedding","dims":3,"v":[0.1,0.2,0.3],"model":"text-embedding-3-small"}
{"t":"date","iso":"2026-01-18"}
{"t":"time","iso":"12:34:56.789Z"}
{"t":"datetime","iso":"2026-01-18T12:34:56.789Z"}
{"t":"uuid","v":"550e8400-e29b-41d4-a716-446655440000"}
```

Notes:
- `dec.v` is a string to avoid JSON float rounding.
- `json.v` is an embedded JSON value.

### 3.4 Type descriptors

Column and result metadata uses a type descriptor.

Minimal:

```json
{"kind":"u64"}
```

Extended:

```json
{"kind":"varchar","max":255,"collation":"utf8mb4_unicode_520_ci"}
```

Recommended kind strings (v1 core):
- `bool`
- `i64`, `u64`
- `f64`
- `dec` (precision/scale optional)
- `str` (unbounded text)
- `varchar` (max)
- `bytes`
- `json`
- `date`, `time`, `datetime`
- `uuid`
- `embedding` (experimental)

---

## 4) Expressions

Expressions are JSON objects.

### 4.1 Column reference

```json
{"col":"email"}
{"col":"id","table":"u"}
```

### 4.2 Literal

```json
{"lit":{"t":"u64","v":1}}
```

### 4.3 Parameters

Parameters are positional:

```json
{"param":0}
```

A request that uses parameters supplies `args` alongside the query (see `query.select`).

### 4.4 Operators

Unary:

```json
{"op":"not","a":{"col":"is_active"}}
```

Binary:

```json
{"op":"eq","a":{"col":"status"},"b":{"lit":{"t":"str","v":"open"}}}
```

N-ary (and/or):

```json
{"op":"and","args":[
  {"op":"gt","a":{"col":"age"},"b":{"lit":{"t":"u64","v":18}}},
  {"op":"lt","a":{"col":"age"},"b":{"lit":{"t":"u64","v":66}}}
]}
```

`IN` list:

```json
{"op":"in","a":{"col":"status"},"list":[{"lit":{"t":"str","v":"open"}},{"lit":{"t":"str","v":"closed"}}]}
```

`BETWEEN`:

```json
{"op":"between","a":{"col":"age"},"lo":{"lit":{"t":"u64","v":18}},"hi":{"lit":{"t":"u64","v":65}}}
```

`IS NULL`:

```json
{"op":"is_null","a":{"col":"deleted_at"}}
```

Supported `op` strings (v1 core):
- boolean: `and`, `or`, `not`
- comparison: `eq`, `ne`, `lt`, `le`, `gt`, `ge`
- arithmetic: `add`, `sub`, `mul`, `div`, `mod`
- string: `like`
- membership: `in`
- null: `is_null`
- range: `between`

Servers MAY support additional ops.

### 4.5 Function calls

```json
{"fn":"lower","args":[{"col":"email"}]}
```

Optional fields:
- `distinct` (boolean) for aggregate calls

Experimental vector helpers:

```json
{"fn":"vector.cosine","args":[{"col":"embedding"},{"lit":{"t":"embedding","dims":3,"v":[0.1,0.2,0.3]}}]}
```

Supported vector functions (experimental):
- `vector.cosine`
- `vector.dot`
- `vector.l2`

### 4.6 Cast

```json
{"cast":{"expr":{"col":"age"},"to":{"kind":"str"}}}
```

### 4.7 Case

```json
{"case":{
  "when":[
    {"if":{"op":"gt","a":{"col":"age"},"b":{"lit":{"t":"u64","v":65}}},"then":{"lit":{"t":"str","v":"senior"}}}
  ],
  "else":{"lit":{"t":"str","v":"adult"}}
}}
```

### 4.8 Subqueries

Scalar subquery:

```json
{"subquery": {"query": { ... } }}
```

EXISTS:

```json
{"exists": {"query": { ... }, "negated": false}}
```

Servers that do not support subqueries MUST return `not_supported`.

---

## 5) Query language

SkeinQL expresses relational queries as a structured AST.

### 5.1 Query object

```json
{
  "with": [
    {"name":"cte1","query": {"body": {"select": { ... }} }}
  ],
  "body": {"select": { ... }},
  "order_by": [ {"expr": {"col":"id"}, "dir":"asc"} ],
  "limit": {"limit": 100, "offset": 0},
  "lock": "none"
}
```

Fields:
- `with` (optional): common table expressions (CTEs)
- `body` (required): either `select` or `setop`
- `order_by` (optional): ordering keys
- `limit` (optional): limit/offset clause
- `lock` (optional): `none | for_update | for_share`

### 5.2 Select body

```json
{
  "distinct": false,
  "projection": [
    {"expr": {"col":"id"}, "as": "id"},
    {"expr": {"fn":"lower","args":[{"col":"email"}]}, "as": "email_lc"}
  ],
  "from": [
    {"db":"mydb","table":"users","as":"u"}
  ],
  "where": {"op":"eq","a":{"col":"status"},"b":{"param":0}},
  "group_by": [],
  "having": null
}
```

Fields:
- `distinct` (optional)
- `projection` (required): list of select items
- `from` (optional): list of table refs (empty allowed for constant queries)
- `where` (optional)
- `group_by` (optional)
- `having` (optional)

### 5.3 Table references and joins

TableRef is one of:

Base table:

```json
{"db":"mydb","table":"users","as":"u"}
```

Join:

```json
{"join": {
  "type": "inner",
  "left": {"db":"mydb","table":"users","as":"u"},
  "right": {"db":"mydb","table":"orders","as":"o"},
  "on": {"op":"eq","a":{"col":"id","table":"u"},"b":{"col":"user_id","table":"o"}}
}}
```

Subquery table:

```json
{"subquery": {"query": { ... }, "as": "sq"}}
```

Join types (v1): `inner | left | right | full | cross`

### 5.4 Set operations

```json
{"setop": {
  "kind": "union",
  "all": false,
  "left": {"select": { ... }},
  "right": {"select": { ... }}
}}
```

Set kinds: `union | intersect | except`

---

## 6) Result sets

### 6.1 ResultSet object

Rows-as-arrays:

```json
{
  "columns": [
    {"name":"id","type":{"kind":"u64"}},
    {"name":"email","type":{"kind":"str"}}
  ],
  "rows": [
    [ {"t":"u64","v":1}, {"t":"str","v":"a@example.com"} ]
  ],
  "truncated": false,
  "cursor": null
}
```

Rows-as-objects:

```json
{
  "columns": [
    {"name":"id","type":{"kind":"u64"}},
    {"name":"email","type":{"kind":"str"}}
  ],
  "rows": [
    {"id": {"t":"u64","v":1}, "email": {"t":"str","v":"a@example.com"} }
  ],
  "truncated": false,
  "cursor": null
}
```

Cursor pagination (optional):
- If the server truncates output, it MUST set `truncated:true` and provide `cursor:{"id":"c_..."}`.
- Clients MAY call `query.fetch` with that cursor id.

---

## 7) Web-native cache validators (ETags)

SkeinDB treats HTTP validators as first-class query features.

The `cache` block on query execution is the protocol-level interface:

```json
"cache": {
  "want_etag": true,
  "if_none_match": "W/\"abc\"",
  "min_causality": {"format":"etag_chain_v1","deps":[{"table":"app.users","v":3}]},
  "mode": "weak",
  "persist": false
}
```

Rules:
- If `want_etag:true`, the server MUST compute an ETag for the result.
- If `if_none_match` matches, the server SHOULD avoid full execution and return `not_modified:true` with no rows.
- If `min_causality` is provided, the server MUST ensure the result is causally consistent with that token.

---

## 8) Optional wire encodings for traffic reduction

SkeinQL defaults to JSON literal rows.

Implementations MAY support alternative encodings optimized for high fan-out workloads:

### 8.1 `skeinpack_v1` (dictionary transfer)

`skeinpack_v1` allows the server to send repeated values once, referenced by stable `ValueID`s.

Request hint:

```json
"wire": {
  "format": "skeinpack_v1",
  "known_valueids": {"mode":"bloom","b64":"...","k":7}
}
```

Response includes:
- `dict`: a list of newly-needed `ValueID -> literal` mappings
- `rows`: rows referencing `ValueID`s for large/repeated values

This mechanism is intentionally optional and MUST be explicitly enabled.

See `docs/TRAFFIC_REDUCTION.md` for design notes.

---

### 8.2 `wasm_batch_v1` (columnar batch output)

`wasm_batch_v1` returns the result set as a base64-encoded columnar batch using the
`skein.wasm.batch.v1` ABI (see `docs/WASM_OPERATORS.md`).

Request:

```json
"result_format": "wasm_batch_v1"
```

Response `data` shape:

```json
{
  "format": "skein.wasm.batch.v1",
  "columns": [ {"name":"id","type":{"kind":"u64"}} ],
  "batch_b64": "..."
}
```

Notes:
- `columns` are still included for names/type metadata (the batch payload is type-tagged but
  does not carry column names).

---

## 9) Canonicalization and stable hashes

Prepared queries and certain cache keys require a stable hash.

Recommended rule:
- `query_id = base32( SHA-256( JCS(query_json) ) )`

Where:
- `JCS(x)` is the JSON Canonicalization Scheme (RFC 8785).

If a server uses a different hash (e.g., BLAKE3), it MUST specify it in `system.capabilities`.

---

## 10) Method catalog (v1)

This section defines SkeinQL methods and their request/response shapes. The current runtime advertises 129 unique methods through `system.capabilities`; clients should feature-detect against that list instead of assuming every experimental family is enabled in every deployment.

### 10.0 Reading the catalog

Method stability levels used below:

- **Core**: intended for normal clients and admin automation.
- **Compatibility**: primarily used by SQL/MySQL/PostgreSQL adoption paths.
- **Experimental**: callable today but still tied to a research track or operator workflow.
- **Prototype**: shape is documented, but compatibility is deliberately looser while the feature hardens.

Common parameter conventions:

- `table` is always `{ "db": "...", "table": "..." }` unless a method explicitly uses SQL text.
- `args` are positional typed literals that satisfy `{ "param": N }` expressions.
- `result_format` defaults to `rows_json` for tabular calls unless otherwise documented.
- Cache-aware methods accept `want_etag`, `if_none_match`, or `min_causality` either directly or under `cache`.
- Methods that mutate state return either an explicit `ok` boolean or a lifecycle object with `status`, `affected`, `applied`, or `accepted`.

### 10.1 system.*

#### system.ping
Params: `{}`

Result:

```json
{"pong":true,"time_unix_ms":0}
```

#### system.version
Params: `{}`

Result:

```json
{"name":"skeindb","version":"0.x.y","skeinql":"1.0"}
```

#### system.shutdown
Gracefully stop the server process. The server checkpoints state, marks the local cluster node offline, and sends best-effort leave notifications to peers.

Params: `{}`

Result:

```json
{"ok":true,"message":"shutdown initiated"}
```

#### system.capabilities
Params: `{}`

Result:

```json
{
  "mysql_compat": false,
  "skeinql": true,
  "causal_etags": true,
  "wasm_operators": true,
  "methods": ["system.ping","query.select"],
  "hash": {"canonicalization":"RFC8785","algorithm":"SHA-256","encoding":"base32"},
  "wire": {"skeinpack_v1": false}
}
```

### 10.2 settings.* (core)

#### settings.get
Params:

```json
{"keys":["http.port","sql.port","cluster.enabled"]}
```

Result:

```json
{"http.port":8080,"sql.port":3306,"cluster.enabled":false}
```

#### settings.set
Params: an object of key/value pairs.

```json
{"http.port":8080,"sql.port":3306}
```

Result:

```json
{"ok":true}
```

#### settings.list
Params: `{}`

Result:

```json
{"keys":["http.port","sql.port","cluster.enabled"]}
```

### 10.3 schema.*

#### schema.list_databases
Params: `{}`

Result:

```json
{"databases":["mydb","test"]}
```

#### schema.create_database
Params:

```json
{"db":"mydb"}
```

Result:

```json
{"ok":true}
```

#### schema.drop_database
Params:

```json
{"db":"mydb","if_exists":true}
```

Result:

```json
{"ok":true}
```

#### schema.list_tables
Params:

```json
{"db":"mydb"}
```

Result:

```json
{"tables":["users","orders"]}
```

#### schema.create_table
Params:

```json
{
  "db":"mydb",
  "table":"users",
  "if_not_exists": false,
  "primary_key": ["id"],
  "columns": [
    {"name":"id","type":{"kind":"u64"},"nullable":false,"auto_increment":true},
    {"name":"email","type":{"kind":"varchar","max":255},"nullable":false,"auto_increment":false},
    {"name":"status","type":{"kind":"varchar","max":32},"nullable":true,"auto_increment":false}
  ],
  "compat_mysql": {"engine":"InnoDB"}
}
```

Result:

```json
{"ok":true}
```

#### schema.describe_table
Params:

```json
{"db":"mydb","table":"users"}
```

Result:

```json
{
  "db":"mydb",
  "table":"users",
  "columns":[
    {"name":"id","type":{"kind":"u64"},"nullable":false,"auto_increment":true},
    {"name":"email","type":{"kind":"varchar","max":255},"nullable":false,"auto_increment":false}
  ],
  "primary_key":["id"],
  "indexes":[],
  "compat_mysql": {"engine":"InnoDB"}
}
```

#### schema.drop_table
Params: `{ "db":"mydb", "table":"users", "if_exists":true }`

Result: `{ "ok": true }`

#### schema.propose_change (experimental)
Propose a schema evolution change set (R15).

Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "base_version": 1,
  "changes": [
    {"op":"add_column","name":"region","type":{"kind":"str"},"nullable":true,"auto_increment":false,"default":null}
  ],
  "message": "add region for rollout"
}
```

Result:

```json
{"change_id":"sch_1","status":"pending"}
```

#### schema.merge_status (experimental)
Show pending changes and merge plan.

Params:

```json
{"table":{"db":"mydb","table":"users"}}
```

Result:

```json
{
  "table":{"db":"mydb","table":"users"},
  "current_version": 1,
  "pending":[{"change_id":"sch_1","base_version":1,"status":"pending","created_at_ms":0,"changes":[{"op":"add_column","name":"region","type":{"kind":"str"},"nullable":true,"auto_increment":false}]}],
  "conflicts":[],
  "merge_plan":["sch_1"]
}
```

#### schema.apply_merge (experimental)
Apply a merge plan (or a subset).

Params:

```json
{"table":{"db":"mydb","table":"users"},"change_ids":["sch_1"]}
```

Result:

```json
{"applied":["sch_1"],"conflicts":[],"new_version":2}
```

### 10.4 tx.*

#### tx.begin
Params:

```json
{"isolation":"snapshot","read_only":false}
```

Optional historical read-only snapshot:

```json
{"isolation":"snapshot","read_only":true,"as_of":{"t":"datetime","iso":"2026-01-01T00:00:00Z"}}
```

Result:

```json
{"tx":"tx-abc"}
```

#### tx.commit / tx.rollback
Params: `{"tx":"tx-abc"}`

Result: `{"ok":true}`

### 10.5 query.*

#### query.select
Executes a query AST.

Params:

```json
{
  "query": { ...Query... },
  "args": [ {"t":"str","v":"open"} ],
  "result_format": "rows_json",
  "cache": {
    "want_etag": true,
    "if_none_match": null,
    "min_causality": {"format":"etag_chain_v1","deps":[{"table":"app.users","v":3}]},
    "persist": false
  },
  "wire": {"format": "rows_json"},
  "tx": null,
  "as_of": null
}
```

Result:

```json
{
  "etag":"W/\"q:...\"",
  "not_modified": false,
  "data": { ...ResultSet... },
  "deps": {"tables":[{"table":"app.users","v":3}]},
  "causality": {"format":"etag_chain_v1","deps":[{"table":"app.users","v":3}]},
  "wire": null
}
```


#### query.patch
Returns a **delta patch** between a previously seen query result (identified by `base_etag`) and the current result.

This method is intended to reduce network traffic when only a small portion of a list changes between refreshes.

See also: `docs/QUERY_PATCH.md`.

Params:

```json
{
  "query": { ...Query... },
  "args": [],
  "base_etag": "W/\"q:...\"",
  "result_format": "rows_json",
  "include_full": true,
  "client_state": {
    "mode": "strict",
    "rows": [ {"pk": [ ...Lit... ], "fp": "<row_fp_hex>"} ]
  },
  "wire": {"format": "skeinpack_v1", "known_valueids": []}
}
```

Notes:
- `client_state` is optional and is primarily used to avoid a full reset when the server no longer has the base snapshot cached.
- For add-only best-effort patches (large windows), set `client_state.mode="add_only"` and provide `client_state.keys_bloom`.

Result:

- If `base_etag` matches the current ETag: `{"not_modified": true}`.
- Otherwise returns a patch object:

```json
{
  "etag": "W/\"q:...\"",
  "not_modified": false,
  "data": {
    "reset": false,
    "base_etag": "W/\"q:...\"",
    "base_source": "patch_cache",
    "columns": [ {"name":"id","type":{"kind":"u64"}} ],
    "window": {"limit": 50, "offset": 0},
    "added": [ {"pk":[{"t":"u64","v":42}], "at": 0, "row":[{"t":"u64","v":42}]} ],
    "updated": [],
    "removed": [ {"pk":[{"t":"u64","v":7}]} ],
    "moved": [ {"pk":[{"t":"u64","v":9}], "from": 3, "to": 4} ],
    "reorder": {"kind":"shift", "delta": 1, "coverage": 0.95}
  }
}
```

Patchability constraints:
- In the current prototype, `query.patch` is supported for **single-table SELECTs** where the base table has a primary key.
- The query must select the primary key columns so rows can be identified.

When the server cannot compute a safe patch it returns `reset:true` (and may include `full` when `include_full=true`).

Patch row formats depend on `result_format`:
- `rows_json`: `{ pk: Lit[], at: number, row: Lit[] }`
- `objects_json`: `{ pk: Lit[], at: number, obj: Record<string, Lit> }`
- `skeinpack_v1` (with `wire.format="skeinpack_v1"`): `{ pk: Lit[], at: number, row: SkeinpackRow }` plus top-level `dict`
- `wasm_batch_v1`: `{ pk: Lit[], at: number, row_idx: number }` plus `added_batch` / `updated_batch` with `format:"skein.wasm.batch.v1"`

#### query.prepare
Params:

```json
{"query": { ...Query... }, "name":"optional"}
```

Result:

```json
{"query_id":"q_...","canonical":{...}}
```

#### query.execute_prepared
Execute a previously prepared query by `query_id`.

Params:

```json
{
  "query_id": "q_...",
  "args": [ {"t":"u64","v":1} ],
  "if_none_match": "W/\"q:...\"",
  "min_causality": {"format":"etag_chain_v1","deps":[{"table":"app.users","v":3}]},
  "result_format": "rows_json",
  "wire": { "format": "skeinpack_v1", "known_valueids": ["v1","v2"] }
}
```

Result: same as `query.select`.

#### query.open_cursor / query.fetch / query.close
Cursor methods (optional) for large results.

### 10.6 data.*

#### data.get
Fetch a single row by primary key.

Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "pk": [ {"t":"u64","v":1} ],
  "format": "objects_json"
}
```

Result:

```json
{
  "row": {"id":{"t":"u64","v":1},"email":{"t":"str","v":"a@example.com"}},
  "etag": "W/\"r:...\""
}
```

#### data.insert
Insert one or more row objects.

Params:

```json
{
  "into": {"db":"mydb","table":"users"},
  "rows": [
    {"email":{"t":"str","v":"a@example.com"},"status":{"t":"str","v":"open"}}
  ],
  "returning": ["id"]
}
```

Result:

```json
{"affected":1,"last_insert_id":1,"returning":[{"id":{"t":"u64","v":1}}]}
```

#### data.update
Update rows matching a predicate.

Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "where": {"op":"eq","a":{"col":"id"},"b":{"lit":{"t":"u64","v":1}}},
  "set": {"status":{"t":"str","v":"closed"}},
  "limit": 1,
  "if_match": "W/\"r:...\"",
  "args": []
}
```

Result: `{ "affected": 1 }`

#### data.delete
Soft-delete rows matching a predicate.

Params: same shape as `data.update` but without `set`.

Result: `{ "affected": 1 }`

### 10.7 sql.*

SkeinDB exposes an optional SQL facade for compatibility.

#### sql.exec (optional)
Params:

```json
{"db":"mydb","sql":"SELECT id,email FROM users WHERE status = ?","args":[{"t":"str","v":"open"}]}
```

Result (shape is implementation-defined; typically includes a result set plus a compatibility report).

### 10.8 cdc.*

#### cdc.subscribe_table
Params: `{ "db":"mydb", "table":"users" }`

Result:

```json
{"sub_id":"sub_...","offset":123,"sse_url":"/api/v1/cdc/sse/sub_..."}
```

#### cdc.subscribe_query
Params:

```json
{"query_id":"q_...","args":[]}
```

Result:

```json
{"sub_id":"sub_...","offset":123,"sse_url":"/api/v1/cdc/sse/sub_..."}
```

#### cdc.poll
Params:

```json
{"sub_id":"sub_...","from_offset":123,"limit":200}
```

Result:

```json
{
  "events":[{"seq":124,"db":"mydb","table":"users","op":"update","pk":[{"t":"u64","v":1}],"commit_ts_ms":1710000000000,"lsn":124}],
  "next_offset":124,
  "earliest_offset":120,
  "latest_offset":124,
  "resnapshot_required":false
}
```

When a consumer falls behind the retained horizon, the result becomes:

```json
{
  "events":[],
  "next_offset":199,
  "earliest_offset":200,
  "latest_offset":245,
  "resnapshot_required":true,
  "resnapshot_from_offset":199,
  "resnapshot_reason":"wal_horizon_exceeded"
}
```

#### CDC SSE transport

`GET /api/v1/cdc/sse/{sub_id}` streams the same event payloads as `cdc.poll` over Server-Sent Events.

Reconnect options:

- `Last-Event-ID: <seq>`
- `GET /api/v1/cdc/sse/{sub_id}?from_offset=<seq>`

If the reconnect cursor falls behind the retained horizon, the stream emits `event: resnapshot` with the same recovery metadata returned by `cdc.poll` and then closes.

### 10.9 dp.* (experimental)

Differential privacy aggregates with budget tracking.

#### dp.aggregate
Params:

```json
{
  "table": {"db":"mydb","table":"orders"},
  "aggregates": [
    {"op":"count"},
    {"op":"sum","column":"amount","bounds":{"min":0,"max":1000},"as":"total_amount"},
    {"op":"avg","column":"amount","bounds":{"min":0,"max":1000}}
  ],
  "where": {"op":"eq","a":{"col":"status"},"b":{"lit":{"t":"str","v":"paid"}}},
  "group_by": ["region"],
  "epsilon": 1.0,
  "delta": 0.000001,
  "mechanism": "laplace",
  "principal": "analyst",
  "seed": 42
}
```

Result:

```json
{
  "columns": ["region","count","total_amount","avg_amount"],
  "rows": [
    [
      {"t":"str","v":"EU"},
      {"t":"f64","v":123.4},
      {"t":"f64","v":4567.8},
      {"t":"f64","v":37.0}
    ]
  ],
  "privacy": {
    "epsilon": 1.0,
    "delta": 0.000001,
    "mechanism": "laplace",
    "query_fingerprint": "abcd1234",
    "privacy_etag": "W/\"dp:abcd1234\"",
    "table_version": 7,
    "rows_examined": 1200,
    "rows_matched": 410,
    "aggregates": [
      {"op":"count","sensitivity":1.0,"epsilon":0.3333333333333333,"delta":0.0000003333333333333333},
      {"op":"sum","column":"amount","bounds":{"min":0,"max":1000},"sensitivity":1000.0,"epsilon":0.3333333333333333,"delta":0.0000003333333333333333},
      {"op":"avg","column":"amount","bounds":{"min":0,"max":1000},"sensitivity":1000.0,"epsilon":0.3333333333333333,"delta":0.0000003333333333333333}
    ],
    "budget": {"principal":"analyst","remaining_epsilon":9.0,"remaining_delta":0.000999}
  }
}
```

The `privacy_etag` is a weak validator for privacy-aware caches. It is derived from a v1 DP validator payload that includes the table version, canonical DP query fingerprint, epsilon/delta, mechanism, principal, seed, and post-query budget metadata when a principal budget is consumed.

#### dp.evaluate

`dp.evaluate` runs the same bounded DP aggregate plan against an exact baseline and a seeded grid of noisy trials. It is intended for research/evaluation and CI-style reports, not production query serving. The method does **not** consume a principal privacy budget because it omits `principal` by design.

Params:

```json
{
  "table": {"db":"mydb","table":"orders"},
  "aggregates": [
    {"op":"sum","column":"amount","bounds":{"min":0,"max":1000},"as":"total_amount"}
  ],
  "where": {"op":"eq","a":{"col":"status"},"b":{"lit":{"t":"str","v":"paid"}}},
  "group_by": ["region"],
  "epsilons": [0.25, 0.5, 1.0, 2.0],
  "delta": 0.0,
  "mechanism": "laplace",
  "trials": 25,
  "seed": 42
}
```

Result:

```json
{
  "table": {"db":"mydb","table":"orders"},
  "columns": ["region","total_amount"],
  "exact_rows": [[{"t":"str","v":"EU"},{"t":"f64","v":4567.0}]],
  "rows_examined": 1200,
  "rows_matched": 410,
  "exact_latency_ms": 0.42,
  "mechanism": "laplace",
  "delta": 0.0,
  "trials": 25,
  "report": [
    {
      "epsilon": 0.5,
      "trials": 25,
      "samples": 25,
      "mean_abs_error": 23.1,
      "p95_abs_error": 71.4,
      "max_abs_error": 95.0,
      "mean_relative_error": 0.005,
      "avg_latency_ms": 0.48,
      "overhead_vs_exact": 1.14
    }
  ]
}
```

#### dp.budget.set
Params:

```json
{"principal":"analyst","total_epsilon":10.0,"total_delta":0.001,"refresh_window_ms":86400000}
```

Result:

```json
{"budgets":[{"principal":"analyst","total_epsilon":10.0,"consumed_epsilon":0.0}]}
```

#### dp.budget.get
Params:

```json
{"principal":"analyst","include_usage":true}
```

Result:

```json
{"budgets":[{"principal":"analyst","total_epsilon":10.0,"consumed_epsilon":1.0}],"usage":[{"query_fingerprint":"abcd1234","epsilon":1.0,"count":1}]}
```

#### dp.audit.log
Params:

```json
{"principal":"analyst","from_id":0,"limit":200}
```

Result:

```json
{"events":[{"id":1,"principal":"analyst","epsilon":1.0}],"next_id":2}
```

### 10.10 vector.* (experimental)

Embedding-aware methods backed by the ValueStore. These are experimental.

#### vector.insert
Insert or update embeddings by primary key.

Params:

```json
{
  "table": {"db":"mydb","table":"docs"},
  "column": "embedding",
  "rows": [
    {
      "pk": [{"t":"u64","v":1}],
      "embedding": {"t":"embedding","dims":3,"v":[0.1,0.2,0.3],"model":"text-embed-v1"}
    }
  ],
  "upsert": true
}
```

Result:

```json
{"inserted":1,"updated":0,"embedding_ids":["<vid_hex>"]}
```

#### vector.search
Approximate nearest neighbor search using an LSH bucket filter followed by a distance refinement.

Params:

```json
{
  "table": {"db":"mydb","table":"docs"},
  "column": "embedding",
  "query": {"t":"embedding","dims":3,"v":[0.1,0.2,0.3]},
  "k": 10,
  "metric": "cosine",
  "filter": {"op":"eq","a":{"col":"status"},"b":{"lit":{"t":"str","v":"public"}}},
  "include_row": true,
  "use_lsh": true
}
```

Result:

```json
{
  "matches": [
    {
      "pk": [{"t":"u64","v":1}],
      "score": 0.997,
      "embedding_id": "<vid_hex>",
      "row": {"id":{"t":"u64","v":1},"status":{"t":"str","v":"public"}}
    }
  ]
}
```

Notes:
- `metric` supports `cosine`, `dot`, and `l2`. Scores are similarity values (higher is better).
- `filter` cannot reference the embedding column.
- `use_lsh=true` restricts candidates to the query bucket (prototype behavior).

Hybrid query example (filter + vector order):

```json
{
  "body": {"select": {"projection":[{"expr":{"col":"id"}}],"from":[{"db":"mydb","table":"docs"}],
    "where":{"op":"eq","a":{"col":"status"},"b":{"lit":{"t":"str","v":"public"}}}}},
  "order_by": [{"expr":{"fn":"vector.cosine","args":[{"col":"embedding"},{"lit":{"t":"embedding","dims":3,"v":[0.1,0.2,0.3]}}]},"dir":"desc"}],
  "limit": {"limit": 10}
}
```

Notes:
- ORDER BY with `vector.*` and `limit` may use an LSH bucket prefilter (approximate; can miss true top-k).

#### vector.index.status
Return LSH bucket statistics for the embedding column.

Params:

```json
{"table":{"db":"mydb","table":"docs"},"column":"embedding"}
```

Result:

```json
{"table":{"db":"mydb","table":"docs"},"column":"embedding","vectors":1200,"buckets":64,"min_bucket":12,"max_bucket":18446744073709551615}
```

### 10.11 ai.* (experimental)

LLM-assisted autoparameterization helpers (prototype).

#### ai.autoparam.classify
Classify literals as parameterizable vs semantic constants.

Params:

```json
{
  "sql": "select * from users where status = 'active' and id = 42",
  "db": "app",
  "literals": [
    {"value":{"t":"str","v":"active"},"column":"status","table":"users","op":"eq"},
    {"value":{"t":"u64","v":42},"column":"id","table":"users","op":"eq"}
  ],
  "rules": {
    "semantic_constant_columns": ["users.status", "*.state"],
    "parameterize_columns": ["users.id"]
  }
}
```

Result:

```json
{
  "labels": [
    {"idx":0,"decision":"semantic_constant","confidence":0.75,"reason":"enum-like column"},
    {"idx":1,"decision":"parameterize","confidence":0.7,"reason":"id-like column"}
  ]
}
```

Decisions:
- `parameterize`
- `semantic_constant`
- `unknown`

Notes:
- `db` enables schema-aware classification when literals include table names.
- `rules` supports `*` globs (e.g., `*.status`).

#### ai.autoparam.analyze
Extract literals from SQL text and classify them.

Params:

```json
{
  "sql": "select * from users where status = 'active' and id = 42",
  "db": "app",
  "rules": {
    "semantic_constant_columns": ["users.status"],
    "parameterize_columns": ["users.id"]
  }
}
```

Result:

```json
{
  "normalized_sql": "select * from users where status = ? and id = ?",
  "fingerprint": "3a5b2c...",
  "literals": [
    {"value":{"t":"str","v":"active"},"column":"status","table":"users","op":"eq"},
    {"value":{"t":"u64","v":42},"column":"id","table":"users","op":"eq"}
  ],
  "labels": [
    {"idx":0,"decision":"semantic_constant","confidence":0.95,"reason":"rule: semantic_constant"},
    {"idx":1,"decision":"parameterize","confidence":0.95,"reason":"rule: parameterize"}
  ]
}
```

#### ai.nl.translate
Package a natural-language request into a SkeinQL prompt bundle and (optionally) a rule-based query.

Params:

```json
{
  "db": "app",
  "request": "list users",
  "tables": ["users"],
  "read_only": true,
  "include_schema": true,
  "max_tables": 16
}
```

Result:

```json
{
  "package": {
    "version": "nl_prompt_v1",
    "db": "app",
    "request": "list users",
    "read_only": true,
    "tables": [
      {
        "table": "users",
        "columns": [{"name":"id","type":{"kind":"u64"},"nullable":false}],
        "primary_key": ["id"]
      }
    ],
    "prompt": "You generate SkeinQL JSON for SkeinDB...\n",
    "fingerprint": "9c6c..."
  },
  "query": {
    "body": {"select": {"projection":[{"expr":{"col":"id"}}],"from":[{"db":"app","table":"users"}]}}
  },
  "warnings": []
}
```

Notes:
- `query` is only populated for simple rule-based patterns (e.g., “list users”).
- Use `prompt` + `tables` to drive offline/LLM translation.

#### ai.nl.explain
Generate a verification summary and preview rows for a SkeinQL query.

Params:

```json
{
  "query": {
    "body": {"select": {"projection":[{"expr":{"col":"id"}}],"from":[{"db":"app","table":"users"}]}}
  },
  "preview_limit": 3,
  "preview_format": "objects_json"
}
```

Result:

```json
{
  "summary": "Selects id from app.users",
  "tables": ["app.users"],
  "projection": ["id"],
  "filters": [],
  "order_by": [],
  "limit": null,
  "deps": {"tables":[{"table":"app.users","v":3}]},
  "approval_token": "b8e4...",
  "preview": [{"id":{"t":"u64","v":1}}],
  "preview_format": "objects_json"
}
```

Notes:
- `approval_token` must be supplied to `ai.nl.execute`.

#### ai.nl.execute
Execute a verified query using the `approval_token` from `ai.nl.explain`.

Params:

```json
{
  "query": {
    "body": {"select": {"projection":[{"expr":{"col":"id"}}],"from":[{"db":"app","table":"users"}]}}
  },
  "approval_token": "b8e4...",
  "min_causality": {"format":"etag_chain_v1","deps":[{"table":"app.users","v":3}]},
  "result_format": "objects_json"
}
```

Result (same shape as `query.select`):

```json
{
  "etag": "W/\"q:...\"",
  "not_modified": false,
  "data": [{"id":{"t":"u64","v":1}}],
  "deps": {"tables":[{"table":"app.users","v":3}]},
  "causality": {"format":"etag_chain_v1","deps":[{"table":"app.users","v":3}]}
}
```

### 10.12 oblivious.* (experimental)

Oblivious execution policy controls for multi-tenant access-pattern protection.

#### oblivious.policy.set
Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "policy": {
    "level":"basic",
    "pad_to_multiple": 32,
    "target_rows": null,
    "dummy_value_lookups": 64,
    "shuffle": false
  }
}
```

Result:

```json
{"policies":[{"table":{"db":"mydb","table":"users"},"policy":{"level":"basic","pad_to_multiple":32}}]}
```

#### oblivious.policy.get
Params:

```json
{"table":{"db":"mydb","table":"users"}}
```

Result:

```json
{"policies":[{"table":{"db":"mydb","table":"users"},"policy":{"level":"basic","pad_to_multiple":32}}]}
```

#### oblivious.explain
Params:

```json
{"table":{"db":"mydb","table":"users"}}
```

Result:

```json
{"plan":{"table":"mydb.users","level":"basic","actual_rows":123,"target_rows":128,"dummy_rows":5}}
```

#### oblivious.evaluate
Params:

```json
{"table":{"db":"mydb","table":"users"},"trace_rows":[1,2,31,32,33,63,64,65]}
```

Result:

```json
{
  "table": {"db":"mydb","table":"users"},
  "policy": {"level":"basic","pad_to_multiple":32,"dummy_value_lookups":64,"shuffle":false},
  "actual_rows": 123,
  "trace": [
    {"actual_rows":31,"unpadded_accesses":31,"target_rows":32,"dummy_rows":1,"dummy_value_lookups":64,"observed_accesses":96,"overhead_ratio":3.0967741935}
  ],
  "leakage": {
    "unique_actual_rows": 8,
    "unique_unpadded_observations": 8,
    "unique_padded_observations": 3,
    "unpadded_mutual_information_bits": 3.0,
    "padded_mutual_information_bits": 1.5,
    "reduction_bits": 1.5
  },
  "performance": {"mean_overhead_ratio":2.4,"max_overhead_ratio":96.0,"total_dummy_rows":40,"total_dummy_value_lookups":512}
}
```

### 10.13 forensic.* (experimental, R06 hardened)

Forensic query and verification over the hash-chained audit log. `forensic.query` accepts structured table/op/id bounds plus a minimal JSON filter grammar, and returns proof material for contiguous chain slices and filtered Merkle-inclusion result sets.

#### forensic.query
Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "from_id": 0,
  "to_id": 500,
  "limit": 200,
  "filter": {"op":"eq","a":{"col":"op"},"b":{"lit":{"t":"str","v":"insert"}}}
}
```

Result:

```json
{
  "records": [{"id":1,"op":"insert","hash":"abcd","prev_hash":"genesis"}],
  "proof": {
    "format": "skein.forensic.proof.v1",
    "contiguous": true,
    "preceding_hash": null,
    "following_hash": null,
    "checkpoint_anchor": null,
    "chain_head": "abcd",
    "merkle_root": "...",
    "chain_merkle_root": "...",
    "inclusion_proofs": [{"record_id":1,"chain_index":0,"record_hash":"abcd","siblings":[]}],
    "index_summary": {"matched_records":1,"by_table":{"mydb.users":1},"by_op":{"insert":1},"by_actor":{"unknown":1}}
  }
}
```

Filter operators: `and`, `or`, `not`, `eq`, `ne`, `gt`, `ge`, `lt`, `le`, and `contains`. Filter operands can reference `id`, `ts_ms`, `db`, `schema`, `table`, `op`, `operation`, `change_seq`, `seq`, `pk`, `prev_hash`, or `hash` with `{ "col": "..." }`, or typed SkeinQL literals with `{ "lit": {"t":"str","v":"..."} }`.

#### forensic.verify
Params:

```json
{"records":[{"id":1,"op":"insert","hash":"abcd","prev_hash":"genesis"}],"start_hash":"genesis"}
```

Result:

```json
{"ok":true,"head_hash":"abcd"}
```

#### forensic.export
Params:

```json
{"table":{"db":"mydb","table":"users"},"bundle_id":"case-17","limit":200}
```

Result:

```json
{
  "bundle": {
    "format": "skein.forensic.bundle.v1",
    "bundle_id": "case-17",
    "generated_at_ms": 1778500000000,
    "query": {"table":{"db":"mydb","table":"users"},"limit":200},
    "records": [{"id":1,"op":"insert"}],
    "proof": {"format":"skein.forensic.proof.v1","chain_head":"abcd"},
    "verification": {"ok":true,"head_hash":"abcd"}
  }
}
```

### 10.14 merge.* (experimental)

Optimistic concurrency with built-in and values-only Wasm merge functions. See `docs/MERGE_FUNCTIONS.md`.

#### merge.register
Params:

```json
{
  "table": {"db":"mydb","table":"counters"},
  "policy": {
    "default": {"kind":"builtin","name":"last_write_wins"},
    "per_column": {"count": {"kind":"builtin","name":"sum"}}
  }
}
```

Result:

```json
{"ok": true}
```

#### merge.apply
Params:

```json
{
  "table": {"db":"mydb","table":"counters"},
  "pk": [{"t":"u64","v":1}],
  "incoming": {"count": {"t":"u64","v":3}},
  "expected_etag": "W/\"r:...\"",
  "min_causality": {"format":"etag_chain_v1","deps":[{"table":"mydb.counters","v":12}]},
  "policy": {"default":{"kind":"builtin","name":"sum"}}
}
```

Result:

```json
{"applied":true,"conflict":true,"merged":{"count":{"t":"u64","v":13}},"etag":"W/\"r:...\"","conflicts":["write_write"]}
```

`conflicts` may include: `write_write`, `dependency`, `constraint`.

#### merge.simulate
Params:

```json
{
  "current": {"count": {"t":"u64","v":10}},
  "incoming": {"count": {"t":"u64","v":3}},
  "policy": {"default": {"kind":"builtin","name":"sum"}}
}
```

Result:

```json
{"merged":{"count":{"t":"u64","v":13}}}
```

#### merge.evaluate
Params:

```json
{
  "policy": {
    "default": {"kind":"builtin","name":"last_write_wins"},
    "per_column": {"count":{"kind":"builtin","name":"sum"}}
  },
  "iterations": 10,
  "cases": [
    {
      "name": "counter conflict",
      "current": {"count":{"t":"u64","v":7}},
      "incoming": {"count":{"t":"u64","v":4}},
      "expected_etag_match": false,
      "min_causality_satisfied": true,
      "constraint_ok": true
    }
  ]
}
```

Result:

```json
{
  "format": "skein.merge.evaluate.v1",
  "cases": 1,
  "iterations": 10,
  "conflict_count": 1,
  "resolved_count": 1,
  "conflict_rate": 1.0,
  "resolution_success_rate": 1.0,
  "mean_merge_ns": 1200,
  "p95_merge_ns": 1500,
  "results": [
    {
      "name": "counter conflict",
      "conflict": true,
      "conflicts": ["write_write"],
      "resolved": true,
      "merged": {"count":{"t":"u64","v":11}},
      "mean_merge_ns": 1200
    }
  ]
}
```

#### merge.wasm.register
Params:

```json
{
  "module_id": "merge_sum",
  "name": "sum merge",
  "wasm_b64": "<base64 wasm module>",
  "capabilities": {
    "values_only": true,
    "deterministic": true,
    "max_fuel": 1000,
    "max_memory_bytes": 65536,
    "max_output_bytes": 4096
  }
}
```

Result:

```json
{"ok":true,"value_id":"...","size_bytes":1}
```

#### merge.wasm.list
Result:

```json
{
  "modules": [
    {
      "module_id": "merge_sum",
      "value_id": "...",
      "size_bytes": 1,
      "capabilities": {"values_only":true,"deterministic":true,"max_fuel":1000,"max_memory_bytes":65536,"max_output_bytes":4096},
      "name": "sum merge",
      "created_at_ms": 1730000000000
    }
  ]
}
```

#### merge.wasm.drop
Params:

```json
{"module_id":"merge_sum"}
```

Result:

```json
{"ok":true}
```

### 10.15 wasm.plan.* (experimental)

Portable Wasm plan compilation and execution. See `docs/WASM_OPERATORS.md`.

#### wasm.plan.compile
Params:

```json
{
  "query": {
    "body": {
      "select": {
        "projection": [{"expr": {"col": "id"}}, {"expr": {"col": "score"}}],
        "from": [{"db": "app", "table": "users"}],
        "where": {"op": "gt", "a": {"col": "score"}, "b": {"param": 0}}
      }
    },
    "order_by": []
  },
  "abi": "skein.wasm.batch.v1",
  "target": "wasm32-unknown-unknown"
}
```

Result:

```json
{
  "format": "skein.wasm.plan.v1",
  "abi": "skein.wasm.batch.v1",
  "artifact_b64": "...",
  "target": "wasm32-unknown-unknown",
  "execution": "host_interpreted_v1",
  "artifact_bytes": 392,
  "operator_count": 3,
  "operators": ["scan", "filter", "project"],
  "supports_edge_package": true,
  "supports_simd": false
}
```

#### wasm.plan.inspect
Params:

```json
{"artifact_b64":"..."}
```

Result:

```json
{
  "format": "skein.wasm.plan.v1",
  "abi": "skein.wasm.batch.v1",
  "target": "wasm32-unknown-unknown",
  "execution": "host_interpreted_v1",
  "artifact_bytes": 392,
  "operator_count": 3,
  "operators": ["scan", "filter", "project"],
  "table": {"db":"app","table":"users"},
  "has_filter": true,
  "projection_count": 2,
  "supports_edge_package": true,
  "supports_simd": false
}
```

#### wasm.plan.edge_package
Params:

```json
{"artifact_b64":"...","package_name":"users-score-plan"}
```

Result:

```json
{
  "format": "skein.wasm.edge_package.v1",
  "package_name": "users-score-plan",
  "artifact_b64": "...",
  "artifact_bytes": 392,
  "artifact_sha256": "...",
  "manifest_json": "{...}",
  "runner_js": "export async function runSkeinWasmPlan(...) { ... }",
  "instructions": ["Store artifact_b64 and manifest_json with the edge worker or browser bundle."]
}
```

#### wasm.plan.run
Params:

```json
{
  "artifact_b64": "...",
  "args": [{"t":"u64","v":7}],
  "result_format": "objects_json",
  "cache": {"want_etag": true}
}
```

Result: same envelope as `query.select` (`QueryExecResult`).

### 10.16 view.* (experimental)

Materialized view management (prototype). See `docs/INCREMENTAL_VIEWS.md`.

#### view.create
Params:

```json
{
  "view": {"db":"mydb","table":"top_users"},
  "query": {
    "body": {"select": {"projection":[{"expr":{"col":"id"}},{"expr":{"col":"city"}}],"from":[{"db":"mydb","table":"users"}],"where":{"op":"gt","a":{"col":"score"},"b":{"lit":{"t":"u64","v":10}}}}},
    "order_by": []
  }
}
```

Result:

```json
{"ok":true,"columns":["id","city"]}
```

`view.create` supports single-table filter/project views and single-table grouped
views whose projection contains only grouped columns plus `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`.

#### view.drop
Params:

```json
{"view":{"db":"mydb","table":"top_users"},"if_exists":true}
```

Result:

```json
{"ok":true}
```

#### view.refresh
Params:

```json
{"view":{"db":"mydb","table":"top_users"},"mode":"incremental"}
```

Result:

```json
{"mode":"incremental","rows":2,"last_change_seq":128}
```

When `mode` is `"auto"`, the result `mode` reports the actual path chosen by the
engine (`"incremental"` or `"full"`).

#### view.evaluate
Params:

```json
{"view":{"db":"mydb","table":"top_users"},"iterations":5}
```

Result:

```json
{"format":"skein.view.evaluate.v1","iterations":5,"pending_changes":3,"touched_pks":3,"stale_before":true,"incremental_ok":true,"correct":true,"rows_incremental":42,"rows_full":42,"mean_incremental_ns":18000,"mean_full_ns":97000,"speedup_vs_full":5.38,"recommended_mode":"incremental"}
```

The method is read-only: it clones the stored view state, compares incremental
refresh with full recompute, reports timing/speedup, and leaves the live view
unchanged.

#### view.status
Params:

```json
{"view":{"db":"mydb","table":"top_users"}}
```

Result:

```json
{"views":[{"db":"mydb","view":"top_users","rows":2,"stale":false,"last_change_seq":128,"last_refresh_mode":"incremental","deps":[{"db":"mydb","table":"users","columns":["id","city","score"],"projection_columns":["id","city"],"predicate_columns":["score"],"group_by_columns":[]}]}]}
```

#### view.explain_deps
Params:

```json
{"view":{"db":"mydb","table":"top_users"}}
```

Result:

```json
{"deps":[{"db":"mydb","table":"users","columns":["id","city","score"],"projection_columns":["id","city"],"predicate_columns":["score"],"group_by_columns":[]}]}
```

### 10.17 edge.* (experimental)

Replay bundles as an edge replication primitive (R14). See `docs/TIME_TRAVEL_REPLAY.md`.

#### edge.bundle.request
Request bounded change windows as a replay bundle.

Params:

```json
{
  "windows": [
    {"table":{"db":"app","table":"users"}, "from_seq": 10, "to_seq": 40, "max_events": 100}
  ],
  "redaction": {"mode":"hash_pk","salt":"optional"},
  "bundle_id": "edge_bundle_01"
}
```

Notes:
- `from_seq` is **exclusive** (only records with `seq > from_seq` are included).
- `to_seq` defaults to the latest known sequence for the table.
- `redaction.mode` may be `none`, `hash_pk`, or `drop_pk`.

Result:

```json
{
  "bundle": {
    "bundle_id": "edge_bundle_01",
    "generated_at_ms": 0,
    "redaction": {"mode":"hash_pk"},
    "coverage": [
      {"table":{"db":"app","table":"users"},"start_seq":11,"end_seq":40,"updated_at_ms":0,"bundle_id":"edge_bundle_01","redaction_mode":"hash_pk"}
    ],
    "records": [
      {"seq":11,"db":"app","table":"users","op":"insert","pk_hash":"..."}
    ]
  }
}
```

#### edge.bundle.apply
Apply bundle coverage to an edge node.

Params:

```json
{"bundle": { ...EdgeBundle... }}
```

Result:

```json
{"applied":true,"coverage":[{"table":{"db":"app","table":"users"},"start_seq":11,"end_seq":40,"updated_at_ms":0,"bundle_id":"edge_bundle_01","redaction_mode":"hash_pk"}]}
```

#### edge.bundle.status
Return current coverage and routing eligibility for a bounded-staleness query.

Params:

```json
{
  "query": { ...Query... },
  "max_lag": 5
}
```

Result:

```json
{
  "coverage": [{"table":{"db":"app","table":"users"},"start_seq":11,"end_seq":40,"updated_at_ms":0,"bundle_id":"edge_bundle_01","redaction_mode":"hash_pk"}],
  "route": {"eligible":true,"max_lag":5,"observed_lag":0,"tables":["app.users"]}
}
```

### 10.18 udf.*

UDF methods are defined in `docs/WASM_UDFS.md`. Implementations MAY expose:
- `udf.register_wasm`
- `udf.call_scalar`
- `udf.list`
- `udf.drop`

### 10.19 maintenance.*

Maintenance methods are defined in `docs/PROJECT_BACKLOG.md`. Current runtime support includes:
- `maintenance.audit_status`
- `maintenance.audit_verify`
- `maintenance.compaction.status`: return live compaction pressure plus scheduler policy/budget/admission state.
- `maintenance.compaction.set_policy`: persist `compaction.*` settings including `policy`, `enabled`, `paused`, `max_l0_files`, `max_l0_bytes`, `budget`, and `peak_windows`.
- `maintenance.compaction.pause`
- `maintenance.compaction.resume`
- `maintenance.replay.export`: export snapshot-based replay bundles. Params include optional `db`, `from_lsn`, `to_lsn`, `bundle_id`, and `redaction: {"mode":"none"|"hash_pk"|"drop_pk", "salt":"optional"}`.
- `maintenance.replay.import`: validate and materialize a replay bundle into a hidden replay workspace.
- `maintenance.replay.run`: reopen a replay workspace and compare canonical checksums; returns `performance_report` for performance-annotated bundles.

### 10.20 telemetry.* / advisor.* / admin.* / cluster.*

#### advisor.index_synthesize (experimental)
Propose index candidates from observed dependency telemetry (R16).

Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "limit": 20,
  "min_queries": 3,
  "min_rows": 32
}
```

Result:

```json
{
  "table": {"db":"mydb","table":"users"},
  "suggestions": [
    {
      "id": "idxs_5a2c...",
      "columns": ["city","created_at"],
      "include": ["name"],
      "score": 128,
      "count": 4,
      "rows_scanned": 128
    }
  ]
}
```

#### advisor.apply_index (experimental)
Queue an in-memory secondary-index build for the suggestion and record the action.

Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "columns": ["city","created_at"],
  "include": ["name"],
  "note": "accepted for rollout"
}
```

Result:

```json
{"accepted":true,"action_id":"adv_0000000000000001","status":"queued","progress_pct":0}
```

Initial status values: `queued`, `exists`.

Follow `advisor.history` for terminal lifecycle state and result metadata.

#### advisor.dismiss (experimental)
Dismiss an index suggestion (suppressed from future synthesize output). Drops any advisor-built in-memory index.

Params:

```json
{
  "table": {"db":"mydb","table":"users"},
  "columns": ["city","created_at"],
  "include": ["name"],
  "note": "not selective enough"
}
```

Result:

```json
{"dismissed":true,"action_id":"adv_0000000000000002"}
```

#### advisor.history (experimental)
List advisor actions (apply/dismiss).

Params:

```json
{"table":{"db":"mydb","table":"users"},"limit":20}
```

Result:

```json
{
  "entries": [
    {
      "id":"adv_0000000000000002",
      "suggestion_id":"idxs_5a2c...",
      "table":{"db":"mydb","table":"users"},
      "columns":["city","created_at"],
      "include":["name"],
      "action":"apply",
      "created_at_ms":0,
      "note":"accepted for rollout",
      "status":"completed",
      "progress_pct":100,
      "result_status":"built",
      "updated_at_ms":0
    }
  ]
}
```

#### migration.intent_report (experimental)
Detect application intent patterns (pagination, polling, soft deletes, hierarchy self-joins, recursive CTEs, EXISTS membership, COALESCE defaults) and suggest SkeinQL-native alternatives.

Params:

```json
{
  "samples": [
    { "query": { "...": "..." }, "args": [], "at_ms": 0 }
  ],
  "limit": 20,
  "window_ms": 60000
}
```

Result:

```json
{
  "suggestions": [
    {
      "intent":"pagination.offset_limit",
      "confidence":0.7,
      "title":"Offset pagination detected",
      "recommendation":"Replace LIMIT/OFFSET pagination with cursor-based pagination on a stable ordering column.",
      "skeinql_snippet":"cursor pagination: ...",
      "evidence":[{"query_index":0,"columns":["created_at"]}]
    }
  ]
}
```

#### migration.rewrite_preview (experimental)
Generate "before/after" rewrite hints based on detected intent patterns.

Params:

```json
{
  "samples": [
    { "query": { "...": "..." }, "args": [], "at_ms": 0 }
  ],
  "limit": 10,
  "window_ms": 60000
}
```

Result:

```json
{
  "rewrites": [
    {
      "intent":"pagination.offset_limit",
      "confidence":0.7,
      "title":"Offset pagination detected",
      "before":"SELECT ... FROM app.users ORDER BY created_at LIMIT 10 OFFSET 20",
      "after":"cursor pagination: ...",
      "evidence":[{"query_index":0,"columns":["created_at"]}]
    }
  ]
}
```

#### migration.report_export (experimental)
Generate an offline migration report with both machine-readable JSON and Markdown.

Params:

```json
{
  "samples": [
    { "query": { "...": "..." }, "args": [], "at_ms": 0 }
  ],
  "limit": 10,
  "window_ms": 60000,
  "title": "SkeinDB migration report"
}
```

Result:

```json
{
  "generated_at_ms": 0,
  "title": "SkeinDB migration report",
  "report_json": {
    "suggestion_count": 1,
    "rewrite_count": 1,
    "suggestions": [],
    "rewrites": []
  },
  "markdown": "# SkeinDB migration report\n..."
}
```

Control-plane method families for operations and adoption:
- telemetry: `telemetry.compat_log`, `telemetry.snapshot`
- advisor: `advisor.index_synthesize`, `advisor.apply_index`, `advisor.dismiss`, `advisor.history`, `advisor.migration_hints` (MySQL -> SkeinQL)
- migration: `migration.intent_report`, `migration.rewrite_preview`, `migration.report_export`
- admin: `admin.users.*`, `admin.roles.*`
- cluster: `cluster.status`, `cluster.nodes`, `cluster.join_token.create`, `cluster.node.join`, `cluster.node.remove`, `cluster.node.leave`, `cluster.replica.promote`, `cluster.shard.create`, `cluster.shard.move`, `cluster.shard.rebalance`

#### cluster.status (experimental)
Return control-plane state, nodes, shards, and replication counters.

Params:

```json
{}
```

Result (example):

```json
{
  "enabled": true,
  "cluster_id": "cluster-1760000000000",
  "local_node_id": "node-127-0-0-1-9090",
  "primary_node_id": "node-127-0-0-1-9090",
  "local_role": "primary",
  "nodes": [],
  "shards": [],
  "replication": {"shipped_ops":0,"applied_ops":0,"failed_ops":0,"last_updated_ms":0},
  "methods": ["cluster.status","cluster.nodes","cluster.join_token.create"]
}
```

#### cluster.node.join (experimental)
Join a node to the cluster using a short-lived token.

Params:

```json
{
  "token": "join_...",
  "node_id": "replica-a",
  "rpc_url": "http://127.0.0.1:8081",
  "role": "replica"
}
```

#### cluster.node.leave (experimental)
Mark a node as offline without deleting its metadata. This is used for graceful shutdown and node liveness signaling.

Params:

```json
{
  "node_id": "replica-a"
}
```

#### cluster.shard.create / cluster.shard.move / cluster.shard.rebalance (experimental)
Manage shard placement for table-level write ownership and replica fanout.

---

## 11) Error codes

Errors use a stable `code` string.

Common codes:
- `invalid_request`
- `unsupported_version`
- `unauthorized`
- `forbidden`
- `not_found`
- `not_supported`
- `not_implemented`
- `conflict`
- `timeout`
- `internal`

`conflict` is used for optimistic concurrency failures (e.g., `if_match`).

---

## 12) Implementation notes

This specification defines the protocol contract. Implementations may ship with partial method coverage.
Servers SHOULD expose supported method names in `system.capabilities`.

For experimental/research methods, see:
- `docs/SKEINQL_RESEARCH_EXTENSIONS.md`
