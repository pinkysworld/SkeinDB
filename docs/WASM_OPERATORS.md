# Wasm Query Operators

Status: Prototype
Last updated: 2026-05-09

Goal:
Define a stable ABI for columnar batches and a portable plan artifact so SkeinDB can compile query operators to WebAssembly.

This document defines the v1 ABI and the initial plan subset used by `wasm.plan.*`.

---

## 1) Scope (v1)

Supported operator subset:
- scan (single base table)
- filter (WHERE predicate)
- project (SELECT expressions)

Unsupported in v1:
- joins, aggregates, group_by/having
- order_by, limit, distinct
- subqueries, case/cast expressions

---

## 2) Columnar batch ABI (skein.wasm.batch.v1)

### 2.1 Layout overview

Batches are encoded as a single byte buffer in little-endian order.
All offsets are relative to the start of the buffer.

```
struct BatchHeaderV1 {
  u32 magic;         // 'S','K','B','1'
  u16 version;       // 1
  u16 flags;         // reserved
  u32 row_count;
  u32 column_count;
  u32 columns_offset; // start of ColumnMeta array
}

struct ColumnMetaV1 {
  u32 type_tag;      // see 2.2
  u32 data_offset;   // start of column data
  u32 data_len;      // bytes
  u32 nulls_offset;  // 0 if no null bitmap
  u32 nulls_len;     // bytes
  u32 aux_offset;    // for varlen (offsets) or 0
  u32 aux_len;
}
```

### 2.2 Type tags

Type tags align with SkeinQL literal kinds:
- 1: bool (1 byte per row)
- 2: i64 (8 bytes)
- 3: u64 (8 bytes)
- 4: f64 (8 bytes)
- 5: str (varlen, UTF-8)
- 6: bytes (varlen)

### 2.3 Null bitmap

If `nulls_offset` is non-zero, it points to a bitmap with 1 bit per row.
Bit=1 indicates a non-null value, bit=0 indicates NULL.
If omitted, all values are non-null.

### 2.4 Varlen encoding

For `str` and `bytes` columns:
- `aux_offset` points to a u32 offsets array of length `row_count + 1`.
- `data_offset` points to the concatenated payload bytes.
- The i-th value spans `data[offs[i]..offs[i+1]]`.

---

## 3) Operator ABI (v1)

Operators are pure batch-to-batch transforms. The module exports:

```
// Returns (ptr << 32) | len, like skein UDFs.
export fn skein_plan_eval(ptr: u32, len: u32) -> u64
```

Rules:
- The host writes the input batch into module memory at (ptr,len).
- The function returns a packed (ptr,len) for the output batch.
- Returning len=0 indicates end-of-stream.

Memory management follows the UDF ABI in `docs/WASM_UDFS.md`.

---

## 4) Plan artifact format (skein.wasm.plan.v1)

The portable plan artifact is JSON, base64-encoded for transport:

```json
{
  "format": "skein.wasm.plan.v1",
  "abi": "skein.wasm.batch.v1",
  "target": "wasm32-unknown-unknown",
  "execution": "host_interpreted_v1",
  "plan": {
    "ops": [
      {"op": "scan", "table": {"db": "app", "table": "users"}},
      {"op": "filter", "predicate": {"op":"gt","a":{"col":"score"},"b":{"param":0}}},
      {"op": "project", "projection": [{"expr":{"col":"id"}}, {"expr":{"col":"score"}}]}
    ]
  }
}
```

Rules:
- `scan` must be first and exactly once.
- `project` must be last and exactly once.
- `filter` is optional and must appear between scan and project.
- `execution` is `host_interpreted_v1` in the current implementation. Native Wasm code generation remains open work.

---

## 5) SkeinQL methods

### wasm.plan.compile

Params:

```json
{
  "query": {"body": {"select": {"projection": [{"expr": {"col": "id"}}], "from": [{"db": "app", "table": "users"}]}}},
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

### wasm.plan.inspect

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

### wasm.plan.edge_package

Params:

```json
{
  "artifact_b64": "...",
  "package_name": "users-score-plan"
}
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
  "instructions": [
    "Store artifact_b64 and manifest_json with the edge worker or browser bundle.",
    "Call runSkeinWasmPlan with the SkeinDB RPC URL, artifact_b64, args, and desired result_format."
  ]
}
```

The v1 edge package ships the plan artifact, a manifest, and a small JavaScript runner that calls `wasm.plan.run` on a SkeinDB host. It does not yet execute a generated Wasm module inside the edge process.

### wasm.plan.run

Params:

```json
{
  "artifact_b64": "...",
  "args": [{"t":"u64","v":7}],
  "result_format": "objects_json",
  "cache": {"want_etag": true},
  "wire": {"format": "skeinpack_v1"}
}
```

Result: the same envelope as `query.select` (`QueryExecResult`).

When `result_format: "wasm_batch_v1"` is used, `data` contains a columnar batch:

```json
{
  "format": "skein.wasm.batch.v1",
  "columns": [ {"name":"id","type":{"kind":"u64"}} ],
  "batch_b64": "..."
}
```

---

## 6) Prototype notes

Current implementation:
- The plan artifact is interpreted by the host (`execution: "host_interpreted_v1"`).
- Only the scan/filter/project subset is accepted.
- `abi` and `target` are validated; `target` is recorded in the artifact for inspection and packaging.
- `wasm.plan.inspect` exposes artifact metadata without running the query.
- `wasm.plan.edge_package` emits a host-backed edge runner package. Native in-edge Wasm codegen and SIMD remain open T085/T086 work.
