# Getting Started

This repository contains a **single-binary** SkeinDB prototype.

The focus of the scaffold is:
- a portable executable (`skeindb`) that runs on Linux/macOS/Windows
- an embedded HTTP API (SkeinQL JSON-RPC) + embedded admin UI (SkeinAdmin)
- a MySQL compatibility surface (protocol + subset of SQL) intended as an adoption layer
- research primitives (ETags, query patches, hash-chained WAL, ...)

> Note
> The current executor is a small in-memory/JSON-backed prototype intended to make SkeinQL immediately usable.
> The full ValueID/MVCC/LSM storage engine described in the paper is a planned build-out.

---

## 1) Build

Prerequisites:
- Rust toolchain (stable)

From the repo root:

```bash
cargo build --release
```

The binary will be available at:

```text
./target/release/skeindb
```

---

## 2) Run the single binary

Run with an HTTP port and MySQL port of your choice:

```bash
./target/release/skeindb serve \
  --data ./data \
  --http 8080 \
  --mysql 3306
```

You can now access:
- SkeinAdmin: `http://127.0.0.1:8080/admin`
- SkeinQL JSON-RPC: `http://127.0.0.1:8080/api/v1/rpc`
- Prepared-query GET endpoint: `http://127.0.0.1:8080/api/v1/q/<query_id>`
- MySQL listener on `127.0.0.1:3306` (handshake/auth + literal `SELECT` over `COM_QUERY`; broader SQL compatibility is still expanding)

---

## 3) First SkeinQL commands

### 3.1 Health check

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"system.ping","params":{}}'
```

### 3.2 Create a schema

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{
    "jsonrpc":"2.0",
    "id":2,
    "method":"schema.create",
    "params":{
      "name":"demo",
      "tables":[
        {"name":"users","primary_key":["id"],"columns":[
          {"name":"id","type":"i64"},
          {"name":"name","type":"string"},
          {"name":"updated_at","type":"i64"}
        ]}
      ]
    }
  }'
```

### 3.3 Insert data

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{
    "jsonrpc":"2.0",
    "id":3,
    "method":"data.insert",
    "params":{
      "schema":"demo",
      "table":"users",
      "rows":[
        {"id":1,"name":"Ada","updated_at":1},
        {"id":2,"name":"Linus","updated_at":1}
      ]
    }
  }'
```

### 3.4 Query select

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{
    "jsonrpc":"2.0",
    "id":4,
    "method":"query.select",
    "params":{
      "query":{
        "schema":"demo",
        "table":"users",
        "select":[{"col":"id"},{"col":"name"}],
        "order_by":[{"col":"id","dir":"asc"}]
      },
      "result_format":"objects_json"
    }
  }'
```

### 3.5 SQL compatibility endpoint + information_schema

The SQL compatibility helper endpoint is available at `POST /api/v1/sql/exec`.
It now supports virtual metadata queries over:
- `information_schema.tables`
- `information_schema.columns`

Example:

```bash
curl -s http://127.0.0.1:8080/api/v1/sql/exec \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT table_schema, table_name FROM information_schema.tables ORDER BY table_schema, table_name LIMIT 10"}'
```

### 3.6 Transaction handles (SkeinQL)

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":8,"method":"tx.begin","params":{"read_only":true}}'
```

Commit:

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":9,"method":"tx.commit","params":{"tx_id":"tx_0000000000000001"}}'
```

---

## 4) Storage + dedup stats

`stats.snapshot` now reports live dedup effectiveness from persisted table data:
- `storage.dedup_ratio`: logical bytes / unique bytes (higher means better dedup)
- `storage.logical_bytes`: total bytes before dedup
- `storage.unique_bytes`: unique bytes after content-addressed dedup
- `storage.duplicate_bytes`: bytes saved by dedup
- `storage.interned_values`: unique values currently interned

Optional persistence mode (default is `hybrid`):
- `--storage-mode json`: write/read `tables/<db>/<table>.json`
- `--storage-mode segment`: write/read `tables/<db>/<table>.rseg`
- `--storage-mode hybrid`: write both formats; read prefers `.rseg`

Compatibility:
- `SKEINDB_STORAGE_MODE=dual` remains accepted as an alias for `hybrid`.

Example:

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":5,"method":"stats.snapshot","params":{}}'
```

Top query fingerprints (by total time/count/latency):

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":6,"method":"stats.top_queries","params":{"limit":10,"sort_by":"total_ms"}}'
```

Recent slow queries:

```bash
curl -s http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":7,"method":"stats.slow_queries","params":{"min_ms":200,"limit":20}}'
```

---

## 5) QueryPatch (delta updates)

See `docs/QUERY_PATCH.md` for a complete spec.

At a high level:
1) run `query.select` and keep the returned `etag`
2) periodically call `query.patch(base_etag=...)`
3) apply the returned delta to your cached list

---

## 6) MySQL compatibility

The MySQL surface is intended as an *adoption layer* so existing software can connect using standard MySQL drivers.

Current status is documented in:
- `docs/MYSQL_COMPAT.md`

---

## 7) Where to go next

- Operator knobs: `docs/CONFIGURATION.md`
- Specs and research notes: `docs/README.md`
