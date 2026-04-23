# Your first query (SkeinQL)

This tutorial walks through creating a database + table, inserting rows, and querying them using **SkeinQL** — SkeinDB's native typed RPC API.

Prerequisite: [Quickstart](quickstart.html) completed and `skeindb` running on `http://127.0.0.1:8080`.

## 1. The envelope

SkeinQL is a typed JSON-RPC-like protocol served at `POST /api/v1/rpc`. Every request has this envelope:

```json
{"skeinql": "1.0", "id": <n>, "method": "<family>.<method>", "params": { ... }}
```

> **Note** — SkeinQL is *not* JSON-RPC 2.0. The protocol marker is `"skeinql": "1.0"`, not `"jsonrpc": "2.0"`. The response shape is `{"id": ..., "ok": true|false, "result"|"error": ...}`.

Ping the server:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"ping","params":{}}' | jq
```

## 2. Create a database + table

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":2,"method":"schema.create_database","params":{"db":"demo"}}' | jq
```

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0","id":3,"method":"schema.create_table",
    "params":{
      "db":"demo",
      "table":"users",
      "columns":[
        {"name":"id",    "type":{"kind":"i64"},    "nullable":false},
        {"name":"email", "type":{"kind":"string"}, "nullable":false},
        {"name":"age",   "type":{"kind":"i64"},    "nullable":true}
      ],
      "primary_key":["id"]
    }}' | jq
```

`type.kind` accepts the typical primitives: `i64`, `u64`, `f64`, `bool`, `string`, `bytes`, `date`, `time`, `datetime`, `uuid`, `dec`, `json`, `embedding`.

## 3. Insert rows

Values are tagged JSON literals: `{"t":"<type>","v":<value>}`.

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0","id":4,"method":"data.insert",
    "params":{
      "into":{"db":"demo","table":"users"},
      "rows":[
        {"id":{"t":"i64","v":1},"email":{"t":"str","v":"ada@example.com"},  "age":{"t":"i64","v":36}},
        {"id":{"t":"i64","v":2},"email":{"t":"str","v":"grace@example.com"},"age":{"t":"i64","v":85}},
        {"id":{"t":"i64","v":3},"email":{"t":"str","v":"linus@example.com"},"age":{"t":"null"}}
      ]
    }}' | jq
```

The response returns `{ "affected": 3, "last_insert_id": 0 }`.

## 4. Query

### Point lookup

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0","id":5,"method":"data.get",
    "params":{"from":{"db":"demo","table":"users"},"key":{"id":{"t":"i64","v":1}}}
  }' | jq
```

### Scan / filter

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0","id":6,"method":"query.select",
    "params":{
      "from":{"db":"demo","table":"users"},
      "select":[{"col":"id"},{"col":"email"}],
      "where":{"op":"gte","left":{"col":"age"},"right":{"t":"i64","v":30}},
      "order_by":[{"col":"id","dir":"asc"}]
    }}' | jq
```

## 5. Try the SQL surface

The same data is visible through the MySQL wire:

```bash
mysql -h 127.0.0.1 -P 3306 -u root demo -e "SELECT id, email, age FROM users;"
```

And through the web console at `http://127.0.0.1:8080/admin` → **SQL Console**.

## 6. Cleanup

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":7,"method":"schema.drop_table","params":{"db":"demo","table":"users"}}' | jq
```

## Reference

- [SkeinQL reference](skeinql.html) — every method and family in detail.
- [SkeinQL research extensions](skeinql-research.html) — opt-in experimental methods.
- [OpenAPI schema](https://github.com/pinkysworld/SkeinDB/blob/main/openapi/skeinql.yaml) — machine-readable.

## Next

- [MySQL in 5 minutes](mysql-in-5-min.html)
- [Admin console tour](admin-tour.html)
- [Query patch + coalescing](query-patch.html) — how to stream deltas instead of full results.
