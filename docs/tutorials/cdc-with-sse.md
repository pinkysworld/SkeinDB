# CDC with SSE

This guide creates a table-level CDC subscription, streams change events over Server-Sent Events, and shows how to poll and acknowledge offsets over SkeinQL.

Prerequisite: [Your first query (SkeinQL)](first-query.html) completed.

## 1. Create a database and source table

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"schema.create_database","params":{"db":"app"}}' | jq

curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0","id":2,"method":"schema.create_table",
    "params":{
      "db":"app",
      "table":"events",
      "columns":[
        {"name":"id",   "type":{"kind":"u64"}, "nullable":false},
        {"name":"data", "type":{"kind":"str"}, "nullable":false}
      ],
      "primary_key":["id"]
    }}' | jq
```

## 2. Subscribe to table changes

```bash
SUB_JSON=$(curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":3,"method":"cdc.subscribe_table","params":{"db":"app","table":"events"}}')

SUB_ID=$(printf '%s' "$SUB_JSON" | jq -r '.result.sub_id')
FROM_OFFSET=$(printf '%s' "$SUB_JSON" | jq -r '.result.offset')

printf 'sub_id=%s\nfrom_offset=%s\n' "$SUB_ID" "$FROM_OFFSET"
```

The subscription result also includes the SSE URL path.

## 3. Open the SSE stream

In **terminal A**:

```bash
curl -N "http://127.0.0.1:8080/api/v1/cdc/sse/$SUB_ID"
```

Keep it running.

## 4. Insert data and watch the stream

In **terminal B**:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0","id":4,"method":"data.insert",
    "params":{
      "into":{"db":"app","table":"events"},
      "rows":[{"id":{"t":"u64","v":1},"data":{"t":"str","v":"Ada"}}]
    }}' | jq
```

Terminal A should receive an `insert` event with `db`, `table`, `op`, `pk`, `commit_ts_ms`, and `lsn` metadata.

## 5. Poll and acknowledge over RPC

You can read the same events over SkeinQL:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d "{\"skeinql\":\"1.0\",\"id\":5,\"method\":\"cdc.poll\",\"params\":{\"sub_id\":\"$SUB_ID\",\"from_offset\":$FROM_OFFSET,\"limit\":10}}" \
  | jq
```

When you are done processing those events, acknowledge the offset returned in `next_offset`:

```bash
NEXT_OFFSET=$(curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d "{\"skeinql\":\"1.0\",\"id\":6,\"method\":\"cdc.poll\",\"params\":{\"sub_id\":\"$SUB_ID\",\"from_offset\":$FROM_OFFSET,\"limit\":10}}" \
  | jq -r '.result.next_offset')

curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d "{\"skeinql\":\"1.0\",\"id\":7,\"method\":\"cdc.ack\",\"params\":{\"sub_id\":\"$SUB_ID\",\"offset\":$NEXT_OFFSET}}" \
  | jq
```

## 6. Resume after reconnect

SSE reconnect uses event ids as offsets. To resume from a prior event:

```bash
curl -N -H 'Last-Event-ID: 1' "http://127.0.0.1:8080/api/v1/cdc/sse/$SUB_ID"
```

If the reconnect cursor falls behind the retained event horizon, SkeinDB returns `resnapshot_required = true` for `cdc.poll` and emits an SSE `resnapshot` control event instead of replaying a partial stream.

## 7. Close the subscription

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d "{\"skeinql\":\"1.0\",\"id\":8,\"method\":\"cdc.close\",\"params\":{\"sub_id\":\"$SUB_ID\"}}" \
  | jq
```

## Next

- [CDC changefeed](cdc-changefeed.html)
- [Query patch protocol](query-patch.html)
- [Observability](observability.html)