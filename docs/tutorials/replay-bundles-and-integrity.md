# Replay bundles and integrity

This guide exports a replay bundle, imports it into a replay workspace, and verifies the checksums that prove the imported snapshot matches the exported one.

Prerequisite: [Your first query (SkeinQL)](first-query.html) completed.

## 1. Create a database and some history

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"schema.create_database","params":{"db":"app"}}' | jq

for sql in \
  "CREATE TABLE IF NOT EXISTS replay_events (id INT PRIMARY KEY, payload TEXT)" \
  "INSERT INTO replay_events (id, payload) VALUES (1, 'one')" \
  "INSERT INTO replay_events (id, payload) VALUES (2, 'two')" \
  "UPDATE replay_events SET payload = 'two-updated' WHERE id = 2" \
  "DELETE FROM replay_events WHERE id = 1"
do
  curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
    -H 'Content-Type: application/json' \
    -d "{\"skeinql\":\"1.0\",\"id\":\"seed\",\"method\":\"sql.exec\",\"params\":{\"default_db\":\"app\",\"sql\":\"$sql\"}}" \
    | jq '.ok'
done
```

## 2. Export a replay bundle

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":2,"method":"maintenance.replay.export","params":{"db":"app","bundle_id":"baseline"}}' \
  | tee replay-export.json | jq '.result.bundle.manifest'
```

That file now contains the typed replay bundle under `.result.bundle`.

## 3. Import the bundle into a replay workspace

```bash
jq '{skeinql:"1.0",id:3,method:"maintenance.replay.import",params:{bundle:.result.bundle,workspace_id:"verify1"}}' replay-export.json \
  | curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
      -H 'Content-Type: application/json' \
      -d @- \
  | tee replay-import.json | jq '.result'
```

The import result returns:

- `workspace_id`
- `workspace_path`
- imported table / row / change counts
- the bundle checksum recorded for that workspace

## 4. Run integrity verification

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":4,"method":"maintenance.replay.run","params":{"workspace_id":"verify1"}}' \
  | jq '.result | {ok, expected_checksum, observed_checksum, replayed_tables, replayed_rows, replayed_changes}'
```

When `expected_checksum` and `observed_checksum` match, the imported replay workspace reproduces the exported bundle deterministically.

## 5. What this is good for

Replay bundles are useful when you want to:

- hand a deterministic database snapshot to another environment
- verify that a captured bundle replays cleanly
- investigate point-in-time behavior without touching the live working set

## 6. Admin UI path

The same flow is available in SkeinAdmin under **Time Travel & Replay**:

- export/download bundle
- import bundle
- run integrity verification
- inspect checksum summaries

## Next

- [Time-travel & replay](time-travel-replay.html)
- [Admin console tour](admin-tour.html)
- [Audit WAL](audit-wal.html)