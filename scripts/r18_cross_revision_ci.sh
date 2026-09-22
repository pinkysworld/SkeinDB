#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
BASE_SHA="${BASE_SHA:?BASE_SHA must be set to the pull request base commit}"
HEAD_SHA="${HEAD_SHA:-$(git rev-parse HEAD)}"
OUT_DIR="${R18_OUT_DIR:-$ROOT/target/r18-cross-revision}"
TMP_ROOT="${RUNNER_TEMP:-/tmp}/skeindb-r18-cross-${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-1}"
BASE_WORKTREE="$TMP_ROOT/base"
BASE_TARGET="$TMP_ROOT/base-target"
HEAD_TARGET="$TMP_ROOT/head-target"
BIN_DIR="$TMP_ROOT/bin"
DATA_DIR="$TMP_ROOT/data"
HTTP_PORT="${R18_HTTP_PORT:-18080}"
CLUSTER_PORT="${R18_CLUSTER_PORT:-19090}"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ -d "$BASE_WORKTREE" ]]; then
    git -C "$ROOT" worktree remove --force "$BASE_WORKTREE" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

rm -rf "$OUT_DIR" "$TMP_ROOT"
mkdir -p "$OUT_DIR" "$BIN_DIR" "$DATA_DIR"

actual_head="$(git -C "$ROOT" rev-parse HEAD)"
if [[ "$actual_head" != "$HEAD_SHA" ]]; then
  echo "error: checked-out HEAD $actual_head does not match requested HEAD_SHA $HEAD_SHA" >&2
  exit 2
fi

echo "R18 cross-revision replay"
echo "  base: $BASE_SHA"
echo "  head: $HEAD_SHA"

git -C "$ROOT" worktree add --detach "$BASE_WORKTREE" "$BASE_SHA"

echo "::group::Build base SkeinDB"
(
  cd "$BASE_WORKTREE"
  CARGO_TARGET_DIR="$BASE_TARGET" cargo build -p skeindb --bin skeindb
)
cp "$BASE_TARGET/debug/skeindb" "$BIN_DIR/skeindb-base"
echo "::endgroup::"

echo "::group::Build head SkeinDB"
(
  cd "$ROOT"
  CARGO_TARGET_DIR="$HEAD_TARGET" cargo build -p skeindb --bin skeindb
)
cp "$HEAD_TARGET/debug/skeindb" "$BIN_DIR/skeindb-head"
echo "::endgroup::"

BASE_BIN="$BIN_DIR/skeindb-base"
HEAD_BIN="$BIN_DIR/skeindb-head"
SERVER_LOG="$OUT_DIR/base-server.log"

echo "::group::Create replay workload with base binary"
"$BASE_BIN" serve   --data "$DATA_DIR"   --http "$HTTP_PORT"   --mysql 0   --pg 0   --cluster-port "$CLUSTER_PORT"   >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$HTTP_PORT/api/v1/rpc"       -H 'content-type: application/json'       -d '{"skeinql":"1.0","id":1,"method":"system.version","params":{}}'       >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    break
  fi
  sleep 1
done

if [[ "$ready" -ne 1 ]]; then
  echo "base server failed to become ready" >&2
  cat "$SERVER_LOG" >&2 || true
  exit 3
fi

rpc() {
  local payload="$1"
  local response
  response="$(curl -fsS "http://127.0.0.1:$HTTP_PORT/api/v1/rpc"     -H 'content-type: application/json'     -d "$payload")"
  printf '%s' "$response" | python3 -c '
import json, sys
r = json.load(sys.stdin)
if not r.get("ok", False):
    raise SystemExit("SkeinQL request failed: " + json.dumps(r, sort_keys=True))
'
}

rpc '{"skeinql":"1.0","id":2,"method":"schema.create_database","params":{"db":"r18_ci"}}'
rpc '{"skeinql":"1.0","id":3,"method":"schema.create_table","params":{"db":"r18_ci","table":"items","primary_key":["id"],"columns":[{"name":"id","type":{"kind":"i64"},"nullable":false},{"name":"bucket","type":{"kind":"string"},"nullable":false},{"name":"payload","type":{"kind":"string"},"nullable":false},{"name":"revision","type":{"kind":"i64"},"nullable":false}]}}'

python3 - <<'PY' >"$TMP_ROOT/insert-1.json"
import json
rows = []
for i in range(1, 9):
    rows.append({
        "id": {"t": "i64", "v": i},
        "bucket": {"t": "str", "v": "shared-bucket"},
        "payload": {"t": "str", "v": "skeindb-repeated-payload-" + ("A" * 96)},
        "revision": {"t": "i64", "v": 1},
    })
print(json.dumps({
    "skeinql": "1.0",
    "id": 4,
    "method": "data.insert",
    "params": {"into": {"db": "r18_ci", "table": "items"}, "rows": rows},
}))
PY
rpc "$(cat "$TMP_ROOT/insert-1.json")"
sleep 0.03

python3 - <<'PY' >"$TMP_ROOT/insert-2.json"
import json
rows = []
for i in range(9, 17):
    rows.append({
        "id": {"t": "i64", "v": i},
        "bucket": {"t": "str", "v": "shared-bucket" if i % 2 else "secondary-bucket"},
        "payload": {"t": "str", "v": "skeindb-repeated-payload-" + ("A" * 96)},
        "revision": {"t": "i64", "v": 2},
    })
print(json.dumps({
    "skeinql": "1.0",
    "id": 5,
    "method": "data.insert",
    "params": {"into": {"db": "r18_ci", "table": "items"}, "rows": rows},
}))
PY
rpc "$(cat "$TMP_ROOT/insert-2.json")"
sleep 0.03

python3 - <<'PY' >"$TMP_ROOT/insert-3.json"
import json
rows = []
for i in range(17, 21):
    rows.append({
        "id": {"t": "i64", "v": i},
        "bucket": {"t": "str", "v": "tail"},
        "payload": {"t": "str", "v": "skeindb-tail-" + str(i)},
        "revision": {"t": "i64", "v": 3},
    })
print(json.dumps({
    "skeinql": "1.0",
    "id": 6,
    "method": "data.insert",
    "params": {"into": {"db": "r18_ci", "table": "items"}, "rows": rows},
}))
PY
rpc "$(cat "$TMP_ROOT/insert-3.json")"

rpc '{"skeinql":"1.0","id":7,"method":"query.select","params":{"query":{"body":{"select":{"projection":[{"expr":{"col":"id"}},{"expr":{"col":"bucket"}},{"expr":{"col":"payload"}}],"from":[{"db":"r18_ci","table":"items"}]}},"order_by":[{"expr":{"col":"id"},"dir":"asc"}]},"result_format":"objects_json"}}'

kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
echo "::endgroup::"

BUNDLE="$OUT_DIR/base-generated.sreplay"
BASE_REPORT="$OUT_DIR/base-report.json"
HEAD_REPORT="$OUT_DIR/head-report.json"
COMPARISON="$OUT_DIR/comparison.json"

echo "::group::Generate base replay bundle"
"$BASE_BIN" replay export --data "$DATA_DIR" --db r18_ci --out "$BUNDLE"
"$BASE_BIN" replay run --bundle "$BUNDLE" --json --out "$BASE_REPORT"
echo "::endgroup::"

echo "::group::Replay identical bundle with head binary"
"$HEAD_BIN" replay run --bundle "$BUNDLE" --json --out "$HEAD_REPORT"
echo "::endgroup::"

echo "::group::Compare base and head replay reports"
"$HEAD_BIN" replay compare   --baseline "$BASE_REPORT"   --candidate "$HEAD_REPORT"   --max-p95-delta-ms 5   --max-p99-delta-ms 10   --max-span-delta-ms 20   --max-disk-bytes-delta 65536   --max-missing-hot-tables-delta 0   --out "$COMPARISON"
echo "::endgroup::"

BASE_SHA="$BASE_SHA" HEAD_SHA="$HEAD_SHA" BUNDLE="$BUNDLE" python3 - <<'PY' >"$OUT_DIR/metadata.json"
import hashlib, json, os
bundle = os.environ["BUNDLE"]
with open(bundle, "rb") as f:
    digest = hashlib.sha256(f.read()).hexdigest()
print(json.dumps({
    "format": "skein.r18.cross_revision.v1",
    "base_sha": os.environ["BASE_SHA"],
    "head_sha": os.environ["HEAD_SHA"],
    "bundle_sha256": digest,
}, indent=2, sort_keys=True))
PY

echo "R18 cross-revision replay passed."
echo "Artifacts: $OUT_DIR"
