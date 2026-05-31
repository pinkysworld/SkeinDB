#!/usr/bin/env bash
# Reference SkeinQL calls over plain curl. Requires a running skeindb.
#
#   ./skeinql.sh [BASE_URL]
#
# BASE_URL defaults to http://localhost:8080. Set SKEINDB_TOKEN to send a bearer
# token. Every SkeinQL request is a JSON envelope POSTed to /api/v1/rpc; always
# inspect the response `ok` flag (HTTP 200 may still contain an RPC error).
set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
AUTH=()
if [[ -n "${SKEINDB_TOKEN:-}" ]]; then
  AUTH=(-H "Authorization: Bearer ${SKEINDB_TOKEN}")
fi

rpc() {
  local method="$1" params="${2:-{}}"
  curl -sS "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -X POST "${BASE_URL}/api/v1/rpc" \
    -d "{\"skeinql\":\"1.0\",\"id\":\"req-1\",\"method\":\"${method}\",\"params\":${params}}"
  echo
}

echo "# system.capabilities"
rpc system.capabilities

echo "# query.select"
rpc query.select '{"query":"SELECT 1 AS one"}'

echo "# sql.exec (MySQL/PostgreSQL compatibility translator)"
rpc sql.exec '{"sql":"SELECT VERSION()"}'
