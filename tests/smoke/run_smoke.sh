#!/usr/bin/env bash
# Real-driver smoke matrix for the SkeinDB MySQL and PostgreSQL wire listeners.
#
# Boots `skeindb serve` on ephemeral ports with a fresh data directory and an
# auth token, then runs smoke scripts using genuine client drivers:
#   - PostgreSQL: psql, psycopg3, node-postgres
#   - MySQL:      mysql CLI, mysql2, PyMySQL
#
# A driver whose tooling is not installed is skipped, unless
# SKEINDB_SMOKE_STRICT=1 (set in CI), in which case a missing driver fails.
#
# Env knobs:
#   SKEINDB_BIN            path to the skeindb binary (else target/{debug,release})
#   SKEINDB_SMOKE_STRICT   1 to treat missing tooling as a failure
#   SKEINDB_SMOKE_TOKEN    auth token / password (default: a fixed test secret)
#   SKEINDB_SMOKE_USER     login user name (default: skein)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

STRICT="${SKEINDB_SMOKE_STRICT:-0}"
TOKEN="${SKEINDB_SMOKE_TOKEN:-smoke-matrix-secret}"
LOGIN_USER="${SKEINDB_SMOKE_USER:-skein}"
HOST="127.0.0.1"

BIN="${SKEINDB_BIN:-}"
if [ -z "${BIN}" ]; then
  if [ -x "${REPO_ROOT}/target/debug/skeindb" ]; then
    BIN="${REPO_ROOT}/target/debug/skeindb"
  elif [ -x "${REPO_ROOT}/target/release/skeindb" ]; then
    BIN="${REPO_ROOT}/target/release/skeindb"
  else
    echo "error: skeindb binary not found; set SKEINDB_BIN or run 'cargo build -p skeindb'" >&2
    exit 1
  fi
fi

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

HTTP_PORT="$(free_port)"
MYSQL_PORT="$(free_port)"
PG_PORT="$(free_port)"
CLUSTER_PORT="$(free_port)"
DATA_DIR="$(mktemp -d)"
LOG_FILE="$(mktemp)"
SERVER_PID=""

cleanup() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${DATA_DIR}" "${LOG_FILE}"
}
trap cleanup EXIT

echo "== booting skeindb serve =="
echo "   bin=${BIN}"
echo "   http=${HTTP_PORT} mysql=${MYSQL_PORT} pg=${PG_PORT} cluster=${CLUSTER_PORT}"
SKEINDB_TOKEN="${TOKEN}" "${BIN}" serve \
  --data "${DATA_DIR}" --bind "${HOST}" \
  --http "${HTTP_PORT}" --mysql "${MYSQL_PORT}" --pg "${PG_PORT}" \
  --cluster-port "${CLUSTER_PORT}" >"${LOG_FILE}" 2>&1 &
SERVER_PID=$!

deadline=$(( $(date +%s) + 60 ))
until curl -fsS "http://${HOST}:${HTTP_PORT}/health" >/dev/null 2>&1; do
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "error: server exited during startup" >&2
    cat "${LOG_FILE}" >&2
    exit 1
  fi
  if [ "$(date +%s)" -ge "${deadline}" ]; then
    echo "error: server did not become healthy within 60s" >&2
    cat "${LOG_FILE}" >&2
    exit 1
  fi
  sleep 0.25
done
echo "   server healthy"

export SMOKE_HOST="${HOST}"
export SMOKE_PG_PORT="${PG_PORT}"
export SMOKE_MYSQL_PORT="${MYSQL_PORT}"
export SMOKE_USER="${LOGIN_USER}"
export SMOKE_PASSWORD="${TOKEN}"

cd "${SCRIPT_DIR}"

PASS=0
FAIL=0
SKIP=0

run_one() {
  local label="$1"; shift
  echo "-- ${label}"
  if "$@"; then
    echo "PASS  ${label}"
    PASS=$((PASS + 1))
  else
    echo "FAIL  ${label}"
    FAIL=$((FAIL + 1))
  fi
}

skip_or_fail() {
  local label="$1"
  if [ "${STRICT}" = "1" ]; then
    echo "FAIL  ${label} (required tooling missing)"
    FAIL=$((FAIL + 1))
  else
    echo "SKIP  ${label} (tooling missing)"
    SKIP=$((SKIP + 1))
  fi
}

echo "== running driver smoke matrix =="

if command -v psql >/dev/null 2>&1; then
  run_one "pg/psql" bash pg_psql.sh
else
  skip_or_fail "pg/psql"
fi

if python3 -c 'import psycopg' >/dev/null 2>&1; then
  run_one "pg/psycopg3" python3 pg_psycopg.py
else
  skip_or_fail "pg/psycopg3"
fi

if node -e "require.resolve('pg')" >/dev/null 2>&1; then
  run_one "pg/node-postgres" node pg_node.mjs
else
  skip_or_fail "pg/node-postgres"
fi

if command -v mysql >/dev/null 2>&1; then
  run_one "mysql/cli" bash mysql_cli.sh
else
  skip_or_fail "mysql/cli"
fi

if node -e "require.resolve('mysql2')" >/dev/null 2>&1; then
  run_one "mysql/mysql2" node mysql_node.mjs
else
  skip_or_fail "mysql/mysql2"
fi

if python3 -c 'import pymysql' >/dev/null 2>&1; then
  run_one "mysql/pymysql" python3 mysql_pymysql.py
else
  skip_or_fail "mysql/pymysql"
fi

echo "== summary =="
echo "   pass=${PASS} fail=${FAIL} skip=${SKIP}"

if [ "${FAIL}" -gt 0 ]; then
  echo "-- server log tail --" >&2
  tail -n 60 "${LOG_FILE}" >&2 || true
  exit 1
fi
exit 0
