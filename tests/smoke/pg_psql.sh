#!/usr/bin/env bash
# psql CLI smoke test against the SkeinDB PostgreSQL wire listener.
#
# Exercises SCRAM-SHA-256 auth (when SMOKE_PASSWORD is set) over the simple-query
# protocol. Env: SMOKE_HOST, SMOKE_PG_PORT, SMOKE_USER, SMOKE_PASSWORD.
set -euo pipefail

HOST="${SMOKE_HOST:-127.0.0.1}"
PORT="${SMOKE_PG_PORT:-5432}"
DB_USER="${SMOKE_USER:-skein}"
DB="smoke_psql"

export PGPASSWORD="${SMOKE_PASSWORD:-}"
CONNINFO="host=${HOST} port=${PORT} user=${DB_USER} dbname=skein sslmode=disable"
psql_q() { psql "${CONNINFO}" -v ON_ERROR_STOP=1 -At "$@"; }

one="$(psql_q -c 'SELECT 1')"
[ "${one}" = "1" ] || { echo "SELECT 1 returned '${one}'" >&2; exit 1; }

psql_q -c "CREATE DATABASE ${DB}" >/dev/null
psql_q -c "CREATE TABLE ${DB}.items (id INT PRIMARY KEY, label VARCHAR(64))" >/dev/null
psql_q -c "INSERT INTO ${DB}.items (id, label) VALUES (1, 'alpha')" >/dev/null
psql_q -c "INSERT INTO ${DB}.items (id, label) VALUES (2, 'beta')" >/dev/null

rows="$(psql_q -c "SELECT id, label FROM ${DB}.items ORDER BY id")"
expected=$'1|alpha\n2|beta'
[ "${rows}" = "${expected}" ] || { echo "unexpected rows:" >&2; echo "${rows}" >&2; exit 1; }

psql_q -c "DROP TABLE ${DB}.items" >/dev/null
echo "psql smoke OK"
