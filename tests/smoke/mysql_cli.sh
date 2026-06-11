#!/usr/bin/env bash
# mysql CLI smoke test against the SkeinDB MySQL wire listener.
#
# Exercises caching_sha2_password auth (when SMOKE_PASSWORD is set) over an
# unencrypted connection (--ssl-mode=DISABLED; the server completes the
# fast-auth path). Env: SMOKE_HOST, SMOKE_MYSQL_PORT, SMOKE_USER, SMOKE_PASSWORD.
set -euo pipefail

HOST="${SMOKE_HOST:-127.0.0.1}"
PORT="${SMOKE_MYSQL_PORT:-3306}"
DB_USER="${SMOKE_USER:-skein}"
DB="smoke_mysqlcli"

# Pass the password via the environment so it never appears in the process list.
export MYSQL_PWD="${SMOKE_PASSWORD:-}"
mysql_q() {
  mysql --host="${HOST}" --port="${PORT}" --user="${DB_USER}" \
    --protocol=TCP --ssl-mode=DISABLED --batch --skip-column-names "$@"
}

one="$(printf 'SELECT 1;' | mysql_q)"
[ "${one}" = "1" ] || { echo "SELECT 1 returned '${one}'" >&2; exit 1; }

mysql_q <<SQL
CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};
DROP TABLE IF EXISTS items;
CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(64));
INSERT INTO items (id, label) VALUES (1, 'alpha');
INSERT INTO items (id, label) VALUES (2, 'beta');
SQL

rows="$(mysql_q --database="${DB}" -e 'SELECT id, label FROM items ORDER BY id')"
expected=$'1\talpha\n2\tbeta'
[ "${rows}" = "${expected}" ] || { echo "unexpected rows:" >&2; echo "${rows}" >&2; exit 1; }

mysql_q --database="${DB}" -e 'DROP TABLE items' >/dev/null
echo "mysql CLI smoke OK"
