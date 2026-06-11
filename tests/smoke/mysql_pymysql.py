"""PyMySQL smoke test against the SkeinDB MySQL wire listener.

Exercises caching_sha2_password auth (when SMOKE_PASSWORD is set) over an
unencrypted connection (the server completes the fast-auth path). Env:
SMOKE_HOST, SMOKE_MYSQL_PORT, SMOKE_USER, SMOKE_PASSWORD.
"""

import os
import sys

import pymysql

DB = "smoke_pymysql"


def main() -> int:
    conn = pymysql.connect(
        host=os.environ.get("SMOKE_HOST", "127.0.0.1"),
        port=int(os.environ.get("SMOKE_MYSQL_PORT", "3306")),
        user=os.environ.get("SMOKE_USER", "skein"),
        password=os.environ.get("SMOKE_PASSWORD", ""),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            # The MySQL text protocol returns scalar values as strings, so
            # compare loosely (the smoke test validates round-trips, not the
            # driver's type coercion).
            assert str(cur.fetchone()[0]) == "1", "SELECT 1 failed"

            cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
            cur.execute(f"USE {DB}")
            cur.execute("DROP TABLE IF EXISTS items")
            cur.execute("CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(64))")
            cur.execute("INSERT INTO items (id, label) VALUES (1, 'alpha')")
            cur.execute("INSERT INTO items (id, label) VALUES (2, 'beta')")

            cur.execute("SELECT id, label FROM items ORDER BY id")
            rows = [(str(r[0]), r[1]) for r in cur.fetchall()]
            assert rows == [("1", "alpha"), ("2", "beta")], f"unexpected rows: {rows!r}"

            cur.execute("SELECT label FROM items WHERE id = %s", (2,))
            assert cur.fetchone()[0] == "beta", "parameterized query failed"

            cur.execute("DROP TABLE items")
    finally:
        conn.close()
    print("PyMySQL smoke OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
