"""psycopg3 smoke test against the SkeinDB PostgreSQL wire listener.

Exercises SCRAM-SHA-256 auth (when SMOKE_PASSWORD is set) and the extended-query
protocol that psycopg3 uses for every statement, including a parameterized
SELECT. Env: SMOKE_HOST, SMOKE_PG_PORT, SMOKE_USER, SMOKE_PASSWORD.
"""

import os
import sys

import psycopg

DB = "smoke_psycopg"


def main() -> int:
    conninfo = (
        f"host={os.environ.get('SMOKE_HOST', '127.0.0.1')} "
        f"port={int(os.environ.get('SMOKE_PG_PORT', '5432'))} "
        f"user={os.environ.get('SMOKE_USER', 'skein')} "
        f"password={os.environ.get('SMOKE_PASSWORD', '')} "
        f"dbname=skein sslmode=disable"
    )
    # autocommit so CREATE DATABASE is not wrapped in a transaction block.
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1, "SELECT 1 failed"

            cur.execute(f"CREATE DATABASE {DB}")
            cur.execute(f"CREATE TABLE {DB}.items (id INT PRIMARY KEY, label VARCHAR(64))")
            cur.execute(f"INSERT INTO {DB}.items (id, label) VALUES (1, 'alpha')")
            cur.execute(f"INSERT INTO {DB}.items (id, label) VALUES (2, 'beta')")

            cur.execute(f"SELECT id, label FROM {DB}.items ORDER BY id")
            rows = cur.fetchall()
            assert rows == [(1, "alpha"), (2, "beta")], f"unexpected rows: {rows!r}"

            cur.execute(f"SELECT label FROM {DB}.items WHERE id = %s", (2,))
            assert cur.fetchone()[0] == "beta", "parameterized query failed"

            cur.execute(f"DROP TABLE {DB}.items")
    print("psycopg3 smoke OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
