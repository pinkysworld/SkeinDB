# PostgreSQL in 5 minutes

This guide brings up SkeinDB's PostgreSQL listener, connects with `psql`, and exercises the current PG-compatible surface.

Prerequisite: [Quickstart](quickstart.html) completed.

## 1. Start SkeinDB with the PG listener enabled

```bash
cargo run -- serve --data ./data --http 8080 --mysql 3306 --pg 5432
```

If you set `SKEINDB_TOKEN`, use the same value as the PostgreSQL password.

## 2. Connect with `psql`

When no token is set, the current baseline uses trust auth:

```bash
psql "host=127.0.0.1 port=5432 user=skein dbname=app sslmode=disable"
```

With a token:

```bash
PGPASSWORD="$SKEINDB_TOKEN" \
  psql "host=127.0.0.1 port=5432 user=skein dbname=app sslmode=disable"
```

`sslmode=disable` is recommended for now because SkeinDB explicitly rejects PostgreSQL SSL negotiation with `N`.

## 3. Smoke-test the connection

Once connected, run a few bootstrap queries:

```sql
SELECT 1;
SELECT version();
SHOW server_version;
SELECT current_database(), current_schema();
```

## 4. Create a table and insert data

```sql
CREATE TABLE IF NOT EXISTS events (
  id BIGINT PRIMARY KEY,
  payload TEXT NOT NULL
);

INSERT INTO events (id, payload)
VALUES (1, 'hello'), (2, 'from pg');

SELECT id, payload FROM events ORDER BY id;
```

The PG listener routes supported statements into the same shared SQL engine used by MySQL and SkeinQL.

## 5. Try a few PG-flavoured features that are already wired up

```sql
SELECT 'skein' || '-db' AS concat;
SELECT gen_random_uuid();
SELECT date_trunc('day', clock_timestamp());
SELECT string_agg(payload, ', ' ORDER BY id) FROM events;
```

The current baseline also supports text-format extended query flow, regex operators, JSON access operators, PostgreSQL-style casts, array constructors, and `CREATE SCHEMA` compatibility rewriting.

## 6. Know the current limits

- The listener is a **partial baseline**, not full PostgreSQL compatibility.
- SSL/TLS negotiation is not implemented yet.
- SCRAM-SHA-256 is used when `SKEINDB_TOKEN` is set; trust auth remains the default when it is unset.
- Some PostgreSQL-native features like `COPY`, partial portal suspension, broader dialect/catalog parity, and production-grade driver matrices are still open work.

For the current implementation surface and gaps, read [PostgreSQL compatibility](pg-compat.html).

## Next

- [PostgreSQL compatibility](pg-compat.html)
- [Admin console tour](admin-tour.html)
- [Monitoring and metrics](monitoring-and-metrics.html)