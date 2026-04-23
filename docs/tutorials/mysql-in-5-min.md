# MySQL in 5 minutes

SkeinDB speaks the **MySQL wire protocol** out of the box. If you already know MySQL, you can connect with any MySQL client, migrate a schema, and run real queries without installing anything SkeinDB-specific.

Prerequisite: [Quickstart](quickstart.html) completed; `skeindb serve` running with `--mysql 3306`.

## 1. Connect

```bash
mysql -h 127.0.0.1 -P 3306 -u root
```

```sql
mysql> SELECT VERSION();
+--------------------+
| VERSION()          |
+--------------------+
| 8.0.0-skeindb      |
+--------------------+
```

## 2. Create a schema

```sql
CREATE DATABASE shop;
USE shop;

CREATE TABLE products (
    id       BIGINT PRIMARY KEY,
    sku      VARCHAR(32) NOT NULL UNIQUE,
    name     VARCHAR(255) NOT NULL,
    price    DECIMAL(10,2) NOT NULL,
    in_stock BOOLEAN DEFAULT TRUE
);

INSERT INTO products (id, sku, name, price) VALUES
  (1, 'SK-001', 'SkeinDB hoodie',  59.00),
  (2, 'SK-002', 'SkeinDB mug',     15.00),
  (3, 'SK-003', 'SkeinDB sticker', 3.00);

SELECT * FROM products WHERE price < 20 ORDER BY price;
```

## 3. Prepared statements

```sql
PREPARE p FROM 'SELECT name, price FROM products WHERE price >= ?';
SET @min = 10;
EXECUTE p USING @min;
DEALLOCATE PREPARE p;
```

## 4. Transactions

```sql
START TRANSACTION;
UPDATE products SET price = price * 1.1 WHERE id = 1;
SAVEPOINT sp1;
UPDATE products SET price = 0 WHERE id = 2;
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
```

## 5. What is supported

See the [MySQL compatibility](mysql-compat.html) page for the complete, authoritative matrix. In short:

- **DDL**: `CREATE / DROP / ALTER TABLE`, primary & unique keys, numeric/string/bool/date types.
- **DML**: `INSERT / UPDATE / DELETE`, `REPLACE`, `INSERT ... ON DUPLICATE KEY UPDATE`.
- **DQL**: `SELECT` with `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `LIMIT/OFFSET`, joins, subqueries, CTEs for the supported subset.
- **Transactions**: `BEGIN / COMMIT / ROLLBACK`, `SAVEPOINT`, `SET TRANSACTION ISOLATION LEVEL`.
- **Admin**: `SHOW DATABASES / TABLES / COLUMNS / VARIABLES / STATUS`, `USE`, `SELECT DATABASE()`.
- **Prepared statements** (binary protocol) with metadata for common projections.
- **Compatibility telemetry**: unsupported features are surfaced in the admin panel and in [Compatibility telemetry](compatibility.html).

## 6. Migrate an existing schema

```bash
mysqldump --no-data mysql-source mydb > schema.sql
mysql -h 127.0.0.1 -P 3306 -u root mydb < schema.sql
```

Then watch **Admin → Compatibility** for any statement SkeinDB flagged as unsupported. The [telemetry + migration](telemetry-and-migration.html) advisor will suggest a SkeinQL equivalent.

## Next

- [Your first query (SkeinQL)](first-query.html) — the native, strongly typed RPC.
- [PostgreSQL compatibility](pg-compat.html) — partial pg v3 wire is also available.
- [Configuration](configuration.html) — tuning ports, auth, and storage paths.
