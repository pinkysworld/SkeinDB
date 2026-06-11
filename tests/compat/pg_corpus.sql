-- PostgreSQL wire-compatibility corpus
-- Every statement here is executed end-to-end over the live PG listener by
-- `cluster_rpc::pg_compat_corpus_roundtrip` and must complete WITHOUT an
-- ErrorResponse. A curated subset of deterministic literal/scalar expressions
-- additionally has exact-value assertions in that test. Only add statements the
-- shared engine actually supports over the PG path (see docs/PG_COMPAT.md).

-- ===== Startup / session probes =====
SELECT 1;
SELECT version();
SELECT current_database();
SELECT current_schema();
SHOW server_version;
SHOW server_version_num;
SHOW standard_conforming_strings;
SHOW max_identifier_length;
SHOW transaction isolation level;

-- ===== Core DDL/DML + savepoints =====
CREATE DATABASE app;
CREATE TABLE app.pg_corpus_users (id BIGINT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY (id));
INSERT INTO app.pg_corpus_users (id, name) VALUES (1, 'Ada');
INSERT INTO app.pg_corpus_users (id, name) VALUES (2, 'Grace');
SELECT COUNT(*) FROM app.pg_corpus_users;
SELECT name FROM app.pg_corpus_users WHERE id = 2;

BEGIN;
SAVEPOINT before_insert;
INSERT INTO app.pg_corpus_users (id, name) VALUES (3, 'Linus');
ROLLBACK TO SAVEPOINT before_insert;
RELEASE SAVEPOINT before_insert;
COMMIT;

SELECT COUNT(*) FROM app.pg_corpus_users;

-- ===== Type casts (:: and CAST) =====
SELECT '42'::int;
SELECT '42'::integer;
SELECT 3::bigint;
SELECT '3.14'::numeric;
SELECT 'hello'::text;
SELECT 1::text;
SELECT '2024-01-15'::date;
SELECT 't'::bool;
SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid;
SELECT CAST('42' AS INTEGER);

-- ===== Dollar-quoted string literals =====
SELECT $$hello world$$;
SELECT $$it's a test$$;
SELECT $tag$body with $$ inside$tag$;

-- ===== ARRAY constructor and array helpers =====
SELECT ARRAY[1, 2, 3];
SELECT ARRAY['a', 'b', 'c'];
SELECT array_length(ARRAY[1,2,3], 1);
SELECT string_to_array('a,b,c', ',');

-- ===== JSON / JSONB operators =====
SELECT '{"a": 1}'::json -> 'a';
SELECT '{"a": 1}'::json ->> 'a';
SELECT '{"a": {"b": 2}}'::jsonb -> 'a';
SELECT '{"a": {"b": 2}}'::jsonb -> 'a' ->> 'b';

-- ===== Working tables for query-shape coverage =====
CREATE TABLE app.items (id INT NOT NULL, label VARCHAR(64) NOT NULL, qty INT NOT NULL, price DOUBLE NOT NULL, PRIMARY KEY (id));
INSERT INTO app.items (id, label, qty, price) VALUES (1, 'apple', 10, 1.5);
INSERT INTO app.items (id, label, qty, price) VALUES (2, 'banana', 5, 0.75);
INSERT INTO app.items (id, label, qty, price) VALUES (3, 'cherry', 20, 3.25);
INSERT INTO app.items (id, label, qty, price) VALUES (4, 'date', 7, 2.0), (5, 'elderberry', 3, 4.0);
CREATE TABLE app.orders (oid INT NOT NULL, item_id INT NOT NULL, amount INT NOT NULL, PRIMARY KEY (oid));
INSERT INTO app.orders (oid, item_id, amount) VALUES (10, 1, 2);
INSERT INTO app.orders (oid, item_id, amount) VALUES (11, 1, 4);
INSERT INTO app.orders (oid, item_id, amount) VALUES (12, 3, 1);

-- ===== String concatenation (||) on columns =====
SELECT label || '!' FROM app.items WHERE id = 1;
SELECT concat('a', 'b', 'c');

-- ===== Regular-expression match on columns (~ and ~*) =====
SELECT label FROM app.items WHERE label ~ '^b' ORDER BY id;
SELECT label FROM app.items WHERE label ~* '^B' ORDER BY id;

-- ===== FETCH FIRST =====
SELECT label FROM app.items ORDER BY id FETCH FIRST 1 ROW ONLY;
SELECT label FROM app.items ORDER BY id FETCH FIRST 2 ROWS ONLY;

-- ===== Aggregates =====
SELECT COUNT(*) FROM app.items;
SELECT COUNT(DISTINCT item_id) FROM app.orders;
SELECT SUM(qty) FROM app.items;
SELECT AVG(price) FROM app.items;
SELECT string_agg(label, ', ') FROM app.items;
SELECT array_agg(id) FROM app.items;

-- ===== GROUP BY / HAVING =====
SELECT item_id, COUNT(*) FROM app.orders GROUP BY item_id;
SELECT item_id, SUM(amount) FROM app.orders GROUP BY item_id HAVING SUM(amount) > 2;

-- ===== ORDER BY / LIMIT / OFFSET / DISTINCT =====
SELECT label FROM app.items ORDER BY id DESC;
SELECT label FROM app.items ORDER BY id LIMIT 2;
SELECT label FROM app.items ORDER BY id LIMIT 1 OFFSET 1;
SELECT DISTINCT item_id FROM app.orders;

-- ===== Joins =====
SELECT i.label, o.amount FROM app.items AS i INNER JOIN app.orders AS o ON i.id = o.item_id ORDER BY o.oid;
SELECT i.label, o.amount FROM app.items AS i LEFT JOIN app.orders AS o ON i.id = o.item_id ORDER BY i.id, o.oid;

-- ===== CASE / COALESCE / NULLIF =====
SELECT CASE WHEN qty > 8 THEN 'high' ELSE 'low' END FROM app.items WHERE id = 1;
SELECT COALESCE(NULL, 'fallback');
SELECT NULLIF(1, 1);
SELECT coalesce(qty, 0) FROM app.items WHERE id = 1;

-- ===== Predicates =====
SELECT label FROM app.items WHERE qty BETWEEN 5 AND 15 ORDER BY id;
SELECT label FROM app.items WHERE label LIKE 'a%';
SELECT label FROM app.items WHERE id IN (1, 3) ORDER BY id;
SELECT label FROM app.items WHERE id NOT IN (2) ORDER BY id;
SELECT label FROM app.items WHERE qty IS NOT NULL ORDER BY id;

-- ===== Subqueries / derived tables / CTE =====
SELECT label FROM app.items WHERE id = (SELECT MIN(id) FROM app.items);
SELECT label FROM app.items WHERE id IN (SELECT item_id FROM app.orders) ORDER BY id;
SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM app.items) AS sub;
WITH busy AS (SELECT item_id, COUNT(*) AS c FROM app.orders GROUP BY item_id) SELECT item_id FROM busy WHERE c > 1;

-- ===== Set operations =====
SELECT id FROM app.items WHERE id = 1 UNION SELECT id FROM app.items WHERE id = 2;

-- ===== Scalar string / math functions =====
SELECT lower('ABC');
SELECT upper('abc');
SELECT length('hello');
SELECT char_length('hello');
SELECT trim('  x  ');
SELECT substring('hello', 2, 3);
SELECT substring('hello', 2);
SELECT replace('aaa', 'a', 'b');
SELECT left('hello', 2);
SELECT right('hello', 2);
SELECT abs(-5);
SELECT round(3.14159, 2);
SELECT ceil(1.2);
SELECT floor(1.8);
SELECT power(2, 3);
SELECT mod(10, 3);
SELECT greatest(1, 2, 3);
SELECT least(3, 1, 2);
SELECT split_part('a,b,c', ',', 2);
SELECT starts_with('alphabet', 'alph');

-- ===== PostgreSQL-specific functions =====
SELECT pg_typeof(1);
SELECT date_trunc('day', '2024-01-15 10:30:00'::timestamp);
SELECT gen_random_uuid();
SELECT current_date;
SELECT current_timestamp;
SELECT to_char(qty, '999') FROM app.items WHERE id = 1;

-- ===== Math expressions =====
SELECT 2 + 3 * 4;
SELECT 10 % 3;

-- ===== DML: UPDATE / DELETE / RETURNING / ON CONFLICT =====
CREATE TABLE app.mutate_t (id INT NOT NULL, label VARCHAR(64) NOT NULL, PRIMARY KEY (id));
INSERT INTO app.mutate_t (id, label) VALUES (1, 'one');
INSERT INTO app.mutate_t (id, label) VALUES (2, 'two');
UPDATE app.mutate_t SET label = 'updated' WHERE id = 1;
DELETE FROM app.mutate_t WHERE id = 2;
INSERT INTO app.mutate_t (id, label) VALUES (3, 'three') RETURNING id;
UPDATE app.mutate_t SET label = 'changed' WHERE id = 1 RETURNING label;
DELETE FROM app.mutate_t WHERE id = 3 RETURNING id;
INSERT INTO app.mutate_t (id, label) VALUES (1, 'conflict') ON CONFLICT (id) DO UPDATE SET label = 'upserted';
SELECT label FROM app.mutate_t WHERE id = 1;
