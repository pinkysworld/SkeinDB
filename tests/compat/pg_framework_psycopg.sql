-- Representative psycopg-style bootstrap and metadata probes.
SELECT current_database();
SELECT current_schema();
SELECT current_schemas(true);
SELECT oid, relname, relkind
FROM pg_catalog.pg_class
ORDER BY relname
LIMIT 20;
SELECT attname,
       attnum,
       attnotnull,
       atttypid
FROM pg_catalog.pg_attribute
WHERE attnum > 0
ORDER BY attrelid, attnum
LIMIT 50;
