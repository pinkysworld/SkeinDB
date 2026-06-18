-- Representative SQLAlchemy inspector-style metadata probes.
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema IN ('public', 'app')
ORDER BY table_schema, table_name;
SELECT table_schema,
       table_name,
       column_name,
       data_type,
       is_nullable,
       ordinal_position
FROM information_schema.columns
WHERE table_schema IN ('public', 'app')
ORDER BY table_schema, table_name, ordinal_position;
SELECT conname,
       contype,
       conrelid,
       conindid
FROM pg_catalog.pg_constraint
ORDER BY conname
LIMIT 50;
SELECT schemaname, tablename, indexname, indexdef
FROM pg_catalog.pg_indexes
ORDER BY schemaname, tablename, indexname
LIMIT 50;
