-- Representative pgAdmin-style introspection statements.
SELECT nspname, oid FROM pg_catalog.pg_namespace ORDER BY nspname;
SELECT datname, datistemplate FROM pg_catalog.pg_database ORDER BY datname;
SELECT oid, relname, relkind, relnamespace FROM pg_catalog.pg_class ORDER BY relname LIMIT 20;
SELECT schemaname, tablename, hasindexes FROM pg_catalog.pg_tables ORDER BY tablename LIMIT 20;
SELECT schemaname, tablename, indexname FROM pg_catalog.pg_indexes ORDER BY tablename, indexname LIMIT 20;
SELECT relid, schemaname, relname, n_live_tup FROM pg_catalog.pg_stat_all_tables ORDER BY relname LIMIT 20;
SELECT relid, indexrelid, schemaname, relname, indexrelname, idx_scan FROM pg_catalog.pg_stat_all_indexes ORDER BY relname, indexrelname LIMIT 20;
SELECT locktype, database, relation, mode, granted FROM pg_catalog.pg_locks LIMIT 20;
