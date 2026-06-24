-- Representative DBeaver-style metadata browsing statements.
SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;
SELECT table_schema, table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position;
SELECT attrelid, attname, atttypid, attnum, attnotnull FROM pg_catalog.pg_attribute ORDER BY attrelid, attnum LIMIT 50;
SELECT conname, contype, conrelid, conindid FROM pg_catalog.pg_constraint ORDER BY conname LIMIT 50;
SELECT relid, indexrelid, schemaname, relname, indexrelname, idx_scan FROM pg_catalog.pg_stat_user_indexes ORDER BY relname, indexrelname LIMIT 20;
SELECT relid, schemaname, relname, seq_scan, n_live_tup FROM pg_catalog.pg_stat_user_tables ORDER BY relname LIMIT 20;
