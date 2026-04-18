-- PostgreSQL wire-compatibility corpus
-- Keep this focused on the current PG baseline: startup probes, shared-engine SQL,
-- and transaction/savepoint behavior.

SELECT 1;
SELECT version();
SELECT current_database();
SELECT current_schema();
SHOW server_version;
SHOW server_version_num;
SHOW standard_conforming_strings;
SHOW max_identifier_length;
SHOW transaction isolation level;

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