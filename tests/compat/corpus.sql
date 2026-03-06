-- SkeinDB Compatibility Corpus v0.1

SELECT 1;
SELECT VERSION();
SELECT DATABASE();
SELECT @@sql_mode;
SELECT @@lower_case_table_names;
SELECT @@version_comment LIMIT 1;
SELECT @@version_comment LIMIT 0,1;
SELECT @@version_comment LIMIT 1 OFFSET 1;

SHOW VARIABLES LIKE 'sql_mode';
SHOW VARIABLES LIKE 'lower_case_table_names';
SHOW VARIABLES LIKE 'sql_auto_is_null';
SHOW VARIABLES LIKE 'time_zone';
SHOW VARIABLES LIKE 'transaction_isolation';
SHOW VARIABLES LIKE 'character_set_%';
SHOW VARIABLES LIKE 'collation_%';
SHOW VARIABLES;
SHOW SESSION VARIABLES LIKE 'sql_mode';
SHOW GLOBAL VARIABLES WHERE Variable_name = 'time_zone';
SHOW STATUS;
SHOW GLOBAL STATUS LIKE 'threads_%';
SHOW CHARACTER SET LIKE 'utf8mb4';
SHOW COLLATION WHERE Charset = 'utf8mb4';

SELECT @@transaction_isolation;
SELECT @@sql_auto_is_null;
SELECT @@character_set_server;
SELECT @@collation_database;

SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;
SET SESSION sql_mode = '';
SET SQL_AUTO_IS_NULL = 0;
SET SESSION sql_notes = 0;
SET time_zone = '+00:00';
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET SESSION transaction_read_only = OFF;

CREATE DATABASE IF NOT EXISTS skein_test;
USE skein_test;

DROP TABLE IF EXISTS wp_options;
DROP TABLE IF EXISTS wp_posts;
DROP TABLE IF EXISTS wp_users;

CREATE TABLE wp_options (
  option_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  option_name VARCHAR(191) NOT NULL,
  option_value LONGTEXT NOT NULL,
  autoload VARCHAR(20) NOT NULL DEFAULT 'yes',
  PRIMARY KEY (option_id),
  UNIQUE KEY option_name (option_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

CREATE TABLE wp_posts (
  ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  post_author BIGINT UNSIGNED NOT NULL DEFAULT 0,
  post_date DATETIME NOT NULL,
  post_status VARCHAR(20) NOT NULL DEFAULT 'publish',
  post_title TEXT NOT NULL,
  PRIMARY KEY (ID),
  KEY post_status (post_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

ALTER TABLE wp_posts
  ADD COLUMN post_name VARCHAR(200) NOT NULL DEFAULT '' AFTER post_title;

ALTER TABLE wp_posts
  ADD KEY post_author (post_author);

CREATE TABLE wp_users (
  id BIGINT UNSIGNED NOT NULL,
  user_login VARCHAR(60) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

LOCK TABLES wp_options WRITE;
UNLOCK TABLES;

SHOW DATABASES;
SHOW TABLES FROM skein_test;
SHOW FULL TABLES FROM skein_test;
SHOW FULL TABLES FROM skein_test WHERE Table_type = 'BASE TABLE';
SHOW TABLES FROM skein_test LIKE 'wp_%';

SHOW TABLE STATUS FROM skein_test LIKE 'wp_posts';
SHOW FULL COLUMNS FROM wp_options;
SHOW INDEX FROM wp_options;
SHOW INDEX FROM wp_posts;
SHOW KEYS FROM wp_posts;
SHOW CREATE TABLE wp_options;
SHOW CREATE TABLE wp_posts;
DESCRIBE wp_posts;

SELECT table_name
  FROM information_schema.tables
 WHERE table_schema = 'skein_test';

SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema='skein_test' AND table_name='wp_posts'
 ORDER BY ordinal_position;

INSERT INTO wp_options (option_name, option_value, autoload)
VALUES
  ('siteurl', 'https://example.com', 'yes'),
  ('home', 'https://example.com', 'yes'),
  ('blogname', 'SkeinDB Test', 'yes');

INSERT INTO wp_options (option_name, option_value)
VALUES ('timezone_string', 'UTC');

SELECT option_name, option_value FROM wp_options WHERE autoload = 'yes' ORDER BY option_name;
SELECT option_name FROM wp_options WHERE option_name IN ('siteurl', 'home') ORDER BY option_name;
SELECT option_value FROM wp_options WHERE option_name = 'siteurl';
SELECT option_value FROM wp_options WHERE option_name = 'home';
SELECT autoload FROM wp_options WHERE option_name='timezone_string';
INSERT IGNORE INTO wp_options (option_name, option_value)
VALUES ('timezone_string', 'Europe/Berlin');
SELECT option_value FROM wp_options WHERE option_name='timezone_string';
REPLACE INTO wp_options (option_name, option_value, autoload)
VALUES ('timezone_string', 'Europe/Berlin', 'no');
SELECT option_value FROM wp_options WHERE option_name='timezone_string';
SELECT autoload FROM wp_options WHERE option_name='timezone_string';

UPDATE wp_options SET option_value='https://example.org' WHERE option_name='siteurl';
UPDATE wp_options SET option_value='https://example.org' WHERE option_name='home';
INSERT IGNORE INTO wp_options (option_id, option_name, option_value, autoload)
VALUES (1, 'siteurl', 'https://ignored.example', 'yes');

SELECT option_value FROM wp_options WHERE option_name = 'siteurl';
SELECT option_value FROM wp_options WHERE option_name = 'home';

INSERT INTO wp_options (option_name, option_value, autoload)
VALUES ('siteurl', 'https://example.net', 'yes')
ON DUPLICATE KEY UPDATE
  option_value = VALUES(option_value),
  autoload = VALUES(autoload);

SELECT option_value FROM wp_options WHERE option_name='siteurl';

INSERT INTO wp_options (option_value, option_name, autoload)
VALUES ('https://example.shuffle', 'siteurl', 'no')
ON DUPLICATE KEY UPDATE
  option_value = VALUES(option_value),
  autoload = VALUES(autoload);

SELECT option_value, autoload FROM wp_options WHERE option_name='siteurl';

REPLACE INTO wp_options (option_value, option_name, autoload)
VALUES ('https://example.replace-shuffled', 'siteurl', 'yes');

SELECT option_value, autoload FROM wp_options WHERE option_name='siteurl';

REPLACE INTO wp_options (option_id, option_name, option_value, autoload)
VALUES (1, 'siteurl', 'https://example.replace', 'yes');

SELECT option_value FROM wp_options WHERE option_name='siteurl';

INSERT INTO wp_users (id, user_login)
VALUES
  (1, 'ada'),
  (2, 'grace'),
  (4, 'margaret');

CREATE UNIQUE INDEX user_login_unique ON wp_users (user_login);

SHOW INDEX FROM wp_users;

INSERT IGNORE INTO wp_users (id, user_login)
VALUES (5, 'ada');

SELECT COUNT(*) AS user_count
  FROM wp_users;

ALTER TABLE wp_users DROP INDEX user_login_unique;

SHOW INDEX FROM wp_users;

INSERT INTO wp_posts (post_author, post_date, post_status, post_title)
VALUES
  (1, '2020-01-01 00:00:00', 'publish', 'Hello'),
  (1, '2020-01-02 00:00:00', 'draft', 'Draft 1'),
  (2, '2020-01-03 00:00:00', 'publish', 'World'),
  (2, '2020-01-04 00:00:00', 'publish', 'More'),
  (3, '2020-01-05 00:00:00', 'publish', 'Even More');

SELECT post_name
  FROM wp_posts
 WHERE ID = 1;

SELECT p.post_author, u.user_login
  FROM wp_posts AS p
  LEFT JOIN wp_users AS u
    ON p.post_author = u.id
 WHERE u.user_login IS NULL
 ORDER BY p.post_author ASC;

SELECT p.ID
  FROM wp_posts AS p
  LEFT JOIN wp_users AS u
    ON p.post_author = u.id
 WHERE u.user_login = 'ada'
 ORDER BY p.ID ASC;

SELECT u.id, p.ID
  FROM wp_posts AS p
  RIGHT JOIN wp_users AS u
    ON p.post_author = u.id
 WHERE p.ID IS NULL
 ORDER BY u.id ASC;

SELECT p.ID, u.user_login, ux.user_login
  FROM wp_posts AS p
  LEFT JOIN wp_users AS u
    ON p.post_author = u.id
  LEFT JOIN wp_users AS ux
    ON ux.id = u.id
 WHERE p.ID = 1
 ORDER BY p.ID ASC;

SELECT ID
  FROM wp_posts
 WHERE post_title = NULL
 ORDER BY ID ASC;

SELECT COUNT(*) AS publish_count
  FROM wp_posts
 WHERE post_status = 'publish';

SELECT COUNT(post_author) AS author_count
  FROM wp_posts
 WHERE post_status = 'publish';

SELECT SUM(post_author) AS author_sum
  FROM wp_posts
 WHERE post_status = 'publish';

SELECT MIN(post_author) AS min_author
  FROM wp_posts
 WHERE post_status = 'publish';

SELECT MAX(post_author) AS max_author
  FROM wp_posts
 WHERE post_status = 'publish';

SELECT AVG(post_author) AS avg_author
  FROM wp_posts
 WHERE post_status = 'publish';

SELECT post_status, COUNT(*) AS status_count
  FROM wp_posts
 GROUP BY post_status
 ORDER BY post_status ASC;

SELECT post_author, SUM(post_author) AS author_sum_by_author
  FROM wp_posts
 WHERE post_status = 'publish'
 GROUP BY post_author
 ORDER BY post_author ASC;

SELECT post_status, MAX(post_author) AS max_author_by_status
  FROM wp_posts
 GROUP BY post_status
 ORDER BY post_status ASC;

SELECT ID
  FROM wp_posts
 WHERE post_status = 'publish' OR post_status = 'draft'
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE (post_status = 'publish' OR post_status = 'draft')
   AND post_author = 1
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE post_status NOT IN ('draft')
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE post_title NOT LIKE 'Dr%'
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE post_status LIKE 'pub%'
 ORDER BY ID DESC
 LIMIT 0, 2;

SELECT SQL_CALC_FOUND_ROWS ID
  FROM wp_posts
 WHERE post_status='publish'
 ORDER BY ID
 LIMIT 2;

SELECT FOUND_ROWS();

SELECT SQL_CALC_FOUND_ROWS p.ID
  FROM wp_posts AS p
  LEFT JOIN wp_posts AS px
    ON px.post_author = p.post_author
 WHERE p.post_status='publish'
 GROUP BY p.ID
 ORDER BY p.ID ASC
 LIMIT 0, 2;

SELECT FOUND_ROWS();

SET @@SESSION.autocommit=0;

BEGIN;
INSERT INTO wp_options (option_name, option_value, autoload)
VALUES ('txn_test', '1', 'no');
SELECT option_value FROM wp_options WHERE option_name='txn_test';
ROLLBACK;
SELECT option_value FROM wp_options WHERE option_name='txn_test';

BEGIN;
INSERT INTO wp_options (option_name, option_value, autoload)
VALUES ('txn_test', '2', 'no');
COMMIT;
SELECT option_value FROM wp_options WHERE option_name='txn_test';

SET LOCAL autocommit=1;

CREATE TABLE compat_alter_subq (
  id BIGINT UNSIGNED NOT NULL,
  ref_id BIGINT UNSIGNED NULL,
  slug VARCHAR(20) NOT NULL DEFAULT '',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

INSERT INTO compat_alter_subq (id, ref_id, slug)
VALUES
  (1, NULL, 'root'),
  (2, 1, 'child'),
  (3, 2, 'leaf');

ALTER TABLE compat_alter_subq
  MODIFY COLUMN slug VARCHAR(20) NOT NULL DEFAULT 'n-a';

ALTER TABLE compat_alter_subq
  CHANGE COLUMN ref_id parent_id BIGINT UNSIGNED NULL;

INSERT INTO compat_alter_subq (id, parent_id)
VALUES (4, 1);

SELECT slug
  FROM compat_alter_subq
 WHERE id = 4;

ALTER TABLE compat_alter_subq
  RENAME COLUMN slug TO post_slug;

SHOW FULL COLUMNS FROM compat_alter_subq;

SELECT post_slug
  FROM compat_alter_subq
 WHERE id = 4;

SELECT id
  FROM compat_alter_subq
 WHERE parent_id IN (
   SELECT id
     FROM compat_alter_subq
    WHERE id < 3
 )
 ORDER BY id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE EXISTS (
   SELECT 1
     FROM compat_alter_subq
    WHERE post_slug = 'n-a'
 )
 ORDER BY id ASC
 LIMIT 0, 2;

SELECT outer_q.id
  FROM compat_alter_subq AS outer_q
 WHERE EXISTS (
   SELECT 1
     FROM compat_alter_subq AS inner_q
    WHERE inner_q.parent_id = outer_q.id
 )
 ORDER BY outer_q.id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE NOT EXISTS (
   SELECT 1
     FROM compat_alter_subq
    WHERE id = 999
 )
 ORDER BY id ASC
 LIMIT 0, 2;

SELECT id
  FROM compat_alter_subq
 WHERE parent_id IN (
   SELECT id
     FROM compat_alter_subq
    WHERE id < 3
 )
   AND id > 1
 ORDER BY id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE EXISTS (
   SELECT 1
     FROM compat_alter_subq
    WHERE post_slug = 'n-a'
 )
   AND parent_id IS NOT NULL
 ORDER BY id ASC
 LIMIT 0, 2;

SELECT id
  FROM compat_alter_subq
 WHERE LOWER(post_slug) = 'n-a'
 ORDER BY id ASC;

SELECT LOWER(post_slug),
       UPPER(post_slug),
       LENGTH(post_slug),
       CHAR_LENGTH(post_slug),
       COALESCE(parent_id, 0),
       IFNULL(parent_id, 0),
       CONCAT(post_slug, '-', IFNULL(parent_id, 0))
  FROM compat_alter_subq
 WHERE id = 4;

SELECT TRIM('  n-a  '),
       LTRIM('  n-a'),
       RTRIM('n-a  '),
       LEFT(post_slug, 1),
       RIGHT(post_slug, 1),
       SUBSTRING(post_slug, 2, 2),
       REPLACE(post_slug, '-', '_'),
       NULLIF(post_slug, 'n-a')
  FROM compat_alter_subq
 WHERE id = 4;

SELECT IF(1, post_slug, 'miss'),
       LOCATE('a', post_slug),
       INSTR(post_slug, 'a'),
       ABS(-7),
       ROUND(1.75, 1),
       FLOOR(1.75),
       CEIL(1.2),
       MOD(7, 4),
       LEAST('z', 'a'),
       GREATEST(1, 5, 2)
  FROM compat_alter_subq
 WHERE id = 4;

SELECT CAST(id AS CHAR),
       CAST('7' AS UNSIGNED),
       CASE WHEN parent_id IS NULL THEN 'root' ELSE 'child' END,
       CASE post_slug WHEN 'n-a' THEN 'match' ELSE 'miss' END
  FROM compat_alter_subq
 WHERE id = 4;

SELECT id
  FROM compat_alter_subq
 WHERE CAST(parent_id AS UNSIGNED) = 1
 ORDER BY id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE parent_id IS NOT NULL
 ORDER BY CAST(parent_id AS UNSIGNED) DESC, id ASC
 LIMIT 0, 2;

INSERT INTO compat_alter_subq (id, parent_id)
VALUES (5, 1);

SELECT outer_q.id
  FROM compat_alter_subq AS outer_q
 WHERE EXISTS (
   SELECT 1
     FROM compat_alter_subq AS inner_q
    WHERE inner_q.parent_id = outer_q.parent_id
      AND inner_q.post_slug = outer_q.post_slug
      AND inner_q.id > 4
 )
 ORDER BY outer_q.id ASC;

SELECT outer_q.id
  FROM compat_alter_subq AS outer_q
 WHERE outer_q.id IN (
   SELECT inner_q.id
     FROM compat_alter_subq AS inner_q
    WHERE inner_q.parent_id = outer_q.parent_id
 )
 ORDER BY outer_q.id ASC;

CREATE TABLE compat_dropcol (
  id BIGINT UNSIGNED NOT NULL,
  keep_col VARCHAR(20) NOT NULL DEFAULT '',
  drop_col VARCHAR(20) NULL,
  PRIMARY KEY (id),
  KEY drop_col_idx (drop_col)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

INSERT INTO compat_dropcol (id, keep_col, drop_col)
VALUES (1, 'stay', 'gone');

ALTER TABLE compat_dropcol
  DROP COLUMN drop_col;

SHOW FULL COLUMNS FROM compat_dropcol;
SHOW INDEX FROM compat_dropcol;

SELECT keep_col
  FROM compat_dropcol
 WHERE id = 1;

SHOW STATUS LIKE 'Threads_connected';
SHOW ENGINES;
SHOW GRANTS;
