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

ALTER TABLE wp_users RENAME INDEX user_login_unique TO user_login_renamed;

SHOW INDEX FROM wp_users;

INSERT IGNORE INTO wp_users (id, user_login)
VALUES (5, 'ada');

SELECT COUNT(*) AS user_count
  FROM wp_users;

SELECT COUNT(*) AS user_count
  FROM wp_users
HAVING COUNT(*) >= 3
   AND user_count = 3;

ALTER TABLE wp_users DROP INDEX user_login_renamed;

SHOW INDEX FROM wp_users;

INSERT INTO wp_posts (post_author, post_date, post_status, post_title)
VALUES
  (1, '2020-01-01 00:00:00', 'publish', 'Hello'),
  (1, '2020-01-02 00:00:00', 'draft', 'Draft 1'),
  (2, '2020-01-03 00:00:00', 'publish', 'World'),
  (2, '2020-01-04 00:00:00', 'publish', 'More'),
  (3, '2020-01-05 00:00:00', 'publish', 'Even More');

SELECT u.id, u.user_login, p.post_title
  FROM wp_users AS u
  INNER JOIN wp_posts AS p USING (id)
 ORDER BY u.id ASC;

SELECT post_name
  FROM wp_posts
 WHERE ID = 1;

SELECT DATE(post_date), YEAR(post_date), MONTH(post_date), DAY(post_date), HOUR(post_date), MINUTE(post_date), SECOND(post_date)
  FROM wp_posts
 WHERE ID = 1;

SELECT DATE_FORMAT(post_date, '%Y-%m-%d %H:%i:%s'),
       FROM_UNIXTIME(UNIX_TIMESTAMP(post_date))
  FROM wp_posts
 WHERE ID = 1;

SELECT DATEDIFF(post_date, '2020-01-01 00:00:00'),
       TIMESTAMPDIFF(HOUR, '2020-01-01 00:00:00', post_date)
  FROM wp_posts
 WHERE ID = 2;

SELECT WEEKDAY(post_date),
       DAYOFWEEK(post_date),
       DAYOFYEAR(post_date),
       MONTHNAME(post_date),
       DAYNAME(post_date)
  FROM wp_posts
 WHERE ID = 2;

SELECT QUARTER(post_date),
       LAST_DAY(post_date),
       EXTRACT(YEAR FROM post_date),
       EXTRACT(HOUR FROM post_date)
  FROM wp_posts
 WHERE ID = 2;

SELECT DATE_ADD(post_date, INTERVAL 2 DAY),
       DATE_SUB(post_date, INTERVAL 3 HOUR),
       TIMESTAMPADD(MINUTE, 30, post_date)
  FROM wp_posts
 WHERE ID = 2;

SELECT ID
  FROM wp_posts
 WHERE DATE(post_date) = '2020-01-03'
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE DATE_FORMAT(post_date, '%Y-%m-%d') = '2020-01-03'
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE YEAR(post_date) = 2020
 ORDER BY UNIX_TIMESTAMP(post_date) DESC
 LIMIT 0, 2;

SELECT ID
  FROM wp_posts
 WHERE DATEDIFF(post_date, '2020-01-01 00:00:00') >= 2
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE DAYNAME(post_date) = 'Friday'
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE EXTRACT(DAY FROM post_date) = 3
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE DATE_ADD(post_date, INTERVAL 1 DAY) = '2020-01-04 00:00:00'
 ORDER BY ID ASC;

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

SELECT p.ID post_id, u.user_login author_login
  FROM wp_posts AS p
  LEFT JOIN wp_users AS u
    ON p.post_author = u.id
 WHERE p.ID = 1;

SELECT DISTINCT p.post_author AS author_id, u.user_login
  FROM wp_posts AS p, wp_users AS u
 WHERE p.post_author = u.id
 ORDER BY p.post_author ASC;

SELECT *
  FROM wp_users
 GROUP BY id, user_login
HAVING id = 1
 ORDER BY id ASC;

SELECT *
  FROM wp_posts AS p
  LEFT JOIN wp_users AS u
    ON p.post_author = u.id
 WHERE p.ID = 1;

SELECT p.*, u.user_login
  FROM wp_posts AS p
  LEFT JOIN wp_users AS u
    ON p.post_author = u.id
 WHERE p.ID = 1;

SELECT skein_test.wp_posts.*, u.user_login
  FROM skein_test.wp_posts
  LEFT JOIN skein_test.wp_users AS u
    ON wp_posts.post_author = u.id
 WHERE wp_posts.ID = 1;

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

SELECT post_status, COUNT(*) AS status_count
  FROM wp_posts
 GROUP BY post_status
HAVING status_count > 1
 ORDER BY status_count DESC, post_status ASC;

SELECT post_status, COUNT(*) AS status_count
  FROM wp_posts
 GROUP BY post_status
HAVING COUNT(*) > 1 AND post_status = 'publish'
 ORDER BY status_count DESC, post_status ASC;

SELECT p.ID AS post_id, p.post_status
  FROM wp_posts AS p
 GROUP BY p.ID, p.post_status
HAVING post_id = 1 AND p.post_status = 'publish'
 ORDER BY p.ID ASC;

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
 WHERE parent_id = (
   SELECT parent_id
     FROM compat_alter_subq
    WHERE id = 4
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
 WHERE parent_id IN (
   SELECT id
     FROM compat_alter_subq
    WHERE id < 3
 )
    OR id = 1
 ORDER BY id ASC;

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
       FIND_IN_SET(post_slug, 'miss,n-a,root'),
       ISNULL(parent_id),
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

SELECT parent_id + 1,
       parent_id - 1,
       parent_id * 2,
       parent_id / 2,
       parent_id % 2
  FROM compat_alter_subq
 WHERE id = 4;

SELECT id
  FROM compat_alter_subq
 WHERE CAST(parent_id AS UNSIGNED) = 1
 ORDER BY id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE FIND_IN_SET(post_slug, 'n-a,miss') > 0
 ORDER BY id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE parent_id + 0 = 1
 ORDER BY id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE parent_id IS NOT NULL
 ORDER BY CAST(parent_id AS UNSIGNED) DESC, id ASC
 LIMIT 0, 2;

SELECT id
  FROM compat_alter_subq
 WHERE parent_id IS NOT NULL
 ORDER BY parent_id + 0 DESC, id ASC
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

SELECT outer_q.id
  FROM compat_alter_subq AS outer_q
 WHERE (
   EXISTS (
     SELECT 1
       FROM compat_alter_subq AS inner_q
      WHERE inner_q.parent_id = outer_q.id
   )
   AND outer_q.id > 1
 )
    OR outer_q.id = 1
 ORDER BY outer_q.id ASC;

SELECT id
  FROM compat_alter_subq
 WHERE parent_id IN (
   SELECT id
     FROM compat_alter_subq
    WHERE id IN (
      SELECT parent_id
        FROM compat_alter_subq
       WHERE id = 3
    )
 )
 ORDER BY id ASC;

SELECT outer_q.id
  FROM compat_alter_subq AS outer_q
 WHERE NOT (
   outer_q.id = 1
   OR EXISTS (
     SELECT 1
       FROM compat_alter_subq AS inner_q
      WHERE inner_q.parent_id = outer_q.id
   )
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

CREATE TABLE compat_rename_src (
  id BIGINT UNSIGNED NOT NULL,
  slug VARCHAR(20) NOT NULL DEFAULT 'seed',
  PRIMARY KEY (id),
  UNIQUE KEY slug_unique (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

INSERT INTO compat_rename_src (id, slug)
VALUES (1, 'hello');

ALTER TABLE compat_rename_src
  RENAME TO compat_rename_dst;

SHOW TABLES FROM skein_test LIKE 'compat_rename_%';
SHOW FULL COLUMNS FROM compat_rename_dst;
SHOW INDEX FROM compat_rename_dst;
SHOW CREATE TABLE compat_rename_dst;

SELECT slug
  FROM compat_rename_dst
 WHERE id = 1;

-- BETWEEN support
SELECT ID
  FROM wp_posts
 WHERE ID BETWEEN 2 AND 4
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE ID NOT BETWEEN 2 AND 4
 ORDER BY ID ASC;

SELECT ID
  FROM wp_posts
 WHERE post_date BETWEEN '2020-01-02 00:00:00' AND '2020-01-04 00:00:00'
 ORDER BY ID ASC;

-- TRUNCATE TABLE
CREATE TABLE compat_truncate (
  id BIGINT UNSIGNED NOT NULL,
  val VARCHAR(20) NOT NULL DEFAULT '',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

INSERT INTO compat_truncate (id, val) VALUES (1, 'a'), (2, 'b');
TRUNCATE TABLE compat_truncate;
SELECT COUNT(*) AS cnt FROM compat_truncate;

-- SHOW WARNINGS (should return empty result)
SHOW WARNINGS;

-- EXPLAIN stub
EXPLAIN SELECT * FROM wp_posts WHERE ID = 1;

-- RENAME TABLE
CREATE TABLE compat_rename_test (
  id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
INSERT INTO compat_rename_test (id) VALUES (1);
RENAME TABLE compat_rename_test TO compat_renamed;
SELECT id FROM compat_renamed WHERE id = 1;

-- SHOW CREATE DATABASE
SHOW CREATE DATABASE skein_test;

-- START TRANSACTION (synonym for BEGIN)
START TRANSACTION;
INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('start_txn_test', '1', 'no');
COMMIT;
SELECT option_value FROM wp_options WHERE option_name='start_txn_test';

-- COUNT(DISTINCT)
SELECT COUNT(DISTINCT post_status) AS distinct_statuses
  FROM wp_posts;

SELECT COUNT(DISTINCT post_author) AS distinct_authors
  FROM wp_posts
 WHERE post_status = 'publish';

-- INSERT ... SELECT
CREATE TABLE compat_insert_select (
  id BIGINT UNSIGNED NOT NULL,
  val VARCHAR(200) NOT NULL DEFAULT '',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

INSERT INTO compat_insert_select (id, val)
SELECT ID, post_title FROM wp_posts WHERE post_status = 'publish';

SELECT COUNT(*) AS cnt FROM compat_insert_select;

-- information_schema.schemata
SELECT SCHEMA_NAME
  FROM information_schema.schemata
 WHERE SCHEMA_NAME = 'skein_test';

-- UNION / UNION ALL
SELECT ID FROM wp_posts WHERE post_status = 'publish'
UNION ALL
SELECT ID FROM wp_posts WHERE post_status = 'draft';

SELECT post_status FROM wp_posts WHERE ID = 1
UNION
SELECT post_status FROM wp_posts WHERE ID = 2;

-- GROUP_CONCAT
SELECT GROUP_CONCAT(post_status) AS statuses
  FROM wp_posts
 WHERE ID <= 3;

-- COUNT(DISTINCT) with GROUP BY
SELECT post_author, COUNT(DISTINCT post_status) AS status_count
  FROM wp_posts
 GROUP BY post_author
 ORDER BY post_author ASC;

-- SHOW PROCESSLIST
SHOW PROCESSLIST;

-- USER() and friends
SELECT USER();
SELECT CURRENT_USER();
SELECT SESSION_USER();
SELECT SYSTEM_USER();
SELECT CONNECTION_ID();

-- LAST_INSERT_ID tracking
INSERT INTO wp_posts (post_author, post_date, post_status, post_title) VALUES (1, '2020-02-01 00:00:00', 'publish', 'AutoInc');
SELECT LAST_INSERT_ID();

-- DO statement
DO 1;
DO SLEEP(0);

-- SHOW PLUGINS / SHOW PROFILES
SHOW PLUGINS;
SHOW PROFILES;

-- Maintenance no-ops
FLUSH TABLES;
FLUSH PRIVILEGES;
ANALYZE TABLE wp_posts;
OPTIMIZE TABLE wp_posts;
CHECK TABLE wp_posts;

-- Additional scalar functions
SELECT CONCAT_WS('-', 'a', 'b', 'c');
SELECT CONCAT_WS(',', 'hello', NULL, 'world');
SELECT REPEAT('ab', 3);
SELECT REVERSE('hello');
SELECT LPAD('hi', 5, '0');
SELECT RPAD('hi', 5, '0');
SELECT SPACE(3);
SELECT HEX(255);
SELECT SIGN(-5), SIGN(0), SIGN(5);
SELECT SQRT(16);
SELECT POW(2, 10);
SELECT TRUNCATE(1.999, 1);
SELECT PI();
SELECT UUID() AS uuid_val;
SELECT SLEEP(0);
SELECT BENCHMARK(1, 1+1);

-- SELECT ... FOR UPDATE (strip locking hints)
SELECT option_value FROM wp_options WHERE option_name = 'siteurl' FOR UPDATE;

-- STR_TO_DATE
SELECT STR_TO_DATE('2020-01-15', '%Y-%m-%d');

-- WEEK / YEARWEEK
SELECT WEEK('2020-01-15');
SELECT YEARWEEK('2020-01-15');

-- UTC time functions
SELECT UTC_TIMESTAMP() AS utc_ts;

-- SYSDATE
SELECT SYSDATE() AS sysdate_ts;

-- CREATE VIEW / DROP VIEW
CREATE VIEW wp_published AS SELECT ID, post_title FROM wp_posts WHERE post_status = 'publish';
DROP VIEW IF EXISTS wp_published;

-- SAVEPOINT support
BEGIN;
SAVEPOINT sp1;
INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('savepoint_test', '1', 'no');
RELEASE SAVEPOINT sp1;
COMMIT;

-- SHOW TRIGGERS / EVENTS
SHOW TRIGGERS;
SHOW EVENTS;
SHOW PROCEDURE STATUS;

SHOW STATUS LIKE 'Threads_connected';
SHOW ENGINES;
SHOW GRANTS;

-- ============================================================
-- Derived tables (FROM subqueries)
-- ============================================================
SELECT t.post_title FROM (SELECT ID, post_title FROM wp_posts WHERE post_status = 'publish') AS t WHERE t.ID > 0;

-- ============================================================
-- CTEs (WITH ... AS)
-- ============================================================
WITH published AS (SELECT ID, post_title FROM wp_posts WHERE post_status = 'publish') SELECT * FROM published;

-- ============================================================
-- REGEXP / RLIKE
-- ============================================================
SELECT * FROM wp_posts WHERE post_title REGEXP '^Hello';
SELECT * FROM wp_posts WHERE post_title RLIKE 'World$';
SELECT * FROM wp_options WHERE option_name NOT REGEXP '^site';

-- ============================================================
-- NULL-safe equality <=>
-- ============================================================
SELECT * FROM wp_options WHERE option_name <=> 'siteurl';
SELECT * FROM wp_options WHERE option_name <=> NULL;

-- ============================================================
-- NATURAL JOIN
-- ============================================================
SELECT * FROM wp_posts NATURAL JOIN wp_users;

-- ============================================================
-- FULL OUTER JOIN (syntax acceptance)
-- ============================================================
SELECT * FROM wp_posts AS p FULL OUTER JOIN wp_users AS u ON p.post_author = u.ID;

-- ============================================================
-- JSON functions
-- ============================================================
SELECT JSON_OBJECT('key', 'value', 'num', 42);
SELECT JSON_ARRAY(1, 2, 'three');
SELECT JSON_EXTRACT('{"a":1,"b":{"c":2}}', '$.b.c');
SELECT JSON_UNQUOTE('"hello"');
SELECT JSON_LENGTH('[1,2,3]');
SELECT JSON_TYPE('{"a":1}');
SELECT JSON_TYPE('"hello"');
SELECT JSON_TYPE('42');
SELECT JSON_TYPE('null');
SELECT JSON_VALID('{"a":1}');
SELECT JSON_VALID('not json');
SELECT JSON_CONTAINS('[1,2,3]', '2');
SELECT JSON_KEYS('{"a":1,"b":2}');
SELECT JSON_SET('{"a":1}', '$.b', 2);
SELECT JSON_MERGE_PRESERVE('[1,2]', '[3,4]');

-- ============================================================
-- Multi-table DELETE (syntax acceptance)
-- ============================================================
DELETE p FROM wp_posts AS p JOIN wp_users AS u ON p.post_author = u.ID WHERE u.user_login = 'nonexistent';

-- ============================================================
-- Multi-table UPDATE (syntax acceptance)
-- ============================================================
UPDATE wp_posts AS p JOIN wp_users AS u ON p.post_author = u.ID SET p.post_status = 'updated' WHERE u.user_login = 'nonexistent';

-- ============================================================
-- Additional MySQL scalar functions
-- ============================================================
SELECT IFNULL(NULL, 'default');
SELECT IFNULL('value', 'default');
SELECT NULLIF(1, 1);
SELECT NULLIF(1, 2);
SELECT COALESCE(NULL, NULL, 'found');
SELECT IF(1, 'yes', 'no');
SELECT IF(0, 'yes', 'no');
SELECT GREATEST(1, 3, 2);
SELECT LEAST(1, 3, 2);
SELECT FIELD('b', 'a', 'b', 'c');
SELECT ELT(2, 'a', 'b', 'c');
SELECT FIND_IN_SET('b', 'a,b,c');
SELECT INET_ATON('192.168.1.1');
SELECT INET_NTOA(3232235777);
SELECT BIN(10);
SELECT OCT(10);
SELECT CONV(255, 10, 16);
SELECT CRC32('hello');
SELECT MD5('hello');
SELECT SHA1('hello');
SELECT SHA2('hello', 256);

-- ============================================================
-- JSON_REMOVE / JSON_REPLACE / JSON_INSERT
-- ============================================================
SELECT JSON_REMOVE('{"a":1,"b":2,"c":3}', '$.b');
SELECT JSON_REPLACE('{"a":1,"b":2}', '$.b', 99);
SELECT JSON_INSERT('{"a":1}', '$.b', 2);

-- ============================================================
-- information_schema virtual tables
-- ============================================================
SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = 'default';
SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA = 'default' LIMIT 5;
SELECT * FROM information_schema.schemata;
SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA = 'default';
SELECT * FROM information_schema.key_column_usage WHERE TABLE_SCHEMA = 'default';
SELECT * FROM information_schema.table_constraints WHERE TABLE_SCHEMA = 'default';
SELECT * FROM information_schema.character_sets;
SELECT * FROM information_schema.collations;
SELECT * FROM information_schema.engines;
SELECT TABLE_NAME AS 'table', TABLE_ROWS AS 'rows', SUM(data_length + index_length) AS 'bytes' FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'default' GROUP BY TABLE_NAME;

-- ============================================================
-- EXPLAIN with table extraction
-- ============================================================
EXPLAIN SELECT * FROM wp_posts WHERE ID = 1;

-- ============================================================
-- Additional string / math functions
-- ============================================================
SELECT TRUNCATE(3.14159, 2);
SELECT FORMAT(1234567.891, 2);
SELECT INSERT('Hello World', 7, 5, 'MySQL');
SELECT MAKE_SET(5, 'a', 'b', 'c');
SELECT EXPORT_SET(5, 'Y', 'N', ',', 4);
SELECT QUOTE('hello');
SELECT UNHEX(HEX('abc'));

-- ============================================================
-- GROUP_CONCAT
-- ============================================================
SELECT GROUP_CONCAT(post_title SEPARATOR ', ') FROM wp_posts;

-- ============================================================
-- SHOW ENGINES / SHOW PLUGINS
-- ============================================================
SHOW ENGINES;
SHOW PLUGINS;

-- ============================================================
-- Multi-column GROUP BY
-- ============================================================
SELECT post_status, post_author, COUNT(*) FROM wp_posts GROUP BY post_status, post_author;
SELECT post_status, post_author, COUNT(*), MAX(ID) FROM wp_posts GROUP BY post_status, post_author;

-- ============================================================
-- SUBSTRING_INDEX / ASCII / ORD / CHAR / STRCMP
-- ============================================================
SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);
SELECT SUBSTRING_INDEX('www.mysql.com', '.', -1);
SELECT ASCII('A');
SELECT ORD('A');
SELECT CHAR(65);
SELECT STRCMP('a', 'b');
SELECT STRCMP('b', 'a');

-- ============================================================
-- BIT_LENGTH / OCTET_LENGTH
-- ============================================================
SELECT BIT_LENGTH('hello');
SELECT OCTET_LENGTH('hello');

-- ============================================================
-- REGEXP_REPLACE / REGEXP_SUBSTR
-- ============================================================
SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X');
SELECT REGEXP_SUBSTR('abc 123 def', '[0-9]+');

-- ============================================================
-- TO_BASE64 / FROM_BASE64
-- ============================================================
SELECT TO_BASE64('hello');
SELECT FROM_BASE64('aGVsbG8=');

-- ============================================================
-- information_schema stubs (routines, triggers, views, processlist, user_privileges)
-- ============================================================
SELECT * FROM information_schema.routines;
SELECT * FROM information_schema.triggers;
SELECT * FROM information_schema.views;
SELECT * FROM information_schema.processlist;
SELECT * FROM information_schema.user_privileges;

-- ============================================================
-- SHOW TABLE STATUS
-- ============================================================
SHOW TABLE STATUS;
