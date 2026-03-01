-- SkeinDB Compatibility Corpus v0.1

SELECT 1;
SELECT VERSION();
SELECT DATABASE();
SELECT @@sql_mode;
SELECT @@lower_case_table_names;

SHOW VARIABLES LIKE 'sql_mode';
SHOW VARIABLES LIKE 'lower_case_table_names';

SET NAMES utf8mb4;
SET SESSION sql_mode = '';

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

CREATE TABLE wp_users (
  id BIGINT UNSIGNED NOT NULL,
  user_login VARCHAR(60) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

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

INSERT INTO wp_posts (post_author, post_date, post_status, post_title)
VALUES
  (1, '2020-01-01 00:00:00', 'publish', 'Hello'),
  (1, '2020-01-02 00:00:00', 'draft', 'Draft 1'),
  (2, '2020-01-03 00:00:00', 'publish', 'World'),
  (2, '2020-01-04 00:00:00', 'publish', 'More'),
  (3, '2020-01-05 00:00:00', 'publish', 'Even More');

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

SELECT ID
  FROM wp_posts
 WHERE post_title = NULL
 ORDER BY ID ASC;

SELECT COUNT(*) AS publish_count
  FROM wp_posts
 WHERE post_status = 'publish';

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

SET autocommit=0;

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

SET autocommit=1;

SHOW STATUS LIKE 'Threads_connected';
SHOW ENGINES;
SHOW GRANTS;
