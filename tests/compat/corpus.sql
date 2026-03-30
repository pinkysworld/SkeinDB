-- SkeinDB MySQL Compatibility Corpus
-- Each statement is separated by a semicolon and exercised by the mysql_compat_corpus_roundtrip test.
-- Comments starting with -- are stripped before execution.

-- ── DDL ──────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS corpus_db;
USE corpus_db;
CREATE TABLE IF NOT EXISTS users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100),
  email VARCHAR(200),
  age INT,
  salary DECIMAL(10,2),
  department VARCHAR(50),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS orders (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT,
  product VARCHAR(100),
  amount DECIMAL(10,2),
  status VARCHAR(20) DEFAULT 'pending',
  ordered_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS categories (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100),
  parent_id INT
);

-- ── INSERT ───────────────────────────────────────────────
INSERT INTO users (name, email, age, salary, department) VALUES ('Alice', 'alice@example.com', 30, 75000.00, 'Engineering');
INSERT INTO users (name, email, age, salary, department) VALUES ('Bob', 'bob@example.com', 25, 65000.00, 'Marketing');
INSERT INTO users (name, email, age, salary, department) VALUES ('Charlie', 'charlie@example.com', 35, 85000.00, 'Engineering');
INSERT INTO users (name, email, age, salary, department) VALUES ('Diana', 'diana@example.com', 28, 70000.00, 'Sales');
INSERT INTO users (name, email, age, salary, department) VALUES ('Eve', 'eve@example.com', 32, 90000.00, 'Engineering');
INSERT INTO orders (user_id, product, amount, status) VALUES (1, 'Widget', 29.99, 'completed');
INSERT INTO orders (user_id, product, amount, status) VALUES (1, 'Gadget', 49.99, 'completed');
INSERT INTO orders (user_id, product, amount, status) VALUES (2, 'Widget', 29.99, 'pending');
INSERT INTO orders (user_id, product, amount, status) VALUES (3, 'Gizmo', 99.99, 'completed');
INSERT INTO orders (user_id, product, amount, status) VALUES (4, 'Widget', 29.99, 'cancelled');
INSERT INTO categories (name, parent_id) VALUES ('Electronics', NULL);
INSERT INTO categories (name, parent_id) VALUES ('Software', 1);
INSERT INTO categories (name, parent_id) VALUES ('Hardware', 1);

-- ── Basic SELECT ─────────────────────────────────────────
SELECT 1;
SELECT 1 + 1;
SELECT 'hello';
SELECT NULL;
SELECT VERSION();
SELECT DATABASE();
SELECT USER();
SELECT NOW();
SELECT CURDATE();
SELECT CURTIME();
SELECT @@version_comment LIMIT 1;

-- ── SELECT with expressions ──────────────────────────────
SELECT * FROM users;
SELECT name, email FROM users;
SELECT name AS user_name, email AS user_email FROM users;
SELECT * FROM users WHERE age > 30;
SELECT * FROM users WHERE department = 'Engineering';
SELECT * FROM users WHERE age BETWEEN 25 AND 35;
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE name IN ('Alice', 'Bob');
SELECT * FROM users WHERE email IS NOT NULL;
SELECT * FROM users ORDER BY age ASC;
SELECT * FROM users ORDER BY salary DESC;
SELECT * FROM users ORDER BY department, name;
SELECT * FROM users LIMIT 3;
SELECT * FROM users LIMIT 2 OFFSET 1;
SELECT DISTINCT department FROM users;

-- ── Aggregates ───────────────────────────────────────────
SELECT COUNT(*) FROM users;
SELECT COUNT(name) FROM users;
SELECT SUM(salary) FROM users;
SELECT AVG(salary) FROM users;
SELECT MIN(age) FROM users;
SELECT MAX(age) FROM users;
SELECT department, COUNT(*) FROM users GROUP BY department;
SELECT department, AVG(salary) FROM users GROUP BY department;
SELECT department, SUM(salary) FROM users GROUP BY department;
SELECT department, MIN(age), MAX(age) FROM users GROUP BY department;
SELECT department, COUNT(*) AS cnt FROM users GROUP BY department HAVING cnt > 1;
SELECT department, GROUP_CONCAT(name) FROM users GROUP BY department;

-- ── String functions ─────────────────────────────────────
SELECT UPPER('hello');
SELECT LOWER('HELLO');
SELECT LENGTH('hello');
SELECT CONCAT('hello', ' ', 'world');
SELECT CONCAT_WS('-', 'a', 'b', 'c');
SELECT SUBSTRING('hello world', 1, 5);
SELECT TRIM('  hello  ');
SELECT LTRIM('  hello');
SELECT RTRIM('hello  ');
SELECT REPLACE('hello world', 'world', 'earth');
SELECT REVERSE('hello');
SELECT REPEAT('ab', 3);
SELECT LPAD('hi', 5, '0');
SELECT RPAD('hi', 5, '0');
SELECT LEFT('hello', 3);
SELECT RIGHT('hello', 3);
SELECT CHAR_LENGTH('hello');
SELECT SUBSTRING_INDEX('www.example.com', '.', 2);
SELECT ASCII('A');
SELECT CHAR(65);
SELECT QUOTE('hello');

-- ── Numeric functions ────────────────────────────────────
SELECT ABS(-5);
SELECT CEIL(4.3);
SELECT FLOOR(4.7);
SELECT ROUND(4.567, 2);
SELECT TRUNCATE(4.567, 2);
SELECT MOD(10, 3);
SELECT POWER(2, 10);
SELECT SQRT(16);
SELECT GREATEST(1, 2, 3);
SELECT LEAST(1, 2, 3);
SELECT DEGREES(3.14159265358979);
SELECT RADIANS(180);

-- ── Date/Time functions ──────────────────────────────────
SELECT PERIOD_ADD(202301, 5);
SELECT PERIOD_DIFF(202306, 202301);
SELECT MAKEDATE(2023, 32);
SELECT MAKETIME(10, 30, 45);

-- ── Hash / Crypto functions ──────────────────────────────
SELECT MD5('hello');
SELECT SHA1('hello');
SELECT SHA2('hello', 256);
SELECT CRC32('hello');

-- ── JSON functions ───────────────────────────────────────
SELECT JSON_EXTRACT('{"a":1,"b":2}', '$.a');
SELECT JSON_UNQUOTE('"hello"');
SELECT JSON_OBJECT('key', 'value');
SELECT JSON_ARRAY(1, 2, 3);
SELECT JSON_CONTAINS('{"a":1}', '1', '$.a');

-- ── Encoding functions ───────────────────────────────────
SELECT TO_BASE64('hello');
SELECT FROM_BASE64('aGVsbG8=');
SELECT HEX('hello');
SELECT UNHEX('68656C6C6F');
SELECT BIT_LENGTH('hello');

-- ── SET / GET user variables ─────────────────────────────
SET @myvar = 'test_value';
SELECT @myvar;
SET @counter = 42;
SELECT @counter;

-- ── Session compat SET statements ────────────────────────
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;
SET @@session.sql_mode = '';
SET autocommit = 1;

-- ── Transaction support ──────────────────────────────────
BEGIN;
INSERT INTO users (name, email, age, salary, department) VALUES ('TxUser', 'tx@example.com', 40, 50000.00, 'Test');
COMMIT;

-- ── JOIN queries ─────────────────────────────────────────
SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id;
SELECT u.name, o.product FROM users u LEFT JOIN orders o ON u.id = o.user_id;
SELECT u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name;

-- ── Subqueries ───────────────────────────────────────────
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed');
-- TODO: correlated subquery in projection not yet supported
-- SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count FROM users;

-- ── UNION ────────────────────────────────────────────────
SELECT name FROM users WHERE department = 'Engineering' UNION ALL SELECT name FROM users WHERE department = 'Marketing';

-- ── UPDATE / DELETE ──────────────────────────────────────
UPDATE users SET salary = salary * 1.1 WHERE department = 'Engineering';
DELETE FROM orders WHERE status = 'cancelled';

-- ── INSERT variants ──────────────────────────────────────
INSERT INTO users (name, email, age, salary, department) VALUES ('Frank', 'frank@example.com', 45, 95000.00, 'Engineering');
INSERT IGNORE INTO users (name, email, age, salary, department) VALUES ('Grace', 'grace@example.com', 27, 62000.00, 'Marketing');
REPLACE INTO categories (id, name, parent_id) VALUES (2, 'Software', 1);

-- ── CASE expressions ─────────────────────────────────────
SELECT name, CASE WHEN age < 30 THEN 'young' WHEN age < 35 THEN 'mid' ELSE 'senior' END AS age_group FROM users;

-- ── COALESCE / IFNULL / IF / NULLIF ──────────────────────
SELECT COALESCE(NULL, NULL, 'default');
SELECT IFNULL(NULL, 'fallback');
SELECT IF(1, 'yes', 'no');
-- TODO: SELECT IF(1 > 0, 'yes', 'no');  -- expression-args in IF() not yet supported
SELECT NULLIF(1, 1);
SELECT NULLIF(1, 2);

-- ── EXISTS ───────────────────────────────────────────────
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id);

-- ── SHOW / DESCRIBE / EXPLAIN ────────────────────────────
SHOW DATABASES;
SHOW TABLES;
DESCRIBE users;

-- ── ALTER TABLE ──────────────────────────────────────────
ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE;

-- ── TRUNCATE ─────────────────────────────────────────────
-- TRUNCATE TABLE categories;  -- commented out to preserve data for later statements

-- ── LIKE patterns ────────────────────────────────────────
SELECT * FROM users WHERE name LIKE '%li%';
SELECT * FROM users WHERE email LIKE '%@example.com';

-- ── REGEXP ───────────────────────────────────────────────
SELECT 'hello world' REGEXP 'world';
SELECT 'hello world' RLIKE 'hello';
SELECT REGEXP_REPLACE('hello world', 'world', 'earth');
SELECT REGEXP_SUBSTR('hello123world', '[0-9]+');

-- ── CAST / CONVERT ───────────────────────────────────────
SELECT CAST(42 AS CHAR);
SELECT CAST('2023-01-15' AS DATE);

-- ── Compound INSERT ──────────────────────────────────────
INSERT INTO categories (name, parent_id) VALUES ('Networking', 1), ('Storage', 1);

-- ── COUNT DISTINCT ───────────────────────────────────────
SELECT COUNT(DISTINCT department) FROM users;

-- ── ORDER BY with LIMIT ──────────────────────────────────
SELECT name, salary FROM users ORDER BY salary DESC LIMIT 3;

-- ── SELECT with arithmetic ───────────────────────────────
SELECT name, salary, salary * 12 AS annual FROM users;

-- ── BETWEEN with dates ───────────────────────────────────
SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age;

-- ── Multiple conditions ──────────────────────────────────
SELECT * FROM users WHERE department = 'Engineering' AND age > 30;
SELECT * FROM users WHERE department = 'Engineering' OR department = 'Marketing';

-- ── NULL handling ────────────────────────────────────────
SELECT * FROM users WHERE email IS NOT NULL;
SELECT COALESCE(NULL, 'default_value');

-- ── INSERT ... ON DUPLICATE KEY UPDATE ───────────────────
INSERT INTO users (name, email, age, salary, department) VALUES ('Hank', 'hank@example.com', 50, 100000.00, 'Executive') ON DUPLICATE KEY UPDATE salary = VALUES(salary);

-- ── Information Schema ───────────────────────────────────
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'corpus_db' LIMIT 5;
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'corpus_db' AND TABLE_NAME = 'users' LIMIT 10;

-- ── LOCK / UNLOCK stubs ─────────────────────────────────
LOCK TABLES users READ;
UNLOCK TABLES;

-- ── DROP TABLE ───────────────────────────────────────────
-- DROP TABLE IF EXISTS categories;  -- keep for other tests

-- ── Final verification ───────────────────────────────────
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM orders;

-- ── Window functions ─────────────────────────────────────
SELECT name, salary, ROW_NUMBER() OVER(ORDER BY salary DESC) AS rn FROM users;
SELECT name, department, RANK() OVER(PARTITION BY department ORDER BY salary DESC) AS dept_rank FROM users;
SELECT name, DENSE_RANK() OVER(ORDER BY age) AS age_rank FROM users;

-- ── CTE (Common Table Expressions) ──────────────────────
WITH eng AS (SELECT * FROM users WHERE department = 'Engineering')
SELECT name, salary FROM eng ORDER BY salary DESC;
WITH dept_stats AS (SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM users GROUP BY department)
SELECT * FROM dept_stats WHERE cnt > 1;

-- ── Multi-table JOIN variants ────────────────────────────
SELECT u.name, o.product, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed';
SELECT u.name, o.product FROM users u RIGHT JOIN orders o ON u.id = o.user_id;
SELECT u.name, c.name AS category FROM users u CROSS JOIN categories c LIMIT 10;
-- TODO: SELECT u.name, COUNT(o.id) AS total_orders, SUM(o.amount) AS total_amount FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total_amount DESC;  -- GROUP BY with mixed aggregates over JOIN not yet supported

-- ── UNION / UNION ALL ────────────────────────────────────
SELECT name, 'user' AS type FROM users UNION ALL SELECT name, 'category' AS type FROM categories;
SELECT department FROM users UNION SELECT name FROM categories;

-- ── Derived tables (FROM subqueries) ─────────────────────
-- TODO: SELECT dept, avg_sal FROM (SELECT department AS dept, AVG(salary) AS avg_sal FROM users GROUP BY department) sub WHERE avg_sal > 70000;  -- numeric aggregate in derived table not yet supported
SELECT sub.name FROM (SELECT name FROM users LIMIT 5) sub;

-- ── EXISTS / NOT EXISTS ──────────────────────────────────
SELECT name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- ── IN subquery ──────────────────────────────────────────
SELECT name FROM users WHERE department IN (SELECT DISTINCT department FROM users WHERE salary > 80000);

-- ── Scalar subquery ──────────────────────────────────────
-- TODO: SELECT name, salary, salary - (SELECT AVG(salary) FROM users) AS diff_from_avg FROM users;  -- scalar subquery in arithmetic not yet supported

-- ── Multi-table DELETE ───────────────────────────────────
DELETE o FROM orders o JOIN users u ON o.user_id = u.id WHERE u.department = 'Test';

-- ── Multi-table UPDATE ───────────────────────────────────
UPDATE orders o JOIN users u ON o.user_id = u.id SET o.status = 'verified' WHERE u.department = 'Engineering';

-- ── Additional string functions ──────────────────────────
SELECT INSTR('hello world', 'world');
SELECT LOCATE('world', 'hello world');
SELECT INSERT('hello world', 6, 1, '_');
SELECT FORMAT(1234567.891, 2);
SELECT SPACE(5);
SELECT FIELD('b', 'a', 'b', 'c');
SELECT ELT(2, 'a', 'b', 'c');

-- ── Additional numeric functions ─────────────────────────
SELECT SIGN(-5);
SELECT SIGN(0);
SELECT SIGN(5);
SELECT LOG(100);
SELECT LOG2(8);
SELECT LOG10(1000);
SELECT LN(2.71828);
SELECT EXP(1);
SELECT PI();
SELECT CONV(255, 10, 16);
SELECT BIN(255);
SELECT OCT(255);

-- ── Additional date/time ─────────────────────────────────
SELECT DATE_FORMAT(NOW(), '%Y-%m-%d');
SELECT DATEDIFF('2023-12-31', '2023-01-01');
SELECT TIMESTAMPDIFF(MONTH, '2023-01-01', '2023-06-15');
SELECT DATE_ADD(NOW(), INTERVAL 1 DAY);
SELECT DATE_SUB(NOW(), INTERVAL 1 MONTH);
SELECT DAYOFWEEK(NOW());
SELECT DAYOFYEAR(NOW());
SELECT LAST_DAY(NOW());
SELECT WEEK(NOW());
SELECT YEARWEEK(NOW());
SELECT EXTRACT(YEAR FROM NOW());
SELECT STR_TO_DATE('2023-06-15', '%Y-%m-%d');
SELECT UTC_TIMESTAMP();
SELECT UTC_DATE();

-- ── Bitwise aggregates ───────────────────────────────────
SELECT BIT_AND(age) FROM users;
SELECT BIT_OR(age) FROM users;
SELECT BIT_XOR(age) FROM users;

-- ── Encoding functions (additional) ──────────────────────
SELECT INET_ATON('192.168.1.1');
SELECT INET_NTOA(3232235777);

-- ── DISTINCT with ORDER BY ───────────────────────────────
SELECT DISTINCT department FROM users ORDER BY department;

-- ── GROUP_CONCAT with ORDER BY ───────────────────────────
SELECT department, GROUP_CONCAT(name ORDER BY name) FROM users GROUP BY department;

-- ── Multiple-column ORDER BY with DESC/ASC ───────────────
SELECT * FROM users ORDER BY department ASC, salary DESC;

-- ── LIMIT with OFFSET ────────────────────────────────────
SELECT name FROM users ORDER BY name LIMIT 3 OFFSET 0;
SELECT name FROM users ORDER BY name LIMIT 2 OFFSET 2;

-- ── NULL-safe equality ───────────────────────────────────
SELECT * FROM categories WHERE parent_id <=> NULL;

-- ── NOT IN / NOT LIKE / NOT BETWEEN ──────────────────────
SELECT * FROM users WHERE department NOT IN ('Engineering', 'Marketing');
SELECT * FROM users WHERE name NOT LIKE 'A%';
SELECT * FROM users WHERE age NOT BETWEEN 30 AND 40;

-- ── Nested functions ─────────────────────────────────────
SELECT UPPER(CONCAT(LEFT(name, 1), '.', SUBSTRING(email, 1, LOCATE('@', email) - 1))) FROM users LIMIT 3;

-- ── SHOW variants ────────────────────────────────────────
SHOW CREATE DATABASE corpus_db;
SHOW WARNINGS;
SHOW ERRORS;
SHOW PROCESSLIST;
SHOW FULL PROCESSLIST;
SHOW PLUGINS;

-- ── INFORMATION_SCHEMA.SCHEMATA ──────────────────────────
SELECT * FROM INFORMATION_SCHEMA.SCHEMATA LIMIT 5;

-- ── Additional JOIN variants ─────────────────────────────
SELECT u.name, o.product FROM users u NATURAL JOIN orders o LIMIT 5;
SELECT u.name, o.product FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id LIMIT 10;
SELECT u.name, o.product FROM users u, orders o WHERE u.id = o.user_id LIMIT 5;

-- ── INSERT ... SELECT ────────────────────────────────────
INSERT INTO categories (name, parent_id) SELECT CONCAT('Sub_', name), id FROM categories WHERE parent_id IS NULL LIMIT 2;

-- ── DO statement ─────────────────────────────────────────
DO 1;

-- ── EXPLAIN stub ─────────────────────────────────────────
EXPLAIN SELECT * FROM users WHERE age > 30;

-- ── SHOW extended variants ───────────────────────────────
SHOW CREATE TABLE users;
SHOW INDEX FROM users;
SHOW TABLE STATUS;
SHOW CHARACTER SET;
SHOW COLLATION;
SHOW ENGINES;
SHOW GRANTS;
SHOW TRIGGERS;
SHOW EVENTS;
SHOW PROFILES;
SHOW VARIABLES LIKE '%timeout%';
SHOW STATUS LIKE '%Threads%';

-- ── System variables ─────────────────────────────────────
SELECT @@version;
SELECT @@autocommit;
SELECT @@tx_isolation;
SELECT @@sql_mode;
SELECT @@time_zone;
SELECT @@character_set_client;
-- TODO: SELECT @@max_allowed_packet;  -- not yet in session var map

-- ── Maintenance no-ops ───────────────────────────────────
FLUSH TABLES;
ANALYZE TABLE users;
OPTIMIZE TABLE users;
CHECK TABLE users;
REPAIR TABLE users;
KILL 0;

-- ── CREATE VIEW / DROP VIEW stubs ────────────────────────
CREATE VIEW user_summary AS SELECT department, COUNT(*) AS cnt FROM users GROUP BY department;
DROP VIEW IF EXISTS user_summary;

-- ── SAVEPOINT stubs ──────────────────────────────────────
SAVEPOINT sp1;
RELEASE SAVEPOINT sp1;

-- ── GROUP_CONCAT with DISTINCT and SEPARATOR ─────────────
SELECT GROUP_CONCAT(DISTINCT department SEPARATOR ' | ') FROM users;

-- ── Multi-column GROUP BY with multiple aggregates ───────
SELECT department, age, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM users GROUP BY department, age;

-- ── Additional INFORMATION_SCHEMA tables ─────────────────
SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = 'corpus_db' LIMIT 5;

-- ── Session functions ────────────────────────────────────
SELECT CURRENT_USER();
SELECT SESSION_USER();
SELECT SYSTEM_USER();
SELECT LAST_INSERT_ID();
SELECT CONNECTION_ID();
-- NOTE: FOUND_ROWS() is exercised in the hardcoded WordPress-style section of cluster_rpc.rs (depends on prior SQL_CALC_FOUND_ROWS state)

-- ── SELECT FOR UPDATE / SHARE (locking hint stripping) ───
-- TODO: locking hints not yet stripped from WHERE clause parsing
-- SELECT * FROM users WHERE id = 1 FOR UPDATE;
-- SELECT * FROM users WHERE id = 1 FOR SHARE;
-- SELECT * FROM users WHERE id = 1 LOCK IN SHARE MODE;

-- ── Additional scalar functions ──────────────────────────
SELECT RAND();
SELECT UUID();
SELECT SLEEP(0);
SELECT BENCHMARK(1, 1+1);
SELECT ISNULL(NULL);
SELECT FIND_IN_SET('b', 'a,b,c,d');
SELECT ADDTIME('10:00:00', '01:30:00');
SELECT SUBTIME('10:00:00', '01:30:00');
SELECT TIME_TO_SEC('01:00:00');
SELECT SEC_TO_TIME(3600);
SELECT CONVERT_TZ('2023-06-15 12:00:00', 'UTC', 'US/Eastern');
SELECT WEEKDAY(NOW());
SELECT MONTHNAME(NOW());
SELECT DAYNAME(NOW());
SELECT QUARTER(NOW());
SELECT SYSDATE();

-- ── SET GLOBAL no-op ─────────────────────────────────────
SET GLOBAL wait_timeout = 28800;

-- ── End-of-corpus verification ───────────────────────────
SELECT 'corpus_complete' AS status;
