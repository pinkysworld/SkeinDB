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
SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count FROM users;

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

-- ── COALESCE / IFNULL / IF ───────────────────────────────
SELECT COALESCE(NULL, NULL, 'default');
SELECT IFNULL(NULL, 'fallback');
SELECT IF(1 > 0, 'yes', 'no');
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
