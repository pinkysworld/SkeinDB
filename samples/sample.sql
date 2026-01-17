DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS projects;
DROP TABLE IF EXISTS settings;

CREATE TABLE users (
  id INT,
  name TEXT,
  email TEXT,
  active BOOL
);

INSERT INTO users (id, name, email, active) VALUES
  (1, 'Ada Lovelace', 'ada@example.com', true),
  (2, 'Linus Torvalds', 'linus@example.com', true),
  (3, 'Grace Hopper', 'grace@example.com', false);

CREATE TABLE projects (
  id INT,
  name TEXT,
  owner_id INT
);

INSERT INTO projects (id, name, owner_id) VALUES
  (100, 'Skein Console', 1),
  (101, 'Edge Cache', 2),
  (102, 'Telemetry', 1);

CREATE TABLE settings (
  setting_key TEXT,
  setting_value TEXT
);

INSERT INTO settings (setting_key, setting_value) VALUES
  ('theme', 'ember'),
  ('mode', 'dev'),
  ('region', 'local');
