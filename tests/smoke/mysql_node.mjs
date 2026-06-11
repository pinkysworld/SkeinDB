// mysql2 smoke test against the SkeinDB MySQL wire listener.
//
// Exercises caching_sha2_password auth (when SMOKE_PASSWORD is set) over an
// unencrypted connection (the server completes the fast-auth path), plus a
// server-side prepared statement. Env: SMOKE_HOST, SMOKE_MYSQL_PORT,
// SMOKE_USER, SMOKE_PASSWORD.
import mysql from 'mysql2/promise';

const DB = 'smoke_mysqlnode';

function assert(cond, msg) {
  if (!cond) {
    throw new Error(`assertion failed: ${msg}`);
  }
}

const conn = await mysql.createConnection({
  host: process.env.SMOKE_HOST || '127.0.0.1',
  port: Number(process.env.SMOKE_MYSQL_PORT || 3306),
  user: process.env.SMOKE_USER || 'skein',
  password: process.env.SMOKE_PASSWORD || '',
  ssl: undefined,
});

try {
  const [one] = await conn.query('SELECT 1 AS one');
  assert(String(one[0].one) === '1', `SELECT 1 -> ${JSON.stringify(one[0])}`);

  await conn.query(`CREATE DATABASE IF NOT EXISTS ${DB}`);
  await conn.query(`USE ${DB}`);
  await conn.query('DROP TABLE IF EXISTS items');
  await conn.query('CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(64))');
  await conn.query("INSERT INTO items (id, label) VALUES (1, 'alpha')");
  await conn.query("INSERT INTO items (id, label) VALUES (2, 'beta')");

  const [rows] = await conn.query('SELECT id, label FROM items ORDER BY id');
  assert(rows.length === 2, `row count ${rows.length}`);
  assert(String(rows[0].id) === '1' && rows[0].label === 'alpha', `row0 ${JSON.stringify(rows[0])}`);
  assert(String(rows[1].id) === '2' && rows[1].label === 'beta', `row1 ${JSON.stringify(rows[1])}`);

  // execute() uses the binary prepared-statement protocol (COM_STMT_*).
  const [prepared] = await conn.execute('SELECT label FROM items WHERE id = ?', [2]);
  assert(prepared.length === 1 && prepared[0].label === 'beta', `prepared -> ${JSON.stringify(prepared)}`);

  await conn.query('DROP TABLE items');
  console.log('mysql2 smoke OK');
} finally {
  await conn.end();
}
