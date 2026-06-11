// node-postgres (pg) smoke test against the SkeinDB PostgreSQL wire listener.
//
// Exercises SCRAM-SHA-256 auth (when SMOKE_PASSWORD is set), the simple-query
// protocol for DDL/DML, and the extended-query protocol for a parameterized
// SELECT. Env: SMOKE_HOST, SMOKE_PG_PORT, SMOKE_USER, SMOKE_PASSWORD.
import pg from 'pg';

const { Client } = pg;
const DB = 'smoke_pgnode';

function assert(cond, msg) {
  if (!cond) {
    throw new Error(`assertion failed: ${msg}`);
  }
}

const client = new Client({
  host: process.env.SMOKE_HOST || '127.0.0.1',
  port: Number(process.env.SMOKE_PG_PORT || 5432),
  user: process.env.SMOKE_USER || 'skein',
  password: process.env.SMOKE_PASSWORD || '',
  database: 'skein',
  ssl: false,
});

await client.connect();
try {
  const one = await client.query('SELECT 1 AS one');
  assert(String(one.rows[0].one) === '1', `SELECT 1 -> ${JSON.stringify(one.rows[0])}`);

  await client.query(`CREATE DATABASE ${DB}`);
  await client.query(`CREATE TABLE ${DB}.items (id INT PRIMARY KEY, label VARCHAR(64))`);
  await client.query(`INSERT INTO ${DB}.items (id, label) VALUES (1, 'alpha')`);
  await client.query(`INSERT INTO ${DB}.items (id, label) VALUES (2, 'beta')`);

  const all = await client.query(`SELECT id, label FROM ${DB}.items ORDER BY id`);
  assert(all.rows.length === 2, `row count ${all.rows.length}`);
  assert(String(all.rows[0].id) === '1' && all.rows[0].label === 'alpha', `row0 ${JSON.stringify(all.rows[0])}`);
  assert(String(all.rows[1].id) === '2' && all.rows[1].label === 'beta', `row1 ${JSON.stringify(all.rows[1])}`);

  // Parameterized query -> extended-query protocol (Parse/Bind/Execute).
  const filtered = await client.query(`SELECT label FROM ${DB}.items WHERE id = $1`, [2]);
  assert(filtered.rows.length === 1 && filtered.rows[0].label === 'beta', `param query -> ${JSON.stringify(filtered.rows)}`);

  await client.query(`DROP TABLE ${DB}.items`);
  console.log('node-postgres smoke OK');
} finally {
  await client.end();
}
