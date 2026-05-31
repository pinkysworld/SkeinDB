// Network-free unit tests for the Node.js SkeinQL client.
// Run with Node >= 18:  node --test examples/sdk/node/
import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  SKEINQL_VERSION,
  SkeinClient,
  SkeinError,
  buildRequest,
  parseResponse,
} from './skeindb_client.mjs';

test('buildRequest produces the envelope shape', () => {
  const env = buildRequest('query.select', { query: 'SELECT 1' }, 'req-7');
  assert.equal(env.skeinql, SKEINQL_VERSION);
  assert.equal(env.id, 'req-7');
  assert.equal(env.method, 'query.select');
  assert.deepEqual(env.params, { query: 'SELECT 1' });
});

test('buildRequest defaults params to an empty object', () => {
  assert.deepEqual(buildRequest('system.capabilities').params, {});
});

test('parseResponse returns result on ok', () => {
  assert.deepEqual(parseResponse({ id: 'req-1', ok: true, result: { rows: [] } }), { rows: [] });
});

test('parseResponse throws on error envelope', () => {
  assert.throws(
    () => parseResponse({ id: 'req-1', ok: false, error: { code: 'invalid_request', message: 'm' } }),
    (err) => err instanceof SkeinError && err.code === 'invalid_request',
  );
});

test('rpc uses injected transport and increments id', async () => {
  const seen = [];
  const transport = (envelope) => {
    seen.push(envelope.id);
    return Promise.resolve({ id: envelope.id, ok: true, result: { echo: envelope.method } });
  };
  const client = new SkeinClient('http://localhost:8080', { transport });
  assert.deepEqual(await client.rpc('a.method'), { echo: 'a.method' });
  assert.deepEqual(await client.rpc('b.method'), { echo: 'b.method' });
  assert.deepEqual(seen, ['req-1', 'req-2']);
});

test('select builds query params', async () => {
  let captured;
  const transport = (envelope) => {
    captured = envelope;
    return Promise.resolve({ id: envelope.id, ok: true, result: {} });
  };
  const client = new SkeinClient('http://localhost:8080', { transport });
  await client.select('SELECT 1 AS one', { limit: 10 });
  assert.equal(captured.method, 'query.select');
  assert.deepEqual(captured.params, { query: 'SELECT 1 AS one', params: { limit: 10 } });
});
