// Minimal, dependency-free SkeinQL client for Node.js (>= 18, uses global fetch).
//
// This reference driver talks to the SkeinQL JSON-RPC surface
// (`POST /api/v1/rpc`). The envelope helpers are pure functions so they can be
// unit-tested without a server; the HTTP transport is injectable.
//
// Usage:
//   import { SkeinClient } from './skeindb_client.mjs';
//   const client = new SkeinClient('http://localhost:8080');
//   const caps = await client.capabilities();
//   const rows = await client.select('SELECT 1 AS one');

export const SKEINQL_VERSION = '1.0';

export class SkeinError extends Error {
  constructor(code, message, details) {
    super(`${code}: ${message}`);
    this.name = 'SkeinError';
    this.code = code;
    this.details = details || {};
  }
}

export function buildRequest(method, params = {}, requestId = 'req-1') {
  return { skeinql: SKEINQL_VERSION, id: requestId, method, params: params || {} };
}

export function parseResponse(envelope) {
  if (envelope === null || typeof envelope !== 'object' || Array.isArray(envelope)) {
    throw new SkeinError('invalid_response', 'response was not a JSON object');
  }
  if (envelope.ok) {
    return envelope.result;
  }
  const error = envelope.error || {};
  throw new SkeinError(
    String(error.code ?? 'unknown'),
    String(error.message ?? 'request failed'),
    error.details && typeof error.details === 'object' ? error.details : {},
  );
}

export class SkeinClient {
  constructor(baseUrl = 'http://localhost:8080', options = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.token = options.token || null;
    // Injectable transport: (envelope) => Promise<envelope>. Defaults to fetch.
    this.transport = options.transport || ((envelope) => this.#httpTransport(envelope));
    this.requestSeq = 0;
  }

  async #httpTransport(envelope) {
    const headers = { 'Content-Type': 'application/json' };
    if (this.token) headers.Authorization = `Bearer ${this.token}`;
    const resp = await fetch(`${this.baseUrl}/api/v1/rpc`, {
      method: 'POST',
      headers,
      body: JSON.stringify(envelope),
    });
    // HTTP 200 may still carry an RPC error; non-200 responses also return an
    // envelope, so we parse the body regardless of status.
    return resp.json();
  }

  async rpc(method, params = {}) {
    this.requestSeq += 1;
    const envelope = buildRequest(method, params, `req-${this.requestSeq}`);
    return parseResponse(await this.transport(envelope));
  }

  capabilities() {
    return this.rpc('system.capabilities');
  }

  select(query, params) {
    const callParams = { query };
    if (params) callParams.params = params;
    return this.rpc('query.select', callParams);
  }

  execSql(sql) {
    return this.rpc('sql.exec', { sql });
  }
}
