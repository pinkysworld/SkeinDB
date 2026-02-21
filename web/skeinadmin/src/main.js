/* SkeinAdmin – main.js
 * Full phpMyAdmin-like admin with all 20 research features (R01-R20).
 * Works for both /admin and /console routes.
 */

const DEFAULT_BASE_URL = window.location.origin || 'http://localhost:8080';

const STATE = {
  methods: [],
  selectedDb: '',
  selectedTable: '',
  dbTree: {},
  isConsole: false,
  connected: false,
  browseOffset: 0
};

// ---------------------------------------------------------------------------
// Panel metadata
// ---------------------------------------------------------------------------
const PANEL_META = {
  overview:   { title: 'Admin Overview',      subtitle: 'Single-binary admin console with all 20 research features.' },
  workspace:  { title: 'SQL Workspace',        subtitle: 'Run SQL for compatibility or SkeinQL for full control.' },
  schema:     { title: 'Structure Manager',    subtitle: 'Create databases, design tables, and review schema.' },
  data:       { title: 'Browse & Edit',        subtitle: 'Browse rows, insert data, and run table edits.' },
  cluster:    { title: 'Cluster Manager',      subtitle: 'Plan topology, inspect transport, and manage layouts.' },
  settings:   { title: 'Settings Manager',     subtitle: 'Read and update server settings and feature config.' },
  users:      { title: 'Users & Grants',       subtitle: 'Create users, assign roles, grant database privileges.' },
  import:     { title: 'Import / Export',      subtitle: 'Bulk import data or export schemas and rows.' },
  research:   { title: 'Research Agenda',      subtitle: 'Dashboard for all 20 research tracks R01–R20.' },
  vectors:    { title: 'Vector Search (R10)',  subtitle: 'kNN search, vector insert, index status.' },
  privacy:    { title: 'Privacy & DP (R04-R05)', subtitle: 'Differential privacy aggregates and oblivious execution.' },
  forensics:  { title: 'Forensic Audit (R06)', subtitle: 'Hash-chain verification and forensic queries.' },
  views:      { title: 'Incremental Views (R08)', subtitle: 'Create, refresh, and inspect materialized views.' },
  merge:      { title: 'Merge & CRDT (R07)',   subtitle: 'Client-side merge functions and Wasm merge modules.' },
  wasm:       { title: 'Wasm Operators (R19)', subtitle: 'Compile and run Wasm query operators.' },
  advisor:    { title: 'Index Advisor (R16)',   subtitle: 'Synthesize, review, and apply index recommendations.' },
  migration:  { title: 'Migration (R17)',      subtitle: 'Compatibility rewrites and migration reports.' },
  nl:         { title: 'NL Lab (R11-R12)',     subtitle: 'NL-to-SkeinQL translation and autoparameterization.' },
  rpc:        { title: 'RPC Explorer',         subtitle: 'Full access to every SkeinDB method.' }
};

// ---------------------------------------------------------------------------
// Research tracks
// ---------------------------------------------------------------------------
const RESEARCH_TRACKS = [
  { id: 'R01', title: 'Learned Index Structures', desc: 'CDF-based learned indexes for ValueID lookup.', methods: ['system.capabilities'] },
  { id: 'R02', title: 'Adaptive Row-Column Hybrid', desc: 'Dynamic row/column execution selection.', methods: ['system.capabilities'] },
  { id: 'R03', title: 'Delta-Chain Topology', desc: 'Linear, tree, skip-list delta chains for versioned values.', methods: ['settings.get'] },
  { id: 'R04', title: 'Differential Privacy', desc: 'DP aggregates with calibrated Laplace noise.', methods: ['dp.aggregate', 'dp.budget.get', 'dp.budget.set', 'dp.audit_log'], panel: 'privacy' },
  { id: 'R05', title: 'Oblivious Execution', desc: 'Padding and dummy-row injection to hide access patterns.', methods: ['oblivious.policy.get', 'oblivious.policy.set', 'oblivious.explain'], panel: 'privacy' },
  { id: 'R06', title: 'Forensic Audit', desc: 'Hash-chained WAL with integrity verification.', methods: ['forensic.verify', 'forensic.query', 'forensic.export'], panel: 'forensics' },
  { id: 'R07', title: 'Merge & CRDT', desc: 'Client-side merge functions: LWW, max-wins, union, Wasm.', methods: ['merge.apply', 'merge.register', 'merge.simulate', 'merge.wasm.register', 'merge.wasm.list', 'merge.wasm.drop'], panel: 'merge' },
  { id: 'R08', title: 'Incremental Views', desc: 'Dependency-graph-driven materialized view maintenance.', methods: ['view.create', 'view.refresh', 'view.status', 'view.drop', 'view.explain_deps'], panel: 'views' },
  { id: 'R09', title: 'QUIC Transport', desc: 'HTTP/3 and QUIC-native database protocol.', methods: ['transport.capabilities'] },
  { id: 'R10', title: 'Vector Embeddings', desc: 'First-class vector columns with kNN search.', methods: ['vector.search', 'vector.insert', 'vector.index_status'], panel: 'vectors' },
  { id: 'R11', title: 'Autoparameterization', desc: 'LLM-assisted SQL parameterization.', methods: ['autoparam.analyze', 'autoparam.classify'], panel: 'nl' },
  { id: 'R12', title: 'NL-to-SkeinQL', desc: 'Natural language query translation with verification.', methods: ['ai.nl.translate', 'ai.nl.explain', 'ai.nl.execute'], panel: 'nl' },
  { id: 'R13', title: 'Causal Consistency', desc: 'ETag-chain causal ordering across replicas.', methods: ['query.patch'] },
  { id: 'R14', title: 'Edge Bundles', desc: 'Offline write queue with sync-on-reconnect.', methods: ['settings.get'] },
  { id: 'R15', title: 'Schema Evolution', desc: 'Online schema changes with merge-based migration.', methods: ['schema.propose_change', 'schema.merge_status', 'schema.apply_merge'] },
  { id: 'R16', title: 'Index Advisor', desc: 'Workload-driven index synthesis and recommendation.', methods: ['advisor.synthesize', 'advisor.history', 'advisor.apply', 'advisor.dismiss'], panel: 'advisor' },
  { id: 'R17', title: 'Migration Hints', desc: 'Compatibility telemetry and rewrite previews.', methods: ['migration.rewrite_preview', 'migration.intent_report'], panel: 'migration' },
  { id: 'R18', title: 'Perf Replay', desc: 'Snapshot + replay for performance regression testing.', methods: ['system.capabilities'] },
  { id: 'R19', title: 'Wasm Operators', desc: 'User-defined Wasm query plan operators.', methods: ['wasm.plan.compile', 'wasm.plan.run'], panel: 'wasm' },
  { id: 'R20', title: 'Energy-Aware Compaction', desc: 'Carbon-aware scheduling for background compaction.', methods: ['system.capabilities'] }
];

// ---------------------------------------------------------------------------
// Feature center items
// ---------------------------------------------------------------------------
const FEATURE_CENTER = [
  { title: 'SQL Compat', desc: 'MySQL-compatible SQL layer.', panel: 'workspace' },
  { title: 'SkeinQL', desc: 'Native structured query API.', panel: 'workspace' },
  { title: 'Schema Mgmt', desc: 'Create/alter DB and tables.', panel: 'schema' },
  { title: 'Data Browse', desc: 'phpMyAdmin-style data browser.', panel: 'data' },
  { title: 'Cluster', desc: 'Multi-node topology and sharding.', panel: 'cluster' },
  { title: 'CDC', desc: 'Change data capture + polling.', panel: 'rpc' },
  { title: 'Vectors', desc: 'kNN embedding search.', panel: 'vectors' },
  { title: 'Differential Privacy', desc: 'DP aggregates w/ Laplace noise.', panel: 'privacy' },
  { title: 'Oblivious Exec', desc: 'Access pattern hiding.', panel: 'privacy' },
  { title: 'Forensic Audit', desc: 'Hash-chain WAL verification.', panel: 'forensics' },
  { title: 'Views', desc: 'Incremental materialized views.', panel: 'views' },
  { title: 'Merge/CRDT', desc: 'Client merge + Wasm merge.', panel: 'merge' },
  { title: 'Wasm Ops', desc: 'Custom query plan operators.', panel: 'wasm' },
  { title: 'Index Advisor', desc: 'Workload-driven index suggestion.', panel: 'advisor' },
  { title: 'Migration', desc: 'Compat rewrites + intent reports.', panel: 'migration' },
  { title: 'NL Lab', desc: 'NL-to-SkeinQL + autoparam.', panel: 'nl' },
  { title: 'QUIC Transport', desc: 'HTTP/3 native transport.', panel: 'cluster' },
  { title: 'Import/Export', desc: 'Bulk data operations.', panel: 'import' }
];

// ---------------------------------------------------------------------------
// RPC templates
// ---------------------------------------------------------------------------
const RPC_TEMPLATES = [
  { label: 'system.ping', method: 'system.ping', params: {} },
  { label: 'system.version', method: 'system.version', params: {} },
  { label: 'system.capabilities', method: 'system.capabilities', params: {} },
  { label: 'stats.snapshot', method: 'stats.snapshot', params: {} },
  { label: 'schema.list_databases', method: 'schema.list_databases', params: {} },
  { label: 'schema.list_tables', method: 'schema.list_tables', params: { db: 'demo' } },
  { label: 'schema.create_database', method: 'schema.create_database', params: { db: 'demo' } },
  { label: 'schema.create_table', method: 'schema.create_table', params: { db:'demo', table:'users', columns:[{name:'id',type:'i64'},{name:'name',type:'string'}], primary_key:['id'], if_not_exists:true } },
  { label: 'schema.describe_table', method: 'schema.describe_table', params: { db:'demo', table:'users' } },
  { label: 'data.insert', method: 'data.insert', params: { into:{db:'demo',table:'users'}, rows:[{id:{t:'i64',v:1},name:{t:'string',v:'Ada'}}] } },
  { label: 'data.get', method: 'data.get', params: { table:{db:'demo',table:'users'}, pk:[{t:'i64',v:1}] } },
  { label: 'data.update', method: 'data.update', params: { table:{db:'demo',table:'users'}, where:{op:'=',a:{col:'id'},b:{lit:{t:'i64',v:1}}}, set:{name:{t:'string',v:'Ada Lovelace'}}, limit:1 } },
  { label: 'data.delete', method: 'data.delete', params: { table:{db:'demo',table:'users'}, where:{op:'=',a:{col:'id'},b:{lit:{t:'i64',v:1}}}, limit:1 } },
  { label: 'query.select', method: 'query.select', params: { query:{schema:'demo',table:'users',select:[{col:'id'},{col:'name'}]}, result_format:'rows_json' } },
  { label: 'query.patch', method: 'query.patch', params: { query:{schema:'demo',table:'users',select:[{col:'id'}]}, base_etag:'', include_full:true, result_format:'rows_json' } },
  { label: 'vector.search', method: 'vector.search', params: { table:{db:'demo',table:'items'}, query:{dims:3,v:[0.1,0.2,0.3]}, k:5 } },
  { label: 'dp.aggregate', method: 'dp.aggregate', params: { table:{db:'demo',table:'events'}, aggregate:{op:'count',col:'id'}, epsilon:1.0 } },
  { label: 'oblivious.policy.get', method: 'oblivious.policy.get', params: { db:'demo', table:'events' } },
  { label: 'forensic.verify', method: 'forensic.verify', params: { from_id:0, limit:100 } },
  { label: 'forensic.query', method: 'forensic.query', params: { from_id:0, limit:50 } },
  { label: 'view.create', method: 'view.create', params: { db:'demo', name:'active_users', query:{schema:'demo',table:'users',select:[{col:'id'}]} } },
  { label: 'view.status', method: 'view.status', params: { db:'demo', name:'active_users' } },
  { label: 'merge.apply', method: 'merge.apply', params: { table:{db:'demo',table:'users'}, pk:[{t:'i64',v:1}], incoming:{id:{t:'i64',v:1},name:{t:'string',v:'Ada'}} } },
  { label: 'merge.wasm.register', method: 'merge.wasm.register', params: { name:'merge_sum', wasm_b64:'AA==' } },
  { label: 'wasm.plan.compile', method: 'wasm.plan.compile', params: { wasm_b64:'AA==', schema:{columns:[{name:'x',type:'i64'}]} } },
  { label: 'advisor.synthesize', method: 'advisor.synthesize', params: { db:'demo', table:'users' } },
  { label: 'advisor.history', method: 'advisor.history', params: { db:'demo' } },
  { label: 'autoparam.analyze', method: 'autoparam.analyze', params: { sql:'SELECT * FROM users WHERE id = 42' } },
  { label: 'cdc.subscribe_table', method: 'cdc.subscribe_table', params: { db:'demo', table:'users' } },
  { label: 'cdc.poll', method: 'cdc.poll', params: { sub_id:'sub_1', from_offset:0, limit:200 } },
  { label: 'settings.get', method: 'settings.get', params: { keys:['cluster.state.v1'] } },
  { label: 'cluster.status', method: 'cluster.status', params: {} },
  { label: 'cluster.join_token.create', method: 'cluster.join_token.create', params: { ttl_ms:600000, role:'replica' } },
  { label: 'cluster.node.join', method: 'cluster.node.join', params: { token:'join_token_here', node_id:'replica-a', rpc_url:'http://127.0.0.1:8081', role:'replica' } },
  { label: 'cluster.shard.create', method: 'cluster.shard.create', params: { db:'app', table:'users', replicas:['replica-a'] } },
  { label: 'ai.nl.translate', method: 'ai.nl.translate', params: { db:'app', request:'list users who signed up this week' } },
  { label: 'migration.rewrite_preview', method: 'migration.rewrite_preview', params: {} },
  { label: 'migration.intent_report', method: 'migration.intent_report', params: {} },
  { label: 'transport.capabilities', method: 'transport.capabilities', params: {} }
];

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function $(id) { return document.getElementById(id); }
function getBaseUrl() { const v = $('baseUrl'); const raw = v ? v.value.trim() : ''; return raw || DEFAULT_BASE_URL; }
function getToken() { const v = $('token'); return v ? v.value.trim() : ''; }

async function rpc(baseUrl, token, method, params) {
  const url = baseUrl.replace(/\/$/, '') + '/api/v1/rpc';
  const body = { skeinql: '1.0', id: String(Date.now()), method, params: params || {} };
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = 'Bearer ' + token;
  const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
  return { status: res.status, json };
}

function setConnStatus(kind, message, detail) {
  STATE.connected = kind === 'ok';
  [$('connStatus'), $('connBadge')].filter(Boolean).forEach(p => {
    p.classList.remove('ok', 'warn', 'error');
    if (kind) p.classList.add(kind);
    p.textContent = message || 'Disconnected';
  });
  const s = $('connSummary');
  if (s) s.textContent = detail || message || 'Disconnected';
}

function setSelectedDb(db) {
  STATE.selectedDb = (db || '').trim();
  ['schemaDb','dataDb','dpDb','oblDb','vecDb','viewDb','mergeDb','advDb','importDb','nlDb'].forEach(id => {
    const el = $(id); if (el && !el.value.trim()) el.value = STATE.selectedDb;
  });
}

function setSelectedTable(table) {
  STATE.selectedTable = (table || '').trim();
  ['schemaTable','dataTable','dpTable','oblTable','vecTable','mergeTable','advTable','importTable'].forEach(id => {
    const el = $(id); if (el && !el.value.trim()) el.value = STATE.selectedTable;
  });
}

function resolveDefaultDb() {
  return [STATE.selectedDb, v('schemaDb'), v('dataDb')].map(s => (s||'').trim()).find(Boolean);
  function v(id) { const e = $(id); return e ? e.value : ''; }
}

function updateHeader(panel) {
  const meta = PANEL_META[panel] || PANEL_META.overview;
  const t = $('pageTitle'), s = $('pageSubtitle');
  if (t) t.textContent = STATE.isConsole && panel === 'workspace' ? 'Console Workspace' : meta.title;
  if (s) s.textContent = meta.subtitle;
}

function updateContext() {
  const srv = $('contextServer'); if (srv) srv.textContent = getBaseUrl() || '--';
  const db = $('contextDb'); if (db) db.textContent = STATE.selectedDb || '--';
  const tbl = $('contextTable'); if (tbl) tbl.textContent = STATE.selectedTable || '--';
  const h = $('contextHint');
  if (h) h.textContent = !STATE.selectedDb ? 'Select a database from the left tree.' : !STATE.selectedTable ? 'Select a table to browse.' : 'Ready.';
}

function persistInputs() {
  const b = $('baseUrl'), t = $('token');
  if (b) localStorage.setItem('skeinadmin.baseUrl', b.value);
  if (t) localStorage.setItem('skeinadmin.token', t.value);
  updateContext();
}

function loadInputs() {
  const b = $('baseUrl'), t = $('token');
  const sb = localStorage.getItem('skeinadmin.baseUrl');
  const st = localStorage.getItem('skeinadmin.token');
  if (b) b.value = sb || window.location.origin || DEFAULT_BASE_URL;
  if (t && st) t.value = st;
  updateContext();
}

function setOut(obj, targetId = 'out') {
  const el = $(targetId); if (!el) return;
  el.textContent = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
}

async function call(method, params = {}, targetId = 'out') {
  const baseUrl = getBaseUrl(), token = getToken();
  setConnStatus('warn', 'Connecting', 'Connecting to ' + baseUrl);
  try {
    const res = await rpc(baseUrl, token, method, params || {});
    setOut(res, targetId);
    if (res.json && res.json.ok) setConnStatus('ok', 'Connected', 'Connected to ' + baseUrl);
    else setConnStatus('error', 'RPC error', 'RPC error from ' + baseUrl);
    return res;
  } catch (e) {
    setOut({ error: String(e), hint: baseUrl !== window.location.origin ? 'Cross-origin? Enable CORS.' : 'Server unreachable.' }, targetId);
    setConnStatus('error', 'Offline', 'Unable to reach ' + baseUrl);
    throw e;
  }
}

function parseJsonInput(raw, label) {
  const t = raw.trim(); if (!t) return null;
  try { return JSON.parse(t); } catch (e) { throw new Error(label + ' JSON invalid: ' + e.message); }
}

function parseJsonArrayInput(id, label) {
  const raw = $(id) ? $(id).value.trim() : ''; if (!raw) return undefined;
  const p = parseJsonInput(raw, label); if (!Array.isArray(p)) throw new Error(label + ' must be array'); return p;
}

function cleanParams(p) {
  const o = { ...p };
  Object.keys(o).forEach(k => { const v = o[k]; if (v === undefined || v === null || v === '' || (Array.isArray(v) && !v.length)) delete o[k]; });
  return o;
}

function formatBytes(b) {
  if (!Number.isFinite(b)) return '--';
  const u = ['B','KB','MB','GB','TB']; let v = b, i = 0;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return v.toFixed(v >= 10 || i === 0 ? 0 : 1) + ' ' + u[i];
}

function formatLit(value) {
  if (value === null || value === undefined) return '';
  if (typeof value !== 'object') return String(value);
  if (!value.t) return JSON.stringify(value);
  if (value.t === 'null') return 'null';
  if ('v' in value) return String(value.v);
  if ('iso' in value) return value.iso;
  if ('b64' in value) return value.b64;
  if ('dims' in value) return 'vec[' + value.dims + ']';
  return JSON.stringify(value);
}

function tableRef(db, table) { return { db, table }; }

function readDbTable(dbId, tableId) {
  const db = $(dbId) ? $(dbId).value.trim() : '';
  const table = $(tableId) ? $(tableId).value.trim() : '';
  if (!db || !table) throw new Error('Database and table are required');
  return tableRef(db, table);
}

function escapeHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ---------------------------------------------------------------------------
// Table rendering
// ---------------------------------------------------------------------------
function renderTable(targetId, columns, rows) {
  const table = $(targetId); if (!table) return;
  table.textContent = '';
  if (!columns || !columns.length) return;
  const thead = document.createElement('thead');
  const hr = document.createElement('tr');
  columns.forEach(c => { const th = document.createElement('th'); th.textContent = c; hr.appendChild(th); });
  thead.appendChild(hr); table.appendChild(thead);
  const tbody = document.createElement('tbody');
  (rows || []).forEach(row => {
    const tr = document.createElement('tr');
    row.forEach(cell => { const td = document.createElement('td'); td.textContent = formatLit(cell); tr.appendChild(td); });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
}

function renderStructure(result) {
  const cols = Array.isArray(result.columns) ? result.columns : [];
  renderTable('structureTable', ['Column','Type','Nullable','Auto Inc'], cols.map(c => [
    c.name, c.type && c.type.kind ? c.type.kind : JSON.stringify(c.type||''), c.nullable ? 'YES' : 'NO', c.auto_increment ? 'YES' : 'NO'
  ]));
  const bc = $('structureBreadcrumb');
  if (bc) bc.textContent = result.db && result.table ? result.db + ' / ' + result.table : 'No table selected.';
}

function normalizeSqlColumnName(col, i) {
  if (typeof col === 'string') return col;
  if (!col || typeof col !== 'object') return 'col' + (i+1);
  return col.name || col.col || 'col' + (i+1);
}

function extractSqlTable(result) {
  const dn = result && result.result && result.result.data ? result.result.data : null;
  if (dn && Array.isArray(dn.columns) && Array.isArray(dn.rows)) return { columns: dn.columns.map(normalizeSqlColumnName), rows: dn.rows };
  if (Array.isArray(result && result.columns)) return { columns: ['Column','Type','Nullable','Auto Inc'], rows: result.columns.map(c => [c.name||'', c.type && c.type.kind || '', c.nullable?'YES':'NO', c.auto_increment?'YES':'NO']) };
  if (Array.isArray(result && result.result) && result.result.length > 0) {
    const f = result.result[0]; if (f && typeof f === 'object' && !Array.isArray(f)) { const cols = Object.keys(f); return { columns: cols, rows: result.result.map(o => cols.map(k => o[k])) }; }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------
function updateStats(s) {
  if (!s) return;
  if ($('statUptime')) $('statUptime').textContent = Number.isFinite(s.uptime_s) ? s.uptime_s + 's' : '--';
  if ($('statCpu')) $('statCpu').textContent = s.process && Number.isFinite(s.process.cpu_pct) ? s.process.cpu_pct.toFixed(1)+'%' : '--';
  if ($('statRss')) $('statRss').textContent = s.process ? formatBytes(s.process.rss_bytes) : '--';
  if ($('statQps')) $('statQps').textContent = (s.qps !== undefined ? s.qps : '--') + ' / ' + (s.tps !== undefined ? s.tps : '--');
}

async function loadStats() {
  const res = await call('stats.snapshot', {}, 'out');
  if (res && res.json && res.json.ok && res.json.result) { updateStats(res.json.result); setOut(res.json.result, 'out'); }
}

// ---------------------------------------------------------------------------
// Connect / disconnect
// ---------------------------------------------------------------------------
async function ping() {
  const t0 = performance.now();
  const res = await call('system.ping', {}, 'out');
  const ms = Math.round(performance.now() - t0);
  if (res && res.json && res.json.ok) { if ($('infoPing')) $('infoPing').textContent = ms + ' ms'; setConnStatus('ok', 'Connected', 'Latency ' + ms + ' ms'); }
  return res;
}

async function loadVersion() {
  const res = await call('system.version', {}, 'out');
  if (res && res.json && res.json.ok && res.json.result) {
    const r = res.json.result;
    if ($('infoVersion')) $('infoVersion').textContent = r.version || '--';
    if ($('infoSkeinql')) $('infoSkeinql').textContent = r.skeinql || '--';
  }
}

async function loadTransport() {
  const res = await call('transport.capabilities', {}, 'out');
  if (res && res.json && res.json.ok && res.json.result) {
    const t = res.json.result;
    if ($('infoTransport')) $('infoTransport').textContent = 'http=' + (t.http?'on':'off') + ' quic=' + (t.quic?'on':'off');
  }
}

async function loadCapabilities() {
  const res = await call('system.capabilities', {}, 'capabilitiesOut');
  if (res && res.json && res.json.ok && res.json.result) {
    const caps = res.json.result;
    setOut(caps, 'capabilitiesOut');
    const methods = Array.isArray(caps.methods) ? caps.methods : [];
    STATE.methods = methods;
    if ($('statMethods')) $('statMethods').textContent = methods.length;
    populateMethodSelect(methods);
    renderMethodList(methods, $('methodSearch') ? $('methodSearch').value : '');
  }
}

async function connect() {
  try {
    await ping(); await loadVersion(); await loadCapabilities(); await loadTransport(); await loadStats();
    await clusterReadStatus(); await loadDbTree(); updateContext();
    if ($('statDatabases')) $('statDatabases').textContent = Object.keys(STATE.dbTree).length;
  } catch {}
}

function disconnect() {
  setConnStatus('warn', 'Disconnected', 'Disconnected.');
  STATE.methods = []; STATE.dbTree = {};
  setSelectedDb(''); setSelectedTable('');
  renderDbTree({}, '');
  renderTable('browseTable', [], []); renderTable('structureTable', [], []); renderTable('sqlTable', [], []);
  updateContext();
}

// ---------------------------------------------------------------------------
// Profiles
// ---------------------------------------------------------------------------
function getProfiles() { try { return JSON.parse(localStorage.getItem('skeinadmin.profiles') || '{}'); } catch { return {}; } }
function saveProfiles(p) { localStorage.setItem('skeinadmin.profiles', JSON.stringify(p)); }

function refreshProfiles(sel) {
  const s = $('profileSelect'); if (!s) return;
  s.textContent = '';
  const e = document.createElement('option'); e.value = ''; e.textContent = 'Select profile'; s.appendChild(e);
  Object.keys(getProfiles()).sort().forEach(n => { const o = document.createElement('option'); o.value = n; o.textContent = n; s.appendChild(o); });
  if (sel) s.value = sel;
}

function saveProfile() {
  const n = $('profileName') ? $('profileName').value.trim() : ''; if (!n) return;
  const p = getProfiles(); p[n] = { baseUrl: getBaseUrl(), token: getToken() }; saveProfiles(p); refreshProfiles(n);
}

function deleteProfile() {
  const s = $('profileSelect'); if (!s || !s.value) return;
  const p = getProfiles(); delete p[s.value]; saveProfiles(p); refreshProfiles('');
}

function loadProfile(n) {
  const p = getProfiles()[n]; if (!p) return;
  if ($('baseUrl')) $('baseUrl').value = p.baseUrl || '';
  if ($('token')) $('token').value = p.token || '';
  persistInputs(); setConnStatus('warn', 'Disconnected', 'Profile loaded.');
}

// ---------------------------------------------------------------------------
// DB Tree
// ---------------------------------------------------------------------------
function renderDbTree(tree, filter) {
  const target = $('dbTree'); if (!target) return;
  target.textContent = '';
  const match = filter ? filter.toLowerCase() : '';
  const dbs = Object.keys(tree).sort();
  if (!dbs.length) { target.textContent = 'No databases.'; return; }
  dbs.forEach(db => {
    const tables = tree[db] || [];
    const tm = match ? tables.filter(t => t.toLowerCase().includes(match)) : tables;
    if (match && !db.toLowerCase().includes(match) && !tm.length) return;
    const det = document.createElement('details'); det.open = true;
    const sum = document.createElement('summary'); sum.textContent = db; det.appendChild(sum);
    const list = document.createElement('div'); list.className = 'tree-table';
    tm.forEach(tbl => {
      const btn = document.createElement('button'); btn.textContent = tbl;
      btn.addEventListener('click', async () => { setSelectedDb(db); setSelectedTable(tbl); updateContext(); await schemaDescribe(); });
      list.appendChild(btn);
    });
    det.appendChild(list); target.appendChild(det);
  });
}

async function loadDbTree() {
  const res = await call('schema.list_databases', {}, 'schemaOut');
  if (!res || !res.json || !res.json.ok) return;
  const dbs = res.json.result && Array.isArray(res.json.result.databases) ? res.json.result.databases : [];
  const tree = {};
  for (const db of dbs) {
    const tr = await call('schema.list_tables', { db }, 'schemaOut');
    tree[db] = tr && tr.json && tr.json.ok ? (tr.json.result.tables || []) : [];
  }
  STATE.dbTree = tree;
  renderDbTree(tree, $('dbSearch') ? $('dbSearch').value : '');
  updateContext();
}

function renderDatabaseList(dbs) {
  const t = $('dbList'); if (!t) return; t.textContent = '';
  if (!dbs || !dbs.length) { t.textContent = 'No databases.'; return; }
  dbs.forEach(db => {
    const btn = document.createElement('button'); btn.textContent = db;
    btn.addEventListener('click', async () => { setSelectedDb(db); setSelectedTable(''); await schemaListTables(); updateContext(); });
    t.appendChild(btn);
  });
}

function renderTableList(tables) {
  const t = $('tableList'); if (!t) return; t.textContent = '';
  if (!tables || !tables.length) { t.textContent = 'No tables.'; return; }
  tables.forEach(tbl => {
    const btn = document.createElement('button'); btn.textContent = tbl;
    btn.addEventListener('click', () => { setSelectedTable(tbl); updateContext(); });
    t.appendChild(btn);
  });
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------
async function schemaListDatabases() {
  const res = await call('schema.list_databases', {}, 'schemaOut');
  if (res && res.json && res.json.ok && res.json.result) renderDatabaseList(res.json.result.databases || []);
}

async function schemaListTables() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : ''; if (!db) return;
  const res = await call('schema.list_tables', { db }, 'schemaOut');
  if (res && res.json && res.json.ok && res.json.result) { setSelectedDb(db); renderTableList(res.json.result.tables || []); }
}

async function schemaDescribe() {
  try {
    const db = $('schemaDb').value.trim(), table = $('schemaTable').value.trim();
    if (!db || !table) throw new Error('DB and table required');
    const res = await call('schema.describe_table', { db, table }, 'schemaOut');
    if (res && res.json && res.json.ok && res.json.result) { renderStructure(res.json.result); setOut(res.json.result, 'structureOut'); }
  } catch (e) { setOut({ error: String(e) }, 'schemaOut'); }
}

async function schemaCreateDb() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : ''; if (!db) return;
  const res = await call('schema.create_database', { db }, 'schemaOut');
  if (res && res.json && res.json.ok) await loadDbTree();
}

async function schemaCreateTable() {
  try {
    const db = $('schemaDb').value.trim(), table = $('schemaTable').value.trim(); if (!db || !table) throw new Error('DB+table required');
    const columns = parseJsonInput($('schemaColumns').value, 'Columns'); if (!Array.isArray(columns)) throw new Error('Columns must be array');
    const pk = ($('schemaPk').value.trim() || '').split(',').map(c => c.trim()).filter(Boolean);
    const ine = $('schemaIfNotExists').value === 'true';
    const res = await call('schema.create_table', { db, table, columns, primary_key: pk, if_not_exists: ine }, 'schemaOut');
    if (res && res.json && res.json.ok) await loadDbTree();
  } catch (e) { setOut({ error: String(e) }, 'schemaOut'); }
}

async function schemaDropDb() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : ''; if (!db) return;
  if (!confirm('Drop database "' + db + '"? This cannot be undone.')) return;
  await call('schema.drop_database', { db }, 'schemaOut');
  await loadDbTree();
}

async function schemaDropTable() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : '', table = $('schemaTable') ? $('schemaTable').value.trim() : '';
  if (!db || !table) return;
  if (!confirm('Drop table "' + db + '.' + table + '"?')) return;
  await call('schema.drop_table', { db, table }, 'schemaOut');
  await loadDbTree();
}

async function schemaProposeChange() {
  try {
    const db = $('schemaDb').value.trim(), table = $('schemaTable').value.trim(); if (!db || !table) throw new Error('DB+table required');
    const columns = parseJsonInput($('schemaColumns').value, 'Columns');
    await call('schema.propose_change', cleanParams({ db, table, columns }), 'schemaOut');
  } catch (e) { setOut({ error: String(e) }, 'schemaOut'); }
}

async function schemaMergeStatus() {
  try {
    const db = $('schemaDb').value.trim(), table = $('schemaTable').value.trim(); if (!db || !table) throw new Error('DB+table required');
    await call('schema.merge_status', { db, table }, 'schemaOut');
  } catch (e) { setOut({ error: String(e) }, 'schemaOut'); }
}

async function schemaApplyMerge() {
  try {
    const db = $('schemaDb').value.trim(), table = $('schemaTable').value.trim(); if (!db || !table) throw new Error('DB+table required');
    await call('schema.apply_merge', { db, table }, 'schemaOut');
  } catch (e) { setOut({ error: String(e) }, 'schemaOut'); }
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------
async function dataGet() {
  try { const t = readDbTable('dataDb','dataTable'); const pk = parseJsonInput($('dataPk').value,'PK') || []; await call('data.get',{table:t,pk},'dataOut'); } catch (e) { setOut({error:String(e)},'dataOut'); }
}

async function dataInsert() {
  try { const t = readDbTable('dataDb','dataTable'); const rows = parseJsonInput($('dataRows').value,'Rows') || []; await call('data.insert',{into:t,rows},'dataOut'); } catch (e) { setOut({error:String(e)},'dataOut'); }
}

async function dataUpdate() {
  try {
    const t = readDbTable('dataDb','dataTable');
    const w = parseJsonInput($('dataWhere').value,'Where'), s = parseJsonInput($('dataSet').value,'Set');
    if (!w || !s) throw new Error('Where+Set required');
    const lim = parseInt($('dataLimit').value,10);
    await call('data.update', cleanParams({table:t,where:w,set:s,limit:Number.isNaN(lim)?undefined:lim}),'dataOut');
  } catch (e) { setOut({error:String(e)},'dataOut'); }
}

async function dataDelete() {
  try {
    const t = readDbTable('dataDb','dataTable');
    const w = parseJsonInput($('dataWhere').value,'Where'); if (!w) throw new Error('Where required');
    const lim = parseInt($('dataLimit').value,10);
    await call('data.delete', cleanParams({table:t,where:w,limit:Number.isNaN(lim)?undefined:lim}),'dataOut');
  } catch (e) { setOut({error:String(e)},'dataOut'); }
}

async function browseTable() {
  try {
    const t = readDbTable('dataDb','dataTable');
    const limit = parseInt($('dataBrowseLimit').value,10) || 50;
    const offset = parseInt($('dataBrowseOffset').value,10) || 0;
    const orderCol = $('dataBrowseOrder').value.trim();
    STATE.browseOffset = offset;
    const desc = await call('schema.describe_table', {db:t.db,table:t.table},'browseOut');
    if (!desc || !desc.json || !desc.json.ok) return;
    const cols = Array.isArray(desc.json.result.columns) ? desc.json.result.columns.map(c=>c.name) : [];
    const projection = cols.map(n => ({expr:{col:n},as:null}));
    const query = { with:[], body:{select:{projection,from:[{db:t.db,table:t.table}]}}, order_by: orderCol ? [{expr:{col:orderCol},dir:'asc'}] : [], limit:{limit,offset} };
    const res = await call('query.select',{query,result_format:'rows_json'},'browseOut');
    if (res && res.json && res.json.ok && res.json.result && res.json.result.data) {
      const d = res.json.result.data;
      renderTable('browseTable', (d.columns||[]).map(c=>c.name), d.rows||[]);
      const info = $('browsePageInfo'); if (info) info.textContent = 'Showing ' + (d.rows||[]).length + ' rows (offset ' + offset + ')';
    }
  } catch (e) { setOut({error:String(e)},'browseOut'); }
}

function browsePrev() {
  const limit = parseInt($('dataBrowseLimit').value,10) || 50;
  const cur = parseInt($('dataBrowseOffset').value,10) || 0;
  $('dataBrowseOffset').value = Math.max(0, cur - limit);
  browseTable();
}

function browseNext() {
  const limit = parseInt($('dataBrowseLimit').value,10) || 50;
  const cur = parseInt($('dataBrowseOffset').value,10) || 0;
  $('dataBrowseOffset').value = cur + limit;
  browseTable();
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------
function setSqlText(v) { if ($('sqlText')) { $('sqlText').value = v; $('sqlText').focus(); } }

async function runSql(explain) {
  const sql = $('sqlText') ? $('sqlText').value.trim() : ''; if (!sql) return;
  const res = await call('sql.exec', cleanParams({sql, explain:!!explain, default_db:resolveDefaultDb()}), 'sqlOut');
  if (!res || !res.json || !res.json.ok || !res.json.result) return;
  const r = res.json.result;
  const tbl = extractSqlTable(r); if (tbl) renderTable('sqlTable', tbl.columns, tbl.rows); else renderTable('sqlTable',[],[]);
  if (r.statement === 'use' && r.default_db) { setSelectedDb(r.default_db); updateContext(); }
  if (r.statement === 'create_database' || r.statement === 'create_table') await loadDbTree();
}

async function runSkeinQuery() {
  try {
    const method = $('skeinMethod').value, format = $('skeinFormat').value;
    const args = parseJsonInput($('skeinArgs').value,'Args') || [];
    const qid = $('skeinQueryId').value.trim();
    const baseEtag = $('skeinBaseEtag').value.trim();
    const incFull = $('skeinIncludeFull').value === 'true';
    const params = {};
    if (method === 'query.execute_prepared') { if (!qid) throw new Error('Query id required'); params.query_id = qid; if (args.length) params.args = args; if (format) params.result_format = format; }
    else if (method === 'query.prepare') { const q = parseJsonInput($('skeinQuery').value,'Query'); if (!q) throw new Error('Query required'); params.query = q; }
    else { const q = parseJsonInput($('skeinQuery').value,'Query'); if (!q) throw new Error('Query required'); params.query = q; if (args.length) params.args = args; if (format) params.result_format = format; if (method === 'query.patch') { if (baseEtag) params.base_etag = baseEtag; params.include_full = incFull; } }
    const res = await call(method, params, 'skeinOut');
    if (method === 'query.prepare' && res && res.json && res.json.ok && res.json.result && $('skeinQueryId')) $('skeinQueryId').value = res.json.result.query_id || '';
  } catch (e) { setOut({error:String(e)},'skeinOut'); }
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------
async function clusterReadStatus() { await call('cluster.status',{},'clusterOut'); }
async function clusterReadNodes() { await call('cluster.nodes',{},'clusterOut'); }
async function clusterTransportCapabilities() { await call('transport.capabilities',{},'clusterOut'); }

async function clusterCreateToken() {
  try {
    const ttl = parseInt($('clusterJoinTtl').value,10), role = $('clusterJoinRole') ? $('clusterJoinRole').value : 'replica';
    const res = await call('cluster.join_token.create', cleanParams({ttl_ms:Number.isNaN(ttl)?undefined:ttl,role}), 'clusterOut');
    if (res && res.json && res.json.ok && res.json.result && $('clusterJoinToken')) $('clusterJoinToken').value = res.json.result.token || '';
  } catch (e) { setOut({error:String(e)},'clusterOut'); }
}

async function clusterJoinNode() {
  try {
    const token = $('clusterJoinToken')?.value.trim(), nodeId = $('clusterNodeId')?.value.trim(), nodeUrl = $('clusterNodeUrl')?.value.trim();
    if (!token || !nodeId || !nodeUrl) throw new Error('Token, id, url required');
    await call('cluster.node.join', cleanParams({token,node_id:nodeId,rpc_url:nodeUrl,role:$('clusterJoinRole')?.value}), 'clusterOut');
  } catch (e) { setOut({error:String(e)},'clusterOut'); }
}

async function clusterRemoveNode() {
  try { const id = $('clusterNodeId')?.value.trim(); if (!id) throw new Error('Node id required'); await call('cluster.node.remove',{node_id:id},'clusterOut'); } catch (e) { setOut({error:String(e)},'clusterOut'); }
}

async function clusterPromoteNode() {
  try {
    const id = $('clusterNodeId')?.value.trim(); if (!id) throw new Error('Node id required');
    const shard = $('clusterShardId')?.value.trim();
    await call('cluster.replica.promote', cleanParams({node_id:id,shard_id:shard||undefined}), 'clusterOut');
  } catch (e) { setOut({error:String(e)},'clusterOut'); }
}

async function clusterShardCreate() {
  try {
    const db = $('clusterShardDb')?.value.trim(); if (!db) throw new Error('DB required');
    const table = $('clusterShardTable')?.value.trim(), shardId = $('clusterShardId')?.value.trim();
    const primary = $('clusterShardPrimary')?.value.trim(), replicas = parseJsonArrayInput('clusterShardReplicas','Replicas');
    await call('cluster.shard.create', cleanParams({db,table:table||undefined,shard_id:shardId||undefined,primary_node_id:primary||undefined,replicas}), 'clusterOut');
  } catch (e) { setOut({error:String(e)},'clusterOut'); }
}

async function clusterShardMove() {
  try {
    const shardId = $('clusterShardId')?.value.trim(), toNode = $('clusterMoveTarget')?.value.trim();
    if (!shardId || !toNode) throw new Error('Shard id + target required');
    await call('cluster.shard.move',{shard_id:shardId,to_node_id:toNode,dry_run:false},'clusterOut');
  } catch (e) { setOut({error:String(e)},'clusterOut'); }
}

async function clusterShardRebalance() { await call('cluster.shard.rebalance',{max_moves:8,dry_run:false},'clusterOut'); }

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------
async function settingsGetKey() {
  try { const k = $('settingsKey')?.value.trim(); if (!k) throw new Error('Key required'); const res = await call('settings.get',{keys:[k]},'settingsOut'); if (res?.json?.ok && res.json.result) { const v = res.json.result[k]; if (v !== undefined && $('settingsValue')) $('settingsValue').value = JSON.stringify(v); } } catch (e) { setOut({error:String(e)},'settingsOut'); }
}

async function settingsSetKey() {
  try {
    const k = $('settingsKey')?.value.trim(); if (!k) throw new Error('Key required');
    const raw = $('settingsValue')?.value.trim(); if (!raw) throw new Error('Value required');
    const v = parseJsonInput(raw,'Settings value'); const payload = {}; payload[k] = v;
    await call('settings.set', payload, 'settingsOut');
  } catch (e) { setOut({error:String(e)},'settingsOut'); }
}

async function settingsListAll() { await call('settings.list',{},'settingsOut'); }

// ---------------------------------------------------------------------------
// Users & Grants
// ---------------------------------------------------------------------------
async function userCreate() {
  try {
    const name = $('userName')?.value.trim(), pass = $('userPass')?.value.trim(), role = $('userRole')?.value;
    if (!name) throw new Error('Username required');
    await call('settings.set', { ['user.' + name]: { password: pass, role, grants: {} } }, 'usersOut');
  } catch (e) { setOut({error:String(e)},'usersOut'); }
}

async function userList() { await call('settings.get', { keys: ['users'] }, 'usersOut'); }

async function userDrop() {
  const name = $('userName')?.value.trim(); if (!name) return;
  await call('settings.set', { ['user.' + name]: null }, 'usersOut');
}

async function userGrant() {
  try {
    const name = $('userName')?.value.trim(), db = $('userGrantDb')?.value.trim(), privs = $('userGrantPrivs')?.value.trim();
    if (!name || !db) throw new Error('User + db required');
    await call('settings.set', { ['grant.' + name + '.' + db]: { privileges: privs ? privs.split(',').map(s=>s.trim()) : ['SELECT'] } }, 'usersOut');
  } catch (e) { setOut({error:String(e)},'usersOut'); }
}

// ---------------------------------------------------------------------------
// Import / Export
// ---------------------------------------------------------------------------
async function exportData() {
  try {
    const t = readDbTable('importDb','importTable');
    const res = await call('query.select', { query: { schema: t.db, table: t.table, select: [{ col: '*' }] }, result_format: 'rows_json' }, 'importOut');
    if (res?.json?.ok && res.json.result) {
      const fmt = $('importFormat')?.value || 'json';
      if (fmt === 'json') downloadBlob(JSON.stringify(res.json.result, null, 2), t.db + '_' + t.table + '.json', 'application/json');
      else setOut(res.json.result, 'importOut');
    }
  } catch (e) { setOut({error:String(e)},'importOut'); }
}

async function exportSchema() {
  try {
    const db = $('importDb')?.value.trim(); if (!db) throw new Error('DB required');
    const res = await call('schema.list_tables', { db }, 'importOut');
    if (res?.json?.ok && res.json.result) {
      const tables = res.json.result.tables || [];
      const schemas = {};
      for (const t of tables) { const d = await call('schema.describe_table',{db,table:t},'importOut'); if (d?.json?.ok) schemas[t] = d.json.result; }
      downloadBlob(JSON.stringify(schemas, null, 2), db + '_schema.json', 'application/json');
    }
  } catch (e) { setOut({error:String(e)},'importOut'); }
}

async function exportAll() {
  try { const db = $('importDb')?.value.trim(); if (!db) throw new Error('DB required'); await exportSchema(); setOut({ok:true,message:'Exported schema for '+db},'importOut'); } catch (e) { setOut({error:String(e)},'importOut'); }
}

async function importData() {
  try {
    const t = readDbTable('importDb','importTable');
    const raw = $('importData')?.value.trim(); if (!raw) throw new Error('Data required');
    const data = parseJsonInput(raw, 'Import data');
    const rows = Array.isArray(data) ? data : [data];
    // Convert plain objects to typed values
    const typedRows = rows.map(row => {
      const out = {};
      for (const [k, v] of Object.entries(row)) {
        if (v && typeof v === 'object' && 't' in v) out[k] = v;
        else if (typeof v === 'number') out[k] = { t: Number.isInteger(v) ? 'i64' : 'f64', v };
        else if (typeof v === 'string') out[k] = { t: 'string', v };
        else if (typeof v === 'boolean') out[k] = { t: 'bool', v };
        else if (v === null) out[k] = { t: 'null' };
        else out[k] = { t: 'string', v: JSON.stringify(v) };
      }
      return out;
    });
    await call('data.insert', { into: t, rows: typedRows }, 'importOut');
  } catch (e) { setOut({error:String(e)},'importOut'); }
}

function downloadBlob(content, filename, mime) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

// ---------------------------------------------------------------------------
// Research Dashboard
// ---------------------------------------------------------------------------
function renderResearchDashboard() {
  const grid = $('researchDashboard'); if (!grid) return;
  grid.textContent = '';
  RESEARCH_TRACKS.forEach(track => {
    const card = document.createElement('div'); card.className = 'research-card';
    card.innerHTML = '<h3>' + escapeHtml(track.id) + ' — ' + escapeHtml(track.title) + '</h3>' +
      '<div class="desc">' + escapeHtml(track.desc) + '</div>' +
      '<div class="hint">Methods: ' + escapeHtml(track.methods.join(', ')) + '</div>';
    const acts = document.createElement('div'); acts.className = 'actions';
    if (track.panel) {
      const btn = document.createElement('button'); btn.className = 'sm primary'; btn.textContent = 'Open Panel';
      btn.addEventListener('click', () => setActivePanel(track.panel, true));
      acts.appendChild(btn);
    }
    track.methods.forEach(m => {
      const btn = document.createElement('button'); btn.className = 'sm ghost'; btn.textContent = m;
      btn.addEventListener('click', () => { if ($('rpcMethod')) $('rpcMethod').value = m; if ($('rpcParams')) $('rpcParams').value = '{}'; setActivePanel('rpc', true); });
      acts.appendChild(btn);
    });
    card.appendChild(acts); grid.appendChild(card);
  });
}

function renderFeatureCenterGrid() {
  const grid = $('featureCenterGrid'); if (!grid) return;
  grid.textContent = '';
  FEATURE_CENTER.forEach(f => {
    const card = document.createElement('div'); card.className = 'feature-card';
    card.innerHTML = '<div class="feature-title">' + escapeHtml(f.title) + '</div><div class="hint">' + escapeHtml(f.desc) + '</div>';
    const btn = document.createElement('button'); btn.className = 'sm'; btn.textContent = 'Open';
    btn.addEventListener('click', () => setActivePanel(f.panel, true));
    card.appendChild(btn); grid.appendChild(card);
  });
}

function renderResearchSettings() {
  const grid = $('researchSettingsGrid'); if (!grid) return;
  grid.textContent = '';
  RESEARCH_TRACKS.forEach(track => {
    const card = document.createElement('div'); card.className = 'feature-card';
    card.innerHTML = '<div class="feature-title">' + escapeHtml(track.id) + '</div><div class="hint">' + escapeHtml(track.title) + '</div>';
    const toggle = document.createElement('label'); toggle.style.cssText = 'display:flex;gap:4px;align-items:center;font-size:11px;cursor:pointer;';
    const cb = document.createElement('input'); cb.type = 'checkbox'; cb.checked = true; cb.dataset.track = track.id;
    toggle.appendChild(cb); toggle.appendChild(document.createTextNode('Enabled'));
    card.appendChild(toggle); grid.appendChild(card);
  });
}

async function researchSettingsLoad() {
  try {
    await call('settings.get', { keys: ['research.config'] }, 'researchSettingsOut');
  } catch (e) { setOut({error:String(e)}, 'researchSettingsOut'); }
}

async function researchSettingsSave() {
  try {
    const grid = $('researchSettingsGrid'); if (!grid) return;
    const config = {};
    grid.querySelectorAll('input[type="checkbox"]').forEach(cb => { config[cb.dataset.track] = { enabled: cb.checked }; });
    await call('settings.set', { 'research.config': config }, 'researchSettingsOut');
  } catch (e) { setOut({error:String(e)}, 'researchSettingsOut'); }
}

// ---------------------------------------------------------------------------
// Vectors (R10)
// ---------------------------------------------------------------------------
async function vecSearch() {
  try {
    const t = readDbTable('vecDb','vecTable');
    const raw = $('vecQuery')?.value.trim(); if (!raw) throw new Error('Query vector required');
    const v = raw.split(',').map(Number);
    const k = parseInt($('vecK')?.value,10) || 5;
    const col = $('vecCol')?.value.trim();
    const prefilter = $('vecPrefilter')?.value.trim();
    const params = cleanParams({ table: t, query: { dims: v.length, v }, k, column: col || undefined, prefilter: prefilter ? parseJsonInput(prefilter,'Prefilter') : undefined });
    await call('vector.search', params, 'vecOut');
  } catch (e) { setOut({error:String(e)},'vecOut'); }
}

async function vecInsert() {
  try {
    const t = readDbTable('vecDb','vecTable');
    const raw = $('vecQuery')?.value.trim(); if (!raw) throw new Error('Vector required');
    const v = raw.split(',').map(Number);
    const col = $('vecCol')?.value.trim() || 'embedding';
    await call('vector.insert', { table: t, column: col, vector: { dims: v.length, v } }, 'vecOut');
  } catch (e) { setOut({error:String(e)},'vecOut'); }
}

async function vecIndexStatus() {
  try { const t = readDbTable('vecDb','vecTable'); await call('vector.index_status',{table:t},'vecOut'); } catch (e) { setOut({error:String(e)},'vecOut'); }
}

// ---------------------------------------------------------------------------
// Privacy / DP (R04-R05)
// ---------------------------------------------------------------------------
async function dpAggregate() {
  try {
    const t = readDbTable('dpDb','dpTable');
    const op = $('dpOp')?.value || 'count', col = $('dpCol')?.value.trim() || 'id', eps = parseFloat($('dpEpsilon')?.value) || 1.0;
    await call('dp.aggregate', { table: t, aggregate: { op, col }, epsilon: eps }, 'dpOut');
  } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function dpBudgetGet() {
  try { const t = readDbTable('dpDb','dpTable'); await call('dp.budget.get',{table:t},'dpOut'); } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function dpBudgetSet() {
  try {
    const t = readDbTable('dpDb','dpTable');
    const eps = parseFloat($('dpEpsilon')?.value) || 1.0;
    await call('dp.budget.set',{table:t,budget:{total_epsilon:eps}},'dpOut');
  } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function dpAudit() {
  try { const t = readDbTable('dpDb','dpTable'); await call('dp.audit_log',{table:t},'dpOut'); } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function oblGet() {
  try { const t = readDbTable('oblDb','oblTable'); await call('oblivious.policy.get',{db:t.db,table:t.table},'oblOut'); } catch (e) { setOut({error:String(e)},'oblOut'); }
}

async function oblSet() {
  try {
    const t = readDbTable('oblDb','oblTable');
    const enabled = $('oblEnabled')?.value === 'true';
    const minBatch = parseInt($('oblMinBatch')?.value,10) || 64;
    const dummyRatio = parseFloat($('oblDummyRatio')?.value) || 0.1;
    await call('oblivious.policy.set',{db:t.db,table:t.table,enabled,min_batch_size:minBatch,dummy_ratio:dummyRatio},'oblOut');
  } catch (e) { setOut({error:String(e)},'oblOut'); }
}

async function oblExplain() {
  try { const t = readDbTable('oblDb','oblTable'); await call('oblivious.explain',{db:t.db,table:t.table},'oblOut'); } catch (e) { setOut({error:String(e)},'oblOut'); }
}

// ---------------------------------------------------------------------------
// Forensics (R06)
// ---------------------------------------------------------------------------
async function forVerify() {
  try {
    const from = parseInt($('forFromId')?.value,10) || 0, limit = parseInt($('forLimit')?.value,10) || 100;
    await call('forensic.verify',{from_id:from,limit},'forOut');
  } catch (e) { setOut({error:String(e)},'forOut'); }
}

async function forQuery() {
  try {
    const from = parseInt($('forFromId')?.value,10) || 0, limit = parseInt($('forLimit')?.value,10) || 100;
    const filter = parseJsonInput($('forFilter')?.value || '','Filter');
    await call('forensic.query', cleanParams({from_id:from,limit,filter:filter||undefined}), 'forOut');
  } catch (e) { setOut({error:String(e)},'forOut'); }
}

async function forExport() {
  try {
    const from = parseInt($('forFromId')?.value,10) || 0, limit = parseInt($('forLimit')?.value,10) || 100;
    await call('forensic.export',{from_id:from,limit},'forOut');
  } catch (e) { setOut({error:String(e)},'forOut'); }
}

// ---------------------------------------------------------------------------
// Views (R08)
// ---------------------------------------------------------------------------
async function viewCreate() {
  try {
    const db = $('viewDb')?.value.trim(), name = $('viewName')?.value.trim(); if (!db || !name) throw new Error('DB+name required');
    const query = parseJsonInput($('viewQuery')?.value,'Query'); if (!query) throw new Error('Query required');
    await call('view.create',{db,name,query},'viewOut');
  } catch (e) { setOut({error:String(e)},'viewOut'); }
}

async function viewRefresh() {
  try { const db = $('viewDb')?.value.trim(), name = $('viewName')?.value.trim(); if (!db||!name) throw new Error('DB+name required'); await call('view.refresh',{db,name},'viewOut'); } catch (e) { setOut({error:String(e)},'viewOut'); }
}

async function viewStatus() {
  try { const db = $('viewDb')?.value.trim(), name = $('viewName')?.value.trim(); if (!db||!name) throw new Error('DB+name required'); await call('view.status',{db,name},'viewOut'); } catch (e) { setOut({error:String(e)},'viewOut'); }
}

async function viewDrop() {
  try { const db = $('viewDb')?.value.trim(), name = $('viewName')?.value.trim(); if (!db||!name) throw new Error('DB+name required'); await call('view.drop',{db,name},'viewOut'); } catch (e) { setOut({error:String(e)},'viewOut'); }
}

async function viewExplainDeps() {
  try { const db = $('viewDb')?.value.trim(), name = $('viewName')?.value.trim(); if (!db||!name) throw new Error('DB+name required'); await call('view.explain_deps',{db,name},'viewOut'); } catch (e) { setOut({error:String(e)},'viewOut'); }
}

// ---------------------------------------------------------------------------
// Merge & CRDT (R07)
// ---------------------------------------------------------------------------
async function mergeApply() {
  try {
    const t = readDbTable('mergeDb','mergeTable');
    const pk = parseJsonInput($('mergePk')?.value,'PK') || [];
    const incoming = parseJsonInput($('mergeIncoming')?.value,'Incoming');
    await call('merge.apply', cleanParams({table:t,pk,incoming}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeRegister() {
  try {
    const t = readDbTable('mergeDb','mergeTable');
    const policy = parseJsonInput($('mergePolicy')?.value,'Policy');
    await call('merge.register', cleanParams({table:t,policy}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeSimulate() {
  try {
    const t = readDbTable('mergeDb','mergeTable');
    const pk = parseJsonInput($('mergePk')?.value,'PK') || [];
    const incoming = parseJsonInput($('mergeIncoming')?.value,'Incoming');
    await call('merge.simulate', cleanParams({table:t,pk,incoming}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeWasmRegister() {
  try {
    const name = $('mergeWasmName')?.value.trim(); if (!name) throw new Error('Name required');
    const b64 = $('mergeWasmB64')?.value.trim() || 'AA==';
    await call('merge.wasm.register',{name,wasm_b64:b64},'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeWasmList() { await call('merge.wasm.list',{},'mergeOut'); }

async function mergeWasmDrop() {
  try { const name = $('mergeWasmName')?.value.trim(); if (!name) throw new Error('Name required'); await call('merge.wasm.drop',{name},'mergeOut'); } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

// ---------------------------------------------------------------------------
// Wasm Operators (R19)
// ---------------------------------------------------------------------------
async function wasmCompile() {
  try {
    const b64 = $('wasmB64')?.value.trim() || 'AA==';
    const schema = parseJsonInput($('wasmSchema')?.value,'Schema');
    await call('wasm.plan.compile', cleanParams({wasm_b64:b64,schema:schema?{columns:schema}:undefined}), 'wasmOut');
  } catch (e) { setOut({error:String(e)},'wasmOut'); }
}

async function wasmRun() {
  try {
    const b64 = $('wasmB64')?.value.trim() || 'AA==';
    const schema = parseJsonInput($('wasmSchema')?.value,'Schema');
    const input = parseJsonInput($('wasmInput')?.value,'Input');
    await call('wasm.plan.run', cleanParams({wasm_b64:b64,schema:schema?{columns:schema}:undefined,input:input||undefined}), 'wasmOut');
  } catch (e) { setOut({error:String(e)},'wasmOut'); }
}

// ---------------------------------------------------------------------------
// Index Advisor (R16)
// ---------------------------------------------------------------------------
async function advSynthesize() {
  try { const db = $('advDb')?.value.trim(), table = $('advTable')?.value.trim(); if (!db) throw new Error('DB required'); await call('advisor.synthesize', cleanParams({db,table:table||undefined}), 'advOut'); } catch (e) { setOut({error:String(e)},'advOut'); }
}

async function advHistory() {
  try { const db = $('advDb')?.value.trim(); if (!db) throw new Error('DB required'); await call('advisor.history',{db},'advOut'); } catch (e) { setOut({error:String(e)},'advOut'); }
}

async function advApply() {
  try { const name = $('advIndexName')?.value.trim(); if (!name) throw new Error('Index name required'); const db = $('advDb')?.value.trim(); await call('advisor.apply', cleanParams({db,index_name:name}), 'advOut'); } catch (e) { setOut({error:String(e)},'advOut'); }
}

async function advDismiss() {
  try { const name = $('advIndexName')?.value.trim(); if (!name) throw new Error('Index name required'); const db = $('advDb')?.value.trim(); await call('advisor.dismiss', cleanParams({db,index_name:name}), 'advOut'); } catch (e) { setOut({error:String(e)},'advOut'); }
}

// ---------------------------------------------------------------------------
// NL Lab (R11-R12)
// ---------------------------------------------------------------------------
async function nlTranslate() {
  const db = $('nlDb').value.trim(), request = $('nlRequest').value.trim();
  if (!db || !request) { setOut({error:'db+request required'},'nlOut'); return; }
  const tablesRaw = $('nlTables').value.trim();
  const tables = tablesRaw ? tablesRaw.split(',').map(s=>s.trim()).filter(Boolean) : [];
  const res = await call('ai.nl.translate', cleanParams({db, request, tables: tables.length?tables:undefined, include_schema:$('nlIncludeSchema').checked, read_only:$('nlReadOnly').checked, max_tables:parseInt($('nlMaxTables').value,10)||undefined}), 'nlOut');
  if (res?.json?.ok && res.json.result?.query) $('nlQuery').value = JSON.stringify(res.json.result.query, null, 2);
}

async function nlExplain() {
  try {
    const query = parseJsonInput($('nlQuery').value,'Query'); if (!query) throw new Error('Query required');
    const args = parseJsonInput($('nlArgs').value,'Args');
    const limit = parseInt($('nlPreviewLimit').value,10);
    const res = await call('ai.nl.explain', cleanParams({query,args:args||undefined,preview_limit:Number.isNaN(limit)?undefined:limit,preview_format:$('nlFormat').value}), 'nlOut');
    if (res?.json?.ok && res.json.result) $('nlApproval').value = res.json.result.approval_token || '';
  } catch (e) { setOut({error:String(e)},'nlOut'); }
}

async function nlExecute() {
  try {
    const query = parseJsonInput($('nlQuery').value,'Query'); if (!query) throw new Error('Query required');
    const args = parseJsonInput($('nlArgs').value,'Args');
    const token = $('nlApproval').value.trim(); if (!token) throw new Error('Approval token required');
    await call('ai.nl.execute', cleanParams({query,args:args||undefined,approval_token:token,result_format:$('nlFormat').value}), 'nlOut');
  } catch (e) { setOut({error:String(e)},'nlOut'); }
}

async function autoparamAnalyze() {
  try { const sql = $('autoparamSql')?.value.trim(); if (!sql) throw new Error('SQL required'); await call('autoparam.analyze',{sql},'autoparamOut'); } catch (e) { setOut({error:String(e)},'autoparamOut'); }
}

async function autoparamClassify() {
  try { const sql = $('autoparamSql')?.value.trim(); if (!sql) throw new Error('SQL required'); await call('autoparam.classify',{sql},'autoparamOut'); } catch (e) { setOut({error:String(e)},'autoparamOut'); }
}

// ---------------------------------------------------------------------------
// Migration (R17)
// ---------------------------------------------------------------------------
let lastMigrationRewrites = [], lastMigrationGeneratedAt = null;

function formatConfidenceValue(v) { return typeof v === 'number' && !Number.isNaN(v) ? Math.round(v*100)+'%' : 'n/a'; }

function renderMigrationReport(rewrites) {
  const target = $('migrationReport'); if (!target) return;
  target.textContent = '';
  if (!Array.isArray(rewrites) || !rewrites.length) { target.textContent = 'No rewrites.'; return; }
  rewrites.forEach(item => {
    const card = document.createElement('div'); card.className = 'rewrite-item';
    card.innerHTML = '<div class="rewrite-head"><div class="rewrite-title">' + escapeHtml(item.title || item.intent || 'Rewrite') + '</div><div class="rewrite-meta">' + formatConfidenceValue(item.confidence) + '</div></div>' +
      '<div class="rewrite-tags"><span class="tag">' + escapeHtml(item.intent||'unknown') + '</span><span class="tag secondary">' + formatConfidenceValue(item.confidence) + '</span></div>' +
      '<div class="rewrite-grid"><div class="rewrite-block">' + escapeHtml(item.before||'') + '</div><div class="rewrite-block">' + escapeHtml(item.after||'') + '</div></div>';
    target.appendChild(card);
  });
}

function buildMigrationMarkdown(rewrites, stamp) {
  const o = ['# SkeinDB Migration Report','','Generated: '+(stamp||new Date().toISOString()),''];
  if (!rewrites?.length) { o.push('No rewrites.'); return o.join('\n'); }
  rewrites.forEach((item,i) => {
    o.push('## '+(item.title||item.intent||'Rewrite '+(i+1)),'','- Intent: '+(item.intent||'unknown'),'- Confidence: '+formatConfidenceValue(item.confidence),'');
    o.push('Before:','```sql',item.before||'','```','','After:','```',item.after||'','```','');
  });
  return o.join('\n');
}

function buildMigrationHtml(rewrites, stamp) {
  const s = stamp||new Date().toISOString();
  let h = '<!doctype html><html><head><meta charset="utf-8"><title>Migration Report</title><style>body{font-family:sans-serif;margin:24px}pre{background:#f5f5f5;padding:8px;border-radius:8px}.card{border:1px solid #ddd;border-radius:12px;padding:12px;margin-bottom:12px}</style></head><body>';
  h += '<h1>SkeinDB Migration Report</h1><p>' + escapeHtml(s) + '</p>';
  if (!rewrites?.length) h += '<p>No rewrites.</p>';
  else rewrites.forEach((item,i) => {
    h += '<div class="card"><h2>'+escapeHtml(item.title||item.intent||'Rewrite '+(i+1))+'</h2><p>Intent: '+escapeHtml(item.intent||'')+' | Confidence: '+formatConfidenceValue(item.confidence)+'</p><pre>'+escapeHtml(item.before||'')+'</pre><pre>'+escapeHtml(item.after||'')+'</pre></div>';
  });
  h += '</body></html>'; return h;
}

function migrationParams() {
  const samples = parseJsonInput($('migSamples')?.value||'','Samples');
  const limit = parseInt($('migLimit')?.value,10), windowMs = parseInt($('migWindow')?.value,10);
  return cleanParams({ samples: Array.isArray(samples)?samples:undefined, limit:Number.isNaN(limit)?undefined:limit, window_ms:Number.isNaN(windowMs)?undefined:windowMs });
}

async function migrationPreview() {
  try {
    const res = await call('migration.rewrite_preview', migrationParams(), 'migrationOut');
    if (res?.json?.ok && res.json.result) {
      const rw = res.json.result.rewrites || [];
      lastMigrationRewrites = rw; lastMigrationGeneratedAt = new Date().toISOString();
      renderMigrationReport(rw);
    }
  } catch (e) { setOut({error:String(e)},'migrationOut'); }
}

async function migrationIntent() { try { await call('migration.intent_report', migrationParams(), 'migrationOut'); } catch (e) { setOut({error:String(e)},'migrationOut'); } }

function exportMigrationReport(fmt) {
  if (!lastMigrationRewrites?.length) { setOut({error:'Run preview first'},'migrationOut'); return; }
  const stamp = (lastMigrationGeneratedAt||new Date().toISOString()).replace(/[:.]/g,'-');
  if (fmt === 'json') downloadBlob(JSON.stringify({generated_at:lastMigrationGeneratedAt,rewrites:lastMigrationRewrites},null,2),'migration-'+stamp+'.json','application/json');
  else if (fmt === 'md') downloadBlob(buildMigrationMarkdown(lastMigrationRewrites,lastMigrationGeneratedAt),'migration-'+stamp+'.md','text/markdown');
  else if (fmt === 'html') downloadBlob(buildMigrationHtml(lastMigrationRewrites,lastMigrationGeneratedAt),'migration-'+stamp+'.html','text/html');
}

async function copyMigrationMarkdown() {
  if (!lastMigrationRewrites?.length) { setOut({error:'Run preview first'},'migrationOut'); return; }
  try { await navigator.clipboard.writeText(buildMigrationMarkdown(lastMigrationRewrites,lastMigrationGeneratedAt)); setOut({ok:true,copied:true},'migrationOut'); } catch (e) { setOut({error:String(e)},'migrationOut'); }
}

// ---------------------------------------------------------------------------
// RPC Explorer
// ---------------------------------------------------------------------------
function populateMethodSelect(methods) {
  const s = $('rpcMethod'); if (!s) return; s.textContent = '';
  methods.forEach(m => { const o = document.createElement('option'); o.value = m; o.textContent = m; s.appendChild(o); });
}

function renderMethodList(methods, filter) {
  const t = $('methodList'); if (!t) return; t.textContent = '';
  const match = filter ? filter.toLowerCase() : '';
  const filtered = methods.filter(m => !match || m.toLowerCase().includes(match));
  if (!filtered.length) { t.textContent = 'No methods match.'; return; }
  filtered.forEach(m => {
    const btn = document.createElement('button'); btn.textContent = m;
    btn.addEventListener('click', () => { if ($('rpcMethod')) $('rpcMethod').value = m; setActivePanel('rpc', true); });
    t.appendChild(btn);
  });
}

function populateTemplateSelect() {
  const s = $('rpcTemplate'); if (!s) return; s.textContent = '';
  const e = document.createElement('option'); e.value = ''; e.textContent = 'Choose template'; s.appendChild(e);
  RPC_TEMPLATES.forEach((tpl,i) => { const o = document.createElement('option'); o.value = String(i); o.textContent = tpl.label; s.appendChild(o); });
}

function loadTemplate() {
  const s = $('rpcTemplate'); if (!s) return;
  const i = parseInt(s.value,10); if (Number.isNaN(i) || !RPC_TEMPLATES[i]) return;
  const tpl = RPC_TEMPLATES[i];
  if ($('rpcMethod')) $('rpcMethod').value = tpl.method;
  if ($('rpcParams')) $('rpcParams').value = JSON.stringify(tpl.params, null, 2);
}

async function rpcSend() {
  const method = $('rpcMethod')?.value.trim(); if (!method) { setOut({error:'Select method'},'rpcOut'); return; }
  try { const params = parseJsonInput($('rpcParams').value,'Params') || {}; await call(method, params, 'rpcOut'); } catch (e) { setOut({error:String(e)},'rpcOut'); }
}

// ---------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------
function setActivePanel(panel, updateHash) {
  document.querySelectorAll('.panel').forEach(el => el.classList.toggle('active', el.dataset.panel === panel));
  document.querySelectorAll('.nav-item').forEach(el => el.classList.toggle('active', el.dataset.panel === panel));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.toggle('active', el.dataset.panel === panel));
  updateHeader(panel); updateContext();
  if (updateHash) window.location.hash = panel;
}

function resolveInitialPanel() {
  const h = window.location.hash.replace('#','').trim();
  if (h) return h;
  if (window.location.pathname.includes('/console')) return 'workspace';
  return 'overview';
}

function initNav() {
  document.querySelectorAll('button[data-panel]').forEach(btn => btn.addEventListener('click', () => setActivePanel(btn.dataset.panel, true)));
  setActivePanel(resolveInitialPanel(), false);
  window.addEventListener('hashchange', () => setActivePanel(resolveInitialPanel(), false));
}

function applyMode() {
  const isConsole = window.location.pathname.includes('/console');
  STATE.isConsole = isConsole;
  document.body.dataset.mode = isConsole ? 'console' : 'admin';
  if (isConsole && !window.location.hash) setActivePanel('workspace', false);
  updateHeader(resolveInitialPanel());
}

// ---------------------------------------------------------------------------
// SQL Templates
// ---------------------------------------------------------------------------
function sqlTemplateSelect() {
  if (STATE.selectedDb && STATE.selectedTable) setSqlText('SELECT * FROM '+STATE.selectedDb+'.'+STATE.selectedTable+' LIMIT 50;');
  else if (STATE.selectedDb) setSqlText('SELECT * FROM '+STATE.selectedDb+'.table_name LIMIT 50;');
  else setSqlText('SELECT 1 AS healthcheck;');
}

// ---------------------------------------------------------------------------
// Wire all buttons
// ---------------------------------------------------------------------------
function wire(id, fn) { const el = $(id); if (el) el.addEventListener('click', fn); }

// Connect
wire('btnConnect', connect);
wire('btnDisconnect', disconnect);
wire('btnPing', ping);
wire('btnStats', loadStats);
wire('btnCapabilities', loadCapabilities);

// Profiles
wire('btnSaveProfile', saveProfile);
wire('btnDeleteProfile', deleteProfile);
if ($('profileSelect')) $('profileSelect').addEventListener('change', e => loadProfile(e.target.value));

// SQL
wire('btnSqlExec', () => runSql(false));
wire('btnSqlExplain', () => runSql(true));
wire('btnSqlTplSelect', sqlTemplateSelect);
wire('btnSqlTplShowDb', () => setSqlText('SHOW DATABASES;'));
wire('btnSqlTplShowTables', () => { const db = resolveDefaultDb(); setSqlText('SHOW TABLES FROM '+(db||'demo')+';'); });
wire('btnSqlTplUseDb', () => { const db = resolveDefaultDb(); if (db) setSqlText('USE '+db+';'); });
wire('btnSqlTplInsert', () => setSqlText("INSERT INTO demo.users (id, name) VALUES (1, 'Ada');"));
wire('btnSqlTplCreateDb', () => setSqlText('CREATE DATABASE demo;'));
wire('btnSqlTplCreateTable', () => setSqlText("CREATE TABLE demo.users (id BIGINT PRIMARY KEY, name VARCHAR(255));"));
wire('btnSkeinRun', runSkeinQuery);

// Schema
wire('btnSchemaListDb', schemaListDatabases);
wire('btnSchemaListTables', schemaListTables);
wire('btnSchemaDescribe', schemaDescribe);
wire('btnSchemaCreateDb', schemaCreateDb);
wire('btnSchemaCreateTable', schemaCreateTable);
wire('btnSchemaDropDb', schemaDropDb);
wire('btnSchemaDropTable', schemaDropTable);
wire('btnSchemaPropose', schemaProposeChange);
wire('btnSchemaMergeStatus', schemaMergeStatus);
wire('btnSchemaApplyMerge', schemaApplyMerge);

// Data
wire('btnDataGet', dataGet);
wire('btnDataInsert', dataInsert);
wire('btnDataUpdate', dataUpdate);
wire('btnDataDelete', dataDelete);
wire('btnBrowse', browseTable);
wire('btnBrowsePrev', browsePrev);
wire('btnBrowseNext', browseNext);

// Cluster
wire('btnClusterStatus', clusterReadStatus);
wire('btnClusterNodes', clusterReadNodes);
wire('btnClusterTransport', clusterTransportCapabilities);
wire('btnClusterCreateToken', clusterCreateToken);
wire('btnClusterJoinNode', clusterJoinNode);
wire('btnClusterRemoveNode', clusterRemoveNode);
wire('btnClusterPromote', clusterPromoteNode);
wire('btnClusterShardCreate', clusterShardCreate);
wire('btnClusterShardMove', clusterShardMove);
wire('btnClusterShardRebalance', clusterShardRebalance);

// Settings
wire('btnSettingsGet', settingsGetKey);
wire('btnSettingsSet', settingsSetKey);
wire('btnSettingsClusterPreset', () => { if ($('settingsKey')) $('settingsKey').value = 'cluster.state.v1'; settingsGetKey(); });
wire('btnSettingsListAll', settingsListAll);

// Research settings
wire('btnResearchSettingsLoad', researchSettingsLoad);
wire('btnResearchSettingsSave', researchSettingsSave);

// Users
wire('btnUserCreate', userCreate);
wire('btnUserList', userList);
wire('btnUserDrop', userDrop);
wire('btnUserGrant', userGrant);

// Import/Export
wire('btnExportData', exportData);
wire('btnExportSchema', exportSchema);
wire('btnExportAll', exportAll);
wire('btnImportData', importData);

// Vectors
wire('btnVecSearch', vecSearch);
wire('btnVecInsert', vecInsert);
wire('btnVecIndexStatus', vecIndexStatus);

// Privacy
wire('btnDpAggregate', dpAggregate);
wire('btnDpBudgetGet', dpBudgetGet);
wire('btnDpBudgetSet', dpBudgetSet);
wire('btnDpAudit', dpAudit);
wire('btnOblGet', oblGet);
wire('btnOblSet', oblSet);
wire('btnOblExplain', oblExplain);

// Forensics
wire('btnForVerify', forVerify);
wire('btnForQuery', forQuery);
wire('btnForExport', forExport);

// Views
wire('btnViewCreate', viewCreate);
wire('btnViewRefresh', viewRefresh);
wire('btnViewStatus', viewStatus);
wire('btnViewDrop', viewDrop);
wire('btnViewExplainDeps', viewExplainDeps);

// Merge
wire('btnMergeApply', mergeApply);
wire('btnMergeRegister', mergeRegister);
wire('btnMergeSimulate', mergeSimulate);
wire('btnMergeWasmRegister', mergeWasmRegister);
wire('btnMergeWasmList', mergeWasmList);
wire('btnMergeWasmDrop', mergeWasmDrop);

// Wasm
wire('btnWasmCompile', wasmCompile);
wire('btnWasmRun', wasmRun);

// Advisor
wire('btnAdvSynthesize', advSynthesize);
wire('btnAdvHistory', advHistory);
wire('btnAdvApply', advApply);
wire('btnAdvDismiss', advDismiss);

// NL
wire('btnNlTranslate', nlTranslate);
wire('btnNlExplain', nlExplain);
wire('btnNlExecute', nlExecute);
wire('btnAutoparamAnalyze', autoparamAnalyze);
wire('btnAutoparamClassify', autoparamClassify);

// Migration
wire('btnMigrationPreview', migrationPreview);
wire('btnMigrationIntent', migrationIntent);
wire('btnMigrationDownloadJson', () => exportMigrationReport('json'));
wire('btnMigrationDownloadMd', () => exportMigrationReport('md'));
wire('btnMigrationDownloadHtml', () => exportMigrationReport('html'));
wire('btnMigrationCopyMd', copyMigrationMarkdown);

// RPC
wire('btnRpcSend', rpcSend);
wire('btnRpcLoadTemplate', loadTemplate);
if ($('methodSearch')) $('methodSearch').addEventListener('input', e => renderMethodList(STATE.methods, e.target.value));

// DB tree
wire('btnReloadTree', loadDbTree);
if ($('dbSearch')) $('dbSearch').addEventListener('input', e => renderDbTree(STATE.dbTree, e.target.value));

// Inputs persistence
if ($('baseUrl')) $('baseUrl').addEventListener('change', () => { persistInputs(); setConnStatus('warn','Disconnected','Server changed.'); });
if ($('token')) $('token').addEventListener('change', () => { persistInputs(); setConnStatus('warn','Disconnected','Token changed.'); });

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
loadInputs();
populateTemplateSelect();
initNav();
applyMode();
refreshProfiles();
renderResearchDashboard();
renderFeatureCenterGrid();
renderResearchSettings();
setConnStatus('warn', 'Disconnected', 'Connect to start.');

// Auto-connect if same origin
setTimeout(() => { if (getBaseUrl() === window.location.origin) connect(); }, 200);
