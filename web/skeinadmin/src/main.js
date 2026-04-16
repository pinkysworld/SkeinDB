/* SkeinAdmin – main.js
 * Full SkeinDB admin surface with all 20 research features (R01-R20).
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
  browseOffset: 0,
  sqlHistory: [],
  schemaBuilderRows: [],
  dataFormColumns: [],
  easyTableBuilderRows: [],
  easyRowColumns: [],
  easyBrowseColumns: [],
  easyBrowseRows: [],
  easyBrowseOffset: 0,
  easyBrowseFilter: '',
  easySelectedRowIndex: -1,
  easySelectedRowPk: [],
  easySelectedRowObject: null,
  easyVisualMode: 'insert',
  easyGridMode: false,
  easyGridEditIndex: -1,
  easyGridEditDraft: {},
  easyGridInsertActive: false,
  easyGridInsertDraft: {},
  easyGridCheckedRows: {},
  easySubTab: 'browse',
  easySortColumn: '',
  easySortDir: 'asc',
  qbConditions: [],
  advisorSuggestions: [],
  advisorHistory: [],
  advisorSelection: null,
  cdcSubscriptions: [],
  cdcSelectedSubId: ''
};

// ---------------------------------------------------------------------------
// Panel metadata
// ---------------------------------------------------------------------------
const PANEL_META = {
  overview:   { title: 'Admin Overview',      subtitle: 'Single-binary admin console with all 20 research features.' },
  easy:       { title: 'Easy Viewer',         subtitle: 'Click-first controls with inline grid editing and guided forms.' },
  workspace:  { title: 'SQL Workspace',        subtitle: 'Run SQL for compatibility or SkeinQL for full control.' },
  schema:     { title: 'Structure Manager',    subtitle: 'Create databases, design tables, and review schema.' },
  data:       { title: 'Browse & Edit',        subtitle: 'Browse rows, insert data, and run table edits.' },
  cluster:    { title: 'Cluster Manager',      subtitle: 'Plan topology, inspect transport, and manage layouts.' },
  settings:   { title: 'Settings Manager',     subtitle: 'Read and update server settings and feature config.' },
  telemetry:  { title: 'Telemetry Center',     subtitle: 'Inspect compatibility, feature usage, plan cache, and query pressure.' },
  cdc:        { title: 'CDC Manager',          subtitle: 'Subscribe to table changefeeds, poll events, ACK offsets, and inspect lag.' },
  security:   { title: 'Security Center',      subtitle: 'Manage tokens, review grants, and control sensitive operations.' },
  engine:     { title: 'Engine Config',        subtitle: 'Toggle storage, MVCC, compaction, cache, and security features.' },
  users:      { title: 'Users & Grants',       subtitle: 'Create users, assign roles, grant database privileges.' },
  import:     { title: 'Import / Export',      subtitle: 'Bulk import data or export schemas and rows.' },
  research:   { title: 'Research Agenda',      subtitle: 'Dashboard for all 20 research tracks R01–R20.' },
  vectors:    { title: 'Vector Search (R10)',  subtitle: 'kNN search, vector insert, index status.' },
  privacy:    { title: 'Privacy & DP (R04-R05)', subtitle: 'Differential privacy aggregates and oblivious execution.' },
  forensics:  { title: 'Forensic Audit (R06)', subtitle: 'Audit chain health, verification, and forensic queries.' },
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
  { id: 'R01', title: 'Learned Index Structures', desc: 'CDF-based learned indexes for ValueID lookup.', methods: ['system.capabilities'], status: 'prototype' },
  { id: 'R02', title: 'Adaptive Row-Column Hybrid', desc: 'Dynamic row/column execution selection.', methods: ['system.capabilities'], status: 'hardened' },
  { id: 'R03', title: 'Delta-Chain Topology', desc: 'Linear, tree, skip-list delta chains for versioned values.', methods: ['settings.get'], status: 'hardened' },
  { id: 'R04', title: 'Differential Privacy', desc: 'DP aggregates with calibrated Laplace noise.', methods: ['dp.aggregate', 'dp.budget.get', 'dp.budget.set', 'dp.audit.log'], panel: 'privacy', status: 'hardened' },
  { id: 'R05', title: 'Oblivious Execution', desc: 'Padding and dummy-row injection to hide access patterns.', methods: ['oblivious.policy.get', 'oblivious.policy.set', 'oblivious.explain'], panel: 'privacy', status: 'hardened' },
  { id: 'R06', title: 'Forensic Audit', desc: 'Hash-chained WAL with integrity verification.', methods: ['maintenance.audit_status', 'maintenance.audit_verify', 'forensic.verify', 'forensic.query', 'forensic.export'], panel: 'forensics', status: 'hardened' },
  { id: 'R07', title: 'Merge & CRDT', desc: 'Client-side merge functions: LWW, max-wins, union, Wasm.', methods: ['merge.apply', 'merge.register', 'merge.simulate', 'merge.wasm.register', 'merge.wasm.list', 'merge.wasm.drop'], panel: 'merge', status: 'hardened' },
  { id: 'R08', title: 'Incremental Views', desc: 'Dependency-graph-driven materialized view maintenance.', methods: ['view.create', 'view.refresh', 'view.status', 'view.drop', 'view.explain_deps'], panel: 'views', status: 'hardened' },
  { id: 'R09', title: 'QUIC Transport', desc: 'HTTP/3 and QUIC-native database protocol.', methods: ['transport.capabilities'], status: 'hardened' },
  { id: 'R10', title: 'Vector Embeddings', desc: 'First-class vector columns with kNN search.', methods: ['vector.search', 'vector.insert', 'vector.index.status'], panel: 'vectors', status: 'hardened' },
  { id: 'R11', title: 'Autoparameterization', desc: 'LLM-assisted SQL parameterization.', methods: ['ai.autoparam.analyze', 'ai.autoparam.classify'], panel: 'nl', status: 'hardened' },
  { id: 'R12', title: 'NL-to-SkeinQL', desc: 'Natural language query translation with verification.', methods: ['ai.nl.translate', 'ai.nl.explain', 'ai.nl.execute'], panel: 'nl', status: 'prototype' },
  { id: 'R13', title: 'Causal Consistency', desc: 'ETag-chain causal ordering across replicas.', methods: ['query.patch'], status: 'hardened' },
  { id: 'R14', title: 'Edge Bundles', desc: 'Geo-distributed replay bundles with edge caching.', methods: ['edge.bundle.request', 'edge.bundle.apply', 'edge.bundle.status'], panel: 'rpc', status: 'hardened' },
  { id: 'R15', title: 'Schema Evolution', desc: 'Conflict-free schema evolution with propose/merge/apply.', methods: ['schema.propose_change', 'schema.merge_status', 'schema.apply_merge'], panel: 'schema', status: 'hardened' },
  { id: 'R16', title: 'Index Advisor', desc: 'Workload-driven index synthesis and recommendation.', methods: ['advisor.index_synthesize', 'advisor.history', 'advisor.apply_index', 'advisor.dismiss'], panel: 'advisor', status: 'hardened' },
  { id: 'R17', title: 'Migration Hints', desc: 'Compatibility telemetry and rewrite previews.', methods: ['migration.rewrite_preview', 'migration.intent_report'], panel: 'migration', status: 'prototype' },
  { id: 'R18', title: 'Perf Replay', desc: 'Snapshot + replay for performance regression testing.', methods: ['system.capabilities'], status: 'prototype' },
  { id: 'R19', title: 'Wasm Operators', desc: 'User-defined Wasm query plan operators.', methods: ['wasm.plan.compile', 'wasm.plan.run'], panel: 'wasm', status: 'prototype' },
  { id: 'R20', title: 'Energy-Aware Compaction', desc: 'Carbon-aware scheduling for background compaction.', methods: ['system.capabilities'], status: 'prototype' }
];

// ---------------------------------------------------------------------------
// Feature center items
// ---------------------------------------------------------------------------
const FEATURE_CENTER = [
  { title: 'Easy Viewer', desc: 'Click-first controls for daily operations.', panel: 'easy' },
  { title: 'SQL Compat', desc: 'MySQL-compatible SQL layer with window functions.', panel: 'workspace' },
  { title: 'SkeinQL', desc: 'Native structured query API.', panel: 'workspace' },
  { title: 'Schema Mgmt', desc: 'Create/alter DB and tables.', panel: 'schema' },
  { title: 'Data Browse', desc: 'Guided row browser and editor.', panel: 'data' },
  { title: 'Engine Config', desc: 'Toggle dedup, MVCC, cache, security.', panel: 'engine' },
  { title: 'Cluster', desc: 'Multi-node topology and sharding.', panel: 'cluster' },
  { title: 'CDC', desc: 'Table subscriptions, polling, ACKs, and lag view.', panel: 'cdc' },
  { title: 'Security', desc: 'API tokens, grants, and sensitive controls.', panel: 'security' },
  { title: 'Vectors', desc: 'kNN embedding search.', panel: 'vectors' },
  { title: 'Differential Privacy', desc: 'DP aggregates w/ Laplace noise.', panel: 'privacy' },
  { title: 'Oblivious Exec', desc: 'Access pattern hiding.', panel: 'privacy' },
  { title: 'Forensic Audit', desc: 'Audit chain health plus forensic proof tools.', panel: 'forensics' },
  { title: 'Views', desc: 'Incremental materialized views.', panel: 'views' },
  { title: 'Merge/CRDT', desc: 'Client merge + Wasm merge.', panel: 'merge' },
  { title: 'Wasm Ops', desc: 'Custom query plan operators.', panel: 'wasm' },
  { title: 'Index Advisor', desc: 'Workload-driven index suggestion.', panel: 'advisor' },
  { title: 'Migration', desc: 'Compat rewrites + intent reports.', panel: 'migration' },
  { title: 'NL Lab', desc: 'NL-to-SkeinQL + autoparam.', panel: 'nl' },
  { title: 'QUIC Transport', desc: 'HTTP/3 native transport.', panel: 'cluster' },
  { title: 'Import/Export', desc: 'Bulk data operations.', panel: 'import' },
  { title: 'Window Functions', desc: 'ROW_NUMBER, RANK, DENSE_RANK + OVER.', panel: 'workspace' },
  { title: 'User Variables', desc: 'SET @var / SELECT @var session state.', panel: 'workspace' },
  { title: 'Telemetry', desc: 'Feature flags, compat summary, migration hints.', panel: 'telemetry' },
  { title: 'Plan Cache', desc: 'Query plan cache with fingerprinting.', panel: 'telemetry' },
  { title: 'Query Coalescing', desc: 'Thundering herd protection with metrics.', panel: 'telemetry' }
];

const SETTINGS_PRESET_KEYS = [
  'cluster.state.v1',
  'research.config',
  'engine.storage_mode',
  'engine.cache.enabled',
  'engine.coalescing.enabled',
  'engine.autoparameterize.enabled',
  'engine.cdc.enabled',
  'engine.quic.enabled'
];

// ---------------------------------------------------------------------------
// RPC templates
// ---------------------------------------------------------------------------
const RPC_TEMPLATES = [
  { label: 'system.ping', method: 'system.ping', params: {} },
  { label: 'system.version', method: 'system.version', params: {} },
  { label: 'system.shutdown', method: 'system.shutdown', params: {} },
  { label: 'system.capabilities', method: 'system.capabilities', params: {} },
  { label: 'stats.snapshot', method: 'stats.snapshot', params: {} },
  { label: 'schema.list_databases', method: 'schema.list_databases', params: {} },
  { label: 'schema.list_tables', method: 'schema.list_tables', params: { db: 'demo' } },
  { label: 'schema.create_database', method: 'schema.create_database', params: { db: 'demo' } },
  { label: 'schema.create_table', method: 'schema.create_table', params: { db:'demo', table:'users', columns:[{name:'id',type:{kind:'i64'}},{name:'name',type:{kind:'string'}}], primary_key:['id'], if_not_exists:true } },
  { label: 'schema.describe_table', method: 'schema.describe_table', params: { db:'demo', table:'users' } },
  { label: 'data.insert', method: 'data.insert', params: { into:{db:'demo',table:'users'}, rows:[{id:{t:'i64',v:1},name:{t:'str',v:'Ada'}}] } },
  { label: 'data.get', method: 'data.get', params: { table:{db:'demo',table:'users'}, pk:[{t:'i64',v:1}] } },
  { label: 'data.update', method: 'data.update', params: { table:{db:'demo',table:'users'}, where:{op:'eq',a:{col:'id'},b:{lit:{t:'i64',v:1}}}, set:{name:{t:'str',v:'Ada Lovelace'}}, limit:1 } },
  { label: 'data.delete', method: 'data.delete', params: { table:{db:'demo',table:'users'}, where:{op:'eq',a:{col:'id'},b:{lit:{t:'i64',v:1}}}, limit:1 } },
  { label: 'query.select', method: 'query.select', params: { query:{schema:'demo',table:'users',select:[{col:'id'},{col:'name'}]}, result_format:'rows_json' } },
  { label: 'query.patch', method: 'query.patch', params: { query:{schema:'demo',table:'users',select:[{col:'id'}]}, base_etag:'', include_full:true, result_format:'rows_json' } },
  { label: 'vector.search', method: 'vector.search', params: { table:{db:'demo',table:'items'}, query:{dims:3,v:[0.1,0.2,0.3]}, k:5 } },
  { label: 'dp.aggregate', method: 'dp.aggregate', params: { table:{db:'demo',table:'events'}, aggregate:{op:'count',col:'id'}, epsilon:1.0 } },
  { label: 'oblivious.policy.get', method: 'oblivious.policy.get', params: { db:'demo', table:'events' } },
  { label: 'maintenance.audit_status', method: 'maintenance.audit_status', params: {} },
  { label: 'maintenance.audit_verify', method: 'maintenance.audit_verify', params: {} },
  { label: 'forensic.verify', method: 'forensic.verify', params: { from_id:0, limit:100 } },
  { label: 'forensic.query', method: 'forensic.query', params: { from_id:0, limit:50 } },
  { label: 'view.create', method: 'view.create', params: { db:'demo', name:'active_users', query:{schema:'demo',table:'users',select:[{col:'id'}]} } },
  { label: 'view.status', method: 'view.status', params: { db:'demo', name:'active_users' } },
  { label: 'merge.apply', method: 'merge.apply', params: { table:{db:'demo',table:'users'}, pk:[{t:'i64',v:1}], incoming:{id:{t:'i64',v:1},name:{t:'str',v:'Ada'}} } },
  { label: 'merge.wasm.register', method: 'merge.wasm.register', params: { name:'merge_sum', wasm_b64:'AA==' } },
  { label: 'wasm.plan.compile', method: 'wasm.plan.compile', params: { wasm_b64:'AA==', schema:{columns:[{name:'x',type:'i64'}]} } },
  { label: 'advisor.index_synthesize', method: 'advisor.index_synthesize', params: { table:{db:'demo',table:'users'}, limit:5, min_queries:1, min_rows:1 } },
  { label: 'advisor.apply_index', method: 'advisor.apply_index', params: { table:{db:'demo',table:'users'}, columns:['city'], include:['name'] } },
  { label: 'advisor.history', method: 'advisor.history', params: { table:{db:'demo',table:'users'}, limit:10 } },
  { label: 'ai.autoparam.analyze', method: 'ai.autoparam.analyze', params: { sql:'SELECT * FROM users WHERE id = 42' } },
  { label: 'cdc.subscribe_table', method: 'cdc.subscribe_table', params: { db:'demo', table:'users' } },
  { label: 'cdc.poll', method: 'cdc.poll', params: { sub_id:'sub_1', from_offset:0, limit:200 } },
  { label: 'cdc.ack', method: 'cdc.ack', params: { sub_id:'sub_1', offset:42 } },
  { label: 'cdc.close', method: 'cdc.close', params: { sub_id:'sub_1' } },
  { label: 'settings.get', method: 'settings.get', params: { keys:['cluster.state.v1'] } },
  { label: 'settings.list', method: 'settings.list', params: {} },
  { label: 'cluster.status', method: 'cluster.status', params: {} },
  { label: 'cluster.join_token.create', method: 'cluster.join_token.create', params: { ttl_ms:600000, role:'replica' } },
  { label: 'cluster.node.join', method: 'cluster.node.join', params: { token:'join_token_here', node_id:'replica-a', rpc_url:'http://127.0.0.1:8081', role:'replica' } },
  { label: 'cluster.node.leave', method: 'cluster.node.leave', params: { node_id:'replica-a' } },
  { label: 'cluster.shard.create', method: 'cluster.shard.create', params: { db:'app', table:'users', replicas:['replica-a'] } },
  { label: 'admin.user.revoke', method: 'admin.user.revoke', params: { username:'reader', db:'app', privileges:['SELECT'] } },
  { label: 'ai.nl.translate', method: 'ai.nl.translate', params: { db:'app', request:'list users who signed up this week' } },
  { label: 'migration.rewrite_preview', method: 'migration.rewrite_preview', params: {} },
  { label: 'migration.intent_report', method: 'migration.intent_report', params: {} },
  { label: 'telemetry.feature_flags', method: 'telemetry.feature_flags', params: {} },
  { label: 'telemetry.compat_summary', method: 'telemetry.compat_summary', params: {} },
  { label: 'telemetry.migration_hints', method: 'telemetry.migration_hints', params: { limit: 10 } },
  { label: 'telemetry.workload_features', method: 'telemetry.workload_features', params: {} },
  { label: 'plan_cache.status', method: 'plan_cache.status', params: {} },
  { label: 'plan_cache.clear', method: 'plan_cache.clear', params: {} },
  { label: 'stats.coalescing', method: 'stats.coalescing', params: {} },
  { label: 'transport.capabilities', method: 'transport.capabilities', params: {} }
];

const SCHEMA_TYPE_OPTIONS = ['i64', 'u64', 'f64', 'bool', 'string', 'json', 'datetime', 'date', 'uuid', 'bytes'];
let schemaBuilderNextId = 1;
let easyBuilderNextId = 1;

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function $(id) { return document.getElementById(id); }
function getBaseUrl() { const v = $('baseUrl'); const raw = v ? v.value.trim() : ''; return raw || DEFAULT_BASE_URL; }
function getToken() { const v = $('token'); return v ? v.value.trim() : ''; }
const SIMPLE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

function isSimpleIdentifier(name) {
  return SIMPLE_IDENTIFIER_RE.test((name || '').trim());
}

function validateEasyIdentifier(name, label) {
  const value = (name || '').trim();
  if (!value) throw new Error(label + ' is required');
  if (!isSimpleIdentifier(value)) {
    throw new Error(label + ' must use a simple unquoted identifier (letters, numbers, underscore; no spaces).');
  }
  return value;
}

function analyzeEasyCreateDraft(db, table, rows) {
  const errors = [];
  const warnings = [];
  if (db && !isSimpleIdentifier(db)) errors.push('Database name "' + db + '" needs a simple unquoted identifier for Easy Viewer.');
  if (table && !isSimpleIdentifier(table)) errors.push('Table name "' + table + '" needs a simple unquoted identifier for Easy Viewer.');
  const duplicates = [];
  const seen = new Set();
  const autoIncrement = [];
  rows.forEach((row) => {
    const name = String(row.name || '').trim();
    if (!name) return;
    if (!isSimpleIdentifier(name)) errors.push('Column "' + name + '" needs a simple unquoted identifier.');
    const key = name.toLowerCase();
    if (seen.has(key)) duplicates.push(name);
    else seen.add(key);
    if (row.auto_increment) autoIncrement.push(name);
  });
  if (duplicates.length) errors.push('Duplicate column names: ' + duplicates.join(', '));
  if (autoIncrement.length > 1) errors.push('Only one AUTO_INCREMENT column is supported in Easy Viewer.');
  if (!rows.some((row) => row.primary)) warnings.push('No primary key selected. Inline edit/delete flows work best with one.');
  if (rows.some((row) => row.auto_increment && row.nullable)) warnings.push('AUTO_INCREMENT columns should be NOT NULL; Easy Viewer will still send your current draft.');
  return { errors, warnings };
}

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
    const dot = p.querySelector('.conn-dot');
    if (dot) {
      dot.classList.remove('ok', 'warn', 'error');
      if (kind) dot.classList.add(kind);
      dot.nextSibling && dot.nextSibling.nodeType === 3 ? dot.nextSibling.textContent = message || 'Disconnected' : p.childNodes.forEach(n => { if (n.nodeType === 3) n.textContent = ''; });
    }
    const textNode = Array.from(p.childNodes).find(n => n.nodeType === 3);
    if (textNode) textNode.textContent = message || 'Disconnected';
    else if (!dot) p.textContent = message || 'Disconnected';
  });
  const s = $('connSummary');
  if (s) s.textContent = detail || message || 'Disconnected';
}

function setSelectedDb(db) {
  STATE.selectedDb = (db || '').trim();
  ['schemaDb','dataDb','dataFormDb','dpDb','oblDb','vecDb','viewDb','mergeDb','advDb','importDb','nlDb','clusterShardDb'].forEach(id => {
    const el = $(id); if (el) el.value = STATE.selectedDb;
  });
  const easyDb = $('easyCreateDb');
  if (easyDb && !easyDb.value.trim()) easyDb.value = STATE.selectedDb;
}

function setSelectedTable(table) {
  STATE.selectedTable = (table || '').trim();
  ['schemaTable','dataTable','dataFormTable','dpTable','oblTable','vecTable','mergeTable','advTable','importTable','clusterShardTable'].forEach(id => {
    const el = $(id); if (el) el.value = STATE.selectedTable;
  });
  const easyTable = $('easyCreateTableName');
  if (easyTable && !easyTable.value.trim()) easyTable.value = STATE.selectedTable;
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
    else setConnStatus('warn', 'Connected', 'Connected to ' + baseUrl + ' (last RPC returned an error)');
    return res;
  } catch (e) {
    setOut({ error: String(e), hint: baseUrl !== window.location.origin ? 'Cross-origin? Enable CORS.' : 'Server unreachable.' }, targetId);
    setConnStatus('error', 'Offline', 'Unable to reach ' + baseUrl);
    throw e;
  }
}

function unwrapRpcResult(res, method) {
  if (!res) throw new Error(method + ' failed: no response');
  if (!res.json || !res.json.ok) {
    const err = res.json && res.json.error ? res.json.error : {};
    const code = err.code || 'rpc_error';
    const msg = err.message || ('status ' + (res.status || 'unknown'));
    throw new Error(method + ' failed [' + code + ']: ' + msg);
  }
  return res.json.result;
}

function normalizeSchemaColumnsPayload(columns) {
  if (!Array.isArray(columns)) return [];
  return columns.map((col) => {
    const out = { ...col };
    if (typeof out.type === 'string') out.type = { kind: out.type };
    if (!out.type || typeof out.type !== 'object') out.type = { kind: 'string' };
    if (!out.type.kind && typeof out.kind === 'string') out.type.kind = out.kind;
    if (!out.type.kind) out.type.kind = 'string';
    return out;
  });
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

function canonicalLitTag(tag) {
  if (typeof tag !== 'string') return tag;
  const k = tag.trim().toLowerCase();
  if (k === 'string' || k === 'text' || k === 'varchar') return 'str';
  if (k === 'integer' || k === 'int' || k === 'bigint') return 'i64';
  if (k === 'float' || k === 'double' || k === 'real') return 'f64';
  if (k === 'boolean') return 'bool';
  if (k === 'timestamp') return 'datetime';
  return k;
}

function normalizeLitAliases(value) {
  if (Array.isArray(value)) return value.map(normalizeLitAliases);
  if (!value || typeof value !== 'object') return value;
  const out = {};
  Object.entries(value).forEach(([k, v]) => { out[k] = normalizeLitAliases(v); });
  if (typeof out.t === 'string') out.t = canonicalLitTag(out.t);
  return out;
}

function parseLitJsonInput(raw, label) {
  const parsed = parseJsonInput(raw, label);
  return normalizeLitAliases(parsed);
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
// Toast notifications
// ---------------------------------------------------------------------------
function showToast(message, type = 'info', duration = 3000) {
  const container = $('toastContainer');
  if (!container) return;
  const toast = document.createElement('div');
  toast.className = 'toast-msg ' + type;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => {
    toast.style.animation = 'toastOut .3s ease forwards';
    setTimeout(() => toast.remove(), 300);
  }, duration);
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
function setStat(id, value) { const el = $(id); if (el) el.textContent = value; }

function formatUptime(seconds) {
  if (!Number.isFinite(seconds)) return '--';
  if (seconds < 60) return seconds + 's';
  if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';
  const h = Math.floor(seconds / 3600), m = Math.floor((seconds % 3600) / 60);
  if (seconds < 86400) return h + 'h ' + m + 'm';
  const d = Math.floor(seconds / 86400);
  return d + 'd ' + (h % 24) + 'h';
}

function formatNumber(n) {
  if (!Number.isFinite(n)) return '--';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}

function advisorSelectionKey(columns, include) {
  return JSON.stringify({
    columns: Array.isArray(columns) ? columns : [],
    include: Array.isArray(include) ? include : []
  });
}

function advisorLabel(columns, include) {
  const key = Array.isArray(columns) ? columns.filter(Boolean).join(', ') : '';
  const extras = Array.isArray(include) ? include.filter(Boolean).join(', ') : '';
  return extras ? key + ' INCLUDE ' + extras : key;
}

function advisorExpectedAccess(columns) {
  if (!Array.isArray(columns) || !columns.length) return 'indexed lookup';
  if (columns.length === 1) return 'indexed lookup on ' + columns[0];
  return 'composite index scan on ' + columns.join(' -> ');
}

function formatAdvisorTime(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return '--';
  return new Date(ms).toLocaleString();
}

function findAdvisorHistoryEntry(selection) {
  if (!selection) return null;
  const key = advisorSelectionKey(selection.columns, selection.include);
  return (STATE.advisorHistory || []).find((entry) => advisorSelectionKey(entry.columns, entry.include) === key) || null;
}

function setAdvisorSelection(selection, options = {}) {
  if (!selection || !Array.isArray(selection.columns) || !selection.columns.length) {
    STATE.advisorSelection = null;
    const input = $('advSelection');
    if (input) input.value = '';
    return;
  }
  const override = options.tableRef || (options.table && typeof options.table === 'object' ? options.table : null);
  const db = override?.db || options.db || $('advDb')?.value.trim() || '';
  const table = override?.table || options.tableName || $('advTable')?.value.trim() || '';
  STATE.advisorSelection = {
    table: tableRef(db, table),
    columns: [...selection.columns],
    include: Array.isArray(selection.include) ? [...selection.include] : [],
    id: selection.id || selection.suggestion_id || null,
    action: selection.action || null
  };
  const input = $('advSelection');
  if (input) input.value = advisorLabel(STATE.advisorSelection.columns, STATE.advisorSelection.include);
}

function buildAdvisorReportCard(title, meta, tags, beforeText, afterText, buttonLabel, source, index) {
  const card = document.createElement('div');
  card.className = 'rewrite-item';
  const tagHtml = tags.map((tag, idx) => '<span class="tag' + (idx % 2 ? ' secondary' : '') + '">' + escapeHtml(tag) + '</span>').join('');
  card.innerHTML =
    '<div class="rewrite-head"><div class="rewrite-title">' + escapeHtml(title) + '</div><div class="rewrite-meta">' + escapeHtml(meta) + '</div></div>' +
    '<div class="rewrite-tags">' + tagHtml + '</div>' +
    '<div class="rewrite-grid"><div class="rewrite-block">' + escapeHtml(beforeText) + '</div><div class="rewrite-block">' + escapeHtml(afterText) + '</div></div>' +
    '<div class="actions" style="margin-top:8px"><button class="secondary sm" type="button" data-adv-source="' + source + '" data-adv-index="' + index + '">' + escapeHtml(buttonLabel) + '</button></div>';
  return card;
}

function renderAdvisorReport() {
  const target = $('advisorReport');
  if (!target) return;
  target.textContent = '';

  const suggestions = Array.isArray(STATE.advisorSuggestions) ? STATE.advisorSuggestions : [];
  const history = Array.isArray(STATE.advisorHistory) ? STATE.advisorHistory : [];

  if (!suggestions.length && !history.length) {
    target.textContent = 'Run Synthesize to see ranked index suggestions and an observed-before/expected-after report.';
    return;
  }

  if (suggestions.length) {
    const header = document.createElement('div');
    header.className = 'builder-muted';
    header.textContent = 'Top suggestions';
    target.appendChild(header);
    suggestions.forEach((item, idx) => {
      const avgRows = item.count ? Math.round(item.rows_scanned / item.count) : 0;
      const historyEntry = findAdvisorHistoryEntry(item);
      const afterBits = [
        'After (expected)',
        advisorExpectedAccess(item.columns),
        'scan opportunity: up to ' + formatNumber(item.rows_scanned) + ' historical rows avoided',
        historyEntry ? ('latest action: ' + historyEntry.action + ' at ' + formatAdvisorTime(historyEntry.created_at_ms)) : 'latest action: not yet applied'
      ];
      target.appendChild(buildAdvisorReportCard(
        advisorLabel(item.columns, item.include),
        'score ' + formatNumber(item.score),
        [
          formatNumber(item.count) + ' query observations',
          formatNumber(item.rows_scanned) + ' rows scanned',
          formatNumber(avgRows) + ' rows/query'
        ],
        'Before\n' +
          'observed workload rows scanned: ' + formatNumber(item.rows_scanned) + '\n' +
          'observed queries matched: ' + formatNumber(item.count) + '\n' +
          'average scan pressure: ' + formatNumber(avgRows) + ' rows/query',
        afterBits.join('\n'),
        'Select suggestion',
        'suggestion',
        idx
      ));
    });
  }

  if (history.length) {
    const header = document.createElement('div');
    header.className = 'builder-muted';
    header.style.marginTop = suggestions.length ? '10px' : '0';
    header.textContent = 'Recent advisor history';
    target.appendChild(header);
    history.forEach((entry, idx) => {
      target.appendChild(buildAdvisorReportCard(
        advisorLabel(entry.columns, entry.include),
        entry.action + ' at ' + formatAdvisorTime(entry.created_at_ms),
        [
          entry.table && entry.table.db ? (entry.table.db + '.' + entry.table.table) : 'unknown table',
          entry.note || 'no note'
        ],
        'Before\n' +
          'suggestion id: ' + (entry.suggestion_id || 'n/a') + '\n' +
          'action id: ' + (entry.id || 'n/a'),
        'After\n' +
          'recorded action: ' + entry.action + '\n' +
          'selection: ' + advisorLabel(entry.columns, entry.include),
        'Load selection',
        'history',
        idx
      ));
    });
  }
}

function updateStats(s) {
  if (!s) return;
  // Runtime
  setStat('statUptime', formatUptime(s.uptime_s));
  setStat('statCpu', s.process && Number.isFinite(s.process.cpu_pct) ? s.process.cpu_pct.toFixed(1) + '%' : '--');
  setStat('statRss', s.process ? formatBytes(s.process.rss_bytes) : '--');
  setStat('statQps', (s.qps !== undefined ? s.qps : '--') + ' / ' + (s.tps !== undefined ? s.tps : '--'));
  setStat('statOpenTxns', s.open_txns !== undefined ? String(s.open_txns) : '--');
  setStat('statConnections', s.connections !== undefined ? String(s.connections) : '--');

  // Storage & Dedup
  const storage = s.storage || {};
  const dedupRatio = storage.dedup_ratio;
  setStat('statDedupRatio', Number.isFinite(dedupRatio) ? dedupRatio.toFixed(2) + 'x' : '--');
  setStat('statDedupEnabled', storage.dedup_enabled !== undefined ? (storage.dedup_enabled ? '✅ ON' : '❌ OFF') : '--');
  setStat('statDedupSaved', formatBytes(storage.duplicate_bytes));
  setStat('statLogicalBytes', formatBytes(storage.logical_bytes));
  setStat('statUniqueBytes', formatBytes(storage.unique_bytes));
  setStat('statInternedValues', formatNumber(storage.interned_values));
  setStat('statUniqueValues', formatNumber(storage.unique_values));
  // Savings percentage
  const logical = storage.logical_bytes || 0, unique = storage.unique_bytes || 0;
  const pct = logical > 0 ? ((1 - unique / logical) * 100) : 0;
  setStat('statDedupPct', logical > 0 ? pct.toFixed(1) + '%' : '--');
  setStat('statTotalRows', formatNumber(storage.total_rows));
  setStat('statTotalTables', storage.total_tables !== undefined ? String(storage.total_tables) : '--');
  setStat('statDiskSize', formatBytes(storage.disk_bytes));
  setStat('statWalSize', formatBytes(storage.wal_bytes));

  // Dedup bar
  const barUnique = $('dedupBarUnique'), barSaved = $('dedupBarSaved');
  if (barUnique && barSaved && logical > 0) {
    const uPct = Math.max(1, (unique / logical) * 100);
    const sPct = Math.max(0, 100 - uPct);
    barUnique.style.width = uPct.toFixed(1) + '%';
    barSaved.style.width = sPct.toFixed(1) + '%';
    barUnique.title = 'Unique: ' + formatBytes(unique);
    barSaved.title = 'Saved: ' + formatBytes(logical - unique);
  }

  // MVCC & Compaction
  const mvcc = s.mvcc || {};
  setStat('statMvccVersions', formatNumber(mvcc.versions));
  setStat('statDeltaChains', formatNumber(mvcc.delta_chains));
  const compaction = s.compaction || {};
  setStat('statCompactionRuns', compaction.runs !== undefined ? String(compaction.runs) : '--');
  setStat('statCompactionStatus', compaction.status || (compaction.running ? 'Running' : 'Idle'));
  setStat('statL0Files', compaction.l0_files !== undefined ? String(compaction.l0_files) : '--');
  setStat('statStallRate', compaction.stall_rate !== undefined ? compaction.stall_rate.toFixed(2) + '%' : '--');

  // Cache & Query
  const cache = s.cache || {};
  setStat('statCacheHit', Number.isFinite(cache.hit_pct) ? cache.hit_pct.toFixed(1) + '%' : '--');
  setStat('statCacheSize', formatBytes(cache.size_bytes));
  const query = s.query || {};
  setStat('statSlowQueries', query.slow_count !== undefined ? String(query.slow_count) : '--');
  setStat('statAvgLatency', Number.isFinite(query.avg_latency_ms) ? query.avg_latency_ms.toFixed(1) + ' ms' : '--');
  setStat('statEtagHits', formatNumber(query.etag_hits));
  setStat('statCoalesced', formatNumber(query.coalesced));
}

let autoRefreshInterval = null;

async function loadStats() {
  const res = await call('stats.snapshot', {}, 'out');
  if (res && res.json && res.json.ok && res.json.result) { updateStats(res.json.result); setOut(res.json.result, 'out'); }
}

function toggleAutoRefresh() {
  if (autoRefreshInterval) {
    clearInterval(autoRefreshInterval);
    autoRefreshInterval = null;
    const btn = $('btnAutoRefreshStats');
    if (btn) btn.textContent = 'Auto \u23F1';
  } else {
    autoRefreshInterval = setInterval(() => { if (STATE.connected) loadStats(); }, 5000);
    const btn = $('btnAutoRefreshStats');
    if (btn) btn.textContent = 'Stop \u23F9';
  }
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
    renderSettingsCapabilities(caps);
  }
}

async function connect() {
  try {
    await ping(); await loadVersion(); await loadCapabilities(); await loadTransport(); await loadStats();
    await clusterReadStatus(); await loadDbTree(); updateContext();
    if ($('statDatabases')) $('statDatabases').textContent = Object.keys(STATE.dbTree).length;
    showToast('Connected to ' + getBaseUrl(), 'success');
  } catch { showToast('Connection failed', 'error'); }
}

function disconnect() {
  setConnStatus('warn', 'Disconnected', 'Disconnected.');
  showToast('Disconnected', 'info');
  STATE.methods = []; STATE.dbTree = {};
  setSelectedDb(''); setSelectedTable('');
  renderDbTree({}, '');
  dataFormApplyColumns([], []);
  easyApplyColumns([], []);
  easyRefreshTargetsFromTree();
  renderTable('browseTable', [], []); renderTable('easyDataGrid', [], []); renderTable('structureTable', [], []); renderTable('sqlTable', [], []);
  updateContext();
}

async function shutdownServer() {
  const baseUrl = getBaseUrl();
  const ok = await skeinModal(
    '\u26A0\uFE0F',
    'Shutdown Server',
    'Shutdown SkeinDB at ' + baseUrl + '? This closes active sessions and marks this node offline in cluster state.',
    [
      { label: 'Cancel', value: false, cls: 'ghost' },
      { label: 'Shutdown', value: true, cls: 'danger' }
    ]
  );
  if (!ok) return;
  setConnStatus('warn', 'Shutting down', 'Sending shutdown request to ' + baseUrl);
  try {
    const res = await call('system.shutdown', {}, 'out');
    if (res && res.json && res.json.ok) {
      setOut(
        {
          ok: true,
          note: 'Shutdown request accepted. The server process is stopping gracefully.'
        },
        'out'
      );
    }
  } catch (_) {
    // Server may terminate before the response reaches the browser.
    setOut(
      {
        ok: true,
        note: 'Shutdown request sent. Server became unreachable, which can be expected during shutdown.'
      },
      'out'
    );
  }
  setConnStatus('warn', 'Offline', 'Shutdown requested for ' + baseUrl);
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
  easyRefreshTargetsFromTree();
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
    btn.addEventListener('click', async () => {
      setSelectedTable(tbl);
      updateContext();
      await schemaDescribe();
      await dataFormLoadColumns();
    });
    t.appendChild(btn);
  });
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------
function defaultSchemaBuilderRows() {
  return [
    { id: schemaBuilderNextId++, name: 'id', type: 'i64', nullable: false, auto_increment: true, primary: true },
    { id: schemaBuilderNextId++, name: 'name', type: 'string', nullable: false, auto_increment: false, primary: false }
  ];
}

function normalizeSchemaBuilderRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return defaultSchemaBuilderRows();
  return rows
    .map((row) => ({
      id: row.id || schemaBuilderNextId++,
      name: String(row.name || '').trim(),
      type: String(row.type || 'string').trim() || 'string',
      nullable: !!row.nullable,
      auto_increment: !!row.auto_increment,
      primary: !!row.primary
    }))
    .filter((row) => row.name);
}

function schemaBuilderSetRows(rows) {
  STATE.schemaBuilderRows = normalizeSchemaBuilderRows(rows);
  renderSchemaBuilderRows();
}

function renderSchemaBuilderRows() {
  const target = $('schemaBuilderRows');
  if (!target) return;
  target.textContent = '';
  if (!STATE.schemaBuilderRows.length) {
    target.textContent = 'No columns defined yet.';
    return;
  }
  STATE.schemaBuilderRows.forEach((row) => {
    const wrap = document.createElement('div');
    wrap.className = 'builder-row';
    wrap.dataset.rowId = String(row.id);
    wrap.innerHTML =
      '<div class="field"><label>Column name</label><input data-role="name" value="' + escapeHtml(row.name) + '" placeholder="column_name" /></div>' +
      '<div class="field"><label>Type</label><select data-role="type">' +
      SCHEMA_TYPE_OPTIONS.map((opt) => '<option value="' + opt + '"' + (opt === row.type ? ' selected' : '') + '>' + opt + '</option>').join('') +
      '</select></div>' +
      '<label class="builder-check"><input type="checkbox" data-role="nullable"' + (row.nullable ? ' checked' : '') + ' />Nullable</label>' +
      '<label class="builder-check"><input type="checkbox" data-role="auto_increment"' + (row.auto_increment ? ' checked' : '') + ' />Auto Inc</label>' +
      '<label class="builder-check"><input type="checkbox" data-role="primary"' + (row.primary ? ' checked' : '') + ' />Primary Key</label>' +
      '<button class="danger sm" data-role="remove">Remove</button>';
    const remove = wrap.querySelector('[data-role="remove"]');
    if (remove) remove.addEventListener('click', () => {
      STATE.schemaBuilderRows = STATE.schemaBuilderRows.filter((item) => item.id !== row.id);
      renderSchemaBuilderRows();
      schemaBuilderSyncToJson(false);
    });
    target.appendChild(wrap);
  });
}

function schemaBuilderCollectRows() {
  const target = $('schemaBuilderRows');
  if (!target) return [];
  const rows = [];
  target.querySelectorAll('.builder-row').forEach((rowEl) => {
    const name = rowEl.querySelector('[data-role="name"]')?.value.trim() || '';
    if (!name) return;
    rows.push({
      id: Number(rowEl.dataset.rowId || 0) || schemaBuilderNextId++,
      name,
      type: rowEl.querySelector('[data-role="type"]')?.value || 'string',
      nullable: !!rowEl.querySelector('[data-role="nullable"]')?.checked,
      auto_increment: !!rowEl.querySelector('[data-role="auto_increment"]')?.checked,
      primary: !!rowEl.querySelector('[data-role="primary"]')?.checked
    });
  });
  return rows;
}

function schemaBuilderAddColumn(seed) {
  const normalized = schemaBuilderCollectRows();
  if (normalized.length) STATE.schemaBuilderRows = normalized;
  const payload = seed || { id: schemaBuilderNextId++, name: '', type: 'string', nullable: true, auto_increment: false, primary: false };
  STATE.schemaBuilderRows.push(payload);
  renderSchemaBuilderRows();
}

function schemaBuilderSyncToJson(showMessage = true) {
  const rows = schemaBuilderCollectRows();
  if (!rows.length) {
    if (showMessage) setOut({ error: 'Define at least one column.' }, 'schemaBuilderOut');
    return null;
  }
  const columns = rows.map((row) => cleanParams({
    name: row.name,
    type: { kind: row.type },
    nullable: row.nullable,
    auto_increment: row.auto_increment
  }));
  const primaryKey = rows.filter((row) => row.primary).map((row) => row.name);
  STATE.schemaBuilderRows = rows;
  if ($('schemaColumns')) $('schemaColumns').value = JSON.stringify(columns, null, 2);
  if ($('schemaPk')) $('schemaPk').value = primaryKey.join(',');
  if (showMessage) setOut({ ok: true, columns: columns.length, primary_key: primaryKey }, 'schemaBuilderOut');
  return { columns, primaryKey };
}

function schemaBuilderSeedDefaults() {
  schemaBuilderSetRows(defaultSchemaBuilderRows());
  schemaBuilderSyncToJson(false);
  setOut({ ok: true, message: 'Starter columns loaded.' }, 'schemaBuilderOut');
}

async function schemaBuilderLoadCurrent() {
  try {
    const db = $('schemaDb')?.value.trim();
    const table = $('schemaTable')?.value.trim();
    if (!db || !table) throw new Error('Database and table are required');
    const res = await call('schema.describe_table', { db, table }, 'schemaBuilderOut');
    if (!res?.json?.ok || !res.json.result) return;
    const result = res.json.result;
    const pk = new Set(Array.isArray(result.primary_key) ? result.primary_key : []);
    const rows = (result.columns || []).map((col) => ({
      id: schemaBuilderNextId++,
      name: col.name,
      type: col.type?.kind || 'string',
      nullable: !!col.nullable,
      auto_increment: !!col.auto_increment,
      primary: pk.has(col.name)
    }));
    schemaBuilderSetRows(rows);
    schemaBuilderSyncToJson(false);
    setOut({ ok: true, message: 'Loaded existing structure.', columns: rows.length }, 'schemaBuilderOut');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaBuilderOut');
  }
}

async function schemaBuilderCreateDb() {
  try {
    const db = $('schemaDb')?.value.trim();
    if (!db) throw new Error('Database is required');
    const res = await schemaCreateDb();
    unwrapRpcResult(res, 'schema.create_database');
    setOut({ ok: true, message: 'Database ensured: ' + db }, 'schemaBuilderOut');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaBuilderOut');
  }
}

async function schemaBuilderCreateTable() {
  const packed = schemaBuilderSyncToJson(false);
  if (!packed) return;
  try {
    const db = $('schemaDb')?.value.trim();
    const table = $('schemaTable')?.value.trim();
    if (!db || !table) throw new Error('Database and table are required');
    const ine = $('schemaIfNotExists')?.value === 'true';
    const res = await call(
      'schema.create_table',
      { db, table, columns: packed.columns, primary_key: packed.primaryKey, if_not_exists: ine },
      'schemaBuilderOut'
    );
    unwrapRpcResult(res, 'schema.create_table');
    await loadDbTree();
    setOut({ ok: true, message: 'Table created.', columns: packed.columns.length }, 'schemaBuilderOut');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaBuilderOut');
  }
}

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
    if (res && res.json && res.json.ok && res.json.result) {
      const result = res.json.result;
      setSelectedDb(db);
      setSelectedTable(table);
      renderStructure(result);
      setOut(result, 'structureOut');
      const pk = new Set(Array.isArray(result.primary_key) ? result.primary_key : []);
      schemaBuilderSetRows((result.columns || []).map((col) => ({
        id: schemaBuilderNextId++,
        name: col.name,
        type: col.type?.kind || 'string',
        nullable: !!col.nullable,
        auto_increment: !!col.auto_increment,
        primary: pk.has(col.name)
      })));
      schemaBuilderSyncToJson(false);
      dataFormApplyColumns(result.columns || [], result.primary_key || []);
      updateContext();
    }
  } catch (e) { setOut({ error: String(e) }, 'schemaOut'); }
}

async function schemaCreateDb() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : ''; if (!db) return;
  const res = await call('schema.create_database', { db }, 'schemaOut');
  if (res?.json?.ok) await loadDbTree();
  return res;
}

async function schemaCreateTable() {
  try {
    const db = $('schemaDb').value.trim(), table = $('schemaTable').value.trim(); if (!db || !table) throw new Error('DB+table required');
    const rawColumns = parseJsonInput($('schemaColumns').value, 'Columns'); if (!Array.isArray(rawColumns)) throw new Error('Columns must be array');
    const columns = normalizeSchemaColumnsPayload(rawColumns);
    const pk = ($('schemaPk').value.trim() || '').split(',').map(c => c.trim()).filter(Boolean);
    const ine = $('schemaIfNotExists').value === 'true';
    const res = await call('schema.create_table', { db, table, columns, primary_key: pk, if_not_exists: ine }, 'schemaOut');
    unwrapRpcResult(res, 'schema.create_table');
    await loadDbTree();
    return res;
  } catch (e) { setOut({ error: String(e) }, 'schemaOut'); return null; }
}

async function schemaDropDb() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : ''; if (!db) return;
  const ok = await skeinModal('⚠️', 'Drop Database', 'Drop database "' + db + '"? This cannot be undone.', [{ label: 'Cancel', value: false }, { label: 'Drop', value: true, cls: 'primary' }]);
  if (!ok) return;
  await call('schema.drop_database', { db }, 'schemaOut');
  await loadDbTree();
}

async function schemaDropTable() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : '', table = $('schemaTable') ? $('schemaTable').value.trim() : '';
  if (!db || !table) return;
  const ok = await skeinModal('⚠️', 'Drop Table', 'Drop table "' + db + '.' + table + '"?', [{ label: 'Cancel', value: false }, { label: 'Drop', value: true, cls: 'primary' }]);
  if (!ok) return;
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
function normalizeTypeKind(kind) {
  return String(kind || 'string').trim().toLowerCase();
}

function dataTypePlaceholder(kind) {
  const k = normalizeTypeKind(kind);
  if (k.includes('bool')) return 'true / false';
  if (k.includes('json')) return '{"field":"value"}';
  if (k.includes('date') || k.includes('time')) return 'ISO-8601 value';
  if (k.includes('bytes') || k.includes('blob')) return 'base64 bytes';
  if (k.includes('int') || k.includes('u64') || k.includes('i64')) return '123';
  if (k.includes('float') || k.includes('double') || k.includes('dec') || k.includes('f64')) return '123.45';
  return 'Text value';
}

function dataTypeHint(kind) {
  const k = normalizeTypeKind(kind);
  if (k.includes('bool')) return 'Stored as bool literal.';
  if (k.includes('json')) return 'Stored as JSON literal.';
  if (k.includes('date')) return 'Use ISO date/datetime.';
  if (k.includes('bytes') || k.includes('blob')) return 'Paste base64 payload.';
  if (k.includes('int') || k.includes('u64') || k.includes('i64')) return 'Stored as i64.';
  if (k.includes('float') || k.includes('double') || k.includes('dec') || k.includes('f64')) return 'Stored as f64.';
  return 'Stored as string literal.';
}

function literalFromInput(kind, raw, forceNull) {
  if (forceNull) return { t: 'null' };
  const value = String(raw || '').trim();
  if (!value.length) return { t: 'null' };
  const k = normalizeTypeKind(kind);
  if (k.includes('bool')) {
    const boolValue = /^(true|1|yes|on)$/i.test(value) ? true : /^(false|0|no|off)$/i.test(value) ? false : null;
    if (boolValue === null) throw new Error('Invalid boolean: ' + value);
    return { t: 'bool', v: boolValue };
  }
  if (k.includes('json')) return { t: 'json', v: JSON.parse(value) };
  if (k.includes('date') || k.includes('time')) {
    if (k.includes('date') && k.includes('time')) return { t: 'datetime', iso: value };
    if (k.includes('time')) return { t: 'time', iso: value };
    return { t: 'date', iso: value };
  }
  if (k.includes('bytes') || k.includes('blob')) return { t: 'bytes', b64: value };
  if (k.includes('int') || k.includes('u64') || k.includes('i64')) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || !Number.isInteger(parsed)) throw new Error('Invalid integer: ' + value);
    return { t: 'i64', v: parsed };
  }
  if (k.includes('float') || k.includes('double') || k.includes('dec') || k.includes('f64')) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) throw new Error('Invalid number: ' + value);
    return { t: 'f64', v: parsed };
  }
  return { t: 'str', v: value };
}

function dataFormApplyColumns(columns, primaryKey) {
  const target = $('dataFormFields');
  if (!target) return;
  const pk = new Set(Array.isArray(primaryKey) ? primaryKey : []);
  STATE.dataFormColumns = (columns || []).map((col) => ({
    name: col.name,
    kind: col.type?.kind || 'string',
    nullable: !!col.nullable,
    primary: pk.has(col.name)
  }));
  target.textContent = '';
  if (!STATE.dataFormColumns.length) {
    target.textContent = 'No columns returned for this table.';
    return;
  }
  STATE.dataFormColumns.forEach((col) => {
    const row = document.createElement('div');
    row.className = 'builder-row compact';
    row.dataset.colName = col.name;
    row.dataset.colKind = col.kind;
    row.dataset.colPrimary = col.primary ? 'true' : 'false';
    row.innerHTML =
      '<div class="field"><label>' + escapeHtml(col.name) + '</label><input data-role="value" placeholder="' + escapeHtml(dataTypePlaceholder(col.kind)) + '" /></div>' +
      '<div class="field"><label>Type</label><div class="builder-muted">' + escapeHtml(col.kind + (col.primary ? ' (PK)' : '')) + '</div><div class="builder-muted">' + escapeHtml(dataTypeHint(col.kind)) + '</div></div>' +
      '<label class="builder-check"><input type="checkbox" data-role="null" ' + (col.nullable ? '' : 'disabled') + ' />Set NULL</label>' +
      '<button class="sm ghost" data-role="clear">Clear</button>';
    const clear = row.querySelector('[data-role="clear"]');
    if (clear) {
      clear.addEventListener('click', () => {
        const input = row.querySelector('[data-role="value"]');
        const nullToggle = row.querySelector('[data-role="null"]');
        if (input) input.value = '';
        if (nullToggle && !nullToggle.disabled) nullToggle.checked = false;
      });
    }
    target.appendChild(row);
  });
}

function dataFormCollect(includeAllColumns) {
  const target = $('dataFormFields');
  if (!target) throw new Error('Load table columns first');
  const rows = {};
  const pk = [];
  const pkMissing = [];
  target.querySelectorAll('.builder-row').forEach((row) => {
    const name = row.dataset.colName || '';
    if (!name) return;
    const kind = row.dataset.colKind || 'string';
    const primary = row.dataset.colPrimary === 'true';
    const input = row.querySelector('[data-role="value"]');
    const nullToggle = row.querySelector('[data-role="null"]');
    const raw = input ? input.value : '';
    const forceNull = !!nullToggle?.checked;
    const hasValue = forceNull || String(raw || '').trim().length > 0;
    if (!hasValue && !includeAllColumns && !primary) return;
    const lit = literalFromInput(kind, raw, forceNull);
    if (includeAllColumns || hasValue || primary) rows[name] = lit;
    if (primary) {
      if (!hasValue || lit.t === 'null') pkMissing.push(name);
      pk.push(lit);
    }
  });
  return { row: rows, pk, pkMissing };
}

async function dataFormLoadColumns() {
  try {
    const db = $('dataFormDb')?.value.trim() || $('dataDb')?.value.trim();
    const table = $('dataFormTable')?.value.trim() || $('dataTable')?.value.trim();
    if (!db || !table) throw new Error('Database and table are required');
    setSelectedDb(db);
    setSelectedTable(table);
    const res = await call('schema.describe_table', { db, table }, 'dataGuideOut');
    if (!res?.json?.ok || !res.json.result) return;
    const result = res.json.result;
    dataFormApplyColumns(result.columns || [], result.primary_key || []);
    setOut({ ok: true, columns: (result.columns || []).length, primary_key: result.primary_key || [] }, 'dataGuideOut');
    if ($('dataDb')) $('dataDb').value = db;
    if ($('dataTable')) $('dataTable').value = table;
  } catch (e) {
    setOut({ error: String(e) }, 'dataGuideOut');
  }
}

async function dataFormInsertRow() {
  try {
    const t = readDbTable('dataFormDb', 'dataFormTable');
    const payload = dataFormCollect(false);
    if (!Object.keys(payload.row).length) throw new Error('Enter at least one value');
    if ($('dataRows')) $('dataRows').value = JSON.stringify([payload.row], null, 2);
    await call('data.insert', { into: t, rows: [payload.row] }, 'dataGuideOut');
    await browseTable();
  } catch (e) {
    setOut({ error: String(e) }, 'dataGuideOut');
  }
}

function whereByPkColumns(columns, pkValues) {
  const names = columns.filter((col) => col.primary).map((col) => col.name);
  if (!names.length || names.length !== pkValues.length) return null;
  if (names.length === 1) {
    return { op: 'eq', a: { col: names[0] }, b: { lit: pkValues[0] } };
  }
  return {
    op: 'and',
    args: names.map((name, idx) => ({ op: 'eq', a: { col: name }, b: { lit: pkValues[idx] } }))
  };
}

async function dataFormGetByPk() {
  try {
    const t = readDbTable('dataFormDb', 'dataFormTable');
    const payload = dataFormCollect(true);
    if (payload.pkMissing.length) throw new Error('Primary-key fields required: ' + payload.pkMissing.join(', '));
    if (!payload.pk.length) throw new Error('This table has no primary key');
    if ($('dataPk')) $('dataPk').value = JSON.stringify(payload.pk, null, 2);
    await call('data.get', { table: t, pk: payload.pk }, 'dataGuideOut');
  } catch (e) {
    setOut({ error: String(e) }, 'dataGuideOut');
  }
}

async function dataFormDeleteByPk() {
  try {
    const t = readDbTable('dataFormDb', 'dataFormTable');
    const payload = dataFormCollect(true);
    if (payload.pkMissing.length) throw new Error('Primary-key fields required: ' + payload.pkMissing.join(', '));
    const where = whereByPkColumns(STATE.dataFormColumns, payload.pk);
    if (!where) throw new Error('Could not build PK filter for this table');
    if ($('dataWhere')) $('dataWhere').value = JSON.stringify(where, null, 2);
    await call('data.delete', { table: t, where, limit: 1 }, 'dataGuideOut');
    await browseTable();
  } catch (e) {
    setOut({ error: String(e) }, 'dataGuideOut');
  }
}

async function dataGet() {
  try {
    const t = readDbTable('dataDb', 'dataTable');
    const pk = parseLitJsonInput($('dataPk').value, 'PK') || [];
    await call('data.get', { table: t, pk }, 'dataOut');
  } catch (e) { setOut({ error: String(e) }, 'dataOut'); }
}

async function dataInsert() {
  try {
    const t = readDbTable('dataDb', 'dataTable');
    const rows = parseLitJsonInput($('dataRows').value, 'Rows') || [];
    await call('data.insert', { into: t, rows }, 'dataOut');
  } catch (e) { setOut({ error: String(e) }, 'dataOut'); }
}

async function dataUpdate() {
  try {
    const t = readDbTable('dataDb', 'dataTable');
    const w = parseLitJsonInput($('dataWhere').value, 'Where');
    const s = parseLitJsonInput($('dataSet').value, 'Set');
    if (!w || !s) throw new Error('Where+Set required');
    const lim = parseInt($('dataLimit').value, 10);
    await call('data.update', cleanParams({ table: t, where: w, set: s, limit: Number.isNaN(lim) ? undefined : lim }), 'dataOut');
  } catch (e) { setOut({ error: String(e) }, 'dataOut'); }
}

async function dataDelete() {
  try {
    const t = readDbTable('dataDb', 'dataTable');
    const w = parseLitJsonInput($('dataWhere').value, 'Where');
    if (!w) throw new Error('Where required');
    const lim = parseInt($('dataLimit').value, 10);
    await call('data.delete', cleanParams({ table: t, where: w, limit: Number.isNaN(lim) ? undefined : lim }), 'dataOut');
  } catch (e) { setOut({ error: String(e) }, 'dataOut'); }
}

async function browseTable() {
  try {
    const t = readDbTable('dataDb', 'dataTable');
    if ($('dataFormDb')) $('dataFormDb').value = t.db;
    if ($('dataFormTable')) $('dataFormTable').value = t.table;
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
// Easy Viewer (familiar table-admin workflow)
// ---------------------------------------------------------------------------

function easySetSubTab(tab) {
  STATE.easySubTab = tab;
  document.querySelectorAll('.easy-tab').forEach(el => el.classList.toggle('active', el.dataset.etab === tab));
  document.querySelectorAll('.easy-tab-pane').forEach(el => el.classList.toggle('active', el.dataset.etab === tab));
  const db = easyGetSelectedDb(), table = easyGetSelectedTable();
  if (tab === 'structure' && db && table) easyLoadStructure();
  if (tab === 'insert' && db && table) easyRenderInsertForm();
  if (tab === 'create') {
    if ($('easyCreateDb') && !$('easyCreateDb').value.trim()) $('easyCreateDb').value = db || '';
  }
  if (tab === 'search') easyPopulateSearchCols();
  if (tab === 'query' && db && table) qbInit();
}

function easyShowToast(msg, type) {
  const el = $('easyToast');
  if (!el) return;
  el.textContent = msg;
  el.className = 'easy-toast ' + (type || 'info');
  clearTimeout(el._t);
  el._t = setTimeout(() => { el.className = 'easy-toast'; }, 5000);
}

// ---------------------------------------------------------------------------
// Modal confirmation dialog (replaces window.confirm)
// ---------------------------------------------------------------------------

function skeinModal(icon, title, body, buttons) {
  return new Promise(resolve => {
    const overlay = $('modalOverlay');
    if (!overlay) { resolve(false); return; }
    const iconEl = $('modalIcon');
    const titleEl = $('modalTitle');
    const bodyEl = $('modalBody');
    const actionsEl = $('modalActions');
    if (iconEl) iconEl.textContent = icon || '\u26A0\uFE0F';
    if (titleEl) titleEl.textContent = title || 'Confirm';
    if (bodyEl) bodyEl.textContent = body || '';
    if (actionsEl) {
      actionsEl.textContent = '';
      (buttons || [{ label: 'Cancel', value: false }, { label: 'OK', value: true, cls: 'primary' }]).forEach(btn => {
        const b = document.createElement('button');
        b.textContent = btn.label;
        b.className = btn.cls || 'ghost';
        b.addEventListener('click', () => { overlay.classList.remove('active'); resolve(btn.value); });
        actionsEl.appendChild(b);
      });
    }
    overlay.classList.add('active');
    overlay.addEventListener('click', e => { if (e.target === overlay) { overlay.classList.remove('active'); resolve(false); } }, { once: true });
  });
}

function easyUpdateBreadcrumb() {
  const s = $('easyBcServer'); if (s) s.textContent = getBaseUrl().replace(/^https?:\/\//, '') || 'Server';
  const d = $('easyBcDb'); if (d) d.textContent = easyGetSelectedDb() || '(no database)';
  const t = $('easyBcTable'); if (t) t.textContent = easyGetSelectedTable() || '';
  const sep = $('easyBcSep2');
  if (sep) sep.style.display = easyGetSelectedTable() ? '' : 'none';
  if (t) t.style.display = easyGetSelectedTable() ? '' : 'none';
}

function easyRenderTree() {
  const target = $('easyTree');
  if (!target) return;
  target.textContent = '';
  const tree = STATE.dbTree || {};
  const filter = ($('easyTreeFilter')?.value || '').toLowerCase();
  const dbs = Object.keys(tree).sort();
  if (!dbs.length) { target.textContent = 'No databases loaded. Click Connect above.'; return; }
  const curDb = easyGetSelectedDb();
  const curTable = easyGetSelectedTable();
  dbs.forEach(db => {
    const tables = tree[db] || [];
    const filtered = filter ? tables.filter(t => t.toLowerCase().includes(filter) || db.toLowerCase().includes(filter)) : tables;
    if (filter && !db.toLowerCase().includes(filter) && !filtered.length) return;
    const wrap = document.createElement('div');
    wrap.className = 'easy-tree-db' + (db === curDb ? ' active' : '');
    const header = document.createElement('div');
    header.className = 'easy-tree-db-header';
    header.innerHTML = '<span class="easy-tree-icon">\uD83D\uDDC3</span> ' + escapeHtml(db) + ' <span class="easy-tree-count">(' + tables.length + ')</span>';
    header.addEventListener('click', () => easySelectDatabase(db));
    wrap.appendChild(header);
    if (db === curDb || filter) {
      const list = document.createElement('div');
      list.className = 'easy-tree-tables';
      filtered.forEach(tbl => {
        const item = document.createElement('div');
        item.className = 'easy-tree-table' + (db === curDb && tbl === curTable ? ' active' : '');
        item.innerHTML = '<span class="easy-tree-icon">\uD83D\uDCC4</span> ' + escapeHtml(tbl);
        item.addEventListener('click', e => { e.stopPropagation(); easyNavigateToTable(db, tbl); });
        list.appendChild(item);
      });
      wrap.appendChild(list);
    }
    target.appendChild(wrap);
  });
}

function easySelectDatabase(db) {
  setSelectedDb(db);
  setSelectedTable('');
  STATE.easyBrowseColumns = [];
  STATE.easyBrowseRows = [];
  STATE.easyRowColumns = [];
  easyResetEditState();
  easyUpdateBreadcrumb();
  easyRenderTree();
  easyRenderDataGrid();
  updateContext();
}

async function easyNavigateToTable(db, table) {
  setSelectedDb(db);
  setSelectedTable(table);
  easyUpdateBreadcrumb();
  easyRenderTree();
  updateContext();
  easySetSubTab('browse');
  try {
    const res = await call('schema.describe_table', { db, table }, 'easyInsertOut');
    const result = unwrapRpcResult(res, 'schema.describe_table');
    easyApplyColumns(result.columns || [], result.primary_key || []);
    dataFormApplyColumns(result.columns || [], result.primary_key || []);
    if ($('dataFormDb')) $('dataFormDb').value = db;
    if ($('dataFormTable')) $('dataFormTable').value = table;
    if ($('dataDb')) $('dataDb').value = db;
    if ($('dataTable')) $('dataTable').value = table;
    if ($('schemaDb')) $('schemaDb').value = db;
    if ($('schemaTable')) $('schemaTable').value = table;
    await easyBrowseRows();
  } catch (e) {
    easyShowToast('Error loading table: ' + e.message, 'error');
  }
}

// --- Column builder helpers ---

function defaultEasyBuilderRows() {
  return [
    { id: easyBuilderNextId++, name: 'id', type: 'i64', nullable: false, auto_increment: true, primary: true },
    { id: easyBuilderNextId++, name: 'name', type: 'string', nullable: false, auto_increment: false, primary: false }
  ];
}

function easySetBuilderRows(rows) {
  const normalized = Array.isArray(rows) ? rows : [];
  STATE.easyTableBuilderRows = normalized
    .map(row => ({
      id: row.id || easyBuilderNextId++,
      name: String(row.name || '').trim(),
      type: String(row.type || 'string').trim() || 'string',
      nullable: !!row.nullable,
      auto_increment: !!row.auto_increment,
      primary: !!row.primary
    }))
    .filter(row => row.name);
  easyRenderColBuilder();
  easyUpdateCreatePreview();
}

function renderEasyBuilderRows() { easyRenderColBuilder(); }

function easyRenderColBuilder() {
  const target = $('easyColRows');
  if (!target) return;
  target.textContent = '';
  if (!STATE.easyTableBuilderRows.length) return;
  STATE.easyTableBuilderRows.forEach(row => {
    const tr = document.createElement('tr');
    tr.dataset.rowId = String(row.id);
    tr.innerHTML =
      '<td><input data-role="name" value="' + escapeHtml(row.name) + '" placeholder="column_name" style="min-width:100px" /></td>' +
      '<td><select data-role="type">' + SCHEMA_TYPE_OPTIONS.map(opt => '<option value="' + opt + '"' + (opt === row.type ? ' selected' : '') + '>' + opt + '</option>').join('') + '</select></td>' +
      '<td style="text-align:center"><input type="checkbox" data-role="nullable"' + (row.nullable ? ' checked' : '') + ' /></td>' +
      '<td style="text-align:center"><input type="checkbox" data-role="auto_increment"' + (row.auto_increment ? ' checked' : '') + ' /></td>' +
      '<td style="text-align:center"><input type="checkbox" data-role="primary"' + (row.primary ? ' checked' : '') + ' /></td>' +
      '<td><button class="danger sm" data-role="remove" style="padding:2px 8px">\u2715</button></td>';
    tr.querySelector('[data-role="remove"]')?.addEventListener('click', () => {
      STATE.easyTableBuilderRows = STATE.easyTableBuilderRows.filter(item => item.id !== row.id);
      easyRenderColBuilder();
      easyUpdateCreatePreview();
    });
    const nullable = tr.querySelector('[data-role="nullable"]');
    const autoIncrement = tr.querySelector('[data-role="auto_increment"]');
    const primary = tr.querySelector('[data-role="primary"]');
    const syncNullability = () => {
      if ((autoIncrement && autoIncrement.checked) || (primary && primary.checked)) {
        nullable.checked = false;
      }
      easyUpdateCreatePreview();
    };
    autoIncrement?.addEventListener('change', syncNullability);
    primary?.addEventListener('change', syncNullability);
    tr.querySelectorAll('input,select').forEach((el) => {
      if (el.dataset.role === 'remove') return;
      el.addEventListener('input', () => easyUpdateCreatePreview());
      el.addEventListener('change', () => easyUpdateCreatePreview());
    });
    target.appendChild(tr);
  });
}

function easyCollectBuilderRows() {
  const target = $('easyColRows');
  if (!target) return [];
  const rows = [];
  target.querySelectorAll('tr').forEach(tr => {
    const name = tr.querySelector('[data-role="name"]')?.value.trim() || '';
    if (!name) return;
    rows.push({
      id: Number(tr.dataset.rowId || 0) || easyBuilderNextId++,
      name,
      type: tr.querySelector('[data-role="type"]')?.value || 'string',
      nullable: !!tr.querySelector('[data-role="nullable"]')?.checked,
      auto_increment: !!tr.querySelector('[data-role="auto_increment"]')?.checked,
      primary: !!tr.querySelector('[data-role="primary"]')?.checked
    });
  });
  return rows;
}

function easyAddColumn() {
  const current = easyCollectBuilderRows();
  if (current.length) STATE.easyTableBuilderRows = current;
  STATE.easyTableBuilderRows.push({
    id: easyBuilderNextId++, name: '', type: 'string', nullable: true, auto_increment: false, primary: false
  });
  easyRenderColBuilder();
}

function easySeedColumns() {
  easySetBuilderRows(defaultEasyBuilderRows());
}

// --- Selection helpers ---

function easyGetSelectedDb() { return STATE.selectedDb || ''; }
function easyGetSelectedTable() { return STATE.selectedTable || ''; }

function easyRefreshTargetsFromTree() {
  easyRenderTree();
  easyUpdateBreadcrumb();
}

function easyApplyColumns(columns, primaryKey) {
  const pk = new Set(Array.isArray(primaryKey) ? primaryKey : []);
  STATE.easyRowColumns = (columns || []).map(col => ({
    name: col.name,
    kind: col.type?.kind || 'string',
    nullable: !!col.nullable,
    primary: pk.has(col.name),
    auto_increment: !!col.auto_increment
  }));
  STATE.easyBrowseColumns = [];
  STATE.easyBrowseRows = [];
  STATE.easySelectedRowIndex = -1;
  STATE.easySelectedRowPk = [];
  STATE.easySelectedRowObject = null;
  easyResetEditState();
  easyRenderDataGrid();
}

function easyResetEditState() {
  STATE.easyGridEditIndex = -1;
  STATE.easyGridEditDraft = {};
  STATE.easyGridCheckedRows = {};
}

function easyReadTableRef() {
  const db = easyGetSelectedDb();
  const table = easyGetSelectedTable();
  if (!db || !table) throw new Error('Select a database and table first');
  return { db, table };
}

function easyPkColumns() {
  return STATE.easyRowColumns.filter(col => col.primary).map(col => col.name);
}

function easyColumnSchema(name) {
  return STATE.easyRowColumns.find(col => col.name === name) || { name, kind: 'string', nullable: true, primary: false, auto_increment: false };
}

function easyInputFromLit(lit) {
  if (!lit || lit.t === 'null') return '';
  if ('v' in lit) return typeof lit.v === 'object' ? JSON.stringify(lit.v) : String(lit.v ?? '');
  if ('iso' in lit) return String(lit.iso ?? '');
  if ('b64' in lit) return String(lit.b64 ?? '');
  return formatLit(lit);
}

function easyParseGridLit(col, raw) {
  const value = String(raw ?? '').trim();
  if (!value.length) {
    if (col.nullable || col.auto_increment) return { t: 'null' };
    throw new Error('Column "' + col.name + '" cannot be empty');
  }
  if (value.toLowerCase() === 'null') {
    if (!col.nullable) throw new Error('Column "' + col.name + '" cannot be NULL');
    return { t: 'null' };
  }
  return literalFromInput(col.kind, value, false);
}

function easyLitEquals(a, b) {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
}

function easyRowObjectToPk(rowObject) {
  return easyPkColumns().map(col => rowObject[col]);
}

function easyPkKey(pkValues) { return JSON.stringify(pkValues || []); }
function easyRowPkKey(rowObject) { return easyPkKey(easyRowObjectToPk(rowObject || {})); }

function easyGridCheckedCount() {
  return Object.keys(STATE.easyGridCheckedRows || {}).length;
}

// ---------------------------------------------------------------------------
// Browse tab – Data grid with inline edit/delete/copy
// ---------------------------------------------------------------------------

function easyIsNumericType(kind) {
  return ['i64', 'u64', 'f64', 'i32', 'u32', 'f32', 'int', 'float', 'decimal', 'number'].includes((kind || '').toLowerCase());
}

function easyFormatCellValue(lit, colName) {
  if (!lit || lit.t === 'null') return { text: 'NULL', isNull: true };
  const val = formatLit(lit);
  const truncated = val.length > 120 ? val.substring(0, 120) + '\u2026' : val;
  const colMeta = easyColumnSchema(colName);
  return { text: truncated, isNull: false, fullText: val, numeric: easyIsNumericType(colMeta.kind) };
}

function easyRenderDataGrid() {
  const table = $('easyDataGrid');
  if (!table) return;
  table.textContent = '';
  const cols = STATE.easyBrowseColumns;
  if (!cols.length) {
    const info = $('easyPgInfo');
    if (info) info.textContent = easyGetSelectedTable() ? 'Click Browse or refresh to load rows.' : 'Select a table from the sidebar to browse data.';
    return;
  }
  const thead = document.createElement('thead');
  const hr = document.createElement('tr');
  const thNum = document.createElement('th'); thNum.style.cssText = 'width:36px;text-align:center;color:var(--muted);font-size:10px'; thNum.textContent = '#'; hr.appendChild(thNum);
  const thCheck = document.createElement('th'); thCheck.style.width = '32px'; thCheck.textContent = '\u2610'; hr.appendChild(thCheck);
  const thActions = document.createElement('th'); thActions.textContent = 'Actions'; thActions.style.minWidth = '110px'; hr.appendChild(thActions);
  cols.forEach(col => {
    const th = document.createElement('th');
    const colMeta = easyColumnSchema(col);
    th.textContent = col;
    th.title = col + ' (' + (colMeta.kind || 'string') + ')' + (colMeta.primary ? ' \uD83D\uDD11' : '') + (colMeta.nullable ? ', nullable' : '') + ' \u2014 click to sort';
    if (colMeta.primary) th.style.borderBottom = '2px solid var(--accent)';
    if (easyIsNumericType(colMeta.kind)) th.style.textAlign = 'right';
    th.classList.add('sortable');
    if (STATE.easySortColumn === col) th.classList.add(STATE.easySortDir === 'desc' ? 'sort-desc' : 'sort-asc');
    th.style.cursor = 'pointer';
    th.addEventListener('click', () => easySetSort(col));
    hr.appendChild(th);
  });
  thead.appendChild(hr); table.appendChild(thead);

  const tbody = document.createElement('tbody');
  const filter = (STATE.easyBrowseFilter || '').toLowerCase();
  const offset = STATE.easyBrowseOffset || 0;
  let visibleCount = 0;
  STATE.easyBrowseRows.forEach((row, idx) => {
    if (filter && !cols.some(col => formatLit(row[col]).toLowerCase().includes(filter))) return;
    visibleCount++;
    const rowPkKey = easyRowPkKey(row);
    const isEditing = idx === STATE.easyGridEditIndex;
    const tr = document.createElement('tr');
    if (isEditing) tr.className = 'editing-row';

    // Row number
    const tdNum = document.createElement('td');
    tdNum.style.cssText = 'text-align:center;color:var(--muted);font-size:10px;user-select:none';
    tdNum.textContent = String(offset + idx + 1);
    tr.appendChild(tdNum);

    // Checkbox
    const tdCheck = document.createElement('td'); tdCheck.className = 'table-check';
    const cb = document.createElement('input'); cb.type = 'checkbox';
    cb.checked = !!STATE.easyGridCheckedRows[rowPkKey];
    cb.addEventListener('click', e => e.stopPropagation());
    cb.addEventListener('change', () => {
      if (cb.checked) STATE.easyGridCheckedRows[rowPkKey] = true;
      else delete STATE.easyGridCheckedRows[rowPkKey];
      easyUpdateCheckedInfo();
    });
    tdCheck.appendChild(cb); tr.appendChild(tdCheck);

    // Actions
    const tdAct = document.createElement('td'); tdAct.className = 'row-actions';
    if (isEditing) {
      const saveBtn = document.createElement('button'); saveBtn.className = 'row-action-btn edit'; saveBtn.textContent = '\uD83D\uDCBE'; saveBtn.title = 'Save';
      saveBtn.addEventListener('click', e => { e.stopPropagation(); easySaveRowEdit(idx); });
      const cancelBtn = document.createElement('button'); cancelBtn.className = 'row-action-btn'; cancelBtn.textContent = '\u2715'; cancelBtn.title = 'Cancel';
      cancelBtn.addEventListener('click', e => { e.stopPropagation(); easyCancelRowEdit(); });
      tdAct.appendChild(saveBtn); tdAct.appendChild(cancelBtn);
    } else {
      const editBtn = document.createElement('button'); editBtn.className = 'row-action-btn edit'; editBtn.textContent = '\u270E'; editBtn.title = 'Edit inline';
      editBtn.addEventListener('click', e => { e.stopPropagation(); easyStartRowEdit(idx); });
      const copyBtn = document.createElement('button'); copyBtn.className = 'row-action-btn copy'; copyBtn.textContent = '\u29C9'; copyBtn.title = 'Copy to Insert';
      copyBtn.addEventListener('click', e => { e.stopPropagation(); easyCopyRowToInsert(idx); });
      const delBtn = document.createElement('button'); delBtn.className = 'row-action-btn delete'; delBtn.textContent = '\u2715'; delBtn.title = 'Delete';
      delBtn.addEventListener('click', e => { e.stopPropagation(); easyDeleteRow(idx); });
      tdAct.appendChild(editBtn); tdAct.appendChild(copyBtn); tdAct.appendChild(delBtn);
    }
    tr.appendChild(tdAct);

    // Data cells
    cols.forEach(col => {
      const td = document.createElement('td');
      if (isEditing) {
        const colMeta = easyColumnSchema(col);
        const input = document.createElement('input');
        input.className = 'inline-edit-input' + (colMeta.primary ? ' pk-readonly' : '');
        input.value = String((STATE.easyGridEditDraft || {})[col] ?? easyInputFromLit(row[col]));
        input.placeholder = dataTypePlaceholder(colMeta.kind);
        if (colMeta.primary) input.readOnly = true;
        input.addEventListener('click', e => e.stopPropagation());
        input.addEventListener('input', e => { STATE.easyGridEditDraft[col] = e.target.value; });
        input.addEventListener('keydown', e => { if (e.key === 'Enter') easySaveRowEdit(idx); if (e.key === 'Escape') easyCancelRowEdit(); });
        td.appendChild(input);
      } else {
        const cell = easyFormatCellValue(row[col], col);
        if (cell.isNull) {
          td.innerHTML = '<i class="null-value">NULL</i>';
        } else {
          td.textContent = cell.text;
          td.title = cell.fullText || cell.text;
          if (cell.numeric) td.style.textAlign = 'right';
        }
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  // Pagination info
  const info = $('easyPgInfo');
  if (info) {
    const count = STATE.easyBrowseRows.length;
    const shown = filter ? visibleCount + ' of ' + count + ' (filtered)' : count;
    info.textContent = count ? 'Showing rows ' + (offset + 1) + '\u2013' + (offset + count) + ' \u00B7 ' + shown + ' rows' : 'No rows in this table.';
  }
  easyUpdateCheckedInfo();
}

function easyUpdateCheckedInfo() {
  const el = $('easyCheckedInfo');
  if (el) el.textContent = easyGridCheckedCount() + ' selected';
  const checkAll = $('easyCheckAll');
  if (checkAll) checkAll.checked = STATE.easyBrowseRows.length > 0 && easyGridCheckedCount() === STATE.easyBrowseRows.length;
}

function easyStartRowEdit(idx) {
  const row = STATE.easyBrowseRows[idx];
  if (!row) return;
  STATE.easyGridEditIndex = idx;
  const draft = {};
  STATE.easyBrowseColumns.forEach(name => { draft[name] = easyInputFromLit(row[name]); });
  STATE.easyGridEditDraft = draft;
  easyRenderDataGrid();
}

function easyCancelRowEdit() {
  STATE.easyGridEditIndex = -1;
  STATE.easyGridEditDraft = {};
  easyRenderDataGrid();
}

async function easySaveRowEdit(idx) {
  try {
    const row = STATE.easyBrowseRows[idx];
    if (!row) throw new Error('Row not found');
    const tableRef = easyReadTableRef();
    const pk = easyRowObjectToPk(row);
    const where = whereByPkColumns(STATE.easyRowColumns, pk);
    if (!where) throw new Error('Could not build primary-key filter');
    const changes = {};
    STATE.easyRowColumns.forEach(col => {
      if (col.primary) return;
      if (!(col.name in STATE.easyGridEditDraft)) return;
      const before = row[col.name];
      const raw = String(STATE.easyGridEditDraft[col.name] ?? '').trim();
      let after;
      if (!raw.length) {
        if (!col.nullable) throw new Error('Column "' + col.name + '" cannot be empty');
        after = { t: 'null' };
      } else {
        after = easyParseGridLit(col, raw);
      }
      if (!easyLitEquals(before, after)) changes[col.name] = after;
    });
    if (!Object.keys(changes).length) { easyShowToast('No changes detected.', 'info'); easyCancelRowEdit(); return; }
    const res = await call('data.update', { table: tableRef, where, set: changes, limit: 1 }, 'easyInsertOut');
    unwrapRpcResult(res, 'data.update');
    STATE.easyGridEditIndex = -1;
    STATE.easyGridEditDraft = {};
    easyShowToast('\u2713 Row updated (' + Object.keys(changes).join(', ') + ').', 'success');
    await easyBrowseRows();
  } catch (e) {
    easyShowToast('Update failed: ' + e.message, 'error');
  }
}

function easyCopyRowToInsert(idx) {
  const row = STATE.easyBrowseRows[idx];
  if (!row) return;
  easySetSubTab('insert');
  easyRenderInsertForm(row);
  easyShowToast('Row copied to Insert tab. Modify and insert.', 'info');
}

async function easyDeleteRow(idx) {
  const row = STATE.easyBrowseRows[idx];
  if (!row) return;
  const pkDisplay = easyPkColumns().map(c => c + '=' + formatLit(row[c])).join(', ');
  const ok = await skeinModal('\uD83D\uDDD1\uFE0F', 'Delete Row', 'Delete row where ' + pkDisplay + '?', [{ label: 'Cancel', value: false }, { label: 'Delete', value: true, cls: 'primary' }]);
  if (!ok) return;
  try {
    const tableRef = easyReadTableRef();
    const pk = easyRowObjectToPk(row);
    const where = whereByPkColumns(STATE.easyRowColumns, pk);
    if (!where) throw new Error('Could not build primary-key filter');
    const res = await call('data.delete', { table: tableRef, where, limit: 1 }, 'easyInsertOut');
    unwrapRpcResult(res, 'data.delete');
    delete STATE.easyGridCheckedRows[easyRowPkKey(row)];
    easyShowToast('\u2713 Row deleted.', 'success');
    await easyBrowseRows();
  } catch (e) {
    easyShowToast('Delete failed: ' + e.message, 'error');
  }
}

async function easyDeleteCheckedRows() {
  const count = easyGridCheckedCount();
  if (!count) { easyShowToast('No rows selected.', 'info'); return; }
  const ok = await skeinModal('\uD83D\uDDD1\uFE0F', 'Delete Selected', 'Delete ' + count + ' selected row(s)? This cannot be undone.', [{ label: 'Cancel', value: false }, { label: 'Delete All', value: true, cls: 'primary' }]);
  if (!ok) return;
  try {
    const tableRef = easyReadTableRef();
    let deleted = 0;
    for (const row of STATE.easyBrowseRows) {
      const key = easyRowPkKey(row);
      if (!STATE.easyGridCheckedRows[key]) continue;
      const where = whereByPkColumns(STATE.easyRowColumns, easyRowObjectToPk(row));
      if (!where) continue;
      await call('data.delete', { table: tableRef, where, limit: 1 }, 'easyInsertOut');
      deleted++;
    }
    STATE.easyGridCheckedRows = {};
    easyShowToast('\u2713 ' + deleted + ' row(s) deleted.', 'success');
    await easyBrowseRows();
  } catch (e) {
    easyShowToast('Batch delete failed: ' + e.message, 'error');
  }
}

function easyToggleCheckAll() {
  const checkAll = $('easyCheckAll');
  if (!checkAll) return;
  STATE.easyGridCheckedRows = {};
  if (checkAll.checked) {
    STATE.easyBrowseRows.forEach(row => {
      const key = easyRowPkKey(row);
      if (key && key !== '[]') STATE.easyGridCheckedRows[key] = true;
    });
  }
  easyRenderDataGrid();
}

function easySetSort(col) {
  if (STATE.easySortColumn === col) {
    STATE.easySortDir = STATE.easySortDir === 'asc' ? 'desc' : 'asc';
  } else {
    STATE.easySortColumn = col;
    STATE.easySortDir = 'asc';
  }
  easyBrowseRows();
}

// ---------------------------------------------------------------------------
// Browse – Pagination & Loading
// ---------------------------------------------------------------------------

function easyBrowsePrev() {
  const limit = parseInt($('easyPerPage')?.value || '', 10) || 25;
  STATE.easyBrowseOffset = Math.max(0, (STATE.easyBrowseOffset || 0) - limit);
  easyBrowseRows();
}

function easyBrowseNext() {
  const limit = parseInt($('easyPerPage')?.value || '', 10) || 25;
  STATE.easyBrowseOffset = (STATE.easyBrowseOffset || 0) + limit;
  easyBrowseRows();
}

async function easyBrowseRows() {
  try {
    const tableRef = easyReadTableRef();
    const limit = parseInt($('easyPerPage')?.value || '', 10) || 25;
    const offset = STATE.easyBrowseOffset || 0;
    const orderCol = STATE.easySortColumn || $('easyOrderCol')?.value.trim() || '';
    const orderDir = STATE.easySortColumn ? STATE.easySortDir : 'asc';
    const desc = await call('schema.describe_table', tableRef, 'easyInsertOut');
    const descResult = unwrapRpcResult(desc, 'schema.describe_table');
    if (!STATE.easyRowColumns.length) {
      easyApplyColumns(descResult.columns || [], descResult.primary_key || []);
    }
    const cols = (descResult.columns || []).map(col => col.name);
    const projection = cols.map(name => ({ expr: { col: name }, as: null }));
    const query = {
      with: [],
      body: { select: { projection, from: [tableRef] } },
      order_by: orderCol ? [{ expr: { col: orderCol }, dir: orderDir }] : [],
      limit: { limit, offset }
    };
    const res = await call('query.select', { query, result_format: 'rows_json' }, 'easyInsertOut');
    const result = unwrapRpcResult(res, 'query.select');
    if (result?.data) {
      const data = result.data;
      STATE.easyBrowseColumns = (data.columns || []).map((col, idx) => {
        if (typeof col === 'string') return col;
        return col?.name || col?.col || ('col' + (idx + 1));
      });
      STATE.easyBrowseRows = (data.rows || []).map(row => {
        const obj = {};
        STATE.easyBrowseColumns.forEach((col, idx) => { obj[col] = Array.isArray(row) ? row[idx] : undefined; });
        return obj;
      });
      STATE.easyBrowseOffset = offset;
      STATE.easyGridEditIndex = -1;
      STATE.easyGridEditDraft = {};
    } else {
      STATE.easyBrowseColumns = cols;
      STATE.easyBrowseRows = [];
    }
    easyRenderDataGrid();
  } catch (e) {
    if (e.message.includes('Select a database')) {
      const info = $('easyPgInfo');
      if (info) info.textContent = 'Select a table from the sidebar.';
    } else {
      easyShowToast('Browse failed: ' + e.message, 'error');
    }
  }
}

// Backward compat aliases
function easyRenderBrowseTable() { easyRenderDataGrid(); }

// ---------------------------------------------------------------------------
// Structure tab
// ---------------------------------------------------------------------------

async function easyLoadStructure() {
  try {
    const tableRef = easyReadTableRef();
    const res = await call('schema.describe_table', tableRef, 'easyInsertOut');
    const result = unwrapRpcResult(res, 'schema.describe_table');
    const table = $('easyStructureGrid');
    if (!table) return;
    table.textContent = '';
    const cols = result.columns || [];
    const pk = new Set(Array.isArray(result.primary_key) ? result.primary_key : []);
    const thead = document.createElement('thead');
    const hr = document.createElement('tr');
    ['#', 'Column', 'Type', 'Nullable', 'Auto Increment', 'Primary Key'].forEach(h => {
      const th = document.createElement('th'); th.textContent = h; hr.appendChild(th);
    });
    thead.appendChild(hr); table.appendChild(thead);
    const tbody = document.createElement('tbody');
    cols.forEach((col, i) => {
      const tr = document.createElement('tr');
      [String(i + 1), col.name, col.type?.kind || JSON.stringify(col.type || ''),
       col.nullable ? 'YES' : 'NO', col.auto_increment ? 'YES' : 'NO',
       pk.has(col.name) ? '\uD83D\uDD11 YES' : 'NO'
      ].forEach(v => { const td = document.createElement('td'); td.textContent = v; tr.appendChild(td); });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    const info = $('easyStructureInfo');
    if (info) info.textContent = cols.length + ' column(s). Primary key: ' + (result.primary_key || []).join(', ');
  } catch (e) {
    easyShowToast('Structure load failed: ' + e.message, 'error');
  }
}

// ---------------------------------------------------------------------------
// Insert tab
// ---------------------------------------------------------------------------

function easyRenderInsertForm(prefill) {
  const target = $('easyInsertFields');
  if (!target) return;
  target.textContent = '';
  if (!STATE.easyRowColumns.length) {
    target.textContent = 'Select a table from the sidebar to insert data.';
    return;
  }
  // Table summary
  const summary = document.createElement('div');
  summary.className = 'hint';
  const pkCols = STATE.easyRowColumns.filter(c => c.primary).map(c => c.name);
  const reqCols = STATE.easyRowColumns.filter(c => !c.nullable && !c.auto_increment && !c.primary).map(c => c.name);
  summary.textContent = STATE.easyRowColumns.length + ' columns' +
    (pkCols.length ? ' \u00B7 PK: ' + pkCols.join(', ') : '') +
    (reqCols.length ? ' \u00B7 Required: ' + reqCols.join(', ') : '');
  target.appendChild(summary);
  const grid = document.createElement('div');
  grid.className = 'easy-edit-fields-grid';
  STATE.easyRowColumns.forEach(col => {
    const item = document.createElement('div');
    item.className = 'easy-field-item';
    if (!col.nullable && !col.auto_increment) item.style.borderLeft = '3px solid var(--accent)';
    else if (col.primary) item.style.borderLeft = '3px solid var(--accent-3)';
    item.dataset.colName = col.name;
    item.dataset.colKind = col.kind;
    item.dataset.colPrimary = col.primary ? 'true' : 'false';
    const label = document.createElement('label');
    label.textContent = col.name + (col.primary ? ' \uD83D\uDD11' : '') + (col.auto_increment ? ' (auto)' : '');
    item.appendChild(label);
    const input = document.createElement('input');
    input.dataset.role = 'value';
    input.placeholder = col.auto_increment ? '(auto-generated)' : dataTypePlaceholder(col.kind);
    if (prefill && prefill[col.name]) {
      input.value = easyInputFromLit(prefill[col.name]);
      if (col.primary && col.auto_increment) input.value = '';
    }
    item.appendChild(input);
    const hint = document.createElement('div');
    hint.className = 'field-hint';
    hint.textContent = col.kind + (col.nullable ? ', nullable' : ', required');
    item.appendChild(hint);
    grid.appendChild(item);
  });
  target.appendChild(grid);
}

function easyCollectInsertRow() {
  const target = $('easyInsertFields');
  if (!target) throw new Error('Load a table first');
  const row = {};
  const missing = [];
  target.querySelectorAll('.easy-field-item').forEach(item => {
    const name = item.dataset.colName || '';
    const kind = item.dataset.colKind || 'string';
    const isPrimary = item.dataset.colPrimary === 'true';
    const colMeta = easyColumnSchema(name);
    if (!name) return;
    const raw = item.querySelector('[data-role="value"]')?.value || '';
    const value = String(raw).trim();
    if (!value.length) {
      if (!colMeta.nullable && !colMeta.auto_increment) missing.push(name + (isPrimary ? ' (primary key)' : ''));
      return;
    }
    row[name] = literalFromInput(kind, raw, false);
  });
  if (missing.length) throw new Error('Missing required fields: ' + missing.join(', '));
  return row;
}

async function easyDoInsert(keepForm) {
  try {
    const tableRef = easyReadTableRef();
    const row = easyCollectInsertRow();
    if (!Object.keys(row).length) throw new Error('Enter at least one value');
    const res = await call('data.insert', { into: tableRef, rows: [row] }, 'easyInsertOut');
    unwrapRpcResult(res, 'data.insert');
    easyShowToast('\u2713 Row inserted successfully!', 'success');
    setOut({ ok: true, inserted: 1 }, 'easyInsertOut');
    if (!keepForm) {
      easySetSubTab('browse');
      await easyBrowseRows();
    } else {
      easyClearInsertForm();
      await easyBrowseRows();
    }
  } catch (e) {
    easyShowToast('Insert failed: ' + e.message, 'error');
    setOut({ error: e.message }, 'easyInsertOut');
  }
}

function easyClearInsertForm() {
  const target = $('easyInsertFields');
  if (!target) return;
  target.querySelectorAll('.easy-field-item input[data-role="value"]').forEach(input => { input.value = ''; });
}

// ---------------------------------------------------------------------------
// Search tab
// ---------------------------------------------------------------------------

function easyPopulateSearchCols() {
  const sel = $('easySearchCol');
  if (!sel) return;
  const current = sel.value;
  sel.textContent = '';
  const all = document.createElement('option'); all.value = ''; all.textContent = 'All columns'; sel.appendChild(all);
  (STATE.easyBrowseColumns.length ? STATE.easyBrowseColumns : STATE.easyRowColumns.map(c => c.name)).forEach(col => {
    const opt = document.createElement('option'); opt.value = col; opt.textContent = col; sel.appendChild(opt);
  });
  if (current) sel.value = current;
}

async function easyDoSearch() {
  try {
    const tableRef = easyReadTableRef();
    const searchVal = $('easySearchValue')?.value.trim() || '';
    const searchCol = $('easySearchCol')?.value || '';
    const searchOp = $('easySearchOp')?.value || 'LIKE';
    if (!searchVal && searchOp !== 'IS NULL' && searchOp !== 'IS NOT NULL') throw new Error('Enter a search value');
    const desc = await call('schema.describe_table', tableRef, 'easyInsertOut');
    const descResult = unwrapRpcResult(desc, 'schema.describe_table');
    const cols = (descResult.columns || []).map(c => c.name);
    const projection = cols.map(name => ({ expr: { col: name }, as: null }));
    let where;
    function buildCondition(col) {
      const colExpr = { col };
      switch (searchOp) {
        case '=': return { op: 'eq', a: colExpr, b: { lit: { t: 'str', v: searchVal } } };
        case '!=': return { op: 'neq', a: colExpr, b: { lit: { t: 'str', v: searchVal } } };
        case '>': return { op: 'gt', a: colExpr, b: { lit: { t: 'str', v: searchVal } } };
        case '<': return { op: 'lt', a: colExpr, b: { lit: { t: 'str', v: searchVal } } };
        case '>=': return { op: 'gte', a: colExpr, b: { lit: { t: 'str', v: searchVal } } };
        case '<=': return { op: 'lte', a: colExpr, b: { lit: { t: 'str', v: searchVal } } };
        case 'IS NULL': return { op: 'is_null', a: colExpr };
        case 'IS NOT NULL': return { op: 'is_not_null', a: colExpr };
        case 'REGEXP': return { op: 'regexp', a: colExpr, b: { lit: { t: 'str', v: searchVal } } };
        case 'BETWEEN': {
          const parts = searchVal.split(',').map(s => s.trim());
          if (parts.length !== 2) throw new Error('BETWEEN requires two comma-separated values');
          return { op: 'between', a: colExpr, b: { lit: { t: 'str', v: parts[0] } }, c: { lit: { t: 'str', v: parts[1] } } };
        }
        default: return { op: 'like', a: colExpr, b: { lit: { t: 'str', v: '%' + searchVal + '%' } } };
      }
    }
    if (searchCol) {
      where = buildCondition(searchCol);
    } else {
      if (searchOp === 'IS NULL' || searchOp === 'IS NOT NULL') {
        const conditions = cols.map(col => buildCondition(col));
        where = conditions.length === 1 ? conditions[0] : { op: 'or', args: conditions };
      } else {
        const conditions = cols.map(col => buildCondition(col));
        where = conditions.length === 1 ? conditions[0] : { op: 'or', args: conditions };
      }
    }
    const query = {
      with: [],
      body: { select: { projection, from: [tableRef], where } },
      order_by: [],
      limit: { limit: 100, offset: 0 }
    };
    const res = await call('query.select', { query, result_format: 'rows_json' }, 'easyInsertOut');
    const result = unwrapRpcResult(res, 'query.select');
    if (result?.data) {
      const data = result.data;
      const columns = (data.columns || []).map((c, i) => typeof c === 'string' ? c : (c?.name || 'col' + (i + 1)));
      renderTable('easySearchGrid', columns, data.rows || []);
      const info = $('easySearchInfo');
      if (info) info.textContent = (data.rows || []).length + ' result(s) found.';
    }
  } catch (e) {
    easyShowToast('Search failed: ' + e.message, 'error');
  }
}

// ---------------------------------------------------------------------------
// Query Builder tab
// ---------------------------------------------------------------------------

function qbInit() {
  const colsAvail = STATE.easyBrowseColumns.length ? STATE.easyBrowseColumns : STATE.easyRowColumns.map(c => c.name);
  qbRenderColumnPicker(colsAvail);
  qbPopulateOrderCol(colsAvail);
  STATE.qbConditions = [];
  qbUpdatePreview();
}

function qbRenderColumnPicker(cols) {
  const container = $('qbColumnPicker');
  if (!container) return;
  container.textContent = '';
  cols.forEach(col => {
    const label = document.createElement('label');
    label.style.cssText = 'display:flex;align-items:center;gap:4px;font-size:12px';
    const cb = document.createElement('input');
    cb.type = 'checkbox'; cb.checked = true; cb.value = col;
    cb.addEventListener('change', () => qbUpdatePreview());
    label.appendChild(cb);
    label.appendChild(document.createTextNode(col));
    container.appendChild(label);
  });
}

function qbPopulateOrderCol(cols) {
  const sel = $('qbOrderCol');
  if (!sel) return;
  const current = sel.value;
  sel.textContent = '';
  const none = document.createElement('option'); none.value = ''; none.textContent = '\u2014'; sel.appendChild(none);
  cols.forEach(col => {
    const opt = document.createElement('option'); opt.value = col; opt.textContent = col; sel.appendChild(opt);
  });
  if (current) sel.value = current;
}

function qbAddCondition() {
  const cols = STATE.easyBrowseColumns.length ? STATE.easyBrowseColumns : STATE.easyRowColumns.map(c => c.name);
  STATE.qbConditions.push({ col: cols[0] || '', op: '=', value: '' });
  qbRenderConditions();
  qbUpdatePreview();
}

function qbRemoveCondition(idx) {
  STATE.qbConditions.splice(idx, 1);
  qbRenderConditions();
  qbUpdatePreview();
}

function qbClearConditions() {
  STATE.qbConditions = [];
  qbRenderConditions();
  qbUpdatePreview();
}

function qbRenderConditions() {
  const container = $('qbConditions');
  if (!container) return;
  container.textContent = '';
  const cols = STATE.easyBrowseColumns.length ? STATE.easyBrowseColumns : STATE.easyRowColumns.map(c => c.name);
  const ops = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IS NULL', 'IS NOT NULL', 'REGEXP'];
  STATE.qbConditions.forEach((cond, i) => {
    const row = document.createElement('div');
    row.className = 'qb-row';
    if (i > 0) {
      const logic = document.createElement('button');
      logic.className = 'qb-logic-btn';
      logic.textContent = 'AND';
      row.appendChild(logic);
    }
    const colSel = document.createElement('select');
    cols.forEach(c => { const o = document.createElement('option'); o.value = c; o.textContent = c; colSel.appendChild(o); });
    colSel.value = cond.col;
    colSel.addEventListener('change', () => { cond.col = colSel.value; qbUpdatePreview(); });
    row.appendChild(colSel);
    const opSel = document.createElement('select');
    ops.forEach(o => { const opt = document.createElement('option'); opt.value = o; opt.textContent = o; opSel.appendChild(opt); });
    opSel.value = cond.op;
    opSel.addEventListener('change', () => { cond.op = opSel.value; qbUpdatePreview(); });
    row.appendChild(opSel);
    if (cond.op !== 'IS NULL' && cond.op !== 'IS NOT NULL') {
      const input = document.createElement('input');
      input.value = cond.value; input.placeholder = 'value';
      input.addEventListener('input', () => { cond.value = input.value; qbUpdatePreview(); });
      row.appendChild(input);
    }
    const removeBtn = document.createElement('button');
    removeBtn.className = 'qb-remove';
    removeBtn.textContent = '\u2715';
    removeBtn.addEventListener('click', () => qbRemoveCondition(i));
    row.appendChild(removeBtn);
    container.appendChild(row);
  });
}

function qbGetSelectedColumns() {
  const picks = [];
  const container = $('qbColumnPicker');
  if (!container) return [];
  container.querySelectorAll('input[type="checkbox"]:checked').forEach(cb => picks.push(cb.value));
  return picks.length ? picks : (STATE.easyBrowseColumns.length ? STATE.easyBrowseColumns : STATE.easyRowColumns.map(c => c.name));
}

function qbBuildSQL() {
  const tableRef = easyReadTableRef();
  const tableName = tableRef.db + '.' + tableRef.table;
  const selectedCols = qbGetSelectedColumns();
  const colStr = selectedCols.length ? selectedCols.join(', ') : '*';
  let sql = 'SELECT ' + colStr + ' FROM ' + tableName;
  if (STATE.qbConditions.length) {
    const parts = STATE.qbConditions.map(c => {
      if (c.op === 'IS NULL') return c.col + ' IS NULL';
      if (c.op === 'IS NOT NULL') return c.col + ' IS NOT NULL';
      if (c.op === 'LIKE') return c.col + " LIKE '%" + c.value.replace(/'/g, "''") + "%'";
      return c.col + ' ' + c.op + " '" + c.value.replace(/'/g, "''") + "'";
    });
    sql += ' WHERE ' + parts.join(' AND ');
  }
  const orderCol = $('qbOrderCol')?.value || '';
  const orderDir = $('qbOrderDir')?.value || 'ASC';
  if (orderCol) sql += ' ORDER BY ' + orderCol + ' ' + orderDir;
  const limit = parseInt($('qbLimit')?.value || '', 10) || 50;
  sql += ' LIMIT ' + limit;
  return sql + ';';
}

function qbUpdatePreview() {
  try {
    const sql = qbBuildSQL();
    const el = $('qbPreview');
    if (el) el.textContent = sql;
  } catch (e) {
    const el = $('qbPreview');
    if (el) el.textContent = 'SELECT * FROM ...';
  }
}

async function qbExecute() {
  try {
    const sql = qbBuildSQL();
    const res = await call('sql.exec', { sql, default_db: easyGetSelectedDb() }, 'qbOut');
    if (!res || !res.json) throw new Error('No response');
    const r = res.json.result || res.json;
    const tbl = extractSqlTable(r);
    if (tbl) {
      renderTable('qbResultGrid', tbl.columns, tbl.rows);
      setOut({ rows: (tbl.rows || []).length }, 'qbOut');
    } else {
      setOut(r, 'qbOut');
    }
    easyShowToast('\u2713 Query executed.', 'success');
  } catch (e) {
    easyShowToast('Query Builder: ' + e.message, 'error');
    setOut({ error: e.message }, 'qbOut');
  }
}

function qbCopySQL() {
  try {
    const sql = qbBuildSQL();
    navigator.clipboard.writeText(sql);
    easyShowToast('SQL copied to clipboard.', 'info');
  } catch (e) { easyShowToast('Copy failed.', 'error'); }
}

function qbSendToSQL() {
  try {
    const sql = qbBuildSQL();
    if ($('sqlText')) $('sqlText').value = sql;
    easyShowToast('SQL sent to SQL workspace tab.', 'info');
  } catch (e) { easyShowToast('Send failed.', 'error'); }
}

// ---------------------------------------------------------------------------
// Create Table tab
// ---------------------------------------------------------------------------

function easyUpdateCreatePreview() {
  const el = $('easyCreatePreview');
  if (!el) return;
  const db = $('easyCreateDb')?.value.trim() || easyGetSelectedDb() || 'demo';
  const table = $('easyCreateTableName')?.value.trim() || 'my_table';
  const rows = easyCollectBuilderRows();
  const analysis = analyzeEasyCreateDraft(db, table, rows);
  if (!rows.length) {
    const intro = analysis.errors.length
      ? '-- Add at least one valid column below.\n'
      : '';
    el.textContent = intro + 'CREATE TABLE ' + db + '.' + table + ' (...);';
    return;
  }
  const defs = rows.map((row) => {
    let out = row.name + ' ' + row.type.toUpperCase();
    if (!row.nullable) out += ' NOT NULL';
    if (row.auto_increment) out += ' AUTO_INCREMENT';
    return out;
  });
  const primaryKey = rows.filter((row) => row.primary).map((row) => row.name);
  if (primaryKey.length) defs.push('PRIMARY KEY (' + primaryKey.join(', ') + ')');
  const notes = [];
  analysis.errors.forEach((msg) => notes.push('-- ERROR: ' + msg));
  analysis.warnings.forEach((msg) => notes.push('-- NOTE: ' + msg));
  const sql = 'CREATE TABLE ' + db + '.' + table + ' (\n  ' + defs.join(',\n  ') + '\n);';
  el.textContent = (notes.length ? notes.join('\n') + '\n\n' : '') + sql;
}

async function easyDoCreateTable() {
  try {
    const db = validateEasyIdentifier($('easyCreateDb')?.value.trim() || easyGetSelectedDb(), 'Database name');
    const table = validateEasyIdentifier($('easyCreateTableName')?.value.trim(), 'Table name');
    const rows = easyCollectBuilderRows();
    if (!rows.length) throw new Error('Define at least one column');
    const analysis = analyzeEasyCreateDraft(db, table, rows);
    if (analysis.errors.length) throw new Error(analysis.errors.join(' '));
    const columns = rows.map(row => cleanParams({
      name: row.name, type: { kind: row.type }, nullable: row.nullable, auto_increment: row.auto_increment
    }));
    const primaryKey = rows.filter(row => row.primary).map(row => row.name);
    const res = await call('schema.create_table', { db, table, columns, primary_key: primaryKey, if_not_exists: true }, 'easyCreateOut');
    unwrapRpcResult(res, 'schema.create_table');
    easyShowToast('\u2713 Table "' + db + '.' + table + '" created!', 'success');
    setOut({ ok: true, table: db + '.' + table }, 'easyCreateOut');
    await loadDbTree();
    easyRefreshTargetsFromTree();
    await easyNavigateToTable(db, table);
  } catch (e) {
    easyShowToast('Create table failed: ' + e.message, 'error');
    setOut({ error: e.message }, 'easyCreateOut');
  }
}

async function easyDoCreateDb() {
  try {
    const db = validateEasyIdentifier($('easyCreateDb')?.value.trim(), 'Database name');
    const res = await call('schema.create_database', { db }, 'easyCreateOut');
    unwrapRpcResult(res, 'schema.create_database');
    setSelectedDb(db);
    easyShowToast('\u2713 Database "' + db + '" created!', 'success');
    setOut({ ok: true, database: db }, 'easyCreateOut');
    await loadDbTree();
    easyRefreshTargetsFromTree();
  } catch (e) {
    easyShowToast('Create database failed: ' + e.message, 'error');
    setOut({ error: e.message }, 'easyCreateOut');
  }
}

function easyToggleNewDbForm(show) {
  const form = $('easyNewDbForm');
  if (!form) return;
  form.classList.toggle('active', !!show);
  if (show) $('easyNewDbName')?.focus();
}

async function easyCreateDbInline() {
  const input = $('easyNewDbName');
  const raw = input?.value.trim() || '';
  if (!raw) {
    easyShowToast('Enter a database name first.', 'info');
    return;
  }
  try {
    const name = validateEasyIdentifier(raw, 'Database name');
    const res = await call('schema.create_database', { db: name }, 'easyCreateOut');
    unwrapRpcResult(res, 'schema.create_database');
    if (input) input.value = '';
    setSelectedDb(name);
    easyToggleNewDbForm(false);
    easyShowToast('\u2713 Database "' + name + '" created!', 'success');
    await loadDbTree();
    easyRefreshTargetsFromTree();
    easySelectDatabase(name);
  } catch (e) {
    easyShowToast('Create failed: ' + e.message, 'error');
  }
}

// ---------------------------------------------------------------------------
// Export tab
// ---------------------------------------------------------------------------

async function easyDoExport() {
  try {
    const tableRef = easyReadTableRef();
    const fmt = $('easyExportFmt')?.value || 'json';
    const desc = await call('schema.describe_table', tableRef, 'easyExportOut');
    const descResult = unwrapRpcResult(desc, 'schema.describe_table');
    const cols = (descResult.columns || []).map(c => c.name);
    const projection = cols.map(name => ({ expr: { col: name }, as: null }));
    const query = { with: [], body: { select: { projection, from: [tableRef] } }, order_by: [], limit: { limit: 10000, offset: 0 } };
    const res = await call('query.select', { query, result_format: 'rows_json' }, 'easyExportOut');
    const result = unwrapRpcResult(res, 'query.select');
    if (!result?.data) throw new Error('No data returned');
    const data = result.data;
    const columns = (data.columns || []).map((c, i) => typeof c === 'string' ? c : (c?.name || 'col' + (i + 1)));
    const rows = data.rows || [];
    if (fmt === 'json') {
      const objects = rows.map(row => {
        const obj = {}; columns.forEach((col, i) => { obj[col] = Array.isArray(row) ? formatLit(row[i]) : ''; }); return obj;
      });
      downloadBlob(JSON.stringify(objects, null, 2), tableRef.db + '_' + tableRef.table + '.json', 'application/json');
    } else if (fmt === 'csv') {
      const lines = [columns.join(',')];
      rows.forEach(row => {
        lines.push(columns.map((_, i) => { const v = Array.isArray(row) ? formatLit(row[i]) : ''; return '"' + String(v).replace(/"/g, '""') + '"'; }).join(','));
      });
      downloadBlob(lines.join('\n'), tableRef.db + '_' + tableRef.table + '.csv', 'text/csv');
    } else {
      const inserts = rows.map(row => {
        const vals = columns.map((_, i) => {
          const v = Array.isArray(row) ? row[i] : null;
          if (!v || v.t === 'null') return 'NULL';
          if (v.t === 'i64' || v.t === 'f64' || v.t === 'u64') return String(v.v);
          if (v.t === 'bool') return v.v ? 'TRUE' : 'FALSE';
          return "'" + String(formatLit(v)).replace(/'/g, "''") + "'";
        });
        return 'INSERT INTO ' + tableRef.db + '.' + tableRef.table + ' (' + columns.join(', ') + ') VALUES (' + vals.join(', ') + ');';
      });
      downloadBlob(inserts.join('\n'), tableRef.db + '_' + tableRef.table + '.sql', 'text/sql');
    }
    easyShowToast('\u2713 Exported ' + rows.length + ' rows as ' + fmt.toUpperCase() + '.', 'success');
    setOut({ ok: true, rows: rows.length, format: fmt }, 'easyExportOut');
  } catch (e) {
    easyShowToast('Export failed: ' + e.message, 'error');
    setOut({ error: e.message }, 'easyExportOut');
  }
}

async function easyDoExportStruct() {
  try {
    const tableRef = easyReadTableRef();
    const res = await call('schema.describe_table', tableRef, 'easyExportOut');
    const result = unwrapRpcResult(res, 'schema.describe_table');
    downloadBlob(JSON.stringify(result, null, 2), tableRef.db + '_' + tableRef.table + '_schema.json', 'application/json');
    easyShowToast('\u2713 Structure exported.', 'success');
  } catch (e) {
    easyShowToast('Export failed: ' + e.message, 'error');
  }
}

// ---------------------------------------------------------------------------
// Operations tab
// ---------------------------------------------------------------------------

async function easyTruncateTable() {
  try {
    const tableRef = easyReadTableRef();
    const ok = await skeinModal('\u26A0\uFE0F', 'Truncate Table', 'Truncate all rows from "' + tableRef.db + '.' + tableRef.table + '"? This cannot be undone.', [{ label: 'Cancel', value: false }, { label: 'Truncate', value: true, cls: 'primary' }]);
    if (!ok) return;
    const res = await call('sql.exec', { sql: 'DELETE FROM ' + tableRef.db + '.' + tableRef.table + ';' }, 'easyOpsOut');
    easyShowToast('\u2713 Table truncated.', 'success');
    setOut(res, 'easyOpsOut');
    await easyBrowseRows();
  } catch (e) {
    easyShowToast('Truncate failed: ' + e.message, 'error');
    setOut({ error: e.message }, 'easyOpsOut');
  }
}

async function easyDropTableOp() {
  try {
    const tableRef = easyReadTableRef();
    const okDrop = await skeinModal('\u26A0\uFE0F', 'Drop Table', 'DROP TABLE "' + tableRef.db + '.' + tableRef.table + '"? This permanently deletes the table and all data.', [{ label: 'Cancel', value: false }, { label: 'Drop', value: true, cls: 'primary' }]);
    if (!okDrop) return;
    const res = await call('schema.drop_table', tableRef, 'easyOpsOut');
    easyShowToast('\u2713 Table "' + tableRef.table + '" dropped.', 'success');
    setOut(res, 'easyOpsOut');
    setSelectedTable('');
    await loadDbTree();
    easyRefreshTargetsFromTree();
    easyUpdateBreadcrumb();
    STATE.easyBrowseColumns = []; STATE.easyBrowseRows = []; STATE.easyRowColumns = [];
    easyRenderDataGrid();
  } catch (e) {
    easyShowToast('Drop table failed: ' + e.message, 'error');
  }
}

async function easyDropDbOp() {
  try {
    const db = easyGetSelectedDb();
    if (!db) throw new Error('Select a database first');
    const okDropDb = await skeinModal('\u26A0\uFE0F', 'Drop Database', 'DROP DATABASE "' + db + '"? This permanently deletes ALL tables and data.', [{ label: 'Cancel', value: false }, { label: 'Drop', value: true, cls: 'primary' }]);
    if (!okDropDb) return;
    const res = await call('schema.drop_database', { db }, 'easyOpsOut');
    easyShowToast('\u2713 Database "' + db + '" dropped.', 'success');
    setOut(res, 'easyOpsOut');
    setSelectedDb(''); setSelectedTable('');
    await loadDbTree();
    easyRefreshTargetsFromTree();
    easyUpdateBreadcrumb();
    STATE.easyBrowseColumns = []; STATE.easyBrowseRows = []; STATE.easyRowColumns = [];
    easyRenderDataGrid();
  } catch (e) {
    easyShowToast('Drop database failed: ' + e.message, 'error');
  }
}

// ---------------------------------------------------------------------------
// Easy Viewer – SQL sub-tab
// ---------------------------------------------------------------------------

async function easyRunSql() {
  const sql = $('easySqlText') ? $('easySqlText').value.trim() : '';
  if (!sql) { easyShowToast('Enter a SQL query first.', 'info'); return; }
  try {
    const db = easyGetSelectedDb();
    const res = await call('sql.exec', cleanParams({ sql, default_db: db || resolveDefaultDb() }), 'easySqlOut');
    if (!res || !res.json || !res.json.ok || !res.json.result) {
      setOut(res?.json || res, 'easySqlOut');
      return;
    }
    const r = res.json.result;
    const tbl = extractSqlTable(r);
    if (tbl) renderTable('easySqlGrid', tbl.columns, tbl.rows);
    else renderTable('easySqlGrid', [], []);
    setOut(r, 'easySqlOut');
    easyShowToast('\u2713 SQL executed.', 'success');
    if (r.statement === 'create_database' || r.statement === 'create_table' || r.statement === 'drop_table') {
      await loadDbTree();
      easyRefreshTargetsFromTree();
    }
  } catch (e) {
    easyShowToast('SQL error: ' + e.message, 'error');
    setOut({ error: e.message }, 'easySqlOut');
  }
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------
function setSqlText(v) { if ($('sqlText')) { $('sqlText').value = v; $('sqlText').focus(); } }

async function runSql(explain) {
  const sql = $('sqlText') ? $('sqlText').value.trim() : ''; if (!sql) return;
  addSqlHistory(sql);
  const res = await call('sql.exec', cleanParams({sql, explain:!!explain, default_db:resolveDefaultDb()}), 'sqlOut');
  if (!res || !res.json || !res.json.ok || !res.json.result) return;
  const r = res.json.result;
  const tbl = extractSqlTable(r); if (tbl) renderTable('sqlTable', tbl.columns, tbl.rows); else renderTable('sqlTable',[],[]);
  if (r.statement === 'use' && r.default_db) { setSelectedDb(r.default_db); updateContext(); }
  if (r.statement === 'create_database' || r.statement === 'create_table') await loadDbTree();
  showToast('SQL executed', 'success', 2000);
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

async function clusterLeaveNode() {
  try {
    const id = $('clusterNodeId')?.value.trim();
    if (!id) throw new Error('Node id required');
    await call('cluster.node.leave', { node_id: id }, 'clusterOut');
  } catch (e) { setOut({error:String(e)}, 'clusterOut'); }
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

function settingsUsePreset() {
  const preset = $('settingsPreset')?.value || SETTINGS_PRESET_KEYS[0];
  if ($('settingsKey')) $('settingsKey').value = preset;
  settingsGetKey();
}

async function settingsListAll() {
  const res = await call('settings.list', {}, 'settingsOut');
  if (res?.json?.ok && res.json.result) {
    const keys = Object.keys(res.json.result).sort();
    const list = $('settingsKeyList');
    if (list) {
      list.textContent = '';
      keys.forEach((key) => {
        const btn = document.createElement('button');
        btn.className = 'settings-key-btn sm';
        btn.textContent = key;
        btn.addEventListener('click', () => {
          if ($('settingsKey')) $('settingsKey').value = key;
          if ($('settingsValue')) $('settingsValue').value = JSON.stringify(res.json.result[key], null, 2);
        });
        list.appendChild(btn);
      });
    }
  }
}

async function settingsLoadCapabilities() { await loadCapabilities(); }
async function settingsLoadTransport() {
  const res = await call('transport.capabilities', {}, 'settingsCapabilitiesOut');
  if (res?.json?.ok && res.json.result) renderSettingsCapabilities({ methods: [], transport: res.json.result });
}
async function settingsLoadFeatureFlags() { await call('telemetry.feature_flags', {}, 'settingsCapabilitiesOut'); }
async function settingsLoadWorkloadFeatures() { await call('telemetry.workload_features', {}, 'settingsCapabilitiesOut'); }

// ---------------------------------------------------------------------------
// Engine Config
// ---------------------------------------------------------------------------
const ENGINE_TOGGLES = [
  { id: 'engDedup',       key: 'engine.dedup.enabled' },
  { id: 'engCompression', key: 'engine.compression.enabled' },
  { id: 'engEncryption',  key: 'engine.encryption.enabled' },
  { id: 'engMvcc',        key: 'engine.mvcc.enabled' },
  { id: 'engDeltaChains', key: 'engine.delta_chains.enabled' },
  { id: 'engTimeTravel',  key: 'engine.time_travel.enabled' },
  { id: 'engAutoCompact', key: 'engine.compaction.auto' },
  { id: 'engEnergyAware', key: 'engine.compaction.energy_aware' },
  { id: 'engQueryCache',  key: 'engine.cache.enabled' },
  { id: 'engCoalescing',  key: 'engine.coalescing.enabled' },
  { id: 'engAutoParam',   key: 'engine.autoparameterize.enabled' },
  { id: 'engAuditWal',    key: 'engine.audit_wal.enabled' },
  { id: 'engDiffPrivacy', key: 'engine.differential_privacy.enabled' },
  { id: 'engOblivious',   key: 'engine.oblivious.enabled' },
  { id: 'engReplication',  key: 'engine.replication.enabled' },
  { id: 'engCdc',         key: 'engine.cdc.enabled' },
  { id: 'engQuic',        key: 'engine.quic.enabled' }
];

const ENGINE_FIELDS = [
  { id: 'engStorageMode',   key: 'engine.storage_mode' },
  { id: 'engRetentionDays', key: 'engine.time_travel.retention_days', type: 'number' },
  { id: 'engMaxL0',         key: 'engine.compaction.max_l0_files', type: 'number' },
  { id: 'engCacheSizeMb',   key: 'engine.cache.size_mb', type: 'number' }
];

async function engineLoadConfig() {
  try {
    const keys = [
      ...ENGINE_TOGGLES.map(t => t.key),
      ...ENGINE_FIELDS.map(f => f.key)
    ];
    const res = await call('settings.get', { keys }, 'engineOut');
    if (!res?.json?.ok) return;
    const cfg = res.json.result || {};
    ENGINE_TOGGLES.forEach(t => {
      const el = $(t.id);
      if (el && cfg[t.key] !== undefined) el.checked = !!cfg[t.key];
    });
    ENGINE_FIELDS.forEach(f => {
      const el = $(f.id);
      if (el && cfg[f.key] !== undefined) el.value = String(cfg[f.key]);
    });
    setOut({ loaded: true, config: cfg }, 'engineOut');
  } catch (e) { setOut({ error: String(e) }, 'engineOut'); }
}

async function engineSaveConfig() {
  try {
    const payload = {};
    ENGINE_TOGGLES.forEach(t => {
      const el = $(t.id);
      if (el) payload[t.key] = el.checked;
    });
    ENGINE_FIELDS.forEach(f => {
      const el = $(f.id);
      if (!el) return;
      if (f.type === 'number') payload[f.key] = parseInt(el.value, 10) || 0;
      else payload[f.key] = el.value;
    });
    await call('settings.set', payload, 'engineOut');
    setOut({ saved: true, config: payload }, 'engineOut');
  } catch (e) { setOut({ error: String(e) }, 'engineOut'); }
}

function engineResetDefaults() {
  const defaults = {
    engDedup: true, engCompression: false, engEncryption: false,
    engMvcc: true, engDeltaChains: false, engTimeTravel: false,
    engAutoCompact: true, engEnergyAware: false,
    engQueryCache: true, engCoalescing: false, engAutoParam: false,
    engAuditWal: false, engDiffPrivacy: false, engOblivious: false,
    engReplication: false, engCdc: false, engQuic: false
  };
  Object.entries(defaults).forEach(([id, val]) => {
    const el = $(id); if (el) el.checked = val;
  });
  const fieldDefaults = { engStorageMode: 'segment', engRetentionDays: '7', engMaxL0: '8', engCacheSizeMb: '256' };
  Object.entries(fieldDefaults).forEach(([id, val]) => {
    const el = $(id); if (el) el.value = val;
  });
  setOut({ reset: true, note: 'Defaults applied locally. Click Save to persist.' }, 'engineOut');
}

// ---------------------------------------------------------------------------
// Users & Grants (T044 – admin.user.* RPCs)
// ---------------------------------------------------------------------------
async function userCreate() {
  try {
    const name = $('userName')?.value.trim(), pass = $('userPass')?.value.trim(), role = $('userRole')?.value;
    if (!name) throw new Error('Username required');
    await call('admin.user.create', { username: name, role }, 'usersOut');
  } catch (e) { setOut({error:String(e)},'usersOut'); }
}

async function userList() { await call('admin.user.list', {}, 'usersOut'); }

async function userDrop() {
  const name = $('userName')?.value.trim(); if (!name) return;
  const ok = await skeinModal('\u26A0\uFE0F', 'Drop User', 'Drop user "' + name + '"?', [
    { label: 'Cancel', value: false, cls: 'ghost' },
    { label: 'Drop', value: true, cls: 'danger' }
  ]);
  if (!ok) return;
  await call('admin.user.drop', { username: name }, 'usersOut');
}

async function userGrant() {
  try {
    const name = $('userName')?.value.trim(), db = $('userGrantDb')?.value.trim(), privs = $('userGrantPrivs')?.value.trim();
    if (!name || !db) throw new Error('User + db required');
    await call('admin.user.grant', { username: name, db, privileges: privs ? privs.split(',').map(s=>s.trim()) : ['SELECT'] }, 'usersOut');
  } catch (e) { setOut({error:String(e)},'usersOut'); }
}

async function userRevoke() {
  try {
    const name = $('userName')?.value.trim(), db = $('userGrantDb')?.value.trim(), privs = $('userGrantPrivs')?.value.trim();
    if (!name || !db) throw new Error('User + db required');
    await call('admin.user.revoke', { username: name, db, privileges: privs ? privs.split(',').map(s=>s.trim()) : ['SELECT'] }, 'usersOut');
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
      if (fmt === 'csv') {
        const csv = resultToCSV(res.json.result);
        downloadBlob(csv, t.db + '_' + t.table + '.csv', 'text/csv');
      } else {
        downloadBlob(JSON.stringify(res.json.result, null, 2), t.db + '_' + t.table + '.json', 'application/json');
      }
    }
  } catch (e) { setOut({error:String(e)},'importOut'); }
}

function resultToCSV(result) {
  const rows = result.rows || result.data || [];
  if (!rows.length) return '';
  const keys = Object.keys(rows[0]);
  const escape = v => { const s = String(v ?? ''); return s.includes(',') || s.includes('"') || s.includes('\n') ? '"' + s.replace(/"/g, '""') + '"' : s; };
  const header = keys.map(escape).join(',');
  const body = rows.map(row => keys.map(k => { const cell = row[k]; return escape(cell && typeof cell === 'object' && 'v' in cell ? cell.v : cell); }).join(',')).join('\n');
  return header + '\n' + body;
}

function parseCSV(text) {
  const lines = text.split('\n').map(l => l.trim()).filter(l => l.length > 0);
  if (lines.length < 2) return [];
  const parseRow = line => {
    const cells = []; let cur = '', inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQ) { if (ch === '"' && line[i+1] === '"') { cur += '"'; i++; } else if (ch === '"') inQ = false; else cur += ch; }
      else { if (ch === '"') inQ = true; else if (ch === ',') { cells.push(cur); cur = ''; } else cur += ch; }
    }
    cells.push(cur);
    return cells;
  };
  const headers = parseRow(lines[0]);
  return lines.slice(1).map(line => {
    const vals = parseRow(line);
    const obj = {};
    headers.forEach((h, i) => { obj[h] = vals[i] ?? ''; });
    return obj;
  });
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
    const fmt = $('importFormat')?.value || 'json';
    let rows;
    if (fmt === 'csv') {
      rows = parseCSV(raw);
    } else {
      const data = parseJsonInput(raw, 'Import data');
      rows = Array.isArray(data) ? data : [data];
    }
    // Convert plain objects to typed values
    const typedRows = rows.map(row => {
      const out = {};
      for (const [k, v] of Object.entries(row)) {
        if (v && typeof v === 'object' && 't' in v) out[k] = v;
        else if (typeof v === 'number') out[k] = { t: Number.isInteger(v) ? 'i64' : 'f64', v };
        else if (typeof v === 'string') out[k] = { t: 'str', v };
        else if (typeof v === 'boolean') out[k] = { t: 'bool', v };
        else if (v === null) out[k] = { t: 'null' };
        else out[k] = { t: 'str', v: JSON.stringify(v) };
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
    const statusCls = track.status === 'hardened' ? 'tag secondary' : 'tag';
    const statusLabel = track.status === 'hardened' ? '\u2705 Hardened' : '\uD83E\uDDEA Prototype';
    card.innerHTML = '<h3>' + escapeHtml(track.id) + ' — ' + escapeHtml(track.title) +
      ' <span class="' + statusCls + '">' + statusLabel + '</span></h3>' +
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

function renderSettingsCapabilities(payload) {
  const out = $('settingsCapabilitiesOut');
  if (out) setOut(payload, 'settingsCapabilitiesOut');
  const grid = $('settingsKeyList');
  if (!grid) return;
  const methods = Array.isArray(payload?.methods) ? payload.methods : [];
  if (!methods.length) {
    if (!grid.childElementCount) grid.textContent = 'No methods loaded yet.';
    return;
  }
  grid.textContent = '';
  methods.forEach((method) => {
    const btn = document.createElement('button');
    btn.className = 'settings-key-btn sm';
    btn.textContent = method;
    btn.addEventListener('click', () => {
      if ($('rpcMethod')) $('rpcMethod').value = method;
      if ($('rpcParams')) $('rpcParams').value = '{}';
      setActivePanel('rpc', true);
    });
    grid.appendChild(btn);
  });
}

// ---------------------------------------------------------------------------
// Dashboard cards – Top Tables, Slow Queries, Sessions, Index Health, Research
// ---------------------------------------------------------------------------

async function refreshTopTables() {
  try {
    const tree = STATE.dbTree || {};
    const rows = [];
    for (const db of Object.keys(tree)) {
      for (const table of (tree[db] || [])) {
        rows.push([db + '.' + table, '\u2014']);
      }
    }
    rows.sort((a, b) => a[0].localeCompare(b[0]));
    renderTable('topTablesGrid', ['Table', 'Rows (est.)'], rows.slice(0, 10));
  } catch (e) { easyShowToast('Top tables: ' + e.message, 'error'); }
}

async function refreshSlowQueries() {
  try {
    const history = STATE.sqlHistory || [];
    const rows = history.slice(-10).reverse().map(entry => [
      typeof entry === 'string' ? entry : (entry.sql || ''),
      typeof entry === 'string' ? '\u2014' : (entry.ms || '\u2014')
    ]);
    renderTable('slowQueryGrid', ['Query', 'Time (ms)'], rows.length ? rows : [['No queries recorded', '\u2014']]);
  } catch (e) { easyShowToast('Slow queries: ' + e.message, 'error'); }
}

async function refreshActiveSessions() {
  try {
    const el = (id, v) => { const e = $(id); if (e) e.textContent = v; };
    el('statActiveSessions', '1');
    el('statIdleSessions', '0');
    el('statLongestQuery', '<1s');
  } catch (e) { easyShowToast('Sessions: ' + e.message, 'error'); }
}

async function refreshIndexHealth() {
  try {
    const res = await call('advisor.history', {}, 'out');
    const result = res?.json?.result;
    const recs = Array.isArray(result?.recommendations) ? result.recommendations : [];
    const applied = recs.filter(r => r.status === 'applied').length;
    const dismissed = recs.filter(r => r.status === 'dismissed').length;
    const el = (id, v) => { const e = $(id); if (e) e.textContent = v; };
    el('statIndexRecs', String(recs.length));
    el('statIndexApplied', String(applied));
    el('statIndexDismissed', String(dismissed));
  } catch (e) {
    const el = (id, v) => { const e = $(id); if (e) e.textContent = v; };
    el('statIndexRecs', '0');
    el('statIndexApplied', '0');
    el('statIndexDismissed', '0');
  }
}

function renderResearchStatusGrid() {
  const grid = $('researchStatusGrid'); if (!grid) return;
  grid.textContent = '';
  RESEARCH_TRACKS.forEach(track => {
    const card = document.createElement('div');
    card.className = 'feature-card';
    const badge = track.status === 'hardened' ? '<span class="tag secondary" style="font-size:9px">\u2705 hardened</span>' : '<span class="tag" style="font-size:9px">\uD83D\uDEA7 proto</span>';
    card.innerHTML = '<div class="feature-title" style="font-size:11px">' + escapeHtml(track.id) + ' ' + badge + '</div><div class="hint" style="font-size:10px">' + escapeHtml(track.title) + '</div>';
    if (track.panel) {
      card.style.cursor = 'pointer';
      card.addEventListener('click', () => setActivePanel(track.panel, true));
    }
    grid.appendChild(card);
  });
}

// ---------------------------------------------------------------------------
// Security — Token management (T122)
// ---------------------------------------------------------------------------
async function securityCreateToken() {
  const label = $('secTokenLabel')?.value.trim() || '';
  const role = $('secTokenRole')?.value || 'admin';
  const ttlHrs = parseInt($('secTokenTtl')?.value, 10) || 0;
  const ttlMs = ttlHrs > 0 ? ttlHrs * 3600000 : 0;
  try {
    const r = await call('security.token.create', { role, label, ttl_ms: ttlMs }, 'secTokenOut');
    if (r?.secret) {
      const el = $('secTokenOut');
      if (el) el.textContent = 'Token created. Secret (copy now — shown once):\n' + r.secret;
    }
    await securityRefreshTokens();
  } catch (e) { setOut({ error: String(e) }, 'secTokenOut'); }
}

async function securityRefreshTokens() {
  try {
    const r = await call('security.token.list', {}, 'secTokenOut');
    const grid = $('secTokenGrid'); if (!grid) return;
    const tokens = r?.tokens || [];
    if (!tokens.length) { grid.innerHTML = '<div class="hint">No tokens created yet.</div>'; return; }
    let h = '<table class="data-table"><thead><tr><th>ID</th><th>Role</th><th>Label</th><th>Created</th><th>Expires</th><th></th></tr></thead><tbody>';
    tokens.forEach(t => {
      const created = t.created_at_ms ? new Date(t.created_at_ms).toLocaleString() : '-';
      const expires = t.expires_at_ms ? new Date(t.expires_at_ms).toLocaleString() : 'never';
      h += '<tr><td style="font-family:monospace;font-size:11px">' + escapeHtml(t.token_id) + '</td><td>' + escapeHtml(t.role) + '</td><td>' + escapeHtml(t.label || '-') + '</td><td>' + created + '</td><td>' + expires + '</td>';
      h += '<td><button class="danger sm" onclick="securityRevokeToken(\'' + escapeHtml(t.token_id) + '\')">Revoke</button></td></tr>';
    });
    h += '</tbody></table>';
    grid.innerHTML = h;
  } catch (e) { setOut({ error: String(e) }, 'secTokenOut'); }
}

async function securityRevokeToken(tokenId) {
  const ok = await skeinModal('\uD83D\uDD12', 'Revoke Token', 'Permanently revoke token <b>' + escapeHtml(tokenId) + '</b>?', [
    { label: 'Cancel', cls: 'secondary' },
    { label: 'Revoke', cls: 'danger', value: true }
  ]);
  if (!ok) return;
  try {
    await call('security.token.revoke', { token_id: tokenId }, 'secTokenOut');
    await securityRefreshTokens();
  } catch (e) { setOut({ error: String(e) }, 'secTokenOut'); }
}

// ---------------------------------------------------------------------------
// Top Queries by fingerprint (T214)
// ---------------------------------------------------------------------------
async function securityTopQueries() {
  try {
    const r = await call('stats.top_queries', { limit: 20 }, null);
    const grid = $('secTopQueryGrid'); if (!grid) return;
    const queries = r?.queries || [];
    if (!queries.length) { grid.innerHTML = '<div class="hint">No query statistics available yet.</div>'; return; }
    let h = '<table class="data-table"><thead><tr><th>#</th><th>Fingerprint</th><th>Count</th><th>Avg (ms)</th><th>Last Seen</th></tr></thead><tbody>';
    queries.forEach((q, i) => {
      const fp = q.fingerprint || q.sql || '-';
      const count = q.count || q.exec_count || 0;
      const avg = typeof q.avg_ms === 'number' ? q.avg_ms.toFixed(1) : (typeof q.total_ms === 'number' && count ? (q.total_ms / count).toFixed(1) : '-');
      const last = q.last_seen_ms ? new Date(q.last_seen_ms).toLocaleString() : '-';
      h += '<tr><td>' + (i + 1) + '</td><td style="font-family:monospace;font-size:11px;max-width:400px;overflow:hidden;text-overflow:ellipsis">' + escapeHtml(fp) + '</td><td>' + count + '</td><td>' + avg + '</td><td>' + last + '</td></tr>';
    });
    h += '</tbody></table>';
    grid.innerHTML = h;
  } catch (e) {
    const grid = $('secTopQueryGrid');
    if (grid) grid.innerHTML = '<div class="hint">Query stats not available.</div>';
  }
}

// ---------------------------------------------------------------------------
// CDC (Phase 23)
// ---------------------------------------------------------------------------
function formatUiTimestamp(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return '--';
  return new Date(ms).toLocaleString();
}

function cdcHydrateDefaultsFromContext() {
  const dbInput = $('cdcDb');
  const tableInput = $('cdcTable');
  if (dbInput && !dbInput.value.trim() && STATE.selectedDb) dbInput.value = STATE.selectedDb;
  if (tableInput && !tableInput.value.trim() && STATE.selectedTable) tableInput.value = STATE.selectedTable;
}

function cdcFindSubscription(subId) {
  return (STATE.cdcSubscriptions || []).find((sub) => sub.sub_id === subId) || null;
}

function cdcLagForSub(sub) {
  return Math.max(0, (Number(sub?.next_offset) || 0) - (Number(sub?.acked_offset) || 0));
}

function formatCdcPk(pk) {
  if (!Array.isArray(pk) || !pk.length) return '--';
  return pk.map((part) => formatLit(part)).join(', ');
}

function renderCdcLagSummary(subs, selected) {
  const el = $('cdcLagSummary');
  if (!el) return;
  if (!subs.length) {
    el.innerHTML = 'No CDC subscriptions in this browser session yet.';
    return;
  }
  const totalLag = subs.reduce((sum, sub) => sum + cdcLagForSub(sub), 0);
  const maxLag = subs.reduce((value, sub) => Math.max(value, cdcLagForSub(sub)), 0);
  const selectedLabel = selected ? (selected.db + '.' + selected.table + ' (' + selected.sub_id + ')') : 'none';
  el.innerHTML = '<strong>Subscriptions</strong>: ' + escapeHtml(String(subs.length))
    + ' | <strong>Total lag</strong>: ' + escapeHtml(String(totalLag)) + ' event(s)'
    + ' | <strong>Max lag</strong>: ' + escapeHtml(String(maxLag))
    + ' | <strong>Selected</strong>: ' + escapeHtml(selectedLabel);
}

function renderCdcSubscriptionTable(subs) {
  const host = $('cdcSubGrid');
  if (!host) return;
  if (!subs.length) {
    host.innerHTML = '<div class="hint" style="padding:10px">Create a subscription to start tracking CDC lag.</div>';
    return;
  }
  const maxLag = Math.max(1, ...subs.map((sub) => cdcLagForSub(sub)));
  let h = '<table class="data-table"><thead><tr><th>Subscription</th><th>Table</th><th>Start</th><th>Acked</th><th>Next</th><th>Lag</th><th>Last Poll</th></tr></thead><tbody>';
  subs.forEach((sub) => {
    const lag = cdcLagForSub(sub);
    const barWidth = lag <= 0 ? 0 : Math.max(8, Math.round((lag / maxLag) * 100));
    const lagBar = lag <= 0
      ? '<span class="hint">caught up</span>'
      : '<div class="lag-bar"><div class="lag-bar-fill" style="width:' + barWidth + '%"></div></div>';
    h += '<tr' + (sub.sub_id === STATE.cdcSelectedSubId ? ' class="row-selected"' : '') + '>'
      + '<td><strong>' + escapeHtml(sub.sub_id) + '</strong></td>'
      + '<td>' + escapeHtml(sub.db + '.' + sub.table) + '</td>'
      + '<td>' + escapeHtml(String(Number(sub.offset) || 0)) + '</td>'
      + '<td>' + escapeHtml(String(Number(sub.acked_offset) || 0)) + '</td>'
      + '<td>' + escapeHtml(String(Number(sub.next_offset) || 0)) + '</td>'
      + '<td><div style="min-width:120px">' + lagBar + '<div class="hint" style="margin-top:4px">' + escapeHtml(String(lag)) + ' event(s)</div></div></td>'
      + '<td>' + escapeHtml(formatUiTimestamp(Number(sub.last_polled_at_ms) || 0)) + '</td>'
      + '</tr>';
  });
  h += '</tbody></table>';
  host.innerHTML = h;
}

function renderCdcEvents(selected) {
  if (!selected || !Array.isArray(selected.last_events) || !selected.last_events.length) {
    renderTable('cdcEventGrid', ['Seq', 'DB', 'Table', 'Op', 'PK'], [['--', 'No polled events yet', '', '', '']]);
    return;
  }
  renderTable(
    'cdcEventGrid',
    ['Seq', 'DB', 'Table', 'Op', 'PK'],
    selected.last_events.map((event) => [
      event.seq,
      event.db || '',
      event.table || '',
      event.op || '',
      formatCdcPk(event.pk),
    ]),
  );
}

function renderCdcPanel() {
  cdcHydrateDefaultsFromContext();
  const subs = Array.isArray(STATE.cdcSubscriptions)
    ? STATE.cdcSubscriptions.slice().sort((a, b) => (b.created_at_ms || 0) - (a.created_at_ms || 0))
    : [];
  if (STATE.cdcSelectedSubId && !subs.some((sub) => sub.sub_id === STATE.cdcSelectedSubId)) {
    STATE.cdcSelectedSubId = '';
  }
  if (!STATE.cdcSelectedSubId && subs.length) {
    STATE.cdcSelectedSubId = subs[0].sub_id;
  }
  const select = $('cdcSubId');
  if (select) {
    select.innerHTML = subs.length
      ? subs.map((sub) => '<option value="' + escapeHtml(sub.sub_id) + '">' + escapeHtml(sub.sub_id + ' · ' + sub.db + '.' + sub.table) + '</option>').join('')
      : '<option value="">No subscriptions</option>';
    select.disabled = !subs.length;
    select.value = STATE.cdcSelectedSubId || '';
  }
  const selected = cdcFindSubscription(STATE.cdcSelectedSubId);
  const dbInput = $('cdcDb');
  const tableInput = $('cdcTable');
  const fromInput = $('cdcFromOffset');
  const ackInput = $('cdcAckOffset');
  if (selected) {
    if (dbInput && document.activeElement !== dbInput) dbInput.value = selected.db;
    if (tableInput && document.activeElement !== tableInput) tableInput.value = selected.table;
    if (fromInput && document.activeElement !== fromInput) fromInput.value = String(Number(selected.next_offset) || Number(selected.offset) || 0);
    if (ackInput && document.activeElement !== ackInput) ackInput.value = String(Math.max(Number(selected.next_offset) || 0, Number(selected.acked_offset) || 0));
  }
  renderCdcLagSummary(subs, selected);
  renderCdcSubscriptionTable(subs);
  renderCdcEvents(selected);
}

async function cdcSubscribe() {
  try {
    cdcHydrateDefaultsFromContext();
    const db = ($('cdcDb')?.value || '').trim();
    const table = ($('cdcTable')?.value || '').trim();
    if (!db || !table) throw new Error('Database and table are required');
    const res = await call('cdc.subscribe_table', { db, table }, 'cdcOut');
    const result = unwrapRpcResult(res, 'cdc.subscribe_table');
    const offset = Number(result.offset) || 0;
    STATE.cdcSubscriptions = (STATE.cdcSubscriptions || []).filter((sub) => sub.sub_id !== result.sub_id);
    STATE.cdcSubscriptions.unshift({
      sub_id: result.sub_id,
      db,
      table,
      offset,
      acked_offset: offset,
      next_offset: offset,
      created_at_ms: Date.now(),
      last_polled_at_ms: 0,
      last_events: [],
    });
    STATE.cdcSelectedSubId = result.sub_id;
    setOut({ subscription: { sub_id: result.sub_id, db, table, offset } }, 'cdcOut');
    renderCdcPanel();
    showToast('CDC subscription created for ' + db + '.' + table + '.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'cdcOut');
  }
}

async function cdcPoll() {
  try {
    const selected = cdcFindSubscription(STATE.cdcSelectedSubId || $('cdcSubId')?.value || '');
    if (!selected) throw new Error('Select a subscription first');
    const fromRaw = parseInt($('cdcFromOffset')?.value || '', 10);
    const limitRaw = parseInt($('cdcLimit')?.value || '', 10);
    const fromOffset = Number.isFinite(fromRaw)
      ? Math.max(fromRaw, 0)
      : (Number(selected.next_offset) || Number(selected.acked_offset) || Number(selected.offset) || 0);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 200;
    const res = await call('cdc.poll', { sub_id: selected.sub_id, from_offset: fromOffset, limit }, 'cdcOut');
    const result = unwrapRpcResult(res, 'cdc.poll');
    selected.last_events = Array.isArray(result.events) ? result.events : [];
    selected.last_polled_at_ms = Date.now();
    selected.next_offset = Number(result.next_offset) || fromOffset;
    setOut({ subscription: { sub_id: selected.sub_id, db: selected.db, table: selected.table }, poll: result }, 'cdcOut');
    renderCdcPanel();
    showToast('CDC poll returned ' + String(selected.last_events.length) + ' event(s).', selected.last_events.length ? 'success' : 'info');
  } catch (e) {
    setOut({ error: String(e) }, 'cdcOut');
  }
}

async function cdcAck() {
  try {
    const selected = cdcFindSubscription(STATE.cdcSelectedSubId || $('cdcSubId')?.value || '');
    if (!selected) throw new Error('Select a subscription first');
    const ackRaw = parseInt($('cdcAckOffset')?.value || '', 10);
    const offset = Number.isFinite(ackRaw) ? Math.max(ackRaw, 0) : (Number(selected.next_offset) || Number(selected.acked_offset) || 0);
    const res = await call('cdc.ack', { sub_id: selected.sub_id, offset }, 'cdcOut');
    const result = unwrapRpcResult(res, 'cdc.ack');
    selected.acked_offset = Number(result.acked_offset) || selected.acked_offset;
    setOut({ subscription: { sub_id: selected.sub_id, db: selected.db, table: selected.table }, ack: result }, 'cdcOut');
    renderCdcPanel();
    showToast('CDC subscription acked through offset ' + String(selected.acked_offset) + '.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'cdcOut');
  }
}

async function cdcClose() {
  try {
    const selected = cdcFindSubscription(STATE.cdcSelectedSubId || $('cdcSubId')?.value || '');
    if (!selected) throw new Error('Select a subscription first');
    const ok = await skeinModal('🛰️', 'Close CDC Subscription', 'Close <b>' + escapeHtml(selected.sub_id) + '</b> for <b>' + escapeHtml(selected.db + '.' + selected.table) + '</b>?', [
      { label: 'Cancel', value: false },
      { label: 'Close', value: true, cls: 'primary' },
    ]);
    if (!ok) return;
    const res = await call('cdc.close', { sub_id: selected.sub_id }, 'cdcOut');
    const result = unwrapRpcResult(res, 'cdc.close');
    STATE.cdcSubscriptions = (STATE.cdcSubscriptions || []).filter((sub) => sub.sub_id !== selected.sub_id);
    if (STATE.cdcSelectedSubId === selected.sub_id) {
      STATE.cdcSelectedSubId = STATE.cdcSubscriptions[0]?.sub_id || '';
    }
    setOut({ close: result }, 'cdcOut');
    renderCdcPanel();
    showToast('CDC subscription ' + selected.sub_id + ' closed.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'cdcOut');
  }
}

function renderResearchSettings(config = {}) {
  const grid = $('researchSettingsGrid'); if (!grid) return;
  grid.textContent = '';
  RESEARCH_TRACKS.forEach(track => {
    const card = document.createElement('div'); card.className = 'feature-card';
    card.dataset.track = track.id;
    const statusBadge = track.status === 'hardened' ? ' <span class="tag secondary" style="font-size:8px">hardened</span>' : ' <span class="tag" style="font-size:8px">prototype</span>';
    card.innerHTML = '<div class="feature-title">' + escapeHtml(track.id) + statusBadge + '</div><div class="hint">' + escapeHtml(track.title) + '</div><div class="hint">Methods: ' + escapeHtml(track.methods.join(', ')) + '</div>';
    const toggle = document.createElement('label'); toggle.style.cssText = 'display:flex;gap:4px;align-items:center;font-size:11px;cursor:pointer;';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = (config[track.id]?.enabled ?? true) !== false;
    cb.dataset.track = track.id;
    toggle.appendChild(cb); toggle.appendChild(document.createTextNode('Enabled'));
    card.appendChild(toggle);
    const text = document.createElement('textarea');
    text.dataset.role = 'config';
    const extra = { ...(config[track.id] || {}) };
    delete extra.enabled;
    text.placeholder = '{"note":"optional per-track config"}';
    text.value = Object.keys(extra).length ? JSON.stringify(extra, null, 2) : '';
    card.appendChild(text);
    if (track.panel) {
      const btn = document.createElement('button');
      btn.className = 'sm ghost';
      btn.textContent = 'Open Panel';
      btn.addEventListener('click', () => setActivePanel(track.panel, true));
      card.appendChild(btn);
    }
    grid.appendChild(card);
  });
}

async function researchSettingsLoad() {
  try {
    const res = await call('settings.get', { keys: ['research.config'] }, 'researchSettingsOut');
    const config = res?.json?.ok ? (res.json.result?.['research.config'] || {}) : {};
    renderResearchSettings(config);
    setOut({ loaded: true, config }, 'researchSettingsOut');
  } catch (e) { setOut({error:String(e)}, 'researchSettingsOut'); }
}

async function researchSettingsSave() {
  try {
    const grid = $('researchSettingsGrid'); if (!grid) return;
    const config = {};
    grid.querySelectorAll('.feature-card').forEach((card) => {
      const track = card.dataset.track;
      if (!track) return;
      const cb = card.querySelector('input[type="checkbox"]');
      const raw = card.querySelector('textarea[data-role="config"]')?.value.trim() || '';
      const extra = raw ? parseJsonInput(raw, track + ' config') : {};
      config[track] = { ...(extra || {}), enabled: !!cb?.checked };
    });
    await call('settings.set', { 'research.config': config }, 'researchSettingsOut');
    setOut({ saved: true, config }, 'researchSettingsOut');
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
  try { const t = readDbTable('vecDb','vecTable'); await call('vector.index.status',{table:t},'vecOut'); } catch (e) { setOut({error:String(e)},'vecOut'); }
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
  try { const t = readDbTable('dpDb','dpTable'); await call('dp.audit.log',{table:t},'dpOut'); } catch (e) { setOut({error:String(e)},'dpOut'); }
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
function formatAuditTimestamp(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return 'never';
  return new Date(ms).toLocaleString();
}

function renderForAuditSummary(status, verify) {
  const el = $('forAuditSummary');
  if (!el) return;
  const chainLength = Number(status?.chain_length) || 0;
  const anchorCount = Number(status?.anchor_count) || 0;
  const headHash = status?.chain_head_hash || 'genesis';
  const lastVerified = formatAuditTimestamp(Number(status?.last_verified_ms) || 0);
  const lastAnchor = status?.last_anchor && typeof status.last_anchor === 'object'
    ? status.last_anchor
    : null;
  const anchorSummary = lastAnchor && lastAnchor.checkpoint_id
    ? lastAnchor.checkpoint_id + ' @ ' + formatAuditTimestamp(Number(lastAnchor.ts_ms) || 0)
    : 'none';
  let verifySummary = 'Verification not run in this session.';
  if (verify && typeof verify === 'object') {
    if (verify.ok) {
      verifySummary = 'OK: ' + String(Number(verify.records_checked) || 0) + ' record(s) checked in ' + String(Number(verify.elapsed_ms) || 0) + ' ms.';
    } else {
      const reason = verify.reason || 'unknown';
      const badIndex = verify.bad_index === undefined || verify.bad_index === null ? 'n/a' : String(verify.bad_index);
      verifySummary = 'FAILED: reason=' + reason + ', bad_index=' + badIndex + '.';
    }
  }
  el.innerHTML = '<strong>Chain</strong>: ' + escapeHtml(String(chainLength))
    + ' record(s) | <strong>Anchors</strong>: ' + escapeHtml(String(anchorCount))
    + ' | <strong>Last verified</strong>: ' + escapeHtml(lastVerified)
    + '<br><strong>Head</strong>: ' + escapeHtml(headHash)
    + '<br><strong>Last anchor</strong>: ' + escapeHtml(anchorSummary)
    + '<br><strong>Verification</strong>: ' + escapeHtml(verifySummary);
}

async function forAuditStatus() {
  try {
    const res = await call('maintenance.audit_status', {}, 'forAuditOut');
    const result = unwrapRpcResult(res, 'maintenance.audit_status');
    renderForAuditSummary(result, null);
  } catch (e) { setOut({error:String(e)}, 'forAuditOut'); }
}

async function forAuditVerify() {
  try {
    const verifyRes = await call('maintenance.audit_verify', {}, 'forAuditOut');
    const verify = unwrapRpcResult(verifyRes, 'maintenance.audit_verify');
    const statusRes = await call('maintenance.audit_status', {}, 'forAuditOut');
    const status = unwrapRpcResult(statusRes, 'maintenance.audit_status');
    renderForAuditSummary(status, verify);
    setOut({ status, verify }, 'forAuditOut');
  } catch (e) { setOut({error:String(e)}, 'forAuditOut'); }
}

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
  try {
    const table = readDbTable('advDb', 'advTable');
    const res = await call('advisor.index_synthesize', { table }, 'advOut');
    const result = unwrapRpcResult(res, 'advisor.index_synthesize');
    STATE.advisorSuggestions = Array.isArray(result.suggestions) ? result.suggestions : [];
    if (STATE.advisorSuggestions.length) setAdvisorSelection(STATE.advisorSuggestions[0], { tableRef: table });
    else setAdvisorSelection(null);
    renderAdvisorReport();
    showToast('Index suggestions refreshed.', 'success');
  } catch (e) { setOut({error:String(e)},'advOut'); }
}

async function advHistory() {
  try {
    const db = $('advDb')?.value.trim();
    const table = $('advTable')?.value.trim();
    const params = db && table ? { table: tableRef(db, table), limit: 20 } : { limit: 20 };
    const res = await call('advisor.history', params, 'advOut');
    const result = unwrapRpcResult(res, 'advisor.history');
    STATE.advisorHistory = Array.isArray(result.entries) ? result.entries : [];
    if (!STATE.advisorSelection && STATE.advisorHistory.length) {
      const first = STATE.advisorHistory[0];
      setAdvisorSelection(first, { tableRef: first.table || null });
    }
    renderAdvisorReport();
    showToast('Advisor history loaded.', 'info');
  } catch (e) { setOut({error:String(e)},'advOut'); }
}

async function advApply() {
  try {
    const selection = STATE.advisorSelection;
    if (!selection) throw new Error('Select a suggestion first');
    const res = await call('advisor.apply_index', cleanParams({
      table: selection.table,
      columns: selection.columns,
      include: selection.include,
      note: $('advNote')?.value.trim() || undefined
    }), 'advOut');
    const result = unwrapRpcResult(res, 'advisor.apply_index');
    STATE.advisorHistory.unshift({
      id: result.action_id,
      suggestion_id: selection.id || advisorSelectionKey(selection.columns, selection.include),
      table: selection.table,
      columns: [...selection.columns],
      include: [...selection.include],
      action: 'apply',
      created_at_ms: Date.now(),
      note: $('advNote')?.value.trim() || null
    });
    const selectionKey = advisorSelectionKey(selection.columns, selection.include);
    STATE.advisorSuggestions = STATE.advisorSuggestions.filter((item) => advisorSelectionKey(item.columns, item.include) !== selectionKey);
    renderAdvisorReport();
    showToast('Advisor suggestion applied (' + (result.status || 'ok') + ').', 'success');
  } catch (e) { setOut({error:String(e)},'advOut'); }
}

async function advDismiss() {
  try {
    const selection = STATE.advisorSelection;
    if (!selection) throw new Error('Select a suggestion first');
    const res = await call('advisor.dismiss', cleanParams({
      table: selection.table,
      columns: selection.columns,
      include: selection.include,
      note: $('advNote')?.value.trim() || undefined
    }), 'advOut');
    const result = unwrapRpcResult(res, 'advisor.dismiss');
    STATE.advisorHistory.unshift({
      id: result.action_id,
      suggestion_id: selection.id || advisorSelectionKey(selection.columns, selection.include),
      table: selection.table,
      columns: [...selection.columns],
      include: [...selection.include],
      action: 'dismiss',
      created_at_ms: Date.now(),
      note: $('advNote')?.value.trim() || null
    });
    const selectionKey = advisorSelectionKey(selection.columns, selection.include);
    STATE.advisorSuggestions = STATE.advisorSuggestions.filter((item) => advisorSelectionKey(item.columns, item.include) !== selectionKey);
    renderAdvisorReport();
    showToast('Advisor suggestion dismissed.', 'info');
  } catch (e) { setOut({error:String(e)},'advOut'); }
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
  try { const sql = $('autoparamSql')?.value.trim(); if (!sql) throw new Error('SQL required'); await call('ai.autoparam.analyze',{sql},'autoparamOut'); } catch (e) { setOut({error:String(e)},'autoparamOut'); }
}

async function autoparamClassify() {
  try { const sql = $('autoparamSql')?.value.trim(); if (!sql) throw new Error('SQL required'); await call('ai.autoparam.classify',{sql},'autoparamOut'); } catch (e) { setOut({error:String(e)},'autoparamOut'); }
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
  if (panel === 'cdc') renderCdcPanel();
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
// Dark mode
// ---------------------------------------------------------------------------
function initDarkMode() {
  const saved = localStorage.getItem('skeinadmin.theme');
  if (saved === 'dark' || (!saved && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
    document.documentElement.classList.add('dark');
  }
  updateThemeToggle();
}

function toggleDarkMode() {
  document.documentElement.classList.toggle('dark');
  const isDark = document.documentElement.classList.contains('dark');
  localStorage.setItem('skeinadmin.theme', isDark ? 'dark' : 'light');
  updateThemeToggle();
}

function updateThemeToggle() {
  const btn = $('themeToggle');
  if (!btn) return;
  const isDark = document.documentElement.classList.contains('dark');
  btn.innerHTML = '<span class="theme-toggle-icon">' + (isDark ? '☀️' : '🌙') + '</span> ' + (isDark ? 'Light mode' : 'Dark mode');
}

// ---------------------------------------------------------------------------
// Collapsible sidebar groups
// ---------------------------------------------------------------------------
function initCollapsibleGroups() {
  document.querySelectorAll('.nav-group-title').forEach(title => {
    const savedState = localStorage.getItem('skeinadmin.navgroup.' + title.textContent.trim());
    if (savedState === 'collapsed') title.classList.add('collapsed');
    title.addEventListener('click', () => {
      title.classList.toggle('collapsed');
      localStorage.setItem('skeinadmin.navgroup.' + title.textContent.trim(), title.classList.contains('collapsed') ? 'collapsed' : 'expanded');
    });
  });
}

// ---------------------------------------------------------------------------
// Command palette
// ---------------------------------------------------------------------------
const CMD_ITEMS = [];

function initCommandPalette() {
  // Build command list from panels
  Object.entries(PANEL_META).forEach(([panel, meta]) => {
    CMD_ITEMS.push({ label: meta.title, hint: meta.subtitle, action: () => setActivePanel(panel, true) });
  });
  // Add common actions
  CMD_ITEMS.push({ label: 'Connect to server', hint: 'Establish RPC connection', action: connect });
  CMD_ITEMS.push({ label: 'Disconnect', hint: 'Close connection', action: disconnect });
  CMD_ITEMS.push({ label: 'Run SQL', hint: 'Execute current SQL query', action: () => runSql(false) });
  CMD_ITEMS.push({ label: 'Reload database tree', hint: 'Refresh databases & tables', action: loadDbTree });
  CMD_ITEMS.push({ label: 'Toggle dark mode', hint: 'Switch light/dark theme', action: toggleDarkMode });
  CMD_ITEMS.push({ label: 'Refresh stats', hint: 'Reload server statistics', action: loadStats });
  CMD_ITEMS.push({ label: 'New database', hint: 'Create a new database', action: quickCreateDb });
  CMD_ITEMS.push({ label: 'New table', hint: 'Create a new table', action: quickCreateTable });
  CMD_ITEMS.push({ label: 'Insert row', hint: 'Insert a new row', action: quickInsertRow });
  CMD_ITEMS.push({ label: 'Browse table', hint: 'View table data', action: quickBrowseData });
  // Add RPC templates
  RPC_TEMPLATES.forEach(tpl => {
    CMD_ITEMS.push({ label: 'RPC: ' + tpl.label, hint: 'Send ' + tpl.method, action: () => {
      setActivePanel('rpc', true);
      if ($('rpcMethod')) $('rpcMethod').value = tpl.method;
      if ($('rpcParams')) $('rpcParams').value = JSON.stringify(tpl.params, null, 2);
    }});
  });
}

let cmdSelectedIndex = 0;

function openCommandPalette() {
  const overlay = $('cmdPalette');
  if (!overlay) return;
  overlay.classList.add('open');
  const input = $('cmdInput');
  if (input) { input.value = ''; input.focus(); }
  cmdSelectedIndex = 0;
  renderCommandResults('');
}

function closeCommandPalette() {
  const overlay = $('cmdPalette');
  if (overlay) overlay.classList.remove('open');
}

function renderCommandResults(filter) {
  const container = $('cmdResults');
  if (!container) return;
  container.innerHTML = '';
  const lf = filter.toLowerCase();
  const matches = CMD_ITEMS.filter(item => !lf || item.label.toLowerCase().includes(lf) || (item.hint && item.hint.toLowerCase().includes(lf)));
  const shown = matches.slice(0, 12);
  cmdSelectedIndex = Math.min(cmdSelectedIndex, shown.length - 1);
  if (cmdSelectedIndex < 0) cmdSelectedIndex = 0;
  shown.forEach((item, i) => {
    const div = document.createElement('div');
    div.className = 'cmd-palette-item' + (i === cmdSelectedIndex ? ' selected' : '');
    div.innerHTML = '<span class="cmd-item-label">' + escapeHtml(item.label) + '</span>' + (item.hint ? '<span class="cmd-item-hint">' + escapeHtml(item.hint) + '</span>' : '');
    div.addEventListener('click', () => { closeCommandPalette(); item.action(); });
    container.appendChild(div);
  });
  if (!shown.length) {
    container.innerHTML = '<div style="padding:14px;color:var(--muted);font-size:12px">No matching commands.</div>';
  }
}

function executeSelectedCommand(filter) {
  const lf = (filter || '').toLowerCase();
  const matches = CMD_ITEMS.filter(item => !lf || item.label.toLowerCase().includes(lf) || (item.hint && item.hint.toLowerCase().includes(lf)));
  if (matches[cmdSelectedIndex]) {
    closeCommandPalette();
    matches[cmdSelectedIndex].action();
  }
}

function initCommandPaletteEvents() {
  const input = $('cmdInput');
  if (input) {
    input.addEventListener('input', () => { cmdSelectedIndex = 0; renderCommandResults(input.value); });
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') { e.preventDefault(); closeCommandPalette(); }
      else if (e.key === 'ArrowDown') { e.preventDefault(); cmdSelectedIndex++; renderCommandResults(input.value); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); cmdSelectedIndex = Math.max(0, cmdSelectedIndex - 1); renderCommandResults(input.value); }
      else if (e.key === 'Enter') { e.preventDefault(); executeSelectedCommand(input.value); }
    });
  }
  const overlay = $('cmdPalette');
  if (overlay) overlay.addEventListener('click', (e) => { if (e.target === overlay) closeCommandPalette(); });
}

// ---------------------------------------------------------------------------
// Keyboard shortcuts
// ---------------------------------------------------------------------------
function initKeyboardShortcuts() {
  document.addEventListener('keydown', (e) => {
    // Cmd/Ctrl+K = Command palette
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      e.preventDefault();
      openCommandPalette();
    }
    // Cmd/Ctrl+Enter = Run SQL (when in SQL workspace)
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      const sqlEl = $('sqlText');
      if (sqlEl && document.activeElement === sqlEl) {
        e.preventDefault();
        runSql(false);
      }
    }
    // Escape = close command palette
    if (e.key === 'Escape') {
      closeCommandPalette();
    }
  });
}

// ---------------------------------------------------------------------------
// SQL history
// ---------------------------------------------------------------------------
function addSqlHistory(sql) {
  if (!sql || !sql.trim()) return;
  STATE.sqlHistory = STATE.sqlHistory.filter(s => s !== sql);
  STATE.sqlHistory.unshift(sql);
  if (STATE.sqlHistory.length > 20) STATE.sqlHistory.pop();
  try { localStorage.setItem('skeinadmin.sqlHistory', JSON.stringify(STATE.sqlHistory)); } catch {}
  renderSqlHistory();
}

function loadSqlHistory() {
  try { STATE.sqlHistory = JSON.parse(localStorage.getItem('skeinadmin.sqlHistory') || '[]'); } catch { STATE.sqlHistory = []; }
  renderSqlHistory();
}

function renderSqlHistory() {
  const container = $('sqlHistory');
  if (!container) return;
  container.innerHTML = '';
  if (!STATE.sqlHistory.length) { container.textContent = 'No queries yet.'; return; }
  STATE.sqlHistory.forEach(sql => {
    const item = document.createElement('div');
    item.className = 'sql-history-item';
    item.textContent = sql;
    item.title = sql;
    item.addEventListener('click', () => { if ($('sqlText')) $('sqlText').value = sql; });
    container.appendChild(item);
  });
}

// ---------------------------------------------------------------------------
// SQL Templates
// ---------------------------------------------------------------------------
function sqlTemplateSelect() {
  if (STATE.selectedDb && STATE.selectedTable) setSqlText('SELECT * FROM '+STATE.selectedDb+'.'+STATE.selectedTable+' LIMIT 50;');
  else if (STATE.selectedDb) setSqlText('SELECT * FROM '+STATE.selectedDb+'.table_name LIMIT 50;');
  else setSqlText('SELECT 1 AS healthcheck;');
}

function quickCreateDb() {
  setActivePanel('easy', true);
  easySetSubTab('create');
  if ($('easyCreateDb')) $('easyCreateDb').focus();
}

function quickCreateTable() {
  setActivePanel('easy', true);
  easySetSubTab('create');
  if (!STATE.easyTableBuilderRows.length) easySeedColumns();
  if ($('easyCreateTableName')) $('easyCreateTableName').focus();
}

function quickInsertRow() {
  setActivePanel('easy', true);
  easySetSubTab('insert');
}

function quickBrowseData() {
  setActivePanel('easy', true);
  easySetSubTab('browse');
  easyBrowseRows();
}

function quickOpenCluster() {
  setActivePanel('cluster', true);
}

// ---------------------------------------------------------------------------
// Wire all buttons
// ---------------------------------------------------------------------------
function wire(id, fn) { const el = $(id); if (el) el.addEventListener('click', fn); }

// Connect
wire('btnConnect', connect);
wire('btnDisconnect', disconnect);
wire('btnPing', ping);
wire('btnVersion', loadVersion);
wire('btnStats', loadStats);
wire('btnCapabilities', loadCapabilities);
wire('btnTransport', loadTransport);
wire('btnShutdown', shutdownServer);

// Overview quick actions & stats
wire('btnQuickCreateDb', quickCreateDb);
wire('btnQuickCreateTable', quickCreateTable);
wire('btnQuickInsertRow', quickInsertRow);
wire('btnQuickBrowseData', quickBrowseData);
wire('btnQuickCluster', quickOpenCluster);
wire('btnRefreshStats', loadStats);
wire('btnAutoRefreshStats', toggleAutoRefresh);

// Engine config
wire('btnEngineLoad', engineLoadConfig);
wire('btnEngineSave', engineSaveConfig);
wire('btnEngineReset', engineResetDefaults);

// Easy viewer
wire('themeToggle', toggleDarkMode);
wire('btnEasyReloadTree', async () => { await loadDbTree(); easyRefreshTargetsFromTree(); });
wire('easyBtnNewDb', () => easyToggleNewDbForm(true));
wire('easyBtnCreateDbInline', easyCreateDbInline);
wire('easyBtnCancelDbInline', () => easyToggleNewDbForm(false));
wire('easyBtnInsertFromBrowse', () => { easySetSubTab('insert'); easyRenderInsertForm(); });
wire('easyBtnRefreshBrowse', () => { STATE.easyBrowseOffset = 0; easyBrowseRows(); });
wire('easyPgPrev', easyBrowsePrev);
wire('easyPgNext', easyBrowseNext);
wire('easyCheckAll', easyToggleCheckAll);
wire('easyBtnDeleteChecked', easyDeleteCheckedRows);
wire('easyBtnDoInsert', () => easyDoInsert(false));
wire('easyBtnInsertAnother', () => easyDoInsert(true));
wire('easyBtnClearInsert', easyClearInsertForm);
wire('easyBtnDoSearch', easyDoSearch);
wire('easyBtnClearSearch', () => { if ($('easySearchValue')) $('easySearchValue').value = ''; renderTable('easySearchGrid', [], []); if ($('easySearchInfo')) $('easySearchInfo').textContent = ''; });
wire('easyBtnAddCol', easyAddColumn);
wire('easyBtnSeedCols', easySeedColumns);
wire('easyBtnDoCreateTable', easyDoCreateTable);
wire('easyBtnDoCreateDb', easyDoCreateDb);
wire('easyBtnExportData', easyDoExport);
wire('easyBtnExportStruct', easyDoExportStruct);
wire('easyBtnTruncate', easyTruncateTable);
wire('easyBtnDropTable', easyDropTableOp);
wire('easyBtnDropDb', easyDropDbOp);
wire('easyBtnRunSql', easyRunSql);
wire('easyBtnClearSql', () => { if ($('easySqlText')) $('easySqlText').value = ''; renderTable('easySqlGrid', [], []); setOut('', 'easySqlOut'); });

// Query Builder
wire('qbBtnAddCondition', qbAddCondition);
wire('qbBtnClearConditions', qbClearConditions);
wire('qbBtnExecute', qbExecute);
wire('qbBtnCopySQL', qbCopySQL);
wire('qbBtnSendToSQL', qbSendToSQL);
if ($('qbOrderCol')) $('qbOrderCol').addEventListener('change', () => qbUpdatePreview());
if ($('qbOrderDir')) $('qbOrderDir').addEventListener('change', () => qbUpdatePreview());
if ($('qbLimit')) $('qbLimit').addEventListener('change', () => qbUpdatePreview());

// Dashboard cards
wire('btnRefreshTopTables', refreshTopTables);
wire('btnRefreshSlowQueries', refreshSlowQueries);
wire('btnRefreshSessions', refreshActiveSessions);
wire('btnRefreshIndexHealth', refreshIndexHealth);
if ($('easySqlText')) $('easySqlText').addEventListener('keydown', e => { if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); easyRunSql(); } });
if ($('easyTreeFilter')) $('easyTreeFilter').addEventListener('input', () => easyRenderTree());
if ($('easyPerPage')) $('easyPerPage').addEventListener('change', () => { STATE.easyBrowseOffset = 0; easyBrowseRows(); });
if ($('easyQuickFilter')) $('easyQuickFilter').addEventListener('input', e => { STATE.easyBrowseFilter = e.target.value; easyRenderDataGrid(); });
if ($('easyCreateDb')) $('easyCreateDb').addEventListener('input', easyUpdateCreatePreview);
if ($('easyCreateTableName')) $('easyCreateTableName').addEventListener('input', easyUpdateCreatePreview);
document.querySelectorAll('.easy-tab').forEach(btn => {
  btn.addEventListener('click', () => easySetSubTab(btn.dataset.etab));
});

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
wire('btnSchemaBuilderSeed', schemaBuilderSeedDefaults);
wire('btnSchemaBuilderAddCol', () => schemaBuilderAddColumn());
wire('btnSchemaBuilderLoad', schemaBuilderLoadCurrent);
wire('btnSchemaBuilderSync', () => schemaBuilderSyncToJson(true));
wire('btnSchemaBuilderCreateDb', schemaBuilderCreateDb);
wire('btnSchemaBuilderCreateTable', schemaBuilderCreateTable);

// Data
wire('btnDataFormLoad', dataFormLoadColumns);
wire('btnDataFormInsert', dataFormInsertRow);
wire('btnDataFormGetPk', dataFormGetByPk);
wire('btnDataFormDeletePk', dataFormDeleteByPk);
wire('btnDataGet', dataGet);
wire('btnDataInsert', dataInsert);
wire('btnDataUpdate', dataUpdate);
wire('btnDataDelete', dataDelete);
wire('btnBrowse', browseTable);
wire('btnBrowsePrev', browsePrev);
wire('btnBrowseNext', browseNext);
if ($('dataDb')) $('dataDb').addEventListener('change', () => { if ($('dataFormDb')) $('dataFormDb').value = $('dataDb').value; });
if ($('dataTable')) $('dataTable').addEventListener('change', () => { if ($('dataFormTable')) $('dataFormTable').value = $('dataTable').value; });

// Cluster
wire('btnClusterStatus', clusterReadStatus);
wire('btnClusterNodes', clusterReadNodes);
wire('btnClusterTransport', clusterTransportCapabilities);
wire('btnClusterCreateToken', clusterCreateToken);
wire('btnClusterJoinNode', clusterJoinNode);
wire('btnClusterLeaveNode', clusterLeaveNode);
wire('btnClusterRemoveNode', clusterRemoveNode);
wire('btnClusterPromote', clusterPromoteNode);
wire('btnClusterShardCreate', clusterShardCreate);
wire('btnClusterShardMove', clusterShardMove);
wire('btnClusterShardRebalance', clusterShardRebalance);

// Settings
wire('btnSettingsGet', settingsGetKey);
wire('btnSettingsSet', settingsSetKey);
wire('btnSettingsClusterPreset', () => { if ($('settingsKey')) $('settingsKey').value = 'cluster.state.v1'; settingsGetKey(); });
wire('btnSettingsUsePreset', settingsUsePreset);
wire('btnSettingsListAll', settingsListAll);
wire('btnSettingsCapabilities', settingsLoadCapabilities);
wire('btnSettingsTransport', settingsLoadTransport);
wire('btnSettingsFeatureFlags', settingsLoadFeatureFlags);
wire('btnSettingsWorkloadFeatures', settingsLoadWorkloadFeatures);

// Research settings
wire('btnResearchSettingsLoad', researchSettingsLoad);
wire('btnResearchSettingsSave', researchSettingsSave);

// Users
wire('btnUserCreate', userCreate);
wire('btnUserList', userList);
wire('btnUserDrop', userDrop);
wire('btnUserGrant', userGrant);
wire('btnUserRevoke', userRevoke);

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

// CDC
wire('btnCdcSubscribe', cdcSubscribe);
wire('btnCdcPoll', cdcPoll);
wire('btnCdcAck', cdcAck);
wire('btnCdcClose', cdcClose);
const cdcSubIdSelect = $('cdcSubId');
if (cdcSubIdSelect) cdcSubIdSelect.addEventListener('change', () => { STATE.cdcSelectedSubId = cdcSubIdSelect.value; renderCdcPanel(); });

// Forensics
wire('btnForAuditStatus', forAuditStatus);
wire('btnForAuditVerify', forAuditVerify);
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
if ($('advisorReport')) $('advisorReport').addEventListener('click', (event) => {
  const btn = event.target.closest('[data-adv-source][data-adv-index]');
  if (!btn) return;
  const idx = Number.parseInt(btn.dataset.advIndex || '', 10);
  const source = btn.dataset.advSource;
  if (Number.isNaN(idx)) return;
  if (source === 'suggestion' && STATE.advisorSuggestions[idx]) {
    setAdvisorSelection(STATE.advisorSuggestions[idx], { tableRef: readDbTable('advDb', 'advTable') });
    renderAdvisorReport();
    showToast('Advisor suggestion selected.', 'info');
  }
  if (source === 'history' && STATE.advisorHistory[idx]) {
    const entry = STATE.advisorHistory[idx];
    setAdvisorSelection(entry, { tableRef: entry.table || null });
    renderAdvisorReport();
    showToast('Advisor history entry loaded.', 'info');
  }
});

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

// Security Tokens
wire('btnSecCreateToken', securityCreateToken);
wire('btnSecRefreshTokens', securityRefreshTokens);
wire('btnSecTopQueries', securityTopQueries);

// Telemetry
wire('btnTelemetryCompatSummary', () => call('telemetry.compat_summary', {}, 'telemetryOut'));
wire('btnTelemetryFeatureFlags', () => call('telemetry.feature_flags', {}, 'telemetryOut'));
wire('btnTelemetryMigrationHints', () => call('telemetry.migration_hints', { limit: 20 }, 'telemetryOut'));
wire('btnTelemetryWorkloadFeatures', () => call('telemetry.workload_features', {}, 'telemetryOut'));
wire('btnTelemetryPlanCacheStatus', () => call('plan_cache.status', {}, 'telemetryPlanOut'));
wire('btnTelemetryPlanCacheClear', () => call('plan_cache.clear', {}, 'telemetryPlanOut'));
wire('btnTelemetryTopQueries', () => call('stats.top_queries', { limit: 20 }, 'telemetryPlanOut'));
wire('btnTelemetrySlowQueries', () => call('stats.slow_queries', { limit: 20 }, 'telemetryPlanOut'));
wire('btnTelemetryCoalescing', () => call('stats.coalescing', {}, 'telemetryPlanOut'));

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
initDarkMode();
loadInputs();
populateTemplateSelect();
initNav();
applyMode();
refreshProfiles();
initCollapsibleGroups();
initCommandPalette();
initCommandPaletteEvents();
initKeyboardShortcuts();
loadSqlHistory();
renderResearchDashboard();
renderFeatureCenterGrid();
renderResearchStatusGrid();
renderResearchSettings();
schemaBuilderSeedDefaults();
easySetBuilderRows(defaultEasyBuilderRows());
easyRefreshTargetsFromTree();
setConnStatus('warn', 'Disconnected', 'Connect to start.');

// Auto-connect if same origin
setTimeout(() => { if (getBaseUrl() === window.location.origin) connect(); }, 200);
