const DEFAULT_BASE_URL = window.location.origin || 'http://localhost:8080';
const STATE = {
  methods: [],
  selectedDb: '',
  selectedTable: '',
  dbTree: {},
  isConsole: false
};

const PANEL_META = {
  overview: {
    title: 'Admin Overview',
    subtitle: 'Single-binary admin console. Use RPC Explorer for full control.'
  },
  workspace: {
    title: 'SQL Workspace',
    subtitle: 'Run SQL for compatibility or SkeinQL for full control.'
  },
  schema: {
    title: 'Structure Manager',
    subtitle: 'Create databases, design tables, and review schema.'
  },
  data: {
    title: 'Browse & Edit',
    subtitle: 'Browse rows, insert data, and run table edits.'
  },
  cluster: {
    title: 'Cluster Manager',
    subtitle: 'Plan topology, inspect transport, and manage cluster layouts.'
  },
  settings: {
    title: 'Settings Manager',
    subtitle: 'Read and update server settings, including cluster metadata.'
  },
  migration: {
    title: 'Migration Assistant',
    subtitle: 'Compatibility rewrites and migration reports.'
  },
  nl: {
    title: 'NL Lab',
    subtitle: 'Translate natural language into SkeinQL safely.'
  },
  rpc: {
    title: 'RPC Explorer',
    subtitle: 'Full access to every SkeinDB method.'
  }
};

async function rpc(baseUrl, token, method, params) {
  const url = baseUrl.replace(/\/$/, '') + '/api/v1/rpc';
  const body = {
    skeinql: '1.0',
    id: String(Date.now()),
    method,
    params: params || {}
  };

  const headers = { 'Content-Type': 'application/json' };
  if (token && token.trim().length > 0) {
    headers['Authorization'] = 'Bearer ' + token.trim();
  }

  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body)
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }

  return { status: res.status, json };
}

function $(id) {
  return document.getElementById(id);
}

function getBaseUrl() {
  const input = $('baseUrl');
  if (!input) return DEFAULT_BASE_URL;
  const raw = input.value.trim();
  return raw.length ? raw : DEFAULT_BASE_URL;
}

function getToken() {
  const input = $('token');
  return input ? input.value.trim() : '';
}

function setConnStatus(kind, message, detail) {
  const targets = [$('connStatus'), $('connBadge')].filter(Boolean);
  targets.forEach((pill) => {
    pill.classList.remove('ok', 'warn', 'error');
    if (kind) pill.classList.add(kind);
    pill.textContent = message || 'Disconnected';
  });
  const summary = $('connSummary');
  if (summary) {
    summary.textContent = detail || message || 'Disconnected';
  }
}

function updateHeader(panel) {
  const title = $('pageTitle');
  const subtitle = $('pageSubtitle');
  const meta = PANEL_META[panel] || PANEL_META.overview;
  if (title) {
    if (STATE.isConsole && panel === 'workspace') {
      title.textContent = 'Console Workspace';
    } else {
      title.textContent = meta.title;
    }
  }
  if (subtitle) {
    if (STATE.isConsole && panel === 'workspace') {
      subtitle.textContent = 'Query editor + data tools. Switch servers with Connect.';
    } else {
      subtitle.textContent = meta.subtitle;
    }
  }
}

function updateContext() {
  const server = getBaseUrl();
  const serverTarget = $('contextServer');
  if (serverTarget) serverTarget.textContent = server || '--';
  const dbTarget = $('contextDb');
  if (dbTarget) dbTarget.textContent = STATE.selectedDb || '--';
  const tableTarget = $('contextTable');
  if (tableTarget) tableTarget.textContent = STATE.selectedTable || '--';
  const hint = $('contextHint');
  if (hint) {
    if (!STATE.selectedDb) {
      hint.textContent = 'Select a database from the left tree to begin.';
    } else if (!STATE.selectedTable) {
      hint.textContent = 'Select a table to browse data or open the structure view.';
    } else {
      hint.textContent = 'Ready: use the tabs to browse, edit, or run queries.';
    }
  }
}

function persistInputs() {
  const baseUrl = $('baseUrl');
  const token = $('token');
  if (baseUrl) localStorage.setItem('skeinadmin.baseUrl', baseUrl.value);
  if (token) localStorage.setItem('skeinadmin.token', token.value);
  updateContext();
}

function loadInputs() {
  const baseUrl = $('baseUrl');
  const token = $('token');
  const savedBase = localStorage.getItem('skeinadmin.baseUrl');
  const savedToken = localStorage.getItem('skeinadmin.token');
  if (baseUrl) {
    if (savedBase) {
      baseUrl.value = savedBase;
    } else {
      baseUrl.value = window.location.origin || DEFAULT_BASE_URL;
    }
  }
  if (token && savedToken) token.value = savedToken;
  updateContext();
}

function setOut(obj, targetId = 'out') {
  const target = $(targetId);
  if (!target) return;
  target.textContent = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
}

async function call(method, params = {}, targetId = 'out') {
  const baseUrl = getBaseUrl();
  const token = getToken();
  setConnStatus('warn', 'Connecting', 'Connecting to ' + baseUrl);
  try {
    const res = await rpc(baseUrl, token, method, params || {});
    setOut(res, targetId);
    if (res.json && res.json.ok) {
      setConnStatus('ok', 'Connected', 'Connected to ' + baseUrl);
    } else {
      setConnStatus('error', 'RPC error', 'RPC error from ' + baseUrl);
    }
    return res;
  } catch (e) {
    const hint = baseUrl !== window.location.origin
      ? 'Cross-origin request blocked. Use the same host as the console or enable CORS on the target server.'
      : 'Server not reachable. Verify skeindb is running and the port is correct.';
    setOut({ error: String(e), hint }, targetId);
    setConnStatus('error', 'Offline', 'Unable to reach ' + baseUrl);
    throw e;
  }
}

if ($('baseUrl')) {
  $('baseUrl').addEventListener('change', () => {
    persistInputs();
    setConnStatus('warn', 'Disconnected', 'Server changed. Connect to resume.');
  });
}
if ($('token')) {
  $('token').addEventListener('change', () => {
    persistInputs();
    setConnStatus('warn', 'Disconnected', 'Token updated. Reconnect to apply.');
  });
}
loadInputs();

async function ping() {
  const started = performance.now();
  const res = await call('system.ping', {}, 'out');
  const elapsed = Math.round(performance.now() - started);
  if (res && res.json && res.json.ok && $('infoPing')) {
    $('infoPing').textContent = elapsed + ' ms';
    setConnStatus('ok', 'Connected', 'Latency ' + elapsed + ' ms to ' + getBaseUrl());
    if (res.json.result) {
      setOut(res.json.result, 'out');
    }
  }
  return res;
}

async function loadVersion() {
  const res = await call('system.version', {}, 'out');
  if (res && res.json && res.json.ok && res.json.result) {
    const r = res.json.result;
    if ($('infoVersion')) $('infoVersion').textContent = r.version || '--';
    if ($('infoSkeinql')) $('infoSkeinql').textContent = r.skeinql || '--';
    setOut(r, 'out');
  }
  return res;
}

async function loadTransport() {
  const res = await call('transport.capabilities', {}, 'out');
  if (res && res.json && res.json.ok && res.json.result) {
    const t = res.json.result;
    const desc = 'http=' + (t.http ? 'on' : 'off') + ' · quic=' + (t.quic ? 'on' : 'off');
    if ($('infoTransport')) $('infoTransport').textContent = desc;
    if ($('clusterOut')) setOut(t, 'clusterOut');
    setOut(t, 'out');
  }
  return res;
}

if ($('btnPing')) $('btnPing').addEventListener('click', ping);
if ($('btnVersion')) $('btnVersion').addEventListener('click', loadVersion);

// Additional topbar buttons
if ($('btnStats')) $('btnStats').addEventListener('click', () => loadStats());
if ($('btnCapabilities')) $('btnCapabilities').addEventListener('click', () => loadCapabilities());
if ($('btnCluster')) $('btnCluster').addEventListener('click', () => loadTransport());
if ($('btnAdvisor')) $('btnAdvisor').addEventListener('click', () => call('advisor.history'));

function parseJsonInput(raw, label) {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed);
  } catch (e) {
    throw new Error(label + ' JSON is invalid: ' + e.message);
  }
}

function cleanParams(params) {
  const out = { ...params };
  Object.keys(out).forEach((key) => {
    const value = out[key];
    if (
      value === undefined ||
      value === null ||
      value === '' ||
      (Array.isArray(value) && value.length === 0)
    ) {
      delete out[key];
    }
  });
  return out;
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return '--';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return value.toFixed(value >= 10 || idx === 0 ? 0 : 1) + ' ' + units[idx];
}

function updateStats(stats) {
  if (!stats) return;
  const uptime = Number.isFinite(stats.uptime_s) ? stats.uptime_s + 's' : '--';
  const cpu = stats.process && Number.isFinite(stats.process.cpu_pct)
    ? stats.process.cpu_pct.toFixed(1) + '%'
    : '--';
  const rss = stats.process ? formatBytes(stats.process.rss_bytes) : '--';
  const qps = stats.qps !== undefined ? stats.qps : '--';
  const tps = stats.tps !== undefined ? stats.tps : '--';

  if ($('statUptime')) $('statUptime').textContent = uptime;
  if ($('statCpu')) $('statCpu').textContent = cpu;
  if ($('statRss')) $('statRss').textContent = rss;
  if ($('statQps')) $('statQps').textContent = qps + ' / ' + tps;
}

async function loadStats() {
  const res = await call('stats.snapshot', {}, 'out');
  if (res && res.json && res.json.ok && res.json.result) {
    updateStats(res.json.result);
    setOut(res.json.result, 'out');
  }
}

async function connect() {
  try {
    await ping();
    await loadVersion();
    await loadCapabilities();
    await loadTransport();
    await loadStats();
    await clusterReadStatus();
    await loadDbTree();
    updateContext();
  } catch (e) {
    // call() already handled status updates
  }
}

function disconnect() {
  setConnStatus('warn', 'Disconnected', 'Disconnected. Connect to resume.');
  setOut('Disconnected.', 'out');
  setOut('Run Capabilities to load server features.', 'capabilitiesOut');
  const methodList = $('methodList');
  if (methodList) methodList.textContent = 'Load capabilities to populate methods.';
  STATE.methods = [];
  STATE.dbTree = {};
  STATE.selectedDb = '';
  STATE.selectedTable = '';
  renderDbTree({}, '');
  renderTable('browseTable', [], []);
  renderTable('structureTable', [], []);
  const breadcrumb = $('structureBreadcrumb');
  if (breadcrumb) breadcrumb.textContent = 'No table selected.';
  updateContext();
}

function getProfiles() {
  const raw = localStorage.getItem('skeinadmin.profiles');
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function saveProfiles(profiles) {
  localStorage.setItem('skeinadmin.profiles', JSON.stringify(profiles));
}

function refreshProfiles(selected) {
  const select = $('profileSelect');
  if (!select) return;
  const profiles = getProfiles();
  select.textContent = '';
  const empty = document.createElement('option');
  empty.value = '';
  empty.textContent = 'Select profile';
  select.appendChild(empty);
  Object.keys(profiles).sort().forEach((name) => {
    const opt = document.createElement('option');
    opt.value = name;
    opt.textContent = name;
    select.appendChild(opt);
  });
  if (selected) select.value = selected;
}

function saveProfile() {
  const name = $('profileName') ? $('profileName').value.trim() : '';
  if (!name) {
    setOut({ error: 'Profile name is required' }, 'out');
    return;
  }
  const profiles = getProfiles();
  profiles[name] = {
    baseUrl: getBaseUrl(),
    token: getToken()
  };
  saveProfiles(profiles);
  refreshProfiles(name);
}

function deleteProfile() {
  const select = $('profileSelect');
  if (!select || !select.value) return;
  const profiles = getProfiles();
  delete profiles[select.value];
  saveProfiles(profiles);
  refreshProfiles('');
}

function loadProfile(name) {
  const profiles = getProfiles();
  const profile = profiles[name];
  if (!profile) return;
  if ($('baseUrl')) $('baseUrl').value = profile.baseUrl || '';
  if ($('token')) $('token').value = profile.token || '';
  persistInputs();
  setConnStatus('warn', 'Disconnected', 'Profile loaded. Connect to resume.');
}

function parseJsonArrayInput(inputId, label) {
  const raw = $(inputId) ? $(inputId).value.trim() : '';
  if (!raw) return undefined;
  const parsed = parseJsonInput(raw, label);
  if (!Array.isArray(parsed)) {
    throw new Error(label + ' must be a JSON array');
  }
  return parsed;
}

async function clusterReadStatus() {
  const res = await call('cluster.status', {}, 'clusterOut');
  if (res && res.json && res.json.ok && res.json.result) {
    const status = res.json.result;
    const local = status.local_node_id || '';
    if ($('clusterNodeId') && !($('clusterNodeId').value || '').trim()) {
      $('clusterNodeId').value = local;
    }
    setOut(status, 'clusterOut');
  }
  return res;
}

async function clusterReadNodes() {
  const res = await call('cluster.nodes', {}, 'clusterOut');
  return res;
}

async function clusterCreateToken() {
  try {
    const ttl = parseInt($('clusterJoinTtl').value, 10);
    const role = $('clusterJoinRole') ? $('clusterJoinRole').value : 'replica';
    const params = cleanParams({
      ttl_ms: Number.isNaN(ttl) ? undefined : ttl,
      role
    });
    const res = await call('cluster.join_token.create', params, 'clusterOut');
    if (res && res.json && res.json.ok && res.json.result && $('clusterJoinToken')) {
      $('clusterJoinToken').value = res.json.result.token || '';
    }
  } catch (e) {
    setOut({ error: String(e) }, 'clusterOut');
  }
}

async function clusterJoinNode() {
  try {
    const token = $('clusterJoinToken') ? $('clusterJoinToken').value.trim() : '';
    const nodeId = $('clusterNodeId') ? $('clusterNodeId').value.trim() : '';
    const nodeUrl = $('clusterNodeUrl') ? $('clusterNodeUrl').value.trim() : '';
    const role = $('clusterJoinRole') ? $('clusterJoinRole').value : undefined;
    if (!token || !nodeId || !nodeUrl) {
      throw new Error('Join token, node id, and node URL are required');
    }
    const params = cleanParams({
      token,
      node_id: nodeId,
      rpc_url: nodeUrl,
      role
    });
    await call('cluster.node.join', params, 'clusterOut');
  } catch (e) {
    setOut({ error: String(e) }, 'clusterOut');
  }
}

async function clusterRemoveNode() {
  try {
    const nodeId = $('clusterNodeId') ? $('clusterNodeId').value.trim() : '';
    if (!nodeId) throw new Error('Node id is required');
    await call('cluster.node.remove', { node_id: nodeId }, 'clusterOut');
  } catch (e) {
    setOut({ error: String(e) }, 'clusterOut');
  }
}

async function clusterPromoteNode() {
  try {
    const nodeId = $('clusterNodeId') ? $('clusterNodeId').value.trim() : '';
    if (!nodeId) throw new Error('Node id is required');
    const shardId = $('clusterShardId') ? $('clusterShardId').value.trim() : '';
    const params = cleanParams({
      node_id: nodeId,
      shard_id: shardId || undefined
    });
    await call('cluster.replica.promote', params, 'clusterOut');
  } catch (e) {
    setOut({ error: String(e) }, 'clusterOut');
  }
}

async function clusterShardCreate() {
  try {
    const db = $('clusterShardDb') ? $('clusterShardDb').value.trim() : '';
    if (!db) throw new Error('Shard DB is required');
    const table = $('clusterShardTable') ? $('clusterShardTable').value.trim() : '';
    const shardId = $('clusterShardId') ? $('clusterShardId').value.trim() : '';
    const primary = $('clusterShardPrimary') ? $('clusterShardPrimary').value.trim() : '';
    const replicas = parseJsonArrayInput('clusterShardReplicas', 'Shard replicas');
    const params = cleanParams({
      db,
      table: table || undefined,
      shard_id: shardId || undefined,
      primary_node_id: primary || undefined,
      replicas
    });
    await call('cluster.shard.create', params, 'clusterOut');
  } catch (e) {
    setOut({ error: String(e) }, 'clusterOut');
  }
}

async function clusterShardMove() {
  try {
    const shardId = $('clusterShardId') ? $('clusterShardId').value.trim() : '';
    const toNode = $('clusterMoveTarget') ? $('clusterMoveTarget').value.trim() : '';
    if (!shardId || !toNode) throw new Error('Shard id and move target are required');
    await call(
      'cluster.shard.move',
      { shard_id: shardId, to_node_id: toNode, dry_run: false },
      'clusterOut'
    );
  } catch (e) {
    setOut({ error: String(e) }, 'clusterOut');
  }
}

async function clusterShardRebalance() {
  try {
    await call('cluster.shard.rebalance', { max_moves: 8, dry_run: false }, 'clusterOut');
  } catch (e) {
    setOut({ error: String(e) }, 'clusterOut');
  }
}

async function clusterTransportCapabilities() {
  await call('transport.capabilities', {}, 'clusterOut');
}

async function settingsGetKey() {
  try {
    const key = $('settingsKey') ? $('settingsKey').value.trim() : '';
    if (!key) throw new Error('Settings key is required');
    const res = await call('settings.get', { keys: [key] }, 'settingsOut');
    if (res && res.json && res.json.ok && res.json.result) {
      const value = res.json.result[key];
      if (value !== undefined && $('settingsValue')) {
        $('settingsValue').value = JSON.stringify(value);
      }
    }
  } catch (e) {
    setOut({ error: String(e) }, 'settingsOut');
  }
}

async function settingsSetKey() {
  try {
    const key = $('settingsKey') ? $('settingsKey').value.trim() : '';
    if (!key) throw new Error('Settings key is required');
    const raw = $('settingsValue') ? $('settingsValue').value.trim() : '';
    if (!raw) throw new Error('Settings value is required');
    const value = parseJsonInput(raw, 'Settings value');
    const payload = {};
    payload[key] = value;
    await call('settings.set', payload, 'settingsOut');
  } catch (e) {
    setOut({ error: String(e) }, 'settingsOut');
  }
}

async function settingsLoadClusterKeys() {
  if ($('settingsKey')) $('settingsKey').value = 'cluster.state.v1';
  await settingsGetKey();
}

function populateMethodSelect(methods) {
  const select = $('rpcMethod');
  if (!select) return;
  select.textContent = '';
  methods.forEach((method) => {
    const opt = document.createElement('option');
    opt.value = method;
    opt.textContent = method;
    select.appendChild(opt);
  });
}

function renderMethodList(methods, filter) {
  const target = $('methodList');
  if (!target) return;
  target.textContent = '';
  const match = filter ? filter.toLowerCase() : '';
  const filtered = methods.filter((m) => !match || m.toLowerCase().includes(match));
  if (filtered.length === 0) {
    target.textContent = 'No methods match the filter.';
    return;
  }
  filtered.forEach((method) => {
    const btn = document.createElement('button');
    btn.textContent = method;
    btn.addEventListener('click', () => {
      const select = $('rpcMethod');
      if (select) select.value = method;
      setActivePanel('rpc', true);
    });
    target.appendChild(btn);
  });
}

function formatLit(value) {
  if (value === null || value === undefined) return '';
  if (typeof value !== 'object') return String(value);
  const kind = value.t;
  if (!kind) return JSON.stringify(value);
  if (kind === 'null') return 'null';
  if (Object.prototype.hasOwnProperty.call(value, 'v')) return String(value.v);
  if (Object.prototype.hasOwnProperty.call(value, 'iso')) return value.iso;
  if (Object.prototype.hasOwnProperty.call(value, 'b64')) return value.b64;
  if (Object.prototype.hasOwnProperty.call(value, 'dims')) return 'embedding[' + value.dims + ']';
  return JSON.stringify(value);
}

function renderTable(targetId, columns, rows) {
  const table = $(targetId);
  if (!table) return;
  table.textContent = '';
  if (!columns || columns.length === 0) {
    table.textContent = 'No columns.';
    return;
  }
  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  columns.forEach((col) => {
    const th = document.createElement('th');
    th.textContent = col;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    row.forEach((cell) => {
      const td = document.createElement('td');
      td.textContent = formatLit(cell);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
}

function renderStructure(result) {
  const cols = Array.isArray(result.columns) ? result.columns : [];
  const columns = ['Column', 'Type', 'Nullable', 'Auto Increment'];
  const rows = cols.map((c) => [
    c.name,
    c.type && c.type.kind ? c.type.kind : JSON.stringify(c.type || ''),
    c.nullable ? 'YES' : 'NO',
    c.auto_increment ? 'YES' : 'NO'
  ]);
  renderTable('structureTable', columns, rows);
  const breadcrumb = $('structureBreadcrumb');
  if (breadcrumb) {
    breadcrumb.textContent = result.db && result.table ? result.db + ' / ' + result.table : 'No table selected.';
  }
}

async function loadCapabilities() {
  const res = await call('system.capabilities', {}, 'capabilitiesOut');
  if (res && res.json && res.json.ok && res.json.result) {
    const caps = res.json.result;
    setOut(caps, 'capabilitiesOut');
    const methods = Array.isArray(caps.methods) ? caps.methods : [];
    STATE.methods = methods;
    populateMethodSelect(methods);
    renderMethodList(methods, $('methodSearch') ? $('methodSearch').value : '');
  }
}

function setActivePanel(panel, updateHash) {
  const panels = document.querySelectorAll('.panel');
  const navItems = document.querySelectorAll('.nav-item');
  const tabButtons = document.querySelectorAll('.tab-btn');
  panels.forEach((el) => {
    el.classList.toggle('active', el.dataset.panel === panel);
  });
  navItems.forEach((el) => {
    el.classList.toggle('active', el.dataset.panel === panel);
  });
  tabButtons.forEach((el) => {
    el.classList.toggle('active', el.dataset.panel === panel);
  });
  updateHeader(panel);
  updateContext();
  if (updateHash) {
    window.location.hash = panel;
  }
}

function resolveInitialPanel() {
  const hash = window.location.hash.replace('#', '').trim();
  if (hash) return hash;
  if (window.location.pathname.includes('/console')) return 'workspace';
  return 'overview';
}

function initNav() {
  const navItems = document.querySelectorAll('button[data-panel]');
  navItems.forEach((btn) => {
    btn.addEventListener('click', () => setActivePanel(btn.dataset.panel, true));
  });
  const panelLinks = document.querySelectorAll('[data-panel-link]');
  panelLinks.forEach((btn) => {
    btn.addEventListener('click', () => setActivePanel(btn.dataset.panelLink, true));
  });
  setActivePanel(resolveInitialPanel(), false);
  window.addEventListener('hashchange', () => {
    const panel = resolveInitialPanel();
    setActivePanel(panel, false);
  });
}

function applyMode() {
  const isConsole = window.location.pathname.includes('/console');
  STATE.isConsole = isConsole;
  const body = document.body;
  if (body) {
    body.dataset.mode = isConsole ? 'console' : 'admin';
  }
  if (isConsole && window.location.hash === '') {
    setActivePanel('workspace', false);
  }
  updateHeader(resolveInitialPanel());
}

async function callNl(method, params) {
  const baseUrl = $('baseUrl').value;
  const token = $('token').value;
  const res = await rpc(baseUrl, token, method, params);
  setOut(res, 'nlOut');
  return res;
}

async function nlTranslate() {
  const db = $('nlDb').value.trim();
  const request = $('nlRequest').value.trim();
  const tablesRaw = $('nlTables').value.trim();
  const tables = tablesRaw.length ? tablesRaw.split(',').map((t) => t.trim()).filter(Boolean) : [];
  const includeSchema = $('nlIncludeSchema').checked;
  const readOnly = $('nlReadOnly').checked;
  const maxTables = parseInt($('nlMaxTables').value, 10);
  if (!db || !request) {
    setOut({ error: 'db and request are required' }, 'nlOut');
    return;
  }
  const params = cleanParams({
    db,
    request,
    tables: tables.length ? tables : undefined,
    include_schema: includeSchema,
    read_only: readOnly,
    max_tables: Number.isNaN(maxTables) ? undefined : maxTables
  });
  const res = await callNl('ai.nl.translate', params);
  if (res.json && res.json.ok && res.json.result && res.json.result.query) {
    $('nlQuery').value = JSON.stringify(res.json.result.query, null, 2);
  }
}

async function nlExplain() {
  try {
    const query = parseJsonInput($('nlQuery').value, 'Query');
    if (!query) {
      setOut({ error: 'query JSON is required for explain' }, 'nlOut');
      return;
    }
    const args = parseJsonInput($('nlArgs').value, 'Args');
    const previewLimit = parseInt($('nlPreviewLimit').value, 10);
    const previewFormat = $('nlFormat').value;
    const params = cleanParams({
      query,
      args: args || undefined,
      preview_limit: Number.isNaN(previewLimit) ? undefined : previewLimit,
      preview_format: previewFormat
    });
    const res = await callNl('ai.nl.explain', params);
    if (res.json && res.json.ok && res.json.result) {
      const token = res.json.result.approval_token || '';
      $('nlApproval').value = token;
    }
  } catch (e) {
    setOut({ error: String(e) }, 'nlOut');
  }
}

async function nlExecute() {
  try {
    const query = parseJsonInput($('nlQuery').value, 'Query');
    if (!query) {
      setOut({ error: 'query JSON is required for execute' }, 'nlOut');
      return;
    }
    const args = parseJsonInput($('nlArgs').value, 'Args');
    const approvalToken = $('nlApproval').value.trim();
    if (!approvalToken) {
      setOut({ error: 'approval token is required' }, 'nlOut');
      return;
    }
    const format = $('nlFormat').value;
    const params = cleanParams({
      query,
      args: args || undefined,
      approval_token: approvalToken,
      result_format: format
    });
    await callNl('ai.nl.execute', params);
  } catch (e) {
    setOut({ error: String(e) }, 'nlOut');
  }
}

if ($('btnNlTranslate')) $('btnNlTranslate').addEventListener('click', nlTranslate);
if ($('btnNlExplain')) $('btnNlExplain').addEventListener('click', nlExplain);
if ($('btnNlExecute')) $('btnNlExecute').addEventListener('click', nlExecute);

const RPC_TEMPLATES = [
  {
    label: 'schema.list_databases',
    method: 'schema.list_databases',
    params: {}
  },
  {
    label: 'schema.list_tables',
    method: 'schema.list_tables',
    params: { db: 'demo' }
  },
  {
    label: 'schema.create_table',
    method: 'schema.create_table',
    params: {
      db: 'demo',
      table: 'users',
      columns: [
        { name: 'id', type: 'i64' },
        { name: 'name', type: 'string' }
      ],
      primary_key: ['id'],
      if_not_exists: true
    }
  },
  {
    label: 'data.insert',
    method: 'data.insert',
    params: {
      into: { db: 'demo', table: 'users' },
      rows: [{ id: { t: 'i64', v: 1 }, name: { t: 'string', v: 'Ada' } }]
    }
  },
  {
    label: 'data.update',
    method: 'data.update',
    params: {
      table: { db: 'demo', table: 'users' },
      where: { op: '=', a: { col: 'id' }, b: { lit: { t: 'i64', v: 1 } } },
      set: { name: { t: 'string', v: 'Ada Lovelace' } },
      limit: 1
    }
  },
  {
    label: 'data.delete',
    method: 'data.delete',
    params: {
      table: { db: 'demo', table: 'users' },
      where: { op: '=', a: { col: 'id' }, b: { lit: { t: 'i64', v: 1 } } },
      limit: 1
    }
  },
  {
    label: 'query.select',
    method: 'query.select',
    params: {
      query: { schema: 'demo', table: 'users', select: [{ col: 'id' }, { col: 'name' }] },
      result_format: 'rows_json'
    }
  },
  {
    label: 'vector.search',
    method: 'vector.search',
    params: {
      table: { db: 'demo', table: 'items' },
      query: { dims: 3, v: [0.1, 0.2, 0.3] },
      k: 5
    }
  },
  {
    label: 'cdc.subscribe_table',
    method: 'cdc.subscribe_table',
    params: {
      db: 'demo',
      table: 'users'
    }
  },
  {
    label: 'cdc.poll',
    method: 'cdc.poll',
    params: {
      sub_id: 'sub_1',
      from_offset: 0,
      limit: 200
    }
  },
  {
    label: 'dp.aggregate',
    method: 'dp.aggregate',
    params: {
      table: { db: 'demo', table: 'events' },
      aggregate: { op: 'count', col: 'id' },
      epsilon: 1.0
    }
  },
  {
    label: 'oblivious.policy.get',
    method: 'oblivious.policy.get',
    params: { db: 'demo', table: 'events' }
  },
  {
    label: 'forensic.verify',
    method: 'forensic.verify',
    params: { from_id: 0, limit: 100 }
  },
  {
    label: 'settings.get',
    method: 'settings.get',
    params: { keys: ['cluster.state.v1'] }
  },
  {
    label: 'cluster.status',
    method: 'cluster.status',
    params: {}
  },
  {
    label: 'cluster.join_token.create',
    method: 'cluster.join_token.create',
    params: { ttl_ms: 600000, role: 'replica' }
  },
  {
    label: 'cluster.node.join',
    method: 'cluster.node.join',
    params: {
      token: 'join_token_here',
      node_id: 'replica-a',
      rpc_url: 'http://127.0.0.1:8081',
      role: 'replica'
    }
  },
  {
    label: 'cluster.shard.create',
    method: 'cluster.shard.create',
    params: {
      db: 'app',
      table: 'users',
      replicas: ['replica-a']
    }
  },
  {
    label: 'merge.apply',
    method: 'merge.apply',
    params: {
      table: { db: 'demo', table: 'users' },
      pk: [{ t: 'i64', v: 1 }],
      incoming: { id: { t: 'i64', v: 1 }, name: { t: 'string', v: 'Ada' } }
    }
  },
  {
    label: 'wasm.plan.compile',
    method: 'wasm.plan.compile',
    params: {
      wasm_b64: 'AA==',
      schema: { columns: [{ name: 'x', type: 'i64' }] }
    }
  },
  {
    label: 'merge.wasm.register',
    method: 'merge.wasm.register',
    params: {
      name: 'merge_sum',
      wasm_b64: 'AA=='
    }
  }
];

function populateTemplateSelect() {
  const select = $('rpcTemplate');
  if (!select) return;
  select.textContent = '';
  const empty = document.createElement('option');
  empty.value = '';
  empty.textContent = 'Choose a template';
  select.appendChild(empty);
  RPC_TEMPLATES.forEach((tpl, idx) => {
    const opt = document.createElement('option');
    opt.value = String(idx);
    opt.textContent = tpl.label;
    select.appendChild(opt);
  });
}

function loadTemplate() {
  const select = $('rpcTemplate');
  if (!select) return;
  const idx = parseInt(select.value, 10);
  if (Number.isNaN(idx) || !RPC_TEMPLATES[idx]) return;
  const tpl = RPC_TEMPLATES[idx];
  if ($('rpcMethod')) $('rpcMethod').value = tpl.method;
  if ($('rpcParams')) $('rpcParams').value = JSON.stringify(tpl.params, null, 2);
}

async function rpcSend() {
  const method = $('rpcMethod') ? $('rpcMethod').value.trim() : '';
  if (!method) {
    setOut({ error: 'Select an RPC method' }, 'rpcOut');
    return;
  }
  try {
    const params = parseJsonInput($('rpcParams').value, 'Params') || {};
    await call(method, params, 'rpcOut');
  } catch (e) {
    setOut({ error: String(e) }, 'rpcOut');
  }
}

function tableRef(db, table) {
  return { db, table };
}

function readDbTable(dbId, tableId) {
  const db = $(dbId) ? $(dbId).value.trim() : '';
  const table = $(tableId) ? $(tableId).value.trim() : '';
  if (!db || !table) {
    throw new Error('Database and table are required');
  }
  return tableRef(db, table);
}

async function schemaListDatabases() {
  const res = await call('schema.list_databases', {}, 'schemaOut');
  if (res && res.json && res.json.ok && res.json.result) {
    const dbs = res.json.result.databases || [];
    STATE.selectedDb = '';
    STATE.selectedTable = '';
    renderDatabaseList(dbs);
    updateContext();
  }
}

async function schemaListTables() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : '';
  if (!db) {
    setOut({ error: 'Database is required' }, 'schemaOut');
    return;
  }
  const res = await call('schema.list_tables', { db }, 'schemaOut');
  if (res && res.json && res.json.ok && res.json.result) {
    const tables = res.json.result.tables || [];
    STATE.selectedDb = db;
    renderTableList(tables);
    updateContext();
  }
}

async function schemaDescribe() {
  try {
    const db = $('schemaDb').value.trim();
    const table = $('schemaTable').value.trim();
    if (!db || !table) throw new Error('Database and table are required');
    const res = await call('schema.describe_table', { db, table }, 'schemaOut');
    if (res && res.json && res.json.ok && res.json.result) {
      renderStructure(res.json.result);
    }
  } catch (e) {
    setOut({ error: String(e) }, 'schemaOut');
  }
}

async function schemaCreateDb() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : '';
  if (!db) {
    setOut({ error: 'Database is required' }, 'schemaOut');
    return;
  }
  const res = await call('schema.create_database', { db }, 'schemaOut');
  if (res && res.json && res.json.ok) {
    await loadDbTree();
  }
}

async function schemaCreateTable() {
  try {
    const db = $('schemaDb').value.trim();
    const table = $('schemaTable').value.trim();
    if (!db || !table) throw new Error('Database and table are required');
    const columns = parseJsonInput($('schemaColumns').value, 'Columns');
    if (!Array.isArray(columns)) throw new Error('Columns must be a JSON array');
    const pkRaw = $('schemaPk').value.trim();
    const pk = pkRaw ? pkRaw.split(',').map((c) => c.trim()).filter(Boolean) : [];
    const ifNotExists = $('schemaIfNotExists').value === 'true';
    const res = await call(
      'schema.create_table',
      { db, table, columns, primary_key: pk, if_not_exists: ifNotExists },
      'schemaOut'
    );
    if (res && res.json && res.json.ok) {
      await loadDbTree();
    }
  } catch (e) {
    setOut({ error: String(e) }, 'schemaOut');
  }
}

function renderDatabaseList(dbs) {
  const target = $('dbList');
  if (!target) return;
  target.textContent = '';
  if (!Array.isArray(dbs) || dbs.length === 0) {
    target.textContent = 'No databases.';
    return;
  }
  dbs.forEach((db) => {
    const btn = document.createElement('button');
    btn.textContent = db;
    btn.addEventListener('click', async () => {
      STATE.selectedDb = db;
      STATE.selectedTable = '';
      if ($('schemaDb')) $('schemaDb').value = db;
      await schemaListTables();
      updateContext();
    });
    target.appendChild(btn);
  });
}

function renderTableList(tables) {
  const target = $('tableList');
  if (!target) return;
  target.textContent = '';
  if (!Array.isArray(tables) || tables.length === 0) {
    target.textContent = 'No tables.';
    return;
  }
  tables.forEach((tbl) => {
    const btn = document.createElement('button');
    btn.textContent = tbl;
    btn.addEventListener('click', () => {
      STATE.selectedTable = tbl;
      if ($('schemaTable')) $('schemaTable').value = tbl;
      if ($('dataTable')) $('dataTable').value = tbl;
      updateContext();
    });
    target.appendChild(btn);
  });
}

function renderDbTree(tree, filter) {
  const target = $('dbTree');
  if (!target) return;
  target.textContent = '';
  const match = filter ? filter.toLowerCase() : '';
  const dbs = Object.keys(tree).sort();
  if (dbs.length === 0) {
    target.textContent = 'No databases loaded.';
    return;
  }
  dbs.forEach((db) => {
    const tables = tree[db] || [];
    const tableMatches = match
      ? tables.filter((t) => t.toLowerCase().includes(match))
      : tables;
    if (match && !db.toLowerCase().includes(match) && tableMatches.length === 0) return;
    const details = document.createElement('details');
    details.open = true;
    const summary = document.createElement('summary');
    summary.textContent = db;
    details.appendChild(summary);
    const list = document.createElement('div');
    list.className = 'tree-table';
    tableMatches.forEach((table) => {
      const btn = document.createElement('button');
      btn.textContent = table;
      btn.addEventListener('click', async () => {
        STATE.selectedDb = db;
        STATE.selectedTable = table;
        if ($('schemaDb')) $('schemaDb').value = db;
        if ($('schemaTable')) $('schemaTable').value = table;
        if ($('dataDb')) $('dataDb').value = db;
        if ($('dataTable')) $('dataTable').value = table;
        updateContext();
        await schemaDescribe();
      });
      list.appendChild(btn);
    });
    details.appendChild(list);
    target.appendChild(details);
  });
}

async function loadDbTree() {
  const res = await call('schema.list_databases', {}, 'schemaOut');
  if (!res || !res.json || !res.json.ok) return;
  const dbs = res.json.result && Array.isArray(res.json.result.databases)
    ? res.json.result.databases
    : [];
  const tree = {};
  for (const db of dbs) {
    const tablesRes = await call('schema.list_tables', { db }, 'schemaOut');
    const tables = tablesRes && tablesRes.json && tablesRes.json.ok
      ? (tablesRes.json.result.tables || [])
      : [];
    tree[db] = tables;
  }
  STATE.dbTree = tree;
  renderDbTree(tree, $('dbSearch') ? $('dbSearch').value : '');
  updateContext();
}

async function browseSelectedTable() {
  const db = $('schemaDb') ? $('schemaDb').value.trim() : '';
  const table = $('schemaTable') ? $('schemaTable').value.trim() : '';
  if (!db || !table) {
    setOut({ error: 'Select a database and table first' }, 'schemaOut');
    return;
  }
  const res = await call('schema.describe_table', { db, table }, 'schemaOut');
  if (res && res.json && res.json.ok && res.json.result) {
    const cols = Array.isArray(res.json.result.columns) ? res.json.result.columns : [];
    const projection = cols.map((c) => ({ expr: { col: c.name }, as: null }));
    const query = {
      with: [],
      body: {
        select: {
          projection,
          from: [{ db, table }]
        }
      },
      order_by: [],
      limit: { limit: 50 }
    };
    if ($('skeinMethod')) $('skeinMethod').value = 'query.select';
    if ($('skeinQuery')) {
      $('skeinQuery').value = JSON.stringify(query, null, 2);
    }
    setActivePanel('workspace', true);
  }
}

async function dataGet() {
  try {
    const table = readDbTable('dataDb', 'dataTable');
    const pk = parseJsonInput($('dataPk').value, 'PK') || [];
    await call('data.get', { table, pk }, 'dataOut');
  } catch (e) {
    setOut({ error: String(e) }, 'dataOut');
  }
}

async function dataInsert() {
  try {
    const table = readDbTable('dataDb', 'dataTable');
    const rows = parseJsonInput($('dataRows').value, 'Rows') || [];
    await call('data.insert', { into: table, rows }, 'dataOut');
  } catch (e) {
    setOut({ error: String(e) }, 'dataOut');
  }
}

async function dataUpdate() {
  try {
    const table = readDbTable('dataDb', 'dataTable');
    const where = parseJsonInput($('dataWhere').value, 'Where');
    const set = parseJsonInput($('dataSet').value, 'Set');
    if (!where || !set) throw new Error('Where and set are required');
    const limit = parseInt($('dataLimit').value, 10);
    const params = cleanParams({
      table,
      where,
      set,
      limit: Number.isNaN(limit) ? undefined : limit
    });
    await call('data.update', params, 'dataOut');
  } catch (e) {
    setOut({ error: String(e) }, 'dataOut');
  }
}

async function dataDelete() {
  try {
    const table = readDbTable('dataDb', 'dataTable');
    const where = parseJsonInput($('dataWhere').value, 'Where');
    if (!where) throw new Error('Where is required');
    const limit = parseInt($('dataLimit').value, 10);
    const params = cleanParams({
      table,
      where,
      limit: Number.isNaN(limit) ? undefined : limit
    });
    await call('data.delete', params, 'dataOut');
  } catch (e) {
    setOut({ error: String(e) }, 'dataOut');
  }
}

async function browseTable() {
  try {
    const table = readDbTable('dataDb', 'dataTable');
    const limit = parseInt($('dataBrowseLimit').value, 10);
    const orderCol = $('dataBrowseOrder').value.trim();
    const desc = await call('schema.describe_table', { db: table.db, table: table.table }, 'browseOut');
    if (!desc || !desc.json || !desc.json.ok) return;
    const cols = Array.isArray(desc.json.result.columns)
      ? desc.json.result.columns.map((c) => c.name)
      : [];
    const projection = cols.map((name) => ({ expr: { col: name }, as: null }));
    const query = {
      with: [],
      body: {
        select: {
          projection,
          from: [{ db: table.db, table: table.table }]
        }
      },
      order_by: orderCol ? [{ expr: { col: orderCol }, dir: 'asc' }] : [],
      limit: Number.isNaN(limit) ? undefined : { limit }
    };
    const res = await call('query.select', { query, result_format: 'rows_json' }, 'browseOut');
    if (res && res.json && res.json.ok && res.json.result && res.json.result.data) {
      const data = res.json.result.data;
      const columns = Array.isArray(data.columns) ? data.columns.map((c) => c.name) : [];
      const rows = Array.isArray(data.rows) ? data.rows : [];
      renderTable('browseTable', columns, rows);
      setOut({ rows: rows.length }, 'browseOut');
    }
  } catch (e) {
    setOut({ error: String(e) }, 'browseOut');
  }
}

async function runSql(explain) {
  const sql = $('sqlText') ? $('sqlText').value.trim() : '';
  if (!sql) {
    setOut({ error: 'SQL is required' }, 'sqlOut');
    return;
  }
  await call('sql.exec', { sql, explain: !!explain }, 'sqlOut');
}

async function runSkeinQuery() {
  try {
    const method = $('skeinMethod').value;
    const format = $('skeinFormat').value;
    const args = parseJsonInput($('skeinArgs').value, 'Args') || [];
    const queryId = $('skeinQueryId').value.trim();
    const baseEtag = $('skeinBaseEtag').value.trim();
    const includeFull = $('skeinIncludeFull').value === 'true';
    const params = {};

    if (method === 'query.execute_prepared') {
      if (!queryId) throw new Error('Prepared query id is required');
      params.query_id = queryId;
      if (Array.isArray(args) && args.length) params.args = args;
      if (format) params.result_format = format;
    } else if (method === 'query.prepare') {
      const query = parseJsonInput($('skeinQuery').value, 'Query');
      if (!query) throw new Error('Query JSON is required');
      params.query = query;
    } else {
      const query = parseJsonInput($('skeinQuery').value, 'Query');
      if (!query) throw new Error('Query JSON is required');
      params.query = query;
      if (Array.isArray(args) && args.length) params.args = args;
      if (format) params.result_format = format;
      if (method === 'query.patch') {
        if (baseEtag) params.base_etag = baseEtag;
        params.include_full = includeFull;
      }
    }

    const res = await call(method, params, 'skeinOut');
    if (method === 'query.prepare' && res && res.json && res.json.ok && res.json.result) {
      if ($('skeinQueryId')) $('skeinQueryId').value = res.json.result.query_id || '';
    }
  } catch (e) {
    setOut({ error: String(e) }, 'skeinOut');
  }
}

if ($('btnRpcSend')) $('btnRpcSend').addEventListener('click', rpcSend);
if ($('btnRpcLoadTemplate')) $('btnRpcLoadTemplate').addEventListener('click', loadTemplate);
if ($('methodSearch')) {
  $('methodSearch').addEventListener('input', (e) => {
    renderMethodList(STATE.methods, e.target.value);
  });
}
if ($('rpcMethod') && $('rpcParams')) {
  $('rpcMethod').addEventListener('change', () => {
    if (!$('rpcParams').value.trim()) {
      $('rpcParams').value = '{}';
    }
  });
}

if ($('btnSchemaListDb')) $('btnSchemaListDb').addEventListener('click', schemaListDatabases);
if ($('btnSchemaListTables')) $('btnSchemaListTables').addEventListener('click', schemaListTables);
if ($('btnSchemaDescribe')) $('btnSchemaDescribe').addEventListener('click', schemaDescribe);
if ($('btnSchemaCreateDb')) $('btnSchemaCreateDb').addEventListener('click', schemaCreateDb);
if ($('btnSchemaCreateTable')) $('btnSchemaCreateTable').addEventListener('click', schemaCreateTable);
if ($('btnBrowseTable')) $('btnBrowseTable').addEventListener('click', browseSelectedTable);

if ($('btnDataGet')) $('btnDataGet').addEventListener('click', dataGet);
if ($('btnDataInsert')) $('btnDataInsert').addEventListener('click', dataInsert);
if ($('btnDataUpdate')) $('btnDataUpdate').addEventListener('click', dataUpdate);
if ($('btnDataDelete')) $('btnDataDelete').addEventListener('click', dataDelete);
if ($('btnBrowse')) $('btnBrowse').addEventListener('click', browseTable);

if ($('btnSqlExec')) $('btnSqlExec').addEventListener('click', () => runSql(false));
if ($('btnSqlExplain')) $('btnSqlExplain').addEventListener('click', () => runSql(true));
if ($('btnSkeinRun')) $('btnSkeinRun').addEventListener('click', runSkeinQuery);

if ($('btnConnect')) $('btnConnect').addEventListener('click', connect);
if ($('btnDisconnect')) $('btnDisconnect').addEventListener('click', disconnect);
if ($('btnSaveProfile')) $('btnSaveProfile').addEventListener('click', saveProfile);
if ($('btnDeleteProfile')) $('btnDeleteProfile').addEventListener('click', deleteProfile);
if ($('profileSelect')) {
  $('profileSelect').addEventListener('change', (e) => loadProfile(e.target.value));
}

if ($('btnClusterStatus')) $('btnClusterStatus').addEventListener('click', clusterReadStatus);
if ($('btnClusterNodes')) $('btnClusterNodes').addEventListener('click', clusterReadNodes);
if ($('btnClusterTransport')) $('btnClusterTransport').addEventListener('click', clusterTransportCapabilities);
if ($('btnClusterCreateToken')) $('btnClusterCreateToken').addEventListener('click', clusterCreateToken);
if ($('btnClusterJoinNode')) $('btnClusterJoinNode').addEventListener('click', clusterJoinNode);
if ($('btnClusterRemoveNode')) $('btnClusterRemoveNode').addEventListener('click', clusterRemoveNode);
if ($('btnClusterPromote')) $('btnClusterPromote').addEventListener('click', clusterPromoteNode);
if ($('btnClusterShardCreate')) $('btnClusterShardCreate').addEventListener('click', clusterShardCreate);
if ($('btnClusterShardMove')) $('btnClusterShardMove').addEventListener('click', clusterShardMove);
if ($('btnClusterShardRebalance')) $('btnClusterShardRebalance').addEventListener('click', clusterShardRebalance);

if ($('btnSettingsGet')) $('btnSettingsGet').addEventListener('click', settingsGetKey);
if ($('btnSettingsSet')) $('btnSettingsSet').addEventListener('click', settingsSetKey);
if ($('btnSettingsClusterPreset')) $('btnSettingsClusterPreset').addEventListener('click', settingsLoadClusterKeys);

if ($('btnJumpConsole')) $('btnJumpConsole').addEventListener('click', () => setActivePanel('workspace', true));
if ($('btnJumpSchema')) $('btnJumpSchema').addEventListener('click', () => setActivePanel('schema', true));
if ($('btnJumpData')) $('btnJumpData').addEventListener('click', () => setActivePanel('data', true));
if ($('btnJumpCluster')) $('btnJumpCluster').addEventListener('click', () => setActivePanel('cluster', true));

if ($('btnReloadTree')) $('btnReloadTree').addEventListener('click', loadDbTree);
if ($('dbSearch')) {
  $('dbSearch').addEventListener('input', (e) => {
    renderDbTree(STATE.dbTree, e.target.value);
  });
}

let lastMigrationRewrites = [];
let lastMigrationGeneratedAt = null;

function formatConfidenceValue(value) {
  if (typeof value !== 'number' || Number.isNaN(value)) return 'n/a';
  return Math.round(value * 100) + '%';
}

function formatConfidence(value) {
  return 'confidence: ' + formatConfidenceValue(value);
}

function renderMigrationReport(rewrites) {
  const target = $('migrationReport');
  if (!target) return;
  target.textContent = '';
  if (!Array.isArray(rewrites) || rewrites.length === 0) {
    target.textContent = 'No rewrites yet.';
    return;
  }

  rewrites.forEach((item) => {
    const card = document.createElement('div');
    card.className = 'rewrite-item';

    const head = document.createElement('div');
    head.className = 'rewrite-head';

    const title = document.createElement('div');
    title.className = 'rewrite-title';
    title.textContent = item.title || item.intent || 'Rewrite';

    const meta = document.createElement('div');
    meta.className = 'rewrite-meta';
    meta.textContent = formatConfidence(item.confidence);

    head.appendChild(title);
    head.appendChild(meta);
    card.appendChild(head);

    const tags = document.createElement('div');
    tags.className = 'rewrite-tags';

    const intentTag = document.createElement('span');
    intentTag.className = 'tag';
    intentTag.textContent = item.intent || 'unknown';

    const confidenceTag = document.createElement('span');
    confidenceTag.className = 'tag secondary';
    confidenceTag.textContent = formatConfidence(item.confidence);

    tags.appendChild(intentTag);
    tags.appendChild(confidenceTag);
    card.appendChild(tags);

    const grid = document.createElement('div');
    grid.className = 'rewrite-grid';

    const before = document.createElement('div');
    before.className = 'rewrite-block';
    before.textContent = item.before || '';

    const after = document.createElement('div');
    after.className = 'rewrite-block';
    after.textContent = item.after || '';

    grid.appendChild(before);
    grid.appendChild(after);
    card.appendChild(grid);

    target.appendChild(card);
  });
}

function updateMigrationCache(rewrites) {
  lastMigrationRewrites = Array.isArray(rewrites) ? rewrites : [];
  lastMigrationGeneratedAt = new Date().toISOString();
}

function parseMigrationSamples() {
  const samples = parseJsonInput($('migSamples').value, 'Samples');
  if (samples === null) return null;
  if (!Array.isArray(samples)) {
    throw new Error('Samples JSON must be an array');
  }
  return samples;
}

function migrationParams() {
  const samples = parseMigrationSamples();
  const limit = parseInt($('migLimit').value, 10);
  const windowMs = parseInt($('migWindow').value, 10);
  return cleanParams({
    samples: samples || undefined,
    limit: Number.isNaN(limit) ? undefined : limit,
    window_ms: Number.isNaN(windowMs) ? undefined : windowMs
  });
}

function buildMigrationMarkdown(rewrites, generatedAt) {
  const out = [];
  out.push('# SkeinDB Migration Rewrite Report');
  out.push('');
  out.push('Generated: ' + (generatedAt || new Date().toISOString()));
  out.push('');
  if (!Array.isArray(rewrites) || rewrites.length === 0) {
    out.push('No rewrites found.');
    return out.join('\n');
  }
  rewrites.forEach((item, idx) => {
    const title = item.title || item.intent || 'Rewrite ' + (idx + 1);
    out.push('## ' + title);
    out.push('');
    out.push('- Intent: ' + (item.intent || 'unknown'));
    out.push('- Confidence: ' + formatConfidenceValue(item.confidence));
    if (Array.isArray(item.evidence) && item.evidence.length > 0) {
      out.push('- Evidence:');
      item.evidence.forEach((ev) => {
        const cols = Array.isArray(ev.columns) ? ev.columns.join(', ') : '';
        const note = ev.note ? ' (' + ev.note + ')' : '';
        out.push('  - query[' + ev.query_index + ']' + (cols ? ' columns=' + cols : '') + note);
      });
    }
    out.push('');
    out.push('Before:');
    out.push('```sql');
    out.push(item.before || '');
    out.push('```');
    out.push('');
    out.push('After:');
    out.push('```');
    out.push(item.after || '');
    out.push('```');
    out.push('');
  });
  return out.join('\n');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildMigrationHtml(rewrites, generatedAt) {
  const stamp = generatedAt || new Date().toISOString();
  const parts = [];
  parts.push('<!doctype html>');
  parts.push('<html lang="en"><head><meta charset="utf-8" />');
  parts.push('<title>SkeinDB Migration Rewrite Report</title>');
  parts.push('<style>');
  parts.push('body{font-family:Arial,Helvetica,sans-serif;margin:24px;color:#1c1712;}');
  parts.push('h1{font-size:20px;margin-bottom:4px;}');
  parts.push('h2{font-size:16px;margin-top:24px;}');
  parts.push('.meta{color:#6f6256;font-size:12px;margin-bottom:16px;}');
  parts.push('.card{border:1px solid #e7dccd;border-radius:12px;padding:12px;margin-bottom:16px;background:#fffaf2;}');
  parts.push('pre{background:#fdf6ea;border:1px solid #e7dccd;border-radius:8px;padding:8px;white-space:pre-wrap;}');
  parts.push('.tag{display:inline-block;margin-right:8px;font-size:11px;color:#6f6256;}');
  parts.push('</style></head><body>');
  parts.push('<h1>SkeinDB Migration Rewrite Report</h1>');
  parts.push('<div class="meta">Generated: ' + escapeHtml(stamp) + '</div>');
  if (!Array.isArray(rewrites) || rewrites.length === 0) {
    parts.push('<p>No rewrites found.</p>');
  } else {
    rewrites.forEach((item, idx) => {
      const title = item.title || item.intent || 'Rewrite ' + (idx + 1);
      parts.push('<div class="card">');
      parts.push('<h2>' + escapeHtml(title) + '</h2>');
      parts.push(
        '<div class="tag">Intent: ' + escapeHtml(item.intent || 'unknown') + '</div>'
      );
      parts.push(
        '<div class="tag">Confidence: ' + escapeHtml(formatConfidenceValue(item.confidence)) + '</div>'
      );
      if (Array.isArray(item.evidence) && item.evidence.length > 0) {
        parts.push('<div class="meta">Evidence:</div><ul>');
        item.evidence.forEach((ev) => {
          const cols = Array.isArray(ev.columns) ? ev.columns.join(', ') : '';
          const note = ev.note ? ' (' + ev.note + ')' : '';
          parts.push(
            '<li>query[' +
              escapeHtml(ev.query_index) +
              ']' +
              (cols ? ' columns=' + escapeHtml(cols) : '') +
              escapeHtml(note) +
              '</li>'
          );
        });
        parts.push('</ul>');
      }
      parts.push('<div class="meta">Before:</div>');
      parts.push('<pre>' + escapeHtml(item.before || '') + '</pre>');
      parts.push('<div class="meta">After:</div>');
      parts.push('<pre>' + escapeHtml(item.after || '') + '</pre>');
      parts.push('</div>');
    });
  }
  parts.push('</body></html>');
  return parts.join('');
}

function downloadReport(content, filename, mime) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function exportMigrationReport(format) {
  if (!lastMigrationRewrites || lastMigrationRewrites.length === 0) {
    setOut({ error: 'Run rewrite preview before exporting.' }, 'migrationOut');
    return;
  }
  const stamp = (lastMigrationGeneratedAt || new Date().toISOString()).replace(/[:.]/g, '-');
  if (format === 'json') {
    const payload = {
      generated_at: lastMigrationGeneratedAt || new Date().toISOString(),
      rewrites: lastMigrationRewrites
    };
    downloadReport(
      JSON.stringify(payload, null, 2),
      'skeindb-migration-rewrites-' + stamp + '.json',
      'application/json'
    );
    return;
  }
  if (format === 'md') {
    downloadReport(
      buildMigrationMarkdown(lastMigrationRewrites, lastMigrationGeneratedAt),
      'skeindb-migration-rewrites-' + stamp + '.md',
      'text/markdown'
    );
    return;
  }
  if (format === 'html') {
    downloadReport(
      buildMigrationHtml(lastMigrationRewrites, lastMigrationGeneratedAt),
      'skeindb-migration-rewrites-' + stamp + '.html',
      'text/html'
    );
  }
}

async function copyToClipboard(text) {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return true;
  }
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  const ok = document.execCommand('copy');
  textarea.remove();
  return ok;
}

async function copyMigrationMarkdown() {
  if (!lastMigrationRewrites || lastMigrationRewrites.length === 0) {
    setOut({ error: 'Run rewrite preview before copying.' }, 'migrationOut');
    return;
  }
  const text = buildMigrationMarkdown(lastMigrationRewrites, lastMigrationGeneratedAt);
  try {
    const ok = await copyToClipboard(text);
    if (!ok) {
      throw new Error('Clipboard copy failed');
    }
    setOut({ ok: true, copied: true }, 'migrationOut');
  } catch (e) {
    setOut({ error: String(e) }, 'migrationOut');
  }
}

async function migrationPreview() {
  const baseUrl = $('baseUrl').value;
  const token = $('token').value;
  try {
    setOut({ loading: true }, 'migrationOut');
    const res = await rpc(baseUrl, token, 'migration.rewrite_preview', migrationParams());
    setOut(res, 'migrationOut');
    if (res.json && res.json.ok && res.json.result) {
      const rewrites = res.json.result.rewrites || [];
      updateMigrationCache(rewrites);
      renderMigrationReport(rewrites);
    }
  } catch (e) {
    setOut({ error: String(e) }, 'migrationOut');
  }
}

async function migrationIntent() {
  const baseUrl = $('baseUrl').value;
  const token = $('token').value;
  try {
    setOut({ loading: true }, 'migrationOut');
    const res = await rpc(baseUrl, token, 'migration.intent_report', migrationParams());
    setOut(res, 'migrationOut');
  } catch (e) {
    setOut({ error: String(e) }, 'migrationOut');
  }
}

if ($('btnMigrationPreview')) $('btnMigrationPreview').addEventListener('click', migrationPreview);
if ($('btnMigrationIntent')) $('btnMigrationIntent').addEventListener('click', migrationIntent);
if ($('btnMigrationDownloadJson')) {
  $('btnMigrationDownloadJson').addEventListener('click', () => exportMigrationReport('json'));
}
if ($('btnMigrationDownloadMd')) {
  $('btnMigrationDownloadMd').addEventListener('click', () => exportMigrationReport('md'));
}
if ($('btnMigrationDownloadHtml')) {
  $('btnMigrationDownloadHtml').addEventListener('click', () => exportMigrationReport('html'));
}
if ($('btnMigrationCopyMd')) {
  $('btnMigrationCopyMd').addEventListener('click', copyMigrationMarkdown);
}

populateTemplateSelect();
initNav();
applyMode();
refreshProfiles();
setConnStatus('warn', 'Disconnected', 'Connect to start.');
setTimeout(() => {
  if (getBaseUrl() === window.location.origin) {
    connect();
  }
}, 200);
