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

function setOut(obj, targetId = 'out') {
  const target = $(targetId);
  if (!target) return;
  target.textContent = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
}

async function call(method) {
  const baseUrl = $('baseUrl').value;
  const token = $('token').value;
  try {
    const res = await rpc(baseUrl, token, method, {});
    setOut(res);
  } catch (e) {
    setOut({ error: String(e) });
  }
}

$('btnPing').addEventListener('click', () => call('system.ping'));
$('btnVersion').addEventListener('click', () => call('system.version'));

// Additional placeholder buttons
if ($('btnStats')) $('btnStats').addEventListener('click', () => call('stats.snapshot'));
if ($('btnCluster')) $('btnCluster').addEventListener('click', () => call('cluster.status'));
if ($('btnAdvisor')) $('btnAdvisor').addEventListener('click', () => call('advisor.index_suggestions'));

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
