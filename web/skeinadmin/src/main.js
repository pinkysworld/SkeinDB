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
  easyDesignTable: null,
  easyDesignOriginal: null,
  easyDesignDraft: null,
  easySortColumn: '',
  easySortDir: 'asc',
  preparedQueries: [],
  txCurrentId: '',
  txReadOnly: false,
  schemaLastIndexCount: null,
  schemaLastIndexDb: '',
  schemaLastIndexTable: '',
  qbConditions: [],
  advisorSuggestions: [],
  advisorHistory: [],
  advisorSelection: null,
  cdcSubscriptions: [],
  cdcSelectedSubId: '',
  replayHistoryStatus: null,
  replayLastBundle: null,
  edgeLastBundle: null,
  replayImports: [],
  replaySelectedWorkspaceId: '',
  replayLastRun: null,
  wasmArtifactB64: ''
};

// ---------------------------------------------------------------------------
// Panel metadata
// ---------------------------------------------------------------------------
const PANEL_META = {
  overview:   { title: 'Admin Overview',      subtitle: 'Single-binary admin console with all 20 research features.' },
  easy:       { title: 'Easy Viewer',         subtitle: 'Click-first controls with inline grid editing and guided forms.' },
  workspace:  { title: 'SQL Workspace',        subtitle: 'Run SQL, prepare SkeinQL queries, and manage transaction handles.' },
  schema:     { title: 'Structure Manager',    subtitle: 'Create databases, design tables, and manage secondary indexes.' },
  data:       { title: 'Browse & Edit',        subtitle: 'Browse rows, insert data, and run table edits.' },
  cluster:    { title: 'Cluster Manager',      subtitle: 'Plan topology, inspect transport, and manage layouts.' },
  settings:   { title: 'Settings Manager',     subtitle: 'Read and update server settings and feature config.' },
  telemetry:  { title: 'Telemetry Center',     subtitle: 'Inspect compatibility, feature usage, plan cache, and query pressure.' },
  cdc:        { title: 'CDC Manager',          subtitle: 'Subscribe to tables or prepared queries, poll events, ACK offsets, and inspect lag.' },
  replay:     { title: 'Time Travel & Replay', subtitle: 'Run point-in-time queries, manage history retention, verify replay bundles, and manage edge bundles.' },
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
  rpc:        { title: 'RPC Explorer',         subtitle: 'Full access to every SkeinDB method.' },
  help:       { title: 'Help & Documentation', subtitle: 'Quick start, panel reference, research-track index, shortcuts, and links to the canonical docs site.' }
};

// ---------------------------------------------------------------------------
// Research tracks
// ---------------------------------------------------------------------------
const RESEARCH_TRACKS = [
  { id: 'R01', title: 'Learned Index Structures', desc: 'CDF-based learned indexes for ValueID lookup.', methods: ['stats.snapshot', 'system.capabilities'], panel: 'overview', status: 'prototype' },
  { id: 'R02', title: 'Adaptive Row-Column Hybrid', desc: 'Dynamic row/column execution selection.', methods: ['system.capabilities', 'settings.get'], panel: 'engine', status: 'hardened' },
  { id: 'R03', title: 'Delta-Chain Topology', desc: 'Linear, tree, skip-list delta chains for versioned values.', methods: ['stats.snapshot', 'settings.get'], panel: 'engine', status: 'hardened' },
  { id: 'R04', title: 'Differential Privacy', desc: 'DP aggregates with calibrated Laplace noise.', methods: ['dp.aggregate', 'dp.evaluate', 'dp.budget.get', 'dp.budget.set', 'dp.audit.log'], panel: 'privacy', status: 'hardened' },
  { id: 'R05', title: 'Oblivious Execution', desc: 'Padding, dummy lookups, leakage reports, and overhead reports for access-pattern protection.', methods: ['oblivious.policy.get', 'oblivious.policy.set', 'oblivious.explain', 'oblivious.evaluate'], panel: 'privacy', status: 'hardened' },
  { id: 'R06', title: 'Forensic Audit', desc: 'Filtered hash-chain queries with boundary, checkpoint, and Merkle inclusion proofs.', methods: ['maintenance.audit_status', 'maintenance.audit_verify', 'forensic.verify', 'forensic.query', 'forensic.export'], panel: 'forensics', status: 'hardened' },
  { id: 'R07', title: 'Merge & CRDT', desc: 'Client-side merge functions with conflict hooks, offline queues, evaluation, and values-only Wasm execution.', methods: ['merge.apply', 'merge.register', 'merge.simulate', 'merge.evaluate', 'merge.wasm.register', 'merge.wasm.list', 'merge.wasm.drop'], panel: 'merge', status: 'hardened' },
  { id: 'R08', title: 'Incremental Views', desc: 'Dependency-graph-driven materialized view maintenance.', methods: ['view.create', 'view.refresh', 'view.evaluate', 'view.status', 'view.drop', 'view.explain_deps'], panel: 'views', status: 'hardened' },
  { id: 'R09', title: 'QUIC Transport', desc: 'HTTP/3 and QUIC-native protocol with prepared-query streams, 0-RTT write rejection, and rebind coverage; comparative p99 benchmarking remains open.', methods: ['transport.capabilities'], panel: 'cluster', status: 'hardened' },
  { id: 'R10', title: 'Vector Embeddings', desc: 'First-class vector columns with kNN search and recall/latency benchmarking.', methods: ['vector.search', 'vector.benchmark', 'vector.insert', 'vector.index.status'], panel: 'vectors', status: 'hardened' },
  { id: 'R11', title: 'Autoparameterization', desc: 'LLM-assisted SQL parameterization.', methods: ['ai.autoparam.analyze', 'ai.autoparam.classify'], panel: 'nl', status: 'hardened' },
  { id: 'R12', title: 'NL-to-SkeinQL', desc: 'Natural language query translation with verification.', methods: ['ai.nl.translate', 'ai.nl.explain', 'ai.nl.execute'], panel: 'nl', status: 'hardened' },
  { id: 'R13', title: 'Causal Consistency', desc: 'ETag-chain causal ordering across replicas.', methods: ['query.patch', 'query.select'], panel: 'workspace', status: 'hardened' },
  { id: 'R14', title: 'Edge Bundles', desc: 'Geo-distributed replay bundles with edge caching.', methods: ['edge.bundle.request', 'edge.bundle.apply', 'edge.bundle.status'], panel: 'replay', status: 'hardened' },
  { id: 'R15', title: 'Schema Evolution', desc: 'Conflict-free schema evolution with divergence guidance, rollout simulation, and controlled apply.', methods: ['schema.propose_change', 'schema.merge_status', 'schema.simulate_rollout', 'schema.apply_merge'], panel: 'schema', status: 'hardened' },
  { id: 'R16', title: 'Index Advisor', desc: 'Workload-driven index synthesis and recommendation.', methods: ['advisor.index_synthesize', 'advisor.history', 'advisor.apply_index', 'advisor.dismiss'], panel: 'advisor', status: 'hardened' },
  { id: 'R17', title: 'Migration Hints', desc: 'Compatibility telemetry and rewrite previews.', methods: ['migration.rewrite_preview', 'migration.intent_report', 'migration.report_export'], panel: 'migration', status: 'hardened' },
  { id: 'R18', title: 'Perf Replay', desc: 'Snapshot + replay for performance regression testing.', methods: ['maintenance.replay.export', 'maintenance.replay.import', 'maintenance.replay.run'], panel: 'replay', status: 'prototype' },
  { id: 'R19', title: 'Wasm Operators', desc: 'User-defined Wasm query plan operators.', methods: ['wasm.plan.compile', 'wasm.plan.run'], panel: 'wasm', status: 'prototype' },
  { id: 'R20', title: 'Energy-Aware Compaction', desc: 'Carbon-aware scheduling for background compaction.', methods: ['maintenance.compaction.status', 'maintenance.compaction.set_policy', 'maintenance.compaction.pause', 'maintenance.compaction.resume'], panel: 'engine', status: 'hardened' }
];

// ---------------------------------------------------------------------------
// Feature center items
// ---------------------------------------------------------------------------
const FEATURE_CENTER = [
  { title: 'Easy Viewer', desc: 'Click-first controls for daily operations.', panel: 'easy' },
  { title: 'SQL Compat', desc: 'MySQL and PostgreSQL compatibility catalogs, window functions, and client bootstrap probes.', panel: 'workspace' },
  { title: 'SkeinQL', desc: 'Native structured query API.', panel: 'workspace' },
  { title: 'Prepared Queries', desc: 'Prepare once, execute repeatedly, and expose GET + CDC hooks.', panel: 'workspace' },
  { title: 'Transactions', desc: 'Open, commit, and roll back explicit tx handles.', panel: 'workspace' },
  { title: 'Schema Mgmt', desc: 'Create/alter DB and tables.', panel: 'schema' },
  { title: 'Secondary Indexes', desc: 'Inspect and manage index DDL from guided fields.', panel: 'schema' },
  { title: 'Schema Evolution', desc: 'Propose changes, inspect divergence, simulate rollout, and apply merges.', panel: 'schema' },
  { title: 'Data Browse', desc: 'Guided row browser and editor.', panel: 'data' },
  { title: 'Engine Config', desc: 'Toggle dedup, MVCC, cache, security.', panel: 'engine' },
  { title: 'Cluster', desc: 'Multi-node topology and sharding.', panel: 'cluster' },
  { title: 'CDC', desc: 'Table subscriptions, polling, ACKs, and lag view.', panel: 'cdc' },
  { title: 'CDC Query Feeds', desc: 'Subscribe prepared queries to invalidation streams.', panel: 'cdc' },
  { title: 'Time Travel & Replay', desc: 'As-of queries, retention controls, and replay integrity checks.', panel: 'replay' },
  { title: 'Edge Bundles', desc: 'Request/apply edge coverage bundles.', panel: 'replay' },
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
  { label: 'stats.top_queries', method: 'stats.top_queries', params: { limit: 10 } },
  { label: 'stats.slow_queries', method: 'stats.slow_queries', params: { limit: 10, min_ms: 0 } },
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
  { label: 'query.select (as_of)', method: 'query.select', params: { query:{schema:'demo',table:'users',select:[{col:'id'},{col:'name'}]}, as_of:{t:'datetime',iso:'2026-01-01T00:00:00Z'}, result_format:'rows_json' } },
  { label: 'query.patch', method: 'query.patch', params: { query:{schema:'demo',table:'users',select:[{col:'id'}]}, base_etag:'', include_full:true, result_format:'rows_json' } },
  { label: 'query.prepare', method: 'query.prepare', params: { query:{body:{select:{from:[{db:'demo',table:'users'}],projection:[{expr:{col:'id'}},{expr:{col:'name'}}]}}} } },
  { label: 'query.execute_prepared', method: 'query.execute_prepared', params: { query_id:'query_demo', args:[], result_format:'rows_json' } },
  { label: 'tx.begin', method: 'tx.begin', params: { read_only:false } },
  { label: 'tx.commit', method: 'tx.commit', params: { tx_id:'tx_demo' } },
  { label: 'tx.rollback', method: 'tx.rollback', params: { tx_id:'tx_demo' } },
  { label: 'vector.search', method: 'vector.search', params: { table:{db:'demo',table:'items'}, column:'embedding', query:{t:'embedding',dims:3,v:[0.1,0.2,0.3]}, k:5 } },
  { label: 'vector.benchmark', method: 'vector.benchmark', params: { table:{db:'demo',table:'items'}, column:'embedding', queries:[{t:'embedding',dims:3,v:[0.1,0.2,0.3]}], k:5 } },
  { label: 'dp.aggregate', method: 'dp.aggregate', params: { table:{db:'demo',table:'events'}, aggregates:[{op:'count'}], epsilon:1.0, mechanism:'laplace', seed:42 } },
  { label: 'dp.evaluate', method: 'dp.evaluate', params: { table:{db:'demo',table:'events'}, aggregates:[{op:'sum',column:'value',bounds:{min:0,max:100}}], epsilons:[0.25,0.5,1,2], trials:25, mechanism:'laplace', seed:42 } },
  { label: 'oblivious.policy.get', method: 'oblivious.policy.get', params: { table:{db:'demo', table:'events'} } },
  { label: 'oblivious.evaluate', method: 'oblivious.evaluate', params: { table:{db:'demo', table:'events'}, trace_rows:[1,2,8,16,32,64] } },
  { label: 'maintenance.audit_status', method: 'maintenance.audit_status', params: {} },
  { label: 'maintenance.audit_verify', method: 'maintenance.audit_verify', params: {} },
  { label: 'forensic.verify', method: 'forensic.verify', params: { records:[], start_hash:'genesis' } },
  { label: 'forensic.query', method: 'forensic.query', params: { from_id:0, limit:50, filter:{op:'eq', a:{col:'db'}, b:{lit:{t:'str', v:'demo'}}} } },
  { label: 'forensic.export', method: 'forensic.export', params: { from_id:0, limit:50, bundle_id:'incident-demo' } },
  { label: 'view.create', method: 'view.create', params: { view:{db:'demo',table:'active_users'}, query:{schema:'demo',table:'users',select:[{col:'id'}]} } },
  { label: 'view.refresh', method: 'view.refresh', params: { view:{db:'demo',table:'active_users'}, mode:'auto' } },
  { label: 'view.evaluate', method: 'view.evaluate', params: { view:{db:'demo',table:'active_users'}, iterations:5 } },
  { label: 'view.status', method: 'view.status', params: { view:{db:'demo',table:'active_users'} } },
  { label: 'merge.apply', method: 'merge.apply', params: { table:{db:'demo',table:'users'}, pk:[{t:'i64',v:1}], incoming:{id:{t:'i64',v:1},name:{t:'str',v:'Ada'}} } },
  { label: 'merge.evaluate', method: 'merge.evaluate', params: { policy:{default:{kind:'builtin',name:'last_write_wins'},per_column:{count:{kind:'builtin',name:'sum'}}}, iterations:10, cases:[{name:'counter conflict',current:{count:{t:'u64',v:7}},incoming:{count:{t:'u64',v:4}},expected_etag_match:false,min_causality_satisfied:true,constraint_ok:true}] } },
  { label: 'merge.wasm.register', method: 'merge.wasm.register', params: { module_id:'merge_sum', name:'Counter sum', wasm_b64:'', capabilities:{values_only:true,deterministic:true,max_fuel:20000,max_memory_bytes:65536,max_output_bytes:64} } },
  { label: 'wasm.plan.compile', method: 'wasm.plan.compile', params: { query:{body:{select:{projection:[{expr:{col:'id'}}],from:[{db:'demo',table:'users'}]}}} } },
  { label: 'wasm.plan.inspect', method: 'wasm.plan.inspect', params: { artifact_b64:'<compile-result-artifact>' } },
  { label: 'wasm.plan.edge_package', method: 'wasm.plan.edge_package', params: { artifact_b64:'<compile-result-artifact>', package_name:'demo-plan' } },
  { label: 'advisor.index_synthesize', method: 'advisor.index_synthesize', params: { table:{db:'demo',table:'users'}, limit:5, min_queries:1, min_rows:1 } },
  { label: 'advisor.apply_index', method: 'advisor.apply_index', params: { table:{db:'demo',table:'users'}, columns:['city'], include:['name'] } },
  { label: 'advisor.history', method: 'advisor.history', params: { table:{db:'demo',table:'users'}, limit:10 } },
  { label: 'ai.autoparam.analyze', method: 'ai.autoparam.analyze', params: { sql:'SELECT * FROM users WHERE id = 42' } },
  { label: 'cdc.subscribe_table', method: 'cdc.subscribe_table', params: { db:'demo', table:'users' } },
  { label: 'cdc.subscribe_query', method: 'cdc.subscribe_query', params: { query_id:'query_demo', args:[] } },
  { label: 'cdc.poll', method: 'cdc.poll', params: { sub_id:'sub_1', from_offset:0, limit:200 } },
  { label: 'cdc.ack', method: 'cdc.ack', params: { sub_id:'sub_1', offset:42 } },
  { label: 'cdc.close', method: 'cdc.close', params: { sub_id:'sub_1' } },
  { label: 'maintenance.history.status', method: 'maintenance.history.status', params: { horizon_ms: 1767225600000 } },
  { label: 'maintenance.history.set_policy', method: 'maintenance.history.set_policy', params: { enabled: true, window_ms: 604800000 } },
  { label: 'maintenance.history.gc', method: 'maintenance.history.gc', params: { horizon_ms: 1767225600000 } },
  { label: 'maintenance.compaction.status', method: 'maintenance.compaction.status', params: {} },
  { label: 'maintenance.compaction.set_policy', method: 'maintenance.compaction.set_policy', params: { policy: 'energy_aware', enabled: true, paused: false, max_l0_files: 16, budget: { max_io_bytes_per_s: 33554432, max_cpu_pct: 35 }, external_signals: { power_source: 'plugged', price_multiplier: 0.85, carbon_multiplier: 0.9 }, peak_windows: ['08:00-18:00'] } },
  { label: 'maintenance.compaction.pause', method: 'maintenance.compaction.pause', params: {} },
  { label: 'maintenance.compaction.resume', method: 'maintenance.compaction.resume', params: {} },
  { label: 'maintenance.replay.export', method: 'maintenance.replay.export', params: { db:'demo', bundle_id:'replay_bundle_demo' } },
  { label: 'maintenance.replay.import', method: 'maintenance.replay.import', params: { bundle:{manifest:{bundle_id:'replay_bundle_demo'},tables:[],changes:[]}, workspace_id:'replay_demo' } },
  { label: 'maintenance.replay.run', method: 'maintenance.replay.run', params: { workspace_id:'replay_demo' } },
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
  { label: 'migration.report_export', method: 'migration.report_export', params: { title: 'SkeinDB migration report', limit: 10 } },
  { label: 'telemetry.feature_flags', method: 'telemetry.feature_flags', params: {} },
  { label: 'telemetry.compat_summary', method: 'telemetry.compat_summary', params: {} },
  { label: 'telemetry.migration_hints', method: 'telemetry.migration_hints', params: { limit: 10 } },
  { label: 'telemetry.workload_features', method: 'telemetry.workload_features', params: {} },
  { label: 'plan_cache.status', method: 'plan_cache.status', params: {} },
  { label: 'plan_cache.clear', method: 'plan_cache.clear', params: {} },
  { label: 'stats.coalescing', method: 'stats.coalescing', params: {} },
  { label: 'transport.capabilities', method: 'transport.capabilities', params: {} },
  { label: 'edge.bundle.request', method: 'edge.bundle.request', params: { windows: [{ table: { db: 'demo', table: 'users' }, from_seq: 0, max_events: 100 }], redaction: { mode: 'hash_pk', salt: 'demo' }, bundle_id: 'edge_bundle_demo' } },
  { label: 'edge.bundle.apply', method: 'edge.bundle.apply', params: { bundle: { bundle_id: 'edge_bundle_demo', generated_at_ms: 0, redaction: { mode: 'hash_pk' }, coverage: [], records: [] } } },
  { label: 'edge.bundle.status', method: 'edge.bundle.status', params: { max_lag: 100 } }
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
  refreshDashboardSummaries();
}

function setSelectedDb(db) {
  STATE.selectedDb = (db || '').trim();
  ['schemaDb','dataDb','dataFormDb','dpDb','oblDb','vecDb','viewDb','mergeDb','advDb','importDb','nlDb','clusterShardDb','ttDb','replayDb','edgeDb'].forEach(id => {
    const el = $(id); if (el) el.value = STATE.selectedDb;
  });
  const easyDb = $('easyCreateDb');
  if (easyDb && !easyDb.value.trim()) easyDb.value = STATE.selectedDb;
  updateContext();
}

function setSelectedTable(table) {
  STATE.selectedTable = (table || '').trim();
  ['schemaTable','dataTable','dataFormTable','dpTable','oblTable','vecTable','mergeTable','advTable','importTable','clusterShardTable','ttTable','edgeTable'].forEach(id => {
    const el = $(id); if (el) el.value = STATE.selectedTable;
  });
  const easyTable = $('easyCreateTableName');
  if (easyTable && !easyTable.value.trim()) easyTable.value = STATE.selectedTable;
  updateContext();
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
  refreshDashboardSummaries();
}

function renderGlanceCard(label, value, detail) {
  return '<div class="glance-card"><div class="glance-label">' + escapeHtml(label)
    + '</div><div class="glance-value">' + escapeHtml(value)
    + '</div><div class="glance-detail">' + escapeHtml(detail) + '</div></div>';
}

function currentSelectionLabel() {
  if (STATE.selectedDb && STATE.selectedTable) return STATE.selectedDb + '.' + STATE.selectedTable;
  if (STATE.selectedDb) return STATE.selectedDb + '.*';
  return 'No active selection';
}

function cdcSubscriptionStats() {
  const subs = Array.isArray(STATE.cdcSubscriptions) ? STATE.cdcSubscriptions : [];
  const stats = { total: subs.length, tableCount: 0, queryCount: 0, selected: null };
  subs.forEach((sub) => {
    if (sub.kind === 'query') stats.queryCount += 1;
    else stats.tableCount += 1;
  });
  stats.selected = cdcFindSubscription(STATE.cdcSelectedSubId);
  return stats;
}

function renderOverviewHero() {
  const connection = $('overviewHeroConnection');
  const methods = $('overviewHeroMethods');
  const research = $('overviewHeroResearch');
  const mode = $('overviewHeroMode');
  const selectionSummary = $('overviewSelectionSummary');
  const sessionSummary = $('overviewSessionSummary');
  const coverageSummary = $('overviewCoverageSummary');
  const methodCount = Array.isArray(STATE.methods) ? STATE.methods.length : 0;
  const hardenedCount = RESEARCH_TRACKS.filter((track) => track.status === 'hardened').length;
  const prepared = Array.isArray(STATE.preparedQueries) ? STATE.preparedQueries : [];
  const latestPrepared = latestPreparedQuery();
  const txId = (($('txId')?.value || STATE.txCurrentId || '').trim());
  const replayCount = Array.isArray(STATE.replayImports) ? STATE.replayImports.length : 0;
  const cdcStats = cdcSubscriptionStats();
  if (connection) connection.textContent = STATE.connected ? 'Online' : 'Offline';
  if (methods) methods.textContent = methodCount ? String(methodCount) : '--';
  if (research) research.textContent = hardenedCount + '/' + String(RESEARCH_TRACKS.length);
  if (mode) mode.textContent = STATE.isConsole ? 'Console' : 'Admin';
  if (selectionSummary) {
    const copy = !STATE.selectedDb
      ? 'No database selected yet. Use the left tree or Schema panel to establish the active working set.'
      : !STATE.selectedTable
        ? 'Database ' + STATE.selectedDb + ' is active. Select a table to unlock seeded browse, index, CDC, and replay workflows.'
        : 'Active selection is ' + STATE.selectedDb + '.' + STATE.selectedTable + '. Workspace, Schema, CDC, and Replay panels will seed from it.';
    selectionSummary.innerHTML = '<strong>Selection</strong>' + escapeHtml(copy);
  }
  if (sessionSummary) {
    const txCopy = txId ? txId + (STATE.txReadOnly ? ' (read only)' : ' (read/write)') : 'No active tx_id';
    sessionSummary.innerHTML = '<strong>Session State</strong>Prepared queries: ' + escapeHtml(String(prepared.length))
      + ' | Active transaction: ' + escapeHtml(txCopy)
      + '<br>CDC subscriptions: ' + escapeHtml(String(cdcStats.total) + ' total (' + cdcStats.tableCount + ' table / ' + cdcStats.queryCount + ' query)')
      + ' | Replay workspaces: ' + escapeHtml(String(replayCount));
  }
  if (coverageSummary) {
    const coverageCopy = methodCount
      ? ('Loaded methods: ' + methodCount + ' | Latest prepared query: ' + (latestPrepared?.query_id || 'none yet'))
      : 'Connect to load the live method surface and enable direct jump links into supported RPC examples.';
    coverageSummary.innerHTML = '<strong>Coverage</strong>' + escapeHtml(coverageCopy)
      + '<br>' + escapeHtml(hardenedCount + ' hardened research tracks and ' + (RESEARCH_TRACKS.length - hardenedCount) + ' prototype tracks are mapped into this console.');
  }
}

function renderWorkspaceGlance() {
  const host = $('workspaceSummaryBar');
  if (!host) return;
  const latestPrepared = latestPreparedQuery();
  const preparedCount = Array.isArray(STATE.preparedQueries) ? STATE.preparedQueries.length : 0;
  const txId = (($('txId')?.value || STATE.txCurrentId || '').trim());
  host.innerHTML = [
    renderGlanceCard(
      'Selection',
      currentSelectionLabel(),
      STATE.selectedTable
        ? 'SQL templates, SkeinQL helpers, CDC, and replay tools can seed from the active table.'
        : 'Pick a table from the left tree to prefill the SQL and SkeinQL surfaces.'
    ),
    renderGlanceCard(
      'Prepared Studio',
      latestPrepared ? latestPrepared.query_id : 'No prepared query',
      preparedCount + ' query id(s) cached in this browser session.'
    ),
    renderGlanceCard(
      'Transactions',
      txId || 'Idle',
      txId ? ('Mode: ' + (STATE.txReadOnly ? 'read only' : 'read/write')) : 'Begin a transaction when you need a stable tx_id across multiple steps.'
    ),
    renderGlanceCard(
      'Next Move',
      latestPrepared ? 'CDC or GET handoff ready' : 'Prepare the current query',
      latestPrepared
        ? 'Use the CDC handoff or copy the GET URL for zero-argument prepared queries.'
        : 'Preparing once unlocks reusable execution, GET, and query-scoped CDC flows.'
    )
  ].join('');
}

function renderSchemaGlance() {
  const host = $('schemaSummaryBar');
  if (!host) return;
  const describedTarget = STATE.schemaLastIndexDb && STATE.schemaLastIndexTable
    ? (STATE.schemaLastIndexDb + '.' + STATE.schemaLastIndexTable)
    : 'No described table';
  const indexCount = STATE.schemaLastIndexCount;
  host.innerHTML = [
    renderGlanceCard(
      'Selection',
      currentSelectionLabel(),
      STATE.selectedTable
        ? 'Use the selected table to drive describe, structure, and index flows.'
        : 'Choose a table from the tree to seed schema actions and the index manager.'
    ),
    renderGlanceCard(
      'Last Index Snapshot',
      describedTarget,
      indexCount === null ? 'Describe a table or load indexes to capture the current secondary-index picture.' : String(indexCount) + ' secondary index(es) loaded.'
    ),
    renderGlanceCard(
      'Working Style',
      'Guided + JSON',
      'Start with the builder for common paths, then drop into JSON only when you need full RPC fidelity.'
    ),
    renderGlanceCard(
      'Best Next Step',
      STATE.selectedTable ? 'Inspect indexes or describe' : 'Select a target table',
      STATE.selectedTable ? 'Use Describe or Load Indexes to turn the current tree selection into a live schema snapshot.' : 'Once a table is active, structure, index, and view workflows will prefill automatically.'
    )
  ].join('');
}

function renderCdcGlance() {
  const host = $('cdcSummaryBar');
  if (!host) return;
  const stats = cdcSubscriptionStats();
  const selected = stats.selected ? cdcSubscriptionLabel(stats.selected) : 'No active subscription';
  const latestPrepared = latestPreparedQuery();
  host.innerHTML = [
    renderGlanceCard(
      'Subscriptions',
      stats.total ? String(stats.total) + ' active' : 'No feeds yet',
      stats.total ? (stats.tableCount + ' table feed(s) and ' + stats.queryCount + ' query feed(s) tracked in this browser session.') : 'Start with a table feed or reuse the latest prepared query for invalidation-based CDC.'
    ),
    renderGlanceCard(
      'Selected Feed',
      selected,
      stats.selected ? ('ACK and poll actions operate on ' + selected + '.') : 'Choose a subscription after creating one to inspect lag and recent events.'
    ),
    renderGlanceCard(
      'Prepared Handoff',
      latestPrepared ? latestPrepared.query_id : 'None prepared',
      latestPrepared ? 'The latest prepared query can be promoted into a query-scoped CDC feed with one click.' : 'Prepare a query in Workspace first if you want invalidation tied to query semantics.'
    ),
    renderGlanceCard(
      'Selection Seed',
      currentSelectionLabel(),
      STATE.selectedTable ? 'The current table selection can seed table subscriptions immediately.' : 'Pick a table from the tree to prefill database and table subscription targets.'
    )
  ].join('');
}

function refreshDashboardSummaries() {
  renderOverviewHero();
  renderWorkspaceGlance();
  renderSchemaGlance();
  renderCdcGlance();
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

function parseLitArgsInput(raw, label) {
  const parsed = parseJsonInput(raw || '', label);
  if (parsed === null) return [];
  if (!Array.isArray(parsed)) throw new Error(label + ' must be an array');
  return parsed;
}

function parseOptionalU64Input(id, label) {
  const raw = $(id) ? $(id).value.trim() : '';
  if (!raw) return undefined;
  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(label + ' must be a non-negative integer');
  }
  return value;
}

function readViewRef() {
  const db = $('viewDb')?.value.trim();
  const table = $('viewName')?.value.trim();
  if (!db || !table) throw new Error('DB+name required');
  return { db, table };
}

function setViewSummaryMarkup(html) {
  const el = $('viewSummary');
  if (!el) return;
  el.innerHTML = html || 'Load view status or dependency details to inspect freshness and column usage.';
}

function viewColumnsLabel(columns) {
  return Array.isArray(columns) && columns.length ? columns.join(', ') : 'none';
}

function renderViewDependency(dep) {
  const db = dep && dep.db ? String(dep.db) : '?';
  const table = dep && dep.table ? String(dep.table) : '?';
  const columns = viewColumnsLabel(dep && dep.columns);
  const projection = viewColumnsLabel(dep && dep.projection_columns);
  const predicate = viewColumnsLabel(dep && dep.predicate_columns);
  const groupBy = viewColumnsLabel(dep && dep.group_by_columns);
  return '<div class="callout" style="margin-top:8px">'
    + '<strong>' + escapeHtml(db + '.' + table) + '</strong>'
    + '<br><strong>All columns</strong>: ' + escapeHtml(columns)
    + '<br><strong>Projection</strong>: ' + escapeHtml(projection)
    + '<br><strong>Predicate</strong>: ' + escapeHtml(predicate)
    + '<br><strong>Group By</strong>: ' + escapeHtml(groupBy)
    + '</div>';
}

function renderViewSummary(result, mode) {
  if (!result || typeof result !== 'object') {
    setViewSummaryMarkup('Load view status or dependency details to inspect freshness and column usage.');
    return;
  }
  if (mode === 'status') {
    const views = Array.isArray(result.views) ? result.views : [];
    if (!views.length) {
      setViewSummaryMarkup('No view status returned.');
      return;
    }
    setViewSummaryMarkup(views.map((view) => {
      const deps = Array.isArray(view.deps) ? view.deps : [];
      const lastRefresh = Number(view.last_refresh_ms || 0);
      return '<div><strong>' + escapeHtml(String(view.db || '?') + '.' + String(view.view || '?')) + '</strong>'
        + ' | <strong>Rows</strong>: ' + escapeHtml(String(Number(view.rows) || 0))
        + ' | <strong>Stale</strong>: ' + escapeHtml(view.stale ? 'yes' : 'no')
        + ' | <strong>Mode</strong>: ' + escapeHtml(String(view.last_refresh_mode || 'unknown'))
        + ' | <strong>Last refresh</strong>: ' + escapeHtml(lastRefresh ? formatAuditTimestamp(lastRefresh) : 'never')
        + deps.map(renderViewDependency).join('')
        + '</div>';
    }).join('<hr>'));
    return;
  }
  if (mode === 'deps') {
    const deps = Array.isArray(result.deps) ? result.deps : [];
    if (!deps.length) {
      setViewSummaryMarkup('No dependency details returned.');
      return;
    }
    setViewSummaryMarkup(deps.map((dep) => {
      if (dep && dep.transitive) {
        const items = Array.isArray(dep.transitive) ? dep.transitive : [];
        return '<div class="callout" style="margin-top:8px"><strong>Transitive deps</strong><br>'
          + items.map((item) => escapeHtml(String(item.path || '?')) + ' (' + escapeHtml(item.stale ? 'stale' : 'fresh') + ')').join('<br>')
          + '</div>';
      }
      return renderViewDependency(dep);
    }).join(''));
    return;
  }
  if (mode === 'create') {
    setViewSummaryMarkup('<strong>View created.</strong> Columns: ' + escapeHtml(viewColumnsLabel(result.columns)));
    return;
  }
  if (mode === 'refresh') {
    setViewSummaryMarkup('<strong>Refresh complete.</strong> Mode: ' + escapeHtml(String(result.mode || 'unknown'))
      + ' | <strong>Rows</strong>: ' + escapeHtml(String(Number(result.rows) || 0))
      + ' | <strong>Last change seq</strong>: ' + escapeHtml(String(Number(result.last_change_seq) || 0)));
    return;
  }
  if (mode === 'evaluate') {
    setViewSummaryMarkup('<strong>Evaluation complete.</strong> Correct: ' + escapeHtml(result.correct ? 'yes' : 'no')
      + ' | <strong>Pending changes</strong>: ' + escapeHtml(String(Number(result.pending_changes) || 0))
      + ' | <strong>Recommended</strong>: ' + escapeHtml(String(result.recommended_mode || 'unknown'))
      + ' | <strong>Speedup vs full</strong>: ' + escapeHtml(Number(result.speedup_vs_full || 0).toFixed(2)) + 'x'
      + (result.incremental_error ? '<br><strong>Incremental error</strong>: ' + escapeHtml(String(result.incremental_error)) : ''));
    return;
  }
  if (mode === 'drop') {
    setViewSummaryMarkup('<strong>View dropped.</strong>');
  }
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

function renderQueryResultTable(targetId, result) {
  const data = result && result.data;
  if (!data || !Array.isArray(data.columns) || !Array.isArray(data.rows)) {
    renderTable(targetId, [], []);
    return;
  }
  renderTable(targetId, data.columns.map(normalizeSqlColumnName), data.rows);
}

function findRpcTemplate(method) {
  return RPC_TEMPLATES.find((tpl) => tpl.method === method) || null;
}

function openRpcMethod(method, fallbackParams) {
  if ($('rpcMethod')) $('rpcMethod').value = method;
  const templateIndex = RPC_TEMPLATES.findIndex((tpl) => tpl.method === method);
  if ($('rpcTemplate')) $('rpcTemplate').value = templateIndex >= 0 ? String(templateIndex) : '';
  const params = templateIndex >= 0 ? RPC_TEMPLATES[templateIndex].params : (fallbackParams || {});
  if ($('rpcParams')) $('rpcParams').value = JSON.stringify(params, null, 2);
  setActivePanel('rpc', true);
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

function formatAdvisorHistoryState(entry) {
  if (!entry) return 'unknown';
  const bits = [entry.action || 'action'];
  if (entry.status) bits.push(entry.status);
  if (Number.isFinite(entry.progress_pct)) bits.push(String(entry.progress_pct) + '%');
  if (entry.result_status) bits.push(entry.result_status);
  if (entry.rollback_status) bits.push('rollback=' + entry.rollback_status);
  return bits.join(' | ');
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
        historyEntry ? ('latest action: ' + formatAdvisorHistoryState(historyEntry) + ' at ' + formatAdvisorTime(historyEntry.updated_at_ms || historyEntry.created_at_ms)) : 'latest action: not yet applied'
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
        formatAdvisorHistoryState(entry) + ' at ' + formatAdvisorTime(entry.updated_at_ms || entry.created_at_ms),
        [
          entry.table && entry.table.db ? (entry.table.db + '.' + entry.table.table) : 'unknown table',
          entry.note || 'no note'
        ],
        'Before\n' +
          'suggestion id: ' + (entry.suggestion_id || 'n/a') + '\n' +
          'action id: ' + (entry.id || 'n/a'),
        'After\n' +
          'recorded action: ' + formatAdvisorHistoryState(entry) + '\n' +
          'selection: ' + advisorLabel(entry.columns, entry.include) + '\n' +
          'error: ' + (entry.error || 'none'),
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

  const lookup = storage.value_lookup || {};
  const topBucket = Array.isArray(lookup.top_buckets) && lookup.top_buckets.length ? lookup.top_buckets[0] : null;
  setStat('statLookupTotal', formatNumber(lookup.total_lookups));
  setStat('statLookupBuckets', lookup.non_empty_buckets !== undefined ? String(lookup.non_empty_buckets) : '--');
  setStat('statLookupShift', Number.isFinite(lookup.model_shift_l1) ? lookup.model_shift_l1.toFixed(3) : '--');
  setStat('statLookupTop', topBucket ? (topBucket.prefix_hex + ' / ' + (Number.isFinite(topBucket.share) ? (topBucket.share * 100).toFixed(1) + '%' : formatNumber(topBucket.count))) : '--');
  const learned = storage.learned_index || {};
  setStat('statLearnedBuilt', learned.enabled === false ? 'disabled' : (learned.built ? 'built' : 'pending'));
  setStat('statLearnedSegments', formatNumber(learned.segment_count));
  setStat('statLearnedKeys', formatNumber(learned.total_keys));
  setStat('statLearnedError', learned.max_error !== undefined ? String(learned.max_error) : '--');
  setStat('statLearnedWindow', learned.max_search_window !== undefined ? String(learned.max_search_window) : '--');
  setStat('statLearnedBytes', formatBytes((Number(learned.approx_model_bytes) || 0) + (Number(learned.approx_fallback_bytes) || 0)));

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
  STATE.schemaLastIndexCount = null;
  STATE.schemaLastIndexDb = '';
  STATE.schemaLastIndexTable = '';
  STATE.replayHistoryStatus = null;
  STATE.replayLastBundle = null;
  STATE.edgeLastBundle = null;
  STATE.replayImports = [];
  STATE.replaySelectedWorkspaceId = '';
  STATE.replayLastRun = null;
  setSelectedDb(''); setSelectedTable('');
  renderDbTree({}, '');
  dataFormApplyColumns([], []);
  easyApplyColumns([], []);
  easyRefreshTargetsFromTree();
  renderTable('browseTable', [], []); renderTable('easyDataGrid', [], []); renderTable('structureTable', [], []); renderTable('sqlTable', [], []);
  renderTable('ttResultGrid', [], []); renderTable('historyTableGrid', [], []); renderTable('replayManifestTable', [], []); renderTable('replayIntegrityTable', [], []);
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
      renderSchemaIndexes(result);
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

function readSchemaEvolutionContext() {
  const db = validateEasyIdentifier($('schemaDb')?.value.trim(), 'Database');
  const table = validateEasyIdentifier($('schemaTable')?.value.trim(), 'Table');
  return { db, table };
}

function readSchemaEvolutionChanges() {
  const parsed = parseJsonInput($('schemaEvolutionChanges')?.value || '', 'Change ops');
  if (!Array.isArray(parsed) || !parsed.length) {
    throw new Error('Change ops must be a non-empty JSON array');
  }
  return parsed;
}

function readSchemaEvolutionChangeIds() {
  const raw = $('schemaEvolutionChangeIds')?.value.trim() || '';
  return raw ? parseIdentifierList(raw, 'Change IDs') : undefined;
}

function setSchemaEvolutionSummary(text) {
  const host = $('schemaEvolutionSummary');
  if (host) host.textContent = text;
}

function renderSchemaEvolutionSummary(result, mode) {
  if (!result) {
    setSchemaEvolutionSummary('Review divergence, rollout stages, and merge results for the active table.');
    return;
  }
  if ((mode === 'status' || mode === 'rollout') && $('schemaEvolutionBaseVersion')) {
    $('schemaEvolutionBaseVersion').value = String(result.current_version ?? '');
  }
  if (mode === 'apply' && $('schemaEvolutionBaseVersion')) {
    $('schemaEvolutionBaseVersion').value = String(result.new_version ?? '');
  }
  if (mode === 'propose') {
    setSchemaEvolutionSummary('Queued ' + (result.change_id || 'pending change') + ' with status ' + (result.status || 'pending') + '.');
    return;
  }
  if (mode === 'status') {
    const pending = Array.isArray(result.pending) ? result.pending.length : 0;
    const mergePlan = Array.isArray(result.merge_plan) ? result.merge_plan.length : 0;
    const conflicts = Array.isArray(result.conflicts) ? result.conflicts.length : 0;
    setSchemaEvolutionSummary('Current version ' + (result.current_version ?? '--') + '; pending ' + pending + '; merge plan ' + mergePlan + '; conflicts ' + conflicts + '.');
    return;
  }
  if (mode === 'rollout') {
    const stages = Array.isArray(result.stages) ? result.stages.length : 0;
    const ready = result.ready_for_rollout ? 'ready' : 'blocked';
    setSchemaEvolutionSummary('Target version ' + (result.target_version ?? '--') + ' across ' + (result.nodes ?? '--') + ' node(s); ' + ready + '; stages ' + stages + '; legacy rows ' + (result.legacy_row_count ?? 0) + '.');
    return;
  }
  if (mode === 'apply') {
    const applied = Array.isArray(result.applied) ? result.applied.length : 0;
    const rolledBack = Array.isArray(result.rolled_back) ? result.rolled_back.length : 0;
    const conflicts = Array.isArray(result.conflicts) ? result.conflicts.length : 0;
    setSchemaEvolutionSummary('Applied ' + applied + ' change(s); rolled back ' + rolledBack + '; remaining conflicts ' + conflicts + '; new version ' + (result.new_version ?? '--') + '.');
  }
}

async function schemaProposeChange() {
  try {
    const table = readSchemaEvolutionContext();
    const changes = readSchemaEvolutionChanges();
    const message = $('schemaEvolutionMessage')?.value.trim();
    const baseVersion = parseOptionalU64Input('schemaEvolutionBaseVersion', 'Base version');
    const res = await call('schema.propose_change', cleanParams({ table, base_version: baseVersion ?? 0, changes, message }), 'schemaEvolutionOut');
    renderSchemaEvolutionSummary(unwrapRpcResult(res, 'schema.propose_change'), 'propose');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaEvolutionOut');
    setSchemaEvolutionSummary(String(e));
  }
}

async function schemaMergeStatus() {
  try {
    const table = readSchemaEvolutionContext();
    const res = await call('schema.merge_status', { table }, 'schemaEvolutionOut');
    renderSchemaEvolutionSummary(unwrapRpcResult(res, 'schema.merge_status'), 'status');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaEvolutionOut');
    setSchemaEvolutionSummary(String(e));
  }
}

async function schemaSimulateRollout() {
  try {
    const table = readSchemaEvolutionContext();
    const nodes = parseOptionalU64Input('schemaEvolutionNodes', 'Rollout nodes');
    const res = await call('schema.simulate_rollout', cleanParams({ table, nodes }), 'schemaEvolutionOut');
    renderSchemaEvolutionSummary(unwrapRpcResult(res, 'schema.simulate_rollout'), 'rollout');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaEvolutionOut');
    setSchemaEvolutionSummary(String(e));
  }
}

async function schemaApplyMerge() {
  try {
    const table = readSchemaEvolutionContext();
    const changeIds = readSchemaEvolutionChangeIds();
    const res = await call('schema.apply_merge', cleanParams({ table, change_ids: changeIds }), 'schemaEvolutionOut');
    renderSchemaEvolutionSummary(unwrapRpcResult(res, 'schema.apply_merge'), 'apply');
    await schemaDescribe();
  } catch (e) {
    setOut({ error: String(e) }, 'schemaEvolutionOut');
    setSchemaEvolutionSummary(String(e));
  }
}

function readSchemaIndexContext() {
  const db = validateEasyIdentifier($('schemaDb')?.value.trim(), 'Database');
  const table = validateEasyIdentifier($('schemaTable')?.value.trim(), 'Table');
  return { db, table };
}

function parseIdentifierList(raw, label) {
  const parts = String(raw || '').split(',').map((part) => part.trim()).filter(Boolean);
  if (!parts.length) throw new Error(label + ' required');
  return parts.map((part) => validateEasyIdentifier(part, label));
}

function renderSchemaIndexes(result) {
  const indexes = Array.isArray(result?.indexes) ? result.indexes : [];
  const db = result?.db || $('schemaDb')?.value.trim() || '--';
  const table = result?.table || $('schemaTable')?.value.trim() || '--';
  STATE.schemaLastIndexCount = indexes.length;
  STATE.schemaLastIndexDb = db;
  STATE.schemaLastIndexTable = table;
  const summary = $('schemaIndexSummary');
  if (summary) {
    summary.innerHTML = '<strong>' + escapeHtml(db + '.' + table) + '</strong> has '
      + escapeHtml(String(indexes.length)) + ' secondary index(es).';
  }
  if (!indexes.length) {
    renderTable('schemaIndexTable', ['Name', 'Columns', 'Unique'], [['--', 'No secondary indexes defined', '--']]);
    refreshDashboardSummaries();
    return;
  }
  renderTable('schemaIndexTable', ['Name', 'Columns', 'Unique'], indexes.map((idx) => [
    idx.name || '',
    Array.isArray(idx.columns) ? idx.columns.join(', ') : '',
    idx.unique ? 'YES' : 'NO',
  ]));
  refreshDashboardSummaries();
}

async function schemaLoadIndexes() {
  try {
    const ctx = readSchemaIndexContext();
    const res = await call('schema.describe_table', ctx, 'schemaIndexOut');
    const result = unwrapRpcResult(res, 'schema.describe_table');
    renderSchemaIndexes(result);
    setOut(result, 'schemaIndexOut');
  } catch (e) {
    renderTable('schemaIndexTable', [], []);
    setOut({ error: String(e) }, 'schemaIndexOut');
  }
}

async function schemaCreateIndex() {
  try {
    const ctx = readSchemaIndexContext();
    const indexName = validateEasyIdentifier($('schemaIndexName')?.value.trim(), 'Index name');
    const columns = parseIdentifierList($('schemaIndexColumns')?.value.trim(), 'Columns');
    const unique = $('schemaIndexUnique')?.value === 'true';
    const sql = 'CREATE ' + (unique ? 'UNIQUE ' : '') + 'INDEX ' + indexName + ' ON ' + ctx.db + '.' + ctx.table + ' (' + columns.join(', ') + ');';
    const res = await call('sql.exec', { sql, default_db: ctx.db }, 'schemaIndexOut');
    const result = unwrapRpcResult(res, 'sql.exec');
    setOut({ sql, result }, 'schemaIndexOut');
    await schemaLoadIndexes();
    showToast('Index ' + indexName + ' created on ' + ctx.db + '.' + ctx.table + '.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaIndexOut');
  }
}

async function schemaDropIndex() {
  try {
    const ctx = readSchemaIndexContext();
    const indexName = validateEasyIdentifier($('schemaIndexName')?.value.trim(), 'Index name');
    const ok = await skeinModal('⚠️', 'Drop Index', 'Drop index <b>' + escapeHtml(indexName) + '</b> from <b>' + escapeHtml(ctx.db + '.' + ctx.table) + '</b>?', [
      { label: 'Cancel', value: false },
      { label: 'Drop', value: true, cls: 'primary' },
    ]);
    if (!ok) return;
    const sql = 'DROP INDEX ' + indexName + ' ON ' + ctx.db + '.' + ctx.table + ';';
    const res = await call('sql.exec', { sql, default_db: ctx.db }, 'schemaIndexOut');
    const result = unwrapRpcResult(res, 'sql.exec');
    setOut({ sql, result }, 'schemaIndexOut');
    await schemaLoadIndexes();
    showToast('Index ' + indexName + ' dropped.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'schemaIndexOut');
  }
}

function schemaUseSelectedTable() {
  if (!STATE.selectedDb || !STATE.selectedTable) {
    setOut({ error: 'Select a table from the tree first.' }, 'schemaIndexOut');
    return;
  }
  if ($('schemaDb')) $('schemaDb').value = STATE.selectedDb;
  if ($('schemaTable')) $('schemaTable').value = STATE.selectedTable;
  schemaLoadIndexes();
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

// ----------------------------------------------------------------
// WYSIWYG Design tab (Easy Viewer -> Design)
// Loads a table's columns via schema.describe_table, lets the user
// rename / retype / add / drop / mark-nullable, then computes an
// ALTER TABLE plan by diffing original vs draft and applies it via
// sql.exec one statement at a time.
// ----------------------------------------------------------------

const EASY_DESIGN_KINDS = ['string', 'i64', 'f64', 'bool', 'json', 'bytes', 'time', 'date', 'datetime', 'decimal'];

function easyDesignSetState(message) {
  const el = $('easyDesignStatus');
  if (el) el.textContent = message;
}

function easyDesignNormalizeColumn(col, primaryKeySet) {
  return {
    original_name: col.name,
    name: col.name,
    kind: (col.type && col.type.kind) ? String(col.type.kind) : 'string',
    nullable: !!col.nullable,
    primary: primaryKeySet.has(col.name),
    auto_increment: !!col.auto_increment,
    default: col.default == null ? '' : (typeof col.default === 'object' ? JSON.stringify(col.default) : String(col.default)),
    action: 'keep' // 'keep' | 'drop'
  };
}

async function easyDesignLoad() {
  try {
    const ref = easyReadTableRef();
    const res = await call('schema.describe_table', { db: ref.db, table: ref.table }, 'easyDesignOut');
    const result = unwrapRpcResult(res, 'schema.describe_table');
    const pk = new Set(Array.isArray(result.primary_key) ? result.primary_key : []);
    const cols = (result.columns || []).map((col) => easyDesignNormalizeColumn(col, pk));
    STATE.easyDesignTable = { db: ref.db, table: ref.table };
    STATE.easyDesignOriginal = JSON.parse(JSON.stringify(cols));
    STATE.easyDesignDraft = cols;
    easyDesignSetState('Loaded ' + ref.db + '.' + ref.table + ' (' + cols.length + ' columns).');
    easyDesignRender();
    easyDesignRefreshPreview();
    setOut({ ok: true, loaded: ref.db + '.' + ref.table, columns: cols.length }, 'easyDesignOut');
  } catch (e) {
    easyShowToast('Design load failed: ' + e.message, 'error');
    setOut({ error: e.message }, 'easyDesignOut');
  }
}

function easyDesignRender() {
  const tbody = $('easyDesignRows');
  if (!tbody) return;
  const rows = STATE.easyDesignDraft || [];
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="9" class="hint" style="padding:8px">No columns loaded. Click "Load selected table" to begin.</td></tr>';
    return;
  }
  const kindOptions = EASY_DESIGN_KINDS.map((k) => '<option value="' + k + '">' + k + '</option>').join('');
  tbody.innerHTML = rows.map((row, idx) => {
    const isNew = row.original_name == null;
    const dropped = row.action === 'drop';
    const rowStyle = dropped ? 'opacity:0.45;text-decoration:line-through' : (isNew ? 'background:rgba(0,160,80,0.08)' : '');
    const orig = isNew ? '<em>(new)</em>' : escapeHtml(row.original_name);
    const kindSelect = '<select data-design-idx="' + idx + '" data-design-field="kind">' +
      EASY_DESIGN_KINDS.map((k) => '<option value="' + k + '"' + (row.kind === k ? ' selected' : '') + '>' + k + '</option>').join('') + '</select>';
    const action = isNew
      ? '<span class="hint">new</span>'
      : (dropped
          ? '<button class="sm" data-design-idx="' + idx + '" data-design-action="undrop">Undo drop</button>'
          : '<button class="sm" data-design-idx="' + idx + '" data-design-action="drop">Drop</button>');
    const removeBtn = isNew
      ? '<button class="ghost sm" data-design-idx="' + idx + '" data-design-action="remove">Remove</button>'
      : '';
    return '<tr style="' + rowStyle + '">' +
      '<td>' + orig + '</td>' +
      '<td><input type="text" data-design-idx="' + idx + '" data-design-field="name" value="' + escapeHtml(row.name) + '"' + (dropped ? ' disabled' : '') + ' /></td>' +
      '<td>' + kindSelect + '</td>' +
      '<td style="text-align:center"><input type="checkbox" data-design-idx="' + idx + '" data-design-field="nullable"' + (row.nullable ? ' checked' : '') + (dropped ? ' disabled' : '') + ' /></td>' +
      '<td><input type="text" data-design-idx="' + idx + '" data-design-field="default" value="' + escapeHtml(row.default || '') + '" placeholder="(none)"' + (dropped ? ' disabled' : '') + ' /></td>' +
      '<td style="text-align:center"><input type="checkbox" data-design-idx="' + idx + '" data-design-field="auto_increment"' + (row.auto_increment ? ' checked' : '') + (dropped ? ' disabled' : '') + ' /></td>' +
      '<td style="text-align:center"><input type="checkbox" data-design-idx="' + idx + '" data-design-field="primary"' + (row.primary ? ' checked' : '') + (dropped ? ' disabled' : '') + ' /></td>' +
      '<td>' + action + '</td>' +
      '<td>' + removeBtn + '</td>' +
      '</tr>';
  }).join('');
  // wire input/change handlers
  tbody.querySelectorAll('[data-design-field]').forEach((el) => {
    const idx = parseInt(el.getAttribute('data-design-idx'), 10);
    const field = el.getAttribute('data-design-field');
    const handler = () => {
      const row = STATE.easyDesignDraft[idx];
      if (!row) return;
      if (el.type === 'checkbox') row[field] = !!el.checked;
      else row[field] = el.value;
      easyDesignRefreshPreview();
    };
    el.addEventListener('input', handler);
    el.addEventListener('change', handler);
  });
  tbody.querySelectorAll('[data-design-action]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const idx = parseInt(btn.getAttribute('data-design-idx'), 10);
      const action = btn.getAttribute('data-design-action');
      const row = STATE.easyDesignDraft[idx];
      if (!row) return;
      if (action === 'drop') row.action = 'drop';
      else if (action === 'undrop') row.action = 'keep';
      else if (action === 'remove') STATE.easyDesignDraft.splice(idx, 1);
      easyDesignRender();
      easyDesignRefreshPreview();
    });
  });
}

function easyDesignAddColumn() {
  if (!STATE.easyDesignTable) {
    easyShowToast('Load a table first.', 'info');
    return;
  }
  STATE.easyDesignDraft = STATE.easyDesignDraft || [];
  STATE.easyDesignDraft.push({
    original_name: null,
    name: 'new_col_' + (STATE.easyDesignDraft.length + 1),
    kind: 'string',
    nullable: true,
    primary: false,
    auto_increment: false,
    default: '',
    action: 'keep'
  });
  easyDesignRender();
  easyDesignRefreshPreview();
}

function easyDesignReset() {
  if (!STATE.easyDesignOriginal) {
    easyShowToast('Nothing to reset.', 'info');
    return;
  }
  STATE.easyDesignDraft = JSON.parse(JSON.stringify(STATE.easyDesignOriginal));
  easyDesignRender();
  easyDesignRefreshPreview();
}

function easyDesignSqlType(kind) {
  const k = String(kind || 'string').toLowerCase();
  switch (k) {
    case 'i64': return 'BIGINT';
    case 'f64': return 'DOUBLE';
    case 'bool': return 'BOOLEAN';
    case 'json': return 'JSON';
    case 'bytes': return 'BLOB';
    case 'time': return 'TIME';
    case 'date': return 'DATE';
    case 'datetime': return 'DATETIME';
    case 'decimal': return 'DECIMAL';
    case 'string':
    default:
      return 'VARCHAR(255)';
  }
}

function easyDesignColumnSpec(row) {
  let spec = quoteIdent(row.name) + ' ' + easyDesignSqlType(row.kind);
  if (!row.nullable) spec += ' NOT NULL';
  if (row.auto_increment) spec += ' AUTO_INCREMENT';
  if (row.default && row.default.trim()) spec += ' DEFAULT ' + row.default.trim();
  return spec;
}

function quoteIdent(name) {
  // Conservative backtick quoting matching MySQL identifier rules.
  return '`' + String(name || '').replace(/`/g, '``') + '`';
}

function easyDesignBuildAlterPlan() {
  const ref = STATE.easyDesignTable;
  if (!ref) return { statements: [], summary: 'No table loaded.' };
  const original = STATE.easyDesignOriginal || [];
  const draft = STATE.easyDesignDraft || [];
  const tableRef = quoteIdent(ref.db) + '.' + quoteIdent(ref.table);
  const stmts = [];
  const notes = [];
  const seenOriginals = new Set();

  draft.forEach((row) => {
    const name = (row.name || '').trim();
    if (row.original_name == null) {
      if (row.action === 'drop') return; // never created
      if (!name) { notes.push('-- skipped: new column missing name'); return; }
      stmts.push('ALTER TABLE ' + tableRef + ' ADD COLUMN ' + easyDesignColumnSpec(row) + ';');
      return;
    }
    seenOriginals.add(row.original_name);
    if (row.action === 'drop') {
      stmts.push('ALTER TABLE ' + tableRef + ' DROP COLUMN ' + quoteIdent(row.original_name) + ';');
      return;
    }
    if (!name) { notes.push('-- skipped: column ' + row.original_name + ' has empty name'); return; }
    const orig = original.find((o) => o.original_name === row.original_name);
    if (!orig) return;
    const renamed = row.original_name !== name;
    const retyped = orig.kind !== row.kind
      || orig.nullable !== row.nullable
      || orig.auto_increment !== row.auto_increment
      || (orig.default || '') !== (row.default || '');
    if (renamed && retyped) {
      stmts.push('ALTER TABLE ' + tableRef + ' CHANGE COLUMN ' + quoteIdent(row.original_name) + ' ' + easyDesignColumnSpec(row) + ';');
    } else if (renamed) {
      stmts.push('ALTER TABLE ' + tableRef + ' RENAME COLUMN ' + quoteIdent(row.original_name) + ' TO ' + quoteIdent(name) + ';');
    } else if (retyped) {
      stmts.push('ALTER TABLE ' + tableRef + ' MODIFY COLUMN ' + easyDesignColumnSpec(row) + ';');
    }
  });

  // Detect implicit drops (rows removed entirely from draft).
  original.forEach((orig) => {
    if (!seenOriginals.has(orig.original_name)) {
      stmts.push('ALTER TABLE ' + tableRef + ' DROP COLUMN ' + quoteIdent(orig.original_name) + ';');
    }
  });

  const summary = stmts.length
    ? stmts.length + ' statement(s) planned.'
    : 'No changes.';
  return { statements: stmts, notes, summary };
}

function easyDesignRefreshPreview() {
  const el = $('easyDesignPreview');
  if (!el) return;
  if (!STATE.easyDesignTable) { el.textContent = '-- Load a table to begin.'; return; }
  const plan = easyDesignBuildAlterPlan();
  const body = plan.statements.length ? plan.statements.join('\n') : '-- No changes.';
  const notes = (plan.notes && plan.notes.length) ? '\n\n' + plan.notes.join('\n') : '';
  el.textContent = '-- ' + plan.summary + '\n' + body + notes;
}

async function easyDesignApply() {
  try {
    if (!STATE.easyDesignTable) throw new Error('Load a table first');
    const plan = easyDesignBuildAlterPlan();
    if (!plan.statements.length) {
      easyShowToast('No changes to apply.', 'info');
      return;
    }
    if (typeof window !== 'undefined' && typeof window.confirm === 'function') {
      const ok = window.confirm('Apply ' + plan.statements.length + ' ALTER TABLE statement(s) to '
        + STATE.easyDesignTable.db + '.' + STATE.easyDesignTable.table + '?');
      if (!ok) return;
    }
    const results = [];
    for (const sql of plan.statements) {
      const res = await call('sql.exec', cleanParams({ sql, default_db: STATE.easyDesignTable.db }), 'easyDesignOut');
      try {
        unwrapRpcResult(res, 'sql.exec');
      } catch (err) {
        results.push({ sql, ok: false, error: err.message });
        setOut({ applied: results, halted_on: sql, error: err.message }, 'easyDesignOut');
        easyShowToast('ALTER halted: ' + err.message, 'error');
        return;
      }
      results.push({ sql, ok: true });
    }
    easyShowToast('\u2713 Applied ' + results.length + ' ALTER statement(s).', 'success');
    setOut({ ok: true, applied: results }, 'easyDesignOut');
    await easyDesignLoad();
  } catch (e) {
    easyShowToast('Apply failed: ' + e.message, 'error');
    setOut({ error: e.message }, 'easyDesignOut');
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
    const args = parseLitArgsInput($('skeinArgs').value, 'Args');
    const qid = $('skeinQueryId').value.trim();
    const baseEtag = $('skeinBaseEtag').value.trim();
    const incFull = $('skeinIncludeFull').value === 'true';
    const params = {};
    if (method === 'query.execute_prepared') { if (!qid) throw new Error('Query id required'); params.query_id = qid; if (args.length) params.args = args; if (format) params.result_format = format; }
    else if (method === 'query.prepare') { const q = parseJsonInput($('skeinQuery').value,'Query'); if (!q) throw new Error('Query required'); params.query = q; }
    else { const q = parseJsonInput($('skeinQuery').value,'Query'); if (!q) throw new Error('Query required'); params.query = q; if (args.length) params.args = args; if (format) params.result_format = format; if (method === 'query.patch') { if (baseEtag) params.base_etag = baseEtag; params.include_full = incFull; } }
    const res = await call(method, params, 'skeinOut');
    if (method === 'query.prepare' && res && res.json && res.json.ok && res.json.result) {
      const queryId = res.json.result.query_id || '';
      const argsJson = ($('skeinArgs')?.value || '').trim() || '[]';
      setPreparedQueryFields(queryId, argsJson, true);
      rememberPreparedQuery({
        query_id: queryId,
        canonical: res.json.result.canonical || '',
        args_json: argsJson,
        created_at_ms: Date.now(),
      });
      renderPreparedWorkspace();
    }
    if (method === 'query.execute_prepared') {
      const argsJson = ($('skeinArgs')?.value || '').trim() || '[]';
      setPreparedQueryFields(qid, argsJson, false);
      renderPreparedWorkspace();
    }
  } catch (e) { setOut({error:String(e)},'skeinOut'); }
}

function preparedEndpointUrl(queryId) {
  return getBaseUrl().replace(/\/$/, '') + '/api/v1/q/' + encodeURIComponent(queryId);
}

function setPreparedQueryFields(queryId, argsJson, overwriteArgs) {
  if (queryId) {
    if ($('skeinQueryId')) $('skeinQueryId').value = queryId;
    if ($('preparedQueryId')) $('preparedQueryId').value = queryId;
    if ($('cdcQueryId') && !$('cdcQueryId').value.trim()) $('cdcQueryId').value = queryId;
  }
  if (overwriteArgs && $('preparedArgs')) $('preparedArgs').value = argsJson || '[]';
}

function latestPreparedQuery() {
  return Array.isArray(STATE.preparedQueries) && STATE.preparedQueries.length ? STATE.preparedQueries[0] : null;
}

function rememberPreparedQuery(entry) {
  if (!entry || !entry.query_id) return;
  STATE.preparedQueries = (STATE.preparedQueries || []).filter((item) => item.query_id !== entry.query_id);
  STATE.preparedQueries.unshift(entry);
  if (STATE.preparedQueries.length > 10) STATE.preparedQueries = STATE.preparedQueries.slice(0, 10);
  renderPreparedWorkspace();
}

function renderPreparedWorkspace() {
  const summary = $('preparedSummary');
  const host = $('preparedQueryList');
  const latest = latestPreparedQuery();
  if (summary) {
    if (!latest) {
      summary.innerHTML = 'Prepare a SkeinQL query to unlock cacheable GET and query-scoped CDC workflows.';
    } else {
      const argsRaw = ($('preparedArgs')?.value || latest.args_json || '').trim() || '[]';
      const getHint = argsRaw === '[]'
        ? 'GET endpoint ready: ' + preparedEndpointUrl(latest.query_id)
        : 'GET endpoint is only valid for zero-arg prepared queries; use RPC for parameterized execution.';
      summary.innerHTML = '<strong>Active query</strong>: ' + escapeHtml(latest.query_id)
        + ' | <strong>Prepared</strong>: ' + escapeHtml(formatUiTimestamp(Number(latest.created_at_ms) || 0))
        + '<br>' + escapeHtml(getHint);
    }
  }
  if (host) {
    host.textContent = '';
    if (!STATE.preparedQueries.length) {
      host.textContent = 'No prepared queries in this browser session yet.';
    } else {
      STATE.preparedQueries.forEach((entry) => {
        const btn = document.createElement('button');
        btn.className = 'settings-key-btn sm';
        btn.textContent = entry.query_id;
        btn.addEventListener('click', () => {
          setPreparedQueryFields(entry.query_id, entry.args_json || '[]', true);
          renderPreparedWorkspace();
        });
        host.appendChild(btn);
      });
    }
  }
  refreshDashboardSummaries();
}

async function preparedPrepareCurrentQuery() {
  try {
    const query = parseJsonInput($('skeinQuery')?.value || '', 'Query');
    if (!query) throw new Error('Query required');
    const argsJson = ($('skeinArgs')?.value || '').trim() || '[]';
    const res = await call('query.prepare', { query }, 'preparedOut');
    const result = unwrapRpcResult(res, 'query.prepare');
    setPreparedQueryFields(result.query_id || '', argsJson, true);
    rememberPreparedQuery({
      query_id: result.query_id || '',
      canonical: result.canonical || '',
      args_json: argsJson,
      created_at_ms: Date.now(),
    });
    setOut(result, 'preparedOut');
    showToast('Prepared query ' + (result.query_id || 'created') + '.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'preparedOut');
  }
}

async function preparedExecuteCurrentQuery() {
  try {
    const queryId = ($('preparedQueryId')?.value || $('skeinQueryId')?.value || '').trim();
    if (!queryId) throw new Error('Prepared query id required');
    const argsRaw = ($('preparedArgs')?.value || '').trim() || '[]';
    const args = parseLitArgsInput(argsRaw, 'Prepared args');
    const format = $('skeinFormat')?.value || 'rows_json';
    const res = await call('query.execute_prepared', cleanParams({ query_id: queryId, args, result_format: format }), 'preparedOut');
    const result = unwrapRpcResult(res, 'query.execute_prepared');
    renderQueryResultTable('preparedResultGrid', result);
    setPreparedQueryFields(queryId, argsRaw, false);
    rememberPreparedQuery({
      query_id: queryId,
      canonical: latestPreparedQuery()?.canonical || '',
      args_json: argsRaw,
      created_at_ms: latestPreparedQuery()?.created_at_ms || Date.now(),
    });
    setOut(result, 'preparedOut');
    showToast('Prepared query executed.', 'success');
  } catch (e) {
    renderTable('preparedResultGrid', [], []);
    setOut({ error: String(e) }, 'preparedOut');
  }
}

async function preparedCopyGetUrl() {
  try {
    const queryId = ($('preparedQueryId')?.value || $('skeinQueryId')?.value || '').trim();
    if (!queryId) throw new Error('Prepared query id required');
    const args = parseLitArgsInput(($('preparedArgs')?.value || '').trim() || '[]', 'Prepared args');
    if (args.length) throw new Error('GET endpoint does not support args; clear Prepared Args or use RPC execution');
    await navigator.clipboard.writeText(preparedEndpointUrl(queryId));
    showToast('Prepared query GET URL copied.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'preparedOut');
  }
}

function preparedUseForCdc() {
  const latest = latestPreparedQuery();
  const queryId = ($('preparedQueryId')?.value || $('skeinQueryId')?.value || latest?.query_id || '').trim();
  const argsJson = ($('preparedArgs')?.value || latest?.args_json || '').trim() || '[]';
  if (!queryId) {
    setOut({ error: 'Prepare or select a query first.' }, 'preparedOut');
    return;
  }
  if ($('cdcQueryId')) $('cdcQueryId').value = queryId;
  if ($('cdcQueryArgs')) $('cdcQueryArgs').value = argsJson;
  setActivePanel('cdc', true);
  showToast('Prepared query sent to CDC query subscriptions.', 'info');
}

function renderTxState() {
  const input = $('txId');
  const summary = $('txSummary');
  if (input && STATE.txCurrentId && !input.value.trim()) input.value = STATE.txCurrentId;
  if (summary) {
    if (!STATE.txCurrentId) summary.innerHTML = 'No active transaction handle in this browser session.';
    else summary.innerHTML = '<strong>Active TX</strong>: ' + escapeHtml(STATE.txCurrentId)
      + ' | <strong>Read only</strong>: ' + escapeHtml(String(!!STATE.txReadOnly));
  }
  refreshDashboardSummaries();
}

async function txBegin() {
  try {
    const readOnly = $('txReadOnly')?.value === 'true';
    const res = await call('tx.begin', { read_only: readOnly }, 'txOut');
    const result = unwrapRpcResult(res, 'tx.begin');
    STATE.txCurrentId = result.tx_id || '';
    STATE.txReadOnly = readOnly;
    if ($('txId')) $('txId').value = STATE.txCurrentId;
    renderTxState();
    setOut(result, 'txOut');
    showToast('Transaction ' + STATE.txCurrentId + ' started.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'txOut');
  }
}

async function txCommit() {
  try {
    const txId = ($('txId')?.value || STATE.txCurrentId || '').trim();
    if (!txId) throw new Error('tx_id required');
    const res = await call('tx.commit', { tx_id: txId }, 'txOut');
    const result = unwrapRpcResult(res, 'tx.commit');
    STATE.txCurrentId = '';
    if ($('txId')) $('txId').value = '';
    renderTxState();
    setOut(result, 'txOut');
    showToast('Transaction committed.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'txOut');
  }
}

async function txRollback() {
  try {
    const txId = ($('txId')?.value || STATE.txCurrentId || '').trim();
    if (!txId) throw new Error('tx_id required');
    const res = await call('tx.rollback', { tx_id: txId }, 'txOut');
    const result = unwrapRpcResult(res, 'tx.rollback');
    STATE.txCurrentId = '';
    if ($('txId')) $('txId').value = '';
    renderTxState();
    setOut(result, 'txOut');
    showToast('Transaction rolled back.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'txOut');
  }
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

function renderCompactionSummary(result) {
  const el = $('compactionSummary');
  if (!el || !result || typeof result !== 'object') return;
  const cfg = result.scheduler || result.config || {};
  const workload = result.workload || {};
  const pressure = result.pressure || {};
  const energy = cfg.energy || {};
  const activeWindow = cfg.peak_window
    ? ((cfg.peak_window.start || '--') + '-' + (cfg.peak_window.end || '--'))
    : 'none';
  el.innerHTML = '<strong>Status</strong>: ' + escapeHtml(result.status || 'unknown')
    + ' | <strong>Mode</strong>: ' + escapeHtml(cfg.mode || cfg.policy || 'unknown')
    + ' | <strong>Paused</strong>: ' + escapeHtml(String(!!cfg.paused))
    + '<br><strong>L0</strong>: ' + escapeHtml(String(result.l0_files ?? '--')) + ' file(s), ' + escapeHtml(formatBytes(Number(result.l0_bytes) || 0))
    + ' | <strong>Hard pressure</strong>: ' + escapeHtml(String(!!pressure.hard_limit_exceeded))
    + ' | <strong>Active peak window</strong>: ' + escapeHtml(activeWindow)
    + '<br><strong>Workload</strong>: reads ' + escapeHtml(String(workload.point_reads_per_s ?? '--'))
    + '/s, ranges ' + escapeHtml(String(workload.range_reads_per_s ?? '--'))
    + '/s, writes ' + escapeHtml(String(workload.write_ops_per_s ?? '--')) + '/s'
    + '<br><strong>Energy</strong>: ' + escapeHtml(String(energy.estimated_joules_per_s ?? '--'))
    + ' J/s, signal ' + escapeHtml(String(energy.signal_multiplier ?? '--'))
    + ', defer ' + escapeHtml(String(energy.defer_ratio ?? '--'));
}

async function compactionStatus() {
  try {
    const res = await call('maintenance.compaction.status', {}, 'compactionOut');
    const result = unwrapRpcResult(res, 'maintenance.compaction.status');
    renderCompactionSummary(result);
    setOut(result, 'compactionOut');
  } catch (e) { setOut({ error: String(e) }, 'compactionOut'); }
}

function readCompactionPolicyPatch() {
  const payload = {};
  const policy = $('compactionPolicy')?.value || 'workload_guided';
  payload.policy = policy;
  const enabled = $('compactionEnabled')?.value || '';
  const paused = $('compactionPaused')?.value || '';
  if (enabled) payload.enabled = enabled === 'true';
  if (paused) payload.paused = paused === 'true';
  const maxL0Files = parseOptionalU64Input('compactionMaxL0Files', 'Max L0 files');
  const maxL0Bytes = parseOptionalU64Input('compactionMaxL0Bytes', 'Max L0 bytes');
  const maxIo = parseOptionalU64Input('compactionMaxIo', 'Max IO bytes per second');
  const rawCpu = $('compactionMaxCpu')?.value.trim() || '';
  if (maxL0Files !== undefined) payload.max_l0_files = maxL0Files;
  if (maxL0Bytes !== undefined) payload.max_l0_bytes = maxL0Bytes;
  const budget = {};
  if (maxIo !== undefined) budget.max_io_bytes_per_s = maxIo;
  if (rawCpu) {
    const maxCpu = Number.parseFloat(rawCpu);
    if (!Number.isFinite(maxCpu) || maxCpu <= 0 || maxCpu > 100) throw new Error('Max CPU % must be > 0 and <= 100');
    budget.max_cpu_pct = maxCpu;
  }
  if (Object.keys(budget).length) payload.budget = budget;
  const signals = {};
  const powerSource = $('compactionPowerSource')?.value || '';
  const rawBattery = $('compactionBatteryPct')?.value.trim() || '';
  const rawPrice = $('compactionPriceMultiplier')?.value.trim() || '';
  const rawCarbon = $('compactionCarbonMultiplier')?.value.trim() || '';
  if (powerSource) signals.power_source = powerSource;
  if (rawBattery) {
    const batteryPct = Number.parseFloat(rawBattery);
    if (!Number.isFinite(batteryPct) || batteryPct < 0 || batteryPct > 100) throw new Error('Battery % must be between 0 and 100');
    signals.battery_pct = batteryPct;
  }
  if (rawPrice) {
    const priceMultiplier = Number.parseFloat(rawPrice);
    if (!Number.isFinite(priceMultiplier) || priceMultiplier <= 0) throw new Error('Price multiplier must be > 0');
    signals.price_multiplier = priceMultiplier;
  }
  if (rawCarbon) {
    const carbonMultiplier = Number.parseFloat(rawCarbon);
    if (!Number.isFinite(carbonMultiplier) || carbonMultiplier <= 0) throw new Error('Carbon multiplier must be > 0');
    signals.carbon_multiplier = carbonMultiplier;
  }
  if (Object.keys(signals).length) payload.external_signals = signals;
  const peakWindows = ($('compactionPeakWindows')?.value || '')
    .split(/\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
  if (peakWindows.length) payload.peak_windows = peakWindows;
  return payload;
}

async function compactionSavePolicy() {
  try {
    const payload = readCompactionPolicyPatch();
    const res = await call('maintenance.compaction.set_policy', payload, 'compactionOut');
    const result = unwrapRpcResult(res, 'maintenance.compaction.set_policy');
    renderCompactionSummary(result);
    setOut({ saved: true, config: payload, status: result }, 'compactionOut');
    showToast('Compaction policy saved.', 'success');
  } catch (e) { setOut({ error: String(e) }, 'compactionOut'); }
}

async function compactionPause() {
  try {
    const res = await call('maintenance.compaction.pause', {}, 'compactionOut');
    const result = unwrapRpcResult(res, 'maintenance.compaction.pause');
    renderCompactionSummary(result);
    setOut(result, 'compactionOut');
  } catch (e) { setOut({ error: String(e) }, 'compactionOut'); }
}

async function compactionResume() {
  try {
    const res = await call('maintenance.compaction.resume', {}, 'compactionOut');
    const result = unwrapRpcResult(res, 'maintenance.compaction.resume');
    renderCompactionSummary(result);
    setOut(result, 'compactionOut');
  } catch (e) { setOut({ error: String(e) }, 'compactionOut'); }
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
      btn.addEventListener('click', () => openRpcMethod(m));
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
    btn.addEventListener('click', () => openRpcMethod(method));
    grid.appendChild(btn);
  });
}

// ---------------------------------------------------------------------------
// Dashboard cards – Top Tables, Slow Queries, Sessions, Index Health, Research
// ---------------------------------------------------------------------------

function unwrapCellValue(cell) {
  if (cell && typeof cell === 'object' && 'v' in cell) return cell.v;
  return cell;
}

async function silentRpc(method, params) {
  try {
    const baseUrl = getBaseUrl(), token = getToken();
    return await rpc(baseUrl, token, method, params || {});
  } catch (e) {
    return null;
  }
}

async function refreshTopTables() {
  const grid = $('topTablesGrid'); if (!grid) return;
  const summary = $('topTablesSummary');
  try {
    const res = await silentRpc('sql.exec', {
      sql: 'SELECT table_schema, table_name, table_rows, data_length FROM information_schema.tables ORDER BY table_rows DESC LIMIT 10'
    });
    const result = res?.json?.result;
    const data = result?.result?.data;
    if (!data || !Array.isArray(data.rows) || !data.rows.length) {
      if (summary) summary.textContent = 'No tables discovered yet. Create one from Schema or Easy Viewer to populate this ranking.';
      renderTable('topTablesGrid', ['Table', 'Rows', 'Data'], [['No tables found', '\u2014', '\u2014']]);
      return;
    }
    const rows = data.rows.map(r => {
      const schema = unwrapCellValue(r[0]) || '';
      const name = unwrapCellValue(r[1]) || '';
      const rc = unwrapCellValue(r[2]);
      const dl = unwrapCellValue(r[3]);
      return [
        (schema ? schema + '.' : '') + name,
        rc == null ? '\u2014' : String(rc),
        dl == null ? '\u2014' : formatBytes(Number(dl))
      ];
    });
    if (summary) summary.textContent = rows.length + ' table(s) ranked by row count. Largest observed dataset: ' + rows[0][0] + ' with ' + rows[0][1] + ' row(s).';
    renderTable('topTablesGrid', ['Table', 'Rows', 'Data'], rows);
  } catch (e) {
    if (summary) summary.textContent = 'Top-table ranking is unavailable right now. Check connectivity or information_schema support.';
    renderTable('topTablesGrid', ['Table', 'Rows', 'Data'], [['Error: ' + (e.message || e), '\u2014', '\u2014']]);
  }
}

async function refreshSlowQueries() {
  const grid = $('slowQueryGrid'); if (!grid) return;
  const summary = $('slowQuerySummary');
  try {
    const res = await silentRpc('stats.slow_queries', { limit: 10, min_ms: 0 });
    const queries = res?.json?.result?.queries || [];
    if (!queries.length) {
      if (summary) summary.textContent = 'No slow queries recorded in the current telemetry window.';
      renderTable('slowQueryGrid', ['Method', 'Query', 'Time (ms)'], [['No slow queries recorded', '\u2014', '\u2014']]);
      return;
    }
    const rows = queries.map(q => [
      q.method || '\u2014',
      q.fingerprint || '\u2014',
      typeof q.duration_ms === 'number' ? q.duration_ms.toFixed(1) : '\u2014'
    ]);
    const hottest = queries[0];
    if (summary) summary.textContent = queries.length + ' query sample(s) loaded. Slowest seen: ' + (hottest?.method || 'query') + ' at ' + (typeof hottest?.duration_ms === 'number' ? hottest.duration_ms.toFixed(1) + ' ms' : '--') + '.';
    renderTable('slowQueryGrid', ['Method', 'Query', 'Time (ms)'], rows);
  } catch (e) {
    if (summary) summary.textContent = 'Slow-query telemetry is unavailable right now.';
    renderTable('slowQueryGrid', ['Method', 'Query', 'Time (ms)'], [['Error: ' + (e.message || e), '\u2014', '\u2014']]);
  }
}

async function refreshActiveSessions() {
  const el = (id, v) => { const e = $(id); if (e) e.textContent = v; };
  const summary = $('activeSessionsSummary');
  try {
    const res = await silentRpc('stats.snapshot', {});
    const snap = res?.json?.result;
    if (!snap) {
      el('statActiveSessions', '--');
      el('statIdleSessions', '--');
      el('statLongestQuery', '--');
      if (summary) summary.textContent = 'Live workload is unavailable until stats can be loaded.';
      return;
    }
    const active = snap.sessions?.active ?? snap.connections ?? 0;
    const openTxns = snap.open_txns ?? 0;
    const avg = snap.query?.avg_latency_ms;
    el('statActiveSessions', String(active));
    el('statIdleSessions', String(openTxns));
    el('statLongestQuery', typeof avg === 'number' ? avg.toFixed(1) + 'ms' : '--');
    if (summary) summary.textContent = String(active) + ' live session(s), ' + String(openTxns) + ' open transaction(s), average latency ' + (typeof avg === 'number' ? avg.toFixed(1) + ' ms' : '--') + '.';
  } catch (e) {
    el('statActiveSessions', '--');
    el('statIdleSessions', '--');
    el('statLongestQuery', '--');
    if (summary) summary.textContent = 'Live workload could not be refreshed.';
  }
}

async function refreshIndexHealth() {
  const el = (id, v) => { const e = $(id); if (e) e.textContent = v; };
  const summary = $('indexHealthSummary');
  try {
    const res = await silentRpc('advisor.history', {});
    const result = res?.json?.result;
    const entries = Array.isArray(result?.entries)
      ? result.entries
      : Array.isArray(result?.recommendations)
        ? result.recommendations
        : [];
    const applied = entries.filter((entry) => entry.action === 'apply' || entry.status === 'applied' || entry.result_status === 'applied').length;
    const dismissed = entries.filter((entry) => entry.action === 'dismiss' || entry.status === 'dismissed' || entry.result_status === 'dismissed').length;
    el('statIndexRecs', String(entries.length));
    el('statIndexApplied', String(applied));
    el('statIndexDismissed', String(dismissed));
    if (summary) summary.textContent = entries.length ? (entries.length + ' advisor event(s) loaded. ' + applied + ' applied and ' + dismissed + ' dismissed.') : 'No advisor history entries recorded yet. Run the Index Advisor to create evidence for this card.';
  } catch (e) {
    el('statIndexRecs', '0');
    el('statIndexApplied', '0');
    el('statIndexDismissed', '0');
    if (summary) summary.textContent = 'Advisor history is unavailable right now.';
  }
}

function renderResearchStatusGrid() {
  const grid = $('researchStatusGrid'); if (!grid) return;
  const summary = $('researchStatusSummary');
  grid.textContent = '';
  const hardenedCount = RESEARCH_TRACKS.filter((track) => track.status === 'hardened').length;
  if (summary) summary.textContent = hardenedCount + ' hardened track(s) and ' + (RESEARCH_TRACKS.length - hardenedCount) + ' prototype track(s). Click a card to jump directly when a panel exists.';
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
    const secret = r?.json?.result?.secret;
    await securityRefreshTokens({ silent: true });
    if (secret) {
      const el = $('secTokenOut');
      if (el) el.textContent = 'Token created. Secret (copy now — shown once):\n' + secret;
    }
  } catch (e) { setOut({ error: String(e) }, 'secTokenOut'); }
}

async function securityRefreshTokens(opts) {
  const silent = !!(opts && opts.silent);
  try {
    const r = silent
      ? await silentRpc('security.token.list', {})
      : await call('security.token.list', {}, 'secTokenOut');
    const grid = $('secTokenGrid'); if (!grid) return;
    const tokens = r?.json?.result?.tokens || [];
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
    const queries = r?.json?.result?.queries || [];
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

// ---------------------------------------------------------------------------
// Time travel + replay (Phase 19 / T184)
// ---------------------------------------------------------------------------
function replayHydrateDefaultsFromContext() {
  const replayDb = $('replayDb');
  const ttDb = $('ttDb');
  const ttTable = $('ttTable');
  if (replayDb && !replayDb.value.trim() && STATE.selectedDb) replayDb.value = STATE.selectedDb;
  if (ttDb && !ttDb.value.trim() && STATE.selectedDb) ttDb.value = STATE.selectedDb;
  if (ttTable && !ttTable.value.trim() && STATE.selectedTable) ttTable.value = STATE.selectedTable;
}

function buildAsOfLit(raw) {
  const value = String(raw || '').trim();
  if (!value) return undefined;
  if (/^[0-9]+$/.test(value)) return { t: 'u64', v: Number(value) };
  return { t: 'datetime', iso: value };
}

function replayFindImport(workspaceId) {
  return (STATE.replayImports || []).find((entry) => entry.workspace_id === workspaceId) || null;
}

function replayRememberImport(entry) {
  STATE.replayImports = (STATE.replayImports || []).filter((item) => item.workspace_id !== entry.workspace_id);
  STATE.replayImports.unshift(entry);
  if (STATE.replayImports.length > 12) STATE.replayImports = STATE.replayImports.slice(0, 12);
}

function renderHistoryStatusCard(status) {
  const summary = $('historySummary');
  const enabledSelect = $('historyEnabled');
  const windowInput = $('historyWindowMs');
  if (!status) {
    if (summary) summary.innerHTML = 'Load history status to inspect retained tombstones, active policy, and purgeable rows.';
    renderTable('historyTableGrid', ['Table', 'Live', 'Tombstones', 'Purgeable', 'Oldest Tombstone'], [['--', 'No history status loaded yet', '', '', '']]);
    return;
  }

  const policy = status.policy || {};
  if (enabledSelect && document.activeElement !== enabledSelect) enabledSelect.value = policy.enabled ? 'true' : 'false';
  if (windowInput && document.activeElement !== windowInput && Number.isFinite(policy.window_ms)) windowInput.value = String(policy.window_ms || 0);

  if (summary) {
    const horizonText = status.horizon_ms === null || status.horizon_ms === undefined
      ? 'policy/default'
      : formatUiTimestamp(Number(status.horizon_ms));
    summary.innerHTML = '<strong>Policy</strong>: ' + escapeHtml(policy.enabled ? 'enabled' : 'disabled')
      + ' | <strong>Window</strong>: ' + escapeHtml(String(Number(policy.window_ms) || 0)) + ' ms'
      + ' | <strong>Live rows</strong>: ' + escapeHtml(String(Number(status.total_live_rows) || 0))
      + ' | <strong>Tombstones</strong>: ' + escapeHtml(String(Number(status.total_tombstones) || 0))
      + ' | <strong>Purgeable</strong>: ' + escapeHtml(String(Number(status.total_purgeable) || 0))
      + '<br><strong>Oldest tombstone</strong>: ' + escapeHtml(formatUiTimestamp(Number(status.oldest_tombstone_commit_ts_ms) || 0))
      + ' | <strong>Evaluated horizon</strong>: ' + escapeHtml(horizonText);
  }

  const rows = Array.isArray(status.tables)
    ? status.tables.map((table) => [
        (table.db || '') + '.' + (table.table || ''),
        Number(table.live_rows) || 0,
        Number(table.tombstones) || 0,
        Number(table.purgeable) || 0,
        formatUiTimestamp(Number(table.oldest_tombstone_commit_ts_ms) || 0),
      ])
    : [];
  renderTable('historyTableGrid', ['Table', 'Live', 'Tombstones', 'Purgeable', 'Oldest Tombstone'], rows.length ? rows : [['--', 'No tables reported', '', '', '']]);
}

function renderReplayBundleSummary(bundle) {
  const summary = $('replayBundleSummary');
  if (!bundle || typeof bundle !== 'object' || !bundle.manifest) {
    if (summary) summary.innerHTML = 'Export a replay bundle or load one from disk to inspect its manifest.';
    renderTable('replayManifestTable', ['Field', 'Value'], [['Bundle', 'No replay bundle loaded']]);
    return;
  }

  const manifest = bundle.manifest || {};
  const lsnRange = (manifest.start_lsn ?? '--') + ' → ' + (manifest.end_lsn ?? '--');
  const commitRange = formatUiTimestamp(Number(manifest.start_commit_ts_ms) || 0) + ' → ' + formatUiTimestamp(Number(manifest.end_commit_ts_ms) || 0);
  if (summary) {
    summary.innerHTML = '<strong>' + escapeHtml(manifest.bundle_id || 'bundle') + '</strong>'
      + ' | <strong>Tables</strong>: ' + escapeHtml(String(Number(manifest.table_count) || 0))
      + ' | <strong>Rows</strong>: ' + escapeHtml(String(Number(manifest.row_count) || 0))
      + ' | <strong>Changes</strong>: ' + escapeHtml(String(Number(manifest.change_count) || 0))
      + '<br><strong>Checksum</strong>: ' + escapeHtml(manifest.checksum || '--');
  }

  renderTable('replayManifestTable', ['Field', 'Value'], [
    ['Bundle ID', manifest.bundle_id || '--'],
    ['Format Version', manifest.format_version ?? '--'],
    ['Generated', formatUiTimestamp(Number(manifest.generated_at_ms) || 0)],
    ['Engine Version', manifest.engine_version || '--'],
    ['Storage Mode', manifest.storage_mode || '--'],
    ['Table Count', Number(manifest.table_count) || 0],
    ['Row Count', Number(manifest.row_count) || 0],
    ['Live Rows', Number(manifest.live_row_count) || 0],
    ['Tombstones', Number(manifest.tombstone_count) || 0],
    ['Change Count', Number(manifest.change_count) || 0],
    ['LSN Range', lsnRange],
    ['Commit Range', commitRange],
    ['Checksum', manifest.checksum || '--'],
  ]);
}

function renderReplayWorkspaceOptions() {
  const select = $('replayWorkspaceSelect');
  const input = $('replayWorkspaceId');
  const summary = $('replayWorkspaceSummary');
  const imports = Array.isArray(STATE.replayImports)
    ? STATE.replayImports.slice().sort((a, b) => (b.imported_at_ms || 0) - (a.imported_at_ms || 0))
    : [];

  if (STATE.replaySelectedWorkspaceId && !imports.some((entry) => entry.workspace_id === STATE.replaySelectedWorkspaceId)) {
    STATE.replaySelectedWorkspaceId = '';
  }
  if (!STATE.replaySelectedWorkspaceId && imports.length) {
    STATE.replaySelectedWorkspaceId = imports[0].workspace_id;
  }

  if (select) {
    select.innerHTML = imports.length
      ? imports.map((entry) => '<option value="' + escapeHtml(entry.workspace_id) + '">' + escapeHtml(entry.workspace_id + ' · ' + (entry.bundle_id || 'bundle')) + '</option>').join('')
      : '<option value="">No session imports</option>';
    select.disabled = !imports.length;
    select.value = STATE.replaySelectedWorkspaceId || '';
  }

  if (input && document.activeElement !== input && STATE.replaySelectedWorkspaceId) {
    input.value = STATE.replaySelectedWorkspaceId;
  }

  if (summary) {
    if (!imports.length) {
      summary.innerHTML = 'No replay workspaces tracked in this browser session yet. Import a bundle or type a workspace ID manually.';
    } else {
      summary.innerHTML = imports.map((entry) => {
        const run = entry.last_run_result;
        const runLabel = !run ? 'imported' : (run.ok ? 'checksum verified' : 'checksum mismatch');
        return '<strong>' + escapeHtml(entry.workspace_id) + '</strong> → ' + escapeHtml(entry.bundle_id || 'bundle') + ' (' + escapeHtml(runLabel) + ')';
      }).join('<br>');
    }
  }
}

function renderReplayIntegrity(result) {
  const summary = $('replayIntegritySummary');
  if (!result) {
    if (summary) summary.innerHTML = 'Run a replay workspace integrity check to compare manifest and observed checksums.';
    renderTable('replayIntegrityTable', ['Table', 'Rows', 'Live', 'Tombstones', 'Checksum'], [['--', 'No integrity run yet', '', '', '']]);
    return;
  }

  if (summary) {
    const perf = result.performance_report;
    const perfLine = perf
      ? '<br><strong>Perf replay</strong>: checksum ' + escapeHtml(perf.checksum_match ? 'matched' : 'differs')
        + ' | rows Δ ' + escapeHtml(String(perf.storage?.total_rows_delta ?? '--'))
        + ' | p95 Δ ' + escapeHtml(String(perf.timing?.p95_inter_event_ms_delta ?? '--')) + ' ms'
      : '';
    summary.innerHTML = '<strong>Workspace</strong>: ' + escapeHtml(result.workspace_id || '--')
      + ' | <strong>Bundle</strong>: ' + escapeHtml(result.bundle_id || '--')
      + ' | <strong>Status</strong>: ' + escapeHtml(result.ok ? 'PASS' : 'FAIL')
      + '<br><strong>Expected checksum</strong>: ' + escapeHtml(result.expected_checksum || '--')
      + '<br><strong>Observed checksum</strong>: ' + escapeHtml(result.observed_checksum || '--')
      + ' | <strong>Replayed rows</strong>: ' + escapeHtml(String(Number(result.replayed_rows) || 0))
      + ' | <strong>Replayed changes</strong>: ' + escapeHtml(String(Number(result.replayed_changes) || 0))
      + perfLine;
  }

  const rows = Array.isArray(result.table_checksums)
    ? result.table_checksums.map((entry) => [
        (entry.table?.db || '') + '.' + (entry.table?.table || ''),
        Number(entry.row_count) || 0,
        Number(entry.live_row_count) || 0,
        Number(entry.tombstone_count) || 0,
        entry.checksum || '--',
      ])
    : [];
  renderTable('replayIntegrityTable', ['Table', 'Rows', 'Live', 'Tombstones', 'Checksum'], rows.length ? rows : [['--', 'No table checksums returned', '', '', '']]);
}

function renderReplayPanel() {
  replayHydrateDefaultsFromContext();
  renderHistoryStatusCard(STATE.replayHistoryStatus);
  renderReplayBundleSummary(STATE.replayLastBundle);
  renderReplayWorkspaceOptions();
  renderReplayIntegrity(STATE.replayLastRun);
  refreshDashboardSummaries();
}

async function timeTravelSeedQuery() {
  try {
    replayHydrateDefaultsFromContext();
    const db = ($('ttDb')?.value || '').trim();
    const table = ($('ttTable')?.value || '').trim();
    if (!db || !table) throw new Error('Database and table are required');
    const limit = parseOptionalU64Input('ttLimit', 'Point-in-time limit') || 50;
    const res = await call('schema.describe_table', { db, table }, 'ttOut');
    const result = unwrapRpcResult(res, 'schema.describe_table');
    const columns = Array.isArray(result.columns) ? result.columns.map((col) => col.name).filter(Boolean) : [];
    if (!columns.length) throw new Error('No columns found for ' + db + '.' + table);
    const query = {
      with: [],
      body: {
        select: {
          projection: columns.map((name) => ({ expr: { col: name }, as: null })),
          from: [tableRef(db, table)],
        },
      },
      order_by: [],
      limit: { limit, offset: 0 },
    };
    if ($('ttQuery')) $('ttQuery').value = JSON.stringify(query, null, 2);
    if ($('ttSummary')) $('ttSummary').innerHTML = 'Seeded <strong>' + escapeHtml(db + '.' + table) + '</strong> with ' + escapeHtml(String(columns.length)) + ' projected column(s).';
    setOut({ seeded: true, db, table, columns, limit }, 'ttOut');
    showToast('Time-travel query seeded from ' + db + '.' + table + '.', 'info');
  } catch (e) {
    setOut({ error: String(e) }, 'ttOut');
  }
}

async function timeTravelRunQuery() {
  try {
    const query = parseJsonInput($('ttQuery')?.value || '', 'Time-travel query');
    if (!query) throw new Error('Query JSON is required');
    const asOf = buildAsOfLit($('ttAsOf')?.value || '');
    const res = await call('query.select', cleanParams({ query, as_of: asOf, result_format: 'rows_json' }), 'ttOut');
    const result = unwrapRpcResult(res, 'query.select');
    const data = result?.data;
    if (data) {
      const columns = (data.columns || []).map((col, idx) => typeof col === 'string' ? col : (col?.name || ('col' + (idx + 1))));
      renderTable('ttResultGrid', columns, data.rows || []);
      if ($('ttSummary')) {
        const asOfLabel = asOf ? (asOf.iso || String(asOf.v)) : 'current snapshot';
        $('ttSummary').innerHTML = 'Showing <strong>' + escapeHtml(String((data.rows || []).length)) + '</strong> row(s) for <strong>' + escapeHtml(asOfLabel) + '</strong>.';
      }
    } else {
      renderTable('ttResultGrid', [], []);
    }
    setOut(result, 'ttOut');
    showToast('Point-in-time query executed.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'ttOut');
  }
}

function timeTravelClear() {
  if ($('ttAsOf')) $('ttAsOf').value = '';
  if ($('ttQuery')) $('ttQuery').value = '';
  renderTable('ttResultGrid', [], []);
  if ($('ttSummary')) $('ttSummary').innerHTML = 'Seed a query from the selected table or paste a full <code>query.select</code> payload, then add an <code>as_of</code> timestamp to inspect historical rows.';
  setOut('Ready.', 'ttOut');
}

async function historyLoadStatus() {
  try {
    const horizonMs = parseOptionalU64Input('historyHorizonMs', 'History horizon');
    const res = await call('maintenance.history.status', cleanParams({ horizon_ms: horizonMs }), 'historyOut');
    const result = unwrapRpcResult(res, 'maintenance.history.status');
    STATE.replayHistoryStatus = result;
    renderReplayPanel();
    setOut(result, 'historyOut');
    showToast('History status loaded.', 'info');
  } catch (e) {
    setOut({ error: String(e) }, 'historyOut');
  }
}

async function historySavePolicy() {
  try {
    const enabled = $('historyEnabled')?.value === 'true';
    const windowMs = parseOptionalU64Input('historyWindowMs', 'Retention window');
    const res = await call('maintenance.history.set_policy', cleanParams({ enabled, window_ms: windowMs }), 'historyOut');
    const result = unwrapRpcResult(res, 'maintenance.history.set_policy');
    if (!STATE.replayHistoryStatus) STATE.replayHistoryStatus = {};
    STATE.replayHistoryStatus.policy = result.policy || { enabled, window_ms: windowMs || 0 };
    renderReplayPanel();
    setOut(result, 'historyOut');
    showToast('History retention policy saved.', 'success');
    await historyLoadStatus();
  } catch (e) {
    setOut({ error: String(e) }, 'historyOut');
  }
}

async function historyRunGc() {
  try {
    const horizonMs = parseOptionalU64Input('historyHorizonMs', 'History horizon');
    const label = horizonMs === undefined ? 'the active retention horizon' : ('horizon ' + horizonMs);
    const ok = await skeinModal('🧹', 'Run History GC', 'Permanently remove purgeable tombstones using <b>' + escapeHtml(label) + '</b>? Pre-T180 tombstones remain protected.', [
      { label: 'Cancel', value: false },
      { label: 'Run GC', value: true, cls: 'danger' },
    ]);
    if (!ok) return;
    const res = await call('maintenance.history.gc', cleanParams({ horizon_ms: horizonMs }), 'historyOut');
    const result = unwrapRpcResult(res, 'maintenance.history.gc');
    setOut(result, 'historyOut');
    showToast('History GC completed.', 'success');
    await historyLoadStatus();
  } catch (e) {
    setOut({ error: String(e) }, 'historyOut');
  }
}

function replayBundleFromText() {
  const raw = $('replayBundleJson')?.value.trim() || '';
  if (!raw) {
    if (STATE.replayLastBundle) return STATE.replayLastBundle;
    throw new Error('Export a replay bundle first or paste a replay bundle JSON document');
  }
  const bundle = parseJsonInput(raw, 'Replay bundle');
  if (!bundle || typeof bundle !== 'object') throw new Error('Replay bundle must be a JSON object');
  return bundle;
}

function replayLoadBundleIntoEditor(bundle) {
  STATE.replayLastBundle = bundle;
  if ($('replayBundleJson')) $('replayBundleJson').value = JSON.stringify(bundle, null, 2);
  renderReplayPanel();
}

function replayUseLastBundle() {
  try {
    if (!STATE.replayLastBundle) throw new Error('No replay bundle available in this browser session');
    replayLoadBundleIntoEditor(STATE.replayLastBundle);
    setOut({ bundle_id: STATE.replayLastBundle.manifest?.bundle_id || null }, 'replayOut');
    showToast('Loaded the latest replay bundle into the editor.', 'info');
  } catch (e) {
    setOut({ error: String(e) }, 'replayOut');
  }
}

async function replayExportBundle() {
  try {
    replayHydrateDefaultsFromContext();
    const db = ($('replayDb')?.value || '').trim() || undefined;
    const fromLsn = parseOptionalU64Input('replayFromLsn', 'From LSN');
    const toLsn = parseOptionalU64Input('replayToLsn', 'To LSN');
    const bundleId = ($('replayBundleId')?.value || '').trim() || undefined;
    const redactionMode = $('replayRedactionMode')?.value || 'none';
    const redactionSalt = ($('replayRedactionSalt')?.value || '').trim() || undefined;
    const redaction = redactionMode === 'none' && !redactionSalt
      ? undefined
      : cleanParams({ mode: redactionMode, salt: redactionSalt });
    const res = await call('maintenance.replay.export', cleanParams({ db, from_lsn: fromLsn, to_lsn: toLsn, bundle_id: bundleId, redaction }), 'replayOut');
    const result = unwrapRpcResult(res, 'maintenance.replay.export');
    replayLoadBundleIntoEditor(result.bundle);
    setOut(result, 'replayOut');
    showToast('Replay bundle exported.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'replayOut');
  }
}

function replayDownloadBundle() {
  try {
    const bundle = replayBundleFromText();
    replayLoadBundleIntoEditor(bundle);
    const bundleId = bundle.manifest?.bundle_id || 'replay_bundle';
    downloadBlob(JSON.stringify(bundle, null, 2), bundleId + '.sreplay', 'application/json');
    showToast('Replay bundle downloaded.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'replayOut');
  }
}

async function replayImportBundle() {
  try {
    const bundle = replayBundleFromText();
    const workspaceId = ($('replayWorkspaceId')?.value || '').trim() || undefined;
    replayLoadBundleIntoEditor(bundle);
    const res = await call('maintenance.replay.import', cleanParams({ bundle, workspace_id: workspaceId }), 'replayOut');
    const result = unwrapRpcResult(res, 'maintenance.replay.import');
    replayRememberImport({ ...result, imported_at_ms: Date.now(), last_run_result: null });
    STATE.replaySelectedWorkspaceId = result.workspace_id;
    if ($('replayWorkspaceId')) $('replayWorkspaceId').value = result.workspace_id;
    renderReplayPanel();
    setOut({ import: result, bundle_manifest: bundle.manifest || null }, 'replayOut');
    showToast('Replay workspace ' + result.workspace_id + ' imported.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'replayOut');
  }
}

async function replayRunIntegrity() {
  try {
    const workspaceId = (($('replayWorkspaceId')?.value || $('replayWorkspaceSelect')?.value || '').trim());
    if (!workspaceId) throw new Error('Workspace ID is required');
    const res = await call('maintenance.replay.run', { workspace_id: workspaceId }, 'replayOut');
    const result = unwrapRpcResult(res, 'maintenance.replay.run');
    STATE.replayLastRun = result;
    STATE.replaySelectedWorkspaceId = result.workspace_id;
    const entry = replayFindImport(result.workspace_id);
    if (entry) {
      entry.last_run_result = result;
      entry.last_run_at_ms = Date.now();
    }
    renderReplayPanel();
    setOut(result, 'replayOut');
    showToast(result.ok ? ('Replay integrity verified for ' + result.workspace_id + '.') : ('Replay integrity check failed for ' + result.workspace_id + '.'), result.ok ? 'success' : 'error');
  } catch (e) {
    setOut({ error: String(e) }, 'replayOut');
  }
}

async function replayBundleFileChanged(event) {
  try {
    const file = event.target.files && event.target.files[0];
    if (!file) return;
    const text = await file.text();
    if ($('replayBundleJson')) $('replayBundleJson').value = text;
    const bundle = parseJsonInput(text, 'Replay bundle file');
    STATE.replayLastBundle = bundle;
    renderReplayPanel();
    setOut({ loaded_file: file.name, bundle_id: bundle?.manifest?.bundle_id || null }, 'replayOut');
    showToast('Replay bundle ' + file.name + ' loaded.', 'info');
  } catch (e) {
    setOut({ error: String(e) }, 'replayOut');
  }
}

function edgeHydrateDefaultsFromContext() {
  const dbInput = $('edgeDb');
  const tableInput = $('edgeTable');
  if (dbInput && !dbInput.value.trim() && STATE.selectedDb) dbInput.value = STATE.selectedDb;
  if (tableInput && !tableInput.value.trim() && STATE.selectedTable) tableInput.value = STATE.selectedTable;
}

function edgeBundleFromText() {
  const raw = $('edgeBundleJson')?.value.trim() || '';
  if (!raw) {
    if (STATE.edgeLastBundle) return STATE.edgeLastBundle;
    throw new Error('Request an edge bundle first or paste an edge bundle JSON document');
  }
  const bundle = parseJsonInput(raw, 'Edge bundle');
  if (!bundle || typeof bundle !== 'object') throw new Error('Edge bundle must be a JSON object');
  return bundle;
}

function edgeLoadBundleIntoEditor(bundle) {
  STATE.edgeLastBundle = bundle;
  if ($('edgeBundleJson')) $('edgeBundleJson').value = JSON.stringify(bundle, null, 2);
}

async function edgeRequestBundle() {
  try {
    edgeHydrateDefaultsFromContext();
    const table = readDbTable('edgeDb', 'edgeTable');
    const fromSeq = parseOptionalU64Input('edgeFromSeq', 'From seq');
    const toSeq = parseOptionalU64Input('edgeToSeq', 'To seq');
    const maxEvents = parseOptionalU64Input('edgeMaxEvents', 'Max events');
    const bundleId = ($('edgeBundleId')?.value || '').trim() || undefined;
    const redactionMode = $('edgeRedactionMode')?.value || 'hash_pk';
    const redactionSalt = ($('edgeRedactionSalt')?.value || '').trim() || undefined;
    const res = await call('edge.bundle.request', {
      windows: [cleanParams({ table, from_seq: fromSeq, to_seq: toSeq, max_events: maxEvents })],
      redaction: cleanParams({ mode: redactionMode, salt: redactionSalt }),
      bundle_id: bundleId,
    }, 'edgeOut');
    const result = unwrapRpcResult(res, 'edge.bundle.request');
    edgeLoadBundleIntoEditor(result.bundle);
    setOut(result, 'edgeOut');
    showToast('Edge bundle requested.', 'success');
  } catch (e) { setOut({ error: String(e) }, 'edgeOut'); }
}

async function edgeApplyBundle() {
  try {
    const bundle = edgeBundleFromText();
    edgeLoadBundleIntoEditor(bundle);
    const res = await call('edge.bundle.apply', { bundle }, 'edgeOut');
    const result = unwrapRpcResult(res, 'edge.bundle.apply');
    setOut(result, 'edgeOut');
    showToast(result.applied ? 'Edge coverage applied.' : 'Edge coverage already current.', 'success');
  } catch (e) { setOut({ error: String(e) }, 'edgeOut'); }
}

async function edgeStatus() {
  try {
    const rawQuery = ($('edgeStatusQuery')?.value || '').trim();
    const maxLag = parseOptionalU64Input('edgeMaxLag', 'Max lag');
    const params = cleanParams({
      query: rawQuery ? parseJsonInput(rawQuery, 'Edge status query') : undefined,
      max_lag: maxLag,
    });
    const res = await call('edge.bundle.status', params, 'edgeOut');
    const result = unwrapRpcResult(res, 'edge.bundle.status');
    setOut(result, 'edgeOut');
  } catch (e) { setOut({ error: String(e) }, 'edgeOut'); }
}

function cdcHydrateDefaultsFromContext() {
  const dbInput = $('cdcDb');
  const tableInput = $('cdcTable');
  if (dbInput && !dbInput.value.trim() && STATE.selectedDb) dbInput.value = STATE.selectedDb;
  if (tableInput && !tableInput.value.trim() && STATE.selectedTable) tableInput.value = STATE.selectedTable;
}

function cdcSubscriptionLabel(sub) {
  if (!sub) return '--';
  if (sub.kind === 'query') return 'query ' + (sub.query_id || sub.sub_id);
  if (sub.target_label) return sub.target_label;
  if (sub.db && sub.table) return sub.db + '.' + sub.table;
  return sub.sub_id || '--';
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
  const selectedLabel = selected ? (cdcSubscriptionLabel(selected) + ' (' + selected.sub_id + ')') : 'none';
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
  let h = '<table class="data-table"><thead><tr><th>Subscription</th><th>Kind</th><th>Target</th><th>Start</th><th>Acked</th><th>Next</th><th>Lag</th><th>Last Poll</th></tr></thead><tbody>';
  subs.forEach((sub) => {
    const lag = cdcLagForSub(sub);
    const barWidth = lag <= 0 ? 0 : Math.max(8, Math.round((lag / maxLag) * 100));
    const lagBar = lag <= 0
      ? '<span class="hint">caught up</span>'
      : '<div class="lag-bar"><div class="lag-bar-fill" style="width:' + barWidth + '%"></div></div>';
    h += '<tr' + (sub.sub_id === STATE.cdcSelectedSubId ? ' class="row-selected"' : '') + '>'
      + '<td><strong>' + escapeHtml(sub.sub_id) + '</strong></td>'
      + '<td>' + escapeHtml(sub.kind || 'table') + '</td>'
      + '<td>' + escapeHtml(cdcSubscriptionLabel(sub)) + '</td>'
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
      ? subs.map((sub) => '<option value="' + escapeHtml(sub.sub_id) + '">' + escapeHtml(sub.sub_id + ' · ' + cdcSubscriptionLabel(sub)) + '</option>').join('')
      : '<option value="">No subscriptions</option>';
    select.disabled = !subs.length;
    select.value = STATE.cdcSelectedSubId || '';
  }
  const selected = cdcFindSubscription(STATE.cdcSelectedSubId);
  const dbInput = $('cdcDb');
  const tableInput = $('cdcTable');
  const queryIdInput = $('cdcQueryId');
  const queryArgsInput = $('cdcQueryArgs');
  const fromInput = $('cdcFromOffset');
  const ackInput = $('cdcAckOffset');
  if (selected) {
    if (selected.kind === 'table') {
      if (dbInput && document.activeElement !== dbInput) dbInput.value = selected.db;
      if (tableInput && document.activeElement !== tableInput) tableInput.value = selected.table;
    }
    if (selected.kind === 'query') {
      if (queryIdInput && document.activeElement !== queryIdInput) queryIdInput.value = selected.query_id || '';
      if (queryArgsInput && document.activeElement !== queryArgsInput) queryArgsInput.value = selected.args_json || '[]';
    }
    if (fromInput && document.activeElement !== fromInput) fromInput.value = String(Number(selected.next_offset) || Number(selected.offset) || 0);
    if (ackInput && document.activeElement !== ackInput) ackInput.value = String(Math.max(Number(selected.next_offset) || 0, Number(selected.acked_offset) || 0));
  }
  renderCdcLagSummary(subs, selected);
  renderCdcSubscriptionTable(subs);
  renderCdcEvents(selected);
  refreshDashboardSummaries();
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
      kind: 'table',
      db,
      table,
      target_label: db + '.' + table,
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

async function cdcSubscribeQuery() {
  try {
    const latest = latestPreparedQuery();
    const queryId = (($('cdcQueryId')?.value || latest?.query_id || '').trim());
    if (!queryId) throw new Error('Prepared query id required');
    const argsRaw = ($('cdcQueryArgs')?.value || latest?.args_json || '').trim() || '[]';
    const args = parseLitArgsInput(argsRaw, 'CDC query args');
    const res = await call('cdc.subscribe_query', cleanParams({ query_id: queryId, args }), 'cdcQueryOut');
    const result = unwrapRpcResult(res, 'cdc.subscribe_query');
    const offset = Number(result.offset) || 0;
    STATE.cdcSubscriptions = (STATE.cdcSubscriptions || []).filter((sub) => sub.sub_id !== result.sub_id);
    STATE.cdcSubscriptions.unshift({
      sub_id: result.sub_id,
      kind: 'query',
      query_id: queryId,
      args_json: argsRaw,
      target_label: 'query ' + queryId,
      offset,
      acked_offset: offset,
      next_offset: offset,
      created_at_ms: Date.now(),
      last_polled_at_ms: 0,
      last_events: [],
    });
    STATE.cdcSelectedSubId = result.sub_id;
    setOut({ subscription: { sub_id: result.sub_id, query_id: queryId, offset } }, 'cdcQueryOut');
    renderCdcPanel();
    showToast('CDC query subscription created for ' + queryId + '.', 'success');
  } catch (e) {
    setOut({ error: String(e) }, 'cdcQueryOut');
  }
}

function cdcUseLatestPrepared() {
  const latest = latestPreparedQuery();
  const queryId = ($('preparedQueryId')?.value || $('skeinQueryId')?.value || latest?.query_id || '').trim();
  const argsJson = ($('preparedArgs')?.value || latest?.args_json || '').trim() || '[]';
  if (!queryId) {
    setOut({ error: 'Prepare a query in the workspace first.' }, 'cdcQueryOut');
    return;
  }
  if ($('cdcQueryId')) $('cdcQueryId').value = queryId;
  if ($('cdcQueryArgs')) $('cdcQueryArgs').value = argsJson;
  setOut({ prepared_query: queryId, args: argsJson }, 'cdcQueryOut');
  showToast('Loaded latest prepared query into CDC form.', 'info');
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
    setOut({ subscription: { sub_id: selected.sub_id, kind: selected.kind || 'table', target: cdcSubscriptionLabel(selected) }, poll: result }, 'cdcOut');
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
    setOut({ subscription: { sub_id: selected.sub_id, kind: selected.kind || 'table', target: cdcSubscriptionLabel(selected) }, ack: result }, 'cdcOut');
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
    const ok = await skeinModal('🛰️', 'Close CDC Subscription', 'Close <b>' + escapeHtml(selected.sub_id) + '</b> for <b>' + escapeHtml(cdcSubscriptionLabel(selected)) + '</b>?', [
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
function readVectorLiteral() {
  const raw = $('vecQuery')?.value.trim();
  if (!raw) throw new Error('Vector required');
  const v = raw.split(',').map((part) => Number(part.trim()));
  if (!v.length || v.some((n) => !Number.isFinite(n))) throw new Error('Vector must contain comma-separated numbers');
  return { t: 'embedding', dims: v.length, v };
}

function readVectorColumn() {
  return $('vecCol')?.value.trim() || 'embedding';
}

function readVectorPk() {
  const raw = $('vecPk')?.value.trim();
  const pk = raw ? parseJsonInput(raw, 'PK JSON') : [{ t: 'u64', v: 1 }];
  if (!Array.isArray(pk)) throw new Error('PK JSON must be an array of typed literals');
  return pk;
}

async function vecSearch() {
  try {
    const t = readDbTable('vecDb','vecTable');
    const query = readVectorLiteral();
    const k = parseInt($('vecK')?.value,10) || 5;
    const col = readVectorColumn();
    const prefilter = $('vecPrefilter')?.value.trim();
    const params = cleanParams({ table: t, query, k, column: col, filter: prefilter ? parseJsonInput(prefilter,'Filter JSON') : undefined });
    await call('vector.search', params, 'vecOut');
  } catch (e) { setOut({error:String(e)},'vecOut'); }
}

async function vecBenchmark() {
  try {
    const t = readDbTable('vecDb','vecTable');
    const query = readVectorLiteral();
    const k = parseInt($('vecK')?.value,10) || 5;
    await call('vector.benchmark', { table: t, column: readVectorColumn(), queries: [query], k, metric: 'cosine' }, 'vecOut');
  } catch (e) { setOut({error:String(e)},'vecOut'); }
}

async function vecInsert() {
  try {
    const t = readDbTable('vecDb','vecTable');
    await call('vector.insert', { table: t, column: readVectorColumn(), rows: [{ pk: readVectorPk(), embedding: readVectorLiteral() }], upsert: true }, 'vecOut');
  } catch (e) { setOut({error:String(e)},'vecOut'); }
}

async function vecIndexStatus() {
  try { const t = readDbTable('vecDb','vecTable'); await call('vector.index.status',{table:t,column:readVectorColumn()},'vecOut'); } catch (e) { setOut({error:String(e)},'vecOut'); }
}

// ---------------------------------------------------------------------------
// Privacy / DP (R04-R05)
// ---------------------------------------------------------------------------
function readDpAggregateSpec() {
  const op = $('dpOp')?.value || 'count';
  const col = $('dpCol')?.value.trim();
  const spec = { op };
  if (col && (op !== 'count' || col !== '*')) spec.column = col;
  if (op !== 'count') {
    spec.bounds = {
      min: parseFloat($('dpBoundsMin')?.value) || 0,
      max: parseFloat($('dpBoundsMax')?.value) || 100,
    };
  }
  return spec;
}

function readDpMechanismParams() {
  const mechanism = $('dpMechanism')?.value || 'laplace';
  const delta = parseFloat($('dpDelta')?.value) || 0;
  return { mechanism, delta };
}

async function dpAggregate() {
  try {
    const t = readDbTable('dpDb','dpTable');
    const eps = parseFloat($('dpEpsilon')?.value) || 1.0;
    const principal = $('dpPrincipal')?.value.trim();
    const { mechanism, delta } = readDpMechanismParams();
    await call('dp.aggregate', {
      table: t,
      aggregates: [readDpAggregateSpec()],
      epsilon: eps,
      delta,
      mechanism,
      principal: principal || undefined,
      seed: parseInt($('dpSeed')?.value, 10) || undefined,
    }, 'dpOut');
  } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function dpEvaluate() {
  try {
    const t = readDbTable('dpDb','dpTable');
    const { mechanism, delta } = readDpMechanismParams();
    const epsilons = ($('dpEvalEps')?.value || '0.25,0.5,1,2')
      .split(',')
      .map(v => parseFloat(v.trim()))
      .filter(v => Number.isFinite(v) && v > 0);
    await call('dp.evaluate', {
      table: t,
      aggregates: [readDpAggregateSpec()],
      epsilons,
      delta,
      mechanism,
      trials: parseInt($('dpTrials')?.value, 10) || 25,
      seed: parseInt($('dpSeed')?.value, 10) || 42,
    }, 'dpOut');
  } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function dpBudgetGet() {
  try {
    const principal = $('dpPrincipal')?.value.trim();
    await call('dp.budget.get',{principal:principal || undefined,include_usage:true},'dpOut');
  } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function dpBudgetSet() {
  try {
    const principal = $('dpPrincipal')?.value.trim() || 'analyst';
    const eps = parseFloat($('dpEpsilon')?.value) || 1.0;
    const delta = parseFloat($('dpDelta')?.value) || 0;
    await call('dp.budget.set',{principal,total_epsilon:eps,total_delta:delta},'dpOut');
  } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function dpAudit() {
  try {
    const principal = $('dpPrincipal')?.value.trim();
    await call('dp.audit.log',{principal:principal || undefined,limit:50},'dpOut');
  } catch (e) { setOut({error:String(e)},'dpOut'); }
}

async function oblGet() {
  try { const t = readDbTable('oblDb','oblTable'); await call('oblivious.policy.get',{table:t},'oblOut'); } catch (e) { setOut({error:String(e)},'oblOut'); }
}

async function oblSet() {
  try {
    const t = readDbTable('oblDb','oblTable');
    const level = $('oblLevel')?.value || 'basic';
    const pad = parseInt($('oblPadMultiple')?.value, 10);
    const target = parseInt($('oblTargetRows')?.value, 10);
    const dummy = parseInt($('oblDummyLookups')?.value, 10);
    const policy = {
      level,
      pad_to_multiple: Number.isFinite(pad) && pad > 0 ? pad : undefined,
      target_rows: Number.isFinite(target) && target > 0 ? target : undefined,
      dummy_value_lookups: Number.isFinite(dummy) && dummy > 0 ? dummy : undefined,
      shuffle: $('oblShuffle')?.value === 'true'
    };
    await call('oblivious.policy.set',{table:t,policy},'oblOut');
  } catch (e) { setOut({error:String(e)},'oblOut'); }
}

async function oblExplain() {
  try { const t = readDbTable('oblDb','oblTable'); await call('oblivious.explain',{table:t},'oblOut'); } catch (e) { setOut({error:String(e)},'oblOut'); }
}

async function oblEvaluate() {
  try {
    const t = readDbTable('oblDb','oblTable');
    const traceRows = ($('oblTraceRows')?.value || '')
      .split(',')
      .map(v => parseInt(v.trim(), 10))
      .filter(v => Number.isFinite(v) && v >= 0);
    await call('oblivious.evaluate',{table:t,trace_rows:traceRows},'oblOut');
  } catch (e) { setOut({error:String(e)},'oblOut'); }
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

function readForensicParams(includeBundle) {
  const from = parseInt($('forFromId')?.value, 10);
  const to = parseInt($('forToId')?.value, 10);
  const limit = parseInt($('forLimit')?.value, 10);
  const db = ($('forDb')?.value || '').trim();
  const table = ($('forTable')?.value || '').trim();
  const op = ($('forOp')?.value || '').trim();
  const bundleId = ($('forBundleId')?.value || '').trim();
  const filter = parseJsonInput($('forFilter')?.value || '', 'Filter');
  const payload = cleanParams({
    from_id: Number.isFinite(from) ? from : undefined,
    to_id: Number.isFinite(to) ? to : undefined,
    limit: Number.isFinite(limit) && limit > 0 ? limit : 100,
    op: op || undefined,
    filter: filter || undefined
  });
  if (db && table) payload.table = { db, table };
  if (includeBundle && bundleId) payload.bundle_id = bundleId;
  return payload;
}

async function forVerify() {
  try {
    const queryRes = await call('forensic.query', readForensicParams(false), 'forOut');
    const query = unwrapRpcResult(queryRes, 'forensic.query');
    const startHash = query?.proof?.preceding_hash || 'genesis';
    const verifyRes = await call('forensic.verify', { records: query?.records || [], start_hash: startHash }, 'forOut');
    const verify = unwrapRpcResult(verifyRes, 'forensic.verify');
    setOut({ query, verify }, 'forOut');
  } catch (e) { setOut({error:String(e)},'forOut'); }
}

async function forQuery() {
  try {
    await call('forensic.query', readForensicParams(false), 'forOut');
  } catch (e) { setOut({error:String(e)},'forOut'); }
}

async function forExport() {
  try {
    await call('forensic.export', readForensicParams(true), 'forOut');
  } catch (e) { setOut({error:String(e)},'forOut'); }
}

// ---------------------------------------------------------------------------
// Views (R08)
// ---------------------------------------------------------------------------
async function viewCreate() {
  try {
    const query = parseJsonInput($('viewQuery')?.value,'Query'); if (!query) throw new Error('Query required');
    const res = await call('view.create',{view:readViewRef(),query},'viewOut');
    renderViewSummary(unwrapRpcResult(res, 'view.create'), 'create');
  } catch (e) { setViewSummaryMarkup('View action failed.'); setOut({error:String(e)},'viewOut'); }
}

async function viewRefresh() {
  try {
    const mode = ($('viewRefreshMode')?.value || 'auto').trim() || 'auto';
    const res = await call('view.refresh',{view:readViewRef(),mode},'viewOut');
    renderViewSummary(unwrapRpcResult(res, 'view.refresh'), 'refresh');
  } catch (e) { setViewSummaryMarkup('View action failed.'); setOut({error:String(e)},'viewOut'); }
}

async function viewEvaluate() {
  try {
    const iterations = parseOptionalU64Input('viewEvalIterations', 'Evaluate iterations') || undefined;
    const res = await call('view.evaluate',{view:readViewRef(),iterations},'viewOut');
    renderViewSummary(unwrapRpcResult(res, 'view.evaluate'), 'evaluate');
  } catch (e) { setViewSummaryMarkup('View action failed.'); setOut({error:String(e)},'viewOut'); }
}

async function viewStatus() {
  try { const res = await call('view.status',{view:readViewRef()},'viewOut'); renderViewSummary(unwrapRpcResult(res, 'view.status'), 'status'); } catch (e) { setViewSummaryMarkup('View action failed.'); setOut({error:String(e)},'viewOut'); }
}

async function viewDrop() {
  try { const res = await call('view.drop',{view:readViewRef()},'viewOut'); renderViewSummary(unwrapRpcResult(res, 'view.drop'), 'drop'); } catch (e) { setViewSummaryMarkup('View action failed.'); setOut({error:String(e)},'viewOut'); }
}

async function viewExplainDeps() {
  try { const res = await call('view.explain_deps',{view:readViewRef()},'viewOut'); renderViewSummary(unwrapRpcResult(res, 'view.explain_deps'), 'deps'); } catch (e) { setViewSummaryMarkup('View action failed.'); setOut({error:String(e)},'viewOut'); }
}

// ---------------------------------------------------------------------------
// Merge & CRDT (R07)
// ---------------------------------------------------------------------------
function readOptionalJsonInput(id, label) {
  const raw = ($(id)?.value || '').trim();
  return raw ? parseJsonInput(raw, label) : undefined;
}

function readMergePolicy(required = false) {
  const raw = ($('mergePolicy')?.value || '').trim();
  if (!raw && !required) return undefined;
  return raw ? parseJsonInput(raw, 'Policy') : { default: { kind: 'builtin', name: 'last_write_wins' }, per_column: {} };
}

function readMergeModuleId() {
  const moduleId = ($('mergeWasmModuleId')?.value || '').trim() || ($('mergeWasmName')?.value || '').trim();
  if (!moduleId) throw new Error('Module ID required');
  return moduleId;
}

async function mergeApply() {
  try {
    const t = readDbTable('mergeDb','mergeTable');
    const pk = parseJsonInput($('mergePk')?.value,'PK') || [];
    const incoming = parseJsonInput($('mergeIncoming')?.value,'Incoming');
    const expected_etag = ($('mergeExpectedEtag')?.value || '').trim() || undefined;
    const min_causality = readOptionalJsonInput('mergeMinCausality', 'Min Causality');
    const policy = readMergePolicy(false);
    await call('merge.apply', cleanParams({table:t,pk,incoming,expected_etag,min_causality,policy}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeRegister() {
  try {
    const t = readDbTable('mergeDb','mergeTable');
    const policy = readMergePolicy(true);
    await call('merge.register', cleanParams({table:t,policy}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeSimulate() {
  try {
    const current = parseJsonInput($('mergeCurrent')?.value,'Current') || {};
    const incoming = parseJsonInput($('mergeIncoming')?.value,'Incoming');
    const policy = readMergePolicy(true);
    await call('merge.simulate', cleanParams({current,incoming,policy}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeEvaluate() {
  try {
    const policy = readMergePolicy(true);
    const cases = parseJsonInput($('mergeEvalCases')?.value, 'Evaluate Cases') || [];
    const iterations = Number($('mergeEvalIterations')?.value || 1) || 1;
    await call('merge.evaluate', cleanParams({policy,cases,iterations}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeWasmRegister() {
  try {
    const module_id = readMergeModuleId();
    const name = ($('mergeWasmName')?.value || '').trim() || undefined;
    const wasm_b64 = ($('mergeWasmB64')?.value || '').trim();
    if (!wasm_b64) throw new Error('Wasm B64 required');
    const capabilities = {
      values_only: true,
      deterministic: true,
      max_fuel: Number($('mergeWasmFuel')?.value || 0) || 0,
      max_memory_bytes: Number($('mergeWasmMemory')?.value || 0) || 0,
      max_output_bytes: Number($('mergeWasmOutput')?.value || 0) || 0,
    };
    await call('merge.wasm.register', cleanParams({module_id,name,wasm_b64,capabilities}), 'mergeOut');
  } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

async function mergeWasmList() { await call('merge.wasm.list',{},'mergeOut'); }

async function mergeWasmDrop() {
  try { await call('merge.wasm.drop',{module_id:readMergeModuleId()},'mergeOut'); } catch (e) { setOut({error:String(e)},'mergeOut'); }
}

// ---------------------------------------------------------------------------
// Wasm Operators (R19)
// ---------------------------------------------------------------------------
function readWasmArtifact() {
  const artifact = ($('wasmArtifact')?.value || STATE.wasmArtifactB64 || '').trim();
  if (!artifact) throw new Error('Compile or paste an artifact first');
  return artifact;
}

async function wasmCompile() {
  try {
    const query = parseJsonInput($('wasmQuery')?.value, 'Query');
    if (!query) throw new Error('Query JSON required');
    const target = $('wasmTarget')?.value || 'wasm32-unknown-unknown';
    const res = await call('wasm.plan.compile', cleanParams({ query, target }), 'wasmOut');
    const result = unwrapRpcResult(res, 'wasm.plan.compile');
    STATE.wasmArtifactB64 = result.artifact_b64 || '';
    if ($('wasmArtifact')) $('wasmArtifact').value = STATE.wasmArtifactB64;
  } catch (e) { setOut({error:String(e)},'wasmOut'); }
}

async function wasmInspect() {
  try {
    const artifact_b64 = readWasmArtifact();
    await call('wasm.plan.inspect', { artifact_b64 }, 'wasmOut');
  } catch (e) { setOut({error:String(e)},'wasmOut'); }
}

async function wasmEdgePackage() {
  try {
    const artifact_b64 = readWasmArtifact();
    const package_name = $('wasmPackageName')?.value.trim() || 'skein-wasm-plan';
    await call('wasm.plan.edge_package', cleanParams({ artifact_b64, package_name }), 'wasmOut');
  } catch (e) { setOut({error:String(e)},'wasmOut'); }
}

async function wasmRun() {
  try {
    const artifact_b64 = readWasmArtifact();
    const args = parseJsonInput($('wasmArgs')?.value, 'Args') || [];
    const result_format = $('wasmResultFormat')?.value || 'objects_json';
    await call('wasm.plan.run', cleanParams({ artifact_b64, args, result_format }), 'wasmOut');
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
      note: $('advNote')?.value.trim() || null,
      status: result.status || 'queued',
      progress_pct: Number.isFinite(result.progress_pct) ? result.progress_pct : 0,
      updated_at_ms: Date.now()
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

async function migrationExport() {
  try {
    const params = migrationParams();
    const res = await call('migration.report_export', cleanParams({...params, title:'SkeinDB migration report'}), 'migrationOut');
    const result = unwrapRpcResult(res, 'migration.report_export');
    const rewrites = result?.report_json?.rewrites || [];
    lastMigrationRewrites = rewrites;
    lastMigrationGeneratedAt = result?.generated_at_ms ? new Date(result.generated_at_ms).toISOString() : new Date().toISOString();
    renderMigrationReport(rewrites);
  } catch (e) { setOut({error:String(e)},'migrationOut'); }
}

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
    btn.addEventListener('click', () => openRpcMethod(m));
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
const HELP_PANEL_REFERENCE = [
  { panel: 'overview',  title: 'Overview',          purpose: 'Operator command center with live connection, selection, and session summary bands.', actions: 'Quick create DB/table, browse data, open cluster, see hardened-research counts.' },
  { panel: 'easy',      title: 'Easy Viewer',       purpose: 'Click-first inline grid editor and guided forms for daily row operations.', actions: 'Browse, insert, edit, delete rows; design schema in the WYSIWYG ALTER planner.' },
  { panel: 'workspace', title: 'SQL Workspace',     purpose: 'Run SQL/SkeinQL, prepare statements, manage explicit transaction handles.', actions: 'Execute (⌘↵), prepare/execute, begin/commit/rollback, ETag-aware patches.' },
  { panel: 'schema',    title: 'Schema',            purpose: 'Database, table, column, secondary-index, and schema-evolution DDL with conflict-free rollout planning.', actions: 'CREATE/ALTER tables, manage indexes, propose/inspect/simulate/apply schema changes (R15).' },
  { panel: 'data',      title: 'Data Browse',       purpose: 'Row browser with filters, pagination, and inline edits.', actions: 'Filter, paginate, patch rows, cross-link to CDC and replay panels.' },
  { panel: 'cluster',   title: 'Cluster',           purpose: 'Topology, transport capabilities, shard placement, and node enrollment.', actions: 'Observe nodes, enroll members, plan shard placement, inspect QUIC/HTTP transport.' },
  { panel: 'settings',  title: 'Settings',          purpose: 'Server settings and feature flags with safe round-tripping.', actions: 'Read/update settings; toggle dedup, MVCC, cache, and research feature flags.' },
  { panel: 'engine',    title: 'Engine Config',     purpose: 'Storage, compaction, energy policy, and learned-index controls.', actions: 'Configure compaction (R20 energy-aware), MVCC, learned ValueID index (R01).' },
  { panel: 'users',     title: 'Users & Grants',    purpose: 'Identity and access control surface.', actions: 'Create users, assign roles, grant per-database privileges.' },
  { panel: 'security',  title: 'Security',          purpose: 'Tokens, sensitive operations, top-query review.', actions: 'Manage bearer tokens, review activity, enforce limits.' },
  { panel: 'encryption',title: 'Encryption',        purpose: 'At-rest encryption mode and key management.', actions: 'Toggle ENC_OFF/ENC_RANDOM/ENC_MLE_DB, register keys, set active key, rotate.' },
  { panel: 'import',    title: 'Import / Export',   purpose: 'Bulk import data and export schemas/rows.', actions: 'Import JSON/CSV, export schema and table contents.' },
  { panel: 'telemetry', title: 'Telemetry',         purpose: 'Workload insights, plan cache, slow queries, compatibility usage.', actions: 'Inspect plan cache, slow query log, feature usage histograms.' },
  { panel: 'cdc',       title: 'CDC',               purpose: 'Change-data-capture subscriptions over tables and prepared queries.', actions: 'Subscribe, poll, ack, close; inspect lag and event grid.' },
  { panel: 'replay',    title: 'Time Travel & Replay', purpose: 'Point-in-time queries, history retention, replay-bundle integrity, edge bundles.', actions: 'Run point-in-time, GC history, export/import/run replay bundles (with R14 redaction), R18 performance variance reports.' },
  { panel: 'research',  title: 'Research Dashboard',purpose: 'Single-pane status for all 20 research tracks.', actions: 'See hardened vs prototype state, jump to each track\'s panel, open relevant RPC methods.' },
  { panel: 'vectors',   title: 'Vectors (R10)',     purpose: 'First-class vector columns with kNN search.', actions: 'Insert, index status, top-k similarity search.' },
  { panel: 'privacy',   title: 'Privacy & DP',      purpose: 'Differential privacy aggregates and oblivious execution.', actions: 'Run DP aggregates with budget, register oblivious policies, explain padding.' },
  { panel: 'forensics', title: 'Forensics (R06)',   purpose: 'Hash-chained audit log with filtered verification and forensic proof bundles.', actions: 'Audit status, verify chain, query by DB/table/op/id/filter, proof-verify the returned slice, and export report bundles.' },
  { panel: 'views',     title: 'Views (R08)',       purpose: 'Incremental materialized views with dependency graphs.', actions: 'Create, refresh, evaluate incremental-vs-full correctness, status, drop, explain dependencies.' },
  { panel: 'merge',     title: 'Merge & CRDT',      purpose: 'Client-side merge functions, conflict evaluation, and values-only Wasm merge modules.', actions: 'Apply/register policies, simulate current+incoming rows, evaluate conflict workloads, manage Wasm modules.' },
  { panel: 'wasm',      title: 'Wasm Operators',    purpose: 'User-defined Wasm query plan operators.', actions: 'Compile, run, inspect plan artifacts, package for edge.' },
  { panel: 'advisor',   title: 'Index Advisor',     purpose: 'Workload-driven index recommendation and synthesis.', actions: 'Synthesize, history, apply, dismiss recommendations.' },
  { panel: 'migration', title: 'Migration',         purpose: 'Compatibility telemetry, rewrite previews, intent reports.', actions: 'Preview rewrites, export intent reports as JSON/Markdown.' },
  { panel: 'nl',        title: 'NL Lab',            purpose: 'Natural-language to SkeinQL translation and SQL autoparameterization.', actions: 'Translate, explain, approve-and-execute; classify and analyze.' },
  { panel: 'rpc',       title: 'RPC Explorer',      purpose: 'Browse every advertised method and dispatch JSON params directly.', actions: 'Filter methods, load templates, send raw RPC.' },
  { panel: 'help',      title: 'Help & Docs',       purpose: 'This page. Quick start, panel reference, research index, shortcuts, glossary, and doc links.', actions: 'Search topics, jump to any panel, open canonical documentation.' }
];

function renderHelpPanel() {
  const panelTbody = document.getElementById('helpPanelTable');
  if (panelTbody) {
    panelTbody.innerHTML = HELP_PANEL_REFERENCE.map(row => `
      <tr data-help-row="panel" data-help-text="${row.panel} ${row.title} ${row.purpose} ${row.actions}">
        <td><strong>${row.title}</strong></td>
        <td>${row.purpose}</td>
        <td>${row.actions}</td>
        <td><button class="sm" data-panel="${row.panel}">Open</button></td>
      </tr>`).join('');
    panelTbody.querySelectorAll('button[data-panel]').forEach(btn => {
      btn.addEventListener('click', () => setActivePanel(btn.dataset.panel, true));
    });
  }
  const researchTbody = document.getElementById('helpResearchTable');
  if (researchTbody) {
    researchTbody.innerHTML = RESEARCH_TRACKS.map(track => `
      <tr data-help-row="research" data-help-text="${track.id} ${track.title} ${track.desc} ${track.methods.join(' ')} ${track.status}">
        <td><strong>${track.id}</strong></td>
        <td>${track.title}<div class="hint" style="font-size:11px">${track.desc}</div></td>
        <td><span class="pill ${track.status === 'hardened' ? 'ok' : 'warn'}">${track.status}</span></td>
        <td><code style="font-size:11px">${track.methods.slice(0,3).join(', ')}${track.methods.length > 3 ? '…' : ''}</code></td>
        <td><button class="sm" data-panel="${track.panel}">Open</button></td>
      </tr>`).join('');
    researchTbody.querySelectorAll('button[data-panel]').forEach(btn => {
      btn.addEventListener('click', () => setActivePanel(btn.dataset.panel, true));
    });
  }
  const search = document.getElementById('helpSearch');
  const results = document.getElementById('helpResults');
  if (search && !search.dataset.wired) {
    search.dataset.wired = '1';
    search.addEventListener('input', () => {
      const q = search.value.trim().toLowerCase();
      const sections = document.querySelectorAll('section[data-panel="help"] [data-help-section]');
      let visible = 0;
      let matchedRows = 0;
      sections.forEach(sec => {
        const rows = sec.querySelectorAll('[data-help-row]');
        if (rows.length === 0) {
          const text = sec.textContent.toLowerCase();
          const hit = !q || text.includes(q);
          sec.style.display = hit ? '' : 'none';
          if (hit) visible++;
          return;
        }
        let anyVisible = false;
        rows.forEach(row => {
          const text = (row.dataset.helpText || row.textContent).toLowerCase();
          const hit = !q || text.includes(q);
          row.style.display = hit ? '' : 'none';
          if (hit) { anyVisible = true; matchedRows++; }
        });
        sec.style.display = (anyVisible || !q) ? '' : 'none';
        if (anyVisible || !q) visible++;
      });
      if (results) {
        results.textContent = q
          ? `Showing ${visible} section(s), ${matchedRows} matching row(s) for "${q}".`
          : '';
      }
    });
  }
}

function setActivePanel(panel, updateHash) {
  document.querySelectorAll('.panel').forEach(el => el.classList.toggle('active', el.dataset.panel === panel));
  document.querySelectorAll('.nav-item').forEach(el => el.classList.toggle('active', el.dataset.panel === panel));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.toggle('active', el.dataset.panel === panel));
  updateHeader(panel); updateContext();
  if (panel === 'workspace') {
    renderPreparedWorkspace();
    renderTxState();
  }
  if (panel === 'cdc') renderCdcPanel();
  if (panel === 'replay') renderReplayPanel();
  if (panel === 'help') renderHelpPanel();
  if (panel === 'overview') {
    refreshTopTables();
    refreshSlowQueries();
    refreshActiveSessions();
    refreshIndexHealth();
  }
  if (panel === 'security') {
    securityRefreshTokens();
    securityTopQueries();
  }
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
    // ? = open Help panel (only when not typing in an input)
    if (e.key === '?' && !e.metaKey && !e.ctrlKey && !e.altKey) {
      const tag = (document.activeElement && document.activeElement.tagName) || '';
      if (tag !== 'INPUT' && tag !== 'TEXTAREA' && tag !== 'SELECT') {
        e.preventDefault();
        setActivePanel('help', true);
      }
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
wire('btnCompactionStatus', compactionStatus);
wire('btnCompactionSavePolicy', compactionSavePolicy);
wire('btnCompactionPause', compactionPause);
wire('btnCompactionResume', compactionResume);

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
wire('easyDesignLoad', easyDesignLoad);
wire('easyDesignAddCol', easyDesignAddColumn);
wire('easyDesignReset', easyDesignReset);
wire('easyDesignPreviewBtn', easyDesignRefreshPreview);
wire('easyDesignApply', easyDesignApply);
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
wire('btnPreparedPrepare', preparedPrepareCurrentQuery);
wire('btnPreparedExecute', preparedExecuteCurrentQuery);
wire('btnPreparedCopyHttp', preparedCopyGetUrl);
wire('btnPreparedUseForCdc', preparedUseForCdc);
wire('btnTxBegin', txBegin);
wire('btnTxCommit', txCommit);
wire('btnTxRollback', txRollback);

// Schema
wire('btnSchemaListDb', schemaListDatabases);
wire('btnSchemaListTables', schemaListTables);
wire('btnSchemaDescribe', schemaDescribe);
wire('btnSchemaCreateDb', schemaCreateDb);
wire('btnSchemaCreateTable', schemaCreateTable);
wire('btnSchemaDropDb', schemaDropDb);
wire('btnSchemaDropTable', schemaDropTable);
wire('btnSchemaEvolutionPropose', schemaProposeChange);
wire('btnSchemaEvolutionStatus', schemaMergeStatus);
wire('btnSchemaEvolutionSimulate', schemaSimulateRollout);
wire('btnSchemaEvolutionApply', schemaApplyMerge);
wire('btnSchemaBuilderSeed', schemaBuilderSeedDefaults);
wire('btnSchemaBuilderAddCol', () => schemaBuilderAddColumn());
wire('btnSchemaBuilderLoad', schemaBuilderLoadCurrent);
wire('btnSchemaBuilderSync', () => schemaBuilderSyncToJson(true));
wire('btnSchemaBuilderCreateDb', schemaBuilderCreateDb);
wire('btnSchemaBuilderCreateTable', schemaBuilderCreateTable);
wire('btnSchemaLoadIndexes', schemaLoadIndexes);
wire('btnSchemaCreateIndex', schemaCreateIndex);
wire('btnSchemaDropIndex', schemaDropIndex);
wire('btnSchemaUseSelection', schemaUseSelectedTable);

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
wire('btnVecBenchmark', vecBenchmark);
wire('btnVecInsert', vecInsert);
wire('btnVecIndexStatus', vecIndexStatus);

// Privacy
wire('btnDpAggregate', dpAggregate);
wire('btnDpEvaluate', dpEvaluate);
wire('btnDpBudgetGet', dpBudgetGet);
wire('btnDpBudgetSet', dpBudgetSet);
wire('btnDpAudit', dpAudit);
wire('btnOblGet', oblGet);
wire('btnOblSet', oblSet);
wire('btnOblExplain', oblExplain);
wire('btnOblEvaluate', oblEvaluate);

// CDC
wire('btnCdcSubscribe', cdcSubscribe);
wire('btnCdcPoll', cdcPoll);
wire('btnCdcAck', cdcAck);
wire('btnCdcClose', cdcClose);
wire('btnCdcUsePrepared', cdcUseLatestPrepared);
wire('btnCdcSubscribeQuery', cdcSubscribeQuery);
const cdcSubIdSelect = $('cdcSubId');
if (cdcSubIdSelect) cdcSubIdSelect.addEventListener('change', () => { STATE.cdcSelectedSubId = cdcSubIdSelect.value; renderCdcPanel(); });

// Time travel + replay
wire('btnTimeTravelSeed', timeTravelSeedQuery);
wire('btnTimeTravelRun', timeTravelRunQuery);
wire('btnTimeTravelClear', timeTravelClear);
wire('btnHistoryStatus', historyLoadStatus);
wire('btnHistorySetPolicy', historySavePolicy);
wire('btnHistoryGc', historyRunGc);
wire('btnReplayExport', replayExportBundle);
wire('btnReplayDownload', replayDownloadBundle);
wire('btnReplayUseLastBundle', replayUseLastBundle);
wire('btnReplayImport', replayImportBundle);
wire('btnReplayRunIntegrity', replayRunIntegrity);
wire('btnEdgeRequest', edgeRequestBundle);
wire('btnEdgeApply', edgeApplyBundle);
wire('btnEdgeStatus', edgeStatus);
const replayWorkspaceSelect = $('replayWorkspaceSelect');
if (replayWorkspaceSelect) replayWorkspaceSelect.addEventListener('change', () => {
  STATE.replaySelectedWorkspaceId = replayWorkspaceSelect.value;
  const input = $('replayWorkspaceId');
  if (input) input.value = STATE.replaySelectedWorkspaceId;
  renderReplayPanel();
});
const replayBundleFile = $('replayBundleFile');
if (replayBundleFile) replayBundleFile.addEventListener('change', replayBundleFileChanged);
const replayBundleJson = $('replayBundleJson');
if (replayBundleJson) replayBundleJson.addEventListener('change', () => {
  try {
    STATE.replayLastBundle = parseJsonInput(replayBundleJson.value, 'Replay bundle');
    renderReplayPanel();
  } catch (_) {}
});
const edgeBundleJson = $('edgeBundleJson');
if (edgeBundleJson) edgeBundleJson.addEventListener('change', () => {
  try { STATE.edgeLastBundle = parseJsonInput(edgeBundleJson.value, 'Edge bundle'); } catch (_) {}
});

// Forensics
wire('btnForAuditStatus', forAuditStatus);
wire('btnForAuditVerify', forAuditVerify);
wire('btnForVerify', forVerify);
wire('btnForQuery', forQuery);
wire('btnForExport', forExport);

// Views
wire('btnViewCreate', viewCreate);
wire('btnViewRefresh', viewRefresh);
wire('btnViewEvaluate', viewEvaluate);
wire('btnViewStatus', viewStatus);
wire('btnViewDrop', viewDrop);
wire('btnViewExplainDeps', viewExplainDeps);

// Merge
wire('btnMergeApply', mergeApply);
wire('btnMergeRegister', mergeRegister);
wire('btnMergeSimulate', mergeSimulate);
wire('btnMergeEvaluate', mergeEvaluate);
wire('btnMergeWasmRegister', mergeWasmRegister);
wire('btnMergeWasmList', mergeWasmList);
wire('btnMergeWasmDrop', mergeWasmDrop);

// Wasm
wire('btnWasmCompile', wasmCompile);
wire('btnWasmInspect', wasmInspect);
wire('btnWasmEdgePackage', wasmEdgePackage);
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
wire('btnMigrationExport', migrationExport);
wire('btnMigrationDownloadJson', () => exportMigrationReport('json'));
wire('btnMigrationDownloadMd', () => exportMigrationReport('md'));
wire('btnMigrationDownloadHtml', () => exportMigrationReport('html'));
wire('btnMigrationCopyMd', copyMigrationMarkdown);

// Security Tokens
wire('btnSecCreateToken', securityCreateToken);
wire('btnSecRefreshTokens', securityRefreshTokens);
wire('btnSecTopQueries', securityTopQueries);

// Encryption (T193)
wire('btnEncStatus', () => call('settings.encryption.status', {}, 'encStatusOut'));
wire('btnEncSetMode', () => {
  const db = (document.getElementById('encModeDb')?.value || '').trim();
  const mode = document.getElementById('encModeSelect')?.value || 'off';
  if (!db) { document.getElementById('encModeOut').textContent = 'Database name required.'; return; }
  return call('settings.encryption.set_mode', { db, mode }, 'encModeOut');
});
wire('btnEncRegisterKey', () => {
  const db = (document.getElementById('encRegDb')?.value || '').trim();
  const key_id = (document.getElementById('encRegKeyId')?.value || '').trim();
  const master_key_b64 = (document.getElementById('encRegMaster')?.value || '').trim();
  const make_active = (document.getElementById('encRegMakeActive')?.value || 'true') === 'true';
  if (!db || !key_id || !master_key_b64) {
    document.getElementById('encRegOut').textContent = 'db, key_id, and base64 master key are required.';
    return;
  }
  return call('settings.encryption.register_key', { db, key_id, master_key_b64, make_active }, 'encRegOut');
});
wire('btnEncSetActive', () => {
  const db = (document.getElementById('encActiveDb')?.value || '').trim();
  const key_id = (document.getElementById('encActiveKeyId')?.value || '').trim();
  if (!db || !key_id) { document.getElementById('encActiveOut').textContent = 'db and key_id required.'; return; }
  return call('settings.encryption.set_active_key', { db, key_id }, 'encActiveOut');
});
wire('btnEncRotate', () => {
  const db = (document.getElementById('encRotateDb')?.value || '').trim();
  const new_key_id = (document.getElementById('encRotateKeyId')?.value || '').trim();
  if (!db || !new_key_id) { document.getElementById('encRotateOut').textContent = 'db and new_key_id required.'; return; }
  return call('settings.encryption.rotate_key', { db, new_key_id }, 'encRotateOut');
});

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
renderPreparedWorkspace();
renderTxState();
schemaBuilderSeedDefaults();
easySetBuilderRows(defaultEasyBuilderRows());
easyRefreshTargetsFromTree();
setConnStatus('warn', 'Disconnected', 'Connect to start.');

// Auto-connect if same origin
setTimeout(() => { if (getBaseUrl() === window.location.origin) connect(); }, 200);
