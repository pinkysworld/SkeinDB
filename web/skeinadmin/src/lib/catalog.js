/* SkeinAdmin – catalog.js
 * Static, dependency-free catalog data extracted from main.js as the first step
 * of modularizing the console. Pure data only: panel metadata, the R01-R20
 * research track index, and the feature-center grid. No DOM or runtime state.
 */

// ---------------------------------------------------------------------------
// Panel metadata
// ---------------------------------------------------------------------------
export const PANEL_META = {
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
export const RESEARCH_TRACKS = [
  { id: 'R01', title: 'Learned Index Structures', desc: 'CDF-based learned indexes for ValueID lookup.', methods: ['stats.snapshot', 'system.capabilities'], panel: 'overview', status: 'hardened' },
  { id: 'R02', title: 'Adaptive Row-Column Hybrid', desc: 'Dynamic row/column execution selection.', methods: ['system.capabilities', 'settings.get'], panel: 'engine', status: 'hardened' },
  { id: 'R03', title: 'Delta-Chain Topology', desc: 'Linear, tree, skip-list delta chains for versioned values.', methods: ['stats.snapshot', 'settings.get'], panel: 'engine', status: 'hardened' },
  { id: 'R04', title: 'Differential Privacy', desc: 'DP aggregates with calibrated Laplace noise.', methods: ['dp.aggregate', 'dp.evaluate', 'dp.budget.get', 'dp.budget.set', 'dp.audit.log'], panel: 'privacy', status: 'hardened' },
  { id: 'R05', title: 'Oblivious Execution', desc: 'Padding, dummy lookups, leakage reports, and overhead reports for access-pattern protection.', methods: ['oblivious.policy.get', 'oblivious.policy.set', 'oblivious.explain', 'oblivious.evaluate'], panel: 'privacy', status: 'hardened' },
  { id: 'R06', title: 'Forensic Audit', desc: 'Filtered hash-chain queries with boundary, checkpoint, and Merkle inclusion proofs.', methods: ['maintenance.audit_status', 'maintenance.audit_verify', 'forensic.verify', 'forensic.query', 'forensic.export'], panel: 'forensics', status: 'hardened' },
  { id: 'R07', title: 'Merge & CRDT', desc: 'Client-side merge functions with conflict hooks, offline queues, evaluation, and values-only Wasm execution.', methods: ['merge.apply', 'merge.register', 'merge.simulate', 'merge.evaluate', 'merge.wasm.register', 'merge.wasm.list', 'merge.wasm.drop'], panel: 'merge', status: 'hardened' },
  { id: 'R08', title: 'Incremental Views', desc: 'Dependency-graph-driven materialized view maintenance.', methods: ['view.create', 'view.refresh', 'view.evaluate', 'view.status', 'view.drop', 'view.explain_deps'], panel: 'views', status: 'hardened' },
  { id: 'R09', title: 'QUIC Transport', desc: 'HTTP/3 and QUIC-native protocol with prepared-query streams, 0-RTT write rejection, and rebind coverage; comparative p99 benchmarking remains open.', methods: ['transport.capabilities'], panel: 'cluster', status: 'hardened' },
  { id: 'R10', title: 'Vector Embeddings', desc: 'First-class vector columns with kNN search and recall/latency benchmarking.', methods: ['vector.search', 'vector.benchmark', 'vector.insert', 'vector.index.status'], panel: 'vectors', status: 'hardened' },
  { id: 'R11', title: 'Autoparameterization', desc: 'LLM-assisted SQL parameterization.', methods: ['ai.autoparam.analyze', 'ai.autoparam.classify', 'ai.autoparam.classifiers', 'ai.autoparam.label_schema', 'ai.autoparam.feedback', 'ai.autoparam.metrics'], panel: 'nl', status: 'hardened' },
  { id: 'R12', title: 'NL-to-SkeinQL', desc: 'Natural language query translation with verification.', methods: ['ai.nl.translate', 'ai.nl.explain', 'ai.nl.execute'], panel: 'nl', status: 'hardened' },
  { id: 'R13', title: 'Causal Consistency', desc: 'ETag-chain causal ordering across replicas.', methods: ['query.patch', 'query.select', 'query.subscribe'], panel: 'workspace', status: 'hardened' },
  { id: 'R14', title: 'Edge Bundles', desc: 'Geo-distributed replay bundles with edge caching.', methods: ['edge.bundle.request', 'edge.bundle.apply', 'edge.bundle.status'], panel: 'replay', status: 'hardened' },
  { id: 'R15', title: 'Schema Evolution', desc: 'Conflict-free schema evolution with divergence guidance, rollout simulation, and controlled apply.', methods: ['schema.propose_change', 'schema.merge_status', 'schema.simulate_rollout', 'schema.apply_merge'], panel: 'schema', status: 'hardened' },
  { id: 'R16', title: 'Index Advisor', desc: 'Workload-driven index synthesis and recommendation.', methods: ['advisor.index_synthesize', 'advisor.evaluate', 'advisor.history', 'advisor.apply_index', 'advisor.dismiss', 'advisor.retire_unused'], panel: 'advisor', status: 'hardened' },
  { id: 'R17', title: 'Migration Hints', desc: 'Compatibility telemetry and rewrite previews.', methods: ['migration.rewrite_preview', 'migration.intent_report', 'migration.report_export'], panel: 'migration', status: 'hardened' },
  { id: 'R18', title: 'Perf Replay', desc: 'Snapshot + replay for performance regression testing.', methods: ['maintenance.replay.export', 'maintenance.replay.import', 'maintenance.replay.run'], panel: 'replay', status: 'prototype' },
  { id: 'R19', title: 'Wasm Operators', desc: 'User-defined Wasm query plan operators.', methods: ['wasm.plan.compile', 'wasm.plan.inspect', 'wasm.plan.perf_report', 'wasm.plan.edge_package', 'wasm.plan.run'], panel: 'wasm', status: 'prototype' },
  { id: 'R20', title: 'Energy-Aware Compaction', desc: 'Carbon-aware scheduling for background compaction.', methods: ['maintenance.compaction.status', 'maintenance.compaction.set_policy', 'maintenance.compaction.pause', 'maintenance.compaction.resume'], panel: 'engine', status: 'hardened' }
];

// ---------------------------------------------------------------------------
// Feature center items
// ---------------------------------------------------------------------------
export const FEATURE_CENTER = [
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
