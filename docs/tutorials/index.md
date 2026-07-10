# SkeinDB documentation

Welcome to the SkeinDB documentation. SkeinDB is a **single-binary database engine** with cell-interned MVCC storage, a native typed RPC (**SkeinQL**), a MySQL-compatible wire protocol, a partial PostgreSQL wire protocol, and a full web admin console — all in one executable.

This site is organised into six sections:

<div class="home-grid">
  <a class="home-card" href="quickstart.html"><div class="home-icon">🚀</div><h3>Quickstart</h3><p>Install, start the server, and run your first query in under 5 minutes.</p></a>
  <a class="home-card" href="getting-started.html"><div class="home-icon">📖</div><h3>User Guide</h3><p>Day-to-day usage: SkeinQL, MySQL/Postgres compatibility, query features, offline writes.</p></a>
  <a class="home-card" href="configuration.html"><div class="home-icon">🛠</div><h3>Admin Guide</h3><p>Configuration, clustering, observability, audit WAL, and the web admin console.</p></a>
  <a class="home-card" href="skeinir.html"><div class="home-icon">⚡</div><h3>Developer / API</h3><p>SkeinIR internals, research extensions, Wasm UDFs, CRDT merges, CDC, ETags.</p></a>
  <a class="home-card" href="on-disk-format.html"><div class="home-icon">🧬</div><h3>Internals</h3><p>On-disk format, delta chains, column snapshots, convergent encryption, QUIC.</p></a>
  <a class="home-card" href="research-agenda.html"><div class="home-icon">📐</div><h3>Research</h3><p>The 20-track research agenda, project backlog, and authoritative status matrix.</p></a>
</div>

## New here? Start with these five

1. [Quickstart — 5 minutes](quickstart.html) — install, run, connect.
2. [Your first query (SkeinQL)](first-query.html) — create a table and query it using the native RPC.
3. [MySQL in 5 minutes](mysql-in-5-min.html) — if you already speak MySQL, connect with `mysql` and go.
4. [Admin console tour](admin-tour.html) — a guided walkthrough of the 26-panel web admin.
5. [Setting up a 3-node cluster](setting-up-cluster.html) — tokens, topology, promotion.

## More guided walkthroughs

<div class="home-grid">
  <a class="home-card" href="postgresql-in-5-min.html"><div class="home-icon">🐘</div><h3>PostgreSQL in 5 minutes</h3><p>Bring up the PG listener, connect with <code>psql</code>, and try the current compatibility surface.</p></a>
  <a class="home-card" href="monitoring-and-metrics.html"><div class="home-icon">📈</div><h3>Monitoring and metrics</h3><p>Use <code>/health</code>, <code>/metrics</code>, <code>stats.snapshot</code>, and SkeinAdmin to watch a live server.</p></a>
  <a class="home-card" href="vector-rag.html"><div class="home-icon">🧠</div><h3>Vector RAG retrieval</h3><p>Run a credential-free sample app that seeds embeddings, calls <code>vector.search</code>, and builds grounded context.</p></a>
  <a class="home-card" href="cdc-with-sse.html"><div class="home-icon">📡</div><h3>CDC with SSE</h3><p>Create a table subscription, stream changes over SSE, ack offsets, and reconnect safely.</p></a>
  <a class="home-card" href="encryption-and-key-rotation.html"><div class="home-icon">🔐</div><h3>Encryption and key rotation</h3><p>Register keys, switch modes, inspect status, and rotate the active database key.</p></a>
  <a class="home-card" href="rbac-and-access-control.html"><div class="home-icon">🔑</div><h3>RBAC and access control</h3><p>Turn on RBAC, mint read-only and database-scoped tokens, and create per-database users with grants.</p></a>
  <a class="home-card" href="replay-bundles-and-integrity.html"><div class="home-icon">🧾</div><h3>Replay bundles and integrity</h3><p>Export a deterministic replay bundle, import it into a workspace, and verify checksums.</p></a>
</div>

## Advanced & research features

<div class="home-grid">
  <a class="home-card" href="oblivious-execution.html"><div class="home-icon">🛡️</div><h3>Privacy &amp; Differential Privacy</h3><p>R04/R05: DP aggregates with Laplace noise, oblivious execution for access-pattern protection, budgets, audit, and privacy ETags.</p></a>
  <a class="home-card" href="incremental-views.html"><div class="home-icon">🔄</div><h3>Incremental Views &amp; Pipelines</h3><p>R08: Dependency-graph materialized view maintenance, refresh modes, evaluate, explain_deps, and tight integration with CDC/ETags.</p></a>
  <a class="home-card" href="audit-wal.html"><div class="home-icon">🕵️</div><h3>Forensic Audit &amp; Tamper-Evident WAL</h3><p>R06: Hash-chained WAL, forensic queries, Merkle proofs, and exportable proof bundles for compliance or incident response.</p></a>
  <a class="home-card" href="delta-values.html"><div class="home-icon">🧬</div><h3>Delta-chained Values + ETags</h3><p>Core MVCC storage efficiencies, traffic reduction, cache-coherent reads via ETags, and causal consistency.</p></a>
  <a class="home-card" href="research-agenda.html"><div class="home-icon">📐</div><h3>Full Research Tracks (R01–R20)</h3><p>Learned indexes, delta chains, vectors, Wasm operators, schema evolution, energy-aware compaction, and more. See the True Status Matrix for hardened vs. prototype.</p></a>
</div>

## Suggested paths

1. **Application developer**: [Quickstart](quickstart.html) → [Your first query (SkeinQL)](first-query.html) → [Vector RAG retrieval](vector-rag.html) → [MySQL in 5 minutes](mysql-in-5-min.html) or [PostgreSQL in 5 minutes](postgresql-in-5-min.html) → [CDC with SSE](cdc-with-sse.html).
2. **Operator / admin**: [Admin console tour](admin-tour.html) → [Monitoring and metrics](monitoring-and-metrics.html) → [RBAC and access control](rbac-and-access-control.html) → [Setting up a 3-node cluster](setting-up-cluster.html) → [Encryption and key rotation](encryption-and-key-rotation.html) → [Replay bundles and integrity](replay-bundles-and-integrity.html).

## Reference

- [SkeinQL reference](skeinql.html) — every RPC method, families, and payloads.
- [MySQL compatibility](mysql-compat.html) — supported statements and known deviations.
- [PostgreSQL compatibility](pg-compat.html) — partial v3 wire protocol status.
- [Configuration](configuration.html) — every key in `skeindb-config.json`.
- [Observability](observability.html) — Prometheus metrics, tracing, logs.

## Status

The **[True Status Matrix](true-status-matrix.html)** is the short authority for current compatibility claims, partial core phases, and research maturity. Use it to see what is shipped, what remains partial, and what should not be overclaimed.

Looking to edit something? Each page has an "Edit this page on GitHub" link at the bottom.
