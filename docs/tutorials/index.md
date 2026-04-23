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
  <a class="home-card" href="cdc-with-sse.html"><div class="home-icon">📡</div><h3>CDC with SSE</h3><p>Create a table subscription, stream changes over SSE, ack offsets, and reconnect safely.</p></a>
  <a class="home-card" href="encryption-and-key-rotation.html"><div class="home-icon">🔐</div><h3>Encryption and key rotation</h3><p>Register keys, switch modes, inspect status, and rotate the active database key.</p></a>
  <a class="home-card" href="replay-bundles-and-integrity.html"><div class="home-icon">🧾</div><h3>Replay bundles and integrity</h3><p>Export a deterministic replay bundle, import it into a workspace, and verify checksums.</p></a>
</div>

## Suggested paths

1. **Application developer**: [Quickstart](quickstart.html) → [Your first query (SkeinQL)](first-query.html) → [MySQL in 5 minutes](mysql-in-5-min.html) or [PostgreSQL in 5 minutes](postgresql-in-5-min.html) → [CDC with SSE](cdc-with-sse.html).
2. **Operator / admin**: [Admin console tour](admin-tour.html) → [Monitoring and metrics](monitoring-and-metrics.html) → [Setting up a 3-node cluster](setting-up-cluster.html) → [Encryption and key rotation](encryption-and-key-rotation.html) → [Replay bundles and integrity](replay-bundles-and-integrity.html).

## Reference

- [SkeinQL reference](skeinql.html) — every RPC method, families, and payloads.
- [MySQL compatibility](mysql-compat.html) — supported statements and known deviations.
- [PostgreSQL compatibility](pg-compat.html) — partial v3 wire protocol status.
- [Configuration](configuration.html) — every key in `skeindb-config.json`.
- [Observability](observability.html) — Prometheus metrics, tracing, logs.

## Status

The **[True Status Matrix](true-status-matrix.html)** is the single source of truth for what is hardened, what is prototype, and what is planned. 14 of the 20 research tracks are currently marked hardened with evidence and test links.

Looking to edit something? Each page has an "Edit this page on GitHub" link at the bottom.
