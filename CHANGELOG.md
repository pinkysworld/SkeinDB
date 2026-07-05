# Changelog

## Unreleased

Production-readiness hardening (all opt-in / additive; default behavior unchanged).

- **Cooperative statement timeout.** A runaway query could previously hold the global engine lock indefinitely. SELECT execution now installs a per-thread deadline (RAII-guarded; nested subqueries share one budget) that the executor's scan/batch loops check cooperatively, aborting with a `statement_timeout` error. Opt-in via `SKEINDB_STATEMENT_TIMEOUT_MS` (0/unset = disabled); checks are amortized (every 1024 rows / once per batch) so per-row cost is negligible.
- **Deeper observability.** `/metrics` now also exposes per-method query latency (sum/max), error counts, rows-returned, overall latency quantiles (p50/p95/p99), and cheap storage-engine internals (tables, streaming tables, dirty tables + mutations-since-flush as deferred-flush lag, WAL size, total rows) via a new O(tables) `runtime_storage_metrics` that never scans rows. Adds an opt-in slow-query log (`SKEINDB_SLOW_QUERY_MS`) that logs completed queries at/above the threshold at WARN.

## v0.3.22 - 2026-07-04

Storage Slice 3 + Slice 4: the row store now defers snapshot flushes and can stream tables larger than RAM at query time, plus the `wasmtime`/`getrandom` major bumps.

- **Deferred snapshot flush (storage Slice 3).** Row mutations (`insert`/`update`/`delete`) no longer rewrite the full table snapshot on every commit. Durability comes from the WAL fsync; the table is marked dirty and its snapshot is flushed in a batch once `WAL_FLUSH_THRESHOLD` mutations accumulate (and at every checkpoint), which removes the O(rows) write amplification per mutation while bounding WAL size and crash-recovery replay time. WAL replay on open now also re-interns recovered cell values into the content-addressed ValueStore so dedup/value-ref stats are correct after recovery. Encryption-locked and corrupt tables still route through `persist_table`, preserving their write-refusal.
- **Streaming segment load (storage Slice 4, first step).** Segment-backed tables now load by streaming length-prefixed row records off disk one at a time, instead of reading the entire encoded `.rseg` file into a buffer and then decoding it. This removes the full-file buffer (roughly halving peak load memory for large segment tables) and establishes the incremental segment-read primitive. Corruption detection is unchanged (a present-but-unreadable segment is still treated as corrupt, not empty). This established the incremental segment-read primitive that the query-time streaming below builds on.
- **Query-time streaming reads (storage Slice 4, opt-in).** A large table can now be *read* without materializing it into memory. `TableData` carries a residency — `Resident` (in-memory, the default) or `Streaming` (rows stay in the on-disk `.rseg`, read on demand) — and every read funnels through one seam (`Engine::for_each_table_row`): resident tables iterate memory, streaming tables stream the segment under a shared borrow (pure file I/O, no materialization). Point lookups seek directly to a record via an in-memory `StreamingIndex` (primary-key → byte-offset plus a value-ref dictionary, never the rows), so `WHERE pk = …` stays a single seek on a table larger than RAM; the streaming decoder is value-ref-correct (two passes: build the interned-literal dictionary, then decode+visit, since a record may reference a value seeded in another record). Writes are unaffected: `get_table_mut` — the single choke point every mutation acquires its `&mut` through — materializes a streaming table back to resident first, and `persist_table` / history-GC skip streaming tables so an empty in-memory image can never overwrite the segment (WAL recovery likewise materializes before replay). Opt-in via `SKEINDB_STREAMING_MIN_BYTES` (default off ⇒ load behaves exactly as before); eligible tables are large segment-backed tables with a primary key and no embedding/oblivious features. v1 targets read-mostly tables accessed by SELECT / pk-lookup / DML; views, snapshots, differential-privacy aggregates, and CDC over a streaming base are unsupported (they observe it as empty), and writing a larger-than-RAM table materializes it and so needs the RAM. Covered by new streaming-reader, streaming-index point-lookup, streaming-SELECT (both executor entry points), materialize-on-write, and eligibility tests.
- Bumps `wasmtime` 45→46 and `getrandom` 0.3→0.4 (plus the grouped minor/patch dependency updates) on top of v0.3.21.
- Validated with clean `cargo fmt --all -- --check`, clean `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and a green `cargo test -p skeindb` suite including the new deferred-flush and query-time streaming (streaming reader, streaming-index point lookup, streaming SELECT, materialize-on-write, eligibility) tests.

## v0.3.21 - 2026-06-24

Storage durability hardening, segment-default persistence, dependency modernization, and PostgreSQL admin/framework compatibility.

- **Atomic, durable persistence.** All on-disk writes (catalog, `.rseg` table data, column-snapshot segments, and metadata JSON) now go through a temp-file → `fsync` → atomic-rename → parent-dir-`fsync` path. Previously files were overwritten in place with no `fsync`, so a crash mid-write could corrupt the whole file. Stale `*.skein-tmp.*` files left by a crash are swept on open (symlink- and depth-safe).
- **Row-level redo WAL with crash recovery.** Every committed `insert`/`update`/`delete` appends the affected rows' final images to a global WAL (`data/wal-000001.log`) and `fsync`s before the snapshot write; committed records replay idempotently on open to recover any mutation lost between the WAL commit and the snapshot write, then the WAL is truncated. Primary-key-changing updates log an old-key tombstone so replay leaves no phantom row. See `docs/ON_DISK_FORMAT.md` §11.11.
- **Segment is now the default row store.** `serve` / `Engine::open` default to segment-backed `.rseg` (was hybrid JSON+segment). Reads still fall back to JSON so existing databases load unchanged; `SKEINDB_STORAGE_MODE=hybrid` opts back in.
- **Corruption fail-safe.** A present-but-undecodable table file is detected as corrupt (distinct from missing): the table loads empty and is blocked from being persisted, so a corrupt file is never silently overwritten with an empty table.
- **PostgreSQL admin/framework compatibility.** Adds `pg_catalog` column-type mappings plus `pg_stat_user_tables` / `pg_stat_user_indexes` / `pg_locks` virtual-table handling so pgAdmin/DBeaver introspection and psycopg/SQLAlchemy reflection round-trip over the PG wire, covered by new `tests/compat/pg_admin_*.sql` and `pg_framework_*.sql` fixtures.
- **Dependency modernization.** Upgrades `axum` 0.7→0.8 (route syntax), the RustCrypto digest family (`sha1`/`sha2`/`md-5`) →0.11, `getrandom` →0.3, and `hkdf` →0.13, with the required call-site updates.
- **Engine module split.** Extracts the forensic audit-chain helpers (R06) and migration-intent inference (R17) out of the monolithic `engine.rs` into their own modules (~2,370 fewer lines in `engine.rs`); no behavior change.
- Groups coordinated RustCrypto and HTTP/web-stack dependency bumps in Dependabot and adds a router-construction regression test so `axum` path-syntax breakage is caught by CI before fragmented upgrade PRs land.
- Validated with clean `cargo fmt --all -- --check`, clean `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and a green `cargo test --workspace` suite (including the new durable-write, WAL crash-recovery, corruption-guard, and PG admin/framework round-trip tests).

## v0.3.20 - 2026-06-11

- Adds **opt-in TLS** on the MySQL and PostgreSQL wire listeners via `serve --tls-cert <pem> --tls-key <pem>`: the PostgreSQL listener answers `SSLRequest` with `S` and completes a rustls handshake before startup, and the MySQL listener advertises `CLIENT_SSL` and upgrades after the short SSLRequest packet. Without a configured certificate both listeners stay plaintext (PG replies `N`; MySQL omits `CLIENT_SSL` and rejects an SSL request). The wire handlers now operate over a `MaybeTlsStream` so the full protocol surface works identically over plaintext and TLS.
- Adds a **real-driver smoke matrix** (`tests/smoke/`) that boots `skeindb serve` and round-trips DDL/DML/SELECT through `psql`, psycopg3, node-postgres, the `mysql` CLI, mysql2, and PyMySQL, gated as a strict `driver-smoke` CI job. It validates PostgreSQL SCRAM-SHA-256 and MySQL caching_sha2_password fast-auth end-to-end.
- Fixes a PostgreSQL extended-protocol bug the smoke matrix surfaced: `Execute` emitted a duplicate `RowDescription` after a portal `Describe`, which strict drivers such as psycopg3 reject. `Execute` now suppresses the redundant `RowDescription` (matching PostgreSQL), and portal `Describe` reflects the `Bind` result-format codes.
- Expands the PostgreSQL wire-compatibility corpus (`tests/compat/pg_corpus.sql`) from 14 to 124 statements covering `::`/`CAST` casts, dollar-quoted literals, `ARRAY[…]`, JSON/JSONB operators, column-side regex, `FETCH FIRST`, aggregates, `GROUP BY`/`HAVING`, joins, subqueries/CTEs/`UNION`, scalar functions, and DML with `RETURNING` and `ON CONFLICT … DO UPDATE`.
- Adds PostgreSQL extended-protocol binary `Bind` parameter decoding for the common scalar OID baseline and advertises MySQL `caching_sha2_password` with a `mysql_native_password` fallback.
- Validated with clean `cargo fmt --all -- --check`, clean `cargo clippy --all-targets --all-features -- -D warnings`, and a green `cargo test -p skeindb` suite including new PG/MySQL TLS round-trip integration tests.

## v0.3.19 - 2026-06-01

- Ships a stabilization and operator-readiness release on top of v0.3.18, keeping compatibility claims conservative: MySQL remains corpus-backed but not complete, PostgreSQL remains a partial PG v3 baseline, and R18/R19 remain prototype implemented.
- Completes PostgreSQL binary `COPY FROM STDIN` support for the current live PG path and fixes a panic regression so malformed binary COPY input returns a PostgreSQL error instead of terminating the connection worker.
- Adds encryption re-sealing coverage with batch `EncryptedValueStore::reencrypt_values` and the `settings.encryption.reencrypt_table` RPC for rewriting encrypted table cells under the active key after rotation.
- Adds CDC external file-sink draining, size-tiered compaction telemetry, alert escalation automation, and a reproducible transport benchmark report generator for operator workflows.
- Adds stable-Rust wire-protocol fuzz coverage to CI and introduces Python, Node, and shell SkeinQL SDK examples with focused tests.
- Extends generated Wasm filter/project plans to signed `i64` columns, extracts SkeinAdmin catalog data into a module, and corrects `system.capabilities` / docs to the current 147 advertised methods.
- Validated during release prep with clean `cargo fmt --all -- --check`, clean `cargo clippy --workspace --all-targets --all-features -- -D warnings`, green `cargo test --workspace --all-features`, green Node/Python SDK example tests, green benchmark-report generator tests, and the 18-test SkeinAdmin Playwright live-UI suite.

## v0.3.18 - 2026-05-28

- Adds encrypted-at-rest cell payloads for table rows, introducing on-disk format v4 with transparent encrypt-on-write / decrypt-on-read of cell values while keeping older v1-v3 segments readable.
- Expands PostgreSQL `COPY` parity: PostgreSQL-style `WITH (TEXT|CSV|BINARY)` aliases and legacy bare `WITH TEXT|CSV|BINARY` forms, text/csv `NULL '...'`, CSV `HEADER MATCH` on `COPY ... FROM STDIN`, and single-byte `QUOTE` and `ESCAPE` on supported CSV forms.
- Expands virtual `pg_catalog` coverage with `pg_am`, `pg_description`, and related probes for broader tooling introspection.
- Fixes a `COPY FROM STDIN` regression where text-format ingestion panicked the connection worker (clients saw "unexpected end of file"); text and CSV `COPY FROM STDIN` round-trips are restored.
- Validates the release with a green `cargo test --workspace`, clean `cargo clippy --workspace --all-targets`, and the 15-test SkeinAdmin Playwright live-UI suite. Roadmap stays **140 done / 0 open** and research **109 done / 0 open** (R18/R19 prototype caveats intact).

## v0.3.17 - 2026-05-13

- Improves SQL compatibility catalogs with MySQL `information_schema.check_constraints`, `information_schema.parameters`, and `information_schema.tablespaces` probes.
- Expands PostgreSQL virtual catalogs with `pg_catalog.pg_authid`, `pg_group`, `pg_indexes`, `pg_matviews`, `pg_sequences`, `pg_stats`, and `pg_stat_database`, including RowDescription overrides for the new OID/bool/int/float catalog columns.
- Updates compatibility docs, website copy, in-app help notes, generated docs site, and package metadata for the v0.3.17 catalog-polish release. Research backlog counts stay **71 done / 38 open**.

## v0.3.16 - 2026-05-11

- Improves SQL adoption catalogs with MySQL `information_schema.plugins`, `information_schema.events`, `information_schema.partitions`, and `information_schema.referential_constraints`, plus PostgreSQL `pg_catalog.pg_roles`, `pg_catalog.pg_user`, and `pg_catalog.pg_tablespace`.
- Removes a stale unreachable `information_schema.views` stub left behind after the v0.3.15 live view catalog implementation.
- Closes R09/T290-T293 and T295 with existing QUIC framing, server, prepared-query, zero-RTT write-rejection, and rebind/multi-stream test evidence; T294 remains open for comparative p99 benchmarking.
- Updates the research backlog and True Status Matrix to **71 done / 38 open**, refreshes compatibility docs and generated docs site, and fixes duplicate favicon tags on the R19 research page.

## v0.3.15 - 2026-05-11

- Closes R08/T280-T287 with persisted `view.create`, column-granular dependency metadata, restricted filter/project/group-by incremental maintenance, auto full-refresh fallback, a deterministic correctness oracle, and benchmark-style `view.evaluate` reports.
- Adds `view.evaluate` to SkeinQL types, JSON-RPC dispatch, read-only capability handling, SkeinAdmin Views controls, RPC templates, and `system.capabilities` for 133 advertised methods.
- Improves view compatibility metadata with MySQL `information_schema.views`, `information_schema.tables` VIEW rows, and PostgreSQL `pg_catalog.pg_views`, backed by focused SQL catalog tests.
- Updates the research backlog and True Status Matrix to **66 done / 43 open** while keeping R18/R19 prototype caveats intact.
- Refreshes README, SkeinQL/API/Incremental Views/SkeinAdmin/compat docs, generated docs site, website status copy, and release metadata for v0.3.15.

## v0.3.14 - 2026-05-11

- Closes R07/T270-T276 with write-write/dependency/constraint conflict hooks, values-only Wasm merge policy execution, deterministic cancellation coverage, `merge.evaluate` workload reports, offline queue docs, and SkeinAdmin Merge & CRDT wiring.
- Adds `merge.evaluate` to SkeinQL types, JSON-RPC dispatch, read-only capability handling, and `system.capabilities` for 132 advertised methods.
- Fixes SkeinAdmin Merge payloads for apply/register/simulate/Wasm register/drop and adds expected ETag, min-causality, current-row, evaluation-case, and Wasm limit controls.
- Improves compatibility probes with MySQL `information_schema.table_privileges` and PostgreSQL `pg_catalog.pg_tables`, backed by focused SQL catalog tests.
- Updates the research backlog and True Status Matrix to **58 done / 51 open** while keeping R18/R19 prototype caveats intact.
- Refreshes README, SkeinQL/API/Wasm/Merge/SkeinAdmin/compat docs, generated docs site, website status copy, and release metadata for v0.3.14.

## v0.3.13 - 2026-05-11

- Closes R06/T260-T266 with the SkeinForensic JSON filter grammar, chain-consistent index summaries, boundary hashes, checkpoint anchor metadata, Merkle roots, per-record inclusion proofs, and `skein.forensic.bundle.v1` report exports.
- Fixes SkeinAdmin's Forensics panel so Proof Verify first queries records and then calls `forensic.verify` with the correct `{records,start_hash}` payload; query/export now share DB/table/op/id/filter/bundle controls.
- Adds a simulated incident-timeline harness covering non-contiguous filtered forensic results and proof-bundle export strategy.
- Improves MySQL/ORM compatibility by expanding `information_schema.columns` with `COLUMN_TYPE`, length/precision/scale, charset/collation, privileges, comments, generated expression, and `EXTRA` metadata.
- Updates the research backlog and True Status Matrix to **51 done / 58 open** while keeping R18/R19 prototype caveats intact.
- Refreshes README, SkeinQL/API/Audit WAL/SkeinAdmin/docs-site/website status copy and release metadata for v0.3.13.

## v0.3.12 - 2026-05-11

- Closes R05/T250-T256 with a documented oblivious-execution threat model, per-table policy schema, padded scan/dummy ValueStore lookup execution, explain output, and deterministic leakage/overhead evaluation reports.
- Adds `oblivious.evaluate`, typed SkeinQL result structs, JSON-RPC dispatch, capability advertising, and focused engine/RPC/integration coverage for padded-vs-unpadded trace metrics.
- Fixes SkeinAdmin's R05 Privacy card to use the runtime policy shape and nested table payloads, adds trace-row controls, and exposes the leakage evaluator from the UI.
- Updates the research backlog and True Status Matrix to **44 done / 65 open** while keeping R18/R19 prototype caveats intact.
- Refreshes README, SkeinQL/API/SkeinAdmin/oblivious docs, the R05 research page, generated docs site, website status copy, and release metadata for v0.3.12.

## v0.3.11 - 2026-05-09

- Closes R04/T240-T245 with test-backed DP aggregate hardening: COUNT/SUM/AVG `dp.aggregate` payloads, bounded sensitivity metadata, per-principal persisted budgets, seeded Laplace/Gaussian mechanisms, privacy-aware `privacy_etag` validators, and persisted budget-consumption audit events.
- Adds a focused runtime regression that verifies DP aggregate sensitivities, privacy ETags, budget persistence, audit persistence, and deterministic Gaussian behavior.
- Updates the research backlog and True Status Matrix to **37 done / 72 open** while keeping R18/R19 prototype caveats intact.
- Refreshes README, SkeinQL/API/SkeinAdmin docs, the R04 research page, generated docs site, website status copy, and release metadata for v0.3.11.

## v0.3.10 - 2026-05-09

- Adds `dp.evaluate`, a deterministic differential-privacy evaluation harness that reports exact baselines, accuracy-vs-epsilon error metrics, noisy-query latency, and overhead-vs-exact timings for seeded DP aggregate trials.
- Wires `dp.evaluate` through `system.capabilities`, JSON-RPC dispatch, RPC templates, and SkeinAdmin's Privacy panel with epsilon-grid, trials, seed, mechanism, and bounds controls.
- Fixes SkeinAdmin's existing DP aggregate/budget/audit actions to send the typed `aggregates`, `principal`, and budget/audit parameter shapes expected by the runtime.
- Closes research backlog T246 and updates status counts to **31 done / 78 open** while keeping R18/R19 prototype caveats intact.
- Updates API/SkeinQL/SkeinAdmin docs, website method counts, generated docs site, and release packaging metadata for v0.3.10.

## v0.3.9 - 2026-05-09

- Closes R18/T189 with a replay-regression CI comparison harness: `skeindb replay run --json --out <report.json>` emits machine-readable run evidence, and `skeindb replay compare --baseline <base.json> --candidate <head.json>` compares p95/p99/span/storage/cache-hot-table deltas against threshold flags.
- Adds focused CLI parsing and threshold regression tests for the replay comparison path.
- Keeps R18 honestly prototype-level because deterministic timing injection and cache/LSM reconstruction fidelity remain open under T188.
- Updates the research backlog and True Status Matrix to **30 done / 79 open**.
- Refreshes README, formula metadata, runtime baseline docs, docs site, and website status for v0.3.9.

## v0.3.8 - 2026-05-09

- Adds a comprehensive **Help & Docs** panel to SkeinAdmin: quick-start checklist, panel reference table with one-click jumps, R01-R20 research-track index with hardness pills and primary RPC methods, keyboard-shortcut and deep-link reference, glossary, and links to the canonical documentation site.
- Wires Help into the left nav, top tabs, the topbar `? Help` button, and a `?` keyboard shortcut.
- Adds live filter search across panel and research entries inside the Help Center.
- Locks the new Help panel surface with `skeinadmin_help_panel_exposes_comprehensive_documentation_center` so docs claims stay test-backed.
- Keeps the research backlog status honest: 29 done / 80 open, 18 hardened / 2 prototype tracks (R18 perf replay and R19 Wasm operators remain at prototype level).
- Updates README, SKEINADMIN.md, the public website, and the docs site to advertise v0.3.8 and the new Help Center.
- Refreshes the Homebrew formula to v0.3.8.

## v0.3.7 - 2026-05-09

- Promotes the post-R12/R17/R20 hardening line into a versioned release-prep state.
- Keeps the research backlog status honest: 29 done / 80 open, with R18 and R19 still prototype-level.
- Adds R19 Wasm plan artifact metadata, inspect RPC, host-backed edge package helper, and current SkeinAdmin Wasm controls.
- Adds R18 performance-annotated replay bundles with storage/cache/timing metadata and replay variance reports.
- Adds replay bundle primary-key redaction for `maintenance.replay.export`, SkeinAdmin replay export, and `skeindb replay export`.
- Updates public website, docs, and status copy to the current 18 hardened / 2 prototype research-track state.
- Adds review-driven regression coverage for NL approval-token mismatch behavior and migration-intent combined/false-positive cases.
- Carries the Wasmtime 43.0.2 security update and R17 migration report exporter from the prior batch.
