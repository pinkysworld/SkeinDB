# Performance Improvements (Beyond Baseline)

Status: Draft v0.1
Last updated: 2026-01-17

This document proposes additional performance features for SkeinDB that complement
Cell-Interned MVCC, Delta values, ETag caching, Wasm UDFs, and Column Snapshots.

The goal is to add at least one **novel, measurable** performance improvement that is also
worth describing as a research contribution.

---

## 1) ValueID-first execution (VAX)

### 1.1 Core idea

SkeinDB already uses a content-addressed ValueStore where identical values share the same
ValueID. This makes ValueID behave like a **universal dictionary encoding**.

**ValueID-first execution** means:
- predicates (equality, IN-list) and grouping/join keys can operate on ValueIDs instead of raw bytes
- row materialization (turn ValueIDs into decoded values) is delayed until the end of the pipeline

This can reduce CPU cost significantly for string-heavy workloads because comparisons become:
- `memcmp(16)` on ValueIDs instead of `memcmp(n)` on long strings

It can also reduce memory bandwidth and improve cache locality.

### 1.2 Requirements

- Columns that are stored as ValueIDs must be marked as `interned` in schema metadata.
- The executor must be able to evaluate expressions on ValueIDs where semantics are preserved.

Safe operators on ValueIDs:
- `eq`, `ne`
- `in` (when list is pre-interned)
- hash keys for `GROUP BY` and hash join

Unsafe operators (still require decoded bytes):
- `like` (pattern match)
- collation-aware comparisons (unless you intern collation-normalized forms)

### 1.3 Planner rule

If a predicate references only interned columns and uses ValueID-safe operators:
- choose an execution path that keeps the column in ValueID form

If a query needs decoded values only for output columns:
- keep ValueIDs through filter/join/agg
- decode only the projected output columns

### 1.4 Measurable outcomes

Benchmarks should report:
- CPU time per query for string predicates
- memory bandwidth (optional)
- p95 latency improvements

### 1.5 Prototype status

Current prototype coverage:
- interned-column schema metadata persists in `data/schema_flags.json`
- `schema_set_column_interned(...)` toggles the flag and `describe_table(...)` reports it per column
- single-table scan paths precompile ValueID-safe predicates over interned columns and compare ValueIDs for `eq`, `ne`, and `in`
- single-table row and snapshot scan paths now materialize only query-referenced columns, and when the predicate stays on the ValueID lookup path they only build row context for projection and `ORDER BY` columns
- eligible single-table full scans now run through a 1024-row batch pipeline that transposes visible rows into a columnar buffer, filters them, and then projects result rows
- `skeindb-core::mvcc::VisibleVersionIndex` now caches validated `row_id + snapshot_epoch_bucket` lookups and reuses the resolved visible version when the `RowDir` head pointer still matches and the cached version remains visible for the exact snapshot timestamp
- unsupported operators or non-interned columns fall back to the normal expression evaluator

---

## 2) Vectorized execution batches

### 2.1 Core idea

Instead of processing rows one-by-one, the executor processes batches (e.g., 1024 rows) in a
columnar-in-memory format (vectors). This reduces per-row overhead and can enable SIMD.

This pairs well with ValueID-first execution:
- ValueIDs can be processed in tight loops

### 2.2 Minimal v1 implementation

- Scan operator outputs batches
- Filter operator evaluates batch predicates
- Project operator computes output batch

Vectorized joins/aggregates can be added later.

---

## 3) MVCC chain acceleration (Visible Version Index)

### 3.1 Problem

In MVCC, a row_id may have a long version chain.
Finding the visible version can degrade to following pointers repeatedly.

### 3.2 Proposal

Maintain a small per-row "visible hint" cache keyed by `(row_id, snapshot_epoch_bucket)`.

- When a snapshot reads a row, store the resolved version pointer in the cache.
- For future reads at similar snapshot epochs, jump directly to the likely visible version.

Current prototype coverage:

- bounded `VisibleVersionIndex` cache in `crates/skeindb-core/src/mvcc.rs`
- cache validation against the current `RowDir` head pointer and the cached version's exact `[begin_ts, end_ts)` visibility window
- automatic fallback to normal chain walking on head changes or same-bucket timestamp drift

This is safe if the cache is validated (begin/end ts check) before use.

### 3.3 Measurable outcomes

- reduced pointer chasing on hot rows
- improved latency for read-heavy workloads with frequent updates

---

## 4) Backlog

- PF01: Schema flag for interned columns (implemented via `schema_flags.json`)
- PF02: Executor support for ValueID-safe ops (implemented for `eq`, `ne`, and `in` on single-table scan paths)
- PF03: Late materialization (implemented for single-table row/snapshot scan contexts)
- PF04: Batch execution framework (implemented for eligible single-table full scans)
- PF05: Visible Version Index cache

## 4b) Write concurrency and the global engine lock

The engine is a single `Arc<RwLock<Engine>>`; every mutation acquires the write lock. Two `#[ignore]`d benchmarks measure the write path (macOS/APFS, run with `cargo test -p skeindb --bins <name> -- --ignored --nocapture`):

- **`bench_concurrent_write_throughput`** — N writers into *one* table.
  - `batch=1` (fsync every commit): ~150–170 inserts/sec regardless of writer count — writes are **fsync-bound** (each commit pays one `F_FULLFSYNC`), and adding writers does not help.
  - `batch=64` (`SKEINDB_WAL_SYNC_BATCH`, fsync amortized): single writer ~1700/sec; 8 writers ~750/sec — with the fsync amortized, concurrency now *hurts* because writers contend on the one global lock.
- **`bench_multidb_write_throughput`** — N writers, each into its *own* database (`batch=64`). This is the workload per-database lock-sharding would help. Throughput **falls** as databases increase: 1 db ~1560/sec, 4 dbs ~600/sec, 8 dbs ~460/sec. Writes to unrelated databases still serialize on the single global lock (8 independent databases are slower in aggregate than one).

**Finding.** For **single-table** write bursts the bottleneck is the WAL fsync, not the lock; the durability knobs (`SKEINDB_WAL_SYNC_BATCH`) address that and the append-only CDC/forensic logs removed the other per-mutation fsyncs (~7×, see the changelog). For **many-database concurrent** writes, the single global engine lock is the real ceiling — sharding it per database would let unrelated databases proceed in parallel.

**Why it is not yet done — and the plan for when it is.** Per-database lock-sharding is a large, dedicated engine rewrite, not an incremental change, and — crucially — it does **not** decompose into independently-mergeable slices: a half-partitioned engine is a broken engine, so it must land as one large, well-tested change (or a short-lived feature branch), not a sequence of small PRs to `main`. Essentially all engine state (`tables`, `catalog`, `ValueStore`, the WAL, the forensic chain, CDC) is global today, and Rust's `&mut self` write methods make concurrent per-database writes impossible without restructuring that state behind per-database interior mutability. The benchmarks above are the yardstick for the project when it is scheduled.

Two candidate architectures were evaluated:

1. **Interior per-database locks inside `Engine`.** Replace the global `tables: HashMap<TableKey, TableData>` (and the per-database slice of the catalog) with `databases: HashMap<String, Arc<RwLock<DatabaseState>>>`, so a write to `db1` locks only `db1`. *Cost:* every one of the hundreds of `self.tables` / `self.get_table` / `self.get_schema` access sites in the ~54k-line `engine.rs` must route through the per-database partition; cross-database reads (joins, `list_databases`) must acquire multiple partitions in a canonical order to avoid deadlock; and truly-global structures (`ValueStore` CAS dedup, CDC change-seq, the forensic hash-chain) must either stay shared (a residual serialization point) or be partitioned too.
2. **One `Engine` per database, behind the server.** Turn `AppState.engine: Arc<RwLock<Engine>>` into a registry `engines: HashMap<String, Arc<RwLock<Engine>>>`, each managing one database in its own sub-directory. *Cost:* every `state.engine.read()/write()` site in `handle_rpc` must resolve the target database first; cross-database queries need multi-engine coordination; the on-disk layout changes (so existing single-engine data needs a one-time migration); and CAS dedup no longer spans databases.

**Prerequisite either way: per-database WALs.** With a single shared WAL, the fsync/group-commit path is a *second* serialization point that would blunt most of the lock-sharding win — so the WAL (and the append-only CDC/forensic sidecars) must be partitioned per database first. That is itself a self-contained, independently-valuable slice (per-DB WAL directories + recovery), and is the recommended **first** step of the project: it de-risks the larger lock change and can ship on its own.

**Step (i) is done (v0.3.38).** The row-redo WAL is now partitioned per database at `<data_dir>/wal/<db>/wal-000001.log`, each with its own writer, transaction stream, and group-commit fsync counter, opened lazily on that database's first mutation (`engine/wal.rs`). Recovery replays every database's WAL independently and drains + removes any legacy single global WAL on the first open after upgrade (a one-time migration). Because a single DML statement only ever touches one database, a WAL transaction never spans databases, so the partition is a pure refactor of *where* redo lands — no cross-database atomicity change. The full WAL crash-recovery harness (torn-tail-at-every-offset, group-commit, lost-snapshot, PK-update phantom-row) still passes, plus new multi-database and legacy-migration recovery tests. The append-only CDC/forensic sidecars remain shared for now — partitioning them is folded into step (ii), since they only become a serialization point once the write lock itself is sharded.

**Recommended sequencing when scheduled:** (i) ~~per-database WAL + sidecar directories with per-DB recovery (independently shippable)~~ — **done, v0.3.38**; (ii) the lock restructure via approach 1 on a feature branch, gated behind a config flag and validated against `bench_multidb_write_throughput` (the pass criterion is that N-database throughput scales up, not down, with N), partitioning the CDC/forensic sidecars alongside it; (iii) cross-database transaction/query coordination and multi-lock ordering; (iv) flip the default once the benchmark and the full suite are green. Step (ii) onward is a focused multi-week effort best started at the top of a working block, not appended to unrelated work.

### Snapshot streaming for very large tables (done, v0.3.39)

`cluster.replication.snapshot` (used for automated re-sync, §Clustering 2.5) previously gathered the whole logical database into memory as one op array before returning it in a single response — a memory spike on both the primary and the re-syncing replica proportional to the database size. It is now **memory-bounded on both ends** via a streamed, spill-based protocol:

- `Engine::snapshot_export_stream` emits the snapshot's ops one at a time (rows in bounded `data.insert` batches, and a `Streaming` table's rows are read one at a time off its segment), so the primary never holds the whole database in memory.
- `cluster.replication.snapshot` with `mode:"begin"` captures the log position under the consistency barrier + engine read lock and **spills** the streamed export to a temp file (`<data_dir>/tmp/resync-snapshot-<term>-<seq>.ndjson`), returning only the position + op/byte counts. Spilling under the lock (bounded RAM, primary-paced) — rather than streaming to the client under the lock — means a slow or stuck replica cannot stall the primary's writes mid-transfer.
- `mode:"chunk"` reads bounded pages of ops from that immutable file **lock-free** (byte-offset cursor). The replica applies each page and drops it before fetching the next, so peak memory is one chunk.
- Correctness is identical to the old single-shot snapshot — the spilled file is one consistent capture at `snapshot_seq`, just read in pages — so post-resync catch-up from `snapshot_seq` still neither skips nor duplicate-applies. Abandoned temp files (a replica that died mid-resync) are swept by mtime on the next `begin`. The method is unchanged for older nodes: a primary that predates streaming ignores `mode` and returns the ops inline, and the replica falls back to applying them (rolling-upgrade safe).

The remaining follow-on is *lock-hold* (not memory): the spill still holds the engine read lock (blocking writes) for its duration. Bounding that too would need MVCC snapshot isolation so pages could be read lock-free from a point-in-time view — a larger engine change, and a rare-re-sync availability nicety rather than a correctness or memory gap.

## 5) Autoparameterization + plan reuse (SQL clients)

Many MySQL-compatible clients send repetitive ad-hoc SQL differing only by literals.
If SkeinDB normalizes queries into a stable fingerprint and extracts parameters (docs/AUTOPARAMETERIZATION.md),
the engine can reuse parse/plan work and improve throughput at high QPS.

Measurable outcomes:
- reduced CPU spent in parsing and translation
- higher plan-cache hit rate
- lower tail latency under bursty workloads

## 6) Stall-aware compaction scheduling

Compaction backlogs can induce write stalls and p99 spikes.
The workload-guided scheduler (docs/COMPACTION_SCHEDULER.md) treats compaction as a budgeted background workload
and prioritizes work to avoid stalls.

Measurable outcomes:
- fewer write stalls
- lower p99 write latency
- smoother throughput over time

## Reproducing the transport benchmark

SkeinDB ships a built-in `transport-bench` subcommand that measures SkeinQL
round-trip latency (p50/p95/p99) across the HTTP/2, QUIC, and MySQL transports
under concurrent load, and emits a JSON report. `eval/benchmark_report.py`
renders that JSON into a deterministic Markdown report annotated with the
environment (git commit, rustc version, OS, CPU count) so runs are reproducible
and comparable across commits.

```bash
# 1. Build and start a server.
cargo build -p skeindb --release
target/release/skeindb serve --data ./data &

# 2. Capture a JSON benchmark report.
target/release/skeindb transport-bench --json > report.json

# 3. Render a reproducible Markdown report.
python eval/benchmark_report.py --from-json report.json -o BENCHMARK_REPORT.md
```

The report-formatting and summary logic is pure and unit-tested
(`eval/test_benchmark_report.py`, run(`eval/test_benchmark_report.py`, run(`eval/test_benchmark_report.py`, run(`eval/test_benchmark_report.py`, run(`evalrver.(`eval/test_benchmark_report.py`, ruepro(`eval/test_benchmark_report.py`, run(`eval/test_benchmark_report.py`, ruful deltas.
