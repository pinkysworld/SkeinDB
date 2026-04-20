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
- unsupported operators or non-interned columns fall back to the normal expression evaluator

Still open:
- PF03 late materialization
- PF04 vectorized batches
- PF05 MVCC visible-version cache

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

This is safe if the cache is validated (begin/end ts check) before use.

### 3.3 Measurable outcomes

- reduced pointer chasing on hot rows
- improved latency for read-heavy workloads with frequent updates

---

## 4) Backlog

- PF01: Schema flag for interned columns (implemented via `schema_flags.json`)
- PF02: Executor support for ValueID-safe ops (implemented for `eq`, `ne`, and `in` on single-table scan paths)
- PF03: Late materialization (decode only projected columns)
- PF04: Batch execution framework (scan->filter->project)
- PF05: Visible Version Index cache

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
