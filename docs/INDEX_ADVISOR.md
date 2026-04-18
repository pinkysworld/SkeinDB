# Self-tuning Index Advisor (Telemetry-driven)

Status: Partial implementation
Last updated: 2026-04-16

Current runtime baseline:
- Query fingerprint telemetry, candidate generation, and Level 0 scoring are implemented in the engine.
- `advisor.index_synthesize`, `advisor.apply_index`, `advisor.dismiss`, and `advisor.history` are live.
- Candidate synthesis suppresses exact duplicates, primary-key prefixes, prefixes already covered by existing MySQL-compatible indexes, and suggestion IDs that were previously applied or dismissed.
- `advisor.apply_index` now queues background in-memory secondary-index builds, returns `queued` or `exists`, and `advisor.history` records lifecycle state (`queued`, `building`, `completed`, `failed`, `cancelled`) with progress percentages plus optional result/rollback metadata.
- SkeinAdmin has a working Index Advisor page that renders ranked suggestions, action history, and an observed-before / expected-after scan report for each suggestion.
- Measured before/after latency deltas remain open work.

Goal:
Automatically suggest indexes that improve real workloads while preserving SkeinDB's drop-in MySQL compatibility.

Key idea:
- The MySQL translator and SkeinQL engine already see every query.
- We can record lightweight telemetry (fingerprints, predicate columns, sort columns).
- From that telemetry, we can generate candidate index suggestions, estimate benefit, and present them in SkeinAdmin.

This feature is designed to be research-friendly:
- it is measurable (latency/CPU before and after)
- it can be evaluated on real trace-like corpora
- it supports safe deployment (suggestions are human-approved by default)

---

## 1) Inputs (collected telemetry)

The telemetry layer should record the following per query fingerprint:
- count
- total_time_ms, p95_time_ms (approximate)
- logical reads (optional)
- table(s) referenced
- for each table:
  - equality predicates: columns in (col = lit/param)
  - range predicates: columns in (col <, <=, >, >=, BETWEEN)
  - join keys: columns used in equi-joins
  - order_by keys
  - group_by keys

Important:
- Do not store literal values unless explicitly enabled (privacy).
- Store only column IDs or normalized names.

---

## 2) Candidate generation heuristics (v1)

The advisor produces candidate indexes per table.

### 2.1 Equality-first composite indexes

If many queries filter by equality on columns (a,b) and then order by c:
Suggest INDEX(a,b,c).

Heuristic:
- For each frequent query pattern, create a candidate key list:
  - all equality columns (sorted by selectivity if stats known)
  - then one range column (at most one)
  - then ORDER BY columns (if compatible)

### 2.2 Covering indexes

If queries repeatedly select the same small set of columns:
- Suggest including them (covering) if the engine supports INCLUDE columns.
- If not, keep as a normal index and rely on row lookup.

### 2.3 Avoiding pathological suggestions

Reject candidates that:
- exceed max columns (e.g., > 4)
- start with low-cardinality columns unless they are always paired with selective columns
- duplicate an existing index prefix

---

## 3) Benefit estimation

SkeinDB can estimate benefit using increasing levels of sophistication.

### 3.1 Level 0 (no stats)
- Use rule-based benefit categories:
  - "HIGH" if it changes SEQ_SCAN to INDEX_RANGE on a frequent query
  - "MED" if it improves sorting elimination
  - "LOW" otherwise

### 3.2 Level 1 (basic stats)
Maintain per-column:
- approximate distinct count
- null fraction
- min/max (for numeric)

Then estimate selectivity of equality predicates.

### 3.3 Level 2 (histograms)
Build simple equi-depth histograms for hot columns.

---

## 4) Safety and workflow

Default posture:
- Advisor only suggests; it does not auto-apply.

Workflow:
1) SkeinAdmin shows suggestions with:
   - candidate key columns (+ INCLUDE columns when available)
   - observed scan pressure from recent workload telemetry
   - an expected-after access-path summary
2) Admin clicks "Apply"
3) Engine records a queued advisor action and completes the build in the background
4) History records queued/building/completed-or-failed state for later review
5) Failed builds record rollback state before the suggestion can surface again

Note:
- The current "before/after" report is workload-derived and expected-after, not a measured latency benchmark yet.
- Progress reporting is lifecycle-level (`queued` -> `building` -> terminal state), not per-row physical build accounting.

An optional "auto-apply" mode can exist for development environments.

---

## 5) SkeinQL API

Recommended methods:

- `advisor.index_synthesize` (experimental, R16)
  Params:
  ```json
  { "table": {"db":"mydb","table":"users"}, "limit": 20, "min_queries": 3, "min_rows": 32 }
  ```

- `advisor.apply_index` (queues an in-memory secondary-index build in the prototype)
  - indexes rebuild lazily on first use after table changes
- `advisor.dismiss` (suppresses the suggestion and drops any advisor-built index)
- `advisor.history`

- `advisor.index_suggestions`
  Params:
  ```json
  { "db": "mydb", "table": "users", "limit": 20 }
  ```

- `advisor.apply_index`
  Params:
  ```json
  { "table": {"db": "mydb", "table": "users"}, "columns": ["city","created_at"], "include": ["name"] }
  ```

- `advisor.dismiss`
- `advisor.history`

---

Telemetry persistence (prototype):
- Set `SKEINDB_ADVISOR_PERSIST=1` to persist advisor patterns/history on disk.
- Files: `advisor_patterns.json` + `advisor_history.json`.
- Applied advisor indexes are restored from the history log on startup when their latest apply action reached a terminal success state.

## 6) Metrics

Expose:
- advisor_suggestions_total
- advisor_applied_total
- advisor_rejected_total
- advisor_estimated_saved_ms_total

Note: `advisor_estimated_saved_ms_total` is a placeholder in the prototype.

---

## 7) Backlog

- [x] IA01: Telemetry query fingerprint + column feature extraction
- [x] IA02: Candidate generation + duplication checks
- [x] IA03: Benefit estimation level 0
- [x] IA04: SkeinQL endpoints + SkeinAdmin UI page
- [ ] IA05: Measured before/after reporting for advisor-applied indexes

---

## Research extension: Automatic index synthesis from dependency analysis

The baseline index advisor uses query fingerprints and rule-based heuristics.
The research agenda proposes a stronger signal: **runtime dependency tracking** (which key ranges, columns, and ordering requirements were actually used).
See: `docs/research_agenda/R16_automatic-index-synthesis-from-dependency-analysis.md`.

Adaptation sketch:
- Extend dependency recording to include predicate columns, range shapes, and sort/group requirements.
- Generate candidate indexes (including covering indexes) from aggregated dependencies.
- Retire indexes that no longer deliver measurable benefit, with safety checks.
