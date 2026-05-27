# Self-tuning Index Advisor (Telemetry-driven)

Status: Partial implementation
Last updated: 2026-05-24

Current runtime baseline:
- Query fingerprint telemetry, candidate generation, and Level 0 scoring are implemented in the engine.
- `advisor.index_synthesize`, `advisor.apply_index`, `advisor.dismiss`, `advisor.retire_unused`, `advisor.evaluate`, and `advisor.history` are live.
- `advisor.index_synthesize` suggestions now include optional dependency-capture metadata: predicate columns, equality/range columns, range shape, order-by/group-by/join-key columns, and projected covering columns.
- Candidate key generation is dependency-driven: equality columns come first, then join keys, one range column, group-by columns, and order-by columns; projected non-key columns become covering `include` columns.
- Suggestions also include an optional cost estimate with read benefit, write overhead, compaction overhead, write pressure, key/include width, and net score.
- Candidate synthesis suppresses exact duplicates, primary-key prefixes, prefixes already covered by existing MySQL-compatible indexes, and suggestion IDs that were previously applied or dismissed.
- `advisor.retire_unused` can dry-run or retire advisor-built secondary indexes when the latest dependency signal is absent or older than a caller-provided idle threshold; recent dependency signals block retirement.
- `advisor.evaluate` replays phased dependency samples against a scratch advisor state and reports top-suggestion convergence after workload shifts without mutating live advisor telemetry.
- For benchmarkable equality, join-key filters, multi-range filters, narrow order-by, grouped phases including mixed range/order/group layouts, and non-grouped same-leading range+order, `advisor.evaluate` also returns measured before/after latency stats and row-scan comparisons by comparing live full scans against a hypothetical advisor-built secondary index.
- `advisor.apply_index` now queues background in-memory secondary-index builds, returns `queued` or `exists`, and `advisor.history` records lifecycle state (`queued`, `building`, `completed`, `failed`, `cancelled`) with progress percentages plus optional result/rollback metadata.
- SkeinAdmin has a working Index Advisor page that renders ranked suggestions, action history, and an observed-before / expected-after scan report for each suggestion.
- Non-grouped range+order layouts without a same-leading key still rely on observed-before / expected-after scan reports; measured latency now covers benchmarkable equality, join-key filters, multi-range filters, narrow order-by, grouped phases including mixed range/order/group layouts, and non-grouped same-leading range+order.

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

If many queries filter by equality on columns (a,b), join on j, range-scan r, and then order by c:
Suggest INDEX(a,b,j,r,c).

Heuristic:
- For each frequent query pattern, create a candidate key list:
  - all equality columns (sorted by selectivity if stats known)
  - then join-key columns not already present
  - then one range column (at most one)
  - then GROUP BY columns
  - then ORDER BY columns (if compatible)

### 2.2 Covering indexes

If queries repeatedly select the same small set of columns:
- Suggest including them as covering `include` columns when they are not already part of the key.
- The current in-memory advisor API carries include columns through `advisor.index_synthesize` and `advisor.apply_index` so operators can review and apply the same candidate shape.

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

Current prototype cost output is heuristic and transparent: `read_benefit` starts from observed rows scanned, while `write_overhead` is derived from table write pressure and candidate width, and `compaction_overhead` is derived from observed scan volume and candidate width. `score` equals `cost.net_score`.

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
6) Optional retirement scans can dry-run first, then drop only advisor-built indexes whose latest dependency signal is stale or absent

Note:
- SkeinAdmin's per-suggestion "before/after" report remains workload-derived and expected-after; `advisor.evaluate` adds measured latency benchmarks only for benchmarkable equality, join-key filters, multi-range filters, narrow order-by, grouped phases including mixed range/order/group layouts, and non-grouped same-leading range+order.
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
  Result suggestions include the index key, include columns, scoring telemetry, and the dependency evidence that produced the candidate:
  ```json
  {
    "id": "idxs_5a2c...",
    "columns": ["city", "created_at"],
    "include": ["name"],
    "score": 128,
    "count": 4,
    "rows_scanned": 128,
    "cost": {
      "read_benefit": 128.4,
      "write_overhead": 0.05,
      "compaction_overhead": 0.10,
      "net_score": 128.25,
      "write_pressure": 1,
      "key_columns": 2,
      "include_columns": 1
    },
    "dependency": {
      "predicate_columns": ["city"],
      "equality_columns": ["city"],
      "range_columns": [],
      "range_shape": "none",
      "order_by_columns": ["created_at"],
      "group_by_columns": [],
      "join_key_columns": [],
      "projection_columns": ["name"]
    }
  }
  ```

- `advisor.apply_index` (queues an in-memory secondary-index build in the prototype)
  - indexes rebuild lazily on first use after table changes
- `advisor.dismiss` (suppresses the suggestion and drops any advisor-built index)
- `advisor.retire_unused` (dry-runs or retires stale advisor-built indexes)
- `advisor.evaluate` (replays phased workload samples and reports top-suggestion convergence)
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
- `advisor.retire_unused`
- `advisor.history`

`advisor.retire_unused` example:

```json
{
  "table": {"db":"mydb","table":"users"},
  "max_idle_ms": 86400000,
  "dry_run": true,
  "limit": 20,
  "note": "daily advisor retirement review"
}
```

When `dry_run` is false, eligible retirements are written to `advisor.history` with action `retire`. Safety rules:
- only the latest active `advisor.apply_index` action for a suggestion is considered
- a newer `dismiss` or `retire` action removes it from retirement candidates
- indexes with dependency signals newer than `max_idle_ms` return `reason: "recently_used"` and are not dropped
- indexes with no dependency signal or stale dependency signal return `reason: "unused"` when retired

`advisor.evaluate` example:

```json
{
  "table": {"db":"mydb","table":"users"},
  "min_queries": 1,
  "min_rows": 1,
  "phases": [
    {
      "label": "city_lookup",
      "samples": [
        {
          "equality_columns": ["city"],
          "rows_scanned": 400,
          "repeats": 3
        }
      ]
    },
    {
      "label": "email_lookup",
      "samples": [
        {
          "equality_columns": ["email"],
          "rows_scanned": 500,
          "repeats": 3
        }
      ]
    }
  ]
}
```

Result summary:

```json
{
  "format": "skein.advisor.evaluate.v1",
  "phase_count": 2,
  "total_observations": 6,
  "initial_top": {"columns": ["city"]},
  "final_top": {"columns": ["email"]},
  "phases": [
    {
      "label": "city_lookup",
      "observations": 3,
      "top_after": {"columns": ["city"]},
      "latency_benchmark": {
        "benchmarkable_samples": 1,
        "benchmark_runs": 8,
        "observed_rows_scanned": 400,
        "before_rows_scanned": 1024,
        "after_rows_scanned": 32,
        "before": {
          "min_ns": 18200,
          "p50_ns": 19600,
          "p95_ns": 22100,
          "p99_ns": 22900,
          "max_ns": 22900,
          "mean_ns": 19875.0
        },
        "after": {
          "min_ns": 2900,
          "p50_ns": 3200,
          "p95_ns": 4100,
          "p99_ns": 4300,
          "max_ns": 4300,
          "mean_ns": 3362.5
        },
        "speedup": 5.91
      },
      "top_changes": 1,
      "distinct_top_suggestions": 1
    },
    {
      "label": "email_lookup",
      "observations": 3,
      "top_before": {"columns": ["city"]},
      "top_after": {"columns": ["email"]},
      "final_top_stable_after_observation": 3,
      "top_changes": 1,
      "distinct_top_suggestions": 2
    }
  ]
}
```

Each sample can supply `equality_columns`, `range_columns`, `order_by_columns`, `group_by_columns`, `join_key_columns`, `projection_columns`, `rows_scanned`, and `repeats`. The harness validates every referenced column against the live schema, requires at least one dependency column per sample, and rejects zero `rows_scanned` or zero `repeats`. `latency_benchmark` is optional and currently appears only for benchmarkable equality, join-key filters, multi-range filters, narrow order-by, grouped phases including mixed range/order/group layouts, and non-grouped same-leading range+order whose dependency columns cover the winning suggestion key; non-grouped range+order layouts without a same-leading key are still excluded.

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
- [x] IA05: Safe advisor-built index retirement with dry-run and stale-dependency checks
- [x] IA06: Measured before/after reporting for benchmarkable advisor recommendations (equality + single-range + narrow order-by + narrow group-by today)

---

## Research extension: Automatic index synthesis from dependency analysis

The baseline index advisor uses query fingerprints and rule-based heuristics.
The research agenda proposes a stronger signal: **runtime dependency tracking** (which key ranges, columns, and ordering requirements were actually used).
See: `docs/research_agenda/R16_automatic-index-synthesis-from-dependency-analysis.md`.

Adaptation sketch:
- Dependency recording now surfaces predicate columns, equality/range columns, range shapes, and sort/group/join/projection requirements on each synthesized suggestion.
- Candidate indexes, including composite keys and covering include columns, are generated from aggregated dependencies.
- Cost scoring now subtracts explicit write and compaction overhead estimates from observed read benefit.
- `advisor.retire_unused` retires indexes that no longer have a fresh dependency signal, with dry-run and history safety checks.
