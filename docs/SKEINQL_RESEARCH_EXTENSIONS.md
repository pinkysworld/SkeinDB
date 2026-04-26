# SkeinQL Research Extensions (Experimental)

This document lists **experimental** SkeinQL method families that support the **20-proposal research agenda**.

Principles:
- These methods are **not required** for MySQL compatibility.
- They should be implemented behind feature flags and exposed only when enabled.
- For each family, the canonical research description is in `docs/research_agenda/`.

## 1. dp.* — Differential privacy
Related: R04

- `dp.aggregate` — aggregates with DP parameters (epsilon, delta, mechanism)
- `dp.budget.get` / `dp.budget.set` — budget tracking and policies
- `dp.audit.log` — budget consumption events

## 2. oblivious.* — Oblivious execution policies
Related: R05

- `oblivious.policy.get` / `oblivious.policy.set`
- `oblivious.explain` — show which operators will be padded/shuffled

## 3. forensic.* — Forensic WAL queries with proofs
Related: R06

- `forensic.query` — run a forensic query over the hash-chained WAL
- `forensic.verify` — verify returned proofs
- `forensic.export` — export a signed/anchored report bundle

## 4. merge.* — Optimistic writes with merge semantics
Related: R07

- `merge.register` — register a merge function (Wasm module reference)
- `merge.apply` — apply merge to conflicting versions (server-driven)
- `merge.simulate` — test merges without committing
- `merge.wasm.register` / `merge.wasm.list` / `merge.wasm.drop` — manage Wasm merge modules

## 5. view.* — Materialized views + incremental maintenance
Related: R08

- `view.create` / `view.drop`
- `view.refresh` — incremental, full, or auto refresh for restricted single-table views (including grouped views)
- `view.status` — lag/last refresh
- `view.explain_deps` — dependency graph edges

## 6. transport.* — Protocol negotiation hints
Related: R09

- `transport.capabilities` — advertise supported transports (HTTP/1.1, HTTP/2, QUIC)

## 7. vector.* — Embeddings & ANN search
Related: R10

- `vector.insert` — store embedding values (ValueKind::Embedding)
- `vector.search` — ANN query (LSH buckets + refine)
- `vector.index.status` — index health and coverage

## 8. ai.* — AI-assisted query workflows
Related: R11, R12

- `ai.autoparam.classify` — classify literals (semantic-constant vs parameterizable)
- `ai.autoparam.analyze` — extract literals from SQL and classify them
- `ai.nl.translate` — prompt packaging + optional rule-based translation (read-only by default)
- `ai.nl.explain` — explanation + preview rows for verification
- `ai.nl.execute` — execution gated by approval token

## 9. causal.* — Causal consistency via ETag chains
Related: R13

- `query.select` / `query.execute_prepared` accept `cache.min_causality`
- query results include `causality` tokens (etag_chain_v1)
- `causal.session.begin` / `causal.session.end` (future)

## 10. edge.* — Replay bundles as edge replication primitive
Related: R14

- `edge.bundle.request` — request bounded WAL slice / replay bundle
  - windows: table + seq bounds + max events
  - redaction: `none` | `hash_pk` | `drop_pk`
- `edge.bundle.apply` — apply bundle coverage to the edge node
- `edge.bundle.status` — coverage + bounded-staleness routing verdict

## 11. schema.* (extensions) — Conflict-free schema evolution
Related: R15

- `schema.propose_change` — propose schema evolution changeset
- `schema.merge_status` — show divergence/merge plan
- `schema.apply_merge` — apply merged schema
- Prototype change ops: `add_column` (nullable/default support)

## 12. advisor.* (extensions) — Dependency-driven index synthesis
Related: R16

- `advisor.index_synthesize` — propose indexes based on dependency analysis
- `advisor.apply_index` — apply an index suggestion (in-memory secondary index)
- `advisor.dismiss` — suppress a suggestion
- `advisor.history` — list advisor actions

## 13. migration.* (extensions) — Intent inference
Related: R17

- `migration.intent_report` — detect idioms (pagination, polling, hierarchy self-joins, recursive CTEs, EXISTS membership, COALESCE defaults)
- `migration.rewrite_preview` — show suggested SkeinQL migration

Example (intent_report):
```json
{
  "samples": [
    { "query": { "...": "..." }, "args": [] }
  ],
  "limit": 20,
  "window_ms": 60000
}
```

Example (rewrite_preview):
```json
{
  "samples": [
    { "query": { "...": "..." }, "args": [] }
  ],
  "limit": 10
}
```

## 14. replay.* (extensions) — Performance replay
Related: R18

- `replay.bundle.export_perf` — export bundle with performance state
- `replay.bundle.import_perf` — restore performance state
- `replay.run_perf` — deterministic runner

## 15. wasm.* (extensions) — Wasm-native operators
Related: R19

- `wasm.plan.compile` — compile plan subset to Wasm (see `docs/WASM_OPERATORS.md`)
- `wasm.plan.run` — run compiled plan (sandbox)

## 16. energy.* — Energy-aware scheduling
Related: R20

- `energy.policy.set` — compaction energy policies
- `energy.status` — current energy signals and scheduling decisions

---

**Important:** method shapes are intentionally left high-level in this document. Use the proposal docs to define exact parameter schemas.
