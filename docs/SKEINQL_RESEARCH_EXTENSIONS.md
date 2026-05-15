# SkeinQL Research Extensions (Experimental)

Last updated: 2026-05-14

This document lists **experimental** SkeinQL method families that support the **20-proposal research agenda**.

Principles:
- These methods are **not required** for MySQL compatibility.
- They should be implemented behind feature flags and exposed only when enabled.
- For each family, the canonical research description is in `docs/research_agenda/`.
- Use `docs/SKEINQL.md` for method payload examples and `docs/API_REFERENCE.md` for the v0.3.17 runtime method map exposed through `system.capabilities`.

## 1. dp.* — Differential privacy
Related: R04

- `dp.aggregate` — COUNT/SUM/AVG aggregates with explicit DP parameters, bounded sensitivities, and `privacy_etag` validators
- `dp.evaluate` — seeded accuracy-vs-epsilon and overhead-vs-exact report for bounded DP aggregate plans
- `dp.budget.get` / `dp.budget.set` — persisted per-principal budget tracking and policies
- `dp.audit.log` — persisted budget consumption events

## 2. oblivious.* — Oblivious execution policies
Related: R05

- `oblivious.policy.get` / `oblivious.policy.set`
- `oblivious.explain` — show which operators will be padded/shuffled
- `oblivious.evaluate` — deterministic trace-based leakage and overhead report for the active table policy

## 3. forensic.* — Forensic WAL queries with proofs
Related: R06

- `forensic.query` — run table/op/id-bounded forensic queries with the SkeinForensic JSON filter grammar and return boundary, checkpoint-anchor, Merkle-root, inclusion-proof, and index-summary metadata
- `forensic.verify` — verify contiguous returned record slices against a supplied start hash
- `forensic.export` — export `skein.forensic.bundle.v1` report bundles with the query manifest, records, proof, and verification summary

## 4. merge.* — Optimistic writes with merge semantics
Related: R07

- `merge.register` — register a per-table merge policy with built-in or values-only Wasm functions
- `merge.apply` — apply a policy to an incoming row with ETag/min-causality conflict hooks
- `merge.simulate` — test current+incoming row merges without committing
- `merge.evaluate` — report conflict rate, resolution success, and merge timing for example workloads
- `merge.wasm.register` / `merge.wasm.list` / `merge.wasm.drop` — manage values-only Wasm merge modules

## 5. view.* — Materialized views + incremental maintenance
Related: R08

- `view.create` / `view.drop`
- `view.refresh` — incremental, full, or auto refresh for restricted single-table views (including grouped views)
- `view.evaluate` — read-only incremental-vs-full correctness oracle and timing report
- `view.status` — lag/last refresh
- `view.explain_deps` — dependency graph edges

## 6. transport.* — Protocol negotiation hints
Related: R09

- `transport.capabilities` — advertise supported transports (HTTP/1.1, HTTP/2, QUIC); the QUIC runtime has framing, prepared-query, 0-RTT write rejection, and rebind coverage, while comparative p99 benchmarking remains open

## 7. vector.* — Embeddings & ANN search
Related: R10

- `vector.insert` — store embedding values (ValueKind::Embedding)
- `vector.search` — ANN query (HNSW when available, with LSH bucket filtering as the prototype fallback) with cache validators, table-version dependency metadata, V2 causality tokens, and source-change invalidation
- `vector.benchmark` — exact brute-force vs indexed top-k recall and latency report for embedding columns
- `vector.index.status` — index health and coverage

See [Vector RAG retrieval](tutorials/vector-rag.md) for a credential-free sample application that uses `vector.insert` and `vector.search` to assemble grounded context.

## 8. ai.* — AI-assisted query workflows
Related: R11, R12

- `ai.autoparam.classify` — classify literals (semantic-constant vs parameterizable)
- `ai.autoparam.analyze` — extract literals from SQL and classify them
- `ai.nl.translate` — prompt packaging + optional rule-based translation (read-only by default)
- `ai.nl.explain` — explanation + preview rows for verification
- `ai.nl.execute` — execution gated by approval token
- `skeindb nl-eval` — JSONL evaluation harness with exact and execution-match metrics

## 9. causal.* — Causal consistency via ETag chains
Related: R13

- `query.select` / `query.execute_prepared` accept `cache.min_causality`
- query results include `causality` tokens (`vector_clock_v2`; legacy `etag_chain_v1` remains accepted on input)
- `causal.session.begin` / `causal.session.end` (future)

## 10. edge.* — Replay bundles as edge replication primitive
Related: R14

- `maintenance.replay.export` — export snapshot replay bundles with optional primary-key redaction (`none`, `hash_pk`, `drop_pk`) before checksums/performance metadata are computed
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

- `maintenance.replay.export` — exports data bundles with optional `performance` profile metadata (`lsm_state`, `cache_warm`, and `timing`).
- `maintenance.replay.import` — validates both correctness checksums and performance-profile checksums when present.
- `maintenance.replay.run` — returns `performance_report` variance deltas for performance-annotated bundles.

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
