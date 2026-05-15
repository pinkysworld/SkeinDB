# R13 — Causal Consistency via ETag Chains

**Area:** Consistency & Distribution

## Problem Statement

SkeinDB's ETags currently reflect 'has this data changed?' but could encode causal dependencies. In distributed deployments, clients often need 'session consistency' - results consistent with everything they've seen - without the overhead of full serializability. By extending ETags to encode causal dependencies, SkeinDB could provide causal consistency guarantees using existing web infrastructure.

## Research Hypotheses

- **H1:** Causal ETags (encoding vector clocks or similar) can be propagated through standard HTTP caching infrastructure.
- **H2:** Causal consistency with ETags provides lower latency than serializability for geo-distributed deployments while satisfying most application requirements.
- **H3:** Incremental causal ETag updates (based on query dependencies) are more efficient than full vector clocks for typical workloads.

## Methodology

- Phase 1 - Causal ETag Design: Design ETag format encoding causal dependencies. Options: (a) compressed vector clocks, (b) dependency set hashes, (c) hybrid version-dependency encoding.
- Phase 2 - Consistency Protocol: Implement causal consistency protocol: (a) queries include 'minimum causality' ETag, (b) responses include 'result causality' ETag, (c) clients propagate causality through their operations.
- Phase 3 - Cluster Integration: Extend SkeinDB clustering to propagate causal metadata. Design replication protocol that preserves causal ordering without requiring total order.
- Phase 4 - Caching Interaction: Analyze interaction with HTTP caching. Can intermediate caches respect causal ETags? Design cache validation protocols that preserve causal guarantees.

## Evaluation Plan

- **E1:** Latency comparison: causal consistency vs. serializability vs. eventual consistency.
- **E2:** Anomaly rate: application-level bugs due to consistency violations under each model.
- **E3:** ETag overhead: size and computation cost of causal ETags.
- **E4:** Geo-distributed benchmark: performance across multiple regions.
- **E5:** Cache effectiveness: hit rate with causal validation vs. traditional ETags.

## Current Runtime Evidence

- `query.select` and `query.execute_prepared` emit `causality` alongside `etag`, `deps`, and `not_modified`, and both accept `min_causality` on requests.
- Response tokens use the `vector_clock_v2` shape: `{"format":"vector_clock_v2","deps":[{"table":"app.users","v":3}]}`.
- The runtime still accepts legacy `etag_chain_v1` tokens on input while clients migrate, via `ensure_min_causality()` compatibility handling.
- Replicated writes now fan out the same dependency token through `x-skeindb-replication-causality`, and replicas expose the merged applied watermark through `cluster.status.replication.causality` and `stats.snapshot.cluster.replication.causality`.
- Cache validation and causality interact in tests: matching `if_none_match` + satisfied `min_causality` returns `not_modified`, and an ahead-of-time token is rejected with `precondition_failed`.
- Evidence: `r13_vector_clock_causality`, `query_select_min_causality_enforced`, `query_execute_prepared_honors_causal_cache_validators`, `replicated_writes_include_causality_header`, and `cluster_replication_ships_schema_and_rows`.

## Expected Contributions

- Novel ETag format encoding causal dependencies.
- Integration of causal consistency with HTTP caching semantics.
- Practical causal consistency for web applications without specialized infrastructure.
- Analysis of caching-consistency interactions in distributed databases.

## Key Related Work

- Lloyd et al. 'COPS: Causal+ Consistency' (2011); Mehdi et al. 'Bolt-on Causal Consistency' (2017)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
