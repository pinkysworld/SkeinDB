# R10 — Vector Embeddings as First-Class ValueIDs

**Area:** AI/ML Integration

## Problem Statement

SkeinDB's content-addressed ValueStore could naturally extend to vector embeddings. If ValueIDs incorporate locality-sensitive hashing, similar vectors would have related IDs, potentially enabling deduplication of similar (not just identical) content. This creates a unified storage model where text, embeddings, and structured data share infrastructure, with hybrid queries combining SQL predicates with approximate nearest neighbor (ANN) search.

## Research Hypotheses

- **H1:** Locality-sensitive hashing can extend ValueID semantics to support 'similar' lookups while preserving exact-match deduplication.
- **H2:** Co-locating embeddings with source data in a unified ValueStore reduces the impedance mismatch of separate vector databases.
- **H3:** SkeinDB's dependency tracking can naturally extend to embedding-based queries, enabling cache invalidation when source data changes affect embeddings.

## Methodology

- Phase 1 - LSH-ValueID Design: Design ValueID scheme incorporating locality-sensitive hashing. For embeddings, ValueID = (LSH_bucket, content_hash). Similar embeddings share LSH_bucket, enabling approximate lookup.
- Phase 2 - Unified Storage: Extend ValueStore to support embedding-type values. Implement: (a) embedding insertion with automatic LSH computation, (b) ANN query using LSH buckets as first-stage filter, (c) exact distance computation for refinement.
- Phase 3 - Hybrid Queries: Design SkeinQL extensions for hybrid queries: SELECT * FROM docs WHERE category = 'tech' ORDER BY embedding <-> query_vector LIMIT 10. SQL predicates filter, then ANN ranking applies.
- Phase 4 - Dependency Tracking: Extend dependency tracking to embedding relationships. If document D has embedding E, queries depending on E are invalidated when D changes.

## Evaluation Plan

- **E1:** ANN recall and latency compared to dedicated vector databases (Pinecone, Milvus, pgvector).
- **E2:** Deduplication effectiveness for embedding storage (similar documents with similar embeddings).
- **E3:** Hybrid query performance: SQL filter selectivity vs. ANN performance.
- **E4:** Invalidation correctness: do embedding-dependent queries properly invalidate?
- **E5:** Real-world RAG application benchmark: end-to-end retrieval-augmented generation latency.

## Current Runtime Evidence

- `Lit::Embedding` and `ValueKind::Embedding` provide typed SkeinQL literals and ValueStore-backed embedding objects.
- Embedding ValueIDs combine a deterministic LSH bucket prefix with a content-hash suffix, so exact identity and approximate locality are both represented in the identifier.
- `vector.insert`, `vector.search`, and `vector.index.status` provide the current embedding insert, HNSW/LSH-backed search, and index-inspection surface, including hybrid filter/order-by usage through vector scoring expressions.
- `vector.benchmark` supplies the first built-in E1 harness: it compares exact brute-force top-k results with the indexed search path, reports nanosecond latency percentiles, and computes recall@k for one or more query embeddings.
- SkeinAdmin's Vector panel can run search, benchmark, insert, and index-status calls from the same typed payloads used by client applications.
- `samples/vector_rag_pipeline.py` and the Vector RAG retrieval tutorial demonstrate the end-to-end application path: deterministic embeddings, `vector.insert`, `vector.search` with `include_row`, and grounded prompt assembly without external credentials.

## Expected Contributions

- LSH-extended ValueID scheme for approximate content addressing.
- Unified storage model for structured data and embeddings.
- Hybrid query execution combining SQL predicates with ANN search.
- Dependency tracking for embedding-derived data.

## Key Related Work

- Johnson et al. 'FAISS' (2019); Malkov & Yashunin 'HNSW' (2018); Wang et al. 'Milvus' (2021)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
