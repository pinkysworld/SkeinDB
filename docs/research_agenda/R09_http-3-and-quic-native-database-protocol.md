# R09 — HTTP/3 and QUIC-Native Database Protocol

**Area:** Web-Native & Modern Applications

## Problem Statement

SkeinDB supports HTTP for administration but uses TCP for the MySQL protocol. Modern web infrastructure increasingly uses HTTP/3 with QUIC, offering multiplexed streams, 0-RTT connection resumption, and better handling of network changes. A QUIC-native database protocol could provide: multiplexed queries without head-of-line blocking, prepared query handles that survive connection migration, and native integration with CDN edge infrastructure.

## Research Hypotheses

- **H1:** QUIC's multiplexed streams eliminate head-of-line blocking, improving p99 latency for concurrent queries compared to TCP-based protocols.
- **H2:** 0-RTT connection resumption with prepared query handles reduces connection establishment overhead for serverless and edge deployments.
- **H3:** QUIC's connection migration enables seamless query continuation across network changes, important for mobile applications.

## Methodology

- Phase 1 - Protocol Design: Design SkeinQL-over-QUIC protocol. Each query uses a separate QUIC stream. Define framing for: (a) query submission, (b) streaming results, (c) prepared query handles, (d) ETag validation.
- Phase 2 - 0-RTT Integration: Implement 0-RTT query submission where the first flight includes a prepared query invocation. Handle replay protection to prevent duplicate writes.
- Phase 3 - Connection Migration: Design stateful query handling that survives connection migration. Query state (cursor position, transaction context) binds to connection ID, transferred during migration.
- Phase 4 - CDN Integration: Explore edge caching integration where CDN nodes can validate ETags and serve cached results without contacting origin database.

## Evaluation Plan

- **E1:** Latency comparison (p50, p99) vs. MySQL protocol over TCP under concurrent query loads.
- **E2:** Connection establishment time with 0-RTT vs. TCP+TLS handshake.
- **E3:** Query continuation success rate during simulated network changes.
- **E4:** CDN cache hit rate and latency reduction for ETag-validated queries.
- **E5:** Protocol overhead (bandwidth, CPU) compared to binary protocols.

## Implementation Status

Status: **Hardened runtime surface with comparative benchmark evidence**.

SkeinDB now ships SkeinQL-over-QUIC using a Quinn-backed listener configured with
`skeindb serve --quic --quic-cert --quic-key`. The runtime uses the documented
length-prefixed JSON frame format, one request/response per bidirectional stream,
and the same JSON-RPC request/response envelope as HTTP. Prepared queries are
transport-neutral and execute over fresh QUIC streams after `query.prepare`.

The test suite covers ping, prepared-query execution, Wasm and vector RPC parity,
0-RTT write rejection, client socket rebind, and multi-stream RPC reuse in
`crates/skeindb/tests/quic_rpc.rs`. Comparative transport benchmarking is now
covered by `skeindb transport-bench`, which drives the same `sql.exec` request
payload over HTTP/2 prior knowledge and QUIC, uses `COM_QUERY` for the same SQL
on MySQL/TCP, and reports nanosecond p50/p95/p99/mean latency summaries.
`crates/skeindb/tests/transport_bench.rs::transport_bench_reports_http2_quic_and_mysql`
locks the end-to-end comparison path against a live multi-protocol server.

## Expected Contributions

- First database protocol designed natively for HTTP/3 and QUIC.
- Integration of prepared query handles with QUIC 0-RTT.
- Query-level connection migration for mobile and edge scenarios.
- Framework for CDN-integrated database query caching.

## Key Related Work

- Iyengar & Thomson 'QUIC: A UDP-Based Multiplexed and Secure Transport' (RFC 9000, 2021); Bishop 'HTTP/3' (RFC 9114, 2022)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Runtime surface:** `docs/TRANSPORT_QUIC.md`, `crates/skeindb/src/quic.rs`, and `crates/skeindb/tests/quic_rpc.rs` for framing/stream mapping, safety, and rebind coverage.
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
