# Time-travel queries and reproducible replay bundles

Status: Draft
Last updated: 2026-01-17

This document specifies two related capabilities:

1. Time-travel queries: allow applications and administrators to query data "as of" a previous point in time.
2. Reproducible replay bundles: export a self-contained artifact containing schema and WAL history for deterministic replays in a clean environment.

These features build on SkeinDB's MVCC model (commit timestamps) and write-ahead log (WAL).
Time travel is useful for auditing, debugging, and point-in-time analytics.
Replay bundles are useful for reproducing production-only bugs, doing incident response, and validating correctness after engine changes.

## 1. Time travel semantics

### 1.1 As-of reads

- Each committed write transaction is assigned a monotonically increasing commit timestamp (`commit_ts`).
- Each row version stores `commit_ts` and (optionally) `end_ts` (or a tombstone marker).
- An as-of read at timestamp `T` returns the newest row version with `commit_ts <= T` that is not deleted as of `T`.

Visibility rules:

- If a row was inserted after `T`, it is invisible.
- If a row was deleted at time `D`, it is visible for `T < D` and invisible for `T >= D`.
- For updates, the old version remains visible before the update commit timestamp.

### 1.2 Isolation

- As-of reads are snapshot reads pinned to a historical timestamp.
- A transaction may be started with an `as_of` timestamp; all reads observe that snapshot.
- Writes are rejected in a historical snapshot transaction by default (`read_only=true`), to prevent confusing "time-travel writes".

### 1.3 Retention and garbage collection

Time travel requires retaining old versions. SkeinDB supports a retention policy:

- `retain_versions`: duration or minimum horizon expressed as an oldest retained `commit_ts`.
- `retain_wal`: duration or LSN horizon (affects replay bundle exportability).

Garbage collection removes:

- row versions older than the retention horizon,
- and ValueStore objects that are unreferenced by any retained row version.

#### 1.3.1 `maintenance.history.*` RPC surface (T182)

The retention policy is configured through the `settings.*` subsystem and the
following three RPC methods (matching the `maintenance.compaction.*` layout):

| Method | Direction | Description |
| --- | --- | --- |
| `maintenance.history.status` | read-only | Returns live/tombstone/purgeable row counts per table, the `oldest_tombstone_commit_ts_ms`, the effective `horizon_ms`, and the persisted retention policy. Included in the read-only RPC allowlist. |
| `maintenance.history.set_policy` | write | Persists `history.retention.enabled` (bool) and `history.retention.window_ms` (u64). When enabled, an absent explicit `horizon_ms` in subsequent calls resolves to `now_ms - window_ms`. |
| `maintenance.history.gc` | write | Permanently removes MVCC tombstones whose `commit_ts_ms <= horizon_ms`. Accepts an explicit `horizon_ms` parameter; otherwise uses the policy-derived horizon. |

Horizon resolution precedence:

1. Explicit `params.horizon_ms` (if provided, wins outright).
2. `history.retention.enabled == true` and `history.retention.window_ms > 0` → `now_ms - window_ms`.
3. Otherwise `None` (status reports all tombstones as purgeable; GC purges all timestamped tombstones).

Safety rule: tombstones with `commit_ts_ms == 0` are **never** purged. These
originate from the pre-T180 era when tombstones did not carry a commit
timestamp; retaining them avoids accidentally resurrecting rows whose
deletion point cannot be proven. Operators should monitor the status
surface's `oldest_tombstone_commit_ts_ms` to confirm the steady state.

After a successful GC pass per table the engine:

1. Rebuilds the primary-key index (`pk_index`) since retained-row indices shift.
2. Bumps `schema.table_version` so secondary indexes refresh lazily on next use.
3. Clears cached vector indexes (stored row indices are stale).
4. Persists the table to disk (best-effort; the first error is returned as `history_gc_partial` after the in-memory pass completes).

### 1.4 SQL compatibility surface

Because SQL/MySQL is a compatibility ingress, SkeinDB exposes time travel to SQL clients without requiring new SQL syntax:

- Session variable: `SET @@skein.as_of = '2026-01-01T00:00:00Z';` affects subsequent `SELECT` statements (read-only).
- Query hint: `SELECT /*+ SKEIN_AS_OF('2026-01-01T00:00:00Z') */ ...;` overrides session setting for the statement.

The default remains current-time behavior.

## 2. Replay bundles

### 2.1 Goals

A replay bundle should allow:

- reconstructing database state at a chosen point (LSN or commit_ts),
- replaying a sequence of operations deterministically,
- sharing a minimal artifact with developers without exposing unrelated data (optional redaction).

### 2.2 Bundle contents

A bundle includes:

- a schema snapshot (catalog tables + schema version map at `start_lsn`),
- a WAL slice (records from `start_lsn` to `end_lsn`),
- optional ValueStore objects referenced by the WAL slice (content-addressed, deduplicated),
- a manifest (JSON) containing:
  - engine version, build identifier (if available),
  - start/end LSN and start/end timestamps,
  - checksums for integrity (leveraging AUDIT_WAL hash chaining if enabled),
  - redaction policy metadata.

### 2.3 Deterministic replay model

Replay applies WAL records in commit order.
To improve determinism during debugging:

- use the WAL-defined record ordering,
- fix randomness seeds used by internal scheduling during replay,
- disable background compaction (or run it in deterministic mode with a fixed plan).

Replay bundles are intended for debugging/validation, not for live replication.

### 2.4 Redaction modes (optional)

- `full`: include all referenced values (default).
- `schema_only`: include schema + statement shapes without payload values.
- `selective`: include only values for listed tables/keys.

Redaction must be used carefully because it can change query plans and outcomes.

## 3. APIs

### 3.1 SkeinQL additions

- `query.select`: add optional `as_of` ISO timestamp.
- `tx.begin`: add optional `as_of` timestamp; default `read_only=true` for historical snapshots.
- `maintenance.replay.export`: creates a replay bundle.
- `maintenance.replay.import`: imports a bundle into a temporary replay workspace.
- `maintenance.replay.run`: replays until target LSN/ts and reports checksums.

### 3.2 CLI additions

- `skeindb replay export --db mydb --from-lsn X --to-lsn Y --out file.sreplay`
- `skeindb replay verify --bundle file.sreplay`
- `skeindb replay run --bundle file.sreplay --until-ts <iso>`

## 4. Observability

Expose at least:

- `history.retained_bytes`
- `history.oldest_retained_commit_ts`
- `replay.exports_total`
- `replay.imports_total`
- `replay.verify_failures_total`

## 5. Testing

Minimum tests:

- visibility correctness across insert/update/delete at different `as_of` timestamps.
- retention GC does not delete versions required by policy.
- replay bundle round trip: export -> import -> checksum matches reference snapshot.

---

## Research extension: Geo-distributed replay bundles for edge caching

Replay bundles are designed for debugging and reproducibility, but they can also serve as a **partial replication** primitive for edge deployments.
See: `docs/research_agenda/R14_geo-distributed-replay-bundles-for-edge-caching.md`.

Adaptation sketch:
- Edge nodes maintain bounded WAL windows ("bundle coverage") for hot tables.
- A router can choose edge vs origin based on a staleness bound (bounded-staleness reads).
- Bundles can be compacted and redacted (privacy policies) before distribution.
- Prototype methods: `edge.bundle.request`, `edge.bundle.apply`, `edge.bundle.status`.

---

## Research extension: Performance-annotated replay bundles

Correctness replay is often not sufficient for performance investigations.
The agenda proposes extending bundles to capture performance-critical state (LSM layout metadata, cache warm-hints, and timing annotations) to enable reproducible performance regression tests.
See: `docs/research_agenda/R18_reproducible-performance-regression-testing.md`.

Adaptation sketch:
- Extend the bundle format with optional sections: `lsm_state`, `cache_warm`, and `timing`.
- Provide a deterministic replay runner that replays operations while injecting timing.
- Provide a variance report (how close replayed p95/p99 are to the captured baseline).
