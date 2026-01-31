# Compatibility Telemetry and Migration Hints (MySQL -> SkeinQL)

Status: Draft
Last updated: 2026-01-17

Goal:
Help operators and developers understand which MySQL features their applications use, and provide actionable migration hints to SkeinQL.

This supports a long-term strategy:
- keep MySQL compatibility as an adoption path
- encourage new apps (and gradually existing apps) to move to SkeinQL for better safety and proprietary features

---

## 1) What is collected

SkeinDB records compatibility events per session and per database.
Events are high-level feature flags, not full query text by default.

Examples of feature IDs:
- mysql.SQL_CALC_FOUND_ROWS
- mysql.FOUND_ROWS
- mysql.INSERT_ON_DUPLICATE_KEY_UPDATE
- mysql.SHOW_TABLE_STATUS
- mysql.INFORMATION_SCHEMA_COLUMNS
- mysql.IMPLICIT_TYPE_CAST
- mysql.ZERO_DATE

Optional (opt-in) sampling:
- store a redacted query fingerprint for debugging

---

## 2) Storage model

Expose a virtual table (or internal system table):
- skein_internal.compat_telemetry

Columns:
- ts (unix_ms)
- db
- user
- app_name (if available from connection attributes)
- feature_id
- count
- sample_fingerprint (optional)

Aggregation should be cheap:
- maintain counters in memory and periodically flush

---

## 3) Migration hints

For each feature_id, define:
- a description (what it means)
- risk level (deprecated, slow, unsafe)
- suggested SkeinQL replacement

Examples:

1) mysql.SHOW_TABLES
- SkeinQL replacement: schema.list_tables

2) mysql.INSERT_ON_DUPLICATE_KEY_UPDATE
- SkeinQL replacement: data.upsert

3) mysql.SQL_CALC_FOUND_ROWS
- SkeinQL replacement: query.select with cache.want_etag and an explicit count query

Hints can include code snippets in multiple languages.

---

## 4) API surface

SkeinQL methods:
- telemetry.compat_summary { db?, window_ms? }
- telemetry.compat_top_queries { db?, limit? } (optional)
- telemetry.migration_hints { db?, feature_ids? }
- migration.intent_report { samples?, limit?, window_ms? }
- migration.rewrite_preview { samples?, limit?, window_ms? }

Console pages:
- Compatibility dashboard
- Migration assistant

---

## 5) Privacy and safety

Defaults:
- do not store raw SQL text
- do not store literal parameter values
- store only high-level features

Opt-in modes may store query fingerprints, but must support redaction.

---

## 6) Evaluation

Measure:
- reduction in deprecated feature usage over time
- number of apps migrated to SkeinQL
- decrease in production incidents due to SQL injection class (for SkeinQL clients)

---

## Research extension: Query intent inference

The baseline migration hints are feature-driven ("this MySQL feature is unsupported").
The research agenda proposes **intent inference**: detect higher-level application idioms (pagination, polling, hierarchical queries) and recommend SkeinQL-native alternatives that preserve intent.
See: `docs/research_agenda/R17_query-intent-inference-for-compatibility-migration.md`.

Adaptation sketch:
- Maintain a library of patterns (single-query and multi-query sequences).
- Emit migration reports with "before" (MySQL) and "after" (SkeinQL) examples.
- Offer rewrite previews via `migration.rewrite_preview` for quick adoption.
- Surface recommendations in SkeinAdmin with exportable HTML/markdown reports.
