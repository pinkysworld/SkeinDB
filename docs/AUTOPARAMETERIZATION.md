# SQL autoparameterization and query fingerprints

Status: Draft
Last updated: 2026-01-17

Many applications (especially web apps) send large numbers of ad-hoc SQL statements that differ only in literal values.
This increases parse/plan overhead, fragments telemetry, and prevents effective caching/coalescing.

This document specifies "autoparameterization" for SkeinDB's SQL/MySQL compatibility layer:
- compute a normalized statement form (fingerprint/digest)
- extract parameters (typed)
- optionally route execution through the plan cache keyed by fingerprint

The design is inspired by existing normalized query tracking systems.

## 1. Goals

- Improve performance by reusing parse/plan work for repetitive statements.
- Provide stable query identifiers for telemetry, query coalescing, and ETag caching.
- Preserve MySQL protocol compatibility.

Non-goals:
- Full SQL server optimizer equivalence.
- Autoparameterizing statements where it changes semantics.

## 2. Normalization

Given a parsed SQL AST, compute a canonical form:
- keywords lowercased
- whitespace removed
- identifiers normalized as they appear (no renaming)
- constants replaced with parameters (?, $1...) depending on output format

Example:
- Input: SELECT * FROM users WHERE id = 123 AND status = 'active'
- Normalized: select * from users where id = ? and status = ?

## 3. Parameter extraction

From the AST, collect literals in left-to-right order and produce typed parameters:
- int/float/string/bytes/date/time
- NULL retains type "null"

## 4. Safety rules

Do NOT parameterize:
- identifiers (table/column names)
- ORDER BY direction (ASC/DESC)
- LIMIT/OFFSET when used to control optimizer behavior (policy choice)

Parameterize cautiously:
- IN (...) lists: optionally parameterize but cap list length
- LIKE patterns: safe to parameterize, but may affect index usage; acceptable

## 5. Plan cache integration

- Key: (normalized_sql, schema_version, session_flags)
- Value: prepared plan / SkeinIR + physical plan

Sessions may opt in using:
- SET @@skein.autoparameterize = 1

## 6. SkeinQL surfaces

- telemetry includes fingerprint and counts
- stats.top_queries groups by fingerprint
- query.prepare may accept sql text and return query_id + parameter schema

## 7. Observability

Expose:
- autoparam.enabled
- autoparam.hit_rate
- autoparam.cache_entries

## 8. Testing

- normalization is stable across whitespace and literal changes
- execution results identical with and without autoparam (for supported statements)

---

## Research extension: LLM-assisted semantic autoparameterization

Baseline autoparameterization is syntactic: it replaces literals with parameters.
The research agenda proposes a semantic classifier that can identify literals that should remain constants (enums, status codes, type tags) to improve plan quality and avoid poor grouping.
See: `docs/research_agenda/R11_llm-assisted-query-autoparameterization.md`.

Adaptation sketch:
- Introduce a label taxonomy: 
  - parameterize (default)
  - semantic_constant
  - unknown (defer)
- Make the classifier pluggable: offline rules first; LLM-assisted mode as optional.
- Cache classification results by query fingerprint and schema version.
- Add feedback: frequent plan-cache misses or regressions trigger reclassification.

Prototype RPC:
- `ai.autoparam.classify` accepts a list of literal contexts + optional rules and returns labels (see `docs/SKEINQL.md`).
- `ai.autoparam.analyze` accepts SQL text and returns normalized SQL, fingerprint, literals, and labels.
