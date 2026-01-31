# R12 — Natural Language to SkeinQL with Verification

**Area:** AI/ML Integration

## Problem Statement

Natural language interfaces to databases often produce incorrect queries, and users cannot verify correctness without understanding the generated query. SkeinQL's structured JSON-RPC format is more amenable to LLM generation than raw SQL, and SkeinDB's dependency tracking can help verify that generated queries match user intent by enumerating what data the query could return, enabling a verification step before execution.

## Research Hypotheses

- **H1:** LLMs generate more accurate queries in SkeinQL's structured format than in SQL, due to explicit field names and reduced syntactic complexity.
- **H2:** Dependency tracking can generate 'query explanations' that help users verify intent without understanding SkeinQL syntax.
- **H3:** Iterative refinement (user feedback on explanations) converges to correct queries faster than direct SQL editing.

## Methodology

- Phase 1 - NL-to-SkeinQL: Implement natural language to SkeinQL translation using LLMs. Provide schema context, example queries, and SkeinQL documentation in prompts.
- Phase 2 - Explanation Generation: Use dependency tracking to generate explanations: 'This query will return rows from table X where column Y matches Z, and could return up to N rows based on current data.'
- Phase 3 - Verification Protocol: Design verification UI: (a) show generated query explanation, (b) present sample results (dry run on subset), (c) allow user to confirm, modify, or reject.
- Phase 4 - Refinement Loop: Implement iterative refinement where user feedback ('no, I meant last week, not this week') is incorporated into subsequent generation attempts.

## Evaluation Plan

- **E1:** Query accuracy (exact match, execution match) on Spider and WikiSQL benchmarks adapted for SkeinQL.
- **E2:** User study: can non-technical users verify query correctness using explanations?
- **E3:** Refinement efficiency: iterations needed to reach correct query.
- **E4:** Comparison with direct SQL generation (accuracy, user preference).
- **E5:** Safety evaluation: does verification prevent unintended data access or modification?

## Expected Contributions

- Natural language interface to SkeinQL with formal verification.
- Dependency-tracking-based query explanation generation.
- Human-in-the-loop verification protocol for AI-generated queries.
- Empirical comparison of structured vs. SQL-based NL interfaces.

## Key Related Work

- Scholak et al. 'PICARD' (2021); Rajkumar et al. 'Evaluating Text-to-SQL' (2022)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
