# R11 — LLM-Assisted Query Autoparameterization

**Area:** AI/ML Integration

## Problem Statement

SkeinDB's autoparameterization extracts literals into parameters, but some 'literals' are semantically fixed (status codes, enum values, type discriminators). Incorrect parameterization can lead to poor plan choices or semantic errors. Language models, trained on code and SQL, can classify which literals should be parameterized versus which are semantically fixed, improving plan cache hit rates while avoiding semantic errors.

## Research Hypotheses

- **H1:** LLMs can classify query literals with high accuracy based on context (column names, query structure, value patterns).
- **H2:** Semantic parameterization (guided by LLM classification) achieves higher plan cache hit rates than syntactic parameterization.
- **H3:** Fine-tuned small models (distilled from larger LLMs) can perform classification with acceptable latency for online use.

## Methodology

- Phase 1 - Dataset Construction: Collect query corpuses from open-source applications. Manually label literals as 'parameterizable' or 'semantic constant' based on application semantics.
- Phase 2 - LLM Classification: Evaluate LLM classification accuracy: (a) zero-shot with GPT-4/Claude, (b) few-shot with examples, (c) fine-tuned smaller models. Input includes query text, schema context, and value statistics.
- Phase 3 - Integration: Integrate classification into SkeinDB's compatibility layer. For new query patterns, invoke classifier; cache classification results per query fingerprint.
- Phase 4 - Feedback Loop: Implement feedback mechanism where plan cache misses (due to incorrect parameterization) trigger reclassification with additional context.

## Evaluation Plan

- **E1:** Classification accuracy (precision, recall) compared to human-labeled ground truth.
- **E2:** Plan cache hit rate improvement vs. syntactic-only parameterization.
- **E3:** Classification latency for various model sizes.
- **E4:** Impact on query performance (better plans from correct parameterization).
- **E5:** Cost analysis: LLM API costs vs. efficiency gains.

## Expected Contributions

- First application of LLMs to query parameterization classification.
- Labeled dataset of parameterizable vs. semantic-constant literals.
- Integration framework for LLM-assisted query processing in databases.
- Analysis of accuracy-latency-cost tradeoffs for LLM classification.

## Key Related Work

- Trummer 'From BERT to GPT-3 Codex: Harnessing LLMs for Text-to-SQL' (2022); Zhou et al. 'SQL-PaLM' (2023)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
