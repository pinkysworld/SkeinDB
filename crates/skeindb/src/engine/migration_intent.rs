//! Migration intent inference (R17): detect common MySQL application idioms
//! (pagination, polling, soft-delete, hierarchy, recursive-CTE, EXISTS, COALESCE)
//! from query/workload samples and render SkeinQL-native rewrite previews and
//! reports. Self-contained analysis types + free functions extracted from engine.rs
//! as part of the monolith split (see CONTRIBUTING.md > Working style).

use super::*;

// -----------------------------
// Migration intent inference (R17)
// -----------------------------

#[derive(Debug, Clone)]
pub(crate) enum ComparisonValue {
    Lit(Lit),
    Param(u32),
}

#[derive(Debug, Clone)]
pub(crate) struct Comparison {
    col: String,
    op: String,
    value: ComparisonValue,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnRef {
    table: Option<String>,
    col: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PaginationSignal {
    table: Option<BaseTableRef>,
    order_col: Option<String>,
    limit: Option<u64>,
    offset: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PollingSignal {
    table: Option<BaseTableRef>,
    column: String,
    value: Option<Lit>,
    order_match: bool,
    limit: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct SoftDeleteSignal {
    table: Option<BaseTableRef>,
    column: String,
}

#[derive(Debug, Clone)]
pub(crate) struct HierarchySignal {
    table: BaseTableRef,
    columns: Vec<String>,
    parent_col: Option<String>,
    id_col: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecursiveCteSignal {
    cte_name: String,
    table: Option<BaseTableRef>,
    parent_col: Option<String>,
    id_col: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ExistsSignal {
    outer_table: Option<BaseTableRef>,
    inner_table: Option<BaseTableRef>,
    inner_column: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CoalesceSignal {
    table: Option<BaseTableRef>,
    column: String,
    default_value: Option<Lit>,
    arg_count: usize,
}

pub(crate) fn detect_migration_intents(
    samples: &[MigrationIntentSample],
) -> Vec<MigrationIntentSuggestion> {
    let mut suggestions = Vec::new();

    let mut pagination_evidence = Vec::new();
    let mut pagination_offsets = Vec::new();
    let mut pagination_best: Option<PaginationSignal> = None;

    let mut polling_evidence = Vec::new();
    let mut polling_values: HashMap<String, Vec<f64>> = HashMap::new();
    let mut polling_best: Option<PollingSignal> = None;

    let mut soft_delete_evidence = Vec::new();
    let mut soft_delete_best: Option<SoftDeleteSignal> = None;

    let mut hierarchy_evidence = Vec::new();
    let mut hierarchy_best: Option<HierarchySignal> = None;

    let mut recursive_evidence = Vec::new();
    let mut recursive_best: Option<RecursiveCteSignal> = None;

    let mut exists_evidence = Vec::new();
    let mut exists_best: Option<ExistsSignal> = None;

    let mut coalesce_evidence = Vec::new();
    let mut coalesce_best: Option<CoalesceSignal> = None;

    for (idx, sample) in samples.iter().enumerate() {
        if let Some(signal) = detect_pagination_signal(&sample.query) {
            pagination_offsets.push(signal.offset);
            let mut columns = Vec::new();
            if let Some(col) = signal.order_col.clone() {
                columns.push(col);
            }
            pagination_evidence.push(MigrationIntentEvidence {
                query_index: idx as u64,
                table: signal.table.clone(),
                columns,
                note: Some(format!("limit={:?} offset={}", signal.limit, signal.offset)),
            });
            let prefer_signal = match pagination_best.as_ref() {
                None => true,
                Some(best) => best.order_col.is_none() && signal.order_col.is_some(),
            };
            if prefer_signal {
                pagination_best = Some(signal);
            }
        }

        if let Some(signal) = detect_polling_signal(&sample.query, &sample.args) {
            if let Some(value) = signal.value.as_ref().and_then(lit_to_f64) {
                polling_values
                    .entry(signal.column.clone())
                    .or_default()
                    .push(value);
            }
            polling_evidence.push(MigrationIntentEvidence {
                query_index: idx as u64,
                table: signal.table.clone(),
                columns: vec![signal.column.clone()],
                note: Some(format!(
                    "order_match={} limit={:?}",
                    signal.order_match, signal.limit
                )),
            });
            let prefer_signal = match polling_best.as_ref() {
                None => true,
                Some(best) => !best.order_match && signal.order_match,
            };
            if prefer_signal {
                polling_best = Some(signal);
            }
        }

        if let Some(signal) = detect_soft_delete_signal(&sample.query, &sample.args) {
            soft_delete_evidence.push(MigrationIntentEvidence {
                query_index: idx as u64,
                table: signal.table.clone(),
                columns: vec![signal.column.clone()],
                note: Some("soft delete predicate".to_string()),
            });
            if soft_delete_best.is_none() {
                soft_delete_best = Some(signal);
            }
        }

        for signal in detect_hierarchy_signals(&sample.query).into_iter() {
            let mut columns = signal.columns.clone();
            if columns.is_empty() {
                if let Some(parent) = signal.parent_col.clone() {
                    columns.push(parent);
                }
                if let Some(id_col) = signal.id_col.clone() {
                    columns.push(id_col);
                }
            }
            hierarchy_evidence.push(MigrationIntentEvidence {
                query_index: idx as u64,
                table: Some(signal.table.clone()),
                columns,
                note: Some("self join hierarchy".to_string()),
            });
            let prefer_signal = match hierarchy_best.as_ref() {
                None => true,
                Some(best) => best.parent_col.is_none() && signal.parent_col.is_some(),
            };
            if prefer_signal {
                hierarchy_best = Some(signal);
            }
        }

        for signal in detect_recursive_cte_signals(&sample.query).into_iter() {
            let mut columns = Vec::new();
            if let Some(parent) = signal.parent_col.clone() {
                columns.push(parent);
            }
            if let Some(id_col) = signal.id_col.clone() {
                columns.push(id_col);
            }
            recursive_evidence.push(MigrationIntentEvidence {
                query_index: idx as u64,
                table: signal.table.clone(),
                columns,
                note: Some(format!("recursive cte: {}", signal.cte_name)),
            });
            let prefer_signal = match recursive_best.as_ref() {
                None => true,
                Some(best) => best.parent_col.is_none() && signal.parent_col.is_some(),
            };
            if prefer_signal {
                recursive_best = Some(signal);
            }
        }

        for signal in detect_exists_signals(&sample.query).into_iter() {
            let mut columns = Vec::new();
            if let Some(col) = signal.inner_column.clone() {
                columns.push(col);
            }
            exists_evidence.push(MigrationIntentEvidence {
                query_index: idx as u64,
                table: signal
                    .inner_table
                    .clone()
                    .or_else(|| signal.outer_table.clone()),
                columns,
                note: Some("exists subquery".to_string()),
            });
            let prefer_signal = match exists_best.as_ref() {
                None => true,
                Some(best) => best.inner_table.is_none() && signal.inner_table.is_some(),
            };
            if prefer_signal {
                exists_best = Some(signal);
            }
        }

        for signal in detect_coalesce_signals(&sample.query).into_iter() {
            let mut note = format!("coalesce args={}", signal.arg_count);
            if let Some(default_value) = signal.default_value.as_ref() {
                note.push_str(&format!(" default={}", render_lit_summary(default_value)));
            }
            coalesce_evidence.push(MigrationIntentEvidence {
                query_index: idx as u64,
                table: signal.table.clone(),
                columns: vec![signal.column.clone()],
                note: Some(note),
            });
            let prefer_signal = match coalesce_best.as_ref() {
                None => true,
                Some(best) => best.default_value.is_none() && signal.default_value.is_some(),
            };
            if prefer_signal {
                coalesce_best = Some(signal);
            }
        }
    }

    if !pagination_evidence.is_empty() {
        let distinct_offsets = pagination_offsets
            .iter()
            .cloned()
            .filter(|v| *v > 0)
            .collect::<HashSet<_>>()
            .len();
        let mut confidence = 0.55;
        if pagination_best
            .as_ref()
            .and_then(|sig| sig.order_col.as_ref())
            .is_some()
        {
            confidence += 0.2;
        }
        if distinct_offsets >= 2 {
            confidence += 0.1;
        }
        if pagination_evidence.len() > 1 {
            confidence += ((pagination_evidence.len() - 1) as f64 * 0.05).min(0.2);
        }
        if pagination_best
            .as_ref()
            .map(|sig| sig.offset > 0)
            .unwrap_or(false)
        {
            confidence += 0.05;
        }
        confidence = confidence.min(1.0);

        let (table_hint, order_col, limit) = pagination_best
            .as_ref()
            .map(|sig| (sig.table.clone(), sig.order_col.clone(), sig.limit))
            .unwrap_or((None, None, None));
        let table_label = table_hint
            .as_ref()
            .map(|t| format!("{}.{}", t.db, t.table))
            .unwrap_or_else(|| "<table>".to_string());
        let order_label = order_col.unwrap_or_else(|| "<cursor_column>".to_string());
        let limit_label = limit.unwrap_or(50);
        let skeinql_snippet = format!(
            "cursor pagination:\nquery.select {{ query: SELECT ... FROM {} WHERE {} > ? ORDER BY {} LIMIT {} }}",
            table_label, order_label, order_label, limit_label
        );

        suggestions.push(MigrationIntentSuggestion {
            intent: "pagination.offset_limit".to_string(),
            confidence,
            title: "Offset pagination detected".to_string(),
            recommendation: "Replace LIMIT/OFFSET pagination with cursor-based pagination on a stable ordering column.".to_string(),
            skeinql_snippet: Some(skeinql_snippet),
            evidence: pagination_evidence,
        });
    }

    if !polling_evidence.is_empty() {
        let mut confidence = 0.45;
        if polling_best
            .as_ref()
            .map(|sig| sig.order_match)
            .unwrap_or(false)
        {
            confidence += 0.2;
        }
        if polling_best
            .as_ref()
            .and_then(|sig| sig.limit)
            .map(|limit| limit <= 100)
            .unwrap_or(false)
        {
            confidence += 0.1;
        }
        let has_sequence = polling_values
            .values()
            .any(|values| has_increasing_sequence(values));
        if has_sequence {
            confidence += 0.2;
        }
        if polling_evidence.len() > 1 {
            confidence += ((polling_evidence.len() - 1) as f64 * 0.05).min(0.2);
        }
        confidence = confidence.min(1.0);

        let (table_hint, column) = polling_best
            .as_ref()
            .map(|sig| (sig.table.clone(), sig.column.clone()))
            .unwrap_or((None, "<cursor_column>".to_string()));
        let skeinql_snippet = format!(
            "polling detected:\ncdc.subscribe_table {{ db: \"{}\", table: \"{}\" }} then cdc.poll {{ sub_id, from_offset }} (cursor: {})",
            table_hint
                .as_ref()
                .map(|t| t.db.as_str())
                .unwrap_or("<db>"),
            table_hint
                .as_ref()
                .map(|t| t.table.as_str())
                .unwrap_or("<table>"),
            column
        );

        suggestions.push(MigrationIntentSuggestion {
            intent: "polling.incremental".to_string(),
            confidence,
            title: "Incremental polling detected".to_string(),
            recommendation:
                "Replace polling SELECTs with CDC subscriptions to reduce load and latency."
                    .to_string(),
            skeinql_snippet: Some(skeinql_snippet),
            evidence: polling_evidence,
        });
    }

    if !soft_delete_evidence.is_empty() {
        let mut confidence = 0.5;
        if soft_delete_evidence.len() > 1 {
            confidence += ((soft_delete_evidence.len() - 1) as f64 * 0.05).min(0.2);
        }
        confidence = confidence.min(1.0);

        let (table_hint, column) = soft_delete_best
            .as_ref()
            .map(|sig| (sig.table.clone(), sig.column.clone()))
            .unwrap_or((None, "<deleted_column>".to_string()));
        let table_label = table_hint
            .as_ref()
            .map(|t| format!("{}.{}", t.db, t.table))
            .unwrap_or_else(|| "<table>".to_string());
        let skeinql_snippet = format!(
            "soft delete filter:\nview.create {{ name: \"active_{}\", query: SELECT ... FROM {} WHERE {} IS NULL }}",
            table_hint
                .as_ref()
                .map(|t| t.table.as_str())
                .unwrap_or("table"),
            table_label,
            column
        );

        suggestions.push(MigrationIntentSuggestion {
            intent: "soft_delete.filter".to_string(),
            confidence,
            title: "Soft delete filter detected".to_string(),
            recommendation:
                "Consider a filtered view for active rows to centralize soft-delete logic."
                    .to_string(),
            skeinql_snippet: Some(skeinql_snippet),
            evidence: soft_delete_evidence,
        });
    }

    if !hierarchy_evidence.is_empty() {
        let mut confidence = 0.45;
        if hierarchy_best
            .as_ref()
            .and_then(|sig| sig.parent_col.as_ref())
            .is_some()
        {
            confidence += 0.15;
        }
        if hierarchy_best
            .as_ref()
            .and_then(|sig| sig.id_col.as_ref())
            .is_some()
        {
            confidence += 0.1;
        }
        if hierarchy_evidence.len() > 1 {
            confidence += ((hierarchy_evidence.len() - 1) as f64 * 0.05).min(0.2);
        }
        confidence = confidence.min(1.0);

        let (table_hint, parent_col, id_col) = hierarchy_best
            .as_ref()
            .map(|sig| {
                let (parent_col, id_col) = hierarchy_columns_from_signal(sig);
                (sig.table.clone(), parent_col, id_col)
            })
            .unwrap_or_else(|| {
                (
                    BaseTableRef {
                        db: "<db>".to_string(),
                        table: "<table>".to_string(),
                        r#as: None,
                    },
                    "<parent_id>".to_string(),
                    "<id>".to_string(),
                )
            });
        let table_label = format!("{}.{}", table_hint.db, table_hint.table);
        let skeinql_snippet = format!(
            "hierarchy traversal:\nroots = query.select {{ query: SELECT {} FROM {} WHERE {} IS NULL }}\npaths = graph.traverse {{ db: \"{}\", table: \"{}\", edge: {{ parent: \"{}\", id: \"{}\" }}, roots, max_depth: 10 }}",
            id_col,
            table_label,
            parent_col,
            table_hint.db,
            table_hint.table,
            parent_col,
            id_col
        );

        suggestions.push(MigrationIntentSuggestion {
            intent: "hierarchy.adjacency".to_string(),
            confidence,
            title: "Hierarchy self-join detected".to_string(),
            recommendation:
                "Consider a graph traversal or hierarchy view to model parent/child relationships."
                    .to_string(),
            skeinql_snippet: Some(skeinql_snippet),
            evidence: hierarchy_evidence,
        });
    }

    if !recursive_evidence.is_empty() {
        let mut confidence = 0.5;
        if recursive_best
            .as_ref()
            .and_then(|sig| sig.parent_col.as_ref())
            .is_some()
        {
            confidence += 0.15;
        }
        if recursive_best
            .as_ref()
            .and_then(|sig| sig.id_col.as_ref())
            .is_some()
        {
            confidence += 0.1;
        }
        if recursive_evidence.len() > 1 {
            confidence += ((recursive_evidence.len() - 1) as f64 * 0.05).min(0.2);
        }
        confidence = confidence.min(1.0);

        let (table_hint, parent_col, id_col) = recursive_best
            .as_ref()
            .map(|sig| {
                let (parent_col, id_col) = recursive_columns_from_signal(sig);
                (
                    sig.table.clone().unwrap_or(BaseTableRef {
                        db: "<db>".to_string(),
                        table: "<table>".to_string(),
                        r#as: None,
                    }),
                    parent_col,
                    id_col,
                )
            })
            .unwrap_or_else(|| {
                (
                    BaseTableRef {
                        db: "<db>".to_string(),
                        table: "<table>".to_string(),
                        r#as: None,
                    },
                    "<parent_id>".to_string(),
                    "<id>".to_string(),
                )
            });
        let table_label = format!("{}.{}", table_hint.db, table_hint.table);
        let skeinql_snippet = format!(
            "hierarchy traversal:\nroots = query.select {{ query: SELECT {} FROM {} WHERE {} IS NULL }}\npaths = graph.traverse {{ db: \"{}\", table: \"{}\", edge: {{ parent: \"{}\", id: \"{}\" }}, roots, max_depth: 10 }}",
            id_col,
            table_label,
            parent_col,
            table_hint.db,
            table_hint.table,
            parent_col,
            id_col
        );

        suggestions.push(MigrationIntentSuggestion {
            intent: "hierarchy.recursive_cte".to_string(),
            confidence,
            title: "Recursive hierarchy CTE detected".to_string(),
            recommendation:
                "Consider graph traversal helpers for recursive parent/child structures."
                    .to_string(),
            skeinql_snippet: Some(skeinql_snippet),
            evidence: recursive_evidence,
        });
    }

    if !exists_evidence.is_empty() {
        let mut confidence = 0.45;
        if exists_best
            .as_ref()
            .and_then(|sig| sig.inner_table.as_ref())
            .is_some()
        {
            confidence += 0.1;
        }
        if exists_best
            .as_ref()
            .and_then(|sig| sig.inner_column.as_ref())
            .is_some()
        {
            confidence += 0.05;
        }
        if exists_evidence.len() > 1 {
            confidence += ((exists_evidence.len() - 1) as f64 * 0.05).min(0.2);
        }
        confidence = confidence.min(1.0);

        let (outer_table, inner_table, inner_column) = exists_best
            .as_ref()
            .map(|sig| {
                (
                    sig.outer_table.clone(),
                    sig.inner_table.clone(),
                    sig.inner_column.clone(),
                )
            })
            .unwrap_or((None, None, None));
        let outer_label = outer_table
            .as_ref()
            .map(|t| format!("{}.{}", t.db, t.table))
            .unwrap_or_else(|| "<outer_table>".to_string());
        let inner_label = inner_table
            .as_ref()
            .map(|t| format!("{}.{}", t.db, t.table))
            .unwrap_or_else(|| "<inner_table>".to_string());
        let inner_column = inner_column.unwrap_or_else(|| "<inner_id>".to_string());
        let skeinql_snippet = format!(
            "membership join:\nquery.select {{ query: SELECT ... FROM {} JOIN {} ON {}.{} = {}.<outer_id> }}",
            outer_label, inner_label, inner_label, inner_column, outer_label
        );

        suggestions.push(MigrationIntentSuggestion {
            intent: "exists.membership".to_string(),
            confidence,
            title: "Membership EXISTS detected".to_string(),
            recommendation:
                "Replace EXISTS subqueries with explicit joins or a membership view for reuse."
                    .to_string(),
            skeinql_snippet: Some(skeinql_snippet),
            evidence: exists_evidence,
        });
    }

    if !coalesce_evidence.is_empty() {
        let mut confidence = 0.4;
        if coalesce_best
            .as_ref()
            .and_then(|sig| sig.default_value.as_ref())
            .is_some()
        {
            confidence += 0.1;
        }
        if coalesce_evidence.len() > 1 {
            confidence += ((coalesce_evidence.len() - 1) as f64 * 0.05).min(0.2);
        }
        confidence = confidence.min(1.0);

        let (table_hint, column, default_value) = coalesce_best
            .as_ref()
            .map(|sig| {
                (
                    sig.table.clone(),
                    sig.column.clone(),
                    sig.default_value.clone(),
                )
            })
            .unwrap_or((None, "<column>".to_string(), None));
        let table_label = table_hint
            .as_ref()
            .map(|t| format!("{}.{}", t.db, t.table))
            .unwrap_or_else(|| "<table>".to_string());
        let view_name = table_hint
            .as_ref()
            .map(|t| format!("defaults_{}", t.table))
            .unwrap_or_else(|| "defaults_table".to_string());
        let default_label = default_value
            .as_ref()
            .map(render_lit_summary)
            .unwrap_or_else(|| "<default>".to_string());
        let skeinql_snippet = format!(
            "default normalization:\nview.create {{ name: \"{}\", query: SELECT ..., coalesce({}, {}) AS {} FROM {} }}",
            view_name, column, default_label, column, table_label
        );

        suggestions.push(MigrationIntentSuggestion {
            intent: "defaults.coalesce".to_string(),
            confidence,
            title: "Coalesce defaults detected".to_string(),
            recommendation:
                "Consider a view to centralize default value logic for nullable columns."
                    .to_string(),
            skeinql_snippet: Some(skeinql_snippet),
            evidence: coalesce_evidence,
        });
    }

    suggestions
}

pub(crate) fn rewrite_preview_from_suggestion(
    suggestion: &MigrationIntentSuggestion,
    samples: &[MigrationIntentSample],
) -> MigrationRewritePreview {
    let (before, after) = rewrite_snippets_for_intent(suggestion, samples);
    MigrationRewritePreview {
        intent: suggestion.intent.clone(),
        confidence: suggestion.confidence,
        title: suggestion.title.clone(),
        before,
        after,
        evidence: suggestion.evidence.clone(),
    }
}

pub(crate) fn rewrite_snippets_for_intent(
    suggestion: &MigrationIntentSuggestion,
    samples: &[MigrationIntentSample],
) -> (String, String) {
    let table_ref = evidence_table_ref(&suggestion.evidence).or_else(|| {
        sample_from_evidence(samples, &suggestion.evidence)
            .and_then(|sample| query_single_base_table(&sample.query))
    });
    let mut table_label = table_ref
        .as_ref()
        .map(|table| format!("{}.{}", table.db, table.table))
        .unwrap_or_else(|| "<table>".to_string());
    let column_hint =
        evidence_column_label(&suggestion.evidence).unwrap_or_else(|| "<column>".to_string());
    let sample = sample_from_evidence(samples, &suggestion.evidence);

    match suggestion.intent.as_str() {
        "pagination.offset_limit" => {
            let mut order_col = column_hint.clone();
            let mut limit = 50u64;
            let mut offset = 0u64;
            if let Some(sample) = sample {
                if let Some(signal) = detect_pagination_signal(&sample.query) {
                    if let Some(col) = signal.order_col {
                        order_col = col;
                    }
                    if let Some(sig_limit) = signal.limit {
                        limit = sig_limit;
                    }
                    offset = signal.offset;
                }
            }
            let before = format!(
                "SELECT ... FROM {} ORDER BY {} LIMIT {} OFFSET {}",
                table_label, order_col, limit, offset
            );
            let after = suggestion.skeinql_snippet.clone().unwrap_or_else(|| {
                format!(
                    "cursor pagination:\nquery.select {{ query: SELECT ... FROM {} WHERE {} > ? ORDER BY {} LIMIT {} }}",
                    table_label, order_col, order_col, limit
                )
            });
            (before, after)
        }
        "polling.incremental" => {
            let mut column = column_hint.clone();
            let mut limit = 100u64;
            if let Some(sample) = sample {
                if let Some(signal) = detect_polling_signal(&sample.query, &sample.args) {
                    column = signal.column;
                    if let Some(sig_limit) = signal.limit {
                        limit = sig_limit;
                    }
                }
            }
            let before = format!(
                "SELECT ... FROM {} WHERE {} > ? ORDER BY {} LIMIT {}",
                table_label, column, column, limit
            );
            let after = suggestion.skeinql_snippet.clone().unwrap_or_else(|| {
                let db = table_ref
                    .as_ref()
                    .map(|table| table.db.as_str())
                    .unwrap_or("<db>");
                let table = table_ref
                    .as_ref()
                    .map(|table| table.table.as_str())
                    .unwrap_or("<table>");
                format!(
                    "cdc.subscribe_table {{ db: \"{}\", table: \"{}\" }} then cdc.poll {{ sub_id, from_offset }}",
                    db, table
                )
            });
            (before, after)
        }
        "soft_delete.filter" => {
            let mut column = column_hint.clone();
            if let Some(sample) = sample {
                if let Some(signal) = detect_soft_delete_signal(&sample.query, &sample.args) {
                    column = signal.column;
                }
            }
            let before = format!("SELECT ... FROM {} WHERE {} IS NULL", table_label, column);
            let after = suggestion.skeinql_snippet.clone().unwrap_or_else(|| {
                format!(
                    "view.create {{ name: \"active_<table>\", query: SELECT ... FROM {} WHERE {} IS NULL }}",
                    table_label, column
                )
            });
            (before, after)
        }
        "hierarchy.adjacency" => {
            let (parent_col, id_col) = sample
                .and_then(|sample| detect_hierarchy_signals(&sample.query).into_iter().next())
                .map(|signal| hierarchy_columns_from_signal(&signal))
                .unwrap_or_else(|| ("<parent_id>".to_string(), "<id>".to_string()));
            let before = format!(
                "SELECT ... FROM {} AS child JOIN {} AS parent ON child.{} = parent.{}",
                table_label, table_label, parent_col, id_col
            );
            let after = suggestion.skeinql_snippet.clone().unwrap_or_else(|| {
                let db = table_ref
                    .as_ref()
                    .map(|table| table.db.as_str())
                    .unwrap_or("<db>");
                let table = table_ref
                    .as_ref()
                    .map(|table| table.table.as_str())
                    .unwrap_or("<table>");
                format!(
                    "graph.traverse {{ db: \"{}\", table: \"{}\", edge: {{ parent: \"{}\", id: \"{}\" }} }}",
                    db, table, parent_col, id_col
                )
            });
            (before, after)
        }
        "hierarchy.recursive_cte" => {
            let mut cte_name = "<cte>".to_string();
            let mut parent_col = "<parent_id>".to_string();
            let mut id_col = "<id>".to_string();
            if let Some(sample) = sample {
                if let Some(signal) = detect_recursive_cte_signals(&sample.query)
                    .into_iter()
                    .next()
                {
                    cte_name = signal.cte_name.clone();
                    let (parent_hint, id_hint) = recursive_columns_from_signal(&signal);
                    parent_col = parent_hint;
                    id_col = id_hint;
                    if let Some(table) = signal.table.as_ref() {
                        table_label = format!("{}.{}", table.db, table.table);
                    }
                }
            }
            let cte_cols = format!("{}, {}", id_col, parent_col);
            let before = format!(
                "WITH RECURSIVE {} ({}) AS (\n  SELECT {}, {} FROM {} WHERE {} IS NULL\n  UNION ALL\n  SELECT child.{}, child.{} FROM {} AS child JOIN {} AS parent ON child.{} = parent.{}\n)\nSELECT ... FROM {}",
                cte_name,
                cte_cols,
                id_col,
                parent_col,
                table_label,
                parent_col,
                id_col,
                parent_col,
                table_label,
                cte_name,
                parent_col,
                id_col,
                cte_name
            );
            let after = suggestion.skeinql_snippet.clone().unwrap_or_else(|| {
                let db = table_ref
                    .as_ref()
                    .map(|table| table.db.as_str())
                    .unwrap_or("<db>");
                let table = table_ref
                    .as_ref()
                    .map(|table| table.table.as_str())
                    .unwrap_or("<table>");
                format!(
                    "roots = query.select {{ query: SELECT {} FROM {} WHERE {} IS NULL }}\npaths = graph.traverse {{ db: \"{}\", table: \"{}\", edge: {{ parent: \"{}\", id: \"{}\" }}, roots, max_depth: 10 }}",
                    id_col, table_label, parent_col, db, table, parent_col, id_col
                )
            });
            (before, after)
        }
        "exists.membership" => {
            let outer_label = sample
                .and_then(|sample| query_single_base_table(&sample.query))
                .map(|table| format!("{}.{}", table.db, table.table))
                .unwrap_or_else(|| "<outer_table>".to_string());
            let inner_label = evidence_table_ref(&suggestion.evidence)
                .map(|table| format!("{}.{}", table.db, table.table))
                .unwrap_or_else(|| "<inner_table>".to_string());
            let inner_column = column_hint.clone();
            let before = format!(
                "SELECT ... FROM {} WHERE EXISTS (SELECT 1 FROM {} WHERE {}.{} = {}.<outer_id>)",
                outer_label, inner_label, inner_label, inner_column, outer_label
            );
            let after = suggestion.skeinql_snippet.clone().unwrap_or_else(|| {
                format!(
                    "query.select {{ query: SELECT ... FROM {} JOIN {} ON {}.{} = {}.<outer_id> }}",
                    outer_label, inner_label, inner_label, inner_column, outer_label
                )
            });
            (before, after)
        }
        "defaults.coalesce" => {
            let mut column = column_hint.clone();
            let mut default_label = "<default>".to_string();
            if let Some(sample) = sample {
                if let Some(signal) = detect_coalesce_signals(&sample.query).into_iter().next() {
                    column = signal.column;
                    if let Some(lit) = signal.default_value {
                        default_label = render_lit_summary(&lit);
                    }
                }
            }
            let before = format!(
                "SELECT ..., COALESCE({}, {}) AS {} FROM {}",
                column, default_label, column, table_label
            );
            let after = suggestion.skeinql_snippet.clone().unwrap_or_else(|| {
                format!(
                    "view.create {{ name: \"defaults_<table>\", query: SELECT ..., coalesce({}, {}) AS {} FROM {} }}",
                    column, default_label, column, table_label
                )
            });
            (before, after)
        }
        _ => {
            let after = suggestion
                .skeinql_snippet
                .clone()
                .unwrap_or_else(|| "no rewrite available".to_string());
            ("unknown".to_string(), after)
        }
    }
}

pub(crate) fn migration_report_markdown(
    title: &str,
    generated_at_ms: u64,
    rewrites: &[MigrationRewritePreview],
) -> String {
    let mut out = vec![
        format!("# {}", title),
        String::new(),
        format!("Generated at ms: {}", generated_at_ms),
        String::new(),
    ];
    if rewrites.is_empty() {
        out.push("No migration rewrites were detected.".to_string());
        return out.join("\n");
    }

    for (idx, rewrite) in rewrites.iter().enumerate() {
        out.push(format!("## {}", rewrite.title));
        out.push(String::new());
        out.push(format!("- Intent: {}", rewrite.intent));
        out.push(format!(
            "- Confidence: {}%",
            (rewrite.confidence * 100.0).round()
        ));
        out.push(format!("- Evidence items: {}", rewrite.evidence.len()));
        out.push(String::new());
        out.push("Before:".to_string());
        out.push("```sql".to_string());
        out.push(rewrite.before.clone());
        out.push("```".to_string());
        out.push(String::new());
        out.push("After:".to_string());
        out.push("```text".to_string());
        out.push(rewrite.after.clone());
        out.push("```".to_string());
        if idx + 1 < rewrites.len() {
            out.push(String::new());
        }
    }

    out.join("\n")
}

pub(crate) fn evidence_table_ref(evidence: &[MigrationIntentEvidence]) -> Option<BaseTableRef> {
    evidence
        .iter()
        .find_map(|entry| entry.table.as_ref().cloned())
}

pub(crate) fn evidence_column_label(evidence: &[MigrationIntentEvidence]) -> Option<String> {
    evidence
        .iter()
        .find_map(|entry| entry.columns.first().cloned())
}

pub(crate) fn sample_from_evidence<'a>(
    samples: &'a [MigrationIntentSample],
    evidence: &[MigrationIntentEvidence],
) -> Option<&'a MigrationIntentSample> {
    let index = evidence.first()?.query_index as usize;
    samples.get(index)
}

pub(crate) fn hierarchy_columns_from_signal(signal: &HierarchySignal) -> (String, String) {
    let mut parent = signal.parent_col.clone();
    let mut id_col = signal.id_col.clone();
    if parent.is_none() || id_col.is_none() {
        for col in signal.columns.iter() {
            if parent.is_none() && is_parent_like_column(col) {
                parent = Some(col.clone());
            }
            if id_col.is_none() && is_id_like_column(col) {
                id_col = Some(col.clone());
            }
        }
    }
    if (parent.is_none() || id_col.is_none()) && signal.columns.len() >= 2 {
        if parent.is_none() {
            parent = Some(signal.columns[0].clone());
        }
        if id_col.is_none() {
            id_col = Some(signal.columns[1].clone());
        }
    }
    let parent = parent.unwrap_or_else(|| "<parent_id>".to_string());
    let id_col = id_col.unwrap_or_else(|| "<id>".to_string());
    (parent, id_col)
}

pub(crate) fn recursive_columns_from_signal(signal: &RecursiveCteSignal) -> (String, String) {
    let parent = signal
        .parent_col
        .clone()
        .unwrap_or_else(|| "<parent_id>".to_string());
    let id_col = signal.id_col.clone().unwrap_or_else(|| "<id>".to_string());
    (parent, id_col)
}

pub(crate) fn detect_pagination_signal(query: &Query) -> Option<PaginationSignal> {
    let limit = query.limit.as_ref()?.limit?;
    let offset = query.limit.as_ref()?.offset?;
    let order_col = order_by_cols(query).first().cloned();
    Some(PaginationSignal {
        table: query_single_base_table(query),
        order_col,
        limit: Some(limit),
        offset,
    })
}

pub(crate) fn detect_polling_signal(query: &Query, args: &[Lit]) -> Option<PollingSignal> {
    let QueryBody::Select { select } = query.body.as_ref() else {
        return None;
    };
    let predicate = select.r#where.as_ref()?;
    let mut comparisons = Vec::new();
    collect_comparisons(predicate, &mut comparisons);
    let order_cols = order_by_cols(query);
    let limit = query.limit.as_ref().and_then(|l| l.limit);
    let offset = query.limit.as_ref().and_then(|l| l.offset).unwrap_or(0);

    for comp in comparisons.into_iter() {
        if !matches!(comp.op.as_str(), "gt" | "ge") {
            continue;
        }
        let value = comparison_value_lit(&comp.value, args);
        let order_match = order_cols.iter().any(|col| col == &comp.col);
        if offset > 0 {
            continue;
        }
        return Some(PollingSignal {
            table: query_single_base_table(query),
            column: comp.col,
            value,
            order_match,
            limit,
        });
    }

    None
}

pub(crate) fn detect_soft_delete_signal(query: &Query, args: &[Lit]) -> Option<SoftDeleteSignal> {
    let QueryBody::Select { select } = query.body.as_ref() else {
        return None;
    };
    let predicate = select.r#where.as_ref()?;
    let mut comparisons = Vec::new();
    collect_comparisons(predicate, &mut comparisons);

    for comp in comparisons.into_iter() {
        if !is_soft_delete_column(&comp.col) {
            continue;
        }
        match comp.op.as_str() {
            "eq" => {
                let value = comparison_value_lit(&comp.value, args)?;
                if lit_is_false(&value) || matches!(value, Lit::Null) {
                    return Some(SoftDeleteSignal {
                        table: query_single_base_table(query),
                        column: comp.col,
                    });
                }
            }
            "is_null" => {
                return Some(SoftDeleteSignal {
                    table: query_single_base_table(query),
                    column: comp.col,
                });
            }
            _ => {}
        }
    }

    None
}

pub(crate) fn detect_hierarchy_signals(query: &Query) -> Vec<HierarchySignal> {
    let QueryBody::Select { select } = query.body.as_ref() else {
        return Vec::new();
    };
    let from = match select.from.as_ref() {
        Some(from) => from,
        None => return Vec::new(),
    };

    let mut alias_map = HashMap::new();
    for tref in from.iter() {
        if !collect_table_aliases(tref, &mut alias_map) {
            return Vec::new();
        }
    }

    let mut joins = Vec::new();
    for tref in from.iter() {
        collect_join_refs(tref, &mut joins);
    }

    let mut signals = Vec::new();
    for join in joins.into_iter() {
        let Some(left) = base_table_from_ref(&join.left) else {
            continue;
        };
        let Some(right) = base_table_from_ref(&join.right) else {
            continue;
        };
        if left.db != right.db || left.table != right.table {
            continue;
        }

        let Some(on_expr) = join.on.as_ref() else {
            continue;
        };
        let mut comparisons = Vec::new();
        collect_column_comparisons(on_expr, &mut comparisons);

        let mut best_parent = None;
        let mut best_id = None;
        let mut best_cols: Option<(String, String)> = None;
        for (left_ref, right_ref) in comparisons.into_iter() {
            let Some((left_alias, left_col)) = resolve_column_ref(&left_ref) else {
                continue;
            };
            let Some((right_alias, right_col)) = resolve_column_ref(&right_ref) else {
                continue;
            };
            if left_alias == right_alias {
                continue;
            }
            let forward = alias_map.get(&left_alias) == Some(&left)
                && alias_map.get(&right_alias) == Some(&right);
            let swapped = alias_map.get(&left_alias) == Some(&right)
                && alias_map.get(&right_alias) == Some(&left);
            if !forward && !swapped {
                continue;
            }
            best_cols.get_or_insert_with(|| (left_col.clone(), right_col.clone()));
            let (parent, id_col) = classify_hierarchy_columns(&left_col, &right_col);
            if parent.is_some() {
                best_parent = parent;
                best_id = id_col;
                break;
            }
            if best_id.is_none() && id_col.is_some() {
                best_id = id_col;
            }
        }

        let columns = best_cols
            .map(|(left_col, right_col)| vec![left_col, right_col])
            .unwrap_or_default();
        signals.push(HierarchySignal {
            table: left.clone(),
            columns,
            parent_col: best_parent,
            id_col: best_id,
        });
    }

    signals
}

pub(crate) fn detect_recursive_cte_signals(query: &Query) -> Vec<RecursiveCteSignal> {
    if query.with.is_empty() {
        return Vec::new();
    }
    let mut signals = Vec::new();
    for cte in query.with.iter() {
        if !query_body_references_table(cte.query.body.as_ref(), &cte.name) {
            continue;
        }
        let table = first_non_cte_table(cte.query.body.as_ref(), &cte.name);
        let (parent_col, id_col) = infer_hierarchy_columns_from_cte(&cte.query, &cte.name);
        signals.push(RecursiveCteSignal {
            cte_name: cte.name.clone(),
            table,
            parent_col,
            id_col,
        });
    }
    signals
}

pub(crate) fn detect_exists_signals(query: &Query) -> Vec<ExistsSignal> {
    let QueryBody::Select { select } = query.body.as_ref() else {
        return Vec::new();
    };
    let mut exists_exprs = Vec::new();
    if let Some(predicate) = select.r#where.as_ref() {
        collect_exists_exprs(predicate, &mut exists_exprs);
    }
    if let Some(predicate) = select.having.as_ref() {
        collect_exists_exprs(predicate, &mut exists_exprs);
    }

    let outer_table = query_single_base_table(query);
    exists_exprs
        .into_iter()
        .map(|exists| ExistsSignal {
            outer_table: outer_table.clone(),
            inner_table: query_single_base_table(&exists.query),
            inner_column: exists_inner_column(&exists.query),
        })
        .collect()
}

pub(crate) fn detect_coalesce_signals(query: &Query) -> Vec<CoalesceSignal> {
    let QueryBody::Select { select } = query.body.as_ref() else {
        return Vec::new();
    };
    let table = query_single_base_table(query);
    let mut signals = Vec::new();

    for item in select.projection.iter() {
        collect_coalesce_calls(&item.expr, &table, &mut signals);
    }
    if let Some(predicate) = select.r#where.as_ref() {
        collect_coalesce_calls(predicate, &table, &mut signals);
    }
    if let Some(predicate) = select.having.as_ref() {
        collect_coalesce_calls(predicate, &table, &mut signals);
    }
    for ob in query.order_by.iter() {
        collect_coalesce_calls(&ob.expr, &table, &mut signals);
    }

    signals
}

pub(crate) fn query_body_references_table(body: &QueryBody, table_name: &str) -> bool {
    let mut tables = Vec::new();
    collect_tables_recursive(body, &mut tables);
    tables.iter().any(|(_, table)| table == table_name)
}

pub(crate) fn collect_tables_recursive(body: &QueryBody, out: &mut Vec<(String, String)>) {
    match body {
        QueryBody::Select { select } => {
            if let Some(from) = select.from.as_ref() {
                for tref in from.iter() {
                    collect_tables_recursive_from_ref(tref, out);
                }
            }
        }
        QueryBody::Setop { setop } => {
            collect_tables_recursive(&setop.left, out);
            collect_tables_recursive(&setop.right, out);
        }
    }
}

pub(crate) fn collect_tables_recursive_from_ref(tref: &TableRef, out: &mut Vec<(String, String)>) {
    match tref {
        TableRef::Base(base) => out.push((base.db.clone(), base.table.clone())),
        TableRef::Join(join) => {
            collect_tables_recursive_from_ref(&join.join.left, out);
            collect_tables_recursive_from_ref(&join.join.right, out);
        }
        TableRef::Subquery(sub) => {
            collect_tables_recursive(&sub.subquery.query.body, out);
        }
    }
}

pub(crate) fn collect_table_aliases_from_body(
    body: &QueryBody,
    alias_map: &mut HashMap<String, BaseTableRef>,
) -> bool {
    match body {
        QueryBody::Select { select } => {
            let mut ok = true;
            if let Some(from) = select.from.as_ref() {
                for tref in from.iter() {
                    if !collect_table_aliases(tref, alias_map) {
                        ok = false;
                    }
                }
            }
            ok
        }
        QueryBody::Setop { setop } => {
            collect_table_aliases_from_body(&setop.left, alias_map)
                && collect_table_aliases_from_body(&setop.right, alias_map)
        }
    }
}

pub(crate) fn collect_column_comparisons_from_body(
    body: &QueryBody,
    out: &mut Vec<(ColumnRef, ColumnRef)>,
) {
    match body {
        QueryBody::Select { select } => {
            if let Some(from) = select.from.as_ref() {
                for tref in from.iter() {
                    collect_join_comparisons_from_ref(tref, out);
                }
            }
            if let Some(predicate) = select.r#where.as_ref() {
                collect_column_comparisons(predicate, out);
            }
            if let Some(predicate) = select.having.as_ref() {
                collect_column_comparisons(predicate, out);
            }
        }
        QueryBody::Setop { setop } => {
            collect_column_comparisons_from_body(&setop.left, out);
            collect_column_comparisons_from_body(&setop.right, out);
        }
    }
}

pub(crate) fn collect_join_comparisons_from_ref(
    tref: &TableRef,
    out: &mut Vec<(ColumnRef, ColumnRef)>,
) {
    match tref {
        TableRef::Join(join) => {
            if let Some(on) = join.join.on.as_ref() {
                collect_column_comparisons(on, out);
            }
            collect_join_comparisons_from_ref(&join.join.left, out);
            collect_join_comparisons_from_ref(&join.join.right, out);
        }
        TableRef::Subquery(sub) => {
            collect_column_comparisons_from_body(&sub.subquery.query.body, out);
        }
        TableRef::Base(_) => {}
    }
}

pub(crate) fn collect_base_tables_from_body(body: &QueryBody, out: &mut Vec<BaseTableRef>) {
    match body {
        QueryBody::Select { select } => {
            if let Some(from) = select.from.as_ref() {
                for tref in from.iter() {
                    collect_base_tables_from_ref(tref, out);
                }
            }
        }
        QueryBody::Setop { setop } => {
            collect_base_tables_from_body(&setop.left, out);
            collect_base_tables_from_body(&setop.right, out);
        }
    }
}

pub(crate) fn collect_base_tables_from_ref(tref: &TableRef, out: &mut Vec<BaseTableRef>) {
    match tref {
        TableRef::Base(base) => out.push(base.clone()),
        TableRef::Join(join) => {
            collect_base_tables_from_ref(&join.join.left, out);
            collect_base_tables_from_ref(&join.join.right, out);
        }
        TableRef::Subquery(sub) => {
            collect_base_tables_from_body(&sub.subquery.query.body, out);
        }
    }
}

pub(crate) fn first_non_cte_table(body: &QueryBody, cte_name: &str) -> Option<BaseTableRef> {
    let mut alias_map = HashMap::new();
    collect_table_aliases_from_body(body, &mut alias_map);
    if let Some((_, table)) = alias_map.iter().find(|(_, table)| table.table != cte_name) {
        return Some(table.clone());
    }

    let mut tables = Vec::new();
    collect_base_tables_from_body(body, &mut tables);
    tables.into_iter().find(|table| table.table != cte_name)
}

pub(crate) fn infer_hierarchy_columns_from_cte(
    query: &Query,
    cte_name: &str,
) -> (Option<String>, Option<String>) {
    let mut alias_map = HashMap::new();
    collect_table_aliases_from_body(query.body.as_ref(), &mut alias_map);
    let mut base_aliases = HashSet::new();
    for (alias, table) in alias_map.iter() {
        if table.table != cte_name {
            base_aliases.insert(alias.clone());
        }
    }

    let mut comparisons = Vec::new();
    collect_column_comparisons_from_body(query.body.as_ref(), &mut comparisons);

    let mut parent = None;
    let mut id_col = None;
    let consider = |col: &ColumnRef, parent: &mut Option<String>, id_col: &mut Option<String>| {
        if parent.is_none() && is_parent_like_column(&col.col) {
            *parent = Some(col.col.clone());
        }
        if id_col.is_none() && is_id_like_column(&col.col) {
            *id_col = Some(col.col.clone());
        }
    };

    if !base_aliases.is_empty() {
        for (left, right) in comparisons.iter() {
            if let Some(table) = left.table.as_ref() {
                if base_aliases.contains(table) {
                    consider(left, &mut parent, &mut id_col);
                }
            }
            if let Some(table) = right.table.as_ref() {
                if base_aliases.contains(table) {
                    consider(right, &mut parent, &mut id_col);
                }
            }
        }
    }

    if parent.is_none() || id_col.is_none() {
        for (left, right) in comparisons.iter() {
            consider(left, &mut parent, &mut id_col);
            consider(right, &mut parent, &mut id_col);
        }
    }

    (parent, id_col)
}

pub(crate) fn collect_comparisons(expr: &Expr, out: &mut Vec<Comparison>) {
    match expr {
        Expr::Op {
            op,
            a,
            b,
            args,
            list,
            lo,
            hi,
        } => match op.as_str() {
            "and" | "or" => {
                if let Some(items) = args.as_ref() {
                    for item in items.iter() {
                        collect_comparisons(item, out);
                    }
                } else {
                    if let Some(left) = a.as_deref() {
                        collect_comparisons(left, out);
                    }
                    if let Some(right) = b.as_deref() {
                        collect_comparisons(right, out);
                    }
                }
            }
            "eq" | "gt" | "ge" | "lt" | "le" => {
                if let (Some(left), Some(right)) = (a.as_deref(), b.as_deref()) {
                    if let Some((col, value)) = extract_col_value(left, right) {
                        out.push(Comparison {
                            col,
                            op: op.clone(),
                            value,
                        });
                    } else if let Some((col, value)) = extract_col_value(right, left) {
                        out.push(Comparison {
                            col,
                            op: op.clone(),
                            value,
                        });
                    }
                }
            }
            "between" => {
                if let (Some(expr), Some(lo), Some(hi)) =
                    (a.as_deref(), lo.as_deref(), hi.as_deref())
                {
                    if let Some((col, value)) = extract_col_value(expr, lo) {
                        out.push(Comparison {
                            col,
                            op: "ge".to_string(),
                            value,
                        });
                    }
                    if let Some((col, value)) = extract_col_value(expr, hi) {
                        out.push(Comparison {
                            col,
                            op: "le".to_string(),
                            value,
                        });
                    }
                }
            }
            "is_null" => {
                if let Some(Expr::Col { col, .. }) = a.as_deref() {
                    out.push(Comparison {
                        col: col.clone(),
                        op: op.clone(),
                        value: ComparisonValue::Lit(Lit::Null),
                    });
                }
            }
            _ => {
                if let Some(expr) = a.as_deref() {
                    collect_comparisons(expr, out);
                }
                if let Some(expr) = b.as_deref() {
                    collect_comparisons(expr, out);
                }
                if let Some(items) = args.as_ref() {
                    for item in items.iter() {
                        collect_comparisons(item, out);
                    }
                }
                if let Some(items) = list.as_ref() {
                    for item in items.iter() {
                        collect_comparisons(item, out);
                    }
                }
                if let Some(expr) = lo.as_deref() {
                    collect_comparisons(expr, out);
                }
                if let Some(expr) = hi.as_deref() {
                    collect_comparisons(expr, out);
                }
            }
        },
        Expr::Func { args, .. } => {
            for item in args.iter() {
                collect_comparisons(item, out);
            }
        }
        Expr::Cast { cast } => {
            collect_comparisons(&cast.expr, out);
        }
        Expr::Case { case_ } => {
            for when in case_.when.iter() {
                collect_comparisons(&when.r#if, out);
                collect_comparisons(&when.then, out);
            }
            if let Some(other) = case_.r#else.as_ref() {
                collect_comparisons(other, out);
            }
        }
        Expr::Subquery { .. }
        | Expr::Exists { .. }
        | Expr::Col { .. }
        | Expr::Lit { .. }
        | Expr::Param { .. } => {}
    }
}

pub(crate) fn collect_column_comparisons(expr: &Expr, out: &mut Vec<(ColumnRef, ColumnRef)>) {
    match expr {
        Expr::Op {
            op,
            a,
            b,
            args,
            list,
            lo,
            hi,
        } => match op.as_str() {
            "and" | "or" => {
                if let Some(items) = args.as_ref() {
                    for item in items.iter() {
                        collect_column_comparisons(item, out);
                    }
                } else {
                    if let Some(left) = a.as_deref() {
                        collect_column_comparisons(left, out);
                    }
                    if let Some(right) = b.as_deref() {
                        collect_column_comparisons(right, out);
                    }
                }
            }
            "eq" => {
                if let (Some(left), Some(right)) = (a.as_deref(), b.as_deref()) {
                    if let (Some(left_col), Some(right_col)) =
                        (column_ref_from_expr(left), column_ref_from_expr(right))
                    {
                        out.push((left_col, right_col));
                    }
                }
            }
            _ => {
                if let Some(expr) = a.as_deref() {
                    collect_column_comparisons(expr, out);
                }
                if let Some(expr) = b.as_deref() {
                    collect_column_comparisons(expr, out);
                }
                if let Some(items) = args.as_ref() {
                    for item in items.iter() {
                        collect_column_comparisons(item, out);
                    }
                }
                if let Some(items) = list.as_ref() {
                    for item in items.iter() {
                        collect_column_comparisons(item, out);
                    }
                }
                if let Some(expr) = lo.as_deref() {
                    collect_column_comparisons(expr, out);
                }
                if let Some(expr) = hi.as_deref() {
                    collect_column_comparisons(expr, out);
                }
            }
        },
        Expr::Func { args, .. } => {
            for item in args.iter() {
                collect_column_comparisons(item, out);
            }
        }
        Expr::Cast { cast } => {
            collect_column_comparisons(&cast.expr, out);
        }
        Expr::Case { case_ } => {
            for when in case_.when.iter() {
                collect_column_comparisons(&when.r#if, out);
                collect_column_comparisons(&when.then, out);
            }
            if let Some(other) = case_.r#else.as_ref() {
                collect_column_comparisons(other, out);
            }
        }
        Expr::Subquery { .. }
        | Expr::Exists { .. }
        | Expr::Col { .. }
        | Expr::Lit { .. }
        | Expr::Param { .. } => {}
    }
}

pub(crate) fn column_ref_from_expr(expr: &Expr) -> Option<ColumnRef> {
    match expr {
        Expr::Col { col, table } => Some(ColumnRef {
            table: table.clone(),
            col: col.clone(),
        }),
        _ => None,
    }
}

pub(crate) fn collect_join_refs(tref: &TableRef, out: &mut Vec<JoinRef>) {
    let TableRef::Join(join) = tref else {
        return;
    };
    out.push(join.join.clone());
    collect_join_refs(&join.join.left, out);
    collect_join_refs(&join.join.right, out);
}

pub(crate) fn base_table_from_ref(tref: &TableRef) -> Option<BaseTableRef> {
    match tref {
        TableRef::Base(base) => Some(base.clone()),
        _ => None,
    }
}

pub(crate) fn resolve_column_ref(col: &ColumnRef) -> Option<(String, String)> {
    let alias = col.table.clone()?;
    Some((alias, col.col.clone()))
}

pub(crate) fn classify_hierarchy_columns(
    left: &str,
    right: &str,
) -> (Option<String>, Option<String>) {
    if is_parent_like_column(left) && is_id_like_column(right) {
        return (Some(left.to_string()), Some(right.to_string()));
    }
    if is_parent_like_column(right) && is_id_like_column(left) {
        return (Some(right.to_string()), Some(left.to_string()));
    }
    if is_parent_like_column(left) {
        return (Some(left.to_string()), None);
    }
    if is_parent_like_column(right) {
        return (Some(right.to_string()), None);
    }
    if is_id_like_column(left) {
        return (None, Some(left.to_string()));
    }
    if is_id_like_column(right) {
        return (None, Some(right.to_string()));
    }
    (None, None)
}

pub(crate) fn collect_exists_exprs(expr: &Expr, out: &mut Vec<ExistsExpr>) {
    match expr {
        Expr::Exists { exists } => {
            out.push(exists.clone());
        }
        Expr::Op {
            a,
            b,
            args,
            list,
            lo,
            hi,
            ..
        } => {
            if let Some(left) = a.as_deref() {
                collect_exists_exprs(left, out);
            }
            if let Some(right) = b.as_deref() {
                collect_exists_exprs(right, out);
            }
            if let Some(items) = args.as_ref() {
                for item in items.iter() {
                    collect_exists_exprs(item, out);
                }
            }
            if let Some(items) = list.as_ref() {
                for item in items.iter() {
                    collect_exists_exprs(item, out);
                }
            }
            if let Some(expr) = lo.as_deref() {
                collect_exists_exprs(expr, out);
            }
            if let Some(expr) = hi.as_deref() {
                collect_exists_exprs(expr, out);
            }
        }
        Expr::Func { args, .. } => {
            for arg in args.iter() {
                collect_exists_exprs(arg, out);
            }
        }
        Expr::Cast { cast } => {
            collect_exists_exprs(&cast.expr, out);
        }
        Expr::Case { case_ } => {
            for when in case_.when.iter() {
                collect_exists_exprs(&when.r#if, out);
                collect_exists_exprs(&when.then, out);
            }
            if let Some(other) = case_.r#else.as_deref() {
                collect_exists_exprs(other, out);
            }
        }
        Expr::Subquery { .. } => {}
        Expr::Col { .. } | Expr::Lit { .. } | Expr::Param { .. } => {}
    }
}

pub(crate) fn collect_coalesce_calls(
    expr: &Expr,
    table: &Option<BaseTableRef>,
    out: &mut Vec<CoalesceSignal>,
) {
    match expr {
        Expr::Func { name, args, .. } => {
            if is_coalesce_name(name) {
                if let Some(signal) = coalesce_signal_from_args(args, table) {
                    out.push(signal);
                }
            }
            for arg in args.iter() {
                collect_coalesce_calls(arg, table, out);
            }
        }
        Expr::Op {
            a,
            b,
            args,
            list,
            lo,
            hi,
            ..
        } => {
            if let Some(left) = a.as_deref() {
                collect_coalesce_calls(left, table, out);
            }
            if let Some(right) = b.as_deref() {
                collect_coalesce_calls(right, table, out);
            }
            if let Some(items) = args.as_ref() {
                for item in items.iter() {
                    collect_coalesce_calls(item, table, out);
                }
            }
            if let Some(items) = list.as_ref() {
                for item in items.iter() {
                    collect_coalesce_calls(item, table, out);
                }
            }
            if let Some(expr) = lo.as_deref() {
                collect_coalesce_calls(expr, table, out);
            }
            if let Some(expr) = hi.as_deref() {
                collect_coalesce_calls(expr, table, out);
            }
        }
        Expr::Cast { cast } => {
            collect_coalesce_calls(&cast.expr, table, out);
        }
        Expr::Case { case_ } => {
            for when in case_.when.iter() {
                collect_coalesce_calls(&when.r#if, table, out);
                collect_coalesce_calls(&when.then, table, out);
            }
            if let Some(other) = case_.r#else.as_deref() {
                collect_coalesce_calls(other, table, out);
            }
        }
        Expr::Subquery { .. } | Expr::Exists { .. } => {}
        Expr::Col { .. } | Expr::Lit { .. } | Expr::Param { .. } => {}
    }
}

pub(crate) fn coalesce_signal_from_args(
    args: &[Expr],
    table: &Option<BaseTableRef>,
) -> Option<CoalesceSignal> {
    if args.len() < 2 {
        return None;
    }
    let column = match &args[0] {
        Expr::Col { col, .. } => col.clone(),
        _ => return None,
    };
    let default_value = match &args[1] {
        Expr::Lit { lit } => Some(lit.clone()),
        _ => None,
    };
    Some(CoalesceSignal {
        table: table.clone(),
        column,
        default_value,
        arg_count: args.len(),
    })
}

pub(crate) fn is_coalesce_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(lower.as_str(), "coalesce" | "ifnull" | "nvl")
}

pub(crate) fn exists_inner_column(query: &Query) -> Option<String> {
    let QueryBody::Select { select } = query.body.as_ref() else {
        return None;
    };
    if let Some(predicate) = select.r#where.as_ref() {
        if let Some(col) = first_column_in_expr(predicate) {
            return Some(col);
        }
    }
    if let Some(predicate) = select.having.as_ref() {
        if let Some(col) = first_column_in_expr(predicate) {
            return Some(col);
        }
    }
    for item in select.projection.iter() {
        if let Some(col) = first_column_in_expr(&item.expr) {
            return Some(col);
        }
    }
    None
}

pub(crate) fn first_column_in_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Col { col, .. } => Some(col.clone()),
        Expr::Op {
            a,
            b,
            args,
            list,
            lo,
            hi,
            ..
        } => {
            if let Some(left) = a.as_deref() {
                if let Some(col) = first_column_in_expr(left) {
                    return Some(col);
                }
            }
            if let Some(right) = b.as_deref() {
                if let Some(col) = first_column_in_expr(right) {
                    return Some(col);
                }
            }
            if let Some(items) = args.as_ref() {
                for item in items.iter() {
                    if let Some(col) = first_column_in_expr(item) {
                        return Some(col);
                    }
                }
            }
            if let Some(items) = list.as_ref() {
                for item in items.iter() {
                    if let Some(col) = first_column_in_expr(item) {
                        return Some(col);
                    }
                }
            }
            if let Some(expr) = lo.as_deref() {
                if let Some(col) = first_column_in_expr(expr) {
                    return Some(col);
                }
            }
            if let Some(expr) = hi.as_deref() {
                if let Some(col) = first_column_in_expr(expr) {
                    return Some(col);
                }
            }
            None
        }
        Expr::Func { args, .. } => {
            for arg in args.iter() {
                if let Some(col) = first_column_in_expr(arg) {
                    return Some(col);
                }
            }
            None
        }
        Expr::Cast { cast } => first_column_in_expr(&cast.expr),
        Expr::Case { case_ } => {
            for when in case_.when.iter() {
                if let Some(col) = first_column_in_expr(&when.r#if) {
                    return Some(col);
                }
                if let Some(col) = first_column_in_expr(&when.then) {
                    return Some(col);
                }
            }
            case_.r#else.as_deref().and_then(first_column_in_expr)
        }
        Expr::Subquery { .. } | Expr::Exists { .. } => None,
        Expr::Lit { .. } | Expr::Param { .. } => None,
    }
}

pub(crate) fn extract_col_value(left: &Expr, right: &Expr) -> Option<(String, ComparisonValue)> {
    let Expr::Col { col, .. } = left else {
        return None;
    };
    let value = match right {
        Expr::Lit { lit } => ComparisonValue::Lit(lit.clone()),
        Expr::Param { param } => ComparisonValue::Param(*param),
        _ => return None,
    };
    Some((col.clone(), value))
}

pub(crate) fn comparison_value_lit(value: &ComparisonValue, args: &[Lit]) -> Option<Lit> {
    match value {
        ComparisonValue::Lit(lit) => Some(lit.clone()),
        ComparisonValue::Param(param) => args.get(*param as usize).cloned(),
    }
}

pub(crate) fn order_by_cols(query: &Query) -> Vec<String> {
    let mut out = Vec::new();
    for ob in query.order_by.iter() {
        if let Expr::Col { col, .. } = &ob.expr {
            out.push(col.clone());
        }
    }
    out
}

pub(crate) fn query_single_base_table(query: &Query) -> Option<BaseTableRef> {
    let QueryBody::Select { select } = query.body.as_ref() else {
        return None;
    };
    let from = select.from.as_ref()?;
    if from.len() != 1 {
        return None;
    }
    match &from[0] {
        TableRef::Base(base) => Some(base.clone()),
        _ => None,
    }
}

pub(crate) fn is_soft_delete_column(column: &str) -> bool {
    let col = column.to_ascii_lowercase();
    matches!(
        col.as_str(),
        "deleted" | "is_deleted" | "deleted_at" | "deleted_on" | "deleted_at_ms" | "deleted_on_ms"
    ) || col.ends_with("_deleted")
        || col.ends_with("_deleted_at")
        || col.ends_with("_deleted_on")
        || col.ends_with("_deleted_at_ms")
        || col.ends_with("_deleted_on_ms")
}

pub(crate) fn lit_is_false(lit: &Lit) -> bool {
    matches!(lit, Lit::Bool { v: false })
        || matches!(lit, Lit::I64 { v: 0 })
        || matches!(lit, Lit::U64 { v: 0 })
        || matches!(lit, Lit::Str { v } if v == "false" || v == "0")
}

pub(crate) fn has_increasing_sequence(values: &[f64]) -> bool {
    if values.len() < 2 {
        return false;
    }
    let mut prev = values[0];
    for v in values.iter().skip(1) {
        if *v <= prev {
            return false;
        }
        prev = *v;
    }
    true
}

pub(crate) fn expr_const_value(expr: &Expr, args: &[Lit]) -> Option<Lit> {
    match expr {
        Expr::Lit { lit } => Some(lit.clone()),
        Expr::Param { param } => args.get(*param as usize).cloned(),
        _ => None,
    }
}

pub(crate) fn collect_index_eq_filters(
    expr: &Expr,
    single_alias: Option<&str>,
    alias_map: &HashMap<String, BaseTableRef>,
    args: &[Lit],
    out: &mut Vec<(String, Lit)>,
) -> bool {
    let Expr::Op {
        op,
        a,
        b,
        args: vargs,
        ..
    } = expr
    else {
        return true;
    };

    match op.as_str() {
        "and" => {
            if let Some(items) = vargs.as_ref() {
                for item in items.iter() {
                    if !collect_index_eq_filters(item, single_alias, alias_map, args, out) {
                        return false;
                    }
                }
            } else {
                if let Some(left) = a.as_deref() {
                    if !collect_index_eq_filters(left, single_alias, alias_map, args, out) {
                        return false;
                    }
                }
                if let Some(right) = b.as_deref() {
                    if !collect_index_eq_filters(right, single_alias, alias_map, args, out) {
                        return false;
                    }
                }
            }
            true
        }
        "or" | "not" => false,
        "eq" => {
            let left = a
                .as_deref()
                .and_then(|expr| expr_column_ref(expr, single_alias, alias_map));
            let right = b
                .as_deref()
                .and_then(|expr| expr_column_ref(expr, single_alias, alias_map));
            if let (Some((_alias, col)), Some(value)) = (
                left,
                b.as_deref().and_then(|expr| expr_const_value(expr, args)),
            ) {
                out.push((col, value));
            } else if let (Some((_alias, col)), Some(value)) = (
                right,
                a.as_deref().and_then(|expr| expr_const_value(expr, args)),
            ) {
                out.push((col, value));
            }
            true
        }
        _ => true,
    }
}
