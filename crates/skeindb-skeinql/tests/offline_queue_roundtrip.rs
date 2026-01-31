use skeindb_skeinql::methods::{MergeApplyResult, MergeFunctionRef, MergePolicySpec};
use skeindb_skeinql::offline::{
    OfflineWriteDecision, OfflineWriteItem, OfflineWriteOutcome, OfflineWriteQueue,
    OFFLINE_QUEUE_FORMAT_V1,
};
use skeindb_skeinql::types::{BaseTableRef, CausalityDependency, CausalityToken, Lit};

fn row(entries: &[(&str, Lit)]) -> std::collections::BTreeMap<String, Lit> {
    let mut out = std::collections::BTreeMap::new();
    for (k, v) in entries {
        out.insert((*k).to_string(), v.clone());
    }
    out
}

#[test]
fn offline_queue_roundtrip() {
    let queue = OfflineWriteQueue {
        format: OFFLINE_QUEUE_FORMAT_V1.to_string(),
        client_id: "client-1".to_string(),
        created_at_ms: 1234,
        min_causality: Some(CausalityToken {
            format: "etag_chain_v1".to_string(),
            deps: vec![CausalityDependency {
                table: "app.counters".to_string(),
                v: Some(7),
                view: None,
                change_seq: None,
                stale: None,
            }],
        }),
        items: vec![OfflineWriteItem {
            op_id: "op-1".to_string(),
            table: BaseTableRef {
                db: "app".to_string(),
                table: "counters".to_string(),
                r#as: None,
            },
            pk: vec![Lit::U64 { v: 1 }],
            incoming: row(&[("count", Lit::U64 { v: 2 })]),
            expected_etag: Some("W/\"r:abc\"".to_string()),
            min_causality: None,
            policy: Some(MergePolicySpec {
                default: MergeFunctionRef {
                    kind: "builtin".to_string(),
                    name: "sum".to_string(),
                    module: None,
                },
                per_column: std::collections::HashMap::new(),
            }),
            depends_on: vec!["op-0".to_string()],
        }],
    };

    let encoded = serde_json::to_string(&queue).unwrap();
    let decoded: OfflineWriteQueue = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.format, OFFLINE_QUEUE_FORMAT_V1);
    assert_eq!(decoded.items.len(), 1);
    assert_eq!(decoded.items[0].op_id, "op-1");
}

#[test]
fn offline_decision_from_merge_result() {
    let applied = MergeApplyResult {
        applied: true,
        conflict: false,
        merged: row(&[("count", Lit::U64 { v: 2 })]),
        etag: None,
        conflicts: Vec::new(),
    };
    let decision = OfflineWriteDecision::from_merge_result(&applied);
    assert_eq!(decision.outcome, OfflineWriteOutcome::Applied);

    let conflicted = MergeApplyResult {
        applied: true,
        conflict: true,
        merged: row(&[("count", Lit::U64 { v: 5 })]),
        etag: Some("W/\"r:abc\"".to_string()),
        conflicts: vec!["write_write".to_string()],
    };
    let decision = OfflineWriteDecision::from_merge_result(&conflicted);
    assert_eq!(decision.outcome, OfflineWriteOutcome::AppliedWithConflict);
    assert_eq!(decision.conflicts, vec!["write_write".to_string()]);

    let rejected = MergeApplyResult {
        applied: false,
        conflict: true,
        merged: row(&[("count", Lit::U64 { v: 5 })]),
        etag: None,
        conflicts: vec!["dependency".to_string()],
    };
    let decision = OfflineWriteDecision::from_merge_result(&rejected);
    assert_eq!(decision.outcome, OfflineWriteOutcome::Rejected);
}
