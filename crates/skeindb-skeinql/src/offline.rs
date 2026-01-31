//! Offline write queue format (experimental).
//!
//! This is a client-side schema for batching merge.apply-compatible writes
//! while offline, then replaying them when connectivity returns.

use serde::{Deserialize, Serialize};

use crate::methods::{MergeApplyResult, MergePolicySpec, RowObject};
use crate::types::{BaseTableRef, CausalityToken, Lit};

pub const OFFLINE_QUEUE_FORMAT_V1: &str = "offline_queue_v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineWriteQueue {
    pub format: String,
    pub client_id: String,
    pub created_at_ms: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_causality: Option<CausalityToken>,

    pub items: Vec<OfflineWriteItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineWriteItem {
    pub op_id: String,
    pub table: BaseTableRef,
    pub pk: Vec<Lit>,
    pub incoming: RowObject,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_etag: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_causality: Option<CausalityToken>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<MergePolicySpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OfflineWriteOutcome {
    Applied,
    AppliedWithConflict,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfflineWriteDecision {
    pub outcome: OfflineWriteOutcome,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,
}

impl OfflineWriteDecision {
    pub fn from_merge_result(result: &MergeApplyResult) -> Self {
        let mut conflicts = result.conflicts.clone();
        if result.conflict && conflicts.is_empty() {
            conflicts.push("unknown".to_string());
        }

        let outcome = if result.applied {
            if result.conflict {
                OfflineWriteOutcome::AppliedWithConflict
            } else {
                OfflineWriteOutcome::Applied
            }
        } else {
            OfflineWriteOutcome::Rejected
        };

        Self { outcome, conflicts }
    }
}
