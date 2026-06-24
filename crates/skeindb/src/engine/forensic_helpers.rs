//! Forensic audit-chain helpers (R06): genesis/record hashing, Merkle root and
//! inclusion proofs, and forensic-query filter evaluation. Pure free functions over
//! `ForensicRecord` slices with no engine state. Extracted verbatim from engine.rs as
//! the first slice of the engine monolith split (see CONTRIBUTING.md > Working style).

use super::*;

// -----------------------------
// Forensic helpers
// -----------------------------

pub(crate) fn forensic_genesis_hash() -> String {
    "genesis".to_string()
}

pub(crate) fn forensic_index_by_id(records: &[ForensicRecord], id: u64) -> Option<usize> {
    records.iter().position(|r| r.id == id)
}

pub(crate) fn forensic_filter_matches(
    rec: &ForensicRecord,
    filter: &serde_json::Value,
) -> anyhow::Result<bool> {
    if filter.is_null() {
        return Ok(true);
    }
    let Some(obj) = filter.as_object() else {
        anyhow::bail!("invalid_request: forensic filter must be a JSON object");
    };
    let looks_like_expr = obj
        .get("op")
        .and_then(|v| v.as_str())
        .map(|op| forensic_filter_operator(op).is_some())
        .unwrap_or(false)
        && (obj.contains_key("a") || obj.contains_key("b") || obj.contains_key("args"));

    if !looks_like_expr {
        for (field, expected) in obj {
            let actual = forensic_filter_field_value(rec, field)?;
            let expected = forensic_filter_literal_value(expected)?;
            if actual != expected {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    let op = obj
        .get("op")
        .and_then(|v| v.as_str())
        .and_then(forensic_filter_operator)
        .ok_or_else(|| anyhow::anyhow!("invalid_request: unknown forensic filter operator"))?;
    match op {
        "and" => {
            for arg in forensic_filter_args(obj)? {
                if !forensic_filter_matches(rec, arg)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        "or" => {
            for arg in forensic_filter_args(obj)? {
                if forensic_filter_matches(rec, arg)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "not" => {
            let arg = obj.get("a").or_else(|| obj.get("arg")).ok_or_else(|| {
                anyhow::anyhow!("invalid_request: not filter requires an operand")
            })?;
            Ok(!forensic_filter_matches(rec, arg)?)
        }
        "eq" | "ne" | "gt" | "ge" | "lt" | "le" | "contains" => {
            let left = forensic_filter_operand_value(rec, obj.get("a"))?;
            let right = forensic_filter_operand_value(rec, obj.get("b"))?;
            let matched = match op {
                "eq" => left == right,
                "ne" => left != right,
                "contains" => forensic_value_contains(&left, &right),
                "gt" | "ge" | "lt" | "le" => forensic_compare_values(&left, &right, op)?,
                _ => false,
            };
            Ok(matched)
        }
        _ => anyhow::bail!("invalid_request: unknown forensic filter operator"),
    }
}

pub(crate) fn forensic_filter_operator(op: &str) -> Option<&'static str> {
    match op.to_ascii_lowercase().as_str() {
        "and" => Some("and"),
        "or" => Some("or"),
        "not" => Some("not"),
        "eq" | "=" => Some("eq"),
        "ne" | "!=" | "<>" => Some("ne"),
        "gt" | ">" => Some("gt"),
        "ge" | ">=" => Some("ge"),
        "lt" | "<" => Some("lt"),
        "le" | "<=" => Some("le"),
        "contains" => Some("contains"),
        _ => None,
    }
}

pub(crate) fn forensic_filter_args(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Vec<&serde_json::Value>> {
    if let Some(args) = obj.get("args").and_then(|v| v.as_array()) {
        return Ok(args.iter().collect());
    }
    let mut args = Vec::new();
    if let Some(a) = obj.get("a") {
        args.push(a);
    }
    if let Some(b) = obj.get("b") {
        args.push(b);
    }
    if args.is_empty() {
        anyhow::bail!("invalid_request: forensic boolean filter requires args");
    }
    Ok(args)
}

pub(crate) fn forensic_filter_operand_value(
    rec: &ForensicRecord,
    operand: Option<&serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let operand = operand
        .ok_or_else(|| anyhow::anyhow!("invalid_request: forensic filter missing operand"))?;
    if let Some(obj) = operand.as_object() {
        if let Some(col) = obj.get("col").and_then(|v| v.as_str()) {
            return forensic_filter_field_value(rec, col);
        }
        if let Some(lit) = obj.get("lit") {
            return forensic_filter_literal_value(lit);
        }
        if let Some(value) = obj.get("value") {
            return forensic_filter_literal_value(value);
        }
    }
    forensic_filter_literal_value(operand)
}

pub(crate) fn forensic_filter_literal_value(
    value: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    if value.get("t").is_some() {
        let lit: Lit = serde_json::from_value(value.clone())?;
        return Ok(forensic_lit_plain_value(&lit));
    }
    Ok(value.clone())
}

pub(crate) fn forensic_filter_field_value(
    rec: &ForensicRecord,
    field: &str,
) -> anyhow::Result<serde_json::Value> {
    match field.to_ascii_lowercase().as_str() {
        "id" => Ok(serde_json::json!(rec.id)),
        "ts" | "ts_ms" | "time" => Ok(serde_json::json!(rec.ts_ms)),
        "db" | "schema" => Ok(serde_json::json!(rec.db)),
        "table" => Ok(serde_json::json!(rec.table)),
        "op" | "operation" => Ok(serde_json::json!(rec.op)),
        "change_seq" | "seq" => Ok(serde_json::json!(rec.change_seq)),
        "prev_hash" => Ok(serde_json::json!(rec.prev_hash)),
        "hash" => Ok(serde_json::json!(rec.hash)),
        "pk" => serde_json::to_value(&rec.pk).map_err(|e| anyhow::anyhow!(e)),
        other => anyhow::bail!("invalid_request: unknown forensic filter field {other}"),
    }
}

pub(crate) fn forensic_lit_plain_value(lit: &Lit) -> serde_json::Value {
    match lit {
        Lit::Null => serde_json::Value::Null,
        Lit::Bool { v } => serde_json::json!(v),
        Lit::I64 { v } => serde_json::json!(v),
        Lit::U64 { v } => serde_json::json!(v),
        Lit::F64 { v } => serde_json::json!(v),
        Lit::Dec { v } | Lit::Str { v } | Lit::Uuid { v } => serde_json::json!(v),
        Lit::Bytes { b64 } => serde_json::json!(b64),
        Lit::Json { v } => v.clone(),
        Lit::Embedding { dims, v, model } => serde_json::json!({
            "dims": dims,
            "v": v,
            "model": model,
        }),
        Lit::Date { iso } | Lit::Time { iso } | Lit::Datetime { iso } => serde_json::json!(iso),
    }
}

pub(crate) fn forensic_value_contains(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::String(left), serde_json::Value::String(right)) => left.contains(right),
        (serde_json::Value::Array(items), needle) => items.iter().any(|item| item == needle),
        _ => false,
    }
}

pub(crate) fn forensic_compare_values(
    left: &serde_json::Value,
    right: &serde_json::Value,
    op: &str,
) -> anyhow::Result<bool> {
    if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
        return Ok(match op {
            "gt" => left > right,
            "ge" => left >= right,
            "lt" => left < right,
            "le" => left <= right,
            _ => false,
        });
    }
    let (Some(left), Some(right)) = (left.as_str(), right.as_str()) else {
        anyhow::bail!("invalid_request: forensic comparison requires numeric or string operands");
    };
    Ok(match op {
        "gt" => left > right,
        "ge" => left >= right,
        "lt" => left < right,
        "le" => left <= right,
        _ => false,
    })
}

pub(crate) fn forensic_inclusion_proofs(
    chain: &[ForensicRecord],
    records: &[ForensicRecord],
) -> Vec<serde_json::Value> {
    records
        .iter()
        .filter_map(|rec| {
            let chain_index = forensic_index_by_id(chain, rec.id)?;
            let siblings = forensic_merkle_proof(chain, chain_index)
                .into_iter()
                .map(|(hash, sibling_is_right)| {
                    serde_json::json!({
                        "hash": hash,
                        "sibling_side": if sibling_is_right { "right" } else { "left" },
                    })
                })
                .collect::<Vec<_>>();
            Some(serde_json::json!({
                "record_id": rec.id,
                "chain_index": chain_index,
                "record_hash": rec.hash,
                "siblings": siblings,
            }))
        })
        .collect()
}

pub(crate) fn forensic_index_summary(
    chain: &[ForensicRecord],
    records: &[ForensicRecord],
) -> serde_json::Value {
    let mut by_table: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_op: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_actor: BTreeMap<String, u64> = BTreeMap::new();
    let mut first_id = None;
    let mut last_id = None;
    let mut min_ts_ms = None;
    let mut max_ts_ms = None;

    for rec in records {
        *by_table
            .entry(format!("{}.{}", rec.db, rec.table))
            .or_default() += 1;
        *by_op.entry(rec.op.clone()).or_default() += 1;
        *by_actor.entry("unknown".to_string()).or_default() += 1;
        first_id = Some(first_id.map_or(rec.id, |v: u64| v.min(rec.id)));
        last_id = Some(last_id.map_or(rec.id, |v: u64| v.max(rec.id)));
        min_ts_ms = Some(min_ts_ms.map_or(rec.ts_ms, |v: u64| v.min(rec.ts_ms)));
        max_ts_ms = Some(max_ts_ms.map_or(rec.ts_ms, |v: u64| v.max(rec.ts_ms)));
    }

    serde_json::json!({
        "format": "skein.forensic.index.v1",
        "chain_records": chain.len(),
        "matched_records": records.len(),
        "first_id": first_id,
        "last_id": last_id,
        "min_ts_ms": min_ts_ms,
        "max_ts_ms": max_ts_ms,
        "by_table": by_table,
        "by_op": by_op,
        "by_actor": by_actor,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn forensic_record_hash(
    prev_hash: &str,
    id: u64,
    ts_ms: u64,
    db: &str,
    table: &str,
    op: &str,
    pk: Option<&Vec<Lit>>,
    change_seq: u64,
) -> String {
    let payload = serde_json::json!({
        "prev_hash": prev_hash,
        "id": id,
        "ts_ms": ts_ms,
        "db": db,
        "table": table,
        "op": op,
        "pk": pk,
        "change_seq": change_seq
    });
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let hash = audit_hash256(&bytes);
    hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
}

/// Compute a Merkle tree root over a sequence of forensic records.
/// The tree is constructed as a binary hash tree over the record hashes.
/// This provides O(log n) proof of inclusion for any record in the chain.
pub(crate) fn forensic_merkle_root(records: &[ForensicRecord]) -> Option<String> {
    if records.is_empty() {
        return None;
    }
    let mut hashes: Vec<String> = records.iter().map(|r| r.hash.clone()).collect();
    // Iteratively combine pairs until we reach a single root.
    while hashes.len() > 1 {
        let mut next = Vec::new();
        let mut i = 0;
        while i < hashes.len() {
            if i + 1 < hashes.len() {
                let combined = format!("{}:{}", hashes[i], hashes[i + 1]);
                let id = value_id(combined.as_bytes());
                next.push(hex16(&id));
                i += 2;
            } else {
                // Odd element: promote unchanged.
                next.push(hashes[i].clone());
                i += 1;
            }
        }
        hashes = next;
    }
    hashes.into_iter().next()
}

/// Generate a Merkle inclusion proof for a specific record index.
/// Returns the sibling hashes needed to reconstruct the root.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn forensic_merkle_proof(
    records: &[ForensicRecord],
    target_idx: usize,
) -> Vec<(String, bool)> {
    if records.is_empty() || target_idx >= records.len() {
        return Vec::new();
    }
    let mut hashes: Vec<String> = records.iter().map(|r| r.hash.clone()).collect();
    let mut proof = Vec::new();
    let mut idx = target_idx;

    while hashes.len() > 1 {
        let mut next = Vec::new();
        let mut i = 0;
        let mut new_idx = 0;
        while i < hashes.len() {
            if i + 1 < hashes.len() {
                if i == idx {
                    // Target is left child; sibling is right.
                    proof.push((hashes[i + 1].clone(), true));
                    new_idx = next.len();
                } else if i + 1 == idx {
                    // Target is right child; sibling is left.
                    proof.push((hashes[i].clone(), false));
                    new_idx = next.len();
                }
                let combined = format!("{}:{}", hashes[i], hashes[i + 1]);
                let id = value_id(combined.as_bytes());
                next.push(hex16(&id));
                i += 2;
            } else {
                if i == idx {
                    new_idx = next.len();
                }
                next.push(hashes[i].clone());
                i += 1;
            }
        }
        hashes = next;
        idx = new_idx;
    }
    proof
}
