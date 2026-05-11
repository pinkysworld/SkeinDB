# Tamper-evident Audit Logging (Hash-chained WAL)

Status: Hardened runtime surface (prototype WAL log, test-backed proofs)
Last updated: 2026-05-11

Goal:
Provide a tamper-evident history of committed operations by hash-chaining the WAL.

A hash-chained WAL makes it possible to detect deletion, insertion, or modification of WAL records after the fact.

---

## 1) Record hash chain

Define:
- H(...) = cryptographic hash (e.g., BLAKE3-256 or SHA-256)
- rec_hash = H(prev_hash || lsn || txn_id || rec_type || payload_bytes)

Each WAL record stores:
- prev_hash[32]
- rec_hash[32]

The first record in a WAL segment uses a segment_start_hash stored in the WAL file header.

---

## 2) File header extension

WAL FileHeader (v0.2) adds:
- chain_start_hash[32]

This allows verification to begin at any segment boundary.

---

## 3) Checkpoints and anchoring

At checkpoint, write a manifest record:
- checkpoint_id
- last_lsn
- chain_head_hash

Optional: sign the chain_head_hash with an operator key.

---

## 4) Verification algorithm

Given a starting hash (from the first segment header or a checkpoint anchor):
- for each record in LSN order:
  - recompute rec_hash'
  - verify rec_hash' == stored rec_hash
  - verify stored prev_hash == previous rec_hash

If any check fails, the WAL history is not intact.

Complexity:
- O(n) records
- streaming (no random access required)

---

## 5) Retention policies

WALs are often truncated after checkpoint.
If audit retention is required:
- keep archived WAL segments in an audit/ directory
- do not delete segments unless the operator accepts breaking the chain

---

## 6) Runtime API surface

SkeinQL:
- maintenance.audit_verify
- maintenance.audit_status
- forensic.query / forensic.verify / forensic.export

CLI:
- skeindb audit-verify --data ./data

Runtime note:
- The current runtime stores a hash-chained record log in `forensic_chain.json`.
- Each entry links to the previous hash and records id, timestamp, db, table, op, primary key, change sequence, previous hash, and record hash metadata.
- `skeindb audit-verify` and `maintenance.audit_verify` verify the persisted chain, persist `last_verified_ms` on success, and return a non-zero CLI exit on mismatch.
- `forensic.query` returns filtered records plus a `skein.forensic.proof.v1` proof with boundary hashes, checkpoint anchor metadata, Merkle roots, inclusion proofs, and an index summary.
- `forensic.verify` verifies contiguous returned record slices from a supplied `start_hash`.
- `forensic.export` emits a `skein.forensic.bundle.v1` report bundle with the query manifest, records, proof, and verification summary.
- SkeinAdmin's Forensics panel exposes chain health, verification, filtered query, proof verification, and bundle export controls.

### 6.1 SkeinForensic JSON filter grammar

The minimal query grammar is JSON so it can be sent through SkeinQL without a separate parser:

```json
{
  "op": "and",
  "args": [
    {"op":"eq","a":{"col":"db"},"b":{"lit":{"t":"str","v":"app"}}},
    {"op":"eq","a":{"col":"table"},"b":{"lit":{"t":"str","v":"sessions"}}},
    {"op":"ge","a":{"col":"id"},"b":{"lit":{"t":"u64","v":3}}}
  ]
}
```

Supported operators:
- `and`, `or`, `not`
- `eq`, `ne`, `gt`, `ge`, `lt`, `le`
- `contains` for string containment or array membership

Supported columns:
- `id`, `ts_ms`, `db`, `schema`, `table`, `op`, `operation`, `change_seq`, `seq`, `pk`, `prev_hash`, `hash`

For short exact-match filters, an object without expression operands is treated as field equality:

```json
{"table":"sessions","op":"insert"}
```

### 6.2 Query and proof format

Example query:

```json
{
  "table": {"db":"app","table":"sessions"},
  "from_id": 3,
  "limit": 50,
  "filter": {"op":"eq","a":{"col":"op"},"b":{"lit":{"t":"str","v":"insert"}}}
}
```

The proof contains:
- `format`: `skein.forensic.proof.v1`
- `contiguous`: whether the returned records form a contiguous hash-chain slice
- `preceding_hash` / `following_hash`: boundary proof material for contiguous slices
- `checkpoint_anchor` / `next_checkpoint_anchor` / `anchor_count`: incremental verification anchors
- `chain_head`: current chain head hash
- `merkle_root` and `chain_merkle_root`: filtered result root and full-chain root
- `inclusion_proofs`: per-record Merkle sibling paths against the full-chain root
- `index_summary`: matched record counts by table, operation, actor bucket, id range, and timestamp range

For filtered non-contiguous result sets, the boundary chain cannot be verified as a standalone contiguous slice; use the Merkle inclusion proofs and full-chain root instead.

### 6.3 Export bundle format

`forensic.export` returns:

```json
{
  "bundle": {
    "format": "skein.forensic.bundle.v1",
    "bundle_id": "incident-17",
    "generated_at_ms": 1778500000000,
    "query": {"from_id":3,"limit":50},
    "records": [],
    "proof": {"format":"skein.forensic.proof.v1"},
    "verification": {"ok":true,"head_hash":"..."}
  }
}
```

When a filter produces a non-contiguous result set, `verification.strategy` is `merkle_inclusion_filtered` and the bundle carries generated inclusion proofs rather than a contiguous chain-slice verification result.

---

## 7) Threats and limitations

- Hash chaining detects tampering but does not prevent it.
- For non-repudiation, anchoring (signing) is required.
- If an attacker can both tamper and rewrite the signature anchor, the system is compromised.
- The current record log captures operation metadata, not full WAL payload bytes.
- Actor/user attribution is currently summarized as `unknown` until authenticated principal metadata is attached to forensic records.

---

## 8) Metrics

Expose:
- audit_chain_head_hash
- audit_last_verified_lsn
- audit_verify_time_ms
