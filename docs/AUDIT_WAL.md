# Tamper-evident Audit Logging (Hash-chained WAL)

Status: Draft
Last updated: 2026-01-19

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

## 6) API surface

SkeinQL:
- maintenance.audit_verify { from_checkpoint_id?, from_wal_segment? }
- maintenance.audit_status
- forensic.query / forensic.verify / forensic.export (prototype)

CLI:
- skeindb audit verify --data ./data --from-checkpoint <id>

Prototype note:
- The current scaffold stores a hash-chained record log in `forensic_chain.json`.
- Each entry links to the previous hash and records db/table/op/pk metadata.
- This is a stand-in for the WAL chain until the real WAL is implemented.

---

## 7) Threats and limitations

- Hash chaining detects tampering but does not prevent it.
- For non-repudiation, anchoring (signing) is required.
- If an attacker can both tamper and rewrite the signature anchor, the system is compromised.

---

## 8) Metrics

Expose:
- audit_chain_head_hash
- audit_last_verified_lsn
- audit_verify_time_ms
