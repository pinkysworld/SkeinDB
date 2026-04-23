# Dedup-preserving encryption (message-locked / convergent mode)

Status: Partial (T190 implemented; T191-T193 open)
Last updated: 2026-04-23

SkeinDB's storage design uses content addressing and optional deduplication in the ValueStore.
Traditional randomized encryption breaks deduplication because identical plaintexts produce different ciphertexts.

This document specifies an **optional** encryption mode that preserves deduplication by using
message-derived keys (message-locked / convergent encryption style). It also documents the
security tradeoffs and safe defaults.

## 1. Problem statement

- Goal A: encryption at rest for ValueStore objects.
- Goal B: preserve deduplication benefits for repeated values.

Naive deterministic encryption enables deduplication but can leak information (equality and,
in some cases, permit brute-force confirmation attacks on low-entropy values). Therefore this
feature MUST be opt-in and MUST offer safer scope defaults.

Current implementation note:

- T190 is now implemented in `skeindb-core::encryption` as a standalone database-scoped key manager plus AEAD wrapper layer.
- `ENC_RANDOM` uses AES-256-GCM-SIV with a randomized 96-bit nonce under a mode-specific key derived from the active database master secret.
- `ENC_MLE_DB` derives a deterministic content key from the active database master secret plus a SHA-256 digest of the plaintext, plus a separately HKDF-derived 96-bit nonce bound to the same (master_key, plaintext_digest) scope. Both the content key and the nonce are deterministic but content-dependent, so identical plaintexts within a database still converge to identical ciphertexts (preserving dedup) while no fixed or zero nonce is reused across plaintexts. The returned `EncryptionEnvelope` carries the derivation salt so later storage integration can decrypt the object without redesigning the wrapper contract.
- ValueStore metadata, encrypted on-disk entries, and `ValueID = hash(stored_bytes)` integration are still pending in T191, so this phase does not yet change any on-disk format.

## 2. Threat model (explicit)

This feature is designed primarily for:

- single-tenant deployments (one application / one trust domain),
- or multi-tenant deployments where dedup is scoped per tenant/database.

It is NOT designed to provide semantic security under chosen-plaintext attacks for predictable
messages while still enabling cross-tenant deduplication. Administrators must choose an
appropriate mode.

## 3. Modes

SkeinDB supports the following encryption modes:

### 3.1 ENC_OFF (default)

- ValueStore objects are stored in plaintext (existing behavior).

### 3.2 ENC_RANDOM (recommended for multi-tenant)

- Each ValueStore object is encrypted with a randomized nonce under a tenant key.
- Provides strong confidentiality but DOES NOT preserve dedup across independently encrypted
  copies.
- Dedup may still happen at the database level if the same ciphertext is reused (rare).
- Current T190 wrapper: AES-256-GCM-SIV with a mode-specific key derived from the active database master secret and a fresh 96-bit nonce per object.

### 3.3 ENC_MLE_DB (dedup-preserving within a database/tenant)

- Deterministic AEAD is used.
- A per-database (or per-tenant) master secret prevents cross-tenant confirmation attacks.
- Identical plaintexts within the same database produce identical ciphertexts, enabling dedup.
- Current T190 wrapper: derive a content key from `(database master secret, SHA-256(plaintext))` via HKDF, derive a separate 96-bit AEAD nonce from the same `(master_key, plaintext_digest)` via a distinct HKDF info label, then encrypt with AES-256-GCM-SIV. Both derivations are deterministic, so identical plaintexts within a database still produce identical ciphertexts; no fixed or zero nonce is reused. The wrapper returns the derivation salt in the envelope so decryption remains possible before T191 finalizes persistent metadata.

### 3.4 ENC_MLE_OPRF (server-aided, optional)

- A key server provides message-derived keys via an oblivious PRF (OPRF) protocol.
- Intended to mitigate brute-force attacks on predictable values in scenarios where a shared
  secret cannot be safely distributed.
- This mode is optional and requires additional deployment components.

## 4. Cryptographic construction (implementation guidance)

### 4.1 Hashing

- Define `m = plaintext_bytes`.
- Compute `h = SHA-256(m)`.

### 4.2 Key derivation

For ENC_MLE_DB:

- `K = HKDF(master_key, salt = h, info = "skeindb-mle", out_len = 32)`

For ENC_MLE_OPRF:

- `K = OPRF(master_key_server, input = h)` (protocol-specific)

### 4.3 Deterministic AEAD

Use a misuse-resistant or deterministic AEAD construction.
Recommended candidates:

- AES-SIV (RFC 5297) for deterministic AEAD.
- AES-GCM-SIV (RFC 8452) for nonce-misuse resistance.

Associated data SHOULD bind the ciphertext to context to prevent cut-and-paste:

- `aad = encode(db_id, table_id, column_id, value_kind, codec_version)`

Ciphertext payload:

- `ct = AEAD_Encrypt(K, nonce = fixed_or_derived, aad, m)`

For AES-SIV, the construction itself produces a synthetic IV; explicit nonces are not required.

### 4.4 ValueID computation

When encryption is enabled, the ValueID MUST be computed over the stored bytes to preserve
content addressing:

- `ValueID = hash(ct)`

Because encryption is deterministic in ENC_MLE_DB, equal plaintext within a database produces
equal ciphertext and thus equal ValueIDs.

## 5. Key management and rotation

- Each database has an active `key_id`.
- ValueStore entries store `(enc_mode, key_id)`.
- Rotation strategy:
  - new writes use new key_id
  - background rewrite can re-encrypt old objects, or keep mixed keys

Key storage options (deployment-dependent):
- OS keychain / DPAPI / Keychain / libsecret
- environment variable for development
- external KMS (future)

Current T190 surface:

- `DatabaseKeyManager::register_database_key(db_id, key_id, master_key)` registers a 32-byte database master secret.
- `DatabaseKeyManager::set_active_database_key(db_id, key_id)` switches the active key for future encryptions.
- `DatabaseKeyManager::set_database_mode(db_id, mode)` enables `ENC_OFF`, `ENC_RANDOM`, or `ENC_MLE_DB` per database profile.
- `DatabaseKeyManager::encrypt(...)` and `DatabaseKeyManager::decrypt(...)` operate over `EncryptionContext` + `EncryptionEnvelope` wrappers only; they do not yet mutate ValueStore persistence.

## 6. API surface

### 6.1 Settings

- `settings.set`: `encryption = { mode, scope, key_id, rotate_policy }`

### 6.2 Schema hints

Columns may opt in/out:

- `storage: { encrypted: true, dedup: "auto" }`

### 6.3 Observability

Expose:
- `encryption.mode`
- `encryption.objects_encrypted_total`
- `encryption.reencrypt_backlog_bytes`

## 7. Limitations and safety notes

- Deterministic encryption leaks equality. This is inherent to dedup-preserving encryption.
- Predictable values (e.g., "yes", "no", small integers) can be vulnerable to confirmation
  attacks if an attacker can guess plaintext and test equality.
- Therefore:
  - ENC_MLE_DB MUST be scoped to a secret per database/tenant.
  - ENC_RANDOM remains recommended where confidentiality dominates space savings.

## 8. Testing and evaluation

Functional tests:
- encrypt/decrypt round trip for each mode
- ValueID equality for identical values in ENC_MLE_DB
- key rotation correctness (mixed keys)

Shipped T190 coverage:

- `crates/skeindb-core/tests/encryption.rs::enc_random_roundtrip_uses_randomized_nonces`
- `crates/skeindb-core/tests/encryption.rs::enc_mle_db_roundtrip_is_deterministic_within_database_scope`
- `crates/skeindb-core/tests/encryption.rs::enc_mle_db_binds_context_and_database_key_scope`

Evaluation metrics:
- dedup ratio with/without encryption
- CPU overhead per write/read
- attack surface discussion (qualitative)


## 9. T191 — `EncryptedValueStore` wrapper

The `skeindb_core::encrypted_valuestore::EncryptedValueStore<'a>` type layers
encrypt-on-write / decrypt-on-read over an existing `ValueStore` **without
changing the on-disk `.vseg` format**. This is possible because every
`EncryptionEnvelope` is fully self-describing:

| Field           | Width       | Notes                                                      |
| --------------- | ----------- | ---------------------------------------------------------- |
| version         | u8          | currently `1`                                              |
| mode_code       | u8          | `0=Off`, `1=ENC_RANDOM`, `2=ENC_MLE_DB`                    |
| scope_id        | length-prefixed UTF-8 | database id (and AAD anchor)                     |
| key_id          | optional length-prefixed UTF-8 | active key id at write time             |
| nonce           | optional 12 bytes | only present for AEAD modes                          |
| derivation_salt | optional 32 bytes | only present for `ENC_MLE_DB` (plaintext digest)     |
| ciphertext      | remainder   | AEAD ciphertext (or raw plaintext for `Off`)               |

The wrapper stores the encoded envelope as an ordinary `ValueKind::Cell` blob
and reads it back using the strict `EncryptionEnvelope::from_stored_bytes`
parser (rejects trailing bytes and unknown version codes). `Off` values bypass
the envelope entirely and are stored as raw bytes — there is no overhead when
encryption is disabled.

API surface:

```rust
let mut store = ValueStore::new(ValueStoreConfig::default());
let mut keys  = DatabaseKeyManager::new();
keys.register_database_key("app", "k1", master_key)?;
keys.set_database_mode("app", EncryptionMode::EncMleDb)?;

let mut wrap = EncryptedValueStore::new(&mut store, &keys);
let rec = wrap.put_encrypted(&ctx, b"hello")?;          // EncryptedRecord { value_id, mode, scope_id, key_id }
let plain = wrap.get_decrypted(&ctx, &rec.value_id)?;   // back to bytes
let env = wrap.read_envelope(&rec.value_id)?;           // inspect (no decrypt)
```

## 10. T192 — Key rotation and re-encryption

`DatabaseKeyManager::rotate_active_key(db, new_key_id)` swaps the active key
for `db` and returns a `KeyRotationPlan`:

```rust
pub struct KeyRotationPlan {
    pub db: String,
    pub mode: EncryptionMode,
    pub previous_key_id: Option<String>,
    pub new_key_id: String,
}
```

To rewrite individual envelopes under the new active key, call
`reencrypt_envelope` (envelope-level) or `EncryptedValueStore::reencrypt_value`
(ValueStore-level). Both update a `ReencryptionProgress` counter set so an
operator can drive a long-running rotation pass in user code:

```rust
pub struct ReencryptionProgress {
    pub db: String,
    pub envelopes_inspected: u64,
    pub envelopes_rewritten: u64,
    pub envelopes_skipped_off: u64,
    pub envelopes_skipped_current: u64,
    pub previous_key_id: Option<String>,
    pub new_key_id: Option<String>,
}
```

Old envelopes are intentionally **left in place** so historical reads continue
to work under prior keys until a separate GC pass collects them.

## 11. T193 — `settings.encryption.*` RPC + SkeinAdmin panel

The engine exposes the runtime key-management surface via JSON-RPC. Master key
bytes are **never persisted to disk** by these methods — operators must
re-register keys after restart.

| Method                                   | Direction | Notes                                              |
| ---------------------------------------- | --------- | -------------------------------------------------- |
| `settings.encryption.status`             | read      | per-database mode, active key id, registered keys, recent audit ring |
| `settings.encryption.set_mode`           | write     | switch a database to `off` / `enc_random` / `enc_mle_db` |
| `settings.encryption.register_key`       | write     | accepts base64 (standard / URL-safe / no-pad), validates 32-byte length, optional `make_active` |
| `settings.encryption.set_active_key`     | write     | promote an already-registered key                  |
| `settings.encryption.rotate_key`         | write     | returns a `KeyRotationPlan`; envelope rewriting is opt-in via `EncryptedValueStore::reencrypt_value` |

SkeinAdmin gains an `Encryption` panel (sidebar + top tab) with cards for
Status, Set Mode, Register Key, Set Active Key, and Rotate Key. The panel uses
the same JSON-RPC client as every other admin tab.

Audit: every mutating call appends a redacted `EncryptionAuditEntry` (no key
material, ever) to a 256-entry in-memory ring exposed via
`settings.encryption.status` → `recent_audit`.

Coverage: `skeinadmin_encryption_panel_exposes_key_management_controls` plus
the unit tests listed in section 8.
