# Dedup-preserving encryption (message-locked / convergent mode)

Status: Draft
Last updated: 2026-01-17

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

### 3.3 ENC_MLE_DB (dedup-preserving within a database/tenant)

- Deterministic AEAD is used.
- A per-database (or per-tenant) master secret prevents cross-tenant confirmation attacks.
- Identical plaintexts within the same database produce identical ciphertexts, enabling dedup.

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

Evaluation metrics:
- dedup ratio with/without encryption
- CPU overhead per write/read
- attack surface discussion (qualitative)
