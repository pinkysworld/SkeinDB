//! Encrypted [`ValueStore`] wrapper (T191 baseline).
//!
//! Stores encrypted plaintexts as self-describing [`EncryptionEnvelope::stored_bytes`]
//! payloads inside the existing on-disk `ValueStore`/`.vseg` format. The envelope itself
//! carries the version, mode, scope, key id, nonce, and derivation salt, so a value can be
//! decrypted from disk without any sidecar metadata as long as the database key is
//! available in the [`DatabaseKeyManager`].
//!
//! No on-disk format change: this module uses [`ValueStore::put`] and [`ValueStore::get`]
//! as-is and stores encrypted blobs under [`ValueKind::Cell`].

use thiserror::Error;

use crate::encryption::{
    DatabaseKeyManager, EncryptionContext, EncryptionEnvelope, EncryptionError, EncryptionMode,
    KeyRotationPlan, ReencryptionProgress,
};
use crate::valuestore::{ValueId, ValueStore};
use crate::ValueKind;

/// Result of a successful [`EncryptedValueStore::put_encrypted`] call. Captures both the
/// content-addressed [`ValueId`] of the stored envelope and the metadata needed by
/// callers/operators to reason about which key wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedRecord {
    pub value_id: ValueId,
    pub mode: EncryptionMode,
    pub scope_id: String,
    pub key_id: Option<String>,
}

/// Errors specific to the encrypted value store wrapper.
#[derive(Debug, Error)]
pub enum EncryptedValueStoreError {
    #[error("value not found: {0:?}")]
    Missing(ValueId),
    #[error("value at {value_id:?} is not stored under ValueKind::Cell (got {kind:?})")]
    UnexpectedKind { value_id: ValueId, kind: ValueKind },
    #[error("encryption error: {0}")]
    Encryption(#[from] EncryptionError),
}

/// In-memory wrapper that combines a [`ValueStore`] with a [`DatabaseKeyManager`] and
/// exposes encrypt-on-write / decrypt-on-read semantics. The wrapper borrows both, so the
/// caller controls the lifetime and persistence of the underlying ValueStore.
pub struct EncryptedValueStore<'a> {
    inner: &'a mut ValueStore,
    keys: &'a DatabaseKeyManager,
}

impl<'a> EncryptedValueStore<'a> {
    pub fn new(inner: &'a mut ValueStore, keys: &'a DatabaseKeyManager) -> Self {
        Self { inner, keys }
    }

    /// Encrypt `plaintext` under the database's active key and store the resulting envelope
    /// as a [`ValueKind::Cell`] entry. The returned [`EncryptedRecord`] contains the
    /// content-addressed [`ValueId`] plus the envelope metadata.
    pub fn put_encrypted(
        &mut self,
        ctx: &EncryptionContext,
        plaintext: &[u8],
    ) -> Result<EncryptedRecord, EncryptedValueStoreError> {
        let envelope = self.keys.encrypt(ctx, plaintext)?;
        let record = EncryptedRecord {
            value_id: [0u8; 16],
            mode: envelope.mode,
            scope_id: envelope.scope_id.clone(),
            key_id: envelope.key_id.clone(),
        };
        let bytes = envelope.stored_bytes();
        let value_id = self.inner.put(ValueKind::Cell, bytes);
        Ok(EncryptedRecord { value_id, ..record })
    }

    /// Read the stored envelope at `value_id` and decrypt it under `ctx`.
    pub fn get_decrypted(
        &mut self,
        ctx: &EncryptionContext,
        value_id: &ValueId,
    ) -> Result<Vec<u8>, EncryptedValueStoreError> {
        let envelope = self.read_envelope(value_id)?;
        let plain = self.keys.decrypt(ctx, &envelope)?;
        Ok(plain)
    }

    /// Read the raw envelope at `value_id` without decrypting it. Useful for re-encryption
    /// passes that need to inspect the envelope's `key_id` / `mode` before deciding whether
    /// to rewrite it.
    pub fn read_envelope(
        &mut self,
        value_id: &ValueId,
    ) -> Result<EncryptionEnvelope, EncryptedValueStoreError> {
        let entry = self
            .inner
            .get(value_id)
            .ok_or(EncryptedValueStoreError::Missing(*value_id))?;
        if entry.kind != ValueKind::Cell {
            return Err(EncryptedValueStoreError::UnexpectedKind {
                value_id: *value_id,
                kind: entry.kind,
            });
        }
        Ok(EncryptionEnvelope::from_stored_bytes(&entry.bytes)?)
    }

    /// Re-encrypt the envelope at `value_id` so it uses the database's currently-active
    /// key. Returns the new [`ValueId`] when the envelope is rewritten, or `Ok(None)` when
    /// the envelope is already current (or has mode `ENC_OFF`). Updates `progress` either
    /// way. The old envelope is *not* removed so historical references remain valid.
    pub fn reencrypt_value(
        &mut self,
        ctx: &EncryptionContext,
        value_id: &ValueId,
        progress: &mut ReencryptionProgress,
    ) -> Result<Option<ValueId>, EncryptedValueStoreError> {
        let envelope = self.read_envelope(value_id)?;
        let new_envelope = self.keys.reencrypt_envelope(ctx, &envelope, progress)?;
        match new_envelope {
            Some(env) => {
                let new_id = self.inner.put(ValueKind::Cell, env.stored_bytes());
                Ok(Some(new_id))
            }
            None => Ok(None),
        }
    }
}

/// Convenience wrapper around [`DatabaseKeyManager::rotate_active_key`] that re-exports the
/// rotation plan type so callers only need to import this module.
pub fn rotate_active_key(
    keys: &mut DatabaseKeyManager,
    db_id: &str,
    new_key_id: &str,
) -> Result<KeyRotationPlan, EncryptionError> {
    keys.rotate_active_key(db_id, new_key_id)
}
