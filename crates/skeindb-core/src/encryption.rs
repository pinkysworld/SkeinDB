use std::collections::HashMap;

use aes_gcm_siv::{
    aead::{Aead, KeyInit, Payload},
    Aes256GcmSiv,
};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{decode_varu, encode_varu, ValueKind};

pub const ENCRYPTION_ENVELOPE_VERSION: u8 = 1;
pub const ENCRYPTION_MASTER_KEY_LEN: usize = 32;
pub const ENCRYPTION_NONCE_LEN: usize = 12;
pub const ENCRYPTION_DERIVATION_SALT_LEN: usize = 32;

const ENC_RANDOM_INFO: &[u8] = b"skeindb-enc-random";
const ENC_MLE_DB_INFO: &[u8] = b"skeindb-enc-mle-db";
const ENC_MLE_DB_NONCE_INFO: &[u8] = b"skeindb-enc-mle-db-nonce";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EncryptionMode {
    #[default]
    #[serde(rename = "ENC_OFF")]
    Off,
    #[serde(rename = "ENC_RANDOM")]
    EncRandom,
    #[serde(rename = "ENC_MLE_DB")]
    EncMleDb,
}

impl EncryptionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "ENC_OFF",
            Self::EncRandom => "ENC_RANDOM",
            Self::EncMleDb => "ENC_MLE_DB",
        }
    }

    /// Parse a mode label. Accepts both upper- and lower-case forms used in
    /// configuration files and RPC parameters (`off`, `enc_off`, `enc_random`,
    /// `enc_mle_db`).
    pub fn parse(label: &str) -> Option<Self> {
        match label.trim().to_ascii_lowercase().as_str() {
            "off" | "enc_off" => Some(Self::Off),
            "enc_random" | "random" => Some(Self::EncRandom),
            "enc_mle_db" | "mle_db" | "mle" => Some(Self::EncMleDb),
            _ => None,
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Off => 0,
            Self::EncRandom => 1,
            Self::EncMleDb => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseEncryptionProfile {
    pub db_id: String,
    pub mode: EncryptionMode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_key_id: Option<String>,
}

impl DatabaseEncryptionProfile {
    fn new(db_id: String) -> Self {
        Self {
            db_id,
            mode: EncryptionMode::Off,
            active_key_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionContext {
    pub db_id: String,
    pub table_id: String,
    pub column_id: String,
    pub value_kind: ValueKind,
    pub codec_version: u32,
}

impl EncryptionContext {
    pub fn new(
        db_id: impl Into<String>,
        table_id: impl Into<String>,
        column_id: impl Into<String>,
        value_kind: ValueKind,
        codec_version: u32,
    ) -> Self {
        Self {
            db_id: db_id.into(),
            table_id: table_id.into(),
            column_id: column_id.into(),
            value_kind,
            codec_version,
        }
    }

    pub fn aad_bytes(&self) -> Result<Vec<u8>, EncryptionError> {
        let db_id = normalize_required(&self.db_id, "db_id")?;
        let table_id = self.table_id.trim();
        let column_id = self.column_id.trim();

        let mut out = Vec::new();
        push_len_prefixed(&mut out, b"skeindb-encryption-aad-v1");
        push_len_prefixed(&mut out, db_id.as_bytes());
        push_len_prefixed(&mut out, table_id.as_bytes());
        push_len_prefixed(&mut out, column_id.as_bytes());
        out.push(self.value_kind as u8);
        out.extend_from_slice(&self.codec_version.to_le_bytes());
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionEnvelope {
    pub version: u8,
    pub mode: EncryptionMode,
    pub scope_id: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<[u8; ENCRYPTION_NONCE_LEN]>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derivation_salt: Option<[u8; ENCRYPTION_DERIVATION_SALT_LEN]>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ciphertext: Vec<u8>,
}

impl EncryptionEnvelope {
    pub fn stored_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(self.version);
        out.push(self.mode.code());
        push_len_prefixed(&mut out, self.scope_id.as_bytes());

        match self.key_id.as_deref() {
            Some(key_id) => {
                out.push(1);
                push_len_prefixed(&mut out, key_id.as_bytes());
            }
            None => out.push(0),
        }

        match self.nonce {
            Some(nonce) => {
                out.push(1);
                out.extend_from_slice(&nonce);
            }
            None => out.push(0),
        }

        match self.derivation_salt {
            Some(salt) => {
                out.push(1);
                out.extend_from_slice(&salt);
            }
            None => out.push(0),
        }

        push_len_prefixed(&mut out, &self.ciphertext);
        out
    }
}

impl EncryptionEnvelope {
    /// Parse an envelope produced by [`EncryptionEnvelope::stored_bytes`]. This is the
    /// canonical T191 / T192 reader: it consumes the entire input and rejects trailing bytes
    /// so a caller can safely treat any successful parse as a complete, self-describing
    /// encryption envelope.
    pub fn from_stored_bytes(bytes: &[u8]) -> Result<Self, EncryptionError> {
        let mut cursor: &[u8] = bytes;
        let version = read_byte(&mut cursor)?;
        if version != ENCRYPTION_ENVELOPE_VERSION {
            return Err(EncryptionError::InvalidEnvelope(format!(
                "unsupported envelope version: {version}"
            )));
        }
        let mode_code = read_byte(&mut cursor)?;
        let mode = match mode_code {
            0 => EncryptionMode::Off,
            1 => EncryptionMode::EncRandom,
            2 => EncryptionMode::EncMleDb,
            other => {
                return Err(EncryptionError::InvalidEnvelope(format!(
                    "unknown encryption mode code: {other}"
                )))
            }
        };
        let scope_id = read_string(&mut cursor)?;
        let key_id = match read_byte(&mut cursor)? {
            0 => None,
            1 => Some(read_string(&mut cursor)?),
            other => {
                return Err(EncryptionError::InvalidEnvelope(format!(
                    "invalid key_id presence flag: {other}"
                )))
            }
        };
        let nonce = match read_byte(&mut cursor)? {
            0 => None,
            1 => Some(read_fixed::<ENCRYPTION_NONCE_LEN>(&mut cursor)?),
            other => {
                return Err(EncryptionError::InvalidEnvelope(format!(
                    "invalid nonce presence flag: {other}"
                )))
            }
        };
        let derivation_salt = match read_byte(&mut cursor)? {
            0 => None,
            1 => Some(read_fixed::<ENCRYPTION_DERIVATION_SALT_LEN>(&mut cursor)?),
            other => {
                return Err(EncryptionError::InvalidEnvelope(format!(
                    "invalid derivation_salt presence flag: {other}"
                )))
            }
        };
        let ciphertext = read_bytes(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(EncryptionError::InvalidEnvelope(
                "trailing bytes after envelope".to_string(),
            ));
        }
        Ok(EncryptionEnvelope {
            version,
            mode,
            scope_id,
            key_id,
            nonce,
            derivation_salt,
            ciphertext,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct DatabaseKeyManager {
    profiles: HashMap<String, DatabaseEncryptionProfile>,
    keys: HashMap<(String, String), [u8; ENCRYPTION_MASTER_KEY_LEN]>,
}

impl DatabaseKeyManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_database_key(
        &mut self,
        db_id: impl AsRef<str>,
        key_id: impl AsRef<str>,
        master_key: [u8; ENCRYPTION_MASTER_KEY_LEN],
    ) -> Result<(), EncryptionError> {
        let db_id = normalize_required(db_id.as_ref(), "db_id")?.to_string();
        let key_id = normalize_required(key_id.as_ref(), "key_id")?.to_string();

        self.keys
            .insert((db_id.clone(), key_id.clone()), master_key);
        let profile = self
            .profiles
            .entry(db_id.clone())
            .or_insert_with(|| DatabaseEncryptionProfile::new(db_id));
        if profile.active_key_id.is_none() {
            profile.active_key_id = Some(key_id);
        }
        Ok(())
    }

    pub fn set_active_database_key(
        &mut self,
        db_id: impl AsRef<str>,
        key_id: impl AsRef<str>,
    ) -> Result<(), EncryptionError> {
        let db_id = normalize_required(db_id.as_ref(), "db_id")?.to_string();
        let key_id = normalize_required(key_id.as_ref(), "key_id")?.to_string();
        if !self.keys.contains_key(&(db_id.clone(), key_id.clone())) {
            return Err(EncryptionError::MissingKey { db_id, key_id });
        }

        let profile = self
            .profiles
            .entry(db_id.clone())
            .or_insert_with(|| DatabaseEncryptionProfile::new(db_id));
        profile.active_key_id = Some(key_id);
        Ok(())
    }

    pub fn set_database_mode(
        &mut self,
        db_id: impl AsRef<str>,
        mode: EncryptionMode,
    ) -> Result<(), EncryptionError> {
        let db_id = normalize_required(db_id.as_ref(), "db_id")?.to_string();
        let profile = self
            .profiles
            .entry(db_id.clone())
            .or_insert_with(|| DatabaseEncryptionProfile::new(db_id.clone()));

        if mode != EncryptionMode::Off {
            let key_id = profile
                .active_key_id
                .as_ref()
                .ok_or_else(|| EncryptionError::MissingActiveKey(db_id.clone()))?;
            if !self.keys.contains_key(&(db_id.clone(), key_id.clone())) {
                return Err(EncryptionError::MissingKey {
                    db_id,
                    key_id: key_id.clone(),
                });
            }
        }

        profile.mode = mode;
        Ok(())
    }

    pub fn profile(&self, db_id: &str) -> Option<&DatabaseEncryptionProfile> {
        self.profiles.get(db_id.trim())
    }

    /// Sorted list of all database IDs that currently have an encryption profile registered.
    pub fn database_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.profiles.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Sorted list of registered key IDs for a database (empty if the database is unknown).
    pub fn registered_key_ids(&self, db_id: &str) -> Vec<String> {
        let trimmed = db_id.trim();
        let mut ids: Vec<String> = self
            .keys
            .keys()
            .filter_map(|(d, k)| if d == trimmed { Some(k.clone()) } else { None })
            .collect();
        ids.sort();
        ids
    }

    pub fn encrypt(
        &self,
        ctx: &EncryptionContext,
        plaintext: &[u8],
    ) -> Result<EncryptionEnvelope, EncryptionError> {
        let db_id = normalize_required(&ctx.db_id, "db_id")?.to_string();
        let profile = self
            .profiles
            .get(&db_id)
            .ok_or_else(|| EncryptionError::UnknownDatabase(db_id.clone()))?;
        let aad = ctx.aad_bytes()?;

        match profile.mode {
            EncryptionMode::Off => Ok(EncryptionEnvelope {
                version: ENCRYPTION_ENVELOPE_VERSION,
                mode: EncryptionMode::Off,
                scope_id: db_id,
                key_id: None,
                nonce: None,
                derivation_salt: None,
                ciphertext: plaintext.to_vec(),
            }),
            EncryptionMode::EncRandom => {
                let key_id = profile
                    .active_key_id
                    .clone()
                    .ok_or_else(|| EncryptionError::MissingActiveKey(db_id.clone()))?;
                let master_key = self.lookup_key(&db_id, &key_id)?;
                let content_key = derive_key(master_key, None, ENC_RANDOM_INFO)?;
                let mut nonce = [0u8; ENCRYPTION_NONCE_LEN];
                getrandom::getrandom(&mut nonce)
                    .map_err(|err| EncryptionError::Entropy(err.to_string()))?;
                let ciphertext = encrypt_with_key(&content_key, &nonce, &aad, plaintext)?;
                Ok(EncryptionEnvelope {
                    version: ENCRYPTION_ENVELOPE_VERSION,
                    mode: EncryptionMode::EncRandom,
                    scope_id: db_id,
                    key_id: Some(key_id),
                    nonce: Some(nonce),
                    derivation_salt: None,
                    ciphertext,
                })
            }
            EncryptionMode::EncMleDb => {
                let key_id = profile
                    .active_key_id
                    .clone()
                    .ok_or_else(|| EncryptionError::MissingActiveKey(db_id.clone()))?;
                let master_key = self.lookup_key(&db_id, &key_id)?;
                let digest = Sha256::digest(plaintext);
                let mut derivation_salt = [0u8; ENCRYPTION_DERIVATION_SALT_LEN];
                derivation_salt.copy_from_slice(&digest);
                let content_key = derive_key(master_key, Some(&derivation_salt), ENC_MLE_DB_INFO)?;
                let nonce =
                    derive_nonce(master_key, Some(&derivation_salt), ENC_MLE_DB_NONCE_INFO)?;
                let ciphertext = encrypt_with_key(&content_key, &nonce, &aad, plaintext)?;
                Ok(EncryptionEnvelope {
                    version: ENCRYPTION_ENVELOPE_VERSION,
                    mode: EncryptionMode::EncMleDb,
                    scope_id: db_id,
                    key_id: Some(key_id),
                    nonce: None,
                    derivation_salt: Some(derivation_salt),
                    ciphertext,
                })
            }
        }
    }

    pub fn decrypt(
        &self,
        ctx: &EncryptionContext,
        envelope: &EncryptionEnvelope,
    ) -> Result<Vec<u8>, EncryptionError> {
        let db_id = normalize_required(&ctx.db_id, "db_id")?.to_string();
        if envelope.scope_id != db_id {
            return Err(EncryptionError::ScopeMismatch {
                expected: envelope.scope_id.clone(),
                actual: db_id,
            });
        }
        let aad = ctx.aad_bytes()?;

        match envelope.mode {
            EncryptionMode::Off => Ok(envelope.ciphertext.clone()),
            EncryptionMode::EncRandom => {
                let key_id = envelope
                    .key_id
                    .as_deref()
                    .ok_or(EncryptionError::MissingEnvelopeKeyId)?;
                let nonce = envelope.nonce.ok_or(EncryptionError::MissingNonce)?;
                let master_key = self.lookup_key(&envelope.scope_id, key_id)?;
                let content_key = derive_key(master_key, None, ENC_RANDOM_INFO)?;
                decrypt_with_key(&content_key, &nonce, &aad, &envelope.ciphertext)
            }
            EncryptionMode::EncMleDb => {
                let key_id = envelope
                    .key_id
                    .as_deref()
                    .ok_or(EncryptionError::MissingEnvelopeKeyId)?;
                let derivation_salt = envelope
                    .derivation_salt
                    .ok_or(EncryptionError::MissingDerivationSalt)?;
                let master_key = self.lookup_key(&envelope.scope_id, key_id)?;
                let content_key = derive_key(master_key, Some(&derivation_salt), ENC_MLE_DB_INFO)?;
                let nonce =
                    derive_nonce(master_key, Some(&derivation_salt), ENC_MLE_DB_NONCE_INFO)?;
                decrypt_with_key(&content_key, &nonce, &aad, &envelope.ciphertext)
            }
        }
    }

    fn lookup_key(
        &self,
        db_id: &str,
        key_id: &str,
    ) -> Result<&[u8; ENCRYPTION_MASTER_KEY_LEN], EncryptionError> {
        self.keys
            .get(&(db_id.to_string(), key_id.to_string()))
            .ok_or_else(|| EncryptionError::MissingKey {
                db_id: db_id.to_string(),
                key_id: key_id.to_string(),
            })
    }
}

/// Plan summary for a T192 key-rotation operation.
///
/// `previous_key_id` is the active key prior to the rotation (may be `None` if the database
/// did not have an active key yet); `new_key_id` is the key that is now marked active. The
/// caller is responsible for re-encrypting existing values via [`DatabaseKeyManager::reencrypt_envelope`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRotationPlan {
    pub db_id: String,
    pub previous_key_id: Option<String>,
    pub new_key_id: String,
    pub mode: EncryptionMode,
}

/// Progress counters for a streaming re-encryption pass driven by [`DatabaseKeyManager::reencrypt_envelope`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReencryptionProgress {
    pub db_id: String,
    pub previous_key_id: Option<String>,
    pub new_key_id: Option<String>,
    pub envelopes_inspected: u64,
    pub envelopes_rewritten: u64,
    pub envelopes_skipped_off: u64,
    pub envelopes_skipped_current: u64,
}

impl ReencryptionProgress {
    pub fn new(db_id: impl Into<String>) -> Self {
        Self {
            db_id: db_id.into(),
            ..Default::default()
        }
    }

    pub fn note_inspected(&mut self) {
        self.envelopes_inspected = self.envelopes_inspected.saturating_add(1);
    }

    pub fn note_rewritten(&mut self) {
        self.envelopes_rewritten = self.envelopes_rewritten.saturating_add(1);
    }

    pub fn note_off(&mut self) {
        self.envelopes_skipped_off = self.envelopes_skipped_off.saturating_add(1);
    }

    pub fn note_already_current(&mut self) {
        self.envelopes_skipped_current = self.envelopes_skipped_current.saturating_add(1);
    }
}

impl DatabaseKeyManager {
    /// Rotate the active key for `db_id` to `new_key_id`. Both the new key and (implicitly)
    /// any prior keys remain available for decrypting historical envelopes; only the
    /// *active* key — the one used to encrypt new envelopes — changes.
    pub fn rotate_active_key(
        &mut self,
        db_id: impl AsRef<str>,
        new_key_id: impl AsRef<str>,
    ) -> Result<KeyRotationPlan, EncryptionError> {
        let db_id = normalize_required(db_id.as_ref(), "db_id")?.to_string();
        let new_key_id = normalize_required(new_key_id.as_ref(), "key_id")?.to_string();
        if !self.keys.contains_key(&(db_id.clone(), new_key_id.clone())) {
            return Err(EncryptionError::MissingKey {
                db_id,
                key_id: new_key_id,
            });
        }
        let profile = self
            .profiles
            .entry(db_id.clone())
            .or_insert_with(|| DatabaseEncryptionProfile::new(db_id.clone()));
        let previous = profile.active_key_id.clone();
        profile.active_key_id = Some(new_key_id.clone());
        Ok(KeyRotationPlan {
            db_id,
            previous_key_id: previous,
            new_key_id,
            mode: profile.mode,
        })
    }

    /// Re-encrypt a single envelope so it uses the database's currently-active key. Returns
    /// `Ok(None)` if the envelope is already up to date or has mode `ENC_OFF`. Updates the
    /// supplied [`ReencryptionProgress`] counters either way.
    pub fn reencrypt_envelope(
        &self,
        ctx: &EncryptionContext,
        envelope: &EncryptionEnvelope,
        progress: &mut ReencryptionProgress,
    ) -> Result<Option<EncryptionEnvelope>, EncryptionError> {
        progress.note_inspected();

        if envelope.mode == EncryptionMode::Off {
            progress.note_off();
            return Ok(None);
        }

        let db_id = normalize_required(&ctx.db_id, "db_id")?.to_string();
        let profile = self
            .profiles
            .get(&db_id)
            .ok_or_else(|| EncryptionError::UnknownDatabase(db_id.clone()))?;
        let active_key_id = profile
            .active_key_id
            .clone()
            .ok_or_else(|| EncryptionError::MissingActiveKey(db_id.clone()))?;

        progress.previous_key_id = envelope.key_id.clone();
        progress.new_key_id = Some(active_key_id.clone());

        if envelope.key_id.as_deref() == Some(active_key_id.as_str())
            && envelope.mode == profile.mode
        {
            progress.note_already_current();
            return Ok(None);
        }

        let plaintext = self.decrypt(ctx, envelope)?;
        let new_envelope = self.encrypt(ctx, &plaintext)?;
        progress.note_rewritten();
        Ok(Some(new_envelope))
    }
}

#[derive(Debug, Error)]
pub enum EncryptionError {
    #[error("invalid {field}: must not be empty")]
    InvalidIdentifier { field: &'static str },
    #[error("database encryption profile not configured for '{0}'")]
    UnknownDatabase(String),
    #[error("database '{0}' does not have an active encryption key")]
    MissingActiveKey(String),
    #[error("missing key '{key_id}' for database '{db_id}'")]
    MissingKey { db_id: String, key_id: String },
    #[error("encryption scope mismatch: expected '{expected}', got '{actual}'")]
    ScopeMismatch { expected: String, actual: String },
    #[error("encryption envelope is missing key_id")]
    MissingEnvelopeKeyId,
    #[error("encryption envelope is missing nonce")]
    MissingNonce,
    #[error("encryption envelope is missing derivation_salt")]
    MissingDerivationSalt,
    #[error("invalid derived encryption key length")]
    InvalidDerivedKeyLength,
    #[error("HKDF key derivation failed")]
    KeyDerivation,
    #[error("AEAD operation failed")]
    Aead,
    #[error("system entropy unavailable: {0}")]
    Entropy(String),
    #[error("invalid stored encryption envelope: {0}")]
    InvalidEnvelope(String),
}

fn normalize_required<'a>(value: &'a str, field: &'static str) -> Result<&'a str, EncryptionError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(EncryptionError::InvalidIdentifier { field });
    }
    Ok(trimmed)
}

fn push_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&encode_varu(bytes.len() as u64));
    out.extend_from_slice(bytes);
}

fn read_byte(cursor: &mut &[u8]) -> Result<u8, EncryptionError> {
    let (&head, rest) = cursor.split_first().ok_or_else(|| {
        EncryptionError::InvalidEnvelope("unexpected end of envelope".to_string())
    })?;
    *cursor = rest;
    Ok(head)
}

fn read_bytes(cursor: &mut &[u8]) -> Result<Vec<u8>, EncryptionError> {
    let len = decode_varu(cursor)
        .map_err(|err| EncryptionError::InvalidEnvelope(format!("invalid length prefix: {err}")))?
        as usize;
    if cursor.len() < len {
        return Err(EncryptionError::InvalidEnvelope(format!(
            "length-prefixed slice exceeds remaining bytes (need {len}, have {})",
            cursor.len()
        )));
    }
    let (head, rest) = cursor.split_at(len);
    let out = head.to_vec();
    *cursor = rest;
    Ok(out)
}

fn read_string(cursor: &mut &[u8]) -> Result<String, EncryptionError> {
    let bytes = read_bytes(cursor)?;
    String::from_utf8(bytes).map_err(|err| {
        EncryptionError::InvalidEnvelope(format!("invalid UTF-8 in envelope string: {err}"))
    })
}

fn read_fixed<const N: usize>(cursor: &mut &[u8]) -> Result<[u8; N], EncryptionError> {
    if cursor.len() < N {
        return Err(EncryptionError::InvalidEnvelope(format!(
            "expected {N} bytes, found {}",
            cursor.len()
        )));
    }
    let (head, rest) = cursor.split_at(N);
    let mut out = [0u8; N];
    out.copy_from_slice(head);
    *cursor = rest;
    Ok(out)
}

fn derive_key(
    master_key: &[u8; ENCRYPTION_MASTER_KEY_LEN],
    salt: Option<&[u8]>,
    info: &[u8],
) -> Result<[u8; ENCRYPTION_MASTER_KEY_LEN], EncryptionError> {
    let hkdf = Hkdf::<Sha256>::new(salt, master_key);
    let mut out = [0u8; ENCRYPTION_MASTER_KEY_LEN];
    hkdf.expand(info, &mut out)
        .map_err(|_| EncryptionError::KeyDerivation)?;
    Ok(out)
}

fn derive_nonce(
    master_key: &[u8; ENCRYPTION_MASTER_KEY_LEN],
    salt: Option<&[u8]>,
    info: &[u8],
) -> Result<[u8; ENCRYPTION_NONCE_LEN], EncryptionError> {
    let hkdf = Hkdf::<Sha256>::new(salt, master_key);
    let mut out = [0u8; ENCRYPTION_NONCE_LEN];
    hkdf.expand(info, &mut out)
        .map_err(|_| EncryptionError::KeyDerivation)?;
    Ok(out)
}

fn encrypt_with_key(
    key: &[u8; ENCRYPTION_MASTER_KEY_LEN],
    nonce: &[u8; ENCRYPTION_NONCE_LEN],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    let cipher =
        Aes256GcmSiv::new_from_slice(key).map_err(|_| EncryptionError::InvalidDerivedKeyLength)?;
    cipher
        .encrypt(
            nonce.into(),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| EncryptionError::Aead)
}

fn decrypt_with_key(
    key: &[u8; ENCRYPTION_MASTER_KEY_LEN],
    nonce: &[u8; ENCRYPTION_NONCE_LEN],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    let cipher =
        Aes256GcmSiv::new_from_slice(key).map_err(|_| EncryptionError::InvalidDerivedKeyLength)?;
    cipher
        .decrypt(
            nonce.into(),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| EncryptionError::Aead)
}
