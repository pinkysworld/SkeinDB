//! Wasm module store + catalog metadata for UDFs (T080).
//!
//! T080 is intentionally scoped to storage/catalog concerns only:
//!
//! - Wasm module bytes are stored immutably in [`crate::valuestore::ValueStore`]
//!   as `ValueKind::BlobChunk` values.
//! - Metadata is tracked in an in-memory catalog keyed by `module_id`.
//! - The catalog persists to a JSON file (`wasm_catalog.json`, format v1).
//! - No Wasm execution, sandboxing, or cancellation is implemented here; that
//!   remains for T081/T082.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::valuestore::{ValueId, ValueStore, ValueStoreError};
use crate::ValueKind;

pub const WASM_CATALOG_FORMAT_VERSION: u32 = 1;
pub const WASM_UDF_ABI_V1: &str = "skein.wasm.udf.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WasmModuleKind {
    Scalar,
    Aggregate,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmTableCapability {
    pub db: String,
    pub table: String,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub write: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WasmModuleCapabilities {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_hostcalls: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tables: Vec<WasmTableCapability>,
    #[serde(default)]
    pub deterministic: bool,
    #[serde(default)]
    pub max_fuel: u64,
    #[serde(default)]
    pub max_memory_bytes: u64,
    #[serde(default)]
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmModuleRecord {
    pub module_id: String,
    pub name: Option<String>,
    pub kind: WasmModuleKind,
    pub abi: String,
    pub entrypoint: String,
    pub value_id: ValueId,
    pub size_bytes: u64,
    pub capabilities: WasmModuleCapabilities,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct WasmModuleInstallRequest {
    pub module_id: String,
    pub name: Option<String>,
    pub kind: WasmModuleKind,
    pub abi: String,
    pub entrypoint: String,
    pub capabilities: WasmModuleCapabilities,
    pub wasm_bytes: Vec<u8>,
    pub overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmModuleInstallResult {
    pub module_id: String,
    pub value_id: ValueId,
    pub size_bytes: u64,
}

#[derive(Debug, Error)]
pub enum WasmCatalogError {
    #[error("invalid module_id")]
    InvalidModuleId,
    #[error("invalid entrypoint")]
    InvalidEntrypoint,
    #[error("invalid abi")]
    InvalidAbi,
    #[error("wasm module bytes must not be empty")]
    EmptyModule,
    #[error("module already exists")]
    Conflict,
    #[error("module not found")]
    NotFound,
    #[error("unsupported wasm catalog format version: {0}")]
    UnsupportedFormatVersion(u32),
    #[error("invalid value id hex in wasm catalog")]
    InvalidValueIdHex,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ValueStore(#[from] ValueStoreError),
}

#[derive(Debug, Clone, Default)]
pub struct WasmModuleCatalog {
    modules: BTreeMap<String, WasmModuleRecord>,
}

impl WasmModuleCatalog {
    pub fn new() -> Self {
        Self {
            modules: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.modules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    pub fn contains(&self, module_id: &str) -> bool {
        self.modules.contains_key(module_id)
    }

    pub fn get(&self, module_id: &str) -> Option<&WasmModuleRecord> {
        self.modules.get(module_id)
    }

    pub fn list(&self) -> Vec<&WasmModuleRecord> {
        self.modules.values().collect()
    }

    pub fn install(
        &mut self,
        value_store: &mut ValueStore,
        req: WasmModuleInstallRequest,
        created_at_ms: u64,
    ) -> Result<WasmModuleInstallResult, WasmCatalogError> {
        let module_id = req.module_id.trim();
        if module_id.is_empty() {
            return Err(WasmCatalogError::InvalidModuleId);
        }
        let entrypoint = req.entrypoint.trim();
        if entrypoint.is_empty() {
            return Err(WasmCatalogError::InvalidEntrypoint);
        }
        let abi = req.abi.trim();
        if abi.is_empty() {
            return Err(WasmCatalogError::InvalidAbi);
        }
        if req.wasm_bytes.is_empty() {
            return Err(WasmCatalogError::EmptyModule);
        }
        if self.modules.contains_key(module_id) && !req.overwrite {
            return Err(WasmCatalogError::Conflict);
        }

        let value_id = value_store.put(ValueKind::BlobChunk, req.wasm_bytes.clone());
        let record = WasmModuleRecord {
            module_id: module_id.to_string(),
            name: req.name,
            kind: req.kind,
            abi: abi.to_string(),
            entrypoint: entrypoint.to_string(),
            value_id,
            size_bytes: req.wasm_bytes.len() as u64,
            capabilities: req.capabilities,
            created_at_ms,
        };
        self.modules.insert(module_id.to_string(), record);
        Ok(WasmModuleInstallResult {
            module_id: module_id.to_string(),
            value_id,
            size_bytes: req.wasm_bytes.len() as u64,
        })
    }

    pub fn drop_module(&mut self, module_id: &str) -> Result<(), WasmCatalogError> {
        if self.modules.remove(module_id).is_none() {
            return Err(WasmCatalogError::NotFound);
        }
        Ok(())
    }

    pub fn module_bytes(
        &self,
        value_store: &mut ValueStore,
        module_id: &str,
    ) -> Result<Vec<u8>, WasmCatalogError> {
        let record = self
            .modules
            .get(module_id)
            .ok_or(WasmCatalogError::NotFound)?;
        Ok(value_store.materialize(&record.value_id)?)
    }

    pub fn save_json(&self, path: impl AsRef<Path>) -> Result<(), WasmCatalogError> {
        let disk = WasmCatalogDisk {
            format_version: WASM_CATALOG_FORMAT_VERSION,
            modules: self
                .modules
                .values()
                .cloned()
                .map(WasmCatalogDiskEntry::from_record)
                .collect(),
        };
        let bytes = serde_json::to_vec_pretty(&disk)?;
        fs::write(path, bytes)?;
        Ok(())
    }

    pub fn load_json(path: impl AsRef<Path>) -> Result<Self, WasmCatalogError> {
        let bytes = fs::read(path)?;
        let disk: WasmCatalogDisk = serde_json::from_slice(&bytes)?;
        if disk.format_version != WASM_CATALOG_FORMAT_VERSION {
            return Err(WasmCatalogError::UnsupportedFormatVersion(
                disk.format_version,
            ));
        }

        let mut modules = BTreeMap::new();
        for entry in disk.modules {
            let record = entry.into_record()?;
            modules.insert(record.module_id.clone(), record);
        }
        Ok(Self { modules })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WasmCatalogDisk {
    format_version: u32,
    modules: Vec<WasmCatalogDiskEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WasmCatalogDiskEntry {
    module_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    kind: WasmModuleKind,
    abi: String,
    entrypoint: String,
    value_id: String,
    size_bytes: u64,
    capabilities: WasmModuleCapabilities,
    created_at_ms: u64,
}

impl WasmCatalogDiskEntry {
    fn from_record(record: WasmModuleRecord) -> Self {
        Self {
            module_id: record.module_id,
            name: record.name,
            kind: record.kind,
            abi: record.abi,
            entrypoint: record.entrypoint,
            value_id: encode_value_id_hex(record.value_id),
            size_bytes: record.size_bytes,
            capabilities: record.capabilities,
            created_at_ms: record.created_at_ms,
        }
    }

    fn into_record(self) -> Result<WasmModuleRecord, WasmCatalogError> {
        Ok(WasmModuleRecord {
            module_id: self.module_id,
            name: self.name,
            kind: self.kind,
            abi: self.abi,
            entrypoint: self.entrypoint,
            value_id: decode_value_id_hex(&self.value_id)?,
            size_bytes: self.size_bytes,
            capabilities: self.capabilities,
            created_at_ms: self.created_at_ms,
        })
    }
}

fn encode_value_id_hex(id: ValueId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for byte in id {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_value_id_hex(s: &str) -> Result<ValueId, WasmCatalogError> {
    if s.len() != 32 {
        return Err(WasmCatalogError::InvalidValueIdHex);
    }
    let mut out = [0u8; 16];
    let bytes = s.as_bytes();
    for idx in 0..16 {
        let hi = from_hex_nibble(bytes[idx * 2])?;
        let lo = from_hex_nibble(bytes[idx * 2 + 1])?;
        out[idx] = (hi << 4) | lo;
    }
    Ok(out)
}

fn from_hex_nibble(byte: u8) -> Result<u8, WasmCatalogError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(WasmCatalogError::InvalidValueIdHex),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::valuestore::ValueStoreConfig;

    fn install_request(module_id: &str, bytes: &[u8]) -> WasmModuleInstallRequest {
        WasmModuleInstallRequest {
            module_id: module_id.to_string(),
            name: Some(format!("{module_id} name")),
            kind: WasmModuleKind::Scalar,
            abi: WASM_UDF_ABI_V1.to_string(),
            entrypoint: "skein_scalar".to_string(),
            capabilities: WasmModuleCapabilities {
                allowed_hostcalls: vec!["log.debug".to_string()],
                allowed_tables: vec![WasmTableCapability {
                    db: "app".to_string(),
                    table: "users".to_string(),
                    read: true,
                    write: false,
                }],
                deterministic: true,
                max_fuel: 10_000,
                max_memory_bytes: 64 * 1024,
                max_output_bytes: 4 * 1024,
            },
            wasm_bytes: bytes.to_vec(),
            overwrite: false,
        }
    }

    #[test]
    fn install_and_get_module_bytes() {
        let mut store = ValueStore::new(ValueStoreConfig::default());
        let mut catalog = WasmModuleCatalog::new();
        let install = catalog
            .install(&mut store, install_request("math_abs", b"\0asm"), 123)
            .unwrap();
        assert_eq!(install.module_id, "math_abs");
        assert_eq!(install.size_bytes, 4);
        assert_eq!(catalog.len(), 1);
        assert!(store.contains(install.value_id));
        assert_eq!(
            catalog.module_bytes(&mut store, "math_abs").unwrap(),
            b"\0asm"
        );
    }

    #[test]
    fn install_rejects_invalid_requests() {
        let mut store = ValueStore::new(ValueStoreConfig::default());
        let mut catalog = WasmModuleCatalog::new();

        let mut bad = install_request("", b"\0asm");
        assert!(matches!(
            catalog.install(&mut store, bad, 0).unwrap_err(),
            WasmCatalogError::InvalidModuleId
        ));

        bad = install_request("ok", b"\0asm");
        bad.entrypoint.clear();
        assert!(matches!(
            catalog.install(&mut store, bad, 0).unwrap_err(),
            WasmCatalogError::InvalidEntrypoint
        ));

        bad = install_request("ok", b"\0asm");
        bad.abi.clear();
        assert!(matches!(
            catalog.install(&mut store, bad, 0).unwrap_err(),
            WasmCatalogError::InvalidAbi
        ));

        bad = install_request("ok", b"");
        assert!(matches!(
            catalog.install(&mut store, bad, 0).unwrap_err(),
            WasmCatalogError::EmptyModule
        ));
    }

    #[test]
    fn overwrite_and_drop_module() {
        let mut store = ValueStore::new(ValueStoreConfig::default());
        let mut catalog = WasmModuleCatalog::new();
        catalog
            .install(&mut store, install_request("f", b"old"), 10)
            .unwrap();
        assert!(matches!(
            catalog
                .install(&mut store, install_request("f", b"new"), 11)
                .unwrap_err(),
            WasmCatalogError::Conflict
        ));

        let mut overwrite = install_request("f", b"new");
        overwrite.overwrite = true;
        let result = catalog.install(&mut store, overwrite, 11).unwrap();
        assert_eq!(catalog.module_bytes(&mut store, "f").unwrap(), b"new");
        assert_eq!(catalog.get("f").unwrap().created_at_ms, 11);
        assert_eq!(catalog.get("f").unwrap().value_id, result.value_id);

        catalog.drop_module("f").unwrap();
        assert!(matches!(
            catalog.drop_module("f").unwrap_err(),
            WasmCatalogError::NotFound
        ));
    }

    #[test]
    fn value_id_hex_roundtrip() {
        let id: ValueId = [0xab; 16];
        let hex = encode_value_id_hex(id);
        assert_eq!(hex, "abababababababababababababababab");
        assert_eq!(decode_value_id_hex(&hex).unwrap(), id);
        assert!(matches!(
            decode_value_id_hex("xyz"),
            Err(WasmCatalogError::InvalidValueIdHex)
        ));
    }
}
