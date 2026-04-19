use std::path::PathBuf;

use skeindb_core::valuestore::{ValueStore, ValueStoreConfig};
use skeindb_core::wasm_catalog::{
    WasmCatalogError, WasmModuleCapabilities, WasmModuleCatalog, WasmModuleInstallRequest,
    WasmModuleKind, WasmTableCapability, WASM_UDF_ABI_V1,
};

struct Cleanup(Vec<PathBuf>);

impl Drop for Cleanup {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn temp_path(tag: &str, ext: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "skeindb-wasm-catalog-{}-{}-{}.{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        ext
    ));
    p
}

fn install_request(module_id: &str, bytes: &[u8]) -> WasmModuleInstallRequest {
    WasmModuleInstallRequest {
        module_id: module_id.to_string(),
        name: Some(format!("{module_id} udf")),
        kind: WasmModuleKind::Scalar,
        abi: WASM_UDF_ABI_V1.to_string(),
        entrypoint: "skein_scalar".to_string(),
        capabilities: WasmModuleCapabilities {
            allowed_hostcalls: vec!["log.debug".to_string(), "stats.increment".to_string()],
            allowed_tables: vec![WasmTableCapability {
                db: "app".to_string(),
                table: "metrics".to_string(),
                read: true,
                write: false,
            }],
            deterministic: true,
            max_fuel: 50_000,
            max_memory_bytes: 256 * 1024,
            max_output_bytes: 8 * 1024,
        },
        wasm_bytes: bytes.to_vec(),
        overwrite: false,
    }
}

#[test]
fn wasm_catalog_roundtrip_via_json_and_vseg_preserves_metadata_and_bytes() {
    let vseg_path = temp_path("store", "vseg");
    let catalog_path = temp_path("catalog", "json");
    let _cleanup = Cleanup(vec![vseg_path.clone(), catalog_path.clone()]);

    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(&mut store, install_request("math_abs", b"\0asmabs"), 100)
        .unwrap();
    catalog
        .install(&mut store, install_request("math_sqrt", b"\0asmsqrt"), 200)
        .unwrap();

    store.write_segment_file(&vseg_path).unwrap();
    catalog.save_json(&catalog_path).unwrap();

    let mut loaded_store =
        ValueStore::load_segment_file(&vseg_path, ValueStoreConfig::default()).unwrap();
    let loaded_catalog = WasmModuleCatalog::load_json(&catalog_path).unwrap();
    let modules = loaded_catalog.list();
    assert_eq!(modules.len(), 2);
    assert_eq!(modules[0].module_id, "math_abs");
    assert_eq!(modules[1].module_id, "math_sqrt");
    assert_eq!(
        loaded_catalog
            .module_bytes(&mut loaded_store, "math_abs")
            .unwrap(),
        b"\0asmabs"
    );
    assert_eq!(
        loaded_catalog
            .module_bytes(&mut loaded_store, "math_sqrt")
            .unwrap(),
        b"\0asmsqrt"
    );
}

#[test]
fn wasm_catalog_load_rejects_unsupported_format_version() {
    let catalog_path = temp_path("badver", "json");
    let _cleanup = Cleanup(vec![catalog_path.clone()]);
    std::fs::write(&catalog_path, br#"{"format_version":99,"modules":[]}"#).unwrap();

    let err = WasmModuleCatalog::load_json(&catalog_path).unwrap_err();
    assert!(matches!(
        err,
        WasmCatalogError::UnsupportedFormatVersion(99)
    ));
}

#[test]
fn wasm_catalog_load_rejects_invalid_value_id_hex() {
    let catalog_path = temp_path("badhex", "json");
    let _cleanup = Cleanup(vec![catalog_path.clone()]);
    std::fs::write(
        &catalog_path,
        br#"{
            "format_version":1,
            "modules":[{
                "module_id":"bad",
                "kind":"scalar",
                "abi":"skein.wasm.udf.v1",
                "entrypoint":"skein_scalar",
                "value_id":"xyz",
                "size_bytes":4,
                "capabilities":{"deterministic":true},
                "created_at_ms":1
            }]
        }"#,
    )
    .unwrap();

    let err = WasmModuleCatalog::load_json(&catalog_path).unwrap_err();
    assert!(matches!(err, WasmCatalogError::InvalidValueIdHex));
}
