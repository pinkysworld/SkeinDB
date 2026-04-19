use skeindb_core::valuestore::{ValueStore, ValueStoreConfig};
use skeindb_core::wasm_catalog::{
    WasmModuleCapabilities, WasmModuleCatalog, WasmModuleInstallRequest, WasmModuleKind,
    WASM_UDF_ABI_V1,
};
use skeindb_core::wasm_udf::{execute_scalar_udf, WasmUdfError, WasmValue};

fn install_request(
    module_id: &str,
    bytes: &[u8],
    allowed_hostcalls: Vec<&str>,
    max_memory_bytes: u64,
    max_output_bytes: u64,
) -> WasmModuleInstallRequest {
    WasmModuleInstallRequest {
        module_id: module_id.to_string(),
        name: Some(module_id.to_string()),
        kind: WasmModuleKind::Scalar,
        abi: WASM_UDF_ABI_V1.to_string(),
        entrypoint: "skein_scalar".to_string(),
        capabilities: WasmModuleCapabilities {
            allowed_hostcalls: allowed_hostcalls.into_iter().map(str::to_string).collect(),
            allowed_tables: Vec::new(),
            deterministic: true,
            max_fuel: 0,
            max_memory_bytes,
            max_output_bytes,
        },
        wasm_bytes: bytes.to_vec(),
        overwrite: false,
    }
}

fn constant_u64_module(value: u64) -> Vec<u8> {
    wat::parse_str(format!(
        r#"(module
            (memory (export "memory") 1 1)
            (func (export "skein_alloc") (param i32) (result i32)
              (i32.const 0)
            )
            (data (i32.const 16) "\04{}")
            (func (export "skein_scalar") (param i32 i32) (result i64)
              (i64.or
                (i64.shl (i64.extend_i32_u (i32.const 16)) (i64.const 32))
                (i64.extend_i32_u (i32.const 9))
              )
            )
        )"#,
        value
            .to_le_bytes()
            .iter()
            .map(|b| format!("\\{:02x}", b))
            .collect::<String>()
    ))
    .unwrap()
}

fn log_debug_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
            (import "skein" "log_debug" (func $log_debug (param i32 i32) (result i32)))
            (memory (export "memory") 1 1)
            (func (export "skein_alloc") (param i32) (result i32)
              (i32.const 0)
            )
            (data (i32.const 32) "hello")
            (data (i32.const 64) "\04\07\00\00\00\00\00\00\00")
            (func (export "skein_scalar") (param i32 i32) (result i64)
              (drop (call $log_debug (i32.const 32) (i32.const 5)))
              (i64.or
                (i64.shl (i64.extend_i32_u (i32.const 64)) (i64.const 32))
                (i64.extend_i32_u (i32.const 9))
              )
            )
        )"#,
    )
    .unwrap()
}

fn oversized_output_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
            (memory (export "memory") 1 1)
            (func (export "skein_alloc") (param i32) (result i32)
              (i32.const 0)
            )
            (func (export "skein_scalar") (param i32 i32) (result i64)
              (i64.or
                (i64.shl (i64.extend_i32_u (i32.const 0)) (i64.const 32))
                (i64.extend_i32_u (i32.const 100))
              )
            )
        )"#,
    )
    .unwrap()
}

fn growth_failure_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
            (memory (export "memory") 1 2)
            (func (export "skein_alloc") (param i32) (result i32)
              (i32.const 0)
            )
            (data (i32.const 16) "\04\01\00\00\00\00\00\00\00")
            (func (export "skein_scalar") (param i32 i32) (result i64)
              (local $prev i32)
              (local.set $prev (memory.grow (i32.const 1)))
              (if (i32.eq (local.get $prev) (i32.const -1))
                (then unreachable)
              )
              (i64.or
                (i64.shl (i64.extend_i32_u (i32.const 16)) (i64.const 32))
                (i64.extend_i32_u (i32.const 9))
              )
            )
        )"#,
    )
    .unwrap()
}

#[test]
fn scalar_udf_executes_constant_value() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request("const42", &constant_u64_module(42), vec![], 64 * 1024, 64),
            1,
        )
        .unwrap();

    let result = execute_scalar_udf(&catalog, &mut store, "const42", &[WasmValue::U64(1)]).unwrap();
    assert_eq!(result.value, WasmValue::U64(42));
    assert!(result.logs.is_empty());
}

#[test]
fn scalar_udf_rejects_disallowed_hostcall() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request("log_no", &log_debug_module(), vec![], 64 * 1024, 64),
            1,
        )
        .unwrap();

    let err = execute_scalar_udf(&catalog, &mut store, "log_no", &[]).unwrap_err();
    assert!(matches!(err, WasmUdfError::HostcallNotAllowed(name) if name == "log.debug"));
}

#[test]
fn scalar_udf_allows_log_debug_hostcall_when_declared() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request(
                "log_yes",
                &log_debug_module(),
                vec!["log.debug"],
                64 * 1024,
                64,
            ),
            1,
        )
        .unwrap();

    let result = execute_scalar_udf(&catalog, &mut store, "log_yes", &[]).unwrap();
    assert_eq!(result.value, WasmValue::U64(7));
    assert_eq!(result.logs, vec!["hello".to_string()]);
}

#[test]
fn scalar_udf_enforces_max_output_bytes() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request("big", &oversized_output_module(), vec![], 64 * 1024, 8),
            1,
        )
        .unwrap();

    let err = execute_scalar_udf(&catalog, &mut store, "big", &[]).unwrap_err();
    assert!(matches!(
        err,
        WasmUdfError::OutputTooLarge { len: 100, max: 8 }
    ));
}

#[test]
fn scalar_udf_memory_growth_beyond_limit_fails() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request("grow", &growth_failure_module(), vec![], 64 * 1024, 64),
            1,
        )
        .unwrap();

    let err = execute_scalar_udf(&catalog, &mut store, "grow", &[]).unwrap_err();
    assert!(matches!(err, WasmUdfError::Execution(_)));
}
