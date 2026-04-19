use std::time::Duration;

use skeindb_core::valuestore::{ValueStore, ValueStoreConfig};
use skeindb_core::wasm_catalog::{
    WasmModuleCapabilities, WasmModuleCatalog, WasmModuleInstallRequest, WasmModuleKind,
    WASM_UDF_ABI_V1,
};
use skeindb_core::wasm_udf::{
    execute_aggregate_udf, execute_scalar_udf, execute_scalar_udf_with_options, execute_table_udf,
    ScalarUdfExecutionOptions, TableUdfExecutionResult, WasmUdfError, WasmValue,
};

#[derive(Clone, Copy)]
struct InstallLimits {
    max_fuel: u64,
    max_memory_bytes: u64,
    max_output_bytes: u64,
}

fn install_request(
    module_id: &str,
    kind: WasmModuleKind,
    entrypoint: &str,
    bytes: &[u8],
    allowed_hostcalls: Vec<&str>,
    limits: InstallLimits,
) -> WasmModuleInstallRequest {
    WasmModuleInstallRequest {
        module_id: module_id.to_string(),
        name: Some(module_id.to_string()),
        kind,
        abi: WASM_UDF_ABI_V1.to_string(),
        entrypoint: entrypoint.to_string(),
        capabilities: WasmModuleCapabilities {
            allowed_hostcalls: allowed_hostcalls.into_iter().map(str::to_string).collect(),
            allowed_tables: Vec::new(),
            deterministic: true,
            max_fuel: limits.max_fuel,
            max_memory_bytes: limits.max_memory_bytes,
            max_output_bytes: limits.max_output_bytes,
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

fn infinite_loop_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
                        (memory (export "memory") 1 1)
                        (func (export "skein_alloc") (param i32) (result i32)
                            (i32.const 0)
                        )
                        (func (export "skein_scalar") (param i32 i32) (result i64)
                            (loop $spin
                                (br $spin)
                            )
                            (i64.const 0)
                        )
                )"#,
    )
    .unwrap()
}

fn aggregate_sum_module() -> Vec<u8> {
    wat::parse_str(
                r#"(module
                        (memory (export "memory") 1 1)
                        (func (export "skein_alloc") (param i32) (result i32)
                            (i32.const 0)
                        )
                        (func (export "skein_aggregate") (param $ptr i32) (param $len i32) (result i64)
                            (local $cursor i32)
                            (local $remaining i32)
                            (local $sum i64)
                            (local.set $remaining (i32.load8_u (local.get $ptr)))
                            (local.set $cursor (i32.add (local.get $ptr) (i32.const 1)))
                            (block $done
                                (loop $rows
                                    (br_if $done (i32.eqz (local.get $remaining)))
                                    (local.set $cursor (i32.add (local.get $cursor) (i32.const 1)))
                                    (local.set $cursor (i32.add (local.get $cursor) (i32.const 1)))
                                    (local.set $sum
                                        (i64.add (local.get $sum) (i64.load (local.get $cursor))))
                                    (local.set $cursor (i32.add (local.get $cursor) (i32.const 8)))
                                    (local.set $remaining (i32.sub (local.get $remaining) (i32.const 1)))
                                    (br $rows)
                                )
                            )
                            (i32.store8 (i32.const 256) (i32.const 4))
                            (i64.store (i32.const 257) (local.get $sum))
                            (i64.or
                                (i64.shl (i64.extend_i32_u (i32.const 256)) (i64.const 32))
                                (i64.extend_i32_u (i32.const 9))
                            )
                        )
                )"#,
        )
        .unwrap()
}

fn table_rows_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
                        (memory (export "memory") 1 1)
                        (func (export "skein_alloc") (param i32) (result i32)
                            (i32.const 0)
                        )
                        (func (export "skein_table") (param $ptr i32) (param $len i32) (result i64)
                            (local $first i64)
                            (local $second i64)
                            (local.set $first (i64.load (i32.add (local.get $ptr) (i32.const 2))))
                            (local.set $second (i64.load (i32.add (local.get $ptr) (i32.const 11))))
                            (i32.store8 (i32.const 256) (i32.const 2))
                            (i32.store8 (i32.const 257) (i32.const 1))
                            (i32.store8 (i32.const 258) (i32.const 4))
                            (i64.store (i32.const 259) (local.get $first))
                            (i32.store8 (i32.const 267) (i32.const 1))
                            (i32.store8 (i32.const 268) (i32.const 4))
                            (i64.store (i32.const 269) (local.get $second))
                            (i64.or
                                (i64.shl (i64.extend_i32_u (i32.const 256)) (i64.const 32))
                                (i64.extend_i32_u (i32.const 21))
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
            install_request(
                "const42",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &constant_u64_module(42),
                vec![],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
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
            install_request(
                "log_no",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &log_debug_module(),
                vec![],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
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
                WasmModuleKind::Scalar,
                "skein_scalar",
                &log_debug_module(),
                vec!["log.debug"],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
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
            install_request(
                "big",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &oversized_output_module(),
                vec![],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 8,
                },
            ),
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
            install_request(
                "grow",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &growth_failure_module(),
                vec![],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
            1,
        )
        .unwrap();

    let err = execute_scalar_udf(&catalog, &mut store, "grow", &[]).unwrap_err();
    assert!(matches!(err, WasmUdfError::Execution(_)));
}

#[test]
fn scalar_udf_cancels_when_fuel_budget_exhausted() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request(
                "spin_fuel",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &infinite_loop_module(),
                vec![],
                InstallLimits {
                    max_fuel: 1_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
            1,
        )
        .unwrap();

    let err = execute_scalar_udf(&catalog, &mut store, "spin_fuel", &[]).unwrap_err();
    assert!(matches!(
        err,
        WasmUdfError::FuelExhausted { max_fuel: 1_000 }
    ));
}

#[test]
fn scalar_udf_cancels_when_wall_clock_timeout_expires() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request(
                "spin_time",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &infinite_loop_module(),
                vec![],
                InstallLimits {
                    max_fuel: 0,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
            1,
        )
        .unwrap();

    let err = execute_scalar_udf_with_options(
        &catalog,
        &mut store,
        "spin_time",
        &[],
        ScalarUdfExecutionOptions {
            wall_clock_timeout: Some(Duration::from_millis(10)),
        },
    )
    .unwrap_err();
    assert!(matches!(
        err,
        WasmUdfError::TimeoutExceeded { timeout_ms: 10 }
    ));
}

#[test]
fn scalar_udf_recovers_after_cancelled_call() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request(
                "spin_once",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &infinite_loop_module(),
                vec![],
                InstallLimits {
                    max_fuel: 1_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
            1,
        )
        .unwrap();
    catalog
        .install(
            &mut store,
            install_request(
                "const_after",
                WasmModuleKind::Scalar,
                "skein_scalar",
                &constant_u64_module(42),
                vec![],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
            1,
        )
        .unwrap();

    let err = execute_scalar_udf(&catalog, &mut store, "spin_once", &[]).unwrap_err();
    assert!(matches!(
        err,
        WasmUdfError::FuelExhausted { max_fuel: 1_000 }
    ));

    let result = execute_scalar_udf(&catalog, &mut store, "const_after", &[]).unwrap();
    assert_eq!(result.value, WasmValue::U64(42));
}

#[test]
fn aggregate_udf_sums_row_values() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request(
                "sum_rows",
                WasmModuleKind::Aggregate,
                "skein_aggregate",
                &aggregate_sum_module(),
                vec![],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
            1,
        )
        .unwrap();

    let rows = vec![
        vec![WasmValue::U64(3)],
        vec![WasmValue::U64(5)],
        vec![WasmValue::U64(8)],
    ];
    let result = execute_aggregate_udf(&catalog, &mut store, "sum_rows", &rows).unwrap();
    assert_eq!(result.value, WasmValue::U64(16));
    assert!(result.logs.is_empty());
}

#[test]
fn table_udf_returns_rows() {
    let mut store = ValueStore::new(ValueStoreConfig::default());
    let mut catalog = WasmModuleCatalog::new();
    catalog
        .install(
            &mut store,
            install_request(
                "rows_from_args",
                WasmModuleKind::Table,
                "skein_table",
                &table_rows_module(),
                vec![],
                InstallLimits {
                    max_fuel: 10_000,
                    max_memory_bytes: 64 * 1024,
                    max_output_bytes: 64,
                },
            ),
            1,
        )
        .unwrap();

    let result = execute_table_udf(
        &catalog,
        &mut store,
        "rows_from_args",
        &[WasmValue::U64(2), WasmValue::U64(9)],
    )
    .unwrap();
    assert_eq!(
        result,
        TableUdfExecutionResult {
            rows: vec![vec![WasmValue::U64(2)], vec![WasmValue::U64(9)]],
            logs: Vec::new(),
        }
    );
}
