//! Scalar, aggregate, and table Wasm UDF execution sandbox with resource
//! limits and safe cancellation (T083).
//!
//! Scope for this slice:
//!
//! - execute scalar, aggregate, and table UDF modules stored in
//!   [`crate::wasm_catalog::WasmModuleCatalog`] and referenced via
//!   [`crate::valuestore::ValueStore`]
//! - enforce memory/output-byte limits and optional fuel budgets from the
//!   module capabilities
//! - enforce a bounded wall-clock timeout via epoch interruption
//! - expose a tiny capability-gated hostcall surface (`log.debug`)
//! - keep execution side-effect free by default; no filesystem/network/clock/
//!   randomness imports are exposed
//!
//! Not in scope here:
//! - host table access or streaming/table-iterator ABIs

use std::collections::HashSet;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use thiserror::Error;
use wasmtime::{Caller, Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};

use crate::valuestore::ValueStore;
use crate::wasm_catalog::{
    WasmCatalogError, WasmModuleCatalog, WasmModuleKind, WasmModuleRecord, WASM_UDF_ABI_V1,
};
use crate::{decode_varu, encode_varu, CoreError};

const DEFAULT_MAX_MEMORY_BYTES: u64 = 1024 * 1024;
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 64 * 1024;
const DEFAULT_WALL_CLOCK_TIMEOUT_MS: u64 = 1_000;

#[derive(Debug, Clone, PartialEq)]
pub enum WasmValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
    String(String),
}

#[derive(Debug, Error)]
pub enum WasmAbiError {
    #[error("unexpected EOF while decoding Wasm ABI value")]
    UnexpectedEof,
    #[error("unknown Wasm ABI tag {0:#04x}")]
    UnknownTag(u8),
    #[error("invalid UTF-8 in Wasm ABI string")]
    InvalidUtf8,
    #[error("trailing bytes after Wasm ABI value")]
    TrailingBytes,
    #[error(transparent)]
    Core(#[from] CoreError),
}

#[derive(Debug, Error)]
pub enum WasmUdfError {
    #[error("module not found: {0}")]
    ModuleNotFound(String),
    #[error("module {module_id} has wrong UDF kind: expected {expected:?}, got {actual:?}")]
    WrongModuleKind {
        module_id: String,
        expected: WasmModuleKind,
        actual: WasmModuleKind,
    },
    #[error("unsupported Wasm UDF ABI: {0}")]
    UnsupportedAbi(String),
    #[error("unsupported import: {module}.{name}")]
    UnsupportedImport { module: String, name: String },
    #[error("hostcall not allowed by module capabilities: {0}")]
    HostcallNotAllowed(String),
    #[error("module must export memory")]
    MissingMemory,
    #[error("module must export skein_alloc(len: u32) -> u32")]
    MissingAllocator,
    #[error("module missing scalar entrypoint export: {0}")]
    MissingEntrypoint(String),
    #[error("scalar UDF exhausted max_fuel ({max_fuel})")]
    FuelExhausted { max_fuel: u64 },
    #[error("scalar UDF exceeded wall-clock timeout ({timeout_ms} ms)")]
    TimeoutExceeded { timeout_ms: u64 },
    #[error("scalar UDF output exceeds max_output_bytes ({len} > {max})")]
    OutputTooLarge { len: u32, max: u64 },
    #[error("failed to access guest memory")]
    MemoryAccess,
    #[error("wasm execution failed: {0}")]
    Execution(String),
    #[error(transparent)]
    Catalog(#[from] WasmCatalogError),
    #[error(transparent)]
    Abi(#[from] WasmAbiError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScalarUdfExecutionResult {
    pub value: WasmValue,
    pub logs: Vec<String>,
}

pub type AggregateUdfExecutionResult = ScalarUdfExecutionResult;

#[derive(Debug, Clone, PartialEq)]
pub struct TableUdfExecutionResult {
    pub rows: Vec<Vec<WasmValue>>,
    pub logs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarUdfExecutionOptions {
    pub wall_clock_timeout: Option<Duration>,
}

pub type WasmUdfExecutionOptions = ScalarUdfExecutionOptions;

impl Default for ScalarUdfExecutionOptions {
    fn default() -> Self {
        Self {
            wall_clock_timeout: Some(Duration::from_millis(DEFAULT_WALL_CLOCK_TIMEOUT_MS)),
        }
    }
}

pub fn encode_scalar_args(args: &[WasmValue]) -> Vec<u8> {
    let mut out = encode_varu(args.len() as u64);
    for value in args {
        encode_value(&mut out, value);
    }
    out
}

pub fn decode_scalar_args(bytes: &[u8]) -> Result<Vec<WasmValue>, WasmAbiError> {
    let mut cursor = bytes;
    let count = decode_varu(&mut cursor)? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode_value(&mut cursor)?);
    }
    if !cursor.is_empty() {
        return Err(WasmAbiError::TrailingBytes);
    }
    Ok(values)
}

pub fn encode_scalar_result(value: &WasmValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(&mut out, value);
    out
}

pub fn decode_scalar_result(bytes: &[u8]) -> Result<WasmValue, WasmAbiError> {
    let mut cursor = bytes;
    let value = decode_value(&mut cursor)?;
    if !cursor.is_empty() {
        return Err(WasmAbiError::TrailingBytes);
    }
    Ok(value)
}

pub fn encode_rows(rows: &[Vec<WasmValue>]) -> Vec<u8> {
    let mut out = encode_varu(rows.len() as u64);
    for row in rows {
        out.extend_from_slice(&encode_varu(row.len() as u64));
        for value in row {
            encode_value(&mut out, value);
        }
    }
    out
}

pub fn decode_rows(bytes: &[u8]) -> Result<Vec<Vec<WasmValue>>, WasmAbiError> {
    let mut cursor = bytes;
    let row_count = decode_varu(&mut cursor)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let value_count = decode_varu(&mut cursor)? as usize;
        let mut row = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            row.push(decode_value(&mut cursor)?);
        }
        rows.push(row);
    }
    if !cursor.is_empty() {
        return Err(WasmAbiError::TrailingBytes);
    }
    Ok(rows)
}

pub fn execute_scalar_udf(
    catalog: &WasmModuleCatalog,
    value_store: &mut ValueStore,
    module_id: &str,
    args: &[WasmValue],
) -> Result<ScalarUdfExecutionResult, WasmUdfError> {
    execute_scalar_udf_with_options(
        catalog,
        value_store,
        module_id,
        args,
        ScalarUdfExecutionOptions::default(),
    )
}

pub fn execute_scalar_udf_with_options(
    catalog: &WasmModuleCatalog,
    value_store: &mut ValueStore,
    module_id: &str,
    args: &[WasmValue],
    options: WasmUdfExecutionOptions,
) -> Result<ScalarUdfExecutionResult, WasmUdfError> {
    let raw = execute_module_entrypoint(
        catalog,
        value_store,
        module_id,
        WasmModuleKind::Scalar,
        &encode_scalar_args(args),
        options,
    )?;
    Ok(ScalarUdfExecutionResult {
        value: decode_scalar_result(&raw.output)?,
        logs: raw.logs,
    })
}

pub fn execute_aggregate_udf(
    catalog: &WasmModuleCatalog,
    value_store: &mut ValueStore,
    module_id: &str,
    rows: &[Vec<WasmValue>],
) -> Result<AggregateUdfExecutionResult, WasmUdfError> {
    execute_aggregate_udf_with_options(
        catalog,
        value_store,
        module_id,
        rows,
        ScalarUdfExecutionOptions::default(),
    )
}

pub fn execute_aggregate_udf_with_options(
    catalog: &WasmModuleCatalog,
    value_store: &mut ValueStore,
    module_id: &str,
    rows: &[Vec<WasmValue>],
    options: WasmUdfExecutionOptions,
) -> Result<AggregateUdfExecutionResult, WasmUdfError> {
    let raw = execute_module_entrypoint(
        catalog,
        value_store,
        module_id,
        WasmModuleKind::Aggregate,
        &encode_rows(rows),
        options,
    )?;
    Ok(AggregateUdfExecutionResult {
        value: decode_scalar_result(&raw.output)?,
        logs: raw.logs,
    })
}

pub fn execute_table_udf(
    catalog: &WasmModuleCatalog,
    value_store: &mut ValueStore,
    module_id: &str,
    args: &[WasmValue],
) -> Result<TableUdfExecutionResult, WasmUdfError> {
    execute_table_udf_with_options(
        catalog,
        value_store,
        module_id,
        args,
        ScalarUdfExecutionOptions::default(),
    )
}

pub fn execute_table_udf_with_options(
    catalog: &WasmModuleCatalog,
    value_store: &mut ValueStore,
    module_id: &str,
    args: &[WasmValue],
    options: WasmUdfExecutionOptions,
) -> Result<TableUdfExecutionResult, WasmUdfError> {
    let raw = execute_module_entrypoint(
        catalog,
        value_store,
        module_id,
        WasmModuleKind::Table,
        &encode_scalar_args(args),
        options,
    )?;
    Ok(TableUdfExecutionResult {
        rows: decode_rows(&raw.output)?,
        logs: raw.logs,
    })
}

fn execute_module_entrypoint(
    catalog: &WasmModuleCatalog,
    value_store: &mut ValueStore,
    module_id: &str,
    expected_kind: WasmModuleKind,
    input: &[u8],
    options: WasmUdfExecutionOptions,
) -> Result<RawUdfExecutionResult, WasmUdfError> {
    let record = resolve_record(catalog, module_id, expected_kind)?;
    let wasm_bytes = catalog.module_bytes(value_store, module_id)?;
    let max_memory_bytes = if record.capabilities.max_memory_bytes == 0 {
        DEFAULT_MAX_MEMORY_BYTES
    } else {
        record.capabilities.max_memory_bytes
    };
    let max_output_bytes = if record.capabilities.max_output_bytes == 0 {
        DEFAULT_MAX_OUTPUT_BYTES
    } else {
        record.capabilities.max_output_bytes
    };
    let max_fuel = record.capabilities.max_fuel;
    let allowed_hostcalls: HashSet<String> = record
        .capabilities
        .allowed_hostcalls
        .iter()
        .cloned()
        .collect();

    let mut config = Config::new();
    config.wasm_memory64(false);
    config.wasm_multi_memory(false);
    config.consume_fuel(max_fuel > 0);
    config.epoch_interruption(options.wall_clock_timeout.is_some());
    let engine = Engine::new(&config).map_err(|e| WasmUdfError::Execution(e.to_string()))?;
    let module =
        Module::new(&engine, &wasm_bytes).map_err(|e| WasmUdfError::Execution(e.to_string()))?;

    validate_imports(&module, &allowed_hostcalls)?;

    let mut linker = Linker::new(&engine);
    if allowed_hostcalls.contains("log.debug") {
        linker
            .func_wrap(
                "skein",
                "log_debug",
                move |mut caller: Caller<'_, SandboxState>,
                      ptr: u32,
                      len: u32|
                      -> Result<i32, wasmtime::Error> {
                    if u64::from(len) > max_output_bytes {
                        return Err(wasmtime::Error::msg(
                            "log.debug payload exceeds max_output_bytes",
                        ));
                    }
                    let memory = caller
                        .get_export("memory")
                        .and_then(|e| e.into_memory())
                        .ok_or_else(|| wasmtime::Error::msg("missing memory export"))?;
                    let mut buf = vec![0u8; len as usize];
                    memory
                        .read(&caller, ptr as usize, &mut buf)
                        .map_err(|_| wasmtime::Error::msg("failed to read guest memory"))?;
                    caller
                        .data_mut()
                        .logs
                        .push(String::from_utf8_lossy(&buf).into_owned());
                    Ok(0)
                },
            )
            .map_err(|e| WasmUdfError::Execution(e.to_string()))?;
    }

    let limits = StoreLimitsBuilder::new()
        .memory_size(max_memory_bytes as usize)
        .build();
    let mut store = Store::new(
        &engine,
        SandboxState {
            limits,
            logs: Vec::new(),
        },
    );
    store.limiter(|state| &mut state.limits);
    if max_fuel > 0 {
        store
            .set_fuel(max_fuel)
            .map_err(|e| WasmUdfError::Execution(e.to_string()))?;
    }
    let timeout_guard = if let Some(timeout) = options.wall_clock_timeout {
        store.epoch_deadline_trap();
        store.set_epoch_deadline(1);
        Some(TimeoutGuard::spawn(engine.clone(), timeout))
    } else {
        None
    };

    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| classify_execution_error(e, max_fuel, options.wall_clock_timeout))?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or(WasmUdfError::MissingMemory)?;
    let alloc = instance
        .get_typed_func::<u32, u32>(&mut store, "skein_alloc")
        .map_err(|_| WasmUdfError::MissingAllocator)?;
    let entrypoint = instance
        .get_typed_func::<(u32, u32), u64>(&mut store, &record.entrypoint)
        .map_err(|_| WasmUdfError::MissingEntrypoint(record.entrypoint.clone()))?;

    let input_len = u32::try_from(input.len()).map_err(|_| WasmUdfError::MemoryAccess)?;
    let input_ptr = alloc
        .call(&mut store, input_len)
        .map_err(|e| classify_execution_error(e, max_fuel, options.wall_clock_timeout))?;
    memory
        .write(&mut store, input_ptr as usize, input)
        .map_err(|_| WasmUdfError::MemoryAccess)?;

    let packed = entrypoint
        .call(&mut store, (input_ptr, input_len))
        .map_err(|e| classify_execution_error(e, max_fuel, options.wall_clock_timeout))?;
    let output_ptr = (packed >> 32) as u32;
    let output_len = packed as u32;
    if u64::from(output_len) > max_output_bytes {
        return Err(WasmUdfError::OutputTooLarge {
            len: output_len,
            max: max_output_bytes,
        });
    }

    let mut output = vec![0u8; output_len as usize];
    memory
        .read(&store, output_ptr as usize, &mut output)
        .map_err(|_| WasmUdfError::MemoryAccess)?;
    let logs = store.data().logs.clone();
    drop(timeout_guard);

    Ok(RawUdfExecutionResult { output, logs })
}

fn resolve_record(
    catalog: &WasmModuleCatalog,
    module_id: &str,
    expected_kind: WasmModuleKind,
) -> Result<WasmModuleRecord, WasmUdfError> {
    let record = catalog
        .get(module_id)
        .cloned()
        .ok_or_else(|| WasmUdfError::ModuleNotFound(module_id.to_string()))?;
    if record.kind != expected_kind {
        return Err(WasmUdfError::WrongModuleKind {
            module_id: record.module_id.clone(),
            expected: expected_kind,
            actual: record.kind.clone(),
        });
    }
    if record.abi != WASM_UDF_ABI_V1 {
        return Err(WasmUdfError::UnsupportedAbi(record.abi));
    }
    Ok(record)
}

struct RawUdfExecutionResult {
    output: Vec<u8>,
    logs: Vec<String>,
}

fn validate_imports(
    module: &Module,
    allowed_hostcalls: &HashSet<String>,
) -> Result<(), WasmUdfError> {
    for import in module.imports() {
        let import_module = import.module().to_string();
        let import_name = import.name().to_string();
        if import_module != "skein" {
            return Err(WasmUdfError::UnsupportedImport {
                module: import_module,
                name: import_name,
            });
        }
        match import_name.as_str() {
            "log_debug" => {
                if !allowed_hostcalls.contains("log.debug") {
                    return Err(WasmUdfError::HostcallNotAllowed("log.debug".to_string()));
                }
            }
            _ => {
                return Err(WasmUdfError::UnsupportedImport {
                    module: import_module,
                    name: import_name,
                });
            }
        }
    }
    Ok(())
}

fn encode_value(out: &mut Vec<u8>, value: &WasmValue) {
    match value {
        WasmValue::Null => out.push(0),
        WasmValue::Bool(false) => out.push(1),
        WasmValue::Bool(true) => out.push(2),
        WasmValue::I64(v) => {
            out.push(3);
            out.extend_from_slice(&v.to_le_bytes());
        }
        WasmValue::U64(v) => {
            out.push(4);
            out.extend_from_slice(&v.to_le_bytes());
        }
        WasmValue::F64(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_le_bytes());
        }
        WasmValue::Bytes(bytes) => {
            out.push(6);
            out.extend_from_slice(&encode_varu(bytes.len() as u64));
            out.extend_from_slice(bytes);
        }
        WasmValue::String(s) => {
            out.push(7);
            out.extend_from_slice(&encode_varu(s.len() as u64));
            out.extend_from_slice(s.as_bytes());
        }
    }
}

fn decode_value(cursor: &mut &[u8]) -> Result<WasmValue, WasmAbiError> {
    if cursor.is_empty() {
        return Err(WasmAbiError::UnexpectedEof);
    }
    let tag = cursor[0];
    *cursor = &cursor[1..];
    match tag {
        0 => Ok(WasmValue::Null),
        1 => Ok(WasmValue::Bool(false)),
        2 => Ok(WasmValue::Bool(true)),
        3 => Ok(WasmValue::I64(take_i64(cursor)?)),
        4 => Ok(WasmValue::U64(take_u64(cursor)?)),
        5 => Ok(WasmValue::F64(f64::from_le_bytes(take_array(cursor)?))),
        6 => Ok(WasmValue::Bytes(take_len_prefixed(cursor)?)),
        7 => {
            let bytes = take_len_prefixed(cursor)?;
            let s = String::from_utf8(bytes).map_err(|_| WasmAbiError::InvalidUtf8)?;
            Ok(WasmValue::String(s))
        }
        other => Err(WasmAbiError::UnknownTag(other)),
    }
}

fn take_i64(cursor: &mut &[u8]) -> Result<i64, WasmAbiError> {
    Ok(i64::from_le_bytes(take_array(cursor)?))
}

fn take_u64(cursor: &mut &[u8]) -> Result<u64, WasmAbiError> {
    Ok(u64::from_le_bytes(take_array(cursor)?))
}

fn take_array<const N: usize>(cursor: &mut &[u8]) -> Result<[u8; N], WasmAbiError> {
    if cursor.len() < N {
        return Err(WasmAbiError::UnexpectedEof);
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&cursor[..N]);
    *cursor = &cursor[N..];
    Ok(out)
}

fn take_len_prefixed(cursor: &mut &[u8]) -> Result<Vec<u8>, WasmAbiError> {
    let len = decode_varu(cursor)? as usize;
    if cursor.len() < len {
        return Err(WasmAbiError::UnexpectedEof);
    }
    let bytes = cursor[..len].to_vec();
    *cursor = &cursor[len..];
    Ok(bytes)
}

fn classify_execution_error(
    error: wasmtime::Error,
    max_fuel: u64,
    wall_clock_timeout: Option<Duration>,
) -> WasmUdfError {
    if let Some(trap) = error.downcast_ref::<wasmtime::Trap>() {
        match *trap {
            wasmtime::Trap::OutOfFuel => {
                return WasmUdfError::FuelExhausted { max_fuel };
            }
            wasmtime::Trap::Interrupt => {
                return WasmUdfError::TimeoutExceeded {
                    timeout_ms: wall_clock_timeout.map(duration_to_ms).unwrap_or(0),
                };
            }
            _ => {}
        }
    }
    WasmUdfError::Execution(error.to_string())
}

fn duration_to_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

struct SandboxState {
    limits: StoreLimits,
    logs: Vec<String>,
}

struct TimeoutGuard {
    done_tx: Option<mpsc::Sender<()>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TimeoutGuard {
    fn spawn(engine: Engine, timeout: Duration) -> Self {
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || match done_rx.recv_timeout(timeout) {
            Err(RecvTimeoutError::Timeout) => engine.increment_epoch(),
            Err(RecvTimeoutError::Disconnected) | Ok(()) => {}
        });
        Self {
            done_tx: Some(done_tx),
            handle: Some(handle),
        }
    }
}

impl Drop for TimeoutGuard {
    fn drop(&mut self) {
        if let Some(done_tx) = self.done_tx.take() {
            let _ = done_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_args_roundtrip() {
        let values = vec![
            WasmValue::Null,
            WasmValue::Bool(true),
            WasmValue::I64(-7),
            WasmValue::U64(42),
            WasmValue::F64(3.5),
            WasmValue::Bytes(vec![1, 2, 3]),
            WasmValue::String("hello".to_string()),
        ];
        let encoded = encode_scalar_args(&values);
        let decoded = decode_scalar_args(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn scalar_result_roundtrip() {
        let encoded = encode_scalar_result(&WasmValue::String("ok".to_string()));
        assert_eq!(
            decode_scalar_result(&encoded).unwrap(),
            WasmValue::String("ok".to_string())
        );
    }

    #[test]
    fn rows_roundtrip() {
        let rows = vec![
            vec![WasmValue::U64(1), WasmValue::String("a".to_string())],
            vec![WasmValue::Null, WasmValue::Bool(true)],
        ];
        let encoded = encode_rows(&rows);
        let decoded = decode_rows(&encoded).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn scalar_result_rejects_trailing_bytes() {
        let mut encoded = encode_scalar_result(&WasmValue::U64(5));
        encoded.push(0);
        assert!(matches!(
            decode_scalar_result(&encoded),
            Err(WasmAbiError::TrailingBytes)
        ));
    }

    #[test]
    fn rows_reject_trailing_bytes() {
        let mut encoded = encode_rows(&[vec![WasmValue::U64(5)]]);
        encoded.push(0);
        assert!(matches!(
            decode_rows(&encoded),
            Err(WasmAbiError::TrailingBytes)
        ));
    }
}
