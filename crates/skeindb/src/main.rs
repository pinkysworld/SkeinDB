use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use skeindb_skeinql::methods::{
    EdgeBundleRedaction, MaintenanceReplayExportParams, MaintenanceReplayImportParams,
    MaintenanceReplayRunParams, MaintenanceReplayRunResult, ReplayBundle,
};

mod engine;
mod nl_eval;
mod pg_wire;
mod quic;
mod server;
mod transport_bench;

#[derive(Debug)]
struct AuditVerifyReport {
    status: serde_json::Value,
    verify: serde_json::Value,
}

impl AuditVerifyReport {
    fn is_ok(&self) -> bool {
        self.verify
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    fn records_checked(&self) -> u64 {
        self.verify
            .get("records_checked")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    }

    fn elapsed_ms(&self) -> u64 {
        self.verify
            .get("elapsed_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    }

    fn chain_head_hash(&self) -> &str {
        self.verify
            .get("chain_head_hash")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("genesis")
    }

    fn anchor_count(&self) -> u64 {
        self.status
            .get("anchor_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    }

    fn last_verified_ms(&self) -> u64 {
        self.status
            .get("last_verified_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    }

    fn bad_index(&self) -> Option<u64> {
        self.verify
            .get("bad_index")
            .and_then(serde_json::Value::as_u64)
    }

    fn reason(&self) -> Option<&str> {
        self.verify
            .get("reason")
            .and_then(serde_json::Value::as_str)
    }
}

fn collect_audit_verify_report(data: &str) -> anyhow::Result<AuditVerifyReport> {
    let mut engine = engine::Engine::open(data)?;
    let verify = engine.maintenance_audit_verify();
    let status = engine.maintenance_audit_status();
    Ok(AuditVerifyReport { status, verify })
}

fn audit_verify_failure_message(report: &AuditVerifyReport) -> String {
    let reason = report.reason().unwrap_or("unknown");
    let bad_index = report
        .bad_index()
        .map(|idx| idx.to_string())
        .unwrap_or_else(|| "n/a".to_string());
    format!(
        "audit verification failed: reason={reason}, bad_index={bad_index}, records_checked={}, chain_head_hash={}",
        report.records_checked(),
        report.chain_head_hash(),
    )
}

fn replay_temp_dir(name: &str) -> anyhow::Result<PathBuf> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("skeindb_{name}_{}_{}", std::process::id(), suffix));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn run_replay_bundle_in_workspace(
    bundle: &ReplayBundle,
    workspace_root: &std::path::Path,
    workspace_id: &str,
) -> anyhow::Result<MaintenanceReplayRunResult> {
    let engine =
        engine::Engine::open_with_storage_mode_name(workspace_root, &bundle.manifest.storage_mode)?;
    let _ = engine.maintenance_replay_import(MaintenanceReplayImportParams {
        bundle: bundle.clone(),
        workspace_id: Some(workspace_id.to_string()),
    })?;
    engine.maintenance_replay_run(MaintenanceReplayRunParams {
        workspace_id: workspace_id.to_string(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplayCiThresholds {
    max_p95_delta_ms: i64,
    max_p99_delta_ms: i64,
    max_span_delta_ms: i64,
    max_disk_bytes_delta: i64,
    max_missing_hot_tables_delta: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplayCiCheck {
    name: String,
    baseline: i64,
    candidate: i64,
    delta: i64,
    threshold: i64,
    ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplayCiSummary {
    bundle_id: String,
    ok: bool,
    checksum_match: bool,
    replayed_tables: u64,
    replayed_rows: u64,
    replayed_changes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplayCiComparisonReport {
    ok: bool,
    baseline: ReplayCiSummary,
    candidate: ReplayCiSummary,
    thresholds: ReplayCiThresholds,
    checks: Vec<ReplayCiCheck>,
    failures: Vec<String>,
}

fn replay_ci_summary(result: &MaintenanceReplayRunResult) -> ReplayCiSummary {
    ReplayCiSummary {
        bundle_id: result.bundle_id.clone(),
        ok: result.ok,
        checksum_match: result
            .performance_report
            .as_ref()
            .map(|report| report.checksum_match)
            .unwrap_or(false),
        replayed_tables: result.replayed_tables,
        replayed_rows: result.replayed_rows,
        replayed_changes: result.replayed_changes,
    }
}

fn replay_ci_check(name: &str, baseline: i64, candidate: i64, threshold: i64) -> ReplayCiCheck {
    let delta = candidate - baseline;
    ReplayCiCheck {
        name: name.to_string(),
        baseline,
        candidate,
        delta,
        threshold,
        ok: delta <= threshold,
    }
}

fn compare_replay_run_reports(
    baseline: &MaintenanceReplayRunResult,
    candidate: &MaintenanceReplayRunResult,
    thresholds: ReplayCiThresholds,
) -> anyhow::Result<ReplayCiComparisonReport> {
    let baseline_perf = baseline
        .performance_report
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("baseline replay report is missing performance_report"))?;
    let candidate_perf = candidate
        .performance_report
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("candidate replay report is missing performance_report"))?;

    let mut failures = Vec::new();
    if !baseline.ok {
        failures.push("baseline replay run failed correctness verification".to_string());
    }
    if !candidate.ok {
        failures.push("candidate replay run failed correctness verification".to_string());
    }
    if !baseline_perf.checksum_match {
        failures.push("baseline performance checksum did not match".to_string());
    }
    if !candidate_perf.checksum_match {
        failures.push("candidate performance checksum did not match".to_string());
    }

    let checks = vec![
        replay_ci_check(
            "p95_inter_event_ms_delta",
            baseline_perf.timing.p95_inter_event_ms_delta,
            candidate_perf.timing.p95_inter_event_ms_delta,
            thresholds.max_p95_delta_ms,
        ),
        replay_ci_check(
            "p99_inter_event_ms_delta",
            baseline_perf.timing.p99_inter_event_ms_delta,
            candidate_perf.timing.p99_inter_event_ms_delta,
            thresholds.max_p99_delta_ms,
        ),
        replay_ci_check(
            "span_ms_delta",
            baseline_perf.timing.span_ms_delta,
            candidate_perf.timing.span_ms_delta,
            thresholds.max_span_delta_ms,
        ),
        replay_ci_check(
            "disk_bytes_delta",
            baseline_perf.storage.disk_bytes_delta,
            candidate_perf.storage.disk_bytes_delta,
            thresholds.max_disk_bytes_delta,
        ),
        replay_ci_check(
            "missing_hot_table_count",
            baseline_perf.cache_warm.missing_hot_table_count as i64,
            candidate_perf.cache_warm.missing_hot_table_count as i64,
            thresholds.max_missing_hot_tables_delta,
        ),
    ];

    for check in checks.iter().filter(|check| !check.ok) {
        failures.push(format!(
            "{} regressed by {} (threshold {})",
            check.name, check.delta, check.threshold
        ));
    }

    Ok(ReplayCiComparisonReport {
        ok: failures.is_empty(),
        baseline: replay_ci_summary(baseline),
        candidate: replay_ci_summary(candidate),
        thresholds,
        checks,
        failures,
    })
}

fn read_replay_run_report(path: &str) -> anyhow::Result<MaintenanceReplayRunResult> {
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn write_json_report<T: Serialize>(report: &T, out: Option<&str>) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(report)?;
    if let Some(out) = out {
        let out_path = PathBuf::from(out);
        if let Some(parent) = out_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        fs::write(out_path, bytes)?;
    } else {
        println!("{}", String::from_utf8(bytes)?);
    }
    Ok(())
}

fn run_info_command(data: &str, json: bool) -> anyhow::Result<()> {
    let data_path = PathBuf::from(data);
    let exists = data_path.exists();
    let mut databases: Vec<(String, usize)> = Vec::new();
    let mut total_tables = 0usize;
    let mut storage_mode = String::from("(unopened)");
    if exists {
        match engine::Engine::open(&data_path) {
            Ok(engine) => {
                storage_mode = engine.storage_mode_name().to_string();
                for db in engine.list_databases() {
                    let tables = engine.list_tables(&db).unwrap_or_default();
                    total_tables += tables.len();
                    databases.push((db, tables.len()));
                }
            }
            Err(err) => {
                if json {
                    let payload = serde_json::json!({
                        "ok": false,
                        "data_dir": data_path.display().to_string(),
                        "error": err.to_string(),
                    });
                    println!("{}", serde_json::to_string_pretty(&payload)?);
                    return Ok(());
                } else {
                    eprintln!("warning: failed to open data dir: {err}");
                }
            }
        }
    }

    if json {
        let payload = serde_json::json!({
            "ok": true,
            "version": env!("CARGO_PKG_VERSION"),
            "data_dir": data_path.display().to_string(),
            "data_dir_exists": exists,
            "storage_mode": storage_mode,
            "databases": databases.iter().map(|(name, tables)| serde_json::json!({
                "name": name,
                "table_count": tables,
            })).collect::<Vec<_>>(),
            "total_databases": databases.len(),
            "total_tables": total_tables,
            "default_ports": {
                "http": 8080u16,
                "mysql": 3306u16,
                "pg": 5432u16,
                "cluster": 9090u16,
            },
            "docs": {
                "mysql_compat": "docs/MYSQL_COMPAT.md",
                "pg_compat": "docs/PG_COMPAT.md",
                "true_status": "docs/TRUE_STATUS_MATRIX.md",
            },
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!("SkeinDB v{}", env!("CARGO_PKG_VERSION"));
        println!("Data dir       : {}", data_path.display());
        println!("Data dir exists: {}", exists);
        println!("Storage mode   : {storage_mode}");
        println!("Databases      : {}", databases.len());
        println!("Total tables   : {total_tables}");
        if !databases.is_empty() {
            println!("\nCatalog:");
            for (name, tables) in &databases {
                println!("  - {name}  ({tables} table(s))");
            }
        }
        println!("\nDefault ports  : http=8080, mysql=3306, pg=5432, cluster=9090");
        println!("MySQL compat   : adoption layer (see docs/MYSQL_COMPAT.md)");
        println!("PG compat      : partial baseline (see docs/PG_COMPAT.md)");
        println!("Status matrix  : docs/TRUE_STATUS_MATRIX.md");
    }
    Ok(())
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum StorageModeArg {
    Json,
    Segment,
    Hybrid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use skeindb_skeinql::methods::RowObject;
    use skeindb_skeinql::types::{BaseTableRef, Lit, TypeDesc};
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn serve_defaults_to_segment_storage_mode() {
        let cli = Cli::try_parse_from(["skeindb", "serve"]).expect("parse serve defaults");
        match cli.command {
            Commands::Serve { storage_mode, .. } => {
                assert_eq!(storage_mode, StorageModeArg::Segment);
            }
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn serve_parses_storage_mode_flag() {
        let cli = Cli::try_parse_from(["skeindb", "serve", "--storage-mode", "segment"])
            .expect("parse serve with storage mode");
        match cli.command {
            Commands::Serve { storage_mode, .. } => {
                assert_eq!(storage_mode, StorageModeArg::Segment);
            }
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn serve_default_pg_port() {
        let cli = Cli::try_parse_from(["skeindb", "serve"]).expect("parse serve defaults");
        match cli.command {
            Commands::Serve { pg, .. } => {
                assert_eq!(pg, 5432);
            }
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn serve_disable_pg_port() {
        let cli = Cli::try_parse_from(["skeindb", "serve", "--pg", "0"])
            .expect("parse serve with pg disabled");
        match cli.command {
            Commands::Serve { pg, .. } => {
                assert_eq!(pg, 0);
            }
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn replay_export_parses_flags() {
        let cli = Cli::try_parse_from([
            "skeindb",
            "replay",
            "export",
            "--data",
            "./data",
            "--db",
            "app",
            "--from-lsn",
            "10",
            "--to-lsn",
            "20",
            "--redaction",
            "hash_pk",
            "--redaction-salt",
            "test-salt",
            "--out",
            "bundle.sreplay",
        ])
        .expect("parse replay export");
        match cli.command {
            Commands::Replay { command } => match command {
                ReplayCommands::Export {
                    data,
                    db,
                    from_lsn,
                    to_lsn,
                    redaction,
                    redaction_salt,
                    out,
                } => {
                    assert_eq!(data, "./data");
                    assert_eq!(db.as_deref(), Some("app"));
                    assert_eq!(from_lsn, Some(10));
                    assert_eq!(to_lsn, Some(20));
                    assert_eq!(redaction, "hash_pk");
                    assert_eq!(redaction_salt.as_deref(), Some("test-salt"));
                    assert_eq!(out, "bundle.sreplay");
                }
                _ => panic!("expected replay export command"),
            },
            _ => panic!("expected replay command"),
        }
    }

    #[test]
    fn replay_verify_parses_bundle_path() {
        let cli =
            Cli::try_parse_from(["skeindb", "replay", "verify", "--bundle", "bundle.sreplay"])
                .expect("parse replay verify");
        match cli.command {
            Commands::Replay { command } => match command {
                ReplayCommands::Verify { bundle } => {
                    assert_eq!(bundle, "bundle.sreplay");
                }
                _ => panic!("expected replay verify command"),
            },
            _ => panic!("expected replay command"),
        }
    }

    #[test]
    fn replay_compare_parses_threshold_flags() {
        let cli = Cli::try_parse_from([
            "skeindb",
            "replay",
            "compare",
            "--baseline",
            "baseline.json",
            "--candidate",
            "candidate.json",
            "--max-p95-delta-ms",
            "15",
            "--max-p99-delta-ms",
            "25",
            "--max-span-delta-ms",
            "100",
            "--max-disk-bytes-delta",
            "4096",
            "--max-missing-hot-tables-delta",
            "1",
            "--out",
            "report.json",
        ])
        .expect("parse replay compare");
        match cli.command {
            Commands::Replay { command } => match command {
                ReplayCommands::Compare {
                    baseline,
                    candidate,
                    max_p95_delta_ms,
                    max_p99_delta_ms,
                    max_span_delta_ms,
                    max_disk_bytes_delta,
                    max_missing_hot_tables_delta,
                    out,
                } => {
                    assert_eq!(baseline, "baseline.json");
                    assert_eq!(candidate, "candidate.json");
                    assert_eq!(max_p95_delta_ms, 15);
                    assert_eq!(max_p99_delta_ms, 25);
                    assert_eq!(max_span_delta_ms, 100);
                    assert_eq!(max_disk_bytes_delta, 4096);
                    assert_eq!(max_missing_hot_tables_delta, 1);
                    assert_eq!(out.as_deref(), Some("report.json"));
                }
                _ => panic!("expected replay compare command"),
            },
            _ => panic!("expected replay command"),
        }
    }

    #[test]
    fn transport_bench_parses_flags() {
        let cli = Cli::try_parse_from([
            "skeindb",
            "transport-bench",
            "--http-url",
            "http://127.0.0.1:8080",
            "--mysql-port",
            "3306",
            "--quic-port",
            "4433",
            "--quic-cert",
            "./cert.pem",
            "--quic-server-name",
            "localhost",
            "--concurrency",
            "6",
            "--requests",
            "24",
            "--sql",
            "SELECT 1 AS one",
            "--json",
        ])
        .expect("parse transport-bench");

        match cli.command {
            Commands::TransportBench {
                http_url,
                mysql_port,
                quic_port,
                quic_cert,
                quic_server_name,
                concurrency,
                requests,
                sql,
                json,
            } => {
                assert_eq!(http_url, "http://127.0.0.1:8080");
                assert_eq!(mysql_port, 3306);
                assert_eq!(quic_port, 4433);
                assert_eq!(quic_cert, PathBuf::from("./cert.pem"));
                assert_eq!(quic_server_name, "localhost");
                assert_eq!(concurrency, 6);
                assert_eq!(requests, 24);
                assert_eq!(sql, "SELECT 1 AS one");
                assert!(json);
            }
            _ => panic!("expected transport-bench command"),
        }
    }

    fn replay_result_with_perf(
        bundle_id: &str,
        p95_delta: i64,
        p99_delta: i64,
        missing_hot_tables: u64,
    ) -> MaintenanceReplayRunResult {
        use skeindb_skeinql::methods::{
            ReplayBundlePerformanceCacheVariance, ReplayBundlePerformanceRunReport,
            ReplayBundlePerformanceStorageVariance, ReplayBundlePerformanceTimingVariance,
        };

        MaintenanceReplayRunResult {
            ok: true,
            workspace_id: format!("{bundle_id}-workspace"),
            bundle_id: bundle_id.to_string(),
            workspace_path: ".replay_workspaces/test".to_string(),
            expected_checksum: "abc".to_string(),
            observed_checksum: "abc".to_string(),
            table_checksums: Vec::new(),
            replayed_tables: 1,
            replayed_rows: 10,
            replayed_changes: 4,
            performance_report: Some(ReplayBundlePerformanceRunReport {
                format: "skein.replay.performance.v1".to_string(),
                baseline_checksum: "perf".to_string(),
                observed_checksum: "perf".to_string(),
                checksum_match: true,
                storage: ReplayBundlePerformanceStorageVariance {
                    disk_bytes_delta: 0,
                    wal_bytes_delta: 0,
                    total_tables_delta: 0,
                    total_rows_delta: 0,
                    mvcc_versions_delta: 0,
                    delta_chains_delta: 0,
                },
                cache_warm: ReplayBundlePerformanceCacheVariance {
                    cached_select_entries_delta: 0,
                    cached_patch_entries_delta: 0,
                    hot_table_match_count: 1,
                    missing_hot_table_count: missing_hot_tables,
                    extra_hot_table_count: 0,
                },
                timing: ReplayBundlePerformanceTimingVariance {
                    change_count_delta: 0,
                    span_ms_delta: 0,
                    p50_inter_event_ms_delta: 0,
                    p95_inter_event_ms_delta: p95_delta,
                    p99_inter_event_ms_delta: p99_delta,
                },
            }),
        }
    }

    #[test]
    fn replay_compare_reports_threshold_failures() {
        let baseline = replay_result_with_perf("baseline", 4, 8, 0);
        let candidate = replay_result_with_perf("candidate", 22, 40, 2);
        let report = compare_replay_run_reports(
            &baseline,
            &candidate,
            ReplayCiThresholds {
                max_p95_delta_ms: 10,
                max_p99_delta_ms: 20,
                max_span_delta_ms: 0,
                max_disk_bytes_delta: 0,
                max_missing_hot_tables_delta: 1,
            },
        )
        .expect("compare replay reports");

        assert!(!report.ok);
        assert!(report
            .failures
            .iter()
            .any(|failure| failure.contains("p95_inter_event_ms_delta")));
        assert!(report
            .failures
            .iter()
            .any(|failure| failure.contains("missing_hot_table_count")));
    }

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("skeindb_{name}_{}_{}", std::process::id(), suffix));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn type_desc(kind: &str) -> TypeDesc {
        TypeDesc {
            kind: kind.to_string(),
            max: None,
            precision: None,
            scale: None,
            charset: None,
            collation: None,
            unsigned: None,
        }
    }

    fn row(entries: &[(&str, Lit)]) -> RowObject {
        let mut out = RowObject::new();
        for (key, value) in entries.iter() {
            out.insert((*key).to_string(), value.clone());
        }
        out
    }

    fn seed_forensic_chain(dir: &Path) -> anyhow::Result<()> {
        let mut engine = engine::Engine::open(dir)?;
        engine.create_table(
            "app",
            "logs",
            vec![
                engine::ColumnSchema {
                    name: "id".to_string(),
                    r#type: type_desc("u64"),
                    nullable: false,
                    auto_increment: false,
                },
                engine::ColumnSchema {
                    name: "message".to_string(),
                    r#type: type_desc("str"),
                    nullable: false,
                    auto_increment: false,
                },
            ],
            vec!["id".to_string()],
            false,
            None,
        )?;
        engine.data_insert(
            &BaseTableRef {
                db: "app".to_string(),
                table: "logs".to_string(),
                r#as: None,
            },
            vec![row(&[
                ("id", Lit::U64 { v: 1 }),
                (
                    "message",
                    Lit::Str {
                        v: "hello".to_string(),
                    },
                ),
            ])],
            None,
        )?;
        Ok(())
    }

    #[test]
    fn audit_verify_command_reports_valid_chain_and_persists_timestamp() -> anyhow::Result<()> {
        let dir = temp_dir("audit_verify_ok");
        seed_forensic_chain(&dir)?;

        let report = collect_audit_verify_report(dir.to_str().expect("temp dir utf8"))?;
        assert!(report.is_ok());
        assert!(report.records_checked() >= 1);
        assert!(report.last_verified_ms() > 0);

        let reopened = engine::Engine::open(&dir)?;
        let persisted = reopened.maintenance_audit_status();
        assert!(
            persisted
                .get("last_verified_ms")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
                > 0
        );

        fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn audit_verify_command_detects_tampered_chain() -> anyhow::Result<()> {
        let dir = temp_dir("audit_verify_tampered");
        seed_forensic_chain(&dir)?;

        let forensic_path = dir.join("forensic_chain.json");
        let mut disk: serde_json::Value = serde_json::from_slice(&fs::read(&forensic_path)?)?;
        disk["records"][0]["hash"] = serde_json::Value::String("tampered".to_string());
        fs::write(&forensic_path, serde_json::to_vec_pretty(&disk)?)?;

        let report = collect_audit_verify_report(dir.to_str().expect("temp dir utf8"))?;
        assert!(!report.is_ok());
        assert_eq!(report.reason(), Some("hash_mismatch"));
        assert!(audit_verify_failure_message(&report).contains("hash_mismatch"));

        fs::remove_dir_all(&dir).ok();
        Ok(())
    }
}

impl StorageModeArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Segment => "segment",
            Self::Hybrid => "hybrid",
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "skeindb")]
#[command(version)]
#[command(about = "SkeinDB - single-binary DB concept (scaffold + runnable SkeinQL RPC)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the server (HTTP SkeinQL + embedded consoles).
    Serve {
        /// Data directory
        #[arg(long, default_value = "./data")]
        data: String,

        /// Table row persistence mode (json, segment, hybrid).
        #[arg(long, value_enum, default_value_t = StorageModeArg::Segment)]
        storage_mode: StorageModeArg,

        /// Bind address for listeners
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// MySQL protocol port (0 disables listener)
        #[arg(long, default_value_t = 3306)]
        mysql: u16,

        /// PostgreSQL protocol port (0 disables listener)
        #[arg(long, default_value_t = 5432)]
        pg: u16,

        /// HTTP port (SkeinQL + consoles + admin APIs)
        #[arg(long, default_value_t = 8080)]
        http: u16,

        /// Cluster port (replication / node-to-node RPC metadata and node identity seed).
        #[arg(long, default_value_t = 9090)]
        cluster_port: u16,

        /// QUIC port for experimental SkeinQL-over-QUIC transport.
        #[arg(long)]
        quic: Option<u16>,

        /// QUIC TLS certificate (PEM). Required when --quic is set.
        #[arg(long)]
        quic_cert: Option<PathBuf>,

        /// QUIC TLS private key (PEM). Required when --quic is set.
        #[arg(long)]
        quic_key: Option<PathBuf>,
    },

    /// Print current format versions
    Version,

    /// Verify tamper-evident audit chain (placeholder)
    AuditVerify {
        /// Data directory
        #[arg(long, default_value = "./data")]
        data: String,
    },

    /// Build a column snapshot and write manifest + .cseg files
    SnapshotBuild {
        /// Data directory
        #[arg(long, default_value = "./data")]
        data: String,

        /// Table name (db.table)
        #[arg(long)]
        table: String,

        /// Snapshot timestamp (unix micros)
        #[arg(long)]
        snapshot_ts: u64,
    },

    /// Export, verify, and run deterministic replay bundles.
    Replay {
        #[command(subcommand)]
        command: ReplayCommands,
    },

    /// Print a runtime summary for a data directory (version, storage mode, databases, tables).
    Info {
        /// Data directory
        #[arg(long, default_value = "./data")]
        data: String,

        /// Output as JSON instead of a human-readable summary
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Evaluate NL-to-SkeinQL datasets (experimental).
    NlEval {
        /// Dataset JSONL path
        #[arg(long)]
        dataset: String,

        /// Execute queries and compare result sets
        #[arg(long, default_value_t = false)]
        execute: bool,

        /// Maximum examples to process
        #[arg(long)]
        limit: Option<usize>,
    },

    /// Benchmark QUIC vs HTTP/2 and MySQL/TCP under concurrent SQL load.
    TransportBench {
        /// Base HTTP URL for the running SkeinQL server, e.g. http://127.0.0.1:8080
        #[arg(long)]
        http_url: String,

        /// MySQL listener port on the same host as --http-url
        #[arg(long)]
        mysql_port: u16,

        /// QUIC listener port on the same host as --http-url
        #[arg(long)]
        quic_port: u16,

        /// QUIC server certificate PEM used to trust the listener
        #[arg(long)]
        quic_cert: PathBuf,

        /// TLS server name to present to the QUIC listener
        #[arg(long, default_value = "localhost")]
        quic_server_name: String,

        /// Number of concurrent workers or streams per transport
        #[arg(long, default_value_t = 8)]
        concurrency: usize,

        /// Number of requests to run per transport
        #[arg(long, default_value_t = 64)]
        requests: u64,

        /// SQL text used for each request
        #[arg(long, default_value = "SELECT 1 AS one")]
        sql: String,

        /// Emit the benchmark report as JSON
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ReplayCommands {
    /// Export a replay bundle from a data directory.
    Export {
        /// Data directory
        #[arg(long, default_value = "./data")]
        data: String,

        /// Optional database filter
        #[arg(long)]
        db: Option<String>,

        /// Inclusive lower bound for bundled change events
        #[arg(long)]
        from_lsn: Option<u64>,

        /// Inclusive upper bound for bundled change events
        #[arg(long)]
        to_lsn: Option<u64>,

        /// Primary-key redaction mode: none, hash_pk, or drop_pk
        #[arg(long, default_value = "none")]
        redaction: String,

        /// Optional salt used by hash_pk redaction
        #[arg(long)]
        redaction_salt: Option<String>,

        /// Output replay bundle path (.sreplay)
        #[arg(long)]
        out: String,
    },

    /// Verify a replay bundle by materializing it in a temporary workspace.
    Verify {
        /// Replay bundle path
        #[arg(long)]
        bundle: String,
    },

    /// Run a replay bundle in a temporary deterministic workspace.
    Run {
        /// Replay bundle path
        #[arg(long)]
        bundle: String,

        /// Emit the full replay run result as JSON
        #[arg(long, default_value_t = false)]
        json: bool,

        /// Optional output path for --json
        #[arg(long)]
        out: Option<String>,
    },

    /// Compare two JSON replay run reports and fail on performance regressions.
    Compare {
        /// Baseline replay run JSON, usually from the main/base commit
        #[arg(long)]
        baseline: String,

        /// Candidate replay run JSON, usually from the PR/head commit
        #[arg(long)]
        candidate: String,

        /// Maximum allowed increase in p95 inter-event replay variance
        #[arg(long, default_value_t = 0)]
        max_p95_delta_ms: i64,

        /// Maximum allowed increase in p99 inter-event replay variance
        #[arg(long, default_value_t = 0)]
        max_p99_delta_ms: i64,

        /// Maximum allowed increase in total replay span variance
        #[arg(long, default_value_t = 0)]
        max_span_delta_ms: i64,

        /// Maximum allowed increase in disk-byte replay variance
        #[arg(long, default_value_t = 0)]
        max_disk_bytes_delta: i64,

        /// Maximum allowed increase in missing hot-table hints
        #[arg(long, default_value_t = 0)]
        max_missing_hot_tables_delta: i64,

        /// Optional JSON output path for the comparison report
        #[arg(long)]
        out: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
            data,
            storage_mode,
            bind,
            mysql,
            pg,
            http,
            cluster_port,
            quic,
            quic_cert,
            quic_key,
        } => {
            server::serve(server::ServeOpts {
                data_dir: data,
                storage_mode: storage_mode.as_str().to_string(),
                bind,
                mysql_port: mysql,
                pg_port: pg,
                http_port: http,
                cluster_port,
                quic_port: quic,
                quic_cert,
                quic_key,
            })
            .await
        }
        Commands::Version => {
            println!("SkeinDB v{}", env!("CARGO_PKG_VERSION"));
            println!("Build target   : {}", env!("CARGO_PKG_NAME"));
            println!("On-disk format : v0.2 (v0.1 compatible) - see docs/ON_DISK_FORMAT.md");
            println!("SkeinIR        : v1   - see docs/SKEINIR.md");
            println!("SkeinQL        : v1.0 - see docs/SKEINQL.md");
            println!("MySQL compat   : adoption layer - see docs/MYSQL_COMPAT.md");
            println!("PG compat      : partial baseline - see docs/PG_COMPAT.md");
            println!("Status matrix  : docs/TRUE_STATUS_MATRIX.md");
            Ok(())
        }
        Commands::Info { data, json } => run_info_command(&data, json),
        Commands::AuditVerify { data } => {
            let report = collect_audit_verify_report(&data)?;
            println!(
                "Audit verification checked {} record(s) in {} ms.",
                report.records_checked(),
                report.elapsed_ms()
            );
            println!("Chain head hash: {}", report.chain_head_hash());
            println!("Checkpoint anchors: {}", report.anchor_count());
            if report.is_ok() {
                println!(
                    "Audit chain OK. last_verified_ms={}",
                    report.last_verified_ms()
                );
                Ok(())
            } else {
                anyhow::bail!(audit_verify_failure_message(&report));
            }
        }
        Commands::SnapshotBuild {
            data,
            table,
            snapshot_ts,
        } => {
            let (db, table_name) = match table.split_once('.') {
                Some((db, table_name)) => (db.to_string(), table_name.to_string()),
                None => anyhow::bail!("table must be in db.table form"),
            };
            let engine = engine::Engine::open(&data)?;
            engine.build_column_snapshot(&db, &table_name, None, snapshot_ts)?;
            println!(
                "Built column snapshot for {db}.{table_name} at {snapshot_ts} (data dir: {data})"
            );
            Ok(())
        }
        Commands::Replay { command } => match command {
            ReplayCommands::Export {
                data,
                db,
                from_lsn,
                to_lsn,
                redaction,
                redaction_salt,
                out,
            } => {
                let engine = engine::Engine::open(&data)?;
                let redaction = if redaction == "none" && redaction_salt.is_none() {
                    None
                } else {
                    Some(EdgeBundleRedaction {
                        mode: redaction,
                        salt: redaction_salt,
                    })
                };
                let result = engine.maintenance_replay_export(MaintenanceReplayExportParams {
                    db,
                    from_lsn,
                    to_lsn,
                    bundle_id: None,
                    redaction,
                })?;
                let out_path = PathBuf::from(out);
                if let Some(parent) = out_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        fs::create_dir_all(parent)?;
                    }
                }
                fs::write(&out_path, serde_json::to_vec_pretty(&result.bundle)?)?;
                println!(
                    "Wrote replay bundle {} with {} table(s), {} row(s), and {} change event(s) to {}",
                    result.bundle.manifest.bundle_id,
                    result.bundle.manifest.table_count,
                    result.bundle.manifest.row_count,
                    result.bundle.manifest.change_count,
                    out_path.display()
                );
                println!("Checksum: {}", result.bundle.manifest.checksum);
                Ok(())
            }
            ReplayCommands::Verify { bundle } => {
                let bundle_path = PathBuf::from(bundle);
                let bundle: ReplayBundle = serde_json::from_slice(&fs::read(&bundle_path)?)?;
                let temp = replay_temp_dir("replay_verify")?;
                let result = run_replay_bundle_in_workspace(&bundle, &temp, "verify")?;
                fs::remove_dir_all(&temp).ok();
                println!(
                    "Verified replay bundle {}: checksum {} (tables={}, rows={}, changes={})",
                    result.bundle_id,
                    result.observed_checksum,
                    result.replayed_tables,
                    result.replayed_rows,
                    result.replayed_changes
                );
                if result.ok {
                    Ok(())
                } else {
                    anyhow::bail!(
                        "replay verification failed: expected_checksum={}, observed_checksum={}",
                        result.expected_checksum,
                        result.observed_checksum
                    );
                }
            }
            ReplayCommands::Run { bundle, json, out } => {
                let bundle_path = PathBuf::from(bundle);
                let bundle: ReplayBundle = serde_json::from_slice(&fs::read(&bundle_path)?)?;
                let temp = replay_temp_dir("replay_run")?;
                let result = run_replay_bundle_in_workspace(&bundle, &temp, "run")?;
                if json || out.is_some() {
                    write_json_report(&result, out.as_deref())?;
                } else {
                    println!(
                        "Replay run {} completed in {}: checksum {} (tables={}, rows={}, changes={})",
                        result.bundle_id,
                        result.workspace_path,
                        result.observed_checksum,
                        result.replayed_tables,
                        result.replayed_rows,
                        result.replayed_changes
                    );
                }
                if result.ok {
                    Ok(())
                } else {
                    anyhow::bail!(
                        "replay run failed: expected_checksum={}, observed_checksum={}",
                        result.expected_checksum,
                        result.observed_checksum
                    );
                }
            }
            ReplayCommands::Compare {
                baseline,
                candidate,
                max_p95_delta_ms,
                max_p99_delta_ms,
                max_span_delta_ms,
                max_disk_bytes_delta,
                max_missing_hot_tables_delta,
                out,
            } => {
                let baseline = read_replay_run_report(&baseline)?;
                let candidate = read_replay_run_report(&candidate)?;
                let report = compare_replay_run_reports(
                    &baseline,
                    &candidate,
                    ReplayCiThresholds {
                        max_p95_delta_ms,
                        max_p99_delta_ms,
                        max_span_delta_ms,
                        max_disk_bytes_delta,
                        max_missing_hot_tables_delta,
                    },
                )?;
                let ok = report.ok;
                let failures = report.failures.join("; ");
                write_json_report(&report, out.as_deref())?;
                if ok {
                    Ok(())
                } else {
                    anyhow::bail!("replay regression comparison failed: {failures}");
                }
            }
        },
        Commands::NlEval {
            dataset,
            execute,
            limit,
        } => {
            let dataset_path = PathBuf::from(dataset);
            nl_eval::run_nl_eval(&dataset_path, execute, limit)
        }
        Commands::TransportBench {
            http_url,
            mysql_port,
            quic_port,
            quic_cert,
            quic_server_name,
            concurrency,
            requests,
            sql,
            json,
        } => {
            let report = transport_bench::run(transport_bench::TransportBenchOptions {
                http_url,
                mysql_port,
                quic_port,
                quic_cert,
                quic_server_name,
                concurrency,
                requests,
                sql,
            })
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                transport_bench::print_human(&report);
            }
            Ok(())
        }
    }
}
