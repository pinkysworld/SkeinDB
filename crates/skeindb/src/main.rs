use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand, ValueEnum};
use skeindb_skeinql::methods::{
    MaintenanceReplayExportParams, MaintenanceReplayImportParams, MaintenanceReplayRunParams,
    ReplayBundle,
};

mod engine;
mod nl_eval;
mod pg_wire;
mod quic;
mod server;

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
) -> anyhow::Result<skeindb_skeinql::methods::MaintenanceReplayRunResult> {
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
                    out,
                } => {
                    assert_eq!(data, "./data");
                    assert_eq!(db.as_deref(), Some("app"));
                    assert_eq!(from_lsn, Some(10));
                    assert_eq!(to_lsn, Some(20));
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
                out,
            } => {
                let engine = engine::Engine::open(&data)?;
                let result = engine.maintenance_replay_export(MaintenanceReplayExportParams {
                    db,
                    from_lsn,
                    to_lsn,
                    bundle_id: None,
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
            ReplayCommands::Run { bundle } => {
                let bundle_path = PathBuf::from(bundle);
                let bundle: ReplayBundle = serde_json::from_slice(&fs::read(&bundle_path)?)?;
                let temp = replay_temp_dir("replay_run")?;
                let result = run_replay_bundle_in_workspace(&bundle, &temp, "run")?;
                println!(
                    "Replay run {} completed in {}: checksum {} (tables={}, rows={}, changes={})",
                    result.bundle_id,
                    result.workspace_path,
                    result.observed_checksum,
                    result.replayed_tables,
                    result.replayed_rows,
                    result.replayed_changes
                );
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
        },
        Commands::NlEval {
            dataset,
            execute,
            limit,
        } => {
            let dataset_path = PathBuf::from(dataset);
            nl_eval::run_nl_eval(&dataset_path, execute, limit)
        }
    }
}
