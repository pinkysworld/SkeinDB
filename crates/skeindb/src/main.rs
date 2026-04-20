use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

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
            println!("SkeinDB scaffold v{}", env!("CARGO_PKG_VERSION"));
            println!("On-disk format: v0.2 (v0.1 compatible) - see docs/ON_DISK_FORMAT.md");
            println!("SkeinIR: v1 - see docs/SKEINIR.md");
            println!("SkeinQL: v1.0 - see docs/SKEINQL.md");
            Ok(())
        }
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
