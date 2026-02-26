use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

mod engine;
mod nl_eval;
mod quic;
mod server;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum StorageModeArg {
    Json,
    Segment,
    Hybrid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_defaults_to_hybrid_storage_mode() {
        let cli = Cli::try_parse_from(["skeindb", "serve"]).expect("parse serve defaults");
        match cli.command {
            Commands::Serve { storage_mode, .. } => {
                assert_eq!(storage_mode, StorageModeArg::Hybrid);
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
        #[arg(long, value_enum, default_value_t = StorageModeArg::Hybrid)]
        storage_mode: StorageModeArg,

        /// Bind address for listeners
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// MySQL protocol port (0 disables listener)
        #[arg(long, default_value_t = 3306)]
        mysql: u16,

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

    /// Build a column snapshot (placeholder)
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
            println!("Audit verify is a placeholder. Data dir: {data}");
            println!("See docs/AUDIT_WAL.md for the design.");
            Ok(())
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
