use clap::{Parser, Subcommand};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

#[derive(Parser, Debug)]
#[command(name = "skeindb")]
#[command(version)]
#[command(about = "SkeinDB (scaffold) – single-binary MySQL-compatible DB concept", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the server (MySQL protocol + HTTP console)
    Serve {
        /// Data directory
        #[arg(long, default_value = "./data")]
        data: PathBuf,

        /// MySQL port
        #[arg(long, default_value_t = 3306)]
        mysql: u16,

        /// MySQL bind address
        #[arg(long, default_value = "127.0.0.1")]
        mysql_bind: IpAddr,

        /// HTTP port
        #[arg(long, default_value_t = 8080)]
        http: u16,

        /// HTTP bind address
        #[arg(long, default_value = "127.0.0.1")]
        http_bind: IpAddr,

        /// Config file path
        #[arg(long, default_value = "./skeindb-config.json")]
        config: PathBuf,

        /// Load bundled sample SQL into the console database at startup
        #[arg(long, default_value_t = false)]
        load_sample: bool,
    },

    /// Print current format versions (placeholder)
    Version,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
            data,
            mysql,
            mysql_bind,
            http,
            http_bind,
            config,
            load_sample,
        } => {
            let mysql_addr = SocketAddr::new(mysql_bind, mysql);
            let http_addr = SocketAddr::new(http_bind, http);
            tracing::info!(
                data_dir = %data.display(),
                mysql_addr = %mysql_addr,
                http_addr = %http_addr,
                config_path = %config.display(),
                load_sample = %load_sample,
                "starting SkeinDB"
            );
            if let Err(err) = skeindb::server::run(skeindb::server::ServeOptions {
                data_dir: data,
                mysql_addr,
                http_addr,
                config_path: config,
                load_sample,
            })
            .await
            {
                tracing::error!(%err, "SkeinDB exited with error");
                std::process::exit(1);
            }
        }
        Commands::Version => {
            println!("SkeinDB scaffold v{}", env!("CARGO_PKG_VERSION"));
            println!("On-disk format: v0.1 (see docs/ON_DISK_FORMAT.md)");
            println!("SkeinIR: v1 (see docs/SKEINIR.md)");
            println!("Build: {}-{}", std::env::consts::OS, std::env::consts::ARCH);
        }
    }
}
