use crate::{
    config::{ConfigError, ConfigStore},
    console_db::ConsoleDb,
    http::{run_http_server, AppState},
    mysql::{run_mysql_listener, MysqlError},
};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use thiserror::Error;
use tokio::sync::watch;
use tokio::sync::RwLock;

pub struct ServeOptions {
    pub data_dir: PathBuf,
    pub mysql_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub config_path: PathBuf,
    pub load_sample: bool,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("http server error: {0}")]
    Http(#[from] std::io::Error),
    #[error("mysql listener error: {0}")]
    Mysql(#[from] MysqlError),
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub async fn run(options: ServeOptions) -> Result<(), ServerError> {
    let config_store = ConfigStore::load_or_default(options.config_path.clone()).await?;
    config_store
        .update(|config| {
            config.data_dir = options.data_dir.to_string_lossy().to_string();
            config.mysql_port = options.mysql_addr.port();
            config.http_port = options.http_addr.port();
        })
        .await?;

    let db = Arc::new(RwLock::new(ConsoleDb::new()));
    if options.load_sample {
        let mut db_guard = db.write().await;
        if let Err(err) = db_guard.load_sample() {
            tracing::warn!(%err, "failed to load sample SQL");
        }
    }

    let state = AppState::new(config_store, db, options.http_addr, options.mysql_addr);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut http_task = tokio::spawn(run_http_server(
        options.http_addr,
        state,
        shutdown_rx.clone(),
    ));
    let mut mysql_task = tokio::spawn(run_mysql_listener(options.mysql_addr, shutdown_rx.clone()));

    tokio::select! {
        res = &mut http_task => {
            let _ = shutdown_tx.send(true);
            res??;
            return Ok(());
        }
        res = &mut mysql_task => {
            let _ = shutdown_tx.send(true);
            res??;
            return Ok(());
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
        }
    }

    let http_res = http_task.await?;
    let mysql_res = mysql_task.await?;
    http_res?;
    mysql_res?;

    Ok(())
}
