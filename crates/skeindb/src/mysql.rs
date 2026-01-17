use std::net::SocketAddr;
use thiserror::Error;
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::watch};

#[derive(Debug, Error)]
pub enum MysqlError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub async fn run_mysql_listener(
    addr: SocketAddr,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), MysqlError> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "MySQL listener started (placeholder)");
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                break;
            }
            result = listener.accept() => {
                let (mut socket, peer) = result?;
                tracing::info!(%peer, "accepted MySQL connection (placeholder)");
                tokio::spawn(async move {
                    let _ = socket
                        .write_all(b"SkeinDB: MySQL protocol not implemented yet.\n")
                        .await;
                    let _ = socket.shutdown().await;
                });
            }
        }
    }
    Ok(())
}
