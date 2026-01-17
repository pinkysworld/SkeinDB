use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineFeatures {
    pub dedup: bool,
    pub compaction: bool,
    pub value_cache: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerConfig {
    pub server_name: String,
    pub data_dir: String,
    pub mysql_port: u16,
    pub http_port: u16,
    pub max_connections: u32,
    pub allow_remote_console: bool,
    pub log_level: String,
    pub features: EngineFeatures,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server_name: "skeindb".to_string(),
            data_dir: "./data".to_string(),
            mysql_port: 3306,
            http_port: 8080,
            max_connections: 256,
            allow_remote_console: false,
            log_level: "info".to_string(),
            features: EngineFeatures {
                dedup: true,
                compaction: true,
                value_cache: false,
            },
        }
    }
}

impl ServerConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.server_name.trim().is_empty() {
            return Err(ConfigError::Invalid {
                message: "server_name must not be empty".to_string(),
            });
        }
        if self.data_dir.trim().is_empty() {
            return Err(ConfigError::Invalid {
                message: "data_dir must not be empty".to_string(),
            });
        }
        if self.mysql_port == 0 {
            return Err(ConfigError::Invalid {
                message: "mysql_port must be > 0".to_string(),
            });
        }
        if self.http_port == 0 {
            return Err(ConfigError::Invalid {
                message: "http_port must be > 0".to_string(),
            });
        }
        if self.max_connections == 0 {
            return Err(ConfigError::Invalid {
                message: "max_connections must be > 0".to_string(),
            });
        }
        if self.log_level.trim().is_empty() {
            return Err(ConfigError::Invalid {
                message: "log_level must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },

    #[error("json error at {path}: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[error("invalid config: {message}")]
    Invalid { message: String },
}

#[derive(Clone)]
pub struct ConfigStore {
    path: PathBuf,
    config: Arc<RwLock<ServerConfig>>,
}

impl ConfigStore {
    pub async fn load_or_default(path: PathBuf) -> Result<Self, ConfigError> {
        let mut from_disk = None;
        match tokio::fs::read_to_string(&path).await {
            Ok(contents) => {
                let parsed: ServerConfig =
                    serde_json::from_str(&contents).map_err(|source| ConfigError::Json {
                        path: path.clone(),
                        source,
                    })?;
                from_disk = Some(parsed);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ConfigError::Io {
                    path: path.clone(),
                    source,
                });
            }
        }

        let loaded_from_disk = from_disk.is_some();
        let config = from_disk.unwrap_or_default();
        config.validate()?;

        let store = Self {
            path,
            config: Arc::new(RwLock::new(config)),
        };

        if !loaded_from_disk {
            store.persist().await?;
        }

        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn get(&self) -> ServerConfig {
        self.config.read().await.clone()
    }

    pub async fn set(&self, config: ServerConfig) -> Result<(), ConfigError> {
        config.validate()?;
        self.persist_config(&config).await?;
        let mut guard = self.config.write().await;
        *guard = config;
        Ok(())
    }

    pub async fn update<F>(&self, updater: F) -> Result<ServerConfig, ConfigError>
    where
        F: FnOnce(&mut ServerConfig),
    {
        let mut config = self.get().await;
        updater(&mut config);
        self.set(config.clone()).await?;
        Ok(config)
    }

    async fn persist(&self) -> Result<(), ConfigError> {
        let config = self.get().await;
        self.persist_config(&config).await
    }

    async fn persist_config(&self, config: &ServerConfig) -> Result<(), ConfigError> {
        ensure_parent_dir(&self.path).await?;
        let mut json =
            serde_json::to_string_pretty(config).map_err(|source| ConfigError::Json {
                path: self.path.clone(),
                source,
            })?;
        json.push('\n');
        tokio::fs::write(&self.path, json)
            .await
            .map_err(|source| ConfigError::Io {
                path: self.path.clone(),
                source,
            })?;
        Ok(())
    }
}

async fn ensure_parent_dir(path: &Path) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| ConfigError::Io {
                    path: parent.to_path_buf(),
                    source,
                })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = ServerConfig::default();
        config.validate().unwrap();
    }

    #[test]
    fn invalid_ports_are_rejected() {
        let mut config = ServerConfig::default();
        config.mysql_port = 0;
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn config_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("skeindb-config.json");
        let store = ConfigStore::load_or_default(path.clone()).await.unwrap();
        let mut config = store.get().await;
        config.max_connections = 42;
        store.set(config.clone()).await.unwrap();

        let reloaded = ConfigStore::load_or_default(path).await.unwrap();
        let loaded = reloaded.get().await;
        assert_eq!(loaded.max_connections, 42);
    }
}
