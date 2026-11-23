use alice_core::error::HdbError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AliceConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub replication: ReplicationConfig,
    pub auth: AuthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub read_timeout_ms: u64,
    pub max_connections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_segment_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    pub primary_addr: Option<String>,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AuthConfig {
    pub users: Vec<UserEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEntry {
    pub username: String,
    pub password: String,
}

impl Default for AliceConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            replication: ReplicationConfig::default(),
            auth: AuthConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:7432".to_string(),
            read_timeout_ms: 200,
            max_connections: 256,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./alice_data".to_string(),
            wal_segment_mb: 4,
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            primary_addr: None,
            poll_interval_ms: 50,
        }
    }
}

impl AliceConfig {
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, HdbError> {
        let content = fs::read_to_string(path)?;
        toml::from_str(&content)
            .map_err(|_| HdbError::CorruptedData("Failed to parse TOML configuration"))
    }

    pub fn sample_toml() -> String {
        toml::to_string_pretty(&Self::default()).unwrap_or_default()
    }
}
