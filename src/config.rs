// tldr: deserialize ark.toml into typed config structs.
// the daemon reads this on startup to know which repos to watch and which peers to sync with.

use camino::{Utf8Path, Utf8PathBuf};
use serde::Deserialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error reading config: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub store: StoreConfig,
    #[serde(default)]
    pub repos: Vec<RepoConfig>,
    #[serde(default)]
    pub sync: SyncConfig,
}

#[derive(Debug, Deserialize)]
pub struct StoreConfig {
    pub path: Utf8PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct RepoConfig {
    pub id: String,
    #[serde(default)]
    pub remotes: Vec<RemoteConfig>,
}

#[derive(Debug, Deserialize)]
pub struct RemoteConfig {
    pub name: String,
    pub node_id: String,
}

#[derive(Debug, Deserialize)]
pub struct SyncConfig {
    #[serde(default = "default_enumerate_threshold")]
    pub enumerate_threshold: usize,
}

fn default_enumerate_threshold() -> usize {
    8
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            enumerate_threshold: default_enumerate_threshold(),
        }
    }
}

impl Config {
    pub fn load(path: &Utf8Path) -> Result<Self, ConfigError> {
        let contents = fs_err::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}
