// tldr: deserialize ark.toml into typed config structs.
// the daemon reads this on startup to know which repos to watch and which peers to sync with.

use camino::{Utf8Path, Utf8PathBuf};
use iroh::{EndpointAddr, EndpointId};
use serde::Deserialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error reading config: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("Invalid node_id for remote '{name}': {source}")]
    InvalidNodeId {
        name: String,
        source: iroh::KeyParsingError,
    },
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

pub struct ResolvedRemote {
    pub name: String,
    pub endpoint_id: EndpointId,
}

impl ResolvedRemote {
    pub fn endpoint_addr(&self) -> EndpointAddr {
        EndpointAddr::from(self.endpoint_id)
    }
}

// @todo(o11y): resolve_remotes validates node_id strings eagerly at startup.
//   if a single remote has an invalid node_id, the whole config is rejected.
//   acceptable for now; could soften to warn-and-skip per remote later.
pub fn resolve_remotes(remotes: &[RemoteConfig]) -> Result<Vec<ResolvedRemote>, ConfigError> {
    let mut resolved = Vec::with_capacity(remotes.len());
    for remote in remotes {
        let endpoint_id: EndpointId =
            remote
                .node_id
                .parse()
                .map_err(|e| ConfigError::InvalidNodeId {
                    name: remote.name.clone(),
                    source: e,
                })?;
        resolved.push(ResolvedRemote {
            name: remote.name.clone(),
            endpoint_id,
        });
    }
    Ok(resolved)
}
