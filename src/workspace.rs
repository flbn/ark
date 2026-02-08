// tldr: wires ArkBackend into jj-lib's workspace machinery.
// provides init (new workspace) and load (existing workspace) entry points,
// plus helpers to build UserSettings and StoreFactories for the ark backend.
//
// jj-lib's public API uses std::path::Path throughout — BackendInitializer,
// BackendFactory, Workspace::init_with_backend, Workspace::load all take &Path.
// we allow disallowed_types at this module boundary to bridge into camino.
#![allow(clippy::disallowed_types)]

use std::path::Path;
use std::sync::Arc;

use jj_lib::backend::{BackendInitError, BackendLoadError};
use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::repo::StoreFactories;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::workspace::{
    default_working_copy_factories, Workspace, WorkingCopyFactories, WorkspaceInitError,
    WorkspaceLoadError,
};

use camino::Utf8PathBuf;
use thiserror::Error;

use crate::backend::ArkBackend;
use crate::storage::BlobStore;

pub const BACKEND_NAME: &str = ArkBackend::BACKEND_NAME;

#[derive(Error, Debug)]
pub enum WorkspaceError {
    #[error("Non-UTF8 path: {0}")]
    NonUtf8Path(std::path::PathBuf),
    #[error("Failed to parse config: {0}")]
    Config(jj_lib::config::ConfigLoadError),
    #[error("Failed to build settings: {0}")]
    Settings(jj_lib::config::ConfigGetError),
    #[error("Failed to initialize signer: {0}")]
    Signer(jj_lib::signing::SignInitError),
    #[error("Workspace init failed: {0}")]
    Init(WorkspaceInitError),
    #[error("Workspace load failed: {0}")]
    Load(WorkspaceLoadError),
}

// @todo(o11y): block_on bridges async BlobStore/ArkBackend init into jj-lib's
//   sync factory closures. uses block_in_place when inside tokio to avoid
//   deadlocking the runtime. if called outside tokio, spins up a throwaway
//   current-thread runtime. acceptable for Phase II; may revisit if backend
//   init becomes a hot path.
// @todo(o11y): requires multi-threaded tokio runtime when called inside an
//   existing runtime (block_in_place panics on current_thread). callers must
//   ensure #[tokio::main] or #[tokio::test(flavor = "multi_thread")]. when
//   called outside any runtime (pure sync context), spins up a throwaway one.
fn block_on<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap_or_else(|_| {
                    // @todo(o11y): runtime creation only fails on OS resource exhaustion.
                    //   can't propagate because jj-lib's closure signature is fixed.
                    std::process::abort()
                });
            rt.block_on(future)
        }
    }
}

fn to_utf8_pathbuf(path: &Path) -> Result<Utf8PathBuf, std::path::PathBuf> {
    Utf8PathBuf::from_path_buf(path.to_path_buf())
}

fn init_backend_from_store_path(
    store_path: &Path,
) -> Result<Box<dyn jj_lib::backend::Backend>, Box<dyn std::error::Error + Send + Sync>> {
    let store_path = store_path.to_path_buf();
    block_on(async move {
        let utf8_path = to_utf8_pathbuf(&store_path)
            .map_err(|p| -> Box<dyn std::error::Error + Send + Sync> {
                WorkspaceError::NonUtf8Path(p).into()
            })?;
        let blob_store = Arc::new(BlobStore::new(&utf8_path).await?);
        let backend = ArkBackend::new(blob_store).await?;
        Ok(Box::new(backend) as Box<dyn jj_lib::backend::Backend>)
    })
}

pub fn ark_store_factories() -> StoreFactories {
    let mut factories = StoreFactories::default();
    factories.add_backend(
        BACKEND_NAME,
        Box::new(|_settings, store_path| {
            init_backend_from_store_path(store_path)
                .map_err(BackendLoadError)
        }),
    );
    factories
}

pub fn ark_working_copy_factories() -> WorkingCopyFactories {
    default_working_copy_factories()
}

// @todo(o11y): default_user_settings uses hardcoded identity values. once ark
//   has its own config file with user identity fields, read them from there.
//   acceptable for Phase II because ark is an archival tool, not a social VCS.
pub fn default_user_settings() -> Result<UserSettings, WorkspaceError> {
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
    let username = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "ark".to_string());

    let toml_text = format!(
        r#"
[user]
name = "Ark"
email = "ark@localhost"

[operation]
hostname = "{hostname}"
username = "{username}"

[signing]
behavior = "drop"
"#
    );

    let mut config = StackedConfig::with_defaults();
    let layer = ConfigLayer::parse(ConfigSource::User, &toml_text)
        .map_err(WorkspaceError::Config)?;
    config.add_layer(layer);

    UserSettings::from_config(config).map_err(WorkspaceError::Settings)
}

pub fn init_workspace(
    workspace_root: &Path,
    settings: &UserSettings,
) -> Result<(Workspace, Arc<jj_lib::repo::ReadonlyRepo>), WorkspaceError> {
    let signer = Signer::from_settings(settings).map_err(WorkspaceError::Signer)?;

    let backend_initializer = |_settings: &UserSettings, store_path: &Path| {
        init_backend_from_store_path(store_path)
            .map_err(BackendInitError)
    };

    Workspace::init_with_backend(settings, workspace_root, &backend_initializer, signer)
        .map_err(WorkspaceError::Init)
}

pub fn load_workspace(
    workspace_root: &Path,
    settings: &UserSettings,
) -> Result<Workspace, WorkspaceError> {
    let store_factories = ark_store_factories();
    let wc_factories = ark_working_copy_factories();
    Workspace::load(settings, workspace_root, &store_factories, &wc_factories)
        .map_err(WorkspaceError::Load)
}
