pub mod blobs;
pub mod index;

use crate::domain::{BlobHash, BlobMetadata, RefName};
use blobs::{BlobError, NetworkedBlobStore};
use index::{Index, IndexError};

#[allow(clippy::disallowed_types)]
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Index error: {0}")]
    Index(#[from] IndexError),
    #[error("Blob error: {0}")]
    Blob(#[from] BlobError),
}

pub struct BlobStore {
    pub index: Index,
    pub blobs: NetworkedBlobStore,
}

impl BlobStore {
    #[allow(clippy::disallowed_types)]
    pub async fn new(db_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let index = Index::new(db_path)?;
        let blobs = NetworkedBlobStore::new().await?;

        Ok(Self { index, blobs })
    }

    pub async fn put_object(
        &self,
        data: &[u8],
        meta: BlobMetadata,
    ) -> Result<BlobHash, StoreError> {
        // write data to blob store
        let hash = self.blobs.put(data).await?;

        // write metadata to index
        // NOTE(@o11y): blocks async thread briefly, which is acceptable for Redb's speed, but for massive loads we might spawn_blocking.
        self.index.register_blob(hash, meta)?;

        Ok(hash)
    }

    pub async fn update_reference(&self, name: RefName, hash: BlobHash) -> Result<(), StoreError> {
        self.index.update_ref(&name, hash)?;
        Ok(())
    }
}
