pub mod blobs;
pub mod index;

use crate::domain::{BlobHash, BlobMetadata, RefName};
use blobs::{BlobError, NetworkedBlobStore};
use index::{Index, IndexError};

use camino::Utf8Path;
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
    pub async fn new(root_path: impl AsRef<Utf8Path>) -> anyhow::Result<Self> {
        let root = root_path.as_ref();

        if !root.exists() {
            fs_err::create_dir_all(root)?;
        }

        // init redb (metdata) @ root/index.redb
        let db_path = root.join("index.redb");
        let index = Index::new(db_path)?;

        // init iroh (data) # root/blobs
        // pass the dir name
        let blobs = NetworkedBlobStore::new(root.join("blobs")).await?;

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
        // NOTE(@o11y): sync, but fast. for massive loads we might want to spawn blocking.
        self.index.register_blob(hash, meta)?;

        Ok(hash)
    }

    #[allow(dead_code)]
    pub async fn shutdown(&self) -> Result<(), StoreError> {
        self.blobs.shutdown().await?;
        Ok(())
    }

    pub async fn update_reference(&self, name: RefName, hash: BlobHash) -> Result<(), StoreError> {
        self.index.update_ref(&name, hash)?;
        Ok(())
    }
}
