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
    Blob(#[from] Box<BlobError>),
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

    // @todo(o11y): put_object writes blob then metadata in separate steps — a crash
    //   between the two leaves an orphan blob with no index entry. needs atomic
    //   rollback or a startup reconciliation pass.
    pub async fn put_object(
        &self,
        data: &[u8],
        meta: BlobMetadata,
    ) -> Result<BlobHash, StoreError> {
        let hash = self.blobs.put(data).await.map_err(Box::new)?;

        // NOTE(@o11y): sync, but fast. for massive loads we might want to spawn blocking.
        self.index.register_blob(hash, meta)?;

        Ok(hash)
    }

    #[allow(dead_code)]
    pub async fn shutdown(&self) -> Result<(), StoreError> {
        self.blobs.shutdown().await.map_err(Box::new)?;
        Ok(())
    }

    // @todo(o11y): promote is metadata-only right now — once gossip lands,
    //   promotion should also trigger a HEAD broadcast for commit blobs
    pub fn promote_blob(&self, hash: BlobHash) -> Result<(), StoreError> {
        self.index.set_blob_local_only(hash, false)?;
        Ok(())
    }

    pub async fn update_reference(&self, name: RefName, hash: BlobHash) -> Result<(), StoreError> {
        self.index.update_ref(&name, hash)?;
        Ok(())
    }
}
