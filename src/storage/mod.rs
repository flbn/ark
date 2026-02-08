pub mod blobs;
pub mod index;

use crate::domain::{BlobHash, BlobMetadata, RefName};
use crate::network::recon::{self, ReconError};
use crate::network::{GossipError, GossipHandle, HeadUpdate};
use blobs::{BlobError, NetworkedBlobStore};
use index::{Index, IndexError};

use camino::Utf8Path;
use thiserror::Error;

use std::sync::Arc;
use std::time::SystemTime;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Index error: {0}")]
    Index(#[from] IndexError),
    #[error("Blob error: {0}")]
    Blob(#[from] Box<BlobError>),
    #[error("Gossip error: {0}")]
    Gossip(#[from] Box<GossipError>),
    #[error("Reconciliation error: {0}")]
    Recon(#[from] ReconError),
}

pub struct BlobStore {
    pub index: Arc<Index>,
    pub blobs: NetworkedBlobStore,
    pub gossip: GossipHandle,
}

impl BlobStore {
    pub async fn new(root_path: impl AsRef<Utf8Path>) -> anyhow::Result<Self> {
        let root = root_path.as_ref();

        if !root.exists() {
            fs_err::create_dir_all(root)?;
        }

        let db_path = root.join("index.redb");
        let index = Arc::new(Index::new(db_path)?);

        let blobs = NetworkedBlobStore::new(root.join("blobs"), index.clone()).await?;

        let gossip = GossipHandle::new(blobs.gossip.clone());

        Ok(Self {
            index,
            blobs,
            gossip,
        })
    }

    // @todo(o11y): put_object writes blob then metadata in separate steps — a crash
    //   between the two leaves an orphan blob with no index entry. acceptable pre-release
    //   because iroh-blobs is content-addressed and idempotent: orphan blobs only waste disk
    //   space, they don't corrupt correctness. on startup, a future reconciliation pass
    //   should scan (blob store − index) and delete orphans.
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

    // @todo(o11y): delete_object removes index metadata but blob deletion from iroh
    //   is best-effort (no-op in 0.97). orphan blob data may persist on disk until
    //   iroh exposes hash-level deletion or tag tracking is implemented.
    pub fn delete_object(&self, hash: BlobHash) -> Result<(), StoreError> {
        self.index.remove_blob(hash)?;
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

    pub async fn reconcile_with_peer(
        &self,
        peer: iroh::EndpointAddr,
        enumerate_threshold: usize,
    ) -> Result<crate::network::sync::FetchResult, StoreError> {
        let result = recon::initiate_recon(
            &self.blobs.endpoint,
            &self.blobs.store,
            &self.index,
            peer,
            enumerate_threshold,
        )
        .await?;
        Ok(result)
    }

    // @todo(o11y): update_reference_and_announce requires a pre-joined topic — callers
    //   must call gossip.join_topic before this. no auto-join to keep control explicit.
    pub async fn update_reference_and_announce(
        &self,
        name: RefName,
        hash: BlobHash,
        topic: iroh_gossip::TopicId,
    ) -> Result<(), StoreError> {
        self.index.update_ref(&name, hash)?;

        let blob_type = self
            .index
            .get_blob_meta(hash)?
            .map(|m| m.blob_type.to_u8())
            .unwrap_or(2);

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let update = HeadUpdate::new(name.as_str(), hash, timestamp, blob_type);
        self.gossip
            .broadcast_head_update(topic, &update)
            .await
            .map_err(Box::new)?;

        Ok(())
    }
}
