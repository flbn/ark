// tldr: wraps iroh-blobs to handle content-addressed storage and p2p transfer

use crate::domain::BlobHash;
use iroh::{Endpoint, SecretKey, discovery::dns::DnsDiscovery, protocol::Router};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, store::fs::FsStore};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip};
use camino::Utf8Path;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Error, Debug)]
pub enum BlobError {
    #[error("Iroh networking error: {0}")]
    Net(#[from] anyhow::Error),
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Endpoint bind error: {0}")]
    Bind(#[from] iroh::endpoint::BindError),
    #[error("Endpoint request error: {0}")]
    RequestError(#[from] iroh_blobs::api::RequestError),
    #[error("JoinError request error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

pub struct NetworkedBlobStore {
    pub endpoint: Endpoint,
    pub router: Router,
    pub store: FsStore,
    pub gossip: Gossip,
}

// @todo(o11y): key persistence writes raw bytes with no integrity check — a truncated
//   or corrupted key file silently produces a different identity. add a checksum header
//   before any production deployment.
async fn load_or_create_secret_key(path: &Utf8Path) -> Result<SecretKey, BlobError> {
    if path.exists() {
        let bytes = tokio::fs::read(path).await?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            return Ok(SecretKey::from_bytes(&arr));
        }
    }

    let secret_key = SecretKey::generate(&mut rand::rng());

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, secret_key.to_bytes()).await?;

    Ok(secret_key)
}

impl NetworkedBlobStore {
    pub async fn new(blobs_dir: impl AsRef<Utf8Path>) -> Result<Self, BlobError> {
        let blobs_dir = blobs_dir.as_ref();

        tokio::fs::create_dir_all(blobs_dir).await?;

        let key_path = blobs_dir.join("identity.key");
        let secret_key = load_or_create_secret_key(&key_path).await?;

        let store = FsStore::load(blobs_dir).await?;

        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .discovery(DnsDiscovery::n0_dns())
            .bind()
            .await?;

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let blobs_protocol = BlobsProtocol::new(&store, None);

        let router = Router::builder(endpoint.clone())
            .accept(BLOBS_ALPN, blobs_protocol)
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();

        Ok(Self {
            endpoint,
            router,
            store,
            gossip,
        })
    }

    pub async fn shutdown(&self) -> Result<(), BlobError> {
        self.router.shutdown().await?;
        Ok(())
    }

    // @todo(o11y): strictly local right now — gossip announcement on put deferred until network module lands
    pub async fn put(&self, data: &[u8]) -> Result<BlobHash, BlobError> {
        let tag = self.store.add_slice(data).await?;
        Ok(BlobHash::from(tag.hash))
    }

    // @todo(o11y): deletion removes the iroh tag which may allow iroh's internal GC
    //   to reclaim the data. if other tags reference the same hash, the blob persists.
    //   acceptable for Phase II — commit blobs are uniquely tagged.
    pub async fn delete(&self, hash: BlobHash) -> Result<(), BlobError> {
        let iroh_hash = iroh_blobs::Hash::from(hash);
        // @todo(o11y): FsStore doesn't expose direct hash deletion in 0.97 —
        //   blobs are kept alive by tags. best-effort: this is a no-op until
        //   we track tags or iroh exposes hash-level deletion.
        let _ = iroh_hash;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_reader(&self, hash: BlobHash) -> Result<impl AsyncRead + Unpin, BlobError> {
        let iroh_hash = iroh_blobs::Hash::from(hash);
        let reader = self.store.reader(iroh_hash);
        Ok(reader)
    }

    #[allow(dead_code)]
    pub async fn get_bytes(&self, hash: BlobHash) -> Result<Vec<u8>, BlobError> {
        let iroh_hash = iroh_blobs::Hash::from(hash);
        let mut reader = self.store.reader(iroh_hash);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok(buf)
    }
}
