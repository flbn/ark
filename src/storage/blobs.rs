// tldr: wraps iroh-blobs to handle content-addressed storage and p2p transfer

use crate::domain::BlobHash;
use iroh::{Endpoint, SecretKey, discovery::dns::DnsDiscovery, protocol::Router};
use iroh_blobs::{ALPN, BlobsProtocol, store::fs::FsStore};
use rand_chacha::rand_core::SeedableRng;
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
    pub router: Router, // keep the router handle alive, or node stops listening
    pub store: FsStore, // keep a ref to store so we can insert/read locally
}

impl NetworkedBlobStore {
    // initializes a p2p node:
    // 1) generates ephemeral identity (for now)
    // 2) binds udp socket
    // 3) spawns background router to handle 'blobs' protocol
    pub async fn new(blobs_dir: impl AsRef<Utf8Path>) -> Result<Self, BlobError> {
        let blobs_dir = blobs_dir.as_ref();
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0u64);
        let secret_key = SecretKey::generate(&mut rng);

        // create blob dir if it doesn't exist
        tokio::fs::create_dir_all(blobs_dir).await?;

        // load the persistent store. spins up a bg task to manage IO
        let store = FsStore::load(blobs_dir).await?;

        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .discovery(DnsDiscovery::n0_dns())
            .bind()
            .await?;

        let blobs_protocol = BlobsProtocol::new(&store, None);

        let router = Router::builder(endpoint)
            .accept(ALPN, blobs_protocol)
            .spawn();

        Ok(Self { router, store })
    }

    // tell bg task to stop
    pub async fn shutdown(&self) -> Result<(), BlobError> {
        self.router.shutdown().await?;
        Ok(())
    }

    // writes raw bytes. returns verified blake3 hash.
    // NOTE: this is strictly local right now; gossip comes later.
    pub async fn put(&self, data: &[u8]) -> Result<BlobHash, BlobError> {
        let tag = self.store.add_slice(data).await?;
        Ok(BlobHash::from(tag.hash))
    }

    #[allow(dead_code)]
    // verified streaming. returns a reader that validates hashes on the fly as you consume bytes
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
