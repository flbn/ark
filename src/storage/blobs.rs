// tldr: wraps iroh-blobs to handle content-addressed storage and p2p transfer
// NOTE: currently uses memstore (ram only) for simplicity; phase 2 moves to disk

use crate::domain::BlobHash;
use iroh::{Endpoint, SecretKey, discovery::dns::DnsDiscovery, protocol::Router};
use iroh_blobs::{ALPN, BlobsProtocol, store::mem::MemStore};
use rand_chacha::rand_core::SeedableRng;
use thiserror::Error;
use tokio::io::AsyncRead;

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
}

pub struct NetworkedBlobStore {
    _router: Router,     // keep the router handle alive, or node stops listening
    pub store: MemStore, // keep a ref to store so we can insert/read locally
}

impl NetworkedBlobStore {
    // initializes a p2p node:
    // 1) generates ephemeral identity (for now)
    // 2) binds udp socket
    // 3) spawns background router to handle 'blobs' protocol
    pub async fn new() -> Result<Self, BlobError> {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0u64);
        let secret_key = SecretKey::generate(&mut rng);

        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .discovery(DnsDiscovery::n0_dns())
            .bind()
            .await?;

        let store = MemStore::new();

        let blobs_protocol = BlobsProtocol::new(&store, None);

        let router = Router::builder(endpoint)
            .accept(ALPN, blobs_protocol)
            .spawn();

        Ok(Self {
            _router: router,
            store,
        })
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
}
