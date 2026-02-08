// tldr: cold-start reconciliation protocol over iroh QUIC connections.
// when a node starts up, it connects to each configured peer, exchanges
// SyncMessages over a length-prefixed batch framing protocol, identifies
// missing blobs, and fetches them via iroh-blobs. the responder side is
// registered as a ProtocolHandler on the iroh Router so peers can initiate.
//
// protocol flow (mirrors simulate_sync):
//   initiator sends batch → responder processes, sends response batch →
//   initiator processes, sends next batch → ... until initiator sends
//   an empty batch (count=0), signaling done.

use std::sync::Arc;

use iroh::endpoint::{
    ClosedStream, Connection, ReadExactError, RecvStream, SendStream, WriteError,
};
use iroh::protocol::AcceptError;
use iroh::EndpointAddr;

use crate::network::sync::{fetch_missing_blobs, FetchResult, Reconciler, SyncError, SyncMessage};
use crate::storage::index::{Index, IndexError};

use thiserror::Error;

pub const RECON_ALPN: &[u8] = b"ark/recon/0";

// @todo(o11y): MAX_FRAME_LEN is a guardrail against malicious peers sending
//   enormous frames. 16 MiB is generous — a SyncMessage::Items with 65k hashes
//   is ~2 MiB. revisit if reconciliation sets grow beyond expectations.
const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

// @todo(o11y): MAX_BATCH_COUNT prevents a peer from claiming a batch of billions
//   of messages. 4096 is generous for any realistic reconciliation.
const MAX_BATCH_COUNT: u32 = 4096;

// @todo(o11y): DEFAULT_ENUMERATE_THRESHOLD is used by both initiator and responder.
//   no threshold negotiation in the protocol — both sides must use compatible values
//   for efficient convergence. acceptable because all nodes run the same binary.
const DEFAULT_ENUMERATE_THRESHOLD: usize = 8;

#[derive(Error, Debug)]
pub enum ReconError {
    #[error("Connection error: {0}")]
    Connection(#[from] iroh::endpoint::ConnectionError),
    #[error("Connect error: {0}")]
    Connect(#[from] iroh::endpoint::ConnectError),
    #[error("Write error: {0}")]
    Write(#[from] WriteError),
    #[error("Read error: {0}")]
    Read(#[from] ReadExactError),
    #[error("Closed stream: {0}")]
    ClosedStream(#[from] ClosedStream),
    #[error("Index error: {0}")]
    Index(#[from] IndexError),
    #[error("Sync error: {0}")]
    Sync(#[from] SyncError),
    #[error("Serialization error: {0}")]
    Rkyv(String),
    #[error("Frame too large: {len} bytes (max {MAX_FRAME_LEN})")]
    FrameTooLarge { len: u32 },
    #[error("Batch too large: {count} messages (max {MAX_BATCH_COUNT})")]
    BatchTooLarge { count: u32 },
}

// ---------------------------------------------------------------------------
// Batch-round framing over QUIC streams
// ---------------------------------------------------------------------------
// Each round sends: [4-byte batch count] then [count × length-prefixed messages].
// A batch count of 0 signals "I'm done, no more messages from me."

async fn write_msg(send: &mut SendStream, msg: &SyncMessage) -> Result<(), ReconError> {
    let bytes = rkyv::to_bytes::<rancor::Error>(msg)
        .map_err(|e: rancor::Error| ReconError::Rkyv(e.to_string()))?;
    let len = bytes.len() as u32;
    send.write_all(&len.to_be_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

async fn read_msg(recv: &mut RecvStream) -> Result<SyncMessage, ReconError> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;

    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(ReconError::FrameTooLarge { len });
    }

    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf).await?;

    let msg = rkyv::from_bytes::<SyncMessage, rancor::Error>(&buf)
        .map_err(|e: rancor::Error| ReconError::Rkyv(e.to_string()))?;
    Ok(msg)
}

async fn write_batch(send: &mut SendStream, msgs: &[SyncMessage]) -> Result<(), ReconError> {
    let count = msgs.len() as u32;
    send.write_all(&count.to_be_bytes()).await?;
    for msg in msgs {
        write_msg(send, msg).await?;
    }
    Ok(())
}

async fn read_batch(recv: &mut RecvStream) -> Result<Vec<SyncMessage>, ReconError> {
    let mut count_buf = [0u8; 4];
    recv.read_exact(&mut count_buf).await?;
    let count = u32::from_be_bytes(count_buf);

    if count > MAX_BATCH_COUNT {
        return Err(ReconError::BatchTooLarge { count });
    }

    let mut msgs = Vec::with_capacity(count as usize);
    for _ in 0..count {
        msgs.push(read_msg(recv).await?);
    }
    Ok(msgs)
}

// ---------------------------------------------------------------------------
// ReconProtocol — responder side (registered on iroh Router)
// ---------------------------------------------------------------------------

pub struct ReconProtocol {
    index: Arc<Index>,
}

impl std::fmt::Debug for ReconProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconProtocol").finish()
    }
}

impl ReconProtocol {
    pub fn new(index: Arc<Index>) -> Self {
        Self { index }
    }
}

impl iroh::protocol::ProtocolHandler for ReconProtocol {
    // @todo(o11y): responder only reacts to initiator batches — never sends
    //   unsolicited. the responder's have_for_remote set is computed but not
    //   acted on (the initiator drives fetching). acceptable for now.
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .map_err(AcceptError::from_err)?;

        let local_hashes = self
            .index
            .list_shared_blobs()
            .map_err(AcceptError::from_err)?;
        let mut reconciler = Reconciler::new(local_hashes, DEFAULT_ENUMERATE_THRESHOLD);

        loop {
            let batch = read_batch(&mut recv)
                .await
                .map_err(AcceptError::from_err)?;

            if batch.is_empty() {
                break;
            }

            let mut responses = Vec::new();
            for msg in batch {
                responses.extend(
                    reconciler
                        .process(msg)
                        .map_err(AcceptError::from_err)?,
                );
            }

            write_batch(&mut send, &responses)
                .await
                .map_err(AcceptError::from_err)?;
        }

        send.finish().map_err(AcceptError::from_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Initiator side — connect to peer and run reconciliation
// ---------------------------------------------------------------------------

// @todo(o11y): initiate_recon does a full list_shared_blobs scan on every call.
//   for large stores this is O(n). acceptable for cold-start (runs once on startup).
//   if reconciliation becomes frequent, cache the hash set or use a secondary table.
pub async fn initiate_recon(
    endpoint: &iroh::Endpoint,
    store: &iroh_blobs::store::fs::FsStore,
    index: &Index,
    peer: EndpointAddr,
    enumerate_threshold: usize,
) -> Result<FetchResult, ReconError> {
    let local_hashes = index.list_shared_blobs()?;
    let mut reconciler = Reconciler::new(local_hashes, enumerate_threshold);

    let connection = endpoint.connect(peer.clone(), RECON_ALPN).await?;
    let (mut send, mut recv) = connection.open_bi().await.map_err(ReconError::from)?;

    let mut pending = vec![reconciler.initial_message()];

    while !pending.is_empty() {
        write_batch(&mut send, &pending).await?;

        let responses = read_batch(&mut recv).await?;

        pending = Vec::new();
        for msg in responses {
            pending.extend(reconciler.process(msg)?);
        }
    }

    // signal done with empty batch
    write_batch(&mut send, &[]).await?;
    send.finish().map_err(ReconError::from)?;

    let missing = reconciler.need_from_remote();
    if missing.is_empty() {
        return Ok(FetchResult {
            fetched: 0,
            failed: Vec::new(),
        });
    }

    let result = fetch_missing_blobs(store, endpoint, missing, vec![peer]).await?;
    Ok(result)
}
