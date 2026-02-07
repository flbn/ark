// tldr: gossip layer for broadcasting HEAD updates across peers via iroh-gossip.
// nodes subscribe to a topic per repo and broadcast ref updates on checkpoint commits.

use std::collections::HashMap;

use bytes::Bytes;
use iroh::EndpointId;
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::{Gossip, TopicId};
use parking_lot::Mutex;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::domain::BlobHash;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GossipError {
    #[error("Gossip API error: {0}")]
    Api(#[from] iroh_gossip::api::ApiError),
    #[error("Serialization error: {0}")]
    Rkyv(String),
    #[error("Topic not joined: {0:?}")]
    TopicNotJoined(TopicId),
}

// @todo(o11y): HeadUpdate has no signature field — any peer can forge updates.
//   acceptable pre-gossip-auth; add ed25519 sig verification before untrusted peers.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct HeadUpdate {
    pub ref_name: String,
    pub blob_hash: [u8; 32],
    pub timestamp: u64,
}

impl HeadUpdate {
    pub fn new(ref_name: &str, hash: BlobHash, timestamp: u64) -> Self {
        Self {
            ref_name: ref_name.to_owned(),
            blob_hash: hash.0,
            timestamp,
        }
    }

    pub fn hash(&self) -> BlobHash {
        BlobHash(self.blob_hash)
    }
}

pub fn derive_topic_id(repo_id: &[u8]) -> TopicId {
    let hash = blake3::hash(&[b"ark:gossip:v1:", repo_id].concat());
    TopicId::from_bytes(*hash.as_bytes())
}

struct TopicState {
    sender: GossipSender,
    updates_tx: broadcast::Sender<HeadUpdate>,
    _recv_task: JoinHandle<()>,
}

pub struct GossipHandle {
    gossip: Gossip,
    // @todo(o11y): parking_lot::Mutex is fine here because we never hold it across an
    //   await point — lock, clone the sender/tx, drop, then await. if topic count grows
    //   large enough that lock contention matters, switch to a sharded map.
    topics: Mutex<HashMap<TopicId, TopicState>>,
}

// @todo(o11y): broadcast channel capacity is hardcoded at 256 — may need tuning
//   under high-throughput checkpoint storms. monitor for Lagged events.
const BROADCAST_CAPACITY: usize = 256;

impl GossipHandle {
    pub fn new(gossip: Gossip) -> Self {
        Self {
            gossip,
            topics: Mutex::new(HashMap::new()),
        }
    }

    pub async fn join_topic(
        &self,
        topic: TopicId,
        peers: Vec<EndpointId>,
    ) -> Result<(), GossipError> {
        {
            let topics = self.topics.lock();
            if topics.contains_key(&topic) {
                return Ok(());
            }
        }

        let gossip_topic = self.gossip.subscribe_and_join(topic, peers).await?;
        let (sender, mut receiver) = gossip_topic.split();
        let (updates_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let tx = updates_tx.clone();

        let recv_task = tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(result) = receiver.next().await {
                match result {
                    Ok(Event::Received(msg)) => {
                        match rkyv::from_bytes::<HeadUpdate, rancor::Error>(&msg.content) {
                            Ok(update) => {
                                // @todo(o11y): ignoring send errors here — means no active
                                //   subscribers, which is fine. log once metrics land.
                                let _ = tx.send(update);
                            }
                            Err(_) => {
                                // @todo(o11y): log malformed gossip message with peer id
                                //   once tracing is wired up
                            }
                        }
                    }
                    Ok(Event::NeighborUp(_) | Event::NeighborDown(_) | Event::Lagged) => {
                        // @todo(o11y): track neighbor churn and lag events in metrics
                    }
                    Err(_) => {
                        // @todo(o11y): gossip receiver stream error — log and break
                        //   to avoid spinning on a broken stream
                        break;
                    }
                }
            }
        });

        let mut topics = self.topics.lock();
        topics.insert(
            topic,
            TopicState {
                sender,
                updates_tx,
                _recv_task: recv_task,
            },
        );

        Ok(())
    }

    pub async fn broadcast_head_update(
        &self,
        topic: TopicId,
        update: &HeadUpdate,
    ) -> Result<(), GossipError> {
        let sender = {
            let topics = self.topics.lock();
            let state = topics
                .get(&topic)
                .ok_or(GossipError::TopicNotJoined(topic))?;
            state.sender.clone()
        };

        let bytes = rkyv::to_bytes::<rancor::Error>(update)
            .map_err(|e: rancor::Error| GossipError::Rkyv(e.to_string()))?;

        sender.broadcast(Bytes::from(bytes.to_vec())).await?;
        Ok(())
    }

    pub fn subscribe_updates(
        &self,
        topic: TopicId,
    ) -> Result<broadcast::Receiver<HeadUpdate>, GossipError> {
        let topics = self.topics.lock();
        let state = topics
            .get(&topic)
            .ok_or(GossipError::TopicNotJoined(topic))?;
        Ok(state.updates_tx.subscribe())
    }
}
