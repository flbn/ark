// tldr: reactive sync listener — bridges gossip HeadUpdate events to targeted blob fetches.
// spawns one task per repo topic. when a HeadUpdate arrives, fetches the referenced blob
// from known providers. this is the "gossip event → fetch" steady-state path.

use std::collections::BTreeSet;

use iroh::EndpointAddr;
use iroh_blobs::store::fs::FsStore;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::network::sync::fetch_missing_blobs;
use crate::network::HeadUpdate;

// @todo(o11y): SyncListener holds no in-flight dedupe set — duplicate HeadUpdates
//   for the same blob_hash will trigger redundant (but idempotent) downloads.
//   acceptable because iroh's downloader is content-addressed and won't re-download
//   data already present in the store. add a small HashSet<BlobHash> if this becomes noisy.
pub struct SyncListener {
    _task: JoinHandle<()>,
}

impl SyncListener {
    // @todo(o11y): providers list is static for the lifetime of the listener.
    //   if peers come and go, the listener won't discover new providers until restart.
    //   acceptable for now; dynamic provider discovery deferred until needed.
    pub fn spawn(
        mut updates_rx: broadcast::Receiver<HeadUpdate>,
        store: FsStore,
        endpoint: iroh::Endpoint,
        providers: Vec<EndpointAddr>,
    ) -> Self {
        let task = tokio::spawn(async move {
            loop {
                match updates_rx.recv().await {
                    Ok(update) => {
                        let hash = update.hash();
                        let mut missing = BTreeSet::new();
                        missing.insert(hash);

                        // @todo(o11y): fetch errors are silently dropped here.
                        //   wire up tracing once the logging stack is chosen.
                        let _ =
                            fetch_missing_blobs(&store, &endpoint, &missing, providers.clone())
                                .await;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // @todo(o11y): lagged means we dropped messages — log this
                        //   once tracing is wired. for now, continue and catch up
                        //   via the next update or reconciliation.
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        Self { _task: task }
    }
}
