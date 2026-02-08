use std::time::Duration;

use ark::domain::{BlobHash, BlobMetadata, BlobType, RefName};
use ark::network::derive_topic_id;
use ark::network::listener::SyncListener;
use ark::storage::BlobStore;
use camino::Utf8Path;
use iroh::discovery::static_provider::StaticProvider;

async fn tmp_store() -> (tempfile::TempDir, BlobStore) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();
    let store = BlobStore::new(&root)
        .await
        .expect("failed to create store");
    (dir, store)
}

fn seed_addresses(stores: &[&BlobStore]) {
    for (i, store_i) in stores.iter().enumerate() {
        let provider = StaticProvider::new();
        for (j, store_j) in stores.iter().enumerate() {
            if i != j {
                provider.add_endpoint_info(store_j.blobs.endpoint.addr());
            }
        }
        store_i.blobs.endpoint.discovery().add(provider);
    }
}

fn commit_meta(ts: u64) -> BlobMetadata {
    BlobMetadata {
        blob_type: BlobType::Commit,
        created_at: ts,
        local_only: false,
    }
}

#[tokio::test]
async fn listener_fetches_blob_on_gossip_update() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let stores = [&store_a, &store_b];
    seed_addresses(&stores);

    let ids: Vec<_> = stores.iter().map(|s| s.blobs.endpoint.id()).collect();

    let topic = derive_topic_id(b"listener-test");

    store_a
        .gossip
        .join_topic(topic, vec![ids[1]])
        .await
        .expect("A join failed");
    store_b
        .gossip
        .join_topic(topic, vec![ids[0]])
        .await
        .expect("B join failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let updates_rx = store_b
        .gossip
        .subscribe_updates(topic)
        .expect("B subscribe failed");

    let _listener = SyncListener::spawn(
        updates_rx,
        store_b.blobs.store.clone(),
        store_b.blobs.endpoint.clone(),
        vec![store_a.blobs.endpoint.addr()],
        store_b.index.clone(),
    );

    let data = b"reactive sync test blob";
    let hash = store_a
        .put_object(data, commit_meta(100))
        .await
        .expect("A put failed");

    let ref_name = RefName::new("main").expect("bad ref");
    store_a
        .update_reference_and_announce(ref_name, hash, topic)
        .await
        .expect("A announce failed");

    let timeout = Duration::from_secs(10);
    let poll_interval = Duration::from_millis(100);
    let start = tokio::time::Instant::now();

    let fetched = loop {
        if start.elapsed() > timeout {
            break false;
        }
        match store_b.blobs.get_bytes(hash).await {
            Ok(bytes) if bytes == data => break true,
            _ => tokio::time::sleep(poll_interval).await,
        }
    };

    assert!(fetched, "listener did not fetch blob within timeout");

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}

#[tokio::test]
async fn listener_fetches_multiple_blobs() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let stores = [&store_a, &store_b];
    seed_addresses(&stores);

    let ids: Vec<_> = stores.iter().map(|s| s.blobs.endpoint.id()).collect();

    let topic = derive_topic_id(b"listener-multi-test");

    store_a
        .gossip
        .join_topic(topic, vec![ids[1]])
        .await
        .expect("A join failed");
    store_b
        .gossip
        .join_topic(topic, vec![ids[0]])
        .await
        .expect("B join failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let updates_rx = store_b
        .gossip
        .subscribe_updates(topic)
        .expect("B subscribe failed");

    let _listener = SyncListener::spawn(
        updates_rx,
        store_b.blobs.store.clone(),
        store_b.blobs.endpoint.clone(),
        vec![store_a.blobs.endpoint.addr()],
        store_b.index.clone(),
    );

    let mut expected: Vec<(BlobHash, Vec<u8>)> = Vec::new();
    for i in 0u8..3 {
        let data = format!("blob number {i}").into_bytes();
        let hash = store_a
            .put_object(&data, commit_meta(i as u64))
            .await
            .expect("A put failed");

        let ref_name = RefName::new(format!("ref/{i}")).expect("bad ref");
        store_a
            .update_reference_and_announce(ref_name, hash, topic)
            .await
            .expect("A announce failed");

        expected.push((hash, data));
    }

    let timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(100);
    let start = tokio::time::Instant::now();

    let all_fetched = loop {
        if start.elapsed() > timeout {
            break false;
        }

        let mut all_ok = true;
        for (hash, data) in &expected {
            match store_b.blobs.get_bytes(*hash).await {
                Ok(bytes) if bytes == *data => {}
                _ => {
                    all_ok = false;
                    break;
                }
            }
        }

        if all_ok {
            break true;
        }
        tokio::time::sleep(poll_interval).await;
    };

    assert!(all_fetched, "listener did not fetch all blobs within timeout");

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}
