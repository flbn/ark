use std::time::Duration;

use ark::domain::{BlobMetadata, BlobType};
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

// --- cold-start recon: identical stores produce no fetches ---

#[tokio::test]
async fn recon_identical_stores_no_fetch() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let stores = [&store_a, &store_b];
    seed_addresses(&stores);

    let data = b"shared blob";
    store_a
        .put_object(data, commit_meta(1))
        .await
        .expect("A put failed");
    store_b
        .put_object(data, commit_meta(1))
        .await
        .expect("B put failed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = store_a
        .reconcile_with_peer(store_b.blobs.endpoint.addr(), 8)
        .await
        .expect("recon failed");

    assert_eq!(result.fetched, 0);
    assert!(result.failed.is_empty());

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}

// --- cold-start recon: one-sided difference ---

#[tokio::test]
async fn recon_fetches_missing_blobs() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let stores = [&store_a, &store_b];
    seed_addresses(&stores);

    let shared_data = b"shared commit";
    store_a
        .put_object(shared_data, commit_meta(1))
        .await
        .expect("A put shared failed");
    store_b
        .put_object(shared_data, commit_meta(1))
        .await
        .expect("B put shared failed");

    let extra_data = b"only on B";
    let extra_hash = store_b
        .put_object(extra_data, commit_meta(2))
        .await
        .expect("B put extra failed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = store_a
        .reconcile_with_peer(store_b.blobs.endpoint.addr(), 8)
        .await
        .expect("recon failed");

    assert_eq!(result.fetched, 1);
    assert!(result.failed.is_empty());

    let fetched = store_a
        .blobs
        .get_bytes(extra_hash)
        .await
        .expect("read fetched blob failed");
    assert_eq!(fetched, extra_data);

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}

// --- cold-start recon: both sides have unique blobs ---

#[tokio::test]
async fn recon_bidirectional_difference() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let stores = [&store_a, &store_b];
    seed_addresses(&stores);

    let a_only_data = b"only on A";
    let a_only_hash = store_a
        .put_object(a_only_data, commit_meta(1))
        .await
        .expect("A put failed");

    let b_only_data = b"only on B";
    let b_only_hash = store_b
        .put_object(b_only_data, commit_meta(2))
        .await
        .expect("B put failed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result_a = store_a
        .reconcile_with_peer(store_b.blobs.endpoint.addr(), 8)
        .await
        .expect("A recon failed");

    assert_eq!(result_a.fetched, 1);

    let fetched = store_a
        .blobs
        .get_bytes(b_only_hash)
        .await
        .expect("A read fetched blob failed");
    assert_eq!(fetched, b_only_data);

    let result_b = store_b
        .reconcile_with_peer(store_a.blobs.endpoint.addr(), 8)
        .await
        .expect("B recon failed");

    assert_eq!(result_b.fetched, 1);

    let fetched = store_b
        .blobs
        .get_bytes(a_only_hash)
        .await
        .expect("B read fetched blob failed");
    assert_eq!(fetched, a_only_data);

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}

// --- cold-start recon: empty initiator catches up ---

#[tokio::test]
async fn recon_empty_node_catches_up() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let stores = [&store_a, &store_b];
    seed_addresses(&stores);

    let mut expected: Vec<(ark::domain::BlobHash, Vec<u8>)> = Vec::new();
    for i in 0u8..5 {
        let data = format!("commit {i}").into_bytes();
        let hash = store_b
            .put_object(&data, commit_meta(i as u64))
            .await
            .expect("B put failed");
        expected.push((hash, data));
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = store_a
        .reconcile_with_peer(store_b.blobs.endpoint.addr(), 8)
        .await
        .expect("recon failed");

    assert_eq!(result.fetched, 5);
    assert!(result.failed.is_empty());

    for (hash, data) in &expected {
        let fetched = store_a
            .blobs
            .get_bytes(*hash)
            .await
            .expect("read failed");
        assert_eq!(fetched, *data);
    }

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}

// --- cold-start recon: both empty is a no-op ---

#[tokio::test]
async fn recon_both_empty_noop() {
    let (_dir_a, store_a) = tmp_store().await;
    let (_dir_b, store_b) = tmp_store().await;

    let stores = [&store_a, &store_b];
    seed_addresses(&stores);

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = store_a
        .reconcile_with_peer(store_b.blobs.endpoint.addr(), 8)
        .await
        .expect("recon failed");

    assert_eq!(result.fetched, 0);
    assert!(result.failed.is_empty());

    store_a.shutdown().await.expect("shutdown a failed");
    store_b.shutdown().await.expect("shutdown b failed");
}
