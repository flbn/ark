use std::time::Duration;

use ark::domain::{BlobMetadata, BlobType, RefName};
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

#[tokio::test]
async fn server_publish_client_receives_via_sync_listener() {
    let (_dir_server, server) = tmp_store().await;
    let (_dir_client, client) = tmp_store().await;

    let stores = [&server, &client];
    seed_addresses(&stores);

    let ids: Vec<_> = stores.iter().map(|s| s.blobs.endpoint.id()).collect();
    let topic = derive_topic_id(b"e2e-client-server-test");

    server
        .gossip
        .join_topic(topic, vec![ids[1]])
        .await
        .expect("server join failed");
    client
        .gossip
        .join_topic(topic, vec![ids[0]])
        .await
        .expect("client join failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let updates_rx = client
        .gossip
        .subscribe_updates(topic)
        .expect("client subscribe failed");

    let _listener = SyncListener::spawn(
        updates_rx,
        client.blobs.store.clone(),
        client.blobs.endpoint.clone(),
        vec![server.blobs.endpoint.addr()],
        client.index.clone(),
    );

    let image_data = b"\x89PNG\r\n\x1a\nfake image payload for e2e test - \
        this simulates a real file being published and synced between two ark nodes";

    let meta = BlobMetadata {
        blob_type: BlobType::File,
        created_at: 1000,
        local_only: false,
    };
    let hash = server
        .put_object(image_data, meta)
        .await
        .expect("server put_object failed");

    let ref_name = RefName::new("images/photo.png").expect("bad ref name");
    server
        .update_reference_and_announce(ref_name, hash, topic)
        .await
        .expect("server announce failed");

    let timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(200);
    let start = tokio::time::Instant::now();

    let arrived = loop {
        if start.elapsed() > timeout {
            break false;
        }
        match client.blobs.get_bytes(hash).await {
            Ok(bytes) if bytes == image_data => break true,
            _ => tokio::time::sleep(poll_interval).await,
        }
    };

    assert!(arrived, "client did not receive blob within timeout");

    let client_bytes = client
        .blobs
        .get_bytes(hash)
        .await
        .expect("client read failed");
    assert_eq!(
        client_bytes, image_data,
        "blob data mismatch: transfer corruption"
    );

    let client_meta = client
        .index
        .get_blob_meta(hash)
        .expect("client index read failed")
        .expect("blob not registered in client index");
    assert!(matches!(client_meta.blob_type, BlobType::File));
    assert!(!client_meta.local_only);

    let client_ref = client
        .index
        .get_ref(&RefName::new("images/photo.png").expect("bad ref"))
        .expect("client ref read failed")
        .expect("ref not set on client");
    assert_eq!(client_ref, hash);

    server.shutdown().await.expect("server shutdown failed");
    client.shutdown().await.expect("client shutdown failed");
}

#[tokio::test]
async fn server_publish_multiple_files_client_receives_all() {
    let (_dir_server, server) = tmp_store().await;
    let (_dir_client, client) = tmp_store().await;

    let stores = [&server, &client];
    seed_addresses(&stores);

    let ids: Vec<_> = stores.iter().map(|s| s.blobs.endpoint.id()).collect();
    let topic = derive_topic_id(b"e2e-multi-file-test");

    server
        .gossip
        .join_topic(topic, vec![ids[1]])
        .await
        .expect("server join failed");
    client
        .gossip
        .join_topic(topic, vec![ids[0]])
        .await
        .expect("client join failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let updates_rx = client
        .gossip
        .subscribe_updates(topic)
        .expect("client subscribe failed");

    let _listener = SyncListener::spawn(
        updates_rx,
        client.blobs.store.clone(),
        client.blobs.endpoint.clone(),
        vec![server.blobs.endpoint.addr()],
        client.index.clone(),
    );

    let files: Vec<(&str, Vec<u8>)> = vec![
        ("images/a.png", vec![0xAAu8; 1024]),
        ("images/b.jpg", vec![0xBBu8; 2048]),
        ("docs/readme.md", b"# Ark\nDistributed archival system".to_vec()),
    ];

    let mut published = Vec::new();
    for (ref_str, data) in &files {
        let meta = BlobMetadata {
            blob_type: BlobType::File,
            created_at: 2000,
            local_only: false,
        };
        let hash = server
            .put_object(data, meta)
            .await
            .expect("server put failed");
        let rn = RefName::new(*ref_str).expect("bad ref");
        server
            .update_reference_and_announce(rn, hash, topic)
            .await
            .expect("announce failed");
        published.push((hash, data.clone(), *ref_str));
    }

    let timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(200);
    let start = tokio::time::Instant::now();

    let all_arrived = loop {
        if start.elapsed() > timeout {
            break false;
        }
        let mut ok = true;
        for (hash, data, _) in &published {
            match client.blobs.get_bytes(*hash).await {
                Ok(bytes) if bytes == *data => {}
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            break true;
        }
        tokio::time::sleep(poll_interval).await;
    };

    assert!(all_arrived, "client did not receive all blobs within timeout");

    for (hash, data, ref_str) in &published {
        let fetched = client
            .blobs
            .get_bytes(*hash)
            .await
            .expect("client read failed");
        assert_eq!(fetched, *data, "data mismatch for {ref_str}");

        let meta = client
            .index
            .get_blob_meta(*hash)
            .expect("index read failed")
            .expect("blob not in client index");
        assert!(matches!(meta.blob_type, BlobType::File));

        let ref_hash = client
            .index
            .get_ref(&RefName::new(*ref_str).expect("bad ref"))
            .expect("ref read failed")
            .expect("ref not set");
        assert_eq!(ref_hash, *hash);
    }

    server.shutdown().await.expect("server shutdown failed");
    client.shutdown().await.expect("client shutdown failed");
}
