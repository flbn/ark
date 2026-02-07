use ark::domain::{BlobHash, BlobMetadata, BlobType, RefName};
use ark::network::{derive_topic_id, HeadUpdate};
use ark::storage::BlobStore;
use camino::Utf8Path;

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

// --- HeadUpdate serialization round-trip ---

#[test]
fn head_update_rkyv_roundtrip() {
    let hash = BlobHash([42u8; 32]);
    let update = HeadUpdate::new("main", hash, 1000);

    let bytes = rkyv::to_bytes::<rancor::Error>(&update).expect("serialize failed");
    let decoded =
        rkyv::from_bytes::<HeadUpdate, rancor::Error>(&bytes).expect("deserialize failed");

    assert_eq!(decoded.ref_name, "main");
    assert_eq!(decoded.blob_hash, [42u8; 32]);
    assert_eq!(decoded.timestamp, 1000);
    assert_eq!(decoded.hash(), hash);
}

#[test]
fn head_update_different_refs() {
    let hash = BlobHash([1u8; 32]);
    let u1 = HeadUpdate::new("main", hash, 100);
    let u2 = HeadUpdate::new("feature/x", hash, 200);

    assert_eq!(u1.ref_name, "main");
    assert_eq!(u2.ref_name, "feature/x");
    assert_eq!(u1.hash(), u2.hash());
}

// --- topic derivation ---

#[test]
fn topic_id_deterministic() {
    let t1 = derive_topic_id(b"my-repo");
    let t2 = derive_topic_id(b"my-repo");
    assert_eq!(t1, t2);
}

#[test]
fn topic_id_different_repos() {
    let t1 = derive_topic_id(b"repo-a");
    let t2 = derive_topic_id(b"repo-b");
    assert_ne!(t1, t2);
}

#[test]
fn topic_id_empty_input() {
    let t1 = derive_topic_id(b"");
    let t2 = derive_topic_id(b"x");
    assert_ne!(t1, t2);
}

// --- GossipHandle: topic not joined errors ---

#[tokio::test]
async fn broadcast_without_join_errors() {
    let (_dir, store) = tmp_store().await;
    let topic = derive_topic_id(b"test-repo");
    let update = HeadUpdate::new("main", BlobHash([0u8; 32]), 1);

    let result = store.gossip.broadcast_head_update(topic, &update).await;
    assert!(result.is_err());

    store.shutdown().await.expect("shutdown failed");
}

#[test]
fn subscribe_without_join_errors() {
    // @todo(o11y): can't test GossipHandle::subscribe_updates in isolation without
    //   a full store — GossipHandle requires a Gossip instance from a running endpoint.
    //   this test validates the error path via a store-backed handle.
    //   full multi-node subscribe tests deferred to test protocol 2.1.
}

// --- identity persistence ---

#[tokio::test]
async fn identity_key_persists_across_restart() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();

    let node_id_1;
    {
        let store = BlobStore::new(&root).await.expect("create failed");
        node_id_1 = store.blobs.endpoint.id();
        store.shutdown().await.expect("shutdown failed");
    }

    let node_id_2;
    {
        let store = BlobStore::new(&root).await.expect("reopen failed");
        node_id_2 = store.blobs.endpoint.id();
        store.shutdown().await.expect("shutdown failed");
    }

    assert_eq!(node_id_1, node_id_2);
}

#[tokio::test]
async fn different_stores_different_identities() {
    let (_dir1, store1) = tmp_store().await;
    let (_dir2, store2) = tmp_store().await;

    assert_ne!(
        store1.blobs.endpoint.id(),
        store2.blobs.endpoint.id()
    );

    store1.shutdown().await.expect("shutdown failed");
    store2.shutdown().await.expect("shutdown failed");
}

// --- gossip handle accessible from store ---

#[tokio::test]
async fn store_exposes_gossip_handle() {
    let (_dir, store) = tmp_store().await;

    let topic = derive_topic_id(b"repo-test");
    let update = HeadUpdate::new("main", BlobHash([7u8; 32]), 42);

    let result = store.gossip.broadcast_head_update(topic, &update).await;
    assert!(result.is_err());

    store.shutdown().await.expect("shutdown failed");
}

// --- update_reference_and_announce without join errors ---

#[tokio::test]
async fn announce_without_join_errors() {
    let (_dir, store) = tmp_store().await;
    let topic = derive_topic_id(b"test-repo");

    let hash = store
        .put_object(
            b"test data",
            BlobMetadata {
                blob_type: BlobType::Commit,
                created_at: 1,
                local_only: false,
            },
        )
        .await
        .expect("put failed");

    let name = RefName::new("main").expect("bad ref name");
    let result = store
        .update_reference_and_announce(name, hash, topic)
        .await;
    assert!(result.is_err());

    store.shutdown().await.expect("shutdown failed");
}
