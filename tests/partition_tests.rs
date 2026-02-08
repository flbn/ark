use ark::domain::{BlobMetadata, BlobType, RefName, RemoteTicket};
use ark::storage::BlobStore;
use camino::Utf8Path;
use tokio::io::AsyncReadExt;
use std::time::Duration;

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

fn make_meta(ts: u64) -> BlobMetadata {
    BlobMetadata {
        blob_type: BlobType::File,
        created_at: ts,
        local_only: false,
    }
}

// --- test protocol 1.2: network partition tolerance ---
// all operations must succeed fully offline (no peers configured, no network).
// on macOS we cannot use `unshare -n`, so we verify the store never blocks
// on network I/O by enforcing a 5-second timeout on every operation.

#[tokio::test]
async fn all_store_operations_succeed_offline_within_timeout() {
    let timeout_dur = Duration::from_secs(5);
    let (_dir, store) = tmp_store().await;

    let hash = tokio::time::timeout(timeout_dur, async {
        store
            .put_object(b"offline put", make_meta(1))
            .await
            .expect("put_object failed offline")
    })
    .await
    .expect("put_object timed out — possible network dependency");

    tokio::time::timeout(timeout_dur, async {
        let mut reader = store.blobs.get_reader(hash).await.expect("get_reader failed offline");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read failed offline");
        assert_eq!(buf, b"offline put");
    })
    .await
    .expect("get_reader timed out — possible network dependency");

    tokio::time::timeout(timeout_dur, async {
        let bytes = store.blobs.get_bytes(hash).await.expect("get_bytes failed offline");
        assert_eq!(bytes, b"offline put");
    })
    .await
    .expect("get_bytes timed out — possible network dependency");

    tokio::time::timeout(timeout_dur, async {
        let meta = store
            .index
            .get_blob_meta(hash)
            .expect("index read failed offline")
            .expect("metadata missing offline");
        assert_eq!(meta.created_at, 1);
    })
    .await
    .expect("index metadata read timed out — possible network dependency");

    let ref_name = RefName::new("offline/head").expect("bad ref name");
    tokio::time::timeout(timeout_dur, async {
        store
            .update_reference(ref_name.clone(), hash)
            .await
            .expect("update_reference failed offline");
    })
    .await
    .expect("update_reference timed out — possible network dependency");

    tokio::time::timeout(timeout_dur, async {
        let fetched = store
            .index
            .get_ref(&ref_name)
            .expect("ref read failed offline")
            .expect("ref missing offline");
        assert_eq!(fetched, hash);
    })
    .await
    .expect("ref read timed out — possible network dependency");

    let ticket = RemoteTicket::new(vec![1, 2, 3]).expect("bad ticket");
    tokio::time::timeout(timeout_dur, async {
        store
            .index
            .store_remote("origin", &ticket)
            .expect("store_remote failed offline");
    })
    .await
    .expect("store_remote timed out — possible network dependency");

    tokio::time::timeout(timeout_dur, async {
        let fetched = store
            .index
            .get_remote("origin")
            .expect("get_remote failed offline")
            .expect("remote missing offline");
        assert_eq!(fetched, ticket);
    })
    .await
    .expect("get_remote timed out — possible network dependency");

    store.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn multiple_put_then_read_cycles_offline() {
    let (_dir, store) = tmp_store().await;
    let mut hashes = Vec::new();

    for i in 0..50u64 {
        let data = format!("offline blob {i}");
        let hash = store
            .put_object(data.as_bytes(), make_meta(i))
            .await
            .expect("put failed offline");
        hashes.push((hash, data));
    }

    for (hash, expected) in &hashes {
        let bytes = store
            .blobs
            .get_bytes(*hash)
            .await
            .expect("get_bytes failed offline");
        assert_eq!(bytes, expected.as_bytes());
    }

    store.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn promote_blob_offline() {
    let (_dir, store) = tmp_store().await;

    let hash = store
        .put_object(
            b"local-only snapshot",
            BlobMetadata {
                blob_type: BlobType::Commit,
                created_at: 1,
                local_only: true,
            },
        )
        .await
        .expect("put failed offline");

    let meta = store
        .index
        .get_blob_meta(hash)
        .expect("read failed")
        .expect("missing");
    assert!(meta.local_only);

    store.promote_blob(hash).expect("promote failed offline");

    let meta = store
        .index
        .get_blob_meta(hash)
        .expect("read failed")
        .expect("missing after promote");
    assert!(!meta.local_only);

    let visible = store.index.list_shared_blobs().expect("list failed");
    assert!(visible.contains(&hash));

    store.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn delete_object_offline() {
    let (_dir, store) = tmp_store().await;

    let hash = store
        .put_object(b"to be deleted", make_meta(1))
        .await
        .expect("put failed offline");

    assert!(store.index.get_blob_meta(hash).expect("read failed").is_some());

    store.delete_object(hash).expect("delete failed offline");

    assert!(store.index.get_blob_meta(hash).expect("read failed").is_none());

    store.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn shutdown_and_reopen_offline() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();

    let data = b"survives restart offline";
    let hash;
    let ref_name = RefName::new("offline/persist").expect("bad ref name");
    let ticket = RemoteTicket::new(vec![10, 20, 30]).expect("bad ticket");

    {
        let store = BlobStore::new(&root).await.expect("create failed");
        hash = store
            .put_object(data, make_meta(7777))
            .await
            .expect("put failed");
        store
            .update_reference(ref_name.clone(), hash)
            .await
            .expect("update ref failed");
        store
            .index
            .store_remote("backup", &ticket)
            .expect("store remote failed");
        store.shutdown().await.expect("shutdown failed");
    }

    {
        let store = BlobStore::new(&root).await.expect("reopen failed");

        let meta = store
            .index
            .get_blob_meta(hash)
            .expect("index read failed")
            .expect("metadata lost after restart");
        assert_eq!(meta.created_at, 7777);

        let bytes = store
            .blobs
            .get_bytes(hash)
            .await
            .expect("get_bytes failed after restart");
        assert_eq!(bytes, data);

        let fetched_ref = store
            .index
            .get_ref(&ref_name)
            .expect("ref read failed")
            .expect("ref lost after restart");
        assert_eq!(fetched_ref, hash);

        let fetched_remote = store
            .index
            .get_remote("backup")
            .expect("get_remote failed")
            .expect("remote lost after restart");
        assert_eq!(fetched_remote, ticket);

        store.shutdown().await.expect("shutdown failed");
    }
}
