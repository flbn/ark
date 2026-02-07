use ark::domain::{BlobMetadata, BlobType, RefName};
use ark::storage::BlobStore;
use camino::Utf8Path;
use tokio::io::AsyncReadExt;

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

// --- basic put + read ---

#[tokio::test]
async fn put_and_read_blob() {
    let (_dir, store) = tmp_store().await;

    let data = b"hello sovereign archive";
    let hash = store
        .put_object(data, make_meta(1))
        .await
        .expect("put failed");

    let mut reader = store.blobs.get_reader(hash).await.expect("reader failed");
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read failed");
    assert_eq!(buf, data);

    store.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn put_object_indexes_metadata() {
    let (_dir, store) = tmp_store().await;

    let hash = store
        .put_object(b"payload", make_meta(42))
        .await
        .expect("put failed");

    let meta = store
        .index
        .get_blob_meta(hash)
        .expect("index read failed")
        .expect("metadata missing");
    assert_eq!(meta.created_at, 42);
    assert!(matches!(meta.blob_type, BlobType::File));

    store.shutdown().await.expect("shutdown failed");
}

// --- refs through store ---

#[tokio::test]
async fn update_and_read_ref_via_store() {
    let (_dir, store) = tmp_store().await;

    let hash = store
        .put_object(b"ref target", make_meta(1))
        .await
        .expect("put failed");

    let name = RefName::new("main").expect("bad ref name");
    store
        .update_reference(name.clone(), hash)
        .await
        .expect("update ref failed");

    let fetched = store
        .index
        .get_ref(&name)
        .expect("read failed")
        .expect("missing ref");
    assert_eq!(fetched, hash);

    store.shutdown().await.expect("shutdown failed");
}

// --- content-addressing: same data -> same hash ---

#[tokio::test]
async fn same_content_same_hash() {
    let (_dir, store) = tmp_store().await;
    let data = b"deterministic content addressing";

    let h1 = store
        .put_object(data, make_meta(1))
        .await
        .expect("put 1 failed");
    let h2 = store
        .put_object(data, make_meta(2))
        .await
        .expect("put 2 failed");

    assert_eq!(h1, h2);

    store.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
async fn different_content_different_hash() {
    let (_dir, store) = tmp_store().await;

    let h1 = store
        .put_object(b"aaa", make_meta(1))
        .await
        .expect("put 1 failed");
    let h2 = store
        .put_object(b"bbb", make_meta(2))
        .await
        .expect("put 2 failed");

    assert_ne!(h1, h2);

    store.shutdown().await.expect("shutdown failed");
}

// --- persistence across restart (test protocol 1.1 integrated) ---

#[tokio::test]
async fn data_survives_shutdown_and_reopen() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();

    let data = b"this data must survive restart";
    let hash;

    {
        let store = BlobStore::new(&root).await.expect("create failed");
        hash = store
            .put_object(data, make_meta(9999))
            .await
            .expect("put failed");
        store.shutdown().await.expect("shutdown failed");
    }

    {
        let store = BlobStore::new(&root).await.expect("reopen failed");

        let meta = store
            .index
            .get_blob_meta(hash)
            .expect("index read failed")
            .expect("metadata lost after restart");
        assert_eq!(meta.created_at, 9999);

        let mut reader = store.blobs.get_reader(hash).await.expect("reader failed");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read failed");
        assert_eq!(buf, data);

        store.shutdown().await.expect("shutdown failed");
    }
}

// --- offline local operations (test protocol 1.2 simplified) ---
// iroh with FsStore operates locally without needing network.
// we verify that put + get work without any peer connections.

#[tokio::test]
async fn local_only_put_and_read_no_peers() {
    let (_dir, store) = tmp_store().await;

    let data = b"offline-first: no peers needed for local ops";
    let hash = store
        .put_object(data, make_meta(1))
        .await
        .expect("local put failed");

    let mut reader = store.blobs.get_reader(hash).await.expect("reader failed");
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read failed");
    assert_eq!(buf, data);

    store.shutdown().await.expect("shutdown failed");
}

// --- multiple blobs ---

#[tokio::test]
async fn store_and_read_many_blobs() {
    let (_dir, store) = tmp_store().await;
    let mut hashes = Vec::new();

    for i in 0..50u64 {
        let data = format!("blob number {i}");
        let hash = store
            .put_object(data.as_bytes(), make_meta(i))
            .await
            .expect("put failed");
        hashes.push((hash, data));
    }

    for (hash, expected) in &hashes {
        let mut reader = store.blobs.get_reader(*hash).await.expect("reader failed");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read failed");
        assert_eq!(buf, expected.as_bytes());
    }

    store.shutdown().await.expect("shutdown failed");
}

// --- delete_object ---

#[tokio::test]
async fn delete_object_removes_from_index() {
    let (_dir, store) = tmp_store().await;

    let hash = store
        .put_object(
            b"ephemeral data",
            BlobMetadata {
                blob_type: BlobType::File,
                created_at: 1,
                local_only: false,
            },
        )
        .await
        .expect("put_object failed");

    assert!(store.index.get_blob_meta(hash).expect("read meta failed").is_some());

    store.delete_object(hash).expect("delete_object failed");

    assert!(store.index.get_blob_meta(hash).expect("read meta failed").is_none());

    store.shutdown().await.expect("shutdown failed");
}
