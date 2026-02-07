use ark::domain::{BlobHash, BlobMetadata, BlobType};
use ark::storage::BlobStore;
use ark::storage::index::Index;
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

fn tmp_index() -> (tempfile::TempDir, Index) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .join("test.redb");
    let index = Index::new(&db_path).expect("failed to create index");
    (dir, index)
}

fn make_hash(seed: u8) -> BlobHash {
    BlobHash([seed; 32])
}

// --- local_only flag round-trip ---

#[test]
fn local_only_blob_excluded_from_shared() {
    let (_dir, index) = tmp_index();
    let hash = make_hash(1);
    let meta = BlobMetadata {
        blob_type: BlobType::File,
        created_at: 100,
        local_only: true,
    };

    index.register_blob(hash, meta).expect("register failed");

    let visible = index
        .list_shared_blobs()
        .expect("list failed");
    assert!(!visible.contains(&hash));
}

#[test]
fn non_local_blob_included_in_shared() {
    let (_dir, index) = tmp_index();
    let hash = make_hash(2);
    let meta = BlobMetadata {
        blob_type: BlobType::Commit,
        created_at: 200,
        local_only: false,
    };

    index.register_blob(hash, meta).expect("register failed");

    let visible = index
        .list_shared_blobs()
        .expect("list failed");
    assert!(visible.contains(&hash));
}

// --- set_blob_local_only ---

#[test]
fn set_blob_local_only_promotes() {
    let (_dir, index) = tmp_index();
    let hash = make_hash(3);
    let meta = BlobMetadata {
        blob_type: BlobType::Commit,
        created_at: 300,
        local_only: true,
    };

    index.register_blob(hash, meta).expect("register failed");

    let visible = index.list_shared_blobs().expect("list failed");
    assert!(!visible.contains(&hash));

    index
        .set_blob_local_only(hash, false)
        .expect("promote failed");

    let visible = index.list_shared_blobs().expect("list failed");
    assert!(visible.contains(&hash));

    let fetched = index
        .get_blob_meta(hash)
        .expect("read failed")
        .expect("missing");
    assert!(!fetched.local_only);
    assert_eq!(fetched.created_at, 300);
}

#[test]
fn set_blob_local_only_demotes() {
    let (_dir, index) = tmp_index();
    let hash = make_hash(4);
    let meta = BlobMetadata {
        blob_type: BlobType::File,
        created_at: 400,
        local_only: false,
    };

    index.register_blob(hash, meta).expect("register failed");

    index
        .set_blob_local_only(hash, true)
        .expect("demote failed");

    let fetched = index
        .get_blob_meta(hash)
        .expect("read failed")
        .expect("missing");
    assert!(fetched.local_only);

    let visible = index.list_shared_blobs().expect("list failed");
    assert!(!visible.contains(&hash));
}

#[test]
fn set_blob_local_only_nonexistent_errors() {
    let (_dir, index) = tmp_index();
    let result = index.set_blob_local_only(make_hash(99), false);
    assert!(result.is_err());
}

#[test]
fn promote_idempotent() {
    let (_dir, index) = tmp_index();
    let hash = make_hash(5);
    let meta = BlobMetadata {
        blob_type: BlobType::Commit,
        created_at: 500,
        local_only: false,
    };

    index.register_blob(hash, meta).expect("register failed");

    index
        .set_blob_local_only(hash, false)
        .expect("idempotent promote failed");

    let fetched = index
        .get_blob_meta(hash)
        .expect("read failed")
        .expect("missing");
    assert!(!fetched.local_only);
}

// --- mixed blobs ---

#[test]
fn mixed_local_and_visible_blobs() {
    let (_dir, index) = tmp_index();

    let local_hashes: Vec<BlobHash> = (10..15u8).map(make_hash).collect();
    let visible_hashes: Vec<BlobHash> = (20..25u8).map(make_hash).collect();

    for &h in &local_hashes {
        index
            .register_blob(
                h,
                BlobMetadata {
                    blob_type: BlobType::File,
                    created_at: 0,
                    local_only: true,
                },
            )
            .expect("register failed");
    }

    for &h in &visible_hashes {
        index
            .register_blob(
                h,
                BlobMetadata {
                    blob_type: BlobType::Commit,
                    created_at: 0,
                    local_only: false,
                },
            )
            .expect("register failed");
    }

    let visible = index.list_shared_blobs().expect("list failed");
    for h in &visible_hashes {
        assert!(visible.contains(h));
    }
    for h in &local_hashes {
        assert!(!visible.contains(h));
    }
    assert_eq!(visible.len(), visible_hashes.len());
}

// --- promote_blob via BlobStore ---

#[tokio::test]
async fn store_promote_blob() {
    let (_dir, store) = tmp_store().await;

    let hash = store
        .put_object(
            b"ephemeral snapshot",
            BlobMetadata {
                blob_type: BlobType::Commit,
                created_at: 1,
                local_only: true,
            },
        )
        .await
        .expect("put failed");

    let visible = store.index.list_shared_blobs().expect("list failed");
    assert!(!visible.contains(&hash));

    store.promote_blob(hash).expect("promote failed");

    let visible = store.index.list_shared_blobs().expect("list failed");
    assert!(visible.contains(&hash));

    store.shutdown().await.expect("shutdown failed");
}

// --- promotion survives reopen ---

#[tokio::test]
async fn promotion_persists_across_restart() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();

    let hash;

    {
        let store = BlobStore::new(&root).await.expect("create failed");
        hash = store
            .put_object(
                b"promote me",
                BlobMetadata {
                    blob_type: BlobType::Commit,
                    created_at: 42,
                    local_only: true,
                },
            )
            .await
            .expect("put failed");

        store.promote_blob(hash).expect("promote failed");
        store.shutdown().await.expect("shutdown failed");
    }

    {
        let store = BlobStore::new(&root).await.expect("reopen failed");

        let meta = store
            .index
            .get_blob_meta(hash)
            .expect("read failed")
            .expect("missing after restart");
        assert!(!meta.local_only);
        assert_eq!(meta.created_at, 42);

        let visible = store.index.list_shared_blobs().expect("list failed");
        assert!(visible.contains(&hash));

        store.shutdown().await.expect("shutdown failed");
    }
}
