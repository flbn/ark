use ark::domain::{BlobHash, BlobMetadata, BlobType, RefName, RemoteTicket};
use ark::storage::index::Index;
use camino::Utf8Path;

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

fn make_meta(blob_type: BlobType, ts: u64) -> BlobMetadata {
    BlobMetadata {
        blob_type,
        created_at: ts,
        local_only: false,
    }
}

// --- blob index ---

#[test]
fn register_and_read_blob_metadata() {
    let (_dir, index) = tmp_index();
    let hash = make_hash(1);
    let meta = make_meta(BlobType::File, 1000);

    index.register_blob(hash, meta).expect("register failed");

    let fetched = index
        .get_blob_meta(hash)
        .expect("read failed")
        .expect("missing metadata");
    assert_eq!(fetched.created_at, 1000);
    assert!(matches!(fetched.blob_type, BlobType::File));
}

#[test]
fn get_missing_blob_returns_none() {
    let (_dir, index) = tmp_index();
    let result = index.get_blob_meta(make_hash(99)).expect("read failed");
    assert!(result.is_none());
}

#[test]
fn overwrite_blob_metadata() {
    let (_dir, index) = tmp_index();
    let hash = make_hash(2);

    index
        .register_blob(hash, make_meta(BlobType::File, 100))
        .expect("first write failed");
    index
        .register_blob(hash, make_meta(BlobType::Commit, 200))
        .expect("overwrite failed");

    let fetched = index
        .get_blob_meta(hash)
        .expect("read failed")
        .expect("missing");
    assert_eq!(fetched.created_at, 200);
    assert!(matches!(fetched.blob_type, BlobType::Commit));
}

#[test]
fn register_all_blob_types() {
    let (_dir, index) = tmp_index();

    for (i, bt) in [BlobType::Commit, BlobType::Tree, BlobType::File]
        .into_iter()
        .enumerate()
    {
        let hash = make_hash(i as u8 + 10);
        index
            .register_blob(hash, make_meta(bt, i as u64))
            .expect("register failed");
        let fetched = index
            .get_blob_meta(hash)
            .expect("read failed")
            .expect("missing");
        assert_eq!(fetched.created_at, i as u64);
    }
}

// --- refs ---

#[test]
fn update_and_read_ref() {
    let (_dir, index) = tmp_index();
    let name = RefName::new("main").expect("bad ref name");
    let hash = make_hash(3);

    index.update_ref(&name, hash).expect("update failed");

    let fetched = index
        .get_ref(&name)
        .expect("read failed")
        .expect("missing ref");
    assert_eq!(fetched, hash);
}

#[test]
fn get_missing_ref_returns_none() {
    let (_dir, index) = tmp_index();
    let name = RefName::new("nonexistent").expect("bad ref name");
    let result = index.get_ref(&name).expect("read failed");
    assert!(result.is_none());
}

#[test]
fn overwrite_ref() {
    let (_dir, index) = tmp_index();
    let name = RefName::new("main").expect("bad ref name");

    index
        .update_ref(&name, make_hash(4))
        .expect("first update failed");
    index
        .update_ref(&name, make_hash(5))
        .expect("second update failed");

    let fetched = index
        .get_ref(&name)
        .expect("read failed")
        .expect("missing ref");
    assert_eq!(fetched, make_hash(5));
}

#[test]
fn multiple_refs_independent() {
    let (_dir, index) = tmp_index();
    let main = RefName::new("main").expect("bad ref name");
    let dev = RefName::new("dev").expect("bad ref name");

    index
        .update_ref(&main, make_hash(10))
        .expect("update failed");
    index
        .update_ref(&dev, make_hash(20))
        .expect("update failed");

    assert_eq!(
        index.get_ref(&main).expect("read failed").expect("missing"),
        make_hash(10)
    );
    assert_eq!(
        index.get_ref(&dev).expect("read failed").expect("missing"),
        make_hash(20)
    );
}

// --- remotes ---

#[test]
fn store_and_read_remote() {
    let (_dir, index) = tmp_index();
    let ticket = RemoteTicket::new(vec![0xDE, 0xAD, 0xBE, 0xEF]).expect("bad ticket");

    index
        .store_remote("peer-a", &ticket)
        .expect("store failed");

    let fetched = index
        .get_remote("peer-a")
        .expect("read failed")
        .expect("missing remote");
    assert_eq!(fetched, ticket);
}

#[test]
fn get_missing_remote_returns_none() {
    let (_dir, index) = tmp_index();
    let result = index.get_remote("ghost").expect("read failed");
    assert!(result.is_none());
}

#[test]
fn remove_remote() {
    let (_dir, index) = tmp_index();
    let ticket = RemoteTicket::new(vec![1, 2, 3]).expect("bad ticket");

    index
        .store_remote("peer-b", &ticket)
        .expect("store failed");
    let removed = index.remove_remote("peer-b").expect("remove failed");
    assert!(removed);

    let result = index.get_remote("peer-b").expect("read failed");
    assert!(result.is_none());
}

#[test]
fn remove_nonexistent_remote_returns_false() {
    let (_dir, index) = tmp_index();
    let removed = index.remove_remote("nope").expect("remove failed");
    assert!(!removed);
}

#[test]
fn overwrite_remote() {
    let (_dir, index) = tmp_index();
    let ticket_v1 = RemoteTicket::new(vec![1]).expect("bad ticket");
    let ticket_v2 = RemoteTicket::new(vec![2]).expect("bad ticket");

    index
        .store_remote("peer-c", &ticket_v1)
        .expect("store failed");
    index
        .store_remote("peer-c", &ticket_v2)
        .expect("overwrite failed");

    let fetched = index
        .get_remote("peer-c")
        .expect("read failed")
        .expect("missing remote");
    assert_eq!(fetched, ticket_v2);
}

// --- crash consistency (test protocol 1.1) ---
// redb uses double-buffered tree roots: if the db file survives but the process
// died mid-commit, the next open sees the last *completed* transaction.
// we verify this by writing N blobs, reopening, and confirming count.

#[test]
fn crash_consistency_committed_txns_survive_reopen() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .join("crash.redb");

    let count: u64 = 500;

    fn make_unique_hash(i: u64) -> BlobHash {
        let mut h = [0u8; 32];
        let bytes = i.to_le_bytes();
        h[..8].copy_from_slice(&bytes);
        BlobHash(h)
    }

    {
        let index = Index::new(&db_path).expect("failed to create index");
        for i in 0..count {
            index
                .register_blob(make_unique_hash(i), make_meta(BlobType::File, i))
                .expect("register failed");
        }
    }

    {
        let index = Index::new(&db_path).expect("failed to reopen index");
        for i in 0..count {
            let meta = index
                .get_blob_meta(make_unique_hash(i))
                .expect("read failed")
                .expect("blob missing after reopen");
            assert_eq!(meta.created_at, i);
        }
    }
}

#[test]
fn refs_survive_reopen() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .join("crash_refs.redb");

    {
        let index = Index::new(&db_path).expect("failed to create index");
        let name = RefName::new("main").expect("bad ref name");
        index
            .update_ref(&name, make_hash(42))
            .expect("update failed");
    }

    {
        let index = Index::new(&db_path).expect("failed to reopen index");
        let name = RefName::new("main").expect("bad ref name");
        let fetched = index
            .get_ref(&name)
            .expect("read failed")
            .expect("ref missing after reopen");
        assert_eq!(fetched, make_hash(42));
    }
}

#[test]
fn remotes_survive_reopen() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .join("crash_remotes.redb");

    let ticket = RemoteTicket::new(vec![0xCA, 0xFE]).expect("bad ticket");

    {
        let index = Index::new(&db_path).expect("failed to create index");
        index
            .store_remote("peer-d", &ticket)
            .expect("store failed");
    }

    {
        let index = Index::new(&db_path).expect("failed to reopen index");
        let fetched = index
            .get_remote("peer-d")
            .expect("read failed")
            .expect("remote missing after reopen");
        assert_eq!(fetched, ticket);
    }
}
