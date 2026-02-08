use ark::domain::{BlobHash, BlobMetadata, BlobType};
use ark::storage::index::Index;
use camino::Utf8Path;
use redb::{Database, TableDefinition};

const BLOB_INDEX: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("blob_index");

fn make_unique_hash(i: u64) -> BlobHash {
    let mut h = [0u8; 32];
    h[..8].copy_from_slice(&i.to_le_bytes());
    BlobHash(h)
}

fn make_meta(ts: u64) -> BlobMetadata {
    BlobMetadata {
        blob_type: BlobType::File,
        created_at: ts,
        local_only: false,
    }
}

fn serialize_meta(meta: &BlobMetadata) -> Vec<u8> {
    rkyv::to_bytes::<rancor::Error>(meta)
        .expect("rkyv serialize failed")
        .to_vec()
}

// ---------------------------------------------------------------------------
// Test Protocol 1.1 — Crash Consistency Verification
// ---------------------------------------------------------------------------

#[test]
fn uncommitted_transaction_invisible_after_reopen() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .join("crash_uncommitted.redb");

    let committed_count: u64 = 100;
    let phantom_count: u64 = 100_000;

    {
        let index = Index::new(&db_path).expect("failed to create index");
        for i in 0..committed_count {
            index
                .register_blob(make_unique_hash(i), make_meta(i))
                .expect("register committed blob failed");
        }
    }

    {
        let db = Database::create(db_path.as_str()).expect("failed to open raw db");
        let write_txn = db.begin_write().expect("begin_write failed");
        {
            let mut table = write_txn.open_table(BLOB_INDEX).expect("open table failed");
            let meta_bytes = serialize_meta(&make_meta(0));
            for i in committed_count..(committed_count + phantom_count) {
                let hash = make_unique_hash(i);
                table
                    .insert(&hash.0, meta_bytes.as_slice())
                    .expect("insert phantom blob failed");
            }
        }
        drop(write_txn);
        drop(db);
    }

    {
        let index = Index::new(&db_path).expect("failed to reopen index");

        for i in 0..committed_count {
            let meta = index
                .get_blob_meta(make_unique_hash(i))
                .expect("read committed blob failed")
                .expect("committed blob missing after simulated crash");
            assert_eq!(meta.created_at, i);
        }

        for i in committed_count..(committed_count + phantom_count) {
            let result = index
                .get_blob_meta(make_unique_hash(i))
                .expect("read phantom blob failed");
            assert!(
                result.is_none(),
                "phantom blob {i} should not exist after uncommitted txn drop"
            );
        }

        let all = index.list_all_blobs().expect("list_all_blobs failed");
        assert_eq!(
            all.len(),
            committed_count as usize,
            "expected exactly {committed_count} blobs, got {}",
            all.len()
        );
    }
}

#[test]
fn committed_data_survives_simulated_crash_at_scale() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_path = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .join("crash_committed.redb");

    let count: u64 = 100_000;
    let batch_size: u64 = 10_000;

    {
        let _index = Index::new(&db_path).expect("failed to create index (init tables)");
    }

    {
        let db = Database::create(db_path.as_str()).expect("failed to open raw db for writes");
        for batch_start in (0..count).step_by(batch_size as usize) {
            let batch_end = (batch_start + batch_size).min(count);
            let write_txn = db.begin_write().expect("begin_write failed");
            {
                let mut table = write_txn.open_table(BLOB_INDEX).expect("open table failed");
                for i in batch_start..batch_end {
                    let hash = make_unique_hash(i);
                    let meta_bytes = serialize_meta(&make_meta(i));
                    table
                        .insert(&hash.0, meta_bytes.as_slice())
                        .expect("insert blob failed");
                }
            }
            write_txn.commit().expect("commit failed");
        }
    }

    {
        let index = Index::new(&db_path).expect("failed to reopen index");

        let all = index.list_all_blobs().expect("list_all_blobs failed");
        assert_eq!(
            all.len(),
            count as usize,
            "expected {count} blobs after reopen, got {}",
            all.len()
        );

        for i in 0..count {
            let meta = index
                .get_blob_meta(make_unique_hash(i))
                .expect("read failed")
                .expect("blob missing after reopen");
            assert_eq!(meta.created_at, i);
        }
    }
}
