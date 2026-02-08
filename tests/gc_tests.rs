use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ark::backend::ids::{commit_id_to_blob_hash, file_id_to_blob_hash, tree_id_to_blob_hash};
use ark::backend::ArkBackend;
use ark::storage::BlobStore;
use camino::Utf8Path;
use jj_lib::backend::{
    Backend, ChangeId, Commit, CopyId, MillisSinceEpoch, Signature, Timestamp, Tree, TreeValue,
};
use jj_lib::index::{Index as JjIndex, IndexError as JjIndexError, IndexResult as JjIndexResult};
use jj_lib::merge::Merge;
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::repo_path::RepoPathComponentBuf;

struct StubIndex {
    reachable: HashSet<Vec<u8>>,
}

impl StubIndex {
    fn new(ids: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            reachable: ids.into_iter().collect(),
        }
    }
}

impl JjIndex for StubIndex {
    fn shortest_unique_commit_id_prefix_len(
        &self,
        _commit_id: &jj_lib::backend::CommitId,
    ) -> JjIndexResult<usize> {
        Err(JjIndexError::Other(Box::from("stub")))
    }

    fn resolve_commit_id_prefix(
        &self,
        _prefix: &HexPrefix,
    ) -> JjIndexResult<PrefixResolution<jj_lib::backend::CommitId>> {
        Err(JjIndexError::Other(Box::from("stub")))
    }

    fn has_id(&self, commit_id: &jj_lib::backend::CommitId) -> JjIndexResult<bool> {
        Ok(self.reachable.contains(commit_id.as_bytes()))
    }

    fn is_ancestor(
        &self,
        _ancestor_id: &jj_lib::backend::CommitId,
        _descendant_id: &jj_lib::backend::CommitId,
    ) -> JjIndexResult<bool> {
        Err(JjIndexError::Other(Box::from("stub")))
    }

    fn common_ancestors(
        &self,
        _set1: &[jj_lib::backend::CommitId],
        _set2: &[jj_lib::backend::CommitId],
    ) -> JjIndexResult<Vec<jj_lib::backend::CommitId>> {
        Err(JjIndexError::Other(Box::from("stub")))
    }

    fn all_heads_for_gc(
        &self,
    ) -> JjIndexResult<Box<dyn Iterator<Item = jj_lib::backend::CommitId> + '_>> {
        Err(JjIndexError::AllHeadsForGcUnsupported)
    }

    fn heads(
        &self,
        _candidates: &mut dyn Iterator<Item = &jj_lib::backend::CommitId>,
    ) -> JjIndexResult<Vec<jj_lib::backend::CommitId>> {
        Err(JjIndexError::Other(Box::from("stub")))
    }

    fn changed_paths_in_commit(
        &self,
        _commit_id: &jj_lib::backend::CommitId,
    ) -> JjIndexResult<Option<Box<dyn Iterator<Item = jj_lib::repo_path::RepoPathBuf> + '_>>> {
        Ok(None)
    }

    fn evaluate_revset(
        &self,
        _expression: &jj_lib::revset::ResolvedExpression,
        _store: &Arc<jj_lib::store::Store>,
    ) -> Result<Box<dyn jj_lib::revset::Revset + '_>, jj_lib::revset::RevsetEvaluationError> {
        Err(jj_lib::revset::RevsetEvaluationError::Other(Box::from(
            "stub",
        )))
    }
}

async fn tmp_backend_with_store() -> (tempfile::TempDir, Arc<BlobStore>, ArkBackend) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();
    let store = Arc::new(
        BlobStore::new(&root)
            .await
            .expect("failed to create store"),
    );
    let backend = ArkBackend::new(store.clone())
        .await
        .expect("failed to create backend");
    (dir, store, backend)
}

fn make_signature() -> Signature {
    Signature {
        name: "test".to_string(),
        email: "test@example.com".to_string(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(1700000000000),
            tz_offset: 0,
        },
    }
}

fn make_commit(backend: &ArkBackend, tree_id: jj_lib::backend::TreeId, change_byte: u8) -> Commit {
    Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![change_byte; 32]),
        description: format!("commit-{change_byte:#x}"),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_removes_unreachable_local_only_commit() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    backend.mark_next_commit_local_only();
    let (commit_id, _) = backend
        .write_commit(make_commit(&backend, tree_id, 0xA1), None)
        .await
        .expect("write_commit");

    let stub = StubIndex::new(std::iter::empty::<Vec<u8>>());
    let future = SystemTime::now() + Duration::from_secs(3600);
    backend.gc(&stub, future).expect("gc failed");

    let hash = commit_id_to_blob_hash(&commit_id);
    let meta = store.index.get_blob_meta(hash).expect("read meta failed");
    assert!(meta.is_none(), "unreachable local-only commit should be removed");
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_preserves_reachable_commit_and_children() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let file_data = b"reachable file content";
    let file_id = backend
        .write_file(&path, &mut &file_data[..])
        .await
        .expect("write_file");
    let file_hash = file_id_to_blob_hash(&file_id);
    store
        .index
        .set_blob_local_only(file_hash, true)
        .expect("set local_only");

    let entries = vec![(
        RepoPathComponentBuf::new("kept.txt").expect("valid name"),
        TreeValue::File {
            id: file_id,
            executable: false,
            copy_id: CopyId::placeholder(),
        },
    )];
    let tree = Tree::from_sorted_entries(entries);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");
    let tree_hash = tree_id_to_blob_hash(&tree_id);
    store
        .index
        .set_blob_local_only(tree_hash, true)
        .expect("set local_only");

    backend.mark_next_commit_local_only();
    let (commit_id, _) = backend
        .write_commit(make_commit(&backend, tree_id, 0xB1), None)
        .await
        .expect("write_commit");

    let stub = StubIndex::new(vec![commit_id.as_bytes().to_vec()]);
    backend
        .gc(&stub, SystemTime::UNIX_EPOCH)
        .expect("gc failed");

    let commit_hash = commit_id_to_blob_hash(&commit_id);
    assert!(
        store.index.get_blob_meta(commit_hash).expect("meta").is_some(),
        "reachable commit should survive gc"
    );
    assert!(
        store.index.get_blob_meta(tree_hash).expect("meta").is_some(),
        "tree of reachable commit should survive gc"
    );
    assert!(
        store.index.get_blob_meta(file_hash).expect("meta").is_some(),
        "file of reachable commit should survive gc"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_removes_unreachable_tree_and_file_blobs() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let file_data = b"orphan file content";
    let file_id = backend
        .write_file(&path, &mut &file_data[..])
        .await
        .expect("write_file");
    let file_hash = file_id_to_blob_hash(&file_id);
    store
        .index
        .set_blob_local_only(file_hash, true)
        .expect("set local_only");

    let entries = vec![(
        RepoPathComponentBuf::new("orphan.txt").expect("valid name"),
        TreeValue::File {
            id: file_id,
            executable: false,
            copy_id: CopyId::placeholder(),
        },
    )];
    let tree = Tree::from_sorted_entries(entries);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");
    let tree_hash = tree_id_to_blob_hash(&tree_id);
    store
        .index
        .set_blob_local_only(tree_hash, true)
        .expect("set local_only");

    backend.mark_next_commit_local_only();
    let (commit_id, _) = backend
        .write_commit(make_commit(&backend, tree_id, 0xC1), None)
        .await
        .expect("write_commit");
    let commit_hash = commit_id_to_blob_hash(&commit_id);

    let stub = StubIndex::new(std::iter::empty::<Vec<u8>>());
    let future = SystemTime::now() + Duration::from_secs(3600);
    backend.gc(&stub, future).expect("gc failed");

    assert!(
        store.index.get_blob_meta(commit_hash).expect("meta").is_none(),
        "unreachable commit should be removed"
    );
    assert!(
        store.index.get_blob_meta(tree_hash).expect("meta").is_none(),
        "tree of unreachable commit should be removed"
    );
    assert!(
        store.index.get_blob_meta(file_hash).expect("meta").is_none(),
        "file of unreachable commit should be removed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_preserves_recent_blobs() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    backend.mark_next_commit_local_only();
    let (commit_id, _) = backend
        .write_commit(make_commit(&backend, tree_id, 0xD1), None)
        .await
        .expect("write_commit");

    let stub = StubIndex::new(std::iter::empty::<Vec<u8>>());
    backend
        .gc(&stub, SystemTime::UNIX_EPOCH)
        .expect("gc failed");

    let hash = commit_id_to_blob_hash(&commit_id);
    assert!(
        store.index.get_blob_meta(hash).expect("meta").is_some(),
        "recent blob should be preserved even if unreachable"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn gc_mixed_reachable_and_unreachable() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let kept_file_id = backend
        .write_file(&path, &mut &b"kept"[..])
        .await
        .expect("write_file");
    let kept_file_hash = file_id_to_blob_hash(&kept_file_id);
    store
        .index
        .set_blob_local_only(kept_file_hash, true)
        .expect("set local_only");

    let kept_tree = Tree::from_sorted_entries(vec![(
        RepoPathComponentBuf::new("kept.txt").expect("valid name"),
        TreeValue::File {
            id: kept_file_id,
            executable: false,
            copy_id: CopyId::placeholder(),
        },
    )]);
    let kept_tree_id = backend
        .write_tree(&path, &kept_tree)
        .await
        .expect("write_tree");
    let kept_tree_hash = tree_id_to_blob_hash(&kept_tree_id);
    store
        .index
        .set_blob_local_only(kept_tree_hash, true)
        .expect("set local_only");

    backend.mark_next_commit_local_only();
    let (kept_commit_id, _) = backend
        .write_commit(make_commit(&backend, kept_tree_id, 0xE1), None)
        .await
        .expect("write_commit");
    let kept_commit_hash = commit_id_to_blob_hash(&kept_commit_id);

    let dead_file_id = backend
        .write_file(&path, &mut &b"dead"[..])
        .await
        .expect("write_file");
    let dead_file_hash = file_id_to_blob_hash(&dead_file_id);
    store
        .index
        .set_blob_local_only(dead_file_hash, true)
        .expect("set local_only");

    let dead_tree = Tree::from_sorted_entries(vec![(
        RepoPathComponentBuf::new("dead.txt").expect("valid name"),
        TreeValue::File {
            id: dead_file_id,
            executable: false,
            copy_id: CopyId::placeholder(),
        },
    )]);
    let dead_tree_id = backend
        .write_tree(&path, &dead_tree)
        .await
        .expect("write_tree");
    let dead_tree_hash = tree_id_to_blob_hash(&dead_tree_id);
    store
        .index
        .set_blob_local_only(dead_tree_hash, true)
        .expect("set local_only");

    backend.mark_next_commit_local_only();
    let (dead_commit_id, _) = backend
        .write_commit(make_commit(&backend, dead_tree_id, 0xE2), None)
        .await
        .expect("write_commit");
    let dead_commit_hash = commit_id_to_blob_hash(&dead_commit_id);

    let stub = StubIndex::new(vec![kept_commit_id.as_bytes().to_vec()]);
    let future = SystemTime::now() + Duration::from_secs(3600);
    backend.gc(&stub, future).expect("gc failed");

    assert!(
        store.index.get_blob_meta(kept_commit_hash).expect("meta").is_some(),
        "reachable commit should survive"
    );
    assert!(
        store.index.get_blob_meta(kept_tree_hash).expect("meta").is_some(),
        "reachable tree should survive"
    );
    assert!(
        store.index.get_blob_meta(kept_file_hash).expect("meta").is_some(),
        "reachable file should survive"
    );

    assert!(
        store.index.get_blob_meta(dead_commit_hash).expect("meta").is_none(),
        "unreachable commit should be removed"
    );
    assert!(
        store.index.get_blob_meta(dead_tree_hash).expect("meta").is_none(),
        "unreachable tree should be removed"
    );
    assert!(
        store.index.get_blob_meta(dead_file_hash).expect("meta").is_none(),
        "unreachable file should be removed"
    );
}
