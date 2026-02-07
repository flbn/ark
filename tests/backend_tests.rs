use std::sync::Arc;

use ark::backend::ids::commit_id_to_blob_hash;
use ark::backend::ArkBackend;
use ark::storage::BlobStore;
use camino::Utf8Path;
use jj_lib::backend::{Backend, Commit, ChangeId, SecureSig, Signature, Timestamp, MillisSinceEpoch, TreeValue, Tree, CopyId};
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::RepoPathComponentBuf;
use tokio::io::AsyncReadExt;

async fn tmp_backend() -> (tempfile::TempDir, ArkBackend) {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let root = Utf8Path::from_path(dir.path())
        .expect("non-utf8 temp path")
        .to_owned();
    let store = Arc::new(
        BlobStore::new(&root)
            .await
            .expect("failed to create store"),
    );
    let backend = ArkBackend::new(store)
        .await
        .expect("failed to create backend");
    (dir, backend)
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

// --- identity ---

#[tokio::test]
async fn backend_name() {
    let (_dir, backend) = tmp_backend().await;
    assert_eq!(backend.name(), "ark");
}

#[tokio::test]
async fn backend_id_lengths() {
    let (_dir, backend) = tmp_backend().await;
    assert_eq!(backend.commit_id_length(), 32);
    assert_eq!(backend.change_id_length(), 32);
}

#[tokio::test]
async fn root_ids_are_32_bytes() {
    let (_dir, backend) = tmp_backend().await;
    assert_eq!(backend.root_commit_id().as_bytes().len(), 32);
    assert_eq!(backend.root_change_id().as_bytes().len(), 32);
    assert_eq!(backend.empty_tree_id().as_bytes().len(), 32);
}

// --- file round-trip ---

#[tokio::test]
async fn write_and_read_file() {
    let (_dir, backend) = tmp_backend().await;
    let data = b"hello sovereign archive";
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let file_id = backend
        .write_file(&path, &mut &data[..])
        .await
        .expect("write_file failed");

    let mut reader = backend
        .read_file(&path, &file_id)
        .await
        .expect("read_file failed");
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read failed");
    assert_eq!(buf, data);
}

#[tokio::test]
async fn same_file_content_same_id() {
    let (_dir, backend) = tmp_backend().await;
    let data = b"deterministic";
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let id1 = backend
        .write_file(&path, &mut &data[..])
        .await
        .expect("write 1 failed");
    let id2 = backend
        .write_file(&path, &mut &data[..])
        .await
        .expect("write 2 failed");
    assert_eq!(id1, id2);
}

// --- symlink round-trip ---

#[tokio::test]
async fn write_and_read_symlink() {
    let (_dir, backend) = tmp_backend().await;
    let target = "/usr/bin/env";
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let symlink_id = backend
        .write_symlink(&path, target)
        .await
        .expect("write_symlink failed");

    let read_target = backend
        .read_symlink(&path, &symlink_id)
        .await
        .expect("read_symlink failed");
    assert_eq!(read_target, target);
}

// --- tree round-trip ---

#[tokio::test]
async fn write_and_read_empty_tree() {
    let (_dir, backend) = tmp_backend().await;
    let tree = Tree::from_sorted_entries(vec![]);
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree_id = backend
        .write_tree(&path, &tree)
        .await
        .expect("write_tree failed");

    let read_tree = backend
        .read_tree(&path, &tree_id)
        .await
        .expect("read_tree failed");
    assert!(read_tree.is_empty());
}

#[tokio::test]
async fn empty_tree_determinism() {
    let (_dir, backend) = tmp_backend().await;
    let tree = Tree::from_sorted_entries(vec![]);
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let id1 = backend.write_tree(&path, &tree).await.expect("write 1");
    let id2 = backend.write_tree(&path, &tree).await.expect("write 2");
    assert_eq!(id1, id2);
}

#[tokio::test]
async fn write_and_read_tree_with_entries() {
    let (_dir, backend) = tmp_backend().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let file_data = b"file content";
    let file_id = backend
        .write_file(&path, &mut &file_data[..])
        .await
        .expect("write_file failed");

    let symlink_target = "target_path";
    let symlink_id = backend
        .write_symlink(&path, symlink_target)
        .await
        .expect("write_symlink failed");

    let entries = vec![
        (
            RepoPathComponentBuf::new("a_file").expect("valid name"),
            TreeValue::File {
                id: file_id,
                executable: false,
                copy_id: CopyId::placeholder(),
            },
        ),
        (
            RepoPathComponentBuf::new("b_link").expect("valid name"),
            TreeValue::Symlink(symlink_id),
        ),
    ];
    let tree = Tree::from_sorted_entries(entries);

    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");
    let read_tree = backend
        .read_tree(&path, &tree_id)
        .await
        .expect("read_tree");

    let read_entries: Vec<_> = read_tree.entries().collect();
    assert_eq!(read_entries.len(), 2);
    assert_eq!(read_entries[0].name().as_internal_str(), "a_file");
    assert_eq!(read_entries[1].name().as_internal_str(), "b_link");
}

// --- commit round-trip ---

#[tokio::test]
async fn write_and_read_commit() {
    let (_dir, backend) = tmp_backend().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    let commit = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![0x42; 32]),
        description: "test commit".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };

    let (commit_id, _written) = backend
        .write_commit(commit, None)
        .await
        .expect("write_commit failed");

    let read_commit = backend
        .read_commit(&commit_id)
        .await
        .expect("read_commit failed");

    assert_eq!(read_commit.description, "test commit");
    assert_eq!(read_commit.change_id, ChangeId::new(vec![0x42; 32]));
    assert_eq!(read_commit.parents.len(), 1);
    assert_eq!(read_commit.parents[0], *backend.root_commit_id());
    assert_eq!(read_commit.author.name, "test");
    assert_eq!(read_commit.author.email, "test@example.com");
}

#[tokio::test]
async fn root_commit_readable() {
    let (_dir, backend) = tmp_backend().await;
    let root = backend
        .read_commit(backend.root_commit_id())
        .await
        .expect("read root commit failed");
    assert!(root.parents.is_empty());
    assert!(root.description.is_empty());
}

#[tokio::test]
async fn empty_tree_readable() {
    let (_dir, backend) = tmp_backend().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();
    let tree = backend
        .read_tree(&path, backend.empty_tree_id())
        .await
        .expect("read empty tree failed");
    assert!(tree.is_empty());
}

// --- copy tracking returns unsupported ---

#[tokio::test]
async fn copy_tracking_unsupported() {
    let (_dir, backend) = tmp_backend().await;
    let copy_id = CopyId::new(vec![1, 2, 3]);
    assert!(backend.read_copy(&copy_id).await.is_err());
    assert!(backend
        .write_copy(&jj_lib::backend::CopyHistory {
            current_path: jj_lib::repo_path::RepoPathBuf::root(),
            parents: vec![],
            salt: vec![],
        })
        .await
        .is_err());
    assert!(backend.get_related_copies(&copy_id).await.is_err());
}

// --- id mapping: content-addressed bijection ---

#[tokio::test]
async fn commit_id_is_content_addressed() {
    let (_dir, backend) = tmp_backend().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    let commit_a = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id.clone()),
        change_id: ChangeId::new(vec![0xAA; 32]),
        description: "commit A".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };
    let commit_b = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![0xBB; 32]),
        description: "commit B".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };

    let (id_a, _) = backend.write_commit(commit_a, None).await.expect("write a");
    let (id_b, _) = backend.write_commit(commit_b, None).await.expect("write b");

    assert_ne!(id_a, id_b);
}

// --- get_copy_records returns empty stream ---

#[tokio::test]
async fn get_copy_records_empty() {
    use futures::StreamExt;
    let (_dir, backend) = tmp_backend().await;
    let root_id = backend.root_commit_id().clone();
    let stream = backend
        .get_copy_records(None, &root_id, &root_id)
        .expect("get_copy_records failed");
    let records: Vec<_> = stream.collect().await;
    assert!(records.is_empty());
}

// --- working copy snapshot signal ---

#[tokio::test]
async fn mark_next_commit_local_only_marks_commit() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    let commit = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![0xA1; 32]),
        description: "snapshot".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };
    backend.mark_next_commit_local_only();
    let (commit_id, _) = backend
        .write_commit(commit, None)
        .await
        .expect("write_commit");

    let hash = commit_id_to_blob_hash(&commit_id);
    let meta = store
        .index
        .get_blob_meta(hash)
        .expect("read meta failed")
        .expect("missing metadata");
    assert!(meta.local_only);
}

#[tokio::test]
async fn default_commit_is_not_local_only() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    let commit = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![0xB1; 32]),
        description: "normal".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };
    let (commit_id, _) = backend
        .write_commit(commit, None)
        .await
        .expect("write_commit");

    let hash = commit_id_to_blob_hash(&commit_id);
    let meta = store
        .index
        .get_blob_meta(hash)
        .expect("read meta failed")
        .expect("missing metadata");
    assert!(!meta.local_only);
}

#[tokio::test]
async fn local_only_flag_is_one_shot() {
    let (_dir, store, backend) = tmp_backend_with_store().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    let commit1 = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id.clone()),
        change_id: ChangeId::new(vec![0xC1; 32]),
        description: "first".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };
    backend.mark_next_commit_local_only();
    let (id1, _) = backend
        .write_commit(commit1, None)
        .await
        .expect("write first commit");
    let meta1 = store
        .index
        .get_blob_meta(commit_id_to_blob_hash(&id1))
        .expect("read meta failed")
        .expect("missing metadata");
    assert!(meta1.local_only);

    let commit2 = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![0xC2; 32]),
        description: "second".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };
    let (id2, _) = backend
        .write_commit(commit2, None)
        .await
        .expect("write second commit");
    let meta2 = store
        .index
        .get_blob_meta(commit_id_to_blob_hash(&id2))
        .expect("read meta failed")
        .expect("missing metadata");
    assert!(!meta2.local_only);
}

// --- signature round-trip ---

#[tokio::test]
async fn commit_with_secure_sig_round_trips() {
    let (_dir, backend) = tmp_backend().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    let commit = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![0xD1; 32]),
        description: "signed commit".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: Some(SecureSig {
            data: vec![1, 2, 3],
            sig: vec![4, 5, 6],
        }),
    };

    let (commit_id, _) = backend
        .write_commit(commit, None)
        .await
        .expect("write_commit");

    let read_commit = backend
        .read_commit(&commit_id)
        .await
        .expect("read_commit");
    let sig = read_commit
        .secure_sig
        .expect("expected secure_sig to be Some");
    assert_eq!(sig.data, vec![1, 2, 3]);
    assert_eq!(sig.sig, vec![4, 5, 6]);
}

#[tokio::test]
async fn commit_without_secure_sig_round_trips() {
    let (_dir, backend) = tmp_backend().await;
    let path = jj_lib::repo_path::RepoPathBuf::root();

    let tree = Tree::from_sorted_entries(vec![]);
    let tree_id = backend.write_tree(&path, &tree).await.expect("write_tree");

    let commit = Commit {
        parents: vec![backend.root_commit_id().clone()],
        predecessors: vec![],
        root_tree: Merge::resolved(tree_id),
        change_id: ChangeId::new(vec![0xE1; 32]),
        description: "unsigned".to_string(),
        author: make_signature(),
        committer: make_signature(),
        secure_sig: None,
    };
    let (commit_id, _) = backend
        .write_commit(commit, None)
        .await
        .expect("write_commit");

    let read_commit = backend
        .read_commit(&commit_id)
        .await
        .expect("read_commit");
    assert!(read_commit.secure_sig.is_none());
}
