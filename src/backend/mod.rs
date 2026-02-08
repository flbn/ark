pub mod ids;
pub mod objects;

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt};

use jj_lib::backend::{
    Backend, BackendError, BackendResult, ChangeId, Commit, CommitId, CopyHistory, CopyId,
    CopyRecord, FileId, SecureSig, Signature as JjSignature, MillisSinceEpoch,
    SigningFn, SymlinkId, Timestamp, Tree, TreeId, TreeValue,
};
use jj_lib::index::Index;
use jj_lib::merge::Merge;
use jj_lib::object_id::ObjectId;
use jj_lib::repo_path::{RepoPath, RepoPathBuf, RepoPathComponentBuf};

use crate::domain::{BlobMetadata, BlobType};
use crate::storage::BlobStore;

use ids::{
    blob_hash_to_commit_id, blob_hash_to_file_id,
    blob_hash_to_symlink_id, blob_hash_to_tree_id, bytes_to_copy_id,
    commit_id_to_blob_hash, file_id_to_blob_hash, symlink_id_to_blob_hash, tree_id_to_blob_hash,
};
use objects::{
    StoredCommit, StoredEntryKind, StoredSecureSig, StoredSignature, StoredTree, StoredTreeEntry,
};

pub struct ArkBackend {
    store: Arc<BlobStore>,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
    // @todo(o11y): one-shot flag assumes concurrency() == 1. if backend concurrency
    //   increases, replace with a scoped context or task-local storage.
    next_commit_local_only: Mutex<bool>,
}

impl Debug for ArkBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArkBackend")
            .field("root_commit_id", &self.root_commit_id.hex())
            .field("empty_tree_id", &self.empty_tree_id.hex())
            .finish()
    }
}

impl ArkBackend {
    pub const BACKEND_NAME: &str = "ark";

    pub async fn new(store: Arc<BlobStore>) -> Result<Self, BackendError> {
        let empty_tree = StoredTree {
            entries: Vec::new(),
        };
        let empty_tree_bytes = rkyv::to_bytes::<rancor::Error>(&empty_tree)
            .map_err(|e| BackendError::Other(e.into()))?;
        let empty_tree_hash = store
            .put_object(
                &empty_tree_bytes,
                BlobMetadata {
                    blob_type: BlobType::Tree,
                    created_at: 0,
                    local_only: false,
                },
            )
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        let empty_tree_id = blob_hash_to_tree_id(empty_tree_hash);

        let root_change_id = ChangeId::new(vec![0u8; 32]);

        let root_commit = jj_lib::backend::make_root_commit(
            root_change_id.clone(),
            empty_tree_id.clone(),
        );
        let root_stored = jj_commit_to_stored(&root_commit);
        let root_bytes = rkyv::to_bytes::<rancor::Error>(&root_stored)
            .map_err(|e| BackendError::Other(e.into()))?;
        let root_hash = store
            .put_object(
                &root_bytes,
                BlobMetadata {
                    blob_type: BlobType::Commit,
                    created_at: 0,
                    local_only: false,
                },
            )
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        let root_commit_id = blob_hash_to_commit_id(root_hash);

        Ok(Self {
            store,
            root_commit_id,
            root_change_id,
            empty_tree_id,
            next_commit_local_only: Mutex::new(false),
        })
    }

    pub fn store(&self) -> &Arc<BlobStore> {
        &self.store
    }

    pub fn mark_next_commit_local_only(&self) {
        *self.next_commit_local_only.lock() = true;
    }
}

fn id_bytes_to_hash(bytes: &[u8]) -> [u8; 32] {
    let mut h = [0u8; 32];
    let len = bytes.len().min(32);
    h[..len].copy_from_slice(&bytes[..len]);
    h
}

fn jj_commit_to_stored(c: &Commit) -> StoredCommit {
    let root_tree_ids: Vec<[u8; 32]> = c
        .root_tree
        .iter()
        .map(|id| id_bytes_to_hash(id.as_bytes()))
        .collect();

    StoredCommit {
        parents: c
            .parents
            .iter()
            .map(|id| id_bytes_to_hash(id.as_bytes()))
            .collect(),
        predecessors: c
            .predecessors
            .iter()
            .map(|id| id_bytes_to_hash(id.as_bytes()))
            .collect(),
        root_tree_ids,
        change_id: id_bytes_to_hash(c.change_id.as_bytes()),
        description: c.description.clone(),
        author: jj_sig_to_stored(&c.author),
        committer: jj_sig_to_stored(&c.committer),
        secure_sig: c.secure_sig.as_ref().map(|s| StoredSecureSig {
            data: s.data.clone(),
            sig: s.sig.clone(),
        }),
    }
}

fn jj_sig_to_stored(s: &JjSignature) -> StoredSignature {
    StoredSignature {
        name: s.name.clone(),
        email: s.email.clone(),
        millis_since_epoch: s.timestamp.timestamp.0,
        tz_offset_minutes: s.timestamp.tz_offset,
    }
}

fn stored_to_jj_sig(s: &StoredSignature) -> JjSignature {
    JjSignature {
        name: s.name.clone(),
        email: s.email.clone(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(s.millis_since_epoch),
            tz_offset: s.tz_offset_minutes,
        },
    }
}

fn stored_to_jj_commit(s: &StoredCommit) -> Commit {
    let root_tree = Merge::from_vec(
        s.root_tree_ids
            .iter()
            .map(|h| TreeId::new(h.to_vec()))
            .collect::<Vec<_>>(),
    );

    Commit {
        parents: s.parents.iter().map(|h| CommitId::new(h.to_vec())).collect(),
        predecessors: s
            .predecessors
            .iter()
            .map(|h| CommitId::new(h.to_vec()))
            .collect(),
        root_tree,
        change_id: ChangeId::new(s.change_id.to_vec()),
        description: s.description.clone(),
        author: stored_to_jj_sig(&s.author),
        committer: stored_to_jj_sig(&s.committer),
        secure_sig: s.secure_sig.as_ref().map(|ss| SecureSig {
            data: ss.data.clone(),
            sig: ss.sig.clone(),
        }),
    }
}

fn jj_tree_to_stored(tree: &Tree) -> Result<StoredTree, BackendError> {
    let mut entries = Vec::new();
    for entry in tree.entries() {
        let name = entry.name().as_internal_str().to_string();
        let (kind, id) = match entry.value() {
            TreeValue::File {
                id,
                executable,
                copy_id,
            } => (
                StoredEntryKind::File {
                    executable: *executable,
                    copy_id: copy_id.as_bytes().to_vec(),
                },
                id_bytes_to_hash(id.as_bytes()),
            ),
            TreeValue::Symlink(id) => (StoredEntryKind::Symlink, id_bytes_to_hash(id.as_bytes())),
            TreeValue::Tree(id) => (StoredEntryKind::Tree, id_bytes_to_hash(id.as_bytes())),
            TreeValue::GitSubmodule(id) => {
                (StoredEntryKind::GitSubmodule, id_bytes_to_hash(id.as_bytes()))
            }
        };
        entries.push(StoredTreeEntry { name, kind, id });
    }
    Ok(StoredTree { entries })
}

fn stored_to_jj_tree(s: &StoredTree) -> Result<Tree, BackendError> {
    let mut entries: Vec<(RepoPathComponentBuf, TreeValue)> = Vec::new();
    for entry in &s.entries {
        let name = RepoPathComponentBuf::new(&entry.name)
            .map_err(|e| BackendError::Other(e.into()))?;
        let value = match &entry.kind {
            StoredEntryKind::File {
                executable,
                copy_id,
            } => TreeValue::File {
                id: FileId::new(entry.id.to_vec()),
                executable: *executable,
                copy_id: bytes_to_copy_id(copy_id),
            },
            StoredEntryKind::Symlink => TreeValue::Symlink(SymlinkId::new(entry.id.to_vec())),
            StoredEntryKind::Tree => TreeValue::Tree(TreeId::new(entry.id.to_vec())),
            StoredEntryKind::GitSubmodule => {
                TreeValue::GitSubmodule(CommitId::new(entry.id.to_vec()))
            }
        };
        entries.push((name, value));
    }
    Ok(Tree::from_sorted_entries(entries))
}

fn timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[async_trait]
impl Backend for ArkBackend {
    fn name(&self) -> &str {
        Self::BACKEND_NAME
    }

    fn commit_id_length(&self) -> usize {
        32
    }

    fn change_id_length(&self) -> usize {
        32
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        _path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let hash = file_id_to_blob_hash(id);
        let bytes =
            self.store
                .blobs
                .get_bytes(hash)
                .await
                .map_err(|e| BackendError::ObjectNotFound {
                    object_type: "file".to_string(),
                    hash: id.hex(),
                    source: e.into(),
                })?;
        Ok(Box::pin(std::io::Cursor::new(bytes)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let mut buf = Vec::new();
        contents
            .read_to_end(&mut buf)
            .await
            .map_err(|e| BackendError::WriteObject {
                object_type: "file",
                source: e.into(),
            })?;
        let hash = self
            .store
            .put_object(
                &buf,
                BlobMetadata {
                    blob_type: BlobType::File,
                    created_at: timestamp_now(),
                    local_only: false,
                },
            )
            .await
            .map_err(|e| BackendError::WriteObject {
                object_type: "file",
                source: e.into(),
            })?;
        Ok(blob_hash_to_file_id(hash))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let hash = symlink_id_to_blob_hash(id);
        let bytes =
            self.store
                .blobs
                .get_bytes(hash)
                .await
                .map_err(|e| BackendError::ObjectNotFound {
                    object_type: "symlink".to_string(),
                    hash: id.hex(),
                    source: e.into(),
                })?;
        String::from_utf8(bytes).map_err(|e| BackendError::InvalidUtf8 {
            object_type: "symlink".to_string(),
            hash: id.hex(),
            source: e.utf8_error(),
        })
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let hash = self
            .store
            .put_object(
                target.as_bytes(),
                BlobMetadata {
                    blob_type: BlobType::Symlink,
                    created_at: timestamp_now(),
                    local_only: false,
                },
            )
            .await
            .map_err(|e| BackendError::WriteObject {
                object_type: "symlink",
                source: e.into(),
            })?;
        Ok(blob_hash_to_symlink_id(hash))
    }

    async fn read_copy(&self, _id: &CopyId) -> BackendResult<CopyHistory> {
        Err(BackendError::Unsupported(
            "copy tracking not supported".to_string(),
        ))
    }

    async fn write_copy(&self, _copy: &CopyHistory) -> BackendResult<CopyId> {
        Err(BackendError::Unsupported(
            "copy tracking not supported".to_string(),
        ))
    }

    async fn get_related_copies(&self, _copy_id: &CopyId) -> BackendResult<Vec<CopyHistory>> {
        Err(BackendError::Unsupported(
            "copy tracking not supported".to_string(),
        ))
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        let hash = tree_id_to_blob_hash(id);
        let bytes =
            self.store
                .blobs
                .get_bytes(hash)
                .await
                .map_err(|e| BackendError::ObjectNotFound {
                    object_type: "tree".to_string(),
                    hash: id.hex(),
                    source: e.into(),
                })?;
        let stored = rkyv::from_bytes::<StoredTree, rancor::Error>(&bytes).map_err(|e| {
            BackendError::ReadObject {
                object_type: "tree".to_string(),
                hash: id.hex(),
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
            }
        })?;
        stored_to_jj_tree(&stored)
    }

    async fn write_tree(&self, _path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let stored = jj_tree_to_stored(contents)?;
        let bytes = rkyv::to_bytes::<rancor::Error>(&stored).map_err(|e| {
            BackendError::WriteObject {
                object_type: "tree",
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
            }
        })?;
        let hash = self
            .store
            .put_object(
                &bytes,
                BlobMetadata {
                    blob_type: BlobType::Tree,
                    created_at: timestamp_now(),
                    local_only: false,
                },
            )
            .await
            .map_err(|e| BackendError::WriteObject {
                object_type: "tree",
                source: e.into(),
            })?;
        Ok(blob_hash_to_tree_id(hash))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        let hash = commit_id_to_blob_hash(id);
        let bytes =
            self.store
                .blobs
                .get_bytes(hash)
                .await
                .map_err(|e| BackendError::ObjectNotFound {
                    object_type: "commit".to_string(),
                    hash: id.hex(),
                    source: e.into(),
                })?;
        let stored =
            rkyv::from_bytes::<StoredCommit, rancor::Error>(&bytes).map_err(|e| {
                BackendError::ReadObject {
                    object_type: "commit".to_string(),
                    hash: id.hex(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    )),
                }
            })?;
        Ok(stored_to_jj_commit(&stored))
    }

    async fn write_commit(
        &self,
        contents: Commit,
        _sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let local_only = {
            let mut flag = self.next_commit_local_only.lock();
            let v = *flag;
            *flag = false;
            v
        };

        let stored = jj_commit_to_stored(&contents);
        let bytes = rkyv::to_bytes::<rancor::Error>(&stored).map_err(|e| {
            BackendError::WriteObject {
                object_type: "commit",
                source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
            }
        })?;
        let hash = self
            .store
            .put_object(
                &bytes,
                BlobMetadata {
                    blob_type: BlobType::Commit,
                    created_at: timestamp_now(),
                    local_only,
                },
            )
            .await
            .map_err(|e| BackendError::WriteObject {
                object_type: "commit",
                source: e.into(),
            })?;
        let id = blob_hash_to_commit_id(hash);
        Ok((id, contents))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(Box::pin(futures::stream::empty()))
    }

    // @todo(o11y): gc only considers commit blobs — tree/file/symlink blobs referenced
    //   by unreachable commits are not reclaimed. full object-graph GC would require
    //   traversing from reachable commits down to trees and files. acceptable for Phase II.
    fn gc(&self, index: &dyn Index, keep_newer: SystemTime) -> BackendResult<()> {
        let keep_newer_secs = keep_newer
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let locals = self
            .store
            .index
            .list_local_only_blobs()
            .map_err(|e| BackendError::Other(e.into()))?;

        for (hash, meta) in locals {
            if !matches!(meta.blob_type, BlobType::Commit) {
                continue;
            }
            if meta.created_at >= keep_newer_secs {
                continue;
            }

            let commit_id = blob_hash_to_commit_id(hash);
            let reachable = index
                .has_id(&commit_id)
                .map_err(|e| BackendError::Other(e.into()))?;
            if reachable {
                continue;
            }

            self.store
                .delete_object(hash)
                .map_err(|e| BackendError::Other(e.into()))?;
        }

        Ok(())
    }
}
