use crate::domain::BlobHash;
use jj_lib::backend::{
    ChangeId, CommitId, CopyId, FileId, SymlinkId, TreeId,
};
use jj_lib::object_id::ObjectId;

pub fn blob_hash_to_commit_id(h: BlobHash) -> CommitId {
    CommitId::new(h.0.to_vec())
}

pub fn commit_id_to_blob_hash(id: &CommitId) -> BlobHash {
    let mut h = [0u8; 32];
    h.copy_from_slice(id.as_bytes());
    BlobHash(h)
}

pub fn blob_hash_to_tree_id(h: BlobHash) -> TreeId {
    TreeId::new(h.0.to_vec())
}

pub fn tree_id_to_blob_hash(id: &TreeId) -> BlobHash {
    let mut h = [0u8; 32];
    h.copy_from_slice(id.as_bytes());
    BlobHash(h)
}

pub fn blob_hash_to_file_id(h: BlobHash) -> FileId {
    FileId::new(h.0.to_vec())
}

pub fn file_id_to_blob_hash(id: &FileId) -> BlobHash {
    let mut h = [0u8; 32];
    h.copy_from_slice(id.as_bytes());
    BlobHash(h)
}

pub fn blob_hash_to_symlink_id(h: BlobHash) -> SymlinkId {
    SymlinkId::new(h.0.to_vec())
}

pub fn symlink_id_to_blob_hash(id: &SymlinkId) -> BlobHash {
    let mut h = [0u8; 32];
    h.copy_from_slice(id.as_bytes());
    BlobHash(h)
}

pub fn blob_hash_to_change_id(h: BlobHash) -> ChangeId {
    ChangeId::new(h.0.to_vec())
}

pub fn bytes_to_blob_hash(bytes: &[u8; 32]) -> BlobHash {
    BlobHash(*bytes)
}

#[allow(dead_code)]
pub fn copy_id_to_bytes(id: &CopyId) -> Vec<u8> {
    id.as_bytes().to_vec()
}

pub fn bytes_to_copy_id(bytes: &[u8]) -> CopyId {
    CopyId::new(bytes.to_vec())
}
