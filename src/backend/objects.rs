use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct StoredSignature {
    pub name: String,
    pub email: String,
    pub millis_since_epoch: i64,
    pub tz_offset_minutes: i32,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct StoredSecureSig {
    pub data: Vec<u8>,
    pub sig: Vec<u8>,
}

// @todo(o11y): adding secure_sig changes the on-disk format — no versioned
//   decoding yet. old commits without this field will fail to deserialize.
//   acceptable pre-release; add V1 compat reader before persistent deployment.
#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct StoredCommit {
    pub parents: Vec<[u8; 32]>,
    pub predecessors: Vec<[u8; 32]>,
    pub root_tree_ids: Vec<[u8; 32]>,
    pub change_id: [u8; 32],
    pub description: String,
    pub author: StoredSignature,
    pub committer: StoredSignature,
    pub secure_sig: Option<StoredSecureSig>,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
#[repr(u8)]
pub enum StoredEntryKind {
    File { executable: bool, copy_id: Vec<u8> },
    Symlink,
    Tree,
    GitSubmodule,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct StoredTreeEntry {
    pub name: String,
    pub kind: StoredEntryKind,
    pub id: [u8; 32],
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize, CheckBytes)]
#[bytecheck(crate = bytecheck)]
pub struct StoredTree {
    pub entries: Vec<StoredTreeEntry>,
}
