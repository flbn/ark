// tldr: map iroh's blake3 hashes to strongly typed wrappers to prevent misuse

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{FromBytes, IntoBytes};

#[derive(Error, Debug)]
#[error("Reference name cannot be empty")]
pub struct EmptyRefName;

#[derive(Error, Debug)]
#[error("Remote ticket cannot be empty")]
pub struct EmptyRemoteTicket;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IntoBytes, FromBytes, Serialize, Deserialize,
)]
#[repr(C)] // NOTE:(@o11y) this allows direct casting from bytes w/o parsing
pub struct BlobHash(pub [u8; 32]); // 32-byte blake3 hash

// wrapped to ensure we never pass fuqqd up arrays where a hash is expected
impl From<iroh_blobs::Hash> for BlobHash {
    fn from(h: iroh_blobs::Hash) -> Self {
        Self(*h.as_bytes())
    }
}

// wrapped to ensure we never pass fuqqd up arrays where a hash is expected
impl From<BlobHash> for iroh_blobs::Hash {
    fn from(h: BlobHash) -> Self {
        iroh_blobs::Hash::from_bytes(h.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefName(String);

impl RefName {
    pub fn new(s: impl Into<String>) -> Result<Self, EmptyRefName> {
        let s = s.into();
        if s.is_empty() {
            return Err(EmptyRefName);
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTicket(Vec<u8>);

impl RemoteTicket {
    pub fn new(bytes: Vec<u8>) -> Result<Self, EmptyRemoteTicket> {
        if bytes.is_empty() {
            return Err(EmptyRemoteTicket);
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

// on disk header for every blob.
#[derive(
    Debug,
    Archive, // generates an 'ArchivedBlobMetadata' struct for raw mapping
    RkyvSerialize,
    RkyvDeserialize,
    CheckBytes, // checkbytes required to verify data integrity before accessing memory.
)]
#[bytecheck(crate = bytecheck)]
#[rkyv(compare(PartialEq))]
pub struct BlobMetadata {
    pub blob_type: BlobType,
    pub created_at: u64,
}

#[derive(
    Debug,
    Archive, // generates an 'ArchivedBlobType' struct for raw mapping
    RkyvSerialize,
    RkyvDeserialize,
    CheckBytes, // checkbytes required to verify data integrity before accessing memory.
)]
#[bytecheck(crate = bytecheck)]
#[rkyv(compare(PartialEq))]
#[repr(u8)]
pub enum BlobType {
    Commit,
    Tree,
    File,
}
