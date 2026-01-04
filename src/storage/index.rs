// tldr: persistent index mapping hashes -> metadata and refs -> hashes.
// redb for acid guarantees (panic safe) + rkyv to map disk bytes directly to struct layout

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::domain::{
    ArchivedBlobMetadata, // on-disk type
    BlobHash,
    BlobMetadata,
    RefName,
};

#[allow(clippy::disallowed_types)]
use std::path::Path;
use thiserror::Error;

const BLOB_INDEX: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("blob_index");
const REFS: TableDefinition<&str, &[u8; 32]> = TableDefinition::new("refs");

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Database transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("Database table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("Database storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("Database commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("Serialization error: {0}")]
    Rkyv(String),
}

pub struct Index {
    db: Database,
}

impl Index {
    // opens db, ensures tables exist
    #[allow(clippy::disallowed_types)]
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(BLOB_INDEX)?;
            let _ = write_txn.open_table(REFS)?;
        }
        write_txn.commit()?;
        Ok(Self { db })
    }

    // atomic metadata write. serialize w rkyv + store in redb
    pub fn register_blob(&self, hash: BlobHash, meta: BlobMetadata) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOB_INDEX)?;

            let bytes = rkyv::to_bytes::<rancor::Error>(&meta)
                .map_err(|e: rancor::Error| IndexError::Rkyv(e.to_string()))?;

            table.insert(&hash.0, bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    // atomic ref update. here we move a pointer (i.e. 'main') to a new hash
    pub fn update_ref(&self, name: &RefName, hash: BlobHash) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REFS)?;
            table.insert(name.as_str(), &hash.0)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    // read metadata: verifies data integrity (checkbytes) before returning. we trust nothing on disk
    #[allow(dead_code)]
    pub fn get_blob_meta(&self, hash: BlobHash) -> Result<Option<BlobMetadata>, IndexError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLOB_INDEX)?;

        match table.get(&hash.0)? {
            Some(access) => {
                let bytes = access.value();

                // validate bytes match expected layout (checkbytes). access::<T> will return the _archived_ type, not the rust struct.
                let archived = rkyv::access::<ArchivedBlobMetadata, rancor::Error>(bytes)
                    .map_err(|e: rancor::Error| IndexError::Rkyv(e.to_string()))?;

                // deserialize back to usable rust struct (BlobMetadata)
                let meta: BlobMetadata = rkyv::deserialize::<BlobMetadata, rancor::Error>(archived)
                    .map_err(|e: rancor::Error| IndexError::Rkyv(e.to_string()))?;

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }
}
