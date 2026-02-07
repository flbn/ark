// tldr: persistent index mapping hashes -> metadata and refs -> hashes.
// redb for acid guarantees (panic safe) + rkyv to map disk bytes directly to struct layout

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use camino::Utf8Path;

use crate::domain::{BlobHash, BlobMetadata, RefName, RemoteTicket};

use thiserror::Error;

const BLOB_INDEX: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("blob_index");
const REFS: TableDefinition<&str, &[u8; 32]> = TableDefinition::new("refs");
const REMOTES: TableDefinition<&str, &[u8]> = TableDefinition::new("remotes");

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
    pub fn new(path: impl AsRef<Utf8Path>) -> anyhow::Result<Self> {
        let db = Database::create(path.as_ref())?;
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(BLOB_INDEX)?;
            let _ = write_txn.open_table(REFS)?;
            let _ = write_txn.open_table(REMOTES)?;
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

    #[allow(dead_code)]
    pub fn get_ref(&self, name: &RefName) -> Result<Option<BlobHash>, IndexError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REFS)?;
        match table.get(name.as_str())? {
            Some(access) => Ok(Some(BlobHash(*access.value()))),
            None => Ok(None),
        }
    }

    // read metadata: validates + deserializes from disk bytes.
    // uses from_bytes which handles alignment internally (redb gives us unaligned slices).
    #[allow(dead_code)]
    pub fn get_blob_meta(&self, hash: BlobHash) -> Result<Option<BlobMetadata>, IndexError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLOB_INDEX)?;

        match table.get(&hash.0)? {
            Some(access) => {
                let bytes = access.value();
                let meta = rkyv::from_bytes::<BlobMetadata, rancor::Error>(bytes)
                    .map_err(|e: rancor::Error| IndexError::Rkyv(e.to_string()))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    pub fn set_blob_local_only(&self, hash: BlobHash, local_only: bool) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(BLOB_INDEX)?;
            let new_bytes = {
                let existing = table.get(&hash.0)?;
                match existing {
                    Some(access) => {
                        let bytes = access.value();
                        let mut meta = rkyv::from_bytes::<BlobMetadata, rancor::Error>(bytes)
                            .map_err(|e: rancor::Error| IndexError::Rkyv(e.to_string()))?;
                        meta.local_only = local_only;
                        rkyv::to_bytes::<rancor::Error>(&meta)
                            .map_err(|e: rancor::Error| IndexError::Rkyv(e.to_string()))?
                    }
                    None => return Err(IndexError::Rkyv("blob not found in index".to_string())),
                }
            };
            table.insert(&hash.0, new_bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    // @todo(o11y): O(n) scan — add a secondary SHARED_BLOBS table if this becomes a bottleneck at scale
    pub fn list_shared_blobs(&self) -> Result<Vec<BlobHash>, IndexError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLOB_INDEX)?;
        let mut result = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let meta = rkyv::from_bytes::<BlobMetadata, rancor::Error>(value.value())
                .map_err(|e: rancor::Error| IndexError::Rkyv(e.to_string()))?;
            if !meta.local_only {
                result.push(BlobHash(*key.value()));
            }
        }
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn store_remote(&self, name: &str, ticket: &RemoteTicket) -> Result<(), IndexError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REMOTES)?;
            table.insert(name, ticket.as_bytes())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_remote(&self, name: &str) -> Result<Option<RemoteTicket>, IndexError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REMOTES)?;
        match table.get(name)? {
            Some(access) => {
                let bytes = access.value().to_vec();
                RemoteTicket::new(bytes)
                    .map(Some)
                    .map_err(|e| IndexError::Rkyv(e.to_string()))
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn remove_remote(&self, name: &str) -> Result<bool, IndexError> {
        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(REMOTES)?;
            table.remove(name)?.is_some()
        };
        write_txn.commit()?;
        Ok(removed)
    }
}
