mod domain;
mod storage;

use domain::{BlobMetadata, BlobType, RefName};
use storage::BlobStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("initing ark...");

    let store = BlobStore::new("ark.db").await?;

    let content = b"foo.";
    let metadata = BlobMetadata {
        blob_type: BlobType::File,
        created_at: 1735689600,
    };

    println!("storing blob...");
    let hash = store.put_object(content, metadata).await?;
    println!("stored Blob hash: {:?}", hash);

    let ref_name = RefName::new("main")?;

    store.update_reference(ref_name, hash).await?;
    println!("updated ref 'main' to point to blob.");

    println!("phase 1 dun");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::domain::{BlobMetadata, BlobType};
    use crate::storage::BlobStore;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_end_to_end_storage() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("ark_e2e.db");
        let store = BlobStore::new(&db_path).await?;

        let data = b"Phase 1 Verification Payload";
        let meta = BlobMetadata {
            blob_type: BlobType::File,
            created_at: 1234567890,
        };

        let hash = store.put_object(data, meta).await?;

        let mut reader = store.blobs.get_reader(hash).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        assert_eq!(buffer, data);

        let fetched_meta = store.index.get_blob_meta(hash)?.expect("Metadata missing");
        assert_eq!(fetched_meta.created_at, 1234567890);

        Ok(())
    }

    #[tokio::test]
    async fn test_persistence_simulator() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("ark_persistence.db");

        let data = b"Persistent Data";
        let meta = BlobMetadata {
            blob_type: BlobType::Commit,
            created_at: 9999,
        };

        let hash = {
            let store = BlobStore::new(&db_path).await?;
            store.put_object(data, meta).await?
        };

        {
            let store = BlobStore::new(&db_path).await?;

            let fetched_meta = store.index.get_blob_meta(hash)?;
            assert!(fetched_meta.is_some(), "index data lost after restart!");
            assert_eq!(fetched_meta.unwrap().created_at, 9999);
        }

        Ok(())
    }
}
