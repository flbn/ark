use ark::domain::{BlobMetadata, BlobType, RefName};
use ark::storage::BlobStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = BlobStore::new("./data").await?;

    let content = b"data on disk (persistent)";
    let metadata = BlobMetadata {
        blob_type: BlobType::File,
        created_at: 1735689600,
    };

    println!("storing blob...");
    let hash = store.put_object(content, metadata).await?;
    println!("blob hash stored: {:?}", hash);

    let ref_name = RefName::new("main")?;
    store.update_reference(ref_name, hash).await?;
    println!("updated ref 'main'.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use ark::domain::{BlobMetadata, BlobType};
    use ark::storage::BlobStore;
    use camino::Utf8Path;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_persistence_simulator() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let root_path = Utf8Path::from_path(temp_dir.path())
            .ok_or_else(|| anyhow::anyhow!("non-utf8 temp path"))?;

        let data = b"this data must survive the dropped connection";
        let meta = BlobMetadata {
            blob_type: BlobType::File,
            created_at: 9999,
        };

        let hash;

        {
            let store = BlobStore::new(root_path).await?;
            hash = store.put_object(data, meta).await?;
            store.shutdown().await?;
        }

        {
            let store = BlobStore::new(root_path).await?;

            let fetched_meta = store.index.get_blob_meta(hash)?.ok_or_else(|| {
                anyhow::anyhow!("index lost blob metadata after restart")
            })?;
            assert_eq!(fetched_meta.created_at, 9999);

            let mut reader = store.blobs.get_reader(hash).await?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await?;
            assert_eq!(buffer, data);

            store.shutdown().await?;
        }

        Ok(())
    }
}
