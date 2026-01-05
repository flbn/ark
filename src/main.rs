mod domain;
mod storage;

use domain::{BlobMetadata, BlobType, RefName};
use storage::BlobStore;

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
    use crate::domain::{BlobMetadata, BlobType};
    use crate::storage::BlobStore;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_persistence_simulator() -> anyhow::Result<()> {
        println!("starting test");
        let temp_dir = tempfile::tempdir()?;
        let root_path = temp_dir.path();

        let data = b"this data must survive the dropped connection";
        let meta = BlobMetadata {
            blob_type: BlobType::File,
            created_at: 9999,
        };

        let hash;

        {
            println!("new blob store");
            let store = BlobStore::new(root_path).await?;

            println!("storing blob hash...");
            hash = store.put_object(data, meta).await?;

            println!("shutting down store 1...");
            store.shutdown().await?;
        } // store drops here, but router is already dead

        {
            println!("new blob store");
            let store = BlobStore::new(root_path).await?;

            println!("getting blob meta...");
            let fetched_meta = store.index.get_blob_meta(hash)?.expect("index lost :0");
            assert_eq!(fetched_meta.created_at, 9999);

            println!("getting blob reader...");
            let mut reader = store.blobs.get_reader(hash).await?;
            let mut buffer = Vec::new();

            println!("reading blob data...");
            reader.read_to_end(&mut buffer).await?;
            assert_eq!(buffer, data);

            store.shutdown().await?;
        }

        Ok(())
    }
}
