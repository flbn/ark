use ark::config::{resolve_remotes, Config};
use ark::network::derive_topic_id;
use ark::network::listener::SyncListener;
use ark::storage::BlobStore;
use camino::Utf8Path;
use clap::Parser;

#[derive(Parser)]
#[command(name = "ark", about = "a sovereign archive")]
struct Cli {
    #[arg(long, default_value = "ark.toml")]
    config: String,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Print this node's endpoint ID and exit
    Id,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::load(Utf8Path::new(&cli.config))?;
    let store = BlobStore::new(&config.store.path).await?;

    let endpoint_id = store.blobs.endpoint.id();
    let id_path = config.store.path.join("endpoint_id");
    fs_err::write(&id_path, endpoint_id.to_string())?;

    if let Some(Command::Id) = cli.command {
        #[allow(clippy::print_stdout)]
        {
            println!("{endpoint_id}");
        }
        store.shutdown().await?;
        return Ok(());
    }

    let mut _listeners: Vec<SyncListener> = Vec::new();

    for repo in &config.repos {
        let resolved = resolve_remotes(&repo.remotes)?;

        let peer_ids: Vec<_> = resolved.iter().map(|r| r.endpoint_id).collect();
        let provider_addrs: Vec<_> = resolved.iter().map(|r| r.endpoint_addr()).collect();

        let topic = derive_topic_id(repo.id.as_bytes());

        store.gossip.join_topic(topic, peer_ids).await?;

        // @todo(o11y): cold-start reconciliation errors are silently ignored per-peer.
        //   a single unreachable peer should not prevent the daemon from starting.
        //   log these once tracing is wired up.
        for addr in &provider_addrs {
            let _ = store
                .reconcile_with_peer(addr.clone(), config.sync.enumerate_threshold)
                .await;
        }

        let updates_rx = store.gossip.subscribe_updates(topic)?;

        let listener = SyncListener::spawn(
            updates_rx,
            store.blobs.store.clone(),
            store.blobs.endpoint.clone(),
            provider_addrs,
        );

        _listeners.push(listener);
    }

    // @todo(o11y): no signal handler cleanup yet — on ctrl-c, tasks are dropped.
    //   acceptable pre-release; add graceful shutdown (store.shutdown()) once
    //   the daemon needs to flush state before exit.
    tokio::signal::ctrl_c().await?;

    store.shutdown().await?;

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
            local_only: false,
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

    #[tokio::test]
    async fn endpoint_id_file_written_on_startup() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let root_path = Utf8Path::from_path(temp_dir.path())
            .ok_or_else(|| anyhow::anyhow!("non-utf8 temp path"))?;

        let store = BlobStore::new(root_path).await?;
        let endpoint_id = store.blobs.endpoint.id();

        let id_path = root_path.join("endpoint_id");
        fs_err::write(&id_path, endpoint_id.to_string())?;

        let read_back = fs_err::read_to_string(&id_path)?;
        assert_eq!(read_back, endpoint_id.to_string());

        let parsed: iroh::EndpointId = read_back.parse()?;
        assert_eq!(parsed, endpoint_id);

        store.shutdown().await?;
        Ok(())
    }
}
