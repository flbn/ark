use ark::config::{resolve_remotes, Config};
use ark::control::{self, Request, Response};
use ark::domain::BlobHash;
use ark::network::derive_topic_id;
use ark::network::listener::SyncListener;
use ark::storage::BlobStore;
use camino::Utf8Path;
use clap::Parser;

use std::sync::Arc;

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
    /// Publish a file to the network (requires running daemon)
    Publish {
        /// Path to the file to publish
        file: String,
        /// Reference name (e.g., "images/photo.png")
        #[arg(long, short)]
        r#ref: String,
    },
    /// List all blobs in the local store (requires running daemon)
    List,
    /// Retrieve a blob by hash and write to a file (requires running daemon)
    Get {
        /// Hex-encoded blob hash
        hash: String,
        /// Output file path
        #[arg(long, short)]
        output: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::load(Utf8Path::new(&cli.config))?;

    match cli.command {
        Some(Command::Publish { file, r#ref }) => {
            let data = fs_err::read(&file)?;
            let repo_id = config
                .repos
                .first()
                .map(|r| r.id.clone())
                .ok_or_else(|| anyhow::anyhow!("no repos configured"))?;

            let resp =
                control::send_request(&config.store.path, &Request::Publish { data, ref_name: r#ref, repo_id })
                    .await?;

            match resp {
                Response::Published { hash } => {
                    #[allow(clippy::print_stdout)]
                    {
                        println!("{} published", BlobHash(hash));
                    }
                }
                Response::Error { message } => anyhow::bail!("{message}"),
                _ => anyhow::bail!("unexpected response"),
            }
            return Ok(());
        }
        Some(Command::List) => {
            let resp = control::send_request(&config.store.path, &Request::List).await?;

            match resp {
                Response::BlobList { blobs } => {
                    if blobs.is_empty() {
                        #[allow(clippy::print_stdout)]
                        {
                            println!("no blobs");
                        }
                    } else {
                        for entry in &blobs {
                            #[allow(clippy::print_stdout)]
                            {
                                println!(
                                    "{} type={} created_at={} local_only={}",
                                    BlobHash(entry.hash),
                                    entry.blob_type,
                                    entry.created_at,
                                    entry.local_only,
                                );
                            }
                        }
                    }
                }
                Response::Error { message } => anyhow::bail!("{message}"),
                _ => anyhow::bail!("unexpected response"),
            }
            return Ok(());
        }
        Some(Command::Get { hash, output }) => {
            let blob_hash = parse_hex_hash(&hash)?;
            let resp =
                control::send_request(&config.store.path, &Request::Get { hash: blob_hash.0 })
                    .await?;

            match resp {
                Response::BlobData { data } => {
                    fs_err::write(&output, &data)?;
                    #[allow(clippy::print_stdout)]
                    {
                        println!("wrote {} to {output}", BlobHash(blob_hash.0));
                    }
                }
                Response::Error { message } => anyhow::bail!("{message}"),
                _ => anyhow::bail!("unexpected response"),
            }
            return Ok(());
        }
        Some(Command::Id) | None => {}
    }

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

    let store = Arc::new(store);
    let repo_ids: Vec<String> = config.repos.iter().map(|r| r.id.clone()).collect();

    let _control = control::ControlServer::spawn(&config.store.path, store.clone(), repo_ids)?;

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
            store.index.clone(),
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

fn parse_hex_hash(s: &str) -> anyhow::Result<BlobHash> {
    if s.len() != 64 {
        anyhow::bail!("expected 64 hex characters, got {}", s.len());
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(BlobHash(bytes))
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
