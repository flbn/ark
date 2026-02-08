use camino::Utf8Path;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;

use std::sync::Arc;

use crate::domain::{BlobMetadata, BlobType, RefName};
use crate::network::derive_topic_id;
use crate::storage::BlobStore;

#[derive(Error, Debug)]
pub enum ControlError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Server returned error: {0}")]
    Server(String),
    #[error("Socket not found at {0} — is the daemon running?")]
    NotRunning(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Publish {
        data: Vec<u8>,
        ref_name: String,
        repo_id: String,
    },
    List,
    Get {
        hash: [u8; 32],
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Published { hash: [u8; 32] },
    BlobList { blobs: Vec<BlobEntry> },
    BlobData { data: Vec<u8> },
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobEntry {
    pub hash: [u8; 32],
    pub blob_type: String,
    pub created_at: u64,
    pub local_only: bool,
}

pub fn socket_path(store_path: &Utf8Path) -> camino::Utf8PathBuf {
    store_path.join("control.sock")
}

async fn send_msg(stream: &mut UnixStream, payload: &[u8]) -> Result<(), ControlError> {
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

async fn recv_msg(stream: &mut UnixStream) -> Result<Vec<u8>, ControlError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

// --- client ---

pub async fn send_request(
    store_path: &Utf8Path,
    request: &Request,
) -> Result<Response, ControlError> {
    let path = socket_path(store_path);
    let mut stream = UnixStream::connect(path.as_std_path())
        .await
        .map_err(|_| ControlError::NotRunning(path.to_string()))?;

    let payload = serde_json::to_vec(request)?;
    send_msg(&mut stream, &payload).await?;

    let resp_bytes = recv_msg(&mut stream).await?;
    let response: Response = serde_json::from_slice(&resp_bytes)?;
    Ok(response)
}

// --- server ---

pub struct ControlServer {
    _task: JoinHandle<()>,
}

impl ControlServer {
    pub fn spawn(
        store_path: &Utf8Path,
        store: Arc<BlobStore>,
        repo_ids: Vec<String>,
    ) -> Result<Self, ControlError> {
        let sock = socket_path(store_path);
        let _ = fs_err::remove_file(sock.as_std_path());

        let listener = std::os::unix::net::UnixListener::bind(sock.as_std_path())?;
        listener.set_nonblocking(true)?;
        let listener = UnixListener::from_std(listener)?;

        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => continue,
                };

                let store = store.clone();
                let repo_ids = repo_ids.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, &store, &repo_ids).await;
                });
            }
        });

        Ok(Self { _task: task })
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    store: &BlobStore,
    repo_ids: &[String],
) -> Result<(), ControlError> {
    let req_bytes = recv_msg(&mut stream).await?;
    let request: Request = serde_json::from_slice(&req_bytes)?;

    let response = match request {
        Request::Publish {
            data,
            ref_name,
            repo_id,
        } => handle_publish(store, repo_ids, &data, &ref_name, &repo_id).await,
        Request::List => handle_list(store),
        Request::Get { hash } => handle_get(store, hash).await,
    };

    let payload = serde_json::to_vec(&response)?;
    send_msg(&mut stream, &payload).await?;
    Ok(())
}

async fn handle_publish(
    store: &BlobStore,
    repo_ids: &[String],
    data: &[u8],
    ref_name: &str,
    repo_id: &str,
) -> Response {
    if !repo_ids.iter().any(|r| r == repo_id) {
        return Response::Error {
            message: format!("repo '{repo_id}' not configured"),
        };
    }

    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let meta = BlobMetadata {
        blob_type: BlobType::File,
        created_at,
        local_only: false,
    };

    let hash = match store.put_object(data, meta).await {
        Ok(h) => h,
        Err(e) => return Response::Error { message: e.to_string() },
    };

    let rn = match RefName::new(ref_name) {
        Ok(r) => r,
        Err(e) => return Response::Error { message: e.to_string() },
    };

    let topic = derive_topic_id(repo_id.as_bytes());
    if let Err(e) = store.update_reference_and_announce(rn, hash, topic).await {
        return Response::Error { message: e.to_string() };
    }

    Response::Published { hash: hash.0 }
}

fn handle_list(store: &BlobStore) -> Response {
    match store.index.list_all_blobs() {
        Ok(blobs) => {
            let entries = blobs
                .into_iter()
                .map(|(hash, meta)| BlobEntry {
                    hash: hash.0,
                    blob_type: format!("{:?}", meta.blob_type),
                    created_at: meta.created_at,
                    local_only: meta.local_only,
                })
                .collect();
            Response::BlobList { blobs: entries }
        }
        Err(e) => Response::Error { message: e.to_string() },
    }
}

async fn handle_get(store: &BlobStore, hash: [u8; 32]) -> Response {
    let blob_hash = crate::domain::BlobHash(hash);
    match store.blobs.get_bytes(blob_hash).await {
        Ok(data) => Response::BlobData { data },
        Err(e) => Response::Error { message: e.to_string() },
    }
}
