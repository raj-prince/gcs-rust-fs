//! gRPC server implementation for [`FileSystemService`](proto::file_system_service_server::FileSystemService).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream, UnixListenerStream};
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

use crate::error::Error;
use crate::filesystem::FileSystem;
use crate::options::{ListOptions, OpenOptions, ReadOptions, ReadPattern, WriteOptions};
use crate::range::ByteRange;
use crate::rpc::convert::{error_to_status, ERROR_KIND_HEADER};
use crate::rpc::proto::file_system_service_server::{
    FileSystemService, FileSystemServiceServer,
};
use crate::rpc::proto::write_request::Payload;
use crate::rpc::proto::{
    InfoRequest, InfoResponse, ListRequest, ListResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};

/// Default chunk size (`1 MiB`) streamed per [`ReadResponse`] message when
/// the client does not specify `ReadOptions::chunk_size`.
pub const DEFAULT_READ_CHUNK_SIZE: usize = 1024 * 1024;

/// Maximum chunk size (`8 MiB`) streamed per [`ReadResponse`] message when
/// requested via `ReadOptions::chunk_size`.
pub const MAX_READ_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Maximum gRPC message size (`16 MiB`) accepted and emitted by the sidecar.
pub const MAX_GRPC_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// HTTP/2 initial per-stream flow-control window (`64 MiB`).
const HTTP2_STREAM_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

/// HTTP/2 initial connection-level flow-control window (`256 MiB`).
const HTTP2_CONNECTION_WINDOW_SIZE: u32 = 64 * 1024 * 1024;

/// HTTP/2 maximum frame size (`16 MiB - 1`, RFC 7540 limit).
const HTTP2_MAX_FRAME_SIZE: u32 = 4 * 1024 * 1024;

/// gRPC service handler backed by any [`FileSystem`] implementation.
#[derive(Clone)]
pub struct FileSystemRpcService {
    fs: Arc<dyn FileSystem>,
}

impl std::fmt::Debug for FileSystemRpcService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSystemRpcService").finish_non_exhaustive()
    }
}

impl FileSystemRpcService {
    /// Wrap an [`Arc<dyn FileSystem>`] in a gRPC service handler.
    pub fn new(fs: Arc<dyn FileSystem>) -> Self {
        Self { fs }
    }

    /// Convert this handler into a Tonic [`FileSystemServiceServer`] ready to
    /// be registered with [`tonic::transport::Server`].
    pub fn into_server(self) -> FileSystemServiceServer<Self> {
        FileSystemServiceServer::new(self)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    }

    fn configured_server_builder() -> Server {
        Server::builder()
            .initial_stream_window_size(HTTP2_STREAM_WINDOW_SIZE)
            .initial_connection_window_size(HTTP2_CONNECTION_WINDOW_SIZE)
            .max_frame_size(HTTP2_MAX_FRAME_SIZE)
    }

    /// Serve the [`FileSystemService`] over a Unix Domain Socket at `socket_path`
    /// until `shutdown` resolves.
    ///
    /// If a stale socket file already exists at `socket_path`, it is removed
    /// before binding, and parent directories are created if missing.
    pub async fn serve_uds_with_shutdown<F>(
        self,
        socket_path: impl AsRef<Path>,
        shutdown: F,
    ) -> crate::error::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.serve_uds_with_mode_and_shutdown(socket_path, None, shutdown).await
    }

    /// Serve the [`FileSystemService`] over a Unix Domain Socket at `socket_path`
    /// with optional Unix file permissions `mode` (e.g. `0o666`) until `shutdown` resolves.
    pub async fn serve_uds_with_mode_and_shutdown<F>(
        self,
        socket_path: impl AsRef<Path>,
        mode: Option<u32>,
        shutdown: F,
    ) -> crate::error::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let path = socket_path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| Error::io("create_dir_all", parent.display(), e))?;
            }
        }
        if path.exists() {
            std::fs::remove_file(path)
                .map_err(|e| Error::io("remove_file", path.display(), e))?;
        }
        let uds = UnixListener::bind(path).map_err(|e| Error::io("bind_uds", path.display(), e))?;

        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
                .map_err(|e| Error::io("set_permissions", path.display(), e))?;
        }

        let incoming = UnixListenerStream::new(uds);

        Self::configured_server_builder()
            .add_service(self.into_server())
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
            .map_err(|e| Error::with_source(crate::ErrorKind::Other, "gRPC UDS server error", e))
    }

    /// Serve the [`FileSystemService`] simultaneously across multiple Unix Domain
    /// Sockets at `socket_paths` until `shutdown` resolves.
    ///
    /// Stale socket files are unlinked, parent directories are created, and
    /// optional Unix file permissions `mode` (e.g. `0o666`) are set on each socket.
    pub async fn serve_multiple_uds_with_mode_and_shutdown<P, F>(
        self,
        socket_paths: impl IntoIterator<Item = P>,
        mode: Option<u32>,
        shutdown: F,
    ) -> crate::error::Result<()>
    where
        P: AsRef<Path>,
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let paths: Vec<std::path::PathBuf> = socket_paths
            .into_iter()
            .map(|p| p.as_ref().to_path_buf())
            .collect();
        if paths.is_empty() {
            return Err(Error::new(
                crate::ErrorKind::InvalidConfig,
                "at least one UDS socket path must be provided",
            ));
        }

        let mut streams = Vec::with_capacity(paths.len());
        for path in &paths {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| Error::io("create_dir_all", parent.display(), e))?;
                }
            }
            if path.exists() {
                std::fs::remove_file(path)
                    .map_err(|e| Error::io("remove_file", path.display(), e))?;
            }
            let uds = UnixListener::bind(path)
                .map_err(|e| Error::io("bind_uds", path.display(), e))?;

            #[cfg(unix)]
            if let Some(mode) = mode {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
                    .map_err(|e| Error::io("set_permissions", path.display(), e))?;
            }

            streams.push(UnixListenerStream::new(uds));
        }

        let incoming = futures_util::stream::select_all(streams);

        Self::configured_server_builder()
            .add_service(self.into_server())
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
            .map_err(|e| Error::with_source(crate::ErrorKind::Other, "gRPC multi-UDS server error", e))
    }

    /// Serve the [`FileSystemService`] over TCP at `addr` until `shutdown` resolves.
    pub async fn serve_tcp_with_shutdown<F>(
        self,
        addr: SocketAddr,
        shutdown: F,
    ) -> crate::error::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::io("bind_tcp", addr, e))?;
        let incoming = TcpListenerStream::new(listener);

        Self::configured_server_builder()
            .add_service(self.into_server())
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
            .map_err(|e| Error::with_source(crate::ErrorKind::Other, "gRPC TCP server error", e))
    }
}

#[tonic::async_trait]
impl FileSystemService for FileSystemRpcService {
    type ReadStream = ReceiverStream<std::result::Result<ReadResponse, Status>>;

    async fn info(
        &self,
        request: Request<InfoRequest>,
    ) -> std::result::Result<Response<InfoResponse>, Status> {
        let req = request.into_inner();
        debug!(path = %req.path, "rpc Info");
        let entry = self.fs.info(&req.path).await.map_err(error_to_status)?;
        Ok(Response::new(InfoResponse {
            entry: Some(entry.into()),
        }))
    }

    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> std::result::Result<Response<ListResponse>, Status> {
        let req = request.into_inner();
        debug!(path = %req.path, versions = req.versions, refresh = req.refresh, "rpc List");
        let opts = ListOptions {
            versions: req.versions,
            refresh: req.refresh,
        };
        let entries = self.fs.ls(&req.path, opts).await.map_err(error_to_status)?;
        let entries = entries.into_iter().map(Into::into).collect();
        Ok(Response::new(ListResponse { entries }))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> std::result::Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();
        let chunk_size = req
            .options
            .and_then(|o| o.chunk_size)
            .and_then(|c| usize::try_from(c).ok())
            .filter(|&c| c > 0)
            .unwrap_or(DEFAULT_READ_CHUNK_SIZE)
            .min(MAX_READ_CHUNK_SIZE);

        let range = req.range.map(ByteRange::from).unwrap_or(ByteRange::ALL);
        debug!(path = %req.path, ?range, chunk_size, "rpc Read");

        let open_opts = match (range.start(), range.end()) {
            (start_opt, Some(end))
                if start_opt.unwrap_or(0) >= 0 && end >= start_opt.unwrap_or(0) =>
            {
                let start = start_opt.unwrap_or(0) as u64;
                let end = end as u64;
                let span = end.saturating_sub(start);
                if span <= 4 * 1024 * 1024 {
                    if span as usize <= chunk_size {
                        let data = self
                            .fs
                            .cat_file(&req.path, range, ReadOptions::default())
                            .await
                            .map_err(error_to_status)?;
                        let (tx, rx) = mpsc::channel(1);
                        if !data.is_empty() {
                            let _ = tx.try_send(Ok(ReadResponse { data }));
                        }
                        return Ok(Response::new(ReceiverStream::new(rx)));
                    }
                    OpenOptions::read()
                        .with_block_size(chunk_size)
                        .with_pattern(ReadPattern::Random)
                } else if span >= 16 * 1024 * 1024 {
                    OpenOptions::read()
                        .with_block_size(8 * 1024 * 1024)
                        .with_pattern(ReadPattern::Sequential)
                } else {
                    OpenOptions::read()
                        .with_block_size(chunk_size)
                        .with_pattern(ReadPattern::Auto)
                }
            }
            (start_opt, None) if start_opt.unwrap_or(0) >= 0 => OpenOptions::read()
                .with_block_size(8 * 1024 * 1024)
                .with_pattern(ReadPattern::Sequential),
            _ => OpenOptions::read()
                .with_block_size(chunk_size)
                .with_pattern(ReadPattern::Auto),
        };

        // Stream chunks via `fs.open` to avoid buffering whole multi-gigabyte
        // files into memory (preventing sidecar daemon OOM).
        let mut handle = self
            .fs
            .open(&req.path, open_opts)
            .await
            .map_err(error_to_status)?;

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let file_size = match handle.size() {
                Some(s) => s,
                None => match handle.stat() {
                    Some(stat) => stat.size,
                    None => 0,
                },
            };

            let std_range = range.clamp(file_size);
            let start = std_range.start;
            let end = std_range.end;

            if start >= end {
                return;
            }

            if start > 0 {
                if let Err(e) = handle.seek(std::io::SeekFrom::Start(start)) {
                    let _ = tx.send(Err(error_to_status(e))).await;
                    return;
                }
            }

            let mut remaining = end - start;
            while remaining > 0 {
                let to_read = (chunk_size as u64).min(remaining) as usize;
                match handle.read(Some(to_read)).await {
                    Ok(chunk) => {
                        if chunk.is_empty() {
                            break;
                        }
                        let len = chunk.len() as u64;
                        remaining = remaining.saturating_sub(len);
                        if tx.send(Ok(ReadResponse { data: chunk })).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(error_to_status(e))).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();

        let first = stream
            .next()
            .await
            .ok_or_else(|| {
                let mut s = Status::invalid_argument("empty Write stream: expected WriteSpec first");
                if let Ok(v) = crate::ErrorKind::InvalidPath.as_str().parse() {
                    s.metadata_mut().insert(ERROR_KIND_HEADER, v);
                }
                s
            })?
            .map_err(|status| Status::new(status.code(), status.message().to_owned()))?;

        let spec = match first.payload {
            Some(Payload::Spec(spec)) => spec,
            _ => {
                let mut s = Status::invalid_argument(
                    "first WriteRequest message must carry WriteSpec",
                );
                if let Ok(v) = crate::ErrorKind::InvalidPath.as_str().parse() {
                    s.metadata_mut().insert(ERROR_KIND_HEADER, v);
                }
                return Err(s);
            }
        };

        debug!(path = %spec.path, mode = spec.mode, "rpc Write open");
        let write_opts = WriteOptions::from(&spec);
        let open_opts = OpenOptions::from_write(write_opts);
        let mut file = self
            .fs
            .open(&spec.path, open_opts)
            .await
            .map_err(error_to_status)?;

        while let Some(msg) = stream.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(status) => {
                    // Client cancelled or stream failed: explicitly discard the
                    // pending upload before returning the error.
                    let _ = file.discard().await;
                    return Err(status);
                }
            };
            match msg.payload {
                Some(Payload::Data(chunk)) => {
                    if !chunk.is_empty() {
                        if let Err(err) = file.write(chunk).await {
                            let _ = file.discard().await;
                            return Err(error_to_status(err));
                        }
                    }
                }
                Some(Payload::Spec(_)) => {
                    let _ = file.discard().await;
                    return Err(Status::invalid_argument(
                        "unexpected duplicate WriteSpec mid-stream",
                    ));
                }
                None => {}
            }
        }

        file.close().await.map_err(error_to_status)?;
        let size = file.size().unwrap_or_else(|| file.tell());
        let stat = file.stat().cloned().map(Into::into);
        debug!(path = %spec.path, size, "rpc Write completed");

        Ok(Response::new(WriteResponse { size, stat }))
    }
}
