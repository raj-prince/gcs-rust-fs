//! gRPC client ([`RemoteFs`] and [`RemoteFile`]) that connects to a
//! [`FileSystemRpcService`](crate::rpc::FileSystemRpcService) over a Unix Domain
//! Socket (`unix://`) or TCP endpoint (`http://`).

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::{Response, Streaming};
use tower::service_fn;

use crate::entry::Entry;
use crate::error::{Error, ErrorKind, Result};
use crate::file::File;
use crate::filesystem::FileSystem;
use crate::options::{
    ListOptions, MkdirOptions, OpenMode, OpenOptions, ReadOptions, WriteMode, WriteOptions,
};
use crate::range::ByteRange;
use crate::rpc::convert::status_to_error;
use crate::rpc::proto;
use crate::rpc::proto::file_system_service_client::FileSystemServiceClient;
use crate::rpc::proto::write_request::Payload;
use crate::rpc::proto::{
    InfoRequest, ListRequest, ReadOptions as ProtoReadOptions, ReadRequest, ReadResponse,
    WriteRequest, WriteResponse, WriteSpec,
};
use crate::rpc::server::{DEFAULT_READ_CHUNK_SIZE, MAX_GRPC_MESSAGE_SIZE, MAX_READ_CHUNK_SIZE};
use crate::stat::ObjectStat;

/// Default read-ahead block size (`8 MiB`) used by [`RemoteFile`] when
/// `OpenOptions::block_size` is unset.
const DEFAULT_BLOCK_SIZE: usize = 8 * 1024 * 1024;

/// Default number of parallel UDS HTTP/2 subchannels opened by [`RemoteFs::connect_uds`].
const DEFAULT_UDS_POOL_SIZE: usize = 16;

/// HTTP/2 initial per-stream flow-control window (`16 MiB`).
const HTTP2_STREAM_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

/// HTTP/2 initial connection-level flow-control window (`64 MiB`).
const HTTP2_CONNECTION_WINDOW_SIZE: u32 = 64 * 1024 * 1024;

/// HTTP/2 maximum frame size (`4 MiB`).
const HTTP2_MAX_FRAME_SIZE: u32 = 4 * 1024 * 1024;

/// A [`FileSystem`] client that delegates operations to a `gcs-rust-fs` gRPC
/// sidecar server over a Unix Domain Socket or TCP connection.
#[derive(Clone, Debug)]
pub struct RemoteFs {
    clients: Arc<Vec<FileSystemServiceClient<Channel>>>,
    next_idx: Arc<AtomicUsize>,
}

impl RemoteFs {
    fn new_client(channel: Channel) -> FileSystemServiceClient<Channel> {
        FileSystemServiceClient::new(channel)
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    }

    /// Wrap an existing Tonic [`Channel`].
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            clients: Arc::new(vec![Self::new_client(channel)]),
            next_idx: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Wrap multiple Tonic [`Channel`]s for round-robin load balancing.
    pub fn from_channels(channels: Vec<Channel>) -> Self {
        let clients = channels.into_iter().map(Self::new_client).collect();
        Self {
            clients: Arc::new(clients),
            next_idx: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn next_client(&self) -> FileSystemServiceClient<Channel> {
        if self.clients.len() == 1 {
            return self.clients[0].clone();
        }
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[idx].clone()
    }

    async fn connect_one_uds(path: PathBuf) -> Result<Channel> {
        // Tonic requires a valid HTTP URI for the HTTP/2 authority header even
        // when the underlying transport is a Unix Domain Socket.
        Endpoint::try_from("http://[::]:50051")
            .map_err(|e| Error::with_source(ErrorKind::ClientInit, "invalid dummy URI", e))?
            .initial_stream_window_size(HTTP2_STREAM_WINDOW_SIZE)
            .initial_connection_window_size(HTTP2_CONNECTION_WINDOW_SIZE)
            .max_frame_size(HTTP2_MAX_FRAME_SIZE)
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    let stream = UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|e| {
                Error::with_source(
                    ErrorKind::ClientInit,
                    "failed to connect to gRPC Unix Domain Socket",
                    e,
                )
            })
    }

    /// Connect to a sidecar gRPC server listening on a Unix Domain Socket at
    /// `socket_path` (e.g. `/var/run/gcsfs/sidecar.sock`).
    pub async fn connect_uds(socket_path: impl AsRef<Path>) -> Result<Self> {
        Self::connect_uds_with_pool_size(socket_path, DEFAULT_UDS_POOL_SIZE).await
    }

    /// Connect to a sidecar gRPC server listening on a Unix Domain Socket at
    /// `socket_path` with a specified number of pooled UDS HTTP/2 connections.
    pub async fn connect_uds_with_pool_size(
        socket_path: impl AsRef<Path>,
        pool_size: usize,
    ) -> Result<Self> {
        let path: PathBuf = socket_path.as_ref().to_path_buf();
        let n = pool_size.max(1);
        let mut futs = Vec::with_capacity(n);
        for _ in 0..n {
            futs.push(Self::connect_one_uds(path.clone()));
        }
        let channels = futures_util::future::try_join_all(futs).await?;
        Ok(Self::from_channels(channels))
    }

    /// Connect to a sidecar gRPC server listening on a TCP endpoint (e.g.
    /// `"http://127.0.0.1:50051"`).
    pub async fn connect_tcp(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Endpoint::from_shared(endpoint.clone())
            .map_err(|e| {
                Error::with_source(
                    ErrorKind::InvalidConfig,
                    format!("invalid gRPC endpoint {endpoint:?}"),
                    e,
                )
            })?
            .initial_stream_window_size(HTTP2_STREAM_WINDOW_SIZE)
            .initial_connection_window_size(HTTP2_CONNECTION_WINDOW_SIZE)
            .max_frame_size(HTTP2_MAX_FRAME_SIZE)
            .connect()
            .await
            .map_err(|e| {
                Error::with_source(
                    ErrorKind::ClientInit,
                    format!("failed to connect to gRPC endpoint {endpoint:?}"),
                    e,
                )
            })?;

        Ok(Self::from_channel(channel))
    }

    /// Perform a streaming `Write` RPC from an already-constructed `WriteSpec`
    /// and a single contiguous payload, returning the resulting `(size, stat)`.
    pub async fn write_bytes(
        &self,
        path: &str,
        data: Bytes,
        opts: WriteOptions,
    ) -> Result<(u64, Option<ObjectStat>)> {
        let chunk_size = opts
            .block_size
            .filter(|&b| b > 0)
            .unwrap_or(MAX_READ_CHUNK_SIZE)
            .min(MAX_READ_CHUNK_SIZE);

        let spec = WriteSpec {
            path: path.to_owned(),
            mode: proto::WriteMode::from(opts.mode) as i32,
            content_type: opts.content_type,
            metadata: opts.metadata,
            block_size: opts.block_size.map(|b| b as u64),
        };

        let mut messages = Vec::new();
        messages.push(WriteRequest {
            payload: Some(Payload::Spec(spec)),
        });

        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + chunk_size).min(data.len());
            messages.push(WriteRequest {
                payload: Some(Payload::Data(data.slice(offset..end))),
            });
            offset = end;
        }

        let mut client = self.next_client();
        let response = client
            .write(tokio_stream::iter(messages))
            .await
            .map_err(status_to_error)?
            .into_inner();

        Ok((response.size, response.stat.map(ObjectStat::from)))
    }

    async fn read_bytes_with_chunk_size(
        &self,
        path: &str,
        range: ByteRange,
        chunk_size: Option<usize>,
    ) -> Result<Bytes> {
        let mut client = self.next_client();
        let proto_opts = chunk_size.map(|c| ProtoReadOptions {
            chunk_size: Some(c as u64),
        });
        let mut stream = client
            .read(ReadRequest {
                path: path.to_owned(),
                range: Some(range.into()),
                options: proto_opts,
            })
            .await
            .map_err(status_to_error)?
            .into_inner();

        let mut first: Option<Bytes> = None;
        let mut buf: Option<BytesMut> = None;
        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res.map_err(status_to_error)?.data;
            if chunk.is_empty() {
                continue;
            }
            if let Some(b) = buf.as_mut() {
                b.extend_from_slice(&chunk);
            } else if let Some(head) = first.take() {
                let mut b = BytesMut::with_capacity(head.len() + chunk.len());
                b.extend_from_slice(&head);
                b.extend_from_slice(&chunk);
                buf = Some(b);
            } else {
                first = Some(chunk);
            }
        }

        Ok(match (buf, first) {
            (Some(b), _) => b.freeze(),
            (None, Some(head)) => head,
            (None, None) => Bytes::new(),
        })
    }
}

#[async_trait]
impl FileSystem for RemoteFs {
    async fn info(&self, path: &str) -> Result<Entry> {
        let mut client = self.next_client();
        let response = client
            .info(InfoRequest {
                path: path.to_owned(),
            })
            .await
            .map_err(status_to_error)?
            .into_inner();

        let proto_entry = response.entry.ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                "server returned InfoResponse without an Entry",
            )
        })?;
        Entry::try_from(proto_entry)
    }

    async fn cat_file(&self, path: &str, range: ByteRange, _opts: ReadOptions) -> Result<Bytes> {
        self.read_bytes_with_chunk_size(path, range, None).await
    }

    async fn pipe_file(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<()> {
        self.write_bytes(path, data, opts).await.map(|_| ())
    }

    async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>> {
        match opts.mode {
            OpenMode::Read => {
                let entry = self.info(path).await?;
                if entry.is_dir() {
                    return Err(Error::is_a_directory(path));
                }
                let stat = entry.stat.unwrap_or_else(|| {
                    let mut s = ObjectStat::default();
                    s.size = entry.size;
                    s
                });
                // Pin the generation observed at open time when present so
                // subsequent ranged reads see an immutable snapshot.
                let pinned_path = if stat.generation > 0 && !path.contains('#') {
                    format!("{}#{}", entry.path, stat.generation)
                } else {
                    entry.path.clone()
                };
                Ok(Box::new(RemoteFile::new_reader(
                    entry.path,
                    pinned_path,
                    stat,
                    self.clone(),
                    opts.block_size,
                )))
            }
            OpenMode::Write | OpenMode::CreateNew => {
                let write_mode = match opts.mode {
                    OpenMode::CreateNew => WriteMode::Create,
                    _ => WriteMode::Overwrite,
                };
                let spec = WriteSpec {
                    path: path.to_owned(),
                    mode: proto::WriteMode::from(write_mode) as i32,
                    content_type: opts.content_type,
                    metadata: opts.metadata,
                    block_size: opts.block_size.map(|b| b as u64),
                };

                let (tx, rx) = mpsc::channel::<WriteRequest>(16);
                tx.send(WriteRequest {
                    payload: Some(Payload::Spec(spec)),
                })
                .await
                .map_err(|_| Error::new(ErrorKind::Other, "failed to enqueue WriteSpec"))?;

                let mut client = self.next_client();
                let task = tokio::spawn(async move {
                    client
                        .write(ReceiverStream::new(rx))
                        .await
                        .map(Response::into_inner)
                        .map_err(status_to_error)
                });

                Ok(Box::new(RemoteFile::new_writer(
                    path.to_owned(),
                    opts.mode,
                    tx,
                    task,
                )))
            }
            OpenMode::Append => Err(Error::unsupported(
                "append mode is not yet supported in Phase 1 of the gRPC sidecar",
            )),
        }
    }

    async fn ls(&self, path: &str, opts: ListOptions) -> Result<Vec<Entry>> {
        let mut client = self.next_client();
        let response = client
            .list(ListRequest {
                path: path.to_owned(),
                versions: opts.versions,
                refresh: opts.refresh,
            })
            .await
            .map_err(status_to_error)?
            .into_inner();

        response
            .entries
            .into_iter()
            .map(Entry::try_from)
            .collect()
    }

    async fn rm_file(&self, _path: &str) -> Result<()> {
        Err(Error::unsupported(
            "rm_file is not yet exposed by the Phase 1 gRPC sidecar (only Info, Read, Write)",
        ))
    }

    async fn mkdir(&self, _path: &str, _opts: MkdirOptions) -> Result<()> {
        Err(Error::unsupported(
            "mkdir is not yet exposed by the Phase 1 gRPC sidecar (only Info, Read, Write)",
        ))
    }

    async fn rmdir(&self, _path: &str) -> Result<()> {
        Err(Error::unsupported(
            "rmdir is not yet exposed by the Phase 1 gRPC sidecar (only Info, Read, Write)",
        ))
    }
}

/// A [`File`] handle backed by [`RemoteFs`].
pub struct RemoteFile {
    path: String,
    mode: OpenMode,
    closed: bool,
    state: RemoteFileState,
}

enum RemoteFileState {
    Read {
        pinned_path: String,
        stat: ObjectStat,
        fs: RemoteFs,
        pos: u64,
        block_size: usize,
        seeked: bool,
        prev_read_end: Option<u64>,
        stream: Option<Streaming<ReadResponse>>,
        stream_chunk: Bytes,
    },
    Write {
        tx: Option<mpsc::Sender<WriteRequest>>,
        task: Option<tokio::task::JoinHandle<Result<WriteResponse>>>,
        written: u64,
        size: Option<u64>,
        stat: Option<ObjectStat>,
    },
}

impl RemoteFile {
    fn new_reader(
        path: String,
        pinned_path: String,
        stat: ObjectStat,
        fs: RemoteFs,
        block_size: Option<usize>,
    ) -> Self {
        Self {
            path,
            mode: OpenMode::Read,
            closed: false,
            state: RemoteFileState::Read {
                pinned_path,
                stat,
                fs,
                pos: 0,
                block_size: block_size.unwrap_or(DEFAULT_BLOCK_SIZE),
                seeked: false,
                prev_read_end: None,
                stream: None,
                stream_chunk: Bytes::new(),
            },
        }
    }

    fn new_writer(
        path: String,
        mode: OpenMode,
        tx: mpsc::Sender<WriteRequest>,
        task: tokio::task::JoinHandle<Result<WriteResponse>>,
    ) -> Self {
        Self {
            path,
            mode,
            closed: false,
            state: RemoteFileState::Write {
                tx: Some(tx),
                task: Some(task),
                written: 0,
                size: None,
                stat: None,
            },
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(Error::closed(&self.path))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl File for RemoteFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn mode(&self) -> OpenMode {
        self.mode
    }

    fn closed(&self) -> bool {
        self.closed
    }

    fn tell(&self) -> u64 {
        match &self.state {
            RemoteFileState::Read { pos, .. } => *pos,
            RemoteFileState::Write { written, size, .. } => size.unwrap_or(*written),
        }
    }

    fn size(&self) -> Option<u64> {
        match &self.state {
            RemoteFileState::Read { stat, .. } => Some(stat.size),
            RemoteFileState::Write { size, .. } => *size,
        }
    }

    fn stat(&self) -> Option<&ObjectStat> {
        match &self.state {
            RemoteFileState::Read { stat, .. } => Some(stat),
            RemoteFileState::Write { stat, .. } => stat.as_ref(),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.ensure_open()?;
        match &mut self.state {
            RemoteFileState::Read {
                pos: cursor,
                stat,
                seeked,
                prev_read_end,
                stream,
                stream_chunk,
                ..
            } => {
                let (base, offset) = match pos {
                    SeekFrom::Start(offset) => (0u64, offset as i64),
                    SeekFrom::Current(offset) => (*cursor, offset),
                    SeekFrom::End(offset) => (stat.size, offset),
                };
                let target = i128::from(base) + i128::from(offset);
                if target < 0 {
                    return Err(Error::invalid_range(format!(
                        "seek to {target} is before the start of the file"
                    )));
                }
                let new_pos =
                    u64::try_from(target).map_err(|_| Error::invalid_range("seek overflow"))?;
                if new_pos != *cursor {
                    *cursor = new_pos;
                    *seeked = true;
                    *prev_read_end = None;
                    *stream = None;
                    *stream_chunk = Bytes::new();
                } else if new_pos > 0 && prev_read_end.is_none() {
                    *seeked = true;
                }
                Ok(*cursor)
            }
            RemoteFileState::Write { .. } => Err(Error::unsupported(format!(
                "{} is open for writing ({})",
                self.path,
                self.mode.as_str()
            ))),
        }
    }

    async fn read(&mut self, len: Option<usize>) -> Result<Bytes> {
        self.ensure_open()?;
        match &mut self.state {
            RemoteFileState::Read {
                pinned_path,
                stat,
                fs,
                pos,
                block_size,
                seeked,
                prev_read_end,
                stream,
                stream_chunk,
            } => {
                let remaining = stat.size.saturating_sub(*pos);
                let want = match len {
                    None => remaining,
                    Some(n) => (n as u64).min(remaining),
                };
                if want == 0 {
                    return Ok(Bytes::new());
                }
                let want_usize = want as usize;

                let is_sequential = (*pos == 0 && !*seeked && prev_read_end.is_none())
                    || *prev_read_end == Some(*pos)
                    || stream.is_some();

                if !is_sequential {
                    // Rule 7 single-read isolation on the first read after a non-zero seek:
                    // fetch the exact requested byte range `[pos, pos + want)`.
                    let req_chunk = want_usize.max(DEFAULT_READ_CHUNK_SIZE).min(MAX_READ_CHUNK_SIZE);
                    let bytes = fs
                        .read_bytes_with_chunk_size(
                            pinned_path,
                            ByteRange::span(*pos, *pos + want),
                            Some(req_chunk),
                        )
                        .await?;
                    let out = bytes.slice(0..want_usize.min(bytes.len()));
                    *pos += out.len() as u64;
                    *prev_read_end = Some(*pos);
                    return Ok(out);
                }

                if stream.is_none() {
                    let stream_chunk_size =
                        (*block_size).max(DEFAULT_READ_CHUNK_SIZE).min(MAX_READ_CHUNK_SIZE);
                    let mut client = fs.next_client();
                    let s = client
                        .read(ReadRequest {
                            path: pinned_path.clone(),
                            range: Some(ByteRange::span(*pos, stat.size).into()),
                            options: Some(ProtoReadOptions {
                                chunk_size: Some(stream_chunk_size as u64),
                            }),
                        })
                        .await
                        .map_err(status_to_error)?
                        .into_inner();
                    *stream = Some(s);
                }

                // Fast path 1: satisfy directly from leftover bytes in `stream_chunk`.
                if stream_chunk.len() >= want_usize {
                    let out = stream_chunk.split_to(want_usize);
                    *pos += out.len() as u64;
                    *prev_read_end = Some(*pos);
                    return Ok(out);
                }

                // Fast path 2: `stream_chunk` is empty and the next frame from `stream`
                // has at least `want_usize` bytes (zero-copy `Bytes` return).
                if stream_chunk.is_empty() {
                    if let Some(s) = stream.as_mut() {
                        while let Some(res) = s.next().await {
                            let mut chunk = res.map_err(status_to_error)?.data;
                            if chunk.is_empty() {
                                continue;
                            }
                            if chunk.len() >= want_usize {
                                let out = chunk.split_to(want_usize);
                                *stream_chunk = chunk;
                                *pos += out.len() as u64;
                                *prev_read_end = Some(*pos);
                                return Ok(out);
                            }
                            *stream_chunk = chunk;
                            break;
                        }
                    }
                    if stream_chunk.is_empty() {
                        *stream = None;
                        return Ok(Bytes::new());
                    }
                }

                // Multi-chunk assembly when `want_usize` spans multiple frames.
                let mut buf = BytesMut::with_capacity(want_usize);
                buf.extend_from_slice(stream_chunk);
                *stream_chunk = Bytes::new();

                if let Some(s) = stream.as_mut() {
                    while buf.len() < want_usize {
                        match s.next().await {
                            Some(res) => {
                                let mut chunk = res.map_err(status_to_error)?.data;
                                if chunk.is_empty() {
                                    continue;
                                }
                                let needed = want_usize - buf.len();
                                if chunk.len() <= needed {
                                    buf.extend_from_slice(&chunk);
                                } else {
                                    let head = chunk.split_to(needed);
                                    buf.extend_from_slice(&head);
                                    *stream_chunk = chunk;
                                }
                            }
                            None => {
                                *stream = None;
                                break;
                            }
                        }
                    }
                }

                let out = buf.freeze();
                *pos += out.len() as u64;
                *prev_read_end = Some(*pos);
                Ok(out)
            }
            RemoteFileState::Write { .. } => Err(Error::unsupported(format!(
                "{} is open for writing ({})",
                self.path,
                self.mode.as_str()
            ))),
        }
    }

    async fn read_range(&self, range: ByteRange) -> Result<Bytes> {
        self.ensure_open()?;
        match &self.state {
            RemoteFileState::Read {
                pinned_path,
                stat,
                fs,
                ..
            } => {
                let clamped = range.clamp(stat.size);
                if clamped.start >= clamped.end {
                    return Ok(Bytes::new());
                }
                let span = (clamped.end - clamped.start) as usize;
                let req_chunk = span.max(DEFAULT_READ_CHUNK_SIZE).min(MAX_READ_CHUNK_SIZE);
                fs.read_bytes_with_chunk_size(
                    pinned_path,
                    ByteRange::span(clamped.start, clamped.end),
                    Some(req_chunk),
                )
                .await
            }
            RemoteFileState::Write { .. } => Err(Error::unsupported(format!(
                "{} is open for writing ({})",
                self.path,
                self.mode.as_str()
            ))),
        }
    }

    async fn write(&mut self, data: Bytes) -> Result<()> {
        self.ensure_open()?;
        match &mut self.state {
            RemoteFileState::Write {
                tx, task, written, ..
            } => {
                let len = data.len() as u64;
                let Some(sender) = tx.as_ref() else {
                    return Err(Error::closed(&self.path));
                };
                let mut offset = 0;
                while offset < data.len() {
                    let end = (offset + MAX_READ_CHUNK_SIZE).min(data.len());
                    let chunk = data.slice(offset..end);
                    if sender
                        .send(WriteRequest {
                            payload: Some(Payload::Data(chunk)),
                        })
                        .await
                        .is_err()
                    {
                        *tx = None;
                        if let Some(handle) = task.take() {
                            return match handle.await {
                                Ok(Err(e)) => Err(e),
                                Ok(Ok(_)) => Err(Error::new(
                                    ErrorKind::Other,
                                    "remote write finished before all bytes were sent",
                                )),
                                Err(join) => Err(Error::new(
                                    ErrorKind::Other,
                                    format!("remote write task failed: {join}"),
                                )),
                            };
                        }
                        return Err(Error::new(
                            ErrorKind::Other,
                            "remote write stream closed unexpectedly",
                        ));
                    }
                    offset = end;
                }
                *written += len;
                Ok(())
            }
            RemoteFileState::Read { .. } => Err(Error::unsupported(format!(
                "{} is open for reading",
                self.path
            ))),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        self.ensure_open()?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        match &mut self.state {
            RemoteFileState::Read {
                stream,
                stream_chunk,
                ..
            } => {
                *stream = None;
                *stream_chunk = Bytes::new();
            }
            RemoteFileState::Write {
                tx,
                task,
                size,
                stat,
                ..
            } => {
                drop(tx.take());
                if let Some(handle) = task.take() {
                    let resp = handle.await.map_err(|join| {
                        Error::new(
                            ErrorKind::Other,
                            format!("remote write task failed: {join}"),
                        )
                    })??;
                    *size = Some(resp.size);
                    *stat = resp.stat.map(ObjectStat::from);
                }
            }
        }
        self.closed = true;
        Ok(())
    }

    async fn discard(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        match &mut self.state {
            RemoteFileState::Read {
                stream,
                stream_chunk,
                ..
            } => {
                *stream = None;
                *stream_chunk = Bytes::new();
            }
            RemoteFileState::Write { tx, task, .. } => {
                if let Some(handle) = task.take() {
                    handle.abort();
                }
                drop(tx.take());
            }
        }
        self.closed = true;
        Ok(())
    }
}

impl Drop for RemoteFile {
    fn drop(&mut self) {
        if let RemoteFileState::Write { tx, task, .. } = &mut self.state {
            if let Some(handle) = task.take() {
                handle.abort();
            }
            drop(tx.take());
        }
    }
}
