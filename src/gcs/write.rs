//! Uploads: one-shot `pipe_file` writes and the streaming writers behind
//! `open(.., write)`. Extends [`Backend`].
//!
//! | Bucket kind | Mechanism |
//! |-------------|-----------|
//! | flat / hierarchical | `WriteObject` (resumable above the SDK threshold) fed through an mpsc channel from a spawned upload task; nothing is visible until `close` |
//! | zonal | `BidiWriteObject` appendable object: `flush` persists (readers can see the bytes), `close` finalizes only when [`GcsFsBuilder::finalize_on_close`](crate::GcsFsBuilder::finalize_on_close) is set |
//!
//! The appendable-object API is gated by the storage SDK behind the rustc
//! cfg `google_cloud_unstable_storage_bidi` (see `.cargo/config.toml` and the
//! dev guide). Without it zonal writes fail with
//! [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported).

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::StreamExt as _;
#[cfg(google_cloud_unstable_storage_bidi)]
use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
use google_cloud_storage::model::Object;
use google_cloud_storage::streaming_source::StreamingSource;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::debug;
#[cfg(google_cloud_unstable_storage_bidi)]
use tracing::warn;

use crate::error::{classify_storage_error, Error, ErrorKind, Result, StorageError};
use crate::gcs::backend::Backend;
use crate::gcs::layout::BucketKind;
use crate::gcs::path::GcsPath;
use crate::options::{OpenMode, OpenOptions, WriteMode, WriteOptions};

/// Chunks queued between `File::write` and the upload task. The SDK keeps its
/// own resumable buffer; this only smooths producer/consumer jitter.
const CHANNEL_DEPTH: usize = 4;

/// Default file size threshold in bytes to trigger parallel composite uploads (64 MiB).
pub(crate) const DEFAULT_COMPOSITE_THRESHOLD: u64 = 64 * 1024 * 1024;

/// Default target chunk / component size in bytes for parallel composite uploads (64 MiB).
pub(crate) const DEFAULT_COMPOSITE_CHUNK_SIZE: u64 = 64 * 1024 * 1024;

/// Default maximum concurrency for parallel composite upload workers.
pub(crate) const DEFAULT_COMPOSITE_CONCURRENCY: usize = 8;

/// Maximum components allowed in a single GCS `ComposeObject` call.
pub(crate) const MAX_COMPOSITE_COMPONENTS: usize = 32;

/// Buffer size in bytes when streaming chunks from local disk slices into upload streams (2 MiB).
pub(crate) const COMPOSITE_SLICE_STREAM_CHUNK_BYTES: usize = 2 * 1024 * 1024;

/// Represents one slice of a local file to be uploaded as a composite component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ComponentSlice {
    pub(crate) index: usize,
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

/// Calculate balanced component slices for a composite upload.
///
/// Ensures that:
/// 1. The number of components is at least 2 and at most [`MAX_COMPOSITE_COMPONENTS`].
/// 2. Slices are balanced as evenly as possible to avoid stragglers.
/// 3. Slices partition the exact range `[0, file_size)` without gaps or overlaps.
pub(crate) fn calculate_component_slices(file_size: u64, chunk_size: u64) -> Vec<ComponentSlice> {
    if file_size == 0 {
        return Vec::new();
    }
    let chunk_size = chunk_size.max(1);
    let target_parts = (file_size.div_ceil(chunk_size) as usize).clamp(2, MAX_COMPOSITE_COMPONENTS);

    let base_part_size = file_size / target_parts as u64;
    let remainder = file_size % target_parts as u64;

    let mut slices = Vec::with_capacity(target_parts);
    let mut offset = 0;
    for index in 0..target_parts {
        let length = base_part_size + if (index as u64) < remainder { 1 } else { 0 };
        slices.push(ComponentSlice {
            index,
            offset,
            length,
        });
        offset += length;
    }
    slices
}

#[cfg(unix)]
fn read_exact_at_offset(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at_offset(file: &std::fs::File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let read = file.seek_read(buf, offset)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        let rest = &mut buf[read..];
        buf = rest;
        offset += read as u64;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at_offset(_file: &std::fs::File, _buf: &mut [u8], _offset: u64) -> std::io::Result<()> {
    compile_error!("unsupported platform for positional reads");
}

/// A [`StreamingSource`] that streams a bounded byte range directly from a local file
/// without buffering the whole slice in memory.
pub(crate) struct FileSliceSource {
    file: Arc<std::fs::File>,
    slice: ComponentSlice,
    offset: u64,
    remaining: u64,
    chunk_size: usize,
}

impl FileSliceSource {
    pub(crate) fn new(file: Arc<std::fs::File>, slice: ComponentSlice) -> Self {
        Self {
            file,
            offset: slice.offset,
            remaining: slice.length,
            chunk_size: COMPOSITE_SLICE_STREAM_CHUNK_BYTES,
            slice,
        }
    }
}

impl google_cloud_storage::streaming_source::Seek for FileSliceSource {
    type Error = std::io::Error;

    async fn seek(&mut self, offset: u64) -> std::result::Result<(), Self::Error> {
        if offset > self.slice.length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("seek offset {offset} exceeds slice length {}", self.slice.length),
            ));
        }
        self.offset = self.slice.offset + offset;
        self.remaining = self.slice.length - offset;
        Ok(())
    }
}

impl StreamingSource for FileSliceSource {
    type Error = std::io::Error;

    async fn next(&mut self) -> Option<std::result::Result<Bytes, Self::Error>> {
        if self.remaining == 0 {
            return None;
        }
        let to_read = (self.remaining as usize).min(self.chunk_size);
        let file = Arc::clone(&self.file);
        let offset = self.offset;

        let res = tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; to_read];
            read_exact_at_offset(&file, &mut buf, offset)?;
            Ok::<Bytes, std::io::Error>(Bytes::from(buf))
        })
        .await;

        match res {
            Ok(Ok(bytes)) => {
                let len = bytes.len() as u64;
                self.offset += len;
                self.remaining -= len;
                Some(Ok(bytes))
            }
            Ok(Err(e)) => Some(Err(e)),
            Err(join_err) => Some(Err(std::io::Error::other(format!(
                "disk read join error: {join_err}"
            )))),
        }
    }
}

/// Tracks uploaded temporary component objects and ensures parallel deletion
/// upon upload failure, cancellation, or abort.
pub(crate) struct CompositeUploadGuard {
    backend: Option<Backend>,
    parts: Vec<GcsPath>,
    disarmed: bool,
}

impl CompositeUploadGuard {
    pub(crate) fn new(backend: Backend) -> Self {
        Self {
            backend: Some(backend),
            parts: Vec::new(),
            disarmed: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_dummy() -> Self {
        Self {
            backend: None,
            parts: Vec::new(),
            disarmed: false,
        }
    }

    pub(crate) fn push(&mut self, path: GcsPath) {
        self.parts.push(path);
    }

    #[cfg(test)]
    pub(crate) fn parts(&self) -> &[GcsPath] {
        &self.parts
    }

    #[cfg(test)]
    pub(crate) fn is_disarmed(&self) -> bool {
        self.disarmed
    }

    pub(crate) fn disarm(&mut self) {
        self.disarmed = true;
        self.parts.clear();
    }

    pub(crate) async fn cleanup(&mut self) {
        if self.disarmed || self.parts.is_empty() {
            return;
        }
        let parts = std::mem::take(&mut self.parts);
        if let Some(backend) = &self.backend {
            futures_util::future::join_all(parts.iter().map(|p| backend.delete_object(p))).await;
        }
    }
}

impl Drop for CompositeUploadGuard {
    fn drop(&mut self) {
        if !self.disarmed && !self.parts.is_empty() {
            if let Some(backend) = self.backend.clone() {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let parts = std::mem::take(&mut self.parts);
                    handle.spawn(async move {
                        futures_util::future::join_all(parts.iter().map(|p| backend.delete_object(p))).await;
                    });
                }
            }
        }
    }
}

/// Error for zonal writes when the SDK's appendable-object API is compiled out.
#[cfg(not(google_cloud_unstable_storage_bidi))]
const ZONAL_WRITE_UNAVAILABLE: &str = "writing to zonal buckets needs the storage SDK's \
     appendable-object API, which is only compiled in with \
     RUSTFLAGS=\"--cfg google_cloud_unstable_storage_bidi\" (see docs/dev_guide.md)";

/// What an upload needs to know besides the bytes.
#[derive(Clone, Debug, Default)]
pub(crate) struct UploadSpec {
    /// Fail with `AlreadyExists` if the object exists (`if_generation_match = 0`).
    pub(crate) create_only: bool,
    pub(crate) content_type: Option<String>,
    pub(crate) metadata: HashMap<String, String>,
}

impl From<&WriteOptions> for UploadSpec {
    fn from(opts: &WriteOptions) -> Self {
        Self {
            create_only: opts.mode == WriteMode::Create,
            content_type: opts.content_type.clone(),
            metadata: opts.metadata.clone(),
        }
    }
}

impl From<&OpenOptions> for UploadSpec {
    fn from(opts: &OpenOptions) -> Self {
        Self {
            create_only: opts.mode == OpenMode::CreateNew,
            content_type: opts.content_type.clone(),
            metadata: opts.metadata.clone(),
        }
    }
}

/// What `close` reports back.
#[derive(Debug)]
pub(crate) struct Finished {
    /// The published object; `None` for a zonal object left unfinalized.
    pub(crate) object: Option<Object>,
    /// Bytes persisted.
    pub(crate) size: u64,
}

/// Classify an upload failure. The only precondition a create-only upload
/// sends is `if_generation_match = 0`, so when the service reports a failed
/// precondition (`FAILED_PRECONDITION` / HTTP 412) the object already exists.
fn write_error(path: &GcsPath, create_only: bool, source: StorageError) -> Error {
    if create_only && classify_storage_error(&source) == ErrorKind::PreconditionFailed {
        return Error::with_source(
            ErrorKind::AlreadyExists,
            format!("{path} already exists"),
            source,
        );
    }
    Error::storage("write", path, source)
}

/// Apply an [`UploadSpec`] to any of the SDK's write builders (they share
/// method names but not a trait).
macro_rules! apply_spec {
    ($request:expr, $spec:expr) => {{
        let mut request = $request;
        if $spec.create_only {
            request = request.set_if_generation_match(0);
        }
        if let Some(content_type) = &$spec.content_type {
            request = request.set_content_type(content_type);
        }
        if !$spec.metadata.is_empty() {
            request = request.set_metadata($spec.metadata.clone());
        }
        request
    }};
}

impl Backend {
    /// Upload `data` as the whole object in one call.
    pub(crate) async fn upload_bytes(
        &self,
        path: &GcsPath,
        data: Bytes,
        spec: &UploadSpec,
        kind: BucketKind,
    ) -> Result<Object> {
        debug!(%path, len = data.len(), %kind, "upload");
        if kind.objects_appendable() {
            let mut writer = self.open_appendable(path, spec).await?;
            writer.write(data).await?;
            let finished = writer.close().await?;
            return match finished.object {
                Some(object) => Ok(object),
                // Left unfinalized: report what the service knows now.
                None => self.get_object(path).await,
            };
        }
        let request = apply_spec!(
            self.storage()
                .write_object(path.bucket_resource(), path.object(), data),
            spec
        );
        request
            .send_unbuffered()
            .await
            .map_err(|e| write_error(path, spec.create_only, e))
    }

    /// Upload a local file (non-zonal buckets only). The source is seekable,
    /// so the SDK can resume after a failure without buffering in memory.
    pub(crate) async fn upload_file(
        &self,
        path: &GcsPath,
        file: tokio::fs::File,
        spec: &UploadSpec,
    ) -> Result<Object> {
        debug!(%path, "upload local file");
        let request = apply_spec!(
            self.storage()
                .write_object(path.bucket_resource(), path.object(), file),
            spec
        );
        request
            .send_unbuffered()
            .await
            .map_err(|e| write_error(path, spec.create_only, e))
    }

    /// Upload a local file using parallel composite uploads: slices the file into
    /// balanced components, uploads them concurrently using zero-copy positional
    /// streaming, and composes them into the destination object server-side.
    pub(crate) async fn upload_composite_file(
        &self,
        dst: &GcsPath,
        file_path: &Path,
        file_size: u64,
        spec: &UploadSpec,
        opts: &WriteOptions,
    ) -> Result<Object> {
        let chunk_size = opts
            .chunk_size
            .unwrap_or(DEFAULT_COMPOSITE_CHUNK_SIZE as usize) as u64;
        let concurrency = opts
            .concurrency
            .unwrap_or(DEFAULT_COMPOSITE_CONCURRENCY)
            .max(1);

        let slices = calculate_component_slices(file_size, chunk_size);
        if slices.len() <= 1 {
            let file = tokio::fs::File::open(file_path)
                .await
                .map_err(|e| Error::io("open_file_for_upload", file_path.display().to_string(), e))?;
            return self.upload_file(dst, file, spec).await;
        }

        let file = std::fs::File::open(file_path)
            .map_err(|e| Error::io("open_file_for_upload", file_path.display().to_string(), e))?;
        let file = Arc::new(file);

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let pid = std::process::id();
        let num_slices = slices.len();

        let slices_with_paths: Vec<(ComponentSlice, GcsPath)> = slices
            .into_iter()
            .map(|s| {
                let part_name = format!("{}.gstmp.{}.{}.{}", dst.object(), pid, nanos, s.index);
                let part_path = GcsPath::new(dst.bucket(), part_name)?;
                Ok((s, part_path))
            })
            .collect::<Result<Vec<_>>>()?;

        let guard = Arc::new(Mutex::new(CompositeUploadGuard::new(self.clone())));

        let mut stream = tokio_stream::iter(slices_with_paths)
            .map(|(slice, part_path)| {
                let backend = self.clone();
                let file = Arc::clone(&file);
                let guard = Arc::clone(&guard);
                async move {
                    let part_source = FileSliceSource::new(file, slice);
                    let req = backend
                        .storage()
                        .write_object(part_path.bucket_resource(), part_path.object(), part_source);
                    let obj = req
                        .send_unbuffered()
                        .await
                        .map_err(|e| write_error(&part_path, false, e))?;
                    guard.lock().await.push(part_path);
                    Ok::<_, Error>((slice.index, obj))
                }
            })
            .buffer_unordered(concurrency);

        let mut component_objects: Vec<Option<Object>> = vec![None; num_slices];
        while let Some(res) = stream.next().await {
            match res {
                Ok((idx, obj)) => {
                    component_objects[idx] = Some(obj);
                }
                Err(e) => {
                    guard.lock().await.cleanup().await;
                    return Err(e);
                }
            }
        }

        let sources: Vec<Object> = component_objects
            .into_iter()
            .enumerate()
            .map(|(i, opt)| {
                opt.ok_or_else(|| {
                    Error::new(ErrorKind::Other, format!("part {i} was not uploaded"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let compose_res = self.compose_objects(&sources, dst, spec).await;
        match compose_res {
            Ok(obj) => {
                guard.lock().await.disarm();
                Ok(obj)
            }
            Err(e) => {
                guard.lock().await.cleanup().await;
                Err(e)
            }
        }
    }

    /// Start a streaming upload for `open(.., write)`.
    pub(crate) async fn start_upload(
        &self,
        path: &GcsPath,
        spec: &UploadSpec,
        kind: BucketKind,
    ) -> Result<Writer> {
        debug!(%path, %kind, create_only = spec.create_only, "start upload");
        if kind.objects_appendable() {
            return self.open_appendable(path, spec).await;
        }
        let (tx, rx) = mpsc::channel::<Bytes>(CHANNEL_DEPTH);
        let request = apply_spec!(
            self.storage()
                .write_object(path.bucket_resource(), path.object(), ChannelSource(rx)),
            spec
        );
        let (upload_path, create_only) = (path.clone(), spec.create_only);
        let task = tokio::spawn(async move {
            request
                .send_buffered()
                .await
                .map_err(|e| write_error(&upload_path, create_only, e))
        });
        Ok(Writer::Resumable(ResumableWriter {
            tx: Some(tx),
            task,
            written: 0,
        }))
    }

    /// Open a new appendable object (zonal buckets).
    #[cfg(google_cloud_unstable_storage_bidi)]
    async fn open_appendable(&self, path: &GcsPath, spec: &UploadSpec) -> Result<Writer> {
        let request = apply_spec!(
            self.storage()
                .open_appendable_object(path.bucket_resource(), path.object()),
            spec
        );
        let inner = request
            .send()
            .await
            .map_err(|e| write_error(path, spec.create_only, e))?;
        Ok(Writer::Appendable(AppendableWriter {
            path: path.clone(),
            inner,
            finalize: self.finalize_on_close(),
            written: 0,
        }))
    }

    /// Reopen an existing appendable object (zonal buckets) at the end of
    /// its persisted bytes.
    #[cfg(google_cloud_unstable_storage_bidi)]
    pub(crate) async fn reopen_append(&self, path: &GcsPath, generation: i64) -> Result<Writer> {
        debug!(%path, generation, "reopen appendable");
        let inner = self
            .storage()
            .reopen_appendable_object(path.bucket_resource(), path.object(), generation)
            .send()
            .await
            .map_err(|e| Error::storage("append", path, e))?;
        let persisted = u64::try_from(inner.persisted_size()).unwrap_or(0);
        Ok(Writer::Appendable(AppendableWriter {
            path: path.clone(),
            inner,
            finalize: self.finalize_on_close(),
            written: persisted,
        }))
    }

    #[cfg(not(google_cloud_unstable_storage_bidi))]
    async fn open_appendable(&self, path: &GcsPath, _spec: &UploadSpec) -> Result<Writer> {
        Err(Error::unsupported(format!(
            "{path}: {ZONAL_WRITE_UNAVAILABLE}"
        )))
    }

    #[cfg(not(google_cloud_unstable_storage_bidi))]
    pub(crate) async fn reopen_append(&self, path: &GcsPath, _generation: i64) -> Result<Writer> {
        Err(Error::unsupported(format!(
            "{path}: {ZONAL_WRITE_UNAVAILABLE}"
        )))
    }
}

/// A streaming upload in progress.
#[derive(Debug)]
pub(crate) enum Writer {
    Resumable(ResumableWriter),
    #[cfg(google_cloud_unstable_storage_bidi)]
    Appendable(AppendableWriter),
}

impl Writer {
    /// Bytes accepted so far (for appends: including what existed before).
    pub(crate) fn written(&self) -> u64 {
        match self {
            Writer::Resumable(w) => w.written,
            #[cfg(google_cloud_unstable_storage_bidi)]
            Writer::Appendable(w) => w.written,
        }
    }

    pub(crate) async fn write(&mut self, data: Bytes) -> Result<()> {
        match self {
            Writer::Resumable(w) => w.write(data).await,
            #[cfg(google_cloud_unstable_storage_bidi)]
            Writer::Appendable(w) => w.write(data).await,
        }
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        match self {
            // Chunks are already handed to the upload task as they arrive.
            Writer::Resumable(_) => Ok(()),
            #[cfg(google_cloud_unstable_storage_bidi)]
            Writer::Appendable(w) => w.flush().await,
        }
    }

    pub(crate) async fn close(self) -> Result<Finished> {
        match self {
            Writer::Resumable(w) => w.close().await,
            #[cfg(google_cloud_unstable_storage_bidi)]
            Writer::Appendable(w) => w.close().await,
        }
    }

    /// Abandon the upload (from `File::discard`, or `Drop` as the safety
    /// net). A resumable upload is aborted and publishes nothing. An
    /// appendable object exists from the moment it was opened and bytes
    /// already persisted by `flush` stay (as in gcsfs, which only warns).
    pub(crate) fn discard(self) {
        match self {
            Writer::Resumable(w) => w.task.abort(),
            #[cfg(google_cloud_unstable_storage_bidi)]
            Writer::Appendable(w) => warn!(
                path = %w.path,
                "discarding an appendable object: it stays with the bytes persisted so far"
            ),
        }
    }
}

/// Feeds the SDK's `WriteObject` from an mpsc channel.
struct ChannelSource(mpsc::Receiver<Bytes>);

impl StreamingSource for ChannelSource {
    type Error = Infallible;

    async fn next(&mut self) -> Option<std::result::Result<Bytes, Infallible>> {
        self.0.recv().await.map(Ok)
    }
}

/// Resumable upload: bytes go through a bounded channel to a task that runs
/// the SDK upload; dropping the sender ends the stream and `close` collects
/// the resulting object.
#[derive(Debug)]
pub(crate) struct ResumableWriter {
    tx: Option<mpsc::Sender<Bytes>>,
    task: JoinHandle<Result<Object>>,
    written: u64,
}

impl ResumableWriter {
    async fn write(&mut self, data: Bytes) -> Result<()> {
        let len = data.len() as u64;
        let Some(tx) = &self.tx else {
            return Err(Error::new(ErrorKind::Closed, "upload already closed"));
        };
        if tx.send(data).await.is_err() {
            // The upload task is gone: it failed before consuming everything.
            self.tx = None;
            return match (&mut self.task).await {
                Ok(Ok(_)) => Err(Error::new(
                    ErrorKind::Other,
                    "upload finished before all data was written",
                )),
                Ok(Err(e)) => Err(e),
                Err(join) => Err(Error::new(
                    ErrorKind::Other,
                    format!("upload task failed: {join}"),
                )),
            };
        }
        self.written += len;
        Ok(())
    }

    async fn close(mut self) -> Result<Finished> {
        drop(self.tx.take());
        let object = match (&mut self.task).await {
            Ok(result) => result?,
            Err(join) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("upload task failed: {join}"),
                ))
            }
        };
        Ok(Finished {
            size: u64::try_from(object.size).unwrap_or(self.written),
            object: Some(object),
        })
    }
}

/// Appendable (zonal) object writer.
#[cfg(google_cloud_unstable_storage_bidi)]
#[derive(Debug)]
pub(crate) struct AppendableWriter {
    path: GcsPath,
    inner: AppendableObjectWriter,
    finalize: bool,
    written: u64,
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl AppendableWriter {
    async fn write(&mut self, data: Bytes) -> Result<()> {
        let len = data.len() as u64;
        self.inner
            .append(data)
            .await
            .map_err(|e| Error::storage("write", &self.path, e))?;
        self.written += len;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner
            .flush()
            .await
            .map(|_| ())
            .map_err(|e| Error::storage("flush", &self.path, e))
    }

    async fn close(self) -> Result<Finished> {
        if self.finalize {
            let object = self
                .inner
                .finalize()
                .await
                .map_err(|e| Error::storage("close", &self.path, e))?;
            Ok(Finished {
                size: u64::try_from(object.size).unwrap_or(self.written),
                object: Some(object),
            })
        } else {
            let persisted = self
                .inner
                .close()
                .await
                .map_err(|e| Error::storage("close", &self.path, e))?;
            Ok(Finished {
                object: None,
                size: u64::try_from(persisted).unwrap_or(self.written),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_slices_for_200_mib() {
        let size = 200 * 1024 * 1024;
        let chunk = 64 * 1024 * 1024;
        let slices = calculate_component_slices(size, chunk);
        assert_eq!(slices.len(), 4);
        for s in &slices {
            assert_eq!(s.length, 50 * 1024 * 1024);
        }
        assert_eq!(slices[0].offset, 0);
        assert_eq!(slices[1].offset, 50 * 1024 * 1024);
        assert_eq!(slices[2].offset, 100 * 1024 * 1024);
        assert_eq!(slices[3].offset, 150 * 1024 * 1024);

        let total: u64 = slices.iter().map(|s| s.length).sum();
        assert_eq!(total, size);
    }

    #[test]
    fn component_slices_edge_cases() {
        // Empty file
        assert!(calculate_component_slices(0, 64 * 1024 * 1024).is_empty());

        // File smaller than chunk size is clamped to min 2 parts
        let slices = calculate_component_slices(100, 64);
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].length, 50);
        assert_eq!(slices[1].length, 50);
        assert_eq!(slices[1].offset, 50);

        // Uneven remainder distribution
        let slices = calculate_component_slices(101, 50);
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].length, 34);
        assert_eq!(slices[1].length, 34);
        assert_eq!(slices[2].length, 33);
        let total: u64 = slices.iter().map(|s| s.length).sum();
        assert_eq!(total, 101);

        // Huge file clamped to MAX_COMPOSITE_COMPONENTS
        let huge_size = 5 * 1024 * 1024 * 1024; // 5 GiB
        let slices = calculate_component_slices(huge_size, 64 * 1024 * 1024);
        assert_eq!(slices.len(), MAX_COMPOSITE_COMPONENTS);
        let total: u64 = slices.iter().map(|s| s.length).sum();
        assert_eq!(total, huge_size);
    }

    #[tokio::test]
    async fn file_slice_source_reads_arbitrary_ranges() {
        use std::io::Write;

        let path = std::env::temp_dir().join(format!(
            "slice_test_{}_{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let data: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&data).unwrap();
        }

        let file = Arc::new(std::fs::File::open(&path).unwrap());
        let slice = ComponentSlice {
            index: 0,
            offset: 1_234,
            length: 4_567,
        };
        let mut source = FileSliceSource::new(file, slice);
        // Overwrite chunk size to 1024 to exercise multiple chunk iterations
        source.chunk_size = 1024;

        let mut collected = Vec::new();
        while let Some(chunk) = source.next().await {
            collected.extend_from_slice(&chunk.unwrap());
        }

        let _ = std::fs::remove_file(&path);
        assert_eq!(collected.len(), 4_567);
        assert_eq!(collected, &data[1_234..1_234 + 4_567]);
    }

    #[tokio::test]
    async fn file_slice_source_seek() {
        use google_cloud_storage::streaming_source::Seek as _;
        use std::io::Write;

        let path = std::env::temp_dir().join(format!(
            "slice_seek_test_{}_{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let data: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&data).unwrap();
        }

        let file = Arc::new(std::fs::File::open(&path).unwrap());
        let slice = ComponentSlice {
            index: 0,
            offset: 1_000,
            length: 2_000,
        };
        let mut source = FileSliceSource::new(file, slice);
        source.chunk_size = 500;

        // Read first 500 bytes
        let first = source.next().await.unwrap().unwrap();
        assert_eq!(first.len(), 500);
        assert_eq!(&first[..], &data[1000..1500]);

        // Seek back to 200 (relative to slice)
        source.seek(200).await.unwrap();
        let second = source.next().await.unwrap().unwrap();
        assert_eq!(second.len(), 500);
        assert_eq!(&second[..], &data[1200..1700]);

        // Seek past end errors
        assert!(source.seek(2500).await.is_err());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn composite_upload_guard_lifecycle() {
        let p1 = GcsPath::new("my-bucket", "obj.gstmp.1.1.0").unwrap();
        let p2 = GcsPath::new("my-bucket", "obj.gstmp.1.1.1").unwrap();

        let mut guard = CompositeUploadGuard::new_dummy();
        assert!(!guard.is_disarmed());
        assert!(guard.parts().is_empty());

        guard.push(p1.clone());
        guard.push(p2.clone());
        assert_eq!(guard.parts().len(), 2);
        assert_eq!(guard.parts()[0], p1);
        assert_eq!(guard.parts()[1], p2);

        // Cleanup before disarm clears parts
        guard.cleanup().await;
        assert!(guard.parts().is_empty());

        // Disarm sets flag
        guard.push(p1);
        assert!(!guard.is_disarmed());
        guard.disarm();
        assert!(guard.is_disarmed());
        assert!(guard.parts().is_empty());

        // Cleanup after disarm is a no-op
        guard.cleanup().await;
    }
}
