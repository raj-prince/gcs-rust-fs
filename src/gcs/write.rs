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

use bytes::Bytes;
#[cfg(google_cloud_unstable_storage_bidi)]
use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
use google_cloud_storage::model::Object;
use google_cloud_storage::streaming_source::StreamingSource;
use tokio::sync::mpsc;
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
        if kind == BucketKind::Zonal {
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

    /// Start a streaming upload for `open(.., write)`.
    pub(crate) async fn start_upload(
        &self,
        path: &GcsPath,
        spec: &UploadSpec,
        kind: BucketKind,
    ) -> Result<Writer> {
        debug!(%path, %kind, create_only = spec.create_only, "start upload");
        if kind == BucketKind::Zonal {
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
