//! [`GcsFs`]: the [`FileSystem`] implementation for Cloud Storage.
//!
//! This module holds file-system semantics only — how a path maps to an
//! object, a placeholder, an HNS folder or a bucket, which errors mean what,
//! and how the three bucket kinds differ (see `BucketKind`). The SDK plumbing
//! lives in the sibling modules (`backend`, `control`, `write`).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::stream::{self, StreamExt};
use google_cloud_storage::model::{Bucket, Folder, Object};
use google_cloud_storage::model_ext::ReadRange;
use tracing::{debug, warn};

use crate::derived;
use crate::entry::{self, Entry};
use crate::error::{Error, ErrorKind, Result};
use crate::file::File;
use crate::filesystem::FileSystem;
use crate::gcs::backend::{Backend, BackendOptions, Transport};
use crate::gcs::control::{bucket_id, folder_key, BucketSpec, ListRequest, Listing};
use crate::gcs::file::GcsFile;
use crate::gcs::layout::BucketKind;
use crate::gcs::path::{GcsPath, Loc};
use crate::gcs::write::UploadSpec;
use crate::metrics::{
    ActiveStreamGuard, Direction, MetricsRecorder, NoopMetricsRecorder, Status,
};
use crate::options::{
    CopyOptions, FindOptions, ListOptions, MkdirOptions, OpenMode, OpenOptions, ReadOptions,
    RmOptions, WriteOptions,
};
use crate::range::{ByteRange, ResolvedRange};
use crate::stat::ObjectStat;

/// Default slice / chunk size in bytes for parallel downloads in [`GcsFs::get_file`] (128 MiB).
pub const DEFAULT_GET_CHUNK_SIZE: usize = 128 * 1024 * 1024;

/// Default concurrency (number of in-flight byte-range requests) for [`GcsFs::get_file`].
pub const DEFAULT_GET_CONCURRENCY: usize = 24;

/// Coalescing threshold in bytes before issuing a positional disk write in [`GcsFs::get_file`] (2 MiB).
const PWRITE_COALESCE_BYTES: usize = 2 * 1024 * 1024;

/// Environment variables consulted by [`GcsFsBuilder::from_env`] for the
/// project, in order.
pub const PROJECT_ENV_VARS: [&str; 2] = ["GCS_RUST_FS_PROJECT", "GOOGLE_CLOUD_PROJECT"];

/// Environment variable consulted by [`GcsFsBuilder::from_env`] for the HTTP
/// client pool size.
pub const HTTP_POOL_SIZE_ENV_VAR: &str = "GCS_RUST_FS_HTTP_POOL_SIZE";

/// Builder for [`GcsFs`].
#[derive(Clone, Debug, Default)]
pub struct GcsFsBuilder {
    transport: Option<Transport>,
    endpoint: Option<String>,
    grpc_subchannel_count: Option<usize>,
    http_pool_size: Option<usize>,
    project: Option<String>,
    bucket_kinds: HashMap<String, BucketKind>,
    finalize_on_close: bool,
    metrics: Option<Arc<dyn MetricsRecorder>>,
}

impl GcsFsBuilder {
    /// Create a builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Select the data-read [`Transport`] for non-zonal buckets (default:
    /// [`Transport::Grpc`]). Zonal buckets always use gRPC.
    pub fn transport(mut self, transport: Transport) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Override the service endpoint (e.g. to target a test bench or a
    /// private endpoint).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Number of gRPC subchannels (HTTP/2 connections) used for data reads.
    /// Defaults to the SDK's choice (the available parallelism).
    pub fn grpc_subchannel_count(mut self, count: usize) -> Self {
        self.grpc_subchannel_count = Some(count);
        self
    }

    /// Number of striped HTTP `Storage` client instances used for
    /// [`Transport::Http`] range reads (default: `16` when [`Transport::Http`]
    /// is selected).
    pub fn http_pool_size(mut self, count: usize) -> Self {
        self.http_pool_size = Some(count);
        self
    }

    /// The project that owns the buckets listed by `ls("")` and created by
    /// `mkdir("bucket")`. Not needed for anything else.
    pub fn project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    /// Declare the kind of a bucket up front instead of detecting it with
    /// `GetStorageLayout` on first use (useful for principals that may not
    /// read the layout, and for tests).
    pub fn bucket_kind(mut self, bucket: impl Into<String>, kind: BucketKind) -> Self {
        self.bucket_kinds.insert(bucket.into(), kind);
        self
    }

    /// Whether closing a write handle on a **zonal** bucket finalizes the
    /// appendable object (default `false`, as in gcsfs: the object stays
    /// appendable and can be reopened with [`OpenMode::Append`]). Other
    /// bucket kinds always publish the complete object on close.
    pub fn finalize_on_close(mut self, finalize: bool) -> Self {
        self.finalize_on_close = finalize;
        self
    }

    /// Register a custom metrics recorder for observability, GCP Metrics Explorer,
    /// or internal Google3 telemetry.
    ///
    /// If not set, defaults to [`NoopMetricsRecorder`].
    pub fn with_metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(recorder);
        self
    }

    /// Fill unset options from the environment: the transport from
    /// [`Transport::ENV_VAR`], the HTTP pool size from
    /// [`HTTP_POOL_SIZE_ENV_VAR`], and the project from [`PROJECT_ENV_VARS`].
    /// Explicitly set options take precedence.
    pub fn from_env(mut self) -> Result<Self> {
        if self.transport.is_none() {
            self.transport = Some(Transport::from_env()?);
        }
        if self.http_pool_size.is_none() {
            if let Ok(raw) = std::env::var(HTTP_POOL_SIZE_ENV_VAR) {
                let trimmed = raw.trim();
                if !trimmed.is_empty() {
                    let count = trimmed.parse::<usize>().map_err(|_| {
                        Error::invalid_config(format!(
                            "invalid {HTTP_POOL_SIZE_ENV_VAR} {trimmed:?}: expected positive integer"
                        ))
                    })?;
                    if count == 0 {
                        return Err(Error::invalid_config(format!(
                            "invalid {HTTP_POOL_SIZE_ENV_VAR} {trimmed:?}: must be at least 1"
                        )));
                    }
                    self.http_pool_size = Some(count);
                }
            }
        }
        if self.project.is_none() {
            self.project = PROJECT_ENV_VARS
                .iter()
                .filter_map(|var| std::env::var(var).ok())
                .find(|value| !value.trim().is_empty());
        }
        Ok(self)
    }

    /// Connect to Cloud Storage. Credentials are discovered via
    /// [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
    pub async fn build(self) -> Result<GcsFs> {
        if matches!(self.http_pool_size, Some(0)) {
            return Err(Error::invalid_config(
                "http_pool_size must be at least 1",
            ));
        }
        let metrics = self.metrics.unwrap_or_else(NoopMetricsRecorder::new);
        let backend = Backend::connect(BackendOptions {
            transport: self.transport.unwrap_or_default(),
            endpoint: self.endpoint,
            grpc_subchannel_count: self.grpc_subchannel_count,
            http_pool_size: self.http_pool_size,
            project: self.project,
            bucket_kinds: self.bucket_kinds,
            finalize_on_close: self.finalize_on_close,
            metrics: Some(Arc::clone(&metrics)),
        })
        .await?;
        Ok(GcsFs { backend, metrics })
    }
}

/// Google Cloud Storage as a [`FileSystem`].
///
/// `GcsFs` is cheap to clone and holds connection pools, so create one and
/// share it. All paths are `fsspec` strings (`bucket/key`, optionally
/// `gs://`-prefixed or `#generation`-suffixed); the root `""` is the list of
/// buckets of the configured project.
///
/// Flat, hierarchical-namespace and zonal buckets are handled transparently
/// — see [`BucketKind`] for what differs.
///
/// ```no_run
/// use gcs_rust_fs::{ByteRange, FileSystem, GcsFs, OpenOptions, ReadOptions};
///
/// # async fn demo() -> gcs_rust_fs::Result<()> {
/// let fs = GcsFs::new().await?;
///
/// let info = fs.info("my-bucket/checkpoint.pt").await?;
/// println!("{} bytes", info.size);
///
/// let header = fs
///     .cat_file("my-bucket/checkpoint.pt", ByteRange::head(1024), ReadOptions::default())
///     .await?;
/// assert!(header.len() <= 1024);
///
/// let mut file = fs.open("my-bucket/out.txt", OpenOptions::write()).await?;
/// file.write(b"hello".as_ref().into()).await?;
/// file.close().await?;
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct GcsFs {
    backend: Backend,
    metrics: Arc<dyn MetricsRecorder>,
}

struct PutOutcome {
    bytes: u64,
    composite: bool,
}

impl GcsFs {
    /// Return the configured metrics recorder.
    pub fn metrics(&self) -> &Arc<dyn MetricsRecorder> {
        &self.metrics
    }

    /// Start configuring a file system.
    pub fn builder() -> GcsFsBuilder {
        GcsFsBuilder::new()
    }

    /// Connect with settings from the environment
    /// ([`GcsFsBuilder::from_env`]) and Application Default Credentials.
    pub async fn new() -> Result<Self> {
        Self::builder().from_env()?.build().await
    }

    /// The transport used for data reads from non-zonal buckets.
    pub fn transport(&self) -> Transport {
        self.backend.transport()
    }

    /// The configured project, if any.
    pub fn project(&self) -> Option<&str> {
        self.backend.project_opt()
    }

    /// The kind of `bucket`, detected with `GetStorageLayout` on first use
    /// and cached afterwards.
    pub async fn bucket_kind(&self, bucket: &str) -> Result<BucketKind> {
        let loc = Loc::parse(bucket)?;
        if loc.is_root() {
            return Err(Error::invalid_path(bucket, "expected a bucket name"));
        }
        self.backend.bucket_kind(loc.bucket()).await
    }

    /// Create a bucket with Cloud Storage specific placement — the
    /// counterpart of gcsfs's `mkdir(enable_hierarchical_namespace=…,
    /// placement=…)`. Plain `mkdir("bucket")` creates a flat bucket.
    pub async fn create_bucket(&self, bucket: &str, spec: BucketSpec) -> Result<()> {
        let loc = Loc::parse(bucket)?;
        if !loc.is_bucket() {
            return Err(Error::invalid_path(bucket, "expected a bucket name"));
        }
        self.backend.create_bucket(loc.bucket(), &spec).await?;
        let kind = if spec.zone.is_some() {
            BucketKind::Zonal
        } else if spec.hierarchical {
            BucketKind::Hierarchical
        } else {
            BucketKind::Flat
        };
        self.backend.seed_kind(loc.bucket(), kind);
        Ok(())
    }

    // ---- internal helpers --------------------------------------------------

    /// `GetObject` that reports a missing object as `None` and, like gcsfs,
    /// falls back to an exact-name listing when `GetObject` is forbidden
    /// (list-only IAM roles).
    async fn stat_object(&self, object: &GcsPath) -> Result<Option<ObjectStat>> {
        match self.backend.get_object(object).await {
            Ok(o) => Ok(Some(o.into())),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                match self.backend.find_exact(object).await {
                    Ok(found) => Ok(found.map(Into::into)),
                    Err(_) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// One-page listing below `loc/` to decide whether it is a directory.
    async fn dir_listing(&self, loc: &Loc, include_folders: bool) -> Result<Listing> {
        self.backend
            .list_objects(ListRequest {
                bucket: loc.bucket(),
                prefix: &loc.prefix(),
                delimiter: true,
                versions: false,
                include_folders,
                limit: Some(1),
            })
            .await
    }

    /// Is the nested location `loc` a directory? Returns its entry if so.
    ///
    /// Flat buckets: anything listed below `key/` (a placeholder counts).
    /// Hierarchical buckets: `GetFolder`, with the listing as a fallback when
    /// folder metadata is not readable.
    async fn dir_entry(&self, loc: &Loc, kind: BucketKind) -> Result<Option<Entry>> {
        if kind.is_hierarchical() {
            match self.backend.get_folder(loc.bucket(), loc.key()).await {
                Ok(folder) => return Ok(Some(folder_entry(loc.bucket(), &folder))),
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
                Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                    debug!(path = %loc.path(), "GetFolder forbidden; probing with a listing");
                }
                Err(e) => return Err(e),
            }
        }
        let listing = self.dir_listing(loc, kind.is_hierarchical()).await?;
        if listing.objects.is_empty() && listing.prefixes.is_empty() {
            return Ok(None);
        }
        let mut dir = Entry::directory(loc.path());
        let placeholder = loc.prefix();
        if let Some(object) = listing.objects.into_iter().find(|o| o.name == placeholder) {
            dir = dir.with_stat(object.into());
        }
        Ok(Some(dir))
    }

    /// `info` for a bucket: `GetBucket`, falling back to a listing when the
    /// principal may list but not read bucket metadata (as gcsfs does).
    async fn bucket_entry(&self, loc: &Loc) -> Result<Entry> {
        match self.backend.get_bucket(loc.bucket()).await {
            Ok(bucket) => Ok(bucket_entry(&bucket)),
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                debug!(
                    bucket = loc.bucket(),
                    "GetBucket forbidden; probing with a listing"
                );
                match self.dir_listing(loc, false).await {
                    Ok(_) => Ok(Entry::directory(loc.bucket())),
                    Err(list_err) if list_err.kind() == ErrorKind::NotFound => Err(list_err),
                    Err(_) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Turn a `NotFound` for an object into `IsADirectory` when the same
    /// path is a directory.
    async fn refine_not_found(&self, loc: &Loc, kind: BucketKind, err: Error) -> Error {
        if err.kind() != ErrorKind::NotFound {
            return err;
        }
        match self.dir_entry(loc, kind).await {
            Ok(Some(_)) => Error::is_a_directory(&loc.path()),
            _ => err,
        }
    }

    /// Parse `path` and require it to name an object (not the root or a
    /// bucket), returning the bucket kind as well.
    async fn object_loc(&self, path: &str) -> Result<(Loc, BucketKind, GcsPath)> {
        let loc = Loc::parse(path)?;
        if loc.key().is_empty() {
            return Err(Error::is_a_directory(&loc.path()));
        }
        let kind = self.backend.kind_or_flat(loc.bucket()).await;
        let object = loc.object()?;
        Ok((loc, kind, object))
    }

    /// Delete many objects concurrently; objects that are already gone are
    /// not errors. Returns the first real error after draining the stream.
    async fn delete_all(&self, objects: Vec<GcsPath>, concurrency: usize) -> Result<()> {
        let mut first_error = None;
        let mut deletes = stream::iter(objects)
            .map(|object| async move {
                match self.backend.delete_object(&object).await {
                    Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                    other => other,
                }
            })
            .buffer_unordered(concurrency.max(1));
        while let Some(result) = deletes.next().await {
            if let Err(e) = result {
                first_error.get_or_insert(e);
            }
        }
        drop(deletes);
        first_error.map_or(Ok(()), Err)
    }

    /// Delete HNS folders deepest-first, one depth level at a time with
    /// bounded concurrency inside a level.
    async fn delete_folders(
        &self,
        bucket: &str,
        keys: Vec<String>,
        concurrency: usize,
    ) -> Result<()> {
        let mut by_depth: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        for key in keys {
            by_depth.entry(entry::depth(&key)).or_default().push(key);
        }
        for (_, level) in by_depth.into_iter().rev() {
            let mut first_error = None;
            let mut deletes = stream::iter(level)
                .map(|key| async move {
                    match self.backend.delete_folder(bucket, &key).await {
                        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                        other => other,
                    }
                })
                .buffer_unordered(concurrency.max(1));
            while let Some(result) = deletes.next().await {
                if let Err(e) = result {
                    first_error.get_or_insert(e);
                }
            }
            drop(deletes);
            if let Some(e) = first_error {
                return Err(e);
            }
        }
        Ok(())
    }

    /// Native `rm -r` without a depth limit: one flat listing, concurrent
    /// deletes, then folders (HNS) and finally the bucket itself when `loc`
    /// is a bucket.
    async fn rm_tree(&self, loc: &Loc, kind: BucketKind, concurrency: usize) -> Result<()> {
        let bucket = loc.bucket();
        let prefix = loc.prefix();
        let (listing, folders) = tokio::join!(
            self.backend.list_objects(ListRequest {
                bucket,
                prefix: &prefix,
                delimiter: false,
                versions: false,
                include_folders: false,
                limit: None,
            }),
            self.all_folders(bucket, &prefix, kind),
        );
        let objects = listing?
            .objects
            .into_iter()
            .map(|o| GcsPath::new(bucket, o.name))
            .collect::<Result<Vec<_>>>()?;
        debug!(path = %loc.path(), objects = objects.len(), "rm -r");
        self.delete_all(objects, concurrency).await?;
        let keys = folders?
            .iter()
            .map(|f| folder_key(f).to_owned())
            .collect::<Vec<_>>();
        self.delete_folders(bucket, keys, concurrency).await?;
        if loc.is_bucket() {
            warn!(bucket, "rm -r on a bucket: deleting the bucket itself");
            self.backend.delete_bucket(bucket).await?;
        }
        Ok(())
    }

    /// Every HNS folder below `prefix` (including the folder `prefix` itself);
    /// empty for flat buckets. Missing permission degrades to "no folders"
    /// (directories are then synthesised from object names).
    async fn all_folders(
        &self,
        bucket: &str,
        prefix: &str,
        kind: BucketKind,
    ) -> Result<Vec<Folder>> {
        if !kind.is_hierarchical() {
            return Ok(Vec::new());
        }
        match self.backend.list_folders(bucket, prefix).await {
            Ok(folders) => Ok(folders),
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                warn!(bucket, prefix, error = %e, "cannot list folders; empty folders will be missed");
                Ok(Vec::new())
            }
            Err(e) => Err(e),
        }
    }

    /// `move_file` inside one bucket via the atomic `MoveObject` RPC, with
    /// gcsfs's copy + delete fallback for buckets where it is not available.
    async fn move_within_bucket(
        &self,
        src: &GcsPath,
        dst: &GcsPath,
        kind: BucketKind,
    ) -> Result<()> {
        match self.backend.move_object(src, dst.object()).await {
            Ok(_) => Ok(()),
            Err(e) if !kind.supports_server_copy() => Err(e),
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::NotFound
                        | ErrorKind::PermissionDenied
                        | ErrorKind::Unauthenticated
                        | ErrorKind::AlreadyExists
                ) =>
            {
                Err(e)
            }
            Err(e) => {
                warn!(%src, %dst, error = %e, "MoveObject failed; falling back to copy + delete");
                self.backend.rewrite_object(src, dst).await?;
                self.backend.delete_object(src).await
            }
        }
    }

    async fn get_file_impl(&self, path: &str, local: &Path, opts: ReadOptions) -> Result<u64> {
        let ReadOptions {
            chunk_size,
            concurrency,
        } = opts;
        if matches!(chunk_size, Some(0)) {
            return Err(Error::invalid_config("chunk_size must be at least 1"));
        }
        if matches!(concurrency, Some(0)) {
            return Err(Error::invalid_config("concurrency must be at least 1"));
        }
        let chunk_size = chunk_size.unwrap_or(DEFAULT_GET_CHUNK_SIZE) as u64;
        let concurrency = concurrency.unwrap_or(DEFAULT_GET_CONCURRENCY);

        let is_null = is_dev_null(local);
        if !is_null {
            if local.as_os_str().is_empty() || local.file_name().is_none() {
                return Err(Error::invalid_path(
                    &local.display().to_string(),
                    "target path must not be empty",
                ));
            }
            if local.to_str().is_some_and(|s| s.ends_with(std::path::is_separator))
                || tokio::fs::metadata(local).await.is_ok_and(|m| m.is_dir())
            {
                return Err(Error::is_a_directory(&local.display().to_string()));
            }
        }

        let (loc, kind, object) = self.object_loc(path).await?;
        debug!(path = %loc.path(), local = %local.display(), "get_file");
        let (meta, handle) = match self.backend.open(&object, kind).await {
            Ok(opened) => opened,
            Err(e) => return Err(self.refine_not_found(&loc, kind, e).await),
        };
        let file_size = u64::try_from(meta.size).unwrap_or(0);
        let pinned = object.with_generation(Some(meta.generation));

        let (target_file, tmp_path, mut guard) = if is_null {
            let target_path = local.to_path_buf();
            let target_file = tokio::task::spawn_blocking(move || -> std::io::Result<Arc<std::fs::File>> {
                let f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&target_path)?;
                Ok(Arc::new(f))
            })
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("join error: {e}")))?
            .map_err(|e| Error::io("open", local.display(), e))?;
            (target_file, local.to_path_buf(), TempFileGuard::new(None))
        } else {
            if let Some(parent) = local.parent().filter(|p| !p.as_os_str().is_empty()) {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| Error::io("mkdir", parent.display(), e))?;
            }

            let tmp_path = staging_path(local);
            let guard = TempFileGuard::new(Some(tmp_path.clone()));
            let open_tmp = tmp_path.clone();
            let target_file = tokio::task::spawn_blocking(move || -> std::io::Result<Arc<std::fs::File>> {
                let f = std::fs::OpenOptions::new()
                    .read(false)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&open_tmp)?;
                f.set_len(file_size)?;
                Ok(Arc::new(f))
            })
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("join error: {e}")))?
            .map_err(|e| Error::io("create", tmp_path.display(), e))?;
            (target_file, tmp_path, guard)
        };

        if file_size > 0 {
            let num_slices = file_size.div_ceil(chunk_size);
            let slices: Vec<(u64, u64)> = (0..num_slices)
                .map(|i| {
                    let start = i * chunk_size;
                    let len = (file_size - start).min(chunk_size);
                    (start, len)
                })
                .collect();

            let loc_path = loc.path().to_string();
            let metrics = Arc::clone(&self.metrics);
            let mut stream = stream::iter(slices)
                .map(|(start, len)| {
                    let handle = handle.clone();
                    let pinned = pinned.clone();
                    let target_file = Arc::clone(&target_file);
                    let loc_path = loc_path.clone();
                    let tmp_display = tmp_path.display().to_string();
                    let metrics = Arc::clone(&metrics);
                    async move {
                        let _guard = ActiveStreamGuard::new(Arc::clone(&metrics));
                        let slice_start = Instant::now();
                        let slice_res = async {
                            let mut reader = handle.read_range(ReadRange::segment(start, len)).await?;
                            let mut written_in_slice: u64 = 0;
                            let mut buf = BytesMut::with_capacity(PWRITE_COALESCE_BYTES);

                            while let Some(chunk) = reader.next_chunk(&pinned).await? {
                                if buf.len() + chunk.len() > PWRITE_COALESCE_BYTES && !buf.is_empty() {
                                    let offset = start + written_in_slice;
                                    let to_write = buf.split().freeze();
                                    let len_written = to_write.len() as u64;
                                    let target_file = Arc::clone(&target_file);
                                    let tmp_display = tmp_display.clone();
                                    tokio::task::spawn_blocking(move || {
                                        write_all_at_offset(&target_file, &to_write, offset)
                                    })
                                    .await
                                    .map_err(|e| Error::new(ErrorKind::Other, format!("join error: {e}")))?
                                    .map_err(|e| Error::io("write", tmp_display, e))?;
                                    written_in_slice += len_written;
                                }
                                buf.extend_from_slice(&chunk);
                            }

                            if !buf.is_empty() {
                                let offset = start + written_in_slice;
                                let to_write = buf.split().freeze();
                                let len_written = to_write.len() as u64;
                                let target_file = Arc::clone(&target_file);
                                let tmp_display = tmp_display.clone();
                                tokio::task::spawn_blocking(move || {
                                    write_all_at_offset(&target_file, &to_write, offset)
                                })
                                .await
                                .map_err(|e| Error::new(ErrorKind::Other, format!("join error: {e}")))?
                                .map_err(|e| Error::io("write", tmp_display, e))?;
                                written_in_slice += len_written;
                            }

                            if written_in_slice != len {
                                return Err(Error::new(
                                    ErrorKind::Other,
                                    format!(
                                        "short read in {loc_path}: expected {len} bytes at offset {start}, got {written_in_slice}"
                                    ),
                                ));
                            }
                            Ok::<(), Error>(())
                        }
                        .await;

                        match &slice_res {
                            Ok(()) => {
                                metrics.record_slice_download(len, slice_start.elapsed(), Status::Ok);
                            }
                            Err(_) => {
                                metrics.record_slice_download(len, slice_start.elapsed(), Status::Error);
                            }
                        }
                        slice_res
                    }
                })
                .buffer_unordered(concurrency);

            while let Some(result) = stream.next().await {
                result?;
            }
        }

        drop(target_file);
        if !is_null {
            tokio::fs::rename(&tmp_path, local)
                .await
                .map_err(|e| Error::io("rename", local.display(), e))?;
            guard.disarm();
        }
        Ok(file_size)
    }

    async fn put_file_impl(&self, local: &Path, path: &str, opts: WriteOptions) -> Result<PutOutcome> {
        if matches!(opts.block_size, Some(0)) {
            return Err(Error::invalid_config("block_size must be at least 1"));
        }
        let metadata = tokio::fs::metadata(local)
            .await
            .map_err(|e| Error::io("stat", local.display(), e))?;
        let file_size = metadata.len();
        let (loc, kind, object) = self.object_loc(path).await?;
        if kind.objects_appendable() {
            // Appendable objects take the streaming path.
            derived::put_file(self, local, path, opts).await?;
            return Ok(PutOutcome {
                bytes: file_size,
                composite: false,
            });
        }
        debug!(path = %loc.path(), local = %local.display(), "put_file");
        let spec = UploadSpec::from(&opts);

        let composite_threshold = opts
            .composite_threshold
            .unwrap_or(crate::gcs::write::DEFAULT_COMPOSITE_THRESHOLD as usize) as u64;

        if file_size >= composite_threshold && kind.supports_server_copy() {
            self.backend
                .upload_composite_file(&object, local, file_size, &spec, &opts)
                .await
                .map(|_| PutOutcome {
                    bytes: file_size,
                    composite: true,
                })
        } else {
            let file = tokio::fs::File::open(local)
                .await
                .map_err(|e| Error::io("open", local.display(), e))?;
            self.backend
                .upload_file(&object, file, &spec)
                .await
                .map(|_| PutOutcome {
                    bytes: file_size,
                    composite: false,
                })
        }
    }

    async fn cat_file_impl(&self, path: &str, range: ByteRange, opts: ReadOptions) -> Result<Bytes> {
        let ReadOptions {
            chunk_size: _,
            concurrency: _,
        } = opts;
        let (loc, kind, object) = self.object_loc(path).await?;
        debug!(path = %loc.path(), ?range, "cat_file");
        let (object, resolved) = if range.needs_size() {
            let meta = match self.backend.get_object(&object).await {
                Ok(meta) => meta,
                Err(e) => return Err(self.refine_not_found(&loc, kind, e).await),
            };
            let size = u64::try_from(meta.size).unwrap_or(0);
            let generation = object.generation().or(Some(meta.generation));
            (
                object.with_generation(generation),
                range.resolve(Some(size))?,
            )
        } else {
            (object, range.resolve(None)?)
        };
        let ResolvedRange::Read { range, len_hint } = resolved else {
            return Ok(Bytes::new());
        };
        match self.backend.read_range(&object, range, kind).await {
            Ok(reader) => reader.collect(len_hint, &object).await,
            // A start at or past the end is an empty slice, not an error.
            Err(e) if e.kind() == ErrorKind::OutOfRange => Ok(Bytes::new()),
            Err(e) => Err(self.refine_not_found(&loc, kind, e).await),
        }
    }
}

/// A file entry for an object; with `versions` the path carries `#generation`.
fn object_entry(object: Object, versions: bool) -> Entry {
    let stat = ObjectStat::from(object);
    let path = if versions {
        format!("{}#{}", stat.path(), stat.generation)
    } else {
        stat.path()
    };
    Entry::file(path, stat)
}

/// A directory entry for an HNS folder, carrying its times and metageneration.
fn folder_entry(bucket: &str, folder: &Folder) -> Entry {
    let key = folder_key(folder);
    let stat = ObjectStat {
        bucket: bucket.to_owned(),
        name: format!("{key}/"),
        metageneration: folder.metageneration,
        time_created: folder.create_time.map(String::from),
        updated: folder.update_time.map(String::from),
        ..Default::default()
    };
    Entry::directory(format!("{bucket}/{key}")).with_stat(stat)
}

/// A directory entry for a bucket.
fn bucket_entry(bucket: &Bucket) -> Entry {
    let id = bucket_id(bucket);
    let stat = ObjectStat {
        bucket: id.to_owned(),
        metageneration: bucket.metageneration,
        storage_class: bucket.storage_class.clone(),
        time_created: bucket.create_time.map(String::from),
        updated: bucket.update_time.map(String::from),
        ..Default::default()
    };
    Entry::directory(id).with_stat(stat)
}

/// Whether `object` is a zero-byte `dir/` placeholder.
fn is_placeholder(object: &Object) -> bool {
    object.size == 0 && object.name.ends_with('/')
}

/// Every directory key implied by object `name` strictly below `prefix`
/// (`prefix = "a/"`, `name = "a/b/c/d"` → `a/b`, `a/b/c`). For a placeholder
/// `a/b/` the last item is `a/b` itself.
fn implied_dirs<'a>(prefix: &str, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
    let start = prefix.len();
    name.match_indices('/')
        .map(|(i, _)| i)
        .filter(move |&i| i >= start)
        .map(move |i| &name[..i])
        .filter(move |dir| dir.len() > start.saturating_sub(1) && !dir.ends_with('/'))
}

fn invalid_maxdepth() -> Error {
    Error::new(ErrorKind::InvalidRange, "maxdepth must be at least 1")
}

#[async_trait]
impl FileSystem for GcsFs {
    // =====================================================================
    // Required primitives
    // =====================================================================

    async fn info(&self, path: &str) -> Result<Entry> {
        let loc = Loc::parse(path)?;
        debug!(path = %loc.path(), "info");
        if loc.is_root() {
            return Ok(Entry::directory(""));
        }
        if loc.is_bucket() {
            return self.bucket_entry(&loc).await;
        }
        let kind = self.backend.kind_or_flat(loc.bucket()).await;
        let object = loc.object()?;

        // Fast path: probe the object first (Class B GetObject). In real-world
        // workloads >95% of lookups target files. Probing sequentially avoids
        // unconditionally burning expensive Class A ListObjects QPS / billing.
        if let Some(stat) = self.stat_object(&object).await? {
            // If the object has size 0 and its name ends with a slash (or looks
            // like a placeholder), it could be a directory marker, but standard
            // files return immediately.
            return Ok(Entry::file(stat.path(), stat));
        }

        // Object was not found: probe for a directory if no generation was specified.
        if loc.generation().is_none() {
            if let Some(dir) = self.dir_entry(&loc, kind).await? {
                return Ok(dir);
            }
        }

        Err(Error::not_found(&loc.path()))
    }

    async fn ls(&self, path: &str, opts: ListOptions) -> Result<Vec<Entry>> {
        let loc = Loc::parse(path)?;
        debug!(path = %loc.path(), versions = opts.versions, "ls");
        if loc.is_root() {
            let mut out: Vec<Entry> = self
                .backend
                .list_buckets()
                .await?
                .iter()
                .map(bucket_entry)
                .collect();
            out.sort_by(|a, b| a.path.cmp(&b.path));
            return Ok(out);
        }
        let kind = self.backend.kind_or_flat(loc.bucket()).await;
        let prefix = loc.prefix();
        let listing = self
            .backend
            .list_objects(ListRequest {
                bucket: loc.bucket(),
                prefix: &prefix,
                delimiter: true,
                versions: opts.versions && kind.has_versioning(),
                include_folders: kind.is_hierarchical(),
                limit: None,
            })
            .await?;
        let mut out = Vec::with_capacity(listing.objects.len() + listing.prefixes.len());
        let mut own_placeholder = false;
        for object in listing.objects {
            // With a `/` delimiter the only `x/` object that can appear is the
            // listed directory's own placeholder; it is not a child.
            if !loc.is_bucket() && object.name == prefix {
                own_placeholder = true;
                continue;
            }
            out.push(object_entry(object, opts.versions));
        }
        for dir in listing.prefixes {
            out.push(Entry::directory(format!(
                "{}/{}",
                loc.bucket(),
                dir.trim_end_matches('/')
            )));
        }
        if out.is_empty() && !loc.is_bucket() && !own_placeholder {
            // Nothing below: `path` is a file (listed as itself), an empty
            // directory (HNS folder) or missing.
            let entry = self.info(&loc.path()).await?;
            return Ok(if entry.is_file() {
                vec![entry]
            } else {
                Vec::new()
            });
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out.dedup_by(|a, b| a.path == b.path);
        Ok(out)
    }

    async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>> {
        let (loc, kind, object) = self.object_loc(path).await?;
        debug!(path = %loc.path(), mode = opts.mode.as_str(), %kind, "open");
        match opts.mode {
            OpenMode::Read => {
                let (meta, handle) = match self.backend.open(&object, kind).await {
                    Ok(opened) => opened,
                    Err(e) => return Err(self.refine_not_found(&loc, kind, e).await),
                };
                let pinned = object.with_generation(Some(meta.generation));
                Ok(Box::new(GcsFile::reader(
                    loc.path(),
                    pinned,
                    meta.into(),
                    handle,
                    opts.block_size,
                )))
            }
            OpenMode::Write | OpenMode::CreateNew => {
                let spec = UploadSpec::from(&opts);
                let writer = self.backend.start_upload(&object, &spec, kind).await?;
                Ok(Box::new(GcsFile::writer(
                    loc.path(),
                    opts.mode,
                    writer,
                    Arc::clone(&self.metrics),
                )))
            }
            OpenMode::Append => {
                if !kind.objects_appendable() {
                    return Err(Error::unsupported(format!(
                        "append mode on {} ({kind} bucket): objects are immutable; only zonal buckets support appends",
                        loc.path()
                    )));
                }
                let writer = match self.backend.get_object(&object).await {
                    Ok(existing) => {
                        self.backend
                            .reopen_append(&object, existing.generation)
                            .await?
                    }
                    // Like `"ab"` on a local file system: create it.
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        let spec = UploadSpec::from(&opts);
                        self.backend.start_upload(&object, &spec, kind).await?
                    }
                    Err(e) => return Err(e),
                };
                Ok(Box::new(GcsFile::writer(
                    loc.path(),
                    opts.mode,
                    writer,
                    Arc::clone(&self.metrics),
                )))
            }
        }
    }

    async fn rm_file(&self, path: &str) -> Result<()> {
        let (loc, kind, object) = self.object_loc(path).await?;
        debug!(path = %loc.path(), "rm_file");
        match self.backend.delete_object(&object).await {
            Ok(()) => Ok(()),
            Err(e) => Err(self.refine_not_found(&loc, kind, e).await),
        }
    }

    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<()> {
        let loc = Loc::parse(path)?;
        debug!(path = %loc.path(), ?opts, "mkdir");
        if loc.is_root() {
            return Err(Error::invalid_path(path, "cannot create the root"));
        }
        let spec = BucketSpec {
            location: opts.location.clone(),
            ..Default::default()
        };
        if loc.is_bucket() {
            return self.create_bucket(loc.bucket(), spec).await;
        }
        // The layout lookup doubles as the bucket existence check.
        let kind = match self.backend.bucket_kind(loc.bucket()).await {
            Ok(kind) => kind,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                if !opts.create_parents {
                    return Err(Error::not_found(loc.bucket()));
                }
                self.create_bucket(loc.bucket(), spec).await?;
                BucketKind::Flat
            }
            Err(e) => {
                warn!(bucket = loc.bucket(), error = %e, "could not determine bucket kind; assuming flat");
                BucketKind::Flat
            }
        };
        if kind.is_hierarchical() {
            return match self
                .backend
                .create_folder(loc.bucket(), loc.key(), opts.create_parents)
                .await
            {
                Ok(_) => Ok(()),
                // The service reports a missing parent as a failed precondition.
                Err(e) if e.kind() == ErrorKind::PreconditionFailed => Err(Error::not_found(
                    entry::parent(&loc.path()).unwrap_or_default(),
                )),
                Err(e) => Err(e),
            };
        }
        if opts.placeholder {
            let spec = UploadSpec {
                create_only: true,
                ..Default::default()
            };
            self.backend
                .upload_bytes(&loc.placeholder()?, Bytes::new(), &spec, kind)
                .await?;
        }
        // Otherwise directories are implicit in flat buckets: nothing to do.
        Ok(())
    }

    async fn rmdir(&self, path: &str) -> Result<()> {
        let loc = Loc::parse(path)?;
        debug!(path = %loc.path(), "rmdir");
        if loc.is_root() {
            return Err(Error::invalid_path(path, "cannot remove the root"));
        }
        if loc.is_bucket() {
            return self.backend.delete_bucket(loc.bucket()).await;
        }
        let kind = self.backend.kind_or_flat(loc.bucket()).await;
        let placeholder = loc.placeholder()?;
        if kind.is_hierarchical() {
            // A stray placeholder object would make the folder non-empty.
            if let Err(e) = self.backend.delete_object(&placeholder).await {
                if e.kind() != ErrorKind::NotFound {
                    debug!(path = %placeholder, error = %e, "ignoring placeholder delete failure");
                }
            }
            return match self.backend.delete_folder(loc.bucket(), loc.key()).await {
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    match self.stat_object(&loc.object()?).await? {
                        Some(_) => Err(Error::not_a_directory(&loc.path())),
                        None => Err(e),
                    }
                }
                other => other,
            };
        }
        // Flat: the directory may only contain its own placeholder.
        let listing = self
            .backend
            .list_objects(ListRequest {
                bucket: loc.bucket(),
                prefix: &loc.prefix(),
                delimiter: true,
                versions: false,
                include_folders: false,
                limit: Some(2),
            })
            .await?;
        let has_placeholder = listing
            .objects
            .iter()
            .any(|o| o.name == placeholder.object());
        let children =
            listing.objects.len() - usize::from(has_placeholder) + listing.prefixes.len();
        if children > 0 {
            return Err(Error::directory_not_empty(&loc.path()));
        }
        if has_placeholder {
            return self.backend.delete_object(&placeholder).await;
        }
        match self.stat_object(&loc.object()?).await? {
            Some(_) => Err(Error::not_a_directory(&loc.path())),
            None => Err(Error::not_found(&loc.path())),
        }
    }

    // =====================================================================
    // Overridable primitives: native versions
    // =====================================================================

    /// One flat listing (plus, for hierarchical buckets with `withdirs`, one
    /// folder listing) instead of a breadth-first walk.
    async fn find(&self, path: &str, opts: FindOptions) -> Result<Vec<Entry>> {
        if opts.maxdepth == Some(0) {
            return Err(invalid_maxdepth());
        }
        let loc = Loc::parse(path)?;
        if loc.is_root() {
            // Spanning every bucket: let the generic walk fan out per bucket.
            return derived::find(self, path, opts).await;
        }
        let kind = self.backend.kind_or_flat(loc.bucket()).await;
        debug!(path = %loc.path(), ?opts, %kind, "find");
        let bucket = loc.bucket();
        let prefix = loc.prefix();
        let versions = opts.versions && kind.has_versioning();
        let (listing, folders) = tokio::join!(
            self.backend.list_objects(ListRequest {
                bucket,
                prefix: &prefix,
                delimiter: false,
                versions,
                include_folders: false,
                limit: None,
            }),
            async {
                if opts.withdirs {
                    self.all_folders(bucket, &prefix, kind).await
                } else {
                    Ok(Vec::new())
                }
            }
        );
        let (listing, folders) = (listing?, folders?);

        if listing.objects.is_empty() && folders.is_empty() && !loc.is_bucket() {
            // Nothing below `path/`: it is a file, an empty directory, or
            // missing (gcsfs returns `[]` rather than raising).
            return Ok(match self.info(&loc.path()).await {
                Ok(e) if e.is_file() || opts.withdirs => vec![e],
                Ok(_) => Vec::new(),
                Err(e) if e.kind() == ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(e),
            });
        }

        let mut files = Vec::with_capacity(listing.objects.len());
        // key → directory entry (folder metadata or placeholder stat when known)
        let mut dirs: BTreeMap<String, Entry> = BTreeMap::new();
        let dir_path = |key: &str| format!("{bucket}/{key}");
        for object in listing.objects {
            if opts.withdirs {
                for dir in implied_dirs(&prefix, &object.name) {
                    dirs.entry(dir.to_owned())
                        .or_insert_with(|| Entry::directory(dir_path(dir)));
                }
            }
            if is_placeholder(&object) {
                if opts.withdirs {
                    let key = object.name.trim_end_matches('/').to_owned();
                    let stat = ObjectStat::from(object);
                    let path = dir_path(&key);
                    dirs.insert(key, Entry::directory(path).with_stat(stat));
                }
                continue;
            }
            files.push(object_entry(object, versions));
        }
        for folder in &folders {
            let key = folder_key(folder);
            dirs.insert(key.to_owned(), folder_entry(bucket, folder));
        }
        let mut out = files;
        if opts.withdirs {
            // The starting directory itself, as the generic `find` does.
            dirs.entry(loc.key().to_owned())
                .or_insert_with(|| Entry::directory(loc.path()));
            out.extend(dirs.into_values());
        }
        if let Some(max) = opts.maxdepth {
            let root_depth = entry::depth(&loc.path());
            out.retain(|e| entry::depth(&e.path) - root_depth <= max);
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out.dedup_by(|a, b| a.path == b.path);
        Ok(out)
    }

    /// One ranged read; ranges mixing a negative bound with another bound
    /// cost an extra `GetObject` and are then pinned to that generation.
    async fn cat_file(&self, path: &str, range: ByteRange, opts: ReadOptions) -> Result<Bytes> {
        let start = Instant::now();
        match self.cat_file_impl(path, range, opts).await {
            Ok(bytes) => {
                self.metrics
                    .record_bytes(Direction::Read, bytes.len() as u64, "cat_file");
                self.metrics
                    .record_operation("cat_file", start.elapsed(), Status::Ok);
                Ok(bytes)
            }
            Err(e) => {
                self.metrics
                    .record_operation("cat_file", start.elapsed(), Status::Error);
                Err(e)
            }
        }
    }

    async fn pipe_file(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<()> {
        let start = Instant::now();
        let _stream_guard = ActiveStreamGuard::new(Arc::clone(&self.metrics));
        let len = data.len() as u64;
        let (loc, kind, object) = match self.object_loc(path).await {
            Ok(res) => res,
            Err(e) => {
                self.metrics.record_operation("pipe_file", start.elapsed(), Status::Error);
                return Err(e);
            }
        };
        debug!(path = %loc.path(), len, "pipe_file");
        let spec = UploadSpec::from(&opts);
        match self.backend.upload_bytes(&object, data, &spec, kind).await {
            Ok(_) => {
                self.metrics.record_bytes(Direction::Write, len, "pipe_file");
                self.metrics.record_operation("pipe_file", start.elapsed(), Status::Ok);
                Ok(())
            }
            Err(e) => {
                self.metrics.record_operation("pipe_file", start.elapsed(), Status::Error);
                Err(e)
            }
        }
    }

    async fn put_file(&self, local: &Path, path: &str, opts: WriteOptions) -> Result<()> {
        let start = Instant::now();
        let _stream_guard = ActiveStreamGuard::new(Arc::clone(&self.metrics));
        match self.put_file_impl(local, path, opts).await {
            Ok(outcome) => {
                let op = if outcome.composite {
                    "put_file_composite"
                } else {
                    "put_file"
                };
                self.metrics.record_bytes(Direction::Write, outcome.bytes, op);
                self.metrics.record_operation(op, start.elapsed(), Status::Ok);
                Ok(())
            }
            Err(e) => {
                self.metrics.record_operation("put_file", start.elapsed(), Status::Error);
                Err(e)
            }
        }
    }

    /// Download the file at `path` to the local path `local`, creating parent
    /// directories as needed.
    ///
    /// Downloads are split into parallel byte ranges according to `opts.chunk_size`
    /// (default [`DEFAULT_GET_CHUNK_SIZE`], 128 MiB) and `opts.concurrency`
    /// (default [`DEFAULT_GET_CONCURRENCY`], 24). Bytes stream directly to disk at
    /// assigned offsets without buffering full slices in memory. Writes target an
    /// ephemeral staging file (`<local>.gstmp.<pid>.<nanos>.<counter>`) that is atomically
    /// renamed to `local` on completion and unlinked on error or cancellation.
    async fn get_file(&self, path: &str, local: &Path, opts: ReadOptions) -> Result<()> {
        let start = Instant::now();
        match self.get_file_impl(path, local, opts).await {
            Ok(bytes) => {
                self.metrics.record_bytes(Direction::Read, bytes, "get_file");
                self.metrics.record_operation("get_file", start.elapsed(), Status::Ok);
                Ok(())
            }
            Err(e) => {
                self.metrics.record_operation("get_file", start.elapsed(), Status::Error);
                Err(e)
            }
        }
    }

    /// Server-side `RewriteObject`; not available when either side is a
    /// zonal bucket.
    async fn copy_file(&self, src: &str, dst: &str) -> Result<()> {
        let ((src_loc, src_kind, src_obj), (dst_loc, dst_kind, dst_obj)) =
            tokio::try_join!(self.object_loc(src), self.object_loc(dst))?;
        debug!(src = %src_loc.path(), dst = %dst_loc.path(), "copy_file");
        if !src_kind.supports_server_copy() || !dst_kind.supports_server_copy() {
            return Err(Error::unsupported(format!(
                "copy_file {} -> {}: zonal buckets have no server-side copy; download and upload instead",
                src_loc.path(),
                dst_loc.path()
            )));
        }
        if dst_loc.generation().is_some() {
            return Err(Error::invalid_path(
                dst,
                "cannot copy onto a specific generation",
            ));
        }
        match self.backend.rewrite_object(&src_obj, &dst_obj).await {
            Ok(_) => Ok(()),
            Err(e) => Err(self.refine_not_found(&src_loc, src_kind, e).await),
        }
    }

    /// Atomic `MoveObject` within a bucket; copy + delete across buckets.
    async fn move_file(&self, src: &str, dst: &str) -> Result<()> {
        let ((src_loc, src_kind, src_obj), (dst_loc, dst_kind, dst_obj)) =
            tokio::try_join!(self.object_loc(src), self.object_loc(dst))?;
        debug!(src = %src_loc.path(), dst = %dst_loc.path(), "move_file");
        if src_loc.generation().is_some() || dst_loc.generation().is_some() {
            return Err(Error::invalid_path(
                if src_loc.generation().is_some() {
                    src
                } else {
                    dst
                },
                "specific generations cannot be moved",
            ));
        }
        if src_loc.path() == dst_loc.path() {
            return Ok(());
        }
        if src_loc.bucket() == dst_loc.bucket() {
            return match self.move_within_bucket(&src_obj, &dst_obj, src_kind).await {
                Ok(()) => Ok(()),
                Err(e) => Err(self.refine_not_found(&src_loc, src_kind, e).await),
            };
        }
        if !src_kind.supports_server_copy() || !dst_kind.supports_server_copy() {
            return Err(Error::unsupported(format!(
                "move_file {} -> {}: moves out of or into a zonal bucket need a download and upload",
                src_loc.path(),
                dst_loc.path()
            )));
        }
        if let Err(e) = self.backend.rewrite_object(&src_obj, &dst_obj).await {
            return Err(self.refine_not_found(&src_loc, src_kind, e).await);
        }
        self.backend.delete_object(&src_obj).await
    }

    // =====================================================================
    // Derived operations with a native fast path
    // =====================================================================

    /// Without a depth limit, one listing and concurrent deletes (plus
    /// folder deletes on hierarchical buckets) replace the per-directory
    /// `rmdir` round trips of the generic version.
    async fn rm(&self, path: &str, opts: RmOptions) -> Result<()> {
        let loc = Loc::parse(path)?;
        if loc.is_root() {
            return Err(Error::invalid_path(path, "refusing to remove every bucket"));
        }
        if opts.maxdepth.is_some() {
            return derived::rm(self, path, opts).await;
        }
        let root = self.info(&loc.path()).await?;
        if root.is_file() {
            return self.rm_file(&root.path).await;
        }
        if !opts.recursive {
            return Err(Error::is_a_directory(&loc.path()));
        }
        let kind = self.backend.kind_or_flat(loc.bucket()).await;
        self.rm_tree(&loc, kind, opts.concurrency).await
    }

    /// Directories in hierarchical buckets are renamed atomically with
    /// `RenameFolder`; everything else uses the generic per-file move.
    async fn mv(&self, src: &str, dst: &str, opts: CopyOptions) -> Result<()> {
        let (src_loc, dst_loc) = (Loc::parse(src)?, Loc::parse(dst)?);
        let same_bucket = !src_loc.is_root()
            && !src_loc.is_bucket()
            && !dst_loc.is_root()
            && src_loc.bucket() == dst_loc.bucket()
            && src_loc.generation().is_none()
            && dst_loc.generation().is_none();
        if !same_bucket || opts.maxdepth.is_some() {
            return derived::mv(self, src, dst, opts).await;
        }
        let kind = self.backend.kind_or_flat(src_loc.bucket()).await;
        if !kind.is_hierarchical() {
            return derived::mv(self, src, dst, opts).await;
        }
        let info = self.info(&src_loc.path()).await?;
        if info.is_file() {
            return derived::mv(self, src, dst, opts).await;
        }
        if !opts.recursive {
            return Err(Error::is_a_directory(&src_loc.path()));
        }
        // Same destination rules as `copy`: where would the contents land?
        let base = derived::destination_base(
            self,
            dst,
            entry::basename(&src_loc.path()),
            src.ends_with('/'),
        )
        .await?;
        let base_loc = Loc::parse(&base)?;
        if base_loc.bucket() != src_loc.bucket() || base_loc.is_bucket() {
            return derived::mv(self, src, dst, opts).await;
        }
        if base_loc.path() == src_loc.path() {
            return Ok(());
        }
        debug!(src = %src_loc.path(), dst = %base_loc.path(), "mv: RenameFolder");
        match self
            .backend
            .rename_folder(src_loc.bucket(), src_loc.key(), base_loc.key())
            .await
        {
            Ok(()) => Ok(()),
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::PermissionDenied | ErrorKind::Unauthenticated
                ) =>
            {
                Err(e)
            }
            // Destination exists (merge), parent missing, or the rename is
            // otherwise unavailable: the per-object move handles all of them.
            Err(e) => {
                warn!(src = %src_loc.path(), dst = %base_loc.path(), error = %e, "RenameFolder failed; moving object by object");
                derived::mv(self, src, dst, opts).await
            }
        }
    }
}

/// Helper checking if `path` targets a platform null device (`/dev/null` or Windows `NUL`).
fn is_dev_null(path: &Path) -> bool {
    #[cfg(unix)]
    {
        path == Path::new("/dev/null")
    }
    #[cfg(windows)]
    {
        path == Path::new("NUL") || path == Path::new(r"\\.\NUL")
    }
    #[cfg(not(any(unix, windows)))]
    {
        false
    }
}

static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates an ephemeral staging path `<local>.gstmp.<pid>.<nanos>.<counter>`
/// in the same directory as `local`.
fn staging_path(local: &Path) -> PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = local
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download");
    let tmp_name = format!("{file_name}.gstmp.{}.{nanos}.{counter}", std::process::id());
    local.with_file_name(tmp_name)
}

/// RAII guard that unlinks a staging file on drop unless explicitly disarmed.
struct TempFileGuard(Option<PathBuf>);

impl TempFileGuard {
    fn new(path: Option<PathBuf>) -> Self {
        Self(path)
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(unix)]
fn write_all_at_offset(file: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn write_all_at_offset(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let written = file.seek_write(buf, offset)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        buf = &buf[written..];
        offset += written as u64;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn write_all_at_offset(_file: &std::fs::File, _buf: &[u8], _offset: u64) -> std::io::Result<()> {
    compile_error!("unsupported platform for positional writes");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_prefers_explicit_transport_over_env() {
        let b = GcsFsBuilder::new().transport(Transport::Http);
        let b = b.from_env().unwrap();
        assert_eq!(b.transport, Some(Transport::Http));
    }

    #[test]
    fn builder_records_bucket_kinds() {
        let b = GcsFsBuilder::new()
            .bucket_kind("z", BucketKind::Zonal)
            .project("p")
            .finalize_on_close(true);
        assert_eq!(b.bucket_kinds.get("z"), Some(&BucketKind::Zonal));
        assert_eq!(b.project.as_deref(), Some("p"));
        assert!(b.finalize_on_close);
    }

    #[test]
    fn implied_dirs_between_prefix_and_object() {
        let dirs: Vec<_> = implied_dirs("a/", "a/b/c/d.txt").collect();
        assert_eq!(dirs, ["a/b", "a/b/c"]);
        let dirs: Vec<_> = implied_dirs("", "x/y.txt").collect();
        assert_eq!(dirs, ["x"]);
        assert!(implied_dirs("a/", "a/file").next().is_none());
        // A placeholder implies itself.
        let dirs: Vec<_> = implied_dirs("a/", "a/b/").collect();
        assert_eq!(dirs, ["a/b"]);
        let dirs: Vec<_> = implied_dirs("", "a/").collect();
        assert_eq!(dirs, ["a"]);
    }

    #[test]
    fn entries_from_sdk_models() {
        let object = Object::new()
            .set_bucket("projects/_/buckets/b")
            .set_name("d/f")
            .set_size(3)
            .set_generation(9);
        let e = object_entry(object.clone(), false);
        assert_eq!(e.path, "b/d/f");
        assert!(e.is_file());
        assert_eq!(e.size, 3);
        assert_eq!(object_entry(object, true).path, "b/d/f#9");

        let folder = Folder::default()
            .set_name("projects/_/buckets/b/folders/d/e/")
            .set_metageneration(2);
        let e = folder_entry("b", &folder);
        assert_eq!(e.path, "b/d/e");
        assert!(e.is_dir());
        assert_eq!(e.stat.as_ref().map(|s| s.metageneration), Some(2));

        let bucket = Bucket::default()
            .set_name("projects/_/buckets/b")
            .set_storage_class("STANDARD");
        let e = bucket_entry(&bucket);
        assert_eq!(e.path, "b");
        assert!(e.is_dir());
        assert_eq!(
            e.stat.as_ref().map(|s| s.storage_class.as_str()),
            Some("STANDARD")
        );

        let placeholder = Object::new().set_name("d/").set_size(0);
        assert!(is_placeholder(&placeholder));
        assert!(!is_placeholder(&Object::new().set_name("d/").set_size(1)));
    }

    #[test]
    fn staging_path_is_unique_and_preserves_parent() {
        let local = Path::new("/var/tmp/download.bin");
        let p1 = staging_path(local);
        let p2 = staging_path(local);

        assert_ne!(p1, p2);
        assert_eq!(p1.parent(), local.parent());
        assert_eq!(p2.parent(), local.parent());

        let f1 = p1.file_name().unwrap().to_str().unwrap();
        let f2 = p2.file_name().unwrap().to_str().unwrap();
        assert!(f1.starts_with("download.bin.gstmp."));
        assert!(f2.starts_with("download.bin.gstmp."));
    }

    #[test]
    fn temp_file_guard_drops_and_disarms() {
        let temp_dir = std::env::temp_dir();
        let drop_path = staging_path(&temp_dir.join("test_guard_drop.bin"));
        std::fs::write(&drop_path, b"test-drop").unwrap();
        assert!(drop_path.exists());

        {
            let _guard = TempFileGuard::new(Some(drop_path.clone()));
        }
        assert!(!drop_path.exists());

        let disarm_path = staging_path(&temp_dir.join("test_guard_disarm.bin"));
        std::fs::write(&disarm_path, b"test-disarm").unwrap();
        assert!(disarm_path.exists());

        {
            let mut guard = TempFileGuard::new(Some(disarm_path.clone()));
            guard.disarm();
        }
        assert!(disarm_path.exists());
        let _ = std::fs::remove_file(disarm_path);
    }

    #[test]
    fn write_all_at_offset_concurrent_disjoint_slices() {
        let temp_dir = std::env::temp_dir();
        let path = staging_path(&temp_dir.join("test_pwrite.bin"));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(16384).unwrap();
        let file = Arc::new(file);

        let mut handles = Vec::new();
        for i in 0..4u64 {
            let file = Arc::clone(&file);
            let byte = b'A' + i as u8;
            let handle = std::thread::spawn(move || {
                let chunk = vec![byte; 4096];
                write_all_at_offset(&file, &chunk, i * 4096).unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        let content = std::fs::read(&path).unwrap();
        assert_eq!(content.len(), 16384);
        assert_eq!(&content[0..4096], &[b'A'; 4096]);
        assert_eq!(&content[4096..8192], &[b'B'; 4096]);
        assert_eq!(&content[8192..12288], &[b'C'; 4096]);
        assert_eq!(&content[12288..16384], &[b'D'; 4096]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn dev_null_detection() {
        #[cfg(unix)]
        {
            assert!(is_dev_null(Path::new("/dev/null")));
            assert!(!is_dev_null(Path::new("/dev/shm/file.bin")));
            assert!(!is_dev_null(Path::new("/tmp/null")));
        }
        #[cfg(windows)]
        {
            assert!(is_dev_null(Path::new("NUL")));
            assert!(is_dev_null(Path::new(r"\\.\NUL")));
            assert!(!is_dev_null(Path::new(r"C:\temp\null")));
        }
    }
}
