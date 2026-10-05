//! [`GcsFs`]: the [`FileSystem`] implementation for Cloud Storage.
//!
//! This module holds file-system semantics only — how a path maps to an
//! object, a placeholder, an HNS folder or a bucket, which errors mean what,
//! and how the three bucket kinds differ (see `BucketKind`). The SDK plumbing
//! lives in the sibling modules (`backend`, `control`, `write`).

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use google_cloud_storage::model::{Bucket, Folder, Object};
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
use crate::options::{
    CopyOptions, FindOptions, ListOptions, MkdirOptions, OpenMode, OpenOptions, RmOptions,
    WriteOptions,
};
use crate::range::{ByteRange, ResolvedRange};
use crate::stat::ObjectStat;

/// Environment variables consulted by [`GcsFsBuilder::from_env`] for the
/// project, in order.
pub const PROJECT_ENV_VARS: [&str; 2] = ["GCS_RUST_FS_PROJECT", "GOOGLE_CLOUD_PROJECT"];

/// Builder for [`GcsFs`].
#[derive(Clone, Debug, Default)]
pub struct GcsFsBuilder {
    transport: Option<Transport>,
    endpoint: Option<String>,
    grpc_subchannel_count: Option<usize>,
    project: Option<String>,
    bucket_kinds: HashMap<String, BucketKind>,
    finalize_on_close: bool,
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

    /// Fill unset options from the environment: the transport from
    /// [`Transport::ENV_VAR`] and the project from [`PROJECT_ENV_VARS`].
    /// Explicitly set options take precedence.
    pub fn from_env(mut self) -> Result<Self> {
        if self.transport.is_none() {
            self.transport = Some(Transport::from_env()?);
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
        let backend = Backend::connect(BackendOptions {
            transport: self.transport.unwrap_or_default(),
            endpoint: self.endpoint,
            grpc_subchannel_count: self.grpc_subchannel_count,
            project: self.project,
            bucket_kinds: self.bucket_kinds,
            finalize_on_close: self.finalize_on_close,
        })
        .await?;
        Ok(GcsFs { backend })
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
/// use gcs_rust_fs::{ByteRange, FileSystem, GcsFs, OpenOptions};
///
/// # async fn demo() -> gcs_rust_fs::Result<()> {
/// let fs = GcsFs::new().await?;
///
/// let info = fs.info("my-bucket/checkpoint.pt").await?;
/// println!("{} bytes", info.size);
///
/// let header = fs.cat_file("my-bucket/checkpoint.pt", ByteRange::head(1024)).await?;
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
}

impl GcsFs {
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
            Err(e) if kind == BucketKind::Zonal => Err(e),
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
        // Like gcsfs, ask for the object and probe for a directory at the
        // same time: latency is the slower of the two, not the sum.
        let (file, dir) = tokio::join!(self.stat_object(&object), async {
            if loc.generation().is_some() {
                Ok(None)
            } else {
                self.dir_entry(&loc, kind).await
            }
        });
        if let Some(stat) = file? {
            return Ok(Entry::file(stat.path(), stat));
        }
        dir?.ok_or_else(|| Error::not_found(&loc.path()))
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
                // Hierarchical buckets have no object versioning.
                versions: opts.versions && !kind.is_hierarchical(),
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
                Ok(Box::new(GcsFile::writer(loc.path(), opts.mode, writer)))
            }
            OpenMode::Append => {
                if kind != BucketKind::Zonal {
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
                Ok(Box::new(GcsFile::writer(loc.path(), opts.mode, writer)))
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
        let versions = opts.versions && !kind.is_hierarchical();
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
    async fn cat_file(&self, path: &str, range: ByteRange) -> Result<Bytes> {
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

    async fn pipe_file(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<()> {
        let (loc, kind, object) = self.object_loc(path).await?;
        debug!(path = %loc.path(), len = data.len(), "pipe_file");
        let spec = UploadSpec::from(&opts);
        self.backend
            .upload_bytes(&object, data, &spec, kind)
            .await
            .map(drop)
    }

    async fn put_file(&self, local: &Path, path: &str, opts: WriteOptions) -> Result<()> {
        let (loc, kind, object) = self.object_loc(path).await?;
        if kind == BucketKind::Zonal {
            // Appendable objects take the streaming path.
            return derived::put_file(self, local, path, opts).await;
        }
        debug!(path = %loc.path(), local = %local.display(), "put_file");
        let file = tokio::fs::File::open(local)
            .await
            .map_err(|e| Error::io("open", local.display(), e))?;
        let spec = UploadSpec::from(&opts);
        self.backend
            .upload_file(&object, file, &spec)
            .await
            .map(drop)
    }

    /// Server-side `RewriteObject`; not available when either side is a
    /// zonal bucket.
    async fn copy_file(&self, src: &str, dst: &str) -> Result<()> {
        let ((src_loc, src_kind, src_obj), (dst_loc, dst_kind, dst_obj)) =
            tokio::try_join!(self.object_loc(src), self.object_loc(dst))?;
        debug!(src = %src_loc.path(), dst = %dst_loc.path(), "copy_file");
        if src_kind == BucketKind::Zonal || dst_kind == BucketKind::Zonal {
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
        if src_kind == BucketKind::Zonal || dst_kind == BucketKind::Zonal {
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
}
