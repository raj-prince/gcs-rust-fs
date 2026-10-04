//! # gcs-rust-fs
//!
//! Non-POSIX, pure-Rust file-system primitives for Google Cloud Storage, built
//! on the official [`google-cloud-storage`](https://docs.rs/google-cloud-storage)
//! SDK.
//!
//! The crate exists to be the Rust core behind `gcsfs`'s Rust backend: it has
//! **no** Python/PyO3 dependency, so it can be unit-tested with `cargo test`
//! and reused by any Rust application. A thin PyO3 bridge (living in `gcsfs`)
//! converts the types below into Python objects.
//!
//! ## Architecture
//!
//! The crate is split into a storage-agnostic **contract** and a Google Cloud
//! Storage **implementation**:
//!
//! | Layer | Items | Knows about |
//! |-------|-------|-------------|
//! | contract | [`FileSystem`], [`File`], [`Entry`], option structs | paths, bytes, directories, errors |
//! | derived operations | default methods of [`FileSystem`] (`find`, `glob`, `walk`, `rm`, `copy`, ...) | only the six required primitives |
//! | implementation | [`GcsFs`], [`GcsFile`], [`Transport`] | buckets, generations, gRPC / JSON API |
//!
//! [`FileSystem`] is the Rust counterpart of `fsspec`'s `AbstractFileSystem`
//! and [`File`] of its `AbstractBufferedFile`. Both are object-safe async
//! traits, so a language bridge can hold an `Arc<dyn FileSystem>` and a
//! `Box<dyn File>` and pick the implementation at runtime. Implementors use
//! the re-exported [`macro@async_trait`] attribute.
//!
//! > **Status:** the traits and their derived operations are complete;
//! > [`GcsFs`] / [`GcsFile`] currently expose the read-only subset through
//! > inherent methods and do not implement the traits yet.
//!
//! ## Operations available today
//!
//! | Operation | Entry points | Wire protocol |
//! |-----------|--------------|---------------|
//! | metadata  | [`GcsFs::stat`], [`stat`] | gRPC `GetObject` |
//! | ranged read into memory | [`GcsFs::cat_file`], [`cat_file`] | gRPC `BidiReadObject` (default) or JSON API over HTTP — see [`Transport`] |
//! | repeated ranged reads | [`GcsFs::open`] → [`GcsFile`] | same as above, pinned to one generation |
//!
//! ## Two ways to use it
//!
//! Own a client (recommended for Rust applications):
//!
//! ```no_run
//! use gcs_rust_fs::{ByteRange, GcsFs, GcsPath, Transport};
//!
//! # async fn demo() -> gcs_rust_fs::Result<()> {
//! let fs = GcsFs::builder().transport(Transport::Grpc).build().await?;
//! let path: GcsPath = "gs://my-bucket/checkpoint.pt".parse()?;
//! let stat = fs.stat(&path).await?;
//! let first_mib = fs.cat_file(&path, ByteRange::head(1 << 20)).await?;
//! # let _ = (stat, first_mib);
//! # Ok(()) }
//! ```
//!
//! Or use the process-wide [shared] client with path strings — the shape a
//! foreign-language bridge wants:
//!
//! ```no_run
//! # async fn demo() -> gcs_rust_fs::Result<()> {
//! let info = gcs_rust_fs::stat("my-bucket/checkpoint.pt", None).await?;
//! let bytes = gcs_rust_fs::cat_file("my-bucket/checkpoint.pt", Some(0), Some(1024), None).await?;
//! # let _ = (info, bytes);
//! # Ok(()) }
//! ```
//!
//! ## Errors
//!
//! Every error carries an [`ErrorKind`] so callers can react without parsing
//! strings. A Python bridge would typically map `NotFound` →
//! `FileNotFoundError`, `PermissionDenied` → `PermissionError`,
//! `IsADirectory` / `NotADirectory` / `AlreadyExists` / `DirectoryNotEmpty` →
//! the matching `OSError` subclass, `InvalidPath`/`InvalidRange` →
//! `ValueError`, everything else → `OSError`.
//!
//! ## Authentication
//!
//! Credentials come from
//! [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod derived;
mod entry;
mod error;
mod file;
mod filesystem;
mod gcs;
mod glob;
mod options;
mod path;
mod range;
mod stat;

pub use entry::{DiskUsage, Entry, EntryKind, WalkEntry};
pub use error::{classify_storage_error, Error, ErrorKind, Result, StorageError};
pub use file::File;
pub use filesystem::FileSystem;
pub use gcs::{GcsFile, GcsFs, GcsFsBuilder, Transport};
pub use options::{
    BulkOptions, CopyOptions, DuOptions, FindOptions, GlobOptions, ListOptions, MkdirOptions,
    OnError, OpenMode, OpenOptions, PutOptions, RmOptions, WalkOptions, WriteMode, WriteOptions,
    DEFAULT_CONCURRENCY,
};
pub use path::GcsPath;
pub use range::ByteRange;
pub use stat::ObjectStat;

/// The attribute macro that [`FileSystem`] and [`File`] are declared with.
///
/// Implementations must annotate their `impl` blocks with it too:
///
/// ```ignore
/// #[gcs_rust_fs::async_trait]
/// impl gcs_rust_fs::FileSystem for MyFs { /* ... */ }
/// ```
pub use async_trait::async_trait;

/// Re-export of the SDK crate this library is built on, so downstream code can
/// inspect [`StorageError`] details (via [`Error::storage_source`]) without a
/// separate dependency.
pub use google_cloud_storage as sdk;

use bytes::Bytes;
use tokio::sync::OnceCell;

static SHARED: OnceCell<GcsFs> = OnceCell::const_new();

/// The process-wide shared [`GcsFs`].
///
/// Lazily built on first use from [`GcsFsBuilder::from_env`] (so
/// [`Transport::ENV_VAR`] is honoured), unless [`init_shared`] installed a
/// client first. Initialisation failures are not cached: a later call retries.
pub async fn shared() -> Result<&'static GcsFs> {
    SHARED
        .get_or_try_init(|| async { GcsFs::builder().from_env()?.build().await })
        .await
}

/// Install a pre-configured client as the [shared] instance.
///
/// Must be called before the first use of [`shared`] (or of the free functions
/// that rely on it); otherwise returns [`ErrorKind::AlreadyInitialized`].
pub fn init_shared(fs: GcsFs) -> Result<()> {
    SHARED.set(fs).map_err(|_| {
        Error::new(
            ErrorKind::AlreadyInitialized,
            "the shared GcsFs client has already been initialised",
        )
    })
}

/// Parse `path` and apply an explicit `generation` override.
fn resolve_path(path: &str, generation: Option<i64>) -> Result<GcsPath> {
    let parsed = GcsPath::parse(path)?;
    Ok(match generation {
        Some(generation) => parsed.with_generation(Some(generation)),
        None => parsed,
    })
}

/// Fetch object metadata using the [shared] client.
///
/// `path` accepts any form understood by [`GcsPath::parse`]; an explicit
/// `generation` takes precedence over a `#generation` suffix in the path.
pub async fn stat(path: &str, generation: Option<i64>) -> Result<ObjectStat> {
    let path = resolve_path(path, generation)?;
    shared().await?.stat(&path).await
}

/// Whether an object exists, using the [shared] client.
pub async fn exists(path: &str, generation: Option<i64>) -> Result<bool> {
    let path = resolve_path(path, generation)?;
    shared().await?.exists(&path).await
}

/// Read `[start, end)` of an object into memory using the [shared] client.
///
/// `start`/`end` follow Python-slice semantics (see [`ByteRange::new`]):
/// `None` means "from the beginning" / "to the end" and negative values count
/// back from the end of the object.
pub async fn cat_file(
    path: &str,
    start: Option<i64>,
    end: Option<i64>,
    generation: Option<i64>,
) -> Result<Bytes> {
    let path = resolve_path(path, generation)?;
    shared()
        .await?
        .cat_file(&path, ByteRange::new(start, end))
        .await
}

/// Open an object for repeated ranged reads using the [shared] client.
pub async fn open(path: &str, generation: Option<i64>) -> Result<GcsFile> {
    let path = resolve_path(path, generation)?;
    shared().await?.open(&path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_generation_overrides_path_suffix() {
        let p = resolve_path("gs://b/o#5", None).unwrap();
        assert_eq!(p.generation(), Some(5));
        let p = resolve_path("gs://b/o#5", Some(9)).unwrap();
        assert_eq!(p.generation(), Some(9));
        let p = resolve_path("b/o", None).unwrap();
        assert_eq!(p.generation(), None);
        assert_eq!(
            resolve_path("nope", None).unwrap_err().kind(),
            ErrorKind::InvalidPath
        );
    }
}
