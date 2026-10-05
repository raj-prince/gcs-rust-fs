//! # gcs-rust-fs
//!
//! Non-POSIX, pure-Rust file-system primitives for Google Cloud Storage, built
//! on the official [`google-cloud-storage`](https://docs.rs/google-cloud-storage)
//! SDK.
//!
//! The crate exists to be the Rust core behind `gcsfs`'s Rust backend: it has
//! **no** Python/PyO3 dependency, so it can be unit-tested with `cargo test`
//! and reused by any Rust application. A thin PyO3 bridge (living in `gcsfs`)
//! owns one file-system instance and converts the types below into Python
//! objects.
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
//! | decorators | [`CachedFs`] (opt-in `ls`/`info` cache with `fsspec` `dircache` semantics) | any [`FileSystem`] |
//! | implementation | [`GcsFs`], [`GcsFile`], [`BucketKind`], [`Transport`] | buckets, generations, folders, gRPC / JSON API |
//!
//! [`FileSystem`] is the Rust counterpart of `fsspec`'s `AbstractFileSystem`
//! and [`File`] of its `AbstractBufferedFile`. Both are object-safe async
//! traits, so a language bridge can hold an `Arc<dyn FileSystem>` and a
//! `Box<dyn File>` and pick the implementation at runtime. Implementors use
//! the re-exported [`macro@async_trait`] attribute.
//!
//! ## Bucket kinds
//!
//! Cloud Storage has three kinds of bucket and [`GcsFs`] handles all of them
//! behind the same contract, detecting the kind of each bucket on first use:
//!
//! | [`BucketKind`] | Directories | Notes |
//! |----------------|-------------|-------|
//! | `Flat` | emulated from object-name prefixes and `dir/` placeholders | gRPC or HTTP reads |
//! | `Hierarchical` | real folders (empty ones exist); `mv` of a directory is an atomic rename | gRPC or HTTP reads |
//! | `Zonal` (Rapid Storage) | as hierarchical | gRPC only; appendable objects (`flush` persists, [`OpenMode::Append`] works); no server-side copy |
//!
//! ```no_run
//! use gcs_rust_fs::{ByteRange, FileSystem, FindOptions, GcsFs, OpenOptions, ReadOptions, Transport};
//!
//! # async fn demo() -> gcs_rust_fs::Result<()> {
//! let fs = GcsFs::builder().transport(Transport::Grpc).build().await?;
//!
//! let info = fs.info("gs://my-bucket/checkpoint.pt").await?;
//! let first_mib = fs
//!     .cat_file("my-bucket/checkpoint.pt", ByteRange::head(1 << 20), ReadOptions::default())
//!     .await?;
//! let everything = fs.find("my-bucket/data", FindOptions::default()).await?;
//!
//! let mut out = fs.open("my-bucket/out.bin", OpenOptions::write()).await?;
//! out.write(first_mib).await?;
//! out.close().await?;
//! # let _ = (info, everything);
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
//! `ValueError`, `Unsupported` → `NotImplementedError`, everything else →
//! `OSError`.
//!
//! ## Authentication
//!
//! Credentials come from
//! [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod cached;
mod derived;
mod dircache;
mod entry;
mod error;
mod file;
mod filesystem;
mod gcs;
mod glob;
mod options;
mod range;
mod stat;

pub use cached::{CacheConfig, CacheStats, CachedFs};
pub use entry::{DiskUsage, Entry, EntryKind, WalkEntry};
pub use error::{classify_storage_error, Error, ErrorKind, Result, StorageError};
pub use file::File;
pub use filesystem::FileSystem;
pub use gcs::{BucketKind, BucketSpec, GcsFile, GcsFs, GcsFsBuilder, Transport, PROJECT_ENV_VARS};
pub use options::{
    BulkOptions, CopyOptions, DuOptions, FindOptions, GlobOptions, ListOptions, MkdirOptions,
    OnError, OpenMode, OpenOptions, PutOptions, ReadOptions, RmOptions, WalkOptions, WriteMode,
    WriteOptions, DEFAULT_CONCURRENCY,
};
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
