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
//! | Operation | Entry point | Wire protocol |
//! |-----------|-------------|---------------|
//! | metadata  | [`GcsFs::stat`] | gRPC `GetObject` |
//! | ranged read into memory | [`GcsFs::cat_file`] | gRPC `BidiReadObject` (default) or JSON API over HTTP — see [`Transport`] |
//! | repeated ranged reads | [`GcsFs::open`] → [`GcsFile`] | same as above, pinned to one generation |
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
