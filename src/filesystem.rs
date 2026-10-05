//! The [`FileSystem`] contract.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use crate::derived;
use crate::entry::{DiskUsage, Entry, WalkEntry};
use crate::error::Result;
use crate::file::File;
use crate::options::{
    BulkOptions, CopyOptions, DuOptions, FindOptions, GlobOptions, ListOptions, MkdirOptions,
    OpenOptions, PutOptions, ReadOptions, RmOptions, WalkOptions, WriteOptions,
};
use crate::range::ByteRange;

/// A non-POSIX file system over an object store — the Rust counterpart of
/// `fsspec`'s `AbstractFileSystem`.
///
/// Paths are strings in `fsspec` form: `bucket/key`, optionally prefixed with
/// a scheme (`gs://bucket/key`) and optionally suffixed with a version
/// (`bucket/key#generation`). Directories are emulated: a directory is a
/// bucket, a zero-byte `dir/` placeholder object, or any prefix with at least
/// one object below it. Consequently an empty nested directory does not exist
/// unless a placeholder was created for it.
///
/// # Implementing
///
/// Only six methods are required; everything else has a default built on
/// top of them (the same layering `fsspec` uses):
///
/// | Required primitive | Purpose |
/// |---|---|
/// | [`info`](Self::info) | one entry: file, directory, or `NotFound` |
/// | [`ls`](Self::ls) | immediate children of a directory |
/// | [`open`](Self::open) | obtain a [`File`] handle for reading or writing |
/// | [`rm_file`](Self::rm_file) | delete one file |
/// | [`mkdir`](Self::mkdir) | create a directory (bucket / placeholder) |
/// | [`rmdir`](Self::rmdir) | remove an empty directory |
///
/// The remaining methods fall into two groups. *Overridable primitives*
/// ([`find`](Self::find), [`cat_file`](Self::cat_file),
/// [`pipe_file`](Self::pipe_file), [`put_file`](Self::put_file),
/// [`get_file`](Self::get_file), [`copy_file`](Self::copy_file),
/// [`move_file`](Self::move_file)) work generically but have much faster
/// native equivalents on real object stores (flat listings, ranged reads,
/// server-side copy/rename) and should be overridden. *Derived operations*
/// ([`exists`](Self::exists), [`is_file`](Self::is_file),
/// [`is_dir`](Self::is_dir), [`size`](Self::size), [`walk`](Self::walk),
/// [`du`](Self::du), [`glob`](Self::glob), [`cat`](Self::cat),
/// [`cat_ranges`](Self::cat_ranges), [`rm`](Self::rm), [`copy`](Self::copy),
/// [`mv`](Self::mv), [`put`](Self::put)) encode `fsspec` semantics and
/// normally need no override.
///
/// # Semantics shared by every implementation
///
/// * `exists` is `true` for files **and** directories.
/// * File operations on a directory fail with
///   [`ErrorKind::IsADirectory`](crate::ErrorKind::IsADirectory); recursive
///   variants exist for `rm`, `copy` and `mv`.
/// * Bulk operations run with bounded concurrency and honour
///   [`OnError`](crate::OnError) where `fsspec` exposes `on_error`.
/// * Nothing is cached: every call reflects the store at the time of the
///   request. Directory caching, if wanted, belongs to the caller.
///
/// The trait is object-safe: `Arc<dyn FileSystem>` and `Box<dyn File>` are
/// the intended currency for language bridges.
#[async_trait]
pub trait FileSystem: Send + Sync {
    // =====================================================================
    // Required primitives
    // =====================================================================

    /// Describe `path`: a file entry, a directory entry, or
    /// [`ErrorKind::NotFound`](crate::ErrorKind::NotFound).
    ///
    /// For object stores the order of precedence is: exact object (unless it
    /// is a zero-byte `dir/` placeholder) → directory if anything exists
    /// below `path/` → not found. Buckets are directories.
    async fn info(&self, path: &str) -> Result<Entry>;

    /// List the immediate children of the directory at `path`, sorted by
    /// path. Listing a file returns that single file; listing a missing
    /// path is [`ErrorKind::NotFound`](crate::ErrorKind::NotFound).
    ///
    /// Implementations decide what the root (`""`) means; for an object
    /// store it is the list of buckets.
    async fn ls(&self, path: &str, opts: ListOptions) -> Result<Vec<Entry>>;

    /// Open a [`File`] handle at `path` in the requested mode.
    ///
    /// Read modes require an existing file (directories are
    /// [`ErrorKind::IsADirectory`](crate::ErrorKind::IsADirectory)). Write
    /// modes create the file on [`File::close`]. Modes an implementation
    /// cannot honour (e.g. append on an object store) fail with
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported).
    async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>>;

    /// Delete the file at `path`. Directories are
    /// [`ErrorKind::IsADirectory`](crate::ErrorKind::IsADirectory); a
    /// missing file is [`ErrorKind::NotFound`](crate::ErrorKind::NotFound).
    async fn rm_file(&self, path: &str) -> Result<()>;

    /// Create a directory. For object stores a top-level path creates a
    /// bucket; a nested path is a no-op (directories are implicit) unless
    /// [`MkdirOptions::placeholder`] asks for a placeholder object. Creating
    /// an existing directory is
    /// [`ErrorKind::AlreadyExists`](crate::ErrorKind::AlreadyExists).
    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<()>;

    /// Remove an **empty** directory (a bucket, or a placeholder). A
    /// non-empty directory is
    /// [`ErrorKind::DirectoryNotEmpty`](crate::ErrorKind::DirectoryNotEmpty),
    /// a file is [`ErrorKind::NotADirectory`](crate::ErrorKind::NotADirectory).
    async fn rmdir(&self, path: &str) -> Result<()>;

    // =====================================================================
    // Overridable primitives (generic defaults; override for performance)
    // =====================================================================

    /// Every file below `path` (recursively), sorted by path.
    ///
    /// * `find` on a file returns that file; on a missing path returns an
    ///   empty list.
    /// * With [`FindOptions::withdirs`] directories are included, as is the
    ///   starting directory itself.
    /// * [`FindOptions::maxdepth`] limits the descent (`Some(1)` = children).
    ///
    /// The default walks [`ls`](Self::ls) breadth-first; object stores
    /// should override it with one flat (delimiter-less) listing.
    async fn find(&self, path: &str, opts: FindOptions) -> Result<Vec<Entry>> {
        derived::find(self, path, opts).await
    }

    /// Read a byte range of a file into memory (Python-slice semantics for
    /// `range`; see [`ByteRange`]). `opts` carries per-read knobs; pass
    /// [`ReadOptions::default()`] unless one is needed.
    ///
    /// The default opens the file and performs one positional read.
    async fn cat_file(&self, path: &str, range: ByteRange, opts: ReadOptions) -> Result<Bytes> {
        derived::cat_file(self, path, range, opts).await
    }

    /// Create or replace the file at `path` with `data` in one call.
    async fn pipe_file(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<()> {
        derived::pipe_file(self, path, data, opts).await
    }

    /// Upload the local file `local` to `path`.
    async fn put_file(&self, local: &Path, path: &str, opts: WriteOptions) -> Result<()> {
        derived::put_file(self, local, path, opts).await
    }

    /// Download the file at `path` to the local path `local`, creating parent
    /// directories as needed.
    async fn get_file(&self, path: &str, local: &Path, opts: ReadOptions) -> Result<()> {
        derived::get_file(self, path, local, opts).await
    }

    /// Copy one file to a new path, replacing any existing file there.
    ///
    /// The default streams the bytes through the client; object stores
    /// should override it with a server-side copy.
    async fn copy_file(&self, src: &str, dst: &str) -> Result<()> {
        derived::copy_file(self, src, dst).await
    }

    /// Move one file to a new path, replacing any existing file there.
    ///
    /// The default is [`copy_file`](Self::copy_file) followed by
    /// [`rm_file`](Self::rm_file); override with an atomic rename when the
    /// store has one.
    async fn move_file(&self, src: &str, dst: &str) -> Result<()> {
        derived::move_file(self, src, dst).await
    }

    // =====================================================================
    // Derived operations
    // =====================================================================

    /// `true` if `path` is a file **or** a directory.
    async fn exists(&self, path: &str) -> Result<bool> {
        derived::exists(self, path).await
    }

    /// `true` if `path` exists and is a file.
    async fn is_file(&self, path: &str) -> Result<bool> {
        derived::is_file(self, path).await
    }

    /// `true` if `path` exists and is a directory.
    async fn is_dir(&self, path: &str) -> Result<bool> {
        derived::is_dir(self, path).await
    }

    /// Size in bytes of the file at `path` (`0` for directories).
    async fn size(&self, path: &str) -> Result<u64> {
        derived::size(self, path).await
    }

    /// Visit `path` and every directory below it, top-down and sorted —
    /// `os.walk` for object stores. Walking a file or a missing path yields
    /// nothing.
    ///
    /// The default groups one [`find`](Self::find) listing, so it costs a
    /// single listing pass rather than one `ls` per directory.
    async fn walk(&self, path: &str, opts: WalkOptions) -> Result<Vec<WalkEntry>> {
        derived::walk(self, path, opts).await
    }

    /// Total size of all files below `path`, plus the per-entry breakdown.
    async fn du(&self, path: &str, opts: DuOptions) -> Result<DiskUsage> {
        derived::du(self, path, opts).await
    }

    /// Entries matching a shell-style `pattern` (`*`, `?`, `[seq]`, `**`),
    /// sorted by path. `*` never crosses `/`; `**` matches any number of
    /// levels. A pattern ending in `/` matches directories only; a pattern
    /// without wildcards returns the entry itself if it exists.
    ///
    /// The default lists the longest wildcard-free prefix with
    /// [`find`](Self::find) and filters client-side.
    async fn glob(&self, pattern: &str, opts: GlobOptions) -> Result<Vec<Entry>> {
        derived::glob(self, pattern, opts).await
    }

    /// Read several whole files concurrently. The result preserves the input
    /// order; with [`OnError::Ignore`](crate::OnError::Ignore) failed paths
    /// are omitted, with [`OnError::Return`](crate::OnError::Return) they are
    /// reported in place.
    async fn cat(&self, paths: &[&str], opts: BulkOptions) -> Result<Vec<(String, Result<Bytes>)>> {
        derived::cat(self, paths, opts).await
    }

    /// Read several `(path, range)` pairs concurrently, preserving order.
    /// Because positions must stay aligned, `OnError::Ignore` behaves like
    /// `OnError::Return` here.
    async fn cat_ranges(
        &self,
        requests: &[(&str, ByteRange)],
        opts: BulkOptions,
    ) -> Result<Vec<Result<Bytes>>> {
        derived::cat_ranges(self, requests, opts).await
    }

    /// Delete a file, or — with [`RmOptions::recursive`] — a directory and
    /// everything below it. Files that vanish concurrently are not errors;
    /// a missing `path` is [`ErrorKind::NotFound`](crate::ErrorKind::NotFound).
    ///
    /// The default deletes files concurrently, then removes directories
    /// deepest-first and finally `path` itself via [`rmdir`](Self::rmdir).
    async fn rm(&self, path: &str, opts: RmOptions) -> Result<()> {
        derived::rm(self, path, opts).await
    }

    /// Copy a file or (with [`CopyOptions::recursive`]) a directory tree.
    ///
    /// Destination rules follow `fsspec`:
    /// * a file copied to a `dst` that ends in `/` or is an existing
    ///   directory lands at `dst/<name>`, otherwise `dst` is the new name;
    /// * a directory `src/` (trailing slash) copies its *contents* into
    ///   `dst`, while `src` (no trailing slash) copied into an existing
    ///   directory lands at `dst/<name>/...`.
    async fn copy(&self, src: &str, dst: &str, opts: CopyOptions) -> Result<()> {
        derived::copy(self, src, dst, opts).await
    }

    /// Move a file or directory tree; same destination rules as
    /// [`copy`](Self::copy). The default moves file by file with
    /// [`move_file`](Self::move_file) and then removes what is left of
    /// `src`.
    async fn mv(&self, src: &str, dst: &str, opts: CopyOptions) -> Result<()> {
        derived::mv(self, src, dst, opts).await
    }

    /// Upload a local file or (with [`PutOptions::recursive`]) directory
    /// tree; same destination rules as [`copy`](Self::copy).
    async fn put(&self, local: &Path, path: &str, opts: PutOptions) -> Result<()> {
        derived::put(self, local, path, opts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time checks: object-safe and shareable across threads.
    fn _assert_object_safe(_: &dyn FileSystem) {}
    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn dyn_filesystem_is_send_sync() {
        _assert_send_sync::<std::sync::Arc<dyn FileSystem>>();
    }
}
