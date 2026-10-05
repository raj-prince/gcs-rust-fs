//! [`CachedFs`]: an opt-in directory and metadata cache for any
//! [`FileSystem`]. The semantics are documented on the struct; this module
//! is private and re-exported flat from the crate root.

use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;

use crate::dircache::{parent_key, DirCache, Lookup};
use crate::entry::{self, Entry};
use crate::error::{Error, Result};
use crate::file::File;
use crate::filesystem::FileSystem;
use crate::options::{
    CopyOptions, FindOptions, ListOptions, MkdirOptions, OpenMode, OpenOptions, PutOptions,
    ReadOptions, RmOptions, WriteOptions,
};
use crate::range::ByteRange;
use crate::stat::ObjectStat;

/// Tuning for [`CachedFs`]. The defaults mirror `fsspec`'s `DirCache`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheConfig {
    /// How long a listing or entry stays valid (`fsspec` `listings_expiry_time`).
    /// `None` = until invalidated.
    pub ttl: Option<Duration>,
    /// Maximum number of directories kept; least recently used ones are
    /// evicted first (`fsspec` `max_paths`). `None` = unbounded.
    pub max_dirs: Option<usize>,
    /// Answer [`ErrorKind::NotFound`](crate::ErrorKind::NotFound) from a
    /// cached parent listing when the name is absent, as `fsspec` does. Fast
    /// for `exists` loops, but a file created by *another* process stays
    /// invisible until the listing expires or is invalidated.
    pub negative: bool,
    /// Let a complete `find` fill the listing of every directory it visits.
    pub populate_from_find: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: None,
            max_dirs: None,
            negative: false,
            populate_from_find: true,
        }
    }
}

/// Counters exposed by [`CachedFs::stats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// `ls`/`info` calls answered from the cache.
    pub hits: u64,
    /// `ls`/`info` calls that consulted the cache and went to the backend.
    pub misses: u64,
    /// Invalidation events (one per mutating operation or explicit call).
    pub invalidations: u64,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
}

/// A [`FileSystem`] that serves `ls` and `info` from a shared cache and
/// forwards everything else to the wrapped filesystem.
///
/// Backends in this crate cache nothing. Wrapping one in `CachedFs` adds the
/// `fsspec` directory cache (`dircache`) semantics, extended so that `ls` and
/// `info` share one store:
///
/// * `ls` fills the listing of a directory; `info` on any child is then served
///   from it. Individual `info` results are cached too, without ever being
///   mistaken for a complete listing.
/// * `find` with [`FindOptions::withdirs`] and no `maxdepth` fills every
///   directory it visits (gcsfs `_find(update_cache=True)`).
/// * Every mutating operation invalidates what it may have changed, following
///   gcsfs's `DirCacheUpdater`: writes drop the immediate parent when it is
///   cached and the parent plus all ancestors otherwise (an implicit directory
///   may have appeared); deletes, moves and tree operations drop the affected
///   subtree plus parents and ancestors. Write handles invalidate at `open`
///   and again at `close`.
/// * Listings with [`ListOptions::versions`] and paths carrying a
///   `#<generation>` suffix bypass the cache; [`ListOptions::refresh`] skips
///   the lookup and replaces the stored listing; data (`cat_file`, `open` for
///   reading, `get_file`) is never cached.
///
/// The cache is exact as long as this client is the only writer between
/// listings; other writers are reconciled by [`CacheConfig::ttl`],
/// [`FileSystem::invalidate_cache`] or `refresh` — exactly the contract gcsfs
/// documents.
///
/// ```no_run
/// use bytes::Bytes;
/// use gcs_rust_fs::{CacheConfig, CachedFs, FileSystem, GcsFs, ListOptions, WriteOptions};
///
/// # async fn demo() -> gcs_rust_fs::Result<()> {
/// let fs = CachedFs::new(GcsFs::new().await?, CacheConfig::default());
///
/// fs.ls("my-bucket/data", ListOptions::default()).await?; // one request
/// fs.info("my-bucket/data/part-0.parquet").await?; // answered from the listing
/// assert!(fs.is_dir("my-bucket/data").await?); // likewise
///
/// let opts = WriteOptions::default();
/// fs.pipe_file("my-bucket/data/new.bin", Bytes::from_static(b"x"), opts).await?;
/// fs.ls("my-bucket/data", ListOptions::default()).await?; // refetched: own writes invalidate
///
/// fs.invalidate_cache(Some("my-bucket/data")); // another writer changed things
/// let refresh = ListOptions { refresh: true, ..Default::default() };
/// fs.ls("my-bucket/data", refresh).await?; // or bypass once and re-store
/// println!("{:?}", fs.stats()); // hits / misses / invalidations
/// # Ok(()) }
/// ```
pub struct CachedFs<F> {
    inner: F,
    cache: Arc<Mutex<DirCache>>,
    counters: Arc<Counters>,
    negative: bool,
    populate_from_find: bool,
}

impl<F> CachedFs<F> {
    /// Wrap `inner`.
    pub fn new(inner: F, config: CacheConfig) -> Self {
        Self {
            inner,
            cache: Arc::new(Mutex::new(DirCache::new(config.ttl, config.max_dirs))),
            counters: Arc::default(),
            negative: config.negative,
            populate_from_find: config.populate_from_find,
        }
    }

    /// The wrapped filesystem.
    pub fn inner(&self) -> &F {
        &self.inner
    }

    /// Hit/miss/invalidation counters since construction.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            invalidations: self.counters.invalidations.load(Ordering::Relaxed),
        }
    }

    fn lock(&self) -> MutexGuard<'_, DirCache> {
        lock(&self.cache)
    }

    fn hit(&self) {
        self.counters.hits.fetch_add(1, Ordering::Relaxed);
    }

    fn miss(&self) {
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// A file or directory was created or replaced at `key`.
    fn invalidated_write(&self, key: &str) {
        self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        written(&mut self.lock(), key);
    }

    /// Everything at and below `key` may have changed or disappeared.
    fn invalidated_tree(&self, key: &str) {
        self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        let mut cache = self.lock();
        cache.drop_tree(key);
        if let Some(parent) = parent_key(key) {
            cache.drop_ancestors(parent);
        }
    }

    /// Store one complete listing per directory found in a complete `find`.
    fn populate(&self, root: &str, entries: &[Entry]) {
        // `find` on a file returns that file, on a missing path nothing: neither
        // says anything about directories.
        if entries.is_empty() || entries.iter().any(|e| e.path == root && e.is_file()) {
            return;
        }
        let mut groups: HashMap<&str, Vec<Entry>> = HashMap::new();
        groups.entry(root).or_default();
        for e in entries {
            if e.path == root {
                continue;
            }
            if e.is_dir() {
                groups.entry(e.path.as_str()).or_default();
            }
            if let Some(parent) = parent_key(&e.path) {
                groups.entry(parent).or_default().push(e.clone());
            }
        }
        let now = Instant::now();
        let mut cache = self.lock();
        for (dir, children) in groups {
            cache.put_listing(dir, &children, now);
        }
    }
}

impl<F: Clone> Clone for CachedFs<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: Arc::clone(&self.cache),
            counters: Arc::clone(&self.counters),
            negative: self.negative,
            populate_from_find: self.populate_from_find,
        }
    }
}

impl<F: fmt::Debug> fmt::Debug for CachedFs<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedFs")
            .field("inner", &self.inner)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

fn lock(cache: &Mutex<DirCache>) -> MutexGuard<'_, DirCache> {
    // The critical sections never panic in normal operation; a poisoned cache
    // is still a valid cache.
    cache.lock().unwrap_or_else(|e| e.into_inner())
}

/// gcsfs `_write_file_cache_update`: a cached parent is known to exist, so
/// only its listing changed; an uncached parent may have been created by this
/// write, which can surface new directories in every ancestor.
fn written(cache: &mut DirCache, key: &str) {
    cache.drop_node(key);
    if let Some(parent) = parent_key(key) {
        if cache.contains(parent) {
            cache.drop_node(parent);
        } else {
            cache.drop_ancestors(parent);
        }
    }
}

/// Cache key for a user path: scheme and surrounding slashes removed.
fn canonical(path: &str) -> String {
    let p = path.find("://").map_or(path, |i| &path[i + 3..]);
    p.trim_matches('/').to_owned()
}

/// `true` for paths that name a specific generation (`path#123`), which the
/// cache never answers for.
fn versioned(path: &str) -> bool {
    path.rsplit_once('#')
        .is_some_and(|(_, gen)| !gen.is_empty() && gen.bytes().all(|b| b.is_ascii_digit()))
}

/// `true` when `entries` is the listing of directory `dir` (every entry is an
/// immediate child), as opposed to `ls` of a file returning that file.
fn is_listing_of(dir: &str, entries: &[Entry]) -> bool {
    entries.iter().all(|e| parent_key(&e.path) == Some(dir))
}

#[async_trait]
impl<F: FileSystem> FileSystem for CachedFs<F> {
    async fn info(&self, path: &str) -> Result<Entry> {
        let key = canonical(path);
        if key.is_empty() || versioned(&key) {
            return self.inner.info(path).await;
        }
        let now = Instant::now();
        {
            let mut cache = self.lock();
            if let Some(parent) = parent_key(&key) {
                match cache.entry(parent, entry::basename(&key), now) {
                    Lookup::Hit(found) => {
                        drop(cache);
                        self.hit();
                        return Ok(*found);
                    }
                    Lookup::Absent if self.negative => {
                        drop(cache);
                        self.hit();
                        return Err(Error::not_found(&key));
                    }
                    _ => {}
                }
            }
            if cache.is_complete_dir(&key, now) {
                drop(cache);
                self.hit();
                return Ok(Entry::directory(key));
            }
        }
        self.miss();
        let found = self.inner.info(path).await?;
        self.lock().put_entry(&found, Instant::now());
        Ok(found)
    }

    async fn ls(&self, path: &str, opts: ListOptions) -> Result<Vec<Entry>> {
        let key = canonical(path);
        if opts.versions || versioned(&key) {
            return self.inner.ls(path, opts).await;
        }
        if !opts.refresh {
            let now = Instant::now();
            let mut cache = self.lock();
            if let Some(entries) = cache.listing(&key, now) {
                drop(cache);
                self.hit();
                return Ok(entries);
            }
            if self.negative {
                if let Some(parent) = parent_key(&key) {
                    if cache.entry(parent, entry::basename(&key), now) == Lookup::Absent {
                        drop(cache);
                        self.hit();
                        return Err(Error::not_found(&key));
                    }
                }
            }
        }
        self.miss();
        let entries = self.inner.ls(path, opts).await?;
        let now = Instant::now();
        let mut cache = self.lock();
        if is_listing_of(&key, &entries) {
            cache.put_listing(&key, &entries, now);
        } else if let [single] = entries.as_slice() {
            cache.put_entry(single, now);
        }
        Ok(entries)
    }

    async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>> {
        if !opts.mode.is_write() {
            return self.inner.open(path, opts).await;
        }
        let key = canonical(path);
        let inner = self.inner.open(path, opts).await?;
        // Appendable (zonal) objects exist from `open`; resumable uploads only
        // from `close`. Invalidate at both points.
        self.invalidated_write(&key);
        Ok(Box::new(CachedFile {
            inner,
            key,
            cache: Arc::clone(&self.cache),
            counters: Arc::clone(&self.counters),
        }))
    }

    async fn rm_file(&self, path: &str) -> Result<()> {
        let result = self.inner.rm_file(path).await;
        self.invalidated_tree(&canonical(path));
        result
    }

    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<()> {
        let result = self.inner.mkdir(path, opts).await;
        self.invalidated_write(&canonical(path));
        result
    }

    async fn rmdir(&self, path: &str) -> Result<()> {
        let result = self.inner.rmdir(path).await;
        self.invalidated_tree(&canonical(path));
        result
    }

    async fn find(&self, path: &str, opts: FindOptions) -> Result<Vec<Entry>> {
        let key = canonical(path);
        let complete = self.populate_from_find
            && opts.withdirs
            && opts.maxdepth.is_none()
            && !opts.versions
            && !versioned(&key);
        let entries = self.inner.find(path, opts).await?;
        if complete {
            self.populate(&key, &entries);
        }
        Ok(entries)
    }

    async fn cat_file(&self, path: &str, range: ByteRange, opts: ReadOptions) -> Result<Bytes> {
        self.inner.cat_file(path, range, opts).await
    }

    async fn pipe_file(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<()> {
        let result = self.inner.pipe_file(path, data, opts).await;
        self.invalidated_write(&canonical(path));
        result
    }

    async fn put_file(&self, local: &Path, path: &str, opts: WriteOptions) -> Result<()> {
        let result = self.inner.put_file(local, path, opts).await;
        self.invalidated_write(&canonical(path));
        result
    }

    async fn get_file(&self, path: &str, local: &Path, opts: ReadOptions) -> Result<()> {
        self.inner.get_file(path, local, opts).await
    }

    async fn copy_file(&self, src: &str, dst: &str) -> Result<()> {
        let result = self.inner.copy_file(src, dst).await;
        self.invalidated_write(&canonical(dst));
        result
    }

    async fn move_file(&self, src: &str, dst: &str) -> Result<()> {
        let result = self.inner.move_file(src, dst).await;
        self.invalidated_tree(&canonical(src));
        self.invalidated_write(&canonical(dst));
        result
    }

    fn invalidate_cache(&self, path: Option<&str>) {
        self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        let mut cache = self.lock();
        match path {
            None => cache.clear(),
            Some(p) => {
                let key = canonical(p);
                cache.drop_tree(&key);
                cache.drop_ancestors(&key);
            }
        }
    }

    // `exists`, `is_file`, `is_dir`, `size`, `walk`, `du`, `glob`, `cat` and
    // `cat_ranges` keep their generic defaults so they run on top of the
    // cached primitives above. The mutating bulk operations are forwarded so
    // the wrapped filesystem's native implementations are used.

    async fn rm(&self, path: &str, opts: RmOptions) -> Result<()> {
        let result = self.inner.rm(path, opts).await;
        self.invalidated_tree(&canonical(path));
        result
    }

    async fn copy(&self, src: &str, dst: &str, opts: CopyOptions) -> Result<()> {
        let result = self.inner.copy(src, dst, opts).await;
        self.invalidated_tree(&canonical(dst));
        result
    }

    async fn mv(&self, src: &str, dst: &str, opts: CopyOptions) -> Result<()> {
        let result = self.inner.mv(src, dst, opts).await;
        self.invalidated_tree(&canonical(src));
        self.invalidated_tree(&canonical(dst));
        result
    }

    async fn put(&self, local: &Path, path: &str, opts: PutOptions) -> Result<()> {
        let result = self.inner.put(local, path, opts).await;
        self.invalidated_tree(&canonical(path));
        result
    }
}

/// A write handle that invalidates the cached parent listing when the file is
/// published.
struct CachedFile {
    inner: Box<dyn File>,
    key: String,
    cache: Arc<Mutex<DirCache>>,
    counters: Arc<Counters>,
}

#[async_trait]
impl File for CachedFile {
    fn path(&self) -> &str {
        self.inner.path()
    }

    fn mode(&self) -> OpenMode {
        self.inner.mode()
    }

    fn closed(&self) -> bool {
        self.inner.closed()
    }

    fn tell(&self) -> u64 {
        self.inner.tell()
    }

    fn size(&self) -> Option<u64> {
        self.inner.size()
    }

    fn stat(&self) -> Option<&ObjectStat> {
        self.inner.stat()
    }

    fn readable(&self) -> bool {
        self.inner.readable()
    }

    fn writable(&self) -> bool {
        self.inner.writable()
    }

    fn seekable(&self) -> bool {
        self.inner.seekable()
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    async fn read(&mut self, len: Option<usize>) -> Result<Bytes> {
        self.inner.read(len).await
    }

    async fn read_range(&self, range: ByteRange) -> Result<Bytes> {
        self.inner.read_range(range).await
    }

    async fn write(&mut self, data: Bytes) -> Result<()> {
        self.inner.write(data).await
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }

    async fn close(&mut self) -> Result<()> {
        let result = self.inner.close().await;
        self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
        written(&mut lock(&self.cache), &self.key);
        result
    }

    async fn discard(&mut self) -> Result<()> {
        // Nothing is published, so nothing cached can have changed.
        self.inner.discard().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_paths() {
        assert_eq!(canonical("gs://b/x/"), "b/x");
        assert_eq!(canonical("/b/x"), "b/x");
        assert_eq!(canonical("b"), "b");
        assert_eq!(canonical(""), "");
        assert_eq!(canonical("gs://"), "");
    }

    #[test]
    fn generation_suffixes() {
        assert!(versioned("b/x#123"));
        assert!(!versioned("b/x"));
        assert!(!versioned("b/x#"));
        assert!(!versioned("b/x#abc"));
        assert!(!versioned("b/c#1/y"));
    }

    #[test]
    fn listing_detection() {
        let dir = |p: &str| Entry::directory(p);
        assert!(is_listing_of("b", &[dir("b/x"), dir("b/y")]));
        assert!(is_listing_of("", &[dir("b"), dir("c")]));
        assert!(is_listing_of("b/empty", &[]));
        assert!(!is_listing_of("b/x", &[dir("b/x")]), "ls of a file");
    }

    #[test]
    fn cached_fs_is_object_safe_and_shareable() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CachedFs<crate::gcs::GcsFs>>();
        assert_send_sync::<CachedFile>();
    }
}
