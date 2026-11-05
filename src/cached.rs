//! [`CachedFs`]: an opt-in directory and metadata cache for any
//! [`FileSystem`]. The semantics are documented on the struct; this module
//! is private and re-exported flat from the crate root.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::future::{BoxFuture, FutureExt, Shared};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::dircache::{parent_key, DirCache, Lookup};
use crate::entry::{self, Entry};
use crate::error::{Error, Result};
use crate::file::File;
use crate::filesystem::FileSystem;
use crate::options::{
    CopyOptions, FindOptions, ListOptions, MkdirOptions, OpenMode, OpenOptions, PutOptions,
    ReadOptions, ReadPattern, RmOptions, WriteOptions,
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
    /// Data caching and asynchronous readahead configuration.
    /// `None` = data operations are passed through unbuffered and unreadahead.
    pub data: Option<DataConfig>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: None,
            max_dirs: None,
            negative: false,
            populate_from_find: true,
            data: None,
        }
    }
}

impl CacheConfig {
    /// Configure data caching and asynchronous readahead.
    pub fn with_data(mut self, data: DataConfig) -> Self {
        self.data = Some(data);
        self
    }
}

/// Key uniquely identifying an immutable Cloud Storage object generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    /// The bucket name.
    pub bucket: Arc<str>,
    /// The object path within the bucket.
    pub object: Arc<str>,
    /// Content generation.
    pub generation: i64,
}

/// Data caching and readahead configuration for [`CachedFs`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataConfig {
    /// Asynchronous readahead engine configuration.
    pub readahead: ReadaheadConfig,
    /// Shared page cache configuration (`None` = Mode 1 readahead-only; `Some` = Mode 2 shared page cache).
    pub cache: Option<PageCacheConfig>,
}

impl DataConfig {
    /// Create readahead-only (Mode 1) configuration with default readahead tuning.
    pub fn readahead_only() -> Self {
        Self {
            readahead: ReadaheadConfig::default(),
            cache: None,
        }
    }

    /// Create shared page cache (Mode 2) configuration with default tuning and specified capacity.
    pub fn page_cache(capacity: usize) -> Self {
        Self {
            readahead: ReadaheadConfig::default(),
            cache: Some(PageCacheConfig { capacity }),
        }
    }
}

/// Readahead engine tuning parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadaheadConfig {
    /// Fundamental page unit for indexing and alignment (default: 256 KiB).
    pub page_size: usize,
    /// Maximum readahead window size in bytes (default: 64 MiB).
    pub max_readahead: usize,
    /// Initial / default folio request size in bytes (default: 8 MiB).
    pub io_size: usize,
    /// Maximum folio request size in bytes (default: 32 MiB).
    pub max_folio_size: usize,
    /// Maximum concurrent folios in flight per handle (default: 8).
    pub max_inflight: usize,
    /// Global limit on in-flight speculative download bytes across all handles (default: 256 MiB).
    pub max_inflight_bytes: usize,
}

impl Default for ReadaheadConfig {
    fn default() -> Self {
        Self {
            page_size: 256 * 1024,
            max_readahead: 64 * 1024 * 1024,
            io_size: 8 * 1024 * 1024,
            max_folio_size: 32 * 1024 * 1024,
            max_inflight: 8,
            max_inflight_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Shared page cache configuration (Mode 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageCacheConfig {
    /// Global resident memory capacity in bytes (default: 512 MiB).
    pub capacity: usize,
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            capacity: 512 * 1024 * 1024,
        }
    }
}

/// A contiguous block of pages representing a single backend fetch response.
pub struct Folio {
    /// First page index (in page_size units).
    pub first_page: u64,
    /// Total pages in this folio.
    pub num_pages: u32,
    /// Linux-style `PG_readahead` marker.
    pub readahead_mark: AtomicBool,
    /// Folio state (loading future, ready bytes, or quarantined failure).
    pub state: Mutex<FolioState>,
}

/// Lifecycle state of a [`Folio`].
#[derive(Clone)]
pub enum FolioState {
    /// In-flight fetch future shared among concurrent readers.
    Loading(Shared<BoxFuture<'static, std::result::Result<Bytes, Arc<Error>>>>),
    /// Downloaded and verified bytes.
    Ready(Bytes),
    /// Quarantined terminal error.
    Failed(Arc<Error>),
}

/// Shared per-object page directory in Mode 2.
pub struct ObjectPages {
    /// Identity of the object.
    pub key: ObjectKey,
    /// Object size in bytes.
    #[allow(dead_code)]
    pub size: u64,
    /// Sparse map of start page index -> folio.
    pub folios: Mutex<BTreeMap<u64, Arc<Folio>>>,
}

/// Global LRU page cache across all objects in Mode 2.
pub struct PageCache {
    /// Active objects.
    pub objects: Mutex<HashMap<ObjectKey, Arc<ObjectPages>>>,
    /// Logical clock for LRU recency ordering.
    pub tick: AtomicU64,
    /// Map of tick -> (ObjectKey, start_page).
    pub recency: Mutex<BTreeMap<u64, (ObjectKey, u64)>>,
    /// Current resident bytes across all ready folios.
    pub used_bytes: AtomicUsize,
    /// Capacity ceiling in bytes.
    pub capacity_bytes: usize,
}

impl PageCache {
    /// Create a new shared page cache with the specified byte budget.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            tick: AtomicU64::new(0),
            recency: Mutex::new(BTreeMap::new()),
            used_bytes: AtomicUsize::new(0),
            capacity_bytes,
        }
    }

    /// Record an access to a folio, updating its position in the LRU recency list.
    pub fn record_access(&self, key: &ObjectKey, first_page: u64) {
        let t = self.tick.fetch_add(1, Ordering::Relaxed);
        let mut rec = self.recency.lock().unwrap_or_else(|e| e.into_inner());
        rec.insert(t, (key.clone(), first_page));
    }

    /// Register newly resident bytes and evict older folios if above capacity.
    pub fn add_bytes(&self, bytes: usize) {
        self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.evict_if_needed();
    }

    /// Evict folios in LRU order until `used_bytes <= capacity_bytes`.
    pub fn evict_if_needed(&self) {
        while self.used_bytes.load(Ordering::Relaxed) > self.capacity_bytes {
            let victim = {
                let mut rec = self.recency.lock().unwrap_or_else(|e| e.into_inner());
                let first_key = rec.keys().next().copied();
                if let Some(k) = first_key {
                    rec.remove(&k)
                } else {
                    None
                }
            };

            let Some((obj_key, page)) = victim else {
                break;
            };

            let obj = {
                let objs = self.objects.lock().unwrap_or_else(|e| e.into_inner());
                objs.get(&obj_key).cloned()
            };

            if let Some(obj) = obj {
                let removed_bytes = {
                    let mut folios = obj.folios.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(f) = folios.get(&page) {
                        let st = f.state.lock().unwrap_or_else(|e| e.into_inner());
                        match *st {
                            FolioState::Ready(ref b) => {
                                let len = b.len();
                                drop(st);
                                folios.remove(&page);
                                Some(len)
                            }
                            FolioState::Loading(ref shared_fut) => {
                                if let Some(Ok(ref b)) = shared_fut.peek() {
                                    let len = b.len();
                                    drop(st);
                                    folios.remove(&page);
                                    Some(len)
                                } else {
                                    None
                                }
                            }
                            FolioState::Failed(_) => {
                                drop(st);
                                folios.remove(&page);
                                None
                            }
                        }
                    } else {
                        None
                    }
                };

                if let Some(bytes) = removed_bytes {
                    self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
                }
            }
        }
    }
}

/// Shared data runtime in [`CachedFs`].
pub struct DataRuntime {
    /// Global in-flight bandwidth semaphore.
    pub inflight_semaphore: Arc<Semaphore>,
    /// Global page cache (Mode 2) if configured.
    pub cache: Option<Arc<PageCache>>,
    /// Readahead configuration.
    pub config: ReadaheadConfig,
}

impl DataRuntime {
    /// Create a new runtime from [`DataConfig`].
    pub fn new(config: DataConfig) -> Self {
        let inflight_semaphore = Arc::new(Semaphore::new(config.readahead.max_inflight_bytes));
        let cache = config.cache.map(|c| Arc::new(PageCache::new(c.capacity)));
        Self {
            inflight_semaphore,
            cache,
            config: config.readahead,
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
    data_runtime: Option<Arc<DataRuntime>>,
}

impl<F> CachedFs<F> {
    /// Wrap `inner`.
    pub fn new(inner: F, config: CacheConfig) -> Self {
        let data_runtime = config.data.map(|d| Arc::new(DataRuntime::new(d)));
        Self {
            inner,
            cache: Arc::new(Mutex::new(DirCache::new(config.ttl, config.max_dirs))),
            counters: Arc::default(),
            negative: config.negative,
            populate_from_find: config.populate_from_find,
            data_runtime,
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
            data_runtime: self.data_runtime.as_ref().map(Arc::clone),
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
        if opts.mode.is_write() {
            let key = canonical(path);
            let inner = self.inner.open(path, opts).await?;
            // Appendable (zonal) objects exist from `open`; resumable uploads only
            // from `close`. Invalidate at both points.
            self.invalidated_write(&key);
            return Ok(Box::new(CachedFile {
                inner,
                key,
                cache: Arc::clone(&self.cache),
                counters: Arc::clone(&self.counters),
            }));
        }

        if let Some(runtime) = &self.data_runtime {
            let inner = self.inner.open(path, opts.clone()).await?;
            let stat = inner.stat().cloned();
            let size = inner.size().unwrap_or(0);
            let key = canonical(path);

            let index = if let (Some(stat_ref), Some(cache)) = (&stat, &runtime.cache) {
                let obj_key = ObjectKey {
                    bucket: Arc::from(stat_ref.bucket.as_str()),
                    object: Arc::from(stat_ref.name.as_str()),
                    generation: stat_ref.generation,
                };
                let pages = {
                    let mut objs = cache.objects.lock().unwrap_or_else(|e| e.into_inner());
                    objs.entry(obj_key.clone())
                        .or_insert_with(|| {
                            Arc::new(ObjectPages {
                                key: obj_key,
                                size,
                                folios: Mutex::new(BTreeMap::new()),
                            })
                        })
                        .clone()
                };
                Index::Shared {
                    pages,
                    cache: Arc::clone(cache),
                }
            } else {
                Index::Private {
                    folios: Arc::new(Mutex::new(BTreeMap::new())),
                    cancel_token: Arc::new(Mutex::new(CancellationToken::new())),
                }
            };

            let ra_pages = runtime.config.max_readahead / runtime.config.page_size;
            let ra_state = ReadaheadState::new(opts.read_pattern, ra_pages.max(2));

            Ok(Box::new(ReadaheadFile {
                inner: Arc::from(inner),
                path: key,
                pos: 0,
                size,
                closed: false,
                is_clone: false,
                stat,
                ra_state: Mutex::new(ra_state),
                index,
                runtime: Arc::clone(runtime),
            }))
        } else {
            self.inner.open(path, opts).await
        }
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

/// Storage index for cached and prefetched folios.
pub enum Index {
    /// Mode 1: Private to this handle; drop-behind enabled; abort on seek/drop.
    Private {
        /// Map of start page -> folio.
        folios: Arc<Mutex<BTreeMap<u64, Arc<Folio>>>>,
        /// Token for cancelling active speculative background downloads.
        cancel_token: Arc<Mutex<CancellationToken>>,
    },
    /// Mode 2: Shared across all handles for (bucket, object, generation); global LRU.
    Shared {
        /// Per-object pages directory.
        pages: Arc<ObjectPages>,
        /// Global page cache with LRU eviction.
        cache: Arc<PageCache>,
    },
}

/// Linux v6.6 `file_ra_state` port.
pub struct ReadaheadState {
    /// Start page index of the active readahead window.
    pub start: u64,
    /// Size of the readahead window in pages.
    pub size: usize,
    /// Number of pages at the tail of the window where the `PG_readahead` marker is set.
    pub async_size: usize,
    /// Maximum readahead window size in pages.
    pub ra_pages: usize,
    /// Last read byte offset (-1 if none).
    pub prev_pos: i64,
    /// Access pattern hint.
    pub pattern: ReadPattern,
}

impl ReadaheadState {
    /// Initialize state machine for a newly opened handle.
    pub fn new(pattern: ReadPattern, ra_pages: usize) -> Self {
        Self {
            start: 0,
            size: 0,
            async_size: 0,
            ra_pages,
            prev_pos: -1,
            pattern,
        }
    }

    /// Reset readahead state upon non-contiguous seek or stream discontinuity.
    pub fn reset(&mut self) {
        self.start = 0;
        self.size = 0;
        self.async_size = 0;
        self.prev_pos = -1;
    }
}

/// Decorated read handle providing kernel-style readahead and page caching.
pub struct ReadaheadFile {
    inner: Arc<dyn File>,
    path: String,
    pos: u64,
    size: u64,
    closed: bool,
    is_clone: bool,
    stat: Option<ObjectStat>,
    ra_state: Mutex<ReadaheadState>,
    index: Index,
    runtime: Arc<DataRuntime>,
}

impl ReadaheadFile {
    fn ensure_open(&self) -> Result<()> {
        if self.closed || self.inner.closed() {
            Err(Error::closed(&self.path))
        } else {
            Ok(())
        }
    }

    /// Retrieve or initiate a folio covering `page_idx`.
    async fn get_or_fetch_folio(&self, page_idx: u64) -> Result<Arc<Folio>> {
        let (existing, cancel_token) = match &self.index {
            Index::Private { folios, cancel_token } => {
                let map = folios.lock().unwrap_or_else(|e| e.into_inner());
                let found = map
                    .range(..=page_idx)
                    .next_back()
                    .and_then(|(&start, f)| {
                        if start + (f.num_pages as u64) > page_idx {
                            Some(Arc::clone(f))
                        } else {
                            None
                        }
                    });
                let token = cancel_token.lock().unwrap_or_else(|e| e.into_inner()).clone();
                (found, Some(token))
            }
            Index::Shared { pages, cache } => {
                let map = pages.folios.lock().unwrap_or_else(|e| e.into_inner());
                let found = map
                    .range(..=page_idx)
                    .next_back()
                    .and_then(|(&start, f)| {
                        if start + (f.num_pages as u64) > page_idx {
                            Some(Arc::clone(f))
                        } else {
                            None
                        }
                    });
                if let Some(ref f) = found {
                    cache.record_access(&pages.key, f.first_page);
                }
                (found, None)
            }
        };

        if let Some(folio) = existing {
            let state = folio.state.lock().unwrap_or_else(|e| e.into_inner()).clone();
            match state {
                FolioState::Ready(_) => return Ok(folio),
                FolioState::Failed(err) => return Err(Error::new(err.kind(), err.to_string())),
                FolioState::Loading(shared_fut) => {
                    match shared_fut.await {
                        Ok(b) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Ready(b);
                            drop(st);
                            return Ok(folio);
                        }
                        Err(err) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Failed(Arc::clone(&err));
                            drop(st);
                            return Err(Error::new(err.kind(), err.to_string()));
                        }
                    }
                }
            }
        }

        // Folio is absent: decide window size and initiate synchronous fetch (and optional readahead expansion)
        self.ondemand_readahead(page_idx, 1).await?;

        // After ondemand_readahead, folio should now exist in map
        let created = match &self.index {
            Index::Private { folios, .. } => {
                let map = folios.lock().unwrap_or_else(|e| e.into_inner());
                map.range(..=page_idx)
                    .next_back()
                    .and_then(|(&start, f)| {
                        if start + (f.num_pages as u64) > page_idx {
                            Some(Arc::clone(f))
                        } else {
                            None
                        }
                    })
            }
            Index::Shared { pages, cache } => {
                let map = pages.folios.lock().unwrap_or_else(|e| e.into_inner());
                let found = map
                    .range(..=page_idx)
                    .next_back()
                    .and_then(|(&start, f)| {
                        if start + (f.num_pages as u64) > page_idx {
                            Some(Arc::clone(f))
                        } else {
                            None
                        }
                    });
                if let Some(ref f) = found {
                    cache.record_access(&pages.key, f.first_page);
                }
                found
            }
        };

        if let Some(folio) = created {
            let state = folio.state.lock().unwrap_or_else(|e| e.into_inner()).clone();
            match state {
                FolioState::Ready(_) => Ok(folio),
                FolioState::Failed(err) => Err(Error::new(err.kind(), err.to_string())),
                FolioState::Loading(shared_fut) => {
                    match shared_fut.await {
                        Ok(b) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Ready(b);
                            drop(st);
                            Ok(folio)
                        }
                        Err(err) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Failed(Arc::clone(&err));
                            drop(st);
                            Err(Error::new(err.kind(), err.to_string()))
                        }
                    }
                }
            }
        } else {
            // Fallback: fetch a single folio directly
            self.fetch_folio_range(page_idx, 1, false, cancel_token).await
        }
    }

    /// Linux v6.6 `ondemand_readahead` logic: evaluate streak, compute window, and dispatch folios.
    async fn ondemand_readahead(&self, page_idx: u64, req_pages: usize) -> Result<()> {
        let (start, size, async_size) = {
            let mut ra = self.ra_state.lock().unwrap_or_else(|e| e.into_inner());
            let page_size = self.runtime.config.page_size;
            let total_pages = (self.size.saturating_add(page_size as u64 - 1) / page_size as u64).max(1);

            if ra.pattern == ReadPattern::Random {
                // Random pattern: exact requested page(s) only, no ladder
                let count = req_pages.min((total_pages - page_idx) as usize);
                (page_idx, count.max(1), 0)
            } else if ra.pattern == ReadPattern::Sequential {
                // Sequential hint: start directly at max_readahead
                let win = ra.ra_pages;
                let count = win.min((total_pages - page_idx) as usize);
                let a_size = count / 2;
                ra.start = page_idx;
                ra.size = count;
                ra.async_size = a_size;
                (page_idx, count, a_size)
            } else {
                // Auto mode: Linux on-demand readahead
                let prev_page = if ra.prev_pos >= 0 {
                    (ra.prev_pos as u64) / page_size as u64
                } else {
                    u64::MAX
                };

                let is_sequential = prev_page != u64::MAX && (page_idx == prev_page || page_idx == prev_page + 1);

                if !is_sequential && ra.size == 0 {
                    // Initial read or random seek
                    let initial_win = (req_pages * 4).max(4).min(ra.ra_pages);
                    let count = initial_win.min((total_pages - page_idx) as usize);
                    let a_size = count / 2;
                    ra.start = page_idx;
                    ra.size = count;
                    ra.async_size = a_size;
                    (page_idx, count, a_size)
                } else if is_sequential {
                    // Geometric ramp: double previous window
                    let next_win = if ra.size == 0 {
                        4.min(ra.ra_pages)
                    } else {
                        (ra.size * 2).min(ra.ra_pages)
                    };
                    let count = next_win.min((total_pages - page_idx) as usize);
                    let a_size = count / 2;
                    ra.start = page_idx;
                    ra.size = count;
                    ra.async_size = a_size;
                    (page_idx, count, a_size)
                } else {
                    // Rule 7 (standalone random read): read exact pages without ramp
                    let count = req_pages.min((total_pages - page_idx) as usize);
                    (page_idx, count.max(1), 0)
                }
            }
        };

        self.submit_window(start, size, async_size).await
    }

    /// Submit a window of folios starting at `start` totaling `size` pages.
    /// Sets the `readahead_mark` at `start + size - async_size`.
    async fn submit_window(&self, start: u64, size: usize, async_size: usize) -> Result<()> {
        if size == 0 {
            return Ok(());
        }

        let page_size = self.runtime.config.page_size;
        let io_pages = (self.runtime.config.io_size / page_size).max(1);
        let max_folio_pages = (self.runtime.config.max_folio_size / page_size).max(io_pages);

        // If in sequential pattern, folio size can grow up to max_folio_size
        let ra_pattern = self.ra_state.lock().unwrap_or_else(|e| e.into_inner()).pattern;
        let target_folio_pages = if ra_pattern == ReadPattern::Sequential {
            max_folio_pages
        } else {
            io_pages
        };

        let marker_page = if async_size > 0 && async_size <= size {
            start + (size - async_size) as u64
        } else {
            u64::MAX
        };

        let cancel_token = match &self.index {
            Index::Private { cancel_token, .. } => {
                Some(cancel_token.lock().unwrap_or_else(|e| e.into_inner()).clone())
            }
            Index::Shared { .. } => None,
        };

        let mut curr_page = start;
        let end_page = start + size as u64;

        while curr_page < end_page {
            let remain = (end_page - curr_page) as usize;
            let folio_pages = remain.min(target_folio_pages);

            let has_marker = curr_page <= marker_page && marker_page < curr_page + folio_pages as u64;

            // Check if folio already exists
            let exists = match &self.index {
                Index::Private { folios, .. } => {
                    let map = folios.lock().unwrap_or_else(|e| e.into_inner());
                    map.contains_key(&curr_page)
                }
                Index::Shared { pages, .. } => {
                    let map = pages.folios.lock().unwrap_or_else(|e| e.into_inner());
                    map.contains_key(&curr_page)
                }
            };

            if !exists {
                self.fetch_folio_range(curr_page, folio_pages, has_marker, cancel_token.clone()).await?;
            }

            curr_page += folio_pages as u64;
        }

        Ok(())
    }

    /// Fetch a single folio spanning `[first_page, first_page + num_pages)`.
    async fn fetch_folio_range(
        &self,
        first_page: u64,
        num_pages: usize,
        has_marker: bool,
        token: Option<CancellationToken>,
    ) -> Result<Arc<Folio>> {
        let page_size = self.runtime.config.page_size;
        let start_byte = first_page * page_size as u64;
        let end_byte = (start_byte + (num_pages * page_size) as u64).min(self.size);

        if start_byte >= self.size {
            let folio = Arc::new(Folio {
                first_page,
                num_pages: num_pages as u32,
                readahead_mark: AtomicBool::new(false),
                state: Mutex::new(FolioState::Ready(Bytes::new())),
            });
            self.insert_folio(first_page, Arc::clone(&folio));
            return Ok(folio);
        }

        let inner = Arc::clone(&self.inner);
        let semaphore = Arc::clone(&self.runtime.inflight_semaphore);
        let cache_opt = self.runtime.cache.as_ref().map(Arc::clone);
        let obj_key = if let Index::Shared { pages, .. } = &self.index {
            Some(pages.key.clone())
        } else {
            None
        };

        let fetch_len = (end_byte - start_byte) as usize;

        let fut = async move {
            if let Some(ref t) = token {
                if t.is_cancelled() {
                    return Err(Arc::new(Error::new(crate::ErrorKind::Closed, "task cancelled")));
                }
            }

            let _permit = semaphore
                .acquire_many(fetch_len.max(1) as u32)
                .await
                .map_err(|_| Arc::new(Error::new(crate::ErrorKind::Other, "semaphore closed")))?;

            if let Some(ref t) = token {
                if t.is_cancelled() {
                    return Err(Arc::new(Error::new(crate::ErrorKind::Closed, "task cancelled")));
                }
            }

            // Retry loop for transient failures (up to 3 retries)
            let mut delay = std::time::Duration::from_millis(50);
            let mut last_err = None;

            for _ in 0..3 {
                let range = ByteRange::from(start_byte..end_byte);
                match inner.read_range(range).await {
                    Ok(bytes) => {
                        if let (Some(cache), Some(ref key)) = (&cache_opt, &obj_key) {
                            cache.add_bytes(bytes.len());
                            cache.record_access(key, first_page);
                        }
                        return Ok(bytes);
                    }
                    Err(err) => {
                        let kind = err.kind();
                        let is_retryable = matches!(kind, crate::ErrorKind::Timeout | crate::ErrorKind::Other);
                        last_err = Some(err);
                        if is_retryable {
                            tokio::time::sleep(delay).await;
                            delay *= 3;
                        } else {
                            break;
                        }
                    }
                }
            }

            Err(Arc::new(last_err.unwrap_or_else(|| {
                Error::new(crate::ErrorKind::Other, "backend read failed after retries")
            })))
        };

        let shared_fut = fut.boxed().shared();
        let folio = Arc::new(Folio {
            first_page,
            num_pages: num_pages as u32,
            readahead_mark: AtomicBool::new(has_marker),
            state: Mutex::new(FolioState::Loading(shared_fut.clone())),
        });

        self.insert_folio(first_page, Arc::clone(&folio));

        // Background supervisor task to update FolioState on completion
        let folio_clone = Arc::clone(&folio);
        tokio::spawn(async move {
            match shared_fut.await {
                Ok(bytes) => {
                    let mut st = folio_clone.state.lock().unwrap_or_else(|e| e.into_inner());
                    *st = FolioState::Ready(bytes);
                }
                Err(err) => {
                    let mut st = folio_clone.state.lock().unwrap_or_else(|e| e.into_inner());
                    *st = FolioState::Failed(err);
                }
            }
        });

        Ok(folio)
    }

    fn insert_folio(&self, first_page: u64, folio: Arc<Folio>) {
        match &self.index {
            Index::Private { folios, .. } => {
                let mut map = folios.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(first_page, folio);
            }
            Index::Shared { pages, .. } => {
                let mut map = pages.folios.lock().unwrap_or_else(|e| e.into_inner());
                map.insert(first_page, folio);
            }
        }
    }

    /// Trigger readahead pipeline when touching a folio with `readahead_mark`.
    fn submit_async_readahead(&self) {
        let (start, size, async_size) = {
            let mut ra = self.ra_state.lock().unwrap_or_else(|e| e.into_inner());
            let page_size = self.runtime.config.page_size;
            let total_pages = (self.size.saturating_add(page_size as u64 - 1) / page_size as u64).max(1);

            let next_start = ra.start + ra.size as u64;
            if next_start >= total_pages {
                return;
            }

            let next_win = (ra.size * 2).min(ra.ra_pages).max(4);
            let count = next_win.min((total_pages - next_start) as usize);
            let a_size = count / 2;

            ra.start = next_start;
            ra.size = count;
            ra.async_size = a_size;
            (next_start, count, a_size)
        };

        // Spawn async window submission
        let file = Arc::new(self.clone_handle());
        tokio::spawn(async move {
            let _ = file.submit_window(start, size, async_size).await;
        });
    }

    fn clone_handle(&self) -> Self {
        let ra_st = {
            let ra = self.ra_state.lock().unwrap_or_else(|e| e.into_inner());
            ReadaheadState::new(ra.pattern, ra.ra_pages)
        };
        Self {
            inner: Arc::clone(&self.inner),
            path: self.path.clone(),
            pos: self.pos,
            size: self.size,
            closed: self.closed,
            is_clone: true,
            stat: self.stat.clone(),
            ra_state: Mutex::new(ra_st),
            index: match &self.index {
                Index::Private { folios, cancel_token } => Index::Private {
                    folios: Arc::clone(folios),
                    cancel_token: Arc::clone(cancel_token),
                },
                Index::Shared { pages, cache } => Index::Shared {
                    pages: Arc::clone(pages),
                    cache: Arc::clone(cache),
                },
            },
            runtime: Arc::clone(&self.runtime),
        }
    }
}

#[async_trait]
impl File for ReadaheadFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn mode(&self) -> OpenMode {
        OpenMode::Read
    }

    fn closed(&self) -> bool {
        self.closed || self.inner.closed()
    }

    fn tell(&self) -> u64 {
        self.pos
    }

    fn size(&self) -> Option<u64> {
        Some(self.size)
    }

    fn stat(&self) -> Option<&ObjectStat> {
        self.stat.as_ref()
    }

    fn readable(&self) -> bool {
        true
    }

    fn writable(&self) -> bool {
        false
    }

    fn seekable(&self) -> bool {
        true
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.ensure_open()?;
        let target = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(delta) => self.size as i64 + delta,
            SeekFrom::Current(delta) => self.pos as i64 + delta,
        };

        if target < 0 {
            return Err(Error::invalid_range("seek before the start of the file"));
        }

        let new_offset = (target as u64).min(self.size);
        let forward_gap = 8 * 1024 * 1024; // 8 MiB forward gap tolerance

        if let Index::Private { folios, cancel_token } = &self.index {
            if new_offset >= self.pos && (new_offset - self.pos) <= forward_gap {
                // Forward gap within 8 MiB: preserve in-flight tasks and readahead ladder
                let page_size = self.runtime.config.page_size;
                let target_page = new_offset / page_size as u64;
                let mut map = folios.lock().unwrap_or_else(|e| e.into_inner());
                map.retain(|&start_page, f| start_page + (f.num_pages as u64) > target_page);
            } else {
                // Backward seek or large forward jump > 8 MiB: abort tasks and reset
                let mut token_guard = cancel_token.lock().unwrap_or_else(|e| e.into_inner());
                token_guard.cancel();
                *token_guard = CancellationToken::new();

                let mut ra = self.ra_state.lock().unwrap_or_else(|e| e.into_inner());
                ra.reset();

                let page_size = self.runtime.config.page_size;
                let target_page = new_offset / page_size as u64;
                let mut map = folios.lock().unwrap_or_else(|e| e.into_inner());
                map.retain(|&start_page, f| start_page + (f.num_pages as u64) > target_page);
            }
        }

        self.pos = new_offset;
        Ok(self.pos)
    }

    async fn read(&mut self, len: Option<usize>) -> Result<Bytes> {
        self.ensure_open()?;
        let target_len = len
            .unwrap_or((self.size.saturating_sub(self.pos)) as usize)
            .min((self.size.saturating_sub(self.pos)) as usize);

        if target_len == 0 {
            return Ok(Bytes::new());
        }

        let page_size = self.runtime.config.page_size;
        let mut buf = BytesMut::with_capacity(target_len);

        while buf.len() < target_len && self.pos < self.size {
            let page_idx = self.pos / page_size as u64;
            let folio = self.get_or_fetch_folio(page_idx).await?;

            if folio.readahead_mark.swap(false, Ordering::Relaxed) {
                self.submit_async_readahead();
            }

            let folio_data = {
                let st = folio.state.lock().unwrap_or_else(|e| e.into_inner()).clone();
                match st {
                    FolioState::Ready(b) => b,
                    FolioState::Failed(err) => return Err(Error::new(err.kind(), err.to_string())),
                    FolioState::Loading(shared_fut) => match shared_fut.await {
                        Ok(b) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Ready(b.clone());
                            b
                        }
                        Err(err) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Failed(Arc::clone(&err));
                            return Err(Error::new(err.kind(), err.to_string()));
                        }
                    },
                }
            };

            let offset_in_folio = (self.pos - (folio.first_page * page_size as u64)) as usize;
            if offset_in_folio >= folio_data.len() {
                break;
            }

            let avail = folio_data.len() - offset_in_folio;
            let take = avail.min(target_len - buf.len());
            buf.extend_from_slice(&folio_data[offset_in_folio..offset_in_folio + take]);
            self.pos += take as u64;

            // Maintain sequential readahead ladder continuity across folio boundaries
            {
                let mut ra = self.ra_state.lock().unwrap_or_else(|e| e.into_inner());
                ra.prev_pos = self.pos as i64;
            }
        }

        // Update last-read byte position in readahead state
        {
            let mut ra = self.ra_state.lock().unwrap_or_else(|e| e.into_inner());
            ra.prev_pos = self.pos as i64;
        }

        // Drop-behind: in Mode 1, remove folios strictly behind the current cursor
        if let Index::Private { folios, .. } = &self.index {
            let current_page = self.pos / page_size as u64;
            let mut map = folios.lock().unwrap_or_else(|e| e.into_inner());
            map.retain(|&start_page, f| start_page + (f.num_pages as u64) > current_page);
        }

        Ok(buf.freeze())
    }

    async fn read_range(&self, range: ByteRange) -> Result<Bytes> {
        self.ensure_open()?;
        let clamped = range.clamp(self.size);
        if clamped.is_empty() {
            return Ok(Bytes::new());
        }

        let page_size = self.runtime.config.page_size;
        let start_page = clamped.start / page_size as u64;
        let end_page = (clamped.end + page_size as u64 - 1) / page_size as u64;

        // In Mode 1: check private folios for hit; if miss, pass through directly
        if let Index::Private { folios, .. } = &self.index {
            let mut all_ready = true;
            {
                let map = folios.lock().unwrap_or_else(|e| e.into_inner());
                for p in start_page..end_page {
                    let found = map
                        .range(..=p)
                        .next_back()
                        .is_some_and(|(&start, f)| start + (f.num_pages as u64) > p);
                    if !found {
                        all_ready = false;
                        break;
                    }
                }
            }

            if !all_ready {
                return self.inner.read_range(clamped.into()).await;
            }
        }

        // In Mode 2 (or Mode 1 hit): assemble from folios
        let mut buf = BytesMut::with_capacity((clamped.end - clamped.start) as usize);
        let mut cursor = clamped.start;

        while cursor < clamped.end {
            let p = cursor / page_size as u64;
            let folio = self.get_or_fetch_folio(p).await?;

            let folio_data = {
                let st = folio.state.lock().unwrap_or_else(|e| e.into_inner()).clone();
                match st {
                    FolioState::Ready(b) => b,
                    FolioState::Failed(err) => return Err(Error::new(err.kind(), err.to_string())),
                    FolioState::Loading(shared_fut) => match shared_fut.await {
                        Ok(b) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Ready(b.clone());
                            b
                        }
                        Err(err) => {
                            let mut st = folio.state.lock().unwrap_or_else(|e| e.into_inner());
                            *st = FolioState::Failed(Arc::clone(&err));
                            return Err(Error::new(err.kind(), err.to_string()));
                        }
                    },
                }
            };

            let offset_in_folio = (cursor - (folio.first_page * page_size as u64)) as usize;
            if offset_in_folio >= folio_data.len() {
                break;
            }

            let avail = folio_data.len() - offset_in_folio;
            let take = avail.min((clamped.end - cursor) as usize);
            buf.extend_from_slice(&folio_data[offset_in_folio..offset_in_folio + take]);
            cursor += take as u64;
        }

        Ok(buf.freeze())
    }

    async fn write(&mut self, _data: Bytes) -> Result<()> {
        Err(Error::unsupported("write on a read handle"))
    }

    async fn flush(&mut self) -> Result<()> {
        self.ensure_open()
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        if !self.is_clone {
            if let Index::Private { folios, cancel_token } = &self.index {
                cancel_token.lock().unwrap_or_else(|e| e.into_inner()).cancel();
                folios.lock().unwrap_or_else(|e| e.into_inner()).clear();
            }
        }

        Ok(())
    }

    async fn discard(&mut self) -> Result<()> {
        self.close().await
    }
}

impl Drop for ReadaheadFile {
    fn drop(&mut self) {
        if !self.is_clone {
            if let Index::Private { folios, cancel_token } = &self.index {
                cancel_token.lock().unwrap_or_else(|e| e.into_inner()).cancel();
                folios.lock().unwrap_or_else(|e| e.into_inner()).clear();
            }
        }
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
        assert_send_sync::<ReadaheadFile>();
        assert_send_sync::<DataRuntime>();
        assert_send_sync::<PageCache>();
    }
}
