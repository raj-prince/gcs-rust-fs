# Readahead and page cache design for `gcs-rust-fs`

Status: **proposal — nothing implemented yet; decisions in
[§10](#10-decisions) are open.** The directory half of `CachedFs` is built
and described in [`dir_cache_design.md`](dir_cache_design.md); this document
specifies the data half.

Goal: per-object data caching and prefetching that makes sequential reads
fast (checkpoint loading) without making random reads slow, modelled on the
Linux page cache and on-demand readahead rather than on fsspec's per-handle
block cache or the gcsfs fork's streaming `BackgroundPrefetcher`.

Sources: [`mm/readahead.c` at
v6.6](https://github.com/torvalds/linux/blob/v6.6/mm/readahead.c), the
current [`GcsFile`](../src/gcs/file.rs) read path, and the gcsfs fork's
(`raj-prince/gcsfs`) `gcsfs/prefetcher.py` and `zonal_file.py` — referred to
below as "the fork".

---

## 0. What exists today (and why it is not enough)

| Layer | Behaviour | Problem |
|---|---|---|
| `GcsFile::read` | one private 5 MiB buffer per handle (`block_size`); small reads pull a block, large reads fetch exactly | no pipelining: every block is a stall; no sharing between handles or re-opens; `read_range` is never buffered |
| `GcsFs::cat_file` | exactly one range request per call | a 1 GiB read is one single-stream request |
| fork `BackgroundPrefetcher` (Python) | producer/consumer queue per handle; prefetch size = `(streak − 2) × avg_io`, capped at 128 MiB; chunks ≥ 5 MiB; concurrency by splitting; hard seek resets | linear ramp; consumed bytes are discarded (no re-read hits, no cross-handle sharing); state machine tied to one cursor, so `pread`-style access defeats it |

The kernel design fixes exactly these: a **shared cache** indexed by page, a
**per-open readahead state** that ramps geometrically, and a **marker page**
that submits the next window before the reader stalls.

---

## 1. The kernel algorithm, precisely (v6.6)

Two separate structures:

* **Page cache** — per inode (`address_space`), shared by every open file and
  by `mmap`. Pages are 4 KiB, indexed by page number, evicted by memory
  pressure (LRU, inactive → active on second touch). A page being read is
  *locked*; other readers wait on it instead of issuing a second I/O.
* **Readahead state** — per open file (`struct file_ra_state`):
  `start, size, async_size` (the last window), `ra_pages` (max window, from
  `read_ahead_kb`, default 128 KiB = 32 pages), `prev_pos` (last byte of the
  previous read; initialised to −1).

```
                       |<----- async_size ---------|
    |------------------- size -------------------->|
    |==================#===========================|
    ^start             ^page marked PG_readahead
```

**Triggers** (`filemap_read` → `filemap_get_pages`):

1. requested page **not in cache** → `page_cache_sync_ra` (synchronous: the
   reader will wait for the first page);
2. requested page **present and marked `PG_readahead`** → clear the mark,
   `page_cache_async_ra` (the next window is submitted while the reader is
   still consuming cached pages — "readahead pipelining").

**Decision ladder** (`ondemand_readahead`, `index` = first requested page,
`req_size` = pages requested, `max` = `ra_pages`):

| # | Condition | Action |
|---|---|---|
| 0 | `req_size > max` and the device allows bigger I/O | raise `max` to `min(req_size, io_pages)` |
| 1 | `index == 0` (start of file) | **initial**: `start = 0; size = init(req, max); async_size = size − req` |
| 2 | `index == start + size − async_size` (the marker) or `index == start + size` (ran off the end) | **sequential**: `start += size; size = next(size, max); async_size = size` |
| 3 | marker hit but state does not match (interleaved streams) | window = distance to the next uncached page + `req`, then `next()`; `async_size = size` |
| 4 | `req_size > max` | initial |
| 5 | `index − page(prev_pos) ≤ 1` (continues the previous read, cache miss) | initial |
| 6 | `try_context_readahead`: count contiguous cached pages just below `index`; if more than `req_size`, treat as a stream (×2 when it reaches the file start), `async_size = 1` | window of that size |
| 7 | otherwise: **standalone random read** | read exactly the requested pages, *do not touch* `ra` |

After 1–6, "readit": if the window begins at `index` and is fully async
(`size == async_size`), merge the following window in now
(`size += next(size)`, capped at `max`, `async_size = max/2` at the cap).
Then submit the window, **skipping pages already present or in flight** (an
existing page splits the batch), and set `PG_readahead` on the page at
`start + size − async_size`.

**Window sizes** (`get_init_ra_size`, `get_next_ra_size`), in pages:

```
init(req, max):  n = next_pow2(req);  n ≤ max/32 → 4n;  n ≤ max/4 → 2n;  else max
next(cur, max):  cur < max/16 → 4·cur;  cur ≤ max/2 → 2·cur;  else max
```

For `max = 32`: a 1-page read at offset 0 gets 4 pages, then 16, then 32 —
aggressive at first, capped quickly.

**Folios** (`page_cache_ra_order`, since 5.18): once a window is ≥ 4 pages
the kernel allocates *large folios* instead of single pages — one
allocation, one lock, one LRU entry and one `PG_readahead` bit covering
2^order pages. The order is seeded by the folio that triggered the
readahead (0 for a sync miss) and grows by 2 per window (×4), capped by
`MAX_PAGECACHE_ORDER` and by the window size. Folios are naturally aligned:
a window starting mid-way is filled with smaller folios until the index is
aligned (`order = __ffs(index)`), and the last one is shrunk to stop at
EOF. The XArray holds one multi-index entry per folio, so a lookup of any
page inside it returns the folio. The mark goes on the folio whose first
page is `round_up(start + size − async_size, 2^order)`; async readahead is
then invoked with that folio's first page as `index` and rule 2 compares
against the marker rounded up the same way (`expected = round_up(…, 1 <<
order)`). `filemap_readahead` fires on a marked folio *before* waiting for
it to become up to date, so a reader that catches up with its own window
still submits the next one. If a page already exists inside the window,
`filemap_add_folio` fails and the window falls back to the single-page
path, which skips present pages.

**Advice** (`fadvise`): `SEQUENTIAL` doubles `ra_pages`; `RANDOM` sets
`FMODE_RANDOM` → every read becomes "read exactly these pages"; `WILLNEED`
forces a window; `DONTNEED` drops pages. `O_DIRECT` bypasses the cache.

Two properties worth keeping in mind for the translation:

* **The page is the index unit, the folio the allocation unit, the window
  the I/O unit.** The kernel indexes 4 KiB pages, allocates folios of up to
  2 MiB, and submits the whole window as one batch, which the block layer
  merges into large requests.
* **Readahead never duplicates I/O.** Pages present or locked are skipped;
  concurrent readers wait on the locked page.

---

## 2. Translation to object storage

| Kernel | Here | Why it differs |
|---|---|---|
| page = 4 KiB | `page_size` = **256 KiB** (config) — the *index* unit, not the I/O unit | the smallest extent a random read pays for: a 4 KiB `pread` costs one page ≈ 2 ms (first-byte latency dominates below this size). I/O size is set by folios and windows, so a small page does not raise request counts |
| folio = 2^order pages, naturally aligned, one lock + one mark | `Folio` = **one range response**: 1 … `io_size / page_size` pages, *any* extent | the alignment exists for PMD mappings and XArray multi-index entries; without it a 16-page window starting at page 7 is one request instead of 1 + 8 + 4 + 2 + 1. A `BTreeMap` keyed by first page stores arbitrary extents for free |
| `ra_pages` = 128 KiB | `max_readahead` = **64 MiB** per handle (256 pages) | bandwidth-delay product of GCS ≫ a disk's; fork caps at 128 MiB, gcsfuse at 200 MiB |
| block layer merges pages into bios, queue depth gives parallelism | window carved into folios of ≤ `io_size` (**8 MiB**) — one range request each — ≤ `max_inflight` (**8**) concurrent per handle, global byte cap on in-flight data | one GCS stream does not saturate a NIC; the fork and gcsfuse both split for the same reason |
| `address_space` per inode | `ObjectPages` per **(bucket, object, generation)** | a generation is immutable, so pages never need invalidation — a rewrite is a new key, old pages age out |
| memory pressure | byte budget, **LRU over folios** (default 512 MiB per `CachedFs`) | explicit, because the bridge process also holds tensors |
| locked folio | `FolioState::Loading(shared future)` | same semantics: waiters join the in-flight read; on error the folio is removed and the next reader retries synchronously |
| `PG_readahead` on the folio | `readahead_mark` on the folio (not on the handle); a read touching *any* page of it triggers the async window | same reason as the kernel: interleaved streams and other handles must trigger the right window |
| `file_ra_state` in `struct file` | `ReadaheadState` behind a `Mutex` in the read handle | `read_range(&self)` is concurrent |
| `fadvise` | `OpenOptions::read_pattern: Auto / Sequential / Random` (decision 6) | — |
| `O_DIRECT` | not wrapped (no page cache configured) | — |

Window ramp with these defaults (`max` = 256 pages): 256 KiB reads from
offset 0 get windows of **1, 4, 8, 16, 32, 64, 64 … MiB**; 1 MiB reads start
at 4 MiB. Each window is carved into ≤ 8 MiB folios (one request each) and
submitted while the previous one is being consumed; steady state has ≤ 64
MiB in flight as 8 parallel 8 MiB requests. A 1 GiB sequential read issues
~135 requests of 8 MiB instead of 205 serial 5 MiB fetches.

Random reads: a 4 KiB `read_range` at an arbitrary offset costs one 256 KiB
single-page folio (rule 7, no state change) — ≈ 2 ms, against today's
`read()` (5 MiB) or `read_range()` (exact). That is the page-size trade-off;
see decision 2 and the `Random` hint.

**What the cache does not know about and does not need to:** gRPC vs HTTP,
descriptors, generations beyond the key. It drives everything through
`File::read_range` on the wrapped handle (positional, concurrent-safe,
already pinned to a generation).

---

## 3. Placement: the data half of `CachedFs`

| Option | For | Against |
|---|---|---|
| **(a) `CachedFs` gains a page cache** (recommended) | one decorator = "the VFS caches" (dir cache + page cache, like the kernel's dentry cache + page cache); testable on `MemoryFs` with no network; `GcsFs` stays stateless (backends cache nothing — D30); works for any `FileSystem` | the decorator cannot pool gRPC descriptors — but that is a `GcsFile` concern (see §7) |
| (b) inside `GcsFs`/`Backend` | closest to the kernel's layering | state in the backend; untestable without GCS; duplicates work when another backend appears |
| (c) a second decorator `PageCachedFs` | separation | two wrappers to compose in the bridge for no benefit |

With (a), `CacheConfig` becomes:

```rust
pub struct CacheConfig {
    pub dir:  Option<DirCacheConfig>,     // today's fields: ttl, max_dirs, negative, populate_from_find
    pub data: Option<DataConfig>,         // None = reads pass straight through (today's behaviour)
}
pub struct DataConfig {
    pub readahead: ReadaheadConfig,       // always on when `data` is set
    pub cache: Option<PageCacheConfig>,   // None = readahead only (see below)
}
pub struct ReadaheadConfig {
    pub page_size: usize,          // 256 KiB   index unit: readahead arithmetic, smallest fetch
    pub max_readahead: usize,      // 64 MiB    ra_pages: largest window per handle
    pub io_size: usize,            // 8 MiB     largest folio = largest single range request
    pub max_inflight: usize,       // 8         concurrent requests (folios) per handle
    pub max_inflight_bytes: usize, // 256 MiB   global cap on data in flight, both modes
}
pub struct PageCacheConfig {
    pub capacity: usize,           // 512 MiB   byte budget, LRU over folios
    // phase 2: store (memory | node-local directory), §4.2
}
```

**Two modes, one engine.** Readahead decides *when and how much to fetch
ahead*; the page cache decides *how long to keep it and who may see it*.
The kernel bundles them; here they are separate so that readahead can run
without any caching — the kernel's readahead followed by `fadvise(DONTNEED)`
behind the cursor (`nocache`, rsync `--drop-cache`), and the default read
path of gcsfuse, Mountpoint and the fork's `BackgroundPrefetcher`:

| | **readahead only** (`cache: None`) | **page cache** (`cache: Some`) |
|---|---|---|
| who owns the folio index (§4) | the handle: a private `BTreeMap<PageIndex, Folio>` | `PageCache`: one `ObjectPages` per (bucket, object, generation) |
| retention | **drop-behind**: after each read, folios ending at or before the read's last page are dropped | LRU under `capacity` (§4.1) |
| sharing | none — two handles on one object both fetch; concurrent `pread`s on the *same* handle still share in-flight folios | across handles; across processes with `DirStore` (§4.2) |
| in-flight work at `close` | aborted — nobody can use it | completed (decision 8) |
| memory | per handle ≤ window being read + next window ≈ 2 × `max_readahead`, zero after `close`; only `max_inflight_bytes` is global | the shared budget |
| gives up | re-read hits, dedup across handles and ranks, warm reopen | — |
| right for | one-pass loading (a page cache's hit rate on a single sequential pass is zero) | ranks on a node reading the same shards, re-reads, random access to hot regions |

Everything else — the ladder, folios as one response, floor lookup and
carve, `Loading` futures, marks, parallel ≤ `io_size` requests — is
identical; the modes differ in who owns the index and in the retention hook
that runs after a read (`Index::Private` vs `Index::Shared` in §4), not in a
second engine.

`CachedFs::open` for read modes wraps the inner `Box<dyn File>` as
`Arc<dyn File>` in a `ReadaheadFile`. In page-cache mode the key comes from
`File::stat()` (`bucket`, `name`, `generation`, `size`) — if a backend
returns `stat() == None` the handle falls back to readahead-only, which
needs no key. Write handles are unchanged.

---

## 4. Data structures

```rust
struct PageCache {                                   // one per CachedFs, behind a Mutex
    objects: HashMap<ObjectKey, Arc<ObjectPages>>,   // (bucket, object, generation)
    lru: LruIndex<(ObjectKey, PageIndex)>,           // folio-granular recency (first page), bytes accounted
    used: usize, capacity: usize,
    inflight: Arc<Semaphore>,                        // max_inflight_bytes, in page units
}

struct ObjectPages {
    size: u64,                                       // object size the key was created with
    folios: Mutex<BTreeMap<PageIndex, Folio>>,       // key = first page; extents never overlap
}

struct Folio {                                       // one range request = one entry
    pages: u32,                                      // 1 ..= io_size / page_size (shorter at EOF)
    readahead_mark: bool,                            // PG_readahead; honoured whether Loading or Ready
    referenced: bool,                                // touched by a reader since it was submitted
    state: FolioState,
}

enum FolioState {
    Loading(Shared<BoxFuture<'static, Result<Bytes, Arc<Error>>>>),  // locked folio
    Ready(Bytes),                                                     // uptodate folio
}

struct ReadaheadState {        // per read handle, kernel field for field
    start: u64, size: u64, async_size: u64,   // pages
    max_pages: u64,                           // max_readahead / page_size (256), ×2 for Sequential
    prev_pos: Option<u64>,                    // last byte of the previous read
}

struct ReadaheadFile {
    inner: Arc<dyn File>,              // read_range is the only I/O entry point
    index: Index,                      // who owns the folios — the only difference between the modes
    ra: Mutex<ReadaheadState>, pos: u64, page_size: usize, io_size: usize,
    inflight: Arc<Semaphore>,          // max_inflight_bytes, shared by every handle of the CachedFs
    random: bool,                      // ReadPattern::Random → rule 7 for every read
}

enum Index {
    Private(Mutex<BTreeMap<PageIndex, Folio>>),              // readahead only: drop-behind, aborted at close
    Shared { pages: Arc<ObjectPages>, cache: Arc<Mutex<PageCache>> },  // page cache: LRU, completes after close
}
```

**How "is this range cached?" is answered.** The map holds non-overlapping
extents keyed by first page, so the folio with the largest `first ≤ p` is
the only one that can contain `p`, and a range is checked by walking from
that entry until the first hole:

```rust
fn lookup(folios: &BTreeMap<PageIndex, Folio>, p: PageIndex) -> Option<(PageIndex, &Folio)> {
    folios.range(..=p).next_back()                        // floor query, O(log n)
        .filter(|(first, f)| *first + f.pages as u64 > p) // does the extent reach p?
        .map(|(first, f)| (*first, f))
}

/// Holes in `w` — empty means fully covered. Also the carve for `submit`.
fn gaps(folios: &BTreeMap<PageIndex, Folio>, w: Range<PageIndex>) -> Vec<Range<PageIndex>> {
    let mut cursor = w.start;
    if let Some((first, f)) = lookup(folios, cursor) {     // folio straddling the start
        cursor = (first + f.pages as u64).min(w.end);
    }
    let mut out = Vec::new();
    for (first, f) in folios.range(cursor..w.end) {        // folios starting inside w
        if *first > cursor { out.push(cursor..*first); }
        cursor = first + f.pages as u64;
    }
    if cursor < w.end { out.push(cursor..w.end); }
    out
}
```

O(log n + k) for k folios touched — one descent and one comparison for a
1 MiB read over 8 MiB folios. The byte range is clamped to `ObjectPages.size`
first, then `first = start / page_size`, `last = (end − 1) / page_size`; the
slice offset inside a folio is `start − first × page_size`, so "is it
cached" and "where are the bytes" are the same operation (a presence bitmap
would answer only the first and still need the map). "Cached" has three
answers, not two: a covering folio may be `Loading` — not readable yet, but
never fetched again; `gaps` counts it as covered and the reader joins its
future, as the kernel skips locked pages.

**Why a `BTreeMap`.** Every query on the index is ordered: the floor query
above (the `BTreeMap` playing the XArray's multi-index entry); the **window
carve** = `gaps(start..start + size)`; **context readahead**
(`count_history_pages`) walks `range(..index).rev()` while the extents stay
contiguous; **next-miss** (rule 3) walks forward the same way. Entries never
overlap — the invariant that makes an ordered map sufficient where overlaps
would need an interval tree: every insert goes through the carve under the
map lock (no `.await` while held) and fills only holes; eviction removes
whole entries; a zonal size bump replaces the whole `ObjectPages`.

**Folio = one response.** `submit` takes the window `[start, start + size)`,
removes every page already covered by a `Ready` or `Loading` folio, cuts
each remaining run into pieces of ≤ `io_size`, and inserts one `Loading`
folio per piece — that folio *is* the range request, and its `Bytes` is
stored as received. The kernel's power-of-two alignment is dropped
deliberately: it exists so large folios can be PMD-mapped and stored as
multi-index entries; here it would turn a 16-page window starting at page 7
into five requests (1 + 8 + 4 + 2 + 1) instead of one. A pre-existing folio
inside the window splits a run exactly as a present page splits the kernel's
batch. The mark is set on whichever folio covers `start + size − async_size`
after the carve.

Eviction removes a whole `Ready` folio from the map; `Bytes` slices already
handed to readers keep the allocation alive until they are dropped
(refcount), so no pinning is needed. `Loading` folios are never evicted. The
byte accounting is exact — a folio is one allocation — and counts `Ready`
plus `Loading` bytes.

The last folio of an object is short (the carve stops at EOF). For
finalized objects that is fine. For an **unfinalized zonal object** (size
still growing) a handle opened later sees a larger `size`: the key matches
but `ObjectPages.size` is smaller, so the entry is replaced — the stale
short tail can never be served to a reader that knows the object is longer.

### 4.1 Memory pressure

What the kernel has that we do not: it owns all memory, so clean page-cache
pages are simply the first thing reclaim takes, charged to the cgroup that
read them. Readahead on top of that is *best-effort*: if a page allocation
fails the window is cut short silently (`ra_alloc_folio` → `-ENOMEM`), and
`count_history_pages` doubles as a thrashing detector — when read-ahead
pages are evicted before use, the next window is sized from what survived.
Under I/O congestion async readahead is skipped altogether.

A library cannot be reclaimed by the kernel, so pressure is handled in two
layers: **inside the budget** (the cache's job) and **the budget itself**
(the embedding process's job).

In **readahead-only mode** there is no budget to manage: a handle holds at
most the window it is reading plus the next one (≈ 2 × `max_readahead`,
128 MiB with the defaults, and only while it is actually streaming — a
handle that reads 100 KiB holds a 1 MiB window), drop-behind releases
folios as the cursor passes them, and `close` aborts the rest. The only
cross-handle limit is `max_inflight_bytes`, which rule 4 below applies in
both modes. Everything else in this section is about page-cache mode.

**Inside the budget — six rules**

| # | Rule | Kernel counterpart |
|---|---|---|
| 1 | `capacity` is a hard ceiling that counts `Ready` folios **and** `Loading` folios. Nothing is ever submitted that would overshoot it. | reclaim before allocation |
| 2 | Readahead is **demand-driven**: a window is submitted only when the reader reaches the marker, so a slow consumer stalls prefetch at ≤ one window ahead. Per handle: ≤ `2 × max_readahead` resident (current + next window); never a free-running producer. | on-demand readahead; no queue |
| 3 | **Eviction order**: `referenced` folios (a reader has touched them), oldest touch first; only then unreferenced read-ahead folios, most recently submitted first (for a stream, that is the farthest ahead). Evicting an unreferenced folio increments `stats.thrash`. | inactive list is FIFO; first touch does not reorder, so consumed stream pages age out before the pages ahead of the cursor |
| 4 | **Window admission** is best-effort: before submitting, `window = min(window, free_after_evicting_referenced / active_streams)`. The pages the reader actually asked for are always admitted (evicting as needed); if even they do not fit — `capacity` smaller than one request — the read is served **uncached**, never failed. | `-ENOMEM` truncates the window; the requested page is still read |
| 5 | Thrashing feedback: when rule 3 had to evict unreferenced folios, the effective `max_pages` for new windows halves; it doubles back (up to the configured value) after a window completes without such evictions. | `count_history_pages` sizing, `ra_pages` reduction in older kernels |
| 6 | **One allocation per folio, zero copies.** A folio is exactly the `Bytes` of one range response; evicting it frees its memory and the accounting is exact. The alternatives are both worse: slicing a response into per-page `Bytes` keeps the whole response alive until the last slice is evicted, and copying it into fixed-size pages costs ≈ 1 ms per 8 MiB for nothing. | the folio is the allocation unit |

`max_inflight_bytes` sits inside `capacity` and bounds how much of it can be
speculative data in flight at any moment across all handles.

**The budget itself — exposed, not guessed**

* `capacity` is configuration; the default (512 MiB) is a number, not a
  policy. The bridge knows the cgroup limit (`memory.max`) and the trainer's
  shape; it should size the budget.
* `CachedFs::shrink_to(bytes)` / `set_capacity(bytes)` evict immediately
  and take effect for the next window — callable from Python before a large
  allocation, from a cgroup `memory.pressure` (PSI) watcher, or from
  `invalidate_cache(None)`, which also drops pages.
* `CacheStats` exposes `page_bytes`, `inflight_bytes`, `thrash`, so pressure
  is observable rather than inferred from RSS.
* What the budget does **not** cover: `Bytes` the caller still holds. In the
  PyO3 bridge each read is copied into a Python `bytes`, so the Rust buffer
  is released immediately; peak memory is `capacity + Python copies`, and
  the Python side is the consumer's to bound (`read(len)` sizes).

Watching `memory.pressure`/RSS from inside the library is deliberately left
out of the core: it is policy, platform-specific, and the hooks above let the
bridge implement it in a few lines.

### 4.2 Sharing the cache across processes on a node

Eight ranks per node each holding 512 MiB of the same checkpoint pages is
4 GiB for one copy's worth of data, and DataLoader workers multiply it.
Three ways to share, in increasing order of machinery:

| | **A. Node-local directory** (local SSD, or tmpfs) | **B. Cache daemon** (one process owns the cache, clients over a Unix socket) | **C. Shared-memory segment** (mmap'd arena + shared index) |
|---|---|---|---|
| who shares | the kernel page cache: one copy per node, visible to every process, **and** reclaimable under pressure when backed by disk | the daemon's budget; clients receive bytes (copy) or an fd to a page file (`SCM_RIGHTS`, zero-copy) | the segment; pinned RAM |
| in-flight dedup | `O_EXCL` marker file per folio; others wait (inotify/poll) with a staleness timeout, then fetch themselves | exact — one owner | needs cross-process locks that survive a crashed holder (robust futexes) |
| eviction | approximate LRU: byte budget + sweep by `atime`, run by whoever notices overflow under a lock file | exact LRU | custom allocator + index in shared memory |
| crash of one process | a `.tmp` file to sweep | daemon restart = cold cache; client restart = nothing | possibly corrupted index |
| survives job restart on the node | **yes** (warm cache after preemption/crash) | only if disk-backed | no |
| hit latency | page-cache read ≈ 0.1 ms/MiB copy; SSD miss ≈ 100 µs + NVMe bandwidth | UDS round trip ≈ 30 µs + copy, or mmap of a passed fd | memcpy |
| operational cost | a directory and a size cap | lifecycle (systemd/DaemonSet or first-process spawn + lock-file leader) | a lot of unsafe code; no mature Rust crate does this crash-safely |
| prior art | gcsfuse file cache, Mountpoint `--cache`, JuiceFS/Alluxio block stores | Alluxio worker, tf.data service, gcsfuse itself (FUSE is the socket) | PostgreSQL shared buffers, Varnish |

**Recommendation: make the page store pluggable now, ship A as phase 2.**

```rust
#[async_trait]
trait PageStore: Send + Sync {
    async fn get(&self, key: &ObjectKey, page: PageIndex) -> Option<(PageIndex, Bytes)>; // folio covering `page`: (first page, bytes)
    async fn put(&self, key: &ObjectKey, first: PageIndex, bytes: Bytes);                // one folio
    fn claim(&self, key: &ObjectKey, first: PageIndex, pages: u32) -> Claim;             // in-flight marker: Owner | Wait(notifier)
    async fn shrink_to(&self, bytes: usize);
}
```

* `MemoryStore` — phase 1, exactly §4.
* `DirStore(path)` — phase 2. Layout
  `<dir>/<bucket>/<object-hash>/<generation>/<first>-<pages>`, one file per
  folio, written to `.tmp` and `rename`d (atomic visibility, no torn
  folios); `claim` creates `<first>-<pages>.inflight` with `O_EXCL`;
  `shrink_to` deletes by `atime` under `<dir>/.sweep.lock`. Each process
  keeps the §4 `BTreeMap` index for the directory (a floor lookup needs
  ordering, which a directory does not give), refreshed by one `readdir` on
  a miss before fetching, so folios written by other ranks are found. Two
  processes that carve overlapping extents in the same instant both fetch
  the overlap — wasted bandwidth, not a correctness problem (both hold valid
  bytes of an immutable generation); an imported folio is trimmed to the
  pages the index does not already cover, so lookups stay exact. The write
  happens *after* the reader is served (`spawn_blocking`), so the first
  reader never waits for the disk. Readahead state, window logic and the
  in-process `Loading` futures are unchanged — only where a `Ready` folio
  lives differs.

Readahead stays per handle in each process (as `file_ra_state` is per open
file); the store is what is shared. Two ranks streaming the same object both
ramp their windows, and the marker files make one of them fetch each folio
while the other waits — the kernel's locked-page rule across processes.

Two caveats worth deciding with eyes open:

* **tmpfs is not reclaimable without swap** — `/dev/shm` pages are pinned RAM, so a tmpfs-backed `DirStore` moves the budget problem from per-process to per-node (still N× better) but does not make it go away. **Local SSD does**: file pages are clean page cache, the kernel drops them under pressure and re-reads from disk. On GCE/GKE training nodes with local NVMe this is the configuration that answers §4.1 completely.
* Writing every fetched folio costs write bandwidth (NVMe: GB/s, fine) and the cache directory is readable by any process of the same user — the same trust domain as the credentials; create it `0700`.

B is the escalation if measurements show the approximate LRU or marker-file
dedup is not good enough; the `PageStore` boundary is where it would plug in
(`DaemonStore` speaking over a UDS). C is not recommended.

Fork-based sharing (DataLoader workers inheriting a populated cache
copy-on-write) is accidental, breaks on the first LRU touch, and does not
survive a tokio runtime across `fork` — not a strategy.

---

## 5. Read path (`read(len)` and `read_range(range)` share it)

```
[first, last] = pages of the clamped range;  p = first
while p ≤ last:
    folio = folios.floor(p)                 // range(..=p).next_back(), if it covers p
    if folio is None:                                                   // sync readahead
        w = ondemand(ra, index = p, req = last − p + 1, trigger = None, cache = folios)   // §1 ladder
        submit(w)                           // carve missing runs of [start, start+size) around
                                            // existing folios into ≤ io_size pieces; insert one
                                            // Loading folio per piece; spawn ≤ max_inflight tasks;
                                            // mark the folio covering start+size−async_size
        folio = folios.floor(p)             // or, if the window did not cover p (rule 2 after an
                                            // evicted marker): demand-read [p, min(last, p+io_pages−1)]
                                            // as one unmarked folio — kernel filemap_create_folio
    if folio.readahead_mark:                                            // async readahead
        folio.readahead_mark = false
        submit(ondemand(ra, index = folio.first, req, trigger = Some(folio.extent), cache = folios))
                                            // no wait; honoured on Loading folios too
    bytes = match folio.state { Loading(fut) => fut.await?, Ready(b) => b }
    out.extend(bytes[overlap of the request with the folio]); folio.referenced = true; lru.touch(folio)
    p = folio.first + folio.pages
ra.prev_pos = last byte of this request
retain():  Private → drop folios with first + pages ≤ last + 1 (drop-behind)
           Shared  → nothing here; LRU touches happened in the loop
```

Notes:

* `ondemand` is the kernel ladder with one generalisation: rule 2 matches
  when the triggering folio's extent contains `start + size − async_size`
  (the kernel's `round_up(expected, folio_size) == folio->index`, for
  unaligned extents).
* A request inside one folio returns a zero-copy `Bytes::slice` — with 8 MiB
  folios that is the common case for 1 MiB sequential reads; a request
  spanning folios is concatenated once (the SDK path copies today as well).
* `submit` spawns `tokio::spawn` tasks holding the `Arc<dyn File>`. In
  page-cache mode they run to completion even if the handle is closed (the
  folios stay useful to the next reader; the window cap bounds the waste —
  decision 8); in readahead-only mode `close` aborts them (`JoinHandle::abort`)
  and releases their in-flight permits, since no one else can see the result.
* An I/O error resolves the shared future with `Err`, every waiter gets the
  error, the folio is removed; the next reader re-issues synchronously —
  the kernel's "failed readahead pages are dropped and read individually".
* `submit` reads only missing pages, so two handles streaming the same object
  share one set of requests; a reader that catches up with the prefetcher
  simply awaits the `Loading` folio instead of racing it.
* The handle's own `read()` cursor still exists; `seek` only moves it. The
  inner `GcsFile` buffer is bypassed because only `read_range` is called —
  no double buffering.

---

## 6. Against the fork's `BackgroundPrefetcher`

| | fork (Python) | this design |
|---|---|---|
| sequential detection | streak counter (3 reads), reset on any seek | kernel ladder: expected-offset match, `prev_pos` adjacency, context from cached pages; a seek is just a random read until the next adjacency |
| ramp | linear in streak, `× avg_io` | 4×/2× geometric per window, capped |
| pipelining | producer refills when the queue drains | marked folio submits the next window while the current one is consumed |
| re-reads / multiple handles / `pread` | discarded after consumption; one cursor | shared LRU cache; any access order benefits |
| random reads | fetch exact bytes; large variable reads disable prefetch | fetch one 256 KiB page, state untouched; `Random` hint for exact-size reads |
| concurrency | split by `concurrency` | split into `io_size` folios, bounded per handle and globally |
| memory | ≤ max_prefetch per handle, unbounded across handles | readahead only: ≤ 2 windows per handle + one global in-flight cap; page cache: one byte budget |
| no-cache operation | always (consumed data is discarded) | `cache: None` — same memory profile, kernel ramp, `pread`-tolerant; the page cache is opt-in |

The bridge can keep the Python prefetcher as a fallback behind the same
`cache_type` switch; nothing in this design depends on it going away.

---

## 7. Below the cache: bandwidth

The cache turns one logical read into ≤ 8 concurrent `read_range` calls on
one `GcsFile`. On gRPC those multiplex over **one** bidi stream; whether one
stream saturates the link is a `GcsFile`/`Backend` question (a small pool of
descriptors per open object, like the fork's `mrd_pool`). It is orthogonal
to this design and can land separately; the cache's concurrency knobs
already express the demand.

---

## 8. Verification

* **Pure readahead unit tests** — `ondemand()` is a pure function of
  `(ra, index, req, trigger, cached-extent predicate)` returning a `Window`,
  so the kernel table reproduces exactly: initial sizes 4/8/16/32 for
  `max = 32` (and 4/16/32/64/128/256 pages for `max = 256`); ramp
  4×→2×→cap; marker hit advances the window; off-the-end read advances it;
  random read leaves state untouched; oversize read → initial; `prev_pos`
  adjacency → initial; context readahead from cached history; the "merge
  own marker" rule; rule 2 matching on an unaligned folio extent.
* **Page cache unit tests** — floor lookup finds interior pages of a folio
  and misses the page after it; the carve skips existing folios and cuts
  runs into ≤ `io_size` pieces; LRU by bytes evicts whole folios; `Loading`
  sharing (two waiters, one request); error → folio removed → retry;
  generation keying; zonal size bump replaces the entry.
* **`tests/cache.rs` with `MemoryFs`** — `MemoryFile::read_range` records
  every range it serves, so the *request pattern* is asserted: 256 KiB
  sequential reads of a 100 MiB file issue windows of 1, 4, 8, 16, 32, 64
  MiB as ≤ 8 MiB requests (one per folio); 1 MiB reads start at 4 MiB; 100
  random 4 KiB reads issue exactly 100 reads of 256 KiB; a random read that
  landed inside a later window splits that window's requests around it;
  two handles streaming one object issue one set of requests; `pread`
  interleaving still ramps; `Random` hint issues exact ranges.
* **Readahead-only mode** — same request pattern for one handle; two
  handles on one object issue two sets; resident bytes never exceed two
  windows during a 100 MiB stream and are zero after `close`; a re-read of
  consumed bytes is a fresh request; `close` mid-stream leaves no task
  running.
* **Live** — sequential throughput on a 256 MiB scratch object (flat and
  zonal) with `CachedFs` in both modes vs bare `GcsFs`, and the random-read
  request count, reported in the implementing PR.

---

## 9. Phasing

1. `readahead.rs` (pure ladder) + `folios.rs` (the `BTreeMap` index:
   `lookup`, `gaps`, carve, `Loading`/`Ready`) + `ReadaheadFile` in
   `cached.rs` with `Index::Private` (readahead only) and `Index::Shared`
   (`PageCache`, `PageStore` trait, `MemoryStore`), `CacheConfig { dir,
   data }`, tests above. Both modes land together — the mode is an owner
   and a retention hook, not a second engine.
2. `DirStore` for node-local sharing (§4.2); `OpenOptions::read_pattern`
   hint; `invalidate_cache(None)` also dropping pages.
3. Handle-less `cat_file` / `cat_ranges` through the engine (in
   readahead-only mode this is simply the parallel download path: 64 MiB
   windows, 8 requests in flight, drop-behind).
4. Descriptor pooling in `GcsFile` if measurements show one stream is the
   limit; optionally the engine replacing `GcsFile`'s block buffer
   (decision 11).

---

## 10. Decisions

| # | Question | Recommendation |
|---|---|---|
| 1 | Placement | data half of `CachedFs` (§3 a); `CacheConfig { dir, data }` — breaking change to today's `CacheConfig` fields, fine pre-1.0 |
| 2 | `page_size` default | **256 KiB base page, folios up to `io_size`** — the kernel's own split (4 KiB pages, folios to 2 MiB). The page is only the index unit: I/O, allocation and bookkeeping are per folio, so a small page costs nothing in request count or map size; what it buys is a 4 KiB random read paying 256 KiB (≈ 2 ms) instead of 1 MiB. 64 KiB would shave ≈ 0.5 ms more off random reads (first-byte latency dominates) but starts every ramp at a 256 KiB window; 1 MiB is right only if random reads are rare, and `pread`-style loading is the workload |
| 3 | `capacity` default | **512 MiB** per `CachedFs`; the bridge owns one, so one budget per process |
| 4 | `max_readahead` / `io_size` / `max_inflight` | **64 MiB / 8 MiB / 8** → ≤ 64 MiB in flight per handle (`io_size` is also the largest folio), plus a **256 MiB** global in-flight cap |
| 5 | `cat_file` through the cache | phase 3; the bridge's `GCSFile.read` is the hot path and goes through `open` |
| 6 | Hint API | `OpenOptions::read_pattern: ReadPattern { Auto, Sequential, Random }` in phase 2 (`Sequential` = `max ×2` + initial window at open; `Random` = exact-size reads, nothing cached) |
| 7 | `OpenOptions::block_size` | superseded when `data` is configured (documented); still drives the bare `GcsFile` buffer |
| 8 | In-flight windows after `close` | page-cache mode: run to completion (folios serve the next reader; waste ≤ one window); readahead-only: aborted |
| 9 | Cross-process sharing (§4.2) | `PageStore` trait from day one; `MemoryStore` in phase 1, `DirStore` on local SSD (tmpfs if there is none) in phase 2; a daemon only if measurements demand it |
| 10 | Readahead without caching | `DataConfig { readahead, cache: None }` (§3): private per-handle index, drop-behind, abort at `close`. **Recommended as the bridge's default for checkpoint loading** — one pass, no re-reads, memory ≤ 2 windows per handle; the page cache is opt-in where ranks share shards or data is re-read |
| 11 | Engine inside `GcsFile` | Should the same engine replace the private 5 MiB `block_size` buffer so a bare `GcsFs::open` gets readahead with no decorator? Same owner as readahead-only mode, but it puts spawned tasks and a shared in-flight semaphore into `GcsFs`. **Defer** (phase 4): the bridge already wraps in `CachedFs` for the dir cache, so it would gain nothing today |
