# Architecture Design: Asynchronous Readahead and Page Cache in `CachedFs`

*   **Author:** Prince Kumar (@princer)
*   **Status:** Draft (Revised after Stage 9 Implementation Readiness Audit)
*   **Date:** 2026-11-04

--------------------------------------------------------------------------------

## Overview

In distributed machine learning training (PyTorch `DataLoader`, Ray Train, JAX) and large-scale columnar data analytics (Apache Arrow, Parquet, DuckDB), reading multi-gigabyte checkpoints and dataset partitions sequentially from Google Cloud Storage (GCS) incurs high network round-trip latency overhead between blocks. Today's [`GcsFile::read`](file:///home/princer_google_com/dev/gcs-rust-fs/src/gcs/file.rs#L225) issues synchronous 5 MiB block fetches, forcing calling threads to stall on network roundtrips between blocks while GPU/CPU compute sits idle. Furthermore, independent handles opening the same object and concurrent positional [`read_range`](file:///home/princer_google_com/dev/gcs-rust-fs/src/gcs/file.rs#L254) calls redundantly download duplicate byte ranges over the wire.

We implement an **Asynchronous Readahead Engine and Page Cache** within the [`CachedFs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs) VFS decorator. By placing data caching in the VFS decorator rather than modifying raw storage backends, this design preserves the crate's foundational invariant: **backends in this crate cache nothing; [`GcsFs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/gcs/fs.rs) is strictly stateless**.

We model the engine directly on the Linux kernel's dual-tier architecture (`mm/filemap.c` and `mm/readahead.c` at v6.6), providing:
1. **Two Cohesive Operating Modes Under One Engine**:
   * **Readahead-Only Mode (`cache: None`)**: Private per-handle readahead buffer with immediate drop-behind behind the read cursor and per-window task cancellation on seek or handle drop. Optimized for high-throughput single-pass dataset streaming where cache hit rate is zero.
   * **Page Cache Mode (`cache: Some(PageCacheConfig)`)**: Shared object-level page cache (`ObjectPages`) keyed immutably by `ObjectKey` `(bucket, object, generation)`, coalescing concurrent reads and sharing folios across handles under a global LRU byte budget.
2. **Kernel On-Demand Readahead**: An adaptive geometric ladder ($2 \to 4 \to 8 \to 16 \to 32 \to 64\text{ MiB}$) with marker folios (`PG_readahead`), submitting asynchronous background batches while the consumer is still reading current folios to eliminate window boundary stalls.
3. **Clean OpenMode Handling**: `ReadaheadFile` decorates read handles ([`OpenMode::Read`](file:///home/princer_google_com/dev/gcs-rust-fs/src/options.rs#L25)), while write handles continue using [`CachedFile`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs#L484-L550) for directory listing invalidation.
4. **Deterministic In-Memory Testability**: Fully testable on [`MemoryFs`](file:///home/princer_google_com/dev/gcs-rust-fs/tests/common/mod.rs#L75) without requiring live network connections, mock gRPC streams, or cloud credentials.

--------------------------------------------------------------------------------

## The Problem

Embedding synchronous block buffering or stateful caching directly within backend file handles introduces architectural and operational issues:

1. **Pipelining Latency & Hardware Starvation**: In PyTorch `DataLoader` workers, every 5 MiB block boundary forces the thread to block for 50–150 ms of network latency. For a 10 GiB checkpoint or dataset shard, this incurs over 2,000 synchronous stalls, dropping effective storage throughput from multi-gigabit wire capacity down to <150 MB/s.
2. **Violation of Backend Statelessness**: Attempting to embed caching and background task supervisors in `GcsFs` violates the core architecture established in [`docs/filesystem_api_design.md`](file:///home/princer_google_com/dev/gcs-rust-fs/docs/filesystem_api_design.md). Backends must focus exclusively on RPC transport, generation pinning, and error translation; caching belongs in the VFS decorator layer.
2. **Violation of Backend Statelessness**: Attempting to embed caching and background task supervisors in `GcsFs` violates the core architecture established in [`src/README.md`](file:///home/princer_google_com/dev/gcs-rust-fs/src/README.md). Backends must focus exclusively on RPC transport, generation pinning, and error translation; caching belongs in the VFS decorator layer.
3. **Concurrency Hazards on Shared In-Flight Folios**: If a handle spawns a background fetch task into a shared object cache, and a second handle joins that in-flight future, aborting the first handle's task on `seek()` or `drop()` will abruptly cancel or panic the read operation of the second handle. In-flight tasks in a shared cache must be owned by the cache lifecycle, while private streaming tasks must remain strictly per-handle.
4. **Cross-Handle Drop-Behind Complexity**: In a shared object cache, implementing drop-behind requires complex watermark tracking and handle registries, risking memory leaks or premature eviction of folios needed by lagging handles.
5. **Redundant Network I/O Across Handles & Threads**: Multi-worker training pipelines and columnar query engines often open the same object across multiple tasks or issue concurrent positional `read_range` queries. Without an object-level page cache, each handle and range query independently downloads identical byte ranges.

--------------------------------------------------------------------------------

## Goals

*   **Preserve Backend Statelessness**: We implement all readahead scheduling, folio buffering, and page caching inside [`CachedFs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs). Backends (including `GcsFs` and `MemoryFs`) remain completely stateless and provide only [`File::read_range`](file:///home/princer_google_com/dev/gcs-rust-fs/src/file.rs#L114).
*   **Pipelined Sequential Streaming**: Pipeline network transport ahead of downstream consumer computation, serving sequential reads from pre-warmed in-memory buffers at memory copy speeds (<15 µs).
*   **Clean Dual Modes (Readahead-Only vs Shared Page Cache)**:
    * Support zero-overhead drop-behind for single-pass workloads (`DataLoader`, checkpoint ingestion) with private per-handle indexes and per-window cancellation.
    * Support shared object-level caching for multi-reader workloads (columnar table scans, multi-rank node sharing) with global LRU capacity and safe in-flight future coalescing.
*   **Linux Kernel Readahead Parity**: We port the Linux v6.6 `ondemand_readahead` algorithm:
    * Adaptive geometric window expansion up to a configurable maximum (default 64 MiB).
    * `PG_readahead` marker folios to trigger async pipelined prefetching before the consumer stalls.
    * Standalone random read detection (rule 7: non-sequential reads that do not continue the previous read position fetch only the requested extent without advancing readahead state).
*   **Decoupled Memory & Concurrency Bounds**:
    * Regulate active network transport via a page-unit semaphore (`max_inflight_bytes`), released immediately upon chunk arrival.
    * Regulate resident cache memory via an LRU list (`capacity_bytes`), evicting cold folios under memory pressure.
*   **Deterministic In-Memory Testability**: Validate all state transitions, geometric ramps, chunk splitting, marker triggers, and cancellation deterministically on [`MemoryFs`](file:///home/princer_google_com/dev/gcs-rust-fs/tests/common/mod.rs#L75).

--------------------------------------------------------------------------------

## Non-Goals

*   **Cross-Process Shared Memory**: The cache operates within the Rust process memory space. Inter-process or multi-node distributed caches belong in external systems.
*   **Backend-Specific Transport Leaks**: The cache interacts solely with `File::read_range`. Low-level transport concerns (such as HTTP/2 vs gRPC multiplexing) remain encapsulated within `ReadHandle`.
*   **Predictive Arbitrary Strided Prefetching**: Non-linear or backward seek patterns are treated as random access (rule 7) and bypass the readahead ladder.
*   **Write Caching**: Writes are passed straight through to the underlying backend handle; write-back or dirty-page caching is out of scope.

--------------------------------------------------------------------------------

## Architectural Decisions (Open Questions Resolved)

*   **Decision 1: Page and Folio Sizing (256 KiB Pages, Folios Up to 32 MiB)**:
    * `page_size` = **256 KiB**: The fundamental index and smallest random read unit. A 4 KiB random read pays for one 256 KiB folio (~2 ms), avoiding high Class B GetObject request amplification.
    * `io_size` = **8 MiB**: Default folio size for initial ramp stages.
    * `max_folio_size` = **32 MiB**: As sequential read streaks continue, folios grow up to 32 MiB (128 pages). For sustained multi-gigabyte checkpoint ingestion, this cuts request counts and HTTP/2 framing overhead in half compared to 16 MiB slices.
*   **Decision 2: Read Pattern Advice on `OpenOptions` (`ReadPattern`)**:
    * We expose `read_pattern: ReadPattern` (`Auto`, `Sequential`, `Random`) on [`OpenOptions`](file:///home/princer_google_com/dev/gcs-rust-fs/src/options.rs#L113), mirroring `posix_fadvise`:
      * `ReadPattern::Auto` (default): Uses the standard Linux v6.6 `ondemand_readahead` geometric ladder ($2 \to 4 \to 8 \to 16 \to 32 \to 64\text{ MiB}$).
      * `ReadPattern::Sequential`: Skips the initial probe ramp, immediately priming at `max_readahead` (64 MiB) with 32 MiB folios for known sequential streams.
      * `ReadPattern::Random`: Disables the speculative readahead ladder (`FMODE_RANDOM`), treating every read as a standalone single-page fetch to eliminate speculative memory overhead for random access.

--------------------------------------------------------------------------------

## Critical User Journeys (CUJs)

### CUJ 1: High-Throughput Model Checkpoint Loading (Readahead-Only Mode)
*   **Given**: A PyTorch training worker opening a 20 GiB safetensors model checkpoint using `CachedFs` configured with `cache: None` (readahead-only).
*   **When**: The application reads sequentially in 4 MiB increments via `file.read(Some(4 * 1024 * 1024))`.
*   **Then**: 
    1. The first 4 MiB read triggers a synchronous fetch for the requested extent, and dispatches an asynchronous readahead window.
    2. Folios are marked with `readahead_mark`. When the reader touches a marked folio, the engine automatically dispatches the next window (ramping to 8, 16, 32, and 64 MiB).
    3. As soon as the reader finishes consuming a folio (`first_page + num_pages <= current_page`), it is dropped immediately behind the cursor (drop-behind).
    4. Memory footprint never exceeds the active window size (~2 × `max_readahead`).

### CUJ 2: Multi-Reader Concurrent Ingestion (Page Cache Mode)
*   **Given**: Two independent worker threads opening the same object `gs://bucket/shard-001.bin` using `CachedFs` configured with `cache: Some(PageCacheConfig)`.
*   **When**: Handle A reads sequentially from offset 0, and Handle B issues a positional `read_range(ByteRange::bounded(16 * 1024 * 1024, 20 * 1024 * 1024))`.
*   **Then**: 
    1. Handle A's readahead engine dispatches folios into `ObjectPages`.
    2. When Handle B requests offset 16 MiB, it discovers the folio already `Ready` (or joins the `Loading` future) in `ObjectPages`.
    3. If Handle A seeks or drops, Handle B's read completes unaffected because the shared folio is owned by the cache lifecycle, not Handle A's private cancellation token.

### CUJ 3: Columnar File Footer Probe Followed by Table Scan
*   **Given**: A DuckDB or Apache Arrow process opening a 5 GiB Parquet file.
*   **When**: 
    1. The reader seeks to `size - 64 KiB` to inspect the metadata footer.
    2. The reader seeks back to offset `0` and streams consecutive row groups.
*   **Then**: 
    1. The footer probe is classified as a standalone random read (rule 7); exactly one 256 KiB folio is fetched without triggering a readahead window.
    2. Seeking to offset 0 resets the per-handle readahead state cleanly with a fresh cancellation token.
    3. The sequential table scan from offset 0 ramps up geometrically and pipelines data.

### CUJ 4: Worker Process Exit & Handle Drop Teardown
*   **Given**: DataLoader worker tasks reading training shards in readahead-only mode.
*   **When**: An epoch ends early or an exception occurs, dropping the `ReadaheadFile` handle.
*   **Then**: 
    1. `Drop::drop` immediately signals the active `CancellationToken`.
    2. In-flight background download tasks for that handle abort within <1 ms.
    3. Private buffer memory is deallocated immediately.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. High-Level Architecture Overview

We implement data caching and readahead in [`src/cached.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs) as an extension to [`CachedFs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs):

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ CachedFs (VFS Decorator)                                                    │
│                                                                             │
│  CachedFs::open(path, opts)                                                 │
│         │                                                                   │
│         ├─► If opts.mode.is_write() ──────► CachedFile (Directory cache)   │
│         │                                                                   │
│         └─► If opts.mode == OpenMode::Read                                  │
│                   │                                                         │
│                   ├─► If data_runtime is None ──► inner.open (Pass-through) │
│                   │                                                         │
│                   └─► If data_runtime is Some ──► ReadaheadFile             │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ ReadaheadFile (implements File)                                       │  │
│  │ ├── inner: Arc<dyn File> (Read-only backend: GcsFile / MemoryFs)      │  │
│  │ ├── pos: u64 (Read cursor)                                            │  │
│  │ ├── closed: bool (Handle state guard)                                 │  │
│  │ ├── ra_state: Mutex<ReadaheadState> (Linux file_ra_state)             │  │
│  │ └── index: Index                                                      │  │
│  │      ├── Private { folios: Mutex<BTreeMap<u64, Arc<Folio>>>, ... }   │  │
│  │      └── Shared { pages: Arc<ObjectPages>, cache: Arc<PageCache> }    │  │
│  └───────────────────────────────────┬───────────────────────────────────┘  │
│                                      │                                      │
│               ┌──────────────────────┴──────────────────────┐               │
│               ▼                                             ▼               │
│  ┌─────────────────────────────┐           ┌─────────────────────────────┐  │
│  │ Mode 1: Readahead-Only      │           │ Mode 2: Page Cache          │  │
│  │ ├── Private per-handle map  │           │ ├── Shared ObjectPages map  │  │
│  │ ├── Immediate drop-behind   │           │ ├── Global LRU byte budget  │  │
│  │ └── Per-window token abort  │           │ └── Cache-owned task futures│  │
│  └─────────────────────────────┘           └─────────────────────────────┘  │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ File::read_range(&self, range)
                                       ▼
             Stateless Storage Backend (GcsFs / GcsFile / MemoryFs)
```

### 2. Kernel Readahead State Machine & Folios

We port the Linux kernel v6.6 `mm/readahead.c` algorithm:

* **Folio Concept**: A `Folio` represents one contiguous range response from the backend (1 to `io_size / page_size` pages). A folio is the unit of allocation, locking, and readahead marks.
* **Marker Page (`readahead_mark`)**: When an asynchronous window is submitted, the engine marks the folio at `start + size - async_size` with `readahead_mark = true`.
* **Pipelining Trigger**: When `File::read` touches a folio marked with `readahead_mark`:
  1. The mark is cleared atomically (`swap(false, Ordering::Relaxed)`).
  2. The next window is submitted asynchronously in the background.
  3. The consumer continues reading the current folio without stalling.
* **Decision Ladder**:
  * **Initial Read (offset 0 or post-`seek` streaming read `>= io_size`)**: Window size starts at $\max(2 \times \text{req\_pages},\, 2 \times \text{io\_pages},\, 8)$, clamped to `max_readahead` and file EOF, so even small `1 MiB` `read()` calls immediately fetch full `io_size` folios.
  * **Sequential Streak**: Window doubles geometrically ($2 \to 4 \to 8 \to 16 \to 32 \to 64\text{ MiB}$) up to `max_readahead`.
  * **Dynamic Folio Scaling (`submit_window`)**: When sequential readahead is active (`async_size > 0` in `ReadPattern::Auto` or `ReadPattern::Sequential`), each folio's size scales dynamically from `io_size` up to `max_folio_size` via `(size / max_inflight).max(io_pages).min(max_folio_pages)`, keeping `max_inflight` requests pipelined in parallel across the HTTP/2 connection pool.
  * **Synchronous Marker Registration (`submit_async_readahead`)**: Touching a folio with `readahead_mark = true` synchronously inserts the next window's `Loading` folios into the `BTreeMap` and spawns their background fetch tasks, preventing the reader from outrunning window registration when consuming `Ready` folios in memory.
  * **Rule 7 (Standalone Random Read)**: If a read does not extend the previous sequential read position (`index - page(prev_pos) > 1`) and is smaller than `io_size` at a non-zero offset, read only the requested extent (`async_size = 0`, bounded by `io_pages`) without advancing the readahead ladder.

### 3. Core Data Structures

```rust
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use bytes::Bytes;
use futures_util::future::{BoxFuture, Shared};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use crate::error::{Error, Result};
use crate::file::File;
use crate::options::OpenMode;
use crate::stat::ObjectStat;

/// Key uniquely identifying an immutable Cloud Storage object generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    pub bucket: Arc<str>,
    pub object: Arc<str>,
    pub generation: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheConfig {
    pub ttl: Option<Duration>,
    pub max_dirs: Option<usize>,
    pub negative: bool,
    pub populate_from_find: bool,
    pub data: Option<DataConfig>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: None,
            max_dirs: None,
            negative: false,
            populate_from_find: true,
            data: Some(DataConfig::readahead_only()),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataConfig {
    pub readahead: ReadaheadConfig,
    pub cache: Option<PageCacheConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadaheadConfig {
    pub page_size: usize,          // Default: 256 KiB (index unit)
    pub max_readahead: usize,      // Default: 64 MiB (max window)
    pub io_size: usize,            // Default: 8 MiB (initial/default folio request size)
    pub max_folio_size: usize,     // Default: 32 MiB (maximum folio request size)
    pub max_inflight: usize,       // Default: 8 concurrent folios per handle
    pub max_inflight_bytes: usize, // Default: 256 MiB global inflight cap
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

/// Hints to the readahead engine on expected access patterns, configured via OpenOptions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum ReadPattern {
    /// Adaptive Linux v6.6 on-demand readahead (default).
    #[default]
    Auto,
    /// Known sequential stream: skips probe ramp, priming at max window and folios up to 32 MiB.
    Sequential,
    /// Random access: disables readahead window ladder (FMODE_RANDOM), reading exact single-page folios.
    Random,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageCacheConfig {
    pub capacity: usize,           // Global byte budget for resident folios (default: 512 MiB)
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            capacity: 512 * 1024 * 1024,
        }
    }
}

pub struct Folio {
    pub first_page: u64,
    pub num_pages: u32,
    pub readahead_mark: AtomicBool,
    pub state: RwLock<FolioState>,
    pub state: Mutex<FolioState>,
}

#[derive(Clone)]
pub enum FolioState {
    Loading(Shared<BoxFuture<'static, Result<Bytes, Arc<Error>>>>),
    Loading(Shared<BoxFuture<'static, std::result::Result<Bytes, Arc<Error>>>>),
    Ready(Bytes),
    Failed(Arc<Error>),
}

pub enum Index {
    /// Mode 1: Private to this handle; drop-behind enabled; abort on seek/drop.
    Private {
        folios: Mutex<BTreeMap<u64, Arc<Folio>>>,
        cancel_token: Mutex<CancellationToken>,
        folios: Arc<Mutex<BTreeMap<u64, Arc<Folio>>>>,
        cancel_token: Arc<Mutex<CancellationToken>>,
    },
    /// Mode 2: Shared across all handles for (bucket, object, generation); global LRU.
    Shared {
        pages: Arc<ObjectPages>,
        cache: Arc<PageCache>,
    },
}

pub struct ObjectPages {
    pub key: ObjectKey,
    pub size: u64,
    pub folios: Mutex<BTreeMap<u64, Arc<Folio>>>,
}

/// Global page cache managing resident folios across objects with LRU eviction.
pub struct PageCache {
    pub objects: Mutex<HashMap<ObjectKey, Arc<ObjectPages>>>,
    pub tick: AtomicU64,
    pub recency: Mutex<BTreeMap<u64, (ObjectKey, u64)>>,
    pub used_bytes: AtomicUsize,
    pub capacity_bytes: usize,
}

/// Shared data runtime in CachedFs managing in-flight network bandwidth and cache access.
pub struct DataRuntime {
    pub inflight_semaphore: Arc<Semaphore>,
    pub cache: Option<Arc<PageCache>>,
    pub config: ReadaheadConfig,
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
    handle_semaphore: Arc<Semaphore>, // Per-handle in-flight folio pacing (`max_inflight`)
}

pub struct ReadaheadState {
    pub start: u64,          // in page units
    pub size: usize,         // in page units
    pub async_size: usize,   // in page units
    pub ra_pages: usize,     // maximum window size in pages
    pub prev_pos: i64,       // last-read byte index (-1 if none)
    pub pattern: ReadPattern,// Auto, Sequential, Random
}
```

### 4. Concurrency, Cancellation & Task Ownership

1. **Readahead-Only Mode (Mode 1)**:
   * Background tasks are private to the `ReadaheadFile` handle.
   * Tasks hold a clone of the current window's `CancellationToken`.
   * On `seek(target)`:
     * **Forward Gap Tolerance (up to 8 MiB)**:
       * If `target >= cursor && target - cursor <= 8 * 1024 * 1024`:
         * Do not abort background prefetch tasks or reset the readahead ladder.
         * Advance `cursor = target`.
         * Drop private folios preceding the new target (`first_page + num_pages <= target / page_size`).
         * In-flight and prefetched folios ahead are preserved, allowing subsequent reads to continue at in-memory speed.
     * **Out-of-Window / Backward Seek**:
       * If `target < cursor` (backward seek) OR `target - cursor > 8 * 1024 * 1024` (forward jump > 8 MiB):
         * `cancel_token.lock().cancel()` aborts all in-flight tasks for the abandoned window.
         * `*cancel_token.lock() = CancellationToken::new()` creates a fresh token for new tasks.
         * `self.handle_semaphore` is replaced with a fresh `Semaphore` so cancelled tasks cannot starve post-seek reads.
         * Readahead state resets cleanly (`prev_pos = -1`) without latching dead cancellation.
         * Advance `cursor = target`.
   * On `drop()`:
     * `cancel_token.lock().cancel()` halts pending tasks immediately.
     * All private folios are dropped and deallocated.
2. **Page Cache Mode (Mode 2)**:
   * Tasks deposit folios into the shared `ObjectPages` map.
   * Tasks are owned by the cache lifecycle, not by an individual handle's token.
   * If Handle A seeks or drops, its private readahead state resets, but the in-flight tasks in `ObjectPages` continue to completion. Handle B (which joined the `FolioState::Loading` shared future) completes its read successfully.
   * Completed folios enter the global LRU cache and are evicted only under memory pressure.
3. **Lock Ordering Invariant**:
   * To prevent deadlocks, callers must **never** acquire a lock on `PageCache` while holding a lock on `ObjectPages.folios`. Lock on `ObjectPages.folios` is released before calling `PageCache::record_access` or `PageCache::evict`.

### 5. Quarantined Error Handling

* **Transient Failures**: Background worker tasks retry transient transport errors (`ErrorKind::Timeout`, HTTP 503, gRPC `Status::unavailable`) up to 3 times with exponential backoff ($50\text{ ms} \to 150\text{ ms} \to 450\text{ ms}$) using `tokio::time::sleep`.
* **Transient Failures**: Background worker tasks retry transient transport errors (`ErrorKind::Timeout`, `ErrorKind::Other`) up to 3 times with exponential backoff ($50\text{ ms} \to 150\text{ ms} \to 450\text{ ms}$) using `tokio::time::sleep`.
* **Terminal Failures & Quarantining**: If retries fail or an unrecoverable error occurs, the folio transitions to `FolioState::Failed(Arc<Error>)`.
* **Consumer Surfacing**: The error is surfaced to the caller only when a read operation requests bytes intersecting that failed folio. `Failed` folios remain quarantined in the map with a retry backoff cooldown to prevent infinite prefetch retry storms.
* **Consumer Surfacing**: The error is surfaced to the caller only when a read operation requests bytes intersecting that failed folio. `Failed` folios remain quarantined in the map until evicted or dropped to prevent infinite prefetch retry storms.

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative A: Embedding Cache and Prefetch in `GcsFile` / `GcsFs`

*   **Pros**: Direct access to GCS backend specifics.
*   **Cons**: Violates crate invariant that backends are stateless; untestable on `MemoryFs` without network dependencies; duplicates caching logic if another storage backend is added; introduces cancellation hazards when sharing tasks across handles.
*   **Reason for Rejection**: Rejected in Stage 5 review.

### Alternative B: Cross-Handle Drop-Behind in a Shared Map

*   **Pros**: Theoretical memory savings if multiple handles stream the same object in lockstep.
*   **Cons**: Requires complex open-handle registries, heartbeat synchronization, and cursor watermark tracking across asynchronous tasks. Prone to race conditions and premature eviction.
*   **Reason for Rejection**: Rejected in Stage 5 review. Replaced by the clean two-mode model: private drop-behind for streaming vs shared LRU for multi-reader workloads.

--------------------------------------------------------------------------------

## Non-Functional Requirements

### 1. Memory Safety & Ceilings
* **In-Flight Byte Cap**: Regulated globally by `ReadaheadConfig::max_inflight_bytes` via `Arc<Semaphore>`. Speculative fetch tasks acquire permits equal to folio length before issuing backend requests, and release permits immediately upon byte arrival.
* **Resident Memory Cap in Mode 2**: Governed by `PageCacheConfig::capacity`. Folios are evicted via an LRU list when resident bytes exceed capacity.
* **Zero Leakage on Drop**: In Mode 1, all tasks abort and buffers deallocate on handle drop. In Mode 2, unreferenced objects age out through LRU.

### 2. Latency & Pipelining Efficiency
* **Cache Hit Latency**: Reading from a `Ready` folio takes **<15 microseconds** (memory copy).
* **Cache Hit Latency**: Reading from a `Ready` folio takes **<15 microseconds** (zero-copy `Bytes::slice` fast path when satisfied by a single folio, or `BytesMut` copy across folio boundaries).
* **Network Elimination**: Eliminates network stall bubbles for sequential streams exceeding 16 MiB.
* **Seek Cancellation Latency**: Seeking in Mode 1 cancels background tasks within **<1 millisecond**.

### 3. Thread-Safety & Backend Independence
* **Lock Granularity**: Folio maps are guarded by fine-grained `Mutex`. Lookups hold the lock only long enough to locate the folio and clone an `Arc` reference or shared future.
* **Backend Agnostic**: The cache interacts strictly with `File::read_range(&self, range)`. Works identically on `GcsFs`, `MemoryFs`, or any future filesystem.

### 4. Observability & Metrics
* **Structured Tracing**: Log events at `tracing::trace` and `debug`:
  * `readahead_hit`: Cache hit from ready folio.
  * `readahead_pipeline`: Marker touched; next window submitted.
  * `readahead_coalesce`: Secondary reader joined an in-flight loading folio.
  * `readahead_cancel`: Handle seek aborted private tasks.
* **Runtime Metrics**:
  * `gcsfs_cache_hits_total`, `gcsfs_cache_misses_total`
  * `gcsfs_cache_bytes_prefetched_total`, `gcsfs_cache_bytes_consumed_total`
  * `gcsfs_cache_bytes_evicted_total`
* **Metadata Cache Stats**: `CachedFs::stats()` exposes `CacheStats { hits, misses, invalidations }` for directory and entry metadata lookups.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### 1. Target Files and Scope of Modifications

| File | Operation | Description |
| :--- | :--- | :--- |
| [`Cargo.toml`](file:///home/princer_google_com/dev/gcs-rust-fs/Cargo.toml) | **Modify** | Add `tokio-util = { version = "0.7", features = ["sync"] }` and ensure `tokio` in `[dependencies]` enables the `"time"` feature. |
| [`Cargo.toml`](file:///home/princer_google_com/dev/gcs-rust-fs/Cargo.toml) | **Modify** | Add `tokio-util = "0.7"` and ensure `tokio` in `[dependencies]` enables the `"time"` feature. |
| [`src/lib.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/lib.rs) | **Modify** | Re-export `DataConfig`, `ReadaheadConfig`, `PageCacheConfig`, `ObjectKey`, and `ReadPattern` from `cached` and `options`. |
| [`src/options.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/options.rs) | **Modify** | Add `ReadPattern` enum (`Auto`, `Sequential`, `Random`) and `pub read_pattern: ReadPattern` to `OpenOptions` with `with_pattern(mut self, pattern: ReadPattern) -> Self`. |
| [`src/cached.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs) | **Modify** | Extend `CachedFs` with data caching: implement `DataConfig`, `ReadaheadConfig`, `PageCacheConfig`, `DataRuntime`, `PageCache`, `ReadaheadFile`, `Folio`, `FolioState`, and `Index`. Restrict `ReadaheadFile` to read handles; delegate write handles to `CachedFile`. |
| [`src/file.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/file.rs) | **No change** | `File` trait already supports `read(&mut self, len: Option<usize>)` and `read_range(&self, range: ByteRange)`. |
| [`src/gcs/file.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/gcs/file.rs) | **No change** | `GcsFile` remains strictly stateless, handling raw Cloud Storage streaming and positional range queries. |
| [`tests/common/mod.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/tests/common/mod.rs) | **Modify** | Add `CountingFile` spy wrapper around `MemoryFile` to track backend `read_range` call counts and byte totals. |
| [`tests/page_cache.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/tests/page_cache.rs) | **Create** | Deterministic test suite running on [`MemoryFs`](file:///home/princer_google_com/dev/gcs-rust-fs/tests/common/mod.rs#L75). Validates ladders, folios, marker triggers, drop-behind, LRU eviction, error quarantine, and cancellation. |

### 2. Implementation Steps

#### Step 1: Dependencies, Options & Configuration Exports ([`Cargo.toml`](file:///home/princer_google_com/dev/gcs-rust-fs/Cargo.toml), [`src/options.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/options.rs), [`src/lib.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/lib.rs))
* In `Cargo.toml`:
  * Enable `"time"` on `tokio` in `[dependencies]`: `tokio = { version = "1", features = [..., "time"] }`.
  * Add `tokio-util = { version = "0.7", features = ["sync"] }`.
  * Add `tokio-util = "0.7"`.
* In `src/options.rs`:
  * Define `pub enum ReadPattern { Auto, Sequential, Random }`.
  * Add `pub read_pattern: ReadPattern` to `OpenOptions` (default: `ReadPattern::Auto`).
  * Add builder helper: `pub fn with_pattern(mut self, pattern: ReadPattern) -> Self`.
* In `src/lib.rs`:
  * Re-export `pub use cached::{CacheConfig, CacheStats, CachedFs, DataConfig, ReadaheadConfig, PageCacheConfig, ObjectKey};`.
  * Re-export `pub use options::ReadPattern;`.

#### Step 2: Folio Structures & DataRuntime ([`src/cached.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs))
* Define `ObjectKey`, `DataConfig`, `ReadaheadConfig`, and `PageCacheConfig`.
* Implement `Folio` with `first_page`, `num_pages`, `readahead_mark: AtomicBool`, and `state: RwLock<FolioState>`.
* Implement `Folio` with `first_page`, `num_pages`, `readahead_mark: AtomicBool`, and `state: Mutex<FolioState>`.
* Implement `PageCache`:
  * `new(capacity: usize) -> Self`
  * `record_access(&self, key: &ObjectKey, page: u64)`
  * `insert_folio(&self, key: &ObjectKey, folio: Arc<Folio>)`
  * `add_bytes(&self, bytes: usize)`
  * `evict_if_needed(&self)`: evicts oldest folios according to `recency` until `used_bytes <= capacity_bytes`.
* Implement `DataRuntime`:
  * Holds `inflight_semaphore`, `cache: Option<Arc<PageCache>>`, and `config: ReadaheadConfig`.

#### Step 3: Implement `ReadaheadFile` and `Index` ([`src/cached.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs))
* Define `Index::Private` with `cancel_token: Mutex<CancellationToken>`.
* Define `Index::Private` with `folios: Arc<Mutex<BTreeMap<u64, Arc<Folio>>>>` and `cancel_token: Arc<Mutex<CancellationToken>>`.
* Define `Index::Shared { pages: Arc<ObjectPages>, cache: Arc<PageCache> }`.
* Implement `ReadaheadState` with Linux v6.6 `ondemand_readahead` logic:
  * Detect sequential vs random read patterns (Rule 7).
  * Compute window sizes and marker locations clamped to file EOF.
* Implement `File` trait for `ReadaheadFile`:
  * `fn readable(&self) -> bool { true }`
  * `fn seekable(&self) -> bool { true }`
  * `fn writable(&self) -> bool { false }`
  * `fn closed(&self) -> bool { self.closed || self.inner.closed() }`
  * `async fn close(&mut self) -> Result<()>`: sets `self.closed = true`, signals `cancel_token` in Mode 1, drains private folios, and calls `self.inner.close()`.
  * `async fn close(&mut self) -> Result<()>`: sets `self.closed = true`, signals `cancel_token` in Mode 1, and clears private folios.
  * `async fn discard(&mut self) -> Result<()>`: delegates to `self.close().await`.

#### Step 4: Implement Read Paths & Assembly ([`src/cached.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs))
* In `File::read(&mut self, len: Option<usize>) -> Result<Bytes>`:
  * Guard `ensure_open()`.
  * Compute `target_len = len.unwrap_or((self.size - self.pos) as usize).min((self.size - self.pos) as usize)`. If `target_len == 0`, return `Ok(Bytes::new())`.
  * Multi-folio assembly loop:
    ```rust
    let mut buf = bytes::BytesMut::with_capacity(target_len);
    while buf.len() < target_len && self.pos < self.size {
        let page_idx = self.pos / page_size;
        let folio = self.get_or_fetch_folio(page_idx).await?;
        if folio.readahead_mark.swap(false, Ordering::Relaxed) {
            self.submit_async_readahead();
        }
        let offset_in_folio = (self.pos - (folio.first_page * page_size)) as usize;
        let avail = folio.data().len() - offset_in_folio;
        let take = avail.min(target_len - buf.len());
        buf.extend_from_slice(&folio.data()[offset_in_folio..offset_in_folio + take]);
        self.pos += take as u64;
    }
    // In readahead-only mode: drop consumed folios
    if let Index::Private { folios, .. } = &self.index {
        let current_page = self.pos / page_size;
        folios.lock().retain(|&start_page, f| start_page + (f.num_pages as u64) > current_page);
    }
    Ok(buf.freeze())
    ```
  * Zero-copy single-folio fast path (`folio_data.slice(...)`) when satisfied by the first folio, falling back to `BytesMut` assembly across folio boundaries, followed by Mode 1 drop-behind eviction (`folios.lock().retain(...)`).
* In `File::read_range(&self, range: ByteRange) -> Result<Bytes>`:
  * Guard `ensure_open()`.
  * In Mode 1: check private folios for hit; if miss, pass through directly to `self.inner.read_range(range).await` without polluting readahead state.
  * In Mode 2: check `ObjectPages`; on miss, align range to page/folio boundary, fetch into `ObjectPages`, record LRU access, and return the requested subslice.
* In `File::seek(&mut self, pos: SeekFrom) -> Result<u64>`:
  * Guard `ensure_open()`.
  * Compute new absolute offset clamped to `0..=self.size`.
  * If in Mode 1:
    * If `new_offset >= self.pos && new_offset - self.pos <= 8 * 1024 * 1024`:
      * Forward gap $\le 8\text{ MiB}$: preserve in-flight tasks and readahead ladder.
      * Drop folios preceding `new_offset / page_size`.
    * Else (`new_offset < self.pos` or forward jump `> 8 MiB`):
      * Abort active tasks via `self.cancel_token.lock().cancel()`.
      * Re-initialize `*self.cancel_token.lock() = CancellationToken::new()`.
      * Reset `self.ra_state` with `prev_pos = -1`.
  * `self.pos = new_offset; Ok(self.pos)`

#### Step 5: Integrate with `CachedFs::open` ([`src/cached.rs`](file:///home/princer_google_com/dev/gcs-rust-fs/src/cached.rs))
* In `CachedFs::open`:
  * If `opts.mode.is_write()` -> delegate to `CachedFile`.
  * If `opts.mode == OpenMode::Read`:
    * If `self.data_runtime` is `None` -> return `self.inner.open(path, opts).await` directly.
    * If `self.data_runtime` is `Some(runtime)`:
      * Open handle: `let inner = self.inner.open(path, opts).await?;`
      * Query `inner.stat()`:
        * If `Some(stat)` and `runtime.cache.is_some()`: construct `ObjectKey`, get or insert `Arc<ObjectPages>` from `PageCache`, and set `index = Index::Shared { pages, cache }`.
        * If `inner.stat()` is `None` or `runtime.cache.is_none()`: set `index = Index::Private { folios: Mutex::new(BTreeMap::new()), cancel_token: Mutex::new(CancellationToken::new()) }`.
      * Return `Box::new(ReadaheadFile { inner: Arc::from(inner), pos: 0, size: stat.size, closed: false, ra_state: Mutex::new(ReadaheadState::new(opts.read_pattern)), index, runtime })`.

--------------------------------------------------------------------------------

## Verification Plan

### 1. Automated Deterministic Tests (`tests/page_cache.rs`)

```bash
# Run deterministic page cache & readahead test suite on MemoryFs
cargo test --test page_cache

# Run all crate unit & integration tests
cargo test

# Verify zero compiler warnings or lints
cargo clippy --all-targets -- -D warnings
```

* **Test 1: Geometric Readahead Ramp on `MemoryFs`**:
  * Write a 64 MiB synthetic file to `MemoryFs`. Wrap in `CachedFs` (readahead-only).
  * Read sequentially in 256 KiB chunks.
  * Assert window grows geometrically ($2 \to 4 \to 8 \to 16 \to 32 \to 64\text{ MiB}$) and pipelined marker fires.
* **Test 2: Drop-Behind Memory Bound**:
  * Read a 100 MiB file in readahead-only mode.
  * Assert that earlier folios are dropped immediately behind the cursor and total resident folios never exceed `2 * max_readahead`.
* **Test 3: Standalone Random Read (Rule 7)**:
  * Seek to file footer and read 64 KiB.
  * Assert exactly one 256 KiB folio is fetched and readahead state is not ramped.
* **Test 4: Shared Page Cache Coalescing**:
  * In page cache mode, open two handles on the same file.
  * Handle A triggers fetch; Handle B reads overlapping range.
  * Assert Handle B receives data from memory with zero backend calls using `CountingFile`.
* **Test 5: Handle Drop Teardown**:
  * Open file in readahead-only mode, initiate read, drop handle immediately.
  * Assert all background tasks terminate cleanly without panics.
* **Test 6: LRU Capacity Eviction Under Memory Pressure**:
  * In page cache mode with a 16 MiB capacity, write two 12 MiB files.
  * Read File 1 completely, then read File 2 completely.
  * Assert File 1 folios are evicted from `ObjectPages` when memory exceeds 16 MiB.
* **Test 7: Quarantined Error & Cooldown Recovery**:
  * Inject backend read error for folio at page 10.
  * Assert reading page 10 returns error; verify folio enters `Failed` state and subsequent reads do not trigger infinite retry loops.
* **Test 8: Multi-Folio Spanning Read**:
  * Request a 20 MiB read that spans across three 8 MiB folios.
  * Assert returned buffer contains seamless contiguous bytes.
* **Test 9: Forward Gap Seek Tolerance vs. Backward/Large Seek Cancellation**:
  * Trigger background readahead window (e.g. 16 MiB).
  * Seek forward by 2 MiB (within 8 MiB gap). Assert in-flight prefetch tasks are NOT aborted, ladder state is preserved, and upcoming read hits cached folios.
  * Seek backward or jump forward by 20 MiB (> 8 MiB gap). Assert active tasks are aborted immediately and fresh readahead token/ladder initializes.
* **Test 10: In-Flight Bandwidth Throttling**:
  * Configure `max_inflight_bytes: 16 MiB`.
  * Trigger large readahead and assert concurrent backend transfers never exceed 16 MiB.
* **Test 11: ReadPattern Hints (Sequential Pre-priming & Random Isolation)**:
  * Open a 128 MiB file with `OpenOptions::read().with_pattern(ReadPattern::Sequential)`.
  * Assert engine starts immediately at 64 MiB max window with 32 MiB folios.
  * Open with `ReadPattern::Random`. Assert all reads fetch exact single-page folios without speculative background dispatch.

### 2. Manual & Regression Verification Commands

```bash
# Run live Cloud Storage integration tests against real GCS bucket
cargo test --test live_gcs -- --ignored --nocapture

# Run benchmarks for memory and IPC
cargo run --example bench_ipc
```

--------------------------------------------------------------------------------

*Drafted with EGM*
