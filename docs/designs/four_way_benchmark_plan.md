# Four-Way Storage Benchmark Plan: Native Rust, Pure Python `gcsfs`, `gcsfs` PyO3 Binding, and `gcsfs` over gRPC UDS (Regional HTTP/JSON Path)

*   **Author:** Prince Kumar (@princer)
*   **Status:** Approved
*   **Date:** 2026-11-05

--------------------------------------------------------------------------------

## Overview

This document defines the unified benchmarking architecture, throughput optimization plan, and execution specification for evaluating four Google Cloud Storage (GCS) client stacks over the **HTTP/JSON data path (`Transport::Http`)** against a 20 GiB object (`gs://princer-flat/bench/bench_20GB.bin`) in the **`us-west4` regional bucket** (co-located with the `us-west4-a` compute VM):

1.  **Stack 1 — Native Rust (`gcs-rust-fs`, `Transport::Http`)**: Standalone Rust binary ([`examples/bench_live_readahead.rs`](../../examples/bench_live_readahead.rs)) invoking [`GcsFs`](../../src/gcs/fs.rs) (`Transport::Http`) and [`CachedFs`](../../src/cached.rs) directly across **1 thread vs 48 threads** sharing an in-process `Arc<dyn FileSystem>`.
2.  **Stack 2 — Pure Python `gcsfs` (HTTP/JSON)**: Standard Python `gcsfs.GCSFileSystem` backed by `aiohttp` HTTP/JSON range requests and `fsspec` asynchronous event loops across **1 OS process vs 48 OS processes** (`multiprocessing` with `spawn`).
3.  **Stack 3 — Python `gcsfs` with In-Process Rust Binding (`gcsfs_rust` PyO3, `Transport::Http`)**: Python `gcsfs` delegating I/O through a PyO3 C-extension linked against [`gcs-rust-fs`](../../src/lib.rs) (`GcsFs` `Transport::Http` + `CachedFs`), releasing the Python GIL (`py.allow_threads`) across **1 OS process vs 48 OS processes**.
4.  **Stack 4 — Python `gcsfs` with `gcs-rust-fs` over gRPC UDS (`gcsfs-sidecar --transport http`)**: Thin Python gRPC client processes (**1 OS process vs 48 OS processes**) communicating over a local Unix Domain Socket (`unix:///tmp/gcsfs-sidecar.sock`) with a single shared out-of-process Rust [`gcsfs-sidecar`](../../src/bin/gcsfs_sidecar.rs) daemon configured with `--transport http`.

This concurrency model reflects idiomatic application-level parallelism for each filesystem interface: native Rust applications scale across threads sharing an `Arc<FileSystem>`, whereas Python applications (such as PyTorch `DataLoader(num_workers=48)`) scale across 48 OS worker processes to avoid Python GIL serialization.

### Repository & Execution Scope Boundary
*   **`gcs-rust-fs` Repository (This Repository)**: Implements the core Rust HTTP client pool striping ([`src/gcs/backend.rs`](../../src/gcs/backend.rs), [`src/gcs/fs.rs`](../../src/gcs/fs.rs)), continuous sliding-window readahead and zero-copy slicing ([`src/cached.rs`](../../src/cached.rs)), UDS sidecar readahead flags and range-span dispatch ([`src/rpc/server.rs`](../../src/rpc/server.rs), [`src/bin/gcsfs_sidecar.rs`](../../src/bin/gcsfs_sidecar.rs)), and **executes only Stack 1 (Native Rust)** via [`examples/bench_live_readahead.rs`](../../examples/bench_live_readahead.rs). `gcs-rust-fs` remains a pure-Rust crate with zero Python dependencies.
*   **`gcs-rust-fs` Repository (This Repository)**: Implements the core Rust HTTP client pool striping ([`src/gcs/backend.rs`](../../src/gcs/backend.rs), [`src/gcs/fs.rs`](../../src/gcs/fs.rs)), continuous sliding-window readahead and zero-copy slicing ([`src/cached.rs`](../../src/cached.rs)), UDS sidecar readahead flags, `RemoteFs` `UnixStream` pooling, and range-span dispatch ([`src/rpc/server.rs`](../../src/rpc/server.rs), [`src/rpc/client.rs`](../../src/rpc/client.rs), [`src/bin/gcsfs_sidecar.rs`](../../src/bin/gcsfs_sidecar.rs)), and **executes Stack 1 (Native Rust)** via [`examples/bench_live_readahead.rs`](../../examples/bench_live_readahead.rs) as well as the Rust UDS sidecar baseline via [`examples/bench_uds_readahead.rs`](../../examples/bench_uds_readahead.rs). `gcs-rust-fs` remains a pure-Rust crate with zero Python dependencies.
*   **`gcsfs` Repository (`/home/princer_google_com/c1dev/gcsfs`)**: Houses and executes all Python benchmark runners, PyO3 bindings (`gcsfs_rust`), and UDS Python client adapters for **Stacks 2, 3, and 4**, consuming the JSON baseline artifact emitted by Stack 1.

All four stacks are evaluated across an identical three-part workload suite on `gs://princer-flat/bench/bench_20GB.bin` (`US-WEST4`, `Transport::Http` for all object data reads in Workloads 2 and 3; in Workload 1, uncached `GcsFs::info` in `gcs-rust-fs` executes via the SDK's `StorageControl` gRPC metadata client whereas Pure Python `gcsfs` uses the HTTP/JSON metadata endpoint):
All four stacks are evaluated across an identical four-part workload suite on `gs://princer-flat/bench/bench_20GB.bin` (`US-WEST4`, `Transport::Http` for all object data reads in Workloads 2 and 3; in Workload 1, uncached `GcsFs::info` in `gcs-rust-fs` executes via the SDK's `StorageControl` gRPC metadata client whereas Pure Python `gcsfs` uses the HTTP/JSON metadata endpoint):
1.  **Uncached Metadata `info` Latency (`p50`, `p90`, `p99`, `mean`)** (direct GCS metadata round-trip with metadata caching disabled).
2.  **Small I/O (`1 MiB`) Random Read Latency (`p50`, `p90`, `p99`, `IOPS`, `MB/s`)** over the 20 GiB file at `1` worker and `48` workers.
3.  **Sequential Read Latency and Throughput (`p50`, `p90`, `MB/s`, `MiB/s`, wall time)** over the 20 GiB file with prefetch/readahead enabled at `1` worker (targeting `>= 2.0 GB/s`) and `48` workers (targeting host network saturation at `15–22+ GB/s`).
2.  **Small I/O (`1 MiB`) Random Read Latency (`p50`, `p90`, `p99`, `IOPS`, `MB/s`)** over the 20 GiB file (`1T`, `50` reads).
3.  **Sequential Read Latency and Throughput (`p50`, `p90`, `MB/s`, `MiB/s`, wall time)** over the 20 GiB file with prefetch/readahead enabled at `1` worker (`20 GiB`, targeting `>= 2.0 GB/s`), `24` workers (`480 GiB`), and `48` workers (`960 GiB`, targeting host network saturation at `15–22+ GB/s`).
4.  **Single-Worker `200 MiB` Write Latency and Throughput (`p50`, `p90`, `MB/s`, `MiB/s`)** comparing streaming `open(write)` + `write(8 MiB)` + `close` and one-shot `pipe_file`.

--------------------------------------------------------------------------------

## The Problem

Initial calibration runs on `gs://princer-bucket/bench/bench_20GB.bin` over HTTP achieved only `747.55 MB/s` single-threaded and `3,503.46 MB/s` across 48 threads, well below the expected `>= 2.0 GB/s` single-threaded sequential throughput and `15–22+ GB/s` multi-worker network saturation. Furthermore, existing benchmarks across [`gcs-rust-fs`](../../examples/bench_live_readahead.rs) and `gcsfs` cannot compare all four stacks cleanly due to six structural bottlenecks:

1.  **Cross-Region WAN Penalty (`us-central1` vs `us-west4`)**: The compute VM runs in `us-west4-a`, whereas `princer-bucket` is located in `US-CENTRAL1`. Reading across regions adds ~35–50 ms of WAN RTT per HTTP range request and subjects traffic to cross-region egress shaping. All benchmarks must target `gs://princer-flat/bench/bench_20GB.bin` in `US-WEST4`.
2.  **Single `Storage` HTTP Client Pool Bottleneck in [`src/gcs/backend.rs`](../../src/gcs/backend.rs)**:
    *   In [`Backend::connect`](../../src/gcs/backend.rs#L124-L152), `gcs-rust-fs` instantiates a single `google_cloud_storage::client::Storage` client.
    *   On `Transport::Http`, a single `Storage` instance multiplexes concurrent HTTP range requests over a constrained internal connection pool, capping single-client throughput. Reaching `>= 2 GB/s` on a single file handle and `15–22+ GB/s` across 48 threads requires striping `read_range_http` calls across a pool of `Storage` HTTP clients (`Vec<Storage>`).
3.  **Stop-and-Wait Window Batching in [`ReadaheadFile`](../../src/cached.rs#L844-L855)**:
    *   Because `ReadaheadFile::ondemand_readahead` sets `async_size = count` (placing `readahead_mark` on the very first page of Window 0), `ReadaheadFile::read` triggers `submit_async_readahead()` on Folio 0 while Folio 0 is still `Loading`. As a result, Window 0 (`0..256 MiB`) and Window 1 (`256..512 MiB`) are both dispatched simultaneously at $t = 0$ without per-handle in-flight pacing.
    *   All 32 folios (`512 MiB`) compete for HTTP bandwidth simultaneously, finish at the same instant, and are drained from RAM in `< 2 ms`. Window 2 (`512..768 MiB`), triggered only when the reader enters Window 1 in RAM, has only 2 ms of head start before the reader hits `512 MiB` and stalls for a full HTTP RTT.
    *   Placing `readahead_mark` at the midpoint of each window (`async_size = (count / 2).max(1)`) and adding a **per-handle `max_inflight` semaphore** ensures Window 0 (`16` folios) downloads first and Window 1 (`16` folios) queues in FIFO order behind those 16 permits—launching Folio $16 + k$ the exact millisecond Folio $k$ completes, maintaining a continuous sliding pipeline with zero inter-window stalls.
4.  **Unnecessary 20 GiB `memcpy` in [`ReadaheadFile::read`](../../src/cached.rs#L1372-L1413)**:
    *   `ReadaheadFile::read` currently allocates a new `BytesMut::with_capacity(target_len)` eagerly before its read loop and copies bytes via `extend_from_slice` on every read call, even when the requested `8 MiB` chunk lies entirely inside a single `Ready` `8 MiB` or `16 MiB` `Folio`. Returning a zero-copy `Bytes` slice directly and deferring `BytesMut` allocation to the multi-folio slow path eliminates 20 GiB of memory allocations and `memcpy` overhead on the reader thread.
5.  **Post-`seek()` First-Read Rule 7 Misclassification in 48-Worker Partitioned Scans**:
    *   In [`ondemand_readahead`](../../src/cached.rs#L979-L1007), Rule 7 (`prev_pos == -1 && page_idx > 0`) intentionally restricts small random reads after `seek()` to exact single-request fetches (`async_size = 0`) so random `seek() + read()` calls do not trigger speculative readahead amplification. However, when Worker $k > 0$ in a 48-worker partitioned sequential scan opens the 20 GiB file, calls `file.seek(SeekFrom::Start(start_offset))`, and issues a large streaming `read(8 MiB)` under `ReadPattern::Auto`, Rule 7 misclassifies that `8 MiB` streaming read as a small random read (`async_size = 0`), stalling each worker for one synchronous HTTP RTT before readahead starts.
6.  **UDS Sidecar and Benchmark Synchronization Gaps**:
    *   [`src/bin/gcsfs_sidecar.rs`](../../src/bin/gcsfs_sidecar.rs#L176-L178) currently sets `DataConfig = None` on `--cache` (disabling readahead in Stack 4), and both the 48-thread Rust benchmark (`examples/bench_live_readahead.rs`) and 48-process Python benchmarks require a post-`open()` readiness barrier so handle setup, `GetObject` metadata RPCs, and OAuth2 handshakes do not skew timed `p50`/`p90` latency or aggregate throughput.

--------------------------------------------------------------------------------

## Goals

*   **Same-Region (`us-west4`) Regional HTTP/JSON Path Evaluation**: Run all data-read benchmarks over the **HTTP/JSON GCS data path (`Transport::Http`)** against `gs://princer-flat/bench/bench_20GB.bin` in `US-WEST4`, targeting **`>= 2.0 GB/s`** for single-worker sequential read and **host network saturation (`15–22+ GB/s`)** for 48-worker sequential read.
*   **Idiomatic Application-Level Concurrency (`1` vs `48` Workers)**: Execute Stack 1 (Native Rust) in this repository across `1` thread and `48` threads sharing `Arc<dyn FileSystem>`, and define the contract for Stacks 2, 3, and 4 (executed in the `gcsfs` repository) across `1` OS process and `48` OS processes (`multiprocessing` `spawn` context) synchronized via a readiness barrier after client/handle initialization.
*   **Three-Part Workload Parity (`20 GiB`)**: Measure and report identical metrics (`p50`, `p90`, `p99`, `mean` latency in milliseconds/microseconds, IOPS, and `MB/s` / `MiB/s` throughput) across all four stacks for:
    1.  Uncached `info` metadata lookup latency (direct GCS call without local `DirCache` hits).
    2.  `1 MiB` random read latency (`1` worker and `48` workers).
    3.  `20 GiB` sequential read latency and throughput with prefetch enabled (`1` worker and `48` workers).
*   **Machine-Readable and Markdown Output**: Emit structured JSON result artifacts from Stack 1 (`--json-out`) alongside formatted Markdown comparison tables.

--------------------------------------------------------------------------------

## Non-Goals

*   **GCS gRPC (`BidiReadObject` / DirectPath) Data Transport**: Evaluating `Transport::Grpc` for object data reads against GCS is out of scope for this benchmark suite; all four stacks strictly use the **HTTP/JSON data path (`Transport::Http`)** for GCS object reads. *(Local UDS gRPC is used solely for IPC between the thin Python client and `gcsfs-sidecar` in Stack 4, while `gcsfs-sidecar` itself reads object data from GCS via `--transport http`).*
*   **Cached Metadata (`DirCache` Hit) Benchmarks in Workload 1**: Benchmarking in-memory `DirCache` warm hits for `info()` is out of scope; Workload 1 measures **only uncached `info` latency** against GCS.
*   **Python / PyO3 Code inside `gcs-rust-fs`**: Creating Python benchmark scripts or PyO3 wrapper crates inside `gcs-rust-fs` is out of scope; all Python/PyO3 code and executions for Stacks 2, 3, and 4 live in the `gcsfs` repository.
*   **Zonal (`Rapid`) and Multi-Region Buckets**: Benchmarking zonal buckets (`princer-rapid-bucket`) or multi-region buckets is out of scope; this plan targets **only** the regional bucket in `US-WEST4` (`gs://princer-flat/bench/bench_20GB.bin`).
*   **Write / Upload / Delete Benchmarks**: Streaming writes (`put_file`, `pipe_file`) and metadata mutations (`rm`, `mv`) are out of scope for this read and `info` benchmark suite.
*   **Multi-Node Distributed Cluster Orchestration**: All benchmarks run on a single host VM (`c4-standard-192`, 192 vCPUs, 708 GiB RAM, `us-west4-a`).
*   **Shared-Memory (`memfd_create` / `/dev/shm`) IPC Daemons**: Out-of-process shared-memory ring buffers are out of scope.

--------------------------------------------------------------------------------

## Open Questions

*   None currently open; HTTP client pool striping, sliding-window pacing, and UDS streaming semantics are specified in the Technical Plan below.

--------------------------------------------------------------------------------

## Critical User Journeys (CUJs)

### CUJ 1: Uncached Metadata `info` Latency Comparison Across 4 Stacks in `us-west4`

*   **Given**: The target object `gs://princer-flat/bench/bench_20GB.bin` in `US-WEST4` and initialized client instances for Native Rust (`GcsFs`), Pure Python `gcsfs`, `gcsfs` + `gcsfs_rust` (PyO3), and `gcsfs` + `gcsfs-sidecar` (gRPC UDS) with metadata caching bypassed/disabled.
*   **When**: The benchmark runner executes `30` uncached `info(path)` iterations directly against GCS.
*   **Then**: The runner records per-call network latencies and outputs `mean`, `p50`, `p90`, and `p99` latencies in milliseconds for all four stacks.

### CUJ 2: Small I/O (`1 MiB`) Random Read Latency Over HTTP/JSON at `1` Worker and `48` Workers

*   **Given**: The 20 GiB target object (`20,480` distinct `1 MiB`-aligned blocks) in `gs://princer-flat/bench/bench_20GB.bin` and a deterministic pseudo-random offset sequence per worker (`seed = 0x5EED_206B ^ worker_id`).
*   **When**: `1` worker (`1` Rust thread or `1` Python process) or `48` synchronized workers (`48` Rust threads or `48` Python processes) execute `1 MiB` positional HTTP range reads (`cat_file` / `read_range`) after passing a post-`open()` readiness barrier.
*   **Then**: The runner verifies that random reads do not trigger speculative sequential readahead amplification, records per-operation latencies across all workers, and reports `p50`, `p90`, `p99` latency, aggregate IOPS, and aggregate `MB/s` across all four stacks.

### CUJ 3: 20 GiB Sequential Read Over HTTP/JSON with Prefetch Enabled (`>= 2 GB/s` at `1` Worker, Network Saturation at `48` Workers)

*   **Given**: The 20 GiB target object in `gs://princer-flat/bench/bench_20GB.bin` with HTTP/JSON prefetch/readahead enabled across all four stacks (`CachedFs` Mode 1 Async Readahead over `Transport::Http` in Native Rust, PyO3, and UDS Sidecar; `readahead` buffering in Pure Python `gcsfs`).
*   **When**: `1` worker streams the full 20 GiB file in `8 MiB` sequential chunks, or `48` synchronized workers concurrently stream 48 disjoint `8 MiB`-aligned partitions (`432 MiB` / `424 MiB`, ~`426.67 MiB` mean; `20 GiB` aggregate scan) in `8 MiB` chunks.
*   **Then**: Single-worker sequential read achieves `>= 2.0 GB/s` with sub-millisecond `p50` chunk latency, 48-worker sequential read scales to host network saturation (`15–22+ GB/s`) for in-process Rust/PyO3 while quantifying the UDS socket copy ceiling in Stack 4, and the runner outputs `p50`, `p90`, `p99`, wall time, and `MB/s` (`MiB/s`).

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. System Topology & Caching Modes Across the Four Stacks (HTTP/JSON GCS Path)

In `gcs-rust-fs`, [`CachedFs`](../../src/cached.rs) supports two distinct data-path modes:
*   **Mode 1 — Asynchronous Streaming Readahead (`Index::Private`)**: Maintains a private per-handle folio ring with drop-behind eviction (`evict_before` as the read cursor advances) and Linux-style on-demand readahead state (`ondemand_readahead` Rules 1–7).
*   **Mode 2 — Shared Multi-Handle Page Cache (`Index::Shared` / `PageCache`)**: Shares downloaded folios across all file handles in the process using a concurrent sharded page table with CLOCK second-chance eviction bounded by a byte capacity (`page_cache_mb`).
*   **Mode 1 — Asynchronous Streaming Readahead (`Index::Private`)**: Maintains a private per-handle folio map (`BTreeMap<u64, Arc<Folio>>`) with drop-behind eviction (`retain` as the read cursor advances) and Linux-style on-demand readahead state (`ondemand_readahead` Rules 1–7).
*   **Mode 2 — Shared Multi-Handle Page Cache (`Index::Shared` / `PageCache`)**: Shares downloaded folios across all file handles in the process using a per-object page directory (`Mutex<HashMap<ObjectKey, Arc<ObjectPages>>>`) with a logical-tick LRU recency map (`BTreeMap<u64, (ObjectKey, u64)>`) bounded by a byte capacity (`page_cache_mb`).

The benchmark harness evaluates four distinct client architectures over the **HTTP/JSON GCS data path (`Transport::Http`)** against `gs://princer-flat/bench/bench_20GB.bin` in `US-WEST4`:

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                     TARGET: gs://princer-flat/bench/bench_20GB.bin                     │
│                (20.00 GiB Regional Bucket, US-WEST4, HTTP/JSON Data Path)              │
└───────────────▲───────────────────────▲──────────────────────▲─────────────────────────┘
                │ HTTP Range GET        │ HTTP Range GET       │ HTTP Range GET
   ┌────────────┴───────────┐  ┌────────┴───────────┐  ┌───────┴─────────────────────────┐
   │ STACK 1: Native Rust   │  │ STACK 2: Python    │  │ STACK 3: Python + PyO3 Binding  │
   │ (1 Process, 1T vs 48T) │  │ Pure gcsfs         │  │ (1P vs 48 OS Processes)         │
   │ [Executed in this repo]│  │ [Executed in gcsfs]│  │ [Executed in gcsfs repo]        │
   │  bench_live_readahead  │  │  GCSFileSystem     │  │  Python Worker Process (x1..48) │
   │  ├── Transport::Http   │  │  ├── fsspec.asyn   │  │  ├── PyO3 FFI (GIL released)    │
   │  ├── CachedFs (Mode1/2)│  │  ├── aiohttp JSON  │  │  ├── CachedFs (Mode 1 Readahead)│
   │  └── Striped HTTP Pool │  │  └── 1 Loop / Proc │  │  └── Transport::Http + Tokio    │
   └────────────────────────┘  └────────────────────┘  └─────────────────────────────────┘

   ┌─────────────────────────────────────────────────────────────────────────────────────┐
   │ STACK 4: Python gcsfs over gRPC UDS Sidecar (1P vs 48 Python Client OS Processes)   │
   │ [Sidecar built in this repo; Python client benchmark executed in gcsfs repo]        │
   │                                                                                     │
   │  Python Worker Process #0 .. #47                                                    │
   │  └── UdsGCSFileSystem / UdsBufferedFile (grpc.insecure_channel("unix:///tmp/..."))  │
   │         │                                                                           │
   │         ▼  Local UDS IPC (ReadResponse 2 MiB–3.5 MiB frames)                        │
   │  Rust Sidecar Daemon (`gcsfs-sidecar --transport http --readahead`)                 │
   │  ├── FileSystemRpcService (Info, List, Read, Write)                                 │
   │  ├── Shared CachedFs (Mode 1 Readahead / Mode 2 PageCache)                          │
   │  └── Striped Transport::Http Client Pool ──► GCS Regional Bucket (HTTP/JSON)        │
   └─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2. Core Throughput Architecture Optimizations in `gcs-rust-fs` (`Transport::Http`)

To lift single-threaded sequential HTTP read from `~700 MB/s` to **`>= 2.0 GB/s`** and 48-thread sequential HTTP read from `~3.5 GB/s` to **network saturation (`15–22+ GB/s`)**, three targeted improvements are made to [`src/gcs/backend.rs`](../../src/gcs/backend.rs) and [`src/cached.rs`](../../src/cached.rs):

#### A. Multi-Client HTTP Connection Striping ([`src/gcs/backend.rs`](../../src/gcs/backend.rs))
*   **Root Cause**: [`Backend::connect`](../../src/gcs/backend.rs#L124-L152) creates only a single `google_cloud_storage::client::Storage` instance. On `Transport::Http`, a single `Storage` client multiplexes concurrent range reads over a shared HTTP/2 connection pool, capping single-instance throughput behind single-connection TCP/HTTP2 window limits.
*   **Mechanism**:
    1.  Extend `Backend` to hold a striped pool of HTTP `Storage` clients (`storage_pool`, configurable via `GcsFsBuilder::http_pool_size(n)` or `GCS_RUST_FS_HTTP_POOL_SIZE`). When `Transport::Http` is active, default the pool size to `16` clients; when `Transport::Grpc` is active, default to `1` client so gRPC callers incur zero extra initialization cost.
    2.  In [`Backend::read_range_http`](../../src/gcs/backend.rs#L305-L318), select the next `Storage` client via an atomic round-robin counter so concurrent 8–16 MiB readahead folios execute across independent HTTP/TLS connection pools in parallel.

#### B. Continuous FIFO Sliding Readahead Window, Dynamic Folio Scaling, & Post-`seek` Streaming Ramp ([`src/cached.rs`](../../src/cached.rs))
*   **Root Cause**: Previously, triggering Window 1 (`256..512 MiB`) while Window 0 (`0..256 MiB`) was still `Loading` without per-handle in-flight pacing caused all 32 folios across both windows to race across worker threads and stall at window boundaries. Furthermore, under `ReadPattern::Auto`, `submit_window` kept `target_folio_pages` pinned to `io_pages` even after the sequential window ramped up, capping throughput when `io_size = 1 MiB`, and `submit_async_readahead` deferred window registration to a spawned task that could be outrun when the reader sliced through `Ready` folios in memory.
*   **Mechanism**:
    1.  **Per-Handle In-Flight Semaphore (`handle_semaphore: Arc<Semaphore>`) with Eager FIFO Acquisition**: Each `ReadaheadFile` initializes a per-handle semaphore with `max_inflight` permits (`24` for `1T`, `4` per handle across 48 handles for `48T`). In `fetch_folio_range`, permits are claimed eagerly in loop submission order via `try_acquire_owned()` when available so Window 0 folios never race with Window 1 tasks across multi-threaded workers. As each folio in Window 0 completes, it releases its permit and immediately unblocks the next queued folio in Window 1 in FIFO order. On non-contiguous `seek()`, `cancel_token` cancels in-flight downloads via `tokio::select!` and `handle_semaphore` is reset so pre-seek tasks never starve post-seek reads.
    2.  **Dynamic Folio Scaling in `ReadPattern::Auto` (`submit_window`)**: When sequential readahead is active (`async_size > 0`), `target_folio_pages` scales dynamically from `io_pages` (e.g., `1 MiB`) up to `max_folio_pages` (`16 MiB`) via `(size / max_inflight).max(io_pages).min(max_folio_pages)`, while standalone random reads (`async_size == 0`) remain bounded to `io_pages`.
    3.  **Initial Window Floor & Synchronous Window Registration**: In `ondemand_readahead`, the initial sequential window is floored at `(req_pages * 2).max(io_pages * 2).max(8).min(ra.ra_pages)` so even `1 MiB` `read()` calls fetch full `io_size` folios from the start. In `submit_async_readahead`, `submit_window` is called synchronously to insert `Loading` folios into `self.index` immediately when `readahead_mark` is touched.
    4.  **Post-`seek()` Streaming Ramp vs. Rule 7 Random Isolation**: Under `ReadPattern::Auto` in `ondemand_readahead`, when `prev_pos == -1 && page_idx > 0`, trigger the initial Rule 1 ramp when the read request size is at least `io_size`, while preserving Rule 7 single-read isolation (`async_size = 0`) for smaller random reads (`64 KiB` and `1 MiB`).

#### C. Zero-Copy `Bytes::slice` Fast Path in `ReadaheadFile::read` ([`src/cached.rs`](../../src/cached.rs))
*   **Root Cause**: `ReadaheadFile::read` eagerly allocated `BytesMut::with_capacity(target_len)` before entering its read loop and copied bytes out of each folio, burning ~20 GiB of heap allocation and `memcpy` bandwidth during a 20 GiB read.
*   **Mechanism**: Defer `BytesMut` allocation until a read actually spans across multiple folios. When the first `Ready` folio has enough available bytes to satisfy `target_len`, return a zero-copy `Bytes` slice directly ($O(1)$ atomic refcount increment, zero heap allocation, zero `memcpy`) after updating the file cursor and drop-behind eviction state.

### 3. Synchronization and Worker Lifecycle Protocol

To prevent handle creation, `GetObject` metadata RPCs, Python interpreter boot time, and initial OAuth2 token exchange from polluting I/O latency and throughput measurements in both 48-thread Rust runs and 48-process Python runs:

1.  **Phase A — Worker Spawn & Client/Handle Warmup (Untimed)**:
    *   **In Rust (Stack 1, [`examples/bench_live_readahead.rs`](../../examples/bench_live_readahead.rs))**: Before starting the benchmark timer, each of the `T` (`1` or `48`) Tokio worker tasks opens its `File` handle (`fs.open(...)`, completing the initial `GetObject` metadata lookup) and seeks to its starting partition offset. All `T` tasks then await a shared `tokio::sync::Barrier::new(T + 1)`.
    *   **In Python (Stacks 2, 3, and 4, executed in `gcsfs`)**: The orchestrator uses `multiprocessing.get_context("spawn")` to create `P` (`1` or `48`) worker processes with a shared `multiprocessing.Barrier(P + 1, timeout=60)`. Each worker initializes its client over the HTTP/JSON path and performs a single warmup `info()` call before waiting on the barrier. In Stack 3 (`PyO3`), `48`-process runs set `GCS_RUST_FS_HTTP_POOL_SIZE=2` and `GCSFS_RUST_WORKER_THREADS=2` ($48 \times 2 = 96$ HTTP clients and $96$ Tokio threads total across the host).
2.  **Phase B — Barrier Release & Timed Execution (Timed)**:
    *   Once all workers reach the readiness barrier, the coordinator releases the barrier and records `Instant::now()` / `time.perf_counter()` immediately after barrier release so only pure data I/O is timed.
    *   Each worker records per-operation latencies in a thread/process-local `Vec<f64>` / `list[float]`.
3.  **Phase C — Aggregation**:
    *   Upon completion, workers return their raw latency arrays and byte counts to the coordinator, which computes global `mean`, `p50`, `p90`, `p99`, `IOPS`, and `MB/s` (`10^6` bytes/s) / `MiB/s` (`2^20` bytes/s) over wall-clock time.

### 4. Three-Part Workload Specification on `gs://princer-flat/bench/bench_20GB.bin` (HTTP/JSON Path)

#### Workload 1: Uncached Metadata `info` Latency (`p50`, `p90`, `p99`, `mean`)

Measures single-worker **uncached** metadata lookup latency on `gs://princer-flat/bench/bench_20GB.bin` directly against GCS (no local `DirCache` warm hits). *(Note: In Stacks 1, 3, and 4, uncached `GcsFs::info` uses the SDK's `StorageControl::get_object` gRPC client per [`src/gcs/backend.rs`](../../src/gcs/backend.rs#L31-L35), whereas Stack 2 Pure Python `gcsfs` uses the HTTP/JSON metadata endpoint).*

| Test | Mechanism Across All 4 Stacks | Iterations | Primary Bottleneck Measured |
| :--- | :--- | :--- | :--- |
| **Uncached `info`** | Direct `GcsFs::info` (Stack 1), `fs.info()` with `dircache.clear()` per call (Stack 2), `PyGcsFs.info_uncached()` (Stack 3), Sidecar without metadata `--cache` (Stack 4) | 30 | Same-region (`us-west4`) GCS metadata RTT + TLS + parsing + UDS hop (Stack 4) |

#### Workload 2: Small I/O (`1 MiB`) Random Read Latency Over HTTP/JSON (`1` Worker vs `48` Workers, `ReadPattern::Auto`)

Measures positional `1 MiB` (`1,048,576` bytes) random HTTP range read latency across the 20 GiB (`21,474,836,480` bytes) object with `read_size = 1 MiB` and `io_size = 1 MiB`:

*   **Offset Generation**: Generates 480 non-overlapping `4 MiB`-aligned byte offsets across `[0, 20 GiB)` using deterministic `splitmix64` permutation so random reads never coalesce unless testing warm cache.
*   **Sample Counts**:
    *   **Single Worker (`1` Rust thread / `1` Python process)**: `50` random `1 MiB` reads (`50 MiB` total).
    *   **Multi-Worker (`48` Rust threads / `48` Python processes)**: `10` random `1 MiB` reads per worker (`480` reads = `480 MiB` total across 48 concurrent workers).
*   **API Entry Points (HTTP/JSON Only, `ReadPattern::Auto`)**:
    *   **Stack 1 (Native Rust, `Transport::Http`)**: `FileSystem::cat_file(path, ByteRange::span(offset, offset + 1048576))` on Direct `GcsFs` (HTTP) and `File::seek` + `File::read(1 MiB)` on `CachedFs` Mode 1 (`ReadPattern::Auto` Rule 7 single-read isolation) and Mode 2 (`PageCache` cold and warm hits).
    *   **Stack 2 (Pure Python `gcsfs`, HTTP)**: `fs.cat_file(path, start=offset, end=offset + 1048576)` (and `f.seek(offset); f.read(1048576)` with `cache_type="none"`).
    *   **Stack 3 (`gcsfs_rust` PyO3, `Transport::Http`)**: `py_fs.read_range(path, offset, offset + 1048576)` releasing the GIL via `py.allow_threads`.
    *   **Stack 4 (`gcsfs` over gRPC UDS, Sidecar `--transport http`)**: `uds_fs.cat_file(path, start=offset, end=offset + 1048576)` issuing a bounded `1 MiB` `ReadRequest` over UDS. Inside [`FileSystemRpcService::read`](../../src/rpc/server.rs#L229-L302), bounded requests with `(end - start) <= 4 MiB` use `self.fs.cat_file` directly when `(end - start) <= chunk_size` (or `ReadPattern::Random` otherwise) so `1 MiB` random reads neither pay an extra `open()` `GetObject` RPC nor trigger sequential readahead amplification.

#### Workload 3: Sequential Read over `20 GiB` Files with Prefetch Enabled (`1` Worker = `20 GiB` vs `48` Workers = `48 × 20 GiB = 960 GiB`, `ReadPattern::Auto`)

Measures sequential streaming read latency (`p50`, `p90`) and sustained throughput (`MB/s` and `MiB/s`) over `gs://princer-flat/bench/bench_20GB.bin` using `Transport::Http` and `ReadPattern::Auto`:

*   **Application Read Size & Readahead IO Size**: Configurable via `--read-size-mb <MIB>` (default `8 MiB`) and `--io-size-mb <MIB>` (default `16 MiB`, with `max_folio_size = 16 MiB`).
*   **Single-Worker (`1T` / `1P`) Execution (`1 × 20 GiB = 20 GiB`)**:
    *   Streams a full `20 GiB` (`21,474,836,480` bytes) object from offset `0` to `20 GiB` with `ReadPattern::Auto`.
*   **Multi-Worker (`48T` / `48P`) Execution Across 48 Distinct 20 GiB Objects (`48 × 20 GiB = 960 GiB` / `1.03 TB` Total)**:
    *   Each of the `48` workers opens and streams its own dedicated `20 GiB` object (`gs://princer-flat/bench/bench_20GB_00.bin` .. `gs://princer-flat/bench/bench_20GB_47.bin`, provisioned via server-side GCS `copy_file` rewrite) from `0` to `20 GiB` using `ReadPattern::Auto`, transferring `960 GiB` (`1,030.79 GB`) total per 48-worker run.
*   **Prefetch Configuration Per Stack (HTTP/JSON)**:
    *   **Stack 1 (Native Rust `CachedFs` Mode 1, `Transport::Http`, `ReadPattern::Auto`)**: `page_size = 256 KiB`, `io_size = 16 MiB` (or `1 MiB`), `max_folio_size = 16 MiB`, `max_readahead = 384 MiB` (`1T`, `max_inflight = 24`) / `max_readahead = 64 MiB` (`48T`, `max_inflight = 4` per handle across 48 handles = `192` concurrent folios), `max_inflight_bytes = 3–4 GiB`, striped across `24` (`1T`) / `48` (`48T`) HTTP `Storage` clients.
    *   **Stack 2 (Pure Python `gcsfs`, HTTP)**: `fs.open(path, "rb", block_size=8*1024*1024, cache_type="readahead")`.
    *   **Stack 3 (`gcsfs_rust` PyO3, `Transport::Http`)**: `PyGcsFile` wrapping `gcs_rust_fs::CachedFs<GcsFs>` (`Transport::Http`) with Mode 1 `ReadaheadConfig` and `ReadPattern::Auto`.
    *   **Stack 4 (`gcsfs` over gRPC UDS, Sidecar `--transport http`)**:
        *   `gcsfs-sidecar` started with `--transport http --readahead --http-pool-size 32 --uds /tmp/gcsfs-sidecar.sock`.
        *   `UdsBufferedFile` opens a single server-streaming `ReadRequest(path, range=ByteRange(0, file_size), options=ReadOptions(chunk_size=3*1024*1024))` per worker.

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative A: Evaluating Both GCS `Transport::Grpc` (`BidiReadObject`) and `Transport::Http`

*   **Pros**: Compares DirectPath gRPC streaming against HTTP/JSON range requests.
*   **Cons**: Pure Python `gcsfs` natively targets the HTTP/JSON REST path on regional buckets, and `BidiReadObject` requires per-bucket allowlisting and separate channel/subchannel tuning (`with_grpc_subchannel_count`), expanding the test matrix by 2x and conflating transport protocol differences with storage engine and binding architecture differences.
*   **Reason for Rejection**: Rejected by author: Standardize strictly on the **HTTP/JSON data path (`Transport::Http`)** on the regional `US-WEST4` bucket so all four stacks are compared over the exact same GCS object-read wire protocol.

### Alternative B: Benchmarking Against `gs://princer-bucket` (`US-CENTRAL1`) from `us-west4-a`

*   **Pros**: `gs://princer-bucket/bench/bench_20GB.bin` was already composed and used in initial smoke tests.
*   **Cons**: The compute VM resides in `us-west4-a` while `princer-bucket` is in `US-CENTRAL1`. Cross-region traffic incurs ~35–50 ms of WAN RTT and cross-region egress bandwidth shaping, capping single-threaded HTTP readahead at `747.55 MB/s` and 48-thread readahead at `3,503.46 MB/s`.
*   **Reason for Rejection**: Rejected by author: Cross-region WAN bottlenecks obscure client storage engine performance and prevent measuring same-region network saturation (`>= 2.0 GB/s` single-worker, `15–22+ GB/s` 48-worker) on `gs://princer-flat/bench/bench_20GB.bin` (`US-WEST4`).

### Alternative C: Using 48 Threads (`ThreadPoolExecutor`) for the Python Stacks

*   **Pros**: Uses a single OS process across all four stacks, avoiding `multiprocessing` `spawn` orchestration.
*   **Cons**: In Pure Python `gcsfs`, all 48 threads contend on the Python GIL and serialize through a single background `asyncio` event loop thread (`fsspec.asyn`), pinning throughput at `~610–730 MB/s` regardless of thread count. Real Python data loaders (such as PyTorch `DataLoader(num_workers=48)`) use multi-processing to scale across CPU cores.
*   **Reason for Rejection**: Rejected by author: Benchmarking Python stacks with 48 threads measures Python GIL and single-event-loop contention rather than multi-process application parallelism.

### Alternative D: Using 48 OS Processes for Native Rust (`Stack 1`)

*   **Pros**: Enforces identical OS process boundaries across both Rust and Python.
*   **Cons**: Native Rust applications consuming [`gcs-rust-fs`](../../src/lib.rs) scale via threads/tasks sharing a single in-process `Arc<CachedFs<GcsFs>>` instance, sharing one Mode 2 `PageCache` and one HTTP connection pool. Spawning 48 isolated Rust processes artificially duplicates caches and connections in a way no Rust application uses the library.
*   **Reason for Rejection**: Rejected by author: Concurrency must reflect idiomatic application parallelism for each filesystem interface (`48` threads in `1` process for Native Rust `gcs-rust-fs`; `48` OS processes for Python `gcsfs`).

### Alternative E: Single `Storage` HTTP Client & Unpaced Double-Window Readahead

*   **Pros**: Requires zero changes to [`src/gcs/backend.rs`](../../src/gcs/backend.rs) or [`src/cached.rs`](../../src/cached.rs).
*   **Cons**: A single `Storage` HTTP client caps per-handle bandwidth behind one HTTP/2 connection pool. Furthermore, triggering Window 1 (`256..512 MiB`) at $t = 0$ while Folio 0 is still `Loading` without a per-handle `max_inflight` semaphore causes all 32 folios (`512 MiB`) to download in lockstep and stall every 512 MiB when drained from RAM.
*   **Reason for Rejection**: Rejected by author: Cannot reach the `>= 2.0 GB/s` single-threaded sequential HTTP target or saturate host network bandwidth at 48 workers.

### Alternative F: Issuing Separate `8 MiB` `ReadRequest` RPCs Over UDS in Stack 4

*   **Pros**: Maps each Python `f.read(8 MiB)` call 1:1 to a standalone `ReadRequest` RPC over UDS.
*   **Cons**: [`FileSystemRpcService::read`](../../src/rpc/server.rs#L229-L302) opens a new `File` handle per `ReadRequest` and drops it when the request range completes. Issuing 2,560 separate `8 MiB` RPCs over a 20 GiB scan destroys Mode 1 per-handle readahead state after every chunk.
*   **Reason for Rejection**: Rejected by author: `UdsBufferedFile` must open a long-lived server-streaming `ReadRequest` for the contiguous scan range so the sidecar's `ReadaheadFile` handle maintains its sliding readahead pipeline.

--------------------------------------------------------------------------------

## Non-Functional Requirements

### Performance & Latency Targets (Same-Region `US-WEST4`, HTTP/JSON Path)
*   **Single-Worker Sequential Read (`20 GiB`, Prefetch Enabled, `Transport::Http`)**: Target `>= 2.0 GB/s` (`2,000 MB/s`) in Native Rust (`Stack 1`) and `gcsfs` + PyO3 (`Stack 3`), with `p50` chunk read latency `< 2.0 ms`.
*   **48-Worker Sequential Read (`20 GiB`, Prefetch Enabled, `Transport::Http`)**: Target host network saturation (`15–22+ GB/s`) for Native Rust (`48T`) and `gcsfs` + PyO3 (`48P`), while empirically measuring the UDS kernel socket copy ceiling (`~4–5 GB/s`) in Stack 4.
*   **Uncached Metadata `info` Latency**: Measure direct same-region (`us-west4`) GCS metadata lookup latency (`p50`, `p90`, `p99`, `mean` in milliseconds) without local cache hits across all four stacks.

### Memory & Resource Bounds
*   **Bounded Per-Handle Readahead Footprint**: Mode 1 drop-behind (`Index::Private`) must evict consumed folios behind the read cursor so single-handle resident memory stays bounded to `<= 2 * max_readahead` (`<= 512 MiB` for `1T` where `max_readahead = 256 MiB`, and `<= 256 MiB` per worker for `48T`/`48P` where `max_readahead = 128 MiB` and global `max_inflight_bytes = 4 GiB`).
*   **Bounded Process Thread Count in 48-Process PyO3 (`Stack 3`)**: Each of the 48 Python worker processes must cap its Tokio runtime worker threads (`GCSFS_RUST_WORKER_THREADS=2`) and HTTP client pool (`GCS_RUST_FS_HTTP_POOL_SIZE=2`) so total host threads (`96`) and HTTP client pools (`96`) remain well below OS limits.

### Risks & Mitigations
*   **48-Worker Startup Barrier Deadlock**: If a worker task/process fails during untimed warmup (`fs.open()` or `info()`), waiting indefinitely on a 48-worker barrier would hang the benchmark suite. Mitigated by unwrapping/propagating `open()` errors before barrier entry in Rust and enforcing a `60s` timeout on `multiprocessing.Barrier` in Python.
*   **In-Flight Readahead Cancellation & Handle Permit Starvation on `seek()`**: If a handle seeks backward or far forward while background tasks hold `handle_semaphore` permits, post-seek reads could starve waiting for old HTTP transfers to finish. Mitigated by wrapping `handle_sem.acquire()` and `inner.read_range()` in `tokio::select!` on `cancel_token.cancelled()` and replacing `self.handle_semaphore` with a fresh semaphore in `ReadaheadFile::seek`.
*   **UDS Socket Permissions & Stale Socket Cleanup**: [`src/bin/gcsfs_sidecar.rs`](../../src/bin/gcsfs_sidecar.rs) unlinks any stale socket file before binding and supports `--uds-perm` (`0600` recommended on shared hosts) to restrict local IPC access.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

All changes in this repository are partitioned into three incremental, self-contained phases. Each phase leaves `gcs-rust-fs` in a compilable, testable state. *(All Python/PyO3 code and benchmark executions for Stacks 2, 3, and 4 are implemented and executed separately in the `gcsfs` repository).*

### Phase 1: Core HTTP Client Pool Striping & Continuous Readahead Pipeline

#### 1. [`../../src/gcs/backend.rs`](../../src/gcs/backend.rs) (Modify)

*   **Type / Scope**: Rust GCS Transport Layer (`Transport::Http` Client Pool Striping)
*   **Proposed Changes**:
    *   Add `pub(crate) http_pool_size: Option<usize>` to `BackendOptions`.
    *   Update `Backend` struct to replace `storage: Storage` with `storage_pool: Arc<Vec<Storage>>` and `next_http_client: Arc<AtomicUsize>`.
    *   Preserve `pub(crate) fn storage(&self) -> &Storage { &self.storage_pool[0] }` on `Backend` so `read_range_grpc`, `open_grpc`, and `src/gcs/write.rs` continue to compile without modification.
    *   In `Backend::connect`, compute `let pool_size = options.http_pool_size.unwrap_or(match options.transport { Transport::Http => 16, Transport::Grpc => 1 }).max(1);` and initialize `pool_size` `Storage` client instances concurrently using `futures_util::future::try_join_all`.
    *   In `Backend::read_range_http`, select the client via round-robin index `let idx = self.next_http_client.fetch_add(1, Ordering::Relaxed) % self.storage_pool.len();` and call `self.storage_pool[idx].read_object(...)`.
*   **Rationale**: Eliminates single-client HTTP/2 connection multiplexing bottlenecks for `Transport::Http` while keeping single-client initialization for `Transport::Grpc` and guarding against zero-size pools.

#### 2. [`../../src/gcs/fs.rs`](../../src/gcs/fs.rs) (Modify)

*   **Type / Scope**: Public Builder API (`GcsFsBuilder`)
*   **Proposed Changes**:
    *   Define `pub const HTTP_POOL_SIZE_ENV_VAR: &str = "GCS_RUST_FS_HTTP_POOL_SIZE";`.
    *   Add `http_pool_size: Option<usize>` to `GcsFsBuilder` and add builder method `pub fn http_pool_size(mut self, count: usize) -> Self` (storing `Some(count)`).
    *   In `GcsFsBuilder::build`, validate `if matches!(self.http_pool_size, Some(0))` and return `Err(Error::invalid_config("gcs", "http_pool_size must be at least 1"))`, passing `http_pool_size: self.http_pool_size` into `BackendOptions`.
    *   In `GcsFsBuilder::from_env()`, when `self.http_pool_size.is_none()`, read `HTTP_POOL_SIZE_ENV_VAR` (`GCS_RUST_FS_HTTP_POOL_SIZE`), parse as `usize`, reject `0` or parse errors with `Error::invalid_config("gcs", ...)`, and set `self.http_pool_size = Some(val)`.
*   **Rationale**: Allows native Rust benchmarks, `gcsfs-sidecar`, and PyO3 workers to configure the per-process HTTP client pool size via builder method or environment variable.

#### 3. [`../../src/cached.rs`](../../src/cached.rs) & [`../../tests/page_cache.rs`](../../tests/page_cache.rs) (Modify)

*   **Type / Scope**: Asynchronous Readahead Engine & Page Cache (`ReadaheadFile`) + Unit Tests
*   **Proposed Changes**:
    *   **Per-Handle In-Flight Semaphore**:
    *   **Per-Handle In-Flight Semaphore & Eager FIFO Pacing**:
        *   Add `handle_semaphore: Arc<Semaphore>` to `ReadaheadFile`, initialized in `ReadaheadFile::new` with `Arc::new(Semaphore::new(runtime.config.max_inflight.max(1)))`, and clone the `Arc` in `ReadaheadFile::clone_handle`.
        *   In `ReadaheadFile::seek`, when a non-contiguous seek resets `self.cancel_token`, also replace `self.handle_semaphore = Arc::new(Semaphore::new(self.runtime.config.max_inflight.max(1)));` so cancelled pre-seek tasks cannot starve post-seek reads of handle permits.
        *   In `ReadaheadFile::fetch_folio_range`, clone `let handle_sem = Arc::clone(&self.handle_semaphore);` and move `handle_sem` **into the `let fut = async move { ... }` closure**. Inside `fut`, acquire `_handle_permit` before `semaphore.acquire_many(fetch_len)` and wrap both permit acquisition and `inner.read_range(range)` in `tokio::select! { biased; _ = t.cancelled() => return Err(Arc::new(Error::Cancelled)), ... }` so cancelled tasks immediately abort and release permits.
    *   **Mid-Window Readahead Marker & Post-`seek` Streaming Ramp**:
        *   In `ReadaheadFile::ondemand_readahead` (Rules 1, 2, 4, and `ReadPattern::Sequential`) and `ReadaheadFile::submit_async_readahead`, set `async_size = (count / 2).max(1)` (instead of `count`) so `readahead_mark` is placed at the midpoint of each window (`128 MiB` of a `256 MiB` window) rather than Folio 0 (`0 MiB`).
        *   In `ReadaheadFile::ondemand_readahead` under `ReadPattern::Auto`, when `ra.prev_pos == -1` and `page_idx > 0`, if `req_pages.saturating_mul(self.runtime.config.page_size) >= self.runtime.config.io_size` (e.g. `>= 8 MiB` partitioned streaming read), execute Rule 1 initial ramp (`size = get_init_ra_size(...)`, `async_size = (size / 2).max(1)`); otherwise preserve Rule 7 single-read isolation (`size = req_pages.max(1)`, `async_size = 0`) for small random reads (`64 KiB`, `1 MiB`).
        *   In `ReadaheadFile::fetch_folio_range`, eagerly call `let handle_permit_opt = Arc::clone(&self.handle_semaphore).try_acquire_owned().ok();` synchronously on the caller thread before `tokio::spawn` to preserve strict FIFO ordering of folio fetches across concurrent background tasks, falling back to `handle_sem.acquire_owned().await` inside `fut` only when all `max_inflight` slots are busy. Wrap both permit acquisition and `inner.read_range(range)` in `tokio::select! { biased; _ = t.cancelled() => return Err(Arc::new(Error::Cancelled)), ... }` so cancelled tasks immediately abort and release permits.
    *   **Early Readahead Marker & Post-`seek` Streaming Ramp**:
        *   In `ReadaheadFile::ondemand_readahead` (Rules 1, 2, 4, and `ReadPattern::Sequential`) and `ReadaheadFile::submit_async_readahead`, set `async_size = count` so `readahead_mark` is placed at Folio 0 (`start`) of each window, keeping `handle_semaphore` (`max_inflight`) continuously pipelined with the next window while the reader consumes the current window.
        *   In `ReadaheadFile::ondemand_readahead` under `ReadPattern::Auto`, when `ra.prev_pos == -1` and `page_idx > 0`, if `req_pages.saturating_mul(self.runtime.config.page_size) >= self.runtime.config.io_size` (e.g. `>= 8 MiB` partitioned streaming read), execute Rule 1 initial ramp (`size = get_init_ra_size(...)`, `async_size = size`); otherwise preserve Rule 7 single-read isolation (`size = req_pages.max(1)`, `async_size = 0`) for small random reads (`64 KiB`, `1 MiB`).
    *   **Zero-Copy Single-Folio Fast Path in `ReadaheadFile::read`**:
        *   Replace the eager `let mut buf = BytesMut::with_capacity(target_len);` before the `while` loop with `let mut buf = BytesMut::new();`.
        *   Inside the loop, when `buf.is_empty()` and the first `Ready` folio has `avail >= target_len`, slice `let out = folio_data.slice(offset_in_folio..offset_in_folio + target_len);`, update `self.pos`, `self.ra_state.lock().prev_pos`, and Mode 1 drop-behind eviction (`Index::Private`), and return `Ok(out)` immediately without allocating `BytesMut`. When `avail < target_len` (multi-folio boundary read), lazily reserve `if buf.capacity() == 0 { buf.reserve(target_len); }` and copy via `buf.extend_from_slice(...)`.
        *   Inside the loop, when `buf.is_empty()` and the first `Ready` folio has `avail >= target_len`, slice `let out = folio_data.slice(offset_in_folio..offset_in_folio + target_len);`, update `self.pos`, `self.ra_state.lock().unwrap().prev_pos`, and Mode 1 drop-behind eviction (`Index::Private`), and return `Ok(out)` immediately without allocating `BytesMut`. When `avail < target_len` (multi-folio boundary read), lazily reserve `if buf.capacity() == 0 { buf.reserve(target_len); }` and copy via `buf.extend_from_slice(...)`.
    *   **Unit Test in [`tests/page_cache.rs`](../../tests/page_cache.rs)**:
        *   Add a deterministic test verifying that `ReadaheadFile` respects `handle_semaphore` (`max_inflight`), returns zero-copy slices on single-folio reads, and preserves `test_3_standalone_random_read_rule_7` while ramping on `>= io_size` post-`seek` reads.
*   **Phase 1 Verification Checkpoint**:
    *   `PROTOC=/home/princer_google_com/c1dev/gcsfs/.venv/lib/python3.14/site-packages/torch/bin/protoc cargo test --test page_cache`

--------------------------------------------------------------------------------

### Phase 2: UDS Sidecar Readahead & Range-Span Pattern Selection

#### 4. [`../../src/rpc/server.rs`](../../src/rpc/server.rs) (Modify)

*   **Type / Scope**: UDS gRPC Server Handler (`FileSystemRpcService::read`)
*   **Proposed Changes**:
    *   In `FileSystemRpcService::read`, inspect `req.range` (`Option<proto::ByteRange>`) after converting to `range: ByteRange` and determining `chunk_size`:
        *   **Bounded Small Random Read Fast Path (`span <= 4 MiB`)**: When `req.range` has both `start` and `end: Some(end)` with `end > start` and `(end - start) <= 4 * 1024 * 1024`:
            *   If `(end - start) as usize <= chunk_size`, call `let data = self.fs.cat_file(&req.path, range).await.map_err(fs_err_to_status)?;` directly and send a single `ReadResponse { data, offset: start, eof: true }` over the stream (avoiding a separate `open()` `GetObject` metadata RPC on `1 MiB` random reads).
            *   If `(end - start) as usize <= chunk_size`, call `let data = self.fs.cat_file(&req.path, range).await.map_err(fs_err_to_status)?;` directly and send a single `ReadResponse { data }` over the stream (avoiding a separate `open()` `GetObject` metadata RPC on `1 MiB` random reads).
            *   Otherwise (`chunk_size < span <= 4 MiB`), open with `OpenOptions::read().with_block_size(chunk_size).with_pattern(ReadPattern::Random)`.
        *   **Sequential Streaming Range (`unbounded` or `span >= 16 MiB`)**: When `req.range` is `None`, has `end: None`, or `end.saturating_sub(start) >= 16 * 1024 * 1024`, open with `OpenOptions::read().with_block_size(8 * 1024 * 1024).with_pattern(ReadPattern::Sequential)`.
        *   **Intermediate Range (`4 MiB < span < 16 MiB`)**: Open with `OpenOptions::read().with_block_size(chunk_size).with_pattern(ReadPattern::Auto)`.
    *   Increase the internal streaming `mpsc::channel` capacity in `FileSystemRpcService::read` from `4` to `16` frames so UDS socket writes pipeline ahead of `handle.read()`.
    *   Increase the internal streaming `mpsc::channel` capacity in `FileSystemRpcService::read` from `4` to `32` frames so UDS socket writes pipeline ahead of `handle.read()`.
*   **Rationale**: Eliminates the `(4 MiB, 16 MiB)` dead zone, avoids redundant `GetObject` metadata RPCs on `1 MiB` random UDS reads, and enables full Mode 1 readahead for `>= 16 MiB` partition scans over UDS.

#### 5. [`../../src/bin/gcsfs_sidecar.rs`](../../src/bin/gcsfs_sidecar.rs) & [`../../tests/rpc.rs`](../../tests/rpc.rs) (Modify)

*   **Type / Scope**: Sidecar Daemon CLI Binary & UDS RPC Integration Tests
*   **Proposed Changes**:
    *   Add `--readahead` (`bool`), `--page-cache-mb <MB>` (`Option<usize>`), and `--http-pool-size <N>` (`Option<usize>`) flags to `SidecarArgs` in `src/bin/gcsfs_sidecar.rs`.
    *   When `args.http_pool_size` is `Some(n)`, pass `.http_pool_size(n)` to `GcsFs::builder()`.
    *   Wrap `GcsFs` in `CachedFs` whenever `args.cache || args.readahead || args.page_cache_mb.is_some()`. When `args.readahead` or `args.page_cache_mb.is_some()` is set, construct `DataConfig`:
        *   `readahead`: when `args.readahead` is true, use `ReadaheadConfig { page_size: 256 * 1024, io_size: 16 * 1024 * 1024, max_folio_size: 16 * 1024 * 1024, max_readahead: 256 * 1024 * 1024, max_inflight: 16, max_inflight_bytes: 4 * 1024 * 1024 * 1024 }`; otherwise set `max_readahead: 0` (disabled).
        *   `cache`: when `args.page_cache_mb` is `Some(mb)`, construct `Some(PageCache::new(mb * 1024 * 1024))` (or `None` for Mode 1 private readahead).
    *   In `tests/rpc.rs`, add an integration test verifying `FileSystemRpcService` with `CachedFs` (`DataConfig` enabled) across `1 MiB` bounded random reads (`<= 4 MiB`) and `>= 16 MiB` sequential streaming reads.
*   **Phase 2 Verification Checkpoint**:
    *   `PROTOC=/home/princer_google_com/c1dev/gcsfs/.venv/lib/python3.14/site-packages/torch/bin/protoc cargo test --test rpc`

--------------------------------------------------------------------------------

### Phase 3: Stack 1 Native Rust 20 GiB Benchmark Suite (`US-WEST4`, `Transport::Http`, `ReadPattern::Auto`)

#### 6. [`../../examples/bench_live_readahead.rs`](../../examples/bench_live_readahead.rs) (Modify)
#### 6. [`../../examples/bench_live_readahead.rs`](../../examples/bench_live_readahead.rs) & [`../../examples/bench_uds_readahead.rs`](../../examples/bench_uds_readahead.rs) (Modify)

*   **Type / Scope**: Stack 1 Native Rust Benchmark Binary (`1T` vs `48T`, `US-WEST4`, `Transport::Http`, `ReadPattern::Auto`)
*   **Type / Scope**: Stack 1 Native Rust & UDS Sidecar Benchmark Binaries (`1T`, `24T`, `48T`, `US-WEST4`, `Transport::Http`, `ReadPattern::Auto`)
*   **Proposed Changes**:
    *   **Target, Transport, & CLI Flags**:
        *   Default target URI to `gs://princer-flat/bench/bench_20GB.bin` (`US-WEST4`) and execute exclusively over `Transport::Http` using `ReadPattern::Auto`.
        *   Support `--only <all|info|random|seq|seq-1t|seq-48t>`, `--read-size-mb <MIB>` (default `8`), `--io-size-mb <MIB>` (default `16`), and `--json-out <PATH>`.
        *   Construct `gcs_http_1t` with `GcsFs::builder().transport(Transport::Http).http_pool_size(24).from_env()?.build().await?` for `1T` runs and `gcs_http_48t` with `GcsFs::builder().transport(Transport::Http).http_pool_size(48).from_env()?.build().await?` for `48T` runs, warming connection pools before timed measurement.
        *   Support `--only <all|info|random|seq|seq-1t|seq-24t|seq-48t|write>`, `--read-size-mb <MIB>` (default `8`), `--io-size-mb <MIB>` (default `16`), and `--json-out <PATH>`.
        *   Construct `gcs_http_1t` with `GcsFs::builder().transport(Transport::Http).http_pool_size(24).from_env()?.build().await?`, `gcs_http_24t` with `.http_pool_size(24)`, and `gcs_http_48t` with `.http_pool_size(48)`, warming connection pools before timed measurement.
    *   **Part 1 (`info` Latency — Uncached Only)**:
        *   Run only the **uncached** `Direct GcsFs::info` benchmark (`30` iterations against `gs://princer-flat/bench/bench_20GB.bin`).
    *   **Post-`open()` Readiness Barrier (`tokio::sync::Barrier`)**:
        *   In `bench_random` and `bench_sequential`, have all `threads` (`1` or `48`) open their `File` handles (`fs_c.open(...)`) *before* waiting on `Arc<tokio::sync::Barrier>::new(threads + 1)`. Record `let start = Instant::now();` in the coordinator immediately after `barrier.wait().await` releases all workers simultaneously.
    *   **Workload 2 & Workload 3 Parameter Alignment**:
        *   In Part 2 (`1 MiB` random reads, `read_size = 1 MiB`, `io_size = 1 MiB`), run `50` reads for `1T` and `480` reads (`10` per thread) for `48T` across non-overlapping `4 MiB`-aligned offsets.
        *   In Part 3 (`20 GiB` sequential read, `ReadPattern::Auto`), `1T` reads `1 × 20 GiB = 20 GiB` from `gs://princer-flat/bench/bench_20GB.bin`, and `48T` reads `48 × 20 GiB = 960 GiB` (`1.03 TB`) across 48 distinct objects (`bench_20GB_00.bin` .. `bench_20GB_47.bin`).
        *   In `bench_random` and `bench_sequential`, have all `threads` (`1`, `24`, or `48`) open their `File` handles (`fs_c.open(...)`) *before* waiting on `Arc<tokio::sync::Barrier>::new(threads + 1)`. Record `let start = Instant::now();` in the coordinator immediately after `barrier.wait().await` releases all workers simultaneously.
    *   **Workload 2, Workload 3, & Workload 4 Parameter Alignment**:
        *   In Part 2 (`1 MiB` random reads, `CachedFs` Mode 1 default, `read_size = 1 MiB`, `io_size = 1 MiB`), run `50` reads for `1T`, `240` reads (`10` per thread) for `24T`, and `480` reads (`10` per thread) for `48T` across non-overlapping `4 MiB`-aligned offsets.
        *   In Part 3 (`20 GiB` sequential read, `ReadPattern::Auto`), `1T` reads `1 × 20 GiB = 20 GiB`, `24T` reads `24 × 20 GiB = 480 GiB`, and `48T` reads `48 × 20 GiB = 960 GiB` (`1.03 TB`) across distinct objects (`bench_20GB_00.bin` .. `bench_20GB_47.bin`).
        *   In Part 4 (`200 MiB` single-threaded write, `5` iterations), compare `CachedFs::open(WriteMode::Overwrite)` (`8 MiB` chunks) and `CachedFs::pipe_file`.

--------------------------------------------------------------------------------

### Verification Plan & Measured Stack 1 Results (Executed in `gcs-rust-fs`)

*   **Compilation & Static Analysis**:
    *   `PROTOC=/home/princer_google_com/c1dev/gcsfs/.venv/lib/python3.14/site-packages/torch/bin/protoc cargo check --all-targets`
*   **Unit & Integration Tests**:
    *   `PROTOC=/home/princer_google_com/c1dev/gcsfs/.venv/lib/python3.14/site-packages/torch/bin/protoc cargo test --test page_cache --test rpc` (`12 passed` in `page_cache`; `11 passed` in `rpc`)
*   **Stack 1 (Native Rust) Live 20 GiB Benchmark on `US-WEST4` (`gs://princer-flat/bench/bench_20GB.bin`, `Transport::Http`, `ReadPattern::Auto`)**:

#### Part 1: Uncached Metadata `info()` Latency (`30` Iterations)
#### Part 1: Uncached Metadata `info()` Latency (`1T`, `30` Iterations)

| Operation | Iters | Mean | Min | `p50` | `p90` | `p99` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `GcsFs::info` (Direct Uncached HTTP) | 30 | `25.79 ms` | `20.38 ms` | **`25.27 ms`** | `33.10 ms` | `34.16 ms` |
| `GcsFs::info` (Direct Uncached HTTP) | 30 | `28.38 ms` | `23.21 ms` | **`28.38 ms`** | `31.00 ms` | `38.53 ms` |
| `GcsFs::info` (Direct Uncached) | 30 | `28.38 ms` | `23.21 ms` | **`28.38 ms`** | `31.00 ms` | `38.53 ms` |

#### Part 2: `1 MiB` Random Read (`read_size = 1 MiB`, `io_size = 1 MiB`, `ReadPattern::Auto`)
#### Part 2: `1 MiB` Random Read (`CachedFs` Mode 1 Default, `read_size = 1 MiB`, `io_size = 1 MiB`, `ReadPattern::Auto`)
#### Part 2: Single-Threaded `1 MiB` Random Read (`CachedFs` Mode 1 Default, `1T`, `read_size = 1 MiB`, `io_size = 1 MiB`, `ReadPattern::Auto`)

| Approach | Threads | Reads | `p50` | `p90` | `p99` | IOPS | Throughput |
| Approach | Threads | Reads (`Total Data`) | `p50` | `p90` | `p99` | IOPS | Throughput |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Direct `GcsFs::cat_file` (Uncached HTTP) | `1T` | 50 | `63.96 ms` | `72.24 ms` | `77.69 ms` | `16.7` | `17.54 MB/s` (`16.73 MiB/s`) |
| `CachedFs` Mode 1 (Drop-Behind, `Auto`, `io=1MiB`) | `1T` | 50 | **`28.61 ms`** | `36.38 ms` | `46.53 ms` | **`34.4`** | **`36.10 MB/s` (`34.43 MiB/s`)** |
| `CachedFs` Mode 2 (`PageCache` Cold, `Auto`, `io=1MiB`) | `1T` | 50 | `28.59 ms` | `35.42 ms` | `36.73 ms` | `35.3` | `36.99 MB/s` (`35.28 MiB/s`) |
| `CachedFs` Mode 2 (`PageCache` Warm, `Auto`, `io=1MiB`) | `1T` | 50 | **`0.22 ms`** | `0.24 ms` | `0.36 ms` | `4,462.6` | `4,679.35 MB/s` (`4,462.58 MiB/s`) |
| Direct `GcsFs::cat_file` (Uncached HTTP) | `48T` | 480 | `53.70 ms` | `84.61 ms` | `151.14 ms` | `497.4` | `521.52 MB/s` (`497.36 MiB/s`) |
| `CachedFs` Mode 1 (Drop-Behind, `Auto`, `io=1MiB`) | `48T` | 480 | **`29.67 ms`** | `44.97 ms` | `66.16 ms` | **`1,163.6`** | **`1,220.16 MB/s` (`1,163.64 MiB/s`)** |
| `CachedFs` Mode 2 (`PageCache` Cold, `Auto`, `io=1MiB`) | `48T` | 480 | `28.94 ms` | `38.68 ms` | `55.68 ms` | `1,367.2` | `1,433.63 MB/s` (`1,367.22 MiB/s`) |
| `CachedFs` Mode 2 (`PageCache` Warm, `Auto`, `io=1MiB`) | `48T` | 480 | **`0.25 ms`** | `0.50 ms` | `0.94 ms` | `102,431.6` | `107,407.30 MB/s` (`102,431.59 MiB/s`) |
| `CachedFs` Mode 1 (Drop-Behind, `Auto`, `io=1MiB`) | **`1T`** | `50` (`50 MiB`) | **`39.15 ms`** | `53.03 ms` | `128.24 ms` | **`23.9`** | **`25.03 MB/s` (`23.87 MiB/s`)** |
| `CachedFs` Mode 1 (Drop-Behind, `Auto`, `io=1MiB`) | **`24T`** | `240` (`240 MiB`) | **`35.36 ms`** | `45.46 ms` | `78.90 ms` | **`545.1`** | **`571.59 MB/s` (`545.11 MiB/s`)** |
| `CachedFs` Mode 1 (Drop-Behind, `Auto`, `io=1MiB`) | **`48T`** | `480` (`480 MiB`) | **`31.54 ms`** | `42.61 ms` | `65.13 ms` | **`765.1`** | **`802.24 MB/s` (`765.08 MiB/s`)** |

#### Part 3: Sequential Read (`1T = 20 GiB`, `48T = 48 × 20 GiB = 960 GiB`, `ReadPattern::Auto`)
#### Part 3: Sequential Read (`CachedFs` Mode 1 Default, `ReadPattern::Auto` — `1T = 20 GiB`, `24T = 480 GiB`, `48T = 960 GiB`)

| Approach | Threads | Total Data | `read_size` | `io_size` | `max_folio` | Wall Time | Chunk `p50` | Chunk `p90` | Throughput |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `CachedFs` Mode 1 (`Auto`) | `1T` | `20 GiB` | `1 MiB` | `1 MiB` | `16 MiB` | **`5.50 s`** | `0.00 ms` | `0.00 ms` | **`3,905.67 MB/s` (`3,724.73 MiB/s`)** |
| `CachedFs` Mode 1 (`Auto`) | `1T` | `20 GiB` | `1 MiB` | `16 MiB` | `16 MiB` | **`5.67 s`** | `0.00 ms` | `0.00 ms` | **`3,786.41 MB/s` (`3,611.00 MiB/s`)** |
| `CachedFs` Mode 1 (`Auto`) | `1T` | `20 GiB` | `8 MiB` | `16 MiB` | `16 MiB` | **`4.88 s`** | `0.00 ms` | `0.04 ms` | **`4,401.46 MB/s` (`4,197.56 MiB/s`)** |
| Direct `GcsFs` HTTP (Uncached) | `48T` | `960 GiB` (`48 × 20 GiB`) | `8 MiB` | — | — | `150.92 s` | `51.63 ms` | `67.97 ms` | `6,830.02 MB/s` (`6,513.61 MiB/s`) |
| `CachedFs` Mode 1 (`Auto`) | `48T` | `960 GiB` (`48 × 20 GiB`) | `8 MiB` | `16 MiB` | `16 MiB` | **`48.48 s`** | `0.00 ms` | `53.20 ms` | **`21,261.74 MB/s` (`20,276.78 MiB/s`)** |
| `CachedFs` Mode 1 (`Auto`) | **`1T`** | `20 GiB` (`1 × 20 GiB`) | `8 MiB` | `16 MiB` | `16 MiB` | **`5.23 s`** | **`0.00 ms`** | `0.11 ms` | **`4,106.42 MB/s` (`3,916.18 MiB/s`)** |
| `CachedFs` Mode 1 (`Auto`) | **`24T`** | `480 GiB` (`24 × 20 GiB`) | `8 MiB` | `16 MiB` | `16 MiB` | **`33.03 s`** | **`0.00 ms`** | `49.88 ms` | **`15,604.48 MB/s` (`14,881.59 MiB/s`)** |
| `CachedFs` Mode 1 (`Auto`) | **`48T`** | `960 GiB` (`48 × 20 GiB`) | `8 MiB` | `16 MiB` | `16 MiB` | **`49.14 s`** | **`0.00 ms`** | `75.63 ms` | **`20,976.12 MB/s` (`20,004.39 MiB/s`)** |

#### Part 4: Single-Threaded `200 MiB` Write (`1T`, `5` Iterations)

| Approach | Threads | Iters | Mean | `p50` | `p90` | Throughput (at `p50`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `CachedFs::open(write)` + `write(8 MiB)` + `close` (`200 MiB`) | **`1T`** | `5` | `2.14 s` | **`2.14 s`** | `2.17 s` | **`97.88 MB/s` (`93.34 MiB/s`)** |
| `CachedFs::pipe_file` (One-Shot `200 MiB` Upload) | **`1T`** | `5` | `1.34 s` | **`1.34 s`** | `1.71 s` | **`156.46 MB/s` (`149.21 MiB/s`)** |

--------------------------------------------------------------------------------

*Drafted with EGM*
