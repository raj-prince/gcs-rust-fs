# Architecture Design: Unified Filesystem Engine and UDS Service for `gcs-rust-fs`

*   **Author:** `gcs-rust-fs` team & Prince Kumar (@princer)
*   **Status:** Implemented & Verified (Phases 0, 1, 2 ✅)
*   **Date:** 2026-11-04

--------------------------------------------------------------------------------

## Overview

Python [`gcsfs`](https://github.com/fsspec/gcsfs) is the standard Cloud Storage backend for the Python `fsspec` ecosystem, powering distributed machine learning frameworks (PyTorch `DataLoader`, Ray Train, JAX) and batch analytical workflows (Dask, Apache Arrow). However, emulating POSIX filesystem semantics on top of an object store requires complex directory marker tracking, recursive tree traversals, and multi-bucket layout handling.

This design document establishes the architecture and interface specifications for [`gcs-rust-fs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/lib.rs): a high-performance, async, object-safe Rust filesystem engine. The engine provides:
1. **Core Trait Abstractions**: [`FileSystem`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs) and [`File`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs) traits implementing `fsspec` semantics with six mandatory primitives.
2. **Generic Derived Layer**: Storage-agnostic implementations of recursive walks, globs, directory usage, and batch transfers ([`src/derived.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/derived.rs)).
3. **Multi-Bucket GCS Backend**: Unified dispatch across Standard Flat, Hierarchical Namespace (HNS), and Zonal (Rapid) buckets ([`src/gcs/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/)).
4. **Out-of-Process UDS Service**: A gRPC sidecar daemon (`gcsfs-sidecar`) communicating over Unix Domain Sockets ([`src/rpc/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/)) with bounded chunk streaming.

--------------------------------------------------------------------------------

## The Problem

Embedding raw Python HTTP client runtimes across thousands of data workers introduces severe performance and reliability bottlenecks:

1. **Class A Listing Billing Penalties**: Naive path queries often trigger expensive `ListObjects` RPCs ($0.0050/1k Class A) rather than `GetObject` ($0.0004/1k Class B), multiplying Cloud Storage operational costs by over 1,200% for simple file existence checks.
2. **Worker Memory Bloat & Daemon OOM**: In large training clusters, workers reading multi-gigabyte checkpoints through IPC can cause severe memory amplification if data is buffered in memory rather than streamed in bounded chunks.
3. **Semantic Divergence Across Bucket Types**: GCS buckets behave differently across Flat, HNS, and Zonal storage (e.g., atomic `RenameFolder` vs iterative copy-delete loops; appendable objects vs immutable uploads). The storage engine must handle these quirks without breaking callers.
4. **Python Multi-Process Fork Hazards**: PyTorch DataLoaders relying on `os.fork()` corrupt internal connection pools and background threads in native runtimes, demanding clean process isolation or stateless client stubs.

--------------------------------------------------------------------------------

## Goals

*   **Fsspec Behavioral Parity**: Expose the full `fsspec` filesystem surface (`open`, `read`, `write`, `info`, `ls`, `find`, `glob`, `walk`, `du`, `cat`, `put`, `copy`, `mv`, `rm`, `mkdir`, `rmdir`) without behavior divergence.
*   **Object-Safe, Minimal Core**: Enforce a clean separation of concerns: six mandatory storage primitives, native performance overrides, and generic derived algorithms.
*   **Optimal GCS RPC Economics**: Probe file lookups sequentially (`GetObject` first) to avoid burning expensive Class A List quotas for regular files.
*   **Bounded Memory Streaming**: Ensure IPC read paths stream in bounded chunks (1–3.5 MiB) directly from file handles without whole-file memory buffering.
*   **Multi-Bucket Transparency**: Provide seamless dispatch across Standard Flat, HNS, and Zonal buckets based on capability flags rather than leaky inheritance hierarchies.
*   **Stateless Backends & Pluggable Caching**: Keep `GcsFs` completely stateless while providing an opt-in decorator ([`CachedFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/cached.rs)) for client-side directory and metadata caching.

--------------------------------------------------------------------------------

## Non-Goals

*   **POSIX File Locking**: Advisory POSIX record locking (`fcntl`/`flock`) across concurrent remote processes is out of scope. Object immutability and atomic overwrites via GCS generations apply.
*   **In-Backend Caching**: `GcsFs` itself never caches state internally; all caching logic is isolated in decorators.
*   **Non-Filesystem GCS Features**: GCS bucket lifecycle rules, IAM policies, HMAC keys, and requester-pays configuration are administrative Cloud Storage features, not filesystem operations.
*   **Remote Cross-Host Proxies**: The UDS server is strictly node-local (co-located in the same Kubernetes Pod or host); network routing across VM boundaries is out of scope.

--------------------------------------------------------------------------------

## Open Questions

*   **Question 1: Python Bridge Packaging Strategy**: Should the thin client communicate exclusively over UDS sockets (`gcsfs-sidecar`) or also provide in-process PyO3 C-extensions for single-container environments? (Current status: Both supported; UDS is prioritized for multi-worker container pods).
*   **Question 2: Prefix Partitioning for Massive Directories**: Should Phase 4 introduce lexicographic shard ranges for directories with >1,000,000 objects? (Deferred to Phase 4 optimization based on production profiling).

--------------------------------------------------------------------------------

## Critical User Journeys (CUJs)

### CUJ 1: High-Performance Positional Range Reads in DataLoader Workers
*   **Given**: A PyTorch training worker opening a 10 GiB Parquet partition or safetensors checkpoint.
*   **When**: The worker requests a byte range `[start, end)` via `fs.cat_file` or positional `File::read_range`.
*   **Then**: The engine resolves the slice using Python-slice semantics, avoids unnecessary `stat` calls when bounds are positive, executes a direct HTTP Range / gRPC bidi read, and streams bytes with zero copy.

### CUJ 2: Cost-Effective Path Inspection (`info` / `exists`)
*   **Given**: A data ingestion pipeline validating the existence of 100,000 file paths before processing.
*   **When**: The application calls `fs.info(path)` or `fs.exists(path)`.
*   **Then**: The engine issues a single Class B `GetObject` RPC. Only if the object is missing or represents a directory placeholder does it fallback to a Class A prefix listing, saving over 90% in GCS API billing.

### CUJ 3: Atomic Folder Moves on Hierarchical Namespace (HNS) Buckets
*   **Given**: A dataset generation script publishing an output folder `gs://hns-bucket/output_staging/` to `gs://hns-bucket/published/`.
*   **When**: The caller invokes `fs.mv("output_staging", "published", recursive=True)`.
*   **Then**: The backend identifies the bucket as HNS, triggers an atomic `RenameFolder` Long-Running Operation (LRO) in a single RPC, and completes instantly without copying individual files.

### CUJ 4: Streaming Out-of-Process Reads over Unix Domain Sockets
*   **Given**: A Kubernetes container pod where `gcsfs-sidecar` listens on `unix:///var/run/gcsfs/sidecar.sock`.
*   **When**: A client worker requests a multi-gigabyte file read over the UDS connection.
*   **Then**: The sidecar streams 1–3.5 MiB chunks iteratively from an active `File` handle, keeping server memory bounded to `O(chunk_size * streams)` and preventing daemon OOM kills.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. Two-Tier Trait Architecture

The public API is partitioned into two core traits in [`src/filesystem.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs) and [`src/file.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs):

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Core Public API                                                             │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ FileSystem Trait (Send + Sync, Object-Safe)                           │  │
│  │ ├── Required Primitives: info, ls, open, rm_file, mkdir, rmdir        │  │
│  │ ├── Overridable: find, cat_file, pipe_file, put_file, copy_file, mv   │  │
│  │ └── Derived Operations: walk, du, glob, cat, put, exists, is_file...  │  │
│  └───────────────────────────────────┬───────────────────────────────────┘  │
│                                      │ open(path, OpenOptions)              │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ File Trait (Send + Sync, Exclusive Handle)                            │  │
│  │ ├── Reading: seek, read, read_range                                   │  │
│  │ ├── Writing: write, flush                                             │  │
│  │ └── Lifecycle: stat, size, close (commit), discard (abort)            │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### The Six Mandatory Primitives
1. `info(&self, path: &str) -> Result<Entry>`
2. `ls(&self, path: &str, opts: ListOptions) -> Result<Vec<Entry>>`
3. `open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>>`
4. `rm_file(&self, path: &str) -> Result<()>`
5. `mkdir(&self, path: &str, opts: MkdirOptions) -> Result<()>`
6. `rmdir(&self, path: &str) -> Result<()>`

### 2. Multi-Bucket Capability Architecture

`GcsFs` serves all three Google Cloud Storage bucket layouts through capability-based dispatch:

| Feature / Capability | Flat Buckets | Hierarchical Namespace (HNS) | Zonal (Rapid Storage) |
|---|---|---|---|
| **Directory Model** | Prefixes & zero-byte placeholders | Real `Folder` resources | Real `Folder` resources |
| **Directory Rename** | Iterative per-object copy-delete | Atomic `RenameFolder` LRO | Atomic `RenameFolder` LRO |
| **Server-Side Copy** | `RewriteObject` token loop | `RewriteObject` token loop | `Unsupported` |
| **Write Model** | Single-shot or Resumable upload | Single-shot or Resumable upload | Appendable objects |
| **Publish Invariant** | Atomic on `close()` | Atomic on `close()` | Persists on `flush()` |
| **Data Transport** | gRPC bidi with HTTP fallback | gRPC bidi with HTTP fallback | gRPC DirectPath only |

### 3. Path & Emulation Semantics

* **Root**: The root is represented by the empty string `""`. `ls("")` lists all buckets visible to the project.
* **Entry Paths**: Normalized as `bucket/key` without leading slashes or URI schemes.
* **Placeholders**: A zero-byte object ending in `/` (`b/dir/`) is treated as a directory. Standard listings suppress self-placeholders.
* **Generations**: Paths with `#generation` pin the object version. Reads honor pinned generations; listings emit generations only when `versions = true`.

### 4. IPC Sidecar Architecture (`gcsfs-sidecar`)

The out-of-process UDS service is implemented via Tonic gRPC in [`src/rpc/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/):

* **Socket Management**: Binds to multiple `--uds` paths simultaneously, creates parent directories, sets permissions (`0666`), and cleans up stale socket files.
* **High-Fidelity Errors**: Attaches the exact [`ErrorKind`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/error.rs) string to the `x-gcsfs-error-kind` metadata header for lossless Python exception mapping.
* **Bounded Chunk Streaming**: Streaming read handlers read sequentially from `File::read` in chunks up to 3.5 MiB, eliminating whole-file memory buffering.

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative A: Unified Reader and Writer Handle vs Separate Traits
*   **Pros**: Separate traits (`FileReader` and `FileWriter`) prevent runtime capability checks on read handles.
*   **Cons**: Forces `FileSystem` to maintain two separate opening methods (`open` vs `create`), breaking 1:1 symmetry with Python `fsspec` and standard file descriptor models.
*   **Reason for Rejection**: Rejected by author (D3): A unified `open(path, OpenOptions) -> Box<dyn File>` keeps the entry point identical to `fsspec` while returning `ErrorKind::Unsupported` for invalid mode operations.

### Alternative B: In-Backend Caching vs Opt-in Decorator
*   **Pros**: In-backend caching inside `GcsFs` provides caching out-of-the-box without requiring client composition.
*   **Cons**: Destroys testability, violates separation of concerns, and prevents sharing the cache with alternative implementations (such as in-memory test filesystems).
*   **Reason for Rejection**: Rejected by author (D1/D30): Backends remain strictly stateless. Caching is encapsulated in the [`CachedFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/cached.rs) decorator.

### Alternative C: Concurrent Probe in `info()`
*   **Pros**: Overlaps `GetObject` and `ListObjects` network latency via `tokio::join!`.
*   **Cons**: Burns expensive Class A listing requests for every file query, generating a 1,250% billing penalty and risking bucket rate-limit exhaustion.
*   **Reason for Rejection**: Rejected during Stage 7/8 review: Probing sequentially executes 1 Class B RPC for >95% of queries, drastically reducing API costs.

--------------------------------------------------------------------------------

## Non-Functional Requirements

### Security & Isolation
*   **Socket Permissions**: Unix domain socket files are created with configurable octal permissions (`0666` default) to allow communication across container boundaries in shared pod volumes.
*   **Credential Encapsulation**: Cloud Storage credentials (ADC tokens) remain isolated in the sidecar daemon, eliminating the need to mount service account keys into application containers.

### Performance & Efficiency
*   **IPC Read Throughput**: Deliver > 1.5 GB/s transfer rates over local UDS socket buffers.
*   **Sub-Millisecond Metadata**: Unary `info` lookups execute under 500 µs over UDS.
*   **Memory Footprint**: Sidecar daemon stays bounded to `O(concurrency * chunk_size)` regardless of the size of the files being read.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Implementation Modules & Responsibilities

#### 1. [`src/filesystem.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs)
*   Defines the [`FileSystem`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs) trait with 6 required primitives, native overrides, and derived operations.

#### 2. [`src/file.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs)
*   Defines the unified [`File`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs) handle trait for cursor reads, positional range reads, streaming writes, and commit/abort lifecycles.

#### 3. [`src/derived.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/derived.rs)
*   Encodes storage-agnostic `fsspec` algorithms: breadth-first search `find`, grouped `walk`, pattern-matching `glob`, recursive `rm`, `copy`, and `mv`.

#### 4. [`src/gcs/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/)
*   `fs.rs`: Implementation of `FileSystem for GcsFs` with sequential `info` probe, bucket listing, and multi-bucket layout handling.
*   `file.rs`: Implementation of `File for GcsFile` with read-ahead buffering and resumable/appendable writer backends.
*   `backend.rs`: Connection pool and SDK client management.
*   `control.rs`: Storage Control API operations (HNS folders, bucket operations, warning log on bucket deletions).

#### 5. [`src/rpc/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/)
*   `server.rs`: Tonic service implementation over UDS/TCP with chunked streaming reads.
*   `client.rs`: `RemoteFs` client implementing `FileSystem` across the RPC channel.
*   `convert.rs`: Bidirectional status and error conversion.

#### 6. [`src/bin/gcsfs_sidecar.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/bin/gcsfs_sidecar.rs)
*   Production daemon binary supporting `--uds <PATH>` (multi-socket), `--tcp <ADDR>`, `--mode <OCTAL>`, and `SIGINT`/`SIGTERM` signal traps.

--------------------------------------------------------------------------------

## Architecture Decision Records (ADRs)

| ADR | Domain | Decision Summary |
|---|---|---|
| **D1 / D30** | Caching | Stateless backends; opt-in [`CachedFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/cached.rs) decorator for unified directory and metadata caching. |
| **D2** | Root Model | Root is `""`; `ls("")` lists buckets without specialized top-level methods. |
| **D3** | Handle API | Unified `open(path, OpenOptions) -> Box<dyn File>` rather than separate open and create methods. |
| **D4** | Read Buffer | 5 MiB configurable read-ahead buffer in `GcsFile::read`. |
| **D5** | Globbing | Native, dependency-free matcher in [`src/glob.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/glob.rs) with `fsspec.glob_translate` semantics. |
| **D6** | Entry Kinds | Buckets are represented as `EntryKind::Directory` to preserve pure filesystem abstractions. |
| **D7** | Bucket Info | `info(bucket)` probes `GetBucket`, falling back to listing on permission denied. |
| **D8** | Mkdir Semantics | `mkdir(path)` is a no-op by default; writes zero-byte marker only when `placeholder = true`. |
| **D9** | Error Fidelity | Strict POSIX error mapping (`IsADirectory`, `NotADirectory`, `DirectoryNotEmpty`). |
| **D10** | Bucket Deletion | `rm(bucket, recursive=true)` deletes the bucket itself and logs at `warn!` level. |
| **D11** | Path Mapping | Recursive copy and move destination mapping implemented natively via `destination_base`. |
| **D12** | Rmdir Semantics | Strict directory deletion: non-empty directories raise `DirectoryNotEmpty`. |
| **D13 / D22** | Transport | Default `Grpc` with automatic fallback to `Http` on bidi read unavailability. |
| **D14** | Transport Accessor | Public `GcsFs::transport()` accessor exposed for diagnostic inspection. |
| **D15** | Compat Switch | Directory placeholder quirks normalized away in core; bridge layer handles compatibility shims. |
| **D16** | Handle Bounds | `File` requires `Send + Sync`; `Debug` supertrait omitted. |
| **D17** | Delivery Order | Phase 1 delivered in one unified release covering all three bucket types. |
| **D18** | Layout Detection | Lazy `GetStorageLayout` lookup cached for the client lifetime; un-cached fallback on error. |
| **D19 / D29** | Kind Dispatch | Dispatch driven by named capabilities of `BucketKind` rather than hardcoded variant branching. |
| **D20** | Zonal Copy | Zonal server-side copies return `ErrorKind::Unsupported` (requires client-side read/write). |
| **D21** | Zonal Writing | Zonal appendable objects persist on `flush()`; `finalize_on_close` defaults to `false`. |
| **D23** | Bucket Creation | Storage-specific creation handled outside trait via `GcsFs::create_bucket(name, BucketSpec)`. |
| **D24** | Test Harness | Integration tests run against live Flat, HNS, and Zonal buckets under isolated scratch prefixes. |
| **D25** | Create Exclusivity | `if_generation_match = 0` precondition failures map to `ErrorKind::AlreadyExists`. |
| **D26** | Zonal Discard | Discarding an appendable writer leaves already-flushed bytes intact with a warning log. |
| **D27** | Explicit Discard | Explicit async `File::discard()` retained, backed by `Drop` safety net. |
| **D28** | Read Options | Extensible `ReadOptions` struct constructed via `Default` with `#[non_exhaustive]` protection. |

--------------------------------------------------------------------------------

## Verification Plan

*   **Deterministic Contract Tests**:
    ```bash
    cargo test --test memory_fs
    ```
    Executes 12 test scenarios verifying all derived operations over in-memory storage without network calls.
*   **Directory & Metadata Cache Tests**:
    ```bash
    cargo test --test cache
    ```
    Executes 21 test scenarios verifying TTL expiration, LRU eviction, and invalidation rules.
*   **Live Multi-Bucket Integration Tests**:
    ```bash
    cargo test --test live
    ```
    Executes 10 live scenarios across Flat, HNS, and Zonal buckets when `GCS_RUST_FS_TEST_BUCKETS` is configured.
*   **RPC Sidecar Integration Tests**:
    ```bash
    cargo test --test rpc
    ```
    Executes 10 test scenarios verifying multi-UDS serving, concurrent client streaming, and error mapping over UDS.
*   **Full Crate Verification**:
    ```bash
    cargo test
    cargo clippy --all-targets -D warnings
    ```

--------------------------------------------------------------------------------

*Drafted with EGM*
