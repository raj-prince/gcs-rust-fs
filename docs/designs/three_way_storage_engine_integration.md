# Architecture Design: Three-Way Storage Engine Integration for Python `gcsfs`

*   **Author:** Prince Kumar (@princer)
*   **Status:** Proposed
*   **Date:** 2026-11-04

--------------------------------------------------------------------------------

## Overview

Python [`gcsfs`](https://github.com/fsspec/gcsfs) is the standard Google Cloud Storage filesystem interface across Python data science, batch ETL, and distributed deep learning. In distributed training workloads (such as PyTorch `DataLoader` multi-worker pipelines, Ray Train, and batch ML feature extractors), clusters of worker processes stream tens of terabytes of sharded datasets and write multi-gigabyte checkpoints to and from GCS.

[`gcs-rust-fs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/lib.rs) was engineered as an asynchronous Rust storage engine implementing the [`FileSystem`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs) and [`File`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs) traits to replace the pure-Python `aiohttp` stack. This document evaluates three competing deployment topologies to connect `gcs-rust-fs` to Python runtimes:
1. **Candidate (A): In-Process Native PyO3 Extension (`gcsfs_rust` built in the `gcsfs` repository, depending on `gcs-rust-fs` via crates.io)**
2. **Candidate (B): Out-of-Process UDS gRPC Sidecar Daemon**
3. **Candidate (C): Out-of-Process Shared-Memory IPC Daemon** (evaluating POSIX `/dev/shm` circular ring buffers and anonymous `memfd_create` descriptors passed over UDS via `SCM_RIGHTS`).

This design establishes the technical plan, empirical trade-offs, and decisive verdict recommending **Candidate (A)**. Crucially, the pure-Rust storage core (`gcs-rust-fs`) remains zero-dependency on Python, published as a standard Rust crate on crates.io, while the PyO3 C-extension (`gcsfs_rust`) and Maturin wheel packaging live directly within the upstream Python [`gcsfs`](https://github.com/fsspec/gcsfs) repository.

--------------------------------------------------------------------------------

## The Problem

Modern GPU compute instances (such as GCP A3 / A2 VMs) provide network interfaces scaling to 200 Gbps (25.0 GB/s) and 400 Gbps. However, existing Python storage integrations fail to deliver this bandwidth to distributed ML training workers due to three structural bottlenecks:

1. **The IPC Memory Bus Tax**: Out-of-process daemons communicating over Unix Domain Sockets (UDS) force data through 4 to 5 copies between the network socket, daemon heap, kernel socket buffer, and Python address space. At 200 Gbps, this burns 100 to 125 GB/s of continuous DDR memory bus bandwidth—roughly 40% to 50% of the host memory bus—saturating memory channels and capping throughput at ~40 Gbps.
2. **The Small-Read Syscall Storm**: ML workloads execute millions of small reads (< 64 KiB) for metadata, Parquet footers, and indices. Moving small payloads through out-of-process IPC (UDS or `memfd_create` shared memory) requires hundreds of thousands of context switches or `mmap`/`munmap` syscalls per second, causing kernel `mmap_lock` contention and millisecond-level scheduling latency.
3. **Operational Fragility & Kubernetes Lifecycle Traps**: Centralizing node-local storage in an external daemon creates a Single Point of Failure (SPoF) where a daemon crash or shared-memory deadlock destroys all 8 GPUs on a node. In Kubernetes batch `Job` workloads, persistent sidecar containers remain running indefinitely after training completes, preventing pods from exiting cleanly.

We must select and specify an architecture that saturates 200 Gbps line rate, bounds local metadata overhead, guarantees fault isolation across workers, and minimizes deployment friction in Kubernetes clusters.

--------------------------------------------------------------------------------

## Goals

*   **200 Gbps Line-Rate Saturation**: Deliver sustained aggregate read throughput of up to 25.0 GB/s across 16 to 64 concurrent Python DataLoader worker processes on a single VM.
*   **True Zero-to-One Copy Memory Handoff**: Eliminate intermediate user-kernel memory copies; hand downloaded GCS buffers directly into Python `PyMemoryView` and PyTorch `torch.from_blob()` tensors with $O(1)$ pointer slicing.
*   **Bounded Local Metadata Overhead**: Keep cached metadata query overhead (`info`, `exists`) strictly within `<= 100 µs` per call (ideal north star: sub-microsecond in-process execution).
*   **Strict Fault Isolation & Resilience**: Ensure that an unexpected worker crash (`SIGSEGV`, `SIGBUS`) or kernel cgroup OOM-kill (`SIGKILL`) is strictly confined to that individual worker process; neither sibling workers nor node-level storage services may crash, deadlock, or leak host memory.
*   **Minimal Deployment & Job Lifecycle Friction**: Target standard Python wheel distribution (`pip install gcsfs`) as the primary path. For GKE environments, tolerate reasonable sidecar packaging and lifecycle management overhead if technically justified, but explicitly account for GKE operational complexity (manifest injection, shared volumes, sentinel shutdown hooks) as a trade-off con during architectural scoring.

--------------------------------------------------------------------------------

## Non-Goals

*   **Non-Python Runtimes in Phase 1**: Supporting C++, Go, or CLI tools (`gcloud`) through this interface is out of scope; Phase 1 targets Python `gcsfs` (PyTorch DataLoaders, Ray, batch ML).
*   **Cross-Host / Remote Storage Gateways**: Running a centralized remote proxy across multiple nodes is out of scope; all candidate evaluations are strictly node-local.
*   **Complex Multi-Writer Semantics**: POSIX distributed file locking, cross-process append modes on standard regional buckets, and atomic cross-bucket renames are deferred to Phase 2.

--------------------------------------------------------------------------------

## Open Questions

*   **Question 1: Python DataLoader `fork()` Safety**: When PyTorch DataLoaders use Linux `fork`, how do we guarantee background Tokio runtime threads and TLS connection pools are not corrupted by child processes? (Resolved in Technical Plan via post-fork lazy instantiation, orphan runtime resetting without drop, and recommending `multiprocessing.set_start_method("spawn")` or `forkserver`).
*   **Question 2: Application Default Credentials (ADC) Burst Throttling**: When 64 DataLoader workers initialize simultaneously at an epoch boundary, could concurrent token requests rate-limit against the GCE metadata server? (Resolved: The link-local metadata server at `169.254.169.254` easily handles high-concurrency bursts; standard Google Cloud SDK jittered backoff handles retries cleanly without complex custom cross-process token cache files).

--------------------------------------------------------------------------------

## Critical User Journeys (CUJs)

### CUJ 1: High-Throughput Shard Streaming in Multi-Worker PyTorch DataLoader

*   **Given**: A distributed training script running on an 8-GPU GCE VM with 32 PyTorch DataLoader workers (`num_workers=4` per GPU).
*   **When**: All 32 workers simultaneously stream 50 MiB dataset shards from a Google Cloud Storage bucket across a 200 Gbps network fabric.
*   **Then**: Aggregate read throughput reaches up to 25.0 GB/s without saturating host DDR memory channels, each chunk is sliced directly into PyTorch tensors with zero intermediate IPC copies, and CPU utilization is confined to network TLS decryption.

### CUJ 2: Sudden Worker Failure / Cgroup OOM Recovery

*   **Given**: A high-concurrency training job where one Python DataLoader worker process exceeds its memory budget and is terminated with `SIGKILL` by the Linux kernel cgroup OOM-killer.
*   **When**: The offending worker process abruptly terminates mid-read.
*   **Then**: Sibling workers and active GPU training steps continue uninterrupted, zero shared memory files or ring buffers are leaked or deadlocked in `/dev/shm`, and the PyTorch DataLoader supervisor automatically respawns a replacement worker process.

### CUJ 3: Serverless / Kubernetes Batch Job Completion

*   **Given**: A Kubernetes `Job` executing a finite batch feature extraction or fine-tuning run.
*   **When**: The primary Python training script completes its workload and calls `sys.exit(0)`.
*   **Then**: The pod immediately transitions to `Completed` status; no auxiliary daemon or sidecar container remains running as a zombie to block pod termination.

### CUJ 4: Single-Process Multi-Threaded Ingestion (Zero Multi-Processing Mandate for Cx)

*   **Given**: A Python data preprocessing, batch ETL, or inference pipeline executing on a VM utilizing standard Python multi-threading (e.g. `concurrent.futures.ThreadPoolExecutor(max_workers=16)` or Ray/FastAPI threads).
*   **When**: 16 concurrent Python threads invoke `fs.cat_file(...)` simultaneously on a single shared `GCSFileSystem` instance.
*   **Then**: All 16 threads immediately release the Python GIL via `py.allow_threads`, execute network downloads in parallel on the underlying multi-threaded Tokio runtime across GCS DirectPath subchannels, completely bypass the pure-Python single-event-loop bottleneck, and achieve up to 25.0 GB/s throughput without requiring the customer to refactor their application to `multiprocessing`.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. High-Level Architecture Overview

To eliminate the 100+ GB/s memory bus copy tax, small-read context-switch storms, and Kubernetes sidecar operational friction, the storage engine integrates directly into the Python worker process via an **in-process native PyO3 C-extension (`gcsfs_rust`)**.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Python Worker Address Space (Isolated OS Process)                           │
│                                                                             │
│   Python Layer (PyTorch DataLoader / ThreadPoolExecutor / Ray Worker)       │
│      │                                                                      │
│      ▼ Direct C-ABI FFI Call (GIL Released via py.allow_threads)            │
│   PyO3 Bridge Layer (`gcsfs_rust`)                                          │
│      │                                                                      │
│      ▼ Rust Trait Invocations (fs.cat_file / fs.open / fs.info)             │
│   gcs-rust-fs Core Engine                                                   │
│      ├── Directory & Metadata Cache (DirCache, StatCache)                   │
│      ├── Worker-Local Tokio Runtime (Multi-Threaded / Current-Thread pool)  │
│      └── GCS Transport (DirectPath gRPC / HTTP/2 Hyper Connection Pool)     │
│             │                                                               │
│             ▼ Direct Network I/O (Zero IPC Boundaries)                      │
│   Google Cloud Storage (200 Gbps Line Rate: 25.0 GB/s Wire Ingress)        │
│                                                                             │
│   Zero-Copy Payload Return:                                                 │
│      Rust bytes::Bytes ──► PyMemoryView (readonly=1) ──► torch.from_blob    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2. Component Structure & Cross-Repository Architecture

The architecture maintains a strict decoupling between the pure-Rust storage engine and the Python bindings across two repositories:

1. **Rust Storage Engine (`gcs-rust-fs` on crates.io)**:
   * Maintained in the current repository (`raj-prince/gcs-rust-fs`).
   * **Pure Rust, Zero Python Dependencies**: Implements [`FileSystem`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs), [`File`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs), [`GcsFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/fs.rs), and DirectPath gRPC transport. Tested independently with standard `cargo test`.
   * Published as an open-source Rust crate on crates.io, consumable by any Rust systems software (e.g. data connectors, storage proxies, and native Python extensions).
2. **PyO3 Extension & Packaging (`gcsfs_rust` in upstream `fsspec/gcsfs` repository)**:
   * Implemented and maintained directly within the official Python [`gcsfs`](https://github.com/fsspec/gcsfs) repository.
   * Pulls in `gcs-rust-fs = "0.1"` as a standard Cargo dependency.
   * Implements the PyO3 FFI bridge (`PyGcsFs`), wrapping Rust filesystem traits and error conversions.
   * Maps GCS range requests into Rust [`ByteRange`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/range.rs) and options into [`ReadOptions`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/options.rs).
   * Configures `pyproject.toml` and Maturin build backend to build and distribute native C-extension wheels (`pip install gcsfs`).
3. **Python `gcsfs` Integration Layer (in `fsspec/gcsfs` repository)**:
   * `gcsfs.GCSFileSystem` subclasses or delegates storage operations to the embedded `gcsfs_rust.PyGcsFs`.
   * Unary metadata calls (`info`, `exists`, `checksum`) invoke native Rust C-ABI functions without crossing socket or IPC boundaries, returning cached Python `dict` records in `< 1.0 µs`.
   * **Universal Concurrency Support (Multi-Threaded & Multi-Processing)**: Customers (Cx) are **not required to use `multiprocessing`** to achieve high throughput. In pure Python `gcsfs`, multi-threading is crippled by serialization on a single background `asyncio` loop. In `gcsfs_rust`, each process hosts an isolated engine backed by a lightweight Tokio runtime (defaulting to 2–4 background worker threads, configurable via `GCSFS_TOKIO_WORKERS`, or current-thread mode for single-threaded worker loops). When multiple Python threads call `fs.cat_file()` concurrently, they release the GIL and download across parallel GCS DirectPath subchannels simultaneously.
   * **Multi-Worker DataLoader Sizing**: For multi-process workloads (PyTorch DataLoader with `num_workers=64`), connection pools are sized conservatively per worker (e.g. 2 HTTP/2 or DirectPath subchannels), maintaining 25 GB/s aggregate throughput while avoiding socket exhaustion ($64 \times 2 = 128$ connections per host). Configuring `DataLoader(..., persistent_workers=True)` avoids connection renegotiation churn at epoch boundaries.

### 3. Critical-Path Read & Write Data Flow

#### Read Path (Zero-to-One Copy Handoff)
1. Python invokes `fs.cat_file(path, start, end)` or `file.read(size)`.
2. PyO3 releases the Python GIL via `py.allow_threads(...)` to allow concurrent Python threads (such as data preprocessing or GPU scheduling) to execute unhindered.
3. The Rust Tokio runtime streams payload bytes from GCS directly into a contiguous `bytes::Bytes` heap buffer in user space (1 copy: NIC socket buffer to user heap).
4. Upon stream completion, Rust constructs a Python `PyMemoryView` referencing the underlying `Bytes` buffer via the Python Buffer Protocol with an $O(1)$ atomic reference-count increment. The exported buffer enforces `readonly = 1` in `Py_buffer` to guarantee Rust's memory safety invariants against in-place mutation from Python.
5. PyTorch workers consume the `PyMemoryView` directly into tensors via `torch.from_blob(...)` or DLPack with **zero intermediate memory copies and zero IPC overhead**.
6. At 200 Gbps line rate (25.0 GB/s), total host DDR memory bus load is exactly **25.0 GB/s** (compared to 100–125 GB/s in Candidate B).

#### Write Path (Direct Streaming)
1. Python passes a buffer (`bytes`, `bytearray`, or `memoryview`) to `fs.put_file(...)` or `file.write(...)`.
2. PyO3 binds the underlying pointer without copying data, releasing the GIL.
3. Rust's [`ResumableWriter`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/write.rs) streams the bytes directly into the GCS TLS socket in 16 MiB chunks with streaming CRC32c verification.
4. Because the memory is owned by the worker process, there is zero risk of Time-of-Check to Time-of-Use (TOCTOU) corruption from external processes.

### 4. Concurrency & Reliability Contracts

#### PyTorch DataLoader Linux `fork()` Safety Contract
* **The Hazard**: PyTorch DataLoaders default to `multiprocessing` with the `fork` start method on Linux. In standard training pipelines, dataset initialization in the parent process calls `fs.ls()` or `fs.info()` to discover shards *before* `DataLoader(num_workers=N)` is invoked, initializing the parent's `GcsFs` runtime. When the process forks, child processes inherit the parent's runtime handle. Invoking `Runtime::drop` in a child process deadlocks or panics because it attempts to join threads that no longer exist in the child.
* **The Solution**:
  1. **Strict Post-Fork Lazy Initialization**: The PyO3 extension struct `PyGcsFs` holds `Option<Arc<GcsFs>>`. On creation in Python, no threads or runtimes are created until first I/O.
  2. **Orphan Runtime Handle in Fork Child**: Register `os.register_at_fork(after_in_child=...)` to detach the inherited runtime handle without dropping it (using `std::mem::forget` or pointer reassignment), ensuring child processes instantiate a completely fresh current-thread runtime on their first I/O call without triggering `drop()` deadlocks.
  3. **Runtime Recommendation**: Document `multiprocessing.set_start_method("spawn")` or `forkserver` as the recommended, crash-safe configuration for high-concurrency multi-worker pipelines.

#### Multi-Threaded Python Support Contract (Zero Multi-Processing Mandate for Cx)
* **The Pure-Python Bottleneck**: In pure-Python `gcsfs` (`aiohttp` + `fsspec`), multi-threaded Python applications suffer from extreme lock contention because all calls serialize through a single background `asyncio` event loop thread (`fsspec.asyn.sync`). This caps throughput at ~2–4 Gbps regardless of thread pool size.
* **The Solution in `gcsfs_rust`**:
  1. **Non-Blocking GIL Release**: All entry points release the Python GIL via `py.allow_threads(...)`. When 16 to 32 Python threads call `fs.cat_file(...)` (e.g. from `concurrent.futures.ThreadPoolExecutor`), they execute native Rust code simultaneously across all CPU cores.
  2. **Concurrent Tokio Multiplexing**: The in-process Tokio runtime distributes async I/O across background worker threads. Calling `tokio::runtime::Handle::block_on` allows multiple calling Python OS threads to execute independent async downloads simultaneously without mutex contention.
  3. **Zero Refactoring for Customers**: Customers (Cx) are **not required to adopt `multiprocessing`**. Existing multi-threaded applications scale linearly and can saturate the 200 Gbps network fabric without process management overhead.

#### Non-Blocking GIL Release Contract
* All blocking and asynchronous Rust operations must release the Python GIL:
  ```rust
  #[pyfunction]
  fn cat_file(py: Python<'_>, path: String, start: Option<i64>, end: Option<i64>) -> PyResult<PyObject> {
      let fs = get_or_init_fs()?;
      let data = py.allow_threads(|| {
          fs.runtime.block_on(fs.core.cat_file(&path, ByteRange::new(start, end), ReadOptions::default()))
      })?;
      bytes_to_py_memoryview(py, data)
  }
  ```

#### Ambient Credential (ADC) Handling
* When 64 DataLoader workers initialize, each worker accesses credentials via standard Application Default Credentials (ADC) managed by the official Google Cloud SDK.
* The link-local GCE metadata server (`169.254.169.254`) easily absorbs concurrent token requests without artificial throttling. Standard SDK retry policies with exponential jitter handle transient network blips cleanly, avoiding the fragility, file permissions, and security risks of custom cross-process token cache files in `/tmp`.

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative 1: Candidate (B) — Out-of-Process UDS gRPC Sidecar Daemon

*   **Architecture**: Run `gcs-rust-fs` as an auxiliary daemon container in the same Kubernetes pod or VM ([`docs/designs/grpc_sidecar.md`](file:///usr/local/google/home/princer/code/gcs-rust-fs/docs/designs/grpc_sidecar.md)). Python workers connect to the daemon over a local Unix Domain Socket (UDS) and stream data using standard gRPC Protobuf messages (`ReadRequest` / `ReadResponse`).
*   **Pros**:
    *   *Shared Storage Instance & In-Memory Cache*: Multiple Python DataLoader worker processes in the GKE pod connect to a single, unified `gcs-rust-fs` daemon instance. Directory listing (`DirCache`) and file metadata (`StatCache`) are cached once in daemon memory and shared across all workers, eliminating redundant GCS metadata RPCs.
    *   *Connection & Runtime Consolidation*: Centralizes GCS TCP/DirectPath connection pools and Tokio runtimes in a single daemon process. Sizing a single connection pool (e.g., 8–16 subchannels) avoids initializing 64 independent connection pools and 64 independent Tokio runtimes across 64 DataLoader workers ($64 \times 4 = 256$ connections per host).
    *   *Consolidated Authentication*: A single daemon manages OAuth2/ADC token refreshes and ALTS handshakes, eliminating the risk of 64 simultaneous workers stampeding the GCE metadata server at startup.
    *   *Language-Agnostic IPC Interface*: Non-Python clients (C++, Go) can theoretically connect to the same daemon.
*   **Cons**:
    *   *Throughput Bottleneck*: Empirical release benchmarks ([`examples/bench_ipc.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/examples/bench_ipc.rs)) show a single UDS socket caps at 0.90 GB/s (~7.7 Gbps). Even scaling to 16 dedicated UDS sockets caps out at **4.69 GB/s (40.3 Gbps)**—falling more than **5x below the 200 Gbps (25.0 GB/s) requirement**.
    *   *Memory Bus Exhaustion*: Pushing 25 GB/s through 4–5 memory copies (GCS socket $\to$ sidecar heap $\to$ Protobuf serialization $\to$ kernel socket buffer $\to$ Python heap) consumes **100 to 125 GB/s of continuous DDR memory bus bandwidth**, starving GPU Host-to-Device transfers.
    *   *High Metadata Overhead*: Unary metadata operations (`info`) average **136.44 µs** (7,329 QPS), breaching the relaxed `<= 100 µs` target and introducing 2,564x higher latency than in-process direct FFI (0.05 µs).
    *   *Single Point of Failure (SPoF)*: A crash, deadlock, or OOM-kill of the sidecar daemon terminates storage for all 8 GPUs simultaneously, killing long-running distributed training jobs.
    *   *Kubernetes Batch Job Zombie Trap*: In Kubernetes batch `Job` workloads, pods remain in `Running` status until *all* containers terminate. An auxiliary daemon container leaves training pods hung as zombies indefinitely unless complex webhook reapers or sentinel shutdown files (`/var/run/gcsfs/.shutdown`) are engineered.
*   **Reason for Rejection**: *Rejected by author*: Candidate (B) cannot physically achieve the 200 Gbps line-rate requirement over UDS sockets, burns 40–50% of the host DDR memory bus, exceeds the metadata latency budget, and introduces severe operational failure modes in Kubernetes batch clusters.

### Alternative 2: Candidate (C) — Out-of-Process Shared-Memory IPC Daemon

*   **Architecture**: Run an auxiliary daemon, but bypass UDS socket buffer copies by transferring bulk payload data through Linux shared memory, evaluating two implementation variants:
    1.  *Variant C1 (POSIX `/dev/shm` Circular Ring Buffers)*: SPSC circular ring buffers backed by tmpfs files in `/dev/shm`, synchronized via atomic `head`/`tail` pointers and `sys_futex` or `eventfd(2)`.
    2.  *Variant C2 (Anonymous `memfd_create` Descriptors via `SCM_RIGHTS`)*: Daemon allocates anonymous RAM pages via `memfd_create`, populates them from GCS, seals the file descriptors (`F_SEAL_WRITE`), and transmits the fds across UDS via `sendmsg(SCM_RIGHTS)`. The Python client calls `recvmsg()` and maps the memory via `mmap(PROT_READ)`.
*   **Pros**:
    *   Eliminates intermediate kernel UDS socket buffer copies for large bulk reads (> 10 MiB).
    *   Retains connection pooling benefits of an out-of-process daemon while reducing memory bus load compared to Candidate (B).
    *   Variant C2 (`memfd_create`) provides automatic kernel resource reclamation when worker processes crash (file descriptors close automatically).
*   **Cons**:
    *   *Small-Read Synchronization Cliff & Syscall Storm*: At 200 Gbps line rate with 64 KiB chunks (metadata, Parquet footers), Candidate (C2) must dispatch **408,000 transfers/sec**. Each transfer requires 7–9 syscalls: `memfd_create` $\to$ `ftruncate` $\to$ `mmap` $\to$ `sendmsg(SCM_RIGHTS)` $\to$ `recvmsg` $\to$ `mmap` $\to$ `munmap` $\times 2$ $\to$ `close` $\times 2$. This causes severe Linux kernel `mmap_lock` spinlock contention and floods all CPU cores with Inter-Processor Interrupts (IPIs) for global TLB shootdowns, making shared memory dramatically *slower* than raw sockets for small I/O.
    *   *Worker Crash Poisoning & Leaked Host Memory (Variant C1)*: DataLoader workers are routinely terminated by the Linux OOM-killer or PyTorch supervisor (`SIGKILL`). Dead workers cannot run userland `shm_unlink()` cleanup hooks, permanently leaking named `/dev/shm` files in tmpfs until the host runs out of RAM. If a worker dies while holding a ring slot, the circular ring deadlocks the shared daemon.
    *   *Cgroup v2 Daemon OOM-Killer Traps (Variant C2)*: In Kubernetes, memory allocated via `memfd_create` is charged to the memory cgroup (`memory.current`) of the creating daemon process. If a Python DataLoader pauses during a long PyTorch GPU training step or CUDA synchronization, unconsumed shared buffers accumulate in the daemon's allocation. When the daemon hits its cgroup memory limit, the Linux kernel OOM-killer sends a `SIGKILL` to the **storage daemon**, terminating storage for **all 8 GPUs on the node**.
    *   *Write-Path TOCTOU Corruption*: In shared memory, Python shares a mutable memory segment with the daemon. If Python mutates buffer contents *while* the Rust daemon is computing CRC32c checksums or streaming bytes into the GCS TLS socket, GCS rejects the upload with `DigestMismatch`, or worse, silent data corruption is written to the cloud bucket. Defending against TOCTOU requires either copying data or applying kernel write sealing (`F_SEAL_WRITE`), adding further syscall overhead.
    *   *Multi-Tenant Data Privacy Breach*: `/dev/shm` uses filesystem namespace paths. In multi-tenant environments where containers share IPC namespaces or access `/dev/shm`, named memory files can be inspected or tampered with by unauthorized processes. Recycling circular ring buffer slots leaks unscrubbed training data unless the daemon zeroes out memory (`memset_s`) at 25 GB/s—burning 25 GB/s of write bandwidth and cutting memory bus performance in half.
    *   *Tonic / gRPC Incompatibility*: HTTP/2 framing over UDS cannot carry ancillary socket control messages (`cmsghdr` with `SCM_RIGHTS`). Tonic abstracts the socket away behind `AsyncRead`/`AsyncWrite`, forcing the team to build an ad-hoc raw socket wire protocol from scratch.
    *   *Kubernetes Batch Job Zombie Trap*: Like Candidate (B), the daemon container still runs indefinitely in the pod, blocking clean batch job termination.
*   **Reason for Rejection**: *Rejected by author*: Candidate (C) attempts to solve an artificial process boundary problem by introducing thousands of lines of fragile, low-level OS plumbing. It suffers from small-read syscall storms, cgroup OOM traps, write-path TOCTOU vulnerabilities, host memory leaks upon worker crash, and Kubernetes batch job completion traps.

---

### Three-Way Architectural Comparison Matrix

| Evaluation Dimension | Candidate (A): In-Process PyO3 [WINNER] | Candidate (B): UDS gRPC Sidecar [REJECTED] | Candidate (C): Shared-Memory Daemon [REJECTED] |
| :--- | :--- | :--- | :--- |
| **Data Movement Architecture** | Pure in-process $O(1)$ pointer handoff (`PyMemoryView`) | Protobuf chunk streaming over UDS socket buffers | Shared memory mapping (`memfd_create` / `/dev/shm` ring) |
| **Host Memory Bus Load at 200 Gbps** | **25.0 GB/s** (1 copy: socket $\to$ RAM) | **100–125 GB/s** (4–5 copies) | **25–50 GB/s** (1 copy into shm, 1 copy in Python if converted) |
| **Cached Metadata Overhead (`info`)** | **0.05 µs (50 ns)**, 18.7M QPS | **136.4 µs**, 7.3K QPS (Breaches 100 µs target) | **~85.0 µs**, ~11.7K QPS (Syscall overhead) |
| **Throughput Ceiling (16 Workers)** | **> 7,400 GB/s** (unbounded memory speed) | **4.69 GB/s (40.3 Gbps)** (Capped by kernel copy) | Up to ~15–20 GB/s (Throttled by `mmap_lock` / IPC signaling) |
| **Small Read Overhead (< 64 KiB)** | Negligible (Direct function call) | Moderate (UDS round-trip) | **Severe** (Syscall storm: 400k `mmap`/`munmap`/sec) |
| **Write-Path Safety** | **Guaranteed** (Worker owns memory) | **Guaranteed** (Data copied into daemon) | **Hazardous** (TOCTOU mutation unless write-sealed) |
| **Fault Isolation & Blast Radius** | **Isolated to single worker**; PyTorch auto-restarts | **SPoF**: Daemon crash kills all 8 GPUs / 64 workers | **SPoF**: Daemon crash or shm ring deadlock kills all workers |
| **Kubernetes Batch Job Lifecycle** | **Clean**: Exits code 0 with training process | **Zombie Trap**: Daemon keeps K8s job running indefinitely | **Zombie Trap**: Daemon keeps K8s job running indefinitely |
| **Crash Safety & Memory Leakage** | **Zero leakage**: Process exit reclaims all pages | **Zero leakage**: Socket drop reclaims buffers | **High Risk**: `/dev/shm` leaks files on `SIGKILL`; memfd cgroup trap |
| **Shared Storage Instance & Cache** | Per-process instances (isolated caches & connections; ML workers read disjoint dataset shards; 0 background threads via current-thread runtime) | **Consolidated single instance** (unified metadata/dir cache, shared connection pool, and amortized Tokio runtime across all pod workers) | **Consolidated single instance** (unified metadata/dir cache, shared connection pool, and amortized Tokio runtime across all pod workers) |
| **Packaging & Deployment** | Standard wheel (`pip install gcsfs`) | Sidecar container, shared volumes, K8s manifests | Sidecar container, `/dev/shm` mounts or Linux 3.17+ syscalls |
| **Conway's Law Ownership** | Single unified library team | Split infrastructure daemon vs client teams | Split infrastructure daemon vs client teams |

--------------------------------------------------------------------------------

## Non-Functional Requirements

### Security & Privacy
*   **Process Address Space Isolation**: Data memory remains strictly confined to the worker process's private virtual address space. Unlike POSIX `/dev/shm` files, there are zero shared filesystem paths or memory segments accessible to unauthorized containers or multi-tenant neighbors.
*   **Credential Hygiene**: OAuth2/ADC bearer tokens are kept strictly in-process memory, avoiding exposure across container IPC namespaces or shared temporary files.

### Performance & Latency
*   **200 Gbps Line-Rate Ingress**: Data flows directly from the network socket into user-space `Bytes` buffers, consuming no more than 25.0 GB/s of DDR memory bus bandwidth at 200 Gbps line rate.
*   **Bounded Metadata Overhead**: Unary metadata queries (`info`, `exists`) execute in `< 1.0 µs` when cached (far exceeding the relaxed `<= 100 µs` requirement).
*   **GIL-Free Asynchronous I/O**: The Python Global Interpreter Lock is released via `py.allow_threads(...)` for all network reads, writes, and metadata fetches.

### Rollout & Rollback
*   **Feature Flag in Python `gcsfs`**: Roll out via an environment variable `GCSFS_USE_RUST=1` or constructor argument `GCSFileSystem(use_rust=True)`.
*   **Automatic Fallback**: If the native `gcsfs_rust` C-extension fails to load (e.g., unsupported platform or architecture), log an advisory warning and fall back transparently to the existing pure-Python `aiohttp` implementation without crashing user applications.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

The implementation is partitioned across two distinct repositories following Conway's Law and clean library boundary separation:
1. **Repository 1 (`raj-prince/gcs-rust-fs`)**: The pure-Rust storage core published to crates.io.
2. **Repository 2 (`fsspec/gcsfs`)**: The Python `gcsfs` package housing the PyO3 C-extension (`gcsfs_rust`), Maturin build config, and `fsspec` adapter.

---

### Step-by-Step Implementation Sequence

#### Part A: `gcs-rust-fs` (crates.io Core Storage Library)

1.  **Phase 1: Pure-Rust Trait Stabilization & API Surface**: Audit [`src/lib.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/lib.rs), [`src/filesystem.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs), and [`src/error.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/error.rs) to ensure all core filesystem types, metadata structs, range options, and error categories are public and well-documented. Ensure `#![forbid(unsafe_code)]` remains strictly enforced.
2.  **Phase 2: Deprecate Out-of-Process Artifacts**: In `src/bin/gcsfs_sidecar.rs`, print an stderr deprecation advisory warning: `eprintln!("WARNING: gcsfs-sidecar is deprecated. Migrate to in-process gcsfs_rust.");`. Annotate `pub mod rpc;` with `#[deprecated(note = "Sidecar RPC module is deprecated; use in-process gcsfs_rust via gcsfs")]`.
3.  **Phase 3: Crates.io Packaging & Release**: Prepare crate metadata in [`Cargo.toml`](file:///usr/local/google/home/princer/code/gcs-rust-fs/Cargo.toml) (description, license, documentation, repository URL) and publish `gcs-rust-fs = "0.1.0"` to crates.io.

#### Part B: `fsspec/gcsfs` (Python Upstream Repository)

4.  **Phase 4: Cargo & Maturin Setup**: In `fsspec/gcsfs`, configure `pyproject.toml` with `build-backend = "maturin"` and create a Rust crate manifest `Cargo.toml` specifying `crate-type = ["cdylib"]`, depending on `gcs-rust-fs = "0.1"` and `pyo3 = { version = "0.22", features = ["extension-module"] }`.
5.  **Phase 5: PyO3 Foundation & Buffer Protocol Exporter**: Implement `src/buffer.rs` (exporting a read-only Python Buffer Protocol object wrapping `bytes::Bytes` with `readonly = 1`) and `src/error.rs` (mapping `gcs_rust_fs::Error` into `fsspec`-compliant Python exceptions like `FileNotFoundError`, `PermissionError`).
6.  **Phase 6: `PyGcsFs` Class, Multi-Threading & Fork-Safety**: Implement `src/fs.rs` with `#[pyclass]` exposing `info`, `cat_file`, `ls`, and `exists`. Implement GIL release via `py.allow_threads` for all network reads. Support multi-threading by multiplexing calls across Tokio runtime worker threads. In `src/fork.rs`, implement PID checking on entry: if `pid != std::process::id()`, orphan the parent runtime without calling `Runtime::drop` to prevent join deadlocks in forked PyTorch DataLoader workers.
7.  **Phase 7: Python `GCSFileSystem` Integration & Tests**: In `gcsfs/core.py`, check for the presence of the native extension; if available and enabled (via `use_rust=True` or `GCSFS_USE_RUST=1`), route file operations through `PyGcsFs`. Implement fallback to `aiohttp` on unsupported platforms. Author multi-worker and multi-threaded integration tests.

---

### Target Files to Create / Modify

#### Repository 1: `raj-prince/gcs-rust-fs` (Pure Rust Crate)

##### 1. [`Cargo.toml`](file:///usr/local/google/home/princer/code/gcs-rust-fs/Cargo.toml) (Modify)
*   **Type / Scope**: Rust Crate Metadata
*   **Proposed Changes**:
    *   Verify package metadata: `name = "gcs-rust-fs"`, `version = "0.1.0"`, `license = "Apache-2.0"`, `edition = "2021"`.
    *   Ensure all public dependencies (`bytes`, `tokio`, `tonic`, `async-trait`) have appropriate version bounds.
    *   **Keep pure-Rust scope**: Do **not** add PyO3 or maturin dependencies here.
*   **Rationale**: Preserves `gcs-rust-fs` as an unpolluted, zero-Python-dependency Rust library consumable anywhere in the Rust ecosystem.

##### 2. [`src/lib.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/lib.rs) (Modify)
*   **Type / Scope**: Crate Root & Module Declarations
*   **Proposed Changes**:
    *   Retain `#![forbid(unsafe_code)]` unconditionally.
    *   Annotate `pub mod rpc;` with:
        ```rust
        #[deprecated(since = "0.1.0", note = "Sidecar RPC is deprecated; use in-process gcsfs_rust via gcsfs")]
        pub mod rpc;
        ```
*   **Rationale**: Enforces 100% safe Rust across the core storage engine while signaling deprecation of the out-of-process gRPC prototype.

##### 3. `src/bin/gcsfs_sidecar.rs` (Modify)
*   **Type / Scope**: Legacy Sidecar Binary
*   **Proposed Changes**:
    *   Emit an advisory deprecation warning on startup to `stderr`:
        ```rust
        eprintln!("WARNING: gcsfs-sidecar is deprecated and will be removed in a future release. Please migrate to the in-process gcsfs-rust native extension in gcsfs.");
        ```
*   **Rationale**: Prevents users from investing in new out-of-process sidecar deployments without breaking existing prototype evaluations.

---

#### Repository 2: `fsspec/gcsfs` (Python Package & PyO3 Extension)

##### 1. `Cargo.toml` (Create in `fsspec/gcsfs`)
*   **Type / Scope**: PyO3 Extension Manifest
*   **Proposed Changes**:
    ```toml
    [package]
    name = "gcsfs-rust"
    version = "0.1.0"
    edition = "2021"

    [lib]
    name = "gcsfs_rust"
    crate-type = ["cdylib"]

    [dependencies]
    gcs-rust-fs = "0.1"
    pyo3 = { version = "0.22", features = ["extension-module"] }
    bytes = "1.5"
    tokio = { version = "1.36", features = ["rt-multi-thread"] }
    ```
*   **Rationale**: Pulls `gcs-rust-fs` directly from crates.io and configures PyO3 as a C-extension dynamic library (`cdylib`).

##### 2. `pyproject.toml` (Create/Modify in `fsspec/gcsfs`)
*   **Type / Scope**: Build Tooling & Packaging
*   **Proposed Changes**:
    *   Configure `maturin` as the build backend:
        ```toml
        [build-system]
        requires = ["maturin>=1.5,<2.0"]
        build-backend = "maturin"

        [tool.maturin]
        module-name = "gcsfs.gcsfs_rust"
        features = ["pyo3/extension-module"]
        ```
*   **Rationale**: Integrates native Rust extension compilation into standard Python wheel workflows (`pip install gcsfs`, `cibuildwheel`).

##### 3. `src/lib.rs` & `src/buffer.rs` (Create in `fsspec/gcsfs`)
*   **Type / Scope**: PyO3 C-Extension Root & Zero-Copy Buffer Exporter
*   **Proposed Changes**:
    *   Implement `PyBytesBuffer` wrapping `bytes::Bytes` with the Python Buffer Protocol (`bf_getbuffer` with `readonly = 1`, `bf_releasebuffer`).
    *   Expose helper to return `PyMemoryView` directly into Python without intermediate copies.
    *   Map `gcs_rust_fs::Error` exhaustively into standard Python exceptions (`FileNotFoundError`, `PermissionError`, `ValueError`, `OSError`).
*   **Rationale**: Provides true zero-copy data transfer to Python and PyTorch (`torch.from_blob()`) with memory safety.

##### 4. `src/fs.rs` & `src/fork.rs` (Create in `fsspec/gcsfs`)
*   **Type / Scope**: Native Filesystem Interface & Concurrency Manager
*   **Proposed Changes**:
    *   Define `#[pyclass] pub struct PyGcsFs` wrapping `Arc<gcs_rust_fs::GcsFs>` and a PID-tracked runtime state.
    *   Implement `cat_file`, `info`, `ls`, and `exists` releasing the Python GIL via `py.allow_threads(...)`.
    *   Multiplex concurrent calls from multiple Python threads across Tokio worker threads, giving multi-threaded applications line-rate throughput without needing `multiprocessing`.
    *   In `fork.rs`, detect child process forks via PID comparison; orphan inherited runtime handles without invoking `Runtime::drop` to prevent deadlocks in PyTorch `DataLoader` workers.
*   **Rationale**: Delivers high-throughput multi-threading and rock-solid multi-processing fork safety in a single unified class.

##### 5. `gcsfs/core.py` (Modify in `fsspec/gcsfs`)
*   **Type / Scope**: Python Filesystem Adapter
*   **Proposed Changes**:
    *   Add an optional `use_rust: bool = True` parameter (also reading `GCSFS_USE_RUST` env var).
    *   Import `gcsfs_rust`; if present and enabled, delegate `cat_file`, `info`, `ls`, and `open` to `PyGcsFs`.
    *   If native extension is unavailable or disabled, log an advisory notice and fall back transparently to pure-Python `aiohttp`.
*   **Rationale**: Guarantees zero regression for environments without prebuilt wheels while delivering 200 Gbps performance when available.

---

### Verification Plan

*   **Repository 1 (`gcs-rust-fs`) Verification**:
    ```bash
    cargo check --all-targets
    cargo test --all-targets
    cargo clippy --all-targets -- -D warnings
    cargo publish --dry-run
    ```
*   **Repository 2 (`fsspec/gcsfs`) Verification**:
    *   **Build & Wheel Packaging**:
        ```bash
        maturin develop --release
        ```
    *   **Multi-Threaded Throughput Benchmark (No Multi-Processing Required)**:
        ```bash
        python -m pytest tests/test_threading.py -k "test_concurrent_threads_saturation"
        ```
    *   **PyTorch Multi-Worker DataLoader Fork-Safety**:
        ```bash
        python -m pytest tests/test_dataloader_fork.py --workers 16 --shards 32
        ```
    *   **Regression Fallback Test**:
        ```bash
        GCSFS_USE_RUST=0 pytest tests/ -v
        ```

--------------------------------------------------------------------------------

*Drafted with EGM*
