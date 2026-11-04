# Architecture Design: Exposing `gcs-rust-fs` as a Unix Domain Socket (UDS) Server for Thin `gcsfs` Clients

*   **Author:** Prince Kumar (@princer)
*   **Status:** Proposed
*   **Date:** 2026-11-04

--------------------------------------------------------------------------------

## Overview

Python [`gcsfs`](https://github.com/fsspec/gcsfs) is the dominant Google Cloud Storage filesystem interface used across distributed machine learning (PyTorch `DataLoader`, Ray Train, JAX) and batch data processing (Dask, Apache Arrow). In containerized multi-tenant environments like Google Kubernetes Engine (GKE), running heavyweight storage runtimes directly inside every Python worker process leads to severe memory duplication, client initialization latency, and complex credential distribution.

This design document establishes the architecture, wire protocol, and implementation plan for exposing [`gcs-rust-fs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/lib.rs) as an out-of-process server communicating over a Unix Domain Socket (UDS, e.g. `unix:///var/run/gcsfs/sidecar.sock`). A lightweight, thin Python `gcsfs` client connects to this socket, translating standard `fsspec` filesystem calls (`info`, `cat_file`, `open`, `ls`, `pipe_file`) into streaming gRPC calls backed by high-throughput HTTP/JSON and DirectPath gRPC transports.

--------------------------------------------------------------------------------

## The Problem

Containerized ML training and data ingestion pipelines in GKE and Vertex AI encounter operational and efficiency barriers when embedding native storage stacks inside every Python container:

1. **Worker Resource Bloat and Connection Explosion**: In an 8-GPU node running 32 to 64 DataLoader workers, each Python worker process independently instantiates an async runtime, TLS state, connection pools, and in-memory metadata/page caches. This multiplies memory footprints by dozens of copies and creates hundreds of concurrent TCP/TLS handshakes to GCS backends at epoch boundaries.
2. **Cold-Start Auth Throttling & Credential Exposure**: Short-lived workers repeatedly negotiate Application Default Credentials (ADC) tokens against the node metadata server (`169.254.169.254`). Managing credentials inside application containers increases security attack surfaces and risk of token leakage across multi-tenant boundaries.
3. **Application Container Coupling**: Any optimization to the Rust core, DirectPath subchannel tuning, or HTTP/2 transport fixes requires rebuilding, re-tagging, and redeploying entire application container images rather than independently updating an infrastructure sidecar daemon.
4. **Python Fork Hazards**: PyTorch DataLoaders relying on `os.fork()` inherit existing background threads and open epoll file descriptors, frequently causing threadpool lock deadlocks and corrupted connection state in unmanaged in-process native runtimes.

An out-of-process daemon communicating over a local Unix Domain Socket decouples the application runtime from storage transport while consolidating caching, connection pooling, and token lifecycle management.

--------------------------------------------------------------------------------

## Goals

*   **Standardized UDS Communication**: Expose `gcs-rust-fs` over a local Unix Domain Socket (`unix:///var/run/gcsfs/sidecar.sock`) using gRPC/HTTP2 (`gcsfs.v1.FileSystemService`).
*   **Full `fsspec` Primitive Parity**: Support foundational filesystem operations:
    *   Metadata inspection (`Info`: file existence, sizes, generation, custom metadata).
    *   Directory listing (`List`: directory contents, recursive listing, object stats).
    *   Ranged streaming reads (`Read`: arbitrary byte ranges, streaming multi-megabyte payloads in configurable chunk sizes).
    *   Atomic streaming writes (`Write`: chunked upload with atomic publishing on stream half-close).
*   **Consolidated Resource Footprint**: Pool GCS connections and cache metadata (directory structure, object stats) within a single daemon process per node/pod, eliminating redundant client handshakes across sibling workers.
*   **Zero-Overhead Fault Isolation & Fork Safety**: Ensure client container failures (`SIGKILL`, cgroup OOM) cannot crash the storage server, and Python workers spawning via `fork()` simply open fresh UDS stream connections without inheriting corrupted in-process threads or sockets.
*   **High-Fidelity Error Mapping**: Propagate GCS error categories across the UDS wire using structured gRPC status codes and metadata headers (`x-gcsfs-error-kind`), mapping transparently to standard Python exceptions (`FileNotFoundError`, `PermissionError`, `FileExistsError`, `ValueError`).

--------------------------------------------------------------------------------

## Non-Goals

*   **In-Process Native C-Extensions**: This document focuses exclusively on the out-of-process UDS server model. In-process PyO3 C-extensions and shared-memory POSIX ring buffers are out of scope for this design.
*   **Remote Cross-Host Gateways**: The UDS server is strictly node-local (co-located in the same Kubernetes Pod via a shared `emptyDir` volume or host path). Remote network proxies across VM boundaries are out of scope.
*   **POSIX File Locking**: Advisory POSIX record locking (`fcntl`) across concurrent remote writers is not supported. Object immutability and atomic overwrites via GCS generations apply.

--------------------------------------------------------------------------------

## Open Questions

*   **Question 1: Optimal Streaming Chunk Size over UDS**: What default chunk size maximizes throughput over UDS socket buffers without exceeding gRPC framing limits or inducing kernel buffer thrashing? (Baseline: `1 MiB` default chunks, up to `3.5 MiB` maximum, leaving 512 KiB headroom under gRPC's 4 MiB frame limit).
*   **Question 2: Directory Listing (`List` RPC)**: Should `List` be added to `gcsfs.v1.FileSystemService` in `proto/gcsfs/v1/filesystem.proto` to complement single-object `Info` for high-performance `ls` and `find` operations? (Resolved in Technical Plan: Add `rpc List(ListRequest) returns (ListResponse)` to avoid N+1 `Info` queries).

--------------------------------------------------------------------------------

## Critical User Journeys (CUJs)

### CUJ 1: Multi-Worker PyTorch Dataset Streaming via UDS

*   **Given**: A PyTorch training pod on GKE with an application container and a `gcsfs-sidecar` container sharing an `emptyDir` volume mounted at `/var/run/gcsfs`.
*   **When**: 16 DataLoader workers concurrently call `fs.open("gs://bucket/shard-001.tar", "rb")` and stream data in 2 MiB blocks.
*   **Then**: Workers connect to `/var/run/gcsfs/sidecar.sock`, issue `Read` requests with byte ranges, receive streaming `ReadResponse` chunks over UDS, and pipeline tensors to GPU memory while the sidecar multiplexes requests across shared GCS HTTP/gRPC channels.

### CUJ 2: Resilient Worker Crash and Clean Daemon Recovery

*   **Given**: An application worker executing a streaming write or read that is abruptly terminated by a Linux kernel cgroup OOM-killer (`SIGKILL`).
*   **When**: The UDS connection abruptly closes or resets (`ECONNRESET`).
*   **Then**: The `gcs-rust-fs` UDS server intercepts the socket disconnection, aborts in-flight upload buffers without publishing half-written files to GCS, cleans up channel state, and continues serving sibling workers without resource leaks or crash cascades.

### CUJ 3: Atomic File Upload with Integrity Verification

*   **Given**: A Python script generating a 500 MiB model checkpoint or Parquet file.
*   **When**: Python `gcsfs` calls `with fs.open("gs://bucket/model.bin", "wb") as f: f.write(data)`.
*   **Then**: The client initiates a streaming `Write` RPC, sends a `WriteSpec` followed by data chunks, and half-closes the stream upon exit. The sidecar commits the upload atomically to GCS, verifies object size and generation, and returns a `WriteResponse` with final object metadata.

--------------------------------------------------------------------------------

## Technical Plan (Architecture)

### 1. High-Level Architecture Overview

The system architecture partitions into an out-of-process Rust daemon (`gcsfs-sidecar`) and a lightweight Python client (`UdsGCSFileSystem`) co-located within the same Kubernetes Pod, communicating via a Unix Domain Socket mounted over a shared local volume:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Kubernetes Pod / Local Host Environment                                     │
│                                                                             │
│  ┌─────────────────────────────────┐   ┌─────────────────────────────────┐  │
│  │ Application Container (Python)  │   │ Application Container (Worker)  │  │
│  │                                 │   │                                 │  │
│  │   gcsfs.core.UdsGCSFileSystem   │   │   PyTorch DataLoader Worker     │  │
│  │   (Thin Python gRPC Client)     │   │   (Thin Python gRPC Client)     │  │
│  └────────────────┬────────────────┘   └────────────────┬────────────────┘  │
│                   │                                     │                   │
│                   │ unix:///var/run/gcsfs/sidecar.sock  │                   │
│                   ▼                                     ▼                   │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Storage Server Container (`gcsfs-sidecar`)                            │  │
│  │                                                                       │  │
│  │   Tonic gRPC Service (`FileSystemRpcService`)                         │  │
│  │   ├── Unix Domain Socket Listener (UnixListener)                      │  │
│  │   ├── High-Fidelity Error Mapping (Status <-> ErrorKind)              │  │
│  │   └── Service Primitives: Info, Read, Write, List                     │  │
│  │                                                                       │  │
│  │   gcs-rust-fs Engine Core                                             │  │
│  │   ├── Shared Metadata & Directory Cache (CachedFs, DirCache)          │  │
│  │   ├── Tokio Asynchronous Runtime (Worker Threadpool)                  │  │
│  │   └── Unified GCS Backend Pool (Transport::Http / Transport::Grpc)    │  │
│  └───────────────────────────────────┬───────────────────────────────────┘  │
│                                      │                                      │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       ▼ Direct Network I/O
                        Google Cloud Storage (DirectPath / HTTP)
```

### 2. Protobuf Service Contract (`gcsfs.v1.FileSystemService`)

The wire contract is defined in `proto/gcsfs/v1/filesystem.proto`:

```protobuf
syntax = "proto3";
package gcsfs.v1;

service FileSystemService {
  // Inspect single path metadata (File or Directory entry)
  rpc Info(InfoRequest) returns (InfoResponse);

  // List directory contents with prefix match
  rpc List(ListRequest) returns (ListResponse);

  // Stream byte range of an object
  rpc Read(ReadRequest) returns (stream ReadResponse);

  // Stream write chunks with atomic commit on half-close
  rpc Write(stream WriteRequest) returns (WriteResponse);
}
```

#### Message Definitions

1. **`Info`**:
   * Request: `path` (`gs://bucket/object`, `bucket/object`, or `bucket/object#generation`).
   * Response: `Entry` (`path`, `kind: FILE | DIRECTORY`, `size`, and full `ObjectStat`).
2. **`List`**:
   * Request: `prefix`, optional `delimiter`, and `detail: bool`.
   * Response: `repeated Entry entries`.
3. **`Read`**:
   * Request: `path`, optional `ByteRange` (`start`, `end` using Python-slice semantics), and `ReadOptions` (`chunk_size`).
   * Response: Server-streaming `ReadResponse` yielding `bytes data` frames (default `1 MiB`, bounded to `3.5 MiB`).
4. **`Write`**:
   * Request: Client-streaming `WriteRequest` with `oneof { WriteSpec spec; bytes data; }`.
   * The first message must contain `WriteSpec` (`path`, `mode: OVERWRITE | CREATE`, `content_type`, `metadata`). Subsequent messages stream raw byte chunks.
   * Atomicity: Half-closing the stream signals completion, triggering atomic flush and GCS commit. Stream cancellation or error discards the pending multipart/resumable upload.
   * Response: `WriteResponse` containing `size` and final `ObjectStat`.

### 3. High-Fidelity Error Mapping Wire Protocol

Standard gRPC error codes conflate distinct filesystem failure modes (e.g. `INVALID_ARGUMENT` covers both invalid paths and invalid byte ranges; `FAILED_PRECONDITION` covers `IsADirectory`, `NotADirectory`, and `DirectoryNotEmpty`).

To preserve 100% exception fidelity across the UDS wire, the server attaches the exact [`ErrorKind`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/error.rs) string to the gRPC response metadata header `x-gcsfs-error-kind`.

The thin client reads `x-gcsfs-error-kind` from trailing metadata and maps directly to Python standard exceptions:

| Rust `ErrorKind` | Header Value (`x-gcsfs-error-kind`) | Canonical gRPC Code | Python Standard Exception |
|---|---|---|---|
| `NotFound` | `"not_found"` | `NOT_FOUND` | `FileNotFoundError` |
| `PermissionDenied` | `"permission_denied"` | `PERMISSION_DENIED` | `PermissionError` |
| `Unauthenticated` | `"unauthenticated"` | `UNAUTHENTICATED` | `PermissionError` |
| `AlreadyExists` | `"already_exists"` | `ALREADY_EXISTS` | `FileExistsError` |
| `IsADirectory` | `"is_a_directory"` | `FAILED_PRECONDITION` | `IsADirectoryError` |
| `NotADirectory` | `"not_a_directory"` | `FAILED_PRECONDITION` | `NotADirectoryError` |
| `InvalidPath` | `"invalid_path"` | `INVALID_ARGUMENT` | `ValueError` |
| `InvalidRange` | `"invalid_range"` | `INVALID_ARGUMENT` | `ValueError` |
| `Timeout` | `"timeout"` | `DEADLINE_EXCEEDED` | `TimeoutError` |
| `Unsupported` | `"unsupported"` | `UNIMPLEMENTED` | `NotImplementedError` |
| `Other` | `"other"` | `INTERNAL` | `OSError` |

### 4. Server Daemon Architecture (`gcsfs-sidecar`)

The server binary is implemented in `src/bin/gcsfs_sidecar.rs` and backed by `src/rpc/server.rs`:

1. **Multi-UDS Listener & Stream Merging (`serve_multiple_uds_with_mode_and_shutdown`)**:
   * Binds to one or more Unix Domain Socket paths simultaneously (via repeated `--uds <PATH>` flags, defaulting to `/var/run/gcsfs/sidecar.sock`).
   * Safely creates parent directories if absent (`fs::create_dir_all`).
   * Probes for existing stale socket files on startup and unlinks them (`fs::remove_file`) before binding `tokio::net::UnixListener`.
   * Sets configurable Unix file permissions (`--mode <OCTAL>`, default `0666`) on all bound socket files.
   * Merges incoming connection streams across all sockets using `futures_util::stream::select_all`, allowing multiple distinct pods, namespaces, or worker cgroups to talk to the same storage daemon with pooled connections and shared caches.
2. **Graceful Shutdown**:
   * Traps OS signals (`SIGINT`, `SIGTERM`).
   * Drains in-flight gRPC streams via `serve_with_incoming_shutdown`.
   * Unlinks all socket files upon process termination.
3. **Transport & Caching Configuration**:
   * Supports command-line flags:
     * `--uds <PATH>`: Path to Unix Domain Socket (repeatable for multi-UDS).
     * `--mode <OCTAL>`: Unix socket file permissions (e.g. `0666`).
     * `--tcp <ADDR>`: Alternative TCP address for testing.
     * `--transport <http|grpc>`: Underlying GCS transport (`Transport::Http` default, or `Transport::Grpc` for DirectPath).
     * `--cache`: Wraps backend in [`CachedFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/cached.rs) to share directory listings and metadata cache across all connected client processes.

### 5. Thin Python Client Architecture (`gcsfs-uds`)

The thin Python client implements an `fsspec`-compliant filesystem by wrapping `grpc.aio` or sync `grpc` channels over UDS:

1. **UDS Channel Connection**:
   * Opens a gRPC channel targeting the UDS endpoint using standard gRPC UDS target URI syntax: `unix:///var/run/gcsfs/sidecar.sock`.
2. **`fsspec` Delegation**:
   * `info(path)`: Invokes `stub.Info(InfoRequest(path=path))`, unpacks `entry`, and returns a dictionary matching `fsspec` file attributes (`name`, `size`, `type: 'file' | 'directory'`, `generation`).
   * `cat_file(path, start, end)`: Invokes `stub.Read(ReadRequest(path=path, range=ByteRange(start=start, end=end)))`, iterates over streaming chunks, and concatenates bytes into a single `bytes` object.
   * `ls(path, detail)`: Invokes `stub.List(ListRequest(prefix=path))`, returning listing records.
   * `open(path, mode="rb" | "wb")`: Returns a stream-backed file object (`UdsBufferedFile`):
     * Read mode: Buffers chunks on demand using range reads.
     * Write mode: Accumulates bytes and streams chunks over a client-streaming `Write` RPC, half-closing on `.close()`.

--------------------------------------------------------------------------------

## Alternatives Considered

### Alternative A: Raw Binary Socket Protocol (Custom Framing)

*   **Pros**: Slightly smaller message overhead compared to HTTP/2 and Protobuf framing.
*   **Cons**: Requires authoring custom framing, multiplexing, backpressure flow-control, and Python deserialization logic from scratch; breaks ecosystem observability (cannot inspect traffic with standard gRPC tools).
*   **Reason for Rejection**: Rejected by author: gRPC over UDS provides battle-tested HTTP/2 multiplexing, backpressure streaming, cross-language code generation, and metadata headers out-of-the-box.

### Alternative B: REST/HTTP-1.1 Daemon over UDS

*   **Pros**: Extremely simple Python client using `requests_unixsocket` or `httpx`.
*   **Cons**: Lacks efficient binary streaming framing; chunked transfer encoding in HTTP/1.1 over UDS introduces higher CPU overhead and serialization friction compared to Protobuf; lacks native bidirectional streaming.
*   **Reason for Rejection**: Rejected by author: gRPC provides strict Protobuf schemas, lower serialization CPU tax, and native streaming RPCs (`Read` and `Write`).

--------------------------------------------------------------------------------

## Non-Functional Requirements

### Security & Privacy
*   **File Permissions**: The Unix Domain Socket file must be created with appropriate permissions (e.g. `0660` or `0666`) to ensure authorized pod containers can communicate without exposing access to unprivileged host users.
*   **Centralized Credential Scoping**: GCS credentials remain isolated inside the sidecar container, removing the need to project cloud identity tokens or service account keys into application containers.

### Performance & Latency
*   **Throughput**: Deliver > 1.5 GB/s (12+ Gbps) aggregate read throughput over UDS per socket stream.
*   **Metadata Overhead**: Keep unary `Info` queries over UDS under 500 µs (sub-millisecond round-trip across UDS).
*   **Streaming Chunk Size**: Stream payloads in 1 MiB chunks to maximize socket buffer efficiency while remaining well within gRPC's 4 MiB frame cap.

### Rollout & Rollback
*   **Container Sidecar Injection**: Deployable via standard Kubernetes Pod spec manifests (co-located container sharing an `emptyDir` volume).
*   **Zero Application Recompilation**: Applications consume standard `gcsfs` APIs without needing Rust build tools or native C-compilers in the application container image.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Step-by-Step Implementation Sequence

1.  **Phase 1: Proto Schema Enhancement**:
    *   Add `ListRequest` and `ListResponse` to `proto/gcsfs/v1/filesystem.proto`.
    *   Regenerate protobuf bindings via `build.rs` and verify Tonic stubs compile cleanly.
2.  **Phase 2: Server Service & Binary Stabilization**:
    *   Implement `list` handler in `src/rpc/server.rs` backed by `FileSystem::ls` / `FileSystem::find`.
    *   Enhance `src/bin/gcsfs_sidecar.rs` with clean signal handling (`tokio::signal::unix`), socket chmod permissions (`0666`), and configurable chunk size arguments.
3.  **Phase 3: Rust Remote Client Parity**:
    *   Add `list` method to `RemoteFs` in `src/rpc/client.rs`.
    *   Expand `tests/rpc.rs` integration suite to cover `list`, concurrent UDS streams, and socket reconnection.
4.  **Phase 4: Thin Python Client (`gcsfs_uds`)**:
    *   Generate Python gRPC stubs from `filesystem.proto`.
    *   Implement `UdsGCSFileSystem` adhering to `fsspec.spec.AbstractFileSystem`.
    *   Implement `UdsBufferedFile` for streaming reads and atomic uploads.
    *   Map trailing metadata `x-gcsfs-error-kind` to Python standard exceptions.
5.  **Phase 5: Kubernetes Manifests & End-to-End Validation**:
    *   Author reference Kubernetes Pod manifest demonstrating multi-container shared `emptyDir` volume mount.
    *   Author automated integration benchmark measuring streaming throughput over UDS.

---

### Target Files to Create / Modify

#### 1. [`proto/gcsfs/v1/filesystem.proto`](file:///usr/local/google/home/princer/code/gcs-rust-fs/proto/gcsfs/v1/filesystem.proto) (Modify)
*   **Type / Scope**: Protobuf Schema Definition
*   **Proposed Changes**:
    *   Add `rpc List(ListRequest) returns (ListResponse);` to `service FileSystemService`.
    *   Define `message ListRequest { string prefix = 1; optional string delimiter = 2; bool detail = 3; }`.
    *   Define `message ListResponse { repeated Entry entries = 1; }`.
*   **Rationale**: Enables directory listing and tree traversal over UDS without requiring N+1 `Info` queries.

#### 2. [`src/rpc/server.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/server.rs) (Modify)
*   **Type / Scope**: Rust gRPC Server Service Implementation
*   **Proposed Changes**:
    *   Implement `list` RPC method on `FileSystemRpcService` calling `self.fs.ls()` or `self.fs.find()`.
    *   Expose socket file permission configuration (`0666` default for UDS).
*   **Rationale**: Completes `fsspec` API coverage for the gRPC server.

#### 3. [`src/bin/gcsfs_sidecar.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/bin/gcsfs_sidecar.rs) (Modify)
*   **Type / Scope**: Server Binary CLI & Lifecycle
*   **Proposed Changes**:
    *   Remove deprecation warnings (since sidecar is the target architecture of this design).
    *   Add `--socket-mode <OCTAL>` flag (defaulting to `0666`) to configure UDS socket file permissions.
    *   Add Unix signal traps (`SIGTERM`, `SIGINT`) to ensure graceful cleanup of the socket file.
*   **Rationale**: Ensures reliable deployment in multi-user and Kubernetes pod container environments.

#### 4. [`src/rpc/client.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/client.rs) (Modify)
*   **Type / Scope**: Rust Remote Client
*   **Proposed Changes**:
    *   Implement `ls` on `RemoteFs` delegating to the new `List` RPC.
*   **Rationale**: Keeps Rust `RemoteFs` client 100% feature-complete with the server.

#### 5. [`tests/rpc.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/tests/rpc.rs) (Modify)
*   **Type / Scope**: Integration Test Suite
*   **Proposed Changes**:
    *   Add integration tests for `List` RPC over UDS.
    *   Add multi-client concurrent streaming test over UDS.
*   **Rationale**: Verifies streaming correctness, concurrency, and error handling over UDS.

#### 6. `python/gcsfs_uds/` (Create)
*   **Type / Scope**: Thin Python gRPC Client Library
*   **Proposed Changes**:
    *   `python/gcsfs_uds/client.py`: Implements `UdsGCSFileSystem` extending `fsspec.spec.AbstractFileSystem`.
    *   `python/gcsfs_uds/file.py`: Implements `UdsBufferedFile` for streaming read/write file handles.
    *   `python/gcsfs_uds/errors.py`: Maps `x-gcsfs-error-kind` into Python exception types.
*   **Rationale**: Delivers the thin client package for Python users.

---

### Verification Plan

*   **Rust Server & Client Build**:
    ```bash
    cargo check --all-targets
    cargo test --test rpc
    ```
*   **End-to-End UDS Streaming Benchmark**:
    ```bash
    cargo run --release --bin gcsfs-sidecar -- --uds /tmp/test-sidecar.sock &
    # Run client streaming verification
    ```
*   **Python Integration Tests**:
    ```bash
    pytest python/gcsfs_uds/tests/ -v
    ```
