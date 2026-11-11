# gcs-rust-fs Blueprint

## System Overview

Non-POSIX, pure-Rust file-system primitives for Google Cloud Storage, built on
the official [`google-cloud-storage`](https://docs.rs/google-cloud-storage)
Rust SDK.

This crate is the **Rust core** behind the `gcsfs` Rust backend. It deliberately
has no Python/PyO3 dependency: the storage logic lives here, is tested with
`cargo test`, and is reusable from any Rust program. A thin PyO3 bridge inside
`gcsfs` converts the types below into Python objects.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  gcs-rust-fs (this repo — pure Rust, zero Python knowledge)          │
│                                                                      │
│  contract        trait FileSystem  (≙ fsspec AbstractFileSystem)     │
│                  trait File        (≙ fsspec AbstractBufferedFile)   │
│                  Entry, *Options, ByteRange, Error { ErrorKind }     │
│                        ▲ six required primitives:                    │
│                        │ info · ls · open · rm_file · mkdir · rmdir  │
│  derived ops     find walk du glob cat cat_ranges rm copy mv put …   │
│                  (fsspec semantics, written once, storage-agnostic)  │
│                        ▲                                             │
│  implementation  src/gcs/: GcsFs, GcsFile, BucketKind, Transport     │
│                  (the only code that knows about buckets, gRPC, …)   │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ Cargo dependency
┌──────────────────────────────▼───────────────────────────────────────┐
│  gcsfs/rust (thin PyO3 bridge, lives in the gcsfs repo)              │
│  Arc<dyn FileSystem> / Box<dyn File>; Bytes → PyBytes,               │
│  Entry → info dict, ErrorKind → exceptions                           │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────┐
│  gcsfs.GCSFileSystem(read_backend="rust")                            │
└──────────────────────────────────────────────────────────────────────┘
```

Both traits are object-safe `async_trait` traits, so the bridge can pick an
implementation at runtime; `tests/common/mod.rs` is an in-memory implementation
that exercises every derived operation without network. The full semantics
(directory emulation, fsspec copy rules, error classes, open decisions) are in
[`docs/filesystem_api_design.md`](docs/filesystem_api_design.md).
implementation at runtime; [`tests/common/mod.rs`](tests/common/mod.rs) is an in-memory implementation
that exercises every derived operation without network. The core contract and
decorator architecture are documented in [`src/README.md`](src/README.md) and
[`docs/designs/async_prefetch_engine.md`](docs/designs/async_prefetch_engine.md).

## Features

Every `fsspec` operation that `gcsfs` needs, behind the `FileSystem` trait:

| Group | Methods | Implementation on GCS |
|-------|---------|-----------------------|
| metadata | `info`, `exists`, `is_file`, `is_dir`, `size` | `GetObject` **and** a directory probe issued concurrently (like gcsfs); `GetBucket` / `GetFolder` where applicable |
| listing | `ls`, `find`, `walk`, `du`, `glob` | one `ListObjects` page stream per call (`find` is a single flat listing, not a walk); HNS folders via `ListFolders` |
| reading | `cat_file`, `cat`, `cat_ranges`, `open(rb)` → `read`/`seek`/`read_range`, `get_file` | gRPC `BidiReadObject` or JSON API ([Transports](#transports)); read handles pin one generation |
| writing | `pipe_file`, `put_file`, `put`, `open(wb/xb)` → `write`/`flush`/`close` | parallel composite uploads for files $\ge 64\text{ MiB}$ via `put_file`; resumable `WriteObject` streamed from a spawned task; zonal buckets use appendable objects |
| copy / move | `copy_file`, `copy`, `move_file`, `mv` | server-side `RewriteObject`; atomic `MoveObject` in one bucket; HNS directories renamed with `RenameFolder` |
| delete | `rm_file`, `rm`, `rmdir` | one listing + concurrent `DeleteObject`; folders deepest-first; bucket deletion for `rm -r bucket` |
| directories | `mkdir`, `rmdir`, `GcsFs::create_bucket` | buckets, `dir/` placeholders (flat), real folders (HNS) |
| caching (opt-in) | `CachedFs::new(fs, CacheConfig)`, `invalidate_cache`, `ListOptions::refresh` | `fsspec` `dircache` semantics for `ls` **and** `info` from one store; see [Caching](#caching) |

### Bucket kinds

Cloud Storage has three kinds of bucket; `GcsFs` detects each bucket's kind
on first use (one `GetStorageLayout`, cached) and adapts:

| `BucketKind` | Directories | Reads | Writes | Not available |
|--------------|-------------|-------|--------|---------------|
| `Flat` | emulated from prefixes and `dir/` placeholders; an empty directory only exists with a placeholder (`MkdirOptions::placeholder`) | gRPC or HTTP | resumable upload, published on `close` | append mode |
| `Hierarchical` (HNS) | real folders — empty ones exist, `mkdir`/`rmdir` create and delete them, `mv` of a directory is one atomic rename | gRPC or HTTP | as flat | append mode, object versioning |
| `Zonal` (Rapid Storage) | as HNS | gRPC only (the `Transport` setting is ignored) | appendable object: `flush` persists bytes readers can see; `close` finalizes only with `GcsFsBuilder::finalize_on_close(true)`; `open(ab)` reopens by generation | server-side copy (`copy_file` → `Unsupported`), cross-bucket `move_file` |

`GcsFsBuilder::bucket_kind(bucket, kind)` pre-seeds the cache for principals
that cannot read the layout. If detection fails the bucket is treated as flat
for that call (and retried next time), which is what gcsfs does.

Other design points:

* **Python-slice byte ranges.** `ByteRange::new(start, end)` follows the exact
  `fsspec.cat_file(start, end)` contract: exclusive `end`, optional bounds,
  negative bounds count from the end, past-the-end reads are empty. Ranges the
  API supports natively (absolute offsets, `head(n)`, `tail(n)`) never cost an
  extra request; only mixed negative bounds trigger one `GetObject`.
* **Parallel composite upload (`put_file`).** Files $\ge 64\text{ MiB}$ uploaded via
  `FileSystem::put_file` to composite-capable buckets (flat and hierarchical) automatically
  split into up to 32 balanced component slices streamed concurrently (`FileSliceSource`)
  using positional reads without in-memory buffering. Upon completion, parts are
  atomically composed on the server via `StorageControl::compose_object` with precondition
  matching (`WriteMode::Create` / `CreateNew`), and temporary components are cleaned up
  automatically via `CompositeUploadGuard`. Smaller files (< 64 MiB) and zonal buckets
  stream directly via single-stream uploads. Slicing threshold, chunk size, and concurrency
  can be configured via `WriteOptions::with_composite_threshold`, `with_chunk_size`, and
  `with_concurrency`.
* **Typed errors.** Every `Error` has an `ErrorKind` (`NotFound`,
  `IsADirectory`, `AlreadyExists`, `DirectoryNotEmpty`, `Unsupported`, ...)
  classified from the gRPC status *or* HTTP status, so bridges never
  string-match messages.
* **gcsfs path conventions.** Paths are strings: `gs://b/k`, `gcs://b/k`,
  `b/k`, `/b/k`, trailing slashes ignored, `b/k#<generation>` pins a
  generation — exactly like `GCSFileSystem.split_path`. Entry paths come back
  as `bucket/key` without scheme or trailing slash.
* **Snapshot semantics.** Read handles pin the generation observed at open
  time, so concurrent overwrites never produce torn reads.
* **No global state, no hidden cache.** `GcsFs` wraps the SDK's pooled clients
  and is cheap to clone; every call reflects the bucket at the time of the
  request. Caching is a separate, explicit layer ([Caching](#caching)).

## Usage

```toml
[dependencies]
gcs-rust-fs = { git = "https://github.com/raj-prince/gcs-rust-fs" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use gcs_rust_fs::{
    ByteRange, FileSystem, FindOptions, GcsFs, OpenOptions, ReadOptions, RmOptions, WriteOptions,
};

#[tokio::main]
async fn main() -> gcs_rust_fs::Result<()> {
    let fs = GcsFs::new().await?; // Application Default Credentials

    let info = fs.info("gs://my-bucket/checkpoint.pt").await?;
    println!("{} {} bytes", info.kind.as_str(), info.size);

    let header = fs
        .cat_file("my-bucket/checkpoint.pt", ByteRange::head(1 << 20), ReadOptions::default())
        .await?;
    let footer = fs
        .cat_file("my-bucket/checkpoint.pt", ByteRange::tail(64), ReadOptions::default())
        .await?;

    for entry in fs.find("my-bucket/data", FindOptions::default()).await? {
        println!("{:>12} {}", entry.size, entry.path);
    }

    fs.pipe_file("my-bucket/out/small.bin", footer, WriteOptions::default()).await?;
    let mut big = fs.open("my-bucket/out/big.bin", OpenOptions::write()).await?;
    big.write(header).await?;
    big.close().await?; // published atomically here

    fs.rm("my-bucket/out", RmOptions { recursive: true, ..Default::default() }).await?;
    Ok(())
}
```

Writing to zonal buckets additionally requires building with the storage
SDK's `google_cloud_unstable_storage_bidi` cfg — this repo sets it in
[`.cargo/config.toml`](.cargo/config.toml); see the
[dev guide](docs/dev_guide.md#zonal-writes-need-a-rustc-cfg).
[`.cargo/config.toml`](.cargo/config.toml).

## Caching

`CachedFs` wraps any `FileSystem` (it is one itself, so `Arc<dyn FileSystem>`
works either way) and gives `ls` and `info` the `fsspec` directory-cache
behaviour that `gcsfs` users rely on, with both served from **one** store: a
listing warms `info` for every child, and individual `info` results are kept
without ever being mistaken for a complete listing. Design and rationale:
[`docs/dir_cache_design.md`](docs/dir_cache_design.md); the planned data
half (readahead and page cache):
[`docs/page_cache_design.md`](docs/page_cache_design.md).
without ever being mistaken for a complete listing. On the data path,
`CachedFs` also implements a Linux v6.6-inspired asynchronous readahead engine
(Mode 1 private drop-behind readahead by default via `DataConfig::readahead_only()`,
and Mode 2 shared generation-keyed LRU `PageCache` via `DataConfig::page_cache(capacity)`):
see [`docs/designs/async_prefetch_engine.md`](docs/designs/async_prefetch_engine.md).

```rust
use bytes::Bytes;
use gcs_rust_fs::{CacheConfig, CachedFs, FileSystem, GcsFs, ListOptions, WriteOptions};

let fs = CachedFs::new(GcsFs::new().await?, CacheConfig::default());

fs.ls("my-bucket/data", ListOptions::default()).await?; // one request
fs.info("my-bucket/data/part-0.parquet").await?; // answered from the listing
assert!(fs.is_dir("my-bucket/data").await?); // likewise

let opts = WriteOptions::default();
fs.pipe_file("my-bucket/data/new.bin", Bytes::from_static(b"x"), opts).await?;
fs.ls("my-bucket/data", ListOptions::default()).await?; // refetched: own writes invalidate

fs.invalidate_cache(Some("my-bucket/data")); // another writer changed things
let refresh = ListOptions { refresh: true, ..Default::default() };
fs.ls("my-bucket/data", refresh).await?; // or bypass once and re-store
println!("{:?}", fs.stats()); // hits / misses / invalidations
```

What it does, and the `gcsfs` behaviour it mirrors:

* Every mutating call — `pipe_file`, `put_file`, `copy_file`, `move_file`,
  `mkdir`, `rmdir`, `rm_file`, the bulk `rm`/`copy`/`mv`/`put`, and write
  handles at both `open` and `close` — invalidates exactly what gcsfs's
  `DirCacheUpdater` invalidates: a write drops the cached parent (or the
  parent and all ancestors when the parent was not cached, since an implicit
  directory may have appeared); deletes and moves drop the affected subtree
  plus ancestors.
* A complete `find` (`withdirs`, no `maxdepth`) fills every directory it
  visits, so a following `walk`/`du`/`glob` costs no requests
  (`_find(update_cache=True)`).
* `ListOptions::versions` and `path#generation` bypass the cache; data reads
  are never cached.
* `CacheConfig { ttl, max_dirs, negative, populate_from_find }` map to
  fsspec's `listings_expiry_time`, `max_paths` and the implicit
  `_ls_from_cache` behaviours. `negative` (answer *not found* from a cached
* `ListOptions::versions` and `path#generation` bypass the directory/metadata cache.
* `CacheConfig { ttl, max_dirs, negative, populate_from_find, data }` map to
  fsspec's `listings_expiry_time`, `max_paths`, the implicit
  `_ls_from_cache` behaviours, and the data readahead/page-cache engine (`data`
  defaults to `Some(DataConfig::readahead_only())`; set `data: None` for raw
  unbuffered pass-through). `negative` (answer *not found* from a cached
  listing without asking) is **off** by default because it is the one answer
  that is wrong when another process creates a file; turn it on for
  `exists`-heavy loops when this client is the only writer.
* The cache is exact for this client's own writes. Other writers are
  reconciled by `ttl`, `invalidate_cache(path)` / `invalidate_cache(None)`,
  or `ListOptions::refresh` — the same contract `gcsfs` documents.

`GcsFs` on its own caches nothing except each bucket's kind; `invalidate_cache`
and `refresh` are no-ops there.

## Transports

Metadata always travels over gRPC. For object **data** in flat and HNS
buckets choose with `GcsFsBuilder::transport(..)` or the
`GCS_RUST_FS_TRANSPORT` environment variable:

| `Transport` | Mechanism | Notes |
|-------------|-----------|-------|
| `Grpc` (default) | `Storage::open_object` → `BidiReadObject` | Fastest path; one RPC per `cat_file` (open + read are fused). **Only enabled for some projects/buckets** — when the service reports it unavailable the read falls back to HTTP with a warning. |
| `Http` | `Storage::read_object` → JSON API with `Range` headers | Universally available. |
| `Http` | `Storage::read_object` → JSON API with `Range` headers | Universally available. Stripes requests across a pool of `Storage` HTTP clients (`GcsFsBuilder::http_pool_size` / `GCS_RUST_FS_HTTP_POOL_SIZE`, default `16`). |

Zonal buckets are gRPC-only and ignore the setting. Both transports return
byte-identical results and classify errors identically.

## Error mapping for bridges

| `ErrorKind` | Source | Suggested Python exception |
|-------------|--------|----------------------------|
| `NotFound` | 404 / `NOT_FOUND`, missing directory | `FileNotFoundError` |
| `PermissionDenied`, `Unauthenticated` | 403 / 401 | `PermissionError` |
| `IsADirectory`, `NotADirectory` | file operation on a directory and vice versa | `IsADirectoryError`, `NotADirectoryError` |
| `AlreadyExists` | create-only write on an existing object, `mkdir` of an existing bucket/folder | `FileExistsError` |
| `DirectoryNotEmpty` | `rmdir` | `OSError(ENOTEMPTY)` |
| `Unsupported` | append on immutable objects, server-side copy on zonal buckets | `NotImplementedError` |
| `OutOfRange` | 416 / `OUT_OF_RANGE` (only from `File::read_range`-free paths; `cat_file` returns `b""`) | `RuntimeError` or `b""` |
| `InvalidPath`, `InvalidRange`, `InvalidConfig` | client-side validation | `ValueError` |
| `Timeout` | 408 / 504 / `DEADLINE_EXCEEDED` | `TimeoutError` |
| `Closed` | I/O on a closed handle | `ValueError("I/O operation on closed file")` |
| `ClientInit`, `PreconditionFailed`, `Other` | everything else | `OSError` |

`Error` also converts into `std::io::Error` with the analogous `io::ErrorKind`.

## Example CLI

```bash
cargo run --example gcs -- info  gs://my-bucket/path
cargo run --example gcs -- ls    gs://my-bucket/dir
cargo run --example gcs -- find  gs://my-bucket/dir --withdirs
cargo run --example gcs -- cat   gs://my-bucket/file --start -100 --transport http
cargo run --example gcs -- put   ./local.bin gs://my-bucket/file
cargo run --example gcs -- mv    gs://my-bucket/dir gs://my-bucket/renamed -r
cargo run --example gcs -- rm    gs://my-bucket/dir -r
cargo run --example gcs -- kind  my-bucket
```

## Running the `gcsfs-sidecar` UDS Server
 
`gcs-rust-fs` provides an out-of-process gRPC daemon (`gcsfs-sidecar`) that serves `gcsfs.v1.FileSystemService` over Unix Domain Sockets:
 
```bash
# Start server listening on /tmp/gcsfs.sock
cargo run --bin gcsfs-sidecar -- --uds /tmp/gcsfs.sock --mode 0666 --cache
# Start server listening on /tmp/gcsfs.sock with metadata cache, Mode 1 readahead, and 32 HTTP clients
cargo run --bin gcsfs-sidecar -- --uds /tmp/gcsfs.sock --mode 0666 --cache --readahead --http-pool-size 32

# Or serve multiple UDS paths simultaneously:
# Or serve multiple UDS paths simultaneously (optionally with Mode 2 shared page cache):
cargo run --bin gcsfs-sidecar -- \
  --uds /tmp/gcsfs-worker0.sock \
  --uds /tmp/gcsfs-worker1.sock \
  --mode 0666
  --mode 0666 \
  --readahead \
  --page-cache-mb 512
```

### Testing the Running Server with the CLI

Point `examples/gcs.rs` at the running socket via `--uds <PATH>`:

```bash
# Query info via UDS
cargo run --example gcs -- info gs://my-bucket/checkpoint.pt --uds /tmp/gcsfs.sock

# List directory contents via UDS
cargo run --example gcs -- ls gs://my-bucket/data --uds /tmp/gcsfs.sock

# Stream byte range via UDS
cargo run --example gcs -- cat gs://my-bucket/checkpoint.pt --start 0 --end 1024 --uds /tmp/gcsfs.sock

# Write data via UDS stream
cargo run --example gcs -- pipe gs://my-bucket/hello.txt "hello from uds" --uds /tmp/gcsfs.sock
```

## Development

```bash
cargo test                                                # unit + in-memory FS + doctests; live tests self-skip
GCS_RUST_FS_TEST_OBJECT=gs://my-bucket/file.bin cargo test --test live          # read-only, real bucket
GCS_RUST_FS_TEST_BUCKETS=flat=b1,hns=b2,zonal=b3 cargo test --test live          # read/write, per bucket kind
```

Building the example binary, the CI lint gate and the repo layout are in
[`docs/dev_guide.md`](docs/dev_guide.md).
Benchmark reports and architectural designs are located in [`docs/`](docs/):
- [`docs/native_rust_fs_benchmark_report.md`](docs/native_rust_fs_benchmark_report.md)
- [`docs/uds_rust_fs_benchmark_report.md`](docs/uds_rust_fs_benchmark_report.md)
- [`docs/designs/`](docs/designs/)

## Local Interface & Invariants
- `[build.rs:main]`: Cargo build script executing `tonic_prost_build` to compile `proto/gcsfs/v1/filesystem.proto` into Rust tonic client/server code during crate build.
- `[build.rs:rerun-if-changed]`: Re-runs code generation whenever `proto/gcsfs/v1/filesystem.proto` is modified.

## Subdirectories & System Boundaries
- [src](./src/README.md): Core `FileSystem` and `File` async traits, error classification, range abstractions (`ByteRange`), in-memory directory cache decorator (`CachedFs`), GCS storage backend (`gcs`), and gRPC sidecar daemon (`bin`, `rpc`).
- [src](./src/README.md): Core `FileSystem` and `File` async traits, error classification, range abstractions (`ByteRange`), directory/metadata cache and readahead/page-cache decorator (`CachedFs`), GCS storage backend (`gcs`), and gRPC sidecar daemon (`bin`, `rpc`).
- [proto](./proto/README.md): Protobuf schema root for all RPC service definitions and data serialization contracts used across `gcs-rust-fs`.
- [examples](./examples/README.md): CLI example utility (`examples/gcs.rs`) for interacting with Google Cloud Storage buckets (Flat, Hierarchical Namespace, and Zonal) using the `FileSystem` interface.
- [examples](./examples/README.md): CLI example utility (`examples/gcs.rs`) and benchmark suites (`examples/bench_ipc.rs`, `examples/bench_live_readahead.rs`, `examples/bench_uds_readahead.rs`).
- `docs/`: Benchmark reports and architecture design documents (`docs/designs/`).
- `tests/`: Integration test suites (`tests/memory_fs.rs`, `tests/cache.rs`, `tests/page_cache.rs`, `tests/rpc.rs`, `tests/live.rs`).

## Global Invariants
- Directories are emulated within flat buckets via `/` delimiters and placeholder objects; Hierarchical Namespace (HNS) buckets provide real server-side directory objects and atomic folder renames.
- Read handles pin the object generation at open time, guaranteeing immutable snapshot reads immune to concurrent overwrites or deletions.
- Regular writes are staged and published atomically on Cloud Storage only upon successful `close()`; unclosed dropped handles discard pending uploads.
- Python-slice byte range indexing (`ByteRange`) applies across all read APIs, resolving bounds backwards for negative indices and returning empty byte buffers for out-of-range reads.
- Cache entries in `CachedFs` never store file payload data; mutating operations consistently invalidate affected directories, ancestors, or entire subtrees across both flat and hierarchical stores.
- `CachedFs` maintains `fsspec`-consistent directory and entry metadata caches (invalidating affected directories, ancestors, or subtrees on mutation) alongside Mode 1 per-handle drop-behind readahead (`DataConfig::readahead_only()`, default) or Mode 2 shared generation-keyed LRU page caching (`DataConfig::page_cache`).

## Global Side Effects
- Cloud Storage Remote State: Communicates over gRPC and HTTP with Google Cloud Storage endpoints, creating, rewriting, renaming, and deleting objects and folders.
- Sidecar IPC & Networking: Binds Unix Domain Sockets or TCP sockets to serve incoming `FileSystemService` requests.
- Local Disk I/O: Reads local files for upload (`put`), writes downloaded objects (`get`), and generates Protobuf Rust code in `OUT_DIR` via `build.rs`.

## System Verification
- Run: `cargo test`

## Roadmap

* Parallel listing shards for very large prefixes (`lexicographic_start/end`)
* Server-side `match_glob` for `glob`
* Concurrent multi-range `cat_file` / `get_file` for very large objects
* Streaming `find` / `walk` results

## License

Apache-2.0
