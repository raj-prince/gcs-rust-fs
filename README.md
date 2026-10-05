# gcs-rust-fs

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

## Features

Every `fsspec` operation that `gcsfs` needs, behind the `FileSystem` trait:

| Group | Methods | Implementation on GCS |
|-------|---------|-----------------------|
| metadata | `info`, `exists`, `is_file`, `is_dir`, `size` | `GetObject` **and** a directory probe issued concurrently (like gcsfs); `GetBucket` / `GetFolder` where applicable |
| listing | `ls`, `find`, `walk`, `du`, `glob` | one `ListObjects` page stream per call (`find` is a single flat listing, not a walk); HNS folders via `ListFolders` |
| reading | `cat_file`, `cat`, `cat_ranges`, `open(rb)` → `read`/`seek`/`read_range`, `get_file` | gRPC `BidiReadObject` or JSON API ([Transports](#transports)); read handles pin one generation |
| writing | `pipe_file`, `put_file`, `put`, `open(wb/xb)` → `write`/`flush`/`close` | resumable `WriteObject` streamed from a spawned task; zonal buckets use appendable objects |
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

## Caching

`CachedFs` wraps any `FileSystem` (it is one itself, so `Arc<dyn FileSystem>`
works either way) and gives `ls` and `info` the `fsspec` directory-cache
behaviour that `gcsfs` users rely on, with both served from **one** store: a
listing warms `info` for every child, and individual `info` results are kept
without ever being mistaken for a complete listing.

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

## Development

```bash
cargo test                                                # unit + in-memory FS + doctests; live tests self-skip
GCS_RUST_FS_TEST_OBJECT=gs://my-bucket/file.bin cargo test --test live          # read-only, real bucket
GCS_RUST_FS_TEST_BUCKETS=flat=b1,hns=b2,zonal=b3 cargo test --test live          # read/write, per bucket kind
```

Building the example binary, the CI lint gate and the repo layout are in
[`docs/dev_guide.md`](docs/dev_guide.md).

## Roadmap

* Parallel listing shards for very large prefixes (`lexicographic_start/end`)
* Server-side `match_glob` for `glob`
* Concurrent multi-range `cat_file` / `get_file` for very large objects
* Streaming `find` / `walk` results

## License

Apache-2.0
