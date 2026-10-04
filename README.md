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
│  implementation  src/gcs/: GcsFs, GcsFile, Transport {Grpc, Http}    │
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
implementation at runtime; `tests/memory_fs.rs` is an in-memory implementation
that exercises every derived operation without network. The full semantics
(directory emulation, fsspec copy rules, error classes, open decisions) are in
[`docs/filesystem_api_design.md`](docs/filesystem_api_design.md).

> **Status:** the contract and derived operations are complete. `GcsFs` /
> `GcsFile` currently implement the read-only subset below as inherent
> methods; wiring them to the traits (and adding writes, listing, delete,
> copy) is the next step.

## Features

| Operation | API | Wire protocol |
|-----------|-----|---------------|
| Object metadata | `GcsFs::stat` | gRPC `GetObject` (`StorageControl`) |
| Ranged read into memory | `GcsFs::cat_file` | gRPC `BidiReadObject` **or** JSON API over HTTP (see [Transports](#transports)) |
| Repeated ranged reads on one generation | `GcsFs::open` → `GcsFile::read_range` | same as above, multiplexed over one bidi stream on gRPC |

Design points:

* **Python-slice byte ranges.** `ByteRange::new(start, end)` follows the exact
  `fsspec.cat_file(start, end)` contract: exclusive `end`, optional bounds,
  negative bounds count from the end. Ranges the API supports natively
  (absolute offsets, `head(n)`, `tail(n)`) never cost an extra request; only
  mixed negative bounds trigger one `stat`. Empty ranges short-circuit.
* **Typed errors.** Every `Error` has an `ErrorKind` (`NotFound`,
  `PermissionDenied`, `OutOfRange`, `InvalidPath`, ...) classified from the
  gRPC status *or* HTTP status, so bridges never string-match messages.
* **gcsfs path conventions.** `GcsPath::parse` accepts `gs://b/o`, `gcs://b/o`,
  `b/o`, `/b/o`, and the `b/o#<generation>` suffix exactly like
  `GCSFileSystem.split_path`.
* **Snapshot semantics.** `GcsFile` pins the generation observed at open time,
  so concurrent overwrites never produce torn reads.
* **No global state.** `GcsFs` wraps the SDK's pooled clients and is cheap to
  clone; build one instance and share it (the bridge keeps exactly one).

## Usage

```toml
[dependencies]
gcs-rust-fs = { git = "https://github.com/raj-prince/gcs-rust-fs" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use gcs_rust_fs::{ByteRange, GcsFs, GcsPath};

#[tokio::main]
async fn main() -> gcs_rust_fs::Result<()> {
    let fs = GcsFs::new().await?; // Application Default Credentials
    let path: GcsPath = "gs://my-bucket/checkpoint.pt".parse()?;

    let stat = fs.stat(&path).await?;
    println!("{} bytes, generation {}, updated {:?}", stat.size, stat.generation, stat.updated);

    let header = fs.cat_file(&path, ByteRange::head(1 << 20)).await?;   // first MiB
    let footer = fs.cat_file(&path, ByteRange::tail(64)).await?;        // last 64 bytes
    let window = fs.cat_file(&path, ByteRange::span(4096, 8192)).await?; // [4096, 8192)

    // Repeated reads against one pinned generation:
    let file = fs.open(&path).await?;
    let chunk = file.read_range(ByteRange::head(4096)).await?;
    assert!(chunk.len() <= 4096);
    Ok(())
}
```

## Transports

Metadata always travels over gRPC. For object **data** choose with
`GcsFsBuilder::transport(..)` or the `GCS_RUST_FS_TRANSPORT` environment
variable:

| `Transport` | Mechanism | Notes |
|-------------|-----------|-------|
| `Grpc` (default) | `Storage::open_object` → `BidiReadObject` | Fastest path; one RPC per `cat_file` (open + read are fused). **Only enabled for some projects/buckets** — contact your account team. |
| `Http` | `Storage::read_object` → JSON API with `Range` headers | Universally available fallback. |

Both return byte-identical results and classify errors identically.

## Error mapping for bridges

| `ErrorKind` | Source | Suggested Python exception |
|-------------|--------|----------------------------|
| `NotFound` | 404 / `NOT_FOUND` | `FileNotFoundError` |
| `PermissionDenied` | 403 / `PERMISSION_DENIED` | `PermissionError` |
| `Unauthenticated` | 401 / `UNAUTHENTICATED` | `PermissionError` |
| `OutOfRange` | 416 / `OUT_OF_RANGE` | `RuntimeError("... not satisfiable")` (what `GCSFile._fetch_range` expects) or return `b""` |
| `InvalidPath`, `InvalidRange`, `InvalidConfig` | client-side validation | `ValueError` |
| `Timeout` | 408 / 504 / `DEADLINE_EXCEEDED` | `TimeoutError` |
| `ClientInit`, `Other` | everything else | `OSError` |

`Error` also converts into `std::io::Error` with the analogous `io::ErrorKind`.

## Examples

```bash
cargo run --example stat -- gs://my-bucket/path/to/object
cargo run --example cat  -- gs://my-bucket/path/to/object --start 0 --end 20
cargo run --example cat  -- gs://my-bucket/path/to/object --start -100 --transport http
```

## Development

```bash
cargo test                                                # unit + in-memory FS + doctests; live tests self-skip
GCS_RUST_FS_TEST_OBJECT=gs://my-bucket/file.bin cargo test --test live   # against a real bucket (needs ADC)
```

Building the example binaries, the CI lint gate and the repo layout are in
[`docs/dev_guide.md`](docs/dev_guide.md).

## Roadmap

* `ls` / prefix listing with directory emulation (`GcsFs::list`)
* Concurrent multi-range `cat_file` for very large reads
* Writes (`put`, resumable / appendable uploads)

## License

Apache-2.0
