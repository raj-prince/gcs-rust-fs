# GCS Storage Backend Implementation

## Overview
Google Cloud Storage backend implementation for `gcs-rust-fs`, implementing the storage-agnostic `FileSystem` and `File` contracts using official Google Cloud Storage SDK clients (`google-cloud-storage`). It encapsulates all knowledge of GCS bucket kinds (flat, hierarchical namespace, and zonal rapid storage), generation-pinned reads, resumable and appendable uploads, HNS directory folders, and SDK transport protocols (`Transport::Grpc` bidi-streaming and `Transport::Http` JSON API).

## Interface
- `[mod.rs / fs.rs:GcsFs]`: Primary `FileSystem` implementation for Cloud Storage. Cheap to clone and holds connection pools.
- `[mod.rs / fs.rs:GcsFs::builder]`: Creates a new `GcsFsBuilder` instance.
- `[mod.rs / fs.rs:GcsFs::new]`: Asynchronously constructs a `GcsFs` configured from environment variables.
- `[mod.rs / fs.rs:GcsFs::transport]`: Returns the configured data transport (`Transport::Grpc` or `Transport::Http`).
- `[mod.rs / fs.rs:GcsFs::project]`: Returns the active GCP project ID string, if set.
- `[mod.rs / fs.rs:GcsFs::bucket_kind]`: Queries and caches the `BucketKind` of a bucket.
- `[mod.rs / fs.rs:GcsFs::create_bucket]`: Creates a bucket with custom placement and layout specifications (`BucketSpec`).
- `[mod.rs / fs.rs:GcsFsBuilder]`: Fluent builder for `GcsFs` with options: `transport`, `endpoint`, `grpc_subchannel_count`, `project`, `bucket_kind`, `finalize_on_close`, `from_env`, and `build`.
- `[mod.rs / fs.rs:GcsFsBuilder]`: Fluent builder for `GcsFs` with options: `transport`, `endpoint`, `grpc_subchannel_count`, `http_pool_size`, `project`, `bucket_kind`, `finalize_on_close`, `from_env`, and `build`.
- `[mod.rs / fs.rs:PROJECT_ENV_VARS]`: Environment variable fallback array: `["GCS_RUST_FS_PROJECT", "GOOGLE_CLOUD_PROJECT"]`.
- `[mod.rs / fs.rs:HTTP_POOL_SIZE_ENV_VAR]`: Environment variable (`"GCS_RUST_FS_HTTP_POOL_SIZE"`) configuring the number of pooled HTTP `Storage` client instances.
- `[mod.rs / fs.rs:DEFAULT_GET_CHUNK_SIZE]`: Default slice size in bytes for parallel downloads in `GcsFs::get_file` (128 MiB).
- `[mod.rs / fs.rs:DEFAULT_GET_CONCURRENCY]`: Default number of in-flight byte-range slice requests for `GcsFs::get_file` (24).
- `[mod.rs / file.rs:GcsFile]`: Implementation of the `File` trait for Cloud Storage objects. Supports generation-pinned sequential reads, ranged reads, resumable uploads, and appendable zonal writes.
- `[mod.rs / layout.rs:BucketKind]`: Classification of GCS bucket architectures: `Flat`, `Hierarchical`, and `Zonal`. Provides capability inspection methods (`is_hierarchical`, `has_versioning`, `supports_server_copy`, `supports_http`, `objects_appendable`).
- `[mod.rs / control.rs:BucketSpec]`: Configuration struct for bucket creation (`location`, `zone`, `hierarchical`, `storage_class`).
- `[mod.rs / backend.rs:Transport]`: Transport protocol enum (`Grpc`, `Http`), environment variable constant `ENV_VAR = "GCS_RUST_FS_TRANSPORT"`, and helpers `as_str()` and `from_env()`.

## Invariants
- `[layout.rs:BucketKind]`: Flat buckets emulate directories via `/` delimiters and placeholder objects; Hierarchical buckets use real folders managed via the Storage Control API; Zonal buckets are hierarchical and appendable, require gRPC, and do not support HTTP transport or server-side copy.
- `[fs.rs:GcsFsBuilder::build / from_env]`: `http_pool_size` must be at least `1` (`0` returns `ErrorKind::InvalidConfig`). `Backend::connect` defaults the pool size to `16` for `Transport::Http` and `1` for `Transport::Grpc`, striping `read_range_http` calls round-robin across `storage_pool`.
- `[path.rs:Loc::parse]`: Root paths (`""`, `"/"`, `"gs://"`) map to root; bucket names cannot be empty when a key or generation is present; generations can only be specified for objects; object keys cannot start with `/` or contain empty `//` segments.
- `[fs.rs:GcsFs::open]`: Read handles pin the object generation at open time; all subsequent reads see the same immutable snapshot even if the object is modified concurrently.
- `[fs.rs:GcsFs::open]`: `OpenMode::Append` returns `ErrorKind::Unsupported` unless the bucket is Zonal (`objects_appendable()`).
- `[fs.rs:GcsFs::rmdir]`: Removing the root path returns `ErrorKind::InvalidPath`. In Flat buckets, `rmdir` checks that no child objects or prefixes exist (except the directory's own placeholder); non-empty directories return `ErrorKind::DirectoryNotEmpty`.
- `[fs.rs:GcsFs::mkdir]`: Cannot create root `""`. Flat buckets only upload a `dir/` placeholder object when `opts.placeholder` is true; hierarchical buckets create HNS folders.
- `[fs.rs:GcsFs::copy_file]`: Server-side copy (`RewriteObject`) requires both source and destination buckets to support server copy (`supports_server_copy`), returning `ErrorKind::Unsupported` for zonal buckets; destination path cannot specify a generation suffix.
- `[fs.rs:GcsFs::mv]`: Atomic folder rename (`RenameFolder` LRO) is used only within the same hierarchical bucket without generation specifiers; otherwise falls back to per-object move. Moving directories requires `opts.recursive = true`.
- `[file.rs:GcsFile::ensure_open]`: Invoking operations on a closed `GcsFile` returns `ErrorKind::Closed`. Calling read operations on a write-mode handle or write operations on a read-mode handle returns `ErrorKind::Unsupported`.
- `[fs.rs:GcsFs::put_file]`: `opts.block_size` cannot be `Some(0)` (returns `ErrorKind::InvalidConfig`).
- `[fs.rs:GcsFs::get_file]`: `opts.chunk_size` and `opts.concurrency` cannot be `Some(0)` (returns `ErrorKind::InvalidConfig`). Empty local path returns `ErrorKind::InvalidPath`. Downloading onto an existing directory returns `ErrorKind::IsADirectory`. Writes target an ephemeral staging path (`<local>.gstmp.<pid>.<nanos>.<counter>`) atomically renamed to destination upon completion and removed on failure or cancellation.
- `[write.rs:write_error]`: When creating new objects (`if_generation_match = 0`), precondition failure from the service maps to `ErrorKind::AlreadyExists`.

## Side Effects
- `[backend.rs:Backend::connect]`: Connects to Google Cloud Storage endpoints via gRPC (`StorageControl`, `Storage`) and HTTP REST APIs using Application Default Credentials (ADC).
- `[backend.rs:Backend::bucket_kind]`: Queries `GetStorageLayout` and updates the internal thread-safe `kinds` cache.
- `[control.rs:Backend]`: Mutates remote GCS state via `CreateBucket`, `DeleteBucket`, `CreateFolder`, `DeleteFolder`, `DeleteObject`, `RewriteObject`, and `RenameFolder` (long-running operation).
- `[write.rs:Backend::upload_bytes / start_upload]`: Uploads bytes to GCS via unbuffered `WriteObject` or spawns a background Tokio task running a resumable / appendable stream over an mpsc channel.
- `[file.rs:GcsFile::drop]`: Automatically discards incomplete, unfinalized uploads if dropped without calling `close()` or `discard()`.

## Verification
- Run: `cargo test`
