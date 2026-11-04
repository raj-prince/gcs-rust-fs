# gcs-rust-fs Core Contracts & Architecture

## Overview
Pure-Rust, non-POSIX asynchronous filesystem abstractions and Google Cloud Storage primitives designed for high-performance object storage access. The crate decouples a storage-agnostic filesystem contract (`FileSystem` and `File` traits following Python `fsspec` semantics) from concrete storage engines and decorators. It provides comprehensive error classification, Pythonic slice-based ranged reading (`ByteRange`), in-memory directory caching (`CachedFs`), out-of-process gRPC communication (`rpc`), and native Cloud Storage operations (`gcs`).

## Interface
### Child Subsystems
- [bin](./bin/README.md): Sidecar daemon binary (`gcsfs-sidecar`) exposing `FileSystemRpcService` over Unix Domain Sockets or TCP.
- [gcs](./gcs/README.md): Google Cloud Storage backend implementation (`GcsFs`, `GcsFile`, `BucketKind`, `BucketSpec`, `Transport`).
- [rpc](./rpc/README.md): Out-of-process gRPC protocol definitions, server implementation (`FileSystemRpcService`), and client (`RemoteFs`, `RemoteFile`).

### Core Traits & Types
- `[filesystem.rs:FileSystem]`: Asynchronous trait modeling an object-store filesystem. Requires 6 primitives (`info`, `ls`, `open`, `rm_file`, `mkdir`, `rmdir`) and provides overridable defaults for derived operations (`find`, `cat_file`, `pipe_file`, `put_file`, `get_file`, `copy_file`, `move_file`, `invalidate_cache`, `exists`, `is_file`, `is_dir`, `size`, `walk`, `du`, `glob`, `cat`, `cat_ranges`, `rm`, `copy`, `mv`, `put`).
- `[file.rs:File]`: Asynchronous trait modeling an opened file handle (`read`, `read_range`, `write`, `flush`, `close`, `discard`, `seek`, `tell`, `size`, `stat`, `readable`, `writable`, `seekable`).
- `[cached.rs:CachedFs]`: Decorator struct wrapping any `FileSystem` to provide `fsspec`-compliant directory and metadata caching.
- `[cached.rs:CacheConfig]`: Configuration tuning for `CachedFs` (`ttl`, `max_dirs`, `negative`, `populate_from_find`).
- `[cached.rs:CacheStats]`: Real-time telemetry counters (`hits`, `misses`, `invalidations`).
- `[entry.rs:Entry]`: Structural representation of an object or directory entry (`path`, `kind`, `size`, `stat`).
- `[entry.rs:EntryKind]`: Enum distinguishing filesystem nodes (`File`, `Directory`).
- `[entry.rs:WalkEntry]`: Container representing a directory traversal step (`dir`, `dirs`, `files`).
- `[entry.rs:DiskUsage]`: Structural representation of disk usage aggregations (`total`, `sizes`).
- `[error.rs:Error]`: Crate-wide error representation bundling `ErrorKind`, descriptive message, and underlying source errors.
- `[error.rs:ErrorKind]`: Enumeration of distinct failure modes (`NotFound`, `PermissionDenied`, `IsADirectory`, `NotADirectory`, `AlreadyExists`, `DirectoryNotEmpty`, `InvalidPath`, `InvalidRange`, `InvalidConfig`, `Unsupported`, `PreconditionFailed`, `Closed`, etc.).
- `[error.rs:classify_storage_error]`: Helper mapping SDK errors into canonical `ErrorKind` variants.
- `[options.rs:OpenOptions]`: Struct configuring handle creation mode (`Read`, `Write`, `Append`, `CreateNew`), block sizes, and object metadata.
- `[options.rs:ReadOptions]`: Non-exhaustive configuration struct for file read calls.
- `[options.rs:WriteOptions]`: Struct configuring whole-file uploads (`Overwrite` vs `Create`, MIME content type, custom metadata).
- `[options.rs:CopyOptions]`: Configuration for copy/move operations (`recursive`, `maxdepth`, `concurrency`, `on_error`).
- `[options.rs:RmOptions]`: Configuration for deletion operations (`recursive`, `maxdepth`, `concurrency`).
- `[options.rs:ListOptions]`: Configuration for directory listings (`versions`, `refresh`).
- `[options.rs:FindOptions]`: Configuration for recursive discovery (`maxdepth`, `withdirs`, `versions`).
- `[options.rs:BulkOptions]`: Configuration for batch concurrent reads (`concurrency`, `on_error`).
- `[range.rs:ByteRange]`: Half-open byte range `[start, end)` implementing Python-slice semantics and resolving against concrete object lengths.
- `[stat.rs:ObjectStat]`: Detailed metadata container representing Cloud Storage object properties (generation, etag, crc32c, timestamps, custom metadata).
- `[lib.rs:async_trait]`: Re-exported procedural macro required for `FileSystem` and `File` trait implementations.

## Invariants
- `[filesystem.rs:FileSystem]`: Directories are emulated within flat object stores: a directory is a bucket, an empty `dir/` placeholder object, or an implied prefix with at least one object beneath it.
- `[filesystem.rs:FileSystem]`: File operations (`cat_file`, `rm_file`, read open) on directories return `ErrorKind::IsADirectory`; directory operations (`rmdir`) on files return `ErrorKind::NotADirectory`.
- `[filesystem.rs:FileSystem]`: Calling `rmdir` on a directory that contains child objects or subdirectories returns `ErrorKind::DirectoryNotEmpty`.
- `[filesystem.rs:FileSystem]`: Invoking `find` with `maxdepth == Some(0)` returns `ErrorKind::InvalidRange`.
- `[file.rs:File]`: Reading at or past the end of a file returns an empty `Bytes` buffer, never an error (Python-slice semantics).
- `[file.rs:File]`: Read handles pin the object generation at open time; subsequent modifications or deletions do not alter bytes returned by that handle.
- `[file.rs:File]`: Calling read methods on write handles or write methods on read handles returns `ErrorKind::Unsupported`.
- `[file.rs:File]`: Regular object writes are atomic and published remotely only upon successful execution of `close()`. Dropping or discarding an unclosed handle cancels the pending upload without modifying remote state.
- `[file.rs:File]`: `close()` and `discard()` are idempotent; subsequent I/O on closed handles returns `ErrorKind::Closed`.
- `[range.rs:ByteRange]`: Negative indexing counts back from the end of the object; resolving negative bounds without object size returns `ErrorKind::InvalidRange` (except for `tail(n)` without end).
- `[cached.rs:CachedFs]`: File data and byte reads (`cat_file`, `open` read, `get_file`) are never cached; only directory listings and entry metadata are cached.
- `[cached.rs:CachedFs]`: Listings requesting object versions (`ListOptions::versions == true`) or paths containing `#<generation>` suffixes bypass the cache.

## Side Effects
- `[file.rs:File]`: `File::write` and `File::close` transfer data and commit objects to remote storage.
- `[filesystem.rs:FileSystem]`: Mutating methods (`rm_file`, `mkdir`, `rmdir`, `pipe_file`, `put_file`, `copy_file`, `move_file`, `rm`, `copy`, `mv`, `put`) create, mutate, or delete remote storage objects and local staging files.
- `[cached.rs:CachedFs]`: Traversal methods (`ls`, `info`, `find`) update in-memory caches and modify telemetry counters; mutating operations invalidate cached directories, parent chains, or entire subtrees.

## Verification
- Run: `cargo test`
