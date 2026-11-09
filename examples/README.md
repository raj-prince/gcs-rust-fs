# examples

## Overview
Command-line example demonstrating integration with the `gcs-rust-fs` crate. It provides an interactive CLI tool (`examples/gcs.rs`) for testing and interacting with Google Cloud Storage buckets (Flat, Hierarchical Namespace, and Zonal) using the `FileSystem` abstraction.
Command-line utilities and benchmark harnesses demonstrating integration with the `gcs-rust-fs` crate:
- `examples/gcs.rs`: Interactive CLI tool for testing and interacting with Google Cloud Storage buckets (Flat, Hierarchical Namespace, and Zonal) directly or via a UDS sidecar using the `FileSystem` abstraction.
- `examples/bench_ipc.rs`: Synthetic microbenchmark comparing in-process zero-copy memory transfer against out-of-process gRPC over Unix Domain Sockets (`FileSystemRpcService` + `RemoteFs`).
- `examples/bench_live_readahead.rs`: Live 4-part `20 GiB` Native Rust benchmark suite (`Transport::Http`, `ReadPattern::Auto`) measuring uncached `info()`, `1 MiB` random reads, `20 GiB` sequential reads (`1T`, `24T`, `48T`), and `200 MiB` writes.
- `examples/bench_live_readahead.rs`: Live 5-part `20 GiB` Native Rust benchmark suite (`Transport::Http`, `ReadPattern::Auto`) measuring uncached `info()`, `1 MiB` random reads, `20 GiB` sequential reads (`1T`, `24T`, `48T`), `200 MiB` writes (`open(write)`, `pipe_file`, and `put_file` from `/dev/shm`), and `20 GiB` parallel sliced download (`FileSystem::get_file` to `/dev/shm`).
- `examples/bench_uds_readahead.rs`: Live 4-part `20 GiB` UDS benchmark suite (`RemoteFs` over `unix://` to `FileSystemRpcService` wrapping `CachedFs<GcsFs>`) measuring uncached `info()`, `1 MiB` random reads, `20 GiB` sequential reads (`1T`, `24T`, `48T` with configurable `UnixStream` pooling via `--uds-pool-size`), and `200 MiB` writes.

## Interface
### `examples/gcs.rs`
- `[gcs.rs:main]`: CLI entry point returning `std::process::ExitCode`.
- `[gcs.rs:run]`: Async execution dispatcher mapping CLI commands (`info`, `ls`, `find`, `walk`, `du`, `cat`, `get`, `put`, `pipe`, `cp`, `mv`, `rm`, `mkdir`, `rmdir`, `kind`) to `FileSystem` and `GcsFs` operations.
- `[gcs.rs:parse_args]`: Parses command line options, flags (`--start`, `--end`, `--maxdepth`, `--transport`, `--uds`, `-r`, `--recursive`, `--withdirs`, `--versions`, `-p`, `--placeholder`), and positional parameters into `Args`.
- `[gcs.rs:Args]`: Structure encapsulating CLI parameters (`command`, `positional`, `flags`, `start`, `end`, `maxdepth`, `transport`, `uds`).
- `[gcs.rs:print_entry]`: Formats and displays an `Entry` (kind, size, path) to standard output.
- `[gcs.rs:run:info]`: `gcs info <path>` — Displays object metadata and debug stat representation.
- `[gcs.rs:run:ls]`: `gcs ls <path> [--versions]` — Lists directory contents, optionally with version markers.
- `[gcs.rs:run:find]`: `gcs find <path> [--withdirs] [--maxdepth N] [--versions]` — Flat listing with optional directory inclusion and max depth traversal.
- `[gcs.rs:run:walk]`: `gcs walk <path> [--maxdepth N]` — Hierarchical directory walk separating directories and files.
- `[gcs.rs:run:du]`: `gcs du <path>` — Computes recursive object size usage per item and overall total.
- `[gcs.rs:run:cat]`: `gcs cat <path> [--start N] [--end N]` — Streams a Python-slice byte range to standard output.
- `[gcs.rs:run:get]`: `gcs get <path> <local path>` — Downloads a remote GCS object to the local filesystem.
- `[gcs.rs:run:put]`: `gcs put <local path> <path>` — Uploads a local file to a remote GCS object.
- `[gcs.rs:run:pipe]`: `gcs pipe <path> <text>` — Writes inline text to a remote GCS object.
- `[gcs.rs:run:cp]`: `gcs cp <source> <destination> [-r|--recursive]` — Copies files or prefixes across GCS paths.
- `[gcs.rs:run:mv]`: `gcs mv <source> <destination> [-r|--recursive]` — Moves/renames files or prefixes across GCS paths.
- `[gcs.rs:run:rm]`: `gcs rm <path> [-r|--recursive]` — Deletes files or prefixes across GCS paths.
- `[gcs.rs:run:mkdir]`: `gcs mkdir <path> [-p] [--placeholder]` — Creates folders or placeholder directory objects.
- `[gcs.rs:run:rmdir]`: `gcs rmdir <path>` — Deletes an empty directory.
- `[gcs.rs:run:kind]`: `gcs kind <bucket>` — Resolves and displays the storage layout kind (`Flat`, `Hierarchical`, `Zonal`).

### `examples/bench_ipc.rs`
- `[bench_ipc.rs:main]`: Runs in-process `BenchFs` vs UDS `RemoteFs` latency and throughput benchmarks across single-socket and multi-socket topologies.

### `examples/bench_live_readahead.rs`
- `[bench_live_readahead.rs:main]`: Parses `--bench-20gb`, `--only <all|info|random|seq|seq-1t|seq-24t|seq-48t|write>`, `--read-size-mb <MIB>`, `--io-size-mb <MIB>`, `--json-out <PATH>`, and optional `[GS_PATH]` (default `gs://princer-flat/bench/bench_20GB.bin`).
- `[bench_live_readahead.rs:run_20gb_benchmark_suite]`: Executes the 4-part live Native Rust benchmark suite using `CachedFs<GcsFs>` (`Transport::Http`, `ReadPattern::Auto`) and optionally writes a JSON summary report.
- `[bench_live_readahead.rs:main]`: Parses `--bench-20gb`, `--only <all|info|random|seq|seq-1t|seq-24t|seq-48t|write|put|get>`, `--read-size-mb <MIB>`, `--io-size-mb <MIB>`, `--get-chunk-mb <MIB>`, `--get-concurrency <N>`, `--json-out <PATH>`, and optional `[GS_PATH]` (default `gs://princer-flat/bench/bench_20GB.bin`).
- `[bench_live_readahead.rs:run_20gb_benchmark_suite]`: Executes the 5-part live Native Rust benchmark suite using `CachedFs<GcsFs>` and `GcsFs` (`Transport::Http`, `ReadPattern::Auto`) and optionally writes a JSON summary report.

### `examples/bench_uds_readahead.rs`
- `[bench_uds_readahead.rs:main]`: Parses `--bench-20gb`, `--only <all|info|random|seq|seq-1t|seq-24t|seq-48t|write>`, `--read-size-mb <MIB>`, `--io-size-mb <MIB>`, `--uds-pool-size <N>` (default `48`), `--json-out <PATH>`, and optional `[GS_PATH]` (default `gs://princer-flat/bench/bench_20GB.bin`).
- `[bench_uds_readahead.rs:run_20gb_uds_benchmark_suite]`: Spawns an in-process UDS sidecar (`FileSystemRpcService`) and executes the 4-part benchmark suite through `RemoteFs::connect_uds_with_pool_size`.

## Invariants
- `[gcs.rs:parse_args]`: A command keyword must be supplied as the first positional argument; omitting it fails with `"missing command"` and returns exit code 2.
- `[gcs.rs:parse_args]`: Flags `--start`, `--end`, `--maxdepth`, and `--transport` require immediate value arguments or fail with `"<flag> needs a value"`.
- `[gcs.rs:parse_args]`: `--start` and `--end` must parse as signed 64-bit integers (`i64`); `--maxdepth` must parse as `usize`; `--transport` must parse into `Transport` enum (`grpc` or `http`).
- `[gcs.rs:Args::arg]`: Positional arguments are validated by index; missing arguments trigger runtime error `"missing argument: <what>"`.
- `[gcs.rs:run]`: Environment credentials must be available for `GcsFs::builder().from_env()` initialization.
- `[gcs.rs:run]`: Any unrecognized command string yields an `"unknown command <other>"` error.

## Side Effects
- `[gcs.rs:run]`: Initiates network I/O calls over gRPC or HTTP to Google Cloud Storage endpoints for querying metadata and transferring data.
- `[gcs.rs:run:get]`: Writes downloaded object content directly to the local filesystem at the destination path.
- `[gcs.rs:run:put]`: Reads file data from the local filesystem at the source path.
- `[gcs.rs:run:put|pipe|cp|mv|rm|mkdir|rmdir]`: Modifies remote Google Cloud Storage state by creating, updating, copying, moving, or deleting bucket objects and folder markers.
- `[gcs.rs:main|print_entry|cat]`: Writes output to standard output (`stdout`) and errors or usage diagnostics to standard error (`stderr`).
- `[gcs.rs:main]`: Exits process with code 0 on success, code 1 on runtime error, or code 2 on CLI argument parsing failure.

## Verification
- Run: `cargo test --example gcs`
