# gcsfs-sidecar Binary

## Overview
Binary executable (`gcsfs-sidecar`) providing an out-of-process gRPC daemon serving `gcsfs.v1.FileSystemService` (`Info`, `Read`, `Write`) backed by `gcs-rust-fs`. It exposes Google Cloud Storage operations over a Unix Domain Socket (UDS) or TCP endpoint, enabling non-Rust callers (such as Python `gcsfs`) or isolated microservices to interact with GCS with zero Python-native C-extension dependencies.
Binary executable (`gcsfs-sidecar`) providing an out-of-process gRPC daemon serving `gcsfs.v1.FileSystemService` (`Info`, `List`, `Read`, `Write`) backed by `gcs-rust-fs`. It exposes Google Cloud Storage operations over a Unix Domain Socket (UDS) or TCP endpoint, enabling non-Rust callers (such as Python `gcsfs`) or isolated microservices to interact with GCS with zero Python-native C-extension dependencies.

## Interface
- `[gcsfs_sidecar.rs:main]`: Binary entrypoint for the `gcsfs-sidecar` service daemon.
- `[gcsfs_sidecar.rs:--uds <PATH>]`: CLI option specifying a Unix Domain Socket path to bind (repeatable to serve multiple socket paths simultaneously; defaults to `/var/run/gcsfs/sidecar.sock` if neither `--uds` nor `--tcp` is provided).
- `[gcsfs_sidecar.rs:--mode <OCTAL>]`: CLI option specifying Unix file permissions (e.g. `0666`) for the created socket files.
- `[gcsfs_sidecar.rs:--tcp <HOST:PORT>]`: CLI option specifying a TCP socket address (`HOST:PORT`) to bind.
- `[gcsfs_sidecar.rs:--transport <http|grpc>]`: CLI option selecting the GCS backend transport protocol (`http` JSON API or `grpc` Bidi streaming).
- `[gcsfs_sidecar.rs:--endpoint <URL>]`: CLI option overriding the Cloud Storage service endpoint (useful for storage testbenches and emulators).
- `[gcsfs_sidecar.rs:--project <PROJECT>]`: CLI option setting the GCP project ID.
- `[gcsfs_sidecar.rs:--cache]`: CLI flag enabling in-memory directory and metadata caching via `CachedFs`.
- `[gcsfs_sidecar.rs:--readahead]`: CLI flag enabling Mode 1 asynchronous streaming readahead (`max_readahead = 256 MiB`, `io_size = 16 MiB`, `max_folio_size = 16 MiB`, `max_inflight = 16`, `max_inflight_bytes = 4 GiB`).
- `[gcsfs_sidecar.rs:--page-cache-mb <MB>]`: CLI option enabling Mode 2 shared `PageCache` with the specified MiB capacity.
- `[gcsfs_sidecar.rs:--http-pool-size <N>]`: CLI option configuring the number of pooled HTTP `Storage` clients on `GcsFsBuilder`.
- `[gcsfs_sidecar.rs:--help | -h]`: CLI flags printing command usage to standard error and exiting.

## Invariants
- `[gcsfs_sidecar.rs:SidecarArgs::parse]`: If neither `--uds` nor `--tcp` is provided, listener defaults to `--uds /var/run/gcsfs/sidecar.sock`.
- `[gcsfs_sidecar.rs:SidecarArgs::parse]`: Multiple `--uds <PATH>` arguments can be passed to listen on multiple distinct socket paths simultaneously.
- `[gcsfs_sidecar.rs:SidecarArgs::parse]`: When `--tcp` is provided, TCP listener takes precedence over `--uds`.
- `[gcsfs_sidecar.rs:SidecarArgs::parse]`: Transport defaults to the `GCS_RUST_FS_TRANSPORT` environment variable value if set and non-empty, otherwise defaulting to `Transport::Http`.
- `[gcsfs_sidecar.rs:SidecarArgs::parse]`: `--tcp` must parse to a valid `std::net::SocketAddr`, and `--transport` must be either `http` or `grpc`; invalid values yield `ErrorKind::InvalidConfig`.
- `[gcsfs_sidecar.rs:SidecarArgs::parse]`: `--tcp` must parse to a valid `std::net::SocketAddr`, `--page-cache-mb` and `--http-pool-size` must parse as `usize`, and `--transport` must be either `http` or `grpc`; invalid values yield `ErrorKind::InvalidConfig`.
- `[gcsfs_sidecar.rs:SidecarArgs::parse]`: Encountering any unrecognized command-line flag returns `ErrorKind::InvalidConfig`.
- `[gcsfs_sidecar.rs:main]`: `GcsFs` is wrapped in `CachedFs` whenever any of `--cache`, `--readahead`, or `--page-cache-mb` is supplied.
- `[gcsfs_sidecar.rs:main]`: Running server terminates cleanly upon receiving `SIGINT` (Ctrl+C) or `SIGTERM`.

## Side Effects
- `[gcsfs_sidecar.rs:main]`: Binds and listens on a local Unix Domain Socket or TCP address, serving incoming `FileSystemRpcService` requests.
- `[gcsfs_sidecar.rs:main]`: Deletes any existing socket file at the specified UDS path and creates missing parent directories before binding.
- `[gcsfs_sidecar.rs:main]`: Discovers Google Cloud credentials via Application Default Credentials (ADC) and initiates network connections to GCS endpoints.
- `[gcsfs_sidecar.rs:main]`: Writes diagnostic and startup messages to standard error (`eprintln!`).
- `[gcsfs_sidecar.rs:--help]`: Prints usage documentation to standard error and exits the process immediately with code 0.

## Verification
- Run: `cargo test`
