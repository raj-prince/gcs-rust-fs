# gRPC Sidecar RPC Protocol & Client/Server

## Overview
Implementation of the gRPC sidecar service (`gcsfs.v1.FileSystemService`) and client bindings for `gcs-rust-fs`. It enables decoupled out-of-process filesystem operations across Unix Domain Sockets (`unix://`) or TCP (`http://`). The server exposes `Info`, streaming `Read`, and streaming `Write` operations for any underlying `FileSystem` implementation, while `RemoteFs` and `RemoteFile` provide transparent client implementations satisfying the crate's `FileSystem` and `File` contracts. Error classifications (`ErrorKind`) are preserved across the wire via custom gRPC metadata headers.
Implementation of the gRPC sidecar service (`gcsfs.v1.FileSystemService`) and client bindings for `gcs-rust-fs`. It enables decoupled out-of-process filesystem operations across Unix Domain Sockets (`unix://`) or TCP (`http://`). The server exposes `Info`, `List`, streaming `Read`, and streaming `Write` operations for any underlying `FileSystem` implementation, while `RemoteFs` and `RemoteFile` provide transparent client implementations satisfying the crate's `FileSystem` and `File` contracts. Error classifications (`ErrorKind`) are preserved across the wire via custom gRPC metadata headers.

## Interface
- `[client.rs / mod.rs:RemoteFs]`: `FileSystem` client implementation communicating with a remote `FileSystemService` over UDS or TCP.
- `[client.rs:RemoteFs::from_channel]`: Wraps an existing Tonic transport `Channel`.
- `[client.rs:RemoteFs::connect_uds]`: Connects to a sidecar gRPC server listening on a Unix Domain Socket path.
- `[client.rs:RemoteFs::from_channels]`: Wraps multiple Tonic transport `Channel`s for round-robin load balancing.
- `[client.rs:RemoteFs::connect_uds]`: Connects to a sidecar gRPC server listening on a Unix Domain Socket path using a default pool of `16` parallel `UnixStream` HTTP/2 connections.
- `[client.rs:RemoteFs::connect_uds_with_pool_size]`: Connects to a sidecar gRPC server listening on a Unix Domain Socket path with a caller-specified number of pooled `UnixStream` HTTP/2 connections.
- `[client.rs:RemoteFs::connect_tcp]`: Connects to a sidecar gRPC server listening on a TCP endpoint.
- `[client.rs:RemoteFs::ls]`: Queries directory contents from the sidecar via the `List` RPC.
- `[client.rs:RemoteFs::write_bytes]`: Convenience streaming `Write` RPC executing from a single contiguous byte buffer.
- `[client.rs / mod.rs:RemoteFile]`: Client-side `File` implementation representing a remote file opened for reading or streaming writing.
- `[server.rs / mod.rs:FileSystemRpcService]`: Tonic gRPC service handler wrapping any `Arc<dyn FileSystem>`.
- `[server.rs:FileSystemRpcService::new]`: Wraps an `Arc<dyn FileSystem>` in a service handler.
- `[server.rs:FileSystemRpcService::into_server]`: Converts the handler into a Tonic `FileSystemServiceServer`.
- `[server.rs:FileSystemRpcService::serve_uds_with_shutdown]`: Binds and serves incoming gRPC requests on a Unix Domain Socket until shutdown.
- `[server.rs:FileSystemRpcService::serve_uds_with_mode_and_shutdown]`: Binds and serves incoming gRPC requests on a Unix Domain Socket with configurable file permissions until shutdown.
- `[server.rs:FileSystemRpcService::serve_multiple_uds_with_mode_and_shutdown]`: Binds and serves incoming gRPC requests simultaneously across multiple Unix Domain Sockets until shutdown.
- `[server.rs:FileSystemRpcService::serve_tcp_with_shutdown]`: Binds and serves incoming gRPC requests on a TCP `SocketAddr` until shutdown.
- `[server.rs:DEFAULT_READ_CHUNK_SIZE]`: Default read stream chunk size (`1 MiB`).
- `[server.rs:MAX_READ_CHUNK_SIZE]`: Maximum streaming chunk size (`3.5 MiB`).
- `[server.rs:MAX_READ_CHUNK_SIZE]`: Maximum streaming chunk size (`8 MiB`).
- `[server.rs:MAX_GRPC_MESSAGE_SIZE]`: Maximum gRPC message size (`16 MiB`) accepted and emitted by the sidecar.
- `[convert.rs:ERROR_KIND_HEADER]`: gRPC metadata header (`x-gcsfs-error-kind`) used for lossless round-tripping of `ErrorKind`.
- `[convert.rs:error_to_status]`: Converts an internal `Error` into a `tonic::Status` with `ERROR_KIND_HEADER`.
- `[convert.rs:status_to_error]`: Converts a `tonic::Status` back into an internal `Error`, extracting `ErrorKind` from header or falling back to gRPC `Code`.
- `[convert.rs:parse_error_kind]`: Parses an `ErrorKind` from its string identifier.
- `[mod.rs:proto]`: Re-exports generated Protobuf messages and Tonic client/server code (`gcsfs.v1`).

## Invariants
- `[server.rs:FileSystemRpcService::read]`: Read chunk size is clamped between 1 byte and `MAX_READ_CHUNK_SIZE` (3.5 MiB), defaulting to `DEFAULT_READ_CHUNK_SIZE` (1 MiB).
- `[server.rs:FileSystemRpcService::read]`: Read chunk size is clamped between 1 byte and `MAX_READ_CHUNK_SIZE` (`8 MiB`), defaulting to `DEFAULT_READ_CHUNK_SIZE` (`1 MiB`).
- `[server.rs:FileSystemRpcService::read]`: Bounded small reads (`span <= 4 MiB` and `span <= chunk_size`) execute via `self.fs.cat_file` directly without an extra `open()` RPC; `chunk_size < span <= 4 MiB` opens with `ReadPattern::Random`; `4 MiB < span < 16 MiB` opens with `ReadPattern::Auto`; `span >= 16 MiB` or unbounded ranges open with `ReadPattern::Sequential` (`block_size = 8 MiB`).
- `[server.rs:FileSystemRpcService::write]`: The first message in a streaming `Write` request must contain `Payload::Spec`; missing spec or duplicate spec mid-stream triggers `file.discard()` and returns `Status::invalid_argument`.
- `[server.rs:FileSystemRpcService::write]`: Client stream errors trigger `file.discard()` to abort partial uploads before returning error status.
- `[client.rs:RemoteFs::open]`: Opening a directory with `OpenMode::Read` returns `ErrorKind::IsADirectory`. In read mode, objects with generation > 0 pin `{path}#{generation}` for consistent snapshot reads.
- `[client.rs:RemoteFs::open]`: `OpenMode::Append` returns `ErrorKind::Unsupported`.
- `[client.rs:RemoteFs unsupported methods]`: `rm_file`, `mkdir`, and `rmdir` return `ErrorKind::Unsupported` in remote mode (`info`, `ls`, `cat_file`, `open`, and `pipe_file` are supported).
- `[client.rs:RemoteFile]`: Calling read on a write handle, write on a read handle, or any operation on a closed handle returns `ErrorKind::Closed` or `ErrorKind::Unsupported`. Seeking on write mode files returns `ErrorKind::Unsupported`.
- `[client.rs:RemoteFile]`: Sequential reads (`pos == 0 && !seeked` or `prev_read_end == Some(pos)`) maintain a persistent server-streaming `Read` RPC from `pos..stat.size`, whereas the first read after a non-zero `seek()` issues a bounded single-range read (`[pos, pos + want)`) for Rule 7 isolation. Calling read on a write handle, write on a read handle, or any operation on a closed handle returns `ErrorKind::Closed` or `ErrorKind::Unsupported`. Seeking on write mode files returns `ErrorKind::Unsupported`.
- `[convert.rs:ERROR_KIND_HEADER]`: Lossless serialization of `ErrorKind` ensures client errors accurately reflect original server errors despite coarse gRPC status codes.

## Side Effects
- `[server.rs:FileSystemRpcService::serve_uds_with_shutdown]`: Cleans up existing socket files, creates parent directories, binds a Unix Domain Socket listener, and spawns the Tonic HTTP/2 server.
- `[server.rs:FileSystemRpcService::serve_tcp_with_shutdown]`: Binds a TCP listener and serves incoming gRPC requests.
- `[server.rs:FileSystemRpcService::write]`: Modifies backend files by streaming chunks to `file.write()` and completing with `file.close()` or `file.discard()`.
- `[client.rs:RemoteFs::connect_uds / connect_tcp]`: Establishes network socket connections to the sidecar server.
- `[client.rs:RemoteFile::drop]`: Aborts active background write stream tasks if the file was not explicitly closed.

## Verification
- Run: `cargo test`
