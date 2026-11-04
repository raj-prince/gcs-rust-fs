//! gRPC sidecar service (`gcsfs.v1.FileSystemService`) and [`RemoteFs`] client.
//!
//! Exposes [`FileSystem::info`](crate::FileSystem::info), streaming reads
//! ([`FileSystem::cat_file`](crate::FileSystem::cat_file) /
//! [`FileSystem::open`](crate::FileSystem::open)), and streaming writes
//! ([`FileSystem::pipe_file`](crate::FileSystem::pipe_file) /
//! [`FileSystem::open`](crate::FileSystem::open)) over a Unix Domain Socket or
//! TCP endpoint.

pub mod client;
pub mod convert;
pub mod server;

/// Generated `gcsfs.v1` Protobuf messages and Tonic service/client stubs.
#[allow(missing_docs, clippy::all)]
pub mod proto {
    tonic::include_proto!("gcsfs.v1");
}

pub use client::{RemoteFile, RemoteFs};
pub use convert::{error_to_status, status_to_error, ERROR_KIND_HEADER};
pub use server::{FileSystemRpcService, DEFAULT_READ_CHUNK_SIZE, MAX_READ_CHUNK_SIZE};
