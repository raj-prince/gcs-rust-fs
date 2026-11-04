//! Conversions between `gcs-rust-fs` domain types and the `gcsfs.v1` Protobuf
//! / `tonic::Status` wire representation.

use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

use crate::entry::{Entry, EntryKind};
use crate::error::{Error, ErrorKind, Result};
use crate::options::{WriteMode, WriteOptions};
use crate::range::ByteRange;
use crate::rpc::proto;
use crate::stat::ObjectStat;

/// gRPC response metadata header used to preserve the exact [`ErrorKind`] across
/// the wire when multiple `ErrorKind` variants share a canonical gRPC [`Code`]
/// (such as `FAILED_PRECONDITION` or `INVALID_ARGUMENT`).
pub const ERROR_KIND_HEADER: &str = "x-gcsfs-error-kind";

impl From<ObjectStat> for proto::ObjectStat {
    fn from(s: ObjectStat) -> Self {
        Self {
            bucket: s.bucket,
            name: s.name,
            size: s.size,
            generation: s.generation,
            metageneration: s.metageneration,
            content_type: s.content_type,
            content_encoding: s.content_encoding,
            cache_control: s.cache_control,
            content_disposition: s.content_disposition,
            content_language: s.content_language,
            storage_class: s.storage_class,
            etag: s.etag,
            crc32c: s.crc32c,
            md5_hash: s.md5_hash,
            kms_key: s.kms_key,
            component_count: s.component_count,
            time_created: s.time_created,
            updated: s.updated,
            custom_time: s.custom_time,
            metadata: s.metadata,
        }
    }
}

impl From<proto::ObjectStat> for ObjectStat {
    fn from(p: proto::ObjectStat) -> Self {
        Self {
            bucket: p.bucket,
            name: p.name,
            size: p.size,
            generation: p.generation,
            metageneration: p.metageneration,
            content_type: p.content_type,
            content_encoding: p.content_encoding,
            cache_control: p.cache_control,
            content_disposition: p.content_disposition,
            content_language: p.content_language,
            storage_class: p.storage_class,
            etag: p.etag,
            crc32c: p.crc32c,
            md5_hash: p.md5_hash,
            kms_key: p.kms_key,
            component_count: p.component_count,
            time_created: p.time_created,
            updated: p.updated,
            custom_time: p.custom_time,
            metadata: p.metadata,
        }
    }
}

impl From<EntryKind> for proto::EntryKind {
    fn from(k: EntryKind) -> Self {
        match k {
            EntryKind::File => proto::EntryKind::File,
            EntryKind::Directory => proto::EntryKind::Directory,
        }
    }
}

impl From<Entry> for proto::Entry {
    fn from(e: Entry) -> Self {
        Self {
            path: e.path,
            kind: proto::EntryKind::from(e.kind) as i32,
            size: e.size,
            stat: e.stat.map(proto::ObjectStat::from),
        }
    }
}

impl TryFrom<proto::Entry> for Entry {
    type Error = Error;

    fn try_from(p: proto::Entry) -> Result<Self> {
        let kind = match proto::EntryKind::try_from(p.kind) {
            Ok(proto::EntryKind::File) => EntryKind::File,
            Ok(proto::EntryKind::Directory) => EntryKind::Directory,
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("unknown proto EntryKind value: {}", p.kind),
                ))
            }
        };
        Ok(Self {
            path: p.path,
            kind,
            size: p.size,
            stat: p.stat.map(ObjectStat::from),
        })
    }
}

impl From<ByteRange> for proto::ByteRange {
    fn from(r: ByteRange) -> Self {
        Self {
            start: r.start(),
            end: r.end(),
        }
    }
}

impl From<proto::ByteRange> for ByteRange {
    fn from(p: proto::ByteRange) -> Self {
        Self::new(p.start, p.end)
    }
}

impl From<WriteMode> for proto::WriteMode {
    fn from(m: WriteMode) -> Self {
        match m {
            WriteMode::Overwrite => proto::WriteMode::Overwrite,
            WriteMode::Create => proto::WriteMode::Create,
        }
    }
}

impl From<proto::WriteMode> for WriteMode {
    fn from(p: proto::WriteMode) -> Self {
        match p {
            proto::WriteMode::Unspecified | proto::WriteMode::Overwrite => WriteMode::Overwrite,
            proto::WriteMode::Create => WriteMode::Create,
        }
    }
}

impl From<&proto::WriteSpec> for WriteOptions {
    fn from(spec: &proto::WriteSpec) -> Self {
        let mode = proto::WriteMode::try_from(spec.mode)
            .map(WriteMode::from)
            .unwrap_or(WriteMode::Overwrite);
        Self {
            mode,
            content_type: spec.content_type.clone(),
            metadata: spec.metadata.clone(),
            block_size: spec.block_size.and_then(|b| usize::try_from(b).ok()),
        }
    }
}

/// Convert a [`gcs_rust_fs::Error`](Error) into a [`tonic::Status`] carrying
/// [`ERROR_KIND_HEADER`].
pub fn error_to_status(err: Error) -> Status {
    let kind = err.kind();
    let code = match kind {
        ErrorKind::NotFound => Code::NotFound,
        ErrorKind::PermissionDenied => Code::PermissionDenied,
        ErrorKind::Unauthenticated => Code::Unauthenticated,
        ErrorKind::AlreadyExists => Code::AlreadyExists,
        ErrorKind::InvalidPath | ErrorKind::InvalidRange | ErrorKind::InvalidConfig => {
            Code::InvalidArgument
        }
        ErrorKind::OutOfRange => Code::OutOfRange,
        ErrorKind::IsADirectory
        | ErrorKind::NotADirectory
        | ErrorKind::DirectoryNotEmpty
        | ErrorKind::PreconditionFailed
        | ErrorKind::Closed => Code::FailedPrecondition,
        ErrorKind::Timeout => Code::DeadlineExceeded,
        ErrorKind::Unsupported => Code::Unimplemented,
        ErrorKind::ClientInit | ErrorKind::Other => Code::Internal,
    };

    let mut status = Status::new(code, err.to_string());
    if let Ok(val) = MetadataValue::try_from(kind.as_str()) {
        status.metadata_mut().insert(ERROR_KIND_HEADER, val);
    }
    status
}

/// Parse an [`ErrorKind`] from its [`ErrorKind::as_str`] representation.
pub fn parse_error_kind(s: &str) -> Option<ErrorKind> {
    match s {
        "invalid_path" => Some(ErrorKind::InvalidPath),
        "invalid_range" => Some(ErrorKind::InvalidRange),
        "invalid_config" => Some(ErrorKind::InvalidConfig),
        "not_found" => Some(ErrorKind::NotFound),
        "permission_denied" => Some(ErrorKind::PermissionDenied),
        "unauthenticated" => Some(ErrorKind::Unauthenticated),
        "out_of_range" => Some(ErrorKind::OutOfRange),
        "timeout" => Some(ErrorKind::Timeout),
        "client_init" => Some(ErrorKind::ClientInit),
        "is_a_directory" => Some(ErrorKind::IsADirectory),
        "not_a_directory" => Some(ErrorKind::NotADirectory),
        "already_exists" => Some(ErrorKind::AlreadyExists),
        "directory_not_empty" => Some(ErrorKind::DirectoryNotEmpty),
        "precondition_failed" => Some(ErrorKind::PreconditionFailed),
        "unsupported" => Some(ErrorKind::Unsupported),
        "closed" => Some(ErrorKind::Closed),
        "other" => Some(ErrorKind::Other),
        _ => None,
    }
}

/// Reconstruct a [`gcs_rust_fs::Error`](Error) from a [`tonic::Status`],
/// consulting [`ERROR_KIND_HEADER`] first and falling back to the gRPC [`Code`].
pub fn status_to_error(status: Status) -> Error {
    let header_kind = status
        .metadata()
        .get(ERROR_KIND_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_error_kind);

    let kind = header_kind.unwrap_or_else(|| match status.code() {
        Code::NotFound => ErrorKind::NotFound,
        Code::PermissionDenied => ErrorKind::PermissionDenied,
        Code::Unauthenticated => ErrorKind::Unauthenticated,
        Code::AlreadyExists => ErrorKind::AlreadyExists,
        Code::InvalidArgument => ErrorKind::InvalidPath,
        Code::OutOfRange => ErrorKind::OutOfRange,
        Code::FailedPrecondition => ErrorKind::PreconditionFailed,
        Code::DeadlineExceeded => ErrorKind::Timeout,
        Code::Unimplemented => ErrorKind::Unsupported,
        _ => ErrorKind::Other,
    });

    Error::with_source(kind, status.message().to_owned(), status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_all_error_kinds_through_grpc_status() {
        let kinds = [
            ErrorKind::InvalidPath,
            ErrorKind::InvalidRange,
            ErrorKind::InvalidConfig,
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::Unauthenticated,
            ErrorKind::OutOfRange,
            ErrorKind::Timeout,
            ErrorKind::ClientInit,
            ErrorKind::IsADirectory,
            ErrorKind::NotADirectory,
            ErrorKind::AlreadyExists,
            ErrorKind::DirectoryNotEmpty,
            ErrorKind::PreconditionFailed,
            ErrorKind::Unsupported,
            ErrorKind::Closed,
            ErrorKind::Other,
        ];

        for kind in kinds {
            let original = Error::new(kind, format!("test failure for {kind}"));
            let status = error_to_status(original);
            let recovered = status_to_error(status);
            assert_eq!(recovered.kind(), kind, "failed round-trip for {kind:?}");
            assert!(recovered.to_string().contains(&format!("test failure for {kind}")));
        }
    }
}
