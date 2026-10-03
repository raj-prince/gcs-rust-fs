//! Error types for `gcs-rust-fs`.
//!
//! Every fallible operation returns [`Result<T>`], whose error type is
//! [`Error`]. An [`Error`] always carries an [`ErrorKind`], so callers — in
//! particular foreign-language bridges such as the `gcsfs` PyO3 layer — can map
//! failures onto their own exception hierarchy (`FileNotFoundError`,
//! `PermissionError`, ...) without parsing message strings.

use std::fmt;

use google_cloud_gax::error::rpc::Code;

use crate::path::GcsPath;

/// The error type produced by the underlying Google Cloud Rust SDK.
pub type StorageError = google_cloud_storage::Error;

/// A specialised `Result` type for `gcs-rust-fs` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Broad classification of an [`Error`].
///
/// The mapping to transport-level failures is:
///
/// | `ErrorKind`        | HTTP (JSON API) | gRPC status         |
/// |--------------------|-----------------|---------------------|
/// | `NotFound`         | 404             | `NOT_FOUND`         |
/// | `PermissionDenied` | 403             | `PERMISSION_DENIED` |
/// | `Unauthenticated`  | 401             | `UNAUTHENTICATED`   |
/// | `OutOfRange`       | 416             | `OUT_OF_RANGE`      |
/// | `Timeout`          | 408 / 504       | `DEADLINE_EXCEEDED` |
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The supplied path could not be parsed into a bucket and object name.
    InvalidPath,
    /// The supplied byte range could not be resolved.
    InvalidRange,
    /// A configuration value (builder option or environment variable) is invalid.
    InvalidConfig,
    /// The bucket, object, or object generation does not exist.
    NotFound,
    /// The caller is not allowed to perform the operation.
    PermissionDenied,
    /// The request lacked valid credentials.
    Unauthenticated,
    /// The requested byte range lies entirely outside the object.
    OutOfRange,
    /// The operation did not complete within its deadline.
    Timeout,
    /// The SDK client could not be constructed (e.g. no credentials found).
    ClientInit,
    /// [`init_shared`](crate::init_shared) was called more than once.
    AlreadyInitialized,
    /// Any other failure reported by the service or the transport.
    Other,
}

impl ErrorKind {
    /// A short, stable, machine-friendly name for this kind (e.g. `"not_found"`).
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::InvalidPath => "invalid_path",
            ErrorKind::InvalidRange => "invalid_range",
            ErrorKind::InvalidConfig => "invalid_config",
            ErrorKind::NotFound => "not_found",
            ErrorKind::PermissionDenied => "permission_denied",
            ErrorKind::Unauthenticated => "unauthenticated",
            ErrorKind::OutOfRange => "out_of_range",
            ErrorKind::Timeout => "timeout",
            ErrorKind::ClientInit => "client_init",
            ErrorKind::AlreadyInitialized => "already_initialized",
            ErrorKind::Other => "other",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

type BoxedSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The error type returned by every operation in this crate.
///
/// Use [`Error::kind`] to decide how to react to a failure. The `Display`
/// implementation includes the message of the underlying SDK error (if any), so
/// `to_string()` is suitable for surfacing to end users or foreign languages.
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<BoxedSource>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub(crate) fn invalid_path(path: &str, reason: &str) -> Self {
        Self::new(
            ErrorKind::InvalidPath,
            format!("invalid GCS path {path:?}: {reason}"),
        )
    }

    pub(crate) fn invalid_range(reason: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidRange, reason)
    }

    pub(crate) fn invalid_config(reason: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidConfig, reason)
    }

    pub(crate) fn client_init(source: google_cloud_gax::client_builder::Error) -> Self {
        Self::with_source(
            ErrorKind::ClientInit,
            "failed to initialise the Google Cloud Storage client",
            source,
        )
    }

    /// Wrap an SDK error raised while performing `op` on `path`, classifying it
    /// into the most specific [`ErrorKind`] available.
    pub(crate) fn storage(op: &str, path: &GcsPath, source: StorageError) -> Self {
        let kind = classify_storage_error(&source);
        Self::with_source(kind, format!("{op} {path} failed ({kind})"), source)
    }

    /// The classification of this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns `true` if the bucket, object or generation does not exist.
    pub fn is_not_found(&self) -> bool {
        self.kind == ErrorKind::NotFound
    }

    /// Returns `true` if the caller lacks permission for the operation.
    pub fn is_permission_denied(&self) -> bool {
        self.kind == ErrorKind::PermissionDenied
    }

    /// Returns `true` if the requested byte range was outside the object.
    pub fn is_out_of_range(&self) -> bool {
        self.kind == ErrorKind::OutOfRange
    }

    /// The underlying SDK error, if this error originated from the SDK.
    ///
    /// This gives access to the full detail (HTTP status, headers, gRPC
    /// `Status`, ...) for logging or advanced handling.
    pub fn storage_source(&self) -> Option<&StorageError> {
        self.source
            .as_deref()
            .and_then(|s| s.downcast_ref::<StorageError>())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .field("source", &self.source)
            .finish()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|s| s as &(dyn std::error::Error + 'static))
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        use std::io::ErrorKind as IoKind;
        let kind = match err.kind {
            ErrorKind::InvalidPath | ErrorKind::InvalidRange | ErrorKind::InvalidConfig => {
                IoKind::InvalidInput
            }
            ErrorKind::NotFound => IoKind::NotFound,
            ErrorKind::PermissionDenied | ErrorKind::Unauthenticated => IoKind::PermissionDenied,
            ErrorKind::OutOfRange => IoKind::UnexpectedEof,
            ErrorKind::Timeout => IoKind::TimedOut,
            ErrorKind::AlreadyInitialized => IoKind::AlreadyExists,
            ErrorKind::ClientInit | ErrorKind::Other => IoKind::Other,
        };
        std::io::Error::new(kind, err)
    }
}

/// Classify an SDK error into an [`ErrorKind`].
///
/// Both the HTTP (JSON API) and gRPC transports are handled: the canonical
/// gRPC status is preferred when present, falling back to the HTTP status code
/// and finally to the SDK's own predicates (authentication, timeouts).
pub fn classify_storage_error(err: &StorageError) -> ErrorKind {
    if let Some(status) = err.status() {
        match status.code {
            Code::NotFound => return ErrorKind::NotFound,
            Code::PermissionDenied => return ErrorKind::PermissionDenied,
            Code::Unauthenticated => return ErrorKind::Unauthenticated,
            Code::OutOfRange => return ErrorKind::OutOfRange,
            Code::DeadlineExceeded => return ErrorKind::Timeout,
            _ => {}
        }
    }
    match err.http_status_code() {
        Some(404) => return ErrorKind::NotFound,
        Some(403) => return ErrorKind::PermissionDenied,
        Some(401) => return ErrorKind::Unauthenticated,
        Some(416) => return ErrorKind::OutOfRange,
        Some(408) | Some(504) => return ErrorKind::Timeout,
        _ => {}
    }
    if err.is_authentication() {
        return ErrorKind::Unauthenticated;
    }
    if err.is_timeout() {
        return ErrorKind::Timeout;
    }
    ErrorKind::Other
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_storage::http::HeaderMap;

    fn http(code: u16) -> StorageError {
        StorageError::http(code, HeaderMap::new(), bytes::Bytes::new())
    }

    fn grpc(code: Code) -> StorageError {
        StorageError::service(Status::default().set_code(code).set_message("boom"))
    }

    #[test]
    fn classifies_http_status_codes() {
        assert_eq!(classify_storage_error(&http(404)), ErrorKind::NotFound);
        assert_eq!(
            classify_storage_error(&http(403)),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            classify_storage_error(&http(401)),
            ErrorKind::Unauthenticated
        );
        assert_eq!(classify_storage_error(&http(416)), ErrorKind::OutOfRange);
        assert_eq!(classify_storage_error(&http(504)), ErrorKind::Timeout);
        assert_eq!(classify_storage_error(&http(500)), ErrorKind::Other);
    }

    #[test]
    fn classifies_grpc_status_codes() {
        assert_eq!(
            classify_storage_error(&grpc(Code::NotFound)),
            ErrorKind::NotFound
        );
        assert_eq!(
            classify_storage_error(&grpc(Code::PermissionDenied)),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            classify_storage_error(&grpc(Code::Unauthenticated)),
            ErrorKind::Unauthenticated
        );
        assert_eq!(
            classify_storage_error(&grpc(Code::OutOfRange)),
            ErrorKind::OutOfRange
        );
        assert_eq!(
            classify_storage_error(&grpc(Code::DeadlineExceeded)),
            ErrorKind::Timeout
        );
        assert_eq!(
            classify_storage_error(&grpc(Code::Internal)),
            ErrorKind::Other
        );
    }

    #[test]
    fn storage_error_keeps_context_and_source() {
        let path = GcsPath::parse("gs://b/o").unwrap();
        let err = Error::storage("stat", &path, http(404));
        assert!(err.is_not_found());
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert!(err.storage_source().is_some());
        let text = err.to_string();
        assert!(text.contains("stat gs://b/o failed (not_found)"), "{text}");
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn converts_to_io_error() {
        let path = GcsPath::parse("gs://b/o").unwrap();
        let io: std::io::Error = Error::storage("stat", &path, http(403)).into();
        assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied);
        let io: std::io::Error = Error::invalid_path("x", "nope").into();
        assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput);
    }
}
