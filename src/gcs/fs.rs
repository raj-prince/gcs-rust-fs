//! The [`GcsFs`] file system: `stat`, `exists`, `cat_file` and `open`.
//!
//! This module contains only file-system semantics (path and range
//! resolution, generation pinning, error context). How bytes and metadata
//! actually travel to Cloud Storage is the concern of the private backend
//! module.

use std::borrow::Cow;

use bytes::Bytes;
use tracing::debug;

use crate::error::Result;
use crate::gcs::backend::{Backend, BackendOptions, Transport};
use crate::gcs::file::GcsFile;
use crate::path::GcsPath;
use crate::range::{ByteRange, ResolvedRange};
use crate::stat::ObjectStat;

/// Builder for [`GcsFs`].
#[derive(Clone, Debug, Default)]
pub struct GcsFsBuilder {
    transport: Option<Transport>,
    endpoint: Option<String>,
    grpc_subchannel_count: Option<usize>,
}

impl GcsFsBuilder {
    /// Create a builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Select the data-read [`Transport`] (default: [`Transport::Grpc`]).
    pub fn transport(mut self, transport: Transport) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Override the service endpoint (e.g. to target a test bench or a
    /// private endpoint).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Number of gRPC subchannels (HTTP/2 connections) used for data reads.
    /// Defaults to the SDK's choice (the available parallelism).
    pub fn grpc_subchannel_count(mut self, count: usize) -> Self {
        self.grpc_subchannel_count = Some(count);
        self
    }

    /// Fill unset options from the environment (currently
    /// [`Transport::ENV_VAR`]). Explicitly set options take precedence.
    pub fn from_env(mut self) -> Result<Self> {
        if self.transport.is_none() {
            self.transport = Some(Transport::from_env()?);
        }
        Ok(self)
    }

    /// Connect to Cloud Storage. Credentials are discovered via
    /// [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
    pub async fn build(self) -> Result<GcsFs> {
        let backend = Backend::connect(BackendOptions {
            transport: self.transport.unwrap_or_default(),
            endpoint: self.endpoint,
            grpc_subchannel_count: self.grpc_subchannel_count,
        })
        .await?;
        Ok(GcsFs { backend })
    }
}

/// A non-POSIX view of Google Cloud Storage: look up object metadata and read
/// whole objects or byte ranges straight into memory.
///
/// `GcsFs` is cheap to clone and holds connection pools, so create one and
/// share it.
///
/// ```no_run
/// use gcs_rust_fs::{ByteRange, GcsFs, GcsPath};
///
/// # async fn demo() -> gcs_rust_fs::Result<()> {
/// let fs = GcsFs::new().await?;
/// let path = GcsPath::parse("gs://my-bucket/checkpoint.pt")?;
///
/// let stat = fs.stat(&path).await?;
/// println!("{} bytes, generation {}", stat.size, stat.generation);
///
/// let header = fs.cat_file(&path, ByteRange::head(1024)).await?;
/// assert!(header.len() <= 1024);
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct GcsFs {
    backend: Backend,
}

impl GcsFs {
    /// Start configuring a file system.
    pub fn builder() -> GcsFsBuilder {
        GcsFsBuilder::new()
    }

    /// Connect with default settings and Application Default Credentials.
    pub async fn new() -> Result<Self> {
        Self::builder().build().await
    }

    /// The transport used for data reads.
    pub fn transport(&self) -> Transport {
        self.backend.transport()
    }

    /// Fetch object metadata.
    ///
    /// Honours `path.generation()`; otherwise the live generation is returned.
    pub async fn stat(&self, path: &GcsPath) -> Result<ObjectStat> {
        debug!(%path, "stat");
        let object = self.backend.get_object(path).await?;
        Ok(ObjectStat::from(object))
    }

    /// Whether the object (or the given generation of it) exists.
    pub async fn exists(&self, path: &GcsPath) -> Result<bool> {
        match self.stat(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Read `range` of the object into memory.
    ///
    /// * Empty ranges return empty bytes without a request.
    /// * Ranges that mix a negative bound with another bound
    ///   ([`ByteRange::needs_size`]) cost one extra `stat` call; the read is
    ///   then pinned to the generation observed by that `stat`.
    /// * A start offset at or beyond the end of the object is reported by the
    ///   service as [`ErrorKind::OutOfRange`](crate::ErrorKind::OutOfRange).
    pub async fn cat_file(&self, path: &GcsPath, range: ByteRange) -> Result<Bytes> {
        debug!(%path, ?range, "cat_file");
        let (path, resolved) = if range.needs_size() {
            let stat = self.stat(path).await?;
            let pinned = path
                .clone()
                .with_generation(path.generation().or(Some(stat.generation)));
            (Cow::Owned(pinned), range.resolve(Some(stat.size))?)
        } else {
            (Cow::Borrowed(path), range.resolve(None)?)
        };
        let ResolvedRange::Read { range, len_hint } = resolved else {
            return Ok(Bytes::new());
        };
        let reader = self.backend.read_range(&path, range).await?;
        reader.collect(len_hint, &path).await
    }

    /// Open an object for repeated ranged reads. See [`GcsFile`].
    pub async fn open(&self, path: &GcsPath) -> Result<GcsFile> {
        debug!(%path, "open");
        GcsFile::open(&self.backend, path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_prefers_explicit_transport_over_env() {
        let b = GcsFsBuilder::new().transport(Transport::Http);
        let b = b.from_env().unwrap();
        assert_eq!(b.transport, Some(Transport::Http));
    }
}
