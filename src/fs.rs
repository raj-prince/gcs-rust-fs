//! The [`GcsFs`] client: `stat`, `cat_file` and `open`.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use bytes::{Bytes, BytesMut};
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use google_cloud_storage::read_object::ReadObjectResponse;
use tracing::debug;

use crate::error::{Error, Result};
use crate::file::GcsFile;
use crate::path::GcsPath;
use crate::range::{ByteRange, ResolvedRange};
use crate::stat::ObjectStat;

/// The protocol used to read object **data**.
///
/// Metadata (`stat`) always uses gRPC via the `StorageControl` client, which is
/// generally available. Data reads can use either:
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Transport {
    /// Bidirectional-streaming gRPC (`BidiReadObject`, via
    /// `Storage::open_object`). This is the fastest path and supports
    /// concurrent ranged reads over a single stream, but **the API is only
    /// enabled for some projects and buckets** — contact your Google Cloud
    /// account team to enable it.
    #[default]
    Grpc,
    /// The JSON API over HTTP (`Storage::read_object`). Universally available.
    Http,
}

impl Transport {
    /// Environment variable consulted by [`GcsFsBuilder::from_env`] and the
    /// [shared](crate::shared) client. Accepts `grpc` or `http`.
    pub const ENV_VAR: &'static str = "GCS_RUST_FS_TRANSPORT";

    /// The canonical lower-case name (`"grpc"` / `"http"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Transport::Grpc => "grpc",
            Transport::Http => "http",
        }
    }

    /// Read the transport from [`Transport::ENV_VAR`], defaulting to
    /// [`Transport::Grpc`] when unset or empty.
    pub fn from_env() -> Result<Self> {
        match std::env::var(Self::ENV_VAR) {
            Ok(value) if !value.trim().is_empty() => value.parse(),
            _ => Ok(Self::default()),
        }
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Transport {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "grpc" => Ok(Transport::Grpc),
            "http" | "json" | "rest" => Ok(Transport::Http),
            other => Err(Error::invalid_config(format!(
                "unknown transport {other:?}: expected \"grpc\" or \"http\""
            ))),
        }
    }
}

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

    /// Override the service endpoint for both the data and control clients
    /// (e.g. to target a test bench or private endpoint).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Number of gRPC subchannels (HTTP/2 connections) used by the data client.
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

    /// Construct the clients. Credentials are discovered via
    /// [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
    pub async fn build(self) -> Result<GcsFs> {
        let transport = self.transport.unwrap_or_default();
        let mut storage = Storage::builder();
        let mut control = StorageControl::builder();
        if let Some(endpoint) = &self.endpoint {
            storage = storage.with_endpoint(endpoint.clone());
            control = control.with_endpoint(endpoint.clone());
        }
        if let Some(count) = self.grpc_subchannel_count {
            storage = storage.with_grpc_subchannel_count(count);
        }
        debug!(%transport, endpoint = ?self.endpoint, "building GcsFs clients");
        let (storage, control) = tokio::try_join!(
            async { storage.build().await.map_err(Error::client_init) },
            async { control.build().await.map_err(Error::client_init) },
        )?;
        Ok(GcsFs {
            storage,
            control,
            transport,
        })
    }
}

/// A non-POSIX view of Google Cloud Storage: look up object metadata and read
/// whole objects or byte ranges straight into memory.
///
/// `GcsFs` is cheap to clone (the SDK clients are reference counted) and holds
/// connection pools, so create one and share it.
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
    storage: Storage,
    control: StorageControl,
    transport: Transport,
}

impl GcsFs {
    /// Start configuring a client.
    pub fn builder() -> GcsFsBuilder {
        GcsFsBuilder::new()
    }

    /// Build a client with default settings and Application Default Credentials.
    pub async fn new() -> Result<Self> {
        Self::builder().build().await
    }

    /// The transport used for data reads.
    pub fn transport(&self) -> Transport {
        self.transport
    }

    /// The underlying SDK data-plane client, for operations not wrapped here.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// The underlying SDK control-plane client, for operations not wrapped here.
    pub fn control(&self) -> &StorageControl {
        &self.control
    }

    /// Fetch object metadata (one gRPC `GetObject` call).
    ///
    /// Honours `path.generation()`; otherwise the live generation is returned.
    pub async fn stat(&self, path: &GcsPath) -> Result<ObjectStat> {
        debug!(%path, "stat");
        let mut request = self
            .control
            .get_object()
            .set_bucket(path.bucket_resource())
            .set_object(path.object());
        if let Some(generation) = path.generation() {
            request = request.set_generation(generation);
        }
        let object = request
            .send()
            .await
            .map_err(|e| Error::storage("stat", path, e))?;
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
        debug!(%path, ?range, transport = %self.transport, "cat_file");
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
        let read = self.start_read(&path, range).await?;
        read.collect(len_hint, &path).await
    }

    /// Open an object for repeated ranged reads. See [`GcsFile`].
    pub async fn open(&self, path: &GcsPath) -> Result<GcsFile> {
        GcsFile::open(self.clone(), path).await
    }

    /// Issue a one-shot ranged read using the configured transport.
    pub(crate) async fn start_read(&self, path: &GcsPath, range: ReadRange) -> Result<ActiveRead> {
        match self.transport {
            Transport::Grpc => {
                let mut request = self
                    .storage
                    .open_object(path.bucket_resource(), path.object());
                if let Some(generation) = path.generation() {
                    request = request.set_generation(generation);
                }
                // `send_and_read` opens the object and requests the range in a
                // single RPC, avoiding the extra round trip of `send()` +
                // `read_range()`.
                let (descriptor, reader) = request
                    .send_and_read(range)
                    .await
                    .map_err(|e| Error::storage("read", path, e))?;
                Ok(ActiveRead {
                    reader,
                    _descriptor: Some(descriptor),
                })
            }
            Transport::Http => {
                let mut request = self
                    .storage
                    .read_object(path.bucket_resource(), path.object())
                    .set_read_range(range);
                if let Some(generation) = path.generation() {
                    request = request.set_generation(generation);
                }
                let reader = request
                    .send()
                    .await
                    .map_err(|e| Error::storage("read", path, e))?;
                Ok(ActiveRead {
                    reader,
                    _descriptor: None,
                })
            }
        }
    }

    /// Open a bidi descriptor (gRPC transport only).
    pub(crate) async fn open_descriptor(&self, path: &GcsPath) -> Result<ObjectDescriptor> {
        let mut request = self
            .storage
            .open_object(path.bucket_resource(), path.object());
        if let Some(generation) = path.generation() {
            request = request.set_generation(generation);
        }
        request
            .send()
            .await
            .map_err(|e| Error::storage("open", path, e))
    }
}

/// An in-flight ranged read.
///
/// For the gRPC transport the [`ObjectDescriptor`] **must** outlive the
/// reader: dropping the descriptor closes the bidi stream and interrupts any
/// reads still in progress. Bundling them guarantees the ordering.
pub(crate) struct ActiveRead {
    pub(crate) reader: ReadObjectResponse,
    _descriptor: Option<ObjectDescriptor>,
}

impl ActiveRead {
    pub(crate) fn from_reader(reader: ReadObjectResponse) -> Self {
        Self {
            reader,
            _descriptor: None,
        }
    }

    /// Drain the stream into a single contiguous buffer.
    ///
    /// Single-chunk responses (the common case for small ranged reads) are
    /// returned without copying.
    pub(crate) async fn collect(mut self, len_hint: Option<u64>, path: &GcsPath) -> Result<Bytes> {
        // Cap the up-front allocation; `BytesMut` grows as needed beyond this.
        const MAX_INITIAL_CAPACITY: u64 = 64 * 1024 * 1024;

        let mut first: Option<Bytes> = None;
        let mut buffer: Option<BytesMut> = None;
        while let Some(chunk) = self
            .reader
            .next()
            .await
            .transpose()
            .map_err(|e| Error::storage("read", path, e))?
        {
            if chunk.is_empty() {
                continue;
            }
            if let Some(buffer) = buffer.as_mut() {
                buffer.extend_from_slice(&chunk);
            } else if let Some(head) = first.take() {
                let hint = len_hint.map_or(0, |h| h.min(MAX_INITIAL_CAPACITY) as usize);
                let mut b = BytesMut::with_capacity(hint.max(head.len() + chunk.len()));
                b.extend_from_slice(&head);
                b.extend_from_slice(&chunk);
                buffer = Some(b);
            } else {
                first = Some(chunk);
            }
        }
        Ok(match (buffer, first) {
            (Some(buffer), _) => buffer.freeze(),
            (None, Some(head)) => head,
            (None, None) => Bytes::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_parses_case_insensitively() {
        assert_eq!("grpc".parse::<Transport>().unwrap(), Transport::Grpc);
        assert_eq!(" GRPC ".parse::<Transport>().unwrap(), Transport::Grpc);
        assert_eq!("http".parse::<Transport>().unwrap(), Transport::Http);
        assert_eq!("JSON".parse::<Transport>().unwrap(), Transport::Http);
        let err = "carrier-pigeon".parse::<Transport>().unwrap_err();
        assert_eq!(err.kind(), crate::ErrorKind::InvalidConfig);
        assert_eq!(Transport::default(), Transport::Grpc);
        assert_eq!(Transport::Http.to_string(), "http");
    }

    #[test]
    fn builder_prefers_explicit_transport_over_env() {
        let b = GcsFsBuilder::new().transport(Transport::Http);
        let b = b.from_env().unwrap();
        assert_eq!(b.transport, Some(Transport::Http));
    }

    /// A fake response that yields the given chunks in order.
    fn fake_response(chunks: &[&'static [u8]]) -> ReadObjectResponse {
        use google_cloud_storage::model_ext::ObjectHighlights;
        use google_cloud_storage::streaming_source::StreamingSource;
        use std::collections::VecDeque;

        struct Chunks(VecDeque<Bytes>);
        impl StreamingSource for Chunks {
            type Error = std::io::Error;
            async fn next(&mut self) -> Option<std::result::Result<Bytes, Self::Error>> {
                self.0.pop_front().map(Ok)
            }
        }

        let chunks = Chunks(chunks.iter().map(|c| Bytes::from_static(c)).collect());
        ReadObjectResponse::from_source(ObjectHighlights::default(), chunks)
    }

    #[tokio::test]
    async fn collect_handles_single_and_multi_chunk_streams() {
        let path = GcsPath::parse("gs://b/o").unwrap();

        let out = ActiveRead::from_reader(fake_response(&[b"hello"]))
            .collect(Some(5), &path)
            .await
            .unwrap();
        assert_eq!(out, Bytes::from_static(b"hello"));

        let out = ActiveRead::from_reader(fake_response(&[b"hel", b"", b"lo ", b"world"]))
            .collect(None, &path)
            .await
            .unwrap();
        assert_eq!(out, Bytes::from_static(b"hello world"));

        let out = ActiveRead::from_reader(fake_response(&[]))
            .collect(None, &path)
            .await
            .unwrap();
        assert!(out.is_empty());
    }
}
