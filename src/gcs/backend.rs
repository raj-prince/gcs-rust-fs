//! GCS-specific transport plumbing.
//!
//! Everything that knows about the Google Cloud Storage SDK lives below
//! `src/gcs/`; this module owns the clients and the **read** path:
//!
//! * [`Backend::get_object`] — fetch metadata,
//! * [`Backend::read_range`] — read one byte range,
//! * [`Backend::open`] — obtain a [`ReadHandle`] for repeated byte-range reads,
//! * [`Backend::bucket_kind`] — detect and cache the [`BucketKind`].
//!
//! Listing, bucket/folder management and deletes are in `control.rs`; uploads
//! are in `write.rs`. Both extend `Backend` with further `impl` blocks.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use google_cloud_storage::read_object::ReadObjectResponse;
use tracing::{debug, warn};

use crate::error::{Error, ErrorKind, Result};
use crate::gcs::layout::BucketKind;
use crate::gcs::path::GcsPath;

/// The protocol used to read object **data** from non-zonal buckets.
///
/// Metadata always uses gRPC via the `StorageControl` client, which is
/// generally available. Zonal buckets are gRPC-only and ignore this setting.
/// Data reads from other buckets can use either:
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Transport {
    /// Bidirectional-streaming gRPC (`BidiReadObject`). This is the fastest
    /// path and supports concurrent ranged reads over a single stream, but
    /// **the API is only enabled for some projects and buckets** — contact
    /// your Google Cloud account team to enable it. When the service reports
    /// it as unavailable for a bucket, the read transparently falls back to
    /// [`Transport::Http`].
    #[default]
    Grpc,
    /// The JSON API over HTTP with `Range` headers. Universally available.
    Http,
}

impl Transport {
    /// Environment variable consulted by
    /// [`GcsFsBuilder::from_env`](crate::GcsFsBuilder::from_env). Accepts
    /// `grpc` or `http`.
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

/// Connection options for [`Backend::connect`].
#[derive(Clone, Debug, Default)]
pub(crate) struct BackendOptions {
    pub(crate) transport: Transport,
    pub(crate) endpoint: Option<String>,
    pub(crate) grpc_subchannel_count: Option<usize>,
    pub(crate) project: Option<String>,
    pub(crate) bucket_kinds: HashMap<String, BucketKind>,
    pub(crate) finalize_on_close: bool,
}

/// Owns the SDK clients and implements the primitive operations the file
/// system is built from. Cheap to clone: the SDK clients are reference counted
/// and the bucket-kind cache is shared.
#[derive(Clone, Debug)]
pub(crate) struct Backend {
    storage: Storage,
    control: StorageControl,
    transport: Transport,
    project: Option<String>,
    /// Only consumed by the appendable writer, which the storage SDK compiles
    /// in behind the `google_cloud_unstable_storage_bidi` cfg.
    #[cfg_attr(not(google_cloud_unstable_storage_bidi), allow(dead_code))]
    finalize_on_close: bool,
    kinds: Arc<Mutex<HashMap<String, BucketKind>>>,
}

impl Backend {
    /// Build both SDK clients using Application Default Credentials.
    pub(crate) async fn connect(options: BackendOptions) -> Result<Self> {
        let mut storage = Storage::builder();
        let mut control = StorageControl::builder();
        if let Some(endpoint) = &options.endpoint {
            storage = storage.with_endpoint(endpoint.clone());
            control = control.with_endpoint(endpoint.clone());
        }
        if let Some(count) = options.grpc_subchannel_count {
            storage = storage.with_grpc_subchannel_count(count);
        }
        debug!(
            transport = %options.transport,
            endpoint = ?options.endpoint,
            project = ?options.project,
            "connecting storage clients"
        );
        let (storage, control) = tokio::try_join!(
            async { storage.build().await.map_err(Error::client_init) },
            async { control.build().await.map_err(Error::client_init) },
        )?;
        Ok(Self {
            storage,
            control,
            transport: options.transport,
            project: options.project,
            finalize_on_close: options.finalize_on_close,
            kinds: Arc::new(Mutex::new(options.bucket_kinds)),
        })
    }

    pub(crate) fn storage(&self) -> &Storage {
        &self.storage
    }

    pub(crate) fn control(&self) -> &StorageControl {
        &self.control
    }

    /// The configured transport for data reads from non-zonal buckets.
    pub(crate) fn transport(&self) -> Transport {
        self.transport
    }

    /// Whether zonal writers finalize the object on `close`.
    #[cfg_attr(not(google_cloud_unstable_storage_bidi), allow(dead_code))]
    pub(crate) fn finalize_on_close(&self) -> bool {
        self.finalize_on_close
    }

    /// The project used for bucket listing and creation.
    pub(crate) fn project(&self) -> Result<&str> {
        self.project.as_deref().ok_or_else(|| {
            Error::invalid_config(
                "a project is required to list or create buckets: set \
                 GcsFsBuilder::project(..), or GOOGLE_CLOUD_PROJECT together \
                 with GcsFsBuilder::from_env()",
            )
        })
    }

    /// The project, if one was configured.
    pub(crate) fn project_opt(&self) -> Option<&str> {
        self.project.as_deref()
    }

    /// Resolve the kind of `bucket`, consulting the cache first and
    /// `GetStorageLayout` otherwise. Successful lookups are cached forever.
    pub(crate) async fn bucket_kind(&self, bucket: &str) -> Result<BucketKind> {
        if let Some(kind) = self.kinds.lock().expect("kind cache poisoned").get(bucket) {
            return Ok(*kind);
        }
        let layout = self
            .control
            .get_storage_layout()
            .set_name(format!("projects/_/buckets/{bucket}/storageLayout"))
            .send()
            .await
            .map_err(|e| Error::storage("storage_layout", bucket, e))?;
        let kind = BucketKind::from_layout(&layout);
        debug!(bucket, %kind, location = %layout.location, "detected bucket kind");
        self.seed_kind(bucket, kind);
        Ok(kind)
    }

    /// Record the kind of `bucket` without a lookup (builder overrides and
    /// buckets this client created).
    pub(crate) fn seed_kind(&self, bucket: &str, kind: BucketKind) {
        self.kinds
            .lock()
            .expect("kind cache poisoned")
            .insert(bucket.to_owned(), kind);
    }

    /// Like [`bucket_kind`](Self::bucket_kind) but never fails: when the
    /// layout cannot be read the bucket is treated as flat (without caching,
    /// so the next call retries), which is what gcsfs does.
    pub(crate) async fn kind_or_flat(&self, bucket: &str) -> BucketKind {
        match self.bucket_kind(bucket).await {
            Ok(kind) => kind,
            // A missing bucket is reported by whatever operation follows.
            Err(e) if e.kind() == ErrorKind::NotFound => BucketKind::Flat,
            Err(e) => {
                warn!(bucket, error = %e, "could not determine bucket kind; assuming flat");
                BucketKind::Flat
            }
        }
    }

    /// The transport actually used for `kind`: zonal buckets are gRPC-only.
    fn data_transport(&self, kind: BucketKind) -> Transport {
        if kind == BucketKind::Zonal {
            Transport::Grpc
        } else {
            self.transport
        }
    }

    /// Whether a failed bidi read should be retried over HTTP: only for
    /// buckets where HTTP is an option, and only when the service says the
    /// bidi API is not available rather than reporting a real failure.
    fn should_fall_back(kind: BucketKind, err: &Error) -> bool {
        kind != BucketKind::Zonal
            && matches!(
                err.kind(),
                ErrorKind::Unsupported | ErrorKind::PreconditionFailed
            )
    }

    /// Fetch object metadata with one gRPC `GetObject` call, honouring
    /// `path.generation()`.
    pub(crate) async fn get_object(&self, path: &GcsPath) -> Result<Object> {
        let mut request = self
            .control
            .get_object()
            .set_bucket(path.bucket_resource())
            .set_object(path.object());
        if let Some(generation) = path.generation() {
            request = request.set_generation(generation);
        }
        request
            .send()
            .await
            .map_err(|e| Error::storage("stat", path, e))
    }

    /// Start a one-shot read of `range`.
    pub(crate) async fn read_range(
        &self,
        path: &GcsPath,
        range: ReadRange,
        kind: BucketKind,
    ) -> Result<Reader> {
        match self.data_transport(kind) {
            Transport::Grpc => match self.read_range_grpc(path, range.clone()).await {
                Err(e) if Self::should_fall_back(kind, &e) => {
                    warn!(%path, error = %e, "bidi read unavailable; falling back to HTTP");
                    self.read_range_http(path, range).await
                }
                other => other,
            },
            Transport::Http => self.read_range_http(path, range).await,
        }
    }

    async fn read_range_grpc(&self, path: &GcsPath, range: ReadRange) -> Result<Reader> {
        let mut request = self
            .storage
            .open_object(path.bucket_resource(), path.object());
        if let Some(generation) = path.generation() {
            request = request.set_generation(generation);
        }
        // `send_and_read` opens the object and requests the range in a single
        // RPC, avoiding the extra round trip of `send()` + `read_range()`.
        let (descriptor, stream) = request
            .send_and_read(range)
            .await
            .map_err(|e| Error::storage("read", path, e))?;
        Ok(Reader::with_descriptor(stream, descriptor))
    }

    async fn read_range_http(&self, path: &GcsPath, range: ReadRange) -> Result<Reader> {
        let mut request = self
            .storage
            .read_object(path.bucket_resource(), path.object())
            .set_read_range(range);
        if let Some(generation) = path.generation() {
            request = request.set_generation(generation);
        }
        let stream = request
            .send()
            .await
            .map_err(|e| Error::storage("read", path, e))?;
        Ok(Reader::new(stream))
    }

    /// Open `path` for repeated ranged reads of a single generation.
    ///
    /// Returns the object metadata together with the handle. On gRPC the
    /// metadata comes back with the open response (no extra RPC); on HTTP it
    /// is fetched with [`Backend::get_object`] and the handle is pinned to the
    /// generation observed.
    pub(crate) async fn open(
        &self,
        path: &GcsPath,
        kind: BucketKind,
    ) -> Result<(Object, ReadHandle)> {
        match self.data_transport(kind) {
            Transport::Grpc => match self.open_grpc(path).await {
                Err(e) if Self::should_fall_back(kind, &e) => {
                    warn!(%path, error = %e, "bidi read unavailable; falling back to HTTP");
                    self.open_http(path).await
                }
                other => other,
            },
            Transport::Http => self.open_http(path).await,
        }
    }

    async fn open_grpc(&self, path: &GcsPath) -> Result<(Object, ReadHandle)> {
        let mut request = self
            .storage
            .open_object(path.bucket_resource(), path.object());
        if let Some(generation) = path.generation() {
            request = request.set_generation(generation);
        }
        let descriptor = request
            .send()
            .await
            .map_err(|e| Error::storage("open", path, e))?;
        Ok((descriptor.object(), ReadHandle::Grpc(descriptor)))
    }

    async fn open_http(&self, path: &GcsPath) -> Result<(Object, ReadHandle)> {
        let object = self.get_object(path).await?;
        let pinned = path.clone().with_generation(Some(object.generation));
        let handle = ReadHandle::Http(Box::new(HttpHandle {
            backend: self.clone(),
            path: pinned,
        }));
        Ok((object, handle))
    }
}

/// A handle for repeated ranged reads of one object generation.
#[derive(Clone, Debug)]
pub(crate) enum ReadHandle {
    /// One bidi gRPC stream; reads are multiplexed over it and may run
    /// concurrently. Dropping the handle closes the stream.
    Grpc(ObjectDescriptor),
    /// Each read is an independent HTTP request pinned to the opened
    /// generation. Boxed so both variants stay pointer-sized.
    Http(Box<HttpHandle>),
}

/// State for [`ReadHandle::Http`].
#[derive(Clone, Debug)]
pub(crate) struct HttpHandle {
    backend: Backend,
    /// The opened path with its generation pinned.
    path: GcsPath,
}

impl ReadHandle {
    /// Start reading `range`. The handle must outlive the returned [`Reader`].
    pub(crate) async fn read_range(&self, range: ReadRange) -> Result<Reader> {
        match self {
            ReadHandle::Grpc(descriptor) => Ok(Reader::new(descriptor.read_range(range).await)),
            ReadHandle::Http(http) => http.backend.read_range_http(&http.path, range).await,
        }
    }
}

/// An in-flight ranged read.
///
/// For one-shot gRPC reads the [`ObjectDescriptor`] **must** outlive the
/// stream: dropping the descriptor closes the bidi connection and interrupts
/// any reads still in progress. Bundling them guarantees the ordering.
pub(crate) struct Reader {
    stream: ReadObjectResponse,
    _descriptor: Option<ObjectDescriptor>,
}

impl Reader {
    fn new(stream: ReadObjectResponse) -> Self {
        Self {
            stream,
            _descriptor: None,
        }
    }

    fn with_descriptor(stream: ReadObjectResponse, descriptor: ObjectDescriptor) -> Self {
        Self {
            stream,
            _descriptor: Some(descriptor),
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
            .stream
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
    fn fallback_only_for_non_zonal_unavailability() {
        let unsupported = Error::unsupported("bidi");
        assert!(Backend::should_fall_back(BucketKind::Flat, &unsupported));
        assert!(Backend::should_fall_back(
            BucketKind::Hierarchical,
            &unsupported
        ));
        assert!(!Backend::should_fall_back(BucketKind::Zonal, &unsupported));
        assert!(!Backend::should_fall_back(
            BucketKind::Flat,
            &Error::not_found("b/o")
        ));
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

        let out = Reader::new(fake_response(&[b"hello"]))
            .collect(Some(5), &path)
            .await
            .unwrap();
        assert_eq!(out, Bytes::from_static(b"hello"));

        let out = Reader::new(fake_response(&[b"hel", b"", b"lo ", b"world"]))
            .collect(None, &path)
            .await
            .unwrap();
        assert_eq!(out, Bytes::from_static(b"hello world"));

        let out = Reader::new(fake_response(&[]))
            .collect(None, &path)
            .await
            .unwrap();
        assert!(out.is_empty());
    }
}
