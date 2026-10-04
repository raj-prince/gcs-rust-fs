//! GCS-specific transport plumbing.
//!
//! Everything that knows about the Google Cloud Storage SDK — gRPC
//! `GetObject`, bidi-streaming `BidiReadObject` descriptors, JSON-API ranged
//! reads — lives here. The public [`GcsFs`](crate::GcsFs) and
//! [`GcsFile`](crate::GcsFile) types only speak in file-system terms and
//! delegate to the three primitives below:
//!
//! * [`Backend::get_object`] — fetch metadata,
//! * [`Backend::read_range`] — read one byte range,
//! * [`Backend::open`] — obtain a [`ReadHandle`] for repeated byte-range reads.

use std::fmt;
use std::str::FromStr;

use bytes::{Bytes, BytesMut};
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use google_cloud_storage::read_object::ReadObjectResponse;
use tracing::debug;

use crate::error::{Error, Result};
use crate::path::GcsPath;

/// The protocol used to read object **data**.
///
/// Metadata always uses gRPC via the `StorageControl` client, which is
/// generally available. Data reads can use either:
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Transport {
    /// Bidirectional-streaming gRPC (`BidiReadObject`). This is the fastest
    /// path and supports concurrent ranged reads over a single stream, but
    /// **the API is only enabled for some projects and buckets** — contact
    /// your Google Cloud account team to enable it.
    #[default]
    Grpc,
    /// The JSON API over HTTP with `Range` headers. Universally available.
    Http,
}

impl Transport {
    /// Environment variable consulted by
    /// [`GcsFsBuilder::from_env`](crate::GcsFsBuilder::from_env) and the
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

/// Connection options for [`Backend::connect`].
#[derive(Clone, Debug, Default)]
pub(crate) struct BackendOptions {
    pub(crate) transport: Transport,
    pub(crate) endpoint: Option<String>,
    pub(crate) grpc_subchannel_count: Option<usize>,
}

/// Owns the SDK clients and implements the primitive operations the file
/// system is built from. Cheap to clone: the SDK clients are reference counted.
#[derive(Clone, Debug)]
pub(crate) struct Backend {
    storage: Storage,
    control: StorageControl,
    transport: Transport,
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
        })
    }

    /// The transport used for data reads.
    pub(crate) fn transport(&self) -> Transport {
        self.transport
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

    /// Start a one-shot read of `range` using the configured transport.
    pub(crate) async fn read_range(&self, path: &GcsPath, range: ReadRange) -> Result<Reader> {
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
                let (descriptor, stream) = request
                    .send_and_read(range)
                    .await
                    .map_err(|e| Error::storage("read", path, e))?;
                Ok(Reader::with_descriptor(stream, descriptor))
            }
            Transport::Http => {
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
        }
    }

    /// Open `path` for repeated ranged reads of a single generation.
    ///
    /// Returns the object metadata together with the handle. On gRPC the
    /// metadata comes back with the open response (no extra RPC); on HTTP it
    /// is fetched with [`Backend::get_object`] and the handle is pinned to the
    /// generation observed.
    pub(crate) async fn open(&self, path: &GcsPath) -> Result<(Object, ReadHandle)> {
        match self.transport {
            Transport::Grpc => {
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
            Transport::Http => {
                let object = self.get_object(path).await?;
                let pinned = path.clone().with_generation(Some(object.generation));
                let handle = ReadHandle::Http(Box::new(HttpHandle {
                    backend: self.clone(),
                    path: pinned,
                }));
                Ok((object, handle))
            }
        }
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
            ReadHandle::Http(http) => http.backend.read_range(&http.path, range).await,
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
