//! An opened object supporting repeated ranged reads.

use bytes::Bytes;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use tracing::debug;

use crate::error::Result;
use crate::fs::{ActiveRead, GcsFs, Transport};
use crate::path::GcsPath;
use crate::range::{ByteRange, ResolvedRange};
use crate::stat::ObjectStat;

/// A handle to a single generation of an object, for repeated ranged reads.
///
/// Opening pins the generation (unless the path already specifies one), so
/// every subsequent read observes the same immutable bytes even if the object
/// is overwritten concurrently. Because the size is known, ranges are clamped
/// with Python-slice semantics: reading at or beyond the end yields empty
/// bytes rather than an error, matching `fsspec`'s `AbstractBufferedFile`.
///
/// With [`Transport::Grpc`] the handle keeps one bidi stream open and
/// multiplexes reads over it (`read_range` may be called concurrently from
/// several tasks). With [`Transport::Http`] each read is an independent
/// request pinned to the opened generation.
///
/// ```no_run
/// use gcs_rust_fs::{ByteRange, GcsFs, GcsPath};
///
/// # async fn demo() -> gcs_rust_fs::Result<()> {
/// let fs = GcsFs::new().await?;
/// let file = fs.open(&GcsPath::parse("gs://my-bucket/data.bin")?).await?;
/// let footer = file.read_range(ByteRange::tail(64)).await?;
/// let header = file.read_at(0, 4096).await?;
/// println!("{} bytes total", file.size());
/// # let _ = (footer, header);
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct GcsFile {
    fs: GcsFs,
    path: GcsPath,
    stat: ObjectStat,
    descriptor: Option<ObjectDescriptor>,
}

impl GcsFile {
    pub(crate) async fn open(fs: GcsFs, path: &GcsPath) -> Result<Self> {
        debug!(%path, transport = %fs.transport(), "open");
        let (stat, descriptor) = match fs.transport() {
            Transport::Grpc => {
                // Opening the descriptor returns the metadata for free.
                let descriptor = fs.open_descriptor(path).await?;
                (ObjectStat::from(descriptor.object()), Some(descriptor))
            }
            Transport::Http => (fs.stat(path).await?, None),
        };
        let path = path
            .clone()
            .with_generation(path.generation().or(Some(stat.generation)));
        Ok(Self {
            fs,
            path,
            stat,
            descriptor,
        })
    }

    /// The path this handle was opened with, pinned to the opened generation.
    pub fn path(&self) -> &GcsPath {
        &self.path
    }

    /// Metadata captured when the file was opened.
    pub fn stat(&self) -> &ObjectStat {
        &self.stat
    }

    /// Size of the object in bytes.
    pub fn size(&self) -> u64 {
        self.stat.size
    }

    /// The generation this handle is pinned to.
    pub fn generation(&self) -> i64 {
        self.stat.generation
    }

    /// Read `range` of the object. Out-of-bounds ranges are clamped; an empty
    /// result means the range lies entirely past the end of the object.
    pub async fn read_range(&self, range: ByteRange) -> Result<Bytes> {
        let ResolvedRange::Read { range, len_hint } = range.resolve(Some(self.stat.size))? else {
            return Ok(Bytes::new());
        };
        debug!(path = %self.path, ?range, "read_range");
        let read = match &self.descriptor {
            Some(descriptor) => ActiveRead::from_reader(descriptor.read_range(range).await),
            None => self.fs.start_read(&self.path, range).await?,
        };
        read.collect(len_hint, &self.path).await
    }

    /// Read up to `len` bytes starting at `offset` (the `pread` idiom).
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Bytes> {
        self.read_range(ByteRange::span(offset, offset.saturating_add(len)))
            .await
    }

    /// Read the whole object.
    pub async fn read_all(&self) -> Result<Bytes> {
        self.read_range(ByteRange::ALL).await
    }
}
