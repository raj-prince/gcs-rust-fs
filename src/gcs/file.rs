//! An opened object supporting repeated ranged reads.

use bytes::Bytes;
use tracing::debug;

use crate::error::Result;
use crate::gcs::backend::{Backend, ReadHandle};
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
/// With [`Transport::Grpc`](crate::Transport::Grpc) the handle keeps one
/// stream open and multiplexes reads over it (`read_range` may be called
/// concurrently from several tasks). With
/// [`Transport::Http`](crate::Transport::Http) each read is an independent
/// request pinned to the opened generation.
///
/// ```no_run
/// use gcs_rust_fs::{ByteRange, GcsFs, GcsPath};
///
/// # async fn demo() -> gcs_rust_fs::Result<()> {
/// let fs = GcsFs::new().await?;
/// let file = fs.open(&GcsPath::parse("gs://my-bucket/data.bin")?).await?;
/// let footer = file.read_range(ByteRange::tail(64)).await?;
/// let header = file.read_range(ByteRange::head(4096)).await?;
/// println!("{} bytes total", file.size());
/// # let _ = (footer, header);
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct GcsFile {
    path: GcsPath,
    stat: ObjectStat,
    handle: ReadHandle,
}

impl GcsFile {
    pub(crate) async fn open(backend: &Backend, path: &GcsPath) -> Result<Self> {
        let (object, handle) = backend.open(path).await?;
        let stat = ObjectStat::from(object);
        let path = path
            .clone()
            .with_generation(path.generation().or(Some(stat.generation)));
        Ok(Self { path, stat, handle })
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
        let reader = self.handle.read_range(range).await?;
        reader.collect(len_hint, &self.path).await
    }
}
