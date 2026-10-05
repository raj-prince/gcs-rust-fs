//! The [`File`] contract: an opened file handle.

use std::io::SeekFrom;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use crate::options::OpenMode;
use crate::range::ByteRange;
use crate::stat::ObjectStat;

/// An opened file — the Rust counterpart of `fsspec`'s `AbstractBufferedFile`.
///
/// A handle is opened in exactly one [`OpenMode`] and, like `std::fs::File`
/// or a Python file object, is a single type for both directions: calling a
/// write method on a read handle (or vice versa) fails with
/// [`ErrorKind::Unsupported`]. Use [`readable`](Self::readable) /
/// [`writable`](Self::writable) to check first.
///
/// # Read handles
///
/// * The content is **pinned** when the handle is opened: later overwrites
///   of the same path are not observed by this handle.
/// * [`read`](Self::read) / [`seek`](Self::seek) / [`tell`](Self::tell)
///   maintain a cursor; [`read_range`](Self::read_range) is positional
///   (`pread`) and does not move the cursor, so several tasks may read
///   through a shared `&File` concurrently.
/// * Reading at or past the end yields empty bytes, never an error
///   (Python-slice semantics).
///
/// # Write handles
///
/// * Object stores have immutable objects: **nothing is visible to other
///   readers until [`close`](Self::close) succeeds**, which publishes the
///   whole file atomically. [`flush`](Self::flush) only hands buffered bytes
///   to the upload and provides back-pressure; it cannot publish partial
///   data.
/// * [`discard`](Self::discard) abandons the upload: the file is not created
///   (or, when overwriting, the previous content stays untouched). Dropping
///   an unclosed write handle has the same effect, as a safety net;
///   implementations must publish only from `close`.
/// * Writes are append-only; [`seek`](Self::seek) is unsupported.
/// * **Appendable files** (zonal buckets, and any handle opened with
///   [`OpenMode::Append`]) are the exception: the file exists as soon as the
///   handle is opened, [`flush`](Self::flush) makes the bytes written so far
///   readable by others, and [`discard`](Self::discard) cannot take them
///   back — it only stops writing.
///
/// # Lifecycle
///
/// [`close`](Self::close) and [`discard`](Self::discard) are idempotent. Any
/// other I/O on a closed handle fails with [`ErrorKind::Closed`].
///
/// [`ErrorKind::Unsupported`]: crate::ErrorKind::Unsupported
/// [`ErrorKind::Closed`]: crate::ErrorKind::Closed
#[async_trait]
pub trait File: Send + Sync {
    // ---- state ------------------------------------------------------------

    /// The path this handle was opened on, in `bucket/key` form.
    fn path(&self) -> &str;

    /// The mode this handle was opened in.
    fn mode(&self) -> OpenMode;

    /// `true` once [`close`](Self::close) or [`discard`](Self::discard) ran.
    fn closed(&self) -> bool;

    /// Current cursor position: bytes consumed so far for read handles, bytes
    /// written so far for write handles.
    fn tell(&self) -> u64;

    /// Total size in bytes. Known for read handles; for write handles `None`
    /// until the handle is closed.
    fn size(&self) -> Option<u64>;

    /// Metadata of the underlying object. Available for read handles and for
    /// write handles after a successful [`close`](Self::close).
    fn stat(&self) -> Option<&ObjectStat>;

    /// `true` if [`read`](Self::read) and [`read_range`](Self::read_range)
    /// are available.
    fn readable(&self) -> bool {
        self.mode().is_read()
    }

    /// `true` if [`write`](Self::write) is available.
    fn writable(&self) -> bool {
        self.mode().is_write()
    }

    /// `true` if [`seek`](Self::seek) is available (read handles only).
    fn seekable(&self) -> bool {
        self.mode().is_read()
    }

    // ---- reading ----------------------------------------------------------

    /// Move the cursor. Returns the new absolute position.
    ///
    /// Positions beyond the end are allowed (reads there return empty
    /// bytes); a negative resulting position is
    /// [`ErrorKind::InvalidRange`](crate::ErrorKind::InvalidRange).
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;

    /// Read up to `len` bytes from the cursor (`None` = to end of file) and
    /// advance it. Returns fewer bytes only at end of file; returns empty
    /// bytes at end of file.
    async fn read(&mut self, len: Option<usize>) -> Result<Bytes>;

    /// Positional read that leaves the cursor untouched. The range is
    /// resolved against the pinned size with Python-slice semantics.
    async fn read_range(&self, range: ByteRange) -> Result<Bytes>;

    // ---- writing ----------------------------------------------------------

    /// Append `data`. Always consumes all of `data`; waits (back-pressure)
    /// when the upload lags behind.
    async fn write(&mut self, data: Bytes) -> Result<()>;

    /// Hand all buffered bytes to the upload. Does **not** publish the file —
    /// see the trait documentation.
    async fn flush(&mut self) -> Result<()>;

    // ---- lifecycle --------------------------------------------------------

    /// Finish the handle. For write handles this finalises the upload and
    /// publishes the file; afterwards [`stat`](Self::stat) and
    /// [`size`](Self::size) describe the new object. Idempotent.
    async fn close(&mut self) -> Result<()>;

    /// Abandon the handle. For write handles the pending upload is cancelled
    /// and nothing is published (except for appendable files, see the trait
    /// docs). Idempotent; equivalent to `close` for read handles.
    ///
    /// Async and fallible so that implementations whose cancellation is a
    /// remote call can report it; the GCS implementation never fails here.
    async fn discard(&mut self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-time check that the trait is object-safe.
    fn _assert_object_safe(_: &dyn File) {}
    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn boxed_file_is_send_sync() {
        _assert_send_sync::<Box<dyn File>>();
    }
}
