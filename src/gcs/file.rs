//! [`GcsFile`]: the [`File`] implementation for Cloud Storage.

use std::io::SeekFrom;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::debug;

use crate::error::{Error, Result};
use crate::file::File;
use crate::gcs::backend::ReadHandle;
use crate::gcs::path::GcsPath;
use crate::gcs::write::Writer;
use crate::options::OpenMode;
use crate::range::{ByteRange, ResolvedRange};
use crate::stat::ObjectStat;

/// Read-ahead used by [`File::read`] when `OpenOptions::block_size` is unset
/// (fsspec's default block size).
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 5 * 1024 * 1024;

/// An opened object, returned by [`GcsFs::open`](crate::GcsFs::open).
///
/// **Read handles** pin the generation observed at open time, so every read
/// sees the same immutable bytes even if the object is overwritten
/// concurrently. [`read`](File::read) keeps a cursor and reads ahead in
/// `block_size` units; [`read_range`](File::read_range) is positional and may
/// be called concurrently through a shared reference. With
/// [`Transport::Grpc`](crate::Transport::Grpc) all reads share one bidi
/// stream; with [`Transport::Http`](crate::Transport::Http) each read is an
/// independent ranged request.
///
/// **Write handles** stream into a resumable upload that is published by
/// [`close`](File::close). On zonal buckets the object is appendable:
/// [`flush`](File::flush) persists the bytes written so far and `close`
/// finalizes the object only if
/// [`GcsFsBuilder::finalize_on_close`](crate::GcsFsBuilder::finalize_on_close)
/// was set.
#[derive(Debug)]
pub struct GcsFile {
    path: String,
    mode: OpenMode,
    closed: bool,
    state: State,
}

#[derive(Debug)]
enum State {
    Read(ReadState),
    Write(WriteState),
}

#[derive(Debug)]
struct ReadState {
    object: GcsPath,
    stat: ObjectStat,
    handle: ReadHandle,
    pos: u64,
    block_size: usize,
    /// Read-ahead buffer and the absolute offset of its first byte.
    buffer: Bytes,
    buffer_start: u64,
}

#[derive(Debug)]
struct WriteState {
    writer: Option<Writer>,
    /// Size reported by the upload once closed.
    size: Option<u64>,
    stat: Option<ObjectStat>,
}

impl GcsFile {
    pub(crate) fn reader(
        path: String,
        object: GcsPath,
        stat: ObjectStat,
        handle: ReadHandle,
        block_size: Option<usize>,
    ) -> Self {
        Self {
            path,
            mode: OpenMode::Read,
            closed: false,
            state: State::Read(ReadState {
                object,
                stat,
                handle,
                pos: 0,
                block_size: block_size.unwrap_or(DEFAULT_BLOCK_SIZE),
                buffer: Bytes::new(),
                buffer_start: 0,
            }),
        }
    }

    pub(crate) fn writer(path: String, mode: OpenMode, writer: Writer) -> Self {
        Self {
            path,
            mode,
            closed: false,
            state: State::Write(WriteState {
                writer: Some(writer),
                size: None,
                stat: None,
            }),
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(Error::closed(&self.path))
        } else {
            Ok(())
        }
    }

    fn read_state(&self) -> Result<&ReadState> {
        match &self.state {
            State::Read(r) => Ok(r),
            State::Write(_) => Err(Error::unsupported(format!(
                "{} is open for writing ({})",
                self.path,
                self.mode.as_str()
            ))),
        }
    }

    fn read_state_mut(&mut self) -> Result<&mut ReadState> {
        match &mut self.state {
            State::Read(r) => Ok(r),
            State::Write(_) => Err(Error::unsupported(format!(
                "{} is open for writing ({})",
                self.path,
                self.mode.as_str()
            ))),
        }
    }

    fn writer_mut(&mut self) -> Result<&mut Writer> {
        match &mut self.state {
            State::Write(WriteState {
                writer: Some(writer),
                ..
            }) => Ok(writer),
            State::Write(_) => Err(Error::closed(&self.path)),
            State::Read(_) => Err(Error::unsupported(format!(
                "{} is open for reading",
                self.path
            ))),
        }
    }
}

impl ReadState {
    async fn fetch(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        let ResolvedRange::Read { range, len_hint } =
            ByteRange::span(range.start, range.end).resolve(Some(self.stat.size))?
        else {
            return Ok(Bytes::new());
        };
        debug!(path = %self.object, ?range, "read");
        let reader = self.handle.read_range(range).await?;
        reader.collect(len_hint, &self.object).await
    }
}

#[async_trait]
impl File for GcsFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn mode(&self) -> OpenMode {
        self.mode
    }

    fn closed(&self) -> bool {
        self.closed
    }

    fn tell(&self) -> u64 {
        match &self.state {
            State::Read(r) => r.pos,
            State::Write(w) => w
                .writer
                .as_ref()
                .map_or(w.size.unwrap_or(0), Writer::written),
        }
    }

    fn size(&self) -> Option<u64> {
        match &self.state {
            State::Read(r) => Some(r.stat.size),
            State::Write(w) => w.size,
        }
    }

    fn stat(&self) -> Option<&ObjectStat> {
        match &self.state {
            State::Read(r) => Some(&r.stat),
            State::Write(w) => w.stat.as_ref(),
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.ensure_open()?;
        let r = self.read_state_mut()?;
        let (base, offset) = match pos {
            SeekFrom::Start(offset) => (0u64, offset as i64),
            SeekFrom::Current(offset) => (r.pos, offset),
            SeekFrom::End(offset) => (r.stat.size, offset),
        };
        let target = i128::from(base) + i128::from(offset);
        if target < 0 {
            return Err(Error::invalid_range(format!(
                "seek to {target} is before the start of the file"
            )));
        }
        r.pos = u64::try_from(target).map_err(|_| Error::invalid_range("seek overflow"))?;
        Ok(r.pos)
    }

    async fn read(&mut self, len: Option<usize>) -> Result<Bytes> {
        self.ensure_open()?;
        let r = self.read_state_mut()?;
        let remaining = r.stat.size.saturating_sub(r.pos);
        let want = match len {
            None => remaining,
            Some(n) => (n as u64).min(remaining),
        };
        if want == 0 {
            return Ok(Bytes::new());
        }
        let buffer_end = r.buffer_start + r.buffer.len() as u64;
        if r.pos >= r.buffer_start && r.pos + want <= buffer_end {
            let start = (r.pos - r.buffer_start) as usize;
            let out = r.buffer.slice(start..start + want as usize);
            r.pos += want;
            return Ok(out);
        }
        // Small reads pull in a whole block; large ones are fetched exactly.
        let fetch_len = if want < r.block_size as u64 {
            (r.block_size as u64).min(remaining)
        } else {
            want
        };
        let bytes = r.fetch(r.pos..r.pos + fetch_len).await?;
        let out = bytes.slice(0..(want as usize).min(bytes.len()));
        if fetch_len > want {
            r.buffer_start = r.pos;
            r.buffer = bytes;
        }
        r.pos += out.len() as u64;
        Ok(out)
    }

    async fn read_range(&self, range: ByteRange) -> Result<Bytes> {
        self.ensure_open()?;
        let r = self.read_state()?;
        r.fetch(range.clamp(r.stat.size)).await
    }

    async fn write(&mut self, data: Bytes) -> Result<()> {
        self.ensure_open()?;
        self.writer_mut()?.write(data).await
    }

    async fn flush(&mut self) -> Result<()> {
        self.ensure_open()?;
        match &mut self.state {
            State::Read(_) => Ok(()),
            State::Write(_) => self.writer_mut()?.flush().await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if let State::Write(w) = &mut self.state {
            if let Some(writer) = w.writer.take() {
                let finished = writer.close().await?;
                w.size = Some(finished.size);
                w.stat = finished.object.map(ObjectStat::from);
            }
        }
        self.closed = true;
        Ok(())
    }

    async fn discard(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if let State::Write(w) = &mut self.state {
            if let Some(writer) = w.writer.take() {
                writer.discard();
            }
        }
        self.closed = true;
        Ok(())
    }
}

/// Safety net for handles that were neither closed nor discarded: the
/// upload is abandoned exactly as by [`File::discard`], so nothing is
/// published (an appendable object keeps the bytes persisted so far).
impl Drop for GcsFile {
    fn drop(&mut self) {
        if let State::Write(w) = &mut self.state {
            if let Some(writer) = w.writer.take() {
                writer.discard();
            }
        }
    }
}
