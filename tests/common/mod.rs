//! Shared test support: an in-memory object store implementing only the six
//! required [`FileSystem`] primitives, with GCS-style directory emulation.
//!
//! Written entirely against `gcs_rust_fs`'s public API, so it doubles as a
//! check that the contract is sufficient for an implementation outside the
//! crate. Each integration test binary compiles this module separately and
//! uses a different subset of it.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::SeekFrom;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::{Bytes, BytesMut};
use gcs_rust_fs::{
    async_trait, ByteRange, Entry, Error, ErrorKind, File, FileSystem, ListOptions, MkdirOptions,
    ObjectStat, OpenMode, OpenOptions, Result,
};

// ===========================================================================
// In-memory object store with GCS-style directory emulation
// ===========================================================================

#[derive(Default)]
struct Store {
    buckets: BTreeSet<String>,
    objects: BTreeMap<String, Bytes>,
}

impl Store {
    /// Keys starting with `prefix` (which must end in `/`), in order.
    fn below<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = (&'a String, &'a Bytes)> + 'a {
        self.objects
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(move |(k, _)| k.starts_with(prefix))
    }

    fn is_dir(&self, path: &str) -> bool {
        if path.is_empty() {
            return true;
        }
        if !path.contains('/') {
            return self.buckets.contains(path);
        }
        self.below(&format!("{path}/")).next().is_some()
    }

    /// Anything strictly below `path`, not counting its own placeholder.
    fn has_children(&self, path: &str) -> bool {
        let prefix = format!("{path}/");
        let found = self.below(&prefix).any(|(k, _)| k.len() > prefix.len());
        found
    }
}

fn normalize(path: &str) -> String {
    let p = path.strip_prefix("gs://").unwrap_or(path);
    let p = p.split_once('#').map_or(p, |(p, _)| p);
    p.trim_matches('/').to_string()
}

fn stat_for(key: &str, data: &Bytes) -> ObjectStat {
    let (bucket, name) = key.split_once('/').unwrap_or((key, ""));
    let mut stat = ObjectStat::default();
    stat.bucket = bucket.to_string();
    stat.name = name.to_string();
    stat.size = data.len() as u64;
    stat.generation = 1;
    stat
}

#[derive(Clone, Default)]
pub struct MemoryFs {
    store: Arc<Mutex<Store>>,
}

impl MemoryFs {
    pub fn with_files(files: &[(&str, &str)]) -> Self {
        let fs = Self::default();
        {
            let mut s = fs.lock();
            for (key, content) in files {
                let bucket = key.split('/').next().unwrap_or_default();
                s.buckets.insert(bucket.to_string());
                s.objects
                    .insert(key.to_string(), Bytes::copy_from_slice(content.as_bytes()));
            }
        }
        fs
    }

    fn lock(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap()
    }
}

#[async_trait]
impl FileSystem for MemoryFs {
    async fn info(&self, path: &str) -> Result<Entry> {
        let path = normalize(path);
        let s = self.lock();
        if let Some(data) = s.objects.get(&path) {
            return Ok(Entry::file(path.clone(), stat_for(&path, data)));
        }
        if !s.is_dir(&path) {
            return Err(Error::not_found(&path));
        }
        let placeholder = format!("{path}/");
        let entry = Entry::directory(path);
        Ok(match s.objects.get(&placeholder) {
            Some(data) => entry.with_stat(stat_for(&placeholder, data)),
            None => entry,
        })
    }

    async fn ls(&self, path: &str, _opts: ListOptions) -> Result<Vec<Entry>> {
        let path = normalize(path);
        let s = self.lock();
        if path.is_empty() {
            return Ok(s.buckets.iter().map(Entry::directory).collect());
        }
        if let Some(data) = s.objects.get(&path) {
            return Ok(vec![Entry::file(path.clone(), stat_for(&path, data))]);
        }
        if !s.is_dir(&path) {
            return Err(Error::not_found(&path));
        }
        let prefix = format!("{path}/");
        let mut out = Vec::new();
        let mut dirs = BTreeSet::new();
        for (key, data) in s.below(&prefix) {
            let rest = &key[prefix.len()..];
            match rest.split_once('/') {
                // The directory's own placeholder object.
                _ if rest.is_empty() => {}
                Some((dir, _)) => {
                    if dirs.insert(dir.to_string()) {
                        out.push(Entry::directory(format!("{prefix}{dir}")));
                    }
                }
                None => out.push(Entry::file(key.clone(), stat_for(key, data))),
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>> {
        let path = normalize(path);
        let s = self.lock();
        match opts.mode {
            OpenMode::Read => match s.objects.get(&path) {
                Some(data) => Ok(Box::new(MemoryFile::reader(path.clone(), data.clone()))),
                None if s.is_dir(&path) => Err(Error::is_a_directory(&path)),
                None => Err(Error::not_found(&path)),
            },
            OpenMode::Append => Err(Error::unsupported("append mode")),
            OpenMode::Write | OpenMode::CreateNew => {
                let Some((bucket, _)) = path.split_once('/') else {
                    return Err(Error::is_a_directory(&path));
                };
                if !s.buckets.contains(bucket) {
                    return Err(Error::not_found(bucket));
                }
                if opts.mode == OpenMode::CreateNew && s.objects.contains_key(&path) {
                    return Err(Error::already_exists(&path));
                }
                if s.is_dir(&path) {
                    return Err(Error::is_a_directory(&path));
                }
                Ok(Box::new(MemoryFile::writer(
                    path.clone(),
                    opts.mode,
                    self.store.clone(),
                )))
            }
        }
    }

    async fn rm_file(&self, path: &str) -> Result<()> {
        let path = normalize(path);
        let mut s = self.lock();
        if s.objects.remove(&path).is_some() {
            Ok(())
        } else if s.is_dir(&path) {
            Err(Error::is_a_directory(&path))
        } else {
            Err(Error::not_found(&path))
        }
    }

    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<()> {
        let path = normalize(path);
        let mut s = self.lock();
        let bucket = path.split('/').next().unwrap_or_default().to_string();
        if bucket.is_empty() {
            return Err(Error::new(ErrorKind::InvalidPath, "mkdir needs a path"));
        }
        if bucket == path {
            return if s.buckets.insert(bucket) {
                Ok(())
            } else {
                Err(Error::already_exists(&path))
            };
        }
        if !s.buckets.contains(&bucket) {
            if !opts.create_parents {
                return Err(Error::not_found(&bucket));
            }
            s.buckets.insert(bucket);
        }
        if opts.placeholder {
            if s.objects.contains_key(&path) || s.is_dir(&path) {
                return Err(Error::already_exists(&path));
            }
            s.objects.insert(format!("{path}/"), Bytes::new());
        }
        Ok(())
    }

    async fn rmdir(&self, path: &str) -> Result<()> {
        let path = normalize(path);
        let mut s = self.lock();
        if s.objects.contains_key(&path) {
            return Err(Error::not_a_directory(&path));
        }
        if !s.is_dir(&path) {
            return Err(Error::not_found(&path));
        }
        if s.has_children(&path) {
            return Err(Error::directory_not_empty(&path));
        }
        if path.contains('/') {
            s.objects.remove(&format!("{path}/"));
        } else {
            s.buckets.remove(&path);
        }
        Ok(())
    }
}

struct MemoryFile {
    path: String,
    mode: OpenMode,
    data: Bytes,
    pending: BytesMut,
    pos: u64,
    closed: bool,
    stat: Option<ObjectStat>,
    store: Option<Arc<Mutex<Store>>>,
}

impl MemoryFile {
    fn reader(path: String, data: Bytes) -> Self {
        let stat = stat_for(&path, &data);
        Self {
            path,
            mode: OpenMode::Read,
            data,
            pending: BytesMut::new(),
            pos: 0,
            closed: false,
            stat: Some(stat),
            store: None,
        }
    }

    fn writer(path: String, mode: OpenMode, store: Arc<Mutex<Store>>) -> Self {
        Self {
            path,
            mode,
            data: Bytes::new(),
            pending: BytesMut::new(),
            pos: 0,
            closed: false,
            stat: None,
            store: Some(store),
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(Error::closed(&self.path))
        } else {
            Ok(())
        }
    }

    fn ensure_readable(&self) -> Result<()> {
        self.ensure_open()?;
        if self.readable() {
            Ok(())
        } else {
            Err(Error::unsupported("read on a write handle"))
        }
    }
}

#[async_trait]
impl File for MemoryFile {
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
        self.pos
    }

    fn size(&self) -> Option<u64> {
        self.stat.as_ref().map(|s| s.size)
    }

    fn stat(&self) -> Option<&ObjectStat> {
        self.stat.as_ref()
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.ensure_open()?;
        if !self.seekable() {
            return Err(Error::unsupported("seek on a write handle"));
        }
        let len = self.data.len() as i64;
        let target = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(delta) => len + delta,
            SeekFrom::Current(delta) => self.pos as i64 + delta,
        };
        if target < 0 {
            return Err(Error::invalid_range("seek before the start of the file"));
        }
        self.pos = target as u64;
        Ok(self.pos)
    }

    async fn read(&mut self, len: Option<usize>) -> Result<Bytes> {
        self.ensure_readable()?;
        let start = (self.pos as usize).min(self.data.len());
        let end = len.map_or(self.data.len(), |n| {
            start.saturating_add(n).min(self.data.len())
        });
        self.pos = end as u64;
        Ok(self.data.slice(start..end))
    }

    async fn read_range(&self, range: ByteRange) -> Result<Bytes> {
        self.ensure_readable()?;
        let r = range.clamp(self.data.len() as u64);
        Ok(self.data.slice(r.start as usize..r.end as usize))
    }

    async fn write(&mut self, data: Bytes) -> Result<()> {
        self.ensure_open()?;
        if !self.writable() {
            return Err(Error::unsupported("write on a read handle"));
        }
        self.pos += data.len() as u64;
        self.pending.extend_from_slice(&data);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.ensure_open()
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if let Some(store) = self.store.take() {
            let data = self.pending.split().freeze();
            let mut s = store.lock().unwrap();
            if self.mode == OpenMode::CreateNew && s.objects.contains_key(&self.path) {
                return Err(Error::already_exists(&self.path));
            }
            self.stat = Some(stat_for(&self.path, &data));
            s.objects.insert(self.path.clone(), data);
        }
        Ok(())
    }

    async fn discard(&mut self) -> Result<()> {
        self.closed = true;
        self.store = None;
        self.pending.clear();
        Ok(())
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

pub fn fixture() -> MemoryFs {
    MemoryFs::with_files(&[
        ("b/root.txt", "root"),
        ("b/data/a.parquet", "aaaa"),
        ("b/data/b.parquet", "bb"),
        ("b/data/nested/c.csv", "c"),
        ("b/data/nested/deep/d.parquet", "dddddd"),
        ("b/logs/2024/x.log", "x"),
        ("other/o.txt", "o"),
    ])
}

pub fn paths(entries: &[Entry]) -> Vec<&str> {
    entries.iter().map(|e| e.path.as_str()).collect()
}

pub fn text(bytes: &Bytes) -> &str {
    std::str::from_utf8(bytes).unwrap()
}
