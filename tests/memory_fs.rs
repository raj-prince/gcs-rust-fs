//! The derived `FileSystem` operations exercised against an in-memory object
//! store that implements only the six required primitives.
//!
//! This doubles as a check that the public contract is sufficient for an
//! implementation written outside the crate: everything below uses only
//! `gcs_rust_fs`'s public API.

use std::collections::{BTreeMap, BTreeSet};
use std::io::SeekFrom;
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::{Bytes, BytesMut};
use gcs_rust_fs::{
    async_trait, BulkOptions, ByteRange, CopyOptions, DuOptions, Entry, Error, ErrorKind, File,
    FileSystem, FindOptions, GlobOptions, ListOptions, MkdirOptions, ObjectStat, OnError, OpenMode,
    OpenOptions, PutOptions, ReadOptions, Result, RmOptions, WalkOptions, WriteMode, WriteOptions,
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
struct MemoryFs {
    store: Arc<Mutex<Store>>,
}

impl MemoryFs {
    fn with_files(files: &[(&str, &str)]) -> Self {
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

fn fixture() -> MemoryFs {
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

fn paths(entries: &[Entry]) -> Vec<&str> {
    entries.iter().map(|e| e.path.as_str()).collect()
}

fn text(bytes: &Bytes) -> &str {
    std::str::from_utf8(bytes).unwrap()
}

async fn files_below(fs: &MemoryFs, path: &str) -> Vec<String> {
    fs.find(path, FindOptions::default())
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect()
}

async fn glob_paths(fs: &MemoryFs, pattern: &str) -> Vec<String> {
    fs.glob(pattern, GlobOptions::default())
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect()
}

fn recursive() -> CopyOptions {
    CopyOptions {
        recursive: true,
        ..Default::default()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[tokio::test]
async fn info_exists_is_file_is_dir_size() {
    let fs = fixture();
    assert!(fs.exists("b").await.unwrap());
    assert!(fs.is_dir("b").await.unwrap());
    assert!(fs.is_dir("b/data").await.unwrap());
    assert!(fs.is_file("b/data/a.parquet").await.unwrap());
    assert!(!fs.is_file("b/data").await.unwrap());
    assert!(!fs.is_dir("b/data/a.parquet").await.unwrap());
    assert!(!fs.exists("b/nope").await.unwrap());
    assert!(!fs.exists("nobucket").await.unwrap());

    assert_eq!(fs.size("b/data/a.parquet").await.unwrap(), 4);
    assert_eq!(fs.size("b/data").await.unwrap(), 0);
    assert_eq!(
        fs.size("b/nope").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    // Paths are normalised: scheme, trailing slash, generation suffix.
    let info = fs.info("gs://b/data/a.parquet#1").await.unwrap();
    assert_eq!(info.path, "b/data/a.parquet");
    assert!(info.is_file());
    assert_eq!(info.stat.as_ref().unwrap().name, "data/a.parquet");
    assert_eq!(fs.info("b/data/").await.unwrap().path, "b/data");

    // The trait is usable through a trait object.
    let dynamic: Arc<dyn FileSystem> = Arc::new(fs);
    assert!(dynamic.exists("other/o.txt").await.unwrap());
}

#[tokio::test]
async fn find_walks_breadth_first_and_sorts() {
    let fs = fixture();
    assert_eq!(
        files_below(&fs, "b/data").await,
        [
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested/c.csv",
            "b/data/nested/deep/d.parquet",
        ]
    );

    let with_dirs = fs
        .find(
            "b/data",
            FindOptions {
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        paths(&with_dirs),
        [
            "b/data",
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested",
            "b/data/nested/c.csv",
            "b/data/nested/deep",
            "b/data/nested/deep/d.parquet",
        ]
    );

    let shallow = fs
        .find(
            "b/data",
            FindOptions {
                maxdepth: Some(1),
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        paths(&shallow),
        [
            "b/data",
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested"
        ]
    );

    // A file finds itself; a missing path finds nothing.
    assert_eq!(files_below(&fs, "b/root.txt").await, ["b/root.txt"]);
    assert!(files_below(&fs, "b/missing").await.is_empty());

    // The root spans buckets and is never included itself.
    let everything = fs
        .find(
            "",
            FindOptions {
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(everything.iter().filter(|e| e.is_file()).count(), 7);
    assert!(everything.iter().all(|e| !e.path.is_empty()));

    assert_eq!(
        fs.find(
            "b",
            FindOptions {
                maxdepth: Some(0),
                ..Default::default()
            }
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::InvalidRange
    );
}

#[tokio::test]
async fn walk_groups_one_listing_per_directory() {
    let fs = fixture();
    let walk = fs.walk("b/data", WalkOptions::default()).await.unwrap();
    let dirs: Vec<&str> = walk.iter().map(|w| w.dir.as_str()).collect();
    assert_eq!(dirs, ["b/data", "b/data/nested", "b/data/nested/deep"]);
    assert_eq!(paths(&walk[0].dirs), ["b/data/nested"]);
    assert_eq!(
        paths(&walk[0].files),
        ["b/data/a.parquet", "b/data/b.parquet"]
    );
    assert_eq!(paths(&walk[1].dirs), ["b/data/nested/deep"]);
    assert_eq!(paths(&walk[1].files), ["b/data/nested/c.csv"]);
    assert!(walk[2].dirs.is_empty());
    assert_eq!(paths(&walk[2].files), ["b/data/nested/deep/d.parquet"]);

    let limited = fs
        .walk("b/data", WalkOptions { maxdepth: Some(1) })
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(paths(&limited[0].dirs), ["b/data/nested"]);

    assert!(fs
        .walk("b/root.txt", WalkOptions::default())
        .await
        .unwrap()
        .is_empty());
    assert!(fs
        .walk("b/missing", WalkOptions::default())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn du_sums_file_sizes() {
    let fs = fixture();
    let du = fs.du("b/data", DuOptions::default()).await.unwrap();
    assert_eq!(du.total, 4 + 2 + 1 + 6);
    assert_eq!(du.sizes.len(), 4);

    let with_dirs = fs
        .du(
            "b/data",
            DuOptions {
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(with_dirs.total, 13);
    assert_eq!(with_dirs.sizes.len(), 7);
    assert!(with_dirs.sizes.contains(&("b/data/nested".to_string(), 0)));
}

#[tokio::test]
async fn glob_follows_fsspec_rules() {
    let fs = fixture();
    assert_eq!(
        glob_paths(&fs, "b/data/*.parquet").await,
        ["b/data/a.parquet", "b/data/b.parquet"]
    );
    // `*` never crosses a separator; `**` may match zero levels.
    assert_eq!(
        glob_paths(&fs, "b/data/**/*.parquet").await,
        [
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested/deep/d.parquet",
        ]
    );
    assert_eq!(
        glob_paths(&fs, "b/*").await,
        ["b/data", "b/logs", "b/root.txt"]
    );
    // Trailing slash: directories only.
    assert_eq!(glob_paths(&fs, "b/*/").await, ["b/data", "b/logs"]);
    // A trailing `**` matches everything below, not the root itself.
    assert_eq!(glob_paths(&fs, "b/**").await.len(), 11);
    // Wildcard in the first segment searches across buckets.
    assert_eq!(glob_paths(&fs, "*/root.txt").await, ["b/root.txt"]);
    assert_eq!(
        glob_paths(&fs, "b/data/nested/?.csv").await,
        ["b/data/nested/c.csv"]
    );
    // No wildcard: the entry itself, if it exists.
    assert_eq!(
        glob_paths(&fs, "b/data/a.parquet").await,
        ["b/data/a.parquet"]
    );
    assert!(glob_paths(&fs, "b/data/zzz").await.is_empty());
    assert!(glob_paths(&fs, "b/data/a.parquet/").await.is_empty());
}

#[tokio::test]
async fn cat_and_cat_ranges_preserve_order_and_honour_on_error() {
    let fs = fixture();
    let out = fs
        .cat(&["b/data/b.parquet", "b/root.txt"], BulkOptions::default())
        .await
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, "b/data/b.parquet");
    assert_eq!(text(out[0].1.as_ref().unwrap()), "bb");
    assert_eq!(text(out[1].1.as_ref().unwrap()), "root");

    let err = fs
        .cat(&["b/root.txt", "b/missing"], BulkOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let ignored = fs
        .cat(
            &["b/missing", "b/root.txt"],
            BulkOptions {
                on_error: OnError::Ignore,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(ignored.len(), 1);
    assert_eq!(ignored[0].0, "b/root.txt");

    let returned = fs
        .cat(
            &["b/missing", "b/root.txt"],
            BulkOptions {
                on_error: OnError::Return,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(returned.len(), 2);
    assert_eq!(
        returned[0].1.as_ref().unwrap_err().kind(),
        ErrorKind::NotFound
    );

    let ranges = fs
        .cat_ranges(
            &[
                ("b/data/a.parquet", ByteRange::head(2)),
                ("b/data/nested/deep/d.parquet", ByteRange::tail(3)),
                ("b/missing", ByteRange::ALL),
                ("b/data/b.parquet", ByteRange::new(Some(-1), None)),
            ],
            BulkOptions {
                on_error: OnError::Return,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(text(ranges[0].as_ref().unwrap()), "aa");
    assert_eq!(text(ranges[1].as_ref().unwrap()), "ddd");
    assert_eq!(ranges[2].as_ref().unwrap_err().kind(), ErrorKind::NotFound);
    assert_eq!(text(ranges[3].as_ref().unwrap()), "b");

    assert_eq!(
        fs.cat_file("b/data", ByteRange::ALL, ReadOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );
}

#[tokio::test]
async fn file_handle_contract() {
    let fs = fixture();

    // Reading: cursor, seek, positional reads, EOF, closed state.
    let mut f = fs
        .open("b/data/nested/deep/d.parquet", OpenOptions::read())
        .await
        .unwrap();
    assert_eq!(f.mode(), OpenMode::Read);
    assert!(f.readable() && f.seekable() && !f.writable());
    assert_eq!(f.size(), Some(6));
    assert_eq!(f.tell(), 0);
    assert_eq!(text(&f.read(Some(2)).await.unwrap()), "dd");
    assert_eq!(f.tell(), 2);
    assert_eq!(f.seek(SeekFrom::End(-1)).unwrap(), 5);
    assert_eq!(f.read(None).await.unwrap().len(), 1);
    assert!(f.read(None).await.unwrap().is_empty());
    assert_eq!(
        text(&f.read_range(ByteRange::span(1, 3)).await.unwrap()),
        "dd"
    );
    assert_eq!(f.tell(), 6, "read_range must not move the cursor");
    assert_eq!(
        f.seek(SeekFrom::Current(-100)).unwrap_err().kind(),
        ErrorKind::InvalidRange
    );
    assert_eq!(
        f.write(Bytes::new()).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    f.close().await.unwrap();
    assert!(f.closed());
    assert_eq!(f.read(None).await.unwrap_err().kind(), ErrorKind::Closed);
    f.close().await.unwrap(); // idempotent

    // Writing: nothing is visible until close.
    let mut w = fs.open("b/w.txt", OpenOptions::write()).await.unwrap();
    assert!(w.writable() && !w.readable());
    w.write(Bytes::from_static(b"ab")).await.unwrap();
    w.write(Bytes::from_static(b"c")).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(w.tell(), 3);
    assert_eq!(w.size(), None);
    assert!(!fs.exists("b/w.txt").await.unwrap());
    assert_eq!(
        w.seek(SeekFrom::Start(0)).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    w.close().await.unwrap();
    assert_eq!(w.size(), Some(3));
    assert_eq!(w.stat().unwrap().size, 3);
    assert_eq!(
        text(
            &fs.cat_file("b/w.txt", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "abc"
    );

    // Discard publishes nothing and is idempotent.
    let mut d = fs.open("b/d.txt", OpenOptions::write()).await.unwrap();
    d.write(Bytes::from_static(b"zzz")).await.unwrap();
    d.discard().await.unwrap();
    assert!(d.closed());
    d.discard().await.unwrap();
    assert_eq!(
        d.write(Bytes::from_static(b"x")).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
    assert!(!fs.exists("b/d.txt").await.unwrap());

    // Dropping an unclosed write handle publishes nothing either.
    let mut d = fs.open("b/d2.txt", OpenOptions::write()).await.unwrap();
    d.write(Bytes::from_static(b"zzz")).await.unwrap();
    drop(d);
    assert!(!fs.exists("b/d2.txt").await.unwrap());

    // Mode errors.
    async fn open_err(fs: &MemoryFs, path: &str, opts: OpenOptions) -> ErrorKind {
        match fs.open(path, opts).await {
            Ok(_) => panic!("open({path}) unexpectedly succeeded"),
            Err(e) => e.kind(),
        }
    }
    assert_eq!(
        open_err(&fs, "b/a.txt", OpenOptions::with_mode(OpenMode::Append)).await,
        ErrorKind::Unsupported
    );
    assert_eq!(
        open_err(&fs, "b/data", OpenOptions::read()).await,
        ErrorKind::IsADirectory
    );
    assert_eq!(
        open_err(&fs, "b/missing", OpenOptions::read()).await,
        ErrorKind::NotFound
    );
    assert_eq!(
        open_err(&fs, "b/w.txt", OpenOptions::with_mode(OpenMode::CreateNew)).await,
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        open_err(&fs, "nobucket/x", OpenOptions::write()).await,
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn pipe_put_get_round_trip() {
    let fs = fixture();
    fs.pipe_file(
        "b/new/file.bin",
        Bytes::from_static(b"hello"),
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        text(
            &fs.cat_file("b/new/file.bin", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "hello"
    );
    assert!(fs.is_dir("b/new").await.unwrap());

    let err = fs
        .pipe_file(
            "b/new/file.bin",
            Bytes::new(),
            WriteOptions {
                mode: WriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    assert_eq!(
        fs.pipe_file("nobucket/x", Bytes::new(), WriteOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );

    // Local round trip through put / put_file / get_file.
    let dir = std::env::temp_dir().join(format!("gcs-rust-fs-test-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(dir.join("sub")).await.unwrap();
    tokio::fs::write(dir.join("one.txt"), b"1").await.unwrap();
    tokio::fs::write(dir.join("sub/two.txt"), b"22")
        .await
        .unwrap();
    let dir_name = dir.file_name().unwrap().to_str().unwrap().to_string();
    let put_recursive = || PutOptions {
        recursive: true,
        ..Default::default()
    };

    assert_eq!(
        fs.put(&dir, "b/upload", PutOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );

    // New destination: the tree lands at `dst`.
    fs.put(&dir, "b/upload", put_recursive()).await.unwrap();
    assert_eq!(
        files_below(&fs, "b/upload").await,
        ["b/upload/one.txt", "b/upload/sub/two.txt"]
    );
    assert_eq!(
        text(
            &fs.cat_file(
                "b/upload/sub/two.txt",
                ByteRange::ALL,
                ReadOptions::default()
            )
            .await
            .unwrap()
        ),
        "22"
    );

    // Trailing separator on the source: copy the *contents*.
    let with_sep = format!("{}{}", dir.display(), std::path::MAIN_SEPARATOR);
    fs.put(Path::new(&with_sep), "b/upload2/", put_recursive())
        .await
        .unwrap();
    assert_eq!(
        files_below(&fs, "b/upload2").await,
        ["b/upload2/one.txt", "b/upload2/sub/two.txt"]
    );

    // Existing destination directory, no trailing separator: nest under the
    // source's name.
    fs.put(&dir, "b/upload2", put_recursive()).await.unwrap();
    assert!(fs
        .exists(&format!("b/upload2/{dir_name}/sub/two.txt"))
        .await
        .unwrap());

    // Single files.
    fs.put_file(
        &dir.join("one.txt"),
        "b/single.txt",
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(fs.size("b/single.txt").await.unwrap(), 1);
    fs.put(&dir.join("one.txt"), "b/dest/", PutOptions::default())
        .await
        .unwrap();
    assert!(fs.is_file("b/dest/one.txt").await.unwrap());

    let target = dir.join("out/nested/got.txt");
    fs.get_file("b/upload/sub/two.txt", &target, ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"22");
    assert_eq!(
        fs.get_file("b/missing", &target, ReadOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );

    tokio::fs::remove_dir_all(&dir).await.unwrap();
}

#[tokio::test]
async fn rm_semantics() {
    let fs = fixture();
    assert_eq!(
        fs.rm("b/data", RmOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(
        fs.rm("b/missing", RmOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    fs.rm("b/root.txt", RmOptions::default()).await.unwrap();
    assert!(!fs.exists("b/root.txt").await.unwrap());

    fs.rm(
        "b/data",
        RmOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("b/data").await.unwrap());
    assert!(fs.exists("b/logs/2024/x.log").await.unwrap());

    // A bucket is removed along with its content.
    fs.rm(
        "other",
        RmOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("other").await.unwrap());
    assert_eq!(
        paths(&fs.ls("", ListOptions::default()).await.unwrap()),
        ["b"]
    );

    // maxdepth limits how deep the delete reaches.
    let fs = fixture();
    fs.rm(
        "b/data",
        RmOptions {
            recursive: true,
            maxdepth: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("b/data/a.parquet").await.unwrap());
    assert!(fs.exists("b/data/nested/c.csv").await.unwrap());
    assert!(fs.is_dir("b/data").await.unwrap());
}

#[tokio::test]
async fn mkdir_rmdir_and_placeholders() {
    let fs = MemoryFs::default();
    fs.mkdir("bkt", MkdirOptions::default()).await.unwrap();
    assert!(fs.is_dir("bkt").await.unwrap());
    assert_eq!(
        fs.mkdir("bkt", MkdirOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );

    // Nested directories are implicit; parents are only created on request.
    assert_eq!(
        fs.mkdir("nob/dir", MkdirOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    fs.mkdir(
        "nob/dir",
        MkdirOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(fs.is_dir("nob").await.unwrap());
    assert!(!fs.exists("nob/dir").await.unwrap());

    // A placeholder makes an empty directory exist.
    fs.mkdir(
        "bkt/empty",
        MkdirOptions {
            placeholder: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(fs.is_dir("bkt/empty").await.unwrap());
    assert!(fs.info("bkt/empty").await.unwrap().stat.is_some());
    assert!(fs
        .ls("bkt/empty", ListOptions::default())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        paths(&fs.ls("bkt", ListOptions::default()).await.unwrap()),
        ["bkt/empty"]
    );
    assert_eq!(
        fs.rmdir("bkt").await.unwrap_err().kind(),
        ErrorKind::DirectoryNotEmpty
    );

    fs.pipe_file(
        "bkt/f.txt",
        Bytes::from_static(b"f"),
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        fs.rmdir("bkt/f.txt").await.unwrap_err().kind(),
        ErrorKind::NotADirectory
    );
    fs.rm_file("bkt/f.txt").await.unwrap();

    fs.rm(
        "bkt/empty",
        RmOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("bkt/empty").await.unwrap());
    fs.rmdir("bkt").await.unwrap();
    assert!(!fs.exists("bkt").await.unwrap());
    assert_eq!(
        fs.rmdir("bkt").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn copy_destination_rules() {
    let fs = fixture();

    // File to a new name.
    fs.copy("b/root.txt", "b/copy.txt", CopyOptions::default())
        .await
        .unwrap();
    assert_eq!(
        text(
            &fs.cat_file("b/copy.txt", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "root"
    );
    // File into an existing directory, or into a `dst/`.
    fs.copy("b/root.txt", "b/logs", CopyOptions::default())
        .await
        .unwrap();
    assert!(fs.is_file("b/logs/root.txt").await.unwrap());
    fs.copy("b/root.txt", "b/fresh/", CopyOptions::default())
        .await
        .unwrap();
    assert!(fs.is_file("b/fresh/root.txt").await.unwrap());

    // Directories need `recursive`.
    assert_eq!(
        fs.copy("b/data", "b/dst", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );

    // Directory to a new destination: contents land at `dst/...`.
    fs.copy("b/data", "b/dst", recursive()).await.unwrap();
    assert_eq!(
        files_below(&fs, "b/dst").await,
        [
            "b/dst/a.parquet",
            "b/dst/b.parquet",
            "b/dst/nested/c.csv",
            "b/dst/nested/deep/d.parquet",
        ]
    );
    // Directory into an existing directory: nests under its own name ...
    fs.copy("b/data", "b/dst", recursive()).await.unwrap();
    assert!(fs.is_file("b/dst/data/a.parquet").await.unwrap());
    // ... unless the source has a trailing slash (copy the contents).
    fs.copy("b/data/", "b/logs", recursive()).await.unwrap();
    assert!(fs.is_file("b/logs/a.parquet").await.unwrap());
    assert!(!fs.exists("b/logs/data").await.unwrap());
    // Across buckets.
    fs.copy("b/data", "other/", recursive()).await.unwrap();
    assert!(fs.is_file("other/data/nested/c.csv").await.unwrap());
    // maxdepth.
    fs.copy(
        "b/data",
        "b/shallow",
        CopyOptions {
            maxdepth: Some(1),
            ..recursive()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        files_below(&fs, "b/shallow").await,
        ["b/shallow/a.parquet", "b/shallow/b.parquet"]
    );

    assert_eq!(
        fs.copy("b/missing", "b/x", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        fs.copy("b/root.txt", "nobucket/x", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn mv_moves_and_cleans_up_the_source() {
    let fs = fixture();
    fs.mv("b/root.txt", "b/moved.txt", CopyOptions::default())
        .await
        .unwrap();
    assert!(!fs.exists("b/root.txt").await.unwrap());
    assert_eq!(
        text(
            &fs.cat_file("b/moved.txt", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "root"
    );

    assert_eq!(
        fs.mv("b/data", "b/renamed", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );
    fs.mv("b/data", "b/renamed", recursive()).await.unwrap();
    assert!(!fs.exists("b/data").await.unwrap());
    assert_eq!(
        files_below(&fs, "b/renamed").await,
        [
            "b/renamed/a.parquet",
            "b/renamed/b.parquet",
            "b/renamed/nested/c.csv",
            "b/renamed/nested/deep/d.parquet",
        ]
    );

    // Moving a whole bucket's content leaves the (now empty) bucket removed.
    fs.mv("other", "b/other-copy", recursive()).await.unwrap();
    assert!(!fs.exists("other").await.unwrap());
    assert!(fs.is_file("b/other-copy/o.txt").await.unwrap());

    assert_eq!(
        fs.mv("b/nope", "b/x", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}
