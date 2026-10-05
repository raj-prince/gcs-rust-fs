//! Generic implementations of the overridable and derived
//! [`FileSystem`] methods, expressed purely in terms of the required
//! primitives (`info`, `ls`, `open`, `rm_file`, `mkdir`, `rmdir`).
//!
//! Everything here is storage-agnostic and encodes `fsspec` semantics; see
//! the trait documentation for the rules each function follows.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::entry::{self, DiskUsage, Entry, WalkEntry};
use crate::error::{Error, ErrorKind, Result};
use crate::file::File;
use crate::filesystem::FileSystem;
use crate::glob;
use crate::options::{
    BulkOptions, CopyOptions, DuOptions, FindOptions, GlobOptions, ListOptions, OnError,
    OpenOptions, PutOptions, RmOptions, WalkOptions, WriteOptions,
};
use crate::range::ByteRange;

/// Chunk size used when streaming bytes through the client.
const IO_CHUNK: usize = 8 << 20;

fn invalid_maxdepth() -> Error {
    Error::new(ErrorKind::InvalidRange, "maxdepth must be at least 1")
}

fn not_found_is_ok(result: Result<()>) -> Result<()> {
    match result {
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// `info` that turns `NotFound` into `None`.
async fn try_info<F: FileSystem + ?Sized>(fs: &F, path: &str) -> Result<Option<Entry>> {
    match fs.info(path).await {
        Ok(entry) => Ok(Some(entry)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// =========================================================================
// Overridable primitives
// =========================================================================

pub(crate) async fn find<F: FileSystem + ?Sized>(
    fs: &F,
    path: &str,
    opts: FindOptions,
) -> Result<Vec<Entry>> {
    if opts.maxdepth == Some(0) {
        return Err(invalid_maxdepth());
    }
    let root = match try_info(fs, path).await? {
        None => return Ok(Vec::new()),
        Some(entry) if entry.is_file() => return Ok(vec![entry]),
        Some(entry) => entry,
    };
    let list_opts = ListOptions {
        versions: opts.versions,
    };
    let mut out = Vec::new();
    if opts.withdirs && !root.path.is_empty() {
        out.push(root.clone());
    }
    let mut queue = VecDeque::from([(root.path.clone(), 1usize)]);
    while let Some((dir, depth)) = queue.pop_front() {
        let children = match fs.ls(&dir, list_opts.clone()).await {
            Ok(children) => children,
            // The directory vanished between listing its parent and now.
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for child in children {
            // Guard against listings that echo the directory itself.
            if entry::relative(&dir, &child.path).is_none_or(str::is_empty) {
                continue;
            }
            if child.is_dir() {
                if opts.maxdepth.is_none_or(|max| depth < max) {
                    queue.push_back((child.path.clone(), depth + 1));
                }
                if opts.withdirs {
                    out.push(child);
                }
            } else {
                out.push(child);
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out.dedup_by(|a, b| a.path == b.path);
    Ok(out)
}

pub(crate) async fn cat_file<F: FileSystem + ?Sized>(
    fs: &F,
    path: &str,
    range: ByteRange,
) -> Result<Bytes> {
    let file = fs.open(path, OpenOptions::read()).await?;
    file.read_range(range).await
}

pub(crate) async fn pipe_file<F: FileSystem + ?Sized>(
    fs: &F,
    path: &str,
    data: Bytes,
    opts: WriteOptions,
) -> Result<()> {
    let mut file = fs.open(path, OpenOptions::from_write(opts)).await?;
    let result = file.write(data).await;
    finish_write(file.as_mut(), result).await
}

/// End a streaming write: publish on success, [`File::discard`] on failure.
/// The original error is reported; a failure to discard is secondary.
async fn finish_write(file: &mut dyn File, result: Result<()>) -> Result<()> {
    match result {
        Ok(()) => file.close().await,
        Err(e) => {
            let _ = file.discard().await;
            Err(e)
        }
    }
}

pub(crate) async fn put_file<F: FileSystem + ?Sized>(
    fs: &F,
    local: &Path,
    path: &str,
    opts: WriteOptions,
) -> Result<()> {
    let mut source = tokio::fs::File::open(local)
        .await
        .map_err(|e| Error::io("open", local.display(), e))?;
    let mut file = fs.open(path, OpenOptions::from_write(opts)).await?;
    let result = async {
        let mut buf = BytesMut::with_capacity(IO_CHUNK);
        loop {
            buf.reserve(IO_CHUNK);
            let n = source
                .read_buf(&mut buf)
                .await
                .map_err(|e| Error::io("read", local.display(), e))?;
            if n == 0 {
                break;
            }
            file.write(buf.split().freeze()).await?;
        }
        Ok(())
    }
    .await;
    finish_write(file.as_mut(), result).await
}

pub(crate) async fn get_file<F: FileSystem + ?Sized>(
    fs: &F,
    path: &str,
    local: &Path,
) -> Result<()> {
    let mut file = fs.open(path, OpenOptions::read()).await?;
    if let Some(parent) = local.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::io("mkdir", parent.display(), e))?;
    }
    let mut target = tokio::fs::File::create(local)
        .await
        .map_err(|e| Error::io("create", local.display(), e))?;
    loop {
        let chunk = file.read(Some(IO_CHUNK)).await?;
        if chunk.is_empty() {
            break;
        }
        target
            .write_all(&chunk)
            .await
            .map_err(|e| Error::io("write", local.display(), e))?;
    }
    target
        .flush()
        .await
        .map_err(|e| Error::io("flush", local.display(), e))?;
    file.close().await
}

pub(crate) async fn copy_file<F: FileSystem + ?Sized>(fs: &F, src: &str, dst: &str) -> Result<()> {
    let mut source = fs.open(src, OpenOptions::read()).await?;
    let mut target = fs.open(dst, OpenOptions::write()).await?;
    let result = async {
        loop {
            let chunk = source.read(Some(IO_CHUNK)).await?;
            if chunk.is_empty() {
                break;
            }
            target.write(chunk).await?;
        }
        Ok(())
    }
    .await;
    finish_write(target.as_mut(), result).await
}

pub(crate) async fn move_file<F: FileSystem + ?Sized>(fs: &F, src: &str, dst: &str) -> Result<()> {
    fs.copy_file(src, dst).await?;
    fs.rm_file(src).await
}

// =========================================================================
// Derived operations
// =========================================================================

pub(crate) async fn exists<F: FileSystem + ?Sized>(fs: &F, path: &str) -> Result<bool> {
    Ok(try_info(fs, path).await?.is_some())
}

pub(crate) async fn is_file<F: FileSystem + ?Sized>(fs: &F, path: &str) -> Result<bool> {
    Ok(try_info(fs, path).await?.is_some_and(|e| e.is_file()))
}

pub(crate) async fn is_dir<F: FileSystem + ?Sized>(fs: &F, path: &str) -> Result<bool> {
    Ok(try_info(fs, path).await?.is_some_and(|e| e.is_dir()))
}

pub(crate) async fn size<F: FileSystem + ?Sized>(fs: &F, path: &str) -> Result<u64> {
    Ok(fs.info(path).await?.size)
}

pub(crate) async fn walk<F: FileSystem + ?Sized>(
    fs: &F,
    path: &str,
    opts: WalkOptions,
) -> Result<Vec<WalkEntry>> {
    if opts.maxdepth == Some(0) {
        return Err(invalid_maxdepth());
    }
    let root = match try_info(fs, path).await? {
        Some(entry) if entry.is_dir() => entry,
        _ => return Ok(Vec::new()),
    };
    let entries = fs
        .find(
            &root.path,
            FindOptions {
                maxdepth: opts.maxdepth,
                withdirs: true,
                versions: false,
            },
        )
        .await?;

    let root_depth = entry::depth(&root.path);
    let mut nodes: BTreeMap<String, (Vec<Entry>, Vec<Entry>)> = BTreeMap::new();
    nodes.insert(root.path.clone(), Default::default());
    for e in entries {
        if e.path == root.path {
            continue;
        }
        let Some(parent) = entry::parent(&e.path) else {
            continue;
        };
        if e.is_dir() {
            // Only directories we actually descended into get their own node.
            let rel_depth = entry::depth(&e.path) - root_depth;
            if opts.maxdepth.is_none_or(|max| rel_depth < max) {
                nodes.entry(e.path.clone()).or_default();
            }
            nodes.entry(parent.to_string()).or_default().0.push(e);
        } else {
            nodes.entry(parent.to_string()).or_default().1.push(e);
        }
    }
    Ok(nodes
        .into_iter()
        .map(|(dir, (dirs, files))| WalkEntry { dir, dirs, files })
        .collect())
}

pub(crate) async fn du<F: FileSystem + ?Sized>(
    fs: &F,
    path: &str,
    opts: DuOptions,
) -> Result<DiskUsage> {
    let entries = fs
        .find(
            path,
            FindOptions {
                maxdepth: opts.maxdepth,
                withdirs: opts.withdirs,
                versions: false,
            },
        )
        .await?;
    let total = entries.iter().filter(|e| e.is_file()).map(|e| e.size).sum();
    let sizes = entries.into_iter().map(|e| (e.path, e.size)).collect();
    Ok(DiskUsage { total, sizes })
}

pub(crate) async fn glob<F: FileSystem + ?Sized>(
    fs: &F,
    pattern: &str,
    opts: GlobOptions,
) -> Result<Vec<Entry>> {
    let dirs_only = pattern.ends_with('/');
    let pattern = pattern.trim_end_matches('/');
    if !glob::has_magic(pattern) {
        return Ok(match try_info(fs, pattern).await? {
            Some(entry) if !dirs_only || entry.is_dir() => vec![entry],
            _ => Vec::new(),
        });
    }
    let root = glob::root(pattern);
    let maxdepth = if pattern.contains("**") {
        opts.maxdepth
    } else {
        let needed = entry::depth(pattern) - entry::depth(root);
        Some(opts.maxdepth.map_or(needed, |max| max.min(needed)))
    };
    let entries = fs
        .find(
            root,
            FindOptions {
                maxdepth,
                withdirs: true,
                versions: false,
            },
        )
        .await?;
    let mut out: Vec<Entry> = entries
        .into_iter()
        .filter(|e| (!dirs_only || e.is_dir()) && glob::matches(pattern, &e.path))
        .collect();
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

pub(crate) async fn cat<F: FileSystem + ?Sized>(
    fs: &F,
    paths: &[&str],
    opts: BulkOptions,
) -> Result<Vec<(String, Result<Bytes>)>> {
    // Owned items keep the closure free of lifetimes, which the `Send` check
    // on async trait bodies otherwise trips over (rust-lang/rust#102211).
    let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
    let mut results = stream::iter(owned)
        .map(|path| async move {
            let result = fs.cat_file(&path, ByteRange::ALL).await;
            (path, result)
        })
        .buffered(opts.concurrency.max(1));
    let mut out = Vec::with_capacity(paths.len());
    while let Some((path, result)) = results.next().await {
        match (opts.on_error, result) {
            (OnError::Raise, Err(e)) => return Err(e),
            (OnError::Ignore, Err(_)) => {}
            (_, result) => out.push((path, result)),
        }
    }
    Ok(out)
}

pub(crate) async fn cat_ranges<F: FileSystem + ?Sized>(
    fs: &F,
    requests: &[(&str, ByteRange)],
    opts: BulkOptions,
) -> Result<Vec<Result<Bytes>>> {
    let owned: Vec<(String, ByteRange)> = requests
        .iter()
        .map(|(path, range)| (path.to_string(), *range))
        .collect();
    let mut results = stream::iter(owned)
        .map(|(path, range)| async move { fs.cat_file(&path, range).await })
        .buffered(opts.concurrency.max(1));
    let mut out = Vec::with_capacity(requests.len());
    while let Some(result) = results.next().await {
        if opts.on_error == OnError::Raise {
            if let Err(e) = result {
                return Err(e);
            }
        }
        out.push(result);
    }
    Ok(out)
}

pub(crate) async fn rm<F: FileSystem + ?Sized>(fs: &F, path: &str, opts: RmOptions) -> Result<()> {
    let root = fs.info(path).await?;
    if root.is_file() {
        return fs.rm_file(&root.path).await;
    }
    if !opts.recursive {
        return Err(Error::is_a_directory(path));
    }
    let entries = fs
        .find(
            &root.path,
            FindOptions {
                maxdepth: opts.maxdepth,
                withdirs: true,
                versions: false,
            },
        )
        .await?;
    let (dirs, files): (Vec<Entry>, Vec<Entry>) = entries.into_iter().partition(Entry::is_dir);

    // Files first, concurrently. A file that is already gone is fine.
    let mut first_error = None;
    let mut deletes = stream::iter(files)
        .map(|e| async move { not_found_is_ok(fs.rm_file(&e.path).await) })
        .buffer_unordered(opts.concurrency.max(1));
    while let Some(result) = deletes.next().await {
        if let Err(e) = result {
            first_error.get_or_insert(e);
        }
    }
    drop(deletes);
    if let Some(e) = first_error {
        return Err(e);
    }

    // Then directories, deepest first, finishing with `path` itself. When the
    // descent was depth-limited, directories legitimately stay non-empty.
    let mut dirs: Vec<Entry> = dirs.into_iter().filter(|d| d.path != root.path).collect();
    dirs.sort_by(|a, b| {
        entry::depth(&b.path)
            .cmp(&entry::depth(&a.path))
            .then_with(|| b.path.cmp(&a.path))
    });
    for dir in dirs.iter().chain(std::iter::once(&root)) {
        match fs.rmdir(&dir.path).await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) if e.kind() == ErrorKind::DirectoryNotEmpty && opts.maxdepth.is_some() => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Resolve the `fsspec` destination rules shared by `copy`, `mv` and `put`.
///
/// Returns the base path every relative source path is joined onto.
/// `src_name` is the last component of the source, `src_trailing` whether the
/// caller wrote the source with a trailing separator ("copy the contents").
pub(crate) async fn destination_base<F: FileSystem + ?Sized>(
    fs: &F,
    dst: &str,
    src_name: &str,
    src_trailing: bool,
) -> Result<String> {
    let dst_norm = dst.trim_end_matches('/');
    let dst_is_dir = dst.ends_with('/') || fs.is_dir(dst_norm).await?;
    Ok(if dst_is_dir && !src_trailing {
        entry::join(dst_norm, src_name)
    } else {
        dst_norm.to_string()
    })
}

/// Run `op` over `(src, dst)` pairs with bounded concurrency and the `fsspec`
/// error policy: `Raise` fails fast, `Ignore` skips `NotFound` (a source that
/// vanished), `Return` skips every failure.
async fn transfer_all<Op, Fut>(
    pairs: Vec<(String, String)>,
    concurrency: usize,
    on_error: OnError,
    op: Op,
) -> Result<()>
where
    Op: Fn(String, String) -> Fut,
    Fut: Future<Output = Result<()>> + Send,
{
    let mut first_error = None;
    let mut results = stream::iter(pairs)
        .map(|(s, d)| op(s, d))
        .buffer_unordered(concurrency.max(1));
    while let Some(result) = results.next().await {
        match (on_error, result) {
            (_, Ok(())) => {}
            (OnError::Raise, Err(e)) => return Err(e),
            (OnError::Ignore, Err(e)) if e.kind() != ErrorKind::NotFound => {
                first_error.get_or_insert(e);
            }
            (_, Err(_)) => {}
        }
    }
    drop(results);
    first_error.map_or(Ok(()), Err)
}

/// Shared body of `copy` and `mv`: `op` is `copy_file` or `move_file`.
async fn transfer_tree<'a, F, Op>(
    fs: &'a F,
    src: &str,
    dst: &str,
    opts: &CopyOptions,
    op: Op,
) -> Result<Entry>
where
    F: FileSystem + ?Sized,
    Op: Fn(String, String) -> BoxFuture<'a, Result<()>>,
{
    let src_info = fs.info(src).await?;
    let base =
        destination_base(fs, dst, entry::basename(&src_info.path), src.ends_with('/')).await?;
    if src_info.is_file() {
        // `src_trailing` is false for files, so `base` is either `dst/<name>`
        // (dst is a directory) or `dst` itself.
        op(src_info.path.clone(), base).await?;
        return Ok(src_info);
    }
    if !opts.recursive {
        return Err(Error::is_a_directory(src));
    }
    let files = fs
        .find(
            &src_info.path,
            FindOptions {
                maxdepth: opts.maxdepth,
                withdirs: false,
                versions: false,
            },
        )
        .await?;
    let pairs = files
        .into_iter()
        .filter_map(|e| {
            entry::relative(&src_info.path, &e.path)
                .filter(|rel| !rel.is_empty())
                .map(|rel| (e.path.clone(), entry::join(&base, rel)))
        })
        .collect();
    let on_error = opts.on_error.unwrap_or(OnError::Ignore);
    transfer_all(pairs, opts.concurrency, on_error, op).await?;
    Ok(src_info)
}

pub(crate) async fn copy<F: FileSystem + ?Sized>(
    fs: &F,
    src: &str,
    dst: &str,
    opts: CopyOptions,
) -> Result<()> {
    transfer_tree(fs, src, dst, &opts, move |s, d| {
        Box::pin(async move { fs.copy_file(&s, &d).await })
    })
    .await
    .map(drop)
}

pub(crate) async fn mv<F: FileSystem + ?Sized>(
    fs: &F,
    src: &str,
    dst: &str,
    opts: CopyOptions,
) -> Result<()> {
    let src_info = transfer_tree(fs, src, dst, &opts, move |s, d| {
        Box::pin(async move { fs.move_file(&s, &d).await })
    })
    .await?;
    if src_info.is_dir() {
        // Remove whatever is left of the source tree (empty directories,
        // placeholders). It may already be gone on stores without real
        // directories.
        not_found_is_ok(
            fs.rm(
                &src_info.path,
                RmOptions {
                    recursive: true,
                    maxdepth: opts.maxdepth,
                    concurrency: opts.concurrency,
                },
            )
            .await,
        )?;
    }
    Ok(())
}

pub(crate) async fn put<F: FileSystem + ?Sized>(
    fs: &F,
    local: &Path,
    path: &str,
    opts: PutOptions,
) -> Result<()> {
    let meta = tokio::fs::metadata(local)
        .await
        .map_err(|e| Error::io("stat", local.display(), e))?;
    let name = local
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let trailing = local
        .as_os_str()
        .to_string_lossy()
        .ends_with(std::path::MAIN_SEPARATOR);
    let base = destination_base(fs, path, &name, trailing && meta.is_dir()).await?;
    if meta.is_file() {
        return fs.put_file(local, &base, opts.write).await;
    }
    if !opts.recursive {
        return Err(Error::is_a_directory(&local.display().to_string()));
    }
    let pairs = walk_local(local)
        .await?
        .into_iter()
        .map(|(abs, rel)| (abs.to_string_lossy().into_owned(), entry::join(&base, &rel)))
        .collect();
    let write = &opts.write;
    transfer_all(
        pairs,
        opts.concurrency,
        OnError::Raise,
        |abs, dst| async move { fs.put_file(Path::new(&abs), &dst, write.clone()).await },
    )
    .await
}

/// All regular files below `root` as `(absolute, relative-with-slashes)`.
async fn walk_local(root: &Path) -> Result<Vec<(PathBuf, String)>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| Error::io("read_dir", dir.display(), e))?;
        while let Some(item) = entries
            .next_entry()
            .await
            .map_err(|e| Error::io("read_dir", dir.display(), e))?
        {
            let abs = item.path();
            let meta = tokio::fs::metadata(&abs)
                .await
                .map_err(|e| Error::io("stat", abs.display(), e))?;
            if meta.is_dir() {
                stack.push(abs);
            } else if meta.is_file() {
                let rel = abs
                    .strip_prefix(root)
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push((abs, rel));
            }
        }
    }
    out.sort();
    Ok(out)
}
