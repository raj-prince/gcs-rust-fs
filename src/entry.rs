//! Directory-listing entries: what `fsspec` calls an *info dict*.

use crate::stat::ObjectStat;

/// Whether an [`Entry`] is a file or a directory.
///
/// Buckets are reported as directories — they are the top-level directories
/// of the file system. Use the path to tell them apart if needed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EntryKind {
    /// A regular object.
    File,
    /// A bucket, a zero-byte `dir/` placeholder object, or a prefix that has
    /// at least one object below it.
    Directory,
}

impl EntryKind {
    /// The `fsspec` spelling: `"file"` or `"directory"`.
    pub fn as_str(self) -> &'static str {
        match self {
            EntryKind::File => "file",
            EntryKind::Directory => "directory",
        }
    }
}

/// One item of a listing, or the result of [`FileSystem::info`].
///
/// `path` follows the `fsspec`/`gcsfs` convention: `bucket/key` with no
/// scheme and no trailing slash. Directories have `size == 0` and, unless they
/// are backed by a placeholder object, no [`stat`](Self::stat).
///
/// [`FileSystem::info`]: crate::FileSystem::info
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// Full path, e.g. `my-bucket/data/part-0.parquet`.
    pub path: String,
    /// File or directory.
    pub kind: EntryKind,
    /// Size in bytes (`0` for directories).
    pub size: u64,
    /// Full object metadata when the entry is backed by an object.
    pub stat: Option<ObjectStat>,
}

impl Entry {
    /// A file entry; `size` is taken from `stat`.
    pub fn file(path: impl Into<String>, stat: ObjectStat) -> Self {
        Self {
            path: path.into(),
            kind: EntryKind::File,
            size: stat.size,
            stat: Some(stat),
        }
    }

    /// A directory entry without backing metadata (a bucket or a prefix).
    pub fn directory(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            kind: EntryKind::Directory,
            size: 0,
            stat: None,
        }
    }

    /// Attach object metadata (e.g. the placeholder object of a directory).
    pub fn with_stat(mut self, stat: ObjectStat) -> Self {
        self.stat = Some(stat);
        self
    }

    /// `true` for [`EntryKind::File`].
    pub fn is_file(&self) -> bool {
        self.kind == EntryKind::File
    }

    /// `true` for [`EntryKind::Directory`].
    pub fn is_dir(&self) -> bool {
        self.kind == EntryKind::Directory
    }

    /// The last path component (`part-0.parquet` for
    /// `my-bucket/data/part-0.parquet`, `my-bucket` for a bucket).
    pub fn name(&self) -> &str {
        basename(&self.path)
    }

    /// The parent path, or `None` for a bucket.
    pub fn parent(&self) -> Option<&str> {
        parent(&self.path)
    }
}

/// One directory visited by [`FileSystem::walk`](crate::FileSystem::walk):
/// the directory itself plus its immediate children, split by kind — the
/// shape of `os.walk` / `fsspec.walk`.
#[derive(Clone, Debug, PartialEq)]
pub struct WalkEntry {
    /// The directory being visited.
    pub dir: String,
    /// Immediate sub-directories, sorted by path.
    pub dirs: Vec<Entry>,
    /// Immediate files, sorted by path.
    pub files: Vec<Entry>,
}

/// Result of [`FileSystem::du`](crate::FileSystem::du).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiskUsage {
    /// Sum of the sizes of all files found.
    pub total: u64,
    /// `(path, size)` for every entry counted, sorted by path — what `fsspec`
    /// returns for `du(total=False)`.
    pub sizes: Vec<(String, u64)>,
}

/// Last component of a `bucket/key` path (trailing slashes ignored).
pub(crate) fn basename(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(idx) => &trimmed[idx + 1..],
        None => trimmed,
    }
}

/// Parent of a `bucket/key` path, or `None` for a bucket / empty path.
pub(crate) fn parent(path: &str) -> Option<&str> {
    let trimmed = path.trim_end_matches('/');
    trimmed.rfind('/').map(|idx| &trimmed[..idx])
}

/// Number of path components (`bucket` = 1, `bucket/a/b` = 3).
pub(crate) fn depth(path: &str) -> usize {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        0
    } else {
        trimmed.split('/').count()
    }
}

/// Join `rel` onto `base` with exactly one separator.
pub(crate) fn join(base: &str, rel: &str) -> String {
    let base = base.trim_end_matches('/');
    let rel = rel.trim_start_matches('/');
    match (base.is_empty(), rel.is_empty()) {
        (true, _) => rel.to_string(),
        (_, true) => base.to_string(),
        _ => format!("{base}/{rel}"),
    }
}

/// Path of `path` relative to `root`, or `None` if `path` is not below `root`.
pub(crate) fn relative<'a>(root: &str, path: &'a str) -> Option<&'a str> {
    let root = root.trim_end_matches('/');
    if root.is_empty() {
        return Some(path.trim_start_matches('/'));
    }
    let rest = path.strip_prefix(root)?;
    rest.strip_prefix('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_helpers() {
        let stat = ObjectStat {
            size: 7,
            ..Default::default()
        };
        let f = Entry::file("b/a/x.txt", stat);
        assert!(f.is_file());
        assert_eq!(f.size, 7);
        assert_eq!(f.name(), "x.txt");
        assert_eq!(f.parent(), Some("b/a"));

        let d = Entry::directory("b");
        assert!(d.is_dir());
        assert_eq!(d.name(), "b");
        assert_eq!(d.parent(), None);
        assert_eq!(EntryKind::Directory.as_str(), "directory");
    }

    #[test]
    fn path_helpers() {
        assert_eq!(basename("b/a/c/"), "c");
        assert_eq!(parent("b/a/c"), Some("b/a"));
        assert_eq!(parent("b/"), None);
        assert_eq!(depth(""), 0);
        assert_eq!(depth("b"), 1);
        assert_eq!(depth("b/a/c/"), 3);
        assert_eq!(join("b/", "/x"), "b/x");
        assert_eq!(join("", "x"), "x");
        assert_eq!(join("b", ""), "b");
        assert_eq!(relative("b/a", "b/a/c/d"), Some("c/d"));
        assert_eq!(relative("b/a/", "b/a"), None);
        assert_eq!(relative("b/a", "b/ab/c"), None);
        assert_eq!(relative("", "b/c"), Some("b/c"));
    }
}
