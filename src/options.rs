//! Option structs for [`FileSystem`](crate::FileSystem) operations.
//!
//! Every struct implements [`Default`] with the `fsspec` defaults, so call
//! sites read like keyword arguments:
//!
//! ```
//! use gcs_rust_fs::{FindOptions, RmOptions};
//!
//! let find = FindOptions { withdirs: true, ..Default::default() };
//! let rm = RmOptions { recursive: true, ..Default::default() };
//! # let _ = (find, rm);
//! ```

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Default number of operations kept in flight by bulk methods
/// (`cat`, `rm`, `copy`, `put`, ...).
pub const DEFAULT_CONCURRENCY: usize = 64;

/// How a [`File`](crate::File) was opened — the `fsspec` `mode` string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum OpenMode {
    /// `"rb"`: read an existing file.
    #[default]
    Read,
    /// `"wb"`: create or overwrite.
    Write,
    /// `"ab"`: append to an existing file (or create it).
    Append,
    /// `"xb"`: create; fail with [`ErrorKind::AlreadyExists`] if present.
    ///
    /// [`ErrorKind::AlreadyExists`]: crate::ErrorKind::AlreadyExists
    CreateNew,
}

impl OpenMode {
    /// Parse a Python-style mode string (`"rb"`, `"wb"`, `"ab"`, `"xb"`;
    /// the `b` is optional, `+` and text modes are rejected).
    pub fn parse(mode: &str) -> Result<Self> {
        match mode {
            "r" | "rb" => Ok(OpenMode::Read),
            "w" | "wb" => Ok(OpenMode::Write),
            "a" | "ab" => Ok(OpenMode::Append),
            "x" | "xb" => Ok(OpenMode::CreateNew),
            other => Err(Error::unsupported(format!(
                "unsupported open mode {other:?}; expected one of rb, wb, ab, xb"
            ))),
        }
    }

    /// The canonical mode string (`"rb"`, `"wb"`, `"ab"`, `"xb"`).
    pub fn as_str(self) -> &'static str {
        match self {
            OpenMode::Read => "rb",
            OpenMode::Write => "wb",
            OpenMode::Append => "ab",
            OpenMode::CreateNew => "xb",
        }
    }

    /// `true` for [`OpenMode::Read`].
    pub fn is_read(self) -> bool {
        matches!(self, OpenMode::Read)
    }

    /// `true` for every mode except [`OpenMode::Read`].
    pub fn is_write(self) -> bool {
        !self.is_read()
    }
}

/// Whether a whole-file write may replace an existing file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum WriteMode {
    /// Replace the file if it exists (`fsspec` `mode="overwrite"`).
    #[default]
    Overwrite,
    /// Fail with [`ErrorKind::AlreadyExists`] if the file exists
    /// (`fsspec` `mode="create"`).
    ///
    /// [`ErrorKind::AlreadyExists`]: crate::ErrorKind::AlreadyExists
    Create,
}

/// Options for whole-file writes (`pipe_file`, `put_file`, `put`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteOptions {
    /// Overwrite or create-only.
    pub mode: WriteMode,
    /// `Content-Type` to store with the file, if the file system supports it.
    pub content_type: Option<String>,
    /// User metadata to store with the file, if the file system supports it.
    pub metadata: HashMap<String, String>,
    /// Upload chunk / buffer size hint in bytes (`None` = implementation default).
    pub block_size: Option<usize>,
}

/// Options for whole-file reads (`cat_file`, `get_file`).
///
/// There are no per-read knobs yet; the struct exists so that future ones
/// (parallel range requests, checksum verification, ...) can be added without
/// changing the [`FileSystem`](crate::FileSystem) signatures. It is
/// `#[non_exhaustive]`, so callers outside this crate construct it with
/// [`ReadOptions::default()`] and keep compiling when fields appear.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {}

/// Options for [`FileSystem::open`](crate::FileSystem::open).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    /// Read, write, append or create-new.
    pub mode: OpenMode,
    /// Read-ahead (read modes) or upload chunk (write modes) size hint in
    /// bytes. `Some(0)` disables read-ahead; `None` = implementation default.
    pub block_size: Option<usize>,
    /// `Content-Type` for files created in a write mode.
    pub content_type: Option<String>,
    /// User metadata for files created in a write mode.
    pub metadata: HashMap<String, String>,
}

impl OpenOptions {
    /// Open for reading (`"rb"`).
    pub fn read() -> Self {
        Self::default()
    }

    /// Open for writing (`"wb"`).
    pub fn write() -> Self {
        Self {
            mode: OpenMode::Write,
            ..Self::default()
        }
    }

    /// Open with the given mode.
    pub fn with_mode(mode: OpenMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    /// Translate whole-file [`ReadOptions`] into the equivalent open options.
    pub fn from_read(opts: ReadOptions) -> Self {
        // Destructured rather than ignored so that adding a field to
        // `ReadOptions` fails to compile here until it is forwarded.
        let ReadOptions {} = opts;
        Self::read()
    }

    /// Translate whole-file [`WriteOptions`] into the equivalent open options.
    pub fn from_write(opts: WriteOptions) -> Self {
        Self {
            mode: match opts.mode {
                WriteMode::Overwrite => OpenMode::Write,
                WriteMode::Create => OpenMode::CreateNew,
            },
            block_size: opts.block_size,
            content_type: opts.content_type,
            metadata: opts.metadata,
        }
    }
}

/// Options for [`FileSystem::ls`](crate::FileSystem::ls).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListOptions {
    /// List every version/generation of each file; entry paths then carry a
    /// `#<generation>` suffix (`gcsfs` convention).
    pub versions: bool,
    /// Bypass any cached listing and fetch from the store (`fsspec`
    /// `ls(refresh=True)`); the fresh result replaces the cached one. Ignored
    /// by filesystems that cache nothing.
    pub refresh: bool,
}

/// Options for [`FileSystem::find`](crate::FileSystem::find).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FindOptions {
    /// Descend at most this many levels below the starting path
    /// (`Some(1)` = immediate children only). Must be at least 1.
    pub maxdepth: Option<usize>,
    /// Include directory entries (and the starting directory itself) in the
    /// result, not only files.
    pub withdirs: bool,
    /// See [`ListOptions::versions`].
    pub versions: bool,
}

/// Options for [`FileSystem::walk`](crate::FileSystem::walk).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalkOptions {
    /// Visit at most this many levels (`Some(1)` = the starting directory only).
    pub maxdepth: Option<usize>,
}

/// Options for [`FileSystem::du`](crate::FileSystem::du).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DuOptions {
    /// Count at most this many levels below the starting path.
    pub maxdepth: Option<usize>,
    /// Also report directory entries (with size 0) in
    /// [`DiskUsage::sizes`](crate::DiskUsage::sizes).
    pub withdirs: bool,
}

/// Options for [`FileSystem::glob`](crate::FileSystem::glob).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GlobOptions {
    /// Upper bound on how deep `**` may descend (`None` = unbounded).
    pub maxdepth: Option<usize>,
}

/// What a bulk operation does when one item fails — `fsspec`'s `on_error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum OnError {
    /// Stop at the first failure and return it (`"raise"`).
    #[default]
    Raise,
    /// Drop failed items from the result (`"omit"` / `"ignore"`).
    Ignore,
    /// Keep going and report each failure in place (`"return"`).
    Return,
}

/// Options for concurrent read operations (`cat`, `cat_ranges`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkOptions {
    /// Maximum number of requests in flight.
    pub concurrency: usize,
    /// Failure policy.
    pub on_error: OnError,
}

impl Default for BulkOptions {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            on_error: OnError::Raise,
        }
    }
}

/// Options for [`FileSystem::rm`](crate::FileSystem::rm).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RmOptions {
    /// Delete directories and their content. Without it, deleting a directory
    /// fails with [`ErrorKind::IsADirectory`](crate::ErrorKind::IsADirectory).
    pub recursive: bool,
    /// With `recursive`, delete at most this many levels deep.
    pub maxdepth: Option<usize>,
    /// Maximum number of deletes in flight.
    pub concurrency: usize,
}

impl Default for RmOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            maxdepth: None,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }
}

/// Options for [`FileSystem::copy`](crate::FileSystem::copy) and
/// [`FileSystem::mv`](crate::FileSystem::mv).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyOptions {
    /// Copy directories and their content. Without it, copying a directory
    /// fails with [`ErrorKind::IsADirectory`](crate::ErrorKind::IsADirectory).
    pub recursive: bool,
    /// With `recursive`, copy at most this many levels deep.
    pub maxdepth: Option<usize>,
    /// Maximum number of copies in flight.
    pub concurrency: usize,
    /// Failure policy. `None` picks the `fsspec` default: ignore
    /// `NotFound` for recursive copies, raise otherwise.
    pub on_error: Option<OnError>,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            maxdepth: None,
            concurrency: DEFAULT_CONCURRENCY,
            on_error: None,
        }
    }
}

/// Options for [`FileSystem::put`](crate::FileSystem::put).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutOptions {
    /// Upload local directories and their content.
    pub recursive: bool,
    /// Maximum number of uploads in flight.
    pub concurrency: usize,
    /// Applied to every uploaded file.
    pub write: WriteOptions,
}

impl Default for PutOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            concurrency: DEFAULT_CONCURRENCY,
            write: WriteOptions::default(),
        }
    }
}

/// Options for [`FileSystem::mkdir`](crate::FileSystem::mkdir).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MkdirOptions {
    /// Create missing parents (for object stores: create the bucket when a
    /// nested path is given). `fsspec` default is `false`.
    pub create_parents: bool,
    /// Location/region hint for newly created top-level directories (buckets).
    pub location: Option<String>,
    /// For object stores: materialise a nested directory with a zero-byte
    /// `dir/` placeholder object so that it exists while empty.
    pub placeholder: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn open_mode_round_trips() {
        for (s, m) in [
            ("rb", OpenMode::Read),
            ("r", OpenMode::Read),
            ("wb", OpenMode::Write),
            ("ab", OpenMode::Append),
            ("x", OpenMode::CreateNew),
        ] {
            assert_eq!(OpenMode::parse(s).unwrap(), m);
        }
        assert_eq!(OpenMode::Read.as_str(), "rb");
        assert!(OpenMode::Write.is_write());
        assert!(!OpenMode::Read.is_write());
        assert_eq!(
            OpenMode::parse("r+b").unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn open_options_from_write() {
        let w = WriteOptions {
            mode: WriteMode::Create,
            content_type: Some("text/plain".into()),
            ..Default::default()
        };
        let o = OpenOptions::from_write(w);
        assert_eq!(o.mode, OpenMode::CreateNew);
        assert_eq!(o.content_type.as_deref(), Some("text/plain"));
        assert_eq!(OpenOptions::write().mode, OpenMode::Write);
    }

    #[test]
    fn defaults_match_fsspec() {
        assert_eq!(BulkOptions::default().concurrency, DEFAULT_CONCURRENCY);
        assert_eq!(BulkOptions::default().on_error, OnError::Raise);
        assert!(!RmOptions::default().recursive);
        assert!(CopyOptions::default().on_error.is_none());
        assert!(!MkdirOptions::default().create_parents);
    }
}
