//! Parsing of `gcsfs`-style paths.
//!
//! Two representations are used inside the GCS implementation:
//!
//! * [`Loc`] — any location a file-system call may name: the root (`""`), a
//!   bucket, or a key inside a bucket (file or directory). Trailing slashes
//!   are normalised away, so `b/dir/` and `b/dir` are the same location.
//! * [`GcsPath`] — a reference to one **object** (bucket + non-empty object
//!   name + optional generation), i.e. what the SDK calls need.
//!
//! Both accept the spellings `gcsfs.GCSFileSystem.split_path` accepts:
//! `gs://b/k`, `gcs://b/k`, `b/k`, `/b/k` and the `b/k#<generation>` suffix.

use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// A parsed location: root, bucket, or key within a bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Loc {
    bucket: String,
    key: String,
    generation: Option<i64>,
}

impl Loc {
    /// Parse any path accepted by the file system.
    ///
    /// * `""`, `"/"`, `"gs://"` → root;
    /// * `"b"`, `"b/"`, `"gs://b"` → bucket `b`;
    /// * `"b/x/y/"`, `"b/x/y"`, `"b/x/y#12"` → key `x/y` (generation `12`).
    pub(crate) fn parse(path: &str) -> Result<Self> {
        let stripped = strip_scheme(path).trim_start_matches('/');
        // Split the generation off the whole path first: bucket names cannot
        // contain '#', and doing it here lets `b#7` be rejected below instead
        // of being read as a bucket called `b#7`.
        let (stripped, generation) = split_generation(stripped);
        let (bucket, key) = stripped.split_once('/').unwrap_or((stripped, ""));
        let key = key.trim_end_matches('/');
        if bucket.is_empty() && (!key.is_empty() || generation.is_some()) {
            return Err(Error::invalid_path(path, "bucket name is empty"));
        }
        if key.is_empty() && generation.is_some() {
            return Err(Error::invalid_path(
                path,
                "a generation can only be given for an object",
            ));
        }
        if key.starts_with('/') || key.contains("//") {
            return Err(Error::invalid_path(path, "empty path segment"));
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            generation,
        })
    }

    pub(crate) fn is_root(&self) -> bool {
        self.bucket.is_empty()
    }

    pub(crate) fn is_bucket(&self) -> bool {
        !self.bucket.is_empty() && self.key.is_empty()
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The key without trailing slash; empty for the root and for buckets.
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn generation(&self) -> Option<i64> {
        self.generation
    }

    /// The normalised entry path: `""`, `bucket` or `bucket/key`.
    pub(crate) fn path(&self) -> String {
        if self.key.is_empty() {
            self.bucket.clone()
        } else {
            format!("{}/{}", self.bucket, self.key)
        }
    }

    /// The listing prefix for this location as a directory: `""` for a bucket,
    /// `key/` otherwise.
    pub(crate) fn prefix(&self) -> String {
        if self.key.is_empty() {
            String::new()
        } else {
            format!("{}/", self.key)
        }
    }

    /// The object this location names. Fails for the root and for buckets.
    pub(crate) fn object(&self) -> Result<GcsPath> {
        if self.key.is_empty() {
            return Err(Error::invalid_path(
                &self.path(),
                "expected '<bucket>/<object>'",
            ));
        }
        Ok(GcsPath {
            bucket: self.bucket.clone(),
            object: self.key.clone(),
            generation: self.generation,
        })
    }

    /// The zero-byte `key/` placeholder object for this directory.
    pub(crate) fn placeholder(&self) -> Result<GcsPath> {
        let mut object = self.object()?;
        object.object.push('/');
        object.generation = None;
        Ok(object)
    }
}

/// A fully-qualified reference to a GCS object: bucket, object name and an
/// optional generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct GcsPath {
    bucket: String,
    object: String,
    generation: Option<i64>,
}

impl GcsPath {
    /// Build a path from an already split bucket and object name.
    ///
    /// Returns [`ErrorKind::InvalidPath`](crate::ErrorKind::InvalidPath) if
    /// either component is empty or the bucket contains a `/`.
    pub(crate) fn new(bucket: impl Into<String>, object: impl Into<String>) -> Result<Self> {
        let bucket = bucket.into();
        let object = object.into();
        let display = || format!("{bucket}/{object}");
        if bucket.is_empty() {
            return Err(Error::invalid_path(&display(), "bucket name is empty"));
        }
        if bucket.contains('/') {
            return Err(Error::invalid_path(
                &display(),
                "bucket name must not contain '/'",
            ));
        }
        if object.is_empty() {
            return Err(Error::invalid_path(&display(), "object name is empty"));
        }
        Ok(Self {
            bucket,
            object,
            generation: None,
        })
    }

    /// Parse a path of the form `[gs://|gcs://][/]<bucket>/<object>[#<generation>]`.
    ///
    /// The `#<generation>` suffix is only interpreted as a generation when it
    /// parses as an integer (matching `gcsfs`); otherwise the `#` is treated as
    /// part of the object name. Unlike [`Loc::parse`], trailing slashes are
    /// preserved so placeholder objects stay addressable.
    pub(crate) fn parse(path: &str) -> Result<Self> {
        let stripped = strip_scheme(path).trim_start_matches('/');
        let Some((bucket, rest)) = stripped.split_once('/') else {
            return Err(Error::invalid_path(
                path,
                "expected '<bucket>/<object>' (bucket-only paths are not supported)",
            ));
        };
        if bucket.is_empty() {
            return Err(Error::invalid_path(path, "bucket name is empty"));
        }
        let (object, generation) = split_generation(rest);
        if object.is_empty() {
            return Err(Error::invalid_path(path, "object name is empty"));
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            generation,
        })
    }

    /// Return a copy of this path pinned to (or cleared of) a specific generation.
    pub(crate) fn with_generation(mut self, generation: Option<i64>) -> Self {
        self.generation = generation;
        self
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn object(&self) -> &str {
        &self.object
    }

    pub(crate) fn generation(&self) -> Option<i64> {
        self.generation
    }

    /// The bucket in the resource-name form expected by the Rust SDK:
    /// `projects/_/buckets/<bucket>`.
    pub(crate) fn bucket_resource(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket)
    }
}

impl fmt::Display for GcsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gs://{}/{}", self.bucket, self.object)?;
        if let Some(generation) = self.generation {
            write!(f, "#{generation}")?;
        }
        Ok(())
    }
}

impl FromStr for GcsPath {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

fn strip_scheme(path: &str) -> &str {
    for scheme in ["gs://", "gcs://"] {
        if let Some(rest) = path.strip_prefix(scheme) {
            return rest;
        }
    }
    path
}

/// Split a trailing `#<generation>` suffix, but only when it is an integer.
fn split_generation(rest: &str) -> (&str, Option<i64>) {
    match rest.rsplit_once('#') {
        Some((object, suffix)) => match suffix.parse::<i64>() {
            Ok(generation) => (object, Some(generation)),
            Err(_) => (rest, None),
        },
        None => (rest, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn loc_parses_root_bucket_and_keys() {
        for root in ["", "/", "gs://", "gs:///"] {
            let l = Loc::parse(root).unwrap();
            assert!(l.is_root(), "{root:?}");
            assert_eq!(l.path(), "");
        }
        for bucket in ["b", "b/", "gs://b", "gs://b/", "/b"] {
            let l = Loc::parse(bucket).unwrap();
            assert!(l.is_bucket(), "{bucket:?}");
            assert_eq!(l.path(), "b");
            assert_eq!(l.prefix(), "");
            assert!(l.object().is_err());
        }
        let l = Loc::parse("gs://b/x/y/").unwrap();
        assert_eq!((l.bucket(), l.key(), l.generation()), ("b", "x/y", None));
        assert_eq!(l.path(), "b/x/y");
        assert_eq!(l.prefix(), "x/y/");
        assert_eq!(l.object().unwrap().to_string(), "gs://b/x/y");
        assert_eq!(l.placeholder().unwrap().object(), "x/y/");

        let l = Loc::parse("b/x#7").unwrap();
        assert_eq!(l.generation(), Some(7));
        assert_eq!(l.object().unwrap().generation(), Some(7));
        assert_eq!(l.placeholder().unwrap().generation(), None);
    }

    #[test]
    fn loc_rejects_malformed_paths() {
        for input in ["b#7", "b//x", "/#7", "gs://b/x//y"] {
            let err = Loc::parse(input).expect_err(input);
            assert_eq!(err.kind(), ErrorKind::InvalidPath, "{input}");
        }
    }

    #[test]
    fn parses_all_spellings() {
        for input in [
            "gs://bucket/dir/obj.txt",
            "gcs://bucket/dir/obj.txt",
            "bucket/dir/obj.txt",
            "/bucket/dir/obj.txt",
            "gs:///bucket/dir/obj.txt",
        ] {
            let p = GcsPath::parse(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(p.bucket(), "bucket", "{input}");
            assert_eq!(p.object(), "dir/obj.txt", "{input}");
            assert_eq!(p.generation(), None, "{input}");
        }
    }

    #[test]
    fn parses_generation_suffix_like_gcsfs() {
        let p = GcsPath::parse("bucket/obj#42").unwrap();
        assert_eq!(p.object(), "obj");
        assert_eq!(p.generation(), Some(42));

        // Non-integer suffix: '#' is part of the object name.
        let p = GcsPath::parse("bucket/obj#notanumber").unwrap();
        assert_eq!(p.object(), "obj#notanumber");
        assert_eq!(p.generation(), None);

        // Only the last '#' is considered.
        let p = GcsPath::parse("bucket/a#b#7").unwrap();
        assert_eq!(p.object(), "a#b");
        assert_eq!(p.generation(), Some(7));
    }

    #[test]
    fn rejects_invalid_paths() {
        for input in [
            "",
            "bucket",
            "gs://bucket",
            "/bucket/",
            "gs:///obj",
            "bucket/#12",
        ] {
            let err = GcsPath::parse(input).expect_err(input);
            assert_eq!(err.kind(), ErrorKind::InvalidPath, "{input}");
        }
        assert_eq!(
            GcsPath::new("", "o").unwrap_err().kind(),
            ErrorKind::InvalidPath
        );
        assert_eq!(
            GcsPath::new("b", "").unwrap_err().kind(),
            ErrorKind::InvalidPath
        );
        assert_eq!(
            GcsPath::new("b/c", "o").unwrap_err().kind(),
            ErrorKind::InvalidPath
        );
    }

    #[test]
    fn preserves_object_names_verbatim() {
        // Trailing slashes and odd characters are legal in object names.
        let p = GcsPath::parse("bucket/folder/").unwrap();
        assert_eq!(p.object(), "folder/");
        let p = GcsPath::parse("bucket/a b+c%20d").unwrap();
        assert_eq!(p.object(), "a b+c%20d");
    }

    #[test]
    fn formatting_helpers() {
        let p = GcsPath::new("b", "o").unwrap().with_generation(Some(5));
        assert_eq!(p.bucket_resource(), "projects/_/buckets/b");
        assert_eq!(p.to_string(), "gs://b/o#5");
        assert_eq!(p.with_generation(None).to_string(), "gs://b/o");
        let parsed: GcsPath = "gs://b/o".parse().unwrap();
        assert_eq!(parsed, GcsPath::new("b", "o").unwrap());
    }
}
