//! Parsing and representation of Google Cloud Storage object paths.

use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// A fully-qualified reference to a GCS object: bucket, object name and an
/// optional generation.
///
/// `GcsPath` accepts the same spellings as `gcsfs.GCSFileSystem.split_path`:
///
/// ```
/// use gcs_rust_fs::GcsPath;
///
/// let p = GcsPath::parse("gs://my-bucket/dir/file.bin#1712345678901234").unwrap();
/// assert_eq!(p.bucket(), "my-bucket");
/// assert_eq!(p.object(), "dir/file.bin");
/// assert_eq!(p.generation(), Some(1712345678901234));
///
/// // The scheme and leading slashes are optional.
/// assert_eq!(GcsPath::parse("my-bucket/dir/file.bin").unwrap().object(), "dir/file.bin");
/// assert_eq!(GcsPath::parse("/my-bucket/dir/file.bin").unwrap().bucket(), "my-bucket");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GcsPath {
    bucket: String,
    object: String,
    generation: Option<i64>,
}

impl GcsPath {
    /// Build a path from an already split bucket and object name.
    ///
    /// Returns [`ErrorKind::InvalidPath`](crate::ErrorKind::InvalidPath) if
    /// either component is empty or the bucket contains a `/`.
    pub fn new(bucket: impl Into<String>, object: impl Into<String>) -> Result<Self> {
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
    /// part of the object name.
    pub fn parse(path: &str) -> Result<Self> {
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
    pub fn with_generation(mut self, generation: Option<i64>) -> Self {
        self.generation = generation;
        self
    }

    /// The bucket name, e.g. `my-bucket`.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The object name (key) within the bucket.
    pub fn object(&self) -> &str {
        &self.object
    }

    /// The object generation, if one was specified.
    pub fn generation(&self) -> Option<i64> {
        self.generation
    }

    /// The bucket in the resource-name form expected by the Rust SDK:
    /// `projects/_/buckets/<bucket>`.
    pub fn bucket_resource(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket)
    }

    /// `<bucket>/<object>` — the `name` convention used by `gcsfs` info dicts.
    pub fn relative(&self) -> String {
        format!("{}/{}", self.bucket, self.object)
    }

    /// `gs://<bucket>/<object>[#<generation>]`.
    pub fn uri(&self) -> String {
        self.to_string()
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

impl TryFrom<&str> for GcsPath {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl TryFrom<String> for GcsPath {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Self::parse(&value)
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
        assert_eq!(p.relative(), "b/o");
        assert_eq!(p.uri(), "gs://b/o#5");
        assert_eq!(p.with_generation(None).to_string(), "gs://b/o");
        let parsed: GcsPath = "gs://b/o".parse().unwrap();
        assert_eq!(parsed, GcsPath::new("b", "o").unwrap());
    }
}
