//! Byte ranges with Python-slice semantics.
//!
//! `fsspec`'s `cat_file(path, start, end)` follows Python slicing rules: `end`
//! is exclusive, either bound may be omitted, and negative bounds count back
//! from the end of the object. [`ByteRange`] models exactly that contract and
//! resolves it into the SDK's [`ReadRange`] type.

use google_cloud_storage::model_ext::ReadRange;

use crate::error::{Error, Result};

/// A half-open byte range `[start, end)` using Python-slice semantics.
///
/// ```
/// use gcs_rust_fs::ByteRange;
///
/// let whole = ByteRange::ALL;
/// let first_kib = ByteRange::head(1024);
/// let last_kib = ByteRange::tail(1024);
/// let window = ByteRange::span(100, 200);          // bytes 100..=199
/// let from_python = ByteRange::new(Some(-10), None); // the last 10 bytes
///
/// assert!(!whole.needs_size());
/// assert!(!first_kib.needs_size());
/// assert!(!last_kib.needs_size());
/// assert!(!window.needs_size());
/// assert!(!from_python.needs_size());
/// // Only ranges mixing negative and explicit bounds need the object size.
/// assert!(ByteRange::new(Some(0), Some(-1)).needs_size());
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ByteRange {
    start: Option<i64>,
    end: Option<i64>,
}

/// A [`ByteRange`] resolved against (optionally) the size of the object.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ResolvedRange {
    /// The range contains no bytes; no request is necessary.
    Empty,
    /// A concrete SDK range plus, when known, the number of bytes it covers.
    Read {
        range: ReadRange,
        len_hint: Option<u64>,
    },
}

impl ByteRange {
    /// The entire object.
    pub const ALL: ByteRange = ByteRange {
        start: None,
        end: None,
    };

    /// Create a range from optional, possibly negative, Python-style bounds.
    pub const fn new(start: Option<i64>, end: Option<i64>) -> Self {
        Self { start, end }
    }

    /// Bytes `[start, end)`.
    pub fn span(start: u64, end: u64) -> Self {
        Self::new(Some(clamp_i64(start)), Some(clamp_i64(end)))
    }

    /// All bytes from `start` to the end of the object.
    pub fn from_offset(start: u64) -> Self {
        Self::new(Some(clamp_i64(start)), None)
    }

    /// The first `count` bytes.
    pub fn head(count: u64) -> Self {
        Self::new(None, Some(clamp_i64(count)))
    }

    /// The last `count` bytes.
    pub fn tail(count: u64) -> Self {
        if count == 0 {
            // `start = -0` would mean "everything"; an empty tail is empty.
            return Self::span(0, 0);
        }
        Self::new(Some(-clamp_i64(count)), None)
    }

    /// The start bound, if any (negative values count from the end).
    pub fn start(&self) -> Option<i64> {
        self.start
    }

    /// The exclusive end bound, if any (negative values count from the end).
    pub fn end(&self) -> Option<i64> {
        self.end
    }

    /// Whether resolving this range requires knowing the object's size.
    ///
    /// Ranges expressible directly by the storage API — absolute offsets,
    /// `head(n)`, `tail(n)` — do not; mixing a negative bound with another
    /// bound does.
    pub fn needs_size(&self) -> bool {
        match (self.start, self.end) {
            (_, Some(end)) if end < 0 => true,
            (Some(start), Some(_)) if start < 0 => true,
            _ => false,
        }
    }

    /// Resolve the range against a known object `size` into concrete
    /// `[start, end)` offsets, exactly like a Python slice: negative bounds
    /// count back from the end, bounds beyond the object are truncated, and
    /// an empty or inverted range yields `start == end`.
    ///
    /// This is what a [`File::read_range`](crate::File::read_range)
    /// implementation applies once it knows the size of its file.
    ///
    /// ```
    /// use gcs_rust_fs::ByteRange;
    ///
    /// assert_eq!(ByteRange::ALL.clamp(10), 0..10);
    /// assert_eq!(ByteRange::span(2, 50).clamp(10), 2..10);
    /// assert_eq!(ByteRange::tail(3).clamp(10), 7..10);
    /// assert_eq!(ByteRange::new(Some(-4), Some(-2)).clamp(10), 6..8);
    /// assert_eq!(ByteRange::span(20, 30).clamp(10), 10..10);
    /// ```
    pub fn clamp(&self, size: u64) -> std::ops::Range<u64> {
        let sz = clamp_i64(size);
        let normalise = |bound: i64| -> i64 {
            if bound >= 0 {
                bound.min(sz)
            } else {
                (sz + bound).max(0)
            }
        };
        let start = normalise(self.start.unwrap_or(0));
        let end = normalise(self.end.unwrap_or(sz)).max(start);
        start as u64..end as u64
    }

    /// Resolve into a concrete request.
    ///
    /// When `size` is known the range is clamped exactly like a Python slice:
    /// a start at or beyond the end yields [`ResolvedRange::Empty`] and an end
    /// beyond the object is truncated. Without `size`, absolute ranges are
    /// passed through and the service decides (an out-of-range start surfaces
    /// as [`ErrorKind::OutOfRange`](crate::ErrorKind::OutOfRange)).
    pub(crate) fn resolve(&self, size: Option<u64>) -> Result<ResolvedRange> {
        let size_i = size.map(clamp_i64);
        let normalise = |bound: i64, what: &str| -> Result<i64> {
            if bound >= 0 {
                return Ok(bound);
            }
            match size_i {
                Some(sz) => Ok((sz + bound).max(0)),
                None => Err(Error::invalid_range(format!(
                    "negative {what} ({bound}) requires the object size to resolve"
                ))),
            }
        };

        match (self.start, self.end) {
            (None, None) => Ok(ResolvedRange::Read {
                range: ReadRange::all(),
                len_hint: size,
            }),
            // `tail(n)` is natively supported by the API; no size needed.
            (Some(start), None) if start < 0 && size.is_none() => {
                let count = start.unsigned_abs();
                Ok(ResolvedRange::Read {
                    range: ReadRange::tail(count),
                    len_hint: Some(count),
                })
            }
            (start, end) => {
                let mut start = normalise(start.unwrap_or(0), "start")?;
                if let Some(sz) = size_i {
                    if start >= sz {
                        return Ok(ResolvedRange::Empty);
                    }
                    start = start.min(sz);
                }
                let start_u = start as u64;
                match end {
                    None => Ok(ResolvedRange::Read {
                        range: ReadRange::offset(start_u),
                        len_hint: size.map(|sz| sz.saturating_sub(start_u)),
                    }),
                    Some(end) => {
                        let mut end = normalise(end, "end")?;
                        if let Some(sz) = size_i {
                            end = end.min(sz);
                        }
                        if end <= start {
                            return Ok(ResolvedRange::Empty);
                        }
                        let count = (end - start) as u64;
                        Ok(ResolvedRange::Read {
                            range: ReadRange::segment(start_u, count),
                            len_hint: Some(count),
                        })
                    }
                }
            }
        }
    }
}

impl From<std::ops::Range<u64>> for ByteRange {
    fn from(r: std::ops::Range<u64>) -> Self {
        Self::span(r.start, r.end)
    }
}

impl From<std::ops::RangeFrom<u64>> for ByteRange {
    fn from(r: std::ops::RangeFrom<u64>) -> Self {
        Self::from_offset(r.start)
    }
}

impl From<std::ops::RangeTo<u64>> for ByteRange {
    fn from(r: std::ops::RangeTo<u64>) -> Self {
        Self::head(r.end)
    }
}

impl From<std::ops::RangeFull> for ByteRange {
    fn from(_: std::ops::RangeFull) -> Self {
        Self::ALL
    }
}

/// Object sizes are bounded by 5 TiB, so this never saturates in practice.
fn clamp_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    fn read(range: ReadRange, len_hint: Option<u64>) -> ResolvedRange {
        ResolvedRange::Read { range, len_hint }
    }

    #[test]
    fn resolves_without_size() {
        assert_eq!(
            ByteRange::ALL.resolve(None).unwrap(),
            read(ReadRange::all(), None)
        );
        assert_eq!(
            ByteRange::from_offset(10).resolve(None).unwrap(),
            read(ReadRange::offset(10), None)
        );
        assert_eq!(
            ByteRange::head(10).resolve(None).unwrap(),
            read(ReadRange::segment(0, 10), Some(10))
        );
        assert_eq!(
            ByteRange::tail(10).resolve(None).unwrap(),
            read(ReadRange::tail(10), Some(10))
        );
        assert_eq!(
            ByteRange::span(5, 20).resolve(None).unwrap(),
            read(ReadRange::segment(5, 15), Some(15))
        );
        assert_eq!(
            ByteRange::from(5..20).resolve(None).unwrap(),
            read(ReadRange::segment(5, 15), Some(15))
        );
    }

    #[test]
    fn empty_ranges_need_no_request() {
        assert_eq!(
            ByteRange::span(10, 10).resolve(None).unwrap(),
            ResolvedRange::Empty
        );
        assert_eq!(
            ByteRange::span(15, 10).resolve(None).unwrap(),
            ResolvedRange::Empty
        );
        assert_eq!(
            ByteRange::head(0).resolve(None).unwrap(),
            ResolvedRange::Empty
        );
        assert_eq!(
            ByteRange::tail(0).resolve(None).unwrap(),
            ResolvedRange::Empty
        );
        assert_eq!(
            ByteRange::new(Some(-5), Some(-10))
                .resolve(Some(100))
                .unwrap(),
            ResolvedRange::Empty
        );
    }

    #[test]
    fn negative_bounds_resolve_against_size() {
        // start=-10, end=None, size known → offset(90)
        assert_eq!(
            ByteRange::new(Some(-10), None).resolve(Some(100)).unwrap(),
            read(ReadRange::offset(90), Some(10))
        );
        // start=0, end=-1 → [0, 99)
        assert_eq!(
            ByteRange::new(Some(0), Some(-1))
                .resolve(Some(100))
                .unwrap(),
            read(ReadRange::segment(0, 99), Some(99))
        );
        // start=-30, end=-20 → [70, 80)
        assert_eq!(
            ByteRange::new(Some(-30), Some(-20))
                .resolve(Some(100))
                .unwrap(),
            read(ReadRange::segment(70, 10), Some(10))
        );
        // Negative start larger than the object clamps to zero, like Python.
        assert_eq!(
            ByteRange::new(Some(-500), Some(10))
                .resolve(Some(100))
                .unwrap(),
            read(ReadRange::segment(0, 10), Some(10))
        );
    }

    #[test]
    fn negative_bounds_without_size_are_rejected() {
        for range in [
            ByteRange::new(Some(0), Some(-1)),
            ByteRange::new(Some(-5), Some(10)),
            ByteRange::new(None, Some(-1)),
        ] {
            assert!(range.needs_size(), "{range:?}");
            let err = range.resolve(None).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidRange, "{range:?}");
        }
    }

    #[test]
    fn clamps_to_size_like_python_slices() {
        // Start beyond EOF → empty, no error.
        assert_eq!(
            ByteRange::span(100, 200).resolve(Some(100)).unwrap(),
            ResolvedRange::Empty
        );
        assert_eq!(
            ByteRange::from_offset(500).resolve(Some(100)).unwrap(),
            ResolvedRange::Empty
        );
        // End beyond EOF → truncated.
        assert_eq!(
            ByteRange::span(90, 200).resolve(Some(100)).unwrap(),
            read(ReadRange::segment(90, 10), Some(10))
        );
        assert_eq!(
            ByteRange::head(1000).resolve(Some(100)).unwrap(),
            read(ReadRange::segment(0, 100), Some(100))
        );
        // Whole object with known size carries the size as a hint.
        assert_eq!(
            ByteRange::ALL.resolve(Some(100)).unwrap(),
            read(ReadRange::all(), Some(100))
        );
        // Zero-sized object: everything is empty.
        assert_eq!(
            ByteRange::from_offset(0).resolve(Some(0)).unwrap(),
            ResolvedRange::Empty
        );
    }

    #[test]
    fn range_conversions() {
        assert_eq!(ByteRange::from(..), ByteRange::ALL);
        assert_eq!(ByteRange::from(..10), ByteRange::head(10));
        assert_eq!(ByteRange::from(10..), ByteRange::from_offset(10));
        assert_eq!(ByteRange::from(1..3), ByteRange::span(1, 3));
    }
}
