//! Shell-style pattern matching with `fsspec` semantics.
//!
//! * `*` matches any run of characters **within** one path segment,
//! * `?` matches one character within a segment,
//! * `[seq]` / `[!seq]` match one character from / outside a set (ranges like
//!   `a-z` allowed),
//! * `**` as a whole segment matches zero or more segments in the middle of a
//!   pattern and one or more segments at the end (`b/**` matches `b/x` and
//!   `b/x/y` but not `b` itself — exactly like `fsspec.utils.glob_translate`).

/// `true` if `s` contains any glob metacharacter.
pub(crate) fn has_magic(s: &str) -> bool {
    s.contains(['*', '?', '['])
}

/// The longest leading path (whole segments only) that contains no
/// metacharacters — the directory a `find` must start from.
///
/// `"b/data/*.parquet"` → `"b/data"`, `"b/dat*/x"` → `"b"`, `"*"` → `""`.
pub(crate) fn root(pattern: &str) -> &str {
    let mut end = 0;
    for (idx, segment) in split_indices(pattern) {
        if has_magic(segment) {
            break;
        }
        end = idx + segment.len();
    }
    // Drop the trailing slash of a pattern such as "b/data/*" root "b/data".
    pattern[..end].trim_end_matches('/')
}

/// Does `path` match `pattern`? Both are `bucket/key` paths; trailing slashes
/// are ignored.
pub(crate) fn matches(pattern: &str, path: &str) -> bool {
    let pat: Vec<&str> = pattern.trim_matches('/').split('/').collect();
    let segs: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    match_segments(&pat, &segs)
}

fn split_indices(s: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut start = 0;
    s.split('/').map(move |seg| {
        let idx = start;
        start += seg.len() + 1;
        (idx, seg)
    })
}

fn match_segments(pat: &[&str], segs: &[&str]) -> bool {
    match pat.split_first() {
        None => segs.is_empty(),
        Some((&"**", rest)) => {
            // Trailing `**` needs at least one segment; a middle one may be empty.
            let min = usize::from(rest.is_empty());
            (min..=segs.len()).any(|skip| match_segments(rest, &segs[skip..]))
        }
        Some((first, rest)) => match segs.split_first() {
            Some((seg, seg_rest)) => {
                match_segment(first.as_bytes(), seg.as_bytes()) && match_segments(rest, seg_rest)
            }
            None => false,
        },
    }
}

/// Match one segment (no `/` inside) with `*`, `?` and `[...]`.
fn match_segment(pat: &[u8], s: &[u8]) -> bool {
    match pat.first() {
        None => s.is_empty(),
        Some(b'*') => {
            // Collapse consecutive stars, then try every split point.
            let rest = &pat[pat.iter().take_while(|&&c| c == b'*').count()..];
            (0..=s.len()).any(|i| match_segment(rest, &s[i..]))
        }
        Some(b'?') => !s.is_empty() && match_segment(&pat[1..], &s[1..]),
        Some(b'[') => match parse_class(&pat[1..]) {
            Some((class, negate, consumed)) => {
                !s.is_empty()
                    && class_contains(class, s[0]) != negate
                    && match_segment(&pat[1 + consumed..], &s[1..])
            }
            // Unterminated class: treat '[' literally, like fnmatch.
            None => !s.is_empty() && s[0] == b'[' && match_segment(&pat[1..], &s[1..]),
        },
        Some(&c) => !s.is_empty() && s[0] == c && match_segment(&pat[1..], &s[1..]),
    }
}

/// Parse the body of a `[...]` class (after the `[`). Returns the class
/// bytes, whether it is negated, and how many bytes were consumed including
/// the closing `]`.
fn parse_class(pat: &[u8]) -> Option<(&[u8], bool, usize)> {
    let (negate, body_start) = match pat.first() {
        Some(b'!') | Some(b'^') => (true, 1),
        _ => (false, 0),
    };
    // A ']' right after the opening is literal, so start searching after it.
    let search_from = if pat.get(body_start) == Some(&b']') {
        body_start + 1
    } else {
        body_start
    };
    let close = search_from + pat[search_from..].iter().position(|&c| c == b']')?;
    Some((&pat[body_start..close], negate, close + 1))
}

fn class_contains(class: &[u8], c: u8) -> bool {
    let mut i = 0;
    while i < class.len() {
        if i + 2 < class.len() && class[i + 1] == b'-' {
            if class[i] <= c && c <= class[i + 2] {
                return true;
            }
            i += 3;
        } else {
            if class[i] == c {
                return true;
            }
            i += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots() {
        assert_eq!(root("b/data/*.parquet"), "b/data");
        assert_eq!(root("b/dat*/x"), "b");
        assert_eq!(root("b/**"), "b");
        assert_eq!(root("b/data/"), "b/data");
        assert_eq!(root("b/data"), "b/data");
        assert_eq!(root("*"), "");
        assert_eq!(root("b/[ab]/c"), "b");
    }

    #[test]
    fn star_does_not_cross_separator() {
        assert!(matches("b/*.txt", "b/a.txt"));
        assert!(!matches("b/*.txt", "b/x/a.txt"));
        assert!(matches("b/*/a.txt", "b/x/a.txt"));
        assert!(!matches("b/*", "b"));
        assert!(matches("b/*", "b/x"));
        assert!(matches("b/*", "b/.hidden"));
    }

    #[test]
    fn double_star() {
        assert!(matches("b/**", "b/x"));
        assert!(matches("b/**", "b/x/y/z"));
        assert!(!matches("b/**", "b"));
        assert!(matches("b/**/c", "b/c"));
        assert!(matches("b/**/c", "b/x/c"));
        assert!(matches("b/**/c", "b/x/y/c"));
        assert!(!matches("b/**/c", "b/x/y/d"));
        assert!(matches("b/**/*.parquet", "b/x/y/p.parquet"));
        assert!(matches("b/**/*.parquet", "b/p.parquet"));
    }

    #[test]
    fn question_and_classes() {
        assert!(matches("b/file?.txt", "b/file1.txt"));
        assert!(!matches("b/file?.txt", "b/file10.txt"));
        assert!(matches("b/file[0-9].txt", "b/file7.txt"));
        assert!(!matches("b/file[0-9].txt", "b/filex.txt"));
        assert!(matches("b/file[!0-9].txt", "b/filex.txt"));
        assert!(matches("b/f[]a]", "b/f]"));
        assert!(matches("b/f[", "b/f["));
        assert!(matches("b/*/**/x", "b/a/x"));
    }

    #[test]
    fn literal_and_trailing_slashes() {
        assert!(matches("b/a/c", "b/a/c"));
        assert!(matches("b/a/c/", "b/a/c"));
        assert!(!matches("b/a/c", "b/a/cc"));
        assert!(matches("b/a/*/", "b/a/c/"));
    }
}
