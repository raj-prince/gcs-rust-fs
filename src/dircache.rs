//! The directory-node store behind [`CachedFs`](crate::CachedFs).
//!
//! One structure serves both `ls` and `info`: every cached [`Entry`] lives as
//! a child of its parent directory's [`DirNode`], and a node additionally
//! records whether it holds a *complete* listing. A listing warms `info` for
//! every child; an individual `info` result is kept in a *partial* node that
//! can never be mistaken for a listing. Eviction is per node, so a listing is
//! served whole or not at all.
//!
//! Keys are canonical directory paths (`bucket/dir`, no scheme, no slashes at
//! either end); the root is `""`. The store knows nothing about time or
//! locking: callers pass `now` and wrap it in a mutex.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use crate::entry::{self, Entry};

/// Result of looking a single name up in a directory node.
#[derive(Debug, PartialEq)]
pub(crate) enum Lookup {
    /// A fresh entry (boxed: [`Entry`] is large and the other variants are
    /// unit-like).
    Hit(Box<Entry>),
    /// The directory holds a fresh, complete listing and the name is not in it.
    Absent,
    /// Nothing can be concluded (no node, expired, or partial without the name).
    Unknown,
}

struct Cached {
    entry: Entry,
    at: Instant,
}

struct DirNode {
    /// When a complete listing was stored; `None` for a partial node that
    /// only holds children learned from individual `info` calls.
    complete: Option<Instant>,
    /// Children keyed by basename.
    children: HashMap<String, Cached>,
    /// Recency tick, also the key into [`DirCache::recency`].
    last_used: u64,
}

pub(crate) struct DirCache {
    dirs: HashMap<String, DirNode>,
    /// Recency index: tick → key, oldest first. Kept in lock-step with `dirs`.
    recency: BTreeMap<u64, String>,
    tick: u64,
    ttl: Option<Duration>,
    max_dirs: Option<usize>,
}

impl DirCache {
    pub(crate) fn new(ttl: Option<Duration>, max_dirs: Option<usize>) -> Self {
        Self {
            dirs: HashMap::new(),
            recency: BTreeMap::new(),
            tick: 0,
            ttl,
            max_dirs: max_dirs.map(|n| n.max(1)),
        }
    }

    fn fresh(&self, at: Instant, now: Instant) -> bool {
        self.ttl
            .is_none_or(|ttl| now.saturating_duration_since(at) < ttl)
    }

    /// Mark `key` as most recently used.
    fn touch(&mut self, key: &str) {
        if let Some(node) = self.dirs.get_mut(key) {
            self.recency.remove(&node.last_used);
            self.tick += 1;
            node.last_used = self.tick;
            self.recency.insert(self.tick, key.to_owned());
        }
    }

    /// The complete, fresh listing of `dir`, sorted by path.
    pub(crate) fn listing(&mut self, dir: &str, now: Instant) -> Option<Vec<Entry>> {
        let node = self.dirs.get(dir)?;
        if !self.fresh(node.complete?, now) {
            return None;
        }
        let mut out: Vec<Entry> = node.children.values().map(|c| c.entry.clone()).collect();
        out.sort_by(|a, b| a.path.cmp(&b.path));
        self.touch(dir);
        Some(out)
    }

    /// `true` when `dir` holds a complete, fresh listing (so it is known to be
    /// a directory).
    pub(crate) fn is_complete_dir(&mut self, dir: &str, now: Instant) -> bool {
        let fresh = match self.dirs.get(dir).and_then(|n| n.complete) {
            Some(at) => self.fresh(at, now),
            None => false,
        };
        if fresh {
            self.touch(dir);
        }
        fresh
    }

    /// Look `name` up among the children of `parent`.
    pub(crate) fn entry(&mut self, parent: &str, name: &str, now: Instant) -> Lookup {
        let Some(node) = self.dirs.get(parent) else {
            return Lookup::Unknown;
        };
        let result = match node.children.get(name) {
            Some(c) if self.fresh(c.at, now) => Lookup::Hit(Box::new(c.entry.clone())),
            Some(_) => Lookup::Unknown,
            None => match node.complete {
                Some(at) if self.fresh(at, now) => Lookup::Absent,
                _ => Lookup::Unknown,
            },
        };
        if result != Lookup::Unknown {
            self.touch(parent);
        }
        result
    }

    /// Store the complete listing of `dir`, replacing whatever was there.
    pub(crate) fn put_listing(&mut self, dir: &str, entries: &[Entry], now: Instant) {
        let children = entries
            .iter()
            .map(|e| {
                (
                    entry::basename(&e.path).to_owned(),
                    Cached {
                        entry: e.clone(),
                        at: now,
                    },
                )
            })
            .collect();
        self.insert(
            dir,
            DirNode {
                complete: Some(now),
                children,
                last_used: 0,
            },
        );
    }

    /// Record a single entry under its parent, creating a partial node if the
    /// parent is not cached. A complete parent stays complete.
    pub(crate) fn put_entry(&mut self, e: &Entry, now: Instant) {
        let Some(parent) = parent_key(&e.path) else {
            return;
        };
        let name = entry::basename(&e.path).to_owned();
        let cached = Cached {
            entry: e.clone(),
            at: now,
        };
        if let Some(node) = self.dirs.get_mut(parent) {
            node.children.insert(name, cached);
            self.touch(parent);
        } else {
            let children = HashMap::from([(name, cached)]);
            self.insert(
                parent,
                DirNode {
                    complete: None,
                    children,
                    last_used: 0,
                },
            );
        }
    }

    fn insert(&mut self, key: &str, mut node: DirNode) {
        self.remove(key);
        self.tick += 1;
        node.last_used = self.tick;
        self.recency.insert(self.tick, key.to_owned());
        self.dirs.insert(key.to_owned(), node);
        if let Some(max) = self.max_dirs {
            while self.dirs.len() > max {
                let Some((_, oldest)) = self.recency.pop_first() else {
                    break;
                };
                self.dirs.remove(&oldest);
            }
        }
    }

    fn remove(&mut self, key: &str) -> bool {
        match self.dirs.remove(key) {
            Some(node) => {
                self.recency.remove(&node.last_used);
                true
            }
            None => false,
        }
    }

    pub(crate) fn contains(&self, key: &str) -> bool {
        self.dirs.contains_key(key)
    }

    /// Drop the node at `path` and every node below it.
    pub(crate) fn drop_tree(&mut self, path: &str) {
        if path.is_empty() {
            return self.clear();
        }
        let prefix = format!("{path}/");
        let below: Vec<String> = self
            .dirs
            .keys()
            .filter(|k| k.as_str() == path || k.starts_with(&prefix))
            .cloned()
            .collect();
        for key in below {
            self.remove(&key);
        }
    }

    /// Drop the node at `path` and each ancestor up to and including the root.
    pub(crate) fn drop_ancestors(&mut self, path: &str) {
        let mut current = Some(path.to_owned());
        while let Some(key) = current {
            self.remove(&key);
            current = parent_key(&key).map(str::to_owned);
        }
    }

    /// Drop only the node at `path`.
    pub(crate) fn drop_node(&mut self, path: &str) {
        self.remove(path);
    }

    pub(crate) fn clear(&mut self) {
        self.dirs.clear();
        self.recency.clear();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.dirs.len()
    }
}

/// The directory a canonical path lives in: `""` for a bucket, `None` for
/// the root itself.
pub(crate) fn parent_key(path: &str) -> Option<&str> {
    if path.is_empty() {
        None
    } else {
        Some(entry::parent(path).unwrap_or(""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stat::ObjectStat;

    fn file(path: &str) -> Entry {
        Entry::file(path, ObjectStat::default())
    }

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn listing_warms_children_and_partial_nodes_stay_partial() {
        let mut c = DirCache::new(None, None);
        let t = now();
        assert_eq!(c.entry("b", "x", t), Lookup::Unknown);

        c.put_entry(&file("b/x"), t);
        assert_eq!(c.entry("b", "x", t), Lookup::Hit(Box::new(file("b/x"))));
        assert_eq!(c.entry("b", "y", t), Lookup::Unknown, "partial node");
        assert!(c.listing("b", t).is_none(), "partial node is not a listing");
        assert!(!c.is_complete_dir("b", t));

        c.put_listing("b", &[file("b/y"), file("b/x")], t);
        assert_eq!(
            c.listing("b", t)
                .unwrap()
                .iter()
                .map(|e| e.path.as_str())
                .collect::<Vec<_>>(),
            ["b/x", "b/y"]
        );
        assert_eq!(c.entry("b", "z", t), Lookup::Absent);
        assert!(c.is_complete_dir("b", t));

        // Upserting into a complete node keeps it complete.
        c.put_entry(&file("b/z"), t);
        assert_eq!(c.listing("b", t).unwrap().len(), 3);
    }

    #[test]
    fn ttl_expires_listings_and_entries() {
        let mut c = DirCache::new(Some(Duration::from_secs(10)), None);
        let t0 = now();
        c.put_listing("b", &[file("b/x")], t0);
        let t1 = t0 + Duration::from_secs(5);
        assert_eq!(c.entry("b", "x", t1), Lookup::Hit(Box::new(file("b/x"))));
        let t2 = t0 + Duration::from_secs(11);
        assert!(c.listing("b", t2).is_none());
        assert_eq!(c.entry("b", "x", t2), Lookup::Unknown);
        assert_eq!(
            c.entry("b", "nope", t2),
            Lookup::Unknown,
            "no negative from stale"
        );
    }

    #[test]
    fn lru_evicts_least_recently_used_directory() {
        let mut c = DirCache::new(None, Some(2));
        let t = now();
        c.put_listing("a", &[], t);
        c.put_listing("b", &[], t);
        assert!(c.listing("a", t).is_some()); // touch a → b is now the oldest
        c.put_listing("c", &[], t);
        assert_eq!(c.len(), 2);
        assert!(c.contains("a") && c.contains("c") && !c.contains("b"));
    }

    #[test]
    fn drop_tree_and_ancestors() {
        let mut c = DirCache::new(None, None);
        let t = now();
        for d in ["", "b", "b/x", "b/x/y", "b/z", "o"] {
            c.put_listing(d, &[], t);
        }
        c.drop_tree("b/x");
        assert!(!c.contains("b/x") && !c.contains("b/x/y"));
        assert!(c.contains("b") && c.contains("b/z") && c.contains("o") && c.contains(""));

        c.drop_ancestors("b/z");
        assert!(!c.contains("b/z") && !c.contains("b") && !c.contains(""));
        assert!(c.contains("o"));

        c.drop_tree("");
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn parent_keys() {
        assert_eq!(parent_key(""), None);
        assert_eq!(parent_key("b"), Some(""));
        assert_eq!(parent_key("b/x"), Some("b"));
        assert_eq!(parent_key("b/x/y"), Some("b/x"));
    }
}
