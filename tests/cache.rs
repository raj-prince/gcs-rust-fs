//! `CachedFs` over the in-memory store: what is served from the cache, what
//! goes to the backend, and what every mutating operation invalidates.
//!
//! The backend handle is kept alongside the cached one. Because `MemoryFs`
//! clones share a store, writing through the backend handle is exactly what
//! "another process" looks like to the cache.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{fixture, paths, MemoryFs};
use gcs_rust_fs::{
    CacheConfig, CacheStats, CachedFs, CopyOptions, Entry, ErrorKind, FileSystem, FindOptions,
    ListOptions, MkdirOptions, OpenOptions, RmOptions, WalkOptions, WriteOptions,
};

fn cached(config: CacheConfig) -> (CachedFs<MemoryFs>, MemoryFs) {
    let backend = fixture();
    (CachedFs::new(backend.clone(), config), backend)
}

fn negative() -> CacheConfig {
    CacheConfig {
        negative: true,
        ..Default::default()
    }
}

fn hits_misses(fs: &CachedFs<MemoryFs>) -> (u64, u64) {
    let CacheStats { hits, misses, .. } = fs.stats();
    (hits, misses)
}

async fn ls(fs: &impl FileSystem, path: &str) -> Vec<Entry> {
    fs.ls(path, ListOptions::default()).await.unwrap()
}

async fn write(fs: &impl FileSystem, path: &str) {
    fs.pipe_file(path, Bytes::from_static(b"new"), WriteOptions::default())
        .await
        .unwrap();
}

fn recursive() -> CopyOptions {
    CopyOptions {
        recursive: true,
        ..Default::default()
    }
}

/// Lists `dir` through the cache and asserts whether that was a hit.
async fn assert_ls(fs: &CachedFs<MemoryFs>, dir: &str, expect_hit: bool) -> Vec<Entry> {
    let before = hits_misses(fs);
    let out = ls(fs, dir).await;
    let after = hits_misses(fs);
    let delta = (after.0 - before.0, after.1 - before.1);
    let expected = if expect_hit { (1, 0) } else { (0, 1) };
    assert_eq!(delta, expected, "ls({dir:?}) hit={expect_hit}");
    out
}

// ===========================================================================
// Reads: hits, misses and the shared ls/info store
// ===========================================================================

#[tokio::test]
async fn ls_is_served_from_cache_after_the_first_call() {
    let (fs, backend) = cached(CacheConfig::default());
    let first = assert_ls(&fs, "b/data", false).await;
    let second = assert_ls(&fs, "b/data", true).await;
    assert_eq!(first, second);
    assert_eq!(first, ls(&backend, "b/data").await);
    assert_eq!(hits_misses(&fs), (1, 1));
}

#[tokio::test]
async fn info_is_served_from_a_cached_listing() {
    let (fs, backend) = cached(CacheConfig::default());
    ls(&fs, "b/data").await;

    // Children of the listed directory, file and directory alike.
    let file = fs.info("b/data/a.parquet").await.unwrap();
    assert_eq!(file, backend.info("b/data/a.parquet").await.unwrap());
    let dir = fs.info("b/data/nested").await.unwrap();
    assert!(dir.is_dir());
    // The listed directory itself is known to exist even though its parent
    // was never listed.
    let listed = fs.info("b/data").await.unwrap();
    assert_eq!(listed, backend.info("b/data").await.unwrap());

    assert_eq!(hits_misses(&fs), (3, 1));
    assert_eq!(fs.size("b/data/a.parquet").await.unwrap(), 4);
    assert!(fs.is_dir("b/data/nested").await.unwrap());
    assert_eq!(hits_misses(&fs), (5, 1), "derived ops run on the cache");
}

#[tokio::test]
async fn info_results_are_cached_without_becoming_a_listing() {
    let (fs, _) = cached(CacheConfig::default());
    fs.info("b/data/a.parquet").await.unwrap();
    fs.info("b/data/a.parquet").await.unwrap();
    assert_eq!(hits_misses(&fs), (1, 1));

    // A partial node must not answer `ls`, nor say anything about names it
    // has not seen.
    assert_ls(&fs, "b/data", false).await;
    fs.info("b/data/b.parquet").await.unwrap();
    assert_eq!(hits_misses(&fs), (2, 2), "listing warmed the sibling");
}

#[tokio::test]
async fn ls_of_a_file_caches_that_file_only() {
    let (fs, _) = cached(CacheConfig::default());
    let out = assert_ls(&fs, "b/root.txt", false).await;
    assert_eq!(paths(&out), ["b/root.txt"]);
    fs.info("b/root.txt").await.unwrap();
    assert_eq!(hits_misses(&fs), (1, 1));
    assert_ls(&fs, "b", false).await;
}

#[tokio::test]
async fn missing_names_go_to_the_backend_unless_negative_is_enabled() {
    let (fs, _) = cached(CacheConfig::default());
    ls(&fs, "b/data").await;
    let err = fs.info("b/data/missing").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(!fs.exists("b/data/missing").await.unwrap());
    assert_eq!(hits_misses(&fs), (0, 3), "default: every miss is verified");

    let (fs, _) = cached(negative());
    ls(&fs, "b/data").await;
    let err = fs.info("b/data/missing").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    let err = fs
        .ls("b/data/missing", ListOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(
        hits_misses(&fs),
        (2, 1),
        "negative: answered from the listing"
    );

    // Only a complete listing can vouch for absence; a node that merely holds
    // individual `info` results cannot.
    fs.info("b/root.txt").await.unwrap();
    assert!(!fs.exists("b/nope").await.unwrap());
    assert_eq!(hits_misses(&fs), (2, 3));
}

#[tokio::test]
async fn versions_and_generation_paths_bypass_the_cache() {
    let (fs, _) = cached(CacheConfig::default());
    let versions = ListOptions {
        versions: true,
        ..Default::default()
    };
    fs.ls("b/data", versions).await.unwrap();
    fs.info("b/data/a.parquet#1").await.unwrap();
    assert_eq!(hits_misses(&fs), (0, 0), "neither consulted nor filled");
    assert_ls(&fs, "b/data", false).await;
}

#[tokio::test]
async fn refresh_bypasses_and_replaces_the_cached_listing() {
    let (fs, backend) = cached(CacheConfig::default());
    ls(&fs, "b/data").await;
    write(&backend, "b/data/external.bin").await;

    let stale = assert_ls(&fs, "b/data", true).await;
    assert!(!paths(&stale).contains(&"b/data/external.bin"));

    let refresh = ListOptions {
        refresh: true,
        ..Default::default()
    };
    let fresh = fs.ls("b/data", refresh).await.unwrap();
    assert!(paths(&fresh).contains(&"b/data/external.bin"));
    assert_eq!(hits_misses(&fs), (1, 2));

    let again = assert_ls(&fs, "b/data", true).await;
    assert_eq!(again, fresh);
}

#[tokio::test]
async fn external_writes_are_invisible_until_invalidated() {
    // The documented contract: the cache is exact for this client's own
    // writes; other writers are reconciled by `invalidate_cache` (or a TTL).
    let (fs, backend) = cached(negative());
    ls(&fs, "b/data").await;
    write(&backend, "b/data/external.bin").await;

    assert!(!fs.exists("b/data/external.bin").await.unwrap());
    fs.invalidate_cache(Some("b/data"));
    assert!(fs.exists("b/data/external.bin").await.unwrap());
    assert!(paths(&ls(&fs, "b/data").await).contains(&"b/data/external.bin"));

    // Without negative caching a new file is found as soon as it is asked
    // for, because absence is always verified with the backend.
    let (fs, backend) = cached(CacheConfig::default());
    ls(&fs, "b/data").await;
    write(&backend, "b/data/external.bin").await;
    assert!(fs.exists("b/data/external.bin").await.unwrap());
}

// ===========================================================================
// Populating from find; derived operations on top
// ===========================================================================

#[tokio::test]
async fn complete_find_populates_every_directory_it_visits() {
    let (fs, backend) = cached(CacheConfig::default());
    let withdirs = FindOptions {
        withdirs: true,
        ..Default::default()
    };
    fs.find("b", withdirs).await.unwrap();
    assert_eq!(hits_misses(&fs), (0, 0), "find itself is not counted");

    for dir in [
        "b",
        "b/data",
        "b/data/nested",
        "b/data/nested/deep",
        "b/logs",
        "b/logs/2024",
    ] {
        let listing = assert_ls(&fs, dir, true).await;
        assert_eq!(listing, ls(&backend, dir).await, "{dir}");
    }
    fs.info("b/data/nested/deep/d.parquet").await.unwrap();
    assert_eq!(hits_misses(&fs), (7, 0));

    // `walk` and `du` are derived from `info`/`ls`, so they are now free.
    let before = hits_misses(&fs);
    fs.walk("b", WalkOptions::default()).await.unwrap();
    fs.du("b", Default::default()).await.unwrap();
    assert_eq!(hits_misses(&fs).1, before.1, "no backend calls");
}

#[tokio::test]
async fn partial_finds_populate_nothing() {
    for (opts, config) in [
        (FindOptions::default(), CacheConfig::default()),
        (
            FindOptions {
                withdirs: true,
                maxdepth: Some(1),
                ..Default::default()
            },
            CacheConfig::default(),
        ),
        (
            FindOptions {
                withdirs: true,
                ..Default::default()
            },
            CacheConfig {
                populate_from_find: false,
                ..Default::default()
            },
        ),
    ] {
        let (fs, _) = cached(config);
        fs.find("b", opts.clone()).await.unwrap();
        assert_ls(&fs, "b", false).await;
    }
}

// ===========================================================================
// Invalidation by this client's own mutations
// ===========================================================================

#[tokio::test]
async fn a_write_drops_only_a_cached_parent() {
    let (fs, _) = cached(CacheConfig::default());
    ls(&fs, "b").await;
    ls(&fs, "b/data").await;

    write(&fs, "b/data/new.bin").await;
    // gcsfs `_write_file_cache_update`: the parent was cached, so only its
    // listing changed; the grandparent still lists the same names.
    let data = assert_ls(&fs, "b/data", false).await;
    assert!(paths(&data).contains(&"b/data/new.bin"));
    assert_ls(&fs, "b", true).await;
    assert_eq!(fs.stats().invalidations, 1);
}

#[tokio::test]
async fn a_write_below_an_uncached_parent_drops_every_ancestor() {
    let (fs, _) = cached(CacheConfig::default());
    ls(&fs, "").await;
    ls(&fs, "b").await;
    ls(&fs, "other").await;

    // `b/logs/2025` did not exist: the write created an implicit directory,
    // which changes `b/logs` and could have changed anything above it.
    write(&fs, "b/logs/2025/y.log").await;
    let b = assert_ls(&fs, "b", false).await;
    assert!(paths(&b).contains(&"b/logs"));
    assert_ls(&fs, "", false).await;
    assert_ls(&fs, "other", true).await;
    assert!(paths(&ls(&fs, "b/logs").await).contains(&"b/logs/2025"));
}

#[tokio::test]
async fn write_handles_invalidate_at_open_and_at_close() {
    let (fs, _) = cached(CacheConfig::default());
    ls(&fs, "b/data").await;

    let mut file = fs.open("b/data/w.bin", OpenOptions::write()).await.unwrap();
    let during = assert_ls(&fs, "b/data", false).await;
    assert!(
        !paths(&during).contains(&"b/data/w.bin"),
        "not published yet"
    );

    file.write(Bytes::from_static(b"w")).await.unwrap();
    file.close().await.unwrap();
    let after = assert_ls(&fs, "b/data", false).await;
    assert!(paths(&after).contains(&"b/data/w.bin"));

    // Reading never invalidates.
    let mut reader = fs
        .open("b/data/a.parquet", OpenOptions::read())
        .await
        .unwrap();
    reader.read(None).await.unwrap();
    assert_ls(&fs, "b/data", true).await;

    // A discarded handle publishes nothing, so nothing is dropped.
    let mut aborted = fs
        .open("b/data/never.bin", OpenOptions::write())
        .await
        .unwrap();
    assert_ls(&fs, "b/data", false).await; // the open itself invalidated
    aborted.discard().await.unwrap();
    assert_ls(&fs, "b/data", true).await;
}

#[tokio::test]
async fn deletes_drop_the_subtree_and_every_ancestor() {
    let (fs, _) = cached(CacheConfig::default());
    for dir in ["", "b", "b/data", "b/data/nested", "b/logs", "other"] {
        ls(&fs, dir).await;
    }

    fs.rm_file("b/data/a.parquet").await.unwrap();
    let data = assert_ls(&fs, "b/data", false).await;
    assert!(!paths(&data).contains(&"b/data/a.parquet"));
    assert_ls(&fs, "b", false).await;
    assert_ls(&fs, "", false).await;
    // Siblings and unrelated subtrees are untouched.
    assert_ls(&fs, "b/data/nested", true).await;
    assert_ls(&fs, "b/logs", true).await;
    assert_ls(&fs, "other", true).await;

    let recursive = RmOptions {
        recursive: true,
        ..Default::default()
    };
    fs.rm("b/data/nested", recursive).await.unwrap();
    assert_eq!(
        fs.ls("b/data/nested", ListOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    assert_ls(&fs, "b/data", false).await;
    assert_ls(&fs, "b/logs", true).await;
}

#[tokio::test]
async fn directory_operations_invalidate_like_writes_and_deletes() {
    let (fs, _) = cached(CacheConfig::default());
    ls(&fs, "").await;
    ls(&fs, "b").await;

    let placeholder = MkdirOptions {
        placeholder: true,
        ..Default::default()
    };
    fs.mkdir("b/newdir", placeholder).await.unwrap();
    assert!(paths(&assert_ls(&fs, "b", false).await).contains(&"b/newdir"));
    assert_ls(&fs, "", true).await;

    fs.mkdir("third", MkdirOptions::default()).await.unwrap();
    assert!(paths(&assert_ls(&fs, "", false).await).contains(&"third"));

    fs.rmdir("b/newdir").await.unwrap();
    assert!(!paths(&assert_ls(&fs, "b", false).await).contains(&"b/newdir"));
}

#[tokio::test]
async fn moves_drop_source_and_destination() {
    let (fs, _) = cached(CacheConfig::default());
    for dir in ["b", "b/data", "b/data/nested", "b/logs"] {
        ls(&fs, dir).await;
    }

    fs.mv("b/data", "b/moved", recursive()).await.unwrap();
    let b = assert_ls(&fs, "b", false).await;
    assert!(paths(&b).contains(&"b/moved") && !paths(&b).contains(&"b/data"));
    // Both the moved directory and its cached subdirectory are gone: the
    // backend's NotFound reaches us instead of a stale listing.
    for gone in ["b/data", "b/data/nested"] {
        let before = hits_misses(&fs);
        let err = fs.ls(gone, ListOptions::default()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound, "{gone}");
        assert_eq!(hits_misses(&fs), (before.0, before.1 + 1), "{gone}");
    }
    assert_ls(&fs, "b/logs", true).await;

    ls(&fs, "b/moved").await;
    fs.move_file("b/moved/a.parquet", "b/logs/a.parquet")
        .await
        .unwrap();
    assert_eq!(
        fs.info("b/moved/a.parquet").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert!(fs.info("b/logs/a.parquet").await.unwrap().is_file());
    assert_ls(&fs, "b/moved", false).await;
    assert_ls(&fs, "b/logs", false).await;
}

#[tokio::test]
async fn copies_drop_the_destination_only() {
    let (fs, _) = cached(CacheConfig::default());
    ls(&fs, "b/data").await;
    ls(&fs, "b/logs").await;

    fs.copy_file("b/data/a.parquet", "b/logs/a.parquet")
        .await
        .unwrap();
    assert!(paths(&assert_ls(&fs, "b/logs", false).await).contains(&"b/logs/a.parquet"));
    assert_ls(&fs, "b/data", true).await;

    fs.copy("b/data", "b/copy", recursive()).await.unwrap();
    assert_ls(&fs, "b/data", true).await;
    assert_eq!(ls(&fs, "b/copy").await.len(), ls(&fs, "b/data").await.len());
}

#[tokio::test]
async fn invalidate_cache_drops_a_subtree_with_its_ancestors_or_everything() {
    let (fs, _) = cached(CacheConfig::default());
    for dir in ["", "b", "b/data", "b/data/nested", "b/logs", "other"] {
        ls(&fs, dir).await;
    }

    fs.invalidate_cache(Some("b/data"));
    assert_ls(&fs, "b/data", false).await;
    assert_ls(&fs, "b/data/nested", false).await;
    assert_ls(&fs, "b", false).await;
    assert_ls(&fs, "", false).await;
    assert_ls(&fs, "b/logs", true).await;
    assert_ls(&fs, "other", true).await;

    fs.invalidate_cache(None);
    for dir in ["", "b", "b/data", "b/data/nested", "b/logs", "other"] {
        assert_ls(&fs, dir, false).await;
    }
    assert_eq!(fs.stats().invalidations, 2);

    // On a filesystem that caches nothing the method is a no-op.
    fixture().invalidate_cache(None);
}

// ===========================================================================
// Configuration and sharing
// ===========================================================================

#[tokio::test]
async fn ttl_expires_cached_listings() {
    let (fs, _) = cached(CacheConfig {
        ttl: Some(Duration::from_millis(20)),
        ..Default::default()
    });
    assert_ls(&fs, "b/data", false).await;
    assert_ls(&fs, "b/data", true).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_ls(&fs, "b/data", false).await;
}

#[tokio::test]
async fn max_dirs_evicts_the_least_recently_used_directory() {
    let (fs, _) = cached(CacheConfig {
        max_dirs: Some(2),
        ..Default::default()
    });
    ls(&fs, "b/data").await;
    ls(&fs, "b/logs").await;
    assert_ls(&fs, "b/data", true).await; // b/logs is now the oldest
    ls(&fs, "other").await;
    assert_ls(&fs, "b/data", true).await;
    assert_ls(&fs, "b/logs", false).await;
}

#[tokio::test]
async fn clones_share_the_cache_and_a_trait_object_still_works() {
    let (fs, _) = cached(CacheConfig::default());
    let twin = fs.clone();
    ls(&fs, "b/data").await;
    assert_ls(&twin, "b/data", true).await;
    assert_eq!(fs.stats(), twin.stats());

    let shared: Arc<dyn FileSystem> = Arc::new(fs);
    shared.ls("b/data", ListOptions::default()).await.unwrap();
    shared.invalidate_cache(None);
    assert_eq!(twin.stats().invalidations, 1);
}
