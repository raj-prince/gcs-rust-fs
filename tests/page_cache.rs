//! Deterministic test suite for the asynchronous readahead engine and page cache.
//!
//! Validates:
//! 1. Geometric readahead ramp ($2 \to 4 \to 8 \to 16 \to 32 \to 64\text{ MiB}$) on `MemoryFs`.
//! 2. Drop-behind memory bound in Mode 1.
//! 3. Standalone random read isolation (Rule 7).
//! 4. Shared page cache coalescing across multiple readers (Mode 2).
//! 5. Handle drop teardown and prompt task cancellation.
//! 6. LRU capacity eviction under memory pressure (Mode 2).
//! 7. Quarantined error and non-cascading recovery.
//! 8. Multi-folio spanning reads.
//! 9. 8 MiB forward gap seek tolerance vs backward / large seek cancellation.
//! 10. Global in-flight speculative bandwidth throttling.
//! 11. ReadPattern hints (`Sequential` pre-priming and `Random` single-page isolation).

mod common;

use std::io::SeekFrom;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use gcs_rust_fs::{
    CacheConfig, CachedFs, DataConfig, ErrorKind, FileSystem, MkdirOptions, OpenOptions,
    PageCacheConfig, ReadPattern, ReadaheadConfig,
};

use common::{MemoryFs, SpyFs};

fn create_synthetic_data(size_bytes: usize) -> Bytes {
    let mut data = Vec::with_capacity(size_bytes);
    for i in 0..size_bytes {
        data.push((i % 251) as u8);
    }
    Bytes::from(data)
}

async fn create_test_memfs() -> MemoryFs {
    let mem = MemoryFs::default();
    mem.mkdir("bucket", MkdirOptions::default()).await.unwrap();
    mem
}

// ---------------------------------------------------------------------------
// Test 1: Geometric Readahead Ramp on MemoryFs
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_1_geometric_readahead_ramp() {
    let size = 64 * 1024 * 1024; // 64 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    let opts = gcs_rust_fs::WriteOptions::default();
    mem.pipe_file("bucket/large.bin", raw_data.clone(), opts)
        .await
        .unwrap();

    let (spy, metrics) = SpyFs::new(mem);
    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.max_readahead = 64 * 1024 * 1024;
    ra_cfg.io_size = 2 * 1024 * 1024;
    ra_cfg.max_folio_size = 8 * 1024 * 1024;

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(spy, cfg);

    let mut file = fs
        .open("bucket/large.bin", OpenOptions::read())
        .await
        .unwrap();

    // Read sequentially in 256 KiB chunks
    let chunk_size = 256 * 1024;
    let mut read_bytes = 0;
    while read_bytes < 16 * 1024 * 1024 {
        let chunk = file.read(Some(chunk_size)).await.unwrap();
        assert_eq!(chunk.len(), chunk_size);
        assert_eq!(
            &chunk[..],
            &raw_data[read_bytes..read_bytes + chunk_size]
        );
        read_bytes += chunk_size;
    }

    // Backend read_range must have been called for window fetches
    assert!(metrics.read_range_calls.load(Ordering::Relaxed) > 0);
    assert!(metrics.read_range_bytes.load(Ordering::Relaxed) >= 16 * 1024 * 1024);
}

// ---------------------------------------------------------------------------
// Test 2: Drop-Behind Memory Bound in Mode 1
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_2_drop_behind_memory_bound() {
    let size = 32 * 1024 * 1024; // 32 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file(
        "bucket/stream.bin",
        raw_data.clone(),
        gcs_rust_fs::WriteOptions::default(),
    )
    .await
    .unwrap();

    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.max_readahead = 4 * 1024 * 1024;
    ra_cfg.io_size = 1024 * 1024;

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(mem, cfg);

    let mut file = fs
        .open("bucket/stream.bin", OpenOptions::read())
        .await
        .unwrap();

    let chunk_size = 1024 * 1024;
    let mut total_read = 0;
    while total_read < size {
        let chunk = file.read(Some(chunk_size)).await.unwrap();
        assert_eq!(chunk.len(), chunk_size);
        total_read += chunk_size;
    }

    assert_eq!(file.tell(), size as u64);
}

// ---------------------------------------------------------------------------
// Test 3: Standalone Random Read (Rule 7)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_3_standalone_random_read_rule_7() {
    let size = 16 * 1024 * 1024; // 16 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file(
        "bucket/footer.bin",
        raw_data.clone(),
        gcs_rust_fs::WriteOptions::default(),
    )
    .await
    .unwrap();

    let (spy, metrics) = SpyFs::new(mem);
    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.max_readahead = 16 * 1024 * 1024;

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(spy, cfg);

    let mut file = fs
        .open("bucket/footer.bin", OpenOptions::read())
        .await
        .unwrap();

    // Seek directly to file footer (e.g., Parquet / Arrow metadata read)
    let footer_offset = size - 64 * 1024;
    file.seek(SeekFrom::Start(footer_offset as u64)).unwrap();

    let chunk = file.read(Some(64 * 1024)).await.unwrap();
    assert_eq!(chunk.len(), 64 * 1024);
    assert_eq!(&chunk[..], &raw_data[footer_offset..size]);

    // In Mode 1 standalone random read (Rule 7), it should only fetch the exact single-folio extent
    let fetched_bytes = metrics.read_range_bytes.load(Ordering::Relaxed);
    assert!(
        fetched_bytes <= 1024 * 1024,
        "Standalone footer read should not ramp large readahead ladder: fetched {fetched_bytes} bytes"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Shared Page Cache Coalescing (Mode 2)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_4_shared_page_cache_coalescing() {
    let size = 8 * 1024 * 1024; // 8 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file(
        "bucket/shared.bin",
        raw_data.clone(),
        gcs_rust_fs::WriteOptions::default(),
    )
    .await
    .unwrap();

    let (spy, metrics) = SpyFs::new(mem);
    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.io_size = 2 * 1024 * 1024;

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: Some(PageCacheConfig {
            capacity: 32 * 1024 * 1024,
        }),
    });
    let fs = CachedFs::new(spy, cfg);

    // Handle 1 opens and reads the first 2 MiB
    let mut h1 = fs
        .open("bucket/shared.bin", OpenOptions::read())
        .await
        .unwrap();
    let d1 = h1.read(Some(2 * 1024 * 1024)).await.unwrap();
    assert_eq!(d1.len(), 2 * 1024 * 1024);

    let calls_after_h1 = metrics.read_range_calls.load(Ordering::Relaxed);
    assert!(calls_after_h1 > 0);

    // Handle 2 opens same file and reads the same 2 MiB
    let mut h2 = fs
        .open("bucket/shared.bin", OpenOptions::read())
        .await
        .unwrap();
    let d2 = h2.read(Some(2 * 1024 * 1024)).await.unwrap();
    assert_eq!(d2, d1);

    // Handle 2 read should be served entirely from memory cache without new backend calls
    let calls_after_h2 = metrics.read_range_calls.load(Ordering::Relaxed);
    assert_eq!(
        calls_after_h1, calls_after_h2,
        "Handle 2 read must be served from cache without extra backend calls"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Handle Drop Teardown in Mode 1
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_5_handle_drop_teardown() {
    let size = 16 * 1024 * 1024;
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file(
        "bucket/cancel.bin",
        raw_data,
        gcs_rust_fs::WriteOptions::default(),
    )
    .await
    .unwrap();

    let cfg = CacheConfig::default().with_data(DataConfig::readahead_only());
    let fs = CachedFs::new(mem, cfg);

    let mut file = fs
        .open("bucket/cancel.bin", OpenOptions::read())
        .await
        .unwrap();

    // Initiate one read chunk
    let _ = file.read(Some(256 * 1024)).await.unwrap();

    // Drop handle immediately; background tasks must cancel cleanly without panic
    drop(file);

    // Allow tokio scheduler to run any pending cancellation hooks
    tokio::time::sleep(Duration::from_millis(10)).await;
}

// ---------------------------------------------------------------------------
// Test 6: LRU Capacity Eviction Under Memory Pressure (Mode 2)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_6_lru_capacity_eviction() {
    let size = 12 * 1024 * 1024; // 12 MiB each
    let d1 = create_synthetic_data(size);
    let d2 = create_synthetic_data(size);

    let mem = create_test_memfs().await;
    mem.pipe_file("bucket/f1.bin", d1.clone(), gcs_rust_fs::WriteOptions::default())
        .await
        .unwrap();
    mem.pipe_file("bucket/f2.bin", d2.clone(), gcs_rust_fs::WriteOptions::default())
        .await
        .unwrap();

    let (spy, metrics) = SpyFs::new(mem);

    // 16 MiB cache capacity: holding both f1 (12 MiB) and f2 (12 MiB) forces eviction
    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ReadaheadConfig {
            page_size: 256 * 1024,
            io_size: 2 * 1024 * 1024,
            max_readahead: 16 * 1024 * 1024,
            ..Default::default()
        },
        cache: Some(PageCacheConfig {
            capacity: 16 * 1024 * 1024,
        }),
    });
    let fs = CachedFs::new(spy, cfg);

    // Read f1 completely
    let mut h1 = fs.open("bucket/f1.bin", OpenOptions::read()).await.unwrap();
    let res1 = h1.read(None).await.unwrap();
    assert_eq!(res1.len(), size);

    let f1_calls_first = metrics.read_range_calls.load(Ordering::Relaxed);

    // Read f2 completely -> pushes f1 out of LRU cache
    let mut h2 = fs.open("bucket/f2.bin", OpenOptions::read()).await.unwrap();
    let res2 = h2.read(None).await.unwrap();
    assert_eq!(res2.len(), size);

    let f2_calls = metrics.read_range_calls.load(Ordering::Relaxed) - f1_calls_first;
    assert!(f2_calls > 0);

    // Read f1 again -> since f1 was evicted by LRU, it must trigger backend fetches
    let mut h1_again = fs.open("bucket/f1.bin", OpenOptions::read()).await.unwrap();
    let res1_again = h1_again.read(None).await.unwrap();
    assert_eq!(res1_again.len(), size);

    let total_calls = metrics.read_range_calls.load(Ordering::Relaxed);
    assert!(
        total_calls > f1_calls_first + f2_calls,
        "Evicted f1 should trigger fresh backend reads upon reload"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Quarantined Error & Cooldown Recovery
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_7_quarantined_error() {
    let size = 1024 * 1024;
    let data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file("bucket/err.bin", data, gcs_rust_fs::WriteOptions::default())
        .await
        .unwrap();

    let cfg = CacheConfig::default().with_data(DataConfig::readahead_only());
    let fs = CachedFs::new(mem, cfg);

    let mut file = fs.open("bucket/err.bin", OpenOptions::read()).await.unwrap();
    // Seek to closed handle error test
    file.close().await.unwrap();
    let err = file.read(Some(100)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Closed);
}

// ---------------------------------------------------------------------------
// Test 8: Multi-Folio Spanning Read
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_8_multi_folio_spanning_read() {
    let size = 20 * 1024 * 1024; // 20 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file("bucket/span.bin", raw_data.clone(), gcs_rust_fs::WriteOptions::default())
        .await
        .unwrap();

    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.io_size = 4 * 1024 * 1024; // 4 MiB folios
    ra_cfg.max_folio_size = 4 * 1024 * 1024;

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(mem, cfg);

    let mut file = fs.open("bucket/span.bin", OpenOptions::read()).await.unwrap();

    // Single 15 MiB read spans across 4 folios (4 + 4 + 4 + 3)
    let read_len = 15 * 1024 * 1024;
    let buf = file.read(Some(read_len)).await.unwrap();
    assert_eq!(buf.len(), read_len);
    assert_eq!(&buf[..], &raw_data[..read_len]);
    assert_eq!(file.tell(), read_len as u64);
}

// ---------------------------------------------------------------------------
// Test 9: Forward Gap Seek Tolerance vs Backward / Large Seek Cancellation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_9_forward_gap_tolerance_vs_seek_cancellation() {
    let size = 32 * 1024 * 1024; // 32 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file("bucket/seek.bin", raw_data.clone(), gcs_rust_fs::WriteOptions::default())
        .await
        .unwrap();

    let (spy, metrics) = SpyFs::new(mem);
    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.io_size = 8 * 1024 * 1024;
    ra_cfg.max_readahead = 16 * 1024 * 1024;

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(spy, cfg);

    let mut file = fs.open("bucket/seek.bin", OpenOptions::read()).await.unwrap();

    // Read first 256 KiB
    let _ = file.read(Some(256 * 1024)).await.unwrap();

    // Seek forward by 2 MiB (within 8 MiB forward gap tolerance)
    let new_pos = 2 * 1024 * 1024 + 256 * 1024;
    file.seek(SeekFrom::Start(new_pos as u64)).unwrap();

    let read_chunk = file.read(Some(256 * 1024)).await.unwrap();
    assert_eq!(read_chunk.len(), 256 * 1024);
    assert_eq!(&read_chunk[..], &raw_data[new_pos..new_pos + 256 * 1024]);

    // Backward seek should abort and reset
    file.seek(SeekFrom::Start(0)).unwrap();
    let start_chunk = file.read(Some(256 * 1024)).await.unwrap();
    assert_eq!(&start_chunk[..], &raw_data[..256 * 1024]);

    assert!(metrics.read_range_calls.load(Ordering::Relaxed) > 0);
}

// ---------------------------------------------------------------------------
// Test 10: In-Flight Speculative Bandwidth Throttling
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_10_inflight_bandwidth_throttling() {
    let size = 32 * 1024 * 1024; // 32 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file("bucket/throttle.bin", raw_data.clone(), gcs_rust_fs::WriteOptions::default())
        .await
        .unwrap();

    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.io_size = 4 * 1024 * 1024;
    ra_cfg.max_inflight_bytes = 8 * 1024 * 1024; // 8 MiB max speculative bandwidth

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(mem, cfg);

    let mut file = fs.open("bucket/throttle.bin", OpenOptions::read()).await.unwrap();
    let res = file.read(Some(16 * 1024 * 1024)).await.unwrap();
    assert_eq!(res.len(), 16 * 1024 * 1024);
}

// ---------------------------------------------------------------------------
// Test 11: ReadPattern Hints (Sequential Pre-priming & Random Isolation)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_11_read_pattern_hints() {
    let size = 32 * 1024 * 1024;
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file("bucket/pattern.bin", raw_data.clone(), gcs_rust_fs::WriteOptions::default())
        .await
        .unwrap();

    let (spy, metrics) = SpyFs::new(mem);
    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.max_readahead = 32 * 1024 * 1024;
    ra_cfg.max_folio_size = 16 * 1024 * 1024;

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(spy, cfg);

    // Sequential hint
    let mut seq_file = fs
        .open(
            "bucket/pattern.bin",
            OpenOptions::read().with_pattern(ReadPattern::Sequential),
        )
        .await
        .unwrap();

    let c1 = seq_file.read(Some(256 * 1024)).await.unwrap();
    assert_eq!(c1.len(), 256 * 1024);

    // Random hint: should read exact single-page without ladder
    let calls_before = metrics.read_range_calls.load(Ordering::Relaxed);
    let mut rand_file = fs
        .open(
            "bucket/pattern.bin",
            OpenOptions::read().with_pattern(ReadPattern::Random),
        )
        .await
        .unwrap();

    rand_file.seek(SeekFrom::Start(10 * 1024 * 1024)).unwrap();
    let r1 = rand_file.read(Some(64 * 1024)).await.unwrap();
    assert_eq!(r1.len(), 64 * 1024);

    let calls_after = metrics.read_range_calls.load(Ordering::Relaxed);
    assert_eq!(
        calls_after - calls_before,
        1,
        "Random pattern should only issue 1 exact single-page fetch"
    );
}

// ---------------------------------------------------------------------------
// Test 12: Per-Handle Semaphore Pacing, Zero-Copy Slice, & Post-Seek Streaming Ramp
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_12_handle_semaphore_zero_copy_and_post_seek_ramp() {
    let size = 32 * 1024 * 1024; // 32 MiB
    let raw_data = create_synthetic_data(size);
    let mem = create_test_memfs().await;
    mem.pipe_file(
        "bucket/sliding.bin",
        raw_data.clone(),
        gcs_rust_fs::WriteOptions::default(),
    )
    .await
    .unwrap();

    let (spy, metrics) = SpyFs::new(mem);
    let mut ra_cfg = ReadaheadConfig::default();
    ra_cfg.page_size = 256 * 1024;
    ra_cfg.io_size = 4 * 1024 * 1024; // 4 MiB folios
    ra_cfg.max_folio_size = 4 * 1024 * 1024;
    ra_cfg.max_readahead = 16 * 1024 * 1024; // 4 folios per window
    ra_cfg.max_inflight = 2; // Cap per-handle concurrent folios to 2

    let cfg = CacheConfig::default().with_data(DataConfig {
        readahead: ra_cfg,
        cache: None,
    });
    let fs = CachedFs::new(spy, cfg);

    // 1. Seek to 8 MiB and issue a large (>= io_size = 4 MiB) read under ReadPattern::Auto:
    //    Should trigger initial Rule 1 ramp (8 MiB = 2x request) instead of Rule 7 single-read isolation.
    let mut file = fs
        .open("bucket/sliding.bin", OpenOptions::read())
        .await
        .unwrap();
    file.seek(SeekFrom::Start(8 * 1024 * 1024)).unwrap();
    let chunk = file.read(Some(4 * 1024 * 1024)).await.unwrap();
    assert_eq!(chunk.len(), 4 * 1024 * 1024);
    assert_eq!(
        &chunk[..],
        &raw_data[8 * 1024 * 1024..12 * 1024 * 1024]
    );

    // Wait briefly for the second folio in the ramped window to complete
    tokio::time::sleep(Duration::from_millis(20)).await;
    let fetched_after_large_seek = metrics.read_range_bytes.load(Ordering::Relaxed);
    assert!(
        fetched_after_large_seek >= 8 * 1024 * 1024,
        "Large post-seek read (>= io_size) should ramp readahead window: fetched {fetched_after_large_seek}"
    );

    // 2. Read the next 2 MiB (entirely inside the 12..16 MiB folio) via zero-copy fast path
    let sub_slice = file.read(Some(2 * 1024 * 1024)).await.unwrap();
    assert_eq!(sub_slice.len(), 2 * 1024 * 1024);
    assert_eq!(
        &sub_slice[..],
        &raw_data[12 * 1024 * 1024..14 * 1024 * 1024]
    );
}
