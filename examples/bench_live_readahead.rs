//! Live 20 GiB Benchmark Suite for `gcs-rust-fs`:
//! 1. Uncached `info()` metadata latency (30 iterations)
//! 2. `1 MiB` random reads (`ReadPattern::Auto`, `io_size = 1 MiB`) @ 1 & 48 threads
//! 3. `20 GiB` sequential reads (`ReadPattern::Auto`) @ 1 thread (`1 × 20 GiB`) & 48 threads (`48 × 20 GiB = 960 GiB`)
//!
//! Usage:
//!   cargo run --release --example bench_live_readahead -- --bench-20gb [OPTIONS] [GS_PATH]
//!
//! Options:
//!   --only <all|info|random|seq|seq-1t|seq-48t>   Filter benchmark section (default: all)
//!   --read-size-mb <MIB>                          Sequential application read() size in MiB (default: 8)
//!   --io-size-mb <MIB>                            Sequential readahead io_size in MiB (default: 16)
//!   --json-out <PATH>                             Write summary JSON to file

use std::io::{SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gcs_rust_fs::{
    ByteRange, CacheConfig, CachedFs, DataConfig, FileSystem, GcsFs, OpenOptions, PageCacheConfig,
    ReadOptions, ReadPattern, ReadaheadConfig, Transport,
};

fn percentile(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn mean(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.iter().sum::<f64>() / samples.len() as f64
}

/// Deterministic SplitMix64 generator for reproducible random read offsets across runs.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

#[derive(Default)]
struct BenchStats {
    lats_ms: Vec<f64>,
    elapsed: Duration,
    mb_s: f64,
    mib_s: f64,
    iops: f64,
}

async fn warm_pool(fs: &Arc<GcsFs>, path: &str, conns: usize) {
    let mut handles = Vec::with_capacity(conns);
    for i in 0..conns {
        let fs_c = Arc::clone(fs);
        let p = path.to_string();
        handles.push(tokio::spawn(async move {
            let off = (i as u64) * 8 * 1024 * 1024;
            let _ = fs_c
                .cat_file(
                    &p,
                    ByteRange::span(off, off + 8 * 1024 * 1024),
                    ReadOptions::default(),
                )
                .await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Benchmark 1 MiB random reads at non-overlapping 4 MiB-aligned offsets.
async fn bench_random<F: FileSystem + 'static>(
    label: &str,
    fs: Arc<F>,
    path: &str,
    offsets: &[u64],
    read_size: usize,
    threads: usize,
    use_open_seek: bool,
) -> Result<BenchStats, Box<dyn std::error::Error>> {
    let total_ops = offsets.len();
    let ops_per_thread = total_ops / threads;
    let offsets = Arc::new(offsets.to_vec());
    let barrier = Arc::new(tokio::sync::Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);

    for tid in 0..threads {
        let fs_c = Arc::clone(&fs);
        let p = path.to_string();
        let offs = Arc::clone(&offsets);
        let bar = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let start_idx = tid * ops_per_thread;
            let end_idx = if tid == threads - 1 {
                total_ops
            } else {
                start_idx + ops_per_thread
            };
            let mut lats = Vec::with_capacity(end_idx - start_idx);
            let mut file_opt = if use_open_seek {
                Some(
                    fs_c.open(&p, OpenOptions::read().with_pattern(ReadPattern::Auto))
                        .await
                        .unwrap(),
                )
            } else {
                None
            };

            bar.wait().await;
            for i in start_idx..end_idx {
                let off = offs[i];
                let t0 = Instant::now();
                if let Some(ref mut f) = file_opt {
                    f.seek(SeekFrom::Start(off)).unwrap();
                    let data = f.read(Some(read_size)).await.unwrap();
                    assert_eq!(data.len(), read_size);
                } else {
                    let data = fs_c
                        .cat_file(
                            &p,
                            ByteRange::span(off, off + read_size as u64),
                            ReadOptions::default(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(data.len(), read_size);
                }
                lats.push(t0.elapsed().as_secs_f64() * 1000.0);
            }
            lats
        }));
    }

    barrier.wait().await;
    let start = Instant::now();
    let mut lats_ms = Vec::with_capacity(total_ops);
    for h in handles {
        lats_ms.extend(h.await?);
    }
    let elapsed = start.elapsed();
    let total_bytes = total_ops * read_size;
    let mb_s = (total_bytes as f64 / 1_000_000.0) / elapsed.as_secs_f64();
    let mib_s = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    let iops = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "{:<56} | {:>4}T | {:>5} | {:>7.2}ms | {:>7.2}ms | {:>7.2}ms | {:>7.1} | {:>8.2} MB/s ({:>7.2} MiB/s)",
        label,
        threads,
        total_ops,
        percentile(&lats_ms, 0.50),
        percentile(&lats_ms, 0.90),
        percentile(&lats_ms, 0.99),
        iops,
        mb_s,
        mib_s
    );
    let _ = std::io::stdout().flush();

    Ok(BenchStats {
        lats_ms,
        elapsed,
        mb_s,
        mib_s,
        iops,
    })
}

/// Benchmark sequential full-file reads across `paths` (1 file for 1T, 48 distinct files for 48T)
/// using `ReadPattern::Auto`.
async fn bench_sequential<F: FileSystem + 'static>(
    label: &str,
    fs: Arc<F>,
    paths: &[String],
    bytes_per_file: u64,
    read_size: usize,
) -> Result<BenchStats, Box<dyn std::error::Error>> {
    let threads = paths.len();
    let total_bytes = bytes_per_file * threads as u64;
    let barrier = Arc::new(tokio::sync::Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);

    for p in paths.iter().cloned() {
        let fs_c = Arc::clone(&fs);
        let bar = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let mut f = fs_c
                .open(&p, OpenOptions::read().with_pattern(ReadPattern::Auto))
                .await
                .unwrap();
            let mut lats =
                Vec::with_capacity((bytes_per_file as usize + read_size - 1) / read_size);
            let mut rem = bytes_per_file as usize;

            bar.wait().await;
            while rem > 0 {
                let to_read = rem.min(read_size);
                let t0 = Instant::now();
                let chunk = f.read(Some(to_read)).await.unwrap();
                lats.push(t0.elapsed().as_secs_f64() * 1000.0);
                if chunk.is_empty() {
                    break;
                }
                rem -= chunk.len();
            }
            lats
        }));
    }

    barrier.wait().await;
    let start = Instant::now();
    let mut lats_ms = Vec::new();
    for h in handles {
        lats_ms.extend(h.await?);
    }
    let elapsed = start.elapsed();
    let mb_s = (total_bytes as f64 / 1_000_000.0) / elapsed.as_secs_f64();
    let mib_s = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();

    println!(
        "{:<56} | {:>4}T | {:>7.2}s | {:>7.2}ms | {:>7.2}ms | {:>8.2} MB/s ({:>7.2} MiB/s)",
        label,
        threads,
        elapsed.as_secs_f64(),
        percentile(&lats_ms, 0.50),
        percentile(&lats_ms, 0.90),
        mb_s,
        mib_s
    );
    let _ = std::io::stdout().flush();

    Ok(BenchStats {
        lats_ms,
        elapsed,
        mb_s,
        mib_s,
        iops: 0.0,
    })
}

async fn run_20gb_benchmark_suite(
    path: &str,
    only: Option<&str>,
    seq_read_mb: usize,
    seq_io_mb: usize,
    json_out: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    const RAND_IO_SIZE: usize = 1024 * 1024; // 1 MiB random read size & readahead io_size
    let seq_read_size: usize = seq_read_mb * 1024 * 1024;
    let seq_io_size: usize = seq_io_mb * 1024 * 1024;
    const SEQ_MAX_FOLIO_SIZE: usize = 16 * 1024 * 1024; // 16 MiB max folio size

    let run_info = matches!(only, None | Some("all" | "info"));
    let run_random = matches!(only, None | Some("all" | "random"));
    let run_seq_1t = matches!(only, None | Some("all" | "seq" | "seq-1t"));
    let run_seq_48t = matches!(only, None | Some("all" | "seq" | "seq-48t"));

    let gcs_http_1t = Arc::new(
        GcsFs::builder()
            .transport(Transport::Http)
            .http_pool_size(24)
            .from_env()?
            .build()
            .await?,
    );
    let gcs_http_48t = Arc::new(
        GcsFs::builder()
            .transport(Transport::Http)
            .http_pool_size(48)
            .from_env()?
            .build()
            .await?,
    );

    // Warm up TLS + HTTP/2 connection pools before timed benchmarks
    for _ in 0..5 {
        let _ = gcs_http_1t.info(path).await?;
    }
    if run_random || run_seq_1t {
        warm_pool(&gcs_http_1t, path, 24).await;
    }
    if run_random || run_seq_48t {
        warm_pool(&gcs_http_48t, path, 48).await;
    }

    let obj_stat = gcs_http_1t.info(path).await?;
    let file_size = obj_stat.size;

    println!("\n================================================================================");
    println!("  GCS-RUST-FS: 20 GB OBJECT BENCHMARK SUITE (Transport::Http, ReadPattern::Auto)");
    println!("  Target Object        : {path}");
    println!(
        "  Object Size          : {:.2} GiB ({file_size} bytes)",
        file_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!("  Random Read Config   : read_size = 1 MiB, readahead io_size = 1 MiB");
    println!(
        "  Sequential Config    : read_size = {} MiB, readahead io_size = {} MiB (max_folio_size = {} MiB)",
        seq_read_mb,
        seq_io_mb,
        SEQ_MAX_FOLIO_SIZE / (1024 * 1024)
    );
    println!("================================================================================\n");

    // -------------------------------------------------------------------------
    // PART 1: Uncached Metadata `info()` Latency
    // -------------------------------------------------------------------------
    let mut info_uncached_ms = Vec::new();
    if run_info {
        println!("================================================================================");
        println!("PART 1: UNCACHED METADATA `info()` LATENCY (30 ITERATIONS)");
        println!("================================================================================");
        let iters = 30;
        for _ in 0..iters {
            let t0 = Instant::now();
            let st = gcs_http_1t.info(path).await?;
            assert_eq!(st.size, file_size);
            info_uncached_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
        println!(
            "{:<44} | {:>6} | {:>9} | {:>9} | {:>9} | {:>9} | {:>9}",
            "Operation", "Iters", "Mean", "Min", "p50", "p90", "p99"
        );
        println!("------------------------------------------------------------------------------------------------------------");
        println!(
            "{:<44} | {:>6} | {:>7.2}ms | {:>7.2}ms | {:>7.2}ms | {:>7.2}ms | {:>7.2}ms\n",
            "GcsFs::info (Direct Uncached HTTP)",
            iters,
            mean(&info_uncached_ms),
            percentile(&info_uncached_ms, 0.0),
            percentile(&info_uncached_ms, 0.50),
            percentile(&info_uncached_ms, 0.90),
            percentile(&info_uncached_ms, 0.99),
        );
    }

    // -------------------------------------------------------------------------
    // PART 2: 1 MiB Random Read Benchmark (1 Thread vs 48 Threads, Auto)
    // -------------------------------------------------------------------------
    let mut rand_1t_direct = BenchStats::default();
    let mut rand_1t_mode1 = BenchStats::default();
    let mut rand_1t_mode2_cold = BenchStats::default();
    let mut rand_1t_mode2_warm = BenchStats::default();
    let mut rand_48t_direct = BenchStats::default();
    let mut rand_48t_mode1 = BenchStats::default();
    let mut rand_48t_mode2_cold = BenchStats::default();
    let mut rand_48t_mode2_warm = BenchStats::default();

    if run_random {
        println!("================================================================================");
        println!("PART 2: 1 MiB RANDOM READS (read_size = 1 MiB, io_size = 1 MiB, ReadPattern::Auto)");
        println!("================================================================================");

        // Generate 480 non-overlapping 4 MiB-aligned offsets so random reads never coalesce unless testing warm cache
        let stride = 4 * 1024 * 1024u64;
        let max_slots = (file_size - stride) / stride;
        let mut slots: Vec<u64> = (1..max_slots).collect();
        for i in 0..480 {
            let j = i + (splitmix64(i as u64 + 12345) as usize % (slots.len() - i));
            slots.swap(i, j);
        }
        let offsets_1t: Vec<u64> = slots[0..50].iter().map(|&s| s * stride).collect();
        let offsets_48t: Vec<u64> = slots[0..480].iter().map(|&s| s * stride).collect();

        let ra_rand_cfg = ReadaheadConfig {
            page_size: 256 * 1024,
            max_readahead: 16 * 1024 * 1024,
            io_size: RAND_IO_SIZE,
            max_folio_size: 4 * 1024 * 1024,
            max_inflight: 64,
            max_inflight_bytes: 512 * 1024 * 1024,
        };
        let mode1_1t = Arc::new(CachedFs::new(
            (*gcs_http_1t).clone(),
            CacheConfig::default().with_data(DataConfig {
                readahead: ra_rand_cfg.clone(),
                cache: None,
            }),
        ));
        let mode1_48t = Arc::new(CachedFs::new(
            (*gcs_http_48t).clone(),
            CacheConfig::default().with_data(DataConfig {
                readahead: ra_rand_cfg.clone(),
                cache: None,
            }),
        ));
        let mode2_1t = Arc::new(CachedFs::new(
            (*gcs_http_1t).clone(),
            CacheConfig::default().with_data(DataConfig {
                readahead: ra_rand_cfg.clone(),
                cache: Some(PageCacheConfig {
                    capacity: 2 * 1024 * 1024 * 1024,
                }),
            }),
        ));
        let mode2_48t = Arc::new(CachedFs::new(
            (*gcs_http_48t).clone(),
            CacheConfig::default().with_data(DataConfig {
                readahead: ra_rand_cfg,
                cache: Some(PageCacheConfig {
                    capacity: 2 * 1024 * 1024 * 1024,
                }),
            }),
        ));

        println!(
            "{:<56} | {:>5} | {:>5} | {:>9} | {:>9} | {:>9} | {:>7} | {:>24}",
            "Approach (read_size=1MiB, io_size=1MiB, Auto)",
            "Thrds",
            "Reads",
            "p50",
            "p90",
            "p99",
            "IOPS",
            "Throughput"
        );
        println!("---------------------------------------------------------------------------------------------------------------------------------------------");

        rand_1t_direct = bench_random(
            "Direct GcsFs::cat_file (Uncached HTTP, io=1MiB)",
            Arc::clone(&gcs_http_1t),
            path,
            &offsets_1t,
            RAND_IO_SIZE,
            1,
            false,
        )
        .await?;
        rand_1t_mode1 = bench_random(
            "CachedFs Mode 1 (Drop-Behind, Auto, io=1MiB)",
            Arc::clone(&mode1_1t),
            path,
            &offsets_1t,
            RAND_IO_SIZE,
            1,
            true,
        )
        .await?;
        rand_1t_mode2_cold = bench_random(
            "CachedFs Mode 2 (PageCache Cold, Auto, io=1MiB)",
            Arc::clone(&mode2_1t),
            path,
            &offsets_1t,
            RAND_IO_SIZE,
            1,
            true,
        )
        .await?;
        rand_1t_mode2_warm = bench_random(
            "CachedFs Mode 2 (PageCache Warm, Auto, io=1MiB)",
            Arc::clone(&mode2_1t),
            path,
            &offsets_1t,
            RAND_IO_SIZE,
            1,
            true,
        )
        .await?;

        println!("---------------------------------------------------------------------------------------------------------------------------------------------");

        rand_48t_direct = bench_random(
            "Direct GcsFs::cat_file (Uncached HTTP, io=1MiB)",
            Arc::clone(&gcs_http_48t),
            path,
            &offsets_48t,
            RAND_IO_SIZE,
            48,
            false,
        )
        .await?;
        rand_48t_mode1 = bench_random(
            "CachedFs Mode 1 (Drop-Behind, Auto, io=1MiB)",
            Arc::clone(&mode1_48t),
            path,
            &offsets_48t,
            RAND_IO_SIZE,
            48,
            true,
        )
        .await?;
        rand_48t_mode2_cold = bench_random(
            "CachedFs Mode 2 (PageCache Cold, Auto, io=1MiB)",
            Arc::clone(&mode2_48t),
            path,
            &offsets_48t,
            RAND_IO_SIZE,
            48,
            true,
        )
        .await?;
        rand_48t_mode2_warm = bench_random(
            "CachedFs Mode 2 (PageCache Warm, Auto, io=1MiB)",
            Arc::clone(&mode2_48t),
            path,
            &offsets_48t,
            RAND_IO_SIZE,
            48,
            true,
        )
        .await?;
        println!();
    }

    // -------------------------------------------------------------------------
    // PART 3: Sequential Read over 20 GiB File (1T = 20 GiB, 48T = 48 * 20 GiB)
    // -------------------------------------------------------------------------
    let mut seq_1t_auto = BenchStats::default();
    let mut seq_48t_direct = BenchStats::default();
    let mut seq_48t_auto = BenchStats::default();

    if run_seq_1t || run_seq_48t {
        println!("================================================================================");
        println!(
            "PART 3: SEQUENTIAL READ (read_size = {} MiB, ra io_size = {} MiB, max_folio = 16 MiB, ReadPattern::Auto)",
            seq_read_mb, seq_io_mb
        );
        println!("================================================================================");
        println!(
            "{:<56} | {:>5} | {:>8} | {:>9} | {:>9} | {:>24}",
            "Approach (Sequential Scan, ReadPattern::Auto)",
            "Thrds",
            "WallTime",
            "Chunk p50",
            "Chunk p90",
            "Throughput"
        );
        println!("------------------------------------------------------------------------------------------------------------------------------");

        if run_seq_1t {
            let mode1_seq_1t = Arc::new(CachedFs::new(
                (*gcs_http_1t).clone(),
                CacheConfig::default().with_data(DataConfig {
                    readahead: ReadaheadConfig {
                        page_size: 256 * 1024,
                        max_readahead: 384 * 1024 * 1024,
                        io_size: seq_io_size,
                        max_folio_size: SEQ_MAX_FOLIO_SIZE,
                        max_inflight: 24,
                        max_inflight_bytes: 4 * 1024 * 1024 * 1024,
                    },
                    cache: None,
                }),
            ));
            let single_path = vec![path.to_string()];
            let label = format!(
                "CachedFs Mode 1 (1x20GiB, read={}M, io={}M, Auto)",
                seq_read_mb, seq_io_mb
            );
            seq_1t_auto = bench_sequential(
                &label,
                mode1_seq_1t,
                &single_path,
                file_size,
                seq_read_size,
            )
            .await?;
        }

        if run_seq_1t && run_seq_48t {
            println!("------------------------------------------------------------------------------------------------------------------------------");
        }

        if run_seq_48t {
            let base_prefix = path.strip_suffix(".bin").unwrap_or(path);
            let shard_paths: Vec<String> = (0..48)
                .map(|tid| format!("{base_prefix}_{tid:02}.bin"))
                .collect();

            // Ensure all 48 distinct 20 GiB objects exist via server-side GCS rewrite
            let mut copy_handles = Vec::with_capacity(48);
            for dst in shard_paths.iter().cloned() {
                let fs_c = Arc::clone(&gcs_http_48t);
                let src = path.to_string();
                copy_handles.push(tokio::spawn(async move {
                    if fs_c.info(&dst).await.is_err() {
                        fs_c.copy_file(&src, &dst).await?;
                    }
                    Ok::<_, gcs_rust_fs::Error>(())
                }));
            }
            for h in copy_handles {
                h.await??;
            }

            let mode1_seq_48t = Arc::new(CachedFs::new(
                (*gcs_http_48t).clone(),
                CacheConfig::default().with_data(DataConfig {
                    readahead: ReadaheadConfig {
                        page_size: 256 * 1024,
                        max_readahead: 64 * 1024 * 1024,
                        io_size: seq_io_size,
                        max_folio_size: SEQ_MAX_FOLIO_SIZE,
                        max_inflight: 4,
                        max_inflight_bytes: 3 * 1024 * 1024 * 1024,
                    },
                    cache: None,
                }),
            ));

            let direct_label =
                format!("Direct GcsFs HTTP (48x20GiB=960GiB, read={}M)", seq_read_mb);
            seq_48t_direct = bench_sequential(
                &direct_label,
                Arc::clone(&gcs_http_48t),
                &shard_paths,
                file_size,
                seq_read_size,
            )
            .await?;

            let auto_label = format!(
                "CachedFs Mode 1 (48x20GiB=960GiB, read={}M, io={}M, Auto)",
                seq_read_mb, seq_io_mb
            );
            seq_48t_auto =
                bench_sequential(&auto_label, mode1_seq_48t, &shard_paths, file_size, seq_read_size)
                    .await?;
        }
        println!();
    }

    if let Some(out_path) = json_out {
        let json = format!(
            r#"{{
  "stack": "native_rust_gcs_rust_fs",
  "object_path": "{path}",
  "object_size_bytes": {file_size},
  "seq_read_size_bytes": {seq_read_size},
  "seq_readahead_io_size_bytes": {seq_io_size},
  "rand_io_size_bytes": {RAND_IO_SIZE},
  "info_uncached": {{ "mean_ms": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3} }},
  "random_1mib_1t": {{
    "direct_http": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "cached_mode1_auto": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "cached_mode2_cold": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "cached_mode2_warm": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }}
  }},
  "random_1mib_48t": {{
    "direct_http": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "cached_mode1_auto": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "cached_mode2_cold": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "cached_mode2_warm": {{ "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2} }}
  }},
  "sequential_20gib": {{
    "1t_cached_mode1_auto": {{ "wall_s": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "48t_48files_direct_http": {{ "wall_s": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "48t_48files_cached_mode1_auto": {{ "wall_s": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }}
  }}
}}
"#,
            mean(&info_uncached_ms),
            percentile(&info_uncached_ms, 0.50),
            percentile(&info_uncached_ms, 0.90),
            percentile(&info_uncached_ms, 0.99),
            percentile(&rand_1t_direct.lats_ms, 0.50),
            percentile(&rand_1t_direct.lats_ms, 0.90),
            percentile(&rand_1t_direct.lats_ms, 0.99),
            rand_1t_direct.iops,
            rand_1t_direct.mb_s,
            rand_1t_direct.mib_s,
            percentile(&rand_1t_mode1.lats_ms, 0.50),
            percentile(&rand_1t_mode1.lats_ms, 0.90),
            percentile(&rand_1t_mode1.lats_ms, 0.99),
            rand_1t_mode1.iops,
            rand_1t_mode1.mb_s,
            rand_1t_mode1.mib_s,
            percentile(&rand_1t_mode2_cold.lats_ms, 0.50),
            percentile(&rand_1t_mode2_cold.lats_ms, 0.90),
            percentile(&rand_1t_mode2_cold.lats_ms, 0.99),
            rand_1t_mode2_cold.iops,
            rand_1t_mode2_cold.mb_s,
            rand_1t_mode2_cold.mib_s,
            percentile(&rand_1t_mode2_warm.lats_ms, 0.50),
            percentile(&rand_1t_mode2_warm.lats_ms, 0.90),
            percentile(&rand_1t_mode2_warm.lats_ms, 0.99),
            rand_1t_mode2_warm.iops,
            rand_1t_mode2_warm.mb_s,
            rand_1t_mode2_warm.mib_s,
            percentile(&rand_48t_direct.lats_ms, 0.50),
            percentile(&rand_48t_direct.lats_ms, 0.90),
            percentile(&rand_48t_direct.lats_ms, 0.99),
            rand_48t_direct.iops,
            rand_48t_direct.mb_s,
            rand_48t_direct.mib_s,
            percentile(&rand_48t_mode1.lats_ms, 0.50),
            percentile(&rand_48t_mode1.lats_ms, 0.90),
            percentile(&rand_48t_mode1.lats_ms, 0.99),
            rand_48t_mode1.iops,
            rand_48t_mode1.mb_s,
            rand_48t_mode1.mib_s,
            percentile(&rand_48t_mode2_cold.lats_ms, 0.50),
            percentile(&rand_48t_mode2_cold.lats_ms, 0.90),
            percentile(&rand_48t_mode2_cold.lats_ms, 0.99),
            rand_48t_mode2_cold.iops,
            rand_48t_mode2_cold.mb_s,
            rand_48t_mode2_cold.mib_s,
            percentile(&rand_48t_mode2_warm.lats_ms, 0.50),
            percentile(&rand_48t_mode2_warm.lats_ms, 0.90),
            percentile(&rand_48t_mode2_warm.lats_ms, 0.99),
            rand_48t_mode2_warm.iops,
            rand_48t_mode2_warm.mb_s,
            rand_48t_mode2_warm.mib_s,
            seq_1t_auto.elapsed.as_secs_f64(),
            percentile(&seq_1t_auto.lats_ms, 0.50),
            percentile(&seq_1t_auto.lats_ms, 0.90),
            seq_1t_auto.mb_s,
            seq_1t_auto.mib_s,
            seq_48t_direct.elapsed.as_secs_f64(),
            percentile(&seq_48t_direct.lats_ms, 0.50),
            percentile(&seq_48t_direct.lats_ms, 0.90),
            seq_48t_direct.mb_s,
            seq_48t_direct.mib_s,
            seq_48t_auto.elapsed.as_secs_f64(),
            percentile(&seq_48t_auto.lats_ms, 0.50),
            percentile(&seq_48t_auto.lats_ms, 0.90),
            seq_48t_auto.mb_s,
            seq_48t_auto.mib_s,
        );
        std::fs::write(out_path, json)?;
        println!("Saved JSON benchmark summary to: {out_path}");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut path = "gs://princer-flat/bench/bench_20GB.bin".to_string();
    let mut only: Option<String> = None;
    let mut seq_read_mb: usize = 8;
    let mut seq_io_mb: usize = 16;
    let mut json_out: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--bench-20gb" => {}
            "--only" => {
                i += 1;
                only = args.get(i).cloned();
            }
            "--read-size-mb" => {
                i += 1;
                if let Some(v) = args.get(i) {
                    seq_read_mb = v.parse()?;
                }
            }
            "--io-size-mb" => {
                i += 1;
                if let Some(v) = args.get(i) {
                    seq_io_mb = v.parse()?;
                }
            }
            "--json-out" => {
                i += 1;
                json_out = args.get(i).cloned();
            }
            arg if !arg.starts_with("--") => {
                path = arg.to_string();
            }
            _ => {}
        }
        i += 1;
    }

    run_20gb_benchmark_suite(
        &path,
        only.as_deref(),
        seq_read_mb,
        seq_io_mb,
        json_out.as_deref(),
    )
    .await
}
