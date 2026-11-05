//! Live 20 GiB Benchmark Suite for `gcs-rust-fs` (`Transport::Http`, `ReadPattern::Auto`):
//! 1. Uncached `info()` metadata latency (1T, 30 iterations)
//! 2. `1 MiB` random reads (`CachedFs` Mode 1, `ReadPattern::Auto`, `io_size = 1 MiB`) @ 1T (50 reads)
//! 3. `20 GiB` sequential reads (`CachedFs` Mode 1, `ReadPattern::Auto`) @ 1T (`20 GiB`), 24T (`480 GiB`), & 48T (`960 GiB`)
//! 4. `200 MiB` write (`CachedFs` streaming `open(write)` & `pipe_file`) @ 1T
//!
//! Usage:
//!   cargo run --release --example bench_live_readahead -- --bench-20gb [OPTIONS] [GS_PATH]
//!
//! Options:
//!   --only <all|info|random|seq|seq-1t|seq-24t|seq-48t|write>   Filter benchmark section (default: all)
//!   --read-size-mb <MIB>                                        Sequential application read() size in MiB (default: 8)
//!   --io-size-mb <MIB>                                          Sequential readahead io_size in MiB (default: 16)
//!   --json-out <PATH>                                           Write summary JSON to file

use std::io::{SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use gcs_rust_fs::{
    ByteRange, CacheConfig, CachedFs, DataConfig, FileSystem, GcsFs, OpenOptions, ReadOptions,
    ReadaheadConfig, Transport, WriteOptions,
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

/// Benchmark single-threaded 1 MiB random reads at non-overlapping 4 MiB-aligned offsets
/// using `CachedFs` (`OpenOptions::read()`, `ReadPattern::Auto`).
async fn bench_random_1t<F: FileSystem + 'static>(
    label: &str,
    fs: Arc<F>,
    path: &str,
    offsets: &[u64],
    read_size: usize,
) -> Result<BenchStats, Box<dyn std::error::Error>> {
    let total_ops = offsets.len();
    let mut lats_ms = Vec::with_capacity(total_ops);
    let mut file = fs.open(path, OpenOptions::read()).await?;

    let start = Instant::now();
    for &off in offsets {
        let t0 = Instant::now();
        file.seek(SeekFrom::Start(off))?;
        let data = file.read(Some(read_size)).await?;
        assert_eq!(data.len(), read_size);
        lats_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    let elapsed = start.elapsed();
    let total_bytes = total_ops * read_size;
    let mb_s = (total_bytes as f64 / 1_000_000.0) / elapsed.as_secs_f64();
    let mib_s = (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    let iops = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "{:<56} | {:>4}T | {:>5} | {:>7.2}ms | {:>7.2}ms | {:>7.2}ms | {:>7.1} | {:>8.2} MB/s ({:>7.2} MiB/s)",
        label,
        1,
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

/// Benchmark sequential full-file reads across `paths` (1 file for 1T, 24 files for 24T, 48 files for 48T)
/// using the default read path (`OpenOptions::read()`, `ReadPattern::Auto`).
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
            let mut f = fs_c.open(&p, OpenOptions::read()).await.unwrap();
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

/// Benchmark single-threaded 200 MiB write (5 iterations) using `CachedFs`.
async fn bench_write_200mib_1t<F: FileSystem + 'static>(
    fs: Arc<F>,
    write_path: &str,
    write_bytes: usize,
    chunk_size: usize,
) -> Result<(BenchStats, BenchStats), Box<dyn std::error::Error>> {
    let payload = Bytes::from(vec![0xABu8; write_bytes]);
    let iters = 5;

    // 1. Streaming File::write (8 MiB chunks) + File::close
    let mut stream_durs_s = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let mut f = fs.open(write_path, OpenOptions::write()).await?;
        let mut offset = 0;
        while offset < write_bytes {
            let end = (offset + chunk_size).min(write_bytes);
            f.write(payload.slice(offset..end)).await?;
            offset = end;
        }
        f.close().await?;
        stream_durs_s.push(t0.elapsed().as_secs_f64());
    }
    let stream_mean_s = mean(&stream_durs_s);
    let stream_p50_s = percentile(&stream_durs_s, 0.50);
    let stream_p90_s = percentile(&stream_durs_s, 0.90);
    let stream_mb_s = (write_bytes as f64 / 1_000_000.0) / stream_p50_s;
    let stream_mib_s = (write_bytes as f64 / (1024.0 * 1024.0)) / stream_p50_s;

    println!(
        "{:<56} | {:>4}T | {:>5} | {:>7.2}s | {:>7.2}s | {:>7.2}s | {:>8.2} MB/s ({:>7.2} MiB/s)",
        "CachedFs::open(write) + write(8MiB) + close (200 MiB)",
        1,
        iters,
        stream_mean_s,
        stream_p50_s,
        stream_p90_s,
        stream_mb_s,
        stream_mib_s
    );

    // 2. One-shot FileSystem::pipe_file (200 MiB)
    let mut pipe_durs_s = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        fs.pipe_file(write_path, payload.clone(), WriteOptions::default())
            .await?;
        pipe_durs_s.push(t0.elapsed().as_secs_f64());
    }
    let _ = fs.rm_file(write_path).await;

    let pipe_mean_s = mean(&pipe_durs_s);
    let pipe_p50_s = percentile(&pipe_durs_s, 0.50);
    let pipe_p90_s = percentile(&pipe_durs_s, 0.90);
    let pipe_mb_s = (write_bytes as f64 / 1_000_000.0) / pipe_p50_s;
    let pipe_mib_s = (write_bytes as f64 / (1024.0 * 1024.0)) / pipe_p50_s;

    println!(
        "{:<56} | {:>4}T | {:>5} | {:>7.2}s | {:>7.2}s | {:>7.2}s | {:>8.2} MB/s ({:>7.2} MiB/s)",
        "CachedFs::pipe_file (One-Shot 200 MiB Upload)",
        1,
        iters,
        pipe_mean_s,
        pipe_p50_s,
        pipe_p90_s,
        pipe_mb_s,
        pipe_mib_s
    );
    let _ = std::io::stdout().flush();

    Ok((
        BenchStats {
            lats_ms: stream_durs_s.iter().map(|s| s * 1000.0).collect(),
            elapsed: Duration::from_secs_f64(stream_p50_s),
            mb_s: stream_mb_s,
            mib_s: stream_mib_s,
            iops: 0.0,
        },
        BenchStats {
            lats_ms: pipe_durs_s.iter().map(|s| s * 1000.0).collect(),
            elapsed: Duration::from_secs_f64(pipe_p50_s),
            mb_s: pipe_mb_s,
            mib_s: pipe_mib_s,
            iops: 0.0,
        },
    ))
}

async fn run_20gb_benchmark_suite(
    path: &str,
    only: Option<&str>,
    seq_read_mb: usize,
    seq_io_mb: usize,
    json_out: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    const RAND_IO_SIZE: usize = 1024 * 1024; // 1 MiB random read size & readahead io_size
    const WRITE_SIZE: usize = 200 * 1024 * 1024; // 200 MiB write payload
    let seq_read_size: usize = seq_read_mb * 1024 * 1024;
    let seq_io_size: usize = seq_io_mb * 1024 * 1024;
    const SEQ_MAX_FOLIO_SIZE: usize = 16 * 1024 * 1024; // 16 MiB max folio size

    let run_info = matches!(only, None | Some("all" | "info"));
    let run_random = matches!(only, None | Some("all" | "random"));
    let run_seq_1t = matches!(only, None | Some("all" | "seq" | "seq-1t"));
    let run_seq_24t = matches!(only, None | Some("all" | "seq" | "seq-24t"));
    let run_seq_48t = matches!(only, None | Some("all" | "seq" | "seq-48t"));
    let run_write = matches!(only, None | Some("all" | "write"));

    let gcs_http_1t = Arc::new(
        GcsFs::builder()
            .transport(Transport::Http)
            .http_pool_size(24)
            .from_env()?
            .build()
            .await?,
    );
    let gcs_http_mt = Arc::new(
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
    if run_random || run_seq_1t || run_write {
        warm_pool(&gcs_http_1t, path, 24).await;
    }
    if run_seq_24t || run_seq_48t {
        warm_pool(&gcs_http_mt, path, 48).await;
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
    println!("  Random Read Config   : 1T, read_size = 1 MiB, readahead io_size = 1 MiB");
    println!(
        "  Sequential Config    : 1T/24T/48T, read_size = {} MiB, readahead io_size = {} MiB (max_folio = {} MiB)",
        seq_read_mb,
        seq_io_mb,
        SEQ_MAX_FOLIO_SIZE / (1024 * 1024)
    );
    println!("  Write Config         : 1T, payload = 200 MiB (chunk_size = 8 MiB)");
    println!("================================================================================\n");

    // -------------------------------------------------------------------------
    // PART 1: Uncached Metadata `info()` Latency (1T)
    // -------------------------------------------------------------------------
    let mut info_uncached_ms = Vec::new();
    if run_info {
        println!("================================================================================");
        println!("PART 1: UNCACHED METADATA `info()` LATENCY (1T, 30 ITERATIONS)");
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
            "GcsFs::info (Direct Uncached)",
            iters,
            mean(&info_uncached_ms),
            percentile(&info_uncached_ms, 0.0),
            percentile(&info_uncached_ms, 0.50),
            percentile(&info_uncached_ms, 0.90),
            percentile(&info_uncached_ms, 0.99),
        );
    }

    // -------------------------------------------------------------------------
    // PART 2: Single-Threaded 1 MiB Random Read (1T — CachedFs Mode 1 Auto)
    // -------------------------------------------------------------------------
    let mut rand_1t_mode1 = BenchStats::default();

    if run_random {
        println!("================================================================================");
        println!("PART 2: SINGLE-THREADED 1 MiB RANDOM READ (1T, read_size = 1 MiB, io_size = 1 MiB, Auto)");
        println!("================================================================================");

        let stride = 4 * 1024 * 1024u64;
        let max_slots = (file_size - stride) / stride;
        let mut slots: Vec<u64> = (1..max_slots).collect();
        for i in 0..50 {
            let j = i + (splitmix64(i as u64 + 12345) as usize % (slots.len() - i));
            slots.swap(i, j);
        }
        let offsets_1t: Vec<u64> = slots[0..50].iter().map(|&s| s * stride).collect();

        let mode1_1t = Arc::new(CachedFs::new(
            (*gcs_http_1t).clone(),
            CacheConfig::default().with_data(DataConfig {
                readahead: ReadaheadConfig {
                    page_size: 256 * 1024,
                    max_readahead: 16 * 1024 * 1024,
                    io_size: RAND_IO_SIZE,
                    max_folio_size: 4 * 1024 * 1024,
                    max_inflight: 64,
                    max_inflight_bytes: 512 * 1024 * 1024,
                },
                cache: None,
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

        rand_1t_mode1 = bench_random_1t(
            "CachedFs Mode 1 (Drop-Behind, Auto, io=1MiB)",
            mode1_1t,
            path,
            &offsets_1t,
            RAND_IO_SIZE,
        )
        .await?;
        println!();
    }

    // -------------------------------------------------------------------------
    // PART 3: Sequential Read over 20 GiB Files (1T = 20 GiB, 24T = 480 GiB, 48T = 960 GiB)
    // -------------------------------------------------------------------------
    let mut seq_1t_auto = BenchStats::default();
    let mut seq_24t_auto = BenchStats::default();
    let mut seq_48t_auto = BenchStats::default();

    if run_seq_1t || run_seq_24t || run_seq_48t {
        println!("================================================================================");
        println!(
            "PART 3: SEQUENTIAL READ (read_size = {} MiB, ra io_size = {} MiB, max_folio = 16 MiB, Auto)",
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
                "CachedFs Mode 1 (1x20GiB=20GiB, read={}M, io={}M, Auto)",
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

        if run_seq_24t || run_seq_48t {
            let max_shards = if run_seq_48t { 48 } else { 24 };
            let base_prefix = path.strip_suffix(".bin").unwrap_or(path);
            let shard_paths: Vec<String> = (0..max_shards)
                .map(|tid| format!("{base_prefix}_{tid:02}.bin"))
                .collect();

            let mut copy_handles = Vec::with_capacity(max_shards);
            for dst in shard_paths.iter().cloned() {
                let fs_c = Arc::clone(&gcs_http_mt);
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

            let mode1_seq_mt = Arc::new(CachedFs::new(
                (*gcs_http_mt).clone(),
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

            if run_seq_24t {
                let label_24t = format!(
                    "CachedFs Mode 1 (24x20GiB=480GiB, read={}M, io={}M, Auto)",
                    seq_read_mb, seq_io_mb
                );
                seq_24t_auto = bench_sequential(
                    &label_24t,
                    Arc::clone(&mode1_seq_mt),
                    &shard_paths[0..24],
                    file_size,
                    seq_read_size,
                )
                .await?;
            }

            if run_seq_48t {
                let label_48t = format!(
                    "CachedFs Mode 1 (48x20GiB=960GiB, read={}M, io={}M, Auto)",
                    seq_read_mb, seq_io_mb
                );
                seq_48t_auto = bench_sequential(
                    &label_48t,
                    mode1_seq_mt,
                    &shard_paths[0..48],
                    file_size,
                    seq_read_size,
                )
                .await?;
            }
        }
        println!();
    }

    // -------------------------------------------------------------------------
    // PART 4: Single-Threaded 200 MiB Write Benchmark (1T)
    // -------------------------------------------------------------------------
    let mut write_stream_1t = BenchStats::default();
    let mut write_pipe_1t = BenchStats::default();

    if run_write {
        println!("================================================================================");
        println!("PART 4: SINGLE-THREADED 200 MiB WRITE (1T, 5 ITERATIONS)");
        println!("================================================================================");
        println!(
            "{:<56} | {:>5} | {:>5} | {:>9} | {:>9} | {:>9} | {:>24}",
            "Approach (200 MiB Write Payload)",
            "Thrds",
            "Iters",
            "Mean",
            "p50",
            "p90",
            "Throughput (at p50)"
        );
        println!("-----------------------------------------------------------------------------------------------------------------------------------");

        let base_prefix = path.strip_suffix(".bin").unwrap_or(path);
        let write_path = format!("{base_prefix}_write_200MB.bin");
        let cached_write_fs = Arc::new(CachedFs::new(
            (*gcs_http_1t).clone(),
            CacheConfig::default(),
        ));

        let (ws, wp) = bench_write_200mib_1t(
            cached_write_fs,
            &write_path,
            WRITE_SIZE,
            8 * 1024 * 1024,
        )
        .await?;
        write_stream_1t = ws;
        write_pipe_1t = wp;
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
  "write_size_bytes": {WRITE_SIZE},
  "info_uncached_1t": {{ "mean_ms": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3} }},
  "random_1mib_1t_cached_mode1_auto": {{
    "p50_ms": {:.3}, "p90_ms": {:.3}, "p99_ms": {:.3}, "iops": {:.2}, "mb_s": {:.2}, "mib_s": {:.2}
  }},
  "sequential_20gib_cached_mode1_auto": {{
    "1t_20gib": {{ "wall_s": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "24t_480gib": {{ "wall_s": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "48t_960gib": {{ "wall_s": {:.3}, "p50_ms": {:.3}, "p90_ms": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }}
  }},
  "write_200mib_1t": {{
    "streaming_open_write_close": {{ "p50_s": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "oneshot_pipe_file": {{ "p50_s": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }}
  }}
}}
"#,
            mean(&info_uncached_ms),
            percentile(&info_uncached_ms, 0.50),
            percentile(&info_uncached_ms, 0.90),
            percentile(&info_uncached_ms, 0.99),
            percentile(&rand_1t_mode1.lats_ms, 0.50),
            percentile(&rand_1t_mode1.lats_ms, 0.90),
            percentile(&rand_1t_mode1.lats_ms, 0.99),
            rand_1t_mode1.iops,
            rand_1t_mode1.mb_s,
            rand_1t_mode1.mib_s,
            seq_1t_auto.elapsed.as_secs_f64(),
            percentile(&seq_1t_auto.lats_ms, 0.50),
            percentile(&seq_1t_auto.lats_ms, 0.90),
            seq_1t_auto.mb_s,
            seq_1t_auto.mib_s,
            seq_24t_auto.elapsed.as_secs_f64(),
            percentile(&seq_24t_auto.lats_ms, 0.50),
            percentile(&seq_24t_auto.lats_ms, 0.90),
            seq_24t_auto.mb_s,
            seq_24t_auto.mib_s,
            seq_48t_auto.elapsed.as_secs_f64(),
            percentile(&seq_48t_auto.lats_ms, 0.50),
            percentile(&seq_48t_auto.lats_ms, 0.90),
            seq_48t_auto.mb_s,
            seq_48t_auto.mib_s,
            write_stream_1t.elapsed.as_secs_f64(),
            write_stream_1t.mb_s,
            write_stream_1t.mib_s,
            write_pipe_1t.elapsed.as_secs_f64(),
            write_pipe_1t.mb_s,
            write_pipe_1t.mib_s,
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
