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

#[derive(Clone, Debug, Default)]
struct ResourceSnapshot {
    pub avg_cpu_pct: f64,
    pub avg_cpu_cores: f64,
    pub avg_rss_mib: f64,
    pub peak_rss_mib: f64,
}

#[derive(Default)]
struct BenchStats {
    lats_ms: Vec<f64>,
    elapsed: Duration,
    mb_s: f64,
    mib_s: f64,
    iops: f64,
    res: ResourceSnapshot,
}

fn sample_proc() -> Option<(u64, u64)> {
    // 1. CPU ticks (utime + stime)
    let stat_str = std::fs::read_to_string("/proc/self/stat").ok()?;
    let close_paren = stat_str.rfind(')')?;
    let rest = &stat_str[close_paren + 1..].trim_start();
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() < 13 {
        return None;
    }
    let utime: u64 = fields[11].parse().ok()?;
    let stime: u64 = fields[12].parse().ok()?;
    let total_ticks = utime + stime;

    // 2. VmRSS in kB
    let status_str = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut rss_kb = 0u64;
    for line in status_str.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let num_str = rest.trim().split_whitespace().next().unwrap_or("0");
            rss_kb = num_str.parse().unwrap_or(0);
            break;
        }
    }

    Some((total_ticks, rss_kb))
}

struct ResourceSampler {
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<ResourceSnapshot>>,
}

impl ResourceSampler {
    pub fn start() -> Self {
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(50));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let (init_ticks, init_rss) = sample_proc().unwrap_or((0, 0));
            let t_start = Instant::now();
            let mut last_ticks = init_ticks;
            let mut rss_samples = vec![init_rss];

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        if let Some((ticks, rss)) = sample_proc() {
                            last_ticks = ticks;
                            rss_samples.push(rss);
                        }
                        break;
                    }
                    _ = ticker.tick() => {
                        if let Some((ticks, rss)) = sample_proc() {
                            last_ticks = ticks;
                            rss_samples.push(rss);
                        }
                    }
                }
            }

            let wall_s = t_start.elapsed().as_secs_f64();
            let delta_ticks = last_ticks.saturating_sub(init_ticks);
            // 100 ticks per second (standard Linux SC_CLK_TCK)
            let cpu_sec = delta_ticks as f64 / 100.0;
            let avg_cpu_cores = if wall_s > 0.001 { cpu_sec / wall_s } else { 0.0 };
            let avg_cpu_pct = avg_cpu_cores * 100.0;

            let (avg_rss_mib, peak_rss_mib) = if rss_samples.is_empty() {
                (0.0, 0.0)
            } else {
                let sum: u64 = rss_samples.iter().sum();
                let max: u64 = *rss_samples.iter().max().unwrap_or(&0);
                (
                    (sum as f64 / rss_samples.len() as f64) / 1024.0,
                    max as f64 / 1024.0,
                )
            };

            ResourceSnapshot {
                avg_cpu_pct,
                avg_cpu_cores,
                avg_rss_mib,
                peak_rss_mib,
            }
        });

        Self {
            stop_tx: Some(stop_tx),
            handle: Some(handle),
        }
    }

    pub async fn stop(mut self) -> ResourceSnapshot {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.await.unwrap_or_default()
        } else {
            ResourceSnapshot::default()
        }
    }
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
        ..Default::default()
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
        ..Default::default()
    })
}

/// Benchmark single-threaded 200 MiB write (5 iterations) using `CachedFs`.
async fn bench_write_200mib_1t<F: FileSystem + 'static>(
    fs: Arc<F>,
    write_path: &str,
    write_bytes: usize,
    chunk_size: usize,
) -> Result<(BenchStats, BenchStats, BenchStats), Box<dyn std::error::Error>> {
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

    // 3. FileSystem::put_file from /dev/shm (200 MiB)
    let shm_put_path = std::path::PathBuf::from("/dev/shm/gcs_rust_fs_bench_put_200MB.bin");
    std::fs::write(&shm_put_path, &payload)?;

    let mut put_durs_s = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        fs.put_file(&shm_put_path, write_path, WriteOptions::default())
            .await?;
        put_durs_s.push(t0.elapsed().as_secs_f64());
    }
    let _ = std::fs::remove_file(&shm_put_path);
    let _ = fs.rm_file(write_path).await;

    let put_mean_s = mean(&put_durs_s);
    let put_p50_s = percentile(&put_durs_s, 0.50);
    let put_p90_s = percentile(&put_durs_s, 0.90);
    let put_mb_s = (write_bytes as f64 / 1_000_000.0) / put_p50_s;
    let put_mib_s = (write_bytes as f64 / (1024.0 * 1024.0)) / put_p50_s;

    println!(
        "{:<56} | {:>4}T | {:>5} | {:>7.2}s | {:>7.2}s | {:>7.2}s | {:>8.2} MB/s ({:>7.2} MiB/s)",
        "CachedFs::put_file (/dev/shm 200 MiB Disk Upload)",
        1,
        iters,
        put_mean_s,
        put_p50_s,
        put_p90_s,
        put_mb_s,
        put_mib_s
    );
    let _ = std::io::stdout().flush();

    Ok((
        BenchStats {
            lats_ms: stream_durs_s.iter().map(|s| s * 1000.0).collect(),
            elapsed: Duration::from_secs_f64(stream_p50_s),
            mb_s: stream_mb_s,
            mib_s: stream_mib_s,
            iops: 0.0,
            ..Default::default()
        },
        BenchStats {
            lats_ms: pipe_durs_s.iter().map(|s| s * 1000.0).collect(),
            elapsed: Duration::from_secs_f64(pipe_p50_s),
            mb_s: pipe_mb_s,
            mib_s: pipe_mib_s,
            iops: 0.0,
            ..Default::default()
        },
        BenchStats {
            lats_ms: put_durs_s.iter().map(|s| s * 1000.0).collect(),
            elapsed: Duration::from_secs_f64(put_p50_s),
            mb_s: put_mb_s,
            mib_s: put_mib_s,
            iops: 0.0,
            ..Default::default()
        },
    ))
}

/// Benchmark parallel sliced get_file (20 GiB download to local disk or /dev/null).
async fn bench_get_file_20gib<F: FileSystem + 'static>(
    fs: Arc<F>,
    path: &str,
    file_size: u64,
    chunk_mb: usize,
    concurrency: usize,
    dest_path: &std::path::Path,
) -> Result<BenchStats, Box<dyn std::error::Error>> {
    let is_dev_null = dest_path == std::path::Path::new("/dev/null")
        || dest_path == std::path::Path::new("NUL")
        || dest_path == std::path::Path::new(r"\\.\NUL");

    if !is_dev_null {
        let _ = std::fs::remove_file(dest_path);
    }

    let chunk_bytes = chunk_mb * 1024 * 1024;
    let opts = ReadOptions::new()
        .with_chunk_size(chunk_bytes)
        .with_concurrency(concurrency);

    let sampler = ResourceSampler::start();
    let t0 = Instant::now();
    fs.get_file(path, dest_path, opts).await?;
    let elapsed = t0.elapsed();
    let res = sampler.stop().await;
    let elapsed_s = elapsed.as_secs_f64();

    if !is_dev_null {
        let meta = std::fs::metadata(dest_path)?;
        assert_eq!(
            meta.len(),
            file_size,
            "downloaded file size mismatch: expected {file_size}, got {}",
            meta.len()
        );

        // Verify no staging file remains in parent directory
        if let Some(parent) = dest_path.parent() {
            let file_prefix = format!(
                "{}.gstmp.",
                dest_path.file_name().unwrap_or_default().to_string_lossy()
            );
            if let Ok(entries) = std::fs::read_dir(parent) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with(&file_prefix) {
                        panic!("staging file {name} was not cleaned up");
                    }
                }
            }
        }

        let _ = std::fs::remove_file(dest_path);
    }

    let mb_s = (file_size as f64 / 1_000_000.0) / elapsed_s;
    let mib_s = (file_size as f64 / (1024.0 * 1024.0)) / elapsed_s;

    let label = format!(
        "FileSystem::get_file ({} 20 GiB, chunk={}M, conc={})",
        dest_path.display(),
        chunk_mb,
        concurrency
    );

    println!(
        "{:<56} | {:>4}T | {:>5} | {:>7.2}s | {:>8.2} MB/s ({:>7.2} MiB/s) | {:>5.1}% ({:>4.1} cores) | {:>5.1} / {:>5.1} MiB",
        label,
        concurrency,
        1,
        elapsed_s,
        mb_s,
        mib_s,
        res.avg_cpu_pct,
        res.avg_cpu_cores,
        res.avg_rss_mib,
        res.peak_rss_mib,
    );
    let _ = std::io::stdout().flush();

    Ok(BenchStats {
        lats_ms: vec![elapsed_s * 1000.0],
        elapsed,
        mb_s,
        mib_s,
        iops: 0.0,
        res,
    })
}

async fn run_20gb_benchmark_suite(
    path: &str,
    only: Option<&str>,
    seq_read_mb: usize,
    seq_io_mb: usize,
    get_chunk_mb: Option<usize>,
    get_concurrency: usize,
    get_dest: &std::path::Path,
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
    let run_write = matches!(only, None | Some("all" | "write" | "put"));
    let run_get = matches!(only, None | Some("all" | "get"));

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
    if run_seq_24t || run_seq_48t || run_get {
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
    let mut write_put_1t = BenchStats::default();

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

        let (ws, wp, wput) = bench_write_200mib_1t(
            cached_write_fs,
            &write_path,
            WRITE_SIZE,
            8 * 1024 * 1024,
        )
        .await?;
        write_stream_1t = ws;
        write_pipe_1t = wp;
        write_put_1t = wput;
        println!();
    }

    // -------------------------------------------------------------------------
    // PART 5: Parallel Sliced Download to /dev/shm (20 GiB)
    // -------------------------------------------------------------------------
    let mut get_64m_stats = BenchStats::default();
    let mut get_128m_stats = BenchStats::default();

    if run_get {
        println!("================================================================================");
        println!(
            "PART 5: PARALLEL SLICED DISK DOWNLOAD TO {} (20 GiB, 1 ITERATION)",
            get_dest.display()
        );
        println!("================================================================================");
        println!(
            "{:<56} | {:>5} | {:>5} | {:>8} | {:>24} | {:>16} | {:>16}",
            "Approach (20 GiB Sliced Download)",
            "Thrds",
            "Iters",
            "p50",
            "Throughput",
            "Avg CPU",
            "RSS (Avg/Peak)"
        );
        println!("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------");

        let chunks_to_test = if let Some(cmb) = get_chunk_mb {
            vec![cmb]
        } else {
            vec![128]
        };

        for cmb in chunks_to_test {
            let stats = bench_get_file_20gib(
                Arc::clone(&gcs_http_mt),
                path,
                file_size,
                cmb,
                get_concurrency,
                get_dest,
            )
            .await?;
            if cmb == 64 {
                get_64m_stats = stats;
            } else if cmb == 128 {
                get_128m_stats = stats;
            }
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
    "oneshot_pipe_file": {{ "p50_s": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }},
    "put_file_shm": {{ "p50_s": {:.3}, "mb_s": {:.2}, "mib_s": {:.2} }}
  }},
  "get_file_20gib": {{
    "dest": "{}",
    "chunk_64mib": {{ "wall_s": {:.3}, "mb_s": {:.2}, "mib_s": {:.2}, "avg_cpu_pct": {:.2}, "avg_cpu_cores": {:.2}, "avg_rss_mib": {:.2}, "peak_rss_mib": {:.2} }},
    "chunk_128mib": {{ "wall_s": {:.3}, "mb_s": {:.2}, "mib_s": {:.2}, "avg_cpu_pct": {:.2}, "avg_cpu_cores": {:.2}, "avg_rss_mib": {:.2}, "peak_rss_mib": {:.2} }}
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
            write_put_1t.elapsed.as_secs_f64(),
            write_put_1t.mb_s,
            write_put_1t.mib_s,
            get_dest.display(),
            get_64m_stats.elapsed.as_secs_f64(),
            get_64m_stats.mb_s,
            get_64m_stats.mib_s,
            get_64m_stats.res.avg_cpu_pct,
            get_64m_stats.res.avg_cpu_cores,
            get_64m_stats.res.avg_rss_mib,
            get_64m_stats.res.peak_rss_mib,
            get_128m_stats.elapsed.as_secs_f64(),
            get_128m_stats.mb_s,
            get_128m_stats.mib_s,
            get_128m_stats.res.avg_cpu_pct,
            get_128m_stats.res.avg_cpu_cores,
            get_128m_stats.res.avg_rss_mib,
            get_128m_stats.res.peak_rss_mib,
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
    let mut get_chunk_mb: Option<usize> = None;
    let mut get_concurrency: usize = 24;
    let mut get_dest = std::path::PathBuf::from("/dev/null");
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
            "--get-chunk-mb" => {
                i += 1;
                if let Some(v) = args.get(i) {
                    get_chunk_mb = Some(v.parse()?);
                }
            }
            "--get-concurrency" => {
                i += 1;
                if let Some(v) = args.get(i) {
                    get_concurrency = v.parse()?;
                }
            }
            "--get-dest" => {
                i += 1;
                if let Some(v) = args.get(i) {
                    get_dest = std::path::PathBuf::from(v);
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
        get_chunk_mb,
        get_concurrency,
        &get_dest,
        json_out.as_deref(),
    )
    .await
}
