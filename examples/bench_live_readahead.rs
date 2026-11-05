//! End-to-end live benchmark comparing Direct Unbuffered Reads vs
//! Kernel-Grade Asynchronous Readahead (Mode 1: drop-behind, no page cache)
//! against real Google Cloud Storage objects.
//!
//! Usage:
//!   cargo run --release --example bench_live_readahead -- [OPTIONS] [GS_PATH]
//!
//! Options:
//!   --bytes <MIB>       Total MiB to read per run (default: 128)
//!   --chunk-size <MIB>  Read buffer / chunk size in MiB (default: 8)
//!   --max-ra <MIB>      Max readahead window in MiB (default: 64)

use std::sync::Arc;
use std::time::{Duration, Instant};

use gcs_rust_fs::{
    CacheConfig, CachedFs, DataConfig, FileSystem, GcsFs, OpenOptions, ReadPattern,
    ReadaheadConfig, Transport,
};

struct RunMetrics {
    total_bytes: usize,
    elapsed: Duration,
    throughput_mb: f64,
    throughput_mib: f64,
    chunk_latencies_ms: Vec<f64>,
}

async fn run_sequential_read(
    name: &str,
    fs: &dyn FileSystem,
    path: &str,
    total_bytes: usize,
    chunk_size: usize,
    pattern: Option<ReadPattern>,
) -> Result<RunMetrics, Box<dyn std::error::Error>> {
    println!("\n========================================================");
    println!(">>> Starting Run: {name}");
    println!("Target:      {path}");
    println!("Total Read:  {} MiB ({} bytes)", total_bytes / (1024 * 1024), total_bytes);
    println!("Chunk Size:  {} MiB", chunk_size / (1024 * 1024));
    println!("Pattern:     {:?}", pattern.unwrap_or(ReadPattern::Auto));
    println!("========================================================");

    let mut opts = OpenOptions::read().with_block_size(chunk_size);
    if let Some(p) = pattern {
        opts = opts.with_pattern(p);
    }

    let t_open = Instant::now();
    let mut file = fs.open(path, opts).await?;
    let open_dur = t_open.elapsed();
    println!("Handle opened in {:.2}ms (object size: {:?} bytes)",
        open_dur.as_secs_f64() * 1000.0, file.size());

    let mut bytes_read = 0;
    let mut chunk_latencies = Vec::new();
    let start_all = Instant::now();
    let mut chunk_idx = 0;

    while bytes_read < total_bytes {
        let to_read = chunk_size.min(total_bytes - bytes_read);
        let t_chunk = Instant::now();
        let chunk = file.read(Some(to_read)).await?;
        let lat = t_chunk.elapsed().as_secs_f64() * 1000.0;
        let n = chunk.len();

        if n == 0 {
            println!("EOF reached at byte offset {}", bytes_read);
            break;
        }

        chunk_latencies.push(lat);
        bytes_read += n;
        chunk_idx += 1;

        let chunk_speed = (n as f64 / 1_000_000.0) / (lat / 1000.0);
        println!(
            "  Chunk #{:<2} [offset {:>4} MiB -> {:>4} MiB]: {:>8} bytes in {:>7.2}ms ({:>7.2} MB/s)",
            chunk_idx,
            (bytes_read - n) / (1024 * 1024),
            bytes_read / (1024 * 1024),
            n,
            lat,
            chunk_speed
        );
    }

    let elapsed = start_all.elapsed();
    let mb = bytes_read as f64 / 1_000_000.0;
    let mib = bytes_read as f64 / (1024.0 * 1024.0);
    let throughput_mb = mb / elapsed.as_secs_f64();
    let throughput_mib = mib / elapsed.as_secs_f64();

    println!("--------------------------------------------------------");
    println!("Completed:   {name}");
    println!("Bytes Read:  {} bytes ({:.2} MiB)", bytes_read, mib);
    println!("Wall Time:   {:.3}s", elapsed.as_secs_f64());
    println!("Throughput:  {:.2} MB/s ({:.2} MiB/s)", throughput_mb, throughput_mib);
    println!("--------------------------------------------------------");

    Ok(RunMetrics {
        total_bytes: bytes_read,
        elapsed,
        throughput_mb,
        throughput_mib,
        chunk_latencies_ms: chunk_latencies,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut target_object = "gs://princer-test-bucket/1G/experiment.0.0".to_string();
    let mut total_mib: usize = 128;
    let mut chunk_mib: usize = 8;
    let mut max_ra_mib: usize = 64;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bytes" => {
                if let Some(v) = args.next() {
                    total_mib = v.parse()?;
                }
            }
            "--chunk-size" => {
                if let Some(v) = args.next() {
                    chunk_mib = v.parse()?;
                }
            }
            "--max-ra" => {
                if let Some(v) = args.next() {
                    max_ra_mib = v.parse()?;
                }
            }
            other if !other.starts_with('-') => {
                target_object = other.to_string();
            }
            _ => {}
        }
    }

    let builder = GcsFs::builder().transport(Transport::Http).from_env()?;
    let gcs_fs = Arc::new(builder.build().await?);
    println!("Connected to GCS successfully via Application Default Credentials.");

    let total_bytes = total_mib * 1024 * 1024;
    let chunk_size = chunk_mib * 1024 * 1024;

    // -------------------------------------------------------------------------
    // Run 1: Direct unbuffered GCS reads (baseline)
    // -------------------------------------------------------------------------
    let direct_metrics = run_sequential_read(
        "Direct Unbuffered Read (Baseline)",
        gcs_fs.as_ref(),
        &target_object,
        total_bytes,
        chunk_size,
        None,
    )
    .await?;

    // Cool-down pause
    tokio::time::sleep(Duration::from_millis(500)).await;

    // -------------------------------------------------------------------------
    // Run 2: Readahead-only (Mode 1: private BTreeMap, drop-behind, NO page cache)
    // -------------------------------------------------------------------------
    let ra_config = ReadaheadConfig {
        page_size: 256 * 1024,
        max_readahead: max_ra_mib * 1024 * 1024,
        io_size: 8 * 1024 * 1024,
        max_folio_size: 32 * 1024 * 1024,
        max_inflight: 8,
        max_inflight_bytes: 256 * 1024 * 1024,
    };
    let cache_config = CacheConfig::default().with_data(DataConfig {
        readahead: ra_config,
        cache: None, // Mode 1: drop-behind, no page cache!
    });
    let cached_fs = Arc::new(CachedFs::new((*gcs_fs).clone(), cache_config));

    let readahead_metrics = run_sequential_read(
        "Asynchronous Readahead (Mode 1: Drop-Behind, No Page Cache)",
        cached_fs.as_ref(),
        &target_object,
        total_bytes,
        chunk_size,
        Some(ReadPattern::Sequential),
    )
    .await?;

    // -------------------------------------------------------------------------
    // Comparison Summary
    // -------------------------------------------------------------------------
    println!("\n========================================================");
    println!("             FINAL BENCHMARK COMPARISON");
    println!("========================================================");
    println!("Object:       {target_object}");
    println!("Data Read:    {total_mib} MiB");
    println!("Chunk Size:   {chunk_mib} MiB");
    println!("Max Window:   {max_ra_mib} MiB");
    println!("--------------------------------------------------------");
    println!(
        "Direct Read:        {:.3}s  |  {:>7.2} MB/s ({:>7.2} MiB/s)",
        direct_metrics.elapsed.as_secs_f64(),
        direct_metrics.throughput_mb,
        direct_metrics.throughput_mib,
    );
    println!(
        "Async Readahead:    {:.3}s  |  {:>7.2} MB/s ({:>7.2} MiB/s)",
        readahead_metrics.elapsed.as_secs_f64(),
        readahead_metrics.throughput_mb,
        readahead_metrics.throughput_mib,
    );
    let speedup = direct_metrics.elapsed.as_secs_f64() / readahead_metrics.elapsed.as_secs_f64();
    println!("--------------------------------------------------------");
    println!("SPEEDUP:            {:.2}x FASTER", speedup);
    println!("Throughput Delta:   +{:>6.2} MB/s (+{:.1}%)",
        readahead_metrics.throughput_mb - direct_metrics.throughput_mb,
        ((readahead_metrics.throughput_mb / direct_metrics.throughput_mb) - 1.0) * 100.0
    );
    println!("========================================================\n");

    Ok(())
}
