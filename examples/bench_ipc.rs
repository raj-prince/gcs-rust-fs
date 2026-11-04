//! Empirical Benchmark: In-Process Memory Transfer vs. Out-of-Process UDS gRPC
//!
//! Compares:
//! 1. In-process direct memory handoff (simulating PyO3 C-extension FFI).
//! 2. Out-of-process gRPC over Unix Domain Sockets (the gcsfs-sidecar architecture).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use gcs_rust_fs::{
    ByteRange, Entry, EntryKind, Error, ErrorKind, File, FileSystem, FileSystemRpcService,
    ListOptions, OpenOptions, ReadOptions, RemoteFs, Result,
};
use tokio::sync::oneshot;

/// A simple, zero-overhead in-memory FileSystem for benchmarking.
struct BenchFs {
    payload: Bytes,
}

impl BenchFs {
    fn new(size_bytes: usize) -> Self {
        // Pre-allocate buffer with deterministic pattern
        let data = vec![0x42u8; size_bytes];
        Self {
            payload: Bytes::from(data),
        }
    }
}

#[async_trait]
impl FileSystem for BenchFs {
    async fn info(&self, path: &str) -> Result<Entry> {
        Ok(Entry {
            path: path.to_string(),
            kind: EntryKind::File,
            size: self.payload.len() as u64,
            stat: None,
        })
    }

    async fn ls(&self, _path: &str, _opts: ListOptions) -> Result<Vec<Entry>> {
        Ok(vec![])
    }

    async fn cat_file(&self, _path: &str, range: ByteRange, _opts: ReadOptions) -> Result<Bytes> {
        let r = range.clamp(self.payload.len() as u64);
        Ok(self.payload.slice(r.start as usize..r.end as usize))
    }

    async fn open(&self, _path: &str, _opts: OpenOptions) -> Result<Box<dyn File>> {
        Err(Error::new(ErrorKind::Unsupported, "not used in bench"))
    }

    async fn rm_file(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    async fn mkdir(&self, _path: &str, _opts: gcs_rust_fs::MkdirOptions) -> Result<()> {
        Ok(())
    }

    async fn rmdir(&self, _path: &str) -> Result<()> {
        Ok(())
    }
}

static BENCH_SOCKET_ID: AtomicUsize = AtomicUsize::new(0);

struct UdsServer {
    shutdown: Option<oneshot::Sender<()>>,
    sock_path: std::path::PathBuf,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for UdsServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = std::fs::remove_file(&self.sock_path);
    }
}

async fn setup_uds(fs: Arc<BenchFs>) -> (RemoteFs, UdsServer) {
    let id = BENCH_SOCKET_ID.fetch_add(1, Ordering::Relaxed);
    let sock_path = std::env::temp_dir().join(format!("gcsfs-bench-{}-{id}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock_path);

    let (tx, rx) = oneshot::channel::<()>();
    let service = FileSystemRpcService::new(fs);
    let server_sock = sock_path.clone();

    let handle = tokio::spawn(async move {
        let _ = service
            .serve_uds_with_shutdown(&server_sock, async {
                let _ = rx.await;
            })
            .await;
    });

    for _ in 0..100 {
        if sock_path.exists() {
            if let Ok(client) = RemoteFs::connect_uds(&sock_path).await {
                return (
                    client,
                    UdsServer {
                        shutdown: Some(tx),
                        sock_path,
                        handle,
                    },
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("Failed to connect to benchmark UDS server at {:?}", sock_path);
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("================================================================================");
    println!("     EMPIRICAL BENCHMARK: IN-PROCESS (PyO3) vs. OUT-OF-PROCESS UDS gRPC         ");
    println!("================================================================================\n");

    // -------------------------------------------------------------------------
    // Benchmark 1: Metadata Latency (info RPC)
    // -------------------------------------------------------------------------
    println!("--- Benchmark 1: Metadata Query Latency (`info`) ---");
    let bench_fs = Arc::new(BenchFs::new(1024));
    let (remote_fs, _server) = setup_uds(bench_fs.clone()).await;

    let num_metadata_calls = 5_000;

    // In-Process
    let start = Instant::now();
    for _ in 0..num_metadata_calls {
        let _ = bench_fs.info("test/path").await?;
    }
    let inproc_duration = start.elapsed();
    let inproc_latency_us = inproc_duration.as_micros() as f64 / num_metadata_calls as f64;
    let inproc_qps = num_metadata_calls as f64 / inproc_duration.as_secs_f64();

    // UDS gRPC
    let start = Instant::now();
    for _ in 0..num_metadata_calls {
        let _ = remote_fs.info("test/path").await?;
    }
    let uds_duration = start.elapsed();
    let uds_latency_us = uds_duration.as_micros() as f64 / num_metadata_calls as f64;
    let uds_qps = num_metadata_calls as f64 / uds_duration.as_secs_f64();

    println!("  * In-Process (PyO3 Direct): {:>7.2} µs/op | {:>10.0} QPS", inproc_latency_us, inproc_qps);
    println!("  * UDS gRPC (Sidecar):       {:>7.2} µs/op | {:>10.0} QPS", uds_latency_us, uds_qps);
    println!("  ==> UDS Overhead: {:.1}x higher latency ({:.2} µs added per call)\n",
        uds_latency_us / inproc_latency_us, uds_latency_us - inproc_latency_us);

    // -------------------------------------------------------------------------
    // Benchmark 2: Single-Stream Throughput vs. Payload Size
    // -------------------------------------------------------------------------
    println!("--- Benchmark 2: Single-Stream Read Throughput vs. Payload Size ---");
    println!("{:<12} | {:<22} | {:<22} | {:<12}", "Payload Size", "In-Process (PyO3)", "UDS gRPC (Sidecar)", "Throughput Gap");
    println!("-------------+------------------------+------------------------+-------------");

    let payload_sizes = [
        ("1 MiB", 1 * 1024 * 1024, 100),
        ("10 MiB", 10 * 1024 * 1024, 30),
        ("50 MiB", 50 * 1024 * 1024, 10),
        ("200 MiB", 200 * 1024 * 1024, 5),
    ];

    for (label, size, iterations) in payload_sizes {
        let bench_fs = Arc::new(BenchFs::new(size));
        let (remote_fs, _server) = setup_uds(bench_fs.clone()).await;

        // In-Process
        let start = Instant::now();
        for _ in 0..iterations {
            let res = bench_fs.cat_file("test", ByteRange::ALL, ReadOptions::default()).await?;
            assert_eq!(res.len(), size);
        }
        let inproc_secs = start.elapsed().as_secs_f64();
        let inproc_gbps = (size as f64 * iterations as f64 * 8.0) / (inproc_secs * 1_000_000_000.0);
        let inproc_gbs = (size as f64 * iterations as f64) / (inproc_secs * 1024.0 * 1024.0 * 1024.0);

        // UDS gRPC
        let start = Instant::now();
        for _ in 0..iterations {
            let res = remote_fs.cat_file("test", ByteRange::ALL, ReadOptions::default()).await?;
            assert_eq!(res.len(), size);
        }
        let uds_secs = start.elapsed().as_secs_f64();
        let uds_gbps = (size as f64 * iterations as f64 * 8.0) / (uds_secs * 1_000_000_000.0);
        let uds_gbs = (size as f64 * iterations as f64) / (uds_secs * 1024.0 * 1024.0 * 1024.0);

        println!(
            "{:<12} | {:>6.2} GB/s ({:>5.1} Gbps) | {:>6.2} GB/s ({:>5.1} Gbps) | {:.1}x faster",
            label, inproc_gbs, inproc_gbps, uds_gbs, uds_gbps, inproc_gbs / uds_gbs
        );
    }
    println!();

    // -------------------------------------------------------------------------
    // Benchmark 3: Multi-Worker Concurrent Data Loading (PyTorch Simulation)
    // -------------------------------------------------------------------------
    println!("--- Benchmark 3: Multi-Worker Concurrent Streaming (PyTorch DataLoader Simulation) ---");
    println!("Simulating N concurrent workers reading 50 MiB shards simultaneously...\n");
    println!("{:<14} | {:<22} | {:<22} | {:<12}", "Concurrency", "In-Process (PyO3)", "UDS gRPC (Sidecar)", "Speedup");
    println!("---------------+------------------------+------------------------+-------------");

    let worker_counts = [2, 4, 8, 16];
    let shard_size = 50 * 1024 * 1024; // 50 MiB per worker

    for workers in worker_counts {
        let bench_fs = Arc::new(BenchFs::new(shard_size));
        let (remote_fs, _server) = setup_uds(bench_fs.clone()).await;

        // In-Process concurrent workers
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..workers {
            let fs_clone = bench_fs.clone();
            handles.push(tokio::spawn(async move {
                let res = fs_clone.cat_file("test", ByteRange::ALL, ReadOptions::default()).await.unwrap();
                assert_eq!(res.len(), shard_size);
            }));
        }
        for h in handles {
            h.await?;
        }
        let inproc_secs = start.elapsed().as_secs_f64();
        let total_bytes = shard_size as f64 * workers as f64;
        let inproc_gbs = total_bytes / (inproc_secs * 1024.0 * 1024.0 * 1024.0);
        let inproc_gbps = (total_bytes * 8.0) / (inproc_secs * 1_000_000_000.0);

        // UDS gRPC concurrent workers
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..workers {
            let remote_clone = remote_fs.clone();
            handles.push(tokio::spawn(async move {
                let res = remote_clone.cat_file("test", ByteRange::ALL, ReadOptions::default()).await.unwrap();
                assert_eq!(res.len(), shard_size);
            }));
        }
        for h in handles {
            h.await?;
        }
        let uds_secs = start.elapsed().as_secs_f64();
        let uds_gbs = total_bytes / (uds_secs * 1024.0 * 1024.0 * 1024.0);
        let uds_gbps = (total_bytes * 8.0) / (uds_secs * 1_000_000_000.0);

        println!(
            "{:<14} | {:>6.2} GB/s ({:>5.1} Gbps) | {:>6.2} GB/s ({:>5.1} Gbps) | {:.1}x faster",
            format!("{} workers", workers), inproc_gbs, inproc_gbps, uds_gbs, uds_gbps, inproc_gbs / uds_gbs
        );
    }
    println!();

    // -------------------------------------------------------------------------
    // Benchmark 4: Multi-Worker with Dedicated UDS Sockets (1 Socket Per Worker)
    // -------------------------------------------------------------------------
    println!("--- Benchmark 4: Multi-Worker with Dedicated UDS Sockets (1 Socket per Worker) ---");
    println!("Simulating N concurrent workers each with its OWN dedicated UDS socket and listener...\n");
    println!("{:<14} | {:<22} | {:<22} | {:<12}", "Concurrency", "Shared UDS Socket", "Dedicated UDS Sockets", "Speedup vs Shared");
    println!("---------------+------------------------+------------------------+-------------------");

    for workers in worker_counts {
        let bench_fs = Arc::new(BenchFs::new(shard_size));
        let total_bytes = shard_size as f64 * workers as f64;

        // Shared UDS socket (from benchmark 3 logic)
        let (shared_remote_fs, _shared_server) = setup_uds(bench_fs.clone()).await;
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..workers {
            let remote_clone = shared_remote_fs.clone();
            handles.push(tokio::spawn(async move {
                let res = remote_clone.cat_file("test", ByteRange::ALL, ReadOptions::default()).await.unwrap();
                assert_eq!(res.len(), shard_size);
            }));
        }
        for h in handles {
            h.await?;
        }
        let shared_secs = start.elapsed().as_secs_f64();
        let shared_gbs = total_bytes / (shared_secs * 1024.0 * 1024.0 * 1024.0);
        let shared_gbps = (total_bytes * 8.0) / (shared_secs * 1_000_000_000.0);

        // Dedicated UDS sockets (1 independent socket file per worker)
        let mut worker_clients = Vec::new();
        let mut _worker_servers = Vec::new();
        for _ in 0..workers {
            let (client, server) = setup_uds(bench_fs.clone()).await;
            worker_clients.push(client);
            _worker_servers.push(server);
        }

        let start = Instant::now();
        let mut handles = Vec::new();
        for client in worker_clients {
            handles.push(tokio::spawn(async move {
                let res = client.cat_file("test", ByteRange::ALL, ReadOptions::default()).await.unwrap();
                assert_eq!(res.len(), shard_size);
            }));
        }
        for h in handles {
            h.await?;
        }
        let dedicated_secs = start.elapsed().as_secs_f64();
        let dedicated_gbs = total_bytes / (dedicated_secs * 1024.0 * 1024.0 * 1024.0);
        let dedicated_gbps = (total_bytes * 8.0) / (dedicated_secs * 1_000_000_000.0);

        println!(
            "{:<14} | {:>6.2} GB/s ({:>5.1} Gbps) | {:>6.2} GB/s ({:>5.1} Gbps) | {:.1}x",
            format!("{} workers", workers), shared_gbs, shared_gbps, dedicated_gbs, dedicated_gbps, dedicated_gbs / shared_gbs
        );
    }

    println!("\n================================================================================");
    println!("                           BENCHMARK COMPLETE                                   ");
    println!("================================================================================");
    Ok(())
}
