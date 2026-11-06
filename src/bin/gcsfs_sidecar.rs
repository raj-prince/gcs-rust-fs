//! Sidecar gRPC server binary (`gcsfs-sidecar`).
//!
//! Defaults to `Transport::Http` (the JSON/HTTP data path) and serves
//! `gcsfs.v1.FileSystemService` (`Info`, `List`, `Read`, `Write`) over a Unix Domain
//! Socket (or TCP address).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use gcs_rust_fs::{
    CacheConfig, CachedFs, DataConfig, Error, FileSystem, FileSystemRpcService, GcsFs,
    PageCacheConfig, ReadaheadConfig, Result, Transport,
};

const DEFAULT_UDS_PATH: &str = "/var/run/gcsfs/sidecar.sock";

#[derive(Debug)]
struct SidecarArgs {
    uds: Vec<PathBuf>,
    tcp: Option<SocketAddr>,
    transport: Transport,
    endpoint: Option<String>,
    project: Option<String>,
    mode: Option<u32>,
    cache: bool,
    readahead: bool,
    page_cache_mb: Option<usize>,
    http_pool_size: Option<usize>,
}

impl SidecarArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut uds: Vec<PathBuf> = Vec::new();
        let mut tcp: Option<SocketAddr> = None;
        // Focus on the JSON/HTTP path by default unless overridden.
        let mut transport = std::env::var(Transport::ENV_VAR)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.parse())
            .transpose()?
            .unwrap_or(Transport::Http);
        let mut endpoint: Option<String> = None;
        let mut project: Option<String> = None;
        let mut mode: Option<u32> = Some(0o666);
        let mut cache = false;
        let mut readahead = false;
        let mut page_cache_mb: Option<usize> = None;
        let mut http_pool_size: Option<usize> = None;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--uds" => {
                    let val = iter
                        .next()
                        .ok_or_else(|| Error::new(gcs_rust_fs::ErrorKind::InvalidConfig, "--uds requires a path"))?;
                    uds.push(PathBuf::from(val));
                }
                "--mode" => {
                    let val = iter
                        .next()
                        .ok_or_else(|| Error::new(gcs_rust_fs::ErrorKind::InvalidConfig, "--mode requires an octal mode (e.g. 0666)"))?;
                    let m = u32::from_str_radix(val.trim_start_matches("0o"), 8).map_err(|e| {
                        Error::with_source(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            format!("invalid octal mode {val:?}"),
                            e,
                        )
                    })?;
                    mode = Some(m);
                }
                "--tcp" => {
                    let val = iter
                        .next()
                        .ok_or_else(|| Error::new(gcs_rust_fs::ErrorKind::InvalidConfig, "--tcp requires an address"))?;
                    let addr: SocketAddr = val.parse().map_err(|e| {
                        Error::with_source(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            format!("invalid --tcp address {val:?}"),
                            e,
                        )
                    })?;
                    tcp = Some(addr);
                }
                "--transport" => {
                    let val = iter.next().ok_or_else(|| {
                        Error::new(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            "--transport requires 'http' or 'grpc'",
                        )
                    })?;
                    transport = val.parse()?;
                }
                "--endpoint" => {
                    let val = iter.next().ok_or_else(|| {
                        Error::new(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            "--endpoint requires a URL",
                        )
                    })?;
                    endpoint = Some(val);
                }
                "--project" => {
                    let val = iter.next().ok_or_else(|| {
                        Error::new(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            "--project requires a project ID",
                        )
                    })?;
                    project = Some(val);
                }
                "--cache" => {
                    cache = true;
                }
                "--readahead" => {
                    readahead = true;
                }
                "--page-cache-mb" => {
                    let val = iter.next().ok_or_else(|| {
                        Error::new(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            "--page-cache-mb requires a number of MiB",
                        )
                    })?;
                    let mb: usize = val.parse().map_err(|e| {
                        Error::with_source(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            format!("invalid --page-cache-mb {val:?}"),
                            e,
                        )
                    })?;
                    page_cache_mb = Some(mb);
                }
                "--http-pool-size" => {
                    let val = iter.next().ok_or_else(|| {
                        Error::new(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            "--http-pool-size requires a positive integer",
                        )
                    })?;
                    let n: usize = val.parse().map_err(|e| {
                        Error::with_source(
                            gcs_rust_fs::ErrorKind::InvalidConfig,
                            format!("invalid --http-pool-size {val:?}"),
                            e,
                        )
                    })?;
                    http_pool_size = Some(n);
                }
                "--help" | "-h" => {
                    eprintln!(
                        "Usage: gcsfs-sidecar [--uds <PATH>] [--mode <OCTAL>] [--tcp <HOST:PORT>] [--transport http|grpc] [--endpoint <URL>] [--project <PROJECT>] [--cache] [--readahead] [--page-cache-mb <MB>] [--http-pool-size <N>]"
                    );
                    std::process::exit(0);
                }
                other => {
                    return Err(Error::new(
                        gcs_rust_fs::ErrorKind::InvalidConfig,
                        format!("unknown argument {other:?}"),
                    ));
                }
            }
        }

        if uds.is_empty() && tcp.is_none() {
            uds.push(PathBuf::from(DEFAULT_UDS_PATH));
        }

        Ok(Self {
            uds,
            tcp,
            transport,
            endpoint,
            project,
            mode,
            cache,
            readahead,
            page_cache_mb,
            http_pool_size,
        })
    }
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = SidecarArgs::parse(std::env::args().skip(1))?;

    let mut builder = GcsFs::builder().from_env()?.transport(args.transport);
    if let Some(ep) = args.endpoint {
        builder = builder.endpoint(ep);
    }
    if let Some(proj) = args.project {
        builder = builder.project(proj);
    }
    if let Some(n) = args.http_pool_size {
        builder = builder.http_pool_size(n);
    }

    let gcs = builder.build().await?;
    let fs: Arc<dyn FileSystem> = if args.cache || args.readahead || args.page_cache_mb.is_some() {
        let mut cfg = CacheConfig::default();
        if args.readahead || args.page_cache_mb.is_some() {
            cfg.data = Some(DataConfig {
                readahead: ReadaheadConfig {
                    page_size: 256 * 1024,
                    io_size: 16 * 1024 * 1024,
                    max_folio_size: 16 * 1024 * 1024,
                    max_readahead: if args.readahead { 256 * 1024 * 1024 } else { 0 },
                    max_inflight: 16,
                    max_inflight_bytes: 4 * 1024 * 1024 * 1024,
                },
                cache: args.page_cache_mb.map(|mb| PageCacheConfig {
                    capacity: mb * 1024 * 1024,
                }),
            });
        }
        Arc::new(CachedFs::new(gcs, cfg))
    } else {
        Arc::new(gcs)
    };

    let service = FileSystemRpcService::new(fs);
    let shutdown = wait_for_shutdown_signal();

    if let Some(addr) = args.tcp {
        eprintln!(
            "gcsfs-sidecar listening on tcp://{} (gcs transport: {})",
            addr, args.transport
        );
        service.serve_tcp_with_shutdown(addr, shutdown).await
    } else if !args.uds.is_empty() {
        for path in &args.uds {
            eprintln!(
                "gcsfs-sidecar listening on unix://{} (mode: {:?}, gcs transport: {})",
                path.display(),
                args.mode.map(|m| format!("0{:o}", m)),
                args.transport
            );
        }
        service
            .serve_multiple_uds_with_mode_and_shutdown(&args.uds, args.mode, shutdown)
            .await
    } else {
        Ok(())
    }
}
