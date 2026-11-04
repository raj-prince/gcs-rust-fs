//! Sidecar gRPC server binary (`gcsfs-sidecar`).
//!
//! Defaults to `Transport::Http` (the JSON/HTTP data path) and serves
//! `gcsfs.v1.FileSystemService` (`Info`, `Read`, `Write`) over a Unix Domain
//! Socket (or TCP address).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use gcs_rust_fs::{
    CacheConfig, CachedFs, Error, FileSystem, FileSystemRpcService, GcsFs, Result, Transport,
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
                "--help" | "-h" => {
                    eprintln!(
                        "Usage: gcsfs-sidecar [--uds <PATH>] [--mode <OCTAL>] [--tcp <HOST:PORT>] [--transport http|grpc] [--endpoint <URL>] [--project <PROJECT>] [--cache]"
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

    let gcs = builder.build().await?;
    let fs: Arc<dyn FileSystem> = if args.cache {
        Arc::new(CachedFs::new(gcs, CacheConfig::default()))
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
