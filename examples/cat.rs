//! Read an object (or a byte range of it) to stdout.
//!
//! ```text
//! cargo run --example cat -- gs://my-bucket/object [--start N] [--end N] [--transport grpc|http]
//! ```
//!
//! `--start`/`--end` follow Python-slice semantics: `--start -100` reads the
//! last 100 bytes, `--end -1` drops the final byte.

use std::io::Write;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use bytes::Bytes;
use gcs_rust_fs::{ByteRange, GcsFs, GcsPath, Transport};

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut path = None;
    let mut start = None;
    let mut end = None;
    let mut transport = None;

    fn int(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<i64, String> {
        args.next()
            .ok_or_else(|| format!("{flag} requires a value"))?
            .parse()
            .map_err(|e| format!("{flag}: {e}"))
    }

    while let Some(arg) = args.next() {
        let result: Result<(), String> = match arg.as_str() {
            "--start" | "-s" => int(&mut args, "--start").map(|v| start = Some(v)),
            "--end" | "-e" => int(&mut args, "--end").map(|v| end = Some(v)),
            "--transport" | "-t" => args
                .next()
                .ok_or_else(|| "--transport requires a value".to_string())
                .and_then(|v| v.parse::<Transport>().map_err(|e| e.to_string()))
                .map(|t| transport = Some(t)),
            _ if path.is_none() => {
                path = Some(arg);
                Ok(())
            }
            _ => Err(format!("unexpected argument {arg:?}")),
        };
        if let Err(reason) = result {
            return usage(&reason);
        }
    }
    let Some(path) = path else {
        return usage("missing path");
    };

    let (bytes, elapsed, transport) = match run(&path, ByteRange::new(start, end), transport).await
    {
        Ok(result) => result,
        Err(e) => {
            eprintln!("error ({}): {e}", e.kind());
            return ExitCode::FAILURE;
        }
    };

    let mut out = std::io::stdout().lock();
    if let Err(e) = out.write_all(&bytes).and_then(|()| out.flush()) {
        eprintln!("error writing to stdout: {e}");
        return ExitCode::FAILURE;
    }
    eprintln!(
        "\n[{} bytes in {:.1?} via {transport} transport]",
        bytes.len(),
        elapsed
    );
    ExitCode::SUCCESS
}

async fn run(
    path: &str,
    range: ByteRange,
    transport: Option<Transport>,
) -> gcs_rust_fs::Result<(Bytes, Duration, Transport)> {
    let path = GcsPath::parse(path)?;
    let mut builder = GcsFs::builder().from_env()?;
    if let Some(t) = transport {
        builder = builder.transport(t);
    }
    let fs = builder.build().await?;

    let started = Instant::now();
    let bytes = fs.cat_file(&path, range).await?;
    Ok((bytes, started.elapsed(), fs.transport()))
}

fn usage(reason: &str) -> ExitCode {
    eprintln!("error: {reason}");
    eprintln!("usage: cat <gs://bucket/object> [--start N] [--end N] [--transport grpc|http]");
    ExitCode::from(2)
}
