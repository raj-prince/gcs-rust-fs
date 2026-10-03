//! Print object metadata.
//!
//! ```text
//! cargo run --example stat -- gs://my-bucket/path/to/object [--transport grpc|http]
//! ```

use std::process::ExitCode;

use gcs_rust_fs::{GcsFs, GcsPath, Transport};

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut path = None;
    let mut transport = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--transport" | "-t" => match args.next().map(|v| v.parse::<Transport>()) {
                Some(Ok(t)) => transport = Some(t),
                Some(Err(e)) => return usage(&e.to_string()),
                None => return usage("--transport requires a value"),
            },
            _ if path.is_none() => path = Some(arg),
            _ => return usage(&format!("unexpected argument {arg:?}")),
        }
    }
    let Some(path) = path else {
        return usage("missing path");
    };

    match run(&path, transport).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error ({}): {e}", e.kind());
            ExitCode::FAILURE
        }
    }
}

async fn run(path: &str, transport: Option<Transport>) -> gcs_rust_fs::Result<()> {
    let path = GcsPath::parse(path)?;
    let mut builder = GcsFs::builder().from_env()?;
    if let Some(t) = transport {
        builder = builder.transport(t);
    }
    let fs = builder.build().await?;
    let stat = fs.stat(&path).await?;
    println!("{stat:#?}");
    Ok(())
}

fn usage(reason: &str) -> ExitCode {
    eprintln!("error: {reason}");
    eprintln!("usage: stat <gs://bucket/object> [--transport grpc|http]");
    ExitCode::from(2)
}
