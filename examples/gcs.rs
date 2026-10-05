//! A small CLI over the `FileSystem` trait, handy for poking at real buckets.
//!
//! ```text
//! cargo run --example gcs -- info   gs://bucket/path
//! cargo run --example gcs -- ls     gs://bucket/dir [--versions]
//! cargo run --example gcs -- find   gs://bucket/dir [--withdirs] [--maxdepth N]
//! cargo run --example gcs -- walk   gs://bucket/dir
//! cargo run --example gcs -- du     gs://bucket/dir
//! cargo run --example gcs -- cat    gs://bucket/file [--start N] [--end N]
//! cargo run --example gcs -- get    gs://bucket/file ./local
//! cargo run --example gcs -- put    ./local gs://bucket/file
//! cargo run --example gcs -- pipe   gs://bucket/file "some text"
//! cargo run --example gcs -- cp     gs://bucket/src gs://bucket/dst [-r]
//! cargo run --example gcs -- mv     gs://bucket/src gs://bucket/dst [-r]
//! cargo run --example gcs -- rm     gs://bucket/path [-r]
//! cargo run --example gcs -- mkdir  gs://bucket/dir [-p] [--placeholder]
//! cargo run --example gcs -- rmdir  gs://bucket/dir
//! cargo run --example gcs -- kind   bucket
//! ```
//!
//! `--transport grpc|http` (or `GCS_RUST_FS_TRANSPORT`) picks the data path
//! for non-zonal buckets. `--start`/`--end` use Python-slice semantics.

use std::path::Path;
use std::process::ExitCode;

use gcs_rust_fs::{
    ByteRange, CopyOptions, DuOptions, Entry, FileSystem, FindOptions, GcsFs, ListOptions,
    MkdirOptions, RmOptions, Transport, WalkOptions, WriteOptions,
};

struct Args {
    command: String,
    positional: Vec<String>,
    flags: Vec<String>,
    start: Option<i64>,
    end: Option<i64>,
    maxdepth: Option<usize>,
    transport: Option<Transport>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or("missing command")?;
    let mut out = Args {
        command,
        positional: Vec::new(),
        flags: Vec::new(),
        start: None,
        end: None,
        maxdepth: None,
        transport: None,
    };
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or(format!("{name} needs a value"));
        match arg.as_str() {
            "--start" => out.start = Some(value("--start")?.parse().map_err(|e| format!("{e}"))?),
            "--end" => out.end = Some(value("--end")?.parse().map_err(|e| format!("{e}"))?),
            "--maxdepth" => {
                out.maxdepth = Some(value("--maxdepth")?.parse().map_err(|e| format!("{e}"))?)
            }
            "--transport" => {
                out.transport = Some(value("--transport")?.parse().map_err(|e| format!("{e}"))?)
            }
            flag if flag.starts_with('-') => out.flags.push(flag.to_string()),
            _ => out.positional.push(arg),
        }
    }
    Ok(out)
}

impl Args {
    fn flag(&self, name: &str) -> bool {
        self.flags.iter().any(|f| f == name)
    }

    fn arg(&self, index: usize, what: &str) -> Result<&str, String> {
        self.positional
            .get(index)
            .map(String::as_str)
            .ok_or_else(|| format!("missing argument: {what}"))
    }
}

fn print_entry(e: &Entry) {
    println!("{:<9} {:>12} {}", e.kind.as_str(), e.size, e.path);
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = GcsFs::builder().from_env()?;
    if let Some(transport) = args.transport {
        builder = builder.transport(transport);
    }
    let fs = builder.build().await?;
    let recursive = args.flag("-r") || args.flag("--recursive");

    match args.command.as_str() {
        "info" => {
            let e = fs.info(args.arg(0, "path")?).await?;
            print_entry(&e);
            if let Some(stat) = &e.stat {
                println!("{stat:#?}");
            }
        }
        "ls" => {
            let opts = ListOptions {
                versions: args.flag("--versions"),
            };
            for e in fs.ls(args.arg(0, "path")?, opts).await? {
                print_entry(&e);
            }
        }
        "find" => {
            let opts = FindOptions {
                withdirs: args.flag("--withdirs"),
                maxdepth: args.maxdepth,
                versions: args.flag("--versions"),
            };
            for e in fs.find(args.arg(0, "path")?, opts).await? {
                print_entry(&e);
            }
        }
        "walk" => {
            let opts = WalkOptions {
                maxdepth: args.maxdepth,
            };
            for w in fs.walk(args.arg(0, "path")?, opts).await? {
                println!("{}/", w.dir);
                for d in &w.dirs {
                    println!("    [dir]  {}", d.name());
                }
                for f in &w.files {
                    println!("    {:>10} {}", f.size, f.name());
                }
            }
        }
        "du" => {
            let usage = fs.du(args.arg(0, "path")?, DuOptions::default()).await?;
            for (path, size) in &usage.sizes {
                println!("{size:>12} {path}");
            }
            println!("{:>12} total", usage.total);
        }
        "cat" => {
            let range = ByteRange::new(args.start, args.end);
            let bytes = fs.cat_file(args.arg(0, "path")?, range).await?;
            use std::io::Write as _;
            std::io::stdout().write_all(&bytes)?;
        }
        "get" => {
            fs.get_file(args.arg(0, "path")?, Path::new(args.arg(1, "local path")?))
                .await?;
        }
        "put" => {
            fs.put_file(
                Path::new(args.arg(0, "local path")?),
                args.arg(1, "path")?,
                WriteOptions::default(),
            )
            .await?;
        }
        "pipe" => {
            let data = args.arg(1, "text")?.to_owned();
            fs.pipe_file(args.arg(0, "path")?, data.into(), WriteOptions::default())
                .await?;
        }
        "cp" | "mv" => {
            let opts = CopyOptions {
                recursive,
                ..Default::default()
            };
            let (src, dst) = (args.arg(0, "source")?, args.arg(1, "destination")?);
            if args.command == "cp" {
                fs.copy(src, dst, opts).await?;
            } else {
                fs.mv(src, dst, opts).await?;
            }
        }
        "rm" => {
            let opts = RmOptions {
                recursive,
                ..Default::default()
            };
            fs.rm(args.arg(0, "path")?, opts).await?;
        }
        "mkdir" => {
            let opts = MkdirOptions {
                create_parents: args.flag("-p"),
                placeholder: args.flag("--placeholder"),
                location: None,
            };
            fs.mkdir(args.arg(0, "path")?, opts).await?;
        }
        "rmdir" => fs.rmdir(args.arg(0, "path")?).await?,
        "kind" => println!("{}", fs.bucket_kind(args.arg(0, "bucket")?).await?),
        other => return Err(format!("unknown command {other:?}").into()),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("error: {e}\nusage: gcs <info|ls|find|walk|du|cat|get|put|pipe|cp|mv|rm|mkdir|rmdir|kind> ...");
            return ExitCode::from(2);
        }
    };
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
