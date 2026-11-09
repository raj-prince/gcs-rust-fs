fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/gcsfs/v1/filesystem.proto");

    if std::env::var("PROTOC").is_err() {
        if let Ok(home) = std::env::var("HOME") {
            let candidate = std::path::PathBuf::from(home)
                .join("c1dev/gcsfs/.venv/lib/python3.14/site-packages/torch/bin/protoc");
            if candidate.exists() {
                std::env::set_var("PROTOC", candidate);
            }
        }
    }

    tonic_prost_build::configure()
        .bytes(".gcsfs.v1.ReadResponse.data")
        .bytes(".gcsfs.v1.WriteRequest.data")
        .compile_protos(&["proto/gcsfs/v1/filesystem.proto"], &["proto"])?;

    Ok(())
}
