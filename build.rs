fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/gcsfs/v1/filesystem.proto");

    tonic_prost_build::configure()
        .bytes(".gcsfs.v1.ReadResponse.data")
        .bytes(".gcsfs.v1.WriteRequest.data")
        .compile_protos(&["proto/gcsfs/v1/filesystem.proto"], &["proto"])?;

    Ok(())
}
