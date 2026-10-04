//! Live integration tests against a real bucket.
//!
//! These are skipped unless `GCS_RUST_FS_TEST_OBJECT` names an existing object
//! (e.g. `gs://my-bucket/some/file.bin`) readable with Application Default
//! Credentials. Set `GCS_RUST_FS_TRANSPORT=http` to exercise the JSON API path
//! instead of bidi gRPC (which must be enabled for the bucket).
//!
//! ```text
//! GCS_RUST_FS_TEST_OBJECT=gs://my-bucket/file.bin cargo test --test live -- --nocapture
//! ```

use gcs_rust_fs::{ByteRange, ErrorKind, GcsFs, GcsPath, Transport};

const OBJECT_VAR: &str = "GCS_RUST_FS_TEST_OBJECT";

/// Returns the test object path, or `None` (after printing why) to skip.
fn test_path() -> Option<GcsPath> {
    match std::env::var(OBJECT_VAR) {
        Ok(value) if !value.is_empty() => Some(GcsPath::parse(&value).expect("valid test path")),
        _ => {
            eprintln!("skipping live test: {OBJECT_VAR} is not set");
            None
        }
    }
}

async fn fs() -> GcsFs {
    GcsFs::builder()
        .from_env()
        .expect("valid transport env")
        .build()
        .await
        .expect("client builds with ADC")
}

#[tokio::test]
async fn stat_reports_size_and_generation() {
    let Some(path) = test_path() else { return };
    let fs = fs().await;

    let stat = fs.stat(&path).await.expect("stat succeeds");
    assert_eq!(stat.bucket, path.bucket());
    assert_eq!(stat.name, path.object());
    assert!(stat.generation > 0);
    assert!(stat.updated.is_some());
    eprintln!("{stat:#?}");

    // Pinning the live generation must return the same object.
    let pinned = path.clone().with_generation(Some(stat.generation));
    let again = fs.stat(&pinned).await.expect("stat by generation");
    assert_eq!(again.generation, stat.generation);
    assert_eq!(again.size, stat.size);
}

#[tokio::test]
async fn cat_file_ranges_are_consistent_with_full_read() {
    let Some(path) = test_path() else { return };
    let fs = fs().await;
    eprintln!("transport: {}", fs.transport());

    let full = fs.cat_file(&path, ByteRange::ALL).await.expect("full read");
    let size = fs.stat(&path).await.unwrap().size;
    assert_eq!(
        full.len() as u64,
        size,
        "full read length matches stat size"
    );
    if full.is_empty() {
        eprintln!("test object is empty; range assertions are vacuous");
        return;
    }

    let n = full.len();
    let head = (n / 3).max(1);
    let mid_start = n / 3;
    let mid_end = (2 * n / 3).max(mid_start + 1);

    let cases: Vec<(ByteRange, &[u8])> = vec![
        (ByteRange::head(head as u64), &full[..head]),
        (
            ByteRange::span(mid_start as u64, mid_end as u64),
            &full[mid_start..mid_end],
        ),
        (ByteRange::from_offset(mid_start as u64), &full[mid_start..]),
        (ByteRange::tail(head as u64), &full[n - head..]),
        // Python-slice forms that need the size (extra stat under the hood).
        (ByteRange::new(Some(0), Some(-1)), &full[..n - 1]),
        (
            ByteRange::new(Some(-(head as i64)), Some(-1)),
            &full[n - head..n - 1],
        ),
        // Empty ranges never hit the network.
        (ByteRange::span(5, 5), &[]),
        (ByteRange::span(10, 5), &[]),
    ];
    for (range, expected) in cases {
        let got = fs
            .cat_file(&path, range)
            .await
            .unwrap_or_else(|e| panic!("{range:?}: {e}"));
        assert_eq!(&got[..], expected, "range {range:?}");
    }
}

#[tokio::test]
async fn open_file_pins_generation_and_clamps_reads() {
    let Some(path) = test_path() else { return };
    let fs = fs().await;

    let file = fs.open(&path).await.expect("open");
    assert_eq!(file.generation(), file.stat().generation);
    assert_eq!(file.path().generation(), Some(file.generation()));

    let all = file.read_range(ByteRange::ALL).await.expect("read all");
    assert_eq!(all.len() as u64, file.size());

    let size = file.size();
    if size > 0 {
        let first = file.read_range(ByteRange::head(1)).await.unwrap();
        assert_eq!(&first[..], &all[..1]);
        // Reading past the end is clamped, never an error.
        let past = file
            .read_range(ByteRange::span(size + 10, size + 20))
            .await
            .unwrap();
        assert!(past.is_empty());
        let overlap = file
            .read_range(ByteRange::span(size - 1, size + 100))
            .await
            .unwrap();
        assert_eq!(&overlap[..], &all[size as usize - 1..]);
    }
}

#[tokio::test]
async fn missing_object_is_not_found() {
    let Some(path) = test_path() else { return };
    let fs = fs().await;

    let missing = GcsPath::new(
        path.bucket(),
        format!("{}.does-not-exist-{}", path.object(), std::process::id()),
    )
    .unwrap();

    let err = fs
        .stat(&missing)
        .await
        .expect_err("stat of missing object fails");
    assert_eq!(err.kind(), ErrorKind::NotFound, "{err}");

    let err = fs
        .cat_file(&missing, ByteRange::ALL)
        .await
        .expect_err("read of missing object fails");
    assert_eq!(err.kind(), ErrorKind::NotFound, "{err}");

    // A bogus generation of an existing object is also NotFound.
    let err = fs
        .stat(&path.clone().with_generation(Some(1)))
        .await
        .expect_err("stat of bogus generation fails");
    assert_eq!(err.kind(), ErrorKind::NotFound, "{err}");
}

#[tokio::test]
async fn generation_suffix_in_path_string_is_honoured() {
    let Some(path) = test_path() else { return };
    let fs = fs().await;

    let stat = fs.stat(&path).await.expect("stat");
    let pinned: GcsPath = format!("{}#{}", path.relative(), stat.generation)
        .parse()
        .expect("path with #generation parses");
    let by_suffix = fs.stat(&pinned).await.expect("stat via #generation");
    assert_eq!(by_suffix.generation, stat.generation);
}

#[tokio::test]
async fn both_transports_return_identical_bytes() {
    let Some(path) = test_path() else { return };
    // Only meaningful when gRPC is actually enabled for the bucket; a
    // configuration-level failure on the gRPC side is reported, not asserted.
    let http = GcsFs::builder()
        .transport(Transport::Http)
        .build()
        .await
        .unwrap();
    let grpc = GcsFs::builder()
        .transport(Transport::Grpc)
        .build()
        .await
        .unwrap();

    let range = ByteRange::head(4096);
    let via_http = http.cat_file(&path, range).await.expect("http read");
    match grpc.cat_file(&path, range).await {
        Ok(via_grpc) => assert_eq!(via_http, via_grpc),
        Err(e) => eprintln!("gRPC bidi read unavailable for this bucket: {e}"),
    }
}
