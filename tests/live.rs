//! Live integration tests against real buckets. Everything here self-skips
//! (returns early and reports *passed*) unless the relevant variable is set.
//!
//! * `GCS_RUST_FS_TEST_OBJECT=gs://bucket/file` — read-only tests against an
//!   existing object of a few KiB or more.
//! * `GCS_RUST_FS_TEST_BUCKETS=flat=b1,hns=b2,zonal=b3` — read/write scenarios,
//!   one run per listed bucket kind. Each run writes only below a unique
//!   `gcs-rust-fs-test/<run-id>/` prefix and removes it afterwards. Any
//!   subset of kinds may be given.
//!
//! `GCS_RUST_FS_TRANSPORT=http` switches non-zonal data reads to the JSON API.
//!
//! ```text
//! GCS_RUST_FS_TEST_OBJECT=gs://my-bucket/file.bin cargo test --test live -- --nocapture
//! GCS_RUST_FS_TEST_BUCKETS=flat=my-flat,hns=my-hns cargo test --test live -- --nocapture
//! ```

use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::OnceLock;

use bytes::Bytes;
use gcs_rust_fs::{
    BucketKind, ByteRange, CopyOptions, Entry, ErrorKind, FileSystem, FindOptions, GcsFs,
    ListOptions, MkdirOptions, OpenMode, OpenOptions, RmOptions, WalkOptions, WriteMode,
    WriteOptions,
};

const OBJECT_VAR: &str = "GCS_RUST_FS_TEST_OBJECT";
const BUCKETS_VAR: &str = "GCS_RUST_FS_TEST_BUCKETS";

fn env(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|v| !v.trim().is_empty())
}

/// The read-only test object as `bucket/key`, or `None` (after saying why).
fn test_object() -> Option<String> {
    let value = env(OBJECT_VAR);
    if value.is_none() {
        eprintln!("skipping live test: {OBJECT_VAR} is not set");
    }
    value
}

/// `(kind, bucket)` pairs from `GCS_RUST_FS_TEST_BUCKETS`, in the order given.
fn test_buckets() -> Vec<(BucketKind, String)> {
    let Some(value) = env(BUCKETS_VAR) else {
        eprintln!("skipping live test: {BUCKETS_VAR} is not set");
        return Vec::new();
    };
    value
        .split(',')
        .filter_map(|item| {
            let (kind, bucket) = item.split_once('=')?;
            let kind = match kind.trim() {
                "flat" => BucketKind::Flat,
                "hns" | "hierarchical" => BucketKind::Hierarchical,
                "zonal" | "rapid" => BucketKind::Zonal,
                other => panic!("{BUCKETS_VAR}: unknown bucket kind {other:?}"),
            };
            Some((kind, bucket.trim().to_owned()))
        })
        .collect()
}

async fn fs() -> GcsFs {
    GcsFs::builder()
        .from_env()
        .expect("valid env configuration")
        .build()
        .await
        .expect("client builds with ADC")
}

/// Unique per process so concurrent runs never share a prefix.
fn run_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{}-{nanos}", std::process::id())
    })
}

fn paths(entries: &[Entry]) -> Vec<&str> {
    entries.iter().map(|e| e.path.as_str()).collect()
}

fn kind_of(e: &Entry) -> &'static str {
    e.kind.as_str()
}

fn payload(len: usize) -> Bytes {
    (0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>().into()
}

// =========================================================================
// Read-only tests against an existing object
// =========================================================================

#[tokio::test]
async fn info_reports_file_metadata() {
    let Some(path) = test_object() else { return };
    let fs = fs().await;

    let entry = fs.info(&path).await.expect("info succeeds");
    assert!(entry.is_file());
    let stat = entry.stat.as_ref().expect("files carry metadata");
    assert!(stat.generation > 0);
    assert_eq!(entry.size, stat.size);
    assert_eq!(entry.path, stat.path());
    eprintln!("{entry:#?}");

    // The generation suffix pins the same object.
    let pinned = format!("{}#{}", entry.path, stat.generation);
    let again = fs.info(&pinned).await.expect("info by generation");
    assert_eq!(again.size, entry.size);

    assert!(fs.exists(&path).await.unwrap());
    assert!(fs.is_file(&path).await.unwrap());
    assert!(!fs.is_dir(&path).await.unwrap());
    assert_eq!(fs.size(&path).await.unwrap(), entry.size);

    // The parent is a directory and lists the file.
    let parent = entry.parent().expect("object has a parent").to_owned();
    assert!(fs.is_dir(&parent).await.unwrap());
    let listing = fs.ls(&parent, ListOptions::default()).await.unwrap();
    assert!(
        paths(&listing).contains(&entry.path.as_str()),
        "{listing:?}"
    );
}

#[tokio::test]
async fn cat_file_ranges_are_consistent_with_full_read() {
    let Some(path) = test_object() else { return };
    let fs = fs().await;
    eprintln!("transport: {}", fs.transport());

    let all = fs.cat_file(&path, ByteRange::ALL).await.expect("full read");
    let size = fs.size(&path).await.unwrap();
    assert_eq!(all.len() as u64, size);
    assert!(all.len() >= 16, "test object should be at least 16 bytes");

    let head = fs.cat_file(&path, ByteRange::head(10)).await.unwrap();
    assert_eq!(&head[..], &all[..10]);
    let tail = fs.cat_file(&path, ByteRange::tail(7)).await.unwrap();
    assert_eq!(&tail[..], &all[all.len() - 7..]);
    let span = fs.cat_file(&path, ByteRange::span(3, 12)).await.unwrap();
    assert_eq!(&span[..], &all[3..12]);
    let mixed = fs
        .cat_file(&path, ByteRange::new(Some(2), Some(-2)))
        .await
        .unwrap();
    assert_eq!(&mixed[..], &all[2..all.len() - 2]);

    // Python-slice semantics: past the end is empty, not an error.
    let beyond = fs
        .cat_file(&path, ByteRange::from_offset(size + 10))
        .await
        .unwrap();
    assert!(beyond.is_empty());
    let empty = fs.cat_file(&path, ByteRange::span(5, 5)).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn open_read_seek_and_positional_reads() {
    let Some(path) = test_object() else { return };
    let fs = fs().await;
    let all = fs.cat_file(&path, ByteRange::ALL).await.unwrap();

    let mut file = fs
        .open(
            &path,
            OpenOptions {
                block_size: Some(8),
                ..OpenOptions::read()
            },
        )
        .await
        .expect("open for reading");
    assert_eq!(file.mode(), OpenMode::Read);
    assert_eq!(file.size(), Some(all.len() as u64));

    let first = file.read(Some(5)).await.unwrap();
    assert_eq!(&first[..], &all[..5]);
    assert_eq!(file.tell(), 5);
    let next = file.read(Some(5)).await.unwrap(); // served from read-ahead
    assert_eq!(&next[..], &all[5..10]);
    assert_eq!(file.seek(SeekFrom::End(-4)).unwrap(), all.len() as u64 - 4);
    let last = file.read(None).await.unwrap();
    assert_eq!(&last[..], &all[all.len() - 4..]);
    assert!(file.read(Some(1)).await.unwrap().is_empty());

    let positional = file.read_range(ByteRange::span(1, 9)).await.unwrap();
    assert_eq!(&positional[..], &all[1..9]);
    assert_eq!(
        file.tell(),
        all.len() as u64,
        "read_range leaves the cursor"
    );

    file.close().await.unwrap();
    assert!(file.closed());
    assert_eq!(
        file.read(Some(1)).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
}

#[tokio::test]
async fn missing_paths_are_not_found() {
    let Some(path) = test_object() else { return };
    let fs = fs().await;
    let missing = format!("{path}.does-not-exist-{}", run_id());

    assert_eq!(
        fs.info(&missing).await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert!(!fs.exists(&missing).await.unwrap());
    assert_eq!(
        fs.cat_file(&missing, ByteRange::ALL)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    assert!(fs
        .find(&missing, FindOptions::default())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        fs.ls(&missing, ListOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

// =========================================================================
// Read/write scenarios, once per bucket kind
// =========================================================================

/// Runs `scenario` below a scratch prefix unique to this process, test and
/// bucket kind, and removes the test's whole tree afterwards even when the
/// scenario fails. Returns the scenario's panic instead of re-raising it so
/// callers can try every bucket kind before failing.
async fn with_scratch<F, Fut>(
    kind: BucketKind,
    bucket: &str,
    test: &str,
    scenario: F,
) -> std::result::Result<(), Box<dyn std::any::Any + Send>>
where
    F: FnOnce(GcsFs, String) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let fs = fs().await;
    let detected = fs.bucket_kind(bucket).await.expect("bucket kind");
    assert_eq!(detected, kind, "{bucket} is not a {kind} bucket");
    let tree = format!("{bucket}/gcs-rust-fs-test/{}-{test}", run_id());
    let root = format!("{tree}/{kind}");
    eprintln!("[{kind}] scratch prefix: {root}");

    let outcome = tokio::spawn({
        let (fs, root) = (fs.clone(), root.clone());
        async move { scenario(fs, root).await }
    })
    .await;

    let cleanup = fs
        .rm(
            &tree,
            RmOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await;
    match cleanup {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => eprintln!("[{kind}] cleanup of {tree} failed: {e}"),
    }
    let leftovers = fs
        .find(
            &tree,
            FindOptions {
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        leftovers.is_empty(),
        "[{kind}] scratch tree not empty after cleanup: {:?}",
        paths(&leftovers)
    );
    outcome.map_err(|e| e.into_panic())
}

async fn write_and_read_back(fs: GcsFs, root: String) {
    let file = format!("{root}/data/one.bin");
    let data = payload(70_000);
    fs.pipe_file(&file, data.clone(), WriteOptions::default())
        .await
        .expect("pipe_file");

    let info = fs.info(&file).await.unwrap();
    assert!(info.is_file());
    assert_eq!(info.size, data.len() as u64);
    assert!(fs.is_dir(&format!("{root}/data")).await.unwrap());
    assert!(fs.is_dir(&root).await.unwrap());

    assert_eq!(fs.cat_file(&file, ByteRange::ALL).await.unwrap(), data);
    assert_eq!(
        &fs.cat_file(&file, ByteRange::span(100, 200)).await.unwrap()[..],
        &data[100..200]
    );
    assert_eq!(
        &fs.cat_file(&file, ByteRange::tail(9)).await.unwrap()[..],
        &data[data.len() - 9..]
    );

    // create-only refuses to overwrite
    let err = fs
        .pipe_file(
            &file,
            Bytes::from_static(b"x"),
            WriteOptions {
                mode: WriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists, "{err}");

    // metadata round-trips
    let tagged = format!("{root}/data/tagged.txt");
    fs.pipe_file(
        &tagged,
        Bytes::from_static(b"hello"),
        WriteOptions {
            content_type: Some("text/plain".into()),
            metadata: HashMap::from([("k".to_string(), "v".to_string())]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let stat = fs.info(&tagged).await.unwrap().stat.unwrap();
    assert_eq!(stat.content_type, "text/plain");
    assert_eq!(stat.metadata.get("k").map(String::as_str), Some("v"));
}

async fn streaming_open_write_then_read(fs: GcsFs, kind: BucketKind, root: String) {
    let file = format!("{root}/stream/big.bin");
    let chunk = payload(1 << 20);
    let mut w = fs.open(&file, OpenOptions::write()).await.expect("open wb");
    assert!(w.writable() && !w.readable());
    for _ in 0..3 {
        w.write(chunk.clone()).await.unwrap();
    }
    assert_eq!(w.tell(), 3 << 20);
    w.flush().await.unwrap();
    w.close().await.unwrap();
    assert_eq!(w.size(), Some(3 << 20));
    assert!(w.closed());
    w.close().await.unwrap(); // idempotent

    let mut r = fs.open(&file, OpenOptions::read()).await.expect("open rb");
    assert_eq!(r.size(), Some(3 << 20));
    r.seek(SeekFrom::Start((1 << 20) + 10)).unwrap();
    let got = r.read(Some(20)).await.unwrap();
    assert_eq!(&got[..], &chunk[10..30]);
    assert_eq!(
        &r.read_range(ByteRange::span(5, 8)).await.unwrap()[..],
        &chunk[5..8]
    );
    r.close().await.unwrap();

    // A discarded resumable upload publishes nothing; an appendable object
    // exists from the moment it is opened (see the `File` docs).
    let ghost = format!("{root}/stream/ghost.bin");
    let mut w = fs.open(&ghost, OpenOptions::write()).await.unwrap();
    w.write(Bytes::from_static(b"never")).await.unwrap();
    w.discard().await.unwrap();
    assert!(w.closed());
    assert_eq!(
        fs.exists(&ghost).await.unwrap(),
        kind == BucketKind::Zonal,
        "discarded {kind} upload"
    );

    // put_file / get_file through a temp directory
    let dir = std::env::temp_dir().join(format!("gcs-rust-fs-{}", run_id()));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let local = dir.join("up.bin");
    tokio::fs::write(&local, &payload(12_345)).await.unwrap();
    let remote = format!("{root}/stream/up.bin");
    fs.put_file(&local, &remote, WriteOptions::default())
        .await
        .expect("put_file");
    assert_eq!(fs.size(&remote).await.unwrap(), 12_345);
    let back = dir.join("down.bin");
    fs.get_file(&remote, &back).await.expect("get_file");
    assert_eq!(tokio::fs::read(&back).await.unwrap(), payload(12_345));
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

async fn directories_listing_and_walk(fs: GcsFs, kind: BucketKind, root: String) {
    let tree = format!("{root}/tree");
    for name in ["a.txt", "sub/b.txt", "sub/deep/c.txt", "sub2/d.txt"] {
        fs.pipe_file(
            &format!("{tree}/{name}"),
            Bytes::from_static(b"1234"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    // ls: immediate children only, dirs without trailing slash
    let ls = fs.ls(&tree, ListOptions::default()).await.unwrap();
    assert_eq!(
        paths(&ls),
        [
            format!("{tree}/a.txt"),
            format!("{tree}/sub"),
            format!("{tree}/sub2")
        ]
    );
    assert_eq!(
        ls.iter().map(kind_of).collect::<Vec<_>>(),
        ["file", "directory", "directory"]
    );
    // ls on a file lists the file itself
    let single = fs
        .ls(&format!("{tree}/a.txt"), ListOptions::default())
        .await
        .unwrap();
    assert_eq!(paths(&single), [format!("{tree}/a.txt")]);

    // find: flat listing, sorted
    let files = fs.find(&tree, FindOptions::default()).await.unwrap();
    assert_eq!(
        paths(&files),
        [
            format!("{tree}/a.txt"),
            format!("{tree}/sub/b.txt"),
            format!("{tree}/sub/deep/c.txt"),
            format!("{tree}/sub2/d.txt"),
        ]
    );
    let with_dirs = fs
        .find(
            &tree,
            FindOptions {
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        paths(&with_dirs),
        [
            tree.clone(),
            format!("{tree}/a.txt"),
            format!("{tree}/sub"),
            format!("{tree}/sub/b.txt"),
            format!("{tree}/sub/deep"),
            format!("{tree}/sub/deep/c.txt"),
            format!("{tree}/sub2"),
            format!("{tree}/sub2/d.txt"),
        ]
    );
    let shallow = fs
        .find(
            &tree,
            FindOptions {
                maxdepth: Some(1),
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        paths(&shallow),
        [
            tree.clone(),
            format!("{tree}/a.txt"),
            format!("{tree}/sub"),
            format!("{tree}/sub2")
        ]
    );
    let on_file = fs
        .find(&format!("{tree}/a.txt"), FindOptions::default())
        .await
        .unwrap();
    assert_eq!(paths(&on_file), [format!("{tree}/a.txt")]);

    // walk / du / glob are derived from find
    let walk = fs.walk(&tree, WalkOptions::default()).await.unwrap();
    assert_eq!(
        walk.iter().map(|w| w.dir.as_str()).collect::<Vec<_>>(),
        [
            tree.as_str(),
            &format!("{tree}/sub"),
            &format!("{tree}/sub/deep"),
            &format!("{tree}/sub2")
        ]
    );
    assert_eq!(walk[0].files.len(), 1);
    assert_eq!(walk[0].dirs.len(), 2);
    assert_eq!(fs.du(&tree, Default::default()).await.unwrap().total, 16);
    let globbed = fs
        .glob(&format!("{tree}/**/*.txt"), Default::default())
        .await
        .unwrap();
    assert_eq!(globbed.len(), 4);
    let sub_only = fs
        .glob(&format!("{tree}/sub*/"), Default::default())
        .await
        .unwrap();
    assert_eq!(
        paths(&sub_only),
        [format!("{tree}/sub"), format!("{tree}/sub2")]
    );

    // mkdir / rmdir of an empty directory
    let empty = format!("{root}/emptydir");
    let mkdir_opts = MkdirOptions {
        // Flat buckets need a placeholder for an empty directory to exist.
        placeholder: kind == BucketKind::Flat,
        ..Default::default()
    };
    fs.mkdir(&empty, mkdir_opts.clone()).await.expect("mkdir");
    assert!(fs.is_dir(&empty).await.unwrap(), "empty directory exists");
    assert!(fs
        .ls(&empty, ListOptions::default())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        fs.mkdir(&empty, mkdir_opts).await.unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    let listed = fs.ls(&root, ListOptions::default()).await.unwrap();
    assert!(paths(&listed).contains(&empty.as_str()), "{listed:?}");
    // the empty directory shows up in find(withdirs) too
    let found = fs
        .find(
            &root,
            FindOptions {
                withdirs: true,
                maxdepth: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(paths(&found).contains(&empty.as_str()), "{found:?}");
    assert_eq!(
        fs.rmdir(&tree).await.unwrap_err().kind(),
        ErrorKind::DirectoryNotEmpty
    );
    assert_eq!(
        fs.rmdir(&format!("{tree}/a.txt")).await.unwrap_err().kind(),
        ErrorKind::NotADirectory
    );
    fs.rmdir(&empty).await.expect("rmdir");
    assert!(!fs.exists(&empty).await.unwrap());
    assert_eq!(
        fs.rmdir(&empty).await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    // file-vs-directory errors
    assert_eq!(
        fs.cat_file(&tree, ByteRange::ALL).await.unwrap_err().kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(
        fs.rm_file(&tree).await.unwrap_err().kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(
        fs.rm(&tree, RmOptions::default()).await.unwrap_err().kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(
        fs.rm_file(&format!("{tree}/nope"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

async fn copy_move_and_remove(fs: GcsFs, kind: BucketKind, root: String) {
    let src = format!("{root}/mv/src");
    for name in ["x.txt", "d/y.txt"] {
        fs.pipe_file(
            &format!("{src}/{name}"),
            Bytes::from_static(b"abc"),
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    // copy_file: server-side, or Unsupported on zonal buckets
    let copy = fs
        .copy_file(&format!("{src}/x.txt"), &format!("{root}/mv/copy.txt"))
        .await;
    if kind == BucketKind::Zonal {
        assert_eq!(copy.unwrap_err().kind(), ErrorKind::Unsupported);
    } else {
        copy.expect("copy_file");
        assert_eq!(fs.size(&format!("{root}/mv/copy.txt")).await.unwrap(), 3);
        fs.copy(
            &src,
            &format!("{root}/mv/copied/"),
            CopyOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
        .expect("copy -r");
        let copied = fs
            .find(&format!("{root}/mv/copied"), FindOptions::default())
            .await
            .unwrap();
        assert_eq!(
            paths(&copied),
            [
                format!("{root}/mv/copied/src/d/y.txt"),
                format!("{root}/mv/copied/src/x.txt")
            ]
        );
    }

    // move_file: atomic rename within the bucket
    fs.move_file(&format!("{src}/x.txt"), &format!("{src}/x2.txt"))
        .await
        .expect("move_file");
    assert!(!fs.exists(&format!("{src}/x.txt")).await.unwrap());
    assert_eq!(fs.size(&format!("{src}/x2.txt")).await.unwrap(), 3);
    assert_eq!(
        fs.move_file(&format!("{src}/x.txt"), &format!("{src}/x3.txt"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );

    // mv -r of a directory (HNS: RenameFolder; flat: per-object moves)
    let dst = format!("{root}/mv/dst");
    fs.mv(
        &src,
        &dst,
        CopyOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .expect("mv -r");
    let moved = fs.find(&dst, FindOptions::default()).await.unwrap();
    assert_eq!(
        paths(&moved),
        [format!("{dst}/d/y.txt"), format!("{dst}/x2.txt")]
    );
    assert!(
        fs.find(&src, FindOptions::default())
            .await
            .unwrap()
            .is_empty(),
        "source tree is gone"
    );
    if kind.is_hierarchical() {
        assert!(!fs.exists(&src).await.unwrap(), "source folder is gone");
    }

    // rm -r removes everything below and the directory itself
    fs.rm(
        &format!("{root}/mv"),
        RmOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .expect("rm -r");
    assert!(!fs.exists(&format!("{root}/mv")).await.unwrap());
    assert_eq!(
        fs.rm(&format!("{root}/mv"), RmOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

async fn append_mode(fs: GcsFs, kind: BucketKind, root: String) {
    let file = format!("{root}/append/log.txt");
    if kind != BucketKind::Zonal {
        fs.pipe_file(&file, Bytes::from_static(b"x"), WriteOptions::default())
            .await
            .unwrap();
        let err = fs
            .open(&file, OpenOptions::with_mode(OpenMode::Append))
            .await
            .err()
            .expect("append is unsupported on immutable objects");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        return;
    }

    // Zonal: flush makes bytes visible before close; append reopens.
    let mut w = fs.open(&file, OpenOptions::write()).await.expect("open wb");
    w.write(Bytes::from_static(b"hello ")).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(
        fs.cat_file(&file, ByteRange::ALL).await.unwrap(),
        Bytes::from_static(b"hello ")
    );
    w.write(Bytes::from_static(b"world")).await.unwrap();
    w.close().await.unwrap();
    assert_eq!(w.tell(), 11);
    assert_eq!(fs.size(&file).await.unwrap(), 11);

    let mut a = fs
        .open(&file, OpenOptions::with_mode(OpenMode::Append))
        .await
        .expect("open ab");
    assert_eq!(a.tell(), 11, "append starts at the persisted size");
    a.write(Bytes::from_static(b"!")).await.unwrap();
    a.close().await.unwrap();
    assert_eq!(
        fs.cat_file(&file, ByteRange::ALL).await.unwrap(),
        Bytes::from_static(b"hello world!")
    );
    // "ab" on a missing object creates it
    let fresh = format!("{root}/append/fresh.txt");
    let mut a = fs
        .open(&fresh, OpenOptions::with_mode(OpenMode::Append))
        .await
        .unwrap();
    a.write(Bytes::from_static(b"new")).await.unwrap();
    a.close().await.unwrap();
    assert_eq!(fs.size(&fresh).await.unwrap(), 3);
}

macro_rules! per_kind {
    ($name:ident, |$fs:ident, $kind:ident, $root:ident| $body:expr) => {
        #[tokio::test]
        async fn $name() {
            let mut failed = Vec::new();
            for ($kind, bucket) in test_buckets() {
                let outcome = with_scratch(
                    $kind,
                    &bucket,
                    stringify!($name),
                    move |$fs, $root| async move { $body.await },
                )
                .await;
                if let Err(panic) = outcome {
                    failed.push(($kind, panic));
                }
            }
            if !failed.is_empty() {
                let kinds: Vec<_> = failed.iter().map(|(kind, _)| kind.to_string()).collect();
                eprintln!(
                    "{} failed for bucket kinds: {}",
                    stringify!($name),
                    kinds.join(", ")
                );
                std::panic::resume_unwind(failed.swap_remove(0).1);
            }
        }
    };
}

per_kind!(rw_write_and_read_back, |fs, kind, root| {
    let _ = kind;
    write_and_read_back(fs, root)
});
per_kind!(rw_streaming_open_write_then_read, |fs, kind, root| {
    streaming_open_write_then_read(fs, kind, root)
});
per_kind!(rw_directories_listing_and_walk, |fs, kind, root| {
    directories_listing_and_walk(fs, kind, root)
});
per_kind!(rw_copy_move_and_remove, |fs, kind, root| {
    copy_move_and_remove(fs, kind, root)
});
per_kind!(rw_append_mode, |fs, kind, root| append_mode(fs, kind, root));
