//! The derived `FileSystem` operations exercised against an in-memory object
//! store that implements only the six required primitives.
//!
//! This doubles as a check that the public contract is sufficient for an
//! implementation written outside the crate: everything below uses only
//! `gcs_rust_fs`'s public API.

mod common;

use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use common::{fixture, paths, text, MemoryFs};
use gcs_rust_fs::{
    BulkOptions, ByteRange, CopyOptions, DuOptions, ErrorKind, FileSystem, FindOptions,
    GlobOptions, ListOptions, MkdirOptions, OnError, OpenMode, OpenOptions, PutOptions,
    ReadOptions, RmOptions, WalkOptions, WriteMode, WriteOptions,
};

async fn files_below(fs: &MemoryFs, path: &str) -> Vec<String> {
    fs.find(path, FindOptions::default())
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect()
}

async fn glob_paths(fs: &MemoryFs, pattern: &str) -> Vec<String> {
    fs.glob(pattern, GlobOptions::default())
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect()
}

fn recursive() -> CopyOptions {
    CopyOptions {
        recursive: true,
        ..Default::default()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[tokio::test]
async fn info_exists_is_file_is_dir_size() {
    let fs = fixture();
    assert!(fs.exists("b").await.unwrap());
    assert!(fs.is_dir("b").await.unwrap());
    assert!(fs.is_dir("b/data").await.unwrap());
    assert!(fs.is_file("b/data/a.parquet").await.unwrap());
    assert!(!fs.is_file("b/data").await.unwrap());
    assert!(!fs.is_dir("b/data/a.parquet").await.unwrap());
    assert!(!fs.exists("b/nope").await.unwrap());
    assert!(!fs.exists("nobucket").await.unwrap());

    assert_eq!(fs.size("b/data/a.parquet").await.unwrap(), 4);
    assert_eq!(fs.size("b/data").await.unwrap(), 0);
    assert_eq!(
        fs.size("b/nope").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    // Paths are normalised: scheme, trailing slash, generation suffix.
    let info = fs.info("gs://b/data/a.parquet#1").await.unwrap();
    assert_eq!(info.path, "b/data/a.parquet");
    assert!(info.is_file());
    assert_eq!(info.stat.as_ref().unwrap().name, "data/a.parquet");
    assert_eq!(fs.info("b/data/").await.unwrap().path, "b/data");

    // The trait is usable through a trait object.
    let dynamic: Arc<dyn FileSystem> = Arc::new(fs);
    assert!(dynamic.exists("other/o.txt").await.unwrap());
}

#[tokio::test]
async fn find_walks_breadth_first_and_sorts() {
    let fs = fixture();
    assert_eq!(
        files_below(&fs, "b/data").await,
        [
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested/c.csv",
            "b/data/nested/deep/d.parquet",
        ]
    );

    let with_dirs = fs
        .find(
            "b/data",
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
            "b/data",
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested",
            "b/data/nested/c.csv",
            "b/data/nested/deep",
            "b/data/nested/deep/d.parquet",
        ]
    );

    let shallow = fs
        .find(
            "b/data",
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
            "b/data",
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested"
        ]
    );

    // A file finds itself; a missing path finds nothing.
    assert_eq!(files_below(&fs, "b/root.txt").await, ["b/root.txt"]);
    assert!(files_below(&fs, "b/missing").await.is_empty());

    // The root spans buckets and is never included itself.
    let everything = fs
        .find(
            "",
            FindOptions {
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(everything.iter().filter(|e| e.is_file()).count(), 7);
    assert!(everything.iter().all(|e| !e.path.is_empty()));

    assert_eq!(
        fs.find(
            "b",
            FindOptions {
                maxdepth: Some(0),
                ..Default::default()
            }
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::InvalidRange
    );
}

#[tokio::test]
async fn walk_groups_one_listing_per_directory() {
    let fs = fixture();
    let walk = fs.walk("b/data", WalkOptions::default()).await.unwrap();
    let dirs: Vec<&str> = walk.iter().map(|w| w.dir.as_str()).collect();
    assert_eq!(dirs, ["b/data", "b/data/nested", "b/data/nested/deep"]);
    assert_eq!(paths(&walk[0].dirs), ["b/data/nested"]);
    assert_eq!(
        paths(&walk[0].files),
        ["b/data/a.parquet", "b/data/b.parquet"]
    );
    assert_eq!(paths(&walk[1].dirs), ["b/data/nested/deep"]);
    assert_eq!(paths(&walk[1].files), ["b/data/nested/c.csv"]);
    assert!(walk[2].dirs.is_empty());
    assert_eq!(paths(&walk[2].files), ["b/data/nested/deep/d.parquet"]);

    let limited = fs
        .walk("b/data", WalkOptions { maxdepth: Some(1) })
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(paths(&limited[0].dirs), ["b/data/nested"]);

    assert!(fs
        .walk("b/root.txt", WalkOptions::default())
        .await
        .unwrap()
        .is_empty());
    assert!(fs
        .walk("b/missing", WalkOptions::default())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn du_sums_file_sizes() {
    let fs = fixture();
    let du = fs.du("b/data", DuOptions::default()).await.unwrap();
    assert_eq!(du.total, 4 + 2 + 1 + 6);
    assert_eq!(du.sizes.len(), 4);

    let with_dirs = fs
        .du(
            "b/data",
            DuOptions {
                withdirs: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(with_dirs.total, 13);
    assert_eq!(with_dirs.sizes.len(), 7);
    assert!(with_dirs.sizes.contains(&("b/data/nested".to_string(), 0)));
}

#[tokio::test]
async fn glob_follows_fsspec_rules() {
    let fs = fixture();
    assert_eq!(
        glob_paths(&fs, "b/data/*.parquet").await,
        ["b/data/a.parquet", "b/data/b.parquet"]
    );
    // `*` never crosses a separator; `**` may match zero levels.
    assert_eq!(
        glob_paths(&fs, "b/data/**/*.parquet").await,
        [
            "b/data/a.parquet",
            "b/data/b.parquet",
            "b/data/nested/deep/d.parquet",
        ]
    );
    assert_eq!(
        glob_paths(&fs, "b/*").await,
        ["b/data", "b/logs", "b/root.txt"]
    );
    // Trailing slash: directories only.
    assert_eq!(glob_paths(&fs, "b/*/").await, ["b/data", "b/logs"]);
    // A trailing `**` matches everything below, not the root itself.
    assert_eq!(glob_paths(&fs, "b/**").await.len(), 11);
    // Wildcard in the first segment searches across buckets.
    assert_eq!(glob_paths(&fs, "*/root.txt").await, ["b/root.txt"]);
    assert_eq!(
        glob_paths(&fs, "b/data/nested/?.csv").await,
        ["b/data/nested/c.csv"]
    );
    // No wildcard: the entry itself, if it exists.
    assert_eq!(
        glob_paths(&fs, "b/data/a.parquet").await,
        ["b/data/a.parquet"]
    );
    assert!(glob_paths(&fs, "b/data/zzz").await.is_empty());
    assert!(glob_paths(&fs, "b/data/a.parquet/").await.is_empty());
}

#[tokio::test]
async fn cat_and_cat_ranges_preserve_order_and_honour_on_error() {
    let fs = fixture();
    let out = fs
        .cat(&["b/data/b.parquet", "b/root.txt"], BulkOptions::default())
        .await
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, "b/data/b.parquet");
    assert_eq!(text(out[0].1.as_ref().unwrap()), "bb");
    assert_eq!(text(out[1].1.as_ref().unwrap()), "root");

    let err = fs
        .cat(&["b/root.txt", "b/missing"], BulkOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let ignored = fs
        .cat(
            &["b/missing", "b/root.txt"],
            BulkOptions {
                on_error: OnError::Ignore,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(ignored.len(), 1);
    assert_eq!(ignored[0].0, "b/root.txt");

    let returned = fs
        .cat(
            &["b/missing", "b/root.txt"],
            BulkOptions {
                on_error: OnError::Return,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(returned.len(), 2);
    assert_eq!(
        returned[0].1.as_ref().unwrap_err().kind(),
        ErrorKind::NotFound
    );

    let ranges = fs
        .cat_ranges(
            &[
                ("b/data/a.parquet", ByteRange::head(2)),
                ("b/data/nested/deep/d.parquet", ByteRange::tail(3)),
                ("b/missing", ByteRange::ALL),
                ("b/data/b.parquet", ByteRange::new(Some(-1), None)),
            ],
            BulkOptions {
                on_error: OnError::Return,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(text(ranges[0].as_ref().unwrap()), "aa");
    assert_eq!(text(ranges[1].as_ref().unwrap()), "ddd");
    assert_eq!(ranges[2].as_ref().unwrap_err().kind(), ErrorKind::NotFound);
    assert_eq!(text(ranges[3].as_ref().unwrap()), "b");

    assert_eq!(
        fs.cat_file("b/data", ByteRange::ALL, ReadOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );
}

#[tokio::test]
async fn file_handle_contract() {
    let fs = fixture();

    // Reading: cursor, seek, positional reads, EOF, closed state.
    let mut f = fs
        .open("b/data/nested/deep/d.parquet", OpenOptions::read())
        .await
        .unwrap();
    assert_eq!(f.mode(), OpenMode::Read);
    assert!(f.readable() && f.seekable() && !f.writable());
    assert_eq!(f.size(), Some(6));
    assert_eq!(f.tell(), 0);
    assert_eq!(text(&f.read(Some(2)).await.unwrap()), "dd");
    assert_eq!(f.tell(), 2);
    assert_eq!(f.seek(SeekFrom::End(-1)).unwrap(), 5);
    assert_eq!(f.read(None).await.unwrap().len(), 1);
    assert!(f.read(None).await.unwrap().is_empty());
    assert_eq!(
        text(&f.read_range(ByteRange::span(1, 3)).await.unwrap()),
        "dd"
    );
    assert_eq!(f.tell(), 6, "read_range must not move the cursor");
    assert_eq!(
        f.seek(SeekFrom::Current(-100)).unwrap_err().kind(),
        ErrorKind::InvalidRange
    );
    assert_eq!(
        f.write(Bytes::new()).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    f.close().await.unwrap();
    assert!(f.closed());
    assert_eq!(f.read(None).await.unwrap_err().kind(), ErrorKind::Closed);
    f.close().await.unwrap(); // idempotent

    // Writing: nothing is visible until close.
    let mut w = fs.open("b/w.txt", OpenOptions::write()).await.unwrap();
    assert!(w.writable() && !w.readable());
    w.write(Bytes::from_static(b"ab")).await.unwrap();
    w.write(Bytes::from_static(b"c")).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(w.tell(), 3);
    assert_eq!(w.size(), None);
    assert!(!fs.exists("b/w.txt").await.unwrap());
    assert_eq!(
        w.seek(SeekFrom::Start(0)).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    w.close().await.unwrap();
    assert_eq!(w.size(), Some(3));
    assert_eq!(w.stat().unwrap().size, 3);
    assert_eq!(
        text(
            &fs.cat_file("b/w.txt", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "abc"
    );

    // Discard publishes nothing and is idempotent.
    let mut d = fs.open("b/d.txt", OpenOptions::write()).await.unwrap();
    d.write(Bytes::from_static(b"zzz")).await.unwrap();
    d.discard().await.unwrap();
    assert!(d.closed());
    d.discard().await.unwrap();
    assert_eq!(
        d.write(Bytes::from_static(b"x")).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
    assert!(!fs.exists("b/d.txt").await.unwrap());

    // Dropping an unclosed write handle publishes nothing either.
    let mut d = fs.open("b/d2.txt", OpenOptions::write()).await.unwrap();
    d.write(Bytes::from_static(b"zzz")).await.unwrap();
    drop(d);
    assert!(!fs.exists("b/d2.txt").await.unwrap());

    // Mode errors.
    async fn open_err(fs: &MemoryFs, path: &str, opts: OpenOptions) -> ErrorKind {
        match fs.open(path, opts).await {
            Ok(_) => panic!("open({path}) unexpectedly succeeded"),
            Err(e) => e.kind(),
        }
    }
    assert_eq!(
        open_err(&fs, "b/a.txt", OpenOptions::with_mode(OpenMode::Append)).await,
        ErrorKind::Unsupported
    );
    assert_eq!(
        open_err(&fs, "b/data", OpenOptions::read()).await,
        ErrorKind::IsADirectory
    );
    assert_eq!(
        open_err(&fs, "b/missing", OpenOptions::read()).await,
        ErrorKind::NotFound
    );
    assert_eq!(
        open_err(&fs, "b/w.txt", OpenOptions::with_mode(OpenMode::CreateNew)).await,
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        open_err(&fs, "nobucket/x", OpenOptions::write()).await,
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn pipe_put_get_round_trip() {
    let fs = fixture();
    fs.pipe_file(
        "b/new/file.bin",
        Bytes::from_static(b"hello"),
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        text(
            &fs.cat_file("b/new/file.bin", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "hello"
    );
    assert!(fs.is_dir("b/new").await.unwrap());

    let err = fs
        .pipe_file(
            "b/new/file.bin",
            Bytes::new(),
            WriteOptions {
                mode: WriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    assert_eq!(
        fs.pipe_file("nobucket/x", Bytes::new(), WriteOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );

    // Local round trip through put / put_file / get_file.
    let dir = std::env::temp_dir().join(format!("gcs-rust-fs-test-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(dir.join("sub")).await.unwrap();
    tokio::fs::write(dir.join("one.txt"), b"1").await.unwrap();
    tokio::fs::write(dir.join("sub/two.txt"), b"22")
        .await
        .unwrap();
    let dir_name = dir.file_name().unwrap().to_str().unwrap().to_string();
    let put_recursive = || PutOptions {
        recursive: true,
        ..Default::default()
    };

    assert_eq!(
        fs.put(&dir, "b/upload", PutOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );

    // New destination: the tree lands at `dst`.
    fs.put(&dir, "b/upload", put_recursive()).await.unwrap();
    assert_eq!(
        files_below(&fs, "b/upload").await,
        ["b/upload/one.txt", "b/upload/sub/two.txt"]
    );
    assert_eq!(
        text(
            &fs.cat_file(
                "b/upload/sub/two.txt",
                ByteRange::ALL,
                ReadOptions::default()
            )
            .await
            .unwrap()
        ),
        "22"
    );

    // Trailing separator on the source: copy the *contents*.
    let with_sep = format!("{}{}", dir.display(), std::path::MAIN_SEPARATOR);
    fs.put(Path::new(&with_sep), "b/upload2/", put_recursive())
        .await
        .unwrap();
    assert_eq!(
        files_below(&fs, "b/upload2").await,
        ["b/upload2/one.txt", "b/upload2/sub/two.txt"]
    );

    // Existing destination directory, no trailing separator: nest under the
    // source's name.
    fs.put(&dir, "b/upload2", put_recursive()).await.unwrap();
    assert!(fs
        .exists(&format!("b/upload2/{dir_name}/sub/two.txt"))
        .await
        .unwrap());

    // Single files.
    fs.put_file(
        &dir.join("one.txt"),
        "b/single.txt",
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(fs.size("b/single.txt").await.unwrap(), 1);
    fs.put(&dir.join("one.txt"), "b/dest/", PutOptions::default())
        .await
        .unwrap();
    assert!(fs.is_file("b/dest/one.txt").await.unwrap());

    let target = dir.join("out/nested/got.txt");
    fs.get_file("b/upload/sub/two.txt", &target, ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&target).await.unwrap(), b"22");
    assert_eq!(
        fs.get_file("b/missing", &target, ReadOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );

    tokio::fs::remove_dir_all(&dir).await.unwrap();
}

#[tokio::test]
async fn rm_semantics() {
    let fs = fixture();
    assert_eq!(
        fs.rm("b/data", RmOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(
        fs.rm("b/missing", RmOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    fs.rm("b/root.txt", RmOptions::default()).await.unwrap();
    assert!(!fs.exists("b/root.txt").await.unwrap());

    fs.rm(
        "b/data",
        RmOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("b/data").await.unwrap());
    assert!(fs.exists("b/logs/2024/x.log").await.unwrap());

    // A bucket is removed along with its content.
    fs.rm(
        "other",
        RmOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("other").await.unwrap());
    assert_eq!(
        paths(&fs.ls("", ListOptions::default()).await.unwrap()),
        ["b"]
    );

    // maxdepth limits how deep the delete reaches.
    let fs = fixture();
    fs.rm(
        "b/data",
        RmOptions {
            recursive: true,
            maxdepth: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("b/data/a.parquet").await.unwrap());
    assert!(fs.exists("b/data/nested/c.csv").await.unwrap());
    assert!(fs.is_dir("b/data").await.unwrap());
}

#[tokio::test]
async fn mkdir_rmdir_and_placeholders() {
    let fs = MemoryFs::default();
    fs.mkdir("bkt", MkdirOptions::default()).await.unwrap();
    assert!(fs.is_dir("bkt").await.unwrap());
    assert_eq!(
        fs.mkdir("bkt", MkdirOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );

    // Nested directories are implicit; parents are only created on request.
    assert_eq!(
        fs.mkdir("nob/dir", MkdirOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    fs.mkdir(
        "nob/dir",
        MkdirOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(fs.is_dir("nob").await.unwrap());
    assert!(!fs.exists("nob/dir").await.unwrap());

    // A placeholder makes an empty directory exist.
    fs.mkdir(
        "bkt/empty",
        MkdirOptions {
            placeholder: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(fs.is_dir("bkt/empty").await.unwrap());
    assert!(fs.info("bkt/empty").await.unwrap().stat.is_some());
    assert!(fs
        .ls("bkt/empty", ListOptions::default())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        paths(&fs.ls("bkt", ListOptions::default()).await.unwrap()),
        ["bkt/empty"]
    );
    assert_eq!(
        fs.rmdir("bkt").await.unwrap_err().kind(),
        ErrorKind::DirectoryNotEmpty
    );

    fs.pipe_file(
        "bkt/f.txt",
        Bytes::from_static(b"f"),
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        fs.rmdir("bkt/f.txt").await.unwrap_err().kind(),
        ErrorKind::NotADirectory
    );
    fs.rm_file("bkt/f.txt").await.unwrap();

    fs.rm(
        "bkt/empty",
        RmOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!fs.exists("bkt/empty").await.unwrap());
    fs.rmdir("bkt").await.unwrap();
    assert!(!fs.exists("bkt").await.unwrap());
    assert_eq!(
        fs.rmdir("bkt").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn copy_destination_rules() {
    let fs = fixture();

    // File to a new name.
    fs.copy("b/root.txt", "b/copy.txt", CopyOptions::default())
        .await
        .unwrap();
    assert_eq!(
        text(
            &fs.cat_file("b/copy.txt", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "root"
    );
    // File into an existing directory, or into a `dst/`.
    fs.copy("b/root.txt", "b/logs", CopyOptions::default())
        .await
        .unwrap();
    assert!(fs.is_file("b/logs/root.txt").await.unwrap());
    fs.copy("b/root.txt", "b/fresh/", CopyOptions::default())
        .await
        .unwrap();
    assert!(fs.is_file("b/fresh/root.txt").await.unwrap());

    // Directories need `recursive`.
    assert_eq!(
        fs.copy("b/data", "b/dst", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );

    // Directory to a new destination: contents land at `dst/...`.
    fs.copy("b/data", "b/dst", recursive()).await.unwrap();
    assert_eq!(
        files_below(&fs, "b/dst").await,
        [
            "b/dst/a.parquet",
            "b/dst/b.parquet",
            "b/dst/nested/c.csv",
            "b/dst/nested/deep/d.parquet",
        ]
    );
    // Directory into an existing directory: nests under its own name ...
    fs.copy("b/data", "b/dst", recursive()).await.unwrap();
    assert!(fs.is_file("b/dst/data/a.parquet").await.unwrap());
    // ... unless the source has a trailing slash (copy the contents).
    fs.copy("b/data/", "b/logs", recursive()).await.unwrap();
    assert!(fs.is_file("b/logs/a.parquet").await.unwrap());
    assert!(!fs.exists("b/logs/data").await.unwrap());
    // Across buckets.
    fs.copy("b/data", "other/", recursive()).await.unwrap();
    assert!(fs.is_file("other/data/nested/c.csv").await.unwrap());
    // maxdepth.
    fs.copy(
        "b/data",
        "b/shallow",
        CopyOptions {
            maxdepth: Some(1),
            ..recursive()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        files_below(&fs, "b/shallow").await,
        ["b/shallow/a.parquet", "b/shallow/b.parquet"]
    );

    assert_eq!(
        fs.copy("b/missing", "b/x", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        fs.copy("b/root.txt", "nobucket/x", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn mv_moves_and_cleans_up_the_source() {
    let fs = fixture();
    fs.mv("b/root.txt", "b/moved.txt", CopyOptions::default())
        .await
        .unwrap();
    assert!(!fs.exists("b/root.txt").await.unwrap());
    assert_eq!(
        text(
            &fs.cat_file("b/moved.txt", ByteRange::ALL, ReadOptions::default())
                .await
                .unwrap()
        ),
        "root"
    );

    assert_eq!(
        fs.mv("b/data", "b/renamed", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::IsADirectory
    );
    fs.mv("b/data", "b/renamed", recursive()).await.unwrap();
    assert!(!fs.exists("b/data").await.unwrap());
    assert_eq!(
        files_below(&fs, "b/renamed").await,
        [
            "b/renamed/a.parquet",
            "b/renamed/b.parquet",
            "b/renamed/nested/c.csv",
            "b/renamed/nested/deep/d.parquet",
        ]
    );

    // Moving a whole bucket's content leaves the (now empty) bucket removed.
    fs.mv("other", "b/other-copy", recursive()).await.unwrap();
    assert!(!fs.exists("other").await.unwrap());
    assert!(fs.is_file("b/other-copy/o.txt").await.unwrap());

    assert_eq!(
        fs.mv("b/nope", "b/x", CopyOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}
