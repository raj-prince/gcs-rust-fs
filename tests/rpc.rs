//! End-to-end integration tests for [`FileSystemRpcService`] and [`RemoteFs`]
//! over a Unix Domain Socket (the sidecar container transport) and TCP.

mod common;

use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use common::MemoryFs;
use gcs_rust_fs::{
    ByteRange, CacheConfig, CachedFs, DataConfig, EntryKind, ErrorKind, FileSystem,
    FileSystemRpcService, ListOptions, OpenOptions, PageCacheConfig, ReadOptions, ReadaheadConfig,
    RemoteFs, WriteMode, WriteOptions,
};
use tokio::sync::oneshot;

static SOCKET_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Spin up a [`FileSystemRpcService`] listening on a temporary Unix Domain
/// Socket and return a connected [`RemoteFs`] client plus a guard that shuts
/// down the server and cleans up the socket file on drop.
async fn start_uds_sidecar(
    files: &[(&str, &str)],
) -> (RemoteFs, Arc<MemoryFs>, UdsServerGuard) {
    let mem = Arc::new(MemoryFs::with_files(files));
    let id = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sock_path = std::env::temp_dir().join(format!(
        "gcs-rust-fs-test-{}-{id}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&sock_path);

    let (tx, rx) = oneshot::channel::<()>();
    let service = FileSystemRpcService::new(mem.clone());
    let server_sock = sock_path.clone();
    let handle = tokio::spawn(async move {
        let _ = service
            .serve_uds_with_shutdown(&server_sock, async {
                let _ = rx.await;
            })
            .await;
    });

    // Wait briefly until the socket file appears and accepts connections.
    for _ in 0..50 {
        if sock_path.exists() {
            if let Ok(client) = RemoteFs::connect_uds(&sock_path).await {
                return (
                    client,
                    mem,
                    UdsServerGuard {
                        shutdown: Some(tx),
                        handle,
                        sock_path,
                    },
                );
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!(
        "timed out waiting for UDS sidecar server at {}",
        sock_path.display()
    );
}

struct UdsServerGuard {
    shutdown: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
    sock_path: PathBuf,
}

impl Drop for UdsServerGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
        let _ = std::fs::remove_file(&self.sock_path);
    }
}

#[tokio::test]
async fn info_returns_file_and_directory_entries_over_uds() {
    // Arrange
    let (remote, _mem, _guard) =
        start_uds_sidecar(&[("my-bucket/dir/hello.txt", "hello sidecar")]).await;

    // Act
    let file_entry = remote.info("gs://my-bucket/dir/hello.txt").await.unwrap();
    let dir_entry = remote.info("my-bucket/dir").await.unwrap();

    // Assert
    assert_eq!(file_entry.path, "my-bucket/dir/hello.txt");
    assert_eq!(file_entry.kind, EntryKind::File);
    assert_eq!(file_entry.size, 13);
    let stat = file_entry.stat.expect("file entry must carry ObjectStat");
    assert_eq!(stat.bucket, "my-bucket");
    assert_eq!(stat.name, "dir/hello.txt");

    assert_eq!(dir_entry.path, "my-bucket/dir");
    assert_eq!(dir_entry.kind, EntryKind::Directory);
    assert_eq!(dir_entry.size, 0);
}

#[tokio::test]
async fn info_missing_path_preserves_not_found_error_kind() {
    // Arrange
    let (remote, _mem, _guard) = start_uds_sidecar(&[("my-bucket/a.txt", "a")]).await;

    // Act
    let err = remote.info("my-bucket/missing.txt").await.unwrap_err();

    // Assert
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(err.is_not_found());
}

#[tokio::test]
async fn read_whole_and_ranged_slices_over_uds() {
    // Arrange
    let (remote, _mem, _guard) =
        start_uds_sidecar(&[("my-bucket/alphabet.txt", "abcdefghijklmnopqrstuvwxyz")]).await;

    // Act
    let whole = remote
        .cat_file(
            "my-bucket/alphabet.txt",
            ByteRange::ALL,
            ReadOptions::default(),
        )
        .await
        .unwrap();
    let head = remote
        .cat_file(
            "my-bucket/alphabet.txt",
            ByteRange::head(5),
            ReadOptions::default(),
        )
        .await
        .unwrap();
    let span = remote
        .cat_file(
            "my-bucket/alphabet.txt",
            ByteRange::span(10, 15),
            ReadOptions::default(),
        )
        .await
        .unwrap();
    let tail = remote
        .cat_file(
            "my-bucket/alphabet.txt",
            ByteRange::tail(4),
            ReadOptions::default(),
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(whole, Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz"));
    assert_eq!(head, Bytes::from_static(b"abcde"));
    assert_eq!(span, Bytes::from_static(b"klmno"));
    assert_eq!(tail, Bytes::from_static(b"wxyz"));
}

#[tokio::test]
async fn read_multi_megabyte_payload_streams_in_chunks_over_uds() {
    // Arrange: 3.5 MiB payload so it spans multiple 1 MiB gRPC stream chunks.
    let size = 3 * 1024 * 1024 + 512 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let (remote, _mem, _guard) = start_uds_sidecar(&[("my-bucket/seed", "x")]).await;
    remote
        .pipe_file(
            "my-bucket/large.bin",
            Bytes::from(payload.clone()),
            WriteOptions::default(),
        )
        .await
        .unwrap();

    // Act
    let round_tripped = remote
        .cat_file(
            "my-bucket/large.bin",
            ByteRange::ALL,
            ReadOptions::default(),
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(round_tripped.len(), payload.len());
    assert_eq!(&round_tripped[..], &payload[..]);
}

#[tokio::test]
async fn open_read_handle_supports_seek_and_positional_read_range() {
    // Arrange
    let (remote, _mem, _guard) =
        start_uds_sidecar(&[("my-bucket/data.bin", "0123456789abcdef")]).await;

    // Act
    let mut f = remote
        .open("my-bucket/data.bin", OpenOptions::read())
        .await
        .unwrap();
    let first_four = f.read(Some(4)).await.unwrap();
    let pos_after_first = f.tell();
    let pread_slice = f.read_range(ByteRange::span(10, 16)).await.unwrap();
    let pos_after_pread = f.tell();
    let new_pos = f.seek(SeekFrom::End(-6)).unwrap();
    let tail = f.read(None).await.unwrap();
    f.close().await.unwrap();

    // Assert
    assert_eq!(first_four, Bytes::from_static(b"0123"));
    assert_eq!(pos_after_first, 4);
    assert_eq!(pread_slice, Bytes::from_static(b"abcdef"));
    assert_eq!(pos_after_pread, 4);
    assert_eq!(new_pos, 10);
    assert_eq!(tail, Bytes::from_static(b"abcdef"));
}

#[tokio::test]
async fn open_write_handle_publishes_only_on_close_and_discards_when_abandoned() {
    // Arrange
    let (remote, _mem, _guard) = start_uds_sidecar(&[("my-bucket/seed", "1")]).await;

    // Act 1: Write chunks and discard before close -> file must not exist.
    let mut discarded = remote
        .open("my-bucket/aborted.txt", OpenOptions::write())
        .await
        .unwrap();
    discarded
        .write(Bytes::from_static(b"partial "))
        .await
        .unwrap();
    discarded.discard().await.unwrap();

    // Act 2: Write chunks and close -> file is published atomically.
    let mut committed = remote
        .open("my-bucket/committed.txt", OpenOptions::write())
        .await
        .unwrap();
    committed
        .write(Bytes::from_static(b"hello "))
        .await
        .unwrap();
    committed
        .write(Bytes::from_static(b"world"))
        .await
        .unwrap();
    committed.close().await.unwrap();

    // Assert
    assert_eq!(
        remote.info("my-bucket/aborted.txt").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(committed.size(), Some(11));
    let data = remote
        .cat_file(
            "my-bucket/committed.txt",
            ByteRange::ALL,
            ReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(data, Bytes::from_static(b"hello world"));
}

#[tokio::test]
async fn write_create_mode_fails_with_already_exists_over_uds() {
    // Arrange
    let (remote, _mem, _guard) =
        start_uds_sidecar(&[("my-bucket/existing.txt", "original")]).await;

    // Act
    let err = remote
        .pipe_file(
            "my-bucket/existing.txt",
            Bytes::from_static(b"new"),
            WriteOptions {
                mode: WriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

    // Assert
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
}

#[tokio::test]
async fn ls_lists_children_over_uds() {
    // Arrange
    let (remote, _mem, _guard) = start_uds_sidecar(&[
        ("my-bucket/a.txt", "alpha"),
        ("my-bucket/b.txt", "beta"),
        ("my-bucket/subdir/c.txt", "gamma"),
    ])
    .await;

    // Act
    let entries = remote
        .ls("my-bucket", ListOptions::default())
        .await
        .unwrap();

    // Assert
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    assert!(paths.contains(&"my-bucket/a.txt"));
    assert!(paths.contains(&"my-bucket/b.txt"));
    assert!(paths.contains(&"my-bucket/subdir"));
}

#[tokio::test]
async fn concurrent_clients_stream_over_uds() {
    // Arrange
    let payload = "A".repeat(2 * 1024 * 1024);
    let (remote, _mem, _guard) =
        start_uds_sidecar(&[("my-bucket/large.bin", &payload)]).await;

    // Act: run multiple concurrent reads across cloned RemoteFs handles
    let mut handles = Vec::new();
    for _ in 0..8 {
        let client = remote.clone();
        handles.push(tokio::spawn(async move {
            client
                .cat_file(
                    "my-bucket/large.bin",
                    ByteRange::ALL,
                    ReadOptions::default(),
                )
                .await
                .unwrap()
        }));
    }

    // Assert
    for h in handles {
        let res = h.await.unwrap();
        assert_eq!(res.len(), 2 * 1024 * 1024);
    }
}

#[tokio::test]
async fn multi_uds_served_simultaneously_by_one_server() {
    // Arrange: Create two separate socket paths
    let mem = Arc::new(MemoryFs::with_files(&[
        ("my-bucket/msg1.txt", "hello socket 1"),
        ("my-bucket/msg2.txt", "hello socket 2"),
    ]));
    let id1 = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let id2 = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sock1 = std::env::temp_dir().join(format!(
        "gcs-rust-fs-test-multi1-{}-{id1}.sock",
        std::process::id()
    ));
    let sock2 = std::env::temp_dir().join(format!(
        "gcs-rust-fs-test-multi2-{}-{id2}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&sock1);
    let _ = std::fs::remove_file(&sock2);

    let (tx, rx) = oneshot::channel::<()>();
    let service = FileSystemRpcService::new(mem);
    let paths = vec![sock1.clone(), sock2.clone()];
    let handle = tokio::spawn(async move {
        let _ = service
            .serve_multiple_uds_with_mode_and_shutdown(paths, Some(0o666), async {
                let _ = rx.await;
            })
            .await;
    });

    // Wait until both sockets accept connections
    let mut client1 = None;
    let mut client2 = None;
    for _ in 0..50 {
        if sock1.exists() && sock2.exists() {
            if client1.is_none() {
                client1 = RemoteFs::connect_uds(&sock1).await.ok();
            }
            if client2.is_none() {
                client2 = RemoteFs::connect_uds(&sock2).await.ok();
            }
            if client1.is_some() && client2.is_some() {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let client1 = client1.expect("failed to connect to socket 1");
    let client2 = client2.expect("failed to connect to socket 2");

    // Act: stream from client1 over sock1 and client2 over sock2
    let res1 = client1
        .cat_file("my-bucket/msg1.txt", ByteRange::ALL, ReadOptions::default())
        .await
        .unwrap();
    let res2 = client2
        .cat_file("my-bucket/msg2.txt", ByteRange::ALL, ReadOptions::default())
        .await
        .unwrap();

    // Assert
    assert_eq!(res1, Bytes::from_static(b"hello socket 1"));
    assert_eq!(res2, Bytes::from_static(b"hello socket 2"));

    // Cleanup
    let _ = tx.send(());
    let _ = handle.await;
    let _ = std::fs::remove_file(&sock1);
    let _ = std::fs::remove_file(&sock2);
}

#[tokio::test]
async fn cached_readahead_sidecar_serves_bounded_random_and_large_sequential_reads() {
    // Arrange: 20 MiB deterministic payload served through CachedFs + ReadaheadConfig
    let size = 20 * 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| ((i * 31 + 7) % 251) as u8).collect();
    let mem = MemoryFs::with_files(&[("my-bucket/seed", "x")]);
    mem.pipe_file(
        "my-bucket/seq20m.bin",
        Bytes::from(payload.clone()),
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let cfg = CacheConfig {
        data: Some(DataConfig {
            readahead: ReadaheadConfig {
                page_size: 256 * 1024,
                io_size: 4 * 1024 * 1024,
                max_folio_size: 4 * 1024 * 1024,
                max_readahead: 16 * 1024 * 1024,
                max_inflight: 4,
                max_inflight_bytes: 32 * 1024 * 1024,
            },
            cache: Some(PageCacheConfig {
                capacity: 32 * 1024 * 1024,
            }),
        }),
        ..Default::default()
    };
    let cached_fs: Arc<dyn FileSystem> = Arc::new(CachedFs::new(mem, cfg));

    let id = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let sock_path = std::env::temp_dir().join(format!(
        "gcs-rust-fs-test-ra-{}-{id}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&sock_path);

    let (tx, rx) = oneshot::channel::<()>();
    let service = FileSystemRpcService::new(cached_fs);
    let server_sock = sock_path.clone();
    let handle = tokio::spawn(async move {
        let _ = service
            .serve_uds_with_shutdown(&server_sock, async {
                let _ = rx.await;
            })
            .await;
    });

    let mut client = None;
    for _ in 0..50 {
        if sock_path.exists() {
            if let Ok(c) = RemoteFs::connect_uds(&sock_path).await {
                client = Some(c);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let client = client.expect("failed to connect to cached readahead UDS server");

    // Act 1: 1 MiB bounded random read (`span <= chunk_size` fast path)
    let start_1m = 3 * 1024 * 1024;
    let end_1m = 4 * 1024 * 1024;
    let slice_1m = client
        .cat_file(
            "my-bucket/seq20m.bin",
            ByteRange::span(start_1m as u64, end_1m as u64),
            ReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(slice_1m.as_ref(), &payload[start_1m..end_1m]);

    // Act 2: >= 16 MiB sequential read (`ReadPattern::Sequential` streaming path)
    let start_16m = 2 * 1024 * 1024;
    let end_16m = 18 * 1024 * 1024;
    let slice_16m = client
        .cat_file(
            "my-bucket/seq20m.bin",
            ByteRange::span(start_16m as u64, end_16m as u64),
            ReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(slice_16m.as_ref(), &payload[start_16m..end_16m]);

    // Act 3: RemoteFile sequential streaming across multiple read() calls + random seek()
    let mut rfile = client
        .open("my-bucket/seq20m.bin", OpenOptions::read())
        .await
        .unwrap();
    let chunk_a = rfile.read(Some(4 * 1024 * 1024)).await.unwrap();
    let chunk_b = rfile.read(Some(4 * 1024 * 1024)).await.unwrap();
    assert_eq!(chunk_a.as_ref(), &payload[0..4 * 1024 * 1024]);
    assert_eq!(
        chunk_b.as_ref(),
        &payload[4 * 1024 * 1024..8 * 1024 * 1024]
    );
    rfile.seek(SeekFrom::Start(12 * 1024 * 1024)).unwrap();
    let random_chunk = rfile.read(Some(1024 * 1024)).await.unwrap();
    assert_eq!(
        random_chunk.as_ref(),
        &payload[12 * 1024 * 1024..13 * 1024 * 1024]
    );
    rfile.close().await.unwrap();

    // Cleanup
    let _ = tx.send(());
    let _ = handle.await;
    let _ = std::fs::remove_file(&sock_path);
}

