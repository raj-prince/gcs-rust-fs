# Tasks: Parallel Composite Upload

## 1. Write Options & Partitioning Logic

- [x] 1.1 Add `chunk_size`, `concurrency`, and `composite_threshold` optional fields to `WriteOptions` in `src/options.rs`, and verify with unit tests in `src/options.rs`.
- [x] 1.2 Implement the balanced component slicing helper in `src/gcs/write.rs` that divides files into up to 32 equal slices (e.g. 4 parts of 50 MiB for 200 MiB), and verify slice math with unit tests.

## 2. Positional Sliced File Streaming

- [x] 2.1 Implement `FileSliceSource` implementing `StreamingSource` in `src/gcs/write.rs` to stream positional file slices in bounded 2 MiB chunks without buffering full slices in memory.
- [x] 2.2 Implement unit tests in `src/gcs/write.rs` verifying that `FileSliceSource` yields the exact byte sequence for arbitrary offsets and lengths.

## 3. Server-Side Composition & Cleanup Lifecycle

- [x] 3.1 Add `compose_objects` in `src/gcs/control.rs` utilizing `StorageControl::compose_object` with generation match precondition enforcement and temporary part cleanup.
- [x] 3.2 Implement `CompositeUploadGuard` in `src/gcs/write.rs` that tracks temporary component names and ensures parallel deletion of written parts upon upload failure or cancellation.

## 4. Engine Wiring & Verification

- [x] 4.1 Implement `upload_composite_file` in `src/gcs/write.rs` managing concurrent worker uploads via `FuturesUnordered` and calling `compose_objects`.
- [x] 4.2 Connect composite upload into `put_file_impl` in `src/gcs/fs.rs` for files meeting the 64 MiB threshold on composite-capable buckets, preserving direct upload fallback for smaller files and zonal buckets.
- [x] 4.3 Add comprehensive integration tests in `tests/` verifying composite upload integrity, CRC32C validation, `CreateNew` race prevention, and small file fallback.
- [x] 4.4 Record `put_file_composite` operation and transfer byte metrics via `MetricsRecorder`, and update user documentation in `docs/` and `README.md`.

