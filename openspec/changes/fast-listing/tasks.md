# Tasks: Adaptive Fast Listing

## 1. Low-Level Control Primitives & Probing

- [ ] 1.1 Extend `ListRequest` in `src/gcs/control.rs` to support `lexicographic_start`, `lexicographic_end`, and `read_mask` projection, and verify with unit tests in `src/gcs/control.rs`.
- [ ] 1.2 Implement the single-page probe helper in `src/gcs/control.rs` that returns the initial page alongside workload classification signals (`prefixes`, `objects`, `next_page_token`), and verify with unit tests.

## 2. Streaming Filesystem Interface

- [ ] 2.1 Add `find_stream` and `ls_stream` methods returning `BoxStream<'static, Result<Entry>>` to the `FileSystem` trait in `src/filesystem.rs` with default implementations, and verify compilation across `GcsFs`, `CachedFs`, and `RemoteFs`.
- [ ] 2.2 Implement stream consumption and cancellation tests in `tests/memory_fs.rs` to verify proper stream semantics and early termination.

## 3. Hierarchical Concurrent Prefix Crawler

- [ ] 3.1 Implement the bounded work-stealing prefix crawler in `src/gcs/fs.rs` using `FuturesUnordered` bounded by `concurrency` (default 24) to traverse nested subdirectories concurrently.
- [ ] 3.2 Implement mock/unit tests verifying that hierarchical directories crawl concurrently and drain cleanly without unbounded task spawning or deadlock.

## 4. Massive Flat Prefix Range Partitioning

- [ ] 4.1 Implement dynamic key quantile derivation and concurrent lexicographical range partitioning (`[lexicographic_start, lexicographic_end)`) in `src/gcs/fs.rs` for flat prefixes exceeding 1,000 items.
- [ ] 4.2 Verify complete key coverage and absence of duplicate or omitted keys across range partition boundaries using unit tests in `src/gcs/fs.rs`.

## 5. Adaptive Orchestration & Integration Verification

- [ ] 5.1 Connect the adaptive dispatcher into `GcsFs::find`, `GcsFs::ls`, and their streaming counterparts, automatically routing between single-page leaf, concurrent tree crawler, and range partitioner.
- [ ] 5.2 Add end-to-end integration tests in `tests/` comparing adaptive listing results against traditional listings for correctness, deduplication, and ordering.
- [ ] 5.3 Update documentation and docstrings in `src/filesystem.rs` and `src/gcs/fs.rs` detailing streaming usage, concurrency configuration, and adaptive behavior.

