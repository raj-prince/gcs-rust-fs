# Proposal: Adaptive Fast Listing

## Why

Listing keys and directories in Google Cloud Storage currently suffers from two major bottlenecks: serial 1,000-object page token pagination and sequential breadth-first traversal of directory trees. For hierarchical buckets with deep nesting or large flat prefixes with hundreds of thousands of objects, operations like `find`, `ls`, and `walk` take tens of seconds and buffer massive arrays into memory before returning.

An adaptive, zero-configuration listing engine is needed to automatically sense the directory topology (small leaf, deeply nested tree, or massive flat prefix) on the first round-trip and dynamically apply the optimal concurrency and partitioning strategy.

## What Changes

- **Adaptive Workload Detection**: Fingerprint directory structure from the first page response (count of objects, prefixes, and continuation tokens) to select the optimal execution path with zero user configuration.
- **Hierarchical Concurrent Crawler**: Replace serial directory traversal with a dynamic, bounded work-stealing prefix pool (16–32 parallel workers) when nested folders or common prefixes are discovered.
- **Massive Flat Prefix Acceleration**: Break the serial page token chain for large flat prefixes using GCS lexicographical partition sharding (`lexicographic_start` and `lexicographic_end`) and pipelined page prefetching.
- **Streaming Entry API**: Add `find_stream` and `ls_stream` streaming primitives (`impl Stream<Item = Result<Entry>>`) to enable immediate consumption of entries as pages arrive with $O(\text{concurrency} \times \text{page\_size})$ memory footprint.
- **Metadata Payload Trimming**: Apply `read_mask` projections in GCS `ListObjectsRequest` during fast listing to avoid fetching unnecessary metadata fields over the wire.
- **Backward Compatibility**: Keep existing `find`, `ls`, and `walk` APIs fully functional and signature-compatible by having them collect from the adaptive streaming engine.

## Capabilities

### New Capabilities
- `filesystem/listing`: Covers adaptive directory inspection, concurrent hierarchical prefix traversal, lexicographical range partitioning, streaming entry iteration, and metadata projection for Cloud Storage.

### Modified Capabilities
*(None)*

## Impact

- **Affected Code**: `src/filesystem.rs`, `src/gcs/fs.rs`, `src/gcs/control.rs`, `src/gcs/backend.rs`, `src/derived.rs`.
- **API Surface**: Adds default methods `find_stream` and `ls_stream` on the `FileSystem` trait. Existing callers of `find`, `ls`, and `walk` require zero code changes.
- **Dependencies**: No external crate additions; leverages existing `tokio`, `futures`, `google-cloud-storage`, and `google-cloud-gax` capabilities.
- **Breaking Changes**: None.

