# Design: Adaptive Fast Listing Architecture

## Context

In `gcs-rust-fs`, object listing is implemented via two main paths:
1. `GcsFs::find`: Calls `list_objects` with `delimiter: false`, retrieving pages sequentially via `by_page()` and accumulating all objects into memory before returning.
2. `derived::find` and `derived::walk`: Use a serial breadth-first search (`VecDeque`) calling `fs.ls(dir)` one directory at a time.

For directories with high branching factors (e.g. 500 subdirectories) or flat prefixes with > 50,000 keys, serial RTT latency dominates execution time (e.g., 20+ seconds for a 500-folder tree). The Cloud Storage API natively supports features that can alleviate these bottlenecks:
- `lexicographic_start` and `lexicographic_end` for bounded key range slicing.
- `delimiter: "/"` returning `prefixes` for single-level subfolder partitioning.
- `read_mask` for reducing deserialization overhead and network payload size.

See `proposal.md` for motivation.

## Goals / Non-Goals

**Goals:**
- Provide zero-configuration adaptive listing that dynamically classifies workloads on Page 1.
- Eliminate serial bottlenecks in deep hierarchical trees via a bounded concurrent prefix crawler (16–32 parallel workers).
- Accelerate massive flat prefix traversal through range partitioning and lookahead prefetching.
- Introduce `find_stream` and `ls_stream` returning `BoxStream<'static, Result<Entry>>` with bounded $O(\text{concurrency} \times \text{page\_size})$ memory footprint.
- Preserve 100% backward compatibility for all existing `find`, `ls`, and `walk` callers.

**Non-Goals:**
- Guaranteeing lexicographical ordering within raw entry streams (`find_stream` yields as items arrive; `find` preserves exact lexicographical sort).
- Caching listings across runs (handled separately by `CachedFs` / `DirCache`).

## Decisions

### 1. Workload Fingerprinting on Page 1 Response
* **Decision**: Issue an initial probe request with `page_size = 1000` and `delimiter = "/"`. Inspect the returned `(objects, prefixes, next_page_token)`.
* **Rationale**:
  - If `prefixes.is_empty() && next_page_token.is_none()`: small leaf directory $\implies$ terminate immediately in 1 RTT ($< 30\text{ms}$). Zero overhead.
  - If `!prefixes.is_empty()`: branching tree $\implies$ dispatch to concurrent prefix crawler.
  - If `prefixes.is_empty() && next_page_token.is_some()`: massive flat directory $\implies$ activate range partitioner / prefetch pipeline.
* **Alternatives Considered**: Static user flags (`--flat` vs `--hierarchical`). Rejected because callers cannot always anticipate directory topology, and directory structures often vary within the same bucket.

### 2. Bounded Work-Stealing Prefix Crawler for Hierarchical Trees
* **Decision**: Manage concurrent directory crawls using a dynamic work queue bounded by a semaphore / `FuturesUnordered` pool (default concurrency: 24, configurable via `FindOptions::concurrency`).
* **Rationale**: Avoids unbounded Tokio task creation and prevents connection pool exhaustion while keeping network links fully saturated.
* **Alternatives Considered**: Recursive Tokio task spawning without concurrency limits (`tokio::spawn` per directory). Rejected due to file descriptor and socket exhaustion risk on trees with tens of thousands of subfolders.

### 3. Flat Prefix Partitioning via Lexicographical Slicing & Prefetching
* **Decision**: For flat prefixes that exceed 1,000 keys, sample the character distribution of Page 1 keys to calculate partition quantiles for `[lexicographic_start, lexicographic_end)`.
* **Rationale**: GCS natively supports range queries on object names. Concurrent range queries turn a serial 100-page token chain into 8–16 parallel streams.
* **Alternatives Considered**: Pure cursor pipelining. Pipelining alone cannot prefetch page $N+1$ without page $N$'s token. Range partitioning completely removes the token dependency.

### 4. Object-Safe Streaming Interface (`BoxStream`)
* **Decision**: Add `find_stream` and `ls_stream` to `FileSystem` returning `futures_core::Stream<Item = Result<Entry>> + Send + 'static`.
* **Rationale**: Allows downstream consumers (e.g. data loaders, indexers, backup tools) to begin processing files immediately without waiting for full directory crawls to complete or allocating hundreds of megabytes of RAM.
* **Alternatives Considered**: Channel-based callback `find_cb(path, FnMut(Entry))`. Streams integrate more naturally with Rust's async ecosystem (e.g., `StreamExt::filter`, `StreamExt::take`, `buffer_unordered`).

### 5. Metadata Projection via `read_mask`
* **Decision**: Request only necessary fields (`name`, `size`, `generation`, `metageneration`, `update_time`) on GCS `ListObjects` calls during fast listing.
* **Rationale**: Eliminates transmission and protobuf deserialization of unused ACLs, owner details, CRC32C, and custom metadata, cutting network payload size by up to 60%.

## Risks / Trade-offs

- **[Risk] Skewed Key Distributions in Flat Prefixes**: If key names are non-uniform, static range slicing might allocate too many keys to one worker and zero to others.
  - **Mitigation**: Derive range boundaries dynamically from the keys observed in the first 1,000-object probe page rather than assuming a uniform ASCII distribution.
- **[Risk] Increased GCS Request Concurrency**: Rapid parallel listing could trigger GCS rate limiting on high-frequency directories.
  - **Mitigation**: Bound default concurrency to 24–32 workers and use the existing retry and exponential backoff policies in `google-cloud-gax`.
- **[Risk] Stream Memory Backpressure**: If consumers process entries slowly, background worker pages could accumulate in memory.
  - **Mitigation**: Bound the internal stream channel buffer so workers pause fetching until the consumer makes progress.

