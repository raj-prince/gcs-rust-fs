# Proposal: Parallel Composite Upload

## Why

Uploading medium-to-large files (such as 200 MiB model checkpoints or datasets) using a single sequential stream saturates around 85 MiB/s due to single-connection TCP congestion limits and server-side ingress caps, requiring ~2.4s. In benchmark comparisons, multi-component parallel composite uploads achieve 150+ MiB/s (1.34s p50) by transferring components simultaneously across multiple connections and concatenating them server-side via `ComposeObject`.

Adding native parallel composite upload with a 64 MiB threshold to `gcs-rust-fs` will cut upload times by nearly 50% while preserving lean memory consumption (~100 MiB RSS) and single-process async execution.

## What Changes

- **Scoped Exclusively to `put_file`**: Composite upload applies strictly to `GcsFs::put_file` where local file size and seekable byte ranges are known up-front. Streaming writes via `open(..., write)` and `pipe_file` remain direct single-stream writes without composite slicing.
- **Automatic Composite Upload Threshold**: Files $\ge 64\text{ MiB}$ on buckets supporting server-side composition automatically trigger parallel composite upload. Files below 64 MiB continue using direct single-stream uploads with zero compose overhead.
- **Balanced Component Slicing**: Divide files $\ge 64\text{ MiB}$ into up to 32 evenly-sized components (e.g., 4 parts of 50 MiB for 200 MiB) to avoid idle-worker stragglers.
- **Zero-Copy Positional Slicing**: Stream components directly from positional file offsets into upload streams using bounded chunk buffers (2 MiB), maintaining a flat ~100 MiB process RSS without buffering 50 MiB blocks in memory.
- **Atomic Server-Side Composition**: Concatenate uploaded temporary parts via `StorageControl::compose_object`, enforcing generation preconditions (`if_generation_match`) for `WriteMode::CreateNew`.
- **Fail-Safe Cleanup Guard**: Guarantee that temporary `.gstmp` component objects are cleaned up via server-side deletion or an asynchronous RAII cleanup guard on failure or cancellation.
- **Write Configuration Knobs**: Expose optional `chunk_size`, `concurrency`, and `composite_threshold` fields on `WriteOptions`.

## Capabilities

### New Capabilities
- `filesystem/upload`: Covers parallel component slicing, concurrent chunk uploading, atomic server-side object composition, precondition validation, and temporary component lifecycle management.

### Modified Capabilities
*(None)*

## Impact

- **Affected Code**: `src/gcs/fs.rs`, `src/gcs/write.rs`, `src/gcs/control.rs`, `src/options.rs`.
- **API Surface**: Non-breaking additions of optional tuning knobs to `WriteOptions`. `FileSystem::put_file` signature is unchanged.
- **Dependencies**: Uses existing `tokio`, `futures`, and `google-cloud-storage` SDK clients without new external crates.
- **Breaking Changes**: None.

