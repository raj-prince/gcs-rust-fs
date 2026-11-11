# Design: Parallel Composite Upload Architecture

## Context

In `gcs-rust-fs`, file uploads in `GcsFs::put_file` are currently executed through a single `write_object(...).send_unbuffered().await` call. For a 200 MiB upload, this transmits the entire file over a single TCP connection, saturating at ~85 MiB/s (~2.4s) due to single-stream congestion control limits and server-side ingress caps.

In contrast, Google Cloud Storage supports server-side `ComposeObject`, which can concatenate up to 32 independent source objects in a single metadata RPC. Slicing files into concurrent component streams allows multi-connection bandwidth saturation (150+ MiB/s) without increasing client-side CPU or OS process count.

See `proposal.md` for motivation.

## Goals / Non-Goals

**Goals:**
- Accelerate uploads of files $\ge 64\text{ MiB}$ by splitting into concurrent component streams and concatenating via `ComposeObject`.
- Cut 200 MiB upload time from ~2.4s down to $< 1.35s$ (matching or beating `gcloud`).
- Maintain lean memory usage ($< 120\text{ MiB}$ RSS) via zero-copy positional chunk streaming (`FileSliceSource`).
- Provide atomic creation and precondition enforcement (`WriteMode::CreateNew` via `if_generation_match(0)`).
- Ensure zero orphaned temporary objects via `CompositeUploadGuard` cleanup on failure or cancellation.
- Preserve 100% backward compatibility for all `put_file` callers.

**Non-Goals:**
- Supporting composite upload for streaming writes (`open(..., write)` or `pipe_file`). These remain direct single-stream writes because stream length is not known in advance and data cannot be seeked or sliced without unbounded client-side buffering; composite upload is strictly bounded to `put_file`.
- Multi-tier recursive composition trees for > 32 components (for files up to 2 GiB, 32 components with $\ge 64\text{ MiB}$ chunks suffice; for larger files, component size scales proportionally to fit within 32 parts).
- Enabling composite upload on zonal buckets (zonal buckets do not support `ComposeObject` and use appendable streams).

## Decisions

### 1. 64 MiB Threshold & Balanced Equal Slicing
* **Decision**: Trigger parallel composite upload only when `file_size >= 64 MiB` and the bucket supports composition (`kind.supports_server_copy()`). Below 64 MiB, run standard single-stream upload.
* **Component Slicing Formula**:
  ```
  target_parts = min(32, max(2, div_ceil(file_size, chunk_size)))
  part_size    = div_ceil(file_size, target_parts)
  ```
* **Rationale**:
  - For a 200 MiB file with 64 MiB chunk target: `target_parts = 4`, `part_size = 50 MiB`.
  - Dividing into equal 50 MiB slices ensures all 4 connections transfer the exact same byte count and finish simultaneously, eliminating stragglers caused by uneven tail chunks (e.g. 64 + 64 + 64 + 8 MiB).
* **Alternatives Considered**: Static 64 MiB chunks. Rejected because the last chunk becomes a straggler or creates suboptimal component counts.

### 2. Zero-Copy Positional Slicing (`FileSliceSource`)
* **Decision**: Implement `FileSliceSource` implementing `StreamingSource` for `google-cloud-storage`:
  ```rust
  struct FileSliceSource {
      file: Arc<std::fs::File>,
      offset: u64,
      remaining: u64,
      chunk_size: usize,
  }
  ```
* **Rationale**: Each worker reads 2 MiB chunks on-demand using positional `read_exact_at` (Linux/Unix `pread`) directly into a `BytesMut` buffer. Total in-flight buffer memory across 4 workers is $4 \times 2\text{ MiB} = 8\text{ MiB}$. Process RSS remains ~100 MiB.
* **Alternatives Considered**: Loading 50 MiB slices into memory buffers (`Vec<u8>`). Rejected because 4 $\times$ 50 MiB allocations would inflate RSS to > 300 MiB.

### 3. Preconditions & Atomic Composition
* **Decision**: Use `StorageControl::compose_object`:
  - `destination`: target GCS path.
  - `source_objects`: `[part_0, part_1, ..., part_N-1]`.
  - `if_generation_match(0)`: applied when `opts.mode == WriteMode::CreateNew`.
* **Rationale**: Guarantees atomic publication. If `CreateNew` is specified and the object already exists, GCS rejects the compose call with `PreconditionFailed`, preventing race conditions.

### 4. Fail-Safe Lifecycle Management (`CompositeUploadGuard`)
* **Decision**: Encapsulate temporary part paths (`<dest>.gstmp.<pid>.<nanos>.<idx>`) in a `CompositeUploadGuard`.
* **Rationale**:
  - On success: `guard.disarm()` is invoked, and temporary parts are deleted (via `set_delete_source_objects(true)` or parallel delete RPCs).
  - On failure or task cancellation: the guard triggers parallel deletion of any parts already uploaded to GCS.

### 5. Configurable Options on `WriteOptions`
* **Decision**: Add optional fields to `WriteOptions`:
  - `chunk_size: Option<usize>` (default 64 MiB)
  - `concurrency: Option<usize>` (default 4–8)
  - `composite_threshold: Option<usize>` (default 64 MiB)

## Risks / Trade-offs

- **[Risk] CRC32C Checksum vs MD5**: GCS composite objects do not expose an MD5 hash; they use a composite CRC32C checksum.
  - **Mitigation**: Composite CRC32C is standard GCS behavior (identical to `gcloud` and `gsutil`). Downstream consumers and `gcs-rust-fs` stat operations use CRC32C for integrity validation.
- **[Risk] Orphaned Temporary Objects on Process Termination**: If a process is terminated with `SIGKILL`, in-flight `.gstmp` parts could remain in the bucket.
  - **Mitigation**: Use a well-defined prefix pattern (`.gstmp.*`) so standard GCS bucket lifecycle rules (e.g. delete objects with `.gstmp.` older than 1 day) can automatically clean up any aborted uploads.

