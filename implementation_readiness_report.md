# Stage 9: Implementation Readiness Report

**Target Document**: [`docs/filesystem_api_design.md`](file:///usr/local/google/home/princer/code/gcs-rust-fs/docs/filesystem_api_design.md) (Lines 588–643, §10 Phasing & Testing, §11 Decisions D1–D30)  
**Evaluators**: 
- `egm-implementation-readiness` (Pragmatic Tech Lead)
- `egm-codebase-details` (Pedantic Technical Quality Auditor)  
**Date**: 2026-11-04  

---

### Executive Readiness Verdict: CONDITIONAL GO (Re-baselining Required)

The core filesystem engine in `src/` and test suites in `tests/` are significantly further ahead than [`docs/filesystem_api_design.md`](file:///usr/local/google/home/princer/code/gcs-rust-fs/docs/filesystem_api_design.md) indicates. The foundational traits ([`FileSystem`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs), [`File`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs)), generic derived algorithms ([`src/derived.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/derived.rs)), GCS backend ([`GcsFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/fs.rs), [`GcsFile`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/file.rs)), and directory cache decorator ([`CachedFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/cached.rs)) are already written, cleanly structured, and pass both in-memory unit tests ([`tests/memory_fs.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/tests/memory_fs.rs)) and live multi-bucket integration tests ([`tests/live.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/tests/live.rs)).

However, **§10 and §11 suffer from severe architectural desynchronization and critical implementation blind spots**:
1. **Misnomered "Open Decisions"**: §11 lists decisions **D1 through D30** as "Open decisions", yet an exhaustive audit of `src/` confirms that **29 of the 30 are already decided, implemented, and locked into code**. They are historical Architecture Decision Records (ADRs), not pending discussions.
2. **Ghost Decision D15 (`gcsfs_compat`)**: D15 specifies a `GcsFsBuilder::gcsfs_compat(true)` flag for Python `test_dir_marker_*` compatibility. This method and flag are completely missing from [`src/gcs/backend.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/backend.rs) and [`src/gcs/fs.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/fs.rs).
3. **Roadmap Blindness (Missing Sidecar & Python Bridge Phases)**: §10 outlines Phase 0, Phase 1, and Phase 2, but **completely omits the gRPC/UDS sidecar binary** ([`src/bin/gcsfs_sidecar.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/bin/gcsfs_sidecar.rs)) and client/server RPC subsystem ([`src/rpc/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/)). Furthermore, the sidecar protocol is currently partial (`Info`, `List`, `Read`, `Write`), leaving mutations (`rm_file`, `mkdir`, `rmdir`) stubbed with `Unsupported`. §10 also omits the entire PyO3 bridge and upstream `gcsfs` rollout phase.
4. **IPC Sidecar Memory Bomb**: The IPC sidecar read handler in [`src/rpc/server.rs:247-267`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/server.rs#L247-L267) executes whole-object `cat_file`, buffering gigabytes of data into heap RAM before transmitting chunks over UDS.

---

### Detailed Findings by Severity

#### 1. Severity: 🔴 Blocker

* **Finding 1.1: Omission of gRPC/UDS Sidecar from Phasing and Protocol Incompleteness**
  * *Affected Scope*: §10 (Lines 589–594), [`proto/gcsfs/v1/filesystem.proto`](file:///usr/local/google/home/princer/code/gcs-rust-fs/proto/gcsfs/v1/filesystem.proto), [`src/rpc/client.rs:318-325`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/client.rs#L318-L325).
  * *Impact*: The crate ships a production binary ([`gcsfs-sidecar`](file:///usr/local/google/home/princer/code/gcs-rust-fs/Cargo.toml#L19)) and a full client/server RPC subsystem, yet §10 contains no phase or verification milestone for IPC. Furthermore, `filesystem.proto` only defines `Info`, `List`, `Read`, and `Write`; directory and file mutations return `ErrorKind::Unsupported` on [`RemoteFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/client.rs#L318-L325).
  * *Resolution*: Add a dedicated Phase to §10 for the RPC Sidecar. Explicitly document whether `RemoteFs` is a data-plane sidecar only, or expand the proto to cover mutations.

* **Finding 1.2: Ghost Specification: Unimplemented D15 Promise (`GcsFsBuilder::gcsfs_compat`)**
  * *Affected Scope*: §11 Line 627 (Decision D15), [`src/gcs/backend.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/backend.rs), [`src/gcs/fs.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/fs.rs).
  * *Impact*: D15 promises a `GcsFsBuilder::gcsfs_compat(true)` switch to reproduce directory-marker quirks for `gcsfs` compatibility tests. Neither `GcsFsBuilder` nor `Backend` has any such field or method. `GcsFs::ls` unconditionally filters out own placeholder objects.
  * *Resolution*: Either implement `gcsfs_compat(bool)` on `GcsFsBuilder` and thread it into `ls`, or revise D15 to state that directory marker quirks are handled strictly in the Python bridge shim.

* **Finding 1.3: Sidecar Memory Exhaustion via Whole-File Buffering in `cat_file`**
  * *Affected Scope*: [`src/rpc/server.rs:247-267`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/rpc/server.rs#L247-L267).
  * *Impact*: In `FileSystemRpcService::read`, incoming `ReadRequest` calls `fs.cat_file`, buffering the entire requested byte range into contiguous memory before chunking. For ML workloads reading 10–50 GiB files, this triggers heap allocation spikes and container SIGKILL.
  * *Resolution*: Stream iteratively from `File::read` with a fixed buffer (e.g. 1–3.5 MiB) directly into the gRPC response channel.

---

#### 2. Severity: 🟠 Major

* **Finding 2.1: Reality Desynchronization: §11 Catalogues Settled Code as "Open Decisions"**
  * *Affected Scope*: §11 Lines 606–643 (Decisions D1–D30).
  * *Impact*: Section 11 is titled *"11. Open decisions"*, but code verification reveals 29 of 30 decisions are fully resolved and implemented in `src/`:
    - D1/D30: `src/cached.rs` and `src/dircache.rs`
    - D2: `ls("")` root bucket listing in `src/gcs/fs.rs:564`
    - D3: Unified `open(..., OpenOptions)` in `src/filesystem.rs:88`
    - D4: 5 MiB read-ahead buffer in `src/gcs/file.rs:18`
    - D5: Native matcher in `src/glob.rs`
    - D6: `EntryKind` has `File` and `Directory` in `src/entry.rs:30`
    - D7: Bucket probe with listing fallback in `src/gcs/fs.rs:280-298`
    - D8: `MkdirOptions.placeholder` in `src/options.rs:328`
    - D9: Strict POSIX errors in `src/derived.rs` and `src/gcs/fs.rs`
    - D10: Recursive bucket deletion in `src/gcs/fs.rs:740`
    - D11: `destination_base` in `src/derived.rs:596`
    - D12: Strict `rmdir` in `src/gcs/fs.rs:733`
    - D13 / D22: Transport gRPC default with HTTP fallback in `src/gcs/backend.rs:479`
    - D14: `transport()` accessor in `src/gcs/fs.rs:161`
    - D16: `File` trait omitting `Debug` in `src/file.rs:58`
    - D17: Monolithic Phase 1 implementation completed
    - D18: Lazy layout detection in `src/gcs/backend.rs:191-230`
    - D19 / D29: Dispatch by named capabilities of `BucketKind`
    - D20: Zonal `copy_file` returns `Unsupported` in `src/gcs/fs.rs:948`
    - D21: `finalize_on_close` knob in `src/gcs/backend.rs:103`
    - D22: Automatic HTTP fallback on gRPC failure
    - D23: `create_bucket(name, BucketSpec)` in `src/gcs/fs.rs:175`
    - D24: Live test bucket harnesses in `tests/live.rs:58`
    - D25: Precondition failure mapped to `AlreadyExists` in `src/gcs/write.rs:88`
    - D26: Appendable object `discard` leaves bytes intact in `src/gcs/write.rs:304`
    - D27: Explicit `File::discard()` in `src/file.rs:149`
    - D28: `ReadOptions` struct in `src/options.rs:107`
  * *Resolution*: Retitle §11 to *"Architecture Decision Records (ADRs)"*. Reclassify D1–D30 as *Resolved / Implemented*. Reserve "Open Decisions" for genuinely pending topics.

* **Finding 2.2: Omission of Python Bridge & Upstream Rollout Plan in §10**
  * *Affected Scope*: §10 Lines 589–594.
  * *Impact*: The design doc terminates at Phase 2 (Optimization), omitting the PyO3 FFI C-extension binding generation, wheel packaging, upstream `gcsfs` test suite validation, and rollout phases.
  * *Resolution*: Add Phase 3 (Python Bridge & FFI / Packaging) and Phase 4 (Upstream `gcsfs` Integration & Rollout) to §10.

* **Finding 2.3: Retained Superseded Decision (D13 vs D22)**
  * *Affected Scope*: §11 Line 625 (D13) and Line 634 (D22).
  * *Impact*: D22 notes `(supersedes D13)`, but D13 remains marked only as `(carried over)` without stating it has been superseded.
  * *Resolution*: Update D13 status to explicitly state *"Superseded by D22"*.

---

#### 3. Severity: 🟡 Nit

* **Finding 3.1: Scenario Count Discrepancy in Live Testing Verification**: §10 describes 5 read/write scenarios, but `tests/live.rs` contains 6 scenarios (including `rw_cached_listing_and_info`).
* **Finding 3.2: Elevate Bucket Deletion Log Level (D10)**: Decision D10 states that recursive `rm(bucket)` logs at `warn!` level. In `src/gcs/control.rs:263`, `rmdir` logs at `debug!`. Elevate to `warn!`.
* **Finding 3.3: Inconsistent Mention of Glob Matcher Phase**: §10 line 593 slates server-side `match_glob` for Phase 2, while D5 line 617 refers to a Phase 3 override. Harmonize.
