# Stage 5: Initial Zoom Out Review Report

**Target Document**: [`docs/filesystem_api_design.md`](file:///usr/local/google/home/princer/code/gcs-rust-fs/docs/filesystem_api_design.md)  
**Evaluator**: Skeptical Architect (`egm-zoom-out`)  
**Mode**: REVIEW  
**Date**: 2026-11-04  

---

### Executive Summary

[`docs/filesystem_api_design.md`](file:///usr/local/google/home/princer/code/gcs-rust-fs/docs/filesystem_api_design.md) establishes a well-structured fsspec-compatible model for Cloud Storage, but suffers from **status desynchronization** with the codebase, **internal self-contradictions** across its sections, **speculative empty abstractions (YAGNI)**, and **leaky caching concerns** into core traits.

---

### Key Findings by Severity

#### 1. Status Desynchronization & Reality Disconnect
* **Severity**: **Blocker**
* **Affected Scope**: Preamble (lines 3–5, 14–17, 19–27) vs §10 (line 592) and [`src/gcs/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/)
* **Simplicity Rationale**:
  The document's top-level header and introduction state:
  > *"Status: contract implemented (traits + derived operations); GCS implementation of the traits pending — review before continuing"* (lines 3–5)  
  > *"The goal of the doc is to agree on semantics... before any code is written"* (lines 14–15)  
  > *"The GCS types (`GcsFs`, `GcsFile`) still expose only the original read-only inherent methods and do not implement the traits yet"* (lines 24–27)  
  > *"Decisions that need your input are marked D1 … D15"* (line 16)
  
  Yet §10 marks Phase 1 as completed (`Phase 1 — GCS implementation ✅ ... all green on all three kinds`), §11 details decisions up to **D30**, and inspection of [`src/gcs/fs.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/fs.rs) and [`src/gcs/file.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/file.rs) shows that `impl FileSystem for GcsFs` and `impl File for GcsFile` are already written and tested. A new engineer or fresh reviewer reading this design doc is misled into evaluating an unwritten proposal rather than an existing, committed implementation. The document must be updated to reflect current reality.

---

#### 2. Interface Divergence: Trait API vs Bridge/Method Specification
* **Severity**: **Major**
* **Affected Scope**: §4.4, §5 (lines 421–426), §9 (line 576), §11 (D3)
* **Simplicity Rationale**:
  In §4.4 and decision D3, the API was settled as a single unified method:
  `async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>>`.
  
  However, §5 (Table row 424) still specifies `open(mode="wb")` mapping to `create -> GcsWriter`, with `GcsWriter::{write, flush, tell, close}`. Section 9 (Python bridge mapping, line 576) similarly specifies:
  ```python
  def _open(self, path, mode="rb", block_size=None, **kw):
      return RustReadFile(rs.open(path, block_size)) if "r" in mode else RustWriteFile(rs.create(path, ...))
  ```
  Neither `create` nor `GcsWriter` exists in [`src/filesystem.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs) or [`src/gcs/`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/). Having §5 and §9 document non-existent primitives confuses bridge implementors.

---

#### 3. Speculative Generality & YAGNI: Empty `ReadOptions`
* **Severity**: **Major**
* **Affected Scope**: §4.3 (line 342), §11 (D28), [`src/options.rs:100-110`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/options.rs#L100-L110)
* **Simplicity Rationale**:
  [`ReadOptions`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/options.rs#L108-L109) is an entirely empty struct (`pub struct ReadOptions {}`) marked `#[non_exhaustive]` solely for speculative future parameters ("parallel range requests, checksum verification"). It is threaded through `cat_file`, `get_file`, and `open`.
  
  Simultaneously, the doc specifies in line 337 that option structs are deliberately **not** `#[non_exhaustive]` so that callers can use struct-update syntax. Creating an empty, uniquely `#[non_exhaustive]` struct solely to avoid future interface changes is classic speculative complexity (YAGNI). If single-read options aren't needed today, `cat_file(path, range)` without an empty placeholder argument is cleaner and simpler.

---

#### 4. Leaky Abstraction: Caching Methods on Core Stateless Trait
* **Severity**: **Major**
* **Affected Scope**: §1 (lines 47–49), §4.2 (lines 268, 344), §11 (D30), [`src/filesystem.rs:187-189`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs#L187-L189)
* **Simplicity Rationale**:
  Section 1 explicitly declares as a non-goal: *"No caching inside the backends. A directory/metadata cache exists, but as an opt-in decorator (`CachedFs`, D30) — `GcsFs` itself stays stateless."*
  
  Yet the core storage-agnostic [`FileSystem`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs#L74) trait contains:
  * `fn invalidate_cache(&self, path: Option<&str>) {}`
  * `ListOptions::refresh: bool` (documented as "ignored by backends")
  
  Leaking cache-invalidation hooks and bypass flags into the core `FileSystem` trait breaks the separation between backends and decorators. If caching is an opt-in wrapper, cache management belongs on [`CachedFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/cached.rs#L44), not on every stateless backend and the base trait.

---

#### 5. Premature Optimization: Unconditional Double-RPC on `info()`
* **Severity**: **Major**
* **Affected Scope**: §2.1 (lines 95–104), §5 (line 409), [`src/gcs/fs.rs:548`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/fs.rs#L548)
* **Simplicity Rationale**:
  Section 2.1 prescribes issuing `get_object` and `list_objects(prefix=key + "/", delimiter="/", page_size=1)` concurrently via `tokio::join!` for every `info()` call to optimize latency (`latency = max, not sum`).
  
  This is a premature optimization that doubles RPC calls and billing/quota consumption for standard object lookups (e.g., checkpoints, parquet files). In typical workloads, >90% of `info` calls target files, not directories. 
  
  Furthermore, the document contains a direct contradiction: §2 states *"info('b/file') costs one [RPC]"* (line 83), but the algorithm in §2.1 and implementation in `fs.rs` unconditionally fire two RPCs. The simpler, cost-effective design is sequential fallback: attempt object stat first; only probe prefix listing if `NotFound` or a directory placeholder is encountered.

---

#### 6. Internal Self-Contradiction on `glob` Implementation
* **Severity**: **Nit**
* **Affected Scope**: §5 (Table row 417) vs §4.2 (line 274), §11 (D5), [`src/glob.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/glob.rs)
* **Simplicity Rationale**:
  In §5, Table row 417 states `glob` is implemented in **`Where: fsspec (D5)`** using `find(withdirs=true)`. 
  However, §4.2 lists `glob` as a derived `FileSystem` method, §11 (D5) states it is resolved as a native Rust matcher, and [`src/glob.rs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/glob.rs) provides a custom 187-line glob engine. Table 5 should be corrected to avoid conflicting messages on where `glob` execution occurs.

---

#### 7. Phantom References to `stat()` Method
* **Severity**: **Nit**
* **Affected Scope**: §2.1 (line 108), §2.2 (line 126), §5 (line 447)
* **Simplicity Rationale**:
  The document repeatedly references a filesystem-level `stat()` call (*"`stat()` stays as the strict, object-only call"*, *"return [stat(path)]"*, *"`info`/`stat` permission fallback"*).
  
  Neither [`FileSystem`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/filesystem.rs) nor [`GcsFs`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/gcs/fs.rs) exposes a public `stat()` method (only `info()` exists on `FileSystem`, while `stat()` only exists on an opened [`File`](file:///usr/local/google/home/princer/code/gcs-rust-fs/src/file.rs#L80) handle). References to a top-level `stat()` method should be removed or clarified as internal helpers.
