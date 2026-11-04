# Stage 7: Critic Review Report

**Target Document**: [`docs/filesystem_api_design.md`](file:///usr/local/google/home/princer/code/gcs-rust-fs/docs/filesystem_api_design.md) (Lines 1–587, `FeatureRange`)  
**Evaluators**: 
- `egm-design` (Hostile Design Critic)
- `egm-structural` (Structural Layout Validator)
- `egm-style` (Technical Copy Editor)  
**Date**: 2026-11-04  

---

### Executive Synthesis

The three reviewers evaluated [`docs/filesystem_api_design.md`](file:///usr/local/google/home/princer/code/gcs-rust-fs/docs/filesystem_api_design.md) across system architecture, infrastructure economics, structural layout, and editorial hygiene.

The audit identifies four convergent themes:
1. **Status Desynchronization & Preamble Incoherence (Blocker)**: The preamble still claims the GCS trait implementation is unwritten and pending design sign-off, directly contradicting §10 and the completed, tested code in `src/gcs/`.
2. **Economic Explosion in `info()` (Blocker)**: Concurrently issuing `get_object` (Class B, $0.0004/1k) and `list_objects` (Class A, $0.0050/1k) on every file stat imposes a 1,250% cost penalty and exhausts bucket listing quotas.
3. **IPC Buffer-and-Forward OOM Risk (Blocker)**: IPC sidecar `read` calls `cat_file`, buffering entire multi-gigabyte files into heap RAM before transmitting 1 MiB chunks.
4. **Interface & Bridge Divergence (Major)**: Core traits unify handles into `open(..., OpenOptions) -> Box<dyn File>`, but §5 and §9 revive discarded methods (`create`, `GcsWriter`, `RustReadFile`/`RustWriteFile`). Additionally, `glob` execution boundary is contradictory across sections.

---

### Consolidated Findings by Severity

#### 1. Severity: 🔴 Blocker
* **Finding 1.1: Stale Implementation Claims and Status Desynchronization**  
  *Sources*: `egm-structural`, `egm-style`  
  *Scope*: Lines 1–28, 206–208, 392–394 vs §10 Line 592 & §11 Lines 606–643.  
  *Impact*: Lines 3–4 state GCS implementation is pending, lines 14–17 state "before any code is written", and lines 24–27 say GCS types do not implement traits yet. But §10 marks Phase 1 complete and green, and `src/gcs/` already implements the traits. Needs structured RFC frontmatter and calibration to active present tense.

* **Finding 1.2: Economic Explosion & Quota Exhaustion via Double-RPC in `info()`**  
  *Source*: `egm-design` (corroborated by `egm-style` finding on Line 83)  
  *Scope*: Lines 95–104, §5 line 409, `src/gcs/fs.rs:548–554`.  
  *Impact*: Concurrently executing `get_object` and `list_objects` (page_size=1) for every `info()` doubles RPC latency and burns expensive Class A List quotas for regular files. For 10M file queries, cost surges from $4.00 to $54.00 (13.5x), and risks HTTP 429/503 rate-limit cascades.  
  *Resolution*: Sequential probe: execute `get_object` first; only probe prefix listing if the object is `NotFound` or a zero-byte directory marker.

* **Finding 1.3: Sidecar OOM via Buffer-and-Forward Reads and Unbounded Listings**  
  *Source*: `egm-design`  
  *Scope*: `src/rpc/server.rs:247–267`, Doc lines 261–262, 531.  
  *Impact*: Sidecar `read` invokes `cat_file`, buffering entire files into contiguous RAM before streaming chunks. A 10 GiB checkpoint read allocates 10 GiB and triggers SIGKILL. In listing operations, buffering millions of `Entry` structs into a flat `Vec` risks container OOM.  
  *Resolution*: Stream directly from `File::read` or the underlying gRPC stream; introduce chunked streaming for recursive listings.

---

#### 2. Severity: 🟠 Major
* **Finding 2.1: Trait vs Bridge Interface Divergence (`open` vs `create`)**  
  *Sources*: `egm-structural`, `egm-style`  
  *Scope*: §4.2 (Line 253), §4.4 (Lines 360–382), §5 Table (Rows 421–426), §9 (Line 576), §11 D3.  
  *Impact*: Traits settle on `open(path, OpenOptions) -> Result<Box<dyn File>>`. However, Table 5 and the §9 Python bridge snippet invoke `rs.create(...)` returning `GcsWriter` or `RustWriteFile`, which do not exist in the trait system.

* **Finding 2.2: Contradictory `glob` Execution Boundary**  
  *Sources*: `egm-structural`, `egm-style`  
  *Scope*: §4.2 (Line 274), §5 Table (Line 417), §9 (Lines 582–583), §11 D5 (Line 617).  
  *Impact*: §4.2 and D5 define `glob` as a native Rust matcher in `src/glob.rs`. §5 Table row 417 claims `Where: fsspec (D5)` and §9 groups `glob` with Python-side helpers. Needs harmonization to `Where: Rust`.

* **Finding 2.3: `Box<dyn File>` Ownership Precludes Concurrent `read_range`**  
  *Source*: `egm-design`  
  *Scope*: Doc lines 253, 362–376, `src/file.rs:114`.  
  *Impact*: `read_range(&self)` is advertised as concurrent, but `open` returns `Box<dyn File>`, which is exclusive. If wrapped in `Arc`, cursor methods taking `&mut self` (`seek`, `read`, `write`, `close`) cannot be called. If wrapped in `Arc<Mutex<dyn File>>`, positional reads are serialized. Combining readers and writers on one trait forces runtime capability checks.

* **Finding 2.4: Non-Atomic Directory Mutation vs Invariant Guarantees Across Bucket Kinds**  
  *Source*: `egm-design`  
  *Scope*: Lines 180–187, 482–487, `src/derived.rs:455–500`.  
  *Impact*: On HNS, `mv` is an atomic folder rename; on Flat buckets, it is an iterative copy-delete loop that can tear upon failure. Zonal buckets return `Unsupported` on `copy_file`, which causes generic `copy` and `mv` in `derived.rs` to fail hard without fallback.

* **Finding 2.5: Cache Stampede and Infinite Stale Reads in `CachedFs`**  
  *Source*: `egm-design`  
  *Scope*: Doc lines 305–332, `src/cached.rs:49, 304–335`.  
  *Impact*: `CachedFs` releases its lock before awaiting network listings, so 100 concurrent tasks on a cold cache miss dispatch 100 duplicate network calls (thundering herd). Default `ttl: None` means multi-process environments read stale metadata permanently until manual invalidation.

* **Finding 2.6: Structural Inversion and Architecture Leakage in Vocabulary Section**  
  *Source*: `egm-structural`  
  *Scope*: §2 (Lines 62–194) vs §3 & §4.  
  *Impact*: Section 2 ("Vocabulary") embeds complete algorithmic pseudocode for `info()`, `ls()`, and `find()` and multi-page bucket capability matrices before Section 3 (Path model) and Section 4 (Public API) are defined.

* **Finding 2.7: Missing `ReadOptions` in Table 6**  
  *Source*: `egm-structural`  
  *Scope*: §4.3 Table 6 (Lines 340–353), §4.2 (Lines 262, 265), §11 D28.  
  *Impact*: Table 6 catalogues all Option structs but completely omits `ReadOptions`, despite its presence in core signatures and dedicated decision D28.

* **Finding 2.8: Python FFI Dict Serialization Overhead on Large Listings**  
  *Source*: `egm-design`  
  *Scope*: Lines 531–533, 563–566, 579–581.  
  *Impact*: Converting 200,000 `Entry` objects into 200,000 Python dicts with 12 keys each consumes ~100 MiB of Python heap and triggers heavy GC pauses.

---

#### 3. Severity: 🟡 Nit
* **Finding 3.1: Phantom Standalone `stat(path)` Method**: Text refers to top-level `stat(path)` in lines 108, 126, 447, but `FileSystem` only exposes `info(path)`.
* **Finding 3.2: Heading Hierarchy Violation on `4.2.1`**: Line 305 uses `### 4.2.1` as a sibling to `### 4.2` rather than `#### 4.2.1`.
* **Finding 3.3: Missing `file://` Links**: Document uses relative paths (`../src/...`) instead of absolute `file://` URIs.
* **Finding 3.4: Mixed Orthography (UK vs US)**: Standardize British English (`honour`, `synthesise`, `normalised`) to American English (`honor`, `synthesize`, `normalized`).
* **Finding 3.5: Conversational Tone & Hedging**: Cut conversational padding ("where things stand", "trivial glue", "noise next to an RPC", "proposal you can veto").
* **Finding 3.6: Syntax Highlighting in Code Blocks**: Add language specifiers to bare blocks; remove slash delimiters (`exists / is_file`) inside fenced `rust` and `python` blocks.
* **Finding 3.7: Stale Decision Reference**: Update intro reference from `D1 … D15` to `D1 … D30`.
* **Finding 3.8: Leaky Cache Methods in Base Trait**: Clarify that `invalidate_cache` and `refresh` are decorator-specific hooks.
