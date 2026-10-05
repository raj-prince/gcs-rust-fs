# Filesystem API design for `gcs-rust-fs`

Status: **contract implemented (traits + derived operations); GCS
implementation of the traits pending — review before continuing**

This document defines the full fsspec-style filesystem surface that
`gcs-rust-fs` will expose so that `gcsfs` can delegate these methods to Rust:

```
open read close exists isdir isfile write ls glob find rm tell flush walk
put move mv mkdir rm_file du cat cat_file rmdir copy size
```

The goal of the doc is to agree on **semantics, API shape and the split of
responsibilities between Rust and Python** before any code is written.
Decisions that need your input are marked `D1 … D15` and collected in
[§11](#11-open-decisions). Everything else is a proposal you can veto.

> **Where things stand.** The API shape is settled as two traits,
> [`FileSystem`](../src/filesystem.rs) and [`File`](../src/file.rs)
> ([§4](#4-public-api-the-filesystem-and-file-traits)). Their derived
> operations are implemented generically in [`derived.rs`](../src/derived.rs)
> and verified against an in-memory implementation in
> [`tests/memory_fs.rs`](../tests/memory_fs.rs). The GCS types (`GcsFs`,
> `GcsFile`) still expose only the original read-only inherent methods and do
> **not** implement the traits yet — that is the next step, and §5 is its
> specification.

---

## 1. Goals, non-goals, rules

**Goals**

1. Behave like `gcsfs` (fsspec semantics), not like POSIX. A user swapping the
   backend must not see different results for the same call.
2. Pure Rust, async, built only on `google-cloud-storage` (gRPC bidi reads +
   `StorageControl` for metadata) with the HTTP transport as fallback.
3. One place for GCS-specific knowledge: `src/gcs/`. The contract (`FileSystem`,
   `File`, `Entry`, options, errors) talks about *paths, entries, files,
   directories* — never about bucket resource names, descriptors, rewrite
   tokens, etc.

**Non-goals (for now)**

- No PyO3 code here (lives in gcsfs as `gcsfs_bindings`).
- No directory cache in Rust. fsspec's `dircache` stays in Python (**D1**).
- No ACL/IAM, signed URLs, lifecycle, requester-pays, soft-delete, appendable
  objects. These are GCS features, not filesystem operations.
- No local disk cache for reads.

**Rules we already agreed on**

- `GcsFs`/`GcsFile` expose filesystem semantics only; SDK plumbing is private.
- Errors are classified into `ErrorKind` so the bridge can raise the right
  Python exception without parsing messages.

---

## 2. Vocabulary: how a flat object store becomes a filesystem

GCS is flat. Everything below is an emulation; it is the same emulation gcsfs
uses, written down so we can test it.

| Term | Definition |
|---|---|
| **root** (`""` / `/`) | The list of buckets visible to the configured project. |
| **bucket** | A top-level directory. `bucket` ≡ `bucket/`. |
| **file** | An object whose name does not end in `/`, or ends in `/` but has size > 0. |
| **placeholder** | A zero-byte object whose name ends in `/` (created by the console / other tools). Treated as a directory; by default never listed as a file (see D15 for gcsfs's quirks). |
| **directory** | A bucket, a placeholder, or any prefix `p/` that has at least one object under it (a "common prefix"). |
| **entry path** | `bucket/key` — no scheme, no trailing `/` (gcsfs convention). `Entry::uri()` gives `gs://bucket/key`. |
| **generation** | `bucket/key#123` pins a generation. Reads honour it; listing/`find` only emit it when `versions = true`. |

Consequences worth stating explicitly:

- An **empty "directory" does not exist** unless it is a bucket or a
  placeholder. `mkdir("b/x")` followed by `isdir("b/x")` is `false` unless we
  write a placeholder (**D8**).
- `info("b/dir")` costs up to two RPCs: exact `get_object` (miss) + one list
  page. `info("b/file")` costs one.
- Listing is strongly consistent in GCS, so no "eventual consistency" caveats.

### 2.1 `info()` algorithm (mirrors gcsfs `_info`)

```
info(path):
  if path is bucket-only:
      get_bucket(bucket)                   -> Directory entry
      on PermissionDenied: ls(bucket) non-empty -> Directory entry, else NotFound
      # same as gcsfs (`GET b/{bucket}`, fallback to listing on OSError)

  # gcsfs issues both of these concurrently and takes the first useful answer;
  # we do the same (latency = max, not sum).
  a = get_object(bucket, key)
  b = list_objects(prefix = key.trim_end('/') + "/", delimiter = "/", page_size = 1)

  if a is Ok(obj) and !(obj.size == 0 && key.ends_with('/'))  -> File entry   (cancel b)
  # otherwise the object is missing or is a placeholder -> decide via listing
  if b has any object or prefix                               -> Directory entry (size 0)
  else                                                        -> NotFound
```

`exists = info().is_ok()` (so **directories count**, fixing today's
object-only `exists`). `isdir`/`isfile` read `Entry::kind`. `stat()` stays as
the strict, object-only call.

### 2.2 `ls()` algorithm (mirrors gcsfs `_ls` / `_list_objects`)

```
ls(path):
  root               -> list_buckets(project)              # Directory entries
  else:
    prefix = key == "" ? "" : key.trim_end('/') + "/"
    iterate all pages of list_objects(prefix, delimiter = "/")
       objects  -> File entries
                   (with delimiter "/" the only possible zero-byte "x/" object in
                    `objects` is the listed directory's own placeholder; gcsfs turns it
                    into a Directory entry *for `path` itself* — see D15)
       prefixes -> Directory entries (name without trailing '/')
    dedupe directories by path
    if nothing was found and key != "":
       # gcsfs: `ls` on a file returns the file itself
       return [stat(path)]                                  # NotFound propagates
    if versions: entry path becomes "bucket/key#generation"
    sort by path
```

### 2.3 `find()` (mirrors gcsfs `_find`)

Flat listing (`delimiter = ""`) under `prefix`, so one paginated RPC stream
regardless of depth. Then in Rust:

- if the listing under `path/` is empty, retry with `prefix = key` so that
  `find("b/file")` returns `[file]`; a missing path returns `[]` (gcsfs does
  not raise here),
- if `withdirs`, synthesise `Directory` entries for every intermediate prefix
  between `path` and each object; `path` itself is included when it is a
  nested directory (not when it is a bucket),
- `maxdepth` is a post-filter on the full listing (`maxdepth < 1` is an
  error), so cost is still *P* for the whole subtree,
- gcsfs emits placeholders as **File** entries with their raw trailing-slash
  name (`b/dir/`) *and* a synthesised `b/dir` directory when `withdirs`
  (see D15).

`walk`, `du`, `rm -r`, `copy -r`, `mv -r` are all built on `find`, which means
**one listing pass per operation** instead of fsspec's one `ls` per directory.

### 2.4 Bucket kinds: flat, hierarchical (HNS) and zonal (Rapid)

GCS has three kinds of bucket and they differ in exactly the places where the
emulation above matters. gcsfs upstream encodes the same split in
`ExtendedGcsFileSystem`; this is the spec we mirror.

| Kind | Detected by | Directories | Data path |
|---|---|---|---|
| **Flat** (default) | neither of the below | emulated from prefixes / placeholders (§2.1–2.3) | gRPC bidi **or** HTTP |
| **Hierarchical** (HNS) | `GetStorageLayout.hierarchical_namespace.enabled` | real `Folder` resources (Storage Control API); empty folders exist; no object versioning | gRPC bidi **or** HTTP |
| **Zonal** (Rapid Storage) | `GetStorageLayout.location_type == "zone"` (always HNS) | as HNS | **gRPC only**; objects are appendable (single writer, readable while unfinalized); no `RewriteObject` / `Compose` |

Detection is lazy — one `GetStorageLayout` per bucket on first use (it only
needs `storage.objects.list`, which is why gcsfs uses it rather than
`GetBucket`) — and cached for the lifetime of the `GcsFs`. A failed lookup
(missing bucket, missing permission, transient error) is logged at `warn` and
the bucket is treated as **flat without caching**, so a later call retries.
`GcsFsBuilder::bucket_kind(bucket, kind)` pre-seeds the cache for tests or
for principals that cannot read the layout. `GcsFs::bucket_kind(bucket)`
exposes the resolved kind to the bridge (gcsfs's `_is_zonal_bucket`).

What each operation does per kind (object reads/writes/deletes are identical
unless stated):

| Operation | Flat | HNS | Zonal |
|---|---|---|---|
| `info(dir)` | 1-item prefix listing / placeholder | `GetFolder` (times + metageneration in `Entry::stat`) | as HNS |
| `ls` | `ListObjects` delimiter `/` | + `include_folders_as_prefixes` so empty folders appear | as HNS |
| `find` | one flat `ListObjects` | `ListObjects` ∥ `ListFolders(prefix)`, merged (empty folders) | as HNS |
| `mkdir` nested | no-op / placeholder (D8) | `CreateFolder(recursive = create_parents)`; missing parent → `NotFound`; exists → ok | as HNS |
| `rmdir` nested | placeholder deleted if otherwise empty, else `DirectoryNotEmpty` (D12) | drop placeholder, then `DeleteFolder`; non-empty → `DirectoryNotEmpty` | as HNS |
| `mv` of a directory | derived: per-object `move_file` + cleanup | `RenameFolder` (atomic LRO) when same bucket; else derived | as HNS |
| `move_file` | same bucket `MoveObject`; else copy + delete | same | `MoveObject`; cross-bucket → `Unsupported` |
| `copy_file` | `RewriteObject` (token loop) | same | `Unsupported` (no server-side copy); the bridge may fall back to download + upload |
| read | configured `Transport` | same | always gRPC, whatever `Transport` says |
| write (`w`/`x`) | `WriteObject` resumable stream; `close` publishes | same | `open_appendable_object`; `flush` persists (visible to readers); `close` finalizes only if `finalize_on_close` |
| `OpenMode::Append` | `Unsupported` | `Unsupported` | `reopen_appendable_object(generation)` |
| `ls`/`find` `versions` | honoured | ignored (no versioning on HNS) | ignored |

The storage-neutral contract is untouched by all of this; the kind is an
implementation detail of `src/gcs/`. The two public additions are
`BucketKind` (behind `GcsFs`) and the builder knobs listed in §8.

---


## 3. Path model

The contract uses **string paths** in fsspec form — `bucket/key`, optionally
`gs://`-prefixed, optionally `#generation`-suffixed — so that it stays
storage-agnostic (an in-memory file system has no buckets) and matches what the
bridge receives from Python verbatim. Every `Entry.path` is normalised: no
scheme, no trailing `/`. The **root** is the empty string: `ls("")` lists
buckets, `info("")` is a directory, `find("")` spans all buckets.

`GcsPath` becomes an implementation detail of `src/gcs/` (the parsed form the
backend works with). Changes still needed there for the GCS implementation:

```rust
impl GcsPath {
    pub fn parse(s: &str) -> Result<Self>;   // must accept "b", "b/", "gs://b"; still rejects "" and "/"
    pub fn is_bucket(&self) -> bool;         // object.is_empty()
    pub fn as_dir_prefix(&self) -> String;   // "" for buckets, otherwise "key/" (trailing '/' normalised)
}
```

- Trailing slashes are **preserved** in `object` so placeholders remain
  addressable internally; all directory logic normalises via
  `as_dir_prefix()`. gcsfs strips them.
- `GcsPath::parse` keeps rejecting `#gen` that is not an integer (unchanged).

---

## 4. Public API: the `FileSystem` and `File` traits

The source of truth is the code — [`src/filesystem.rs`](../src/filesystem.rs),
[`src/file.rs`](../src/file.rs), [`src/entry.rs`](../src/entry.rs),
[`src/options.rs`](../src/options.rs) — this section explains the shape and the
reasoning.

### 4.1 Why traits, and why these two

- `FileSystem` ≙ fsspec `AbstractFileSystem`, `File` ≙ `AbstractBufferedFile`.
  The split is the same one fsspec, `std::fs` and `object_store` all make: the
  file system is a stateless, shareable service; a file handle carries state
  (mode, cursor, pending upload).
- Both are **object-safe `async_trait` traits** (`Send + Sync`), so the bridge
  can hold `Arc<dyn FileSystem>` / `Box<dyn File>` and pick the implementation
  at runtime (GCS over gRPC, GCS over HTTP, in-memory for tests). The one heap
  allocation per call that `async_trait` costs is noise next to an RPC.
- Implementations outside the crate are first-class: `Error` has public
  constructors for every kind, `ByteRange::clamp` exposes the slice semantics,
  and `tests/memory_fs.rs` is written purely against the public API.

### 4.2 `FileSystem` — three layers of methods

```rust
#[async_trait]
pub trait FileSystem: Send + Sync {
    // Required primitives — the only thing an implementation must provide.
    async fn info(&self, path: &str) -> Result<Entry>;
    async fn ls(&self, path: &str, opts: ListOptions) -> Result<Vec<Entry>>;
    async fn open(&self, path: &str, opts: OpenOptions) -> Result<Box<dyn File>>;
    async fn rm_file(&self, path: &str) -> Result<()>;
    async fn mkdir(&self, path: &str, opts: MkdirOptions) -> Result<()>;
    async fn rmdir(&self, path: &str) -> Result<()>;

    // Overridable primitives — generic defaults that work, but real stores
    // have much faster native versions (flat listing, ranged GET, server-side
    // copy / atomic rename). The GCS implementation overrides all of these.
    async fn find(&self, path: &str, opts: FindOptions) -> Result<Vec<Entry>>;
    async fn cat_file(&self, path: &str, range: ByteRange, opts: ReadOptions) -> Result<Bytes>;
    async fn pipe_file(&self, path: &str, data: Bytes, opts: WriteOptions) -> Result<()>;
    async fn put_file(&self, local: &Path, path: &str, opts: WriteOptions) -> Result<()>;
    async fn get_file(&self, path: &str, local: &Path, opts: ReadOptions) -> Result<()>;
    async fn copy_file(&self, src: &str, dst: &str) -> Result<()>;
    async fn move_file(&self, src: &str, dst: &str) -> Result<()>;

    // Derived operations — encode fsspec semantics once, in derived.rs.
    async fn exists / is_file / is_dir / size(&self, path: &str) -> …;
    async fn walk(&self, path: &str, opts: WalkOptions) -> Result<Vec<WalkEntry>>;
    async fn du(&self, path: &str, opts: DuOptions) -> Result<DiskUsage>;
    async fn glob(&self, pattern: &str, opts: GlobOptions) -> Result<Vec<Entry>>;
    async fn cat(&self, paths: &[&str], opts: BulkOptions) -> Result<Vec<(String, Result<Bytes>)>>;
    async fn cat_ranges(&self, requests: &[(&str, ByteRange)], opts: BulkOptions) -> Result<Vec<Result<Bytes>>>;
    async fn rm(&self, path: &str, opts: RmOptions) -> Result<()>;
    async fn copy(&self, src: &str, dst: &str, opts: CopyOptions) -> Result<()>;
    async fn mv(&self, src: &str, dst: &str, opts: CopyOptions) -> Result<()>;
    async fn put(&self, local: &Path, path: &str, opts: PutOptions) -> Result<()>;
}
```

Mapping of the requested fsspec surface: `exists isdir isfile ls glob find rm
walk put move/mv mkdir rm_file du cat cat_file rmdir copy size` are
`FileSystem` methods; `open` returns a `File`, and `read write tell flush
close` live on `File`.

What the derived layer guarantees (all verified in `tests/memory_fs.rs`):

| Operation | Behaviour encoded |
|---|---|
| `find` | BFS over `ls`; missing path → `[]`; file → `[file]`; `withdirs` adds directories and the start directory itself (never the root `""`); `maxdepth` ≥ 1 |
| `walk` | one `find(withdirs)` grouped into `(dir, dirs, files)` top-down; file or missing path → nothing |
| `glob` | `*`/`?`/`[seq]` within a segment, `**` across segments (zero-or-more mid-pattern, one-or-more trailing, as in `fsspec.utils.glob_translate`); trailing `/` → directories only; wildcard-free pattern → `info` |
| `cat` / `cat_ranges` | bounded concurrency, input order preserved, `OnError::{Raise, Ignore, Return}` |
| `rm` | file → `rm_file`; directory needs `recursive` else `IsADirectory`; files deleted concurrently (vanished files ignored), then directories deepest-first, then the path itself via `rmdir` |
| `copy` / `mv` | fsspec destination rule: `dst` ending in `/` or an existing directory ⇒ nest under `dst/<name>`, unless the *source* ends in `/` (copy contents); directory needs `recursive`; `mv` = `move_file` per file, then `rm` of what is left |
| `put` | same destination rule with the local trailing separator; walks the local tree; `get_file` creates parent directories |

Rules shared by every implementation: `exists` is true for files **and**
directories; file operations on directories fail with `IsADirectory`; nothing
is cached in Rust (**D1**).

### 4.3 Option structs

One plain struct per operation, all `Default` with fsspec's defaults, so calls
read like keyword arguments: `RmOptions { recursive: true, ..Default::default() }`.
They are deliberately **not** `#[non_exhaustive]` so the bridge can use
struct-update syntax; adding a field is a minor-version bump.

| Struct | Fields |
|---|---|
| `OpenOptions` | `mode: OpenMode` (`Read`/`Write`/`Append`/`CreateNew`, parsed from `"rb" "wb" "ab" "xb"`), `block_size`, `content_type`, `metadata` |
| `WriteOptions` | `mode: WriteMode` (`Overwrite`/`Create`), `content_type`, `metadata`, `block_size` |
| `ListOptions` | `versions` |
| `FindOptions` | `maxdepth`, `withdirs`, `versions` |
| `WalkOptions` / `GlobOptions` | `maxdepth` |
| `DuOptions` | `maxdepth`, `withdirs` |
| `BulkOptions` | `concurrency` (64), `on_error` |
| `RmOptions` | `recursive`, `maxdepth`, `concurrency` |
| `CopyOptions` | `recursive`, `maxdepth`, `concurrency`, `on_error: Option<OnError>` (`None` = fsspec default: ignore `NotFound` for recursive copies) |
| `PutOptions` | `recursive`, `concurrency`, `write: WriteOptions` |
| `MkdirOptions` | `create_parents`, `location`, `placeholder` |

GCS-only knobs (`if_generation_match`, `gcsfs_compat` listings, transport)
are **not** options on the trait; they belong to `GcsFsBuilder` configuration
(**D15**).

### 4.4 `File` — one handle type, one mode

```rust
#[async_trait]
pub trait File: Send + Sync {
    fn path(&self) -> &str;  fn mode(&self) -> OpenMode;  fn closed(&self) -> bool;
    fn tell(&self) -> u64;   fn size(&self) -> Option<u64>;  fn stat(&self) -> Option<&ObjectStat>;
    fn readable(&self) -> bool;  fn writable(&self) -> bool;  fn seekable(&self) -> bool;

    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;                 // read handles; sync, no RPC
    async fn read(&mut self, len: Option<usize>) -> Result<Bytes>;    // cursor; None = to EOF; empty at EOF
    async fn read_range(&self, range: ByteRange) -> Result<Bytes>;    // positional; cursor untouched; &self ⇒ concurrent

    async fn write(&mut self, data: Bytes) -> Result<()>;             // append-only, back-pressure
    async fn flush(&mut self) -> Result<()>;                          // hands bytes to the upload; does NOT publish

    async fn close(&mut self) -> Result<()>;                          // writers: finalise + publish atomically; idempotent
    async fn discard(&mut self) -> Result<()>;                        // writers: abort, publish nothing; idempotent (D27)
}
```

- Like `std::fs::File` and Python file objects, read and write handles share
  one type; a wrong-mode call fails with `Unsupported`, I/O after `close` with
  `Closed`. This keeps `open(path, mode)` a single method, matching fsspec
  (**D3** resolved).
- Read handles pin the content at open time (generation on GCS).
- Writers follow object-store semantics that differ from local files and must
  be documented for users: objects are immutable, so **nothing is visible until
  `close()`**; `flush()` only pushes bytes into the upload; `discard()`
  abandons the upload and publishes nothing, and dropping an unclosed writer
  does the same as a safety net (D27); `Append` is `Unsupported` except on
  zonal buckets, where appendable objects make `flush()` persist and
  `discard()` cannot un-publish (D21, D26); `CreateNew` maps to
  `if_generation_match = 0` → `AlreadyExists` (D25).
- Planned GCS writer: a channel-backed `StreamingSource` feeding
  `Storage::write_object(..).send_buffered()` on a background task; the SDK
  handles resumable vs single-shot, retries and CRC32C.
- Read-ahead for `read()` (**D4**) is an implementation choice of `GcsFile`,
  not part of the contract.

---

## 5. Method-by-method mapping

"Where" = **Rust** (native implementation here), **Bridge** (trivial glue in
the PyO3 layer) or **fsspec** (inherited Python logic, Rust only supplies the
primitive). RPC cost is for the common case; *n* = number of objects,
*P* = ⌈n / 1000⌉ list pages.

| fsspec call | Where | Rust API | Backend primitive(s) | SDK RPC(s) | Cost |
|---|---|---|---|---|---|
| `info` | Rust | `info` | `get_bucket` / `get_object` + `list_page` | `StorageControl::{get_bucket,get_object,list_objects}` | 1–2 |
| `exists` | Rust | `exists` | same as `info` | same | 1–2 |
| `isdir` / `isfile` | Rust | `is_dir` / `is_file` | same as `info` | same | 1–2 |
| `size` | Rust | `size` | same as `info` | same | 1–2 |
| `ls` | Rust | `ls` / `ls_buckets` | `list_page` (delimiter `/`) / `list_buckets` | `list_objects` by page / `list_buckets` | P |
| `find` | Rust | `find` | `list_page` (no delimiter) | `list_objects` | P |
| `walk` | Rust | `walk` (grouped `find`) | `list_page` | `list_objects` | P (fsspec: one `ls` per dir) |
| `du` | Rust | `du` | `list_page` | `list_objects` | P |
| `glob` | fsspec (**D5**) | uses `find(withdirs=true)` | `list_page` | `list_objects` | P |
| `cat_file` | Rust | `cat_file` | `read_range` | `open_object`+`read` (gRPC) / `read_object` (HTTP) | 1 (+1 stat if negative bounds) |
| `cat_ranges` | Rust | `cat_ranges` | `read_range` ×k, bounded concurrency | as above | k |
| `cat` | Rust | `cat` (`find` for recursive) | `read_range` ×n | as above | P + n |
| `open(mode="rb")` | Bridge→Rust | `open` → `GcsFile` | `open` (stat + descriptor) | `get_object` + `open_object` | 1–2 |
| `read` / `seek` / `tell` | Rust | `GcsFile::{read,seek,tell}` | `read_range` on the open descriptor | bidi read / HTTP range | per read (≤1) |
| `close` (reader) | Rust | `GcsFile::close` | drop | — | 0 |
| `open(mode="wb")` | Bridge→Rust | `create` → `GcsWriter` | `write_object(stream)` | `WriteObject` (single-shot or resumable) | 1+ |
| `write` / `flush` / `tell` | Rust | `GcsWriter::{write,flush,tell}` | channel → upload task | resumable chunks | per chunk |
| `close` (writer) | Rust | `GcsWriter::close` | finalise | finalise upload | 1 |
| `pipe_file` | Rust | `pipe_file` | `write_object(Bytes)` | `WriteObject` | 1 |
| `put_file` | Rust | `put_file` | `write_object(tokio::fs::File)` | `WriteObject` (resumable, seekable → unbuffered) | 1+ |
| `put` | Rust | `put` (local walk + bounded `put_file`) | `write_object` ×n | `WriteObject` ×n | n |
| `get_file` | Rust | `get_file` | `read_range(all)` → file | read | 1 |
| `rm_file` | Rust | `rm_file` | `delete_object` | `delete_object` | 1 |
| `rm` | Rust | `rm` (`find` + bounded deletes) | `delete_object` ×n (+ `delete_bucket`) | `delete_object` | P + n |
| `copy_file` / `cp_file` | Rust | `copy_file` | `rewrite_object` loop | `rewrite_object` (token loop) | ≥1 |
| `copy` / `cp` | Rust | `copy` (`find` + mapping + bounded `copy_file`) | `rewrite_object` ×n | `rewrite_object` | P + n |
| `mv` / `move` | Rust | `mv` (`find` + mapping + bounded `move_file`) | same bucket: `move_object`; else `rewrite_object` + `delete_object` | `move_object` (atomic rename) / rewrite + delete | P + n (same bucket) / P + 2n |
| `mkdir` | Rust | `mkdir` | `create_bucket` / optional placeholder | `create_bucket` / `WriteObject` | 0–1 |
| `rmdir` | Rust | `rmdir` | `delete_bucket` / `delete_object` | `delete_bucket` / `delete_object` | 1–2 |
| `makedirs`, `touch`, `pipe`, `get`, `modified`, `created`, `checksum`, `expand_path` | fsspec/Bridge | derived from the above | — | — | — |

Notes per method where gcsfs behaviour is subtle (all verified against the
gcsfs source in `~/code/gcsfs` and fsspec 2026.7):

- **`ls` on a file** returns `[that file]`; on a missing path raises NotFound
  (gcsfs does the same via `_get_object`).
- **`ls(versions=true)`** emits one entry per generation with `#gen` in the
  path (gcsfs convention).
- **`info`/`stat` permission fallback**: when `get_object` is *Forbidden*,
  gcsfs falls back to `list_objects(prefix = key, page_size = 1)` and matches
  the exact name (works with list-only IAM). Cheap to mirror; proposed.
- **`find` on a file** returns `[file]`; on a missing path returns `[]`.
- **`du`**: `DiskUsage::total` sums files only (dirs are 0); `sizes` is the
  `find` result with sizes (what fsspec returns for `total=False`).
- **`cat` on a directory path** with `recursive = false` raises
  `IsADirectory`; fsspec's `cat` instead expands globs/dirs via
  `expand_path` — keep that expansion in fsspec (**D5**), Rust `cat` takes
  explicit file paths.
- **`rm_file` on a bucket** → `IsADirectory` (gcsfs silently calls
  `rmdir`) (**D9**).
- **`rm(dir, recursive=false)`** → `IsADirectory` (gcsfs: 404 → NotFound,
  confusing) (**D9**).
- **`rm(bucket, recursive=true)`** deletes every object **and then the
  bucket** (gcsfs `_rm`: paths without a key go to `_rmdir`). Recommend
  mirroring but it is dangerous enough to ask (**D10**). gcsfs also raises
  `NotFound` when *nothing* was deleted and ignores per-object NotFound —
  mirror both.
- **`copy` path mapping** follows fsspec's documented scenarios
  ([copying.html](https://filesystem-spec.readthedocs.io/en/latest/copying.html),
  implemented by `other_paths`) (**D11**):
  - file → `dst` ending in `/` or an existing directory: `dst/basename(src)`;
    otherwise `dst` is the new file name.
  - directory (needs `recursive`): **the trailing slash on the source
    decides** — `copy("b/src/", "b/dst/")` copies the *contents*
    (`b/dst/<rel>`), `copy("b/src", "b/dst/")` copies the directory itself
    (`b/dst/src/<rel>`). The nesting applies only when `dst` is a directory
    (trailing `/` or exists); otherwise contents map to `dst/<rel>`.
  - `maxdepth` limits depth; `recursive = false` on a directory copies nothing
    (fsspec returns silently) — we return `IsADirectory` instead (**D9**).
  - with `recursive`, fsspec defaults `on_error = "ignore"` (NotFound only);
    we default `OnError::Ignore` for recursive and `Raise` otherwise, same.
  - Same bucket or cross bucket both use server-side `rewrite_object`; no data
    flows through the client.
- **`mv`**: current gcsfs uses the **`objects.move` API** (atomic rename, one
  RPC, works for any bucket type) when source and destination are in the same
  bucket, falling back to copy + delete on non-NotFound errors; cross-bucket
  is copy + delete. We map this to `StorageControl::move_object` with the
  same fallback. Moving to a `#generation` destination is an error.
  Path mapping is the same as `copy`.
- **`mkdir(bucket)`** creates the bucket (needs `project`, optional
  `location`); `mkdir(bucket/dir)` is a no-op when the bucket exists; when the
  bucket is missing it is created if `create_parents = true` (gcsfs default is
  `false` → `NotFound`). Optional `placeholder = true` additionally writes the
  zero-byte `dir/` object (**D8**). Root → `InvalidPath`.
- **`rmdir(bucket)`** deletes the bucket; non-empty → `DirectoryNotEmpty`.
  **`rmdir(bucket/dir)`**: gcsfs is a silent no-op. Proposal: delete the
  placeholder if the directory is otherwise empty, `DirectoryNotEmpty` if not,
  `NotFound` if it does not exist (**D12**).

---

## 6. Errors

New `ErrorKind` variants (and their Python mapping in the bridge):

| `ErrorKind` | Raised by | Python |
|---|---|---|
| `IsADirectory` | `cat_file`/`open`/`rm_file`/`rm(recursive=false)` on a directory | `IsADirectoryError` |
| `NotADirectory` | `ls`/`walk`/`rmdir` on a file where a directory is required | `NotADirectoryError` |
| `AlreadyExists` | `create`/`pipe_file` with `if_generation_match = 0`, `mkdir(bucket)` on an existing bucket | `FileExistsError` |
| `DirectoryNotEmpty` | `rmdir` | `OSError(ENOTEMPTY)` |
| `PreconditionFailed` | any `if_*_match` mismatch other than create-only | `OSError` |
| `Unsupported` | append mode, operations the transport cannot do | `NotImplementedError` |

Bulk operations (`cat`, `cat_ranges`, `rm`, `copy`, `put`) honour
`OnError::{Raise, Ignore, Return}`; with `Raise` the first error aborts the
remaining work (in-flight tasks are cancelled) and is returned. `rm` treats
`NotFound` on individual objects as success (idempotent delete, like gcsfs).

---

## 7. Concurrency and resource limits

- `GcsFs` is `Clone + Send + Sync`; all methods take `&self`.
- Bulk work uses `tokio::task::JoinSet` bounded by a `Semaphore(concurrency)`
  (default 64, configurable per call and as a builder default
  `GcsFsBuilder::default_concurrency`).
- Listing is sequential per prefix (page tokens are inherently serial).
  Phase 3 can shard huge prefixes with `lexicographic_start/end`, as gcsfs's
  `_concurrent_list_objects_helper` does.
- Readers hold the bidi descriptor for the lifetime of the `GcsFile` (needed:
  dropping it kills in-flight reads).
- Results are `Vec`, not streams, in phase 1 (simpler for the bridge, matches
  fsspec which returns lists). A streaming `find`/`walk` can be added later
  without breaking changes.

---

## 8. Configuration additions

| Builder | Env var | Used by |
|---|---|---|
| `project(id)` | `GCS_RUST_FS_PROJECT`, then `GOOGLE_CLOUD_PROJECT` | `ls_buckets`, `mkdir(bucket)` |
| `default_concurrency(n)` | `GCS_RUST_FS_CONCURRENCY` | bulk ops |
| `read_block_size(bytes)` | `GCS_RUST_FS_READ_BLOCK_SIZE` | `GcsFile::read` read-ahead |
| `write_block_size(bytes)` | `GCS_RUST_FS_WRITE_BLOCK_SIZE` | `GcsWriter` chunking |
| `bucket_kind(bucket, kind)` | — | pre-seeds the bucket-kind cache (§2.4) |
| `finalize_on_close(bool)` (default `false`, as gcsfs) | — | zonal writers: whether `close` finalizes the appendable object |

Existing: `transport`, `endpoint`, `grpc_subchannel_count`, `from_env()`.

Bucket creation with GCS-specific placement is **not** part of the contract
(`MkdirOptions` stays `create_parents` / `location` / `placeholder`). The
inherent `GcsFs::create_bucket(name, BucketSpec { location, zone,
hierarchical, storage_class })` covers gcsfs's
`mkdir(enable_hierarchical_namespace=…, placement=…)`; the bridge calls it
when those kwargs are present and `mkdir` otherwise.

---

## 9. Python bridge mapping (for context; implemented in gcsfs)

```python
class GCSFileSystem(AsyncFileSystem):
    async def _info(self, path, **kw):        return entry_to_dict(await rs.info(path))
    async def _ls(self, path, detail=False, **kw):
        entries = await rs.ls(path)           # root -> rs.ls_buckets()
        return [entry_to_dict(e) for e in entries] if detail else [e.path for e in entries]
    async def _find(self, path, maxdepth=None, withdirs=False, detail=False, **kw): ...
    async def _cat_file(self, path, start=None, end=None, **kw): return await rs.cat_file(path, start, end)
    async def _pipe_file(self, path, data, **kw):   await rs.pipe_file(path, data)
    async def _put_file(self, lpath, rpath, **kw):  await rs.put_file(lpath, rpath)
    async def _cp_file(self, p1, p2, **kw):         await rs.copy_file(p1, p2)
    async def _rm_file(self, path, **kw):           await rs.rm_file(path)
    async def _rm(self, path, recursive=False, maxdepth=None, **kw): await rs.rm(path, recursive, maxdepth)
    async def _mkdir / _rmdir / _du / _walk / _exists / _isdir / _isfile / _size ...
    def _open(self, path, mode="rb", block_size=None, **kw):
        return RustReadFile(rs.open(path, block_size)) if "r" in mode else RustWriteFile(rs.create(path, ...))
```

`entry_to_dict` produces gcsfs's dict shape:
`{"name", "size", "type": "file"|"directory", "bucket", "generation",
"updated", "mtime", "crc32c", "md5Hash", "contentType", "storageClass", ...}`.
fsspec-derived helpers (`glob`, `expand_path`, `cat` with globs, `get`, `pipe`,
`makedirs`, `touch`) keep working through these primitives.

---

## 10. Phasing and testing

| Phase | Scope | Verification |
|---|---|---|
| **0 — contract** ✅ | `FileSystem` / `File` traits, `Entry`, option structs, new error kinds, generic `derived.rs` (`find walk du glob cat cat_ranges rm copy mv put get_file …`) | `tests/memory_fs.rs`: an in-memory implementation of the six primitives drives every derived operation (12 scenario tests); `cargo clippy -D warnings`, docs |
| **1 — GCS implementation** ✅ (one PR, all three bucket kinds, read + write) | `impl FileSystem for GcsFs` (`info/ls/open/rm_file/mkdir/rmdir` + native `find/cat_file/pipe_file/put_file/copy_file/move_file`, `rm -r` in one listing, HNS `mv` via `RenameFolder`), `impl File for GcsFile` (cursor, `seek`, `read`, resumable writer, zonal appendable writer), bucket-kind detection (§2.4), `GcsFs::create_bucket`, `project` config | `tests/live.rs`: read-only tests via `GCS_RUST_FS_TEST_OBJECT`; five read/write scenarios (write/read back, streaming open, directories/listing/walk, copy/move/remove, append mode) run once per kind via `GCS_RUST_FS_TEST_BUCKETS=flat=princer-ckpt,hns=princer-test-bucket,zonal=princer-zonal-us-west4-a`, each under its own scratch tree that is removed and verified empty afterwards — all green on all three kinds |
| **2 — optimisation** | parallel listing shards, server-side `match_glob` for `glob`, multi-range parallel `cat_file` for large objects, read-ahead tuning | benchmarks vs gcsfs |

Testing approach: the fsspec semantics live in `derived.rs` and are
storage-agnostic, so they are tested once, deterministically and without
network, through the in-memory `FileSystem`. The GCS implementation then only
has to get the six primitives (plus its native overrides) right, which is what
the live tests cover. The acceptance list is the relevant subset of gcsfs's
`test_core.py` (`test_ls`, `test_info`, `test_find`, `test_du`, `test_walk`,
`test_rm`, `test_copy`, `test_mv`, `test_mkdir_rmdir`, the `test_dir_marker_*`
family), ported case by case.

---

## 11. Open decisions

Decisions marked *encoded* are what the trait layer / `derived.rs` currently
implements and tests; say so if you want any of them changed.

| # | Question | Recommendation / status |
|---|---|---|
| **D1** | Directory cache: none in Rust, fsspec `dircache` in Python? | Yes — no cache in Rust; keeps Rust stateless and semantics identical to gcsfs. |
| **D2** | Root handling: `ls_buckets()` method vs `Location { Root, Path }` on every call? | *Resolved by the string-path traits*: the root is `""`; `ls("")` lists buckets, no special method. |
| **D3** | `open` (read) + `create` (write) vs one `open(path, mode)`? | *Resolved*: one `open(path, OpenOptions { mode })` returning `Box<dyn File>`, exactly like fsspec. |
| **D4** | Read-ahead buffer in Rust `GcsFile::read` (default 5 MiB) or keep fsspec's `AbstractBufferedFile` cache in Python? | In Rust, configurable via `OpenOptions::block_size`; lets the Python file object be a thin shim. Not part of the contract. |
| **D5** | `glob` matcher: stay in fsspec over Rust `find`, or native Rust (globset / server `match_glob`)? | *Resolved*: native, dependency-free matcher in `glob.rs` with `fsspec.glob_translate` semantics, driven by `find` on the wildcard-free prefix. Server-side `match_glob` remains a phase-3 override. |
| **D6** | Buckets as `EntryKind::Directory` vs a separate `EntryKind::Bucket`? | `Directory` — keeps the API filesystem-only (*encoded*). |
| **D7** | `info(bucket)` via `get_bucket` with listing fallback. | *Resolved*: gcsfs already does exactly this; mirror it. |
| **D8** | `mkdir(bucket/dir)`: no-op (gcsfs) with opt-in `placeholder = true`? | Yes; default no-op (*encoded* in `MkdirOptions`). |
| **D9** | Stricter errors than gcsfs: `IsADirectory` for `rm_file(bucket)`, `rm(dir)`/`copy(dir)` without `recursive`, `cat_file(dir)`? | Yes (*encoded*); the bridge can relax if a gcsfs test depends on the old behaviour. |
| **D10** | `rm(bucket, recursive=true)` also deletes the bucket (gcsfs behaviour)? | Mirror gcsfs (*encoded*: `rm` ends with `rmdir(path)`), log at `warn` level in the GCS impl. Alternative: require explicit `rmdir`. |
| **D11** | Recursive `copy`/`mv` destination mapping: implement fsspec's `other_paths` scenarios natively? | Yes (*encoded* in `derived::destination_base`, tested). |
| **D12** | `rmdir(bucket/dir)`: gcsfs silent no-op vs delete placeholder / `DirectoryNotEmpty`? | Stricter version (*encoded* as the `rmdir` contract; derived `rm` tolerates `NotFound` so implicit directories work). |
| **D13** | Default transport stays `Grpc` (bidi read requires allowlisted buckets) or switch to `Http`? (carried over) | Keep `Grpc` default, auto-fallback to HTTP on `Unimplemented`/`FailedPrecondition` at `open` time. |
| **D14** | Keep `GcsFs::transport()` accessor? (carried over) | Keep; it is configuration, not a GCS feature. |
| **D15** | Placeholder quirks in listings: mirror gcsfs exactly (`ls("b/dir")` includes a `b/dir` directory entry for its own placeholder; `find` emits `b/dir/` as a file plus a synthesised `b/dir` dir) or clean semantics (placeholders never listed as files, listed directory never lists itself)? | Clean semantics in the contract; a `GcsFsBuilder::gcsfs_compat(true)` switch (not a trait option) reproduces the quirks for the bridge so gcsfs's `test_dir_marker_*` tests keep passing. |
| **D16** | Should `File` require `Debug` (so `Result<Box<dyn File>>` is `unwrap`-friendly and holders can `#[derive(Debug)]`)? | Not required for now; add `Debug` as a supertrait if the bridge wants it — it costs implementors one `impl`. |
| **D17** | Delivery order of the GCS implementation. | *Decided*: one PR — all three bucket kinds, read and write (§10 phase 1). |
| **D18** | Bucket-kind detection and failure behaviour. | *Decided*: lazy `GetStorageLayout` per bucket, cached for the `GcsFs` lifetime; a failed lookup is logged at `warn` and treated as flat **without caching**; `GcsFsBuilder::bucket_kind()` pre-seeds. |
| **D19** | Where kind-specific logic lives. | *Decided*: one `GcsFs`; each trait method `match`es the kind; helpers split into `src/gcs/{control,write,…}.rs`. No per-kind public types. |
| **D20** | Zonal buckets have no `RewriteObject`/`Compose`. | *Decided*: `copy_file` (and cross-bucket `move_file`) involving a zonal bucket → `ErrorKind::Unsupported` (gcsfs raises `NotImplementedError`); same-bucket `move_file` uses `MoveObject`. |
| **D21** | Zonal write policy. | *Decided*: mirror gcsfs — `GcsFsBuilder::finalize_on_close(bool)` default `false` (object stays appendable; `close` = flush + close stream); `"ab"` reopens by generation; `flush` persists bytes (unlike other kinds). |
| **D22** | Transport vs zonal (supersedes D13). | *Decided*: zonal data always travels over gRPC regardless of `Transport`; default stays `Grpc`; a non-zonal bucket whose bidi read fails with `Unimplemented`/`FailedPrecondition` falls back to HTTP for that read with a `warn`. |
| **D23** | gcsfs `mkdir(enable_hierarchical_namespace=…, placement=…)`. | *Decided*: keep `MkdirOptions` storage-neutral; inherent `GcsFs::create_bucket(name, BucketSpec)` outside the trait (§8). |
| **D24** | Live test buckets. | *Decided*: existing buckets — flat `princer-ckpt`, HNS `princer-test-bucket`, zonal `princer-zonal-us-west4-a` — via `GCS_RUST_FS_TEST_BUCKETS`, writing only under a per-run scratch prefix that is removed afterwards. |
| **D25** | Create-only writes (`WriteMode::Create`, `OpenMode::CreateNew`, placeholder `mkdir`) on an existing object: the service answers the `if_generation_match = 0` precondition with `FAILED_PRECONDITION` / HTTP 412 on every bucket kind. | *Decided*: classified as `ErrorKind::AlreadyExists` (`O_EXCL` → `EEXIST`); the SDK error stays reachable through `Error::storage_source()`. A bare `PreconditionFailed` is reserved for preconditions the caller did not ask for. |
| **D26** | `discard` on an appendable (zonal) write handle. | *Decided*: mirror gcsfs's `ZonalFile.discard` — the object was created at open and flushed bytes are persisted, so `discard` only stops writing and logs a `warn`; it does not delete the object. Documented as the stated exception in the `File` trait docs. Alternative (delete the object when this handle created it) is a one-line change in `Writer::discard` if the bridge prefers it. |
| **D27** | Keep an explicit `File::discard()`, or let dropping the handle be the only way to abandon a write? | *Decided*: **keep `discard()`**, with `Drop` as the safety net. Considered and rejected: drop-only. It works for GCS (abort is sync and infallible, and `Drop` is deterministic), but an explicit method (a) is the 1:1 target for fsspec's `AbstractBufferedFile.discard()` and the `commit`/`discard` transaction protocol, (b) is async and fallible, so a backend whose cancellation is a remote call (gcsfs deletes the JSON-API resumable session) can run and report it, (c) marks the handle `closed` so later I/O fails with `Closed` instead of feeding a dead upload, and (d) keeps "abandon, don't publish" an enforceable, testable part of the contract rather than a documented `Drop` convention. Implementations must still publish only from `close`, so an unclosed, undiscarded handle abandons on drop. The bridge maps Python `discard()` straight through. |
| **D28** | How do single-file reads grow new per-call options (parallel range requests, checksum verification, ...) without breaking every `impl FileSystem` and caller? | *Decided*: `cat_file` and `get_file` take a trailing `opts: ReadOptions`, mirroring `pipe_file`/`put_file` with `WriteOptions`; `OpenOptions::from_read` forwards it to `open` like `from_write` does. The struct is **empty today** (no knob is honoured yet, so none is exposed) and `#[non_exhaustive]`, so external code builds it with `ReadOptions::default()` and a new field is a non-breaking addition. Inside the crate the two consumers (`OpenOptions::from_read`, `GcsFs::cat_file`) destructure it (`let ReadOptions {} = opts;`) so a new field fails to compile until it is forwarded or handled. Rejected: a bare `concurrency` parameter (breaks on every later knob) and a silently ignored field. The other option structs are not `#[non_exhaustive]`; whether to apply it across the board is a pre-1.0 decision. |
| **D29** | How `GcsFs` reaches kind-specific behaviour: branch on the kind inline, a per-kind inheritance-like chain, or per-axis driver objects? | *Decided*: **one `GcsFs`, branching on named capabilities of `BucketKind`** (`is_hierarchical`, `has_versioning`, `supports_server_copy`, `supports_http`, `objects_appendable`) — never on the variant itself, so each site states the property it needs and a new kind only answers five questions. Rejected: a `Zonal ⊃ HNS ⊃ Flat` chain — the kind is a property of the bucket, not of the filesystem object (one `GcsFs` serves every bucket and `copy_file` reads two kinds), and each level *removes* capabilities (HNS: versioning; zonal: server copy, HTTP, move fallback), which inheritance cannot express; gcsfs, with inheritance available, also dispatches per bucket inside one class. Deferred, not rejected: per-axis drivers (`NamespaceDriver` {prefix, folder} × `DataDriver` {standard, appendable} as zero-sized statics). They buy per-kind locality at the cost of two traits, four impls, `Backend` threaded through every call and return conventions for mid-method forks, while two-kind operations and `derived::*` fallbacks stay in the core anyway; the capability predicates are the common prefix of both designs, so the move stays mechanical if a fourth kind or more contributors make locality worth it. |
