# UDS `gcs-rust-fs` Benchmark Report (`RemoteFs` $\rightarrow$ UDS $\rightarrow$ Sidecar `CachedFs`)

## 1. Benchmark Environment

| Component | Specification |
| :--- | :--- |
| **Compute Instance** | GCE `c4-standard-192` (`192` vCPUs, 2 sockets × Intel Xeon Platinum 8581C @ 2.30 GHz, 4 NUMA nodes) |
| **System Memory** | `708 GiB` RAM |
| **OS & Kernel** | Linux `7.0.0-1008-gcp` (`x86_64`) |
| **Compiler / Runtime** | `rustc 1.97.1` (`--release` profile), Tokio multi-threaded runtime |
| **Compute Zone** | `us-west4-a` |
| **Target Bucket & Region** | `gs://princer-flat/bench/` (Regional bucket in `US-WEST4`, co-located with compute VM) |
| **Target Objects** | `bench_20GB.bin` (`20.00 GiB` / `21,474,836,480` bytes) and `bench_20GB_00.bin` .. `bench_20GB_47.bin` (`48 × 20 GiB = 960 GiB`) |
| **UDS Wire Transport** | [`RemoteFs`](../src/rpc/client.rs) / [`RemoteFile`](../src/rpc/client.rs) $\xrightarrow{\text{gRPC over unix://}}$ [`FileSystemRpcService`](../src/rpc/server.rs) (Tonic HTTP/2 over Unix Domain Socket) |
| **Server Storage Engine** | `Transport::Http` (HTTP/JSON), [`CachedFs`](../src/cached.rs) Mode 1 Drop-Behind Readahead (`ReadPattern::Auto`, default `OpenOptions::read()`) |

---

## 2. UDS Benchmark Results

### Part 1: Uncached Metadata `info()` Latency over UDS (`1T`, `30` Iterations)

| Operation | Threads | Iters | Mean | Min | `p50` | `p90` | `p99` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| [`RemoteFs::info`](../src/rpc/client.rs) $\rightarrow$ UDS $\rightarrow$ [`GcsFs::info`](../src/gcs/fs.rs) | `1T` | `30` | `27.63 ms` | `23.20 ms` | **`27.67 ms`** | `30.19 ms` | `38.01 ms` |

---

### Part 2: Single-Threaded `1 MiB` Random Read over UDS (`1T`, `ReadPattern::Auto`)

Non-overlapping `4 MiB`-aligned random offsets across the `20 GiB` object (`read_size = 1 MiB`, `io_size = 1 MiB`):

| Approach | Threads | Reads (`Total Data`) | `p50` | `p90` | `p99` | IOPS | Throughput |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| [`RemoteFs`](../src/rpc/client.rs) $\rightarrow$ UDS $\rightarrow$ [`CachedFs`](../src/cached.rs) Mode 1 (`Auto`, `io=1MiB`) | **`1T`** | `50` (`50 MiB`) | **`47.86 ms`** | `66.34 ms` | `96.64 ms` | **`19.5`** | **`20.40 MB/s` (`19.45 MiB/s`)** |

---

### Part 3: Full-File Sequential Read over UDS (`1T`, `24T`, `48T` — `ReadPattern::Auto`)

Each thread streams its own distinct `20 GiB` object from offset `0` to `20 GiB` over UDS (`read_size = 8 MiB`, `io_size = 16 MiB`, `max_folio_size = 16 MiB`):

| Approach | Threads | Distinct Files (`Total Data`) | Wall Time | Chunk `p50` | Chunk `p90` | Throughput |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| UDS [`CachedFs`](../src/cached.rs) Mode 1 (`Auto`) | **`1T`** | `1 × 20 GiB` (**`20 GiB`**) | **`11.25 s`** | **`3.59 ms`** | `7.06 ms` | **`1,909.60 MB/s` (`1,821.14 MiB/s`)** |
| UDS [`CachedFs`](../src/cached.rs) Mode 1 (`Auto`) | **`24T`** | `24 × 20 GiB` (**`480 GiB`**) | **`48.99 s`** | **`18.68 ms`** | `22.27 ms` | **`10,519.39 MB/s` (`10,032.08 MiB/s`)** |
| UDS [`CachedFs`](../src/cached.rs) Mode 1 (`Auto`) | **`48T`** | `48 × 20 GiB` (**`960 GiB`**) | **`82.68 s`** | **`32.08 ms`** | `38.13 ms` | **`12,466.95 MB/s` (`11,889.41 MiB/s`)** |

---

### Part 4: Single-Threaded `200 MiB` Write over UDS (`1T`, `5` Iterations)

Single-threaded `200 MiB` (`209,715,200` bytes) object upload over UDS to `gs://princer-flat/bench/bench_20GB_uds_write_200MB.bin`:

| Approach | Threads | Iters | Mean | `p50` | `p90` | Throughput (at `p50`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| [`RemoteFs::open(write)`](../src/rpc/client.rs) + `write(8 MiB)` + `close` (`200 MiB` over UDS) | **`1T`** | `5` | `2.41 s` | **`2.42 s`** | `2.57 s` | **`86.74 MB/s` (`82.72 MiB/s`)** |
| [`RemoteFs::pipe_file`](../src/rpc/client.rs) (`200 MiB` Upload over UDS) | **`1T`** | `5` | `2.32 s` | **`2.36 s`** | `2.44 s` | **`88.72 MB/s` (`84.61 MiB/s`)** |

---

## 3. Comparison Summary: In-Process Native vs. Rust Thin Client over UDS

| Workload | Metric | Native In-Process (`CachedFs`) | Rust Client over UDS (`RemoteFs` $\rightarrow$ Sidecar) | Notes |
| :--- | :--- | :---: | :---: | :--- |
| **Part 1: Uncached `info()` (`1T`)** | `p50` Latency | `28.38 ms` | **`27.67 ms`** | Local UDS unary RPC adds `< 0.1 ms` overhead |
| **Part 2: `1 MiB` Random Read (`1T`)** | Throughput (`p50`) | `25.03 MB/s` (`39.15 ms`) | **`20.40 MB/s` (`47.86 ms`)** | Rule 7 single-read isolation preserved across UDS |
| **Part 3: `20 GiB` Seq Read (`1T`)** | Throughput | `4,106.42 MB/s` (`3,916.18 MiB/s`) | **`1,909.60 MB/s` (`1,821.14 MiB/s`)** | Exceeds `1.5 GB/s` single-stream UDS target ([`uds_server_and_thin_client.md`](designs/uds_server_and_thin_client.md)) |
| **Part 3: `20 GiB` Seq Read (`24T`)** | Throughput | `15,604.48 MB/s` (`14,881.59 MiB/s`) | **`10,519.39 MB/s` (`10,032.08 MiB/s`)** | `480 GiB` streamed over pooled UDS HTTP/2 connections |
| **Part 3: `20 GiB` Seq Read (`48T`)** | Throughput | `20,976.12 MB/s` (`20,004.39 MiB/s`) | **`12,466.95 MB/s` (`11,889.41 MiB/s`)** | `960 GiB` (`~100 Gbps`) streamed over local UDS |
| **Part 4: `200 MiB` Streaming Write (`1T`)** | Throughput (`p50`) | `97.88 MB/s` (`2.14 s`) | **`86.74 MB/s` (`2.42 s`)** | Client-streaming `Write` RPC with atomic close |
| **Part 4: `200 MiB` `pipe_file` (`1T`)** | Throughput (`p50`) | `156.46 MB/s` (`1.34 s`) | **`88.72 MB/s` (`2.36 s`)** | Uses client-streaming `Write` RPC over UDS |

---

## 4. Reproducing the UDS Benchmarks

```bash
# Run full UDS benchmark suite (info, 1T random read, 1T/24T/48T sequential read, 1T 200 MiB write)
cargo run --release --example bench_uds_readahead -- --bench-20gb gs://princer-flat/bench/bench_20GB.bin

# Run an individual section: --only <all|info|random|seq|seq-1t|seq-24t|seq-48t|write>
cargo run --release --example bench_uds_readahead -- --bench-20gb --only seq-1t gs://princer-flat/bench/bench_20GB.bin
```

