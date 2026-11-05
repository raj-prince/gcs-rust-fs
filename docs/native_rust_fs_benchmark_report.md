# Native `gcs-rust-fs` Benchmark Report (`US-WEST4`, `Transport::Http`)

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
| **Data Transport & Mode** | `Transport::Http` (HTTP/JSON), [`CachedFs`](../src/cached.rs) Mode 1 Drop-Behind Readahead (`ReadPattern::Auto`, default `OpenOptions::read()`) |

---

## 2. Benchmark Results

### Part 1: Uncached Metadata `info()` Latency (`1T`, `30` Iterations)

| Operation | Threads | Iters | Mean | Min | `p50` | `p90` | `p99` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| [`GcsFs::info`](../src/gcs/fs.rs) (Direct Uncached) | `1T` | `30` | `28.38 ms` | `23.21 ms` | **`28.38 ms`** | `31.00 ms` | `38.53 ms` |

---

### Part 2: Single-Threaded `1 MiB` Random Read (`1T`, `ReadPattern::Auto`)

Non-overlapping `4 MiB`-aligned random offsets across the `20 GiB` object (`read_size = 1 MiB`, `io_size = 1 MiB`):

| Approach | Threads | Reads (`Total Data`) | `p50` | `p90` | `p99` | IOPS | Throughput |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| [`CachedFs`](../src/cached.rs) Mode 1 (Drop-Behind, `Auto`, `io=1MiB`) | **`1T`** | `50` (`50 MiB`) | **`39.15 ms`** | `53.03 ms` | `128.24 ms` | **`23.9`** | **`25.03 MB/s` (`23.87 MiB/s`)** |

---

### Part 3: Full-File Sequential Read (`1T`, `24T`, `48T` — `ReadPattern::Auto`)

Each thread streams its own distinct `20 GiB` object from offset `0` to `20 GiB` (`read_size = 8 MiB`, `io_size = 16 MiB`, `max_folio_size = 16 MiB`):

| Approach | Threads | Distinct Files (`Total Data`) | Wall Time | Chunk `p50` | Chunk `p90` | Throughput |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| [`CachedFs`](../src/cached.rs) Mode 1 (`Auto`) | **`1T`** | `1 × 20 GiB` (**`20 GiB`**) | **`5.23 s`** | **`0.00 ms`** | `0.11 ms` | **`4,106.42 MB/s` (`3,916.18 MiB/s`)** |
| [`CachedFs`](../src/cached.rs) Mode 1 (`Auto`) | **`24T`** | `24 × 20 GiB` (**`480 GiB`**) | **`33.03 s`** | **`0.00 ms`** | `49.88 ms` | **`15,604.48 MB/s` (`14,881.59 MiB/s`)** |
| [`CachedFs`](../src/cached.rs) Mode 1 (`Auto`) | **`48T`** | `48 × 20 GiB` (**`960 GiB`**) | **`49.14 s`** | **`0.00 ms`** | `75.63 ms` | **`20,976.12 MB/s` (`20,004.39 MiB/s`)** |

---

### Part 4: Single-Threaded `200 MiB` Write (`1T`, `5` Iterations)

Single-threaded `200 MiB` (`209,715,200` bytes) object upload to `gs://princer-flat/bench/bench_20GB_write_200MB.bin`:

| Approach | Threads | Iters | Mean | `p50` | `p90` | Throughput (at `p50`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| [`CachedFs::open(write)`](../src/cached.rs) + `write(8 MiB)` + `close` (`200 MiB`) | **`1T`** | `5` | `2.14 s` | **`2.14 s`** | `2.17 s` | **`97.88 MB/s` (`93.34 MiB/s`)** |
| [`CachedFs::pipe_file`](../src/cached.rs) (One-Shot `200 MiB` Upload) | **`1T`** | `5` | `1.34 s` | **`1.34 s`** | `1.71 s` | **`156.46 MB/s` (`149.21 MiB/s`)** |

---

## 3. Reproducing the Benchmarks

```bash
# Run full benchmark suite (info, 1T random read, 1T/24T/48T sequential read, 1T 200 MiB write)
cargo run --release --example bench_live_readahead -- --bench-20gb gs://princer-flat/bench/bench_20GB.bin

# Run an individual section: --only <all|info|random|seq|seq-1t|seq-24t|seq-48t|write>
cargo run --release --example bench_live_readahead -- --bench-20gb --only write gs://princer-flat/bench/bench_20GB.bin
```

