# Development guide

How to build, run and test this repo. For *what* the code does, see the
crate docs (`cargo doc --open`) and [`filesystem_api_design.md`](filesystem_api_design.md).

## Prerequisites

* Rust **1.91 or newer** (`rust-version` in `Cargo.toml`, pinned to the SDK's
  MSRV) with the `rustfmt` and `clippy` components:
  `rustup component add rustfmt clippy`.
* For anything that touches a real bucket (live tests, examples): Application
  Default Credentials — `gcloud auth application-default login` — with access
  to the buckets you point them at. Nothing else needs a network.

### Zonal writes need a rustc cfg

The storage SDK ships its appendable-object API (the only way to write to
zonal / Rapid Storage buckets) behind the rustc cfg
`google_cloud_unstable_storage_bidi`, not a Cargo feature. This repo turns it
on for every `cargo` invocation via [`.cargo/config.toml`](../.cargo/config.toml):

```toml
[build]
rustflags = ["--cfg", "google_cloud_unstable_storage_bidi"]
```

Two things to know:

* A `RUSTFLAGS` environment variable **replaces** that setting, so pass the
  cfg yourself when you set one: `RUSTFLAGS="-D warnings --cfg google_cloud_unstable_storage_bidi"`.
* A downstream crate (the `gcsfs` bridge) builds with *its own* config, so it
  needs the same `.cargo/config.toml` or `RUSTFLAGS`. Without the cfg the
  crate still compiles and everything works except zonal writes, which fail
  with `ErrorKind::Unsupported` and a message naming the flag.

## Build

This is a **library crate**; the only binaries are the examples.

| Command | Output |
|---------|--------|
| `cargo build` | debug library (`target/debug/libgcs_rust_fs.rlib`) |
| `cargo build --release` | optimised library — what `gcsfs` links against |
| `cargo build --examples [--release]` | `target/{debug,release}/examples/gcs` |

CI builds with `-D warnings`, so a warning that is harmless locally fails the
pipeline.

### Running the example binary

`examples/gcs.rs` is a small CLI over the `FileSystem` trait:

```bash
cargo run --example gcs -- info   gs://my-bucket/path
cargo run --example gcs -- ls     gs://my-bucket/dir [--versions]
cargo run --example gcs -- find   gs://my-bucket/dir [--withdirs] [--maxdepth N]
cargo run --example gcs -- walk   gs://my-bucket/dir
cargo run --example gcs -- du     gs://my-bucket/dir
cargo run --example gcs -- cat    gs://my-bucket/file [--start N] [--end N]
cargo run --example gcs -- get    gs://my-bucket/file ./local
cargo run --example gcs -- put    ./local gs://my-bucket/file
cargo run --example gcs -- pipe   gs://my-bucket/file "some text"
cargo run --example gcs -- cp     gs://my-bucket/src gs://my-bucket/dst [-r]
cargo run --example gcs -- mv     gs://my-bucket/src gs://my-bucket/dst [-r]
cargo run --example gcs -- rm     gs://my-bucket/path [-r]
cargo run --example gcs -- mkdir  gs://my-bucket/dir [-p] [--placeholder]
cargo run --example gcs -- rmdir  gs://my-bucket/dir
cargo run --example gcs -- kind   my-bucket          # flat | hierarchical | zonal
```

`--start`/`--end` use Python-slice semantics. Pick the data transport for
non-zonal buckets with `--transport grpc|http` or `GCS_RUST_FS_TRANSPORT`;
bidi gRPC (the default) falls back to HTTP when the service reports it
unavailable for a bucket. A built binary runs standalone:
`./target/release/examples/gcs ls gs://b/dir`.

## Test

```bash
cargo test
```

runs four suites; only one needs a network:

| Suite | What it covers | Network |
|-------|----------------|---------|
| unit tests (`src/**`, `#[cfg(test)]`) | path/range parsing, error mapping, glob rules, bucket-kind detection, entry conversion | no |
| `tests/memory_fs.rs` | an in-memory `FileSystem` implementing only the six required primitives; exercises every derived operation (`find`, `walk`, `glob`, `cat`, `rm`, `copy`, `mv`, `put`, …) | no |
| `tests/live.rs` | `GcsFs` against real buckets. **Self-skips** — every test returns early and reports *passed* — unless the variables below are set | yes |
| doctests | the `///` examples in `src/` | no |

Live tests come in two groups:

```bash
# Read-only, against any existing object of a few KiB or more:
export GCS_RUST_FS_TEST_OBJECT=gs://my-bucket/some/file.bin
cargo test --test live -- --nocapture                         # bidi gRPC (default)
GCS_RUST_FS_TRANSPORT=http cargo test --test live              # JSON API path

# Read/write scenarios, run once per bucket kind you list (any subset):
export GCS_RUST_FS_TEST_BUCKETS=flat=my-flat-bucket,hns=my-hns-bucket,zonal=my-zonal-bucket
cargo test --test live -- --nocapture
```

The read/write tests first check that each bucket really is of the declared
kind, then write only below `gcs-rust-fs-test/<pid>-<nanos>/<kind>/` and
remove that prefix at the end (also when an assertion fails). They need
object + folder permissions on those buckets and nothing on the project.

`--nocapture` also shows the "skipping live test" line, which is otherwise
swallowed — if a live run finishes in 0.00s, the variables were not set.

Handy variants:

```bash
cargo test --test memory_fs          # one suite
cargo test glob                      # filter by test name across all suites
cargo test --doc                     # doctests only
```

## Lint gate (what CI runs)

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) is exactly these
four commands; run them before pushing:

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings            # lints tests and examples too
cargo test --all-targets && cargo test --doc         # --all-targets excludes doctests, hence both
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps       # broken intra-doc links fail here
```

(All of these pick up the cfg from `.cargo/config.toml`; do not set
`RUSTFLAGS` without it.)

`cargo fmt --all` fixes formatting; `cargo clippy --fix --all-targets`
applies the lints it can.

## Docs

```bash
cargo doc --no-deps --open
```

## Layout

```
src/
  filesystem.rs, file.rs        the FileSystem / File traits — the contract
  entry.rs, options.rs,         value types used by the contract
  range.rs, stat.rs, error.rs
  derived.rs, glob.rs           fsspec semantics, written once on top of the
                                six required primitives
  gcs/                          the GCS implementation; the only code that
                                knows about buckets, gRPC or the SDK
    fs.rs                       GcsFs + GcsFsBuilder: the FileSystem impl,
                                branching on BucketKind where kinds differ
    file.rs                     GcsFile: read handle (pinned generation,
                                read-ahead) and write handle
    layout.rs                   BucketKind {Flat, Hierarchical, Zonal} detection
    path.rs                     path parsing (gs://bucket/key#generation)
    backend.rs                  SDK clients, Transport, raw ranged reads
    control.rs                  listing, buckets, HNS folders, delete, rewrite, move
    write.rs                    one-shot uploads, resumable + appendable writers
tests/memory_fs.rs              reference in-memory FileSystem
tests/live.rs                   real-bucket tests (env-gated, per bucket kind)
examples/gcs.rs                 CLI binary
docs/filesystem_api_design.md   behavioural spec and decisions
.cargo/config.toml              enables the SDK's appendable-object API
```

### Adding a `FileSystem` implementation

Implement the six required methods — `info`, `ls`, `open`, `rm_file`, `mkdir`,
`rmdir` — under `#[gcs_rust_fs::async_trait]`, plus a `File` for what `open`
returns. Everything else comes from `derived.rs` and may be overridden when
the backend has a native fast path (e.g. flat listing for `find`).
`tests/memory_fs.rs` is the smallest complete example.
