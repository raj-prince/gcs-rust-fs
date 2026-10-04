# Development guide

How to build, run and test this repo. For *what* the code does, see the
crate docs (`cargo doc --open`) and [`filesystem_api_design.md`](filesystem_api_design.md).

## Prerequisites

* Rust **1.91 or newer** (`rust-version` in `Cargo.toml`, pinned to the SDK's
  MSRV) with the `rustfmt` and `clippy` components:
  `rustup component add rustfmt clippy`.
* For anything that touches a real bucket (live tests, examples): Application
  Default Credentials — `gcloud auth application-default login` — with read
  access to the object you point them at. Nothing else needs a network.

## Build

This is a **library crate**; the only binaries are the examples.

| Command | Output |
|---------|--------|
| `cargo build` | debug library (`target/debug/libgcs_rust_fs.rlib`) |
| `cargo build --release` | optimised library — what `gcsfs` links against |
| `cargo build --examples [--release]` | `target/{debug,release}/examples/{stat,cat}` |

CI builds with `RUSTFLAGS=-D warnings`, so a warning that is harmless locally
fails the pipeline.

### Running the example binaries

```bash
cargo run --example stat -- gs://my-bucket/path/to/object
cargo run --example cat  -- gs://my-bucket/path/to/object --start 0 --end 20
cargo run --example cat  -- gs://my-bucket/path/to/object --start -100 --transport http
```

`--start`/`--end` use Python-slice semantics. Pick the data transport with
`--transport grpc|http` or `GCS_RUST_FS_TRANSPORT`; bidi gRPC (the default)
must be enabled for the bucket, HTTP always works. A built binary runs
standalone: `./target/release/examples/cat gs://b/o`.

## Test

```bash
cargo test
```

runs four suites; only one needs a network:

| Suite | What it covers | Network |
|-------|----------------|---------|
| unit tests (`src/**`, `#[cfg(test)]`) | path/range parsing, error mapping, glob rules | no |
| `tests/memory_fs.rs` | an in-memory `FileSystem` implementing only the six required primitives; exercises every derived operation (`find`, `walk`, `glob`, `cat`, `rm`, `copy`, `mv`, `put`, …) | no |
| `tests/live.rs` | `GcsFs` against a real bucket. **Self-skips** — every test returns early and reports *passed* — unless `GCS_RUST_FS_TEST_OBJECT` is set | yes |
| doctests | the `///` examples in `src/` | no |

Live tests:

```bash
export GCS_RUST_FS_TEST_OBJECT=gs://my-bucket/some/file.bin   # any readable object of a few KiB+
cargo test --test live -- --nocapture                         # bidi gRPC (default)
GCS_RUST_FS_TRANSPORT=http cargo test --test live              # JSON API path
```

`--nocapture` also shows the "skipping live test" line, which is otherwise
swallowed — if a live run finishes in 0.00s, the variable was not set.

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
  path.rs                       GcsPath parsing (gs://bucket/object#generation)
tests/memory_fs.rs              reference in-memory FileSystem
tests/live.rs                   real-bucket tests (env-gated)
examples/stat.rs, cat.rs        CLI binaries
docs/filesystem_api_design.md   behavioural spec and open decisions
```

### Adding a `FileSystem` implementation

Implement the six required methods — `info`, `ls`, `open`, `rm_file`, `mkdir`,
`rmdir` — under `#[gcs_rust_fs::async_trait]`, plus a `File` for what `open`
returns. Everything else comes from `derived.rs` and may be overridden when
the backend has a native fast path (e.g. flat listing for `find`).
`tests/memory_fs.rs` is the smallest complete example.
