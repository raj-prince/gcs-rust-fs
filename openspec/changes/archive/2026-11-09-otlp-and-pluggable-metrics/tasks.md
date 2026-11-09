# Tasks: OTLP and Pluggable Metrics Facade

## 1. Metrics Facade Definition

- [x] 1.1 Create `src/metrics.rs` defining the `MetricsRecorder` trait, `Direction`, `Status`, and zero-overhead `NoopMetricsRecorder` with unit tests verifying no-op dispatch.
- [x] 1.2 Add `with_metrics_recorder(Arc<dyn MetricsRecorder>)` to `GcsFsBuilder` and store the recorder on `GcsFs`.

## 2. Core Operation Instrumentation

- [x] 2.1 Instrument `FileSystem::get_file` in `src/gcs/fs.rs` to record total read bytes, total operation duration, slice download durations, and in-flight stream count.
- [x] 2.2 Instrument `FileSystem::put_file` and streaming writers in `src/gcs/fs.rs` to record written bytes, upload duration, and in-flight stream count.
- [x] 2.3 Instrument `Reader::next_chunk` in `src/gcs/backend.rs` and `cat_file` for single-request read volume and latency metrics.
- [x] 2.4 Instrument `CachedFs` in `src/cached.rs` to emit cache hit and miss events upon folio lookups.
- [x] 2.5 Add unit tests in `src/metrics.rs` using an in-memory `MockMetricsRecorder` to verify that transfer sizes, latencies, and stream counters are accurately recorded.

## 3. OpenTelemetry OTLP Exporter Integration

- [x] 3.1 Add optional dependencies (`opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`) in `Cargo.toml` gated behind `[features] otlp = [...]`.
- [x] 3.2 Implement `OtlpMetricsRecorder` in `src/metrics/otlp.rs` mapping facade calls to standard OpenTelemetry semantic conventions (`gcs.client.*`).
- [x] 3.3 Add integration test or example demonstrating OTLP initialization with a local/test collector endpoint under `--features otlp`.

## 4. Documentation & Verification

- [x] 4.1 Update `src/README.md` and create `docs/telemetry.md` documenting GCP Metrics Explorer setup via OTLP, environment variables, and Copybara substitution rules for google3/Borg.
- [x] 4.2 Verify full test suite passes with `cargo test --all-targets` and `cargo test --features otlp`.

