# Proposal: OTLP and Pluggable Metrics Facade

## Why
When integrating `gcs-rust-fs` with production monitoring (such as Google Cloud Metrics Explorer) or embedding it into higher-level applications and CLI tools (e.g., `gcloud storage`), operators and engineers require real-time visibility into transfer throughput, request latencies, in-flight stream concurrency, cache hit rates, and error distributions. Furthermore, because this codebase is mirrored into Google's internal repository (`google3`) via Copybara and executes on Borg, telemetry cannot be tightly coupled to a single open-source SDK. It requires a zero-cost, pluggable facade that exports via standard OpenTelemetry (OTLP) in open-source environments while enabling seamless substitution with Google's internal Monarch and Borg telemetry systems in `google3`.

## What Changes
- Introduce a zero-cost, abstract metrics facade trait (`MetricsRecorder`) in a new module (`src/metrics.rs`).
- Provide a default `NoopMetricsRecorder` that ensures zero heap allocation or overhead when telemetry is disabled or unconfigured.
- Instrument critical execution paths in `gcs-rust-fs`:
  - `FileSystem::get_file`: bytes read, operation duration, slice download durations, and active worker stream count.
  - `FileSystem::put_file`: bytes written, upload duration.
  - `Reader::next_chunk` and `cat_file`: request duration and transferred byte volume.
  - `CachedFs`: cache hit and miss counters, folio eviction counts.
- Add an optional `otlp` feature flag to `Cargo.toml` enabling OpenTelemetry SDK and OTLP gRPC/HTTP exporter integration (`OtlpMetricsRecorder`), allowing direct ingestion into Google Cloud Monitoring (GCP Metrics Explorer) or any standard OTLP collector.
- Structure the metrics facade and module boundaries cleanly so that Copybara transformation rules can replace or bind the recorder to internal Google3 stats (`//monitoring/metrics` or Monarch events) without altering call sites in core filesystem code.
- Add configuration hooks in `GcsFsBuilder` (e.g., `with_metrics_recorder(Arc<dyn MetricsRecorder>)`).

## Capabilities

### New Capabilities
- `telemetry/metrics`: Pluggable metrics recorder facade, instrumentation points across read/write/cache operations, and OTLP metric exporter for visualization in GCP Metrics Explorer and internal Borg/Monarch integration.

### Modified Capabilities
*(None - first specification capability in repository)*

## Impact
- **APIs**: Extends `GcsFsBuilder` with optional metrics recorder registration; core `FileSystem` trait signatures remain completely unchanged.
- **Dependencies**: Adds optional `opentelemetry` and `opentelemetry-otlp` dependencies gated behind an optional Cargo feature `otlp` (no extra dependencies for default or google3 builds).
- **Performance**: Zero-cost dispatch when using default no-op recorder. Minimal atomic/batching overhead when OTLP is enabled.
- **Copybara / google3**: Clean module separation in `src/metrics.rs` guarantees 1-to-1 adapter substitution for internal Borg telemetry.

