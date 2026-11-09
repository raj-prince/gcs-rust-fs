# Design: Pluggable Metrics Facade & OTLP Telemetry

## Context
See `proposal.md` for motivation. Currently, `gcs-rust-fs` emits structural logs via the `tracing` crate, but does not publish quantitative metrics (throughput counters, latency histograms, concurrency gauges, cache hit rates). Call sites in `GcsFs::get_file`, `GcsFs::put_file`, `Reader`, and `CachedFs` execute on high-throughput paths where any telemetry overhead must be negligible ($<1\mu\text{s}$ per operation).

Furthermore, the code is dual-homed:
1. Public open source / crates.io: built with Cargo, targeting GCP Cloud Monitoring via OpenTelemetry OTLP.
2. Google internal (`google3`): imported via Copybara, running on Borg, where telemetry must bind to internal Google monitoring systems (Monarch, Borgvars).

## Goals / Non-Goals

**Goals:**
- Provide a clean, zero-cost `MetricsRecorder` trait facade in `src/metrics.rs`.
- Implement `NoopMetricsRecorder` as the zero-cost default when no telemetry is configured.
- Implement an optional `OtlpMetricsRecorder` feature-gated by `feature = "otlp"` using `opentelemetry` and `opentelemetry-otlp`.
- Instrument critical transfer and caching paths (`get_file`, `put_file`, `cat_file`, `next_chunk`, `CachedFs`).
- Guarantee clean Copybara replaceability: internal Google3 builds can swap `src/metrics.rs` or provide an alternate `MetricsRecorder` implementation without altering call sites.

**Non-Goals:**
- Writing custom metric aggregation engines from scratch (we rely on standard OTel aggregators for open source and Monarch for internal).
- Profiling memory allocations or CPU profiling (this design covers metrics/counters/histograms, not tracing spans or CPU flamegraphs).
- Forcing OTel dependencies on default crate consumers.

## Decisions

### Decision 1: Port-and-Adapter (Facade) vs Direct OTel SDK Binding
- **Choice**: Abstract `MetricsRecorder` trait (`Arc<dyn MetricsRecorder>`) stored in `GcsFs` and passed to subcomponents.
- **Rationale**: Direct binding to `opentelemetry` SDK would break Google3 compilation because Google3 uses Bazel and internal Monarch metrics. An abstract trait enables a 1-to-1 adapter for Borg while keeping the open-source implementation based on standard OTLP.
- **Alternatives Considered**:
  - *Direct `opentelemetry` everywhere*: Breaks Copybara into Google3; pulls in massive dependency graph even for users who don't need telemetry.
  - *`metrics` crate facade*: An extra external dependency that still requires adapter layers for both OTel and Monarch without offering domain-specific batching.

### Decision 2: Feature-gating OTLP behind `feature = "otlp"`
- **Choice**: Keep `Cargo.toml` default dependencies lightweight. Place `opentelemetry`, `opentelemetry_sdk`, and `opentelemetry-otlp` under `[dependencies]` with `optional = true`.
- **Rationale**: Minimal compile-time impact for clients who only need the pure GCS filesystem primitives.

### Decision 3: Standard OpenTelemetry Storage Semantic Conventions
- **Choice**: Metric names adhere to OpenTelemetry semantic conventions for object storage and network I/O:
  - `gcs.client.network.io`: Total bytes read/written with attributes `direction`, `operation`, `bucket`.
  - `gcs.client.operation.duration`: Request latency histogram (seconds) with attributes `operation`, `status`, `error_type`.
  - `gcs.client.slice.duration`: Parallel slice latency histogram with attributes `slice_size_bytes`, `status`.
  - `gcs.client.active_streams`: Concurrent slice streams gauge (UpDownCounter).
  - `gcs.client.cache.lookups`: Folio hit/miss counter with attribute `result: hit | miss`.

## Risks / Trade-offs

- **[Risk] Dynamic dispatch overhead on hot loops**:
  - *Mitigation*: The `MetricsRecorder` methods are called per chunk (minimum 2 MiB coalesced write) or per slice (128 MiB) and per file operation, never per 4 KiB loop iteration. Overhead is completely negligible compared to network I/O.
- **[Risk] OTLP exporter network failure impacting transfers**:
  - *Mitigation*: OTLP export runs asynchronously in a detached background worker with non-blocking channel buffers. Network errors in telemetry collection are silently dropped and never propagate to filesystem operations.
- **[Risk] Copybara divergence between public and internal**:
  - *Mitigation*: The `MetricsRecorder` trait interface is kept minimal and stable. Core filesystem code only depends on trait methods, ensuring zero drift when Copybara transforms the backend adapter.

## Migration & Google3 Copybara Plan
- **Open Source**: Users enable `otlp` in `Cargo.toml` and configure `OTEL_EXPORTER_OTLP_ENDPOINT` (or pass custom recorder to `GcsFs::builder().with_metrics_recorder(...)`).
- **Google3**: Copybara sync rule replaces `src/metrics/otlp.rs` with `monarch.rs` (or binds internal Monarch `stats::EventMetric` to `dyn MetricsRecorder`), with zero modifications required across `src/gcs/` or `src/cached.rs`.

