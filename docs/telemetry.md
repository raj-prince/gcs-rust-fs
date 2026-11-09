# Telemetry & Observability Guide

`gcs-rust-fs` provides a zero-overhead, pluggable metrics architecture designed for dual deployment:
1. **Public Open Source**: Export metrics via standard OpenTelemetry (OTLP) to GCP Cloud Monitoring / GCP Metrics Explorer or any standard OTLP collector.
2. **Google Internal (`google3`)**: Integrate seamlessly into Borg jobs emitting Monarch / Borgvars telemetry with zero modifications to filesystem call sites.

---

## 1. Architecture: Port-and-Adapter Facade

Observability is decoupled from concrete telemetry backends via the `MetricsRecorder` trait:

```text
  ┌─────────────────────────────────────────────────────────────┐
  │         gcs-rust-fs Core Filesystem Call Sites              │
  │ (get_file, put_file, cat_file, Reader, GcsFile, CachedFs)   │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                     Arc<dyn MetricsRecorder>
                                 │
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
┌──────────────────┐   ┌───────────────────┐    ┌──────────────────┐
│NoopMetricsRecorder│   │OtlpMetricsRecorder│    │ MonarchRecorder  │
│ (zero overhead,  │   │ (Open Source OTLP,│    │ (Google3 / Borg, │
│  default)        │   │  feature = "otlp")│    │  Copybara swap)  │
└──────────────────┘   └───────────────────┘    └──────────────────┘
```

- When no metrics recorder is configured, `GcsFs` and `CachedFs` default to `NoopMetricsRecorder`, executing inlined no-ops with zero memory allocations and zero network overhead.
- Transfer calls are instrumented at operation boundaries, slice boundaries (default 128 MiB), and coalesced write boundaries (2 MiB), ensuring telemetry overhead is $<1\mu\text{s}$ per operation.

---

## 2. Standard Metric Conventions

When using `OtlpMetricsRecorder` (gated by `feature = "otlp"`), metrics conform to OpenTelemetry storage and network semantic conventions:

| Metric Name | Type | Unit | Attributes / Labels | Description |
| :--- | :--- | :--- | :--- | :--- |
| `gcs.client.network.io` | Counter (`u64`) | `By` (bytes) | `direction`: `read` \| `write`<br>`operation`: `get_file`, `put_file`, `cat_file`, `pipe_file`, `write`, `read_chunk` | Total volume of network bytes transferred. |
| `gcs.client.operation.duration` | Histogram (`f64`) | `s` (seconds) | `operation`: operation identifier<br>`status`: `ok` \| `error` | End-to-end operation latency. |
| `gcs.client.slice.duration` | Histogram (`f64`) | `s` (seconds) | `status`: `ok` \| `error`<br>`bytes`: slice byte length | Latency of individual parallel slice downloads in `get_file`. |
| `gcs.client.active_streams` | UpDownCounter (`i64`) | `{stream}` | None | Current number of in-flight network transfer streams. |
| `gcs.client.cache.lookups` | Counter (`u64`) | `{lookup}` | `result`: `hit` \| `miss` | Folio and directory cache lookup hit/miss events in `CachedFs`. |

---

## 3. Quick Start: Enabling OpenTelemetry OTLP

### 3.1 Cargo Dependency

Add `gcs-rust-fs` to `Cargo.toml` with the `otlp` feature enabled:

```toml
[dependencies]
gcs-rust-fs = { version = "0.1.2", features = ["otlp"] }
opentelemetry = "0.33"
opentelemetry_sdk = { version = "0.33", features = ["rt-tokio", "metrics"] }
opentelemetry-otlp = { version = "0.33", features = ["metrics", "grpc-tonic"] }
```

### 3.2 Application Initialization

Initialize the OpenTelemetry SDK and register `OtlpMetricsRecorder` with `GcsFs`:

```rust
use std::sync::Arc;
use std::time::Duration;
use gcs_rust_fs::{GcsFs, FileSystem};
use gcs_rust_fs::metrics::otlp::OtlpMetricsRecorder;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure the OTLP exporter
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(5))
        .build()?;

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(15))
        .build();

    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("gcs-rust-fs");
    let recorder = OtlpMetricsRecorder::new(&meter);

    // 2. Build GcsFs with the telemetry recorder registered
    let fs = GcsFs::builder()
        .with_metrics_recorder(recorder)
        .build()
        .await?;

    // 3. Normal filesystem operations now emit metrics automatically
    fs.cat_file("my-bucket/checkpoint.bin", Default::default(), Default::default()).await?;

    Ok(())
}
```

---

## 4. Visualizing in Google Cloud Monitoring (GCP Metrics Explorer)

To visualize metrics in GCP Metrics Explorer:

1. **Deploy OpenTelemetry Collector in GCP**:
   Deploy the OpenTelemetry Collector on GKE or Compute Engine with the `googlecloud` exporter configured:
   ```yaml
   receivers:
     otlp:
       protocols:
         grpc:
           endpoint: 0.0.0.0:4317

   exporters:
     googlecloud:
       metric:
         prefix: custom.googleapis.com

   service:
     pipelines:
       metrics:
         receivers: [otlp]
         exporters: [googlecloud]
   ```

2. **Configure Application**:
   Point `OTEL_EXPORTER_OTLP_ENDPOINT` to the collector:
   ```bash
   export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.monitoring.svc.cluster.local:4317"
   ```

3. **Navigate to Metrics Explorer**:
   - Go to Google Cloud Console $\to$ **Monitoring** $\to$ **Metrics Explorer**.
   - Under **Metric**, select:
     - `custom.googleapis.com/gcs.client.network.io`
     - `custom.googleapis.com/gcs.client.operation.duration`
     - `custom.googleapis.com/gcs.client.slice.duration`
     - `custom.googleapis.com/gcs.client.active_streams`
     - `custom.googleapis.com/gcs.client.cache.lookups`
   - Group by `direction`, `operation`, or `status` to inspect real-time throughput and latency percentiles (p50, p95, p99).

---

## 5. Google3 / Borg Copybara Integration

When importing `gcs-rust-fs` into Google3 for Borg execution:

1. **Port Decoupling**: Core filesystem logic in `src/gcs/` and `src/cached.rs` depends only on `MetricsRecorder` trait methods (`record_bytes`, `record_operation`, `record_slice_download`, `update_active_streams`, `record_cache_access`).
2. **Copybara Substitution Rule**:
   - Replace `src/metrics/otlp.rs` with `src/metrics/monarch.rs` (or map internal Monarch `stats::EventMetric` / Borgvars to `MetricsRecorder`).
   - The open-source `Cargo.toml` feature `otlp` is simply omitted in Google3 `BUILD` files, meaning no external OTLP crates are imported into the internal workspace.
3. **Zero Call Site Divergence**: Because all recording calls pass through the facade, zero internal diffs are generated across `get_file`, `put_file`, `cat_file`, or cache folio management.

