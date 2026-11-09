//! Example demonstrating how to configure GcsFs with OpenTelemetry (OTLP) metrics.
//!
//! Run with:
//! ```bash
//! cargo run --example otlp_export --features otlp
//! ```

#[cfg(feature = "otlp")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use std::time::Duration;
    use gcs_rust_fs::metrics::otlp::OtlpMetricsRecorder;
    use gcs_rust_fs::metrics::MetricsRecorder;
    use gcs_rust_fs::GcsFs;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    println!("Initializing OTLP metric exporter for {endpoint}");

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
    let recorder: Arc<dyn MetricsRecorder> = OtlpMetricsRecorder::new(&meter);

    let fs = GcsFs::builder()
        .with_metrics_recorder(recorder)
        .build()
        .await?;

    println!("GcsFs successfully initialized with OTLP telemetry recorder!");
    let _ = fs;
    Ok(())
}

#[cfg(not(feature = "otlp"))]
fn main() {
    eprintln!("This example requires the 'otlp' feature:");
    eprintln!("  cargo run --example otlp_export --features otlp");
}
