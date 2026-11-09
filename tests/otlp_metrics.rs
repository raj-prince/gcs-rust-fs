//! Integration tests for OpenTelemetry (OTLP) metrics integration.

#![cfg(feature = "otlp")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::fixture;
use gcs_rust_fs::metrics::otlp::OtlpMetricsRecorder;
use gcs_rust_fs::metrics::{Direction, MetricsRecorder, Status};
use gcs_rust_fs::{CacheConfig, CachedFs, FileSystem, WriteOptions};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

#[tokio::test]
async fn otlp_recorder_integration_with_cached_fs() {
    let provider = SdkMeterProvider::builder().build();
    let meter = provider.meter("gcs_otlp_test");
    let recorder: Arc<dyn MetricsRecorder> = OtlpMetricsRecorder::new(&meter);

    let mem = fixture();
    let cached = CachedFs::new(mem, CacheConfig::default())
        .with_metrics_recorder(Arc::clone(&recorder));

    // Perform operations through CachedFs
    cached
        .pipe_file(
            "b/test.bin",
            Bytes::from_static(b"hello world"),
            WriteOptions::default(),
        )
        .await
        .unwrap();

    let data = cached
        .cat_file("b/test.bin", Default::default(), Default::default())
        .await
        .unwrap();
    assert_eq!(&data[..], b"hello world");

    // Telemetry recorder handles events without panicking
    recorder.record_bytes(Direction::Read, 11, "cat_file");
    recorder.record_operation("cat_file", Duration::from_millis(5), Status::Ok);
    recorder.record_slice_download(11, Duration::from_millis(2), Status::Ok);
    recorder.update_active_streams(1);
    recorder.update_active_streams(-1);
}

#[tokio::test]
async fn otlp_builder_with_mock_collector() {
    // Verify building an OTLP metric exporter pipeline configured for a collector endpoint
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint("http://127.0.0.1:4317")
        .with_timeout(Duration::from_secs(3))
        .build();

    assert!(
        exporter.is_ok(),
        "OTLP exporter with tonic should build successfully"
    );
    let exporter = exporter.unwrap();
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(60))
        .build();

    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("gcs_test_pipeline");
    let recorder = OtlpMetricsRecorder::new(&meter);

    recorder.record_bytes(Direction::Write, 1048576, "put_file");
    recorder.record_operation("put_file", Duration::from_millis(250), Status::Ok);
    recorder.record_slice_download(1048576, Duration::from_millis(150), Status::Ok);
}
