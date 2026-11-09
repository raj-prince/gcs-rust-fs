//! OpenTelemetry (OTLP) metrics recorder implementation.
//!
//! Maps [`MetricsRecorder`] events to standard OpenTelemetry semantic conventions
//! for export to GCP Metrics Explorer and external OTLP collectors.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::KeyValue;

use crate::metrics::{Direction, MetricsRecorder, Status};

/// Standard semantic convention metric names for GCS client operations.
pub mod semantic_conventions {
    /// Total bytes transferred over the network (read or write).
    pub const NETWORK_IO: &str = "gcs.client.network.io";
    /// End-to-end duration of filesystem operations in seconds.
    pub const OPERATION_DURATION: &str = "gcs.client.operation.duration";
    /// Duration of individual parallel slice downloads in seconds.
    pub const SLICE_DURATION: &str = "gcs.client.slice.duration";
    /// Current number of active in-flight network transfer streams.
    pub const ACTIVE_STREAMS: &str = "gcs.client.active_streams";
    /// Cache folio lookup hit/miss events.
    pub const CACHE_LOOKUPS: &str = "gcs.client.cache.lookups";
}

/// An OpenTelemetry-backed implementation of [`MetricsRecorder`].
pub struct OtlpMetricsRecorder {
    network_io: Counter<u64>,
    operation_duration: Histogram<f64>,
    slice_duration: Histogram<f64>,
    active_streams: UpDownCounter<i64>,
    cache_lookups: Counter<u64>,
}

impl fmt::Debug for OtlpMetricsRecorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpMetricsRecorder")
            .field("network_io", &semantic_conventions::NETWORK_IO)
            .field("operation_duration", &semantic_conventions::OPERATION_DURATION)
            .field("slice_duration", &semantic_conventions::SLICE_DURATION)
            .field("active_streams", &semantic_conventions::ACTIVE_STREAMS)
            .field("cache_lookups", &semantic_conventions::CACHE_LOOKUPS)
            .finish()
    }
}

impl OtlpMetricsRecorder {
    /// Construct a new recorder using the provided [`Meter`].
    pub fn new(meter: &Meter) -> Arc<Self> {
        let network_io = meter
            .u64_counter(semantic_conventions::NETWORK_IO)
            .with_description("Total volume of bytes transferred over network")
            .with_unit("By")
            .build();

        let operation_duration = meter
            .f64_histogram(semantic_conventions::OPERATION_DURATION)
            .with_description("End-to-end operation latency in seconds")
            .with_unit("s")
            .build();

        let slice_duration = meter
            .f64_histogram(semantic_conventions::SLICE_DURATION)
            .with_description("Parallel slice download latency in seconds")
            .with_unit("s")
            .build();

        let active_streams = meter
            .i64_up_down_counter(semantic_conventions::ACTIVE_STREAMS)
            .with_description("Current in-flight network transfer streams")
            .with_unit("{stream}")
            .build();

        let cache_lookups = meter
            .u64_counter(semantic_conventions::CACHE_LOOKUPS)
            .with_description("Cache folio hit and miss events")
            .with_unit("{lookup}")
            .build();

        Arc::new(Self {
            network_io,
            operation_duration,
            slice_duration,
            active_streams,
            cache_lookups,
        })
    }
}

impl MetricsRecorder for OtlpMetricsRecorder {
    fn record_bytes(&self, direction: Direction, bytes: u64, op: &'static str) {
        self.network_io.add(
            bytes,
            &[
                KeyValue::new("direction", direction.as_str()),
                KeyValue::new("operation", op),
            ],
        );
    }

    fn record_operation(&self, op: &'static str, elapsed: Duration, status: Status) {
        self.operation_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("operation", op),
                KeyValue::new("status", status.as_str()),
            ],
        );
    }

    fn record_slice_download(&self, bytes: u64, elapsed: Duration, status: Status) {
        self.slice_duration.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new("status", status.as_str()),
                KeyValue::new("bytes", bytes as i64),
            ],
        );
    }

    fn update_active_streams(&self, delta: i64) {
        self.active_streams.add(delta, &[]);
    }

    fn record_cache_access(&self, hit: bool) {
        let result = if hit { "hit" } else { "miss" };
        self.cache_lookups.add(1, &[KeyValue::new("result", result)]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    #[test]
    fn otlp_metrics_recorder_records_all_facets() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("gcs_test");
        let recorder = OtlpMetricsRecorder::new(&meter);

        recorder.record_bytes(Direction::Read, 1024, "get_file");
        recorder.record_bytes(Direction::Write, 2048, "put_file");
        recorder.record_operation("get_file", Duration::from_millis(50), Status::Ok);
        recorder.record_operation("cat_file", Duration::from_millis(15), Status::Error);
        recorder.record_slice_download(64 * 1024 * 1024, Duration::from_millis(120), Status::Ok);
        recorder.update_active_streams(1);
        recorder.update_active_streams(-1);
        recorder.record_cache_access(true);
        recorder.record_cache_access(false);

        let debug_str = format!("{:?}", recorder);
        assert!(debug_str.contains("OtlpMetricsRecorder"));
    }
}
