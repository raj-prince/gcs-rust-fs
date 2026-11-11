//! Pluggable metrics recorder facade for observability, GCP Metrics Explorer,
//! and internal Google3 telemetry integration.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "otlp")]
pub mod otlp;

/// Direction of network data transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Inbound data received from Cloud Storage.
    Read,
    /// Outbound data transmitted to Cloud Storage.
    Write,
}

impl Direction {
    /// Return the static string representation.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Outcome status of an operation or slice download.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Status {
    /// The operation completed successfully.
    Ok,
    /// The operation failed.
    Error,
}

impl Status {
    /// Return the static string representation.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Abstract metrics recorder interface.
///
/// Implementors record transfer volumes, request latencies, stream concurrency,
/// and cache behavior. Call sites in [`crate::FileSystem`] and [`crate::GcsFs`]
/// dispatch through `Arc<dyn MetricsRecorder>`, which defaults to [`NoopMetricsRecorder`].
pub trait MetricsRecorder: Send + Sync + fmt::Debug + 'static {
    /// Record the volume of transferred bytes for an operation.
    fn record_bytes(&self, direction: Direction, bytes: u64, op: &'static str);

    /// Record the total duration and status of an operation.
    fn record_operation(&self, op: &'static str, elapsed: Duration, status: Status);

    /// Record the duration, byte size, and status of an individual parallel slice download.
    fn record_slice_download(&self, bytes: u64, elapsed: Duration, status: Status);

    /// Update the number of active in-flight streams by `delta` (+1 or -1).
    fn update_active_streams(&self, delta: i64);

    /// Record a cache folio lookup hit (`true`) or miss (`false`).
    fn record_cache_access(&self, hit: bool);
}

/// A zero-cost metrics recorder that discards all events.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetricsRecorder;

impl NoopMetricsRecorder {
    /// Create a new no-op metrics recorder wrapped in an `Arc`.
    pub fn new() -> Arc<dyn MetricsRecorder> {
        Arc::new(Self)
    }
}

impl MetricsRecorder for NoopMetricsRecorder {
    #[inline(always)]
    fn record_bytes(&self, _direction: Direction, _bytes: u64, _op: &'static str) {}

    #[inline(always)]
    fn record_operation(&self, _op: &'static str, _elapsed: Duration, _status: Status) {}

    #[inline(always)]
    fn record_slice_download(&self, _bytes: u64, _elapsed: Duration, _status: Status) {}

    #[inline(always)]
    fn update_active_streams(&self, _delta: i64) {}

    #[inline(always)]
    fn record_cache_access(&self, _hit: bool) {}
}

/// RAII guard that increments the active stream gauge on creation and decrements on drop.
#[derive(Debug)]
pub struct ActiveStreamGuard {
    metrics: Arc<dyn MetricsRecorder>,
}

impl ActiveStreamGuard {
    /// Create a new stream guard, incrementing active stream count by 1.
    pub fn new(metrics: Arc<dyn MetricsRecorder>) -> Self {
        metrics.update_active_streams(1);
        Self { metrics }
    }
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.metrics.update_active_streams(-1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

    #[derive(Debug, Default)]
    struct MockMetricsRecorder {
        read_bytes: AtomicU64,
        write_bytes: AtomicU64,
        op_count: AtomicU64,
        op_errors: AtomicU64,
        slice_count: AtomicU64,
        slice_bytes: AtomicU64,
        active_streams: AtomicI64,
        cache_hits: AtomicU64,
        cache_misses: AtomicU64,
    }

    impl MetricsRecorder for MockMetricsRecorder {
        fn record_bytes(&self, direction: Direction, bytes: u64, _op: &'static str) {
            match direction {
                Direction::Read => {
                    self.read_bytes.fetch_add(bytes, Ordering::SeqCst);
                }
                Direction::Write => {
                    self.write_bytes.fetch_add(bytes, Ordering::SeqCst);
                }
            }
        }

        fn record_operation(&self, _op: &'static str, _elapsed: Duration, status: Status) {
            self.op_count.fetch_add(1, Ordering::SeqCst);
            if status == Status::Error {
                self.op_errors.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn record_slice_download(&self, bytes: u64, _elapsed: Duration, _status: Status) {
            self.slice_count.fetch_add(1, Ordering::SeqCst);
            self.slice_bytes.fetch_add(bytes, Ordering::SeqCst);
        }

        fn update_active_streams(&self, delta: i64) {
            self.active_streams.fetch_add(delta, Ordering::SeqCst);
        }

        fn record_cache_access(&self, hit: bool) {
            if hit {
                self.cache_hits.fetch_add(1, Ordering::SeqCst);
            } else {
                self.cache_misses.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn direction_and_status_display_and_as_str() {
        assert_eq!(Direction::Read.as_str(), "read");
        assert_eq!(Direction::Write.as_str(), "write");
        assert_eq!(Direction::Read.to_string(), "read");
        assert_eq!(Direction::Write.to_string(), "write");

        assert_eq!(Status::Ok.as_str(), "ok");
        assert_eq!(Status::Error.as_str(), "error");
        assert_eq!(Status::Ok.to_string(), "ok");
        assert_eq!(Status::Error.to_string(), "error");
    }

    #[test]
    fn noop_recorder_handles_all_events_without_panic() {
        let recorder: Arc<dyn MetricsRecorder> = NoopMetricsRecorder::new();
        recorder.record_bytes(Direction::Read, 1024, "get_file");
        recorder.record_bytes(Direction::Write, 2048, "put_file");
        recorder.record_operation("get_file", Duration::from_millis(50), Status::Ok);
        recorder.record_operation("cat_file", Duration::from_millis(10), Status::Error);
        recorder.record_slice_download(64 * 1024 * 1024, Duration::from_millis(20), Status::Ok);
        recorder.update_active_streams(1);
        recorder.update_active_streams(-1);
        recorder.record_cache_access(true);
        recorder.record_cache_access(false);
    }

    #[test]
    fn mock_recorder_tracks_transfers_and_operations() {
        let mock = Arc::new(MockMetricsRecorder::default());
        let recorder: Arc<dyn MetricsRecorder> = mock.clone();

        recorder.record_bytes(Direction::Read, 1024 * 1024, "get_file");
        recorder.record_bytes(Direction::Read, 512 * 1024, "cat_file");
        recorder.record_bytes(Direction::Write, 2 * 1024 * 1024, "put_file");
        recorder.record_bytes(Direction::Write, 200 * 1024 * 1024, "put_file_composite");

        assert_eq!(mock.read_bytes.load(Ordering::SeqCst), 1536 * 1024);
        assert_eq!(mock.write_bytes.load(Ordering::SeqCst), 202 * 1024 * 1024);

        recorder.record_operation("get_file", Duration::from_millis(100), Status::Ok);
        recorder.record_operation("cat_file", Duration::from_millis(30), Status::Error);
        recorder.record_operation("put_file_composite", Duration::from_millis(1200), Status::Ok);

        assert_eq!(mock.op_count.load(Ordering::SeqCst), 3);
        assert_eq!(mock.op_errors.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mock_recorder_tracks_slice_downloads() {
        let mock = Arc::new(MockMetricsRecorder::default());
        let recorder: Arc<dyn MetricsRecorder> = mock.clone();

        recorder.record_slice_download(128 * 1024 * 1024, Duration::from_millis(400), Status::Ok);
        recorder.record_slice_download(64 * 1024 * 1024, Duration::from_millis(200), Status::Ok);

        assert_eq!(mock.slice_count.load(Ordering::SeqCst), 2);
        assert_eq!(mock.slice_bytes.load(Ordering::SeqCst), 192 * 1024 * 1024);
    }

    #[test]
    fn active_stream_guard_lifecycle() {
        let mock = Arc::new(MockMetricsRecorder::default());
        let recorder: Arc<dyn MetricsRecorder> = mock.clone();

        assert_eq!(mock.active_streams.load(Ordering::SeqCst), 0);
        {
            let _g1 = ActiveStreamGuard::new(Arc::clone(&recorder));
            assert_eq!(mock.active_streams.load(Ordering::SeqCst), 1);
            {
                let _g2 = ActiveStreamGuard::new(Arc::clone(&recorder));
                let _g3 = ActiveStreamGuard::new(Arc::clone(&recorder));
                assert_eq!(mock.active_streams.load(Ordering::SeqCst), 3);
            }
            assert_eq!(mock.active_streams.load(Ordering::SeqCst), 1);
        }
        assert_eq!(mock.active_streams.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn mock_recorder_tracks_cache_hits_and_misses() {
        let mock = Arc::new(MockMetricsRecorder::default());
        let recorder: Arc<dyn MetricsRecorder> = mock.clone();

        recorder.record_cache_access(true);
        recorder.record_cache_access(true);
        recorder.record_cache_access(false);
        recorder.record_cache_access(true);

        assert_eq!(mock.cache_hits.load(Ordering::SeqCst), 3);
        assert_eq!(mock.cache_misses.load(Ordering::SeqCst), 1);
    }
}
