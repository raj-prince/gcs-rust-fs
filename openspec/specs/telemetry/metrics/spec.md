# telemetry/metrics Specification

## Purpose
Provides a zero-overhead, pluggable telemetry facade and OpenTelemetry (OTLP) metrics integration to export performance, throughput, latency, and cache metrics to GCP Metrics Explorer and internal Borg telemetry systems.

## Requirements

### Requirement: Pluggable Metrics Recorder Facade
The filesystem SHALL define an abstract `MetricsRecorder` interface that decouples internal operation instrumentation from concrete telemetry backends and SDK dependencies.

#### Scenario: Default unconfigured telemetry has zero overhead
- **WHEN** a `GcsFs` instance is constructed without an explicit metrics recorder
- **THEN** operations route to a no-op implementation with no heap allocation and zero network egress

#### Scenario: Custom recorder registration
- **WHEN** a client configures a custom `MetricsRecorder` via `GcsFsBuilder`
- **THEN** all subsequent storage, transfer, and cache operations emit telemetry events to the registered recorder

### Requirement: Network Transfer and Throughput Instrumentation
The filesystem SHALL record data transfer volume and direction across all read and write operations.

#### Scenario: Download bytes recording
- **WHEN** `FileSystem::get_file` or `cat_file` transfers bytes from GCS
- **THEN** the recorder receives the total byte count, direction marked as read, and the operation identifier

#### Scenario: Upload bytes recording
- **WHEN** `FileSystem::put_file` or streaming write transfers bytes to GCS
- **THEN** the recorder receives the total byte count, direction marked as write, and the operation identifier

### Requirement: Operation and Slice Latency Distribution
The filesystem SHALL measure and emit duration metrics for high-level operations and individual parallel slices.

#### Scenario: Successful get_file execution
- **WHEN** `FileSystem::get_file` downloads an object across parallel slices
- **THEN** the total wall time is recorded under operation duration with status OK
- **THEN** each individual slice completion emits a slice duration event with its byte size and status OK

#### Scenario: Failed operation records error status
- **WHEN** an operation fails due to network unavailability or non-existent object
- **THEN** the duration is recorded with error status and the corresponding classified error kind

### Requirement: Stream Concurrency Tracking
The filesystem SHALL maintain an active count of in-flight network streams.

#### Scenario: In-flight slice streams tracking
- **WHEN** parallel slice workers are dispatched during `get_file`
- **THEN** the active streams gauge increments upon worker dispatch and decrements upon worker completion

### Requirement: Cache Hit and Eviction Metrics
The filesystem SHALL emit cache access metrics when `CachedFs` is enabled.

#### Scenario: Cache lookup accounting
- **WHEN** a read request queries `CachedFs`
- **THEN** cache hit or miss events are recorded according to folio availability

### Requirement: OpenTelemetry OTLP Exporter Integration
When compiled with the `otlp` feature, the system SHALL support exporting recorded metrics via OTLP to a standard collector or Google Cloud Monitoring.

#### Scenario: OTLP exporter emission
- **WHEN** `otlp` feature is enabled and an OTLP endpoint is configured
- **THEN** metrics are periodically aggregated and exported in OTLP format with standard semantic conventions

