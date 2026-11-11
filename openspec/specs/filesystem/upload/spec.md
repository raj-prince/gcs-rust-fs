# Filesystem Upload Specification

## Purpose

Enables high-throughput file uploads to Cloud Storage using parallel composite uploads, balanced component slicing, zero-copy positional streaming, atomic server-side assembly, and fail-safe temporary lifecycle management.

## Requirements

### Requirement: Automatic Parallel Composite Upload Activation
The filesystem SHALL automatically activate parallel composite uploads for files whose size meets or exceeds the composite threshold (default 64 MiB) on buckets supporting server-side object composition.

#### Scenario: Large file triggers composite upload
- **WHEN** a file of size greater than or equal to 64 MiB is uploaded via `put_file` to a bucket supporting server-side composition
- **THEN** the system uploads the file as concurrent component slices and composes them into the destination object

#### Scenario: Small file bypasses composite upload
- **WHEN** a file of size less than 64 MiB is uploaded via `put_file`
- **THEN** the system uploads the file via a direct single-stream write without creating temporary parts or invoking composition

#### Scenario: Appendable zonal buckets bypass composite upload
- **WHEN** uploading to a zonal bucket that does not support server-side composition
- **THEN** the system uses direct streaming append writes regardless of file size

#### Scenario: Streaming writes bypass composite upload
- **WHEN** an upload is initiated through streaming writes via `open` or `pipe_file`
- **THEN** the system writes directly via single-stream uploads without component slicing or composition

### Requirement: Balanced Component Slicing and Concurrency
The filesystem SHALL divide large files into balanced, evenly-sized byte ranges across concurrent component upload tasks up to a maximum of 32 components per composition call.

#### Scenario: Even component division
- **WHEN** a 200 MiB file is uploaded with 64 MiB chunk target
- **THEN** the system divides the file into 4 equal 50 MiB component slices

#### Scenario: Concurrency limit adherence
- **WHEN** component slices are uploaded
- **THEN** the number of concurrent in-flight upload requests SHALL NOT exceed the configured concurrency limit

### Requirement: Zero-Copy Positional Slicing
The filesystem SHALL stream each component slice directly from positional local file offsets into the upload stream without buffering the entire slice in memory.

#### Scenario: Bounded memory footprint during upload
- **WHEN** multiple component upload workers are actively streaming
- **THEN** each worker buffers no more than its assigned chunk boundary (default 2 MiB) in process memory

### Requirement: Atomic Composition and Precondition Enforcement
The filesystem SHALL concatenate all component parts into the target object atomically and enforce generation match preconditions.

#### Scenario: Successful atomic composition
- **WHEN** all component uploads complete successfully
- **THEN** the destination object is created atomically with a composite CRC32C matching the concatenated byte sequence

#### Scenario: Precondition failure for CreateNew
- **WHEN** `WriteMode::CreateNew` is specified and the destination object already exists
- **THEN** the composition operation fails with an already exists error and deletes all uploaded temporary parts

### Requirement: Temporary Component Lifecycle and Cleanup
The filesystem SHALL ensure that temporary component objects are deleted from the storage bucket upon completion, error, or cancellation.

#### Scenario: Immediate post-compose cleanup
- **WHEN** object composition completes successfully
- **THEN** all temporary component objects are deleted from the bucket

#### Scenario: Cleanup on upload failure or cancellation
- **WHEN** any component upload fails or the upload future is cancelled
- **THEN** any component parts already written to the bucket are deleted
