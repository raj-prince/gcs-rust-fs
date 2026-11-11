# Spec Delta: filesystem/listing

## Purpose

Provides high-performance, adaptive directory listing and recursive traversal for Google Cloud Storage with automatic workload detection, concurrent prefix crawling, flat range partitioning, and incremental entry streaming.

## ADDED Requirements

### Requirement: Adaptive Workload Classification
The filesystem SHALL inspect the initial page response of an object storage listing probe and automatically classify the target into a single-page leaf, a hierarchical nested tree, or a massive flat prefix with zero manual configuration.

#### Scenario: Small leaf directory fast-path
- **WHEN** an initial listing probe returns no sub-prefixes and no continuation token
- **THEN** the system returns the directory entries immediately in a single round-trip without allocating worker pools

#### Scenario: Hierarchical tree detection
- **WHEN** an initial listing probe response contains one or more common prefixes or folders
- **THEN** the system transitions the listing task to the concurrent hierarchical prefix crawler

#### Scenario: Massive flat prefix detection
- **WHEN** an initial listing probe returns a full page of objects and a continuation token with no sub-prefixes
- **THEN** the system transitions the listing task to flat prefix range partitioning or pipelined prefetching

### Requirement: Concurrent Hierarchical Prefix Crawling
The filesystem SHALL traverse nested directories and common prefixes concurrently using a bounded asynchronous worker pool.

#### Scenario: Parallel nested subfolder discovery
- **WHEN** a recursive directory traversal encounters multiple subdirectories or prefixes
- **THEN** workers process discovered subdirectories concurrently up to the configured concurrency limit

#### Scenario: Dynamic worker drain and completion
- **WHEN** all discovered prefixes have been visited and the work queue is empty
- **THEN** concurrent workers drain cleanly and return the aggregated results

### Requirement: Massive Flat Prefix Partitioning
The filesystem SHALL accelerate listing massive flat prefixes by issuing parallel requests across disjoint lexicographical intervals or pipelining continuation pages.

#### Scenario: Lexicographical range partition execution
- **WHEN** a flat prefix exceeds single-page listing thresholds
- **THEN** the system splits the key space and queries disjoint lexicographical ranges concurrently

#### Scenario: Deduplicated complete prefix coverage
- **WHEN** parallel range partitions complete
- **THEN** all objects within the prefix are yielded without omissions or duplicate entries

### Requirement: Incremental Entry Streaming API
The filesystem SHALL expose streaming listing interfaces that yield entry items asynchronously as pages and subdirectories arrive.

#### Scenario: Immediate time-to-first-entry
- **WHEN** a client initiates a streaming find or directory list operation
- **THEN** entries from the first page are yielded before subsequent pages or subdirectories complete

#### Scenario: Bounded memory footprint
- **WHEN** streaming a directory tree containing a large number of objects
- **THEN** memory consumption remains bounded by in-flight worker page buffers rather than the total count of objects

### Requirement: Metadata Read Masking
The filesystem SHALL project only necessary object fields during fast listing operations to minimize payload size and serialization overhead.

#### Scenario: Projected attribute retrieval
- **WHEN** fast listing queries are executed against Cloud Storage
- **THEN** the system requests only necessary fields (name, size, generation, updated, and metadata attributes) via `read_mask`

