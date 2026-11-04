# proto/gcsfs/v1

## Overview
Protocol buffer definitions (v1) defining the gRPC sidecar file system service (`FileSystemService`) backed by `gcs-rust-fs`. It provides object storage operations structured as a filesystem, supporting file/directory inspection, streaming byte-range reads, and streaming atomic writes.

## Interface
- `[filesystem.proto:FileSystemService]`: gRPC service exposing `Info`, `Read`, and `Write` RPC methods
- `[filesystem.proto:Info]`: RPC `Info(InfoRequest) returns (InfoResponse)` describing path entries (file, directory, or not found)
- `[filesystem.proto:Read]`: Streaming RPC `Read(ReadRequest) returns (stream ReadResponse)` reading byte ranges or entire objects
- `[filesystem.proto:Write]`: Streaming RPC `Write(stream WriteRequest) returns (WriteResponse)` for atomic object uploads
- `[filesystem.proto:InfoRequest]`: Path query message (`path` in `bucket/key`, `gs://bucket/key`, or generation form)
- `[filesystem.proto:InfoResponse]`: Path entry response wrapping `Entry`
- `[filesystem.proto:Entry]`: Path metadata record containing path, `EntryKind`, size, and optional `ObjectStat`
- `[filesystem.proto:EntryKind]`: Enumeration of entry types (`ENTRY_KIND_UNSPECIFIED`, `ENTRY_KIND_FILE`, `ENTRY_KIND_DIRECTORY`)
- `[filesystem.proto:ObjectStat]`: Detailed GCS object metadata including generation, etag, crc32c, and custom metadata
- `[filesystem.proto:ByteRange]`: Half-open byte range `[start, end)` with slice semantics
- `[filesystem.proto:ReadOptions]`: Server-side streaming configurations (e.g. `chunk_size`)
- `[filesystem.proto:ReadRequest]`: Read specification including path, optional `ByteRange`, and `ReadOptions`
- `[filesystem.proto:ReadResponse]`: Streamed raw byte chunks (`data`)
- `[filesystem.proto:WriteMode]`: Mode flag for writes (`WRITE_MODE_OVERWRITE`, `WRITE_MODE_CREATE`)
- `[filesystem.proto:WriteSpec]`: Upload parameters message including target path, `WriteMode`, content type, metadata, and block size
- `[filesystem.proto:WriteRequest]`: Stream payload union (`WriteSpec` header or raw byte chunk `data`)
- `[filesystem.proto:WriteResponse]`: Final upload confirmation with total persisted size and resulting `ObjectStat`

## Invariants
- `[filesystem.proto:WriteRequest]`: The first message on the `Write` stream must contain `WriteSpec spec`; all subsequent messages on that stream must carry raw byte chunks `bytes data`.
- `[filesystem.proto:ByteRange]`: Range bounds follow half-open `[start, end)` semantics; negative bounds index from the end of the object, and omitted bounds default to object boundaries.
- `[filesystem.proto:WriteMode]`: When `WRITE_MODE_CREATE` is specified, the write must fail with `ALREADY_EXISTS` if the target object already exists.

## Side Effects
- `[filesystem.proto:Write]`: Streams object data to destination storage and atomically publishes the object upon stream half-close; cancels and discards upload if the stream resets or cancels.
- `[filesystem.proto:Read]`: Reads and streams binary content chunks from remote storage.

## Verification
- Run: `cargo test`
