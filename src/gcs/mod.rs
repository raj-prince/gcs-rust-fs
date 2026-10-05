//! The Google Cloud Storage implementation.
//!
//! Everything that knows about buckets, generations, folders, gRPC
//! descriptors or the JSON API lives below this module. The rest of the crate
//! — the [`FileSystem`](crate::FileSystem) and [`File`](crate::File)
//! contracts and their derived operations — is storage-agnostic.
//!
//! | Module    | Role |
//! |-----------|------|
//! | `fs`      | [`GcsFs`] / [`GcsFsBuilder`]: the `FileSystem` implementation, per bucket kind |
//! | `file`    | [`GcsFile`]: the `File` implementation (reader and writer) |
//! | `layout`  | [`BucketKind`] detection (flat / hierarchical / zonal) |
//! | `path`    | parsing of `gcsfs`-style paths |
//! | `backend` | SDK clients, transports, raw reads |
//! | `control` | listing, buckets, folders, delete, server-side copy / move |
//! | `write`   | one-shot uploads and the streaming writers |

pub(crate) mod backend;
pub(crate) mod control;
pub(crate) mod file;
pub(crate) mod fs;
pub(crate) mod layout;
pub(crate) mod path;
pub(crate) mod write;

pub use backend::Transport;
pub use control::BucketSpec;
pub use file::GcsFile;
pub use fs::{GcsFs, GcsFsBuilder, PROJECT_ENV_VARS};
pub use layout::BucketKind;
