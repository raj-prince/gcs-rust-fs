//! The Google Cloud Storage implementation.
//!
//! Everything that knows about buckets, generations, gRPC descriptors or the
//! JSON API lives below this module. The rest of the crate — the
//! [`FileSystem`](crate::FileSystem) and [`File`](crate::File) contracts and
//! their derived operations — is storage-agnostic.
//!
//! | Module    | Role |
//! |-----------|------|
//! | `backend` | private SDK plumbing: clients, transports, raw reads |
//! | `fs`      | [`GcsFs`] / [`GcsFsBuilder`]: file-system semantics over the backend |
//! | `file`    | [`GcsFile`]: an opened object |

pub(crate) mod backend;
pub(crate) mod file;
pub(crate) mod fs;

pub use backend::Transport;
pub use file::GcsFile;
pub use fs::{GcsFs, GcsFsBuilder};
