//! Metadata and control-plane operations: listing, buckets, HNS folders,
//! delete, server-side copy and move. Extends [`Backend`].

use google_cloud_gax::paginator::{ItemPaginator as _, Paginator as _};
use google_cloud_lro::Poller as _;
use google_cloud_storage::model::bucket::iam_config::UniformBucketLevelAccess;
use google_cloud_storage::model::bucket::{
    CustomPlacementConfig, HierarchicalNamespace, IamConfig,
};
use google_cloud_storage::model::{Bucket, Folder, Object};
use tracing::debug;

use crate::error::{Error, ErrorKind, Result};
use crate::gcs::backend::Backend;
use crate::gcs::path::GcsPath;

/// How to create a bucket with [`GcsFs::create_bucket`](crate::GcsFs::create_bucket).
///
/// The defaults create a regular (flat) bucket in the service's default
/// location. Setting `zone` creates a zonal (Rapid Storage) bucket, which
/// implies `hierarchical` and storage class `RAPID`; `location` must then be
/// the zone's region.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BucketSpec {
    /// Location such as `US`, `EU` or `us-central1`.
    pub location: Option<String>,
    /// Zone such as `us-central1-a`; creates a zonal bucket.
    pub zone: Option<String>,
    /// Enable the hierarchical namespace (real folders). Implied by `zone`.
    pub hierarchical: bool,
    /// Storage class such as `STANDARD`, `NEARLINE` or `RAPID`.
    pub storage_class: Option<String>,
}

/// Parameters of one `ListObjects` call.
#[derive(Clone, Debug)]
pub(crate) struct ListRequest<'a> {
    pub(crate) bucket: &'a str,
    /// Object-name prefix (`""` for the whole bucket, `dir/` for a directory).
    pub(crate) prefix: &'a str,
    /// Group names below the first `/` after the prefix into `prefixes`
    /// (one directory level) instead of listing the whole subtree.
    pub(crate) delimiter: bool,
    /// Include non-current generations.
    pub(crate) versions: bool,
    /// Report HNS folders (including empty ones) as `prefixes`. Requires
    /// `delimiter`.
    pub(crate) include_folders: bool,
    /// Stop after at least this many objects + prefixes were collected.
    pub(crate) limit: Option<usize>,
}

/// The result of a (possibly multi-page) listing.
#[derive(Debug, Default)]
pub(crate) struct Listing {
    pub(crate) objects: Vec<Object>,
    /// Directory names relative to the bucket, with their trailing `/`.
    pub(crate) prefixes: Vec<String>,
}

fn bucket_resource(bucket: &str) -> String {
    format!("projects/_/buckets/{bucket}")
}

fn folder_resource(bucket: &str, key: &str) -> String {
    format!(
        "projects/_/buckets/{bucket}/folders/{}",
        key.trim_end_matches('/')
    )
}

/// The key of a folder resource name (`projects/_/buckets/b/folders/a/b/` → `a/b`).
pub(crate) fn folder_key(folder: &Folder) -> &str {
    folder
        .name
        .split_once("/folders/")
        .map_or(folder.name.as_str(), |(_, key)| key)
        .trim_end_matches('/')
}

/// The id of a bucket resource name (`projects/_/buckets/b` → `b`).
pub(crate) fn bucket_id(bucket: &Bucket) -> &str {
    bucket
        .name
        .rsplit_once('/')
        .map_or(bucket.name.as_str(), |(_, id)| id)
}

impl Backend {
    // ---- objects ----------------------------------------------------------

    /// Run one listing to completion (or until `limit` is reached).
    pub(crate) async fn list_objects(&self, req: ListRequest<'_>) -> Result<Listing> {
        debug!(?req, "list_objects");
        let mut builder = self
            .control()
            .list_objects()
            .set_parent(bucket_resource(req.bucket))
            .set_prefix(req.prefix)
            .set_versions(req.versions);
        if req.delimiter {
            builder = builder
                .set_delimiter("/")
                .set_include_folders_as_prefixes(req.include_folders);
        }
        if let Some(limit) = req.limit {
            builder = builder.set_page_size(i32::try_from(limit).unwrap_or(i32::MAX));
        }
        let display = format!("{}/{}", req.bucket, req.prefix);
        let mut listing = Listing::default();
        let mut pages = builder.by_page();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| Error::storage("list", &display, e))?;
            listing.objects.extend(page.objects);
            listing.prefixes.extend(page.prefixes);
            if req
                .limit
                .is_some_and(|limit| listing.objects.len() + listing.prefixes.len() >= limit)
            {
                break;
            }
        }
        Ok(listing)
    }

    /// Look up one object by exact name through a listing. Used when
    /// `GetObject` is forbidden but listing is allowed (list-only IAM roles).
    pub(crate) async fn find_exact(&self, path: &GcsPath) -> Result<Option<Object>> {
        let listing = self
            .list_objects(ListRequest {
                bucket: path.bucket(),
                prefix: path.object(),
                delimiter: false,
                versions: false,
                include_folders: false,
                limit: Some(1),
            })
            .await?;
        Ok(listing
            .objects
            .into_iter()
            .find(|o| o.name == path.object()))
    }

    pub(crate) async fn delete_object(&self, path: &GcsPath) -> Result<()> {
        debug!(%path, "delete_object");
        let mut request = self
            .control()
            .delete_object()
            .set_bucket(path.bucket_resource())
            .set_object(path.object());
        if let Some(generation) = path.generation() {
            request = request.set_generation(generation);
        }
        request
            .send()
            .await
            .map_err(|e| Error::storage("rm_file", path, e))
    }

    /// Server-side copy, looping on the rewrite token for large objects.
    pub(crate) async fn rewrite_object(&self, src: &GcsPath, dst: &GcsPath) -> Result<Object> {
        debug!(%src, %dst, "rewrite_object");
        let mut token = String::new();
        loop {
            let mut request = self
                .control()
                .rewrite_object()
                .set_source_bucket(src.bucket_resource())
                .set_source_object(src.object())
                .set_destination_bucket(dst.bucket_resource())
                .set_destination_name(dst.object())
                .set_rewrite_token(token);
            if let Some(generation) = src.generation() {
                request = request.set_source_generation(generation);
            }
            let response = request
                .send()
                .await
                .map_err(|e| Error::storage("copy_file", src, e))?;
            if response.done {
                return Ok(response.resource.unwrap_or_default());
            }
            token = response.rewrite_token;
        }
    }

    /// Atomic rename within one bucket.
    pub(crate) async fn move_object(&self, src: &GcsPath, dst_key: &str) -> Result<Object> {
        debug!(%src, dst_key, "move_object");
        self.control()
            .move_object()
            .set_bucket(src.bucket_resource())
            .set_source_object(src.object())
            .set_destination_object(dst_key)
            .send()
            .await
            .map_err(|e| Error::storage("move_file", src, e))
    }

    // ---- buckets ----------------------------------------------------------

    pub(crate) async fn list_buckets(&self) -> Result<Vec<Bucket>> {
        let project = self.project()?;
        let mut buckets = Vec::new();
        let mut items = self
            .control()
            .list_buckets()
            .set_parent(format!("projects/{project}"))
            .by_item();
        while let Some(bucket) = items.next().await {
            buckets.push(bucket.map_err(|e| Error::storage("ls", "", e))?);
        }
        Ok(buckets)
    }

    pub(crate) async fn get_bucket(&self, bucket: &str) -> Result<Bucket> {
        self.control()
            .get_bucket()
            .set_name(bucket_resource(bucket))
            .send()
            .await
            .map_err(|e| Error::storage("stat", bucket, e))
    }

    pub(crate) async fn create_bucket(&self, name: &str, spec: &BucketSpec) -> Result<Bucket> {
        debug!(name, ?spec, "create_bucket");
        let project = self.project()?;
        let mut bucket = Bucket::default();
        if let Some(location) = &spec.location {
            bucket = bucket.set_location(location);
        }
        if let Some(class) = &spec.storage_class {
            bucket = bucket.set_storage_class(class);
        } else if spec.zone.is_some() {
            bucket = bucket.set_storage_class("RAPID");
        }
        if let Some(zone) = &spec.zone {
            bucket = bucket.set_custom_placement_config(
                CustomPlacementConfig::default().set_data_locations([zone.clone()]),
            );
        }
        if spec.hierarchical || spec.zone.is_some() {
            // HNS requires uniform bucket-level access.
            bucket = bucket
                .set_hierarchical_namespace(HierarchicalNamespace::default().set_enabled(true))
                .set_iam_config(IamConfig::default().set_uniform_bucket_level_access(
                    UniformBucketLevelAccess::default().set_enabled(true),
                ));
        }
        self.control()
            .create_bucket()
            .set_parent(format!("projects/{project}"))
            .set_bucket_id(name)
            .set_bucket(bucket)
            .send()
            .await
            .map_err(|e| Error::storage("mkdir", name, e))
    }

    /// Delete an empty bucket; a non-empty one is `DirectoryNotEmpty`.
    pub(crate) async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        debug!(bucket, "delete_bucket");
        self.control()
            .delete_bucket()
            .set_name(bucket_resource(bucket))
            .send()
            .await
            .map_err(|e| match Error::storage("rmdir", bucket, e) {
                e if e.kind() == ErrorKind::PreconditionFailed => {
                    Error::directory_not_empty(bucket)
                }
                e => e,
            })
    }

    // ---- HNS folders -------------------------------------------------------

    pub(crate) async fn get_folder(&self, bucket: &str, key: &str) -> Result<Folder> {
        self.control()
            .get_folder()
            .set_name(folder_resource(bucket, key))
            .send()
            .await
            .map_err(|e| Error::storage("stat", format!("{bucket}/{key}"), e))
    }

    /// Every folder whose name starts with `prefix` (recursively).
    pub(crate) async fn list_folders(&self, bucket: &str, prefix: &str) -> Result<Vec<Folder>> {
        debug!(bucket, prefix, "list_folders");
        let mut folders = Vec::new();
        let mut items = self
            .control()
            .list_folders()
            .set_parent(bucket_resource(bucket))
            .set_prefix(prefix)
            .by_item();
        while let Some(folder) = items.next().await {
            folders
                .push(folder.map_err(|e| Error::storage("list", format!("{bucket}/{prefix}"), e))?);
        }
        Ok(folders)
    }

    /// Create a folder; with `recursive` missing parents are created too.
    /// An existing folder is `AlreadyExists`, a missing parent (without
    /// `recursive`) is `PreconditionFailed`.
    pub(crate) async fn create_folder(
        &self,
        bucket: &str,
        key: &str,
        recursive: bool,
    ) -> Result<Folder> {
        debug!(bucket, key, recursive, "create_folder");
        self.control()
            .create_folder()
            .set_parent(bucket_resource(bucket))
            .set_folder_id(format!("{}/", key.trim_end_matches('/')))
            .set_recursive(recursive)
            .send()
            .await
            .map_err(|e| Error::storage("mkdir", format!("{bucket}/{key}"), e))
    }

    /// Delete an empty folder; a non-empty one is `DirectoryNotEmpty`.
    pub(crate) async fn delete_folder(&self, bucket: &str, key: &str) -> Result<()> {
        debug!(bucket, key, "delete_folder");
        let display = format!("{bucket}/{key}");
        self.control()
            .delete_folder()
            .set_name(folder_resource(bucket, key))
            .send()
            .await
            .map_err(|e| match Error::storage("rmdir", &display, e) {
                e if e.kind() == ErrorKind::PreconditionFailed => {
                    Error::directory_not_empty(&display)
                }
                e => e,
            })
    }

    /// Atomically rename a folder (and everything below it) within a bucket.
    /// This is a long-running operation; the call returns once it completed.
    pub(crate) async fn rename_folder(
        &self,
        bucket: &str,
        src_key: &str,
        dst_key: &str,
    ) -> Result<()> {
        debug!(bucket, src_key, dst_key, "rename_folder");
        self.control()
            .rename_folder()
            .set_name(folder_resource(bucket, src_key))
            .set_destination_folder_id(format!("{}/", dst_key.trim_end_matches('/')))
            .poller()
            .until_done()
            .await
            .map(|_| ())
            .map_err(|e| Error::storage("mv", format!("{bucket}/{src_key}"), e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_name_helpers() {
        assert_eq!(bucket_resource("b"), "projects/_/buckets/b");
        assert_eq!(
            folder_resource("b", "a/b/"),
            "projects/_/buckets/b/folders/a/b"
        );
        let folder = Folder::default().set_name("projects/_/buckets/b/folders/x/y/");
        assert_eq!(folder_key(&folder), "x/y");
        let bucket = Bucket::default().set_name("projects/_/buckets/my-bucket");
        assert_eq!(bucket_id(&bucket), "my-bucket");
    }
}
