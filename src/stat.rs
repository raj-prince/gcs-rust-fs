//! Object metadata.

use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use google_cloud_storage::model::Object;

/// Metadata about a GCS object, as returned by [`GcsFs::stat`](crate::GcsFs::stat).
///
/// Field names follow the JSON API / `gcsfs` conventions where they differ from
/// the gRPC proto (`time_created` rather than `create_time`, base64 `md5_hash`,
/// ...) so a bridge can populate an `fsspec` info dict directly.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct ObjectStat {
    /// Bucket name (without the `projects/_/buckets/` prefix).
    pub bucket: String,
    /// Object name (key) within the bucket.
    pub name: String,
    /// Size of the object data in bytes.
    pub size: u64,
    /// Content generation; changes whenever the data is overwritten.
    pub generation: i64,
    /// Metadata generation; changes whenever the metadata is updated.
    pub metageneration: i64,
    /// `Content-Type` of the object (may be empty).
    pub content_type: String,
    /// `Content-Encoding`, if set (e.g. `gzip`).
    pub content_encoding: Option<String>,
    /// `Cache-Control`, if set.
    pub cache_control: Option<String>,
    /// `Content-Disposition`, if set.
    pub content_disposition: Option<String>,
    /// `Content-Language`, if set.
    pub content_language: Option<String>,
    /// Storage class, e.g. `STANDARD`.
    pub storage_class: String,
    /// HTTP entity tag.
    pub etag: String,
    /// CRC32C checksum of the complete object, if available.
    pub crc32c: Option<u32>,
    /// Base64-encoded MD5 of the complete object, if available (composite
    /// objects do not have one).
    pub md5_hash: Option<String>,
    /// Cloud KMS key used to encrypt the object, if any.
    pub kms_key: Option<String>,
    /// Number of source objects for composite objects.
    pub component_count: Option<i32>,
    /// Creation time in RFC 3339 format.
    pub time_created: Option<String>,
    /// Last modification time in RFC 3339 format.
    pub updated: Option<String>,
    /// User-specified `customTime` in RFC 3339 format.
    pub custom_time: Option<String>,
    /// User-provided metadata key/value pairs.
    pub metadata: HashMap<String, String>,
}

impl ObjectStat {
    /// `<bucket>/<name>` — the `name` convention used by `gcsfs` info dicts.
    pub fn path(&self) -> String {
        format!("{}/{}", self.bucket, self.name)
    }

    /// `gs://<bucket>/<name>`.
    pub fn uri(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.name)
    }

    /// The CRC32C checksum encoded as the JSON API does: base64 of the
    /// big-endian 32-bit value.
    pub fn crc32c_base64(&self) -> Option<String> {
        self.crc32c.map(|c| BASE64.encode(c.to_be_bytes()))
    }
}

impl From<Object> for ObjectStat {
    fn from(o: Object) -> Self {
        let (crc32c, md5_hash) = match o.checksums {
            Some(c) => {
                let md5 = (!c.md5_hash.is_empty()).then(|| BASE64.encode(&c.md5_hash));
                (c.crc32c, md5)
            }
            None => (None, None),
        };
        Self {
            bucket: bucket_id(&o.bucket).to_owned(),
            name: o.name,
            size: u64::try_from(o.size).unwrap_or(0),
            generation: o.generation,
            metageneration: o.metageneration,
            content_type: o.content_type,
            content_encoding: non_empty(o.content_encoding),
            cache_control: non_empty(o.cache_control),
            content_disposition: non_empty(o.content_disposition),
            content_language: non_empty(o.content_language),
            storage_class: o.storage_class,
            etag: o.etag,
            crc32c,
            md5_hash,
            kms_key: non_empty(o.kms_key),
            component_count: (o.component_count > 0).then_some(o.component_count),
            time_created: o.create_time.map(String::from),
            updated: o.update_time.map(String::from),
            custom_time: o.custom_time.map(String::from),
            metadata: o.metadata,
        }
    }
}

fn non_empty(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
}

/// Strip the `projects/<project>/buckets/` resource prefix used by the gRPC API.
fn bucket_id(resource: &str) -> &str {
    match resource.strip_prefix("projects/") {
        Some(rest) => rest
            .split_once("/buckets/")
            .map(|(_, bucket)| bucket)
            .unwrap_or(resource),
        None => resource,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_storage::model::ObjectChecksums;

    #[test]
    fn converts_from_sdk_object() {
        let object = Object::new()
            .set_bucket("projects/_/buckets/my-bucket")
            .set_name("dir/file.bin")
            .set_size(1234)
            .set_generation(42)
            .set_metageneration(3)
            .set_content_type("application/octet-stream")
            .set_storage_class("STANDARD")
            .set_etag("CKih16GL/eUCEAE=")
            .set_checksums(
                ObjectChecksums::new()
                    .set_crc32c(0x1234_5678u32)
                    .set_md5_hash(bytes::Bytes::from_static(&[0u8; 16])),
            )
            .set_metadata([("k".to_string(), "v".to_string())]);

        let stat = ObjectStat::from(object);
        assert_eq!(stat.bucket, "my-bucket");
        assert_eq!(stat.name, "dir/file.bin");
        assert_eq!(stat.path(), "my-bucket/dir/file.bin");
        assert_eq!(stat.uri(), "gs://my-bucket/dir/file.bin");
        assert_eq!(stat.size, 1234);
        assert_eq!(stat.generation, 42);
        assert_eq!(stat.metageneration, 3);
        assert_eq!(stat.content_type, "application/octet-stream");
        assert_eq!(stat.content_encoding, None);
        assert_eq!(stat.storage_class, "STANDARD");
        assert_eq!(stat.crc32c, Some(0x1234_5678));
        assert_eq!(stat.crc32c_base64().as_deref(), Some("EjRWeA=="));
        assert_eq!(stat.md5_hash.as_deref(), Some("AAAAAAAAAAAAAAAAAAAAAA=="));
        assert_eq!(stat.component_count, None);
        assert_eq!(stat.metadata.get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn handles_missing_optional_fields() {
        let stat = ObjectStat::from(Object::new().set_bucket("plain-bucket").set_size(-1));
        assert_eq!(stat.bucket, "plain-bucket");
        assert_eq!(stat.size, 0);
        assert_eq!(stat.crc32c, None);
        assert_eq!(stat.md5_hash, None);
        assert_eq!(stat.updated, None);
        assert_eq!(stat.time_created, None);
    }

    #[test]
    fn strips_bucket_resource_prefix() {
        assert_eq!(bucket_id("projects/_/buckets/b"), "b");
        assert_eq!(bucket_id("projects/my-proj/buckets/b"), "b");
        assert_eq!(bucket_id("b"), "b");
        assert_eq!(bucket_id("projects/weird"), "projects/weird");
    }
}
