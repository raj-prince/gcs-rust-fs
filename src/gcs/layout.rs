//! Bucket kinds and their detection.

use std::fmt;

use google_cloud_storage::model::StorageLayout;

/// The kind of a Cloud Storage bucket, which decides how directories are
/// implemented and which data path is available.
///
/// | Kind | Directories | Data path |
/// |------|-------------|-----------|
/// | [`Flat`](Self::Flat) | emulated from object-name prefixes and `dir/` placeholders | gRPC or HTTP |
/// | [`Hierarchical`](Self::Hierarchical) | real folders (Storage Control API); empty folders exist | gRPC or HTTP |
/// | [`Zonal`](Self::Zonal) | as hierarchical | gRPC only; objects are appendable |
///
/// Code never branches on the variant itself but on the capability it needs,
/// so each site documents *why* it differs and a future kind only has to
/// answer these questions:
///
/// | Capability | Flat | Hierarchical | Zonal |
/// |------------|------|--------------|-------|
/// | [`is_hierarchical`](Self::is_hierarchical) — directories are real folders | no | yes | yes |
/// | [`has_versioning`](Self::has_versioning) — object generations can be listed | yes | no | no |
/// | [`supports_server_copy`](Self::supports_server_copy) — `RewriteObject` is available | yes | yes | no |
/// | [`supports_http`](Self::supports_http) — the JSON API can carry object data | yes | yes | no |
/// | [`objects_appendable`](Self::objects_appendable) — objects are written as appendable | no | no | yes |
///
/// The kind is detected lazily per bucket with one `GetStorageLayout` call
/// and cached by [`GcsFs`](crate::GcsFs); see
/// [`GcsFsBuilder::bucket_kind`](crate::GcsFsBuilder::bucket_kind) to
/// pre-seed it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BucketKind {
    /// A regular bucket with a flat namespace (the default kind).
    #[default]
    Flat,
    /// A bucket with hierarchical namespace enabled.
    Hierarchical,
    /// A zonal (Rapid Storage) bucket: hierarchical, gRPC-only, appendable
    /// objects, no server-side copy.
    Zonal,
}

impl BucketKind {
    /// `true` for buckets whose directories are real folders.
    pub fn is_hierarchical(self) -> bool {
        !matches!(self, BucketKind::Flat)
    }

    /// `true` when object versioning exists, so listings can include
    /// non-current generations. Hierarchical-namespace buckets have none.
    pub fn has_versioning(self) -> bool {
        !self.is_hierarchical()
    }

    /// `true` when objects can be copied server-side (`RewriteObject`), which
    /// `copy_file` and the copy-and-delete move fallback rely on.
    pub fn supports_server_copy(self) -> bool {
        !matches!(self, BucketKind::Zonal)
    }

    /// `true` when object data may travel over the JSON API, so reads can
    /// honour [`Transport::Http`](crate::Transport::Http) and fall back to it.
    pub fn supports_http(self) -> bool {
        !matches!(self, BucketKind::Zonal)
    }

    /// `true` when objects are written as appendable objects: whole-file and
    /// streaming writes go through `BidiWriteObject`, and `open` accepts
    /// append mode.
    pub fn objects_appendable(self) -> bool {
        matches!(self, BucketKind::Zonal)
    }

    /// The canonical lower-case name (`"flat"`, `"hierarchical"`, `"zonal"`).
    pub fn as_str(self) -> &'static str {
        match self {
            BucketKind::Flat => "flat",
            BucketKind::Hierarchical => "hierarchical",
            BucketKind::Zonal => "zonal",
        }
    }

    pub(crate) fn from_layout(layout: &StorageLayout) -> Self {
        if layout.location_type == "zone" {
            BucketKind::Zonal
        } else if layout
            .hierarchical_namespace
            .as_ref()
            .is_some_and(|hns| hns.enabled)
        {
            BucketKind::Hierarchical
        } else {
            BucketKind::Flat
        }
    }
}

impl fmt::Display for BucketKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_storage::model::storage_layout::HierarchicalNamespace;

    #[test]
    fn classifies_storage_layouts() {
        let flat = StorageLayout::default().set_location_type("region");
        assert_eq!(BucketKind::from_layout(&flat), BucketKind::Flat);

        let hns = StorageLayout::default()
            .set_location_type("region")
            .set_hierarchical_namespace(HierarchicalNamespace::default().set_enabled(true));
        assert_eq!(BucketKind::from_layout(&hns), BucketKind::Hierarchical);
        assert!(BucketKind::Hierarchical.is_hierarchical());

        let zonal = StorageLayout::default()
            .set_location_type("zone")
            .set_hierarchical_namespace(HierarchicalNamespace::default().set_enabled(true));
        assert_eq!(BucketKind::from_layout(&zonal), BucketKind::Zonal);
        assert_eq!(zonal.location_type, "zone");
        assert_eq!(BucketKind::Zonal.to_string(), "zonal");
        assert!(!BucketKind::Flat.is_hierarchical());
    }

    #[test]
    fn capability_matrix() {
        use BucketKind::{Flat, Hierarchical, Zonal};
        // (kind, hierarchical, versioning, server copy, http, appendable)
        let expected = [
            (Flat, false, true, true, true, false),
            (Hierarchical, true, false, true, true, false),
            (Zonal, true, false, false, false, true),
        ];
        for (kind, hier, versioning, copy, http, appendable) in expected {
            assert_eq!(kind.is_hierarchical(), hier, "{kind}");
            assert_eq!(kind.has_versioning(), versioning, "{kind}");
            assert_eq!(kind.supports_server_copy(), copy, "{kind}");
            assert_eq!(kind.supports_http(), http, "{kind}");
            assert_eq!(kind.objects_appendable(), appendable, "{kind}");
        }
    }
}
