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
}
