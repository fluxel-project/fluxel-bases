//! Explicit resident-byte accounting and deterministic cache reports.

use crate::{AssetId, AssetKind, ContentGeneration};

/// Caller-reported resident bytes for one committed content value.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ResidentBytes(u64);

impl ResidentBytes {
    /// Creates a resident-byte measurement.
    #[must_use]
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Returns the measured byte count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Maximum current ready bytes retained by the store after collection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheBudget {
    max_resident_bytes: u64,
}

impl CacheBudget {
    /// Creates a fixed ready-content budget.
    #[must_use]
    pub const fn new(max_resident_bytes: u64) -> Self {
        Self { max_resident_bytes }
    }

    /// Returns the maximum retained ready bytes.
    #[must_use]
    pub const fn max_resident_bytes(self) -> u64 {
        self.max_resident_bytes
    }
}

/// One exact content generation removed from the ready cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvictedAsset<K: AssetKind> {
    pub(crate) id: AssetId<K>,
    pub(crate) generation: ContentGeneration,
    pub(crate) bytes: ResidentBytes,
}

impl<K: AssetKind> EvictedAsset<K> {
    /// Returns the logical identity whose cached value was evicted.
    #[must_use]
    pub const fn id(&self) -> AssetId<K> {
        self.id
    }

    /// Returns the evicted content generation.
    #[must_use]
    pub const fn generation(&self) -> ContentGeneration {
        self.generation
    }

    /// Returns the caller-reported bytes removed from store accounting.
    #[must_use]
    pub const fn resident_bytes(&self) -> ResidentBytes {
        self.bytes
    }
}

/// Deterministic result of an explicit collection pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectReport<K: AssetKind> {
    pub(crate) before: ResidentBytes,
    pub(crate) after: ResidentBytes,
    pub(crate) evicted: Vec<EvictedAsset<K>>,
}

impl<K: AssetKind> CollectReport<K> {
    /// Returns ready bytes before collection.
    #[must_use]
    pub const fn before_bytes(&self) -> ResidentBytes {
        self.before
    }

    /// Returns ready bytes retained after collection.
    #[must_use]
    pub const fn after_bytes(&self) -> ResidentBytes {
        self.after
    }

    /// Returns evictions in deterministic policy order.
    #[must_use]
    pub fn evicted(&self) -> &[EvictedAsset<K>] {
        &self.evicted
    }
}

/// Current store-owned ready-cache accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    pub(crate) ready_entries: usize,
    pub(crate) resident_bytes: ResidentBytes,
}

impl CacheStats {
    /// Returns the number of identities with a committed ready value.
    #[must_use]
    pub const fn ready_entries(self) -> usize {
        self.ready_entries
    }

    /// Returns bytes charged to current store-owned ready values.
    #[must_use]
    pub const fn resident_bytes(self) -> ResidentBytes {
        self.resident_bytes
    }
}

/// Result of explicitly removing and recycling one empty identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoveReport<K: AssetKind> {
    pub(crate) id: AssetId<K>,
    pub(crate) removed_generation: Option<ContentGeneration>,
    pub(crate) removed_bytes: ResidentBytes,
}

impl<K: AssetKind> RemoveReport<K> {
    /// Returns the removed logical identity.
    #[must_use]
    pub const fn id(self) -> AssetId<K> {
        self.id
    }

    /// Returns the committed generation removed, if the identity was ready.
    #[must_use]
    pub const fn removed_generation(self) -> Option<ContentGeneration> {
        self.removed_generation
    }

    /// Returns bytes removed from current store accounting.
    #[must_use]
    pub const fn removed_bytes(self) -> ResidentBytes {
        self.removed_bytes
    }
}
