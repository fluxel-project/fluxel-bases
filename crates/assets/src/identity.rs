//! Typed logical identity and strong/weak ownership tokens.

use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Weak};

/// Marks one compile-time asset kind such as mesh data or image data.
pub trait AssetKind: Send + Sync + 'static {}

/// Opaque identity for one logical asset and slot generation.
pub struct AssetId<K: AssetKind> {
    pub(crate) store: u64,
    pub(crate) slot: u32,
    pub(crate) generation: u32,
    pub(crate) marker: PhantomData<fn() -> K>,
}

impl<K: AssetKind> AssetId<K> {
    /// Returns the opaque slot number for diagnostics and deterministic reports.
    #[must_use]
    pub const fn slot(self) -> u32 {
        self.slot
    }

    /// Returns the slot generation used to reject stale identities.
    #[must_use]
    pub const fn slot_generation(self) -> u32 {
        self.generation
    }
}

impl<K: AssetKind> Copy for AssetId<K> {}

impl<K: AssetKind> Clone for AssetId<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: AssetKind> PartialEq for AssetId<K> {
    fn eq(&self, other: &Self) -> bool {
        self.store == other.store && self.slot == other.slot && self.generation == other.generation
    }
}

impl<K: AssetKind> Eq for AssetId<K> {}

impl<K: AssetKind> std::hash::Hash for AssetId<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.store.hash(state);
        self.slot.hash(state);
        self.generation.hash(state);
    }
}

impl<K: AssetKind> PartialOrd for AssetId<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: AssetKind> Ord for AssetId<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.store, self.slot, self.generation).cmp(&(other.store, other.slot, other.generation))
    }
}

impl<K: AssetKind> fmt::Debug for AssetId<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AssetId")
            .field("slot", &self.slot)
            .field("slot_generation", &self.generation)
            .finish_non_exhaustive()
    }
}

pub(crate) struct IdentityToken<K: AssetKind> {
    pub(crate) id: AssetId<K>,
}

/// Strong ownership of a logical asset.
///
/// A strong handle keeps current cached content ineligible for collection.
pub struct AssetHandle<K: AssetKind> {
    pub(crate) token: Arc<IdentityToken<K>>,
}

impl<K: AssetKind> AssetHandle<K> {
    /// Returns this handle's logical identity.
    #[must_use]
    pub fn id(&self) -> AssetId<K> {
        self.token.id
    }

    /// Creates a weak handle that does not keep cached content live.
    #[must_use]
    pub fn downgrade(&self) -> AssetWeak<K> {
        AssetWeak {
            id: self.id(),
            token: Arc::downgrade(&self.token),
        }
    }
}

impl<K: AssetKind> Clone for AssetHandle<K> {
    fn clone(&self) -> Self {
        Self {
            token: Arc::clone(&self.token),
        }
    }
}

impl<K: AssetKind> fmt::Debug for AssetHandle<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("AssetHandle")
            .field(&self.id())
            .finish()
    }
}

/// Non-owning reference to a logical asset.
pub struct AssetWeak<K: AssetKind> {
    pub(crate) id: AssetId<K>,
    pub(crate) token: Weak<IdentityToken<K>>,
}

impl<K: AssetKind> AssetWeak<K> {
    /// Returns the identity this weak handle originally referenced.
    #[must_use]
    pub const fn id(&self) -> AssetId<K> {
        self.id
    }
}

impl<K: AssetKind> Clone for AssetWeak<K> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            token: self.token.clone(),
        }
    }
}

impl<K: AssetKind> fmt::Debug for AssetWeak<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("AssetWeak").field(&self.id).finish()
    }
}
