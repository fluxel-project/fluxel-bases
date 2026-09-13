//! Immutable snapshots and explicit observable lifecycle state.

use std::sync::Arc;

use crate::{AssetHandle, AssetId, AssetKind, ResidentBytes};

/// Monotonic generation of successfully committed content for one identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ContentGeneration(pub(crate) u64);

impl ContentGeneration {
    /// Returns the diagnostic generation number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic generation of a production attempt for one identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AttemptGeneration(pub(crate) u64);

impl AttemptGeneration {
    /// Returns the diagnostic attempt number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Immutable ownership of one exact committed content generation.
pub struct AssetSnapshot<K: AssetKind, V> {
    pub(crate) handle: AssetHandle<K>,
    pub(crate) generation: ContentGeneration,
    pub(crate) value: Arc<V>,
    pub(crate) bytes: ResidentBytes,
}

impl<K: AssetKind, V> AssetSnapshot<K, V> {
    /// Returns the logical identity shared across content replacements.
    #[must_use]
    pub fn id(&self) -> AssetId<K> {
        self.handle.id()
    }

    /// Returns this snapshot's immutable content generation.
    #[must_use]
    pub const fn generation(&self) -> ContentGeneration {
        self.generation
    }

    /// Borrows the immutable content value.
    #[must_use]
    pub fn value(&self) -> &V {
        &self.value
    }

    /// Clones ownership of the immutable content value.
    #[must_use]
    pub fn value_arc(&self) -> Arc<V> {
        Arc::clone(&self.value)
    }

    /// Returns caller-reported resident bytes for this value.
    #[must_use]
    pub const fn resident_bytes(&self) -> ResidentBytes {
        self.bytes
    }

    /// Borrows the strong logical handle retained by the snapshot.
    #[must_use]
    pub const fn handle(&self) -> &AssetHandle<K> {
        &self.handle
    }
}

impl<K: AssetKind, V> Clone for AssetSnapshot<K, V> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            generation: self.generation,
            value: Arc::clone(&self.value),
            bytes: self.bytes,
        }
    }
}

/// A typed production failure, optionally preserving the previous ready value.
pub struct ProductionFailure<K: AssetKind, V, E> {
    pub(crate) id: AssetId<K>,
    pub(crate) attempt: AttemptGeneration,
    pub(crate) error: Arc<E>,
    pub(crate) previous: Option<AssetSnapshot<K, V>>,
}

impl<K: AssetKind, V, E> ProductionFailure<K, V, E> {
    /// Returns the identity whose production failed.
    #[must_use]
    pub const fn id(&self) -> AssetId<K> {
        self.id
    }

    /// Returns the failed attempt generation.
    #[must_use]
    pub const fn attempt(&self) -> AttemptGeneration {
        self.attempt
    }

    /// Borrows the caller-provided typed failure.
    #[must_use]
    pub fn error(&self) -> &E {
        &self.error
    }

    /// Clones ownership of the typed failure.
    #[must_use]
    pub fn error_arc(&self) -> Arc<E> {
        Arc::clone(&self.error)
    }

    /// Returns the previous committed snapshot after a failed replacement.
    #[must_use]
    pub const fn previous(&self) -> Option<&AssetSnapshot<K, V>> {
        self.previous.as_ref()
    }
}

impl<K: AssetKind, V, E> Clone for ProductionFailure<K, V, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            attempt: self.attempt,
            error: Arc::clone(&self.error),
            previous: self.previous.clone(),
        }
    }
}

/// Explicit current state observed without starting production or retry.
pub enum AssetObservation<K: AssetKind, V, E> {
    /// No content has been committed and no attempt is active.
    Missing { id: AssetId<K> },
    /// One attempt is active; replacement may preserve a current snapshot.
    Producing {
        id: AssetId<K>,
        attempt: AttemptGeneration,
        current: Option<AssetSnapshot<K, V>>,
    },
    /// One immutable content generation is ready.
    Ready(AssetSnapshot<K, V>),
    /// The latest attempt failed; replacement may preserve a previous value.
    Failed(ProductionFailure<K, V, E>),
}
