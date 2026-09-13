//! Public owner and entry points for the synchronized logical asset store.

use std::sync::Arc;

use crate::runtime::StoreInner;
use crate::{
    Acquire, AssetError, AssetHandle, AssetId, AssetKind, AssetObservation, AssetWeak, CacheBudget,
    CacheStats, CollectReport, Production, RemoveReport,
};

/// Thread-shareable owner of typed logical identities and current CPU content.
pub struct AssetStore<K: AssetKind, V, E> {
    inner: Arc<StoreInner<K, V, E>>,
}

impl<K, V, E> AssetStore<K, V, E>
where
    K: AssetKind,
    V: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    /// Creates an empty independent store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StoreInner::new()),
        }
    }

    /// Allocates a new missing logical identity.
    pub fn create(&self) -> Result<AssetHandle<K>, AssetError> {
        self.inner.create()
    }

    /// Acquires ready content, the unique producer, or a waiter for one attempt.
    pub fn acquire(&self, handle: &AssetHandle<K>) -> Result<Acquire<K, V, E>, AssetError> {
        self.inner.acquire(handle)
    }

    /// Explicitly retries a failed or cancelled missing identity.
    pub fn retry(&self, handle: &AssetHandle<K>) -> Result<Acquire<K, V, E>, AssetError> {
        self.inner.retry(handle)
    }

    /// Starts or joins an atomic replacement attempt for current content.
    pub fn request_replacement(
        &self,
        handle: &AssetHandle<K>,
    ) -> Result<Production<K, V, E>, AssetError> {
        self.inner.request_replacement(handle)
    }

    /// Observes state without starting production, retry, or collection.
    pub fn observe(
        &self,
        handle: &AssetHandle<K>,
    ) -> Result<AssetObservation<K, V, E>, AssetError> {
        self.inner.observe(handle)
    }

    /// Upgrades a weak handle only while its exact identity remains allocated.
    pub fn upgrade(&self, handle: &AssetWeak<K>) -> Result<Option<AssetHandle<K>>, AssetError> {
        self.inner.upgrade(handle)
    }

    /// Removes and recycles an identity after live-user and producer checks.
    pub fn remove(&self, id: AssetId<K>) -> Result<RemoveReport<K>, AssetError> {
        self.inner.remove(id)
    }

    /// Deterministically evicts eligible ready content to the fixed budget.
    pub fn collect(&self, budget: CacheBudget) -> Result<CollectReport<K>, AssetError> {
        self.inner.collect(budget)
    }

    /// Reports current ready entries and charged resident bytes.
    pub fn cache_stats(&self) -> Result<CacheStats, AssetError> {
        self.inner.cache_stats()
    }
}

impl<K: AssetKind, V, E> Clone for AssetStore<K, V, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V, E> Default for AssetStore<K, V, E>
where
    K: AssetKind,
    V: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
