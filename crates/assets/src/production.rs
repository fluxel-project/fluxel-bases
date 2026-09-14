//! Single-producer capabilities and executor-neutral observation.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use crate::runtime::{AttemptCell, StoreInner, WaiterRegistration};
use crate::{
    AssetError, AssetId, AssetKind, AssetSnapshot, AttemptGeneration, ProductionFailure,
    ResidentBytes,
};

/// Result of acquiring the current logical identity state.
pub enum Acquire<K: AssetKind, V, E> {
    /// Current content was already committed.
    Ready(AssetSnapshot<K, V>),
    /// This caller exclusively owns the new production attempt.
    Producer(ProducerPermit<K, V, E>),
    /// Another caller owns the same active attempt.
    Waiter(ProductionWaiter<K, V, E>),
    /// The latest attempt failed; retry remains explicit.
    Failed(ProductionFailure<K, V, E>),
}

/// Result of explicitly requesting production when no ready fast path applies.
pub enum Production<K: AssetKind, V, E> {
    /// This caller exclusively owns the attempt.
    Producer(ProducerPermit<K, V, E>),
    /// Another caller already owns the active attempt.
    Waiter(ProductionWaiter<K, V, E>),
}

/// Terminal result observed by all waiters for one exact attempt.
pub enum ProductionOutcome<K: AssetKind, V, E> {
    /// The attempt atomically committed a new content generation.
    Ready(AssetSnapshot<K, V>),
    /// The attempt committed a typed failure.
    Failed(ProductionFailure<K, V, E>),
    /// The producer ended without a value or typed failure.
    Cancelled {
        /// Logical identity whose attempt was cancelled.
        id: AssetId<K>,
        /// Exact attempt that was cancelled.
        attempt: AttemptGeneration,
        /// Previous value preserved after replacement cancellation.
        previous: Option<AssetSnapshot<K, V>>,
    },
}

/// Exclusive capability to complete one exact production attempt.
#[must_use = "dropping an unfinished producer cancels its attempt"]
pub struct ProducerPermit<K: AssetKind, V, E> {
    pub(crate) id: AssetId<K>,
    pub(crate) attempt: AttemptGeneration,
    pub(crate) store: Weak<StoreInner<K, V, E>>,
    pub(crate) cell: Arc<AttemptCell<K, V, E>>,
    pub(crate) completed: bool,
}

impl<K, V, E> ProducerPermit<K, V, E>
where
    K: AssetKind,
    V: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    /// Returns the identity being produced.
    #[must_use]
    pub const fn id(&self) -> AssetId<K> {
        self.id
    }

    /// Returns the exact attempt generation guarded by this permit.
    #[must_use]
    pub const fn attempt(&self) -> AttemptGeneration {
        self.attempt
    }

    /// Atomically commits immutable content and caller-reported resident bytes.
    pub fn commit(
        mut self,
        value: V,
        resident_bytes: ResidentBytes,
    ) -> Result<AssetSnapshot<K, V>, AssetError> {
        let result = self.store.upgrade().ok_or(AssetError::StoreClosed)?.commit(
            self.id,
            self.attempt,
            &self.cell,
            value,
            resident_bytes,
        );
        self.completed = result.is_ok();
        result
    }

    /// Atomically commits a typed production failure.
    pub fn fail(mut self, error: E) -> Result<ProductionFailure<K, V, E>, AssetError> {
        let result = self.store.upgrade().ok_or(AssetError::StoreClosed)?.fail(
            self.id,
            self.attempt,
            &self.cell,
            error,
        );
        self.completed = result.is_ok();
        result
    }

    /// Explicitly cancels the active attempt.
    pub fn cancel(mut self) -> Result<(), AssetError> {
        let result = self.store.upgrade().ok_or(AssetError::StoreClosed)?.cancel(
            self.id,
            self.attempt,
            &self.cell,
        );
        self.completed = result.is_ok();
        result
    }
}

impl<K, V, E> Drop for ProducerPermit<K, V, E>
where
    K: AssetKind,
{
    fn drop(&mut self) {
        if !self.completed {
            if let Some(store) = self.store.upgrade() {
                let _ = store.cancel(self.id, self.attempt, &self.cell);
            }
        }
    }
}

/// Shared observer for one exact in-progress attempt.
///
/// Every waiter, including every clone, holds its own registration slot in the
/// attempt's registry; dropping the waiter deregisters that slot so a pending
/// producer cannot retain the waiter's waker.
#[must_use = "a waiter must be polled or awaited to observe completion"]
pub struct ProductionWaiter<K: AssetKind, V, E> {
    pub(crate) id: AssetId<K>,
    pub(crate) attempt: AttemptGeneration,
    pub(crate) cell: Arc<AttemptCell<K, V, E>>,
    pub(crate) registration: WaiterRegistration,
}

impl<K: AssetKind, V, E> ProductionWaiter<K, V, E> {
    /// Returns the identity being observed.
    #[must_use]
    pub const fn id(&self) -> AssetId<K> {
        self.id
    }

    /// Returns the exact attempt generation being observed.
    #[must_use]
    pub const fn attempt(&self) -> AttemptGeneration {
        self.attempt
    }
}

impl<K: AssetKind, V, E> Future for ProductionWaiter<K, V, E> {
    type Output = Result<ProductionOutcome<K, V, E>, AssetError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        self.cell.poll(&self.registration, _context)
    }
}

impl<K: AssetKind, V, E> Clone for ProductionWaiter<K, V, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            attempt: self.attempt,
            cell: Arc::clone(&self.cell),
            registration: self.cell.register(),
        }
    }
}

impl<K: AssetKind, V, E> Drop for ProductionWaiter<K, V, E> {
    fn drop(&mut self) {
        self.cell.deregister(&self.registration);
    }
}

impl<K: AssetKind, V, E> Clone for ProductionOutcome<K, V, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Ready(snapshot) => Self::Ready(snapshot.clone()),
            Self::Failed(failure) => Self::Failed(failure.clone()),
            Self::Cancelled {
                id,
                attempt,
                previous,
            } => Self::Cancelled {
                id: *id,
                attempt: *attempt,
                previous: previous.clone(),
            },
        }
    }
}
