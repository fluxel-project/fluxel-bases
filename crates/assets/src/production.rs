//! Single-producer capabilities and executor-neutral observation.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

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
    pub(crate) marker: PhantomData<fn() -> (V, E)>,
}

impl<K: AssetKind, V, E> ProducerPermit<K, V, E> {
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
        self,
        _value: V,
        _resident_bytes: ResidentBytes,
    ) -> Result<AssetSnapshot<K, V>, AssetError> {
        unimplemented!("fluxel-assets runtime is implemented in 0.13.2")
    }

    /// Atomically commits a typed production failure.
    pub fn fail(self, _error: E) -> Result<ProductionFailure<K, V, E>, AssetError> {
        unimplemented!("fluxel-assets runtime is implemented in 0.13.2")
    }

    /// Explicitly cancels the active attempt.
    pub fn cancel(self) -> Result<(), AssetError> {
        unimplemented!("fluxel-assets runtime is implemented in 0.13.2")
    }
}

/// Shared observer for one exact in-progress attempt.
#[must_use = "a waiter must be polled or awaited to observe completion"]
pub struct ProductionWaiter<K: AssetKind, V, E> {
    pub(crate) id: AssetId<K>,
    pub(crate) attempt: AttemptGeneration,
    pub(crate) marker: PhantomData<fn() -> (V, E)>,
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
        unimplemented!("fluxel-assets runtime is implemented in 0.13.2")
    }
}
