//! Structured failures returned by asset-core operations.

use std::fmt;

/// A contract or lifecycle failure in the logical asset store.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AssetError {
    /// The identity belongs to a different store.
    ForeignIdentity,
    /// The slot is absent or now contains another generation.
    StaleIdentity {
        /// Opaque slot number used by the rejected identity.
        slot: u32,
        /// Slot generation used by the rejected identity.
        slot_generation: u32,
    },
    /// Explicit removal was rejected while strong users remain.
    LiveUsers {
        /// Number of external strong logical users.
        strong_users: usize,
    },
    /// An attempt is already producing for this identity.
    ProductionInProgress {
        /// Current attempt generation.
        attempt: u64,
    },
    /// The operation requires an active production attempt.
    ///
    /// Retained for 0.13 API compatibility even though the current safe
    /// producer-capability surface does not construct this variant.
    NoActiveProduction,
    /// A producer capability no longer names the active attempt.
    ExpiredProducer {
        /// Attempt carried by the rejected permit.
        actual_attempt: u64,
        /// Current attempt, or `None` when production already ended.
        expected_attempt: Option<u64>,
    },
    /// The same producer permit was completed more than once.
    ///
    /// Retained for 0.13 API compatibility even though consuming completion
    /// methods make this state unreachable through the safe public API.
    ProducerAlreadyCompleted,
    /// Resident byte accounting exceeded `u64`.
    ResidentBytesOverflow,
    /// A slot, content, attempt, or store generation could not advance.
    GenerationExhausted,
    /// Internal synchronization was poisoned by a panic.
    SynchronizationPoisoned,
    /// The store was dropped before a waiter or producer completed.
    StoreClosed,
}

impl fmt::Display for AssetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignIdentity => formatter.write_str("asset identity belongs to another store"),
            Self::StaleIdentity {
                slot,
                slot_generation,
            } => write!(
                formatter,
                "asset identity is stale (slot {slot}, generation {slot_generation})"
            ),
            Self::LiveUsers { strong_users } => {
                write!(formatter, "asset has {strong_users} live strong users")
            }
            Self::ProductionInProgress { attempt } => {
                write!(
                    formatter,
                    "asset production attempt {attempt} is in progress"
                )
            }
            Self::NoActiveProduction => formatter.write_str("asset has no active production"),
            Self::ExpiredProducer {
                actual_attempt,
                expected_attempt,
            } => write!(
                formatter,
                "producer attempt {actual_attempt} expired; active attempt is {expected_attempt:?}"
            ),
            Self::ProducerAlreadyCompleted => {
                formatter.write_str("producer permit was already completed")
            }
            Self::ResidentBytesOverflow => formatter.write_str("resident byte accounting overflow"),
            Self::GenerationExhausted => formatter.write_str("asset generation space exhausted"),
            Self::SynchronizationPoisoned => {
                formatter.write_str("asset store synchronization was poisoned")
            }
            Self::StoreClosed => formatter.write_str("asset store is closed"),
        }
    }
}

impl std::error::Error for AssetError {}
