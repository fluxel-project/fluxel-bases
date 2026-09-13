//! Platform-neutral logical asset contracts for Fluxel.
//!
//! Version 0.13.2 implements the API-first contract with thread-safe
//! single-flight production, immutable snapshots, stale-identity rejection,
//! and deterministic explicit cache collection.

#![forbid(unsafe_code)]

mod budget;
mod error;
mod identity;
mod production;
mod runtime;
mod state;
mod store;

pub use budget::{
    CacheBudget, CacheStats, CollectReport, EvictedAsset, RemoveReport, ResidentBytes,
};
pub use error::AssetError;
pub use identity::{AssetHandle, AssetId, AssetKind, AssetWeak};
pub use production::{Acquire, ProducerPermit, Production, ProductionOutcome, ProductionWaiter};
pub use state::{
    AssetObservation, AssetSnapshot, AttemptGeneration, ContentGeneration, ProductionFailure,
};
pub use store::AssetStore;
