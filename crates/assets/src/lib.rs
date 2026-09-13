//! Platform-neutral logical asset contracts for Fluxel.
//!
//! Version 0.13.1 exposes the API-first contract. Its behavioral examples and
//! tests are executable specifications for the 0.13.2 implementation; this
//! release does not yet provide the runtime.

#![forbid(unsafe_code)]

mod budget;
mod error;
mod identity;
mod production;
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
