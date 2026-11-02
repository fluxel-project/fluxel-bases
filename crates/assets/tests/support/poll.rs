//! Shared polling helpers for public waiter-contract integration tests.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use fluxel_assets::{AssetError, AssetKind, ProductionOutcome, ProductionWaiter};

pub fn poll_waiter<K: AssetKind, V, E>(
    waiter: &mut ProductionWaiter<K, V, E>,
) -> Poll<Result<ProductionOutcome<K, V, E>, AssetError>> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Pin::new(waiter).poll(&mut context)
}
