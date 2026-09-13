//! Resident-byte overflow must finish the exact producer attempt before retry.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use fluxel_assets::{
    Acquire, AssetError, AssetKind, AssetObservation, AssetStore, ProductionOutcome,
    ProductionWaiter, ResidentBytes,
};

struct LargeAsset;
impl AssetKind for LargeAsset {}

fn poll_waiter<K: AssetKind, V, E>(
    waiter: &mut ProductionWaiter<K, V, E>,
) -> Poll<Result<ProductionOutcome<K, V, E>, AssetError>> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Pin::new(waiter).poll(&mut context)
}

#[test]
fn overflowed_commit_cancels_its_exact_attempt_and_leaves_retryable_missing_state() {
    let store = AssetStore::<LargeAsset, Vec<u8>, String>::new();
    let first = store.create().unwrap();
    let first_permit = match store.acquire(&first).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new first identity must nominate producer"),
    };
    let first_snapshot = first_permit
        .commit(Vec::new(), ResidentBytes::new(u64::MAX))
        .unwrap();
    drop(first_snapshot);

    let second = store.create().unwrap();
    let producer = match store.acquire(&second).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new second identity must nominate producer"),
    };
    let attempt = producer.attempt();
    let mut waiter = match store.acquire(&second).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the active attempt"),
    };

    assert!(matches!(
        producer.commit(vec![1], ResidentBytes::new(1)),
        Err(AssetError::ResidentBytesOverflow)
    ));
    match poll_waiter(&mut waiter) {
        Poll::Ready(Ok(ProductionOutcome::Cancelled {
            id,
            attempt: cancelled_attempt,
            previous,
        })) => {
            assert_eq!(id, second.id());
            assert_eq!(cancelled_attempt, attempt);
            assert!(previous.is_none());
        }
        _ => panic!("overflowed producer must cancel the exact waiter attempt"),
    }
    assert!(matches!(
        store.observe(&second).unwrap(),
        AssetObservation::Missing { .. }
    ));

    let retry = match store.retry(&second).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("cancelled overflow attempt must be explicitly retryable"),
    };
    retry.commit(Vec::new(), ResidentBytes::new(0)).unwrap();
    assert!(matches!(store.acquire(&second).unwrap(), Acquire::Ready(_)));
}
