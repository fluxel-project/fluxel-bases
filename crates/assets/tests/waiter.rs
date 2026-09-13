//! Exact-attempt waiter completion and store-closure contracts.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use fluxel_assets::{
    Acquire, AssetError, AssetKind, AssetStore, ProductionOutcome, ProductionWaiter, ResidentBytes,
};

struct Texture;
impl AssetKind for Texture {}

fn poll_waiter<K: AssetKind, V, E>(
    waiter: &mut ProductionWaiter<K, V, E>,
) -> Poll<Result<ProductionOutcome<K, V, E>, AssetError>> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Pin::new(waiter).poll(&mut context)
}

#[test]
fn waiter_observes_the_exact_committed_attempt() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let mut waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the same attempt"),
    };
    assert!(matches!(poll_waiter(&mut waiter), Poll::Pending));

    let committed = permit.commit(vec![4, 2], ResidentBytes::new(2)).unwrap();
    match poll_waiter(&mut waiter) {
        Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) => {
            assert_eq!(snapshot.id(), committed.id());
            assert_eq!(snapshot.generation(), committed.generation());
            assert_eq!(snapshot.value(), &[4, 2]);
        }
        _ => panic!("waiter must observe the exact committed result"),
    }
}

#[test]
fn waiter_observes_the_exact_failed_attempt() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let mut waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the same attempt"),
    };
    let failed = permit.fail("bad source".to_owned()).unwrap();

    match poll_waiter(&mut waiter) {
        Poll::Ready(Ok(ProductionOutcome::Failed(outcome))) => {
            assert_eq!(outcome.id(), failed.id());
            assert_eq!(outcome.attempt(), failed.attempt());
            assert_eq!(outcome.error(), "bad source");
        }
        _ => panic!("waiter must observe the exact typed failure"),
    }
}

#[test]
fn waiter_observes_cancelled_attempt_and_drop_allows_explicit_retry() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let producer = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let mut waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the same attempt"),
    };
    let attempt = producer.attempt();
    drop(producer);

    match poll_waiter(&mut waiter) {
        Poll::Ready(Ok(ProductionOutcome::Cancelled {
            id,
            attempt: cancelled_attempt,
            previous,
        })) => {
            assert_eq!(id, handle.id());
            assert_eq!(cancelled_attempt, attempt);
            assert!(previous.is_none());
        }
        _ => panic!("dropped permit must cancel its exact attempt"),
    }
    assert!(matches!(
        store.retry(&handle).unwrap(),
        Acquire::Producer(_)
    ));
}

#[test]
fn waiter_gets_store_closed_when_its_store_is_dropped() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let producer = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let mut waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the same attempt"),
    };
    drop(store);
    drop(producer);

    assert!(matches!(
        poll_waiter(&mut waiter),
        Poll::Ready(Err(AssetError::StoreClosed))
    ));
}
