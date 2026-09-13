//! Public lifecycle edge cases: replacement retention, removal, and stale IDs.

use fluxel_assets::{
    Acquire, AssetError, AssetKind, AssetObservation, AssetStore, Production, ProductionOutcome,
    ResidentBytes,
};

struct Material;
impl AssetKind for Material {}

fn ready_store() -> (
    AssetStore<Material, Vec<u8>, String>,
    fluxel_assets::AssetHandle<Material>,
) {
    let store = AssetStore::new();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let initial = permit.commit(vec![1], ResidentBytes::new(1)).unwrap();
    drop(initial);
    (store, handle)
}

#[test]
fn replacement_commit_fail_and_cancel_preserve_the_expected_snapshot() {
    let (store, handle) = ready_store();
    let first = match store.observe(&handle).unwrap() {
        AssetObservation::Ready(snapshot) => snapshot,
        _ => panic!("initial content must be ready"),
    };

    let permit = match store.request_replacement(&handle).unwrap() {
        Production::Producer(permit) => permit,
        _ => panic!("replacement must nominate producer"),
    };
    let second = permit.commit(vec![2], ResidentBytes::new(1)).unwrap();
    assert_ne!(first.generation(), second.generation());
    assert_eq!(first.value(), &[1]);

    let permit = match store.request_replacement(&handle).unwrap() {
        Production::Producer(permit) => permit,
        _ => panic!("replacement must nominate producer"),
    };
    let failed = permit.fail("decode error".to_owned()).unwrap();
    assert_eq!(failed.previous().unwrap().value(), &[2]);
    match store.observe(&handle).unwrap() {
        AssetObservation::Failed(observed) => {
            assert_eq!(observed.previous().unwrap().value(), &[2])
        }
        _ => panic!("failed replacement must retain previous snapshot"),
    }

    let permit = match store.request_replacement(&handle).unwrap() {
        Production::Producer(permit) => permit,
        _ => panic!("replacement retry must nominate producer"),
    };
    let waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("acquire must join replacement attempt"),
    };
    permit.cancel().unwrap();
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut waiter = std::pin::pin!(waiter);
    match std::future::Future::poll(waiter.as_mut(), &mut context) {
        std::task::Poll::Ready(Ok(ProductionOutcome::Cancelled {
            previous: Some(snapshot),
            ..
        })) => assert_eq!(snapshot.value(), &[2]),
        _ => panic!("cancelled replacement must return preserved prior snapshot"),
    }
    match store.observe(&handle).unwrap() {
        AssetObservation::Ready(snapshot) => assert_eq!(snapshot.value(), &[2]),
        _ => panic!("cancelled replacement must restore ready state"),
    }
}

#[test]
fn remove_rejects_live_handle_and_snapshot_then_recycles_to_a_stale_weak() {
    let (store, handle) = ready_store();
    let id = handle.id();
    let weak = handle.downgrade();
    assert!(matches!(
        store.remove(id),
        Err(AssetError::LiveUsers { .. })
    ));

    let snapshot = match store.observe(&handle).unwrap() {
        AssetObservation::Ready(snapshot) => snapshot,
        _ => panic!("initial content must be ready"),
    };
    drop(handle);
    assert!(matches!(
        store.remove(id),
        Err(AssetError::LiveUsers { .. })
    ));
    drop(snapshot);
    store.remove(id).unwrap();

    let recycled = store.create().unwrap();
    assert_eq!(recycled.id().slot(), id.slot());
    assert_ne!(recycled.id().slot_generation(), id.slot_generation());
    assert!(store.upgrade(&weak).unwrap().is_none());
}

#[test]
fn foreign_handle_is_rejected_by_every_store_operation() {
    let left = AssetStore::<Material, Vec<u8>, String>::new();
    let right = AssetStore::<Material, Vec<u8>, String>::new();
    let handle = left.create().unwrap();
    let weak = handle.downgrade();

    assert!(matches!(
        right.acquire(&handle),
        Err(AssetError::ForeignIdentity)
    ));
    assert!(matches!(
        right.retry(&handle),
        Err(AssetError::ForeignIdentity)
    ));
    assert!(matches!(
        right.request_replacement(&handle),
        Err(AssetError::ForeignIdentity)
    ));
    assert!(matches!(
        right.observe(&handle),
        Err(AssetError::ForeignIdentity)
    ));
    assert!(matches!(
        right.remove(handle.id()),
        Err(AssetError::ForeignIdentity)
    ));
    assert!(matches!(
        right.upgrade(&weak),
        Err(AssetError::ForeignIdentity)
    ));
}
