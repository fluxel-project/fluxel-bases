//! Structured, side-effect-free failure contract.

use fluxel_assets::{Acquire, AssetKind, AssetObservation, AssetStore};

struct Audio;
impl AssetKind for Audio {}

#[test]
fn producer_failure_is_observable_without_implicit_retry() {
    let store = AssetStore::<Audio, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    permit.fail("unavailable".to_owned()).unwrap();

    match store.observe(&handle).unwrap() {
        AssetObservation::Failed(failure) => assert_eq!(failure.error(), "unavailable"),
        _ => panic!("failed attempt must be observable"),
    }
    assert!(matches!(
        store.acquire(&handle).unwrap(),
        Acquire::Failed(_)
    ));
}

#[test]
fn cancelled_permit_is_a_structured_terminal_attempt() {
    let store = AssetStore::<Audio, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    permit.cancel().unwrap();

    assert!(matches!(
        store.observe(&handle).unwrap(),
        AssetObservation::Missing { .. }
    ));
}
