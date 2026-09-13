//! Explicit lifecycle-state API contract.

use fluxel_assets::{AssetKind, AssetObservation, AssetStore};

struct Font;
impl AssetKind for Font {}

#[test]
fn observation_never_starts_production() {
    let store = AssetStore::<Font, Vec<u8>, String>::new();
    let handle = store.create().unwrap();

    assert!(matches!(
        store.observe(&handle).unwrap(),
        AssetObservation::Missing { .. }
    ));
    assert!(matches!(
        store.observe(&handle).unwrap(),
        AssetObservation::Missing { .. }
    ));
}

#[test]
fn retry_is_explicit_after_a_failed_attempt() {
    let store = AssetStore::<Font, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        fluxel_assets::Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    permit.fail("decode failed".to_owned()).unwrap();

    assert!(matches!(
        store.acquire(&handle).unwrap(),
        fluxel_assets::Acquire::Failed(_)
    ));
    assert!(matches!(
        store.retry(&handle).unwrap(),
        fluxel_assets::Acquire::Producer(_)
    ));
}
