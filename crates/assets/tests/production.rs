//! Single-flight producer-coordination contract.

use fluxel_assets::{Acquire, AssetKind, AssetObservation, AssetStore, Production, ResidentBytes};

struct Shader;
impl AssetKind for Shader {}

#[test]
fn first_acquirer_is_producer_and_concurrent_acquirer_is_waiter() {
    let store = AssetStore::<Shader, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("first acquire must be producer"),
    };
    assert!(matches!(
        store.acquire(&handle).unwrap(),
        Acquire::Waiter(_)
    ));
    permit.commit(vec![3], ResidentBytes::new(1)).unwrap();
    assert!(matches!(store.acquire(&handle).unwrap(), Acquire::Ready(_)));
}

#[test]
fn replacement_coordinates_separately_from_existing_snapshot() {
    let store = AssetStore::<Shader, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let initial = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit.commit(vec![1], ResidentBytes::new(1)).unwrap(),
        _ => panic!("new identity must nominate producer"),
    };

    let replacement = match store.request_replacement(&handle).unwrap() {
        Production::Producer(permit) => permit,
        _ => panic!("first replacement must nominate producer"),
    };
    match store.observe(&handle).unwrap() {
        AssetObservation::Producing {
            current: Some(snapshot),
            ..
        } => assert_eq!(snapshot.value(), initial.value()),
        _ => panic!("replacement must leave prior snapshot observable"),
    }
    replacement.cancel().unwrap();
}
