//! Strong/weak-handle ownership contract.

use fluxel_assets::{Acquire, AssetKind, AssetStore, ResidentBytes};

struct Mesh;
impl AssetKind for Mesh {}

#[test]
#[ignore = "implemented in 0.13.2"]
fn weak_upgrade_requires_a_live_store_identity() {
    let store = AssetStore::<Mesh, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let id = handle.id();
    let weak = handle.downgrade();

    assert_eq!(store.upgrade(&weak).unwrap().unwrap().id(), id);
    drop(handle);
    store.remove(id).unwrap();
    assert!(store.upgrade(&weak).unwrap().is_none());
}

#[test]
#[ignore = "implemented in 0.13.2"]
fn snapshots_pin_the_observed_value_not_a_mutable_slot() {
    let store = AssetStore::<Mesh, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let snapshot = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit.commit(vec![7], ResidentBytes::new(1)).unwrap(),
        _ => panic!("new identity must nominate producer"),
    };

    assert_eq!(snapshot.id(), handle.id());
    assert_eq!(snapshot.value_arc().as_slice(), &[7]);
}
