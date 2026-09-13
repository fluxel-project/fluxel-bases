//! Logical identity and immutable-generation contract.

use fluxel_assets::{Acquire, AssetKind, AssetStore, ResidentBytes};

struct Image;
impl AssetKind for Image {}

#[test]
#[ignore = "implemented in 0.13.2"]
fn committed_snapshot_keeps_its_identity_and_generation() {
    let store = AssetStore::<Image, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let id = handle.id();
    let first = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit.commit(vec![1], ResidentBytes::new(1)).unwrap(),
        _ => panic!("new identity must nominate one producer"),
    };

    assert_eq!(first.id(), id);
    assert_eq!(first.value(), &[1]);

    let replacement = match store.request_replacement(&handle).unwrap() {
        fluxel_assets::Production::Producer(permit) => permit,
        _ => panic!("first replacement must nominate producer"),
    };
    let second = replacement.commit(vec![2], ResidentBytes::new(1)).unwrap();

    assert_eq!(first.id(), second.id());
    assert_ne!(first.generation(), second.generation());
    assert_eq!(first.value(), &[1]);
    assert_eq!(second.value(), &[2]);
}
