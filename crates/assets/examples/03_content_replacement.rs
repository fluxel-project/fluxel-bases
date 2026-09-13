//! Replacing content advances a generation without changing logical identity.

use fluxel_assets::{Acquire, AssetKind, AssetStore, Production, ResidentBytes};

#[derive(Debug)]
struct Sprite;
impl AssetKind for Sprite {}

fn main() {
    let store = AssetStore::<Sprite, String, String>::new();
    let sprite = store.create().expect("a stable logical identity");
    let producer = match store.acquire(&sprite).expect("initial production") {
        Acquire::Producer(permit) => permit,
        Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
    };
    producer
        .commit("first pixels".to_owned(), ResidentBytes::new(16))
        .expect("first generation commits");
    let first = match store.acquire(&sprite).expect("first snapshot") {
        Acquire::Ready(snapshot) => snapshot,
        Acquire::Producer(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
    };

    let replacement = match store
        .request_replacement(&sprite)
        .expect("replacement is explicit")
    {
        Production::Producer(permit) => permit,
        Production::Waiter(_) => unreachable!("there is no competing replacement"),
    };
    replacement
        .commit("second pixels".to_owned(), ResidentBytes::new(32))
        .expect("replacement commits atomically");
    let second = match store.acquire(&sprite).expect("replacement snapshot") {
        Acquire::Ready(snapshot) => snapshot,
        Acquire::Producer(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
    };

    assert_eq!(first.id(), second.id());
    assert!(second.generation() > first.generation());
    assert_eq!(first.value(), "first pixels");
    assert_eq!(second.value(), "second pixels");
}
