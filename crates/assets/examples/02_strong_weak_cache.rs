//! A weak handle keeps typed identity, while the store controls cache residency.

use fluxel_assets::{Acquire, AssetKind, AssetObservation, AssetStore, CacheBudget, ResidentBytes};

#[derive(Debug)]
struct Font;
impl AssetKind for Font {}

fn main() {
    let store = AssetStore::<Font, String, String>::new();
    let strong = store.create().expect("a font identity is allocated");
    let weak = strong.downgrade();
    let permit = match store.acquire(&strong).expect("the font can be produced") {
        Acquire::Producer(permit) => permit,
        Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
    };
    permit
        .commit("font bytes".to_owned(), ResidentBytes::new(128))
        .expect("production completes");

    drop(strong);
    assert!(
        store
            .upgrade(&weak)
            .expect("weak upgrade observes the cache candidate")
            .is_some()
    );
    let report = store
        .collect(CacheBudget::new(0))
        .expect("collection has an explicit budget");
    assert!(report.before_bytes() >= report.after_bytes());
    assert!(!report.evicted().is_empty());
    let upgraded = store
        .upgrade(&weak)
        .expect("collection does not recycle logical identity")
        .expect("the weak identity remains upgradeable");
    assert!(matches!(
        store.observe(&upgraded),
        Ok(AssetObservation::Missing { .. })
    ));
}
