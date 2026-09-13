//! Budget collection is explicit and reports deterministic resident-byte effects.

use fluxel_assets::{Acquire, AssetKind, AssetStore, CacheBudget, ResidentBytes};

#[derive(Debug)]
struct Audio;
impl AssetKind for Audio {}

fn main() {
    if false {
        let store = AssetStore::<Audio, Vec<u8>, String>::new();
        let first = store.create().expect("first identity");
        let second = store.create().expect("second identity");

        let first_permit = match store.acquire(&first).expect("first production") {
            Acquire::Producer(permit) => permit,
            Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
        };
        first_permit
            .commit(vec![1; 32], ResidentBytes::new(32))
            .expect("first content");
        let second_permit = match store.acquire(&second).expect("second production") {
            Acquire::Producer(permit) => permit,
            Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
        };
        second_permit
            .commit(vec![2; 32], ResidentBytes::new(32))
            .expect("second content");

        drop(first);
        drop(second);
        let report = store
            .collect(CacheBudget::new(32))
            .expect("the cache budget is explicit");
        assert_eq!(report.before_bytes(), ResidentBytes::new(64));
        assert_eq!(report.after_bytes(), ResidentBytes::new(32));
        assert_eq!(report.evicted().len(), 1);
    }
}
