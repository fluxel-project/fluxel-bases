//! Deterministic CPU-cache budget contract.

use fluxel_assets::{Acquire, AssetKind, AssetStore, CacheBudget, ResidentBytes};

struct Blob;
impl AssetKind for Blob {}

#[test]
#[ignore = "implemented in 0.13.2"]
fn collection_reports_accounted_bytes_and_evicts_unpinned_ready_content() {
    let store = AssetStore::<Blob, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let snapshot = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit.commit(vec![1, 2, 3], ResidentBytes::new(3)).unwrap(),
        _ => panic!("new identity must nominate producer"),
    };
    drop(snapshot);
    drop(handle);

    let report = store.collect(CacheBudget::new(0)).unwrap();
    assert!(report.before_bytes() >= report.after_bytes());
    assert!(!report.evicted().is_empty());
}
