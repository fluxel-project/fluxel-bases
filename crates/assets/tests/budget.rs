//! Deterministic CPU-cache budget contract.

use fluxel_assets::{Acquire, AssetKind, AssetStore, CacheBudget, ResidentBytes};

struct Blob;
impl AssetKind for Blob {}

#[test]
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

#[test]
fn collection_evicts_by_last_touch_then_reports_stable_identity_order() {
    let store = AssetStore::<Blob, Vec<u8>, String>::new();
    let first = store.create().unwrap();
    let second = store.create().unwrap();
    let first_id = first.id();
    let second_id = second.id();

    let first_snapshot = match store.acquire(&first).unwrap() {
        Acquire::Producer(permit) => permit.commit(vec![1, 1], ResidentBytes::new(2)).unwrap(),
        _ => panic!("first identity must nominate producer"),
    };
    let second_snapshot = match store.acquire(&second).unwrap() {
        Acquire::Producer(permit) => permit.commit(vec![2, 2], ResidentBytes::new(2)).unwrap(),
        _ => panic!("second identity must nominate producer"),
    };
    drop(first_snapshot);
    drop(second_snapshot);
    drop(match store.acquire(&first).unwrap() {
        Acquire::Ready(snapshot) => snapshot,
        _ => panic!("committed identity must be ready"),
    });
    drop(first);
    drop(second);

    let report = store.collect(CacheBudget::new(2)).unwrap();
    assert_eq!(report.before_bytes(), ResidentBytes::new(4));
    assert_eq!(report.after_bytes(), ResidentBytes::new(2));
    assert_eq!(report.evicted().len(), 1);
    assert_eq!(report.evicted()[0].id(), second_id);
    assert_ne!(report.evicted()[0].id(), first_id);
}

#[test]
fn failed_replacement_keeps_previous_bytes_accounted_until_collection() {
    let store = AssetStore::<Blob, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let id = handle.id();
    let snapshot = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit.commit(vec![8; 5], ResidentBytes::new(5)).unwrap(),
        _ => panic!("new identity must nominate producer"),
    };
    drop(snapshot);
    let replacement = match store.request_replacement(&handle).unwrap() {
        fluxel_assets::Production::Producer(permit) => permit,
        _ => panic!("replacement must nominate producer"),
    };
    let failure = replacement.fail("invalid replacement".to_owned()).unwrap();
    assert_eq!(
        failure.previous().unwrap().resident_bytes(),
        ResidentBytes::new(5)
    );
    drop(failure);
    drop(handle);

    assert_eq!(
        store.cache_stats().unwrap().resident_bytes(),
        ResidentBytes::new(5)
    );
    let report = store.collect(CacheBudget::new(0)).unwrap();
    assert_eq!(report.before_bytes(), ResidentBytes::new(5));
    assert_eq!(report.after_bytes(), ResidentBytes::new(0));
    assert_eq!(report.evicted().len(), 1);
    assert_eq!(report.evicted()[0].id(), id);
}
