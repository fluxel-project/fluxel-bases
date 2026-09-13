//! Shared fixtures for the asset-store performance probes.
//!
//! The primary fixture models a prepared renderer asset table.  It deliberately
//! measures coordination only: decoding, I/O, and GPU upload are outside this
//! crate's contract and therefore outside these benchmarks.

use fluxel_assets::{
    Acquire, AssetHandle, AssetKind, AssetObservation, AssetStore, CacheBudget, ResidentBytes,
};

pub const READY_ASSETS: usize = 4_096;
pub const HOT_FRAME_ACQUIRES: usize = 1_024;
pub const VALUE_BYTES: usize = 64;

#[derive(Debug)]
pub struct BenchAsset;

impl AssetKind for BenchAsset {}

pub type Store = AssetStore<BenchAsset, Vec<u8>, String>;
pub type Handle = AssetHandle<BenchAsset>;

pub fn ready_fixture(count: usize) -> (Store, Vec<Handle>) {
    let store = Store::new();
    let handles = (0..count)
        .map(|index| {
            let handle = store.create().expect("fixture identity");
            let producer = match store.acquire(&handle).expect("fixture acquisition") {
                Acquire::Producer(producer) => producer,
                Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => {
                    unreachable!("a newly created fixture identity is missing")
                }
            };
            producer
                .commit(
                    vec![index as u8; VALUE_BYTES],
                    ResidentBytes::new(VALUE_BYTES as u64),
                )
                .expect("fixture commit");
            handle
        })
        .collect();
    (store, handles)
}

/// One prepared 4,096-asset table and one 1,024-access renderer-shaped frame.
pub fn hot_frame(store: &Store, handles: &[Handle]) -> usize {
    let mut checksum = 0_usize;
    for offset in 0..HOT_FRAME_ACQUIRES {
        let handle = &handles[(offset * 17) % handles.len()];
        let snapshot = match store.acquire(handle).expect("prepared asset remains valid") {
            Acquire::Ready(snapshot) => snapshot,
            Acquire::Producer(_) | Acquire::Waiter(_) | Acquire::Failed(_) => {
                unreachable!("prepared asset remains ready")
            }
        };
        checksum ^= usize::from(snapshot.value()[0]);
        drop(snapshot);
        match store
            .observe(handle)
            .expect("prepared asset remains observable")
        {
            AssetObservation::Ready(snapshot) => {
                checksum ^= usize::from(snapshot.value()[0]);
                drop(snapshot);
            }
            AssetObservation::Missing { .. }
            | AssetObservation::Producing { .. }
            | AssetObservation::Failed(_) => unreachable!("prepared asset remains ready"),
        }
    }
    checksum
}

pub fn missing_to_ready_batch(count: usize) -> usize {
    let store = Store::new();
    let mut checksum = 0_usize;
    for index in 0..count {
        let handle = store.create().expect("batch identity");
        let producer = match store.acquire(&handle).expect("batch acquisition") {
            Acquire::Producer(producer) => producer,
            Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
        };
        producer
            .commit(
                vec![index as u8; VALUE_BYTES],
                ResidentBytes::new(VALUE_BYTES as u64),
            )
            .expect("batch commit");
        match store.observe(&handle).expect("batch observation") {
            AssetObservation::Ready(snapshot) => checksum ^= usize::from(snapshot.value()[0]),
            AssetObservation::Missing { .. }
            | AssetObservation::Producing { .. }
            | AssetObservation::Failed(_) => unreachable!(),
        }
    }
    checksum
}

pub fn replacement(store: &Store, handle: &Handle, value: u8) -> usize {
    let producer = match store
        .request_replacement(handle)
        .expect("replacement request")
    {
        fluxel_assets::Production::Producer(producer) => producer,
        fluxel_assets::Production::Waiter(_) => unreachable!("benchmark has one replacement owner"),
    };
    let snapshot = producer
        .commit(
            vec![value; VALUE_BYTES],
            ResidentBytes::new(VALUE_BYTES as u64),
        )
        .expect("replacement commit");
    let observed = usize::from(snapshot.value()[0]);
    drop(snapshot);
    observed
}

pub fn collect_all(store: &Store, handles: Vec<Handle>) -> usize {
    drop(handles);
    store
        .collect(CacheBudget::new(0))
        .expect("collection")
        .evicted()
        .len()
}
