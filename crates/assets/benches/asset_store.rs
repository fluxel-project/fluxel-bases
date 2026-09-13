//! Criterion timing suite.  Roles are encoded in benchmark names so the
//! renderer-shaped primary drives decisions; guards and diagnostics explain it.

mod common;

use std::hint::black_box;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;

use criterion::{Criterion, criterion_group, criterion_main};
use fluxel_assets::{Acquire, AssetError, ResidentBytes};

fn primary_hot_frame(criterion: &mut Criterion) {
    let (store, handles) = common::ready_fixture(common::READY_ASSETS);
    criterion.bench_function(
        "primary/ready_assets_4096/acquire_observe_release/frame_1024",
        |bench| bench.iter(|| black_box(common::hot_frame(&store, &handles))),
    );
}

fn secondary_missing_to_ready(criterion: &mut Criterion) {
    criterion.bench_function("secondary/missing_to_ready/batch_256", |bench| {
        bench.iter(|| black_box(common::missing_to_ready_batch(256)))
    });
}

fn guard_replacement(criterion: &mut Criterion) {
    let (store, handles) = common::ready_fixture(1);
    let handle = handles.into_iter().next().expect("fixture handle");
    let mut generation = 0_u8;
    criterion.bench_function("guard/replacement/ready_asset", |bench| {
        bench.iter(|| {
            generation = generation.wrapping_add(1);
            black_box(common::replacement(&store, &handle, generation))
        })
    });
}

fn guard_budget_collection(criterion: &mut Criterion) {
    criterion.bench_function("guard/budget_collection/ready_assets_256", |bench| {
        bench.iter_batched(
            || common::ready_fixture(256),
            |(store, handles)| black_box(common::collect_all(&store, handles)),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn guard_single_flight(criterion: &mut Criterion) {
    criterion.bench_function("guard/concurrent_single_flight/two_acquirers", |bench| {
        bench.iter(|| {
            let store = Arc::new(common::Store::new());
            let handle = store.create().expect("identity");
            let barrier = Arc::new(Barrier::new(3));
            let (sender, receiver) = mpsc::channel();
            let workers: Vec<_> = (0..2)
                .map(|_| {
                    let store = Arc::clone(&store);
                    let handle = handle.clone();
                    let barrier = Arc::clone(&barrier);
                    let sender = sender.clone();
                    thread::spawn(move || {
                        barrier.wait();
                        sender.send(store.acquire(&handle)).expect("receiver lives");
                    })
                })
                .collect();
            drop(sender);
            barrier.wait();
            let first = receiver
                .recv()
                .expect("first acquisition")
                .expect("valid handle");
            let second = receiver
                .recv()
                .expect("second acquisition")
                .expect("valid handle");
            for worker in workers {
                worker.join().expect("worker finishes");
            }
            let producer = match (first, second) {
                (Acquire::Producer(producer), Acquire::Waiter(_))
                | (Acquire::Waiter(_), Acquire::Producer(producer)) => producer,
                _ => panic!("exactly one producer is elected"),
            };
            let snapshot = producer
                .commit(
                    vec![7; common::VALUE_BYTES],
                    ResidentBytes::new(common::VALUE_BYTES as u64),
                )
                .expect("producer commits");
            black_box(snapshot.value()[0]);
        })
    });
}

fn diagnostic_stale_rejection(criterion: &mut Criterion) {
    criterion.bench_function("diagnostic/stale_identity/rejection", |bench| {
        bench.iter(|| {
            let store = common::Store::new();
            let handle = store.create().expect("identity");
            let stale = handle.id();
            drop(handle);
            store.remove(stale).expect("empty identity removal");
            let replacement = store.create().expect("slot reuse");
            let rejection = store.remove(stale);
            assert!(matches!(rejection, Err(AssetError::StaleIdentity { .. })));
            black_box(replacement.id())
        })
    });
}

criterion_group!(
    asset_store,
    primary_hot_frame,
    secondary_missing_to_ready,
    guard_replacement,
    guard_budget_collection,
    guard_single_flight,
    diagnostic_stale_rejection
);
criterion_main!(asset_store);
