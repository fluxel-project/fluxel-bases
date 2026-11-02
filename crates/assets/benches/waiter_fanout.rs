//! Single-threaded diagnostic for one production attempt's waiter fan-out.
//!
//! Each timed operation constructs a fresh store and one missing identity,
//! elects exactly one producer, registers `N` independent waiters, polls each
//! once while pending, commits the producer, and polls every waiter to observe
//! the same ready result. Thus the timed interval includes store/attempt setup,
//! waiter registration, waker replacement, completion fan-out, and final
//! observation. There is no thread, executor, I/O, decoding, or user-request
//! latency model here. The result is a diagnostic batch cost, not a product
//! baseline and not a per-waiter latency percentile.

use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fluxel_assets::{
    Acquire, AssetKind, AssetStore, ProductionOutcome, ProductionWaiter, ResidentBytes,
};

const WAITER_COUNTS: &[usize] = &[1, 8, 64, 256];

struct FanoutAsset;
impl AssetKind for FanoutAsset {}

type Waiter = ProductionWaiter<FanoutAsset, Vec<u8>, String>;
type Outcome = Result<ProductionOutcome<FanoutAsset, Vec<u8>, String>, fluxel_assets::AssetError>;

struct WakeCounter {
    wakes: AtomicUsize,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

fn poll_waiter(waiter: &mut Waiter, context: &mut Context<'_>) -> Poll<Outcome> {
    Pin::new(waiter).poll(context)
}

fn one_attempt(waiter_count: usize) -> usize {
    let store = AssetStore::<FanoutAsset, Vec<u8>, String>::new();
    let handle = store.create().expect("diagnostic identity allocation");
    let expected_id = handle.id();
    let producer = match store.acquire(&handle).expect("first acquisition") {
        Acquire::Producer(producer) => producer,
        Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => {
            unreachable!("new identity must elect exactly one producer")
        }
    };
    let mut waiters = Vec::with_capacity(waiter_count);
    for _ in 0..waiter_count {
        match store.acquire(&handle).expect("shared attempt acquisition") {
            Acquire::Waiter(waiter) => waiters.push(waiter),
            Acquire::Ready(_) | Acquire::Producer(_) | Acquire::Failed(_) => {
                panic!("each follower must join the same pending attempt")
            }
        }
    }

    let wake_counter = Arc::new(WakeCounter {
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut context = Context::from_waker(&waker);
    for waiter in &mut waiters {
        assert!(matches!(poll_waiter(waiter, &mut context), Poll::Pending));
    }

    let committed = producer
        .commit(vec![0x5a], ResidentBytes::new(1))
        .expect("sole producer must commit");
    let expected_generation = committed.generation();
    assert_eq!(committed.id(), expected_id);
    assert_eq!(committed.value(), &[0x5a]);
    assert_eq!(wake_counter.wakes.load(Ordering::Relaxed), waiter_count);

    let mut checksum = 0_usize;
    for waiter in &mut waiters {
        match poll_waiter(waiter, &mut context) {
            Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) => {
                assert_eq!(snapshot.id(), expected_id);
                assert_eq!(snapshot.generation(), expected_generation);
                assert_eq!(snapshot.value(), &[0x5a]);
                checksum ^= snapshot.generation().get() as usize;
            }
            Poll::Ready(Ok(ProductionOutcome::Failed(_)))
            | Poll::Ready(Ok(ProductionOutcome::Cancelled { .. }))
            | Poll::Ready(Err(_))
            | Poll::Pending => panic!("each waiter must observe the committed exact attempt"),
        }
    }
    checksum ^ waiter_count
}

fn diagnostic_waiter_fanout(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("diagnostic/waiter_fanout_one_attempt");
    for &waiter_count in WAITER_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(waiter_count),
            &waiter_count,
            |bench, &waiter_count| bench.iter(|| black_box(one_attempt(waiter_count))),
        );
    }
    group.finish();
}

criterion_group!(waiter_fanout, diagnostic_waiter_fanout);
criterion_main!(waiter_fanout);
