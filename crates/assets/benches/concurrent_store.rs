//! Synthetic fixed-worker coordination workloads for `fluxel-assets`.
//!
//! These rows complement `asset_store`'s prepared single-thread frame path.
//! They measure only CPU-side store coordination: no filesystem, network,
//! decoding, GPU upload, application executor, priority queue, or cache
//! collection is modeled. Worker creation and teardown happen outside the timed
//! interval. Command dispatch, queueing, synchronization, and store operations
//! are timed because they are part of the coordinated workload.
//!
//! `guard/...` is a deterministic mixed workload: workers repeatedly resolve
//! already-ready shared assets while producing distinct cold assets. It is a
//! synthetic CPU coordination guard, not an end-to-end representative workload.
//! `diagnostic/...` verifies and measures a single missing identity requested
//! by all workers. It deliberately keeps the producer from committing until
//! every worker has acquired, so exactly one producer and `WORKERS - 1` waiters
//! are asserted on every iteration. Neither row yields a per-request latency
//! distribution: Criterion records one coordinated batch wall time.

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use fluxel_assets::{Acquire, AssetHandle, AssetKind, AssetStore, ResidentBytes};

const WORKERS: usize = 4;
const HOT_ASSETS: usize = 512;
const HOT_READS_PER_WORKER: usize = 256;
const COLD_ASSETS_PER_WORKER: usize = 64;
const VALUE_BYTES: usize = 64;

struct ConcurrentAsset;
impl AssetKind for ConcurrentAsset {}

type Store = AssetStore<ConcurrentAsset, Vec<u8>, String>;
type Handle = AssetHandle<ConcurrentAsset>;

#[derive(Default)]
struct RoundStats {
    ready: AtomicUsize,
    producers: AtomicUsize,
    waiters: AtomicUsize,
    failures: AtomicUsize,
    checksum: AtomicUsize,
}

enum Command {
    Mixed {
        store: Store,
        hot: Vec<Handle>,
        stats: Arc<RoundStats>,
    },
    SingleFlight {
        store: Store,
        handle: Handle,
        stats: Arc<RoundStats>,
    },
    Stop,
}

struct WorkerTeam {
    senders: Vec<mpsc::Sender<Command>>,
    start: Arc<Barrier>,
    finished: Arc<Barrier>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl WorkerTeam {
    fn new() -> Self {
        let start = Arc::new(Barrier::new(WORKERS + 1));
        let acquired = Arc::new(Barrier::new(WORKERS));
        let finished = Arc::new(Barrier::new(WORKERS + 1));
        let mut senders = Vec::with_capacity(WORKERS);
        let mut workers = Vec::with_capacity(WORKERS);

        for worker_index in 0..WORKERS {
            let (sender, receiver) = mpsc::channel();
            senders.push(sender);
            let start = Arc::clone(&start);
            let acquired = Arc::clone(&acquired);
            let finished = Arc::clone(&finished);
            workers.push(thread::spawn(move || {
                loop {
                    match receiver
                        .recv()
                        .expect("benchmark coordinator remains alive")
                    {
                        Command::Mixed { store, hot, stats } => {
                            start.wait();
                            run_mixed_worker(worker_index, &store, &hot, &stats);
                            finished.wait();
                        }
                        Command::SingleFlight {
                            store,
                            handle,
                            stats,
                        } => {
                            start.wait();
                            let producer = match store.acquire(&handle) {
                                Ok(Acquire::Producer(producer)) => {
                                    stats.producers.fetch_add(1, Ordering::Relaxed);
                                    Some(producer)
                                }
                                Ok(Acquire::Waiter(_)) => {
                                    stats.waiters.fetch_add(1, Ordering::Relaxed);
                                    None
                                }
                                Ok(Acquire::Ready(_)) | Ok(Acquire::Failed(_)) | Err(_) => {
                                    stats.failures.fetch_add(1, Ordering::Relaxed);
                                    None
                                }
                            };
                            // All workers have observed the same missing attempt
                            // before its one producer can publish a result.
                            acquired.wait();
                            if let Some(producer) = producer {
                                if producer
                                    .commit(
                                        vec![worker_index as u8; VALUE_BYTES],
                                        ResidentBytes::new(VALUE_BYTES as u64),
                                    )
                                    .is_err()
                                {
                                    stats.failures.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            finished.wait();
                        }
                        Command::Stop => break,
                    }
                }
            }));
        }

        Self {
            senders,
            start,
            finished,
            workers,
        }
    }

    fn run_mixed(&self, store: &Store, hot: &[Handle]) -> Arc<RoundStats> {
        let stats = Arc::new(RoundStats::default());
        for sender in &self.senders {
            sender
                .send(Command::Mixed {
                    store: store.clone(),
                    hot: hot.to_vec(),
                    stats: Arc::clone(&stats),
                })
                .expect("fixed worker remains alive");
        }
        self.start.wait();
        self.finished.wait();
        stats
    }

    fn run_single_flight(&self, store: &Store) -> Arc<RoundStats> {
        let handle = store.create().expect("benchmark identity allocation");
        let stats = Arc::new(RoundStats::default());
        for sender in &self.senders {
            sender
                .send(Command::SingleFlight {
                    store: store.clone(),
                    handle: handle.clone(),
                    stats: Arc::clone(&stats),
                })
                .expect("fixed worker remains alive");
        }
        drop(handle);
        self.start.wait();
        self.finished.wait();
        stats
    }
}

impl Drop for WorkerTeam {
    fn drop(&mut self) {
        for sender in &self.senders {
            let _ = sender.send(Command::Stop);
        }
        for worker in self.workers.drain(..) {
            worker.join().expect("benchmark worker must stop cleanly");
        }
    }
}

struct PreparedFixture {
    store: Store,
    hot: Vec<Handle>,
}

impl PreparedFixture {
    fn new() -> Self {
        let store = Store::new();
        let hot = (0..HOT_ASSETS)
            .map(|index| ready_asset(&store, index as u8))
            .collect();
        Self { store, hot }
    }

    fn mixed_round(&self, workers: &WorkerTeam) -> usize {
        let stats = workers.run_mixed(&self.store, &self.hot);
        assert_eq!(stats.failures.load(Ordering::Relaxed), 0);
        assert_eq!(
            stats.ready.load(Ordering::Relaxed),
            WORKERS * HOT_READS_PER_WORKER
        );
        assert_eq!(
            stats.producers.load(Ordering::Relaxed),
            WORKERS * COLD_ASSETS_PER_WORKER
        );
        stats.checksum.load(Ordering::Relaxed)
    }

    fn single_flight_round(&self, workers: &WorkerTeam) -> usize {
        let stats = workers.run_single_flight(&self.store);
        assert_eq!(stats.failures.load(Ordering::Relaxed), 0);
        assert_eq!(stats.producers.load(Ordering::Relaxed), 1);
        assert_eq!(stats.waiters.load(Ordering::Relaxed), WORKERS - 1);
        stats.producers.load(Ordering::Relaxed)
    }
}

fn ready_asset(store: &Store, value: u8) -> Handle {
    let handle = store.create().expect("fixture identity allocation");
    match store.acquire(&handle).expect("fixture acquisition") {
        Acquire::Producer(producer) => producer
            .commit(
                vec![value; VALUE_BYTES],
                ResidentBytes::new(VALUE_BYTES as u64),
            )
            .expect("fixture commit"),
        Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => {
            unreachable!("new fixture identity is missing")
        }
    };
    handle
}

fn run_mixed_worker(worker: usize, store: &Store, hot: &[Handle], stats: &RoundStats) {
    for offset in 0..HOT_READS_PER_WORKER {
        let handle = &hot[(worker * HOT_READS_PER_WORKER + offset * 17) % hot.len()];
        match store.acquire(handle) {
            Ok(Acquire::Ready(snapshot)) => {
                stats.ready.fetch_add(1, Ordering::Relaxed);
                stats
                    .checksum
                    .fetch_xor(usize::from(snapshot.value()[0]), Ordering::Relaxed);
            }
            Ok(Acquire::Producer(_)) | Ok(Acquire::Waiter(_)) | Ok(Acquire::Failed(_)) | Err(_) => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    for offset in 0..COLD_ASSETS_PER_WORKER {
        let value = (worker * COLD_ASSETS_PER_WORKER + offset) as u8;
        let Ok(handle) = store.create() else {
            stats.failures.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        match store.acquire(&handle) {
            Ok(Acquire::Producer(producer)) => {
                stats.producers.fetch_add(1, Ordering::Relaxed);
                match producer.commit(
                    vec![value; VALUE_BYTES],
                    ResidentBytes::new(VALUE_BYTES as u64),
                ) {
                    Ok(snapshot) => {
                        stats
                            .checksum
                            .fetch_xor(usize::from(snapshot.value()[0]), Ordering::Relaxed);
                    }
                    Err(_) => {
                        stats.failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(Acquire::Ready(_)) | Ok(Acquire::Waiter(_)) | Ok(Acquire::Failed(_)) | Err(_) => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn guard_mixed_fixed_workers(criterion: &mut Criterion) {
    let workers = WorkerTeam::new();
    criterion.bench_function(
        "guard/synthetic_cpu_workers_4/mixed_ready_hot_and_unique_cold",
        |bench| {
            bench.iter_batched_ref(
                PreparedFixture::new,
                |fixture| black_box(fixture.mixed_round(&workers)),
                BatchSize::PerIteration,
            )
        },
    );
}

fn guard_same_key_single_flight(criterion: &mut Criterion) {
    let workers = WorkerTeam::new();
    criterion.bench_function(
        "diagnostic/synthetic_cpu_workers_4/same_missing_identity_single_flight",
        |bench| {
            bench.iter_batched_ref(
                PreparedFixture::new,
                |fixture| black_box(fixture.single_flight_round(&workers)),
                BatchSize::PerIteration,
            )
        },
    );
}

criterion_group!(
    concurrent_store,
    guard_mixed_fixed_workers,
    guard_same_key_single_flight
);
criterion_main!(concurrent_store);
