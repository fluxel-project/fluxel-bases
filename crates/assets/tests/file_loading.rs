//! Real filesystem consumer coverage for caller-owned loading policy.

#[path = "../examples/file_loader/mod.rs"]
mod file_loader;

use std::fs;
use std::future::Future;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use file_loader::{
    BlockingPolicy, FileKey, FileLoader, FileProducer, FileRequest, InFlightBudget, LoadError,
    Variant, WaitError, read_exact_bounded, wait_for,
};
use fluxel_assets::{ProductionOutcome, ProductionWaiter};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

struct TestFile(std::path::PathBuf);

impl TestFile {
    fn write(bytes: &[u8]) -> Self {
        let serial = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fluxel-assets-file-loading-{}-{serial}.bin",
            std::process::id()
        ));
        fs::write(&path, bytes).expect("test source file must be writable");
        Self(path)
    }

    fn absent() -> Self {
        let serial = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "fluxel-assets-file-loading-{}-{serial}.missing",
            std::process::id()
        )))
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn producer(request: FileRequest) -> FileProducer {
    match request {
        FileRequest::Producer(producer) => producer,
        _ => panic!("request must elect a producer"),
    }
}

fn waiter(
    request: FileRequest,
) -> ProductionWaiter<file_loader::FileAsset, Vec<u8>, file_loader::LoadError> {
    match request {
        FileRequest::Waiting(waiter) => waiter,
        _ => panic!("request must join an active producer"),
    }
}

struct AsyncNotify(mpsc::Sender<()>);

impl Wake for AsyncNotify {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

fn block_on_test_future<F: Future>(future: F) -> F::Output {
    let (sender, receiver) = mpsc::channel();
    let waker = Waker::from(Arc::new(AsyncNotify(sender)));
    let mut future = std::pin::pin!(future);
    loop {
        let mut context = Context::from_waker(&waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("async waiter must be woken by shared completion"),
        }
    }
}

#[test]
fn real_file_async_producer_and_sync_waiter_share_one_attempt() {
    let source = TestFile::write(b"asset bytes");
    let loader = Arc::new(FileLoader::new());
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let producer = producer(loader.request(key.clone()).unwrap());
    let mut waiting = waiter(loader.request(key).unwrap());

    let (started_sender, started_receiver) = mpsc::channel();
    let worker = thread::spawn(move || {
        started_sender.send(()).unwrap();
        producer.produce().unwrap()
    });
    started_receiver
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    match wait_for(&mut waiting, BlockingPolicy::Allow, Duration::from_secs(1)).unwrap() {
        ProductionOutcome::Ready(snapshot) => assert_eq!(snapshot.value(), b"asset bytes"),
        _ => panic!("shared source attempt must publish independently expected bytes"),
    }
    assert_eq!(worker.join().unwrap().value(), b"asset bytes");
}

#[test]
fn concurrent_same_key_requests_elect_one_producer_and_exact_attempt_waiters() {
    const REQUESTERS: usize = 6;

    let source = TestFile::write(b"one flight");
    let loader = Arc::new(FileLoader::new());
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let barrier = Arc::new(Barrier::new(REQUESTERS + 1));
    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::new();

    for _ in 0..REQUESTERS {
        let worker_loader = Arc::clone(&loader);
        let worker_key = key.clone();
        let worker_barrier = Arc::clone(&barrier);
        let worker_sender = sender.clone();
        workers.push(thread::spawn(move || {
            worker_barrier.wait();
            worker_sender
                .send(worker_loader.request(worker_key))
                .unwrap();
        }));
    }
    drop(sender);
    barrier.wait();

    let mut elected = None;
    let mut waiters = Vec::new();
    for _ in 0..REQUESTERS {
        match receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap()
        {
            FileRequest::Producer(producer) => {
                assert!(
                    elected.replace(producer).is_none(),
                    "only one producer may win"
                );
            }
            FileRequest::Waiting(waiter) => waiters.push(waiter),
            FileRequest::Ready(_) | FileRequest::Failed(_) => {
                panic!("no request may finish before the elected producer is retained")
            }
        }
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(waiters.len(), REQUESTERS - 1);
    let producer = elected.expect("one caller must own the source flight");
    let producer_id = producer.id();
    let producer_attempt = producer.attempt();
    for waiter in &waiters {
        assert_eq!(waiter.id(), producer_id);
        assert_eq!(waiter.attempt(), producer_attempt);
    }

    let committed = producer.produce().unwrap();
    for waiter in &mut waiters {
        match wait_for(waiter, BlockingPolicy::Allow, Duration::from_secs(1)).unwrap() {
            ProductionOutcome::Ready(snapshot) => {
                assert_eq!(snapshot.id(), committed.id());
                assert_eq!(snapshot.generation(), committed.generation());
                assert_eq!(snapshot.value(), b"one flight");
            }
            _ => panic!("all concurrent waiters must observe the same commit"),
        }
    }
}

#[test]
fn async_and_explicit_sync_waiters_observe_the_same_file_generation() {
    let source = TestFile::write(b"three consumers");
    let loader = Arc::new(FileLoader::new());
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let producer = producer(loader.request(key.clone()).unwrap());
    let async_waiter = waiter(loader.request(key.clone()).unwrap());
    let mut sync_waiter = waiter(loader.request(key).unwrap());

    let async_worker =
        thread::spawn(move || block_on_test_future(async move { async_waiter.await.unwrap() }));
    let producer_worker = thread::spawn(move || producer.produce().unwrap());
    let sync_outcome = wait_for(
        &mut sync_waiter,
        BlockingPolicy::Allow,
        Duration::from_secs(1),
    )
    .unwrap();
    let async_outcome = async_worker.join().unwrap();
    let committed = producer_worker.join().unwrap();

    let sync_snapshot = match sync_outcome {
        ProductionOutcome::Ready(snapshot) => snapshot,
        _ => panic!("sync caller must observe ready output"),
    };
    let async_snapshot = match async_outcome {
        ProductionOutcome::Ready(snapshot) => snapshot,
        _ => panic!("async caller must observe ready output"),
    };
    assert_eq!(sync_snapshot.id(), committed.id());
    assert_eq!(async_snapshot.id(), committed.id());
    assert_eq!(sync_snapshot.generation(), committed.generation());
    assert_eq!(async_snapshot.generation(), committed.generation());
    assert_eq!(sync_snapshot.value(), b"three consumers");
    assert_eq!(async_snapshot.value(), b"three consumers");
}

#[test]
fn variants_have_distinct_identities_and_transformed_readbacks() {
    let source = TestFile::write(b"MiXeD");
    let loader = FileLoader::new();
    let raw = producer(
        loader
            .request(FileKey::new(source.0.clone(), Variant::Raw).unwrap())
            .unwrap(),
    )
    .produce()
    .unwrap();
    let uppercase = producer(
        loader
            .request(FileKey::new(source.0.clone(), Variant::Uppercase).unwrap())
            .unwrap(),
    )
    .produce()
    .unwrap();

    assert_ne!(raw.id(), uppercase.id());
    assert_eq!(raw.value(), b"MiXeD");
    assert_eq!(uppercase.value(), b"MIXED");
}

#[test]
fn failed_file_attempt_releases_its_permit_and_can_retry_after_source_arrives() {
    let source = TestFile::absent();
    let loader = FileLoader::new();
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    assert!(
        producer(loader.request(key.clone()).unwrap())
            .produce()
            .is_err()
    );
    match loader.request(key.clone()).unwrap() {
        FileRequest::Failed(failure) => assert!(!failure.error().to_string().is_empty()),
        _ => panic!("a failed source attempt must remain explicit"),
    }

    fs::write(&source.0, b"arrived later").unwrap();
    let loaded = producer(loader.retry(key).unwrap()).produce().unwrap();
    assert_eq!(loaded.value(), b"arrived later");
    match loader
        .request(FileKey::new(source.0.clone(), Variant::Raw).unwrap())
        .unwrap()
    {
        FileRequest::Ready(snapshot) => assert_eq!(snapshot.value(), b"arrived later"),
        _ => panic!("committed source must take the ready path"),
    }
}

#[test]
fn in_flight_budget_rejects_then_releases_for_retry() {
    let source = TestFile::write(b"four");
    let loader = FileLoader::new();
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let budget = InFlightBudget::new(4);
    let held = budget.reserve(4).unwrap();
    assert!(
        producer(loader.request(key.clone()).unwrap())
            .produce_with(Some(&budget))
            .is_err()
    );
    assert_eq!(budget.reserved(), 4);
    drop(held);
    assert_eq!(budget.reserved(), 0);

    let loaded = producer(loader.retry(key).unwrap())
        .produce_with(Some(&budget))
        .unwrap();
    assert_eq!(loaded.value(), b"four");
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn in_flight_budget_limits_zero_byte_task_count_and_releases_it() {
    let budget = InFlightBudget::with_limits(0, 1);
    let first = budget.reserve(0).unwrap();
    assert_eq!(budget.in_flight(), 1);
    assert!(budget.reserve(0).is_none());
    drop(first);
    assert_eq!(budget.in_flight(), 0);
    drop(budget.reserve(0).unwrap());
    assert_eq!(budget.in_flight(), 0);
}

#[test]
fn task_slot_rejects_before_opening_missing_source_and_releases_for_retry() {
    let source = TestFile::absent();
    let loader = FileLoader::new();
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let budget = InFlightBudget::with_limits(1024, 1);
    let held = budget.reserve(0).unwrap();
    assert!(matches!(
        producer(loader.request(key.clone()).unwrap()).produce_with(Some(&budget)),
        Err(LoadError::BudgetRejected { bytes: 0 })
    ));
    assert_eq!(budget.in_flight(), 1);
    drop(held);
    assert_eq!(budget.in_flight(), 0);
    assert!(matches!(
        producer(loader.retry(key).unwrap()).produce_with(Some(&budget)),
        Err(LoadError::SourceIo { .. })
    ));
    assert_eq!(budget.in_flight(), 0);
    assert_eq!(budget.reserved(), 0);
    let stats = loader.stats();
    assert_eq!(stats.budget_rejections, 1);
    assert_eq!(stats.producer_failures, 2);
    assert_eq!(stats.producers_in_flight, 0);
}

#[test]
fn lexical_key_normalization_does_not_require_source_existence() {
    let relative = FileKey::new(std::path::PathBuf::from("./asset.bin"), Variant::Raw).unwrap();
    let absolute = FileKey::new(
        std::env::current_dir().unwrap().join("asset.bin"),
        Variant::Raw,
    )
    .unwrap();
    assert_eq!(relative, absolute);
    let parent_spelling = FileKey::new(
        std::path::PathBuf::from("missing/../asset.bin"),
        Variant::Raw,
    )
    .unwrap();
    assert_ne!(parent_spelling, absolute);
    #[cfg(windows)]
    assert!(FileKey::new(std::path::PathBuf::from("C:asset.bin"), Variant::Raw).is_err());
}

#[test]
fn bounded_read_rejects_deterministic_short_and_long_sources() {
    let mut short = Cursor::new(b"abc".to_vec());
    assert!(matches!(
        read_exact_bounded(&mut short, 4, 4),
        Err(LoadError::SourceChanged {
            expected_bytes: 4,
            observed_bytes: 3,
        })
    ));
    let mut long = Cursor::new(b"abcde".to_vec());
    assert!(matches!(
        read_exact_bounded(&mut long, 4, 4),
        Err(LoadError::SourceChanged {
            expected_bytes: 4,
            observed_bytes: 5,
        })
    ));
}

#[test]
fn explicit_sync_wait_times_out_without_cancelling_the_shared_attempt() {
    let source = TestFile::write(b"late");
    let loader = FileLoader::new();
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let pending = producer(loader.request(key.clone()).unwrap());
    let mut waiting = waiter(loader.request(key).unwrap());
    assert!(matches!(
        wait_for(
            &mut waiting,
            BlockingPolicy::Allow,
            Duration::from_millis(10)
        ),
        Err(WaitError::TimedOut)
    ));

    let committed = pending.produce().unwrap();
    match wait_for(&mut waiting, BlockingPolicy::Allow, Duration::from_secs(1)).unwrap() {
        ProductionOutcome::Ready(snapshot) => assert_eq!(snapshot.id(), committed.id()),
        _ => panic!("timed out waiter must still observe the shared completion"),
    }
}

#[test]
fn blocking_policy_denies_wait_without_cancelling_the_shared_attempt() {
    let source = TestFile::write(b"policy");
    let loader = FileLoader::new();
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let pending = producer(loader.request(key.clone()).unwrap());
    let mut waiting = waiter(loader.request(key).unwrap());
    assert!(matches!(
        wait_for(&mut waiting, BlockingPolicy::Deny, Duration::from_secs(1)),
        Err(WaitError::BlockingDenied)
    ));

    pending.produce().unwrap();
    match wait_for(&mut waiting, BlockingPolicy::Allow, Duration::from_secs(1)).unwrap() {
        ProductionOutcome::Ready(snapshot) => assert_eq!(snapshot.value(), b"policy"),
        _ => panic!("denied waiter must not cancel the shared producer"),
    }
}

#[test]
fn caller_metrics_classify_terminal_producer_outcomes_once() {
    let source = TestFile::write(b"metrics");
    let absent = TestFile::absent();
    let loader = FileLoader::new();
    let ready_key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let failed_key = FileKey::new(absent.0.clone(), Variant::Raw).unwrap();
    let cancelled_key = FileKey::new(source.0.clone(), Variant::Uppercase).unwrap();

    let ready = producer(loader.request(ready_key.clone()).unwrap());
    let joined = waiter(loader.request(ready_key.clone()).unwrap());
    drop(joined);
    ready.produce().unwrap();
    match loader.request(ready_key).unwrap() {
        FileRequest::Ready(snapshot) => assert_eq!(snapshot.value(), b"metrics"),
        _ => panic!("committed source must be a ready hit"),
    }

    assert!(matches!(
        producer(loader.request(failed_key.clone()).unwrap()).produce(),
        Err(LoadError::SourceIo { .. })
    ));
    match loader.request(failed_key).unwrap() {
        FileRequest::Failed(failure) => {
            assert!(matches!(failure.error(), LoadError::SourceIo { .. }))
        }
        _ => panic!("failed source must remain observable"),
    }

    let cancelled = producer(loader.request(cancelled_key.clone()).unwrap());
    let joined = waiter(loader.request(cancelled_key).unwrap());
    drop(joined);
    drop(cancelled);

    let stats = loader.stats();
    assert_eq!(stats.ready_hits, 1);
    assert_eq!(stats.producers, 3);
    assert_eq!(stats.joined_waiters, 2);
    assert_eq!(stats.failed_requests, 1);
    assert_eq!(stats.producer_successes, 1);
    assert_eq!(stats.producer_failures, 1);
    assert_eq!(stats.producer_cancellations, 1);
    assert_eq!(stats.producers_in_flight, 0);
}

#[test]
fn producer_drop_cancels_the_shared_attempt_and_retry_elects_a_new_producer() {
    let source = TestFile::write(b"retry");
    let loader = FileLoader::new();
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let cancelled = producer(loader.request(key.clone()).unwrap());
    let mut waiting = waiter(loader.request(key.clone()).unwrap());
    drop(cancelled);

    match wait_for(&mut waiting, BlockingPolicy::Allow, Duration::from_secs(1)).unwrap() {
        ProductionOutcome::Cancelled { .. } => {}
        _ => panic!("dropping caller-owned work must cancel its exact attempt"),
    }
    let loaded = producer(loader.retry(key).unwrap()).produce().unwrap();
    assert_eq!(loaded.value(), b"retry");
}

#[test]
fn forget_preserves_active_flight_mapping_then_recycles_only_after_removal() {
    let source = TestFile::write(b"forget");
    let loader = FileLoader::new();
    let key = FileKey::new(source.0.clone(), Variant::Raw).unwrap();
    let active = producer(loader.request(key.clone()).unwrap());
    assert!(loader.forget(&key).is_err());
    assert!(matches!(
        loader.request(key.clone()).unwrap(),
        FileRequest::Waiting(_)
    ));
    drop(active);

    let first = producer(loader.retry(key.clone()).unwrap())
        .produce()
        .unwrap();
    let old_id = first.id();
    drop(first);
    loader.forget(&key).unwrap();
    let replacement = producer(loader.request(key).unwrap()).produce().unwrap();
    assert_ne!(replacement.id(), old_id);
}
