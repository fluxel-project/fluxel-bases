//! OS-thread single-flight and destructor re-entrancy regression gates.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Barrier, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use fluxel_assets::{
    Acquire, AssetKind, AssetStore, ProductionOutcome, ProductionWaiter, ResidentBytes,
};

struct Concurrent;
impl AssetKind for Concurrent {}

fn poll_waiter<K: AssetKind, V, E>(
    waiter: &mut ProductionWaiter<K, V, E>,
) -> Poll<Result<ProductionOutcome<K, V, E>, fluxel_assets::AssetError>> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    Pin::new(waiter).poll(&mut context)
}

#[test]
fn concurrent_acquire_has_exactly_one_producer_and_one_waiter() {
    let store = Arc::new(AssetStore::<Concurrent, Vec<u8>, String>::new());
    let handle = store.create().unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::new();

    for _ in 0..2 {
        let store = Arc::clone(&store);
        let handle = handle.clone();
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            sender.send(store.acquire(&handle)).unwrap();
        }));
    }
    drop(sender);
    barrier.wait();

    let first = receiver.recv().unwrap().unwrap();
    let second = receiver.recv().unwrap().unwrap();
    for worker in workers {
        worker.join().unwrap();
    }

    let (producer, mut waiter) = match (first, second) {
        (Acquire::Producer(producer), Acquire::Waiter(waiter))
        | (Acquire::Waiter(waiter), Acquire::Producer(producer)) => (producer, waiter),
        _ => panic!("simultaneous acquisition must elect one producer and one waiter"),
    };
    let committed = producer.commit(vec![9, 8], ResidentBytes::new(2)).unwrap();
    match poll_waiter(&mut waiter) {
        Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) => {
            assert_eq!(snapshot.id(), committed.id());
            assert_eq!(snapshot.generation(), committed.generation());
            assert_eq!(snapshot.value(), &[9, 8]);
        }
        _ => panic!("waiter must observe the producer's exact commit"),
    }
}

struct Reentrant;
impl AssetKind for Reentrant {}

struct ReentrantValue {
    store: AssetStore<Reentrant, ReentrantValue, ReentrantError>,
    dropped: mpsc::Sender<&'static str>,
}

impl Drop for ReentrantValue {
    fn drop(&mut self) {
        let _ = self.store.cache_stats();
        let _ = self.dropped.send("value");
    }
}

struct ReentrantError {
    store: AssetStore<Reentrant, ReentrantValue, ReentrantError>,
    dropped: mpsc::Sender<&'static str>,
}

impl Drop for ReentrantError {
    fn drop(&mut self) {
        let _ = self.store.cache_stats();
        let _ = self.dropped.send("error");
    }
}

fn reentrant_value(
    store: &AssetStore<Reentrant, ReentrantValue, ReentrantError>,
    dropped: mpsc::Sender<&'static str>,
) -> ReentrantValue {
    ReentrantValue {
        store: store.clone(),
        dropped,
    }
}

fn reentrant_error(
    store: &AssetStore<Reentrant, ReentrantValue, ReentrantError>,
    dropped: mpsc::Sender<&'static str>,
) -> ReentrantError {
    ReentrantError {
        store: store.clone(),
        dropped,
    }
}

fn expect_finished<T>(receiver: mpsc::Receiver<T>) -> T {
    receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("operation must not deadlock while dropping a re-entrant value")
}

#[test]
fn collection_drops_values_after_releasing_the_store_lock() {
    let store = AssetStore::<Reentrant, ReentrantValue, ReentrantError>::new();
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let handle = store.create().unwrap();
    let snapshot = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit
            .commit(
                reentrant_value(&store, dropped_sender),
                ResidentBytes::new(1),
            )
            .unwrap(),
        _ => panic!("new identity must nominate producer"),
    };
    drop(snapshot);
    drop(handle);

    let worker_store = store.clone();
    let (done_sender, done_receiver) = mpsc::channel();
    thread::spawn(move || {
        done_sender
            .send(worker_store.collect(fluxel_assets::CacheBudget::new(0)))
            .unwrap();
    });
    assert_eq!(expect_finished(dropped_receiver), "value");
    expect_finished(done_receiver).unwrap();
}

#[test]
fn replacement_drops_failed_error_after_releasing_the_store_lock() {
    let store = AssetStore::<Reentrant, ReentrantValue, ReentrantError>::new();
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let handle = store.create().unwrap();
    let permit = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let failure = permit
        .fail(reentrant_error(&store, dropped_sender))
        .unwrap();
    drop(failure);

    let worker_store = store.clone();
    let worker_handle = handle.clone();
    let (done_sender, done_receiver) = mpsc::channel();
    thread::spawn(move || {
        done_sender
            .send(worker_store.request_replacement(&worker_handle))
            .unwrap();
    });
    assert_eq!(expect_finished(dropped_receiver), "error");
    let replacement = expect_finished(done_receiver).unwrap();
    drop(replacement);
}

#[test]
fn removal_drops_values_after_releasing_the_store_lock() {
    let store = AssetStore::<Reentrant, ReentrantValue, ReentrantError>::new();
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let handle = store.create().unwrap();
    let id = handle.id();
    let snapshot = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit
            .commit(
                reentrant_value(&store, dropped_sender),
                ResidentBytes::new(1),
            )
            .unwrap(),
        _ => panic!("new identity must nominate producer"),
    };
    drop(snapshot);
    drop(handle);

    let worker_store = store.clone();
    let (done_sender, done_receiver) = mpsc::channel();
    thread::spawn(move || {
        done_sender.send(worker_store.remove(id)).unwrap();
    });
    assert_eq!(expect_finished(dropped_receiver), "value");
    expect_finished(done_receiver).unwrap();
}
