//! Waiter registration lifecycle: one replaceable slot per waiter, drop
//! deregistration, and completion wakeups driven by a real `std::task::Wake`
//! signal.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};
use std::time::Duration;

use fluxel_assets::{
    Acquire, AssetError, AssetKind, AssetStore, ProducerPermit, Production, ProductionOutcome,
    ProductionWaiter, ResidentBytes,
};

struct Texture;
impl AssetKind for Texture {}

type Waiter = ProductionWaiter<Texture, Vec<u8>, String>;
type Outcome = Result<ProductionOutcome<Texture, Vec<u8>, String>, AssetError>;

/// Counting `std::task::Wake` signal standing in for a real executor task.
struct Signal {
    wakes: AtomicUsize,
}

impl Signal {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            wakes: AtomicUsize::new(0),
        })
    }

    fn wakes(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

fn waker(signal: &Arc<Signal>) -> Waker {
    Waker::from(Arc::clone(signal))
}

fn poll_waiter(waiter: &mut Waiter, waker: &Waker) -> Poll<Outcome> {
    let mut context = Context::from_waker(waker);
    Pin::new(waiter).poll(&mut context)
}

fn producer_and_waiter(
    store: &AssetStore<Texture, Vec<u8>, String>,
) -> (ProducerPermit<Texture, Vec<u8>, String>, Waiter) {
    let handle = store.create().unwrap();
    let producer = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the same attempt"),
    };
    (producer, waiter)
}

#[test]
fn committing_wakes_the_registered_waiter_exactly_once() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let (producer, mut waiter) = producer_and_waiter(&store);
    let signal = Signal::new();
    let waker = waker(&signal);

    assert!(matches!(poll_waiter(&mut waiter, &waker), Poll::Pending));
    assert_eq!(signal.wakes(), 0);

    let committed = producer.commit(vec![4, 2], ResidentBytes::new(2)).unwrap();
    assert_eq!(signal.wakes(), 1);

    match poll_waiter(&mut waiter, &waker) {
        Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) => {
            assert_eq!(snapshot.generation(), committed.generation());
            assert_eq!(snapshot.value(), &[4, 2]);
        }
        _ => panic!("waiter must observe the committed attempt"),
    }
    assert_eq!(signal.wakes(), 1);
}

#[test]
fn cancelling_a_waiter_releases_its_registered_waker_immediately() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let (producer, mut waiter) = producer_and_waiter(&store);
    let signal = Signal::new();
    let waker = waker(&signal);
    assert!(matches!(poll_waiter(&mut waiter, &waker), Poll::Pending));

    let registered = Arc::downgrade(&signal);
    drop(waker);
    drop(signal);
    // The waiter is still observing a pending attempt, so its registration
    // keeps the task alive until the waiter itself goes away.
    assert!(registered.upgrade().is_some());

    drop(waiter);
    // The producer is still pending; dropping the waiter must release the
    // task immediately instead of parking its waker until completion.
    assert!(registered.upgrade().is_none());
    drop(producer);
}

#[test]
fn dropping_one_waiter_preserves_its_sibling_registration() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let handle = store.create().unwrap();
    let producer = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let mut first = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the same attempt"),
    };
    let mut second = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("third acquirer must wait on the same attempt"),
    };

    let first_signal = Signal::new();
    let second_signal = Signal::new();
    let first_waker = waker(&first_signal);
    let second_waker = waker(&second_signal);
    assert!(matches!(
        poll_waiter(&mut first, &first_waker),
        Poll::Pending
    ));
    assert!(matches!(
        poll_waiter(&mut second, &second_waker),
        Poll::Pending
    ));

    let first_registered = Arc::downgrade(&first_signal);
    drop(first_waker);
    drop(first_signal);
    drop(first);
    assert!(first_registered.upgrade().is_none());

    producer.commit(vec![7], ResidentBytes::new(1)).unwrap();
    assert_eq!(second_signal.wakes(), 1);
    match poll_waiter(&mut second, &second_waker) {
        Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) => {
            assert_eq!(snapshot.value(), &[7]);
        }
        _ => panic!("surviving waiter must still observe the committed result"),
    }
    assert_eq!(second_signal.wakes(), 1);
}

#[test]
fn cloned_waiters_register_independent_slots() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let (producer, mut waiter) = producer_and_waiter(&store);
    let mut clone = waiter.clone();

    let signal = Signal::new();
    let waiter_waker = waker(&signal);
    let clone_waker = waker(&signal);
    assert!(matches!(
        poll_waiter(&mut waiter, &waiter_waker),
        Poll::Pending
    ));
    assert!(matches!(
        poll_waiter(&mut clone, &clone_waker),
        Poll::Pending
    ));

    producer.commit(vec![9], ResidentBytes::new(1)).unwrap();
    // Each registration is woken on its own: clones never share a slot, so a
    // shared task is scheduled once per waiter instead of deduplicated away.
    assert_eq!(signal.wakes(), 2);
    for result in [
        poll_waiter(&mut waiter, &waiter_waker),
        poll_waiter(&mut clone, &clone_waker),
    ] {
        match result {
            Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) => {
                assert_eq!(snapshot.value(), &[9]);
            }
            _ => panic!("every waiter clone must observe the committed result"),
        }
    }
}

#[test]
fn repolling_replaces_the_waiters_registered_waker() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let (producer, mut waiter) = producer_and_waiter(&store);

    let first_signal = Signal::new();
    let second_signal = Signal::new();
    let first_waker = waker(&first_signal);
    let second_waker = waker(&second_signal);
    assert!(matches!(
        poll_waiter(&mut waiter, &first_waker),
        Poll::Pending
    ));
    assert!(matches!(
        poll_waiter(&mut waiter, &second_waker),
        Poll::Pending
    ));

    assert_eq!(first_signal.wakes(), 0);
    let first_registered = Arc::downgrade(&first_signal);
    drop(first_waker);
    drop(first_signal);
    // The slot is replaceable: re-polling with a new task waker releases the
    // superseded one instead of accumulating both.
    assert!(first_registered.upgrade().is_none());

    producer.commit(vec![5], ResidentBytes::new(1)).unwrap();
    assert_eq!(second_signal.wakes(), 1);
    match poll_waiter(&mut waiter, &second_waker) {
        Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) => {
            assert_eq!(snapshot.value(), &[5]);
        }
        _ => panic!("waiter must observe the committed attempt"),
    }
}

/// Holds the only remaining producer permit after its Waker is parked. Dropping
/// the Waker therefore synchronously cancels the attempt and wakes the waiter.
struct CancelProducerOnDrop {
    producer: Mutex<Option<ProducerPermit<Texture, Vec<u8>, String>>>,
}

impl Wake for CancelProducerOnDrop {
    fn wake(self: Arc<Self>) {}
}

impl Drop for CancelProducerOnDrop {
    fn drop(&mut self) {
        let producer = self
            .producer
            .get_mut()
            .expect("drop runs without concurrent access")
            .take()
            .expect("the Waker owns one active producer permit");
        producer
            .cancel()
            .expect("reentrant cancellation must complete the active attempt");
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

/// State behind a RawWaker whose clone callback cancels the active producer.
/// This exercises executor-defined code in `Waker::clone`, distinct from the
/// `Wake`-object destructor used by the replacement regression below.
struct CloneCancelsProducer {
    producer: Mutex<Option<ProducerPermit<Texture, Vec<u8>, String>>>,
    clones: AtomicUsize,
}

unsafe fn clone_cancelling_raw_waker(data: *const ()) -> RawWaker {
    // Borrow the original raw Arc without consuming its strong reference, even
    // if a test assertion unwinds while executing the clone callback.
    // SAFETY: every vtable pointer originates from Arc::into_raw below and
    // represents one live strong reference owned by its RawWaker.
    let state =
        std::mem::ManuallyDrop::new(unsafe { Arc::<CloneCancelsProducer>::from_raw(data.cast()) });
    state.clones.fetch_add(1, Ordering::SeqCst);
    if let Some(producer) = state
        .producer
        .lock()
        .expect("clone callback serializes producer ownership")
        .take()
    {
        producer
            .cancel()
            .expect("clone callback must cancel the active attempt");
    }
    let cloned = Arc::clone(&state);
    RawWaker::new(Arc::into_raw(cloned).cast(), &CLONE_CANCELLING_WAKER_VTABLE)
}

unsafe fn wake_cancelling_waker(data: *const ()) {
    // SAFETY: wake consumes the RawWaker and its one Arc strong reference.
    drop(unsafe { Arc::<CloneCancelsProducer>::from_raw(data.cast()) });
}

unsafe fn wake_by_ref_cancelling_waker(_data: *const ()) {}

unsafe fn drop_cancelling_waker(data: *const ()) {
    // SAFETY: drop consumes the RawWaker and its one Arc strong reference.
    drop(unsafe { Arc::<CloneCancelsProducer>::from_raw(data.cast()) });
}

static CLONE_CANCELLING_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_cancelling_raw_waker,
    wake_cancelling_waker,
    wake_by_ref_cancelling_waker,
    drop_cancelling_waker,
);

fn waker_from_clone_cancelling_state(state: Arc<CloneCancelsProducer>) -> Waker {
    let raw = RawWaker::new(Arc::into_raw(state).cast(), &CLONE_CANCELLING_WAKER_VTABLE);
    // SAFETY: the vtable transfers and clones Arc references correctly, and
    // the shared producer state is synchronized and safe to wake across threads.
    unsafe { Waker::from_raw(raw) }
}

#[test]
fn replacing_a_waker_allows_reentrant_producer_cancellation() {
    // Execute the regression in a child thread so the pre-fix deadlock becomes
    // a bounded test failure instead of stalling the whole test command.
    let (sent, received) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let store = AssetStore::<Texture, Vec<u8>, String>::new();
        let (producer, mut waiter) = producer_and_waiter(&store);
        let first = Waker::from(Arc::new(CancelProducerOnDrop {
            producer: Mutex::new(Some(producer)),
        }));

        assert!(matches!(poll_waiter(&mut waiter, &first), Poll::Pending));
        drop(first);

        let replacement = Waker::from(Arc::new(NoopWake));
        let result = poll_waiter(&mut waiter, &replacement);
        sent.send(matches!(
            result,
            Poll::Ready(Ok(ProductionOutcome::Cancelled { .. }))
        ))
        .expect("test receiver remains available");
    });

    assert_eq!(
        received.recv_timeout(Duration::from_secs(1)),
        Ok(true),
        "replacing a Waker must not deadlock when its drop cancels production"
    );
    thread
        .join()
        .expect("reentrant registration thread must finish");
}

#[test]
fn cloning_a_waker_allows_reentrant_producer_cancellation() {
    // The previous implementation deadlocked in the clone callback, so retain
    // the same bounded harness used by the drop-reentrancy regression.
    let (sent, received) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let store = AssetStore::<Texture, Vec<u8>, String>::new();
        let (producer, mut waiter) = producer_and_waiter(&store);
        let parked_signal = Signal::new();
        let parked_waker = waker(&parked_signal);
        assert!(matches!(
            poll_waiter(&mut waiter, &parked_waker),
            Poll::Pending
        ));

        let state = Arc::new(CloneCancelsProducer {
            producer: Mutex::new(Some(producer)),
            clones: AtomicUsize::new(0),
        });
        let reentrant_waker = waker_from_clone_cancelling_state(Arc::clone(&state));
        let result = poll_waiter(&mut waiter, &reentrant_waker);

        // Completion drains the previously registered task and the polling path
        // observes its terminal state after the clone callback returns.
        sent.send((
            state.clones.load(Ordering::SeqCst),
            parked_signal.wakes(),
            matches!(result, Poll::Ready(Ok(ProductionOutcome::Cancelled { .. }))),
        ))
        .expect("test receiver remains available");
    });

    assert_eq!(
        received.recv_timeout(Duration::from_secs(1)),
        Ok((1, 1, true)),
        "cloning a Waker must not deadlock or lose a registered wake"
    );
    thread
        .join()
        .expect("reentrant clone-registration thread must finish");
}

/// Minimal executor skeleton driving a waiter through a real `Wake` signal.
struct Task {
    waiter: Pin<Box<Waiter>>,
    waker: Waker,
}

impl Task {
    fn new(waiter: Waiter, signal: &Arc<Signal>) -> Self {
        Self {
            waiter: Box::pin(waiter),
            waker: waker(signal),
        }
    }

    fn poll(&mut self) -> Poll<Outcome> {
        let waker = self.waker.clone();
        let mut context = Context::from_waker(&waker);
        self.waiter.as_mut().poll(&mut context)
    }
}

#[test]
fn signal_task_interleaves_registration_and_completion() {
    let store = AssetStore::<Texture, Vec<u8>, String>::new();
    let handle = store.create().unwrap();

    // Registration before completion: the commit must wake the parked task.
    let producer = match store.acquire(&handle).unwrap() {
        Acquire::Producer(permit) => permit,
        _ => panic!("new identity must nominate producer"),
    };
    let waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("second acquirer must wait on the same attempt"),
    };
    let signal = Signal::new();
    let mut task = Task::new(waiter, &signal);
    assert!(matches!(task.poll(), Poll::Pending));
    assert_eq!(signal.wakes(), 0);

    producer.commit(vec![1, 2], ResidentBytes::new(2)).unwrap();
    assert_eq!(signal.wakes(), 1);
    assert!(matches!(
        task.poll(),
        Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) if snapshot.value() == &[1, 2]
    ));

    // Completion before registration: observing is immediate and needs no wake.
    let permit = match store.request_replacement(&handle).unwrap() {
        Production::Producer(permit) => permit,
        _ => panic!("replacement must nominate producer"),
    };
    let late_waiter: Waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(waiter) => waiter,
        _ => panic!("acquire must join the replacement attempt"),
    };
    let late_signal = Signal::new();
    let mut late = Task::new(late_waiter, &late_signal);
    permit.commit(vec![3], ResidentBytes::new(1)).unwrap();
    assert_eq!(late_signal.wakes(), 0);
    assert!(matches!(
        late.poll(),
        Poll::Ready(Ok(ProductionOutcome::Ready(snapshot))) if snapshot.value() == &[3]
    ));
}
