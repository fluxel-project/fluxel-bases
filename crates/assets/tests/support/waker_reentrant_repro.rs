//! Standalone audit reproducer for waker replacement and producer cancellation.
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use fluxel_assets::{Acquire, AssetKind, AssetStore, ProducerPermit};

struct Kind;
impl AssetKind for Kind {}
struct CancelOnDrop(Mutex<Option<ProducerPermit<Kind, (), ()>>>);
impl Wake for CancelOnDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        eprintln!("old_waker_drop_entered");
        let permit = self.0.get_mut().unwrap().take().unwrap();
        permit.cancel().unwrap();
        eprintln!("old_waker_drop_completed");
    }
}
struct Signal;
impl Wake for Signal { fn wake(self: Arc<Self>) {} }

fn main() {
    let store = AssetStore::<Kind, (), ()>::new();
    let handle = store.create().unwrap();
    let producer = match store.acquire(&handle).unwrap() {
        Acquire::Producer(value) => value,
        _ => unreachable!(),
    };
    let mut waiter = match store.acquire(&handle).unwrap() {
        Acquire::Waiter(value) => value,
        _ => unreachable!(),
    };
    let old = Waker::from(Arc::new(CancelOnDrop(Mutex::new(Some(producer)))));
    assert!(matches!(Pin::new(&mut waiter).poll(&mut Context::from_waker(&old)), Poll::Pending));
    drop(old);
    let next = Waker::from(Arc::new(Signal));
    eprintln!("second_poll_entered");
    let _ = Pin::new(&mut waiter).poll(&mut Context::from_waker(&next));
    eprintln!("second_poll_completed");
}
