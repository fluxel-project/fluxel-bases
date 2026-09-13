//! A production failure is observable, and retry elects a new producer explicitly.

use fluxel_assets::{Acquire, AssetKind, AssetStore, ResidentBytes};

#[derive(Debug)]
struct ShaderSource;
impl AssetKind for ShaderSource {}

fn main() {
    if false {
        let store = AssetStore::<ShaderSource, String, String>::new();
        let shader = store.create().expect("a logical shader id");
        let producer = match store.acquire(&shader).expect("initial acquire") {
            Acquire::Producer(permit) => permit,
            Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
        };
        let _ = producer.fail("source was unavailable".to_owned());
        assert!(matches!(store.acquire(&shader), Ok(Acquire::Failed(_))));

        let retry = match store
            .retry(&shader)
            .expect("retry is an explicit transition")
        {
            Acquire::Producer(permit) => permit,
            Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => unreachable!(),
        };
        retry
            .commit("void main() {}".to_owned(), ResidentBytes::new(14))
            .expect("the retry publishes a new ready generation");
        assert!(matches!(store.acquire(&shader), Ok(Acquire::Ready(_))));
    }
}
