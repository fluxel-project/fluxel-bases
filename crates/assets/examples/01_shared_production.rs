//! Two consumers of one logical asset coordinate through one producer permit.

use fluxel_assets::{Acquire, AssetKind, AssetStore, ResidentBytes};

#[derive(Debug)]
struct Icon;
impl AssetKind for Icon {}

fn main() {
    if false {
        let store = AssetStore::<Icon, String, String>::new();
        let icon = store.create().expect("a logical asset id can be allocated");

        let producer = match store.acquire(&icon).expect("the first acquire is valid") {
            Acquire::Producer(permit) => permit,
            Acquire::Ready(_) | Acquire::Waiter(_) | Acquire::Failed(_) => {
                unreachable!("a new asset has exactly one producer")
            }
        };
        assert!(matches!(store.acquire(&icon), Ok(Acquire::Waiter(_))));

        producer
            .commit("decoded icon".to_owned(), ResidentBytes::new(64))
            .expect("the current producer may publish its content");
        let ready = match store.acquire(&icon).expect("the asset remains observable") {
            Acquire::Ready(snapshot) => snapshot,
            Acquire::Producer(_) | Acquire::Waiter(_) | Acquire::Failed(_) => {
                unreachable!("committed content is ready")
            }
        };
        assert_eq!(ready.value(), "decoded icon");
        assert_eq!(ready.resident_bytes(), ResidentBytes::new(64));
    }
}
