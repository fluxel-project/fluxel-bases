//! A removed slot makes old handles fail closed rather than aliasing recycled state.

use fluxel_assets::{AssetKind, AssetStore};

#[derive(Debug)]
struct Mesh;
impl AssetKind for Mesh {}

fn main() {
    let store = AssetStore::<Mesh, Vec<u8>, String>::new();
    let old = store.create().expect("an initial slot");
    let old_id = old.id();
    let old_weak = old.downgrade();
    drop(old);
    store
        .remove(old_id)
        .expect("the slot is removed deliberately");
    let replacement = store.create().expect("a later slot may be allocated");

    assert!(
        store
            .upgrade(&old_weak)
            .expect("a stale weak reports no new strong handle")
            .is_none()
    );
    assert!(store.remove(old_id).is_err());
    assert_ne!(old_id, replacement.id());
}
