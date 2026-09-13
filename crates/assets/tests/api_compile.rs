//! Compile-only assertions for the public 0.13.1 asset contract.

use fluxel_assets::{
    Acquire, AssetError, AssetHandle, AssetKind, AssetObservation, AssetSnapshot, AssetStore,
    CacheBudget, CollectReport, Production, RemoveReport, ResidentBytes,
};

struct Image;

impl AssetKind for Image {}

fn assert_asset_kind<K: AssetKind>() {}

fn snapshot_getters_compile(snapshot: AssetSnapshot<Image, Vec<u8>>) {
    let _ = snapshot.id();
    let _ = snapshot.generation();
    let _ = snapshot.value();
    let _ = snapshot.value_arc();
    let _ = snapshot.resident_bytes();
}

// This is deliberately not a test: it proves the public names, generic
// parameters, and method signatures without depending on 0.13.2 behavior.
fn public_contract_compiles() {
    assert_asset_kind::<Image>();

    let store = AssetStore::<Image, Vec<u8>, String>::new();
    let created: Result<AssetHandle<Image>, AssetError> = store.create();
    let handle = created.expect("declaration-only compile probe");
    let weak = handle.downgrade();
    let _: Result<Option<AssetHandle<Image>>, AssetError> = store.upgrade(&weak);

    let _: Result<Acquire<Image, Vec<u8>, String>, AssetError> = store.acquire(&handle);
    let _: Result<Acquire<Image, Vec<u8>, String>, AssetError> = store.retry(&handle);
    let _: Result<Production<Image, Vec<u8>, String>, AssetError> =
        store.request_replacement(&handle);
    let _: Result<AssetObservation<Image, Vec<u8>, String>, AssetError> = store.observe(&handle);
    let _: Result<RemoveReport<Image>, AssetError> = store.remove(handle.id());
    let _: Result<CollectReport<Image>, AssetError> = store.collect(CacheBudget::new(0));

    let _: ResidentBytes = ResidentBytes::new(0);
}

#[test]
fn public_contract_names_and_markers_compile() {
    // Keep the compiler checking the complete declaration probe.
    let _: fn() = public_contract_compiles;
    let _: fn(AssetSnapshot<Image, Vec<u8>>) = snapshot_getters_compile;
}
