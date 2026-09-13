//! Allocation-counter baseline for the timed `asset_store` workloads.
//!
//! Run with `cargo bench --bench allocations -- [FILTER] [--iterations=N]`.
//! The CSV rows are allocation evidence, not a timing replacement.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fluxel_assets::{AssetError, ResidentBytes};

struct CountingAllocator;

static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static REQUESTED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwards the original request to the system allocator.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let size = layout.size() as u64;
            let live = LIVE_BYTES.fetch_add(size, Ordering::SeqCst) + size;
            record(size, live);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::SeqCst);
        // SAFETY: pointer and layout originated from `System`.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwards the original pointer, layout, and requested size.
        let replacement = unsafe { System.realloc(pointer, layout, new_size) };
        if !replacement.is_null() {
            let old_size = layout.size() as u64;
            let new_size = new_size as u64;
            let live = if new_size >= old_size {
                LIVE_BYTES.fetch_add(new_size - old_size, Ordering::SeqCst) + new_size - old_size
            } else {
                LIVE_BYTES.fetch_sub(old_size - new_size, Ordering::SeqCst) - old_size + new_size
            };
            record(new_size, live);
        }
        replacement
    }
}

fn record(requested: u64, live: u64) {
    if ENABLED.load(Ordering::SeqCst) {
        CALLS.fetch_add(1, Ordering::SeqCst);
        REQUESTED_BYTES.fetch_add(requested, Ordering::SeqCst);
        let mut peak = PEAK_LIVE_BYTES.load(Ordering::SeqCst);
        while live > peak {
            match PEAK_LIVE_BYTES.compare_exchange_weak(
                peak,
                live,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(observed) => peak = observed,
            }
        }
    }
}

#[derive(Default)]
struct Sample {
    calls: u64,
    requested: u64,
    peak_delta: u64,
    end_delta: i128,
}

fn measure(operation: impl FnOnce()) -> Sample {
    let baseline = LIVE_BYTES.load(Ordering::SeqCst);
    CALLS.store(0, Ordering::SeqCst);
    REQUESTED_BYTES.store(0, Ordering::SeqCst);
    PEAK_LIVE_BYTES.store(baseline, Ordering::SeqCst);
    ENABLED.store(true, Ordering::SeqCst);
    operation();
    ENABLED.store(false, Ordering::SeqCst);
    let end = LIVE_BYTES.load(Ordering::SeqCst);
    Sample {
        calls: CALLS.load(Ordering::SeqCst),
        requested: REQUESTED_BYTES.load(Ordering::SeqCst),
        peak_delta: PEAK_LIVE_BYTES
            .load(Ordering::SeqCst)
            .saturating_sub(baseline),
        end_delta: i128::from(end) - i128::from(baseline),
    }
}

struct Config {
    filters: Vec<String>,
    iterations: Option<u64>,
}

fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let mut filters = Vec::new();
        let mut iterations = None;
        for argument in std::env::args().skip(1) {
            if let Some(value) = argument.strip_prefix("--iterations=") {
                iterations = value.parse().ok().filter(|value: &u64| *value > 0);
            } else if !argument.starts_with('-') {
                filters.push(argument);
            }
        }
        Config {
            filters,
            iterations,
        }
    })
}

fn average(name: &str, default_iterations: u64, mut operation: impl FnMut()) {
    if !config().filters.is_empty() && !config().filters.iter().any(|filter| name.contains(filter))
    {
        return;
    }
    let iterations = config().iterations.unwrap_or(default_iterations);
    for _ in 0..5 {
        operation();
    }
    let total = (0..iterations).map(|_| measure(&mut operation)).fold(
        Sample::default(),
        |mut total, sample| {
            total.calls += sample.calls;
            total.requested += sample.requested;
            total.peak_delta += sample.peak_delta;
            total.end_delta += sample.end_delta;
            total
        },
    );
    println!(
        "{name},{},{},{},{},{}",
        total.calls / iterations,
        total.requested / iterations,
        total.peak_delta / iterations,
        total.end_delta / i128::from(iterations),
        iterations
    );
}

/// Rebuilds a consumable fixture outside the allocation window.  Collection is
/// destructive, so this keeps fixture construction out of its reported row.
fn average_batched<Fixture>(
    name: &str,
    default_iterations: u64,
    mut setup: impl FnMut() -> Fixture,
    mut operation: impl FnMut(Fixture),
) {
    if !config().filters.is_empty() && !config().filters.iter().any(|filter| name.contains(filter))
    {
        return;
    }
    let iterations = config().iterations.unwrap_or(default_iterations);
    for _ in 0..5 {
        operation(setup());
    }
    let total = (0..iterations)
        .map(|_| {
            let fixture = setup();
            measure(|| operation(fixture))
        })
        .fold(Sample::default(), |mut total, sample| {
            total.calls += sample.calls;
            total.requested += sample.requested;
            total.peak_delta += sample.peak_delta;
            total.end_delta += sample.end_delta;
            total
        });
    println!(
        "{name},{},{},{},{},{}",
        total.calls / iterations,
        total.requested / iterations,
        total.peak_delta / iterations,
        total.end_delta / i128::from(iterations),
        iterations
    );
}

fn main() {
    if std::env::args().any(|argument| argument == "--help") {
        println!("usage: allocations [FILTER ...] [--iterations=N]");
        return;
    }
    println!(
        "workload,allocation_calls,gross_requested_bytes,peak_live_delta_bytes,end_live_delta_bytes,iterations"
    );

    // Primary: a prepared table; allocation windows contain only a 1,024-use frame.
    let (store, handles) = common::ready_fixture(common::READY_ASSETS);
    average(
        "primary/ready_assets_4096/acquire_observe_release/frame_1024",
        100,
        || {
            black_box(common::hot_frame(&store, &handles));
        },
    );
    average("secondary/missing_to_ready/batch_256", 50, || {
        black_box(common::missing_to_ready_batch(256));
    });

    let (replacement_store, replacement_handles) = common::ready_fixture(1);
    let replacement_handle = replacement_handles.into_iter().next().unwrap();
    let mut value = 0_u8;
    average("guard/replacement/ready_asset", 100, || {
        value = value.wrapping_add(1);
        black_box(common::replacement(
            &replacement_store,
            &replacement_handle,
            value,
        ));
    });
    average_batched(
        "guard/budget_collection/ready_assets_256",
        25,
        || common::ready_fixture(256),
        |(store, handles)| {
            black_box(common::collect_all(&store, handles));
        },
    );
    average("diagnostic/stale_identity/rejection", 100, || {
        let store = common::Store::new();
        let handle = store.create().unwrap();
        let stale = handle.id();
        drop(handle);
        store.remove(stale).unwrap();
        let replacement = store.create().unwrap();
        assert!(matches!(
            store.remove(stale),
            Err(AssetError::StaleIdentity { .. })
        ));
        black_box(replacement.id());
    });
    // Separate diagnostic: commit allocations without the renderer-shaped frame loop.
    average("diagnostic/commit/value_bytes_64", 100, || {
        let store = common::Store::new();
        let handle = store.create().unwrap();
        let producer = match store.acquire(&handle).unwrap() {
            fluxel_assets::Acquire::Producer(p) => p,
            _ => unreachable!(),
        };
        black_box(
            producer
                .commit(
                    vec![1; common::VALUE_BYTES],
                    ResidentBytes::new(common::VALUE_BYTES as u64),
                )
                .unwrap(),
        );
    });
}
