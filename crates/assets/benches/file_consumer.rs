//! Repeatable filesystem-consumer workload for the caller-owned file loader.
//!
//! Every timed iteration creates a fresh `FileLoader`, so its directory and
//! logical-asset cache cannot turn the operation into an in-memory cache hit.
//! Setup writes one fixed-content temporary file before timing starts. The OS
//! may retain those just-written pages in its page cache, so this is a warm
//! filesystem-path measurement, not a physical-disk cold-start measurement.
//! It also does not model a runtime executor, concurrent requests, decoding,
//! network input, GPU upload, or renderer residency.
//!
//! The timed path is `FileKey -> FileLoader::request -> FileProducer::produce
//! -> AssetSnapshot`, followed by a comparison with independently constructed
//! expected bytes. Criterion records the full per-operation wall time; it does
//! not provide a request-latency percentile distribution.

#[path = "../examples/file_loader/mod.rs"]
mod file_loader;

use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};
use file_loader::{FileKey, FileLoader, FileRequest, InFlightBudget, Variant};

const INPUT_BYTES: usize = 64 * 1024;
static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

struct TempInput {
    path: PathBuf,
}

impl TempInput {
    fn write(bytes: &[u8]) -> Self {
        let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fluxel-assets-file-consumer-{}-{sequence}.bin",
            std::process::id()
        ));
        fs::write(&path, bytes).expect("benchmark temporary input must be writable");
        Self { path }
    }
}

impl Drop for TempInput {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct FileFixture {
    input: TempInput,
    expected: Vec<u8>,
    budget: InFlightBudget,
}

impl FileFixture {
    fn new() -> Self {
        let expected = fixed_input();
        let input = TempInput::write(&expected);
        Self {
            input,
            expected,
            budget: InFlightBudget::with_limits(INPUT_BYTES as u64, 1),
        }
    }

    fn load_once(&self) -> usize {
        let loader = FileLoader::new();
        let key = FileKey::new(self.input.path.clone(), Variant::Raw)
            .expect("temporary input must have a normalizable key");
        let request = loader.request(key).expect("source request");
        let producer = match request {
            FileRequest::Producer(producer) => producer,
            FileRequest::Ready(_) | FileRequest::Waiting(_) | FileRequest::Failed(_) => {
                unreachable!("a fresh loader and key must elect one producer")
            }
        };
        let snapshot = producer
            .produce_with(Some(&self.budget))
            .expect("fixed temporary source must load");
        assert_eq!(snapshot.value(), self.expected.as_slice());
        snapshot.value().len()
    }
}

fn fixed_input() -> Vec<u8> {
    (0..INPUT_BYTES)
        .map(|index| ((index.wrapping_mul(31).wrapping_add(17)) % 251) as u8)
        .collect()
}

fn representative_fresh_file_consumer(criterion: &mut Criterion) {
    criterion.bench_function(
        "representative/warm_file_consumer/fresh_loader_key_request_produce_snapshot_64k",
        |bench| {
            let fixture = FileFixture::new();
            bench.iter(|| black_box(fixture.load_once()))
        },
    );
}

criterion_group!(file_consumer, representative_fresh_file_consumer);
criterion_main!(file_consumer);
