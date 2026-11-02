//! Caller-owned source key, single-flight file loading, and explicit waiting.

mod file_loader;

use std::env;
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use file_loader::{
    BlockingPolicy, FileKey, FileLoader, FileRequest, InFlightBudget, Variant, wait_for,
};
use fluxel_assets::ProductionOutcome;

fn main() {
    let mut arguments = env::args_os().skip(1);
    let path = arguments
        .next()
        .expect("usage: cargo run --example 07_file_loader -- <input> [output]");
    let output = arguments.next();
    assert!(
        arguments.next().is_none(),
        "expected at most input and output paths"
    );
    let source_bytes = fs::metadata(&path).expect("input metadata").len();
    let key = FileKey::new(path.into(), Variant::Raw).expect("normalize input key");
    let budget = Arc::new(InFlightBudget::with_limits(source_bytes, 1));
    let loader = Arc::new(FileLoader::new());
    let producer = match loader.request(key.clone()).expect("request source") {
        FileRequest::Producer(producer) => producer,
        _ => unreachable!("first request owns the production attempt"),
    };
    let mut waiter = match loader.request(key).expect("join source request") {
        FileRequest::Waiting(waiter) => waiter,
        _ => unreachable!("second request joins the exact same attempt"),
    };
    let worker_budget = Arc::clone(&budget);
    let worker = thread::spawn(move || {
        producer
            .produce_with(Some(&worker_budget))
            .expect("read and publish source")
    });
    match wait_for(&mut waiter, BlockingPolicy::Allow, Duration::from_secs(5))
        .expect("wait for source")
    {
        ProductionOutcome::Ready(snapshot) => {
            let output_bytes = snapshot.value().to_vec();
            worker.join().expect("file worker must finish");
            if let Some(output) = output {
                fs::write(output, output_bytes).expect("write completed snapshot");
            }
            println!("{} bytes", snapshot.value().len());
        }
        _ => unreachable!("the producer committed the file contents"),
    }
}
