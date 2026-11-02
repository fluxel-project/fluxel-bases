//! Caller-owned file loading policy built on `fluxel-assets`.
//!
//! This is an executable consumer, not a library module. It keeps source keys,
//! filesystem reads, explicit blocking waits, and temporary-byte budgeting out
//! of the platform-neutral logical asset core.

#![allow(
    dead_code,
    reason = "the runnable example demonstrates one path; integration tests exercise the remaining caller controls"
)]

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Component, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use fluxel_assets::{
    Acquire, AssetError, AssetHandle, AssetId, AssetKind, AssetSnapshot, AssetStore, AssetWeak,
    AttemptGeneration, ProducerPermit, ProductionFailure, ProductionOutcome, ProductionWaiter,
    ResidentBytes,
};

pub struct FileAsset;
impl AssetKind for FileAsset {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Variant {
    Raw,
    Uppercase,
}

#[derive(Clone, Debug, Eq)]
pub struct FileKey {
    path: PathBuf,
    variant: Variant,
}

impl FileKey {
    /// Makes an absolute source key without requiring the file to exist.
    ///
    /// This removes only current-directory components and never folds `..`:
    /// folding it can change the opened resource through a symlink or junction.
    /// Callers that need alias resolution must apply that catalog policy before
    /// constructing this key. Metadata-independent normalization keeps a
    /// missing source stable across a later explicit retry.
    pub fn new(path: PathBuf, variant: Variant) -> std::io::Result<Self> {
        if !path.is_absolute()
            && path
                .components()
                .any(|component| matches!(component, Component::Prefix(_)))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "drive-relative source paths are not stable asset keys",
            ));
        }
        let absolute = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };
        let mut normalized = PathBuf::new();
        for component in absolute.components() {
            match component {
                Component::Prefix(_) | Component::RootDir => normalized.push(component.as_os_str()),
                Component::CurDir => {}
                Component::ParentDir => normalized.push(component.as_os_str()),
                Component::Normal(part) => normalized.push(part),
            }
        }
        Ok(Self {
            path: normalized,
            variant,
        })
    }

    fn transformed(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        if self.variant == Variant::Uppercase {
            bytes.make_ascii_uppercase();
        }
        bytes
    }
}

impl PartialEq for FileKey {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && self.variant == other.variant
    }
}

impl Hash for FileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.variant.hash(state);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoadError {
    SourceIo {
        kind: std::io::ErrorKind,
        message: String,
    },
    BudgetRejected {
        bytes: u64,
    },
    SourceChanged {
        expected_bytes: u64,
        observed_bytes: u64,
    },
    SourceTooLarge,
    DirectoryPoisoned,
    Core(AssetError),
}

impl LoadError {
    fn io(error: std::io::Error) -> Self {
        Self::SourceIo {
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

impl fmt::Display for LoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceIo { message, .. } => formatter.write_str(message),
            Self::BudgetRejected { bytes } => {
                write!(formatter, "in-flight budget rejected {bytes} bytes")
            }
            Self::SourceChanged {
                expected_bytes,
                observed_bytes,
            } => write!(
                formatter,
                "source size changed while reading: expected {expected_bytes} bytes, observed {observed_bytes}"
            ),
            Self::SourceTooLarge => formatter.write_str("source is too large for this process"),
            Self::DirectoryPoisoned => formatter.write_str("file-loader directory lock poisoned"),
            Self::Core(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for LoadError {}

type Store = AssetStore<FileAsset, Vec<u8>, LoadError>;
pub type Snapshot = AssetSnapshot<FileAsset, Vec<u8>>;
pub type Failure = ProductionFailure<FileAsset, Vec<u8>, LoadError>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoadStats {
    pub requests: u64,
    pub ready_hits: u64,
    pub producers: u64,
    pub joined_waiters: u64,
    pub failed_requests: u64,
    pub producer_successes: u64,
    pub producer_failures: u64,
    pub producer_cancellations: u64,
    pub budget_rejections: u64,
    pub producers_in_flight: u64,
}

#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    ready_hits: AtomicU64,
    producers: AtomicU64,
    joined_waiters: AtomicU64,
    failed_requests: AtomicU64,
    producer_successes: AtomicU64,
    producer_failures: AtomicU64,
    producer_cancellations: AtomicU64,
    budget_rejections: AtomicU64,
    producers_in_flight: AtomicU64,
}

impl Metrics {
    fn snapshot(&self) -> LoadStats {
        LoadStats {
            requests: self.requests.load(Ordering::Relaxed),
            ready_hits: self.ready_hits.load(Ordering::Relaxed),
            producers: self.producers.load(Ordering::Relaxed),
            joined_waiters: self.joined_waiters.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            producer_successes: self.producer_successes.load(Ordering::Relaxed),
            producer_failures: self.producer_failures.load(Ordering::Relaxed),
            producer_cancellations: self.producer_cancellations.load(Ordering::Relaxed),
            budget_rejections: self.budget_rejections.load(Ordering::Relaxed),
            producers_in_flight: self.producers_in_flight.load(Ordering::Relaxed),
        }
    }
}

/// A private caller directory from normalized source/variant keys to identities.
pub struct FileLoader {
    store: Store,
    directory: Mutex<HashMap<FileKey, AssetWeak<FileAsset>>>,
    metrics: Arc<Metrics>,
}

impl Default for FileLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl FileLoader {
    pub fn new() -> Self {
        Self {
            store: Store::new(),
            directory: Mutex::new(HashMap::new()),
            metrics: Arc::new(Metrics::default()),
        }
    }

    /// Gets the current value, one producer, one shared waiter, or a failure.
    pub fn request(&self, key: FileKey) -> Result<FileRequest, LoadError> {
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let handle = self.handle_for(&key)?;
        self.acquire(key, handle, false)
    }

    /// Explicitly retries a failed or cancelled source attempt.
    pub fn retry(&self, key: FileKey) -> Result<FileRequest, LoadError> {
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let handle = self.handle_for(&key)?;
        self.acquire(key, handle, true)
    }

    /// Removes a directory entry only after the core accepts identity removal.
    ///
    /// The directory lock is always acquired before the store, matching
    /// `handle_for`. Failed removal leaves the mapping intact so active users
    /// continue to join the same logical identity.
    pub fn forget(&self, key: &FileKey) -> Result<(), LoadError> {
        let mut directory = self
            .directory
            .lock()
            .map_err(|_| LoadError::DirectoryPoisoned)?;
        let Some(weak) = directory.get(key).cloned() else {
            return Ok(());
        };
        self.store.remove(weak.id()).map_err(LoadError::Core)?;
        directory.remove(key);
        Ok(())
    }

    /// Reads low-overhead counters independently; concurrent updates can make
    /// fields reflect different instants, so this is not an atomic transaction.
    pub fn stats(&self) -> LoadStats {
        self.metrics.snapshot()
    }

    fn handle_for(&self, key: &FileKey) -> Result<AssetHandle<FileAsset>, LoadError> {
        let mut directory = self
            .directory
            .lock()
            .map_err(|_| LoadError::DirectoryPoisoned)?;
        if let Some(weak) = directory.get(key) {
            if let Some(handle) = self.store.upgrade(weak).map_err(LoadError::Core)? {
                return Ok(handle);
            }
        }
        let handle = self.store.create().map_err(LoadError::Core)?;
        directory.insert(key.clone(), handle.downgrade());
        Ok(handle)
    }

    fn acquire(
        &self,
        key: FileKey,
        handle: AssetHandle<FileAsset>,
        retry: bool,
    ) -> Result<FileRequest, LoadError> {
        let acquired = if retry {
            self.store.retry(&handle)
        } else {
            self.store.acquire(&handle)
        }
        .map_err(LoadError::Core)?;
        Ok(match acquired {
            Acquire::Ready(snapshot) => {
                self.metrics.ready_hits.fetch_add(1, Ordering::Relaxed);
                FileRequest::Ready(snapshot)
            }
            Acquire::Producer(permit) => {
                self.metrics.producers.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .producers_in_flight
                    .fetch_add(1, Ordering::Relaxed);
                FileRequest::Producer(FileProducer {
                    key,
                    permit: Some(permit),
                    metrics: Arc::clone(&self.metrics),
                    terminal: false,
                })
            }
            Acquire::Waiter(waiter) => {
                self.metrics.joined_waiters.fetch_add(1, Ordering::Relaxed);
                FileRequest::Waiting(waiter)
            }
            Acquire::Failed(failure) => {
                self.metrics.failed_requests.fetch_add(1, Ordering::Relaxed);
                FileRequest::Failed(failure)
            }
        })
    }
}

pub enum FileRequest {
    Ready(Snapshot),
    Producer(FileProducer),
    Waiting(ProductionWaiter<FileAsset, Vec<u8>, LoadError>),
    Failed(Failure),
}

pub struct FileProducer {
    key: FileKey,
    permit: Option<ProducerPermit<FileAsset, Vec<u8>, LoadError>>,
    metrics: Arc<Metrics>,
    terminal: bool,
}

impl FileProducer {
    pub fn id(&self) -> AssetId<FileAsset> {
        self.permit.as_ref().expect("active producer permit").id()
    }

    pub fn attempt(&self) -> AttemptGeneration {
        self.permit
            .as_ref()
            .expect("active producer permit")
            .attempt()
    }

    pub fn produce(self) -> Result<Snapshot, LoadError> {
        self.produce_with(None)
    }

    pub fn produce_with(mut self, budget: Option<&InFlightBudget>) -> Result<Snapshot, LoadError> {
        let mut lease = match budget {
            Some(budget) => match budget.reserve_slot() {
                Some(lease) => Some(lease),
                None => {
                    self.metrics
                        .budget_rejections
                        .fetch_add(1, Ordering::Relaxed);
                    return self.fail(LoadError::BudgetRejected { bytes: 0 });
                }
            },
            None => None,
        };
        let mut source = match std::fs::File::open(&self.key.path) {
            Ok(source) => source,
            Err(error) => return self.fail(LoadError::io(error)),
        };
        let source_len = match source.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => return self.fail(LoadError::io(error)),
        };
        if let Some(lease) = lease.as_mut() {
            if !lease.increase(source_len) {
                self.metrics
                    .budget_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return self.fail(LoadError::BudgetRejected { bytes: source_len });
            }
        }
        let capacity = match usize::try_from(source_len) {
            Ok(capacity) => capacity,
            Err(_) => return self.fail(LoadError::SourceTooLarge),
        };
        let bytes = match read_exact_bounded(&mut source, source_len, capacity) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error),
        };
        let bytes = self.key.transformed(bytes);
        let bytes_len = bytes.len() as u64;
        let permit = self
            .permit
            .take()
            .expect("producer permit is present until terminal");
        let outcome = permit.commit(bytes, ResidentBytes::new(bytes_len));
        drop(lease);
        match outcome {
            Ok(snapshot) => {
                self.settle();
                self.metrics
                    .producer_successes
                    .fetch_add(1, Ordering::Relaxed);
                Ok(snapshot)
            }
            Err(error) => {
                self.settle();
                self.metrics
                    .producer_cancellations
                    .fetch_add(1, Ordering::Relaxed);
                Err(LoadError::Core(error))
            }
        }
    }

    fn fail(mut self, error: LoadError) -> Result<Snapshot, LoadError> {
        let permit = self
            .permit
            .take()
            .expect("producer permit is present until terminal");
        let published = permit.fail(error.clone());
        self.settle();
        match published {
            Ok(_) => {
                self.metrics
                    .producer_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
            Err(core_error) => {
                self.metrics
                    .producer_cancellations
                    .fetch_add(1, Ordering::Relaxed);
                Err(LoadError::Core(core_error))
            }
        }
    }

    fn settle(&mut self) {
        if !self.terminal {
            self.terminal = true;
            self.metrics
                .producers_in_flight
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for FileProducer {
    fn drop(&mut self) {
        if !self.terminal {
            self.settle();
            self.metrics
                .producer_cancellations
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) fn read_exact_bounded<R: Read>(
    source: &mut R,
    source_len: u64,
    capacity: usize,
) -> Result<Vec<u8>, LoadError> {
    // This detects source length changes between metadata and reading. It does
    // not provide a content-consistency snapshot when a source is rewritten to
    // the same length; callers that require that guarantee need source locking
    // or a versioned/content-addressed source policy.
    let mut bytes = vec![0; capacity];
    let mut filled = 0;
    while filled < bytes.len() {
        let count = source.read(&mut bytes[filled..]).map_err(LoadError::io)?;
        if count == 0 {
            return Err(LoadError::SourceChanged {
                expected_bytes: source_len,
                observed_bytes: filled as u64,
            });
        }
        filled += count;
    }
    let mut trailing = [0_u8; 1];
    let count = source.read(&mut trailing).map_err(LoadError::io)?;
    if count != 0 {
        return Err(LoadError::SourceChanged {
            expected_bytes: source_len,
            observed_bytes: source_len.saturating_add(1),
        });
    }
    Ok(bytes)
}

/// Caller-owned cap on logical temporary source/decode bytes and task count.
///
/// The byte counter is a policy reservation based on source metadata. It does
/// not claim to measure allocator capacity, transformation scratch space, or
/// other process memory; those need an application-level accounting policy.
pub struct InFlightBudget {
    maximum_bytes: u64,
    maximum_tasks: usize,
    state: Mutex<BudgetState>,
}

struct BudgetState {
    reserved_bytes: u64,
    tasks: usize,
}

impl InFlightBudget {
    pub fn new(maximum: u64) -> Self {
        Self::with_limits(maximum, usize::MAX)
    }

    pub fn with_limits(maximum_bytes: u64, maximum_tasks: usize) -> Self {
        Self {
            maximum_bytes,
            maximum_tasks,
            state: Mutex::new(BudgetState {
                reserved_bytes: 0,
                tasks: 0,
            }),
        }
    }

    pub fn reserve(&self, bytes: u64) -> Option<Reservation<'_>> {
        let mut reservation = self.reserve_slot()?;
        if reservation.increase(bytes) {
            Some(reservation)
        } else {
            None
        }
    }

    fn reserve_slot(&self) -> Option<Reservation<'_>> {
        let mut state = self.state.lock().ok()?;
        let next_tasks = state.tasks.checked_add(1)?;
        if next_tasks > self.maximum_tasks {
            return None;
        }
        state.tasks = next_tasks;
        Some(Reservation {
            budget: self,
            bytes: 0,
        })
    }

    pub fn reserved(&self) -> u64 {
        self.state
            .lock()
            .expect("budget lock must not poison")
            .reserved_bytes
    }

    pub fn in_flight(&self) -> usize {
        self.state
            .lock()
            .expect("budget lock must not poison")
            .tasks
    }
}

pub struct Reservation<'a> {
    budget: &'a InFlightBudget,
    bytes: u64,
}

impl Reservation<'_> {
    fn increase(&mut self, bytes: u64) -> bool {
        let mut state = match self.budget.state.lock() {
            Ok(state) => state,
            Err(_) => return false,
        };
        let Some(next) = state.reserved_bytes.checked_add(bytes) else {
            return false;
        };
        if next > self.budget.maximum_bytes {
            return false;
        }
        state.reserved_bytes = next;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .expect("reservation bytes overflow was checked");
        true
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .state
            .lock()
            .expect("budget lock must not poison");
        state.reserved_bytes = state
            .reserved_bytes
            .checked_sub(self.bytes)
            .expect("reservation must be released exactly once");
        state.tasks = state
            .tasks
            .checked_sub(1)
            .expect("reservation task must be released exactly once");
    }
}

struct Notify(mpsc::Sender<()>);

impl Wake for Notify {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

/// Explicit blocking wait for a caller that can safely block its current thread.
///
/// This is not executor-context detection and deliberately does not call
/// `block_on`; applications decide where such a wait is valid.
pub fn wait_for(
    waiter: &mut ProductionWaiter<FileAsset, Vec<u8>, LoadError>,
    policy: BlockingPolicy,
    timeout: Duration,
) -> Result<ProductionOutcome<FileAsset, Vec<u8>, LoadError>, WaitError> {
    if policy == BlockingPolicy::Deny {
        return Err(WaitError::BlockingDenied);
    }
    let (sender, receiver) = mpsc::channel();
    let waker = Waker::from(Arc::new(Notify(sender)));
    let started = Instant::now();
    loop {
        let mut context = Context::from_waker(&waker);
        match Pin::new(&mut *waiter).poll(&mut context) {
            Poll::Ready(Ok(outcome)) => return Ok(outcome),
            Poll::Ready(Err(error)) => return Err(WaitError::Core(error)),
            Poll::Pending => {}
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() || receiver.recv_timeout(remaining).is_err() {
            return Err(WaitError::TimedOut);
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum WaitError {
    BlockingDenied,
    TimedOut,
    Core(AssetError),
}

/// Application scheduling policy for this explicit blocking adapter.
///
/// It does not inspect executors or browser threads. The caller supplies this
/// decision at the boundary where it knows whether blocking is permitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingPolicy {
    Allow,
    Deny,
}
