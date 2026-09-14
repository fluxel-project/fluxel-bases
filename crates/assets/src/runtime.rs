//! Private synchronized state machine for logical identities and cache values.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use crate::identity::IdentityToken;
use crate::{
    Acquire, AssetError, AssetHandle, AssetId, AssetKind, AssetObservation, AssetSnapshot,
    AssetWeak, AttemptGeneration, CacheBudget, CacheStats, CollectReport, ContentGeneration,
    EvictedAsset, ProducerPermit, Production, ProductionFailure, ProductionOutcome,
    ProductionWaiter, RemoveReport, ResidentBytes,
};

static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

type AttemptResult<K, V, E> = Result<ProductionOutcome<K, V, E>, AssetError>;

/// One waiter's replaceable waker slot.
///
/// Each [`ProductionWaiter`] owns exactly one slot for its whole lifetime, so
/// clones and repeated polls never share storage and dropping one waiter can
/// only release its own registered waker. The registry never runs waiter or
/// executor code while a slot is locked; `wake` happens strictly outside.
struct WakerSlot {
    waker: Mutex<Option<Waker>>,
}

impl WakerSlot {
    fn new() -> Self {
        Self {
            waker: Mutex::new(None),
        }
    }

    /// Stores `waker` unless the slot already holds an equivalent waker.
    fn record(&self, waker: &Waker) {
        let mut current = match self.waker.lock() {
            Ok(current) => current,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !current
            .as_ref()
            .is_some_and(|existing| existing.will_wake(waker))
        {
            *current = Some(waker.clone());
        }
    }

    /// Takes the registered waker, if any.
    fn take(&self) -> Option<Waker> {
        let mut current = match self.waker.lock() {
            Ok(current) => current,
            Err(poisoned) => poisoned.into_inner(),
        };
        current.take()
    }
}

/// A waiter's exclusive registration in one [`AttemptCell`] registry.
///
/// `key` is never reused, so deregistration removes exactly this waiter's slot
/// and cannot disturb a sibling waiter registered for the same attempt.
pub(crate) struct WaiterRegistration {
    key: u64,
    slot: Arc<WakerSlot>,
}

struct WaiterRegistry {
    next_key: u64,
    slots: HashMap<u64, Arc<WakerSlot>>,
}

impl WaiterRegistry {
    fn new() -> Self {
        Self {
            next_key: 0,
            slots: HashMap::new(),
        }
    }

    fn register(&mut self) -> WaiterRegistration {
        let key = self
            .next_key
            .checked_add(1)
            .expect("fluxel-assets waiter registration space exhausted");
        self.next_key = key;
        let slot = Arc::new(WakerSlot::new());
        self.slots.insert(key, Arc::clone(&slot));
        WaiterRegistration { key, slot }
    }

    fn deregister(&mut self, key: u64) {
        self.slots.remove(&key);
    }

    /// Takes every registered waker so callers can wake outside all locks.
    ///
    /// The whole registry is drained: once an attempt completes, no further
    /// wakeup can ever be needed and leftover slots would only leak.
    fn take_wakers(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.slots)
            .into_values()
            .filter_map(|slot| slot.take())
            .collect()
    }
}

pub(crate) struct AttemptCell<K: AssetKind, V, E> {
    outcome: Mutex<Option<AttemptResult<K, V, E>>>,
    registry: Mutex<WaiterRegistry>,
}

impl<K: AssetKind, V, E> AttemptCell<K, V, E> {
    fn new() -> Self {
        Self {
            outcome: Mutex::new(None),
            registry: Mutex::new(WaiterRegistry::new()),
        }
    }

    fn lock_registry(&self) -> MutexGuard<'_, WaiterRegistry> {
        match self.registry.lock() {
            Ok(registry) => registry,
            // The registry never runs foreign code under its lock, so recovery
            // keeps wakeups flowing instead of losing them to a poisoned lock.
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Creates the calling waiter's exclusive waker slot.
    pub(crate) fn register(&self) -> WaiterRegistration {
        self.lock_registry().register()
    }

    /// Removes a waiter's slot and drops its registered waker, if any.
    pub(crate) fn deregister(&self, registration: &WaiterRegistration) {
        self.lock_registry().deregister(registration.key);
    }

    fn complete(&self, outcome: Result<ProductionOutcome<K, V, E>, AssetError>) {
        let mut result = match self.outcome.lock() {
            Ok(result) => result,
            Err(poisoned) => poisoned.into_inner(),
        };
        if result.is_some() {
            return;
        }
        *result = Some(outcome);
        drop(result);
        let wakers = self.lock_registry().take_wakers();
        for waker in wakers {
            waker.wake();
        }
    }

    pub(crate) fn poll(
        &self,
        registration: &WaiterRegistration,
        context: &mut Context<'_>,
    ) -> Poll<Result<ProductionOutcome<K, V, E>, AssetError>> {
        let result = match self.outcome.lock() {
            Ok(result) => result,
            Err(_) => return Poll::Ready(Err(AssetError::SynchronizationPoisoned)),
        };
        if let Some(outcome) = result.as_ref() {
            return Poll::Ready(outcome.clone());
        }
        drop(result);
        registration.slot.record(context.waker());
        let result = match self.outcome.lock() {
            Ok(result) => result,
            Err(_) => return Poll::Ready(Err(AssetError::SynchronizationPoisoned)),
        };
        match result.as_ref() {
            Some(outcome) => Poll::Ready(outcome.clone()),
            None => Poll::Pending,
        }
    }
}

struct ReadyValue<V> {
    generation: ContentGeneration,
    value: Arc<V>,
    bytes: ResidentBytes,
}

impl<V> Clone for ReadyValue<V> {
    fn clone(&self) -> Self {
        Self {
            generation: self.generation,
            value: Arc::clone(&self.value),
            bytes: self.bytes,
        }
    }
}

enum Phase<K: AssetKind, V, E> {
    Missing,
    Producing {
        attempt: AttemptGeneration,
        previous: Option<ReadyValue<V>>,
        cell: Arc<AttemptCell<K, V, E>>,
    },
    Ready(ReadyValue<V>),
    Failed {
        attempt: AttemptGeneration,
        error: Arc<E>,
        previous: Option<ReadyValue<V>>,
    },
}

struct Entry<K: AssetKind, V, E> {
    token: Arc<IdentityToken<K>>,
    phase: Phase<K, V, E>,
    content_generation: u64,
    attempt_generation: u64,
    last_touch: u64,
}

struct Slot<K: AssetKind, V, E> {
    generation: u32,
    entry: Option<Entry<K, V, E>>,
}

struct StoreState<K: AssetKind, V, E> {
    slots: Vec<Slot<K, V, E>>,
    free: BTreeSet<u32>,
    ready_bytes: u64,
    touch: u64,
}

impl<K: AssetKind, V, E> StoreState<K, V, E> {
    fn next_touch(&mut self) -> Result<u64, AssetError> {
        self.touch = self
            .touch
            .checked_add(1)
            .ok_or(AssetError::GenerationExhausted)?;
        Ok(self.touch)
    }
}

pub(crate) struct StoreInner<K: AssetKind, V, E> {
    id: u64,
    state: Mutex<StoreState<K, V, E>>,
}

impl<K: AssetKind, V, E> StoreInner<K, V, E> {
    pub(crate) fn new() -> Self {
        let id = NEXT_STORE_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .unwrap_or_else(|_| panic!("fluxel-assets store identity space exhausted"));
        Self {
            id,
            state: Mutex::new(StoreState {
                slots: Vec::new(),
                free: BTreeSet::new(),
                ready_bytes: 0,
                touch: 0,
            }),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, StoreState<K, V, E>>, AssetError> {
        self.state
            .lock()
            .map_err(|_| AssetError::SynchronizationPoisoned)
    }

    fn stale(id: AssetId<K>) -> AssetError {
        AssetError::StaleIdentity {
            slot: id.slot,
            slot_generation: id.generation,
        }
    }

    fn entry_mut<'a>(
        &self,
        state: &'a mut StoreState<K, V, E>,
        id: AssetId<K>,
    ) -> Result<&'a mut Entry<K, V, E>, AssetError> {
        if id.store != self.id {
            return Err(AssetError::ForeignIdentity);
        }
        let slot = state
            .slots
            .get_mut(id.slot as usize)
            .ok_or_else(|| Self::stale(id))?;
        if slot.generation != id.generation {
            return Err(Self::stale(id));
        }
        slot.entry.as_mut().ok_or_else(|| Self::stale(id))
    }

    fn validate_handle<'a>(
        &self,
        state: &'a mut StoreState<K, V, E>,
        handle: &AssetHandle<K>,
    ) -> Result<&'a mut Entry<K, V, E>, AssetError> {
        let entry = self.entry_mut(state, handle.id())?;
        if !Arc::ptr_eq(&entry.token, &handle.token) {
            return Err(AssetError::ForeignIdentity);
        }
        Ok(entry)
    }

    fn snapshot(entry: &Entry<K, V, E>, ready: &ReadyValue<V>) -> AssetSnapshot<K, V> {
        AssetSnapshot {
            handle: AssetHandle {
                token: Arc::clone(&entry.token),
            },
            generation: ready.generation,
            value: Arc::clone(&ready.value),
            bytes: ready.bytes,
        }
    }

    fn failure(
        entry: &Entry<K, V, E>,
        attempt: AttemptGeneration,
        error: &Arc<E>,
        previous: &Option<ReadyValue<V>>,
    ) -> ProductionFailure<K, V, E> {
        ProductionFailure {
            id: entry.token.id,
            attempt,
            error: Arc::clone(error),
            previous: previous.as_ref().map(|ready| Self::snapshot(entry, ready)),
        }
    }

    fn make_attempt(
        self: &Arc<Self>,
        entry: &mut Entry<K, V, E>,
        previous: Option<ReadyValue<V>>,
    ) -> Result<ProducerPermit<K, V, E>, AssetError> {
        let next_attempt = entry
            .attempt_generation
            .checked_add(1)
            .ok_or(AssetError::GenerationExhausted)?;
        entry.attempt_generation = next_attempt;
        let attempt = AttemptGeneration(entry.attempt_generation);
        let cell = Arc::new(AttemptCell::new());
        entry.phase = Phase::Producing {
            attempt,
            previous,
            cell: Arc::clone(&cell),
        };
        Ok(ProducerPermit {
            id: entry.token.id,
            attempt,
            store: Arc::downgrade(self),
            cell,
            completed: false,
        })
    }

    fn waiter(
        id: AssetId<K>,
        attempt: AttemptGeneration,
        cell: &Arc<AttemptCell<K, V, E>>,
    ) -> ProductionWaiter<K, V, E> {
        ProductionWaiter {
            id,
            attempt,
            cell: Arc::clone(cell),
            registration: cell.register(),
        }
    }

    pub(crate) fn create(self: &Arc<Self>) -> Result<AssetHandle<K>, AssetError> {
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let slot_index = if let Some(slot) = state.free.pop_first() {
            slot
        } else {
            u32::try_from(state.slots.len()).map_err(|_| AssetError::GenerationExhausted)?
        };
        if slot_index as usize == state.slots.len() {
            state.slots.push(Slot {
                generation: 1,
                entry: None,
            });
        }
        let slot = &mut state.slots[slot_index as usize];
        let id = AssetId {
            store: self.id,
            slot: slot_index,
            generation: slot.generation,
            marker: std::marker::PhantomData,
        };
        let token = Arc::new(IdentityToken { id });
        slot.entry = Some(Entry {
            token: Arc::clone(&token),
            phase: Phase::Missing,
            content_generation: 0,
            attempt_generation: 0,
            last_touch: touch,
        });
        Ok(AssetHandle { token })
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        handle: &AssetHandle<K>,
    ) -> Result<Acquire<K, V, E>, AssetError> {
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let entry = self.validate_handle(&mut state, handle)?;
        entry.last_touch = touch;
        match &entry.phase {
            Phase::Ready(ready) => Ok(Acquire::Ready(Self::snapshot(entry, ready))),
            Phase::Failed {
                attempt,
                error,
                previous,
            } => Ok(Acquire::Failed(Self::failure(
                entry, *attempt, error, previous,
            ))),
            Phase::Producing { attempt, cell, .. } => Ok(Acquire::Waiter(Self::waiter(
                entry.token.id,
                *attempt,
                cell,
            ))),
            Phase::Missing => Ok(Acquire::Producer(self.make_attempt(entry, None)?)),
        }
    }

    pub(crate) fn retry(
        self: &Arc<Self>,
        handle: &AssetHandle<K>,
    ) -> Result<Acquire<K, V, E>, AssetError> {
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let entry = self.validate_handle(&mut state, handle)?;
        entry.last_touch = touch;
        match &entry.phase {
            Phase::Ready(ready) => return Ok(Acquire::Ready(Self::snapshot(entry, ready))),
            Phase::Producing { attempt, cell, .. } => {
                return Ok(Acquire::Waiter(Self::waiter(
                    entry.token.id,
                    *attempt,
                    cell,
                )));
            }
            Phase::Missing => return Ok(Acquire::Producer(self.make_attempt(entry, None)?)),
            Phase::Failed { .. } => {}
        }
        if entry.attempt_generation == u64::MAX {
            return Err(AssetError::GenerationExhausted);
        }
        let old = std::mem::replace(&mut entry.phase, Phase::Missing);
        let (previous, retired_error) = match old {
            Phase::Failed {
                error, previous, ..
            } => (previous, error),
            _ => unreachable!("phase was checked before replacement"),
        };
        let permit = self.make_attempt(entry, previous)?;
        drop(state);
        drop(retired_error);
        Ok(Acquire::Producer(permit))
    }

    pub(crate) fn request_replacement(
        self: &Arc<Self>,
        handle: &AssetHandle<K>,
    ) -> Result<Production<K, V, E>, AssetError> {
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let entry = self.validate_handle(&mut state, handle)?;
        entry.last_touch = touch;
        if let Phase::Producing { attempt, cell, .. } = &entry.phase {
            return Ok(Production::Waiter(Self::waiter(
                entry.token.id,
                *attempt,
                cell,
            )));
        }
        if entry.attempt_generation == u64::MAX {
            return Err(AssetError::GenerationExhausted);
        }
        let old = std::mem::replace(&mut entry.phase, Phase::Missing);
        let (previous, retired_error) = match old {
            Phase::Ready(ready) => (Some(ready), None),
            Phase::Failed {
                error, previous, ..
            } => (previous, Some(error)),
            Phase::Missing => (None, None),
            Phase::Producing { .. } => unreachable!("producing returned above"),
        };
        let permit = self.make_attempt(entry, previous)?;
        drop(state);
        drop(retired_error);
        Ok(Production::Producer(permit))
    }

    pub(crate) fn observe(
        &self,
        handle: &AssetHandle<K>,
    ) -> Result<AssetObservation<K, V, E>, AssetError> {
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let entry = self.validate_handle(&mut state, handle)?;
        entry.last_touch = touch;
        match &entry.phase {
            Phase::Missing => Ok(AssetObservation::Missing { id: entry.token.id }),
            Phase::Ready(ready) => Ok(AssetObservation::Ready(Self::snapshot(entry, ready))),
            Phase::Failed {
                attempt,
                error,
                previous,
            } => Ok(AssetObservation::Failed(Self::failure(
                entry, *attempt, error, previous,
            ))),
            Phase::Producing {
                attempt, previous, ..
            } => Ok(AssetObservation::Producing {
                id: entry.token.id,
                attempt: *attempt,
                current: previous.as_ref().map(|ready| Self::snapshot(entry, ready)),
            }),
        }
    }

    pub(crate) fn upgrade(
        &self,
        handle: &AssetWeak<K>,
    ) -> Result<Option<AssetHandle<K>>, AssetError> {
        if handle.id.store != self.id {
            return Err(AssetError::ForeignIdentity);
        }
        let mut state = self.lock()?;
        let entry = match self.entry_mut(&mut state, handle.id) {
            Ok(entry) => entry,
            Err(AssetError::StaleIdentity { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(token) = handle.token.upgrade() else {
            return Ok(None);
        };
        if !Arc::ptr_eq(&token, &entry.token) {
            return Ok(None);
        }
        Ok(Some(AssetHandle { token }))
    }

    pub(crate) fn commit(
        &self,
        id: AssetId<K>,
        attempt: AttemptGeneration,
        cell: &Arc<AttemptCell<K, V, E>>,
        value: V,
        bytes: ResidentBytes,
    ) -> Result<AssetSnapshot<K, V>, AssetError> {
        let value = Arc::new(value);
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let current_total = state.ready_bytes;
        let entry = self.entry_mut(&mut state, id)?;
        let Some(generation) = entry.content_generation.checked_add(1) else {
            drop(state);
            drop(value);
            return Err(AssetError::GenerationExhausted);
        };
        let old = std::mem::replace(&mut entry.phase, Phase::Missing);
        let previous = match old {
            Phase::Producing {
                attempt: active,
                previous,
                cell: active_cell,
            } if active == attempt && Arc::ptr_eq(&active_cell, cell) => previous,
            other => {
                let expected = match &other {
                    Phase::Producing { attempt, .. } => Some(attempt.get()),
                    _ => None,
                };
                entry.phase = other;
                drop(state);
                drop(value);
                return Err(AssetError::ExpiredProducer {
                    actual_attempt: attempt.get(),
                    expected_attempt: expected,
                });
            }
        };
        let previous_bytes = previous.as_ref().map_or(0, |ready| ready.bytes.get());
        let ready_bytes = current_total
            .checked_sub(previous_bytes)
            .and_then(|total| total.checked_add(bytes.get()));
        let Some(ready_bytes) = ready_bytes else {
            entry.phase = Phase::Producing {
                attempt,
                previous,
                cell: Arc::clone(cell),
            };
            drop(state);
            drop(value);
            return Err(AssetError::ResidentBytesOverflow);
        };
        entry.content_generation = generation;
        entry.last_touch = touch;
        let ready = ReadyValue {
            generation: ContentGeneration(generation),
            value,
            bytes,
        };
        let snapshot = Self::snapshot(entry, &ready);
        entry.phase = Phase::Ready(ready);
        state.ready_bytes = ready_bytes;
        drop(state);
        drop(previous);
        cell.complete(Ok(ProductionOutcome::Ready(snapshot.clone())));
        Ok(snapshot)
    }

    pub(crate) fn fail(
        &self,
        id: AssetId<K>,
        attempt: AttemptGeneration,
        cell: &Arc<AttemptCell<K, V, E>>,
        error: E,
    ) -> Result<ProductionFailure<K, V, E>, AssetError> {
        let error = Arc::new(error);
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let entry = self.entry_mut(&mut state, id)?;
        let old = std::mem::replace(&mut entry.phase, Phase::Missing);
        let previous = match old {
            Phase::Producing {
                attempt: active,
                previous,
                cell: active_cell,
            } if active == attempt && Arc::ptr_eq(&active_cell, cell) => previous,
            other => {
                let expected = match &other {
                    Phase::Producing { attempt, .. } => Some(attempt.get()),
                    _ => None,
                };
                entry.phase = other;
                drop(state);
                drop(error);
                return Err(AssetError::ExpiredProducer {
                    actual_attempt: attempt.get(),
                    expected_attempt: expected,
                });
            }
        };
        entry.last_touch = touch;
        let failure = ProductionFailure {
            id,
            attempt,
            error: Arc::clone(&error),
            previous: previous.as_ref().map(|ready| Self::snapshot(entry, ready)),
        };
        entry.phase = Phase::Failed {
            attempt,
            error,
            previous,
        };
        drop(state);
        cell.complete(Ok(ProductionOutcome::Failed(failure.clone())));
        Ok(failure)
    }

    pub(crate) fn cancel(
        &self,
        id: AssetId<K>,
        attempt: AttemptGeneration,
        cell: &Arc<AttemptCell<K, V, E>>,
    ) -> Result<(), AssetError> {
        let mut state = self.lock()?;
        let touch = state.next_touch()?;
        let entry = self.entry_mut(&mut state, id)?;
        let old = std::mem::replace(&mut entry.phase, Phase::Missing);
        let previous = match old {
            Phase::Producing {
                attempt: active,
                previous,
                cell: active_cell,
            } if active == attempt && Arc::ptr_eq(&active_cell, cell) => previous,
            other => {
                let expected = match &other {
                    Phase::Producing { attempt, .. } => Some(attempt.get()),
                    _ => None,
                };
                entry.phase = other;
                return Err(AssetError::ExpiredProducer {
                    actual_attempt: attempt.get(),
                    expected_attempt: expected,
                });
            }
        };
        entry.last_touch = touch;
        let public_previous = previous.as_ref().map(|ready| Self::snapshot(entry, ready));
        if let Some(ready) = previous {
            entry.phase = Phase::Ready(ready);
        }
        drop(state);
        cell.complete(Ok(ProductionOutcome::Cancelled {
            id,
            attempt,
            previous: public_previous,
        }));
        Ok(())
    }

    pub(crate) fn remove(&self, id: AssetId<K>) -> Result<RemoveReport<K>, AssetError> {
        if id.store != self.id {
            return Err(AssetError::ForeignIdentity);
        }
        let mut state = self.lock()?;
        let (entry, next_generation) = {
            let slot = state
                .slots
                .get_mut(id.slot as usize)
                .ok_or_else(|| Self::stale(id))?;
            if slot.generation != id.generation || slot.entry.is_none() {
                return Err(Self::stale(id));
            }
            let entry = slot.entry.as_ref().expect("entry checked above");
            let strong_users = Arc::strong_count(&entry.token).saturating_sub(1);
            if strong_users != 0 {
                return Err(AssetError::LiveUsers { strong_users });
            }
            if let Phase::Producing { attempt, .. } = &entry.phase {
                return Err(AssetError::ProductionInProgress {
                    attempt: attempt.get(),
                });
            }
            let entry = slot.entry.take().expect("entry checked above");
            let next_generation = slot.generation.checked_add(1);
            if let Some(next) = next_generation {
                slot.generation = next;
            }
            (entry, next_generation)
        };
        let (generation, bytes) = match &entry.phase {
            Phase::Ready(ready) => (Some(ready.generation), ready.bytes),
            Phase::Failed { previous, .. } => previous
                .as_ref()
                .map_or((None, ResidentBytes::new(0)), |ready| {
                    (Some(ready.generation), ready.bytes)
                }),
            Phase::Missing => (None, ResidentBytes::new(0)),
            Phase::Producing { .. } => unreachable!("producing returned above"),
        };
        state.ready_bytes = state
            .ready_bytes
            .checked_sub(bytes.get())
            .ok_or(AssetError::ResidentBytesOverflow)?;
        if next_generation.is_some() {
            state.free.insert(id.slot);
        }
        let report = RemoveReport {
            id,
            removed_generation: generation,
            removed_bytes: bytes,
        };
        drop(state);
        drop(entry);
        Ok(report)
    }

    pub(crate) fn collect(&self, budget: CacheBudget) -> Result<CollectReport<K>, AssetError> {
        let mut state = self.lock()?;
        let before = state.ready_bytes;
        let mut candidates = Vec::new();
        for slot in &state.slots {
            let Some(entry) = slot.entry.as_ref() else {
                continue;
            };
            if Arc::strong_count(&entry.token) != 1 {
                continue;
            }
            let eligible = matches!(entry.phase, Phase::Ready(_))
                || matches!(
                    entry.phase,
                    Phase::Failed {
                        previous: Some(_),
                        ..
                    }
                );
            if eligible {
                candidates.push((entry.last_touch, entry.token.id));
            }
        }
        candidates.sort_unstable();
        let mut evicted = Vec::new();
        let mut retired = Vec::new();
        for (_, id) in candidates {
            if state.ready_bytes <= budget.max_resident_bytes() {
                break;
            }
            let entry = self.entry_mut(&mut state, id)?;
            let old = std::mem::replace(&mut entry.phase, Phase::Missing);
            let (ready, replacement) = match old {
                Phase::Ready(ready) => (ready, Phase::Missing),
                Phase::Failed {
                    attempt,
                    error,
                    previous: Some(ready),
                } => (
                    ready,
                    Phase::Failed {
                        attempt,
                        error,
                        previous: None,
                    },
                ),
                other => {
                    entry.phase = other;
                    continue;
                }
            };
            entry.phase = replacement;
            state.ready_bytes = state
                .ready_bytes
                .checked_sub(ready.bytes.get())
                .ok_or(AssetError::ResidentBytesOverflow)?;
            evicted.push(EvictedAsset {
                id,
                generation: ready.generation,
                bytes: ready.bytes,
            });
            retired.push(ready.value);
        }
        let after = state.ready_bytes;
        drop(state);
        drop(retired);
        Ok(CollectReport {
            before: ResidentBytes::new(before),
            after: ResidentBytes::new(after),
            evicted,
        })
    }

    pub(crate) fn cache_stats(&self) -> Result<CacheStats, AssetError> {
        let state = self.lock()?;
        let ready_entries = state
            .slots
            .iter()
            .filter_map(|slot| slot.entry.as_ref())
            .filter(|entry| {
                matches!(entry.phase, Phase::Ready(_))
                    || matches!(
                        entry.phase,
                        Phase::Failed {
                            previous: Some(_),
                            ..
                        }
                    )
                    || matches!(
                        entry.phase,
                        Phase::Producing {
                            previous: Some(_),
                            ..
                        }
                    )
            })
            .count();
        Ok(CacheStats {
            ready_entries,
            resident_bytes: ResidentBytes::new(state.ready_bytes),
        })
    }
}

impl<K: AssetKind, V, E> Drop for StoreInner<K, V, E> {
    fn drop(&mut self) {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let cells: Vec<_> = state
            .slots
            .iter()
            .filter_map(|slot| slot.entry.as_ref())
            .filter_map(|entry| match &entry.phase {
                Phase::Producing { cell, .. } => Some(Arc::clone(cell)),
                _ => None,
            })
            .collect();
        drop(state);
        for cell in cells {
            cell.complete(Err(AssetError::StoreClosed));
        }
    }
}
