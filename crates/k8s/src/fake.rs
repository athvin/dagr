//! **The fake API surface** — an in-process [`PodApi`] whose failures are
//! scripted.
//!
//! The ticket's requirement is not "a fake exists" but that a `resourceVersion`
//! expiry, a **silent stall**, and a **duplicate delivery** are all *inducible*.
//! A fake that cannot produce those does not test the observer, and neither does
//! a real cluster: a fresh cluster cannot produce a 410 at all until its watch
//! cache has aged, and the only reliable way to produce silence is to blackhole
//! the control plane. Scripting them here is what makes the discipline's tests
//! deterministic on both CI platforms, in microseconds, with no cluster.
//!
//! What it deliberately does **not** fake is the wire. The client adapter's own
//! tests push *recorded* frames through the real deserializer, so classification
//! is aimed at the bytes a server actually sends; this fake is aimed at the state
//! machine those bytes drive.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, Instant};

use crate::api::{
    ApiFailure, PodApi, PodListing, PodSnapshot, PodWatch, WATCH_BUFFER, WatchDelivery,
};

/// Build a fake API and the control handle that drives it.
#[must_use]
pub fn fake_api() -> (FakeApi, FakeControl) {
    let (changes, observed) = watch::channel(0_u64);
    let state = Arc::new(Mutex::new(FakeState {
        pods: BTreeMap::new(),
        collection_version: "0".to_string(),
        list_failures: Vec::new(),
        watch_failures: Vec::new(),
        sender: None,
        lists: 0,
        watch_opens: 0,
        watch_versions: Vec::new(),
        watch_open_times: Vec::new(),
        selectors: Vec::new(),
        changes,
    }));
    (
        FakeApi {
            state: Arc::clone(&state),
        },
        FakeControl { state, observed },
    )
}

/// Everything the fake knows, behind one lock.
#[derive(Debug)]
struct FakeState {
    pods: BTreeMap<String, PodSnapshot>,
    collection_version: String,
    /// Scripted failures, consumed in order, newest last.
    list_failures: Vec<ApiFailure>,
    watch_failures: Vec<ApiFailure>,
    sender: Option<mpsc::Sender<WatchDelivery>>,
    lists: u32,
    watch_opens: u32,
    watch_versions: Vec<String>,
    watch_open_times: Vec<Instant>,
    selectors: Vec<String>,
    /// Bumped whenever a watch opens or closes, so a caller can await the change
    /// instead of polling — polling under a paused clock keeps the runtime busy
    /// and stops it from advancing to the deadline the test is waiting for.
    changes: watch::Sender<u64>,
}

/// Take the lock, tolerating poison.
///
/// A test that panicked while holding this lock has already failed; re-panicking
/// in every accessor would replace its diagnostic with a poison error and turn a
/// dozen simple getters into documented panic sites.
fn lock(state: &Mutex<FakeState>) -> MutexGuard<'_, FakeState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FakeState {
    fn bump(&mut self) {
        self.changes.send_modify(|version| *version += 1);
    }

    fn watch_is_open(&self) -> bool {
        self.sender
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
    }
}

/// The [`PodApi`] half: what the observer talks to.
#[derive(Debug, Clone)]
pub struct FakeApi {
    state: Arc<Mutex<FakeState>>,
}

impl PodApi for FakeApi {
    fn list(&self, selector: &str) -> impl Future<Output = Result<PodListing, ApiFailure>> + Send {
        let state = Arc::clone(&self.state);
        let selector = selector.to_string();
        async move {
            let mut guard = lock(&state);
            guard.lists += 1;
            guard.selectors.push(selector);
            guard.bump();
            if !guard.list_failures.is_empty() {
                return Err(guard.list_failures.remove(0));
            }
            Ok(PodListing {
                resource_version: guard.collection_version.clone(),
                pods: guard.pods.values().cloned().collect(),
            })
        }
    }

    fn watch(
        &self,
        selector: &str,
        resource_version: &str,
    ) -> impl Future<Output = Result<PodWatch, ApiFailure>> + Send {
        let state = Arc::clone(&self.state);
        let selector = selector.to_string();
        let resource_version = resource_version.to_string();
        async move {
            let mut guard = lock(&state);
            guard.watch_opens += 1;
            guard.selectors.push(selector);
            guard.watch_versions.push(resource_version);
            guard.watch_open_times.push(Instant::now());
            if !guard.watch_failures.is_empty() {
                let failure = guard.watch_failures.remove(0);
                guard.sender = None;
                guard.bump();
                return Err(failure);
            }
            let (sender, receiver) = mpsc::channel(WATCH_BUFFER);
            guard.sender = Some(sender);
            guard.bump();
            Ok(PodWatch::from_channel(receiver))
        }
    }
}

/// The driving half: what a test uses to make things happen.
#[derive(Debug, Clone)]
pub struct FakeControl {
    state: Arc<Mutex<FakeState>>,
    observed: watch::Receiver<u64>,
}

impl FakeControl {
    /// Put a pod into the state a `list` returns, and move the collection's
    /// version to the pod's. This is the *world*, not an event: a change made
    /// here is invisible until something lists or the test delivers an event for
    /// it, which is exactly how a transition happens during a watch gap.
    pub fn upsert(&self, pod: PodSnapshot) {
        let mut guard = lock(&self.state);
        guard.collection_version.clone_from(&pod.resource_version);
        guard.pods.insert(pod.name.clone(), pod);
    }

    /// Remove a pod from the world.
    pub fn remove(&self, name: &str) {
        lock(&self.state).pods.remove(name);
    }

    /// Wait until a watch is open.
    ///
    /// Blocks on a change notification rather than spinning, so a paused clock
    /// can advance to whatever deadline the observer is parked on.
    ///
    /// # Panics
    ///
    /// Panics if the observer stops before a watch is open — a test that waited
    /// for something that will never happen should fail loudly rather than hang.
    pub async fn await_watch(&self) {
        self.await_state(FakeState::watch_is_open).await;
    }

    /// Wait until at least `count` `list` calls have been made.
    ///
    /// The counter, not the watch, is what a test synchronises on when it wants
    /// "the observer has re-listed" — a watch that is *still* open is not
    /// evidence that the observer has yet reacted to the frame just delivered.
    ///
    /// # Panics
    ///
    /// Panics if the observer stops before the count is reached.
    pub async fn await_lists(&self, count: u32) {
        self.await_state(move |state| state.lists >= count).await;
    }

    /// Wait until at least `count` `watch` calls have been made, successful or
    /// not.
    ///
    /// # Panics
    ///
    /// Panics if the observer stops before the count is reached.
    pub async fn await_watch_opens(&self, count: u32) {
        self.await_state(move |state| state.watch_opens >= count)
            .await;
    }

    async fn await_state(&self, ready: impl Fn(&FakeState) -> bool) {
        let mut observed = self.observed.clone();
        loop {
            if ready(&lock(&self.state)) {
                return;
            }
            observed
                .changed()
                .await
                .expect("the observer stopped before the fake reached the expected state");
        }
    }

    /// Push one item into the open watch, waiting for one to be open first.
    ///
    /// # Panics
    ///
    /// Panics if the observer stops, or stops reading, before the item lands —
    /// both are test failures, and a silent drop would surface later as an
    /// unexplained timeout somewhere else.
    pub async fn deliver(&self, delivery: WatchDelivery) {
        self.await_watch().await;
        let sender = lock(&self.state).sender.clone();
        sender
            .expect("a watch is open")
            .send(delivery)
            .await
            .expect("the observer is reading its watch");
    }

    /// End the open stream cleanly — the shape a transport failure leaves behind.
    ///
    /// # Panics
    ///
    /// Panics if the observer stops before a watch is open.
    pub async fn end_stream(&self) {
        self.await_watch().await;
        let mut guard = lock(&self.state);
        guard.sender = None;
        guard.bump();
    }

    /// Script one failure for the next `list`.
    pub fn fail_next_list(&self, failure: ApiFailure) {
        lock(&self.state).list_failures.push(failure);
    }

    /// Script one failure for the next `watch`.
    pub fn fail_next_watch(&self, failure: ApiFailure) {
        lock(&self.state).watch_failures.push(failure);
    }

    /// How many `list` calls have been made.
    #[must_use]
    pub fn lists(&self) -> u32 {
        lock(&self.state).lists
    }

    /// How many `watch` calls have been made, successful or not.
    #[must_use]
    pub fn watch_opens(&self) -> u32 {
        lock(&self.state).watch_opens
    }

    /// The resource versions each `watch` was opened from, in order. The
    /// assertion that a 410's own version is never resumed from reads this.
    #[must_use]
    pub fn watch_resource_versions(&self) -> Vec<String> {
        lock(&self.state).watch_versions.clone()
    }

    /// The intervals between consecutive `watch` calls — the backoff, measured.
    #[must_use]
    pub fn watch_open_gaps(&self) -> Vec<Duration> {
        lock(&self.state)
            .watch_open_times
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]))
            .collect()
    }

    /// Every selector the observer has listed or watched with.
    #[must_use]
    pub fn selectors(&self) -> Vec<String> {
        lock(&self.state).selectors.clone()
    }

    /// Whether a watch is open right now. Becomes false as soon as the observer
    /// drops its handle, which is how teardown is asserted.
    #[must_use]
    pub fn watch_is_open(&self) -> bool {
        lock(&self.state).watch_is_open()
    }

    /// How many watches are open right now — never more than one, which is the
    /// whole point of a *shared* observer.
    #[must_use]
    pub fn open_watches(&self) -> usize {
        usize::from(self.watch_is_open())
    }
}
