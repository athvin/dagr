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
//!
//! # No runtime, on purpose
//!
//! Everything here is `std`: a queue behind a mutex and a list of
//! [`std::task::Waker`]s.
//! There is no channel type and no timer, because a channel and a timer belong to
//! a runtime and this crate names none — the caller that owns one drives these
//! futures. A watch that must say *nothing* is then the simplest case rather than
//! the awkward one: it parks its waker and is never woken, which is precisely
//! what a blackholed control plane looks like from inside a client.

use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use crate::api::{ApiFailure, PodApi, PodListing, PodSnapshot, PodWatch, WatchDelivery};

/// How the fake reads the clock, for the one thing it measures: the gaps between
/// consecutive `watch` calls, which is the reconnect backoff.
///
/// Injectable because the only interesting clock is a *test's* clock. A suite
/// driving the observer under a paused runtime clock passes that runtime's `now`,
/// and the backoff it asserts is the schedule rather than the wall time; a suite
/// with no runtime gets `std` and measures nothing it looks at.
pub type FakeClock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// Build a fake API and the control handle that drives it, reading the system
/// clock.
#[must_use]
pub fn fake_api() -> (FakeApi, FakeControl) {
    fake_api_with_clock(Arc::new(Instant::now))
}

/// Build a fake API and its control handle, reading `clock` wherever the fake
/// timestamps anything.
#[must_use]
pub fn fake_api_with_clock(clock: FakeClock) -> (FakeApi, FakeControl) {
    let state = Arc::new(Mutex::new(FakeState {
        pods: BTreeMap::new(),
        collection_version: "0".to_string(),
        list_failures: Vec::new(),
        watch_failures: Vec::new(),
        stream: None,
        generations: 0,
        lists: 0,
        watch_opens: 0,
        watch_versions: Vec::new(),
        watch_open_times: Vec::new(),
        selectors: Vec::new(),
        observers: Vec::new(),
        clock,
    }));
    (
        FakeApi {
            state: Arc::clone(&state),
        },
        FakeControl { state },
    )
}

/// The open watch: what has been delivered to it, and whether its producer is
/// still attached.
#[derive(Debug)]
struct Stream {
    /// Which `watch` call opened this one. A handle from an earlier call reads
    /// end-of-stream rather than another call's deliveries.
    generation: u64,
    /// Deliveries not yet read. Kept as a queue rather than handed over directly
    /// so that ending the stream **drains** first: a transport failure delivers
    /// its error item and *then* ends, and a fake that dropped the item would be
    /// testing a different failure.
    queue: VecDeque<WatchDelivery>,
    /// Whether the producer is still attached. `false` plus an empty queue is
    /// end-of-stream.
    open: bool,
    /// The consumer parked on an empty queue — the stall, when nothing ever
    /// wakes it.
    waker: Option<Waker>,
}

/// Everything the fake knows, behind one lock.
struct FakeState {
    pods: BTreeMap<String, PodSnapshot>,
    collection_version: String,
    /// Scripted failures, consumed in order, newest last.
    list_failures: Vec<ApiFailure>,
    watch_failures: Vec<ApiFailure>,
    stream: Option<Stream>,
    generations: u64,
    lists: u32,
    watch_opens: u32,
    watch_versions: Vec<String>,
    watch_open_times: Vec<Instant>,
    selectors: Vec<String>,
    /// Callers parked on a state predicate — woken whenever the fake is touched,
    /// so a test can await "the observer has re-listed" instead of polling.
    /// Polling would keep a paused runtime busy and stop it advancing to the
    /// deadline the test is waiting for.
    observers: Vec<Waker>,
    clock: FakeClock,
}

impl std::fmt::Debug for FakeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeState")
            .field("pods", &self.pods)
            .field("collection_version", &self.collection_version)
            .field("lists", &self.lists)
            .field("watch_opens", &self.watch_opens)
            .field("watch_versions", &self.watch_versions)
            .field("selectors", &self.selectors)
            .finish_non_exhaustive()
    }
}

/// Take the lock, tolerating poison.
///
/// A test that panicked while holding this lock has already failed; re-panicking
/// in every accessor would replace its diagnostic with a poison error and turn a
/// dozen simple getters into documented panic sites.
fn lock(state: &Mutex<FakeState>) -> MutexGuard<'_, FakeState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Wake a batch of parked callers, with no lock held.
///
/// Collected first and woken after the guard is dropped, because waking under
/// the lock invites a re-entrant poll to deadlock on it.
fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

impl FakeState {
    /// Everyone parked on a predicate, to be woken once the lock is released.
    fn observers(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.observers)
    }

    fn watch_is_open(&self) -> bool {
        self.stream.as_ref().is_some_and(|stream| stream.open)
    }
}

/// The [`PodApi`] half: what the observer talks to.
#[derive(Debug, Clone)]
pub struct FakeApi {
    state: Arc<Mutex<FakeState>>,
}

impl PodApi for FakeApi {
    type Watch = FakeWatch;

    fn list(&self, selector: &str) -> impl Future<Output = Result<PodListing, ApiFailure>> + Send {
        let state = Arc::clone(&self.state);
        let selector = selector.to_string();
        async move {
            let (result, parked) = {
                let mut guard = lock(&state);
                guard.lists += 1;
                guard.selectors.push(selector);
                let result = if guard.list_failures.is_empty() {
                    Ok(PodListing {
                        resource_version: guard.collection_version.clone(),
                        pods: guard.pods.values().cloned().collect(),
                    })
                } else {
                    Err(guard.list_failures.remove(0))
                };
                (result, guard.observers())
            };
            wake_all(parked);
            result
        }
    }

    fn watch(
        &self,
        selector: &str,
        resource_version: &str,
    ) -> impl Future<Output = Result<Self::Watch, ApiFailure>> + Send {
        let state = Arc::clone(&self.state);
        let selector = selector.to_string();
        let resource_version = resource_version.to_string();
        async move {
            let (result, parked) = {
                let mut guard = lock(&state);
                guard.watch_opens += 1;
                guard.selectors.push(selector);
                guard.watch_versions.push(resource_version);
                let now = (guard.clock)();
                guard.watch_open_times.push(now);
                let result = if guard.watch_failures.is_empty() {
                    guard.generations += 1;
                    let generation = guard.generations;
                    guard.stream = Some(Stream {
                        generation,
                        queue: VecDeque::new(),
                        open: true,
                        waker: None,
                    });
                    Ok(FakeWatch {
                        state: Arc::clone(&state),
                        generation,
                    })
                } else {
                    guard.stream = None;
                    Err(guard.watch_failures.remove(0))
                };
                (result, guard.observers())
            };
            wake_all(parked);
            result
        }
    }
}

/// One open watch handed back by [`FakeApi::watch`].
///
/// Dropping it closes the fake's stream, which is how "the watch is torn down
/// with the run" is observed: [`FakeControl::watch_is_open`] goes false without
/// anything being told to make it so.
#[derive(Debug)]
pub struct FakeWatch {
    state: Arc<Mutex<FakeState>>,
    generation: u64,
}

impl PodWatch for FakeWatch {
    fn next(&mut self) -> impl Future<Output = Option<WatchDelivery>> + Send {
        let state = Arc::clone(&self.state);
        let generation = self.generation;
        poll_fn(move |cx| {
            let mut guard = lock(&state);
            let Some(stream) = guard
                .stream
                .as_mut()
                .filter(|stream| stream.generation == generation)
            else {
                // Superseded or closed: this handle's stream has ended.
                return Poll::Ready(None);
            };
            if let Some(delivery) = stream.queue.pop_front() {
                return Poll::Ready(Some(delivery));
            }
            if !stream.open {
                return Poll::Ready(None);
            }
            stream.waker = Some(cx.waker().clone());
            Poll::Pending
        })
    }
}

impl Drop for FakeWatch {
    fn drop(&mut self) {
        let parked = {
            let mut guard = lock(&self.state);
            if guard
                .stream
                .as_ref()
                .is_some_and(|stream| stream.generation == self.generation)
            {
                guard.stream = None;
            }
            guard.observers()
        };
        wake_all(parked);
    }
}

/// The driving half: what a test uses to make things happen.
#[derive(Debug, Clone)]
pub struct FakeControl {
    state: Arc<Mutex<FakeState>>,
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
    /// Parks on a waker rather than spinning, so a paused clock can advance to
    /// whatever deadline the observer is sitting on.
    pub async fn await_watch(&self) {
        self.await_state(FakeState::watch_is_open).await;
    }

    /// Wait until at least `count` `list` calls have been made.
    ///
    /// The counter, not the watch, is what a test synchronises on when it wants
    /// "the observer has re-listed" — a watch that is *still* open is not
    /// evidence that the observer has yet reacted to the frame just delivered.
    pub async fn await_lists(&self, count: u32) {
        self.await_state(move |state| state.lists >= count).await;
    }

    /// Wait until at least `count` `watch` calls have been made, successful or
    /// not.
    pub async fn await_watch_opens(&self, count: u32) {
        self.await_state(move |state| state.watch_opens >= count)
            .await;
    }

    async fn await_state(&self, ready: impl Fn(&FakeState) -> bool) {
        poll_fn(|cx| {
            let mut guard = lock(&self.state);
            if ready(&guard) {
                return Poll::Ready(());
            }
            guard.observers.push(cx.waker().clone());
            Poll::Pending
        })
        .await;
    }

    /// Push one item into the open watch, waiting for one to be open first.
    ///
    /// # Panics
    ///
    /// Panics if no watch is open once one was observed to be — a delivery that
    /// went nowhere is a test failure, and a silent drop would surface later as
    /// an unexplained timeout somewhere else.
    pub async fn deliver(&self, delivery: WatchDelivery) {
        self.await_watch().await;
        let (consumer, parked) = {
            let mut guard = lock(&self.state);
            let stream = guard
                .stream
                .as_mut()
                .expect("a watch is open when a delivery is pushed");
            stream.queue.push_back(delivery);
            let consumer = stream.waker.take();
            (consumer, guard.observers())
        };
        if let Some(consumer) = consumer {
            consumer.wake();
        }
        wake_all(parked);
    }

    /// End the open stream cleanly — the shape a transport failure leaves behind.
    ///
    /// Anything already delivered is still read first: an error item followed by
    /// the end of the stream is one termination, not two.
    pub async fn end_stream(&self) {
        self.await_watch().await;
        let (consumer, parked) = {
            let mut guard = lock(&self.state);
            let consumer = guard.stream.as_mut().and_then(|stream| {
                stream.open = false;
                stream.waker.take()
            });
            (consumer, guard.observers())
        };
        if let Some(consumer) = consumer {
            consumer.wake();
        }
        wake_all(parked);
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

    /// The intervals between consecutive `watch` calls — the backoff, measured
    /// against whatever clock this fake was built with.
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
