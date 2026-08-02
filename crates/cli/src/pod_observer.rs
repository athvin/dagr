//! **The pod-observer task** — the runtime half of the shared observer (M10,
//! T107, ADR 115 §2).
//!
//! `dagr-k8s` holds the deterministic half: `ObserverCore`, the reconnect
//! discipline, the identity encoding, the `PodApi` port. It performs no I/O,
//! reads no clock and spawns nothing, and it names **no async runtime** —
//! deliberately, because ADR 004 places the runtime in the crate that owns the
//! run loop and this is that crate.
//!
//! What lives here is everything that needs one: the spawned task that owns
//! **one** watch, the timer that bounds a silence and spaces a reconnect, the
//! command channel that registers waiters, and the routing table that
//! demultiplexes observations to them. "One watch per orchestrator *process*"
//! is a claim about this process, so the singleton lives in the process rather
//! than in the library it drives.
//!
//! The split is what makes the discipline testable without a cluster: the core
//! is a sequence of function calls, and this task is driven against
//! `dagr_k8s::fake` under a paused clock.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use dagr_k8s::api::{PodApi, PodWatch};
use dagr_k8s::identity::AttemptKey;
use dagr_k8s::observer::{
    Delivery, ForeignPod, ObserverAction, ObserverCore, ObserverFailure, ObserverInput,
    ObserverLimits, ObserverStats, PodObservation, RunSelector,
};

/// What a waiter is told.
#[derive(Debug, Clone)]
pub enum WaiterEvent {
    /// Its pod was observed. Boxed for the same reason `Delivery::Observed` is.
    Observed(Box<PodObservation>),
    /// The observer stopped, and no further observation will arrive. Sent to
    /// every outstanding waiter, because a caller blocked on a pod nobody is
    /// watching for is a hung run.
    ObserverFailed(Arc<ObserverFailure>),
}

/// Why a waiter produced no observation.
#[derive(Debug, Clone)]
pub enum WaiterError {
    /// The observer stopped with a classified failure.
    ObserverFailed(Arc<ObserverFailure>),
    /// The observer shut down, or the attempt was already retired.
    Closed,
}

impl fmt::Display for WaiterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaiterError::ObserverFailed(failure) => write!(f, "{failure}"),
            WaiterError::Closed => f.write_str("the pod observer closed this waiter"),
        }
    }
}

impl std::error::Error for WaiterError {}

/// One attempt's view of the shared watch.
#[derive(Debug)]
pub struct AttemptWaiter {
    receiver: mpsc::UnboundedReceiver<WaiterEvent>,
}

impl AttemptWaiter {
    /// The next thing to happen to this attempt's pod, or `None` once the
    /// attempt is retired — by a terminal phase, by a disappearance, or by the
    /// observer shutting down.
    pub async fn next(&mut self) -> Option<WaiterEvent> {
        self.receiver.recv().await
    }

    /// Wait for the observation that retires this attempt.
    ///
    /// # Errors
    ///
    /// Returns [`WaiterError`] if the observer stopped with a classified failure,
    /// or if the waiter closed without a final observation.
    pub async fn terminal(&mut self) -> Result<PodObservation, WaiterError> {
        while let Some(event) = self.receiver.recv().await {
            match event {
                WaiterEvent::Observed(observation) if observation.is_final() => {
                    return Ok(*observation);
                }
                WaiterEvent::Observed(_) => {}
                WaiterEvent::ObserverFailed(failure) => {
                    return Err(WaiterError::ObserverFailed(failure));
                }
            }
        }
        Err(WaiterError::Closed)
    }
}

/// The observer is no longer accepting waiters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObserverStopped;

impl fmt::Display for ObserverStopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the pod observer has stopped and is not accepting waiters")
    }
}

impl std::error::Error for ObserverStopped {}

/// Teardown did not finish inside the budget it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownOverrun {
    /// The budget that was not met.
    pub budget: Duration,
}

impl fmt::Display for ShutdownOverrun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the pod observer did not finish teardown inside {:?}; its task was cancelled",
            self.budget
        )
    }
}

impl std::error::Error for ShutdownOverrun {}

/// What the observer saw, once it has stopped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObserverReport {
    /// Pods that matched the selector and were not this run's.
    pub foreign: Vec<ForeignPod>,
    /// The classified failure that ended it, if one did.
    pub failure: Option<ObserverFailure>,
    /// What it did.
    pub stats: ObserverStats,
}

/// A cloneable way to register waiters with a running observer.
#[derive(Debug, Clone)]
pub struct ObserverHandle {
    commands: mpsc::UnboundedSender<Command>,
}

impl ObserverHandle {
    /// Register a waiter for one attempt's pod.
    ///
    /// Observations that arrived before the waiter registered are buffered and
    /// replayed to it, because registering and submitting are two acts and a pod
    /// that is already terminal on the first reconciliation is a real ordering.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverStopped`] if the observer has stopped — refused rather
    /// than accepted, because a waiter nobody is watching for would hang.
    pub async fn watch_attempt(&self, key: AttemptKey) -> Result<AttemptWaiter, ObserverStopped> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Register { key, reply })
            .map_err(|_| ObserverStopped)?;
        response.await.map_err(|_| ObserverStopped)
    }
}

/// **One** watch per orchestrator process, over one run's pods.
#[derive(Debug)]
pub struct PodObserver {
    handle: ObserverHandle,
    join: Option<JoinHandle<ObserverReport>>,
}

impl PodObserver {
    /// Start the observer on the current runtime.
    ///
    /// It lists immediately, then watches, and keeps doing so until it is shut
    /// down or its retry budget is exhausted.
    #[must_use]
    pub fn spawn<A: PodApi>(api: A, selector: RunSelector, limits: ObserverLimits) -> Self {
        let (commands, inbox) = mpsc::unbounded_channel();
        let join = tokio::spawn(observe(api, selector, limits, inbox));
        Self {
            handle: ObserverHandle { commands },
            join: Some(join),
        }
    }

    /// A cloneable registration handle, for a caller that owns pod submission
    /// somewhere else.
    #[must_use]
    pub fn handle(&self) -> ObserverHandle {
        self.handle.clone()
    }

    /// Register a waiter for one attempt's pod.
    ///
    /// # Errors
    ///
    /// Returns [`ObserverStopped`] if the observer has stopped.
    pub async fn watch_attempt(&self, key: AttemptKey) -> Result<AttemptWaiter, ObserverStopped> {
        self.handle.watch_attempt(key).await
    }

    /// Stop the observer and take its report, inside `budget`.
    ///
    /// The watch is torn down and the task is joined. If the task has not
    /// finished when the budget elapses it is cancelled rather than waited on:
    /// the budget belongs to the process's kill window, and overrunning it would
    /// spend somebody else's.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownOverrun`] if teardown did not complete inside `budget`.
    pub async fn shutdown(mut self, budget: Duration) -> Result<ObserverReport, ShutdownOverrun> {
        let _ = self.handle.commands.send(Command::Shutdown);
        let Some(mut join) = self.join.take() else {
            return Err(ShutdownOverrun { budget });
        };
        match tokio::time::timeout(budget, &mut join).await {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(_)) => Err(ShutdownOverrun { budget }),
            Err(_) => {
                join.abort();
                Err(ShutdownOverrun { budget })
            }
        }
    }
}

impl Drop for PodObserver {
    /// An observer that is dropped without a shutdown still takes its task with
    /// it. "The observer's task does not outlive the run" is then a property of
    /// the type rather than of remembering to call something.
    fn drop(&mut self) {
        let _ = self.handle.commands.send(Command::Shutdown);
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

/// What the task accepts from outside.
#[derive(Debug)]
enum Command {
    Register {
        key: AttemptKey,
        reply: oneshot::Sender<AttemptWaiter>,
    },
    Shutdown,
}

/// The routing table: one sender per registered attempt, plus the buffer for
/// observations that arrived before their waiter did.
#[derive(Debug, Default)]
struct Routes {
    live: BTreeMap<AttemptKey, mpsc::UnboundedSender<WaiterEvent>>,
    pending: BTreeMap<AttemptKey, Vec<PodObservation>>,
}

impl Routes {
    fn register(&mut self, key: AttemptKey) -> AttemptWaiter {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut retired = false;
        if let Some(buffered) = self.pending.remove(&key) {
            for observation in buffered {
                retired |= observation.is_final();
                let _ = sender.send(WaiterEvent::Observed(Box::new(observation)));
            }
        }
        if !retired {
            self.live.insert(key, sender);
        }
        AttemptWaiter { receiver }
    }

    fn route(&mut self, observation: Box<PodObservation>) {
        let key = observation.key.clone();
        let is_final = observation.is_final();
        match self.live.get(&key) {
            Some(sender) => {
                let _ = sender.send(WaiterEvent::Observed(observation));
                if is_final {
                    self.live.remove(&key);
                }
            }
            None => self.pending.entry(key).or_default().push(*observation),
        }
    }

    fn broadcast_failure(&mut self, failure: &Arc<ObserverFailure>) {
        for sender in self.live.values() {
            let _ = sender.send(WaiterEvent::ObserverFailed(Arc::clone(failure)));
        }
        self.live.clear();
    }
}

/// The task: the core, a `PodApi`, a timer, and the routing table.
async fn observe<A: PodApi>(
    api: A,
    selector: RunSelector,
    limits: ObserverLimits,
    mut inbox: mpsc::UnboundedReceiver<Command>,
) -> ObserverReport {
    let started = Instant::now();
    let mut core = ObserverCore::new(selector, limits);
    let label_selector = core.label_selector();
    let mut routes = Routes::default();
    let mut report = ObserverReport::default();
    // A stray pod matching the selector would otherwise be re-reported on every
    // reconciliation, turning a long run's report into a transcript of one pod.
    let mut reported_foreign: BTreeSet<String> = BTreeSet::new();
    let mut watch: Option<A::Watch> = None;
    let mut action = ObserverCore::initial_action();

    'outer: loop {
        let input = match action.clone() {
            ObserverAction::Fail(failure) => {
                let shared = Arc::new(failure.clone());
                routes.broadcast_failure(&shared);
                report.failure = Some(failure);
                break 'outer;
            }
            ObserverAction::Relist { delay } => {
                // Drop the old watch BEFORE the call, so a reconnect never holds
                // two open at once.
                watch = None;
                if !wait(delay, &mut inbox, &mut routes).await {
                    break 'outer;
                }
                match api.list(&label_selector).await {
                    Ok(listing) => ObserverInput::Listed(listing),
                    Err(failure) => ObserverInput::ListFailed(failure),
                }
            }
            ObserverAction::Resume {
                resource_version,
                delay,
            } => {
                watch = None;
                if !wait(delay, &mut inbox, &mut routes).await {
                    break 'outer;
                }
                match api.watch(&label_selector, &resource_version).await {
                    Ok(opened) => {
                        watch = Some(opened);
                        ObserverInput::WatchEstablished
                    }
                    Err(failure) => ObserverInput::WatchFailed(failure),
                }
            }
            ObserverAction::Read { stall_deadline } => {
                let Some(open) = watch.as_mut() else {
                    action = ObserverCore::initial_action();
                    continue 'outer;
                };
                tokio::select! {
                    command = inbox.recv() => {
                        match command {
                            Some(Command::Register { key, reply }) => {
                                let _ = reply.send(routes.register(key));
                                continue 'outer;
                            }
                            Some(Command::Shutdown) | None => break 'outer,
                        }
                    }
                    item = tokio::time::timeout(stall_deadline, open.next()) => {
                        match item {
                            Ok(Some(delivery)) => ObserverInput::Delivered(delivery),
                            Ok(None) => ObserverInput::StreamEnded,
                            Err(_elapsed) => ObserverInput::Tick,
                        }
                    }
                }
            }
        };

        let step = core.step(started.elapsed(), input);
        for delivery in step.deliveries {
            match delivery {
                Delivery::Observed(observation) => routes.route(observation),
                Delivery::Foreign(foreign) => {
                    if reported_foreign.insert(foreign.pod_name.clone()) {
                        report.foreign.push(foreign);
                    }
                }
            }
        }
        action = step.action;
    }

    // Teardown: the watch goes with the run. Dropping the handle is the whole of
    // it — whatever backs it (a client's stream, the fake's queue) goes with it.
    drop(watch);
    report.stats = core.stats();
    report
}

/// Wait `delay`, serving registrations meanwhile.
///
/// Returns `false` if the observer was asked to stop, so a shutdown that arrives
/// during a backoff is honoured immediately rather than after it.
async fn wait(
    delay: Duration,
    inbox: &mut mpsc::UnboundedReceiver<Command>,
    routes: &mut Routes,
) -> bool {
    if delay.is_zero() {
        // Still drain any registration that is already queued, so a waiter
        // registered before the first list is in the table when it happens.
        while let Ok(command) = inbox.try_recv() {
            match command {
                Command::Register { key, reply } => {
                    let _ = reply.send(routes.register(key));
                }
                Command::Shutdown => return false,
            }
        }
        return true;
    }

    let deadline = Instant::now() + delay;
    loop {
        tokio::select! {
            command = inbox.recv() => {
                match command {
                    Some(Command::Register { key, reply }) => {
                        let _ = reply.send(routes.register(key));
                    }
                    Some(Command::Shutdown) | None => return false,
                }
            }
            () = tokio::time::sleep_until(deadline) => return true,
        }
    }
}
