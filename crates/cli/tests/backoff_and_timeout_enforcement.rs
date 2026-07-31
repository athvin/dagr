//! **Real retry backoff and per-attempt timeout on the run-flow path.**
//!
//! Two policies that `NodePolicy` declares, the graph artifact records, and the
//! policy fingerprint hashes were **not enforced** on the path every `#[dag]` and
//! `RunnableFlow` pipeline actually runs:
//!
//! 1. the retry backoff delay was never slept (the retry loop was handed a timer
//!    future that resolved immediately, so the emitted `BackoffStarted` delay was a
//!    claim about wait time that never elapsed);
//! 2. no per-attempt timeout was armed (a hanging task hung the run until the
//!    operator killed it).
//!
//! This suite pins both, per class:
//!
//! - **Backoff elapses.** The wall-clock gap between a failing attempt and the next
//!   attempt is at least the scheduled delay, and — through an **injected** timer —
//!   the scheduled delays are exactly `Backoff::nominal_delay` under `NoJitter`,
//!   capped.
//! - **Await-bound timeout.** A node that awaits forever reaches `timed-out` at its
//!   deadline, releases its permit (a node waiting for that capacity is admitted),
//!   and a retry-eligible timeout with budget left makes a further attempt while the
//!   node still records exactly one terminal state.
//! - **Blocking / compute timeout.** An unkillable busy-loop is marked `timed-out`
//!   immediately (the mark is the framework's isolated timer, not the jammed task
//!   thread), its permit is **held** until the closure returns, no retry starts, the
//!   late result never fills the slot, and a still-live zombie at run end yields
//!   exactly one `zombie-at-exit` event after the bounded grace.
//! - **No regression.** A node with neither policy keeps its exact event shape.
//!
//! Every timing assertion is a **lower bound with slack** (a sleep never returns
//! early; a scheduler may return late), so the suite is portable to macOS CI. Each
//! run is driven on a watchdog thread, so a regression that reintroduces the hang
//! fails the test instead of wedging the suite.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome, read_records};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::{AttemptTimer, RunnableFlow};
use dagr_cli::run_store::SystemClock;
use dagr_core::TaskError;
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::RunContext;
use dagr_core::execution::Backoff;
use dagr_core::task::{ExecutionClass, Task};
use dagr_core::test_kit::TempBase;

// ===========================================================================
// Injection seams
// ===========================================================================

/// An in-memory event sink — the observable oracle for every assertion below.
#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().expect("sink not poisoned").clone()
    }
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines
            .lock()
            .expect("sink not poisoned")
            .extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A monotonic tick clock for the shape-only tests (no wall-clock reading).
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

/// A **recording** timer: it records every delay it is asked to wait and resolves
/// immediately, so the scheduled backoff sequence is assertable without the suite
/// actually sleeping. This is the timer-injection seam that mirrors the existing
/// `Jitter` injection.
#[derive(Clone, Default)]
struct RecordingTimer {
    delays: Arc<Mutex<Vec<Duration>>>,
}
impl RecordingTimer {
    fn recorded(&self) -> Vec<Duration> {
        self.delays.lock().expect("recorder not poisoned").clone()
    }
}
impl AttemptTimer for RecordingTimer {
    fn sleep(&self, delay: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.delays
            .lock()
            .expect("recorder not poisoned")
            .push(delay);
        Box::pin(std::future::ready(()))
    }
}

// ===========================================================================
// Tasks
// ===========================================================================

/// A trivial upstream: every policy-carrying node below is data-dependent, so each
/// test flow starts from this source.
struct Seed;
impl Task for Seed {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

/// Fails retryably on every attempt, recording the instant each attempt began — the
/// oracle for "the backoff actually elapsed between attempt N and attempt N+1".
struct AlwaysRetryable {
    starts: Arc<Mutex<Vec<Instant>>>,
}
impl Task for AlwaysRetryable {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: u64) -> Result<u64, TaskError> {
        self.starts
            .lock()
            .expect("starts not poisoned")
            .push(Instant::now());
        Err(TaskError::retryable("always transient"))
    }
}

/// Awaits forever — the await-bound hang the per-attempt timeout must cut.
struct AwaitsForever;
impl Task for AwaitsForever {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: u64) -> Result<u64, TaskError> {
        std::future::pending::<()>().await;
        unreachable!("a pending future never resolves")
    }
}

/// Hangs on its first attempt and succeeds on the second — the retry-after-timeout
/// oracle.
struct HangsThenSucceeds;
impl Task for HangsThenSucceeds {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, c: &RunContext, i: u64) -> Result<u64, TaskError> {
        if c.attempt() == 1 {
            std::future::pending::<()>().await;
            unreachable!("a pending future never resolves")
        }
        Ok(i + 41)
    }
}

/// An **unkillable** synchronous body: it busy-loops for `spin` regardless of any
/// timeout, counting its invocations. This is the blocking/compute zombie shape —
/// the framework cannot stop it, so it must be *marked* while it runs on.
struct BusyLoops {
    spin: Duration,
    runs: Arc<AtomicUsize>,
    class: ExecutionClass,
}
impl BusyLoops {
    fn blocking(spin: Duration, runs: Arc<AtomicUsize>) -> Self {
        Self {
            spin,
            runs,
            class: ExecutionClass::Blocking,
        }
    }
    fn compute(spin: Duration, runs: Arc<AtomicUsize>) -> Self {
        Self {
            spin,
            runs,
            class: ExecutionClass::Compute,
        }
    }
}
impl Task for BusyLoops {
    type Input = u64;
    type Output = u64;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;
    async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
        // The declared class is overridden per node by policy where the compute
        // surface is wanted; `class` records which surface this instance expects.
        let _ = self.class;
        self.runs.fetch_add(1, Ordering::SeqCst);
        let until = Instant::now() + self.spin;
        while Instant::now() < until {
            std::hint::spin_loop();
        }
        Ok(i + 7)
    }
}

/// Records how long after the run began it was admitted — the oracle for "the
/// permit was (not) released at the timeout mark".
struct RecordsStart {
    origin: Instant,
    started_after: Arc<Mutex<Option<Duration>>>,
}
impl Task for RecordsStart {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
        *self.started_after.lock().expect("start cell not poisoned") = Some(self.origin.elapsed());
        Ok(i)
    }
}

// ===========================================================================
// Stream helpers
// ===========================================================================

type Record = serde_json::Value;

fn records(bytes: &[u8]) -> Vec<Record> {
    read_records(bytes).expect("stream parses").records
}

fn str_field(r: &Record, key: &str) -> Option<String> {
    r.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn kind_of(r: &Record) -> String {
    str_field(r, "kind").unwrap_or_default()
}

/// The ordered `(kind, node, state)` shape — the byte-identical-stream oracle.
fn shape(bytes: &[u8]) -> Vec<(String, Option<String>, Option<String>)> {
    records(bytes)
        .iter()
        .map(|r| (kind_of(r), str_field(r, "node"), str_field(r, "state")))
        .collect()
}

fn terminal_of(bytes: &[u8], node: &str) -> String {
    records(bytes)
        .iter()
        .filter(|r| kind_of(r) == "node-terminal" && str_field(r, "node").as_deref() == Some(node))
        .filter_map(|r| str_field(r, "state"))
        .next_back()
        .unwrap_or_else(|| panic!("no node-terminal for `{node}`"))
}

fn count(bytes: &[u8], kind: &str, node: &str) -> usize {
    records(bytes)
        .iter()
        .filter(|r| kind_of(r) == kind && str_field(r, "node").as_deref() == Some(node))
        .count()
}

/// The monotonic offset of the **first** record of `kind` for `node` — the honest
/// in-stream timeline (durations are read off `offset_ns`, never off wall stamps).
fn offset_of(bytes: &[u8], kind: &str, node: &str) -> Duration {
    let ns = records(bytes)
        .iter()
        .find(|r| kind_of(r) == kind && str_field(r, "node").as_deref() == Some(node))
        .and_then(|r| r.get("offset_ns").and_then(serde_json::Value::as_u64))
        .unwrap_or_else(|| panic!("no `{kind}` record for `{node}`"));
    Duration::from_nanos(ns)
}

// ===========================================================================
// Watchdog — a hang is a test failure, never a wedged suite
// ===========================================================================

fn with_watchdog<F, T>(label: &str, budget: Duration, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(format!("watchdog-{label}"))
        .spawn(move || {
            let _ = tx.send(f());
        })
        .expect("watchdog thread spawns");
    rx.recv_timeout(budget).unwrap_or_else(|_| {
        panic!("`{label}` did not finish within {budget:?} — the enforced policy never fired")
    })
}

const SEED_NODE: &str = "seed";

// ===========================================================================
// Backoff actually elapses
// ===========================================================================

/// **The scheduled backoff is really waited.** A node with one retry and a 60 ms
/// base backoff fails retryably on both attempts; the wall-clock gap between the
/// two attempts is at least the scheduled delay. Before this ticket the retry loop
/// was handed a timer that resolved immediately, so the gap was ~0 and this failed.
#[test]
fn backoff_delay_actually_elapses_between_attempts() {
    let starts: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::new()));
    let starts_for_task = Arc::clone(&starts);
    let base = Duration::from_millis(60);

    let bytes = with_watchdog("backoff-elapses", Duration::from_secs(20), move || {
        let mut flow = RunnableFlow::new();
        let seed = flow.register_source(SEED_NODE, Seed);
        let _ = flow.register_with::<AlwaysRetryable, _>(
            "retrying",
            AlwaysRetryable {
                starts: starts_for_task,
            },
            seed.clone_on_read(),
            NodePolicy::new()
                .retries(1)
                .backoff(Backoff::new(base, 2.0, Duration::from_secs(1))),
        );
        let sink = MemorySink::default();
        let temp = TempBase::new("t102-backoff-elapses");
        let report = flow
            .run(
                "t102",
                &RunConfig::new(temp.as_str()),
                sink.clone(),
                SystemClock::new(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Failed);
        sink.bytes()
    });

    let starts = starts.lock().expect("starts not poisoned").clone();
    assert_eq!(starts.len(), 2, "the node made exactly two attempts");
    let gap = starts[1].duration_since(starts[0]);
    assert!(
        gap >= base.saturating_sub(Duration::from_millis(5)),
        "attempt 2 started {gap:?} after attempt 1 — the scheduled {base:?} backoff did not elapse"
    );
    assert_eq!(terminal_of(&bytes, "retrying"), "failed");
    assert_eq!(count(&bytes, "attempt-started", "retrying"), 2);
    assert_eq!(count(&bytes, "node-terminal", "retrying"), 1);
}

/// **The scheduled delays are the nominal, capped schedule.** With an **injected**
/// timer the suite asserts the *recorded* delays rather than sleeping: under
/// `NoJitter` each delay equals `Backoff::nominal_delay(n)` and none exceeds the
/// cap.
#[test]
fn injected_timer_records_the_nominal_capped_backoff_schedule() {
    let timer = RecordingTimer::default();
    let recorder = timer.clone();
    let backoff = Backoff::new(Duration::from_millis(50), 2.0, Duration::from_millis(120));

    with_watchdog("backoff-schedule", Duration::from_secs(20), move || {
        let mut flow = RunnableFlow::new().with_timer(Arc::new(recorder));
        let seed = flow.register_source(SEED_NODE, Seed);
        let _ = flow.register_with::<AlwaysRetryable, _>(
            "retrying",
            AlwaysRetryable {
                starts: Arc::new(Mutex::new(Vec::new())),
            },
            seed.clone_on_read(),
            NodePolicy::new().retries(3).backoff(backoff),
        );
        let temp = TempBase::new("t102-backoff-schedule");
        let report = flow
            .run(
                "t102",
                &RunConfig::new(temp.as_str()),
                MemorySink::default(),
                TickClock::default(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Failed);
    });

    let scheduled = timer.recorded();
    assert_eq!(scheduled.len(), 3, "three retries scheduled three backoffs");
    for (n, delay) in scheduled.iter().enumerate() {
        let n = u32::try_from(n).expect("a small attempt index");
        assert_eq!(
            *delay,
            backoff.nominal_delay(n),
            "delay {n} is the nominal `base·factor^n` under NoJitter"
        );
        assert!(
            *delay <= backoff.cap(),
            "delay {n} ({delay:?}) exceeds the cap {:?}",
            backoff.cap()
        );
    }
}

// ===========================================================================
// Await-bound timeout
// ===========================================================================

/// **An await-bound hang reaches `timed-out` at its deadline and releases its
/// permit.** The hanging node holds the whole memory pool; the waiting node is
/// admitted only once the timeout drops the hung future — proving the permit moved
/// into the dropped future's ownership chain and was released at the mark. Before
/// this ticket the run hung forever.
#[test]
fn await_bound_hang_times_out_and_releases_its_permit() {
    let started_after: Arc<Mutex<Option<Duration>>> = Arc::new(Mutex::new(None));
    let cell = Arc::clone(&started_after);
    let timeout = Duration::from_millis(200);

    let bytes = with_watchdog("await-timeout", Duration::from_secs(20), move || {
        let origin = Instant::now();
        let mut flow = RunnableFlow::new();
        let seed = flow.register_source(SEED_NODE, Seed);
        // Name order decides the initial frontier and the ready order, so `a_hang`
        // is offered admission before `b_waiter` — deterministically the hung node
        // holds the pool first.
        let _ = flow.register_with::<AwaitsForever, _>(
            "a_hang",
            AwaitsForever,
            seed.clone_on_read(),
            NodePolicy::new().timeout(timeout).working_memory(4096),
        );
        let _ = flow.register_with::<RecordsStart, _>(
            "b_waiter",
            RecordsStart {
                origin,
                started_after: cell,
            },
            seed.clone_on_read(),
            NodePolicy::new().working_memory(4096),
        );
        let sink = MemorySink::default();
        let temp = TempBase::new("t102-await-timeout");
        let report = flow
            .run(
                "t102",
                // The pool holds exactly one of the two nodes at a time.
                &RunConfig::new(temp.as_str())
                    .capacities(PoolCapacities::new().memory(4096))
                    .grace(Duration::from_millis(50)),
                sink.clone(),
                SystemClock::new(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Failed);
        sink.bytes()
    });

    assert_eq!(
        terminal_of(&bytes, "a_hang"),
        "timed-out",
        "the hung await-bound node reached `timed-out` at its deadline"
    );
    assert_eq!(count(&bytes, "node-terminal", "a_hang"), 1);
    assert_eq!(
        terminal_of(&bytes, "b_waiter"),
        "succeeded",
        "the waiting node was admitted once the timeout released the permit"
    );
    let waited = started_after
        .lock()
        .expect("start cell not poisoned")
        .expect("the waiter ran");
    assert!(
        waited >= timeout.saturating_sub(Duration::from_millis(20)),
        "the waiter started after {waited:?}; it should have waited for the hung node's \
         permit (~{timeout:?})"
    );
}

/// **A retry-eligible timeout with budget left makes a further attempt, and the node
/// still records exactly one terminal state.** Attempt 1 hangs and is cut at the
/// deadline; attempt 2 returns a value.
#[test]
fn timeout_with_retries_left_makes_a_further_attempt_and_one_terminal() {
    let bytes = with_watchdog("timeout-retry", Duration::from_secs(20), || {
        let mut flow = RunnableFlow::new();
        let seed = flow.register_source(SEED_NODE, Seed);
        let _ = flow.register_with::<HangsThenSucceeds, _>(
            "retrying",
            HangsThenSucceeds,
            seed.clone_on_read(),
            NodePolicy::new()
                .timeout(Duration::from_millis(150))
                .retries(1)
                .backoff(Backoff::new(
                    Duration::from_millis(5),
                    2.0,
                    Duration::from_millis(20),
                )),
        );
        let sink = MemorySink::default();
        let temp = TempBase::new("t102-timeout-retry");
        let report = flow
            .run(
                "t102",
                &RunConfig::new(temp.as_str()),
                sink.clone(),
                SystemClock::new(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Succeeded);
        sink.bytes()
    });

    assert_eq!(
        count(&bytes, "attempt-started", "retrying"),
        2,
        "the timed-out attempt was followed by a further attempt"
    );
    assert_eq!(
        terminal_of(&bytes, "retrying"),
        "succeeded",
        "the second attempt decided the node"
    );
    assert_eq!(
        count(&bytes, "node-terminal", "retrying"),
        1,
        "a timeout never produces a second terminal state"
    );
}

// ===========================================================================
// Blocking / compute timeout
// ===========================================================================

/// **An unkillable blocking body is marked `timed-out` immediately while its permit
/// is held until the closure returns.** The busy-loop runs far past its timeout: the
/// mark lands at the deadline (the framework's isolated timer, not the jammed task
/// thread), no retry starts while the zombie is live, the late result never fills
/// the slot, and the waiting node is admitted only once the closure actually
/// returned — the permit was held, not released at the mark.
#[test]
fn blocking_timeout_marks_immediately_and_holds_the_permit_until_return() {
    let runs = Arc::new(AtomicUsize::new(0));
    let runs_for_task = Arc::clone(&runs);
    let started_after: Arc<Mutex<Option<Duration>>> = Arc::new(Mutex::new(None));
    let cell = Arc::clone(&started_after);
    let timeout = Duration::from_millis(150);
    let spin = Duration::from_millis(900);

    let (bytes, produced) = with_watchdog("blocking-timeout", Duration::from_secs(30), move || {
        let origin = Instant::now();
        let mut flow = RunnableFlow::new();
        let seed = flow.register_source(SEED_NODE, Seed);
        let blocker = flow.register_with::<BusyLoops, _>(
            "a_blocker",
            BusyLoops::blocking(spin, runs_for_task),
            seed.clone_on_read(),
            NodePolicy::new()
                .timeout(timeout)
                .retries(1)
                .working_memory(4096),
        );
        let _ = flow.register_with::<RecordsStart, _>(
            "b_waiter",
            RecordsStart {
                origin,
                started_after: cell,
            },
            seed.clone_on_read(),
            NodePolicy::new().working_memory(4096),
        );
        let sink = MemorySink::default();
        let temp = TempBase::new("t102-blocking-timeout");
        let report = flow
            .run(
                "t102",
                &RunConfig::new(temp.as_str())
                    .capacities(PoolCapacities::new().memory(4096))
                    // The zombie returns well inside this bound, so the waiter it
                    // blocks is admitted the moment its permit is released.
                    .grace(spin * 2),
                sink.clone(),
                SystemClock::new(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Failed);
        (sink.bytes(), report.output(blocker))
    });

    assert_eq!(
        terminal_of(&bytes, "a_blocker"),
        "timed-out",
        "the unkillable blocking body was marked timed-out"
    );
    assert_eq!(
        count(&bytes, "node-terminal", "a_blocker"),
        1,
        "the terminal state is decided exactly once"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "no retry started while the zombie was live"
    );
    assert_eq!(
        produced, None,
        "the late result never filled the output slot"
    );

    // The mark is immediate: the node-terminal lands near the deadline, long before
    // the closure returns — and the waiter is admitted only after that return.
    let marked_at = offset_of(&bytes, "node-terminal", "a_blocker");
    assert!(
        marked_at < spin / 2,
        "the timeout mark landed {marked_at:?} in, not immediately at the {timeout:?} deadline"
    );
    let waited = started_after
        .lock()
        .expect("start cell not poisoned")
        .expect("the waiter ran");
    assert!(
        waited >= spin.saturating_sub(Duration::from_millis(100)),
        "the waiter started after {waited:?}; the zombie's permit must be held until its \
         closure returns (~{spin:?})"
    );
}

/// **A still-live zombie at run end yields exactly one `zombie-at-exit` event, after
/// the bounded grace.** The blocking body outlives the whole run; the run still
/// terminates promptly (the mark decided the node), waits the bounded grace, and
/// records the leftover thread as an event — never as a second terminal state.
#[test]
fn a_live_blocking_zombie_yields_exactly_one_zombie_at_exit() {
    let runs = Arc::new(AtomicUsize::new(0));
    let runs_for_task = Arc::clone(&runs);
    let grace = Duration::from_millis(60);

    let (bytes, elapsed) = with_watchdog("zombie-at-exit", Duration::from_secs(30), move || {
        let started = Instant::now();
        let mut flow = RunnableFlow::new();
        let seed = flow.register_source(SEED_NODE, Seed);
        let _ = flow.register_with::<BusyLoops, _>(
            "zombie",
            // Outlives the run by a wide margin: the run must not wait for it.
            BusyLoops::blocking(Duration::from_secs(6), runs_for_task),
            seed.clone_on_read(),
            NodePolicy::new().timeout(Duration::from_millis(120)),
        );
        let sink = MemorySink::default();
        let temp = TempBase::new("t102-zombie-at-exit");
        let report = flow
            .run(
                "t102",
                &RunConfig::new(temp.as_str()).grace(grace),
                sink.clone(),
                SystemClock::new(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Failed);
        (sink.bytes(), started.elapsed())
    });

    assert_eq!(terminal_of(&bytes, "zombie"), "timed-out");
    assert_eq!(
        count(&bytes, "zombie-at-exit", "zombie"),
        1,
        "exactly one zombie-at-exit event for the leftover thread"
    );
    assert!(
        elapsed >= grace,
        "the bounded grace ({grace:?}) was not respected (run took {elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the run waited for the zombie ({elapsed:?}) instead of proceeding past the mark"
    );
    assert_eq!(
        records(&bytes).last().map(kind_of).unwrap_or_default(),
        "run-finished",
        "a complete stream is written even with a live zombie"
    );
}

/// **A compute-class body behaves identically to a blocking one.** The class fork is
/// shape-driven (both are unkillable synchronous closures), so the compute surface
/// gets the same immediate mark and the same complete stream.
#[test]
fn compute_timeout_behaves_identically_to_blocking() {
    let runs = Arc::new(AtomicUsize::new(0));
    let runs_for_task = Arc::clone(&runs);

    let bytes = with_watchdog("compute-timeout", Duration::from_secs(30), move || {
        let mut flow = RunnableFlow::new();
        let seed = flow.register_source(SEED_NODE, Seed);
        let _ = flow.register_with::<BusyLoops, _>(
            "spinner",
            BusyLoops::compute(Duration::from_secs(6), runs_for_task),
            seed.clone_on_read(),
            NodePolicy::new()
                .timeout(Duration::from_millis(120))
                .execution_class(ExecutionClass::Compute),
        );
        let sink = MemorySink::default();
        let temp = TempBase::new("t102-compute-timeout");
        let report = flow
            .run(
                "t102",
                &RunConfig::new(temp.as_str()).grace(Duration::from_millis(60)),
                sink.clone(),
                SystemClock::new(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Failed);
        sink.bytes()
    });

    assert_eq!(terminal_of(&bytes, "spinner"), "timed-out");
    assert_eq!(count(&bytes, "node-terminal", "spinner"), 1);
    assert_eq!(count(&bytes, "zombie-at-exit", "spinner"), 1);
}

// ===========================================================================
// No regression
// ===========================================================================

/// **A node with neither policy is untouched.** The two-node no-policy run produces
/// exactly the ordered `(kind, node, state)` shape it produced before this ticket —
/// no timeout arming, no backoff phase, no zombie accounting appears.
#[test]
fn a_node_with_no_timeout_and_no_retries_has_the_unchanged_stream_shape() {
    let bytes = with_watchdog("no-policy", Duration::from_secs(20), || {
        let mut flow = RunnableFlow::new();
        let seed = flow.register_source(SEED_NODE, Seed);
        let _ = flow.register::<RecordsStart, _>(
            "plain",
            RecordsStart {
                origin: Instant::now(),
                started_after: Arc::new(Mutex::new(None)),
            },
            seed.clone_on_read(),
        );
        let sink = MemorySink::default();
        let temp = TempBase::new("t102-no-policy");
        let report = flow
            .run(
                "t102",
                &RunConfig::new(temp.as_str()),
                sink.clone(),
                TickClock::default(),
            )
            .expect("the flow assembles and runs");
        assert_eq!(report.outcome(), RunOutcome::Succeeded);
        sink.bytes()
    });

    let expected: Vec<(String, Option<String>, Option<String>)> = [
        ("run-started", None, None),
        ("node-ready", Some(SEED_NODE), None),
        ("node-admitted", Some(SEED_NODE), None),
        ("attempt-started", Some(SEED_NODE), None),
        ("attempt-succeeded", Some(SEED_NODE), None),
        ("attempt-outcome", Some(SEED_NODE), None),
        ("node-terminal", Some(SEED_NODE), Some("succeeded")),
        ("node-ready", Some("plain"), None),
        ("node-admitted", Some("plain"), None),
        ("attempt-started", Some("plain"), None),
        ("attempt-succeeded", Some("plain"), None),
        ("attempt-outcome", Some("plain"), None),
        ("node-terminal", Some("plain"), Some("succeeded")),
        ("run-finished", None, None),
    ]
    .into_iter()
    .map(|(k, n, s)| (k.to_string(), n.map(str::to_string), s.map(str::to_string)))
    .collect();

    assert_eq!(
        shape(&bytes),
        expected,
        "a no-policy run's event shape changed"
    );
}
