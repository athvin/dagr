//! C16 · OS signals, final flush, and temp cleanup — ticket T36 (046). Written first, TDD.
//!
//! This is the **OS-signal half** of C16 that T35 deferred. It wires real OS
//! termination signals (`SIGINT`/`SIGTERM`) to the T35 [`CancelHandle`] seam,
//! guarantees a complete + fsync'd event stream on shutdown (a bounded final
//! flush that reports the distinct sink-failure cause on an unwritable sink
//! rather than hanging), and establishes the per-run temp-directory convention:
//! everything a task writes locally lives under the run's temp directory,
//! cooperative tasks that observe cancellation within grace clean up their own
//! debris, and the *next* invocation reclaims leftover per-run temp directories
//! regardless of how the prior process ended.
//!
//! Determinism (CI): a real OS signal is **never** raised at the test-runner while
//! it could be *fatal*. Instead:
//!   * the signal→cancel *wiring* is exercised through the same programmatic
//!     [`CancelHandle`] seam the installed handler drives, plus a unit test of the
//!     re-entry-hardened routing that a second signal takes;
//!   * the **end-to-end** path (`#[cfg(unix)]`) installs the real T36 handlers
//!     **first** — so the signal is *caught*, not fatal — then `raise`s a real
//!     `SIGTERM`/`SIGINT` at this process and asserts the run cancels with an
//!     external-interrupt origin and a complete stream; the two real-signal tests
//!     are serialized (a raised signal is process-wide). Non-unix is a documented
//!     no-op (there are no POSIX termination signals — platform-conditional, T70);
//!   * the final flush + temp cleanup are ordinary injected-path logic — no
//!     wall-clock, no network.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, CancelHandle, NodeRunner, RunConfig, RunPlan, ShutdownExit};
use dagr_cli::signals::{route_signal, SignalRouter};
use dagr_cli::temp::{
    cleanup_temp_dir, create_temp_dir, per_run_temp_dir, reclaim_leftover_temp_dirs,
};
use dagr_core::context::{CancellationOrigin, RunContext, TerminalState};
use dagr_core::execution::AttemptEventSink;
use dagr_core::flow::Flow;
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// In-memory + faulting sinks and a deterministic clock (the C19 seam).
// ===========================================================================

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
    flushes: Arc<AtomicU64>,
}
impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().unwrap().clone()
    }
    fn flush_count(&self) -> u64 {
        self.flushes.load(Ordering::SeqCst)
    }
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        // The default local-file sink fsyncs on flush; this records each flush
        // so the "exactly one run-end fsync" guarantee is checkable.
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A sink whose `flush` (the fsync boundary) becomes unwritable, but whose
/// `append_line` still succeeds — the "unwritable sink *at shutdown*" fault from
/// the T0.6 contract. Appends land, so the stream is written; only the final
/// fsync fails, which is exactly the at-shutdown sink-failure path.
#[derive(Clone, Default)]
struct FlushFailsSink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl EventSink for FlushFailsSink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::other("sink unwritable at shutdown"))
    }
}

#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

// ===========================================================================
// Parsed-stream helpers.
// ===========================================================================

fn parse_events(bytes: &[u8]) -> Vec<(String, Option<String>)> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .map(|rec| {
            let kind = rec
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let node = rec.get("node").and_then(|v| v.as_str()).map(str::to_string);
            (kind, node)
        })
        .collect()
}

fn terminal_of(bytes: &[u8], node: &str) -> Option<String> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream.records.iter().find_map(|rec| {
        let is_terminal = rec.get("kind").and_then(|v| v.as_str()) == Some("node-terminal");
        let this_node = rec.get("node").and_then(|v| v.as_str());
        if is_terminal && this_node == Some(node) {
            rec.get("state")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        } else {
            None
        }
    })
}

fn terminal_count(bytes: &[u8], node: &str) -> usize {
    parse_events(bytes)
        .iter()
        .filter(|(k, n)| k == "node-terminal" && n.as_deref() == Some(node))
        .count()
}

/// The whole stream parses, is gapless (sequence numbers 0..N contiguous), and
/// ends with `run-finished` — a complete, valid stream.
fn stream_is_complete_and_parseable(bytes: &[u8]) {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    assert!(
        !stream.trailing_partial_discarded,
        "a clean signal shutdown produces no trailing partial"
    );
    for (i, rec) in stream.records.iter().enumerate() {
        let seq = rec.get("seq").and_then(serde_json::Value::as_u64);
        assert_eq!(seq, Some(i as u64), "gapless sequence at record {i}");
        assert!(
            rec.get("run_id").and_then(|v| v.as_str()).is_some(),
            "record {i} carries run identity"
        );
        assert_eq!(
            rec.get("schema_version").and_then(|v| v.as_str()),
            Some("dagr.event-stream@1"),
            "record {i} carries the schema version"
        );
    }
    let kinds: Vec<&str> = stream
        .records
        .iter()
        .filter_map(|r| r.get("kind").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(
        kinds.last().copied(),
        Some("run-finished"),
        "the stream ends with run-finished (complete)"
    );
}

// ===========================================================================
// Scripted tasks.
// ===========================================================================

/// Fires the programmatic cancel the instant it runs (the in-run stand-in for the
/// signal handler firing the same `CancelHandle`), then succeeds.
struct FiresCancel {
    handle: CancelHandle,
}
impl Task for FiresCancel {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        self.handle.cancel();
        Ok(1)
    }
}

/// A cooperative await-bound task that writes a scratch file under the run's temp
/// directory on start and removes it when it observes cancellation within grace —
/// the within-grace self-cleanup guarantee.
struct CooperativeTempWriter {
    // Set by the test to the run's per-run temp dir; the task writes/removes a file
    // under it (in production the path is reached through the context).
    temp_dir: PathBuf,
    wrote: Arc<Mutex<Option<PathBuf>>>,
}
impl Task for CooperativeTempWriter {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // Everything the task writes locally goes under the run's temp directory.
        // Prove it is reachable through the context too.
        assert_eq!(
            c.temp_dir(),
            Some(self.temp_dir.as_path()),
            "the per-run temp directory is reachable through the context"
        );
        let scratch = self.temp_dir.join("cooperative-scratch.tmp");
        std::fs::write(&scratch, b"in-flight debris").expect("write scratch");
        *self.wrote.lock().unwrap() = Some(scratch.clone());
        for _ in 0..100_000 {
            if c.cancellation().is_cancelled() {
                // Observed cancellation within grace — clean up our own debris.
                let _ = std::fs::remove_file(&scratch);
                return Err(TaskError::permanent("stopped on cancellation"));
            }
            tokio::task::yield_now().await;
        }
        Ok(7)
    }
}

struct Succeeds;
impl Task for Succeeds {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

// ===========================================================================
// A type-erased source runner over the real C14 caught attempt path.
// ===========================================================================

use dagr_core::execution::run_attempt_caught;
use dagr_core::slot::{ResidencyLedger, Slot};

struct SourceRunner<T: Task<Input = ()>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<T::Output>>,
}
impl<T: Task<Input = ()>> SourceRunner<T> {
    fn boxed(name: &str, task: T, slot: Arc<Slot<T::Output>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot,
        })
    }
}
impl<T: Task<Input = ()>> NodeRunner for SourceRunner<T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

fn ledger() -> Arc<ResidencyLedger> {
    ResidencyLedger::new()
}
fn slot_for<T: Send + Sync + 'static>(name: &str, consumers: u32) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        consumers,
        false,
        0,
        ledger(),
    ))
}

const SHORT_GRACE: Duration = Duration::from_millis(150);

/// A per-test **collision-proof** run-store base under the OS temp dir.
///
/// Determinism (CI fs race): several tests in this binary create and later
/// `remove_dir_all` their own base concurrently under `--test-threads>1`. A base
/// keyed only on `process::id()` + a wall-clock timestamp is **not** unique — the
/// system clock's effective resolution is coarse (observed: ~95% of back-to-back
/// `SystemTime::now()` reads on CI return the *same* nanosecond value), so two tests
/// entering here at nearly the same instant get the **same** base path; one test's
/// terminal `remove_dir_all(base)` (or `reclaim_leftover_temp_dirs`) then wipes the
/// other's freshly-created temp dir mid-test, flaking `cleanup_removes_temp_dir`'s
/// `assert!(temp.exists())` (and its siblings). The fix is causal, not a sleep: a
/// process-monotonic `AtomicU64` counter makes every base provably disjoint, so no
/// two tests ever share — or delete — the same subtree. No production change.
fn temp_base() -> PathBuf {
    use std::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "dagr-t36-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        unique
    ))
}

// ===========================================================================
// Signal → CancelHandle wiring (via the seam / re-entry routing).
// ===========================================================================

/// **A signal routes to the `CancelHandle` seam.** The router the installed
/// OS-signal handler drives fires the same programmatic `CancelHandle` a scripted
/// task fires in the T35 suite — the signal path and the programmatic path are the
/// same seam.
#[test]
fn a_signal_routes_to_the_cancel_handle_seam() {
    let cfg = RunConfig::new("/tmp/dagr-t36");
    let handle = cfg.cancel_handle();
    let router = SignalRouter::new(handle);
    assert!(!router.was_fired(), "no signal yet");
    router.on_signal();
    assert!(
        router.was_fired(),
        "a delivered signal fired the cancel handle"
    );
}

/// **A second signal is re-entry hardened: it does not shortcut the final flush.**
/// The first signal starts the budgeted shutdown; subsequent identical signals are
/// idempotent — they neither escalate to an immediate exit nor re-fire a second
/// cancellation, so the shutdown path (and its final flush) is never corrupted.
#[test]
fn a_second_signal_is_idempotent_and_does_not_shortcut_the_flush() {
    let fires = Arc::new(AtomicU64::new(0));
    let f = Arc::clone(&fires);
    let mut count = 0u32;
    // First delivery.
    route_signal(&mut count, &mut || {
        f.fetch_add(1, Ordering::SeqCst);
    });
    // Second (and third) delivery of the same signal during shutdown.
    let f2 = Arc::clone(&fires);
    route_signal(&mut count, &mut || {
        f2.fetch_add(1, Ordering::SeqCst);
    });
    let f3 = Arc::clone(&fires);
    route_signal(&mut count, &mut || {
        f3.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(
        fires.load(Ordering::SeqCst),
        1,
        "only the first signal fires cancellation; repeats are idempotent no-ops"
    );
    assert_eq!(
        count, 3,
        "every signal is counted (observed), none is dropped"
    );
}

// ===========================================================================
// End-to-end: a REAL OS signal, installed handlers, drives the CancelHandle,
// the run cancels + writes a complete stream — WITHOUT killing the test runner.
// ===========================================================================

/// **A real `SIGTERM` triggers cancellation through the installed handlers, and the
/// run drains to a complete stream — without killing the test runner.** We install
/// the real T36 handlers, `raise(SIGTERM)` at *this* process (tokio's registered
/// handler catches it — it does NOT take the default terminate disposition), and a
/// concurrent cooperative task observes the resulting cancellation and returns. The
/// run ends `cancelled` with an external-interrupt origin and a complete, fsync'd
/// stream. This is safe: once handlers are installed, the signal is caught, so the
/// harness is never terminated.
#[cfg(unix)]
#[test]
fn real_sigterm_triggers_cancellation_and_a_complete_stream() {
    real_signal_end_to_end(libc::SIGTERM);
}

/// **`SIGINT` behaves identically to `SIGTERM`** (same installed handlers, same
/// path, same `cancelled` classification and external-interrupt origin).
#[cfg(unix)]
#[test]
fn real_sigint_triggers_cancellation_and_a_complete_stream() {
    real_signal_end_to_end(libc::SIGINT);
}

/// A task that raises the real signal at this process the instant it runs, then
/// cooperatively waits for the resulting cancellation and returns. Module-scoped so
/// both real-signal tests share it.
#[cfg(unix)]
struct RaiseThenWait {
    sig: libc::c_int,
}
#[cfg(unix)]
impl Task for RaiseThenWait {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // Deliver the real signal; the installed handler catches it and fires the
        // cancel handle. `raise` needs unsafe — justified at the calling fn boundary.
        #[allow(
            unsafe_code,
            reason = "libc::raise is the only way to deliver a real OS signal to \
                      prove the installed handler catches it; safe because the \
                      handler is installed first so the signal is caught, not fatal"
        )]
        unsafe {
            libc::raise(self.sig);
        }
        for _ in 0..100_000_000u64 {
            if c.cancellation().is_cancelled() {
                return Err(TaskError::permanent("stopped on signal"));
            }
            tokio::task::yield_now().await;
        }
        Ok(1)
    }
}

/// Serialize the two real-signal tests: a raised signal is process-wide, so running
/// them concurrently could cross-fire installed handlers. Both expect cancellation,
/// so a stray cross-fire is benign, but serializing keeps each test's proof clean.
#[cfg(unix)]
static REAL_SIGNAL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(unix)]
fn real_signal_end_to_end(sig: libc::c_int) {
    use dagr_cli::signals::install_signal_handlers;

    let _serial = REAL_SIGNAL_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let mut flow = Flow::new();
    let _w = flow.register_source("waiter", &Succeeds);
    let pipeline = flow.finish();

    let cfg = RunConfig::new("/tmp/dagr-t36-e2e").grace(SHORT_GRACE);
    let handle = cfg.cancel_handle();
    // Install the REAL OS-signal handlers wiring SIGTERM/SIGINT -> this handle. From
    // here on the signal is CAUGHT, so raising it does not terminate the runner.
    let guard = install_signal_handlers(handle).expect("handlers install");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "waiter".into(),
        SourceRunner::boxed(
            "waiter",
            RaiseThenWait { sig },
            slot_for::<u64>("waiter", 0),
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &cfg,
        "e2e",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    // Keep the handlers installed until the drive returns.
    drop(guard);

    assert_eq!(
        report.cancellation_origin,
        Some(CancellationOrigin::ExternalInterrupt),
        "a real {sig} signal was caught by the installed handler and drove the cancel handle"
    );
    assert_eq!(report.outcome, RunOutcome::Cancelled);
    assert_eq!(
        terminal_of(&sink.bytes(), "waiter").as_deref(),
        Some("cancelled"),
        "the cooperative task observed the signal-driven cancellation and is recorded cancelled"
    );
    stream_is_complete_and_parseable(&sink.bytes());
}

/// A cooperative waiter that returns on cancellation (used by the flush tests).
struct CoopUntilCancelled;
impl Task for CoopUntilCancelled {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        for _ in 0..100_000_000u64 {
            if c.cancellation().is_cancelled() {
                return Err(TaskError::permanent("stopped on signal"));
            }
            tokio::task::yield_now().await;
        }
        Ok(1)
    }
}

// ===========================================================================
// Final flush: complete + fsync'd stream on the cancellation path.
// ===========================================================================

/// **A complete, fsync'd stream is written on the cancellation (signal) path.**
/// Exactly one run-end fsync is observed through the sink after the final records,
/// and the stream is complete + parseable (C19 "fsync at cancellation/run end").
#[test]
fn complete_fsyncd_stream_on_cancellation() {
    let mut flow = Flow::new();
    let _t = flow.register_source("trigger", &Succeeds);
    let _w = flow.register_source("waiter", &Succeeds);
    let pipeline = flow.finish();

    let cfg = RunConfig::new("/tmp/dagr-t36").grace(SHORT_GRACE);
    let handle = cfg.cancel_handle();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "trigger".into(),
        SourceRunner::boxed(
            "trigger",
            FiresCancel {
                handle: handle.clone(),
            },
            slot_for::<u64>("trigger", 0),
        ),
    );
    runners.insert(
        "waiter".into(),
        SourceRunner::boxed("waiter", CoopUntilCancelled, slot_for::<u64>("waiter", 0)),
    );

    let sink = MemorySink::default();
    let report = drive(
        &cfg,
        "flush",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    stream_is_complete_and_parseable(&sink.bytes());
    assert_eq!(
        sink.flush_count(),
        1,
        "exactly one run-end fsync through the sink after the final records"
    );
    assert_eq!(report.outcome, RunOutcome::Cancelled);
    assert_eq!(report.shutdown_exit, ShutdownExit::Cancelled);
}

/// **A complete, fsync'd stream is written on the normal path too.** The final
/// flush is the same single fsync boundary, and the exit is a clean success.
#[test]
fn complete_fsyncd_stream_on_normal_end() {
    let mut flow = Flow::new();
    let _a = flow.register_source("a", &Succeeds);
    let pipeline = flow.finish();

    let cfg = RunConfig::new("/tmp/dagr-t36");
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "a".into(),
        SourceRunner::boxed("a", Succeeds, slot_for::<u64>("a", 0)),
    );

    let sink = MemorySink::default();
    let report = drive(
        &cfg,
        "normal",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    stream_is_complete_and_parseable(&sink.bytes());
    assert_eq!(sink.flush_count(), 1, "exactly one run-end fsync");
    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(report.shutdown_exit, ShutdownExit::Success);
}

// ===========================================================================
// Unwritable sink at shutdown: bounded wait + distinct sink-failure exit code.
// ===========================================================================

/// **An unwritable sink at shutdown yields a bounded wait and the sink-failure
/// exit selection — never a hang, never a success or plain-cancellation report.**
/// The appends land (so the stream is written) but the final fsync fails; the
/// driver reports [`ShutdownExit::SinkFailure`] and returns within the bounded
/// final-flush window.
#[test]
fn unwritable_sink_at_shutdown_yields_bounded_wait_and_sink_failure_code() {
    let mut flow = Flow::new();
    let _a = flow.register_source("a", &Succeeds);
    let pipeline = flow.finish();

    let cfg = RunConfig::new("/tmp/dagr-t36");
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "a".into(),
        SourceRunner::boxed("a", Succeeds, slot_for::<u64>("a", 0)),
    );

    let sink = FlushFailsSink::default();
    let start = std::time::Instant::now();
    let report = drive(
        &cfg,
        "sinkfail",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    let elapsed = start.elapsed();

    assert_eq!(
        report.shutdown_exit,
        ShutdownExit::SinkFailure,
        "an unwritable sink at shutdown produces the distinct sink-failure exit selection"
    );
    assert_ne!(
        report.shutdown_exit,
        ShutdownExit::Success,
        "it does not report success"
    );
    assert_ne!(
        report.shutdown_exit,
        ShutdownExit::Cancelled,
        "it does not report plain cancellation"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the wait is bounded (well under the shutdown budget); it did not hang"
    );
}

/// **A sink-failure at shutdown does not masquerade as a run failure.** Every node
/// succeeded, so the run outcome is `succeeded`, yet the shutdown exit is the
/// sink-failure selection — distinct from a node ending `failed`/`timed-out`.
#[test]
fn sink_failure_at_shutdown_is_not_a_run_failure() {
    let mut flow = Flow::new();
    let _a = flow.register_source("a", &Succeeds);
    let pipeline = flow.finish();

    let cfg = RunConfig::new("/tmp/dagr-t36");
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "a".into(),
        SourceRunner::boxed("a", Succeeds, slot_for::<u64>("a", 0)),
    );

    let sink = FlushFailsSink::default();
    let report = drive(
        &cfg,
        "sinkfail2",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink,
        TickClock::default(),
    );

    assert_eq!(
        report.outcome,
        RunOutcome::Succeeded,
        "no node failed; the run outcome is succeeded"
    );
    assert_eq!(
        report.shutdown_exit,
        ShutdownExit::SinkFailure,
        "the sink fault is reported distinctly, not as a run failure"
    );
}

/// **Run failure wins over sink failure and over cancellation (C26 precedence).**
/// When a node genuinely failed, the shutdown exit reflects the run failure even
/// if the sink also could not flush — a run failure is the highest-precedence code.
#[test]
fn run_failure_wins_over_sink_failure_precedence() {
    struct Fails;
    impl Task for Fails {
        type Input = ();
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
            Err(TaskError::permanent("boom"))
        }
    }

    let mut flow = Flow::new();
    let _a = flow.register_source("a", &Fails);
    let pipeline = flow.finish();

    let cfg = RunConfig::new("/tmp/dagr-t36");
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "a".into(),
        SourceRunner::boxed("a", Fails, slot_for::<u64>("a", 0)),
    );

    let sink = FlushFailsSink::default();
    let report = drive(
        &cfg,
        "runfail",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink,
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Failed);
    assert_eq!(
        report.shutdown_exit,
        ShutdownExit::RunFailure,
        "a run failure outranks a sink failure in the C26 exit-code precedence"
    );
}

// ===========================================================================
// Per-run temp directory: confinement, next-invocation reclamation, cleanup.
// ===========================================================================

/// **Two runs get disjoint per-run temp directories.** The path embeds the
/// pipeline and the run id, so two runs — even of the same pipeline — never share
/// a temp directory (the confinement half of the convention).
#[test]
fn two_runs_get_disjoint_temp_directories() {
    let base = temp_base();
    let a = per_run_temp_dir(base.to_str().unwrap(), "pipe", "run-a");
    let b = per_run_temp_dir(base.to_str().unwrap(), "pipe", "run-b");
    assert_ne!(a, b, "distinct run ids yield distinct temp directories");
    assert!(
        a.starts_with(&base) && b.starts_with(&base),
        "both temp dirs live under the run-store base (confined)"
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// **The next invocation reclaims a leftover per-run temp directory regardless of
/// how the prior process ended, without deleting run outputs it must keep.** A
/// prior run (simulated as killed abruptly) left a populated `tmp/` behind; a fresh
/// invocation removes the leftover temp dir but leaves the reserved run outputs
/// (e.g. `events.jsonl`) untouched, and does not touch the current run's own dir.
#[test]
fn next_invocation_reclaims_leftover_temp_dir_but_keeps_outputs() {
    let base = temp_base();
    let pipeline = "pipe";

    // A prior run directory with a populated temp dir AND a reserved output file.
    let prior_run = "prior-run";
    let prior_temp = per_run_temp_dir(base.to_str().unwrap(), pipeline, prior_run);
    create_temp_dir(&prior_temp).expect("create prior temp");
    std::fs::write(prior_temp.join("leftover.tmp"), b"debris").expect("write debris");
    let prior_events = prior_temp.parent().unwrap().join("events.jsonl");
    std::fs::write(&prior_events, b"{\"seq\":0}\n").expect("write prior events");

    // The current run's own temp dir must be untouched by reclamation.
    let current_run = "current-run";
    let current_temp = per_run_temp_dir(base.to_str().unwrap(), pipeline, current_run);
    create_temp_dir(&current_temp).expect("create current temp");
    std::fs::write(current_temp.join("keep.tmp"), b"mine").expect("write current");

    // The next invocation reclaims leftover per-run temp dirs, keeping the current.
    reclaim_leftover_temp_dirs(base.to_str().unwrap(), pipeline, current_run);

    assert!(
        !prior_temp.exists(),
        "the leftover per-run temp directory was reclaimed regardless of how the prior process ended"
    );
    assert!(
        prior_events.exists(),
        "the prior run's reserved outputs (events.jsonl) were NOT deleted"
    );
    assert!(
        current_temp.exists(),
        "the current run's own temp directory was left untouched"
    );

    let _ = std::fs::remove_dir_all(&base);
}

/// **Cleanup runs on the normal path.** A run's temp directory is removed at exit
/// on a clean, non-cancelled run.
#[test]
fn cleanup_removes_temp_dir() {
    let base = temp_base();
    let temp = per_run_temp_dir(base.to_str().unwrap(), "pipe", "run-x");
    create_temp_dir(&temp).expect("create temp");
    std::fs::write(temp.join("x.tmp"), b"x").expect("write");
    assert!(temp.exists());
    cleanup_temp_dir(&temp);
    assert!(!temp.exists(), "cleanup removed the run's temp directory");
    let _ = std::fs::remove_dir_all(&base);
}

/// **Cleanup is best-effort on a missing directory (the abandon path).** Cleaning a
/// directory that a prior abrupt end never created — or that was already removed —
/// must not panic; the guarantee is best-effort by design (arch.md C16).
#[test]
fn cleanup_is_best_effort_on_missing_dir() {
    let base = temp_base();
    let temp = per_run_temp_dir(base.to_str().unwrap(), "pipe", "never-created");
    // Never created — cleanup must be a harmless no-op.
    cleanup_temp_dir(&temp);
    assert!(!temp.exists());
}

// ===========================================================================
// End-to-end: cooperative temp cleanup on the cancellation path + driver cleanup.
// ===========================================================================

/// **A cooperative task cleans up its own temp artifacts on cancellation, and the
/// driver removes the per-run temp directory at exit.** The cooperative task writes
/// a file under the run's temp dir and removes it when it observes cancellation
/// within grace; after the drive returns, the file is gone AND the run's temp
/// directory has been reclaimed by the driver.
#[test]
fn cooperative_task_cleans_temp_on_cancellation_and_driver_reclaims_dir() {
    let base = temp_base();
    let pipeline = "coop";

    let mut flow = Flow::new();
    let _t = flow.register_source("trigger", &Succeeds);
    let _w = flow.register_source("waiter", &Succeeds);
    let pipe = flow.finish();

    // Configure the run store base so the driver derives the per-run temp dir under
    // it; capture the run id so the test can compute the same temp dir.
    let cfg = RunConfig::new(base.to_str().unwrap())
        .run_id("fixed-run")
        .grace(SHORT_GRACE);
    let handle = cfg.cancel_handle();
    let temp_dir = per_run_temp_dir(base.to_str().unwrap(), pipeline, "fixed-run");

    let wrote = Arc::new(Mutex::new(None));
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "trigger".into(),
        SourceRunner::boxed(
            "trigger",
            FiresCancel {
                handle: handle.clone(),
            },
            slot_for::<u64>("trigger", 0),
        ),
    );
    runners.insert(
        "waiter".into(),
        SourceRunner::boxed(
            "waiter",
            CooperativeTempWriter {
                temp_dir: temp_dir.clone(),
                wrote: Arc::clone(&wrote),
            },
            slot_for::<u64>("waiter", 0),
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &cfg,
        pipeline,
        Ok(RunPlan::new(pipe, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    // The cooperative task observed cancellation within grace and removed its file.
    let scratch = wrote
        .lock()
        .unwrap()
        .clone()
        .expect("task wrote a scratch file");
    assert!(
        !scratch.exists(),
        "the cooperative task removed its own temp artifact on cancellation"
    );
    // The driver reclaimed the run's whole temp directory at exit.
    assert!(
        !temp_dir.exists(),
        "the driver removed the per-run temp directory at exit"
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "waiter").as_deref(),
        Some("cancelled")
    );
    assert_eq!(terminal_count(&sink.bytes(), "waiter"), 1);
    stream_is_complete_and_parseable(&sink.bytes());
    assert_eq!(report.outcome, RunOutcome::Cancelled);

    let _ = std::fs::remove_dir_all(&base);
}
