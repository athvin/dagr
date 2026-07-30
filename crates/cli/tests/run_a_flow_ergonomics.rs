//! run-a-Flow ergonomics — the operator-sanctioned public "run a `Flow` of real
//! `Task`s → a completed run" seam.
//!
//! # Why this suite exists
//!
//! A task author writes tasks + a flow and **never** writes
//! scheduling or permit plumbing. Yet every earlier demo has,
//! until now, hand-written ~150 lines of type-erased `NodeRunner` impls per
//! pipeline — a `Pin<Box<dyn Future<Output = TerminalState> + Send>>` that reads
//! input `SlotRef`s, calls `run_attempt`/`run_with_retries_caught`, and fills the
//! output slot. That is exactly the scheduling plumbing the author must not
//! write. This suite pins the **new one-call path**: build a `RunnableFlow` of real
//! `Task`s and run it with a single call, with **no** hand-written `NodeRunner`.
//!
//! # What it proves (the fidelity + end-to-end contract)
//!
//! - **Fidelity to the hand-wired path.** [`fidelity_auto_adapter_matches_hand_wired`]
//!   mirrors the three-node chain (`source → transform → sink`, the middle node
//!   deterministically flaky with one retry) and asserts the NEW auto-adapter path
//!   produces the **same** outcome, the **same** per-node terminal states, and the
//!   **same** ordered event-stream transition shape as the hand-wired demo —
//!   including `transform`'s two attempt cycles.
//! - **End to end.** [`chain_runs_source_transform_sink_end_to_end`] drives a real
//!   chain and asserts the expected artifact flows through and the run succeeds.
//! - **Retries work through the adapter.** Covered by the fidelity + chain tests
//!   (the middle node retries once and recovers).
//! - **Failure propagates.** [`permanent_failure_propagates_downstream`] fails the
//!   middle node permanently and asserts the sink ends `upstream-failed` and the run
//!   fails overall.
//! - **A scratch-touching node works through the adapter.**
//!   [`a_scratch_touching_node_runs_through_the_adapter`] proves a single-attempt
//!   node reaches its real per-node durable scratch namespace (scratch wiring intact).
//!
//! Determinism: an injected in-memory sink + a monotonic tick clock; the one
//! scratch test uses a PRIVATE temp dir. No wall-clock sleeps (the retry backoff
//! resolves immediately). Portable — macOS is in CI.

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome, read_records};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::TaskError;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::test_kit::TempBase;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ===========================================================================
// Injection seams: an in-memory sink + a monotonic clock — the same
// deterministic seams the hand-wired demos use.
// ===========================================================================

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}
impl MemorySink {
    fn bytes(&self) -> Vec<u8> {
        self.lines.lock().unwrap().clone()
    }
}
impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines.lock().unwrap().extend_from_slice(line);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
// The example tasks — pure `Task`s, no plumbing (identical to the hand-wired demo's).
// ===========================================================================

struct Source {
    value: u64,
}
impl Task for Source {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.value)
    }
}

/// Deterministically-flaky middle: fails retryably on attempt 1, doubles on 2.
struct FlakyTransform;
impl Task for FlakyTransform {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, c: &RunContext, input: u64) -> Result<u64, TaskError> {
        if c.attempt() == 1 {
            Err(TaskError::retryable(
                "transient hiccup on the first attempt",
            ))
        } else {
            Ok(input * 2)
        }
    }
}

/// Middle node that fails **permanently** — for the failure-propagation test.
struct AlwaysFails;
impl Task for AlwaysFails {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _input: u64) -> Result<u64, TaskError> {
        Err(TaskError::permanent("this node always fails"))
    }
}

struct Sink;
impl Task for Sink {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input + 1)
    }
}

const SOURCE: &str = "source";
const TRANSFORM: &str = "transform";
const SINK: &str = "sink";
const SEED: u64 = 21;
const FINAL_VALUE: u64 = (SEED * 2) + 1;

// ===========================================================================
// A minimal event-stream walker (the observable oracle).
// ===========================================================================

fn kinds(bytes: &[u8]) -> Vec<String> {
    let stream = read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .map(|r| {
            r.get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

/// The ordered `(kind, node, state)` shape — the comparison surface for fidelity.
fn shape(bytes: &[u8]) -> Vec<(String, Option<String>, Option<String>)> {
    let stream = read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .map(|r| {
            let kind = r
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let node = r.get("node").and_then(|v| v.as_str()).map(str::to_string);
            let state = r.get("state").and_then(|v| v.as_str()).map(str::to_string);
            (kind, node, state)
        })
        .collect()
}

fn terminal_of(bytes: &[u8], node: &str) -> String {
    let stream = read_records(bytes).expect("stream parses");
    let mut found = None;
    for r in &stream.records {
        if r.get("kind").and_then(|v| v.as_str()) == Some("node-terminal")
            && r.get("node").and_then(|v| v.as_str()) == Some(node)
        {
            found = Some(
                r.get("state")
                    .and_then(|v| v.as_str())
                    .expect("node-terminal carries a state")
                    .to_string(),
            );
        }
    }
    found.unwrap_or_else(|| panic!("no node-terminal for `{node}`"))
}

fn count(bytes: &[u8], kind: &str, node: &str) -> usize {
    let stream = read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .filter(|r| {
            r.get("kind").and_then(|v| v.as_str()) == Some(kind)
                && r.get("node").and_then(|v| v.as_str()) == Some(node)
        })
        .count()
}

// ===========================================================================
// Build the three-node chain through the NEW one-call `RunnableFlow` — no NodeRunner.
// ===========================================================================

/// Build the flaky-retry three-node chain through the auto-adapter (no hand-written
/// runner) and run it in ONE call, returning the recorded stream + outcome +
/// the sink's produced value.
fn run_via_auto_adapter() -> (Vec<u8>, RunOutcome, Option<u64>) {
    let temp_base = TempBase::new("run-a-flow");
    let mut flow = RunnableFlow::new();
    let source = flow.register_source(SOURCE, Source { value: SEED });
    let transform = flow.register_with::<FlakyTransform, _>(
        TRANSFORM,
        FlakyTransform,
        source.clone_on_read(),
        NodePolicy::default().retries(1),
    );
    let sink_handle = flow.register::<Sink, _>(SINK, Sink, transform);

    let mem = MemorySink::default();
    let report = flow
        .run(
            "run-a-flow-m1",
            &dagr_cli::driver::RunConfig::new(temp_base.as_str()),
            mem.clone(),
            TickClock::default(),
        )
        .expect("the flow assembles and runs");
    let produced = report.output(sink_handle);
    (mem.bytes(), report.outcome(), produced)
}

// ===========================================================================
// Tests
// ===========================================================================

/// **End to end.** A `source → transform → sink` chain of real tasks runs through
/// the one-call path: the run succeeds, every node ends `succeeded`, the retried
/// value flows through, and the sink produces `(SEED*2)+1`.
#[test]
fn chain_runs_source_transform_sink_end_to_end() {
    let (bytes, outcome, produced) = run_via_auto_adapter();
    assert_eq!(outcome, RunOutcome::Succeeded, "the run succeeds overall");
    for node in [SOURCE, TRANSFORM, SINK] {
        assert_eq!(terminal_of(&bytes, node), "succeeded", "`{node}` succeeded");
    }
    assert_eq!(
        produced,
        Some(FINAL_VALUE),
        "the retried value flowed through the adapter to the sink"
    );
    // The middle node retried exactly once (two attempts) — retries work.
    assert_eq!(count(&bytes, "attempt-started", TRANSFORM), 2);
    assert_eq!(count(&bytes, "attempt-succeeded", TRANSFORM), 1);
    assert_eq!(
        kinds(&bytes).first().map(String::as_str),
        Some("run-started")
    );
    assert_eq!(
        kinds(&bytes).last().map(String::as_str),
        Some("run-finished")
    );
}

/// **Fidelity.** The auto-adapter path produces the SAME ordered event-stream
/// transition shape (kind + node + terminal state) as the hand-wired demo's
/// runner path. Because the hand-wired demo's shape is pinned by its own suite, matching
/// it here proves the generic adapter reproduces, per node, exactly what the
/// hand-written `SourceRunner`/`RetryingRunner`/`SinkRunner` did.
#[test]
fn fidelity_auto_adapter_matches_hand_wired() {
    // The hand-wired shape, taken verbatim from
    // `m1_demo_three_node_chain::m1_three_node_chain_with_retry_is_the_done_when`
    // (the load-bearing gate). Run-level anchors + the exact per-node cycles.
    let (bytes, _outcome, _produced) = run_via_auto_adapter();
    let got = shape(&bytes);

    // Run-level anchors identical to the hand-wired path.
    assert_eq!(got.first().map(|t| t.0.as_str()), Some("run-started"));
    assert_eq!(got.last().map(|t| t.0.as_str()), Some("run-finished"));

    // The per-node ordered transition sub-sequence for `transform` — identical to
    // the hand-wired demo's asserted sequence (two attempt cycles: a retryable
    // failure with its outcome + backoff marker, then a success with its outcome).
    let transform_kinds: Vec<String> = got
        .iter()
        .filter(|t| t.1.as_deref() == Some(TRANSFORM))
        .map(|t| t.0.clone())
        .collect();
    assert_eq!(
        transform_kinds,
        vec![
            "node-ready",
            "node-admitted",
            "attempt-started",
            "attempt-failed",
            "attempt-outcome",
            "attempt-failed",
            "node-admitted",
            "attempt-started",
            "attempt-succeeded",
            "attempt-outcome",
            "node-terminal",
        ],
        "the auto-adapter reproduces the hand-wired `transform` transition sequence"
    );

    // Per-node terminal states identical to the hand-wired path.
    for node in [SOURCE, TRANSFORM, SINK] {
        assert_eq!(terminal_of(&bytes, node), "succeeded");
    }
    // The retry cycle counts match the hand-wired demo exactly.
    assert_eq!(count(&bytes, "attempt-started", TRANSFORM), 2);
    assert_eq!(count(&bytes, "attempt-failed", TRANSFORM), 2);
    assert_eq!(count(&bytes, "attempt-succeeded", TRANSFORM), 1);
    for node in [SOURCE, SINK] {
        assert_eq!(count(&bytes, "attempt-started", node), 1);
        assert_eq!(count(&bytes, "attempt-succeeded", node), 1);
        assert_eq!(count(&bytes, "attempt-failed", node), 0);
    }
}

/// **Determinism.** Two runs of the same flow through the one-call path produce the
/// identical ordered transition shape — the flakiness is a deterministic counter.
#[test]
fn the_auto_adapter_path_is_deterministic() {
    let (a, oa, _) = run_via_auto_adapter();
    let (b, ob, _) = run_via_auto_adapter();
    assert_eq!(oa, ob);
    assert_eq!(shape(&a), shape(&b), "identical ordered transition shape");
}

/// **Failure propagates.** When the middle node fails permanently, the sink ends
/// `upstream-failed` and the run fails overall — the adapter drives the real
/// terminal-state propagation, not a re-implementation.
#[test]
fn permanent_failure_propagates_downstream() {
    let temp_base = TempBase::new("run-a-flow-fail");
    let mut flow = RunnableFlow::new();
    let source = flow.register_source(SOURCE, Source { value: SEED });
    let transform = flow.register::<AlwaysFails, _>(TRANSFORM, AlwaysFails, source);
    let _sink = flow.register::<Sink, _>(SINK, Sink, transform);

    let mem = MemorySink::default();
    let report = flow
        .run(
            "run-a-flow-fail",
            &dagr_cli::driver::RunConfig::new(temp_base.as_str()),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");
    let bytes = mem.bytes();
    assert_eq!(terminal_of(&bytes, TRANSFORM), "failed", "middle failed");
    assert_eq!(
        terminal_of(&bytes, SINK),
        "upstream-failed",
        "sink is upstream-failed"
    );
    assert_ne!(
        report.outcome(),
        RunOutcome::Succeeded,
        "the run does not succeed when a node fails"
    );
}

/// **A scratch-touching node runs through the adapter (scratch wiring intact).** A
/// single-attempt node writes to and reads back its real per-node durable scratch
/// namespace under the run store — proving the adapter builds a `NodeRunner` that
/// works with the driver's real per-attempt context (`scratch_root` threaded
/// through). Uses a PRIVATE temp dir for the run store.
#[test]
fn a_scratch_touching_node_runs_through_the_adapter() {
    struct WritesScratch;
    impl Task for WritesScratch {
        type Input = ();
        type Output = u64;
        async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<u64, TaskError> {
            let scratch = ctx.scratch();
            scratch
                .put(b"marker", b"scratch-reached")
                .map_err(|e| TaskError::permanent(format!("scratch write failed: {e}")))?;
            let read = scratch
                .get(b"marker")
                .map_err(|e| TaskError::permanent(format!("scratch read failed: {e}")))?;
            assert_eq!(read.as_deref(), Some(&b"scratch-reached"[..]));
            Ok(read.map_or(0, |b| b.len() as u64))
        }
    }

    let tmp = std::env::temp_dir().join(format!(
        "dagr-run-a-flow-scratch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&tmp).unwrap();

    let mut flow = RunnableFlow::new();
    let node = flow.register_source("writes-scratch", WritesScratch);

    let mem = MemorySink::default();
    let report = flow
        .run(
            "run-a-flow-scratch",
            &dagr_cli::driver::RunConfig::new(tmp.to_string_lossy().to_string()),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");
    assert_eq!(report.outcome(), RunOutcome::Succeeded);
    assert_eq!(terminal_of(&mem.bytes(), "writes-scratch"), "succeeded");
    assert_eq!(
        report.output(node),
        Some(b"scratch-reached".len() as u64),
        "the scratch-touching node reached its real scratch namespace"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

/// **Genericity across a type boundary.** The adapter is generic over `Task`
/// `Input`/`Output`, so a chain whose nodes carry **distinct** value types
/// (`u64 → String → usize`) must flow each value through the generic edge-downcast
/// unchanged. This crosses a real type boundary (unlike the all-`u64` chain), so a
/// type-confusion in the erased slot downcast — which would still *compile* — is
/// caught by the observable value at the end.
#[test]
fn distinct_input_and_output_types_flow_through_the_adapter() {
    let temp_base = TempBase::new("run-a-flow-types");
    struct MakeNumber;
    impl Task for MakeNumber {
        type Input = ();
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
            Ok(7)
        }
    }
    /// `u64 → String`: renders its input as text (a different Output type).
    struct Render;
    impl Task for Render {
        type Input = u64;
        type Output = String;
        async fn run(&mut self, _c: &RunContext, input: u64) -> Result<String, TaskError> {
            Ok(format!("value={input}"))
        }
    }
    /// `String → usize`: consumes the `String` (proving the `String` edge
    /// round-tripped through the erased slot) and yields its byte length.
    struct Measure;
    impl Task for Measure {
        type Input = String;
        type Output = usize;
        async fn run(&mut self, _c: &RunContext, input: String) -> Result<usize, TaskError> {
            Ok(input.len())
        }
    }

    let mut flow = RunnableFlow::new();
    let number = flow.register_source("number", MakeNumber);
    let rendered = flow.register::<Render, _>("render", Render, number);
    let measured = flow.register::<Measure, _>("measure", Measure, rendered.clone_on_read());

    let mem = MemorySink::default();
    let report = flow
        .run(
            "run-a-flow-types",
            &dagr_cli::driver::RunConfig::new(temp_base.as_str()),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    let bytes = mem.bytes();
    assert_eq!(report.outcome(), RunOutcome::Succeeded, "the run succeeds");
    for node in ["number", "render", "measure"] {
        assert_eq!(terminal_of(&bytes, node), "succeeded", "`{node}` succeeded");
    }
    // The `String` produced by `render` crossed the erased slot boundary intact:
    // `"value=7"` has 7 bytes, so `measure` reads back exactly that length.
    assert_eq!(
        report.output(rendered),
        Some("value=7".to_string()),
        "the String value flowed through the generic adapter unchanged"
    );
    assert_eq!(
        report.output(measured),
        Some("value=7".len()),
        "the downstream node consumed the String and produced its length"
    );
}

// ===========================================================================
// Multi-input tuple wiring: a 2..=8-input node runs through RunnableFlow,
// assembling `Deps::into_edges` positionally into the declared tuple, honouring
// each edge's declared receive mode, reading INSIDE run() (never at assembly).
// ===========================================================================

/// A two-input node that keeps its inputs distinguishable by POSITION: it
/// concatenates `first` then `second` so a swapped order is observable.
struct JoinInOrder;
impl Task for JoinInOrder {
    type Input = (String, String);
    type Output = String;
    async fn run(
        &mut self,
        _c: &RunContext,
        (first, second): (String, String),
    ) -> Result<String, TaskError> {
        Ok(format!("{first}|{second}"))
    }
}

struct EmitText {
    text: &'static str,
}
impl Task for EmitText {
    type Input = ();
    type Output = String;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<String, TaskError> {
        Ok(self.text.to_string())
    }
}

/// **A two-input node receives both upstream values in DECLARED ORDER.** Two
/// sources emit distinct strings; the join node binds `(left, right)` and must
/// see `left` at position 0 and `right` at position 1 — proving the tuple
/// `InputWiring` reader assembles `Deps::into_edges` positionally.
#[test]
fn two_input_node_receives_upstreams_in_declared_order() {
    let temp_base = TempBase::new("run-a-flow-tuple-order");
    let mut flow = RunnableFlow::new();
    let left = flow.register_source("left", EmitText { text: "LEFT" });
    let right = flow.register_source("right", EmitText { text: "RIGHT" });
    // Bind in declared order (left, right) — both clone-on-read so nothing needs
    // ownership adjudication and each edge is independently readable.
    let joined = flow.register::<JoinInOrder, _>(
        "join",
        JoinInOrder,
        (left.clone_on_read(), right.clone_on_read()),
    );

    let mem = MemorySink::default();
    let report = flow
        .run(
            "run-a-flow-tuple-order",
            &dagr_cli::driver::RunConfig::new(temp_base.as_str()),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    let bytes = mem.bytes();
    assert_eq!(report.outcome(), RunOutcome::Succeeded, "the run succeeds");
    for node in ["left", "right", "join"] {
        assert_eq!(terminal_of(&bytes, node), "succeeded", "`{node}` succeeded");
    }
    assert_eq!(
        report.output(joined),
        Some("LEFT|RIGHT".to_string()),
        "the two upstream values arrived in DECLARED order (left at position 0)"
    );
}

/// **A `handle.shared()` edge is honoured by the tuple reader while its sibling
/// reads under its own declared mode.** One upstream is bound `shared()`, the
/// other `clone_on_read()`; both values must still flow into the tuple in order.
/// The receive mode comes from the registration-site binding, never the macro.
#[test]
fn two_input_node_honours_a_shared_edge_alongside_a_clone_on_read_edge() {
    let temp_base = TempBase::new("run-a-flow-tuple-shared");
    let mut flow = RunnableFlow::new();
    let left = flow.register_source("left", EmitText { text: "SHARED" });
    let right = flow.register_source("right", EmitText { text: "CLONED" });
    // left via shared(), right via clone_on_read() — mixed modes on one node.
    let joined = flow.register::<JoinInOrder, _>(
        "join",
        JoinInOrder,
        (left.shared(), right.clone_on_read()),
    );

    let mem = MemorySink::default();
    let report = flow
        .run(
            "run-a-flow-tuple-shared",
            &dagr_cli::driver::RunConfig::new(temp_base.as_str()),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    let bytes = mem.bytes();
    assert_eq!(report.outcome(), RunOutcome::Succeeded, "the run succeeds");
    for node in ["left", "right", "join"] {
        assert_eq!(terminal_of(&bytes, node), "succeeded", "`{node}` succeeded");
    }
    assert_eq!(
        report.output(joined),
        Some("SHARED|CLONED".to_string()),
        "the shared edge and the clone-on-read edge both delivered, in order"
    );
}

/// **A multi-input node reads its inputs INSIDE `run()`, after its upstreams
/// succeed — not at plan-assembly (read-before-fill would panic on empty
/// slots).** A three-input node fed by three sources completes cleanly and reads
/// all three values; if the tuple reader read at assembly time the run would
/// panic, so a clean success is the proof of deferral.
#[test]
fn three_input_node_reads_inside_run_after_upstreams_succeed() {
    let temp_base = TempBase::new("run-a-flow-tuple-deferred");
    struct SumThree;
    impl Task for SumThree {
        type Input = (u64, u64, u64);
        type Output = u64;
        async fn run(
            &mut self,
            _c: &RunContext,
            (a, b, c): (u64, u64, u64),
        ) -> Result<u64, TaskError> {
            Ok(a + 10 * b + 100 * c)
        }
    }
    struct Emit {
        n: u64,
    }
    impl Task for Emit {
        type Input = ();
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
            Ok(self.n)
        }
    }

    let mut flow = RunnableFlow::new();
    let a = flow.register_source("a", Emit { n: 1 });
    let b = flow.register_source("b", Emit { n: 2 });
    let c = flow.register_source("c", Emit { n: 3 });
    let sum = flow.register::<SumThree, _>(
        "sum",
        SumThree,
        (a.clone_on_read(), b.clone_on_read(), c.clone_on_read()),
    );

    let mem = MemorySink::default();
    let report = flow
        .run(
            "run-a-flow-tuple-deferred",
            &dagr_cli::driver::RunConfig::new(temp_base.as_str()),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    let bytes = mem.bytes();
    assert_eq!(report.outcome(), RunOutcome::Succeeded, "the run succeeds");
    for node in ["a", "b", "c", "sum"] {
        assert_eq!(terminal_of(&bytes, node), "succeeded", "`{node}` succeeded");
    }
    // Positional weights (1 + 10*2 + 100*3 = 321) prove all three arrived in order.
    assert_eq!(
        report.output(sum),
        Some(321),
        "the three inputs were read inside run(), in declared order"
    );
}
