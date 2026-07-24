//! M1 run-loop driver tests — ticket T24 (034). Written first, TDD.
//!
//! These exercise the **real** driver in [`dagr_cli::driver`]: the component that
//! orchestrates one complete run from an assembled pipeline to a truthful end
//! (arch.md "The shape of a run", `### C11`, `### C14`, `### C19`). Each scenario
//! builds a real assembled pipeline (or a deliberately-failing one), drives it
//! through the driver, and asserts against the **actual sink output** (the parsed
//! event stream) and the returned outcome — never internal state.
//!
//! Scope discipline (T24): this is the minimal readiness-driven run loop only. No
//! admission pools (T31), no cancellation triggering/signals (T34/T35), no scale
//! authority (T26), no fault injection (T27), no runtime firing of non-default
//! trigger rules (T34). The loop composes the merged M1 pieces (readiness tracker,
//! single-attempt/caught runner, event-stream writer, run store) and consumes the
//! C16 grace period only as the bounded zombie wait at natural run end.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{run_attempt, run_attempt_caught, AttemptEventSink};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// A capturing, in-memory run-store sink + monotonic clock (C19 injection seam)
// ===========================================================================

/// An in-memory [`EventSink`] capturing every appended line, so a test can parse
/// the real event stream the driver wrote — the T24 test-plan requirement that
/// streams are asserted by parsing the actual sink output.
#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
    flushed: Arc<Mutex<bool>>,
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
        *self.flushed.lock().unwrap() = true;
        Ok(())
    }
}

/// A monotonic clock ticking one nanosecond per read — enough to give distinct,
/// non-decreasing offsets without a real clock.
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
// Parsed event-stream helpers
// ===========================================================================

/// Parse the sink bytes into `(event-kind, node-or-none)` pairs in stream order.
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
            // Per-kind payload is spread top-level now (no `body`).
            let node = rec.get("node").and_then(|v| v.as_str()).map(str::to_string);
            (kind, node)
        })
        .collect()
}

/// The index of the first record matching `(kind, node)`, or `None`.
fn index_of(events: &[(String, Option<String>)], kind: &str, node: Option<&str>) -> Option<usize> {
    events
        .iter()
        .position(|(k, n)| k == kind && n.as_deref() == node)
}

/// The terminal state recorded for `node` in the stream (its `node-terminal`
/// record's `state` field), or `None`.
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

/// The `run-started` record's body (the header), or `None`.
fn run_started_body(bytes: &[u8]) -> Option<serde_json::Value> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream.records.iter().find_map(|rec| {
        if rec.get("kind").and_then(|v| v.as_str()) == Some("run-started") {
            rec.get("header").cloned()
        } else {
            None
        }
    })
}

/// The `run_id` field of the first record, or `None`.
fn run_id_field(bytes: &[u8]) -> Option<String> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream.records.first().and_then(|rec| {
        rec.get("run_id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    })
}

// ===========================================================================
// Test tasks
// ===========================================================================

/// A source task that succeeds, producing a counter value.
struct SucceedsWith(u64);
impl Task for SucceedsWith {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.0)
    }
}

/// A source task that permanently fails.
struct AlwaysFails;
impl Task for AlwaysFails {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Err(TaskError::permanent("nope"))
    }
}

/// A source task that returns a deliberate skip.
struct AlwaysSkips;
impl Task for AlwaysSkips {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Err(TaskError::skip("nothing to do"))
    }
}

/// A source task that busy-blocks its worker forever (never returns) — the
/// misbehaving/zombie task. It consults nothing and simply spins.
struct BlocksForever;
impl Task for BlocksForever {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // A synchronous busy-loop that jams the worker thread it is placed on —
        // it never awaits and never returns, modelling an unkillable blocking task.
        loop {
            std::hint::spin_loop();
        }
    }
}

// ===========================================================================
// Type-erased node runners built on the real C14 attempt path
// ===========================================================================

/// A no-input source runner: runs its task's single attempt through the real
/// caught runner and reports the terminal state.
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
        let mut task = self.task.take().expect("source runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            let outcome = run_attempt_caught(&mut task, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

/// A one-input data runner: reads its single upstream slot (shared), runs a task
/// that consumes that value, and reports the terminal state.
struct MapRunner<U: Send + Sync + 'static, T: Task<Input = U>> {
    name: String,
    task: Option<T>,
    upstream: SlotRef<U>,
    slot: Arc<Slot<T::Output>>,
}

impl<U: Send + Sync + Clone + 'static, T: Task<Input = U>> MapRunner<U, T> {
    fn boxed(
        name: &str,
        task: T,
        upstream: SlotRef<U>,
        slot: Arc<Slot<T::Output>>,
    ) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            upstream,
            slot,
        })
    }
}

impl<U: Send + Sync + Clone + 'static, T: Task<Input = U>> NodeRunner for MapRunner<U, T> {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let task = self.task.take().expect("map runner runs once");
        let slot = Arc::clone(&self.slot);
        // The upstream succeeded before this node was admitted, so its slot is
        // filled; read the value a one-input task expects and pre-bind it into an
        // owned no-input adapter, so the real single-attempt runner drives it and
        // emits the genuine C14 records.
        let input = (*self.upstream.read()).clone();
        let mut bound = Bound {
            inner: task,
            input: Some(input),
        };
        Box::pin(async move {
            let outcome = run_attempt(&mut bound, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

/// An **owned** no-input adapter over a one-input task: it holds the task by value
/// (so it is `'static` and `Send`, satisfying the `Task` bounds `run_attempt`
/// needs) and pre-binds the input value the upstream produced. Reusing the real
/// single-attempt runner over this adapter means the emitted records are the
/// genuine C14 ones (admission marker, attempt-started, attempt-outcome,
/// node-terminal), not a re-implementation.
struct Bound<U, T> {
    inner: T,
    input: Option<U>,
}
impl<U: Send + 'static, T: Task<Input = U>> Task for Bound<U, T> {
    type Input = ();
    type Output = T::Output;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<T::Output, TaskError> {
        let input = self.input.take().expect("bound input consumed once");
        self.inner.run(ctx, input).await
    }
}

// ===========================================================================
// Pipeline + plan builders
// ===========================================================================

fn ledger() -> Arc<ResidencyLedger> {
    ResidencyLedger::new()
}

/// Build a fresh output slot for a node.
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

/// A one-node pipeline whose single source succeeds.
fn single_success_plan() -> (Pipeline, RunPlan) {
    let mut flow = Flow::new();
    let _h = flow.register_source("only", &SucceedsWith(7));
    let pipeline = flow.finish();
    let assembled = pipeline.assemble().expect("assembles");
    let _ = assembled;
    let slot = slot_for::<u64>("only", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "only".into(),
        SourceRunner::boxed("only", SucceedsWith(7), slot),
    );
    let plan = RunPlan::new(pipeline.clone(), runners);
    (pipeline, plan)
}

// ===========================================================================
// The tests
// ===========================================================================

/// Happy-path single node terminates: the stream contains, in order, run-started,
/// node-ready, node-admitted, attempt-started, attempt-succeeded,
/// node-terminal(succeeded), run-finished; the driver returns an overall-success
/// outcome; the call returns rather than hanging.
#[test]
fn happy_path_single_node_terminates() {
    let (_pipeline, plan) = single_success_plan();
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "demo",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded, "overall success");
    assert_eq!(
        report.terminal_states.get("only").copied(),
        Some(TerminalState::Succeeded)
    );

    let events = parse_events(&sink.bytes());
    // The mandated ordered prefix (node records may interleave admission markers,
    // but these anchors must appear in this relative order for the single node).
    let order = [
        ("run-started", None),
        ("node-ready", Some("only")),
        ("node-admitted", Some("only")),
        ("attempt-started", Some("only")),
        ("attempt-succeeded", Some("only")),
        ("node-terminal", Some("only")),
        ("run-finished", None),
    ];
    let mut last = 0;
    for (kind, node) in order {
        let idx = index_of(&events, kind, node)
            .unwrap_or_else(|| panic!("missing {kind} for {node:?} in {events:?}"));
        assert!(
            idx >= last,
            "record {kind}/{node:?} out of order in {events:?}"
        );
        last = idx;
    }
    // run-finished is the final record.
    assert_eq!(events.last().map(|(k, _)| k.as_str()), Some("run-finished"));
}

/// Linear chain drives dependents: A→B→C, all succeeding. B's node-ready appears
/// only after A's node-terminal; C's only after B's; every node ends succeeded;
/// run-finished is the last record.
#[test]
fn linear_chain_drives_dependents() {
    /// A one-input task that passes its input through unchanged.
    struct PassThrough;
    impl Task for PassThrough {
        type Input = u64;
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
            Ok(i)
        }
    }

    let mut flow = Flow::new();
    let a = flow.register_source("a", &SucceedsWith(1));
    let b = flow.register::<PassThrough, _>("b", &PassThrough, a);
    let _c = flow.register::<PassThrough, _>("c", &PassThrough, b);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let a_slot = slot_for::<u64>("a", 1);
    let b_slot = slot_for::<u64>("b", 1);
    let c_slot = slot_for::<u64>("c", 0);

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "a".into(),
        SourceRunner::boxed("a", SucceedsWith(1), Arc::clone(&a_slot)),
    );
    runners.insert(
        "b".into(),
        MapRunner::boxed("b", PassThrough, a_slot.shared_ref(), Arc::clone(&b_slot)),
    );
    runners.insert(
        "c".into(),
        MapRunner::boxed("c", PassThrough, b_slot.shared_ref(), c_slot),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "chain",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    for n in ["a", "b", "c"] {
        assert_eq!(
            terminal_of(&sink.bytes(), n).as_deref(),
            Some("succeeded"),
            "{n}"
        );
    }

    let events = parse_events(&sink.bytes());
    let a_term = index_of(&events, "node-terminal", Some("a")).unwrap();
    let b_ready = index_of(&events, "node-ready", Some("b")).unwrap();
    let b_term = index_of(&events, "node-terminal", Some("b")).unwrap();
    let c_ready = index_of(&events, "node-ready", Some("c")).unwrap();
    assert!(b_ready > a_term, "B ready only after A terminal");
    assert!(c_ready > b_term, "C ready only after B terminal");
    assert_eq!(events.last().map(|(k, _)| k.as_str()), Some("run-finished"));
}

/// Fast branch is not gated on the slow branch (no wave batching): in a diamond
/// where one branch is slow, the fast branch's independent descendant reaches
/// node-admitted before the slow branch's node-terminal appears.
#[test]
fn fast_branch_not_gated_on_slow_branch() {
    /// A one-input task that sleeps `delay` before returning its input.
    struct Slow {
        delay: Duration,
    }
    impl Task for Slow {
        type Input = u64;
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
            tokio::time::sleep(self.delay).await;
            Ok(i)
        }
    }
    struct Fast;
    impl Task for Fast {
        type Input = u64;
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
            Ok(i)
        }
    }

    // root -> slow ; root -> fast -> fast_child   (fast_child does NOT depend on slow)
    // `root` fans out to two consumers, so its edges are received SHARED (an owned
    // multi-consumer edge is an assembly error — C3/T0.2).
    let mut flow = Flow::new();
    let root = flow.register_source("root", &SucceedsWith(1));
    let _slow = flow.register::<Slow, _>(
        "slow",
        &Slow {
            delay: Duration::from_millis(300),
        },
        root.shared(),
    );
    let fast = flow.register::<Fast, _>("fast", &Fast, root.shared());
    let _child = flow.register::<Fast, _>("fast_child", &Fast, fast);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let root_slot = slot_for::<u64>("root", 2);
    let slow_slot = slot_for::<u64>("slow", 0);
    let fast_slot = slot_for::<u64>("fast", 1);
    let child_slot = slot_for::<u64>("fast_child", 0);

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "root".into(),
        SourceRunner::boxed("root", SucceedsWith(1), Arc::clone(&root_slot)),
    );
    runners.insert(
        "slow".into(),
        MapRunner::boxed(
            "slow",
            Slow {
                delay: Duration::from_millis(300),
            },
            root_slot.shared_ref(),
            slow_slot,
        ),
    );
    runners.insert(
        "fast".into(),
        MapRunner::boxed("fast", Fast, root_slot.shared_ref(), Arc::clone(&fast_slot)),
    );
    runners.insert(
        "fast_child".into(),
        MapRunner::boxed("fast_child", Fast, fast_slot.shared_ref(), child_slot),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "diamond",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(report.outcome, RunOutcome::Succeeded);

    let events = parse_events(&sink.bytes());
    let child_admitted = index_of(&events, "node-admitted", Some("fast_child")).unwrap();
    let slow_terminal = index_of(&events, "node-terminal", Some("slow")).unwrap();
    assert!(
        child_admitted < slow_terminal,
        "fast branch descendant must be admitted before the slow branch terminates (no wave batching): {events:?}"
    );
}

/// Identity is a `UUIDv7` minted at bootstrap with no override; every record carries
/// it and the report exposes it.
#[test]
fn identity_is_a_uuidv7_minted_at_bootstrap() {
    let (_p, plan) = single_success_plan();
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "demo",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    let id = run_id_field(&sink.bytes()).expect("a run id on the first record");
    let parsed = uuid_parse(&id);
    assert_eq!(parsed, Some(7), "run id is a well-formed `UUIDv7`");
    assert_eq!(report.run_id, id, "report exposes the same identity");
    // Every record carries that identity.
    let stream = dagr_artifact::event_stream::read_records(&sink.bytes()).unwrap();
    for rec in &stream.records {
        assert_eq!(
            rec.get("run_id").and_then(|v| v.as_str()),
            Some(id.as_str())
        );
    }
}

/// Operator override replaces the minted identity everywhere.
#[test]
fn operator_override_replaces_the_minted_identity() {
    let (_p, plan) = single_success_plan();
    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test").run_id("my-explicit-run-42"),
        "demo",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(report.run_id, "my-explicit-run-42");
    let stream = dagr_artifact::event_stream::read_records(&sink.bytes()).unwrap();
    for rec in &stream.records {
        assert_eq!(
            rec.get("run_id").and_then(|v| v.as_str()),
            Some("my-explicit-run-42")
        );
    }
}

/// Store and stream open before assembly: an assembly failure still records a
/// run-started (carrying the minted identity) and the driver reports an
/// assembly-failure outcome distinct from a successful run.
#[test]
fn assembly_failure_still_records() {
    // A pipeline that fails assembly: two source nodes registered under the same
    // name (a duplicate-name assembly error).
    let mut flow = Flow::new();
    let _a = flow.register_source("dup", &SucceedsWith(1));
    let _b = flow.register_source("dup", &SucceedsWith(2));
    let pipeline = flow.finish();
    let assembled: Result<RunPlan, _> = pipeline
        .assemble()
        .map(|_| RunPlan::new(pipeline.clone(), BTreeMap::new()));
    assert!(
        assembled.is_err(),
        "the fixture must actually fail assembly"
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "broken",
        assembled,
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(report.outcome, RunOutcome::AssemblyFailed);

    // A run-started record exists on disk (proving the stream opened before
    // assembly acted), carrying the minted identity.
    let events = parse_events(&sink.bytes());
    assert!(
        index_of(&events, "run-started", None).is_some(),
        "run-started recorded even though assembly failed: {events:?}"
    );
    assert!(run_id_field(&sink.bytes()).is_some());
    assert_eq!(events.last().map(|(k, _)| k.as_str()), Some("run-finished"));
}

/// Allowlisted environment values are captured; others are not. With a non-empty
/// allowlist the allowlisted value appears in the header and the secret does not;
/// with an empty allowlist no environment value appears.
#[test]
fn allowlisted_env_captured_others_not() {
    // SAFETY: single-threaded test setup before the driver runs.
    std::env::set_var("DAGR_TEST_ALLOWED", "visible-value");
    std::env::set_var("DAGR_TEST_SECRET", "super-secret");

    let (_p, plan) = single_success_plan();
    let sink = MemorySink::default();
    let _report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "demo",
        Ok(plan),
        &["DAGR_TEST_ALLOWED".to_string()],
        sink.clone(),
        TickClock::default(),
    );
    let body = run_started_body(&sink.bytes()).expect("header");
    let captured = body
        .get("captured_environment")
        .expect("captured_environment present");
    assert_eq!(
        captured.get("DAGR_TEST_ALLOWED").and_then(|v| v.as_str()),
        Some("visible-value"),
        "allowlisted value captured"
    );
    // The secret appears nowhere in the whole stream.
    let raw = sink.bytes();
    let all = String::from_utf8_lossy(&raw);
    assert!(
        !all.contains("super-secret"),
        "non-allowlisted secret must not appear"
    );
    assert!(
        !all.contains("DAGR_TEST_SECRET"),
        "non-allowlisted name must not appear"
    );

    // Empty allowlist → no environment value captured.
    let (_p2, plan2) = single_success_plan();
    let sink2 = MemorySink::default();
    let _ = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "demo",
        Ok(plan2),
        &[],
        sink2.clone(),
        TickClock::default(),
    );
    let body2 = run_started_body(&sink2.bytes()).expect("header");
    let captured2 = body2
        .get("captured_environment")
        .expect("captured_environment present");
    assert_eq!(
        captured2.as_object().map(serde_json::Map::len),
        Some(0),
        "empty allowlist captures nothing"
    );

    std::env::remove_var("DAGR_TEST_ALLOWED");
    std::env::remove_var("DAGR_TEST_SECRET");
}

/// The run-started header carries the fields known at start: identity, pipeline
/// identity, both fingerprints, parameters and data interval.
#[test]
fn run_started_header_carries_start_fields() {
    let mut flow = Flow::new();
    let _h = flow.register_source("only", &SucceedsWith(7));
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");
    let slot = slot_for::<u64>("only", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "only".into(),
        SourceRunner::boxed("only", SucceedsWith(7), slot),
    );

    let mut params = BTreeMap::new();
    params.insert("threshold".to_string(), "5".to_string());

    let sink = MemorySink::default();
    let _ = drive(
        &RunConfig::new("/tmp/dagr-test")
            .parameters(params)
            .data_interval(["2026-01-01".into(), "2026-01-02".into()]),
        "demo-pipeline",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    let body = run_started_body(&sink.bytes()).expect("header");
    assert_eq!(
        body.get("pipeline").and_then(|v| v.as_str()),
        Some("demo-pipeline")
    );
    assert!(
        body.get("fingerprint_structural").is_some(),
        "structural fingerprint present"
    );
    assert!(
        body.get("fingerprint_policy").is_some(),
        "policy hash present"
    );
    assert_eq!(
        body.get("parameters")
            .and_then(|p| p.get("threshold"))
            .and_then(|v| v.as_str()),
        Some("5")
    );
    // The data interval threads through `drive()` VERBATIM: the exact configured
    // endpoints appear in the emitted header (not merely "present" — proving the
    // driver carries the interval it was given, byte-for-byte).
    let interval = body.get("data_interval").expect("data interval present");
    assert_eq!(
        interval.get("start").and_then(|v| v.as_str()),
        Some("2026-01-01"),
        "the configured interval start threads verbatim into the emitted header"
    );
    assert_eq!(
        interval.get("end").and_then(|v| v.as_str()),
        Some("2026-01-02"),
        "the configured interval end threads verbatim into the emitted header"
    );
    // The overall outcome and summary are NOT in the run-started header.
    assert!(body.get("outcome").is_none(), "no overall outcome at start");
}

/// Every attempt outcome is fed back and produces its records: a two-node
/// pipeline where the downstream's readiness followed only after the upstream's
/// terminal outcome was fed back; each node produces exactly one attempt-outcome
/// record and a single node-terminal event.
#[test]
fn every_outcome_is_fed_back() {
    struct PassThrough;
    impl Task for PassThrough {
        type Input = u64;
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
            Ok(i)
        }
    }
    let mut flow = Flow::new();
    let up = flow.register_source("up", &SucceedsWith(3));
    let _down = flow.register::<PassThrough, _>("down", &PassThrough, up);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let up_slot = slot_for::<u64>("up", 1);
    let down_slot = slot_for::<u64>("down", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "up".into(),
        SourceRunner::boxed("up", SucceedsWith(3), Arc::clone(&up_slot)),
    );
    runners.insert(
        "down".into(),
        MapRunner::boxed("down", PassThrough, up_slot.shared_ref(), down_slot),
    );

    let sink = MemorySink::default();
    let _ = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "two",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );

    let events = parse_events(&sink.bytes());
    // Each node: exactly one node-terminal record AND exactly one attempt-outcome
    // record (arch.md l.331: every attempt produces exactly one attempt-outcome).
    for n in ["up", "down"] {
        let terminals = events
            .iter()
            .filter(|(k, node)| k == "node-terminal" && node.as_deref() == Some(n))
            .count();
        assert_eq!(terminals, 1, "exactly one node-terminal for {n}");
        let outcomes = events
            .iter()
            .filter(|(k, node)| k == "attempt-outcome" && node.as_deref() == Some(n))
            .count();
        assert_eq!(outcomes, 1, "exactly one attempt-outcome for {n}");
    }
    // down readiness only after up terminal.
    let up_term = index_of(&events, "node-terminal", Some("up")).unwrap();
    let down_ready = index_of(&events, "node-ready", Some("down")).unwrap();
    assert!(down_ready > up_term);
}

/// A failing/mixed graph produces the correct terminal states: an upstream that
/// permanently fails deadens an all-succeeded downstream (upstream-failed), and
/// the overall outcome is failed.
#[test]
fn failing_upstream_propagates_and_run_fails() {
    struct PassThrough;
    impl Task for PassThrough {
        type Input = u64;
        type Output = u64;
        async fn run(&mut self, _c: &RunContext, i: u64) -> Result<u64, TaskError> {
            Ok(i)
        }
    }
    let mut flow = Flow::new();
    let up = flow.register_source("up", &AlwaysFails);
    let _down = flow.register::<PassThrough, _>("down", &PassThrough, up);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let up_slot = slot_for::<u64>("up", 1);
    let down_slot = slot_for::<u64>("down", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "up".into(),
        SourceRunner::boxed("up", AlwaysFails, Arc::clone(&up_slot)),
    );
    runners.insert(
        "down".into(),
        MapRunner::boxed("down", PassThrough, up_slot.shared_ref(), down_slot),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "mixed",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(report.outcome, RunOutcome::Failed);
    assert_eq!(terminal_of(&sink.bytes(), "up").as_deref(), Some("failed"));
    assert_eq!(
        terminal_of(&sink.bytes(), "down").as_deref(),
        Some("upstream-failed"),
        "the deadened downstream carries the propagated state"
    );
    // The propagated (never-executed) node produced no attempt-started record.
    let events = parse_events(&sink.bytes());
    assert!(
        index_of(&events, "attempt-started", Some("down")).is_none(),
        "a propagated-terminal node never executes"
    );
}

/// Skip-only run reports success: the only node returns a deliberate skip; the
/// node ends skipped, the run terminates, and the overall outcome is success.
#[test]
fn skip_only_run_reports_success() {
    let mut flow = Flow::new();
    let _h = flow.register_source("skipper", &AlwaysSkips);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");
    let slot = slot_for::<u64>("skipper", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "skipper".into(),
        SourceRunner::boxed("skipper", AlwaysSkips, slot),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "skips",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    assert_eq!(
        terminal_of(&sink.bytes(), "skipper").as_deref(),
        Some("skipped")
    );
    assert_eq!(
        report.outcome,
        RunOutcome::Succeeded,
        "a run containing only skips is a successful run"
    );
}

/// Run ends precisely when nothing is pending or in flight: the last node has no
/// dependents; run-finished is emitted with no further admissions after it.
#[test]
fn run_ends_when_nothing_pending_or_in_flight() {
    let (_p, plan) = single_success_plan();
    let sink = MemorySink::default();
    let _ = drive(
        &RunConfig::new("/tmp/dagr-test"),
        "demo",
        Ok(plan),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    let events = parse_events(&sink.bytes());
    let finished = index_of(&events, "run-finished", None).unwrap();
    // Nothing appears after run-finished.
    assert_eq!(
        finished,
        events.len() - 1,
        "run-finished is the last record"
    );
    // No node-admitted after the terminal of the only node.
    let terminal = index_of(&events, "node-terminal", Some("only")).unwrap();
    assert!(
        events
            .iter()
            .skip(terminal + 1)
            .all(|(k, _)| k != "node-admitted"),
        "no admissions after the last node terminated"
    );
}

/// Framework machinery survives a misbehaving task: a blocking task jams a worker;
/// with a tiny per-attempt timeout the timeout fires, the stream is still written
/// (run-started, node-ready, node-admitted, attempt-started present), and the
/// driver still reaches run-finished.
#[test]
fn framework_survives_a_misbehaving_task() {
    let mut flow = Flow::new();
    let _h = flow.register_source("blocker", &BlocksForever);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");
    let slot = slot_for::<u64>("blocker", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    // A blocking runner that uses the per-attempt timeout: the driver arms the
    // timeout on the framework runtime so it fires even though the body jams.
    runners.insert(
        "blocker".into(),
        BlockingTimeoutRunner::boxed("blocker", BlocksForever, slot),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-test").grace(Duration::from_millis(200)),
        "blocked",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    // The run still finished, and the node's fate was decided as timed-out.
    let events = parse_events(&sink.bytes());
    for anchor in [
        "run-started",
        "node-ready",
        "node-admitted",
        "attempt-started",
        "run-finished",
    ] {
        let node = if anchor == "run-started" || anchor == "run-finished" {
            None
        } else {
            Some("blocker")
        };
        assert!(
            index_of(&events, anchor, node).is_some(),
            "missing {anchor} — the writer stalled with the task: {events:?}"
        );
    }
    assert_eq!(
        terminal_of(&sink.bytes(), "blocker").as_deref(),
        Some("timed-out"),
        "the blocking timeout fires despite the jammed worker"
    );
    // The node's terminal state stays timed-out (never a second terminal state).
    assert_eq!(
        report.terminal_states.get("blocker").copied(),
        Some(TerminalState::TimedOut)
    );
}

/// Zombie at natural run end: the sole node is a blocking task already marked
/// timed-out while its thread refuses to return; the driver waits no longer than
/// the grace period, emits a zombie-at-exit event, then run-finished — the
/// abandoned closure did not hold the run open indefinitely; the terminal state
/// stays timed-out.
#[test]
fn zombie_at_natural_run_end() {
    let mut flow = Flow::new();
    let _h = flow.register_source("zombie", &BlocksForever);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");
    let slot = slot_for::<u64>("zombie", 0);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "zombie".into(),
        BlockingTimeoutRunner::boxed("zombie", BlocksForever, slot),
    );

    let sink = MemorySink::default();
    let start = std::time::Instant::now();
    let grace = Duration::from_millis(200);
    let report = drive(
        &RunConfig::new("/tmp/dagr-test").grace(grace),
        "zombie-run",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    let elapsed = start.elapsed();

    // The abandoned closure did not hold the run open indefinitely: the whole
    // drive finished within a small multiple of the grace period.
    assert!(
        elapsed < grace + Duration::from_secs(5),
        "the run must not hang on the zombie: took {elapsed:?}"
    );
    let events = parse_events(&sink.bytes());
    let zombie = index_of(&events, "zombie-at-exit", Some("zombie"));
    assert!(
        zombie.is_some(),
        "a zombie-at-exit event for the leftover thread: {events:?}"
    );
    let finished = index_of(&events, "run-finished", None).unwrap();
    assert!(
        zombie.unwrap() < finished,
        "zombie-at-exit precedes run-finished"
    );
    // The terminal state stays timed-out — never a second terminal state.
    assert_eq!(
        terminal_of(&sink.bytes(), "zombie").as_deref(),
        Some("timed-out")
    );
    let terminal_count = events
        .iter()
        .filter(|(k, n)| k == "node-terminal" && n.as_deref() == Some("zombie"))
        .count();
    assert_eq!(
        terminal_count, 1,
        "exactly one terminal state for the zombie node"
    );
    assert_eq!(
        report.terminal_states.get("zombie").copied(),
        Some(TerminalState::TimedOut)
    );
}

/// Two simultaneous runs of the same binary do not interfere: two runs against
/// the same base store write disjoint streams, both valid, each record carrying
/// its own run identity.
#[test]
fn two_simultaneous_runs_do_not_interfere() {
    let run_once = |run_id: &str| {
        let (_p, plan) = single_success_plan();
        let sink = MemorySink::default();
        let _ = drive(
            &RunConfig::new("/tmp/dagr-test-shared").run_id(run_id),
            "demo",
            Ok(plan),
            &[],
            sink.clone(),
            TickClock::default(),
        );
        sink.bytes()
    };
    let h1 = std::thread::spawn(move || run_once("run-alpha"));
    let h2 = {
        let run_once = |run_id: &str| {
            let (_p, plan) = single_success_plan();
            let sink = MemorySink::default();
            let _ = drive(
                &RunConfig::new("/tmp/dagr-test-shared").run_id(run_id),
                "demo",
                Ok(plan),
                &[],
                sink.clone(),
                TickClock::default(),
            );
            sink.bytes()
        };
        std::thread::spawn(move || run_once("run-beta"))
    };
    let bytes1 = h1.join().unwrap();
    let bytes2 = h2.join().unwrap();

    // Both streams are valid and parseable, and each record carries its own id.
    for (bytes, id) in [(&bytes1, "run-alpha"), (&bytes2, "run-beta")] {
        let stream = dagr_artifact::event_stream::read_records(bytes).expect("valid stream");
        assert!(!stream.records.is_empty());
        for rec in &stream.records {
            assert_eq!(rec.get("run_id").and_then(|v| v.as_str()), Some(id));
        }
    }
    // The two streams are disjoint (different identities throughout).
    assert_ne!(bytes1, bytes2);
}

// ===========================================================================
// A blocking-timeout runner (drives the T21 blocking/compute timeout path)
// ===========================================================================

/// A runner for a blocking task that never returns: it arms a short per-attempt
/// timeout on the framework runtime (the driver races it), marks the node
/// `timed-out` at the mark, and leaves the closure running as a zombie. This is
/// the driver-facing shape the misbehaving-task and zombie tests exercise.
struct BlockingTimeoutRunner<T: Task<Input = ()>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<T::Output>>,
}

impl<T: Task<Input = ()>> BlockingTimeoutRunner<T> {
    fn boxed(name: &str, task: T, slot: Arc<Slot<T::Output>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot,
        })
    }
}

impl<T: Task<Input = ()>> NodeRunner for BlockingTimeoutRunner<T>
where
    T::Output: Send,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let task = self.task.take().expect("runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            // Race the (jamming) attempt against a short deadline armed on the
            // current runtime; on timeout the node is marked timed-out and the
            // closure runs on as a zombie. `SpawnBlocking` runs the body on a
            // blocking thread so the await-side race resolves even though the body
            // never yields.
            let deadline = tokio::time::sleep(Duration::from_millis(100));
            let outcome = dagr_core::execution::run_attempt_with_timeout(
                SpawnBlocking { task: Some(task) },
                &name,
                ctx,
                &slot,
                sink,
                deadline,
                (),
            )
            .await;
            outcome.terminal_state()
        })
    }
}

/// A task wrapper that runs the inner blocking task on a dedicated blocking
/// thread, so the framework await-runtime is never itself jammed by the body.
struct SpawnBlocking<T: Task<Input = ()>> {
    task: Option<T>,
}
impl<T: Task<Input = ()>> Task for SpawnBlocking<T>
where
    T::Output: Send,
{
    type Input = ();
    type Output = T::Output;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<T::Output, TaskError> {
        let mut inner = self.task.take().expect("runs once");
        // Run the blocking body on a blocking thread; it never returns, so this
        // future stays pending until the race drops it at the deadline.
        let ctx = ctx.clone();
        tokio::task::spawn_blocking(move || {
            // Drive the inner future to completion synchronously on this blocking
            // thread. It never completes (BlocksForever), so the thread is the
            // leftover zombie.
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async move { inner.run(&ctx, ()).await })
        })
        .await
        .expect("blocking join")
    }
}

// ===========================================================================
// Small util: UUID version parse without pulling the uuid crate directly.
// ===========================================================================

/// Parse the version nibble of a canonical UUID string, returning the version
/// number (7 for `UUIDv7`) if the shape is well-formed.
fn uuid_parse(s: &str) -> Option<u8> {
    // Canonical form: 8-4-4-4-12 hex with dashes; the version is the first nibble
    // of the third group.
    let groups: Vec<&str> = s.split('-').collect();
    if groups.len() != 5 {
        return None;
    }
    if groups[0].len() != 8
        || groups[1].len() != 4
        || groups[2].len() != 4
        || groups[3].len() != 4
        || groups[4].len() != 12
    {
        return None;
    }
    if !s.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return None;
    }
    groups[2]
        .chars()
        .next()
        .and_then(|c| c.to_digit(16))
        .and_then(|v| u8::try_from(v).ok())
}
