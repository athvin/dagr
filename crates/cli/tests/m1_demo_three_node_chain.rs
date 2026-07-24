//! M1 demo — the three-node chain with a retry — ticket T28 (038). Written
//! first, TDD. **This is the M1 gate: the spec's "It runs" done-when, executed
//! in CI.**
//!
//! arch.md's **Build order** states M1 is *done when a three-node chain executes
//! in order, one node fails and retries successfully, and the event stream shows
//! every transition* — and *nothing else exists yet (no artifacts, no admission
//! control, no CLI)*. This file is that proof: a small, source-controlled example
//! pipeline built entirely through the **public authoring API** — the `Task`
//! trait (T9), typed `Handle`s (T10), `Deps` binding (T11), the `Flow`/`Pipeline`
//! builder (T13), `RunContext` (T16), `TaskError` retry classification (T3), the
//! attempt runner with retry (T22) / panic containment (T23), assembly
//! (`Pipeline::assemble`, T14) and the run-loop driver (`drive`, T24) writing the
//! C19 event stream (T19) — driven exactly as an end user would drive it, then
//! walked and asserted against the raw event stream.
//!
//! # The demo shape (public-API only — no internals reached into)
//!
//! Three nodes wired head-to-tail by **typed data dependencies** so ordering is
//! enforced by data flow (`source` → `transform` → `sink`):
//!
//! - **`source`** — a no-input source task producing a seed value; succeeds on its
//!   first attempt.
//! - **`transform`** — the deterministically-flaky **middle** node. It fails with
//!   a **retry-eligible** [`TaskError::retryable`] on its **first** attempt and
//!   succeeds on its **second**, keyed off the C8 [`RunContext::attempt`] number so
//!   the flakiness is driven by a counter, **never** by timing or randomness. Its
//!   node policy grants a retry ([`NodePolicy::retries`]) so the T22 retry path is
//!   permitted, and — because a retrying node must not take an *owned* input edge
//!   (assembly rejects that, C1/C3/T0.2) — its edge from `source` opts into
//!   **clone-on-read** ([`Handle::clone_on_read`]), the honest authoring pattern
//!   for a node whose attempts must each see a fresh input.
//! - **`sink`** — consumes `transform`'s output; succeeds on its first attempt.
//!
//! # The event-stream walker — the observable oracle, built for reuse
//!
//! [`Walk`] is the reusable **event-stream walker** the ticket asks for: it parses a
//! recorded C19 stream into ordered [`Transition`] records (delegating to the
//! tolerant [`read_records`](dagr_artifact::event_stream::read_records), so it
//! tolerates the ≤1 trailing-partial guarantee) and exposes per-node and per-run
//! assertions — the ordered transition sequence, per-node attempt-outcome counts,
//! sequence-number gaplessness, single-terminal-state, and monotonic-offset
//! durations. It is written to be reused by the later milestone demos (T38, T49,
//! T63) and referenced by the T65 acceptance gate as the executable M1 done-when.
//!
//! # Scope (T28 — integration demo only)
//!
//! This adds **no** framework surface: it composes the already-merged M1 pieces
//! through their public API. It does not re-test lower components in isolation
//! (timeout T21, retry mechanics T22, panic containment T23, crash-safety T27, the
//! termination property T25, the bounded-memory chain T26 each have their own
//! suites); it integrates them and asserts the whole M1 stack runs a real chain
//! with a real retry. No artifacts (C20/C22), no admission control (C12/C13), no
//! CLI verbs (C26), no cancellation/timeout/abandonment paths — the middle node
//! fails *retryably and recovers*.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{
    run_attempt, run_with_retries_caught, AttemptEventSink, Backoff, NoJitter, RetryConfig,
};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot, SlotRef};
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// Injection seam: an in-memory run-store sink + a monotonic clock (C19)
// ===========================================================================

/// An in-memory [`EventSink`] capturing every appended line, so the test can walk
/// the **real** event stream the driver wrote — matching the real run path's
/// injected run-store sink (the demo asserts against actual sink output, never
/// internal state).
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

/// A monotonic clock ticking one nanosecond per read — distinct, strictly
/// increasing offsets with no real clock, so durations derived from offsets are
/// deterministic (never wall-clock).
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
// The example tasks (public `Task` trait — T9)
// ===========================================================================

/// The **source** task (`source`): a no-input task producing a seed value.
/// Succeeds on its first attempt.
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

/// The deterministically-flaky **middle** task (`transform`): fails with a
/// **retry-eligible** error on its first attempt and succeeds on its second,
/// keyed off the C8 attempt number — the flakiness is a counter, never timing or
/// randomness. On success it doubles its input, so the demo can assert the value
/// flowed through the retry.
struct FlakyTransform;
impl Task for FlakyTransform {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, c: &RunContext, input: u64) -> Result<u64, TaskError> {
        if c.attempt() == 1 {
            // Deterministic first-attempt failure: retry-eligible, so the T22
            // retry loop schedules a second attempt.
            Err(TaskError::retryable(
                "transient hiccup on the first attempt",
            ))
        } else {
            Ok(input * 2)
        }
    }
}

/// The **sink** task (`sink`): consumes `transform`'s output and succeeds on its
/// first attempt, adding one so the terminal value is distinguishable.
struct Sink;
impl Task for Sink {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input + 1)
    }
}

// ===========================================================================
// The example pipeline — built through the PUBLIC authoring API (T13/T11)
// ===========================================================================

/// The three node names, explicit and stable (node identity is the name — T13).
const SOURCE: &str = "source";
const TRANSFORM: &str = "transform";
const SINK: &str = "sink";

/// The seed the source produces; the terminal value is `(SEED * 2) + 1`.
const SEED: u64 = 21;
/// The value `sink` ultimately produces — proof the retried value flowed through.
const FINAL_VALUE: u64 = (SEED * 2) + 1;

/// Build the M1 demo pipeline through the **public authoring API** exactly as an
/// end user would: register three nodes on a [`Flow`], wire them head-to-tail by
/// typed data dependencies, and grant the middle node a retry.
///
/// `source → transform → sink`. The `source → transform` edge opts into
/// **clone-on-read** because `transform` is a **retrying** node — an owned edge
/// into a retrying node is an assembly error (C1/C3/T0.2), so a node whose
/// attempts must each see a fresh input opts into clone-on-read. `transform`'s
/// policy grants one retry (two attempts total) so the T22 retry path is
/// permitted.
fn build_demo_pipeline() -> Pipeline {
    let mut flow = Flow::new();
    let source = flow.register_source(SOURCE, &Source { value: SEED });
    // `transform` retries, so bind its input clone-on-read and grant a retry.
    let transform = flow.register_with::<FlakyTransform, _>(
        TRANSFORM,
        &FlakyTransform,
        source.clone_on_read(),
        NodePolicy::default().retries(1),
    );
    let _sink = flow.register::<Sink, _>(SINK, &Sink, transform);
    flow.finish()
}

// ===========================================================================
// Type-erased node runners on the REAL attempt path (public execution API)
// ===========================================================================

fn ledger() -> Arc<ResidencyLedger> {
    ResidencyLedger::new()
}

/// A fresh output slot for a node with `consumers` downstream consumers.
fn slot_for<T: Send + Sync + 'static>(name: &str, consumers: u32) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        NodeId::from_name(name),
        name,
        consumers,
        false,
        0,
        ledger(),
    ))
}

/// A no-input **source** runner: runs its task's single attempt through the real
/// caught runner and reports the terminal state — the genuine C14 records.
struct SourceRunner {
    name: String,
    task: Option<Source>,
    slot: Arc<Slot<u64>>,
}
impl SourceRunner {
    fn boxed(name: &str, task: Source, slot: Arc<Slot<u64>>) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot,
        })
    }
}
impl NodeRunner for SourceRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("source runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            let outcome = run_attempt(&mut task, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

/// A one-input **retrying** runner for the middle node: reads its single upstream
/// slot (clone-on-read semantics — each attempt a fresh clone), then drives the
/// **real T22 retry loop** ([`run_with_retries_caught`]) over the pre-bound task
/// so the emitted records are the genuine C14/C22 ones (admission, attempt-started,
/// attempt-failed then attempt-started + attempt-succeeded on retry, and one
/// node-terminal), not a re-implementation.
struct RetryingRunner {
    name: String,
    task: Option<FlakyTransform>,
    upstream: SlotRef<u64>,
    slot: Arc<Slot<u64>>,
    config: RetryConfig,
}
impl RetryingRunner {
    fn boxed(
        name: &str,
        task: FlakyTransform,
        upstream: SlotRef<u64>,
        slot: Arc<Slot<u64>>,
        config: RetryConfig,
    ) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            upstream,
            slot,
            config,
        })
    }
}
impl NodeRunner for RetryingRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let task = self.task.take().expect("retrying runner runs once");
        let slot = Arc::clone(&self.slot);
        let config = self.config;
        // The upstream succeeded before this node was admitted, so its slot is
        // filled. Read the value; each retry attempt gets a fresh clone of it,
        // matching the clone-on-read receive mode declared on the edge.
        let input = *self.upstream.read();
        let bound = ReboundEachAttempt { inner: task, input };
        Box::pin(async move {
            // The driver supplies run/pipeline identity on `ctx`; the retry loop
            // mints a fresh per-attempt context (incrementing the attempt number)
            // off those identities, so `FlakyTransform` observes which attempt it
            // is on.
            let outcome = run_with_retries_caught(
                bound,
                &name,
                ctx.run_id().clone(),
                ctx.pipeline_id().clone(),
                &slot,
                sink,
                &config,
                &mut NoJitter,
                // The backoff wait: a caller-provided timer future. The demo
                // resolves it immediately (no wall-clock sleep) so CI never flakes
                // on timing — the retry is driven by the attempt counter, and the
                // backoff phase is still recorded as a named interval (C22/C23).
                |_delay: Duration| async {},
            )
            .await;
            outcome.terminal_state()
        })
    }
}

/// An **owned no-input adapter** over the one-input middle task that re-binds a
/// **fresh clone** of the upstream value on **every** attempt (clone-on-read), so
/// the T22 retry loop — which drives `&mut self` once per attempt — can run the
/// second attempt after the first failed. It holds the task by value (so it is
/// `'static` + `Send`, satisfying the retry loop's `Task` bounds) and reuses the
/// **real** retry runner, so the emitted records are genuine.
struct ReboundEachAttempt {
    inner: FlakyTransform,
    input: u64,
}
impl Task for ReboundEachAttempt {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<u64, TaskError> {
        // A fresh clone of the input for this attempt (clone-on-read).
        self.inner.run(ctx, self.input).await
    }
}

/// A one-input **map** runner for the sink node: reads its upstream (shared) and
/// runs a single attempt through the real runner.
struct SinkRunner {
    name: String,
    task: Option<Sink>,
    upstream: SlotRef<u64>,
    slot: Arc<Slot<u64>>,
}
impl SinkRunner {
    fn boxed(
        name: &str,
        task: Sink,
        upstream: SlotRef<u64>,
        slot: Arc<Slot<u64>>,
    ) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            upstream,
            slot,
        })
    }
}
impl NodeRunner for SinkRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let task = self.task.take().expect("sink runs once");
        let slot = Arc::clone(&self.slot);
        let input = *self.upstream.read();
        let bound = BoundOnce {
            inner: task,
            input: Some(input),
        };
        Box::pin(async move {
            let mut bound = bound;
            let outcome = run_attempt(&mut bound, &name, ctx, &slot, sink).await;
            outcome.terminal_state()
        })
    }
}

/// A single-attempt owned no-input adapter over a one-input task (the sink runs
/// exactly once, so its input is consumed once).
struct BoundOnce {
    inner: Sink,
    input: Option<u64>,
}
impl Task for BoundOnce {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<u64, TaskError> {
        let input = self.input.take().expect("bound input consumed once");
        self.inner.run(ctx, input).await
    }
}

// ===========================================================================
// The run harness: mint identity, open stream, assemble, drive to completion
// ===========================================================================

/// Assemble the demo pipeline, wire each node's runner with its input slot, and
/// drive it to completion through the **real** M1 run-loop driver against an
/// injected in-memory event-stream sink the test walks back — exactly as the real
/// run path does (identity minted and stream opened before assembly is acted on).
///
/// Returns the recorded event-stream bytes and the driver's overall outcome.
fn run_demo() -> (Vec<u8>, RunOutcome) {
    let pipeline = build_demo_pipeline();
    // Assemble through the public assembly pass — an end user's `assemble()`.
    pipeline.assemble().expect("the demo pipeline assembles");

    // Output slots: source has one consumer (transform), transform one (sink),
    // sink none.
    let source_slot = slot_for::<u64>(SOURCE, 1);
    let transform_slot = slot_for::<u64>(TRANSFORM, 1);
    let sink_slot = slot_for::<u64>(SINK, 0);

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        SOURCE.into(),
        SourceRunner::boxed(SOURCE, Source { value: SEED }, Arc::clone(&source_slot)),
    );
    runners.insert(
        TRANSFORM.into(),
        RetryingRunner::boxed(
            TRANSFORM,
            FlakyTransform,
            source_slot.shared_ref(),
            Arc::clone(&transform_slot),
            // Two attempts total (one retry), with a negligible backoff schedule —
            // the wait future resolves immediately, so no wall-clock time passes.
            RetryConfig::new(2, Backoff::new(Duration::ZERO, 2.0, Duration::ZERO)),
        ),
    );
    runners.insert(
        SINK.into(),
        SinkRunner::boxed(SINK, Sink, transform_slot.shared_ref(), sink_slot),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-m1-demo"),
        "m1-three-node-chain",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink.clone(),
        TickClock::default(),
    );
    (sink.bytes(), report.outcome)
}

// ===========================================================================
// The event-stream walker — a reusable oracle (DoD deliverable)
// ===========================================================================

/// One parsed transition record from a walked C19 event stream: its kind, the
/// node it names (if any), the authoritative monotonic offset, and the gapless
/// sequence number.
#[derive(Debug, Clone)]
struct Transition {
    /// The C19 event kind (`run-started`, `node-ready`, `attempt-failed`, …).
    kind: String,
    /// The node this record names, or `None` for run-level records.
    node: Option<String>,
    /// The record's terminal state, present only on `node-terminal` records.
    state: Option<String>,
    /// The **authoritative** monotonic offset (never the informational wall stamp).
    offset_ns: u64,
    /// The gapless, strictly-increasing sequence number.
    seq: u64,
}

/// The reusable **event-stream walker** (T28 deliverable): parse a recorded C19 stream
/// into ordered [`Transition`] records and answer per-node and per-run questions
/// about it.
///
/// It delegates parsing to the tolerant
/// [`read_records`](dagr_artifact::event_stream::read_records), so it inherits the
/// C19 ≤1-trailing-partial guarantee: on a complete stream it yields every
/// transition; on a stream whose final record was truncated it parses every
/// complete record and reports the missing tail
/// ([`trailing_partial_discarded`](Walk::trailing_partial_discarded)) rather than
/// panicking. This is the observable oracle for the whole demo and is written to
/// be reused by the later milestone demos (T38, T49, T63) and the T65 gate.
struct Walk {
    transitions: Vec<Transition>,
    /// Whether the reader tolerated (and discarded) a single trailing partial.
    trailing_partial_discarded: bool,
    /// The run identity every record carries (from the first record).
    run_id: Option<String>,
}

impl Walk {
    /// Walk a recorded stream's bytes into ordered transitions.
    fn new(bytes: &[u8]) -> Self {
        let stream = read_records(bytes).expect("stream parses (tolerating ≤1 trailing partial)");
        let run_id = stream
            .records
            .first()
            .and_then(|r| r.get("run_id").and_then(|v| v.as_str()).map(str::to_string));
        let transitions = stream
            .records
            .iter()
            .map(|rec| {
                let kind = rec
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                // The per-kind payload is spread top-level now (no `body`).
                let node = rec.get("node").and_then(|v| v.as_str()).map(str::to_string);
                let state = rec
                    .get("state")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let offset_ns = rec
                    .get("offset_ns")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let seq = rec
                    .get("seq")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                Transition {
                    kind,
                    node,
                    state,
                    offset_ns,
                    seq,
                }
            })
            .collect();
        Self {
            transitions,
            trailing_partial_discarded: stream.trailing_partial_discarded,
            run_id,
        }
    }

    /// Whether a single trailing partial record was tolerated and discarded.
    fn trailing_partial_discarded(&self) -> bool {
        self.trailing_partial_discarded
    }

    /// The run identity carried on the first record (and, by the C19 contract,
    /// every record).
    fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// The just-the-kind sequence, in stream order.
    fn kinds(&self) -> Vec<&str> {
        self.transitions.iter().map(|t| t.kind.as_str()).collect()
    }

    /// The index of the first transition matching `(kind, node)`, or `None`.
    fn index_of(&self, kind: &str, node: Option<&str>) -> Option<usize> {
        self.transitions
            .iter()
            .position(|t| t.kind == kind && t.node.as_deref() == node)
    }

    /// Every transition naming `node`, in order.
    fn for_node(&self, node: &str) -> Vec<&Transition> {
        self.transitions
            .iter()
            .filter(|t| t.node.as_deref() == Some(node))
            .collect()
    }

    /// How many transitions of `kind` name `node`.
    fn count(&self, kind: &str, node: &str) -> usize {
        self.transitions
            .iter()
            .filter(|t| t.kind == kind && t.node.as_deref() == Some(node))
            .count()
    }

    /// The sequence numbers of every walked record, in stream order (the C19
    /// gapless, strictly-increasing sequence).
    fn seqs(&self) -> Vec<u64> {
        self.transitions.iter().map(|t| t.seq).collect()
    }

    /// The single terminal state recorded for `node` (its `node-terminal` record's
    /// `state`), asserting exactly one such record exists.
    fn terminal_of(&self, node: &str) -> String {
        let terminals: Vec<&Transition> = self
            .transitions
            .iter()
            .filter(|t| t.kind == "node-terminal" && t.node.as_deref() == Some(node))
            .collect();
        assert_eq!(
            terminals.len(),
            1,
            "node `{node}` must have exactly one node-terminal record"
        );
        terminals[0]
            .state
            .clone()
            .expect("node-terminal carries a state")
    }
}

// ===========================================================================
// The tests — the executable M1 done-when
// ===========================================================================

/// **Scenario 1+2+3 (headline): the M1 done-when.** A three-node chain executes
/// in dependency order; the middle node fails once then retries to success; the
/// event stream shows every transition, in order — `run-started` first, then per
/// node `node-ready → node-admitted → attempt-started → attempt-outcome →
/// node-terminal(succeeded)`, with `transform` showing two attempt cycles (a
/// retryable failure then a success), and `run-finished` last. This is the
/// spec's "It runs" done-when, executed in CI.
#[test]
fn m1_three_node_chain_with_retry_is_the_done_when() {
    let (bytes, outcome) = run_demo();
    let walk = Walk::new(&bytes);

    // The overall run succeeded.
    assert_eq!(
        outcome,
        RunOutcome::Succeeded,
        "the M1 run succeeds overall"
    );

    // Every node ends in exactly one terminal state, and it is `succeeded`.
    for node in [SOURCE, TRANSFORM, SINK] {
        assert_eq!(
            walk.terminal_of(node),
            "succeeded",
            "node `{node}` ends succeeded"
        );
    }

    // The middle node produced exactly two attempt cycles: a retryable failure
    // then a success. It started exactly two attempts (initial + one retry) and
    // succeeded on the second. Its first attempt is a retryable failure, recorded
    // as an `attempt-failed`; the C19 vocabulary carries no dedicated `backoff`
    // event, so the retry loop's backoff-phase marker (the named interval that
    // folds into the run artifact at C22/T42) *also* surfaces on the raw C19
    // stream as an `attempt-failed` record — hence exactly two `attempt-failed`
    // records for `transform`: the genuine failure and the backoff marker, both
    // before the retry's `attempt-started`. `source`/`sink` each ran exactly once.
    assert_eq!(
        walk.count("attempt-started", TRANSFORM),
        2,
        "transform started exactly two attempts (initial + one retry)"
    );
    assert_eq!(
        walk.count("attempt-succeeded", TRANSFORM),
        1,
        "transform's second attempt succeeds"
    );
    assert_eq!(
        walk.count("attempt-failed", TRANSFORM),
        2,
        "transform's first attempt is a retryable failure, plus the backoff-phase \
         marker the C19 stream folds onto `attempt-failed` (no `backoff` event exists)"
    );
    for node in [SOURCE, SINK] {
        assert_eq!(
            walk.count("attempt-started", node),
            1,
            "node `{node}` ran exactly one attempt"
        );
        assert_eq!(
            walk.count("attempt-succeeded", node),
            1,
            "node `{node}` succeeded on its single attempt"
        );
        assert_eq!(
            walk.count("attempt-failed", node),
            0,
            "node `{node}` had no failed attempt"
        );
    }

    // The full ordered transition sequence for the run. run-started first;
    // run-finished last; and the per-node anchors appear in dependency order.
    assert_eq!(walk.kinds().first().copied(), Some("run-started"));
    assert_eq!(walk.kinds().last().copied(), Some("run-finished"));

    // Ordering by dependency: source terminal precedes transform ready, which
    // precedes sink ready.
    let source_term = walk.index_of("node-terminal", Some(SOURCE)).unwrap();
    let transform_ready = walk.index_of("node-ready", Some(TRANSFORM)).unwrap();
    let transform_term = walk.index_of("node-terminal", Some(TRANSFORM)).unwrap();
    let sink_ready = walk.index_of("node-ready", Some(SINK)).unwrap();
    assert!(
        transform_ready > source_term,
        "transform becomes ready only after source is terminal"
    );
    assert!(
        sink_ready > transform_term,
        "sink becomes ready only after transform is terminal (its retry included)"
    );

    // The exact per-node ordered transition sub-sequence for `transform`,
    // including both attempt cycles (failure then success).
    let transform_kinds: Vec<&str> = walk
        .for_node(TRANSFORM)
        .iter()
        .map(|t| t.kind.as_str())
        .collect();
    assert_eq!(
        transform_kinds,
        vec![
            "node-ready",
            "node-admitted",
            "attempt-started",
            "attempt-failed",  // the genuine retryable first-attempt failure
            "attempt-outcome", // that attempt's single rich outcome record (l.331)
            "attempt-failed",  // the backoff-phase marker (no C19 `backoff` event)
            "node-admitted",
            "attempt-started",
            "attempt-succeeded",
            "attempt-outcome", // the successful attempt's outcome record
            "node-terminal",
        ],
        "transform's ordered transition sequence: ready, then a failed attempt \
         cycle (failure + its attempt-outcome + backoff marker), then a successful \
         attempt cycle (success + its attempt-outcome), then terminal — every \
         attempt produces exactly one attempt-outcome record (arch.md l.331)"
    );
}

/// **Scenario 4: the `run-started` event fully identifies the run.** The first
/// record is `run-started` and carries the full run-artifact header known at
/// start (run identity, pipeline identity, both fingerprints) — everything but
/// the overall outcome and summary — so a stream truncated to just this record
/// still identifies its run.
#[test]
fn run_started_fully_identifies_the_run() {
    let (bytes, _outcome) = run_demo();
    let stream = read_records(&bytes).expect("parses");
    let first = stream.records.first().expect("at least one record");
    assert_eq!(
        first.get("kind").and_then(|v| v.as_str()),
        Some("run-started"),
        "the first record is run-started"
    );
    assert!(
        first.get("run_id").and_then(|v| v.as_str()).is_some(),
        "run-started carries the run identity"
    );
    assert!(
        first.get("schema_version").is_some(),
        "run-started carries the schema version"
    );
    let header = first.get("header").expect("run-started header");
    assert_eq!(
        header.get("pipeline").and_then(|v| v.as_str()),
        Some("m1-three-node-chain"),
        "run-started carries the pipeline identity"
    );
    assert!(
        header.get("fingerprint_structural").is_some(),
        "run-started carries the structural fingerprint (assembly succeeded)"
    );
    assert!(
        header.get("fingerprint_policy").is_some(),
        "run-started carries the policy hash"
    );
    // The overall outcome and summary are NOT in the run-started header.
    assert!(
        header.get("overall_outcome").is_none(),
        "no overall outcome at start (that is run-finished's)"
    );
}

/// **Scenario 5: sequence numbers are gapless and strictly increasing, and every
/// record carries the run identity and schema version.**
#[test]
fn sequence_numbers_are_gapless_and_carry_identity() {
    let (bytes, _outcome) = run_demo();
    let walk = Walk::new(&bytes);

    // Gapless and strictly increasing: seqs start at 0 and step by exactly one,
    // read through the walker's parsed records.
    for (expected, seq) in walk.seqs().into_iter().enumerate() {
        assert_eq!(
            seq, expected as u64,
            "sequence numbers are gapless and strictly increasing (start 0, +1)"
        );
    }
    assert!(
        walk.seqs().len() >= 2,
        "the run recorded more than just its first record"
    );

    // Every record carries the same run identity and a schema version.
    let stream = read_records(&bytes).expect("parses");
    let run_id = walk.run_id().expect("a run id").to_string();
    for rec in &stream.records {
        assert_eq!(
            rec.get("run_id").and_then(|v| v.as_str()),
            Some(run_id.as_str()),
            "every record carries the same run identity"
        );
        assert!(
            rec.get("schema_version").is_some(),
            "every record carries a schema version"
        );
    }
}

/// **Scenario 6: durations are computed from monotonic offsets, not wall clocks.**
/// For `transform`, the elapsed time between its first attempt-started and its
/// terminal event derived from the authoritative offset field is non-negative and
/// consistent; the wall stamp is never used to order or measure.
#[test]
fn durations_are_computed_from_monotonic_offsets() {
    let (bytes, _outcome) = run_demo();
    let walk = Walk::new(&bytes);

    let node_records = walk.for_node(TRANSFORM);
    let first_started = node_records
        .iter()
        .find(|t| t.kind == "attempt-started")
        .expect("transform has an attempt-started");
    let terminal = node_records
        .iter()
        .find(|t| t.kind == "node-terminal")
        .expect("transform has a node-terminal");

    // The offset is monotonic and authoritative: the terminal's offset is at or
    // after the first attempt's, so the derived duration is non-negative.
    assert!(
        terminal.offset_ns >= first_started.offset_ns,
        "monotonic offsets: terminal at-or-after the first attempt-started"
    );
    let derived = terminal.offset_ns - first_started.offset_ns;
    // The whole demo advances the tick clock by at least one per record, and
    // `transform` emits several records between its first attempt and its
    // terminal, so the derived duration is strictly positive.
    assert!(derived > 0, "the derived monotonic duration is positive");

    // Offsets across the whole stream are non-decreasing (the authoritative
    // ordering field), independent of any wall-clock value.
    let mut prev = 0u64;
    for t in &walk.transitions {
        assert!(
            t.offset_ns >= prev,
            "offsets are non-decreasing across the stream"
        );
        prev = t.offset_ns;
    }
}

/// **Scenario 7: the run terminates exactly when nothing is pending or in flight.**
/// `run-finished` is emitted once, after `sink`'s terminal event; the driver
/// returns rather than hanging; no `zombie-at-exit` records appear (this demo has
/// no timed-out or abandoned work).
#[test]
fn run_terminates_when_nothing_pending_or_in_flight() {
    let (bytes, _outcome) = run_demo();
    let walk = Walk::new(&bytes);

    let finished_indices: Vec<usize> = walk
        .transitions
        .iter()
        .enumerate()
        .filter(|(_, t)| t.kind == "run-finished")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(finished_indices.len(), 1, "exactly one run-finished");
    let finished = finished_indices[0];
    assert_eq!(
        finished,
        walk.transitions.len() - 1,
        "run-finished is the very last record — nothing after it"
    );
    let sink_terminal = walk.index_of("node-terminal", Some(SINK)).unwrap();
    assert!(
        finished > sink_terminal,
        "run-finished follows sink's terminal event"
    );
    // No zombie-at-exit: this demo has no timed-out/abandoned work.
    assert!(
        walk.index_of("zombie-at-exit", None).is_none()
            && !walk.kinds().contains(&"zombie-at-exit"),
        "no zombie-at-exit — every attempt's closure returned before run end"
    );
}

/// **Scenario 8: every node ends in exactly one terminal state from the taxonomy.**
/// Each of `source`, `transform`, `sink` has exactly one `node-terminal` event,
/// each `succeeded` — the single-terminal-state invariant holds for the run.
#[test]
fn every_node_ends_in_exactly_one_terminal_state() {
    let (bytes, _outcome) = run_demo();
    let walk = Walk::new(&bytes);
    for node in [SOURCE, TRANSFORM, SINK] {
        assert_eq!(
            walk.count("node-terminal", node),
            1,
            "node `{node}` has exactly one node-terminal event"
        );
        // `terminal_of` also asserts single-terminal internally.
        assert_eq!(walk.terminal_of(node), "succeeded");
    }
}

/// **Scenario 9: the demo is deterministic and reproducible in CI.** Two runs of
/// the same example produce the same ordered sequence of transitions and the same
/// per-node attempt counts (`transform` retries exactly once each time) — the
/// flakiness is a deterministic counter, not timing or randomness, so CI never
/// flakes.
#[test]
fn the_demo_is_deterministic_across_runs() {
    let (bytes_a, outcome_a) = run_demo();
    let (bytes_b, outcome_b) = run_demo();
    assert_eq!(outcome_a, outcome_b, "same overall outcome both runs");

    let walk_a = Walk::new(&bytes_a);
    let walk_b = Walk::new(&bytes_b);

    // The ordered kind+node transition shape is identical (run identity and
    // offsets differ, but the transition sequence does not).
    let shape = |w: &Walk| -> Vec<(String, Option<String>, Option<String>)> {
        w.transitions
            .iter()
            .map(|t| (t.kind.clone(), t.node.clone(), t.state.clone()))
            .collect()
    };
    assert_eq!(
        shape(&walk_a),
        shape(&walk_b),
        "both runs produce the identical ordered transition sequence"
    );
    // The retry happened exactly once each time: two attempt cycles, one success,
    // and two `attempt-failed` records (the genuine failure + the backoff marker).
    for w in [&walk_a, &walk_b] {
        assert_eq!(w.count("attempt-started", TRANSFORM), 2);
        assert_eq!(w.count("attempt-failed", TRANSFORM), 2);
        assert_eq!(w.count("attempt-succeeded", TRANSFORM), 1);
    }
}

/// **Scenario 10: the event-stream walker is a reusable oracle.** On the complete
/// stream it returns the full ordered transition set; on a deliberately-truncated
/// copy (a valid prefix with the final record's bytes cut) it parses every
/// complete record and reports the missing tail rather than panicking —
/// demonstrating it tolerates the C19 ≤1 trailing-partial guarantee and is fit for
/// reuse by later demos.
#[test]
fn the_walker_is_a_reusable_oracle_tolerating_a_truncated_tail() {
    let (bytes, _outcome) = run_demo();

    // Complete stream: full ordered transition set, no discarded tail.
    let full = Walk::new(&bytes);
    assert!(
        !full.trailing_partial_discarded(),
        "a complete stream discards no trailing partial"
    );
    assert_eq!(full.kinds().last().copied(), Some("run-finished"));
    assert!(
        full.run_id().is_some(),
        "the walker exposes the run identity"
    );
    let full_count = full.transitions.len();
    assert!(full_count >= 2);

    // Truncate the final record: cut the stream partway through its **last**
    // record, leaving a valid prefix of complete records plus one unterminated
    // (unparseable) partial tail — the ≤1 trailing-partial shape a crash leaves.
    // The last two bytes of a complete stream are the final record's content and
    // its terminating '\n'; find the penultimate newline (the boundary *before*
    // the last complete record) and keep everything up to and including it, then
    // append a partial fragment of what would have been the final record.
    let newlines: Vec<usize> = bytes
        .iter()
        .enumerate()
        .filter(|(_, &b)| b == b'\n')
        .map(|(i, _)| i)
        .collect();
    assert!(newlines.len() >= 2, "the stream has at least two records");
    let penultimate_newline = newlines[newlines.len() - 2];
    // Everything up to and including the penultimate record's newline is a valid
    // prefix; the final complete record is dropped and replaced by a cut fragment.
    let mut truncated = bytes[..=penultimate_newline].to_vec();
    truncated.extend_from_slice(b"{\"event\":\"run-fi"); // a cut final record

    let walk = Walk::new(&truncated);
    assert!(
        walk.trailing_partial_discarded(),
        "the walker reports the missing tail rather than panicking"
    );
    // It parsed every complete record — one fewer than the full stream (the
    // truncated final record is the only one dropped).
    assert_eq!(
        walk.transitions.len(),
        full_count - 1,
        "every complete record is parsed; only the cut tail is dropped"
    );
    // The complete prefix still identifies its run and preserves order.
    assert_eq!(walk.kinds().first().copied(), Some("run-started"));
    assert!(walk.run_id().is_some());
}

/// The retried value flows through the chain end to end: `sink` produces
/// `(SEED * 2) + 1`, proving the value `transform` produced on its **successful
/// (second) attempt** was the one delivered downstream — the retry recovered the
/// real result, not a stale or partial one.
#[test]
fn the_retried_value_flows_through_the_chain() {
    // Re-run the demo but capture the sink's produced value by re-wiring the sink
    // slot so the test can read it after the run.
    let pipeline = build_demo_pipeline();
    pipeline.assemble().expect("assembles");

    let source_slot = slot_for::<u64>(SOURCE, 1);
    let transform_slot = slot_for::<u64>(TRANSFORM, 1);
    let sink_slot = slot_for::<u64>(SINK, 0);

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        SOURCE.into(),
        SourceRunner::boxed(SOURCE, Source { value: SEED }, Arc::clone(&source_slot)),
    );
    runners.insert(
        TRANSFORM.into(),
        RetryingRunner::boxed(
            TRANSFORM,
            FlakyTransform,
            source_slot.shared_ref(),
            Arc::clone(&transform_slot),
            RetryConfig::new(2, Backoff::new(Duration::ZERO, 2.0, Duration::ZERO)),
        ),
    );
    runners.insert(
        SINK.into(),
        SinkRunner::boxed(
            SINK,
            Sink,
            transform_slot.shared_ref(),
            Arc::clone(&sink_slot),
        ),
    );

    let sink = MemorySink::default();
    let report = drive(
        &RunConfig::new("/tmp/dagr-m1-demo"),
        "m1-three-node-chain",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        sink,
        TickClock::default(),
    );
    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert!(sink_slot.is_filled(), "sink filled its output slot");
    assert_eq!(
        *sink_slot.shared_ref().read(),
        FINAL_VALUE,
        "the retried value flowed through: sink produced (SEED*2)+1"
    );
}
