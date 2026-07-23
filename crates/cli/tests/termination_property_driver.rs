//! C11 · termination property — the **driver-level** half, ticket T25 (035).
//!
//! The deep, high-case-count termination property lives in
//! `crates/core/tests/termination_property.rs`, driving the pure C11 tracker
//! directly (deterministic, fast, no runtime). This companion suite closes the
//! T25 definition-of-done requirement that each case also runs through the **real
//! T24 run loop against fakes** — `dagr_cli::driver::drive` — so the full
//! admit/spawn/feed-back loop (two tokio runtimes, the event-stream writer, the
//! bounded zombie wait) is proven to terminate on random shapes too, not just the
//! tracker in isolation.
//!
//! Because spinning two multithreaded tokio runtimes per case is far heavier than
//! stepping the pure tracker, this suite uses a **small, fixed** case count (the
//! tracker suite carries the meaningful volume). Each generated DAG is built
//! through the same real typed `Flow` builder, every node scripted to its assigned
//! outcome via a fake [`NodeRunner`], and admission is capacity-pinned by
//! construction — the M1 driver admits every ready node (no pool, C31 is T31), so
//! results never depend on host resources. The assertions: the drive **returns**
//! (does not hang — a hang is the deadlock this property forbids), the event
//! stream ends with exactly one `run-finished`, and every node records exactly one
//! terminal state drawn from the normative taxonomy.

use std::collections::{BTreeMap, BTreeSet};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::AttemptEventSink;
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::handle::Handle;
use dagr_core::task::Task;
use dagr_core::TaskError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ===========================================================================
// A capturing in-memory sink + monotonic clock (C19 injection seam).
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
// The seeded generator (same shape rules as the tracker suite, kept local so the
// two test binaries stay independent).
// ===========================================================================

struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next_u64() % (n as u64)).unwrap_or(0)
    }
    fn range_inclusive(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.below(hi - lo + 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Succeeded,
    Failed,
    Skipped,
    TimedOut,
}
impl Outcome {
    fn terminal(self) -> TerminalState {
        match self {
            Outcome::Succeeded => TerminalState::Succeeded,
            Outcome::Failed => TerminalState::Failed,
            Outcome::Skipped => TerminalState::Skipped,
            Outcome::TimedOut => TerminalState::TimedOut,
        }
    }
}

#[derive(Debug, Clone)]
struct Case {
    upstreams: Vec<Vec<usize>>,
    outcomes: Vec<Outcome>,
}
impl Case {
    fn node_count(&self) -> usize {
        self.upstreams.len()
    }
    fn name(i: usize) -> String {
        format!("n{i:03}")
    }
}

fn generate(seed: u64) -> Case {
    const MAX_NODES: usize = 10;
    const MAX_ARITY: usize = 8;
    let mut rng = SplitMix64::new(seed);
    let n = rng.range_inclusive(1, MAX_NODES);
    let mut upstreams: Vec<Vec<usize>> = Vec::with_capacity(n);
    let mut outcomes: Vec<Outcome> = Vec::with_capacity(n);
    for i in 0..n {
        let max_up = i.min(MAX_ARITY);
        let want = if max_up == 0 {
            0
        } else {
            rng.below(max_up + 1)
        };
        let mut chosen: BTreeSet<usize> = BTreeSet::new();
        let mut guard = 0;
        while chosen.len() < want && guard < 64 {
            chosen.insert(rng.below(i));
            guard += 1;
        }
        upstreams.push(chosen.into_iter().collect());
        outcomes.push(match rng.below(9) {
            0..=3 => Outcome::Succeeded,
            4..=5 => Outcome::Failed,
            6..=7 => Outcome::Skipped,
            _ => Outcome::TimedOut,
        });
    }
    Case {
        upstreams,
        outcomes,
    }
}

// ===========================================================================
// Real pipeline (single u64 value type, fixed-arity joins, shared edges).
// ===========================================================================

struct Source;
impl Task for Source {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(0)
    }
}
macro_rules! join_task {
    ($name:ident, $input:ty) => {
        struct $name;
        impl Task for $name {
            type Input = $input;
            type Output = u64;
            async fn run(&mut self, _c: &RunContext, _i: $input) -> Result<u64, TaskError> {
                Ok(0)
            }
        }
    };
}
join_task!(Join1, u64);
join_task!(Join2, (u64, u64));
join_task!(Join3, (u64, u64, u64));
join_task!(Join4, (u64, u64, u64, u64));
join_task!(Join5, (u64, u64, u64, u64, u64));
join_task!(Join6, (u64, u64, u64, u64, u64, u64));
join_task!(Join7, (u64, u64, u64, u64, u64, u64, u64));
join_task!(Join8, (u64, u64, u64, u64, u64, u64, u64, u64));

fn build_pipeline(case: &Case) -> Pipeline {
    let mut flow = Flow::new();
    let mut h: Vec<Handle<u64>> = Vec::with_capacity(case.node_count());
    for i in 0..case.node_count() {
        let name = Case::name(i);
        let u = &case.upstreams[i];
        let handle: Handle<u64> = match u.len() {
            0 => flow.register_source(name, &Source),
            1 => flow.register::<Join1, _>(name, &Join1, h[u[0]].shared()),
            2 => flow.register::<Join2, _>(name, &Join2, (h[u[0]].shared(), h[u[1]].shared())),
            3 => flow.register::<Join3, _>(
                name,
                &Join3,
                (h[u[0]].shared(), h[u[1]].shared(), h[u[2]].shared()),
            ),
            4 => flow.register::<Join4, _>(
                name,
                &Join4,
                (
                    h[u[0]].shared(),
                    h[u[1]].shared(),
                    h[u[2]].shared(),
                    h[u[3]].shared(),
                ),
            ),
            5 => flow.register::<Join5, _>(
                name,
                &Join5,
                (
                    h[u[0]].shared(),
                    h[u[1]].shared(),
                    h[u[2]].shared(),
                    h[u[3]].shared(),
                    h[u[4]].shared(),
                ),
            ),
            6 => flow.register::<Join6, _>(
                name,
                &Join6,
                (
                    h[u[0]].shared(),
                    h[u[1]].shared(),
                    h[u[2]].shared(),
                    h[u[3]].shared(),
                    h[u[4]].shared(),
                    h[u[5]].shared(),
                ),
            ),
            7 => flow.register::<Join7, _>(
                name,
                &Join7,
                (
                    h[u[0]].shared(),
                    h[u[1]].shared(),
                    h[u[2]].shared(),
                    h[u[3]].shared(),
                    h[u[4]].shared(),
                    h[u[5]].shared(),
                    h[u[6]].shared(),
                ),
            ),
            8 => flow.register::<Join8, _>(
                name,
                &Join8,
                (
                    h[u[0]].shared(),
                    h[u[1]].shared(),
                    h[u[2]].shared(),
                    h[u[3]].shared(),
                    h[u[4]].shared(),
                    h[u[5]].shared(),
                    h[u[6]].shared(),
                    h[u[7]].shared(),
                ),
            ),
            other => unreachable!("arity bounded to 8, got {other}"),
        };
        h.push(handle);
    }
    flow.finish()
}

// ===========================================================================
// A fake, type-erased runner that scripts a node to its assigned outcome.
// ===========================================================================
//
// The tracker admits a node only once its upstreams satisfied its `all-succeeded`
// rule, so a fake runner that simply returns the scripted terminal is a faithful
// fake (the C28 direction): it exercises the real driver loop (admission, spawn,
// mpsc feed-back, writer drain, run-end condition) without needing real task
// bodies or slot reads. Data-consuming nodes only run when every upstream
// succeeded, so no scripted outcome contradicts the graph invariant.

struct ScriptedRunner {
    name: String,
    state: TerminalState,
}
impl ScriptedRunner {
    fn boxed(name: &str, state: TerminalState) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            state,
        })
    }
}
impl NodeRunner for ScriptedRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        _ctx: &'a RunContext,
        _sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let state = self.state;
        Box::pin(async move { state })
    }
}

fn build_plan(case: &Case) -> (Pipeline, RunPlan) {
    let pipeline = build_pipeline(case);
    pipeline.assemble().expect("generated pipeline assembles");
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    for i in 0..case.node_count() {
        let name = Case::name(i);
        runners.insert(
            name.clone(),
            ScriptedRunner::boxed(&name, case.outcomes[i].terminal()),
        );
    }
    (pipeline.clone(), RunPlan::new(pipeline, runners))
}

// ===========================================================================
// Stream helpers.
// ===========================================================================

fn count_run_finished(bytes: &[u8]) -> usize {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .filter(|r| r.get("event").and_then(|v| v.as_str()) == Some("run-finished"))
        .count()
}

fn last_event(bytes: &[u8]) -> Option<String> {
    let stream = dagr_artifact::event_stream::read_records(bytes).expect("stream parses");
    stream
        .records
        .last()
        .and_then(|r| r.get("event").and_then(|v| v.as_str()).map(str::to_string))
}

fn is_taxonomy(state: TerminalState) -> bool {
    matches!(
        state,
        TerminalState::Succeeded
            | TerminalState::Failed
            | TerminalState::TimedOut
            | TerminalState::Skipped
            | TerminalState::UpstreamSkipped
            | TerminalState::UpstreamFailed
            | TerminalState::Cancelled
            | TerminalState::Abandoned
            | TerminalState::SatisfiedFromPrior
    )
}

// ===========================================================================
// The driver-level termination sweep.
// ===========================================================================

/// Over a small fixed set of random DAGs, driving each through the **real** T24
/// run loop with fake scripted runners: every `drive` returns (no hang — a hang is
/// the deadlock this forbids), the stream ends with exactly one `run-finished`, and
/// every node records exactly one taxonomy terminal state. (C11 — driven for real.)
#[test]
fn generated_runs_terminate_through_the_real_driver() {
    const BASE_SEED: u64 = 0x5EED_10AD_D46C_11E5;
    // A modest, deterministic budget — each case spins two tokio runtimes, so this
    // is the "quick" companion to the tracker suite's high-volume sweep.
    let cases: u64 = std::env::var("DAGR_TERMINATION_DRIVER_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);

    for idx in 0..cases {
        let seed = BASE_SEED ^ idx.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let case = generate(seed);
        let (_pipeline, plan) = build_plan(&case);
        let sink = MemorySink::default();

        // If the driver deadlocked, `drive` would never return and the test would
        // hang (caught by the harness timeout) — returning at all is half the
        // property. A short grace keeps any timed-out zombie wait bounded.
        let report = drive(
            &RunConfig::new("/tmp/dagr-termination").grace(std::time::Duration::from_millis(50)),
            "termination",
            Ok(plan),
            &[],
            sink.clone(),
            TickClock::default(),
        );

        let bytes = sink.bytes();
        assert_eq!(
            count_run_finished(&bytes),
            1,
            "seed={seed:#x}: exactly one run-finished (the run terminated once)"
        );
        assert_eq!(
            last_event(&bytes).as_deref(),
            Some("run-finished"),
            "seed={seed:#x}: run-finished is the last record"
        );

        // Every node ends in exactly one taxonomy terminal state.
        assert_eq!(
            report.terminal_states.len(),
            case.node_count(),
            "seed={seed:#x}: every node has a recorded terminal state"
        );
        for i in 0..case.node_count() {
            let state = report
                .terminal_states
                .get(&Case::name(i))
                .copied()
                .unwrap_or_else(|| {
                    panic!("seed={seed:#x}: node {} has no terminal", Case::name(i))
                });
            assert!(
                is_taxonomy(state),
                "seed={seed:#x}: node {} off-taxonomy {state:?}",
                Case::name(i)
            );
        }

        // The overall outcome is one of the three run outcomes (never a hang state).
        assert!(
            matches!(
                report.outcome,
                RunOutcome::Succeeded | RunOutcome::Failed | RunOutcome::Cancelled
            ),
            "seed={seed:#x}: a terminating run has a decided outcome, got {:?}",
            report.outcome
        );
    }
}
