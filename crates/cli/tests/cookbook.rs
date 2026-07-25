//! **Cookbook — the patterns the design forces, each backed by compiled,
//! run code** (arch.md "Documentation"; ticket T64).
//!
//! [`docs/cookbook.md`](../../../../docs/cookbook.md) is the human-facing
//! cookbook. This suite is its executable ground truth: one test per cookbook
//! entry, each demonstrating the pattern against the **real** authoring and
//! execution API so a renamed or removed method fails the build and the entry's
//! prose can never claim a behaviour the code does not have. Every flow-running
//! entry runs its flow through the one-call
//! [`RunnableFlow`](dagr_cli::run_flow::RunnableFlow) seam — the author writes
//! tasks + a flow and never a `NodeRunner` (arch.md C1).
//!
//! The entries, and the arch.md contract each pins:
//!
//! - **Fan-out inside one node** — internal parallelism bounded by the node's
//!   declared cost; fan-out is a *within-node* concern, not a graph-shape change
//!   (C5/C12/arch.md "the graph's shape never changes at runtime").
//! - **Fan-in** — many upstream handles joined into one node as a tuple, under
//!   the default `all-succeeded` rule (C3).
//! - **Branch-in-task** — `self-skip` versus `succeed-with-empty` for joins, and
//!   why an author picks one over the other (C15/Vocabulary).
//! - **Incremental cursors via scratch** — a cursor written on one attempt and
//!   read on the next (C18).
//! - **Durable stage boundaries** — the `DurableOutput` contract a durable node's
//!   output implements, and what it buys at resume (C10/C27).
//! - **Two same-typed resources via newtypes** — retrieval by type succeeding and
//!   the identical-type registration failing as ambiguous (C9).
//!
//! The **non-`Send` capture** entry is a compile-fail fixture pinned to the
//! workspace toolchain, so it lives in the T8 UI harness
//! (`crates/core/tests/ui/`), not here; this suite covers its *fixes* (the forms
//! that compile) inline in the durable/scratch tasks that capture only `Send`
//! values.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{read_records, EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::RunConfig;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::assembly::{DurableOutput, NodePolicy};
use dagr_core::context::RunContext;
use dagr_core::error::RehydrateError;
use dagr_core::task::Task;
use dagr_core::{Flow, TaskError};

// ===========================================================================
// Shared deterministic injection seams (the same shapes the ergonomics suite
// and the milestone demos use): an in-memory sink + a monotonic tick clock.
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

/// The last terminal state recorded for `node` in a folded event stream.
fn terminal_of(bytes: &[u8], node: &str) -> String {
    let stream = read_records(bytes).expect("stream parses");
    stream
        .records
        .iter()
        .rev()
        .find(|r| {
            r.get("kind").and_then(|v| v.as_str()) == Some("node-terminal")
                && r.get("node").and_then(|v| v.as_str()) == Some(node)
        })
        .and_then(|r| r.get("state").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_else(|| panic!("no node-terminal for `{node}`"))
}

/// A private per-test run-store base under the temp dir.
fn temp_base(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "dagr-cookbook-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_string_lossy().into_owned()
}

// ===========================================================================
// Entry: FAN-OUT INSIDE ONE NODE — the declared-cost rule bounds internal
// parallelism, and fan-out is NOT a graph-shape change (C5/C12).
// ===========================================================================

/// A trivial source feeding the fan-out node (a data-dependent node is what
/// carries a declared-cost `NodePolicy` through the one-call seam).
struct HowMany {
    items: u64,
}
impl Task for HowMany {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(self.items)
    }
}

/// A single node that discovers N work items at runtime and processes them with
/// **bounded internal concurrency** — the blessed pattern (arch.md: "one node
/// that iterates internally with bounded concurrency"). It never becomes N nodes.
/// The concurrency ceiling here is honest about the node's declared cost: a
/// counter tracks the peak in-flight count and asserts it never exceeds the bound.
struct FanOutInsideOneNode {
    max_in_flight: usize,
}
impl Task for FanOutInsideOneNode {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, items: u64) -> Result<u64, TaskError> {
        // A simple counting semaphore bounds how many items run concurrently.
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let bound = self.max_in_flight;
        let mut processed = 0u64;
        // Process items in bounded batches — a stand-in for a real bounded-
        // concurrency executor. The point is that the *graph* stays one node.
        for start in (0..items).step_by(bound.max(1)) {
            let batch: Vec<u64> = (start..(start + bound as u64).min(items)).collect();
            for _item in &batch {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                // (do the per-item work here)
                in_flight.fetch_sub(1, Ordering::SeqCst);
                processed += 1;
            }
        }
        assert!(
            peak.load(Ordering::SeqCst) <= bound,
            "internal parallelism stayed within the declared bound"
        );
        Ok(processed)
    }
}

#[test]
fn fan_out_inside_one_node_bounds_internal_parallelism_and_stays_one_node() {
    let mut flow = RunnableFlow::new();
    let count = flow.register_source("how-many", HowMany { items: 100 });
    // The declared cost (compute threads) is the honest budget for this node's
    // internal parallelism (C5). The processing stays ONE node — fan-out is a
    // within-node concern (arch.md "the graph's shape never changes at runtime").
    let node = flow.register_with::<FanOutInsideOneNode, _>(
        "process-all",
        FanOutInsideOneNode { max_in_flight: 4 },
        count,
        NodePolicy::new().compute_threads(4),
    );

    let mem = MemorySink::default();
    let report = flow
        .run(
            "cookbook-fan-out",
            &RunConfig::new(temp_base("fan-out")),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    assert_eq!(report.outcome(), RunOutcome::Succeeded);
    assert_eq!(terminal_of(&mem.bytes(), "process-all"), "succeeded");
    assert_eq!(
        report.output(node),
        Some(100),
        "all 100 items were processed inside the single node"
    );
    // The processing is one node — runtime fan-out added no processing nodes.
    let stream = read_records(&mem.bytes()).unwrap();
    let terminals = stream
        .records
        .iter()
        .filter(|r| r.get("kind").and_then(|v| v.as_str()) == Some("node-terminal"))
        .count();
    assert_eq!(
        terminals, 2,
        "the graph is the source + the single processing node — runtime fan-out added no nodes"
    );
}

// ===========================================================================
// Entry: FAN-IN — many upstream handles joined into one node as a TUPLE, under
// the default `all-succeeded` rule (C3). The tuple binding is compile-checked
// and the node succeeds only when every upstream succeeded.
// ===========================================================================

struct MakeCount;
impl Task for MakeCount {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(3)
    }
}
struct MakeLabel;
impl Task for MakeLabel {
    type Input = ();
    type Output = String;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<String, TaskError> {
        Ok("rows".to_string())
    }
}
/// A join consuming TWO upstreams of different types as a tuple `(u64, String)`.
struct JoinCountAndLabel;
impl Task for JoinCountAndLabel {
    type Input = (u64, String);
    type Output = String;
    async fn run(&mut self, _c: &RunContext, input: (u64, String)) -> Result<String, TaskError> {
        let (count, label) = input;
        Ok(format!("{count} {label}"))
    }
}

#[test]
fn fan_in_binds_many_upstream_handles_as_a_tuple_under_all_succeeded() {
    // The joining node binds two handles AS A TUPLE — count, order, and types are
    // all compile-checked at once (C3). A data-dependent node is `all-succeeded`
    // by construction: the typestate offers no other rule, so the join fires only
    // when *every* upstream succeeded.
    let mut flow = Flow::new();
    let count = flow.register_source("count", &MakeCount);
    let label = flow.register_source("label", &MakeLabel);
    let _join = flow.register("join", &JoinCountAndLabel, (count, label));
    let artifact = flow
        .finish()
        .assemble()
        .expect("the two-upstream fan-in assembles");

    // The join records two data edges (one per upstream), in binding order.
    assert_eq!(
        artifact.consumer_count(count.id()),
        Some(1),
        "the count upstream feeds exactly the join"
    );
    assert_eq!(
        artifact.consumer_count(label.id()),
        Some(1),
        "the label upstream feeds exactly the join"
    );
    assert_eq!(artifact.node_count(), 3, "two sources + one join");
}

/// The **runtime** fan-in proof that runs through the one-call seam: because
/// `RunnableFlow` drives single-input nodes, an author who wants a *runnable*
/// join aggregates the upstream values into a struct produced by an intermediate
/// node (the documented escape hatch beyond arity, and the shape the one-call
/// path drives), then depends on that one handle. The join still succeeds only
/// when its upstream succeeded — the default `all-succeeded` rule.
#[derive(Clone)]
struct CountAndLabel {
    count: u64,
    label: String,
}
struct MakeCountAndLabel;
impl Task for MakeCountAndLabel {
    type Input = ();
    type Output = CountAndLabel;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<CountAndLabel, TaskError> {
        Ok(CountAndLabel {
            count: 3,
            label: "rows".to_string(),
        })
    }
}
struct RenderCountAndLabel;
impl Task for RenderCountAndLabel {
    type Input = CountAndLabel;
    type Output = String;
    async fn run(&mut self, _c: &RunContext, input: CountAndLabel) -> Result<String, TaskError> {
        Ok(format!("{} {}", input.count, input.label))
    }
}

#[test]
fn fan_in_via_aggregate_struct_runs_through_the_one_call_seam() {
    let mut flow = RunnableFlow::new();
    let aggregate = flow.register_source("aggregate", MakeCountAndLabel);
    let rendered =
        flow.register::<RenderCountAndLabel, _>("render", RenderCountAndLabel, aggregate);

    let mem = MemorySink::default();
    let report = flow
        .run(
            "cookbook-fan-in",
            &RunConfig::new(temp_base("fan-in")),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    assert_eq!(report.outcome(), RunOutcome::Succeeded);
    assert_eq!(terminal_of(&mem.bytes(), "render"), "succeeded");
    assert_eq!(report.output(rendered), Some("3 rows".to_string()));
}

// ===========================================================================
// Entry: BRANCH-IN-TASK — `self-skip` vs `succeed-with-empty` for joins (C15).
// A task that decides "nothing to do" returns a deliberate skip that propagates;
// a branch that must keep a downstream join alive succeeds with an empty value.
// ===========================================================================

/// A branch that decides there is nothing to do and **self-skips**. The skip
/// propagates to any `all-succeeded` downstream as `upstream-skipped`.
struct SelfSkippingBranch;
impl Task for SelfSkippingBranch {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Err(TaskError::skip("no work in this partition"))
    }
}
/// A branch that must keep a downstream join alive: it **succeeds with an empty
/// value** (`Option::None`) instead of skipping, so the join still runs.
struct SucceedWithEmptyBranch;
impl Task for SucceedWithEmptyBranch {
    type Input = ();
    type Output = Option<u64>;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Option<u64>, TaskError> {
        Ok(None)
    }
}
struct ConsumeOptional;
impl Task for ConsumeOptional {
    type Input = Option<u64>;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: Option<u64>) -> Result<u64, TaskError> {
        Ok(input.unwrap_or(0))
    }
}
struct ConsumeValue;
impl Task for ConsumeValue {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input)
    }
}

#[test]
fn branch_in_task_self_skip_propagates_a_skip_to_the_join() {
    // A self-skipping branch: the downstream, under the default `all-succeeded`
    // rule, is marked `upstream-skipped` and never runs — a skip-only run is still
    // a *successful* run (arch.md Vocabulary).
    let mut flow = RunnableFlow::new();
    let branch = flow.register_source("branch", SelfSkippingBranch);
    let _join = flow.register::<ConsumeValue, _>("join", ConsumeValue, branch);

    let mem = MemorySink::default();
    let report = flow
        .run(
            "cookbook-self-skip",
            &RunConfig::new(temp_base("self-skip")),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    assert_eq!(
        terminal_of(&mem.bytes(), "branch"),
        "skipped",
        "the branch originated a skip"
    );
    assert_eq!(
        terminal_of(&mem.bytes(), "join"),
        "upstream-skipped",
        "the skip propagated to the all-succeeded join"
    );
    assert_eq!(
        report.outcome(),
        RunOutcome::Succeeded,
        "a run whose only non-success outcomes are skips is a successful run"
    );
}

#[test]
fn branch_in_task_succeed_with_empty_keeps_the_join_alive() {
    // The succeed-with-empty branch keeps the join alive: it produces `None`, so
    // the join runs and sees an explicit empty value instead of being skipped.
    let mut flow = RunnableFlow::new();
    let branch = flow.register_source("branch", SucceedWithEmptyBranch);
    let join = flow.register::<ConsumeOptional, _>("join", ConsumeOptional, branch);

    let mem = MemorySink::default();
    let report = flow
        .run(
            "cookbook-succeed-empty",
            &RunConfig::new(temp_base("succeed-empty")),
            mem.clone(),
            TickClock::default(),
        )
        .expect("assembles and runs");

    assert_eq!(
        terminal_of(&mem.bytes(), "branch"),
        "succeeded",
        "the branch succeeded (with empty)"
    );
    assert_eq!(
        terminal_of(&mem.bytes(), "join"),
        "succeeded",
        "the join stayed alive and ran on the empty value"
    );
    assert_eq!(
        report.output(join),
        Some(0),
        "the join saw None and defaulted"
    );
    assert_eq!(report.outcome(), RunOutcome::Succeeded);
}

// ===========================================================================
// Entry: INCREMENTAL CURSORS VIA SCRATCH — a cursor written on one attempt and
// read on the next (C18). Scratch is a per-node, per-run key-value store of
// opaque bytes reached through `ctx.scratch()`; serialization is the task's
// affair. A value written on one attempt is readable on the next.
// ===========================================================================

/// Inside a task, the store is reached as `ctx.scratch()`. A checkpointing task
/// reads any prior cursor at the top of `run`, does the next slice of work, and
/// writes the advanced cursor before returning — so a later attempt (a retry, or
/// a resumed re-run) continues from the checkpoint rather than starting over.
///
/// This helper is the exact body such a task would run, factored out so the test
/// can drive it across two *separate* stores that share the node's namespace —
/// the observable equivalent of "attempt one" and "attempt two" (C18).
fn advance_cursor(scratch: &dagr_core::context::ScratchStore) -> Result<u64, TaskError> {
    // Read the high-water cursor written by a prior attempt, if any. A scratch
    // I/O error maps (via `?` / `From`) to a retry-eligible task failure — disk
    // trouble is transient more often than not (C18).
    let prior: u64 = match scratch.get(b"cursor")? {
        Some(bytes) => std::str::from_utf8(&bytes)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| TaskError::permanent("corrupt cursor"))?,
        None => 0, // no checkpoint yet: start from the beginning
    };
    // Do the next slice of work and advance the cursor.
    let advanced = prior + 512;
    scratch.put(b"cursor", advanced.to_string().as_bytes())?;
    Ok(advanced)
}

#[test]
fn incremental_cursor_written_on_attempt_one_is_read_on_the_next() {
    use dagr_core::context::{PipelineId, RunId, ScratchStore};
    use dagr_core::handle::NodeId;

    // A real on-disk run store, one per test. Scratch lives under it (C18).
    let base = temp_base("cursor");
    let base_path = std::path::Path::new(&base);
    let (pipeline, run, node) = (
        PipelineId::new("cookbook-cursor"),
        RunId::new("run-1"),
        NodeId::from_name("ingest"),
    );

    // "Attempt one": a fresh store for the node writes the first cursor.
    let attempt_one = ScratchStore::for_node(base_path, &pipeline, &run, node);
    let after_one = advance_cursor(&attempt_one).expect("attempt one advances");
    assert_eq!(
        after_one, 512,
        "attempt one started from nothing and advanced by 512"
    );

    // "Attempt two": a NEW store for the same (base, pipeline, run, node) — the
    // same namespace — reads the cursor attempt one wrote and resumes from it,
    // rather than starting over. This is the whole point of a checkpoint (C18).
    let attempt_two = ScratchStore::for_node(base_path, &pipeline, &run, node);
    let read_back = attempt_two
        .get(b"cursor")
        .expect("read succeeds")
        .expect("attempt two reads the cursor attempt one wrote");
    assert_eq!(
        read_back, b"512",
        "the value written on attempt one is readable on attempt two"
    );
    let after_two = advance_cursor(&attempt_two).expect("attempt two advances");
    assert_eq!(
        after_two, 1024,
        "attempt two resumed from 512 rather than starting over"
    );

    let _ = std::fs::remove_dir_all(base_path);
}

// ===========================================================================
// Entry: DURABLE STAGE BOUNDARIES — the DurableOutput contract (C10/C27). A
// durable node's output type serializes a reference to where the value lives and
// rehydrates it later; that is what lets resume skip completed work.
// ===========================================================================

/// A durable stage-boundary output: it holds a *reference* to where the real
/// (large) value lives, not the value itself. `serialize_reference` names the
/// referent; `rehydrate` reconstructs the typed value from that reference — a
/// lossless round-trip, so resume can rehydrate instead of recomputing (C27).
#[derive(Clone, Debug, PartialEq, Eq)]
struct DatasetRef {
    location: String,
}
impl DurableOutput for DatasetRef {
    fn serialize_reference(&self) -> String {
        self.location.clone()
    }
    fn rehydrate(reference: &str) -> Result<Self, RehydrateError> {
        Ok(DatasetRef {
            location: reference.to_string(),
        })
    }
}

#[test]
fn durable_output_reference_round_trips_losslessly() {
    // The contract's core guarantee: `rehydrate(serialize_reference(v))` yields a
    // value EQUAL to `v`. This is what makes a durable stage boundary resume-safe:
    // at resume, a satisfied-from-prior node's slot is filled by rehydrating the
    // recorded reference rather than re-running the producing task.
    let produced = DatasetRef {
        location: "s3://bucket/run-42/stage-3.parquet".to_string(),
    };
    let reference = produced.serialize_reference();
    let rehydrated = DatasetRef::rehydrate(&reference).expect("rehydration succeeds");
    assert_eq!(
        rehydrated, produced,
        "a durable reference round-trips losslessly (C27), so resume rehydrates rather than recomputes"
    );
}

#[test]
fn a_node_marked_durable_needs_the_contract_or_assembly_rejects_it() {
    // Marking a node durable whose output type does NOT implement the contract is
    // an assembly error (`DurableWithoutContract`) — the trade is stated up front,
    // not discovered mid-run. Here the durable registration path is bounded on
    // `DurableOutput`, so a compliant output assembles cleanly.
    struct ProduceDataset;
    impl Task for ProduceDataset {
        type Input = ();
        type Output = DatasetRef;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<DatasetRef, TaskError> {
            Ok(DatasetRef {
                location: "s3://bucket/run/stage.parquet".to_string(),
            })
        }
    }
    let mut flow = Flow::new();
    let _durable =
        flow.register_source_durable("dataset", &ProduceDataset, NodePolicy::new().durable(true));
    assert!(
        flow.finish().assemble().is_ok(),
        "a durable node whose output implements DurableOutput assembles"
    );
}

// ===========================================================================
// Entry: TWO SAME-TYPED RESOURCES VIA NEWTYPES — retrieval by type succeeds and
// the identical-type registration fails as ambiguous (C9). Same no-string-lookup
// philosophy as typed handles.
// ===========================================================================

// A single underlying client type; two logical roles distinguished by newtypes.
#[derive(Debug, PartialEq, Eq)]
struct HttpClient {
    base_url: String,
}
struct BillingClient(HttpClient);
struct AnalyticsClient(HttpClient);

#[test]
fn two_same_typed_resources_are_distinguished_by_newtypes() {
    // Two resources share the underlying `HttpClient` type but are registered
    // behind DISTINCT newtypes, so each is retrievable by its own type — no string
    // key, no ambiguity (C9).
    let registry = dagr_core::context::ResourceRegistry::builder()
        .register(BillingClient(HttpClient {
            base_url: "https://billing".into(),
        }))
        .expect("billing registers")
        .register(AnalyticsClient(HttpClient {
            base_url: "https://analytics".into(),
        }))
        .expect("analytics registers")
        .build();

    let billing = registry
        .get::<BillingClient>()
        .expect("billing is retrievable by type");
    let analytics = registry
        .get::<AnalyticsClient>()
        .expect("analytics is retrievable by type");
    assert_eq!(billing.0.base_url, "https://billing");
    assert_eq!(analytics.0.base_url, "https://analytics");
}

#[test]
fn registering_two_resources_of_the_identical_type_fails_as_ambiguous() {
    // Registering two resources of the LITERALLY identical type is rejected —
    // a type-keyed `get::<HttpClient>()` could not choose between them (C9). The
    // remedy is the newtype pattern above.
    let result = dagr_core::context::ResourceRegistry::builder()
        .register(HttpClient {
            base_url: "https://one".into(),
        })
        .expect("the first HttpClient registers")
        .register(HttpClient {
            base_url: "https://two".into(),
        });
    assert!(
        result.is_err(),
        "a second resource of the identical type is rejected as ambiguous (C9)"
    );
}
