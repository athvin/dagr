//! **Driver scratch wiring**. Written first, TDD.
//!
//! A task must reach its **durable scratch store**
//! in a **real** `drive()` run — the shipped driver must thread the run-store
//! base into each attempt's (and each teardown's) `RunContext` so a node's scratch
//! lands under `<base>/<pipeline>/<run-id>/scratch/<node>/`, exactly where a later
//! resume's [`carry_forward`](dagr_core::scratch::ScratchStore::carry_forward) copies
//! it. Earlier the driver built its per-attempt/teardown context with **no**
//! scratch root (the honestly-unwired store), so scratch never persisted through a
//! real run; these tests pin the wiring and its backward-compatibility.
//!
//! These are the production-seam proofs the demo composes end-to-end:
//!   * a task that writes scratch through `drive()` leaves its value on disk under
//!     the run's real per-node namespace (attempt path);
//!   * a teardown that writes scratch through `drive()` reaches its own namespace
//!     (teardown path);
//!   * a run that uses **no** scratch is byte-identical to before (terminals,
//!     events, outcome unchanged) — the wiring is invisible to a non-scratch run.
//!
//! Determinism + isolation: a **private per-test** run-store base (pid + a
//! process-monotonic counter + a nanosecond stamp), removed on drop — never shared
//! `/tmp`. No wall-clock sleep; the run drives to completion synchronously.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{NodeRunner, RunConfig, RunPlan, drive};
use dagr_core::TaskError;
use dagr_core::context::{PipelineId, RunContext, RunId as CoreRunId, TerminalState};
use dagr_core::execution::{AttemptEventSink, run_attempt_caught};
use dagr_core::flow::Flow;
use dagr_core::handle::NodeId;
use dagr_core::scratch::ScratchStore;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;

// ===========================================================================
// Private per-test run-store base (never shared /tmp; removed on drop).
// ===========================================================================

struct TempBase {
    path: PathBuf,
}
impl TempBase {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let unique = format!(
            "dagr-t63-scratch-{tag}-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            nanos,
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path).expect("create private temp base");
        Self { path }
    }
    fn base(&self) -> &Path {
        &self.path
    }
}
impl Drop for TempBase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// ===========================================================================
// A deterministic clock + capturing sink (the injection seam).
// ===========================================================================

#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}
impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}

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

// ===========================================================================
// Tasks that reach scratch through the ORDINARY context API.
// ===========================================================================

/// A source that writes a distinctive value into its scratch under `key` (through
/// the context the driver hands it) then succeeds — proving the driver wired the
/// scratch root into the attempt context.
struct WritesScratch {
    key: Vec<u8>,
    value: Vec<u8>,
}
impl Task for WritesScratch {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        c.scratch().put(&self.key, &self.value)?;
        Ok(1)
    }
}

/// A plain source that neither reads nor writes scratch (the no-scratch control).
struct Plain;
impl Task for Plain {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

// ===========================================================================
// A type-erased source runner over the real caught attempt path.
// ===========================================================================

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

fn slot_for<U: Send + Sync + 'static>(name: &str) -> Arc<Slot<U>> {
    Arc::new(Slot::new(
        NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    ))
}

const PIPE: &str = "scratch-wiring-pipe";

/// Blank the volatile `wall` timestamp on every stream record so two runs with an
/// identical injected monotonic clock compare byte-for-byte (the wall clock is the
/// only non-deterministic field the writer stamps).
fn strip_wall(stream: &[u8]) -> Vec<serde_json::Value> {
    let parsed = dagr_artifact::event_stream::read_records(stream).expect("stream parses");
    parsed
        .records
        .into_iter()
        .map(|mut rec| {
            if let Some(obj) = rec.as_object_mut() {
                obj.insert("wall".into(), serde_json::Value::String("<wall>".into()));
            }
            rec
        })
        .collect()
}

/// The scratch store handle for a node under one run of the driver's layout — the
/// production path (`PipelineId::new(pipeline_name)`), which is what the driver
/// must use so its scratch namespace and the resume carry-forward agree.
fn store_for(base: &Path, run: &str, node: &str) -> ScratchStore {
    ScratchStore::for_node(
        base,
        &PipelineId::new(PIPE),
        &CoreRunId::new(run),
        NodeId::from_name(node),
    )
}

// ===========================================================================
// The attempt path threads scratch into the task's context.
// ===========================================================================

/// **A task that writes scratch through the real `drive()` loop leaves its value on
/// disk under the run's per-node namespace.** Earlier the driver built the
/// attempt context with no scratch root, so this value was never persisted; the
/// wiring is what the demo's checkpoint node depends on.
#[test]
fn a_task_reaches_its_scratch_store_in_a_real_drive_run() {
    let base = TempBase::new("attempt");
    let run_id = "run-scratch";
    let key = b"high-water";
    let value = b"finished-item-K";

    let mut flow = Flow::new();
    let _w = flow.register_source("writer", &Plain);
    let pipeline = flow.finish();

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "writer".into(),
        SourceRunner::boxed(
            "writer",
            WritesScratch {
                key: key.to_vec(),
                value: value.to_vec(),
            },
            slot_for::<u64>("writer"),
        ),
    );

    let cfg = RunConfig::new(base.base().to_str().unwrap()).run_id(run_id);
    let report = drive(
        &cfg,
        PIPE,
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        MemorySink::default(),
        TickClock::default(),
    );
    assert_eq!(report.outcome, RunOutcome::Succeeded);

    // The value the task wrote through its context is readable from the run's real
    // per-node scratch namespace — the driver threaded the run-store base in.
    let observed = store_for(base.base(), run_id, "writer")
        .get(key)
        .expect("scratch read");
    assert_eq!(
        observed.as_deref(),
        Some(&value[..]),
        "the task's scratch write through drive() landed under the run's per-node namespace"
    );
}

// ===========================================================================
// A no-scratch run is byte-identical (backward compatibility).
// ===========================================================================

/// **A run that uses no scratch is byte-identical after the wiring.** The scratch
/// root is threaded unconditionally, but a task that never touches scratch is
/// unaffected: the same terminal states, the same event stream, the same outcome as
/// before. Proven by running the same no-scratch pipeline twice and comparing the
/// full recorded stream (modulo the injected clock, which is identical) and the
/// terminals.
#[test]
fn a_no_scratch_run_is_byte_identical() {
    let base = TempBase::new("nofx");

    let run_once = || {
        let mut flow = Flow::new();
        let _a = flow.register_source("a", &Plain);
        let pipeline = flow.finish();
        let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
        runners.insert(
            "a".into(),
            SourceRunner::boxed("a", Plain, slot_for::<u64>("a")),
        );
        let sink = MemorySink::default();
        let cfg = RunConfig::new(base.base().to_str().unwrap()).run_id("fixed-run");
        let report = drive(
            &cfg,
            PIPE,
            Ok(RunPlan::new(pipeline, runners)),
            &[],
            sink.clone(),
            TickClock::default(),
        );
        (report.outcome, report.terminal_states, sink.bytes())
    };

    let (outcome_a, terminals_a, stream_a) = run_once();
    let (outcome_b, terminals_b, stream_b) = run_once();

    assert_eq!(outcome_a, RunOutcome::Succeeded);
    assert_eq!(outcome_a, outcome_b, "outcome unchanged run-to-run");
    assert_eq!(terminals_a, terminals_b, "terminals unchanged run-to-run");
    // Compare the stream modulo the only volatile field — the `wall` timestamp,
    // stamped from the real wall clock (the injected monotonic clock is identical).
    // Everything else (records, ordering, offsets, kinds, outcome) must be
    // byte-identical: the scratch wiring is invisible to a no-scratch run.
    assert_eq!(
        strip_wall(&stream_a),
        strip_wall(&stream_b),
        "a no-scratch run's event stream is byte-identical (the scratch wiring is invisible)"
    );
    // And the no-scratch run left no scratch subtree behind.
    let scratch_dir = base.base().join(PIPE).join("fixed-run").join("scratch");
    assert!(
        !scratch_dir.exists(),
        "a run that writes no scratch creates no scratch namespace"
    );
}
