//! **Async discipline and allocation review** (T97). Written first, TDD.
//!
//! This ticket is verification-first: the audit's reading was that dagr's async
//! surface is *already* disciplined, and the work is turning that reading into
//! facts a future change cannot quietly break. Each test below pins one of them.
//!
//! * **Channel depth** — the run loop's `AttemptDone` queue is deliberately
//!   `unbounded`. These tests instrument the queue's real occupancy (they read the
//!   receiver's length; nothing is inferred from the loop's own bookkeeping) and
//!   pin it against the bound the admission controller and the loop's in-flight
//!   accounting already impose. See the comment at the construction site in
//!   `crates/cli/src/driver.rs` for why bounding the channel would *duplicate* that
//!   invariant — and deadlock.
//! * **Task lifetimes** — every `tokio::spawn` / `spawn_blocking` / `JoinHandle` /
//!   `thread::spawn` in production source is enumerated with a stated disposition,
//!   so a new spawn site cannot land undocumented.
//! * **Locks across awaits** — `clippy::await_holding_lock` is written out at
//!   `deny` in both halves of the lint policy, so the guarantee survives a future
//!   regrouping of `clippy::all` upstream.
//! * **The blocking-fsync seam** — `RunContext::scratch` and the `scratch` module
//!   docs state, in words, that a synchronous fsync on a default `AwaitBound` node
//!   blocks an async worker, and name `Blocking` as the remedy.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::driver::{AttemptQueueProbe, NodeRunner, RunConfig, RunPlan, drive};
use dagr_core::TaskError;
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{AttemptEventSink, run_attempt_caught};
use dagr_core::flow::{FailureMode, Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;

// ===========================================================================
// Scaffolding: a private temp base, an in-memory sink, a counter clock.
// ===========================================================================

/// A private per-test temp directory, removed on drop. Each test gets its own run
/// store so the suite is collision-proof under CI parallelism.
struct TempBase {
    path: PathBuf,
}

impl TempBase {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path =
            std::env::temp_dir().join(format!("dagr-t97-{tag}-{}-{nanos}-{n}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create temp base");
        Self { path }
    }

    fn as_str(&self) -> &str {
        self.path.to_str().expect("temp base path is utf-8")
    }
}

impl Drop for TempBase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}

impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines
            .lock()
            .expect("sink mutex not poisoned")
            .extend_from_slice(line);
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
// A wide graph of independent source nodes.
// ===========================================================================

/// How many independent source nodes the wide-graph runs use. Wide enough that a
/// queue growing with the frontier would be unmistakable against a bound of one.
const WIDE: usize = 12;

/// The per-node declared working memory, and a pool pinned so **exactly one** node
/// fits at a time.
const NODE_MEMORY: u64 = 600;
const ONE_NODE_POOL: u64 = 1_000;

struct Succeeds;
impl Task for Succeeds {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

struct Fails;
impl Task for Fails {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Err(TaskError::permanent("the deliberate failure"))
    }
}

/// A runner over a source task that always succeeds.
struct SourceRunner {
    name: String,
    task: Option<Succeeds>,
    slot: Arc<Slot<u64>>,
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
        let mut task = self.task.take().expect("source runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

/// A runner over a source task that always fails permanently.
struct FailingRunner {
    name: String,
    task: Option<Fails>,
    slot: Arc<Slot<u64>>,
}

impl NodeRunner for FailingRunner {
    fn name(&self) -> &str {
        &self.name
    }
    fn run<'a>(
        &'a mut self,
        ctx: &'a RunContext,
        sink: &'a mut (dyn AttemptEventSink + Send),
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TerminalState> + Send + 'a>> {
        let name = self.name.clone();
        let mut task = self.task.take().expect("failing runner runs once");
        let slot = Arc::clone(&self.slot);
        Box::pin(async move {
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

fn slot_for(name: &str) -> Arc<Slot<u64>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    ))
}

fn wide_node_name(i: usize) -> String {
    format!("wide-{i:02}")
}

/// `WIDE` independent zero-dependency source nodes, each declaring
/// [`NODE_MEMORY`]. With `failing_first` the lowest-ordered node fails
/// permanently, which is what a stop-on-first-failure run needs.
fn wide_plan(failing_first: bool) -> RunPlan {
    let mut flow = Flow::new();
    let policy = NodePolicy::new().working_memory(NODE_MEMORY);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();

    if failing_first {
        let name = "aaa-fails".to_string();
        let _ = flow.register_source_with(&name, &Fails, policy);
        runners.insert(
            name.clone(),
            Box::new(FailingRunner {
                task: Some(Fails),
                slot: slot_for(&name),
                name,
            }),
        );
    }
    for i in 0..WIDE {
        let name = wide_node_name(i);
        let _ = flow.register_source_with(&name, &Succeeds, policy);
        runners.insert(
            name.clone(),
            Box::new(SourceRunner {
                task: Some(Succeeds),
                slot: slot_for(&name),
                name,
            }),
        );
    }
    let pipeline: Pipeline = flow.finish();
    pipeline.assemble().expect("the wide plan assembles");
    RunPlan::new(pipeline, runners)
}

// ===========================================================================
// Channel depth — the `AttemptDone` queue is structurally bounded
// ===========================================================================

/// **Test-plan scenario: wide graph, narrow admission.** With the memory pool
/// pinned so exactly one node can hold a permit at a time, at most one attempt is
/// ever in flight, so at most one `AttemptDone` can ever be queued. The observed
/// peak is asserted **exactly**: `<= 1` is the bound derived from the admission
/// limit, and `>= 1` proves the probe measured a real queue rather than nothing at
/// all (a vacuous pass is the failure mode a depth assertion is most prone to).
#[test]
fn the_attempt_queue_never_exceeds_the_admission_bound_on_a_wide_graph() {
    let base = TempBase::new("queue-narrow");
    let probe = Arc::new(AttemptQueueProbe::new());
    let sink = MemorySink::default();

    let report = drive(
        &RunConfig::new(base.as_str())
            .capacities(PoolCapacities::new().memory(ONE_NODE_POOL))
            .attempt_queue_probe(Arc::clone(&probe)),
        "t97-queue-narrow",
        Ok(wide_plan(false)),
        &[],
        sink,
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    assert_eq!(report.terminal_states.len(), WIDE);

    // One permit ⇒ one in-flight attempt ⇒ one queued completion. Not "about one".
    assert_eq!(
        probe.peak_depth(),
        1,
        "with a one-node pool the AttemptDone queue holds exactly one message at \
         its peak; observed {}",
        probe.peak_depth()
    );
}

/// **Test-plan scenario: wide graph, unconstrained pools.** Without a pinned pool
/// every ready node is admitted at once, so the queue's peak rises with the
/// frontier — but it can never exceed the node count, because the loop counts each
/// node into `in_flight` exactly once and each counted node reports exactly once.
/// That is the general structural bound; the admission limit is the tighter special
/// case pinned above.
#[test]
fn the_attempt_queue_is_bounded_by_the_node_count_under_unconstrained_pools() {
    let base = TempBase::new("queue-wide");
    let probe = Arc::new(AttemptQueueProbe::new());
    let sink = MemorySink::default();

    let report = drive(
        &RunConfig::new(base.as_str()).attempt_queue_probe(Arc::clone(&probe)),
        "t97-queue-wide",
        Ok(wide_plan(false)),
        &[],
        sink,
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Succeeded);
    let peak = probe.peak_depth();
    assert!(
        (1..=WIDE).contains(&peak),
        "the queue's peak depth stays within [1, {WIDE}] (one message per node \
         counted in flight); observed {peak}"
    );
}

/// **The adversarial case the bound has to survive.** Under stop-on-first-failure
/// the loop itself pushes a *burst* of `cancelled` terminals for every pending
/// default-rule node — synchronously, from inside the loop. So the queue's peak is
/// **not** bounded by the admission limit here (the burst exceeds it), which is
/// precisely why the channel must stay unbounded: a bounded `send` from the loop
/// would block the only task that drains it. The peak is still bounded by the node
/// count, which is the invariant the construction site names.
#[test]
fn a_stop_on_first_failure_burst_exceeds_the_permit_count_but_not_the_node_count() {
    let base = TempBase::new("queue-burst");
    let probe = Arc::new(AttemptQueueProbe::new());
    let sink = MemorySink::default();

    let report = drive(
        &RunConfig::new(base.as_str())
            .capacities(PoolCapacities::new().memory(ONE_NODE_POOL))
            .failure_mode(FailureMode::StopOnFirstFailure)
            .attempt_queue_probe(Arc::clone(&probe)),
        "t97-queue-burst",
        Ok(wide_plan(true)),
        &[],
        sink,
        TickClock::default(),
    );

    assert_eq!(report.outcome, RunOutcome::Failed);

    let nodes = WIDE + 1;
    let peak = probe.peak_depth();
    assert!(
        peak > 1,
        "the stop transition cancels every pending node in one synchronous burst, \
         so the queue exceeds the single permit the pool allows; observed {peak}"
    );
    assert!(
        peak <= nodes,
        "the burst is still bounded by the node count ({nodes}); observed {peak}"
    );
}

// ===========================================================================
// Task lifetimes — every spawn site is accounted for
// ===========================================================================

/// The complete inventory of task/thread-spawning constructs in **production**
/// source (`crates/*/src`), one row per file: the number of matching source lines
/// and the disposition that makes each accounted for. A new spawn site anywhere in
/// the workspace fails this test until it is added here with a reason — which is
/// the point: C16's "nothing orphaned" guarantee is a property of this set, not of
/// any one call site.
const SPAWN_INVENTORY: &[(&str, usize, &str)] = &[
    (
        "crates/cli/src/dispatch.rs",
        3,
        "the three execution surfaces (async runtime spawn, spawn_blocking, rayon \
         compute spawn); every handle is owned by the Dispatcher, which the driver \
         shuts down at run end",
    ),
    (
        "crates/cli/src/driver.rs",
        1,
        "the deliberately DETACHED leftover-temp-dir reclamation thread: it touches \
         only PRIOR runs' ephemeral tmp/ subtrees, never this run's state, so the \
         guarantee is eventual reclamation by a next invocation",
    ),
    (
        "crates/cli/src/signals.rs",
        1,
        "the OS-signal listener, the stated exception: it lives exactly as long as \
         the SignalGuard's own runtime, which is dropped with the guard",
    ),
    (
        "crates/metastore/src/live_sink.rs",
        4,
        "the live-sink projection worker: a named thread whose JoinHandle the sink \
         owns and JOINS on drop, so it cannot outlive the sink",
    ),
];

/// Source lines that spawn a task or thread, or that name a join handle.
fn spawn_tokens() -> [&'static str; 6] {
    [
        "tokio::spawn",
        "spawn_blocking",
        "thread::spawn",
        "thread::Builder",
        "JoinHandle",
        ".spawn(",
    ]
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a workspace root two levels up")
        .to_path_buf()
}

fn rust_sources_under(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources_under(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Count the spawn-shaped **code** lines in `text`. Lines whose first non-space
/// characters are `//` are comments — the modules here discuss `spawn_blocking` at
/// length in their docs, and prose is not a spawn site.
fn spawn_lines(text: &str) -> usize {
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .filter(|line| spawn_tokens().iter().any(|t| line.contains(t)))
        .count()
}

/// **Test-plan scenario: task lifetimes.** Every spawn site in production source is
/// enumerated with a disposition, and nothing outside the inventory exists. The
/// scan is proven non-vacuous by asserting the inventory is non-empty and that the
/// files it names really do match.
#[test]
fn every_spawn_site_in_production_source_is_accounted_for() {
    let root = repo_root();
    let mut found: BTreeMap<String, usize> = BTreeMap::new();

    let crates_dir = root.join("crates");
    let mut sources = Vec::new();
    for entry in std::fs::read_dir(&crates_dir)
        .expect("the crates directory is readable")
        .flatten()
    {
        rust_sources_under(&entry.path().join("src"), &mut sources);
    }
    assert!(
        sources.len() > 20,
        "the scan found only {} production sources — it is not looking where it \
         should be",
        sources.len()
    );

    for path in sources {
        let text = std::fs::read_to_string(&path).expect("source file is readable");
        let count = spawn_lines(&text);
        if count > 0 {
            let rel = path
                .strip_prefix(&root)
                .expect("source lives under the repo root")
                .to_string_lossy()
                .replace('\\', "/");
            found.insert(rel, count);
        }
    }

    let expected: BTreeMap<String, usize> = SPAWN_INVENTORY
        .iter()
        .map(|(file, count, _)| ((*file).to_string(), *count))
        .collect();

    assert_eq!(
        found, expected,
        "the production spawn inventory changed; every spawn site must be listed \
         in SPAWN_INVENTORY with the reason it cannot outlive the run"
    );
    for (file, _, reason) in SPAWN_INVENTORY {
        assert!(
            reason.len() > 40,
            "{file} needs a real disposition, not a placeholder"
        );
    }
}

// ===========================================================================
// Locks and awaits — the lint is written out at deny
// ===========================================================================

/// **Test-plan scenario: locks and awaits.** `clippy::await_holding_lock` is the
/// mechanical guarantee that no guard is ever held across a suspension point. It is
/// already *implied* by the `clippy::all` group dagr denies workspace-wide — which
/// is why the workspace has been green under it all along — but an implied deny
/// disappears the moment upstream regroups the lint. Writing it out at `deny` in
/// both halves of the policy makes it survive that, and this test makes the pair
/// impossible to silently weaken. (The lint firing anywhere is a build failure, so
/// the green build IS the sweep.)
#[test]
fn await_holding_lock_is_denied_in_both_halves_of_the_lint_policy() {
    let root = repo_root();
    for file in ["lints.toml", "Cargo.toml"] {
        let text = std::fs::read_to_string(root.join(file))
            .unwrap_or_else(|e| panic!("{file} is readable: {e}"));
        assert!(
            text.contains(r#"await_holding_lock = "deny""#),
            "{file} must deny clippy::await_holding_lock explicitly, so the \
             guarantee does not depend on the lint's current clippy group"
        );
    }
}

// ===========================================================================
// The blocking-fsync-on-an-async-worker seam is documented
// ===========================================================================

/// **Definition of done: the blocking-I/O gap is documented at the seam.**
/// `ScratchStore::put` fsyncs twice, synchronously, and `RunContext::scratch` hands
/// that store to a task body whose default execution class is `AwaitBound` — so a
/// task author can block an async worker with no type-level warning. An async
/// scratch API is impossible (`dagr-core` carries no runtime dependency), so the
/// fix is words at the seam naming `Blocking` as the remedy. This test keeps those
/// words there.
#[test]
fn the_scratch_seam_documents_the_blocking_fsync_gap() {
    let root = repo_root();

    let context = std::fs::read_to_string(root.join("crates/core/src/context.rs"))
        .expect("context.rs is readable");
    let scratch_doc = doc_block_before(&context, "pub fn scratch(&self) -> &ScratchStore {");
    for needle in ["fsync", "async worker", "Blocking"] {
        assert!(
            scratch_doc.contains(needle),
            "RunContext::scratch's docs must mention {needle:?} — a task author \
             gets no type-level warning, so the words at the seam are the warning. \
             Got:\n{scratch_doc}"
        );
    }

    let scratch = std::fs::read_to_string(root.join("crates/core/src/scratch.rs"))
        .expect("scratch.rs is readable");
    let module_doc: String = scratch
        .lines()
        .take_while(|l| l.starts_with("//!") || l.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    for needle in ["fsync", "async worker", "Blocking"] {
        assert!(
            module_doc.contains(needle),
            "the scratch module docs must mention {needle:?}"
        );
    }
}

/// The contiguous run of `///` doc lines immediately preceding `anchor`.
fn doc_block_before(text: &str, anchor: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let at = lines
        .iter()
        .position(|l| l.contains(anchor))
        .unwrap_or_else(|| panic!("anchor not found: {anchor}"));
    let mut start = at;
    while start > 0 {
        let candidate = lines[start - 1].trim_start();
        if candidate.starts_with("///") || candidate.starts_with('#') {
            start -= 1;
        } else {
            break;
        }
    }
    lines[start..at].join("\n")
}
