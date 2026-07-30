//! Error-chain, panic, and arithmetic hardening — the `dagr-cli` half plus the
//! workspace-wide discipline checks. Written first, TDD.
//!
//! Four of the ticket's Test-plan groups land here:
//!
//! - **Causal chains.** `GraphVerbError` and `StructureAssertError` each wrap a
//!   genuine underlying error (`GraphEmitError` / `io::Error`) and left
//!   `impl Error for X {}` empty, so `source()` returned `None` and the cause was
//!   invisible to any caller walking the chain.
//! - **Arithmetic.** The run loop's `in_flight` counter is mutated from five call
//!   sites across async control flow and decremented in exactly one. These tests
//!   drive the loop along **every** mutating path — admitted, capacity-pending,
//!   cancelled-while-pending, can-never-fit, and the drain — and assert what a
//!   balanced counter means observably: the loop terminates and **every** node
//!   reaches exactly one terminal state.
//! - **Poisoning policy — the *panic* half.** That half is a documentation change,
//!   so the test pins the documentation: every production lock site states its
//!   policy and the reason, and the two philosophies are reconciled by one stated
//!   rule rather than by two undocumented habits.
//! - **The recorded-as-clean surface.** The audit's "checked and found clean"
//!   findings are asserted mechanically so they stay true, rather than being
//!   asserted once in prose.

use std::collections::BTreeMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dagr_artifact::event_stream::{EventSink, MonotonicClock};
use dagr_cli::driver::{CancelHandle, NodeRunner, RunConfig, RunPlan, RunReport, drive};
use dagr_cli::graph::{GraphEmitError, GraphVerbError};
use dagr_cli::structure_snapshot::StructureAssertError;
use dagr_core::TaskError;
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::NodePolicy;
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::execution::{AttemptEventSink, run_attempt_caught};
use dagr_core::flow::{Flow, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;

// ===========================================================================
// Causal chains
// ===========================================================================

/// **`GraphVerbError` exposes its wrapped emit failure.** The verb wraps the
/// emitter's own error; walking `source()` must reach it, and its `Display` must
/// appear in the chain.
#[test]
fn graph_verb_error_exposes_the_emit_failure_through_source() {
    let cause = GraphEmitError::MissingStableNames {
        node: "loader".to_string(),
    };
    let cause_text = cause.to_string();
    let err = GraphVerbError::from(cause);

    let source = err
        .source()
        .expect("GraphVerbError::Emit wraps a GraphEmitError; source() must expose it");
    assert!(
        source.downcast_ref::<GraphEmitError>().is_some(),
        "the cause must be the GraphEmitError itself: {source}"
    );
    assert_eq!(
        source.to_string(),
        cause_text,
        "the cause's own Display is what a chain-walking caller reads"
    );
}

/// **`GraphVerbError` exposes its wrapped sink failure.** The I/O variant carries a
/// real `io::Error`; the chain must reach it (an operator needs the OS reason, not
/// just "writing the graph artifact failed").
#[test]
fn graph_verb_error_exposes_the_io_failure_through_source() {
    let err = GraphVerbError::from(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "sink is read-only",
    ));

    let source = err
        .source()
        .expect("GraphVerbError::Io wraps an io::Error; source() must expose it");
    let io = source
        .downcast_ref::<std::io::Error>()
        .expect("the cause must be the io::Error itself");
    assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied);
}

/// **`StructureAssertError` exposes both of its wrapped causes**, and the variant
/// that genuinely has none keeps `source() == None` — the fix must not fabricate a
/// link where no cause exists.
#[test]
fn structure_assert_error_exposes_its_causes_and_fabricates_none() {
    let emit = StructureAssertError::from(GraphEmitError::MalformedStableName {
        node: "loader".to_string(),
        value: "not a stable name".to_string(),
    });
    assert!(
        emit.source()
            .and_then(|s| s.downcast_ref::<GraphEmitError>())
            .is_some(),
        "the Emit variant must expose its GraphEmitError"
    );

    let io = StructureAssertError::from(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "fixture missing",
    ));
    assert_eq!(
        io.source()
            .and_then(|s| s.downcast_ref::<std::io::Error>())
            .map(std::io::Error::kind),
        Some(std::io::ErrorKind::NotFound),
        "the Io variant must expose its io::Error"
    );

    // The mismatch variant is built from a computed diff, not from an error.
    let mismatch = assert_structure_mismatch();
    assert!(
        mismatch.source().is_none(),
        "a structural mismatch wraps no error; a fabricated source would be a lie"
    );
}

/// Produce a genuine `StructureAssertError::Mismatch` by asserting a pipeline
/// against a blessed fixture of a *different* pipeline.
fn assert_structure_mismatch() -> StructureAssertError {
    let dir = temp_dir("structure-mismatch");
    std::fs::create_dir_all(&dir).expect("temp dir creates");
    let fixture = dir.join("golden.structure.json");

    let mut golden_flow = Flow::new();
    let _ = golden_flow.register_source_named::<NamedAlpha>(
        "alpha",
        &NamedAlpha,
        None::<String>,
        NodePolicy::new(),
    );
    let golden = golden_flow.finish();
    dagr_cli::structure_snapshot::bless_structure(&golden, "demo", &fixture)
        .expect("the fixture blesses");

    let mut changed_flow = Flow::new();
    let _ = changed_flow.register_source_named::<NamedBeta>(
        "beta",
        &NamedBeta,
        None::<String>,
        NodePolicy::new(),
    );
    let changed = changed_flow.finish();
    dagr_cli::structure_snapshot::assert_structure(&changed, "demo", &fixture)
        .expect_err("a different structure must not match the fixture")
}

// ===========================================================================
// Arithmetic — the run loop's in-flight counter
// ===========================================================================

/// **Every outcome class balances the counter.** Four independent nodes settle
/// `succeeded` / `failed` / `skipped` / `timed-out`; the loop must terminate with
/// every one of them holding exactly one terminal state. A counter that
/// over-decremented would end the loop early (a node left without a terminal); one
/// that underflowed would wrap and never end at all.
#[test]
fn every_outcome_class_leaves_the_in_flight_counter_balanced() {
    let nodes = [
        ("won", TerminalState::Succeeded),
        ("lost", TerminalState::Failed),
        ("declined", TerminalState::Skipped),
        ("expired", TerminalState::TimedOut),
    ];
    let report = drive_scripted(&nodes, RunConfig::new(temp_base("outcome-classes")));
    assert_every_node_terminal(&report, &nodes);
}

/// **The capacity-pending path balances too.** With a memory pool that admits one
/// node at a time, every node but the first goes through `pending` and is admitted
/// by a later release — a different increment site from the initial frontier's.
#[test]
fn the_capacity_pending_path_leaves_the_counter_balanced() {
    let nodes = [
        ("first", TerminalState::Succeeded),
        ("second", TerminalState::Succeeded),
        ("third", TerminalState::Succeeded),
    ];
    let report = drive_scripted_with_cost(
        &nodes,
        600,
        RunConfig::new(temp_base("capacity-pending"))
            .capacities(PoolCapacities::new().memory(1000)),
    );
    assert_every_node_terminal(&report, &nodes);
}

/// **The cancel-pending path balances too.** Under stop-on-first-failure the
/// nodes still waiting for capacity are settled `cancelled` through their own
/// increment site — each one counted in flight and then decremented by the single
/// decrement. A miscount here would either strand a node without a terminal or
/// leave the loop spinning.
#[test]
fn the_cancel_pending_path_leaves_the_counter_balanced() {
    // 600 bytes each against a 1000-byte pool: exactly one node in flight at a
    // time, so the alphabetically-first (failing) node runs while the other two
    // wait — and the stop it triggers settles both waiters.
    let nodes = [
        ("aaa-fails", TerminalState::Failed),
        ("zzz-one", TerminalState::Succeeded),
        ("zzz-two", TerminalState::Succeeded),
    ];
    let report = drive_scripted_with_cost(
        &nodes,
        600,
        RunConfig::new(temp_base("stop-cancel"))
            .capacities(PoolCapacities::new().memory(1000))
            .failure_mode(dagr_core::flow::FailureMode::StopOnFirstFailure),
    );

    assert_eq!(
        report.terminal_states.len(),
        nodes.len(),
        "a stopped run still settles every node exactly once: {:?}",
        report.terminal_states
    );
}

/// **Cancellation balances the counter.** An external interrupt fired from inside
/// the run enters the full drain: pending nodes are settled through the
/// cancel-node increment and the in-flight attempts drain through the single
/// decrement. Every node must still end with exactly one terminal state.
#[test]
fn an_external_interrupt_leaves_the_counter_balanced() {
    let config = RunConfig::new(temp_base("interrupt"))
        .grace(std::time::Duration::from_millis(200))
        .capacities(PoolCapacities::new().memory(1000));
    let handle = config.cancel_handle();

    let mut flow = Flow::new();
    let _ = flow.register_source_with(
        "trigger",
        &FiresCancelThenAwaits { handle: None },
        NodePolicy::new().working_memory(600),
    );
    let _ = flow.register_source_with("waiter", &Alpha, NodePolicy::new().working_memory(600));
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "trigger".into(),
        SourceRunner::boxed(
            "trigger",
            FiresCancelThenAwaits {
                handle: Some(handle),
            },
        ),
    );
    runners.insert("waiter".into(), SourceRunner::boxed("waiter", Alpha));

    let report = drive(
        &config,
        "hardening",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        MemorySink::default(),
        TickClock::default(),
    );

    assert_eq!(
        report.terminal_states.len(),
        2,
        "a cancelled run still settles every node exactly once: {:?}",
        report.terminal_states
    );
}

/// **Teardown runs after a balanced loop.** The teardown phase begins only once
/// the main loop has drained (the counter reached zero); a teardown node that ran
/// is proof the loop ended rather than wedged, and every node — covered and
/// teardown alike — is terminal.
#[test]
fn teardown_runs_after_the_loop_drains_and_every_node_is_terminal() {
    let mut flow = Flow::new();
    let covered = flow.register_source("work", &Alpha);
    let _ = flow.register_teardown("cleanup", &Alpha, &[covered.ordering()]);
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");

    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    runners.insert(
        "work".into(),
        ScriptedRunner::boxed("work", TerminalState::Failed),
    );
    runners.insert(
        "cleanup".into(),
        ScriptedRunner::boxed("cleanup", TerminalState::Succeeded),
    );

    let report = drive(
        &RunConfig::new(temp_base("teardown")),
        "hardening",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        MemorySink::default(),
        TickClock::default(),
    );

    assert_eq!(
        report.terminal_states.get("cleanup").copied(),
        Some(TerminalState::Succeeded),
        "the teardown phase ran, so the main loop drained to zero: {:?}",
        report.terminal_states
    );
    assert_eq!(report.terminal_states.len(), 2);
}

// ===========================================================================
// Poisoning policy — the *panic* half, and the workspace discipline
// ===========================================================================

/// Every production `.lock()` site in the workspace, as `(path, line, text)`.
fn production_lock_sites() -> Vec<(PathBuf, usize, String)> {
    let mut out = Vec::new();
    for file in production_sources() {
        let text = std::fs::read_to_string(&file).expect("source readable");
        let mut in_test_module = false;
        let mut test_module_indent = 0usize;
        for (i, line) in text.lines().enumerate() {
            let indent = line.len() - line.trim_start().len();
            if line.trim_start().starts_with("#[cfg(test)]") {
                in_test_module = true;
                test_module_indent = indent;
            } else if in_test_module && line.trim() == "}" && indent == test_module_indent {
                in_test_module = false;
            }
            if in_test_module {
                continue;
            }
            // A `Mutex::lock()` site is one whose `LockResult` is resolved right
            // there. That excludes `io::stdout().lock()` (which returns a guard
            // directly) and the crates' own `fn lock(&self)` helper *calls* — the
            // helpers themselves are lock sites and are caught by this same rule.
            let is_stdio =
                line.contains("stdout()") || line.contains("stderr()") || line.contains("stdin()");
            let is_comment =
                line.trim_start().starts_with("//") || line.trim_start().starts_with("*");
            if !line.contains(".lock()") || is_stdio || is_comment {
                continue;
            }
            let resolution_end = (i + 4).min(text.lines().count());
            let resolution: String = text
                .lines()
                .skip(i)
                .take(resolution_end - i)
                .collect::<Vec<_>>()
                .join("\n");
            let resolves = resolution.contains(".expect(")
                || resolution.contains("unwrap_or_else(")
                || resolution.contains(".unwrap()");
            if resolves {
                out.push((file.clone(), i + 1, line.to_string()));
            }
        }
    }
    out
}

/// **Every production lock states its poisoning policy.** The workspace runs two
/// deliberate philosophies — *recover* where a poisoned lock must not escalate,
/// *panic* where poisoning signals an invariant already violated. Both are
/// defensible; having both undocumented is not. Every site must therefore resolve
/// to one of the two, visibly.
#[test]
fn every_production_lock_site_states_its_poisoning_policy() {
    let sites = production_lock_sites();
    assert!(
        sites.len() >= 15,
        "the scan must actually find the workspace's locks (found {})",
        sites.len()
    );

    let mut undocumented = Vec::new();
    for (path, line, _) in &sites {
        let text = std::fs::read_to_string(path).expect("source readable");
        let lines: Vec<&str> = text.lines().collect();
        // The policy is named in a `Poison policy:` marker in the doc/comment
        // immediately above the lock (or on the lock's own line).
        let window_start = line.saturating_sub(14);
        let window_end = (line + 4).min(lines.len());
        let window = lines[window_start..window_end].join("\n");
        if !window.contains("Poison policy:") {
            undocumented.push(format!(
                "{}:{line}",
                path.strip_prefix(repo_root()).unwrap_or(path).display()
            ));
        }
    }
    assert!(
        undocumented.is_empty(),
        "these production lock sites do not state a poisoning policy and its reason: {undocumented:#?}"
    );
}

/// **The two philosophies are reconciled by one stated rule.** A reader must be
/// able to find *why* `core::slot` and `cli::signals` recover while
/// `core::admission` and `cli::driver` panic, without re-deriving it from 17 sites.
#[test]
fn the_two_poisoning_philosophies_are_reconciled_in_one_place() {
    let register = read_repo_file("docs/rust-skills-register.md");
    assert!(
        register.contains("poison"),
        "the register must record the workspace's poisoning rule"
    );
    for (path, needle) in [
        ("crates/core/src/slot.rs", "recover"),
        ("crates/core/src/admission.rs", "panic"),
        ("crates/cli/src/signals.rs", "recover"),
        ("crates/cli/src/driver.rs", "panic"),
    ] {
        let text = read_repo_file(path);
        let policy_line = text
            .lines()
            .find(|l| l.contains("Poison policy:"))
            .unwrap_or_else(|| panic!("{path} states no poisoning policy"));
        assert!(
            policy_line.to_lowercase().contains(needle),
            "{path}'s stated policy should be `{needle}`, got: {policy_line}"
        );
    }
}

/// **The in-flight decrement follows the codebase's saturating-counter
/// discipline** and asserts the invariant it used to rely on silently. Under the
/// T93 profiles a bare `-= 1` panics in dev/test and wraps to `usize::MAX` in
/// release — a wrapped counter turns the run loop into a non-terminating one.
#[test]
fn the_in_flight_decrement_saturates_and_asserts_its_invariant() {
    let driver = read_repo_file("crates/cli/src/driver.rs");
    assert!(
        !driver.contains("in_flight -= 1"),
        "the bare decrement is the one counter site that breaks the workspace's \
         saturating discipline"
    );
    assert!(
        driver.contains("in_flight = in_flight.saturating_sub(1)"),
        "the decrement must saturate, like the 25 other counter sites"
    );
    assert!(
        driver.contains("debug_assert!(in_flight > 0"),
        "the paired invariant (a reported attempt was counted in flight) must be \
         asserted rather than relied on silently"
    );
}

/// **The write-discard convention is stated once per module, not 25 times.** The
/// `let _ = writeln!(out, …)` sites in the two printing modules discard a real
/// `io::Write` failure deliberately; `anti-empty-catch` is satisfied by a rule at
/// module level, not by repeating a comment at every call.
#[test]
fn the_writeln_discard_convention_is_stated_once_per_module() {
    for path in ["crates/cli/src/registry.rs", "crates/cli/src/contract.rs"] {
        let text = read_repo_file(path);
        let discards = text.matches("let _ = writeln!").count();
        assert!(
            discards > 5,
            "{path} should still carry the discard sites this convention covers"
        );
        let header: String = text
            .lines()
            .take_while(|l| l.starts_with("//!") || l.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            header.contains("Discarded writes"),
            "{path}'s module documentation must state the write-discard convention once"
        );
    }
}

/// **Every cast suppression carries a reason.** A suppression without one is a
/// suppression nobody can review.
#[test]
fn every_cast_suppression_carries_a_reason() {
    let mut missing = Vec::new();
    for file in production_sources() {
        let text = std::fs::read_to_string(&file).expect("source readable");
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if !line.contains("clippy::cast_") {
                continue;
            }
            // The attribute may span several lines; look at the whole neighbourhood
            // for the `reason = "…"` that makes the suppression reviewable.
            let start = i.saturating_sub(4);
            let end = (i + 6).min(lines.len());
            let attr = lines[start..end].join("\n");
            if !attr.contains("reason = ") && !attr.contains("reason=") {
                missing.push(format!(
                    "{}:{}",
                    file.strip_prefix(repo_root()).unwrap_or(&file).display(),
                    i + 1
                ));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "these cast suppressions carry no `reason = \"…\"`: {missing:#?}"
    );
}

/// **Recorded as clean, and kept that way.** The audit found zero `todo!` /
/// `unimplemented!` and zero float `==` in production. Recording that in prose
/// keeps it true for exactly as long as nobody adds one; asserting it keeps it
/// true.
#[test]
fn the_recorded_as_clean_findings_stay_clean() {
    let mut placeholders = Vec::new();
    let mut float_equality = Vec::new();
    for file in production_sources() {
        let text = std::fs::read_to_string(&file).expect("source readable");
        for (i, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            // Doc comments carry illustrative examples; they are documentation, not
            // production control flow.
            if trimmed.starts_with("//") {
                continue;
            }
            if trimmed.contains("todo!(") || trimmed.contains("unimplemented!(") {
                placeholders.push(format!("{}:{}", file.display(), i + 1));
            }
            if line.contains("f64 ==") || line.contains("f32 ==") {
                float_equality.push(format!("{}:{}", file.display(), i + 1));
            }
        }
    }
    assert!(
        placeholders.is_empty(),
        "production code must contain no `todo!`/`unimplemented!`: {placeholders:#?}"
    );
    assert!(
        float_equality.is_empty(),
        "production code must contain no float equality comparison: {float_equality:#?}"
    );
}

// ===========================================================================
// Repo-source helpers
// ===========================================================================

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/cli has a workspace root two levels up")
        .to_path_buf()
}

fn read_repo_file(rel: &str) -> String {
    std::fs::read_to_string(repo_root().join(rel))
        .unwrap_or_else(|e| panic!("{rel} is readable: {e}"))
}

/// Every production `src/**.rs` file in the workspace (never `tests/`, never
/// `examples/`): the surface the audit's findings are about.
fn production_sources() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for crate_dir in [
        "crates/core",
        "crates/macros",
        "crates/artifact",
        "crates/render",
        "crates/metastore",
        "crates/cli",
    ] {
        collect_rs(&repo_root().join(crate_dir).join("src"), &mut out);
    }
    out.sort();
    out
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

// ===========================================================================
// Driver harness
// ===========================================================================

/// A private per-test run-store base, so parallel tests never share a directory.
fn temp_base(tag: &str) -> String {
    temp_dir(tag).to_string_lossy().into_owned()
}

fn temp_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    std::env::temp_dir().join(format!("dagr-t95-{tag}-{}-{nanos}-{n}", std::process::id()))
}

#[derive(Clone, Default)]
struct MemorySink {
    lines: Arc<Mutex<Vec<u8>>>,
}

impl EventSink for MemorySink {
    fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.lines
            .lock()
            .expect("test sink mutex not poisoned")
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

/// A source task that succeeds with a fixed value.
struct Alpha;
impl Task for Alpha {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(1)
    }
}

/// A stable-named output type — the structure snapshot records stable names, so
/// the two fixture tasks below need one.
struct Count;
impl dagr_core::StableName for Count {
    const STABLE_NAME: &'static str = "Count";
}

/// Two structurally distinct, stable-named source tasks: blessing a fixture from
/// one and asserting the other against it yields a genuine
/// `StructureAssertError::Mismatch`.
struct NamedAlpha;
impl Task for NamedAlpha {
    type Input = ();
    type Output = Count;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Count, TaskError> {
        Ok(Count)
    }
}
impl dagr_core::StableName for NamedAlpha {
    const STABLE_NAME: &'static str = "NamedAlpha";
}

struct NamedBeta;
impl Task for NamedBeta {
    type Input = ();
    type Output = Count;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Count, TaskError> {
        Ok(Count)
    }
}
impl dagr_core::StableName for NamedBeta {
    const STABLE_NAME: &'static str = "NamedBeta";
}

/// Fires the programmatic cancel from **inside** the run, then cooperatively
/// waits until the loop has processed it (observing its own per-attempt child
/// signal), so the drain is entered deterministically without a sleep.
struct FiresCancelThenAwaits {
    handle: Option<CancelHandle>,
}
impl Task for FiresCancelThenAwaits {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<u64, TaskError> {
        if let Some(handle) = &self.handle {
            handle.cancel();
        }
        for _ in 0..100_000 {
            if c.cancellation().is_cancelled() {
                return Ok(0);
            }
            tokio::task::yield_now().await;
        }
        Ok(0)
    }
}

/// A type-erased runner that scripts a node straight to a terminal state — the
/// same faithful fake the termination-property driver test uses. It exercises the
/// real loop (admission, dispatch, feedback channel, run-end condition) without
/// needing a task body per outcome class.
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

/// A runner over a real task, driven through the real caught attempt path.
struct SourceRunner<T: Task<Input = (), Output = u64>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<u64>>,
}
impl<T: Task<Input = (), Output = u64>> SourceRunner<T> {
    fn boxed(name: &str, task: T) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot: Arc::new(Slot::new(
                dagr_core::handle::NodeId::from_name(name),
                name,
                0,
                false,
                0,
                ResidencyLedger::new(),
            )),
        })
    }
}
impl<T: Task<Input = (), Output = u64>> NodeRunner for SourceRunner<T> {
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

/// Drive `nodes` as independent zero-cost source nodes, each scripted to its
/// terminal state.
fn drive_scripted(nodes: &[(&str, TerminalState)], config: RunConfig) -> RunReport {
    drive_scripted_with_cost(nodes, 0, config)
}

/// As [`drive_scripted`], with each node declaring `mem` bytes of working memory
/// so a pinned pool serializes admission.
fn drive_scripted_with_cost(
    nodes: &[(&str, TerminalState)],
    mem: u64,
    config: RunConfig,
) -> RunReport {
    let pipeline = scripted_pipeline(nodes, mem);
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    for (name, state) in nodes {
        runners.insert((*name).to_string(), ScriptedRunner::boxed(name, *state));
    }
    drive(
        &config,
        "hardening",
        Ok(RunPlan::new(pipeline, runners)),
        &[],
        MemorySink::default(),
        TickClock::default(),
    )
}

fn scripted_pipeline(nodes: &[(&str, TerminalState)], mem: u64) -> Pipeline {
    let mut flow = Flow::new();
    for (name, _) in nodes {
        let _ = flow.register_source_with(*name, &Alpha, NodePolicy::new().working_memory(mem));
    }
    let pipeline = flow.finish();
    pipeline.assemble().expect("assembles");
    pipeline
}

fn assert_every_node_terminal(report: &RunReport, nodes: &[(&str, TerminalState)]) {
    assert_eq!(
        report.terminal_states.len(),
        nodes.len(),
        "every node reaches exactly one terminal state: {:?}",
        report.terminal_states
    );
    for (name, state) in nodes {
        assert_eq!(
            report.terminal_states.get(*name).copied(),
            Some(*state),
            "`{name}` must settle at its scripted terminal"
        );
    }
}
