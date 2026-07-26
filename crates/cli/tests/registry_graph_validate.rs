//! `run_registry` routing of the remaining flow-selecting verbs — `graph` and
//! `validate` — plus the single-flow ergonomic default and per-verb exit-code
//! fidelity. Ticket T75 (088), written first (TDD) — arch.md `### C20`, `### C26`;
//! ADR 086; graph verb T40.
//!
//! T74 (087) shipped the `FlowRegistry` type, the `Cli.flow_name` positional, and
//! the `run`/`list` verbs. This slice routes the *pipeline-bound* verbs that select
//! a flow **by re-invoking the selected factory once per verb**: `graph <flow>`
//! builds a fresh `RunnableFlow`, finishes it into a live `Pipeline`, and emits the
//! C20 graph artifact via `graph_verb` (byte-identical to a single-flow binary);
//! `validate <flow>` builds another, runs assembly (C7) only, and exits `Success`
//! on a clean assembly or `AssemblyFailure` (3) printing every problem. Each verb
//! maps its **own** `ExitCode`/error type — never through `exit_code_for_run`,
//! which is reserved for a completed `RunReport`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dagr_cli::contract::ExitCode;
use dagr_cli::graph::{graph_verb, mask_generated_at};
use dagr_cli::registry::{run_registry_to, FlowRegistry};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// Test tasks — two distinct stable-name-aware flows named `etl` and `analytics`.
// A `RunnableFlow` whose nodes carry author-declared stable names is emittable to
// the C20 graph contract (a type-erased node is not, T40) — so the flows the
// registry routes `graph` to register through the stable-name-aware surface.
// ===========================================================================

/// A row count payload with a declared stable name (so the graph edge carrying it
/// records that name).
#[derive(Clone)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// A report payload with a declared stable name.
#[derive(Clone)]
struct Report(u64);
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

/// The `etl` source: produces some rows. Stable-named so it is graph-emittable.
struct Extract {
    rows: u64,
}
impl StableName for Extract {
    const STABLE_NAME: &'static str = "Extract";
}
impl Task for Extract {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows(self.rows))
    }
}

/// The `etl` sink: consumes rows and produces a report.
struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "Load";
}
impl Task for Load {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, input: Rows) -> Result<Report, TaskError> {
        Ok(Report(input.0 * 2))
    }
}

/// The `analytics` source: a single stable-named node with a *different* shape
/// (one node, distinct stable task name) so its graph differs from `etl`'s.
struct Aggregate {
    seed: u64,
}
impl StableName for Aggregate {
    const STABLE_NAME: &'static str = "Aggregate";
}
impl Task for Aggregate {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Ok(Report(self.seed))
    }
}

/// Build a two-node `etl` flow (`extract -> load`) using the stable-name-aware
/// registration surface, so it is emittable to the C20 graph contract.
fn build_etl() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let rows = flow.register_source_named("extract", Extract { rows: 3 });
    let _report = flow.register_named::<Load, _>("load", Load, rows);
    flow
}

/// Build a one-node `analytics` flow (`aggregate`), a distinct graph shape.
fn build_analytics() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _report = flow.register_source_named("aggregate", Aggregate { seed: 7 });
    flow
}

/// A flow that does **not** assemble: two nodes registered under the *same*
/// identity name is a duplicate-name assembly failure (C7). Used to prove
/// `validate` reports the failure through its own `AssemblyFailure` (3) code.
struct Effect;
impl StableName for Effect {
    const STABLE_NAME: &'static str = "Effect";
}
impl Task for Effect {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}
fn build_broken() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _a = flow.register_source_named("dup", Effect);
    let _b = flow.register_source_named("dup", Effect);
    flow
}

/// Dispatch `run_registry` while capturing what it writes (the `graph` artifact,
/// the `validate` report, or a selection diagnostic).
fn run_capturing<I, T>(registry: &FlowRegistry, argv: I) -> (ExitCode, Vec<u8>)
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let mut buf: Vec<u8> = Vec::new();
    let code = run_registry_to(registry, argv, &mut buf);
    (code, buf)
}

/// Parse the captured artifact bytes as JSON with the generation-time field masked,
/// so two emissions compare byte-identical outside that single varying field (C20).
fn masked_json(bytes: &[u8]) -> serde_json::Value {
    let text = String::from_utf8(bytes.to_vec()).expect("graph artifact is UTF-8");
    let value: serde_json::Value = serde_json::from_str(text.trim_end())
        .expect("the graph verb emits a single canonical JSON artifact");
    mask_generated_at(value)
}

// ===========================================================================
// graph — routes to the selected flow, emits the C20 artifact.
// ===========================================================================

/// `graph etl` on a multi-flow registry selects `etl`, `graph_verb` emits `etl`'s
/// graph artifact, and the process exits `Success`. The emitted artifact is
/// byte-identical (outside generation time) to what a single-flow `etl` binary
/// emits for that same flow — proving the registry routes to the *same* verb over
/// the *same* pipeline.
#[test]
fn graph_named_flow_emits_its_artifact() {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("analytics", build_analytics);
    let (code, bytes) = run_capturing(&registry, ["dagr", "graph", "etl"]);
    assert_eq!(code, ExitCode::Success, "graph of a well-formed flow succeeds");

    // What a single-flow `etl` binary emits for the same flow: build the same flow,
    // finish it into a pipeline, and run the *same* `graph_verb`.
    let pipeline = build_etl().into_pipeline();
    let mut want = Vec::new();
    graph_verb(&pipeline, "etl", "1970-01-01T00:00:00Z", &mut want).expect("graph emits");

    assert_eq!(
        masked_json(&bytes),
        masked_json(&want),
        "graph etl emits etl's artifact, byte-identical (outside generation time) to a \
         single-flow etl binary's emission"
    );
    // The artifact really is etl's (its two nodes are present).
    let got = masked_json(&bytes);
    let node_names: Vec<&str> = got["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .map(|n| n["name"].as_str().expect("node name"))
        .collect();
    assert!(
        node_names.contains(&"extract") && node_names.contains(&"load"),
        "etl's graph carries its two nodes, got {node_names:?}"
    );
}

/// `graph analytics` emits *analytics'* graph, distinct from `etl`'s — the two
/// flows route to their own factories.
#[test]
fn graph_selects_the_right_flow() {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("analytics", build_analytics);
    let (etl_code, etl_bytes) = run_capturing(&registry, ["dagr", "graph", "etl"]);
    let (an_code, an_bytes) = run_capturing(&registry, ["dagr", "graph", "analytics"]);
    assert_eq!(etl_code, ExitCode::Success);
    assert_eq!(an_code, ExitCode::Success);
    assert_ne!(
        masked_json(&etl_bytes),
        masked_json(&an_bytes),
        "the two flows emit distinct graph artifacts"
    );
}

/// A single-flow registry lets the name be **omitted**: `graph` with no name emits
/// the sole flow's graph (the single-flow ergonomic default).
#[test]
fn single_flow_graph_omits_the_name() {
    let registry = FlowRegistry::single_flow(build_etl);
    let (code, bytes) = run_capturing(&registry, ["dagr", "graph"]);
    assert_eq!(
        code,
        ExitCode::Success,
        "a single-flow registry serves graph with the name omitted"
    );
    let got = masked_json(&bytes);
    assert!(
        got["nodes"].as_array().map(Vec::len) == Some(2),
        "the sole flow's two-node graph was emitted"
    );
}

/// A multi-flow registry with **no** name on `graph` → `InvalidUsage` (2), message
/// lists the available flows; `graph unknown` → `InvalidUsage` (2), message lists
/// the available flows. A malformed selection never reaches the flow build.
#[test]
fn graph_selection_errors_are_invalid_usage() {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("analytics", build_analytics);

    let (no_name_code, no_name_out) = run_capturing(&registry, ["dagr", "graph"]);
    assert_eq!(
        no_name_code,
        ExitCode::InvalidUsage,
        "a multi-flow registry needs a name on graph"
    );
    let no_name = String::from_utf8_lossy(&no_name_out);
    assert!(
        no_name.contains("etl") && no_name.contains("analytics"),
        "the name-required message lists the available flows, got: {no_name}"
    );

    let (unknown_code, unknown_out) = run_capturing(&registry, ["dagr", "graph", "nope"]);
    assert_eq!(
        unknown_code,
        ExitCode::InvalidUsage,
        "an unknown flow name is invalid usage on graph"
    );
    let unknown = String::from_utf8_lossy(&unknown_out);
    assert!(
        unknown.contains("etl") && unknown.contains("analytics"),
        "the unknown-name message lists the available flows, got: {unknown}"
    );
}

// ===========================================================================
// validate — assembly only, prints every problem, per-verb exit code.
// ===========================================================================

/// `validate analytics` builds and assembles the `analytics` flow and exits
/// `Success` on a clean assembly.
#[test]
fn validate_clean_flow_succeeds() {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("analytics", build_analytics);
    let (code, out) = run_capturing(&registry, ["dagr", "validate", "analytics"]);
    assert_eq!(
        code,
        ExitCode::Success,
        "a clean assembly exits Success from validate"
    );
    assert!(
        String::from_utf8_lossy(&out).to_lowercase().contains("valid")
            || String::from_utf8_lossy(&out)
                .to_lowercase()
                .contains("succeeded"),
        "validate reports the clean assembly"
    );
}

/// `validate <broken>` — a flow that fails assembly (a duplicate node name) — exits
/// `AssemblyFailure` (3) **directly** through the verb's own code, independent of
/// any `RunReport`/`exit_code_for_run` path, and prints **every** problem.
#[test]
fn validate_broken_flow_is_assembly_failure_printing_problems() {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("broken", build_broken);
    let (code, out) = run_capturing(&registry, ["dagr", "validate", "broken"]);
    assert_eq!(
        code,
        ExitCode::AssemblyFailure,
        "a flow that fails assembly exits AssemblyFailure (3) directly from validate"
    );
    let text = String::from_utf8_lossy(&out);
    assert!(
        text.to_lowercase().contains("problem") || text.contains("dup"),
        "validate prints the assembly problem(s), got: {text}"
    );
}

// ===========================================================================
// Once-per-verb factory re-invocation — no consumed flow is ever reused.
// ===========================================================================

/// `graph etl` then `validate etl` in succession invoke the `etl` factory **once
/// per verb** — a fresh `RunnableFlow` each time. `RunnableFlow::run`/`into_pipeline`
/// consumes the flow, so reuse is impossible; re-invocation is the only pattern.
#[test]
fn each_verb_re_invokes_the_factory_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_factory = Arc::clone(&calls);
    let registry = FlowRegistry::new().add("etl", move || {
        calls_for_factory.fetch_add(1, Ordering::SeqCst);
        build_etl()
    });

    let (g, _) = run_capturing(&registry, ["dagr", "graph", "etl"]);
    let (v, _) = run_capturing(&registry, ["dagr", "validate", "etl"]);
    assert_eq!(g, ExitCode::Success);
    assert_eq!(v, ExitCode::Success);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the factory is re-invoked once per verb (a fresh flow for graph, another for validate)"
    );
}

// ===========================================================================
// The multi_flow example binary — the copyable two-flow authoring pattern.
// ===========================================================================

/// Locate the repository root from this crate's manifest directory.
fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/cli has a two-level ancestor (the repo root)")
        .to_path_buf()
}

/// Run the compiled `multi_flow` example with `args`, returning (success, stdout).
fn run_example(args: &[&str]) -> (bool, String) {
    let out = std::process::Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .args([
            "run",
            "--quiet",
            "-p",
            "dagr-cli",
            "--example",
            "multi_flow",
            "--",
        ])
        .args(args)
        .output()
        .expect("failed to spawn `cargo run --example multi_flow`");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

/// `crates/cli/examples/multi_flow.rs` registers two flows (`etl`, `analytics`) and
/// dispatches via `run_registry`. `graph etl` and `graph analytics` each emit the
/// corresponding flow's graph artifact, `validate etl` reports its assembly result,
/// and `list` prints both registered names.
#[test]
fn multi_flow_example_dispatches_graph_validate_and_list() {
    // `graph etl` → etl's graph artifact (its nodes present).
    let (ok, etl_graph) = run_example(&["graph", "etl"]);
    assert!(ok, "graph etl exits success, got:\n{etl_graph}");
    let etl: serde_json::Value =
        serde_json::from_str(etl_graph.trim()).expect("graph etl emits a JSON artifact");
    let etl_nodes: Vec<&str> = etl["nodes"]
        .as_array()
        .expect("etl nodes")
        .iter()
        .map(|n| n["name"].as_str().unwrap_or_default())
        .collect();
    assert!(
        etl_nodes.contains(&"extract") && etl_nodes.contains(&"load"),
        "graph etl carries etl's nodes, got {etl_nodes:?}"
    );

    // `graph analytics` → a distinct artifact.
    let (ok2, an_graph) = run_example(&["graph", "analytics"]);
    assert!(ok2, "graph analytics exits success, got:\n{an_graph}");
    let an: serde_json::Value =
        serde_json::from_str(an_graph.trim()).expect("graph analytics emits a JSON artifact");
    assert_ne!(
        mask_generated_at(etl.clone()),
        mask_generated_at(an),
        "the two flows emit distinct graphs from the example"
    );

    // `validate etl` reports etl's assembly result and exits success.
    let (ok3, validate_out) = run_example(&["validate", "etl"]);
    assert!(ok3, "validate etl exits success, got:\n{validate_out}");
    assert!(
        validate_out.to_lowercase().contains("valid")
            || validate_out.to_lowercase().contains("succeeded"),
        "validate etl reports the assembly result, got: {validate_out}"
    );

    // `list` prints both registered names.
    let (ok4, list_out) = run_example(&["list"]);
    assert!(ok4, "list exits success, got:\n{list_out}");
    assert!(
        list_out.contains("etl") && list_out.contains("analytics"),
        "list prints both registered names, got: {list_out}"
    );
}
