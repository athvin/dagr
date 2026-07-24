//! C26 · **Command-line contract** tests — ticket T55 (068). Written first, TDD.
//!
//! These exercise the library-owned verb surface every pipeline binary inherits
//! (arch.md `### C26`): the verb table, no-arg help, `validate` printing every
//! problem, `render` reachable from artifacts alone (with optional run overlay),
//! `fold` producing the interrupted artifact, the `resume` stub, the typed
//! parameter seam and reserved library-flag namespace, and the single-node
//! non-durable-input refusal that shares the resume-refusal exit code.
//!
//! Verb *parsing* and the verb *set* are asserted against the library
//! `dagr_cli::contract` surface, so two distinct pipelines built on the library
//! get the identical verb table by construction (the verbs are library-owned,
//! not per-pipeline).

use std::collections::BTreeMap;

use dagr_cli::contract::{
    fold_verb, parse_cli, render_verb, reserved_flag_names, validate_verb, verb_table, Cli,
    ExitCode, LibraryFlagCollision, ParamSpec, ParseOutcome, RenderFormat, Verb,
};
use dagr_core::flow::Flow;
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{NodePolicy, Pipeline, TaskError};

// ===========================================================================
// Two distinct pipelines — verb parity is library-owned, so both get the same
// table by construction.
// ===========================================================================

struct Rows;
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}

/// Pipeline A's single source, succeeds.
struct AlphaSource;
impl StableName for AlphaSource {
    const STABLE_NAME: &'static str = "alpha-source";
}
impl Task for AlphaSource {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

/// Pipeline B's single source — a *different* pipeline entirely.
struct BetaSource;
impl StableName for BetaSource {
    const STABLE_NAME: &'static str = "beta-source";
}
impl Task for BetaSource {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows)
    }
}

fn alpha() -> Pipeline {
    let mut flow = Flow::new();
    let _h = flow.register_source_named::<AlphaSource>("a", &AlphaSource, None::<String>, NodePolicy::new());
    flow.finish()
}

fn beta() -> Pipeline {
    let mut flow = Flow::new();
    let _h = flow.register_source_named::<BetaSource>("b", &BetaSource, None::<String>, NodePolicy::new());
    flow.finish()
}

// ===========================================================================
// Verb table + parity across pipelines
// ===========================================================================

/// The library verb table lists exactly the C26 verbs, in a fixed order — and it
/// is a library constant, so it is identical regardless of which pipeline hosts
/// it (verb parity is structural).
#[test]
fn the_verb_table_is_the_c26_set() {
    let names: Vec<&str> = verb_table().iter().map(|v| v.name()).collect();
    assert_eq!(
        names,
        vec![
            "graph",
            "validate",
            "render",
            "run",
            "single-node",
            "resume",
            "fold",
            "prune",
        ],
        "the verb table is exactly the C26 verbs, library-owned and order-stable"
    );
}

/// Two distinct pipelines get the identical verb table (it is library-owned, not
/// derived from the pipeline).
#[test]
fn two_pipelines_share_the_identical_verb_table() {
    // The verb table is pipeline-independent by construction; nonetheless assert
    // it holds while building two real distinct pipelines, mirroring the ticket's
    // "two distinct pipelines" scenario.
    let _a = alpha();
    let _b = beta();
    let table_once: Vec<&str> = verb_table().iter().map(Verb::name).collect();
    let table_twice: Vec<&str> = verb_table().iter().map(Verb::name).collect();
    assert_eq!(table_once, table_twice);
}

// ===========================================================================
// No-arg help
// ===========================================================================

/// Invoked with no arguments, the CLI prints the available verbs and exits
/// cleanly (success). The parse outcome is the dedicated "no-args help" outcome.
#[test]
fn no_arguments_prints_verbs_and_exits_success() {
    let outcome = parse_cli(["dagr"]);
    match outcome {
        ParseOutcome::Help { exit, text } => {
            assert_eq!(exit, ExitCode::Success, "no-arg help exits success");
            for verb in verb_table() {
                assert!(
                    text.contains(verb.name()),
                    "help text lists the `{}` verb",
                    verb.name()
                );
            }
        }
        other => panic!("no-arg invocation must print help, got {other:?}"),
    }
}

/// An unknown verb is an invalid-usage error (the invalid-usage exit code), not a
/// panic and not a silent success.
#[test]
fn an_unknown_verb_is_invalid_usage() {
    let outcome = parse_cli(["dagr", "no-such-verb"]);
    match outcome {
        ParseOutcome::Error { exit, .. } => {
            assert_eq!(exit, ExitCode::InvalidUsage, "unknown verb is invalid usage");
        }
        other => panic!("unknown verb must be an invalid-usage error, got {other:?}"),
    }
}

/// A recognized verb parses into the corresponding `Cli` selection.
#[test]
fn a_recognized_verb_parses() {
    match parse_cli(["dagr", "graph"]) {
        ParseOutcome::Parsed(Cli { verb: Verb::Graph, .. }) => {}
        other => panic!("`graph` must parse to the Graph verb, got {other:?}"),
    }
}

// ===========================================================================
// validate — prints EVERY problem, exits assembly-failure
// ===========================================================================

/// `validate` on a pipeline whose assembly succeeds exits success and prints no
/// problem.
#[test]
fn validate_on_a_good_pipeline_succeeds_with_no_problems() {
    let pipeline = alpha();
    let mut out = Vec::new();
    let exit = validate_verb(&pipeline, &mut out);
    assert_eq!(exit, ExitCode::Success);
    let text = String::from_utf8(out).unwrap();
    assert!(
        !text.to_lowercase().contains("problem"),
        "a clean validate reports no problems, got: {text}"
    );
}

/// `validate` on a pipeline with *two independent* assembly failures exits with
/// the assembly-failure code and prints **both** problems (not just the first) —
/// arch.md C7/C26.
#[test]
fn validate_prints_every_assembly_problem() {
    // Build a pipeline that fails assembly with two distinct problems: two nodes
    // each marked durable but whose output type lacks the durable contract
    // (`Rows` does not implement `DurableOutput`). Assembly reports all problems
    // it finds (C7), so both appear.
    let mut flow = Flow::new();
    let _a = flow.register_source_named::<AlphaSource>(
        "durable-a",
        &AlphaSource,
        None::<String>,
        NodePolicy::new().durable(true),
    );
    let _b = flow.register_source_named::<BetaSource>(
        "durable-b",
        &BetaSource,
        None::<String>,
        NodePolicy::new().durable(true),
    );
    let pipeline = flow.finish();
    // Precondition: this pipeline really does fail assembly with two problems.
    let err = pipeline.assemble().expect_err("two durable-without-contract nodes fail assembly");
    assert!(err.problems().len() >= 2, "two independent problems expected");

    let mut out = Vec::new();
    let exit = validate_verb(&pipeline, &mut out);
    assert_eq!(exit, ExitCode::AssemblyFailure, "validate exits assembly-failure");
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.contains("durable-a") && text.contains("durable-b"),
        "validate prints BOTH problems (not just the first), got: {text}"
    );
}

// ===========================================================================
// render — reachable from artifacts alone, with optional run overlay
// ===========================================================================

/// `render` produces diagram source from a graph artifact with no live pipeline —
/// proving the renderer (C24) is reachable purely from artifacts.
#[test]
fn render_produces_diagram_from_a_graph_artifact_alone() {
    let pipeline = alpha();
    // Produce the graph artifact through the real emitter (the library `graph`
    // verb path), then render it from the bytes alone.
    let graph_json =
        dagr_cli::graph::emit_graph(&pipeline, "a-pipeline", "2026-07-24T00:00:00Z", &dagr_cli::graph::BuildProvenance::embedded())
            .expect("emits");

    let mut out = Vec::new();
    let exit = render_verb(graph_json.as_bytes(), None, RenderFormat::Dot, &mut out);
    assert_eq!(exit, ExitCode::Success);
    let dot = String::from_utf8(out).unwrap();
    assert!(dot.contains("digraph"), "DOT diagram source produced, got: {dot}");
    assert!(dot.contains('a'), "the node appears in the diagram");
}

/// `render` given a run artifact to overlay colours nodes by terminal state —
/// the run-overlay path (C24), still from artifacts only.
#[test]
fn render_with_a_run_overlay_colours_nodes_by_state() {
    let pipeline = alpha();
    let graph_json =
        dagr_cli::graph::emit_graph(&pipeline, "a-pipeline", "2026-07-24T00:00:00Z", &dagr_cli::graph::BuildProvenance::embedded())
            .expect("emits");

    // A minimal run artifact overlaying node `a` as succeeded, produced by folding
    // a tiny event stream (the real C22 fold).
    let run_artifact = fold_tiny_success_run("a");

    let mut out = Vec::new();
    let exit = render_verb(
        graph_json.as_bytes(),
        Some(run_artifact.as_bytes()),
        RenderFormat::Dot,
        &mut out,
    );
    assert_eq!(exit, ExitCode::Success);
    let dot = String::from_utf8(out).unwrap();
    // The overlay marks nodes with `style="filled"` and a per-state fill colour;
    // the base renderer never fills nodes, so its presence proves the overlay ran.
    assert!(
        dot.contains("filled"),
        "the run overlay fills nodes by terminal state, got: {dot}"
    );
}

/// A malformed graph artifact is refused with an invalid-usage exit (a clear
/// diagnostic), never a partial diagram.
#[test]
fn render_refuses_a_malformed_graph_artifact() {
    let mut out = Vec::new();
    let exit = render_verb(b"{ not a graph artifact", None, RenderFormat::Dot, &mut out);
    assert_eq!(exit, ExitCode::InvalidUsage, "a malformed artifact is refused");
}

// ===========================================================================
// fold — the crashed-run path
// ===========================================================================

/// `fold` on a crash-truncated stream (no `run-finished`) produces the
/// interrupted run artifact — the standalone C22/T42 function wired as a verb.
#[test]
fn fold_produces_the_interrupted_artifact_from_a_crashed_stream() {
    // A stream that starts a run but is killed before `run-finished`.
    let stream = crashed_stream();
    let mut out = Vec::new();
    let exit = fold_verb(stream.as_bytes(), &["a".to_string()], &mut out);
    assert_eq!(exit, ExitCode::Success, "fold exits cleanly");
    let artifact = String::from_utf8(out).unwrap();
    let value: serde_json::Value = serde_json::from_str(&artifact).expect("fold output is JSON");
    // The crash clause: the folded artifact is flagged interrupted (matching the
    // standalone function's output, T42/T68).
    assert_eq!(
        value.get("interrupted").and_then(serde_json::Value::as_bool),
        Some(true),
        "a crash-truncated stream folds to an interrupted artifact"
    );
}

// ===========================================================================
// resume — recognized, stubbed, defined exit
// ===========================================================================

/// The `resume` verb is recognized and parses (its surface exists so T58 can
/// replace the body without changing the surface).
#[test]
fn resume_is_a_recognized_verb() {
    match parse_cli(["dagr", "resume", "some-run-id"]) {
        ParseOutcome::Parsed(Cli { verb: Verb::Resume, .. }) => {}
        other => panic!("`resume` must be recognized, got {other:?}"),
    }
    // It is listed in the verb table.
    assert!(verb_table().iter().any(|v| v.name() == "resume"));
}

/// The stubbed `resume` verb reports "not yet implemented" and exits with the
/// resume-refusal code — a defined code so T58 replaces only the body.
#[test]
fn resume_stub_reports_not_yet_implemented_with_a_defined_code() {
    let mut out = Vec::new();
    let exit = dagr_cli::contract::resume_verb_stub(&mut out);
    assert_eq!(exit, ExitCode::ResumeRefusal, "the resume stub uses the resume-refusal code");
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.to_lowercase().contains("not yet implemented"),
        "the stub says it is not yet implemented, got: {text}"
    );
}

// ===========================================================================
// Typed parameters + reserved library-flag namespace
// ===========================================================================

/// A pipeline-declared parameter whose flag name lands in the reserved
/// library-flag namespace is a **named, hard collision error** — the run does not
/// proceed (arch.md C26).
#[test]
fn a_parameter_colliding_with_a_library_flag_is_a_named_error() {
    // Pick a genuinely reserved library flag name.
    let reserved = reserved_flag_names();
    assert!(!reserved.is_empty(), "the library reserves at least one flag name");
    let collide = reserved[0];

    let params = vec![ParamSpec::new(collide, "a pipeline parameter that shadows a library flag")];
    let result = dagr_cli::contract::check_reserved_collision(&params);
    match result {
        Err(LibraryFlagCollision { flag }) => {
            assert_eq!(flag, collide, "the collision names the offending flag");
        }
        Ok(()) => panic!("a parameter colliding with `{collide}` must be a hard error"),
    }
}

/// A pipeline parameter whose name is NOT reserved passes the collision check.
#[test]
fn a_non_colliding_parameter_is_accepted() {
    let params = vec![ParamSpec::new("region", "an ordinary pipeline parameter")];
    assert!(dagr_cli::contract::check_reserved_collision(&params).is_ok());
}

/// A typed parameter value that fails validation is rejected with the
/// invalid-usage code (rejected at bootstrap, before any node executes — the
/// no-node-executed half is covered by the run-verb integration test).
#[test]
fn an_invalid_parameter_value_is_invalid_usage() {
    // A parameter declared as an integer, given a non-integer value.
    let params = vec![ParamSpec::int("count", "an integer parameter")];
    let mut supplied = BTreeMap::new();
    supplied.insert("count".to_string(), "not-a-number".to_string());
    let result = dagr_cli::contract::validate_params(&params, &supplied);
    match result {
        Err(exit) => assert_eq!(exit, ExitCode::InvalidUsage),
        Ok(_) => panic!("a non-integer value for an int parameter must be invalid usage"),
    }
}

/// Valid typed parameters and a data interval are carried verbatim (the values
/// the run verb records into the artifact header, C22).
#[test]
fn valid_parameters_and_interval_are_carried_verbatim() {
    let params = vec![ParamSpec::new("region", "a region")];
    let mut supplied = BTreeMap::new();
    supplied.insert("region".to_string(), "eu-west".to_string());
    let carried = dagr_cli::contract::validate_params(&params, &supplied).expect("valid");
    assert_eq!(
        carried.get("region").map(String::as_str),
        Some("eu-west"),
        "the parameter value is carried verbatim into the header map"
    );
}

// ===========================================================================
// single-node — the non-durable-input refusal shares the resume-refusal code
// ===========================================================================

/// A `single-node` replay whose requested input is not durable refuses with a
/// message naming which input and why, and exits with the resume-refusal code
/// (shared with resume refusal).
#[test]
fn single_node_refuses_a_non_durable_input_with_the_resume_refusal_code() {
    // A prior run artifact in which node `consumer` consumed `producer`, but
    // `producer`'s attempt recorded NO durable reference (an in-memory output).
    let prior = prior_run_with_non_durable_producer();
    let mut out = Vec::new();
    let exit = dagr_cli::contract::single_node_refusal_check(
        prior.as_bytes(),
        "consumer",
        &["producer".to_string()],
        &mut out,
    );
    assert_eq!(
        exit,
        Some(ExitCode::ResumeRefusal),
        "a non-durable input refuses with the resume-refusal code"
    );
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.contains("producer"),
        "the refusal names the offending input, got: {text}"
    );
}

/// A `single-node` replay whose requested inputs ARE durable does not refuse on
/// the durability check (returns no refusal).
#[test]
fn single_node_accepts_a_durable_input() {
    let prior = prior_run_with_durable_producer();
    let mut out = Vec::new();
    let exit = dagr_cli::contract::single_node_refusal_check(
        prior.as_bytes(),
        "consumer",
        &["producer".to_string()],
        &mut out,
    );
    assert_eq!(exit, None, "a durable input passes the durability check");
}

// ===========================================================================
// Test-support: tiny event streams and folded artifacts
// ===========================================================================

/// Fold a tiny one-node succeeded run stream into a run-artifact JSON string.
fn fold_tiny_success_run(node: &str) -> String {
    let stream = success_stream(node);
    let artifact =
        dagr_artifact::fold::fold_stream(stream.as_bytes(), &[node.to_string()]).expect("folds");
    artifact.to_canonical_json()
}

/// A complete one-node succeeded event stream (JSON-Lines), hand-built so the
/// fold has real records to derive from.
fn success_stream(node: &str) -> String {
    use dagr_artifact::event_stream::{
        AttemptOutcomeRecord, EventStreamWriter, MonotonicClock, RunId, RunOutcome,
        RunStartedHeader, TerminalState, FINGERPRINT_ALGORITHM_VERSION,
    };

    #[derive(Default)]
    struct Buf(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);
    impl dagr_artifact::event_stream::EventSink for Buf {
        fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
            self.0.borrow_mut().extend_from_slice(line);
            Ok(())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct StepClock(std::cell::Cell<u64>);
    impl MonotonicClock for StepClock {
        fn elapsed_ns(&self) -> u64 {
            let n = self.0.get();
            self.0.set(n + 1);
            n
        }
    }

    let bytes = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = Buf(std::rc::Rc::clone(&bytes));
    let mut writer = EventStreamWriter::new(sink, StepClock(std::cell::Cell::new(0)), RunId::from_operator("run-1"), "p");
    let _ = writer.run_started(RunStartedHeader {
        pipeline: "p".into(),
        fingerprint_structural: None,
        fingerprint_policy: None,
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: BTreeMap::new(),
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    });
    let _ = writer.node_ready(node);
    let _ = writer.node_admitted(node);
    let _ = writer.attempt_started(node, 1);
    let _ = writer.attempt_succeeded(node, 1);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord {
        node: node.into(),
        attempt: 1,
        status: TerminalState::Succeeded.as_str().into(),
        ..AttemptOutcomeRecord::default()
    });
    let _ = writer.node_terminal(node, TerminalState::Succeeded);
    let _ = writer.run_finished(RunOutcome::Succeeded);
    let _ = writer.finish();
    let out = bytes.borrow().clone();
    String::from_utf8(out).unwrap()
}

/// A crash-truncated stream: a run that started but never finished (no
/// `run-finished`). The fold flags it interrupted.
fn crashed_stream() -> String {
    let full = success_stream("a");
    // Drop the last two records (node-terminal + run-finished) so the stream ends
    // without a run-finished — the crash clause.
    let mut lines: Vec<&str> = full.lines().collect();
    lines.truncate(lines.len().saturating_sub(2));
    let mut s = lines.join("\n");
    s.push('\n');
    s
}

/// A prior run artifact in which `producer` recorded NO durable reference
/// (in-memory output) and `consumer` consumed it. Built by folding a stream.
fn prior_run_with_non_durable_producer() -> String {
    two_node_prior(false)
}

/// A prior run artifact in which `producer` recorded a durable reference.
fn prior_run_with_durable_producer() -> String {
    two_node_prior(true)
}

/// Fold a two-node (`producer` → `consumer`) succeeded stream; `producer`'s
/// attempt records a durable reference iff `durable`.
fn two_node_prior(durable: bool) -> String {
    use dagr_artifact::event_stream::{
        record_durable_reference, AttemptOutcomeRecord, EventStreamWriter, MonotonicClock, RunId,
        RunOutcome, RunStartedHeader, TerminalState, FINGERPRINT_ALGORITHM_VERSION,
    };

    #[derive(Default)]
    struct Buf(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);
    impl dagr_artifact::event_stream::EventSink for Buf {
        fn append_line(&mut self, line: &[u8]) -> std::io::Result<()> {
            self.0.borrow_mut().extend_from_slice(line);
            Ok(())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct StepClock(std::cell::Cell<u64>);
    impl MonotonicClock for StepClock {
        fn elapsed_ns(&self) -> u64 {
            let n = self.0.get();
            self.0.set(n + 1);
            n
        }
    }

    let bytes = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = Buf(std::rc::Rc::clone(&bytes));
    let mut writer = EventStreamWriter::new(sink, StepClock(std::cell::Cell::new(0)), RunId::from_operator("run-1"), "p");
    let _ = writer.run_started(RunStartedHeader {
        pipeline: "p".into(),
        fingerprint_structural: None,
        fingerprint_policy: None,
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: BTreeMap::new(),
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    });
    // producer
    let _ = writer.node_ready("producer");
    let _ = writer.node_admitted("producer");
    let _ = writer.attempt_started("producer", 1);
    let _ = writer.attempt_succeeded("producer", 1);
    let mut prod = AttemptOutcomeRecord {
        node: "producer".into(),
        attempt: 1,
        status: TerminalState::Succeeded.as_str().into(),
        ..AttemptOutcomeRecord::default()
    };
    if durable {
        record_durable_reference(&mut prod, Some("s3://bucket/producer-output".into()));
    }
    let _ = writer.attempt_outcome(prod);
    let _ = writer.node_terminal("producer", TerminalState::Succeeded);
    // consumer
    let _ = writer.node_ready("consumer");
    let _ = writer.node_admitted("consumer");
    let _ = writer.attempt_started("consumer", 1);
    let _ = writer.attempt_succeeded("consumer", 1);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord {
        node: "consumer".into(),
        attempt: 1,
        status: TerminalState::Succeeded.as_str().into(),
        ..AttemptOutcomeRecord::default()
    });
    let _ = writer.node_terminal("consumer", TerminalState::Succeeded);
    let _ = writer.run_finished(RunOutcome::Succeeded);
    let _ = writer.finish();
    let out = bytes.borrow().clone();
    let stream = String::from_utf8(out).unwrap();
    let artifact = dagr_artifact::fold::fold_stream(
        stream.as_bytes(),
        &["producer".to_string(), "consumer".to_string()],
    )
    .expect("folds");
    artifact.to_canonical_json()
}
