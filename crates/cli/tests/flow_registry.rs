//! `FlowRegistry` + `dagr run <flow>` / `list` dispatch tests.
//! Written first, TDD.
//!
//! These exercise the many-flows-per-binary registry: a builder
//! mapping a flow **name → a re-invokable factory `Fn() -> RunnableFlow`**, the
//! additive `Cli.flow_name` positional the command-line contract gains, and the two verbs
//! `run_registry` routes in this slice — `list` and `run <flow>`. Every scenario
//! here is a named case: named selection, the single-flow ergonomic
//! default, the "name required (…)" and unknown-name `InvalidUsage` messages,
//! `list`, factory re-invocation (independence), and the backward-compatible
//! `flow_name = None` parse of every existing verb/flag.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dagr_cli::contract::{parse_cli, Cli, ExitCode, ParseOutcome, Verb};
use dagr_cli::registry::{run_registry, FlowRegistry};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

// ===========================================================================
// Test tasks — two trivial single-node flows named `etl` and `nightly`.
// ===========================================================================

/// A no-input source producing a fixed value; succeeds on its first try.
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

/// Build a one-node `etl` flow.
fn build_etl() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _h = flow.register_source("start", Source { value: 1 });
    flow
}

/// Build a one-node `nightly` flow.
fn build_nightly() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _h = flow.register_source("start", Source { value: 2 });
    flow
}

/// A unique run-store base under the OS temp dir, so concurrent test binaries
/// never collide and each test's stores are inspectable in isolation.
fn temp_base(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "dagr-t74-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    dir.to_string_lossy().into_owned()
}

// ===========================================================================
// Registry & selection
// ===========================================================================

/// `run etl --store DIR` parses to `verb = Run`, `flow_name = Some("etl")`, and
/// `run_registry` builds the `etl` flow via its factory and drives it (a clean
/// single-source run exits `Success`).
#[test]
fn run_named_flow_selects_and_drives_it() {
    // The parse half: the first positional after `run` becomes the flow name.
    match parse_cli(["dagr", "run", "etl", "--store", "/tmp/x"]) {
        ParseOutcome::Parsed(Cli {
            verb: Verb::Run,
            flow_name,
        }) => assert_eq!(flow_name.as_deref(), Some("etl")),
        other => panic!("`run etl` must parse to Run with flow_name=etl, got {other:?}"),
    }

    // The dispatch half: `run etl` builds and drives the etl flow.
    let base = temp_base("select");
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("nightly", build_nightly);
    let code = run_registry(&registry, ["dagr", "run", "etl", "--store", base.as_str()]);
    assert_eq!(
        code,
        ExitCode::Success,
        "a clean single-source `etl` run exits Success"
    );
    // The etl flow really ran: its event stream is on disk under the store.
    assert!(
        run_store_has_a_stream(&base, "etl"),
        "the etl flow wrote an event stream under {base}/etl/<run-id>/"
    );
}

/// A single-flow registry (`FlowRegistry::single_flow`) dispatches its one flow on
/// `run` with **no** name — the ergonomic default for the common one-flow binary.
#[test]
fn single_flow_registry_runs_with_no_name() {
    let base = temp_base("single");
    let registry = FlowRegistry::single_flow(build_etl);
    let code = run_registry(&registry, ["dagr", "run", "--store", base.as_str()]);
    assert_eq!(
        code,
        ExitCode::Success,
        "a single-flow registry serves `run` with the name omitted"
    );
    // The sole flow ran even though no name was given. A single-flow registry
    // dispatches under the synthetic `flow` identity (the operator never types a
    // name), so its stream lives under `<base>/flow/<run-id>/`.
    assert!(
        run_store_has_a_stream(&base, "flow"),
        "the sole flow ran (under the single-flow `flow` identity) even with no name given"
    );
}

/// A multi-flow registry with **no** name on `run` → `InvalidUsage`, and the
/// message lists the registered names.
#[test]
fn multi_flow_run_with_no_name_is_invalid_usage_listing_names() {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("nightly", build_nightly);
    let (code, message) = run_registry_capturing(&registry, ["dagr", "run"]);
    assert_eq!(
        code,
        ExitCode::InvalidUsage,
        "a multi-flow registry needs a name on `run`"
    );
    assert!(
        message.contains("etl") && message.contains("nightly"),
        "the `name required` message lists the available flows, got: {message}"
    );
}

/// `run unknown` on a multi-flow registry → `InvalidUsage`, message lists the
/// available flows (`etl`, `nightly`).
#[test]
fn unknown_flow_name_is_invalid_usage_listing_available() {
    let registry = FlowRegistry::new()
        .add("etl", build_etl)
        .add("nightly", build_nightly);
    let (code, message) = run_registry_capturing(&registry, ["dagr", "run", "unknown"]);
    assert_eq!(
        code,
        ExitCode::InvalidUsage,
        "an unknown flow name is invalid usage"
    );
    assert!(
        message.contains("etl") && message.contains("nightly"),
        "the unknown-name message lists the available flows, got: {message}"
    );
}

// ===========================================================================
// list
// ===========================================================================

/// `list` prints every registered flow name (deterministic order) and exits
/// `Success`.
#[test]
fn list_prints_the_registered_names() {
    let registry = FlowRegistry::new()
        .add("a", build_etl)
        .add("b", build_nightly);
    let (code, out) = run_registry_capturing(&registry, ["dagr", "list"]);
    assert_eq!(code, ExitCode::Success, "list exits Success");
    assert!(
        out.contains('a') && out.contains('b'),
        "list prints both registered names, got: {out}"
    );
}

// ===========================================================================
// Independence & factories
// ===========================================================================

/// The same flow run twice through `run_registry` invokes its factory **once per
/// invocation** (a fresh `RunnableFlow` each time), and each invocation produces
/// its own run identity + its own store directory — no shared state.
#[test]
fn each_run_re_invokes_the_factory_with_its_own_identity() {
    let base = temp_base("independence");
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_factory = Arc::clone(&calls);
    // A factory closure that records each call, proving re-invocation (not reuse of
    // a single consumed flow — `RunnableFlow::run(self)` consumes it, so reuse is
    // impossible and re-invocation is the only correct pattern).
    let registry = FlowRegistry::new().add("etl", move || {
        calls_for_factory.fetch_add(1, Ordering::SeqCst);
        build_etl()
    });

    let c1 = run_registry(&registry, ["dagr", "run", "etl", "--store", base.as_str()]);
    let c2 = run_registry(&registry, ["dagr", "run", "etl", "--store", base.as_str()]);
    assert_eq!(c1, ExitCode::Success);
    assert_eq!(c2, ExitCode::Success);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the factory is re-invoked once per `run` (a fresh flow each time)"
    );

    // Two distinct run stores exist: each invocation minted its own run id, so the
    // two runs wrote disjoint `<base>/etl/<run-id>/` directories (concurrent-run
    // disjointness — no shared state between invocations).
    let run_dirs = run_id_dirs(&base, "etl");
    assert_eq!(
        run_dirs, 2,
        "two invocations produced two distinct run-id directories, got {run_dirs}"
    );
}

// ===========================================================================
// Backward compatibility — the new positional is optional and non-breaking.
// ===========================================================================

/// Every existing verb parses unchanged and `Cli.flow_name` is `None` whenever no
/// positional flow name is supplied — the new positional is optional and the
/// contract stays backward-compatible.
#[test]
fn existing_verbs_parse_with_flow_name_none() {
    for verb in [
        "graph",
        "validate",
        "render",
        "run",
        "single-node",
        "resume",
        "fold",
        "prune",
    ] {
        match parse_cli(["dagr", verb]) {
            ParseOutcome::Parsed(Cli { flow_name, .. }) => assert_eq!(
                flow_name, None,
                "`{verb}` with no positional leaves flow_name None"
            ),
            other => panic!("`{verb}` must still parse, got {other:?}"),
        }
    }
}

/// A library flag passed to a verb with no positional keeps `flow_name = None`
/// (a leading `--flag` is never mistaken for a positional flow name).
#[test]
fn a_flag_is_not_mistaken_for_a_flow_name() {
    match parse_cli(["dagr", "run", "--store", "/tmp/x"]) {
        ParseOutcome::Parsed(Cli {
            verb: Verb::Run,
            flow_name,
        }) => assert_eq!(
            flow_name, None,
            "a leading flag is not a positional flow name"
        ),
        other => panic!("`run --store DIR` must parse to Run/None, got {other:?}"),
    }
}

// ===========================================================================
// Test-support: run-store inspection
// ===========================================================================

/// Whether the run store under `<base>/<pipeline>/` contains at least one
/// `<run-id>/events.jsonl` — i.e. a flow really ran and wrote its stream.
fn run_store_has_a_stream(base: &str, pipeline: &str) -> bool {
    let dir = Path::new(base).join(pipeline);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return false;
    };
    for entry in entries.flatten() {
        if entry.path().join("events.jsonl").exists() {
            return true;
        }
    }
    false
}

/// Count the distinct `<run-id>/` directories under `<base>/<pipeline>/` that hold
/// an event stream — one per run.
fn run_id_dirs(base: &str, pipeline: &str) -> usize {
    let dir = Path::new(base).join(pipeline);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|e| e.path().join("events.jsonl").exists())
        .count()
}

/// Drive `run_registry` while capturing the diagnostic it would print (for the
/// `InvalidUsage` / `list` message assertions). The registry dispatch writes its
/// human-readable output through the `*_to` writer-taking entrypoint the library
/// exposes for testability; the process entrypoint routes it to std streams.
fn run_registry_capturing<I, T>(registry: &FlowRegistry, argv: I) -> (ExitCode, String)
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let mut buf: Vec<u8> = Vec::new();
    let code = dagr_cli::registry::run_registry_to(registry, argv, &mut buf);
    (code, String::from_utf8_lossy(&buf).into_owned())
}
