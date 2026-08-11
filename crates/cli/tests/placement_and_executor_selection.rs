//! **Placement in the artifact, and executor selection.** Written first, TDD.
//!
//! This is the CLI half of the placement ticket: what a placed node looks like in
//! the graph artifact and the structure snapshot, and the `--dagr.executor` /
//! `--dagr.max-pods` knobs that select an executor and cap in-flight remote work.
//!
//! Nothing here talks to a cluster, at any feature setting. Selecting the `k8s`
//! executor is a loud, actionable bootstrap failure and never a silent local run —
//! but *which* refusal it is depends on what the build compiled. Without the
//! default-off `k8s` feature (this crate's default, and the setting the file was
//! written at) there is no remote executor at all, and the refusal names the feature
//! and the ticket that wired it. With the feature on, the executor exists and the
//! refusal moves one layer down, to the placement-wiring guard that fires when a
//! placed pipeline was handed no cluster — asserted, with the rest of the wired path,
//! in `tests/m10_remote_execution.rs`. Assertions that read one of those two messages
//! are gated to the setting that produces it; everything else here holds at both.
//!
//! What this file pins:
//!
//! - placement appears in the graph artifact and the structure snapshot, and an
//!   **unplaced** pipeline's artifact is byte-identical to what it was before
//!   placement existed (no `placement` key at all);
//! - a placement change is **named** in the structure diff and moves the policy
//!   fingerprint but not the structural one;
//! - both knobs follow `flag > env > default`, live in the reserved library-flag
//!   namespace, and reject an unknown value loudly;
//! - selecting the `k8s` executor refuses — with zero attempts and the
//!   bootstrap-failure exit code, whichever of the two refusals this build reaches;
//!   the `local` executor runs, records the placement, ignores it, and says nothing
//!   about a cluster.
//!
//! # Hermeticity
//!
//! The resolvers read the **real** `DAGR_*` names, so every env-mutating test takes
//! a process-global lock and sets/removes inside the guard (edition 2024 makes
//! `set_var`/`remove_var` `unsafe` precisely because of the `environ` race that lock
//! closes).

use std::ffi::OsString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, RunOutcome};
use dagr_cli::config::{
    DAGR_EXECUTOR, DAGR_MAX_PODS, EXECUTOR_FLAG, MAX_PODS_DEFAULT, MAX_PODS_FLAG,
    parse_executor_flag, parse_max_pods_flag, resolve_executor, resolve_max_pods,
};
use dagr_cli::contract::{ExitCode, ParamSpec, check_reserved_collision, reserved_flag_names};
use dagr_cli::driver::RunConfig;
use dagr_cli::executor::ExecutorKind;
// Only a build that compiled no remote executor renders the refusal that names it.
#[cfg(not(feature = "k8s"))]
use dagr_cli::executor::{REMOTE_EXECUTOR_FEATURE, REMOTE_EXECUTOR_TICKET};
use dagr_cli::graph::{BuildProvenance, build_artifact};
use dagr_cli::registry::{FlowRegistry, run_registry_to};
use dagr_cli::run_flow::RunnableFlow;
use dagr_cli::structure_snapshot::StructureSnapshot;
use dagr_core::assembly::{NodePolicy, Placement};
use dagr_core::context::{RunContext, TerminalState};
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::test_kit::TempBase;
use dagr_core::{Flow, Pipeline, TaskError};
use serde_json::Value;

// ===========================================================================
// Deterministic injection seams
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

/// Blank the volatile wall stamp so two runs under an identical monotonic clock
/// compare byte-for-byte.
fn strip_wall(stream: &[u8]) -> Vec<Value> {
    dagr_artifact::event_stream::read_records(stream)
        .expect("stream parses")
        .records
        .into_iter()
        .map(|mut rec| {
            if let Some(obj) = rec.as_object_mut() {
                obj.insert("wall".into(), Value::String("<wall>".into()));
            }
            rec
        })
        .collect()
}

/// The process-global lock every env-mutating test takes.
fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[allow(
    unsafe_code,
    reason = "std::env::set_var/remove_var are unsafe fns in edition 2024; the \
              resolvers under test read the REAL process environment by name, so a \
              test cannot avoid mutating it — with_env is the only place this suite \
              does, and it holds the env lock across the whole window"
)]
fn with_env<T>(pairs: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
    let _guard = env_lock();
    for k in [DAGR_EXECUTOR, DAGR_MAX_PODS] {
        // SAFETY: mutating the environment races a concurrent `getenv`; `_guard`
        // holds this suite's process-global env lock for the whole
        // remove → set → read → remove window.
        unsafe { std::env::remove_var(k) };
    }
    for (k, v) in pairs {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::set_var(k, v) };
    }
    let out = body();
    for (k, _) in pairs {
        // SAFETY: as above — still inside the `_guard` window.
        unsafe { std::env::remove_var(k) };
    }
    out
}

// ===========================================================================
// Fixtures
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Report(u64);
impl StableName for Report {
    const STABLE_NAME: &'static str = "Report";
}

struct Extract;
impl StableName for Extract {
    const STABLE_NAME: &'static str = "extract-rows";
}
impl Task for Extract {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows(21))
    }
}

struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "load-report";
}
impl Task for Load {
    type Input = Rows;
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, input: Rows) -> Result<Report, TaskError> {
        Ok(Report(input.0 * 2))
    }
}

const PIPE: &str = "placement-pipe";

fn gpu_placement() -> Placement {
    Placement::new()
        .cpu("500m")
        .memory("2Gi")
        .node_selectors(&[("nodepool", "gpu")])
        .tolerations(&["nvidia.com/gpu=present:NoSchedule"])
}

/// The inspection-side pipeline (`dagr_core::Flow`), with the placement applied to
/// the downstream node only.
fn pipeline(placement: Option<Placement>) -> Pipeline {
    let mut flow = Flow::new();
    let rows = flow.register_source_named::<Extract>(
        "extract",
        &Extract,
        None::<String>,
        NodePolicy::new(),
    );
    let policy = placement.map_or_else(NodePolicy::new, |p| NodePolicy::new().placement(p));
    let _ = flow.register_named::<Load, _>("load", &Load, rows, None::<String>, policy);
    flow.finish()
}

/// The runnable, graph-emittable counterpart — the same graph, driven for real.
fn runnable(placement: Option<Placement>) -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let rows = flow.register_source_named("extract", Extract);
    let policy = placement.map_or_else(NodePolicy::new, |p| NodePolicy::new().placement(p));
    let _ = flow.register_named_with("load", Load, rows, policy);
    flow
}

fn artifact(placement: Option<Placement>) -> Value {
    build_artifact(
        &pipeline(placement),
        PIPE,
        "1970-01-01T00:00:00Z",
        &BuildProvenance::new("dagr@test", "commit", "lock"),
    )
    .expect("the pipeline is emittable")
}

fn node_policy_block(artifact: &Value, node: &str) -> Value {
    artifact
        .get("nodes")
        .and_then(Value::as_array)
        .expect("nodes")
        .iter()
        .find(|n| n.get("name").and_then(Value::as_str) == Some(node))
        .expect("the node is in the artifact")
        .get("policy")
        .expect("every node carries its complete effective policy")
        .clone()
}

// ===========================================================================
// The graph artifact
// ===========================================================================

/// A placed node's **complete effective policy** in the graph artifact carries its
/// placement, verbatim and opaque — so a diagram and a structure diff can show it.
#[test]
fn the_graph_artifact_records_a_placed_nodes_placement() {
    let placed = artifact(Some(gpu_placement()));
    let policy = node_policy_block(&placed, "load");
    let placement = policy
        .get("placement")
        .expect("a placed node's policy block carries its placement");

    assert_eq!(placement.get("cpu").and_then(Value::as_str), Some("500m"));
    assert_eq!(placement.get("memory").and_then(Value::as_str), Some("2Gi"));
    assert_eq!(
        placement.get("node_selectors"),
        Some(&serde_json::json!([{"key": "nodepool", "value": "gpu"}])),
        "node selectors are recorded as opaque key/value pairs in declared order"
    );
    assert_eq!(
        placement.get("tolerations"),
        Some(&serde_json::json!(["nvidia.com/gpu=present:NoSchedule"])),
        "a toleration is recorded as one opaque string"
    );
}

/// **No churn.** An unplaced pipeline's artifact carries **no** `placement` key at
/// all — the two artifacts differ in exactly that one addition, so every existing
/// pipeline's artifact is byte-identical to what it was.
#[test]
fn an_unplaced_pipelines_artifact_is_byte_identical_to_before() {
    let plain = artifact(None);
    let placed = artifact(Some(gpu_placement()));

    assert!(
        node_policy_block(&plain, "load").get("placement").is_none(),
        "an unplaced node's policy block must not gain a placement key"
    );
    assert!(
        node_policy_block(&plain, "extract")
            .get("placement")
            .is_none()
    );

    // Strip the one addition and the two artifacts are byte-for-byte equal.
    let mut stripped = placed.clone();
    for node in stripped
        .get_mut("nodes")
        .and_then(Value::as_array_mut)
        .expect("nodes")
    {
        if let Some(policy) = node.get_mut("policy").and_then(Value::as_object_mut) {
            policy.remove("placement");
        }
    }
    // The header's policy hash legitimately differs; compare the node/edge body.
    assert_eq!(
        dagr_artifact::canonical::to_canonical_string(stripped.get("nodes").expect("nodes")),
        dagr_artifact::canonical::to_canonical_string(plain.get("nodes").expect("nodes")),
        "placement is the ONLY thing a placed pipeline adds to the artifact"
    );
}

/// A placement change is **named** in the structure diff (`policy.placement`), and
/// the companion fingerprints show it moved the policy hash and left the structural
/// fingerprint alone.
#[test]
fn a_placement_change_is_visible_in_the_structure_diff() {
    let plain = StructureSnapshot::from_pipeline(&pipeline(None), PIPE).expect("snapshot");
    let placed =
        StructureSnapshot::from_pipeline(&pipeline(Some(gpu_placement())), PIPE).expect("snapshot");

    let diff = placed.diff(&plain);
    assert!(!diff.is_empty(), "a placement change is review-visible");
    let rendered = diff.to_string();
    assert!(
        rendered.contains("policy.placement"),
        "the structure diff names the changed policy field: {rendered}"
    );
    assert!(
        rendered.contains("500m"),
        "and shows the declared value: {rendered}"
    );

    assert_eq!(
        placed.structural_fingerprint(),
        plain.structural_fingerprint(),
        "placement is out of the structural fingerprint"
    );
    assert_ne!(
        placed.policy_fingerprint(),
        plain.policy_fingerprint(),
        "placement is in the policy hash"
    );
    assert!(
        placed.to_canonical_string().contains("placement"),
        "the snapshot body itself carries the placement"
    );
}

// ===========================================================================
// The `--dagr.executor` knob
// ===========================================================================

/// With neither a flag nor the environment variable, the executor is **local** —
/// so one binary is genuinely both and a placed pipeline still runs on a laptop.
#[test]
fn the_executor_defaults_to_local() {
    let resolved = with_env(&[], || resolve_executor(None, None).expect("no knob set"));
    assert_eq!(resolved, ExecutorKind::Local);
}

/// A present flag wins outright; the environment is never consulted.
#[test]
fn the_executor_flag_beats_the_env_var() {
    let resolved = with_env(&[(DAGR_EXECUTOR, "k8s")], || {
        resolve_executor(Some(ExecutorKind::Local), None).expect("the flag parses")
    });
    assert_eq!(
        resolved,
        ExecutorKind::Local,
        "flag > env: `--dagr.executor=local` beats `DAGR_EXECUTOR=k8s`"
    );
}

/// With no flag, the environment variable is honoured.
#[test]
fn the_executor_env_var_is_used_when_no_flag_is_given() {
    let resolved = with_env(&[(DAGR_EXECUTOR, "k8s")], || {
        resolve_executor(None, None).expect("k8s is a recognized value")
    });
    assert_eq!(resolved, ExecutorKind::Kubernetes);
}

/// An unknown value **fails loudly**, naming both the variable and the rejected
/// value — never silently resolved to the default.
#[test]
fn an_unknown_executor_value_fails_loudly() {
    let err = with_env(&[(DAGR_EXECUTOR, "nomad")], || {
        resolve_executor(None, None).expect_err("`nomad` is not a recognized executor")
    });
    assert_eq!(
        err.source,
        dagr_cli::config::ConfigSource::env(DAGR_EXECUTOR),
        "the discriminator records the env var that supplied the value"
    );
    assert_eq!(err.value, "nomad");
    assert_eq!(
        err.exit_code(),
        ExitCode::InvalidUsage,
        "a syntactically bad knob value is invalid usage"
    );
    let rendered = err.to_string();
    assert!(
        rendered.contains(DAGR_EXECUTOR) && rendered.contains("nomad"),
        "the diagnostic names the variable and the rejected value: {rendered}"
    );
    assert!(
        rendered.contains("local") && rendered.contains("k8s"),
        "and lists what IS accepted: {rendered}"
    );
}

/// Both flag grammars are read: `--dagr.executor=k8s` and `--dagr.executor k8s`.
#[test]
fn the_executor_flag_is_parsed_from_the_invocation() {
    let argv = |args: &[&str]| -> Vec<OsString> { args.iter().map(OsString::from).collect() };
    assert_eq!(
        parse_executor_flag(&argv(&["run", "--dagr.executor=k8s"])).expect("parses"),
        Some(ExecutorKind::Kubernetes)
    );
    assert_eq!(
        parse_executor_flag(&argv(&["run", EXECUTOR_FLAG, "local"])).expect("parses"),
        Some(ExecutorKind::Local)
    );
    assert_eq!(parse_executor_flag(&argv(&["run"])).expect("parses"), None);
    assert!(
        parse_executor_flag(&argv(&["run", "--dagr.executor=nomad"])).is_err(),
        "an unknown value on the flag is loud too"
    );
}

// ===========================================================================
// The `--dagr.max-pods` knob
// ===========================================================================

/// `--dagr.max-pods` follows `flag > env > default`; the default imposes **no**
/// dagr-side ceiling (the cluster owns its own capacity).
#[test]
fn the_max_pods_knob_follows_flag_env_default() {
    assert_eq!(
        with_env(&[], || resolve_max_pods(None, None).expect("default")),
        MAX_PODS_DEFAULT
    );
    assert_eq!(
        with_env(&[(DAGR_MAX_PODS, "8")], || resolve_max_pods(None, None)
            .expect("env parses")),
        8
    );
    assert_eq!(
        with_env(&[(DAGR_MAX_PODS, "8")], || resolve_max_pods(Some(2), None)
            .expect("flag wins")),
        2
    );
}

/// A bad `--dagr.max-pods` value fails loudly, naming the variable and the value.
#[test]
fn a_bad_max_pods_value_fails_loudly() {
    let err = with_env(&[(DAGR_MAX_PODS, "lots")], || {
        resolve_max_pods(None, None).expect_err("`lots` is not a pod count")
    });
    assert_eq!(
        err.source,
        dagr_cli::config::ConfigSource::env(DAGR_MAX_PODS),
        "the discriminator records the env var that supplied the value"
    );
    assert_eq!(err.value, "lots");
    assert_eq!(err.exit_code(), ExitCode::InvalidUsage);

    let argv: Vec<OsString> = ["run", "--dagr.max-pods=lots"]
        .iter()
        .map(OsString::from)
        .collect();
    assert!(parse_max_pods_flag(&argv).is_err());
}

// ===========================================================================
// The reserved library-flag namespace
// ===========================================================================

/// Both knobs are library-owned, so a pipeline parameter can never shadow them.
#[test]
fn the_new_knobs_live_in_the_reserved_flag_namespace() {
    let reserved = reserved_flag_names();
    for name in ["dagr.executor", "dagr.max-pods"] {
        assert!(
            reserved.contains(&name),
            "`{name}` must be reserved; namespace is {reserved:?}"
        );
    }
    assert_eq!(EXECUTOR_FLAG, "--dagr.executor");
    assert_eq!(MAX_PODS_FLAG, "--dagr.max-pods");
}

/// A pipeline parameter named after either knob is a **hard** collision.
#[test]
fn a_pipeline_parameter_named_after_a_knob_is_a_hard_collision() {
    for name in ["dagr.executor", "dagr.max-pods"] {
        let params = [ParamSpec::new(
            name,
            "a pipeline parameter that must not exist",
        )];
        let collision = check_reserved_collision(&params)
            .expect_err("a pipeline parameter cannot shadow a library flag");
        assert_eq!(collision.flag, name);
        assert!(
            collision.to_string().contains(name),
            "the diagnostic names the offending flag"
        );
    }
}

// ===========================================================================
// Executor selection — the refusal, and what it is made of
// ===========================================================================

/// The `local` executor is available in **every** build: it is the one this whole
/// testing surface is built around, and no feature can take it away.
#[test]
fn the_local_executor_is_available_in_every_build() {
    ExecutorKind::Local
        .ensure_available()
        .expect("the local executor is what every build ships");
}

/// A build that did **not** compile the default-off `k8s` feature has no remote
/// executor at all, and says so: an actionable error naming the executor, the feature
/// that would supply it, the ticket that wired it, and the flag to change. Not a
/// panic, and not a silent local run.
///
/// The complementary setting — a build that *did* compile it, where
/// `ensure_available` succeeds and the refusal that matters is the placement-wiring
/// guard's — is `tests/m10_remote_execution.rs`, which needs the executor linked.
#[cfg(not(feature = "k8s"))]
#[test]
fn the_kubernetes_executor_is_refused_by_a_build_that_did_not_compile_it() {
    let refusal = ExecutorKind::Kubernetes
        .ensure_available()
        .expect_err("a build without the `k8s` feature has no remote executor");
    let rendered = refusal.to_string();
    assert!(
        rendered.contains("k8s"),
        "the refusal names the selected executor: {rendered}"
    );
    assert!(
        rendered.contains(&format!("--features {REMOTE_EXECUTOR_FEATURE}")),
        "and how to get one that can — the feature to rebuild with: {rendered}"
    );
    assert!(
        rendered.contains(REMOTE_EXECUTOR_TICKET),
        "and the ticket that wired it: {rendered}"
    );
    assert!(
        rendered.contains(EXECUTOR_FLAG),
        "and the flag to change: {rendered}"
    );
}

/// A run configured for the remote executor **fails bootstrap** — it does not
/// quietly run every node locally. This is the property both refusals exist for, so
/// it is asserted at **both** feature settings: without the executor compiled in the
/// build itself refuses; with it, this placed pipeline was handed no cluster and the
/// placement-wiring guard refuses. Zero attempts either way.
#[test]
fn a_run_configured_for_kubernetes_fails_bootstrap_rather_than_running_locally() {
    let base = TempBase::new("placement-k8s");
    let sink = MemorySink::default();
    let config = RunConfig::new(base.as_str())
        .run_id("fixed-run")
        .executor(ExecutorKind::Kubernetes);
    let report = runnable(Some(gpu_placement()))
        .run(PIPE, &config, sink.clone(), TickClock::default())
        .expect("the flow assembles");

    assert_eq!(
        report.outcome(),
        RunOutcome::BootstrapFailed,
        "selecting the remote executor for a placed pipeline with no cluster wired is a \
         bootstrap failure"
    );
    assert!(
        report.driver_report().terminal_states.is_empty(),
        "and not one node ran locally behind the operator's back"
    );
    assert_eq!(
        dagr_cli::contract::exit_code_for_run(report.driver_report()),
        ExitCode::BootstrapFailure
    );
}

/// The `run` verb refuses the same way, with the bootstrap-failure exit code.
///
/// The **exit code** is the verb's contract at every feature setting, and it is what
/// an operator's shell reads. The diagnostic behind it differs: a build without the
/// executor refuses at the verb, before a store directory exists, on the verb's own
/// output — so the ticket assertion below is gated to that build. A build *with* the
/// executor gets one layer further in and is refused by the placement-wiring guard,
/// which reports on stderr rather than through the verb's writer; that message is
/// `dagr_cli::remote_guard::unwired_refusal`, covered under the feature by
/// `tests/m10_remote_execution.rs`.
#[test]
fn the_run_verb_refuses_the_kubernetes_executor() {
    let base = TempBase::new("placement-verb-k8s");
    let registry = FlowRegistry::new().add(PIPE, || runnable(Some(gpu_placement())));
    let mut out = Vec::new();
    let code = run_registry_to(
        &registry,
        [
            "dagr",
            "run",
            PIPE,
            "--store",
            base.as_str(),
            "--dagr.executor=k8s",
        ],
        &mut out,
    );
    let printed = String::from_utf8(out).expect("utf-8 diagnostics");
    assert_eq!(code, ExitCode::BootstrapFailure, "printed: {printed}");
    #[cfg(not(feature = "k8s"))]
    assert!(
        printed.contains(REMOTE_EXECUTOR_TICKET),
        "the refusal names the ticket that wired the executor: {printed}"
    );
}

// ===========================================================================
// The local executor — recorded and ignored
// ===========================================================================

/// With no executor flag, the local executor runs and a **placed** pipeline's event
/// stream is what the unplaced one's is, record for record — save for the one field
/// that *should* move, the header's recorded **policy hash**. Placement is recorded
/// (in the header, and in the graph artifact) and completely inert at run time: no
/// extra record, no different admission, no different outcome.
#[test]
fn under_the_local_executor_a_placement_is_recorded_and_ignored() {
    let drive = |placement: Option<Placement>| {
        let base = TempBase::new("placement-local");
        let sink = MemorySink::default();
        let config = RunConfig::new(base.as_str()).run_id("fixed-run");
        let report = runnable(placement)
            .run(PIPE, &config, sink.clone(), TickClock::default())
            .expect("the flow assembles");
        (
            report.outcome(),
            report
                .driver_report()
                .terminal_states
                .iter()
                .map(|(n, s)| (n.clone(), *s))
                .collect::<Vec<(String, TerminalState)>>(),
            sink.bytes(),
        )
    };

    let (plain_outcome, plain_terminals, plain_stream) = drive(None);
    let (placed_outcome, placed_terminals, placed_stream) = drive(Some(gpu_placement()));

    assert_eq!(placed_outcome, RunOutcome::Succeeded);
    assert_eq!(plain_outcome, RunOutcome::Succeeded);
    assert_eq!(placed_terminals, plain_terminals);

    let (placed_records, plain_records) = (strip_wall(&placed_stream), strip_wall(&plain_stream));
    // The header legitimately records a different policy hash — that IS the
    // placement, and a resume against the other binary prints it as a policy diff.
    // The structural fingerprint must be identical: the graph did not change.
    let header = |records: &[Value]| {
        records[0]
            .get("header")
            .cloned()
            .expect("run-started header")
    };
    let (placed_header, plain_header) = (header(&placed_records), header(&plain_records));
    assert_ne!(
        placed_header.get("fingerprint_policy"),
        plain_header.get("fingerprint_policy"),
        "the recorded policy hash carries the placement"
    );
    assert_eq!(
        placed_header.get("fingerprint_structural"),
        plain_header.get("fingerprint_structural"),
        "and the structural fingerprint is untouched"
    );

    // Everything else — every field of the header and every subsequent record — is
    // identical: a placement emits no record and changes no outcome locally.
    let mask_policy_hash = |records: Vec<Value>| -> Vec<Value> {
        records
            .into_iter()
            .map(|mut rec| {
                if let Some(h) = rec.get_mut("header").and_then(Value::as_object_mut) {
                    h.insert(
                        "fingerprint_policy".into(),
                        Value::String("<policy>".into()),
                    );
                }
                rec
            })
            .collect()
    };
    assert_eq!(
        mask_policy_hash(placed_records),
        mask_policy_hash(plain_records),
        "a placement adds no record and changes no outcome under the local executor"
    );
}

/// A placed pipeline run through the `run` verb under the local executor succeeds
/// and says **nothing** about a missing cluster — the dual-mode story stays quiet
/// for exactly the developer the local path exists for.
#[test]
fn a_placed_pipeline_runs_locally_without_warning_about_a_cluster() {
    let base = TempBase::new("placement-local-verb");
    let registry = FlowRegistry::new().add(PIPE, || runnable(Some(gpu_placement())));
    let mut out = Vec::new();
    let code = run_registry_to(
        &registry,
        ["dagr", "run", PIPE, "--store", base.as_str()],
        &mut out,
    );
    let printed = String::from_utf8(out).expect("utf-8 diagnostics");
    assert_eq!(code, ExitCode::Success, "printed: {printed}");
    let lowered = printed.to_lowercase();
    for noise in ["cluster", "kubernetes", "k8s", "warn"] {
        assert!(
            !lowered.contains(noise),
            "a local run must not mention `{noise}`: {printed}"
        );
    }
}
