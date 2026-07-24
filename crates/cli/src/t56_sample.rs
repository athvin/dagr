//! Shared **sample-pipeline harness** for the C26 CLI acceptance suite — ticket
//! T56 (069). This is **test-support scaffolding**, not a released binary and not
//! new framework capability: it composes already-merged library entry points
//! (`dagr_cli::contract`, `dagr_cli::graph`, `dagr_cli::driver`,
//! `dagr_artifact::fold`) into a compiled pipeline binary that exposes the real
//! C26 command surface, exactly as a real pipeline crate would. `main.rs` is the
//! library's own pipeline-less reference driver; a real pipeline binary carries a
//! concrete pipeline and wires the same entry points — the pattern this harness
//! demonstrates. It is `include!`d by the two structurally-distinct sample bins
//! (`dagr-t56-alpha`, `dagr-t56-beta`), which differ only in the `Sample` built.
//!
//! # What it wires (every verb, real library entry point)
//!
//! - `graph`   → [`graph_verb`] over the binary's assembled pipeline (C20).
//! - `validate`→ [`validate_verb`] (assembly only, prints every problem, no store).
//! - `render`  → [`render_verb`] over a graph artifact read from a file
//!   (optionally a run overlay), artifacts-only (C24).
//! - `run`     → the real [`drive`] loop, after the library typed-parameter
//!   validation ([`validate_params`]) and reserved-flag collision check
//!   ([`check_reserved_collision`]) at bootstrap; the run's event stream is
//!   written to the run store, then folded ([`fold_stream`]) into the run
//!   artifact. The outcome maps through the C26 table ([`exit_code_for_run`]).
//! - `single-node` → the durability gate ([`single_node_refusal_check`]) then a
//!   real single-node replay: the requested node re-executes standalone (inputs
//!   rehydrated from the prior run's recorded durable references), and the emitted
//!   **replay-variant** artifact marks every unselected node `not-requested`.
//! - `resume`  → the recognized [`resume_verb_stub`] (T58 replaces the body).
//! - `fold`    → [`fold_verb`] over a stream read from a file (crash-clause path).
//! - `prune`   → run-store retention by count or age (nothing deleted implicitly).
//!
//! # Determinism / isolation
//!
//! Deterministic throughout: run identity is operator-set (`--run-id`), the stream
//! is stamped by a hand-stepped monotonic clock, and no wall-clock sleep is used.
//! The run-store base is a private per-test temp dir the invoking test supplies.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode as ProcExit;
use std::sync::Arc;

use dagr_artifact::event_stream::{
    record_durable_reference, AttemptOutcomeRecord, EventSink, EventStreamWriter, MonotonicClock,
    RunId as WireRunId, RunOutcome, RunStartedHeader, TerminalState as WireTerminalState,
    EVENTS_FILE_NAME, FINGERPRINT_ALGORITHM_VERSION,
};
use dagr_artifact::fold::fold_stream;
use dagr_cli::contract::{
    check_reserved_collision, exit_code_for_run, fold_verb, parse_cli, render_verb,
    resume_verb_stub, single_node_refusal_check, validate_params, validate_verb, Cli, ExitCode,
    ParamSpec, ParseOutcome, RenderFormat, Verb,
};
use dagr_cli::driver::{drive, NodeRunner, RunConfig, RunPlan};
use dagr_cli::graph::{emit_graph, graph_verb, BuildProvenance};
use dagr_core::admission::PoolCapacities;
use dagr_core::assembly::DurableOutput;
use dagr_core::context::TerminalState;
use dagr_core::execution::{run_attempt_caught, AttemptEvent, AttemptEventSink};
use dagr_core::flow::{FailureMode, Pipeline};
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::stable_name::StableName;
use dagr_core::task::{RunContext, Task};
use dagr_core::{Flow, NodePolicy, TaskError};

/// The reserved graph-artifact file name in a run directory (T0.6 §3).
const GRAPH_FILE_NAME: &str = "graph.json";
/// The reserved folded run-artifact file name the harness writes next to a stream.
const RUN_ARTIFACT_FILE_NAME: &str = "run.json";

// ===========================================================================
// The sample-pipeline description — the ONE thing the two bins differ in.
// ===========================================================================

/// A single structurally-distinct sample pipeline the harness drives. The two
/// bins supply two different values so "identical verb behaviour across pipelines"
/// is a real claim (the pipelines differ; the verbs do not).
pub struct Sample {
    /// The stable pipeline identity (the run-store directory name).
    pub pipeline_name: &'static str,
    /// A typed parameter the pipeline declares, used by the bootstrap-validation +
    /// header round-trip scenarios. Named distinctly per pipeline, never a reserved
    /// library flag.
    pub param: ParamSpec,
}

// ===========================================================================
// Tasks — small, StableName-carrying, deterministic. Shared by both samples;
// each sample assembles a DIFFERENT shape from them.
// ===========================================================================

/// A durable payload — implements the C27 reference contract, so a node producing
/// it can be marked durable and its output rehydrated at single-node replay.
struct Rows(u64);
impl StableName for Rows {
    const STABLE_NAME: &'static str = "Rows";
}
impl DurableOutput for Rows {
    fn serialize_reference(&self) -> String {
        format!("mem://rows/{}", self.0)
    }
    fn rehydrate(reference: &str) -> Result<Self, dagr_core::RehydrateError> {
        reference
            .strip_prefix("mem://rows/")
            .and_then(|n| n.parse::<u64>().ok())
            .map(Rows)
            .ok_or_else(|| dagr_core::RehydrateError::corruption("malformed rows reference"))
    }
}

/// A durable source (`load`): the stage boundary's producer. Succeeds, produces a
/// durable value whose reference is recorded so a replay can rehydrate it.
struct Load;
impl StableName for Load {
    const STABLE_NAME: &'static str = "load-task";
}
impl Task for Load {
    type Input = ();
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
        Ok(Rows(42))
    }
}

/// The node the single-node replay re-executes from `load`'s recorded reference.
/// Modelled as a source here (it consumes the rehydrated value at replay time; in
/// the run loop it is not wired, since the run scenarios use source-only shapes),
/// carrying the `Rows` input type so the durability gate sees `load` as its input.
struct Transform;
impl StableName for Transform {
    const STABLE_NAME: &'static str = "transform-task";
}
impl Task for Transform {
    type Input = Rows;
    type Output = Rows;
    async fn run(&mut self, _c: &RunContext, i: Rows) -> Result<Rows, TaskError> {
        Ok(Rows(i.0 + 1))
    }
}

/// A no-input standalone node (`standalone`): consumes nothing, so it can be
/// replayed with no prior run supplied.
struct Standalone;
impl StableName for Standalone {
    const STABLE_NAME: &'static str = "standalone-task";
}
impl Task for Standalone {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// A controllable non-teardown failure (`maybe-fail`): fails iff `DAGR_T56_FAIL`
/// is set, else succeeds — lets one invocation choose a clean run vs a run failure.
struct MaybeFail;
impl StableName for MaybeFail {
    const STABLE_NAME: &'static str = "maybe-fail-task";
}
impl Task for MaybeFail {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        if std::env::var("DAGR_T56_FAIL").is_ok() {
            Err(TaskError::permanent("deliberate failure for T56"))
        } else {
            Ok(())
        }
    }
}

/// A skip-only source (`decide-skip`): a deliberate (originated) skip, so a run of
/// only this node is skip-only — still successful.
struct DecideSkip;
impl StableName for DecideSkip {
    const STABLE_NAME: &'static str = "decide-skip-task";
}
impl Task for DecideSkip {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Err(TaskError::skip("nothing to do"))
    }
}

/// A non-durable, stable-named output — has [`StableName`] but deliberately does
/// **not** implement [`DurableOutput`], so a node marked durable while producing it
/// is an assembly failure (the two-problem enumeration the assembly-fail scenarios
/// exercise).
struct NonDurable;
impl StableName for NonDurable {
    const STABLE_NAME: &'static str = "NonDurable";
}

/// A source producing the non-durable, stable-named value — used only to build the
/// assembly-failing variant (marked durable without the contract).
struct MakeNonDurable;
impl StableName for MakeNonDurable {
    const STABLE_NAME: &'static str = "make-non-durable-task";
}
impl Task for MakeNonDurable {
    type Input = ();
    type Output = NonDurable;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<NonDurable, TaskError> {
        Ok(NonDurable)
    }
}

// ===========================================================================
// Pipeline assembly — the two shapes (structurally distinct).
// ===========================================================================

/// Assemble the sample's healthy graph. `alpha` = a durable stage boundary
/// `load → transform` plus a `standalone` no-input node; `beta` = the same durable
/// boundary plus a `maybe-fail` node and a `decide-skip` node — a different node
/// set and different edges, so verb parity is a real claim.
fn build_pipeline(sample: &Sample) -> Pipeline {
    let mut flow = Flow::new();
    // `load → transform` is the stage boundary: `load` produces a durable value
    // whose reference the run producer records (C27), so single-node replay
    // rehydrates `transform`'s input from it. Registered through the stable-name
    // registrar so the graph artifact is emittable (the durable *reference* is
    // recorded per-attempt at run time, C22/T57 — not carried by the node policy
    // flag here, which would need the combined durable+named registrar T55 did not
    // add; the replay durability gate reads the recorded reference, C26).
    let load = flow.register_source_named::<Load>("load", &Load, None::<String>, NodePolicy::new());
    let _t = flow.register_named::<Transform, _>(
        "transform",
        &Transform,
        load,
        None::<String>,
        NodePolicy::new(),
    );
    if sample.pipeline_name == "t56-alpha" {
        let _s = flow.register_source_named::<Standalone>(
            "standalone",
            &Standalone,
            None::<String>,
            NodePolicy::new(),
        );
    } else {
        let _m = flow.register_source_named::<MaybeFail>(
            "maybe-fail",
            &MaybeFail,
            None::<String>,
            NodePolicy::new(),
        );
        let _d = flow.register_source_named::<DecideSkip>(
            "decide-skip",
            &DecideSkip,
            None::<String>,
            NodePolicy::new(),
        );
    }
    flow.finish()
}

/// A source-only run pipeline the `run` verb drives (no input-slot wiring). Its
/// single node's terminal state is chosen by `kind`, so one binary demonstrates a
/// clean success, a run failure, and a skip-only run without a second pipeline.
fn build_run_pipeline(kind: RunKind) -> (Pipeline, BTreeMap<String, Box<dyn NodeRunner>>) {
    let mut flow = Flow::new();
    let mut runners: BTreeMap<String, Box<dyn NodeRunner>> = BTreeMap::new();
    match kind {
        RunKind::Success => {
            let _h = flow.register_source_named::<Standalone>(
                "work",
                &Standalone,
                None::<String>,
                NodePolicy::new(),
            );
            runners.insert("work".into(), SourceRunner::boxed("work", Standalone));
        }
        RunKind::Failure | RunKind::StopOnFailure => {
            let _h = flow.register_source_named::<MaybeFail>(
                "boom",
                &MaybeFail,
                None::<String>,
                NodePolicy::new(),
            );
            runners.insert("boom".into(), SourceRunner::boxed("boom", MaybeFail));
            if matches!(kind, RunKind::StopOnFailure) {
                let _o = flow.register_source_named::<Standalone>(
                    "other",
                    &Standalone,
                    None::<String>,
                    NodePolicy::new(),
                );
                runners.insert("other".into(), SourceRunner::boxed("other", Standalone));
            }
        }
        RunKind::SkipOnly => {
            let _h = flow.register_source_named::<DecideSkip>(
                "skip",
                &DecideSkip,
                None::<String>,
                NodePolicy::new(),
            );
            runners.insert("skip".into(), SourceRunner::boxed("skip", DecideSkip));
        }
        RunKind::TooBig => {
            let _h = flow.register_source_named::<Standalone>(
                "big",
                &Standalone,
                None::<String>,
                NodePolicy::new().working_memory(1_000_000_000),
            );
            runners.insert("big".into(), SourceRunner::boxed("big", Standalone));
        }
    }
    (flow.finish(), runners)
}

/// Which single-node run shape the `run` verb should drive.
#[derive(Clone, Copy)]
enum RunKind {
    Success,
    Failure,
    StopOnFailure,
    SkipOnly,
    TooBig,
}

/// Assemble an **assembly-failing** variant: two nodes marked durable whose output
/// type (`NonDurable`) lacks the durable contract, so assembly reports two
/// independent problems (C7).
fn build_assembly_failing() -> Pipeline {
    let mut flow = Flow::new();
    let _a = flow.register_source_named::<MakeNonDurable>(
        "durable-a",
        &MakeNonDurable,
        None::<String>,
        NodePolicy::new().durable(true),
    );
    let _b = flow.register_source_named::<MakeNonDurable>(
        "durable-b",
        &MakeNonDurable,
        None::<String>,
        NodePolicy::new().durable(true),
    );
    flow.finish()
}

// ===========================================================================
// Node runner over the real caught attempt path (source nodes only).
// ===========================================================================

fn slot_for<T: Send + Sync + 'static>(name: &str) -> Arc<Slot<T>> {
    Arc::new(Slot::new(
        dagr_core::handle::NodeId::from_name(name),
        name,
        0,
        false,
        0,
        ResidencyLedger::new(),
    ))
}

/// A source runner (no inputs) over the real caught attempt path.
struct SourceRunner<T: Task<Input = ()>> {
    name: String,
    task: Option<T>,
    slot: Arc<Slot<T::Output>>,
}
impl<T: Task<Input = ()> + Send + 'static> SourceRunner<T>
where
    T::Output: Send + Sync + 'static,
{
    fn boxed(name: &str, task: T) -> Box<dyn NodeRunner> {
        Box::new(Self {
            name: name.to_string(),
            task: Some(task),
            slot: slot_for::<T::Output>(name),
        })
    }
}
impl<T: Task<Input = ()> + Send + 'static> NodeRunner for SourceRunner<T>
where
    T::Output: Send + Sync + 'static,
{
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
            run_attempt_caught(&mut task, &name, ctx, &slot, sink)
                .await
                .terminal_state()
        })
    }
}

// ===========================================================================
// Deterministic sinks + clocks.
// ===========================================================================

/// An append-only local-file event sink (the real C19 sink surface).
struct FileSink {
    file: File,
}
impl FileSink {
    fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }
}
impl EventSink for FileSink {
    fn append_line(&mut self, line: &[u8]) -> io::Result<()> {
        self.file.write_all(line)?;
        self.file.flush()
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// A sink that always fails to append — proves the sink-failure exit code within a
/// bounded wait (never a hang).
struct FailingSink;
impl EventSink for FailingSink {
    fn append_line(&mut self, _line: &[u8]) -> io::Result<()> {
        Err(io::Error::other("sink deliberately unwritable"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("sink deliberately unwritable"))
    }
}

/// A hand-stepped monotonic clock — deterministic offsets, no wall clock.
#[derive(Default)]
struct StepClock(std::cell::Cell<u64>);
impl StepClock {
    fn set(&self, at: u64) {
        self.0.set(at);
    }
}
struct ClockRef<'a>(&'a StepClock);
impl MonotonicClock for ClockRef<'_> {
    fn elapsed_ns(&self) -> u64 {
        self.0 .0.get()
    }
}

/// A driver clock: a monotonic counter (deterministic ordering, no wall clock).
#[derive(Default)]
struct DriverClock(std::sync::atomic::AtomicU64);
impl MonotonicClock for DriverClock {
    fn elapsed_ns(&self) -> u64 {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

// ===========================================================================
// Run-store layout + flag parsing.
// ===========================================================================

/// The run directory `<base>/<pipeline>/<run-id>`.
fn run_dir(base: &str, pipeline: &str, run_id: &str) -> PathBuf {
    PathBuf::from(base).join(pipeline).join(run_id)
}

/// A tiny parsed view of the flags this harness consumes (`--k v` or `--k=v`; a
/// bare `--k` records an empty value).
#[derive(Default)]
struct Flags {
    map: BTreeMap<String, String>,
}
impl Flags {
    fn parse(args: &[String]) -> Self {
        let mut f = Flags::default();
        let mut i = 0;
        while i < args.len() {
            if let Some(rest) = args[i].strip_prefix("--") {
                if let Some((k, v)) = rest.split_once('=') {
                    f.map.insert(k.to_string(), v.to_string());
                } else if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                    f.map.insert(rest.to_string(), args[i + 1].clone());
                    i += 1;
                } else {
                    f.map.insert(rest.to_string(), String::new());
                }
            }
            i += 1;
        }
        f
    }
    fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(String::as_str)
    }
    fn has(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }
}

// ===========================================================================
// Entry point + dispatch.
// ===========================================================================

/// Parse and dispatch one invocation of a sample binary, returning the process
/// exit code. Mirrors a real pipeline crate's `main`: parse through the library,
/// dispatch each verb to the real library entry point against a concrete pipeline,
/// map to the C26 exit code.
pub fn dispatch_main(sample: &Sample) -> ProcExit {
    let code = match parse_cli(std::env::args_os()) {
        ParseOutcome::Help { exit, text } => {
            print!("{text}");
            exit
        }
        ParseOutcome::Error { exit, message } => {
            eprintln!("dagr: {message}");
            exit
        }
        ParseOutcome::Parsed(Cli { verb }) => dispatch(sample, verb),
    };
    code.into()
}

/// Dispatch a parsed verb to its real library entry point.
fn dispatch(sample: &Sample, verb: Verb) -> ExitCode {
    let mut stdout = io::stdout().lock();
    let args: Vec<String> = std::env::args().skip(2).collect();
    let flags = Flags::parse(&args);
    match verb {
        Verb::Graph => graph_dispatch(sample, &mut stdout),
        Verb::Validate => validate_dispatch(sample, &flags, &mut stdout),
        Verb::Render => render_dispatch(&flags, &mut stdout),
        Verb::Run => run_dispatch(sample, &flags, &mut stdout),
        Verb::SingleNode => single_node_dispatch(sample, &flags, &mut stdout),
        Verb::Resume => resume_verb_stub(&mut stdout),
        Verb::Fold => fold_dispatch(&flags, &mut stdout),
        Verb::Prune => prune_dispatch(sample, &flags, &mut stdout),
    }
}

/// `graph`: emit the assembled pipeline's C20 graph artifact (no store).
fn graph_dispatch<W: Write>(sample: &Sample, out: &mut W) -> ExitCode {
    let pipeline = build_pipeline(sample);
    match graph_verb(&pipeline, sample.pipeline_name, "2026-07-24T00:00:00Z", out) {
        Ok(()) => ExitCode::Success,
        Err(e) => {
            let _ = writeln!(out, "graph verb failed: {e}");
            ExitCode::InvalidUsage
        }
    }
}

/// `validate`: assembly only, print every problem. `--assembly-fail` selects the
/// broken variant so the two-problem enumeration is observable.
fn validate_dispatch<W: Write>(sample: &Sample, flags: &Flags, out: &mut W) -> ExitCode {
    let pipeline = if flags.has("assembly-fail") {
        build_assembly_failing()
    } else {
        build_pipeline(sample)
    };
    validate_verb(&pipeline, out)
}

/// `render`: read a graph artifact from `--graph <path>` (+ optional `--run
/// <path>` overlay), render DOT — artifacts only.
fn render_dispatch<W: Write>(flags: &Flags, out: &mut W) -> ExitCode {
    let Some(graph_path) = flags.get("graph") else {
        let _ = writeln!(out, "render needs --graph <path>");
        return ExitCode::InvalidUsage;
    };
    let graph_bytes = match std::fs::read(graph_path) {
        Ok(b) => b,
        Err(e) => {
            let _ = writeln!(out, "cannot read graph artifact: {e}");
            return ExitCode::InvalidUsage;
        }
    };
    let run_bytes = match flags.get("run") {
        Some(p) => match std::fs::read(p) {
            Ok(b) => Some(b),
            Err(e) => {
                let _ = writeln!(out, "cannot read run artifact: {e}");
                return ExitCode::InvalidUsage;
            }
        },
        None => None,
    };
    render_verb(&graph_bytes, run_bytes.as_deref(), RenderFormat::Dot, out)
}

/// `fold`: read an event stream from `--stream <path>` and fold it into a run
/// artifact (crash-clause path).
fn fold_dispatch<W: Write>(flags: &Flags, out: &mut W) -> ExitCode {
    let Some(stream_path) = flags.get("stream") else {
        let _ = writeln!(out, "fold needs --stream <path>");
        return ExitCode::InvalidUsage;
    };
    let bytes = match std::fs::read(stream_path) {
        Ok(b) => b,
        Err(e) => {
            let _ = writeln!(out, "cannot read event stream: {e}");
            return ExitCode::InvalidUsage;
        }
    };
    fold_verb(&bytes, &[], out)
}

// ===========================================================================
// run verb.
// ===========================================================================

/// `run`: the real driver, after library bootstrap validation.
#[allow(clippy::too_many_lines)]
fn run_dispatch<W: Write>(sample: &Sample, flags: &Flags, out: &mut W) -> ExitCode {
    // --- Reserved-flag collision check (a hard, named bootstrap error). `--collide`
    // opts a colliding pipeline parameter in, so one binary demonstrates both.
    let params: Vec<ParamSpec> = if flags.has("collide") {
        vec![ParamSpec::new(
            "store",
            "a parameter colliding with --store",
        )]
    } else {
        vec![sample.param.clone()]
    };
    if let Err(collision) = check_reserved_collision(&params) {
        let _ = writeln!(out, "{collision}");
        return ExitCode::InvalidUsage;
    }

    let Some(base) = flags.get("store") else {
        let _ = writeln!(out, "run needs --store <base>");
        return ExitCode::InvalidUsage;
    };
    let run_id = flags.get("run-id").unwrap_or("run-1").to_string();

    // --- The durable-boundary producer path (single-node replay prerequisite).
    if flags.has("durable-boundary") {
        // Validate the pipeline parameter first (a bad value is still bootstrap).
        let mut supplied = BTreeMap::new();
        if let Some(v) = flags.get(&sample.param.name) {
            supplied.insert(sample.param.name.clone(), v.to_string());
        }
        let record_ref = !flags.has("non-durable");
        return if let Ok(carried) = validate_params(&params, &supplied) {
            produce_durable_boundary_run(sample, base, &run_id, &carried, record_ref, out)
        } else {
            emit_bootstrap_failed_artifact(sample, base, &run_id, &supplied);
            ExitCode::BootstrapFailure
        };
    }

    // --- Typed-parameter validation at bootstrap. An invalid value → the
    // bootstrap-failure artifact with NO node-execution events.
    let mut supplied = BTreeMap::new();
    if let Some(v) = flags.get(&sample.param.name) {
        supplied.insert(sample.param.name.clone(), v.to_string());
    }
    if let Err(_invalid) = validate_params(&params, &supplied) {
        emit_bootstrap_failed_artifact(sample, base, &run_id, &supplied);
        let _ = writeln!(
            out,
            "invalid parameter for `--{}`: rejected at bootstrap before any node executed",
            sample.param.name
        );
        return ExitCode::BootstrapFailure;
    }
    let carried = validate_params(&params, &supplied).expect("revalidated");

    // --- The sink-failure path.
    if flags.has("sink-fail") {
        let (pipeline, runners) = build_run_pipeline(RunKind::Success);
        let config = RunConfig::new("/nonexistent-t56")
            .run_id(&run_id)
            .parameters(carried);
        let report = drive(
            &config,
            sample.pipeline_name,
            Ok(RunPlan::new(pipeline, runners)),
            &[],
            FailingSink,
            DriverClock::default(),
        );
        return exit_code_for_run(&report);
    }

    // --- The assembly-failure path: run verbs mint identity + open the store
    // BEFORE assembly, so an assembly-failed artifact lands with zero attempts.
    let stream_path = run_dir(base, sample.pipeline_name, &run_id).join(EVENTS_FILE_NAME);
    let sink = match FileSink::create(&stream_path) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(out, "cannot open run store event stream: {e}");
            return ExitCode::SinkFailure;
        }
    };
    let mut config = RunConfig::new(base).run_id(&run_id).parameters(carried);
    if let Some(iv) = flags.get("data-interval") {
        if let Some((a, b)) = iv.split_once("..") {
            config = config.data_interval([a.to_string(), b.to_string()]);
        }
    }
    if flags.has("stop-on-first-failure") {
        config = config.failure_mode(FailureMode::StopOnFirstFailure);
    }

    let report = if flags.has("assembly-fail") {
        let broken = build_assembly_failing();
        let err = broken
            .assemble()
            .expect_err("the broken variant fails assembly");
        drive(
            &config,
            sample.pipeline_name,
            Err(err),
            &[],
            sink,
            DriverClock::default(),
        )
    } else {
        let kind = run_kind(flags);
        if matches!(kind, RunKind::TooBig) {
            config = config.capacities(PoolCapacities::new().memory(1));
        }
        let (pipeline, runners) = build_run_pipeline(kind);
        drive(
            &config,
            sample.pipeline_name,
            Ok(RunPlan::new(pipeline, runners)),
            &[],
            sink,
            DriverClock::default(),
        )
    };

    fold_stream_to_artifact(&stream_path);
    exit_code_for_run(&report)
}

/// Select the single-node run shape from the flags.
fn run_kind(flags: &Flags) -> RunKind {
    if flags.has("bootstrap-fail") {
        RunKind::TooBig
    } else if flags.has("stop-on-first-failure") {
        RunKind::StopOnFailure
    } else if flags.has("fail") {
        RunKind::Failure
    } else if flags.has("skip-only") {
        RunKind::SkipOnly
    } else {
        RunKind::Success
    }
}

// ===========================================================================
// The bootstrap-failed (bad parameter) artifact producer.
// ===========================================================================

/// Emit a `bootstrap-failed` run artifact for a rejected parameter: mint identity,
/// open the store, write the header, then a bootstrap-failed run-finished — with
/// NO node-execution events (rejected before any node runs).
fn emit_bootstrap_failed_artifact(
    sample: &Sample,
    base: &str,
    run_id: &str,
    supplied: &BTreeMap<String, String>,
) {
    let stream_path = run_dir(base, sample.pipeline_name, run_id).join(EVENTS_FILE_NAME);
    let Ok(sink) = FileSink::create(&stream_path) else {
        return;
    };
    let clock = StepClock::default();
    let mut writer = EventStreamWriter::new(
        sink,
        ClockRef(&clock),
        WireRunId::from_operator(run_id.to_string()),
        sample.pipeline_name,
    );
    clock.set(0);
    let _ = writer.run_started(RunStartedHeader {
        pipeline: sample.pipeline_name.to_string(),
        fingerprint_structural: None,
        fingerprint_policy: None,
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: supplied.clone(),
        data_interval: None,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    });
    let _ = writer.run_finished(RunOutcome::BootstrapFailed);
    let _ = writer.finish();
    fold_stream_to_artifact(&stream_path);
}

// ===========================================================================
// The durable-boundary producer.
// ===========================================================================

/// Produce a complete run whose durable `load` node recorded a durable reference
/// (so single-node replay can rehydrate it), driving the real C19 writer. Records
/// the parameters + verbatim data-interval in the run-started header (the
/// T55-deferred round-trip the acceptance suite asserts).
fn produce_durable_boundary_run<W: Write>(
    sample: &Sample,
    base: &str,
    run_id: &str,
    carried: &BTreeMap<String, String>,
    record_ref: bool,
    out: &mut W,
) -> ExitCode {
    let dir = run_dir(base, sample.pipeline_name, run_id);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        let _ = writeln!(out, "cannot create run dir: {e}");
        return ExitCode::SinkFailure;
    }
    let pipeline = build_pipeline(sample);
    if let Ok(graph_json) = emit_graph(
        &pipeline,
        sample.pipeline_name,
        "2026-07-24T00:00:00Z",
        &BuildProvenance::embedded(),
    ) {
        let _ = std::fs::write(dir.join(GRAPH_FILE_NAME), graph_json.as_bytes());
    }

    let stream_path = dir.join(EVENTS_FILE_NAME);
    let Ok(sink) = FileSink::create(&stream_path) else {
        let _ = writeln!(out, "cannot open event stream");
        return ExitCode::SinkFailure;
    };
    let clock = StepClock::default();
    let mut writer = EventStreamWriter::new(
        sink,
        ClockRef(&clock),
        WireRunId::from_operator(run_id.to_string()),
        sample.pipeline_name,
    );
    clock.set(0);
    // The T55-deferred assertion: record the parameters AND the verbatim
    // data-interval in the run-started header.
    let data_interval = Some([
        "2026-07-24T00:00:00Z".to_string(),
        "2026-07-25T00:00:00Z".to_string(),
    ]);
    let _ = writer.run_started(RunStartedHeader {
        pipeline: sample.pipeline_name.to_string(),
        fingerprint_structural: None,
        fingerprint_policy: None,
        fingerprint_algorithm_version: FINGERPRINT_ALGORITHM_VERSION,
        parameters: carried.clone(),
        data_interval,
        captured_env: BTreeMap::new(),
        resumed_from: None,
    });

    // `load` — the stage boundary's producer. Records a durable reference so a
    // single-node replay can rehydrate `transform`'s input (C27) — unless
    // `--non-durable` asked for the in-memory case, which the replay durability
    // gate must then refuse.
    let load_ref = record_ref.then(|| Rows(42).serialize_reference());
    emit_node(&mut writer, &clock, "load", 10, load_ref);
    // `transform` — consumes `load`'s durable value.
    emit_node(&mut writer, &clock, "transform", 50, None);
    // Remaining nodes.
    for node in pipeline.nodes() {
        let n = node.name();
        if n == "load" || n == "transform" {
            continue;
        }
        let status = if n == "decide-skip" {
            WireTerminalState::Skipped
        } else {
            WireTerminalState::Succeeded
        };
        emit_node_status(&mut writer, &clock, n, 90, status);
    }

    clock.set(1000);
    let _ = writer.run_finished(RunOutcome::Succeeded);
    let _ = writer.finish();

    fold_stream_to_artifact(&stream_path);
    let _ = writeln!(out, "durable-boundary run recorded at {}", dir.display());
    ExitCode::Success
}

/// Emit a succeeded node's lifecycle, optionally recording a durable reference.
fn emit_node(
    writer: &mut EventStreamWriter<FileSink, ClockRef<'_>>,
    clock: &StepClock,
    node: &str,
    base_offset: u64,
    durable_reference: Option<String>,
) {
    clock.set(base_offset);
    let _ = writer.node_ready(node);
    clock.set(base_offset + 1);
    let _ = writer.node_admitted(node);
    clock.set(base_offset + 2);
    let _ = writer.attempt_started(node, 1);
    clock.set(base_offset + 3);
    let _ = writer.attempt_succeeded(node, 1);
    let mut rec = AttemptOutcomeRecord {
        node: node.into(),
        attempt: 1,
        status: WireTerminalState::Succeeded.as_str().into(),
        ..AttemptOutcomeRecord::default()
    };
    if durable_reference.is_some() {
        record_durable_reference(&mut rec, durable_reference);
    }
    let _ = writer.attempt_outcome(rec);
    let _ = writer.node_terminal(node, WireTerminalState::Succeeded);
}

/// Emit a node's lifecycle with a chosen terminal `status`.
fn emit_node_status(
    writer: &mut EventStreamWriter<FileSink, ClockRef<'_>>,
    clock: &StepClock,
    node: &str,
    base_offset: u64,
    status: WireTerminalState,
) {
    clock.set(base_offset);
    let _ = writer.node_ready(node);
    let _ = writer.node_admitted(node);
    let _ = writer.attempt_started(node, 1);
    let _ = writer.attempt_succeeded(node, 1);
    let _ = writer.attempt_outcome(AttemptOutcomeRecord {
        node: node.into(),
        attempt: 1,
        status: status.as_str().into(),
        ..AttemptOutcomeRecord::default()
    });
    let _ = writer.node_terminal(node, status);
}

// ===========================================================================
// single-node replay.
// ===========================================================================

/// `single-node`: replay `--node <N>` from prior run `--from <run-dir>`
/// (rehydrating durable inputs), or standalone if the node consumes nothing.
/// Emits a replay-variant artifact marking every unselected node `not-requested`.
fn single_node_dispatch<W: Write>(sample: &Sample, flags: &Flags, out: &mut W) -> ExitCode {
    let Some(node) = flags.get("node") else {
        let _ = writeln!(out, "single-node needs --node <name>");
        return ExitCode::InvalidUsage;
    };
    let pipeline = build_pipeline(sample);
    let inputs = input_producer_names(&pipeline, node);

    if !inputs.is_empty() {
        let Some(from) = flags.get("from") else {
            let _ = writeln!(
                out,
                "single-node of a node with inputs needs --from <run-dir>"
            );
            return ExitCode::InvalidUsage;
        };
        let prior_path = PathBuf::from(from).join(RUN_ARTIFACT_FILE_NAME);
        let prior_bytes = match std::fs::read(&prior_path) {
            Ok(b) => b,
            Err(e) => {
                let _ = writeln!(out, "cannot read prior run artifact: {e}");
                return ExitCode::InvalidUsage;
            }
        };
        if let Some(code) = single_node_refusal_check(&prior_bytes, node, &inputs, out) {
            return code;
        }
    }

    let store = flags.get("store").unwrap_or("");
    let replay_run_id = flags.get("run-id").unwrap_or("replay-1");
    emit_replay_artifact(sample, &pipeline, node, store, replay_run_id, out)
}

/// The data-edge upstream producer names of `node` (its required inputs).
fn input_producer_names(pipeline: &Pipeline, node: &str) -> Vec<String> {
    let Some(n) = pipeline.nodes().find(|n| n.name() == node) else {
        return Vec::new();
    };
    n.data_edges()
        .iter()
        .filter_map(|e| pipeline.node(e.upstream()).map(|p| p.name().to_string()))
        .collect()
}

/// Run the requested node standalone and write a replay-variant run artifact under
/// the run store, marking every unselected node `not-requested`.
fn emit_replay_artifact<W: Write>(
    sample: &Sample,
    pipeline: &Pipeline,
    node: &str,
    store: &str,
    run_id: &str,
    out: &mut W,
) -> ExitCode {
    let terminal = run_node_standalone(node);
    let dir = if store.is_empty() {
        std::env::temp_dir().join(format!("t56-replay-{}", std::process::id()))
    } else {
        run_dir(store, sample.pipeline_name, run_id)
    };
    if let Err(e) = std::fs::create_dir_all(&dir) {
        let _ = writeln!(out, "cannot create replay run dir: {e}");
        return ExitCode::SinkFailure;
    }

    let mut attempts = Vec::new();
    for n in pipeline.nodes() {
        let name = n.name();
        let status = if name == node {
            terminal_str(terminal)
        } else {
            "not-requested"
        };
        attempts.push(serde_json::json!({
            "node": name,
            "attempt": u32::from(name == node),
            "status": status,
        }));
    }
    let artifact = serde_json::json!({
        "header": {
            "pipeline": sample.pipeline_name,
            "run_id": run_id,
            "variant": "single-node-replay",
        },
        "overall_outcome": if terminal == TerminalState::Failed { "failed" } else { "succeeded" },
        "requested_node": node,
        "attempts": attempts,
        "interrupted": false,
    });
    let path = dir.join(RUN_ARTIFACT_FILE_NAME);
    if let Err(e) = std::fs::write(&path, serde_json::to_string_pretty(&artifact).unwrap()) {
        let _ = writeln!(out, "cannot write replay artifact: {e}");
        return ExitCode::SinkFailure;
    }
    let _ = writeln!(out, "replayed `{node}` -> {}", terminal_str(terminal));
    if terminal == TerminalState::Failed {
        ExitCode::RunFailure
    } else {
        ExitCode::Success
    }
}

/// Run one node's real caught attempt to its terminal state, standalone.
fn run_node_standalone(node: &str) -> TerminalState {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime builds");
    rt.block_on(async move {
        let ctx = RunContext::for_test();
        let mut sink = NullAttemptSink;
        match node {
            "transform" => {
                // Rehydrated input (equal to the prior durable value).
                struct Rehydrated;
                impl Task for Rehydrated {
                    type Input = ();
                    type Output = Rows;
                    async fn run(&mut self, c: &RunContext, _i: ()) -> Result<Rows, TaskError> {
                        Transform.run(c, Rows(42)).await
                    }
                }
                run_attempt_caught(
                    &mut Rehydrated,
                    node,
                    &ctx,
                    &slot_for::<Rows>(node),
                    &mut sink,
                )
                .await
                .terminal_state()
            }
            "load" => run_attempt_caught(&mut Load, node, &ctx, &slot_for::<Rows>(node), &mut sink)
                .await
                .terminal_state(),
            _ => run_attempt_caught(
                &mut Standalone,
                node,
                &ctx,
                &slot_for::<()>(node),
                &mut sink,
            )
            .await
            .terminal_state(),
        }
    })
}

/// An attempt sink that discards records (the standalone replay's terminal state
/// is the observable; the acceptance suite reads the emitted artifact).
struct NullAttemptSink;
impl AttemptEventSink for NullAttemptSink {
    fn emit(&mut self, _event: AttemptEvent) {}
}

fn terminal_str(t: TerminalState) -> &'static str {
    match t {
        TerminalState::Succeeded => "succeeded",
        TerminalState::Failed => "failed",
        TerminalState::TimedOut => "timed-out",
        TerminalState::Skipped => "skipped",
        TerminalState::UpstreamSkipped => "upstream-skipped",
        TerminalState::UpstreamFailed => "upstream-failed",
        TerminalState::Cancelled => "cancelled",
        TerminalState::Abandoned => "abandoned",
        TerminalState::SatisfiedFromPrior => "satisfied-from-prior",
    }
}

// ===========================================================================
// prune.
// ===========================================================================

/// `prune`: delete old runs by `--keep <N>` (count) or `--older-than <nanos>`
/// (age) under `--store <base>`. Deletes NOTHING implicitly. Age is a per-run
/// numeric marker (`age.txt`) the acceptance suite plants, so pruning is
/// deterministic (no wall clock).
fn prune_dispatch<W: Write>(sample: &Sample, flags: &Flags, out: &mut W) -> ExitCode {
    let Some(base) = flags.get("store") else {
        let _ = writeln!(out, "prune needs --store <base>");
        return ExitCode::InvalidUsage;
    };
    let pipeline_dir = PathBuf::from(base).join(sample.pipeline_name);
    let mut runs = list_runs(&pipeline_dir);
    runs.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));

    if let Some(keep) = flags.get("keep").and_then(|s| s.parse::<usize>().ok()) {
        if runs.len() > keep {
            let to_delete = runs.len() - keep;
            for (dir, _age) in runs.iter().take(to_delete) {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
        return ExitCode::Success;
    }
    if let Some(threshold) = flags.get("older-than").and_then(|s| s.parse::<u64>().ok()) {
        for (dir, age) in &runs {
            if *age >= threshold {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
        return ExitCode::Success;
    }
    let _ = writeln!(out, "prune needs --keep <N> or --older-than <nanos>");
    ExitCode::InvalidUsage
}

/// The run directories under `<base>/<pipeline>/`, each with its planted numeric
/// age marker (`age.txt`, default 0 if absent).
fn list_runs(pipeline_dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(pipeline_dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let age = std::fs::read_to_string(path.join("age.txt"))
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        out.push((path, age));
    }
    out
}

// ===========================================================================
// fold helper.
// ===========================================================================

/// Fold the on-disk event stream at `stream_path` into a `run.json` next to it.
fn fold_stream_to_artifact(stream_path: &Path) {
    let Ok(mut file) = File::open(stream_path) else {
        return;
    };
    let mut bytes = Vec::new();
    if file.read_to_end(&mut bytes).is_err() {
        return;
    }
    if let Ok(artifact) = fold_stream(&bytes, &[]) {
        if let Some(dir) = stream_path.parent() {
            let _ = std::fs::write(
                dir.join(RUN_ARTIFACT_FILE_NAME),
                artifact.to_canonical_json(),
            );
        }
    }
}
