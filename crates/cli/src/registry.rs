//! **Flow registry** — one pipeline binary hosts **many named flows**
//! and selects one per invocation.
//!
//! # Why this module exists
//!
//! Without this module a dagr pipeline binary carries exactly **one** flow: the
//! reference driver ([`crate`]'s `main`) reports "needs a pipeline-specific binary"
//! for the pipeline-bound verbs, and a real pipeline crate wires each verb to its
//! single assembled pipeline. There is no built-in way for one binary to offer
//! `dagr run etl` versus `dagr run nightly` and select a flow by name. This module
//! provides exactly that — define **many** named flows and pick one per invocation,
//! each `dagr run <flow>` being its own independent run (its own run-id and store).
//!
//! # The crux: factories, not stored flows
//!
//! [`RunnableFlow::run(self, …)`](RunnableFlow::run) **consumes** the flow
//! (`crates/cli/src/run_flow.rs`) and [`RunnableFlow`] is **not** `Clone`, so one
//! instance can serve at most one verb. Storing a built flow would let a binary
//! answer at most one verb. So the registry stores a **re-invokable factory**
//! `Fn() -> RunnableFlow` (boxed) and calls it **once per invocation**: each
//! `run <flow>` builds a *fresh* flow with its own run identity and store —
//! matching the "each invocation its own thing" requirement. This is the only
//! pattern consistent with `run(self)` consuming the flow.
//!
//! # What this module routes
//!
//! [`run_registry`] dispatches every flow-selecting verb over the registry:
//!
//! - `list` — print the registered flow names.
//! - `run <flow>` — build the selected flow, drive it, and map the
//!   [`RunReport`](crate::run_flow::RunReport) through [`exit_code_for_run`].
//! - `graph <flow>` — build the selected flow, finish it into a live [`Pipeline`]
//!   ([`RunnableFlow::into_pipeline`]), and emit the graph artifact via
//!   [`graph_verb`].
//! - `validate <flow>` — build the selected flow, finish it into a [`Pipeline`], and
//!   run assembly only via [`validate_verb`], printing **every** problem.
//!
//! # Per-verb exit-code fidelity
//!
//! Each verb maps its **own** outcome to its [`ExitCode`]: `run` goes through
//! [`exit_code_for_run`] (which is reserved for a completed
//! [`RunReport`](crate::run_flow::RunReport)); `graph` and `validate` return their
//! **own** `ExitCode`/error types **directly** — a `graph` emit failure and a
//! `validate` [`AssemblyFailure`](ExitCode::AssemblyFailure) never go through the
//! run path. Selection failures (a multi-flow registry with no name, an unknown
//! name) fail with [`InvalidUsage`](ExitCode::InvalidUsage) **before** the flow is
//! ever built.
//!
//!
//! # Discarded writes
//!
//! Every `let _ = writeln!(out, …)` in this module discards its
//! [`std::io::Write`] result **deliberately**. This paragraph is the
//! convention — a rule stated once, rather than the same comment repeated at each
//! of the sites — so a reviewer can check the rule instead of counting comments.
//!
//! The rule: *operator-facing output is a courtesy, never a result*. Each `out`
//! here is a caller-supplied writer that is, in practice, the process's
//! stdout/stderr, and the one failure they realistically hit is a **broken pipe**
//! — a downstream `head`, a closed terminal, a killed pager. Propagating that
//! would turn a successful run into a failed one and change the exit code the
//! orchestrator reads, which inverts the exit-code table's whole purpose: the code
//! reports what the *run* did, never what its printing did. Anything a caller must
//! be able to detect is returned as a value or an error instead, never printed and
//! then checked. A write whose failure *does* matter — the run store, the event
//! stream — is not in this module at all, and the handful of writes into an
//! in-memory `String` are infallible by construction.
//! # Out of scope for the registry (pipeline-bound verbs)
//!
//! `single-node` (attempt replay) and `prune` (run-store retention) select a flow
//! but need per-invocation store/parameter/rehydration plumbing the registry does
//! not own; the registry recognizes them and routes to a diagnostic pointing at the
//! pipeline-specific verb bodies (the reference sample binary `dagr-t56-alpha` wires
//! those directly). The artifact-only `render` / `fold` carry no flow and are
//! dispatched by the binary directly, not here.

use std::io::{self, Write};
use std::path::PathBuf;

use dagr_artifact::event_stream::EVENTS_FILE_NAME;
#[cfg(doc)]
use dagr_core::flow::Pipeline;

use crate::contract::{
    ExitCode, ParseOutcome, Verb, exit_code_for_run, parse_cli, split_banner_flag, validate_verb,
};
use crate::graph::graph_verb;
use crate::run_flow::RunnableFlow;
// The run-store defaults (the local-file sink, the deterministic clock the registry
// drives runs with, the default store base, and the run-id minter) live in one place
// so this path and the one-call `RunnableFlow::run_to_store` agree. The registry
// deliberately keeps injecting the deterministic `TickClock` (not `SystemClock`), so
// its streams stay byte-identical.
#[cfg(not(feature = "metastore"))]
use crate::run_store::FileSink;
use crate::run_store::{DEFAULT_STORE_BASE, TickClock, mint_run_id};

/// The library-owned flag naming the run-store base for a `run <flow>` invocation
/// (the reserved `--store`, [`reserved_flag_names`](crate::contract::reserved_flag_names)).
const STORE_FLAG: &str = "--store";

/// A registered flow's **re-invokable factory** — called once per invocation to
/// build a **fresh** [`RunnableFlow`] (the flow is consumed by
/// [`run`](RunnableFlow::run), so it cannot be reused; see the module docs).
type FlowFactory = Box<dyn Fn() -> RunnableFlow + Send + Sync>;

/// A builder mapping a flow **name → a re-invokable factory** `Fn() -> RunnableFlow`
/// so **one** binary can host **many** named flows.
///
/// Factories (not stored flows) are load-bearing: [`RunnableFlow::run(self)`](RunnableFlow::run)
/// **consumes** the flow and the type is not `Clone`, so one instance serves at
/// most one verb. Storing a `Fn() -> RunnableFlow` and calling it once per
/// invocation lets each `run <flow>` build a fresh flow with its own run identity
/// and store.
///
/// Build one with [`new`](Self::new) + [`add`](Self::add) for the many-flows case,
/// or [`single_flow`](Self::single_flow) for the one-flow ergonomic default (its
/// name may be omitted on the command line), then dispatch with [`run_registry`]:
///
/// ```
/// use dagr_cli::contract::ExitCode;
/// use dagr_cli::registry::{run_registry, FlowRegistry};
/// use dagr_cli::run_flow::RunnableFlow;
///
/// fn build_etl() -> RunnableFlow { RunnableFlow::new() }
/// fn build_nightly() -> RunnableFlow { RunnableFlow::new() }
///
/// let registry = FlowRegistry::new()
///     .add("etl", build_etl)
///     .add("nightly", build_nightly);
///
/// // `list` prints the registered names, in registration order, and succeeds.
/// assert_eq!(run_registry(&registry, ["pipeline", "list"]), ExitCode::Success);
///
/// // In a real `main`, dispatch the process's own argv and exit with the code:
/// //   std::process::exit(run_registry(&registry, std::env::args_os()).as_u8().into());
/// ```
#[derive(Default)]
pub struct FlowRegistry {
    /// The registered `(name, factory)` pairs, in **registration order** — the
    /// deterministic order `list` prints and the "available flows" messages list.
    flows: Vec<(String, FlowFactory)>,
    /// Whether this registry was built as a [single-flow](Self::single_flow)
    /// registry: its one flow's name may be **omitted** on `run` (the ergonomic
    /// default for the common one-flow binary).
    single_flow: bool,
}

impl FlowRegistry {
    /// An **empty** registry. Register flows with [`add`](Self::add).
    #[must_use]
    pub fn new() -> Self {
        Self {
            flows: Vec::new(),
            single_flow: false,
        }
    }

    /// Register a named `factory` and return the registry for chaining
    /// (`FlowRegistry::new().add("etl", build_etl).add("nightly", build_nightly)`).
    ///
    /// `factory` is any `Fn() -> RunnableFlow` (a plain `fn`, or a closure), stored
    /// boxed and re-invoked **once per invocation** — a fresh flow each time.
    #[must_use]
    pub fn add<F>(mut self, name: impl Into<String>, factory: F) -> Self
    where
        F: Fn() -> RunnableFlow + Send + Sync + 'static,
    {
        self.flows.push((name.into(), Box::new(factory)));
        self
    }

    /// The **one-flow ergonomic** constructor: a registry with a single flow whose
    /// name may be **omitted** on the command line (`dagr run` dispatches the sole
    /// flow). This is the common one-flow binary's default.
    ///
    /// The flow is registered under the reserved name `"flow"` for `list` and the
    /// store path; because the name may be omitted, an operator never types it.
    #[must_use]
    pub fn single_flow<F>(factory: F) -> Self
    where
        F: Fn() -> RunnableFlow + Send + Sync + 'static,
    {
        Self {
            flows: vec![("flow".to_string(), Box::new(factory))],
            single_flow: true,
        }
    }

    /// The registered flow names, in registration order (what `list` prints and
    /// the "available flows" diagnostics list).
    fn names(&self) -> Vec<&str> {
        self.flows.iter().map(|(name, _)| name.as_str()).collect()
    }

    /// Resolve a `run` invocation's flow name to its factory, applying the
    /// selection rules: a single-flow registry serves the omitted name; a
    /// multi-flow registry requires a name; an unknown name is refused. Returns the
    /// selected `(name, factory)` on success, or the [`ExitCode::InvalidUsage`]
    /// diagnostic to print on refusal.
    fn select(&self, requested: Option<&str>) -> Result<(&str, &FlowFactory), String> {
        match requested {
            Some(name) => self
                .flows
                .iter()
                .find(|(n, _)| n == name)
                .map(|(n, f)| (n.as_str(), f))
                .ok_or_else(|| {
                    format!(
                        "unknown flow `{name}` (available flows: {})",
                        self.names().join(", ")
                    )
                }),
            // No name given: a single-flow registry dispatches its sole flow; a
            // multi-flow registry requires the operator to name one.
            None if self.single_flow || self.flows.len() == 1 => {
                let (n, f) = &self.flows[0];
                Ok((n.as_str(), f))
            }
            None => Err(format!(
                "flow name required ({}); pass one on `run <flow>`",
                self.names().join(", ")
            )),
        }
    }
}

/// Dispatch a command line over `registry` and return the [`ExitCode`], routing
/// human-readable diagnostics to the process's standard streams. A pipeline binary
/// calls this instead of hand-dispatching verbs:
///
/// ```
/// use dagr_cli::contract::ExitCode;
/// use dagr_cli::registry::{run_registry, FlowRegistry};
/// use dagr_cli::run_flow::RunnableFlow;
///
/// fn build_etl() -> RunnableFlow { RunnableFlow::new() }
/// let registry = FlowRegistry::new().add("etl", build_etl);
///
/// // An unknown flow is invalid usage, not a failed run — and says so on stdout.
/// assert_eq!(
///     run_registry(&registry, ["pipeline", "run", "nope"]),
///     ExitCode::InvalidUsage,
/// );
///
/// // In a real `main`, the process's own argv and exit code:
/// //   std::process::exit(run_registry(&registry, std::env::args_os()).as_u8().into());
/// ```
///
/// It routes the flow-selecting verbs: `list` (print the registered names, exit
/// [`ExitCode::Success`]), `run <flow>` (build the selected flow via its factory,
/// drive it, and map the [`RunReport`](crate::run_flow::RunReport) through
/// [`exit_code_for_run`]), and `graph <flow>` / `validate <flow>`.
#[must_use]
pub fn run_registry<I, T>(registry: &FlowRegistry, argv: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    // Route diagnostics to stdout (the `list` output and the selection messages).
    // A run's own machine-readable stream is the on-disk event stream, not stdout.
    let mut stdout = io::stdout().lock();
    run_registry_to(registry, argv, &mut stdout)
}

/// [`run_registry`] with an explicit diagnostic writer — the testable entrypoint
/// (pipelines are testable without infrastructure). `out` receives the `list`
/// output and the `InvalidUsage` selection messages so a test can assert on them;
/// [`run_registry`] routes it to the process's standard streams.
#[must_use]
pub fn run_registry_to<I, T, W>(registry: &FlowRegistry, argv: I, out: &mut W) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
    W: Write,
{
    // Materialize argv once: the registry reads the library-reserved `--store`
    // value from the trailing tokens itself (so `run_registry` needs no ambient
    // process args), and `list` is a registry-specific verb we recognize before the
    // verb parser (which knows nothing of `list`).
    let raw: Vec<std::ffi::OsString> = argv.into_iter().map(Into::into).collect();
    // Strip the cosmetic banner flag exactly as the reference `main` does, so a
    // pipeline binary delegating here keeps `--no-banner` position-independent and
    // the flag never reaches the verb parser.
    let (_no_banner, argv) = split_banner_flag(raw.iter().cloned());

    // `list` is registry-specific (not a library verb, so clap would reject it
    // as an unknown subcommand). Recognize it as the leading token before parsing.
    if leading_token_is(&argv, "list") {
        for name in registry.names() {
            let _ = writeln!(out, "{name}");
        }
        return ExitCode::Success;
    }

    let cli = match parse_cli(argv) {
        ParseOutcome::Parsed(cli) => cli,
        ParseOutcome::Help { exit, text } => {
            let _ = write!(out, "{text}");
            return exit;
        }
        ParseOutcome::Error { exit, message } => {
            let _ = writeln!(out, "dagr: {message}");
            return exit;
        }
    };

    // Every flow-selecting verb resolves its flow through the SAME selection rules
    // (single-flow default, name-required, unknown-name), then routes to its own
    // verb body. Selection failures never reach the flow build.
    let mut dispatch =
        |verb_label: &str, action: FlowAction<W>| match registry.select(cli.flow_name.as_deref()) {
            Ok((name, factory)) => action(name, factory, &raw, out),
            Err(message) => {
                let _ = writeln!(out, "dagr {verb_label}: {message}");
                ExitCode::InvalidUsage
            }
        };

    match cli.verb {
        Verb::Run => dispatch("run", run_selected_flow),
        // `graph <flow>` builds a FRESH flow, finishes it into a live `&Pipeline`,
        // and emits the graph artifact — its own `ExitCode`, never via the run path.
        Verb::Graph => dispatch("graph", graph_selected_flow),
        // `validate <flow>` builds another fresh flow, finishes it into a `Pipeline`,
        // and runs assembly-only — `AssemblyFailure` (3) comes straight from the verb.
        Verb::Validate => dispatch("validate", validate_selected_flow),
        // `single-node` / `prune` select a flow but need per-invocation store /
        // parameter / rehydration plumbing the registry does not own. Recognize them
        // and point at the pipeline-specific verb wiring rather than silently
        // succeeding (per-verb exit-code fidelity; the sample binary
        // `dagr-t56-alpha` wires those directly). Still refuse an unknown/absent flow
        // name here first, so selection stays consistent across every verb.
        verb @ (Verb::SingleNode | Verb::Prune) => {
            match registry.select(cli.flow_name.as_deref()) {
                Ok((name, _factory)) => {
                    let v = verb.name();
                    let _ = writeln!(
                        out,
                        "dagr {v}: the `{v}` verb selects flow `{name}` but needs the \
                         pipeline-specific verb body (its store/parameter/replay plumbing is \
                         not part of the registry entrypoint); wire it directly against the \
                         selected flow's assembled pipeline (dagr_cli::contract / \
                         dagr_cli::driver)",
                    );
                    ExitCode::InvalidUsage
                }
                Err(message) => {
                    let _ = writeln!(out, "dagr {}: {message}", verb.name());
                    ExitCode::InvalidUsage
                }
            }
        }
        // The artifact-only verbs (`render`, `fold`) and the `resume` stub carry no
        // flow, so they are dispatched by the binary directly, never through the
        // registry. A binary that delegates here for them is misrouting.
        other @ (Verb::Render | Verb::Resume | Verb::Fold) => {
            let _ = writeln!(
                out,
                "dagr: the `{}` verb carries no flow and is not routed by the registry \
                 (dispatch it directly — it needs no factory)",
                other.name()
            );
            ExitCode::InvalidUsage
        }
    }
}

/// The signature every flow-selecting verb body shares: given the selected flow's
/// `name` + `factory`, the invocation `argv`, and the diagnostic writer, build a
/// fresh flow and produce the verb's own [`ExitCode`]. A plain `fn` pointer so
/// the `run`/`graph`/`validate` bodies and the inline `single-node`/`prune`
/// diagnostic all coerce to one type the dispatcher can route.
type FlowAction<W> = fn(&str, &FlowFactory, &[std::ffi::OsString], &mut W) -> ExitCode;

/// Build the selected flow via its factory and emit its **graph artifact**
/// through [`graph_verb`] over the flow's live [`Pipeline`]
/// ([`RunnableFlow::into_pipeline`]). The clock is read **once** for the
/// generation-time field (the only byte-varying field); every other byte is
/// fixed per binary, so this is byte-identical to a single-flow binary's emission
/// for the same flow. The exit code is the verb's **own**: `Success` on a clean
/// emit, `AssemblyFailure` on a flow that cannot be emitted to the contract — never
/// via [`exit_code_for_run`].
fn graph_selected_flow<W: Write>(
    name: &str,
    factory: &FlowFactory,
    _argv: &[std::ffi::OsString],
    out: &mut W,
) -> ExitCode {
    // A fresh flow, built now, finished into the immutable pipeline the graph verb
    // reads (never reused — `into_pipeline` consumes it).
    let pipeline = factory().into_pipeline();
    let generated_at = now_rfc3339();
    match graph_verb(&pipeline, name, &generated_at, out) {
        Ok(()) => ExitCode::Success,
        Err(err) => {
            let _ = writeln!(out, "dagr graph {name}: {err}");
            // A structural emit failure (a node without stable names, a malformed
            // stable name) is the graph's own fault, surfaced before any run —
            // AssemblyFailure is the exit code for "the graph itself is unusable".
            ExitCode::AssemblyFailure
        }
    }
}

/// Build the selected flow via its factory and run **assembly only** through
/// [`validate_verb`] over the flow's live [`Pipeline`]. Prints **every** problem and
/// returns the verb's **own** code: `Success` on a clean assembly or
/// [`AssemblyFailure`](ExitCode::AssemblyFailure) (3) otherwise — never via
/// [`exit_code_for_run`], which is reserved for a completed
/// [`RunReport`](crate::run_flow::RunReport).
fn validate_selected_flow<W: Write>(
    name: &str,
    factory: &FlowFactory,
    _argv: &[std::ffi::OsString],
    out: &mut W,
) -> ExitCode {
    let pipeline = factory().into_pipeline();
    let _ = name; // the flow's identity is not part of assembly (assembly is name-agnostic)
    validate_verb(&pipeline, out)
}

/// Format the current wall-clock instant as an RFC 3339 UTC timestamp
/// (`YYYY-MM-DDThh:mm:ssZ`) for the graph artifact's generation-time field.
///
/// This is the **only** byte-varying field of the artifact, so it is not
/// fingerprint-bound and needs no sub-second precision; a plain second-granularity
/// UTC stamp keeps the registry free of a date-formatting dependency. Computed by
/// the standard civil-from-days algorithm (Howard Hinnant's `civil_from_days`) so
/// it is correct across leap years with no external crate.
fn now_rfc3339() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Convert a count of days **since the Unix epoch** (1970-01-01) to a `(year,
/// month, day)` civil date, via Howard Hinnant's public-domain `civil_from_days`
/// algorithm. Written in unsigned arithmetic (valid for every non-negative Unix
/// timestamp, i.e. every real generation time) so it needs no external crate and no
/// lossy signed casts; it is correct across leap years and century rules.
fn civil_from_days(z: u64) -> (u64, u64, u64) {
    // Shift the epoch to 0000-03-01 (era-relative), the algorithm's origin.
    let z = z + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097; // day-of-era, [0, 146_096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // year-of-era, [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day-of-year (Mar-based), [0, 365]
    let mp = (5 * doy + 2) / 153; // Mar-based month index, [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

/// Whether the first token after the program name (skipping the program name only)
/// equals `token` — how the registry recognizes its own `list` verb before the
/// verb parser, which knows nothing of it.
fn leading_token_is(argv: &[std::ffi::OsString], token: &str) -> bool {
    argv.get(1).map(std::ffi::OsString::as_os_str) == Some(std::ffi::OsStr::new(token))
}

/// Build the selected flow via its factory, drive it against a real on-disk event
/// sink under the run store, and map the resulting run report to its exit code
/// through [`exit_code_for_run`]. Each call builds a **fresh** flow (the factory is
/// re-invoked), so two `run` invocations run independently with their own run
/// identity and store.
fn run_selected_flow<W: Write>(
    name: &str,
    factory: &FlowFactory,
    argv: &[std::ffi::OsString],
    out: &mut W,
) -> ExitCode {
    // A fresh flow, built now (never reused — `run(self)` consumes it).
    let flow = factory();
    let base = store_base(argv);

    // The local codec check (`--dagr.force-roundtrip` / `DAGR_FORCE_ROUNDTRIP`,
    // default off). The factory built the flow without ever seeing this invocation,
    // so the answer is applied here — the toggle cell is shared with the nodes
    // already registered. Off, nothing about the run changes.
    let force_roundtrip = match crate::config::parse_force_roundtrip_flag(argv)
        .and_then(|flag| crate::config::resolve_force_roundtrip(flag).map_err(|e| e.to_string()))
    {
        Ok(on) => on,
        Err(detail) => {
            // A bad toggle value is invalid usage, never a silent "off".
            let _ = writeln!(out, "dagr run {name}: {detail}");
            return ExitCode::InvalidUsage;
        }
    };
    let flow = flow.force_roundtrip(force_roundtrip);

    // The run-store event-stream path is `<base>/<pipeline>/<run-id>/events.jsonl`;
    // the run mints its own id, so the id segment is unknown until the run resolves
    // it. The driver opens the stream through the injected sink, which writes under
    // a per-run directory we create eagerly with a stable minted id.
    let run_id = mint_run_id();
    let stream = PathBuf::from(&base)
        .join(name)
        .join(&run_id)
        .join(EVENTS_FILE_NAME);

    // Build the run sink. With the default-off `metastore` feature enabled AND the
    // `--dagr.metastore` / `DAGR_METASTORE` toggle on, the sink is a TEE of the
    // on-disk `events.jsonl` and the guaranteed live `MetastoreSink` (T86); off (or
    // feature-disabled) it is the plain `FileSink` — byte-for-byte the historical
    // behavior, with NO `libsql` activity.
    #[cfg(feature = "metastore")]
    let sink = match crate::metastore_tee::build_run_sink(&stream, &base, argv) {
        Ok(sink) => sink,
        Err(err) => {
            // There is nowhere to write an artifact if the on-disk store or the live
            // index cannot be opened; the sink-failure code covers an unwritable
            // target at open (a bad `--dagr.metastore` value is invalid usage).
            let code = if err.kind() == std::io::ErrorKind::InvalidInput {
                ExitCode::InvalidUsage
            } else {
                ExitCode::SinkFailure
            };
            let _ = writeln!(
                out,
                "dagr run {name}: cannot open the run store at {}: {err}",
                stream.display()
            );
            return code;
        }
    };
    #[cfg(not(feature = "metastore"))]
    let sink = match FileSink::create(&stream) {
        Ok(sink) => sink,
        Err(err) => {
            // There is nowhere to write an artifact if the store cannot be opened;
            // the sink-failure code covers an unwritable store at open.
            let _ = writeln!(
                out,
                "dagr run {name}: cannot open the run store at {}: {err}",
                stream.display()
            );
            return ExitCode::SinkFailure;
        }
    };

    let config = crate::driver::RunConfig::new(base).run_id(run_id);
    match flow.run(name, &config, sink, TickClock::default()) {
        Ok(report) => exit_code_for_run(report.driver_report()),
        Err(err) => {
            // A flow that does not assemble is the graph's fault — the assembly
            // failure short-circuits before execution.
            let _ = writeln!(out, "dagr run {name}: the flow did not assemble: {err}");
            ExitCode::AssemblyFailure
        }
    }
}

/// The run-store base for a `run <flow>` invocation: the `--store DIR` value if the
/// operator passed one, else [`DEFAULT_STORE_BASE`]. The store flag lives in the
/// undifferentiated trailing args the pipeline binary owns; we read only the
/// library-reserved `--store` from the invocation's `argv`, leaving the rest
/// untouched.
fn store_base(argv: &[std::ffi::OsString]) -> String {
    let store = std::ffi::OsStr::new(STORE_FLAG);
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        if arg.as_os_str() == store {
            if let Some(value) = it.next() {
                return value.to_string_lossy().into_owned();
            }
        }
    }
    DEFAULT_STORE_BASE.to_string()
}
