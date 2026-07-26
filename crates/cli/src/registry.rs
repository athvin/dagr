//! C7/C26 · **Flow registry** — one pipeline binary hosts **many named flows**
//! and selects one per invocation (arch.md `### C7`, `### C26`; ADR 086).
//!
//! # Why this module exists
//!
//! Until now a dagr pipeline binary carried exactly **one** flow: the reference
//! driver ([`crate`]'s `main`) reports "needs a pipeline-specific binary" for the
//! pipeline-bound verbs, and a real pipeline crate wires each verb to its single
//! assembled pipeline. There was no built-in way for one binary to offer
//! `dagr run etl` versus `dagr run nightly` and select a flow by name. An operator
//! asked for exactly that — define **many** named flows and pick one per
//! invocation, each `dagr run <flow>` being its own independent run (its own
//! run-id and store). ADR 086 records the fix this module ships.
//!
//! # The crux: factories, not stored flows
//!
//! [`RunnableFlow::run(self, …)`](RunnableFlow::run) **consumes** the flow
//! (`crates/cli/src/run_flow.rs`) and [`RunnableFlow`] is **not** `Clone`, so one
//! instance can serve at most one verb. Storing a built flow would let a binary
//! answer at most one verb. So the registry stores a **re-invokable factory**
//! `Fn() -> RunnableFlow` (boxed) and calls it **once per invocation**: each
//! `run <flow>` builds a *fresh* flow with its own run identity and store —
//! matching the operator's "each invocation its own thing". This is the only
//! pattern consistent with `run(self)` consuming the flow (ADR 086, rejected
//! alternatives).
//!
//! # What this slice ships (T74)
//!
//! The registry type, the [`Cli::flow_name`](crate::contract::Cli::flow_name)
//! contract extension (extracted in [`crate::contract::parse_cli`]), and the two
//! verbs [`run_registry`] routes here — `list` and `run <flow>`. The
//! remaining flow-selecting verbs (`graph`, `validate`, …) and the two-flow
//! example binary are **T75** (`docs/implementation/088-T75-…`), not this slice.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use dagr_artifact::event_stream::{EventSink, MonotonicClock, EVENTS_FILE_NAME};

use crate::contract::{
    exit_code_for_run, parse_cli, split_banner_flag, ExitCode, ParseOutcome, Verb,
};
use crate::run_flow::RunnableFlow;

/// The library-owned flag naming the run-store base for a `run <flow>` invocation
/// (the reserved `--store`, [`reserved_flag_names`](crate::contract::reserved_flag_names)).
const STORE_FLAG: &str = "--store";

/// The default run-store base when `--store` is omitted — a relative directory
/// under the current working directory, mirroring the examples' default
/// (`quickstart.rs` uses `./quickstart-runs`). A real deployment always passes
/// `--store`; the default keeps the ergonomic one-flow case runnable with no flag.
const DEFAULT_STORE_BASE: &str = "./dagr-runs";

/// A registered flow's **re-invokable factory** — called once per invocation to
/// build a **fresh** [`RunnableFlow`] (the flow is consumed by
/// [`run`](RunnableFlow::run), so it cannot be reused; see the module docs).
type FlowFactory = Box<dyn Fn() -> RunnableFlow + Send + Sync>;

/// A builder mapping a flow **name → a re-invokable factory** `Fn() -> RunnableFlow`
/// so **one** binary can host **many** named flows (arch.md `### C26`; ADR 086).
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
/// ```no_run
/// use dagr_cli::registry::{run_registry, FlowRegistry};
/// use dagr_cli::run_flow::RunnableFlow;
///
/// fn build_etl() -> RunnableFlow { RunnableFlow::new() }
/// fn build_nightly() -> RunnableFlow { RunnableFlow::new() }
///
/// let registry = FlowRegistry::new()
///     .add("etl", build_etl)
///     .add("nightly", build_nightly);
/// std::process::exit(run_registry(&registry, std::env::args_os()).as_u8().into());
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

    /// Resolve a `run` invocation's flow name to its factory, applying the ADR's
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

/// Dispatch a command line over `registry` and return the C26 [`ExitCode`], routing
/// human-readable diagnostics to the process's standard streams (arch.md `### C26`;
/// ADR 086). A pipeline binary calls this instead of hand-dispatching verbs:
///
/// ```no_run
/// # use dagr_cli::registry::{run_registry, FlowRegistry};
/// # let registry = FlowRegistry::new();
/// std::process::exit(run_registry(&registry, std::env::args_os()).as_u8().into());
/// ```
///
/// This slice (T74) routes two verbs: `list` (print the registered names, exit
/// [`ExitCode::Success`]) and `run <flow>` (build the selected flow via its
/// factory, drive it, and map the [`RunReport`](crate::run_flow::RunReport) through
/// [`exit_code_for_run`]). The remaining flow-selecting verbs are T75.
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
/// (arch.md C28: pipelines are testable without infrastructure). `out` receives the
/// `list` output and the `InvalidUsage` selection messages so a test can assert on
/// them; [`run_registry`] routes it to the process's standard streams.
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
    // C26 verb parser (which knows nothing of `list`).
    let raw: Vec<std::ffi::OsString> = argv.into_iter().map(Into::into).collect();
    // Strip the cosmetic banner flag exactly as the reference `main` does, so a
    // pipeline binary delegating here keeps `--no-banner` position-independent and
    // the flag never reaches the verb parser.
    let (_no_banner, argv) = split_banner_flag(raw.iter().cloned());

    // `list` is registry-specific (not a C26 library verb, so clap would reject it
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

    match cli.verb {
        Verb::Run => match registry.select(cli.flow_name.as_deref()) {
            Ok((name, factory)) => run_selected_flow(name, factory, &raw, out),
            Err(message) => {
                let _ = writeln!(out, "dagr: {message}");
                ExitCode::InvalidUsage
            }
        },
        // The remaining flow-selecting verbs (`graph`, `validate`, `single-node`,
        // `prune`) and the artifact-only verbs are **not** this slice's — T75 routes
        // them. Refuse them here with a defined code so the surface is honest.
        other => {
            let _ = writeln!(
                out,
                "the `{}` verb is not routed by this registry yet (T74 ships `run`/`list`; \
                 the remaining flow-selecting verbs are T75)",
                other.name()
            );
            ExitCode::InvalidUsage
        }
    }
}

/// Whether the first token after the program name (skipping the program name only)
/// equals `token` — how the registry recognizes its own `list` verb before the C26
/// parser, which knows nothing of it.
fn leading_token_is(argv: &[std::ffi::OsString], token: &str) -> bool {
    argv.get(1).map(std::ffi::OsString::as_os_str) == Some(std::ffi::OsStr::new(token))
}

/// Build the selected flow via its factory, drive it against a real on-disk C19
/// sink under the run store, and map the resulting run report to its C26 exit code
/// through [`exit_code_for_run`] (the numeric half of the exit-code table T55
/// owns). Each call builds a **fresh** flow (the factory is re-invoked), so two
/// `run` invocations run independently with their own run identity and store.
fn run_selected_flow<W: Write>(
    name: &str,
    factory: &FlowFactory,
    argv: &[std::ffi::OsString],
    out: &mut W,
) -> ExitCode {
    // A fresh flow, built now (never reused — `run(self)` consumes it).
    let flow = factory();
    let base = store_base(argv);

    // The run-store event-stream path is `<base>/<pipeline>/<run-id>/events.jsonl`;
    // the run mints its own id, so the id segment is unknown until the run resolves
    // it. The driver opens the stream through the injected sink, which writes under
    // a per-run directory we create eagerly with a stable minted id.
    let run_id = mint_run_id();
    let stream = PathBuf::from(&base)
        .join(name)
        .join(&run_id)
        .join(EVENTS_FILE_NAME);
    let sink = match FileSink::create(&stream) {
        Ok(sink) => sink,
        Err(err) => {
            // There is nowhere to write an artifact if the store cannot be opened;
            // the C26 sink-failure code covers an unwritable store at open.
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
            // failure short-circuits before execution (arch.md C7/C26).
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

/// Mint a run identity for a registry-dispatched run. A wall-clock timestamp,
/// process id, and a process-global monotonic counter together keep every
/// invocation's store directory disjoint (arch.md C19 concurrent-run disjointness)
/// without an external UUID dependency — even two invocations within the same
/// process and the same clock tick get distinct ids (the counter breaks the tie).
fn mint_run_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let seq = SEQ.fetch_add(1, Ordering::SeqCst);
    format!("run-{}-{nanos}-{seq}", std::process::id())
}

// ===========================================================================
// The internal on-disk C19 sink + monotonic clock the registry drives a flow with.
// ===========================================================================

/// A minimal append-only local-file C19 sink: appends each complete line to the
/// run's `events.jsonl` and flushes to the OS. Mirrors every pipeline binary's own
/// file sink (the run store is the operator's one job, arch.md "The shape of a
/// run"); the registry provides one so `run_registry` can drive a flow with no
/// caller-supplied sink.
struct FileSink {
    file: File,
}

impl FileSink {
    fn create(path: &std::path::Path) -> io::Result<Self> {
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

/// A monotonic clock advanced one tick per read — deterministic, wall-clock-free.
/// Durations in the artifact are computed from these monotonic offsets.
#[derive(Default)]
struct TickClock {
    n: AtomicU64,
}

impl MonotonicClock for TickClock {
    fn elapsed_ns(&self) -> u64 {
        self.n.fetch_add(1, Ordering::SeqCst)
    }
}
