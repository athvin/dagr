//! C25 · **Logging and tracing integration** — the developer/operator
//! observability surface (arch.md `### C25 · Logging integration`; ticket T45).
//!
//! # What this module owns
//!
//! A failed run must be debuggable from logs alone, without correlating
//! wall-clock timestamps across interleaved concurrent nodes. This module wires
//! the framework's [`tracing`] subscriber so that:
//!
//! - **Every attempt runs inside a span** carrying run identity, node identity,
//!   and attempt number ([`attempt_span`]). Any line emitted beneath it — a line
//!   the task writes, or a line a **third-party library the task calls** writes
//!   through the global [`tracing`] facade — inherits those fields, so a reader
//!   attributes each line to its node and attempt **without** inspecting or
//!   ordering timestamps. The driver instruments each attempt future with the
//!   span (`.instrument(...)`), so the span follows the future across `.await`
//!   points, and concurrently executing nodes stay unambiguously separable by
//!   their span fields.
//! - **Output is structured by default and human-readable on request.** The
//!   default ([`OutputMode::Structured`]) is line-delimited JSON exposing
//!   run/node/attempt as discrete, machine-queryable fields; the local-development
//!   mode ([`OutputMode::Human`]) is the readable `fmt` format over the *same*
//!   event data. Selection is by the [`LOG_FORMAT_ENV`] environment variable —
//!   no code change and no recompile — and an unset or unrecognized value falls
//!   back **deterministically** to the structured default (M3 ships env-var
//!   selection only; a library-owned CLI flag is deferred to M4 per C26).
//! - **Exactly one process-global subscriber is installed at bootstrap**
//!   ([`init_tracing`]), installed **once**, coexisting with the test harness's
//!   own subscriber and the C14 panic hook, and never double-installing across
//!   repeated calls in one process.
//!
//! # Where tracing lives, and why not in `dagr-core`
//!
//! `dagr-core` is kept **dependency-free** (arch.md "Stability"; the T1 crate-layout
//! ADR): it exposes only the dep-free [`LogSpan`]
//! *identity payload* on the [`RunContext`](dagr_core::context::RunContext). The
//! [`tracing`] dependency and the subscriber wiring live **here**, in `dagr-cli`
//! — the pipeline binary that owns bootstrap and the run loop where attempts
//! execute. Core emits no trace points itself; the driver reads the identity off
//! the context and opens the span around each attempt.
//!
//! # Tracing is not the durable record (C25 ≠ C19)
//!
//! This layer is the **developer/operator observability** surface and is
//! deliberately distinct from the **C19 event stream**, which is the durable,
//! append-only authoritative record of a run. A tracing line is *not* an
//! event-stream record and never replaces one: the event stream is written
//! through the run store's sink and is what artifacts (C20/C22) are folded from;
//! tracing is the human/operator log-and-trace output. Keeping them separate is
//! why this module writes no artifacts and touches no run store.
//!
//! # The secret-redaction boundary (C9 · C25)
//!
//! A value **marked secret** in the C9 [`ResourceRegistry`](dagr_core::context::ResourceRegistry)
//! (wrapped in [`Secret`](dagr_core::context::Secret)) never appears on any
//! **framework-controlled** output path — log lines, span fields, or
//! framework-formatted diagnostics. This holds **by construction**:
//! [`Secret`](dagr_core::context::Secret)
//! implements neither `Debug` nor `Display`, so framework code — which reaches
//! values only through those formatters — literally cannot render the wrapped
//! bytes onto a trace line (a framework `{:?}`/`{}` on a secret fails to compile),
//! and the registry's own hand-written `Debug` never renders resource values.
//!
//! **The boundary, stated explicitly:** a task author who calls
//! [`Secret::expose`](dagr_core::context::Secret::expose) and formats the revealed
//! value into **their own** log line is **outside** this guarantee — dagr does not
//! intercept or sanitize task-authored content and cannot scrub a string the
//! author built themselves. The guarantee covers exactly framework-controlled
//! paths, in both output modes.

use std::sync::atomic::{AtomicBool, Ordering};

use dagr_core::context::LogSpan;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

/// The environment variable that selects the log **output mode** at run time
/// (arch.md C25; M3 env-var selection, C26 CLI flag deferred to M4).
///
/// Set it to `human` for the human-readable local-development format;
/// `structured` (or leaving it unset, or any unrecognized value) selects the
/// machine-queryable structured default. Selection is case-insensitive.
/// Switching modes requires only this environment variable — no code change and
/// no recompile of the same binary.
pub const LOG_FORMAT_ENV: &str = "DAGR_LOG_FORMAT";

/// The maximum trace level the framework subscriber records. `INFO` is the
/// operator-useful default: run/node/attempt lifecycle and task `info!` lines are
/// captured, while `debug!`/`trace!` noise is suppressed unless a future ticket
/// wires a level knob. A level cap set directly here is why `env-filter` (and its
/// `regex` tree) is not pulled into the dependency set.
const MAX_LEVEL: Level = Level::INFO;

/// The framework log **output mode** (arch.md C25): the *same* event data
/// rendered either machine-queryably or human-readably.
///
/// Two modes over one event stream, switchable with no code change via
/// [`LOG_FORMAT_ENV`]:
///
/// - [`Structured`](OutputMode::Structured) — the **default**. Line-delimited
///   JSON exposing run/node/attempt as discrete, queryable fields (not only free
///   text). This is what a log-query tool consumes.
/// - [`Human`](OutputMode::Human) — the readable `fmt` format for local
///   development.
///
/// An **unset or unrecognized** [`LOG_FORMAT_ENV`] value resolves to
/// [`Structured`](OutputMode::Structured) **deterministically**
/// ([`from_env_value`](OutputMode::from_env_value)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Machine-queryable line-delimited JSON — the documented default.
    Structured,
    /// Human-readable `fmt` output for local development.
    Human,
}

impl OutputMode {
    /// The mode selected by the process's [`LOG_FORMAT_ENV`] environment
    /// variable, falling back to the documented [`Structured`](OutputMode::Structured)
    /// default when the variable is unset or unrecognized.
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_env_value(std::env::var(LOG_FORMAT_ENV).ok().as_deref())
    }

    /// Resolve a raw [`LOG_FORMAT_ENV`] value (or its absence) to an [`OutputMode`]
    /// **deterministically** (arch.md C25): `human` → [`Human`](OutputMode::Human);
    /// `structured` → [`Structured`](OutputMode::Structured); **anything else,
    /// including [`None`] and an empty or unrecognized string → the documented
    /// [`Structured`](OutputMode::Structured) default**. Matching is
    /// case-insensitive and trims surrounding whitespace, so an operator's casing
    /// or a stray space never silently changes the mode.
    #[must_use]
    pub fn from_env_value(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("human") => OutputMode::Human,
            // "structured", unset, empty, and every unrecognized value fall back
            // to the structured default — deterministic, never a surprise.
            _ => OutputMode::Structured,
        }
    }
}

/// A process-wide marker recording whether the framework's global tracing
/// subscriber has been installed, so a repeat [`init_tracing`] is a no-op rather
/// than a double-install error.
static SUBSCRIBER_INSTALLED: AtomicBool = AtomicBool::new(false);

/// Install the framework's **single process-global** tracing subscriber at
/// bootstrap (arch.md C25), selecting the [output mode](OutputMode) from the
/// [`LOG_FORMAT_ENV`] environment variable.
///
/// Returns `true` if **this** call installed the subscriber, `false` if a
/// subscriber was already in place (a prior [`init_tracing`], or the test
/// harness's own). It is **idempotent and coexistence-safe**:
///
/// - It uses `try_init`, so when a global subscriber already exists (the test
///   harness installs one) this **does not panic or error** — it returns `false`
///   and leaves the existing subscriber untouched, exactly the "coexists with the
///   test harness" requirement.
/// - A repeat call in the same process is a **no-op**: at most one install per
///   process, so it never double-installs across multiple runs in one process.
///
/// It coexists with the C14 panic hook, which is installed on a separate seam
/// ([`install_panic_hook`](dagr_core::execution::install_panic_hook)) and is
/// orthogonal to the subscriber.
///
/// The return value is informational; the bootstrap caller legitimately discards
/// it (`let _ = init_tracing();`) since installing-or-coexisting is all it needs.
#[must_use = "the return value reports whether THIS call installed the subscriber; \
              a bootstrap caller may discard it with `let _ =`"]
pub fn init_tracing() -> bool {
    init_tracing_with(OutputMode::from_env())
}

/// Install the global subscriber in an explicit [`OutputMode`] (the mode-agnostic
/// core of [`init_tracing`], useful when the mode is chosen by something other
/// than the environment). Same idempotent, coexistence-safe contract:
/// `try_init` never panics when a subscriber already exists, and a repeat call is
/// a no-op.
#[must_use = "the return value reports whether THIS call installed the subscriber"]
pub fn init_tracing_with(mode: OutputMode) -> bool {
    // Fast path: already installed by us — a repeat call is a plain no-op.
    if SUBSCRIBER_INSTALLED.load(Ordering::Acquire) {
        return false;
    }
    // `try_init` returns Err when a global subscriber (e.g. the test harness's)
    // is already set — we treat that as "coexist, do not install", never a panic.
    let installed = match mode {
        OutputMode::Structured => structured_builder(std::io::stdout).try_init().is_ok(),
        OutputMode::Human => human_builder(std::io::stdout).try_init().is_ok(),
    };
    if installed {
        SUBSCRIBER_INSTALLED.store(true, Ordering::Release);
    }
    installed
}

/// Build the **structured** (machine-queryable JSON) subscriber over the given
/// writer, as a value a caller can set as the scoped default
/// (`tracing::subscriber::with_default`) or install globally.
///
/// Each record is a JSON object exposing the current attempt span's run/node/
/// attempt as discrete fields under a `span` key (the C25 "queryable, not only
/// free text" contract), so a captured line is attributable without timestamp
/// correlation. This is the builder [`init_tracing`] installs for
/// [`OutputMode::Structured`], and the one the acceptance tests capture with.
pub fn structured_subscriber<W>(writer: W) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    structured_builder(writer).finish()
}

/// Build the **human-readable** (`fmt`) subscriber over the given writer — the
/// local-development mode. It renders the *same* event data as
/// [`structured_subscriber`] (including the current span's node/attempt), only
/// formatted for a human rather than a machine. This is the builder
/// [`init_tracing`] installs for [`OutputMode::Human`].
pub fn human_subscriber<W>(writer: W) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    human_builder(writer).finish()
}

/// The structured-mode `fmt` builder: JSON output, the current span's fields
/// included (so run/node/attempt appear as discrete keys), ANSI off (structured
/// output is consumed by machines), capped at [`MAX_LEVEL`].
fn structured_builder<W>(
    writer: W,
) -> tracing_subscriber::fmt::SubscriberBuilder<
    tracing_subscriber::fmt::format::JsonFields,
    tracing_subscriber::fmt::format::Format<tracing_subscriber::fmt::format::Json>,
    tracing_subscriber::filter::LevelFilter,
    W,
>
where
    W: for<'a> MakeWriter<'a> + 'static,
{
    tracing_subscriber::fmt::Subscriber::builder()
        .json()
        // Include the current span's fields (run/node/attempt) on every record so
        // every line is attributable to its node+attempt as discrete fields.
        .with_current_span(true)
        .with_span_list(false)
        .with_max_level(MAX_LEVEL)
        .with_writer(writer)
}

/// The human-mode `fmt` builder: the readable text format with the current span's
/// fields, ANSI off (so captured/redirected output has no escape codes), capped
/// at [`MAX_LEVEL`].
fn human_builder<W>(
    writer: W,
) -> tracing_subscriber::fmt::SubscriberBuilder<
    tracing_subscriber::fmt::format::DefaultFields,
    tracing_subscriber::fmt::format::Format,
    tracing_subscriber::filter::LevelFilter,
    W,
>
where
    W: for<'a> MakeWriter<'a> + 'static,
{
    tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(MAX_LEVEL)
        // ANSI off keeps redirected/captured human output free of escape codes so
        // it is still greppable (and the redaction tests can scan it plainly).
        .with_ansi(false)
        .with_writer(writer)
}

/// Open the **attempt span** every attempt runs inside (arch.md C25 · C14): a span
/// carrying the `run`, `node`, and `attempt` identity as recorded fields, so any
/// line emitted beneath it — framework-emitted or from a third-party library the
/// task calls — is attributable to that node and attempt **without** timestamp
/// correlation.
///
/// The driver opens this span around each attempt and instruments the attempt
/// future with it (`.instrument(span)`), so the fields follow the work across
/// `.await` points and concurrently executing nodes' lines stay unambiguously
/// separable. A retry reuses the same `node` with a higher `attempt`, so
/// first-attempt output is distinguishable from retry output.
///
/// The `node` is the node's **author-declared name** (the readable identity C19
/// records by; a `NodeId` is an opaque hash with no route back to a name), and
/// `run` is the run identity as a string.
#[must_use]
pub fn attempt_span(run: &str, node: &str, attempt: u32) -> tracing::Span {
    // A single, canonical attempt span keyed on the three identity fields. Emitted
    // at INFO so it is retained under the default level cap. This is the *one*
    // attempt span (it attaches to the C14 attempt lifecycle rather than competing
    // with a second span).
    tracing::info_span!("attempt", run = %run, node = %node, attempt = attempt)
}

/// Open the [attempt span](attempt_span) directly from the dep-free
/// [`LogSpan`] identity payload the C8
/// [`RunContext`](dagr_core::context::RunContext) carries — the seam the driver
/// uses so the span's identity comes from the context rather than being
/// re-derived. `node` is supplied separately because a
/// [`NodeId`](dagr_core::handle::NodeId) is opaque with no readable name; the
/// driver passes the node's author-declared name (which C19 also records by).
#[must_use]
pub fn attempt_span_from(span: &LogSpan, node: &str) -> tracing::Span {
    attempt_span(span.run_id().as_str(), node, span.attempt())
}
