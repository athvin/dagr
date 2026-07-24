//! C26 · **Command-line contract** — the standard verb surface, typed-parameter
//! seam, reserved library-flag namespace, and the exhaustive exit-code table
//! every pipeline binary inherits unchanged (arch.md `### C26 · Command-line
//! contract`; ticket T55).
//!
//! # What this module owns
//!
//! Every dagr pipeline binary exposes the **same** command surface so operators
//! learn it once and orchestrators (cron, a Kubernetes Job, a CI step, systemd)
//! get truthful exit codes. This module supplies:
//!
//! - the **library-owned verb set** ([`Verb`] / [`verb_table`]) — `graph`,
//!   `validate`, `render`, `run`, `single-node`, `resume` (stubbed until T58),
//!   `fold`, `prune` — identical across every pipeline built on the library, and
//!   the derived argument parser ([`parse_cli`]) built on `clap`;
//! - the **exit-code table** ([`ExitCode`]) — the crux of C26: every run outcome
//!   / error class maps to a **specific numbered code**, by cause, with
//!   precedence, documented in exactly one place ([`ExitCode::as_u8`]) and stable
//!   within a major version. [`exit_code_for_run`] applies the precedence rules to
//!   a completed [`RunReport`];
//! - the **typed-parameter seam** ([`ParamSpec`] / [`validate_params`]) — the
//!   pipeline declares its typed parameters, the library validates them at
//!   bootstrap (after assembly, which never sees them — C7) and carries them into
//!   the context / run-artifact header;
//! - the **reserved library-flag namespace** ([`reserved_flag_names`] /
//!   [`check_reserved_collision`]) — a pipeline parameter can never shadow a
//!   library-owned flag; a collision is a hard, named error
//!   ([`LibraryFlagCollision`]);
//! - the **verb bodies** that wire already-built machinery: [`validate_verb`]
//!   (assembly only, prints every problem), [`render_verb`] (the C24 renderer
//!   reachable from artifacts alone, with an optional run overlay), [`fold_verb`]
//!   (the standalone C22/T42 fold), [`resume_verb`] (the real C27/T58 resume:
//!   gate + seed/closure/demand plan + resumed-artifact recording, wired to a
//!   pipeline; [`resume_verb_stub`] remains for the pipeline-less reference
//!   driver), and the [`single_node_refusal_check`] durability gate.
//!
//! # What this module does NOT own
//!
//! - The **resume *algorithm*** (the pure seed/closure/demand plan + fingerprint
//!   gating) — [`dagr_core::resume`]. This module wires it ([`resume_verb`]): it
//!   reads the prior artifact, derives parameters/interval, and records the
//!   resumed artifact around the pure plan.
//! - **Scratch carry-forward** for re-executing nodes — T54b. [`resume_verb`]
//!   only surfaces which nodes re-execute (the plan's must-run set) so T54b can
//!   copy their retained scratch forward.
//! - The **durable-output reference contract** and reference *recording* — T57.
//!   This module only *consumes* recorded references for the single-node check.
//! - The **renderer internals** — T46/T47. This module only wires the verb.
//! - **When** a pipeline runs — permanent scope boundary. The CLI never schedules,
//!   never advances a data interval, and never coordinates between concurrent
//!   runs (arch.md Operational model).
//!
//! # Determinism
//!
//! `--help`/usage output is deterministic: `clap` is built with `color`,
//! `wrap_help`, and `suggestions` OFF, so no terminal-width- or TTY-dependent
//! formatting leaks in. Machine-readable verb output (the graph artifact, the
//! folded run artifact, the diagram source) is produced by the already-byte-stable
//! library functions this module wraps — their behaviour is unchanged.

use std::collections::BTreeMap;
use std::io::Write;

use clap::ValueEnum;

use dagr_artifact::event_stream::RunOutcome;
use dagr_artifact::fold::fold_stream;
use dagr_core::flow::Pipeline;
use dagr_core::resume::{plan_resume, PriorNode, PriorRun, ReferenceExistence, ResumePlan};
use dagr_core::TerminalState;
use dagr_render::overlay::{render_dot_overlay, render_mermaid_overlay};
use dagr_render::{render_dot, render_mermaid, GraphArtifact};

use crate::driver::{RunReport, ShutdownExit};

// ===========================================================================
// The exit-code table (the crux of C26)
// ===========================================================================

/// The C26 **exit-code table** — every run outcome / error class mapped to a
/// **specific numbered exit code**, by cause, with precedence (arch.md
/// `### C26`). This is the *one place* the numbering is documented; the numbers
/// are stable within a major version (a change here is a review-visible diff).
///
/// The numbering (see [`as_u8`](ExitCode::as_u8)):
///
/// | code | number | cause |
/// |---|---|---|
/// | [`Success`](ExitCode::Success) | `0` | the run/verb completed cleanly (**includes skip-only runs**) |
/// | [`RunFailure`](ExitCode::RunFailure) | `1` | a non-teardown node ended `failed` or `timed-out` |
/// | [`InvalidUsage`](ExitCode::InvalidUsage) | `2` | bad arguments / invalid parameters / a malformed input artifact |
/// | [`AssemblyFailure`](ExitCode::AssemblyFailure) | `3` | assembly (C7) failed before execution |
/// | [`BootstrapFailure`](ExitCode::BootstrapFailure) | `4` | a fail-fast bootstrap check failed (§63) |
/// | [`Cancelled`](ExitCode::Cancelled) | `5` | externally-originated termination with **no** run failure |
/// | [`ResumeRefusal`](ExitCode::ResumeRefusal) | `6` | resume refused (also a single-node replay refused for a non-durable input) |
/// | [`SinkFailure`](ExitCode::SinkFailure) | `7` | the event sink was unwritable at shutdown (§358) |
///
/// **Precedence** (arch.md C26 "Exit codes are by cause, with precedence"):
/// *run failure wins whenever it occurred* — even when the failure then triggered
/// cancellation (stop-on-first-failure) and even over a sink failure at shutdown.
/// Cancellation is reported only for externally-originated termination with no run
/// failure (`abandoned` attributes to cancellation, never to run failure).
/// [`exit_code_for_run`] encodes this precedence.
///
/// `2` is chosen for invalid usage per long-standing Unix CLI convention; `0` is
/// success per the universal convention every orchestrator relies on. The rest
/// are distinct positive integers, each with exactly one cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// The run or verb completed cleanly — **including a skip-only run** (every
    /// node ended skip-family, none `failed`/`timed-out`). Number `0`.
    Success,
    /// A non-teardown node ended `failed` or `timed-out` — a run failure (the
    /// **highest-precedence** cause). Number `1`.
    RunFailure,
    /// The invocation was malformed: a bad/unknown argument, an invalid typed
    /// parameter, or a malformed input artifact handed to a verb. Number `2`
    /// (Unix usage-error convention).
    InvalidUsage,
    /// Assembly (C7) failed before execution — the graph's fault. Number `3`.
    AssemblyFailure,
    /// A fail-fast bootstrap check failed (a declared cost that cannot fit, a
    /// missing declared resource — §63) — the machine's fault, distinct from an
    /// assembly failure. Number `4`.
    BootstrapFailure,
    /// The run was cancelled by externally-originated termination (a signal / the
    /// `CancelHandle` seam) with **no** run failure. Number `5`.
    Cancelled,
    /// A resume was refused, **or** a single-node replay was refused for a
    /// non-durable input (the two share this code, arch.md C26). The `resume`
    /// stub also returns this until T58 lands the algorithm. Number `6`.
    ResumeRefusal,
    /// The event sink was unwritable at the final flush (a bounded wait, not a
    /// hang — §358) with no run failure. Number `7`.
    SinkFailure,
}

impl ExitCode {
    /// Every exit-code variant, in numbering order — so a table-driven test can
    /// assert exhaustiveness and distinctness over the whole table.
    pub const ALL: [ExitCode; 8] = [
        ExitCode::Success,
        ExitCode::RunFailure,
        ExitCode::InvalidUsage,
        ExitCode::AssemblyFailure,
        ExitCode::BootstrapFailure,
        ExitCode::Cancelled,
        ExitCode::ResumeRefusal,
        ExitCode::SinkFailure,
    ];

    /// The documented C26 process exit number for this cause (arch.md C26: "the
    /// exact numbering is documented in one table and never changes within a
    /// major version"). This is the single authoritative mapping.
    #[must_use]
    pub fn as_u8(self) -> u8 {
        match self {
            ExitCode::Success => 0,
            ExitCode::RunFailure => 1,
            ExitCode::InvalidUsage => 2,
            ExitCode::AssemblyFailure => 3,
            ExitCode::BootstrapFailure => 4,
            ExitCode::Cancelled => 5,
            ExitCode::ResumeRefusal => 6,
            ExitCode::SinkFailure => 7,
        }
    }

    /// The `std::process::ExitCode` this cause exits the process with.
    #[must_use]
    pub fn into_process(self) -> std::process::ExitCode {
        std::process::ExitCode::from(self.as_u8())
    }
}

impl From<ExitCode> for std::process::ExitCode {
    fn from(code: ExitCode) -> Self {
        code.into_process()
    }
}

/// Select the C26 exit code for a **completed** run from the report the driver
/// surfaced (arch.md `### C26`), applying the precedence rules.
///
/// The driver reports the outcome, the cancellation origin, and the
/// [`ShutdownExit`] selection (which of run-failure / sink-failure / cancellation
/// / success applies by C26 precedence); this function is the *numeric* half of
/// the table T55 owns. The precedence:
///
/// 1. **Run failure wins** whenever a non-teardown node ended `failed`/`timed-out`
///    — even when that failure triggered a self-inflicted cancellation
///    (stop-on-first-failure), and even over a sink failure at shutdown. The
///    driver's [`ShutdownExit::RunFailure`] already encodes this, so it maps
///    straight to [`ExitCode::RunFailure`].
/// 2. **Assembly / bootstrap failure** each map to their own distinct code
///    (they short-circuit before any node runs, so they cannot coincide with a
///    node failure).
/// 3. **Sink failure** at shutdown (no run failure) → [`ExitCode::SinkFailure`].
/// 4. **Cancellation** (external termination, no run failure) →
///    [`ExitCode::Cancelled`].
/// 5. Otherwise **success**.
#[must_use]
pub fn exit_code_for_run(report: &RunReport) -> ExitCode {
    // Assembly / bootstrap failures short-circuit before execution and cannot be
    // masked by anything else; map them first from the overall outcome.
    match report.outcome {
        RunOutcome::AssemblyFailed => return ExitCode::AssemblyFailure,
        RunOutcome::BootstrapFailed => return ExitCode::BootstrapFailure,
        _ => {}
    }
    // For an executed run, the driver's ShutdownExit already applied C26
    // precedence (run failure beats sink failure beats cancellation beats
    // success), including the stop-on-first-failure case where a FailureUnderStop
    // cancellation is surfaced as RunFailure. Map it straight through.
    match report.shutdown_exit {
        ShutdownExit::RunFailure => ExitCode::RunFailure,
        ShutdownExit::SinkFailure => ExitCode::SinkFailure,
        ShutdownExit::Cancelled => ExitCode::Cancelled,
        ShutdownExit::Success => ExitCode::Success,
    }
}

// ===========================================================================
// The library-owned verb set
// ===========================================================================

/// The C26 **library-owned verbs** every pipeline binary inherits unchanged
/// (arch.md `### C26`). The set and its order are fixed here, so it is identical
/// across every pipeline built on the library — verb parity is *structural*, not
/// a per-pipeline convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    /// Emit the graph artifact (C20) for this binary; no store required.
    Graph,
    /// Run assembly (C7) only; exit non-zero on any failure, print every problem.
    Validate,
    /// Produce a diagram (C24) from a graph artifact, optionally overlaying a run
    /// artifact; no live pipeline needed.
    Render,
    /// Mint run identity and open the store/stream before assembly, then execute.
    Run,
    /// Replay node N from a prior run R, rehydrating inputs from durable
    /// references (C27/T57).
    SingleNode,
    /// Resume a prior run — **stubbed** until T58; recognized and help-listed,
    /// returns a defined "not yet implemented" outcome.
    Resume,
    /// Fold an event stream into a run artifact (the crashed-run path, C22/T42).
    Fold,
    /// Delete old runs from the run store by count or age; nothing is deleted
    /// implicitly by any other verb.
    Prune,
}

impl Verb {
    /// The verb's stable command-line name (the kebab-case token an operator
    /// types). Fixed and library-owned.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Verb::Graph => "graph",
            Verb::Validate => "validate",
            Verb::Render => "render",
            Verb::Run => "run",
            Verb::SingleNode => "single-node",
            Verb::Resume => "resume",
            Verb::Fold => "fold",
            Verb::Prune => "prune",
        }
    }

    /// A one-line description of the verb, for the help listing.
    #[must_use]
    pub fn summary(self) -> &'static str {
        match self {
            Verb::Graph => "emit this binary's graph artifact (no run store)",
            Verb::Validate => "run assembly only and report every problem",
            Verb::Render => "render a diagram from a graph artifact (optionally overlaying a run)",
            Verb::Run => "mint run identity, open the store, and execute the pipeline",
            Verb::SingleNode => "replay a single node from a prior run",
            Verb::Resume => "resume a prior run (not yet implemented — reserved for T58)",
            Verb::Fold => "fold an event stream into a run artifact (crashed-run path)",
            Verb::Prune => "delete old runs from the run store by count or age",
        }
    }
}

/// The complete C26 verb table, in fixed order — library-owned, so identical
/// across every pipeline built on the library (arch.md C26: "every verb behaves
/// identically across all pipelines built with the library").
#[must_use]
pub fn verb_table() -> &'static [Verb] {
    &[
        Verb::Graph,
        Verb::Validate,
        Verb::Render,
        Verb::Run,
        Verb::SingleNode,
        Verb::Resume,
        Verb::Fold,
        Verb::Prune,
    ]
}

// ===========================================================================
// The derived argument parser (clap)
// ===========================================================================

/// The parsed command-line invocation: the selected [`Verb`].
///
/// The pipeline declares its typed parameters separately ([`ParamSpec`]); this is
/// the *library-owned* surface (the verb and the library flags). The two are
/// combined at bootstrap, after the reserved-namespace check
/// ([`check_reserved_collision`]) guarantees no collision. Per-verb options that a
/// later ticket adds attach to [`build_command`]'s subcommands without changing
/// this public type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    /// The selected verb.
    pub verb: Verb,
}

/// Build the library-owned `dagr` [`clap::Command`] — one subcommand per C26
/// [`Verb`], in the fixed [`verb_table`] order.
///
/// The command is configured for **deterministic** help/usage: the crate builds
/// `clap` with `color`/`wrap_help`/`suggestions` OFF, so output carries no
/// terminal-width- or TTY-dependent formatting (arch.md C26 / Determinism). A
/// later ticket adds a verb's flags by extending its subcommand here — the public
/// [`Verb`]/[`Cli`] surface is unaffected.
#[must_use]
pub fn build_command() -> clap::Command {
    let mut cmd = clap::Command::new("dagr")
        .about("a dagr pipeline binary — the standard C26 command surface")
        .subcommand_required(true)
        .arg_required_else_help(false)
        .disable_help_subcommand(true);
    for verb in verb_table() {
        // Each verb's own flags/arguments are added by a later ticket; T55 owns the
        // verb *set*, not the per-verb option surface. Accept trailing arguments
        // permissively so an invocation like `dagr resume <run-id>` or
        // `dagr single-node --node N` parses to its verb here (the verb body /
        // pipeline binary interprets the arguments), rather than clap rejecting a
        // not-yet-declared argument. A truly unknown *verb* is still rejected
        // (`subcommand_required`), so verb recognition stays strict.
        cmd = cmd.subcommand(
            clap::Command::new(verb.name()).about(verb.summary()).arg(
                clap::Arg::new("args")
                    .num_args(0..)
                    .trailing_var_arg(true)
                    .allow_hyphen_values(true)
                    .value_name("ARG"),
            ),
        );
    }
    cmd
}

/// Map a parsed subcommand name back to its [`Verb`].
fn verb_from_name(name: &str) -> Option<Verb> {
    verb_table().iter().copied().find(|v| v.name() == name)
}

/// The outcome of parsing the command line (arch.md C26).
#[derive(Debug)]
pub enum ParseOutcome {
    /// A verb was selected; carry the parsed [`Cli`].
    Parsed(Cli),
    /// Print help/usage and exit with the carried code. Produced for a bare
    /// invocation with **no arguments** (the C26 "print the available verbs and
    /// exit cleanly" case — [`ExitCode::Success`]) and for an explicit
    /// `--help`/`-h`.
    Help {
        /// The exit code to leave with after printing.
        exit: ExitCode,
        /// The help/usage text to print (the verb listing).
        text: String,
    },
    /// The invocation was malformed. Carry the [`ExitCode::InvalidUsage`] code and
    /// the diagnostic to print.
    Error {
        /// Always [`ExitCode::InvalidUsage`].
        exit: ExitCode,
        /// The diagnostic message.
        message: String,
    },
}

/// Parse a command line (argv, program name first) into a [`ParseOutcome`]
/// (arch.md C26).
///
/// - No arguments → [`ParseOutcome::Help`] listing the available verbs, exiting
///   [`ExitCode::Success`] (arch.md C26: "print the available verbs and exit
///   cleanly").
/// - `--help`/`-h` → the same help listing, exiting success.
/// - A recognized verb → [`ParseOutcome::Parsed`].
/// - An unknown verb / malformed arguments → [`ParseOutcome::Error`] with
///   [`ExitCode::InvalidUsage`].
pub fn parse_cli<I, T>(argv: I) -> ParseOutcome
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let raw_args: Vec<std::ffi::OsString> = argv.into_iter().map(Into::into).collect();
    // A bare invocation (program name only) prints the verb listing and exits
    // cleanly — the C26 no-argument contract.
    if raw_args.len() <= 1 {
        return ParseOutcome::Help {
            exit: ExitCode::Success,
            text: help_text(),
        };
    }
    match build_command().try_get_matches_from(&raw_args) {
        Ok(matches) => match matches.subcommand() {
            Some((name, _sub)) => match verb_from_name(name) {
                Some(verb) => ParseOutcome::Parsed(Cli { verb }),
                // clap already gates the subcommand set, so this is unreachable in
                // practice; surface it as invalid usage rather than panicking.
                None => ParseOutcome::Error {
                    exit: ExitCode::InvalidUsage,
                    message: format!("unknown verb `{name}`"),
                },
            },
            None => ParseOutcome::Help {
                exit: ExitCode::Success,
                text: help_text(),
            },
        },
        Err(err) => match err.kind() {
            // clap prints the help/version itself; surface it as a clean-exit help.
            clap::error::ErrorKind::DisplayHelp
            | clap::error::ErrorKind::DisplayVersion
            | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => {
                ParseOutcome::Help {
                    exit: ExitCode::Success,
                    text: help_text(),
                }
            }
            _ => ParseOutcome::Error {
                exit: ExitCode::InvalidUsage,
                message: err.to_string(),
            },
        },
    }
}

/// The deterministic verb-listing help text (arch.md C26 no-arg contract). Lists
/// every library verb with its one-line summary, in the fixed [`verb_table`]
/// order. Deterministic: no colour, no terminal-width wrapping.
#[must_use]
pub fn help_text() -> String {
    use std::fmt::Write as _;
    let mut out = String::from("dagr — a pipeline binary. Available verbs:\n\n");
    for verb in verb_table() {
        // Infallible: writing into a String never errors.
        let _ = writeln!(out, "  {:<12} {}", verb.name(), verb.summary());
    }
    out.push_str("\nRun `dagr <verb> --help` for a verb's options.\n");
    out
}

// ===========================================================================
// Reserved library-flag namespace
// ===========================================================================

/// The reserved **library-flag namespace** (arch.md C26): the long-flag names the
/// library owns, which a pipeline parameter can never shadow or collide with. A
/// collision is a hard, named error ([`LibraryFlagCollision`]).
///
/// These are the library-owned run/inspection flags (the store base, the run-id
/// override, the grace period, the failure mode, the pool pinning, the data
/// interval, and the always-reserved `help`/`version`). Fixed and library-owned,
/// so the namespace is identical across every pipeline.
#[must_use]
pub fn reserved_flag_names() -> &'static [&'static str] {
    &[
        "help",
        "version",
        "run-id",
        "store",
        "grace",
        "teardown-deadline",
        "failure-mode",
        "pool",
        "data-interval",
        "force",
        "run",
    ]
}

/// A pipeline parameter's flag name collided with a reserved library flag
/// (arch.md C26). Names the offending flag so the diagnostic is actionable; the
/// run does not proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibraryFlagCollision {
    /// The offending flag name (the reserved library flag a pipeline parameter
    /// tried to reuse).
    pub flag: &'static str,
}

impl std::fmt::Display for LibraryFlagCollision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pipeline parameter `--{}` collides with the reserved library flag `--{}`; \
             library-owned flags live in a reserved namespace and cannot be shadowed \
             (arch.md C26) — rename the pipeline parameter",
            self.flag, self.flag
        )
    }
}

impl std::error::Error for LibraryFlagCollision {}

/// Reject any pipeline parameter whose flag name lands in the reserved
/// library-flag namespace (arch.md C26). Returns the first collision as a named,
/// hard error; the caller must not proceed with the run.
///
/// # Errors
///
/// Returns [`LibraryFlagCollision`] naming the offending flag if any declared
/// parameter's name is a [reserved library flag](reserved_flag_names).
pub fn check_reserved_collision(params: &[ParamSpec]) -> Result<(), LibraryFlagCollision> {
    for param in params {
        if let Some(reserved) = reserved_flag_names().iter().find(|r| **r == param.name) {
            return Err(LibraryFlagCollision { flag: reserved });
        }
    }
    Ok(())
}

// ===========================================================================
// Typed parameters
// ===========================================================================

/// The scalar type a pipeline parameter is declared with — the library uses it to
/// validate the supplied value at bootstrap (arch.md C26).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    /// A free-form string value (accepted verbatim).
    Str,
    /// A 64-bit signed integer; a non-integer value is invalid usage.
    Int,
    /// A boolean (`true`/`false`); anything else is invalid usage.
    Bool,
}

/// One declared pipeline parameter (arch.md C26): its flag name, its declared
/// [type](ParamType), and a help description. The pipeline declares a set of
/// these once; the library derives the parsing and validates values against the
/// declared type at bootstrap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamSpec {
    /// The parameter's long-flag name (without the leading `--`).
    pub name: String,
    /// The declared scalar type the value is validated against.
    pub ty: ParamType,
    /// The help description.
    pub description: String,
}

impl ParamSpec {
    /// A string parameter named `name`.
    #[must_use]
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ty: ParamType::Str,
            description: description.into(),
        }
    }

    /// An integer parameter named `name`.
    #[must_use]
    pub fn int(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ty: ParamType::Int,
            description: description.into(),
        }
    }

    /// A boolean parameter named `name`.
    #[must_use]
    pub fn boolean(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ty: ParamType::Bool,
            description: description.into(),
        }
    }
}

/// Validate the `supplied` parameter values against their declared
/// [specs](ParamSpec) at bootstrap (arch.md C26 / C7: parameters are validated at
/// bootstrap, *after* assembly, which never sees them).
///
/// On success, returns the validated values as a name→value map (verbatim string
/// values — an integer/boolean is validated but carried as its verbatim string),
/// which the run verb records into the run-artifact header (C22) and carries in
/// the context. On any invalid value it returns [`ExitCode::InvalidUsage`] — the
/// run must not proceed, so no node executes (rejected at bootstrap, before
/// execution).
///
/// # Errors
///
/// Returns [`ExitCode::InvalidUsage`] if any supplied value fails its declared
/// type's validation (a non-integer for an `int`, a non-boolean for a `bool`).
pub fn validate_params(
    specs: &[ParamSpec],
    supplied: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ExitCode> {
    let mut carried = BTreeMap::new();
    for spec in specs {
        let Some(value) = supplied.get(&spec.name) else {
            continue;
        };
        let ok = match spec.ty {
            ParamType::Str => true,
            ParamType::Int => value.parse::<i64>().is_ok(),
            ParamType::Bool => matches!(value.as_str(), "true" | "false"),
        };
        if !ok {
            return Err(ExitCode::InvalidUsage);
        }
        // Carried verbatim — the header records exactly what the operator supplied.
        carried.insert(spec.name.clone(), value.clone());
    }
    Ok(carried)
}

// ===========================================================================
// Verb bodies
// ===========================================================================

/// The `validate` verb (arch.md C26): run assembly (C7) only and report the
/// result. Exits [`ExitCode::Success`] with no problems on a clean assembly, or
/// [`ExitCode::AssemblyFailure`] printing **every** problem assembly found (not
/// just the first, C7).
///
/// Assembly is pure (C7) — no store, no parameters, no network — so this verb
/// runs it with no store at all (arch.md "the inspection verbs run assembly with
/// no store").
pub fn validate_verb<W: Write>(pipeline: &Pipeline, out: &mut W) -> ExitCode {
    match pipeline.assemble() {
        Ok(_) => {
            let _ = writeln!(out, "assembly succeeded: the pipeline is valid");
            ExitCode::Success
        }
        Err(error) => {
            let problems = error.problems();
            let _ = writeln!(out, "assembly failed with {} problem(s):", problems.len());
            // Print EVERY problem, not just the first (arch.md C7/C26).
            for problem in problems {
                let _ = writeln!(out, "  - {}", problem.message());
            }
            ExitCode::AssemblyFailure
        }
    }
}

/// The output format the `render` verb emits (arch.md C24).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum RenderFormat {
    /// Graphviz DOT (the default).
    #[default]
    Dot,
    /// Mermaid flowchart.
    Mermaid,
}

/// The `render` verb (arch.md C26 / C24): produce diagram source from a graph
/// artifact, **optionally overlaying** a run artifact — reachable from artifacts
/// alone, needing no live pipeline.
///
/// `graph_bytes` is a published C20 graph artifact; `run_bytes`, if present, is a
/// C22 run artifact whose per-node terminal states colour the diagram (the T47
/// overlay). A malformed graph artifact is refused with [`ExitCode::InvalidUsage`]
/// and a diagnostic to `out` — never a partial diagram (C24). This verb wires the
/// already-built T46/T47 renderer; it re-implements nothing.
pub fn render_verb<W: Write>(
    graph_bytes: &[u8],
    run_bytes: Option<&[u8]>,
    format: RenderFormat,
    out: &mut W,
) -> ExitCode {
    let graph_str = match std::str::from_utf8(graph_bytes) {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(out, "graph artifact is not valid UTF-8: {e}");
            return ExitCode::InvalidUsage;
        }
    };
    let graph = match GraphArtifact::from_json_str(graph_str) {
        Ok(g) => g,
        Err(e) => {
            let _ = writeln!(out, "cannot render: {e}");
            return ExitCode::InvalidUsage;
        }
    };

    // The optional run overlay: parse the run artifact and render with the overlay
    // if it was supplied. The overlay is a pure function of (graph, run) → text
    // (T47), so this stays artifact-only.
    let run_artifact = match run_bytes {
        Some(bytes) => {
            let run_str = match std::str::from_utf8(bytes) {
                Ok(s) => s,
                Err(e) => {
                    let _ = writeln!(out, "run artifact is not valid UTF-8: {e}");
                    return ExitCode::InvalidUsage;
                }
            };
            match dagr_render::overlay::RunArtifact::from_json_str(run_str) {
                Ok(r) => Some(r),
                Err(e) => {
                    let _ = writeln!(out, "cannot render run overlay: {e}");
                    return ExitCode::InvalidUsage;
                }
            }
        }
        None => None,
    };

    let diagram = match (&run_artifact, format) {
        (Some(run), RenderFormat::Dot) => render_dot_overlay(&graph, run),
        (Some(run), RenderFormat::Mermaid) => render_mermaid_overlay(&graph, run),
        (None, RenderFormat::Dot) => render_dot(&graph),
        (None, RenderFormat::Mermaid) => render_mermaid(&graph),
    };
    let _ = write!(out, "{diagram}");
    ExitCode::Success
}

/// The `fold` verb (arch.md C26 / C22): wire the standalone C22/T42 stream-fold
/// function to produce the (possibly interrupted) run artifact from a run's event
/// stream — the crashed-run path (system criterion 3's crash clause).
///
/// `stream_bytes` is the C19 event stream; `graph_nodes` is the node roster
/// (for coverage). Writes the canonical run-artifact JSON to `out` and exits
/// [`ExitCode::Success`]. A stream that cannot be folded (no `run-started`, or a
/// non-tolerated corruption) is [`ExitCode::InvalidUsage`] with a diagnostic. This
/// verb wires the already-built fold; it re-implements nothing.
pub fn fold_verb<W: Write>(stream_bytes: &[u8], graph_nodes: &[String], out: &mut W) -> ExitCode {
    match fold_stream(stream_bytes, graph_nodes) {
        Ok(artifact) => {
            let _ = writeln!(out, "{}", artifact.to_canonical_json());
            ExitCode::Success
        }
        Err(e) => {
            let _ = writeln!(out, "cannot fold event stream: {e}");
            ExitCode::InvalidUsage
        }
    }
}

/// The `resume` verb **stub** (arch.md C26). It is a recognized, help-listed verb
/// that reports "not yet implemented" for a binary that has **not wired** the real
/// resume behaviour, and exits with the [resume-refusal code](ExitCode::ResumeRefusal).
///
/// T58 lands the real resume as [`resume_verb`] (the C27 gate + seed/closure/demand
/// plan + resumed-artifact recording), which a pipeline binary calls with its own
/// assembled pipeline. A binary that does not opt to wire it — the reference `dagr`
/// driver (no pipeline) and any pipeline that has not adopted resume — keeps this
/// stub, so the verb stays recognized and the C26 refusal code is reserved without
/// changing the surface. The phrase "not yet implemented" marks the unwired seam.
pub fn resume_verb_stub<W: Write>(out: &mut W) -> ExitCode {
    let _ = writeln!(
        out,
        "resume is not yet implemented for this binary: the C27 resume algorithm \
         (`dagr_cli::contract::resume_verb`) lands in T58, but this binary has not wired \
         it to a pipeline. The verb is recognized and its refusal code is reserved. Refusing."
    );
    ExitCode::ResumeRefusal
}

// ===========================================================================
// The real resume verb (C27 / T58) — wired to a pipeline
// ===========================================================================

/// The tool-version string this binary records into (and gates resume against),
/// per the C27 no-cross-tool-version promise. v1: a single stable token.
pub const TOOL_VERSION: &str = "dagr@1";

/// The operator-supplied inputs to a [`resume_verb`] invocation (arch.md C27).
///
/// The library-owned flags (`--run-id`, `--store`, `--force`, `--data-interval`)
/// and the typed parameters are parsed by the C26 surface (T55) and handed here as
/// this struct; `resume_verb` derives the *rest* — the prior parameters and
/// interval — from the prior artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeOptions {
    /// The identity to mint for the resumed run (the new run id).
    pub new_run_id: String,
    /// This binary's tool version (gated against the prior run's; use
    /// [`TOOL_VERSION`]).
    pub tool_version: String,
    /// Whether the prior run's **run store** is still present. A run whose store
    /// is gone is not resumable and is refused up front (arch.md C27).
    pub store_present: bool,
    /// Whether `--force` was given: override a parameter/interval conflict with the
    /// prior run (recorded in the resumed artifact).
    pub force: bool,
    /// Operator-supplied parameter overrides (name → value). A value that
    /// conflicts with the prior run refuses unless [`force`](Self::force) is set.
    pub param_overrides: BTreeMap<String, String>,
    /// An operator-supplied data-interval override (`[start, end]`), or `None` to
    /// derive it from the prior artifact.
    pub interval_override: Option<[String; 2]>,
}

/// The outcome of a [`resume_verb`] invocation (arch.md C27).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// Resume proceeded: the resumed run's own artifact (a `serde_json::Value`
    /// conforming to the published run schema, T39 — satisfied-from-prior nodes
    /// recorded with their originating run identity, durable references copied
    /// forward, lineage linked) and the computed [plan](ResumePlan) the caller
    /// executes (the must-run set is the hand-off T54b consumes).
    Resumed {
        /// The resumed run artifact (schema-valid, self-contained).
        artifact: serde_json::Value,
        /// The computed resume plan (which nodes re-execute, what is rehydrated,
        /// what is satisfied-from-prior).
        plan: ResumePlan,
    },
    /// Resume was refused. The [`code`](ResumeOutcome::Refused::code) is always
    /// [`ExitCode::ResumeRefusal`] (aligned with the C26 exit-code table — the
    /// resume-refusal code shared with a single-node replay refusal), and the
    /// `message` is the printable diagnostic (a gate diff, a store-gone message, a
    /// parameter-conflict diff, or a dangling-reference plan failure).
    Refused {
        /// Always [`ExitCode::ResumeRefusal`].
        code: ExitCode,
        /// The diagnostic to print.
        message: String,
    },
}

impl ResumeOutcome {
    fn refused(message: impl Into<String>) -> Self {
        ResumeOutcome::Refused {
            code: ExitCode::ResumeRefusal,
            message: message.into(),
        }
    }
}

/// The `resume` verb (arch.md `### C27 · Resume`; ticket T58) — the real resume
/// behaviour T58 wires behind the T55 stub.
///
/// Given `pipeline` (this binary's assembled graph), the prior run's folded
/// artifact bytes (`prior_run_bytes`), the operator [`options`](ResumeOptions),
/// and a durable-reference existence `probe`, it:
///
/// 1. **Refuses a gone run store** up front (a run whose store is gone is not
///    resumable).
/// 2. **Reads** the prior run's fingerprints, tool version, run identity, prior
///    parameters/interval, prior lineage, and per-node terminal states + durable
///    references from the artifact.
/// 3. **Derives** the resumed run's parameters and interval from the prior
///    artifact — a supplied value that conflicts refuses with a diff; `--force`
///    overrides and is recorded.
/// 4. Runs the pure [`plan_resume`] gate + seed/closure/demand algorithm (a
///    structural / algorithm-version / tool-version mismatch, or a dangling
///    demanded durable reference, refuses).
/// 5. **Produces** the resumed run artifact: satisfied-from-prior nodes recorded
///    distinctly (status `satisfied-from-prior`) with their originating run
///    identity, durable references copied forward so the artifact is
///    self-contained, and the header linked to both the immediate parent run and
///    the lineage-root run.
///
/// Every refusal maps to [`ExitCode::ResumeRefusal`] (the C26 resume-refusal code,
/// shared with a single-node replay refusal). The `must_run` set the returned
/// [`ResumePlan`] carries is the hand-off T54b consumes to copy retained scratch
/// forward; **this verb does not re-execute nodes** (that is the driver's).
///
/// # Determinism
///
/// The algorithm and the produced artifact are pure functions of the inputs — no
/// clock, no ambient state. A **non-resume** run is unaffected: this path is
/// reached only by the `resume` verb.
pub fn resume_verb<P>(
    pipeline: &Pipeline,
    prior_run_bytes: &[u8],
    options: &ResumeOptions,
    probe: P,
) -> ResumeOutcome
where
    P: Fn(&str, &str) -> ReferenceExistence,
{
    // (1) A run whose store is gone is not resumable — refuse before any planning.
    if !options.store_present {
        return ResumeOutcome::refused(
            "resume refused: the prior run's run store is gone — a run whose store no longer \
             exists is not resumable (arch.md C27). Resume requires the original run to have \
             used a durable run store.",
        );
    }

    // (2) Read the prior artifact.
    let prior_json: serde_json::Value = match serde_json::from_slice(prior_run_bytes) {
        Ok(v) => v,
        Err(e) => {
            return ResumeOutcome::refused(format!(
                "resume refused: cannot read the prior run artifact: {e}"
            ));
        }
    };
    let header = prior_json.get("header").unwrap_or(&serde_json::Value::Null);
    let prior_run_id = header
        .get("run_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    if prior_run_id.is_empty() {
        return ResumeOutcome::refused(
            "resume refused: the prior run artifact carries no run identity.",
        );
    }

    // (3) Derive parameters + interval, applying conflict / force rules.
    let prior_params = read_prior_parameters(header);
    let (parameters, forced_params) =
        match derive_parameters(&prior_params, &options.param_overrides, options.force) {
            Ok(v) => v,
            Err(message) => return ResumeOutcome::refused(message),
        };
    let prior_interval = header
        .get("data_interval")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let (data_interval, forced_interval) = match derive_interval(
        &prior_interval,
        options.interval_override.as_ref(),
        options.force,
    ) {
        Ok(v) => v,
        Err(message) => return ResumeOutcome::refused(message),
    };
    let forced = forced_params || forced_interval;

    // (4) Assemble the serde-free prior facts and run the pure plan.
    let prior = build_prior_run(header, &prior_json, &prior_run_id);
    let plan = match plan_resume(pipeline, &prior, &options.tool_version, probe) {
        Ok(plan) => plan,
        // Every gate / dangling-reference refusal maps to the C26 resume-refusal
        // code and prints the carried diff (the `ResumeRefusal` Display).
        Err(refusal) => return ResumeOutcome::refused(refusal.to_string()),
    };

    // (5) Produce the resumed run artifact (satisfied-from-prior recording,
    //     copy-forward, lineage).
    let artifact = build_resumed_artifact(
        &options.new_run_id,
        header,
        parameters,
        data_interval,
        forced,
        &prior_run_id,
        &prior,
        &plan,
    );
    ResumeOutcome::Resumed { artifact, plan }
}

/// The prior run's parameters as a name→value string map (verbatim from the
/// header).
fn read_prior_parameters(header: &serde_json::Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = header
        .get("parameters")
        .and_then(serde_json::Value::as_object)
    {
        for (k, v) in obj {
            let value = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            out.insert(k.clone(), value);
        }
    }
    out
}

/// Derive the resumed run's parameters from the prior run's, applying any
/// operator overrides. A conflicting override without `--force` refuses with a
/// diff; with `--force` the override wins and the fact that force was used is
/// returned so it can be recorded.
fn derive_parameters(
    prior: &BTreeMap<String, String>,
    overrides: &BTreeMap<String, String>,
    force: bool,
) -> Result<(BTreeMap<String, String>, bool), String> {
    let mut derived = prior.clone();
    let mut forced = false;
    for (name, supplied) in overrides {
        match prior.get(name) {
            Some(prior_value) if prior_value != supplied && !force => {
                return Err(format!(
                    "resume refused: parameter `{name}` conflicts with the prior run \
                     (prior=`{prior_value}`, supplied=`{supplied}`). Resume derives parameters \
                     from the prior artifact; pass --force to override the conflict.",
                ));
            }
            Some(prior_value) if prior_value != supplied => {
                // --force: the override wins and its use is recorded.
                forced = true;
                derived.insert(name.clone(), supplied.clone());
            }
            _ => {
                // No prior value, or an identical value: accept without a conflict.
                derived.insert(name.clone(), supplied.clone());
            }
        }
    }
    Ok((derived, forced))
}

/// Derive the resumed run's data interval from the prior run's, applying an
/// optional operator override with the same conflict/force discipline.
fn derive_interval(
    prior: &serde_json::Value,
    override_interval: Option<&[String; 2]>,
    force: bool,
) -> Result<(serde_json::Value, bool), String> {
    let Some([start, end]) = override_interval else {
        return Ok((prior.clone(), false));
    };
    let supplied = serde_json::json!({ "start": start, "end": end });
    if !prior.is_null() && *prior != supplied {
        if !force {
            return Err(format!(
                "resume refused: the supplied data interval [{start}, {end}] conflicts with the \
                 prior run's interval ({prior}). Pass --force to override the conflict.",
            ));
        }
        return Ok((supplied, true));
    }
    Ok((supplied, false))
}

/// Parse a recorded fingerprint string (`"<algo>:<hex>"`, e.g. `"fnv:00ab…"`) to
/// the `u64` digest the resume gate compares. A missing or malformed value yields
/// `None`, which the gate treats as an unmatched (mismatching) fingerprint.
fn parse_fingerprint(header: &serde_json::Value, field: &str) -> Option<u64> {
    let raw = header.get(field).and_then(serde_json::Value::as_str)?;
    let hex = raw.rsplit(':').next().unwrap_or(raw);
    u64::from_str_radix(hex, 16).ok()
}

/// Assemble the serde-free [`PriorRun`] the pure plan reads: the fingerprints, the
/// algorithm/tool versions, and each node's prior terminal state + durable
/// reference + originating run identity.
fn build_prior_run(
    header: &serde_json::Value,
    artifact: &serde_json::Value,
    prior_run_id: &str,
) -> PriorRun {
    let structural = parse_fingerprint(header, "fingerprint_structural").unwrap_or(u64::MAX);
    let policy = parse_fingerprint(header, "fingerprint_policy").unwrap_or(u64::MAX);
    let algorithm_version = header
        .get("fingerprint_algorithm_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let tool_version = header
        .get("tool_version")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(TOOL_VERSION)
        .to_string();

    // Per-node facts from the attempts (last attempt per node wins for its
    // terminal state; the durable reference and origin come from that record).
    let mut nodes: BTreeMap<String, PriorNode> = BTreeMap::new();
    let attempts = artifact
        .get("attempts")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for a in &attempts {
        let Some(node) = a.get("node").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let status = a
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("failed");
        let terminal = terminal_from_str(status);
        let durable_reference = a
            .get("durable_reference")
            .filter(|v| !v.is_null())
            .map(reference_to_string);
        // A node's originating run: the run it was satisfied-from in the prior run
        // (carried across generations), else the prior run itself.
        let originating_run = a
            .get("satisfied_from_run")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(prior_run_id)
            .to_string();
        nodes.insert(
            node.to_string(),
            PriorNode {
                terminal,
                durable_reference,
                originating_run,
            },
        );
    }

    PriorRun {
        structural_fingerprint: structural,
        policy_hash: policy,
        algorithm_version,
        tool_version,
        nodes,
    }
}

/// Render a recorded durable-reference value to the reference string the resume
/// core existence-probes and copies forward (opaque to dagr; a string stays a
/// string, a structured reference is serialized).
fn reference_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Map a recorded status token to the normative [`TerminalState`]. An unknown
/// token is treated as a failure-like non-success (so the node re-runs).
fn terminal_from_str(status: &str) -> TerminalState {
    match status {
        "succeeded" => TerminalState::Succeeded,
        "timed-out" => TerminalState::TimedOut,
        "skipped" => TerminalState::Skipped,
        "upstream-skipped" => TerminalState::UpstreamSkipped,
        "upstream-failed" => TerminalState::UpstreamFailed,
        "cancelled" => TerminalState::Cancelled,
        "abandoned" => TerminalState::Abandoned,
        "satisfied-from-prior" => TerminalState::SatisfiedFromPrior,
        _ => TerminalState::Failed,
    }
}

/// Build the resumed run's own artifact (arch.md C22/C27): a schema-valid
/// `serde_json::Value` that records satisfied-from-prior nodes with their
/// originating run identity, copies durable references forward so it is
/// self-contained, and links the header to both the immediate parent run and the
/// lineage-root run.
#[allow(clippy::too_many_arguments)]
fn build_resumed_artifact(
    new_run_id: &str,
    prior_header: &serde_json::Value,
    parameters: BTreeMap<String, String>,
    data_interval: serde_json::Value,
    forced: bool,
    parent_run_id: &str,
    prior: &PriorRun,
    plan: &ResumePlan,
) -> serde_json::Value {
    // Lineage: the immediate parent is the prior run; the lineage root is the
    // prior run's own root when it was itself a resume, else the prior run.
    let lineage_root = prior_header
        .get("resume_lineage")
        .and_then(|l| l.get("lineage_root_run_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(parent_run_id)
        .to_string();

    let pipeline = prior_header
        .get("pipeline")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();

    let params_obj: serde_json::Map<String, serde_json::Value> = parameters
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();

    let mut header = serde_json::Map::new();
    header.insert("run_id".into(), serde_json::json!(new_run_id));
    header.insert("pipeline".into(), serde_json::json!(pipeline));
    // The resumed run's fingerprints match this binary's (it passed the gate); the
    // prior header's recorded fingerprints stand for them (structural is byte-equal
    // by the gate, policy is this binary's — carried from the prior header, which
    // is what a fresh non-resume run of the same binary would record).
    for f in [
        "fingerprint_structural",
        "fingerprint_policy",
        "fingerprint_algorithm_version",
    ] {
        if let Some(v) = prior_header.get(f) {
            header.insert(f.into(), v.clone());
        }
    }
    header.insert("tool_version".into(), serde_json::json!(TOOL_VERSION));
    header.insert("parameters".into(), serde_json::Value::Object(params_obj));
    header.insert("data_interval".into(), data_interval);
    header.insert(
        "captured_environment".into(),
        prior_header
            .get("captured_environment")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    );
    header.insert(
        "resume_lineage".into(),
        serde_json::json!({
            "parent_run_id": parent_run_id,
            "lineage_root_run_id": lineage_root,
        }),
    );
    if forced {
        // Additive marker: the resumed artifact records that --force was used
        // (open-world schema, T0.10 — validates against the unmodified schema).
        header.insert("resume_forced".into(), serde_json::json!(true));
    }
    header.insert("overall_outcome".into(), serde_json::json!("succeeded"));

    // Attempts: one record per satisfied-from-prior node (recorded distinctly with
    // its originating run identity + copied-forward durable reference). Nodes in
    // the must-run set are re-executed by the driver (T54b hand-off) and record
    // their own attempts there — not here.
    let mut attempts: Vec<serde_json::Value> = Vec::new();
    for (node, origin) in plan.satisfied_from_prior() {
        let mut record = serde_json::Map::new();
        record.insert("node".into(), serde_json::json!(node));
        record.insert("attempt".into(), serde_json::json!(1));
        record.insert("status".into(), serde_json::json!("satisfied-from-prior"));
        record.insert(
            "phase_durations_ns".into(),
            serde_json::json!({ "executing": 0 }),
        );
        record.insert("worker".into(), serde_json::json!("resume"));
        // The originating run identity a satisfied-from-prior record MUST carry.
        record.insert("satisfied_from_run".into(), serde_json::json!(origin));
        // Copy the durable reference forward so the resumed artifact stands alone.
        if let Some(prior_node) = prior.nodes.get(node) {
            if let Some(reference) = &prior_node.durable_reference {
                record.insert("durable_reference".into(), serde_json::json!(reference));
            }
        }
        attempts.push(serde_json::Value::Object(record));
    }
    // Deterministic ordering (satisfied_from_prior is a BTreeMap, already sorted).

    serde_json::json!({
        "header": serde_json::Value::Object(header),
        "attempts": attempts,
        "summary": serde_json::Value::Null,
    })
}

/// The single-node **durability gate** (arch.md C26): given the prior run
/// artifact and node `node`'s required input-producer node names, refuse the
/// replay if any required input is not durable — i.e. its producer's attempt
/// recorded **no** durable reference in R's artifact (C27/T57).
///
/// Returns `Some(`[`ExitCode::ResumeRefusal`]`)` (the code shared with resume
/// refusal) and writes a message naming the offending input and why to `out` when
/// a required input is not durable; returns `None` when every required input has a
/// recorded durable reference (the replay may proceed). This is the *consume*
/// side of the durable-output contract T57 records — this verb interprets no
/// reference bytes, it only checks presence.
///
/// A consume-nothing node (`inputs` empty) never refuses here — it can run
/// standalone with no prior run.
pub fn single_node_refusal_check<W: Write>(
    prior_run_bytes: &[u8],
    node: &str,
    inputs: &[String],
    out: &mut W,
) -> Option<ExitCode> {
    if inputs.is_empty() {
        // A consume-nothing node runs standalone; nothing to rehydrate.
        return None;
    }
    let prior: serde_json::Value = match serde_json::from_slice(prior_run_bytes) {
        Ok(v) => v,
        Err(e) => {
            let _ = writeln!(out, "cannot read prior run artifact: {e}");
            return Some(ExitCode::InvalidUsage);
        }
    };
    // Collect each producer's recorded durable reference (if any) from the prior
    // run's attempt records. A producer whose latest attempt recorded no
    // `durable_reference` produced an in-memory value that cannot be rehydrated.
    let attempts = prior.get("attempts").and_then(serde_json::Value::as_array);
    for input in inputs {
        let durable = attempts
            .into_iter()
            .flatten()
            .filter(|a| a.get("node").and_then(serde_json::Value::as_str) == Some(input.as_str()))
            .any(|a| a.get("durable_reference").is_some_and(|r| !r.is_null()));
        if !durable {
            let _ = writeln!(
                out,
                "cannot replay node `{node}`: its input `{input}` is not durable — \
                 the prior run recorded no durable reference for it, so its value cannot be \
                 rehydrated (arch.md C26/C27). Refusing.",
            );
            return Some(ExitCode::ResumeRefusal);
        }
    }
    None
}
