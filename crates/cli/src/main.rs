//! The `dagr` pipeline-binary entry point — the C26 command-line contract's
//! reference driver (arch.md `### C26 · Command-line contract`; tickets T55, T74).
//!
//! Every dagr pipeline binary inherits the same command surface from the library
//! ([`dagr_cli::contract`]): the standard verbs, the typed-parameter seam, the
//! reserved library-flag namespace, and the exhaustive exit-code table. This
//! binary is the library's own reference driver. It **delegates flow selection to
//! [`dagr_cli::registry::run_registry`]** over a single-flow registry (ADR 086),
//! so every *flow-selecting* verb — `run`, `graph`, `validate`, `single-node`,
//! `prune`, plus the registry's `list` — routes through the many-flows-per-binary
//! seam rather than being hand-dispatched (T74 shipped `run`/`list`; T75 added the
//! `graph`/`validate` routing and the `single-node`/`prune` selection). The
//! reference driver therefore emits its own flow's C20 graph (`dagr graph`) and
//! validates it (`dagr validate`) instead of reporting "needs a pipeline-specific
//! binary" for those verbs.
//!
//! The **artifact-only** verbs (`render`, `fold`) and the `resume` stub carry no
//! flow, so they are dispatched **directly** here (reading their artifact from
//! standard input), mapping each outcome to its C26 exit code.

use std::io::{self, Read};
use std::process::ExitCode as ProcExit;

use dagr_cli::contract::{
    banner_suppressed_by_env, fold_verb, parse_cli, print_banner, render_verb, resume_verb_stub,
    split_banner_flag, ExitCode, ParseOutcome, RenderFormat, Verb,
};
use dagr_cli::registry::{run_registry, FlowRegistry};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// The reference driver's trivial single-node flow — a consume-nothing source that
/// succeeds. It carries no real pipeline (that is a pipeline crate's job), but a
/// registry needs a factory, so this is the minimal flow that lets every
/// flow-selecting verb route through [`run_registry`] end to end on the reference
/// binary. It carries an author-declared [`StableName`] so it registers through the
/// stable-name-aware surface and is emittable to the C20 graph contract — i.e.
/// `dagr graph` on the reference binary emits a real (if trivial) graph artifact.
struct ReferenceSource;
impl StableName for ReferenceSource {
    const STABLE_NAME: &'static str = "ReferenceSource";
}
impl Task for ReferenceSource {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// Build the reference driver's single flow (a fresh one per invocation — the
/// factory is re-invoked by [`run_registry`], once per verb). Registers through the
/// stable-name-aware surface so `dagr graph` / `dagr validate` route to it.
fn build_reference_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _h = flow.register_source_named("reference", ReferenceSource);
    flow
}

fn main() -> ProcExit {
    // Print the startup banner to stderr before anything else, unless suppressed by
    // `--no-banner`, `DAGR_NO_BANNER`, or `NO_COLOR`. The flag is stripped from argv
    // here so it never reaches the verb parser; stdout stays reserved for
    // machine-readable verb output, so the banner (stderr) never contaminates it.
    let raw_args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let (no_banner, argv) = split_banner_flag(raw_args.iter().cloned());
    if !no_banner && !banner_suppressed_by_env() {
        let _ = print_banner(&mut io::stderr().lock());
    }

    // Every flow-selecting verb routes through the registry (ADR 086): the reference
    // driver hosts one trivial flow, so `dagr run` drives it, `dagr graph` emits its
    // C20 artifact, `dagr validate` assembles it, and `dagr list` prints its name.
    // Recognizing these before the direct dispatch keeps the artifact-only verbs
    // (render/fold/resume) handled here and delegates flow selection to the shared
    // entrypoint. `list` is registry-specific; the rest are C26 verbs.
    if routes_through_registry(&argv) {
        let registry = FlowRegistry::single_flow(build_reference_flow);
        // Pass the banner-stripped argv so the registry never re-sees `--no-banner`.
        return run_registry(&registry, argv).into();
    }

    let outcome = parse_cli(argv);
    let code = match outcome {
        ParseOutcome::Help { exit, text } => {
            print!("{text}");
            exit
        }
        ParseOutcome::Error { exit, message } => {
            eprintln!("dagr: {message}");
            exit
        }
        ParseOutcome::Parsed(cli) => dispatch(cli.verb),
    };
    code.into()
}

/// Whether this invocation is one the registry routes — every *flow-selecting*
/// verb (`run`, `graph`, `validate`, `single-node`, `prune`) plus the
/// registry-specific `list` — recognized from the leading token so the
/// artifact-only verbs (`render`, `fold`) and the `resume` stub stay on the
/// direct-dispatch path.
fn routes_through_registry(argv: &[std::ffi::OsString]) -> bool {
    let leading = argv.get(1).map(std::ffi::OsString::as_os_str);
    matches!(
        leading.and_then(std::ffi::OsStr::to_str),
        Some("list" | "run" | "graph" | "validate" | "single-node" | "prune")
    )
}

/// Dispatch a parsed verb the reference binary serves **directly** — the
/// artifact-only verbs (`render`, `fold`) and the `resume` stub, none of which
/// carries a flow. Artifact-only verbs read their artifact from standard input. The
/// flow-selecting verbs (`run`, `graph`, `validate`, `single-node`, `prune`) and
/// the registry-specific `list` never reach here — they route through the registry
/// in `main` ([`routes_through_registry`]).
fn dispatch(verb: Verb) -> ExitCode {
    let mut stdout = io::stdout().lock();
    match verb {
        Verb::Render => match read_stdin() {
            Ok(bytes) => render_verb(&bytes, None, RenderFormat::Dot, &mut stdout),
            Err(e) => {
                eprintln!("dagr render: cannot read graph artifact from stdin: {e}");
                ExitCode::InvalidUsage
            }
        },
        Verb::Fold => match read_stdin() {
            Ok(bytes) => fold_verb(&bytes, &[], &mut stdout),
            Err(e) => {
                eprintln!("dagr fold: cannot read event stream from stdin: {e}");
                ExitCode::InvalidUsage
            }
        },
        Verb::Resume => resume_verb_stub(&mut stdout),
        // Every flow-selecting verb routes through the registry in `main` and never
        // reaches here (recognized by `routes_through_registry`).
        Verb::Run | Verb::Graph | Verb::Validate | Verb::SingleNode | Verb::Prune => {
            unreachable!("flow-selecting verbs route through the registry in main")
        }
    }
}

/// Read all of standard input into a byte buffer.
fn read_stdin() -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    io::stdin().lock().read_to_end(&mut buf)?;
    Ok(buf)
}
