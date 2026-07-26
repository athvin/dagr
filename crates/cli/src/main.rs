//! The `dagr` pipeline-binary entry point — the C26 command-line contract's
//! reference driver (arch.md `### C26 · Command-line contract`; tickets T55, T74).
//!
//! Every dagr pipeline binary inherits the same command surface from the library
//! ([`dagr_cli::contract`]): the standard verbs, the typed-parameter seam, the
//! reserved library-flag namespace, and the exhaustive exit-code table. This
//! binary is the library's own reference driver. It **delegates flow selection to
//! [`dagr_cli::registry::run_registry`]** over a single-flow registry (ADR 086),
//! so `dagr run` and `dagr list` route through the many-flows-per-binary seam
//! rather than being hand-dispatched — the reference driver stops being a
//! misleading verb-only dispatcher. The artifact-only verbs (`render`, `fold`) and
//! the `resume` stub — the verbs that need no pipeline baked in — are dispatched
//! directly, mapping each outcome to its C26 exit code.
//!
//! The remaining pipeline-bound verbs (`graph`, `validate`, `single-node`,
//! `prune`) require a concrete assembled pipeline; routing them through the
//! registry is **T75** (`docs/implementation/088-T75-…`). Until then this
//! reference binary reports that those verbs need a pipeline-specific binary — a
//! real pipeline crate wires the same verbs to its own pipeline.

use std::io::{self, Read, Write};
use std::process::ExitCode as ProcExit;

use dagr_cli::contract::{
    banner_suppressed_by_env, fold_verb, parse_cli, print_banner, render_verb, resume_verb_stub,
    split_banner_flag, ExitCode, ParseOutcome, RenderFormat, Verb,
};
use dagr_cli::registry::{run_registry, FlowRegistry};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// The reference driver's trivial single-node flow — a consume-nothing source that
/// succeeds. It carries no real pipeline (that is a pipeline crate's job), but a
/// registry needs a factory, so this is the minimal flow that lets `dagr run` /
/// `dagr list` route through [`run_registry`] end to end on the reference binary.
struct ReferenceSource;
impl Task for ReferenceSource {
    type Input = ();
    type Output = ();
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<(), TaskError> {
        Ok(())
    }
}

/// Build the reference driver's single flow (a fresh one per invocation — the
/// factory is re-invoked by [`run_registry`]).
fn build_reference_flow() -> RunnableFlow {
    let mut flow = RunnableFlow::new();
    let _h = flow.register_source("reference", ReferenceSource);
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

    // `run` and `list` route through the registry (ADR 086): the reference driver
    // hosts one trivial flow, so `dagr run` drives it and `dagr list` prints its
    // name. Recognizing these before the direct dispatch keeps the artifact-only
    // verbs (render/fold/resume) handled here and delegates flow selection to the
    // shared entrypoint. `list` is registry-specific; `run` is the C26 run verb.
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

/// Whether this invocation is one the registry routes (`run` or the
/// registry-specific `list`) — recognized from the leading token so the
/// artifact-only verbs stay on the direct-dispatch path.
fn routes_through_registry(argv: &[std::ffi::OsString]) -> bool {
    let leading = argv.get(1).map(std::ffi::OsString::as_os_str);
    leading == Some(std::ffi::OsStr::new("run")) || leading == Some(std::ffi::OsStr::new("list"))
}

/// Dispatch a parsed verb the reference binary serves directly (the artifact-only
/// verbs and the `resume` stub). Artifact-only verbs read their artifact from
/// standard input. `run` / `list` never reach here — they route through the
/// registry in `main`.
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
        // `run` routes through the registry in `main` and never reaches here.
        Verb::Run => unreachable!("run routes through the registry in main"),
        // Pipeline-bound verbs need a concrete pipeline this reference binary does
        // not carry; routing them through the registry is T75. A real pipeline crate
        // wires them to its own pipeline through the same library entry points.
        Verb::Graph | Verb::Validate | Verb::SingleNode | Verb::Prune => {
            let _ = writeln!(
                stdout,
                "the `{}` verb needs a pipeline-specific binary (this is the library's \
                 reference driver, which carries no pipeline); build your pipeline crate and \
                 call the same library entry points (dagr_cli::contract, dagr_cli::graph, \
                 dagr_cli::driver), or route it through dagr_cli::registry::run_registry (T75)",
                verb.name()
            );
            ExitCode::InvalidUsage
        }
    }
}

/// Read all of standard input into a byte buffer.
fn read_stdin() -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    io::stdin().lock().read_to_end(&mut buf)?;
    Ok(buf)
}
