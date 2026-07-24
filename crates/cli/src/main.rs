//! The `dagr` pipeline-binary entry point — the C26 command-line contract's
//! reference driver (arch.md `### C26 · Command-line contract`; ticket T55).
//!
//! Every dagr pipeline binary inherits the same command surface from the library
//! ([`dagr_cli::contract`]): the standard verbs, the typed-parameter seam, the
//! reserved library-flag namespace, and the exhaustive exit-code table. This
//! binary is the library's own reference driver: it parses the command line
//! through the library, prints the available verbs on a bare invocation, and
//! dispatches the **artifact-only** verbs (`render`, `fold`) and the `resume`
//! stub — the verbs that need no pipeline baked in — mapping each outcome to its
//! C26 exit code.
//!
//! The pipeline-bound verbs (`graph`, `validate`, `run`, `single-node`, `prune`)
//! require a concrete assembled pipeline, which a real pipeline binary supplies by
//! calling the same library entry points ([`dagr_cli::contract::validate_verb`],
//! [`dagr_cli::graph::graph_verb`], the driver, …). This reference binary carries
//! no such pipeline, so it reports that those verbs need a pipeline-specific
//! binary and exits with the invalid-usage code — the surface is exercised end to
//! end, and a pipeline crate wires the same verbs to its own pipeline.

use std::io::{self, Read, Write};
use std::process::ExitCode as ProcExit;

use dagr_cli::contract::{
    fold_verb, parse_cli, render_verb, resume_verb_stub, ExitCode, ParseOutcome, RenderFormat, Verb,
};

fn main() -> ProcExit {
    let outcome = parse_cli(std::env::args_os());
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

/// Dispatch a parsed verb the reference binary can serve without a concrete
/// pipeline. Artifact-only verbs read their artifact from standard input.
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
        // Pipeline-bound verbs need a concrete pipeline this reference binary does
        // not carry; a real pipeline crate wires them to its own pipeline through
        // the same library entry points.
        Verb::Graph | Verb::Validate | Verb::Run | Verb::SingleNode | Verb::Prune => {
            let _ = writeln!(
                stdout,
                "the `{}` verb needs a pipeline-specific binary (this is the library's \
                 reference driver, which carries no pipeline); build your pipeline crate and \
                 call the same library entry points (dagr_cli::contract, dagr_cli::graph, \
                 dagr_cli::driver)",
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
