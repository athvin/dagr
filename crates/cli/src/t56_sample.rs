//! Shared **sample-pipeline harness** for the C26 CLI acceptance suite — ticket
//! T56 (069). Test-support scaffolding only; `include!`d by the two
//! structurally-distinct sample bins (`dagr-t56-alpha`, `dagr-t56-beta`).
//!
//! **Tests-first stub.** This first commit lands the failing acceptance suite
//! against a deliberately-incomplete harness: every verb is recognized (parsed
//! through the real library `parse_cli`) but not yet wired to its library entry
//! point, so the acceptance assertions fail. The next commit wires each verb to
//! the real `dagr_cli::contract` / `dagr_cli::driver` / `dagr_cli::graph` entry
//! points and emits the run-store artifacts the suite reads.

use std::process::ExitCode as ProcExit;

use dagr_cli::contract::{parse_cli, Cli, ExitCode, ParamSpec, ParseOutcome};

/// A single structurally-distinct sample pipeline the harness drives.
pub struct Sample {
    /// The stable pipeline identity (the run-store directory name).
    pub pipeline_name: &'static str,
    /// A typed parameter the pipeline declares.
    pub param: ParamSpec,
}

/// Parse and dispatch one invocation of a sample binary, returning the process
/// exit code. Tests-first stub: recognizes the verb set through the real library
/// parser, but does not yet wire the verbs, so the acceptance suite fails.
pub fn dispatch_main(sample: &Sample) -> ProcExit {
    // Touch both fields so the tests-first stub compiles clean under `-D warnings`;
    // the implementation commit reads them for real.
    let _ = (sample.pipeline_name, &sample.param);
    let code = match parse_cli(std::env::args_os()) {
        ParseOutcome::Help { exit, text } => {
            print!("{text}");
            exit
        }
        ParseOutcome::Error { exit, message } => {
            eprintln!("dagr: {message}");
            exit
        }
        // Not yet wired: every recognized verb returns invalid-usage until the
        // implementation commit wires it to its real library entry point.
        ParseOutcome::Parsed(Cli { verb: _ }) => ExitCode::InvalidUsage,
    };
    code.into()
}
