//! `dagr-t56-alpha` — one of the two structurally-distinct **sample pipeline
//! binaries** the C26 CLI acceptance suite (ticket T56, 069) drives as a
//! subprocess. It carries the `alpha` pipeline (a durable stage boundary
//! `load → transform` plus a `standalone` no-input node) and wires the real C26
//! command surface through the shared harness. Test-support scaffolding only —
//! ships in no released binary; it composes merged library entry points and adds
//! no framework capability.

use std::process::ExitCode as ProcExit;

#[path = "../t56_sample.rs"]
mod t56_sample;

use dagr_cli::contract::ParamSpec;
use t56_sample::{dispatch_main, Sample};

fn main() -> ProcExit {
    dispatch_main(&Sample {
        pipeline_name: "t56-alpha",
        param: ParamSpec::int("shard", "a typed integer parameter for the alpha pipeline"),
    })
}
