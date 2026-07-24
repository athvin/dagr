//! `dagr-t56-beta` — the second structurally-distinct **sample pipeline binary**
//! the C26 CLI acceptance suite (ticket T56, 069) drives as a subprocess. It
//! carries the `beta` pipeline (the durable stage boundary `load → transform` plus
//! a controllable `maybe-fail` node and a `decide-skip` node — a different node set
//! and edges than `alpha`) and wires the same real C26 command surface through the
//! shared harness. Test-support scaffolding only — ships in no released binary.

use std::process::ExitCode as ProcExit;

#[path = "../t56_sample.rs"]
mod t56_sample;

use dagr_cli::contract::ParamSpec;
use t56_sample::{dispatch_main, Sample};

fn main() -> ProcExit {
    dispatch_main(&Sample {
        pipeline_name: "t56-beta",
        param: ParamSpec::new("region", "a typed string parameter for the beta pipeline"),
    })
}
