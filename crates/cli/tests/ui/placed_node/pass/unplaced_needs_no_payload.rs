//! Pass: the same codec-less task registers fine as an **unplaced** node, and a
//! `Payload`-typed task registers fine as a **placed** one. The bound is on
//! placement, not on registration in general — nothing about the local path
//! changed.

use dagr_cli::run_flow::RunnableFlow;
use dagr_core::TaskError;
use dagr_core::assembly::{NodePolicy, Placement};
use dagr_core::context::RunContext;
use dagr_core::task::Task;

struct NoCodec;

struct Local;
impl Task for Local {
    type Input = ();
    type Output = NoCodec;
    async fn run(&mut self, _ctx: &RunContext, _i: ()) -> Result<NoCodec, TaskError> {
        Ok(NoCodec)
    }
}

struct Remote;
impl Task for Remote {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _ctx: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(7)
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    // Unplaced: no codec required.
    let _ = flow.register_source("local", Local);
    // Placed, with a `Payload` output: accepted.
    let _ = flow.register_source_placed(
        "remote",
        Remote,
        NodePolicy::new(),
        Placement::new().cpu("500m"),
    );
}
