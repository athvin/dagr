//! Compile-fail: a task **without** `StableName` passed to the graph-emittable
//! `FlowBuilder::source`. `source` forwards to `register_source_named`, whose
//! `T: StableName` (and `T::Output: StableName`) bounds are what let the DAG emit
//! author-declared names — a task lacking that impl reds the build. The escape
//! hatch `source_erased` (not graph-emittable) accepts the same task and compiles;
//! see the `pass/` companion sample.

use dagr_cli::prelude::FlowBuilder;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// A source with **no** `StableName` impl — so it is not graph-emittable and cannot
/// go through the stable-name-aware `source`.
struct Unnamed;
impl Task for Unnamed {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _ctx: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(7)
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let mut f = FlowBuilder::new(&mut flow);
    // `Unnamed` does not implement `StableName`, so the graph-emittable `source`
    // rejects it.
    let _ = f.source("unnamed", Unnamed);
}
