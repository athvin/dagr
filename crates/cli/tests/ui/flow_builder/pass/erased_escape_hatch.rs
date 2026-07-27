//! Compile-pass: the escape hatch. A task **without** `StableName` (which the
//! graph-emittable `source` / `node` reject) compiles through the type-erased
//! `source_erased` / `node_erased` — a DAG built with them is documented as **not**
//! graph-emittable, but it builds and runs. This is the companion to the
//! `source_without_stable_name.rs` compile-fail sample: same task, erased surface,
//! green build.

use dagr_cli::prelude::FlowBuilder;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// A source with **no** `StableName` impl.
struct Unnamed;
impl Task for Unnamed {
    type Input = ();
    type Output = u64;
    async fn run(&mut self, _ctx: &RunContext, _i: ()) -> Result<u64, TaskError> {
        Ok(7)
    }
}

/// A consumer with **no** `StableName` impl.
struct DoubleIt;
impl Task for DoubleIt {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _ctx: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let mut f = FlowBuilder::new(&mut flow);
    // The type-erased escape hatches accept a `StableName`-less task and preserve the
    // real `Handle<T>` (so wiring stays exact-typed).
    let n = f.source_erased("unnamed", Unnamed);
    let _ = f.node_erased::<DoubleIt, _>("double", DoubleIt, n);
}
