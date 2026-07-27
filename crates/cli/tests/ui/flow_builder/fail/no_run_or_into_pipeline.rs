//! Compile-fail: the `FlowBuilder` façade exposes **no** consuming/execution
//! method. Making it a newtype (not a type alias for `RunnableFlow`) is deliberate:
//! an alias would leak `run` / `into_pipeline` into a DAG body, letting a
//! declaration consume itself. Neither method resolves on a `FlowBuilder`, so both
//! calls below fail to compile — a DAG body can only *declare*, never *execute*.

use dagr_cli::prelude::FlowBuilder;
use dagr_cli::run_flow::RunnableFlow;

fn main() {
    let mut flow = RunnableFlow::new();
    let f = FlowBuilder::new(&mut flow);
    // `run` is an execution seam on `RunnableFlow`, not exposed on the façade.
    let _ = f.run();
    // `into_pipeline` consumes the flow — also not exposed on the façade.
    let _ = f.into_pipeline();
}
