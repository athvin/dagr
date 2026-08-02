//! Compile-fail: a **placed** source whose output type is not a `Payload`.
//!
//! A placed node is one dagr may hand to a pod, and a pod can only be handed
//! bytes. `register_source_placed` therefore carries `T::Output: Payload`, so a
//! type with no codec reds the build here instead of failing at submission time
//! with a value nobody can serialize. The companion `pass/` sample registers the
//! same task *unplaced* and compiles.

use dagr_cli::run_flow::RunnableFlow;
use dagr_core::TaskError;
use dagr_core::assembly::{NodePolicy, Placement};
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;

/// An output type with **no** `Payload` impl: it cannot cross a process boundary.
struct NoCodec;

struct Local;
impl StableName for Local {
    const STABLE_NAME: &'static str = "t108.Local";
}
impl Task for Local {
    type Input = ();
    type Output = NoCodec;
    async fn run(&mut self, _ctx: &RunContext, _i: ()) -> Result<NoCodec, TaskError> {
        Ok(NoCodec)
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let _ = flow.register_source_placed(
        "unplaceable",
        Local,
        NodePolicy::new(),
        Placement::new().cpu("500m"),
    );
}
