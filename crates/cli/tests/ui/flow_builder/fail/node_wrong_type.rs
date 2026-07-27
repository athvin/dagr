//! Compile-fail: a `Handle<T>` bound into `FlowBuilder::node` whose task declares a
//! *different* `Input` type. The façade forwards to `register_named`, whose
//! `D: Deps<Inputs = T::Input>` bound is the compile-time exact-type check, so
//! binding a `Handle<String>` into a task whose `type Input = u64` reds the build —
//! a mis-wiring is a compile error, never a runtime surprise.

use dagr_cli::prelude::FlowBuilder;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// A source producing a `String`.
struct Label;
impl StableName for Label {
    const STABLE_NAME: &'static str = "Label";
}
impl Task for Label {
    type Input = ();
    type Output = String;
    async fn run(&mut self, _ctx: &RunContext, _i: ()) -> Result<String, TaskError> {
        Ok("hi".to_string())
    }
}

/// A consumer whose declared input is `u64` — *not* the `String` the upstream
/// produces.
struct WantsU64;
impl StableName for WantsU64 {
    const STABLE_NAME: &'static str = "WantsU64";
}
impl Task for WantsU64 {
    type Input = u64;
    type Output = u64;
    async fn run(&mut self, _ctx: &RunContext, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let mut f = FlowBuilder::new(&mut flow);
    let label = f.source("label", Label); // Handle<String>
    // Bind a `Handle<String>` where `type Input = u64` is required: the exact-type
    // `Deps<Inputs = u64>` bound is not satisfied by `Handle<String>`.
    let _ = f.node::<WantsU64, _>("wants-u64", WantsU64, label);
}
