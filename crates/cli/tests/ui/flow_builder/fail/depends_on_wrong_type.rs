//! Compile-fail: a `Handle<T>` passed to `f.task(..).depends_on(..)` whose task
//! declares a *different* `Input` type. `depends_on` carries the same exact-type
//! `D: Deps<Inputs = T::Input>` bound as the positional `node`, so naming a
//! `Handle<Label>` as the upstream of a task whose `type Input = Count` reds the build
//! — the explicit builder loses none of the compile-time wiring guarantee.
//!
//! The payload types carry `StableName` impls so the *only* error is the deps type
//! mismatch, not incidental `StableName`-bound noise.

use dagr_cli::prelude::FlowBuilder;
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::context::RunContext;
use dagr_core::stable_name::StableName;
use dagr_core::task::Task;
use dagr_core::TaskError;

/// The upstream's output payload.
#[derive(Clone)]
struct Label(String);
impl StableName for Label {
    const STABLE_NAME: &'static str = "Label";
}

/// A *different* payload — what the downstream declares as its input.
#[derive(Clone)]
struct Count(u64);
impl StableName for Count {
    const STABLE_NAME: &'static str = "Count";
}

/// A source producing a `Label`.
struct MakeLabel;
impl StableName for MakeLabel {
    const STABLE_NAME: &'static str = "MakeLabel";
}
impl Task for MakeLabel {
    type Input = ();
    type Output = Label;
    async fn run(&mut self, _ctx: &RunContext, _i: ()) -> Result<Label, TaskError> {
        Ok(Label("hi".to_string()))
    }
}

/// A consumer whose declared input is `Count` — *not* the `Label` the upstream
/// produces.
struct WantsCount;
impl StableName for WantsCount {
    const STABLE_NAME: &'static str = "WantsCount";
}
impl Task for WantsCount {
    type Input = Count;
    type Output = Count;
    async fn run(&mut self, _ctx: &RunContext, input: Count) -> Result<Count, TaskError> {
        Ok(Count(input.0 * 2))
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let mut f = FlowBuilder::new(&mut flow);
    let label = f.source("label", MakeLabel); // Handle<Label>
    // Name a `Handle<Label>` as the upstream where `type Input = Count` is required:
    // the exact-type `Deps<Inputs = Count>` bound is not satisfied by `Handle<Label>`.
    let _ = f.task("wants-count", WantsCount).depends_on(label);
}
