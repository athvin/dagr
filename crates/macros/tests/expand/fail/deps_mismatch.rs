//! Compile-fail: a registration whose bound handle type does **not** match the
//! task's declared input — a deps type mismatch at the `RunnableFlow` seam. The
//! `register` bound `D: Deps<Inputs = T::Input>` is the compile-time exact-type
//! check; binding a `Handle<String>` into a task whose `type Input = u64`
//! reds the build. This snapshot shows the expected-vs-actual `Deps`/`Handle`
//! types so a wrong wiring is a compile error, never a runtime surprise.

use dagr_core::task;

use dagr_cli::run_flow::RunnableFlow;

/// A source producing a `String`.
struct Label;

#[task]
impl Label {
    async fn run(&mut self, _input: ()) -> Result<String, dagr_core::TaskError> {
        Ok("hi".to_string())
    }
}

/// A consumer whose declared input is `u64` — *not* the `String` the upstream
/// produces.
struct WantsU64;

#[task]
impl WantsU64 {
    async fn run(&mut self, input: u64) -> Result<u64, dagr_core::TaskError> {
        Ok(input * 2)
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let label = flow.register_source("label", Label); // Handle<String>
    // Bind a `Handle<String>` where `type Input = u64` is required: the exact-type
    // `Deps<Inputs = u64>` bound is not satisfied by `Handle<String>`.
    let _ = flow.register::<WantsU64, _>("wants-u64", WantsU64, label);
}
