//! **Layer A — the authoring surface.**
//!
//! One runnable example per layer keeps the docs honest: CI compiles and runs it,
//! so a renamed or removed API fails the build. This one shows the *authoring*
//! surface a pipeline developer touches directly — writing [`Task`]s and reading
//! their declared input/output types from the declaration alone —
//! and exercises a single task through the single-task test kit
//! ([`SingleTaskTest`](dagr_core::test_kit::SingleTaskTest)), which drives a
//! synchronous task with **no async runtime** and an await-bound task with the
//! kit's own runtime — the author stands up nothing. Run it with
//! `cargo run --example layer_a_authoring`.

use dagr_core::context::RunContext;
use dagr_core::task::{ExecutionClass, Task};
use dagr_core::{SingleTaskTest, TaskError};

/// A task is a *value* holding constructor-captured configuration (here a
/// threshold), with its input and output types readable from the declaration
/// without reading the body. This one classifies a number.
struct ThresholdGate {
    threshold: u32,
}

impl Task for ThresholdGate {
    type Input = u32;
    type Output = bool;
    // The execution class is declared on the task; the default is await-bound.
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::AwaitBound;

    async fn run(&mut self, _ctx: &RunContext, input: u32) -> Result<bool, TaskError> {
        Ok(input >= self.threshold)
    }
}

/// A task that consumes nothing (`Input = ()`) and produces a value — a *source*.
/// The error a task returns distinguishes retry-eligible, permanent, and skip;
/// this one deliberately skips when its budget is zero.
struct MaybeProduce {
    budget: u32,
}

impl Task for MaybeProduce {
    type Input = ();
    type Output = u32;
    async fn run(&mut self, _ctx: &RunContext, _input: ()) -> Result<u32, TaskError> {
        if self.budget == 0 {
            // A deliberate skip — branching lives in the task, not the graph.
            Err(TaskError::skip("nothing to produce with a zero budget"))
        } else {
            Ok(self.budget * 2)
        }
    }
}

fn main() {
    // A single task can be exercised with a hand-built context and NO runtime —
    // this is the testing surface. `run_sync` needs no async runtime at all.
    let produced = SingleTaskTest::new(MaybeProduce { budget: 21 }).run_sync();
    assert!(produced.is_success(), "a non-zero budget produces a value");
    let value = *produced.output().expect("a produced value");
    println!("source produced {value}");

    // The skip class is a first-class outcome the kit surfaces.
    let skipped = SingleTaskTest::new(MaybeProduce { budget: 0 }).run_sync();
    assert!(!skipped.is_success(), "a zero budget skips");
    println!(
        "zero-budget source skipped: {}",
        skipped.error().map(TaskError::message).unwrap_or_default()
    );

    // Drive the gate with an explicit input through the kit's runtime.
    let passes = SingleTaskTest::new(ThresholdGate { threshold: 40 })
        .input(value)
        .run_await();
    assert!(passes.is_success());
    println!("{value} >= 40 ? {:?}", passes.output());

    // Removing the framework's retry, timeout, and logging features would require
    // no change to any task body — they are pure business logic.
    assert_eq!(
        passes.output(),
        Some(&true),
        "42 clears the threshold of 40"
    );
}
