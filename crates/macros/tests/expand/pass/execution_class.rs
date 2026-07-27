//! Compile-pass: each execution-class attribute form sets the corresponding
//! `EXECUTION_CLASS` const, and the optional `ctx: &RunContext` parameter is
//! accepted both with and without it.
//!
//! - `#[task]`            -> `ExecutionClass::AwaitBound`
//! - `#[task(blocking)]`  -> `ExecutionClass::Blocking`
//! - `#[task(compute)]`   -> `ExecutionClass::Compute`
//!
//! The class comes from the **attribute**, never the body; each `const` assert
//! below is checked at compile time.

use dagr_core::task;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::TaskError;

/// Bare `#[task]` -> await-bound. This one *omits* `ctx`.
struct AwaitBoundTask;

#[task]
impl AwaitBoundTask {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input)
    }
}

/// `#[task(blocking)]` -> blocking. This one *takes* `ctx: &RunContext` and uses
/// it, proving the parameter is detected by type and threaded into the body.
struct BlockingTask;

#[task(blocking)]
impl BlockingTask {
    async fn run(&mut self, ctx: &RunContext, input: u64) -> Result<u32, TaskError> {
        Ok(ctx.attempt().wrapping_add(input as u32))
    }
}

/// `#[task(compute)]` -> compute.
struct ComputeTask;

#[task(compute)]
impl ComputeTask {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input.wrapping_mul(3))
    }
}

/// Compile-time proof that `T::EXECUTION_CLASS` equals `expected`.
const fn assert_class<T: Task>(expected: ExecutionClass) -> bool {
    matches!((T::EXECUTION_CLASS, expected),
        (ExecutionClass::AwaitBound, ExecutionClass::AwaitBound)
        | (ExecutionClass::Blocking, ExecutionClass::Blocking)
        | (ExecutionClass::Compute, ExecutionClass::Compute))
}

fn main() {
    const _: () = assert!(assert_class::<AwaitBoundTask>(ExecutionClass::AwaitBound));
    const _: () = assert!(assert_class::<BlockingTask>(ExecutionClass::Blocking));
    const _: () = assert!(assert_class::<ComputeTask>(ExecutionClass::Compute));
}
