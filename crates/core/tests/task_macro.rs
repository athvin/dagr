//! Behavioral tests for the `#[task]` attribute macro (ticket T71 / 083),
//! written first (TDD). They exercise the ergonomic authoring layer ADR 082
//! decided on: `#[task]` on an inherent `impl` block expands to the exact
//! `impl Task for Foo { … }` a task author writes by hand today.
//!
//! This slice covers **only** zero-input (`Input = ()`) and single-input (bare
//! `Input = T`) tasks in the `AwaitBound` execution class, with an optional
//! `ctx: &RunContext` parameter, and enforcement that `run` returns
//! `Result<T, TaskError>`. Multi-arity, execution-class arguments, and tuple
//! wiring are T72; the quickstart rewrite and the `trybuild` corpus are T73.
//!
//! The macro is reached through `dagr_core::task` — the default-on `macros`
//! feature re-exports `dagr_macros::task` as `dagr_core::task`, so an author
//! writes `use dagr_core::task;` and the attribute resolves. That re-export path
//! is exactly what these tests assert is applicable to an inherent `impl`.
//!
//! Await-bound task futures are driven to completion with a tiny runtime-free
//! block-on (the same poller `task_abstraction.rs` uses); the real runner is
//! C14. What is under test is the *generated* `impl Task`, so each test compares
//! the generated associated types / const and runs the generated body.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use dagr_core::task;
use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::TaskError;

/// Drive a future to completion on the current thread with no runtime — the
/// task futures here never suspend on external I/O, so a busy-poll with the
/// no-op waker is sufficient and keeps the suite runtime-free and `unsafe`-free.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(future);
    loop {
        if let Poll::Ready(value) = fut.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

// --- Zero-input expansion ---------------------------------------------------

/// A zero-input task holding constructor-captured configuration. The author
/// writes only the `run` fn (with a `()` input); `#[task]` generates the
/// `impl Task`.
struct Answer {
    base: u64,
}

#[task]
impl Answer {
    async fn run(&mut self, _input: ()) -> Result<u64, TaskError> {
        Ok(self.base)
    }
}

/// **A zero-input `#[task]` generates `Input = ()`, `Output = T`, the
/// await-bound class, and a `run` that invokes the user's body.**
#[test]
fn zero_input_task_expands_to_the_hand_written_impl() {
    // The generated associated types and const are exactly what a hand author
    // would declare — asserted at the type/const level.
    fn assert_shape<T>()
    where
        T: Task<Input = (), Output = u64>,
    {
    }
    assert_shape::<Answer>();
    assert_eq!(Answer::EXECUTION_CLASS, ExecutionClass::AwaitBound);

    // The generated `run` invokes the user's body over the trait receiver.
    let ctx = RunContext::for_test();
    let mut task = Answer { base: 42 };
    let out = block_on(task.run(&ctx, ())).expect("task succeeds");
    assert_eq!(out, 42);
}

// --- Single-input expansion (bare T, never (T,)) ----------------------------

/// A single-input task: one dep arg `input: u64` delivered **bare** (the
/// arity-1 blanket `Deps` impl delivers the bare value, never `(u64,)`).
struct Stringify;

#[task]
impl Stringify {
    async fn run(&mut self, input: u64) -> Result<String, TaskError> {
        Ok(input.to_string())
    }
}

/// **A single-input `#[task]` infers `Input = u64` (bare, not `(u64,)`) and
/// `Output = String`.**
#[test]
fn single_input_task_infers_bare_input_and_output() {
    // `Input` is the bare value type — a `(u64,)` binding would not satisfy this.
    fn assert_shape<T>()
    where
        T: Task<Input = u64, Output = String>,
    {
    }
    assert_shape::<Stringify>();
    assert_eq!(Stringify::EXECUTION_CLASS, ExecutionClass::AwaitBound);

    let ctx = RunContext::for_test();
    let mut task = Stringify;
    let out = block_on(task.run(&ctx, 7)).expect("task succeeds");
    assert_eq!(out, "7");
}

// --- Optional ctx parameter: absent ----------------------------------------

/// A task whose `run` omits `ctx`. The generated `run` still carries the trait's
/// `ctx` — it must type-check and raise **no** `unused` warning (warnings are
/// denied workspace-wide, so a stray `unused` would fail the build outright).
struct NoCtx;

#[task]
impl NoCtx {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input + 1)
    }
}

/// **A `run` omitting `ctx` type-checks and produces no `unused` warning for the
/// trait-supplied `ctx`.** (If the generated `ctx` were unused-warned, this test
/// file would not compile under `-D warnings`.)
#[test]
fn ctx_omitted_still_type_checks_without_unused_warning() {
    let ctx = RunContext::for_test();
    let mut task = NoCtx;
    let out = block_on(task.run(&ctx, 41)).expect("task succeeds");
    assert_eq!(out, 42);
}

// --- Optional ctx parameter: present, threaded into the body ----------------

/// A task whose `run` declares `ctx: &RunContext` and *uses* it — the generated
/// `run` must thread the trait's `ctx` into the body under that name.
struct UsesCtx;

#[task]
impl UsesCtx {
    async fn run(&mut self, ctx: &RunContext, _input: ()) -> Result<u32, TaskError> {
        Ok(ctx.attempt())
    }
}

/// **A `run` declaring `ctx: &RunContext` has the context threaded in and usable
/// in its body.**
#[test]
fn ctx_declared_is_threaded_into_the_body() {
    let ctx = RunContext::for_test();
    let mut task = UsesCtx;
    let out = block_on(task.run(&ctx, ())).expect("task succeeds");
    // `for_test()` reports attempt 1 — reading it proves the ctx reached the body.
    assert_eq!(out, ctx.attempt());
}

// --- Compatibility: a hand-written impl compiles unchanged alongside --------

/// A hand-written `impl Task` — the macro is purely additive, so the classic
/// path still compiles and runs unchanged next to the generated ones.
struct HandWritten {
    n: u64,
}

impl Task for HandWritten {
    type Input = u64;
    type Output = u64;

    async fn run(&mut self, _ctx: &RunContext, input: Self::Input) -> Result<u64, TaskError> {
        Ok(self.n + input)
    }
}

/// **A hand-written `impl Task` compiles and runs unchanged alongside the
/// generated impls** — the macro adds a path and removes none.
#[test]
fn hand_written_impl_still_works() {
    let ctx = RunContext::for_test();
    let mut task = HandWritten { n: 10 };
    let out = block_on(task.run(&ctx, 5)).expect("task succeeds");
    assert_eq!(out, 15);
}
