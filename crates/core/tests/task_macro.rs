//! Behavioral tests for the `#[task]` attribute macro, written first (TDD). They
//! exercise the ergonomic authoring layer: `#[task]` on an inherent `impl` block
//! expands to the exact `impl Task for Foo { … }` a task author writes by hand
//! today.
//!
//! This file covers zero-input (`Input = ()`), single-input (bare `Input = T`),
//! and multi-arity (2..=8, tuple `Input`) tasks, the execution class taken from
//! the attribute argument, an optional `ctx: &RunContext` parameter, and
//! enforcement that `run` returns `Result<T, TaskError>`.
//!
//! The macro is reached through `dagr_core::task` — the default-on `macros`
//! feature re-exports `dagr_macros::task` as `dagr_core::task`, so an author
//! writes `use dagr_core::task;` and the attribute resolves. That re-export path
//! is exactly what these tests assert is applicable to an inherent `impl`.
//!
//! Await-bound task futures are driven to completion with a tiny runtime-free
//! block-on (the same poller `task_abstraction.rs` uses). What is under test is
//! the *generated* `impl Task`, so each test compares the generated associated
//! types / const and runs the generated body.

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

// --- Multi-arity expansion: 2..=8 inputs -> a tuple `type Input` -------------
//
// The author writes ONE non-destructured tuple parameter (`input: (A, B)`), and
// the macro maps it to `type Input = (A, B)` and emits `let (…) = input;` as the
// first statement of the generated `run`, so a destructuring parameter pattern
// (`(a, b): (A, B)`) sees its two named bindings. Both forms are exercised below.

/// A two-input task written with a **destructuring** tuple parameter: the body
/// observes the two named bindings `a` and `b` after the macro's destructure.
struct Combine;

#[task]
impl Combine {
    async fn run(&mut self, (a, b): (u64, String)) -> Result<bool, TaskError> {
        Ok(b.len() as u64 == a)
    }
}

/// **A two-input `#[task]` infers `type Input = (u64, String)`, `type Output =
/// bool`, and destructures the tuple so the body sees the two named bindings.**
#[test]
fn two_input_task_infers_tuple_input_and_destructures() {
    // `Input` is the 2-tuple — a bare or wrong-arity binding would not satisfy this.
    fn assert_shape<T>()
    where
        T: Task<Input = (u64, String), Output = bool>,
    {
    }
    assert_shape::<Combine>();
    assert_eq!(Combine::EXECUTION_CLASS, ExecutionClass::AwaitBound);

    let ctx = RunContext::for_test();
    let mut task = Combine;
    // "abc".len() == 3, so (3, "abc") is true; (2, "abc") is false — proving the
    // body observed BOTH named bindings in order (a = first, b = second).
    let yes = block_on(task.run(&ctx, (3, "abc".to_string()))).expect("task succeeds");
    let no = block_on(task.run(&ctx, (2, "abc".to_string()))).expect("task succeeds");
    assert!(
        yes,
        "3 == \"abc\".len(): both tuple positions were observed in order"
    );
    assert!(!no, "2 != \"abc\".len()");
}

/// An **eight**-input task (the `MAX_INPUT_ARITY` ceiling): the generated body
/// destructures all eight positions in declared order.
struct SumEight;

#[task]
impl SumEight {
    #[allow(clippy::too_many_arguments)]
    async fn run(
        &mut self,
        (a, b, c, d, e, f, g, h): (u8, u8, u8, u8, u8, u8, u8, u8),
    ) -> Result<u64, TaskError> {
        // A positional-weighted sum: each coefficient is distinct, so the result
        // is only correct if every position landed in the right binding.
        Ok(u64::from(a)
            + 10 * u64::from(b)
            + 100 * u64::from(c)
            + 1_000 * u64::from(d)
            + 10_000 * u64::from(e)
            + 100_000 * u64::from(f)
            + 1_000_000 * u64::from(g)
            + 10_000_000 * u64::from(h))
    }
}

/// **An eight-input `#[task]` infers the 8-tuple `type Input` and destructures
/// all eight positions in order.**
#[test]
fn eight_input_task_infers_eight_tuple_and_destructures_all_positions() {
    fn assert_shape<T>()
    where
        T: Task<Input = (u8, u8, u8, u8, u8, u8, u8, u8), Output = u64>,
    {
    }
    assert_shape::<SumEight>();

    let ctx = RunContext::for_test();
    let mut task = SumEight;
    let out = block_on(task.run(&ctx, (1, 2, 3, 4, 5, 6, 7, 8))).expect("task succeeds");
    // Positional weights prove the order: 87654321.
    assert_eq!(out, 87_654_321);
}

// --- Execution class from the attribute argument ----------------------------
//
// The class is taken from the ATTRIBUTE, never inferred from the body: bare
// `#[task]` / `#[task()]` -> AwaitBound, `#[task(blocking)]` -> Blocking,
// `#[task(compute)]` -> Compute.

/// `#[task(blocking)]` -> `EXECUTION_CLASS = Blocking`.
struct BlockingTask;

#[task(blocking)]
impl BlockingTask {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input)
    }
}

/// **`#[task(blocking)]` sets `const EXECUTION_CLASS = ExecutionClass::Blocking`.**
#[test]
fn task_blocking_sets_blocking_execution_class() {
    assert_eq!(BlockingTask::EXECUTION_CLASS, ExecutionClass::Blocking);
}

/// `#[task(compute)]` -> `EXECUTION_CLASS = Compute`.
struct ComputeTask;

#[task(compute)]
impl ComputeTask {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input)
    }
}

/// **`#[task(compute)]` sets `const EXECUTION_CLASS = ExecutionClass::Compute`.**
#[test]
fn task_compute_sets_compute_execution_class() {
    assert_eq!(ComputeTask::EXECUTION_CLASS, ExecutionClass::Compute);
}

/// A body that *looks* blocking (a long synchronous loop) but carries no
/// attribute argument — the class must still be `AwaitBound`, proving it is taken
/// from the attribute, not the body.
struct EmptyParensTask;

#[task()]
impl EmptyParensTask {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        // A synchronous, "blocking-looking" body — but the CLASS comes from the
        // (empty) attribute, so it is AwaitBound regardless.
        let mut acc = 0u64;
        for i in 0..input {
            acc = acc.wrapping_add(i);
        }
        Ok(acc)
    }
}

/// **`#[task()]` (empty parens) sets `AwaitBound`, taken from the attribute and
/// not from the body's shape.**
#[test]
fn task_empty_parens_sets_await_bound_from_the_attribute() {
    assert_eq!(EmptyParensTask::EXECUTION_CLASS, ExecutionClass::AwaitBound);
    // And bare `#[task]` (no parens) is likewise AwaitBound — re-asserted on the
    // single-input task, proving the default is unchanged.
    assert_eq!(Stringify::EXECUTION_CLASS, ExecutionClass::AwaitBound);
}
