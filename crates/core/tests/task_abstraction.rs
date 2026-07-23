//! Behavioral unit tests for the C1 task abstraction and its classified error
//! (ticket T9 / 019). Written first, TDD: these exercise the public authoring
//! surface `dagr_core` exposes — the [`Task`] trait, its four declared elements
//! (input type, output type, execution class, work), the `&mut self` work
//! signature over a stub run context, constructor-captured configuration, the
//! no-input task, and the three-valued task-facing [`TaskError`].
//!
//! Every scenario here mirrors one bullet of the ticket's Test plan. The
//! compile-fail scenarios (non-`Send` capture, missing-`Sync`/`'static` output,
//! shared-reference invocation) live in the T8 UI harness under `tests/ui/`,
//! not here, because their assertion is *failure to compile*.
//!
//! These are await-bound tasks; their work returns a future. The tests drive
//! that future to completion with a tiny hand-rolled block-on so the suite
//! needs no async runtime and no framework machinery (the real runner is C14 /
//! T20; the real run context is C8 / T16 — this ticket references both only by
//! shape).

use dagr_core::task::{ExecutionClass, RunContext, Task};
use dagr_core::TaskError;

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// Drive a future to completion on the current thread with no runtime. The task
/// futures under test never actually suspend on external I/O in these unit
/// tests, so a busy-poll with the standard no-op waker ([`Waker::noop`], stable
/// since 1.85, well within the workspace MSRV) is sufficient and keeps the suite
/// runtime-free and `unsafe`-free (arch.md C28: a synchronous single-task test
/// needs no runtime; here we exercise await-bound tasks with the minimal poller
/// a test needs — the real runner is C14 / T20).
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(future);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => continue,
        }
    }
}

/// A representative constructor-captured task: it holds a `threshold` set when it
/// was constructed and compares its input against it. Business logic only — no
/// scheduling, retry, permit, timeout, or logging code lives in the body
/// (ticket DoD: task bodies are business logic only).
struct ThresholdGate {
    threshold: u32,
}

impl Task for ThresholdGate {
    type Input = u32;
    type Output = bool;

    async fn run(
        &mut self,
        _ctx: &RunContext,
        input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        Ok(input >= self.threshold)
    }
}

/// A no-input task: it consumes nothing (`Input = ()`) and still produces a
/// value. No placeholder/unit dance the author must explain — the declaration
/// simply states `Input = ()`.
struct ProduceSeven;

impl Task for ProduceSeven {
    type Input = ();
    type Output = u32;

    async fn run(
        &mut self,
        _ctx: &RunContext,
        _input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        Ok(7)
    }
}

/// A task whose work mutates a captured field, proving the `&mut self` signature
/// gives exclusive access without the author writing any synchronization.
struct Counter {
    seen: u32,
}

impl Task for Counter {
    type Input = ();
    type Output = u32;

    async fn run(
        &mut self,
        _ctx: &RunContext,
        _input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        self.seen += 1;
        Ok(self.seen)
    }
}

/// A task arranged to fail, returning each of the three task-facing error
/// classes on demand (selected by its captured `mode`), proving the work can
/// return a classified error instead of the output.
enum FailMode {
    Retryable,
    Permanent,
    Skip,
}

struct AlwaysFails {
    mode: FailMode,
}

impl Task for AlwaysFails {
    type Input = ();
    type Output = u32;

    async fn run(
        &mut self,
        _ctx: &RunContext,
        _input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        Err(match self.mode {
            FailMode::Retryable => TaskError::retryable("transient blip"),
            FailMode::Permanent => TaskError::permanent("bad input"),
            FailMode::Skip => TaskError::skip("nothing to do"),
        })
    }
}

/// A task that declares the blocking execution class, overriding the await-bound
/// default via the associated const.
struct BlockingWork;

impl Task for BlockingWork {
    type Input = ();
    type Output = u32;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Blocking;

    async fn run(
        &mut self,
        _ctx: &RunContext,
        _input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        Ok(0)
    }
}

/// A task that declares the compute execution class.
struct ComputeWork;

impl Task for ComputeWork {
    type Input = ();
    type Output = u32;
    const EXECUTION_CLASS: ExecutionClass = ExecutionClass::Compute;

    async fn run(
        &mut self,
        _ctx: &RunContext,
        _input: Self::Input,
    ) -> Result<Self::Output, TaskError> {
        Ok(0)
    }
}

/// **Readable declaration (positive).** The input and output types are
/// recoverable from the declaration alone — this function names them without
/// reading any work body, and its compilation is the assertion.
#[test]
fn input_and_output_types_are_readable_from_the_declaration() {
    fn declared_input<T: Task>() -> &'static str {
        std::any::type_name::<T::Input>()
    }
    fn declared_output<T: Task>() -> &'static str {
        std::any::type_name::<T::Output>()
    }
    // Named purely from the surface (associated types), never from the body.
    assert!(declared_input::<ThresholdGate>().contains("u32"));
    assert!(declared_output::<ThresholdGate>().contains("bool"));
}

/// **No-input task produces a value.** Running the work with a stub context and
/// no input returns the produced value; `Input = ()` needs no placeholder dance.
#[test]
fn no_input_task_produces_a_value() {
    let ctx = RunContext::for_test();
    let mut task = ProduceSeven;
    let out = block_on(task.run(&ctx, ())).expect("no-input task succeeds");
    assert_eq!(out, 7);
}

/// **Constructor-captured configuration is honored.** Two values of the same
/// task type with different captured thresholds each produce the output their
/// own configuration determines over the same input.
#[test]
fn constructor_captured_configuration_is_honored() {
    let ctx = RunContext::for_test();
    let mut low = ThresholdGate { threshold: 5 };
    let mut high = ThresholdGate { threshold: 50 };

    let low_out = block_on(low.run(&ctx, 10)).unwrap();
    let high_out = block_on(high.run(&ctx, 10)).unwrap();

    assert!(low_out, "10 >= threshold 5");
    assert!(!high_out, "10 < threshold 50");
}

/// **Same task type, several values.** Three independent values of one task
/// type, each with distinct configuration, are distinct units; constructing one
/// affects no other.
#[test]
fn same_task_type_several_independent_values() {
    let a = ThresholdGate { threshold: 1 };
    let b = ThresholdGate { threshold: 2 };
    let c = ThresholdGate { threshold: 3 };
    assert_eq!((a.threshold, b.threshold, c.threshold), (1, 2, 3));
}

/// **Exclusive `&mut self` work signature.** Invoking the work twice in sequence
/// against the same value: the second invocation observes the first's mutation.
/// (The shared-reference-invocation compile-fail is the mirror UI case.)
#[test]
fn mut_self_work_observes_prior_mutation() {
    let ctx = RunContext::for_test();
    let mut task = Counter { seen: 0 };
    let first = block_on(task.run(&ctx, ())).unwrap();
    let second = block_on(task.run(&ctx, ())).unwrap();
    assert_eq!((first, second), (1, 2));
}

/// **Error classification round-trips.** One error of each class is
/// distinguishable as its own class; a retry-eligible value is never observed as
/// permanent or skip, and vice versa.
#[test]
fn error_classification_round_trips() {
    let retry = TaskError::retryable("blip");
    let perm = TaskError::permanent("bad input");
    let skip = TaskError::skip("nothing to do");

    assert!(retry.is_retryable() && !retry.is_permanent() && !retry.is_skip());
    assert!(perm.is_permanent() && !perm.is_retryable() && !perm.is_skip());
    assert!(skip.is_skip() && !skip.is_retryable() && !skip.is_permanent());
}

/// **Error classification carries an underlying cause.** A retry-eligible error
/// constructed with a source error preserves that source (via `std::error::Error::source`).
#[test]
fn error_preserves_underlying_cause() {
    #[derive(Debug)]
    struct Underlying;
    impl std::fmt::Display for Underlying {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "the underlying cause")
        }
    }
    impl std::error::Error for Underlying {}

    let err = TaskError::retryable_from("io failed", Underlying);
    assert!(err.is_retryable());
    let source = std::error::Error::source(&err).expect("cause is preserved");
    assert_eq!(source.to_string(), "the underlying cause");
}

/// **Work returns a classified error.** A task arranged to fail returns the
/// error variant (not the output), and the returned error carries the intended
/// classification — one check per class.
#[test]
fn work_returns_a_classified_error() {
    let ctx = RunContext::for_test();

    let mut retry = AlwaysFails { mode: FailMode::Retryable };
    let mut perm = AlwaysFails { mode: FailMode::Permanent };
    let mut skip = AlwaysFails { mode: FailMode::Skip };

    let re = block_on(retry.run(&ctx, ())).expect_err("arranged to fail");
    let pe = block_on(perm.run(&ctx, ())).expect_err("arranged to fail");
    let se = block_on(skip.run(&ctx, ())).expect_err("arranged to fail");

    assert!(re.is_retryable());
    assert!(pe.is_permanent());
    assert!(se.is_skip());
}

/// **Execution class defaults to await-bound.** A task that states no class
/// reports await-bound; a task that states blocking (or compute) reports that.
#[test]
fn execution_class_defaults_to_await_bound() {
    assert_eq!(ThresholdGate::EXECUTION_CLASS, ExecutionClass::AwaitBound);
    assert_eq!(ProduceSeven::EXECUTION_CLASS, ExecutionClass::AwaitBound);
    assert_eq!(BlockingWork::EXECUTION_CLASS, ExecutionClass::Blocking);
    assert_eq!(ComputeWork::EXECUTION_CLASS, ExecutionClass::Compute);
}

/// **Re-runnability contract holds.** The unit is safely re-runnable in shape:
/// two invocations with equivalent input succeed and produce equivalent output.
/// (Retry itself is out of scope — C14; this checks only that the shape permits
/// it: `&mut self` sequential re-runs are sound.)
#[test]
fn work_is_safely_re_runnable_in_shape() {
    let ctx = RunContext::for_test();
    let mut task = ThresholdGate { threshold: 5 };
    let first = block_on(task.run(&ctx, 10)).unwrap();
    let second = block_on(task.run(&ctx, 10)).unwrap();
    assert_eq!(first, second);
}

/// The task/output type-level bounds hold for a well-formed task: the task value
/// is `Send + 'static` and its output is `Send + Sync + 'static`. These
/// `assert_*` helpers are the positive mirror of the compile-fail UI cases
/// (non-`Send` capture; missing-`Sync`/`'static` output).
#[test]
fn well_formed_task_and_output_satisfy_the_bounds() {
    fn assert_task_bounds<T: Task + Send + 'static>() {}
    fn assert_output_bounds<T: Send + Sync + 'static>() {}
    assert_task_bounds::<ThresholdGate>();
    assert_output_bounds::<<ThresholdGate as Task>::Output>();
    assert_output_bounds::<<ProduceSeven as Task>::Output>();
}

/// A task value carrying a genuinely shared, thread-safe resource remains
/// `Send`, and its work still runs — the shape the cookbook recommends over
/// capturing a non-`Send` value (which is the compile-fail UI case).
#[test]
fn task_may_capture_a_send_sync_shared_resource() {
    let shared = Arc::new(AtomicUsize::new(0));

    struct BumpShared {
        shared: Arc<AtomicUsize>,
    }
    impl Task for BumpShared {
        type Input = ();
        type Output = usize;
        async fn run(
            &mut self,
            _ctx: &RunContext,
            _input: Self::Input,
        ) -> Result<Self::Output, TaskError> {
            Ok(self.shared.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    fn assert_send<T: Send + 'static>(_: &T) {}
    let mut task = BumpShared { shared: shared.clone() };
    assert_send(&task);

    let ctx = RunContext::for_test();
    let n = block_on(task.run(&ctx, ())).unwrap();
    assert_eq!(n, 1);
    assert_eq!(shared.load(Ordering::SeqCst), 1);
}
