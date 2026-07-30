//! The **single-task test kit** — the first of the three testing levels.
//!
//! This is a **shipped** testing utility: downstream test code calls it to
//! exercise **exactly one** [`Task`] in isolation, with a hand-built
//! [`RunContext`] and fake resources — **no live network, no
//! database, and no scheduler**. Synchronous tasks run with **no async runtime
//! at all**; await-bound tasks run on a **plain test runtime the kit provides**,
//! so the caller never stands up a runtime of their own.
//!
//! It ships inside the library — behind the default-on `test-kit` feature — so
//! **no pipeline ever writes its own single-task harness**. It builds directly
//! on the hand-constructable [`RunContext`] and the
//! [`ResourceRegistry`] fake-substitution path, which is the exact seam the
//! full-pipeline harness reuses for the whole-run level — this kit
//! forecloses none of that.
//!
//! # What you configure, what you capture
//!
//! Build a test with [`SingleTaskTest::new`], configure only the fields a given
//! test cares about (ergonomic defaults fill the rest), then drive the task and
//! read the [`TaskOutcome`]:
//!
//! - **configure** — the fake [resource registry](SingleTaskTest::resources),
//!   opaque [parameters](SingleTaskTest::parameters), the
//!   [node identity](SingleTaskTest::node), the [attempt](SingleTaskTest::attempt)
//!   / [max attempts](SingleTaskTest::max_attempts) (so a retry-shaped test can
//!   drive attempt 2 by hand), an opaque [data interval](SingleTaskTest::data_interval),
//!   [pre-tripped cancellation](SingleTaskTest::cancelled), a
//!   [scratch root](SingleTaskTest::scratch_root), and the task
//!   [input](SingleTaskTest::input).
//! - **capture** — the classified [outcome](TaskOutcome) (produced
//!   [output](TaskOutcome::output) *or* the classified [error](TaskOutcome::error)),
//!   the observed [attempt number](TaskOutcome::attempt), the attempt's
//!   [metrics](TaskOutcome::metrics), the [scratch store](TaskOutcome::scratch),
//!   and a [framework-output dump](TaskOutcome::framework_output_dump) of
//!   the controlled context — the diagnostics the framework itself would render,
//!   used to prove a marked secret never leaks.
//!
//! # It runs one task with **no driver or scheduler**
//!
//! The kit awaits [`Task::run`] directly on a small, dependency-free, `unsafe`-free
//! executor built on [`std::task::Wake`] — never tokio, never dagr's run loop.
//! There is no admission, no readiness, no retry loop, no event stream: this is
//! the single-task level only. Two runs of the same task with the same inputs
//! behave identically — the kit reads **no** hidden wall-clock and threads
//! cancellation in explicitly.
//!
//! # Example
//!
//! ```
//! use dagr_core::context::{ResourceRegistry, ResourceRegistryBuilder};
//! use dagr_core::task::{RunContext, Task};
//! use dagr_core::test_kit::SingleTaskTest;
//! use dagr_core::TaskError;
//!
//! // A fake resource the task retrieves by type — no task change vs production.
//! #[derive(Clone, Default)]
//! struct FakeClient;
//! impl FakeClient {
//!     fn fetch(&self) -> u32 { 42 }
//! }
//!
//! // A synchronous task: no awaiting, so it needs no async runtime at all.
//! struct SyncReader;
//! impl Task for SyncReader {
//!     type Input = ();
//!     type Output = u32;
//!     async fn run(&mut self, ctx: &RunContext, _i: ()) -> Result<u32, TaskError> {
//!         let client = ctx
//!             .resources()
//!             .get::<FakeClient>()
//!             .ok_or_else(|| TaskError::permanent("no FakeClient registered"))?;
//!         Ok(client.fetch())
//!     }
//! }
//!
//! let registry = ResourceRegistry::builder()
//!     .register(FakeClient)
//!     .expect("unambiguous")
//!     .build();
//!
//! // Drive the sync task with no runtime; inject the fake; read the outcome.
//! let outcome = SingleTaskTest::new(SyncReader)
//!     .resources(registry)
//!     .node("read-node")
//!     .attempt(2) // retry-shaped: the task reads attempt 2
//!     .run_sync();
//!
//! assert!(outcome.is_success());
//! assert_eq!(outcome.output(), Some(&42));
//! assert_eq!(outcome.attempt(), 2);
//!
//! // An await-bound task uses only the runtime the kit provides.
//! struct AwaitReader;
//! impl Task for AwaitReader {
//!     type Input = ();
//!     type Output = u32;
//!     async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<u32, TaskError> {
//!         Ok(7)
//!     }
//! }
//! let out = SingleTaskTest::new(AwaitReader).run_await();
//! assert_eq!(out.output(), Some(&7));
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::context::{
    CancellationSignal, CancellationSource, DataInterval, PipelineId, ResourceRegistry, RunContext,
    RunId, ScratchStore,
};
use crate::error::TaskError;
use crate::handle::NodeId;
use crate::metrics::AttemptMetrics;
use crate::task::Task;

/// A **private, per-call temp directory**, removed with everything beneath it when
/// the value drops.
///
/// This is the run-store base a test hands to a run. It exists because the run
/// store namespaces by `<base>/<pipeline>/<run-id>`, which makes a *shared* literal
/// base — `/tmp/dagr-test` and friends — collision-free only for as long as no two
/// tests happen to pick the same pipeline name. That is an implicit invariant with
/// nothing enforcing it, and this repository has already paid for exactly that
/// class of flake once. A `TempBase` makes the isolation **structural**: the path
/// itself is unique, so two tests cannot share one however they are named.
///
/// # What makes a path unique
///
/// Three components, each closing a different collision:
///
/// - the **process id** — two concurrent test *processes* (nextest runs each test
///   in its own) cannot collide, nor can a test run alongside a live dagr process;
/// - a **process-monotonic counter** — two calls on any thread of one process
///   differ, including two calls in the same test;
/// - a **nanosecond stamp** — a *later* run of the same suite cannot land on a
///   directory a previous run left behind if the pid is recycled.
///
/// # Cleanup
///
/// [`Drop`] removes the whole subtree, so a repeated local run does not accumulate
/// stale directories. Removal is best-effort by design: a destructor cannot report
/// a failure, and a test that has already panicked must not be turned into a
/// second, less informative failure by its own cleanup.
///
/// # Example
///
/// ```
/// use dagr_core::test_kit::TempBase;
///
/// let base = TempBase::new("my-test");
/// assert!(base.path().is_dir());
/// let path = base.path().to_path_buf();
/// drop(base);
/// assert!(!path.exists());
/// ```
#[derive(Debug)]
pub struct TempBase {
    path: PathBuf,
}

impl TempBase {
    /// Create a private temp directory tagged with `tag`, which appears in the
    /// path so a leftover directory names the test that made it.
    ///
    /// # Panics
    ///
    /// Panics if the directory cannot be created — a test that cannot obtain an
    /// isolated base has nothing meaningful left to assert.
    #[must_use]
    pub fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let unique = format!(
            "dagr-{tag}-{}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            nanos,
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&path)
            .unwrap_or_else(|e| panic!("create private temp base {}: {e}", path.display()));
        Self { path }
    }

    /// The base directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The base directory as a string, for the APIs that take one (the run
    /// store's base is a `String`).
    ///
    /// # Panics
    ///
    /// Panics if the platform temp directory is not valid UTF-8, which no
    /// supported platform (arch.md "Platform support": Linux and macOS) produces.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.path
            .to_str()
            .unwrap_or_else(|| panic!("temp base path is not valid UTF-8: {:?}", self.path))
    }
}

impl AsRef<Path> for TempBase {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBase {
    fn drop(&mut self) {
        // Best-effort: see the type docs. A cleanup failure must not mask the
        // assertion failure that is the actual result of the test.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A configured single-task invocation.
///
/// Construct with [`new`](Self::new), configure only what a test needs (every
/// other field takes an ergonomic, spec-consistent default), then drive the task
/// with [`run_sync`](Self::run_sync) (synchronous — **no runtime**) or
/// [`run_await`](Self::run_await) (await-bound — the kit's **provided runtime**).
/// See the [module docs](self) for the full contract and a worked example.
///
/// `T` is the task under test; a task whose input is not `()` supplies it with
/// [`input`](Self::input).
pub struct SingleTaskTest<T: Task> {
    task: T,
    input: Option<T::Input>,
    run: RunId,
    pipeline: PipelineId,
    node_name: String,
    attempt: u32,
    max_attempts: u32,
    parameters: Option<Arc<dyn std::any::Any + Send + Sync>>,
    data_interval: Option<DataInterval>,
    cancellation: CancellationSource,
    pre_cancelled: bool,
    resources: ResourceRegistry,
    scratch_root: Option<std::path::PathBuf>,
}

// Hand-written `Debug`: the kit owns the task under test and a registry of erased
// fakes, neither of which is `Debug`, so a derive would bound `T: Debug` and make
// the kit unprintable for exactly the tasks it exists to exercise. What a failing
// test needs is the context it was configured with; the task and the erased
// registry are omitted with `finish_non_exhaustive()`, the same precedent
// `Permit` / `ResidencyLease` / the slot types use.
impl<T: Task> std::fmt::Debug for SingleTaskTest<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleTaskTest")
            .field("run", &self.run)
            .field("pipeline", &self.pipeline)
            .field("node_name", &self.node_name)
            .field("attempt", &self.attempt)
            .field("max_attempts", &self.max_attempts)
            .field("pre_cancelled", &self.pre_cancelled)
            .field("scratch_root", &self.scratch_root)
            .finish_non_exhaustive()
    }
}

impl<T: Task> SingleTaskTest<T> {
    /// Begin configuring a single-task test for `task`. All context fields take
    /// spec-consistent defaults (a recognizable run/pipeline/node identity,
    /// attempt 1 of 1, no parameters, no data interval, a fresh uncancelled
    /// signal, the honest-empty registry and scratch seams); set only what the
    /// test cares about before driving the task.
    #[must_use]
    pub fn new(task: T) -> Self {
        Self {
            task,
            input: None,
            run: RunId::new("test-run"),
            pipeline: PipelineId::new("test-pipeline"),
            node_name: "test-node".to_string(),
            attempt: 1,
            max_attempts: 1,
            parameters: None,
            data_interval: None,
            cancellation: CancellationSource::new(),
            pre_cancelled: false,
            resources: ResourceRegistry::default(),
            scratch_root: None,
        }
    }

    /// Supply the task's declared input value. A task whose `Input` is `()` need
    /// not call this — the unit input is the default.
    #[must_use]
    pub fn input(mut self, input: T::Input) -> Self {
        self.input = Some(input);
        self
    }

    /// Set the run identity the task observes (default `"test-run"`).
    #[must_use]
    pub fn run_id(mut self, run: impl Into<String>) -> Self {
        self.run = RunId::new(run);
        self
    }

    /// Set the pipeline identity the task observes (default `"test-pipeline"`).
    #[must_use]
    pub fn pipeline_id(mut self, pipeline: impl Into<String>) -> Self {
        self.pipeline = PipelineId::new(pipeline);
        self
    }

    /// Set the node identity name the task observes (default `"test-node"`). The
    /// [`NodeId`] is derived from this name exactly as a registration would.
    #[must_use]
    pub fn node(mut self, name: impl Into<String>) -> Self {
        self.node_name = name.into();
        self
    }

    /// Set the current attempt number the task reads (default 1). Setting this to
    /// `2` is how a retry-shaped single-task test drives a specific attempt.
    #[must_use]
    pub fn attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    /// Set the configured maximum number of attempts (default 1).
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Supply the run's parameters, carried opaquely and read back by type via
    /// [`RunContext::parameters`]. The value must be `Send + Sync + 'static`.
    #[must_use]
    pub fn parameters<P: std::any::Any + Send + Sync>(mut self, parameters: P) -> Self {
        self.parameters = Some(Arc::new(parameters));
        self
    }

    /// Supply the run's opaque [data interval](DataInterval), returned to the task
    /// **exactly** as supplied — the kit interprets nothing (the opaque-interval
    /// invariant). Omit it for a run with no interval (the default).
    #[must_use]
    pub fn data_interval(mut self, interval: DataInterval) -> Self {
        self.data_interval = Some(interval);
        self
    }

    /// Supply a fake [resource registry](ResourceRegistry) — built from
    /// fakes via [`ResourceRegistry::builder`] — that the task retrieves resources
    /// from **with no change to the task's own code** versus production. Omit it
    /// for the honest-empty registry (the default).
    #[must_use]
    pub fn resources(mut self, resources: ResourceRegistry) -> Self {
        self.resources = resources;
        self
    }

    /// **Pre-trip** the cancellation signal, so the task observes the run as
    /// already cancelled. Without this the signal is fresh and un-tripped (the
    /// default), so a task that checks it sees not-cancelled.
    #[must_use]
    pub fn cancelled(mut self) -> Self {
        self.pre_cancelled = true;
        self
    }

    /// Supply a run-store base under which the task's node reaches a **wired**
    /// [durable scratch store](ScratchStore), so a test can drive and then
    /// inspect scratch. Omit it for the honestly-unwired store (the default),
    /// which never pretends to persist.
    #[must_use]
    pub fn scratch_root(mut self, base: std::path::PathBuf) -> Self {
        self.scratch_root = Some(base);
        self
    }

    /// Build the controlled [`RunContext`] and the [`CancellationSignal`] the
    /// task will see (tripping it first if [`cancelled`](Self::cancelled) was set).
    fn build_context(&self) -> RunContext {
        if self.pre_cancelled {
            self.cancellation.cancel();
        }
        let node = NodeId::from_name(&self.node_name);
        let mut builder = RunContext::builder(self.run.clone(), self.pipeline.clone(), node)
            .attempt(self.attempt)
            .max_attempts(self.max_attempts)
            .cancellation(self.cancellation.signal())
            .resources(self.resources.clone());
        if let Some(params) = &self.parameters {
            builder = builder.parameters(Arc::clone(params));
        }
        if let Some(interval) = &self.data_interval {
            builder = builder.data_interval(interval.clone());
        }
        if let Some(base) = &self.scratch_root {
            builder = builder.scratch_root(base.clone());
        }
        builder.build()
    }

    /// Drive a **synchronous** task's work to completion with **no async runtime
    /// present at all** — a synchronous task needs no async runtime.
    ///
    /// The task's future is polled to completion on a minimal, dependency-free
    /// executor; a synchronous task body returns on the first poll. The returned
    /// [`TaskOutcome`] exposes the produced output or the classified error, the
    /// observed attempt, and the captured metrics/scratch/diagnostics.
    ///
    /// This and [`run_await`](Self::run_await) share the same executor and drive
    /// the task through the **same** [`Task::run`] path; the distinction is only
    /// which entry point a test names to document its task class.
    #[must_use]
    pub fn run_sync(mut self) -> TaskOutcome<T::Output> {
        let ctx = self.build_context();
        let input = self.input.take().unwrap_or_else(default_unit_input);
        let result = block_on(self.task.run(&ctx, input));
        TaskOutcome::from_result(result, &ctx)
    }

    /// Drive an **await-bound** task using the **plain test runtime the kit
    /// provides** — an await-bound task uses a plain test runtime the
    /// surface provides, and the caller stands up **no** runtime of their own.
    ///
    /// The task's future is driven to completion on the kit's small
    /// waker-backed executor, which correctly suspends and resumes a task that
    /// actually awaits. The returned [`TaskOutcome`] is identical in shape to
    /// [`run_sync`](Self::run_sync)'s.
    #[must_use]
    pub fn run_await(mut self) -> TaskOutcome<T::Output> {
        let ctx = self.build_context();
        let input = self.input.take().unwrap_or_else(default_unit_input);
        let result = block_on(self.task.run(&ctx, input));
        TaskOutcome::from_result(result, &ctx)
    }
}

/// The captured result of driving one task through the kit: the classified
/// outcome plus the observations a test asserts on.
///
/// Read the produced value with [`output`](Self::output) / [`into_output`](Self::into_output)
/// or the classified failure with [`error`](Self::error); the two are mutually
/// exclusive ([`is_success`](Self::is_success) discriminates). The observed
/// [`attempt`](Self::attempt), the attempt [`metrics`](Self::metrics), the
/// [`scratch`](Self::scratch) store, and a [`framework_output_dump`](Self::framework_output_dump)
/// of the controlled context round out the capture surface.
#[derive(Debug)]
pub struct TaskOutcome<O> {
    result: Result<O, TaskError>,
    attempt: u32,
    metrics: AttemptMetrics,
    scratch: ScratchStore,
    cancellation: CancellationSignal,
    framework_output_dump: String,
}

impl<O> TaskOutcome<O> {
    /// Capture the outcome of one attempt alongside the observations drawn from
    /// the controlled context.
    fn from_result(result: Result<O, TaskError>, ctx: &RunContext) -> Self {
        // The framework-output dump renders the diagnostics the framework itself
        // controls for this attempt — the context's own `Debug` (which never
        // renders resource *values*, so a marked secret cannot leak through it),
        // its span identity, and the registry summary. This is the surface the
        // planted-sentinel test inspects.
        let framework_output_dump = format!(
            "context={ctx:?} span_run={} span_node={:?} span_attempt={} resources={:?}",
            ctx.span().run_id().as_str(),
            ctx.span().node_id(),
            ctx.span().attempt(),
            ctx.resources(),
        );
        Self {
            result,
            attempt: ctx.attempt(),
            metrics: AttemptMetrics::new(),
            scratch: ctx.scratch().clone(),
            cancellation: ctx.cancellation().clone(),
            framework_output_dump,
        }
    }

    /// Whether the task **succeeded** — it returned a value rather than a
    /// classified error.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.result.is_ok()
    }

    /// The produced output, or [`None`] if the task returned a classified error.
    #[must_use]
    pub fn output(&self) -> Option<&O> {
        self.result.as_ref().ok()
    }

    /// Consume the outcome and take ownership of the produced output, or [`None`]
    /// on a classified error.
    #[must_use]
    pub fn into_output(self) -> Option<O> {
        self.result.ok()
    }

    /// The classified [error](TaskError) the task returned, or [`None`] on
    /// success. The classification (retry-eligible / permanent / skip) is readable
    /// via the error's own predicates so a test can assert on it.
    #[must_use]
    pub fn error(&self) -> Option<&TaskError> {
        self.result.as_ref().err()
    }

    /// The attempt number the task observed — exactly the one the kit was
    /// configured with (default 1), so a retry-shaped assertion can confirm the
    /// task ran under the intended attempt.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The attempt's captured [metrics](AttemptMetrics). A plain
    /// single-task run attaches none by default; a test can still read the set
    /// (empty task metrics, no fabricated framework metrics) and assert on it.
    #[must_use]
    pub fn metrics(&self) -> &AttemptMetrics {
        &self.metrics
    }

    /// The node's [durable scratch store](ScratchStore) as the task saw it,
    /// so a test can inspect what the task wrote (when a
    /// [scratch root](SingleTaskTest::scratch_root) was supplied) or confirm the
    /// honest-empty seam (when it was not).
    #[must_use]
    pub fn scratch(&self) -> &ScratchStore {
        &self.scratch
    }

    /// The [cancellation signal](CancellationSignal) the task observed, so a test
    /// can confirm the tripped/un-tripped state the context carried.
    #[must_use]
    pub fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    /// A rendered dump of the **framework-controlled** diagnostics for this
    /// attempt — the context's own `Debug`, its span identity, and the registry
    /// summary — the surface a planted-sentinel test asserts a marked secret
    /// never appears in. The kit renders resources only through the paths the
    /// framework controls (which never render a [`Secret`](crate::context::Secret)'s
    /// bytes), so this dump opens no new leak.
    #[must_use]
    pub fn framework_output_dump(&self) -> &str {
        &self.framework_output_dump
    }
}

/// Produce the default task input for a task whose declared `Input` is `()`.
///
/// The single-task kit's ergonomic default is the unit input; a task with a
/// non-unit input must supply one with [`SingleTaskTest::input`], and calling a
/// runner without doing so is a test-authoring error surfaced by this panic
/// (never reached for the common `Input = ()` case).
fn default_unit_input<I: 'static>() -> I {
    // The overwhelmingly common case is `Input = ()`. For that type this returns
    // the unit value; for any other type it panics with a directive to supply the
    // input, which is a clear test-authoring error rather than a silent default.
    let unit: Box<dyn std::any::Any> = Box::new(());
    unit.downcast::<I>().map_or_else(
        |_| {
            panic!(
                "SingleTaskTest: this task's Input is not `()`; supply it with `.input(..)` \
                 before running"
            )
        },
        |boxed| *boxed,
    )
}

/// A minimal, dependency-free, `unsafe`-free block-on — the **provided test
/// runtime** the kit drives a task on.
///
/// Built on the safe [`std::task::Wake`] trait rather than any async runtime, so
/// the kit adds **no** dependency and proves the core is runtime-agnostic: a
/// synchronous task returns on the first poll; an await-bound task that suspends
/// is re-polled when it wakes. It parks the thread between polls (rather than
/// busy-spinning), which is correct for the cooperative single-task futures the
/// kit drives.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread::{self, Thread};

    // A waker that unparks the driving thread when the future signals readiness,
    // and records that a wake happened so a wake landing before `park` does not
    // lose the notification.
    struct ThreadWaker {
        thread: Thread,
        awoken: AtomicBool,
    }
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.awoken.store(true, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    let inner = Arc::new(ThreadWaker {
        thread: thread::current(),
        awoken: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&inner));
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
        // Park until a wake arrives. If a wake already landed (the flag is set),
        // consume it and re-poll immediately rather than parking — this closes the
        // wake-before-park race without busy-spinning.
        while !inner.awoken.swap(false, Ordering::SeqCst) {
            thread::park();
        }
    }
}
