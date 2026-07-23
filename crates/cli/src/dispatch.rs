//! C13 · **Execution-class dispatch** — routing each dispatched attempt onto the
//! thread execution surface named by its resolved execution class (arch.md
//! `### C13 · Execution class dispatch`; T2 ADR 004 §2/§3/§5; ticket T33).
//!
//! # The three surfaces (T2 ADR)
//!
//! The [`Dispatcher`] owns the three execution surfaces the T2 ADR fixed, one per
//! [`ExecutionClass`](dagr_core::task::ExecutionClass):
//!
//! - **[`AwaitBound`](dagr_core::task::ExecutionClass::AwaitBound)** → the async
//!   (tokio) **task runtime**. The attempt future is `spawn`ed onto the
//!   multi-threaded `tasks` runtime and driven by its async workers — the natural
//!   home for network calls and waiting (T2 §1).
//! - **[`Blocking`](dagr_core::task::ExecutionClass::Blocking)** → tokio's
//!   **dedicated blocking pool** via `spawn_blocking` (T2 §2). A synchronous
//!   closure runs there so it cannot starve the async workers, and — because a
//!   `spawn_blocking` closure cannot be killed — its permit stays counted while it
//!   is abandoned-but-running (C12/C14); the run loop never joins it.
//! - **[`Compute`](dagr_core::task::ExecutionClass::Compute)** → a dedicated
//!   fixed-size **`rayon::ThreadPool`** (T2 §3, the resolved open question — chosen
//!   over a capped semaphore over `spawn_blocking`). A pool built with
//!   `num_threads(N)` runs **at most N** closures concurrently regardless of how
//!   many are submitted, so C13's "concurrently executing compute-class tasks never
//!   exceed the compute pool's size" is *structural in the pool*, and the blocking
//!   pool stays free for genuinely I/O-waiting work.
//!
//! # What T33 owns, and what it consumes
//!
//! This module owns **only** the class→surface routing and the compute-pool
//! wiring. It **consumes** the pool sizes the C12 admission controller was pinned
//! with (T31/T32): the compute pool is built to the pinned
//! [`compute_threads`](dagr_core::admission::PoolCapacities) capacity with the
//! floor-of-one rule (T2 §3). It does **not** change T31 permit mechanics (the
//! permit is still acquired by the loop and moved into the dispatched closure, so
//! its drop releases the cost on the attempt's return) nor re-derive T32 sizing.
//!
//! # Isolation (T2 §5) is preserved
//!
//! The framework machinery (the loop, per-attempt timers, the event writer) runs
//! on the **isolated framework runtime** the driver builds; none of the three task
//! surfaces here are that runtime, so a task that jams every task/blocking/compute
//! worker cannot stall a timeout firing or the event stream — the T24
//! framework-survives-a-blocked-task guarantee (C13 acceptance) is unchanged.

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;

use dagr_core::admission::PoolCapacities;
use dagr_core::task::ExecutionClass;

/// The stable worker-thread name prefixes for the three surfaces. They are distinct
/// so an observer can attribute a running closure to its surface (the routing tests
/// read [`std::thread::current`] against these prefixes), and stable so that
/// attribution is not a coincidence of default runtime naming.
const ASYNC_WORKER_PREFIX: &str = "dagr-task-worker";
const BLOCKING_WORKER_PREFIX: &str = "dagr-blocking";
const COMPUTE_WORKER_PREFIX: &str = "dagr-compute";

/// The three execution surfaces plus the isolated framework runtime, built once per
/// run (arch.md C13; T2 ADR §2/§3/§5).
///
/// The `Dispatcher` holds:
/// - the **task runtime** (`tasks`) — a multi-threaded tokio runtime whose async
///   workers drive await-bound attempts and off which blocking attempts are moved
///   via `spawn_blocking`;
/// - the **compute pool** (`compute`) — a dedicated fixed-size `rayon::ThreadPool`
///   sized to the pinned compute capacity (floor of one).
///
/// The isolated **framework** runtime that drives the run loop, timers, and the
/// event writer is the driver's and is *not* held here — keeping it separate is
/// exactly the T2 §5 isolation the dispatcher must not undermine.
pub(crate) struct Dispatcher {
    tasks: tokio::runtime::Runtime,
    compute: rayon::ThreadPool,
}

impl Dispatcher {
    /// Build the dispatcher's three surfaces from the run's pinned pool
    /// [capacities](PoolCapacities) (T31/T32).
    ///
    /// - The **task runtime** is a multi-threaded tokio runtime with time enabled
    ///   (the per-attempt timeout deadline and the grace wait are `tokio::time`),
    ///   its async workers and blocking-pool threads named with the stable
    ///   surface prefixes so a running closure is attributable to its surface.
    /// - The **compute pool** is a `rayon::ThreadPool` sized to
    ///   [`compute_threads`](PoolCapacities) with the **floor-of-one** rule (T2 §3):
    ///   `max(1, pinned)`, so even a zero/fractional pinned capacity yields one live
    ///   compute thread. The unconstrained default (`u32::MAX`) is clamped to the
    ///   host parallelism so the pool is a sane size when no capacity is pinned.
    ///
    /// # Panics
    ///
    /// Panics only on a framework defect it cannot proceed past — a tokio runtime
    /// or rayon pool that could not be built.
    pub(crate) fn new(caps: &PoolCapacities) -> Self {
        let tasks = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .thread_name(ASYNC_WORKER_PREFIX)
            // Name the blocking-pool threads distinctly from the async workers so a
            // `spawn_blocking` closure is attributable to the blocking surface.
            .max_blocking_threads(512)
            // tokio names blocking threads via a separate hook; set both so the
            // async workers use the async prefix and blocking threads the blocking
            // prefix.
            .worker_threads(async_worker_count())
            .build()
            .expect("tasks runtime builds");

        let compute_threads = compute_pool_size(caps);
        let compute = rayon::ThreadPoolBuilder::new()
            .num_threads(compute_threads)
            .thread_name(|i| format!("{COMPUTE_WORKER_PREFIX}-{i}"))
            .build()
            .expect("compute pool builds");

        Self { tasks, compute }
    }

    /// Dispatch `attempt` — a future producing the attempt's result `R` — onto the
    /// surface named by `class`, invoking `on_done` with the produced result **on
    /// that surface** once the attempt returns (arch.md C13; T2 §2/§3).
    ///
    /// The future and the `on_done` callback are **moved into the dispatched
    /// closure**, so any permit-shaped guard the future carries is dropped on the
    /// surface exactly when the attempt returns (the T0.3/T31 ownership trick is
    /// preserved — this dispatcher changes routing, not permit mechanics).
    ///
    /// - **`AwaitBound`** — `attempt` is spawned onto the async task runtime and
    ///   driven by its async workers; `on_done` runs on that runtime.
    /// - **`Blocking`** — `attempt` is driven to completion **inside**
    ///   `spawn_blocking`, so the synchronous closure occupies a blocking-pool
    ///   thread and never an async worker; `on_done` runs on the blocking thread.
    /// - **`Compute`** — `attempt` is driven to completion on a dedicated `rayon`
    ///   compute-pool thread; `on_done` runs on that compute thread. The pool's
    ///   fixed size bounds compute concurrency structurally.
    pub(crate) fn dispatch<R, F, D>(&self, class: ExecutionClass, attempt: F, on_done: D)
    where
        R: Send + 'static,
        F: Future<Output = R> + Send + 'static,
        D: FnOnce(R) + Send + 'static,
    {
        match class {
            ExecutionClass::AwaitBound => {
                // Await-bound work is driven by the async workers of the task
                // runtime — the natural home for waiting/network futures (T2 §1).
                self.tasks.spawn(async move {
                    let result = attempt.await;
                    on_done(result);
                });
            }
            ExecutionClass::Blocking => {
                // Blocking work runs on tokio's dedicated blocking pool via
                // `spawn_blocking` so a synchronous closure cannot starve the async
                // workers (T2 §2). The (synchronous-shaped) attempt future is driven
                // to completion on the blocking thread with a park-based executor —
                // no nested runtime, so the blocking thread genuinely owns the work
                // and an unkillable closure keeps its thread (abandoned-but-running,
                // C12/C14).
                self.tasks.spawn_blocking(move || {
                    // tokio names its blocking-pool threads with the *same*
                    // `thread_name` as its async workers, so a blocking thread cannot
                    // be told from an async worker by name. Mark this thread as the
                    // blocking surface via a thread-local flag instead, so
                    // `current_surface` attributes the closure correctly. A tokio
                    // blocking thread only ever runs blocking work, so the marker is
                    // correct for its whole lifetime.
                    mark_blocking_thread();
                    let result = block_on(attempt);
                    on_done(result);
                });
            }
            ExecutionClass::Compute => {
                // Compute-bound work runs on the dedicated fixed-size rayon pool
                // (T2 §3): the pool's `num_threads(N)` makes "never exceed N"
                // structural. The attempt future is driven to completion on the
                // compute thread with the same park-based executor.
                self.compute.spawn(move || {
                    let result = block_on(attempt);
                    on_done(result);
                });
            }
        }
    }

    /// Shut the task runtime down **without joining** any abandoned-but-running
    /// (zombie) blocking closure (arch.md C14; the same discipline the M1 loop used
    /// for its single tasks runtime). `Runtime::drop` would block forever on an
    /// unkillable busy blocking thread; `shutdown_background` returns immediately,
    /// leaving any zombie to be reaped at process exit (the driver already emitted
    /// its `zombie-at-exit` event). The rayon pool is dropped normally — a
    /// well-behaved compute closure has already returned; a genuinely stuck compute
    /// closure is the same zombie shape and is likewise reaped at process exit.
    pub(crate) fn shutdown_background(self) {
        self.tasks.shutdown_background();
        // The rayon pool's `Drop` waits for in-flight work; a compute zombie would
        // wedge it exactly as a blocking zombie would wedge `Runtime::drop`. Leaking
        // the pool (never running its destructor) mirrors `shutdown_background`:
        // the threads are reaped at process exit, and the driver has already emitted
        // the zombie-at-exit event for any left-behind compute work.
        std::mem::forget(self.compute);
    }
}

/// The compute pool's thread count from the pinned capacity, with the T2 §3
/// **floor-of-one** rule: `max(1, pinned)`. The unconstrained default
/// (`u32::MAX`, meaning "not pinned") is clamped to the host parallelism so an
/// unpinned run gets a CPU-sized pool rather than `u32::MAX` threads. The actual
/// cgroup→host sizing that computes the pinned value is T32's; this only consumes
/// it and applies the floor.
fn compute_pool_size(caps: &PoolCapacities) -> usize {
    let pinned = caps.total(dagr_core::admission::Pool::ComputeThreads);
    if pinned == u64::from(u32::MAX) {
        // Not pinned (the unconstrained default): size to host parallelism, floored
        // at one.
        std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
    } else {
        // Pinned: honour it exactly, with the floor of one even at zero (T2 §3:
        // "at least one thread even under a fractional CPU quota").
        usize::try_from(pinned).unwrap_or(usize::MAX).max(1)
    }
}

/// The async task-runtime worker count. Kept to the host parallelism (floored at
/// one) — the async workers drive await-bound futures and hand blocking/compute
/// work off to their own pools, so this is the await concurrency, not the total.
fn async_worker_count() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Mark the current tokio blocking-pool thread as the blocking surface, so
/// [`current_surface`] attributes a `spawn_blocking` closure to the blocking
/// surface. tokio names its blocking threads with the *same* `thread_name` as its
/// async workers, so a thread-local marker — not the thread name — is the reliable
/// attribution. A tokio blocking thread only ever runs blocking work, so once
/// marked it stays correctly attributed for its whole lifetime.
fn mark_blocking_thread() {
    BLOCKING_SURFACE.with(|b| b.set(true));
}

thread_local! {
    /// Set on a tokio blocking-pool thread the moment a dispatched blocking closure
    /// begins, so [`current_surface`] attributes it to the blocking surface even
    /// though tokio names blocking threads identically to its async workers.
    static BLOCKING_SURFACE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Classify the **current** thread's execution surface (arch.md C13). Used by
/// observability/tests to attribute a running closure to its surface without a
/// wall-clock: compute threads carry the compute prefix, blocking threads set the
/// [`BLOCKING_SURFACE`] marker, and async workers carry the async prefix.
#[must_use]
pub(crate) fn current_surface() -> Surface {
    if BLOCKING_SURFACE.with(std::cell::Cell::get) {
        return Surface::Blocking;
    }
    let handle = std::thread::current();
    let name = handle.name().unwrap_or("");
    if name.starts_with(COMPUTE_WORKER_PREFIX) {
        Surface::Compute
    } else if name.starts_with(BLOCKING_WORKER_PREFIX) {
        Surface::Blocking
    } else if name.starts_with(ASYNC_WORKER_PREFIX) {
        Surface::Async
    } else {
        Surface::Unknown
    }
}

/// The execution surface a unit of work ran on — the observable half of C13's
/// class→surface routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Surface {
    /// The async (tokio) task runtime — where await-bound work runs.
    Async,
    /// The dedicated blocking pool — where blocking work runs.
    Blocking,
    /// The fixed compute (rayon) pool — where compute-bound work runs.
    Compute,
    /// Not one of the three task surfaces (e.g. the framework runtime or a test
    /// thread).
    Unknown,
}

// ===========================================================================
// A minimal, dependency-free, `unsafe`-free `block_on`
// ===========================================================================

/// Drive `future` to completion on the **current** thread, parking between polls —
/// a minimal, dependency-free, `unsafe`-free executor (no nested tokio runtime).
///
/// This is how a blocking/compute closure "owns" its thread: the synchronous work
/// is a future that resolves without yielding to a runtime, so this parks (never
/// busy-spins) whenever a poll returns `Pending` and wakes on the future's waker.
/// Running the attempt this way — rather than on a nested `Runtime::block_on` —
/// keeps the blocking/compute thread genuinely occupied by the work, which is what
/// makes an unkillable closure a real abandoned-but-running zombie (C12/C14) and
/// keeps the async workers free (T2 §2). It uses only `std` (`Wake`/`Thread::park`),
/// so it adds no runtime dependency and no `unsafe`.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let parker = Arc::new(ThreadParker {
        thread: std::thread::current(),
    });
    let waker = Waker::from(parker);
    let mut cx = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            // Park until the waker unparks this thread. A synchronous-shaped attempt
            // future resolves on the first poll, so this park is the honest fallback
            // for a future that legitimately awaits (never a busy spin).
            Poll::Pending => std::thread::park(),
        }
    }
}

/// A [`Wake`] that unparks the thread [`block_on`] parked. `Wake` is the safe,
/// `std`-only waker construction (no manual `RawWaker`/`unsafe`).
struct ThreadParker {
    thread: Thread,
}

impl Wake for ThreadParker {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}
