//! The C16 **OS-signal → cancellation wiring** (arch.md `### C16`; ticket T36).
//!
//! This is the OS-signal half of C16 that T35 deferred. T35 built the cancellation
//! *core* and exposed [`CancelHandle`] as the explicit
//! **signal seam** — the programmatic trigger a test fires and, in production, the
//! trigger an OS-signal handler fires. This module installs the handlers.
//!
//! # What it does
//!
//! [`install_signal_handlers`] registers handlers for the two orchestrator
//! termination signals — **`SIGTERM`** (the orchestrator's polite kill, e.g.
//! Kubernetes at the start of its `terminationGracePeriodSeconds`) and **`SIGINT`**
//! (an operator's Ctrl-C) — and wires **both** to fire the same
//! [`CancelHandle`]. The first delivery of either
//! signal starts the budgeted shutdown (the driver stops admitting new work, drains
//! in-flight cooperative work within grace, writes a complete + fsync'd stream, and
//! exits within the printed shutdown budget). The two signals are **observably
//! interchangeable** — both route to the identical cancellation path.
//!
//! # Re-entry hardening (a second signal does not shortcut the flush)
//!
//! A second identical signal *during* shutdown must not corrupt the shutdown path.
//! Both the underlying [`CancelHandle::cancel`]
//! (idempotent — first request wins the origin) and the routing here
//! ([`route_signal`]) are **re-entry hardened**: subsequent signals are counted
//! (observed, never dropped) but do **not** re-fire cancellation and do **not**
//! escalate to an immediate `process::exit` that would shortcut the final flush.
//! This is the arch.md/ticket contract — *"the first signal starts the budgeted
//! shutdown and subsequent ones do not shortcut the final flush"* — chosen over a
//! second-signal-forces-immediate-exit policy precisely because the shutdown budget
//! already bounds the wait (C16), so an escalation would only risk truncating the
//! stream the budget guarantees.
//!
//! # Isolation (C13 / T33)
//!
//! Signal reception runs on its **own** single-worker runtime owned by the returned
//! [`SignalGuard`], separate from every task-execution surface, so a saturated
//! task fleet cannot starve signal delivery (consistent with the T2 isolated
//! framework runtime). The handler does no work beyond firing the cheap, wait-free
//! `CancelHandle` — the real shutdown happens on the driver's framework runtime.
//!
//! # Platform posture (platform-conditional, T70)
//!
//! Unix delivers `SIGTERM`/`SIGINT` and this module installs real handlers via
//! `tokio::signal::unix`. On **non-unix** targets there are no POSIX termination
//! signals to wire; [`install_signal_handlers`] is a documented no-op returning a
//! guard, and the same cancellation is still reachable through the programmatic
//! [`CancelHandle`] seam. The end-to-end signal
//! coverage is therefore gated to unix (T70's platform matrix).

use crate::driver::CancelHandle;

/// The re-entry-hardened routing a delivered OS signal takes (arch.md C16; T36).
///
/// `count` is the running tally of signals delivered so far (each call increments
/// it). `fire` is the cancellation trigger — invoked **only on the first signal**;
/// every subsequent signal is observed (counted) but does **not** re-fire, so a
/// second signal during shutdown neither shortcuts the final flush nor duplicates
/// the cancellation. Factored out so the shortcut-hardening is unit-testable
/// without delivering a real OS signal to the test runner.
pub fn route_signal(count: &mut u32, fire: &mut dyn FnMut()) {
    *count += 1;
    if *count == 1 {
        fire();
    }
    // Subsequent signals: counted (observed) but idempotent — no re-fire, no
    // escalation to an immediate exit that would shortcut the bounded final flush.
}

/// The stateful router an installed OS-signal handler drives (arch.md C16; T36).
///
/// Holds the [`CancelHandle`] seam and the delivered-
/// signal count, applying the [`route_signal`] re-entry hardening. Exposed so the
/// signal→cancel *wiring* is exercised through the same seam the real handler uses,
/// deterministically, without raising a signal at the test runner.
#[derive(Debug)]
pub struct SignalRouter {
    handle: CancelHandle,
    count: std::sync::Mutex<u32>,
}

impl SignalRouter {
    /// A fresh router over `handle` (no signal delivered yet).
    #[must_use]
    pub fn new(handle: CancelHandle) -> Self {
        Self {
            handle,
            count: std::sync::Mutex::new(0),
        }
    }

    /// Handle one delivered signal: fire the cancel handle on the first, and treat
    /// every subsequent signal idempotently (re-entry hardened — no shortcut of the
    /// final flush).
    ///
    /// Poison-tolerant: a panic in a prior handler that poisoned the count is
    /// recovered from rather than propagated, so a signal is never dropped on the
    /// floor because of an unrelated panic.
    pub fn on_signal(&self) {
        let handle = self.handle.clone();
        let mut count = self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut fire = || handle.cancel();
        route_signal(&mut count, &mut fire);
    }

    /// Whether at least one signal has been routed (fired the cancel handle).
    /// Poison-tolerant (recovers a poisoned count rather than panicking).
    #[must_use]
    pub fn was_fired(&self) -> bool {
        *self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            > 0
    }
}

/// A live registration of the C16 OS-signal handlers (arch.md C16; T36).
///
/// Keep it alive for as long as the run should react to `SIGTERM`/`SIGINT`; drop it
/// to stop listening (the listener runtime is torn down without joining the driver,
/// so a late signal after the run ended is harmlessly ignored). Obtain one from
/// [`install_signal_handlers`].
#[derive(Debug)]
pub struct SignalGuard {
    #[cfg(unix)]
    _runtime: tokio::runtime::Runtime,
}

/// Install the C16 OS-signal handlers wiring `SIGTERM`/`SIGINT` to `handle`
/// (arch.md C16; T36).
///
/// Both signals fire the same [`CancelHandle`]; the
/// first delivery starts the budgeted shutdown and subsequent deliveries are
/// idempotent (re-entry hardened — see the [module docs](self)). Reception runs on
/// its own isolated single-worker runtime owned by the returned [`SignalGuard`], so
/// a saturated task fleet cannot starve signal delivery. Call this **before** the
/// drive and hold the guard for the run's lifetime.
///
/// # Errors
/// Returns an [`io::Error`](std::io::Error) if the handlers cannot be registered
/// (e.g. the runtime cannot be built, or the OS refuses the registration).
///
/// # Platform
/// Unix installs real handlers. On **non-unix** targets there are no POSIX
/// termination signals; this is a documented no-op returning a guard, and the same
/// cancellation stays reachable through the programmatic `CancelHandle` seam.
#[cfg(unix)]
pub fn install_signal_handlers(handle: CancelHandle) -> std::io::Result<SignalGuard> {
    use tokio::signal::unix::{signal, SignalKind};

    // A dedicated single-worker runtime for signal reception — isolated from every
    // task-execution surface (C13 / T2), so a jammed task fleet cannot starve it.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_io()
        .build()?;

    // Register the streams on the runtime (registration must happen inside it).
    // Each signal gets its own listener task both routing to the shared router; this
    // awaits BOTH signals without needing tokio's `select!` (and so without the
    // `macros` feature — the driver builds its runtimes by hand). Registration must
    // happen inside the runtime, so it is done in `block_on`; if it fails, the whole
    // install fails and the caller learns the handlers are not armed.
    let router = std::sync::Arc::new(SignalRouter::new(handle));
    runtime.block_on(async {
        // Create the streams inside the runtime and hand each to its own listener.
        let sigterm = signal(SignalKind::terminate())?;
        let sigint = signal(SignalKind::interrupt())?;
        spawn_listener(std::sync::Arc::clone(&router), sigterm);
        spawn_listener(std::sync::Arc::clone(&router), sigint);
        Ok::<(), std::io::Error>(())
    })?;

    Ok(SignalGuard { _runtime: runtime })
}

/// Spawn one listener task that routes every delivery of `stream` through the
/// shared re-entry-hardened `router`. Split out so each signal owns its stream for
/// the runtime's lifetime, and so both signals are awaited without `tokio::select!`
/// (no `macros` feature).
#[cfg(unix)]
fn spawn_listener(router: std::sync::Arc<SignalRouter>, mut stream: tokio::signal::unix::Signal) {
    tokio::spawn(async move {
        // `recv()` yields `None` only when the stream is torn down (runtime drop);
        // until then, every delivery routes through the shared router.
        while stream.recv().await.is_some() {
            router.on_signal();
        }
    });
}

/// Install the C16 OS-signal handlers — the **non-unix documented no-op**.
///
/// There are no POSIX termination signals to wire on this target; the same
/// cancellation stays reachable through the programmatic
/// [`CancelHandle`] seam. Returns a guard that owns
/// nothing.
///
/// # Errors
/// Never fails on non-unix (there is nothing to register); the `Result` is kept so
/// the signature matches the unix path.
#[cfg(not(unix))]
#[allow(
    clippy::unnecessary_wraps,
    reason = "signature parity with the unix path, which can fail to register"
)]
pub fn install_signal_handlers(_handle: CancelHandle) -> std::io::Result<SignalGuard> {
    Ok(SignalGuard {})
}
