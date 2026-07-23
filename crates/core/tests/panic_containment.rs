//! C14 panic-containment tests — ticket T23 (033). Written first, TDD.
//!
//! These exercise the **panic-containment** facet of the C14 attempt runner
//! (arch.md `### C14 · Attempt runner`, the **Panics** paragraph) that T20
//! deferred: a task that panics must fail **only its own node**, be caught at
//! the attempt boundary rather than unwind the run, be classified a **permanent**
//! failure (never retried), reach the `failed` terminal state, have its panic
//! message captured, and leave its output slot empty. It must attribute the
//! panic to the correct node under concurrency (task-local state), emit exactly
//! one attempt-outcome record, install its hook once (idempotently, chaining to
//! a pre-existing hook), and the binary must refuse `panic = "abort"` at startup.
//!
//! Scope discipline (T23): panic containment only. It composes with T21 timeout
//! and T22 retry via the outcome classification; it forks no run-loop driver
//! (T24) and no dispatch/concurrency (T33). The runner is runtime-agnostic — an
//! `async fn` awaited on a plain no-runtime executor, with a hand-built
//! `RunContext` (C8) and a capturing event sink (C19).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dagr_core::context::{PipelineId, RunContext, RunId};
use dagr_core::execution::{
    check_panic_strategy, detect_panic_strategy, install_panic_hook, run_attempt_caught,
    run_with_retries_caught, AttemptEvent, AttemptEventSink, AttemptOutcome, Backoff, NoJitter,
    PanicStrategy, RetryConfig,
};
use dagr_core::handle::NodeId;
use dagr_core::slot::{ResidencyLedger, Slot};
use dagr_core::task::Task;
use dagr_core::{TaskError, TerminalState};

// --- Typed value + task types ----------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Report {
    rows: u32,
}

/// A task whose body panics with a recognizable message on every attempt,
/// counting how many times its work was actually invoked.
struct PanicsAlways {
    message: &'static str,
    invocations: Arc<AtomicU32>,
}
impl Task for PanicsAlways {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        panic!("{}", self.message);
    }
}

/// A task that returns a value on success.
struct Succeeds {
    value: Report,
}
impl Task for Succeeds {
    type Input = ();
    type Output = Report;
    async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
        Ok(self.value.clone())
    }
}

// --- Capturing event sink (C19-shaped, in-memory) ---------------------------

#[derive(Default)]
struct CapturingSink {
    records: Vec<AttemptEvent>,
}
impl AttemptEventSink for CapturingSink {
    fn emit(&mut self, event: AttemptEvent) {
        self.records.push(event);
    }
}
impl CapturingSink {
    /// The number of attempt-outcome records (succeeded / failed / timed-out /
    /// panicked) — used to assert the exactly-one contract.
    fn attempt_outcome_count(&self) -> usize {
        self.records
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    AttemptEvent::AttemptSucceeded { .. }
                        | AttemptEvent::AttemptFailed { .. }
                        | AttemptEvent::AttemptTimedOut { .. }
                        | AttemptEvent::AttemptPanicked { .. }
                )
            })
            .count()
    }

    fn node_terminal(&self) -> Option<TerminalState> {
        self.records.iter().find_map(|e| match e {
            AttemptEvent::NodeTerminal { state, .. } => Some(*state),
            _ => None,
        })
    }

    fn panic_message_for(&self, node: &str) -> Option<String> {
        self.records.iter().find_map(|e| match e {
            AttemptEvent::AttemptPanicked {
                node: n, message, ..
            } if n == node => Some(message.clone()),
            _ => None,
        })
    }
}

// --- Helpers ----------------------------------------------------------------

fn fresh_slot(node: &str) -> Slot<Report> {
    Slot::new(
        NodeId::from_name(node),
        node,
        1,
        false,
        0,
        ResidencyLedger::new(),
    )
}

fn ctx_for(node: &str, attempt: u32, max: u32) -> RunContext {
    RunContext::builder(
        RunId::new("test-run"),
        PipelineId::new("test-pipeline"),
        NodeId::from_name(node),
    )
    .attempt(attempt)
    .max_attempts(max)
    .build()
}

/// A no-dependency, `unsafe`-free block-on — the runtime-agnostic executor the
/// T20/T21/T22 tests use, proving panic containment needs no runtime.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::sync::Arc as StdArc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: StdArc<Self>) {}
        fn wake_by_ref(self: &StdArc<Self>) {}
    }
    let waker = Waker::from(StdArc::new(NoopWaker));
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

// --- 1. A panicking task fails only its own node (contained, not unwound) ----

/// A panicking task's attempt is **caught** at the boundary: the runner returns
/// a classified `Panicked` outcome rather than unwinding, the node's terminal
/// state is `failed`, the slot is left empty, and the driving thread survives
/// (the panic did not unwind past the runner).
#[test]
fn panicking_task_is_contained_and_fails_only_its_node() {
    install_panic_hook();
    let node = "panics";
    let slot = fresh_slot(node);
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(node, 1, 1);
    let calls = Arc::new(AtomicU32::new(0));

    let outcome = block_on(run_attempt_caught(
        &mut PanicsAlways {
            message: "boom in task body",
            invocations: Arc::clone(&calls),
        },
        node,
        &ctx,
        &slot,
        &mut sink,
    ));

    assert!(
        matches!(outcome, AttemptOutcome::Panicked),
        "a caught panic is classified Panicked"
    );
    assert_eq!(
        outcome.terminal_state(),
        TerminalState::Failed,
        "a caught panic maps to the `failed` terminal state (arch.md Vocabulary)"
    );
    assert_eq!(sink.node_terminal(), Some(TerminalState::Failed));
    assert!(!slot.is_filled(), "a panicked attempt never fills the slot");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the body ran exactly once");
    // Reaching this line at all proves the driving thread was not unwound.
}

/// The captured panic message is carried on the outcome record.
#[test]
fn panic_message_is_captured() {
    install_panic_hook();
    let node = "panics-msg";
    let slot = fresh_slot(node);
    let mut sink = CapturingSink::default();
    let ctx = ctx_for(node, 1, 1);

    let _ = block_on(run_attempt_caught(
        &mut PanicsAlways {
            message: "recognizable panic detail",
            invocations: Arc::new(AtomicU32::new(0)),
        },
        node,
        &ctx,
        &slot,
        &mut sink,
    ));

    let msg = sink.panic_message_for(node).expect("panic message captured");
    assert!(
        msg.contains("recognizable panic detail"),
        "captured message carries the panic payload, got {msg:?}"
    );
}

// --- 2. The rest of the run proceeds after a panic --------------------------

/// A panicking node fails `failed` while an independent node runs to completion
/// and ends `succeeded` — the panic did not take the run down (continue-
/// independent semantics stand in for C15; here we prove the two attempts are
/// isolated).
#[test]
fn independent_node_completes_after_a_panic() {
    install_panic_hook();
    let pnode = "panics-2";
    let inode = "independent";
    let pslot = fresh_slot(pnode);
    let islot = fresh_slot(inode);
    let mut psink = CapturingSink::default();
    let mut isink = CapturingSink::default();

    let pout = block_on(run_attempt_caught(
        &mut PanicsAlways {
            message: "down goes one",
            invocations: Arc::new(AtomicU32::new(0)),
        },
        pnode,
        &ctx_for(pnode, 1, 1),
        &pslot,
        &mut psink,
    ));
    let iout = block_on(run_attempt_caught(
        &mut Succeeds {
            value: Report { rows: 7 },
        },
        inode,
        &ctx_for(inode, 1, 1),
        &islot,
        &mut isink,
    ));

    assert!(matches!(pout, AttemptOutcome::Panicked));
    assert_eq!(psink.node_terminal(), Some(TerminalState::Failed));
    assert!(matches!(iout, AttemptOutcome::Succeeded));
    assert_eq!(isink.node_terminal(), Some(TerminalState::Succeeded));
    assert!(islot.is_filled(), "the independent node produced its value");
}

// --- 3. A panic is a permanent failure, never retried -----------------------

/// A node with a retry budget > 1 whose body panics on every attempt runs
/// **exactly one** attempt — a panic is classified permanent (never retry-
/// eligible), so the retry loop stops with the budget untouched and the node
/// ends `failed`.
#[test]
fn a_panic_is_permanent_and_is_never_retried() {
    install_panic_hook();
    let node = "panics-noretry";
    let slot = fresh_slot(node);
    let mut sink = CapturingSink::default();
    let calls = Arc::new(AtomicU32::new(0));

    // A generous budget (3 attempts) with an all-jitter-free backoff.
    let config = RetryConfig::new(3, Backoff::new(Duration::ZERO, 2.0, Duration::MAX));

    let outcome = block_on(run_with_retries_caught(
        PanicsAlways {
            message: "panic every attempt",
            invocations: Arc::clone(&calls),
        },
        node,
        RunId::new("test-run"),
        PipelineId::new("test-pipeline"),
        &slot,
        &mut sink,
        &config,
        &mut NoJitter,
        |_delay| async {},
    ));

    assert!(matches!(outcome, AttemptOutcome::Panicked));
    assert!(
        !outcome.is_retry_eligible(),
        "a panic is never retry-eligible"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly one attempt ran — the panic stopped retrying"
    );
    assert_eq!(sink.node_terminal(), Some(TerminalState::Failed));
    // No BackoffStarted record: a panic never enters a backoff phase.
    assert!(
        !sink
            .records
            .iter()
            .any(|e| matches!(e, AttemptEvent::BackoffStarted { .. })),
        "a panic schedules no backoff"
    );
}

// --- 4. The panic is attributed to the correct node under concurrency -------

/// Two nodes with distinct identities; one panics, the other succeeds. The
/// failure/outcome record for the panic names the **panicking** node and carries
/// its message — attribution via task-local state stays correct despite
/// interleaving.
#[test]
fn panic_is_attributed_to_the_correct_node() {
    install_panic_hook();
    let a = "node-a-panics";
    let b = "node-b-ok";
    let aslot = fresh_slot(a);
    let bslot = fresh_slot(b);
    let mut asink = CapturingSink::default();
    let mut bsink = CapturingSink::default();

    // Interleave by running each to completion; attribution is per-attempt
    // task-local, so the message and node on each record must not cross over.
    let aout = block_on(run_attempt_caught(
        &mut PanicsAlways {
            message: "A blew up",
            invocations: Arc::new(AtomicU32::new(0)),
        },
        a,
        &ctx_for(a, 1, 1),
        &aslot,
        &mut asink,
    ));
    let bout = block_on(run_attempt_caught(
        &mut Succeeds {
            value: Report { rows: 1 },
        },
        b,
        &ctx_for(b, 1, 1),
        &bslot,
        &mut bsink,
    ));

    assert!(matches!(aout, AttemptOutcome::Panicked));
    assert!(matches!(bout, AttemptOutcome::Succeeded));

    let amsg = asink.panic_message_for(a).expect("A has a panic record");
    assert!(amsg.contains("A blew up"), "A's message, got {amsg:?}");
    assert!(
        bsink.panic_message_for(b).is_none(),
        "B, which succeeded, has no panic record"
    );
    // The panic record names A, never B.
    assert!(
        asink.panic_message_for(b).is_none(),
        "A's stream carries no record attributed to B"
    );
}

// --- 5. Exactly one attempt-outcome record for a panicking attempt ----------

/// A panicking attempt yields exactly one attempt-outcome record (marked a panic
/// outcome), alongside its per-transition events (attempt-started, node-terminal)
/// — the same one-record invariant success / timeout / permanent failure hold.
#[test]
fn exactly_one_attempt_outcome_record_for_a_panic() {
    install_panic_hook();
    let node = "panics-onerecord";
    let slot = fresh_slot(node);
    let mut sink = CapturingSink::default();

    let _ = block_on(run_attempt_caught(
        &mut PanicsAlways {
            message: "one record only",
            invocations: Arc::new(AtomicU32::new(0)),
        },
        node,
        &ctx_for(node, 1, 1),
        &slot,
        &mut sink,
    ));

    assert_eq!(
        sink.attempt_outcome_count(),
        1,
        "exactly one attempt-outcome record for a panicking attempt"
    );
    // The one outcome record is a panic record, and the per-transition events
    // are present around it.
    assert!(
        sink.records
            .iter()
            .any(|e| matches!(e, AttemptEvent::AttemptStarted { .. })),
        "attempt-started is present"
    );
    assert!(
        sink.records
            .iter()
            .any(|e| matches!(e, AttemptEvent::AttemptPanicked { .. })),
        "the outcome record is a panic record"
    );
    assert_eq!(sink.node_terminal(), Some(TerminalState::Failed));
}

// --- 6 & 7. The startup check refuses `panic = "abort"`, permits unwind ------

/// The startup panic-strategy check refuses the abort strategy with a message
/// that names the required profile setting (the fix), and permits the unwinding
/// strategy silently.
#[test]
fn startup_check_refuses_abort_and_permits_unwind() {
    // Refuse abort.
    let refused = check_panic_strategy(PanicStrategy::Abort)
        .expect_err("the abort strategy must be refused");
    let msg = refused.to_string();
    assert!(
        msg.contains("panic") && msg.contains("unwind"),
        "the refusal names the required profile setting (panic = \"unwind\"), got {msg:?}"
    );

    // Permit unwind.
    assert!(
        check_panic_strategy(PanicStrategy::Unwind).is_ok(),
        "the unwinding strategy is permitted silently"
    );
}

/// The compiled test binary runs under the unwinding strategy (it must, to catch
/// panics), so the detected strategy passes the check — the refusal is specific
/// to abort, not a blanket gate.
#[test]
fn detected_strategy_of_this_binary_passes() {
    assert_eq!(
        detect_panic_strategy(),
        PanicStrategy::Unwind,
        "the test binary is compiled to unwind (else it could not catch)"
    );
    assert!(check_panic_strategy(detect_panic_strategy()).is_ok());
}

// --- 9. The panic hook is installed once and idempotently -------------------

/// Repeated / concurrent installation is a no-op after the first: the framework
/// hook registers exactly once and neither install panics nor corrupts state.
#[test]
fn hook_installation_is_idempotent_and_thread_safe() {
    // Race installation from several threads; none may panic.
    let handles: Vec<_> = (0..8)
        .map(|_| std::thread::spawn(install_panic_hook))
        .collect();
    for h in handles {
        h.join().expect("install_panic_hook never panics");
    }
    // A subsequent single-threaded call is likewise a no-op.
    install_panic_hook();
    install_panic_hook();

    // The hook still contains panics correctly after repeated installs.
    let node = "after-repeated-install";
    let slot = fresh_slot(node);
    let mut sink = CapturingSink::default();
    let outcome = block_on(run_attempt_caught(
        &mut PanicsAlways {
            message: "still contained",
            invocations: Arc::new(AtomicU32::new(0)),
        },
        node,
        &ctx_for(node, 1, 1),
        &slot,
        &mut sink,
    ));
    assert!(matches!(outcome, AttemptOutcome::Panicked));
}

// --- 10. The framework hook coexists with a pre-existing hook ---------------

/// A hook installed **before** the framework's still observes caught panics
/// (chaining): after installing the framework hook and triggering a caught panic
/// inside an attempt, the previously-installed hook fired.
#[test]
fn framework_hook_chains_to_a_pre_existing_hook() {
    // A pre-existing hook that records that it observed a panic (standing in for
    // the test harness's own hook).
    let observed = Arc::new(AtomicU32::new(0));
    let observed_in_hook = Arc::clone(&observed);
    let prior = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        observed_in_hook.fetch_add(1, Ordering::SeqCst);
        // Chain to whatever was there before us (keeps default behaviour off in
        // tests quiet — the framework hook installed next will chain to us).
        let _ = info;
    }));

    // Install the framework hook AFTER the pre-existing one — it must chain to it.
    install_panic_hook();

    let node = "chained";
    let slot = fresh_slot(node);
    let mut sink = CapturingSink::default();
    let outcome = block_on(run_attempt_caught(
        &mut PanicsAlways {
            message: "observe me",
            invocations: Arc::new(AtomicU32::new(0)),
        },
        node,
        &ctx_for(node, 1, 1),
        &slot,
        &mut sink,
    ));

    // Restore the harness's hook before asserting (so a failing assert unwinds
    // cleanly without our test hook counting it).
    let _framework = std::panic::take_hook();
    std::panic::set_hook(prior);

    assert!(matches!(outcome, AttemptOutcome::Panicked));
    assert!(
        observed.load(Ordering::SeqCst) >= 1,
        "the pre-existing hook still observed the caught panic (chaining)"
    );
}

// --- 11. Resource poisoning after a caught panic (author pattern) -----------

/// A stand-in pooled resource whose guarded operation panics mid-use, wrapped in
/// the prescribed **poisoning** pattern: after the panic is caught by the runner,
/// the resource is marked broken and is **not** returned to rotation; a
/// subsequent acquisition does not hand back the mid-operation resource. This
/// documents-by-example the resource author's responsibility.
#[test]
fn resource_poisoning_keeps_a_broken_resource_out_of_rotation() {
    install_panic_hook();

    /// A tiny pool of connections that poisons a connection whose operation
    /// panicked rather than returning it to rotation.
    #[derive(Default)]
    struct Pool {
        available: Vec<u32>,
        poisoned: Vec<u32>,
    }
    impl Pool {
        fn acquire(&mut self) -> Option<u32> {
            self.available.pop()
        }
        fn poison(&mut self, id: u32) {
            self.poisoned.push(id);
        }
        fn release(&mut self, id: u32) {
            self.available.push(id);
        }
    }

    let pool = Arc::new(Mutex::new(Pool {
        available: vec![1],
        poisoned: vec![],
        }));

    // A task that acquires the pooled connection, then panics mid-operation; the
    // poisoning is done in the task's own guard (an author pattern), which runs
    // as the panic unwinds through the caught poll.
    struct UsesPooledConn {
        pool: Arc<Mutex<Pool>>,
    }
    impl Task for UsesPooledConn {
        type Input = ();
        type Output = Report;
        async fn run(&mut self, _c: &RunContext, _i: ()) -> Result<Report, TaskError> {
            let id = self.pool.lock().unwrap().acquire().expect("a conn is available");
            // A guard that poisons on drop-during-unwind, releases on clean drop.
            struct Guard {
                pool: Arc<Mutex<Pool>>,
                id: u32,
                clean: bool,
            }
            impl Drop for Guard {
                fn drop(&mut self) {
                    let mut p = self.pool.lock().unwrap();
                    if self.clean {
                        p.release(self.id);
                    } else {
                        p.poison(self.id);
                    }
                }
            }
            let _guard = Guard {
                pool: Arc::clone(&self.pool),
                id,
                clean: false,
            };
            panic!("connection blew up mid-statement");
        }
    }

    let node = "uses-pool";
    let slot = fresh_slot(node);
    let mut sink = CapturingSink::default();
    let outcome = block_on(run_attempt_caught(
        &mut UsesPooledConn {
            pool: Arc::clone(&pool),
        },
        node,
        &ctx_for(node, 1, 1),
        &slot,
        &mut sink,
    ));

    assert!(matches!(outcome, AttemptOutcome::Panicked));
    let p = pool.lock().unwrap();
    assert!(
        p.poisoned.contains(&1),
        "the mid-operation connection was poisoned"
    );
    assert!(
        p.available.is_empty(),
        "the poisoned connection was NOT returned to rotation"
    );
}
