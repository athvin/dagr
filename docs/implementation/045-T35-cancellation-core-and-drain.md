# 045 · T35 — C16: cancellation core and graceful drain

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C16
> **Branch:** `feat/t35-cancellation-core-and-drain` · **Depends on:** T24, T34 · **Blocks:** T36, T37, T52

## Why / context
dagr must stop a run without leaving debris it can actually prevent, and be honest about the debris it cannot. This ticket builds the in-process cancellation machinery of C16 (`### C16 · Cancellation and shutdown`): a run-scoped cancellation signal with per-attempt children, a cooperative grace period, drain-before-exit, and the `cancelled`-vs-`abandoned` classification that C12 and C15 already lean on. It builds directly on the T24 run-loop driver (which already performs a bounded grace wait for zombie closures at natural run end) and on the T34 failure policy (stop-on-first-failure is one trigger of cancellation). It deliberately stops short of the OS-signal wiring, fsync/final-flush, and temp-dir cleanup, which T36 layers on top; here we prove the internal token, the classification, and the printed shutdown-budget arithmetic.

## Objective
Build the internal cancellation core and graceful-drain behaviour for a run, independent of any OS signal source:

- A run-scoped cancellation token owned by the driver, with a per-attempt child token handed to each spawned attempt, such that cancelling the run cancels every live child exactly once and is idempotent.
- An internal cancellation entry point the driver can invoke from any origin (currently: a failure under stop-on-first-failure per T34, and a to-be-wired external interrupt in T36); the origin is remembered so C26 exit-code precedence (failure over cancellation) can be decided later.
- A cooperative grace period with a default of 10 seconds, exposed as an operator flag; on cancellation, in-flight await-bound attempts are asked to stop and given up to grace to return.
- Terminal-state classification on the cancellation path: an attempt that observes the signal and returns promptly within grace is recorded `cancelled`; one that does not return within grace is recorded `abandoned`. Both are distinct from `failed` and from `timed-out`; `abandoned` arises only on this path.
- Drain-before-exit: after cancellation the driver stops admitting new work, lets the tracker settle pending non-in-flight nodes per T34's stop-mode rules, and waits up to grace for outstanding attempts to return before proceeding to exit.
- Shutdown-budget arithmetic computed and printed at startup: grace (default 10 s) + teardown deadline (default 15 s, C17) + bounded final flush (2 s), with the worst-case total shown so a misconfiguration is visible before it matters. Grace and teardown deadline are operator flags; this ticket owns the grace flag and consumes the teardown-deadline value.

## Test plan (write these first — TDD)

- **Run token cancels all live children once.** Setup: a run with several concurrently spawned await-bound attempts, each holding its per-attempt child token. Action: cancel the run token once. Expected: every child token reports cancelled, each attempt observes the signal exactly once, and a second cancel call changes nothing (idempotent, no double classification).

- **Child cancels without touching siblings or the parent.** Setup: two attempts with sibling child tokens under one run token. Action: cancel only one child (the per-attempt path used by a timeout in C12, not exercised here beyond the token). Expected: that child is cancelled, the sibling child and the parent run token remain uncancelled, and the run is not treated as cancelled.

- **Prompt observer is recorded `cancelled`.** Setup: an await-bound attempt whose closure checks the signal and returns well inside grace. Action: cancel the run. Expected: the node's terminal state is exactly `cancelled`, distinct from `failed` and `timed-out`; the attempt did not fill its output slot.

- **Non-returning work is recorded `abandoned` after grace.** Setup: an await-bound attempt that ignores the signal and keeps running past a short configured grace. Action: cancel the run and let grace elapse. Expected: the node's terminal state is exactly `abandoned` (never a second state after any prior state), the driver proceeds without waiting indefinitely, and the abandoned attempt can never fill an output slot.

- **`cancelled` and `abandoned` are distinct from `failed`.** Setup: three attempts — one returns promptly on signal, one runs past grace, one returns an error before cancellation. Action: cancel after the error is observed. Expected: the three nodes land `cancelled`, `abandoned`, and `failed` respectively, with no cross-contamination.

- **Stop-on-first-failure triggers cancellation via the core.** Setup: a graph with a node that fails under T34 stop-on-first-failure while unrelated default-rule nodes are still pending, and other await-bound work in flight. Action: run to the failure. Expected: the cancellation core is invoked with a failure origin; pending unrelated default-rule nodes end `cancelled` (per T34's resolved rule); in-flight cooperative work drains to `cancelled`/`abandoned`; the recorded origin marks this as failure-triggered so later exit-code logic can prefer run failure.

- **Drain waits at most grace, then proceeds.** Setup: a cancellation with one attempt that returns just before grace and one that never returns. Action: cancel and observe the drain window. Expected: the driver waits no longer than grace, the returning attempt is `cancelled`, the non-returning one is `abandoned`, and the run reaches its post-drain point within the grace bound.

- **No new admission after cancellation.** Setup: a run with ready-but-unstarted nodes when cancellation fires. Action: cancel. Expected: no new attempt is spawned after cancellation; those nodes are settled to their propagated/`cancelled` terminal states rather than executed.

- **Grace default and flag.** Setup: start the run without the grace flag, then with an explicit override. Action: inspect the effective grace and the drain timing. Expected: default is 10 seconds; the override is honoured and drives the drain wait and the printed budget.

- **Shutdown budget printed at startup and reflects flags.** Setup: default flags, then a grace override. Action: start the binary and capture startup output. Expected: the worst-case budget line prints grace + teardown deadline (15 s) + final flush (2 s) with the correct arithmetic total (27 s at defaults); overriding grace changes the printed total accordingly.

- **Natural run end still bounds the zombie wait.** Setup: a run that completes normally (no cancellation) with one abandoned-but-running closure left over from a C12 timeout. Action: reach natural run end. Expected: the driver waits at most grace for the zombie to return, then proceeds; behaviour matches T24's existing bounded grace wait and is not double-counted by this ticket's cancellation drain.

## Definition of done
- [ ] A run-scoped cancellation signal exists with a per-attempt child; cancelling the run cancels all live children exactly once and is idempotent.
- [ ] An internal cancellation entry point exists that records the cancellation origin (failure-under-stop-on-first-failure vs external interrupt) for later C26 exit-code precedence.
- [ ] A task/attempt that observes cancellation and returns promptly within grace is recorded `cancelled`; one that does not return within grace is recorded `abandoned`; both are distinct from `failed`, and `abandoned` arises only on the cancellation path.
- [ ] The grace period defaults to 10 seconds and is operator-configurable via a flag; the drain wait and printed budget both honour it.
- [ ] Drain-before-exit is implemented: after cancellation no new work is admitted, pending non-in-flight nodes settle per T34 stop-mode rules, and the driver waits at most grace for outstanding attempts before proceeding to exit.
- [ ] Pending unrelated default-rule nodes under stop mode end `cancelled` (consistent with T34's resolved rule), and stop-on-first-failure routes through this cancellation core.
- [ ] An abandoned attempt can never fill its output slot; whatever it computes after grace is discarded.
- [ ] The worst-case shutdown budget — grace (default 10 s) + teardown deadline (default 15 s) + bounded final flush (2 s) — is computed and printed at startup, and reflects the effective flag values.
- [ ] The bounded zombie wait at natural run end (T24) remains correct and is not duplicated by the cancellation drain.
- [ ] All Test plan scenarios are implemented as tests and pass; each is independently checkable.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- OS-signal handling (SIGTERM/SIGINT → cancel), the final event-stream flush with fsync, and the bounded-wait/distinct-exit-code behaviour for an unwritable sink at shutdown — all owned by T36.
- Per-run temp-dir convention and its removal by the next invocation — T36.
- Teardown-node execution under a fresh, uncancelled signal and the teardown deadline itself (C17) — this ticket only consumes the teardown-deadline value for budget arithmetic; teardown behaviour is T52.
- The permit-release outcome matrix and zombie permit accounting under cancellation (C12) — T37; this ticket only produces the `cancelled`/`abandoned` classifications those tests assert against.
- Final exit-code selection and the C26 precedence table — this ticket records the cancellation origin but does not own the exit-code mapping.
- Scheduling, distributed cancellation, or any runtime change to graph shape — permanently outside dagr; cancellation stops the existing run, it never reshapes or reschedules the DAG.
