# 034 · T24 — M1 run-loop driver

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C11, C14, C19
> **Branch:** `feat/t24-m1-run-loop-driver` · **Depends on:** T18, T20, T0.6 · **Blocks:** T25, T26, T27, T31, T33, T34, T62, T67, T69

## Why / context
This ticket stitches the M1 pieces into an actual run: it is the loop that drives a pipeline from "assembled" to "finished" and terminates truthfully. It builds directly on the readiness tracker (C11, T18) that decides what is eligible, the single-attempt runner (C14, T20) that executes one node, and the run-store/event-stream contract (C19, T0.6/T19) that records everything. The governing behavior lives in arch.md's "The shape of a run" (the four phases and the run-store layout), C11 (readiness and the run-end condition), C14 (attempt outcomes and zombie handling), and C19 (the event vocabulary and identity-before-assembly rule). This is the seam T0.5 and T0.6 jointly settled for run identity: identity is minted and the store/stream are opened before assembly runs, so an assembly failure still lands in the record. Because nearly all of M1's tests (T25, T26, T27, T31, T33, T34, T62, T67, T69) drive a real run through this loop, correctness here gates the rest of the milestone.

## Objective
Build the run-loop driver: the component that orchestrates one complete run and exits with control returned to the caller once the run has genuinely ended. Concretely:

- Mint run identity as a UUIDv7 (operator-overridable via flag/environment) at bootstrap, and open the run store and event stream *before* assembly executes, so an assembly failure records itself.
- Capture allowlisted environment values at bootstrap (allowlist declared at pipeline construction, empty by default) for the run-started header; capture nothing outside the allowlist.
- Emit the run-started event carrying every run-artifact header field known at start (identity, pipeline identity, both fingerprints, parameters/data interval, captured allowlisted environment values — everything but the overall outcome and summary).
- Drive the execution loop: admit ready nodes reported by the readiness tracker (C11), spawn each admitted node's attempt through the attempt runner (C14), feed every attempt outcome back into the tracker so dependents decrement and become ready or get their propagated terminal state, and repeat until nothing is pending or in flight.
- Run framework machinery (the loop, timers, cancellation fan-out, the event-stream writer, signal handling) on the isolated framework runtime per T2, kept off the task-execution runtimes so misbehaving tasks cannot disable the loop.
- Terminate exactly when nothing is pending and nothing is in flight, where an abandoned-but-running closure counts as *decided* (not in-flight): at natural run end, wait a bounded grace period (C16) for any zombie closures to return, emit a zombie-at-exit event for each that does not, then emit run-finished and return.
- Emit the run-finished event and surface the run's overall outcome so the caller (the run verb) can select the exit code (C26 is out of scope for this ticket — the driver reports the outcome, it does not own the code table).

## Test plan (write these first — TDD)
Each scenario is set up against a real assembled pipeline (or a deliberately-failing one), driven through the driver, and checked against the event stream and returned outcome. Streams are asserted by parsing the actual sink output, not internal state.

- **Happy-path single node terminates.** Setup: a one-node pipeline whose task succeeds. Action: run the driver to completion. Expected: the stream contains, in order, run-started, node-ready, node-admitted, attempt-started, attempt-succeeded, node-terminal(`succeeded`), run-finished; the driver returns an overall-success outcome; the call returns rather than hanging.

- **Linear chain drives dependents.** Setup: a three-node chain A→B→C, all succeeding. Action: run to completion. Expected: B's node-ready appears only after A's node-terminal; C's only after B's; every node ends `succeeded`; run-finished is the last record.

- **Fast branch is not gated on the slow branch (no wave batching).** Setup: a diamond where one branch is artificially slow and the fast branch has descendants that do not depend on the slow branch. Action: run to completion. Expected: the fast branch's independent descendants reach node-admitted before the slow branch's node-terminal appears — demonstrating the loop admits the instant dependencies allow, never batching a whole level.

- **Run ends precisely when nothing is pending or in flight.** Setup: a pipeline where the last-finishing node has no dependents. Action: run to completion. Expected: run-finished is emitted immediately after that node's terminal event with no further admissions; the driver does not spin or wait beyond that point.

- **Identity is a UUIDv7 minted at bootstrap.** Setup: a valid pipeline with no run-id override. Action: run and inspect the run-started header and the store directory. Expected: the run identity is a well-formed UUIDv7; the run wrote under `<base>/<pipeline>/<run-id>/`; every record carries that identity.

- **Operator override replaces the minted identity.** Setup: the same pipeline invoked with an explicit run-id override. Action: run and inspect. Expected: the run identity everywhere in the stream and the store path equals the supplied value, not a freshly minted UUIDv7.

- **Store and stream open before assembly; assembly failure still records.** Setup: a pipeline that fails assembly (e.g. a duplicate node name). Action: invoke the run verb path. Expected: a run-started record (or the assembly-failure record variant) exists on disk under the run store carrying the minted identity — proving the store/stream were opened before assembly acted — and the driver reports an assembly-failure outcome distinct from a successful run.

- **Allowlisted environment values are captured; others are not.** Setup: an environment with both an allowlisted variable (declared at construction) and a non-allowlisted secret set. Action: run and read the run-started header. Expected: the allowlisted value appears; the non-allowlisted value appears nowhere in the stream or header. With an empty (default) allowlist, no environment value appears.

- **Every attempt outcome is fed back and produces its records.** Setup: a two-node pipeline where the upstream succeeds and the downstream succeeds. Action: run. Expected: each node produces exactly one attempt-outcome record and a single node-terminal event; the downstream's readiness followed only after the upstream's terminal outcome was fed back.

- **Zombie at natural run end: bounded grace wait, then zombie-at-exit event.** Setup: a pipeline whose sole node is a blocking task that has already been marked timed-out (its permit is abandoned-but-running) while its thread refuses to return; no other work is pending. Action: run to natural end. Expected: the node's terminal state is and stays `timed-out` (never a second terminal state); the driver waits no longer than the grace period; a zombie-at-exit event is emitted for the leftover thread; run-finished follows; the abandoned closure did *not* hold the run open indefinitely.

- **Framework machinery survives a misbehaving task.** Setup: a pipeline whose task blocks its worker indefinitely, with a per-attempt timeout set. Action: run. Expected: the timeout still fires, the event stream is still written (run-started, node-ready, node-admitted, attempt-started, timeout events all present and parseable), and the driver still reaches run-finished — proving the loop and writer run isolated from task execution.

- **Two simultaneous runs of the same binary do not interfere.** Setup: two runs launched concurrently against the same base store. Action: run both to completion. Expected: each writes under its own `<base>/<pipeline>/<run-id>/` directory, the two streams are disjoint files, both are valid and parseable, and every record in each carries its own run identity.

- **Skip-only run reports success.** Setup: a pipeline whose only node returns a deliberate skip. Action: run to completion. Expected: the node ends `skipped`, the run terminates, and the driver's reported overall outcome is success (a run containing only skips is a successful run).

## Definition of done
- [ ] Run identity is minted as a UUIDv7 at bootstrap and is operator-overridable via flag/environment; the override, when present, replaces the minted value everywhere.
- [ ] The run store and event stream are opened *before* assembly executes on the run-verb path, so an assembly failure has a place to record itself; the inspection verbs (validate, graph, render) are unaffected and open no store (their handling stays out of this ticket beyond not regressing it).
- [ ] Each run writes under its own `<base>/<pipeline>/<run-id>/` directory; two simultaneous runs write disjoint files and both produce valid, parseable streams, each record carrying its own run identity.
- [ ] Allowlisted environment values are captured at bootstrap (allowlist declared at construction, empty by default); no value outside the allowlist appears anywhere in the stream or header.
- [ ] The run-started event carries every run-artifact header field known at start (identity, pipeline identity, both fingerprints, parameters/data interval, captured allowlisted environment values) and omits only the overall outcome and summary.
- [ ] The driver admits ready nodes from the readiness tracker, spawns each attempt through the attempt runner, and feeds every attempt outcome back so dependents decrement and either become ready or receive their propagated terminal state — with no batching into waves (a node whose dependencies complete early starts before unrelated slower work finishes; in a diamond the fast branch's independent descendants are not delayed by the slow branch).
- [ ] Every node ends in exactly one terminal state from the normative taxonomy, and the run ends precisely when nothing is pending or in flight.
- [ ] An abandoned-but-running closure is treated as *decided* and does not hold the run open indefinitely; at natural run end the driver waits at most the grace period (C16) for zombies to return, then proceeds.
- [ ] A zombie-at-exit event is emitted for each leftover thread that has not returned at exit, and it changes no node's terminal state (a blocking timeout stays `timed-out`, never becomes a second terminal state).
- [ ] Framework machinery (loop, timers, cancellation fan-out, event-stream writer, signal handling) runs on the isolated framework runtime per T2, so a misbehaving task can stall task progress but cannot disable the loop, the timeout, or the event stream — verified by the all-workers-blocked scenario.
- [ ] The run-finished event is emitted as the final record of a completed run, and the driver returns the overall outcome (including the assembly-failure and skip-only-success cases) to its caller; exit-code selection (C26) is explicitly left to the caller.
- [ ] Public items on the driver carry rustdoc; the run-loop's termination condition and the zombie-grace behavior are documented where a reader of the driver will see them.
- [ ] All Test plan scenarios are implemented as automated tests and pass.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
The ticket header and `docs/tasks.md`'s T24 entry list **no** formal open
questions (no `Q:` items). The design choices the ticket deliberately left to
implementation are recorded here for the record:

- **M1 concurrency model — resolved: simple concurrent spawn on an isolated
  framework runtime.** The DoD requires both no-wave-batching (a fast branch's
  descendant admitted before an unrelated slow branch terminates) *and*
  framework-machinery survival under a task that jams its worker (the timeout
  still fires and the stream is still written). A purely sequential loop cannot
  satisfy the second, so the driver builds **two** multi-threaded tokio runtimes
  (T2's isolated framework runtime): attempts spawn onto a `tasks` runtime while
  the loop, per-attempt timers, and the single-owner event writer run on a
  `framework` runtime. Admitted attempts report their terminal state and buffered
  records back over a `tokio::sync::mpsc` channel; the loop drains each into the
  writer in order and feeds the outcome back into the tracker. This is the minimal
  spawn model — no admission pools, weighted permits, or class dispatch (those are
  T31/T33).

- **Zombie-at-exit detection in M1 — resolved: terminal-state-based candidate
  set.** M1 has no permit ledger to confirm a blocking closure returned (that is
  T31), so the driver treats a node that terminated `timed-out`/`abandoned` as a
  zombie candidate, waits at most the C16 grace period at natural run end, then
  emits `zombie-at-exit` for each. The `tasks` runtime is shut down with
  `shutdown_background` (not a blocking `Drop`) so an unkillable busy blocking
  thread never holds the run open — the abandoned closure is *decided*, not
  in-flight.

- **Abstract-sink → concrete-writer adaptation — resolved (the seam T20 left to
  T24).** The C14 runner emits abstract `AttemptEvent` records through the
  infallible `AttemptEventSink` port; the driver translates each into the concrete
  C19 `Event` and stamps the envelope via `EventStreamWriter`. A spawned attempt
  emits into a per-attempt buffering sink off the framework runtime; the loop
  drains it into the single-owner writer, keeping the writer's write-through,
  single-writer contract intact.

## Out of scope
- The exit-code table and precedence rules (C26, T55) — the driver reports the overall outcome; the run verb maps outcome to code.
- The admission controller's capacity pools and permit accounting (C12, T31) — this loop admits the nodes the tracker/runner hand it against whatever admission surface T20 already exposes; weighted pools, bounded-bypass ordering, and zombie cost accounting land in T31.
- Cancellation triggering and graceful drain, OS signal handling, and teardown-node execution (C15/C16/C17, T34/T35/T36) — this ticket only consumes the C16 grace period as the bounded zombie wait at *natural* run end.
- Retry with backoff, per-attempt timeout mechanics, and panic containment internals (C14, T21/T22/T23) — the driver feeds back whatever outcome the runner produces; it does not implement the classification.
- The run artifact fold and the graph artifact themselves (C20/C22) — the driver emits the run-started header fields, not the derived end-of-run artifact.
- Resume, `satisfied-from-prior`, and durable-output rehydration (C27) — M1 runs from scratch every time.
- Anything on the permanent scope boundary: this loop is not a scheduler, admits no cross-run coordination, holds no persistent metadata store, and the graph shape it drives is fixed at assembly and never changes at runtime.
