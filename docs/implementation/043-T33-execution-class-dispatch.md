# 043 · T33 — C13: execution class dispatch

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C13
> **Branch:** `feat/t33-execution-class-dispatch` · **Depends on:** T20, T29, T2 · **Blocks:** T38

## Why / context
This ticket implements C13 (`arch.md` · "C13 · Execution class dispatch"), the layer that puts each kind of work on the right kind of thread so that one class of task can never starve another and so that misdeclaring a class can degrade *task* progress but never disable the safety rails. It builds directly on the T2 ADR (tokio confirmed as a public dependency, blocking-pool and compute-pool strategy chosen, cancellation-token primitive, and — critically — the isolated framework runtime for timers/cancellation/event-writing/signals), on T20's single-attempt execution core (the point where an attempt is actually dispatched), and on T29's node policy (which carries the execution-class override and the assembly-time validation of it, per C5). Dispatch is the seam between "the runner decided to run this attempt" and "the attempt's work is now executing on the correct thread pool." The M2 demo (T38) depends on this: overcommit-and-clean-stop assumes work is already on the right pools.

## Objective
Route every dispatched attempt to the thread execution surface named by its resolved execution class, honour the policy override within C5's limits, and keep the framework's own machinery isolated from task execution so it survives even a fully blocked task fleet.

Concrete pieces of work:
- A three-class dispatcher that sends *await-bound* work onto the async (tokio) runtime, *blocking* work onto a dedicated blocking pool, and *compute-bound* work onto a fixed compute pool sized to the container's CPU allocation (pool sizing itself is C12/T31/T32 territory — consume it, do not re-derive it here).
- Resolution of the effective execution class from the task's declared class (C1) and the node policy override (C5/C29), applied at dispatch time; the override's assembly-time validity check already lives in T29 — this ticket relies on it, it does not duplicate it.
- Wiring so that the compute pool's concurrency is bounded by the pool's size (no more compute-class attempts run at once than the pool holds).
- The isolated framework runtime: timers, cancellation fan-out, the event-stream writer, and signal handling run on machinery that is not shared with task execution, so a task that hogs or blocks every task worker cannot stall a timeout firing, a SIGTERM being handled, or the event stream being flushed.
- Honest exposure of tokio types only where the API is honest about it; context-exposed types remain dagr-owned wherever practical.
- Rustdoc on the dispatcher and the class enum stating which surface each class runs on and the exact override-legality rule, cross-referencing C5 and C13.

## Test plan (write these first — TDD)

**Class-to-surface routing (await-bound).** Setup: a node whose task declares the await-bound class, no override, running under a test runtime instrumented to report which surface a unit of work executed on. Action: dispatch one attempt. Expected: the work runs on the async runtime, not on the blocking or compute pool, and the attempt completes with the produced value.

**Class-to-surface routing (blocking).** Setup: a node whose task declares synchronous work and whose resolved class is blocking. Action: dispatch one attempt that records its executing surface. Expected: the work runs on the dedicated blocking pool; the async runtime's own tasks continue to make progress concurrently.

**Class-to-surface routing (compute).** Setup: a node whose resolved class is compute-bound. Action: dispatch one attempt. Expected: the work runs on the fixed compute pool.

**Override moves class within legal limits.** Setup: a synchronous task declared blocking, with a node policy overriding the class to compute (a legal synchronous-to-synchronous move per C5). Action: dispatch the attempt. Expected: the work runs on the compute pool, i.e. the effective class reflects the override, not the declared class.

**Override respects the C5 legality boundary.** Setup: attempt to assemble a node that overrides an await-bound task to a synchronous class (illegal per C5). Action: run assembly. Expected: assembly fails with a diagnostic naming the node and the illegal transition. (This exercises the boundary through this ticket's dispatch path; the authoritative assembly check is T29 — this scenario confirms dispatch never receives an illegal class.)

**Starvation isolation — a long synchronous task does not delay unrelated await-bound work.** Setup: one blocking-class node whose work sleeps synchronously for a duration far longer than the test's assertion window, plus an unrelated await-bound node whose work completes near-instantly; both dispatched at the same time. Action: dispatch both and measure when the await-bound node's outcome is observed. Expected: the await-bound node completes promptly — well before the long synchronous task returns — proving the synchronous task occupies only the blocking pool and never the async runtime. (This is C13's first acceptance criterion.)

**Compute pool concurrency is bounded by pool size.** Setup: a compute pool pinned to a small size N (via the C12/T32 pinning flag so the bound is deterministic in CI), and more than N compute-class nodes dispatched simultaneously, each recording the peak count of concurrently executing compute attempts. Action: dispatch all of them. Expected: the observed peak concurrency never exceeds N. (C13's second acceptance criterion.)

**Safety machinery survives a fully blocked task fleet — timeout still fires.** Setup: every task worker on the blocking pool occupied by synchronous work that never returns within the test window, and a node with a configured per-attempt timeout among the blocked set. Action: let the timeout elapse. Expected: the timeout still fires — the timed-out event is emitted and the node's fate is decided on schedule — because the timer machinery runs on the isolated framework runtime, not on a task worker. No data already produced is corrupted and no artifact already written is corrupted.

**Safety machinery survives a fully blocked task fleet — SIGTERM still yields a complete stream.** Setup: the same fully blocked task fleet, with the process running under the shutdown path (C16). Action: deliver SIGTERM. Expected: the signal is handled, and a complete event stream is written before the process exits within the shutdown budget — proving signal handling and the event-stream writer are isolated from task execution. (Together with the previous scenario this is C13's third acceptance criterion: misdeclaring a class may stall progress but never corrupts data, never corrupts written artifacts, and never disables timeouts, cancellation, or the event stream.)

**tokio types appear only where honest.** Setup: the public API surface of the dispatcher and the run context. Action: inspect exposed types (a rustdoc/API review, enforceable as a doc assertion). Expected: tokio types appear only where the API is genuinely about the runtime; context-exposed types are dagr-owned wherever practical.

## Definition of done
- [ ] Await-bound work runs on the async (tokio) runtime; blocking work runs on the dedicated blocking pool; compute-bound work runs on the fixed compute pool — each verified by a routing test.
- [ ] The effective execution class is resolved from the task's declared class (C1) and the C5 policy override at dispatch, and dispatch honours the override.
- [ ] A long synchronous task does not delay progress on unrelated await-bound work (starvation-isolation test passes).
- [ ] Concurrently executing compute-class attempts never exceed the compute pool's size (bounded-concurrency test passes, with the pool pinned for CI determinism).
- [ ] Misdeclaring a class may stall the run's progress but never corrupts data, never corrupts artifacts already written, and never disables timeouts, cancellation, or the event stream — verified by the test in which every task worker is blocked yet a timeout still fires and SIGTERM still yields a complete stream.
- [ ] Timers, cancellation fan-out, the event-stream writer, and signal handling run on the isolated framework runtime, not on task-execution threads.
- [ ] Compute-pool sizing and blocking-pool strategy consume the choices fixed by the T2 ADR and the sizing derived by C12/T31/T32; this ticket does not re-derive pool sizes.
- [ ] An execution-class override incompatible with the task's declared work shape never reaches the dispatcher (relies on T29's assembly-time rejection; confirmed by a boundary test).
- [ ] tokio types appear in the public API only where the API is honest about the runtime; context-exposed types are dagr-owned wherever practical.
- [ ] Rustdoc on the dispatcher and the execution-class enum documents each class's execution surface and the override-legality rule, cross-referencing C5 and C13.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Pool sizing derivation from cgroup limits, headroom, unlimited-sentinel fallback, and the capacity-pinning flag — that is C12 (T31/T32); this ticket only consumes the resulting pools.
- The per-attempt timeout mechanism and timeout-by-class semantics themselves — those live in C14 (T21) and the attempt runner; this ticket only proves the *timer machinery's isolation* survives blocked workers.
- Cancellation and shutdown behaviour, the shutdown budget, and abandoned-vs-cancelled recording — C16 (T36/T37); this ticket only proves signal handling and the stream writer stay isolated.
- Panic containment — C14 (T23).
- The assembly-time validation of an illegal execution-class override — owned by T29/C5; consumed here, not re-implemented.
- Permit lifecycle and the abandoned-but-running capacity accounting — C12 (T31).
- Any move toward configurable-at-runtime graph shape, a scheduler, distributed execution, or additional execution classes beyond the three named in C13 — outside dagr's permanent scope boundary; the class set is fixed.
