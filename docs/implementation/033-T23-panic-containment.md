# 033 · T23 — C14: panic containment

> **Milestone:** M1 · **Size:** S · **Type:** feature · **Components:** C14
> **Branch:** `feat/t23-panic-containment` · **Depends on:** T20 · **Blocks:** T28, T37

## Why / context
The single-attempt execution core (T20, C14) can run one node and classify its outcome, but a task that panics currently unwinds straight through the runner and takes the whole run down. This ticket closes that hole: a panic must fail only its own node, be recorded as a permanent `failed` state, and leave the rest of the run free to proceed under the failure policy (C15). It is governed by the **Panics** paragraph and the panic-related acceptance criteria of arch.md's `C14 · Attempt runner`, plus the `failed` terminal-state definition in the Vocabulary. The design decisions here are already resolved (startup refusal of `panic = "abort"`, `AssertUnwindSafe` at the boundary, poisoning as the resource-author pattern, a single hook that coexists with the test harness); this ticket implements them and locks them behind tests, so it blocks the M1 demo (T28) and the permit-release outcome matrix (T37).

## Objective
Contain panics inside the attempt runner so a panicking node fails only itself, and make the binary refuse to run in a configuration where containment is impossible. Concretely:

- Wrap the attempt boundary so a panic in task code is caught rather than propagated, classified as a **permanent failure** (never retry-eligible), and turned into the node's `failed` terminal state.
- Attribute the caught panic to the specific node whose attempt panicked, via task-local state, so the recorded failure and the attempt-outcome record name the right node even when several attempts run concurrently.
- Emit exactly one *attempt-outcome* record for a panicking attempt, alongside its per-transition events, consistent with every other outcome path (C19).
- Apply the unwind-safety assertion at the catch boundary so ordinary task types compile without forcing every task author to reason about unwind safety.
- Install the framework's panic hook exactly once (idempotent, safe under concurrent first-use), have it attribute the panic to the current node, and have it chain to / coexist with any pre-existing hook — including the test harness's hook — rather than replacing it.
- Add a startup check that inspects the compiled panic strategy and refuses to run under `panic = "abort"`, exiting with a clear message that names the required profile setting (the fix), because under abort there is nothing to catch.
- Document the resource-author responsibility: shared resources touched mid-panic are the author's concern, and the prescribed pattern is **poisoning** — a pooled resource that may be mid-operation is marked broken and dropped from rotation rather than handed back for reuse.

## Test plan (write these first — TDD)

**1. A panicking task fails only its own node.**
Setup: a runner harness with a node whose task body panics, running under the unwinding profile. Action: execute the attempt through the runner. Expected: the runner returns a classified permanent failure rather than propagating the panic; the node's terminal state is `failed`; the harness thread that drove the runner is still alive (the run was not unwound).

**2. The rest of the run proceeds after a panic.**
Setup: a small graph (in-flight harness) with one node that panics and one independent node with no dependency on it, under continue-independent failure policy. Action: run both. Expected: the panicking node ends `failed`; the independent node runs to completion and ends `succeeded`; overall outcome reflects the failure per policy, not a crash.

**3. A panic is a permanent failure, never retried.**
Setup: a node configured with a retry budget greater than one whose task panics on every attempt. Action: run it. Expected: exactly one attempt is executed (the panic is classified permanent, not retry-eligible), the remaining retry budget is untouched, and the node ends `failed`.

**4. The panic is attributed to the correct node under concurrency.**
Setup: two nodes executing concurrently, each with a distinct identity; one panics with a recognizable message, the other succeeds. Action: run both concurrently. Expected: the failure record and the attempt-outcome record for the panic name the panicking node's identity (not the other node's), and the panic's captured detail is associated with that node — attribution comes from task-local state, so it stays correct despite interleaving.

**5. Exactly one attempt-outcome record for a panicking attempt.**
Setup: the runner wired to a captured event stream; a node that panics. Action: run the attempt. Expected: the stream contains exactly one *attempt-outcome* record for that attempt, marked as a panic outcome, alongside the normal per-transition events (attempt started, node reached terminal state) — the same one-record invariant that holds for success, timeout, and permanent failure.

**6. The startup check refuses `panic = "abort"`.**
Setup: a build (or a test-controllable equivalent of the abort configuration) whose panic strategy is abort. Action: invoke the startup panic-strategy check. Expected: the binary refuses to proceed and exits with the designated bootstrap-failure path; the message explicitly names the profile setting the operator must change (the fix), so the operator can act without reading source.

**7. The startup check passes under the unwinding profile.**
Setup: the normal unwinding build. Action: invoke the same startup check. Expected: the check succeeds silently and the run is allowed to proceed — the refusal is specific to abort, not a blanket gate.

**8. `AssertUnwindSafe` lets an ordinary task compile at the boundary.**
Setup: a throwaway compile fixture (documented in the ticket, not shipped) wrapping a representative task closure that is not `UnwindSafe` on its own. Action: compile it through the catch boundary as the framework does. Expected: it compiles because the boundary asserts unwind safety; a parallel negative fixture that removes the assertion fails to compile — proving the assertion is load-bearing, not decorative.

**9. The panic hook is installed once and idempotently.**
Setup: the hook-installation entry point invoked more than once, including from multiple threads racing on first use. Action: install repeatedly / concurrently. Expected: the framework's hook is registered exactly once, no install panics or corrupts state, and repeated calls are no-ops.

**10. The framework hook coexists with a pre-existing hook.**
Setup: a hook installed before the framework's (standing in for the test harness's own hook). Action: install the framework hook, then trigger a caught panic inside an attempt. Expected: the framework attributes the panic to its node **and** the previously installed hook still observes the panic (chaining), so running the suite under a harness that sets its own hook does not lose either behavior; the full test suite runs to completion without the hook interfering with unrelated panics.

**11. Resource poisoning after a caught panic (author pattern, exercised).**
Setup: a fixture resource (a stand-in pooled connection) whose guarded operation panics mid-use, wrapped in the prescribed poisoning pattern. Action: run the panicking operation, catch the panic via the runner, then attempt to reuse the resource from the pool. Expected: the resource is marked broken and is **not** returned to rotation; a subsequent acquisition does not hand back the mid-operation resource. This test documents-by-example the resource author's responsibility rather than asserting a framework guarantee about arbitrary shared state.

## Definition of done
- [ ] A panic in task code is caught at the attempt boundary and converted to a **permanent** failure; it is never allowed to unwind the run.
- [ ] A panicking task fails only its own node; the rest of the run proceeds per the failure policy (C15), verified with at least one independent node completing.
- [ ] A caught panic yields the node's `failed` terminal state, consistent with the Vocabulary definition of `failed`.
- [ ] A panic is classified permanent and is never retried, regardless of remaining retry budget.
- [ ] The caught panic is attributed to the correct node via task-local state, and attribution remains correct under concurrent attempts.
- [ ] The catch boundary applies `AssertUnwindSafe`, and a compile fixture demonstrates that ordinary task closures compile because of it (with a negative fixture proving the assertion is required).
- [ ] The framework installs its panic hook exactly once, idempotently and safely under concurrent first-use.
- [ ] The framework's hook chains to / coexists with a pre-existing hook (including the test harness's), attributing panics to nodes without suppressing the other hook, and the full suite runs cleanly under it.
- [ ] The binary checks its panic strategy at startup and refuses to run under `panic = "abort"`, exiting via the bootstrap-failure path with a message that names the required profile setting (the fix).
- [ ] The startup check permits the normal unwinding profile without complaint.
- [ ] Every panicking attempt produces exactly one *attempt-outcome* record in the event stream, alongside its per-transition events (C19).
- [ ] Resource poisoning is documented as the resource author's pattern for shared-resource integrity after a caught panic, with a worked example (poisoned pooled resource not returned to rotation), and the rustdoc for the attempt boundary states that shared-resource integrity after a caught panic is the resource author's responsibility.
- [ ] All Test plan scenarios above are implemented and passing.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

The ticket's original list was "None," and the `docs/tasks.md` T23 entry carries
**resolutions**, not `Q:` items ("Resolved: startup check refuses `panic=abort`
… `AssertUnwindSafe` at the boundary; resource poisoning documented …; hook
installed once, coexists with test harness"). Both open-question sources
(ticket-conventions §5) are therefore discharged by the design already fixed in
arch.md C14, the T3 ADR (016), and that tasks entry. Recorded below are the
**sub-decisions** T23 had latitude on, each resolved against those governing
sources rather than picked silently:

- **Terminal state of a caught panic — `failed`, not a distinct `panicked`
  state.** arch.md's Vocabulary lists **nine** terminal states and defines
  `failed` as *"Permanent failure, retries exhausted, or a caught panic."* There
  is **no** `panicked` terminal state. Resolved: a caught panic maps to
  `TerminalState::Failed` via a new `AttemptOutcome::Panicked` classification
  (the reserved variant T20 named). The *classification* is distinct (`Panicked`)
  so the runner can attribute and message it, but the *terminal state* is
  `failed`, exactly as the T3 ADR §5 mapping table fixes (panic → `failed`,
  failure-like).
- **Retry-eligibility of a panic — never retried.** T3 ADR §4/§5 and the "panic
  is a permanent failure" rejected-alternative fix panic → **permanent** failure
  → not retry-eligible; retrying a body that panicked risks poisoned shared state
  (the prescribed pattern is poisoning, not blind retry). Resolved:
  `AttemptOutcome::Panicked::is_retry_eligible()` is `false`, so
  `run_with_retries` stops after the panicking attempt with the budget untouched
  — panic composes with T22 by ending the loop, exactly like a permanent failure.
- **Panic-message capture rule.** Following the dagx prior art (§4) and the T3
  ADR spike: downcast the panic payload to `&'static str` then `String`, and use
  the literal `"unknown panic"` when it is neither. The captured message is
  carried on the `AttemptEvent::AttemptPanicked` outcome record (the C19 record
  the driver stamps). The default hook's stderr print is suppressed *only for the
  contained attempt's own panic* by the framework hook's attribution path — never
  globally for the process.
- **Catching a panic from an *async* future.** The catch boundary wraps the
  future's **poll** (`catch_unwind(AssertUnwindSafe(|| fut.poll(cx)))`), not just
  a sync closure, so a panic that unwinds *during* an `.await` is contained. This
  is a small dependency-free `CatchUnwindPoll` adapter (heap-pinned like T21's
  `RaceFuture`, hence `Unwind`-safe to poll with **no `unsafe`**); the `futures`
  crate's `CatchUnwind` is deliberately **not** added (`dagr-core` stays
  dependency-free). A future that already panicked is *fused*: it is dropped and
  never polled again.
- **Making the `panic = "abort"` startup check test-controllable.** The real
  check reads the compiled unwind strategy. Because a test binary cannot itself
  be compiled `panic = "abort"` (it would abort, not fail assertably), the check
  is expressed as a pure function `check_panic_strategy(PanicStrategy) ->
  Result<(), BootstrapRefusal>` over an explicit `PanicStrategy` enum, plus a
  thin `detect_panic_strategy()` that reports the compiled strategy. Tests drive
  the pure function with both strategies; the refusal message names the required
  profile setting (`panic = "unwind"`). The driver (T24) calls
  `detect_panic_strategy()` then the pure check at bootstrap.
- **Hook idempotency + chaining mechanism.** A `std::sync::Once` installs the
  framework hook exactly once (safe under concurrent first-use); the hook
  **captures the previously-installed hook** (including the test harness's) and
  **calls it**, so both behaviours survive. Attribution is via a
  `thread_local!` node-name cell set by a scope guard around the caught poll.
- **Resource poisoning is an author pattern, not a framework guarantee.** Proven
  by a worked example test (a pooled resource poisoned after a caught panic, not
  returned to rotation) and stated in the attempt-boundary rustdoc; the framework
  makes no guarantee about arbitrary shared state after a caught panic.

## Out of scope
- Per-attempt timeout semantics and permit release on timeout — that is T21 (C14). This ticket does not touch timeout classification or the abandoned-but-running / zombie accounting path (C12), except to leave those paths undisturbed.
- Retry counting, exponential backoff, and jitter — that is T22 (C14). This ticket only asserts that a panic consumes no retries; it does not implement the retry scheduler.
- Cooperative cancellation, grace periods, and the `abandoned` state — C16. A panic is not a cancellation and must not be conflated with one.
- Failure-policy propagation logic (stop-on-first-failure vs continue-independent, `upstream-failed`) — C15. This ticket relies on that policy to let the run proceed but does not implement it.
- The full permit-release outcome matrix across every outcome including panic — that is the downstream T37, which this ticket blocks; here we only prove the panic path in isolation.
- Turning panic-driven failure into a distributed or restartable concern, a scheduler decision, or a runtime graph mutation — outside dagr's permanent scope boundary. A panic changes one node's terminal state and nothing about the graph shape.
