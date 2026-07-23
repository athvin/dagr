# 031 · T21 — C14: per-attempt timeout

> **Milestone:** M1 · **Size:** S · **Type:** feature · **Components:** C14
> **Branch:** `feat/t21-per-attempt-timeout` · **Depends on:** T20, T0.3 · **Blocks:** T28, T37

## Why / context
The single-attempt execution core (T20) can run one attempt and classify its outcome, but it does not yet bound how long an attempt may run. This ticket adds the per-attempt timeout to the attempt runner (C14 · Attempt runner), applying the per-class semantics that T0.3's ADR + spike proved implementable: an await-bound attempt's future is dropped and its permit releases immediately, while a blocking or compute attempt cannot be killed and so is *marked* timed out immediately with its permit held (C12 · Admission controller) until the closure actually returns. The governing behaviour is in arch.md's "Timeout semantics differ by class, honestly" paragraph under C14 and the abandoned-but-running exception under C12. This ticket locks the observable timeout behaviour that T28's demo exercises and that T37's permit-release outcome matrix asserts against.

## Objective
Give the attempt runner a per-attempt timeout that fires deterministically and behaves correctly by execution class, without ever letting a late result corrupt a slot or scratch.

Concrete pieces of work:
- Start the per-attempt timeout when the attempt begins executing (after admission), using the framework's isolated timer machinery (C13) so a saturated task pool cannot disable it.
- For an **await-bound** attempt that exceeds its timeout: drop its future (true cancellation) and release its permit immediately; record the node `timed-out`.
- For a **blocking or compute** attempt that exceeds its timeout: mark the node `timed-out` immediately (decide its fate and emit the events now), but leave the still-running closure as abandoned-but-running work whose permit stays held until the closure returns (C12).
- Bar a timed-out attempt from ever filling its output slot and from ever writing scratch — whatever it computes after the timeout is discarded.
- Defer any retry of a timed-out blocking/compute node until the previous attempt's closure has actually returned, so the task instance never runs concurrently with its own zombie (C1 exclusivity).
- Classify timeout as a retry-eligible outcome by default, subject to the node's retry budget (the retry loop itself lands in T22; this ticket only ensures timeout enters that path correctly).
- Ensure the terminal state is decided exactly once: a blocking timeout is and stays `timed-out` and never becomes `abandoned` (that state arises only on the cancellation path, C16).
- Emit exactly one attempt-outcome record for a timed-out attempt, alongside its per-transition events (C19), and surface the timeout as a timed-out attempt outcome consistent with the T20 event contract.

## Test plan (write these first — TDD)
Each scenario is independently checkable. Use a pinned/fake clock or short real timeouts and the deterministic capacity pins from C12 so timing is reproducible in CI.

- **Await-bound attempt exceeds its timeout → cancelled immediately.** Setup: a task declared await-bound whose work awaits far longer than its configured per-attempt timeout, admitted with a known permit cost. Action: run the attempt. Expected: the node reaches `timed-out`; the attempt's future is observed to be dropped (the awaited work does not run to completion); the permit is released at the moment of timeout (permit ledger shows the cost returned immediately, no residual counted).

- **Blocking attempt exceeds its timeout → marked immediately, permit held until return.** Setup: a task declared blocking whose synchronous work sleeps well past its per-attempt timeout on a coordination signal the test controls; a known permit cost. Action: run the attempt and, at timeout, inspect state before releasing the signal. Expected: the node is `timed-out` immediately and the timed-out event is emitted immediately; the permit remains counted against the pool while the closure is still running; after the test releases the closure so it returns, the permit is released and never before.

- **Compute attempt exceeds its timeout → same held-permit semantics as blocking.** Setup: a task declared compute-bound whose work spins/blocks past its timeout on a test-controlled gate. Action: run the attempt, observe at timeout, then release the gate. Expected: identical observable behaviour to the blocking case — `timed-out` marked immediately, permit held until the closure returns.

- **Late result of a timed-out attempt never fills the slot.** Setup: a blocking task that, after its timeout has already fired, finishes and produces a value. Action: let the closure return with a value after the node is already `timed-out`. Expected: the output slot is never filled by that value; any downstream consumer sees the node as `timed-out`, not `succeeded`; the value is discarded.

- **Late result of a timed-out attempt never writes scratch.** Setup: a blocking task that, after its timeout, attempts to write a scratch checkpoint. Action: let the post-timeout closure run its scratch write. Expected: no scratch value attributable to that abandoned attempt is persisted for the node; the scratch namespace for the node is unchanged by the post-timeout write.

- **Retry of a timed-out blocking node is deferred past zombie return.** Setup: a blocking task with a retry budget that timeouts on its first attempt while its closure is still running, with the test holding the closure open. Action: observe whether a second attempt begins before the first closure returns, then release the first closure. Expected: no second attempt of the same node starts while the first closure is still running; the retry begins only after the first closure has returned. (Exclusivity — C1 — is never violated.)

- **Terminal state is decided exactly once.** Setup: a blocking task that timeouts and whose leftover thread later returns (or the run reaches natural end while it is still running). Action: drive the attempt to timeout and let the thread linger, then return. Expected: the node's terminal state is `timed-out` and stays `timed-out`; it never transitions to `abandoned`; the leftover thread is not recorded as a second terminal state (any zombie-at-exit recording is an event, not a state change — C19).

- **Timeout is retry-eligible by default.** Setup: a task with a nonzero retry budget whose first attempt times out but whose subsequent attempt would succeed. Action: classify the timed-out outcome. Expected: the outcome is classified retry-eligible (it enters the retry path rather than terminating the node immediately), consistent with the node's remaining budget; a timeout on the final permitted attempt yields terminal `timed-out`.

- **Exactly one attempt-outcome record for a timed-out attempt.** Setup: any task that times out. Action: run the attempt and read the event stream. Expected: precisely one attempt-outcome record exists for that attempt, marked as a timeout, alongside the expected per-transition events (attempt started, node terminal) — no duplicate and no missing outcome record.

- **A well-behaved attempt within its timeout is unaffected.** Setup: an await-bound task and a blocking task that each complete comfortably inside their per-attempt timeout. Action: run each attempt to completion. Expected: each fills its slot and reaches `succeeded`; no timeout event is emitted; permits release on the normal terminal path exactly as in T20.

- **The timeout fires even when task workers are saturated (safety-machinery isolation).** Setup: a blocking task whose timeout is short while other task workers are kept busy. Action: let the attempt exceed its timeout while task threads are occupied. Expected: the timeout still fires and the node is marked `timed-out` — the timer runs on the framework's isolated machinery (C13) and is not gated on task-worker availability. (Full class-dispatch isolation is asserted in T33; this scenario confirms the timeout path does not regress it.)

## Definition of done
- [ ] A per-attempt timeout is started when the attempt begins executing (after admission) and is honoured for every execution class.
- [ ] An await-bound attempt exceeding its timeout is truly cancelled (its future is dropped) and its permit is released immediately.
- [ ] A blocking or compute attempt exceeding its timeout is marked `timed-out` immediately (fate decided, timed-out event emitted) while its permit is held until the closure actually returns, matching C12's abandoned-but-running accounting.
- [ ] Each of the two class behaviours (await-bound immediate release; blocking held-until-return with retry starting only after return) is verified by a separate test.
- [ ] A timed-out attempt can never fill its output slot and can never write scratch; a late result is discarded — verified by test.
- [ ] A retry of a timed-out blocking/compute node is deferred until the previous attempt's closure has returned, preserving C1 exclusivity — verified by test.
- [ ] Timeout is classified retry-eligible by default, subject to the node's retry budget; a timeout on the last permitted attempt yields terminal `timed-out`.
- [ ] A node's terminal state is decided exactly once: a blocking timeout is and stays `timed-out` and never becomes `abandoned`; a lingering thread is recorded only as a zombie event (C19), never as a second terminal state.
- [ ] Every timed-out attempt produces exactly one attempt-outcome record in the event stream, alongside its per-transition events (C19).
- [ ] The timeout runs on the framework's isolated timer machinery so a saturated task pool cannot disable it (consistent with C13); this ticket does not regress that isolation.
- [ ] Public items added or changed carry rustdoc; timeout semantics per class are documented on the relevant runner API.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The retry loop itself — max-attempts counting, jittered exponential backoff, and cap — is T22; this ticket only ensures a timeout is classified into the retry path correctly.
- Panic containment (T23), cooperative cancellation and grace/`abandoned` classification (T35/C16), and the run-loop driver (T24) are separate tickets; this ticket does not implement the cancellation path or the run-level shutdown budget.
- The full permit-release outcome matrix and its capacity-invariant assertions across all outcomes are T37; here only the two timeout outcomes are exercised against the ledger, not the whole matrix.
- Admission-pool construction and permit lifecycle mechanics are T31/C12; this ticket consumes the permit release/hold contract rather than defining pool sizing or acquisition.
- Execution-class dispatch and starvation/isolation guarantees are T33/C13; this ticket relies on the isolated timer but does not build the dispatcher.
- The C5 policy struct that will own the timeout value in M2 (T30) is out of scope; the interim M1 timeout knob suffices here.
- No scheduler, distributed timeout coordination, cross-process capacity coordination, or runtime graph reshaping is introduced — a timeout marks one node in one process and never spawns or removes nodes.
