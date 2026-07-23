# 032 · T22 — C14: retry with jittered exponential backoff

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C14
> **Branch:** `feat/t22-retry-with-backoff` · **Depends on:** T20 · **Blocks:** T28, T29

## Why / context
The single-attempt execution core (T20) runs a node exactly once, classifies the outcome, and fills the slot on success. This ticket wraps that core in a retry loop so the attempt runner (C14) becomes what its spec describes: "either fill the slot, schedule another attempt after a backoff, or reach a terminal failure." The governing behavior is C14 (`arch.md` "C14 · Attempt runner"), specifically its retry-eligibility classification, per-node retry budget, and the acceptance criteria on bounded retries, permanent-error handling, one attempt-outcome record per attempt, and jittered backoff that prevents fan-out resynchronization. A deliberately small, self-contained retry knob is introduced here as an interim M1 surface; in M2 it migrates into the C5 node-policy struct (that migration is T29's concern, which this ticket blocks). This ticket does not add timeout semantics (T21) or permit accounting for zombies (C12/T31) — those are separate — but it must respect the C1-exclusivity rule that a retry may not start while a prior attempt is still abandoned-but-running.

## Objective
Turn the single-attempt runner into a bounded retry loop driven by classification and a jittered exponential-backoff schedule, exposing a minimal per-node retry configuration that later folds into C5 policy.

Concrete pieces of work:
- A per-node interim retry configuration carrying: maximum attempt count (default: no retries — a single attempt), base backoff delay, exponential growth multiplier, jitter, and a maximum backoff cap.
- A retry loop in the C14 runner that, after each failed attempt, consults the outcome classification and either schedules another attempt after a backoff or terminates the node.
- Classification-gated retry: only outcomes classified as retry-eligible failure consume the retry budget and trigger a backoff; permanent failure, deliberate skip, and success end the loop immediately with no further attempts.
- A backoff schedule that is exponential in the attempt index, clamped to the configured cap, and jittered so that two nodes entering backoff at the same instant do not wake in lockstep.
- Correct wiring of the current attempt number and the maximum into whatever the runner already exposes to the attempt (the run context's attempt-number/max fields, C8), so a task can observe which attempt it is on.
- Emission of exactly one attempt-outcome record per attempt through the event stream (C19), including the final failing attempt, and a distinct backoff/waiting phase recorded between attempts (feeding the C23 phase timings — this ticket only needs the backoff phase to be a named, measurable interval, not the full metrics surface).
- Deferral of the next attempt until the prior attempt's work has actually returned (C1 exclusivity / abandoned-but-running rule) — the loop must not launch attempt N+1 while attempt N's closure is still running.

## Test plan (write these first — TDD)
Each scenario is independently checkable. Use a test-controlled clock/time source and a deterministic (seedable or injected) jitter source so backoff timing is assertable without wall-clock flakiness. Use a pinned/deterministic capacity configuration where admission is involved.

- **Retry-eligible error is retried up to the budget and no further.** Setup: a task configured with a maximum of 3 attempts whose work returns a retry-eligible error every time. Action: run the node through the C14 runner. Expected: the work is invoked exactly 3 times, the node reaches a terminal failure state, and no 4th attempt occurs.

- **A single successful retry stops the loop.** Setup: a task with a maximum of 3 attempts whose work returns a retry-eligible error on attempt 1 and succeeds on attempt 2. Action: run the node. Expected: the work is invoked exactly twice, the slot is filled with the successful value, and no 3rd attempt occurs.

- **Permanent error is never retried, regardless of remaining budget.** Setup: a task with a maximum of 5 attempts whose work returns a permanent (non-retry-eligible) error on attempt 1. Action: run the node. Expected: the work is invoked exactly once, the node reaches a terminal failure, and no backoff is scheduled.

- **Deliberate skip is not retried.** Setup: a task with a maximum of 5 attempts whose work returns a deliberate-skip outcome on attempt 1. Action: run the node. Expected: the work is invoked exactly once, the node reaches its skip terminal state, and no backoff is scheduled and no further attempt occurs.

- **Default configuration performs exactly one attempt.** Setup: a node with the interim retry configuration left at its default (no retries) whose work returns a retry-eligible error. Action: run the node. Expected: the work is invoked exactly once and the node fails, proving the conservative default is "no retries."

- **Backoff is exponential and capped.** Setup: a task that always returns retry-eligible errors, configured with a known base delay, growth multiplier, cap, and jitter disabled (or pinned to zero). Action: run through the maximum attempts and record the delay scheduled before each retry using the injected clock. Expected: successive delays grow by the multiplier from the base, and every delay is clamped to be no greater than the configured cap (later delays sit exactly at the cap).

- **Backoff is jittered — a fan-out does not resynchronize.** Setup: a group of N identical always-retry nodes started at the same instant with jitter enabled and a seeded jitter source producing distinct draws. Action: enter the first backoff for all N nodes and record each node's scheduled wake delay. Expected: the wake delays are not all identical (the spread is nonzero), demonstrating that simultaneous retries do not resynchronize; each delay still lies within the jitter window around the nominal exponential value and never exceeds the cap.

- **Exactly one attempt-outcome record per attempt.** Setup: a task with a maximum of 3 attempts that fails retry-eligibly twice then succeeds, wired to a capturing event sink. Action: run the node and collect the stream. Expected: exactly 3 attempt-outcome records appear, one per attempt, with monotonically increasing attempt numbers, the first two marked as retry-eligible failures and the last as success — and the terminal node record appears once.

- **Attempt number is visible to the task.** Setup: a task whose work records the attempt-number and maximum it observes from its context, failing retry-eligibly until the last configured attempt. Action: run the node with a maximum of 3. Expected: the recorded attempt numbers are 1, 2, 3 in order, each paired with the maximum of 3.

- **Backoff phase is a named, measurable interval.** Setup: a task that fails once retry-eligibly then succeeds, run against the injected clock. Action: inspect the attempt records' phase timings. Expected: the interval between the first failed attempt and the second attempt start is attributed to the backoff/waiting phase (distinct from executing), computed from monotonic offsets, and equals the scheduled backoff delay.

- **No resynchronization of the underlying execution (no premature re-entry).** Setup: a blocking-style task whose work signals when it has entered and blocks until released, configured to retry. Action: trigger a retry-eligible outcome for attempt 1 and observe whether attempt 2's work starts before attempt 1's closure has returned. Expected: attempt 2's work never begins until attempt 1's closure has returned — the same task instance is never running concurrently with a prior attempt (C1 exclusivity).

## Definition of done
- [ ] A retry-eligible error is retried up to the configured maximum attempt count and no further (verified).
- [ ] A permanent error is not retried, regardless of remaining attempts (verified).
- [ ] A deliberate skip is not retried and ends the loop at its skip terminal state.
- [ ] Timeout classification remains retry-eligible-by-default so that when T21 lands its outcome flows through this loop unchanged (no code here contradicts that default).
- [ ] Backoff is exponential in the attempt index, jittered, and clamped to the configured cap.
- [ ] Backoff delays are jittered such that a fan-out of simultaneous retries does not resynchronize (verified by the multi-node spread test).
- [ ] Every attempt produces exactly one attempt-outcome record in the event stream (C19), including the final failing attempt; attempt numbers are gapless and increasing within the node.
- [ ] The backoff interval between attempts is recorded as a named, monotonic-offset-derived phase distinct from executing time.
- [ ] The current attempt number and the maximum are exposed to the task through its run context (C8) and reflect the actual attempt.
- [ ] The next attempt is not started until the prior attempt's closure has actually returned, preserving C1 exclusivity (no concurrent same-instance execution).
- [ ] An interim per-node retry configuration (max attempts, base, multiplier, jitter, cap) exists with a conservative default of no retries; its shape is documented as an interim M1 surface that will migrate into C5 node policy in M2 (T29).
- [ ] All backoff timing tests are deterministic via an injected clock and a seedable/injectable jitter source (no wall-clock sleeps in tests).
- [ ] Rustdoc on the retry loop and the interim configuration explains the classification gating, the cap/jitter semantics, and the planned migration into C5.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Per-attempt timeout and per-class cancellation/abandonment semantics — that is T21 (C14); this ticket only ensures timeout outcomes will flow through the retry loop once they exist.
- Panic containment and the `panic = "abort"` startup refusal — that is T23 (C14).
- Permit accounting, the admission pools, and zombie-cost bookkeeping — that is C12 (T31); this ticket only respects the "defer retry until the closure returns" rule, it does not implement permit release or capacity invariants.
- Folding the interim retry knob into the full C5 node-policy struct, defaults hashing, and graph-artifact disclosure of the effective policy — that is T29 (C5), which this ticket blocks.
- Full C23 node-metrics surface and the complete named-phase set — only the backoff phase interval is needed here.
- Failure propagation and trigger-rule handling for what happens to the rest of the run after a node's terminal failure — that is C15 (T34).
- The run-loop driver that schedules ready nodes and drives many nodes concurrently — that is T24; this ticket operates at the single-node runner level.
- Any temptation to make backoff a scheduler feature (global retry queues, cross-run retry policy, backfill of failed nodes). dagr is not a scheduler or a backfill orchestrator; retry is strictly a per-node, in-run concern, and the graph shape never changes because a node retries.
