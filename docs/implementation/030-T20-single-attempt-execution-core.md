# 030 · T20 — C14: single-attempt execution core

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C14
> **Branch:** `feat/t20-single-attempt-execution-core` · **Depends on:** T16, T17, T19 · **Blocks:** T21, T22, T23, T24, T33, T45

## Why / context
C14 (arch.md `### C14 · Attempt runner`) is the component that wraps a node's work in all its operational behavior. This ticket builds the load-bearing spine of that runner — running *one* attempt end to end — on top of the run context (C8/T16), the output slot (C10/T17), and the event-stream writer (C19/T19). It deliberately implements only the single-attempt happy-and-classify path so the run-loop driver (T24) and the execution-class dispatch (T33) have a runner to call; the operationally hard behaviors are split into siblings that build on this core: per-attempt timeout and abandonment (T21), retry with jittered backoff (T22), and panic containment (T23). Getting the span, the outcome classification, the slot fill, and the exactly-once attempt-outcome record right here is what those tickets extend rather than rewrite.

## Objective
Build the runner for a single attempt of one node: it opens a span, records the waiting and admission phases, dispatches the work, awaits its result, classifies the outcome into the normative taxonomy, fills the output slot on success, and emits exactly one attempt-outcome record (alongside the per-transition events) for every attempt regardless of outcome.

Concrete pieces of work:
- A single-attempt runner entry point that takes a node's registered work plus its C8 run context and drives one attempt to a decided outcome.
- Span setup per attempt carrying run, node, and attempt identity (the C25 span surface consumed later by T45), opened before the work runs so everything beneath it is attributable.
- Emission of the ordered per-transition events for the attempt — attempt started, and then attempt succeeded or attempt failed — through the C19 writer (T19), plus a run-through of the node-admitted / node-ready phase markers this runner is responsible for.
- Outcome classification distinguishing the outcomes reachable *in this ticket*: success (a returned value), permanent (retry-ineligible) failure, retry-eligible failure, and deliberate skip. The classifier must present a stable shape that T21 (timeout), T22 (retry), and T23 (panic) plug their additional outcome variants into without reshaping it.
- On a successful outcome only, fill the node's single once-writable output slot (C10/T17) with the produced value, transferring declared output residency to the slot at fill time.
- Emit exactly one attempt-outcome record per attempt through the C19 writer, carrying run/node/attempt identity and the classified outcome, for every attempt including ones that end in failure or skip.
- Ensure the attempt number and maximum from the C8 context are the identity used in the span, the events, and the attempt-outcome record.

## Test plan (write these first — TDD)
Each scenario is independently checkable. Use a hand-constructed C8 run context (C8 acceptance: constructable in a unit test with no runtime running) and an in-memory or capturing C19 sink so the emitted records can be asserted against.

- **Successful attempt fills the slot.** Setup: a node whose work returns a value, a fresh empty output slot, and a capturing event sink. Action: run one attempt. Expected: the slot is now filled with exactly that value; the outcome classifies as success; the sink contains an attempt-started event followed by an attempt-succeeded event; the node's slot was empty before the run and filled after.

- **Exactly one attempt-outcome record on success.** Setup: as above. Action: run one attempt to success. Expected: exactly one attempt-outcome record exists in the stream for this attempt, carrying the run, node, and attempt identity, with an outcome field reading success.

- **Permanent failure does not fill the slot.** Setup: a node whose work returns a classified permanent (retry-ineligible) error, an empty slot, a capturing sink. Action: run one attempt. Expected: the slot remains empty; the outcome classifies as permanent failure; the sink contains an attempt-started then an attempt-failed event; exactly one attempt-outcome record is present, marked as a failure outcome.

- **Retry-eligible failure is classified distinctly and does not fill the slot.** Setup: a node whose work returns a classified retry-eligible error. Action: run one attempt. Expected: the slot remains empty; the outcome classifies as retry-eligible (distinct from permanent) so the retry driver (T22) can act on it; exactly one attempt-outcome record is emitted. This runner schedules no retry itself — it only classifies and reports.

- **Deliberate skip is classified as an originated skip and does not fill the slot.** Setup: a node whose work returns a deliberate skip. Action: run one attempt. Expected: the slot remains empty; the outcome classifies as a deliberate (originated) skip, distinct from both success and failure; exactly one attempt-outcome record is emitted.

- **Every outcome yields exactly one attempt-outcome record.** Setup: parametrize over the four reachable outcomes (success, permanent failure, retry-eligible failure, skip). Action: run one attempt for each. Expected: in every case the stream contains exactly one attempt-outcome record for the attempt — never zero, never two.

- **Attempt number is carried through identity.** Setup: a context whose current attempt number is set to a non-first value with a defined maximum. Action: run one attempt. Expected: the span, the per-transition events, and the attempt-outcome record all carry that same attempt number and maximum; a run under attempt number one carries one.

- **The span opens before the work runs.** Setup: work that records whether a span carrying this node's identity was active at the moment it executed. Action: run one attempt. Expected: the work observed the attempt span active, so any line it (or a library it calls) emits is attributable without correlating timestamps.

- **Event ordering within an attempt.** Setup: a capturing sink that preserves order and sequence numbers. Action: run one successful attempt. Expected: the attempt-started event precedes the attempt-succeeded event, which precedes (or accompanies) the attempt-outcome record, and the C19 sequence numbers are gapless across them.

- **Value type flows unchanged.** Setup: a node producing a non-trivial typed value and a consumer slot typed to it. Action: run one attempt and read the slot. Expected: the value read from the slot equals the produced value with no type coercion, confirming the fill path preserves the slot's type contract (C10).

## Definition of done
- [ ] The runner drives one attempt for a node in the order: open span, record the waiting/admission phase markers, dispatch the work, await the outcome, classify it, then fill the slot on success or report the classified failure/skip (per C14 Behavior).
- [ ] Classification distinguishes the outcomes reachable in this ticket — success, permanent (retry-ineligible) failure, retry-eligible failure, and deliberate skip — with a stable shape the timeout, panic, and abandonment variants extend in T21/T23 without reshaping.
- [ ] A permanent (retry-ineligible) error produces a failure outcome and is never treated as retry-eligible by the classifier (C14: a permanent error is not retried regardless of remaining attempts — this runner surfaces that distinction).
- [ ] A retry-eligible error produces a distinct retry-eligible outcome so the retry driver (T22) can act on it; this runner schedules no retry and applies no backoff.
- [ ] On success, and only on success, the node's single once-writable output slot is filled with the produced value, and declared output residency transfers to the slot at fill (C10/T17).
- [ ] On any non-success outcome the slot is left empty (C14: an abandoned or non-succeeding attempt never fills a slot).
- [ ] Every attempt produces exactly one attempt-outcome record in the event stream, alongside its per-transition events, including attempts that fail or skip (C14 acceptance).
- [ ] The per-transition events (attempt started, attempt succeeded / attempt failed) are emitted through the C19 writer (T19) in order, with gapless sequence numbers and correct run/node/attempt identity on every record.
- [ ] Each attempt runs inside a span carrying run, node, and attempt identity, opened before the work runs, so output beneath it is attributable (the C25 surface T45 consumes).
- [ ] The attempt number and maximum come from the C8 context and appear identically in the span, events, and attempt-outcome record; the runner works on the first attempt of the first node (C8 acceptance).
- [ ] The single-attempt runner is callable with a hand-constructed context and a capturing sink, with no runtime and no run-loop driver present, so it is unit-testable in isolation.
- [ ] All Test plan scenarios above are implemented as tests and pass.
- [ ] Out-of-scope behaviors are explicitly deferred: no timeout, no abandonment/zombie handling, no retry, no backoff, no panic catching, and no execution-class dispatch logic are added here — the runner accepts already-dispatched work.
- [ ] Public items carry rustdoc; the runner's outcome-classification contract documents which outcome variants are populated by this ticket versus reserved for T21/T22/T23.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Per-attempt timeout and abandonment (T21).** No timer is started or enforced here; no await-bound future is dropped, no blocking/compute attempt is marked timed-out, no abandoned-but-running (zombie) accounting, and no barring of late results from slots or scratch. This runner assumes the work returns.
- **Retry with jittered exponential backoff (T22).** This runner classifies an outcome as retry-eligible but never loops, never schedules a second attempt, and computes no backoff delay. "For each attempt in turn" from C14 Behavior is realized by T22 driving this core repeatedly.
- **Panic containment (T23).** No `catch_unwind` / `AssertUnwindSafe` boundary, no panic hook, no `panic = "abort"` startup refusal, no task-local panic attribution.
- **Execution-class dispatch (C13/T33).** The runner receives work that is already on the correct thread/runtime; it does not choose the class, spawn onto the blocking or compute pool, or interact with tokio placement.
- **Run-loop driver (T24).** Admission, readiness feedback, run-started/run-finished events, run identity minting, and the terminate-when-nothing-pending logic belong to T24; this ticket provides only the per-attempt callee.
- **Admission and permit accounting (C12).** Acquiring or releasing capacity permits is not this runner's job; the phase markers it records are informational.
- **Failure propagation and terminal-state assignment for non-executed nodes (C15/C11).** Deciding `upstream-failed` / `upstream-skipped` and propagating skips are the readiness tracker's and failure policy's concern.
- **Scope-boundary reminder.** dagr is not a scheduler or distributed executor: this runner runs one attempt of one node on this machine, influences no scheduling, and never mutates the graph shape at runtime.
