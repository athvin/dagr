# 038 · T28 — M1 demo: three-node chain with retry

> **Milestone:** M1 · **Size:** M · **Type:** feature (demo) · **Components:** M1 gate
> **Branch:** `feat/t28-m1-demo-three-node-chain` · **Depends on:** T12, T15, T21, T22, T23, T25, T26, T27 · **Blocks:** T65

## Why / context
This is the **M1 gate**: the spec's "It runs" done-when, executed in CI. arch.md's **Build order** states M1 is *done when a three-node chain executes in order, one node fails and retries successfully, and the event stream shows every transition* — and *nothing else exists yet (no artifacts, no admission control, no CLI)*. This ticket builds that end-to-end example on top of the now-complete M1 stack (task abstraction, handles, binding, assembly, output slots, readiness, the run-loop driver, the attempt runner with timeout/retry/panic containment, and the `C19 · Event stream` writer) and proves it holds by walking the emitted stream and asserting every transition. It is a *proof of integration*, not new framework surface — it exercises the pieces the earlier M1 tickets built rather than adding capability. It blocks the system acceptance gate (T65), which folds this demo into the criteria matrix as the M1 done-when.

## Objective
Build a small, self-contained example pipeline and a test that runs it end-to-end in CI, demonstrating that the M1 stack executes a linear chain in order, recovers from a transient failure via retry, and records every state transition in the event stream — with an event-stream walker that asserts the exact, ordered sequence of transitions.

Concretely, the work is:

- **A three-node chain pipeline.** Three nodes wired head-to-tail by typed data dependencies (`A → B → C`), so ordering is enforced by data flow and each node's output is the next node's input. Node names are explicit and stable.
- **A deterministically-flaky middle node.** Node `B` fails exactly once with a **retryable** error on its first attempt and succeeds on its second, using test-controllable state (for example an attempt counter captured at construction) so the flakiness is deterministic in CI, never time- or randomness-dependent. Nodes `A` and `C` succeed on their first attempt. `B`'s node policy grants at least two attempts so the retry (T22) is permitted.
- **A run harness that drives the pipeline to completion** through the M1 run-loop driver (T24) against an injected event-stream sink that the test can read back, minting a run identity and opening store+stream before assembly, exactly as the real run path does.
- **An event-stream walker** — a reusable reader helper that parses the stream into records and lets the test assert on the ordered sequence of transitions per node and for the run. This walker is the observable oracle for the whole demo and is written to be reused by later milestone demos.
- **Assertions that every transition appears, in order:** run-started; for each node ready → admitted → attempt-started → attempt-outcome → node-terminal; `B`'s first attempt-outcome is a *retryable failure* and its second is *success*, with two attempt-outcome records for `B` and one each for `A` and `C`; each node ends `succeeded`; run-finished last. Sequence numbers are gapless and strictly increasing; the run terminates exactly when nothing is pending or in flight.
- **CI wiring** so the demo runs as a normal test on every push to the branch and is the artifact T65 will point at as the M1 done-when.

## Test plan (write these first — TDD)

**1. The chain executes in dependency order.**
Setup: the three-node chain (`A → B → C`) wired by data dependencies, each node recording the wall/offset at which its attempt started via the captured stream. Action: run the pipeline to completion through the driver. Expected: `A`'s successful attempt precedes `B`'s first attempt, which precedes `C`'s attempt, as ordered by monotonic offset (never wall clock); no downstream node starts before its upstream reached a success-like terminal state.

**2. The middle node fails once, then retries to success.**
Setup: `B` configured to fail with a **retryable** error on attempt 1 and succeed on attempt 2, with a max-attempts policy of at least two; `A` and `C` succeed first try. Action: run the pipeline. Expected: `B` produces exactly two attempt-outcome records — the first a retryable failure, the second a success — and reaches terminal state `succeeded`; `A` and `C` each produce exactly one attempt-outcome (success) and end `succeeded`; the run's overall outcome is success.

**3. The event stream shows every transition, in order.**
Setup: the run from scenario 2, its stream read back through the event-stream walker. Action: walk the stream and collect the ordered transition events. Expected, in order: `run-started` first; then for `A` the sequence node-ready → admitted → attempt-started → attempt-succeeded → node-terminal(`succeeded`); then for `B` node-ready → admitted → attempt-started → attempt-failed(retryable) → (backoff) → attempt-started → attempt-succeeded → node-terminal(`succeeded`); then for `C` node-ready → admitted → attempt-started → attempt-succeeded → node-terminal(`succeeded`); `run-finished` last. Every expected transition is present exactly once (except `B`'s two attempt cycles) and none is missing.

**4. The run-started event fully identifies the run.**
Setup: the recorded stream. Action: read the first record. Expected: it is `run-started` and carries the full run-artifact header known at start — run identity, pipeline identity, schema version — everything but the overall outcome and summary; so a stream truncated to just this record still identifies its run completely (C19).

**5. Sequence numbers are gapless and strictly increasing.**
Setup: the recorded stream. Action: read all records in order and inspect their sequence numbers and run identities. Expected: sequence numbers start at the run's first record, increase by exactly one with no gaps and no duplicates, and every record carries the same run identity and a schema version (C19).

**6. Durations are computed from monotonic offsets, not wall clocks.**
Setup: the recorded stream. Action: for `B`, derive the elapsed time between its first attempt-started and its terminal event using the authoritative monotonic offset field. Expected: the derived duration is non-negative and consistent with the recorded offsets; the wall-clock stamp is treated as informational only and is never used to order or measure (C19).

**7. The run terminates exactly when nothing is pending or in flight.**
Setup: the full run. Action: observe the driver to natural completion. Expected: `run-finished` is emitted once, after `C`'s terminal event; the process/driver returns rather than hanging; no zombie-at-exit events appear because every attempt's closure returned before run end (this demo has no timed-out or abandoned work).

**8. Every node ends in exactly one terminal state from the taxonomy.**
Setup: the recorded stream. Action: for each of `A`, `B`, `C`, collect node-terminal events. Expected: each node has exactly one node-terminal event, each is `succeeded`, and no node has zero or two terminal events — the single-terminal-state invariant holds for the whole run.

**9. The demo is deterministic and reproducible in CI.**
Setup: the same example run twice in the same process (or two runs of the test). Action: run and walk twice. Expected: both runs produce the same ordered sequence of transitions and the same per-node attempt counts (`B` retries exactly once each time) — the flakiness is driven by a deterministic counter, not by timing, sleeps beyond backoff, or randomness, so CI never flakes.

**10. The event-stream walker is a reusable oracle.**
Setup: the walker helper used by the assertions above. Action: invoke it against the recorded stream and against a deliberately-truncated copy (a valid prefix with the final record removed). Expected: on the complete stream it returns the full ordered transition set; on the truncated stream it parses every complete record and reports the missing tail rather than panicking — demonstrating the walker tolerates the ≤1 trailing-partial guarantee (C19) and is fit for reuse by later demos (T38, T49, T63).

## Definition of done
Component acceptance criteria realized by this demo (from arch.md **Build order** M1 and `C19 · Event stream`):

- [ ] A three-node chain (`A → B → C`) executes strictly in dependency order, driven by the M1 run-loop driver, with no CLI, no artifacts, and no admission control involved (the M1 boundary is respected — nothing from a later milestone is pulled in).
- [ ] The middle node fails exactly once with a retryable error and then retries to success, demonstrating the T22 retry path end-to-end; the other two nodes succeed on first attempt.
- [ ] The event stream shows **every** state transition for the run: run-started, and per node node-ready → admitted → attempt-started → attempt-outcome → node-terminal, with `B` showing two attempt cycles (failure then success), and run-finished last.
- [ ] Every attempt produces exactly one attempt-outcome record (`A`: 1, `B`: 2, `C`: 1), alongside its per-transition events (C19).
- [ ] The `run-started` record carries the full run-artifact header known at start (run identity, pipeline identity, schema version — everything but overall outcome/summary), so the run is identifiable from that record alone (C19).
- [ ] Every record carries the run identity and a schema version; sequence numbers are gapless and strictly increasing within the run (C19).
- [ ] Durations in the walker are computed from the authoritative monotonic offset field, never from the informational wall-clock stamp (C19).
- [ ] Each of the three nodes ends in exactly one terminal state (`succeeded`), and the run ends exactly when nothing is pending or in flight (C19 / M1 done-when).
- [ ] The stream is written through the injected run-store sink and read back by the test through the same sink abstraction, matching the real run path (identity minted and stream opened before assembly).

This ticket's concrete deliverables:

- [ ] An example three-node-chain pipeline (source-controlled example, not throwaway) with a deterministically-flaky middle node whose failure/retry is driven by test-controllable state, not timing or randomness.
- [ ] A reusable **event-stream walker** helper that parses a recorded stream into ordered transition records and supports per-node and per-run assertions, tolerating a ≤1 trailing-partial record.
- [ ] A CI-run test that drives the example to completion and asserts the full ordered transition sequence, the retry-then-success on `B`, gapless sequence numbers, single-terminal-state per node, and run termination — the executable M1 done-when.
- [ ] The demo runs as a standard test in CI on every push to the branch, is deterministic (no flakiness), and is referenceable by the T65 acceptance gate as the M1 milestone demo.
- [ ] Rustdoc on the example and the walker helper explaining what M1 behaviour each demonstrates.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None were listed. Two decisions surfaced while building the demo against the
merged M1 stack, resolved here as prose (no new open question):

- **The retry surfaces *two* `attempt-failed` records on the raw C19 stream, not
  one.** The closed C19 event vocabulary has no dedicated `backoff` event, so the
  T22 retry loop's backoff-phase marker (`AttemptEvent::BackoffStarted`, the named
  interval that folds into the run artifact at C22/T42) is mapped by the run-loop
  driver (T24, `write_attempt_event`) onto an `attempt-failed` record, exactly like
  the genuine first-attempt failure. The middle node's ordered transition sequence
  is therefore `node-ready → node-admitted → attempt-started → attempt-failed
  (the real retryable failure) → attempt-failed (the backoff marker) → node-admitted
  → attempt-started → attempt-succeeded → node-terminal(succeeded)`. The demo asserts
  the **real** driver output honestly (two `attempt-failed`, two `attempt-started`,
  one `attempt-succeeded`, one terminal) rather than an idealized single-failure
  shape. This is existing merged behaviour, not a demo change — the demo does not
  patch it.

- **The middle node's input edge opts into clone-on-read.** Because `transform`
  retries (its `NodePolicy` grants a retry), an *owned* input edge into it is an
  assembly error (C1/C3/T0.2 — "an owned edge into a retrying node without
  clone-on-read fails assembly"). The demo wires `source.clone_on_read()` into
  `transform`, the honest authoring pattern for a node whose attempts must each see
  a fresh input — exercising the public binding API as a user would, not working
  around the check.

**Coverage-matrix note (matrix contract / quality-gates §3):** T28 maps **no new**
row in `docs/coverage-matrix.md`. It is the executable **M1 done-when** (arch.md
Build order), an integration proof that composes the already-merged M1 pieces; the
component criteria it exercises (C7, C8, C10, C11, C14, C19) are already mapped to
their owning tickets' unit suites, and the M1 system-level facets (`SL1`–`SL7`) are
already `unmapped`/deferred to their owning tickets (T64, T12, T27, T41, T62, T43,
T61, T65) — none is `Covered-by T28`. The T65 acceptance gate is the ticket that
folds this demo into the system-level matrix as the M1 done-when (T28 **blocks**
T65). Mapping a row to this demo would double-map a criterion the verifier already
binds elsewhere, so the honest action is no change, recorded here.

## Out of scope
- **Any M2+ capability.** No admission control / memory pools (C12–C13), no failure-policy variants beyond what the linear chain needs (C15), no cancellation, grace periods, or `abandoned` state (C16). The middle node fails *retryably and recovers*; it does not exercise timeout or abandonment paths.
- **Artifacts.** No graph artifact (C20) or run artifact (C22) production or folding — M1 explicitly has no artifacts. The demo asserts against the raw event stream only, not a folded artifact.
- **CLI verbs.** No run/validate/render/fold/prune verbs (C26). The demo drives the pipeline through the in-process run harness, not a command-line binary.
- **Re-testing lower components in isolation.** Timeout (T21), retry mechanics (T22), panic containment (T23), crash-safety fault injection (T27), the termination property test (T25), and the bounded-memory chain (T26) are already covered by their own tickets; this ticket integrates them and must not duplicate their unit-level coverage.
- **New framework surface.** This is an integration demo. Any temptation to add capability to make the demo pass belongs in the owning component's ticket, not here.
- **Scope-boundary temptations.** No scheduler deciding *when* the chain runs, no distributed or multi-machine execution, no metadata store, no runtime graph mutation — the chain's shape is fixed at assembly and never changes at runtime. Deciding *when* to trigger the demo is CI's job (bring your own trigger), not the framework's.
