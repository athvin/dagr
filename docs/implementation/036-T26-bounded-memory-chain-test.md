# 036 · T26 — C10: bounded-memory chain test

> **Milestone:** M1 · **Size:** S · **Type:** feature (tests) · **Components:** C10
> **Branch:** `feat/t26-bounded-memory-chain-test` · **Depends on:** T17, T24 · **Blocks:** T28

## Why / context
The output slot (C10) promises that a produced value is released once every consumer reaches a terminal state and every consumer's closure has returned, and that "released" means the bytes are returned to the allocator. C10's headline acceptance criterion is that *peak allocator-level memory across a long chain does not grow with the chain's length when nothing is retained* — verified against a synthetic hundred-node chain. This ticket delivers exactly that guard test on top of the slot implementation (T17) and the M1 run-loop driver (T24) so that any future regression that leaks slot residency — for example releasing on last-read instead of terminal-and-returned, or forgetting to drop the value — is caught before the M1 demo (T28) is trusted. The critical constraint, resolved in the task record, is that the measurement is taken with an **instrumented allocator, not process RSS**, because RSS reflects what the OS reclaimed, not what the program returned to its allocator (arch.md C10 memory-accounting note).

## Objective
Prove, with a deterministic automated test, that a long linear pipeline holds only a bounded number of in-flight slot values at a time regardless of chain length, and that the bound does not scale with the number of nodes.

Concrete pieces of work:
- Introduce an instrumented global allocator, gated to test builds only, that records live allocated bytes (current and high-water peak) and can be sampled and reset by the test harness. It must not become the production allocator.
- Build a synthetic hundred-node linear chain where each node consumes only its immediate predecessor's slot, produces a value of a known non-trivial size, retains nothing, and reports a declared output residency.
- Drive that chain to completion through the real M1 run-loop driver (T24) reading real slots (T17), on pinned single-unit capacity so execution is deterministic and admission is serialized enough to make the memory bound observable.
- Assert that the allocator high-water peak observed during the run is bounded by a small constant multiple of a single value's size (a handful of concurrently-live slots), and specifically does **not** grow proportionally to chain length.
- Add a parameterised comparison across at least two chain lengths (for example a short chain versus the hundred-node chain) demonstrating that measured peak stays flat while length grows.
- Add a companion assertion that a chain of the same length with the final node's output marked *retained* leaves that one value counted at run end and redeemable, so the test proves the guard measures the right thing (non-retained releases, retained does not).

## Test plan (write these first — TDD)
Each scenario is independently checkable and derives from a C10 acceptance criterion. All memory figures are allocator-level (from the instrumented allocator), never RSS.

- **Peak is flat across chain length.**
  Setup: construct two chains — one of a small length (for example 4 nodes) and one of 100 nodes — each node producing a value of the same fixed non-trivial size, nothing retained, capacity pinned so pool sizing is deterministic (via the C12 pinning flag / test knob).
  Action: reset the allocator's peak counter, run each chain to completion through the T24 driver, and read the recorded high-water live-bytes for each run.
  Expected outcome: the 100-node run's peak live bytes is within a small constant factor of the 4-node run's peak (they differ by at most a few value-sizes, not by ~25x), demonstrating peak does not grow with length.

- **Peak is bounded by a few concurrent values, not the whole chain.**
  Setup: the 100-node chain with per-value size S known to the test.
  Action: run to completion and read the allocator high-water live bytes attributable to slot values.
  Expected outcome: the peak is bounded by a small constant multiple of S (a handful of simultaneously-live slots — producer output plus the immediate downstream's input during handoff — not 100·S). The exact constant is asserted as an explicit ceiling that a real regression would blow through.

- **Slot value is released after the sole consumer is terminal.**
  Setup: a two-node producer→consumer pair, consumer succeeds on its first (only) attempt, nothing retained.
  Action: run to completion; after the consumer reaches a terminal state and its closure has returned, sample live allocated bytes and compare to a baseline taken before the producer ran.
  Expected outcome: live bytes return to (approximately) the pre-producer baseline — the produced value's bytes are back with the allocator; live bytes did not stay elevated by one value's worth.

- **Released-not-retained values are gone; a retained value survives to run end and is redeemable.**
  Setup: two runs of an identical short chain — one with no node retained, one with the terminal node marked retained.
  Action: run each to completion; after the run ends, sample allocator live bytes and, for the retained run, redeem the retained handle for its value via the post-run redemption API (T17).
  Expected outcome: the non-retained run's end-of-run live bytes are at baseline (nothing lingers); the retained run's end-of-run live bytes are exactly one value higher, the redemption returns the correct value, and the run artifact identifies that value as still-retained while the released ones are not (arch.md C10 acceptance: retained values identified and redeemable, released ones not).

- **Peak measured slot residency is reported.**
  Setup: the hundred-node chain run.
  Action: run to completion and read the run summary from the artifact.
  Expected outcome: the summary carries a non-zero *peak measured slot residency* figure (arch.md C10 / run-artifact summary) that is consistent with the bounded peak asserted above, sitting alongside the declared output residency.

- **Determinism / no flakiness.**
  Setup: any of the above with capacity pinned and a fixed value size.
  Action: run the same scenario repeatedly (the test itself, or under the standard CI repeat).
  Expected outcome: the measured peak and the pass/fail verdict are stable across repetitions — the assertion ceiling has enough margin for allocator bookkeeping noise but is far below the chain-length-proportional figure a leak would produce.

## Definition of done
- [ ] An instrumented allocator that records current and high-water live allocated bytes exists, is confined to test builds (not wired in as the production global allocator), and exposes reset-peak and sample operations to the test harness.
- [ ] A synthetic hundred-node linear chain fixture exists where each node consumes only its predecessor's slot, produces a value of known size, retains nothing, and declares an output residency; it runs to completion on pinned deterministic capacity through the real T24 driver over real T17 slots.
- [ ] Measured allocator peak across the hundred-node chain is bounded by a small constant multiple of one value's size and is asserted to **not** grow with chain length (arch.md C10: peak allocator-level memory across a long chain does not grow with the chain's length when nothing is retained — verified against a synthetic hundred-node chain).
- [ ] A multi-length comparison (short chain vs. hundred-node chain) demonstrates the measured peak stays flat while length grows.
- [ ] A test proves that after the final consumer of a node is terminal and its closure has returned, the node's value is unreachable and its bytes returned to the allocator (arch.md C10 acceptance criterion), measured at allocator level, not RSS.
- [ ] A test proves a value marked retained survives to run end, is redeemable via the T17 post-run redemption API, is identified as still-retained in the run artifact, and that released (non-retained) values are not so identified (arch.md C10 acceptance criteria on retained/released values).
- [ ] A test asserts the run artifact summary reports peak measured slot residency (C10) alongside declared output residency.
- [ ] All memory assertions are stated explicitly as allocator-level residency, and no assertion in this ticket reads process RSS.
- [ ] Tests are deterministic (pinned capacity, fixed value size, stable ceilings with justified margin) and pass under CI repeat.
- [ ] Rustdoc on any new test-only helper/allocator explains that it measures allocator residency, not RSS, and is test-only.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
The ticket declared "None." and `docs/tasks.md`'s T26 entry carries no `Q:`
items; the following implementation decisions arose during the work and are
recorded here per the open-questions duty.

- **Which memory instrument bears the peak assertion?** Resolved: **both**, with
  the exact per-run `ResidencyLedger` peak (a deterministic integer, arch.md
  C10's accounting hook the run artifact folds) as the *load-bearing* assertion,
  and the ticket-required test-only instrumented `#[global_allocator]` (live/peak
  allocated bytes, never RSS) as a corroborating allocator-level restatement. The
  ledger peak is per-run and needs no coordination; the allocator counter is
  process-global, so every test that reads it — or that deliberately allocates a
  chain-length-proportional amount (the non-vacuity leak proof) — takes a
  process-wide serialisation lock so parallel test execution cannot pollute the
  reading. Both are allocator-level; neither reads process RSS, per arch.md C10's
  accounting rule.
- **"Run artifact summary reports peak measured slot residency" (test-plan
  bullet / DoD line).** Resolved: the M1 run-loop driver (T24) does **not** yet
  fold a run-artifact summary carrying the peak figure — the merged coverage
  matrix records that facet as *"Deferred to C23 (T44): measured peak slot
  residency appearing in the run artifact … rendering the artifact number is
  C23's."* This ticket therefore asserts the observable seam available at M1: the
  `ResidencyLedger::peak` hook (the number the C23 artifact will fold) is
  non-zero and consistent with the bounded peak. The rendered-artifact assertion
  stays owned by T44; asserting it here would steal C23's scope (ticket-conventions
  §7/§8). Recorded additively in `docs/coverage-matrix.md`'s C10 row.
- **Terminal-node release requires a consumer.** Resolved: a non-retained slot
  with zero consumers is never triggered to release (the release gate advances
  only on a consumer lease's closure-return), so the synthetic chain appends a
  trivial zero-residency **sink** node that drains the last producer through a
  real `ConsumerLease`. This makes a fully non-retained chain end at zero counted
  residency — the honest C10 end state — while the `retain_terminal` variant marks
  the last producer retained so exactly one value survives and is redeemable. No
  T17 behaviour is changed; the sink only exercises the existing release path.

## Out of scope
- Implementing or amending the slot itself, the release rule, the type-erased slot storage, or the redemption API — those are T17; this ticket only exercises them.
- Implementing the run-loop driver, admission, or the pinning flag — those are T24 / C12; this ticket only consumes them and pins capacity for determinism.
- Zombie/abandoned-but-running residency accounting under timeout or cancellation (that the residency lease stays counted while a leftover closure runs) — that behaviour belongs to T17/C14/C12 tests, not this bounded-memory guard; this ticket's chain has no abandoned consumers.
- Any measurement of process RSS or OS-level memory reclamation, and any attempt to force the allocator to return pages to the OS — explicitly excluded by the C10 accounting rule.
- Concurrency/throughput benchmarking or scale benchmarking (T69) — this is a correctness guard on peak residency, not a performance benchmark.
- Multi-consumer fan-out residency semantics beyond the single-consumer chain, durable-output references (C27), and the demo pipeline itself (T28) — all downstream or adjacent, not part of this test.
