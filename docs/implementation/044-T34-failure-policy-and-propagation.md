# 044 · T34 — C15: failure policy, propagation, and trigger-rule runtime

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C15, C11
> **Branch:** `feat/t34-failure-policy-and-propagation` · **Depends on:** T24, T29, T0.4 · **Blocks:** T35, T52, T55

## Why / context
When something fails, the run must decide what still runs and what state every other node ends in — that decision is C15, and it is the last load-bearing piece before cancellation (T35), teardown (T52), and the CLI (T55) can build on it. M1's run-loop (T24) and readiness tracker (T18) already ship `all-succeeded` against the final rule interface; this ticket lands the *runtime* evaluation of all three trigger rules and both failure modes, converting the normative fires/can-never-fire tables settled in T0.4 into observable node outcomes. It is governed by **C15 · Failure policy and propagation**, **C11 · Readiness tracker** (the per-rule fires/can-never-fire criterion), and the **Vocabulary — terminal states and trigger rules** (the state taxonomy and state classes that propagation is defined over). This ticket does not build cancellation, teardown, or the CLI — it establishes the semantics they consume.

## Objective
Implement and test the failure-policy and trigger-rule runtime so that, given a stream of node outcomes, every node lands in exactly one terminal state consistent with the Vocabulary, and the set of nodes that still run after a failure is correct under each mode.

- Wire a run-level **failure mode** with two values — stop-on-first-failure and continue-independent — selected at the builder/assembly level (the mode-selection seam; CLI override deferred, see Open questions).
- Evaluate each trigger rule at the moment all of a node's upstreams are terminal: `all-succeeded` (default), `all-terminal`, `any-failed` — using the state-class taxonomy (success-like, skip-like, failure-like, stop-like), never on a partial upstream picture.
- On a rule that *can never fire*, assign the correct propagated terminal state without executing: `upstream-failed`, `upstream-skipped` (carrying the originating node's identity), `cancelled`, or `skipped` for an `any-failed` contingency that never arose.
- Propagate `upstream-skipped` carrying the originating node's identity, and treat a run whose only non-success outcomes are skips as an overall success.
- Under stop-on-first-failure: on the first terminal failure, stop admitting default-rule non-teardown work; admit the in-flight drain, then consume-nothing non-default-rule contingency nodes whose rule fires on the resulting terminal picture; mark pending unrelated default-rule nodes `cancelled`; leave a documented, unimplemented carve-out for teardown ordering (C17, M4).
- Under continue-independent: let branches with no ancestral relationship to the failure run to completion.
- Guarantee no node executes if any of its data dependencies did not succeed.

## Test plan (write these first — TDD)

**All-succeeded fires (happy path).** *Setup:* a node with two upstreams, both driven to `succeeded` (one via a `satisfied-from-prior` marker to prove that state counts as success-like). *Action:* run to completion. *Expected:* the node executes and ends `succeeded`.

**All-succeeded can-never-fire → upstream-failed.** *Setup:* a default-rule node with one upstream forced to `failed`. *Action:* run. *Expected:* the node never executes and ends `upstream-failed`; the run artifact shows exactly one terminal state for it.

**All-succeeded can-never-fire → upstream-skipped carries origin.** *Setup:* an upstream that returns a deliberate skip, feeding a default-rule downstream. *Action:* run. *Expected:* the downstream ends `upstream-skipped` and records the originating node's identity; the run reports overall success (only skips among non-successes).

**All-succeeded can-never-fire → cancelled class.** *Setup:* a default-rule node whose only non-success upstream ends `cancelled` (stop-like). *Action:* run. *Expected:* the downstream ends `cancelled`, not `upstream-failed` — the stop-like class wins when every non-success upstream is stop-like.

**All-succeeded mixed non-success → upstream-failed.** *Setup:* a default-rule node with one skip-like and one failure-like upstream. *Action:* run. *Expected:* the node ends `upstream-failed` (the "otherwise" branch of the propagation rule dominates when classes are mixed).

**All-terminal fires after a failure.** *Setup:* an `all-terminal`, consume-nothing node ordered after a node forced to `failed`. *Action:* run under both modes. *Expected:* the `all-terminal` node still executes to a real outcome; it is never marked `upstream-failed`. This is the reason non-default rules exist.

**Any-failed fires on a failure-like upstream.** *Setup:* an `any-failed`, consume-nothing contingency node ordered after a node forced to `failed`. *Action:* run. *Expected:* the contingency node executes.

**Any-failed fires on a transitive upstream-failed.** *Setup:* an `any-failed` node whose upstream itself ended `upstream-failed` (never ran). *Action:* run. *Expected:* the contingency executes — a transitively failure-like upstream counts.

**Any-failed contingency never arose → skipped.** *Setup:* an `any-failed` node whose upstreams all end success-like. *Action:* run. *Expected:* the node never executes and ends `skipped` (an originated-form skip meaning the contingency did not arise), distinct from `upstream-skipped`.

**Rule never fires on a partial picture.** *Setup:* a node with a fast and a slow upstream; the fast one fails. *Action:* run and observe evaluation order. *Expected:* the node's rule is not evaluated (and no terminal state assigned) until the slow upstream is also terminal.

**Stop-on-first-failure admits no further default work.** *Setup:* stop mode; a graph where a node fails while other default-rule non-teardown nodes are still pending (not yet admitted) and unrelated to the failure. *Action:* run. *Expected:* no default-rule non-teardown node is admitted after the first terminal failure is observed; those pending unrelated nodes end `cancelled`.

**Stop-on-first-failure still runs a firing contingency.** *Setup:* stop mode; a node fails, and a consume-nothing `any-failed` node is ordered after it. *Action:* run. *Expected:* the contingency node executes on the final terminal picture despite the stop — stop mode does not cancel the very work a failure is meant to trigger.

**Continue-independent runs unrelated branches.** *Setup:* continue mode; two branches with no ancestral relationship — one fails, the other is a chain of ordinary nodes. *Action:* run. *Expected:* the unrelated branch runs to completion and its nodes end `succeeded`.

**No node runs on a non-succeeded data dependency.** *Setup:* a data-dependent (`all-succeeded`, enforced) node whose data upstream ends `failed`, `timed-out`, `skipped`, or `cancelled` in turn. *Action:* run each variant. *Expected:* the node never executes in any variant; it ends in the propagated state matching the upstream's class.

**Skip-only run reports success.** *Setup:* a graph whose every node either originates a skip or propagates one, with no failures. *Action:* run. *Expected:* the run's overall outcome is success, and each propagated skip records its originating node.

**Exactly-one-terminal-state invariant.** *Setup:* a graph exercising success, failure, propagated failure, propagated skip, and cancelled outcomes together. *Action:* run and read the run artifact. *Expected:* every node — including those that never ran — appears with exactly one terminal state, and none appears twice.

## Definition of done
- [ ] A run-level failure mode is selectable at the builder/assembly level with both values (stop-on-first-failure, continue-independent); the default is chosen and documented.
- [ ] Each trigger rule is evaluated only once *all* of a node's upstreams are terminal — never on a partial upstream picture.
- [ ] `all-succeeded` fires when every upstream is success-like (including `satisfied-from-prior`), and its can-never-fire branch assigns `upstream-skipped` (every non-success upstream skip-like), `cancelled` (every non-success upstream stop-like), or `upstream-failed` (otherwise), without executing the node.
- [ ] `all-terminal` fires whenever every upstream is terminal regardless of class, and never propagates failure — an `all-terminal` node downstream of a failure still executes, verified under both modes.
- [ ] `any-failed` fires when every upstream is terminal and at least one is failure-like (including a transitively `upstream-failed` upstream), and marks the node `skipped` when the contingency never arose.
- [ ] Propagated-state selection is driven by the Vocabulary's state classes (success-like / skip-like / failure-like / stop-like), and a node's rule that can never fire assigns its propagated terminal state without executing.
- [ ] `upstream-skipped` carries the identity of the originating node; a run whose only non-success outcomes are skips reports overall success, and every propagated skip records its originating node.
- [ ] Under stop-on-first-failure, no default-rule non-teardown node is admitted after the first terminal failure is observed; the in-flight drain completes, and consume-nothing non-default-rule contingency nodes whose rule fires on the final terminal picture still execute — both verified.
- [ ] Under stop-on-first-failure, pending default-rule nodes unrelated to the failure end `cancelled`.
- [ ] Under continue-independent, a node with no ancestral relationship to the failure completes.
- [ ] No node ever executes if any of its data dependencies did not succeed.
- [ ] A node whose trigger rule can still be satisfied after an upstream failure executes; one whose rule cannot is marked `upstream-failed` without executing — both verified.
- [ ] Every node — including nodes that never ran — has exactly one terminal state in the run artifact.
- [ ] The teardown-ordering carve-out under stop mode (C17, M4) is present as a documented, deliberately unimplemented seam, with the "run teardown nodes after the contingencies" step named but left to T52.
- [ ] The per-rule fires and can-never-fire cases from C11 are each covered by a test, including the resulting terminal states, with `satisfied-from-prior` upstreams covered explicitly.
- [ ] All Test plan scenarios are implemented and passing.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Mode-selection surface in M2, before the CLI exists — builder-level policy with a CLI override later? This ticket lands the builder/assembly seam; the CLI override is deferred to T55 (C26) and must not be pulled forward. Confirm the builder-level default and that the seam accepts a later override without a signature change.
  - **Resolved (2026-07-23).** The mode is a new closed enum `dagr_core::flow::FailureMode { ContinueIndependent, StopOnFirstFailure }`. **Default = `ContinueIndependent`**, documented on the enum, on `Flow::failure_mode`, and on `RunConfig::failure_mode`. arch.md fixes no default, so continue-independent is chosen because it is the least-surprising (a failure cancels nothing unrelated) *and* it is byte-for-byte the M1 run loop's existing behaviour — selecting nothing changes nothing, keeping every T24/T25/T31 driver test green. The **builder/assembly seam** is `Flow::failure_mode(mode)` / `Pipeline::failure_mode()` (a run policy carried on the immutable pipeline, excluded from identity and both fingerprints — a different mode does not change the structural fingerprint). The **driver override seam** is `RunConfig::failure_mode(mode)` (chainable, defaulting to `ContinueIndependent`); the driver reads the config's mode. The **later CLI/operator override (T55, C26)** sets exactly this `FailureMode` value through the same `RunConfig::failure_mode` seam **without a signature change** — it is additive on the existing chainable builder. Nothing about the CLI is pulled forward.
  - **Ordering-seam note (2026-07-23).** Graph ordering edges (T50, M4) do not exist yet, so a consume-nothing non-default-rule node has no way to be "ordered after" another node through the graph-authoring API. To exercise the **runtime firing** of `all-terminal` / `any-failed` end-to-end (the facet T18 deferred here) without pulling T50 forward, this ticket adds a minimal **run-level ordering seam** — `ReadinessTracker::new_with_ordering(pipeline, artifact, ordering)` and `RunPlan::with_ordering(pipeline, runners, ordering)` — that seeds only the readiness tracker's dependency structure with ordering-only upstreams. It touches neither the graph artifact, the fingerprint, nor the renderer (all T50's), and an empty ordering map reproduces the M1 tracker exactly. T50 will later supply the compile-time attach rules, fingerprinting, and rendering of ordering edges on top of this consumed seam.

## Out of scope
- Cancellation mechanics — the run-scoped signal, grace period, cooperative stop, and `abandoned` recording (C16) belong to T35/T36. This ticket only *decides* which nodes should become `cancelled`; it does not implement the signal.
- Teardown node execution and ordering (C17) — deferred to T52; only the carve-out awareness seam is present here.
- The CLI failure-mode flag and any operator override surface (C26) — deferred to T55.
- Retry, backoff, and timeout mechanics that *produce* the `failed`/`timed-out` upstream states (C14) — this ticket consumes terminal states, it does not generate them.
- Resume and `satisfied-from-prior` rehydration (C27) — this ticket treats `satisfied-from-prior` only as a success-like input class; it does not implement resume.
- Any move toward runtime graph reshaping, dynamic branching in the graph, or a scheduler/backfill mode — branching stays in the task via deliberate skips (Vocabulary); the graph shape never changes at runtime.
