# 016 · T3 — ADR: error taxonomy design

> **Milestone:** M0 · **Size:** S · **Type:** decision · **Components:** C1, C14
> **Branch:** `adr/t3-error-taxonomy-adr` · **Depends on:** T0.4 · **Blocks:** T9

## Why / context
Two distinct error vocabularies collide at the attempt runner and must be settled before any task or runner code is written. The *task-facing* enum is what a pipeline author returns from their work; C1 fixes it at exactly three values — retry-eligible failure, permanent failure, and deliberate skip. The *framework-internal* outcome taxonomy is what the runner (C14) produces after classifying an attempt, and it is a strict superset: it also distinguishes timeout and panic, and it must fold cleanly into the normative terminal-state table settled by T0.4 (arch.md "Vocabulary — terminal states and trigger rules"). This ADR locks the boundary between the two so that C1's three-valued author surface and C14's richer runner classification never drift, and so that every runner outcome maps to exactly one terminal state and exactly one state class. It blocks T9, which implements the author-facing task abstraction and its error classification.

## Objective
Decide and record, as an Architecture Decision Record, the shape and mapping of dagr's error taxonomy. Concretely, this ticket must:

- Fix the **task-facing error enum** at exactly three variants — retry-eligible failure, permanent failure, deliberate (originated) skip — and state that it stays three-valued permanently, with rationale for why timeout and panic are *not* author-returnable.
- Define the **framework-internal outcome taxonomy** the runner produces when it classifies an attempt: the three author-returnable outcomes plus timeout and panic (five runner classifications per C14).
- Record the **superset relationship**: the runner taxonomy strictly contains the task-facing enum; name where each extra classification originates (timeout from the per-attempt clock, panic from the catch boundary — never from an author return).
- Record the **mapping from runner outcome to terminal state** against T0.4's table: retry-eligible-but-exhausted and permanent both → `failed`; panic → `failed`; timeout → `timed-out`; originated skip → `skipped`. Note the states the runner does *not* mint directly (`upstream-skipped`, `upstream-failed`, `cancelled`, `abandoned`, `satisfied-from-prior`) and which component owns each, so the ADR is total over the terminal-state table.
- Record the **state-class assignment** for every runner-minted terminal state, consistent with T0.4's classes (success-like / skip-like / failure-like / stop-like), so trigger-rule evaluation downstream (C11, C15) sees a total function.
- Record the **abandoned vs timed-out** distinction and the **satisfied-from-prior** origin as explicit non-goals of the runner classification path, pointing to the owning components (C16 cancellation, C27 resume), so no later ticket re-mints these states from the classification path.
- Record decision drivers, the alternatives considered and rejected (e.g. a unified single enum; a two-valued fail/skip enum; letting authors return timeout), consequences, and the naming/placement guidance that T9 will follow.

## Test plan (write these first — TDD)
Because this is a decision ticket, the "tests" are decision-record checks and throwaway-prototype evidence that validate the chosen taxonomy compiles and constrains as intended. Each is concrete and independently checkable.

- **Three-valued author surface holds.** Setup: sketch the task-facing enum in a throwaway prototype with exactly the three chosen variants. Action: attempt to add a fourth author-returnable variant for timeout. Expected: the ADR records that this is rejected, and the prototype demonstrates that timeout has no author-facing constructor — an author cannot return "timed out."
- **Superset mapping is total and unambiguous.** Setup: enumerate the five runner classifications. Action: for each, look up its target terminal state in the ADR's mapping table. Expected: every runner classification maps to exactly one terminal state (`failed`, `timed-out`, or `skipped`), with no classification unmapped and none mapping to two states.
- **Terminal-state table is covered.** Setup: list all terminal states from arch.md's Vocabulary. Action: for each, the ADR names whether it is runner-minted and, if not, the owning component. Expected: all ten terminal states are accounted for; exactly the runner-minted set (`failed`, `timed-out`, `skipped`) is attributed to C14, and `upstream-skipped`/`upstream-failed`/`cancelled`/`abandoned`/`satisfied-from-prior` are attributed to their owners (C11/C15, C16, C27) with no state left unassigned.
- **State-class assignment matches T0.4.** Setup: take the ADR's runner-minted terminal states. Action: cross-check each state's class against T0.4's success-like/skip-like/failure-like/stop-like partition. Expected: `failed` and `timed-out` are failure-like, `skipped` is skip-like, `succeeded` (the retry-eligible success path) is success-like; assignments are identical to T0.4's tables with no contradiction.
- **Retry-eligibility is a runner concern, not a state.** Setup: consider a retry-eligible failure. Action: trace it through the ADR when the retry budget is exhausted vs not. Expected: the ADR states that "retry-eligible" governs whether another attempt is scheduled (C14) and, once exhausted, resolves to the same `failed` terminal state as a permanent failure — retry-eligibility is not itself a terminal state.
- **Timeout retry-eligibility default is recorded.** Setup: read the ADR's treatment of timeout. Action: check the default retry disposition. Expected: the ADR states timeout is retry-eligible by default, subject to the node's retry budget (per C14), and that this does not add an author-facing variant.
- **Abandoned is not a runner classification.** Setup: read the ADR's outcome list. Action: search for `abandoned` as a runner-minted outcome. Expected: it is explicitly excluded from the classification path and attributed to the cancellation path (C16); a blocking timeout is and stays `timed-out`, never a second `abandoned` state.
- **Panic never unwinds and always fails its own node.** Setup: prototype a task body that panics behind the catch boundary. Action: observe the classification. Expected: the ADR records panic → permanent failure → `failed`, caught (not unwound), attributed to the panicking node only.
- **ADR structure is complete.** Setup: open the finished ADR. Action: verify presence of context, decision drivers, chosen taxonomy for both enums, mapping tables, alternatives-considered with rejection rationale, and consequences. Expected: every section is present and non-empty, and the record names the components it constrains (C1, C14, and the downstream consumers C11/C15).

## Definition of done
- [ ] The ADR fixes the task-facing error enum at exactly three variants — retry-eligible failure, permanent failure, deliberate skip — satisfying C1's criterion that a task's returned error distinguishes at minimum these three.
- [ ] The ADR states the task-facing enum stays three-valued permanently and records why timeout and panic are not author-returnable.
- [ ] The ADR defines the framework-internal runner outcome taxonomy as the five C14 classifications: retry-eligible failure, permanent failure, deliberate skip, timeout, and panic.
- [ ] The ADR records the superset relationship explicitly — the runner taxonomy strictly contains the task-facing enum — and names the origin of the two extra classifications (per-attempt clock for timeout; catch boundary for panic).
- [ ] The ADR maps every runner outcome to exactly one terminal state from T0.4's normative table: exhausted-retry and permanent → `failed`; panic → `failed`; timeout → `timed-out`; originated skip → `skipped`.
- [ ] The ADR records that timeout is retry-eligible by default, subject to the node's retry budget (C14), without adding an author-facing variant.
- [ ] The ADR records that retry-eligibility governs attempt scheduling only and resolves, once exhausted, to the same `failed` state as permanent failure.
- [ ] The ADR records panic handling: caught (not unwound), converted to permanent failure, attributed to its own node only, resolving to `failed`.
- [ ] The ADR assigns a state class (per T0.4: success-like / skip-like / failure-like / stop-like) to every runner-minted terminal state, identical to T0.4's partition, keeping trigger-rule evaluation total.
- [ ] The ADR accounts for all ten terminal states in the Vocabulary: it attributes the runner-minted set (`failed`, `timed-out`, `skipped`, and the success path `succeeded`) to C14 and attributes `upstream-skipped`/`upstream-failed`/`cancelled`/`abandoned`/`satisfied-from-prior` to their owning components (C11/C15, C16, C27), leaving no state unassigned.
- [ ] The ADR records that `abandoned` is not a runner classification and never arises as a second terminal state after `timed-out` (it belongs to the C16 cancellation path).
- [ ] The ADR records that `satisfied-from-prior` is not produced by the classification path (it belongs to C27 resume).
- [ ] The ADR records decision drivers, alternatives considered and rejected (e.g. a single unified enum, a two-valued enum, author-returnable timeout), consequences, and naming/placement guidance for T9.
- [ ] The ADR lives at the agreed docs location, is linked from the ADR index (if one exists), and is referenced by the T9 ticket as its governing decision.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Implementing the task abstraction or the error enum in code** — that is T9 (C1). This ticket decides the taxonomy; it writes no task trait, no enum definition, no classification function.
- **Implementing the attempt runner's classification, timeout, or panic-catch machinery** — that is the C14 implementation milestone (M2). This ticket only fixes what the classifications *are* and how they map.
- **Trigger-rule and terminal-state table authorship** — owned by T0.4 (its dependency); this ADR consumes those tables, it does not redefine them.
- **Minting `upstream-skipped` / `upstream-failed` / `cancelled` / `abandoned` / `satisfied-from-prior`** — these are propagation (C11/C15), cancellation (C16), and resume (C27) concerns; this ADR only records where they originate so the classification path stays bounded.
- **Event-stream record shapes and the attempt-outcome record schema** — C19/T0.6 territory; this ADR names outcomes, not their serialized form.
- **Scope-boundary temptation:** do not let a richer taxonomy grow into scheduler-, retry-policy-, or metadata-store-shaped state; the graph shape never changes at runtime and this ADR must not introduce runtime-mutable error handling. dagr is not a scheduler, distributed executor, metadata store, web interface, DSL, or backfill orchestrator.
