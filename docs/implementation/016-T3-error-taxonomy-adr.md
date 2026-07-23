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

---

# ADR: error taxonomy design

> The repo keeps each ADR inside its own implementation-ticket file (the T1, T2,
> T0.2, T0.3, T0.4, T0.6, T4, and T0.7 ADRs all embed the ADR at the ticket's own
> path). This ADR is committed here, at
> `docs/implementation/016-T3-error-taxonomy-adr.md`, the ADR location for ticket
> T3 — satisfying the DoD line "lives at the agreed docs location" literally,
> with zero deviation, and linked from the tasks/spec index (`docs/tasks.md` T3
> entry and `docs/implementation/README.md`) so its consumer T9 finds it. Its
> mechanical acceptance gate is
> [`scripts/check-error-taxonomy-adr.sh`](../../scripts/check-error-taxonomy-adr.sh),
> which cross-references this ADR against ticket T0.4's (010) merged
> terminal-state and state-class tables so every state, class, mapping, and
> attribution is total and consistent with 010 — no invention and no omission.

## Status

Accepted (2026-07-23). This is a **decision** ticket: it settles dagr's error
taxonomy — the boundary between the author-facing task-error surface (C1) and the
framework-internal runner outcome classification (C14) — and records it as this
ADR. It ships **no production code**: the shipping crates (`core`, `artifact`,
`render`, `cli`) are unchanged, `Cargo.lock` is untouched, and the only committed
artifacts are this ADR and its mechanical acceptance script named above. The
error types, the task trait, and the classification function are **T9's** (C1)
and the **C14** implementation milestone's to write; this ADR fixes only *what
the classifications are* and *how they map*, so T9 implements against a stable,
already-decided contract.

This ADR **consumes** T0.4's (010) reference tables and re-decides none of them:
the nine terminal states, the four state classes as a total partition, and the
per-rule fires/can-never-fire table are 010's, and this ADR maps errors *onto*
them. There is **no supersession**: this ADR adds the error-classification layer
above 010's state tables; it moves no merged decision. It is **consistent with**
the two already-merged M0 ADRs that touch failure modes: **T0.3** (009 — timeout
abandonment and permit accounting) fixes that a blocking `timed-out` attempt
*stays* `timed-out` (its leftover thread is a zombie *event*, never a second
`abandoned` state) and that `timed-out` and `abandoned` are distinct
failure-like states; this ADR's timeout→`timed-out` mapping and its "`abandoned`
is not a runner classification" clause are the same rule stated from the
error-classification side. **T0.2** (008 — output ownership) fixes that a
successful attempt fills the once-writable slot and a failed retry-eligible
attempt finds its input intact on the next attempt; this ADR's success/retry
paths presuppose exactly that slot behavior and contradict none of it.

**Open questions: none.** The ticket's `## Open questions` says "None," and the
`docs/tasks.md` T3 entry carries **no `Q:` items** — its text is a resolution,
not a question: *"Resolved: the runner's classification is the superset; the
task-facing enum stays three-valued."* Both open-question sources
(ticket-conventions §5) are therefore fully discharged: this ADR records the
resolution the task entry already states and elaborates it into the total mapping
below.

## Context

Two distinct error vocabularies collide at the attempt runner and must be settled
before any task or runner code is written (T9 depends on T3):

- The **task-facing** error is what a pipeline author *returns* from their work.
  arch.md `### C1 · Task` fixes it: *"The error a task returns distinguishes at
  minimum: retry-eligible failure, permanent failure, and deliberate skip."* This
  is the author's entire error surface — three outcomes, no more.
- The **framework-internal outcome taxonomy** is what the runner *produces* when
  it classifies a finished attempt. arch.md `### C14 · Attempt runner` fixes it:
  *"Classification distinguishes retry-eligible failure, permanent failure,
  deliberate skip, timeout, and panic. Timeout is retry-eligible by default,
  subject to the node's retry budget."* Five classifications — the three
  author-returnable ones plus two the runner alone originates.

The tension is that these two vocabularies must never drift, yet they are not the
same size: an author cannot *return* "timed out" (the timeout is decided by the
per-attempt clock, not by the task body) and cannot *return* "panicked" (a panic
by definition escaped the author's `Result`, caught at the framework's boundary).
So the runner taxonomy is a **strict superset** of the task-facing enum, and
every runner classification must fold — totally and unambiguously — into T0.4's
normative terminal-state table so that downstream trigger-rule evaluation (C11,
C15) sees a total function.

Three arch.md sections and one merged ADR meet here:

- **`### C1 · Task`** — the three-valued author surface (quoted above).
- **`### C14 · Attempt runner`** — the five-classification runner taxonomy, the
  per-class timeout semantics, and panic containment (*"A panic is caught,
  attributed to its node, and converted to a permanent failure rather than being
  allowed to unwind the run… The framework applies `AssertUnwindSafe` at the
  catch boundary"*).
- **`## Vocabulary — terminal states and trigger rules`** — the nine states, four
  classes, and closed rule set, canonicalized by **T0.4** (010), which this ADR
  maps onto.
- **T0.3** (009) — timeout stays `timed-out`; `abandoned` is a cancellation-path
  state, never a second state after a timeout.

**Prototype evidence.** A throwaway spike (built outside the workspace under
`/tmp`, run, its evidence quoted here, then deleted — nothing promoted into any
crate) validated that the chosen shapes compile and constrain as intended:

1. *Three-valued author surface holds.* With the task-facing enum sketched as
   exactly `{ Retryable, Permanent, Skip }`, an author's attempt to *return* a
   timeout does not compile:

   ```text
   error[E0599]: no variant or associated item named `Timeout` found for enum `TaskError`
    --> neg.rs:2:32
     |
   1 | enum TaskError { Retryable(String), Permanent(String), Skip }
     | -------------- variant or associated item `Timeout` not found for this enum
   2 | fn main() { let _ = TaskError::Timeout; }
     |                                ^^^^^^^ variant or associated item not found
   ```

   Timeout has no author-facing constructor — the mechanized form of "an author
   cannot return 'timed out.'"

2. *Superset mapping is total by construction.* The runner-outcome→terminal-state
   function is an exhaustive `match` over the five classifications; dropping one
   arm is a compile error, so the mapping cannot silently leave a classification
   unmapped:

   ```text
   error[E0004]: non-exhaustive patterns: `&RunnerOutcome::Timeout` not covered
    --> nonexhaustive.rs:6:11
     |
   6 |     match o {
     |           ^ pattern `&RunnerOutcome::Timeout` not covered
   ```

3. *Panic never unwinds and always fails its own node.* A task body that panics
   behind `catch_unwind(AssertUnwindSafe(body))` was caught, its `&str`/`String`
   payload normalized, converted to the `Panic` outcome (→ `failed`), and the run
   proceeded to completion (`t3-spike: all taxonomy invariants hold` printed after
   the caught panic) — never unwinding the process. This mirrors the dagx prior
   art (`AssertUnwindSafe(fut).catch_unwind().unwrap_or_else(…)` normalizing the
   payload to a typed error), the one pattern dagx contributes to dagr's runtime
   half; dagr additionally classifies the caught panic into the terminal-state
   taxonomy below, which dagx's flat `DagError { task_id, panic_message }` does
   not do.

## Decision drivers

The forces that shaped the decision below, in priority order:

1. **C1's minimum is the author's maximum.** The author error surface must
   distinguish *at least* retry-eligible / permanent / deliberate-skip (C1) — and,
   because a bigger author surface only invites faked or unobservable outcomes, it
   distinguishes *exactly* those three. The author surface is minimal and permanent.
2. **The runner must classify richer outcomes than an author can return.** Timeout
   and panic are real attempt outcomes (C14) that no author return can express, so
   the runner needs classifications the author enum lacks — driving the superset.
3. **Two vocabularies must not drift.** The task-facing enum and the runner
   taxonomy meet at the attempt runner and are consumed by different tickets (T9
   vs C14); the boundary between them must be a type-level fact, not a convention,
   so they stay in lockstep.
4. **Every outcome must fold totally into T0.4's terminal states.** Downstream
   trigger-rule evaluation (C11, C15) requires a *total function* from outcome to
   terminal state and state class; an unmapped or ambiguous classification breaks
   readiness and propagation. The mapping must be total and identical to T0.4.
5. **The classification path must stay bounded.** `abandoned` (C16),
   `satisfied-from-prior` (C27), and the propagated `upstream-*` states (C11/C15)
   have other owners; the runner must not re-mint them, or a blocking timeout could
   spawn a second terminal state (contradicting T0.3).
6. **No runtime-mutable error handling.** The taxonomy is closed by construction;
   a new author variant or runner classification is a spec amendment, never a knob
   — dagr is not a DSL or a scheduler, and the error contract never changes at
   runtime.

## Decision

### 1. The task-facing error enum — exactly three variants, permanently

The task-facing error is a **three-variant enum**, and it stays three-valued
**permanently**. These are the only three things a pipeline author may *return*
from their work, satisfying C1's *"distinguishes at minimum: retry-eligible
failure, permanent failure, and deliberate skip."*

| # | Task-facing variant | Meaning | Author intent |
|---|---|---|---|
| 1 | **retry-eligible failure** | A transient failure the framework may retry (I/O blip, rate limit, contended lock). | "Try me again." |
| 2 | **permanent failure** | A failure retrying cannot fix (bad input, invariant violated, missing prerequisite). | "Do not retry me." |
| 3 | **deliberate (originated) skip** | The task decided there is nothing to do; branching is expressed in the task, not the graph (Vocabulary). | "I am declining to run." |

**Why it stays three-valued, and why timeout and panic are NOT
author-returnable.** The author surface is the *minimum* that lets a task express
its own fate and no more. Two runner classifications are deliberately absent from
it:

- **Timeout is not author-returnable.** A timeout is decided by the *per-attempt
  clock* (C14), not by the task body — the whole point of a timeout is to bound
  work the task itself will not bound. An author returning a value has, by
  definition, not timed out; an author who *had* timed out never returns to
  report it. Giving authors a `Timeout` variant would be nonsensical (they cannot
  observe their own timeout to return it) and would invite them to fake timeouts,
  corrupting the runner's clock-owned classification.
- **Panic is not author-returnable.** A panic is precisely the failure that
  *escaped* the author's `Result` — a `Panic` variant is a contradiction, because
  a task that could return `Panic` did not panic, it returned. Panic is caught at
  the framework's boundary (§4), never returned.

Keeping the enum three-valued is a permanent commitment, not a v1 convenience: a
fourth author-returnable variant is a spec amendment, never a runtime knob (the
graph shape and the author contract never change at runtime).

### 2. The framework-internal runner outcome taxonomy — five classifications

When the runner finishes an attempt it produces exactly one of **five**
classifications (C14):

| # | Runner classification | Source |
|---|---|---|
| 1 | **retry-eligible failure** | author returned the retry-eligible variant |
| 2 | **permanent failure** | author returned the permanent variant, **or** a caught panic (§4) |
| 3 | **deliberate skip** | author returned the deliberate-skip variant |
| 4 | **timeout** | the **per-attempt clock** — never an author return |
| 5 | **panic** | the **catch boundary** — never an author return |

### 3. The superset relationship — the runner taxonomy strictly contains the task-facing enum

The runner taxonomy is a **strict superset** of the task-facing enum: the three
author-returnable outcomes map one-to-one into it (variants 1–3), and the runner
adds exactly two classifications the author can never produce:

- **timeout** originates from the **per-attempt clock** (C14's per-attempt
  timeout, per-class semantics fixed by T0.3), **never from an author return**.
- **panic** originates from the **catch boundary** (`AssertUnwindSafe` +
  `catch_unwind`, §4), **never from an author return**.

This is the boundary the ADR locks: C1's three-valued author surface and C14's
five-classification runner surface never drift, because the two extra
classifications have named, framework-owned origins that no author return can
reach. (The strict superset is 3 ⊂ 5; the two extra elements are timeout and
panic.)

### 4. Panic handling — caught, converted, attributed to its own node

A panic in a task body is **caught, not unwound**: the framework wraps the attempt
in `AssertUnwindSafe` and `catch_unwind` (per arch.md C14 and the dagx prior art),
so a panic never unwinds the run. The caught panic is **converted to a permanent
failure** — it is not retried (retrying a panicking body is unlikely to help and
risks corrupting shared state, which is why the C14 pattern is resource
poisoning, not blind retry) — and therefore resolves to the **`failed`** terminal
state (§5). It is **attributed to its own node only** (via task-local state, C14):
a panic fails the panicking node and the rest of the run proceeds per the failure
policy (C15). Startup refuses `panic = "abort"` with a message naming the fix
(C14/T23), because an aborting binary cannot catch — that startup check is C14's
to implement; this ADR only records that panic classification depends on it.

### 5. The mapping — every runner outcome to exactly one terminal state (T0.4's table)

The runner *mints* four of T0.4's nine terminal states — the outcomes it decides
directly. The mapping from runner outcome to terminal state is **total and
unambiguous**: every one of the five classifications (and the success path) maps
to exactly **one** terminal state, none unmapped and none mapping to two. The
prototype's exhaustive `match` (Context, evidence 2) makes this totality a
compile-time property in T9's eventual implementation.

| Runner outcome / path | Retry budget remaining? | Terminal state (T0.4) | State class (T0.4) |
|---|---|---|---|
| success (task returned a value) | — | **`succeeded`** | success-like |
| retry-eligible failure | **yes** | *(no terminal state — schedule another attempt)* | — |
| retry-eligible failure | **no** (exhausted) | **`failed`** | failure-like |
| permanent failure | — | **`failed`** | failure-like |
| **panic** (caught, §4) | — | **`failed`** | failure-like |
| **timeout** | **yes** | *(no terminal state — retry deferred, §6)* | — |
| **timeout** | **no** (exhausted) | **`timed-out`** | failure-like |
| deliberate (originated) skip | — | **`skipped`** | skip-like |

So: exhausted-retry and permanent both → `failed`; panic → `failed`; timeout →
`timed-out`; originated skip → `skipped`; and the success path → `succeeded`.
These four are the **runner-minted** subset of T0.4's nine states.

### 6. Retry-eligibility is a runner concern, not a terminal state

"Retry-eligible" is a property that governs **attempt scheduling** (C14), not a
terminal state. Trace a retry-eligible failure through this ADR:

- **budget remaining** → the runner schedules **another attempt** after a backoff;
  the node has *no* terminal state yet (it is still in flight).
- **budget exhausted** → the node resolves to **`failed`** — the *same* terminal
  state as a permanent failure. There is no `retry-exhausted` terminal state; a
  node that ran out of retries and a node that failed permanently are
  indistinguishable at the terminal-state layer (both `failed`, both
  failure-like). The distinction lived only in whether another attempt was
  scheduled, and that distinction is spent once the budget is gone.

**Timeout retry-eligibility default.** Timeout is **retry-eligible by default,
subject to the node's retry budget** (C14) — a timed-out attempt schedules another
attempt exactly like a retry-eligible failure does, until the budget is exhausted,
at which point it resolves to `timed-out`. This is a *runner default* that is
**not an author-facing variant** — it adds nothing to the three-valued task-facing
enum (§1), because a timeout is decided by the clock, not returned by the author.
Per T0.3, a blocking-class timeout defers its retry until the previous
attempt's closure has actually returned (C1 exclusivity), but that is a
*scheduling* detail C14/T21 owns; the *classification* is unchanged — the outcome
is `timeout`, mapping to `timed-out` once exhausted.

### 7. State-class assignment — identical to T0.4's partition

Every runner-minted terminal state carries **exactly** the state class T0.4 (010)
assigns it — this ADR asserts identity with 010's partition, it does not re-decide
it, so trigger-rule evaluation (C11, C15) stays a total function:

| Runner-minted terminal state | State class (identical to T0.4) |
|---|---|
| `succeeded` | **success-like** |
| `failed` | **failure-like** |
| `timed-out` | **failure-like** |
| `skipped` | **skip-like** |

These assignments are **identical to T0.4's** success-like / skip-like /
failure-like / stop-like partition, with no contradiction: `failed` and
`timed-out` are both failure-like, `skipped` is skip-like, `succeeded` is
success-like. (The runner mints no stop-like state — `cancelled` is C16's, §8.)

### 8. Total over the terminal-state table — who owns the states the runner does not mint

The ADR is **total over T0.4's nine-state table**: the four runner-minted states
above are attributed to **C14**, and the remaining five are attributed to their
owning components so no later ticket re-mints them from the classification path.

| Terminal state | Class (T0.4) | Minted by | Owner / origin |
|---|---|---|---|
| `succeeded` | success-like | **runner (C14)** | task returned a value; slot filled |
| `failed` | failure-like | **runner (C14)** | permanent, exhausted-retry, or caught panic |
| `timed-out` | failure-like | **runner (C14)** | per-attempt clock; retry-eligible by default |
| `skipped` | skip-like | **runner (C14)** | originated skip **or** an `any-failed` contingency that never arose (C11) |
| `upstream-skipped` | skip-like | **not the runner** | propagation — readiness/failure policy (**C11 / C15**) |
| `upstream-failed` | failure-like | **not the runner** | propagation — readiness/failure policy (**C11 / C15**) |
| `cancelled` | stop-like | **not the runner** | cancellation path (**C16**) |
| `abandoned` | failure-like | **not the runner** | cancellation path (**C16**) — §9 |
| `satisfied-from-prior` | success-like | **not the runner** | resume (**C27**) — §10 |

Note on `skipped`: the runner mints `skipped` for an *originated* skip; the C11
readiness tracker also assigns `skipped` for an `any-failed` contingency that
never arose (T0.4 §5c). Both are the same terminal state; only the runner's is an
*error-classification* outcome, which is all this ADR governs.

### 9. `abandoned` is not a runner classification

`abandoned` is **not** one of the five runner classifications and is **excluded
from the classification path**. It arises only on the **cancellation path (C16)**:
a node asked to cancel that does not return within the grace period. Critically
(per T0.3 / arch.md C14), a **blocking timeout stays `timed-out`** — its leftover
thread is recorded as a **zombie-at-exit event** in the stream (C19), **never as a
second `abandoned` terminal state**. A node's terminal state is decided exactly
once; `abandoned` never arises *after* `timed-out`. So the classification path
never produces `abandoned`, and no later ticket may re-mint it from classification.

### 10. `satisfied-from-prior` is not produced by the classification path

`satisfied-from-prior` is **not produced by the runner's classification path**
either — it belongs to **resume (C27)**, which carries a prior run's success
forward (its durable output rehydrated, or its value never demanded). It is a
resume-time assignment over the *prior* run's states, not an outcome the runner
classifies from a *this-run* attempt. The classification path stays bounded to
the four runner-minted states of §8.

### 11. Naming and placement guidance for T9

T9 (019 — C1: task abstraction and error classification) is the implementing
ticket and **references this ADR as its governing decision** (its header already
lists `Depends on: … T3 …`, and its body cites *"the three-valued task-facing
error enum vs the runner's superset outcome taxonomy (T3)"*). Guidance T9 follows:

- **The task-facing enum is the public author surface** — T9 defines a
  three-variant enum (retry-eligible / permanent / deliberate-skip) in the `core`
  crate's authoring surface, with the ergonomic constructors an author calls to
  *return* each. It carries **no** timeout or panic variant (§1). Rich per-attempt
  context (error message, backtrace) hangs off the variants; that shape is T9's,
  bounded by this taxonomy.
- **The runner outcome taxonomy is internal** — the five-classification type
  belongs to the C14 runner (M1 lands the single-attempt core in T20), **not** the
  public author API. T9 defines only the author enum and the lift from an author
  return into the internal taxonomy (variants 1–3); the runner adds timeout and
  panic (§3). Keep the two types distinct so the superset boundary is a type-level
  fact, not a convention.
- **Classification is a function of the attempt result**, not of author-visible
  state — T9 provides the classify-a-return step; the timeout and panic
  classifications are produced by the runner's clock and catch boundary (C14/T20,
  T21, T23), not by anything the author writes.
- **Do not introduce runtime-mutable error handling** — the taxonomy is closed by
  construction; a new author variant or runner classification is a spec amendment.

### 12. Downstream hand-off — the components this ADR constrains

- **C1 (T9)** consumes the three-valued task-facing enum (§1) and the lift into
  the runner taxonomy (§3, §11).
- **C14** (T20 single-attempt core, T21 timeout, T22 retry, T23 panic) consumes
  the five-classification taxonomy (§2), the runner-outcome→terminal-state mapping
  (§5), the timeout-retry default (§6), and the panic-containment record (§4).
- **C11 / C15** (T18 readiness, T34 failure policy) consume the runner-minted
  terminal states and their T0.4 classes (§7) so propagation and trigger-rule
  evaluation see a total function; they *own* `upstream-skipped` / `upstream-failed`
  (§8), which this ADR only attributes, not mints.
- **C16** (T35) owns `cancelled` / `abandoned` (§8, §9); **C27** (T57/T58) owns
  `satisfied-from-prior` (§8, §10). This ADR records their origins so the
  classification path stays bounded.

## Consequences

- **The author surface is minimal and permanent.** Three variants, forever; the
  first-hour author never confronts timeout or panic as things to return. A
  fourth author-returnable variant is a spec amendment, never a runtime knob.
- **The two vocabularies cannot drift.** The runner taxonomy is a strict superset
  whose two extra classifications have framework-owned origins (clock, catch
  boundary); no author return can reach them, so C1 and C14 stay in lockstep by
  type-level construction, not by convention.
- **The mapping is total and compile-checked.** Every runner classification folds
  to exactly one of T0.4's terminal states; T9's exhaustive `match` (Context,
  evidence 2) makes an unmapped classification a compile error, so downstream
  trigger-rule evaluation (C11, C15) always sees a total function.
- **Retry-eligibility never leaks into the state space.** A retry-exhausted node
  and a permanently-failed node are both `failed`; there is no `retry-exhausted`
  state, so the terminal-state table stays at nine (T0.4) and downstream code
  never special-cases retry provenance.
- **The classification path is bounded.** `abandoned` (C16), `satisfied-from-prior`
  (C27), and the propagated `upstream-*` states (C11/C15) are attributed to their
  owners and explicitly excluded from classification, so a blocking timeout is and
  stays `timed-out` and no later ticket re-mints these states from the runner.
- **Consistency with T0.3/T0.4/T0.2 is load-bearing.** The timeout→`timed-out`
  mapping and the "`abandoned` is not classification" clause agree with T0.3; the
  state classes are identical to T0.4; the success/retry paths presuppose T0.2's
  slot behavior. This ADR moves none of those merged decisions.
- **No runtime, no engine.** This ticket produces the taxonomy record only; the
  task abstraction (T9), single-attempt core (T20), timeout (T21), retry (T22),
  and panic containment (T23) implement *against* it and own their own tests.
  Shipping no covering test, this ADR makes **no change to
  `docs/coverage-matrix.md`** — C1 is human-classed (its mechanical sub-criteria
  are machine-tested under T9/T29/T14) and C14 remains `unmapped`, owed by T20's
  attempt-runner tests, which flip it as they land.

## Rejected alternatives

- **A single unified enum for both author and runner.** Rejected: it would give
  authors constructors for `Timeout` and `Panic` — outcomes an author can never
  observe or produce — inviting faked timeouts that corrupt the clock-owned
  classification, and making a `Panic` return (a contradiction: a task that
  returned did not panic) expressible. The superset relationship exists precisely
  so the author surface stays three-valued while the runner classifies richer
  outcomes; collapsing them destroys that boundary. The prototype's E0599
  (Context, evidence 1) is the value of keeping them separate: an author *cannot*
  return a timeout.
- **A two-valued fail/skip author enum (drop the retry/permanent distinction).**
  Rejected: it fails C1's explicit *"distinguishes at minimum: retry-eligible
  failure, permanent failure, and deliberate skip"* — three, not two. Without the
  retry/permanent split the runner cannot know whether to schedule another
  attempt, collapsing C14's retry semantics; retry-eligibility is the author's to
  signal (only the task knows if a failure is transient), so it must live in the
  author enum.
- **Letting authors return timeout (an author-facing `Timeout` variant).**
  Rejected: a timeout is decided by the per-attempt clock, not the task body; an
  author who returns has not timed out, and an author who timed out never returns
  to report it. A `Timeout` variant is unobservable-by-construction and would let
  a task fake a timeout, corrupting the runner's classification. Timeout stays a
  runner-only classification with an origin in the clock (§3), retry-eligible by
  default without adding an author variant (§6).
- **A distinct `retry-exhausted` terminal state.** Rejected: it would add a tenth
  terminal state to T0.4's closed nine, for no downstream benefit — a
  retry-exhausted node and a permanent failure are both `failed` and both
  failure-like; the retry-vs-permanent distinction is a *scheduling* fact spent
  once the budget is gone, not a terminal outcome (§6).
- **Classifying panic as retry-eligible (retry a panicking body).** Rejected:
  arch.md C14 converts a caught panic to a *permanent* failure — retrying a body
  that panicked is unlikely to help and risks running against poisoned shared
  state; the prescribed pattern is resource poisoning, not blind retry. Panic →
  permanent failure → `failed` (§4).
- **Minting `abandoned` from the classification path (a blocking timeout's zombie
  becoming a second `abandoned` state).** Rejected as a permanent non-goal:
  per T0.3, a blocking timeout stays `timed-out` and its leftover thread is a
  zombie *event* (C19), never a second terminal state; `abandoned` arises only on
  the cancellation path (C16). A node's terminal state is decided exactly once
  (§9).
- **A runtime-extensible / pluggable error taxonomy.** Rejected as a permanent
  non-goal: dagr is not a DSL or a scheduler, the graph shape and the error
  contract never change at runtime, and a new author variant or runner
  classification is a spec amendment. The taxonomy is closed by construction.
