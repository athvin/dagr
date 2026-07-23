# 064 · T52 — C17: teardown nodes

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C17
> **Branch:** `feat/t52-teardown-nodes` · **Depends on:** T35, T50, T0.4 · **Blocks:** T63

## Why / context
dagr promises that resource lifecycle cleanup happens regardless of outcome; C17 (`### C17 · Setup and teardown nodes`) is where that promise is kept. This ticket builds on ordering edges (C4, delivered by T50) — a teardown is a consume-nothing node ordered *after* a covered set and firing on `all-terminal` — on the cancellation and graceful-drain core (C16, T35) so teardown can run under a *fresh, uncancelled* signal even when the run was killed, and on the trigger-rule/terminal-state reference tables (T0.4) that define when `all-terminal` fires. It also wires the covered-nodes' terminal states into the run context (C8) and pins declared teardown cost to zero so the admission bypass stays consistent with C12's capacity invariant. The shutdown budget arithmetic (C16: 10 s grace + 15 s teardown + 2 s flush against a 30 s kill window) depends on the teardown deadline this ticket makes an operator flag.

## Objective
Deliver teardown nodes as specified by C17: a node ordered after a covered set that runs exactly once every covered node is terminal, cleans up under isolation from the run's cancellation and outcome, and forces re-execution of covered nodes on resume.

Concrete pieces of work:
- A registration/assembly path that designates a node as a teardown covering an explicit set of already-registered upstream handles, using C4's backward-reference discipline so the compile-time acyclicity guarantee is preserved.
- Firing semantics: the teardown becomes ready when *all* covered nodes are terminal in *any* state (`succeeded`, `failed`, `skipped`, `cancelled`, `abandoned`, and the propagated `upstream-failed`/`upstream-skipped` forms), evaluated with the same "rule never fires on partial results" discipline as C11.
- Assembly rejection of a teardown node whose declared cost is nonzero, with a distinct, review-visible error; and admission bypass so teardown never competes for capacity with the run it is cleaning up after.
- A fresh, uncancelled signal for each teardown attempt, carrying its own deadline that defaults to 15 seconds and is exposed as an operator flag, independent of the run's cancellation signal.
- Extension of the run context (C8) so a teardown's context exposes the terminal states of the nodes it covers, enabling a no-op when setup never ran.
- Failure isolation: a failing teardown is recorded as `failed` but does not change the run's overall outcome (which is determined only by non-teardown nodes) and does not prevent other teardown nodes from running.
- The resume interaction (C27): a node covered by a teardown that executed in the prior run is added to the must-run seed and re-executed, never satisfied-from-prior; and the developer-facing documentation of the rule "outputs that teardown deletes are not resume-safe."

## Test plan (write these first — TDD)
1. **Fires only when all covered nodes are terminal.** Setup: a teardown covering two nodes, one fast and one slow. Action: run to completion. Expected: the teardown does not become ready while the slow covered node is still in flight, and becomes ready only after both covered nodes reach a terminal state — observed via the readiness/event stream ordering, with no partial-result firing.
2. **Runs after every terminal class of covered upstream.** Setup: five separate teardown-covered scenarios where the single covered node ends `succeeded`, `failed`, `skipped`, `cancelled`, and `abandoned` respectively. Action: run each. Expected: in every case the teardown node executes.
3. **Runs after propagated terminal states too.** Setup: a covered node that ends `upstream-failed` (its own upstream failed and its rule can never fire). Action: run. Expected: the teardown still fires once that covered node is marked terminal.
4. **Covered terminal states are visible in context.** Setup: a teardown covering two nodes, one that succeeded and one that was skipped, whose task body records the covered states it sees. Action: run. Expected: the teardown observes exactly those two covered nodes' terminal states through its context, and can branch on them (a recorded "no-op because setup never ran" path is exercised when the covered setup node did not succeed).
5. **Failure isolation — outcome unchanged.** Setup: a pipeline whose non-teardown nodes all succeed, plus a teardown node whose body fails. Action: run. Expected: the teardown is recorded as `failed`, and the run's overall outcome and exit code are those of a successful run (run failure is determined only by non-teardown nodes ending `failed`/`timed-out`, per C26).
6. **One failing teardown does not block others.** Setup: three independent teardown nodes, the second of which fails. Action: run. Expected: all three teardown nodes execute, and the first and third complete normally regardless of the second's failure.
7. **Executes under termination-signal cancellation.** Setup: a run cancelled by a simulated external termination signal (via C16's cancellation core) with a teardown attached to an in-flight covered node. Action: cancel mid-run. Expected: the covered node reaches a terminal state on the cancellation path, the teardown then runs under a *fresh* signal that is not cancelled, its body observes that fresh signal as uncancelled, and its cleanup completes.
8. **Teardown deadline is fresh, defaulting to 15 s, and is a flag.** Setup: a teardown whose body waits past a deadline set well below default via the operator flag. Action: run. Expected: the teardown attempt is bounded by its own deadline (not the run's cancellation state), the default value is 15 seconds when the flag is unset, and the flag overrides it.
9. **Nonzero declared cost is rejected at assembly.** Setup: a teardown node declared with a nonzero cost on any pool. Action: run assembly. Expected: assembly fails with a distinct, complete error naming the offending teardown node, before any node executes; a zero-cost teardown assembles cleanly.
10. **Admission bypass — no capacity competition.** Setup: a memory-constrained run (capacity pinned via C12 flag) saturated by non-teardown work, with a teardown attached. Action: run. Expected: the teardown is admitted without waiting on and without consuming pool capacity, and the combined declared cost of admitted non-teardown work is unaffected by teardown's presence (teardown never appears in the admission ledger).
11. **Resume re-executes covered nodes, never satisfied-from-prior.** Setup: a prior run in which a teardown that covers a durable-output node executed; resume that run. Action: assemble the resume must-run seed and run. Expected: the covered node is in the must-run seed and re-executes, and is never marked `satisfied-from-prior`, even though its output was durable.
12. **No-data-dependency rule is a compile error (UI test).** Setup: a throwaway example attempting to bind a data handle into a teardown node (which requires the `all-terminal` rule). Action: compile against the pinned workspace toolchain (C28). Expected: compilation fails because the builder typestate makes a non-default trigger rule inexpressible on a node that consumes data (C3) — the teardown never-have-data-dependencies invariant is enforced at compile time, not by a runtime or assembly check; a teardown wired with ordering edges only compiles.

## Definition of done
- [ ] A teardown node runs when its covered upstream ended `succeeded`, `failed`, `skipped`, `cancelled`, or `abandoned` (and the propagated `upstream-failed`/`upstream-skipped` forms), firing only once *all* covered nodes are terminal.
- [ ] A failing teardown node is recorded as `failed`, but the run's overall outcome is determined only by non-teardown nodes.
- [ ] Several teardown nodes all run even when one of them fails.
- [ ] Teardown nodes never have data dependencies, and this is enforced at compile time via the builder typestate (C3); their context exposes the covered upstream terminal states (C8).
- [ ] Teardown executes even when the run was cancelled by a termination signal, under a fresh, uncancelled signal with its own deadline.
- [ ] The teardown deadline defaults to 15 seconds and is an operator flag; it participates in the C16 shutdown budget the binary prints at startup.
- [ ] Teardown bypasses admission and never competes for capacity with the run it cleans up after.
- [ ] A teardown node with a nonzero declared cost is rejected at assembly with a distinct, review-visible error, keeping the admission bypass consistent with C12's capacity invariant.
- [ ] On resume, a node covered by a teardown that executed in the prior run is added to the must-run seed and re-executed, never satisfied-from-prior.
- [ ] The developer-facing rule "outputs that teardown deletes are not resume-safe" is documented where developers will see it (rustdoc on the teardown registration API).
- [ ] Every acceptance-criterion scenario in the Test plan is covered by an independent test, including the compile-fail UI test against the pinned toolchain (C28).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Never-have-data-dependencies — enforced at compile time or assembly time? Resolved by this ticket in favor of **compile time**: because teardown fires on the non-default `all-terminal` rule and C3's builder typestate already makes any non-default trigger rule inexpressible on a node that consumes data, a data-dependent teardown is a compile error, not a runtime or assembly check. Assembly still independently rejects nonzero teardown cost. Confirm no registration path can construct a teardown that both consumes data and carries `all-terminal`.

## Out of scope
- Setup nodes as a distinct concept — they are ordinary nodes (C17 explicitly); no special setup machinery is built here.
- The C16 cancellation core, OS-signal handling, final flush, and temp-dir cleanup themselves (T35/T36) — this ticket consumes the fresh-signal mechanism but does not implement or modify signal delivery.
- The C4 ordering-edge mechanics (T50) and the C12 admission controller (T31/T32) — reused as-is; only the zero-cost assembly rejection and the bypass hook are added here.
- The full resume/rehydration algorithm (C27) — this ticket only contributes the "covered nodes join the must-run seed and are never satisfied-from-prior" rule to it.
- The M4 demo wiring (T63), which this ticket blocks.
- Any temptation to let teardown reshape the graph, react to runtime-discovered work, or run on a schedule after the process exits — the graph shape never changes at runtime and dagr is not a scheduler; cleanup after grace is best-effort by design (C16) and residual debris is the province of per-run temp-dir conventions, not new orchestration.
