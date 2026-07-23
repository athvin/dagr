# 028 · T18 — C11: readiness tracker

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C11
> **Branch:** `feat/t18-readiness-tracker` · **Depends on:** T14, T0.4 · **Blocks:** T24

## Why / context
C11 (arch.md · "C11 · Readiness tracker") decides what is eligible to run, and when. It maintains a remaining-dependency countdown per node; when any node reaches a terminal state, each dependent is decremented, and once *all* of a node's upstreams are terminal its trigger rule is evaluated (arch.md · "Vocabulary — terminal states and trigger rules"): a node whose rule fires becomes ready, and a node whose rule can never fire is immediately assigned its propagated terminal state without executing. This ticket builds that pure decision engine on top of the precomputed dependency counts and execution structure from T14 (C7 assembly), evaluating against the normative fires / can-never-fire table fixed by T0.4 — M1 ships only the `all-succeeded` default rule, but the tracker is written against the *final* three-rule interface so T34 can light up `all-terminal` and `any-failed` without reshaping it. The load-bearing behaviour to prove here is that a node becomes ready the instant its own dependencies allow, never batched into level-synchronous waves — so a diamond with one slow branch does not delay the fast branch's independent descendants. T24 (the M1 run-loop driver) consumes this tracker to admit and spawn work.

## Objective
Build the readiness tracker as a pure state machine that consumes T14's precomputed dependency structure and, given terminal-state notifications, decides the next ready nodes and the immediate propagated-terminal assignments — with no scheduling, spawning, timing, or event-writing of its own.

- A per-node **remaining-dependency countdown**, seeded from T14's precomputed dependency counts, that is decremented as each upstream reaches a terminal state.
- A **notify-terminal** operation the driver calls when a node reaches any terminal state, which decrements every dependent and returns the set of downstream decisions that notification unlocked.
- **Trigger-rule evaluation gated on all-upstreams-terminal:** a node's rule is evaluated only once its countdown reaches zero (every upstream terminal), never on a partial result — matching the Vocabulary invariant.
- **Fires → ready:** a node whose rule fires against the current upstream state picture is emitted as *ready to run*.
- **Can-never-fire → immediate propagated terminal:** a node whose rule can never fire is assigned its propagated terminal state (`upstream-failed`, `upstream-skipped`, or `skipped` for an `any-failed` contingency that never arose) **without executing**, per the T0.4 decision table — and that propagated-terminal assignment is itself a terminal-state notification that cascades to *its* dependents.
- **Full rule interface, `all-succeeded` behaviour:** the evaluation seam accepts all three rules from T0.4's closed set; M1 wires and tests the `all-succeeded` default path (fires when all upstreams success-like; propagates `upstream-skipped` / `cancelled` / `upstream-failed` per the table), leaving `all-terminal` and `any-failed` *runtime firing* to T34 while keeping their table entries reachable through the same interface.
- **`satisfied-from-prior` upstreams count success-like**, so a resumed prior success satisfies a downstream `all-succeeded` (C11 "covered explicitly").
- **Source-node seeding:** nodes with zero dependencies are ready from the start (countdown already zero), surfaced through the tracker's initial-ready set so the driver has a starting frontier.
- **Terminal accounting:** the tracker tracks, per node, whether it has been decided (executed-terminal from the driver, or propagated-terminal by the tracker), so the driver can ask whether anything is still pending — the basis for T24's "run ends when nothing is pending or in flight".

## Test plan (write these first — TDD)
Each scenario is independently checkable and drives the tracker directly with synthetic pipelines and injected terminal outcomes (no real task execution, no runtime, no clock). Scenarios are derived from C11's acceptance criteria and T0.4's decision table.

- **Countdown seeds from precomputed dependency counts.** Setup: a small pipeline (a source, two middles depending on the source, a sink depending on both) assembled through T14 so dependency counts are precomputed. Action: build the tracker from the immutable pipeline. Expected: the source's countdown is zero and it appears in the initial-ready set; each middle's countdown is one; the sink's countdown is two; nothing else is ready yet.

- **Decrement on terminal unlocks the exact dependents.** Setup: the pipeline above, tracker built. Action: notify the source `succeeded`. Expected: both middles decrement to zero and are emitted as newly ready; the sink decrements by nothing (it does not depend on the source) and remains at two; the source is now decided.

- **Rule is not evaluated on a partial result.** Setup: the diamond sink depending on two upstreams, both still pending. Action: notify only the first upstream `succeeded`. Expected: the sink's countdown drops to one, its trigger rule is *not* evaluated, and it is neither emitted ready nor assigned a terminal state — no early fire.

- **`all-succeeded` fires when the last upstream completes.** Setup: the diamond sink, one upstream already `succeeded` (countdown at one). Action: notify the second upstream `succeeded`. Expected: the sink's countdown reaches zero, its `all-succeeded` rule fires, and the sink is emitted as newly ready.

- **`all-succeeded` can-never-fire → `upstream-failed`.** Setup: a node with two `all-succeeded` upstreams. Action: notify one upstream `failed` and the other `succeeded`. Expected: once both are terminal the rule can never fire; the node is assigned `upstream-failed` without being emitted ready, and the assignment carries the originating node's identity (the failed upstream).

- **`all-succeeded` can-never-fire → `upstream-skipped`.** Setup: a node whose `all-succeeded` upstreams end with every non-success upstream skip-like. Action: notify one upstream `skipped` (or `upstream-skipped`) and the rest `succeeded`. Expected: the node is assigned `upstream-skipped` without executing, carrying the originating skip node's identity.

- **`all-succeeded` can-never-fire → `cancelled`.** Setup: a node whose `all-succeeded` upstreams end with every non-success upstream stop-like. Action: notify one upstream `cancelled` and the rest `succeeded`. Expected: the node is assigned `cancelled` without executing.

- **Mixed non-success classes propagate `upstream-failed`.** Setup: an `all-succeeded` node with three upstreams that end `succeeded`, `skipped`, and `failed`. Action: notify all three terminal. Expected: because the non-success set is not all-skip-like and not all-stop-like, the node is assigned `upstream-failed` (the "otherwise" branch of the table).

- **`satisfied-from-prior` counts success-like.** Setup: an `all-succeeded` node with two upstreams. Action: notify one `succeeded` and the other `satisfied-from-prior`. Expected: the rule fires and the node becomes ready — the resumed prior success satisfies the join.

- **Propagated terminal cascades.** Setup: a chain where node B (`all-succeeded`) depends on A, and node C (`all-succeeded`) depends on B. Action: notify A `failed`. Expected: B is assigned `upstream-failed` without executing, and that assignment is itself treated as a terminal notification that decrements C, which is then assigned `upstream-failed` in turn — the propagation reaches C without any intervening execution.

- **Diamond proves no wave batching.** Setup: a diamond — source S; a fast branch F and a slow branch W both depending on S; F has an independent descendant Fd depending only on F; the join J depends on both F and W. Action: notify S `succeeded`, then F `succeeded`, while W is still pending. Expected: Fd (which depends only on F) is emitted ready immediately, *before* W reaches any terminal state and before J is eligible; J stays at countdown one and is not ready. This confirms the fast branch's descendant is not delayed by the slow branch, and that readiness is per-node, not level-synchronous.

- **Source nodes are ready without any notification.** Setup: a pipeline with two independent source nodes and one sink depending on both. Action: build the tracker and read its initial-ready set before notifying anything. Expected: both sources are ready from the start; the sink is not.

- **Every node ends in exactly one terminal state.** Setup: any of the pipelines above, driven to completion by notifying executed-terminal outcomes for ready nodes and letting the tracker assign propagated terminals. Action: drive until nothing is pending. Expected: every node has exactly one recorded terminal state — an executed-terminal from the driver or a single propagated-terminal from the tracker — and no node is assigned a terminal state twice.

- **Pending accounting reports run completion.** Setup: a pipeline driven to the point where every node has a terminal state. Action: query the tracker's pending count after each notification. Expected: the pending count monotonically reaches zero exactly when the last node (executed or propagated) becomes terminal, and is nonzero before that — giving T24 its "nothing pending" signal (the "in flight" half is the driver's, not this ticket's).

- **Full rule interface is present though only `all-succeeded` fires in M1.** Setup: the tracker's rule-evaluation seam. Action: inspect the interface it evaluates against. Expected: it accepts all three rules from T0.4's closed set (`all-succeeded`, `all-terminal`, `any-failed`) so T34 can enable the other two without reshaping the tracker; M1's tests exercise the `all-succeeded` fires and can-never-fire branches, and the seam does not hard-code `all-succeeded` as the only expressible rule.

## Definition of done
- [ ] The tracker maintains a per-node remaining-dependency countdown seeded from T14's precomputed dependency counts, decremented each time an upstream reaches a terminal state (C11).
- [ ] A node's trigger rule is evaluated only once *all* of its upstreams are terminal (countdown zero) — never on a partial result (C11, Vocabulary).
- [ ] A node whose rule fires becomes ready and is surfaced to the caller as ready-to-run (C11).
- [ ] A node whose rule can never fire is immediately assigned its propagated terminal state — `upstream-failed`, `upstream-skipped`, or `skipped` for an `any-failed` contingency that never arose — without executing, per T0.4's decision table (C11, Vocabulary).
- [ ] `all-succeeded` propagation follows the table: `upstream-skipped` when every non-success upstream is skip-like, `cancelled` when every non-success upstream is stop-like, `upstream-failed` otherwise (C11, C15, Vocabulary).
- [ ] Propagated `upstream-skipped` / `upstream-failed` assignments carry the originating node's identity (Vocabulary, T0.4).
- [ ] A propagated-terminal assignment is itself treated as a terminal notification and cascades to that node's dependents without any intervening execution (C11).
- [ ] `satisfied-from-prior` upstreams count as success-like, so a resumed prior success satisfies a downstream `all-succeeded` — C11's "covered explicitly" case (C11, Vocabulary).
- [ ] A node with zero dependencies is ready from the start; the tracker exposes an initial-ready frontier for the driver (C11).
- [ ] A node whose dependencies complete early is emitted ready before unrelated slower work finishes — work is never batched into waves where a whole level must finish before the next begins (C11 acceptance).
- [ ] In a diamond with one slow branch, the fast branch's independent descendants are emitted ready without waiting on the slow branch, and the join stays not-ready until both branches are terminal (C11 acceptance).
- [ ] The tracker tracks per-node decided/pending status and exposes a pending count that reaches zero exactly when every node is terminal, giving T24 its "nothing pending" half of the run-end condition (C11 acceptance — the "in flight" half stays with T24).
- [ ] Every node ends in exactly one terminal state and is assigned that state exactly once (no double assignment across executed-terminal and propagated-terminal paths) (C11 acceptance, Vocabulary).
- [ ] The rule-evaluation seam accepts all three rules from T0.4's closed set so T34 can enable `all-terminal` and `any-failed` runtime firing without reshaping the tracker; M1 wires and tests the `all-succeeded` path only (task scope; C11, T0.4).
- [ ] The tracker is a pure decision engine: no spawning, no scheduling, no timers, no event writing, no I/O — it consumes terminal notifications and emits ready-node and propagated-terminal decisions only (C11 boundary; C12/C24/C19 belong to other tickets).
- [ ] Rustdoc on the tracker states the countdown model, the all-upstreams-terminal evaluation gate, and the fires / can-never-fire → propagated-state mapping, pointing to T0.4 for the normative table.
- [ ] The Test plan scenarios above exist as tests and pass — including the diamond no-wave-batching test and the exactly-one-terminal-state check.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **The run-loop driver (T24):** admitting ready nodes, spawning attempts, feeding outcomes back, run-started/run-finished events, the bounded grace wait for zombie closures, and the "in flight" half of the run-end condition. This ticket only supplies the pure readiness decisions T24 drives; the tracker does not spawn or time anything.
- **Admission control (C12, T22):** capacity pools, permit acquisition, oldest-ready-first ordering, and bounded bypass. The tracker decides *what is ready*, not *what is admitted* — a ready node may still wait on a permit.
- **Failure policy and full trigger-rule runtime (C15, T34):** stop-on-first-failure vs continue-independent, in-flight drain, stop-mode contingency admission, `cancelled`-on-pending-unrelated, and the *runtime firing* of `all-terminal` and `any-failed` (including `any-failed` → `skipped` when the contingency never arose). This ticket keeps those rules' table entries reachable through the interface but only fires `all-succeeded` in M1.
- **The termination property test (T25):** the randomized-DAG, randomized-outcome deadlock-freedom proof lands in its own ticket; this ticket ships the deterministic per-shape tests only.
- **The trigger-rule / terminal-state tables themselves (T0.4):** this ticket implements against that decision record and does not redefine states, classes, or the fires / can-never-fire mapping.
- **Assembly and precomputation (C7, T14):** dependency counts, consumer counts, execution order, and the fingerprint slot are consumed here, not computed here.
- **Teardown nodes (C17, T52)** and the event stream (C19, T19): the tracker neither runs teardown nor writes events; it emits decisions the driver acts on.
- **Scope-boundary temptations to resist:** no scheduler behaviour (no priorities, no time-based triggers, no backfill), no runtime graph mutation (the dependency structure is fixed at assembly and the countdown is derived from it), no wave/level batching abstraction, and no by-name runtime rewiring — the graph shape never changes at runtime.
