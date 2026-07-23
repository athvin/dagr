# 070 · T58 — C27: resume core

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C27
> **Branch:** `feat/t58-resume-core` · **Depends on:** T41, T54a, T55, T57 · **Blocks:** T54b, T59

## Why / context
This ticket builds the `resume` verb — the machinery that lets a killed or half-finished run continue instead of repeating expensive work. It stands on the fingerprints from T41 (structural fingerprint gates resume, policy hash diffs-and-proceeds), the durable-output declaration and recording from T57 (references live in the prior artifact), scratch-under-the-run-store from T54a, and the CLI contract from T55 (where the verb is stubbed until now). It is governed by **C27 · Resume** in `docs/arch.md`, whose amended, demand-driven algorithm resolves the component's old self-contradiction. The verb wires the gate, the plan, and the execution hand-off; the exhaustive behavioural proofs land in T59, and scratch carry-forward proper lands in T54b.

## Objective
Implement the resume verb so that, given a prior run's directory in the run store, dagr computes a demand-driven re-execution plan against this binary and executes only what must run, carrying everything else forward.

Concrete pieces of work:
- **Gate.** Verify the prior run against this binary before any planning: structural-fingerprint match (mismatch refuses and prints the structural diff), fingerprint algorithm-version comparability (a distinct "cannot compare" refusal), and tool-version match (v1 refuses across tool versions with its own message). A policy-hash divergence is *not* a refusal — it prints a per-node policy diff and proceeds.
- **Invocation derivation.** Derive parameters and the data interval from the prior artifact. A supplied value that conflicts refuses with a diff; a force flag overrides the conflict and is recorded in the resumed artifact. Refuse, up front, a prior run whose run store is gone, saying so.
- **Reference existence check.** Before anything is skipped, cheaply existence-check the durable references of candidate nodes; a dangling reference fails the resume *plan*, not the eleventh executing node.
- **The seed / closure / demand algorithm** (amended C27, three steps): seed = every node whose prior terminal state was not `succeeded`, plus every node covered by a teardown that executed in the prior run; close downward (everything reachable from the seed re-runs, trigger rule re-evaluated against this run's states); resolve demand upward (a re-running node demands its data inputs — a durable producer with an intact reference is filled by rehydration; a non-durable demanded producer joins the must-run set and cascades its own demands).
- **Satisfied-from-prior marking.** Every prior success left outside the must-run set is `satisfied-from-prior` — durable or not — carrying its originating run identity.
- **Slot filling and copy-forward.** Fill re-running consumers' input slots by rehydrating durable producers on demand; copy durable references forward into the resumed artifact so it is self-contained.
- **Lineage.** Produce a resumed run artifact linked to both its immediate parent run and its lineage root run.

## Test plan (write these first — TDD)

**Structural-fingerprint refusal with diff.** Given a prior run whose recorded structural fingerprint differs from this binary's (a node renamed or rewired), when resume runs against it, then it refuses without executing any node and prints the structural difference.

**Policy-only change proceeds with per-node diff.** Given a prior run identical structurally but with a changed policy value (e.g. a raised timeout), when resume runs, then it does not refuse — it prints the per-node policy diff and proceeds to plan and execute.

**Algorithm-version refusal is distinct.** Given a prior run whose fingerprint algorithm version is not comparable to this binary's, when resume runs, then it refuses with a "cannot compare" message that is distinguishable from the structural-mismatch refusal.

**Tool-version refusal is distinct.** Given a prior run recorded by a different tool version, when resume runs, then it refuses with the cross-tool-version message (per v1's no-cross-version promise), distinguishable from both the structural and algorithm-version refusals.

**Missing run store refuses up front.** Given a prior run whose run store no longer exists, when resume runs, then it refuses before planning with a message stating the store is gone.

**Parameters derived from the prior artifact.** Given a prior run invoked with a set of parameters and a data interval, when resume runs with no parameter overrides, then the resumed run uses exactly the prior parameters and interval.

**Parameter conflict refuses with a diff.** Given the same prior run, when resume runs with a parameter value that conflicts with the prior artifact and no force flag, then it refuses and prints the parameter diff.

**Force flag overrides and is recorded.** Given the same conflicting parameter, when resume runs with the force flag, then it proceeds with the override and the resumed artifact records that force was used.

**Dangling durable reference fails the plan.** Given a prior run in which a candidate durable node's referenced object has been deleted from storage, when resume runs, then the existence check fails the resume plan before any node executes, naming the offending reference.

**Full-success resume is a no-op.** Given a prior run in which every node ended `succeeded`, when resume runs, then the seed is empty, no node re-executes, and the process exits successfully.

**Durable success is satisfied and rehydrated on demand.** Given a prior run where a durable producer succeeded and a downstream consumer must re-run, when resume runs, then the producer is marked `satisfied-from-prior` (carrying its originating run identity), is not re-executed, and the re-running consumer receives the rehydrated value.

**In-memory success re-runs only when demanded.** Given a prior run where a node with an in-memory (non-durable) output succeeded: (a) when nothing that re-runs demands its value, then it is `satisfied-from-prior` and is not re-executed; (b) when a re-executing consumer demands its value, then it joins the must-run set, re-executes, and its own upstream demands cascade.

**Teardown-covered node is re-executed.** Given a prior run where a node was covered by a teardown that executed, when resume runs, then that node is in the seed and re-executes even though it previously succeeded (its durable output may have been destroyed).

**Undemanded prior success is satisfied even when not durable — cleanup-after-publish.** Given the cleanup-after-publish shape (a `publish` node that succeeded, is ordering-only, and whose value nothing demands; a `cleanup` node downstream), when resume runs, then `publish` is `satisfied-from-prior` despite being non-durable, `cleanup` re-runs, and `cleanup`'s trigger rule sees a success-like upstream and fires.

**Downward closure re-runs reachable nodes.** Given a prior run with a node in the seed and successors downstream of it, when resume runs, then every node reachable from the seed is re-run with its trigger rule re-evaluated against this run's fresh states.

**Resumed artifact is linked and self-contained.** Given any resume that executes, when it completes, then it produces its own run artifact whose header links to both the immediate parent run and the lineage root run, and durable references from carried-forward nodes are copied into that artifact so it stands alone.

**Multi-generation lineage.** Given a run resumed from a run that was itself a resume, when the second resume completes, then the immediate parent is the prior resumed run and the lineage root is the original run.

## Definition of done
- [ ] Resuming against a mismatched structural fingerprint refuses and prints the structural difference; no node executes.
- [ ] A policy-only change proceeds and prints the per-node policy diff (it is not a refusal — the raised-timeout case is the motivating case for resume).
- [ ] A fingerprint algorithm-version mismatch refuses as "cannot compare", distinct from the structural-mismatch refusal.
- [ ] A tool-version mismatch refuses with the cross-tool-version message (v1 makes no cross-version resume promise), distinct from the other refusals.
- [ ] A prior run whose run store is gone is refused up front, with a message that says so.
- [ ] Parameters and the data interval are derived from the prior artifact when not overridden.
- [ ] Supplying parameters that conflict with the prior run refuses with a diff; the force flag overrides the conflict and its use is recorded in the resumed artifact.
- [ ] Durable references of candidate nodes get a cheap existence check up front; a dangling reference fails the resume plan before execution begins, naming the reference.
- [ ] The must-run seed = every node whose prior terminal state was not `succeeded`, plus every node covered by a teardown that executed in the prior run (C17).
- [ ] Downward closure: everything reachable from the seed re-runs, its trigger rule re-evaluated against this run's states.
- [ ] Upward demand resolution: a re-running node demands its data inputs; a durable producer with an intact reference is filled by rehydration; a demanded non-durable producer joins the must-run set and cascades its own demands upward.
- [ ] A node that succeeded with an in-memory output is re-executed when and only when a re-executing consumer demands its value.
- [ ] A satisfied node is not re-executed, and a re-executing consumer that demands its value receives the rehydrated value.
- [ ] Every prior success left outside the must-run set is marked `satisfied-from-prior` — durable or not — and carries its originating run identity.
- [ ] The cleanup-after-publish shape resumes correctly: ordering-only upstream is `satisfied-from-prior` even when non-durable, downstream re-runs, and its trigger rule fires on the success-like upstream.
- [ ] Resuming a fully successful run has an empty seed and is a no-op that exits successfully.
- [ ] The resumed run produces its own artifact, linked to both its immediate parent run and its lineage root run.
- [ ] Durable references are copied forward into the resumed artifact so every artifact is self-contained (C22).
- [ ] The resume verb (stubbed in T55) is wired to this implementation; refusal exit codes align with the C26 exit-code table (the resume-refusal code, shared by replay refusal).
- [ ] Scratch carry-forward for re-executing nodes is deferred to T54b and not implemented here (the plan surfaces which nodes will re-execute so T54b can consume it).
- [ ] The rustdoc for the durable, non-rehydratable in-memory pressure (a re-running consumer forces re-execution of in-memory producers) is stated plainly to developers per C10's authoring guidance.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The exhaustive resume acceptance suite (fingerprint refusal with diff, policy proceed-with-diff, satisfied/re-run matrix, dangling-reference plan failure, multi-generation lineage, parameter-conflict and force recording) — that is **T59**, which depends on this ticket.
- Scratch copy-forward for re-executing nodes (continue-from-checkpoint) — that is **T54b**; this ticket only exposes which nodes re-execute so T54b can carry their scratch forward.
- Any change to the fingerprint algorithm or the durable-output declaration/recording contract — those are settled in T41 and T57 respectively; this ticket consumes them unchanged.
- The CLI verb parsing, typed parameter struct, and exit-code table — owned by T55/C26; this ticket only wires the resume behaviour behind the existing verb.
- Single-node replay (`replay-from-run`) — a separate C26 path; it shares the refusal code but is not resume.
- Cross-tool-version resume, distributed or coordinated multi-run resume, backfilling missed intervals, mutating the graph shape between the prior run and the resume, or any form of scheduling — all permanently outside dagr's scope; resume never re-plans a *different* graph, only re-executes the same structural fingerprint.
