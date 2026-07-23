# 078 · T63 — M4 demo: kill, resume, and review

> **Milestone:** M4 · **Size:** M · **Type:** feature (demo) · **Components:** M4 gate
> **Branch:** `feat/t63-m4-demo-kill-resume-review` · **Depends on:** T36, T51, T52, T56, T59, T61, T62 · **Blocks:** T65

## Why / context
This ticket is the M4 milestone gate. The Build order names M4 ("It is operable") *done when* "a pipeline killed mid-run resumes and skips completed durable work, and a structural change to the graph is caught in code review." It exists to prove those two guarantees end-to-end as executable CI tests rather than as unit-level assertions on the individual components: resume (C27, via T58/T59), the durable scratch store and carry-forward (C18, via T53/T54a/T54b), teardown lifecycle (C17, via T52), the CLI contract and its exit-code table (C26, via T55/T56), the structure snapshot surface (C28, via T61), and the full-pipeline fakes harness (C28, via T62). It builds on the run-store shutdown/flush guarantees of C16 (T36) so a "kill" is a real signal-driven cancellation with a complete, fsynced stream, and on groups (C6, T51) so the review scenario can also demonstrate a group rename is review-visible without touching the fingerprint. It blocks T65, the system acceptance gate.

## Objective
Assemble one reference pipeline with a durable stage boundary and a teardown, and drive it through the real CLI verbs (`run`, `resume`) and the C28 structure-snapshot surface as CI tests that demonstrate the two M4 done-when behaviours as directly observable outcomes.

- Build a **kill/resume** reference pipeline with a durable stage boundary: an expensive upstream node whose output is durable (declares the reference contract per C27/T57), at least one in-memory-output node, a downstream consumer, a cleanup-after-publish shape (an ordering-only publish whose value nothing demands, plus a downstream cleanup node), and a teardown node covering part of the graph — all backed by fakes via the T62 harness.
- Run the pipeline against a run store on storage that outlives the process, then send a termination signal mid-run (after the durable upstream has succeeded and recorded its reference, but before the run completes), and assert the killed run flushes a complete artifact and stream (C16).
- Resume the killed run from the same run store and assert the durable upstream is `satisfied-from-prior` (skipped, not re-executed), the demanded slot is filled by rehydration from the recorded reference, the must-run seed re-executes and its scratch is carried forward, the teardown-covered node is re-run (never satisfied-from-prior), the cleanup-after-publish shape resumes correctly, and the resumed run produces its own artifact linked to parent and lineage root.
- Add a **review** scenario: check in a canonical structure fixture for the reference pipeline via the C28 surface, then mutate the assembled graph (a rewiring and, separately, a group rename) and assert the structure test fails with a structural diff, while a rebuild alone does not — proving unintended rewiring fails review rather than production.
- Wire all three scenarios as first-class CI tests that run within the M4 test budget and are deterministic and portable across runners.

## Test plan (write these first — TDD)

**Kill mid-run flushes a complete, resumable artifact.**
Setup: assemble the reference pipeline against fakes via the T62 harness, pointed at a run-store base on storage that outlives the process; script the durable upstream node to succeed and record its reference, and hold a later node in-flight. Action: start the run and deliver a termination signal (SIGTERM) once the durable upstream is observed terminal-`succeeded` but before the run completes. Expected outcome: the process reacts within the C16 shutdown budget; the run's event stream is complete and gapless with exactly one terminal state per node reached and every in-flight node ending on the cancellation path; the artifact is written and fsynced; the durable upstream's recorded reference is present in its attempt record; the run store still holds the run directory (it was not deleted), so the run is resumable.

**Resume skips completed durable work and rehydrates on demand.**
Setup: the killed run from the prior scenario, its run directory intact in the run store. Action: invoke the `resume` verb against that prior run through the CLI surface. Expected outcome: the durable upstream node is marked `satisfied-from-prior` and is not re-executed; the downstream consumer that demands its value receives the value rehydrated from the recorded reference (a cheap existence check passed first); the resumed run completes successfully; and the resumed run's terminal-state picture shows the previously-succeeded durable work as satisfied rather than re-run.

**The must-run seed re-executes and carries scratch forward.**
Setup: the reference pipeline includes a node that did not succeed in the prior run (it was in-flight at kill time) and wrote a scratch checkpoint on its interrupted attempt. Action: resume the killed run. Expected outcome: that node is in the must-run seed and re-executes; on re-execution it observes its prior-run scratch carried forward into the new run's namespace (the checkpoint is readable), continuing rather than starting over; downward closure re-runs everything reachable from the seed.

**An in-memory success re-runs only when demanded.**
Setup: the pipeline contains a node whose prior-run output was an in-memory value (not durable) and that succeeded before the kill; one variant where a re-executing consumer demands that value and one variant where nothing in the must-run set demands it. Action: resume each variant. Expected outcome: in the demanded variant the in-memory producer re-executes (it cannot be rehydrated) and its own demands cascade upward; in the undemanded variant the same node is `satisfied-from-prior` even though it is not durable — its effect stands and no value is needed.

**Cleanup-after-publish resumes correctly.**
Setup: the pipeline includes an ordering-only publish node that succeeded before the kill (nothing demands its value) and a downstream cleanup node with a trigger rule that fires on a success-like upstream. Action: resume the killed run. Expected outcome: publish is `satisfied-from-prior` (undemanded, ordering-only), the cleanup node re-runs, and cleanup's trigger rule sees the satisfied upstream as success-like and fires — the shape resumes without re-publishing.

**A teardown-covered node is never satisfied-from-prior.**
Setup: the pipeline has a teardown node covering a producer node, and that teardown executed in the prior (killed) run. Action: resume the killed run. Expected outcome: the covered producer is re-executed on resume — never `satisfied-from-prior` — because the teardown may have destroyed its durable output; the teardown itself runs again under its fresh, uncancelled signal and its own deadline.

**A dangling durable reference fails the resume plan before execution.**
Setup: the killed run's run directory intact, but the durable object the upstream's recorded reference points to is deleted from underneath. Action: invoke `resume`. Expected outcome: resume refuses during planning — before any node executes — with a message identifying the dangling reference; the failure is a plan failure, not a mid-run crash on a later node.

**Resuming a fully successful run is a no-op.**
Setup: run the reference pipeline to full success (no kill), run store intact. Action: invoke `resume` against that completed run. Expected outcome: the seed is empty, nothing re-executes, and the verb exits with the success code.

**A run whose store is gone is not resumable.**
Setup: a prior run whose run-store directory has been removed. Action: invoke `resume`. Expected outcome: the verb refuses with the resume-refusal exit code and a message stating the original run's store is gone, so it cannot be resumed.

**Resume produces a lineage-linked artifact.**
Setup: resume the killed run, then resume the resumed run (a second generation). Action: read the two resumed artifacts. Expected outcome: each resumed run has its own artifact; each is linked to its immediate parent and to the lineage root; durable references are copied forward so each resumed artifact is self-contained.

**Structural change is caught by the structure fixture (rewiring).**
Setup: check in the canonical, stably-ordered structure fixture for the reference pipeline generated through the blessed single-command update flow; then, in the assembled pipeline under test, change one data edge's wiring. Action: run the C28 structure test against the checked-in fixture. Expected outcome: the test fails and its output is a structural diff naming the changed edge — the rewiring is caught in review, not in production.

**A group rename is review-visible but does not touch the fingerprint.**
Setup: the reference pipeline with a checked-in structure fixture; rename a presentation-only group label. Action: run the structure test, and separately compute the structural fingerprint before and after the rename. Expected outcome: the structure test fails with a diff (the rename is review-visible per C6/C28), while the structural fingerprint is byte-identical across the rename — the label is excluded from identity, so resume across the rename would still be permitted.

**A rebuild alone does not fail the structure test.**
Setup: the reference pipeline with its checked-in fixture, rebuilt without any graph change. Action: run the structure test. Expected outcome: it passes — the fixture excludes volatile header fields and is a semantic comparison, so a clean rebuild (or toolchain bump) is not a spurious failure.

**Fixture regeneration is a single blessed command.**
Setup: intentionally change the pipeline (add a node) and run the documented fixture-update flow. Action: run the update command, then re-run the structure test. Expected outcome: the update command rewrites the canonical, stably-ordered fixture; the diff between old and new fixture is reviewable; and after the deliberate update the structure test passes — proving the blessed flow, not a silent auto-rewrite.

**Deterministic and portable in CI.**
Setup: run all three scenarios in CI against fakes via the T62 harness with the run store on ephemeral-but-process-outliving storage. Action: execute the CI job on a fresh runner. Expected outcome: the kill/resume terminal-state picture, the resumed artifacts' satisfied/re-run classification, and the structure-test verdicts are identical across runners; the full suite completes within the M4 seconds-scale budget.

## Definition of done
- [ ] The kill/resume reference pipeline is assembled against fakes via the T62 harness and includes a durable stage boundary (durable-marked upstream with the reference contract per C27/T57), an in-memory-output node, a downstream demanding consumer, a cleanup-after-publish shape, a scratch-checkpointing node, and a teardown node covering a producer.
- [ ] A mid-run termination signal cancels the run within the C16 shutdown budget and the run writes a complete, gapless, fsynced event stream and artifact before exit, with the run directory left intact in the run store (C16).
- [ ] Resuming against the matching structural fingerprint proceeds; the resume runs from the prior run's run-store directory (C27).
- [ ] A node whose prior terminal state was `succeeded` and durable, whose value is demanded, is not re-executed and its consumer receives the rehydrated value; a cheap reference existence check runs before any skip (C27).
- [ ] A node that succeeded with an in-memory output is re-executed when and only when a re-executing consumer demands its value (C27).
- [ ] A prior success whose value nothing demands is `satisfied-from-prior` even when not durable, verified with the cleanup-after-publish shape: ordering-only publish is satisfied, cleanup re-runs, and cleanup's rule fires on the success-like upstream (C27).
- [ ] Every node in the must-run seed (non-`succeeded` prior state plus teardown-covered nodes) re-executes; downward closure re-runs everything reachable from the seed (C27).
- [ ] A node covered by a teardown that executed in the prior run is re-executed on resume, never `satisfied-from-prior` (C17, C27).
- [ ] The teardown runs on resume under a fresh, uncancelled signal and its own deadline; its own outcome does not change the run's overall outcome (C17).
- [ ] A re-executing node observes its prior-run scratch carried forward into the new run's namespace and continues from its checkpoint (C18, via T54b).
- [ ] A dangling durable reference fails the resume plan before execution begins, with a message identifying the reference (C27).
- [ ] Resuming a fully successful run is a no-op that exits with the success code (C27).
- [ ] A run whose run store is gone refuses with the resume-refusal exit code and a message that says so (C26, C27).
- [ ] Each resumed run produces its own artifact linked to its immediate parent and to the lineage root, with durable references copied forward so the resumed artifact is self-contained; verified across a multi-generation resume (C27).
- [ ] Resume is exercised through the library-supplied `resume` CLI verb, and refusals surface the resume-refusal exit code from the C26 table (C26).
- [ ] A checked-in canonical, stably-ordered structure fixture exists for the reference pipeline; the structure test fails with a structural diff on a rewiring and on a group rename, and does not fail on a clean rebuild or toolchain bump (C6, C28).
- [ ] A group rename fails the structure test (review-visible) while leaving the structural fingerprint byte-identical, demonstrating groups are excluded from identity (C6, C21).
- [ ] The fixture-update flow is a single documented command that rewrites the canonical fixture for review, and the demo exercises it on a deliberate change (C28).
- [ ] All three scenarios are wired as CI tests that run within the M4 seconds-scale budget and are deterministic and portable across runners (C28).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Re-proving the unit-level resume acceptance matrix (T59), the CLI acceptance matrix (T56), the structure-snapshot mechanics (T61), the scratch carry-forward mechanism (T54b), or the fakes harness itself (T62); this ticket consumes those invariants end-to-end and must not duplicate their coverage.
- The internals of the resume seed/closure/demand algorithm, the fingerprint algorithm, and the durable-output contract — owned by T58, T0.7, and T57/T0.8 respectively; the demo asserts observable outcomes, not their implementation.
- Policy-hash divergence proceed-with-diff, parameter-conflict refusal, and force-flag recording — those resume behaviours are covered by T58/T59 and are not part of the M4 done-when this gate proves.
- Artifact rendering, diagrams, and the "which node was slowest, waiting or working?" question — M3 (C20–C25) and their tickets; this demo reads terminal states, satisfied/re-run classification, lineage links, and structural diffs, not human-reviewable renders.
- The system acceptance gate consolidation and the criteria-matrix wiring — that is T65, which this ticket blocks; do not pull cross-milestone acceptance assembly into here.
- Scope-boundary temptations that must not creep in: turning "resume" into a scheduler, backfill orchestrator, or distributed/multi-machine restart; introducing a metadata store, a web/UI review surface, or a DSL to express the pipeline; or mutating the graph shape at runtime to make the kill/resume scenario more dramatic — the graph is fixed at assembly, execution is single-machine, and review is a checked-in fixture diff, not a service.
