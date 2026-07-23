# 080 · T65 — System acceptance gate

> **Milestone:** M4 · **Size:** M · **Type:** feature (gate) · **Components:** system-level acceptance
> **Branch:** `feat/t65-system-acceptance-gate` · **Depends on:** T7, T28, T38, T49, T63, T64, T69, T70 · **Blocks:** —

## Why / context
This is the terminal gate that turns dagr from a collection of passing component tests into a product: it enforces, in one CI job, that all eight system-level acceptance criteria (arch.md "System-level acceptance") hold *simultaneously*. It builds directly on the criteria-matrix scaffold from T7, the four milestone demos (T28/T38/T49/T63) that execute the spec's per-milestone done-whens, the documentation deliverables (T64), the scale benchmark (T69), and the platform matrix (T70). The two determinism checks it owns are the machine-classed halves of criterion 4: *structural* determinism (two builds, two toolchains → identical fingerprints and byte-identical graph artifacts, per C20/C21) and *interpretive* determinism (scripted outcomes replayed through the T62 / C28 fakes harness → identical terminal states, propagation decisions, and run artifact, per C22/C28). It locks the closure claim of criterion 8: the checked-in criteria matrix is the single source of truth, every criterion appears exactly once as machine/human/disclaimer, and a criterion absent from the matrix fails CI. It crosses no scope boundary — it only asserts existing behaviour, adds no runtime feature, and never coordinates across processes.

## Objective
Assemble and enforce the final acceptance gate as a CI job that fails unless the whole product invariant holds. Concretely:

- **Machine-criterion coverage completeness.** Extend the T7 coverage-matrix checker so it verifies that *every* machine-classed criterion — the criteria 1–8 in "System-level acceptance" plus every machine-classed acceptance criterion across C1–C28 — maps to a named, currently-passing test id, and that every criterion referenced anywhere in arch.md appears in the matrix exactly once with a class of machine, human, or disclaimer. An unmapped machine criterion, a criterion missing from the matrix, a duplicate matrix entry, or a matrix entry pointing at a non-existent or failing test all fail the gate.
- **Human-criterion release-checklist binding.** Verify that every human-classed criterion in the matrix (diagram readability C24, documentation-at-point-of-use C21, the thirty-minute walkthrough, C1's types-readable-from-the-declaration, and the criterion-1 walkthrough-duration audit) has a corresponding, version-controlled release-checklist item, and that the disclaimer entry (criterion 4c — task-side external effects) is present and unclassified-as-machine. Checklist coverage is itself asserted by the gate: a human criterion with no checklist line fails CI.
- **Structural-determinism check (criterion 4a).** Add a CI job that builds the reference pipeline from identical source on two distinct pinned toolchains, produces the graph artifact from each, and asserts the structural fingerprint and policy hash are identical across builds and that the graph artifacts are byte-identical with the generation-time header field excluded.
- **Interpretive-determinism check (criterion 4b).** Add a test that drives the reference pipeline through the T62 full-pipeline fakes harness twice with the same scripted task outcomes and asserts identical terminal states per node, identical propagation decisions (originated vs propagated skips/failures), and a byte-identical run artifact (volatile header fields excluded), replaying through the real scheduler.
- **Gate wiring.** Wire all of the above into a single required CI gate job on the ticket branch that depends on the demo jobs (T28/T38/T49/T63), the scale benchmark (T69), and the platform-matrix jobs (T70), so the gate is red unless every constituent is green.

## Test plan (write these first — TDD)
Each scenario is independently checkable. "The gate script" is the criterion-completeness checker; "the reference pipeline" is the checked-in pipeline used by T64's locality check and T61's structure fixture.

1. **Full machine coverage passes.** Setup: a criteria matrix in which every criterion 1–8 and every machine-classed C1–C28 criterion maps to a passing test id, every human criterion is classed `human`, and criterion 4c is classed `disclaimer`. Action: run the gate script. Expected: it exits success and prints a summary listing the count of machine, human, and disclaimer criteria.

2. **An unmapped machine criterion fails.** Setup: same matrix, but delete the test-id mapping for one machine criterion (leave it classed `machine`). Action: run the gate script. Expected: non-zero exit, and the message names exactly the offending criterion id and states it is machine-classed but unmapped.

3. **A machine criterion pointing at a non-existent test fails.** Setup: a machine criterion mapped to a test id that no test suite reports. Action: run the gate script against the collected test-id inventory. Expected: non-zero exit naming the criterion and the missing test id.

4. **A machine criterion whose mapped test is failing fails the gate.** Setup: a machine criterion mapped to a test id that exists but is in the failing set. Action: run the gate over a test report where that test failed. Expected: non-zero exit; the criterion is reported as covered-but-red, distinct from unmapped.

5. **A criterion absent from the matrix fails.** Setup: introduce a criterion id that appears in arch.md but has no row in the matrix. Action: run the gate script with the arch.md criterion inventory as input. Expected: non-zero exit naming the missing criterion and stating "absent from the matrix."

6. **A duplicate matrix entry fails.** Setup: the same criterion id appears twice in the matrix. Action: run the gate script. Expected: non-zero exit stating the criterion must appear exactly once.

7. **A human criterion with no release-checklist item fails.** Setup: a criterion classed `human` in the matrix with no matching line in the version-controlled release checklist. Action: run the checklist-binding check. Expected: non-zero exit naming the human criterion and the empty checklist slot.

8. **The disclaimer criterion is honoured.** Setup: criterion 4c classed `disclaimer`. Action: run the gate script. Expected: success; the disclaimer criterion is neither required to map to a test nor to a checklist item, and is counted in the disclaimer total.

9. **Structural determinism holds across two toolchains.** Setup: the reference pipeline source, plus two distinct pinned toolchains configured in CI. Action: build and emit the graph artifact and fingerprints on each toolchain. Expected: the structural fingerprints are equal, the policy hashes are equal, and the two graph artifacts are byte-identical after excluding only the generation-time header field.

10. **Structural determinism catches a spurious drift.** Setup: as above, but inject a change that would make the graph artifact differ byte-for-byte (for example a non-canonical field ordering) while the pipeline is semantically unchanged. Action: run the structural-determinism check. Expected: the check fails and prints the first differing byte offset or field, proving it is a real comparison and not a no-op.

11. **Interpretive determinism holds under replay.** Setup: the reference pipeline and a fixed script of task outcomes (a mix of success, one retryable-then-success, one permanent failure, and a downstream join) fed to the T62 fakes harness. Action: run the harness twice with the same script. Expected: identical per-node terminal states, identical originated-vs-propagated skip/failure markings, and a byte-identical run artifact with volatile header fields excluded; both runs complete within the T62 seconds budget.

12. **Interpretive determinism distinguishes originated from propagated states.** Setup: a script in which one node fails and two downstream nodes are consequently skipped. Action: fold the two replays' run artifacts and compare. Expected: the failing node is recorded as an originated failure and the downstream nodes as propagated (`upstream-failed` / `upstream-skipped`), identically in both replays.

13. **The gate is red when a constituent is red.** Setup: the CI dependency graph with the gate job depending on the demos, scale benchmark, and platform-matrix jobs; force one demo job to fail. Action: evaluate the gate job's required-status. Expected: the gate job does not run to success (it is blocked or fails), so a red constituent cannot be merged past the gate.

14. **The whole matrix round-trips against the live suite.** Setup: the complete, correct matrix and the actual assembled test inventory of the repository at ticket completion. Action: run the gate script in CI. Expected: success — every machine criterion in criteria 1–8 and C1–C28 is covered by a passing test, and every human criterion has a checklist line — demonstrating the product invariant holds simultaneously.

## Definition of done
- [ ] Criterion 1 (machine half): the README quickstart compiles and runs verbatim in CI, empty directory to a compiled, run, artifact-inspected two-node pipeline (owned by T64) is present in the matrix as a machine criterion mapped to its passing test.
- [ ] Criterion 1 (human half): the "walkthrough completable in under thirty minutes" audit is a version-controlled release-checklist item classed `human` in the matrix.
- [ ] Criterion 2: mis-wiring two tasks is a compile error whose message contains both type names (T8's UI test) is mapped as a machine criterion.
- [ ] Criterion 3: every run — including crashed, cancelled, assembly-failed, and bootstrap-failed runs — produces artifacts is mapped to a passing test.
- [ ] Criterion 4a (structural determinism): a CI check builds the reference pipeline on two pinned toolchains and asserts identical structural fingerprint, identical policy hash, and byte-identical graph artifacts with generation-time excluded (C20/C21), and is mapped in the matrix.
- [ ] Criterion 4b (interpretive determinism): a CI check replays scripted outcomes through the T62 / C28 fakes harness and asserts identical terminal states, identical propagation decisions, and a byte-identical run artifact (C22/C28), and is mapped in the matrix.
- [ ] Criterion 4c: the external-effects disclaimer is carried in the matrix, classed `disclaimer`, mapped to neither a test nor a checklist item.
- [ ] Criterion 5: a run's duration and resource profile are answerable entirely from artifacts with no access to the producing machine is mapped to a passing test.
- [ ] Criterion 6: the add-a-node locality claim, verified by the structure-diff on the reference pipeline (T64), is mapped as a machine criterion.
- [ ] Criterion 7: no server, database, or scheduler is required to run and produce local artifacts (with the run-store-outlives-container caveat for crash-survival/resume) is mapped to a passing test.
- [ ] Criterion 8 (machine half): the gate script verifies every machine-classed criterion in "System-level acceptance" and in C1–C28 maps to a passing, existing test, and this verification itself runs in CI from the checked-in criteria matrix.
- [ ] Criterion 8 (human half): every human-classed criterion (C24 diagram readability, C21 documentation-at-point-of-use, the thirty-minute walkthrough, C1 types-readable-from-the-declaration) has a matching version-controlled release-checklist item, checked by the gate.
- [ ] Matrix exactly-once invariant: every criterion appearing anywhere in arch.md appears in the matrix exactly once with class machine, human, or disclaimer; a missing, duplicated, or unclassified criterion fails CI.
- [ ] Platform-conditional criteria (limit detection, signal handling, flush behaviour, per T70) are named as such in the matrix and mapped to their tier-appropriate jobs.
- [ ] The gate script distinguishes and reports the failure modes: unmapped machine criterion, criterion absent from matrix, duplicate matrix entry, mapped-to-nonexistent-test, and mapped-to-failing-test.
- [ ] The single required CI gate job depends on the milestone demos (T28/T38/T49/T63), the scale benchmark (T69), and the platform-matrix jobs (T70), and is red if any constituent is red.
- [ ] All Test-plan scenarios 1–14 are implemented as tests and pass.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Authoring or executing new pipeline behaviour.** This ticket asserts existing behaviour only; it adds no scheduling, admission, artifact, or resume feature. Any such need belongs to the component tickets it depends on.
- **Building the constituent tests.** The demos (T28/T38/T49/T63), the fakes harness (T62), the structure fixture (T61), the scale benchmark (T69), and the platform matrix (T70) are dependencies, not deliverables here; this ticket wires and gates them, it does not author them.
- **The T7 matrix scaffold itself.** T7 owns the matrix format and the base checker; T65 only extends completeness enforcement and the human-checklist binding.
- **Cross-tool-version or cross-toolchain resume promises** beyond the structural-fingerprint cross-toolchain *stability* check — v1 refuses cross-tool-version resume (C27) and this gate does not attempt to relax that.
- **Multi-process / distributed coordination.** The gate never coordinates across runs or hosts; asserting the single-run-per-container model is the point, and building anything that schedules or reconciles across processes is the permanent scope boundary (no scheduler, no distributed execution, no metadata store, no web interface, no DSL, no backfill orchestrator; the graph shape never changes at runtime).
- **Timing the thirty-minute walkthrough in CI.** It is a human-classed release-checklist audit, not a CI timer, and must stay that way.
