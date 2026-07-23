# 007 · T8 — Compile-failure test harness

> **Milestone:** M0 · **Size:** S · **Type:** setup · **Components:** C2, C3, C28
> **Branch:** `chore/t8-compile-failure-test-harness` · **Depends on:** T1, T7 · **Blocks:** T12

## Why / context
The whole pitch of dagr is compile-time confidence: wrong-type, wrong-arity, and cyclic wiring must fail to *compile*, and the error message must be legible. Those guarantees are only real if a test asserts them, so this ticket stands up the trybuild/UI compile-failure harness — the machinery, one worked sample, and a blessed snapshot-update flow — before any real wiring surface exists. It is governed by **C2 · Handle** (a checked-in compile-failure test demonstrates that a cycle cannot be expressed), **C3 · Data dependency** (wrong-type binding is a compile error whose message names both the expected and supplied types, verified by UI tests against the pinned toolchain), and **C28 · Testing surface** (the framework tests itself the same way it asks pipelines to: compile-fail and error-message tests are library-internal, pinned to the workspace toolchain, assert only that both type names appear, and are regenerated deliberately on toolchain bumps). It builds directly on T1's compiling workspace skeleton and T7's pinned-toolchain policy and CI gate, and it is the foundation T12 extends with the full wiring compile-fail suite.

## Objective
Deliver a working, CI-wired compile-failure (UI-test) harness with exactly one passing sample case and a documented snapshot-regeneration flow, so that later tickets add cases rather than build machinery.

- Add the trybuild-style UI-test harness as a dev-dependency in the crate that will own the authoring API, pinned so its expected-output snapshots are stable under the T7 workspace toolchain.
- Provide one sample compile-failure case: a tiny standalone source file that intentionally fails to compile in a way that mentions two distinct type names in the diagnostic, plus its checked-in expected-output snapshot.
- Write the harness's assertion so it checks only that **both** relevant type-name substrings appear in the diagnostic — never asserting exact prose, note count, span layout, or wording — so ordinary compiler-message churn does not break the suite.
- Wire the UI-test run into the existing CI job so it executes under the pinned toolchain on every pull request and gates the build.
- Establish and document the single-command snapshot-update (blessing) flow: an explicit, opt-in environment-variable or flag invocation that rewrites the checked-in snapshot, kept out of the default test path so snapshots are never silently overwritten.
- Document the toolchain-bump procedure: when the pinned toolchain in T7 changes, snapshots are regenerated deliberately through the blessing flow and reviewed, and a note in the harness's module/README states this contract.
- Keep the harness's canonical location and naming discoverable (a dedicated UI-test directory and a single test entry point) so T12 drops additional cases in with no wiring changes.

## Test plan (write these first — TDD)
These validate the *harness* and its blessing flow, not yet-unwritten wiring cases (those are T12). Each scenario is independently checkable and runs under the pinned toolchain.

- **Sample case fails to compile as intended.** Setup: the one checked-in UI sample source that is written to be non-compiling and whose failure mentions two distinct type names. Action: run the UI-test entry point under the pinned toolchain with snapshots frozen (blessing disabled). Expected: the test passes, because the sample's actual compiler output matches the checked-in snapshot on the both-type-names assertion.
- **Both type names are required.** Setup: temporarily edit the checked-in snapshot so it references only one of the two type names. Action: run the UI-test entry point. Expected: the test fails, and the failure identifies that an expected type-name substring was not satisfied — proving the assertion genuinely requires both names rather than passing vacuously. Reverting the snapshot restores green.
- **Prose churn does not break the suite.** Setup: a snapshot that contains only the two type-name substrings and omits surrounding note/help prose. Action: run the UI-test entry point against the sample whose real diagnostic includes extra notes and spans. Expected: the test passes, demonstrating the assertion tolerates wording, spans, and note count and keys only on the two type names.
- **A case that unexpectedly compiles is caught.** Setup: replace the sample source with a version that actually compiles cleanly (no error). Action: run the UI-test entry point. Expected: the test fails, reporting that a source expected to fail compilation instead succeeded — so a silently-passing (no-op) compile-fail case cannot masquerade as coverage. Restoring the failing sample returns to green.
- **Blessing is off by default.** Setup: a deliberately stale checked-in snapshot (one wrong type name) with no blessing flag/env set. Action: run the default test command as CI runs it. Expected: nonzero exit; the snapshot is **not** rewritten on disk (the working tree is unchanged for the snapshot file), confirming default runs never overwrite snapshots.
- **Blessing flow regenerates the snapshot.** Setup: the same deliberately stale snapshot. Action: run the documented single-command blessing invocation (the opt-in flag/env). Expected: the command exits successfully, the on-disk snapshot is rewritten to the current diagnostic (now containing both type names), and a subsequent frozen run passes. The regenerated snapshot is a reviewable diff, not a binary blob.
- **Pinned toolchain governs output.** Setup: the checked-in `rust-toolchain` pin from T7. Action: query the active toolchain inside the UI-test CI job, then run the UI test. Expected: the resolved toolchain equals the pinned version and the sample test passes under it — establishing the determinism T12's larger suite relies on.
- **CI wiring gates the build.** Setup: the ticket branch with the UI test wired into the CI job. Action: open a pull request and, in one probe run, introduce a snapshot mismatch. Expected: the pipeline goes red on the UI-test step; fixing the snapshot returns the whole pipeline to green, proving the harness actually gates rather than running inertly.

## Definition of done
- [ ] A trybuild/UI-test harness is added as a pinned dev-dependency in the crate that owns the authoring API, producing stable expected-output under the T7 workspace toolchain (C28).
- [ ] Exactly one passing sample compile-failure case is checked in: a non-compiling source file plus its expected-output snapshot, whose diagnostic mentions two distinct type names (C3, C28).
- [ ] The harness assertion checks only that both relevant type-name substrings appear in the diagnostic and never asserts exact prose, wording, spans, or note count (C3 message-quality clause; C28 "assert only that both type names appear").
- [ ] A source that unexpectedly *compiles* causes the suite to fail, so a no-op compile-fail case cannot pass as coverage (C2 checked-in compile-failure test integrity).
- [ ] The UI test runs under the pinned toolchain in CI on every pull request and gates the build (C28; Stability toolchain policy from T7).
- [ ] The snapshot-update (blessing) flow is a single documented command, opt-in via an explicit flag/env, kept out of the default test path so snapshots are never silently overwritten (C28 "fixture-update flow is a single documented command"; deliberate regeneration).
- [ ] The toolchain-bump contract is documented at point of use: pinned-toolchain changes regenerate snapshots deliberately through the blessing flow and are reviewed (C28; Stability MSRV/toolchain).
- [ ] Snapshot files are canonical, human-readable, reviewable diffs — an unintended change to expected output surfaces in review rather than in production (C28 canonical, review-visible fixtures).
- [ ] The harness's canonical location and single entry point are set up so T12 can add cases with no machinery changes; the harness location and blessing command are named in the crate/module docs (C28 "no pipeline needs to write its own test harness"; Documentation).
- [ ] The relevant coverage-matrix rows from T7 are updated: the C2 cycle-compile-failure criterion and the C3 both-type-names criterion point at this harness's sample test id (or remain `unmapped` with the full assertion deferred to T12, per the matrix contract) rather than staying orphaned.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The actual wiring compile-fail cases — cyclic construction, wrong-arity binding, non-`all-succeeded` trigger on a data-consuming node, the arity-ceiling curated diagnostic — are **T12**; this ticket ships only the harness plus one sample and must not attempt to cover C2/C3/C4 exhaustively.
- The authoring API itself (handles, binding, builder typestate) — those are the M1 builder tickets (T9 onward); the sample case here is a throwaway non-compiling snippet, not a use of the real API.
- The structure-fixture assertion and its update flow (semantic node/edge/policy diff against a checked-in pipeline fixture) — a distinct part of C28 handled with the graph-artifact work, not the compile-fail harness.
- The fault-injection suite (kill-points, disk-full, failing sinks) — also C28 but a separate later ticket; not built here.
- The artifact-schema fixture corpus and its forever-parsed CI check — that is T48.
- Multi-toolchain or multi-platform expected-output matrices — snapshots are pinned to the single workspace toolchain by design; cross-toolchain UI testing is explicitly not attempted, and any temptation toward it is rejected here.
- Any drift across the permanent scope boundary — this harness is a plain checked-in test directory plus snapshots and a blessing command; it introduces no scheduler, distributed runner, metadata store, web surface, DSL, or runtime graph mutation.
