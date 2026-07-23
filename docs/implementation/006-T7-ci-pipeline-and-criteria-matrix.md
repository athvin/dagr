# 006 · T7 — CI pipeline and acceptance-criteria coverage matrix

> **Milestone:** M0 · **Size:** M · **Type:** setup · **Components:** system-level (Stability, System-level acceptance, Documentation)
> **Branch:** `chore/t7-ci-pipeline-and-criteria-matrix` · **Depends on:** T1, T0.10, T0.0b · **Blocks:** T8, T48, T65, T70

## Why / context
Every subsequent implementation ticket lands through this gate, so it must exist before real code does. This ticket stands up the GitHub Actions pipeline (fmt, clippy, tests, rustdoc lint, supply-chain checks) on the compiling workspace skeleton from T1, and builds the checked-in acceptance-criteria coverage matrix seeded by T0.10's machine/human/disclaimer classification. It is governed by arch.md's **Stability**, **System-level acceptance** (especially criterion 8: every machine-classed criterion is covered by a test, and that coverage is itself verified in CI from a checked-in matrix), **Documentation**, and **Platform support** sections. The matrix scaffold and the script that fails on any unmapped machine criterion are the load-bearing deliverables; later tickets (T8's compile-fail harness, T48's artifact-validation CI, T70's platform matrix, T65's acceptance gate) extend this foundation rather than rebuild it.

## Objective
Stand up the continuous-integration gate and the machine-checkable coverage matrix that enforces system criterion 8.

- Author a GitHub Actions workflow that runs on every pull request and on pushes to `main`: `cargo fmt --check`, `cargo clippy` with warnings denied, `cargo test`, the rustdoc-on-public-items lint, and the supply-chain checks (`cargo audit`, `cargo deny` covering advisories, licenses, and sources).
- Check in a coverage matrix file that lists every acceptance criterion in arch.md — the eight system-level criteria plus the acceptance criteria of components C1–C28 — each appearing exactly once, tagged `machine`, `human`, or `disclaimer` per T0.10's classification, and each machine criterion carrying either a test identifier (its mapped test) or an explicit `unmapped` placeholder until its test ships.
- Provide a matrix-verification script, wired into CI, that parses the matrix and fails the build if: any criterion in arch.md is absent from the matrix; any criterion appears more than once; any `machine`-classed criterion has no mapped test id (i.e. is still `unmapped`); or any mapped test id names a test that does not exist in the suite.
- Encode the pinned-workspace-toolchain policy (a `rust-toolchain` pin) and document that UI / compile-fail (trybuild-style) tests run only under the pinned toolchain, so T8 can rely on stable snapshot output.
- Wire embedded build provenance verification (tool version, git commit, lockfile hash per the Stability supply-chain commitment) into the pipeline at whatever depth T1/T0.0b already expose it, or record it as a tracked `unmapped` matrix row if the emitting component is not yet built.
- Name platform-conditional criteria (limit detection, signal handling, flush behavior) as such in the matrix so T70 can attach the Linux-tier-1 and macOS-core jobs later.

## Test plan (write these first — TDD)
These validate the *tooling* this ticket delivers — the matrix-verification script and the CI wiring — not yet-unwritten product tests. Each is independently checkable.

- **Complete matrix passes.** Setup: the checked-in matrix listing every arch.md criterion exactly once, with every `machine` row mapped to an existing (possibly trivial placeholder) test id. Action: run the matrix-verification script against the repo. Expected: the script exits zero and prints a summary line reporting counts of machine, human, and disclaimer criteria.
- **Missing criterion fails.** Setup: a matrix identical to the good one but with one criterion row deleted (for example a C14 attempt-runner criterion). Action: run the verification script. Expected: nonzero exit; the error names the missing criterion id and states that a criterion present in arch.md is absent from the matrix.
- **Duplicate criterion fails.** Setup: a matrix where one criterion id appears on two rows. Action: run the script. Expected: nonzero exit; the error names the duplicated criterion id and that it must appear exactly once.
- **Unmapped machine criterion fails.** Setup: a matrix where a `machine`-classed criterion carries the `unmapped` placeholder instead of a test id. Action: run the script. Expected: nonzero exit; the error names the criterion and states that a machine criterion must map to a test.
- **Dangling test reference fails.** Setup: a matrix where a `machine` criterion maps to a test id that does not exist in the suite. Action: run the script. Expected: nonzero exit; the error names the criterion and the missing test id.
- **Human/disclaimer rows need no test.** Setup: a matrix where the thirty-minute-walkthrough and diagram-readability criteria are `human` and the external-effects clause of criterion 4 is `disclaimer`, none carrying a test id. Action: run the script. Expected: exits zero; these rows are accepted without a mapped test.
- **fmt gate catches drift.** Setup: a workspace file with deliberately misformatted whitespace on the branch. Action: run the fmt step as CI runs it. Expected: nonzero exit; diff is reported. Reverting the formatting makes the step pass.
- **clippy denies warnings.** Setup: a placeholder lib item that trips a clippy lint (for example an unused import). Action: run the clippy step as CI configures it. Expected: nonzero exit because warnings are denied. Removing the offending item makes it pass.
- **Rustdoc lint catches an undocumented public item.** Setup: add a public item to a crate with no doc comment. Action: run the rustdoc-lint step. Expected: nonzero exit naming the undocumented item. Adding a doc comment makes it pass.
- **Supply-chain checks run and gate.** Setup: the workspace with its committed lockfile and the `cargo deny` configuration. Action: run the `cargo audit` and `cargo deny` steps. Expected: both run to completion and gate the pipeline; a deny-config entry that forbids a license present in the tree causes `cargo deny` to fail, proving the check is live rather than inert.
- **Pinned toolchain is honored.** Setup: the checked-in toolchain pin. Action: query the active toolchain inside the CI job. Expected: the resolved toolchain matches the pinned version, confirming UI/compile-fail tests will run deterministically for T8.
- **Whole pipeline is green on the branch.** Setup: the ticket branch with all steps wired. Action: trigger the workflow on a pull request. Expected: every job (fmt, clippy, tests including the matrix-verification test, rustdoc lint, audit, deny) reports success.

## Definition of done
- [ ] A GitHub Actions workflow triggers on every pull request and on pushes to `main`, running `cargo fmt --check`, `cargo clippy` with warnings denied, and `cargo test`.
- [ ] The workflow runs the rustdoc-on-public-items lint and fails on any undocumented public item (Documentation).
- [ ] `cargo audit` and `cargo deny` (advisories, licenses, sources) run in CI and gate the build (Stability, supply-chain posture).
- [ ] The workspace toolchain is pinned via a checked-in `rust-toolchain` file, and the policy that UI / compile-fail tests run only under the pinned toolchain is documented for T8 to depend on (Stability MSRV/toolchain; Platform support).
- [ ] A coverage matrix is checked into the repo mapping each criterion id to a test id **or** a `human`/`disclaimer` classification, seeded from T0.10 (System-level acceptance, criterion 8).
- [ ] Every acceptance criterion in arch.md — the eight system-level criteria and the criteria of C1–C28 — appears in the matrix exactly once, classed as `machine`, `human`, or `disclaimer`; a criterion absent from the matrix fails CI (criterion 8).
- [ ] A matrix-verification script runs in CI and fails on any unmapped `machine`-classed criterion, any duplicate criterion, any criterion missing from the matrix, and any mapped test id that names a nonexistent test (criterion 8).
- [ ] Human-classed criteria (thirty-minute walkthrough, diagram readability per C24, documentation-at-point-of-use per C21, and the judgment-shaped criteria) are recorded in the matrix as pointing to the version-controlled release checklist, not to a test (criterion 8, human partition).
- [ ] The external-effects clause of criterion 4 is carried in the matrix as an unclassified `disclaimer` row.
- [ ] Platform-conditional criteria (limit detection, signal handling, flush behavior) are explicitly named as such in the matrix for T70 to attach platform jobs (Platform support).
- [ ] Build-provenance verification (tool version, git commit, lockfile hash) is either wired into CI or tracked as an explicit `unmapped` matrix row awaiting its emitting component (Stability, C20).
- [ ] The matrix-verification script is itself covered by the tests described in the Test plan (complete-passes, missing, duplicate, unmapped, dangling-reference, human/disclaimer-exempt).
- [ ] The full pipeline is green on a pull request from the ticket branch.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Writing the actual product/component tests that machine criteria will eventually map to — until they exist their matrix rows carry `unmapped` placeholders; this ticket only builds the gate and the classification scaffold. Filling in mappings is the job of each component ticket.
- The compile-fail / UI (trybuild) harness itself and its snapshot-update flow — that is T8; this ticket only pins the toolchain and documents the policy T8 relies on.
- Artifact-schema validation and the frozen fixture corpus in CI — that is T48; not built here.
- The multi-platform job matrix (Linux tier-1 full suite, macOS core suite) — that is T70; this ticket only names platform-conditional criteria so T70 can attach jobs.
- The final system-acceptance gate (structural- and interpretive-determinism checks, full machine-to-test enforcement across a complete suite) — that is T65; this ticket lays its foundation only.
- The performance benchmark gate (thousand-node no-op overhead) — tracked elsewhere; not wired here.
- Any drift toward a scheduler, distributed runners, a metadata store, a web dashboard, or a DSL for the matrix — the matrix is a plain checked-in data file plus a verification script, and CI orchestration stops at PR gating; nothing here coordinates runs or persists state beyond the repo.
