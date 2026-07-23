# 059 · T48 — Artifact validation and compatibility CI

> **Milestone:** M3 · **Size:** M · **Type:** feature · **Components:** C22
> **Branch:** `feat/t48-artifact-validation-compatibility-ci` · **Depends on:** T7, T40, T42, T0.10 · **Blocks:** T49, T69

## Why / context
The spec pitch is compile-time confidence, and "breaking recorded artifacts is worse" than breaking the authoring API (arch.md, Stability). C22 · Run artifact promises that every emitted artifact validates against its published schema and that a checked-in fixture corpus — one artifact per released schema version — is "parsed in CI forever after." This ticket turns that promise into an enforced CI gate: it wires schema validation over every artifact the test suite emits (both graph and run artifacts, via T40 and T42) and freezes the M3 fixture corpus per the stability policy seeded in T0.10, including the ten-thousand-attempt scale artifact named in the Performance envelope. It builds on the CI scaffold and coverage matrix from T7 and unblocks the M3 demo (T49) and the scale benchmark (T69).

## Objective
Stand up the artifact-compatibility layer of CI so that no artifact can be emitted that fails its schema, and no future schema change can silently break parsing of an already-released artifact shape.

Concrete pieces of work:
- Add a reusable CI/test validation step that runs the published-schema validation helper (from T39, re-exported through the artifact crate) against every artifact produced anywhere in the test suite — graph artifacts (C20, via T40) and run artifacts (C22, via T42), across all variants: full-run, `assembly-failed`, `bootstrap-failed`, interrupted, and single-node (`not-requested`) artifacts.
- Establish the frozen fixture corpus directory: one checked-in graph artifact and one checked-in run artifact per released schema version, per the T0.10 fixture-corpus plan. Seed it at the current (M3) schema version, and document the append-only, never-mutate rule for the directory.
- Add a corpus-compatibility test that parses every fixture with current tooling and asserts it round-trips — proving additive-only evolution has not broken any prior shape.
- Generate and freeze the ten-thousand-attempt run artifact as a corpus member and assert current tooling parses it and that its size stays proportional to attempt count.
- Add a schema-drift guard: a CI check that fails if the published schemas change in a way that is not additive-only, or if a new schema version is introduced without a matching new corpus fixture.
- Register the C22 validation/compatibility acceptance criteria in the T7 coverage matrix (criterion id → test id), classified `[machine]`.

## Test plan (write these first — TDD)
Each scenario is independently checkable. Derive the expected outcome from the C22 acceptance criteria and the Stability/Performance-envelope clauses.

- **Every emitted graph artifact validates.** Setup: build a small pipeline and emit its graph artifact (T40). Action: run the validation helper against the emitted bytes using the published graph-artifact schema for the artifact's declared schema version. Expected: validation passes with zero errors; a deliberately corrupted copy (a required header field removed) is reported as invalid, naming the offending field.

- **Every emitted run artifact validates, all variants.** Setup: produce run artifacts for each variant — a full run that passed assembly, an `assembly-failed` run, a `bootstrap-failed` run, an interrupted (folded crashed-stream) run, and a single-node replay artifact with `not-requested` markings. Action: validate each against the published run-artifact schema for its version. Expected: every variant validates; each variant is recognizably its own shape (assembly-failed carries no fingerprint and zero attempts; single-node carries `not-requested` markings).

- **The whole test suite emits only valid artifacts.** Setup: enable the validation step as a wrapper/hook invoked by every test path that writes an artifact. Action: run the full test suite in CI. Expected: any test that emits an artifact failing its schema fails that test; a planted test that intentionally emits a malformed artifact fails, proving the gate is live and not a no-op.

- **Fingerprint cross-reference holds.** Setup: from one build, emit a graph artifact and a run artifact for the same pipeline. Action: validate both, then compare the structural fingerprint recorded in the run artifact against the graph artifact from the same build. Expected: the run artifact names a structural fingerprint matching the graph artifact from the same build (C22 acceptance criterion), and both validate.

- **Node coverage in the run artifact.** Setup: a graph with a node that never ran (propagated `upstream-skipped`/`upstream-failed`). Action: fold the stream into the run artifact and validate. Expected: every node present in the graph artifact appears at least once in the run artifact, including never-ran nodes with their propagated terminal states; the artifact validates.

- **Phase durations sum exactly.** Setup: a run artifact with a multi-phase attempt. Action: for each attempt, sum the named phase durations and compare to the attempt total. Expected: phases sum exactly to the attempt total (both derived from monotonic offsets); a fixture that violates this would fail the test.

- **Environment allowlist negative check.** Setup: run with a sentinel environment variable set that is not on the declared allowlist. Action: emit the run artifact and validate, then scan the full artifact bytes for the sentinel. Expected: the sentinel appears nowhere; no environment value outside the declared allowlist appears in any artifact.

- **Frozen corpus parses forever after.** Setup: the checked-in fixture corpus with one graph artifact and one run artifact per released schema version (seeded at the M3 version). Action: parse every corpus member with current tooling and assert it round-trips (readers ignore unknown fields and default missing ones). Expected: every fixture from every prior schema version remains parseable by current tooling; the test enumerates the corpus directory so a newly added fixture is automatically covered.

- **Additive-only evolution is enforced.** Setup: the current published schemas plus their committed baseline. Action: run the schema-drift guard. Expected: an additive change (a new optional field) passes; a simulated breaking change (removing or renaming a required field, or making an optional field required) is rejected by the guard with a message pointing at the offending field.

- **A new schema version requires a new corpus fixture.** Setup: simulate bumping the run-artifact schema version without adding a corresponding corpus fixture. Action: run the corpus-completeness check. Expected: the check fails, stating that version N has no fixture; adding the fixture makes it pass.

- **Ten-thousand-attempt scale artifact stays honest.** Setup: generate a run artifact with ten thousand attempts and freeze it as a corpus member. Action: parse it with current tooling and validate it against the schema; measure its serialized size against attempt count. Expected: it validates and parses; artifact size is proportional to attempt count (within the documented bound), keeping the tooling honest at scale.

- **Coverage-matrix wiring.** Setup: the T7 coverage matrix and its CI script. Action: run the matrix-completeness check. Expected: every C22 validation/compatibility criterion maps to a test id and is classified `[machine]`; no C22 machine criterion is left unmapped, so the CI script does not fail on an unmapped criterion.

## Definition of done
- [ ] Every emitted graph artifact and run artifact validates against its published schema for its declared schema version (C22).
- [ ] All run-artifact variants are covered by validation: full-run, `assembly-failed`, `bootstrap-failed`, interrupted, and single-node `not-requested`.
- [ ] Every node present in the graph artifact appears at least once in the run artifact, including never-ran nodes with propagated terminal states — asserted on a fixture and validated.
- [ ] A crashed run's folded (interrupted) artifact validates and contains everything up to the crash.
- [ ] Phase durations for every attempt sum exactly to that attempt's total, verified on corpus fixtures.
- [ ] The run artifact names a structural fingerprint matching a graph artifact from the same build, and both validate.
- [ ] No environment value outside the declared allowlist appears in any artifact, verified with a planted sentinel.
- [ ] The artifact validation step is invoked by every test path that emits an artifact; a planted malformed-artifact test fails, proving the gate is not a no-op.
- [ ] A checked-in fixture corpus exists with one graph artifact and one run artifact per released schema version, seeded at the current (M3) schema version; the directory's append-only/never-mutate rule is documented.
- [ ] A corpus-compatibility test parses every fixture-corpus artifact from every prior (and current) schema version with current tooling and asserts round-trip; the test enumerates the directory so new fixtures are auto-covered.
- [ ] The ten-thousand-attempt run artifact is frozen as a corpus member, validates, parses, and its size is asserted proportional to attempt count.
- [ ] A schema-drift guard rejects any non-additive change to the published schemas (removed/renamed required field, optional-to-required), and rejects introducing a new schema version without a matching new corpus fixture.
- [ ] Every C22 validation/compatibility acceptance criterion is registered in the T7 coverage matrix (criterion id → test id), classified `[machine]`, with no unmapped machine criterion.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Authoring or publishing the schemas themselves and the validation helper — owned by T39; this ticket consumes them.
- Emitting the artifacts — graph emission is T40 (C20), run-artifact folding is T42 (C22); this ticket validates what they emit.
- The run summary / critical-path computation (T43) and node metrics content (T44) beyond confirming they validate against the schema.
- The scale *benchmark* (the thousand-node no-op under the 1 ms/node overhead budget) — that is T69; this ticket only freezes and parses the ten-thousand-*attempt* fixture, not the runtime performance gate.
- Renderer output validation (C24, DOT/Mermaid reference-tool acceptance) — T46/T47.
- Defining new stability policy: the MSRV/semver/schema-evolution rules and the fixture-corpus plan are set in T0.10; this ticket implements the corpus and the enforcement, it does not decide the policy.
- Scope-boundary temptations to resist: this is compatibility CI, not a metadata store or artifact registry, not a schema-migration/upgrade tool that rewrites old artifacts, and not a runtime schema-negotiation mechanism. Compatibility is enforced by additive-only evolution plus a frozen corpus, never by mutating recorded artifacts or by changing graph shape at runtime.
