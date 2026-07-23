# 074 · T61 — C28: structure snapshot testing

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C28
> **Branch:** `feat/t61-structure-snapshot-testing` · **Depends on:** T40, T0.7 · **Blocks:** T63

## Why / context
This ticket delivers the middle level of the C28 testing surface (arch.md · "C28 · Testing surface"): a shipped-with-the-library way to assert a pipeline's *structure* against a checked-in fixture, so unintended rewiring fails review rather than production. It builds on the graph artifact emitted by T40 (C20) as the sole input and on the field lists and canonicalization decided in the T0.7 ADR (C20/C21), reusing the fingerprint's structural/policy field split for the semantic comparison. The comparison is *semantic*, not textual: it must ignore volatile header fields, survive rebuilds and toolchain bumps, and yet keep a group rename review-visible even though a group label touches neither fingerprint hash (arch.md · "C6 · Group", "C21 · Graph fingerprint"). It gates T63 (the M4 kill/resume/review demo), which relies on this assertion to prove structure is reviewable.

## Objective
Provide a library-shipped structure-assertion API and its blessed fixture-update workflow, so any pipeline can pin its shape without writing its own harness.

- A semantic comparison over two graph artifacts (or an artifact vs. a checked-in fixture) covering: the node set (by stable name), the edge set (with carried type names and edge kinds), and effective policies per node — while excluding volatile header fields (generation time, build provenance, and anything else environmental named in the C20 header).
- Group labels are included in the comparison surface so a group rename is reported as a review-visible change, even though it never touches either fingerprint hash.
- A structural diff as the failure output: a human-readable, node-and-edge-oriented report naming exactly what was added, removed, renamed, rewired, regrouped, or repolicied.
- A canonical, stably-ordered serialization of the fixture so the checked-in file is deterministic across builds and machines.
- A single documented command (an update/bless flag) that deliberately regenerates the canonical fixture from the current pipeline for review.
- The assertion helper and the update flow are usable by any pipeline against its own graph artifact, requiring no per-pipeline harness code.

## Test plan (write these first — TDD)
Each scenario is independently checkable and is derived from the C28, C21, and C6 acceptance criteria. All fixtures below are produced through the blessed update flow, never hand-edited.

- **Baseline match passes.** Setup: a fixed pipeline and its blessed fixture checked in. Action: assemble the pipeline, emit its graph artifact, and run the structure assertion against the fixture. Expected: the assertion passes with no diff output.

- **Rebuild does not fail.** Setup: the same pipeline and blessed fixture. Action: rebuild the binary (fresh `cargo build`, changing only the generation-time and any build-provenance header fields) and re-run the assertion. Expected: passes — volatile header fields are excluded, so a rebuild alone produces no structural difference.

- **Toolchain bump does not fail.** Setup: the same pipeline and blessed fixture. Action: build under a second, different toolchain (the cross-toolchain job) and run the assertion. Expected: passes — the comparison is stable across toolchains, mirroring the C21 cross-toolchain guarantee.

- **Adding a node fails with a diff.** Setup: blessed fixture for the original pipeline. Action: add one node (with its edge) and run the assertion without re-blessing. Expected: fails; the structural diff names the added node and its new edge and no unrelated nodes.

- **Removing a node fails with a diff.** Setup: blessed fixture. Action: delete one node and its incident edges, then assert. Expected: fails; the diff names the removed node and the removed edges.

- **Renaming a node fails with a diff.** Setup: blessed fixture. Action: change one node's stable name, keeping wiring otherwise identical, then assert. Expected: fails; the diff shows the old name as removed and the new name as added (or as a rename) so the reviewer sees the identity change.

- **Rewiring fails with a diff.** Setup: blessed fixture. Action: redirect one edge to a different producer/consumer without changing the node set, then assert. Expected: fails; the diff names the removed edge and the added edge, including carried type names and edge kind.

- **Carried-type change fails with a diff.** Setup: blessed fixture. Action: change the payload type carried on one edge (node and edge endpoints unchanged), then assert. Expected: fails; the diff reports the edge's carried-type change.

- **Effective-policy change fails with a diff.** Setup: blessed fixture. Action: change one node's effective policy (for example a retry count or timeout) with topology unchanged, then assert. Expected: fails; the diff names the node and the changed policy field.

- **Group rename fails with a review-visible diff.** Setup: blessed fixture for a pipeline whose nodes carry group labels. Action: rename a group (no other change) and assert. Expected: fails; the diff reports the regrouping as a review-visible change — even though a companion fingerprint check (from T41/C21) shows both structural and policy hashes unchanged. This is the C6/C28 distinction proven in one place.

- **Regrouping a node fails with a diff.** Setup: blessed fixture. Action: move one node from one existing group to another, then assert. Expected: fails; the diff reports the node's group change; the fingerprint is again unchanged.

- **Defaulted vs. written-out policy does not differ.** Setup: a pipeline authored with a policy value left to its default, blessed into a fixture. Action: rewrite the source to state that same value explicitly and assert. Expected: passes — defaulted values compare identically to written-out defaults (consistent with C5/C21), so no spurious diff.

- **Canonical serialization is stable and order-independent.** Setup: a pipeline whose nodes/edges are registered in one order. Action: bless a fixture, then re-author the pipeline registering the same nodes/edges in a different order and bless again. Expected: the two blessed fixtures are byte-identical — the serialization is canonical and stably ordered, not dependent on registration order.

- **Bless flow regenerates the fixture deliberately.** Setup: a pipeline whose structure has legitimately changed and a now-stale fixture. Action: run the single documented update command. Expected: the fixture file is rewritten to the new canonical structure; a subsequent assertion passes; running the update command again is a no-op (byte-identical output), confirming idempotence.

- **No pipeline writes its own harness.** Setup: a fresh example pipeline that only calls the shipped assertion helper and points it at its own graph artifact and fixture path. Action: run its structure test. Expected: it passes (or fails with a diff) with no bespoke comparison or serialization code in the example — the entire mechanism comes from the library.

## Definition of done
- [ ] The structure assertion is a **semantic** comparison over node set (stable names), edge set (carried type names and edge kinds), and effective policies; volatile header fields (generation time, build provenance, environmental values) are excluded from the comparison.
- [ ] A structure test **fails** when a node is added, removed, renamed, rewired, regrouped, or has its effective policy changed, and **does not fail** on a rebuild or a toolchain bump (C28 criterion).
- [ ] A **group rename is review-visible** in the structure diff (C6) even though it changes neither the structural fingerprint nor the policy hash (C21).
- [ ] A carried-type change on an edge is detected and reported (C28 rewire/type criterion, aligned with C21 carried-type coverage).
- [ ] The failure output is a **structural diff** — node-and-edge oriented, naming exactly what changed — not a raw text or byte diff.
- [ ] The fixture serialization is **canonical and stably ordered**, independent of node/edge registration order, and byte-identical across builds and toolchains.
- [ ] Defaulted policy values compare identically to written-out defaults, producing no spurious diff (consistent with C5/C21).
- [ ] The fixture-update flow is a **single documented command** (a deliberate bless/update flag) that rewrites the canonical fixture for review and is idempotent.
- [ ] The assertion helper and update flow ship **in the library**, consume a graph artifact (T40/C20) as input, and require **no per-pipeline harness code**.
- [ ] The internal-logic limitation of structure identity is documented **at the point of use** on this structure-assertion API (per C21: internal logic changes do not surface here, and a hand-maintained version marker is the honest answer).
- [ ] Rustdoc is present on every public item added by this ticket (assertion entry point, diff/report type, update-flow entry point).
- [ ] Deterministic ordering is reused from the T0.7 canonicalization decision rather than re-invented; the structural/policy field split matches the fingerprint's field lists.
- [ ] Tests from the Test plan above exist and pass, including the cross-toolchain no-diff case in the CI matrix.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The single-task testing level (hand-built context and fake resources) and the whole-pipeline-against-fakes level of C28 — those are separate tickets; this ticket delivers only the structure-assertion level.
- The framework's own fault-injection suite (kill-points, disk-full, failing sinks) — a distinct C28 deliverable.
- Emission of the graph artifact itself (T40/C20) and computation of the fingerprint hashes (T41/C21); this ticket **consumes** those, it does not produce them.
- Diagram rendering from the artifact (C24) — legibility for humans is C24's job, not the structure assertion's.
- Any runtime-mutable or dynamic graph comparison: the graph shape never changes at runtime, so there is nothing to diff live — comparison is strictly artifact-to-artifact/fixture. (Scope boundary: dagr is not a scheduler, a metadata store, or a runtime graph editor.)
- Turning the structure fixture into a metadata store or a history/audit database of past shapes; the fixture is one checked-in file per pipeline reviewed in version control, nothing more.
