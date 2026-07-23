# 052 · T41 — C21: fingerprints

> **Milestone:** M3 · **Size:** M · **Type:** feature · **Components:** C21
> **Branch:** `feat/t41-fingerprints` · **Depends on:** T14, T40, T0.7 · **Blocks:** T49, T58

## Why / context
C21 (`### C21 · Graph fingerprint`) gives a pipeline a stable identity for its *shape*, so a later binary can tell whether a prior run's outputs still mean anything to it. This ticket computes the two hashes the spec mandates — the **structural fingerprint** (node set, edge set with carried type names and edge kinds, trigger rules) and the **policy hash** (retries, timeouts, costs, classes) — over the assembled flow (T14) and surfaces them in the graph artifact (T40) and, later, every run artifact. It implements the composition rule and stable-name trait decided in T0.7 (`**T0.7 — ADR: stable-name trait and fingerprint composition**`), and it is the gate for resume (C27, T58), which keys on the structural fingerprint alone. Getting canonical ordering, algorithm versioning, and cross-toolchain stability right here is what makes T49's "explain a run from artifacts" demo and T58's resume decision trustworthy.

## Objective
Compute and expose the C21 fingerprints deterministically from the assembled, precomputed flow, per the T0.7 composition rule, so that identical source produces identical hashes across machines and toolchains and any structural or policy change is detectable.

Concrete pieces of work:
- Derive a **canonical, order-independent serialization** of the fingerprint inputs from the assembled flow: nodes keyed by their author-declared stable name, edges keyed by their endpoint stable names plus edge kind and (for data edges) the carried type's stable declared name, and trigger rules — all sorted into a total order that does not depend on registration order or map iteration order.
- Compute the **structural fingerprint** over the node set (stable names), the edge set (carried type names and edge kinds), and trigger rules only.
- Compute the **policy hash** over the remaining effective policy values — retries, timeouts, declared costs, execution classes, and the other C5 policy fields — using each node's *effective* policy (defaults materialized) so that a node with no stated policy hashes identically to one with every default written out.
- **Exclude** from both hashes: group labels (C6), `std::any::type_name` output, timestamps, hostnames, compiler/tool versions, git commit, lockfile hash, generation time, and every other environmental input. Only author-declared inputs feed the hash.
- Stamp both hashes with an **algorithm version** identifier, carried alongside the hashes wherever they appear, so a version mismatch can later be read by resume as "cannot compare" rather than "topology differs."
- Wire both hashes plus the algorithm version into the **graph artifact header** (T40) via the fingerprint slot precomputed in assembly (T14, per T0.7), and expose the computation for reuse by the run-artifact header (C22) and resume (C27) without those consumers reaching into internals.
- Add the two-toolchain **cross-toolchain stability check** to CI.

## Test plan (write these first — TDD)
Each scenario is independently checkable against the observable hash outputs and the graph artifact.

- **Defaulted policy hashes identically to written-out defaults.** Setup: build two flows that are structurally identical, where flow A leaves every policy field unstated and flow B writes out every C5 default explicitly (no retries, no timeout, zero declared cost, `all-succeeded` trigger, declared execution class, no group, release-on-consume, not durable). Action: compute the structural fingerprint and policy hash of each. Expected: both structural fingerprints are equal AND both policy hashes are equal — the defaulted flow is indistinguishable from the fully-written one under both hashes.

- **Canonical ordering is registration-order independent.** Setup: assemble the same graph twice, registering its nodes and edges in two different orders (e.g. reversed). Action: compute both hashes for each assembly. Expected: structural fingerprint and policy hash are byte-for-byte identical across the two registration orders.

- **Determinism across map iteration.** Setup: assemble one graph and compute both hashes repeatedly within a single process (enough repetitions to shake out any hash-map iteration nondeterminism). Action: collect the hash values. Expected: every computation yields the identical structural fingerprint and identical policy hash.

- **Group rename/removal changes neither hash.** Setup: two otherwise-identical flows differing only in group labels — one renames a group, one removes all groups. Action: compute both hashes for each against the baseline. Expected: structural fingerprint and policy hash are unchanged in every case (C6). (The rename is expected to surface later in the C28 structure diff, not here.)

- **Structural change matrix — each mutation changes the structural fingerprint.** Setup: a baseline flow and a family of single-mutation variants: add a node, remove a node, rename a node's stable name, rewire an edge to a different endpoint, change an edge from data to ordering-only (or vice versa), change a data edge's carried type stable name, and change a node's trigger rule. Action: compute the structural fingerprint of the baseline and each variant. Expected: every variant's structural fingerprint differs from the baseline's.

- **Policy-only change matrix — changes the policy hash but not the structural fingerprint.** Setup: the baseline flow and single-mutation variants that change only a policy value (retries, timeout, declared cost, execution class, and any other C5 policy field). Action: compute both hashes for baseline and each variant. Expected: each variant's policy hash differs from baseline AND its structural fingerprint equals baseline.

- **No-change control.** Setup: the baseline flow and a byte-different but semantically identical rebuild (e.g. reordered registration plus explicit defaults) that no test above already covers as a pair. Action: compute both hashes. Expected: both hashes match — no false difference is introduced by cosmetic authoring differences.

- **Both hashes and the algorithm version appear in the graph artifact.** Setup: assemble a non-trivial flow and emit its graph artifact (T40). Action: inspect the artifact header. Expected: the structural fingerprint, the policy hash, and the algorithm-version identifier are all present and equal to the values computed directly from the flow.

- **Algorithm version is stable and carried.** Setup: emit the graph artifact for a flow. Action: read the algorithm-version field. Expected: it holds the current declared algorithm version; the field's presence and format are asserted so a future intentional algorithm change is caught by a failing test rather than shipping silently.

- **Environmental inputs are excluded.** Setup: emit the graph artifact for one flow twice under differing environmental conditions (different generation time, and where feasible a differing hostname/build-provenance field). Action: compare the two fingerprint/policy-hash pairs. Expected: both pairs are identical despite the environmental differences — confirming timestamps, hostnames, provenance, and generation time do not feed either hash.

- **Cross-toolchain stability (CI, two toolchains).** Setup: a small fixture pipeline whose graph artifact is emitted by builds produced on two different toolchains in CI. Action: compare the structural fingerprint and policy hash from the two builds. Expected: both hashes are byte-identical across toolchains; a divergence fails the CI job.

## Definition of done
- [ ] The structural fingerprint covers exactly the node set (stable names), the edge set (with carried type stable names and edge kinds), and trigger rules — and nothing else.
- [ ] The policy hash covers the remaining effective policy values (retries, timeouts, costs, classes, and other C5 policy fields) and nothing structural.
- [ ] Two builds from the same source, including on different toolchains, produce the same structural fingerprint and the same policy hash (verified by the two-toolchain CI check).
- [ ] Any structural change (node add/remove/rename, rewire, trigger-rule change, edge-kind change, carried-type change) changes the structural fingerprint.
- [ ] Any policy-only change changes only the policy hash and leaves the structural fingerprint unchanged.
- [ ] A group rename or removal changes neither hash (C6).
- [ ] A node with no stated policy hashes identically to one with every C5 default written out, under the policy hash (C5 acceptance criterion).
- [ ] Both hashes and the algorithm-version identifier appear in the graph artifact header (and the computation is exposed for reuse by the run artifact per C22 and by resume per C27, without those consumers reaching into internals).
- [ ] The fingerprint algorithm carries a version identifier, so a future mismatch can be read as "cannot compare" rather than a false topology difference.
- [ ] Both hashes exclude timestamps, hostnames, compiler/tool versions, git commit, lockfile hash, generation time, `type_name`, and all other environmental inputs — every hashed input is author-declared.
- [ ] Canonical serialization is total and independent of registration order and of map iteration order (proven by the ordering and determinism tests).
- [ ] The internal-logic limitation (changing a task's internal logic without changing its interface does not change the fingerprint) is documented at the point of use — surfaced for the resume verb (C27) and the structure-assertion API (C28), not buried in this component; this ticket adds or links that doc note at the fingerprint's public surface.
- [ ] The change/no-change matrix, the defaulted-values-hash-identical test, the group-exclusion tests, the determinism test, and the artifact-header presence test are all implemented and passing.
- [ ] The two-toolchain cross-toolchain stability check runs in CI and fails on any divergence.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Resume itself (C27, T58)** and the "cannot compare" / "topology differs" reporting — this ticket only produces the versioned hashes resume will key on; it does not implement gating or divergence messaging beyond the documented limitation note.
- **The structure diff / snapshot testing (C28, T61)** — group renames surfacing in a diff belongs there, not here.
- **Graph-artifact emission mechanics and its header/provenance fields (C20, T40)** — this ticket populates the fingerprint slot, it does not re-implement artifact serialization or byte-identity guarantees.
- **Run artifact assembly (C22)** — carrying the hashes into run artifacts is a downstream consumer; here we only expose the computation.
- **Per-node internal-change detection.** The fingerprint deliberately does not detect internal-logic changes behind an unchanged interface; a hand-maintained version marker on the task is the honest answer. Do not add an automatic content hash of task bodies — that would silently under-detect and cross the boundary into pretending dagr is a metadata/change store, which it is not.
- Any temptation to fold environmental or provenance data into the hash "for extra safety" — this breaks the cross-toolchain guarantee and is explicitly forbidden.
