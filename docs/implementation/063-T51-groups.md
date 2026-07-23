# 063 · T51 — C6: groups

> **Milestone:** M4 · **Size:** S · **Type:** feature · **Components:** C6
> **Branch:** `feat/t51-groups` · **Depends on:** T13, T46 · **Blocks:** T63

## Why / context
dagr nodes need a human-facing organizing label so that artifacts group readably and rendered diagrams cluster related work, without that label ever leaking into execution or identity. This ticket implements C6 (arch.md `### C6 · Group`, lines 166–175) on top of the node-identity builder from T13 (C7) and the diagram renderer from T46 (C24). The governing rule is that a group is *presentation metadata only*: it is excluded from node identity and from both graph-fingerprint hashes (C21, arch.md line 456), so a rename or removal changes no behavior and no fingerprint — yet it stays review-visible in the structure diff (C28, arch.md line 595) and clusters in rendered diagrams (C24, arch.md lines 513, 521). Resisting the temptation to give groups any execution semantics is the entire point.

## Objective
Attach an optional, presentation-only group label to nodes, thread it into the graph artifact, cluster by it in the renderer, surface it in the structure diff, and prove it is invisible to identity and the fingerprint.

Concrete pieces of work:
- Extend the C7 registration/builder surface (from T13) so a node may carry an optional group label; labels are flat (groups do **not** nest) and default to none.
- Record the group label on the node in the graph artifact so downstream tooling (renderers, structure tests) can read it, while keeping it out of every field that feeds node identity or either fingerprint hash.
- Have the C24 renderer (T46) emit group clusters in both DOT and Mermaid output, with ungrouped nodes rendered outside any cluster.
- Ensure group labels participate in the C28 semantic structure comparison so a rename or regroup is review-visible, without touching the fingerprint.
- Confirm node-name uniqueness is enforced across the whole pipeline irrespective of grouping (no per-group name namespacing).

## Test plan (write these first — TDD)
- **Group excluded from the fingerprint.** Setup: build two otherwise-identical pipelines, one with every node ungrouped and one where the same nodes carry group labels. Action: compute the structural fingerprint and policy hash for each. Expected: both hashes are byte-for-byte equal across the two pipelines.
- **Rename changes no fingerprint.** Setup: a pipeline with grouped nodes; capture its structural fingerprint and policy hash. Action: rename every group label and reassemble. Expected: both hashes are unchanged from the captured values.
- **Removal changes no fingerprint and no behavior.** Setup: a pipeline with grouped nodes and a known execution order/consumer counts. Action: remove all group labels (leave nodes ungrouped) and reassemble. Expected: both fingerprint hashes are unchanged, and the precomputed execution order and consumer/dependency counts are identical to the grouped version.
- **Group is not part of node identity.** Setup: register a node with a given name inside a group. Action: register a second node with the *same* name in a *different* group in the same builder. Expected: assembly reports a duplicate-node-name error (names are unique across the whole pipeline regardless of grouping), naming both declarations.
- **Reorder-stability holds with groups.** Setup: two builders that register the same grouped nodes in different declaration orders. Action: assemble both and compare node identities and fingerprints. Expected: identities and both hashes are identical — grouping does not reintroduce order sensitivity.
- **Group appears in the graph artifact.** Setup: a pipeline with two groups and some ungrouped nodes. Action: emit the graph artifact. Expected: each node's record carries its group label (or a documented "none" marker for ungrouped nodes), and the artifact round-trips through serialization stably.
- **Renderer clusters by group — DOT.** Setup: the two-group-plus-ungrouped graph artifact. Action: render to Graphviz DOT and feed the output to the `dot` reference tool. Expected: `dot` parses the output, each group renders as a cluster containing exactly its nodes, and ungrouped nodes sit outside every cluster; a golden-file comparison matches the checked-in DOT fixture.
- **Renderer clusters by group — Mermaid.** Setup: the same graph artifact. Action: render to Mermaid and feed it to Mermaid's parser in CI. Expected: the parser accepts the output, groups render as clusters, and a golden-file comparison matches the checked-in Mermaid fixture.
- **Group rename is review-visible in the structure test.** Setup: a checked-in structure fixture for a grouped pipeline. Action: rename one group in the pipeline and run the C28 structure assertion against the unchanged fixture. Expected: the assertion fails and its structural diff names the group change, even though the fingerprint is unchanged.
- **Rebuild/toolchain bump does not fail the structure test on grouping.** Setup: the same grouped pipeline and its blessed fixture. Action: reassemble under an unchanged source (simulating a rebuild) and run the structure assertion. Expected: the assertion passes — grouping introduces no volatile, environment-derived structure-test failures.

## Definition of done
- [ ] The C7 registration/builder surface (T13) accepts an optional, flat group label per node; nesting is not expressible (groups do not nest).
- [ ] Node names remain unique across the whole pipeline regardless of grouping; a duplicate name in different groups is an assembly error naming both declarations.
- [ ] The group label is recorded on the node in the graph artifact and is readable by the renderer and the structure test.
- [ ] The group label is excluded from node identity and from both the structural fingerprint and the policy hash (C21); grouping never changes either hash.
- [ ] Removing or renaming every group changes no execution behavior (execution order, consumer/dependency counts, terminal outcomes) and no fingerprint.
- [ ] The C24 renderer (T46) visually clusters nodes by group in both DOT and Mermaid; ungrouped nodes render outside any cluster.
- [ ] Both rendered formats are accepted by their reference tools in CI (`dot` parses; Mermaid's parser accepts) and match checked-in golden files for a grouped fixture.
- [ ] A group rename or regroup fails the C28 structure assertion (review-visible via the structural diff) and does not fail on a rebuild or toolchain bump.
- [ ] Groups carry no execution semantics — no group-level concurrency limit and no group-level failure handling exist anywhere in the surface.
- [ ] Group behavior is documented at the point of use (builder registration and the renderer), and the machine-classed C6 criteria are registered in the criteria matrix.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Nested or hierarchical groups, and any group-tree structure — groups are strictly flat (arch.md line 170).
- Any execution semantics attached to a group: group-level concurrency limits, group-level admission pools, group-level retry or failure handling, or group-scoped resources. These are the named temptation and must not appear.
- Changing what the structural fingerprint or policy hash covers (owned by C21/T0.7); this ticket only confirms exclusion, it does not redefine the hashes.
- Run-overlay coloring and duration annotation on rendered diagrams (C24 run overlay, T47) — this ticket only clusters by group.
- Redesigning the renderer or the structure-assertion harness themselves (T46, C28); this ticket wires the group label through existing surfaces.
- Any use of the group label as a lookup or wiring key — a handle remains the only way to refer to another task's output (C1). Groups are labels, not addresses.
