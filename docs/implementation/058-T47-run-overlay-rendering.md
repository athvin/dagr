# 058 · T47 — C24: run-overlay rendering

> **Milestone:** M3 · **Size:** S · **Type:** feature · **Components:** C24
> **Branch:** `feat/t47-run-overlay-rendering` · **Depends on:** T42, T46 · **Blocks:** T49

## Why / context
The base diagram renderer (T46, C24) already turns a graph artifact into DOT and Mermaid with distinct edge styling and group clusters, but it renders structure only — it says nothing about what happened when the pipeline ran. This ticket adds the optional *run overlay*: given a run artifact (produced by C22, delivered by T42), colour each node by its terminal state and annotate it with its duration, so a diagram becomes a legible post-mortem. The governing spec is C24 · Renderers (arch.md, "Renderers" section) and the normative Terminal states table (arch.md, top-of-document taxonomy), which requires that originated skips (`skipped`) stay visually distinct from propagated skips (`upstream-skipped`), and originated failures from propagated ones (`upstream-failed`). Because renderers consume artifacts only, the overlay must work identically on a run captured three months ago, with no access to the binary that produced it.

## Objective
Extend the C24 renderer so that, when a run artifact is supplied alongside the graph artifact, each node in both DOT and Mermaid output is styled by its terminal state and annotated with its duration, using a documented, distinct style per state that keeps originated and propagated skips/failures apart.

Concrete pieces of work:
- Accept an optional run artifact as a second input to the existing DOT and Mermaid render paths; with no run artifact supplied, output is byte-for-byte the structural diagram from T46 (no regression).
- Define one documented style mapping covering every entry in the normative Terminal states table — `succeeded`, `failed`, `timed-out`, `skipped`, `upstream-skipped`, `upstream-failed`, `cancelled`, `abandoned`, `satisfied-from-prior` — with a distinct style per state, and with `skipped` vs `upstream-skipped` and `failed`/`timed-out` vs `upstream-failed` visually separable.
- Also handle the `not-requested` artifact marking that appears on single-node-replay run artifacts (C26): it is not a terminal state, but it can appear in a run artifact, so it needs its own documented, distinct rendering rather than being treated as an error.
- Annotate each node with its duration, derived from the run artifact's timing for that node, in both formats.
- Join graph nodes to run records by node identity; a node present in the graph but absent from the run artifact, and a run record with no matching graph node, are each handled with a defined, documented behaviour rather than a panic.
- Publish the state-to-style mapping table in rustdoc on the overlay entry point so the "documented distinct style" criterion is auditable from the source.

## Test plan (write these first — TDD)
- **Overlay is opt-in / no regression.** Setup: a graph artifact and its T46 golden DOT and Mermaid outputs. Action: render with no run artifact supplied. Expected: output is identical to the T46 structural golden files — the overlay code path changes nothing when there is no run to overlay.
- **Every state gets a distinct documented style (DOT).** Setup: a fixture run artifact in which nine nodes carry the nine distinct terminal states from the normative table. Action: render DOT with the overlay. Expected: each node carries the style documented for its state; the nine styles are mutually distinct; DOT golden file matches.
- **Every state gets a distinct documented style (Mermaid).** Setup: the same nine-state fixture. Action: render Mermaid with the overlay. Expected: each node carries its documented Mermaid style; the nine styles are mutually distinct; Mermaid golden file matches.
- **Originated vs propagated skip are distinguishable.** Setup: a run artifact with one `skipped` node and a downstream `upstream-skipped` node caused by it. Action: render both formats. Expected: the two nodes carry different styles; the `skipped` node's style is the one documented for an originated skip and is not reused for any propagated state.
- **Originated vs propagated failure are distinguishable.** Setup: a run artifact with a `failed` node and a downstream `upstream-failed` node; a separate fixture with a `timed-out` node. Action: render both formats. Expected: `failed`, `timed-out`, and `upstream-failed` each carry a distinct documented style; no propagated-failure style collides with an originated-failure style.
- **Cancellation-family states are distinct.** Setup: a run artifact containing a `cancelled` node and an `abandoned` node. Action: render both formats. Expected: the two carry distinct documented styles, distinct from every other state.
- **Resume carry-forward is styled.** Setup: a run artifact with a `satisfied-from-prior` node. Action: render both formats. Expected: the node carries its own documented style, distinct from `succeeded`.
- **Single-node-replay marking is handled.** Setup: a single-node-replay run artifact (C26) in which the selected node has a terminal state and unselected nodes are marked `not-requested`. Action: render both formats. Expected: `not-requested` nodes render with their own documented style; no error is raised; the marking is not treated as a terminal state.
- **Duration annotations appear and are correct.** Setup: a run artifact whose nodes have known, distinct durations. Action: render both formats. Expected: each node's annotation reflects its recorded duration in the documented human-readable form; golden files match.
- **Every node and edge still appears with the overlay on.** Setup: the 30-node fixture from T46 with a matching run artifact covering all nodes. Action: render both formats with the overlay. Expected: every graph node and edge from the artifact is present in the output (structural check), data and ordering edges keep their distinct T46 styling, groups still render as clusters, and every node additionally carries state colouring and a duration.
- **Reference tools accept overlaid output.** Setup: the overlaid DOT and Mermaid from the 30-node fixture. Action: run the CI reference tools — `dot` parses the DOT; the Mermaid parser accepts the Mermaid. Expected: both are accepted, confirming the overlay's styling additions produce valid source.
- **Works on a historical artifact with no producing binary.** Setup: a frozen run artifact from the fixture corpus, rendered by a test binary that never assembled or ran that pipeline. Action: render both formats with the overlay. Expected: output is produced correctly with no access to the originating binary, proving artifact-only operation.
- **Graph/run node mismatch is defined, not fatal.** Setup: (a) a graph node with no corresponding run record; (b) a run record whose node id is absent from the graph. Action: render both formats. Expected: each case follows the documented behaviour (the graph-only node renders without overlay styling; the extra run record is reported per the documented rule) and neither panics.

## Definition of done
- [ ] Rendering accepts an optional run artifact overlaid on the graph artifact, in both DOT and Mermaid, and with none supplied reproduces the T46 structural output unchanged.
- [ ] Every terminal state in the normative table maps to a documented distinct style; the mapping table is published in rustdoc on the overlay entry point.
- [ ] Originated skips (`skipped`) are visually distinguishable from propagated skips (`upstream-skipped`), and originated failures (`failed`, `timed-out`) from propagated failures (`upstream-failed`), in both formats.
- [ ] `cancelled`, `abandoned`, and `satisfied-from-prior` each render with their own distinct documented style.
- [ ] The `not-requested` single-node-replay artifact marking (C26) renders with its own documented style and is never treated as a terminal state or an error.
- [ ] Each node is annotated with its duration, derived from the run artifact, in both formats.
- [ ] With the overlay applied, every node and edge in the artifact still appears in the output, data and ordering edges keep their distinct styling, and groups still render as clusters (T46 guarantees preserved).
- [ ] The overlaid DOT is accepted by `dot` and the overlaid Mermaid by the Mermaid parser in CI.
- [ ] Overlay rendering requires no access to the binary that produced the artifacts and is proven against a historical/frozen run artifact.
- [ ] Node-identity join between graph and run artifacts has defined, documented, non-panicking behaviour when a node appears in only one of the two.
- [ ] Golden-file tests cover overlaid DOT and Mermaid for the nine-state fixture, the skip/failure origination fixtures, the `not-requested` fixture, and the 30-node fixture.
- [ ] The C24 machine-classed acceptance criteria affected by this ticket are covered by automated tests entered in the criteria matrix (per system criterion 8).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The structural (no-run) DOT/Mermaid renderer, edge-kind styling, group clustering, reference-tool acceptance harness, and base golden files — all delivered by T46; this ticket only adds the overlay on top.
- Producing, folding, or validating the run artifact itself (C22 / T42) and the artifact-compatibility CI corpus (T48); this ticket consumes existing artifacts and reuses fixtures, it does not define schemas.
- The M3 explain-a-run demo (T49), which wires this overlay into an end-to-end walkthrough.
- Any interactive, live, or web-served view of a run — renderers consume static artifacts only; dagr is not a web interface and never watches a live pipeline. Do not add HTML, a viewer, or animation.
- Manual layout, theming, or a configurable style DSL — the mapping is a fixed, documented table; dagr is not a DSL and readable-with-no-manual-layout is the design goal, not a knob.
- Run summary, critical-path highlighting, and node metrics (T43, T44), which are separate C22/C23 concerns, not diagram overlays here.
