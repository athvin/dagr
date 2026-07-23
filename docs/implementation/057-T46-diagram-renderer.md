# 057 · T46 — C24: diagram renderer

> **Milestone:** M3 · **Size:** M · **Type:** feature · **Components:** C24
> **Branch:** `feat/t46-diagram-renderer` · **Depends on:** T40 · **Blocks:** T47, T51, T55

## Why / context
dagr promises to explain itself: any checked-in graph artifact (C20, produced by T40 and schematized by T39) can be turned into a diagram a human can read without hand-layout. This ticket delivers the base renderer for component C24 (arch.md `### C24 · Renderers`): given a graph artifact alone, emit Graphviz DOT and Mermaid source that includes every node and edge, styles data edges distinctly from ordering edges (C4, arch.md line 143), and clusters nodes by group (C6, arch.md lines 170–175). It deliberately excludes the run overlay (colour-by-terminal-state), which is a separate concern landed by T47. The hard architectural constraint that governs the whole design is that rendering must work off artifacts only — never a live pipeline and never the binary that produced them (arch.md line 515, line 523) — so this renderer reads the published artifact schema and nothing else.

## Objective
Build the reference diagram renderer that turns one graph artifact into DOT and Mermaid diagram source, verified structurally and by golden files, with the reference tools gating CI.

- Read a graph artifact conforming to the C20/T39 published schema; reject an artifact that fails schema validation with a clear diagnostic naming the problem.
- Emit **Graphviz DOT** source: one node per artifact node, one edge per artifact edge, group membership expressed as DOT subgraph clusters.
- Emit **Mermaid** source: the same node set, edge set, and group clustering expressed in Mermaid's grammar (Mermaid subgraphs for groups).
- Style **data edges** and **ordering edges** with distinct, documented visual treatments in both formats; label data edges with the carried stable type name recorded in the artifact.
- Render each node with its stable declared name (never `type_name`; see C20, arch.md line 439) and its group association.
- Guarantee the renderer takes only an artifact as input — no dependency on the pipeline crate, no network, no credentials, no filesystem access beyond the artifact it is handed.
- Provide a checked-in **30-node fixture** graph artifact spanning multiple groups, data edges, ordering edges, and at least one ungrouped node, plus at least one node carrying both a data dependency and an additional ordering edge (C4, arch.md line 142).
- Establish golden-file (`.dot` and `.mmd`) tests for the fixture and wire the reference tools (`dot`, Mermaid's parser) into CI as acceptance gates.

## Test plan (write these first — TDD)
Every scenario below is derived from the C24 acceptance criteria (arch.md lines 520–523) and is independently checkable. Write them before any renderer logic exists.

- **Every node appears (DOT).** Setup: load the checked-in 30-node fixture artifact. Action: render to DOT. Expected: the DOT output declares exactly one node for each of the 30 artifact nodes, each carrying its stable declared name; no artifact node is missing and no extra node is invented — verified by parsing the output structurally, not by string matching.
- **Every node appears (Mermaid).** Setup: same fixture. Action: render to Mermaid. Expected: the Mermaid output contains exactly one node declaration per artifact node with its stable declared name; count and identity match the artifact one-to-one.
- **Every edge appears (both formats).** Setup: same fixture. Action: render to DOT and to Mermaid. Expected: each output contains exactly one edge per artifact edge, connecting the correct source and target nodes; edge count equals the artifact edge count in both formats.
- **Data vs ordering edges are styled distinctly (DOT).** Setup: a fixture containing at least one data edge and at least one ordering edge. Action: render to DOT. Expected: every data edge carries one documented style and every ordering edge carries a different documented style; the two style sets are disjoint; a node bearing both a data and an ordering edge shows each edge in its correct style.
- **Data vs ordering edges are styled distinctly (Mermaid).** Setup: same fixture. Action: render to Mermaid. Expected: data edges and ordering edges use distinct, documented Mermaid link forms; the two forms are disjoint and each edge uses the form matching its recorded edge kind.
- **Carried type name on data edges.** Setup: a fixture whose data edges record carried stable type names. Action: render to DOT and Mermaid. Expected: each data edge is labelled with the carried stable type name from the artifact; ordering edges (which carry no value, C4 arch.md line 144) carry no type label.
- **Groups render as clusters (DOT).** Setup: fixture with at least three groups plus at least one ungrouped node. Action: render to DOT. Expected: each group renders as a subgraph cluster containing exactly the nodes labelled with that group; the ungrouped node sits outside every cluster; groups do not nest (C6 arch.md line 170).
- **Groups render as clusters (Mermaid).** Setup: same fixture. Action: render to Mermaid. Expected: each group renders as a Mermaid subgraph containing exactly its member nodes; the ungrouped node is outside all subgraphs.
- **Reference tool accepts DOT (CI gate).** Setup: the fixture DOT output. Action: pipe it through `dot` (Graphviz) in parse/validation mode. Expected: `dot` accepts the input and exits zero; a deliberately malformed diagram would be caught, proving the check has teeth.
- **Reference tool accepts Mermaid (CI gate).** Setup: the fixture Mermaid output. Action: run it through Mermaid's parser. Expected: the parser accepts the input without error.
- **Golden DOT is stable.** Setup: the 30-node fixture and its checked-in golden `.dot`. Action: render and compare to the golden. Expected: byte-identical; any change to node set, edge set, styling, or clustering fails the test and is review-visible.
- **Golden Mermaid is stable.** Setup: the 30-node fixture and its checked-in golden `.mmd`. Action: render and compare to the golden. Expected: byte-identical.
- **Renders a historical artifact with no producing binary present.** Setup: hand the renderer a fixture artifact only; the crate/test does not link or depend on the pipeline crate that emitted it. Action: render both formats. Expected: succeeds — demonstrating that rendering requires no access to the binary that produced the artifact (arch.md line 523).
- **Rejects a schema-invalid artifact.** Setup: an artifact that fails the published C20/T39 schema (e.g. a required field removed). Action: attempt to render. Expected: the renderer refuses with a diagnostic naming the schema problem, rather than producing partial or misleading diagram source.
- **Stable declared names only.** Setup: a fixture where a node's informational `type_name` debug field differs from its stable declared name. Action: render both formats. Expected: node labels use the stable declared name; the unstable `type_name` never appears as a node identity or label (C20 arch.md line 439).

## Definition of done
- [ ] Renderer reads a graph artifact conforming to the published C20/T39 schema and produces diagram source without any access to the pipeline binary, network, credentials, or database (arch.md line 523, line 515).
- [ ] A schema-invalid artifact is rejected with a diagnostic naming the problem, not rendered partially.
- [ ] Graphviz **DOT** output is produced from the artifact; **Mermaid** output is produced from the same artifact.
- [ ] Every node in the artifact appears in each output exactly once, labelled with its stable declared name (never `type_name`) — verified structurally (C24 criterion, arch.md line 521).
- [ ] Every edge in the artifact appears in each output exactly once, connecting the correct nodes (arch.md line 521).
- [ ] Data edges and ordering edges carry distinct, documented styling in both DOT and Mermaid, and the styles are disjoint (C4 arch.md line 143; C24 arch.md line 521).
- [ ] Data edges are labelled with the carried stable type name from the artifact; ordering edges carry no value label (C4 arch.md line 144).
- [ ] Groups render as clusters (DOT subgraph clusters / Mermaid subgraphs); ungrouped nodes sit outside all clusters; groups do not nest (C6 arch.md lines 170, 174).
- [ ] Both output formats are accepted by their reference tools in CI: `dot` parses the DOT and Mermaid's parser accepts the Mermaid (C24 arch.md line 520), and the checks have teeth (a malformed input is rejected).
- [ ] A checked-in **30-node fixture** graph artifact exists, spanning multiple groups, at least one ungrouped node, data and ordering edges, and at least one node carrying both a data dependency and an additional ordering edge.
- [ ] Golden-file tests (`.dot` and `.mmd`) for the fixture pass byte-identically and fail on any structural or styling change (C24 arch.md line 521).
- [ ] The set of documented distinct styles is written down where the renderer is documented (edge kinds and group clustering), so downstream T47 overlay styling and T51/T55 consumers can rely on it.
- [ ] The renderer crate does not depend on the pipeline/core crate; the "artifacts only" constraint is enforced by the dependency graph, not merely by convention.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Run overlay / colour-by-terminal-state and duration annotations** — the run-artifact overlay, originated-vs-propagated skip styling, and duration labels are C24's overlay half, delivered by T47 on top of this base. This ticket touches graph artifacts only.
- **CLI `render` verb wiring** — exposing rendering as a command-line verb belongs to C26 / T55; this ticket delivers the renderer as a library capability with tests, not a user-facing verb.
- **Groups as an execution or artifact-organisation feature (C6 / T51)** — this ticket consumes the group label already present in the artifact for clustering only; it must not add group nesting, group-level concurrency, or group-level failure handling (C6 arch.md line 170).
- **Emitting or changing the graph artifact schema** — the artifact and its schema come from T39/T40; this ticket reads them and must not alter emission or the schema.
- **Manual layout, theming, or interactive rendering** — no hand-layout hints, no styling knobs beyond the documented edge-kind and cluster distinctions; readable no-layout output is the goal (arch.md line 517).
- **Scope-boundary temptations to resist:** do not let the renderer grow toward a web interface or an interactive viewer, and do not have it read a live pipeline or reach back to the producing binary — both cross dagr's permanent scope boundary and the C24 "artifacts only" rule.
