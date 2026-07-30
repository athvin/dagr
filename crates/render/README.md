# dagr-render

The diagram renderer for [dagr](https://github.com/athvin/dagr). Given one
**graph artifact** — optionally overlaid with a **run artifact** — it emits
diagram source a human can read without hand-layout: **Graphviz DOT** and
**Mermaid**.

Both outputs carry every node and every edge, style *data* edges distinctly from
*ordering* edges, label data edges with the carried stable type name, and cluster
nodes by group. The run overlay colours nodes by terminal state, distinguishes an
originated skip from a propagated one, and annotates durations.

## Artifacts only — no access to the producing binary

```text
cli ──────► core, artifact, render
render ───► artifact  ONLY             ◄── you are here
metastore ► artifact
core ─────► macros  (build-time)
artifact ─► (nothing)
```

`dagr-render` depends on `dagr-artifact` and the sanctioned `serde`/`serde_json`
reader stack, and on **nothing else** in the workspace — in particular it has
**no** dependency edge onto `dagr-core`, the live-pipeline surface. Because that
edge does not exist, no code here *can* reference a live-pipeline type, so
"rendering requires no access to the binary that produced the artifacts" is a
property of the crate graph rather than a convention. A renderer therefore works
equally on a run from three months ago: it reads the published artifact schema
and nothing else — no network, no credentials, no filesystem access beyond the
artifact it is handed.

The standalone `dagr-render` binary shipped by this crate is that guarantee made
concrete: it builds and links with no access to `dagr-core` or `dagr-cli`.

## Reading is the schema gate

The fields a diagram depends on are *required* on the parsed structs, so an
artifact that fails the schema — a node missing its output type name, an edge
missing its endpoints — is **rejected with a diagnostic naming the problem**
rather than rendered partially. Unknown future fields are ignored, so a newer
artifact still renders (additive-only schema evolution).

## Deterministic and byte-stable

Both renderers are independent of the artifact's input order: clusters are
emitted in group-name order, nodes in identity-name order, edges in canonical
`(from, to, kind)` order. Byte-identity is pinned by golden-file tests, and both
output formats are accepted by their reference tools.

## Documentation

The component specification is
[`docs/arch.md`](https://github.com/athvin/dagr/blob/main/docs/arch.md) — C24
(renderers), C20 (graph artifact), C22 (run artifact).

Licensed MIT.
