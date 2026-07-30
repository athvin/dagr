# dagr-artifact

The artifact types of [dagr](https://github.com/athvin/dagr): the serializable,
self-contained records a pipeline run leaves behind — the **event stream**, the
**graph artifact**, and the **run artifact** — together with their versioned
published schemas.

Every run produces artifacts, including runs that crashed, were cancelled, or
failed during assembly or bootstrap. A run's duration and resource profile is
answerable **entirely from these records**, with no access to the machine that
produced them.

## Its place in the workspace

This crate is a deliberate **boundary**, not a utility bag:

```text
cli ──────► core, artifact, render
render ───► artifact                 (renderers consume artifacts ONLY)
metastore ► artifact                 (the opt-in run index)
core ─────► macros  (build-time)
artifact ─► (nothing in the workspace) ◄── you are here
```

`dagr-artifact` depends on no other workspace crate, so it can never drag in the
live-pipeline surface. That is what makes "rendering requires no access to the
binary that produced the artifacts" a structural fact: the renderer's only edge
is onto this crate, and this crate has no edge onto the engine.

## What is in here

- **`event_stream`** — the append-only JSON Lines writer that is the
  authoritative record of a run, and the reader that tolerates exactly one
  trailing partial record (the crash case) and refuses any other corruption.
- **`fold`** — the standalone fold that turns an event stream into a run
  artifact, one record per *attempt*, never collapsed per node. It is what lets a
  crashed run still produce an artifact after the fact.
- **`canonical`** — the canonical JSON form (sorted keys, integer numbers,
  compact) every writer emits through, so two runs of the same source produce
  byte-identical artifacts.
- **`schema`** — validation against the published JSON Schemas, behind the
  default-off `schema-validation` feature because its validator dependency is
  CI-/dev-scoped. The runtime writers depend only on `serde_json`.

## Schema stability

Each artifact kind carries a schema version. Evolution *within* a version is
additive-only: readers ignore unknown fields and default missing ones, and a
fixture corpus with one artifact per released version is parsed in CI forever
after. The schema documents themselves live at
[`schemas/`](https://github.com/athvin/dagr/tree/main/schemas) in the repository.

## Documentation

The component specification is
[`docs/arch.md`](https://github.com/athvin/dagr/blob/main/docs/arch.md) —
C19 (event stream), C20 (graph artifact), C22 (run artifact), C24 (renderers).

Licensed MIT.
