# 105 · T90 — produced/consumed lineage events

> **Milestone:** M8 · **Size:** M · **Type:** feature · **Components:** C19, C22
> **Branch:** `feat/t90-produced-consumed-lineage` · **Depends on:** T89, T50 · **Blocks:** T91

## Why / context

This is the heart of the operator's data-artifact ask: a first-class, queryable record of **what each run produced and consumed** — dagr's analog of Airflow's `asset_event`, but derived from data dagr already stamps. Today "a durable output was produced here" is implicit in an attempt row; T90 promotes it to an explicit, append-only lineage record, and adds the consumed side. It builds on T89's reference metadata (content hash/size) and on the existing edge model: data edges are known at build time (C3/C11), ordering edges from C4/T50, and on resume the rehydrate map (`crates/core/src/resume.rs`) already resolves which durable producer fills each consumer's slot.

Two Airflow disciplines are adopted verbatim: the produced record is **immutable/append-only**, and it carries **no hard foreign key** to any asset-identity row — so lineage outlives garbage-collection of the referent. Both additions are event-stream-first (a new event kind + additive fold outputs), consistent with dagr's event-sourced-then-folded architecture; the metastore projection is T91.

## Objective

Add produced and consumed lineage to the event stream and fold.

- Add a new **`output-produced`** event kind (`crates/artifact/src/event_stream.rs`, extending the closed `kind` enum) emitted alongside each durable node's succeeded `attempt-outcome`, carrying `{ node, attempt, uri, content_hash, size_bytes, kind, produced_at_offset_ns, originating_run }` (uri/hash/size from T89's reference + metadata). Copy it forward, marked satisfied-from-prior, on resume (so a carried output still appears in the resumed run's lineage).
- Fold `output-produced` events into an **`outputs[]`** array on the run artifact (`crates/artifact/src/fold.rs`) — append-only, immutable, **no FK**.
- Record **consumed `inputs[]`** on the consuming node's `AttemptRecord`: `{ uri, content_hash }` for the durable references it actually read, populated from the resume rehydrate map (on resume) and the resolved static data-edge producers (on a fresh run) — data already computed, so this is near-free.
- Bump the event-stream/run-artifact schema minor version and update the published schemas (T39); keep everything additive (old streams/artifacts still validate; the fold tolerates streams with no `output-produced` events).

## Test plan (write these first — TDD)

**Produced**
- Given a run with two durable-output nodes, when it completes, then two `output-produced` events are emitted with correct `{node, attempt, uri, content_hash, size_bytes, originating_run}`, and `fold_stream` yields a matching `outputs[]` (append-only, no FK to any asset row).
- Given a resume that carries a prior durable output forward, then an `output-produced` entry for it appears attributed to its `originating_run` (satisfied-from-prior), not re-produced.

**Consumed**
- Given a node consuming an upstream durable output, when it runs (fresh) and when it runs after resume-rehydration, then its `AttemptRecord.inputs[]` lists the `{uri, content_hash}` it read, matching the producing output's identity in both cases.

**Additivity**
- Given an old fixture stream with no `output-produced` events, when folded and schema-validated, then it passes and `outputs[]` is empty/absent; the schema version bump is recorded and old+new documents both validate (T48).

## Definition of done

- [ ] An `output-produced` event kind is emitted for each durable node's succeeded attempt with `{node, attempt, uri, content_hash, size_bytes, kind, produced_at_offset_ns, originating_run}`, and copied forward (satisfied-from-prior) on resume.
- [ ] `fold_stream` folds these into an append-only, FK-free `outputs[]` on the run artifact.
- [ ] Consuming attempts carry `inputs[] { uri, content_hash }` populated from the rehydrate map (resume) and static data-edge producers (fresh run).
- [ ] The event-stream/run-artifact schemas are minor-bumped and updated; old fixtures still validate; the fold tolerates streams without the new events.
- [ ] Produced/consumed lineage tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (Producer/consumer edges are known from the graph (C3/C11/C4) and the rehydrate map (C27); `uri`/hash come from T89. The append-only + no-FK disciplines follow the Airflow `asset_event` compass recorded in ADR 097's lineage note.)

## Out of scope
- Projecting `outputs[]`/`inputs[]` into the metastore and the optional `asset` identity table — **T91**.
- Any asset-identity row in this ticket (produced records reference a `uri` by value only).
- Scheduling or data-triggered runs off asset events (Airflow's asset *scheduler* cluster) — permanently out of scope (no scheduler).
- Scope boundary restated: lineage is per-run provenance data, not coordination; dagr remains not a scheduler, distributed system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
