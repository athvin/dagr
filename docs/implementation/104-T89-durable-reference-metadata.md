# 104 · T89 — durable-reference metadata + resume mutation-detection

> **Milestone:** M8 · **Size:** M · **Type:** feature · **Components:** C22, C27
> **Branch:** `feat/t89-durable-reference-metadata` · **Depends on:** T88, T57, T42, T58 · **Blocks:** T90

## Why / context

The Airflow `models/` review found that dagr already models run/attempt state richly, but its **data lineage** is thin: `durable_reference` (C27, T57) is recorded as an **opaque pointer with no metadata** — no content hash, size, or kind. This is the first M8 step and it pays for itself twice. It makes resume rehydration **verifiable**: today's existence probe (C27, `crates/core/src/resume.rs`) is only Present/Absent/CannotDetermine, so a referent that still exists but was overwritten out-of-band silently rehydrates **stale** bytes; a recorded content hash lets resume refuse on a fingerprint mismatch — the same discipline `serialized_dag.dag_hash` gives structure, applied to data. And it seeds cross-run dedup/change-detection for the lineage records T90 adds.

It is a small, additive, schema-versioned change to the durable-output contract, the `attempt-outcome` event, and the fold — the event stream's open-world evolution rules (no `additionalProperties:false`; C19/T39) make it non-breaking. The reference itself stays the task's opaque string (T57); the metadata is optional and recorded only when the `DurableOutput` impl supplies it.

## Objective

Add optional durable-reference metadata and use it to harden resume.

- Extend the `DurableOutput` contract (C27, `crates/core/src/assembly.rs` durable-output surface) so an impl **may** supply a `durable_reference_meta { content_hash, size_bytes, scheme, produced_at_offset_ns }` alongside `serialize_reference()`; absent ⇒ unchanged behavior.
- Carry the optional metadata on the `attempt-outcome` event record (`crates/artifact/src/event_stream.rs`) and fold it onto the `AttemptRecord` in the run artifact (`crates/artifact/src/fold.rs`), as an **additive** field; bump the event-stream/run-artifact schema minor version and update the published schemas (T39) accordingly.
- Harden resume (`crates/core/src/resume.rs`): when `durable_reference_meta.content_hash` is present, the existence probe may verify it; add a `ReferenceExistence::Changed` outcome (or a `ResumeRefusal::MutatedReference { node, reference, expected_hash, actual_hash }`) so resume **refuses up front** on a mismatch instead of rehydrating stale bytes — parity with the existing DanglingReference refusal.
- Keep it optional end to end: streams/artifacts without the field validate and behave exactly as before (round-trip old fixtures).

## Test plan (write these first — TDD)

**Recording**
- Given a durable node whose `DurableOutput` supplies metadata, when it succeeds, then the `attempt-outcome` event carries `durable_reference_meta` and `fold_stream` places it on the `AttemptRecord`; given an impl that supplies none, then the field is absent and behavior is unchanged.
- Given an old fixture stream without the field, when folded and schema-validated, then it passes (additive, open-world).

**Resume hardening**
- Given a resume where a demanded durable referent still exists but its content hash differs from the recorded `content_hash`, then resume **refuses** with the mutation/`Changed` outcome (naming node + expected/actual hash) rather than rehydrating; given a matching hash, then rehydration proceeds; given no recorded hash, then behavior is unchanged (Present/Absent/CannotDetermine as today).

**Schema**
- Given the schema-version bump, when the artifact/event-stream schemas are validated in CI (T48), then old and new documents both validate and the version bump is recorded.

## Definition of done

- [ ] `DurableOutput` may optionally supply `durable_reference_meta { content_hash, size_bytes, scheme, produced_at_offset_ns }`; absent ⇒ no behavior change.
- [ ] The `attempt-outcome` event and the folded `AttemptRecord` carry the optional metadata additively; the event-stream/run-artifact schemas are minor-bumped and the published schemas updated; old fixtures still validate.
- [ ] Resume verifies `content_hash` when present and refuses on mismatch via a `Changed`/`MutatedReference` outcome naming node + expected/actual hash; matching or absent hash behaves as before.
- [ ] Round-trip tests prove streams/artifacts without the field are unaffected.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None. (The field is optional and the reference stays the task's opaque string per T57; hashing is the impl's choice. Any schema-version numbering follows T39/T4 conventions, recorded in-PR per §5.)

## Out of scope
- The `output-produced`/`inputs` lineage records — **T90** (this ticket only enriches the existing `durable_reference`).
- Projecting metadata into the metastore (`node_attempt` columns) — **T91**.
- Forcing content hashing on any `DurableOutput` impl (it stays optional).
- Scope boundary restated: richer reference metadata is still per-run data on the existing record; dagr remains not a scheduler, distributed execution system, coordinating metadata store, web interface, DSL, or backfill orchestrator.
