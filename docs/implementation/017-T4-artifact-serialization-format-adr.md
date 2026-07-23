# 017 · T4 — ADR: artifact serialization format and schema versioning

> **Milestone:** M0 · **Size:** S · **Type:** decision · **Components:** C19, C20, C22
> **Branch:** `adr/t4-artifact-serialization-format-adr` · **Depends on:** T0.6, T0.10 · **Blocks:** T19, T39

## Why / context
Every observable output of a dagr run — the event stream (C19), the graph artifact (C20), and the run artifact (C22) — is a serialized record, and the whole pitch of the tool ("recorded artifacts are worse to break than the API," Stability) rests on those records having a pinned wire format and a disciplined versioning story. This ADR locks the encodings, the schema-version field semantics, the schema language used to describe and validate them, and the in-repo home of the published schemas, so that the writers (T19) and the schema-publishing task (T39) build against a settled decision rather than inventing one each. The durability/sink question is already answered by the run store ADR (T0.6); the schema-evolution rules and fixture-corpus plan are set by T0.10 and this ADR merely names the concrete format that realizes them. This is a decision ticket: its deliverable is a committed ADR plus the tiny throwaway evidence that the chosen format and validation crate actually satisfy the "validates against published schema" criteria of C20 and C22.

## Objective
Produce and commit an Architecture Decision Record that settles, with rationale and explicitly rejected alternatives, the serialization contract shared by C19/C20/C22. Concretely, the ADR must decide and record:

- **Event-stream encoding:** JSON Lines (one self-contained JSON object per line, newline-delimited), matching C19's "append-only sequence of single-line records" and the run store's line-append sink from T0.6. State which JSON value shape a record is and the trailing-partial-record tolerance the format implies.
- **Artifact encoding:** a single pretty-or-canonical JSON document per graph artifact and per run artifact, and which of the two forms is authoritative for the byte-identity criterion (C20) versus human reading.
- **Schema-version field:** the field name, placement (every event record per C19; every artifact header per C20/C22), value form (a single integer or a documented `<name>@<version>` string), and its semantics under T0.10's additive-only, ignore-unknown, default-missing evolution rule. Record that the folding function declares which stream schema versions it reads (C22).
- **Schema language and validation crate:** pick the JSON Schema draft and the Rust validation crate that satisfy the "validates against its published schema" acceptance criterion of C20 and C22 (this is the Open question below); justify the choice against the alternatives and against the core-crate minimal-dependency posture (Stability / supply chain).
- **In-repo schema location and naming:** the directory that holds the published schemas, the per-artifact-kind and per-version file-naming scheme, and how the fixture corpus (one artifact per released schema version, T0.10) is laid out beside them so T39 and the later validation CI (T48) have a fixed target.
- **Canonicalization rules** needed to make C20's "emitting twice produces identical bytes outside the generation-time field" achievable: key ordering, number formatting, whitespace, string escaping, and the explicit exclusion of the generation-time field from byte comparison.

## Test plan (write these first — TDD)
Because this is a decision ticket, the "tests" are decision-record checks plus a throwaway prototype that produces evidence for the format and validation-crate choice. Each is concrete and independently checkable.

- **ADR completeness check.** Setup: open the committed ADR. Action: scan for each required decision (event encoding, artifact encoding, schema-version field, schema language + crate, schema location/naming, canonicalization). Expected outcome: every one is present, each states the chosen option, its rationale, and at least one named rejected alternative; the status is `accepted`.
- **Schema-version semantics check.** Setup: read the ADR's schema-version section against T0.10's evolution rules. Action: confirm it defines what an unknown field, a missing field, and a bumped version each mean to a reader. Expected outcome: the rules are "ignore unknown, default missing, additive-only within a version, version bump for anything else," consistent with C22 and the Stability section, with no contradiction.
- **JSONL round-trip prototype.** Setup: a throwaway prototype writes several representative event records (including a run-started record carrying the full artifact header per C19) as JSON Lines to a temp file. Action: read the file back line by line, parse each line independently, then truncate the last line mid-record and re-read. Expected outcome: every whole line parses to the same logical record; the truncated final line is detected and discarded as the single tolerated trailing partial, and all prior lines still parse — evidence that JSONL satisfies C19's abrupt-kill criterion.
- **Byte-identity prototype.** Setup: the prototype serializes the same sample graph-artifact value twice using the ADR's canonicalization rules. Action: compare the two byte strings, then compare again after changing only the generation-time field. Expected outcome: the two serializations are byte-identical when generation time is held equal, and differ only in the generation-time span when it changes — evidence the canonicalization rules meet C20's determinism criterion.
- **Validates-against-published-schema prototype.** Setup: the prototype hand-writes one minimal schema in the chosen JSON Schema draft for a sample artifact and loads it with the chosen validation crate. Action: validate a well-formed sample artifact and a deliberately malformed one (a required header field removed). Expected outcome: the well-formed artifact passes and the malformed one fails with a locatable error — evidence the chosen draft and crate satisfy the C20/C22 validation criterion. This prototype is deleted after the ADR is accepted; it is not merged code.
- **Additive-evolution prototype.** Setup: take the sample artifact and add one unknown field a future minor version might introduce. Action: validate it against the current schema and parse it with a reader built to the ADR's rules. Expected outcome: validation still passes (schema does not forbid unknowns) and the reader ignores the unknown field — evidence the format supports T0.10's additive-only evolution without breaking existing readers.
- **Location/naming check.** Setup: read the ADR's schema-location decision. Action: confirm it names an exact in-repo directory, a per-kind/per-version filename scheme, and the sibling fixture-corpus layout. Expected outcome: T39 and T48 could be started against those paths with no further decision needed.

## Definition of done
- [ ] The ADR decides JSON Lines as the event-stream encoding — one self-contained JSON object per line, written through the T0.6 line-append sink — consistent with C19's "append-only sequence of single-line records."
- [ ] The ADR decides JSON as the graph-artifact and run-artifact encoding and names which serialization form is authoritative for byte-identity (C20) versus human reading.
- [ ] The ADR fixes the schema-version field's name, placement on every event record (C19) and in every artifact header (C20, C22), value form, and its meaning under the additive-only / ignore-unknown / default-missing evolution rule from T0.10 and the Stability section.
- [ ] The ADR records that the run-artifact folding function declares which event-stream schema versions it can read (C22).
- [ ] The ADR selects a JSON Schema draft and a Rust validation crate that together satisfy "the artifact validates against its published schema" for C20 and C22, with rationale weighed against the core-crate minimal-dependency posture (Stability / supply chain).
- [ ] The ADR specifies canonicalization rules (key ordering, number and whitespace formatting, string escaping) sufficient for C20's "emitting twice produces identical bytes outside the generation-time field," and explicitly excludes the generation-time field from byte comparison (C20, C21).
- [ ] The ADR fixes the in-repo directory, per-kind and per-version filename scheme for published schemas, and the sibling layout for the fixture corpus (one artifact per released schema version, parsed in CI forever after — C22, T0.10), giving T39/T48 a fixed target.
- [ ] The ADR confirms the format tolerates at most one trailing partial record on the event stream (C19) and that concatenating records from concurrent runs, partitioned by run identity, stays valid (C19).
- [ ] The ADR names the assembly-failed / bootstrap-failed and single-node `not-requested` artifact variants (C22) as shapes the schemas must accommodate, deferring their field definitions to T39.
- [ ] Throwaway prototype evidence for the JSONL round-trip/trailing-partial, byte-identity, published-schema validation, and additive-evolution checks is captured in or linked from the ADR; the prototype code is not merged.
- [ ] The ADR is committed at a conventional ADR location with status `accepted`, cross-references C19/C20/C22 and the Stability section, and lists its rejected alternatives.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Which schema language and validation crate satisfy the validates-against-published-schema criterion — specifically, which JSON Schema draft? Resolve this within the ADR by picking a draft and a crate and recording the rationale.

## Out of scope
- Writing the event-stream writer itself (T19) or emitting any real artifact — this ticket only decides the format the writers will target.
- Authoring, publishing, or validating the actual versioned schema files (T39) and the artifact-validation/compatibility CI and fixture-corpus seeding (T48); this ADR names their location and rules, it does not produce them.
- The run store sink trait, base-location flag, run-id scheme, flush and durability semantics, and event-write-failure path — all owned by T0.6 and merely referenced here.
- The stable-name trait, fingerprint composition, and canonicalization of fingerprint inputs (T0.7 / C21) beyond noting the generation-time exclusion needed for byte-identity.
- The durable-output reference field's contract (T0.8 / C27); this ADR only reserves a place for it in the run-artifact shape.
- Node-metrics schema specifics beyond acknowledging metrics are an artifact field (C23).
- Any encoding other than line-delimited JSON events and JSON artifacts — no binary formats, no protobuf/Avro, no columnar store — and no metadata database, query layer, or web viewer for artifacts (permanent scope boundary: dagr is not a metadata store or a web interface).

---

# ADR: artifact serialization format and schema versioning

> The repo keeps each ADR inside its own implementation-ticket file (the T1, T2,
> T0.2, T0.3, T0.4, T0.5, T0.6, T0.7, T0.8, and T0.10 ADRs all embed the ADR at
> the ticket's own `path`). This ADR is committed here, at
> `docs/implementation/017-T4-artifact-serialization-format-adr.md`, the ADR
> location for ticket T4 — satisfying the DoD line "committed at a conventional
> ADR location with status `accepted`" literally, with zero deviation, and linked
> from the tasks/spec index (`docs/tasks.md` T4 entry and
> `docs/implementation/README.md`) so its consumers T19/T39/T48/T40/T42 find it.
> Its mechanical acceptance gate is
> [`scripts/check-artifact-serialization-format-adr.sh`](../../scripts/check-artifact-serialization-format-adr.sh),
> which cross-references this ADR against arch.md (C19, C20, C22, C21, C27,
> "Stability", "Vocabulary") and the merged dependency ADRs (T0.6, T0.10, T0.7,
> T0.8) so every seam element appears with no invention and no omission.

## Status

Accepted (2026-07-23). This is a **decision** ticket that locks the
**serialization contract shared by C19 (event stream), C20 (graph artifact), and
C22 (run artifact)** — the wire encodings, the schema-version field and its
evolution semantics, the schema language and validation crate, the in-repo schema
and fixture-corpus location, and the canonicalization rules — and ships **no
production code**. The shipping crates (`core`, `artifact`, `render`, `cli`) are
unchanged, `Cargo.lock` is untouched, and the only committed artifacts are this
ADR and the mechanical acceptance script named above.

This is a **DOC-ONLY decision** with a **throwaway spike for evidence only**. The
format and versioning scheme are decidable from arch.md (C19, C20, C22,
"Stability") plus the merged dependency ADRs; the **event-stream WRITER is
IMPLEMENTED by T19**, the **versioned SCHEMA FILES and the fixture corpus are
authored by T39 and seeded/validated by T48**, and the artifact **TYPES / emission
/ folding are T40 (C20) and T42 (C22)** — this ticket only *decides* the format
and versioning they target. A small **throwaway prototype was built in `/tmp`
(outside the workspace) purely to gather the format/determinism/validation
evidence the Test plan asks for, and was DELETED after its evidence was quoted
into §Decision below; no prototype code is merged and the tree stays clean** (see
§"Spike disposition and evidence"). No round-trip or determinism *fixture* is
shipped in the repo: the ticket's Test plan explicitly quarantines the prototype
("This prototype is deleted after the ADR is accepted; it is not merged code"),
and shipping executable fixtures would implement T19's writer and T39/T48's schema
corpus, which this ticket's Out of scope forbids.

This ADR is a **canonicalization of the seam**, not a re-decision: the JSONL event
encoding, the JSON artifact encoding, the schema-version field, and the
additive-only evolution rule are exactly what arch.md §§ C19 / C20 / C22 and
"Stability" already fix, and the run-store file names, the evolution rules, the
fingerprint-determinism requirement, and the durable-reference slot are already
fixed by T0.6, T0.10, T0.7, and T0.8. Where arch.md leaves an *interface
presentation* choice (the schema-version field's exact name and value form, which
JSON Schema draft, which validation crate, the exact schema directory and filename
scheme, the concrete canonicalization rules), this ADR records the choice and its
rationale without changing any semantics. Nothing here invents a binary format, a
second metadata store, a query layer, or a web viewer; the seam is closed by
construction (see **Rejected alternatives**).

The consumers it unblocks build against this seam and re-decide none of it:
**T19** (029 — event stream writer, C19) writes JSONL records through the T0.6
sink using this record shape and schema-version field; **T39** (050 — publish
artifact schemas, C22) authors the versioned JSON Schema files at the location and
naming this ADR fixes, including the optional per-attempt `durable_reference` slot
(T0.8) and the assembly-failed / bootstrap-failed / not-requested variants;
**T48** (059 — artifact-validation and compatibility CI) validates emitted
artifacts against those schemas and seeds/freezes the fixture corpus per T0.10;
**T40** (051 — graph artifact emission, C20) serializes the graph artifact in the
canonical form under these canonicalization rules; **T42** (053 — event-stream
folding into a run artifact, C22) serializes the run artifact and declares the
stream schema versions it reads. Each hand-off is named in **§Consequences**.

**Consistency with the already-merged M0 ADRs is load-bearing** and holds:

- **T0.6** (012 — run store contract) reserves the file names **`events.jsonl`**
  (C19 event stream), **`graph.json`** (C20), and **`run.json`** (C22) under
  `<base>/<pipeline>/<run-id>/`, fixes the **two-operation line-append sink**
  (append one complete line, flush), the **per-record header** (run identity,
  schema version, gapless sequence number, informational wall-clock stamp,
  authoritative monotonic offset), and the **at-most-one-trailing-partial-record**
  reader tolerance. This ADR is the reciprocal: it fixes the **byte encodings**
  those reserved names carry and the **shape** of the schema-version field the
  header already reserves. T0.6 explicitly deferred "the concrete artifact
  serialization format and schema-version field semantics (JSONL events, JSON
  artifacts) — that is T4." There is no overlap and no contradiction.
- **T0.10** (005 — stability policy) fixes the **additive-only-within-a-version**
  evolution rule for all three schemas, the **reader contract** (ignore unknown
  fields, default missing ones), that the **folding function declares which stream
  schema versions it reads**, that a **non-additive change is a version bump** (a
  recorded-artifact-compatibility major event), and the **fixture-corpus plan**
  (in-repo at **`tests/fixtures/corpus/`**, one artifact per released schema
  version per kind plus the ten-thousand-attempt scale artifact, seeded at M3,
  parsed in CI forever after). This ADR **consumes** those rules and names the
  concrete format/draft/crate that realizes them; it does **not** re-decide the
  versioning policy. T0.10 explicitly deferred "choosing the concrete
  serialization format, schema language, or validation crate — that is T4."
- **T0.7** (013 — stable-name and fingerprint composition) fixes **BLAKE3** as the
  fingerprint hash (v1 under a versioned algorithm) over a **deterministic,
  registration-order-independent canonical byte encoding**, with **generation time
  excluded** from byte-identity. This ADR's canonicalization rules (§6) are the
  reciprocal at the *artifact* boundary: the same determinism discipline (sorted
  keys, integer numbers, compact whitespace, generation-time exclusion) that makes
  "assemble twice → byte-identical artifact" achievable is what feeds T0.7's
  BLAKE3-v1 fingerprint reproducibly. There is no overlap: T0.7 owns the
  fingerprint *input* canonicalization and hash; this ADR owns the *artifact wire*
  canonicalization. They agree by construction.
- **T0.8** (014 — durable-output contract) fixes that each **attempt record**
  carries an **optional, serde-serializable `durable_reference`** field
  (`Some(reference)` on a durable node's succeeded attempt, `None` otherwise),
  opaque to the schema, published by T39. This ADR **reserves the slot** in the
  run-artifact shape (§8) and its optionality, deferring the field's publication
  to T39 — exactly the boundary T0.8 states.

There is **no supersession** and **no spec conflict**: every clause below is a
direct reading of arch.md, consistent with T0.6 / T0.10 / T0.7 / T0.8.

**Open questions.** The ticket's `## Open questions` and the `docs/tasks.md` T4
entry both carry **one** question: *"Which schema language and validation crate
satisfy the validates-against-published-schema criterion — specifically, which
JSON Schema draft?"* It is **resolved in §4 below**: **JSON Schema draft 2020-12**
validated by the **`jsonschema`** Rust crate, with the rationale and rejected
alternatives recorded there. No open question remains.

## Context

`docs/arch.md` makes every observable output of a run a **serialized record**, and
the tool's whole pitch — *"breaking recorded artifacts is worse than breaking the
API"* (Stability) — rests on those records having a **pinned wire format** and a
**disciplined versioning story**. Three arch.md components and the Stability
section meet at this decision and must mean *exactly* the same thing by "the
encoding," "the schema version," "canonical," and "validates against its schema":

- **C19 · Event stream.** Fixes an **append-only sequence of single-line records**
  written through the run store's sink as events occur; every record carries run
  identity, a **schema version**, a monotonic sequence number, an informational
  wall-clock stamp, and an authoritative monotonic offset; the run-started event
  carries every run-artifact header field known at start; a reader **tolerates and
  discards at most one trailing partial record**; and records from concurrent runs
  **concatenate and partition safely** by run identity.
- **C20 · Graph artifact.** Fixes an on-demand structural artifact carrying a
  header (schema version, tool version, generation time, pipeline identity, build
  provenance, graph fingerprint); that **emitting twice produces identical bytes
  outside the generation-time field**; and that **the artifact validates against
  its published schema**. Generation time is excluded from byte-identity.
- **C22 · Run artifact.** Fixes a run outcome **derived from the event stream** (at
  run end, or by **folding** a partial stream after a crash); a header + one record
  **per attempt** + a summary; the assembly-failed / bootstrap-failed variants and
  the single-node `not-requested` marking; that **schema evolution is
  additive-only** and the **folding function declares which stream schema versions
  it reads**; that **the artifact validates against its published schema** and
  **every fixture-corpus artifact from prior schema versions remains parseable**.
- **Stability.** Fixes that the three schemas each carry a schema version, evolve
  additive-only with reader tolerance, and that **a fixture corpus with one
  artifact per released schema version is parsed in CI forever after (C22)**; and
  that **the core crate holds a minimal dependency set** whose additions are
  reviewed as API decisions (supply chain).

Landing this wrong means T19, T39, T40, T42, and T48 each invent a different
encoding, schema-version shape, canonicalization, or schema location, so the
contract below is fixed once, in one place, in arch.md's exact vocabulary.

## Decision

### 1. Event-stream encoding — JSON Lines

The event stream is **JSON Lines (JSONL)**: an **append-only sequence of records,
one self-contained JSON object per line, newline-delimited (`\n`)**, written into
the reserved **`events.jsonl`** through the **T0.6 line-append sink** (append one
complete line, flush). Concretely:

- **A record is a single-line JSON object** (a JSON value of shape *object*, never
  a bare array/scalar), serialized **compact** (no embedded newline — the sole
  `\n` is the record terminator, so a record occupies exactly one physical line).
  Every record carries the T0.6 header fields (§7 of T0.6): `schema_version`,
  `run_id`, `seq`, `wall`, `offset_ns`, plus the event-specific body.
- **Trailing-partial tolerance.** Because the default local-file sink does not
  fsync per event (T0.6 §6), an abrupt kill can leave the **final** line
  half-written. A conforming reader parses each line independently and **tolerates
  and discards at most one trailing partial record** — an unterminated or
  unparseable *final* line — while every prior whole line still parses. A
  non-final line that fails to parse is a corruption, not the tolerated partial.
  JSONL makes this trivially checkable: record boundaries are physical newlines, so
  a truncated tail is exactly one incomplete line. This is the encoding-level
  realization of C19's abrupt-kill criterion (evidence in §"Spike disposition").
- **Concurrent-run concatenation.** Because every record carries `run_id` (T0.6
  §7) and each run writes its own `events.jsonl` under a disjoint directory,
  concatenating two runs' streams and **partitioning by run identity** stays valid:
  JSONL concatenation is byte-append, and partition is a per-line `run_id` filter.
  No cross-run index is involved (C19; T0.6 §10).

### 2. Artifact encoding — a single JSON document, canonical form authoritative

Each **graph artifact** (`graph.json`, C20) and each **run artifact** (`run.json`,
C22) is a **single JSON document** — one JSON object per file, not a stream.

- **Two forms, one authoritative.** The **canonical form** (§6 — compact, sorted
  keys, integer numbers, minimal escaping) is the **authoritative durable form and
  the one over which byte-identity is defined (C20)**. A **pretty-printed form**
  (indented, human-legible) is a **rendering for human reading only** — never the
  durable bytes, never fed to byte-identity or to a fingerprint. The writer emits
  the canonical form to the run store; a `--pretty` presentation (a CLI/render
  concern, owned by C26/C24) may reformat it for a human without changing the
  authoritative bytes. This mirrors the event stream: the durable `events.jsonl`
  is compact; a human viewer may pretty-print a line.
- **Why JSON (not a second format for artifacts).** JSON is **self-describing**
  (field names travel with the data, which is what lets an older reader ignore
  unknown fields and a newer reader default missing ones — §3), **human-readable**
  (the artifact is meant to be read and diffed by operators), **ubiquitously
  tooled**, and **already the event-record encoding** — so one serde stack serves
  all three schemas. Its costs (larger than binary, floating-point ambiguity) are
  bounded here: artifacts are attempt-count-proportional (Performance envelope),
  not bulk data, and dagr's numeric fields are **integers** (offsets in
  nanoseconds, sequence numbers, costs in bytes/thread-counts per T0.5/T0.7), so
  no float round-tripping ambiguity arises (§6).

### 3. Schema-version field — name, placement, value form, and semantics

- **Field name: `schema_version`.** A single field named `schema_version` on
  **every event record** (C19) and in **every artifact header** (the first-level
  header object of `graph.json` and `run.json`, C20/C22). Snake-case matches the
  rest of the record header (T0.6 §7).
- **Value form: a `<name>@<version>` string.** The value is a **string** of the
  form **`<schema-name>@<major-integer>`**, e.g. **`dagr.event-stream@1`**,
  **`dagr.graph@1`**, **`dagr.run@1`**. The `<name>` part **self-identifies which
  of the three schemas** a record/artifact belongs to — load-bearing because all
  three live side by side under one run directory and a fold reads the stream to
  produce the run artifact, so a reader must tell an event record from an artifact
  header without positional context. The `@<version>` part is a **single
  monotonically increasing integer** (the schema's major version); there is no
  minor component, because *within* a version evolution is additive-only and
  therefore needs no sub-version to negotiate (§below). A `<name>@<version>`
  string is chosen over a bare integer precisely so the name disambiguates the
  three co-located schemas; it is chosen over a nested `{name, version}` object for
  compactness and greppability. (Rationale recorded so T39 does not re-litigate it.)
- **Semantics under T0.10's evolution rule** (this ADR consumes, does not
  re-decide):
  - **Unknown field → ignore.** A reader encountering a field it does not know
    **ignores it** and proceeds. (JSON Schema: the schemas set **no**
    `"additionalProperties": false`, so unknowns validate — §4; evidence in
    §"Spike disposition".)
  - **Missing field → default.** A reader encountering an absent field
    **substitutes the documented default** for that field's version.
  - **Additive-only within a version.** Within a given `@<version>`, the only
    permitted change is **adding** optional fields (with defaults). Existing fields
    never change type or meaning. A newer writer and an older reader interoperate,
    and vice versa, **within the same version**.
  - **Version bump for anything else.** Any **non-additive** change — removing a
    field, renaming it, changing its type or meaning — **requires a version bump**
    (`@1` → `@2`), which is a **recorded-artifact-compatibility major event** per
    T0.10 §2. A bumped version tells a reader "this is a different schema; consult
    its published document," never "a field silently changed."
- **The folding function declares which stream schema versions it reads (C22).**
  The stream→run-artifact fold (a standalone function and CLI verb, C26) **names
  the set of `dagr.event-stream@N` versions it can read**; encountering a stream
  version outside that set is a clean, distinct error, never a silent
  misinterpretation (T0.10 §3; owned by T42).

### 4. Schema language and validation crate — JSON Schema 2020-12 + `jsonschema`

*(This resolves the ticket's sole Open question.)*

- **Schema language: JSON Schema, draft 2020-12.** The published schemas (T39) are
  **JSON Schema documents** in the **2020-12 draft**. 2020-12 is the **current,
  stable** draft; it is expressive enough for the artifact shapes (typed object
  fields, required-field lists, enums for the terminal-state taxonomy, arrays of
  attempt records) while its **open-world default** (`additionalProperties` is
  permissive unless explicitly closed) is **exactly what additive evolution needs**
  — a future minor version's new field validates against the current schema without
  a schema change (§3). We deliberately **do not** set `"additionalProperties":
  false` on evolving objects, so the schema never forbids the unknowns the reader
  is required to ignore.
- **Validation crate: `jsonschema`.** The Rust **`jsonschema`** crate (the
  most-used pure-Rust JSON Schema validator) **supports draft 2020-12**, **compiles
  a schema once and validates many instances**, and **reports a locatable error**
  (a JSON-pointer instance path) on failure — satisfying "validates against its
  published schema" for C20 and C22 (evidence in §"Spike disposition": a
  well-formed artifact passes; a malformed one — a required header field removed —
  fails with `"fingerprint_structural" is a required property`).
- **Weighed against the core-crate minimal-dependency posture (Stability / supply
  chain).** `jsonschema` pulls a **non-trivial transitive tree** (a regex engine,
  a fraction/number-compare crate, ahash, and — with default features — an HTTP
  schema resolver via `reqwest`). Therefore the validator is **CI-/dev-scoped, not
  a core-crate runtime dependency**: schema validation is a **CI obligation** (C20
  "validates against its published schema"; C22 fixture-corpus parse-forever;
  owned by T48), not something the event writer or the artifact emitter does at
  runtime. When T48 adds `jsonschema`, it belongs in a **CI/test target with
  default features disabled** (drop `resolve-http`/`cli`, since dagr's schemas are
  local files, not fetched over the network — no `reqwest`), keeping the core
  crate's dependency set minimal per Stability. The runtime writers (T19, T40, T42)
  depend only on **`serde` + `serde_json`**, which are already the project's
  serialization stack.

### 5. In-repo schema location, naming, and the fixture-corpus layout

- **Schema directory: `schemas/` at the repo root.** The published JSON Schema
  documents live under a top-level **`schemas/`** directory (a stable,
  version-controlled path, visible and reviewed like code, separate from the
  `tests/` tree so the schemas are first-class published artifacts, not test
  fixtures).
- **Per-kind, per-version filename scheme:
  `schemas/<kind>/v<version>.schema.json`.** One file per artifact kind per schema
  version, so a released version's schema is **frozen and addressable**:
  - `schemas/event-stream/v1.schema.json`
  - `schemas/graph/v1.schema.json`
  - `schemas/run/v1.schema.json`

  A version bump adds a **new file** (`v2.schema.json`) beside the old one, which
  is **never edited** — old readers keep validating old artifacts, satisfying
  C22's "prior-version fixtures remain parseable forever." (T39 authors the file
  contents; this ADR fixes the directory and naming.)
- **Sibling fixture-corpus layout (T0.10):
  `tests/fixtures/corpus/<kind>/v<version>/…`.** The fixture corpus lives where
  T0.10 fixed it — **`tests/fixtures/corpus/`** — laid out by artifact **kind**
  then **schema version**, holding **one frozen artifact per released schema
  version per kind** plus the **ten-thousand-attempt scale run artifact**
  (Performance envelope). A schema file `schemas/run/v1.schema.json` has its
  fixture(s) under `tests/fixtures/corpus/run/v1/`, so T48's compatibility job can
  pair each frozen fixture with the schema version that must forever validate it.
  (T48 seeds and freezes the corpus and wires the CI; this ADR fixes the layout so
  **T39 and T48 could be started against these exact paths with no further
  decision needed**.)

### 6. Canonicalization rules — deterministic bytes outside the generation-time field

To make C20's "emitting twice produces identical bytes outside the generation-time
field" achievable — and to feed T0.7's BLAKE3-v1 fingerprint **reproducibly** —
the canonical (authoritative, §2) form of every artifact and every event record
obeys a **single, fixed canonicalization**:

- **Key ordering: lexicographic by Unicode scalar value.** Every JSON object's
  member keys are emitted in **ascending lexicographic (byte/Unicode-scalar)
  order**, recursively, so map/hash iteration order never leaks into the bytes.
  (`serde_json` with a sorted key order, or serialization from an ordered map,
  gives this deterministically.)
- **Number formatting: integers only, shortest form, no exponent, no `-0`.** Every
  numeric field is an **integer** (nanosecond offsets, sequence numbers, attempt
  numbers, costs in bytes/thread-counts, metrics values per C23) emitted in its
  **shortest decimal form** with no leading zeros, no exponent, and no negative
  zero. **Floating point is not used** in canonical artifacts — which removes the
  one genuine JSON determinism hazard (float formatting varies across
  implementations). A task metric that is conceptually fractional is carried as an
  integer in a named unit (C23 convention), not a float.
- **Whitespace: compact, none insignificant.** The canonical form has **no
  insignificant whitespace** — no spaces after `:` or `,`, no indentation, no
  trailing newline inside an artifact document (the JSONL stream's sole whitespace
  is the one `\n` record terminator, §1). Pretty-printing (§2) is a separate
  human-facing rendering.
- **String escaping: minimal, deterministic, UTF-8.** Strings are valid **UTF-8**
  with **minimal JSON escaping** — escape only what JSON requires (`"`, `\`, and
  control characters U+0000–U+001F, via the shortest standard escape; non-ASCII
  printable characters are emitted literally as UTF-8, not `\u`-escaped) — so the
  same logical string always produces the same bytes.
- **Generation-time field excluded from byte comparison (C20, C21).** The
  artifact header's **generation-time field (`generated_at`)** is the *one* field
  that varies between two emissions of the same graph from the same binary. It is
  **explicitly excluded from the byte-identity comparison**: byte-identity is
  asserted over the canonical bytes **with the generation-time field masked/held
  equal**. Everything else is fixed per binary, so two emissions are **byte-
  identical outside `generated_at`**, and when `generated_at` is held equal they
  are **fully identical** (evidence in §"Spike disposition"). This is the same
  exclusion T0.7 already applies to the fingerprint inputs, applied here to the
  artifact wire bytes.

Together these rules make **"emit twice → byte-identical outside generation
time"** (C20) and **"the same canonical bytes feed a reproducible BLAKE3-v1
fingerprint"** (T0.7) both true by construction. The canonicalization is
**fixed**, not runtime-configurable; changing it is a schema-version-affecting
event reviewed as such.

### 7. What T19 inherits for the event record

T19 writes each event record as one compact JSON object per line into
`events.jsonl` through the T0.6 sink, carrying the T0.6 header fields and this
ADR's `schema_version` (`dagr.event-stream@1`). The gapless-sequence machinery,
the run-started event that carries the **full C22 artifact header** (so a stream
that ends one event later still identifies its run completely — C19), and the
per-event body shapes are **T19's** to build; this ADR fixes the **encoding, the
record value shape (single-line object), the schema-version field, and the
canonicalization** only.

### 8. Artifact shapes the schemas must accommodate (deferred field definitions to T39)

The schemas (T39) must accommodate these **run-artifact variants**, whose *field
definitions* are **deferred to T39**; this ADR names them as shapes the schema and
its versioning must leave room for:

- **`assembly-failed` and `bootstrap-failed` variants (C22).** A run that failed
  before execution still produces a `run.json`: the fingerprint is present only
  when assembly succeeded, the error list is complete, and there are zero
  attempts. The two variants are **distinct** (graph's fault vs machine's fault).
  The schema must permit a run artifact with **no fingerprint and no attempts**.
- **Single-node replay `not-requested` marking (C22/C26).** A single-node replay
  produces a distinct artifact variant marking nodes outside the request
  **`not-requested`** — an *artifact marking*, not a terminal state (Vocabulary).
  The schema must permit this marking on node entries.
- **The optional per-attempt `durable_reference` slot (T0.8/C27).** Each **attempt
  record** carries an **optional, serde-serializable `durable_reference`** field
  (`Some` on a durable node's succeeded attempt, `None` otherwise), **opaque to the
  schema** — the schema reserves *the slot and its optionality*, not the
  reference's internal shape, which is the task's. Its **publication is T39's**; this
  ADR reserves the place for it (exactly the boundary T0.8 §6 states).

## Spike disposition and evidence

A **throwaway prototype** was built **in `/tmp/t4-spike` (outside the workspace)**
with `serde`, `serde_json` (`preserve_order`), and `jsonschema` 0.18 (feature
`draft202012`), run under the pinned toolchain **rustc 1.95.0** (the workspace
MSRV, T0.10), and **DELETED after its evidence was quoted here**. **No prototype
code is merged; the repository tree is clean.** The evidence, verbatim from the
prototype's output, substantiates the four Test-plan checks:

- **JSONL round-trip + trailing-partial (C19).** Three event records (including a
  run-started record carrying the full artifact header) were written as JSONL, the
  final line was truncated 20 bytes mid-record, and the file re-read line by line:
  `whole_lines_parsed=2 trailing_partial_discarded=1` — every whole line parsed;
  the single truncated final line was detected and discarded; a non-final parse
  failure would have panicked (it did not). **JSONL satisfies C19's abrupt-kill
  criterion.**
- **Byte-identity determinism (C20).** The same graph-artifact value, built twice
  with **different key-insertion order** and a **different generation time**, was
  canonicalized (§6): `identical_outside_gentime=true (canonical size=255 bytes)`
  and `identical_when_gentime_equal=true` — byte-identical outside `generated_at`,
  and fully identical when `generated_at` is held equal. **The canonicalization
  meets C20's determinism criterion** and feeds a reproducible fingerprint (T0.7).
- **Validates-against-published-schema (C20/C22).** A minimal draft-2020-12 schema
  (no `additionalProperties: false`) was compiled by `jsonschema` and used to
  validate a well-formed artifact and a malformed one (required
  `fingerprint_structural` removed): `wellformed_valid=true malformed_valid=false
  malformed_error_count=1`, first error `"fingerprint_structural" is a required
  property`. **The well-formed artifact passes; the malformed one fails with a
  locatable error** — the C20/C22 validation criterion holds under this
  draft+crate.
- **Additive evolution (T0.10).** The sample artifact plus one **unknown future
  field** was validated and read: `unknown_field_still_valid=true
  reader_ignores_unknown=true` — validation passes (the schema does not forbid
  unknowns) and the reader ignores the unknown field. **The format supports
  T0.10's additive-only evolution without breaking existing readers.**

## Consequences

**Each blocked/related ticket inherits a named seam and reopens no question this
ADR closed:**

- **T19** (029 — event stream writer, C19) binds against **§1 (JSONL, single-line
  object into `events.jsonl` via the T0.6 sink), §3 (`schema_version`
  `dagr.event-stream@1`), §6 (canonicalization), §7**: it writes records, builds
  the run-started header, and manages gapless sequence — targeting this encoding,
  never inventing one.
- **T39** (050 — publish artifact schemas, C22) binds against **§4 (draft 2020-12,
  open-world), §5 (`schemas/<kind>/v<version>.schema.json`), §8 (the variants and
  the optional `durable_reference` slot)**: it authors the three versioned schema
  documents at the fixed paths with the fixed additive-only posture.
- **T48** (059 — artifact-validation and compatibility CI) binds against **§4 (the
  `jsonschema` validator, CI-scoped with default features trimmed), §5 (the
  `tests/fixtures/corpus/<kind>/v<version>/` layout)**: it validates emitted
  artifacts and seeds/freezes/parses-forever the corpus per T0.10.
- **T40** (051 — graph artifact emission, C20) binds against **§2 (single JSON
  document, canonical authoritative) and §6 (canonicalization + generation-time
  exclusion)**: it emits `graph.json` byte-identically outside `generated_at`.
- **T42** (053 — folding into the run artifact, C22) binds against **§2, §3 (the
  fold declares which `dagr.event-stream@N` versions it reads), §8 (the run-artifact
  variants + `durable_reference` slot)**: it serializes `run.json` and names its
  readable stream versions.

**Coverage matrix: no change.** C19, C20, and C22 remain `machine`/`unmapped` in
`docs/coverage-matrix.md`, deferred to their covering-test tickets (C19 → T19,
C20 → T40, C22 → T42), exactly as the matrix already records. A **decision ticket
owes no covering test**; the covering tests (including the "validates against its
published schema" assertions and the fixture-corpus parse-forever job) land with
T40/T42/T48, which each edit the matrix per its per-ticket duty. This ADR makes
**no edit** to the coverage matrix and **no edit** to the criteria-matrix partition
(T0.10's), and it **agrees** with both.

**Reopen condition.** If a downstream ticket cannot honor a seam as written — for
example, if draft 2020-12 or the `jsonschema` crate proves unable to express or
validate a required artifact shape, if the `jsonschema` transitive tree cannot be
trimmed to satisfy the supply-chain policy even in a CI-only target, if a numeric
field genuinely needs a non-integer value that JSON cannot canonicalize
deterministically, or if the `<name>@<version>` string proves insufficient to
disambiguate the co-located schemas — **the contract reopens here**, in this ADR,
rather than being worked around locally. A local workaround that silently diverges
(a second format, a runtime `additionalProperties: false` that forbids the
unknowns the reader must ignore, a float in a canonical artifact) is a defect, not
a fix.

## Rejected alternatives

- **A binary or self-describing binary format for artifacts** (protobuf, Avro,
  CBOR, MessagePack, Cap'n Proto, a columnar store). **Rejected on the scope
  boundary and on the tool's premise:** the ticket's Out of scope and arch.md's
  permanent non-goals forbid "any encoding other than line-delimited JSON events
  and JSON artifacts — no binary formats, no protobuf/Avro, no columnar store."
  Artifacts are **meant to be read and diffed by operators**; a binary format
  trades the self-description, human-readability, and universal tooling that make
  the artifact honest for a size win that is irrelevant at attempt-count scale
  (Performance envelope). The recorded artifact being *worse to break than the
  API* (Stability) argues for the **most legible, most-tooled** format, not the
  most compact.
- **A schema-less "just serde, no published schema"** approach (rely on the Rust
  types as the only contract). **Rejected:** C20 and C22 require the artifact to
  **"validate against its published schema,"** and T0.10 requires a **fixture
  corpus parsed in CI forever after** — both need a **language-independent,
  checked-in schema** a validator can run, which the Rust types alone are not. A
  published JSON Schema (§4/§5) is the contract external tooling and the
  compatibility CI validate against.
- **A bare integer schema-version value** (e.g. `"schema_version": 1`). **Rejected:**
  the three schemas live **side by side** under one run directory and a fold reads
  the stream to build the run artifact, so a reader must identify **which** schema a
  record/artifact is without positional context. A bare integer cannot
  self-identify the kind; the **`<name>@<version>` string** (§3) does, and stays
  compact and greppable — chosen over a nested `{name, version}` object for the same
  reasons.
- **Minor/patch sub-versions in the schema-version field** (`@1.2.0`). **Rejected:**
  *within* a version evolution is **additive-only** and readers **ignore unknown /
  default missing** (T0.10), so there is nothing to negotiate at a sub-version
  granularity — a new optional field needs no version signal at all. A single major
  integer, bumped only on a non-additive (compatibility-breaking) change, is the
  honest signal; a minor component would imply a negotiation the additive rule
  makes unnecessary.
- **Setting `"additionalProperties": false` on the schemas** (strict, closed-world
  validation). **Rejected:** it would make the schema **forbid the very unknown
  fields the reader is required to ignore** (T0.10), breaking additive evolution —
  a future minor version's new field would fail validation against the current
  schema. The open-world default of draft 2020-12 (§4) is deliberately kept.
- **A canonical-JSON RFC (JCS / RFC 8785) dependency** for the canonicalization.
  **Rejected as a dependency, adopted in spirit:** the canonicalization this ADR
  needs (sorted keys, integer numbers, compact, minimal escaping — §6) is a small,
  fixed rule set the writer applies directly with `serde_json`, adding **no new
  core-crate dependency**. Since dagr's canonical artifacts carry **no floats**,
  the hardest part of JCS (I-JSON number canonicalization) does not arise, so
  pulling a canonicalization crate would add supply-chain surface for a rule set
  the writer already satisfies. (If a float ever became unavoidable, the reopen
  condition above applies.)
- **`valico` or `boon` instead of `jsonschema`** as the validator. **Rejected on
  currency and adoption:** `jsonschema` is the most widely used pure-Rust
  validator, tracks the current 2020-12 draft, compiles-once/validates-many, and
  reports locatable errors — the properties C20/C22 need. The alternatives are
  either less maintained or target older drafts; `jsonschema` (CI-scoped, default
  features trimmed — §4) is the minimal-surface choice that still satisfies the
  criterion. (If it proves unable to validate a required shape, the reopen
  condition applies.)

*(Reopen condition stated in §Consequences: if a downstream ticket cannot honor a
seam as written, the contract reopens here rather than being worked around
locally.)*
