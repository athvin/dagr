# 118 · T103 — the `Payload` codec trait and derive

> **Milestone:** M10 · **Size:** M · **Type:** feature · **Components:** C1, C10
> **Branch:** `feat/t103-payload-trait-and-derive` · **Depends on:** T101 · **Blocks:** T104

## Why / context

Today a task's output reaches its consumer as a typed Rust value inside an
`Arc<Slot<T>>` — nothing is serialized, and the only bounds are
`Send + Sync + 'static` (plus `Clone` on the `RunnableFlow` path). Crossing a pod
boundary needs bytes, and ADR 115 §8 decided **how**: a `Payload` trait in
`dagr-core` with a build-time derive, and **no serde**, because `dagr-core`'s runtime
dependency set is empty and additions to it are reviewed as API decisions
(`arch.md` "Stability").

The design point that makes this worth its own ticket is **where the error lands**.
dagr's authoring promise is that mis-wiring is a *compile* error, not a runtime
surprise (system-level criterion 2). Remote-eligibility must behave the same way: a
node placed on remote compute whose input or output cannot be encoded is a compile
error at the registration site, never a run that fails at hour three. So `Payload`
is a bound the remote registration path requires, not a runtime downcast that might
fail.

`Payload` extends `StableName` deliberately. `StableName` already gives every payload
type an author-declared identity that the graph artifact records and the fingerprints
hash, and never uses `std::any::type_name` (ADR 013). That name is exactly what a
decoder needs to refuse a shard encoded from a different type after a refactor —
the encoded form carries the author-declared name, and a mismatch is a classified
error rather than a misinterpreted byte string.

This ticket ships the codec and its derive only. Nothing writes bytes anywhere yet:
the blob port is T104 and the pod-side writer is T106. What lands here is testable in
isolation and independently useful — a round-trip property test needs no cluster.

## Objective

Add the codec contract, its derive, and the local round-trip machinery.

- Add a **`Payload` trait to `dagr-core`** (`encode` into a caller-supplied buffer,
  `decode` from bytes), supertrait `StableName`, with a classified `CodecError` that
  distinguishes a malformed encoding from a **type-identity mismatch** from a
  version/format mismatch. `dagr-core` gains **no runtime dependency**.
- Add **`#[derive(Payload)]` to `dagr-macros`** (build-time only, like `#[task]` and
  `#[derive(StableName)]`), supporting structs and enums over field types that are
  themselves `Payload`, and emitting a spanned `compile_error!` for shapes it cannot
  handle rather than generating something subtly wrong.
- Provide `Payload` implementations for the primitives and standard containers a
  payload realistically uses (integers, `bool`, `String`, `Option`, `Vec`,
  `BTreeMap`, tuples, `()`), so the derive has a base to build on.
- The encoded form is **self-describing enough to refuse the wrong type**: it carries
  the `STABLE_NAME` and a format version, and `decode` fails with a classified error
  on a mismatch.
- Add the **`--dagr.force-roundtrip`** operator toggle (flag > env > default off, via
  the established `config.rs` `resolve` precedence) which makes the *local* executor
  encode and decode every `Payload`-bounded handoff, so codec bugs are catchable
  without a cluster. Default off means the in-memory fast path is untouched.
- Re-export `Payload` and its derive from the `dagr-cli` prelude alongside `task` and
  `StableName`.

## Test plan (write these first — TDD)

**Round-trip**
- Given a derived `Payload` over each supported field shape (unit struct, tuple
  struct, named struct, enum with and without data, nested `Payload`, `Option`,
  `Vec`, `BTreeMap`, tuple), when a value is encoded and decoded, then the result
  equals the original.
- Given a value encoded twice, then the two byte strings are identical
  (deterministic encoding — required for content addressing in T104).
- Given a `BTreeMap` built by inserting in two different orders, then both encode
  identically (canonical ordering, matching the artifact layer's posture).

**Refusal, not misinterpretation**
- Given bytes encoded from type `A`, when decoded as type `B`, then `decode` returns
  a **type-identity mismatch** `CodecError` naming both stable names — never a
  successfully-decoded wrong value.
- Given truncated bytes, given trailing garbage, and given a bumped format version,
  then each yields its own classified `CodecError` variant.
- Given a `CodecError`, then its `Display` names what was expected and what was
  found, and its source chain is intact.

**Derive diagnostics**
- Given `#[derive(Payload)]` on a shape the derive cannot handle, then a `trybuild`
  compile-fail fixture shows a spanned, actionable error rather than a downstream
  trait-bound wall of text.
- Given a struct with a field that is not `Payload`, then the error names the field
  and the missing bound.

**Zero-dependency and zero-cost**
- Given `cargo tree -p dagr-core -e normal --no-default-features`, then the runtime
  dependency set is still **empty**.
- Given a pipeline whose payloads implement `Payload` but which runs locally with the
  toggle off, then its event stream is byte-identical to before this change and the
  handoff still moves the value in memory (no encode call).

**The toggle**
- Given `--dagr.force-roundtrip`, then every `Payload`-bounded handoff encodes and
  decodes locally and the run still succeeds with identical terminal states.
- Given the flag and the env var disagree, then the flag wins (`flag > env >
  default`), and a malformed value fails loudly with the variable and value named.

## Definition of done

- [ ] `Payload` exists in `dagr-core` with `StableName` as a supertrait and a
      classified `CodecError`; `dagr-core`'s runtime dependency set is still empty.
- [ ] `#[derive(Payload)]` in `dagr-macros` covers structs and enums, with
      `trybuild` fixtures for every rejected shape.
- [ ] Primitives and standard containers implement `Payload`; encoding is
      deterministic and canonically ordered.
- [ ] Decoding bytes from the wrong type is a classified error naming both stable
      names; truncation, trailing garbage, and version mismatch are distinct
      variants.
- [ ] `--dagr.force-roundtrip` follows `flag > env > default` (default off); off
      means byte-identical streams and no encode call on the local path.
- [ ] `Payload` and its derive are re-exported from the `dagr-cli` prelude.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Encoding format.** ADR 115 fixes that dagr owns a codec and that `dagr-core`
  takes no dependency; it deliberately does not name a byte format. A simple
  length-prefixed canonical encoding is the default choice, and the decision is
  recorded in-PR with its round-trip and determinism tests as the evidence. It is
  **not** a public wire contract in this milestone — the same binary encodes and
  decodes (ADR 115 §2) — so it can change while the version tag moves.

## Out of scope

- Writing bytes anywhere: the `BlobStore` port and the local backend are **T104**,
  and the blanket `DurableOutput` bridge lands there too.
- The pod-side writer and shard format — **T106**.
- Making `Payload` a *requirement* anywhere: the remote-eligibility bound is applied
  at the remote registration path in **T105**/**T108**. Local pipelines are
  unaffected by this ticket.
- Any serde integration or interoperability with a foreign process — foreign-image
  tasks are named future work in ADR 115, and only then does the encoding become a
  public contract.
- Scope boundary restated: a codec adds no coordination and no server; dagr remains
  not a scheduler, a distributed execution system, a coordinating metadata store, a
  web interface, a DSL, or a backfill orchestrator, and the graph's shape never
  changes at runtime.
