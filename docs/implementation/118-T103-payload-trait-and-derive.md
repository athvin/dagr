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

---

## Decisions recorded

Every open question this ticket carried, resolved with its evidence. (`docs/tasks.md`
stops at T70 — the M9–M11 tickets were appended under `docs/implementation/` only —
so this ticket has **no** additional `Q:` items to answer; checked, not assumed.)

### 1. The encoding format (the ticket's own open question)

**Decided: a length-prefixed, fixed-width, canonically-ordered encoding of dagr's
own, with a self-describing envelope.**

- Envelope: a 4-byte magic (`dgrP`), the format version as a little-endian `u16`
  (`FORMAT_VERSION = 1`), the author-declared stable name as a `u64` length prefix
  plus its UTF-8 bytes, then the body.
- Body: integers fixed-width little-endian (`usize`/`isize` travel as 64-bit, so the
  bytes never depend on the encoder's word size); `bool` one byte, `0` or `1`;
  lengths and counts `u64`; `String`/`Vec`/`BTreeMap` a count followed by elements;
  `Option` a one-byte tag; an enum variant its **declaration index** as a
  little-endian `u32`; a nested payload contributes its **body only**.
- Canonical in both directions: a `BTreeMap` encodes in ascending key order (its own
  iteration order, so insertion order cannot reach the bytes) and a decoder
  **refuses** a non-ascending or duplicate-keyed encoding — otherwise one value would
  have two byte strings, which is exactly what content addressing (T104) cannot
  tolerate.

Evidence: `crates/core/tests/payload_codec.rs` — round trip over every shape,
`encoding_the_same_value_twice_is_byte_identical`,
`a_map_encodes_canonically_whatever_the_insertion_order`, and the four refusal tests.
It is **not** a public wire contract (ADR 115 §2: the same binary encodes and
decodes), so it may change while the version tag moves; the decoder already refuses
any other version as its own error class.

### 2. Two traits, not one: `Codec` (body) + `Payload = Codec + StableName`

The envelope must be written **once, at the top** — a nested payload that re-declared
its own name would bloat every composite and record identity in the wrong place — and
it must not be overridable per type, or the refusal guarantee is only as good as the
least careful `impl`. So the derive emits the **body** codec (`Codec`) and `Payload`
is a blanket `impl<T: Codec + StableName>` carrying the envelope as provided methods.
`Payload`'s supertrait is `StableName`, exactly as the ticket requires.

### 3. Tuples implement `Codec` but **not** `Payload`

`Payload` requires `StableName`, and `impl<A: StableName, B: StableName> StableName
for (A, B)` **does not compile** in `dagr-core`: it collides (`E0119`) with the merged
blanket `impl<T: StableName> StableInputNames for T` in `stable_name.rs`, which is the
multi-input naming decision this ticket does not own. Verified against `rustc`, not
assumed.

That constraint agrees with the design rather than fighting it: dagr binds inputs
**positionally**, so an N-input node's edges carry N separately-named values, and ADR
115 §9 records "the ordered, positional list of `{uri, content_hash}` references" —
one per input, never one encoded tuple. A tuple is therefore composite *data* inside a
payload (a derived struct's field), which is precisely what `Codec` describes. `()`
**is** a `Payload`: it already carries the reserved unit stable-name sentinel, so an
effect-only node's output type is named like any other. Tuple round trips are covered
at the body level in `primitives_and_containers_round_trip`.

### 4. `#[derive(Payload)]` emits `Codec` only; a payload type derives both

`#[derive(StableName, Payload)]` is the authoring line. Emitting `StableName` from the
`Payload` derive as well would collide (`E0119`) with the existing
`#[derive(StableName)]` the moment an author writes both — the same collision the
`StableName` corpus already pins — and would silently take the naming decision away
from the author. Instead `Codec` carries a
`#[diagnostic::on_unimplemented]` note naming the fix, and the derive emits each
field's code at the **field's own span**, so a field without a codec produces an error
pointing at that field (`crates/macros/tests/expand/fail/payload_field_not_payload.stderr`).

### 5. Floating point is deliberately absent

No `f32`/`f64` codec ships. A determinism claim over floats would be a lie without a
normalization rule for `NaN` payloads and signed zero, the ticket's container list does
not name them, and an author who needs one today encodes `f64::to_bits` in a named
payload struct — an explicit choice rather than a silent one.

### 6. Where the local round trip applies, and how the toggle reaches it

The round trip happens **at the producer's handoff**, in a guard wrapped around a
payload-bounded node's task: the produced value is encoded and decoded before it
reaches the slot, which is the single point every consumer's input passes through — so
one round trip per produced value covers both ends of every handoff.

The toggle is a shared `Arc<AtomicBool>` on the `RunnableFlow`, read per attempt,
rather than a registration-time decision, because `dagr run <flow>` builds the flow
through a factory that never sees the invocation's argv: the operator's answer
necessarily arrives *after* registration. Off (the default) costs one relaxed load and
returns the value untouched — no encode call, nothing allocated, no record emitted —
and ordinary registrations carry no guard at all. Proved by the counting-codec test
(`with_the_toggle_off_no_handoff_is_ever_encoded`) and the three-way stream comparison
(`the_event_stream_is_identical_plain_off_and_on`).

### 7. A codec failure is **permanent**, not retry-eligible

A deterministic encoder that failed once fails identically on the next attempt, so the
guard returns `TaskError::permanent_from(…, CodecError)` — the classified error in the
message and preserved as the error's source. Retrying would only turn a codec defect
into a slower codec defect.

### 8. What the event stream carries about a codec failure

Nothing new. The attempt record's optional `message`/`error` fields are not populated
by the run-a-Flow attempt path today, and populating them would change the stream for
**every** failing node — which several M10 tickets' byte-identical-stream assertions
depend on. The codec fault is observable exactly where this ticket's DoD asks: the
node's terminal state (`failed`) and the run outcome.
