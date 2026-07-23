# 018 · T5 — Design spike: typed handle and dependency encoding

> **Milestone:** M0 · **Size:** M · **Type:** decision (spike) · **Components:** C2, C3
> **Branch:** `adr/t5-typed-handle-encoding-spike` · **Depends on:** T1, T0.2 · **Blocks:** T10

## Why / context
The authoring surface's entire pitch is compile-time confidence, and C2 (Handle) and C3 (Data dependency) are where that confidence is won or lost. Before the real handle and binding APIs are implemented (T10, T11), we need a throwaway prototype that proves the chosen encoding actually makes wrong-type, wrong-arity, and cyclic constructions *fail to compile* — not fail at assembly, and not fail with an unreadable wall of trait errors. This ticket locks three decisions that later tickets bake in: the exact input-arity ceiling, that single-input tasks take `T` rather than a one-tuple, and that the `all-succeeded`-only restriction on data-dependent nodes is carried by builder typestate rather than a runtime check. It builds on T1 (the compiling workspace skeleton this spike lives in) and T0.2 (the ownership/sharing model that establishes the author-visible type bounds a handle's value must carry). Governing spec: `arch.md` §C2, §C3, and the Vocabulary trigger-rule paragraph that restricts non-default rules to consume-nothing nodes.

## Objective
Produce a committed ADR plus a throwaway prototype (kept as compile-fail/UI fixtures, not shipped API) that demonstrates the handle-and-binding encoding satisfies C2 and C3 at compile time, and records the three open decisions as resolved. Concretely:

- Prototype a handle representation that carries node identity plus the value's type, is freely copyable, and can only be produced by registering a node — with no escape hatch to fabricate one or to look a node's output up by name, index, or string key.
- Prove structurally (via the backward-reference registration discipline shared with C4 ordering edges) that a cycle cannot be *expressed*, so cycle rejection needs no later validation pass.
- Prototype the binding surface for one and for multiple handles, proving exact value-type matching and exact arity matching are compile errors when violated, with error text that names both the expected and the supplied type.
- Fix the maximum input arity (working assumption: 8) and place a curated `#[diagnostic::on_unimplemented]` diagnostic at the cliff so that crossing it yields one readable message rather than a trait-error cascade.
- Confirm the ergonomics that a single-input task consumes `T` directly, never a one-tuple `(T,)`.
- Prototype the builder typestate that makes any trigger rule other than `all-succeeded` *inexpressible* on a node that carries data dependencies, so the restriction is a compile error rather than a runtime check.
- Record every resolution in the ADR: the arity number and its rationale, the single-input-`T` ergonomics call, the typestate approach, and the pinned-toolchain dependency of the UI/compile-fail fixtures (C28).

## Test plan (write these first — TDD)
Because this is a spike, the "tests" are the prototype's compile-pass and compile-fail evidence plus the ADR decision-record checks. Each scenario is independently checkable against the prototype crate and the pinned workspace toolchain (C28).

- **Handles are freely copyable.** Setup: a prototype pipeline registers two nodes and holds their handles. Action: copy each handle, pass copies into and out of a helper, and use the original again afterward. Expected: the prototype compiles and both the original and the copies remain usable — no move error, no borrow error.

- **No handle without registration.** Setup: the prototype exposes the handle type. Action: attempt, in a compile-fail fixture, to construct or obtain a handle for a node that was never registered (directly, via a public constructor, or via any type-name/index/string lookup). Expected: it fails to compile; the fixture is checked in as evidence that no such API exists.

- **No output lookup by key.** Setup: a registered node with an output. Action: search the prototype's surface for any way to retrieve that output by node name, positional index, or string key. Expected: none exists; a compile-fail fixture asserting such a lookup does not compile is checked in.

- **Cycle is inexpressible (data edge).** Setup: the prototype's registration API only accepts handles of already-registered upstreams. Action: attempt to bind node B's handle as an input to node A when A was registered before B. Expected: the handle for B does not yet exist at A's registration, so the construction fails to compile — a checked-in compile-fail fixture, not a later validation pass.

- **Cycle is inexpressible (ordering edge).** Setup: same backward-reference discipline extended to ordering edges (the C4 mechanism from T0.9). Action: attempt to add an ordering edge that would close a loop between two nodes. Expected: fails to compile; checked-in fixture. (This spike only proves the *shape* enforces it; C4's full implementation is T50.)

- **Wrong-type binding is a compile error naming both types.** Setup: a task in the prototype declares it consumes a value of type `Alpha`. Action: in a UI fixture, bind a handle whose value type is `Beta`. Expected: compilation fails and the captured error message contains the string forms of both `Alpha` and `Beta`; the assertion checks only that both type names appear, not prose quality, and runs against the pinned workspace toolchain.

- **Wrong-arity binding is a compile error.** Setup: a task declares it consumes exactly two inputs. Action: bind one handle, then in a separate fixture bind three. Expected: both fail to compile; checked-in fixtures.

- **Single-input ergonomics take `T`, not `(T,)`.** Setup: a task that consumes a single value of type `Gamma`. Action: bind exactly one handle whose value type is `Gamma`, with no tuple wrapping at any call site. Expected: the prototype compiles; a companion fixture that wraps the single input as `(Gamma,)` is documented as the rejected/unnecessary form, confirming the ergonomics decision.

- **Arity cliff produces the curated diagnostic.** Setup: the maximum arity is fixed (working assumption 8) and a `#[diagnostic::on_unimplemented]` message is attached at the cliff. Action: in a UI fixture, attempt to bind one more handle than the ceiling allows. Expected: compilation fails and the emitted diagnostic is the single curated message pointing at the ceiling and the "aggregate into a struct produced by an intermediate node" remedy — not a wall of trait-bound errors. The ADR records the chosen ceiling number.

- **Fan-out: one handle, many consumers, compiles.** Setup: one registered producer handle. Action: bind that same handle as input to several downstream tasks whose declared input type matches. Expected: the prototype compiles; the handle is reused freely across all bindings.

- **Non-default rule on a data-dependent node is inexpressible.** Setup: the builder typestate from the prototype. Action: in a compile-fail fixture, register a node that carries at least one data dependency and attempt to set its trigger rule to `all-terminal` or `any-failed`. Expected: it fails to compile because the typestate offers no such method in that state — a compile error, not a runtime check; checked-in fixture.

- **Default-rule data node still assembles.** Setup: a data-dependent node with no explicitly stated rule. Action: build the prototype pipeline. Expected: it compiles and behaves as `all-succeeded`, confirming the restriction constrains only the *non-default* rules.

- **ADR decision-record completeness.** Setup: the committed ADR. Action: read it. Expected: it states the resolved arity ceiling and its rationale, the single-input-`T` ergonomics decision, the typestate mechanism for the trigger-rule restriction, the reliance on `#[diagnostic::on_unimplemented]` at the cliff, and the note that the compile-fail/UI fixtures are pinned to the workspace toolchain and regenerated deliberately on a toolchain bump (C28).

## Definition of done
- [ ] The prototype's handle carries node identity plus the value's type and is freely copyable and passable during construction (C2).
- [ ] No API in the prototype produces a handle for a node that has not been registered (C2).
- [ ] No API in the prototype retrieves a node's output by name, index, or string key (C2).
- [ ] A cycle — via data edges or ordering edges — fails to compile in the prototype, demonstrated by checked-in compile-fail fixtures, and the guarantee is structural (backward-reference registration) rather than a later validation pass (C2).
- [ ] The prototype demonstrates that renaming a node changes its identity while reordering registrations changes nothing (C2) — recorded as the identity-from-name decision feeding T10/T13.
- [ ] Binding a handle of the wrong value type is a compile error whose captured message contains both the expected and the supplied type names, verified by a UI test against the pinned workspace toolchain (C3, C28).
- [ ] Binding a different number of handles than the task declares is a compile error, with a checked-in fixture (C3).
- [ ] A node with data dependencies cannot be given any trigger rule other than `all-succeeded`; the builder typestate makes it inexpressible (a compile error, not a runtime check), with a checked-in fixture (C3).
- [ ] One handle can be bound to any number of downstream tasks, demonstrated by a compile-pass fan-out case (C3).
- [ ] The maximum input arity is fixed and documented; exceeding it produces a single curated `#[diagnostic::on_unimplemented]` message pointing at the ceiling and the struct-aggregation remedy (C3).
- [ ] The single-input ergonomics decision — tasks consume `T`, not `(T,)` — is confirmed by a compile-pass case and recorded in the ADR (resolves the open question).
- [ ] The exact arity ceiling (working assumption 8) is chosen during the spike and recorded in the ADR with rationale (resolves the open question).
- [ ] A committed ADR captures every resolution above and notes the pinned-toolchain dependency of the compile-fail/UI fixtures and their deliberate regeneration on toolchain bumps (C28).
- [ ] The prototype is clearly marked throwaway — it does not ship as the real C2/C3 API; its lasting outputs are the ADR and the fixtures that T8/T10/T11/T12 will adopt.
- [ ] The ticket stays inside scope: no runtime binding, no assembly-time checks, no ordering-edge mechanics implementation beyond the cycle-shape proof.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Exact arity ceiling — 8 is the working assumption; the final number is picked during the spike and recorded in the ADR.
- Single-input tasks take `T`, not a one-tuple `(T,)` — confirm the ergonomics during the spike and record the decision.

## Out of scope
- The shipping C2/C3 implementations — typed handles land in T10 and the real data-dependency binding (exact type matching, tuple arities, fan-out, ownership model) in T11; this ticket only proves the encoding and locks the decisions.
- The full compile-failure test harness (T8) and the criteria coverage matrix / CI UI-test policy (T7) — this spike produces fixtures those tickets formalize, not the harness itself.
- Ordering-edge mechanics (C4): the API shape decision is T0.9 and the implementation is T50; here we only prove the backward-reference discipline forbids cycles across ordering edges too.
- The ownership/receive-mode model (owned vs shared vs clone-on-read) and all assembly-time mode-conflict checks — settled in T0.2 and enforced at assembly (C1/C3), never part of the compile-time type matching this spike covers.
- Node policy, execution-class overrides, groups, fingerprint/policy-hash composition, and any runtime behavior — adjacent components (C5, C6, C13, C21) that this spike must not pull in.
- Anything approaching a runtime-mutable graph, a lookup registry, a name-keyed API, or a DSL for wiring — all barred by the permanent scope boundary; the whole point is that wiring is ordinary typed Rust and the graph shape is fixed at compile time.

---

# ADR: typed handle and dependency encoding (C2, C3)

> The repo keeps each ADR inside its own implementation-ticket file (the T1, T2,
> T0.2–T0.9 ADRs all embed the ADR at the ticket's own path). This ADR is
> committed here, at
> `docs/implementation/018-T5-typed-handle-encoding-spike.md`, the ADR location
> for ticket T5 — satisfying the DoD literally, with zero deviation, and linked
> from the tasks/spec index (`docs/tasks.md` T5 entry and
> `docs/implementation/README.md`) so its consumers T10, T11, and T12 find it.
> Its mechanical acceptance gate is
> [`scripts/check-typed-handle-encoding-adr.sh`](../../scripts/check-typed-handle-encoding-adr.sh),
> which cross-references this ADR against arch.md (C2, C3, C4, C28,
> "Vocabulary") so every seam element appears with no invention and no omission.
> The skeletal compile-fail fixtures it names live at
> [`crates/core/tests/ui/typed_handle_wrong_arity.rs`](../../crates/core/tests/ui/typed_handle_wrong_arity.rs),
> [`typed_handle_unforgeable.rs`](../../crates/core/tests/ui/typed_handle_unforgeable.rs),
> [`typed_handle_data_cycle.rs`](../../crates/core/tests/ui/typed_handle_data_cycle.rs),
> and
> [`typed_handle_non_default_rule_on_data_node.rs`](../../crates/core/tests/ui/typed_handle_non_default_rule_on_data_node.rs)
> (each with a sibling `.stderr`), reusing the already-shipped T8 wrong-type seed
> [`wrong_type_binding.rs`](../../crates/core/tests/ui/wrong_type_binding.rs) as
> the C3 wrong-type case; the positive (compiles) fixtures live at
> [`crates/core/tests/typed_handle_positive.rs`](../../crates/core/tests/typed_handle_positive.rs).

## Status

Accepted (2026-07-23). This is a **design spike** that RESOLVES and RECORDS the
**typed-handle representation** (C2) and the **dependency/binding encoding** (C3)
— how a `Handle<T>` carries node identity plus the value type and stays freely
copyable; how a data dependency is encoded so wrong-type, wrong-arity, and
cyclic wiring is a **compile** error; how identity is preserved; and the
type-erasure strategy for the graph — plus the three open decisions (the exact
input-arity **ceiling = 8**, that a single-input task consumes **`T`, not
`(T,)`**, and that the `all-succeeded`-only restriction on data-dependent nodes
is carried by **builder typestate**). Every decision below is backed by
**EVIDENCE from a throwaway spike**: real `rustc` diagnostics (their error codes
quoted verbatim) captured under the pinned toolchain (`rustc 1.95.0`, per
`rust-toolchain.toml`).

It ships **no production code**: the shipping crates (`core`, `artifact`,
`render`, `cli`) carry **no library change** (`crates/*/src` is unchanged),
`Cargo.lock` is untouched, and the only committed artifacts are this ADR, the
mechanical acceptance script named above, and the **test-only** skeletal fixtures
under `crates/core/tests/` (wired to the already-shipped T8 UI harness,
`crates/core/tests/ui.rs`, with **no harness change**). The **real** typed handle
is **IMPLEMENTED by T10**, the real data-dependency binding (exact type matching,
tuple arities, fan-out, ownership model) by **T11**, and the full wiring
compile-failure suite by **T12** — this ticket only *decides* the encoding, locks
the three open decisions, and delivers the fixtures those tickets adopt.

**Spike disposition.** A throwaway prototype was built **outside the workspace**
(under `/tmp/dagr-t5-spike`, never inside the repo) purely to *validate* the
encoding compiles/compile-fails as specified under the pinned toolchain, and was
**DELETED** before this ticket finished. No shipping crate was touched and the
tree is clean. The `Handle`/`Flow`/`Deps` sketches below are **illustrations of
the settled contract, not shipping code**.

**Open questions.** The ticket's `## Open questions` names two, and the
`docs/tasks.md` T5 entry carries the same two `Q:` items. Both are **resolved and
recorded** here: **(1)** the exact input-arity ceiling is **8** (§4); **(2)** a
single-input task consumes **`T`, not a one-tuple `(T,)`** (§5). No question
remains open.

**Consistency with the already-merged M0 ADRs is load-bearing** and holds:

- **T1** (003 — crate layout) fixed the four-crate workspace (`core`, `artifact`,
  `render`, `cli`), the pinned toolchain (1.95.0), and `[workspace.lints]`. This
  spike's fixtures live under `crates/core/tests/` and change no crate boundary,
  no dependency edge, and no `src`.
- **T0.2** (008 — output ownership) fixed that a task declares the **type** it
  consumes (checked at **compile** time, C3) while the **receive mode** (owned vs
  shared vs clone-on-read) is a whole-graph fact checked at **assembly** (C1).
  This ADR keeps that partition untouched: the handle/binding encoding here
  matches **only the value type** at compile time — the mode is **not** part of
  the type match (§2, §3) and is deliberately out of this spike's scope. T0.2's
  author-visible bounds (task `Send + 'static` with `&mut self`; output `Send +
  Sync + 'static`) are the bounds the real `Handle<T>`/binding (T10/T11) will
  carry; the spike elides them so its compile-error evidence is about
  arity/type, not bounds (noted in the fixtures).
- **T0.7** (013 — stable name + fingerprint) fixed that node **identity comes
  from the author-declared stable name, never `std::any::type_name`**, that a
  **rename changes identity** and a **reorder changes nothing**. This ADR reuses
  that exactly: the handle carries the node's **identity** (from the name) plus
  the value's **type**; §1 records the identity-from-name decision that feeds
  T10 (handle) and T13 (builder / node identity).
- **T0.9** (015 — ordering-edge mechanics) fixed the registration-time
  backward-reference discipline for ordering edges and the type-erased ordering
  upstream. This ADR shares that discipline for **data** edges (§6 cycle
  argument) and reuses the type-erased ordering view (§2); the ordering-edge
  *cycle* fixtures are T0.9's, the **data**-edge cycle fixture is this ticket's.

There is **no supersession** and **no spec conflict**: every clause below is a
direct reading of arch.md **C2**, **C3**, and the **Vocabulary**, consistent with
T1 / T0.2 / T0.7 / T0.9.

## Context

`docs/arch.md` fixes the handle (**C2**) and data dependency (**C3**) as the two
components where the authoring surface's compile-time-confidence pitch is won or
lost, and it does so **before** the real handle (T10, M1), the real binding (T11,
M1), and the wiring compile-fail suite (T12, M1) are written — so the *encoding*
that all three bind against must be fixed once, now, in M0:

- **C2 · Handle.** "The handle carries the node's identity and the type of the
  value it will eventually hold. Handles are cheap and freely copyable. A handle
  is the *only* way to refer to another task's output — there is no lookup by
  name, index, or string key." "Because a handle can only be obtained by
  registering a node, and a node can only depend on handles that already exist —
  this holds for ordering edges too (C4) — a cycle cannot be expressed. This is
  structural, not a validation pass that runs later." Acceptance: handles copy
  and pass freely; **no API** obtains a handle for an unregistered node; **no
  API** retrieves an output by name/index/string key; a cycle (data **or**
  ordering) **fails to compile**, demonstrated by a checked-in compile-failure
  test; **renaming** a node changes its identity while **reordering** registrations
  changes nothing.
- **C3 · Data dependency.** "The *value types* of the bound handles must exactly
  match the consuming task's declared input types; a mismatch is a compile
  error." "Multiple inputs bind as a tuple, up to a documented maximum arity; at
  the cliff, a curated diagnostic message says so rather than a wall of trait
  errors." Acceptance: wrong-type binding is a compile error **naming both** the
  expected and supplied type; wrong-**arity** is a compile error; a data-dependent
  node **cannot** be given any trigger rule other than `all-succeeded` — the
  **builder's typestate makes it inexpressible**, a compile error rather than a
  runtime check; **one handle** can be bound to **any number** of downstream
  tasks; the maximum arity is documented and exceeding it produces the curated
  message.
- **Vocabulary.** "Data-dependent nodes always use `all-succeeded` (C3), and
  that restriction is enforced at compile time." The closed trigger-rule set is
  `all-succeeded` (default), `all-terminal`, `any-failed`.
- **C28 · Testing surface.** "compile-fail and error-message tests are
  library-internal, **pinned to the workspace toolchain**, asserting only that
  both type names appear in the message; toolchain bumps regenerate those
  fixtures deliberately." The T8 harness (007) is that surface; T12 reuses it,
  and this ticket's fixtures drop into it with no harness change.

Landing this wrong means T10, T11, and T12 disagree about what a handle *is*,
whether a mis-wiring is a compile error or a late check, and what the arity
ceiling is — so the encoding below is fixed once, with reproducible evidence.

### Prior art mined (dagx §1)

The dagx prototype (routed to T5) demonstrated the exact trio this ADR adopts: a
**typed opaque handle** (`u32` id + `PhantomData<fn() -> T>` for
variance/auto-trait safety), a **sealed positional binding** (a private trait
mapping handle tuples to input tuples so count/order/type are all compile-checked,
macro-generated to arity 8), and a **type-state builder that consumes on wire**
so cycles are unrepresentable with **no runtime cycle detection**. dagx's arity
cap of 8 is the precedent this ADR ratifies. (Its `run(self)` task shape and
`Arc<dyn Any>` output erasure are dagx anti-patterns for dagr's runtime half and
are **out of scope** here — this ticket touches only the compile-time encoding.)

## Decision

### 1. The typed handle — identity + value type, unconditionally `Copy`

A handle is a small value carrying **node identity** plus the **value's type**:

```rust
#[derive(Clone, Copy)]
struct NodeId(u32);                 // opaque; assigned at registration

struct Handle<T> {
    id: NodeId,                     // node identity (from the stable NAME — T0.7)
    _t: PhantomData<fn() -> T>,     // the value's type, carried at compile time
}
impl<T> Clone for Handle<T> { fn clone(&self) -> Self { *self } }
impl<T> Copy  for Handle<T> {}      // hand-written: NOT `#[derive]` (see below)
```

- **Identity comes from the NAME, never from registration order** (C2, T0.7). The
  `NodeId` is an internal handle-equality token; the node's *identity* — what the
  structural fingerprint and resume key on — is the **author-declared stable
  name** (T0.7), so **renaming a node changes its identity** while **reordering
  registrations changes nothing**. This is the identity-from-name decision this
  ticket records for **T10** (which builds the real handle) and **T13** (which
  bakes node identity into the builder). The spike models identity with the
  `NodeId` token only; the name-derived identity is T0.7's contract, reused, not
  re-decided.
- **The phantom is `PhantomData<fn() -> T>`, not `PhantomData<T>`.** The
  fn-pointer phantom is covariant in `T` and owns no `T`, so `Handle<T>` is
  **`Copy + Send + Sync` regardless of `T`** — even for a `T` that is itself
  `!Send + !Sync + !Copy`. This is exactly C2's "handles are cheap and freely
  copyable." The naive `PhantomData<T>` would infect the handle with `T`'s
  auto-traits and is **rejected** (see Rejected alternatives, with the E0277
  evidence).
- **`Copy` is hand-written, not derived.** `#[derive(Copy)]` would emit `impl<T:
  Copy> Copy for Handle<T>`, wrongly requiring `T: Copy`. The manual impls make
  the handle `Copy` for **all** `T`.

**Evidence (spike, pinned toolchain 1.95.0).** With `T = Rc<String>` (deliberately
`!Send + !Sync + !Copy`), `assert_copy/send/sync::<Handle<Rc<String>>>()`
**compiles**. Swapping the phantom to `PhantomData<T>` fails:

```text
error[E0277]: `Rc<String>` cannot be sent between threads safely
error[E0277]: `Rc<String>` cannot be shared between threads safely
```

confirming the fn-pointer phantom is load-bearing. (Positive fixture
`handles_are_freely_copyable` in `typed_handle_positive.rs`.)

### 2. The handle is UNFORGEABLE, and there is NO lookup by name/index/key

A handle is obtainable **only** by registering a node (C2):

- **Private fields, no constructor, no `From`/`new`.** `Handle`'s fields are
  private to the crate; there is no public constructor and no escape hatch to
  fabricate one. The **only** value that produces a `Handle<T>` is a `register`
  (or `source`) call's return value.
- **No lookup by name, index, or string key.** There is deliberately no
  `Flow::get(name)`, `get(index)`, or `get(key)` — the **only** currency is a
  handle a registration already returned. This is the same no-string-lookup
  philosophy as C2's handle rule and C9's resource-by-type rule.

**Evidence (spike).** A struct-literal fabrication of a handle from outside the
defining module fails:

```text
error[E0451]: fields `id` and `_t` of struct `Handle` are private
```

(Compile-fail fixture `typed_handle_unforgeable.rs`; snapshot substrings
`E0451`, `private`, `Handle`.)

### 3. The dependency/binding encoding — a sealed positional `Deps` trait

A data dependency is **binding one or more handles to a task** whose **value
types must exactly match** the task's declared input types (C3). The encoding is
a **sealed positional binding trait** that maps a handle tuple to the task's
input tuple, so **count, order, and types are all compile-checked at once**:

```rust
trait Deps {                        // sealed (crate-private); appears in a public
    type Inputs;                    // signature under `#[allow(private_bounds)]`
    fn ids(&self) -> Vec<NodeId>;
}
impl<A>       Deps for Handle<A>                       { type Inputs = A;      /*..*/ }
impl<A, B>    Deps for (Handle<A>, Handle<B>)          { type Inputs = (A, B); /*..*/ }
// … macro-generated through arity 8 in the real T11 impl …

fn register<T, D>(&mut self, task: T, deps: D) -> Handle<T::Output>
where T: Task, D: Deps<Inputs = T::Input>       // the exact-match bound
```

The `Inputs = T::Input` bound is where the compile-time check lives: **a
wrong-type or wrong-arity `deps` argument cannot satisfy it.** Because `register`
takes `deps` **by value** and returns `Handle<T::Output>`, and `Deps` is
implemented only for tuples of **existing** handles, this is the single choke
point that also delivers the cycle guarantee (§6). **Receive mode is NOT part of
this match** — owned vs shared vs clone-on-read is a whole-graph fact settled by
T0.2 and checked at **assembly** (C1), never here (this keeps T0.2's
type-at-compile-time / mode-at-assembly partition intact).

**Wrong-VALUE-TYPE evidence (spike).** Binding `Handle<Beta>` where `Handle<Alpha>`
is required fails with a message that **names both types** (the C3/C28 contract).
Two forms were measured:

- Through the sealed `Deps` trait (uniform for all arities):

  ```text
  error[E0271]: type mismatch resolving `<Handle<Beta> as Deps>::Inputs == Alpha`
  ```

  The first line names **both** `Beta` (supplied) and `Alpha` (expected); the
  diagnostic body repeats `expected this to be `Alpha`` and ``Deps::Inputs` is
  `Beta` here`.
- Through a direct single-input parameter `Handle<T::Input>` (arity-1 only):

  ```text
  error[E0308]: mismatched types
     = note: expected struct `Handle<Alpha>`
                found struct `Handle<Beta>`
  ```

Both satisfy C3 ("message contains both the expected and the supplied type
names"). The wrong-type **fixture reuses the already-shipped T8 seed**
`wrong_type_binding.rs` (whose snapshot names `ExpectedWidget`/`SuppliedGadget`),
so the C3 wrong-type case is already wired into the harness; T11/T12 will add the
real-API version.

**Wrong-ARITY evidence (spike).** Binding one handle where two are declared fails
as an unsatisfied associated-type bound:

```text
error[E0271]: type mismatch resolving `<Handle<Alpha> as Deps>::Inputs == (Alpha, Beta)`
```

(Compile-fail fixture `typed_handle_wrong_arity.rs`; snapshot substrings `E0271`,
`Handle<Alpha>`, `(Alpha, Beta)`.)

### 4. Arity ceiling = 8, with a curated `#[diagnostic::on_unimplemented]` at the cliff

**The maximum input arity is fixed at 8** (resolving the open question; ratifying
the dagx precedent). Rationale: 8 covers every realistic fan-in without an
unwieldy macro expansion or a combinatorial trait-impl blow-up; beyond it the
right answer is to **aggregate the upstream values into a struct produced by an
intermediate node** and depend on that one handle (C3). Crossing the ceiling must
yield **one readable message**, not a trait-error cascade, so a curated
`#[diagnostic::on_unimplemented]` sits on `Deps`:

```rust
#[diagnostic::on_unimplemented(
    message = "too many inputs bound to one task: the maximum input arity is 8",
    label   = "this binds more than 8 handles",
    note    = "aggregate the upstream values into a struct produced by an \
               intermediate node, then depend on that one handle"
)]
trait Deps { /* impls for arity 1..=8 only */ }
```

**Evidence (spike).** Binding a tuple past the (illustratively capped) arity
yields exactly **one** `E0277` whose message/label/note are the curated text — no
cascade:

```text
error[E0277]: too many inputs bound to one task: the maximum input arity is 8
   |  ... this binds more than 8 handles
   = note: aggregate the upstream values into a struct produced by an intermediate
           node, then depend on that one handle
```

(`grep -c 'error\['` over the diagnostic = **1**.) The real ceiling fixture lands
with T11/T12 against the arity-8 macro; the spike proved the mechanism at a lower
cap so the over-ceiling tuple has no impl to match.

### 5. Single-input ergonomics — a task consumes `T`, never `(T,)`

**A single-input task consumes `T` directly** (resolving the open question). The
arity-1 `Deps` impl is `impl<A> Deps for Handle<A> { type Inputs = A; }` — a
**bare handle** delivers the bare value `A`, so a task declaring `type Input =
Gamma` is bound with `register(task, g)`, **no tuple wrapping at any call site**.
The one-tuple form `(Gamma,)` is **rejected as unnecessary** noise (it would force
`register(task, (g,))` and a `Task<Input = (Gamma,)>` declaration for the common
case). Tuples begin at **arity 2**.

**Evidence (spike).** `register(ConsumeGamma, g)` with `ConsumeGamma::Input =
Gamma` **compiles** with a bare handle; the companion `(Gamma,)` form is
documented as the rejected/unnecessary shape. (Positive fixture
`single_input_takes_t_not_one_tuple`.)

### 6. Cycle inexpressibility — structural (C2), never a runtime/validation pass

A cycle is **inexpressible by construction**, by the backward-reference
registration discipline shared with C4 ordering edges (T0.9):

- **A handle is obtainable only by registering a node** (§2), and **`register`
  accepts only already-existing handles** (§3). Therefore **no expression can name
  a node that is not yet registered.**
- **A data-edge back-edge cannot be written.** To make node A depend on node B,
  A's registration must name B's handle; but if A is registered first, B's handle
  does **not exist yet** at A's registration point — it is a use of an undeclared
  binding. The cycle cannot be expressed; there is **no runtime cycle-detection
  pass** and none is needed.
- **The ordering-edge half** is proven by T0.9's fixtures
  (`ordering_edge_self_cycle`, `ordering_edge_back_edge`); this ticket adds the
  **data-edge** half.

This is **structural per C2** — a property of what expressions *can be written* —
**not a later validation pass** and **not a runtime cycle-detection pass**.

**Evidence (spike).** Registering A against B's handle before B exists fails:

```text
error[E0425]: cannot find value `b` in this scope
```

(Compile-fail fixture `typed_handle_data_cycle.rs`; snapshot substrings `E0425`,
`cannot find value`.)

### 7. Fan-out — one handle bound to any number of consumers

Because `Handle<T>` is `Copy` (§1), one producer handle is bound to **any number**
of downstream tasks by reuse; the value type matches at each binding (C3). No
special API is needed — the copyability *is* the fan-out.

**Evidence (spike).** The same `Handle<Gamma>` is bound to four consumers and
remains usable afterward — **compiles**. (Positive fixture
`fan_out_one_handle_many_consumers`.) The receive-mode consequences of fan-out
(a multiply-consumed value is delivered by shared read access, or the edge opts
into clone-on-read) are **T0.2's assembly-time concern**, not this compile-time
match.

### 8. Trigger-rule restriction by builder TYPESTATE, not a runtime check

A node that carries **any data dependency** cannot be given a trigger rule other
than `all-succeeded`; the builder's **typestate makes the non-default rules
inexpressible** — a **compile** error, not a runtime check (C3, Vocabulary). The
builder is parameterized by a consume-state marker:

```rust
struct ConsumesNothing;  struct ConsumesData;
struct Builder<S> { /* … */ _s: PhantomData<S> }

impl Builder<ConsumesNothing> {
    fn trigger_rule(self, r: TriggerRule) -> Self { /* only offered here */ }
    fn depends_on<T>(self, h: Handle<T>) -> Builder<ConsumesData> { /* transitions */ }
}
impl Builder<ConsumesData> {
    // deliberately NO `trigger_rule` — the restriction IS the absent method
}
```

`.trigger_rule(..)` exists **only** in the `ConsumesNothing` state; binding a data
dependency transitions the builder to `ConsumesData`, a state that offers no such
method. A **default-rule** data node (no `.trigger_rule` call) still assembles and
behaves as `all-succeeded` — the restriction constrains only the **non-default**
rules. The trigger-rule set used is the normative closed Vocabulary set
(`all-succeeded` default, `all-terminal`, `any-failed`). The real builder is
**T13**; this ADR fixes the typestate *shape* T13 and T11 implement.

**Evidence (spike).** Calling `.trigger_rule(AllTerminal)` on a data-dependent
node fails with "no method in this state":

```text
error[E0599]: no method named `trigger_rule` found for struct `Builder<ConsumesData>` in the current scope
```

(Compile-fail fixture `typed_handle_non_default_rule_on_data_node.rs`; snapshot
substrings `E0599`, `trigger_rule`, `ConsumesData`.)

### 9. Type-erasure strategy for the graph

The **authoring surface stays fully typed**: every wiring decision (identity,
value type, arity, trigger-rule eligibility) is checked at compile time via the
typed `Handle<T>` and the `Deps` bound above, with **no** `dyn Any` on the
authoring path. Type **erasure happens later and elsewhere**:

- An **ordering** upstream erases the value type immediately — a `Handle<T>`
  yields a type-erased `Ordering(NodeId)` (T0.9), so ordering edges constrain
  sequence, not data, and mix value types freely.
- The **execution-core** output-slot erasure (`Arc<dyn Any + Send + Sync>`,
  downcast only at typed edges, Arc-wrapped once for fan-out) is the dagx §3
  pattern, but it belongs to **C10 output slots (T17)** and the serialization
  boundary (T4/T0.6) — **out of scope here**. This ADR's only erasure statement
  is that the **authoring encoding stays typed**; where the runtime erases is not
  this ticket's decision.

### 10. Scope boundary (permanent non-goals)

The graph's shape is **fixed at compile time** — there is **no runtime-mutable
graph, no lookup registry, no name-keyed API, and no wiring DSL**; wiring is
ordinary typed Rust. The cycle guarantee is **structural and compile-time**, with
**no runtime cycle detection**. dagr is permanently **not a scheduler,
distributed execution system, metadata store, or DSL**. This ADR fixes a
**fixed-by-construction** handle/binding encoding and introduces no runtime knob
that would breach that boundary (see **Rejected alternatives**).

## Consequences

**Each blocked ticket inherits a named seam and reopens no question this ADR
closed:**

- **T10** (020 — C2 typed handles) consumes the **handle representation (§1)**:
  the `NodeId`-plus-`PhantomData<fn() -> T>` encoding, unconditional `Copy` via
  hand-written impls, the **unforgeable / no-lookup** discipline (§2), and the
  **identity-from-name** decision (§1, feeding T13). It builds the real handle;
  it re-decides none of the encoding.
- **T11** (021 — C3 typed data-dependency binding) consumes the **`Deps`
  encoding (§3)**, the **arity ceiling 8 + curated `on_unimplemented` (§4)**, the
  **single-input-`T` ergonomics (§5)**, and **fan-out (§7)**; it implements the
  real binding (exact type matching, tuple arities macro'd to 8, fan-out, and the
  T0.2 ownership model at assembly).
- **T12** (024 — compile-failure suite for wiring) consumes the **fixtures
  (§§2–8)** and the **assertion table below**: it authors the real-API versions
  of wrong-type, wrong-arity, unforgeable-handle, data-edge cycle, and
  non-default-rule-on-a-data-node cases (plus the ordering-edge cycle cases from
  T0.9), against the same T8 UI harness these skeletal fixtures already use.
- **T13** (023 — flow builder & node identity) consumes the **typestate shape
  (§8)** and the **identity-from-name** decision (§1).

### Compile-fail / positive fixture table (for T12)

Each compile-fail case is a `crates/core/tests/ui/<case>.rs` sample with a
sibling `.stderr` substring snapshot, run by the T8 UI harness pinned to the
workspace toolchain (C28); the harness asserts the sample **fails to compile** and
that **every substring** appears in the diagnostic (substring-only, so message
prose churn does not break the suite). Positive cases live in a compiled
integration test — their **compilation is the assertion**.

| Case name | Kind | Misuse / property | Observable expectation (pinned toolchain, C28) | Satisfies |
|---|---|---|---|---|
| `wrong_type_binding` (T8 seed, reused) | compile-fail | Bind a handle of the wrong **value type**. | **Fails to compile;** message names **both** type names (`ExpectedWidget`, `SuppliedGadget`). Real API → E0271/E0308. | C3 (wrong-type names both). |
| `typed_handle_wrong_arity` | compile-fail | Bind **one** handle where **two** are declared. | **Fails to compile;** substrings `E0271`, `Handle<Alpha>`, `(Alpha, Beta)`. | C3 (wrong-arity is a compile error). |
| `typed_handle_unforgeable` | compile-fail | Fabricate a handle by struct literal from outside the module. | **Fails to compile;** substrings `E0451`, `private`, `Handle`. | C2 (no API obtains a handle for an unregistered node). |
| `typed_handle_data_cycle` | compile-fail | Bind B's not-yet-created handle into A (registered first). | **Fails to compile;** substrings `E0425`, `cannot find value`. | C2 (data-edge cycle inexpressible, structural). |
| `typed_handle_non_default_rule_on_data_node` | compile-fail | Set `all-terminal` on a data-dependent node. | **Fails to compile;** substrings `E0599`, `trigger_rule`, `ConsumesData`. | C3 (typestate forbids non-default rule on a data node). |
| `handles_are_freely_copyable` | **positive** | Copy/pass a `Handle<Rc<String>>`; reuse after. | **Compiles** (fn-pointer phantom keeps it Copy/Send/Sync). | C2 (handles copy/pass freely). |
| `single_input_takes_t_not_one_tuple` | **positive** | Bind exactly one `Handle<Gamma>` as `T`. | **Compiles** with no tuple wrapping. | C3 / open question (single input is `T`). |
| `fan_out_one_handle_many_consumers` | **positive** | Bind one producer handle to four consumers. | **Compiles;** handle reused freely. | C3 (one handle → many consumers). |
| `multi_input_and_type_erased_ordering` | **positive** | Bind a 2-tuple; erase two value types to `Ordering`. | **Compiles.** | C3 (exact 2-arity) / C4 (type-erased ordering). |

The **ordering-edge** cycle cases (`ordering_edge_self_cycle`,
`ordering_edge_back_edge`) are **T0.9's** fixtures, already checked in; C2's
"cycle through data **or** ordering edges fails to compile" is jointly satisfied
by this ticket's `typed_handle_data_cycle` and T0.9's ordering-edge cases.

### Fixtures are pinned-toolchain, regenerated deliberately (C28)

The compile-fail snapshots are **pinned to the workspace toolchain**
(`rust-toolchain.toml`, currently `1.95.0`) and assert only substrings (error
codes and type/method names), so ordinary compiler-message churn does not break
the suite. On a **pinned-toolchain bump** they are **regenerated deliberately**
through the T8 harness's blessing flow (`DAGR_BLESS=1 cargo test -p dagr-core
--test ui`) and the diff reviewed — never silently rewritten. This is the C28
"toolchain bumps regenerate those fixtures deliberately" contract.

### Coverage matrix: no change

**C2** and **C3** remain as they stand in `docs/coverage-matrix.md`:
`machine`/`unmapped`, **deferred to T12** (the cycle-inexpressibility and
wrong-type/wrong-arity compile-failure cases that cover them are authored by T12
against the real T10/T11 authoring API). A **decision/spike ticket owes no
covering test**; the covering tests land with T10/T11/T12, each of which edits the
matrix per its per-ticket duty. This ADR makes **no edit** to the coverage matrix
and **no edit** to the criteria-matrix partition (T0.10's), and it **agrees** with
both. (The T8 harness already ships as C2/C3's UI infrastructure; adding fixtures
to it maps no new criterion here.)

### Reopen condition

If a downstream ticket cannot honor a seam as written — for example, if the real
`Handle<T>` cannot be made unconditionally `Copy`/`Send`/`Sync` under the T0.2
output bounds without coupling `T`'s auto-traits (a coherence obstruction the
phantom cannot route around), if the sealed `Deps` bound cannot produce a
both-type-names message under a future toolchain, if the arity-8 macro forces a
different ceiling, or if the typestate cannot structurally forbid a non-default
rule on a data node — **the contract reopens here**, in this ADR, rather than
being worked around locally. A local workaround that silently diverges (a runtime
cycle check, a name-keyed lookup, a forgeable handle, a runtime trigger-rule
check) is a defect, not a fix.

## Rejected alternatives

- **`PhantomData<T>` for the handle's type marker** (instead of `PhantomData<fn()
  -> T>`). **Rejected on C2's "cheap and freely copyable":** `PhantomData<T>`
  makes `Handle<T>` inherit `T`'s auto-traits, so a handle to a `!Send`/`!Sync`
  value would itself be `!Send`/`!Sync` and could not be passed across the build
  freely. The spike showed the concrete failure — ``error[E0277]: `Rc<String>`
  cannot be sent between threads safely`` — while the fn-pointer phantom compiles.
  The `fn() -> T` form also keeps the handle **covariant** in `T` and owns no `T`.
- **A runtime (or assembly-time) cycle-detection / graph-validation pass**
  (accept edges freely, reject cycles later). **Rejected on C2's design intent:**
  the guarantee is **structural and compile-time** — "a cycle cannot be expressed
  … structural, not a validation pass that runs later." The backward-reference
  discipline (§6) makes the cycle **unwritable** (evidence: `E0425`), so no such
  pass is needed or wanted; adding one concedes a cycle *can* be written and is
  caught late.
- **A name/index/string-key lookup registry** (obtain a handle or an output by
  `get("node")` / `get(3)`). **Rejected on C2 and the permanent scope boundary:**
  "there is no lookup by name, index, or string key," and a lookup registry is a
  step toward a runtime-mutable graph / metadata store, which the boundary
  forbids. The handle a `register` call returns is the **only** currency
  (evidence: the unforgeable `E0451` case).
- **A one-tuple `(T,)` for single-input tasks** (uniform "always a tuple"
  encoding). **Rejected as unnecessary ergonomic noise:** it forces
  `register(task, (g,))` and `Task<Input = (Gamma,)>` for the most common case
  with no benefit; the arity-1 `Deps for Handle<A>` impl delivers the bare value
  `A` and single-input sites stay tuple-free (evidence: the positive
  single-input case compiles). Tuples begin at arity 2.
- **A runtime check of the trigger-rule restriction** (accept any rule on any
  node, reject a non-default rule on a data node at assembly or run time).
  **Rejected on C3:** the restriction "makes the builder's **typestate**
  inexpressible — a compile error rather than a runtime check." The typestate
  (§8) removes the method in the data-consuming state (evidence: `E0599`), so the
  mis-configuration cannot even be written.
- **An arity ceiling other than 8** (unbounded via a blanket impl, or a lower cap
  like 4). **Rejected:** an unbounded/blanket impl defeats the curated
  arity-cliff diagnostic (the whole point is a *finite* set of impls so the
  cliff exists) and risks coherence/compile-time blow-up; a cap much below 8
  would reject realistic fan-ins. 8 matches the dagx precedent and the C3 "aggregate
  into a struct" remedy handles anything larger.

*(Reopen condition stated in §Consequences: if a downstream ticket cannot honor a
seam as written, the contract reopens here rather than being worked around
locally.)*
