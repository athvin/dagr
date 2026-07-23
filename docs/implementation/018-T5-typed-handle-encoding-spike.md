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
