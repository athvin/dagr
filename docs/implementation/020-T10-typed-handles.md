# 020 · T10 — C2: typed handles

> **Milestone:** M1 · **Size:** S · **Type:** feature · **Components:** C2
> **Branch:** `feat/t10-typed-handles` · **Depends on:** T5, T9 · **Blocks:** T11, T13

## Why / context
This ticket lands the concrete `Handle<T>` type that is the *only* way one node ever refers to another node's output. C2 (`### C2 · Handle`, arch.md) governs it: a handle is a typed claim on a value that does not exist yet, obtained solely by registering a node under an explicit name, and it is what makes cycles structurally inexpressible rather than a validation pass that runs later. It builds directly on the T5 design spike (which proved the encoding makes wrong-type / wrong-arity / cyclic constructions fail to compile) and on T9's task abstraction (which defines the declared output type a handle carries). It exists to give T11 (data-dependency binding) and T13 (flow builder and node identity) the handle vocabulary they consume; the compile-failure *tests* that prove the guarantees live in the T8/T12 line, not here.

## Objective
Provide the cheap, freely copyable `Handle<T>` type and wire it as the sole product of node registration, with no forgeable construction path and no lookup escape hatch.

- Define `Handle<T>` carrying the node's identity plus the phantom type of the value the node will eventually produce (`T` from the registered task's declared output type per C1/T9).
- Make `Handle<T>` `Copy` + `Clone` and trivially cheap to pass around during construction; it holds identity, not the value.
- Expose the handle *only* as the return value of registration on the flow builder (the registration surface itself is fleshed out in T13; this ticket delivers the handle type, its identity payload, and a registration-return seam the builder uses).
- Ensure there is no public constructor, no `From`/`Default`, and no way to mint a handle for an unregistered node — construction is crate-private and reachable only through registration.
- Ensure no API returns a handle from a name, index, or string key; the module exposes no such lookup and none can be added without an API review.
- Preserve the structural cycle guarantee at the type level: a handle can only exist for an already-registered node, so a downstream registration can only reference upstreams that already produced handles (the backward-reference discipline C4 also relies on).
- Ensure identity flows from the explicit registration name, not from registration order, so that renaming changes identity and reordering does not (the identity type/derive comes from T0.7 via T13; this ticket consumes it and must not re-derive identity from order).

## Test plan (write these first — TDD)
- **Handles are freely copyable.** Setup: register a node under an explicit name and capture the returned handle. Action: copy the handle into several bindings and pass copies into helper functions, using the original again afterward. Expected: every copy compiles and remains usable; using the original after the copies is not a move error (confirms `Copy`, not just `Clone`); all copies compare equal to one another.
- **A handle carries the node's typed output.** Setup: register a node whose task declares output type `A`, and a second node declaring output type `B` (distinct types). Action: inspect each returned handle's identity and observe its declared value type through the public surface. Expected: the two handles are distinct, each reports the identity of its own registration, and the value type each carries matches the registering task's declared output — a handle for the `A`-producing node is observably a claim on `A`.
- **Identity comes from the name, not registration order.** Setup: two pipelines built from the same set of nodes registered in different orders, each node given the same explicit name in both. Action: compare the handle identities produced for the same-named node across the two orders. Expected: the identity of a given name is byte-for-byte identical regardless of registration order (reorder changes nothing).
- **Renaming a node changes its identity.** Setup: register a node under name `x`; separately register an otherwise-identical node under name `y`. Action: compare the two handles' identities. Expected: the identities differ solely because the names differ, and this difference is exactly what will move the structural fingerprint downstream (C21) — asserted here at the identity level.
- **No forgeable construction path (compile-fail).** Setup: an external-crate consumer (the trybuild/ui fixture harness from T8, pinned to the workspace toolchain) that imports the public API. Action: attempt to construct a `Handle<T>` directly — via a would-be public constructor, `Default`, `From`, struct literal, or transmute-free public field access. Expected: the attempt fails to compile because no such public path exists; the checked-in compile-fail fixture asserts the failure. (The fixture is authored/owned by the T12 suite; this ticket guarantees the *absence* of any public construction surface that would make such a fixture compile.)
- **No lookup by name / index / string key.** Setup: the same external consumer holding a built or in-construction flow. Action: search the public API for any function that returns a `Handle<T>` (or a node reference) given a name, an index, or a string key, and attempt to call one. Expected: no such function is exposed; any attempt to obtain a handle other than as a registration return value fails to compile. Verified both by a compile-fail fixture and by an API-surface assertion (e.g. a doc/`pub` inventory check) that the handle module exports only the type and its registration-return seam.
- **A handle only exists after registration (cycle inexpressibility, foundation).** Setup: a construction sequence that would need node `b`'s handle to register node `a` *and* node `a`'s handle to register node `b`. Action: attempt to write it. Expected: it cannot be written — the second reference has no handle to name yet, so the code fails to compile. This ticket asserts the enabling property (handles are strictly backward-referencing); the full data-and-ordering cycle compile-fail matrix is T12.

## Definition of done
- [ ] `Handle<T>` exists, is `Copy` and `Clone`, is cheap to copy, and carries the node's identity plus the type of the value the node will produce (C2 purpose and behavior).
- [ ] Handles can be copied and passed around freely during pipeline construction (C2 acceptance criterion), covered by a passing test.
- [ ] There is no public API to obtain a handle for a node that has not been registered — construction is crate-private and reachable only through node registration (C2 acceptance criterion), guaranteed by the public surface and demonstrated by a compile-fail fixture seam for T12/T8.
- [ ] There is no public API to retrieve a node's output — or a handle — by name, index, or string key (C2 acceptance criterion), guaranteed by the public surface and an API-inventory assertion.
- [ ] A handle can only reference an already-registered node, so the code cannot express a forward reference — the enabling half of C2's structural, compile-time cycle guarantee (the full cycle compile-fail matrix is deferred to T12).
- [ ] Node identity comes from the explicit registration name and is stable under declaration reordering; renaming changes identity (C2 acceptance criterion), with tests asserting reorder-stability and rename-sensitivity at the identity level.
- [ ] The registration-return seam that hands `Handle<T>` back is in place for T13's builder and T11's binding API to consume, with the handle's value type wired to the registering task's declared output type (C1/T9).
- [ ] The handle module's public exports are limited to the type and its registration-return path; adding a construction or lookup path would require a deliberate `pub` change (documented so review catches it).
- [ ] Rustdoc on `Handle<T>` states, at the point of use, that it is obtained only by registration and is the sole way to reference another node's output (C2), with no lookup-by-name/index/string.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The data-dependency binding API — exact type matching, tuple arities, fan-out, and the ownership model — is **T11 (C3)**; this ticket only provides the handle the binding consumes.
- The flow builder itself, registration ergonomics, duplicate-name checking, and the identity trait/derive are **T13 (C7)** and **T0.7**; this ticket consumes the identity type and must not re-derive identity from registration order.
- The full compile-failure suite — cycle inexpressibility across data *and* ordering edges, wrong-type binding, wrong arity, non-default trigger rule on a data-dependent node, unforgeable handles, ownership-demand on a shared value — is **T12**, built on the **T8** trybuild/ui harness; this ticket only guarantees the public surface those fixtures assert against and does not author the matrix.
- Ordering-edge mechanics (C4) and their backward-reference discipline are **T0.9/T50**; referenced here only because they rely on the same handle guarantee.
- Assembly-time checks (duplicate names, empty pipeline, fingerprinting) are **T14/C7**; a `Handle` is a construction-time value and pulls in no runtime, scheduling, execution, or artifact behavior.
- Scope boundary: a handle is never a runtime lookup key, a name registry, or a metadata handle into a store — dagr is not a metadata store and the graph shape never changes at runtime, so no dynamic-handle, handle-from-string, or runtime-node-discovery affordance may be introduced.
