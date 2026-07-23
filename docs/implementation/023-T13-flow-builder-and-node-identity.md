# 023 · T13 — C7: flow builder and node identity

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C7
> **Branch:** `feat/t13-flow-builder-and-node-identity` · **Depends on:** T10, T0.7 · **Blocks:** T14, T19, T51

## Why / context
C7 (arch.md · "Flow assembly") requires a builder that accumulates node registrations and produces an immutable pipeline. This ticket lands the builder skeleton and the one decision that everything downstream binds to: **node identity is the explicit registration name** — supplied at every registration (C2 · Handle), unique across the whole pipeline, stable under declaration reordering, and excluded from group membership (C6). T10 gives us the `Handle<T>` that registration hands back; T0.7 fixes the stable-name trait and fingerprint composition into which this identity feeds. T14 will bolt full assembly validation and precomputation onto the immutable pipeline this ticket produces, T19's event writer needs the identity to stamp records, and T51's groups attach labels without touching identity — so the identity contract must be pinned correctly here.

## Objective
Build the flow builder and the node-identity model, stopping short of assembly validation and precomputation (those are T14).

- A flow builder value that accepts node registrations, each carrying an explicit caller-supplied node name, and hands back the typed handle from T10.
- Node identity derived **solely** from the registration name — never from registration order, insertion index, or any implicit counter.
- A finalization step that consumes the builder and yields an **immutable** pipeline value; once produced, no further registration or mutation is possible.
- Preservation, on each registered node inside the immutable pipeline, of everything downstream tickets read: the identity name, the handle-to-node linkage, and a slot for the group label that C6/T51 will populate — with the group label explicitly **not** part of identity.
- The internal representation of the node set ordered/keyed so that identity comparison and lookup are order-insensitive (reordering registrations yields the same identities and the same immutable pipeline content).

Explicitly deferred to T14: duplicate-name reporting, empty-pipeline check, execution-class-override validation, duplicate stable-name check, durable-without-contract check, the zero-consumer warning, consumer/dependency counts, execution ordering, and fingerprint computation. This ticket may lay the *seams* (data structures those checks will read) but performs none of the checks.

## Test plan (write these first — TDD)
Each scenario is independently checkable. Where a scenario asserts a compile-time property, it belongs to the checked-in compile-failure suite (per C2/T12 conventions) rather than a runtime assertion.

- **Registration returns a usable handle.** Setup: a fresh builder. Action: register one node under an explicit name. Expected: the call returns a handle of the node's output type that can be copied and passed around, and no separate API exists to fabricate a handle for that node — obtaining a handle requires registering.

- **Identity is the registration name.** Setup: a builder with one node registered under a chosen name. Action: finalize and inspect the immutable pipeline's node set. Expected: the node is found under exactly that name, and the recorded identity equals the supplied name verbatim (no prefix, suffix, index, or normalization).

- **Reordering registrations changes nothing.** Setup: two builders that register the same set of nodes (same names, same node bodies) but in different declaration orders. Action: finalize both. Expected: the two immutable pipelines contain the same node identities associated with the same nodes; identity does not depend on order. (This is the runtime companion to C21's byte-identity guarantee, which T14/T41 assert at the fingerprint level.)

- **Renaming changes identity.** Setup: two pipelines identical except one node's registration name differs. Action: compare the node identities. Expected: the node's identity differs between the two pipelines — renaming a node changes its identity (and, downstream, its structural fingerprint).

- **Group label is excluded from identity.** Setup: two pipelines whose corresponding nodes carry identical names but different group labels (using whatever seam this ticket exposes for the label; T51 supplies the real API). Action: compare node identities. Expected: identities are equal — the group label is presentation metadata carried alongside identity, never part of it.

- **The finalized pipeline is immutable.** Setup: a builder with several nodes registered, then finalized into a pipeline. Action: attempt to register or mutate through the pipeline value. Expected: no such API is reachable — mutation-after-finalize is inexpressible (compile-failure test), and the pipeline exposes only read access to its node set.

- **Handle-to-node linkage survives finalization.** Setup: register two nodes, keep both handles, finalize. Action: resolve each handle against the immutable pipeline's node set. Expected: each handle maps to exactly the node it was returned for; the linkage established at registration is intact in the immutable pipeline.

- **Builder does not touch the environment.** Setup: an environment with no filesystem access, no network, no clock reads, no credentials, and — per C7 — no parameter values reachable. Action: build and finalize a small pipeline. Expected: registration and finalization complete with every external resource absent. (T15 owns the full purity/empty-environment proof; this ticket asserts only that the builder+finalize path introduces no such dependency.)

- **No parameter reachability during registration.** Setup: a builder mid-construction. Action: inspect the registration/finalization API surface. Expected: no path exposes or accepts a parameter value during registration or finalization — parameters are a bootstrap concern (C7), and the type surface makes them unreachable here.

## Definition of done
- [ ] The builder accepts node registrations and returns the typed handle from T10 for each; no API exists to obtain a handle for a node that has not been registered (C2).
- [ ] Every registration supplies an explicit node name; identity comes from that name and never from registration order (C2 · Handle).
- [ ] Node names being unique across the whole pipeline is the identity contract this ticket establishes; **detecting** a duplicate is deferred to T14, but the identity model assumes and preserves uniqueness (C2, C6, C7).
- [ ] Reordering registrations leaves node identities and immutable-pipeline content unchanged; renaming a node changes its identity (C2 acceptance: rename changes identity, reorder changes nothing).
- [ ] The group label is carried alongside identity as presentation metadata and is excluded from identity, leaving a seam for T51 to populate (C6).
- [ ] Finalization consumes the builder and yields an immutable pipeline; no registration or mutation is possible once the pipeline exists (C7 · "produces an immutable pipeline"), enforced at compile time.
- [ ] Each node in the immutable pipeline preserves its identity, its handle linkage, and the group-label slot that downstream tickets read (C7, C19, C21).
- [ ] The internal node-set representation makes identity lookup and comparison order-insensitive (supports the reorder-stability guarantee above).
- [ ] Registration and finalization perform no I/O and reach no parameter value — no network, filesystem, clock, credentials, or parameters during registration or assembly (C7 · "Assembly is pure"); the full mechanical proof stays with T14/T15.
- [ ] Assembly validation and precomputation are **not** performed here — duplicate-name/empty/class-override/stable-name/durable-contract checks, the zero-consumer warning, and consumer/dependency counts, execution order, and fingerprint computation are left to T14, with only their data seams laid where needed.
- [ ] Rustdoc on the builder and pipeline types states the identity contract (name-based, reorder-stable, group-excluded) and points readers to T14 for what assembly additionally checks.
- [ ] The Test plan scenarios above exist as tests (runtime tests plus checked-in compile-failure tests for the inexpressible-mutation and unforgeable-handle cases) and pass.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Assembly validation and precomputation (T14):** duplicate-name reporting naming both declarations, empty-pipeline detection, execution-class-override validation, duplicate stable-name detection, durable-without-contract, the zero-consumer non-unit-output warning, and precomputed consumer counts, dependency counts, execution order, and the fingerprint slot. This ticket only lays their data seams.
- **Data and ordering dependency binding (C3/C4, T11/T12):** the handle-binding API, type matching, tuple arities, fan-out, ordering edges, and the wiring compile-failure suite.
- **Node policy (C5, T29):** retries, timeouts, costs, class overrides, trigger rules, group membership semantics — only the group-label *slot* is stubbed here.
- **Groups (C6, T51):** the real group-labelling API and diagram clustering; this ticket reserves the label seam and asserts identity-exclusion only.
- **Fingerprints (C21, T41) and the graph/run artifacts (C20/C22):** no hashing, canonical ordering, or artifact emission here.
- **The event stream writer (C19, T19)** and any runtime behaviour: this ticket produces the immutable pipeline the writer will later stamp, nothing more.
- **Scope-boundary temptations to resist:** no scheduling, no runtime graph mutation, no by-name/index/string lookup of node outputs, and no route by which the graph shape could change after finalization — the pipeline is immutable and the graph shape is fixed at assembly, permanently.
