# 062 · T50 — C4: ordering dependencies

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C4
> **Branch:** `feat/t50-ordering-dependencies` · **Depends on:** T11, T40, T0.9 · **Blocks:** T52

## Why / context
dagr already lets a node consume another's *value* through data edges (C3, delivered in T11); C4 adds the complementary edge that carries no value and constrains *sequence only* — "run after," for cleanup-after-publish and cache-warm-before-read shapes where a task's *effect* rather than its output matters downstream. This ticket implements the C4 behaviour and acceptance criteria in arch.md (`### C4 · Ordering dependency`), honouring the closed trigger-rule taxonomy in the normative Vocabulary section and the mechanics locked by the T0.9 decision (registration-time backward references, Option A). It builds directly on the data-edge binding surface (T11), on the graph-artifact edge recording (C20, T40), and it unblocks teardown nodes (C17, T52), which are the first heavy consumer of ordering edges. The load-bearing constraint it must preserve: an ordering edge may attach to **any** node, but a *non-default trigger rule* stays restricted — by builder typestate — to nodes that consume nothing, so a firing rule can never leave a hole where an input was expected.

## Objective
Add registration-time ordering edges to the authoring surface and thread them through the graph artifact and diagram rendering, without weakening any compile-time guarantee.

- Extend the node-registration surface so a downstream node can declare one or more ordering edges against handles (C2) of **already-registered** upstreams, with no API to add an edge between two existing nodes after the fact.
- Allow a node to carry **both** data dependencies and additional ordering edges in a single registration; a node attached only by ordering edges receives no value, and its declaration reflects that (it is a consume-nothing node).
- Preserve, via the builder's typestate, the rule that a *non-default* trigger rule (`all-terminal`, `any-failed`) is expressible only on a consume-nothing node; a data-dependent node remains locked to `all-succeeded` (C3) as a compile error, and ordering edges alone do not unlock non-default rules on a data-consuming node.
- Record ordering edges **distinctly** from data edges in the graph artifact (C20): the edge entry carries its kind, and ordering edges carry no type-name field, while data edges continue to carry the stable carried-type name.
- Render ordering edges **distinctly** from data edges in the diagram output (C24 styling), so a reader can tell sequence-only edges from value-carrying ones.
- Ensure default-rule (`all-succeeded`) propagation across an ordering edge behaves exactly as across a data edge: an ordering upstream must succeed for the default downstream to run, and skips and failures propagate across ordering edges identically; a node that must run regardless says so with `all-terminal`.
- Extend the checked-in compile-failure suite (per T0.9 / T12 conventions) to assert that an ordering-edge cycle cannot be expressed and that a non-default rule on a data-consuming node still fails to compile.

## Test plan (write these first — TDD)

**Ordering edge attaches at registration against an existing upstream.**
Setup: build a pipeline with an upstream node registered and its handle captured. Action: register a downstream node that declares an ordering edge against that handle and consumes no value. Expected outcome: registration succeeds, returns a handle for the downstream node, and the assembled graph records exactly one ordering edge from upstream to downstream and zero data edges into the downstream node.

**A node carries both a data dependency and an additional ordering edge.**
Setup: register a producer whose value the downstream consumes, and a separate side-effect node whose handle the downstream will only order after. Action: register the downstream node binding the producer's handle as data input and adding the side-effect node's handle as an ordering edge. Expected outcome: assembly succeeds; the graph shows one data edge (carrying the producer's stable type name) and one ordering edge (carrying no type name) into the same downstream node.

**An ordering-only node receives no value and its declaration reflects that.**
Setup: register an upstream effect node. Action: register a downstream node attached only by an ordering edge to it, declared as consuming nothing. Expected outcome: the downstream node compiles and assembles; its recorded input arity is zero; there is no value slot demanded from the upstream, and no data edge is recorded.

**No API exists to add an edge between two already-registered nodes.**
Setup: register two nodes A and B and hold both handles. Action: inspect the builder/handle surface for any method that would attach an ordering (or data) edge from A to B after B is already registered. Expected outcome: no such method exists on the public surface; the only way to attach an ordering edge is at the downstream node's own registration. Recorded as a checked-in compile-failure test asserting the after-the-fact attach call does not compile.

**An ordering-edge cycle cannot be expressed (compile-fail).**
Setup: attempt to write a pipeline where a node's ordering edge references a handle for a node that has not yet been registered (the only way a cycle could be spelled). Action: compile the fixture. Expected outcome: it fails to compile because the handle does not exist yet; the compile-failure test is checked in alongside the data-edge cycle case, demonstrating C2's structural cycle guarantee holds for ordering edges too.

**A non-default trigger rule on a data-consuming node fails to compile.**
Setup: register a node that binds a data handle. Action: attempt to set its trigger rule to `all-terminal` (or `any-failed`). Expected outcome: a compile error produced by the builder typestate — not a runtime check — and this case is a checked-in compile-failure test. Adding an ordering edge to that same data-consuming node does **not** make the non-default rule compile.

**A non-default trigger rule is expressible on a consume-nothing node with ordering edges.**
Setup: register two upstream effect nodes. Action: register a consume-nothing downstream node ordered after both and give it the `all-terminal` rule. Expected outcome: it compiles and assembles; the graph records both ordering edges and the node's effective trigger rule as `all-terminal`.

**Default-rule failure propagates across an ordering edge.**
Setup: assemble a pipeline where a consume-nothing downstream (default `all-succeeded`) is ordered after an upstream, and drive the upstream to a `failed` terminal state through the deterministic interpretation harness (C28). Action: evaluate the downstream's readiness once all its upstreams are terminal. Expected outcome: the downstream is marked `upstream-failed` without executing, identical to the outcome had the same edge been a data edge.

**Default-rule skip propagates across an ordering edge.**
Setup: same shape, but drive the ordering upstream to an originated `skipped` state. Action: evaluate the downstream's readiness. Expected outcome: the downstream is marked `upstream-skipped`, carrying the originating node's identity — identical to skip propagation across a data edge.

**An `all-terminal` node ordered after a failure still runs.**
Setup: a consume-nothing cleanup node with `all-terminal`, ordered after an upstream that fails. Action: drive the upstream to `failed` and evaluate the cleanup node. Expected outcome: the cleanup node's rule fires and it becomes ready (it executes), because `all-terminal` never propagates failure — the motivating cleanup-after-failure case.

**Ordering edges are recorded distinctly in the graph artifact.**
Setup: assemble a pipeline containing one data edge and one ordering edge into distinct downstream nodes. Action: emit the graph artifact (C20) in an empty environment. Expected outcome: the artifact validates against its published schema; the data edge entry names its carried stable type and its kind marks it as data; the ordering edge entry marks its kind as ordering and carries no type-name field. Emitting the artifact twice from the same binary produces identical bytes outside the generation-time field.

**Ordering edges are drawn distinctly in diagrams.**
Setup: take the graph artifact from the previous scenario. Action: run the renderer (C24) to produce Graphviz DOT and Mermaid output. Expected outcome: both outputs are accepted by their reference tools; the data edge and the ordering edge appear with documented, visually distinct styling; golden-file tests pin the distinction.

**Ordering edges participate in the structural fingerprint.**
Setup: two pipelines identical except that one has an ordering edge the other lacks. Action: compute each pipeline's structural fingerprint (C21). Expected outcome: the fingerprints differ, because the edge set — including edge kinds — is part of the structural fingerprint; adding an ordering edge is a structural change a resume must notice.

## Definition of done
- [ ] An ordering edge can be declared at registration time against any already-registered node, and no API exists to add an edge between two existing nodes afterward.
- [ ] A node may carry both data dependencies and additional ordering edges in one registration.
- [ ] A node attached only by ordering edges receives no value, and its recorded declaration reflects zero data inputs.
- [ ] The builder typestate keeps a *non-default* trigger rule expressible only on a consume-nothing node; a data-consuming node stays locked to `all-succeeded` as a compile error, and adding an ordering edge does not unlock a non-default rule on it.
- [ ] Under the default `all-succeeded` rule, an ordering upstream's failure or skip propagates to the downstream exactly as a data upstream's would (`upstream-failed` / `upstream-skipped`, carrying originating identity), verified through the deterministic interpretation harness (C28).
- [ ] A node ordered after a failure with `all-terminal` still executes; its rule never propagates failure.
- [ ] Ordering edges are recorded distinctly from data edges in the graph artifact (C20): each edge entry carries its kind, ordering edges carry no type-name field, and data edges continue to carry the stable carried-type name.
- [ ] The graph artifact still validates against its published schema, and emitting twice from the same binary produces identical bytes outside the generation-time field, with ordering edges present.
- [ ] Ordering edges are drawn with distinct styling from data edges in both Graphviz DOT and Mermaid output (C24), accepted by the reference tools and pinned by golden-file tests.
- [ ] Edge kind (data vs. ordering) is part of the structural fingerprint (C21), so adding or removing an ordering edge changes the structural fingerprint.
- [ ] A checked-in compile-failure test demonstrates that an ordering-edge cycle cannot be expressed (per T0.9), and one demonstrates a non-default rule on a data-consuming node fails to compile.
- [ ] Public API items introduced or changed carry rustdoc, including a short note on when to reach for an ordering edge versus a data edge.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Teardown nodes (C17, T52): the ordered-after-set semantics, the fresh cancellation signal, admission bypass, and the teardown deadline belong to T52 and build on this ticket — do not implement any teardown behaviour here.
- Groups (C6, T51): group clustering in diagrams is a separate ticket; only edge styling is in scope here.
- Any runtime mutation of the graph shape — no API to attach, detach, or rewire edges after registration, and nothing that lets the DAG change at runtime (scope boundary: the graph shape never changes at runtime).
- New trigger rules or changes to the closed rule set (`all-succeeded`, `all-terminal`, `any-failed`); this ticket wires ordering edges into the existing rules, it does not extend the taxonomy.
- Adding scheduler-like ordering semantics, priorities, or cross-run sequencing — dagr is not a scheduler; an ordering edge is a within-run "run after" and nothing more.
- Resume interaction for ordering-only nodes beyond the propagation this ticket tests (the cleanup-after-publish resume shape is exercised under C27's own tickets).
