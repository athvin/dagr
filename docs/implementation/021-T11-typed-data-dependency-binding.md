# 021 · T11 — C3: typed data-dependency binding

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C3
> **Branch:** `feat/t11-typed-data-dependency-binding` · **Depends on:** T10, T0.2 · **Blocks:** T12, T14, T50

## Why / context
dagr's wiring is handle-based: a task consumes another task's output by binding that upstream's `Handle<T>` at its own registration. This ticket delivers the binding API — component **C3 · Data dependency** (arch.md §115–128) — layered on the typed handles from T10 (C2) and the task abstraction from T9 (C1). It encodes exactly the ownership-and-sharing model locked by the T0.2 ADR (sole-consumer-owns / multi-consumer-shared-read / per-edge clone-on-read), which arch.md §81–96 (C1) makes normative. The split matters and defines the whole ticket: **value-type mismatches and arity mismatches are compile errors here**, while **receive-mode conflicts are whole-graph facts deferred to assembly (T14)** — C3 only records the declared mode; it never adjudicates it. Getting the compile-time surface (exact type match, tuple arities, fan-out, trigger-rule typestate) right is what lets T12 (compile-fail suite), T14 (assembly validation), and T50 (ordering edges) build on a stable binding vocabulary.

## Objective
Provide the API by which a downstream node declares its data dependencies at registration, binding one or more already-registered upstream handles whose value types must exactly match the consuming task's declared input types.

Concrete pieces of work:
- **Exact type matching.** Binding a `Handle<T>` to an input position declared as some other type is a compile error naming both the expected and supplied type names. Single-input tasks bind a bare `Handle<T>` (not a one-tuple); multi-input tasks bind a tuple of handles.
- **Arity matching and the ceiling.** Binding a different number of handles than the task declares is a compile error. Support tuple arities up to the documented maximum (working assumption 8 per T5); at the cliff, surface the curated `#[diagnostic::on_unimplemented]` message that says "too many inputs — aggregate into a struct" rather than a wall of trait errors. Document the ceiling at the point of use.
- **Fan-out.** One upstream handle may be bound to any number of downstream tasks; binding does not consume or invalidate the handle (handles stay cheap and copyable per C2).
- **Receive-mode recording.** Each bound edge carries its declared receive mode — owned, shared, or clone-on-read — captured in the signature/builder call, recorded on the edge, and left un-adjudicated. C3 does no consumer-count or retry reasoning; those are T14's job. Provide the per-edge `clone-on-read` opt-in surface.
- **Trigger-rule typestate.** A node that has bound data dependencies is, by builder typestate, unable to be given any trigger rule other than `all-succeeded` — attempting to set one is a compile error, not a runtime check (arch.md §126, §52).
- **Ordering semantics of a data edge.** A data dependency implies ordering *and* upstream-success (no value otherwise); ensure the edge is recorded so downstream readiness (C11, later) and slot wiring (C10, later) can rely on that invariant. Record data edges distinctly from the ordering edges T50 will add.

## Test plan (write these first — TDD)
Compile-success and runtime-shape behavior are ordinary library tests; compile-failure behavior is checked-in UI tests through the T8 harness, pinned to the workspace toolchain, asserting only that the required substrings appear (per C28, arch.md §591). Every scenario is independently checkable.

**Compile-success: single-input exact match.**
Setup: register an upstream task producing a value of type `A`, obtaining a handle; define a downstream task declaring a single input of type `A`.
Action: register the downstream task, binding the upstream handle as a bare handle (not a tuple).
Expected: it compiles and the downstream registration returns a handle for its own output; the recorded edge names the upstream node as a data dependency.

**Compile-success: multi-input tuple binding at each supported arity.**
Setup: define downstream tasks declaring 2, 3, … up to the documented ceiling of distinct input types, with matching upstream handles registered.
Action: bind the tuple of handles at each arity.
Expected: every arity from 2 through the ceiling compiles; the recorded edges preserve input order and each names the correct upstream.

**Compile-fail: wrong value type.**
Setup: an upstream handle of type `A`; a downstream task declaring input type `B`.
Action: attempt to bind the `A` handle to the `B` input.
Expected: a compile error whose message contains both `A` and `B` (the expected and supplied type names). Captured as a checked-in UI snapshot; only the presence of both type names is asserted, not prose quality (arch.md §124, C28).

**Compile-fail: wrong arity (too few and too many for a fixed-arity task).**
Setup: a downstream task declaring exactly N inputs.
Action: bind N−1 handles; separately, bind N+1 handles.
Expected: both fail to compile; UI snapshots checked in for each. Distinct from the ceiling case below.

**Compile-fail: exceeding the documented arity ceiling.**
Setup: an attempt to bind more handles than the maximum supported tuple arity.
Action: register such a binding.
Expected: a compile error carrying the curated `on_unimplemented` message that names the ceiling and directs the author to aggregate inputs into a struct produced by an intermediate node — not a wall of raw trait errors. UI snapshot checked in.

**Compile-fail: non-default trigger rule on a data-dependent node.**
Setup: a downstream task with at least one data dependency bound.
Action: attempt to set a trigger rule other than `all-succeeded` (for example `all-terminal` or `any-failed`) on that node via the builder.
Expected: it does not compile — the method is unavailable in the data-dependent typestate. UI snapshot checked in (arch.md §126). Conversely, a node that consumes nothing can set a non-default rule and compiles.

**Runtime shape: fan-out — one handle, many consumers.**
Setup: one upstream handle; three downstream tasks each declaring the upstream's output type as input.
Action: bind the same handle into all three downstream registrations.
Expected: all three compile and register; the handle remains usable after each binding (copyable, not moved); the assembled graph records three distinct data edges from the one upstream. No mode adjudication occurs at this layer even though this is a multi-consumer value — that is T14's assertion.

**Receive-mode is recorded, not adjudicated.**
Setup: bind one handle with the owned receive mode into a single consumer; bind another handle with the explicit clone-on-read opt-in.
Action: inspect the recorded edges after registration.
Expected: each edge carries its declared mode verbatim (owned / shared / clone-on-read) and C3 raises no error about consumer counts or retries — proving the type-vs-mode split. A companion note/test documents that the corresponding conflict cases (owned demand on a multi-consumer value; owned edge into a retrying node without clone-on-read) are deliberately *not* rejected here and are exercised in T14.

**Data edge implies ordering and success.**
Setup: bind an upstream handle into a downstream task.
Action: inspect the recorded dependency.
Expected: the edge is recorded as a data dependency (distinct from an ordering-only edge), and the recorded structure reflects that the downstream depends on the upstream having succeeded — the invariant later consumed by readiness (C11) and slot wiring (C10). No API exists to add a data edge between two already-registered nodes after the fact (backward-reference discipline preserved from C2).

**Documentation example compiles.**
Setup: a doctest / example showing a two-input binding and the point-of-use note stating the arity ceiling and the aggregate-into-a-struct escape hatch.
Action: run the doc examples.
Expected: the example compiles and the rendered docs state the maximum arity at the point of use (arch.md §128).

## Definition of done
- [ ] Binding a handle whose value type does not exactly match the declared input is a compile error whose message contains both the expected and the supplied type names, verified by a checked-in UI test on the pinned workspace toolchain (C3; arch.md §124).
- [ ] Binding a different number of handles than the task declares is a compile error, verified by checked-in UI tests for both too-few and too-many (C3; arch.md §125).
- [ ] Single-input tasks bind a bare `Handle<T>` and multi-input tasks bind a tuple of handles; tuple arities from 2 through the documented ceiling all compile (C3; arch.md §121, per T5).
- [ ] The maximum input arity is documented at the point of use, and exceeding it produces the curated `on_unimplemented` diagnostic (not a wall of trait errors) directing the author to aggregate into an intermediate struct, verified by a checked-in UI test (C3; arch.md §121, §128).
- [ ] One handle can be bound to any number of downstream tasks; binding does not move or invalidate the handle, and the assembled structure records one distinct data edge per consumer (C3; arch.md §127).
- [ ] A node with data dependencies cannot be given any trigger rule other than `all-succeeded`; the builder typestate makes it inexpressible as a compile error, verified by a checked-in UI test, and nodes that consume nothing can still set non-default rules (C3; arch.md §126, §52).
- [ ] Each data edge records its declared receive mode (owned / shared / per-edge clone-on-read opt-in) verbatim, and C3 performs no consumer-count or retry adjudication — mode conflicts are left for assembly (T14), demonstrated by a test that binds owned and clone-on-read edges without error (C3/C1; arch.md §81, §119).
- [ ] A data dependency is recorded as implying both ordering and upstream success, distinct from an ordering-only edge, and only against already-registered upstream handles (backward-reference discipline; no post-hoc edge API) (C3; arch.md §119, §136).
- [ ] The `Send + 'static` (tasks) and additional `Sync` (outputs) bounds relevant to bindable values are respected and surfaced at the binding site consistent with C1 (arch.md §86).
- [ ] A doctest/example demonstrates a multi-input binding and states the arity ceiling and the aggregate-into-a-struct escape hatch at the point of use; it compiles under the doc-test run.
- [ ] The compile-fail UI fixtures added here are wired into the T8 harness and pass on the pinned workspace toolchain, asserting only the required substrings.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Receive-mode conflict adjudication** (owned demand on a multi-consumer value; owned edge into a retrying node without clone-on-read; naming both consumers) — those are whole-graph assembly checks owned by **T14 (C7)**. C3 records mode; it does not reject.
- **Ordering (no-data) dependencies** and the `all-terminal` rule for ordering-only nodes — **T50 (C4)**; this ticket only records data edges distinctly so C4 can slot in.
- **Consumer counts, remaining-dependency counts, execution order, and the graph fingerprint** — precomputation in **T14 (C7)**.
- **Output slot wiring, references, and memory accounting** (C10) and **readiness tracking / trigger-rule firing** (C11) — later M1 tickets; C3 only guarantees the success-implies-value invariant they rely on.
- **Node policy** (retry, timeout, cost, execution-class override) — **C5 / T-series later**; the retry facts that interact with owned edges are surfaced only at assembly.
- Scope-boundary temptations to resist: no lookup-by-name/index/string to resolve a dependency (handles only); no API to mutate or add edges after registration; **the graph shape never changes at runtime** — binding is a construction-time act with no runtime rewiring, no scheduler hook, and no dynamic fan-out.
