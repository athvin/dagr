# 024 · T12 — Compile-failure suite for wiring

> **Milestone:** M1 · **Size:** S · **Type:** feature (tests) · **Components:** C2, C3
> **Branch:** `feat/t12-compile-failure-suite-for-wiring` · **Depends on:** T8, T11, T0.9 · **Blocks:** T28

## Why / context
dagr's central promise is that mis-wiring a pipeline fails to *compile* rather than at runtime, and where a message is emitted it is legible. This ticket makes that promise verifiable by filling out the wiring compile-failure suite on top of the harness stood up in T8, exercising the real handle and data-binding surface delivered in T11 and the ordering-edge cycle contract locked in T0.9. It is governed by **C2 · Handle** (cycles — through data *or* ordering edges — are inexpressible by construction, demonstrated by checked-in compile-failure tests; no handle exists for an unregistered node; no by-name/index/string lookup), **C3 · Data dependency** (wrong-type binding is a compile error naming both type names; wrong arity is a compile error; a data-consuming node cannot carry a non-`all-succeeded` trigger rule, made inexpressible by the builder typestate), and the **C4** cycle assertions delegated to it by T0.9. Every case pins to the workspace toolchain per **C28** and asserts only that the required type names / curated diagnostic appear — never exact prose. This is the last M1 confidence gate before the T28 demo.

## Objective
Ship the full checked-in wiring compile-failure suite as a set of trybuild/UI cases in the harness from T8, each proving a specific mis-wiring cannot be expressed (or, where a message is asserted, that its diagnostic names the right types), so that T28 and every later builder change inherit a standing regression net.

- Add compile-fail cases for **cycle inexpressibility** over *data* edges: a node cannot bind its own not-yet-returned handle, and cannot bind a handle of a descendant registered after it.
- Add compile-fail cases for **cycle inexpressibility** over *ordering* edges, adopting the case names and contract from the T0.9 ADR (`ordering_edge_self_cycle`, `ordering_edge_back_edge`) and folding in its skeletal fixture as a first-class suite member.
- Add a **wrong-type binding** case whose asserted diagnostic contains **both** the expected input type name and the supplied handle's value type name.
- Add **wrong-arity** cases: binding fewer and binding more handles than the consuming task declares, each a compile error; include one case at the documented arity ceiling proving the *curated* diagnostic (not a wall of trait errors) appears.
- Add a **non-default trigger rule on a data-dependent node** case: attaching any rule other than `all-succeeded` to a node that consumes data fails to compile because the builder typestate makes it inexpressible.
- Add an **unforgeable-handle** case: there is no public constructor for a handle and no by-name / by-index / by-string lookup, so code that tries to conjure or look up a handle without registering a node fails to compile.
- Add an **ownership-demand-on-a-shared value** case at the level the *type system* can reject it — a task declaring an owned receive mode over a value type that is not movable-into-owned-delivery in that position fails to compile — while explicitly deferring the whole-graph multi-consumer ownership *conflict* (which is an assembly error) to T14.
- Register every case's criterion id in the T7 coverage matrix, and add at least one **positive** (compiles) counterpart for the cycle and typestate cases so a future regression that *loosens* a guarantee fails review.

## Test plan (write these first — TDD)
Every case is a checked-in UI/trybuild fixture run under the pinned workspace toolchain, asserting only substring presence (both type names, or the curated-diagnostic marker) per T8's assertion rule — never exact wording, spans, or note count. Each is independently checkable.

- **Data-edge self-cycle is inexpressible.** Setup: a fixture where one node attempts to bind its own handle as a data input at its own registration. Action: compile under the pinned toolchain with snapshots frozen. Expected: it fails to compile, because no handle for that node exists at the point of binding; recorded as a compile-fail case.
- **Data-edge back-edge is inexpressible.** Setup: a fixture registering node A, then node B binding A, then an attempt to make A bind B's handle. Action: compile. Expected: it fails to compile, because A's registration is closed and B's handle did not exist when A was registered, and there is no after-the-fact edge API; recorded as a compile-fail case.
- **Ordering-edge self-cycle is inexpressible.** Setup: the `ordering_edge_self_cycle` fixture from the T0.9 ADR, wired into this suite. Action: compile. Expected: it fails to compile; the suite adopts the ADR case name verbatim.
- **Ordering-edge back-edge is inexpressible.** Setup: the `ordering_edge_back_edge` fixture from T0.9. Action: compile. Expected: it fails to compile for the same backward-reference reason as the data back-edge, proving the cycle guarantee extends across ordering edges (C2).
- **Wrong-type binding names both types.** Setup: a fixture binding a handle whose value type is one concrete type to a consuming task whose declared input is a distinct concrete type. Action: compile and capture the diagnostic. Expected: it fails to compile, and the checked-in snapshot's assertion requires **both** the expected type name and the supplied type name to appear (C3).
- **Both type names are genuinely required.** Setup: temporarily edit the wrong-type snapshot to reference only one of the two type names. Action: run the suite. Expected: the case fails, proving the assertion is not vacuous; reverting restores green.
- **Wrong arity — too few handles.** Setup: a fixture binding one handle to a task declaring two inputs. Action: compile. Expected: it fails to compile (C3 "binding a different number of handles than the task declares is a compile error").
- **Wrong arity — too many handles.** Setup: a fixture binding three handles to a task declaring two inputs. Action: compile. Expected: it fails to compile.
- **Arity ceiling emits the curated diagnostic.** Setup: a fixture binding one more handle than the documented maximum arity. Action: compile and capture the diagnostic. Expected: it fails to compile and the snapshot asserts the *curated* message marker (the "aggregate into a struct produced by an intermediate node" guidance) appears rather than an unbounded trait-error wall (C3).
- **Non-default trigger rule on a data-dependent node is inexpressible.** Setup: a fixture that gives a node with data dependencies a trigger rule other than `all-succeeded`. Action: compile. Expected: it fails to compile because the builder typestate forbids it — a compile error, not a runtime check (C3 acceptance criterion).
- **Default trigger rule on a data-dependent node compiles.** Setup: the same data-dependent node with `all-succeeded` (explicit or defaulted). Action: compile. Expected: it compiles — the positive counterpart proving the typestate does not over-restrict, so a regression that widens the rule set is caught.
- **A handle cannot be forged.** Setup: a fixture attempting to construct a handle directly (no registration) or to obtain one via a by-name/index/string lookup. Action: compile. Expected: it fails to compile — no public constructor and no lookup surface exists (C2).
- **Ownership demand the type system can reject fails to compile.** Setup: a fixture where a task declares an owned receive mode in a position the type system cannot satisfy for owned delivery. Action: compile. Expected: it fails to compile, keeping the type-level slice of the ownership model honest, while the snapshot documents that the whole-graph multi-consumer ownership *conflict* is asserted elsewhere (T14 assembly).
- **A case that unexpectedly compiles is caught.** Setup: replace any one compile-fail fixture's source with a version that compiles cleanly. Action: run the suite. Expected: the suite fails, reporting that a source expected to fail compilation instead succeeded, so no compile-fail case can silently become a no-op (C2 checked-in-test integrity).
- **Suite runs under the pinned toolchain in CI and gates the build.** Setup: the ticket branch with all cases wired into the existing UI-test entry point. Action: open a pull request; in one probe, introduce a snapshot mismatch. Expected: the pipeline goes red on the UI-test step and returns green once fixed, proving the suite gates rather than running inertly (C28; T7 toolchain policy).

## Definition of done
- [ ] A checked-in compile-fail case proves a **data-edge cycle** (self and back-edge) cannot be expressed, structurally per C2 rather than by a later validation pass.
- [ ] A checked-in compile-fail case proves an **ordering-edge cycle** (self and back-edge) cannot be expressed, adopting the T0.9 case names and folding in its skeletal fixture as a live suite member (C2, C4).
- [ ] A **wrong-type binding** case fails to compile and its snapshot asserts that **both** the expected and the supplied type names appear (C3), verified against the pinned workspace toolchain (C28).
- [ ] The both-type-names assertion is proven non-vacuous (removing one name makes the case fail).
- [ ] **Wrong-arity** cases — too few and too many handles — each fail to compile (C3).
- [ ] An **arity-ceiling** case fails to compile with the *curated* diagnostic marker present rather than an unbounded trait-error wall, and the maximum arity referenced by the case matches the documented ceiling (C3).
- [ ] A **non-default trigger rule on a data-dependent node** fails to compile because the builder typestate makes it inexpressible — a compile error, not a runtime check (C3); the `all-succeeded` positive counterpart compiles.
- [ ] An **unforgeable-handle** case fails to compile, demonstrating there is no public handle constructor and no by-name / by-index / by-string lookup (C2).
- [ ] An **ownership-demand** case that the type system alone can reject fails to compile, with the snapshot noting that the whole-graph multi-consumer ownership conflict (an assembly error) is deferred to T14 and not asserted here.
- [ ] Every case asserts only substring presence (both type names, or the curated-diagnostic marker) and never exact prose, wording, spans, or note count, reusing the T8 harness assertion rule (C28).
- [ ] A source that unexpectedly *compiles* causes the suite to fail, so no compile-fail case can degrade into a silent no-op (C2 checked-in-test integrity).
- [ ] All cases live in the T8 harness's canonical UI-test directory behind its single entry point, adding fixtures with no machinery changes, and run under the pinned toolchain in CI on every pull request (C28; T7).
- [ ] Snapshots are regenerated only through T8's documented single-command blessing flow and are canonical, review-visible diffs; the toolchain-bump contract from T8 continues to hold (C28).
- [ ] The T7 coverage-matrix rows for the C2 cycle-compile-failure criterion, the C3 wrong-type/both-names criterion, the C3 wrong-arity criterion, the C3 trigger-rule-typestate criterion, and the C2 no-forgery / no-lookup criteria point at this suite's case ids rather than remaining `unmapped`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The **harness machinery, sample case, and blessing flow** — delivered by T8; this ticket only adds cases behind its existing entry point and must not re-build or fork the harness.
- The **handle, binding, fan-out, and ownership implementation** (C2/C3) — delivered by T10/T11; this suite consumes that real surface and does not extend it.
- The **ordering-edge implementation, recording, and rendering** (C4) — that is T50; only the ordering-edge *cycle* compile-fail contract from T0.9 is exercised here.
- **Assembly-time** validation and its diagnostics — duplicate names, empty pipeline, invalid class overrides, durable-without-contract, and especially the whole-graph **multi-consumer ownership conflict** and owned-edge-into-retrying-node checks (which are assembly errors naming both consumers, not compile errors) — all belong to T14; this ticket asserts only the type-level ownership slice.
- The **structure-fixture** semantic diff, the **fault-injection** suite, and the artifact-schema corpus (other parts of C28) — separate later tickets.
- **Multi-toolchain / multi-platform** expected-output matrices — snapshots are pinned to the single workspace toolchain by design; cross-toolchain UI testing is explicitly not attempted.
- Any drift across the permanent scope boundary — this is a checked-in test directory plus snapshots; it introduces no scheduler, distributed runner, metadata store, web surface, DSL, runtime cycle-detection pass, or runtime graph mutation.
