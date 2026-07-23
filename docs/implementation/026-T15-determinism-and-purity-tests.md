# 026 · T15 — C7: determinism and purity tests

> **Milestone:** M1 · **Size:** S · **Type:** feature (tests) · **Components:** C7
> **Branch:** `feat/t15-determinism-and-purity-tests` · **Depends on:** T14 · **Blocks:** T28, T40

## Why / context
Assembly (C7) is the load-bearing purity boundary of the whole tool: it collects registrations into an immutable pipeline and computes what the runtime needs (consumer counts, dependency counts, execution order, fingerprint) with **no network, no filesystem, no clock, no credentials, and no parameter values** (arch.md "The shape of a run" step 1; C7 §Behavior). That purity is exactly what lets the graph be emitted and validated in CI on every pull request (C20). T14 implemented the validation and precomputation; this ticket adds the test suite that *proves* the two properties T14 promised but a builder cannot self-certify: **determinism** (assembling the same pipeline twice in one process yields byte-identical graph output, the generation-time field aside) and **purity** (assembly succeeds in a fully empty environment and never touches a parameter value). It locks these as regression guards before T40 emits the real graph artifact and before the T28 M1 demo depends on stable graph output. This ticket writes tests only — it adds no assembly behavior.

## Objective
Add a focused test suite that pins the C7 determinism and purity acceptance criteria and answers the open mechanical-proof question with a chosen, documented convention.

Concrete pieces of work:
- A **determinism** test group asserting that two in-process assemblies of the same pipeline produce byte-identical serialized graph output, with the generation-time field masked/excluded from the comparison (per C20).
- A determinism test proving the byte-identity holds independent of registration *order* where order is not semantically meaningful, and that the precomputed data (consumer counts, remaining-dependency counts, execution order, fingerprint slot) is identical across the two assemblies.
- A **purity** test that runs a real assembly in an **empty environment** — no configuration, no environment variables the code reads, no filesystem paths present, no network — and asserts it succeeds and produces its output.
- A test proving **no parameter value is reachable during registration or assembly**: assembly of a parameterised pipeline completes without any parameter being supplied.
- A decision record in the ticket resolving how no-filesystem/no-network is mechanically proven (sandbox vs. syscall audit vs. review convention), and a test/CI harness that enforces the chosen mechanism.
- A small shared test fixture: a canonical multi-node pipeline (data edges, an ordering-only edge, at least one group label, and a parameter struct) reused by the scenarios below.

## Test plan (write these first — TDD)

**1. Byte-identical output across two in-process assemblies.**
Setup: build the canonical fixture pipeline once in the test binary. Action: assemble it twice in the same process and serialize each resulting graph to bytes, masking only the generation-time field. Expected: the two byte sequences are equal; the test fails loudly if any non-generation-time byte differs.

**2. Generation-time is the *only* permitted difference.**
Setup: two in-process assemblies of the fixture. Action: compare the two serialized outputs *without* masking generation time. Expected: they differ only in the generation-time field (all other bytes equal), confirming that the mask is the sole exclusion and nothing else is non-deterministic (guards against accidentally masking real drift).

**3. Registration order does not change the artifact.**
Setup: two builders registering the same node set and wiring but in different registration orders (where identity is the explicit registration name, so order is not semantic — C7/T13). Action: assemble both and serialize (generation time masked). Expected: byte-identical output — canonical ordering is applied, not registration order.

**4. Precomputed runtime data is identical across assemblies.**
Setup: the fixture pipeline. Action: assemble twice and inspect the precomputed values — per-node consumer count, remaining-dependency count, execution order, and the fingerprint slot. Expected: every value is identical between the two assemblies, and consumer counts are exact for every node (matching the hand-computed expectation for the fixture).

**5. Assembly succeeds in an empty environment.**
Setup: launch assembly with every external resource absent — no config file present, the reader-visible environment variables unset, the working directory pointed at a location with no expected files, and no network reachable. Action: assemble the fixture pipeline. Expected: assembly returns success and produces its graph output; no error, no panic, no hang.

**6. No parameter value is reachable during assembly.**
Setup: a fixture pipeline that declares a typed parameter struct. Action: assemble it *without supplying any parameter values* (as bootstrap has not run). Expected: assembly completes successfully; the test demonstrates there is no assembly-time API by which a node body or the assembler can read a parameter value (parameters are parsed only at bootstrap, after assembly — C7 §Behavior).

**7. Mechanical no-filesystem / no-network proof (the chosen convention, per the resolved open question).**
Setup: run assembly of the fixture under the mechanism selected in the decision record below. Action: assemble. Expected: assembly completes and the enforcement mechanism reports zero disallowed filesystem or network operations during the assembly window; introducing a deliberate stray filesystem/network call inside a throwaway assembler variant makes this test fail (negative control kept as a documented, ignored/`#[should_panic]`-style demonstration or removed after proving the guard bites).

**8. Empty-environment determinism, combined.**
Setup: empty environment (scenario 5 conditions). Action: assemble the fixture twice within that environment and compare bytes (generation time masked). Expected: byte-identical — determinism and purity hold together, which is the exact CI/PR condition C20 relies on.

## Definition of done
- [ ] A test proves assembling the same pipeline twice in one process produces byte-identical graph output, with only the generation-time field excluded (C7 AC: byte-identical twice; C20 AC: identical bytes outside generation time).
- [ ] A test proves the generation-time field is the *sole* difference between two in-process assemblies (nothing else is masked or non-deterministic).
- [ ] A test proves registration order does not alter the serialized output (canonical ordering, not registration order).
- [ ] A test proves consumer counts are exact for every node and that all precomputed runtime data (consumer counts, remaining-dependency counts, execution order, fingerprint slot) is identical across two assemblies (C7 AC: consumer counts exact before execution).
- [ ] A test proves assembly succeeds with every external resource absent, running it in an empty environment (C7 AC: assembly succeeds with every external resource absent; C20 AC: producible in an empty environment with no configuration).
- [ ] A test proves no parameter value is reachable during registration or assembly — assembly of a parameterised pipeline completes with no parameters supplied (C7 AC: no parameter value reachable during registration or assembly).
- [ ] The open question is resolved in a short decision record embedded in this ticket file, selecting the mechanical proof mechanism for no-filesystem/no-network, with the rationale and its limits stated.
- [ ] The chosen mechanism is implemented as a test/CI harness that fails when a disallowed filesystem or network operation occurs during assembly, demonstrated to bite via a documented negative control.
- [ ] A shared, reusable test fixture pipeline (data edges, one ordering-only edge, ≥1 group label, a typed parameter struct) exists and is used by the scenarios above.
- [ ] All new tests are deterministic and hermetic — they set up and tear down their own environment and do not depend on host configuration.
- [ ] These tests exercise only public/observable behavior of the C7 assembly surface; no assembly logic is added or changed in this ticket.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
- Mechanical proof of no-filesystem/no-network — sandboxing, syscall audit, or review convention? Resolve in this ticket's decision record: pick the mechanism, state why (portability across the CI matrix, signal strength, maintenance cost), and note what it does and does not catch. The graph-artifact tests (T40) and the criteria-matrix CI job (referenced by tasks.md, structural-determinism check) will reuse this convention, so the choice must be reusable, not one-off.

## Out of scope
- Any change to assembly validation or precomputation itself — that is T14, already landed; this ticket is tests only.
- Graph-artifact **emission**, its versioned header, build provenance, and schema validation — that is C20 / T40 (which this ticket blocks). These tests assert byte-identity of assembly output but do not define the on-disk artifact format or its published schema.
- Fingerprint algorithm content, canonical ordering rules, and cross-toolchain fingerprint stability — that is C21 / T41; this ticket only checks that the fingerprint *slot* is identical across two in-process assemblies, not how the hash is composed.
- Bootstrap-phase behavior: parameter parsing/validation, capacity and resource checks, credential probing — all deliberately impure and out of C7 (arch.md "The shape of a run" step 2). Do not assert bootstrap outcomes here.
- Cross-machine / cross-toolchain determinism runs — those belong to the C21 fingerprint tests and the CI criteria matrix, not to this in-process suite.
- Scope-boundary temptations to guard against: do not let the "prove purity" work drift into building a scheduler hook, a metadata store, a runtime graph-mutation path, or a parameter-injection-at-assembly escape hatch. dagr is not a scheduler, distributed executor, metadata store, DSL, or backfill orchestrator, and the graph shape never changes at runtime — the purity tests exist precisely to keep that boundary enforceable.
