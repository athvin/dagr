# 073 · T60 — C28: single-task test kit

> **Milestone:** M4 · **Size:** M · **Type:** feature · **Components:** C28
> **Branch:** `feat/t60-single-task-test-kit` · **Depends on:** T16, T30 · **Blocks:** T62

## Why / context
This ticket lands the first of C28's three testing levels (arch.md `### C28 · Testing surface`): the ability to exercise a *single task* with a hand-built run context and fake resources, with no live network and no database, shipped inside the library so no pipeline ever writes its own single-task harness. It builds directly on the hand-constructable run context from C8 (arch.md `### C8 · Run context`, whose acceptance criterion already promises a by-hand context so a task can run with no runtime) and the fake-substitution and secret-marker registry from C9 (arch.md `### C9 · Resource registry`, whose criterion already promises any resource can be replaced with a fake without touching task code). The load-bearing decision this ticket locks is the *shape of the test kit's public surface* — how a caller assembles a context, injects fakes, and drives a synchronous task with no async runtime while an await-bound task gets a provided test runtime — so that T62's full-pipeline fakes harness can build the whole-run level on the same seam rather than reinventing it.

## Objective
Ship a library-provided single-task test kit that lets a caller invoke exactly one task, with a hand-built run context and fake resources, and observe its outcome — proving the synchronous path needs no async runtime and the await-bound path needs only the runtime the kit provides.

- Provide a builder for a hand-constructed run context that populates every C8 field (run identity, pipeline identity, node identity, current attempt number and the maximum, run parameters, optional data interval, cancellation signal, logging span, resource-registry access, scratch access), with ergonomic defaults so a caller sets only what a given test cares about.
- Let the caller inject the resource registry the task will see, built from fakes via C9's existing fake-substitution path, so no task code changes between production and test.
- Provide a way to drive a *synchronous* task's work to completion with no async runtime present at all, and a distinct way to drive an *await-bound* task using a plain test runtime the kit supplies (the caller never stands up their own runtime).
- Surface the task's returned outcome (produced output or classified error) to the caller for assertion, and make the constructed attempt number observable in that outcome path so retry-shaped tests can vary it.
- Ship a runnable example test that demonstrates the kit end to end: a task exercised against a fake resource with a hand-built context, one synchronous case and one await-bound case.
- Keep the kit's context strictly read-only and inert with respect to scheduling — constructing or using it must offer no route to a scheduler, no graph mutation, and no reordering (C8's invariant).

## Test plan (write these first — TDD)
Every scenario is independently checkable. These are behavioral tests exercising the shipped kit; the example test doubles as documentation.

- **Synchronous task, no runtime.** Setup: define a synchronous task and build a run context by hand with the kit, injecting a fake resource. Action: drive the task through the kit's synchronous entry point inside a test binary that starts no async runtime. Expected: the task's work runs and returns its produced value; the test compiles and passes with no runtime attribute, no runtime handle, and no network or database available — asserted by the fact that the test process performs no I/O and the fake is the only resource reachable.
- **Await-bound task, provided runtime only.** Setup: define an await-bound task and build a context by hand. Action: drive it through the kit's await-bound entry point, relying solely on the test runtime the kit provides. Expected: the task completes and returns its produced value; the caller wrote no runtime setup of their own, and removing the caller-side runtime setup (there is none) does not change the result.
- **Every context field is populated.** Setup: build a context with the kit using only defaults. Action: read each C8 field the task can see (run identity, pipeline identity, node identity, current attempt, max attempts, parameters, data interval, cancellation signal, logging span, registry handle, scratch handle). Expected: every field is present and readable, including on a first-attempt-of-first-node default — no field is absent or panics on access.
- **Caller-supplied fields are honored.** Setup: build a context setting non-default values for run parameters, node identity, and the attempt number. Action: run a task whose work reads those fields and returns them. Expected: the task observes exactly the supplied values; changing the supplied attempt number changes what the task reads, so a retry-shaped test can drive attempt 2 by hand.
- **Data interval round-trips verbatim.** Setup: build a context supplying a specific opaque data-interval pair. Action: run a task that returns the interval it was given. Expected: the returned pair is byte-for-byte the supplied pair; no kit code path interprets, advances, or normalizes it (C8's opaque-interval invariant).
- **Fake resource is retrieved by type with no task change.** Setup: register a fake implementation of a resource in the injected registry. Action: run a task that retrieves that resource by type and uses it. Expected: the task receives the fake and uses it with no modification to the task's own code versus production; the fake's recorded interactions are observable to the test.
- **Two same-typed resources via newtypes.** Setup: inject two fakes of the same underlying type distinguished by newtype wrappers into the registry the context exposes. Action: run a task that retrieves both by their newtypes. Expected: each newtype resolves to its own fake and they are not confused (mirrors C9's documented newtype disambiguation, exercised through the kit's context).
- **Secret fake stays redacted.** Setup: inject a resource marked secret carrying a planted sentinel value into the kit's registry. Action: run a task through the kit and capture the kit's own emitted output (context/span diagnostics the framework controls). Expected: the sentinel never appears in any kit- or framework-emitted output path, consistent with C9's redaction guarantee — the kit does not open a new leak.
- **Classified error surfaces to the caller.** Setup: define a task arranged to fail with a retry-eligible (and separately, a permanent) classified error. Action: drive it through the kit. Expected: the kit returns the error variant, not an output, and the caller can read the classification to assert on it.
- **Cancellation signal is observable.** Setup: build a context whose cancellation signal is pre-tripped. Action: run a task whose work checks the signal. Expected: the task observes the cancelled state through the context; a context built without tripping the signal presents it un-tripped.
- **Context is inert toward scheduling (surface check).** Setup: build a context with the kit. Action: review the context's public surface against a checklist. Expected: no method modifies the graph, reorders work, or reaches a scheduler; validated by the acceptance-criterion checklist item plus the absence of any such method on the type (no runtime assertion needed).
- **No pipeline writes its own single-task harness (documentation-backed).** Setup: take the shipped example test. Action: review it against a checklist. Expected: the context construction, fake injection, and task driving are all done through library-provided kit APIs — the example writes no bespoke harness scaffolding of its own.

## Definition of done
- [ ] A single-task test of a synchronous task requires no async runtime, no network, and no database — driven entirely through the library-provided kit (C28).
- [ ] A single-task test of an await-bound task needs only the test runtime the kit provides; the caller stands up no runtime of their own (C28).
- [ ] The kit builds a run context by hand that populates every C8 field, including on the first attempt of the first node, so a single task can be exercised with no runtime running (C8).
- [ ] The attempt number is caller-settable on the hand-built context and is read back unchanged by the task, so a retry-shaped single-task test can drive a specific attempt (C8).
- [ ] The data interval appears to the task exactly as supplied and no kit code path interprets its contents (C8).
- [ ] The hand-built context exposes no API that can modify the graph, reorder work, or influence scheduling (C8).
- [ ] Any resource the task retrieves can be a fake injected through the kit's registry without modifying task code (C9).
- [ ] Two same-typed resources are distinguishable via the newtype pattern when retrieved through the kit's context, exercised in a test (C9).
- [ ] A secret value marked in the injected registry never appears in kit- or framework-emitted output, verified by a planted-sentinel test (C9).
- [ ] A task's classified error surfaces to the caller as an error (not an output) with its classification readable for assertion (C28 single-task level).
- [ ] The kit is shipped with the library, not rebuilt per pipeline, and the example demonstrates that no pipeline needs to write its own single-task harness (C28).
- [ ] A runnable example test demonstrates the kit end to end against a fake resource with a hand-built context, covering both a synchronous and an await-bound task (C28; Documentation — runnable examples covering each layer).
- [ ] The T62 seam is preserved: the context-construction and fake-injection surface this kit exposes is the one the full-pipeline harness can reuse, with no single-task-only assumptions foreclosing the whole-run level.
- [ ] Behavioral scenarios are unit/integration tests and the acceptance-criteria coverage matrix (T7) is updated to map each C28 single-task-level criterion (and the C8/C9 criteria this ticket realizes) to its test.
- [ ] Rustdoc on the kit's public surface documents how to build a context, inject fakes, and drive both task classes, and the crate's rustdoc lint passes.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The structure-snapshot testing level of C28 — semantic node/edge/policy comparison, structural-diff output, and the blessed single-command fixture regeneration (C28 / T61). This ticket is the single-task level only.
- The full-pipeline fakes harness that executes an end-to-end run on the real scheduler against fakes with the completes-in-seconds budget (C28 / T62), which *depends on* this ticket and reuses its context/fake seam.
- The framework's own fault-injection suite — kill-points, disk-full, slow/failing sinks (C28's self-testing clause) — covered elsewhere, not by this single-task kit.
- The compile-fail / UI error-message tests pinned to the workspace toolchain (C28's library-internal clause; T8 harness) — not part of the single-task runtime kit.
- Building or fetching real resources: the registry construction in `main`, bootstrap validation against declared requirements, and the framework's fetch-nothing rule are C9/T30's job; this ticket only *consumes* the fake-substitution path.
- Any admission, permit accounting, timeout, retry-loop, or scheduling behavior (C11–C16) — a single-task invocation runs one task's work directly and asserts nothing about pool sizing or run-level orchestration.
- Any move toward runtime-mutable graph shape, a scheduler, distributed execution, a metadata store, a web UI, a DSL, or backfill orchestration — permanently outside dagr.
