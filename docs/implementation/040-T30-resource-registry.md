# 040 · T30 — C9: resource registry

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C9
> **Branch:** `feat/t30-resource-registry` · **Depends on:** T16 · **Blocks:** T33, T45, T60

## Why / context
dagr injects long-lived external clients (object storage, connection pools, HTTP clients, secret material) through a registry the developer builds by hand in `main` — the framework fetches nothing from anywhere and shares the registry immutably for the whole run. This ticket implements C9 (arch.md `### C9 · Resource registry`), building on the resource-requirement declaration plumbing that landed with T16 (C8 run context). It exists so that a pipeline declaring a resource that was never registered fails at bootstrap — naming the resource and every node requiring it — rather than surprising an operator with a mid-run `None` at three in the morning; and so that secret material carries a redaction guarantee the framework can honor. It feeds the secret-redaction acceptance work in C25 (T45), the single-task test kit (T60), and the M2 execution wiring (T33).

## Objective
Build the type-keyed immutable resource registry, its bootstrap validation against declared per-node requirements, and the secret marker wrapper — with the newtype disambiguation and fake-substitution ergonomics that make the registry testable.

Concrete pieces of work:
- A registry constructed in `main` from the developer's own code: resources are keyed by their concrete type, retrieved by type (no string lookup, the C2 philosophy), and the registry is immutable once constructed and once the run begins.
- Ambiguity rejection at construction: registering a second resource of the literally identical type fails registry construction rather than silently replacing the first; two resources of the same underlying type are distinguished by newtype wrappers.
- A bootstrap validation step that checks the registry against the declared resource requirements (surfaced from C8/T16), producing a startup failure that names the missing resource and every node requiring it, and that produces the bootstrap-failure artifact — before any node executes.
- Declared resource requirements exposed so they appear in the graph artifact (consumed downstream; this ticket surfaces them, does not render the artifact).
- A `Send + Sync + 'static` bound on stored resources, with the owning-worker channel pattern documented as the escape hatch for non-thread-safe clients (documentation only — no worker implementation here).
- A secret marker wrapper with no `Debug`/`Display` path, plus the sentinel-based redaction test hook that later tickets (C25/T45) build on.
- Fake substitution: any resource replaceable by a fake in a test without modifying task code.
- Rustdoc documenting the newtype disambiguation pattern with a worked example and the secret-guarantee boundary (a task author formatting a secret into their own log line is outside the guarantee — stated in C25).

## Test plan (write these first — TDD)
- **Retrieve by type.** Given a registry constructed with one resource of a given concrete type, when a caller retrieves that type, then it gets back the same resource with no string key and no runtime type mismatch.
- **Ambiguous duplicate rejected.** Given a registry builder that has already accepted a resource of type `T`, when a second resource of the literally identical type `T` is registered, then registry construction fails with an ambiguity error and neither silently replaces the first nor keeps both — verified by asserting the error and that no registry is produced.
- **Newtype disambiguation succeeds.** Given two resources of the same underlying type each wrapped in a distinct newtype, when both are registered, then construction succeeds and each is retrievable independently by its newtype — the documented pattern for two same-typed resources.
- **Immutable after construction.** Given a constructed registry, when the run has begun, then there is no API by which the registry contents can be mutated — verified structurally (the type exposes no mutation path) and, where applicable, by a test that the shared registry handle offers read-only access only.
- **Missing declared resource fails at bootstrap, before execution.** Given a pipeline whose nodes declare a resource type that was never registered, when bootstrap validates the registry against declared requirements, then bootstrap fails before any node executes, and the error names both the missing resource type and every node that declared a requirement on it — verified by asserting the named resource and the exact set of requiring node identifiers.
- **Bootstrap-failure artifact produced on missing resource.** Given the missing-resource condition above, when bootstrap fails, then the bootstrap-failure artifact is produced (outcome distinct from an assembly failure) — verified by asserting the artifact exists and carries the bootstrap-failure outcome with the resource-validation error in its error list, and that zero attempts were recorded.
- **All requirements satisfied passes.** Given a registry that contains every declared resource type, when bootstrap validates, then validation passes with no error and execution is allowed to proceed.
- **Declared requirements are surfaced.** Given nodes that declared resource requirements, when the requirements are read for artifact emission, then every declared requirement (resource type name, requiring node) is present in the surfaced set — verified against the declared inputs, so a downstream graph-artifact test can assert they appear.
- **Fake substitution needs no task change.** Given task code written against a resource type, when a test constructs a registry containing a fake of that type instead of the real one, then the task retrieves and uses the fake with no modification to the task code — verified by exercising the task against the fake.
- **Secret wrapper has no `Debug`/`Display`.** Given a secret marker wrapper holding a sentinel value, this is enforced at compile time (the wrapper implements neither trait); the test plan records this as a `trybuild`-style compile-fail check that formatting the wrapper with the debug or display formatter fails to compile, alongside a runtime check that the wrapper still yields its inner value to authorized access.
- **Sentinel redaction.** Given a secret wrapping a unique sentinel string, when the registry contents are handled through any framework-controlled serialization or emission path exercised here, then the sentinel never appears in the output — verified by searching the emitted bytes for the sentinel and asserting absence. (Framework-emitted log-line redaction is completed under C25/T45; this ticket establishes the wrapper and the sentinel hook.)
- **Thread-safety bound holds.** Given the stored-resource bound, this is a compile-time guarantee (`Send + Sync + 'static`); recorded as a compile-fail check that a non-`Send`/non-`Sync` type cannot be registered, with the owning-worker pattern referenced in the error-adjacent docs.

## Definition of done
- [ ] The registry is built once in the developer's own code and is shared immutably for the run; the framework fetches nothing from anywhere to populate it.
- [ ] Resources are retrieved by type with no string lookup and no runtime type check on the happy path (C2 philosophy).
- [ ] Registering a second resource of the literally identical type fails registry construction as ambiguous rather than silently replacing the first.
- [ ] Two same-typed resources are distinguishable via the newtype pattern, demonstrated in a documented example (rustdoc).
- [ ] The registry cannot be mutated once the run begins (no mutation path exposed).
- [ ] Stored resources are `Send + Sync + 'static`; the owning-worker channel pattern for non-thread-safe clients is documented.
- [ ] A run whose pipeline declares an unregistered resource fails at bootstrap, before any node executes, naming both the missing resource and the nodes requiring it.
- [ ] That bootstrap failure produces the bootstrap-failure artifact (distinct from assembly failure), with the validation error in its error list and zero attempts recorded, and never hangs.
- [ ] Declared resource requirements are surfaced from C8/T16 so they can appear in the graph artifact.
- [ ] Any resource can be replaced with a fake in a test without modifying task code.
- [ ] Secrets are marked via a wrapper with no `Debug`/`Display` path (enforced by a compile-fail check).
- [ ] Marked secret values never appear in framework-controlled output paths exercised here, verified by a planted-sentinel test; the guarantee boundary (a task author formatting a secret into their own log line is outside it, per C25) is documented.
- [ ] Rustdoc covers: newtype disambiguation worked example, the owning-worker pattern reference, and the secret-guarantee boundary.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Rendering the graph artifact or the run/bootstrap-failure artifact itself — this ticket only surfaces declared requirements and asserts the bootstrap-failure artifact is produced; artifact schema and emission are C20/C22 work.
- Framework-emitted log-line redaction end to end and the human/structured logging surface — completed under C25 (T45); this ticket provides the secret wrapper and the sentinel hook only.
- The admission controller and capacity/pool sizing at bootstrap (C12) — a separate bootstrap check; do not couple resource validation to capacity validation here beyond both being fail-fast bootstrap steps.
- Implementing the owning-worker thread pattern for non-thread-safe clients — documented as guidance only.
- The durable scratch store and any registry-backed persistence (C18/T53); the registry holds live clients, not state.
- Any move toward a lookup service, a per-task credential fetch, a network round trip to locate resources, a metadata store, or runtime-mutable registration — this is dependency injection built in `main`, and the scope boundary forbids the hosted-scheduler connections-and-variables pattern.
