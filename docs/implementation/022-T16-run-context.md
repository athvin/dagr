# 022 · T16 — C8: run context

> **Milestone:** M1 · **Size:** M · **Type:** feature · **Components:** C8
> **Branch:** `feat/t16-run-context` · **Depends on:** T9 · **Blocks:** T20, T30, T44, T53, T60

## Why / context
This ticket defines the read-only handle (`RunContext`) that every task invocation is handed — the single object that tells a task everything it may know about the run it is part of (arch.md C8 · Run context). It builds on T9 (C1 task abstraction), whose task signature receives this context, and it is the anchor for a large slice of M1/M2: the attempt runner (T20/C14) constructs and threads it, the resource registry (T30/C9) and durable scratch store (T53/C18) fill in the two additive accessor APIs, node metrics (T44/C23) reads identity/attempt fields, and the single-task test kit (T60/C28) leans on the hand-constructability guarantee. This ticket must lock the context's shape and its no-authority contract now, because five downstream tickets wire against it; it deliberately lands the *seams* for registry and scratch access even though the substance of those seams arrives with C9 and C18. It also carries the resource-requirement *declaration* plumbing that feeds T30's bootstrap validation.

## Objective
Build the run context: a read-only, hand-constructable handle passed into every task invocation, carrying all fields the spec enumerates, plus the resource-requirement declaration plumbing that later feeds registry validation. Concretely:

- Define the `RunContext` type carrying: run identity, pipeline identity, node identity, current attempt number and the configured maximum, the run's parameters, an optional caller-supplied opaque data interval, a cancellation signal, a logging span, an accessor for the resource registry, and an accessor for the durable scratch store.
- Define the data interval as a caller-supplied, tool-opaque pair of values recorded verbatim — no framework code path parses, computes, advances, or persists it.
- Expose only read accessors: no API on the context may mutate the graph, reorder work, register or rescind resources, or influence scheduling. There is no route from the context back to the runtime/scheduler.
- Land the resource-registry and scratch accessors as *additive seams* — the type signatures and their placement exist now; the concrete behaviour arrives with T30 (C9) and T53 (C18). Where the substance is not yet available, the seam is stubbed in a way that is honestly unimplemented (not silently wrong) and clearly marked for those tickets, without weakening the fields already present.
- Land the resource-requirement declaration plumbing: the mechanism by which a node records the resource types it requires at registration, carried so bootstrap (T30) can validate the registry against declared requirements and so those declarations can later surface in the graph artifact.
- Provide a hand-construction path (a builder or explicit constructor) usable in a plain unit test with no runtime, no store, and no registry running, so a single task can be exercised in isolation.
- Provide the teardown-only extension (C17): a teardown node's context additionally exposes the terminal states of the nodes it covers, so cleanup can no-op when setup never ran. This ticket defines the shape of that extension and how a non-teardown context reflects its absence; the wiring of covered-node states from the runtime is completed with C17's ticket.

## Test plan (write these first — TDD)
Every scenario below is a unit test that constructs a `RunContext` by hand — no runtime, no store, no registry, no scheduler.

- **All fields populated on a hand-built context.** Setup: construct a context with distinct, recognizable values for every field (run id, pipeline id, node id, attempt = 1, max attempts = 3, a parameters value, a data interval pair, a cancellation signal, a span). Action: read each accessor. Expected: every accessor returns exactly the supplied value; no field is absent, defaulted-away, or panics. This directly exercises the "every field is populated on every invocation, including the first attempt of the first node" criterion.

- **Attempt number is readable and reflects the supplied attempt.** Setup: construct one context with attempt = 1 / max = 3 and a second with attempt = 2 / max = 3. Action: read the attempt-number and max accessors on each. Expected: the first reports 1, the second reports 2, both report max 3 — demonstrating the field carries the retry count that logs and artifacts will later consume.

- **Data interval is carried verbatim and never interpreted.** Setup: construct a context whose data interval is an arbitrary opaque pair, including values that would be nonsensical if the framework tried to parse them as timestamps or ranges (e.g. reversed order, identical endpoints, empty/sentinel content). Action: read the data interval accessor. Expected: the exact pair is returned unchanged; construction and reading never inspect, order, validate, or normalize the contents. This is the concrete check that "no framework code path interprets its contents."

- **Optional data interval absence is representable.** Setup: construct a context with no data interval. Action: read the data interval accessor. Expected: it reports absence cleanly (the optional is empty), and no field elsewhere is affected.

- **The context exposes no mutation or scheduling authority.** Setup: inspect the public surface of `RunContext` (this is partly a compile-time/API-shape assertion realized as tests over the accessors that do exist). Action: enumerate every public method. Expected: every one is a read; there is no method that mutates the graph, reorders work, registers/rescinds a resource, or reaches the scheduler/runtime. A throwaway assertion test confirms the accessors return values and hold no `&mut self` mutating API for graph/scheduling state.

- **Hand-construction requires no runtime.** Setup: none beyond the constructor/builder. Action: build a context and invoke a trivial task-shaped closure with it, entirely within the test, with no store opened and no registry built. Expected: the closure runs and observes the context; nothing in the construction path touches the filesystem, the clock, the network, or a registry. This is the "constructed by hand in a unit test … with no runtime running" criterion.

- **Resource-requirement declaration is carried through to a queryable form.** Setup: register (in a test fixture) a node that declares it requires a particular resource type; construct the plumbing this ticket owns. Action: query the declared requirements for that node. Expected: the declared resource type is reported, in a form bootstrap (T30) can later validate against a registry and that a graph artifact can later render. A node declaring nothing reports an empty requirement set.

- **Registry accessor seam is present and honestly unimplemented.** Setup: construct a context. Action: reach for the registry accessor. Expected: the accessor exists with a stable signature, and — because C9 is not landed here — either returns a clearly-empty/no-op registry handle or a documented not-yet-available result, never a silently-wrong resource. A comment/marker ties the seam to T30. (This test is updated when T30 lands to assert real retrieval.)

- **Scratch accessor seam is present and honestly unimplemented.** Setup: construct a context. Action: reach for the scratch-store accessor. Expected: the accessor exists with a stable signature and, because C18 is not landed here, surfaces a documented not-yet-available result rather than pretending to persist. A marker ties the seam to T53. (This test is updated when T53 lands to assert read-after-write across attempts.)

- **Teardown context exposes covered-node terminal states; a normal context does not claim to.** Setup: construct a teardown-flavoured context that covers two nodes with supplied terminal states (e.g. one `succeeded`, one `skipped`), and separately a non-teardown context. Action: read the covered-terminal-states view on each. Expected: the teardown context reports exactly the supplied terminal states (drawn from the normative taxonomy) so cleanup can no-op when setup never ran; the non-teardown context reports the absence of any covered set. (The runtime-side population of covered states is finished under C17.)

- **Cancellation signal is observable but read-only from the task's side.** Setup: construct a context with a cancellation signal that the test can flip. Action: read the signal before and after the test flips it. Expected: the task-facing side observes the change but has no method to cancel the run itself — the signal is an observation channel, not a lever, consistent with "no route back to the scheduler."

## Definition of done
- [ ] `RunContext` is a read-only handle passed into every task invocation and carries: run identity, pipeline identity, node identity, current attempt number, maximum attempts, run parameters, an optional data interval, a cancellation signal, a logging span, resource-registry access, and durable-scratch access (C8 behavior).
- [ ] Every field is populated on every invocation, including the first attempt of the first node — no field can be silently absent (C8 acceptance).
- [ ] The attempt number is exposed and carries the retry count in a form logs and artifacts consume; it is not fixed or defaulted (C8 acceptance).
- [ ] No context API can modify the graph, reorder work, or influence scheduling; the context holds no mutable shared state and offers no route back to the scheduler (C8 behavior + acceptance).
- [ ] The data interval is a caller-supplied, tool-opaque pair recorded verbatim; no framework code path computes, advances, persists, or interprets its contents (C8 behavior + acceptance).
- [ ] A `RunContext` can be constructed by hand in a unit test — no runtime, no store, no registry, no clock, no network — so a single task can be exercised in isolation (C8 acceptance; feeds T60).
- [ ] The resource-registry accessor exists as a stable seam; it is honestly unimplemented (empty/no-op or documented not-yet-available) and marked for T30, never silently wrong (additive API landing with C9).
- [ ] The durable-scratch accessor exists as a stable seam; it is honestly unimplemented and marked for T53, never pretending to persist (additive API landing with C18).
- [ ] Resource-requirement declaration plumbing exists: a node's declared required resource types are recorded at registration and are queryable in a form bootstrap can validate against a registry and that a graph artifact can later render (feeds T30; supports C9 bootstrap validation).
- [ ] A node that declares no requirements reports an empty requirement set; declarations are additive and do not affect a context's other fields.
- [ ] The teardown-only extension is defined: a teardown node's context additionally exposes the terminal states (from the normative taxonomy) of its covered nodes so cleanup can no-op when setup never ran; a non-teardown context reflects the absence of any covered set (C8 behavior; C17 completes runtime population).
- [ ] The cancellation signal is observable from the task side but exposes no lever to cancel the run from within the context.
- [ ] Every seam left for a later ticket carries an inline marker naming the blocking ticket (T30 for registry, T53 for scratch, C17 for covered-node states).
- [ ] Rustdoc documents `RunContext`, the opaque/verbatim nature of the data interval (including that this is the boundary with "backfill orchestrator"), and the hand-construction path for tests.
- [ ] All Test plan scenarios above are implemented as tests and pass.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None in the ticket; tasks.md pre-resolved the substantive ones (opaque data
interval, additive registry/scratch seams, resource-requirement plumbing). Two
implementation judgment calls are recorded here for audit:

- **Context-exposed types are dagr-owned, keeping `dagr-core` dependency-free.**
  Per the T2 async-runtime ADR (004) — "context-exposed types are dagr-owned
  wherever practical; the cancellation signal is a dagr-owned wrapper, never a
  bare tokio/`tokio-util` type" — the cancellation signal (`CancellationSignal`
  over a shared flag, flipped by a runtime/test-held `CancellationSource`), the
  `LogSpan`, and the identity newtypes (`RunId`, `PipelineId`) are all
  dagr-owned. No tokio/tracing dependency is added; the real token/span/tracing
  backing is wired by the runner and logging tickets (T20/T21/T35, C25) without
  changing this task-facing surface. Parameters are carried opaquely as
  `Arc<dyn Any + Send + Sync>` and read back by type (typed parsing is bootstrap,
  C7/C26).
- **`NodeId::from_name` made public.** Hand-constructing a teardown context names
  the covered nodes, and a teardown developer identifies them by author-declared
  name. `from_name` is a pure name-derived identity token that manufactures no
  `Handle` and consults no registry, so exposing it does **not** weaken C2's
  unforgeable-`Handle`/no-lookup contract (documented at the function). It does
  not re-decide C2; it uses the same identity C2 already mints.

## Out of scope
- The concrete resource registry (C9) — construction, type-keyed retrieval, newtype disambiguation, ambiguity failure, secret wrapping, and bootstrap validation of the registry against declared requirements — all land in **T30**. This ticket lands only the accessor seam and the declaration plumbing.
- The concrete durable scratch store (C18) — key-value persistence, run/node namespacing, read-after-write across attempts, resume copy-forward, and success-time cleanup — lands in **T53**. This ticket lands only the accessor seam.
- Constructing and threading the context inside a real run, attempt sequencing, and retry increment logic belong to the attempt runner (**T20 / C14**), not here.
- Surfacing declared requirements *into the graph artifact* is graph-artifact work (C20); this ticket only makes the declarations queryable.
- Emitting the attempt number and identities into logs and artifacts belongs to node metrics (**T44 / C23**) and the event stream / artifacts components; this ticket only exposes the fields.
- Runtime population of covered-node terminal states, teardown ordering, fresh-signal/deadline behaviour — the substance of teardown — is **C17**, not this ticket.
- Scope-boundary temptations to name and refuse: **the context must never compute, advance, or persist a data interval** (that would make dagr a backfill orchestrator — the caller loops over intervals, not the tool); the context must never expose a lever to change the graph shape at runtime, reorder work, or reach back into scheduling (dagr is not a scheduler and the graph shape never changes at runtime); and the context is not a metadata store or lookup service — resources arrive by type from a registry the developer built, never fetched from anywhere by the framework.
