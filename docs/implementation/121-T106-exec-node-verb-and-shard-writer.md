# 121 · T106 — the `exec-node` pod-side verb and attempt shard writer

> **Milestone:** M10 · **Size:** L · **Type:** feature · **Components:** C19, C26, C27
> **Branch:** `feat/t106-exec-node-verb-and-shard-writer` · **Depends on:** T104, T105 · **Blocks:** T108

## Why / context

This is the **pod side** of ADR 115: what actually runs inside the container. It is
the half of remote execution that can be built and tested with **no cluster at all** —
a subprocess invocation is indistinguishable from a pod invocation from the verb's
point of view — which is why it lands before the executor that submits it.

The design decision that makes this tractable is **re-entrancy** (ADR 115 §2): the pod
runs *the same binary*, so the graph, the task code, the resource registry, and the
codec are identical on both sides by construction. That is what lets the wire format
stay tiny — `{run_id, node, attempt, input references}` — and what makes
orchestrator-to-task version skew impossible by default rather than a matrix to
manage. `crates/cli/src/registry.rs` already recognises a `single-node` verb described
as needing a pipeline-specific body; this is that body, generalised.

Re-entrancy also solves the problem that would otherwise sink the design.
`RunContext` carries three things that cannot be serialized: `parameters:
Arc<dyn Any + Send + Sync>`, `resources: ResourceRegistry` (type-keyed live client
handles), and a live `CancellationSignal`. **None of them are transported.** The pod's
own `main()` rebuilds the registry from the same code that built the orchestrator's;
cancellation becomes pod deletion. The consequence to document, not prevent: a
resource holding a connection pool or a lock file now exists **once per pod**, not
once per run.

The **attempt shard** is the reporting channel. Rather than invent a second event
format, the shard is a fragment of the existing event stream — the same records
`AttemptEventSink` already produces, in the same JSON Lines shape, for exactly one
attempt. The orchestrator replays them into the sink the driver injected (T108), so
`seq` stays gapless and owned by the single writer, and the fold needs no new record
kind. The shard's own numbering is attempt-local and rewritten on replay.

## Objective

Add the pod-side verb, the shard format, and the input rehydration path.

- Add an **`exec-node`** verb taking `--run`, `--node`, `--attempt`, and the node's
  input references, resolving through the existing registry/`#[dag]` discovery so any
  pipeline binary gets it for free.
- **Rehydrate inputs** from blob references via T104's bridge, decode them with T103's
  codec, and run **exactly one attempt** of that node through the existing
  `run_attempt_caught` path — the same panic containment, the same
  `AttemptEvent` sequence, the same `TaskError` classification as a local attempt.
  The pod applies **no retry and no backoff**: retry is the orchestrator's (ADR 115
  §2 — two retry loops would duplicate an attempt).
- **Encode and store the output** through T104, and record its reference plus
  `DurableReferenceMeta` in the shard so the orchestrator can stamp the attempt
  record exactly as a local durable node does today.
- **Write the attempt shard** to the blob store: the attempt's `AttemptEvent` records
  in event-stream JSON Lines shape, the resulting `TerminalState`, the output
  reference and metadata, consumed input references (for M8 lineage), and diagnostic
  strings — written **atomically and last**, so a partial shard is never mistaken for
  a complete one.
- Record the input references **the pod was actually given**, in positional order, so
  they can be compared against the orchestrator's write-ahead `attempt-submitted`
  record (ADR 115 §9, written by T108). Intent and actual are separate facts; recording
  both makes "did this attempt read what we told it to?" a checkable question instead
  of an assumption. A divergence is a defect the pair of records exposes.
- Make the shard **self-identifying and verifiable**: it carries run id, node,
  attempt, both fingerprints, tool version, and image digest, so the orchestrator can
  refuse a shard from the wrong build rather than replaying it.
- **Exit codes** map to the existing `ExitCode` table with no new numbers: a task
  failure, a panic, a codec error, a missing input reference, and a fingerprint
  mismatch are each distinguishable to the orchestrator.
- Handle **SIGTERM** (pod deletion / preemption): stop promptly, write whatever shard
  is truthful — a cancelled attempt is a real outcome, not a missing one — inside the
  existing shutdown budget.

## Test plan (write these first — TDD)

**One attempt, faithfully**
- Given a node and a rehydrated input, when `exec-node` runs, then the node's body
  executes once, the output blob is written, and the shard records `succeeded`.
- Given the same node run locally in-process, then the shard's attempt records are
  **equivalent to the local ones** for the same outcome — same event kinds, same
  order, same classification.
- Given a failing task (each `TaskError` class), then the shard records the matching
  outcome and the exit code distinguishes it.
- Given a panicking task, then the shard records a panicked attempt attributed to the
  node, and the process does not abort (the panic hook and `catch_unwind` path,
  unchanged).
- Given retries configured on the node's policy, then `exec-node` still performs
  **exactly one** attempt and emits no `BackoffStarted` — retry is not the pod's job.

**Shard integrity**
- Given a completed run, then the shard is readable, and its fingerprints, tool
  version, and image digest match the binary that wrote it.
- Given the process killed mid-shard-write, then a reader detects an incomplete shard
  and does not mistake it for a complete one (the atomic-write discipline, asserted by
  fault injection).
- Given a shard written by a binary with a different structural fingerprint, then a
  reader **refuses** it and names both fingerprints.

**Inputs**
- Given a missing input blob, then `exec-node` fails with a classified error naming
  the reference — distinguishable from a task failure.
- Given an input blob whose digest no longer matches, then it fails as corrupt rather
  than decoding a wrong value.
- Given a multi-input node, then inputs are rehydrated in declared order and the
  arity matches the node's declaration.
- Given a completed attempt, then the shard records the input references the pod was
  given, in positional order with their content hashes — so a later comparison against
  the orchestrator's `attempt-submitted` record can detect a divergence.
- Given a consume-nothing node, then no input rehydration is attempted.

**Re-entrancy and resources**
- Given a pipeline whose task needs a registered resource, then `exec-node` rebuilds
  the registry through the binary's own `main()` path and the task obtains it.
- Given a resource that records its construction, then a test asserts it is
  constructed **once per `exec-node` invocation**, documenting the per-pod lifetime.

**Cancellation**
- Given SIGTERM mid-attempt, then the attempt records `cancelled`, a truthful shard is
  written, and the process exits inside the shutdown budget.

**No cluster required**
- Every test above runs by invoking the binary as a **subprocess** with a
  local-filesystem blob store — no Kubernetes anywhere in this ticket's suite.

## Definition of done

- [ ] `exec-node --run --node --attempt <input refs>` runs exactly one attempt of the
      named node through the existing attempt path, with no retry and no backoff.
- [ ] Inputs are rehydrated and decoded through T104/T103; missing, corrupt, and
      wrong-arity inputs are classified errors distinct from task failure.
- [ ] The output is encoded, stored, and named in the shard with its
      `DurableReferenceMeta`.
- [ ] The shard is event-stream-shaped JSON Lines, written atomically and last, and
      carries run/node/attempt identity, both fingerprints, tool version, and image
      digest; a mismatched fingerprint is refused with both values named.
- [ ] Shard attempt records are equivalent to the local in-process records for the
      same outcome.
- [ ] A panic is contained and attributed; SIGTERM yields a truthful `cancelled`
      shard inside the shutdown budget.
- [ ] The `ResourceRegistry` is rebuilt in-pod; a test documents its once-per-pod
      lifetime.
- [ ] Exit codes reuse the existing table with no new numbers.
- [ ] The whole suite runs via subprocess with a local blob store — no cluster.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Does `exec-node` replace or extend the existing `single-node` verb?** The existing
  verb is a registry-recognised diagnostic that replays a node from a prior run with
  rehydrated durable inputs — closely related but not identical (it is operator-facing
  and reads a run store; `exec-node` is machine-facing and reads references from
  argv). Whether they merge or stay siblings is decided in-PR, with the constraint
  that the operator-facing behaviour of `single-node` must not change.
- **Where does the shard live — the blob store or the run store?** The blob store, on
  the reasoning that it is the one place both sides provably reach, and the run store
  is local to the orchestrator. Recorded in-PR; the executor (T108) reads it either
  way through one seam.

## Out of scope

- Submitting pods, watching them, or any Kubernetes client — **T107** / **T108**.
  This verb does not know it is in a pod.
- Replaying shards into the driver's sink — **T108**.
- Retry, backoff, and timeout decisions — the orchestrator's, per ADR 115 §2 (and
  enforced by **T102** / **T108**).
- Orphan adoption and pod ownership — **T109**.
- The S3 backend — **T110**.
- Foreign-image tasks: this verb exists *because* the pod runs the same binary. A
  `PodTask` running a container dagr did not build is named future work in ADR 115.
- Scope boundary restated: a verb that runs one node in one process, reading and
  writing operator-supplied storage, coordinates nothing and serves nothing — dagr
  remains not a scheduler, a distributed execution system, a coordinating metadata
  store, a web interface, a DSL, or a backfill orchestrator, and the graph's shape
  never changes at runtime.
