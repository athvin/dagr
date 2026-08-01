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

## Open questions — resolved

`docs/tasks.md` carries **no `T106` entry** (it enumerates M0–M4 only), so there are
no `Q:` items beyond this file's own two. Both are answered below, together with the
decisions the implementation had to make that the ticket did not name.

### 1. Does `exec-node` replace or extend `single-node`? → **Siblings. `single-node` is untouched.**

They answer different questions for different callers, and merging them would make
each worse. `single-node` is **operator-facing**: it replays node N *from run R*,
reads the prior run's artifact out of the run store, and refuses on a non-durable
input with the resume-refusal code. `exec-node` is **machine-facing**: it takes the
references on argv, needs no prior run at all, and reports through a shard rather
than an artifact. A merged verb would have to guess which mode it was in from which
flags were present — precisely the ambiguity a closed verb table exists to avoid.

Nothing about `single-node` changed: `single_node_refusal_check`, its exit code, its
`not-requested` artifact marking, and its registry diagnostic are byte-for-byte what
they were. `exec-node` is a new member of the verb table (`crates/cli/src/contract.rs`)
and routes through the same flow-selection rules every other flow-selecting verb uses,
so a `#[dag]` binary gets it for free.

### 2. Where does the shard live — the blob store or the run store? → **The blob store, at a deterministic attempt-keyed path.**

As the ticket reasons: the blob store is the one place both sides provably reach, and
the run store is local to the orchestrator. The consequence the ticket does not spell
out is that the shard needs an address the orchestrator can compute **without being
told** — there is no callback, so "the pod writes it and tells us where" is not
available. The port's `put` is content-addressed and therefore cannot supply a name a
reader can derive in advance.

The shard is therefore written to `attempt-shards/<run>/<sha256(node)>/<attempt>.jsonl`
under the blob container (`crate::shard::shard_path`), a pure function of the identity
triple that both sides call. The node name is addressed by its **digest**, for the same
reason ADR 115 §4 puts identity in annotations rather than labels: a node name is
author-chosen and need not be a legal path segment. The full name is recorded inside
the header, so nothing is lost. Attempt and run are both in the address, so a retry
never overwrites its predecessor's record.

### 3. What shape is the shard, and what makes a partial one detectable? → **Header + event-stream records + trailer, written atomically and last.**

The body is the event stream's own records, produced by the *same* translation the
driver uses (`crate::shard::records_for` drives `driver::write_attempt_event`), which
is what makes "equivalent to the local records" true by construction rather than by
review. Around it sit a header (identity, both fingerprints, tool version, image
digest, the ordered inputs) and a trailer (terminal state, output reference and
metadata, diagnostics, record count), both carrying `dagr.attempt-shard@1` so they can
never be mistaken for event-stream records.

Two independent disciplines make a partial shard undetectable-as-complete impossible:
the write is **temp-file + fsync + rename**, so an interrupted write leaves *no shard*
and no debris; and the **trailer is last and declares the record count**, so bytes
truncated by any other route are refused as `Incomplete`. Both are asserted, the first
through a fault-injection seam that performs every step except the rename.

### 4. How are the input references passed? → **A repeated `--input`, not trailing positionals.**

The ticket writes the invocation as `exec-node --run --node --attempt <input refs>`.
A trailing positional list would collide with the **flow-name positional** the C26
command surface already reserves (`Cli::flow_name` is the first positional after the
verb), and a single-flow binary may omit that name — so the first reference would be
eaten as a flow name. A repeated `--input` preserves positional order, is unambiguous,
and is what an orchestrator generating argv would produce anyway. The verb's
value-taking flags are registered in `flag_takes_value` so their values are never
mistaken for the flow name.

### 5. Where does the output blob and the shard get written? → **`--blob-store <container>`.**

Input references are self-describing and name their own container, but the *output*
and the *shard* need a destination the invoker chooses. It is an unprefixed
verb-scoped flag alongside the existing unprefixed `--store`, rather than a
`--dagr.*` knob, because it is not a runtime knob of a run: it is an argument of this
verb, in the same family as `--run` / `--node` / `--attempt`.

### 6. Which "tool version"? → **`contract::TOOL_VERSION` (`dagr@1`), not the package version.**

Two different strings could answer: `env!("DAGR_BUILD_TOOL_VERSION")` (the package
version, build provenance in the graph artifact) and `contract::TOOL_VERSION`
(`"dagr@1"`, the **comparability token** resume refuses across and the run artifact
header records). The shard's tool version exists so a reader can refuse a shard it
cannot interpret, which is exactly the comparability question — so it is the same
token, and a shard's tool version compares against a run artifact's without
translation.

### 7. Where does the image digest come from? → **`--image-digest`, optional, absent when not supplied.**

No image digest exists anywhere in the engine, and none can: Kubernetes does not
expose it through the downward API, so the *submitter* is the only party that knows
it. It is therefore an argument, recorded verbatim when given and omitted from the
header otherwise. T108, which owns submission, is where it starts being supplied.

### 8. What makes a node remote-eligible, and how does its codec reach the pod? → **The `Payload`-bounded registrars, as captured `fn` pointers.**

ADR 115 §8 says a node is remote-eligible **iff** its input and output types implement
`Payload`. That is expressed as a *captured capability*: `register_source_payload` /
`register_payload` / `register_payload_with` monomorphize a decode-into-slot and an
encode-from-slot function where the concrete type is still known, and store the pair on
the registered node. A node registered through an ordinary registrar carries `None`
and `prepare_attempt` refuses it by name (`NotRemoteEligible`) — a compile-time bound
turned into a build-time fact, with no runtime type inspection anywhere.

Filling an *upstream's* slot is what rehydration does, so the node's own runner then
reads its inputs through the ordinary deferred read: the attempt path has no special
case for having run remotely. Two registrars were added to close real gaps the bound
opened — `register_payload_with` (a payload-bounded node with a stated policy, which
is what a placed node is) and `register_source_payload_with` (its source twin).

### 9. How can a `cancelled` attempt be recorded when the attempt runner only reports the task's own result? → **An effective-state override, stamped in exactly one record.**

A task that observes cancellation returns an error, and the runner classifies that as
`failed` — correct in general, wrong here, because the operator originated the outcome
and not the task. The verb therefore computes the **effective** terminal state
(cancelled when the signal fired and the attempt did not succeed; the runner's answer
otherwise) and stamps it onto the single `node-terminal` record. A cancellation that
arrives *after* the attempt already succeeded leaves the success standing, which is
the truthful record. Every other record is byte-for-byte what a local attempt emits.

A task that does **not** observe cancellation cannot be killed, so a watchdog thread
enforces the shutdown budget: after grace it writes a truthful `abandoned` shard from
the records emitted so far and exits, exactly as arch.md C16 describes.

### 10. Is the verb feature-gated? → **In the table unconditionally; its body behind `blob`.**

An operator (or an orchestrator) must get the same answer from every dagr binary
about what a verb *is*, so `exec-node` is in `verb_table()` for every build. Its body
needs the blob store the default-off `blob` feature provides, so a build without that
feature answers with a **recognized stub** naming the missing feature — the shape the
repo already uses for `resume` and the metastore's reserved open modes — rather than
failing to recognize a verb that exists.

### 11. Where do the pod's resources come from? → **`RunnableFlow::with_resources`, built inside the flow factory.**

A `ResourceRegistry` holds live client handles and is exactly the thing ADR 115 §2
says is not transported. The pod rebuilds it by running the binary's own flow-building
code, so the registry is attached to the flow the factory returns and threaded into the
attempt context `exec-node` builds. That is what makes the per-pod lifetime real and
observable: the demo's resource records its own construction, and the test asserts one
construction per invocation and two across two invocations.

The in-process `run` path builds its per-attempt contexts inside the driver, which owns
resource injection there; `with_resources` says so in its own documentation and changes
nothing about it. Wiring the driver's side is not this ticket's, and inventing it here
would take scope the ticket did not name.

### 12. What is the exit-code mapping? → **The existing table, with no new numbers.**

`0` success or a deliberate skip; `1` a task failure, timeout, or caught panic; `2` a
malformed invocation (a missing flag, a wrong input arity, a reference handed to a
source); `3` a node this build's graph does not have, a flow that does not assemble, or
a node with no codec; `4` an absent or corrupt input, a codec refusal, or an
unreachable store; `5` a cancelled or abandoned attempt; `6` an expected fingerprint
that is not this build's; `7` a shard that could not be written. The pair that matters
is `4` versus `1` — "the storage lost the input" versus "the task said no" — because
only one of them is the pipeline's fault. A panic and a task failure share `1` and are
distinguished in the shard, where the panic's payload is recorded and attributed.
