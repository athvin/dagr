# 115 · T100 — ADR: remote-execution scope carve-out and Kubernetes substrate

> **Milestone:** M10 · **Size:** S · **Type:** decision · **Components:** system-level
> **Branch:** `adr/t100-remote-execution-scope-and-substrate-adr` · **Depends on:** — · **Blocks:** T101

## Why / context

M7/M8 gave one binary a queryable, cross-run index of everything it ran (ADR 097).
The operator now wants the next thing that index was built to make possible: **each
task able to run in its own Kubernetes pod, with per-task resource sizing**, while
the local in-process mode stays fast enough to iterate on.

This is a genuine capability decision because `arch.md` states, as a *permanent*
non-goal, that dagr is **not "a distributed execution system"** (`arch.md`, "What
this is"), and promises "no server, no scheduler, no database" in the sentence
above it. ADR 012 (T0.6) sharpened that into "**not** a networked metadata service,
**not** a distributed store"; ADR 014 (T0.8) added "**not** an object-store
abstraction … there is **no built-in remote/object backend**." No feature ticket
may cross those boundaries until the boundaries themselves are amended and the
decision recorded — otherwise every M10 feature diff fails the orchestrator's scope
check (ticket-conventions §8) or triggers a spec-conflict STOP (§10). **This ticket
owns that amendment and the substrate decision; it ships no code.**

The distinction that makes the carve-out sound is the same kind of distinction ADR
097 drew, applied to a different word. ADR 097 read "metadata store" narrowly, as
*coordination*. This ADR reads "distributed execution system" narrowly, as
**distributing the graph**: cooperating schedulers, work-stealing between
orchestrators, cross-run queues, a control plane that outlives a run. What M10
wants is none of those. It is **one orchestrator process, owning one graph, placing
individual node attempts on compute it rents for the duration of a single run, and
exiting when that run ends.** One process still owns the graph. One process still
owns the event stream. Nothing coordinates with anything else, and no server is
introduced.

Three properties of the existing engine make this a small change rather than a
rewrite, and they are why the decision is defensible now and was not before:

- **`NodeRunner` is already the "where does this run" seam.**
  `crates/cli/src/driver.rs:630` presents every node to the driver as a
  `Box<dyn NodeRunner>` that emits through an *injected* `AttemptEventSink` and
  returns only a `TerminalState`. A Kubernetes executor is a new *implementation*
  of that trait; the driver, readiness tracker, admission controller, event stream,
  teardown phase, resume path, and exit-code precedence are untouched.
- **`DurableOutput` is already the cross-process value contract.**
  `crates/core/src/assembly.rs:164` — `serialize_reference` / `rehydrate` / optional
  `DurableReferenceMeta` — exists precisely so a value can be reconstructed
  "without running the producing task." `crates/core/src/task.rs:193` calls the
  durable bound on `Task::Output` "a seam left open here, not foreclosed."
- **The event stream, not the index, is the source of truth.** Because the
  metastore is a deterministic, idempotent projection through one function
  (`mapping::build_statements`), remote pods can ship event-stream *shards* and the
  orchestrator can fold them centrally. **The database is never served, and pods
  never speak SQL.**

## Objective

Produce the ADR (written into this ticket file per ticket-conventions §6) and amend
`arch.md`, recording these decisions:

- **Scope carve-out.** Amend `arch.md`'s permanent-non-goals sentence so
  "distributed execution system" continues to exclude an engine that distributes
  the graph — cooperating orchestrators, a persistent control plane, cross-run
  queues — while **a single orchestrator process placing node attempts on remote
  compute it owns for one run** is explicitly permitted. The permanent exclusions
  (scheduler, coordinating metadata store, web interface, DSL, backfill
  orchestrator) stay **verbatim**, and the graph's shape still never changes at
  runtime. Mark ADR 012 superseded-in-part for its "not a distributed store"
  clause and ADR 014 superseded-in-part for its "no built-in remote/object backend"
  clause; change no other text in either.
- **Substrate = Kubernetes pods, submitted by the orchestrator, observed by watch.**
  One shared watch per orchestrator process, never one per pod. Bare Pods (or Jobs
  pinned to `backoffLimit=0`), because dagr already owns retry.
- **State path = no callback.** The pod writes an attempt-scoped event-stream shard
  and its output blob, then exits. The orchestrator learns terminal state from the
  Kubernetes API (outbound only) and replays the shard into the driver's existing
  sink. **No inbound listener, no pod authentication, no pod database access.**
- **Placement is a `NodePolicy` field, never an `ExecutionClass` variant** — so
  making a node remote does not refuse resume.
- **Data passing = a zero-dep `Payload` codec plus one built-in blob backend**,
  bridged to `DurableOutput`. Local mode keeps the in-memory fast path. Record the
  three local/remote boundary cases and their cost, since only one of them keeps
  payload bytes away from the orchestrator.
- **Sweep the claims this capability (and M6/M7 before it) made false.** Amending the
  boundary is not enough if the surrounding prose still promises the opposite. This
  ticket also corrects, with the audit's file:line list: "that binary *is* the
  pipeline" (singular, false since M6 — and `arch.md` never recorded M6 at all); "no
  database" (false since M7); "**every** runtime knob honours `flag > env > default`"
  (never true of the shipped path — **zero** non-test callers of the five resolvers);
  "store base supplied by flag or **environment variable**" (`DAGR_STORE` does not
  exist); the never-silent-env-value rule (`DAGR_LOG_FORMAT` is a live exception); the
  two knobs missing from the C26 table; `flow-registry.md`'s "usually carries exactly
  **one** flow"; the SL7 matrix paraphrases; the 10-file "distributed **system**"
  drift; and — highest leverage — the three **process gates** whose flat boundary list
  would make the ticket-shipping scope check reject M10 work outright.
- **Submission is recorded write-ahead, as an event.** A new `attempt-submitted`
  event kind (`dagr.event-stream@1.3`) records what an attempt was launched with —
  identity triple, ordered `{ uri, content_hash }` inputs, fingerprints, tool version,
  image digest, intended and observed target identity — written **before** the remote
  work is created. It goes in the event stream, **not** directly into the index, so
  ADR 097's projection guarantee survives; the index gets the row through the existing
  projection.

## Test plan (write these first — TDD)

Decision ticket: the "tests" are mechanical file/content assertions, checked before
authoring and then made true.

- **ADR completeness.** This file contains an ADR with all five sections — Status,
  Context, Decision, Consequences, Rejected alternatives — and Status is `Accepted`,
  citing the dated operator acceptance recorded in Open questions.
- **arch.md amended, exclusions intact.** A grep confirms the words *scheduler*,
  *coordinating metadata store*, *web interface*, *domain-specific language*, and
  *backfill orchestrator* still appear in the permanent-non-goals sentence
  unchanged, and that "the graph's shape never changes at runtime" is untouched.
- **Supersession recorded.** ADR 012's "not a distributed store" clause and ADR
  014's "no built-in remote/object backend" clause each carry a
  "Superseded (in part) by ADR 115 (T100)" note; the rest of both files is
  unchanged.
- **No server, provably.** `scripts/check-metastore-acceptance-boundary.sh` passes
  unchanged — the design introduces no `TcpListener`, no `::bind(`, no `.serve(`,
  no `axum`/`tonic`, and does not lift `ModeNotImplemented`.
- **Schema `@1.3` is additive and safe for old readers.** The event-stream schema
  validates as JSON, the `kind` enum gains exactly one member, the
  `attempt-submitted` conditional requires `node`/`attempt`/`inputs` and caps `inputs`
  at the arity ceiling of 8, and the `@1.3` revision is described inline in the
  schema's own `description` the way `@1.1` and `@1.2` are. A grep confirms no test
  pins the number of event kinds.
- **The projection guarantee is named as the reason.** The ADR states that a
  submission record written directly to the index would break ADR 097's
  live-equals-reconcile property, and records the operator's audit/recovery
  requirement as **accepted** with only the write path changed.
- **Criteria/coverage matrices unchanged, deliberately.** This ADR amends prose and
  **amends system-level criterion 7 in place**; it introduces no *new* numbered
  criterion, so no matrix row is added or removed. A grep confirms criterion 7 is
  still present and still `[machine]`-classed, and that the matrices' row counts are
  unchanged. (M10's *feature* tickets add their own criteria and owe their own rows.)
- **Stale non-goal text fixed.** `README.md` and `CONTRIBUTING.md` carry the amended
  sentence (both currently carry the *pre-ADR-097* wording).
- **No code.** `git diff` for this branch touches only `docs/**`, `README.md`,
  `CONTRIBUTING.md`, and `schemas/event-stream/v1.schema.json`; **no `crates/**`
  changes and no `Cargo.lock` change**. The schema is the normative *record shape*
  this ADR fixes — deliberately included here rather than deferred, because a decision
  about what is recorded is exactly what a schema is; the **writer** that emits it is
  T108, and the fixture-corpus artifact arrives with it.

## Definition of done

- [ ] This file contains an ADR with **Status / Context / Decision / Consequences /
      Rejected alternatives** sections capturing the decisions in Objective.
- [ ] `arch.md`'s permanent-non-goals sentence is amended to permit a single
      orchestrator placing attempts on remote compute for one run, keeping every
      other exclusion verbatim; "Operational model" and "Performance envelope" are
      amended for the executor seam and the local-vs-remote overhead split.
- [ ] `arch.md`'s "Amendment changelog" carries an entry for this decision.
- [ ] ADR 012 is marked "Superseded (in part) by ADR 115" for its distributed-store
      clause only; ADR 014 likewise for its object-backend clause only; no other
      text changes in either.
- [ ] The ADR records: the Kubernetes substrate and one-watch-per-process rule, the
      no-callback state path, the Kubernetes-must-not-retry invariant, the
      annotation-vs-label identity rule, the two separate retry budgets, placement
      as a `NodePolicy` field, the `Payload` + blob-backend data path, the three
      local/remote boundary cases, the write-ahead `attempt-submitted` record and why
      it is an event rather than a direct index write, and the zero-dep-core /
      opt-in-crate / default-off-feature boundaries.
- [ ] `schemas/event-stream/v1.schema.json` adds the `attempt-submitted` kind and its
      conditional payload as `@1.3`, described inline in the schema's own
      `description`; the file still validates as JSON and no test pins the kind count.
- [ ] `README.md` and `CONTRIBUTING.md` carry the amended non-goals sentence.
- [ ] System-level criterion 7 is amended in place to scope "requires a server,
      database, or scheduler" to the default executor while keeping the "no dagr
      server" half unconditional; no *new* numbered criterion is added, and the SL7
      **paraphrases** in `docs/criteria-matrix.md` / `docs/coverage-matrix.md` are
      reworded to match it with **no change to row count, class, or mapped test id**.
- [ ] The truth pass lands: `arch.md` is plural about pipelines and records M6
      (`#[dag]` / `inventory` / leaf-binary constraint); "no database" becomes "none
      required" alongside the opt-in index in `arch.md`, `README.md`, and
      `crates/core/README.md`; `arch.md` states **why** two executors exist; the
      `arch.md:69` environment-variable claim is dropped; C12's purpose is scoped to
      local execution; the C26 table gains `DAGR_METASTORE` and `DAGR_LOG_FORMAT`, and
      the never-silent rule records its one exception; the precedence claim is restated
      as opt-in library surface; `docs/flow-registry.md`'s body is inverted.
- [ ] `README.md` and `crates/cli/examples/quickstart.rs` are edited **together** and
      `cargo test -p dagr-cli readme_quickstart` passes (the block is byte-pinned).
- [ ] The three process gates — `.claude/skills/shipping-dagr-tickets/SKILL.md`,
      `references/ticket-conventions.md` §8, and `.github/pull_request_template.md` —
      name both carve-outs, so a scope check over an M10 diff no longer trips.
- [ ] The M7/M8 tickets no longer say "distributed" + "system" without "execution"
      between them; every restatement matches `arch.md`'s wording and is covered by the
      carve-out. (Grep for the two-word phrase across `docs/implementation`; the only
      permitted match is this checklist line.)
- [ ] The diff touches only `docs/**`, `README.md`, `CONTRIBUTING.md`, and
      `schemas/event-stream/v1.schema.json` — **no `crates/**`, no `Cargo.lock`**.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Operator sign-off on amending a *permanent* boundary — RESOLVED, accepted
  (recorded per §5).** `arch.md` calls the distributed-execution exclusion permanent,
  and ticket-conventions §8/§10 would normally make moving it a hard STOP. The
  operator (athvin) commissioned this ADR and approved its design decisions on
  2026-07-29 (placement model, no-callback state path, re-entrant pod image,
  `Payload` + blob backend, in-memory local fast path, and the write-ahead submission
  record). Asked explicitly to accept the **boundary amendment itself** — this ADR's
  narrowing of "distributed execution system" plus the partial supersessions of ADRs
  012 and 014 — the operator **accepted on 2026-07-29** ("ya I accept those"). The
  ADR is `Accepted`; the loop may ship it without halting. No other contested
  decisions.
- **Milestone ordering — RESOLVED, moot (recorded per §5).** M9 (tickets 110–114 /
  T95–T99) was unfinished when this ticket was written, so it asked whether M10
  should begin before M9 landed. It did not have to: **all five M9 tickets merged
  first**, T99's acceptance gate (ticket 114) last, and T100 opened only afterwards.
  No reordering was exercised, no operator call was needed, and ticket numbering
  stayed a valid topological order — every dependency of 115 has a strictly lower
  NNN and is checked off. The question is recorded closed rather than deleted,
  because the *rule* it protects (numbering is a topological order) is what a later
  M10 ticket inherits.
- **Where the amendment actually landed — RESOLVED, recorded (per §5).** The
  artifacts this ticket is chartered to produce — the embedded ADR below, the
  `arch.md` amendment and its changelog entry, the two partial-supersession notes on
  ADRs 012 and 014, the `README.md` / `CONTRIBUTING.md` de-staling, the SL7 matrix
  rewording, the three process gates, and the additive `@1.3` event-stream schema
  revision — were authored and **merged ahead of this branch**, in PR #116 (ticket
  111 · T96, commit `126cdcb`), alongside the M10/M11 ticket set they belong to.
  Nothing was re-decided or rewritten here: this branch adds only the mechanical
  **pinning check** the Test plan describes
  (`scripts/check-remote-execution-scope-adr.sh`, wired into CI's "ADR content
  contracts" job), which asserts every one of those artifacts still says what this
  ADR decided — and, load-bearingly, that the carve-out has not widened. Recorded
  here because the ticket's own "the diff touches only …" line assumes the amendment
  lands *on this branch*, and a reader comparing the two would otherwise conclude
  the work was skipped; see `docs/implementation/DEVIATIONS.md`.

## Out of scope

- The spike validating kube-rs watch semantics, pod startup latency, and blob
  round-trip — **T101** (it gates every other M10 ticket).
- Real retry backoff and per-attempt timeout on the `RunnableFlow` path — **T102**.
- The `Payload` trait and derive — **T103**; the blob crate — **T104**.
- `Placement`, `Pool::RemoteSlots`, `--dagr.executor` — **T105**.
- The `exec-node` pod-side verb — **T106**; the shared `PodObserver` — **T107**;
  `K8sNodeRunner` — **T108**; orphan adoption — **T109**.
- The S3 backend and intermediate-blob GC — **T110**; the kind/k3s end-to-end demo,
  RBAC manifests, and acceptance gate — **T112**.
- Foreign-image tasks (a `PodTask` running a container dagr did not build) — named
  future work behind the pod-spec seam; **not** an executor mode, and not this
  milestone.
- Serving the metastore to pods, an inbound orchestrator API, `sqld`, and embedded
  replicas — **rejected in this ADR**, not deferred.
- Scope boundary restated: even with this carve-out, dagr remains **not** a
  scheduler, a coordinating metadata store, a web interface, a DSL, or a backfill
  orchestrator; there is still no cross-run coordination and no control plane that
  outlives a run, and the graph's shape never changes at runtime — a task that
  discovers N files does not become N pods.

---

# ADR: remote-execution scope carve-out and Kubernetes substrate

> This repo keeps each ADR inside its own implementation-ticket file (ADR 012
> embeds itself at `docs/implementation/012-…`, and ADR 097 the same way). This ADR
> is committed here, at
> `docs/implementation/115-T100-remote-execution-scope-and-substrate-adr.md`, the
> ADR location for ticket T100 — satisfying ticket-conventions §6
> (literal-DoD-first) with zero deviation. It amends `docs/arch.md` and marks ADRs
> 012 and 014 superseded-in-part for one clause each; it ships **no code**.

## Status

**Accepted (2026-07-29).** This is a **decision** ticket (ticket-conventions §4):
it amends a *permanent* `arch.md` boundary and picks a substrate, and ships **no
production code** — the committed artifacts are this ADR, the `arch.md` amendment,
the two partial-supersession notes, the README / CONTRIBUTING de-staling, and the
`@1.3` event-stream **schema** revision (§9). The shipping crates are unchanged and
`Cargo.lock` is untouched.

The schema is included here, unlike ADR 097's strictly docs-only diff, because §9's
whole subject *is* what gets recorded — and a schema is the normative statement of
that. The **writer** that emits the record, and the fixture-corpus artifact that pins
the revision in CI forever after, are **T108**. Nothing emits an `attempt-submitted`
record until then, so the addition is inert on merge: the `kind` enum gains a member
no shipped code writes, and every existing stream keeps validating.

**Operator acceptance is recorded, not pending.** `arch.md` calls the
distributed-execution exclusion *permanent*, so moving it would normally be a hard
STOP under ticket-conventions §8/§10. The operator (athvin) commissioned this ADR,
settled its design decisions on 2026-07-29, and — presented with the explicit
statement that this ADR and ADR 128 each awaited a dated acceptance line for the
boundary it moves — **explicitly accepted both on 2026-07-29** ("ya I accept
those"), recorded in this ticket's `## Open questions` per ticket-conventions §5.
This ADR is therefore **Accepted** and ships through the normal branch/PR/merge flow
without halting. The acceptance covers the `arch.md` amendment and the two
partial-supersession notes on ADRs 012 and 014; it is deliberately an explicit act
rather than an inference from the design discussion.

**No spike here, and a spike gates what follows.** This ADR fixes decisions; it
validates nothing by running code. The substrate's real risks — watch reconnection
after `resourceVersion` expiry, pod startup latency against the per-node overhead
budget, and blob round-trip cost — are **T101**, which gates every other M10
ticket. If T101's measurements contradict this ADR, the decision reopens here (see
Reopen condition).

## Context

`arch.md:13` states, as a *permanent* non-goal, that dagr is **not "a distributed
execution system"**, and `arch.md:11` promises "no server, no scheduler, no
database." ADR 012 (T0.6 · run store contract) sharpened this into an explicit
scope boundary — "**not** a networked metadata service, **not** a distributed
store" — and ADR 014 (T0.8 · durable-output contract) added "**not** an
object-store abstraction, **not** a networked or distributed store … there is **no
built-in remote/object backend**." Every M10 feature ticket needs at least one of
those clauses moved. **This ticket owns the amendment.**

What the operator wants is specific and narrower than the excluded thing: **task
isolation with per-task infrastructure sizing.** A pipeline that today must size
its admission pools to the largest node's appetite on one machine should instead be
able to put that node in a 16 GiB pod on a GPU nodepool and leave the rest small.
The local in-process mode must stay the development path, unchanged and fast.

The distinction that makes the carve-out sound is **what gets distributed**.
`arch.md`'s permanent exclusions are about an engine that distributes *the graph and
its control*: cooperating schedulers, work-stealing between orchestrators, cross-run
queues, a control plane other processes hand off to and that outlives any single
run. What M10 introduces is the opposite shape — **one process, one graph, one
event stream, one lifetime**, which rents compute per node attempt and releases it.
The engine gains no peer, no election, no shared lock, no queue, and no service.
ADR 012's rejection of a *coordinated multi-process store lock* ("the road to a
scheduler") is untouched: nothing here coordinates between orchestrator processes.

`arch.md` already assumes Kubernetes as dagr's **host** — the shutdown budget is
sized against a 30-second `terminationGracePeriodSeconds` (`arch.md:358`) and
"Operational model" names "a Kubernetes Job" as a legitimate trigger
(`arch.md:624`). This ADR makes Kubernetes additionally a **target**, and keeps the
trigger boundary exactly where it is: **dagr still never decides *when* a pipeline
runs.**

### What the engine already provides

Three shipped seams carry most of the weight, which is why this is a small ADR:

1. **`NodeRunner`** (`crates/cli/src/driver.rs:630`) — type-erased, returns only a
   `TerminalState`, emits through an injected `AttemptEventSink`. The ~30
   hand-written implementations in the test suite are standing proof the seam is
   open. A remote executor plugs in here and the run loop does not change.
2. **`DurableOutput`** (`crates/core/src/assembly.rs:164`) — a reference-carrying
   contract on the *output type*, with optional content hash, existing precisely so
   a value can be rehydrated without re-running its producer. Resume already depends
   on it, including mutation detection via `ReferenceExistence::Changed`.
3. **The event stream as source of truth** — `events.jsonl` → `fold_stream` →
   `RunArtifact` → `mapping::build_statements` → rows, with live-tee rows proven
   byte-identical to reconcile rows. This is what lets pods ship shards instead of
   writing to a database.

### What the field has learned

Two implementations of pod-per-task orchestration were reviewed as primary sources
(Airflow's `cncf-kubernetes` provider and Prefect 3's `prefect-kubernetes`). Their
converged conclusions shaped four decisions below, and two of them corrected a
first draft of this design:

- **Pods do not hold database credentials.** Airflow 3's `KubernetesExecutor`
  accepts only a typed `ExecuteTask` workload and generates the pod's argv from it,
  rather than from a DB-coupled CLI. The stated motivation for removing worker DB
  access was connection-count scalability, the security of running user code under
  the orchestrator's own service account, and task code accidentally coupling to
  orchestrator internals across upgrades. (Whether connection count is the
  *dominant* reason is publicly disputed by a core committer; the security and
  coupling arguments are not.)
- **One watch per orchestrator process, never one per pod.** Prefect's worker had
  per-job watching and log streaming removed in favour of a single observer per
  process; its `job_watch_timeout_seconds`, `pod_watch_timeout_seconds`, and
  `stream_output` fields survive as *dead declarations*. Airflow likewise runs one
  cluster watch.
- **Kubernetes-level retry and orchestrator-level retry are mutually exclusive**,
  because enabling both duplicates execution. Prefect defaults `backoffLimit=0` and
  actively strips its SIGTERM-reschedule environment variable whenever
  `backoffLimit` is non-zero.
- **Labels cannot carry identity.** Kubernetes' 63-character label-value limit makes
  labels lossy — Airflow truncates to 53 characters plus a 9-character md5 — so both
  of its orphan-reattachment paths read identity from **annotations** and use labels
  only as forward selectors.

## Decision

Eight decisions, in the order the M10 tickets consume them.

### 1. Scope carve-out (arch.md amended; ADRs 012 and 014 superseded in part)

`arch.md`'s permanent-non-goals sentence is amended so **"distributed execution
system"** continues to exclude an engine that distributes the graph or its control —
cooperating orchestrators, work-stealing, cross-run queues, a control plane that
outlives a run — while **a single orchestrator process placing individual node
attempts on remote compute it owns for the duration of one run** is explicitly
**permitted**. The other permanent exclusions — **scheduler, coordinating metadata
store, web interface, domain-specific language, backfill orchestrator** — stay
**verbatim**, and the graph's shape still never changes at runtime.

ADR 012's "not a distributed store" clause and ADR 014's "no built-in
remote/object backend" clause each carry a "Superseded (in part) by ADR 115 (T100)"
note; **no other text in either file changes** (ticket-conventions §10 — merged
decision text is never rewritten).

### 2. Substrate — Kubernetes pods, submitted and observed by one process

The orchestrator submits one pod per node attempt through the Kubernetes API and
observes it. Two rules are load-bearing:

- **One shared watch per orchestrator process.** A single `PodObserver` owns the
  watch, handles reconnection after `resourceVersion` expiry, and demultiplexes
  events to per-node waiters by label selector. Per-pod watches are the pattern both
  reviewed implementations abandoned.
- **Kubernetes must not retry.** Bare Pods, or Jobs pinned to `backoffLimit=0`.
  dagr owns retry through `NodePolicy` (`retries` + `backoff`); a Kubernetes-level
  retry running concurrently with a dagr-level retry duplicates execution of the
  same attempt. This is an **invariant, not a default**, and the executor refuses a
  configuration in which both are active.

### 3. State path — no callback, ever

The pod writes an **attempt-scoped event-stream shard** and its output blob to the
blob store, then exits. The orchestrator learns terminal state from the Kubernetes
API — an **outbound** call it can already make — reads the shard, and replays its
records into the `AttemptEventSink` the driver injected.

This is chosen over the narrow task-execution API that Airflow 3 landed on, for
three reasons specific to dagr:

- **It requires no inbound reachability.** A developer running `dagr run --executor
  k8s` on a laptop cannot be dialled by a pod in a cluster. An API would force the
  orchestrator in-cluster, or force a tunnel, and the "iterate locally, execute
  remotely" story is the point of the feature.
- **There is nothing to authenticate.** No listener means no projected service
  account tokens, no bound audiences, no authn surface, and no credential in a pod
  that runs user code.
- **It keeps the event stream single-writer.** Shards are replayed by the
  orchestrator through the existing buffering sink, so `seq` stays gapless, the fold
  is unchanged, and the metastore keeps exactly one writer.

**Pods never read or write the metastore.** `OpenMode::RemoteSqld` and
`OpenMode::SyncedReplica` remain `ModeNotImplemented`, `libsql` stays compiled
without `remote`/`sync`/`tls`, and no `sqld` is introduced. The index remains a
local, non-coordinating projection exactly as ADR 097 decided.

### 4. Identity — annotations authoritative, labels as selectors

- **Labels** (selectors only, each ≤63 characters): run id, a node-name
  *fingerprint* rather than the name, attempt number, an owner key, and a
  completion tombstone key that the adoption selector filters on.
- **Annotations** (authoritative, unbounded): full node name, pipeline name, both
  fingerprints, tool version, and the image digest.

dagr's run ids are 36-character UUIDv7 values before any node name is appended, so
the 63-character limit binds immediately; identity is therefore read from
annotations on every reconciliation path.

### 5. Ownership and orphan adoption

Adopting pods after an orchestrator restart is a **labels-only patch** of the owner
key — never a pod recreation. Terminal pods are deleted or tombstoned with the
completion key. Revoking ownership is a deliberate **two-step patch-then-delete**,
so the delete is not misread as an external deletion. The attempt key
`(run_id, node, attempt)` is the idempotency key: a resubmission for a key that
already has a live or tombstoned pod adopts rather than duplicating.

### 6. Two retry budgets, kept separate

A pod that **never started** — unschedulable, image pull failure, quota rejection —
is an *infrastructure* failure. It is retried against a separate operator-set
budget and is **not** charged against `NodePolicy::retries`. Only a pod whose
container actually ran and reported a task error consumes a user-visible attempt.
Without this split, a cluster at capacity burns a node's entire retry budget without
executing anything.

Terminal classification comes from **pod/Job status**, not container exit codes.
`OOMKilled`, `Evicted`, `Unschedulable`, and `ImagePullBackOff` are carried as
**diagnostic strings** on the attempt record; the nine-state terminal taxonomy
(`arch.md` "Vocabulary") stays **closed** and gains no member.

### 7. Placement is a `NodePolicy` field, never an `ExecutionClass` variant

`ExecutionClass` lives in `dagr-core`, is not `#[non_exhaustive]`, and feeds the
**structural** fingerprint — and a structural mismatch is a hard
`ResumeRefusal::StructuralMismatch`. Adding a `Remote` variant would refuse resume
for every existing pipeline.

Placement therefore lives on `NodePolicy`, which feeds the **policy** hash, where a
divergence proceeds with a `PolicyDiff` instead of refusing. Consequences: a
pipeline can be run locally and resumed remotely (or the reverse), and moving a node
between local and remote is a visible policy diff rather than a broken resume.

The policy carries **opaque strings only** — CPU, memory, node selectors,
tolerations. `dagr-core` never learns what Kubernetes is, exactly as it never learns
where a durable referent lives.

### 8. Data passing — a zero-dep `Payload` codec plus one blob backend

- **`Payload`** is a new trait in `dagr-core` (`encode` to bytes / `decode` from
  bytes), with `#[derive(Payload)]` in the existing build-time-only `dagr-macros`
  crate. **`dagr-core`'s runtime dependency set stays empty** — no serde, no codec
  crate.
- A node is **remote-eligible iff its input and output types implement `Payload`**.
  A non-serializable payload on a remote node is a **compile error**, in keeping
  with mis-wiring already being a compile error.
- A **new opt-in crate** provides a `BlobStore` port with a local-filesystem
  implementation (reusing the existing scratch root, and sufficient for a shared
  read-write-many volume) and, later, an S3-compatible one. A blanket
  `DurableOutput` implementation over `Payload` bridges to the shipped
  durable-reference machinery, so remote outputs inherit content hashes, existence
  probes, and resume mutation detection for free. **This is the clause of ADR 014
  that is superseded**: dagr now ships *one* object backend, behind a default-off
  feature, rather than none.
- **Local mode keeps the in-memory fast path.** A `Payload`-bounded value still
  moves through `Arc<Slot<T>>` locally with no encode/decode. An operator flag
  forces a local round-trip so codec bugs are catchable without a cluster, and CI
  runs the suite both ways.

#### The three data-path cases at the local/remote boundary

`Slot<T>` is typed by `T::Output`, so a remote producer **cannot** fill a slot with a
reference — the type forbids it. The boundary therefore has three distinct cases, and
they have materially different costs:

| Edge | Mechanism | Bytes through the orchestrator |
|---|---|---|
| remote → remote | reference passed to the consumer attempt; **the slot is never filled**; the driver's reference map carries the edge | **none** |
| local → remote | the orchestrator holds the real value; it encodes and uploads it | **out** |
| remote → local | the consumer needs a real `T`; the reference is rehydrated into its slot | **in** |

Two consequences follow, and both are guidance the docs must carry rather than
mechanisms to build. **Alternating local and remote nodes routes payload bytes
through the orchestrator** at every transition — for an orchestrator on a developer's
machine, a `remote → local` edge means a download to that machine. The honest
guidance is to place **contiguous subgraphs** remotely, not every other node. And
because a `remote → remote` edge fills no slot, it consumes **near-zero local memory
residency** — which is what makes `Pool::RemoteSlots` plus a near-zero local cost
(§7's admission model) accurate rather than a fudge.

The `remote → local` case needs no new machinery: it is the resume rehydration path,
which already fetches a reference and pre-fills a consumer's slot with the real
value.

### 9. The submission record — write-ahead, in the event stream

Everything above records an attempt's **outcome**. `durable_reference` lands on the
`attempt-outcome` record, which is written *after* the attempt finishes. That leaves
the platform's own work object as the **only** record of what an attempt was launched
with — and that record is deleted when the executor cleans up (§5). So "what inputs
was this task given?" becomes unanswerable after cleanup, and an orchestrator that
crashed between deciding and submitting has no durable record of its intent.

An **`attempt-submitted`** record closes that gap:

- It is written **before** the remote work is created — **write-ahead**. Recording
  after submission would lose exactly the crash window it exists to cover.
- It carries the identity triple `(run_id, node, attempt)` — the same idempotency key
  adoption uses (§5). A run id alone cannot distinguish try 1 from try 3.
- It carries the **ordered, positional** list of `{ uri, content_hash }` references
  the attempt was given. Order is load-bearing (dagr binds inputs positionally, with
  an arity ceiling of 8), and a **consume-nothing source encodes as an empty array,
  never a null** — dagr already types that case (`Input = ()`), and an empty array
  plus a statically known arity makes a mismatch *detectable*.
- Every reference carries its **content hash**, not just its path, so a recovering
  orchestrator can still detect an out-of-band overwrite (the `MutatedReference`
  gate) instead of trusting a path.
- It carries both fingerprints, the tool version, and the image digest, so a
  recovering orchestrator can refuse work launched by a **different program**.
- It records **intent and reality separately**: the intended target name before
  creation, and the platform's actual name, UID, and host additively after. These
  diverge, and a post-mortem needs both.
- References recorded here **must be opaque and non-secret**. A credential-bearing
  URL (a presigned URL) is never written to an event record or a label; credentials
  come from the ambient environment (§8's backend).

**It goes in the event stream, not straight into the index.** This is the load-bearing
part. ADR 097 guarantees the metastore is a *projection* of the event stream and
nothing else, and that guarantee is machine-checked by asserting live-tee rows are
byte-identical to rows produced by folding streams after the fact. A submission record
written directly to the database would make `dagr metastore sync` unable to reproduce
the index from streams, breaking the projection property. Emitting an event instead
gets the index row **for free** through the existing projection, and gets crash-proof
append-only durability and a place in the run artifact as well.

The record is a **new event kind**, not extra fields on the existing `node-admitted`,
and it is emitted **only by a remote executor** — so a local in-process run's stream
stays byte-identical to a pre-M10 run, which several M10 tickets assert as a DoD line.
This is `dagr.event-stream@1.3`: additive within `@1` (the schema's evolution rule,
already exercised by `@1.1` and `@1.2`), and safe for older readers because the fold
ignores unknown kinds.

## Consequences

- **The M10 boundary is now open — and only this far.** The `arch.md` amendment and
  the two supersession notes let T101–T112 build the executor without each tripping
  the scope check. None of them re-decides anything above.
- **Each M10 ticket inherits a named seam and reopens no question this ADR closes:**
  **T101** (spike: watch reconnection, pod startup latency, blob round-trip — gates
  the rest, §2); **T102** (real backoff + per-attempt timeout, the correctness
  prerequisites below); **T103** (`Payload` + derive, §8); **T104** (blob crate +
  blanket `DurableOutput`, §8); **T105** (`Placement`, remote admission pool,
  executor selection, §7); **T106** (the `exec-node` pod-side verb and shard writer,
  §3); **T107** (the shared `PodObserver`, §2/§4); **T108** (`K8sNodeRunner` and the
  two retry budgets, §6); **T109** (adoption, tombstone, patch-then-delete, §5);
  **T110** (S3 backend + intermediate-blob GC, §8); **T112** (kind/k3s demo, RBAC,
  acceptance gate).
- **Two correctness prerequisites exist in shipped code and must land first.** The
  `RunnableFlow` adapter (`crates/cli/src/run_flow.rs`) passes an empty timer to the
  retry loop, so `BackoffStarted` is emitted but **no delay elapses**; and it arms
  **no per-attempt timeout**, so `NodePolicy::timeout` is fingerprinted but never
  enforced on that path. Remote execution turns both into run-level hazards — a hung
  pod with no timeout hangs the run, and un-delayed retries hammer a throttled API
  server. **T102** owns both. They are defects in the local engine too.
- **The performance envelope must be restated, not quietly broken.** `arch.md`
  budgets framework overhead per node at **under one millisecond**, held by a CI
  benchmark. Pod scheduling is seconds. The envelope is amended to scope that budget
  to **local execution**, with remote submission-to-start latency measured and
  reported as its own figure. Leaving the sentence unqualified would make the
  benchmark's premise silently false.
- **The dependency tree gains a network stack, in a quarantine.** A Kubernetes client
  pulls HTTP and TLS crates into a lockfile that today contains **zero** network
  crates. They are confined to the new opt-in crate behind a default-off feature;
  `dagr-core` stays at zero runtime dependencies, `cargo build --all` and
  `--no-default-features` reach neither, and `deny.toml` is extended for the new
  licenses. This is the same containment shape ADR 097 used for `libsql`.
- **No server surface is added, and that stays machine-checked.**
  `scripts/check-metastore-acceptance-boundary.sh` — which fails the build on a
  network listener, a served endpoint, an HTTP/gRPC server framework, a
  `*Scheduler` type, or a lifted `ModeNotImplemented` — passes **unchanged** under
  this design. M10 extends it with assertions that pods link no metastore and the
  orchestrator opens no listener, rather than relaxing it.
- **A resource with per-process side effects now exists once per pod.** Because the
  pod re-enters the same binary, its `main()` rebuilds the `ResourceRegistry` from
  the same code. A resource that holds a connection pool, a lock file, or a local
  cache is therefore instantiated per pod, not per run. This is documented, not
  prevented.
- **Cancellation gains a Kubernetes obligation.** SIGTERM to the orchestrator must
  delete outstanding pods within the existing grace-plus-teardown budget, which
  already assumes a 30-second kill window. Pods orphaned by a hard kill are handled
  by adoption (§5), not by leaking.
- **Intermediate blobs need a reaper.** Nothing currently deletes them; the `prune`
  verb learns to (T110). Content addressing makes them cacheable across runs, which
  also means a naive reaper can delete a blob another run still references —
  reachability, not age, is the criterion.
- **The event stream schema goes to `@1.3`, and the index gains an audit surface.**
  §9's `attempt-submitted` kind is additive within `@1`; a pre-`@1.3` reader folds an
  `@1.3` stream unchanged because the fold ignores unknown kinds, and a local run
  emits none of these records, so its stream is byte-identical. The fixture corpus
  gains an `@1.3` artifact, which the "one artifact per released schema version is
  parsed in CI forever after" commitment (`arch.md` "Stability") then holds for good.
  The metastore projection of the new record is **T111**, and it is what makes "what
  was this attempt launched with, and did it match what it read?" a SQL question.
- **Recovery has a boundary, and it should be stated rather than discovered.** A
  restarted orchestrator recovers intent by folding its own event stream — which
  requires the run store to be on storage that outlives the process, the one piece of
  infrastructure `arch.md` already asks the operator to supply. If the orchestrator's
  machine is *gone*, the stream and the local index are gone with it, and the only
  surviving records are the platform's own work objects (labels and annotations, §4)
  and the blobs. A run whose store is on ephemeral local disk therefore has **no
  recovery story**, by construction. That is an acceptable and documented consequence
  of the local-first design, not a defect to engineer around.
- **Coverage / criteria matrices: no change, and one criterion reworded.** Like ADR
  097, this is a docs-only decision that adds **no new numbered criterion** — it
  amends the "What this is," "Operational model," and "Performance envelope" prose,
  none of which introduces a classified criterion. It does **amend system-level
  criterion 7 in place**: "nothing requires a server, database, or scheduler" is
  scoped to the *default* executor, since an operator who opts into remote execution
  has chosen a cluster dependency. The **"no dagr server" half stays unconditional**
  and is asserted structurally for both executors — that is the part the boundary
  actually protects. Amending an existing criterion adds **no row**, so the matrices
  gain and lose nothing; M10's *feature* tickets carry their own criteria and owe
  their own rows, and the acceptance gate is **T112**.

  **Both matrices do, however, need a wording touch, and this ticket makes it.** The
  SL7 rows in `docs/criteria-matrix.md` and `docs/coverage-matrix.md` *paraphrase*
  criterion 7 rather than citing it, so amending the criterion in `arch.md` and
  leaving the paraphrases alone would leave the matrices contradicting the spec they
  classify. The paraphrases are reworded to match; the row count, the classification,
  and the mapped test id are unchanged. (The mapped test is still named
  `no_server_database_or_scheduler_is_required`, which now reads as narrower than the
  criterion it covers — renaming it is **T112**'s, alongside the gate that owns that
  test.)
- **Reopen condition.** If **T101** finds that watch reconnection cannot be made
  reliable on the chosen client, that pod startup latency makes per-node pods
  untenable for the target graph sizes, or that the no-callback shard path cannot
  report terminal state reliably for a pod that dies mid-write, then §2/§3 **reopen
  here**, in this ADR, rather than being worked around locally. Likewise if the
  dependency quarantine or the zero-dep-core guarantee cannot be kept (§8). A local
  workaround that silently diverges from this ADR is a defect, not a fix.

## Rejected alternatives

- **Serving the metastore to pods** (libSQL in server mode / `sqld`, or embedded
  replicas, so each pod writes its own rows). **Rejected on the scope boundary and
  on the merits.** It is a *server surface* — the coordinating access point the
  amended boundary still excludes — and it hands SQL to every workload, making the
  schema a public API to user code. Technically it is the worst available option:
  libSQL's WAL is **single-writer**, the store is validated at 5–6 concurrent
  writers with a 250 ms `busy_timeout` and an 8-attempt cap, and the live tee
  re-folds the whole run on every event. Fifty short-lived pods would produce
  `SQLITE_BUSY` exhaustion, which — because the tee is *guaranteed*, not
  best-effort — is a run-killing sink failure rather than graceful degradation. It is
  also the specific coupling Airflow spent a major version removing. Not a later
  ticket.
- **A narrow HTTP/gRPC task-execution API on the orchestrator** (the Airflow 3
  shape: pods POST state and heartbeats). **Rejected for dagr specifically.** It is
  the right answer for a long-lived, in-cluster scheduler and the wrong one for a
  process a developer runs on a laptop: it forces the orchestrator in-cluster or
  behind a tunnel, and introduces a listener, an authentication surface, and pod
  credentials into a project whose README promises no server. The Kubernetes watch
  already provides the liveness signal an inbound heartbeat would carry. Revisit only
  if the no-callback path cannot report terminal state reliably (the reopen
  condition).
- **Writing the submission record straight into the run index** (an INSERT when an
  attempt is submitted, so the audit trail is immediately queryable). **Rejected on
  ADR 097's projection guarantee:** the index is a *projection* of the event stream
  and nothing else, and that is machine-checked by asserting live-tee rows equal
  rows produced by folding streams after the fact. A row the stream cannot produce
  makes `dagr metastore sync` unable to reproduce the index, which turns the index
  from a guaranteed projection into a second, independently-writable source of truth
  — the exact property ADR 097 refused. Emitting an `attempt-submitted` **event**
  (§9) delivers the same queryable row through the existing projection, and adds
  crash-proof append-only durability the direct write would not have. The
  operator's underlying requirement — audit and recovery of what a task was launched
  with — is **accepted in full**; only the write path is different.
- **Recording the submission on the existing `node-admitted` record** instead of a new
  kind. **Rejected on the byte-identical-local-run guarantee:** every local run emits
  `node-admitted`, so adding fields to it would perturb the event stream of every
  existing pipeline. A new kind that only a remote executor emits leaves local streams
  untouched (§9).
- **Passing the input references to the attempt without recording them** (the shape
  this ADR had before §9). **Rejected on recovery:** it leaves the platform's own work
  object as the only record of what an attempt was launched with, and that record is
  deleted on cleanup (§5) — so a post-mortem after cleanup, or an orchestrator that
  crashed between deciding and submitting, has nothing to read. Passing the references
  is still how the attempt *receives* them; §9 adds the durable record.
- **A `Remote` variant on `ExecutionClass`.** **Rejected on resume compatibility:**
  `ExecutionClass` feeds the structural fingerprint, so the variant would make every
  existing pipeline's resume refuse with `StructuralMismatch`. Placement belongs on
  `NodePolicy`, where divergence is a diff (§7).
- **Arbitrary per-task container images in this milestone** (the
  KubernetesPodOperator shape). **Rejected as premature, not as wrong.** It makes the
  payload encoding a public, versioned wire contract for foreign processes and turns
  orchestrator-to-image version skew into a live operational problem, and it requires
  the emptyDir-plus-spin-loop-sidecar machinery — an injected sidecar whose only job
  is to keep the pod non-terminal until the orchestrator reads a file over the exec
  API. The re-entrant same-image design makes skew impossible by construction and
  needs none of it. A foreign-image `PodTask` is named future work behind the
  pod-spec seam, as a **task type** rather than an executor mode — so it would work
  in local mode too.
- **Kubernetes Jobs with a non-zero `backoffLimit`** (letting Kubernetes retry).
  **Rejected on correctness:** dagr already owns retry, and both retry loops active
  at once duplicates execution of the same attempt. Prefect strips its own
  reschedule signal whenever `backoffLimit` is non-zero for exactly this reason
  (§2).
- **Deriving pod identity from labels alone.** **Rejected on a hard platform
  limit:** the 63-character label-value ceiling makes labels a lossy, non-reversible
  encoding of a 36-character run id plus a node name. Identity lives in annotations
  (§4).
- **Serde as the payload codec.** **Rejected on the zero-dep-core boundary:**
  `dagr-core`'s runtime dependency set is empty and additions to it are reviewed as
  API decisions (`arch.md` "Stability"). A `Payload` trait with a build-time derive
  keeps that guarantee while giving authors one line.
- **Always round-tripping payloads through bytes, including locally.** **Rejected on
  the product premise:** it would slow the development loop this feature exists to
  preserve, and change the performance profile of every existing pipeline. Local
  stays in-memory, with an opt-in forced round-trip for codec testing (§8).
- **Multiple cooperating orchestrators, a persistent control plane, work-stealing,
  or cross-run queues** (the thing the permanent non-goal always excluded).
  **Still rejected, unchanged.** This carve-out permits exactly one orchestrator
  process, owning one graph, for one run's lifetime. No election, no peer, no shared
  lock, no queue, no service that outlives the run. Moving that boundary is **not**
  what this ADR does.
- **Runtime graph expansion** (a task discovering N files becoming N pods).
  **Still rejected, unchanged.** Remote execution changes *where* a node runs, never
  *how many* nodes there are. The blessed pattern remains one node iterating
  internally with declared, bounded concurrency.

*(Operator acceptance of the boundary amendment is RECORDED — dated 2026-07-29 in
§Status and this ticket's §Open questions, per ticket-conventions §5. Reopen condition
stated in §Consequences.)*
