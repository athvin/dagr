# 120 · T105 — placement policy, remote admission pool, executor selection

> **Milestone:** M10 · **Size:** M · **Type:** feature · **Components:** C5, C12, C21, C26
> **Branch:** `feat/t105-placement-and-executor-selection` · **Depends on:** T101 · **Blocks:** T106, T107

## Why / context

This is the ticket where remote execution becomes *expressible* — and it is
deliberately separate from the ticket where it becomes *possible* (T108), because the
declaration surface has fingerprint and resume consequences that must be got right
before any Kubernetes code exists to distract from them.

ADR 115 §7 settled the decision that shapes everything here: **placement is
`NodePolicy`, never an `ExecutionClass` variant.** `ExecutionClass` lives in
`dagr-core`, is not `#[non_exhaustive]`, and feeds the **structural** fingerprint —
and a structural mismatch is a hard `ResumeRefusal::StructuralMismatch`
(`crates/core/src/resume.rs`). A `Remote` variant would therefore refuse resume for
every pipeline in existence the moment it was added. `NodePolicy` feeds the **policy**
hash, where a divergence proceeds with a printed `PolicyDiff` (ADR 010 / the
arch.md amendment). The payoff is concrete and worth stating: **a pipeline can be run
locally and resumed remotely**, or the reverse, and moving a node between the two is
a reviewable policy diff rather than a broken resume.

The admission story needs care too. `AdmissionController` sizes its pools from *this
machine's* cgroup limits minus a 20% headroom (`crates/core/src/limits.rs:71`), which
is the right model for in-process work and the wrong one for a pod: a remote node
consumes almost no local memory or threads, and its real cost is cluster capacity
dagr does not model. `crates/core/src/admission.rs:100` names the `Pool` enum as the
extension point and says adding a pool is "a spec-driven change" — which this is.
The remote pool is a flat operator ceiling on in-flight pods (Airflow's `parallelism`),
not an attempt to mirror a `ResourceQuota`; the cluster remains responsible for its
own capacity, and dagr does not second-guess it.

Nothing in this ticket talks to a cluster. It ships the policy field, the pool, the
executor selection plumbing, and a **stub executor** that refuses with a clear error —
so the whole declaration surface is testable, fingerprint-checked, and reviewable
before T108 exists.

## Objective

Make placement declarable and executors selectable, without any Kubernetes code.

- Add a **`Placement`** field to `NodePolicy` in `dagr-core`, carrying **opaque
  strings only** — CPU, memory, node selectors, tolerations — and defaulting to
  absent (= local). `dagr-core` never learns what Kubernetes is, exactly as it never
  learns where a durable referent lives.
- Expose it at the **typed registration site** (a builder method on the node
  registration path, alongside the existing policy setters), so declaring placement
  is ordinary typed Rust and a malformed declaration is a compile or assembly error.
- Include `Placement` in the **policy hash** and its canonical encoding, and
  **exclude it from the structural fingerprint**. Render it in the graph artifact so
  a diagram and a structure diff show it.
- Add **`Pool::RemoteSlots`** with a flat capacity from `--dagr.max-pods`
  (flag > env > default), and give a placed node a near-zero local `PoolCost` plus one
  remote slot. Preserve the existing `can_ever_fit` / over-demand refusal semantics so
  a node demanding more remote slots than the ceiling fails terminally at bootstrap
  rather than stranding.
- Add **`--dagr.executor=local|k8s`** (flag > env `DAGR_EXECUTOR` > default `local`)
  through the established `config.rs` `resolve` precedence, registered in the
  reserved `--dagr.*` flag namespace (`contract::reserved_flag_names`).
- Under the **local** executor, a `Placement` is **recorded and ignored** — so one
  binary is genuinely both, and a placed pipeline still runs on a laptop with no
  cluster.
- Ship the `k8s` executor as a **stub that refuses** with an actionable error naming
  the ticket that implements it — the `OpenMode::ModeNotImplemented` pattern from
  ADR 097 §5, which this repo already uses for a reserved-but-unbuilt seam.

## Test plan (write these first — TDD)

**Fingerprint and resume compatibility — the load-bearing tests**
- Given two pipelines identical except that one node carries a `Placement`, then
  their **structural fingerprints are equal** and their **policy hashes differ**.
- Given a run of the un-placed pipeline and a resume against the placed one, then
  resume **proceeds** and prints a `PolicyDiff` naming the placement change — it does
  **not** refuse.
- Given a placed pipeline resumed against itself, then no policy diff is printed.
- Given the graph artifact for a placed pipeline, then placement appears in it and in
  the structure snapshot; a placement change is visible in a structure diff.

**Policy surface**
- Given a `Placement` declared at registration, then the assembled `NodePolicy`
  carries it verbatim as opaque strings — no parsing, no validation of Kubernetes
  semantics in `dagr-core`.
- Given no `Placement`, then the policy, the policy hash, and the artifact are
  byte-identical to before this change.

**Admission**
- Given `--dagr.max-pods=2` and three placed nodes ready simultaneously, then at most
  two hold remote slots at once and the third waits — asserted through the admission
  ledger, not timing.
- Given a placed node and a memory ceiling that would reject it as a local node, then
  it is admitted anyway (its local cost is near zero).
- Given a node demanding more remote slots than the ceiling can ever supply, then
  bootstrap refuses it the way over-demand is refused today (terminal failure, not a
  stranded node).
- Given no placed nodes, then the remote pool is never consulted and the admission
  ledger's behaviour is unchanged.

**Executor selection**
- Given `--dagr.executor=k8s`, then the run fails with the stub's actionable refusal
  naming the implementing ticket — not a panic, and not a silent local run.
- Given `--dagr.executor` unset, then the local executor runs and the event stream is
  byte-identical to before this change.
- Given the flag and `DAGR_EXECUTOR` disagree, then the flag wins; given an unknown
  value, then it fails loudly naming the variable and the rejected value.
- Given a `Placement` under the local executor, then the run succeeds locally, the
  placement is recorded in the artifact, and nothing warns about a missing cluster.
- Given a pipeline parameter named `dagr.executor` or `dagr.max-pods`, then it is a
  hard `LibraryFlagCollision`.

## Definition of done

- [ ] `NodePolicy` carries an optional `Placement` of opaque strings; `dagr-core`
      gains no Kubernetes knowledge and no dependency.
- [ ] Placement is in the policy hash and the graph artifact, and **out of** the
      structural fingerprint; a local↔remote resume proceeds with a policy diff.
- [ ] `Pool::RemoteSlots` exists with a `--dagr.max-pods` ceiling; placed nodes take
      one remote slot and near-zero local cost; over-demand refusal is preserved.
- [ ] `--dagr.executor=local|k8s` follows `flag > env > default`, is registered in the
      reserved flag namespace, and rejects unknown values loudly.
- [ ] Under the local executor a `Placement` is recorded and ignored; the `k8s`
      executor is a refusing stub naming its implementing ticket.
- [ ] An un-placed pipeline's policy hash, artifact, and event stream are
      byte-identical to before this change.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Does `Placement` belong in the `#[task]` attribute, the registration builder, or
  both?** The builder is required (it is where policy already lives and where the
  typed handle is). Whether the attribute grows a convenience form is decided in-PR;
  the fingerprint behaviour is identical either way, and the attribute can follow
  later without a decision change.
- **Should a `Placement` under the local executor warn?** Recorded-and-silent is the
  chosen behaviour, because warning would make the dual-mode story noisy for exactly
  the developer the local path exists for. Revisit only if it causes a real
  misconfiguration in T112's demo.

## Out of scope

- Any Kubernetes client, pod spec construction, or cluster call — **T107** / **T108**.
  The executor here is a refusing stub.
- The pod-side `exec-node` verb — **T106**.
- Requiring `Payload` bounds on placed nodes — enforced where remote registration
  happens in **T108**; this ticket's placement is inert.
- Modelling cluster capacity, `ResourceQuota`, or bin-packing. The remote pool is a
  flat operator ceiling; the cluster owns its own capacity.
- Re-deciding placement-vs-execution-class (ADR 115 §7) or adding an `ExecutionClass`
  variant.
- Scope boundary restated: a declared, opaque policy field and a flat local ceiling
  add no coordination and no server; dagr remains not a scheduler, a distributed
  execution system, a coordinating metadata store, a web interface, a DSL, or a
  backfill orchestrator, and the graph's shape never changes at runtime — a placed
  node is one node.
