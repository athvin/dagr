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

---

## Open questions — resolved

`docs/tasks.md` carries **no `T105` entry** (it enumerates M0–M4 only), so there are
no `Q:` items beyond this file's own two. Both are answered below, together with the
decisions the implementation had to make that the ticket did not name.

### 1. Does `Placement` belong in the `#[task]` attribute, the registration builder, or both? → **The builder only, in this ticket.**

`NodePolicy::placement(…)` is the whole surface, reached at every registration site
that already takes a policy — plus two new registrars,
`RunnableFlow::register_named_with` / `register_source_named_with`, because a placed
node is precisely a node whose *policy* the graph artifact must show, and until now
one had to choose between stable names and a stated policy.

The attribute deliberately does **not** grow a convenience form. `#[task]` describes
a *task* — its work shape and its types — while placement is a property of a *node*,
and one task may legitimately be registered as several nodes with different
placements. An attribute form would either be a lie for the second registration or
would need a per-registration override anyway, which is the builder. As the ticket
notes, the fingerprint behaviour is identical either way, so the attribute can follow
later without re-deciding anything.

### 2. Should a `Placement` under the local executor warn? → **No — recorded and silent.**

Implemented as stated in the ticket, and asserted: `a_placed_pipeline_runs_locally_without_warning_about_a_cluster`
fails if the local run's output so much as contains "cluster", "kubernetes", "k8s",
or "warn". Warning would make the dual-mode story noisy for exactly the developer the
local path exists for. Revisit only if it causes a real misconfiguration in T112's
demo.

### 3. What owns the strings — `String` or `&'static str`? → **`&'static str`.**

`Placement` is `Copy` over `&'static str` / `&'static [(&'static str, &'static str)]`,
so `NodePolicy` stays `Copy` (no allocation on the admission path, and no ripple
through the ~66 `.policy()` call sites that rely on by-value copies). The stronger
reason is the guarantee it makes structural: placement feeds the **policy hash**,
which arch.md promises is identical across machines and toolchains for unchanged
source. A `String` would let a placement be computed from the environment and quietly
break that; `'static` makes it impossible, and matches the crate's existing
convention for author-declared identity data (`StableName` is a `&'static str`
constant). ADR 128's rule that the profiles file "cannot reach node policy or
placement" is then true by construction rather than by discipline.

### 4. What is `--dagr.max-pods`'s default? → **Unlimited (no dagr-side ceiling).**

Every other pool defaults to unconstrained (`PoolCapacities::new()`), the three local
pools are *derived from the machine* and this one has no machine to derive from, and
ADR 115 is explicit that the cluster remains responsible for its own capacity. Any
finite default would be dagr second-guessing an accounting it cannot see, and would
silently serialize a wide fan-out nobody asked to serialize. `0` is meaningful and
supported: it means *no remote capacity*, and a placed node then fails the bootstrap
over-demand check with a named reason rather than stranding.

### 5. Does the remote cost model apply under the local executor? → **No.**

"Recorded and ignored" has a ledger consequence the ticket does not spell out: a
placed node running **in this process** really does consume this machine's memory and
threads, so charging it near-zero would be a ledger that lies — precisely what C12
forbids. The mapping therefore takes the executor's answer
(`PoolCost::from_policy(policy, PlacementHandling::{Honoured,Ignored})`): honoured →
one remote slot and near-zero local cost; ignored → the declared vector verbatim. The
driver reads it from `RunConfig::executor`, so T108 lifts the refusal and the
admission model is already correct underneath it.

**Output residency is preserved across that mapping**, deliberately. Working memory
and threads belong to the running attempt and genuinely move away with it; residency
is the lease a *produced value* holds in its output slot, and a local consumer
downstream of a placed producer still rehydrates that value into this process. It is
charged at production rather than at admission, so keeping it costs a placed node
nothing at admission time and keeps the memory pool honest when the value does land
locally.

### 6. Where does the `k8s` refusal live? → **In the driver's bootstrap *and* the `run` verb.**

Both, on purpose. The verb refuses before a run-store directory exists (an invocation
that cannot run should not leave a run behind); the driver refuses again so a caller
driving the engine programmatically gets the same answer. If only the verb refused, a
`RunConfig::executor(Kubernetes)` would run every node in-process while the operator
believed their placement was honoured — the one outcome worse than refusing. Both
produce `bootstrap-failed` with zero attempts and exit code `4`.

### 7. Can the `PolicyDiff` *name* the placement change? → **Not from resume; the structure diff does.**

The test plan asks for a policy diff "naming the placement change". The run artifact
records the two aggregate hashes and **no per-node policy**, so the resume core has
nothing finer to report — a limitation its own docs already state. `PolicyDiff` now
implements `Display` (both hashes, and the fact that resume proceeds), and the
*naming* comes from the surface that has the data: the structure diff over the graph
artifact reports the change as `policy.placement` with the declared value, which
`a_placement_change_is_visible_in_the_structure_diff` asserts. Making the resume path
name it would require the run artifact to carry per-node policy — a schema change no
M10 ticket owns.
