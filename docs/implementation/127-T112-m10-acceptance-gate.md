# 127 · T112 — M10 end-to-end demo, RBAC, and acceptance gate

> **Milestone:** M10 · **Size:** L · **Type:** feature (gate) · **Components:** system-level
> **Branch:** `feat/t112-m10-acceptance-gate` · **Depends on:** T100–T111 · **Blocks:** —

## Why / context

Every prior M10 ticket tested against a fake API surface or a subprocess. This one runs
the real thing against a real cluster in CI, and it is the ticket that proves the
milestone's claims rather than asserting them — the same role T88 played for the
metastore and T65 for the system as a whole.

> ## ⛔ BLOCKING OPERATOR PREREQUISITE — do not start this ticket without it
>
> This ticket runs **real dagr DAGs in real pods**, and the infrastructure that needs
> is the **operator's to provision**, not something the implementer improvises. T101
> measured the reference cluster and found two of the three pieces simply absent. When
> this ticket comes up, **HALT and report** — do not work around any of the following:
>
> 1. **A container registry the cluster can pull from.** The pod re-enters the
>    pipeline binary (ADR 115 §2), so an image must be built and pushed. T101 found
>    **no in-cluster registry** on the reference cluster. Needed: a registry URL and
>    push credentials, or an explicit decision to build into the cluster's own image
>    store.
> 2. **Somewhere for pods to exchange payloads and attempt shards** — an
>    S3-compatible bucket **or** a storage class offering **RWX**. T101 found the
>    reference cluster offers exactly one class (`civo-volume`, RWO) and **no RWX**,
>    so on that cluster object storage is **mandatory**. Needed: bucket, region, and
>    credentials scoped to it; or a confirmed RWX class.
> 3. **A cluster with headroom for the concurrency the test asserts.** T101 declined
>    to run a 50-pod fan-out against the reference cluster because its worst node was
>    at **102% memory** and already carrying other work — a namespace quota bounds the
>    test's usage, not the node's existing pressure. Needed: a cluster (or a
>    disposable kind/k3s in CI) that can absorb the fan-out without evicting anything.
>
> **Improvisations that are explicitly NOT acceptable** as substitutes, because each
> would make the gate assert something weaker than it claims: smuggling payloads
> through `ConfigMap`s or `Secret`s (1 MiB limit, and not the shipped code path),
> skipping the fan-out assertion, or running the "end-to-end" test with a stubbed
> executor. If a prerequisite is missing, the ticket **stops** and says which one.

Two kinds of proof are owed, and they are different in character.

**The capability proof** is a demo an operator can read: a pipeline where one node is
placed on a pod with a declared size, run end to end against kind or k3s, producing a
run artifact indistinguishable in shape from a local one. Plus the property that
justifies the whole design — **kill the orchestrator mid-run, restart it, and every node
still executed exactly once** (T109's adoption, against real pods rather than a fake).

**The boundary proof** is structural, and it matters more, because ADR 115 moved a
*permanent* boundary and the only thing keeping that carve-out narrow is a test that
fails when it widens. ADR 097's precedent is the model: T88 asserted its boundary
invariants with `cargo tree` and a diff-scanning script, not with prose. M10's
invariants are: the orchestrator opens **no listener**, pods link **no metastore** and
hold **no database credential**, `dagr-core`'s runtime dependency set is still
**empty**, a build that does not ask for remote execution compiles **no HTTP/TLS
stack**, the terminal-state taxonomy still has **exactly nine members**, and
`OpenMode::RemoteSqld` / `SyncedReplica` are still `ModeNotImplemented`.

The dual-mode claim also needs a real test, because it is the promise the operator cares
about most: **the same binary, the same pipeline, run locally and remotely, produces
artifacts that differ only by policy** — and a run started under one executor can be
resumed under the other (ADR 115 §7's payoff, which exists precisely because placement
is policy and not execution class).

## Objective

Prove the milestone end to end, and pin its boundaries structurally.

- Ship a **runnable example** under the existing examples convention: a small pipeline
  with a placed node declaring CPU/memory, runnable with `--dagr.executor=local` and
  `--dagr.executor=k8s` from the same binary.
- Add a **CI job** that stands up kind or k3s (matching T101's recorded choice), builds
  the image, and runs the demo to completion, asserting terminal states and artifact
  shape.
- Add the **kill-and-restart test against real pods**: kill the orchestrator with pods
  live, restart it, and assert from the artifact that every node executed exactly once.
- Add the **dual-mode parity test**: the same pipeline run locally and remotely produces
  artifacts equal except for policy-derived fields; and a run started under one executor
  resumes under the other with a printed policy diff and no structural refusal.
- Ship the **complete least-privilege RBAC** the orchestrator needs — create, get, list,
  watch, delete, and patch on pods in **one** namespace, and nothing else — as reviewed
  manifests, plus the ServiceAccount and RoleBinding, with a test that the demo works
  under exactly those permissions and **fails informatively** without them.
- Add an **acceptance-gate script** in the style of
  `scripts/check-metastore-acceptance-boundary.sh` asserting every boundary invariant
  above, and wire it into CI.
- Extend `scripts/check-metastore-acceptance-boundary.sh` (or its M10 sibling) with the
  new assertions rather than relaxing it, and confirm the existing forbidden-surface
  scan still passes.
- Document remote execution in the **cookbook** and the **README**: when to reach for
  it, the per-node latency caveat from T101's numbers, the shared-volume vs object-store
  choice, and the RBAC an operator must apply.

## Test plan (write these first — TDD)

**Capability, against a real cluster**
- Given the demo pipeline and a kind/k3s cluster, when it runs with
  `--dagr.executor=k8s`, then every node reaches `succeeded`, one pod per placed node
  attempt was created, and the pods carried the declared resource requests (read back
  from the API).
- Given the run, then `events.jsonl` has gapless `seq`, folds into a `RunArtifact`, and
  the metastore projection (feature on) produces rows identical to a reconcile pass.
- Given a placed node whose task fails retry-eligibly, then it retries with real backoff
  and the artifact shows distinct try numbers.
- Given a placed node OOM-killed by a deliberately low memory limit, then the attempt is
  `failed` with an `OOMKilled` diagnostic and the taxonomy still has nine members.

**The kill-restart guarantee, for real**
- Given the orchestrator killed mid-run with pods live, when it restarts with the same
  run id, then live pods are adopted (not resubmitted), the run completes, and the
  artifact shows **every node executed exactly once**.
- Given that restart, then no pod was recreated (asserted from pod creation timestamps
  and UIDs).

**Dual mode**
- Given the same pipeline run locally and remotely, then the two run artifacts are equal
  except for policy-derived fields; terminal states and node outputs match.
- Given a local run resumed with `--dagr.executor=k8s`, then resume **proceeds** with a
  printed policy diff and completes; and the reverse direction likewise.
- Given `--dagr.executor=local` on the demo, then it runs with no cluster present and no
  warning about one.

**RBAC**
- Given exactly the shipped Role, then the demo completes.
- Given the Role with `watch` removed, then the failure names the missing permission
  rather than hanging or reporting a generic error. Likewise for `create` and `patch`.

**Boundary invariants — the gate**
- `cargo tree -p dagr-core -e normal --no-default-features` shows an **empty** runtime
  dependency set.
- `cargo build --all` and `cargo build --no-default-features` compile **no** HTTP/TLS
  stack and no Kubernetes or S3 client (asserted against the built dependency list, not
  the manifest).
- The pod path links **no** metastore: a pod's binary has no metastore write path
  reachable, and no code path passes a database credential to a pod.
- `OpenMode::RemoteSqld` and `SyncedReplica` still return `ModeNotImplemented`.
- No listener: the forbidden-surface scan for `TcpListener`, `::bind(`, `.serve(`,
  `.listen(`, server frameworks, and `*Scheduler` types passes over all added M10
  source.
- The terminal-state taxonomy has **exactly nine** members and the trigger-rule set
  exactly three.
- Every M10 numbered `arch.md` criterion appears exactly once in
  `docs/criteria-matrix.md` with a covering row in `docs/coverage-matrix.md`.

**Docs claims are true**
- Given the README and cookbook remote-execution sections, then a test asserts the
  claims they make about defaults, flags, and the absence of a server — the pattern
  `crates/cli/tests/metastore_docs_claims.rs` already establishes.

## Definition of done

- [ ] A runnable example ships a pipeline with a placed node, runnable under both
      executors from one binary.
- [ ] The operator-provided prerequisites (registry, pod-to-pod storage, a cluster
      with headroom) are confirmed present and recorded in-PR **before** any other box
      here is attempted.
- [ ] A CI job stands up kind/k3s, builds the image, and runs the demo to completion,
      asserting terminal states, pod count, and declared resource requests.
- [ ] The kill-and-restart test against real pods proves every node executed exactly
      once, with no pod recreated.
- [ ] Dual-mode parity holds, and resume works in both directions with a policy diff and
      no structural refusal.
- [ ] Least-privilege RBAC manifests ship; the demo works under exactly them and fails
      informatively when a verb is removed.
- [ ] An M10 acceptance-gate script asserts every boundary invariant listed above and
      runs in CI; the existing forbidden-surface scan passes and is extended, not
      relaxed.
- [ ] `dagr-core`'s runtime dependency set is empty; a default build pulls no HTTP/TLS,
      Kubernetes, or S3 code; pods link no metastore and carry no database credential.
- [ ] The terminal taxonomy has nine members; `RemoteSqld`/`SyncedReplica` are still
      unimplemented stubs.
- [ ] Criteria and coverage matrix rows exist for every M10 criterion.
- [ ] README and cookbook document remote execution, including T101's latency caveat and
      the RBAC requirement, with a docs-claims test.
- [ ] Tests pass on `ubuntu-latest`; the cluster job's platform support is documented
      (macOS runs the non-cluster suite).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Operator infrastructure — BLOCKING, unresolved.** See the prerequisite block in
  §Why / context. A registry, pod-to-pod storage (S3 or RWX), and a cluster with
  concurrency headroom must all be provisioned by the operator before this ticket can
  begin; T101 confirmed two are absent on the reference cluster. The operator has
  stated they will set this up when the ticket comes up (2026-07-29). **This ticket
  halts until then** — it is not startable, and no part of it should be faked to make
  progress.
- **kind or k3s in CI, and how long does the job take?** T101 recorded the choice for
  measurement; this ticket must also live inside a tolerable CI wall-clock. If the
  cluster job is too slow for every PR, it runs on a schedule or on a label with the
  non-cluster suite still gating merges — decided in-PR and stated in the job's own
  comment so the coverage boundary is visible rather than assumed.
- **Does the demo image get published?** No — built in-job from the workspace, so the
  image digest always matches the orchestrator and the version-skew invariant is
  automatic. Recorded in-PR.
- **macOS coverage.** The cluster job is Linux-only; macOS keeps the non-cluster suite.
  This is a documented platform divergence in the existing style, not a gap to close.

## Out of scope

- Any mechanism work — T101–T110 own it. A failure here is fixed in the owning ticket,
  or reopens ADR 115 if it contradicts a decision.
- Warm pods, pod reuse, batching, or any latency optimisation. If T101's numbers make
  per-node pods untenable, ADR 115 §2 reopens; this gate reports the measured reality
  rather than working around it.
- Foreign-image tasks, multi-namespace operation, and cross-run pod adoption — all named
  future work.
- A published Helm chart, operator, or CRD. dagr is invoked, not installed; a CRD plus a
  controller would be a control plane that outlives a run, which ADR 115 explicitly does
  not permit.
- Performance benchmarking of remote execution beyond reporting T101's latency figures.
  The per-node overhead benchmark stays a local-execution measurement by ADR 115.
- Scope boundary restated: the gate's whole purpose is to hold the carve-out at exactly
  the width ADR 115 granted — one orchestrator process, one graph, one run's lifetime,
  no inbound API, no served database, no second coordinating process. dagr remains not a
  scheduler, a distributed execution system beyond that carve-out, a coordinating
  metadata store, a web interface, a DSL, or a backfill orchestrator, and the graph's
  shape never changes at runtime.
