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

- **Operator infrastructure — RESOLVED (2026-08-04) by the ticket's own sanctioned
  alternative, and then superseded by a *different* blocker (see below).** The
  prerequisite block asks for a registry, pod-to-pod storage, and a cluster with
  headroom, and §Why / context itself permits "a disposable kind/k3s in CI" for the
  third. All three are satisfiable without operator provisioning, exactly as T101's
  Run B demonstrated on this machine:
  - **Registry** — none needed. The image is built in-job from the workspace and
    loaded straight into the node's image store with `kind load docker-image`, which
    is also the answer to the "does the demo image get published" question below.
  - **Pod-to-pod storage** — needs **no RWX class and no bucket** on a single-node
    `kind` cluster, but it is *not* free the way the first pass of this note
    claimed. A `kind` `extraMounts` entry mounts a host directory **into the node**,
    not into a pod; making it visible to a container still needs a `hostPath`
    volume plus a `volumeMount` in the pod spec. T101 Run B's `/dagr-blobs` was the
    node-side half of exactly that pair. So the infrastructure is satisfiable
    without operator provisioning; the **pod-spec half is missing from the shipped
    code**, which is the blocker recorded immediately below.
  - **Headroom** — a fresh single-node `kind` cluster has no other work on it, so
    the reference cluster's 102%-memory worst node is not in the picture.
  So the *stated* prerequisite is not what blocks this ticket.
- **BLOCKING, and NOT the prerequisite the ticket anticipated: the shipped pod spec
  cannot mount the shared container, so a placed attempt cannot report from a real
  pod.** Found 2026-08-04 while wiring the cluster job. The pod side
  (`crates/cli/src/exec_node.rs`) writes its output and its attempt shard to a
  **local filesystem path** — `LocalFsBlob::open(&args.blob_store)` — and the runner
  reads them back out of the orchestrator's own `blob_container` path
  (`RemoteAttemptConfig::blob_container`, a single `PathBuf` documented as "the blob
  container the pod writes its output and shard into, and this runner reads them out
  of"). For those to be the same bytes, the pod needs that path mounted. But
  `dagr_k8s::executor::PodSpec` has **no volume field**, and
  `dagr_k8s::client::pod_object` — the only translation to a real API object — emits
  a `Pod` with a container carrying image, command, resources, `restartPolicy`,
  `nodeSelector` and `tolerations` and **nothing else**: no `volumes`, no
  `volumeMounts`, no `env`, no `serviceAccountName`. The S3 route is closed too:
  `exec-node` refuses any reference that does not name the **local** backend, and a
  pod could not be handed object-store credentials in any case, because there is no
  environment plumbing to hand them through.

  This is **mechanism work, and this ticket's §Out of scope forbids it**: "Any
  mechanism work — T101–T110 own it. A failure here is fixed in the owning ticket."
  The owning ticket is T108 (the node runner and the pod spec it builds); a volume
  seam on `PodSpec` plus its translation in `pod_object` is the missing piece.

  **Exactly what is blocked, and what is not.** The blocker is *reporting*: anything
  that has to read a placed attempt's own artifact needs the pod and the
  orchestrator to share a blob container. So these three, and only these three, are
  unimplementable without faking them, which the prerequisite block explicitly
  refuses:

  - a placed node **running to completion** on a real cluster,
  - **terminal states** and the folded run artifact for a placed node,
  - **"every node executed exactly once"** evidenced from that artifact — including
    the OOM-kill case and the retry-with-backoff case, which are attempt outcomes
    read back the same way.

  What is **not** blocked, and is worth stating so the deferral does not grow: the
  adoption half of the kill-and-restart guarantee. `crates/cli/src/adoption.rs` uses
  only `api.list` and `patch_labels` and never touches a shard, so *"the restarted
  orchestrator adopted the live pod, did not resubmit it, and recreated no pod"* —
  asserted from pod UIDs and creation timestamps read back from the API — is
  provable against **real pods today**, and against real pods it is a strictly
  stronger assertion than the in-process fake's, because the identity it matches on
  has round-tripped through an API server. Only the *"…and the run then completes"*
  clause needs the missing seam.

  **The cluster job stays deferred regardless**, because standing it up is part of
  the operator decision this ticket is halted on, not something to improvise. The
  non-cluster half — every boundary invariant, the wired run path against T107's
  fake API surface, RBAC, dual-mode parity, the docs claims and the local demo — is
  complete and green.
- **kind or k3s in CI, and how long does the job take? — RESOLVED: kind, and the
  cluster job is deferred with the blocker above.** T101 recorded kind for
  measurement (Run B), and the same choice is right here: `kind load docker-image`
  removes the registry prerequisite entirely, and on a single-node cluster an
  `extraMounts` host directory plus a `hostPath` volume in the pod spec would be a
  genuine shared container — the second half of that pair being precisely what does
  not exist. Wall clock, from T101's
  own timings: ~2m46s to create the cluster cold, plus the image build. That is too
  slow for every PR, so the intended shape — to be written by whoever unblocks the
  ticket — is a separate `remote-cluster` workflow on `workflow_dispatch` plus a
  nightly schedule, with the non-cluster suite still gating merges and the boundary
  stated in the job's own comment. **No such workflow ships in this PR**: a cluster
  job that cannot complete a placed attempt would assert something weaker than it
  claims, which §Why / context names as unacceptable.
- **Does the demo image get published? — RESOLVED: no.** Built in-job from the
  workspace and loaded into the node's image store, so the image digest always
  matches the orchestrator and the version-skew invariant is automatic.
- **macOS coverage — RESOLVED: unchanged.** The cluster job is Linux-only; macOS
  keeps the non-cluster suite. In this PR the whole shipped suite, including the
  wired remote run path against the fake API surface, runs on both tiers — the
  `--features k8s` step in `.github/workflows/ci.yml` is not `ubuntu`-gated. The
  Linux-only divergence begins with the cluster job, which is deferred.

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
