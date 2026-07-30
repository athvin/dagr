# 125 · T110 — the S3-compatible backend and intermediate-blob GC

> **Milestone:** M10 · **Size:** M · **Type:** feature · **Components:** C18, C26
> **Branch:** `feat/t110-s3-backend-and-blob-gc` · **Depends on:** T104, T108 · **Blocks:** T112

## Why / context

T104 shipped the `BlobStore` port with a local-filesystem backend, which is sufficient
for a shared read-write-many volume and for every test in the milestone so far. This
ticket adds the backend most clusters actually want — an S3-compatible object store —
behind the same port, and it pays a debt the milestone has been accumulating.

**The debt: nothing deletes intermediate blobs.** Every remote node attempt writes its
output to the store, and until now nothing ever removes one. A pipeline run nightly
leaks its full intermediate set every night, forever. That is not a tolerable shipping
state, and `prune` (C26) is the verb that already owns retention — it deletes run-store
directories and scratch today, so extending it is the honest place for this rather than
a new concept.

The GC has a genuine hazard that content addressing creates. T104's keys incorporate a
digest, so **the same value produced by two runs is one blob**. Reclaiming by run age
would therefore delete a blob that a *newer* run still references — including a blob a
resume needs to rehydrate. So the criterion must be **reachability, not age**: a blob is
reclaimable when no retained run artifact references it. This also interacts with
resume's mutation detection: deleting a referenced blob turns a future resume's refusal
from `MutatedReference` into `DanglingReference`, which is correct but must be
deliberate.

The dependency consequence is real and is the reason this is its own ticket rather than
part of T104: an S3 client brings an HTTP stack and credential handling into a workspace
whose lockfile had zero network crates before M10. It goes in the same opt-in crate
behind the same default-off feature, and `deny.toml` grows accordingly.

## Objective

Add the object-store backend and make intermediate blobs reclaimable.

- Implement **`S3Blob`** against T104's `BlobStore` port: `put`, `get`, `head`, with the
  same classified **absent / transient / corrupt** error split, so the
  `DurableOutput` bridge and the resume existence probe work unchanged.
- **Credentials come from the environment**, in the conventional precedence for the
  chosen client (environment, then the ambient provider chain), so a pod gets them by
  standard cluster mechanisms — a projected service-account token, an IRSA-style
  role, or injected secrets. dagr **holds no credential of its own** and adds no
  credential surface.
- **Retry transient failures** with bounded backoff inside the backend, reusing the
  engine's existing backoff shape rather than a second policy. A permanent failure is a
  classified error; a transient one that outlives the bound is `CannotDetermine` to the
  existence probe, never a false `Absent` (a false absent would turn a healthy resume
  into a spurious `DanglingReference` refusal).
- **Endpoint and bucket are operator configuration** (flag > env > default), so an
  S3-compatible store that is not AWS works without code changes.
- Extend **`prune`** to reclaim intermediate blobs by **reachability**: a blob is
  reclaimable when no retained run artifact references it. Age is never the criterion.
- Make the reclaim **safe by default**: a dry-run listing what would be deleted, an
  explicit opt-in to actually delete, and a refusal to run when any run artifact under
  the base is unreadable (an unreadable artifact means unknown reachability, so
  deleting would be a guess).

## Test plan (write these first — TDD)

**Backend parity — the same tests, both backends**
- Given T104's full port test suite, when it is run against `S3Blob` (via a local
  S3-compatible fixture), then every assertion holds identically to `LocalFsBlob` —
  round-trip, content addressing, deterministic keys, and the three-way error split.
- Given a `Payload` output stored through `S3Blob`, then the `DurableOutput` bridge
  round-trips it and `durable_reference_meta` carries hash and size.
- Given a resume against blobs in `S3Blob`, then `Present` / `Absent` / `Changed` /
  `CannotDetermine` are all reachable and produce the existing resume outcomes.

**Transient vs absent — the distinction that protects resume**
- Given the store unreachable, then `head` reports **transient**, the probe reports
  `CannotDetermine`, and resume does **not** refuse with `DanglingReference`.
- Given a genuinely missing key, then `head` reports **absent** and resume refuses
  `DanglingReference` naming the node.
- Given transient failures that resolve within the bound, then the operation succeeds
  after retry; given failures that outlive it, then a classified error surfaces with
  the attempt count.

**Credentials**
- Given no credentials available, then the failure names what was looked for and is
  distinguishable from a missing object.
- Given credentials, then no credential value appears in any log line, event record, or
  error message (asserted by scanning captured output).

**GC by reachability, not age**
- Given two runs that produced the **same** value (identical content hash) and the older
  run pruned, then the shared blob is **retained** because the newer run still
  references it — the test that would fail under age-based reclaim.
- Given a run whose artifact is retained, then none of its referenced blobs are
  reclaimed.
- Given a run whose artifact has been pruned and whose blobs no other artifact
  references, then its blobs are reclaimed.
- Given a blob no artifact references at all (an abandoned run's leftover), then it is
  reclaimed.
- Given an unreadable run artifact under the base, then `prune` **refuses** to reclaim
  blobs and says why — unknown reachability is not an excuse to guess.
- Given dry-run, then the listing matches exactly what a subsequent real run deletes,
  and nothing is deleted in the dry run.
- Given a resume that needs a blob the operator just reclaimed, then it refuses with
  `DanglingReference` naming the node — the honest outcome, tested so it is not a
  surprise.

**Boundaries**
- Given `cargo build --all` and `--no-default-features`, then no HTTP/TLS stack or S3
  client is compiled; `dagr-core`'s runtime dependency set is still empty.
- Given `cargo deny check licenses`, then it passes with the new transitive
  dependencies.
- Given `scripts/check-metastore-acceptance-boundary.sh`, then it passes — a client is
  not a server.

## Definition of done

- [ ] `S3Blob` implements the T104 port with the same absent / transient / corrupt
      classification; T104's port suite passes against both backends.
- [ ] The `DurableOutput` bridge and all four `ReferenceExistence` outcomes work
      against `S3Blob`; a transient failure never reports `Absent`.
- [ ] Credentials come from the ambient environment; none appear in logs, events, or
      errors; a missing credential is distinguishable from a missing object.
- [ ] Endpoint and bucket are operator configuration following `flag > env > default`.
- [ ] Transient failures retry with bounded backoff reusing the engine's backoff shape.
- [ ] `prune` reclaims intermediate blobs by **reachability**; a blob shared by a
      retained run is never deleted; an unreadable artifact makes `prune` refuse.
- [ ] `prune` supports a dry-run whose listing exactly matches the subsequent real
      deletion, and deletion is opt-in.
- [ ] `--no-default-features` and `cargo build --all` pull no S3 client; `deny.toml`
      covers the new licences; core's runtime dependency set is still empty.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Which S3 client, and does it bring its own retry?** Chosen in-PR against the
  dependency and licence budget T101/T107 established. If the client retries
  internally, the backend must not stack a second policy on top — one bounded retry,
  recorded in-PR either way.
- **Does the GC belong in `prune` or its own verb?** `prune` is chosen because it
  already owns retention (C26) and already reasons over the run store, and a second
  retention verb would be two places to get wrong. Recorded in-PR; the reachability
  criterion is the requirement, the verb name is not.
- **Multipart upload for large payloads.** Deferred unless a real payload needs it; the
  port's `put` signature is chosen so adding it later is not a breaking change, and
  that is asserted by review, not by code here.

## Out of scope

- The port, the local backend, and the `DurableOutput` bridge — **T104**.
- Attempt shards, which share the store but are written by **T106** and read by
  **T108**; their lifetime follows the same reachability rule and needs no separate
  mechanism.
- The end-to-end demo and acceptance gate — **T112**.
- Any change to `plan_resume` or the resume refusal gates. This ticket makes
  `DanglingReference` *reachable by operator action* and tests it; it does not change
  the gate.
- Content-addressed caching across runs as a *feature* (skipping a node because its
  output blob already exists). Blobs collapsing is a storage property here, not a
  scheduling decision — that would be a cache, which no ADR authorises.
- Multi-region, replication, lifecycle policies, or bucket provisioning — the operator's
  and the cluster's job.
- Scope boundary restated: an object-store client and a reachability-based reaper add no
  coordination and no server; dagr holds no credential, hosts nothing, and reclaims only
  what its own artifacts no longer reference. dagr remains not a scheduler, a
  distributed execution system, a coordinating metadata store, a web interface, a DSL,
  or a backfill orchestrator, and the graph's shape never changes at runtime.
