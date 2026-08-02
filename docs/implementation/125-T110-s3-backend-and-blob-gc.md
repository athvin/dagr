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

---

## Decisions recorded

Every open question this ticket carried, resolved with its evidence. (`docs/tasks.md`
enumerates M0–M4 only — the M9–M11 tickets were appended under `docs/implementation/`
alone — so this ticket has **no** additional `Q:` items to answer; checked in the file,
not assumed.)

### 1. Which S3 client, and does it bring its own retry? (the ticket's first open question)

**Decided: no S3 client crate at all. The protocol is in-tree in `dagr-blob`; only the
HTTP transport is a dependency, and it is `ureq` + `rustls` + `rustls-native-certs`
behind a new default-off `blob-s3` feature on `dagr-cli`.**

The forcing constraint is a boundary this milestone already asserted: `dagr-blob`
declares **no dependencies at all**, and `scripts/check-blob-feature-gating.sh` fails
the build if it ever grows one. That is what makes T104's *"`cargo build --all`
compiles no storage dependency"* true. Putting an S3 SDK there — even optionally —
would have meant weakening a shipped assertion, so the backend was split instead,
along the sans-IO line T104 and T107 already established:

- **`dagr-blob`** holds the whole protocol: request construction, AWS SigV4 signing
  (over an in-tree HMAC-SHA256 built on T104's SHA-256), the status→classification
  map, paged `ListObjectsV2`, and the bounded retry. Zero dependencies; every build
  compiles it.
- **`dagr-cli::blob_s3`** holds the socket, behind `blob-s3`. `cargo build --all`,
  `--no-default-features` *and* `--features blob` compile no HTTP or TLS crate —
  containment as strong as `dagr-k8s`'s `client` feature, and stronger than the
  metastore's.

The trust boundary is where the in-tree argument stops, and that line is drawn
deliberately rather than by convenience. T104 justified an in-tree SHA-256 as *"a
fully specified fixed function over public bytes, checkable against published
vectors"*. HMAC-SHA256 and SigV4 satisfy the same test — RFC 4231's vectors and AWS's
published key-derivation and canonical-request values are asserted in
`crates/blob/src/hmac.rs` and `crates/blob/src/s3/sigv4.rs`. TLS, certificate-chain
verification and HTTP framing do **not**: they are negotiated, adversarial, and carry
a standing security-maintenance obligation. So they are a maintained crate's.

**Retry: exactly one policy, and it is the backend's.** `ureq` is configured with
`max_redirects(0)` and does no retrying; the transport port's contract says so in
words (*"an implementation does not retry and does not interpret status codes"*).
`S3Blob` owns the single bounded retry, and the classified error names how many
attempts it spent. A **permanent** failure is never retried — a 403 is transient by
*classification* (it is not evidence of deletion) but is returned on the first
attempt, because a permission problem does not clear by waiting and spending the
budget on it only delays the diagnosis.

**Licence budget: no new SPDX id.** Audited crate by crate; every transitive licence
resolves into `deny.toml`'s existing five, and most of the tree (rustls, ring, http,
httparse, base64, bytes) is already in the lockfile via T107's Kubernetes client. One
addition *was* found and **designed out** rather than allowed: `ureq`'s default
`rustls` feature pulls `webpki-roots`, whose CA bundle is `CDLA-Permissive-2.0`. The
feature therefore takes `rustls-no-provider` and loads roots from the **platform trust
store** (`rustls-native-certs`, `Apache-2.0 OR ISC OR MIT`) — which is also the better
answer independently, because an operator's private CA then works by being trusted
where everything else on the host trusts it. A new assertion in
`check-blob-feature-gating.sh` fails the build if `webpki-roots` re-enters the
resolution.

### 2. Does the GC belong in `prune` or its own verb? (the ticket's second open question)

**Decided: `prune`, as the ticket proposed** — it already owns retention (C26) and
already reasons over the run store, and a second retention verb would be two places to
get wrong. The mechanism is `dagr_cli::blob_gc`, and `prune`'s verb body calls it
**after** its run-directory retention, because retention is what decides which
artifacts survive and the surviving artifacts are exactly what defines reachability.

The flag grammar is `--reclaim-blobs <dry-run|delete>` plus `--blob-store
<container>`. One value-taking flag rather than a bare toggle plus a `--force`: the
destructive mode has to be *typed*, and an unrecognized value is refused rather than
defaulted, so no typo can resolve to `delete`. A bare `prune` is byte-for-byte
unchanged.

### 3. Multipart upload (the ticket's third open question)

**Deferred, as the ticket directs, and the non-breaking-extension claim is now
checkable rather than asserted.** `BlobStore::put(&self, bytes: &[u8]) -> Result<BlobKey,
BlobError>` takes the bytes and returns the address; a multipart implementation
changes only how `S3Blob::put` transfers them and is invisible at the port. Nothing in
the reference grammar, the classification, the retry or the reclaim depends on the
transfer being a single request.

### 4. `Absent` is 404 and nothing else

The three-way split has no "permanent failure" class, so every non-404 failure —
including a 403 — classifies as **transient**. That is not a fudge: `Absent` is the
verdict that refuses a resume plan up front as a `DanglingReference`, and a
credential or policy problem is evidence that *this process could not look*, not that
anything was deleted. The probe turns it into `CannotDetermine`, the plan proceeds,
and the real failure surfaces at rehydration, named. A retry budget that outlives its
bound reports transient too, never a false absent — asserted directly
(`a_transient_failure_that_outlives_the_bound_surfaces_with_the_attempt_count`,
`an_unreachable_object_store_does_not_turn_a_resume_into_a_dangling_reference`).

### 5. `head` measures the hash; it does not trust stored metadata

An object store can answer size from a cheap `HEAD`, and can serve back whatever user
metadata a writer attached — and neither answers the question the probe exists for.
The probe's job is catching an **out-of-band overwrite**, and an overwrite replaces an
object's metadata along with its bytes, so any predicate cheaper than reading would
report `Present` for exactly the case `MutatedReference` exists to refuse. `S3Blob::head`
therefore reads the object and hashes it, exactly as `LocalFsBlob::head` does. The cost
is bounded by blob size and is documented at the method.

### 6. Credentials: the environment and a credentials file; **no STS exchange**

`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`), then
`AWS_SHARED_CREDENTIALS_FILE` / `~/.aws/credentials` under `AWS_PROFILE`. Web-identity
(IRSA) **token exchange** is deliberately not implemented: trading a projected
service-account token for temporary credentials means calling STS — a second service,
a second signing path, and a background refresh loop — which is a credential *broker*,
i.e. precisely the surface this ticket's objective says dagr does not add. An operator
on IRSA runs that exchange where every other workload does and dagr reads the result.
The refusal names every variable and file consulted, so hitting it is a five-second
diagnosis; and it is **not** a `BlobError`, so it can never be mistaken for a missing
object.

`S3Credentials` has no `Display`, its `Debug` renders `<redacted credential>` for
every field, and the secret is reachable only through `expose_secret()`, whose single
caller is the signer — asserted mechanically by a new check in
`check-blob-feature-gating.sh`, alongside checks that no `--dagr.blob.*` flag and no
`DAGR_BLOB_*` variable names a secret.

### 7. The retry shape is the engine's, reproduced rather than imported

The ticket asks the backend to reuse *"the engine's existing backoff shape rather than
a second policy"*. `dagr_core::execution::Backoff` is the shape (`base · factor^n`
clamped to `cap`), but `dagr-blob` cannot depend on `dagr-core` — that boundary is why
the crate exists. So `dagr_blob::retry::RetryBudget` reproduces the curve and a
**parity test in `dagr-cli`** — the one crate where both types are visible — pins them
equal across a matrix of parameters and attempt indices
(`the_backends_retry_budget_is_the_engines_backoff_shape`). Jitter is deliberately
absent: the engine jitters *node* retries because many nodes retry against one
downstream at once, whereas a blob retry is already inside one attempt whose start the
engine jittered.

### 8. Reachability over-approximates, on purpose

Getting reachability wrong is not symmetric. A missed reference deletes a blob a run
still needs — silent, permanent loss whose first symptom is a resume refused months
later. An extra "reference" keeps a dead blob one prune longer. Every uncertain case
therefore resolves toward keeping:

- reference extraction walks the **whole** artifact document and takes every string
  that parses as a blob reference in this container, rather than reading the three
  fields that carry one today (`outputs[].uri`,
  `attempts[].durable_reference`, `attempts[].inputs[].uri`). A schema that grows a
  fourth cannot silently make live blobs collectable;
- an artifact that cannot be read is a **refusal**, not a zero-reference artifact;
- a run directory with an event stream and no folded `run.json` is a refusal too,
  naming `fold` — its references are still in the stream, so its blobs would look
  unreferenced when they are not;
- a missing `--store` base is a refusal rather than "no artifacts", because a mistyped
  base would otherwise make every blob in the container look garbage;
- a reference naming a different container contributes nothing and is not evidence
  about this one.

Attempt shards (T106) share the container and are never enumerated as blobs: the local
backend's `list` walks `<root>/<algorithm>/` and keeps only names that are valid
content addresses, so `attempt-shards/` and hidden write debris are structurally
invisible to it; the object-store `list` scopes its prefix the same way.

### 9. `RUN_ARTIFACT_FILE_NAME` promoted to library surface

`"run.json"` was a private constant in the T56 acceptance sample. The reaper's whole
notion of reachability is "the references in the retained run artifacts", so it has to
recognize one on disk — and a second string literal for the same reserved run-store
name is exactly the drift the run-store contract (ADR 012) exists to prevent. It now
lives in `dagr_cli::run_store` beside `DEFAULT_STORE_BASE`.

### 10. Knobs not yet in arch.md's C26 table

`--dagr.blob.endpoint` / `.bucket` / `.region` / `.prefix` and `--reclaim-blobs` are
reserved in `contract::reserved_flag_names()` and follow `flag > env > default`, but
arch.md's C26 env-fallback table is not amended here — this ticket does not own that
decision, and there is precedent (`--dagr.pod-launch-retries`, added by T108, is not in
it either). T117 (the knob mapping table) is the ticket that reconciles the table with
the shipped set; this entry is the record it will need.
