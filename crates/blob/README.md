# dagr-blob

The **blob port** and its backends — the place a dagr payload's bytes live so that
both the orchestrator and a remote task can reach them.

This crate is **opt-in** and **dependency-free**. It is reached from `dagr-cli`
only behind a default-off `blob` feature, and it has **no dependency edge onto
`dagr-core`** — the same boundary shape `dagr-render` and `dagr-metastore` keep.
A plain `cargo build --all` reaches no storage dependency through the pipeline
binary — not even for the S3-compatible backend, whose HTTP client lives
elsewhere (see below) — and `dagr-core`'s runtime dependency set stays empty.

## What it is

- **`BlobStore`** — a three-operation port over an opaque, content-addressed key:
  `put` (write bytes, get a key), `get` (fetch bytes, verified), and `head`
  (existence + size + hash). Every failure is classified **absent** /
  **transient** / **corrupt** — the same three-way split the durable-output
  contract's rehydrate error already makes, so a caller can map one onto the
  other without inventing a policy.
- **`LocalFsBlob`** — the local backend, writing under a configured root with the
  atomic write discipline the scratch store uses: write a temp file in the same
  directory, `fsync` it, `rename` it into place, `fsync` the directory. A reader
  never observes a partial blob.
- **`S3Blob`** — the object-store backend, over any S3-compatible bucket. Same
  port, same three-way classification, plus a **single** bounded retry on the
  engine's backoff shape and credentials that come only from the ambient
  environment.
- **`BlobReclaim`** — the operator-side half: enumerate a container, delete one
  blob. Deliberately a separate trait, so a node runner is handed a type that
  structurally cannot delete anything.
- **`BlobKey` / `BlobRef`** — content addressing and a self-describing reference
  string. The key is `sha256:<hex>` over the stored bytes, so the same value
  written twice is one blob and a reference verifies itself; the reference is
  `dagr-blob+<backend>://<container>/<algorithm>/<hex>`.

## What it is not

It is not a coordinator, a service, or an index. Nothing here schedules or
watches: a `put` is a write and a `get` is a read. The store holds bytes under a
key an author's output type chose to name; dagr never interprets a reference or a
hash. It holds **no credential of its own** and adds no credential surface — see
`s3::creds` for exactly what it reads out of the environment and what it
deliberately does not implement.

## ⚠ Pod-to-pod handoff needs RWX, and that is a cluster capability to check

The local backend serves pod-to-pod handoff **only** when the same directory is
mounted in every pod — i.e. on a **read-write-many** volume. That is a legitimate
production configuration where the cluster's CSI driver offers one, and it is
exactly what a single-machine run, the local codec round-trip, and the CI
end-to-end test use. It is **not** universally available: the reference cluster
this milestone's spike ran against offers exactly one storage class, and it is
**RWO** — a shared read-write-many volume simply cannot be provisioned there.

So this is a precondition an operator **verifies**, not an assumption:

```console
$ kubectl get sc                 # is a storage class offered at all?
$ kubectl get sc -o custom-columns=NAME:.metadata.name,PROVISIONER:.provisioner
$ # then confirm the driver's supported access modes — ReadWriteMany must be
$ # among them before a shared blob root can back pod-to-pod handoff.
```

On a CSI driver without RWX the local backend still works perfectly well for a
single-machine run and for anything sharing one filesystem; it just cannot be the
handoff between two pods. The object-store backend behind this same port is the
answer there.

## The object store, and where its HTTP client is not

`S3Blob` speaks the S3 protocol; it does **not** contain an HTTP client. The
protocol half — canonical requests, `SigV4` signing, status classification, paged
listing, the bounded retry — lives here and compiles with no dependencies. The
transport is a port (`s3::HttpTransport`), and the client that implements it, with
its TLS stack and its certificate verification, lives in `dagr-cli` behind a
default-off `blob-s3` feature.

Three things follow, and all three are asserted by
`scripts/check-blob-feature-gating.sh`:

- `cargo build --all`, `--no-default-features`, and even `--features blob` compile
  **no HTTP or TLS crate at all**. A pipeline on the local backend pays nothing
  for the object store.
- Every interesting failure is testable **in-process**: `s3::fake::FakeS3` can be
  made unreachable, made to fail a bounded number of times, made to answer a
  specific status, or have an object overwritten out-of-band. No test needs a
  network service.
- The trust boundary sits where the in-tree argument stops. SHA-256, HMAC and
  `SigV4` are fixed, fully specified functions with published vectors, so they are
  here. TLS, certificate-chain verification and HTTP framing are negotiated and
  adversarial, so they are a maintained crate's.

**Configuration is the operator's** — endpoint, bucket, region and key prefix,
each `flag > env > default` (`--dagr.blob.endpoint` / `DAGR_BLOB_ENDPOINT`, and so
on). An S3-compatible store that is not AWS works by pointing the endpoint at it.
**Credentials are the platform's**: dagr reads `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (and an optional `AWS_SESSION_TOKEN`), then a shared
credentials file — and there is no flag, no reference field, and no dagr-owned
file that can carry one.

## Reclaiming blobs: reachability, never age

`BlobReclaim` exists because a purely content-addressed key **cannot** be
reclaimed by age. The same value produced by two runs is one blob, so deleting
"old runs' blobs" would delete a blob a newer run still references — and that
newer run's resume still needs. There is no per-run directory to walk either.

So the criterion is that **no retained run artifact references the blob**, the
walk lives in `dagr-cli`'s `prune` (`dagr_cli::blob_gc`), and the reclaim is
opt-in twice: `--reclaim-blobs dry-run` lists exactly what
`--reclaim-blobs delete` would remove. An artifact that cannot be read, or a run
that has not been folded yet, makes `prune` **refuse** rather than guess — an
unknown reference is indistinguishable from no reference, and only one of those is
safe to act on.

## Digest

Blobs are addressed by **SHA-256**, implemented in this crate with **no
dependency** (FIPS 180-4, checked against the published vectors). A content
address needs collision resistance — two different values that collapsed to one
key would be silent data corruption — and it is a hash of public bytes, never a
MAC or a secret-bearing construction. Keeping it in-tree is what lets
`cargo build --all` compile no storage dependency at all.
