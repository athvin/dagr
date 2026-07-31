# dagr-blob

The **blob port** and its **local filesystem backend** — the place a dagr payload's
bytes live so that both the orchestrator and a remote task can reach them.

This crate is **opt-in** and **dependency-free**. It is reached from `dagr-cli`
only behind a default-off `blob` feature, and it has **no dependency edge onto
`dagr-core`** — the same boundary shape `dagr-render` and `dagr-metastore` keep.
A plain `cargo build --all` reaches no storage dependency through the pipeline
binary, and `dagr-core`'s runtime dependency set stays empty.

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
- **`BlobKey` / `BlobRef`** — content addressing and a self-describing reference
  string. The key is `sha256:<hex>` over the stored bytes, so the same value
  written twice is one blob and a reference verifies itself; the reference is
  `dagr-blob+<backend>://<container>/<algorithm>/<hex>`.

## What it is not

It is not a coordinator, a service, or an index. Nothing here schedules, watches,
or talks to another process: a `put` is a file write and a `get` is a file read.
The store holds bytes under a key an author's output type chose to name; dagr
never interprets a reference or a hash.

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

## Digest

Blobs are addressed by **SHA-256**, implemented in this crate with **no
dependency** (FIPS 180-4, checked against the published vectors). A content
address needs collision resistance — two different values that collapsed to one
key would be silent data corruption — and it is a hash of public bytes, never a
MAC or a secret-bearing construction. Keeping it in-tree is what lets
`cargo build --all` compile no storage dependency at all.
