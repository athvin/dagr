# 119 · T104 — the blob port, local backend, and `DurableOutput` bridge

> **Milestone:** M10 · **Size:** M · **Type:** feature · **Components:** C10, C18, C27
> **Branch:** `feat/t104-blob-port-and-local-backend` · **Depends on:** T103 · **Blocks:** T106, T110

## Why / context

T103 gives payloads bytes. Those bytes need somewhere to live that **both** the
orchestrator and a task pod can reach, and ADR 115 §8 decided dagr ships that place
rather than making every remote task author write storage code. This is the clause of
**ADR 014 that ADR 115 supersedes** — "no built-in remote/object backend" — and the
supersession is narrow: the contract stays over an **opaque reference string the
output type provides**, dagr still never interprets a reference or a hash, and
`dagr-core` still gains no storage dependency.

The bridge is the elegant part, and it is why this ticket is small. `DurableOutput`
(`crates/core/src/assembly.rs:164`) already exists, already carries an optional
`DurableReferenceMeta { content_hash, size_bytes, scheme, produced_at_offset_ns }`,
and resume already depends on it — including out-of-band mutation detection via
`ReferenceExistence::Changed` → `ResumeRefusal::MutatedReference`. A blanket
`DurableOutput` implementation over `Payload` therefore hands remote outputs
content hashes, existence probes, resume rehydration, and M8 lineage rows **for
free**, through machinery that is already tested. Nothing new needs inventing on the
resume or lineage side.

The local backend is not a toy. A read-write-many volume mounted at the same path in
every pod is a legitimate production configuration where the cluster's CSI offers one,
and it is also exactly what `--dagr.force-roundtrip` and the CI end-to-end test use.
The S3-compatible backend (T110) is an addition behind the same port, not a
replacement — which is what keeps the network dependency out of this ticket entirely.

**⚠ RWX is a cluster capability to check, not an assumption — measured in T101.** The
reference cluster T101 ran against offers exactly one storage class (`civo-volume`,
Civo CSI) and it is **RWO**: a shared read-write-many volume is simply
**unavailable** there. So on a CSI without RWX the local backend cannot serve
pod-to-pod handoff at all, and **T110's object-store backend moves onto the critical
path** rather than being a later convenience. This ticket must therefore document RWX
as a precondition an operator verifies (`kubectl get sc`), and must not present the
shared-volume path as universally available.

## Objective

Add the blob port, its local implementation, and the bridge to the shipped durable
contract — with no network dependency anywhere in this ticket.

- Add a **new opt-in workspace crate** for blob storage, reachable from `dagr-cli`
  only behind a **default-off feature**, with **no dependency edge onto
  `dagr-core`** — mirroring the `dagr-render` / `dagr-metastore` boundary shape
  (C24, ADR 097 §5). A plain `cargo build --all` and `--no-default-features` must
  reach neither the crate nor any storage dependency.
- Define a **`BlobStore` port**: `put`, `get`, `head` (existence + size + hash),
  over an opaque key, with a classified error distinguishing *absent* from
  *transiently unreachable* from *corrupt* — the same three-way split
  `RehydrateError` already makes, so the bridge maps cleanly.
- Implement **`LocalFsBlob`**, writing under a configured root, with the **atomic
  write discipline the scratch store already uses** (write temp → fsync → rename →
  fsync dir, `crates/core/src/scratch.rs`). A reader must never observe a partial
  blob.
- **Content-address** blobs: the key incorporates a digest of the encoded bytes, so
  the same value written twice is one blob and a reference is self-verifying. Record
  the digest in `DurableReferenceMeta::content_hash` so resume's mutation detection
  works without extra plumbing.
- Provide the **blanket `DurableOutput` bridge** for `T: Payload`, so a remote
  output's `serialize_reference` names its blob and `rehydrate` fetches and decodes
  it. `dagr-core` keeps no knowledge of the store; the bridge lives outside core.
- Wire the **existence probe** so `plan_resume`'s `ReferenceExistence` is answered by
  `head` — `Present` / `Absent` / `Changed { actual }` / `CannotDetermine` — mapping
  a transient failure to `CannotDetermine` and a digest mismatch to `Changed`.

## Test plan (write these first — TDD)

**Port round-trip**
- Given a `Payload` value, when it is `put` and then `get` through `LocalFsBlob`,
  then decoding yields an equal value.
- Given the same value written twice, then both writes produce the **same key** and
  the store holds one blob (content addressing).
- Given two different values, then their keys differ.

**Atomicity and durability**
- Given a `put` interrupted before the rename (fault-injected), then a concurrent
  `get` sees `absent`, never a partial blob, and a subsequent `put` succeeds.
- Given a completed `put`, then the blob and its directory have been fsynced (the
  scratch store's existing discipline, asserted the way its tests assert it).

**Error classification**
- Given a missing key, then `get` reports **absent** and `head` reports absent —
  distinguishable from a permissions or I/O failure, which reports transient.
- Given a blob whose bytes no longer match its key's digest, then `get` reports
  **corrupt**, not a decoded value.

**The `DurableOutput` bridge**
- Given a `Payload` output produced through the bridge, then `serialize_reference`
  returns a reference that `rehydrate` turns back into an equal value.
- Given that output, then `durable_reference_meta` carries a `content_hash` and
  `size_bytes`, and the attempt's `attempt-outcome` record carries them (the T89
  path, unchanged).
- Given a run that produced a durable output and a resume of it, then the existence
  probe reports `Present` and the node is `satisfied-from-prior`.
- Given the blob deleted between runs, then resume refuses with `DanglingReference`
  naming the node.
- Given the blob **overwritten out-of-band** so its digest no longer matches, then
  resume refuses with `MutatedReference` naming both hashes — the existing gate,
  now reachable through a shipped backend.
- Given lineage enabled, then `output_produced` / `input_consumed` rows carry the
  blob URI and content hash through the existing M8 projection with no mapping
  change.

**Boundaries**
- Given `cargo tree -i dagr-core`, then the blob crate is absent from core's
  reverse-dependency tree; given `cargo tree -p <blob crate> -e normal`, then it
  reaches neither `dagr-core` nor `dagr-cli`.
- Given `cargo build --all` and `--no-default-features`, then no storage dependency
  is compiled and `dagr-core`'s runtime dependency set is still empty.

## Definition of done

- [ ] A new opt-in crate provides `BlobStore` (`put` / `get` / `head`) with a
      classified absent / transient / corrupt error split, behind a default-off
      `dagr-cli` feature and with no edge onto `dagr-core`.
- [ ] `LocalFsBlob` writes atomically (temp → fsync → rename → fsync dir); a reader
      never observes a partial blob.
- [ ] Blob keys are content-addressed; identical values collapse to one blob and the
      digest lands in `DurableReferenceMeta::content_hash`.
- [ ] A blanket `DurableOutput` bridge over `Payload` round-trips through the store;
      `dagr-core` holds no knowledge of it.
- [ ] The existence probe answers all four `ReferenceExistence` cases; resume refuses
      `DanglingReference` on deletion and `MutatedReference` on out-of-band overwrite.
- [ ] M8 lineage rows carry the blob URI and hash with no change to the projection.
- [ ] `cargo tree` proves the crate boundary; `--no-default-features` pulls no
      storage dependency; core's runtime dependency set is still empty.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, the
      rustdoc lint, and cargo-audit/deny where configured).

## Open questions

- **Digest algorithm.** ADR 115 does not name one, and `dagr-core` must not gain a
  hashing dependency (ADR 014's rationale: dagr never computes a content hash — but
  the *backend* may, since it lives outside core). The choice, its dependency, and
  its licence are recorded in-PR. The hash is opaque to core either way.
- **Blob key layout.** Whether keys are purely content-addressed or prefixed by
  run/node for operability is decided in-PR; content addressing is the requirement,
  the prefix is a convenience. Note the interaction with T110's GC: a purely
  content-addressed key shared across runs cannot be reclaimed by run age, only by
  reachability.

## Out of scope

- The S3-compatible backend and any network dependency — **T110**, behind this same
  port. This ticket ships no HTTP client.
- Intermediate-blob garbage collection in `prune` — **T110**.
- The pod-side attempt shard (a different artifact from a payload blob, though it may
  share the store) — **T106**.
- Requiring `Payload` at a registration site — **T105** / **T108**.
- Changing `DurableOutput`, `RehydrateError`, `ReferenceExistence`, or the resume
  gates. This ticket *implements against* them; ADR 014's contract is unchanged apart
  from the one superseded clause.
- Scope boundary restated: a local, embedded blob store on operator-supplied storage
  coordinates nothing and serves nothing — dagr remains not a scheduler, a
  distributed execution system, a coordinating metadata store, a web interface, a
  DSL, or a backfill orchestrator, and the graph's shape never changes at runtime.
