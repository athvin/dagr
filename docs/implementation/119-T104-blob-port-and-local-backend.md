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
  The single-orchestrator remote-execution carve-out this ticket serves is **ADR
  115** (ticket 115 · T100); the local/embedded-store carve-out is **ADR 097**
  (ticket 097 · T82).

---

## Decisions recorded

Every open question this ticket carried, resolved with its evidence. (`docs/tasks.md`
stops at T70 — the M9–M11 tickets were appended under `docs/implementation/` only —
so this ticket has **no** additional `Q:` items to answer; checked, not assumed.)

### 1. Digest algorithm (the ticket's first open question)

**Decided: SHA-256, implemented in `dagr-blob` itself, with no dependency and
therefore no licence.**

A content address needs **collision resistance** — two different values collapsing
onto one key is silent data corruption — so the dependency-free FNV-1a that
`dagr-core` carries for fingerprints is categorically unsuitable, and the choice was
between a third-party cryptographic hash and an in-tree one.

In-tree wins because of what the ticket's own Boundaries test asks for: *"given
`cargo build --all` … then no storage dependency is compiled."* `cargo build --all`
builds every workspace member, so the only way that assertion holds robustly is a
blob crate whose dependency table is **empty**. It also keeps `deny.toml`'s
allow-list and the `cargo audit` surface untouched: this ticket adds **zero** crates
to the lockfile.

The risk normally attached to hand-rolled crypto does not apply in the shape that
makes it a risk. SHA-256 is a fully specified, fixed function (FIPS 180-4) used here
as a hash of **public** bytes — no MAC, no secret material, no key schedule, no
constant-time requirement — and its correctness is *checkable*, not asserted:
`crates/blob/src/digest.rs`'s tests run the published NIST vectors (empty, `abc`,
the two multi-block messages, and the one-million-`a` vector fed in irregular
997-byte chunks so the streaming block boundaries are exercised rather than aligned
away), plus a streaming-equals-one-shot property.

The algorithm is **named in every artifact it produces**: a key renders as
`sha256:<hex>` and a reference carries `/sha256/<hex>`, so changing the function
later is a reference-visible change rather than a silent swap that would make old
recorded hashes un-verifiable. `BlobKey::from_parts` refuses any other algorithm.
The hash stays opaque to `dagr-core`, which computes and interprets none of it.

### 2. Blob key layout (the ticket's second open question)

**Decided: purely content-addressed — no run or node prefix.**

The key is the digest of the encoded bytes and nothing else, so the same value
produced by two nodes, or by the same node in two runs, is **one** blob. That is the
property that makes a reference self-verifying and dedup free, and prefixing it with
run/node would destroy both for an operability gain that the reference already
provides: the reference names its container, `LocalFsBlob::object_path` maps a key to
its file, and the M8 lineage rows (`output_produced` / `input_consumed`) already
answer "which run and node produced this URI" — from the index, which is where that
question belongs.

The **physical** layout adds two levels of digest fan-out
(`<root>/sha256/<aa>/<bb>/<hex>`) so no single directory grows without bound; the
**reference** stays logical (`dagr-blob+file://<root>/sha256/<hex>`), which is what
lets the object-store backend use the same grammar with a different physical layout.

**The GC interaction the ticket flags is real and is recorded for T110:** a purely
content-addressed blob shared across runs **cannot be reclaimed by run age**, only by
**reachability** — a blob is collectable when no retained artifact still references
its URI. `prune` therefore cannot simply delete blobs under an old run's directory,
because there is no such directory. That constraint is stated in `crates/blob/src/lib.rs`
where the absence of a `delete` operation is documented, and it is T110's to
implement.

### 3. Where the bridge lives: `dagr-cli`, not `dagr-blob`

The ticket requires both "no dependency edge onto `dagr-core`" (for the blob crate)
and a blanket `DurableOutput` bridge over `Payload` — and `DurableOutput` and
`Payload` are both `dagr-core`'s. Those cannot both be true inside `dagr-blob`, so
the bridge lives in `dagr-cli` behind the default-off `blob` feature: the one crate
where the port and the contract are both visible. The ticket's own wording anticipates
this — *"the bridge lives outside core"*, not "inside the blob crate" — and the
Boundaries test it asks for (`cargo tree -p <blob crate> -e normal` reaching neither
`dagr-core` nor `dagr-cli`) is only satisfiable this way.

Rust's orphan rule then fixes the bridge's **shape**: `impl<T: Payload> DurableOutput
for T` is illegal outside the crate that defines one of them, so the blanket impl is
over a local generic type — `impl<T: Payload> DurableOutput for Blob<T>`. `Blob<T>`
carries the value, its reference, and its size, and `Deref`s to the value; every
`Payload` gets the bridge through one generic impl, which is the "blanket" the ticket
asks for in the only form the language permits.

### 4. `rehydrate` reaches its store through the reference, not through ambient state

`DurableOutput::rehydrate` is a **static** method with no store parameter, so the
bridge needs some way to reach a store in another process. It takes the reference's:
the reference names its backend and container, so `rehydrate` parses it and opens
exactly that store. No global registry, no process-wide default, no ambient
configuration — which is also what makes `rehydrate` testable without wiring, and
what keeps a reference meaningful in a pod that shares only the volume.

### 5. An unreadable reference is `Transient` / `CannotDetermine`, never `Absent`

`Absent` means *the referent is gone*, and it is the verdict that **refuses a resume
plan up front** (`DanglingReference`). So a reference this build cannot parse, or one
naming a backend it has no store for (`dagr-blob+s3://…` before T110), maps to
`Transient` at rehydration and `CannotDetermine` at the probe. Answering `Absent`
there would refuse a resume over a value that is very probably still present. The plan
proceeds and the real failure surfaces at rehydration, named — which is exactly what
`CannotDetermine` is documented to be for.

The same reasoning covers the one case where the probe *sees* a mismatch but no hash
was recorded: `ReferenceExistence::Changed` is defined as a recorded-hash mismatch and
drives a refusal that names a recorded hash, so with none recorded the probe returns
`CannotDetermine` and the mismatch surfaces at `get`, which refuses those bytes as
**corrupt** naming both digests. In practice the case is vestigial — the bridge always
supplies a `content_hash` — but the probe does not depend on that being true.

### 6. `head` recomputes the digest; `put` writes unconditionally

`head` reads the object and hashes it rather than trusting size or mtime, because the
whole point of the probe is to catch an **out-of-band overwrite**: any cheaper
predicate would report `Present` for exactly the case `MutatedReference` exists to
refuse. It streams in 64 KiB chunks, so the cost is bounded by blob size and never by
memory. (An object store can serve the same answer from stored metadata; that is
T110's, behind the same port.)

`put` always writes, rather than skipping when the object path exists. Content
addressing makes rewriting harmless — the bytes are the same bytes — it keeps one code
path, and it makes a `put` **self-healing**: re-publishing a value whose object was
damaged out-of-band repairs it, instead of trusting a path's existence
(`a_put_overwrites_a_damaged_object_in_place_and_is_self_healing`).

### 7. RWX is documented as an operator precondition, not assumed

Per the ticket's ⚠ correction from T101's spike: the local backend serves pod-to-pod
handoff **only** on a read-write-many volume mounted at the same path in every pod,
and the reference cluster's single storage class is RWO. This is written where an
operator will meet it — `crates/blob/README.md` (which is also the crate's rustdoc
front page) and `LocalFsBlob`'s own docs — with the `kubectl get sc` check and the
explicit statement that on a CSI driver without RWX this backend serves single-machine
runs and nothing wider. Neither document presents the shared-volume path as
universally available.

### 8. What this ticket deliberately did not do

No network client, no `delete`/GC, no attempt shard, no `Payload` requirement at any
registration site, and no change to `DurableOutput`, `RehydrateError`,
`ReferenceExistence`, or the resume gates — the bridge implements *against* them. The
M8 lineage projection (`crates/metastore/src/mapping.rs`) is untouched, which is the
point of the lineage test: a blob URI is just a URI and its digest is just a hash, so
the existing rows carry them unchanged.
