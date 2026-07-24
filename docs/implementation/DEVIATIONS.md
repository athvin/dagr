# Deviations

Deliberate departures from a ticket's Definition of done are recorded here, one
entry each, with: date, ticket, the quoted DoD line, the deviation, its
rationale, and the operator decision it traces to. A matching note goes in the
PR body. Merged decision text elsewhere is never rewritten; this file is the
audit trail for where reality diverges from a DoD line on purpose.

---

## 2026-07-23 · 002 (T0.0b) — autonomous merge vs "every PR requires review"

**Quoted DoD line.** *"A `CODEOWNERS` file exists at a GitHub-honoured location
and assigns review ownership such that every PR requires review before merge
(satisfying the arch.md commitment that the criteria matrix and release
checklist are reviewed like code, and that core-crate dependency additions are
reviewed as API decisions)."*

**Deviation.** `.github/CODEOWNERS` and `CONTRIBUTING.md` are authored exactly as
ticket 002 specifies — a repo-wide owner is assigned and the process contract
states that every PR requires review before merge. However, the mechanism that
would *enforce* required Code-Owner review — GitHub branch protection with
"Require review from Code Owners" — is **not enabled**, and PRs on the ticket
loop are **squash-merged autonomously** by the orchestrator without a
second-party human review.

**Rationale.** Ticket 002 explicitly scopes *"Branch-protection rules configured
in the GitHub UI/API"* out as an operator action outside the repository (its Out
of scope list). CODEOWNERS assigns ownership; only branch protection turns that
into a hard requirement. With enforcement off, the CODEOWNERS assignment is the
recorded intent, and the autonomous squash-merge is the operating reality. The
written contract (review-before-merge) is preserved as the documented norm for
human contributors; the loop is the exception, not the rule.

**Operator decision.** The dagr ticket-loop is run unattended with autonomous
squash-merge per operator policy (the `shipping-dagr-tickets` skill's settled
autonomous-merge decision). This entry is the standing record referenced by the
ticket-conventions §10 "known standing case."

---

## 2026-07-23 · 042 (T32) — supersedes T31's driver-guard over-demand test

**Affected artifact.** `crates/cli/tests/admission_driver.rs`, the T31 (041)
test formerly named `an_over_demand_node_is_failed_terminally_not_silently_stranded`.

**Change.** T31 shipped a *defensive* driver-level guard that caught a
can-never-fit node (declared cost exceeding a pool's total capacity) inside the
run loop and folded it to a `Failed` terminal, because — by T31's own comments —
"the full bootstrap-time rejection of too-big nodes is deferred to T32". T32
implements that authoritative rejection: a too-big node now fails the run at
**bootstrap, before any node executes**, with the distinct `bootstrap-failed`
outcome (arch.md C12 acceptance: "fails at bootstrap, not at admission time").
The bootstrap check therefore intercepts the over-demand node before the loop's
guard is reached. The T31 test's expectation was updated to the T32 behaviour
(renamed to `an_over_demand_node_is_rejected_at_bootstrap_not_silently_stranded`,
now asserting `RunOutcome::BootstrapFailed` and that nothing executed).

**Rationale.** This is a ticket-conventions §10 **supersession**, not a DoD
deviation: T32 owns the "too-big rejection" behaviour, and arch.md's C12
acceptance criterion mandates the bootstrap-time outcome the test now asserts.
The T31 *permit mechanics* (`admission.rs`) and the T31 driver guard code
(`can_ever_fit` / `reject_over_demand`) are **unchanged** — the guard is retained
as a defensive backstop, merely unreached on the default drive path. No test id
referenced by `docs/coverage-matrix.md` was renamed (the matrix maps T31's driver
integration to `a_pinned_pool_admits_one_node_at_a_time_and_the_run_still_completes`,
which is untouched).

**Operator decision.** Traces to the arch.md C12 acceptance criterion and the
T32 ticket DoD, which the loop implements autonomously.

---

## 2026-07-23 · 052 (T41) — fingerprint hash function is FNV-1a, not BLAKE3

**Affected decision.** The T0.7 ADR
(`docs/implementation/013-T0.7-stable-name-and-fingerprint-adr.md`) §6 names
**BLAKE3** as the v1 fingerprint hash function: *"A single named hash function.
Both hashes use one cryptographic hash function, named once here: BLAKE3 … a
pure-Rust implementation, which keeps the core crate's dependency set minimal."*
T41 implements the T0.7 composition, so it inherits that naming; T41's own DoD
requires cross-toolchain-identical hashes but does not itself name the function.

**Deviation.** Algorithm **v1 uses FNV-1a** — the dependency-free digest already
in the tree (`dagr_core::handle::NodeId`, the T40 build script) — not BLAKE3. The
digest is computed in `dagr_core::assembly`, exposed through
`Pipeline::fingerprint()` / `FingerprintSlot`, and written into the graph header
as a version-prefixed `fnv1a-64:v1:<hex>` string. `dagr-core` stays
dependency-free and `deny.toml` is unchanged.

**Rationale — the ADR's own anticipated reopen condition, not a local
work-around.** T0.7 §Consequences "Reopen condition" states: *"if BLAKE3 proves
unavailable under the pinned MSRV or the supply-chain policy — the contract
reopens here … rather than being worked around locally."* Adding `blake3` is
**unavailable under dagr's supply-chain policy**. `deny.toml` allows the **MIT**
license only (plus `Unicode-3.0` for one build tool). Verified via `cargo
metadata` for `blake3 = { version = "1", default-features = false }`: `blake3` is
`CC0-1.0 OR Apache-2.0 OR Apache-2.0 WITH LLVM-exception` (no MIT option); its
transitive `arrayref` is single-licensed **BSD-2-Clause** (cannot resolve to
MIT); `constant_time_eq` is `CC0-1.0 OR MIT-0 OR Apache-2.0` (MIT-0, not MIT).
Admitting BLAKE3 would require widening the MIT-only allow-list to Apache-2.0 +
BSD-2-Clause + CC0-1.0/MIT-0 — a reviewed loosening of the supply-chain gate — and
pull a `cc` build-time C-toolchain dependency (absent the `pure` feature).

FNV-1a satisfies **every** C21 property the ADR's guarantee rests on: pure
integer arithmetic, no float/locale/platform dependence, so identical
author-declared inputs yield byte-identical digests on any machine or toolchain
(the two-toolchain CI job asserts this). Collision resistance is weaker than a
256-bit cryptographic hash, but the fingerprint is a **shape-identity** for
resume/diff gating (C27/C28), not a security primitive; the weaker guarantee is
documented on `FingerprintSlot`, and a stronger hash remains available later as an
**algorithm-version bump** (the mechanism T0.7 §7 provides for exactly this).

**Operator decision.** Traces to the merged `deny.toml` MIT-only license policy
(T7 / 006) and the standing constraint to keep `dagr-core` dependency-free and
prefer no `deny.toml` change. Consistent with the already-merged T14/T29 stand-in
(FNV-1a, BLAKE3 pending). Not a spec conflict: the ADR pre-authorized this
fallback and named its trigger, and the deviation is recorded at the public
surface and here. Adopting a different function later is an algorithm-version bump
(T0.7 §7).

---

## 2026-07-23 · PREREQUISITE fix — T19 event-stream writer never conformed to the T39-published event-stream@1 schema

**Quoted DoD line.** T19 (029): *"Each record is one compact JSON object per
line … carrying the T0.6 §7 header"*, and arch.md l.331 (normative): *"Every
attempt produces exactly one attempt-outcome record in the event stream
(alongside its per-transition events)."*

**Deviation (a defect, now reconciled).** The C19 `EventStreamWriter`
(`crates/artifact/src/event_stream.rs`, T19) shipped a wire form that **diverged
from the ratified published schema** `schemas/event-stream/v1.schema.json`
(T39/050): a real writer stream could not be validated against, nor folded
(C22/T42) from — violating end-to-end C19↔C22. The schema is the ratified
contract, so the reconciliation is writer→schema (the schema is unchanged). The
divergences fixed in the writer's wire output:

- discriminator key `"event"` → `"kind"`.
- per-kind payload nested under `"body"` → **spread top-level** (`header` /
  `node` / `attempt` / `status` / `state` / `outcome` per the schema's per-kind
  shapes).
- `wall`: integer Unix-millis → **RFC3339 string** (schema types `wall` a
  non-empty string). The writer's time seam changed `fn() -> u64` → `fn() ->
  String`; the monotonic `offset_ns` stays the authoritative integer.
- header field names/shapes: `captured_env` → `captured_environment`;
  `resumed_from` → `resume_lineage` (an `{run_id}` object, `object|null`); added
  `run_id` and `fingerprint_algorithm_version`; `data_interval` emitted as a
  `{start,end}` **object** (not a `[start,end]` array). The schema requires the
  two fingerprint fields on **every** `run-started` header, so the assembly-failed
  path (no fingerprints) records a documented `"unavailable"` sentinel
  (`FINGERPRINT_UNAVAILABLE`) that the C22 fold reads as absent.
- added the single rich **`attempt-outcome`** record per attempt, kept
  **alongside** the per-transition `attempt-succeeded`/`attempt-failed` events
  (arch.md l.331). Its field names/status-tokens/worker `"<pool>#<thread>"`
  format satisfy **both** the schema and the T42 fold's reader
  (`node`/`attempt`/`status`/`worker`/`message`/`error`/`metrics`/`cost_declared`/
  `cost_measured`/`durable_reference`/`satisfied_from_run`/`originating_node`).
- `zombie-at-exit` now carries `{node, attempt}` (schema-required; the fold keys
  pinned-time accounting off `(node, attempt)`).

**Live caller.** The T24 run-loop driver (`crates/cli/src/driver.rs`) now emits
one `attempt-outcome` at each attempt's completion (one per attempt for a retried
node), from the terminal state + attempt number it already has. **Execution
behavior is unchanged** — only what is recorded to the stream changed.

**Guarantees added.** A writer→schema round-trip test drives a real writer
producing every record kind and validates each emitted line against the published
schema (it fails if the writer diverges again). The T39 event-stream corpus
fixtures (`tests/fixtures/corpus/event-stream/v1/*.json`) are now **generated from
real writer output**, so they double as a writer-conformance golden while staying
schema-valid (the `fixture_corpus_round_trip` walker stays green).

**Rationale / no new deps.** `dagr-core` stays dependency-free; the writer stays
on `serde_json` + `uuid` (the RFC3339 conversion is a dependency-free
`SystemTime`→civil-date computation — no `chrono`/`time`). No `deny.toml`/`audit`
change.

**Operator decision.** A prerequisite production fix on a dedicated branch
(`fix/reconcile-event-stream-writer-schema`), not a numbered ticket; recorded here
because it corrects a shipped T19 defect against the ratified T39 contract.
