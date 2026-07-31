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

---

## 2026-07-25 · 079 (T64) — cookbook entries use the honest real-API shape where the shipped RunnableFlow seam cannot express a scenario literally

**Quoted Test-plan lines (three, all under "write these first — TDD").**
- *"Incremental-cursor entry checkpoints via scratch. … run an attempt that writes
  a cursor to scratch, force a retry-eligible failure, then let the retry read
  it."*
- *"Fan-in cookbook entry wires many upstreams into one node. … the joining node
  consumes multiple upstream handles as a tuple."*
- *"Fan-out cookbook entry … run it and read its declared versus measured cost
  from the run artifact (C23)."*

**Deviation.** Every flow-running cookbook example uses the mandated one-call
`dagr_cli::run_flow::RunnableFlow` seam and **never** hand-writes a `NodeRunner`.
But the shipped `RunnableFlow` (ADR 081, merged 839d841) has three expressive
limits this ticket may **not** modify (the CRITICAL-CONTEXT mandate: "Do not
modify the shipped ergonomics API to make an example prettier"), so three entries
realize their scenario through the honest real API instead of a literal
one-`RunnableFlow`-node reading:

1. **Incremental cursor via scratch.** `RunnableFlow`'s retry path
   (`run_with_retries_caught`) mints a fresh per-attempt `RunContext` **without**
   the driver's `scratch_root`, so a *retrying* node run through `RunnableFlow`
   reaches only an unwired scratch store (T63's wiring covers the single-attempt
   path; the retry path's scratch is genuinely T53/T54b's runtime concern). The
   entry therefore demonstrates the exact C18 contract — "a value written on
   attempt one is readable on attempt two" — against the **real** `ScratchStore`
   via two stores sharing the node's namespace (the same shape C18's own
   acceptance test uses), rather than through a retrying `RunnableFlow` node whose
   scratch is not wired.
2. **Fan-in.** `RunnableFlow::register`/`register_with` drive **single-input**
   nodes only (its `InputWiring` blanket impl asserts one edge). The entry proves
   the multi-upstream **tuple binding** — "consumes multiple upstream handles as a
   tuple," compile-checked, under the default `all-succeeded` rule — on a raw
   `Flow` via `assemble()` (a structural proof, no NodeRunner), and provides the
   **runtime** fan-in through `RunnableFlow` using the documented
   aggregate-into-a-struct escape hatch (an intermediate node produces a struct of
   the joined values; the consumer depends on that one handle).
3. **Fan-out declared cost.** `RunnableFlow` has no source-with-policy seam, so the
   fan-out node carrying a declared-cost `NodePolicy` is a *data-dependent* node
   (fed by a trivial source) registered with `register_with` — the honest way to
   attach a policy through the seam. The declared-cost-vs-measured **run-artifact**
   read (C23) is the M3 artifact surface proven by T49/T43, not re-built here; the
   entry demonstrates the arch.md invariant it is really about — internal
   parallelism bounded by the declared cost, and runtime fan-out adding **no**
   nodes to the graph — observably.

**Rationale.** Every documented behaviour is TRUE of the shipped code (the ticket
mandate: "verify claims against the real API, do not aspirationally describe").
Each entry is backed by a compiled, passing test in
`crates/cli/tests/cookbook.rs`, so it cannot rot. The alternative — hand-rolling a
`NodeRunner` to force a literal single-node reading of each scenario — is exactly
the ~150 lines of scheduler plumbing arch.md C1 forbids in an author-facing doc,
and is the precise thing this ticket exists to eliminate. No shipped API was
changed; the seam is used as-is.

**Operator decision.** Traces to the CRITICAL-CONTEXT mandate on this ticket:
"MANDATE: EVERY quickstart / README / cookbook code example that runs a flow MUST
use `RunnableFlow` … DO NOT hand-write `impl NodeRunner` …" together with "Do not
modify the shipped ergonomics API (run_flow.rs) to make an example prettier … If
the API genuinely cannot express something … write the example against the API AS
IT IS." These three entries are the "API cannot express it literally" case,
resolved by the real-API shape rather than a workaround.

---

## 2026-07-30 · 114 (T99) — the adopt/satisfied tables live in the register, not only in the PR description

**Quoted DoD line.** *"Every `adopt` row is traced to a shipped ticket item,
tabulated in the PR description; a sample of `satisfied` rows is independently
verified with the check recorded."*

**Deviation.** Both tables are authored in
[`docs/rust-skills-register.md`](../rust-skills-register.md) under "The M9
gate's verification record (T99)", not in the pull-request body. The implementer
of a ticket on the autonomous loop does not author the PR description — the
orchestrator does, after the branch is handed over — so the literal location is
not reachable from inside the ticket. The tables can be lifted verbatim into the
PR body, and the orchestrator is told so at hand-over.

**Rationale.** The chosen location is strictly stronger than the one the DoD
names. A PR description is written once and never checked again; these tables
are **data the gate parses**. `crates/cli/tests/m9_acceptance_gate.rs` fails if
the traceability table does not cover exactly the register's `adopt` rows, if a
row's ticket disagrees with its disposition, if an evidence token has gone
missing from the file that is supposed to carry it, or if the spot-check table
and the checks the suite actually runs are different sets. Both properties were
confirmed by deleting a row and by reverting a shipped item and watching the gate
fail. A table in a PR body would satisfy the sentence and guard nothing.

**Operator decision.** Traces to the standing autonomous-loop split recorded in
the 002 entry above: the implementer owns the branch, the orchestrator owns push,
PR, and merge. Nothing about the substance of the DoD line is reduced — the
tracing and the independent verification both happened, and both are now
enforced.

---

## 2026-07-30 · 114 (T99) — the pre-M9 baseline is captured from the pre-M9 commit, not stashed before the branches landed

**Quoted DoD line.** *"Reference artifacts, event stream, and structural
fingerprint are byte-identical to the pre-M9 baseline modulo legitimately varying
fields; the baseline was captured before the M9 branches landed."*

**Deviation.** The baseline was captured on 2026-07-30, after every M9 branch had
merged, by checking out the pre-M9 tree (`5f87d11`, the last commit before any M9
work) in a worktree and running the probe there. Its second clause — "captured
before the M9 branches landed" — describes a sequencing that only the ticket's
own author could have arranged, and T99 is written and executed last by
construction.

**Rationale.** What the clause is protecting is that the baseline comes from a
pre-M9 **engine** rather than being re-derived from the head it is meant to
check. Git holds that tree exactly, so capturing from the commit is not a
weakening — it is *stronger* than a file stashed at the time, because it is
reproducible by anyone from the recorded sha rather than trusted. The probe used
is one checked-in file copied unmodified into the worktree, and its FNV-1a digest
is recorded in the fixture's `PROVENANCE.md` and recomputed by the gate, so the
comparison cannot silently become one program against another. `PROVENANCE.md`'s
regeneration recipe names only the recorded pre-M9 commit, so re-capturing from
the current head — the tautology the clause exists to prevent — is not the
documented path. The comparison found no difference, so nothing was reclassified.

**Operator decision.** Traces to the ticket's own Open questions section, which
makes the rule explicit: *"If the behavioural-identity comparison finds a
difference, the gate fails and the difference is investigated — it is not
reclassified as an accepted change at gate time."* That rule is preserved intact;
only the capture mechanism differs from the sentence's assumed sequencing.

---

## 2026-07-30 · 115 (T100) — the ADR's pinning check lives in `scripts/`, outside the DoD's path list

**Quoted DoD line.** *"The diff touches only `docs/**`, `README.md`,
`CONTRIBUTING.md`, and `schemas/event-stream/v1.schema.json` — **no `crates/**`,
no `Cargo.lock**`."*

**Deviation.** This branch also adds `scripts/check-remote-execution-scope-adr.sh`
and one `bash scripts/…` run line in `.github/workflows/ci.yml`. Neither path is in
the list the DoD line enumerates.

**Rationale.** The line's *subject* is production code, and that half is honoured
exactly: no `crates/**` file and no `Cargo.lock` entry changes, and the decision
ships no remote-execution code (T101+ owns all of it). What the enumeration cannot
be read literally to forbid is the ticket's own Test plan, which opens *"the 'tests'
are mechanical file/content assertions"* and then lists nine of them — ADR
completeness, exclusions intact, supersession recorded, the additive `@1.3` schema
shape, criterion 7 still `[machine]`-classed, the matrices' row counts, the
de-staled non-goals sentence. The repo keeps assertions of exactly that shape in
`scripts/check-*-adr.sh`; twelve already exist and CI runs them in one step. The
same line's siblings also fall outside its own list (it requires edits to
`.claude/skills/…/SKILL.md`, `references/ticket-conventions.md`,
`.github/pull_request_template.md`, `crates/core/README.md`, and
`crates/cli/examples/quickstart.rs`), so the enumeration is a no-production-code
statement rather than an exhaustive path allowlist. The `ci.yml` line is not
optional: `crates/cli/tests/ci_and_test_hygiene.rs` fails any `scripts/check-*.sh`
the workflow does not invoke, precisely so a checker cannot rot into a comment.

**Operator decision.** Traces to the standing instruction for this ticket — *"this
ticket ships a decision and its pinning checks only"* — and to the ADR's own
premise that a moved *permanent* boundary must be held by something that fails when
it widens, which is also what T112's boundary proof will do for shipped code
(`docs/implementation/127-T112-m10-acceptance-gate.md`: *"the only thing keeping
that carve-out narrow is a test that fails when it widens"*). Scope is respected in
the other direction too: T112 keeps every *structural* invariant over code (no
listener, no metastore link in a pod, zero-dep core, no HTTP/TLS stack in a default
build); this checker asserts only the decision's text.

---

## 2026-07-30 · 115 (T100) — the ADR body, the arch.md amendment, the supersessions and the `@1.3` schema landed in PR #116, ahead of this branch

**Quoted DoD lines (the artifact-producing ones, abbreviated).** *"This file
contains an ADR with **Status / Context / Decision / Consequences / Rejected
alternatives** sections …"*; *"`arch.md`'s permanent-non-goals sentence is amended
to permit a single orchestrator placing attempts on remote compute for one run …"*;
*"`arch.md`'s 'Amendment changelog' carries an entry for this decision."*; *"ADR 012
is marked 'Superseded (in part) by ADR 115' … ADR 014 likewise …"*;
*"`schemas/event-stream/v1.schema.json` adds the `attempt-submitted` kind and its
conditional payload as `@1.3` …"*; *"`README.md` and `CONTRIBUTING.md` carry the
amended non-goals sentence."*; *"System-level criterion 7 is amended in place …"*;
*"The three process gates … name both carve-outs …"* — together with the framing
line *"The diff touches only `docs/**`, `README.md`, `CONTRIBUTING.md`, and
`schemas/event-stream/v1.schema.json`"*, which assumes those edits land **here**.

**Deviation.** Every one of those artifacts already exists on `main`. They were
authored and merged **ahead of this branch**, in **PR #116** (ticket 111 · T96,
commit `126cdcb`), alongside the M10/M11 ticket set they belong to: the embedded ADR
in this ticket file, the `arch.md` permanent-non-goals amendment and its dated
Amendment-changelog entry, the criterion-7 in-place amendment, the two partial
supersessions on ADRs 012 and 014, the `README.md` / `CONTRIBUTING.md` de-staling,
the SL7 matrix rewording, the three process gates, and the additive `@1.3`
event-stream schema revision. This branch therefore adds **none** of them. What it
adds is the mechanical pinning check the Test plan describes —
`scripts/check-remote-execution-scope-adr.sh`, wired into CI's "ADR content
contracts" job — which asserts that every one of those artifacts still says what the
ADR decided, and, load-bearingly, that the carve-out has not widened.

**Rationale.** Nothing was re-decided, re-authored, or rewritten here, which is the
outcome ticket-conventions §10 wants: merged decision text is never rewritten, so
re-emitting the ADR body on this branch would have been the *worse* option. The
substance of each DoD line is satisfied on `main` and is now, for the first time,
**enforced** rather than merely present — the checker fails if the permanent-non-goals
sentence loses an exclusion, if either supersession note is dropped or an older
ADR-097 note is overwritten, if criterion 7 loses its `[machine]` class or its
unconditional "no dagr server" half, if a pre-`@1.3` event kind leaves the enum, or if
a Rejected-alternatives bullet is flipped from "Still rejected, unchanged" to
permitted. Recorded here because a reader diffing this branch against the DoD would
otherwise conclude the chartered work was skipped.

**Operator decision.** Traces to this ticket's own §Open questions, third bullet
("Where the amendment actually landed — RESOLVED, recorded (per §5)"), which points
at this file, and to the operator's dated acceptance of the boundary amendment
itself on 2026-07-29 recorded in the ADR's §Status. The sequencing — M10/M11 ticket
authorship landing before the decision ticket that formalises it — was the
orchestrator's, under the same standing autonomous-loop split recorded in the 002
entry above.

---

## 2026-07-31 · 117 (T102) — the await-bound timeout is armed through the *caught* sibling of `run_attempt_with_timeout`

**Affected DoD line.** *"`NodePolicy::timeout` is armed per attempt on the
`RunnableFlow` path: await-bound via `run_attempt_with_timeout`, blocking/compute via
the existing `TimeoutDecision` / `LateResultBarrier` path."*

**Deviation.** The await-bound half is armed through
`dagr_core::execution::run_attempt_caught_with_timeout` (single attempt) and
`run_with_retries_caught_timed` (with retries), not through
`run_attempt_with_timeout` itself. Both are new in this ticket and both are
*compositions of that function's mechanism*, not a second one: the same `race`
combinator, the same drop-the-losing-future cancellation, the same
`AttemptOutcome::TimedOut` classification, the same emitted records, and the same
permit-into-the-work-future discipline (the run-flow path passes `()`, because the
driver holds the admission permit around the whole attempt). The blocking/compute half
uses the merged `TimeoutDecision` / `LateResultBarrier` path literally, as written.

**Rationale.** `run_attempt_with_timeout` installs **no** `catch_unwind` boundary —
it is the timeout facet alone, and its own rustdoc says so ("Timeout and panic are
independent facets"). Every attempt on the `RunnableFlow` path runs behind panic
containment today (`run_attempt_caught` / `run_with_retries_caught`), because the
driver dispatches attempts onto task surfaces where an escaping panic would unwind
past the dispatch instead of failing one node — the run would then never receive that
node's completion. Satisfying the DoD line *literally* would therefore have silently
removed panic containment from exactly the nodes that declare a timeout, trading one
enforced policy for another. Composing the two facets in core keeps both, keeps the
mechanism single (the extracted `caught_body` is now the one caught-dispatch site both
attempt paths share), and leaves `run_attempt_with_timeout` untouched for its existing
callers. With `timeout: None` the new loop is `run_with_retries_caught` exactly, so a
node that declares no timeout is byte-identical.

**Operator decision.** Traces to arch.md C14 (panic containment and the per-attempt
timeout are both unconditional acceptance criteria of the attempt runner, and *"a
panicking task fails only its own node"* is not suspendable for timeout-carrying
nodes) and to this ticket's recorded resolution "the class decides *who* arms the
deadline", under the standing autonomous-loop split recorded in the 002 entry above.
