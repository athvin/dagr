# dagr acceptance-criteria coverage matrix

> **Status:** enforcement artifact, authored by ticket T7 (ticket 006,
> [`docs/implementation/006-T7-ci-pipeline-and-criteria-matrix.md`](implementation/006-T7-ci-pipeline-and-criteria-matrix.md)).
> A **checked-in, review-owned data file** — never a runtime registry, a
> metadata store, or a scheduler input (that boundary is permanent; see
> [`docs/arch.md`](arch.md) "When not to use this"). Edited only by pull
> request, reviewed like code.

This file is the **coverage** half of arch.md system-level acceptance
**criterion 8**: *"every machine-classed acceptance criterion … is covered by an
automated test, and that coverage is itself verified in continuous integration
from a checked-in criteria matrix."* It binds each **machine**-classed criterion
to the **test id** that covers it, or to an explicit **`unmapped`** placeholder
while its covering test has not yet shipped. It is verified in CI by
[`scripts/check-coverage-matrix.sh`](../scripts/check-coverage-matrix.sh).

## Two matrices, one boundary

There are two distinct artifacts; do not conflate them:

- [`docs/criteria-matrix.md`](criteria-matrix.md) — the **PARTITION** (ticket
  T0.10): each arch.md criterion labelled `machine` / `human` / `disclaimer`.
  It is the authoritative classification. **This file does not restate or
  re-decide it**; the verifier reads each criterion's class *from the partition*
  and fails if this file disagrees.
- **This file** — the **COVERAGE matrix**: for every criterion the partition
  lists, a row binding a machine criterion to its test id (or `unmapped`), the
  owning ticket, and the platform-conditional flag T70 keys on.

## How the verifier reads this file

[`scripts/check-coverage-matrix.sh`](../scripts/check-coverage-matrix.sh) parses
the table below and **fails CI** when any of these holds (each is exercised by
the verifier's own self-tests,
[`scripts/check-coverage-verifier-selftest.sh`](../scripts/check-coverage-verifier-selftest.sh)):

- a criterion in the partition is **absent** from this matrix;
- a criterion appears on **more than one** row;
- a **machine** criterion is **`unmapped` and owed by this ticket** (`Covered-by`
  is `T7`, empty, or `—`) — its covering test was supposed to exist already;
- a **machine** criterion maps to a **test id that does not exist** in the
  `cargo test --workspace` suite (a dangling reference).

**`unmapped` is the normal early state.** A machine criterion whose covering
test ships in a *later* ticket is legitimately `unmapped` and names that future
ticket in `Covered-by` (quality-gates.md §3). Every component ticket, as it
lands its covering test, edits this matrix to replace `unmapped` with the test
id — that per-ticket duty is what keeps the debt from exploding at the T65
acceptance gate. Human and disclaimer rows never carry a test.

## Column meanings

| Column | Meaning |
|---|---|
| **Criterion** | The criterion id, exactly as classified in the partition (`C1`–`C28`; `SL1`–`SL7` with criterion 4 split into `SL4a`/`SL4b`/`SL4c`; criterion 8 split into `SL8machine`/`SL8human`). |
| **Class** | `machine` \| `human` \| `disclaimer`, mirroring the partition (the verifier fails on any disagreement). |
| **Platform** | `platform-conditional` for the limit-detection (C12), signal-handling (C16), and flush/fsync (C19) criteria, so T70 can attach the Linux-tier-1 and macOS-core jobs later; `—` otherwise. |
| **Test** | For a machine criterion: the mapped test id (as `cargo test -- --list` prints it) **or** `unmapped`. For human: `release-checklist`. For disclaimer: `—`. |
| **Covered-by** | The ticket that ships (or shipped) the covering test. `T7` means "owed by this ticket"; any later tid means the mapping is deferred to that ticket; `—` for human/disclaimer rows. |
| **Notes** | Free text: what the mapped/owed test asserts. |

## Layer A–D criteria (C1–C28)

| Criterion | Class | Platform | Test | Covered-by | Notes |
|---|---|---|---|---|---|
| C1 | human | — | release-checklist | — | "types readable from the declaration" is a judgment (criterion 8). C1's mechanical sub-criteria are machine-tested under T9/T29/T14 but the criterion's governing class is human. |
| C2 | machine | — | unmapped | T12 | Handle copy/pass, no-lookup API, compile-fail cycle test, rename-changes-identity, reorder-changes-nothing (T10/T12/T13). T8 shipped the compile-fail UI harness (`crates/core/tests/ui.rs`, test `ui`) and a wrong-type seed sample; the cycle-inexpressibility compile-failure case that covers this criterion is authored by T12 against the real authoring API, so this stays `unmapped`/deferred to T12. |
| C3 | machine | — | unmapped | T12 | Wrong-type/arity compile errors via UI tests on the pinned toolchain; `all-succeeded` typestate; arity-cliff message (T11/T12). T8 shipped the UI harness and proved the both-type-names substring assertion against a seed sample. **T11 shipped the real binding API (`dagr_core::binding`) and wired its own real-API compile-fail fixtures into the `ui` test** — `data_binding_wrong_type`, `data_binding_wrong_arity_too_few`/`_too_many`, `data_binding_arity_ceiling` (curated on_unimplemented), and `data_binding_non_default_rule` (typestate) — plus positive/runtime-shape tests (`data_binding_positive`). Kept `unmapped`/deferred to **T12** per the matrix contract: T12 is the consolidated compile-fail suite that *owns and maps* C3 (the wrong-type/arity/cycle/typestate cases against the real authoring API, plus C2's cycle-inexpressibility), so re-pointing this row now would steal T12's mapping duty. No re-point; debt recorded here. |
| C4 | machine | — | unmapped | T50 | Registration-time backward-reference edge; both edge kinds recorded; effect-only node has no value; propagation across ordering edges (T50). |
| C5 | machine | — | unmapped | T29 | Every field defaulted; full effective policy in artifact; default-equivalence under the policy hash; invalid execution-class override fails assembly (T29). |
| C6 | machine | — | unmapped | T46 | Unique names across grouping; diagram clusters by group; group rename changes no execution behaviour and no fingerprint (T46/T51). |
| C7 | machine | — | unmapped | T13 | Byte-identical graph artifacts; duplicate-name failure; empty-environment assembly; bootstrap rejects unsatisfiable cost/missing resource/invalid param (T13/T14/T15). |
| C8 | machine | — | all_fields_populated_on_a_hand_built_context | T16 | **Mapped by T16** (`crates/core/tests/run_context.rs`): a hand-built `RunContext` populates *every* field on the first attempt and each accessor returns exactly the supplied value — the headline C8 acceptance ("every field populated on every invocation, including the first attempt of the first node"). Sibling tests in the same file cover the rest of C8's owned surface: attempt/max readable (`attempt_number_reflects_the_supplied_attempt`), data interval recorded verbatim (`data_interval_is_carried_verbatim_and_never_interpreted`), and the no-mutation/no-scheduling API shape (`context_exposes_no_mutation_or_scheduling_authority`). The remaining acceptance facet — the attempt number *incrementing across retries* and *appearing in logs and artifacts* — is out of T16's scope (T16 only exposes the field); it is demonstrated end-to-end by the retry runner (T22) and node metrics/artifacts (C22/C23), which map their own rows. |
| C9 | machine | — | unmapped | T30 | Missing-resource bootstrap failure; registry immutable in-run; fake replacement; newtype disambiguation; secret sentinel (T30). |
| C10 | machine | — | unmapped | T17 | Never-read-before-fill; retry finds value intact; release after terminal + closure return; peak-memory-flat over a hundred-node chain; measured residency in artifact (T17/T26). |
| C11 | machine | — | unmapped | T18 | Early-ready node starts first; diamond fast branch not delayed; exactly-one-terminal-state; every rule fires/can-never-fire; no-deadlock property test (T18/T25). |
| C12 | machine | platform-conditional | unmapped | T31 | **Limit detection is platform-conditional** (cgroup v2→v1→host). Capacity invariant incl. abandoned-but-running; over-capacity fails at bootstrap; atomic multi-pool acquisition; starvation; pinning override (T31/T32/T37, gated by T70). |
| C13 | machine | — | unmapped | T33 | Long sync task doesn't delay await-bound work; concurrent compute ≤ pool size; all-workers-blocked timeout still fires and SIGTERM still yields a complete stream (T33). |
| C14 | machine | — | unmapped | T20 | Retry up to count and no further; permanent not retried; per-class timeout semantics; abandoned late result never fills slot; panic isolates its node; `panic=abort` refused; jittered backoff (T20/T21/T22/T23/T37). |
| C15 | machine | — | unmapped | T34 | Stop-on-first-failure runs firing contingency nodes; continue-independent completes unrelated branches; no node runs with an unsucceeded data dep; exactly-one-terminal-state; skip-only run is success (T34). |
| C16 | machine | platform-conditional | unmapped | T35 | **Signal handling is platform-conditional** (SIGTERM/SIGINT; Linux fully, macOS documented divergences). Complete stream on termination within budget; `cancelled`/`abandoned` distinct from `failed`; grace/teardown budget; temp cleanup (T35/T36, gated by T70). |
| C17 | machine | — | unmapped | T52 | Teardown runs after any terminal state; failing teardown recorded but doesn't change outcome; runs under a fresh signal even when cancelled; resume re-executes teardown-covered nodes (T52). |
| C18 | machine | — | unmapped | T53 | Value written attempt-one readable attempt-two; keys namespaced by run+node; cross-node read enforced; durable-store resume carry-forward; I/O failure retry-eligible (T53/T54a/T54b). |
| C19 | machine | platform-conditional | unmapped | T19 | **Flush/fsync behavior is platform-conditional** (different fsync semantics on macOS; the ≤1-trailing-partial tolerance is the platform-sensitive facet). Every record carries run id + schema version; gapless strictly-increasing sequence; disjoint simultaneous streams (T19/T27/T67, flush facet gated by T70). |
| C20 | machine | — | unmapped | T40 | Produced in an empty environment; byte-identical outside generation time; every node/edge incl. declared resources; validates against the published schema (T40, schema from T39). |
| C21 | human | — | release-checklist | — | "documentation-at-point-of-use (C21)" is a judgment (criterion 8). C21's mechanical sub-criteria are machine-tested under T41; the criterion's governing class is human. |
| C22 | machine | — | unmapped | T42 | Every graph node with propagated states; crashed-run fold marked interrupted; phase durations sum exactly; structural fingerprint matches a same-build graph artifact; validates against schema + prior-version fixtures (T42/T48/T43/T68). |
| C23 | machine | — | unmapped | T44 | New measurement with no framework change; framework metrics present without a task; reserved-prefix attach fails + deterministic cap truncation; documented naming/units convention (T44). |
| C24 | human | — | release-checklist | — | "diagram readability (C24)" is a judgment (criterion 8). C24's mechanical proxies (DOT/Mermaid parse in CI; golden tests) are machine-tested under T46/T47; the criterion's governing class is human. |
| C25 | machine | — | unmapped | T45 | Any attempt log line traceable to node+attempt without timestamp correlation; structured/human switch needs no code change; concurrent-node lines separable; secret sentinel never on framework output paths (T45). |
| C26 | machine | — | unmapped | T55 | Verbs behave identically across pipelines; validate exits non-zero + prints all problems; exit-code table exhaustive; no-args prints verbs cleanly; fold produces the interrupted artifact (T55/T56/T68). |
| C27 | machine | — | unmapped | T57 | Structural mismatch refuses + prints diff; satisfied node not re-executed; undemanded prior success is satisfied-from-prior; dangling durable reference fails; no-op resume of a full success; lineage links parent and root (T57/T58/T59). |
| C28 | machine | — | unmapped | T60 | Sync single-task test needs no runtime; structure test fails on add/remove/rename/rewire/regroup/policy-change and not on rebuild/toolchain-bump; single documented fixture-update flow; fault-injection suite (T60/T61/T62). |

## System-level criteria (criteria 1–8)

| Criterion | Class | Platform | Test | Covered-by | Notes |
|---|---|---|---|---|---|
| SL1 | machine | — | unmapped | T64 | **machine** — README quickstart compiles and runs verbatim in CI, empty directory to an artifact-inspected two-node pipeline (T7/T64). The "under thirty minutes" walkthrough is the **human** facet, audited on the release checklist (recorded under SL8human), not a CI timer. |
| SL2 | machine | — | unmapped | T12 | Mis-wiring two tasks is a compile error whose message contains both type names; UI tests on the pinned toolchain. T8 shipped the pinned-toolchain UI harness (`crates/core/tests/ui.rs`) and the both-type-names assertion; the mis-wiring case against the real authoring API lands in T12, so this stays `unmapped`/deferred to T12. |
| SL3 | machine | — | unmapped | T27 | Every run produces artifacts — normal, crashed (fold verb), cancelled, assembly/bootstrap-failed (T27/T42/T68). |
| SL4a | machine | — | unmapped | T41 | *Structural* determinism — two builds of the same source produce identical fingerprints and byte-identical graph artifacts (generation time aside), on different toolchains (T15/T41/T65). |
| SL4b | machine | — | unmapped | T62 | *Interpretive* determinism — the same recorded outcomes yield the same terminal states, propagation decisions, and artifact, replayed through the C28 harness (T62/T65). |
| SL4c | disclaimer | — | — | — | What tasks *do* against external systems is theirs; the tool claims nothing about it. Carried here **present and unclassified-as-testable** so it is never silently absent (arch.md criterion 4(c)). |
| SL5 | machine | — | unmapped | T43 | A run's duration and resource profile are answerable entirely from artifacts, with no access to the producing machine (T43/T65). |
| SL6 | machine | — | unmapped | T61 | Adding a node requires no changes outside its own module, the assembly site, the structure fixture, and (new resource) registry construction in `main` — structure-diff on a reference pipeline (T61/T65). |
| SL7 | machine | — | unmapped | T65 | Nothing requires a server, database, or scheduler; the binary and its arguments suffice to run and produce local artifacts (T65). |
| SL8machine | machine | — | verifier_passes_against_the_checked_in_matrix | T7 | **Mapped now.** This coverage is verified in CI from this checked-in matrix by [`scripts/check-coverage-matrix.sh`](../scripts/check-coverage-matrix.sh); the mapped test ([`crates/cli/tests/coverage_matrix.rs`](../crates/cli/tests/coverage_matrix.rs)) runs that verifier against the real matrix and suite. T7 builds the enforcement; T65 is the full acceptance gate. |
| SL8human | human | — | release-checklist | — | The human-classed criteria — diagram readability (C24), documentation-at-point-of-use (C21), the thirty-minute walkthrough (SL1's human part), and C1's types-readable-from-the-declaration — are on the version-controlled release checklist, reviewed like code. |

## Build-provenance row (Stability, C20)

Build-provenance verification — embedding tool version, git commit, and lockfile
hash in every binary and artifact (arch.md "Stability": Supply chain) — is not
yet emitted by any built component. It is **not** a separate arch.md acceptance
criterion; it is a facet of **C20** (graph artifact) and the Stability
supply-chain commitment, so it does not get its own criterion row (the verifier
requires exactly the partition's id set). It is tracked here as an explicit
`unmapped` obligation folded into **C20**'s row above (Covered-by T40): when the
graph-artifact component emits provenance, T40 replaces C20's `unmapped` with the
provenance-assertion test id. Recorded here so the obligation is visible now
rather than discovered at the T65 acceptance gate.

## Platform-conditional criteria (for T70)

The rows flagged `platform-conditional` — **C12** (limit detection), **C16**
(signal handling), **C19** (flush/fsync behavior) — remain `machine` (they are
tested), but their pass/fail depends on the platform. T70 attaches the
Linux-tier-1 full-suite and macOS-core-suite jobs and gates these rows per
platform; this matrix names them so that ticket has an unambiguous target.
