# dagr criteria matrix

> **Status:** authoritative classification, produced by ticket T0.10
> ([`docs/implementation/005-T0.10-stability-policy-and-criteria-partition.md`](implementation/005-T0.10-stability-policy-and-criteria-partition.md)),
> the ADR that decides this partition. This file is a **checked-in,
> review-owned document** — never a runtime registry, a metadata store, or a
> scheduler input (that boundary is permanent; see
> [`docs/arch.md`](arch.md) "When not to use this" and the ticket's Out of
> scope). It is edited only by pull request and reviewed like code.

This is the single source of truth that classifies **every** acceptance
criterion in [`docs/arch.md`](arch.md) — all of C1–C28 and the eight
system-level criteria — as exactly one of three classes. It is the artifact
[`docs/arch.md`](arch.md) system-level **criterion 8** names: *"each criterion
in this document appears there exactly once, as machine, human, or disclaimer,
and a criterion absent from the matrix fails CI."*

## The three classes

| Class | Meaning | How it is honoured |
|---|---|---|
| **machine** | Verifiable by an automated test whose pass/fail is unambiguous. | T7 wires the CI coverage matrix that maps every machine-classed criterion to a passing test and **fails CI on any unmapped machine criterion**. |
| **human** | A judgment — readability, documentation quality, a time-to-complete goal — that no test can settle honestly. | On the version-controlled release checklist, reviewed like code (arch.md system-level criterion 8). |
| **disclaimer** | Something the tool deliberately claims **nothing** about. | Carried here, present and unclassified-as-testable, so it is never silently absent. Exactly one criterion (SL4c) is a disclaimer. |

## Granularity

The matrix lists one row per **criterion id**. For the two system-level criteria
that arch.md itself splits, the classified ids are the sub-parts, because each
carries a different class:

- **Criterion 4** (determinism) → **SL4a** (structural, machine), **SL4b**
  (interpretive, machine), **SL4c** (external systems, disclaimer). There is no
  bare classified `SL4`; its sub-parts are the criterion ids.
- **Criterion 8** (coverage) → **SL8machine** (coverage-of-machine-criteria,
  machine) and **SL8human** (human-classed criteria on the release checklist,
  human). There is no bare classified `SL8`; its sub-parts are the criterion ids.

**Split criteria that keep one id.** C1 and C21 each hold a bundle of mechanical
acceptance criteria *plus* one judgment-shaped criterion that arch.md system-level
criterion 8 explicitly names as human — C1's "types readable from the
declaration" and C21's "internal-logic limitation documented at the point of
use." Per criterion 8 those two criteria are human, so their rows are classed
**human**; the Notes column records that their remaining, mechanical sub-criteria
are still covered by automated tests. The class in this matrix is the criterion's
*governing* class per criterion 8, not a claim that nothing about C1 or C21 is
testable.

## Platform-conditional criteria

arch.md **Platform support** requires three acceptance areas be *named as
platform-conditional* so the T70 platform-matrix CI job can gate them per
platform rather than treating them as unconditionally machine on every platform:
**limit detection (C12)**, **signal handling (C16)**, and **flush/fsync
behavior (C19)**. These rows carry `platform-conditional` in the Platform
column. They remain `machine` — they are tested — but their pass/fail depends on
the platform (tier-1 Linux fully; dev-supported macOS with documented
divergences; Windows unsupported in v1).

## Layer A–D criteria (C1–C28)

| Criterion | Class | Platform | Governing | Notes / where verified |
|---|---|---|---|---|
| C1 | human | — | Task | Class is human because criterion 8 names "types readable from the declaration" as a judgment. **Note:** C1's mechanical sub-criteria (consumes-nothing produces a value; error distinguishes retry/permanent/skip; strictly-sequential attempts; owned-vs-shared receive-mode assembly errors; documented `Send`/`Sync` bounds; feature-removal needs no task change) are covered by automated tests in T9/T29/T14. |
| C2 | machine | — | Handle | Handle copy/pass; no-lookup-by-name/index API; cycle is a checked-in compile-failure test (T8/T12); rename changes identity + fingerprint; reorder changes nothing (T10/T13). |
| C3 | machine | — | Data dependency | Wrong-type/arity compile errors via UI tests on the pinned toolchain (T8/T12); `all-succeeded`-only typestate; one-to-many binding; arity cliff message (T11). |
| C4 | machine | — | Ordering dependency | Registration-time backward-reference edge; both edge kinds recorded distinctly; effect-only node has no value; propagation across ordering edges (T50). |
| C5 | machine | — | Node policy | Every field defaulted; full effective policy in graph artifact; no code change to alter policy; default-equivalence under the policy hash; invalid execution-class override fails assembly (T29). |
| C6 | machine | — | Group | Unique names across grouping; diagram clusters by group (structural + golden proxy, T46); group rename changes no execution behaviour and no fingerprint (T51). |
| C7 | machine | — | Flow assembly | Byte-identical graph artifacts; duplicate-name fail names both; empty-environment assembly; all-problems-not-first; exact consumer counts; no parameter reachable; bootstrap rejects unsatisfiable cost/missing resource/invalid param (T13/T14/T15). |
| C8 | machine | — | Run context | Every field populated on the first attempt; attempt number increments in logs and artifacts; no context API mutates the graph or scheduling; hand-built context in a unit test; data interval recorded verbatim (T16). |
| C9 | machine | — | Resource registry | Missing-resource bootstrap failure naming resource and nodes; registry immutable in-run; fake replacement; newtype disambiguation; secret-sentinel test (T30). |
| C10 | machine | — | Output slot | Never-read-before-fill; retry finds value intact; allocator-level release after terminal + closure return; peak memory flat over a hundred-node chain; retained values identified and redeemable; measured residency in artifact (T17/T26). |
| C11 | machine | — | Readiness tracker | Early-ready node starts first; diamond fast branch not delayed; exactly-one-terminal-state and run-ends-when-idle; every rule fires/can-never-fire covered incl. `satisfied-from-prior`; no-deadlock property test over random graphs (T18/T25). |
| C12 | machine | platform-conditional | Admission controller | **Limit detection is platform-conditional** (cgroup v2→v1→host; Linux-only cgroups, macOS falls back to host, Windows unsupported). Machine-tested: capacity invariant incl. abandoned-but-running; over-capacity fails at bootstrap; atomic multi-pool acquisition/no-deadlock; starvation; pinning-flag override; wait-vs-exec timing; declared-vs-measured cost (T31/T32/T37, gated by T70). |
| C13 | machine | — | Execution class dispatch | Long sync task doesn't delay await-bound work; concurrent compute ≤ pool size; misdeclaring never corrupts data/artifacts/rails — the all-workers-blocked test where a timeout still fires and SIGTERM still yields a complete stream (T33). |
| C14 | machine | — | Attempt runner | Retry up to count and no further; permanent not retried; per-class timeout semantics (await-bound cancelled + permit freed immediately; blocking marked-but-held, retry deferred past return); abandoned late result never fills slot/scratch; panic isolates its node; `panic=abort` refused; one attempt-outcome record each; jittered backoff (T20/T21/T22/T23/T37). |
| C15 | machine | — | Failure policy and propagation | Stop-on-first-failure admits no default work but runs firing contingency nodes; continue-independent completes unrelated branches; no node runs with an unsucceeded data dep; `all-terminal` cleanup still runs vs `upstream-failed` without executing; exactly-one-terminal-state; skip-only run is success (T34). |
| C16 | machine | platform-conditional | Cancellation and shutdown | **Signal handling is platform-conditional** (SIGTERM/SIGINT semantics; Linux fully, macOS documented divergences, Windows unsupported in v1). Machine-tested: complete stream on termination within budget; `cancelled` vs `abandoned` distinct from `failed`; operator-configurable grace/teardown + startup budget print; cooperative temp cleanup + per-run temp dir removal; unwritable-sink bounded wait + distinct code (T35/T36, gated by T70). |
| C17 | machine | — | Setup and teardown nodes | Teardown runs after any terminal state; failing teardown recorded but doesn't change outcome; several teardowns all run when one fails; no data deps + covered terminal states exposed; runs under a fresh signal even when cancelled; resume re-executes teardown-covered nodes (T52). |
| C18 | machine | — | Durable scratch store | Value written attempt-one readable attempt-two; keys namespaced by run+node; cross-node read enforced-not-conventional; durable-store resume carry-forward; scratch of a succeeded node gone; I/O failure is retry-eligible (T53/T54a/T54b). |
| C19 | machine | platform-conditional | Event stream | **Flush/fsync behavior is platform-conditional** (different fsync semantics on macOS; the "at most one trailing partial record" tolerance and end/cancel fsync are the platform-sensitive facet). Machine-tested: abrupt-kill leaves ≤1 trailing partial; every record carries run id + schema version; gapless strictly-increasing sequence; two simultaneous runs write disjoint valid streams; stream written through the operator-pointable sink; foldable with no access to the run; mid-run sink failure cancels with the sink-failure code (T19/T27/T67, flush facet gated by T70). |
| C20 | machine | — | Graph artifact | Produced in an empty environment; byte-identical outside generation-time; every node/edge incl. declared resources; author-declared stable names + duplicate-name assembly failure; validates against the published schema (T40, schema from T39). |
| C21 | human | — | Graph fingerprint | Class is human because criterion 8 names "documentation-at-point-of-use (C21)" — the internal-logic-limitation documented at the point of use — as a judgment. **Note:** C21's mechanical sub-criteria (same fingerprint across toolchains; structural change ⇒ structural-fingerprint change; policy-only change ⇒ policy-hash only; group rename changes neither; hashes + algorithm version in every artifact) are covered by automated tests in T41. |
| C22 | machine | — | Run artifact | Every graph node appears with propagated states; crashed-run fold marked interrupted (produced by a later invocation); assembly-failure variant; phase durations sum exactly; structural fingerprint matches a same-build graph artifact; validates against schema and every prior-schema-version fixture stays parseable (T42/T48); no environment value outside the declared allowlist (T42/T43/T68). |
| C23 | machine | — | Node metrics | New measurement with no framework change; framework metrics present without a task; measurements reach the artifact unmodified; reserved-prefix attach fails + deterministic cap truncation recorded; documented naming/units convention exists and built-ins follow it (T44). |
| C24 | human | — | Renderers | Class is human because criterion 8 names "diagram readability (C24)" as a judgment (readable output with no manual layout is the design goal). **Note:** C24's mechanical proxies (DOT `dot`-parses and Mermaid parser accepts in CI; every node/edge present with distinct data/ordering styling and group clusters via structural + golden tests; documented distinct per-state styles with originated-vs-propagated skips distinguishable; rendering needs no access to the binary) are covered by automated tests in T46/T47. |
| C25 | machine | — | Logging integration | Any attempt log line traceable to node+attempt without timestamp correlation; structured/human switch needs no code change; concurrent-node lines separable; secret-sentinel never on framework output paths (T45). |
| C26 | machine | — | Command-line contract | Verbs behave identically across pipelines; validate exits non-zero + prints all problems; exit-code table exhaustive over verbs/causes with a test each; invalid params rejected at bootstrap; parameter cannot shadow a library flag; no-args prints verbs cleanly; fold produces the interrupted artifact (T55/T56/T68). |
| C27 | machine | — | Resume | Structural mismatch refuses + prints diff, policy-only proceeds; satisfied node not re-executed and rehydrated on demand; in-memory output re-executed iff demanded; undemanded prior success is satisfied-from-prior (cleanup-after-publish shape); dangling durable reference fails the plan; no-op resume of a full success; param conflict refuses + force records; lineage links parent and root (T57/T58/T59). |
| C28 | machine | — | Testing surface | Sync single-task test needs no runtime, await-bound needs only the provided test runtime; structure test fails on add/remove/rename/rewire/regroup/policy-change and not on rebuild/toolchain-bump, output is a structural diff; single documented fixture-update flow, canonical stable ordering; full-pipeline-against-fakes completes in seconds; no pipeline writes its own harness; fault-injection suite covers kill-points/disk-full/failing sinks (T60/T61/T62; also underpins T8's compile-fail harness). |

## System-level criteria (criteria 1–8)

| Criterion | Class | Platform | Governing | Notes / where verified |
|---|---|---|---|---|
| SL1 | machine | — | System-level 1 | **machine** — README quickstart compiles and runs verbatim in CI, empty directory to a run, artifact-inspected two-node pipeline (T7/T64). **human** — the "under thirty minutes" walkthrough is a design goal audited on the release checklist each release, not a timer in CI. This row is one criterion carrying both parts, as arch.md labels it. |
| SL2 | machine | — | System-level 2 | Mis-wiring two tasks is a compile error whose message contains both type names, verified by UI tests on the pinned toolchain (T8/T12). |
| SL3 | machine | — | System-level 3 | Every run produces artifacts — normal, crashed (via the fold verb), cancelled, and assembly/bootstrap-failed (T27/T42/T68). |
| SL4a | machine | — | System-level 4(a) | *Structural* determinism — two builds of the same source produce identical fingerprints and byte-identical graph artifacts (generation time aside), on different toolchains (T15/T41/T65). |
| SL4b | machine | — | System-level 4(b) | *Interpretive* determinism — the same recorded outcomes yield the same terminal states, propagation decisions, and artifact, replayed through the C28 harness with scripted results (T62/T65). |
| SL4c | disclaimer | — | System-level 4(c) | What tasks *do* against external systems is theirs; the tool claims nothing about it. Carried here as a **disclaimer** — present, not machine, not human — so it is never silently absent (arch.md criterion 4(c): "a disclaimer, carried unclassified in the criteria matrix"). |
| SL5 | machine | — | System-level 5 | A run's duration and resource profile are answerable entirely from artifacts, with no access to the producing machine (T43/T65). |
| SL6 | machine | — | System-level 6 | Adding a node requires no changes outside its own module, the assembly site, the structure fixture, and (for a new resource) registry construction in `main` — verified via the structure-diff on a reference pipeline (T61/T65). |
| SL7 | machine | — | System-level 7 | Nothing requires a server, database, or scheduler running; the binary and its arguments suffice to run and produce local artifacts (crash-surviving artifacts and resume additionally need the operator-supplied durable run store) (T65). |
| SL8machine | machine | — | System-level 8 (machine part) | Every machine-classed criterion above and in C1–C28 is covered by an automated test, and that coverage is verified in CI from this checked-in matrix; a machine criterion with no mapped test fails CI, and a criterion absent from this matrix fails CI (T7 builds the enforcement; T65 is the acceptance gate). |
| SL8human | human | — | System-level 8 (human part) | The human-classed criteria — diagram readability (C24), documentation-at-point-of-use (C21), the thirty-minute walkthrough (SL1's human part), and judgment-shaped criteria such as C1's types-readable-from-the-declaration — are on the version-controlled release checklist, reviewed like code. |

## Classification totals

- **machine:** C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12\*, C13, C14, C15, C16\*, C17, C18, C19\*, C20, C22, C23, C25, C26, C27, C28; SL1 (machine part), SL2, SL3, SL4a, SL4b, SL5, SL6, SL7, SL8machine.
- **human:** C1, C21, C24; SL8human (and SL1's thirty-minute-walkthrough facet).
- **disclaimer:** SL4c (exactly one).
- **platform-conditional (\*, still machine):** C12 (limit detection), C16 (signal handling), C19 (flush/fsync behavior).

Every C1–C28 id and every system-level id (SL1, SL2, SL3, SL4a, SL4b, SL4c, SL5,
SL6, SL7, SL8machine, SL8human) appears above **exactly once**. That totality is
checked mechanically by
[`scripts/check-stability-and-criteria.sh`](../scripts/check-stability-and-criteria.sh),
authored before this matrix; the T7 CI coverage matrix consumes these
classifications and the platform-conditional flags.
