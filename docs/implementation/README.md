# dagr — implementation tickets

Ordered, self-contained work tickets derived from [../tasks.md](../tasks.md) and governed by [../arch.md](../arch.md). Implement top to bottom: each ticket ships as its own branch and PR and must leave CI green before the next begins. These are work instructions — tests are described first in plain English (TDD), and there is no code.

## How to work these tickets

1. Pick the lowest-numbered unchecked ticket whose dependencies are all merged.
2. Cut its branch (the name is in the ticket header).
3. Write the plain-English tests from the ticket first and watch them fail; then implement until they pass.
4. Get CI green (fmt, clippy with warnings denied, tests, rustdoc lint, cargo-audit/deny), open a PR, and check the box here when it merges.

## Critical path

M0 gates the work pairwise, not as a block. Land the two highest-risk spikes first — **T0.2** (output ownership) and **T0.4** (trigger-rule / terminal-state tables, reaching M1 via T3) — because every M1 implementation task reaches T9. Then land the rest of M0 before each first consumer: T0.5 before T14, T0.6 before T19, T0.3 before T21, T0.7 before T13, T0.8 before T14, T0.9 before T12. Each milestone ends with its demo (T28, T38, T49, T63); the system acceptance gate (T65) requires every demo plus T69 and T70.

## Phase 0 — Project scaffolding

- [x] **001** · [T0.0a — Repository initialization and hygiene](001-T0.0a-repo-init-and-hygiene.md) · S · setup
- [x] **002** · [T0.0b — Contributor guide and branch-per-ticket workflow](002-T0.0b-contributor-and-branch-workflow.md) · S · setup — after T0.0a
- [x] **003** · [T1 — Crate layout and workspace skeleton](003-T1-crate-layout-and-workspace-skeleton.md) · S · setup — after T0.0a
- [x] **004** · [T2 — Async runtime and concurrency primitives ADR](004-T2-async-runtime-adr.md) · S · decision — after T1
- [x] **005** · [T0.10 — Stability policy and criteria partition](005-T0.10-stability-policy-and-criteria-partition.md) · S · decision
- [x] **006** · [T7 — CI pipeline and acceptance-criteria coverage matrix](006-T7-ci-pipeline-and-criteria-matrix.md) · M · setup — after T1, T0.10, T0.0b
- [x] **007** · [T8 — Compile-failure test harness](007-T8-compile-failure-test-harness.md) · S · setup — after T1, T7

## Phase 1 — Foundational decisions (M0)

- [x] **008** · [T0.2 — ADR + spike: output ownership and sharing model](008-T0.2-output-ownership-adr-spike.md) · M · decision (spike)
- [x] **009** · [T0.3 — ADR + spike: timeout abandonment and permit accounting](009-T0.3-timeout-and-permit-accounting-spike.md) · M · decision (spike)
- [x] **010** · [T0.4 — Trigger-rule and terminal-state reference tables](010-T0.4-trigger-rule-and-state-tables.md) · S · decision
- [x] **011** · [T0.5 — Bootstrap phase interface and cost model](011-T0.5-bootstrap-phase-and-cost-model.md) · S · decision
- [x] **012** · [T0.6 — ADR: run store contract](012-T0.6-run-store-contract-adr.md) · M · decision
- [x] **013** · [T0.7 — ADR: stable-name trait and fingerprint composition](013-T0.7-stable-name-and-fingerprint-adr.md) · S · decision
- [x] **014** · [T0.8 — Durable-output contract](014-T0.8-durable-output-contract.md) · S · decision
- [x] **015** · [T0.9 — C4 ordering-edge mechanics](015-T0.9-ordering-edge-mechanics.md) · S · decision
- [x] **016** · [T3 — ADR: error taxonomy design](016-T3-error-taxonomy-adr.md) · S · decision — after T0.4
- [x] **017** · [T4 — ADR: artifact serialization format and schema versioning](017-T4-artifact-serialization-format-adr.md) · S · decision — after T0.6, T0.10
- [x] **018** · [T5 — Design spike: typed handle and dependency encoding](018-T5-typed-handle-encoding-spike.md) · M · decision (spike) — after T1, T0.2

## M1 — It runs

- [x] **019** · [T9 — C1: task abstraction and error classification](019-T9-task-abstraction-and-errors.md) · M · feature — after T1, T2, T3, T0.2
- [x] **020** · [T10 — C2: typed handles](020-T10-typed-handles.md) · S · feature — after T5, T9
- [x] **021** · [T11 — C3: typed data-dependency binding](021-T11-typed-data-dependency-binding.md) · M · feature — after T10, T0.2
- [x] **022** · [T16 — C8: run context](022-T16-run-context.md) · M · feature — after T9
- [x] **023** · [T13 — C7: flow builder and node identity](023-T13-flow-builder-and-node-identity.md) · M · feature — after T10, T0.7
- [x] **024** · [T12 — Compile-failure suite for wiring](024-T12-compile-failure-suite-for-wiring.md) · S · feature (tests) — after T8, T11, T0.9
- [x] **025** · [T14 — C7: assembly validation and precomputation](025-T14-assembly-validation-and-precomputation.md) · M · feature — after T11, T13, T0.5, T0.8
- [x] **026** · [T15 — C7: determinism and purity tests](026-T15-determinism-and-purity-tests.md) · S · feature (tests) — after T14
- [x] **027** · [T17 — C10: output slots](027-T17-output-slots.md) · M · feature — after T14, T0.2
- [x] **028** · [T18 — C11: readiness tracker](028-T18-readiness-tracker.md) · M · feature — after T14, T0.4
- [x] **029** · [T19 — C19: event stream writer](029-T19-event-stream-writer.md) · M · feature — after T4, T13, T0.6
- [x] **030** · [T20 — C14: single-attempt execution core](030-T20-single-attempt-execution-core.md) · M · feature — after T16, T17, T19
- [x] **031** · [T21 — C14: per-attempt timeout](031-T21-per-attempt-timeout.md) · S · feature — after T20, T0.3
- [x] **032** · [T22 — C14: retry with jittered exponential backoff](032-T22-retry-with-backoff.md) · M · feature — after T20
- [x] **033** · [T23 — C14: panic containment](033-T23-panic-containment.md) · S · feature — after T20
- [x] **034** · [T24 — M1 run-loop driver](034-T24-m1-run-loop-driver.md) · M · feature — after T18, T20, T0.6
- [x] **035** · [T25 — C11: termination property test](035-T25-termination-property-test.md) · M · feature (tests) — after T24
- [x] **036** · [T26 — C10: bounded-memory chain test](036-T26-bounded-memory-chain-test.md) · S · feature (tests) — after T17, T24
- [x] **037** · [T27 — C19: crash-safety and I/O fault-injection tests](037-T27-crash-safety-fault-injection-tests.md) · M · feature (tests) — after T19, T24, T0.6
- [x] **038** · [T28 — M1 demo: three-node chain with retry](038-T28-m1-demo-three-node-chain.md) · M · feature (demo) — after T12, T15, T21, T22, T23, T25, T26, T27

## M2 — It survives

- [x] **039** · [T29 — C5: node policy](039-T29-node-policy.md) · M · feature — after T14, T22, T0.4, T0.5
- [x] **040** · [T30 — C9: resource registry](040-T30-resource-registry.md) · M · feature — after T16
- [x] **041** · [T31 — C12: admission pools and permit lifecycle](041-T31-admission-pools-and-permits.md) · M · feature — after T24, T29, T0.3
- [x] **042** · [T32 — C12: container limit detection](042-T32-container-limit-detection.md) · M · feature — after T31
- [x] **043** · [T33 — C13: execution class dispatch](043-T33-execution-class-dispatch.md) · M · feature — after T20, T29, T2
- [x] **044** · [T34 — C15: failure policy, propagation, and trigger-rule runtime](044-T34-failure-policy-and-propagation.md) · M · feature — after T24, T29, T0.4
- [x] **045** · [T35 — C16: cancellation core and graceful drain](045-T35-cancellation-core-and-drain.md) · M · feature — after T24, T34
- [x] **046** · [T36 — C16: OS signals, final flush, and temp cleanup](046-T36-os-signals-flush-and-cleanup.md) · M · feature — after T19, T35, T0.6
- [x] **047** · [T37 — C12: permit-release outcome matrix tests](047-T37-permit-release-outcome-matrix-tests.md) · M · feature (tests) — after T21, T23, T31, T35
- [x] **048** · [T67 — Two-concurrent-runs test](048-T67-two-concurrent-runs-test.md) · S · feature (tests) — after T24, T0.6
- [x] **049** · [T38 — M2 demo: overcommit and clean stop](049-T38-m2-demo-overcommit-and-clean-stop.md) · M · feature (demo) — after T30, T32, T33, T34, T36, T37, T67

## M3 — It explains itself

- [x] **050** · [T39 — Publish artifact schemas](050-T39-publish-artifact-schemas.md) · M · feature — after T4, T0.8, T0.10
- [x] **051** · [T40 — C20: graph artifact emission](051-T40-graph-artifact-emission.md) · M · feature — after T15, T29, T39, T0.7
- [x] **052** · [T41 — C21: fingerprints](052-T41-fingerprints.md) · M · feature — after T14, T40, T0.7
- [x] **053** · [T42 — C22: event-stream folding into run artifact](053-T42-event-stream-folding.md) · M · feature — after T19, T31, T39
- [x] **054** · [T43 — C22: run summary and critical path](054-T43-run-summary-and-critical-path.md) · S · feature — after T42
- [ ] **055** · [T44 — C23: node metrics](055-T44-node-metrics.md) · M · feature — after T16, T42
- [ ] **056** · [T45 — C25: logging and tracing integration](056-T45-logging-and-tracing-integration.md) · M · feature — after T20, T30
- [ ] **057** · [T46 — C24: diagram renderer](057-T46-diagram-renderer.md) · M · feature — after T40
- [ ] **058** · [T47 — C24: run-overlay rendering](058-T47-run-overlay-rendering.md) · S · feature — after T42, T46
- [ ] **059** · [T48 — Artifact validation and compatibility CI](059-T48-artifact-validation-compatibility-ci.md) · M · feature — after T7, T40, T42, T0.10
- [ ] **060** · [T68 — Crashed-run finalize path](060-T68-crashed-run-finalize-path.md) · S · feature (tests) — after T42
- [ ] **061** · [T49 — M3 demo: explain a run from artifacts](061-T49-m3-demo-explain-a-run.md) · M · feature (demo) — after T41, T43, T44, T45, T47, T48, T68

## M4 — It is operable

- [ ] **062** · [T50 — C4: ordering dependencies](062-T50-ordering-dependencies.md) · M · feature — after T11, T40, T0.9
- [ ] **063** · [T51 — C6: groups](063-T51-groups.md) · S · feature — after T13, T46
- [ ] **064** · [T52 — C17: teardown nodes](064-T52-teardown-nodes.md) · M · feature — after T35, T50, T0.4
- [ ] **065** · [T53 — C18: durable scratch store (local)](065-T53-durable-scratch-store-local.md) · M · feature — after T16, T0.6
- [ ] **066** · [T54a — C18: scratch survives process restart under the run store](066-T54a-scratch-survives-restart.md) · S · feature — after T53
- [ ] **067** · [T57 — C27: durable-output declaration and recording](067-T57-durable-output-declaration-recording.md) · M · feature — after T42, T0.8
- [ ] **068** · [T55 — C26: CLI contract](068-T55-cli-contract.md) · M · feature — after T34, T36, T40, T42, T46, T57, T0.6
- [ ] **069** · [T56 — C26: CLI acceptance tests](069-T56-cli-acceptance-tests.md) · M · feature (tests) — after T55
- [ ] **070** · [T58 — C27: resume core](070-T58-resume-core.md) · M · feature — after T41, T54a, T55, T57
- [ ] **071** · [T54b — C18: resume scratch carry-forward](071-T54b-resume-scratch-carry-forward.md) · S · feature — after T54a, T58
- [ ] **072** · [T59 — C27: resume acceptance tests](072-T59-resume-acceptance-tests.md) · M · feature (tests) — after T58, T54b
- [ ] **073** · [T60 — C28: single-task test kit](073-T60-single-task-test-kit.md) · M · feature — after T16, T30
- [ ] **074** · [T61 — C28: structure snapshot testing](074-T61-structure-snapshot-testing.md) · M · feature — after T40, T0.7
- [ ] **075** · [T62 — C28: full-pipeline fakes harness](075-T62-full-pipeline-fakes-harness.md) · M · feature — after T24, T60
- [ ] **076** · [T69 — Scale benchmark](076-T69-scale-benchmark.md) · S · feature (bench) — after T24, T48
- [ ] **077** · [T70 — Platform-matrix CI](077-T70-platform-matrix-ci.md) · S · feature (ci) — after T7, T32, T36
- [ ] **078** · [T63 — M4 demo: kill, resume, and review](078-T63-m4-demo-kill-resume-review.md) · M · feature (demo) — after T36, T51, T52, T56, T59, T61, T62
- [ ] **079** · [T64 — README, quickstart, and cookbook](079-T64-readme-quickstart-and-cookbook.md) · L · feature (docs) — after T49, T55
- [ ] **080** · [T65 — System acceptance gate](080-T65-system-acceptance-gate.md) · M · feature (gate) — after T7, T28, T38, T49, T63, T64, T69, T70

---

Total: 80 tickets. `T0.1` (the spec-amendment pass) is already done and has no ticket.
