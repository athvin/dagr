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
- [x] **055** · [T44 — C23: node metrics](055-T44-node-metrics.md) · M · feature — after T16, T42
- [x] **056** · [T45 — C25: logging and tracing integration](056-T45-logging-and-tracing-integration.md) · M · feature — after T20, T30
- [x] **057** · [T46 — C24: diagram renderer](057-T46-diagram-renderer.md) · M · feature — after T40
- [x] **058** · [T47 — C24: run-overlay rendering](058-T47-run-overlay-rendering.md) · S · feature — after T42, T46
- [x] **059** · [T48 — Artifact validation and compatibility CI](059-T48-artifact-validation-compatibility-ci.md) · M · feature — after T7, T40, T42, T0.10
- [x] **060** · [T68 — Crashed-run finalize path](060-T68-crashed-run-finalize-path.md) · S · feature (tests) — after T42
- [x] **061** · [T49 — M3 demo: explain a run from artifacts](061-T49-m3-demo-explain-a-run.md) · M · feature (demo) — after T41, T43, T44, T45, T47, T48, T68

## M4 — It is operable

- [x] **062** · [T50 — C4: ordering dependencies](062-T50-ordering-dependencies.md) · M · feature — after T11, T40, T0.9
- [x] **063** · [T51 — C6: groups](063-T51-groups.md) · S · feature — after T13, T46
- [x] **064** · [T52 — C17: teardown nodes](064-T52-teardown-nodes.md) · M · feature — after T35, T50, T0.4
- [x] **065** · [T53 — C18: durable scratch store (local)](065-T53-durable-scratch-store-local.md) · M · feature — after T16, T0.6
- [x] **066** · [T54a — C18: scratch survives process restart under the run store](066-T54a-scratch-survives-restart.md) · S · feature — after T53
- [x] **067** · [T57 — C27: durable-output declaration and recording](067-T57-durable-output-declaration-recording.md) · M · feature — after T42, T0.8
- [x] **068** · [T55 — C26: CLI contract](068-T55-cli-contract.md) · M · feature — after T34, T36, T40, T42, T46, T57, T0.6
- [x] **069** · [T56 — C26: CLI acceptance tests](069-T56-cli-acceptance-tests.md) · M · feature (tests) — after T55
- [x] **070** · [T58 — C27: resume core](070-T58-resume-core.md) · M · feature — after T41, T54a, T55, T57
- [x] **071** · [T54b — C18: resume scratch carry-forward](071-T54b-resume-scratch-carry-forward.md) · S · feature — after T54a, T58
- [x] **072** · [T59 — C27: resume acceptance tests](072-T59-resume-acceptance-tests.md) · M · feature (tests) — after T58, T54b
- [x] **073** · [T60 — C28: single-task test kit](073-T60-single-task-test-kit.md) · M · feature — after T16, T30
- [x] **074** · [T61 — C28: structure snapshot testing](074-T61-structure-snapshot-testing.md) · M · feature — after T40, T0.7
- [x] **075** · [T62 — C28: full-pipeline fakes harness](075-T62-full-pipeline-fakes-harness.md) · M · feature — after T24, T60
- [x] **076** · [T69 — Scale benchmark](076-T69-scale-benchmark.md) · S · feature (bench) — after T24, T48
- [x] **077** · [T70 — Platform-matrix CI](077-T70-platform-matrix-ci.md) · S · feature (ci) — after T7, T32, T36
- [x] **078** · [T63 — M4 demo: kill, resume, and review](078-T63-m4-demo-kill-resume-review.md) · M · feature (demo) — after T36, T51, T52, T56, T59, T61, T62
- [x] **079** · [T64 — README, quickstart, and cookbook](079-T64-readme-quickstart-and-cookbook.md) · L · feature (docs) — after T49, T55
- [x] **080** · [T65 — System acceptance gate](080-T65-system-acceptance-gate.md) · M · feature (gate) — after T7, T28, T38, T49, T63, T64, T69, T70

## M5 — It's ergonomic (and hosts many flows)

Purely additive `dagx`-inspired ergonomics and multi-flow selection over the finished engine — no existing behaviour changes and `dagr-core` stays runtime-dependency-free. The three decisions are recorded in companion ADRs, accepted alongside this plan: [082 — task-authoring macro](082-task-macro-adr.md), [086 — flow registry](086-flow-registry-adr.md), and [089 — runtime knob precedence](089-config-precedence-adr.md). Work the three streams in order: task-authoring ergonomics (T71–T73), then the flow registry (T74–T75), then runtime-knob precedence (T76–T77). Note: ticket codes continue at **T71** because T66–T70 were already used in earlier milestones.

- [x] **083** · [T71 — dagr-macros scaffold + zero/single-input `#[task]`](083-T71-dagr-macros-scaffold.md) · M · feature — after T9, T13
- [x] **084** · [T72 — `#[task]` multi-arity, ctx, ExecutionClass + tuple InputWiring](084-T72-task-macro-multiarity-and-wiring.md) · M · feature — after T71
- [x] **085** · [T73 — quickstart/cookbook rewrite + trybuild suite](085-T73-quickstart-macro-and-trybuild.md) · M · feature (tests) — after T72
- [x] **087** · [T74 — FlowRegistry + `dagr run <flow>` / `list` dispatch](087-T74-flow-registry-and-dispatch.md) · M · feature — after T13, T24, T55
- [x] **088** · [T75 — registry graph/validate routing + multi-flow example](088-T75-registry-graph-validate-and-example.md) · M · feature — after T74, T40
- [x] **090** · [T76 — precedence helper + parsers](090-T76-precedence-helper-and-parsers.md) · M · feature — after T55
- [x] **091** · [T77 — wire DAGR_* env fallbacks + expose headroom](091-T77-env-fallbacks-and-headroom.md) · M · feature — after T76, T31, T32

## M6 — Auto-discovered multi-dag binaries

Purely additive `dagx`-inspired *declarative-DAG* ergonomics over the finished engine and the M5 flow registry — no existing behaviour changes and `dagr-core` stays runtime-dependency-free. A `#[dag]` attribute (sibling to `#[task]`) declares a flow over a type-checked `FlowBuilder`, and each declaration **auto-registers** via the `inventory` crate so one binary hosts many DAGs discovered by `dagr_cli::run` with a one-line `main`. The decision is recorded in a companion ADR, accepted alongside this plan: [092 — declarative-DAG (`#[dag]` + auto-discovery)](092-dag-macro-and-autodiscovery-adr.md). Work the tickets in order: the `FlowBuilder` façade (T78), then `inventory` discovery + the `dagr_cli::run` entrypoint (T79), then the `#[dag]` macro that submits (T80), then the example, cookbook, and trybuild corpus (T81). Discovery relies on `inventory`'s leaf-binary collection, so `#[dag]`s live in the binary crate; cross-crate DAG libraries are out of scope. Note: ticket codes continue at **T78** because T71–T77 were used in M5.

- [x] **093** · [T78 — `FlowBuilder` declaration façade over `RunnableFlow`](093-T78-flowbuilder-facade.md) · S · feature — after T74
- [x] **094** · [T79 — `DagRegistration` inventory type + `dagr_cli::run` entrypoint](094-T79-dag-registration-and-run-entrypoint.md) · M · feature — after T74, T75
- [x] **095** · [T80 — `#[dag]` attribute macro (keep fn, generate factory, submit)](095-T80-dag-attribute-macro.md) · M · feature — after T78, T79
- [x] **096** · [T81 — many-dags example, cookbook, and `#[dag]` trybuild corpus](096-T81-dag-example-docs-and-trybuild.md) · M · feature (tests) — after T80

## M7 — Persistent metastore (embedded libSQL)

A lightweight, **embedded, opt-in** run index so one many-DAG binary has a single queryable place for cross-run state — derived from the event stream, not a new source of truth. Unlike M5/M6, M7 opens with a decision **ticket** (**T82**), not a pre-accepted ADR record: it amends arch.md's *permanent* "no metadata store" boundary to permit a local, non-coordinating index (keeping every other exclusion) and picks the substrate, so it needs operator sign-off before merge (a STOP per ticket-conventions §8/§10). The store is **libSQL** (the mature SQLite fork, `libsql` crate — not the pre-1.0 `turso` rewrite): embedded local file, multi-process WAL, single-writer + `busy_timeout` + `BEGIN IMMEDIATE` + bounded `SQLITE_BUSY` retry. Writes are **guaranteed** — a live tee sink during a run (same `SinkFault` contract as the JSONL sink) plus an idempotent `sync` reconcile for backfill/repair. Purely additive and behind a **default-off `metastore` feature**; `dagr-core` stays runtime-dependency-free and access is native only (`sqlite3`/`turso`/`libsql`, no Postgres wire). Work in order: ADR + arch.md amendment (T82), crate + schema + connection seam (T83), reconcile `sync` (T84), multi-process write validation (T85), guaranteed live tee (T86), example + docs (T87), acceptance gate (T88). Note: ticket codes continue at **T82** because T71–T81 were used in M5/M6.

- [x] **097** · [T82 — ADR: metastore scope carve-out + libSQL substrate](097-T82-metastore-scope-and-substrate-adr.md) · S · decision
- [x] **098** · [T83 — `dagr-metastore` crate: schema + libSQL connection seam](098-T83-dagr-metastore-crate-and-schema.md) · M · feature — after T82
- [x] **099** · [T84 — event→row mapping + `dagr metastore sync` (reconcile)](099-T84-metastore-reconcile-sync.md) · M · feature — after T83, T42
- [x] **100** · [T85 — multi-process write validation + concurrency hardening](100-T85-metastore-multiprocess-write-tests.md) · M · feature (tests) — after T84, T67
- [x] **101** · [T86 — guaranteed live metastore tee sink](101-T86-metastore-live-tee-sink.md) · M · feature — after T84, T24, T55
- [x] **102** · [T87 — native access, many-dags metastore example, and cookbook](102-T87-metastore-example-and-docs.md) · M · feature (docs) — after T86, T81
- [x] **103** · [T88 — M7 metastore acceptance gate](103-T88-metastore-acceptance-gate.md) · M · feature (gate) — after T85, T86, T87

## M8 — Run/data lineage (fast-follow)

Additive **data-lineage** enrichment over the M7 metastore, sequenced after it (metastore-first was an explicit operator choice). The Airflow `models/` review found dagr already models run/attempt state richly; the one gap is *what each run produced and consumed*. M8 enriches the durable-output contract with reference metadata (content hash/size), promotes production and consumption to first-class append-only lineage records in the event stream + fold, then projects them into the metastore — reusing the T84 reconcile and T86 live paths. Everything is event-stream-first and schema-versioned-additive; `dagr-core` stays runtime-dependency-free. Work in order: reference metadata + resume mutation-detection (T89), produced/consumed lineage events (T90), lineage projection into the metastore (T91).

- [x] **104** · [T89 — durable-reference metadata + resume mutation-detection](104-T89-durable-reference-metadata.md) · M · feature — after T88, T57, T42, T58
- [ ] **105** · [T90 — produced/consumed lineage events](105-T90-produced-consumed-lineage.md) · M · feature — after T89, T50
- [ ] **106** · [T91 — lineage projection into the metastore (+ optional asset identity)](106-T91-lineage-metastore-projection.md) · M · feature — after T90, T84, T86

---

Total: 80 M0–M4 tickets (all merged), plus 7 M5 tickets (**T71–T77**, docs 083–091) and 4 M6 tickets (**T78–T81**, docs 093–096), plus 7 M7 tickets (**T82–T88**, docs 097–103) and 3 M8 tickets (**T89–T91**, docs 104–106). The M5 ADRs (082, 086, 089) and the M6 ADR (092) are decision records like ADR 081 and are not counted as tickets; M7's ADR (097 · **T82**) is instead a decision **ticket** — it amends arch.md's permanent boundary and needs operator sign-off, so it ships through the normal branch/PR flow. `T0.1` (the spec-amendment pass) is already done and has no ticket.
