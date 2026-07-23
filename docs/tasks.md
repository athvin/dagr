# dagr — task breakdown

Engineering tasks for building the tool specified in [arch.md](arch.md). Milestones M1–M4 match the spec's build order; **M0 is pre-code work** — decisions, spikes, and scaffolding that the adversarial design review (2026-07-22) showed must land before implementation starts. Sizes: **S** under a day, **M** 1–3 days, **L** up to a week.

**Critical path.** M0 gates the work pairwise, not as a block: T0.2 and (via T3) T0.4 gate every M1 implementation task, since all of M1 reaches T9; T0.5 first bites at T14; T0.6 at T19; T0.3 at T21; T0.7 gates T13 (and later T40, T41, T61); T0.8 gates T14's durable-contract check and T39 onward; T0.9 gates T12 and T50. Practical reading: land T0.2 and T0.4 first, the rest of M0 before their first consumers. Decisions for later components that leak backward (C4 semantics into M1's compile-fail suite, node identity into M1's builder, the durable-output contract into M1 assembly checks, the run store into M1's event writer) are deliberately made in M0 while their full implementations stay in their spec milestones.

Where the spec amendment already resolved a task's open question, the resolution is folded into the task text; only genuinely open questions remain listed as **Q**.

---

## M0 — Decisions, spikes, scaffolding

**T0.1 — Spec amendment pass** ✅ *done (this revision of arch.md)*
Applied the full review amendment list to arch.md with a changelog; absorbed the former T6 reconciliation task. Blocker-resolving amendments (ownership model, timeout semantics, trigger rules, C4 resolution, teardown carve-out, bootstrap phase, durable-output contract, run store, C9 requirements, C27 algorithm, criteria 4/8 partition) are now spec text.

**T0.2 — ADR + spike: output ownership and sharing model** (M, covers C1/C3/C10; blocks T5, T9, T11, T17, T26)
Prototype the spec's chosen model — sole-consumer-owns, multi-consumer-shared-read, per-edge clone-on-read opt-in — with a non-`Clone` type, a retry-after-consumer-failure case, and the resulting author-visible bounds. Highest-risk feasibility item in the plan; if the model doesn't survive contact with the borrow checker, the spec decision reopens.

**T0.3 — ADR + spike: timeout abandonment and permit accounting** (M, covers C12/C14; blocks T21, T31, T37)
Prototype per-class cancellation: future-drop for await-bound; zombie-thread bookkeeping, permit-held-until-return, and deferred retry for blocking/compute; zombie-cost reporting shape.

**T0.4 — Decision: trigger-rule and terminal-state reference tables** (S, covers C11/C15; blocks T18, T29, T34, T50, T52; feeds event vocabulary in T19)
Turn the spec's Vocabulary section into the normative internal enums — terminal states plus the state classes that make trigger rules total (success-like / skip-like / failure-like / stop-like) — and the per-rule fires / can-never-fire decision table, reviewed against every component that references it.

**T0.5 — Decision: bootstrap phase interface and cost model** (S, covers C5/C7/C12; blocks T14, T29, T31, T32; settles T24's run-identity seam jointly with T0.6)
Define the assembly→bootstrap→execution seam as API: what assembly produces, what bootstrap consumes, the bootstrap-failure artifact path, and the per-pool cost vector types (bytes / thread counts, working memory vs output residency).

**T0.6 — ADR: run store contract** (M, covers C18/C19; blocks T19, T24, T27, T36, T53, T54a, T58)
Sink trait (append line, flush), base-location flag/env, `<base>/<pipeline>/<run-id>/` layout, UUIDv7 run ids, event-write-failure path, flush semantics, prune semantics.

**T0.7 — ADR: stable-name trait and fingerprint composition** (S, covers C20/C21; blocks T13, T40, T41, T58, T61)
The stable-name trait/derive for tasks and payload types; structural-fingerprint vs policy-hash field lists; canonicalization and algorithm versioning; group exclusion. Decided in M0 although C6/C21 are M3–M4, because T13 bakes node identity in.

**T0.8 — Decision: durable-output contract** (S, covers C27; blocks T57, T58; adds a field to T39's schemas)
The durability policy flag + serialize-reference/rehydrate trait pair, assembly-time enforcement, existence-check semantics.

**T0.9 — Decision: C4 ordering-edge mechanics** (S, covers C2/C4; blocks T12, T50)
Spec chose registration-time backward references (Option A). Decide the API shape that enforces it and what the compile-fail suite asserts for ordering-edge cycles.

**T0.10 — Stability policy and criteria partition** (S, covers system-level; reshapes T7, T39, T48, T65)
MSRV, semver, schema-evolution rules, fixture-corpus plan, fingerprint-algorithm versioning; the machine/human criterion classification seeding the coverage matrix.

**T1 — ADR: crate layout and workspace skeleton** (S)
Committed ADR plus a compiling Cargo workspace skeleton (core, artifact, render, cli crates) with placeholder lib targets.
- Q: Single facade crate vs multi-crate workspace; must the renderer be a separate binary given C24 requires rendering with no access to the pipeline binary?

**T2 — ADR: async runtime and concurrency primitives** (S, covers C13/C14/C16)
ADR confirming tokio (now a spec-level public-dependency commitment), blocking-pool strategy, compute-pool implementation, cancellation-token primitive, **and the isolated framework runtime** for timers/cancellation/event-writing/signals required by C13. Also produces the C28 test-runtime shape (plain runtime for await-bound task tests).
- Q: Dedicated compute pool (e.g. rayon) vs capped semaphore over `spawn_blocking`?

**T3 — ADR: error taxonomy design** (S, covers C1/C14; after T0.4)
Task-facing error enum (retryable/permanent/skip) and the framework-internal outcome taxonomy matching the spec's terminal-state table (including `abandoned`, `satisfied-from-prior`). Resolved: the runner's classification is the superset; the task-facing enum stays three-valued.

**T4 — ADR: artifact serialization format and schema versioning** (S, covers C19/C20/C22; after T0.6, T0.10)
Event-stream and artifact encodings (JSONL events, JSON artifacts), schema-version field semantics, published-schema location in-repo. Durability question resolved by T0.6 (run store).
- Q: Schema language for "validates against its published schema" — JSON Schema draft, and which validation crate?

**T5 — Design spike: typed handle and dependency encoding** (M, covers C2/C3; after T1, T0.2)
Throwaway prototype proving the handle/binding API makes wrong-type, wrong-arity, and cyclic constructions fail to compile. Resolved by spec: arity ceiling documented with `#[diagnostic::on_unimplemented]` at the cliff; error messages assert both type names appear (not prose quality); trigger-rule restriction enforced via builder typestate.
- Q: Exact arity ceiling (pick during spike; 8 is the working assumption). Single-input tasks take `T`, not `(T,)` — confirm ergonomics.

**T7 — CI pipeline and acceptance-criteria coverage matrix scaffold** (M, after T1, T0.10)
GitHub Actions running fmt/clippy/test on every PR plus the checked-in criteria matrix (criterion id → test id **or** `human` classification per T0.10) with a CI script failing on unmapped machine criteria. Include cargo-audit/deny per the spec's supply-chain posture, and the pinned-toolchain policy for UI tests.

**T8 — Compile-failure test harness** (S, covers C2/C3; after T1, T7)
trybuild/ui-test harness wired into CI with one passing sample and a snapshot-update flow. Resolved: pinned workspace toolchain; snapshots assert both type names only; toolchain bumps regenerate deliberately.

---

## M1 — It runs (C1, C2, C3, C7, C8, C10, C11, C14, C19)

**T9 — C1: task abstraction and error classification** (M, after T1, T2, T3, T0.2)
Task abstraction with declared input/output types, execution class as fourth declared element (default await-bound), constructor-captured configuration, no-input support, `&mut self` work signature, bounds per spec (`Send + 'static`; outputs `Send + Sync + 'static`), errors classified retryable/permanent/skip.
- Q: ~~Trait impl vs generic struct vs closure wrapper — and how "types readable from the declaration" is judged for closures.~~ **Resolved (ticket 019 design note):** a `Task` **trait** implemented on an author-owned config `struct`; the readable-types rule is defined over the impl's associated types `Input`/`Output`; **closures are not permitted** (no associated-type surface — matches dagx's bad-ergonomics finding), which answers the closure sub-question.

**T10 — C2: typed handles** (S, after T5, T9)
Cheap, copyable `Handle<T>` obtainable only by registering a node (with an explicit node name), no public constructor, no by-name/index/string lookup.

**T11 — C3: typed data-dependency binding** (M, after T10, T0.2)
Binding API with exact type matching, tuple arities per T5, fan-out (one handle to many consumers), and the ownership model from T0.2 (sole-consumer-owns / shared-read / per-edge clone-on-read).

**T12 — Compile-failure suite for wiring** (S, after T8, T11, T0.9)
Compile-fail tests: cycle inexpressibility (data and ordering edges per T0.9), wrong-type binding with both type names, wrong arity, non-default trigger rule on a data-dependent node, unforgeable handles, ownership-demand on a shared value.

**T13 — C7: flow builder and node identity** (M, after T10, T0.7)
Builder accumulating registrations into an immutable pipeline. Resolved: identity is the explicit registration name — unique at assembly, reorder-stable, group-excluded.

**T14 — C7: assembly validation and precomputation** (M, after T11, T13, T0.5, T0.8)
Assembly reporting all problems (duplicate names naming both declarations, empty pipeline, invalid class overrides, duplicate stable names, durable-without-contract per T0.8, ownership-mode conflicts — owned demand on a multi-consumer value, owned edge into a retrying node without clone-on-read, nonzero teardown cost), the zero-consumer non-unit-output *warning*, the environment-capture allowlist declaration API (empty by default), and precomputation (consumer counts, dependency counts, execution order, fingerprint slot per T0.7). Capacity check moved to bootstrap per T0.5. Enforce no-parameters-during-assembly.

**T15 — C7: determinism and purity tests** (S, after T14)
Byte-identical graph output across two in-process assemblies; assembly succeeds in an empty environment.
- Q: Mechanical proof of no-filesystem/no-network — sandboxing, syscall audit, or review convention?

**T16 — C8: run context** (M, after T9)
RunContext with all spec fields, hand-constructable in unit tests. Resolved: data interval is a caller-supplied opaque pair; registry/scratch access are additive APIs landing with C9/C18; includes resource-requirement declaration plumbing (feeds T30).

**T17 — C10: output slots** (M, after T14, T0.2)
Typed once-writable slot per node, assembly-time consumer references, **release when every consumer is terminal and every consumer's closure has returned** (a zombie consumer pins both the value and its residency lease — amended C10), retained flag with post-run redemption API, slot-lease accounting hooks (output residency held until actual release), loud read-before-fill defect naming the node.
- Q: Type-erasure strategy for heterogeneous slot storage keeping reads lookup-free and type-check-free.

**T18 — C11: readiness tracker** (M, after T14, T0.4)
Dependency countdown; trigger-rule evaluation only when all upstreams terminal, per T0.4's fires/can-never-fire table (M1 ships `all-succeeded` only, but against the final rule interface); immediate propagated-terminal assignment for can-never-fire; diamond test proving no wave batching.

**T19 — C19: event stream writer** (M, after T4, T13, T0.6)
Append-only writer through the T0.6 sink: schema version, gapless sequence numbers, wall-clock stamp **and** monotonic offset per record, run identity on every record, run-started event carrying the full artifact header, stream opens at bootstrap before assembly results are acted on.

**T20 — C14: single-attempt execution core** (M, after T16, T17, T19)
Runner: open span, execute one attempt, classify, fill slot on success, exactly one attempt record emitted.

**T21 — C14: per-attempt timeout** (S, after T20, T0.3)
Per-class semantics from T0.3: future-drop for await-bound (immediate permit release); blocking/compute marked timed-out immediately, permit held until closure return, retry deferred, late results barred from slots and scratch. Timeout is retry-eligible by default.

**T22 — C14: retry with jittered exponential backoff** (M, after T20)
Per-node max attempts, retry only retry-eligible errors, exponential backoff with jitter and cap, no-resynchronization test. Interim M1 retry knob migrates into C5 policy in M2.

**T23 — C14: panic containment** (S, after T20)
Catch, attribute to node via task-local, convert to permanent failure, run proceeds. Resolved: startup check refuses `panic=abort` with a message naming the fix; `AssertUnwindSafe` at the boundary; resource poisoning documented as the resource author's pattern; hook installed once, coexists with test harness.

**T24 — M1 run-loop driver** (M, after T18, T20, T0.6)
Driver admitting ready nodes, spawning attempts, feeding outcomes back, run-started/run-finished events, terminating exactly when nothing is pending or in flight (bounded grace wait for zombie closures at natural run end, zombie-at-exit events emitted); captures allowlisted environment values at bootstrap; framework machinery on the isolated runtime per T2. Resolved: run identity is UUIDv7 (operator-overridable) minted at bootstrap, store and stream opened before assembly executes so assembly failures land in the record.

**T25 — C11: termination property test** (M, after T24)
Property test over random DAGs with randomized outcomes: every run terminates, every node in exactly one terminal state.

**T26 — C10: bounded-memory chain test** (S, after T17, T24)
Hundred-node chain: allocator-level peak does not grow with length when nothing is retained. Resolved: instrumented allocator, not RSS.

**T27 — C19: crash-safety and I/O fault-injection tests** (M, after T19, T24, T0.6)
Kill a child run at random points: valid prefix (≤1 trailing partial record), gapless sequences. Extended per review: disk-full and failing-sink injection proving the sink-failure cancellation path and exit code.

**T28 — M1 demo: three-node chain with retry** (M, after T12, T15, T21, T22, T23, T25, T26, T27)
CI-run example: three-node chain, middle node fails once and retries to success, event-stream walker asserts every transition — the M1 done-when.

---

## M2 — It survives (C5, C9, C12, C13, C15, C16)

**T29 — C5: node policy** (M, after T14, T22, T0.4, T0.5)
Policy struct: retries, backoff, timeout, cost vector (bytes/threads; working + output residency per T0.5), trigger rule (closed set per T0.4), constrained class override (invalid override fails assembly), group, retention, durability flag. All-defaults node behaves identically to no-policy node.

**T30 — C9: resource registry** (M, after T16)
Type-keyed immutable registry built in `main` (duplicate same-type registration rejected as ambiguous); bootstrap validation against declared requirements naming resource + requiring nodes, asserting the bootstrap-failure artifact is produced; newtype disambiguation documented; fake substitution; secret marker wrapper with no Debug/Display and sentinel-based redaction test.

**T31 — C12: admission pools and permit lifecycle** (M, after T24, T29, T0.3)
Weighted memory/thread pools; all-or-nothing multi-pool acquisition; oldest-ready-first with bounded bypass; permit held for whole attempt; zombie (abandoned-but-running) cost counted until closure return; slot-lease charging held until actual slot release; warning for undeclared-cost nodes in memory-constrained runs; permit-wait recorded separately.
- Q: Pools beyond memory and threads ("at minimum") in scope for v1?

**T32 — C12: container limit detection** (M, after T31)
Bootstrap probe: cgroup v2 → v1 → host; unlimited sentinels → host; ≥1 unit per pool; 20% headroom default; pinning flag (also the CI determinism mechanism); too-big-node rejection at bootstrap, asserting the bootstrap-failure artifact is produced.

**T33 — C13: execution class dispatch** (M, after T20, T29, T2)
Three-class dispatch with policy override; starvation test (long sync task doesn't delay await-bound work); safety-machinery isolation test (all task workers blocked → timeout still fires, SIGTERM still yields a complete stream).

**T34 — C15: failure policy, propagation, and trigger-rule runtime** (M, after T24, T29, T0.4)
Stop-on-first-failure (with teardown carve-out awareness for M4) and continue-independent; propagation only when a trigger rule is unsatisfiable; `upstream-skipped` carries originating node; skip-only runs succeed. Implements and tests runtime evaluation of all three trigger rules per T0.4's fires/can-never-fire table: `all-terminal` firing after an upstream failure, `any-failed` firing on a failure-like upstream and marking `skipped` when the contingency never arises, propagated-state selection by state class, and stop-mode contingency admission (consume-nothing non-default-rule nodes admitted on the final picture) — covering C11's per-rule criterion. Resolved: pending unrelated default-rule nodes under stop mode end `cancelled`.
- Q: Mode selection surface in M2, before the CLI exists — builder-level policy with CLI override later?

**T35 — C16: cancellation core and graceful drain** (M, after T24, T34)
Run-scoped token with per-attempt children; grace default 10 s (flag); drain-before-exit; `cancelled` vs `abandoned` classification; shutdown-budget arithmetic printed at startup.

**T36 — C16: OS signals, final flush, and temp cleanup** (M, after T19, T35, T0.6)
SIGTERM/SIGINT → cancel, complete stream (fsync) before exit within budget; per-run temp-dir convention, removed by next invocation; bounded wait + distinct exit code on unwritable sink at shutdown.

**T37 — C12: permit-release outcome matrix tests** (M, after T21, T23, T31, T35)
Tests inducing each outcome — success, permanent failure, retryable failure, timeout (both classes), panic, cooperative cancellation, abandonment — asserting the permit ledger including zombie accounting never exceeds capacity, and that slot residency pinned by a zombie consumer stays counted until its closure returns.

**T67 — Two-concurrent-runs test** (S, after T24, T0.6)
Two simultaneous runs of one binary on one machine: disjoint run-store directories, both streams valid and gapless, no file collision.

**T38 — M2 demo: overcommit and clean stop** (M, after T30, T32, T33, T34, T36, T37, T67)
CI test: combined *declared* demand exceeds pinned memory capacity yet the run completes without exceeding it (capacity pinned via the T32 flag — resolves the CI-portability question); induced mid-run failure stops cleanly, all permits released, nothing orphaned — the M2 done-when.

---

## M3 — It explains itself (C20, C21, C22, C23, C24, C25)

**T39 — Publish artifact schemas** (M, after T4, T0.8, T0.10)
Versioned checked-in schemas for graph artifact, run artifact, event records — including the durable-reference field, `satisfied-from-prior` state, distinct `assembly-failed`/`bootstrap-failed` variants, the single-node artifact variant with `not-requested` marking, zombie-at-exit events, allowlisted environment capture — plus a validation helper for tests and CI.

**T40 — C20: graph artifact emission** (M, after T15, T29, T39, T0.7)
Deterministic emission (byte-identical outside generation time) in an empty environment: stable declared names (never `type_name` except as debug field), full effective policy, declared resource requirements, edge kinds and carried type names, versioned header with build provenance (tool version, git SHA, lockfile hash, embedded at build).

**T41 — C21: fingerprints** (M, after T14, T40, T0.7)
Structural fingerprint + policy hash per T0.7: canonical ordering, algorithm version, group exclusion, defaulted-values-hash-identical test, cross-toolchain stability test (two toolchains in CI), change/no-change matrix.

**T42 — C22: event-stream folding into run artifact** (M, after T19, T31, T39)
Standalone fold function: full or truncated stream → artifact; one record per attempt with monotonic-offset phase durations (sums exact by construction), declared-vs-measured cost, worker id, structured error, metrics, durable references; summary with critical path, peak slot residency, retained values, and zombie-pinned time and capacity; allowlisted environment values in the header with a negative sentinel test (nothing outside the allowlist appears); interrupted marking; `assembly-failed`/`bootstrap-failed` variants.
- Q: Canonical phase list — proposal: ready-wait, permit-wait, executing, backoff; confirm during implementation.
- Q: "Which worker" — thread id, pool name, or both.

**T68 — Crashed-run finalize path** (S, after T42)
Test: kill a run, fold its stream with the standalone function into an interrupted artifact — the tested path for system criterion 3's crash clause (verb wiring lands in T55).

**T43 — C22: run summary and critical path** (S, after T42)
Total elapsed + critical-path time; test distinguishing structure-limited from resource-limited runs.
- Q: Critical-path definition under retries and permit waits — is wait time on the path or excluded? (Escalate to a short ADR before implementing.)

**T44 — C23: node metrics** (M, after T16, T42)
Open metrics API; framework metrics under reserved `dagr.` prefix (task use fails at attach); numeric values with unit-suffix names; 128-entry/16 KiB caps with deterministic recorded truncation; allocator-attributed peak memory via task-local.

**T45 — C25: logging and tracing integration** (M, after T20, T30)
Span-per-attempt capturing third-party lines; structured/human modes switchable without code change (env var in M3, CLI flag in M4); sentinel-based secret-redaction test scoped to framework output paths.

**T46 — C24: diagram renderer** (M, after T40)
DOT + Mermaid from a graph artifact only; edge kinds styled distinctly; group clusters; reference-tool acceptance in CI; golden files; 30-node fixture.

**T47 — C24: run-overlay rendering** (S, after T42, T46)
Terminal-state coloring per the normative taxonomy (originated vs propagated skips distinct), duration annotations, works on historical artifacts.

**T48 — Artifact validation and compatibility CI** (M, after T7, T40, T42, T0.10)
CI validates every emitted artifact against schemas; fixture corpus frozen per released schema version (seeded at M3 per T0.10), parsed forever after; includes the ten-thousand-attempt scale artifact.

**T49 — M3 demo: explain a run from artifacts** (M, after T41, T43, T44, T45, T47, T48, T68)
CI test: produce both artifacts, render overlay, programmatically answer "which node was slowest, and was it waiting or working?" from artifacts alone — the M3 done-when.

---

## M4 — It is operable (C4, C6, C17, C18, C26, C27, C28)

**T50 — C4: ordering dependencies** (M, after T11, T40, T0.9)
Registration-time backward-reference ordering edges attachable to any node; non-default trigger rules restricted to consume-nothing nodes (typestate); recorded and rendered distinctly; default-rule propagation across ordering edges tested.

**T51 — C6: groups** (S, after T13, T46)
Presentation-only labels (no nesting): artifact organization, diagram clustering, excluded from identity and fingerprint; removal/rename changes no behavior and no fingerprint.

**T52 — C17: teardown nodes** (M, after T35, T50, T0.4)
Ordered-after-set teardown firing when all covered nodes terminal; fresh signal, 15 s default deadline (flag); admission bypass; upstream terminal states via context; failure isolation; runs under termination-signal cancellation; resume interaction (covered nodes never satisfied-from-prior).
- Q: "Never have data dependencies" — compile-time or assembly-time enforcement.

**T53 — C18: durable scratch store (local)** (M, after T16, T0.6)
Per-run, per-node namespaced KV (opaque bytes) under the run store; enforced cross-node isolation; attempt-1-write-read-on-attempt-2 test; deleted on node success; purged at run end unless durable store; I/O failure classified retry-eligible.

**T54a — C18: scratch survives process restart under the run store** (S, after T53)
Scratch under the run-store base survives a full process restart, retained for non-succeeded nodes per the amended C18 lifecycle (nothing deleted implicitly at run end; prune removes it).

**T54b — C18: resume scratch carry-forward** (S, after T54a, T58)
Resume copies scratch forward for re-executing nodes from the linked prior run into the new run's namespace.

**T55 — C26: CLI contract** (M, after T34, T36, T40, T42, T46, T57, T0.6)
Library-supplied verbs: graph, validate, render, run, single-node (replay-from-run per amended C26, rehydrating inputs from durable references via T57), resume (stubbed until T58), fold (wiring T42's function), prune; typed parameter struct with derived parsing; reserved library-flag namespace; exit-code table by cause with precedence (run-failure = `failed`/`timed-out` on a non-teardown node, beats consequent cancellation; replay refusal shares the resume-refusal code); no-arg help.

**T56 — C26: CLI acceptance tests** (M, after T55)
Black-box tests over two sample pipelines: identical verb behavior, every exit code including the stop-on-first-failure precedence case, validate prints all problems, invalid parameters rejected at bootstrap with the bootstrap-failure artifact produced, parameter/flag collision rejected, single-node replay (rehydrated inputs, non-durable-input refusal naming the input, standalone no-input run, `not-requested` marking in the variant artifact), and prune by count and age with nothing deleted implicitly beforehand.

**T57 — C27: durable-output declaration and recording** (M, after T42, T0.8)
Durability policy flag + reference contract (serialize-reference/rehydrate) per T0.8; assembly rejects durable-marked nodes lacking the contract; reference recorded per attempt in the artifact.

**T58 — C27: resume core** (M, after T41, T54a, T55, T57)
Resume verb: structural-fingerprint gate with printed structural diff; policy-hash divergence proceeds with per-node diff; algorithm-version and tool-version refusals distinct; parameters/interval derived from prior artifact (conflict → refuse with diff; force flag recorded); reference existence check up front; the seed/closure/demand algorithm per amended C27 (seed = non-succeeded + teardown-covered; downward closure re-runs; demanded non-durable producers join the seed; undemanded prior successes are `satisfied-from-prior` even when not durable); slots filled by rehydration on demand; durable references copied forward so the resumed artifact is self-contained; new artifact linked to parent and lineage root.

**T59 — C27: resume acceptance tests** (M, after T58, T54b)
Fingerprint refusal with diff; policy-change proceed-with-diff; durable-success satisfied; in-memory success re-run only when demanded; undemanded non-durable success satisfied-from-prior (the cleanup-after-publish shape, with the downstream rule firing on the satisfied upstream); full-success no-op; dangling-reference plan failure; multi-generation resume with lineage linkage and reference copy-forward; scratch carry-forward observed by a re-executing node; parameter-conflict refusal and force-flag recording.

**T60 — C28: single-task test kit** (M, after T16, T30)
Hand-built context + fake resources; sync tasks need no runtime; await-bound tasks get the provided test runtime; demonstrated in an example test.

**T61 — C28: structure snapshot testing** (M, after T40, T0.7)
Semantic comparison (node set, edge set, effective policies; volatile header fields excluded); structural-diff output; blessed single-command fixture regeneration; canonical stably-ordered serialization; no failure on rebuild/toolchain bump/group rename.

**T62 — C28: full-pipeline fakes harness** (M, after T24, T60)
End-to-end harness on the real scheduler against fakes with a CI-enforced completes-in-seconds budget; supports scripted task outcomes (also serves criterion 4's interpretive-determinism check in T65).

**T69 — Scale benchmark** (S, after T24, T48)
CI benchmark: thousand-node no-op graph held under the 1 ms/node overhead budget; fails on regression.

**T70 — Platform-matrix CI** (S, after T7, T32, T36)
Linux tier-1 full suite (including fault-injection and signal tests); macOS core-suite job with host-fallback pool sizing; platform-conditional criteria named in the coverage matrix; Windows explicitly absent.

**T63 — M4 demo: kill, resume, and review** (M, after T36, T51, T52, T56, T59, T61, T62)
CI test: kill mid-run, resume skips completed durable work, and a structural change is caught by the structure fixture — the M4 done-when.

**T64 — README, quickstart, and cookbook** (L, after T49, T55)
README quickstart (empty directory → compiled, run, artifact-inspected two-node pipeline) with CI-verified verbatim code blocks; rustdoc-on-public-items lint; cookbook: fan-out-inside-one-node with declared-cost rule, fan-in, branch-in-task (self-skip vs succeed-with-empty), incremental cursors via scratch, durable stage boundaries, non-`Send` capture fixes, same-typed resources via newtypes. Criterion-6 locality check via structure-diff on a reference pipeline.

**T65 — System acceptance gate** (M, after T7, T28, T38, T49, T63, T64, T69, T70)
CI job: criteria matrix maps every machine-classed criterion to a passing test and every human-classed criterion to the release checklist (criterion 8 as partitioned); structural-determinism check (two builds, two toolchains → same fingerprints, byte-identical graph artifacts); interpretive-determinism check (scripted outcomes through the T62 harness → identical artifacts).

---

## Dependency notes

- **M0 waves:** T0.1 ✅ → wave 1: T0.2–T0.10, T1, T2 in parallel → wave 2: T3 (after T0.4), T4 (after T0.6, T0.10), T5 (after T1, T0.2), T7 (after T1, T0.10) → wave 3: T8 (after T1, T7).
- **M1 entry:** T9 is blocked on T0.2 directly and T0.4 transitively (via T3). T0.5, T0.6, T0.3, and T0.8 may still be in flight when T9 starts, but must land before their first consumers (T14, T19, T21, T14 respectively).
- **Backward-leaking decisions** (T0.7 identity → T13 in M1; T0.9 ordering edges → T12 in M1; T0.8 durability → T14's assembly check in M1, then T39/T57) are decided in M0; component implementations stay in their spec milestones.
- Each milestone ends with its demo task (T28, T38, T49, T63), which is the spec's done-when executed in CI; the acceptance gate T65 additionally requires T51, T69, and T70, so nothing is an orphan.
