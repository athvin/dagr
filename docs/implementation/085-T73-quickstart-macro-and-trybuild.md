# 085 · T73 — Quickstart/cookbook rewrite + `#[task]` trybuild suite

> **Milestone:** M5 · **Size:** M · **Type:** feature (tests) · **Components:** C1, C28
> **Branch:** `feat/t73-quickstart-macro-trybuild` · **Depends on:** T72 · **Blocks:** —

## Why / context
ADR 082 decided that `#[task]` is the primary task-authoring style and that hand-written `impl Task` stays a first-class fallback; T71 built the macro scaffold and T72 delivered the multi-arity expansion plus the tuple `InputWiring` extension in `crates/cli/src/run_flow.rs`. This ticket proves the macro end-to-end on the *canonical* surface a reader copies first — the README quickstart — and locks the macro's contract with a compile-test corpus. Concretely: rewrite `Count` and `Double` in `crates/cli/examples/quickstart.rs` to use `#[task]` while leaving `main()` and the `RunnableFlow` wiring (`register_source` / `register`) byte-for-byte unchanged, so the two node bodies shrink to a single `run` fn each and the arch.md C1 "declare four things, write no plumbing" claim becomes demonstrably true in the code a new user reads. The README's `## Quickstart` fenced block quotes the `ANCHOR: quickstart` region verbatim, and `crates/cli/tests/readme_quickstart.rs` reds the build on any drift — so the rewrite must move the anchored region and the README in lockstep, and the same suite re-proves the run still exits `0` with both nodes `succeeded` and the doubled output `42`. Alongside the example, add a `trybuild` corpus in `crates/macros/tests` that pins the macro's accept/reject boundary — every arity and execution-class variant compiles, and each documented misuse fails with a committed, stable `stderr` snapshot — turning ADR 082's "known limitation" (diagnostics may point at the `#[task]` site) into a tested, versioned contract instead of a footnote.

## Objective
- Rewrite `crates/cli/examples/quickstart.rs` so `Count` (zero-input source) and `Double` (single-input `u64`) are authored with `#[task]` — each an inherent `impl` block carrying one `async fn run` — replacing the two hand-written `impl Task` blocks; leave `main()`, the `FileSink`/`TickClock` support types, and the `RunnableFlow` registration (`register_source("count", …)` / `register::<Double, _>("double", …, counted)`) unchanged.
- Keep the `ANCHOR: quickstart` / `ANCHOR_END: quickstart` region and the README `## Quickstart` fenced block in exact sync so `readme_rust_block_matches_the_compiled_example_verbatim` stays green; verify the macro-based example produces an **identical run outcome** to the pre-macro version — same `RunOutcome`, same per-node terminal states, same doubled output value, and the same node-terminal event count.
- Update `docs/cookbook.md` (and its executable ground truth in `crates/cli/tests/cookbook.rs` where a snippet changes) so `#[task]` is the *primary* authoring style shown, hand-written `impl Task` is documented as the explicit fallback/escape hatch, and a new **"common mistakes"** section names the macro's real failure modes (bare `-> T` return, non-`Send` capture, over-8 inputs, deps mismatch at registration) with the fix for each.
- Add a `trybuild` corpus under `crates/macros/tests` with a **compile-pass** directory and a **compile-fail** directory (committed `.stderr` snapshots), driven by a `#[test]` that runs `trybuild::TestCases`.
- Document the macro's error-span limitation (diagnostics may attribute to the `#[task]` attribute site rather than the offending line) where an author will meet it — the cookbook "common mistakes" section — cross-referencing the trybuild corpus as the canonical example set.

## Test plan (write these first — TDD)

**Quickstart parity**
- Given the macro-based quickstart, when it is run with a private run-store directory, then the run's `RunOutcome`, the per-node terminal states for `count` and `double`, the value read back through the `double` handle, and the count of `node-terminal` events all equal what the hand-written version produced (same success, both `succeeded`, value `42`, exactly two node-terminal events).
- Given the README `## Quickstart` fenced `rust` block and the example's `ANCHOR: quickstart` region, when `readme_rust_block_matches_the_compiled_example_verbatim` runs, then the two are byte-identical (the rewrite moved README and anchor together).
- Given the rewritten example built as a workspace `[[example]]` target, when `cargo build`/`clippy` compile it, then `Count` and `Double` carry no hand-written `type Input` / `type Output` / `EXECUTION_CLASS` lines — the `#[task]` expansion supplies them — while `main()` and the registration calls are unchanged from the pre-macro file.

**trybuild compile-pass**
- Given a zero-input task, a single-input task (bare `T`, not `(T,)`), a two-input task, and an eight-input task, each authored with `#[task]`, when compiled, then all succeed and the generated `type Input` matches the arity rule (`()`, bare `T`, `(A,B)`, the 8-tuple).
- Given each execution-class attribute form — `#[task]` (AwaitBound), `#[task(blocking)]` (Blocking), and `#[task(compute)]` (Compute) — applied to a task, when compiled, then each succeeds and sets the corresponding associated const.
- Given a task whose `run` takes an optional `ctx: &RunContext` and a sibling that omits it, when both are compiled, then both succeed (the `ctx` parameter is detected by type and threaded or ignored accordingly).

**trybuild compile-fail**
- Given a task binding more than eight inputs, when compiled, then it fails with a stable, actionable error naming the 8-input ceiling, captured in a committed `.stderr` snapshot.
- Given a task whose struct captures a non-`Send` value (e.g. an `Rc`) held across the body, when compiled, then it fails with the natural borrow/`Send` diagnostic, captured verbatim so the "diagnostics may point at the `#[task]` site" limitation is pinned.
- Given a registration whose bound handle type does not match the task's declared deps (a deps type mismatch at the `RunnableFlow` seam), when compiled, then it fails with a stable snapshot showing the expected-vs-actual `Deps`/`Handle` types.
- Given a `run` fn returning a bare non-`Result` type (`-> u64` instead of `Result<u64, TaskError>`), when compiled, then it fails with the `compile_error!` the macro emits naming the required `Result<T, TaskError>` fix, captured in a committed snapshot.

## Definition of done
- [ ] `crates/cli/examples/quickstart.rs` authors `Count` and `Double` with `#[task]`; `main()`, `FileSink`, `TickClock`, and the `RunnableFlow` registration calls are unchanged; and the example's run output (outcome, per-node terminal states, doubled value, node-terminal event count) matches the pre-macro version.
- [ ] The README `## Quickstart` block and the example's `ANCHOR: quickstart` region are byte-identical and `crates/cli/tests/readme_quickstart.rs` (both the verbatim-sync test and the end-to-end run/artifact test) passes.
- [ ] `docs/cookbook.md` presents `#[task]` as the primary authoring style, documents hand-written `impl Task` as the fallback/escape hatch, and adds a "common mistakes" section covering bare-return, non-`Send` capture, over-8 inputs, and deps mismatch; `crates/cli/tests/cookbook.rs` stays green (updated where a snippet changed).
- [ ] `crates/macros/tests` contains a `trybuild` corpus with a compile-pass directory and a compile-fail directory, each `.stderr` snapshot committed, driven by a `trybuild::TestCases` test.
- [ ] The compile-pass corpus covers zero/one/two/eight-input arities across `#[task]`, `#[task(blocking)]`, and `#[task(compute)]`, with and without a `ctx: &RunContext` parameter; the compile-fail corpus covers over-8 inputs, non-`Send` capture, deps mismatch at registration, and a bare non-`Result` return.
- [ ] `cargo test --all` and `cargo test -p dagr-macros` pass.
- [ ] The macro error-span limitation (diagnostics may attribute to the `#[task]` attribute site rather than the offending line) is documented in the cookbook "common mistakes" section, cross-referenced to the trybuild corpus.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Any addition to what `#[task]` expresses (durability markers, custom error types, receive-mode-in-body, generics beyond what T72 shipped) — ADR 082 defers these; this ticket only exercises and documents the T71/T72 contract.
- Any engine or `InputWiring` change — T72 owns the tuple-arity wiring in `crates/cli/src/run_flow.rs`; if a compile-pass arity case does not run, that is a T72 defect, not a change made here.
- The macro scaffold, feature wiring, and `--no-default-features` build proof — T71 owns those; this ticket assumes `use dagr_core::task;` resolves the re-exported attribute.
- Broadening the CLI acceptance or coverage-gate suites (T65/T80) — this ticket ships only the quickstart parity, README-sync, cookbook, and trybuild tests its own TDD requires.
