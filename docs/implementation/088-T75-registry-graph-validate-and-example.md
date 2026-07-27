# 088 · T75 — Registry graph/validate routing + multi_flow example + docs

> **Milestone:** M5 · **Size:** M · **Type:** feature · **Components:** C7, C20, C24, C26
> **Branch:** `feat/t75-registry-graph-validate-example` · **Depends on:** T74, T40 · **Blocks:** —

## Why / context
ADR 086 adds `dagr_cli::registry::FlowRegistry` (name → `Fn() -> RunnableFlow` factory) and the `run_registry(&registry, argv)` dispatch entrypoint so one binary can host many named flows; T74 lands that core plus `run` and `list`. This ticket completes the registry by routing the remaining *pipeline-bound* verbs that select a flow — `graph`, `validate`, and the flow-selecting shape of `single-node`/`prune` — and by shipping a copyable authoring pattern. The crux is the ADR's factory decision: `RunnableFlow::run(self)` **consumes** the flow and the type is not `Clone` (`crates/cli/src/run_flow.rs`), while `graph_verb` and `validate_verb` need a live `&Pipeline` (`crates/cli/src/graph.rs`, `crates/cli/src/contract.rs:725`), which is only obtainable by assembling a flow. So `run_registry` calls the selected factory **once per verb**: `graph <flow>` builds a fresh `RunnableFlow`, finishes+assembles it, and emits the graph artifact (C20/T40); `validate <flow>` builds another, assembles, and reports every problem. This ticket also honours the ADR's per-verb exit-code fidelity — `graph`/`validate` map their *own* `ExitCode`/error types (not through `exit_code_for_run`, which is only for a completed `RunReport`) — and ships `crates/cli/examples/multi_flow.rs` plus a `docs/` usage guide so a pipeline author can copy the whole pattern.

## Objective
Route the remaining flow-selecting verbs through `run_registry` and ship the copyable pattern.

- Route `graph` and `validate` (and the flow-selecting shape of `single-node`/`prune`) through `run_registry` by **re-invoking the selected factory per verb** — each verb gets a fresh `RunnableFlow`, finished into a `Pipeline`, so a single consumed flow never has to answer two verbs.
- For `graph <flow>`: build the flow, obtain a live `&Pipeline`, and emit the C20 graph artifact via the existing `graph_verb` (C20/T40) — byte-identical to what a single-flow binary emits for that flow.
- For `validate <flow>`: build the flow and run `validate_verb` (`crates/cli/src/contract.rs`), which runs assembly (C7) only and prints **every** problem, exiting `Success` on a clean assembly or `AssemblyFailure` (3) otherwise.
- Apply the ADR's **single-flow ergonomic default** uniformly across these verbs: a registry with exactly one flow lets the name be omitted (`dagr graph` selects the sole flow); a multi-flow registry with no name given fails with the same helpful "name required (etl, analytics)" message; an unknown name exits `InvalidUsage` (2) listing the available flows.
- Map each verb's own outcome to its C26 code (ADR "per-verb exit-code fidelity"): `graph`/`validate` produce their own `ExitCode`/error types directly — **not** via `exit_code_for_run`, which is reserved for a completed `RunReport` (`crates/cli/src/contract.rs:203`).
- Add `crates/cli/examples/multi_flow.rs` demonstrating a two-flow binary (`etl` and `analytics`) that registers both factories and dispatches via `run_registry(&registry, std::env::args_os())`, mirroring the ADR sketch and the existing `crates/cli/examples/quickstart.rs` style.
- Add a `docs/` usage guide for pipeline authors: the `FlowRegistry` builder API, how it extends the C26 contract (the optional `flow_name` positional), and the single-flow ergonomics — copyable end to end.

## Test plan (write these first — TDD)

**Verb routing**
- Given a registry with `etl` and `analytics`, when `graph etl` runs, then the selected factory is invoked, `graph_verb` emits `etl`'s graph artifact (byte-identical to a single-flow `etl` binary), and the process exits `Success`.
- Given the same registry, when `validate analytics` runs, then `analytics` is built and assembled and the process exits `Success` on a clean assembly, or `AssemblyFailure` (3) printing **every** problem (not just the first) when assembly fails.
- Given a registry with exactly one flow, when `graph` runs with **no** name, then the sole flow's graph is emitted with no ambiguity error (the single-flow ergonomic default).
- Given `graph etl` then `validate etl` invoked in succession against one registry, when each runs, then the factory for `etl` is invoked **once per verb** (a fresh `RunnableFlow` each time) — proving no consumed flow is reused.
- Given a multi-flow registry, when `graph` is run with **no** name, then it exits `InvalidUsage` (2) with a "name required (etl, analytics)" message; and when `graph unknown` is run, then it exits `InvalidUsage` (2) listing the available flow names.

**Per-verb exit codes**
- Given a flow whose factory builds a flow that **fails assembly**, when `validate <flow>` runs, then the `AssemblyFailure` (3) code is returned directly by the verb — independent of any `RunReport`/`exit_code_for_run` path.
- Given a well-formed flow, when `graph <flow>` runs, then it exits `Success` (0), and a malformed selection (unknown/absent name) never reaches the flow build (it fails at selection with `InvalidUsage`).

**Example & docs**
- Given `crates/cli/examples/multi_flow.rs`, when it is built (`cargo build --example multi_flow`) and invoked as `graph etl` then `graph analytics`, then each emits the corresponding flow's graph artifact.
- Given the same example invoked as `validate etl` and `list`, when each runs, then `validate` reports `etl`'s assembly result and `list` prints both registered names.
- Given the `docs/` usage guide, when a pipeline author reads it, then the `FlowRegistry` + `run_registry` pattern (builder API, contract extension, single-flow ergonomics) is copyable without reading the source.

## Definition of done
- [ ] `graph` and `validate` route correctly for a **named** flow, with the selected factory invoked **once per verb** (a fresh `RunnableFlow` finished into a `Pipeline` each time — never a reused consumed flow).
- [ ] `graph <flow>` emits the C20 graph artifact via `graph_verb`, byte-identical to what a single-flow binary emits for that same flow.
- [ ] `validate <flow>` runs assembly (C7) only, prints every problem, and exits `Success` on a clean assembly or `AssemblyFailure` (3) otherwise.
- [ ] The single-flow ergonomic default applies to `graph`/`validate` (name may be omitted for a one-flow registry); a multi-flow registry with no name and an unknown name each fail with the ADR's helpful `InvalidUsage` (2) message listing the available flows.
- [ ] Each verb's outcome maps to the correct C26 code via its **own** `ExitCode`/error type — `graph`/`validate` do **not** go through `exit_code_for_run` (which is reserved for a completed `RunReport`).
- [ ] `crates/cli/examples/multi_flow.rs` registers two distinct flows (`etl`, `analytics`) and dispatches them through `run_registry`, and `graph etl` / `graph analytics` each emit the corresponding flow's graph.
- [ ] All applicable flow-selecting verbs route through `run_registry` (the reference `crates/cli/src/main.rs` delegates to it rather than reporting "needs a pipeline-specific binary" for these verbs).
- [ ] A `docs/` usage guide explains the `FlowRegistry` builder API, the C26 contract extension (the optional `flow_name` positional), and the single-flow ergonomics — copyable by a pipeline author.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
The ticket declares "None", and `docs/tasks.md` carries no `Q:` items for T75.
Three implementation decisions the ticket left implicit are resolved and recorded
here (per ticket-conventions §5), none of them contested (each has a single
defensible default that the DoD forces):

- **`RunnableFlow` gains a graph seam.** `graph <flow>` must emit the C20 artifact
  (DoD) and `graph_verb`/`validate_verb` need a live `&Pipeline`, but the T74
  `RunnableFlow` only exposed `run(self)` (which consumes the flow and drives it)
  and its `register`/`register_source` are *type-erased* (no stable names, so the
  built pipeline is not graph-emittable — `graph_verb` would return
  `MissingStableNames`). **Resolved:** add `RunnableFlow::into_pipeline(self) ->
  Pipeline` (finish without driving — the crux the ADR names) plus stable-name-aware
  `register_source_named` / `register_named` (registering through the flow's
  `register_source_named` / `register_named`, requiring `T: StableName`). These are
  purely additive over the ADR-081 seam and are what make `graph <flow>`
  byte-identical to a single-flow binary. The reference `main.rs`'s trivial flow and
  `examples/multi_flow.rs` register through the named surface accordingly.

- **`single-node` / `prune` routing shape.** The objective says route "the
  flow-selecting shape of `single-node`/`prune`", but neither has a shipped
  library verb body — the reference sample `dagr-t56-alpha` hand-dispatches them
  with per-invocation store/parameter/replay (C27) plumbing the registry entrypoint
  does not own, and the DoD checkboxes cover only `graph`/`validate`. **Resolved:**
  `run_registry` applies the *same* selection rules to `single-node`/`prune` (so a
  bad/absent/unknown name fails identically to every other verb) and then, on a
  successful selection, prints a diagnostic naming the selected flow and pointing at
  the pipeline-specific verb body, returning `InvalidUsage`. This keeps the surface
  honest without pulling C27 replay / retention plumbing (their owning components)
  into this ticket. The reference `main.rs` still delegates these verbs to
  `run_registry` (satisfying DoD "delegates to it rather than reporting 'needs a
  pipeline-specific binary'").

- **`graph`'s `generated_at` source.** `graph_verb` takes a caller-supplied
  `now_rfc3339` string; the registry has no date-formatting dependency.
  **Resolved:** `run_registry` reads the wall clock **once** and formats a
  second-granularity RFC 3339 UTC stamp via a dependency-free civil-from-days
  helper. Generation time is the only byte-varying field (C20), never
  fingerprint-bound, so second granularity is sufficient and byte-identity
  comparisons mask it.

## Out of scope
- The `FlowRegistry` type + factory contract, the `run_registry` entrypoint, the `Cli.flow_name` positional extraction in `parse_cli`, and the `run`/`list` verbs — T74 owns those; this ticket only routes the remaining flow-selecting verbs and ships the example + docs on top of them.
- Concurrent in-process orchestration of many flows (shared pools, merged event streams, cross-flow cancellation) — a far larger change to the one-run-per-`drive` model, explicitly rejected by ADR 086.
- Sub-DAG composition (a flow reused as a node) — a distinct feature orthogonal to name-based selection, deferred by ADR 086.
- The artifact-only verbs (`render`, `fold`) — unchanged and flow-less; they need no factory and are not re-routed here.
