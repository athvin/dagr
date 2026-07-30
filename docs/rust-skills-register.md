# dagr rust-skills adoption register

> **Status:** enforcement artifact, authored by ticket 107 (T92,
> [`docs/implementation/107-T92-rust-skills-adoption-register.md`](implementation/107-T92-rust-skills-adoption-register.md)).
> A **checked-in, review-owned data file** — never a runtime input. Edited only
> by pull request, reviewed like code. Verified in CI by
> [`scripts/check-rust-skills-adoption.sh`](../scripts/check-rust-skills-adoption.sh).

This file records dagr's disposition of **every rule** in the
[`rust-skills`](../.claude/skills/rust-skills/SKILL.md) skill — 265 rules across
26 categories, written against Rust edition 2024.

## Why this file exists

An audit of dagr against the skill found the codebase already meets most of its
bar: `clippy::all` + `clippy::pedantic` are denied workspace-wide, there are zero
crate-level `#![allow]` escapes, 11 `as` casts in all of production, zero files
missing `//!` docs, no lock held across an `.await`, and determinism carried by a
near-total `BTreeMap` preference. The risk was therefore never under-application
— it was **blind** application.

Several rules are actively wrong for dagr. `err-thiserror-lib` would put a
runtime dependency inside `dagr-core`, whose zero-runtime-dependency guarantee is
an architectural commitment (arch.md "Stability", ADR 081/082). The
`panic = "abort"` half of `perf-release-profile` is *refused at startup* by
`crates/core/src/execution.rs`'s `check_panic_strategy`, because panic
containment needs unwinding. `perf-ahash` would change a hash iteration order
that two CI jobs byte-diff. `perf-io-buffering` would defeat the crash-safety
contract the event sink exists to provide.

Without a record, every future contributor re-derives those conclusions — or,
worse, "fixes" one. This file answers each of them once, and CI keeps it honest.

## The four dispositions

| Disposition | Meaning |
|---|---|
| `satisfied` | dagr already complies. No work. The **Reason** states what makes it true — usually a denied lint that enforces it mechanically. |
| `adopt` | a named M9 ticket applies it. The **Ticket** column names which. |
| `n-a` | structurally inapplicable: the construct does not exist in dagr, or an architectural invariant forbids it. |
| `declined` | applicable, but deliberately not taken. The **Reason** names the trade-off. |

Every row carries a reason — including `satisfied` and `adopt` rows, which the
verifier enforces even though the ticket only required it for `n-a` and
`declined`. An unexplained `satisfied` is exactly the claim the M9 acceptance
gate (T99) has to spot-verify by hand.

Rules are dispositioned against the **post-T94 state** (edition 2024, pinned
newest-stable toolchain), because T94 is a scheduled M9 ticket.

### What the verifier enforces

Every rule id under `.claude/skills/rust-skills/rules/` appears here **exactly
once**; no row names a rule that does not exist; every disposition is one of the
four above; every row states a reason; every `adopt` row names a ticket in
T92–T99. The rule total is derived from the rules directory, never written down
— so a rule added upstream fails the build until it is dispositioned.

---

## 1 · Ownership & Borrowing (`own-`, 12)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| own-arc-shared | own | satisfied | — | `Arc` is the sharing primitive throughout: slot inners, the resource registry, the admission ledger |
| own-borrow-over-clone | own | adopt | T97 | mostly satisfied via denied pedantic lints; T97 removes the two argued `resume.rs` clones |
| own-clone-explicit | own | satisfied | — | every `Clone` on a heap-owning type is explicit; no `Copy` is derived to hide a cost |
| own-copy-small | own | adopt | T96 | T96 added the traits each type's fields freely allow: `LogSpan`/`ScratchStore` gained `PartialEq + Eq + Hash`, `ContainerLimitProbe` `PartialEq` only (an `f64` field), `FoldError` `Copy` as well. `LogSpan` is deliberately NOT `Copy` — `RunId` is `String`-backed, so copying would hide an allocation |
| own-cow-conditional | own | n-a | — | zero `Cow` in the workspace and no conditional-ownership site: values are either borrowed for a read or moved into an attempt |
| own-lifetime-elision | own | satisfied | — | explicit lifetimes appear only where the borrow checker requires them, chiefly the HRTB store callbacks |
| own-move-large | own | satisfied | — | task outputs move into slots; large enum variants are boxed where clippy's perf group requires |
| own-mutex-interior | own | satisfied | — | `std::sync::Mutex` guards the slot ledger, admission ledger, and driver bookkeeping |
| own-rc-single-thread | own | n-a | — | every value crosses a thread boundary, so `Rc` cannot satisfy the `Send` bounds; its only mention is a doc counter-example in `task.rs` |
| own-refcell-interior | own | satisfied | — | the metrics attribution path uses `thread_local!` + `Cell`, the single-threaded interior-mutability form this rule names |
| own-rwlock-readers | own | n-a | — | zero `RwLock`: every lock guards a short bookkeeping mutation, not a read-heavy shared map |
| own-slice-over-vec | own | satisfied | — | zero `&Vec<T>`/`&String`/`&Box<T>` parameters in the workspace; `clippy::ptr_arg` is denied |

## 2 · Error Handling (`err-`, 12)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| err-anyhow-app | err | n-a | — | M9 adds no runtime dependency; the CLI already returns typed errors mapped to a `ExitCode`, which is stronger than an opaque `anyhow` chain |
| err-context-chain | err | adopt | T95 | T95 restored the chain on all six types: `GraphVerbError`, `StructureAssertError`, `OpenError`, `WriteError` override `source()`; `RenderError` and `ReadError` now carry the real `serde_json::Error` instead of a string of it |
| err-custom-type | err | satisfied | — | 29 domain error types implement `std::error::Error`; no `String` or `Box<dyn Error>` is returned as an API error |
| err-doc-errors | err | adopt | T96 | done: `clippy::missing_errors_doc` is `deny` in both `lints.toml` and `[workspace.lints]`, and `scripts/check-lint-parity.sh` fails the build if either drops back to `warn`. It cost nothing — T95's audit had already found zero missing sections |
| err-expect-bugs-only | err | adopt | T95 | T95 classified all of them: the provable-invariant sites are recorded clean below, and the four runtime-builder `expect`s are recorded as an accepted, named bootstrap-panic surface |
| err-from-impl | err | satisfied | — | six `From` impls target error types and are used through `?` at their call sites |
| err-lowercase-msg | err | adopt | T95 | T95 reconciled them as a **deliberate exception**: operator-facing refusal text is terminal prose, not a chain fragment — see "Refusal messages are prose" below |
| err-no-unwrap-prod | err | adopt | T95 | T95 fixed the one genuinely-unjustified production `unwrap` (a `Mutex` lock in the T63 demo) and classified the rest; promoting `clippy::unwrap_used` is **declined** — see "`clippy::unwrap_used` is not promoted" below |
| err-question-mark | err | satisfied | — | `?` is the propagation form throughout; the `map_err` sites convert, they do not branch |
| err-result-over-panic | err | satisfied | — | every recoverable failure is a `Result`; the panic sites are documented framework-invariant violations |
| err-source-chain | err | adopt | T95 | T95 gave all four wrapping types a `source()` override and left `None` where a variant genuinely has no cause; asserted per type by the `error_source_chain` suites |
| err-thiserror-lib | err | n-a | — | `dagr-core` is zero-runtime-dependency by architectural commitment (arch.md "Stability", ADR 081/082); its hand-written types already provide everything `thiserror` generates |

## 3 · Memory Optimization (`mem-`, 17)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| mem-arena-allocator | mem | n-a | — | needs a new runtime dependency, and there is no batch-allocation phase to arena |
| mem-arrayvec | mem | n-a | — | needs a new runtime dependency; forbidden for `dagr-core` and unjustified elsewhere |
| mem-assert-type-size | mem | declined | — | no type's size is load-bearing here; a size assertion would pin a number nobody depends on |
| mem-avoid-format | mem | satisfied | — | `clippy::useless_format` is denied; literals are used where no interpolation is needed |
| mem-box-large-variant | mem | satisfied | — | `clippy::large_enum_variant` is denied via the perf group, so an oversized variant cannot land |
| mem-boxed-slice | mem | declined | — | no fixed-size heap collection on a measured path; `Vec` is the honest type for grow-then-freeze data |
| mem-clone-from | mem | declined | — | no repeated-clone-into-existing-allocation loop exists to reuse; adopting it would be unmeasured churn |
| mem-compact-string | mem | n-a | — | needs a new runtime dependency; node names are few and long-lived |
| mem-drop-order | mem | satisfied | — | `Permit` and `ResidencyLease` release through `Drop` in a deliberately documented order; the slot ledger depends on it |
| mem-reuse-collections | mem | declined | — | unmeasured: the per-run collections are built once, not rebuilt in a loop |
| mem-smaller-integers | mem | satisfied | — | attempt counters are `u32`, byte sizes `u64`, and the pool costs are sized to their domains |
| mem-smallvec | mem | n-a | — | needs a new runtime dependency; `dagr-core`'s zero-dependency guarantee forbids it (ADR 081/082) |
| mem-take-replace | mem | satisfied | — | `mem::take`/`mem::replace` are used to move state out of `&mut` without cloning, notably the buffering sink's drain |
| mem-thinvec | mem | n-a | — | needs a new runtime dependency; no nullable-collection field to shrink |
| mem-with-capacity | mem | adopt | T97 | already used on the measured paths; T97 adds the one site where `pipeline.len()` is known in advance |
| mem-write-over-format | mem | satisfied | — | the canonicalizer and renderers `write!` into an existing buffer rather than building intermediate `String`s |
| mem-zero-copy | mem | satisfied | — | the event sink takes `&[u8]` and the readers borrow their input; records are not copied to be written |

## 4 · Unsafe Code (`unsafe-`, 7)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| unsafe-extern-block | unsafe | n-a | — | the workspace declares no `extern` block; the only FFI is `libc` in a `cfg(unix)` dev-dependency test. T94's edition bump makes `unsafe extern` *available*, but with nothing to apply it to the row stays structurally inapplicable rather than adopted |
| unsafe-maybeuninit | unsafe | n-a | — | no uninitialized memory is constructed anywhere; `mem::uninitialized`/`zeroed` appear nowhere |
| unsafe-minimize-scope | unsafe | satisfied | — | T94 (ticket 109) moved the workspace to edition 2024, where `unsafe_op_in_unsafe_fn` is deny-by-default — the compiler now enforces this rule. Every unsafe operation sits in its own `unsafe { }` block: the four `GlobalAlloc` methods in `crates/core/src/metrics.rs`, the test allocator in `bounded_memory_chain.rs`, the `libc::raise` call, and the `std::env::set_var`/`remove_var` wrappers (unsafe fns as of 2024). `scripts/check-edition-and-msrv-pins.sh` also asserts each block carries its own `// SAFETY:` |
| unsafe-miri-ci | unsafe | adopt | T98 | T98 adds a miri job where it can help, or records why miri cannot exercise a `#[global_allocator]` |
| unsafe-no-mangle-unsafe | unsafe | n-a | — | no `#[no_mangle]`, `#[export_name]`, or `#[link_section]` anywhere in the workspace. T94's edition bump makes `#[unsafe(no_mangle)]` *available* (and the bare form a hard error), but there is no attribute to rewrite |
| unsafe-safety-comment | unsafe | satisfied | — | T94 (ticket 109) added the per-operation `// SAFETY:` comments edition 2024's per-operation blocks require, on top of the block-level comment the production `unsafe impl` already carried, and added the one that was missing at the `libc::raise` site. Kept honest mechanically: `scripts/check-edition-and-msrv-pins.sh` fails the build on an unsafe block with no `// SAFETY:` above it, or on a file with more blocks than comments |
| unsafe-send-sync-manual | unsafe | n-a | — | no manual `Send`/`Sync` impl exists; every auto-derivation is left to the compiler |

## 5 · API Design (`api-`, 17)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| api-builder-must-use | api | satisfied | — | every `-> Self` builder method carries `#[must_use]`; `clippy::must_use_candidate` is denied via pedantic |
| api-builder-pattern | api | satisfied | — | `RunContextBuilder`, `ResourceRegistryBuilder`, `FlowBuilder`, and `NodeBuilder` are the authoring surface |
| api-common-traits | api | adopt | T96 | done: `Handle<T>`, `Slot<T>`, `SlotRef<T>`, `ConsumerLease<T>`, `RedemptionHandle<T>` hand-write `Debug` on the `Permit`/`ResidencyLease` `finish_non_exhaustive()` precedent — unconditional impls, because a derive would emit `impl<T: Debug>` and drop a non-`Debug` output out of every diagnostic. `ReadError` stays underived on purpose: it carries a real `serde_json::Error` (see "`ReadError` trades its derives for its cause" below) |
| api-default-impl | api | satisfied | — | every no-argument `new()` pairs with a `Default`; `clippy::new_without_default` is denied and clean |
| api-extension-trait | api | n-a | — | dagr adds no methods to foreign types; its traits are its own abstractions, not extensions |
| api-from-not-into | api | adopt | T96 | done: `From<CostVector> for PoolCost` plus `TryFrom<&str>` for `GraphArtifact` and `RunArtifact`, each delegating to the inherent constructor it parallels — additive, so no call site changed, and asserted against one shared fixture on both the success and the rejection path. `NodeId::from_name` is a recorded DECLINE, not an oversight (see below) |
| api-impl-asref | api | declined | — | zero `AsRef` impls, and none is wanted: the authoring API takes `impl Into<String>` where flexibility matters and concrete types elsewhere |
| api-impl-fromiterator | api | n-a | — | dagr exposes no collection type; `Pipeline`/`Flow` are assembled through typed registrars, not collected into |
| api-impl-into | api | satisfied | — | `impl Into<String>` is the accepted-input form on ids, node names, and reference metadata |
| api-must-use | api | satisfied | — | 608 `#[must_use]` attributes across the workspace; the denied pedantic group keeps new ones honest |
| api-newtype-safety | api | satisfied | — | `RunId`, `PipelineId`, `NodeId`, `Handle<T>`, `Secret<T>`, and the env-parsing newtypes all prevent value mixing |
| api-non-exhaustive | api | satisfied | — | applied to the 15 deliberately open outcome/error enums; the fixed-cardinality ones (`TaskErrorClass`, `RehydrateClass`) omit it on purpose, since exhaustive matching is their contract |
| api-operator-overload | api | n-a | — | no operator is overloaded, and none of dagr's types has natural arithmetic semantics |
| api-parse-dont-validate | api | satisfied | — | the config layer parses env values into `EnvDuration`/`EnvBool`/`EnvFailureMode` at the boundary rather than validating strings later |
| api-sealed-trait | api | satisfied | — | `BoundInput`, `Deps`, and `StableInputNames` are sealed through private marker traits, closing the implementer set |
| api-serde-optional | api | satisfied | — | `dagr-core` has no serde dependency at all; serialization lives in `dagr-artifact`, which is the crate whose job it is |
| api-typestate | api | satisfied | — | `NodeBinding<S>` encodes the wiring state machine in its type parameter, and the compile-fail corpus pins the illegal transitions |

## 6 · Async/Await (`async-`, 18)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| async-async-fn-bounds | async | declined | — | the callback seams take concrete `Fn` bounds returning a boxed future because they must stay dyn-compatible; `AsyncFn` cannot express that |
| async-bounded-channel | async | adopt | T97 | two unbounded channels exist; T97 either bounds them or proves the admission controller already bounds them structurally |
| async-broadcast-pubsub | async | n-a | — | no pub/sub fan-out: the driver has exactly one consumer of attempt completions |
| async-cancel-safety | async | n-a | — | vacuous here — there is no `select!` anywhere, so no branch can be cancelled mid-poll |
| async-cancellation-token | async | satisfied | — | C16 cancellation uses dagr's own `CancelHandle`/`CancellationSignal` seam, which is the same pattern without the `tokio-util` dependency |
| async-clone-before-await | async | satisfied | — | `Arc` clones happen before the await points; no guard or borrow is carried across a suspension |
| async-fn-in-trait | async | satisfied | — | `Task::run` uses native RPITIT (`-> impl Future + Send`) rather than the `async_trait` macro, which is not a dependency anywhere |
| async-join-parallel | async | n-a | — | concurrency comes from the admission controller spawning attempts, not from joining a fixed set of futures |
| async-joinset-structured | async | declined | — | completion is tracked through a channel plus an in-flight counter, which the driver needs anyway for admission; a `JoinSet` would duplicate that state |
| async-mpsc-queue | async | satisfied | — | the finished-attempt hand-off from spawned attempts back to the single-owner writer is exactly an `mpsc` queue |
| async-no-lock-await | async | adopt | T97 | audited clean today; T97 enables `clippy::await_holding_lock` so it becomes a standing guarantee rather than a review finding |
| async-oneshot-response | async | satisfied | — | the metastore sink's open handshake and per-request reply use rendezvous channels, the request-response shape this names |
| async-select-racing | async | declined | — | deliberately avoided so tokio's `macros` feature stays out of the tree; the timeout path uses a hand-rolled, documented `race` combinator instead |
| async-spawn-blocking | async | adopt | T97 | the execution-class dispatcher already routes blocking work correctly; T97 documents the scratch-store seam where a task author can still block an async worker |
| async-tokio-fs | async | adopt | T97 | `dagr-core` cannot depend on tokio, so the scratch store is synchronous by necessity; T97 documents that at the seam and names `Blocking` as the remedy |
| async-tokio-runtime | async | satisfied | — | the driver builds two separate multi-threaded runtimes by hand so a saturated task pool cannot stall the framework loop (ADR 004) |
| async-try-join | async | n-a | — | no site awaits a fixed set of fallible futures needing early return; failures propagate through the attempt-outcome channel |
| async-watch-latest | async | n-a | — | no latest-value broadcast: cancellation is a one-shot edge, not a changing value observers poll |

## 7 · Concurrency (`conc-`, 4)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| conc-atomic-ordering | conc | satisfied | — | every atomic operation names an explicit `Ordering`; the metrics accounting path documents its choice |
| conc-rayon-par-iter | conc | n-a | — | rayon is used as a capped `ThreadPool` running whole task closures, not for data parallelism; `par_iter` would not express C13's pool bound (ADR 004) |
| conc-scoped-threads | conc | n-a | — | spawned work is `'static` by construction; nothing borrows stack data across a thread boundary |
| conc-thread-local | conc | satisfied | — | the allocator attribution uses `thread_local!` with `Cell`; there is no `static mut` anywhere |

## 8 · Compiler Optimization (`opt-`, 12)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| opt-bounds-check | opt | satisfied | — | iterators and `.get()` are the access forms; the audit found no runtime-variable raw indexing in production |
| opt-cache-friendly | opt | declined | — | unmeasured, and the per-node budget is met with room; reshaping data layout without a profile is what `perf-profile-first` forbids |
| opt-codegen-units | opt | satisfied | — | T93 (ticket 108) set `codegen-units = 1` in `[profile.release]`, replacing Cargo's default 16; `scripts/check-cargo-profiles.sh` fails the build if it drifts back |
| opt-cold-unlikely | opt | declined | — | zero `#[cold]` attributes and no profile showing an error path costs anything |
| opt-inline-always-rare | opt | n-a | — | vacuous: there is no `#[inline(always)]` to over-use, and none is warranted |
| opt-inline-never-cold | opt | declined | — | unmeasured; adding inline hints to error paths without a profile is premature |
| opt-inline-small | opt | declined | — | zero `#[inline]` hints today. With `lto = "fat"` and `codegen-units = 1` (T93) the compiler inlines across crates anyway, so hand hints would be noise |
| opt-likely-hint | opt | n-a | — | the intrinsics are nightly-only and the workspace is pinned to stable |
| opt-lto-release | opt | satisfied | — | T93 (ticket 108) added `[profile.release]` with `lto = "fat"` (fat over thin deliberately: a leaf binary built rarely and run repeatedly, `"thin"` the documented fallback if CI wall-clock binds); pinned by `scripts/check-cargo-profiles.sh` |
| opt-pgo-profile | opt | declined | — | dagr ships portable container builds; a PGO profile gathered on a build host does not transfer, and the workflow cost is real. Restated as out of scope by T93 (ticket 108), which added the profiles without it |
| opt-simd-portable | opt | n-a | — | portable SIMD is nightly-only, and dagr's work is scheduling and I/O, not numeric kernels |
| opt-target-cpu | opt | declined | — | would trade a supported multi-host deployment story for single-digit percentages; arch.md commits to portable Linux containers. Restated as out of scope by T93 (ticket 108), which added the profiles without it |

## 9 · Numeric & Arithmetic Safety (`num-`, 5)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| num-cast-try-from | num | satisfied | — | 13 `as` casts in all of production; the seven widening ones are exact and all six precision-affecting ones carry a reviewed `#[allow]` |
| num-float-compare | num | satisfied | — | zero float `==` comparisons in production; the headroom comparisons use an epsilon |
| num-nonzero | num | satisfied | — | `NonZero` types are used where zero is invalid; pool sizes clamp to at least one |
| num-overflow-explicit | num | adopt | T95 | T95 brought the last outlier onto the pattern: the driver's `in_flight` decrement saturates and asserts its paired invariant, so all 26 counter sites are explicit |
| num-saturating-clamp | num | adopt | T95 | done: `in_flight = in_flight.saturating_sub(1)` — a wrapped counter would have turned `while in_flight > 0` into a non-terminating loop under the release profile |

## 10 · Type Safety (`type-`, 13)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| type-deref-coercion | type | n-a | — | zero `Deref` impls, correctly: none of dagr's types is a smart pointer or a transparent wrapper |
| type-display-vs-debug | type | satisfied | — | `Display` carries operator-facing refusal and error text; `Debug` is diagnostic-only and never swapped for it |
| type-enum-states | type | satisfied | — | terminal states, execution classes, trigger rules, and error classes are all enums, per arch.md's normative vocabulary |
| type-generic-bounds | type | satisfied | — | bounds sit on the impls that need them and use `where` clauses where that reads better; `clippy::type_repetition_in_bounds` is denied |
| type-never-diverge | type | n-a | — | no function diverges; the CLI returns an `ExitCode` rather than calling `process::exit` |
| type-newtype-ids | type | satisfied | — | `RunId`, `PipelineId`, and `NodeId` are newtypes, and `NodeId::from_name` deliberately refuses a `From` impl so identity-minting stays explicit |
| type-newtype-validated | type | satisfied | — | the env newtypes validate in `FromStr`, so an invalid value cannot exist as a parsed type |
| type-no-stringly | type | satisfied | — | verbs, formats, classes, and states are enums; the remaining strings are genuinely open data (node names, URIs) |
| type-numeric-fmt | type | n-a | — | no numeric newtype is formatted as hex, octal, or binary anywhere |
| type-option-nullable | type | satisfied | — | optional fields are `Option<T>`; no sentinel values stand in for absence |
| type-phantom-marker | type | satisfied | — | 23 `PhantomData` uses carry the typestate and handle-type relationships at zero runtime cost |
| type-repr-transparent | type | n-a | — | no newtype crosses an FFI boundary, so the representation guarantee buys nothing |
| type-result-fallible | type | satisfied | — | every fallible operation returns `Result`; the taxonomy is arch.md-normative |

## 11 · Trait & Generics Design (`trait-`, 6)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| trait-associated-type-vs-generic | trait | satisfied | — | `Task::Output` and `DurableOutput::Reference` are associated types (one per impl); `InputReader<Inputs>` is generic because one reader serves many input tuples |
| trait-blanket-impl | trait | satisfied | — | the generic `Task`-to-`NodeRunner` adapter is the blanket impl that gives every task a runner without per-task plumbing (ADR 081) |
| trait-coherence-newtype | trait | satisfied | — | no orphan-rule violation; foreign types are wrapped where a trait must be implemented on them |
| trait-default-methods | trait | satisfied | — | traits are defined in terms of a few required methods; the sinks and observers default the rest |
| trait-dyn-vs-generic | trait | satisfied | — | deliberate split: the hot event-writer path is monomorphized, and only `NodeRunner`/`InputReader` are `dyn` because runners are heterogeneous and stored by node name |
| trait-object-safety | trait | satisfied | — | `NodeRunner` is kept dyn-compatible, which is why its method returns a boxed future rather than `impl Future` |

## 12 · Conversions (`conv-`, 3)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| conv-asmut-mutable | conv | n-a | — | zero `AsMut` impls and no site wanting one; the mutable seams take concrete `&mut` receivers |
| conv-fromstr-parsing | conv | satisfied | — | `FromStr` is implemented for the three env-config newtypes, enabling `str::parse` at the config boundary |
| conv-tryfrom-fallible | conv | adopt | T96 | done: `TryFrom<&str>` on both artifact readers, alongside (never replacing) `from_json_str`, so `.try_into()` and any generic `TryFrom<&str>` bound now reach the published-artifact parse |

## 13 · Const & Compile-Time (`const-`, 4)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| const-block | const | satisfied | — | the fingerprint algorithm version is pinned by a `const _: () = assert!(...)`, a compile-time failure rather than a runtime one |
| const-fn | const | satisfied | — | the small pure constructors and accessors that can be `const fn` are |
| const-generics | const | n-a | — | no value-parameterized type; input arity is bounded by a `const` and expanded by macro-generated tuple impls |
| const-vs-static | const | satisfied | — | inlined values are `const`; the addressed singletons (the global allocator, the pinned toolchain probes) are `static` |

## 14 · Serde (`serde-`, 8)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| serde-custom-with | serde | n-a | — | no field needs custom (de)serialization: the writers build `serde_json::Value`s directly through the T4 canonicalizer |
| serde-default-compat | serde | satisfied | — | `#[serde(default)]` is applied on the reader structs, which is what makes additive schema evolution work (arch.md "Stability") |
| serde-deny-unknown-fields | serde | n-a | — | **contrary to dagr's schema policy**: arch.md requires readers to *ignore* unknown fields so evolution stays additive-only; denying them would break forward compatibility by design |
| serde-enum-representation | serde | satisfied | — | the event stream's tagging is fixed by the published JSON Schema and pinned by the fixture corpus, so the representation is a schema decision already made |
| serde-flatten | serde | declined | — | the artifact shapes are schema-versioned and validated field-by-field; flattening would obscure the mapping between struct and schema |
| serde-rename-all | serde | satisfied | — | the wire names are fixed by the published schemas and asserted byte-for-byte by the determinism jobs |
| serde-skip-empty | serde | declined | — | omitting empty fields would change emitted artifact bytes, which two CI jobs byte-diff; stable presence is worth more than a few bytes |
| serde-try-from-validate | serde | declined | — | validation happens in the schema-validation helper against the published JSON Schema, which is a stronger and externally-checkable gate than a deserialize hook |

## 15 · Pattern Matching (`pat-`, 5)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| pat-at-bindings | pat | declined | — | no match site currently needs to bind and test the same value; forcing `@` in would not improve any existing arm |
| pat-exhaustive-enum | pat | satisfied | — | owned enums are matched exhaustively; `clippy::wildcard_enum_match_arm` pressure plus the `#[non_exhaustive]` discipline keeps new variants visible |
| pat-if-let-chains | pat | declined | — | **now reachable**: T94 (ticket 109) landed edition 2024, so let-chains compile. Still not adopted — T94's Out of scope defers the call-site sweep to ordinary future work rather than folding a 25-site refactor into a migration. The work list is recorded as a lint decision, not prose: `clippy::collapsible_if` is `allow` in the lint policy precisely because its edition-2024 fix *is* a let-chain, so re-denying that one lint enumerates every site an adopting ticket must visit |
| pat-let-else | pat | satisfied | — | `let ... else` is the early-return extraction form throughout the driver and planner |
| pat-matches-macro | pat | satisfied | — | 29 `matches!` uses for boolean pattern tests |

## 16 · Macros (`macro-`, 8)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| macro-export-crate-path | macro | n-a | — | the authoring macros are proc-macros re-exported from the facade crates; there is no `#[macro_export]` declarative macro in the public API |
| macro-fragment-specifiers | macro | satisfied | — | the internal declarative macros capture with precise specifiers rather than raw `:tt` |
| macro-prefer-functions | macro | satisfied | — | `#[task]`/`#[dag]` exist because a function cannot generate a trait impl or a link-time registration; the tuple-arity impls likewise cannot be written generically |
| macro-private-helpers | macro | satisfied | — | generated code targets existing public items and the sealed marker traits, so no helper surface leaks into the docs |
| macro-proc-error-spans | macro | adopt | T95 | T95 replaced the one internal `unreachable!` in `take_run` with a spanned `syn::Error`, so no path in the proc macro panics inside the compiler |
| macro-proc-syn-quote | macro | satisfied | — | `dagr-macros` is built on `syn`, `quote`, and `proc-macro2`, all build-time only |
| macro-proc-two-crate | macro | satisfied | — | `dagr-macros` is a dedicated `proc-macro = true` crate re-exported through `dagr-core` and `dagr-cli` behind features (ADR 082) |
| macro-rules-hygiene | macro | satisfied | — | the declarative macros rely on hygiene; the proc-macro expansions use fully-qualified `::dagr_core::` / `::inventory::` paths |

## 17 · Closures (`closure-`, 5)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| closure-disjoint-capture | closure | satisfied | — | disjoint closure capture (edition 2021 onward) is in force, and T94's bump to edition 2024 preserved it — verified by the suite passing unchanged; captures are minimal at the spawn sites |
| closure-fn-trait-bounds | closure | satisfied | — | callbacks take the least restrictive bound they need; the completion callback is `FnOnce` where it is consumed once |
| closure-impl-fn-return | closure | n-a | — | no function returns a closure; the seams return futures or concrete types |
| closure-move-capture | closure | satisfied | — | spawned attempts `move` their captures and clone `Arc`s beforehand, which is what makes them `'static` |
| closure-static-vs-dyn | closure | satisfied | — | hot callbacks are generic; the stored ones are boxed because they live in a map keyed by node name |

## 18 · Collections (`coll-`, 4)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| coll-binaryheap | coll | n-a | — | zero `BinaryHeap`: readiness is a set-membership question, not a priority-queue one, and admission order is deliberately deterministic |
| coll-map-choice | coll | satisfied | — | `BTreeMap` is the default precisely because iteration order reaches emitted artifacts; the eight `HashMap`s are `TypeId`/`NodeId` lookup tables never iterated |
| coll-seq-choice | coll | satisfied | — | `Vec` by default, `VecDeque` where queue behaviour is wanted, zero `LinkedList` |
| coll-set-membership | coll | satisfied | — | `BTreeSet` carries the membership and dedup work; the two remaining `Vec::contains` loops are bounded by `MAX_INPUT_ARITY = 8` and per-pipeline resource count |

## 19 · Naming Conventions (`name-`, 16)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| name-acronym-word | name | satisfied | — | zero violations: no public type name carries two consecutive capitals; `clippy::upper_case_acronyms` is denied |
| name-as-free | name | satisfied | — | the four `as_*` methods take `self` on `Copy` enums, which is the zero-cost idiomatic form |
| name-consts-screaming | name | satisfied | — | `clippy::style` is denied, so a non-screaming const cannot land |
| name-crate-no-rs | name | satisfied | — | the six crates are `dagr-*`; none carries an `-rs` or `-rust` suffix |
| name-funcs-snake | name | satisfied | — | enforced mechanically by the denied `clippy::style` group and rustc's own naming lints |
| name-into-ownership | name | satisfied | — | `into_*` is used only where `self` is consumed; the audit found no misuse |
| name-is-has-bool | name | satisfied | — | the predicates use `is_`/`has_`; the handful of passive-voice names (`implies_success`, `all_pools_full`) read unambiguously as booleans, matching stdlib precedent like `Iterator::all` |
| name-iter-convention | name | satisfied | — | the iterator methods are `iter()` or domain-named in the `HashMap::keys()` tradition; no misnamed variant exists |
| name-iter-method | name | satisfied | — | `iter()` is used consistently; no `iter_mut`/`into_iter` is needed on the current surface |
| name-iter-type-match | name | n-a | — | no named iterator type is exposed; the iterator methods return `impl Iterator` |
| name-lifetime-short | name | satisfied | — | lifetimes are `'a`/`'b` and the conventional `'de`-style short forms |
| name-no-get-prefix | name | adopt | T96 | done: the four became `recorded_content_hash` / `recorded_size_bytes` / `recorded_scheme` / `recorded_produced_at_offset_ns`. The collision with the consuming builder setters is real and unavoidable, so the getters take a qualifier — the same resolution `NodePolicy` (`is_durable`, `retry_count`, `backoff_shape`) and `PoolCost` (`working_memory_bytes`) already use. Zero `get_`-prefixed accessors remain |
| name-to-expensive | name | satisfied | — | `to_*` marks the allocating conversions; the cheap ones are `as_*` |
| name-type-param-single | name | satisfied | — | type parameters are `T`, `E`, `S`, `C`, `V`, and the descriptive `Inputs` where a tuple is meant |
| name-types-camel | name | satisfied | — | enforced mechanically by rustc's `non_camel_case_types` under `warnings = "deny"` |
| name-variants-camel | name | satisfied | — | same mechanical enforcement; no variant deviates |

## 20 · Testing (`test-`, 15)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| test-arrange-act-assert | test | satisfied | — | the suite is written given/when/then from each ticket's Test plan, which is the same structure |
| test-cfg-test-module | test | satisfied | — | unit tests sit in `#[cfg(test)] mod tests`; the bulk are integration tests, which is the right level for a run engine |
| test-criterion-bench | test | declined | — | the 1000-node per-node budget is a deterministic `cargo test` asserting a wall-clock ceiling; criterion's statistical sampling would add a dependency and a flakier signal for a regression gate |
| test-descriptive-names | test | satisfied | — | across ~1060 tests the shortest names are `ui` and `determinism`; there is no `test1`/`works`/`smoke` anywhere |
| test-doctest-examples | test | adopt | T96 | done: executed `dagr-cli` doctests went 1 → 6 of 9, `ignore`d 2 → 0, and zero `ignore` fences remain anywhere in the workspace. The three that stay `no_run` state the reason at the fence (two are `fn main` one-liners; one writes a golden fixture). `dagr-metastore` gained its first example |
| test-fixture-raii | test | adopt | T98 | `dagr-core`'s scratch tests already use a unique-temp-dir helper; T98 promotes it so `dagr-cli`'s tests stop sharing literal `/tmp` paths |
| test-integration-dir | test | satisfied | — | 117 integration test files across the six crates' `tests/` directories |
| test-loom-concurrency | test | declined | — | a genuine fit for the hand-rolled admission controller, but adopting it means porting the type to loom's primitives under `cfg(loom)` — a design change, not hardening. Recommended as its own follow-up |
| test-mock-traits | test | satisfied | — | every injected dependency is a trait (`EventSink`, `MonotonicClock`, `Jitter`, `ZombieObserver`), which is what makes the fakes possible |
| test-mockall-mocking | test | declined | — | hand-written fakes are already in place and are deterministic by construction; a mocking framework would add a dependency to replace working code |
| test-proptest-properties | test | declined | — | the termination property uses a **seeded deterministic** generator on purpose. A random-search harness would make failures non-reproducible, which contradicts the determinism guarantee the suite exists to protect |
| test-should-panic | test | declined | — | zero uses, deliberately: the panic paths are asserted through the real containment boundary, which checks the recorded outcome rather than just that a panic happened |
| test-snapshot-testing | test | declined | — | already done by hand: checked-in golden `.dot`/`.mmd`/`.json` fixtures plus a `bless` regenerate helper, functionally equivalent to `insta` and dependency-free |
| test-tokio-async | test | satisfied | — | `#[tokio::test]` is used in `dagr-metastore`; the other crates build runtimes by hand so tokio's `macros` feature stays out of the tree |
| test-use-super | test | satisfied | — | test modules import their parent with `use super::*` |

## 21 · Documentation (`doc-`, 12)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| doc-all-public | doc | adopt | T96 | done: `missing_docs` is an explicit `deny` in both files. A DECLARATIVE ratchet, not a bug fix — `warnings = "deny"` was already promoting it, and no suppression was added to keep clippy green |
| doc-cargo-metadata | doc | adopt | T96 | done: all six declare `description`, `documentation`, `readme`, `keywords`, `categories`, and every intra-workspace path dependency carries a version requirement (without which `cargo package` refuses the manifest). `scripts/check-crate-docs-and-metadata.sh` holds it for every future member |
| doc-crate-readme | doc | adopt | T96 | done: six `README.md`s, each inlined with `#![doc = include_str!("../README.md")]` and reconciled with the existing `//!` header rather than duplicating it — the README carries the crates.io orientation, the header keeps the module index |
| doc-errors-section | doc | adopt | T96 | done, together with `err-doc-errors`: the lint is `deny`, and `docs/lint-policy.md`'s deferral to T3 ("revisited then" — T3 shipped long ago) is rewritten to the current reason |
| doc-examples-section | doc | adopt | T96 | PARTIALLY adopted, deliberately: T96 covered the primary public entry points (`FlowBuilder` wiring, `FlowRegistry`/`run_registry` dispatch, `DagRegistration`, `MetaStore::open`) rather than sweeping every public item, because arch.md's "runnable examples covering each layer" is already served by the nine programs in `crates/cli/examples/`. A blanket sweep is not scheduled |
| doc-hidden-setup | doc | satisfied | — | the doc examples use `#`-hidden setup lines to keep the visible snippet to the point |
| doc-intra-links | doc | satisfied | — | intra-doc links are used throughout and `rustdoc::broken_intra_doc_links` is denied in CI |
| doc-link-types | doc | satisfied | — | same enforcement: a broken type link fails the rustdoc job |
| doc-module-inner | doc | satisfied | — | every one of the production `.rs` files opens with a `//!` module doc; zero exceptions |
| doc-panics-section | doc | adopt | T95 | verified by T95: clippy reports none missing and the slot state-machine's panicking readers (`read`, `clone_value`, `ConsumerLease::read`/`take`) each already carry a `# Panics` section naming the framework defect |
| doc-question-mark | doc | adopt | T96 | done: all five `.unwrap()`s in doc examples became `?` (four executed, one `no_run`), and the metadata check fails the build on a new one — an example is read as a template, so it models the propagation a caller should copy |
| doc-safety-section | doc | satisfied | — | the one production `unsafe impl` carries its safety argument, and `unsafe_code` is surfaced for review by lint |

## 22 · Observability (`obs-`, 7)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| obs-error-chain | obs | adopt | T95 | unblocked by T95: `source()` now returns the cause on every wrapping type, so a chain-walking logger reaches the emitter/libSQL/deserializer diagnostic instead of the wrapper's summary |
| obs-instrument-spans | obs | satisfied | — | every attempt runs beneath a span carrying run/node/attempt identity, so any line is attributable without correlating timestamps (C25) |
| obs-levels-filter | obs | satisfied | — | levels are used meaningfully and an env var selects the output mode; `env-filter` is deliberately omitted so `regex`/`matchers` stay out of the tree |
| obs-library-facade | obs | satisfied | — | `dagr-core` emits nothing and installs no subscriber — it exposes only a dependency-free `LogSpan` payload; the subscriber install lives in the binary crate |
| obs-no-sensitive-data | obs | satisfied | — | `Secret<T>` has no `Debug` that reveals its value, and the compile-fail corpus pins that it cannot be printed |
| obs-structured-fields | obs | satisfied | — | run/node/attempt are recorded as discrete fields in the JSON layer, not interpolated into the message |
| obs-tracing-over-log | obs | satisfied | — | `tracing` is the C25 integration in `dagr-cli`; `dagr-core` stays dependency-free, which is why the span payload is passed rather than emitted |

## 23 · Performance Patterns (`perf-`, 13)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| perf-ahash | perf | n-a | — | needs a new runtime dependency, and changing the hasher changes iteration order — which two CI jobs byte-diff. DoS resistance is irrelevant here but determinism is not |
| perf-black-box-bench | perf | n-a | — | there is no micro-benchmark to protect from dead-code elimination; the scale gate measures a real 1000-node run end to end |
| perf-chain-avoid | perf | satisfied | — | no `chain` sits in a hot loop |
| perf-collect-into | perf | declined | — | unmeasured, and the container-reuse APIs it names are unstable |
| perf-collect-once | perf | satisfied | — | the audit found no collect-then-reiterate waste anywhere in the workspace |
| perf-drain-reuse | perf | satisfied | — | the buffering sink drains rather than reallocating between attempts |
| perf-entry-api | perf | satisfied | — | the insert-or-update sites use the entry API; `clippy::map_entry` is denied |
| perf-extend-batch | perf | satisfied | — | batch insertions use `extend` rather than per-item pushes in a loop |
| perf-io-buffering | perf | n-a | — | **contrary to the crash-safety contract**: `FileSink` writes and flushes each record line so a crash cannot lose a buffered event (C19). A `BufWriter` would trade the guarantee the sink exists to provide for syscall count |
| perf-iter-lazy | perf | satisfied | — | iterators stay lazy to the point of use; `clippy::needless_collect` is denied |
| perf-iter-over-index | perf | satisfied | — | iteration is the access form; the audit found no manual indexing in production |
| perf-profile-first | perf | adopt | T97 | this rule *governs* T97: every allocation change must carry a before/after measurement, and unmeasured sites are left alone |
| perf-release-profile | perf | satisfied | — | T93 (ticket 108) added `[profile.release]` (opt-level 3, fat LTO, one codegen unit), `[profile.bench]`, and `[profile.dev.package."*"]`. Adopted **except two knowing deviations**, both asserted mechanically by `scripts/check-cargo-profiles.sh`: `panic = "abort"` is REFUSED (`execution::check_panic_strategy` will not start a run under it — containment needs unwinding — so `panic = "unwind"` is set explicitly instead), and `strip` stays off (it would remove the symbols the panic hook needs to attribute a panic to its node) |

## 24 · Project Structure (`proj-`, 14)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| proj-bin-dir | proj | satisfied | — | the extra binaries live under `src/bin/`, with the test-support ones gated behind `required-features` |
| proj-build-rs-minimal | proj | n-a | — | no crate has a `build.rs`; build provenance is passed in rather than generated |
| proj-feature-additive | proj | adopt | T98 | features are additive by design, but no CI job builds the matrix; T98 adds `--no-default-features` and `--all-features` legs |
| proj-flat-small | proj | satisfied | — | every crate is flat `foo.rs` files in `src/` with no nested module directories |
| proj-lib-main-split | proj | satisfied | — | both binaries are thin (161 and 96 lines) and delegate to their library |
| proj-mod-by-feature | proj | declined | — | modules already follow arch.md's C-numbered components. Splitting the ten files over 1200 lines is a structure refactor, not hardening, and is excluded from M9's scope; recommended as its own follow-up |
| proj-mod-rs-dir | proj | n-a | — | no multi-file module exists to need a `mod.rs`; the flat layout is deliberate |
| proj-msrv-declare | proj | satisfied | — | `rust-version` is declared at the workspace level and CI pins the toolchain; T94 (ticket 109) moved both to 1.97.1 and added the MSRV-aware `resolver = "3"` explicitly (rather than inheriting it from edition 2024), so a transitive dependency upgrade cannot raise the declared minimum unnoticed. `scripts/check-edition-and-msrv-pins.sh` asserts all six sites that name the pin agree |
| proj-prelude-module | proj | satisfied | — | `dagr_cli::prelude` exists so a task author needs one glob import |
| proj-pub-crate-internal | proj | satisfied | — | internal helpers are `pub(crate)`; `clippy::redundant_pub_crate` is denied |
| proj-pub-super-parent | proj | satisfied | — | used where a helper belongs to the parent module only |
| proj-pub-use-reexport | proj | satisfied | — | each crate root re-exports its public surface, so consumers never name a private module path |
| proj-workspace-deps | proj | satisfied | — | shared package metadata is inherited via `[workspace.package]` and `*.workspace = true` in all six members |
| proj-workspace-large | proj | satisfied | — | the six-crate split is what makes the C24 renderer boundary a missing crate-graph edge rather than a convention |

## 25 · Clippy & Linting (`lint-`, 13)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| lint-cargo-metadata | lint | adopt | T96 | the metadata half is done (see `doc-cargo-metadata`), enforced by a dedicated shell check rather than by enabling the `clippy::cargo` GROUP — see "`clippy::cargo` is not enabled as a group" below for why the group is declined and what replaces it |
| lint-cfg-check | lint | adopt | T98 | no `check-cfg` declaration exists. Harmless today (no custom cfgs) but unguarded against a future feature-name typo compiling into dead code |
| lint-clippy-nursery-selected | lint | declined | — | the nursery lints that would matter here are already covered by the denied `pedantic` group; enabling more selectively is churn without a named defect to catch |
| lint-deny-correctness | lint | satisfied | — | `clippy::all` is denied at group level, which subsumes `correctness` |
| lint-missing-docs | lint | adopt | T96 | done: `deny` in both files, the T1-era deferral note retired, and `scripts/check-lint-parity.sh` asserts the two files agree field for field so the pair cannot drift |
| lint-pedantic-selective | lint | satisfied | — | `pedantic` is denied wholesale with exactly two documented exceptions, each justified in `docs/lint-policy.md`'s table — `module_name_repetitions` (the C-numbered module names read fine repeated) and `collapsible_if` (added by T94: its edition-2024 fix *is* a let-chain, and adopting let-chains at call sites is deferred, so denying it would demand a change this milestone declined). Still stricter than selective adoption |
| lint-rustfmt-check | lint | satisfied | — | `cargo fmt --all --check` runs as its own CI job |
| lint-unsafe-doc | lint | adopt | T95 | T95 enabled `clippy::undocumented_unsafe_blocks` workspace-wide; it cost exactly one new `// SAFETY:` (on the `unsafe impl GlobalAlloc`), because T94 had already commented every block |
| lint-warn-complexity | lint | satisfied | — | denied, not warned, via the `clippy::all` group |
| lint-warn-perf | lint | satisfied | — | denied via the `clippy::all` group |
| lint-warn-style | lint | satisfied | — | denied via the `clippy::all` group |
| lint-warn-suspicious | lint | satisfied | — | denied via the `clippy::all` group |
| lint-workspace-lints | lint | satisfied | — | the policy lives in `[workspace.lints]` with `[lints] workspace = true` in all six members, plus `lints.toml` and `docs/lint-policy.md` as its documented source of truth |

## 26 · Anti-patterns (`anti-`, 15)

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| anti-clone-excessive | anti | adopt | T97 | T97 removes the per-append full-buffer clone in the metastore live sink — an O(n) copy per event, quadratic over a run |
| anti-collect-intermediate | anti | satisfied | — | no intermediate collect exists to remove; `clippy::needless_collect` is denied |
| anti-empty-catch | anti | adopt | T95 | T95 stated the write-discard convention once in each of `registry.rs` and `contract.rs` ("operator-facing output is a courtesy, never a result") rather than 25 times |
| anti-expect-lazy | anti | adopt | T95 | T95 classified every production `expect`; each `Mutex` site now also states its poisoning policy and the reason, so no `expect` is left as a shrug |
| anti-format-hot-path | anti | satisfied | — | no `format!` sits in a measured hot loop; the per-row SQL builders are bounded by pipeline size and run once per event |
| anti-index-over-iter | anti | satisfied | — | the only indexing is macro-generated compile-time-constant tuple access, not runtime indexing |
| anti-lock-across-await | anti | adopt | T97 | audited clean; T97 enables `clippy::await_holding_lock` so it stays clean mechanically rather than by review |
| anti-over-abstraction | anti | satisfied | — | the generic surface is driven by the typed-handle guarantees; dynamic dispatch is used exactly where types are heterogeneous |
| anti-panic-expected | anti | adopt | T95 | resolved: the **code** was wrong, not the comment — a rejected `slot.fill` is now `AttemptOutcome::PermanentFailure`, not a `Succeeded` over a discarded value |
| anti-premature-optimize | anti | adopt | T97 | this rule *constrains* T97: no allocation change lands without a measurement, and the unmeasured candidates are recorded as declined |
| anti-string-for-str | anti | satisfied | — | zero `&String` parameters; `clippy::ptr_arg` is denied |
| anti-stringly-typed | anti | satisfied | — | enums and newtypes carry the closed vocabularies; strings remain only for genuinely open data |
| anti-type-erasure | anti | satisfied | — | boxing appears only where a trait must stay dyn-compatible or a future must be stored across polls, each documented at the site |
| anti-unwrap-abuse | anti | adopt | T95 | T95 fixed the unjustified production `unwrap` and documented the remainder; the lint that would "hold the line" is declined with reasons below |
| anti-vec-for-slice | anti | satisfied | — | zero `&Vec<T>` parameters; `clippy::ptr_arg` is denied |

---

## Decisions recorded by T95 (error chains, panics, arithmetic)

Ticket 110 (T95,
[`docs/implementation/110-T95-error-chain-and-panic-hardening.md`](implementation/110-T95-error-chain-and-panic-hardening.md))
audited every `impl Error` block, every production `unwrap`/`expect`, every `as`
cast, and every `let _ =` discard. Four defects were fixed; the rest is recorded
here, because a finding that is only ever "checked once" is a finding that rots.

### The mutex-poisoning rule

The workspace ran **two** poisoning philosophies with nothing to choose between
them: `core::slot` and `cli::signals` recovered
(`unwrap_or_else(PoisonError::into_inner)`), while `core::admission`, `cli::driver`
and the in-memory sinks panicked. Both are defensible; having both, undocumented,
is not. T95 reconciled them under one rule, stated at every lock site:

> **Recover** where user-or-defect code can panic *while the lock is held*;
> **panic** otherwise.

That rule reproduces both existing behaviours, and it explains them. `Slot`'s lock
is the one lock in the workspace that a *documented, expected* panic poisons —
`read_arc` panics loudly on read-before-fill while holding it — so a panicking
policy there would convert one node's defect into a wedged run for every other
node, hiding the original diagnostic behind an unrelated one. `SignalRouter` fires
the cancel handle under its lock, so a panic beneath `fire()` must not cost the
operator the ability to cancel a later run. Every other lock in production guards a
short bookkeeping mutation with no task body, no user callback and no defect
assertion under it, so a poisoned one can only mean the framework panicked inside
its own critical section and left the state half-written — and continuing on that
would corrupt the run record the state exists to produce.

### Refusal messages are prose, and stay prose

`err-lowercase-msg` wants terse, lowercase, punctuation-free fragments, because an
error message is usually a *chain fragment* that gets composed into a longer
sentence by its wrapper. `ResumeRefusal`'s five variants and `BootstrapRefusal` are
not that. They are **terminal operator-facing text**: multi-sentence explanations
that name the offending node, both fingerprints, and the action to take, printed
directly to a human who is deciding what to do next. They already satisfy the
rule's lowercase-initial half; what they do not satisfy is single-fragment brevity,
and they should not — a refusal that fits the convention would have to stop
explaining itself. Several are pinned verbatim by tests, which is the correct
treatment for text that is part of the operator contract. Recorded as a
**deliberate exception**, not an oversight; the convention continues to bind every
non-refusal message.

### The accepted bootstrap-panic surface

Four `.expect()`s build a runtime and are genuinely fallible — OS thread
exhaustion under a tight container `ulimit` is a real scenario:
`crates/cli/src/dispatch.rs` (the task and blocking runtimes) and
`crates/cli/src/driver.rs` (the framework runtime and the teardown-phase runtime).
They are **deliberately fatal**: a run that cannot build its execution surfaces has
not started, so there is nothing to record and no partial state to explain. Routing
them into `BootstrapFailure` would be more honest about the *exit code*, and is a
larger change than this ticket's scope. Recorded as a **known, accepted panic
surface** with a named revisit, rather than left as an unexamined `expect`.

### `clippy::unwrap_used` is not promoted

The obvious mechanical follow-through to the unwrap audit would be to deny
`clippy::unwrap_used` in production. T95 declines it. The audit found 31 of 33
sites provably infallible with their invariant already documented at the call
(a `char::from_digit` on a value masked to 4 bits; a `get_mut` on a key inserted
two lines above), and the lint cannot tell those from a genuine risk. Denying it
would replace 31 documented invariants with 31 `#[expect]` attributes — strictly
less information, and a review signal that has been trained to mean nothing. The
one site that was *not* justified (a `Mutex` lock in the T63 demo) was fixed. The
line is held by the audit and by this record, not by a lint that would cry wolf.

### Recorded as clean, not changed

Verified during the audit and now asserted mechanically by
`crates/cli/tests/error_chain_and_panic_hardening.rs`, so the claims cannot go
quietly stale:

- **zero** `todo!` / `unimplemented!` anywhere in production;
- **zero** float `==` comparisons;
- **zero** missing `# Errors` / `# Panics` sections (clippy's `missing_errors_doc`
  and `missing_panics_doc` both report none);
- every production `Mutex` lock site states its poisoning policy and reason;
- every `#[expect(clippy::cast_*)]` carries a `reason` (T96 converted these from
  `#[allow]`; see below).

---

## Decisions T96 recorded

### `clippy::cargo` is not enabled as a group

`lint-cargo-metadata` asks for `[lints.clippy] cargo = "warn"`. T96 supplied what
that group exists to enforce — every member now declares the full crates.io
metadata set — but enabled the group **no**, for two reasons, and replaced it with
something stricter for the part that matters:

- `multiple_crate_versions` is the loudest lint in the group and is about
  *transitive* dependency resolution, not about dagr's own manifests. It fires on
  duplicates a workspace does not control (libSQL's build toolchain alone brings
  several), so under `warnings = "deny"` it would be a build failure caused by
  somebody else's version bump.
- `cargo_common_metadata` only *warns* about missing fields.
  `scripts/check-crate-docs-and-metadata.sh` **fails** on them, checks fields the
  lint does not (`readme` pointing at a file that actually ships inside the
  `.crate`, the `include_str!` unification, the version requirement on every
  intra-workspace path dependency), and asserts the publish graph is closed —
  which no lint does. It also proves its own scans non-vacuous against a fixture.

If a future ticket wants the group, the honest form is `cargo = "warn"` with
`multiple_crate_versions = "allow"` and a stated reason — not a bare group deny.

### `ReadError` trades its derives for its cause

T96's brief listed `ReadError` among the plain-data types that should gain the
traits their fields freely allow. T95 landed first and made it carry the real
`serde_json::Error` rather than a string copy of it, so its fields now freely
allow **nothing**: that type implements no `Clone`, `PartialEq`, `Eq`, or `Hash`.
This is the same structural split the brief itself names for `Box<dyn Error>`- and
`io::Error`-carrying types, arrived at from the other direction, and it is the
right trade — the line and column an operator needs to fix a truncated artifact
are worth more than a derive. `RenderError` is in the identical position for the
identical reason. `crates/artifact/tests/api_common_traits.rs` pins what
`ReadError` *does* offer, so a future change that drops the carried cause to
regain a derive is visible rather than silent.

### `NodeId::from_name` declines its `From` impl, correctly

`conv-fromstr-parsing` / `api-from-not-into` would nominally want
`impl From<&str> for NodeId` beside `NodeId::from_name`. It is deliberately absent
and stays absent: identity minting must not read as an implicit drive-by
conversion. `from_name` is the single registration seam where a node's identity is
derived from its author-declared name, and a `.into()` at a call site would make
that look incidental. The function's own doc comment already argues this; recorded
here so it reads as a decision rather than an oversight.

---

## Follow-ups recorded here, deliberately outside M9

Three declines are worth revisiting on their own merits, and are recorded so the
reasoning is not lost:

- **`proj-mod-by-feature`** — ten files exceed 1200 lines (`driver.rs` at 2776 is
  the largest). Splitting them would make review easier, but it is an API and
  structure refactor, which M9 explicitly excludes.
- **`test-loom-concurrency`** — the hand-rolled `AdmissionController` is exactly
  what loom exists to model-check; adopting it needs a `cfg(loom)` port of the
  type's primitives.
- **`opt-inline-small` / `opt-cache-friendly` / `opt-pgo-profile`** — all
  unmeasured. dagr meets its per-node budget with room, and `perf-profile-first`
  is itself a rule in this skill.
