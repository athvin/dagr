# 110 · T95 — error-chain, panic, and arithmetic hardening

> **Milestone:** M9 · **Size:** M · **Type:** feature · **Components:** C15, system-level
> **Branch:** `feat/t95-error-chain-and-panic-hardening` · **Depends on:** T94 · **Blocks:** T96, T97

## Why / context

An audit of all 29 `impl Error` blocks, all 33 production `unwrap`/`expect`
sites, all 13 production `as` casts, and every `let _ =` discard found the error
surface in good shape overall — and four specific defects worth fixing.

The most consequential is a **broken causal chain**. `err-source-chain` says a
wrapping error must expose its cause through `Error::source()`. Four types wrap a
genuine underlying error and then leave `impl Error for X {}` empty, so
`source()` returns `None` and the cause is invisible to any caller that walks the
chain:

- `GraphVerbError` (`crates/cli/src/graph.rs:298`) — wraps `GraphEmitError` / `io::Error`
- `StructureAssertError` (`crates/cli/src/structure_snapshot.rs:343`) — wraps `io::Error` / `GraphEmitError`
- `OpenError` (`crates/metastore/src/store.rs:59`) — wraps `libsql::Error`
- `WriteError` (`crates/metastore/src/store.rs:95`) — wraps `libsql::Error`

Two more discard the cause *before* construction, so it cannot be recovered even
in principle: `RenderError::Malformed` stringifies its `serde_json::Error` via
`.map_err(|e| RenderError::Malformed(e.to_string()))` (`crates/render/src/lib.rs:106`),
and `ReadError` keeps only a line index (`crates/artifact/src/event_stream.rs:1293`).
This matters here more than in a typical crate: dagr's whole pitch is explaining a
run after the fact, and a truncated chain is exactly the diagnostic that forces an
operator back into the logs.

The audit's other three findings are below. Everything else it checked came back
clean and is recorded, not changed: **zero** `todo!`/`unimplemented!` anywhere;
**zero** float `==` comparisons; **zero** missing `# Errors`/`# Panics` sections
(clippy confirms); 31 of the 33 production `unwrap`/`expect` sites are provably
infallible and already carry documented invariants.

## Objective

Fix the four defects; record the rest.

- **Restore the causal chains.** Override `source()` on the four wrapping types.
  For `RenderError` and `ReadError`, carry the real error instead of its string
  (both are on read paths where the underlying `serde_json::Error` carries the
  line/column an operator needs). Where a variant genuinely has no cause, the
  default `None` stays correct.
- **Reconcile the two mutex-poisoning philosophies.** `core::slot` and
  `cli::signals` deliberately *recover* from poisoning
  (`unwrap_or_else(PoisonError::into_inner)`), each with a comment explaining why
  a panicking consumer must not wedge the machinery. `core::admission`,
  `cli::driver`, and `cli::scale_bench` instead `.expect("… not poisoned")` at 14
  sites. Both policies are defensible; having both, undocumented, is not. Pick
  per lock — recover where a poisoned lock must not escalate, panic where it
  signals an invariant already violated — and state the reason at each site.
- **Fix the arithmetic outlier.** `crates/cli/src/driver.rs:1574` decrements the
  `in_flight` counter with a bare `-= 1`, mutated from five call sites across
  async control flow, while the codebase's 25 other counter sites use
  `saturating_*` (`crates/core/src/slot.rs:668` is the near-identical
  in-flight-lease counter and *does* saturate). Under the T93 profiles this
  panics in dev/test and wraps to `usize::MAX` in release. Make it consistent, and
  assert the paired invariant rather than relying on it silently.
- **Normalize error-message style** (`err-lowercase-msg`). `ResumeRefusal`'s five
  variants (`crates/core/src/resume.rs:248-283`) and `BootstrapRefusal`
  (`crates/core/src/execution.rs:1892`) are full prose sentences ending in a full
  stop; the other ~34 messages in the workspace are terse fragments with no
  trailing punctuation. These messages are **operator-facing refusal text** and
  several are asserted verbatim by tests, so treat this as a deliberate
  reconciliation: either normalize them or record in the register that refusal
  messages are an intentional exception to the convention. Do not silently change
  a string a test pins without updating the test's intent.
- **Investigate the `slot.fill` discard.** `crates/core/src/execution.rs:456-459`
  and `:695-698` do `let _ = slot.fill(value);` with a comment saying a rejected
  fill "would be a framework defect… so a rejected fill is dropped rather than
  silently swallowed as success" — but the code then reports
  `AttemptOutcome::Succeeded` unconditionally, which *is* swallowing it as
  success. Either the comment or the code is wrong. Resolve it: this is the one
  audit finding that may be a live bug rather than a style gap.
- **Document the write-discard convention.** ~25 `let _ = writeln!(out, …)` sites
  in `crates/cli/src/registry.rs` and `crates/cli/src/contract.rs` discard a real
  `io::Write` failure. Exactly one (`contract.rs:559`) explains why (a broken pipe
  on stderr must not change the exit code). State the convention once at module
  level in both files rather than 25 times, so `anti-empty-catch` is satisfied by
  a rule rather than by repetition.

## Test plan (write these first — TDD)

**Causal chains**
- Given each of the six types, when an instance wrapping a known underlying error
  is constructed, then walking `Error::source()` reaches that underlying error,
  and its `Display` appears in the chain. Assert per type, not once.
- Given a `RenderError::Malformed` produced from malformed artifact JSON, then the
  chain reaches a `serde_json::Error` reporting the offending line/column.
- Given a variant with genuinely no cause, then `source()` is `None` — the fix
  must not fabricate a link.

**Arithmetic**
- Given a run where attempts complete along every path that mutates `in_flight`
  (success, failure, timeout, cancellation, teardown), then the counter returns to
  zero at run end and never underflows. Drive this through the real loop, not a
  unit test of the counter.

**Poisoning policy**
- Given a lock whose chosen policy is *recover*, when a prior holder panicked,
  then the subsequent operation still succeeds.
- Given a lock whose chosen policy is *panic*, then the behaviour is unchanged
  from today — this half is a documentation change, and the test pins that.

**The `fill` discard**
- Given an attempt whose output slot rejects the fill, then the recorded outcome
  reflects what actually happened. Write this test first: it is what decides
  whether the comment or the code was right.

**Regression surface**
- Any test asserting a refusal message verbatim is updated deliberately, with the
  new expected text in the diff.

## Definition of done

- [ ] `GraphVerbError`, `StructureAssertError`, `OpenError`, `WriteError` override `source()` and return the wrapped cause; `RenderError` and `ReadError` carry the real error rather than a string.
- [ ] Every production `Mutex` lock site states its poisoning policy and the reason; the two philosophies are reconciled or explicitly justified as differing.
- [ ] `driver.rs`'s `in_flight` decrement is consistent with the codebase's saturating-counter discipline, with a test driving every mutating path to zero.
- [ ] The `ResumeRefusal`/`BootstrapRefusal` message style is either normalized or recorded as a deliberate exception in `docs/rust-skills-register.md`.
- [ ] The `let _ = slot.fill(value)` discrepancy is resolved, with a test pinning the true behaviour.
- [ ] The `writeln!`-discard convention is documented once per module in `registry.rs` and `contract.rs`.
- [ ] The three `#[allow(clippy::cast_*)]` sites lacking `reason = "…"` (`crates/core/src/limits.rs:476,488`, `crates/core/src/metrics.rs:173`) gain one.
- [ ] Recorded as clean, not changed: zero `todo!`/`unimplemented!`, zero float `==`, zero missing `# Errors`/`# Panics`, and the 31 provably-infallible `unwrap`/`expect` sites.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

The four tokio/rayon runtime-builder `.expect()`s (`crates/cli/src/dispatch.rs:110,117`,
`crates/cli/src/driver.rs:1393,2534`) are genuinely fallible — OS thread
exhaustion under a tight container `ulimit` is a real scenario — but are
deliberately treated as fatal bootstrap failures and documented as such. Routing
them into `BootstrapFailure` would be more honest and is a larger change than
this ticket's scope; **recommend recording them in the register as a known,
accepted panic surface** and revisiting separately.

## Out of scope

- Replacing the hand-written error types with `thiserror`. `dagr-core`'s
  zero-runtime-dependency guarantee forbids it there, and a split where only some
  crates use it would be worse than either end state. Recorded as `n-a` in the
  register.
- Adding `#[non_exhaustive]` to the remaining error enums. Three core enums have
  it; `TaskErrorClass` and `RehydrateClass` deliberately do **not** (their module
  doc fixes them at exactly three variants, and exhaustive matching is the
  intended contract). Auditing the rest is an API decision, excluded by M9's
  no-API-redesign scope.
