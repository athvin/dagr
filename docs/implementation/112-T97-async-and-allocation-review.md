# 112 · T97 — async discipline and allocation review

> **Milestone:** M9 · **Size:** M · **Type:** feature (tests) · **Components:** C13, C16, system-level
> **Branch:** `feat/t97-async-and-allocation-review` · **Depends on:** T95 · **Blocks:** T99

## Why / context

The async rules in the skill (`anti-lock-across-await`, `async-bounded-channel`,
`async-joinset-structured`, `async-cancel-safety`) name a class of defect that is
invisible in review and expensive in production: a guard alive across a
suspension point, or a queue with no backpressure. dagr's driver is the workspace's
largest module (`crates/cli/src/driver.rs`, 2776 lines) and runs two runtimes plus
a rayon pool, so it is exactly where this class would hide.

The audit's preliminary read says the code is *already* disciplined — and that is
precisely why this ticket is **verification-first**. The two candidates it found
both look deliberate:

- `crates/cli/src/driver.rs:1413` builds an **unbounded** channel
  (`tokio::sync::mpsc::unbounded_channel::<AttemptDone>()`), and
  `crates/metastore/src/live_sink.rs:132` an unbounded `std::sync::mpsc`.
  `async-bounded-channel` wants a bound — but the number of in-flight attempts is
  already capped by the admission controller (C12) and the execution-class pools
  (C13), so the queue may be *structurally* bounded by something stronger than a
  channel capacity. If so, adding a bound would at best duplicate the invariant
  and at worst deadlock: a finishing attempt blocking on `send` while the loop
  that would drain it waits on that attempt.
- Every production `Mutex` guard the error audit inspected covers trivial,
  non-awaiting bookkeeping (an `Option` assignment, a `HashSet` insert, a `Vec`
  take). No `std::sync::RwLock` exists in production at all.

Neither observation is a proof. Turning both into pinned, tested facts is the
work — a comment claiming an invariant is worth much less than a test that fails
when it breaks.

## Objective

- **Prove or fix the channel bounds.** For each of the two unbounded channels,
  establish the true upper bound on queue depth. If it is structural, land a test
  that pins it (drive a run whose admission capacity is 1 and whose graph is wide,
  and assert the queue never exceeds the derived bound) plus a comment at the
  construction site naming the invariant and where it is enforced. If it is *not*
  structural, bound the channel — and justify the capacity chosen.
- **Sweep for guards held across `.await`.** Walk every `.lock()` in an `async fn`
  or an async block and establish, per site, that the guard is dropped before the
  next suspension point. Where the compiler can prove it for us, prefer that:
  enabling `clippy::await_holding_lock` turns this from a one-time review into a
  standing guarantee, which is worth more than the sweep itself.
- **Confirm structured task lifetimes.** Check every `tokio::spawn` /
  `spawn_blocking` / `JoinHandle` for orphaned handles — a detached task that
  outlives the run would undermine C16's "nothing orphaned" shutdown guarantee.
  `crates/cli/src/signals.rs:186` spawns a signal listener for the runtime's
  lifetime; confirm that is the intended exception and say so at the site.
- **Fix the one quadratic allocation.** `crates/metastore/src/live_sink.rs:183`
  does `bytes: self.buffer.clone()` inside `project()`, which `append_line()`
  calls on **every appended event line**, while `self.buffer` only grows for the
  life of the run — an O(n) copy per event, **O(n²) over the stream**. This one
  needs no profile: the complexity argument is the evidence. It is also on the
  guaranteed-write path, so fix it without weakening the write guarantee.
- **Document the blocking-I/O-in-async gap.** `ScratchStore::get`/`put`/`remove`
  (`crates/core/src/scratch.rs:269,305,329,359`) are synchronous and do real
  blocking I/O — write, **fsync the file**, atomic rename, **fsync the
  directory**. `RunContext::scratch()` hands that store straight to a task body,
  and the default `ExecutionClass` is `AwaitBound`, driven on the async worker
  pool. So a task author can block a tokio async worker on two fsyncs with no
  type-level warning (`async-tokio-fs`, `async-spawn-blocking`). An async scratch
  API is impossible here — `dagr-core` cannot depend on tokio — so the fix is
  documentation, not code: say so plainly at `RunContext::scratch` and in the
  `scratch` module docs, and name the remedy (declare the node `Blocking`). Record
  it in the register as `n-a — architecturally forced, documented at the seam`.
- **Allocation review, evidence-gated for everything else.** Three sites have a
  stated argument and may be taken: `crates/core/src/resume.rs:519` (`seed.clone()`
  — the sole caller never reuses `seed`, so the parameter can take ownership),
  `crates/core/src/resume.rs:537` (`for producer in inputs.clone()` clones every
  producer eagerly when the next line discards most via an early `continue`), and
  `crates/cli/src/graph.rs:335` (`Vec::new()` where `pipeline.len()` is already
  available for `with_capacity`). Beyond these, `anti-premature-optimize` and
  `perf-profile-first` govern: dagr already meets its per-node budget, so an
  unmeasured change is churn against a byte-for-byte determinism guarantee.
  Explicitly **do not** touch the ~40 `Value::from(self.field.clone())` sites in
  `crates/artifact/` — `Value::from(&str)` allocates identically, so there is no
  saving without consuming `self`, which these retained records cannot do.
- **Record the confirmed non-findings** in `docs/rust-skills-register.md` rather
  than leaving them to be re-discovered. The audit checked and cleared: **no lock
  is held across an `.await` anywhere** (every guard covers a short synchronous
  mutation and is dropped before the next suspension — a consistent discipline,
  not luck); there is **no `select!`** anywhere (deliberate, so tokio's `macros`
  feature is never needed, which also makes `async-cancel-safety` vacuous here);
  determinism is handled by a near-total `BTreeMap`/`BTreeSet` preference (223
  uses against 8 `HashMap`, every one a `TypeId`/`NodeId` lookup table never
  iterated into output); and there is **no collect-then-reiterate** waste in the
  workspace. The two O(n²) `Vec::contains` loops
  (`crates/core/src/readiness.rs:637`, `crates/core/src/context.rs:595`) are
  bounded by `MAX_INPUT_ARITY = 8` and per-pipeline resource-type count
  respectively — record the bound, leave the code.

## Test plan (write these first — TDD)

**Channel depth**
- Given a wide graph run under a deliberately narrow admission capacity, when the
  run completes, then the observed maximum depth of the `AttemptDone` queue never
  exceeds the bound derived from the admission/pool limits. Instrument the depth;
  do not infer it.
- Given the metastore live sink under sustained write load, then its queue depth
  is likewise bounded, and a slow writer applies backpressure rather than growing
  the queue without limit.

**Locks and awaits**
- Given `clippy::await_holding_lock` enabled at deny, then
  `cargo clippy --workspace --all-targets -- -D warnings` is green. This is the
  test: if a guard is held across an await anywhere, the build fails and the
  ticket has found a real defect.

**Task lifetimes**
- Given a run that completes normally and a run cancelled by signal, then no
  spawned task outlives the driver's shutdown beyond the stated budget, and the
  existing C16 "nothing orphaned" assertions still hold.

**The quadratic clone**
- Given a run emitting many event lines through the metastore live sink, then the
  total bytes copied grows **linearly**, not quadratically, in the number of
  lines. Pin this with a measurement that would fail against today's code — a
  test that merely checks correctness would pass both before and after and prove
  nothing.
- Given the same run, then the guaranteed-write contract is unchanged: every
  event still reaches the store, and a sink fault still surfaces as one.

**Nothing regressed**
- Given any allocation change, then the scale benchmark's per-node figure is
  reported before and after, and both determinism jobs still byte-match.

## Definition of done

- [ ] Both unbounded channels are either bounded with a justified capacity, or proven structurally bounded by a test plus a comment naming the enforcing invariant.
- [ ] `clippy::await_holding_lock` is enabled at deny in `lints.toml` and `[workspace.lints]`, and the workspace is green under it.
- [ ] Every `tokio::spawn`/`spawn_blocking`/`JoinHandle` is accounted for; the signal-listener exception is documented at its site; C16's no-orphans assertions pass.
- [ ] `live_sink.rs`'s per-append full-buffer clone is gone, pinned by a test that fails against today's code, with the guaranteed-write contract intact.
- [ ] The blocking-fsync-on-an-async-worker gap is documented at `RunContext::scratch` and in the `scratch` module docs, naming `Blocking` as the remedy.
- [ ] The three argued allocation sites (`resume.rs:519,537`, `graph.rs:335`) are addressed or explicitly deferred with a reason; nothing else is touched without a measurement.
- [ ] The confirmed non-findings (no lock across `.await`, no `select!`, `BTreeMap` determinism, no collect-then-reiterate, the two arity-bounded `Vec::contains` loops) are recorded in `docs/rust-skills-register.md`.
- [ ] The scale benchmark passes and both determinism jobs byte-match.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

If `clippy::await_holding_lock` fires anywhere, that is a genuine finding and may
grow this ticket. Report it rather than suppressing it; a suppression on this
particular lint would defeat the only mechanical guarantee in the ticket.

### Resolved

**Does `clippy::await_holding_lock` fire? No — and it was never off.** The lint
sits in clippy's `suspicious` group, which `clippy::all` includes, and dagr denies
`clippy::all` workspace-wide. So it has been denied for the whole life of the
workspace and `cargo clippy --workspace --all-targets -- -D warnings` is green under
it with **zero** findings and **zero** suppressions. The ticket's premise — that
enabling it would turn a one-time review into a standing guarantee — was already
true; what was missing is that the guarantee depended entirely on upstream keeping
the lint in that group. The lint is therefore written out at `deny` at priority 0 in
both `lints.toml` and `[workspace.lints.clippy]` (the ratchet T96 applied to
`missing_docs`/`missing_errors_doc`, for the same reason), and
`scripts/check-lint-parity.sh` now fails the build if either half drops below
`deny`. The ticket did not grow.

**`docs/tasks.md` carries no `Q:` items for T97.** `tasks.md` covers M0–M4 only;
the M9 tickets (T92–T99) are described in `docs/implementation/README.md`, which
carries no open questions for this one. Both sources of open questions are
therefore accounted for.

**One brief claim did not survive contact with the code, and was declined rather
than forced.** The brief argues `crates/core/src/resume.rs`'s `seed.clone()` away on
the grounds that "the sole caller never reuses `seed`". The caller does reuse it —
`seed` is a field of the returned `ResumePlan` — so `must_run` and `seed` are two
genuinely needed sets and taking the parameter by value would only move the same
clone to the call site. Declined, with the reasoning recorded at the site and in
`docs/rust-skills-register.md`. The brief's bound for the second `Vec::contains`
loop was likewise corrected there (it is bounded by the node count, not the
resource-type count — but the loop only executes on the bootstrap-failure path).

**Scale benchmark, before and after** (same machine, dev/test profile, 1000 no-op
nodes): **466 574 ns/node before**, **468 286 ns/node after**. Two runs inside the
same post-change invocation reported 435 268 and 468 286 ns/node, so the run-to-run
band is roughly ±8 % and the difference is noise. Both are far under the 1 000 000
ns/node spec ceiling and the 16 000 000 ns/node CI budget. No allocation change in
this ticket touches a hot path: two are one-time emitter/plan allocations, and the
third removes work from the metastore sink, which the benchmark does not exercise.

## Out of scope

- Introducing `tokio::sync::Mutex`. The std mutexes here guard synchronous
  bookkeeping; an async mutex would add await points where none are needed.
- Splitting `driver.rs`. Its size makes this review harder, which is an argument
  for the split — but that is `proj-mod-by-feature`, recorded as `declined` for
  M9's no-API-redesign scope, not smuggled in here.
- `mem-smallvec` / `perf-ahash` and the rest of the allocation-crate family: both
  need a new runtime dependency, which M9 forbids and the register records.
