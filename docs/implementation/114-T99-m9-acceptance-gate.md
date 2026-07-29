# 114 · T99 — M9 acceptance gate

> **Milestone:** M9 · **Size:** M · **Type:** feature (gate) · **Components:** system-level
> **Branch:** `feat/t99-m9-acceptance-gate` · **Depends on:** T93, T94, T95, T96, T97, T98 · **Blocks:** —

## Why / context

M9 touched the build profiles, the language edition, the lint policy, the error
surface, the docs, and CI. Individually each ticket proved its own change. What no
individual ticket proves is the property the milestone exists to establish: that
after all of it, **dagr still behaves identically** and the rule dispositions are
complete and honest.

This gate is deliberately shaped like the M7 gate (T88): it asserts *structural*
facts about the finished state, not new behaviour. It is also the ticket that
catches the failure mode a hardening milestone is most prone to — a register row
claiming `satisfied` for something no one checked, or an `adopt` row whose ticket
quietly dropped the item.

## Objective

Prove the milestone landed as claimed.

- **The register is complete and true.** Every rule id is dispositioned exactly
  once and `scripts/check-rust-skills-adoption.sh` passes. Beyond the script's
  structural check, **spot-verify the substantive claims**: for every row marked
  `adopt`, confirm the named ticket actually shipped that item; for a sample of
  rows marked `satisfied`, confirm the claim independently. A register that says
  `satisfied` where nothing was verified is worse than an empty one, because it
  stops the next person from looking.
- **Behaviour is unchanged.** The engine's observable behaviour — event streams,
  artifacts, fingerprints, exit codes, terminal-state transitions — is identical
  to pre-M9. M9 was hardening; anything that changed here is a defect that slipped
  through, and this is the last chance to catch it.
- **The guarantees still hold structurally.** `dagr-core` has no runtime
  dependency edge; `render` and `metastore` still have no edge onto `core`;
  `metastore` and `jsonschema` stay out of a default build; `panic = "abort"` is
  absent from every profile.
- **The performance envelope holds.** The scale benchmark's per-node figure is
  within budget, and the M9 profile change (T93) is accounted for in the reported
  number rather than silently absorbed.
- **Update `docs/implementation/README.md`** with the M9 summary line and the
  running ticket total, matching the format the other milestones use.

## Test plan (write these first — TDD)

**Register integrity**
- Given the checked-in register, then the verifier passes, every rule id appears
  exactly once, and the count matches the rule-file count.
- Given every `adopt` row, then the named ticket's DoD covers that rule — checked
  by reading, and recorded as a table in the PR description.
- Given a sampled subset of `satisfied` rows, then each claim is independently
  reproducible, with the command or observation recorded.

**Behavioural identity**
- Given the reference pipeline, when its artifact and event stream are produced on
  the M9 head, then they are byte-identical to the pre-M9 baseline (excluding
  fields that legitimately vary — tool version, run id, timestamps). Capture the
  baseline from `main` **before** the M9 branches land so there is something real
  to compare against.
- Given the structural fingerprint, then it is unchanged from pre-M9. The edition
  bump must not have altered a hashed input.
- Given the full suite on both OS tiers, then it passes.

**Structural guarantees**
- Given `cargo build --workspace --no-default-features`, then `dagr-core` resolves
  with an empty runtime dependency set.
- Given the crate graph, then `render` and `metastore` have no edge onto `core`,
  and a default build pulls neither `libsql` nor `jsonschema`.
- Given every `[profile.*]` in the workspace, then none sets `panic = "abort"`.

**Performance**
- Given the scale benchmark, then the per-node figure is within budget, reported
  against both the pre-M9 and post-T93 numbers.

## Definition of done

- [ ] `scripts/check-rust-skills-adoption.sh` passes; every rule id dispositioned exactly once.
- [ ] Every `adopt` row is traced to a shipped ticket item, tabulated in the PR description; a sample of `satisfied` rows is independently verified with the check recorded.
- [ ] Reference artifacts, event stream, and structural fingerprint are byte-identical to the pre-M9 baseline modulo legitimately varying fields; the baseline was captured before the M9 branches landed.
- [ ] `dagr-core` has an empty runtime dependency set under `--no-default-features`; `render`/`metastore` have no edge onto `core`; a default build pulls neither `libsql` nor `jsonschema`.
- [ ] No profile anywhere sets `panic = "abort"`.
- [ ] The scale benchmark is within budget, with pre-M9 and post-M9 figures reported.
- [ ] Tests pass on `ubuntu-latest` and `macos-latest`.
- [ ] `docs/implementation/README.md` carries the M9 summary and the updated ticket total.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions

None. If the behavioural-identity comparison finds a difference, the gate fails
and the difference is investigated — it is not reclassified as an accepted change
at gate time. That is the whole purpose of capturing the baseline first.

## Out of scope

- Any new hardening. The gate verifies; it does not add. A rule found unapplied at
  gate time is recorded in the register as `declined` or scheduled as follow-up
  work, not smuggled into this PR.
- Amending `docs/arch.md`. M9 changes no component contract and no acceptance
  criterion, so the spec and the criteria partition are untouched.
