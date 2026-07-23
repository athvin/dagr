<!--
dagr pull-request template. One PR per implementation ticket.
See CONTRIBUTING.md for the full process contract.
-->

## Ticket

<!-- Required. Link the ticket both ways (see CONTRIBUTING.md "One PR per ticket"). -->

- **tid:** `T?.?`
- **Ticket file:** `docs/implementation/NNN-TID-slug.md`

This PR maps to exactly one implementation ticket, on the one branch whose name
was copied verbatim from that ticket's `Branch:` header field.

## Acceptance criteria satisfied

<!--
Restate the specific Definition-of-done items / acceptance criteria this PR
claims to satisfy, and point at the tests that exercise each. A PR that claims a
criterion without a test that pins it does not merge.
-->

-

## Definition of done

- [ ] **Tests first.** The Test plan's failing tests were committed **before**
      implementation (the tests-first commit lands first), and each claimed
      acceptance criterion has a test exercising it.
- [ ] **Branch & PR.** Exactly one branch (name verbatim from the ticket header)
      and this one PR for the ticket.
- [ ] **Scope.** Stays inside the ticket; no deferred scope pulled forward; the
      permanent scope boundary (no scheduler / distributed execution / metadata
      store / web UI / DSL / backfill; graph shape fixed at runtime) is intact.
- [ ] **Open questions.** Every open question (ticket section **and** any
      `docs/tasks.md` `Q:` items) is resolved and recorded.
- [ ] **Docs / rustdoc updated** where the change touches public surface or
      documented behavior.
- [ ] **Merge gate green** — the CI checks below all pass (see CONTRIBUTING.md):
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy` with warnings denied (`-D warnings`)
  - [ ] the test suite (`cargo test`)
  - [ ] the rustdoc lint (`cargo doc`, `RUSTDOCFLAGS=-D warnings`)
  - [ ] `cargo audit` / `cargo deny` where configured
- [ ] **Reviewed.** An owner from `.github/CODEOWNERS` has reviewed (or the
      recorded autonomous-merge policy in `docs/implementation/DEVIATIONS.md`
      applies).

## Notes

<!-- Anything a reviewer needs: deviations (link DEVIATIONS.md), follow-ups, etc. -->
