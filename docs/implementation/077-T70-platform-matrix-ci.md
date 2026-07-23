# 077 · T70 — Platform-matrix CI

> **Milestone:** M4 · **Size:** S · **Type:** feature (ci) · **Components:** Platform support
> **Branch:** `feat/t70-platform-matrix-ci` · **Depends on:** T7, T32, T36 · **Blocks:** T65

## Why / context
The spec commits to a hard, three-tier platform posture (arch.md **Platform support**): Linux containers are Tier 1 where the *full* suite runs including fault-injection and signal tests; macOS is dev-supported where everything compiles and runs with host-fallback pool sizing and the *core* suite runs; Windows is explicitly unsupported in v1. This ticket wires that posture into the CI matrix built by T7, exercising the container limit-detection host-fallback path from T32 (C12) and the signal/flush behaviour from T36 (C16) on the platforms where each is meaningful. It also honours the spec's rule that platform-conditional acceptance criteria (limit detection, signal handling, flush behaviour) are *named as such in the coverage matrix* — so the criteria matrix from T7 gains a per-platform classification the acceptance gate (T65) later reads. Nothing here changes behaviour; it decides where and under what platform conditions existing tests run.

## Objective
Extend the existing CI configuration so that the acceptance suite runs on a Linux tier-1 job and a macOS core-suite job, with platform-conditional criteria annotated in the checked-in coverage matrix and Windows deliberately absent.
- Add a **Linux** CI job that runs the entire test suite — unit, integration, fault-injection, and OS-signal tests — as the tier-1, everything-works reference.
- Add a **macOS** CI job that runs the platform-portable *core* suite: everything that compiles and runs on macOS, with pool sizing exercised through the host-fallback path (no cgroups) and fsync/flush behaviour asserted against documented divergences.
- Annotate the coverage matrix so that every platform-conditional criterion (container/cgroup limit detection from C12, OS signal handling and final flush from C16) is tagged with the platform(s) on which it is enforced, and so that criteria excluded on macOS are recorded as intentionally excluded — not silently unmapped.
- Ensure the macOS job selects only the core suite via an explicit, reviewable mechanism (a test filter, feature/cfg gate, or named test set), so that Linux-only tests are excluded *by intent*, not by accidental failure or a broad `allow-failures`.
- Keep Windows out of the matrix and record, in the matrix or a short repo note, that its absence is deliberate per the v1 platform posture.

## Test plan (write these first — TDD)
Because this is a CI-configuration ticket, the "tests" are (a) the matrix-annotation checker that must fail on missing/incorrect platform tags and (b) observable CI-run outcomes. Each scenario is independently checkable.

- **Linux full suite runs, including signal and fault-injection tests.** *Setup:* the Linux CI job is defined with the complete test invocation. *Action:* trigger the job on the ticket branch. *Expected:* the fault-injection tests (e.g. the T37 permit-release outcome matrix, T36 unwritable-sink and SIGTERM/SIGINT paths) and the cgroup limit-detection tests (T32) all execute and pass; the job log shows these test names ran (they are not skipped or filtered out).
- **cgroup limit detection is exercised on Linux only.** *Setup:* the C12 cgroup v2 → v1 detection tests (T32) exist and are Linux-gated. *Action:* run the Linux job and the macOS job. *Expected:* the cgroup-path tests run and pass on Linux; on macOS they are absent from the run (filtered/`cfg`-excluded) rather than failing.
- **macOS falls back to host pool sizing.** *Setup:* the T32 bootstrap probe's host-fallback branch (no cgroup available → host resources, unlimited sentinels → host) has a test. *Action:* run the macOS job. *Expected:* the host-fallback pool-sizing test runs and passes on macOS, demonstrating pool sizing derives from host resources with no cgroup dependency.
- **macOS core suite excludes Linux-only tests by intent.** *Setup:* the macOS job uses an explicit core-suite selector. *Action:* run the macOS job. *Expected:* the job is green with the Linux-only tests (cgroup detection, any Linux-specific signal semantics) excluded via the named selector; the job does **not** rely on `continue-on-error`/allow-failure to stay green, and removing a core test from the selector would make the job fail.
- **Flush/fsync divergence is documented and asserted, not assumed identical.** *Setup:* C16 final-flush behaviour has platform-aware expectations (arch.md notes different fsync semantics on macOS). *Action:* run the flush test on both jobs. *Expected:* the flush assertion passes on both platforms according to its documented per-platform expectation; no test asserts byte-for-byte identical fsync semantics across the two.
- **Coverage matrix names platform-conditional criteria.** *Setup:* the T7 coverage matrix now carries a platform annotation column/field. *Action:* run the matrix checker script. *Expected:* the C12 limit-detection criterion, and the C16 signal-handling and flush criteria, each carry a platform tag; the checker passes.
- **Matrix checker fails on an untagged platform-conditional criterion.** *Setup:* temporarily remove the platform tag from one platform-conditional criterion (e.g. C12 limit detection) in a throwaway edit. *Action:* run the matrix checker. *Expected:* the checker fails with a message identifying the untagged criterion; restoring the tag makes it pass. (This proves the annotation is enforced, not decorative.)
- **macOS-excluded criterion is recorded, not silently dropped.** *Setup:* a machine criterion that only runs on Linux (cgroup detection). *Action:* run the matrix checker. *Expected:* the criterion is still mapped to its Linux test and marked Linux-only; it is **not** reported as unmapped, so the T7 unmapped-machine-criterion gate stays satisfied.
- **Windows is absent by design.** *Setup:* the CI matrix definition and the platform note. *Action:* inspect the matrix and the note. *Expected:* no Windows job exists; the deliberate-absence note is present and reviewable. (No CI runner is added for Windows.)

## Definition of done
- [ ] A **Linux** CI job runs the complete test suite — unit, integration, fault-injection, and OS-signal tests — as the tier-1 reference, per arch.md Platform support "the full test suite runs in CI here."
- [ ] A **macOS** CI job runs the core suite; everything compiles and runs on macOS with documented divergences only.
- [ ] macOS pool sizing is exercised through the T32 host-fallback path (no cgroups), and that host-fallback test passes on macOS.
- [ ] C12 cgroup v2/v1 limit-detection tests run and pass on Linux and are excluded (not failing) on macOS.
- [ ] C16 signal handling (SIGTERM/SIGINT → cancel → complete, fsync-ed stream before exit) and the fault-injection/unwritable-sink tests run and pass on the Linux tier-1 job.
- [ ] C16 flush/fsync behaviour is asserted against documented per-platform expectations on both jobs; no test asserts cross-platform-identical fsync semantics.
- [ ] The macOS core suite is selected via an explicit, reviewable mechanism (named test set, filter, or `cfg`/feature gate) — Linux-only tests are excluded by intent, and the job does not depend on allow-failure/continue-on-error to be green.
- [ ] The coverage matrix (from T7) carries a platform annotation, and every platform-conditional criterion — C12 limit detection, C16 signal handling, C16 flush — is named as platform-conditional in it.
- [ ] The matrix checker fails when a platform-conditional criterion is missing its platform tag, and passes when tags are present.
- [ ] Linux-only machine criteria remain mapped to their tests and marked Linux-only, so the T7 unmapped-machine-criterion gate stays green (no criterion is silently dropped).
- [ ] Windows has no CI job, and its deliberate absence is recorded in the matrix or a short version-controlled note per the v1 posture; the note flags it as "revisit on demand."
- [ ] The pinned-toolchain policy from T7 is respected by both platform jobs (deterministic toolchain for any pinned/UI-sensitive tests).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- **Windows support of any kind** — no Windows CI job, no signal/process-model shims. Its absence is the point; revisit only on demand (permanent scope-boundary note: this ticket does not turn dagr into a cross-platform-scheduler promise it cannot keep).
- **The scale benchmark (T69)** and the **system acceptance gate (T65)** — this ticket feeds the matrix they consume but does not implement the benchmark job or the gate's determinism checks.
- **Authoring new C12/C16 behaviour or tests** — those land in T31/T32 and T35/T36; this ticket only routes existing tests onto platforms and annotates the matrix.
- **Broadening the coverage-matrix mechanism** (criterion→test mapping, unmapped-criterion enforcement) — owned by T7; here we only add the platform dimension.
- **Additional runners, containers, or self-hosted infrastructure** beyond one Linux and one macOS job — no distributed or multi-arch execution matrix (scope boundary: dagr is not a distributed execution system).
