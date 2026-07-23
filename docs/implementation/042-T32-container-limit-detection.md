# 042 · T32 — C12: container limit detection

> **Milestone:** M2 · **Size:** M · **Type:** feature · **Components:** C12
> **Branch:** `feat/t32-container-limit-detection` · **Depends on:** T31 · **Blocks:** T38, T70

## Why / context
T31 built the admission pools and the permit lifecycle but sized them from whatever it was handed; this ticket makes bootstrap discover the *real* ceiling of the machine the run is executing on and translate it into pool capacities. It implements the sizing half of C12 · Admission controller (arch.md §C12, lines 279 and 285/289), the bootstrap probe that turns a container's memory and CPU limits into weighted pool totals, and the fail-fast rejection of any node that can never fit. It sits inside the impure bootstrap phase (arch.md "The shape of a run", line 63) whose contract T0.5 froze, and it produces the `bootstrap-failed` artifact variant (C22, arch.md line 475) when the machine is too small. It is a hard prerequisite for the M2 overcommit demo (T38, which pins capacity through this ticket's flag to make CI deterministic) and for the platform-matrix CI (T70, which exercises cgroup v2, cgroup v1, and macOS host fallback).

## Objective
Give bootstrap a deterministic, ordered probe that reads the container's actual limits and sizes the C12 admission pools, with an operator override and a fail-fast too-big-node check. Concretely:

- Detect memory and CPU limits by probing sources in strict order: cgroup v2 first, then cgroup v1, then host resources when neither cgroup source is present (the dev-machine and macOS case).
- Treat unlimited-sentinel values from any cgroup source (the "max"/no-limit/absurdly-large representations) as "no limit here" and fall back to host resources for that dimension.
- Apply a headroom fraction that defaults to 20% (pools sized to the detected limit minus headroom).
- Guarantee every pool receives at least one unit after headroom and rounding — in particular the compute/thread pool gets at least one thread even under a fractional CPU quota.
- Add an operator flag, in the library-reserved namespace, that pins any pool's capacity outright, overriding both cgroup detection and host fallback; this same flag is the mechanism CI uses to make capacity deterministic.
- Reject at bootstrap — before any node executes — any node whose declared cost for any pool exceeds that pool's total capacity, emitting a complete error report and the `bootstrap-failed` artifact, and never wedging at admission time.
- Wire the resulting pool sizes into the T31 pool constructor so the admission controller operates against machine-derived capacity.

## Test plan (write these first — TDD)
Detection logic is tested against injected/faked probe sources (fixture files or a probe-source seam), never against the real host, so scenarios are deterministic on any CI runner.

- **cgroup v2 is preferred when present.** Setup: a fake probe environment exposing a cgroup v2 memory limit and CPU quota, and also a cgroup v1 source with different numbers, and a host with different numbers again. Action: run the bootstrap probe. Expected: the memory and thread pool totals are derived from the cgroup v2 numbers (minus 20% headroom), and the v1 and host values are ignored.
- **cgroup v1 fallback when v2 absent.** Setup: a fake environment with no cgroup v2 source but a valid cgroup v1 memory limit and CPU quota, plus differing host numbers. Action: run the probe. Expected: pool totals derive from the cgroup v1 numbers; host numbers are ignored.
- **host fallback when no cgroup exists.** Setup: a fake environment with neither cgroup v2 nor v1 (the dev-machine/macOS case), only host memory and CPU. Action: run the probe. Expected: pool totals derive from host resources, minus headroom.
- **unlimited sentinel falls back to host per dimension.** Setup: a cgroup source that reports an unlimited-sentinel for memory but a real CPU quota, with distinct host memory available. Action: run the probe. Expected: the memory pool is sized from host memory (the sentinel is treated as "no limit here"), while the thread pool is still sized from the cgroup CPU quota — the fallback is per dimension, not all-or-nothing.
- **20% headroom is the default.** Setup: a cgroup source with a round, known memory limit and CPU quota, no pinning flag. Action: run the probe. Expected: each pool's total equals the detected limit reduced by the 20% headroom fraction (within documented rounding), demonstrably less than the raw limit.
- **at least one unit per pool under a fractional quota.** Setup: a cgroup source reporting a sub-one-CPU quota (for example a half-core quota) and a very small memory limit. Action: run the probe. Expected: the thread/compute pool has at least one thread and the memory pool has at least one unit, even though the headroom-adjusted arithmetic would otherwise round to zero.
- **pinning flag overrides cgroup detection.** Setup: a cgroup source with real limits plus the operator flag pinning the memory pool (and/or thread pool) to an explicit value. Action: run the probe. Expected: the pinned pool's total is exactly the flag's value regardless of what cgroup detection found; unpinned pools still derive from detection.
- **pinning flag overrides host fallback.** Setup: no cgroup source (host-fallback path) plus the pinning flag. Action: run the probe. Expected: the pinned pool equals the flag value, not the host-derived value — confirming the flag beats both detection sources named in C12's criterion.
- **too-big node rejected at bootstrap, not at admission.** Setup: a pipeline containing one node whose declared cost for some pool exceeds that pool's headroom-adjusted total, with capacity pinned to a known small value. Action: run bootstrap. Expected: bootstrap fails fast before any node executes; the error report names the offending node, the pool, the declared cost, and the pool capacity; a `bootstrap-failed` run artifact is produced; the process exits with the bootstrap-failure exit code; admission is never entered and nothing hangs.
- **bootstrap-failure artifact is asserted, not assumed.** Setup: the too-big-node scenario above. Action: after bootstrap fails, read the run store. Expected: exactly one run artifact exists with outcome `bootstrap-failed` (distinct from `assembly-failed`), carrying the complete error list and zero attempts — the same assertion T30's resource-check test makes for its bootstrap failure.
- **a node exactly at capacity is admitted, not rejected.** Setup: a node whose declared cost for every pool exactly equals that pool's headroom-adjusted total. Action: run bootstrap. Expected: bootstrap succeeds and the node is a candidate for admission — the rejection rule is strictly "exceeds", so the boundary case fits.
- **sizes flow into the admission pools.** Setup: a fake cgroup source with known limits and a pipeline whose nodes are small relative to those limits. Action: run bootstrap, then start admission. Expected: the T31 pools report totals equal to the probe-derived numbers, and admission decisions honor those totals (a node fitting the derived total is admitted; the combined derived total is the ceiling).

## Definition of done
- [ ] Pool sizes are derived at bootstrap by probing cgroup v2 first, then cgroup v1, then host resources when neither cgroup source exists (C12; arch.md line 279).
- [ ] Unlimited-sentinel values from a cgroup source fall back to host resources for that dimension (C12; arch.md line 279).
- [ ] The headroom fraction defaults to 20% and is applied to every detected pool total (C12; arch.md line 279).
- [ ] Every pool receives at least one unit after headroom and rounding, and the compute/thread pool receives at least one thread even under a fractional CPU quota (C12; arch.md line 279).
- [ ] An operator flag, living in the library-reserved namespace, pins any pool's capacity outright and overrides both cgroup detection and host fallback; this is the mechanism CI uses for deterministic capacity (C12 acceptance: pinning flag overrides both; arch.md line 289).
- [ ] Pool sizes reflect the container's limit when one exists and the host's resources when one does not (C12 acceptance; arch.md line 289).
- [ ] A node whose declared cost exceeds any pool's total capacity is rejected at bootstrap, not at admission time, with a distinct complete error report naming node, pool, declared cost, and capacity (C12 acceptance; arch.md lines 285, 194).
- [ ] The too-big-node rejection produces a `bootstrap-failed` run artifact (distinct from `assembly-failed`, zero attempts, complete error list) and the bootstrap-failure exit code, and never hangs (C22, arch.md line 475; C26/C7 bootstrap fail-fast, arch.md line 63).
- [ ] Detection is exercised through an injectable probe-source seam so cgroup v2, cgroup v1, host-fallback, and sentinel paths are all deterministically testable off the real host.
- [ ] Probe-derived pool totals are wired into the T31 pool constructor and govern subsequent admission decisions.
- [ ] The platform-conditional nature of limit detection is documented as such (Tier-1 Linux cgroups vs macOS host fallback) for the T70 coverage matrix (arch.md lines 629–633).
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None were carried in the ticket or the T32 `docs/tasks.md` entry. The
implementation resolved these design points and records them here:

- **Where the OS-reading code lives.** `dagr-core` has stayed dependency-free,
  and the natural first instinct was to put the cgroup/proc reading in `dagr-cli`.
  But the whole probe is made **injectable** — it reads cgroup/proc values from a
  supplied root `Path` via `std::fs` and takes the host core count as an injected
  value — so it needs no OS-specific dependency and only one `std`-only live-host
  read (`std::thread::available_parallelism`, in the production `from_host`
  constructor). Since `PoolCapacities` already lives in `dagr_core::admission`,
  the probe lives beside it in a new `dagr_core::limits` module. Core stays
  dependency-free; no crate gained a dependency; `cargo deny`/`audit` are
  unchanged.
- **CPU dimension → which thread pool(s).** The detected CPU allocation sizes
  **both** the blocking and compute thread pools identically. Routing a node onto
  one pool versus the other is T33's class dispatch, not this sizing pass, so
  sizing both from the same CPU figure is the honest default (an over-count is
  impossible because a node draws from only the pool its class selects).
- **Pinning-flag namespace.** The operator flag lives in the library-reserved
  `dagr.` namespace (arch.md line 498), concretely `dagr.pool.memory`,
  `dagr.pool.blocking-threads`, `dagr.pool.compute-threads`; `PinnedPools::set_flag`
  rejects any key outside `dagr.pool.` so a task cannot smuggle a capacity
  override. The typed builder (`PinnedPools::new().memory(..)`) is the programmatic
  equivalent; the CLI flag surface itself is T55/T56.
- **Headroom rounding + the at-least-one floor.** Headroom is applied as
  `floor(raw * (1 - headroom))` then `max(1)`, so every pool gets at least one unit
  and the compute pool at least one thread even under a sub-one-core quota. A
  fractional CFS quota rounds toward the floor before the floor-at-one lift.
- **The bootstrap-failure artifact shape.** The too-big-node rejection produces a
  `CapacityBootstrapFailure` mirroring T30's resource-check `BootstrapFailure`
  (same `BootstrapOutcome::BootstrapFailed`, complete error list, zero attempts),
  and the driver emits the new `RunOutcome::BootstrapFailed` wire outcome
  (`bootstrap-failed`), distinct from `assembly-failed`.

## Out of scope
- The admission pools, permit acquisition/release, oldest-ready-first ordering, bounded bypass, and zombie cost accounting — all owned by T31 (C12); this ticket only sizes the pools T31 built and feeds them to it.
- Adding pools beyond memory and threads; the "at minimum" question about additional pools belongs to T31, not here.
- The M2 overcommit-and-clean-stop demo itself (T38) and the platform-matrix CI wiring (T70); this ticket supplies the pinning flag and the documented fallback they depend on but does not implement those tickets.
- The declared/measured cost juxtaposition in the run artifact and undeclared-cost warnings (C12/C23, T31 and C22 tickets); only the too-big rejection artifact is in scope here.
- Cross-process capacity coordination or splitting a host's capacity across simultaneous runs — that is the operator's call via the pinning flag, and coordinating it in-tool would make dagr a scheduler (arch.md line 607). Out of bounds permanently.
- Runtime resizing of pools or any adjustment of capacity after bootstrap; sizing happens once, in bootstrap, and the graph shape and capacities never change at runtime.
- Windows limit detection; Windows is explicitly unsupported in v1 (arch.md line 631).
