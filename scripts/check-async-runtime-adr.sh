#!/usr/bin/env bash
# Async-runtime ADR acceptance checks for ticket 004 (T2).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/004-T2-async-runtime-adr.md, section "Test plan"). T2 is
# a DECISION ticket whose deliverable is an ADR; most of its Test plan scenarios
# are documentary record checks ("the ADR states X", "exactly one of {A,B} is
# chosen") plus throwaway-prototype evidence whose *result* the ADR records.
#
# This script asserts the documentary record: that the ADR — embedded in the
# ticket file at the path the DoD names, matching the T1/T0.6/T3/T4/T0.7
# precedent of keeping each ADR inside its own ticket file — contains the
# required sections and the required decisions, in the exact normative
# vocabulary of arch.md (C13/C14/C16, Stability, C28). Authored FIRST as the
# acceptance gate, it fails on the ticket file as it stands before the ADR is
# written into it, and passes once the ADR records every decision the ticket is
# chartered to lock.
#
# It does NOT re-run the prototypes: the prototype is throwaway spike code,
# quarantined outside the shipping crates and deleted before the PR per the
# ticket-conventions decision(spike) rule; what survives is the ADR's record of
# what each prototype proved, which is what this script checks.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/004-T2-async-runtime-adr.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T0.6/T3/T4/T0.7 precedent: '# ADR: <title>'). The ticket prose
# above it (Objective, Test plan, DoD) already mentions tokio, spawn_blocking,
# cooperative, etc. — so a whole-file grep would pass content checks the ADR
# itself has not yet made. We therefore scope every content assertion to the ADR
# BODY only: the slice of the file from the first 'ADR:' heading line to EOF.
# The ticket's own H1 title ('# … ADR') is deliberately NOT matched (no colon),
# so before the embedded ADR is authored the slice is empty and every content
# check fails — exactly the tests-first behaviour we want.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")

# Case-insensitive extended-regex grep, scoped to the ADR body only.
has() { printf '%s' "$adr_body" | grep -qiE "$1"; }

# --- ADR skeleton (ticket-conventions §4: decision tickets need status /
# context / decision / consequences / rejected alternatives) ------------------
# The ADR is embedded in the ticket file, so it lives below the six ticket
# sections under its own 'ADR:' heading. We look for the ADR heading plus each
# of the five required section headings, all within the ADR body slice.
if printf '%s' "$adr_body" | grep -qiE '^#+[[:space:]]+ADR:'; then
  pass "adr: an embedded 'ADR:' heading is present in the ticket file"
else
  bad "adr: no embedded 'ADR:' heading found (expected '# ADR: …')"
fi
for sect in Status Context Decision Consequences 'Rejected alternative'; do
  if printf '%s' "$adr_body" | grep -qiE "^#+[[:space:]]+$sect"; then
    pass "adr: section '$sect' heading present"
  else
    bad "adr: required section '$sect' heading missing"
  fi
done

# --- Public-dependency record check (Test plan: "Public-dependency record
# check"; DoD lines 1-2) ------------------------------------------------------
# tokio named as the async runtime and a supported *public* dependency.
if has 'tokio' && has 'public dependency'; then
  pass "public-dep: tokio named as a supported public dependency"
else
  bad "public-dep: tokio must be named as a supported *public dependency*"
fi
# Replacing / major-bumping tokio is a major-version event.
if has 'major.version event' || has 'major version event'; then
  pass "public-dep: major-version-event rule for replacing/major-bumping tokio recorded"
else
  bad "public-dep: the 'replace/major-bump tokio = major-version event' rule is missing"
fi
# Context-exposed types are dagr-owned wherever practical.
if has 'dagr.owned' && has 'wherever practical'; then
  pass "public-dep: 'context-exposed types are dagr-owned wherever practical' recorded"
else
  bad "public-dep: the dagr-owned-context-types-wherever-practical rule is missing"
fi
# tokio types may appear in the public API only where the surface is honest.
if has 'honest'; then
  pass "public-dep: 'tokio types may appear only where the API is honest' recorded"
else
  bad "public-dep: the honest-API-surface rule for tokio types is missing"
fi

# --- Blocking-pool strategy (Test plan: "Blocking-does-not-starve",
# "Blocking abandonment"; DoD line 3) -----------------------------------------
if has 'spawn_blocking'; then
  pass "blocking-pool: blocking strategy names the tokio blocking mechanism (spawn_blocking)"
else
  bad "blocking-pool: the blocking strategy must name its mechanism (spawn_blocking)"
fi
if has 'starve' || has 'does not delay' || has "cannot delay"; then
  pass "blocking-pool: records that blocking work cannot starve/delay await-bound work"
else
  bad "blocking-pool: the no-starvation guarantee for await-bound work is missing"
fi
if has 'abandoned.but.running' && has 'cannot be killed'; then
  pass "blocking-pool: abandoned-but-running / cannot-be-killed accounting shape recorded"
else
  bad "blocking-pool: the abandoned-but-running (cannot-be-killed) permit shape is missing"
fi

# --- Compute-pool decision (Test plan: "Compute-pool decision record check",
# "Compute-pool bound evidence"; DoD line 4) ----------------------------------
# Exactly one of {dedicated compute pool (rayon), capped semaphore over
# spawn_blocking} chosen, the other named as rejected.
if has 'rayon' && has 'semaphore'; then
  pass "compute-pool: both candidate mechanisms (rayon vs capped semaphore) named"
else
  bad "compute-pool: the ADR must name BOTH candidates (dedicated pool/rayon; capped semaphore)"
fi
if has 'fixed pool size' || has 'never exceed' || has 'peak concurrency'; then
  pass "compute-pool: fixed-pool-size bound (concurrent compute never exceeds N) recorded"
else
  bad "compute-pool: the fixed-pool-size / never-exceed-N bound is missing"
fi
if has 'at least one thread' || has 'floor of.*one' || has 'at least 1 thread'; then
  pass "compute-pool: floor-of-one-thread under a fractional CPU quota recorded"
else
  bad "compute-pool: the at-least-one-thread floor under a fractional quota is missing"
fi

# --- Cancellation-token primitive (Test plan: "Await-bound cancellation",
# "Cancellation-child"; DoD line 5) -------------------------------------------
if has 'cancellationtoken' || has 'cancellation.token'; then
  pass "cancel: a run-scoped cancellation-token primitive is named"
else
  bad "cancel: the cancellation-token primitive must be named"
fi
if has 'child'; then
  pass "cancel: the per-attempt child-token relationship is recorded"
else
  bad "cancel: the per-attempt child-token relationship is missing"
fi
if has 'future.drop' || has 'drop.*future' || has 'dropping the future'; then
  pass "cancel: await-bound cancellation is by future-drop (immediate permit release)"
else
  bad "cancel: await-bound cancellation-by-future-drop is missing"
fi
if has 'cooperative'; then
  pass "cancel: blocking/compute cancellation recorded as cooperative-only (marked, not killed)"
else
  bad "cancel: blocking/compute cooperative-only (marked-not-killed) cancellation is missing"
fi

# --- Isolated framework runtime (Test plan: "Runtime-isolation evidence";
# DoD line 6) -----------------------------------------------------------------
if has 'isolated' && (has 'framework runtime' || has 'framework.machinery'); then
  pass "isolation: an isolated framework runtime is decided"
else
  bad "isolation: the isolated framework-runtime decision is missing"
fi
for rail in 'timer' 'signal' 'event.stream' 'cancellation'; do
  if has "$rail"; then
    pass "isolation: safety rail '$rail' named as running on the framework runtime"
  else
    bad "isolation: safety rail '$rail' not mentioned in the isolation decision"
  fi
done
if has 'sigterm'; then
  pass "isolation: SIGTERM-still-fires-when-workers-blocked guarantee recorded (C13)"
else
  bad "isolation: the SIGTERM-under-blocked-workforce guarantee is missing"
fi

# --- Shutdown-budget arithmetic (Test plan: "Shutdown-budget arithmetic";
# DoD line 7) -----------------------------------------------------------------
if has '10 ?s' && has '15 ?s' && has '2 ?s' && has '30 ?s'; then
  pass "budget: grace 10s + teardown 15s + flush 2s within the 30s window recorded"
else
  bad "budget: the 10s/15s/2s-within-30s shutdown-budget arithmetic is missing"
fi
if has 'operator flag' || has 'operator.configurable' || has 'operator-configurable'; then
  pass "budget: grace and teardown flagged operator-configurable"
else
  bad "budget: grace/teardown operator-configurability is missing"
fi
if has 'printed at startup' || has 'print.*at startup' || has 'startup'; then
  pass "budget: worst-case budget printed at startup recorded as a T35 obligation"
else
  bad "budget: the print-worst-case-budget-at-startup obligation is missing"
fi

# --- Test-runtime shape / C28 (Test plan: "Test-runtime shape evidence";
# DoD line 8) -----------------------------------------------------------------
if has 'no.*runtime' && has 'synchronous'; then
  pass "test-runtime: synchronous single-task tests need no runtime (C28)"
else
  bad "test-runtime: the 'sync task needs no runtime' rule (C28) is missing"
fi
if has 'test runtime' || has 'test.runtime'; then
  pass "test-runtime: a plain test runtime for await-bound task tests is specified (C28)"
else
  bad "test-runtime: the plain test-runtime for await-bound tests (C28) is missing"
fi

# --- Prototype-evidence record (DoD line 9) ----------------------------------
# The ADR must reference what each throwaway prototype proved, and record that
# the spike is quarantined/removed from the shipping crates.
if has 'prototype' || has 'spike'; then
  pass "evidence: the ADR references the throwaway prototype evidence"
else
  bad "evidence: the ADR must reference what the prototypes proved"
fi
if has 'quarantin' || has 'deleted' || has 'removed' || has 'discard'; then
  pass "evidence: the ADR records that spike code is quarantined/removed from shipping crates"
else
  bad "evidence: the spike-quarantined/removed record is missing"
fi

# --- Unblocks T9 and T33 (DoD line 10) ---------------------------------------
if has 'T9' && has 'T33'; then
  pass "unblocks: the ADR names its downstream consumers (T9, T33)"
else
  bad "unblocks: the ADR must state it leaves no choice open to T9 and T33"
fi

# --- No new dependency leaked into shipping crates ---------------------------
# T2 is a doc-only decision: it names tokio but does NOT wire it into any
# shipping crate (that is T9/T33). Guard against an accidental dependency
# addition that would change Cargo.lock and give cargo-audit new surface.
if grep -qiE '^[[:space:]]*tokio[[:space:]]*=' crates/*/Cargo.toml 2>/dev/null; then
  bad "scope: tokio must NOT be wired into a shipping crate here (that is T9/T33)"
else
  pass "scope: no tokio dependency wired into a shipping crate (doc-only decision)"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
