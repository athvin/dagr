#!/usr/bin/env bash
# Timeout-abandonment & permit-accounting ADR acceptance checks for ticket 009
# (T0.3).
#
# Each check below is a mechanical translation of the ticket's Test plan and
# Definition of done (docs/implementation/009-T0.3-timeout-and-permit-accounting-spike.md).
# T0.3 is a DECISION (SPIKE) ticket whose durable deliverable is an ADR; its
# Test-plan scenarios are throwaway-prototype evidence whose *result* the ADR
# records, plus a decision-record completeness check ("the ADR states X").
#
# This script asserts the documentary record: that the ADR — embedded in the
# ticket file at the path the DoD names, matching the T1/T2/T0.2/T0.7 precedent
# of keeping each ADR inside its own ticket file — contains the required
# sections and the required decisions, in the exact normative vocabulary of
# arch.md (C12/C14, the terminal-state table, C16/C19/C23 scope notes). Authored
# FIRST as the acceptance gate, it fails on the ticket file as it stands before
# the ADR is written into it, and passes once the ADR records every decision the
# ticket is chartered to lock.
#
# It does NOT re-run the prototype: the prototype is throwaway spike code, built
# outside the shipping crates and deleted before the PR per the ticket-conventions
# decision(spike) rule; what survives is the ADR's record of what it proved,
# quoted as EVIDENCE lines, which is what this script checks for.
#
# Run from the repository root. Exit 0 = every ADR assertion holds, 1 = a
# failure, 2 = the ticket file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/009-T0.3-timeout-and-permit-accounting-spike.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.2/T0.7 precedent: '# ADR: <title>'). The ticket prose
# above it (Objective, Test plan, DoD) already mentions timeout, zombie, permit,
# etc. — so a whole-file grep would pass content checks the ADR itself has not
# yet made. We therefore scope every content assertion to the ADR BODY only: the
# slice of the file from the first 'ADR:' heading line to EOF. The ticket's own
# H1 title ('# … spike') is deliberately NOT matched (no 'ADR:' with a colon),
# so before the embedded ADR is authored the slice is empty and every content
# check fails — exactly the tests-first behaviour we want.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")

# Case-insensitive extended-regex grep, scoped to the ADR body only.
has() { printf '%s' "$adr_body" | grep -qiE "$1"; }

# --- ADR skeleton (ticket-conventions §4: decision tickets need status /
# context / decision / consequences / rejected alternatives) ------------------
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

# --- Await-bound: future dropped, permit released IMMEDIATELY (DoD 2; Test
# plan "Await-bound timeout releases the permit immediately") ------------------
if has 'await.bound' && (has 'future.drop' || has 'drop.*future' || has 'dropped'); then
  pass "await-bound: future-drop cancellation recorded"
else
  bad "await-bound: future-drop on timeout is missing"
fi
if has 'released immediately' || has 'immediate.*release' || has 'permit releases immediately'; then
  pass "await-bound: immediate permit release recorded"
else
  bad "await-bound: immediate-permit-release is missing"
fi

# --- Blocking/compute: MARKED timed out immediately, permit HELD until the
# closure returns (DoD 3; Test plan "Blocking timeout …", "Compute … identical")
if has 'blocking' && has 'compute'; then
  pass "blocking/compute: both classes named for the abandoned-but-running path"
else
  bad "blocking/compute: the ADR must name both the blocking and compute classes"
fi
if has 'cannot be killed'; then
  pass "blocking/compute: 'a thread cannot be killed' honesty recorded"
else
  bad "blocking/compute: the unkillable-thread premise is missing"
fi
if has 'marked' && (has 'timed.out immediately' || has 'immediately.*timed.out' || has 'fate.*decided' || has 'fate decided'); then
  pass "blocking/compute: marked-timed-out-immediately (fate decided) recorded"
else
  bad "blocking/compute: the mark-immediately/fate-decided rule is missing"
fi
if has 'abandoned.but.running'; then
  pass "blocking/compute: abandoned-but-running work named"
else
  bad "blocking/compute: 'abandoned-but-running' is missing"
fi
if has 'held until' && has 'returns'; then
  pass "blocking/compute: permit held until the closure actually returns"
else
  bad "blocking/compute: permit-held-until-closure-returns is missing"
fi

# --- Capacity invariant counts zombies (DoD 4; Test plan "capacity invariant
# never lies", C12 acceptance) ------------------------------------------------
if has 'never exceed' && (has 'capacity' || has 'pool'); then
  pass "capacity: counted cost (incl. zombies) never exceeds pool capacity"
else
  bad "capacity: the never-exceed-capacity invariant is missing"
fi
if has 'zombie' && has 'counted'; then
  pass "capacity: the invariant explicitly counts zombies"
else
  bad "capacity: the invariant must state that it counts abandoned-but-running (zombie) cost"
fi
if has 'ledger'; then
  pass "capacity: the ledger mechanism that guarantees the invariant is named"
else
  bad "capacity: the permit-ledger mechanism is not named"
fi

# --- Ledger observes the return (DoD 5) --------------------------------------
if has 'join' || has 'handoff' || has 'hand.off' || has 'observes.*return' || has 'closure return'; then
  pass "return: how the ledger observes the closure's return is named (join/handoff)"
else
  bad "return: the join/handoff mechanism that observes the return is missing"
fi

# --- Deferred retry (DoD 6; Test plan "Retry is deferred"; C1 exclusivity) ---
if has 'retry' && (has 'deferred' || has 'defer')  ; then
  pass "retry: deferred-until-previous-closure-returns recorded"
else
  bad "retry: deferred-retry rule is missing"
fi
if has 'exclusiv' || has 'concurrent.*zombie' || has 'its own zombie'; then
  pass "retry: preserves C1 exclusivity (never concurrent with its own zombie)"
else
  bad "retry: the C1-exclusivity rationale is missing"
fi

# --- Late-result barrier (DoD 7; Test plan "A late result … discarded"; C14) --
if has 'late.result' || has 'late result'; then
  pass "late-result: the late-result barrier is named"
else
  bad "late-result: the late-result barrier is missing"
fi
if has 'never fill' && (has 'slot' || has 'scratch'); then
  pass "late-result: never fills a slot / never writes scratch recorded"
else
  bad "late-result: the never-fill-slot / never-write-scratch rule is missing"
fi

# --- Terminal state decided exactly once (DoD 8; Test plan "Terminal state …
# exactly once"; the normative terminal-state table) --------------------------
if has 'exactly once' || has 'decided.*once'; then
  pass "terminal: terminal state decided exactly once recorded"
else
  bad "terminal: the decided-exactly-once rule is missing"
fi
if has 'stays.*timed.out' || has 'timed.out.*stays' || has 'remains.*timed.out'; then
  pass "terminal: a blocking timeout is and stays 'timed-out'"
else
  bad "terminal: the 'timed-out stays timed-out' rule is missing"
fi
if has 'abandoned' && (has 'cancellation path' || has 'C16'); then
  pass "terminal: 'abandoned' arises only on the cancellation path (C16), not after timeout"
else
  bad "terminal: the 'abandoned only on the cancellation path' distinction is missing"
fi

# --- Zombie-cost reporting shape (DoD 9; Test plan "Zombie cost is reportable")
if has 'zombie.cost' || (has 'count of' && has 'zombie') || has 'how many zombies'; then
  pass "report: zombie-cost reporting shape (count + per-pool cost) recorded"
else
  bad "report: the zombie-cost reporting shape is missing"
fi
if has 'C19' && has 'C23'; then
  pass "report: the shape feeds C19 (zombie-at-exit event) and C23 (declared-vs-measured)"
else
  bad "report: the C19/C23 downstream feed of the report shape is missing"
fi

# --- Named seams for the three consumers (DoD 10) ----------------------------
if has 'T21' && has 'T31' && has 'T37'; then
  pass "seams: the three consumer tickets (T21, T31, T37) are named"
else
  bad "seams: the ADR must name its consumers T21, T31, T37"
fi
if has 'seam'; then
  pass "seams: the ADR explicitly names the seams the consumers implement against"
else
  bad "seams: the ADR must name the concrete seams (permit-ledger ops, timeout-classification, capacity assertions)"
fi

# --- Throwaway prototype evidence + disposition (DoD 11, 12) ------------------
if has 'prototype' || has 'spike'; then
  pass "evidence: the ADR references the throwaway prototype"
else
  bad "evidence: the ADR must reference the throwaway prototype"
fi
if has 'EVIDENCE'; then
  pass "evidence: the ADR quotes the prototype's EVIDENCE lines"
else
  bad "evidence: the ADR must quote the prototype's EVIDENCE (real runtime behaviour)"
fi
if has 'tokio'; then
  pass "evidence: the prototype ran against the real tokio runtime"
else
  bad "evidence: the ADR must record that the prototype ran on the tokio runtime"
fi
if has 'deleted' || has 'quarantin' || has 'removed' || has 'disposed'; then
  pass "evidence: the ADR records the prototype's disposition (deleted/quarantined)"
else
  bad "evidence: the spike-disposition record is missing"
fi

# --- Scope note: what this ADR deliberately does NOT decide (DoD 13) ----------
if has 'C16' && has 'C19' && has 'C23'; then
  pass "scope: the scope note names C16 (cancellation-path abandoned), C19/C23 (wiring)"
else
  bad "scope: the deliberately-not-decided scope note (C16, C19/C23) is missing"
fi
if has 'T32' && (has 'container' || has 'limit detection' || has 'sizing')  ; then
  pass "scope: container-limit-detection / pool-sizing deferred to T32"
else
  bad "scope: the T32 (container limit detection / sizing) scope-out is missing"
fi

# --- The runtime dependencies land where the ADR says they land --------------
#
# PREDICATE CORRECTED BY T98, and deliberately not relaxed.
#
# As authored, this asserted that tokio/rayon were NOT yet wired into any
# shipping crate, because at T0.3 the spike lived outside the workspace and the
# wiring belonged to T21/T31/T33. That is a claim about the ORDER OF WORK, and it
# went permanently false when T33 shipped; unwired in CI, the failure went
# unseen. See the same repair in check-async-runtime-adr.sh.
#
# The DESIGN invariant behind it survives and is re-asserted here: the timeout
# and permit machinery lives in the crate that owns the run loop, and every other
# crate must justify a runtime edge or not have one. That is expressed as an
# ALLOWLIST over every workspace member. A scan narrowed to `crates/core` would
# defend the zero-dependency guarantee and nothing else — rayon appearing on
# `artifact`, or tokio on `render`, would pass unremarked, and the ADR's
# placement decision would go unenforced everywhere except the one crate that was
# never going to break it.
#
# Each name here is a decision, recorded rather than hidden by narrowing:
#   tokio      -> cli (owns the run loop, T21/T31/T33), metastore (libsql's API
#                 is async; the crate sits behind the default-off `metastore`
#                 feature, ADR 097 §5)
#   tokio-util -> cli (the cancellation-token half of the same machinery)
#   rayon      -> cli (the CPU-permit pool the ADR sizes)
# Anything else is a violation, `dagr-core` emphatically included.
#
# Emits one line per edge outside its allowlist, scanning every
# `<root>/<crate>/Cargo.toml`. Emits the sentinel `__NO_MANIFESTS__` when the
# glob matched nothing, so "no output" can never be read as "no violations".
# This function IS the predicate, and the probe below drives it rather than
# re-deriving it.
scan_runtime_edges() {
  _root=$1
  _n=0
  for _manifest in "$_root"/*/Cargo.toml; do
    [ -f "$_manifest" ] || continue
    _n=$((_n + 1))
    _crate=${_manifest%/Cargo.toml}
    _crate=${_crate##*/}
    for _dep in tokio tokio-util rayon; do
      grep -qiE "^[[:space:]]*$_dep[[:space:]]*=" "$_manifest" || continue
      case "$_dep" in
        tokio) _allowed="cli metastore";;
        *)     _allowed="cli";;
      esac
      case " $_allowed " in
        *" $_crate "*) ;;
        *) printf '%s on %s (%s; allowed: %s)\n' "$_dep" "$_crate" "$_manifest" "$_allowed";;
      esac
    done
  done
  [ "$_n" -eq 0 ] && printf '__NO_MANIFESTS__\n'
  return 0
}

offending=$(scan_runtime_edges crates)
case "$offending" in
  __NO_MANIFESTS__*)
    bad "scope: no crates/*/Cargo.toml was scanned — the runtime-placement check is vacuous";;
  "")
    pass "scope: tokio/tokio-util/rayon appear only where the ADR places them, across every workspace crate";;
  *)
    bad "scope: a runtime dependency landed where the ADR does not place it:"
    printf '%s\n' "$offending" | sed 's/^/        /';;
esac

# The scan is non-vacuous, proved by running it against a synthetic tree: each
# dependency planted on a crate its own allowlist excludes must be flagged, and
# the allowlisted placements must not be. Nothing in the working tree is touched.
probe=$(mktemp -d 2>/dev/null) || probe=""
if [ -n "$probe" ]; then
  mkdir -p "$probe/artifact" "$probe/render" "$probe/metastore" "$probe/cli"
  printf '[dependencies]\nrayon = "1"\n' >"$probe/artifact/Cargo.toml"
  printf '[dependencies]\ntokio-util = "0.7"\n' >"$probe/render/Cargo.toml"
  # metastore may carry tokio but not rayon — the allowlist is per dependency.
  printf '[dependencies]\ntokio = "1"\nrayon = "1"\n' >"$probe/metastore/Cargo.toml"
  printf '[dependencies]\ntokio = "1"\nrayon = "1"\ntokio-util = "0.7"\n' >"$probe/cli/Cargo.toml"
  verdict=$(scan_runtime_edges "$probe")
  rm -rf "$probe"
  missed=""
  for expected in "rayon on artifact" "tokio-util on render" "rayon on metastore"; do
    printf '%s\n' "$verdict" | grep -q "^$expected " || missed="$missed$expected
"
  done
  if [ -z "$missed" ]; then
    pass "scope: the scan flags each dependency planted outside its own allowlist (non-vacuous)"
  else
    bad "scope: the scan missed planted violations — it is vacuous for:"
    printf '%s' "$missed" | sed 's/^/        /'
  fi
  if printf '%s\n' "$verdict" | grep -q ' on cli \|^tokio on metastore '; then
    bad "scope: the scan flags an allowlisted placement — the allowlist is not being honored"
  else
    pass "scope: the scan leaves the allowlisted placements alone (legitimate edges are named, not hidden)"
  fi
fi
# Called out separately from the allowlist because the message carries the
# commitment: core's failure here ends the zero-runtime-dependency guarantee.
if grep -qiE '^[[:space:]]*(tokio|rayon|tokio-util)[[:space:]]*=' crates/core/Cargo.toml 2>/dev/null; then
  bad "scope: dagr-core must NOT depend on tokio/rayon/tokio-util — its runtime dependency set is empty (ADR 081/082)"
else
  pass "scope: dagr-core carries no tokio/rayon/tokio-util edge (zero-runtime-dependency guarantee)"
fi
# The compensating positive half, which this checker lacked: a decision nothing
# implements is not a decision, so the machinery must actually be wired into the
# crate the ADR places it in.
if grep -qiE '^[[:space:]]*tokio[[:space:]]*=' crates/cli/Cargo.toml 2>/dev/null \
   && grep -qiE '^[[:space:]]*rayon[[:space:]]*=' crates/cli/Cargo.toml 2>/dev/null; then
  pass "scope: tokio and rayon are both wired into dagr-cli, the crate the ADR places them in"
else
  bad "scope: dagr-cli does not wire both tokio and rayon — the ADR's timeout/permit machinery is unimplemented"
fi

# --- No spike code leaked into the workspace ---------------------------------
if find crates -type d -name '*spike*' 2>/dev/null | grep -q .; then
  bad "scope: a spike directory leaked into crates/"
else
  pass "scope: no spike directory under crates/"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
