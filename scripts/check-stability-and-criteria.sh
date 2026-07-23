#!/usr/bin/env bash
# Stability-policy + criteria-partition acceptance checks for ticket 005 (T0.10).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/005-T0.10-stability-policy-and-criteria-partition.md,
# section "Test plan"). T0.10 is a DECISION ticket whose deliverables are (a) an
# ADR embedded in its own ticket file at the path the DoD names — matching the
# T1/T2/T0.6/T3/T4/T0.7 precedent of keeping each ADR inside its own ticket file
# — and (b) a checked-in, review-owned criteria matrix that classifies every
# arch.md acceptance criterion as machine / human / disclaimer.
#
# The ticket's Test plan is documentary (does the ADR say X; does the matrix
# classify Y). Authored FIRST as the acceptance gate, this script fails on the
# tree as it stands before the ADR and matrix are written, and passes once both
# record every decision and classification the ticket is chartered to lock. It
# builds NO CI workflow and NO matrix-enforcement tooling (that is T7); it only
# asserts the content of the two deliverables this ticket owns.
#
# Run from the repository root. Exit 0 = every assertion holds, 1 = a failure,
# 2 = a required file is missing.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

adr="docs/implementation/005-T0.10-stability-policy-and-criteria-partition.md"
matrix="docs/criteria-matrix.md"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -f "$adr" ]; then
  echo "FAIL  ticket file missing: $adr"
  exit 2
fi

# The ADR is embedded BELOW the six ticket sections, under its own 'ADR:'
# heading (the T1/T2/T0.6/T3/T4/T0.7 precedent: '# ADR: <title>'). The ticket
# prose above it (Objective, Test plan, DoD) already mentions MSRV, tokio,
# additive-only, etc. — so a whole-file grep would pass content checks the ADR
# itself has not yet made. We therefore scope every ADR content assertion to the
# ADR BODY only: the slice of the file from the first 'ADR:' heading to EOF.
# The ticket's own H1 title ('# … partition') is deliberately NOT matched (no
# 'ADR:' colon), so before the embedded ADR is authored the slice is empty and
# every content check fails — exactly the tests-first behaviour we want.
adr_body=$(awk '/^#+[[:space:]]+ADR:/ {found=1} found {print}' "$adr")
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
if printf '%s' "$adr_body" | grep -qiE 'accepted'; then
  pass "adr: recorded in an accepted status"
else
  bad "adr: the ADR must be in an accepted (not draft/proposed) status"
fi

# --- MSRV is stated and singular (Test plan: "MSRV is stated and singular";
# DoD line 1) -----------------------------------------------------------------
if has 'msrv' && has '1\.95\.0'; then
  pass "msrv: a single concrete minimum Rust version (1.95.0) is named"
else
  bad "msrv: a single concrete MSRV (1.95.0) must be named"
fi
if has 'workspace' && has 'readme'; then
  pass "msrv: workspace pin location and README documentation location stated"
else
  bad "msrv: the workspace-pin and README-documentation locations are missing"
fi
if has 'minor version bump' || has 'minor.version bump' || has 'minor bump'; then
  pass "msrv: 'raise = minor version bump, noted in release notes' rule recorded"
else
  bad "msrv: the raise=minor-bump-in-release-notes rule is missing"
fi

# --- Semver major-events list is closed and includes tokio (Test plan; DoD
# line 2) ---------------------------------------------------------------------
if has 'semantic version' || has 'semver'; then
  pass "semver: the authoring-API semantic-versioning contract is named"
else
  bad "semver: the semantic-versioning contract must be named"
fi
if has 'keep compiling' || has 'keep.compiling' || has 'unchanged pipelines'; then
  pass "semver: within-a-major 'pipelines keep compiling' contract recorded"
else
  bad "semver: the within-a-major keep-compiling contract is missing"
fi
if has 'authoring.api'; then
  pass "semver: breaking the authoring API listed as a major event"
else
  bad "semver: authoring-API breakage as a major event is missing"
fi
if has 'tokio'; then
  pass "semver: replacing or major-bumping tokio listed as a major event"
else
  bad "semver: tokio replacement/major-bump as a major event is missing"
fi
if has 'recorded.artifact' || has 'artifact compatib' || has 'artifact incompat'; then
  pass "semver: recorded-artifact incompatibility listed as a major event"
else
  bad "semver: recorded-artifact incompatibility as a major event is missing"
fi

# --- Schema-evolution rule is additive-only and reader-tolerant (Test plan;
# DoD line 3) -----------------------------------------------------------------
if has 'additive.only'; then
  pass "schema: additive-only-within-a-version rule recorded"
else
  bad "schema: the additive-only-within-a-version rule is missing"
fi
for s in 'event stream' 'graph artifact' 'run artifact'; do
  if has "$s"; then
    pass "schema: '$s' named as a versioned schema"
  else
    bad "schema: the '$s' schema is not named"
  fi
done
if has 'ignore unknown' && (has 'default missing' || has 'defaults missing'); then
  pass "schema: reader contract (ignore unknown fields, default missing) recorded"
else
  bad "schema: the reader contract (ignore unknown / default missing) is missing"
fi
if has 'folding function' && has 'schema version'; then
  pass "schema: folding function declares which stream schema versions it reads"
else
  bad "schema: the folding-function schema-version declaration is missing"
fi
if has 'version bump' || has 'bump the version' || has 'bump the schema'; then
  pass "schema: a non-additive change requires a version bump"
else
  bad "schema: the non-additive-change-requires-a-version-bump rule is missing"
fi

# --- Fixture-corpus plan (Test plan; DoD line 4) -----------------------------
if has 'fixture corpus' || has 'fixture.corpus'; then
  pass "fixtures: a fixture corpus is named with an in-repo location"
else
  bad "fixtures: the in-repo fixture-corpus location is missing"
fi
if has 'per released schema version' || has 'per schema version' || has 'one artifact per'; then
  pass "fixtures: one frozen artifact per released schema version per artifact kind"
else
  bad "fixtures: the one-artifact-per-schema-version-per-kind rule is missing"
fi
if has 'ten.thousand' || has '10,000' || has '10000'; then
  pass "fixtures: the ten-thousand-attempt scale artifact is included"
else
  bad "fixtures: the ten-thousand-attempt scale artifact is missing"
fi
if has 'M3' && has 'seed'; then
  pass "fixtures: corpus seeded at M3"
else
  bad "fixtures: the M3 seeding point is missing"
fi
if has 'forever' && (has 'parse' || has 'parseable'); then
  pass "fixtures: standing forever-parse CI obligation recorded"
else
  bad "fixtures: the forever-parse CI obligation is missing"
fi

# --- Fingerprint-algorithm versioning (Test plan; DoD line 5) ----------------
if has 'algorithm' && (has 'version' || has 'versioned'); then
  pass "fingerprint: a versioned algorithm identifier is recorded"
else
  bad "fingerprint: the versioned-algorithm-identifier rule is missing"
fi
if has 'structural fingerprint' && has 'policy hash'; then
  pass "fingerprint: both the structural fingerprint and policy hash carry it"
else
  bad "fingerprint: structural-fingerprint + policy-hash coverage is missing"
fi
if has 'cross.toolchain'; then
  pass "fingerprint: cross-toolchain stability is a tested guarantee"
else
  bad "fingerprint: the cross-toolchain-stability guarantee is missing"
fi
if has 'cannot compare' && has 'topology'; then
  pass "fingerprint: 'cannot compare' distinct from 'topology differs' recorded"
else
  bad "fingerprint: the cannot-compare-vs-topology-differs distinction is missing"
fi
if has 'never.*false difference' || has 'not.*false difference' || has 'false difference'; then
  pass "fingerprint: mismatch never reads as a false difference"
else
  bad "fingerprint: the never-a-false-difference rule is missing"
fi

# --- Platform posture and platform-conditional criteria (Test plan: "Platform-
# conditional criteria are named"; DoD line 6) --------------------------------
if has 'tier.1' && has 'linux'; then
  pass "platform: tier-1 Linux posture recorded"
else
  bad "platform: the tier-1 Linux posture is missing"
fi
if has 'macos'; then
  pass "platform: dev-supported macOS posture recorded"
else
  bad "platform: the dev-supported macOS posture is missing"
fi
if has 'windows' && (has 'unsupported' || has 'not supported'); then
  pass "platform: Windows-unsupported-in-v1 posture recorded"
else
  bad "platform: the Windows-unsupported posture is missing"
fi
if has 'platform.conditional'; then
  pass "platform: platform-conditional criteria named as such in the ADR"
else
  bad "platform: the platform-conditional criteria are not named in the ADR"
fi

# --- Reshape hand-off is explicit (Test plan: "Reshape hand-off is explicit";
# DoD line 9) -----------------------------------------------------------------
for t in T7 T4 T39 T48 T65; do
  if has "$t\b"; then
    pass "handoff: downstream consumer $t is named in the ADR"
  else
    bad "handoff: downstream consumer $t is not named in the ADR"
  fi
done

# --- ADR links to the criteria matrix (Test plan: "ADR is committed in the
# accepted state"; DoD line 11) -----------------------------------------------
if has 'criteria-matrix\.md' || has 'criteria matrix'; then
  pass "handoff: the ADR links to the criteria matrix it produces"
else
  bad "handoff: the ADR does not link to the criteria matrix"
fi

# =============================================================================
# CRITERIA MATRIX CHECKS
# =============================================================================
if [ ! -f "$matrix" ]; then
  bad "matrix: the checked-in criteria matrix ($matrix) does not exist"
  echo "SOME FAILED"; exit 1
fi
mx=$(cat "$matrix")

# Every classification token used must be one of the three allowed classes.
# The matrix rows are of the form: | <id> | ... | machine|human|disclaimer | ...
# We extract the classification column and confirm the vocabulary is closed.

# --- Matrix totality: every criterion id appears exactly once (Test plan:
# "Matrix totality"; DoD line 7). The id set is C1..C28 and SL1..SL8. ----------
mrow() { # mrow <id> : print rows whose FIRST table cell is exactly <id>
  printf '%s\n' "$mx" | grep -E "^\|[[:space:]]*$1[[:space:]]*\|"
}

# The complete id set: C1..C28, and the system-level criteria at the granularity
# arch.md classifies them — SL1,SL2,SL3, then criterion 4 as its three sub-parts
# SL4a/SL4b/SL4c, SL5,SL6,SL7, then criterion 8 as its two sub-parts
# SL8machine/SL8human. Each classified id appears exactly once, as machine,
# human, or disclaimer; the split criteria (4, 8) carry no bare classified row —
# their sub-parts are the criterion ids (Test plan: "including the sub-parts of
# criterion 4 and criterion 8").
sl_ids="SL1 SL2 SL3 SL4a SL4b SL4c SL5 SL6 SL7 SL8machine SL8human"

for n in $(seq 1 28); do
  id="C$n"
  count=$(mrow "$id" | wc -l | tr -d ' ')
  if [ "$count" -eq 1 ]; then
    pass "matrix: criterion $id appears exactly once"
  else
    bad "matrix: criterion $id appears $count times (expected exactly 1)"
  fi
done
for id in $sl_ids; do
  count=$(mrow "$id" | wc -l | tr -d ' ')
  if [ "$count" -eq 1 ]; then
    pass "matrix: system-level criterion $id appears exactly once"
  else
    bad "matrix: system-level criterion $id appears $count times (expected exactly 1)"
  fi
done

# --- Every matrix row is classified as exactly one of the three classes ------
# Pull the second column (the class) of every criterion row and confirm it is a
# member of {machine, human, disclaimer}. A row whose class column is empty or
# an unknown token fails here.
class_of() { # class_of <id> : print the 2nd table cell of that id's row
  mrow "$1" | head -1 | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$3); print $3}'
}
for n in $(seq 1 28); do
  c=$(class_of "C$n")
  case "$c" in
    machine|human|disclaimer) pass "matrix: C$n classified as '$c'" ;;
    *) bad "matrix: C$n has an invalid/empty class '$c' (want machine|human|disclaimer)" ;;
  esac
done
for id in $sl_ids; do
  c=$(class_of "$id")
  case "$c" in
    machine|human|disclaimer) pass "matrix: $id classified as '$c'" ;;
    *) bad "matrix: $id has an invalid/empty class '$c' (want machine|human|disclaimer)" ;;
  esac
done

# --- Classification correctness against the spec's own labels (Test plan:
# "Classification correctness"; DoD line 8) -----------------------------------
# SL1 is machine (quickstart compiles/runs) PLUS human (thirty-minute
# walkthrough): its row must record both words.
sl1=$(mrow SL1 | head -1)
if printf '%s' "$sl1" | grep -qi 'machine' && printf '%s' "$sl1" | grep -qi 'human'; then
  pass "matrix: SL1 records both machine (quickstart) and human (walkthrough)"
else
  bad "matrix: SL1 must record both machine and human parts"
fi
# SL4 sub-parts: (a) machine-structural, (b) machine-interpretive, (c) disclaimer.
for sub in a b c; do
  count=$(printf '%s\n' "$mx" | grep -Ec "^\|[[:space:]]*SL4$sub[[:space:]]*\|")
  if [ "$count" -eq 1 ]; then
    pass "matrix: SL4$sub sub-part present exactly once"
  else
    bad "matrix: SL4$sub sub-part appears $count times (expected exactly 1)"
  fi
done
if printf '%s\n' "$mx" | grep -E '^\|[[:space:]]*SL4a[[:space:]]*\|' | grep -qi 'machine'; then
  pass "matrix: SL4a (structural determinism) is machine"; else
  bad "matrix: SL4a must be machine (structural determinism)"; fi
if printf '%s\n' "$mx" | grep -E '^\|[[:space:]]*SL4b[[:space:]]*\|' | grep -qi 'machine'; then
  pass "matrix: SL4b (interpretive determinism) is machine"; else
  bad "matrix: SL4b must be machine (interpretive determinism)"; fi
if printf '%s\n' "$mx" | grep -E '^\|[[:space:]]*SL4c[[:space:]]*\|' | grep -qi 'disclaimer'; then
  pass "matrix: SL4c (external-systems disclaimer) is disclaimer"; else
  bad "matrix: SL4c must be disclaimer (external systems)"; fi
# SL8 sub-parts: machine coverage-of-machine-criteria + human release-checklist.
for sub in machine human; do
  count=$(printf '%s\n' "$mx" | grep -Ec "^\|[[:space:]]*SL8$sub[[:space:]]*\|")
  if [ "$count" -eq 1 ]; then
    pass "matrix: SL8$sub sub-part present exactly once"
  else
    bad "matrix: SL8$sub sub-part appears $count times (expected exactly 1)"
  fi
done

# --- Disclaimer criterion carried unclassified-but-present (Test plan:
# "Disclaimer criterion is carried unclassified-but-present") ------------------
disc=$(printf '%s\n' "$mx" | grep -Eic '\|[[:space:]]*disclaimer[[:space:]]*\|')
if [ "$disc" -ge 1 ]; then
  pass "matrix: at least one 'disclaimer' row present (SL4c)"
else
  bad "matrix: the disclaimer row (SL4c) must be present and marked disclaimer"
fi

# --- Required human-classed members (Test plan / DoD line 8): the human set
# includes at least C24 (diagram readability), C21 (docs at point of use), the
# thirty-minute walkthrough (SL1 human part), and C1's declaration-readability. -
if [ "$(class_of C24)" = "human" ]; then
  pass "matrix: C24 (diagram readability) is human"; else
  bad "matrix: C24 (diagram readability) must be human"; fi
if [ "$(class_of C21)" = "human" ]; then
  pass "matrix: C21 (documentation-at-point-of-use) is human"; else
  bad "matrix: C21 (documentation-at-point-of-use) must be human"; fi

# --- Platform-conditional criteria flagged in the matrix (Test plan:
# "Platform-conditional criteria are named"; DoD line 10): limit detection C12,
# signal handling C16, flush behavior. Each row must carry a platform-
# conditional flag so the T70 platform matrix can gate them. -------------------
for id in C12 C16; do
  row=$(mrow "$id" | head -1)
  if printf '%s' "$row" | grep -qi 'platform.conditional'; then
    pass "matrix: $id flagged platform-conditional"
  else
    bad "matrix: $id must be flagged platform-conditional"
  fi
done
if printf '%s\n' "$mx" | grep -qi 'flush' && \
   printf '%s\n' "$mx" | grep -i 'flush' | grep -qi 'platform.conditional'; then
  pass "matrix: flush/fsync behavior flagged platform-conditional"
else
  bad "matrix: flush/fsync behavior must be flagged platform-conditional"
fi

# --- The matrix declares its three-class legend and is review-owned, not a
# runtime registry (Out of scope: not a live metadata store / scheduler input) -
if printf '%s\n' "$mx" | grep -qiE 'machine' && \
   printf '%s\n' "$mx" | grep -qiE 'human' && \
   printf '%s\n' "$mx" | grep -qiE 'disclaimer'; then
  pass "matrix: the three-class vocabulary (machine/human/disclaimer) is defined"
else
  bad "matrix: the three-class vocabulary legend is missing"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL PASS"
  exit 0
else
  echo "SOME FAILED"
  exit 1
fi
