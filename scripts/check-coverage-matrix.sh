#!/usr/bin/env bash
# Acceptance-criteria COVERAGE-matrix verifier — ticket 006 (T7).
#
# This is the enforcement half of arch.md system-level acceptance criterion 8:
# "every machine-classed criterion is covered by an automated test, and that
# coverage is itself verified in CI from a checked-in criteria matrix."
#
# BOUNDARY. There are two distinct artifacts, and this script owns only the
# second:
#   * docs/criteria-matrix.md   — the PARTITION (ticket T0.10): each arch.md
#     criterion labelled machine / human / disclaimer. Authoritative; not
#     rewritten here. This script CONSUMES it as the required-id set and the
#     per-criterion class.
#   * docs/coverage-matrix.md   — the COVERAGE matrix (this ticket): binds each
#     machine criterion to a required TEST ID or an explicit `unmapped`
#     placeholder (allowed until the covering test ships), plus the owning
#     ticket. This script VERIFIES it.
#
# It fails the build if:
#   (a) any criterion in the partition is absent from the coverage matrix;
#   (b) any criterion appears on more than one coverage-matrix row;
#   (c) any machine criterion is `unmapped` AND its covering test was supposed
#       to exist already — i.e. its owning ticket is this ticket (T7) or the id
#       carries no future owner (see "deferred vs owed" below);
#   (d) any machine criterion maps to a test id absent from the cargo suite.
# Human and disclaimer rows never require a test.
#
# Deferred vs owed. `unmapped` is the NORMAL early state (quality-gates.md §3):
# a machine criterion whose covering test ships in a LATER ticket is legitimately
# `unmapped` and carries that future ticket in its Covered-by column. What is an
# error is a machine criterion still `unmapped` whose covering test was owed by
# THIS ticket (T7) — those must be mapped now. The verifier treats T7 as the
# "owed here" sentinel; every other Covered-by ticket marks a deferral. (This
# resolves the tension between the ticket Objective's flat "fails on any unmapped
# machine criterion" and the DoD's explicit `unmapped` build-provenance row —
# recorded as an Open-question resolution in the ticket file.)
#
# Usage:
#   check-coverage-matrix.sh
#       Verify the real coverage matrix (docs/coverage-matrix.md) against the
#       real partition (docs/criteria-matrix.md) and the real cargo suite.
#   check-coverage-matrix.sh --matrix M --required-ids F --tests-from T
#       Hermetic mode for the self-tests: read the coverage matrix from M, the
#       required-id set (lines "<id> <class>") from F, and the set of existing
#       test ids from T (one per line). No cargo invocation.
#
# Exit: 0 = matrix is complete and every mapping is sound; 1 = a violation;
# 2 = a usage or missing-input error.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

matrix="docs/coverage-matrix.md"
partition="docs/criteria-matrix.md"
required_ids=""      # optional fixture: lines "<id> <class>"
tests_from=""        # optional fixture: existing test ids, one per line
owner_ticket="T7"    # the "owed here" sentinel; any other owner is a deferral
# --- Platform-conditional annotation set (T70, ticket 077) -------------------
# arch.md "Platform support" (lines 627-633) makes limit detection, signal
# handling, and flush behaviour platform-conditional acceptance criteria, "named
# as such in the coverage matrix". The T70 CI matrix (Linux tier-1 + macOS core)
# keys off that tag, so the tag is load-bearing, not decorative. This is the
# set of criterion ids whose Platform cell MUST read `platform-conditional`:
#   C12 — container/cgroup limit detection (Linux tier-1 vs macOS host-fallback)
#   C16 — OS signal handling + final flush (unix; non-unix is a documented no-op)
#   C19 — event-stream flush/fsync (per-platform fsync semantics)
# Overridable so the hermetic self-tests can inject their own synthetic set.
platform_conditional_ids="C12 C16 C19"
# The single accepted platform tag and the "not platform-conditional" marker.
platform_tag="platform-conditional"

while [ $# -gt 0 ]; do
  case "$1" in
    --matrix)                matrix=$2; shift 2 ;;
    --required-ids)          required_ids=$2; shift 2 ;;
    --tests-from)            tests_from=$2; shift 2 ;;
    --owner)                 owner_ticket=$2; shift 2 ;;
    --platform-conditional)  platform_conditional_ids=$2; shift 2 ;;
    *) echo "usage: check-coverage-matrix.sh [--matrix M] [--required-ids F] [--tests-from T] [--owner TID] [--platform-conditional \"IDS\"]" >&2; exit 2 ;;
  esac
done

fail=0
err() { printf 'FAIL  %s\n' "$1"; fail=1; }
note() { printf '      %s\n' "$1"; }

[ -f "$matrix" ] || { echo "FAIL  coverage matrix not found: $matrix"; exit 2; }

# --- 1. The required-criterion id set + each id's authoritative class ---------
# From the fixture file if given, else derived from the partition
# docs/criteria-matrix.md. The partition's row ids are C1..C28 and the
# system-level ids SL1,SL2,SL3,SL4a,SL4b,SL4c,SL5,SL6,SL7,SL8machine,SL8human
# (arch.md criterion 4 and 8 split into sub-parts; see criteria-matrix.md
# "Granularity"). We read each id's class from the partition's 2nd table cell so
# the coverage matrix cannot silently reclassify a criterion.
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
req="$tmp/required.txt"   # "<id> <class>" per line

if [ -n "$required_ids" ]; then
  [ -f "$required_ids" ] || { echo "FAIL  required-ids file not found: $required_ids"; exit 2; }
  grep -vE '^[[:space:]]*$' "$required_ids" >"$req"
else
  [ -f "$partition" ] || { echo "FAIL  partition not found: $partition"; exit 2; }
  # The canonical id set (static, from criteria-matrix.md "Granularity").
  ids="SL1 SL2 SL3 SL4a SL4b SL4c SL5 SL6 SL7 SL8machine SL8human"
  for n in $(seq 1 28); do ids="$ids C$n"; done
  : >"$req"
  for id in $ids; do
    # The partition classifies each id in the 2nd cell of its "| <id> | class |"
    # row. Read it so this verifier never invents a class.
    class=$(grep -E "^\|[[:space:]]*$id[[:space:]]*\|" "$partition" | head -1 \
            | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$3); print $3}')
    case "$class" in
      machine|human|disclaimer) printf '%s %s\n' "$id" "$class" >>"$req" ;;
      *) err "partition: criterion $id has no machine/human/disclaimer class in $partition (found '$class')" ;;
    esac
  done
fi

# --- 2. The set of existing test ids -----------------------------------------
tests_file="$tmp/tests.txt"
if [ -n "$tests_from" ]; then
  [ -f "$tests_from" ] || { echo "FAIL  tests-from file not found: $tests_from"; exit 2; }
  cp "$tests_from" "$tests_file"
else
  # `cargo test -- --list` prints "path::to::test: test" (and "...: bench").
  # Keep the id, drop the ": test" suffix. Duplicate ids across crates collapse
  # to a set — a mapped id existing in ANY crate satisfies the reference.
  if ! cargo test --workspace -- --list >"$tmp/raw-list.txt" 2>/dev/null; then
    echo "FAIL  could not enumerate the cargo test suite (cargo test --workspace -- --list failed)"
    exit 2
  fi
  sed -nE 's/^([A-Za-z0-9_:]+): (test|bench)$/\1/p' "$tmp/raw-list.txt" \
    | sort -u >"$tests_file"
fi
test_exists() { grep -qxF "$1" "$tests_file"; }

# --- 3. Parse the coverage-matrix rows ---------------------------------------
# A data row is "| <id> | <class> | <platform> | <test> | <covered-by> | ...".
# The header row (| Criterion |) and the |---| separator are skipped because
# their first cell is not a known criterion id. We accumulate, per id, its row
# count (for the duplicate check) and the parsed cells of its first row.
#
# Portability: macOS ships bash 3.2, which has no associative arrays. We key by
# id through a flat file, one line per parsed row:
# "<id>\t<class>\t<test>\t<cover>\t<platform>".
rows="$tmp/rows.tsv"; : >"$rows"
# A row is a data row iff its first cell is a criterion-id token: non-empty, no
# whitespace, not the literal header word "Criterion", and not a "---" table
# separator. This keeps the parser id-scheme-agnostic (the hermetic self-tests
# use synthetic ids) AND lets section 5 still catch a row whose id is not a real
# criterion — an invented row is parsed here and rejected there.
is_required_id() { awk -v want="$1" '$1==want{f=1} END{exit f?0:1}' "$req"; }
while IFS= read -r line; do
  case "$line" in
    \|*) ;; *) continue ;;
  esac
  id=$(printf '%s' "$line" | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}')
  # A first cell is a criterion-row id iff it is either one of the required ids
  # (covers the self-tests' synthetic ids) OR matches the criterion-id shape
  # C<n> / SL<suffix>. This skips this document's own explanatory tables (whose
  # first cells are "Column", "**Criterion**", …) and the |---| separators,
  # while still parsing a real-but-invented criterion-shaped id so section 5 can
  # reject it.
  if ! is_required_id "$id"; then
    case "$id" in
      C[0-9]*|SL[0-9A-Za-z]*) : ;;   # criterion-id shaped
      *) continue ;;
    esac
  fi
  cls=$(printf '%s'  "$line" | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$3); print $3}')
  plat=$(printf '%s' "$line" | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$4); print $4}')
  test=$(printf '%s' "$line" | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$5); print $5}')
  cover=$(printf '%s' "$line" | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$6); print $6}')
  printf '%s\t%s\t%s\t%s\t%s\n' "$id" "$cls" "$test" "$cover" "$plat" >>"$rows"
done <"$matrix"

# Per-id accessors over the flat file (first matching row wins for the cells).
row_count() { awk -F'\t' -v id="$1" '$1==id{c++} END{print c+0}' "$rows"; }
row_class() { awk -F'\t' -v id="$1" '$1==id{print $2; exit}' "$rows"; }
row_test()  { awk -F'\t' -v id="$1" '$1==id{print $3; exit}' "$rows"; }
row_cover() { awk -F'\t' -v id="$1" '$1==id{print $4; exit}' "$rows"; }
row_plat()  { awk -F'\t' -v id="$1" '$1==id{print $5; exit}' "$rows"; }

# --- 4. Totality + duplicate + mapping checks --------------------------------
n_machine=0; n_human=0; n_disc=0
while read -r id class; do
  [ -n "$id" ] || continue
  cnt=$(row_count "$id")
  if [ "$cnt" -eq 0 ]; then
    err "criterion $id is present in the partition but ABSENT from the coverage matrix ($matrix)"
    continue
  fi
  if [ "$cnt" -gt 1 ]; then
    err "criterion $id appears $cnt times in the coverage matrix; it must appear exactly once (no duplicates)"
  fi
  # The coverage matrix must not reclassify a criterion away from the partition.
  mcls=$(row_class "$id")
  if [ "$mcls" != "$class" ]; then
    err "criterion $id is '$mcls' in the coverage matrix but '$class' in the partition ($partition); classes must agree"
  fi
  case "$class" in
    machine)
      n_machine=$((n_machine + 1))
      test=$(row_test "$id")
      cover=$(row_cover "$id")
      if [ -z "$test" ]; then
        err "machine criterion $id has an empty Test cell; a machine criterion must map to a test id or the explicit 'unmapped' placeholder"
      elif [ "$test" = "unmapped" ]; then
        # Deferred is allowed; owed-here is not. Owed-here = owning ticket is
        # this ticket, or no future owner is named.
        if [ -z "$cover" ] || [ "$cover" = "$owner_ticket" ] || [ "$cover" = "—" ] || [ "$cover" = "-" ]; then
          err "machine criterion $id is 'unmapped' but owed by this ticket ($owner_ticket) — a machine criterion owed here must map to a test (Covered-by='$cover')"
        fi
        # else: deferred to a future ticket — allowed, the normal early state.
      else
        # A concrete test id: it must exist in the suite (no dangling reference).
        if ! test_exists "$test"; then
          err "machine criterion $id maps to test id '$test', which does not exist in the test suite (dangling reference)"
        fi
      fi
      ;;
    human)      n_human=$((n_human + 1)) ;;
    disclaimer) n_disc=$((n_disc + 1)) ;;
  esac
done <"$req"

# --- 4b. Platform-conditional annotation (T70, ticket 077) -------------------
# arch.md "Platform support" requires the limit-detection (C12), signal-handling
# (C16), and flush (C19) criteria to be *named as platform-conditional* in the
# coverage matrix; the T70 Linux-tier-1 / macOS-core CI matrix reads that tag.
# This makes the annotation ENFORCED, not decorative (Test plan: "Matrix checker
# fails on an untagged platform-conditional criterion"):
#   * every id in $platform_conditional_ids MUST carry the `platform-conditional`
#     tag in its Platform cell — a missing/blank/wrong tag fails the build, naming
#     the criterion;
#   * conversely NO other criterion may carry the tag (a stray tag is as much a
#     drift as a missing one), so the annotation stays a faithful, reviewable
#     record of exactly which criteria are platform-conditional.
# The Linux-only machine criteria (e.g. C12 cgroup detection) stay MAPPED to their
# Linux tests above (section 4); tagging them platform-conditional records the
# platform dimension WITHOUT dropping the mapping, so the unmapped-machine gate
# stays green (Test plan: "macOS-excluded criterion is recorded, not silently
# dropped").
is_platform_conditional() {
  for want in $platform_conditional_ids; do [ "$1" = "$want" ] && return 0; done
  return 1
}
cut -f1 "$rows" | sort -u | while read -r id; do
  [ -n "$id" ] || continue
  # Only consider ids that are real criteria in the partition (section 5 handles
  # invented rows); read this row's Platform cell.
  is_required_id "$id" || continue
  plat=$(row_plat "$id")
  if is_platform_conditional "$id"; then
    if [ "$plat" != "$platform_tag" ]; then
      echo "FAIL  platform-conditional criterion $id must carry the '$platform_tag' tag in its Platform cell (found '$plat'); arch.md Platform support requires limit-detection/signal-handling/flush criteria to be named platform-conditional (T70)"
    fi
  else
    if [ "$plat" = "$platform_tag" ]; then
      echo "FAIL  criterion $id is tagged '$platform_tag' but is not in the platform-conditional set ($platform_conditional_ids); a stray platform tag misrepresents the platform matrix (T70)"
    fi
  fi
done >"$tmp/platform.txt"
if [ -s "$tmp/platform.txt" ]; then cat "$tmp/platform.txt"; fail=1; fi

# --- 5. Coverage-matrix rows that are not required criteria ------------------
# A row whose id is not in the partition means the coverage matrix invented a
# criterion; flag it so the two files cannot drift apart.
cut -f1 "$rows" | sort -u | while read -r id; do
  [ -n "$id" ] || continue
  if ! awk -v want="$id" '$1==want{found=1} END{exit found?0:1}' "$req"; then
    echo "FAIL  coverage matrix row '$id' is not a criterion in the partition ($partition); the coverage matrix must not invent criteria"
  fi
done >"$tmp/extra.txt"
if [ -s "$tmp/extra.txt" ]; then cat "$tmp/extra.txt"; fail=1; fi

# --- Summary ------------------------------------------------------------------
if [ "$fail" -eq 0 ]; then
  echo "PASS  coverage matrix complete and sound"
  echo "      machine=$n_machine  human=$n_human  disclaimer=$n_disc  (total=$((n_machine + n_human + n_disc)))"
  exit 0
else
  note "coverage-matrix verification failed; see FAIL lines above"
  exit 1
fi
