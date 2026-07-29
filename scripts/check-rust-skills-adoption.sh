#!/usr/bin/env bash
# Verifier for the rust-skills adoption register, ticket 107 (T92).
#
# The register (docs/rust-skills-register.md) records, for every rule in the
# `.claude/skills/rust-skills` skill, whether dagr already satisfies it, adopts
# it in a named M9 ticket, is structurally unable to apply it, or declines it —
# and WHY. This script is what stops that record from rotting into a decorative
# table: it fails CI when a rule is skipped, contradicted, dangling, or given a
# bare verdict with no reasoning.
#
# It mirrors scripts/check-coverage-matrix.sh in role and shape: a checked-in,
# review-owned data file plus a verifier that enforces totality against an
# authoritative source. Here the authoritative source is the rules directory
# itself — the set of rule ids is whatever `rules/*.md` contains, never a number
# written down in this script. Adding a rule upstream therefore fails the build
# until it is dispositioned, which is the intended behaviour.
#
# Checks (each has a fixture self-test in
# scripts/check-rust-skills-verifier-selftest.sh):
#   1. every rule id in the rules directory appears in the register;
#   2. no rule id appears on more than one row;
#   3. no register row names a rule id absent from the rules directory;
#   4. every row's disposition is one of satisfied | adopt | n-a | declined;
#   5. every row carries a non-empty reason;
#   6. every `adopt` row names a ticket in the M9 set.
#
# Check 5 is deliberately STRICTER than the ticket's Definition of done, which
# requires a reason only on `n-a` and `declined` rows. A `satisfied` claim with
# no stated basis is the one the M9 acceptance gate (T99) has to spot-verify by
# hand, and an unexplained `adopt` is just as unhelpful — so the reason column is
# mandatory everywhere. Loosening it would make the register cheaper to write and
# worthless to review.
#
# Usage: check-rust-skills-adoption.sh [--register PATH] [--rules-dir PATH]
#                                      [--m9-tickets "T92 T93 ..."]
# The flags exist so the self-tests can run hermetically against fixtures; CI
# invokes it with no arguments and gets the real paths below.
#
# Run from the repository root. Exit 0 = the register is complete and
# well-formed, 1 = at least one defect, 2 = bad invocation.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

# Real defaults (self-test case 8 asserts these are present).
register="docs/rust-skills-register.md"
rules_dir=".claude/skills/rust-skills/rules"
m9_tickets="T92 T93 T94 T95 T96 T97 T98 T99"

while [ $# -gt 0 ]; do
  case "$1" in
    --register)   register="${2:?--register needs a path}"; shift 2 ;;
    --rules-dir)  rules_dir="${2:?--rules-dir needs a path}"; shift 2 ;;
    --m9-tickets) m9_tickets="${2:?--m9-tickets needs a list}"; shift 2 ;;
    -h|--help)    sed -n '1,40p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

[ -f "$register" ]  || { echo "FAIL  register not found: $register" >&2; exit 1; }
[ -d "$rules_dir" ] || { echo "FAIL  rules directory not found: $rules_dir" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# --- The authoritative rule-id set: the rules directory, not a literal --------
find "$rules_dir" -maxdepth 1 -name '*.md' -type f -exec basename {} .md \; \
  | LC_ALL=C sort >"$work/rule_ids"
rule_total=$(wc -l <"$work/rule_ids" | tr -d ' ')
if [ "$rule_total" -eq 0 ]; then
  bad "rules: no rule files found under $rules_dir (nothing to verify against)"
  echo "REGISTER=FAIL"
  exit 1
fi

# --- Parse the register's table rows ----------------------------------------
# A row is `| rule | category | disposition | ticket | reason |`. The header and
# its `|---|` separator are skipped by requiring column 1 to be a rule-shaped
# token (no spaces) that is not the literal header word.
awk -F'|' '
  /^[[:space:]]*\|/ {
    rule = $2; disp = $4; ticket = $5; reason = $6
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", rule)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", disp)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", ticket)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", reason)
    if (rule == "" || rule == "Rule") next          # header
    if (rule ~ /^-+$/) next                          # separator
    if (rule ~ /[[:space:]]/) next                   # prose, not a row
    printf "%s\t%s\t%s\t%s\n", rule, disp, ticket, reason
  }
' "$register" >"$work/rows"

row_total=$(wc -l <"$work/rows" | tr -d ' ')
if [ "$row_total" -eq 0 ]; then
  bad "register: no rule rows parsed from $register (is the table shaped | Rule | Category | Disposition | Ticket | Reason |?)"
  echo "REGISTER=FAIL"
  exit 1
fi

cut -f1 "$work/rows" | LC_ALL=C sort >"$work/row_ids"

# --- Check 1: every rule id is dispositioned ---------------------------------
if missing=$(LC_ALL=C comm -23 "$work/rule_ids" "$work/row_ids") && [ -z "$missing" ]; then
  pass "totality: all $rule_total rule ids appear in the register"
else
  n=$(printf '%s\n' "$missing" | grep -c . || true)
  bad "totality: $n rule id(s) are absent from the register (every rule must be dispositioned exactly once):"
  printf '%s\n' "$missing" | sed 's/^/        absent: /'
fi

# --- Check 2: no rule id dispositioned twice ---------------------------------
if dupes=$(LC_ALL=C uniq -d "$work/row_ids") && [ -z "$dupes" ]; then
  pass "uniqueness: no rule id appears on more than one row"
else
  bad "uniqueness: rule id(s) appear more than once — a rule must be dispositioned exactly once:"
  printf '%s\n' "$dupes" | sed 's/^/        duplicated: /'
fi

# --- Check 3: no dangling rule reference -------------------------------------
if dangling=$(LC_ALL=C comm -13 "$work/rule_ids" "$work/row_ids") && [ -z "$dangling" ]; then
  pass "references: every register row names an existing rule file"
else
  bad "references: row(s) name a rule id with no such file in $rules_dir (dangling reference — does not exist):"
  printf '%s\n' "$dangling" | sed 's/^/        dangling: /'
fi

# --- Checks 4-6: per-row disposition, reason, and ticket ---------------------
bad_disp=0; bad_reason=0; bad_ticket=0
while IFS=$'\t' read -r rule disp ticket reason; do
  [ -n "$rule" ] || continue

  case "$disp" in
    satisfied|adopt|n-a|declined) ;;
    *)
      bad "disposition: $rule has an unrecognised disposition '$disp' (must be one of satisfied, adopt, n-a, declined)"
      bad_disp=$((bad_disp + 1))
      continue
      ;;
  esac

  # A reason of empty, `—`, `-`, or `n/a` is no reason at all.
  case "$reason" in
    ''|'—'|'-'|'--'|'n/a'|'N/A'|'TBD'|'tbd')
      bad "reason: $rule ($disp) carries no reason — every row must state its reason"
      bad_reason=$((bad_reason + 1))
      ;;
  esac

  if [ "$disp" = "adopt" ]; then
    case " $m9_tickets " in
      *" $ticket "*) ;;
      *)
        bad "ticket: $rule is dispositioned 'adopt' but its ticket '$ticket' is not an M9 ticket ($m9_tickets)"
        bad_ticket=$((bad_ticket + 1))
        ;;
    esac
  fi
done <"$work/rows"

[ "$bad_disp" -eq 0 ]   && pass "disposition: every row uses the closed vocabulary (satisfied, adopt, n-a, declined)"
[ "$bad_reason" -eq 0 ] && pass "reason: every row states a reason"
[ "$bad_ticket" -eq 0 ] && pass "ticket: every 'adopt' row names an M9 ticket"

# --- Summary (derived, never hard-coded) -------------------------------------
n_sat=$(cut -f2 "$work/rows" | grep -cx 'satisfied' || true)
n_ado=$(cut -f2 "$work/rows" | grep -cx 'adopt' || true)
n_na=$(cut -f2 "$work/rows"  | grep -cx 'n-a' || true)
n_dec=$(cut -f2 "$work/rows" | grep -cx 'declined' || true)

printf 'RULES_TOTAL=%s\n' "$rule_total"
printf 'ROWS_TOTAL=%s\n' "$row_total"
printf 'satisfied=%s adopt=%s n-a=%s declined=%s\n' "$n_sat" "$n_ado" "$n_na" "$n_dec"

if [ "$fail" -eq 0 ]; then
  echo "REGISTER=PASS"
  exit 0
else
  echo "REGISTER=FAIL"
  exit 1
fi
