#!/usr/bin/env bash
# Self-tests for the rust-skills adoption-register verifier
# (scripts/check-rust-skills-adoption.sh), ticket 107 (T92).
#
# These are the ticket's Test-plan scenarios for the *tooling* this ticket
# delivers — the register verifier — expressed as a fixture-driven harness, in
# the same shape as scripts/check-coverage-verifier-selftest.sh (ticket 006).
# Each case builds a fixture rules directory and a fixture register (a good one,
# or one with a single injected defect) and asserts the verifier's exit code and
# that its diagnostic names the offending rule or row.
#
# Written FIRST (TDD): before scripts/check-rust-skills-adoption.sh exists this
# harness fails every case, because the verifier it invokes is absent. It goes
# green once the verifier is authored to the contract these cases pin down.
#
# The harness is hermetic — it never reads the real 265-rule skill directory or
# the real register. The verifier is run with --rules-dir / --register /
# --m9-tickets pointing at fixtures, so the self-tests are fast and independent
# of how the real register happens to be dispositioned today. Proving the
# verifier passes against the REAL checked-in register is a separate concern,
# covered by the CI job that runs it with no flags.
#
# Run from the repository root. Exit 0 = every self-test holds, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

verifier="scripts/check-rust-skills-adoption.sh"

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

if [ ! -x "$verifier" ]; then
  bad "verifier: $verifier is missing or not executable"
  echo "SOME SELF-TESTS FAILED"
  exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# ---------------------------------------------------------------------------
# Fixture rules directory: four rule ids standing in for the real 265, one per
# disposition the vocabulary allows. Rule ids are file basenames minus `.md`,
# exactly as in the real skill.
# ---------------------------------------------------------------------------
mk_rules() { # mk_rules <dir>
  mkdir -p "$1"
  for id in aa-satisfied-rule bb-adopt-rule cc-na-rule dd-declined-rule; do
    printf '# %s\n' "$id" >"$1/$id.md"
  done
}

# The fixture's M9 ticket set — what an `adopt` row is allowed to name.
m9="T92 T93 T99"

# ---------------------------------------------------------------------------
# `write_good_register <path>` emits a complete, valid fixture: every fixture
# rule id present exactly once, one row per disposition, a reason on every row,
# and the one `adopt` row naming a ticket inside the M9 set.
# ---------------------------------------------------------------------------
write_good_register() { # write_good_register <path>
  cat >"$1" <<'EOF'
# fixture rust-skills register

| Rule | Category | Disposition | Ticket | Reason |
|---|---|---|---|---|
| aa-satisfied-rule | aa | satisfied | — | already enforced by the denied clippy groups |
| bb-adopt-rule | bb | adopt | T93 | the profiles ticket sets this |
| cc-na-rule | cc | n-a | — | needs a runtime dependency in a zero-dependency crate |
| dd-declined-rule | dd | declined | — | applicable but trades away a determinism guarantee |
EOF
}

run_verifier() { # run_verifier <register> <rules-dir> : prints output, returns rc
  "$verifier" --register "$1" --rules-dir "$2" --m9-tickets "$m9" 2>&1
}

mk_rules "$work/rules"
write_good_register "$work/good.md"

# ---------------------------------------------------------------------------
# Case 1 — A complete, well-formed register passes, and reports a
# per-disposition count summary (so a reviewer can see the shape at a glance).
# ---------------------------------------------------------------------------
out=$(run_verifier "$work/good.md" "$work/rules"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "complete register passes (exit 0)"
else
  bad "complete register should pass but exited $rc; output: $out"
fi
if printf '%s' "$out" | grep -qi 'satisfied' \
   && printf '%s' "$out" | grep -qi 'adopt' \
   && printf '%s' "$out" | grep -qi 'n-a' \
   && printf '%s' "$out" | grep -qi 'declined'; then
  pass "complete register prints a per-disposition count summary"
else
  bad "complete register must print a per-disposition summary; output: $out"
fi
# The count must be DERIVED from the rules directory, never hard-coded: the real
# register has 265 rows today and must not need a script edit when that changes.
if printf '%s' "$out" | grep -q '4'; then
  pass "the rule total is derived from the rules directory (4 fixture rules)"
else
  bad "the verifier must report a derived rule total; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 2 — A rule present in rules/ but ABSENT from the register fails.
# This is the failure mode that matters most: it is how a rule gets silently
# skipped instead of deliberately dispositioned.
# ---------------------------------------------------------------------------
grep -v '^| cc-na-rule ' "$work/good.md" >"$work/absent.md"
out=$(run_verifier "$work/absent.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "rule absent from the register fails (nonzero exit)"
else
  bad "an absent rule should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'cc-na-rule' \
   && printf '%s' "$out" | grep -qiE 'absent|missing|not in the register'; then
  pass "absent-rule error names cc-na-rule and says it is absent"
else
  bad "absent-rule error must name cc-na-rule as absent; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 3 — A rule listed on MORE THAN ONE row fails (contradictory
# dispositions are worse than none).
# ---------------------------------------------------------------------------
{ cat "$work/good.md"
  echo '| aa-satisfied-rule | aa | declined | — | a second, contradictory verdict |'
} >"$work/dup.md"
out=$(run_verifier "$work/dup.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "duplicate rule row fails (nonzero exit)"
else
  bad "a duplicate rule row should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'aa-satisfied-rule' \
   && printf '%s' "$out" | grep -qiE 'duplicat|more than once|exactly once'; then
  pass "duplicate-rule error names aa-satisfied-rule and says exactly once"
else
  bad "duplicate-rule error must name the duplicated rule; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 4 — A register row naming a rule id that does not exist in rules/ fails
# (a dangling reference — typically a typo, or a rule renamed upstream).
# ---------------------------------------------------------------------------
{ cat "$work/good.md"
  echo '| zz-no-such-rule | zz | satisfied | — | refers to a rule that does not exist |'
} >"$work/dangling.md"
out=$(run_verifier "$work/dangling.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "dangling rule reference fails (nonzero exit)"
else
  bad "a dangling rule reference should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'zz-no-such-rule' \
   && printf '%s' "$out" | grep -qiE 'dangling|unknown|no such|does not exist'; then
  pass "dangling-reference error names zz-no-such-rule"
else
  bad "dangling-reference error must name zz-no-such-rule; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 5 — An `n-a` or `declined` row with an EMPTY reason fails. A bare
# verdict is exactly the thing this register exists to prevent, so the reason
# is load-bearing, not decorative.
# ---------------------------------------------------------------------------
sed 's/^| cc-na-rule .*/| cc-na-rule | cc | n-a | — | — |/' "$work/good.md" >"$work/noreason.md"
out=$(run_verifier "$work/noreason.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "n-a row without a reason fails (nonzero exit)"
else
  bad "an n-a row without a reason should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'cc-na-rule' \
   && printf '%s' "$out" | grep -qiE 'reason'; then
  pass "empty-reason error names cc-na-rule and says a reason is required"
else
  bad "empty-reason error must name cc-na-rule and mention the reason; output: $out"
fi

sed 's/^| dd-declined-rule .*/| dd-declined-rule | dd | declined | — |  |/' "$work/good.md" >"$work/noreason2.md"
out=$(run_verifier "$work/noreason2.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q 'dd-declined-rule'; then
  pass "declined row with a blank reason fails and names the row"
else
  bad "a declined row with a blank reason must fail naming it; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 6 — An `adopt` row naming a ticket OUTSIDE the M9 set fails. An adopt
# row is a promise that some ticket does the work; pointing at a ticket that
# does not exist makes the promise unkeepable and unverifiable at the M9 gate.
# ---------------------------------------------------------------------------
sed 's/^| bb-adopt-rule .*/| bb-adopt-rule | bb | adopt | T42 | names a ticket outside M9 |/' "$work/good.md" >"$work/badticket.md"
out=$(run_verifier "$work/badticket.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "adopt row naming a non-M9 ticket fails (nonzero exit)"
else
  bad "an adopt row naming a non-M9 ticket should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'bb-adopt-rule' \
   && printf '%s' "$out" | grep -qiE 'ticket'; then
  pass "bad-ticket error names bb-adopt-rule and mentions the ticket"
else
  bad "bad-ticket error must name bb-adopt-rule; output: $out"
fi

# An `adopt` row with NO ticket at all is the same defect.
sed 's/^| bb-adopt-rule .*/| bb-adopt-rule | bb | adopt | — | adopt with no owning ticket |/' "$work/good.md" >"$work/noticket.md"
out=$(run_verifier "$work/noticket.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q 'bb-adopt-rule'; then
  pass "adopt row with no owning ticket fails and names the row"
else
  bad "an adopt row with no ticket must fail naming it; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 7 — An unrecognised disposition fails. The vocabulary is closed
# (satisfied / adopt / n-a / declined); a fifth word would silently opt a rule
# out of every check above.
# ---------------------------------------------------------------------------
sed 's/^| aa-satisfied-rule .*/| aa-satisfied-rule | aa | probably-fine | — | not a real disposition |/' "$work/good.md" >"$work/baddisp.md"
out=$(run_verifier "$work/baddisp.md" "$work/rules"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "unrecognised disposition fails (nonzero exit)"
else
  bad "an unrecognised disposition should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'aa-satisfied-rule' \
   && printf '%s' "$out" | grep -qiE 'disposition'; then
  pass "bad-disposition error names the row and mentions the disposition"
else
  bad "bad-disposition error must name aa-satisfied-rule; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 8 — The verifier defaults to the REAL register and the REAL rules
# directory when given no flags. This is how CI invokes it, so the default must
# not silently resolve to nothing and report success.
# ---------------------------------------------------------------------------
if grep -q 'rust-skills-register' "$verifier" \
   && grep -qE 'skills/rust-skills/rules' "$verifier"; then
  pass "verifier carries real default paths for the register and the rules dir"
else
  bad "verifier must default to the real register and rules directory"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL RUST-SKILLS VERIFIER SELF-TESTS PASSED"
  exit 0
else
  echo "SOME SELF-TESTS FAILED"
  exit 1
fi
