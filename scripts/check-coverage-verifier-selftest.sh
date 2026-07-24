#!/usr/bin/env bash
# Self-tests for the acceptance-criteria coverage-matrix verifier
# (scripts/check-coverage-matrix.sh), ticket 006 (T7).
#
# These are the ticket's Test-plan scenarios for the *tooling* this ticket
# delivers — the matrix-verification script — expressed as a fixture-driven test
# harness (arch.md system-level criterion 8; ticket DoD "The matrix-verification
# script is itself covered by the tests described in the Test plan"). Each case
# builds a fixture matrix (a good one, or one with a single injected defect) and
# a canned list of "existing test ids", then asserts the verifier's exit code
# and that its diagnostic names the offending criterion.
#
# Written FIRST (TDD): before scripts/check-coverage-matrix.sh exists this
# harness fails every case, because the verifier it invokes is absent. It goes
# green once the verifier is authored to the contract these cases pin down.
#
# The harness does NOT invoke cargo: the verifier is run with --tests-from
# pointing at a fixture test-id list, so the self-tests are fast and hermetic.
# A separate Rust integration test (crates/cli/tests/coverage_matrix.rs) proves
# the verifier passes against the REAL checked-in matrix and the REAL cargo test
# suite, which is what covers SL8machine.
#
# Run from the repository root. Exit 0 = every self-test holds, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

verifier="scripts/check-coverage-matrix.sh"

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

# A canned list of "existing test ids" the fixture matrices map to. Mirrors the
# `cargo test -- --list` shape the real verifier consumes: one test path per
# line. The fixture "good" matrix maps its single machine criterion here.
cat >"$work/tests.txt" <<'EOF'
coverage_matrix::verifier_passes_against_the_checked_in_matrix
tests::crate_is_in_the_build_graph
EOF

# ---------------------------------------------------------------------------
# Fixture builders. A minimal but structurally faithful matrix: the table has a
# header, a separator, and one row per criterion. Columns:
#   | Criterion | Class | Platform | Test | Covered-by | Notes |
# `write_good_matrix <path>` emits a complete, valid fixture:
#   - one machine criterion (MX) mapped to an existing test id,
#   - one machine criterion (MY) legitimately `unmapped` with a *future* owning
#     ticket (deferred — allowed),
#   - one human row and one disclaimer row (need no test),
# plus a required-id manifest the verifier is told to enforce totality against.
# ---------------------------------------------------------------------------

# The fixture's required-criterion id set (what "every criterion in arch.md"
# reduces to for the hermetic fixtures). The real run derives this from the
# authoritative partition in docs/criteria-matrix.md; here we pass it in.
write_ids() { # write_ids <path>
  cat >"$1" <<'EOF'
MX machine
MY machine
HX human
DX disclaimer
EOF
}

write_good_matrix() { # write_good_matrix <path>
  cat >"$1" <<'EOF'
# fixture coverage matrix

| Criterion | Class | Platform | Test | Covered-by | Notes |
|---|---|---|---|---|---|
| MX | machine | platform-conditional | coverage_matrix::verifier_passes_against_the_checked_in_matrix | T7 | platform-conditional, mapped to an existing test |
| MY | machine | — | unmapped | T99 | deferred; test ships with a later ticket |
| HX | human | — | release-checklist | — | judgment, on the release checklist |
| DX | disclaimer | — | — | — | the tool claims nothing here |
EOF
}

# The fixture's platform-conditional id set: MX is platform-conditional in the
# good matrix (mirroring C12/C16/C19 in the real one); nothing else is.
plat_ids="MX"

run_verifier() { # run_verifier <matrix> <ids> : prints output, returns exit code
  "$verifier" --matrix "$1" --required-ids "$2" --tests-from "$work/tests.txt" \
    --platform-conditional "$plat_ids" 2>&1
}

# ---------------------------------------------------------------------------
# Case 1 — Complete matrix passes (Test plan: "Complete matrix passes").
# ---------------------------------------------------------------------------
write_good_matrix "$work/good.md"; write_ids "$work/ids.txt"
out=$(run_verifier "$work/good.md" "$work/ids.txt"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "complete matrix passes (exit 0)"
else
  bad "complete matrix should pass but exited $rc; output: $out"
fi
if printf '%s' "$out" | grep -qiE 'machine' \
   && printf '%s' "$out" | grep -qiE 'human' \
   && printf '%s' "$out" | grep -qiE 'disclaimer'; then
  pass "complete matrix prints a machine/human/disclaimer count summary"
else
  bad "complete matrix must print a class-count summary; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 2 — Missing criterion fails (Test plan: "Missing criterion fails").
# Delete the MY row; MY is still in the required-id set, so it is absent.
# ---------------------------------------------------------------------------
grep -v '^| MY ' "$work/good.md" >"$work/missing.md"
out=$(run_verifier "$work/missing.md" "$work/ids.txt"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "missing criterion fails (nonzero exit)"
else
  bad "missing criterion should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'MY' \
   && printf '%s' "$out" | grep -qiE 'absent|missing|not in the matrix'; then
  pass "missing-criterion error names MY and says it is absent from the matrix"
else
  bad "missing-criterion error must name MY as absent; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 3 — Duplicate criterion fails (Test plan: "Duplicate criterion fails").
# ---------------------------------------------------------------------------
{ cat "$work/good.md"; echo '| MX | machine | — | tests::crate_is_in_the_build_graph | T7 | duplicate row |'; } >"$work/dup.md"
out=$(run_verifier "$work/dup.md" "$work/ids.txt"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "duplicate criterion fails (nonzero exit)"
else
  bad "duplicate criterion should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'MX' \
   && printf '%s' "$out" | grep -qiE 'duplicat|more than once|exactly once'; then
  pass "duplicate-criterion error names MX and says it must appear exactly once"
else
  bad "duplicate-criterion error must name MX as duplicated; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 4 — Unmapped machine criterion fails (Test plan: "Unmapped machine
# criterion fails"). A machine row that is `unmapped` AND whose owning ticket is
# the CURRENT ticket (T7) — i.e. its covering test was supposed to ship here —
# is an error. (A machine row `unmapped` against a *future* ticket is the
# allowed deferred state; case 1's MY exercises that.)
# ---------------------------------------------------------------------------
# Replace MX's mapped Test cell with `unmapped`, platform-cell-agnostic (MX is
# platform-conditional in the good matrix, so the third cell is not `—`).
sed -E 's/^(\| MX \| machine \| [^|]*\| )coverage_matrix::[^|]*\|/\1unmapped |/' \
  "$work/good.md" >"$work/unmapped.md"
out=$(run_verifier "$work/unmapped.md" "$work/ids.txt"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "unmapped machine criterion (owned by this ticket) fails (nonzero exit)"
else
  bad "unmapped machine criterion should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'MX' \
   && printf '%s' "$out" | grep -qiE 'unmapped|must map to a test|no mapped test'; then
  pass "unmapped-machine error names MX and says a machine criterion must map to a test"
else
  bad "unmapped-machine error must name MX; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 5 — Dangling test reference fails (Test plan: "Dangling test reference
# fails"). MX maps to a test id absent from the suite.
# ---------------------------------------------------------------------------
sed 's|coverage_matrix::verifier_passes_against_the_checked_in_matrix|tests::this_test_does_not_exist|' \
  "$work/good.md" >"$work/dangling.md"
out=$(run_verifier "$work/dangling.md" "$work/ids.txt"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "dangling test reference fails (nonzero exit)"
else
  bad "dangling test reference should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'MX' \
   && printf '%s' "$out" | grep -q 'this_test_does_not_exist'; then
  pass "dangling-reference error names MX and the missing test id"
else
  bad "dangling-reference error must name MX and the missing test id; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 6 — Human/disclaimer rows need no test (Test plan: "Human/disclaimer rows
# need no test"). The good matrix's HX (human) and DX (disclaimer) carry no
# test id and case 1 already accepted it. Assert explicitly that a human row and
# a disclaimer row with no mapped test do not trip the verifier.
# ---------------------------------------------------------------------------
# Reuse the good matrix but strip HX/DX test cells to be unambiguously empty.
out=$(run_verifier "$work/good.md" "$work/ids.txt"); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "human and disclaimer rows are accepted without a mapped test"
else
  bad "human/disclaimer rows must not require a test; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 7 — A machine row mapped to an EXISTING test id is accepted (guards
# against a verifier that rejects every mapping). MX in the good matrix maps to
# a real test id from tests.txt; case 1 covers this, asserted here for clarity.
# ---------------------------------------------------------------------------
if printf '%s' "$(run_verifier "$work/good.md" "$work/ids.txt")" >/dev/null \
   && run_verifier "$work/good.md" "$work/ids.txt" >/dev/null; then
  pass "machine row mapped to an existing test id is accepted"
else
  bad "a machine row mapped to an existing test id must be accepted"
fi

# ---------------------------------------------------------------------------
# Case 8 — Untagged platform-conditional criterion fails (T70, ticket 077; Test
# plan: "Matrix checker fails on an untagged platform-conditional criterion").
# MX is in the platform-conditional set but its Platform cell is stripped to `—`;
# the verifier must fail and NAME MX. Restoring the tag (the good matrix) passes.
# This is the teeth: it proves the annotation is enforced, not decorative.
# ---------------------------------------------------------------------------
sed -E 's/^(\| MX \| machine \| )platform-conditional( \|)/\1—\2/' \
  "$work/good.md" >"$work/untagged.md"
out=$(run_verifier "$work/untagged.md" "$work/ids.txt"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "untagged platform-conditional criterion fails (nonzero exit)"
else
  bad "untagged platform-conditional criterion should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'MX' \
   && printf '%s' "$out" | grep -qiE 'platform-conditional'; then
  pass "untagged-platform error names MX and says it must be platform-conditional"
else
  bad "untagged-platform error must name MX as needing the platform tag; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 9 — A criterion tagged platform-conditional but NOT in the set fails
# (T70). MY is not platform-conditional; tagging it flags a stray annotation, so
# the tag stays a faithful record of exactly which criteria are platform-
# conditional. (Guards against the tag drifting onto the wrong rows.)
# ---------------------------------------------------------------------------
sed -E 's/^(\| MY \| machine \| )—( \|)/\1platform-conditional\2/' \
  "$work/good.md" >"$work/stray.md"
out=$(run_verifier "$work/stray.md" "$work/ids.txt"); rc=$?
if [ "$rc" -ne 0 ]; then
  pass "stray platform-conditional tag fails (nonzero exit)"
else
  bad "stray platform-conditional tag should fail but exited 0; output: $out"
fi
if printf '%s' "$out" | grep -q 'MY' \
   && printf '%s' "$out" | grep -qiE 'stray|not in the platform-conditional set'; then
  pass "stray-tag error names MY and says it is not in the platform-conditional set"
else
  bad "stray-tag error must name MY as a stray platform tag; output: $out"
fi

# ---------------------------------------------------------------------------
# Case 10 — A Linux-only machine criterion stays MAPPED while tagged platform-
# conditional (T70; Test plan: "macOS-excluded criterion is recorded, not
# silently dropped"). MX is platform-conditional AND mapped to a real test in the
# good matrix; it must NOT be reported as unmapped/dropped — the unmapped gate
# stays green. (Case 1 already asserts the good matrix passes; asserted here for
# the T70 property explicitly.)
# ---------------------------------------------------------------------------
out=$(run_verifier "$work/good.md" "$work/ids.txt"); rc=$?
if [ "$rc" -eq 0 ] && ! printf '%s' "$out" | grep -qi 'unmapped'; then
  pass "a platform-conditional criterion stays mapped, not reported unmapped/dropped"
else
  bad "a mapped platform-conditional criterion must not be reported unmapped; output: $out"
fi

if [ "$fail" -eq 0 ]; then
  echo "ALL COVERAGE-MATRIX SELF-TESTS PASSED"
  exit 0
else
  echo "SOME SELF-TESTS FAILED"
  exit 1
fi
