#!/usr/bin/env bash
# Contributor-workflow acceptance checks for ticket 002 (T0.0b).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/002-T0.0b-contributor-and-branch-workflow.md, section
# "Test plan"). Like ticket 001's hygiene invariants, these are not unit tests:
# authored FIRST as the acceptance gate, they fail on a tree that lacks the
# contributor-process documents and pass once CONTRIBUTING.md, the PR template,
# CODEOWNERS, and the README cross-links are in place. The scripted quality gate
# (.claude/skills/shipping-dagr-tickets/scripts/run_gate.sh pre-workspace 002)
# encodes a subset of the same assertions; this script is the fuller,
# self-documenting expression of the Test plan and the tests-first artifact.
#
# Run from the repository root. Exit 0 = all invariants hold, 1 = a failure.
set -u

root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$root" || { echo "cannot cd to repo root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

# Resolve the PR template location GitHub honours (any one is valid).
pr_template=""
for cand in .github/pull_request_template.md .github/PULL_REQUEST_TEMPLATE.md \
            .github/PULL_REQUEST_TEMPLATE/*.md; do
  [ -f "$cand" ] && pr_template="$cand" && break
done

# Resolve the CODEOWNERS location GitHub honours.
codeowners=""
for cand in .github/CODEOWNERS CODEOWNERS docs/CODEOWNERS; do
  [ -f "$cand" ] && codeowners="$cand" && break
done

# Resolve the README (the front door).
readme=""
if ls README* >/dev/null 2>&1; then readme=$(ls README* | head -1); fi

# --- Test: branch-name derivation is unambiguous -----------------------------
if [ -f CONTRIBUTING.md ]; then
  pass "test-branch: CONTRIBUTING.md present at repo root"
  # One branch per ticket, name copied verbatim from the header Branch field.
  if grep -qi 'branch' CONTRIBUTING.md \
     && grep -qi 'verbatim' CONTRIBUTING.md; then
    pass "test-branch: states one branch per ticket, name taken verbatim"
  else
    bad "test-branch: CONTRIBUTING.md does not state the verbatim-branch rule"
  fi
  # A real ticket is pointed at as a worked example.
  if grep -q 'chore/t0.0b-contributor-and-branch-workflow' CONTRIBUTING.md; then
    pass "test-branch: points at a real ticket branch as a worked example"
  else
    bad "test-branch: no worked-example ticket branch cited"
  fi
else
  bad "test-branch: CONTRIBUTING.md missing at repo root"
fi

# --- Test: one PR per ticket is mandated -------------------------------------
if [ -f CONTRIBUTING.md ]; then
  if grep -qiE 'one PR|single PR|exactly one PR' CONTRIBUTING.md \
     && grep -qi 'tid' CONTRIBUTING.md \
     && grep -q 'docs/implementation/' CONTRIBUTING.md; then
    pass "test-pr: one-PR-per-ticket rule links ticket by tid and path"
  else
    bad "test-pr: one-PR-per-ticket rule (tid + docs/implementation/ path) missing"
  fi
  if grep -qi 'review' CONTRIBUTING.md \
     && grep -qiE 'CI.*green|green.*CI|checks pass' CONTRIBUTING.md; then
    pass "test-pr: PR not merged until review and CI are green"
  else
    bad "test-pr: 'not merged until review and CI green' rule missing"
  fi
fi

# --- Test: tests-first is stated as a hard rule ------------------------------
if [ -f CONTRIBUTING.md ]; then
  if grep -qiE 'tests[- ]first|TDD' CONTRIBUTING.md \
     && grep -qi 'test plan' CONTRIBUTING.md \
     && grep -qi 'before' CONTRIBUTING.md; then
    pass "test-tdd: tests-first / TDD stated with 'before implementation'"
  else
    bad "test-tdd: tests-first hard rule not stated"
  fi
  # Framed as non-negotiable, and each PR shows tests exercising the criteria.
  if grep -qiE 'non-negotiable|hard rule|not aspirational|mandatory' CONTRIBUTING.md \
     && grep -qi 'acceptance criteria' CONTRIBUTING.md; then
    pass "test-tdd: framed non-negotiable, PR shows tests for the criteria"
  else
    bad "test-tdd: not framed non-negotiable / no criteria-exercising requirement"
  fi
fi

# --- Test: CI merge gate lists the exact checks ------------------------------
if [ -f CONTRIBUTING.md ]; then
  gate_ok=1
  for needle in 'cargo fmt --check' 'cargo clippy' 'cargo audit' 'cargo deny'; do
    grep -qF "$needle" CONTRIBUTING.md || { gate_ok=0; bad "test-gate: merge gate missing '$needle'"; }
  done
  grep -qiE 'warnings.*denied|-D warnings|deny warnings' CONTRIBUTING.md \
    || { gate_ok=0; bad "test-gate: clippy warnings-denied posture not stated"; }
  grep -qiE 'rustdoc' CONTRIBUTING.md \
    || { gate_ok=0; bad "test-gate: rustdoc lint not named in the merge gate"; }
  grep -qiE 'test suite|the tests|cargo test' CONTRIBUTING.md \
    || { gate_ok=0; bad "test-gate: test suite not named in the merge gate"; }
  grep -q 'T7' CONTRIBUTING.md \
    || { gate_ok=0; bad "test-gate: does not note T7 implements the gate in GitHub Actions"; }
  [ "$gate_ok" = 1 ] && pass "test-gate: merge gate enumerates the exact CI checks and cites T7"
fi

# --- Test: PR template exists and links the ticket ---------------------------
if [ -n "$pr_template" ]; then
  pass "test-prtemplate: PR template present ($pr_template)"
  if grep -qi 'tid' "$pr_template" \
     && grep -q 'docs/implementation/' "$pr_template"; then
    pass "test-prtemplate: requires linked ticket (tid + docs/implementation/ path)"
  else
    bad "test-prtemplate: no required linked-ticket field (tid + path)"
  fi
  if grep -qi 'acceptance criteria' "$pr_template"; then
    pass "test-prtemplate: restates the acceptance criteria satisfied"
  else
    bad "test-prtemplate: no place to restate acceptance criteria"
  fi
  if grep -qiE 'tests[- ]first|TDD' "$pr_template" \
     && grep -qF 'cargo clippy' "$pr_template"; then
    pass "test-prtemplate: DoD-style checklist incl. tests-first + CI checks"
  else
    bad "test-prtemplate: checklist missing tests-first confirmation or CI-check names"
  fi
else
  bad "test-prtemplate: no PR template under .github/"
fi

# --- Test: CODEOWNERS forces review ------------------------------------------
if [ -n "$codeowners" ]; then
  pass "test-codeowners: CODEOWNERS present ($codeowners) at a GitHub-honoured path"
  # A wildcard owner covering the whole repo so any PR requires an owner review.
  if grep -qE '^[[:space:]]*\*[[:space:]]+@?[[:graph:]]+' "$codeowners"; then
    pass "test-codeowners: assigns a repo-wide owner (any PR needs review)"
  else
    bad "test-codeowners: no repo-wide owner rule found"
  fi
else
  bad "test-codeowners: no CODEOWNERS at .github/ or repo root"
fi

# --- Test: README points to the workflow -------------------------------------
if [ -n "$readme" ]; then
  if grep -q 'CONTRIBUTING' "$readme"; then
    pass "test-readme: README links CONTRIBUTING.md (workflow front door)"
  else
    bad "test-readme: README does not reference CONTRIBUTING.md"
  fi
  if grep -qiE 'pull request|PR template|CODEOWNERS' "$readme"; then
    pass "test-readme: README mentions the PR process / template / CODEOWNERS"
  else
    bad "test-readme: README does not mention the PR process"
  fi
else
  bad "test-readme: no README to check"
fi

# --- Test: no scope-boundary drift -------------------------------------------
# The permanent non-goals must not be reintroduced as *process* in any doc this
# ticket authors. We scan for phrasing that would imply dagr grows into one of
# the forbidden shapes; a bare mention inside a "not a ..." boundary line is
# fine, so we look for affirmative process verbs near the forbidden nouns.
drift=0
for doc in CONTRIBUTING.md "$pr_template" "$codeowners"; do
  [ -n "$doc" ] && [ -f "$doc" ] || continue
  if grep -qiE '(schedul|distributed execution|metadata store|web (ui|interface)|\bDSL\b|backfill orchestrat)' "$doc" \
     && ! grep -qiE 'not a|never|permanent|out of scope|boundary' "$doc"; then
    bad "test-scope: '$doc' mentions a forbidden capability without a boundary framing"
    drift=1
  fi
done
[ "$drift" = 0 ] && pass "test-scope: no scope-boundary drift in authored documents"

# --- Test: documents are well-formed and reachable ---------------------------
# Every relative markdown link inside the authored docs resolves to a real path.
link_ok=1
for doc in CONTRIBUTING.md "$pr_template"; do
  [ -n "$doc" ] && [ -f "$doc" ] || continue
  base=$(dirname "$doc")
  # Extract markdown link targets, ignore http(s) and pure #anchors.
  grep -oE '\]\([^)]+\)' "$doc" \
    | sed -E 's/^\]\(([^)]+)\)$/\1/' \
    | while IFS= read -r target; do
        case "$target" in
          http://*|https://*|mailto:*|"#"*) continue ;;
        esac
        path=${target%%#*}
        [ -z "$path" ] && continue
        resolved="$base/$path"
        if [ ! -e "$resolved" ]; then
          printf 'BROKEN-LINK %s -> %s\n' "$doc" "$target"
        fi
      done > /tmp/t0.0b_links.$$ 2>/dev/null
  if [ -s /tmp/t0.0b_links.$$ ]; then
    while IFS= read -r line; do bad "test-links: $line"; done < /tmp/t0.0b_links.$$
    link_ok=0
  fi
  rm -f /tmp/t0.0b_links.$$
done
[ "$link_ok" = 1 ] && pass "test-links: internal links in authored docs resolve"

echo "---"
if [ "$fail" -eq 0 ]; then
  echo "WORKFLOW=PASS"
else
  echo "WORKFLOW=FAIL"
fi
exit "$fail"
