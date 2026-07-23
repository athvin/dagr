#!/usr/bin/env bash
# Era-appropriate local quality gate with terse verdicts, so neither the
# orchestrator nor a subagent ingests thousands of lines of cargo output.
#
# Usage: run_gate.sh <era> [nnn]
#   era:  pre-workspace | pre-ci | ci     (from next_ticket.py JSON)
#   nnn:  ticket number — required for pre-workspace (001 and 002 have
#         scripted content assertions derived from their Test plans)
#
# Prints one CHECK:<name>=PASS|FAIL|SKIP:<reason> line per check, a 40-line
# tail of any failing check's output, and a final GATE=PASS|FAIL.
# Exit code: 0 gate passed, 1 gate failed, 2 usage/era anomaly.
set -u

era=${1:?usage: run_gate.sh <era> [nnn]}
nnn=${2:-}
fail=0
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

pass() { echo "CHECK:$1=PASS"; }
skip() { echo "CHECK:$1=SKIP:$2"; }
failed() {
  echo "CHECK:$1=FAIL"
  fail=1
  if [ -s "$tmpdir/$1.log" ]; then
    echo "--- last 40 lines of $1 ---"   # enough for cargo's error summary
    tail -40 "$tmpdir/$1.log"
    echo "--- end $1 ---"
  fi
}
run_check() { # run_check NAME cmd...
  local name=$1; shift
  if "$@" >"$tmpdir/$name.log" 2>&1; then pass "$name"; else failed "$name"; fi
}

# --- era vs repo-state assert: a mismatch means the loop selected a ticket
# whose era doesn't match reality (hand-edited README, wrong branch) --------
case "$era" in
  pre-workspace)
    if [ -f Cargo.toml ]; then
      echo "GATE=FAIL"; echo "ANOMALY=era pre-workspace but Cargo.toml exists"; exit 2
    fi ;;
  pre-ci|ci)
    if [ ! -f Cargo.toml ]; then
      echo "GATE=FAIL"; echo "ANOMALY=era $era but no Cargo.toml at repo root"; exit 2
    fi ;;
  *) echo "GATE=FAIL"; echo "ANOMALY=unknown era $era"; exit 2 ;;
esac

if [ "$era" = "pre-workspace" ]; then
  [ -n "$nnn" ] || { echo "GATE=FAIL"; echo "ANOMALY=pre-workspace gate needs a ticket nnn"; exit 2; }

  # Ticket 001 test 8 / DoD: no crate may leak in before T1.
  leaked=$(find . -path ./.git -prune -o \( -name Cargo.toml -o -name Cargo.lock -o -name '*.rs' \) -print | head -5)
  if [ -z "$leaked" ]; then pass no-crate-leaked; else
    echo "$leaked" >"$tmpdir/no-crate-leaked.log"; failed no-crate-leaked
  fi

  # Ticket 001 content assertions (from its Test plan / DoD).
  if [ "$nnn" = "001" ] || [ "$nnn" = "002" ]; then
    for f in rust-toolchain.toml rustfmt.toml LICENSE .editorconfig; do
      if [ -f "$f" ]; then pass "exists-$f"; else failed "exists-$f"; fi
    done
    if [ -f rust-toolchain.toml ] && grep -q rustfmt rust-toolchain.toml && grep -q clippy rust-toolchain.toml; then
      pass toolchain-components
    else
      failed toolchain-components
    fi
    if ls README* >/dev/null 2>&1 && grep -qi MSRV README* && grep -qi scheduler README*; then
      pass readme-msrv-and-nongoals   # MSRV line + permanent non-goals boundary
    else
      failed readme-msrv-and-nongoals
    fi
    # Gitignore probes (ticket 001 test 7); check-ignore works on nonexistent paths.
    if git check-ignore -q target/probe && git check-ignore -q probe.rs.bk; then
      pass gitignore-covers-generated
    else
      failed gitignore-covers-generated
    fi
  fi

  # Ticket 002 content assertions (from its Test plan / DoD).
  if [ "$nnn" = "002" ]; then
    if [ -f CONTRIBUTING.md ]; then pass exists-CONTRIBUTING; else failed exists-CONTRIBUTING; fi
    if [ -f CONTRIBUTING.md ] && grep -qi "branch" CONTRIBUTING.md \
       && grep -qiE "tests[- ]first|TDD" CONTRIBUTING.md && grep -q clippy CONTRIBUTING.md; then
      pass contributing-contract
    else
      failed contributing-contract
    fi
    if ls .github/pull_request_template.md .github/PULL_REQUEST_TEMPLATE.md \
          .github/PULL_REQUEST_TEMPLATE/*.md >/dev/null 2>&1; then
      pass exists-pr-template
    else
      failed exists-pr-template
    fi
    if [ -f .github/CODEOWNERS ] || [ -f CODEOWNERS ]; then
      pass exists-CODEOWNERS
    else
      failed exists-CODEOWNERS
    fi
    if ls README* >/dev/null 2>&1 && grep -q CONTRIBUTING README*; then
      pass readme-links-workflow
    else
      failed readme-links-workflow
    fi
  fi
else
  # --- pre-ci and ci: local substitutes for (or dry run of) the real CI ----
  run_check fmt cargo fmt --all --check
  run_check clippy cargo clippy --workspace --all-targets -- -D warnings
  run_check test cargo test --workspace
  # Default rustdoc lint; once CONTRIBUTING.md/CI define the exact command,
  # that definition is authoritative — quality-gates.md documents the hand-off.
  RUSTDOCFLAGS='-D warnings' run_check rustdoc cargo doc --workspace --no-deps

  if command -v cargo-audit >/dev/null && [ -f Cargo.lock ]; then
    run_check audit cargo audit
  else
    skip audit "cargo-audit or Cargo.lock missing"
  fi
  if command -v cargo-deny >/dev/null && [ -f deny.toml ]; then
    run_check deny cargo deny check
  else
    skip deny "cargo-deny or deny.toml missing"
  fi

  if [ "$era" = "ci" ]; then
    # Criteria-matrix verification (authored by ticket 006). Location is the
    # repo's choice; probe the conventional spots, SKIP loudly if absent so
    # matrix debt is visible per-ticket instead of exploding at ticket 080.
    matrix=$(ls scripts/*criteria* ci/*criteria* scripts/*matrix* ci/*matrix* 2>/dev/null | head -1)
    if [ -n "$matrix" ]; then
      run_check criteria-matrix "$matrix"
    else
      skip criteria-matrix "no matrix-verification script found"
    fi
  fi
fi

if [ $fail -eq 0 ]; then echo "GATE=PASS"; else echo "GATE=FAIL"; fi
exit $fail
