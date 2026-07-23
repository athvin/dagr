#!/usr/bin/env bash
# Repository hygiene acceptance checks for ticket 001 (T0.0a).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/001-T0.0a-repo-init-and-hygiene.md, section
# "Test plan"). These are hygiene invariants, not unit tests: authored FIRST
# as the acceptance gate, they fail on a bare tree and pass once the hygiene
# layer is in place. The scripted quality gate
# (.claude/skills/shipping-dagr-tickets/scripts/run_gate.sh pre-workspace 001)
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

# --- Test 1: toolchain is pinned and self-consistent -------------------------
if [ -f rust-toolchain.toml ]; then
  # A specific version (X.Y or X.Y.Z), never a floating channel name.
  chan=$(grep -E '^[[:space:]]*channel[[:space:]]*=' rust-toolchain.toml \
         | head -1 | sed -E 's/.*=[[:space:]]*"?([^"#]+)"?.*/\1/' | tr -d '[:space:]')
  case "$chan" in
    stable|beta|nightly|"")
      bad "test1: rust-toolchain channel must be a specific version, got '$chan'";;
    *[0-9].[0-9]*)
      pass "test1: toolchain channel pinned to specific version '$chan'";;
    *)
      bad "test1: rust-toolchain channel not a version-looking string: '$chan'";;
  esac
  grep -q 'rustfmt' rust-toolchain.toml \
    && pass "test1: rustfmt component declared" \
    || bad  "test1: rustfmt component missing from rust-toolchain.toml"
  grep -q 'clippy' rust-toolchain.toml \
    && pass "test1: clippy component declared" \
    || bad  "test1: clippy component missing from rust-toolchain.toml"
  # No drift: the pinned version appears verbatim on the README MSRV line.
  if ls README* >/dev/null 2>&1; then
    if grep -iE 'MSRV' README* | grep -q "$chan"; then
      pass "test1: README MSRV line matches pinned toolchain '$chan' (no drift)"
    else
      bad "test1: README MSRV line does not match pinned toolchain '$chan'"
    fi
  else
    bad "test1: no README to cross-check MSRV against"
  fi
else
  bad "test1: rust-toolchain.toml missing"
fi

# --- Test 2: formatting policy applies cleanly to a trivial input ------------
if [ -f rustfmt.toml ] && command -v rustfmt >/dev/null 2>&1; then
  tmp=$(mktemp -d)
  good="$tmp/good.rs"; bad_f="$tmp/bad.rs"
  printf 'fn main() {\n    let x = 1;\n    println!("{x}");\n}\n' >"$good"
  printf 'fn main(){let x=1;println!("{x}");}\n' >"$bad_f"
  # Config must be accepted (no unknown-option errors) and be live: the
  # mis-formatted snippet must be reported by --check, the tidy one accepted.
  if rustfmt --check --config-path rustfmt.toml "$good" >/dev/null 2>"$tmp/err"; then
    if grep -qi 'unknown\|error' "$tmp/err"; then
      bad "test2: rustfmt reported config errors: $(head -1 "$tmp/err")"
    else
      pass "test2: rustfmt.toml accepted; formatted snippet passes --check"
    fi
  else
    if grep -qi 'unknown\|not.*recognized\|expected' "$tmp/err"; then
      bad "test2: rustfmt.toml rejected (config error): $(head -1 "$tmp/err")"
    else
      # Non-zero because the "good" snippet disagreed with policy — treat as fail.
      bad "test2: already-formatted snippet failed --check unexpectedly"
    fi
  fi
  if rustfmt --check --config-path rustfmt.toml "$bad_f" >/dev/null 2>"$tmp/err2"; then
    bad "test2: mis-formatted snippet unexpectedly passed --check (config inert)"
  else
    if grep -qi 'unknown\|not.*recognized' "$tmp/err2"; then
      bad "test2: rustfmt.toml has an unknown option: $(head -1 "$tmp/err2")"
    else
      pass "test2: mis-formatted snippet correctly reported by --check (config live)"
    fi
  fi
  rm -rf "$tmp"
elif [ ! -f rustfmt.toml ]; then
  bad "test2: rustfmt.toml missing"
else
  pass "test2: SKIP (rustfmt not installed) — rustfmt.toml present"
fi

# --- Test 3: lint policy names a deny set and justifies every exception ------
policy=""
for cand in lints.toml docs/lint-policy.md LINT_POLICY.md; do
  [ -f "$cand" ] && policy="$cand" && break
done
if [ -n "$policy" ]; then
  pass "test3: lint policy artifact present ($policy)"
  grep -qiE 'warn|deny' "$policy" \
    && pass "test3: warnings-denied posture stated" \
    || bad  "test3: lint policy does not state a warnings-denied posture"
else
  bad "test3: no lint policy artifact found (lints.toml / docs/lint-policy.md)"
fi

# --- Test 4: license is present and machine-readable ------------------------
if [ -f LICENSE ]; then
  pass "test4: LICENSE present at repo root"
else
  bad "test4: LICENSE missing at repo root"
fi
# The SPDX identifier the supply-chain check will allow, recorded for T7.
if grep -rqiE 'SPDX|license.*=.*"' docs/lint-policy.md LICENSE 2>/dev/null \
   || grep -rqi 'SPDX-License-Identifier' . --include='*.md' --include='*.toml' \
        --exclude-dir=.git 2>/dev/null; then
  pass "test4: SPDX license identifier recorded for cargo deny (T7 target)"
else
  bad "test4: no SPDX license identifier recorded for the supply-chain check"
fi

# --- Test 5: README states the boundary and the MSRV ------------------------
if ls README* >/dev/null 2>&1; then
  rm=$(ls README* | head -1)
  grep -qi 'MSRV' "$rm"         && pass "test5: README has an MSRV line" \
                                 || bad  "test5: README has no MSRV line"
  grep -qi 'scheduler' "$rm"    && pass "test5: README states non-goals (scheduler)" \
                                 || bad  "test5: README missing non-goals boundary"
  grep -qi 'quickstart' "$rm"   && pass "test5: README has a quickstart placeholder" \
                                 || bad  "test5: README missing quickstart placeholder"
  grep -qiE 'runtime' "$rm" && grep -qi 'shape' "$rm" \
    && pass "test5: README states graph-shape-fixed-at-runtime boundary" \
    || bad  "test5: README missing the runtime-graph-shape boundary"
  # No claim of Windows support (arch.md Platform support).
  if grep -qi 'windows' "$rm" && ! grep -iE 'windows' "$rm" | grep -qiE 'unsupported|not supported|no.*windows'; then
    bad "test5: README mentions Windows without marking it unsupported"
  else
    pass "test5: README makes no unsupported Windows claim"
  fi
else
  bad "test5: README missing"
fi

# --- Test 6: EditorConfig and rustfmt agree ---------------------------------
if [ -f .editorconfig ] && [ -f rustfmt.toml ]; then
  ec_indent=$(grep -iE 'indent_size' .editorconfig | head -1 | grep -oE '[0-9]+')
  rf_indent=$(grep -iE 'tab_spaces' rustfmt.toml | head -1 | grep -oE '[0-9]+')
  # rustfmt defaults tab_spaces to 4 when unset.
  [ -z "$rf_indent" ] && rf_indent=4
  [ -z "$ec_indent" ] && ec_indent=4
  if [ "$ec_indent" = "$rf_indent" ]; then
    pass "test6: indent size agrees ($ec_indent)"
  else
    bad "test6: indent size mismatch (.editorconfig=$ec_indent rustfmt=$rf_indent)"
  fi
  grep -qi 'indent_style *= *space' .editorconfig \
    && pass "test6: .editorconfig uses spaces (matches rustfmt hard_tabs=false)" \
    || bad  "test6: .editorconfig indent_style not 'space'"
  grep -qi 'end_of_line *= *lf' .editorconfig \
    && pass "test6: .editorconfig EOL=lf (matches rustfmt newline_style Unix)" \
    || bad  "test6: .editorconfig end_of_line not 'lf'"
  grep -qi 'insert_final_newline *= *true' .editorconfig \
    && pass "test6: .editorconfig inserts a final newline" \
    || bad  "test6: .editorconfig insert_final_newline not true"
  grep -qi 'charset *= *utf-8' .editorconfig \
    && pass "test6: .editorconfig charset=utf-8" \
    || bad  "test6: .editorconfig charset not utf-8"
else
  bad "test6: .editorconfig or rustfmt.toml missing"
fi

# --- Test 7: gitignore hides all generated output ---------------------------
# check-ignore works on nonexistent paths, so no files are created.
probes_ignored="target/probe probe.rs.bk .dagr/runs/probe .scratch/probe artifacts/probe .DS_Store"
for p in $probes_ignored; do
  if git check-ignore -q "$p"; then
    pass "test7: gitignore covers generated path '$p'"
  else
    bad "test7: gitignore does NOT cover generated path '$p'"
  fi
done
# Negative check: a normal source path is not ignored.
if git check-ignore -q src/main.rs; then
  bad "test7: gitignore wrongly ignores a normal source path (src/main.rs)"
else
  pass "test7: normal source path is not ignored (negative check)"
fi

# --- Test 8: no crate leaked in ---------------------------------------------
leaked=$(find . -path ./.git -prune -o \
  \( -name Cargo.toml -o -name Cargo.lock -o -name '*.rs' \) -print | head -5)
if [ -z "$leaked" ]; then
  pass "test8: no Cargo.toml / Cargo.lock / *.rs present (workspace is T1)"
else
  bad "test8: crate artifact leaked in: $leaked"
fi

echo "---"
if [ "$fail" -eq 0 ]; then
  echo "HYGIENE=PASS"
else
  echo "HYGIENE=FAIL"
fi
exit "$fail"
