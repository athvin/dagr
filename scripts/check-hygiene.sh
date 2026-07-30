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

# --- Test 8: no crate artifact leaks outside the declared workspace ----------
#
# PREDICATE CORRECTED BY T98, and deliberately not relaxed.
#
# As authored, this test asserted that NO `Cargo.toml`, `Cargo.lock`, or `*.rs`
# existed anywhere in the tree — correct for exactly as long as the repository
# had no crate in it, which is to say until T1 landed the workspace one ticket
# later. From that merge onwards it reported the root manifest as a leak and
# could never pass again; being unwired in CI, nobody saw it. That is the rot a
# dormant checker accumulates, and the reason T98 wires every checker.
#
# The INVARIANT the test was defending still holds and is still worth defending:
# a crate artifact must not appear where the workspace does not declare one. A
# stray manifest outside the declared members is either a leaked spike (which
# ticket-conventions §4 requires be quarantined outside the workspace or deleted
# before the PR), a vendored copy, or a member somebody forgot to add to
# `[workspace] members` — all three are exactly what this catches. So the scan is
# re-pointed at that, rather than deleted or weakened into a pass.
#
# The DECLARED workspace is read from the root manifest, never assumed: the
# allowlist is exactly `[workspace] members`, so `crates/<x>` is permitted only
# when `x` is a declared member. A manifest is permitted at the root and at
# `<member>/Cargo.toml` — nowhere else, at no depth. There is exactly one
# lockfile, the workspace's, at the root; a nested `Cargo.lock` is the signature
# of a crate that resolves on its own, i.e. one outside the workspace. Rust
# sources may live only under a declared member's directory. `target/` is build
# output, not source.
#
# Matching is by EXACT PATH against that allowlist rather than by glob. A shell
# `case` glob matches `/`, so a pattern like `./crates/*/Cargo.toml` silently
# admits `./crates/core/spike/Cargo.toml` — precisely the quarantined-spike case
# above — and an undeclared `./crates/rogue/Cargo.toml` besides. Exact matching
# is the only form of this predicate that means what the paragraph above says.
#
# Reading that allowlist is itself a place the check can be silently widened, so
# the parse is a function and TOML COMMENTS ARE STRIPPED BEFORE the quoted paths
# are harvested. Harvesting `"…"` straight out of the array body also harvests
# the quoted text inside a comment, so a members-block line like
#     "crates/cli",   # keep "crates/spike" out
# would add `crates/spike` to the allowlist and the scan would wave through
# precisely the leaked spike it exists to catch. A `#` inside a string is not a
# comment, so the stripper tracks basic (`"`) and literal (`'`) strings instead of
# cutting at the first `#`; the array terminator is looked for in the STRIPPED
# line too, so a `]` inside a comment cannot truncate the member list early.
parse_members() {
  awk '
    function uncomment(s,   out, i, c, n, basic, literal, sq) {
      sq = "\047"; out = ""; n = length(s); basic = 0; literal = 0
      for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (basic) {
          if (c == "\\") { out = out c substr(s, i + 1, 1); i++; continue }
          if (c == "\"") basic = 0
        } else if (literal) {
          if (c == sq) literal = 0
        } else {
          if (c == "#") break
          if (c == "\"") basic = 1; else if (c == sq) literal = 1
        }
        out = out c
      }
      return out
    }
    {
      line = uncomment($0)
      if (line ~ /^[[:space:]]*members[[:space:]]*=/) inblk = 1
      if (!inblk) next
      print line
      if (line ~ /\]/) exit
    }
  ' "$1" 2>/dev/null | grep -oE '"[^"]+"' | tr -d '"'
}

members=$(parse_members Cargo.toml)
if [ -z "$members" ]; then
  bad "test8: no [workspace] members could be read from Cargo.toml — the scan would be vacuous"
fi

# The comment blindness above is closed, and this probe proves it by RUNNING THE
# PARSER against a synthetic manifest rather than by re-deriving the rule. Two
# assertions, because a stripper that ate the whole array would satisfy the first
# one on its own: the commented-out path must NOT reach the allowlist, and every
# real member still must.
mprobe=$(mktemp -d 2>/dev/null) || mprobe=""
if [ -n "$mprobe" ]; then
  printf '[workspace]\nmembers = [\n    "crates/core",\n    "crates/cli",   # keep "crates/spike" out\n]\n' \
    >"$mprobe/Cargo.toml"
  parsed=" $(parse_members "$mprobe/Cargo.toml" | tr '\n' ' ')"
  rm -rf "$mprobe"
  case "$parsed" in
    *" crates/spike "*)
      bad "test8: the members parser harvests quoted paths out of TOML comments — a comment can widen the allowlist";;
    *)
      pass "test8: the members parser ignores quoted paths inside comments (a comment cannot widen the allowlist)";;
  esac
  mmissing=""
  for want in crates/core crates/cli; do
    case "$parsed" in *" $want "*) ;; *) mmissing="$mmissing $want";; esac
  done
  if [ -z "$mmissing" ]; then
    pass "test8: the members parser still reads every real member (the comment strip is not eating the array)"
  else
    bad "test8: the members parser dropped a real member from the synthetic manifest:$mmissing"
  fi
fi
allowed_files="./Cargo.toml
./Cargo.lock"
allowed_dirs=""
for m in $members; do
  allowed_files="$allowed_files
./$m/Cargo.toml"
  allowed_dirs="$allowed_dirs ./$m/"
done

# Emits one line per crate artifact that is not where the declared workspace
# says it may be. This function IS the predicate; the probe below drives it.
scan_for_leaked_artifacts() {
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    printf '%s\n' "$allowed_files" | grep -qxF "$f" || printf '%s\n' "$f"
  done <<EOF
$(find . -path ./.git -prune -o -path ./target -prune \
       -o \( -name Cargo.toml -o -name Cargo.lock \) -print)
EOF
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    inside=0
    for d in $allowed_dirs; do
      case "$f" in "$d"*) inside=1; break;; esac
    done
    [ "$inside" -eq 1 ] || printf '%s\n' "$f"
  done <<EOF
$(find . -path ./.git -prune -o -path ./target -prune -o -name '*.rs' -print)
EOF
}

leaked=$(scan_for_leaked_artifacts)
if [ -z "$leaked" ]; then
  pass "test8: every Cargo.toml / Cargo.lock / *.rs lives inside the declared workspace (no leaked crate)"
else
  bad "test8: crate artifact outside the declared workspace:"
  printf '%s\n' "$leaked" | sed 's/^/        /'
fi

# The scan is non-vacuous — and this probe proves it by RUNNING THE SCAN, not by
# re-deriving the predicate. A probe that re-implements the check cannot fail
# when the check is broken, which is worse than having no probe at all: the
# earlier version of this block re-derived the rule as a stricter grep and so
# reported the scan healthy in exactly the two runs where the scan was blind.
#
# Three shapes are planted, one at a time, because they fail differently: a
# manifest outside the workspace tree, a nested manifest under a real member
# (the leaked-spike case), and an undeclared directory under `crates/` (the
# forgotten-member case).
probe_root="./.hygiene-probe.$$"
cleanup_probe() { rm -rf "$probe_root" "crates/.hygiene-probe-$$" "crates/core/.hygiene-probe-$$"; }
trap cleanup_probe EXIT INT TERM
for plant in "$probe_root" "crates/.hygiene-probe-$$" "crates/core/.hygiene-probe-$$"; do
  if [ -e "$plant" ]; then
    bad "test8: probe path '$plant' already exists — refusing to plant over it"
    continue
  fi
  mkdir -p "$plant" 2>/dev/null || { bad "test8: could not plant probe at '$plant'"; continue; }
  : >"$plant/Cargo.toml"
  caught=$(scan_for_leaked_artifacts | grep -F "$plant/Cargo.toml" || true)
  rm -rf "$plant"
  if [ -n "$caught" ]; then
    pass "test8: the scan rejects a manifest planted at '$plant/Cargo.toml' (non-vacuous)"
  else
    bad "test8: the scan did not notice the manifest planted at '$plant/Cargo.toml' — it is vacuous there"
  fi
done
cleanup_probe
trap - EXIT INT TERM

echo "---"
if [ "$fail" -eq 0 ]; then
  echo "HYGIENE=PASS"
else
  echo "HYGIENE=FAIL"
fi
exit "$fail"
