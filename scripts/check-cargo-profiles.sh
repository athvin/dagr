#!/usr/bin/env bash
# Cargo build-profile invariants for ticket 108 (T93).
#
# Each check below is a mechanical translation of the ticket's Test plan
# (docs/implementation/108-T93-cargo-profiles.md): the workspace's compiler
# settings are a checked-in decision, and two of them are load-bearing
# GUARANTEES rather than tuning knobs.
#
#   * `panic = "abort"` must never be set, anywhere, by any profile or by
#     rustflags. dagr contains a task panic with `catch_unwind` and reports it as
#     a panicked attempt; under `panic = "abort"` there is nothing to catch, and
#     `crates/core/src/execution.rs`'s `check_panic_strategy` refuses to start a
#     run at all. This is the check the ticket asks for by name: "assert this
#     mechanically so a later edit cannot reintroduce it silently."
#   * `strip` must never be enabled. dagr's pitch is explaining a run after the
#     fact, and it attributes a panic to a node through a panic hook; stripping
#     symbols trades that away for a binary size dagr does not compete on.
#
# The remaining checks pin the profile values themselves, so a profile silently
# deleted or down-tuned (LTO back off, codegen-units back to the default 16)
# fails the build instead of quietly costing the per-node budget arch.md's
# "Performance envelope" holds at under a millisecond.
#
# The two scans are non-vacuous by construction: after the real checks pass, the
# script re-invokes ITSELF in `--scan-only` mode against a throwaway fixture that
# does set `panic = "abort"` and `strip = true`, and fails if that fixture is
# reported clean. A guard that cannot fail is not a guard.
#
# Usage:
#   check-cargo-profiles.sh              # full gate, from the repository root
#   check-cargo-profiles.sh --scan-only DIR
#
# `--scan-only DIR` runs ONLY the two tree-wide scans (panic / strip) over DIR
# and skips the root-manifest value checks and the self-check. It exists so the
# self-check can drive the scans hermetically against a fixture; CI invokes the
# script with no arguments and gets the whole gate.
#
# Exit 0 = every invariant holds, 1 = at least one failure, 2 = bad invocation.
set -u

scan_only=""
case "${1:-}" in
  --scan-only) scan_only="${2:?--scan-only needs a directory}" ;;
  -h|--help)   sed -n '1,40p' "$0"; exit 0 ;;
  "")          ;;
  *)           echo "unknown argument: $1" >&2; exit 2 ;;
esac

self=$0
case "$self" in /*) ;; *) self="$PWD/$self" ;; esac

if [ -n "$scan_only" ]; then
  [ -d "$scan_only" ] || { echo "FAIL  --scan-only: no such directory: $scan_only" >&2; exit 2; }
  root=$scan_only
else
  root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
fi
cd "$root" || { echo "cannot cd to $root"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

manifest="Cargo.toml"

# --- TOML helpers -------------------------------------------------------------
# Deliberately small: enough to read a scalar out of a named `[section]` and to
# slice a section's body out of a file. Comment lines are skipped, so the
# rationale comments these checks require cannot themselves be misread as data.

# toml_value FILE SECTION KEY -> the scalar value, quotes and comment stripped.
toml_value() {
  awk -v want="$2" -v key="$3" '
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (line ~ /^#/ || line == "") next
      if (line ~ /^\[/) {
        s = line
        sub(/^\[/, "", s)
        sub(/\][[:space:]]*$/, "", s)
        cur = s
        next
      }
      if (cur != want || line !~ /=/) next
      k = line; sub(/=.*$/, "", k); gsub(/[[:space:]]/, "", k)
      if (k != key) next
      v = line; sub(/^[^=]*=/, "", v)
      sub(/#.*$/, "", v)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
      gsub(/"/, "", v)
      print v
    }
  ' "$1"
}

# toml_section_body FILE SECTION -> every line of the section, comments included.
toml_section_body() {
  awk -v want="$2" '
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (line ~ /^\[/) {
        s = line
        sub(/^\[/, "", s)
        sub(/\][[:space:]]*$/, "", s)
        cur = s
        next
      }
      if (cur == want) print
    }
  ' "$1"
}

# toml_has_section FILE SECTION
toml_has_section() {
  awk -v want="$2" '
    { line = $0; sub(/^[[:space:]]+/, "", line) }
    line ~ /^\[/ {
      s = line; sub(/^\[/, "", s); sub(/\][[:space:]]*$/, "", s)
      if (s == want) { found = 1; exit }
    }
    END { exit(found ? 0 : 1) }
  ' "$1"
}

# expect_value SECTION KEY EXPECTED LABEL
expect_value() {
  got=$(toml_value "$manifest" "$1" "$2")
  if [ "$got" = "$3" ]; then
    pass "$4: [$1] $2 = $3"
  else
    bad "$4: [$1] $2 is '${got:-<unset>}', expected '$3'"
  fi
}

# --- The tree-wide scans (run in both modes) ---------------------------------
# Every TOML in the tree, plus the legacy extensionless cargo config. `target`
# and `.git` are pruned so a vendored dependency's own profile cannot fail the
# build; everything a contributor can edit is in scope, tracked or not.
toml_files() {
  find . -path ./.git -prune -o -path ./target -prune -o \
       \( -name '*.toml' -o -name 'config' -path '*/.cargo/*' \) -print 2>/dev/null
}

# --- Scan A: no profile, and no rustflag, sets panic = "abort" ---------------
# Two routes reach the abort strategy: a profile key (`panic = "abort"`) and a
# rustflag (`-C panic=abort`, which a .cargo/config.toml can set globally). Both
# are refused, because `check_panic_strategy` cannot tell them apart — it sees
# only the compiled result, and refuses to start.
abort_hits=""
for f in $(toml_files); do
  hits=$(sed 's/#.*$//' "$f" \
         | grep -nE '(^|[^-[:alnum:]_])panic[[:space:]]*=[[:space:]]*"?abort"?' || true)
  [ -n "$hits" ] && abort_hits="$abort_hits$f: $hits
"
  flag_hits=$(sed 's/#.*$//' "$f" | grep -nE 'panic=abort|panic[[:space:]]*=[[:space:]]*.abort' || true)
  [ -n "$flag_hits" ] && abort_hits="$abort_hits$f: $flag_hits
"
done
if [ -z "$abort_hits" ]; then
  pass "panic: no profile or rustflag anywhere sets panic = \"abort\" (execution::check_panic_strategy would refuse the run)"
else
  bad "panic: panic = \"abort\" is set — dagr's panic containment needs unwinding, and check_panic_strategy refuses to start under abort:"
  printf '%s' "$abort_hits" | sed 's/^/        /'
fi

# --- Scan B: strip is not enabled anywhere ----------------------------------
# `false` and `"none"` are the disabled spellings and are fine; `true`,
# `"symbols"`, and `"debuginfo"` all remove symbols the panic hook needs to name
# the node a panic came from.
strip_hits=""
for f in $(toml_files); do
  hits=$(sed 's/#.*$//' "$f" \
         | grep -nE '(^|[^-[:alnum:]_])strip[[:space:]]*=[[:space:]]*("?(true|symbols|debuginfo)"?)' || true)
  [ -n "$hits" ] && strip_hits="$strip_hits$f: $hits
"
done
if [ -z "$strip_hits" ]; then
  pass "strip: symbol stripping is not enabled anywhere (the panic hook attributes a panic to its node by symbol)"
else
  bad "strip: symbol stripping is enabled — dagr explains a run after the fact and needs the symbols:"
  printf '%s' "$strip_hits" | sed 's/^/        /'
fi

if [ -n "$scan_only" ]; then
  # Scan-only mode is the self-check's hermetic entry point: report and stop.
  if [ "$fail" -eq 0 ]; then echo "PROFILES=PASS"; else echo "PROFILES=FAIL"; fi
  exit "$fail"
fi

# --- Check 1: the release profile exists and carries the budgeted values -----
# arch.md "Performance envelope" budgets framework overhead per node at under a
# millisecond. Cargo's defaults ship `lto = false` and `codegen-units = 16`,
# which switches off exactly the cross-crate inlining a six-crate workspace with
# a hot per-node scheduling path benefits from most.
if toml_has_section "$manifest" "profile.release"; then
  pass "release: [profile.release] exists in the root manifest"
  expect_value profile.release opt-level 3 release
  expect_value profile.release lto fat release
  expect_value profile.release codegen-units 1 release
  expect_value profile.release panic unwind release
else
  bad "release: [profile.release] is absent from $manifest — release builds run at Cargo's defaults (lto off, 16 codegen units)"
fi

# --- Check 2: the panic setting is explicit AND carries its rationale --------
# Explicit rather than defaulted, so a consumer inheriting these settings cannot
# silently flip it; commented at the setting, so the next reader finds the reason
# where the decision is rather than in a changelog.
release_body=$(toml_section_body "$manifest" "profile.release" 2>/dev/null || true)
if printf '%s' "$release_body" | grep -q 'check_panic_strategy'; then
  pass "rationale: [profile.release] names check_panic_strategy at the setting"
else
  bad "rationale: [profile.release] does not name check_panic_strategy — the panic = \"unwind\" setting must carry its reason in a comment beside it"
fi

# --- Check 3: the bench profile keeps symbols -------------------------------
if toml_has_section "$manifest" "profile.bench"; then
  pass "bench: [profile.bench] exists in the root manifest"
  expect_value profile.bench inherits release bench
  expect_value profile.bench debug true bench
  expect_value profile.bench strip false bench
else
  bad "bench: [profile.bench] is absent from $manifest — a profiled binary needs symbols"
fi

# --- Check 4: dev/test builds optimize DEPENDENCIES only --------------------
# `package."*"` reaches dependencies, never the workspace's own crates, so
# first-party rebuilds stay fast while the test suite (including the scale
# benchmark, which runs under the dev profile) stops paying debug-mode tokio.
if toml_has_section "$manifest" 'profile.dev.package."*"'; then
  pass 'dev-deps: [profile.dev.package."*"] exists in the root manifest'
  expect_value 'profile.dev.package."*"' opt-level 3 dev-deps
else
  bad 'dev-deps: [profile.dev.package."*"] is absent from '"$manifest"' — dev and test builds run unoptimized dependencies'
fi

# --- Check 5: no member manifest declares a profile -------------------------
# Cargo ignores `[profile.*]` outside the workspace root (with a warning), so a
# profile in a member is a silently-inert setting. Keep the decision in one file.
member_profiles=""
for f in crates/*/Cargo.toml; do
  [ -f "$f" ] || continue
  hits=$(grep -nE '^[[:space:]]*\[profile\.' "$f" || true)
  [ -n "$hits" ] && member_profiles="$member_profiles$f: $hits
"
done
if [ -z "$member_profiles" ]; then
  pass "single-source: no member manifest declares a [profile.*] (Cargo would ignore it)"
else
  bad "single-source: a member manifest declares a [profile.*], which Cargo ignores outside the workspace root:"
  printf '%s' "$member_profiles" | sed 's/^/        /'
fi

# --- Check 6: the scans above are non-vacuous -------------------------------
# Drive the two scans against a fixture that DOES set both forbidden values. If
# the fixture comes back clean the scans are decorative, and this script would be
# lying about the guarantee it exists to hold.
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
cat >"$fixture/Cargo.toml" <<'FIXTURE'
[profile.release]
panic = "abort"
strip = true
FIXTURE
if bash "$self" --scan-only "$fixture" >"$fixture/out" 2>&1; then
  bad "self-check: the scans reported a fixture with panic = \"abort\" and strip = true as clean — they are vacuous"
else
  if grep -q 'panic = "abort" is set' "$fixture/out" && grep -q 'stripping is enabled' "$fixture/out"; then
    pass "self-check: both scans reject a fixture that sets panic = \"abort\" and strip = true (non-vacuous)"
  else
    bad "self-check: the fixture failed, but not for the two expected reasons: $(tr '\n' ' ' <"$fixture/out")"
  fi
fi

echo "---"
if [ "$fail" -eq 0 ]; then
  echo "PROFILES=PASS"
else
  echo "PROFILES=FAIL"
fi
exit "$fail"
