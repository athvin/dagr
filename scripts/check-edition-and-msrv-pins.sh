#!/usr/bin/env bash
# Edition/MSRV pin consistency and edition-2024 migration surface, for ticket
# 109 (T94).
#
# dagr's MSRV is not a documented aspiration — it is a *pin*, and the pin is what
# makes trybuild/UI diagnostics byte-reproducible across machines (arch.md
# "Stability": MSRV pinned in the workspace, documented in the README; raising it
# is a minor version bump called out in release notes). That pin is spelled out in
# SIX places, and the whole guarantee collapses the moment they disagree:
#
#   1. Cargo.toml            [workspace.package] rust-version  (+ edition, resolver)
#   2. rust-toolchain.toml   [toolchain] channel               (the source of truth)
#   3. rustfmt.toml          edition
#   4. README.md             the "MSRV" line
#   5. scripts/check-stability-and-criteria.sh   names the concrete MSRV
#   6. crates/core/tests/ui.rs                   the pinned-toolchain doc + the
#                                                `--edition` it hands to rustc
#
# This script asserts they agree, and then asserts something stronger that no
# per-file check can: that **nowhere else in the tree** does a stale Rust version
# or a stale `edition = "…"` survive. A migration that updates five of six sites
# looks fine in review and is wrong.
#
# `docs/implementation/*.md` is EXCLUDED by design. Those files are historical
# records of what was true when each ticket shipped; rewriting them to the current
# pin would falsify the record (ticket 109 DoD: "historical tickets are *not*
# rewritten to the new version"). `.claude/skills/` is excluded from the EDITION
# scan only: it is vendored upstream guidance whose examples legitimately show
# other editions.
#
# Two migration-surface invariants ride along, because they are the two things
# edition 2024 changed about dagr's own code and both are DoD lines of ticket 109
# that a reviewer would otherwise have to re-derive by eye:
#
#   * `unsafe_op_in_unsafe_fn` is deny-by-default in 2024, so each unsafe
#     OPERATION carries its own `unsafe { }` block and its own `// SAFETY:`
#     comment (`unsafe-minimize-scope`, `unsafe-safety-comment`). Asserted as: no
#     `unsafe { }` block anywhere without a `// SAFETY:` above it, and at least as
#     many `// SAFETY:` comments as blocks in every file that has one.
#   * `gen` is a reserved keyword in 2024. It was renamed, NOT escaped, so the
#     `r#gen` raw-identifier escape must not appear (ticket 109 Objective).
#
# The tree scan is non-vacuous by construction: after the real checks pass, the
# script re-invokes ITSELF in `--scan-only` mode against a throwaway fixture that
# does name a stale version and a stale edition, and fails if that fixture is
# reported clean. A guard that cannot fail is not a guard.
#
# Usage:
#   check-toolchain-pin-consistency.sh              # full gate, from the repo root
#   check-toolchain-pin-consistency.sh --scan-only DIR
#
# Exit 0 = every site agrees, 1 = at least one disagreement, 2 = bad invocation.
set -u

# The edition this workspace is migrated to. Written as a literal on purpose: it
# is the floor, so silently reverting the workspace to an older edition fails
# here rather than passing an "everything agrees" check at edition 2021.
EXPECTED_EDITION="2024"

# Rust-release-shaped strings this scan recognises: any 1.8x or 1.9x release with a
# patch component. Narrow on purpose — it must not match dependency versions
# ("1.0", "0.6") or schema versions. Widen the minor range when Rust reaches 1.100.
# Written as a pattern, never as an example release, so this file does not trip its
# own tree scan.
VERSION_RE='1\.[89][0-9]\.[0-9]+'

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

# --- the pin itself -----------------------------------------------------------
# rust-toolchain.toml is the single source of truth: every other site must match
# it, never the other way round.
pin_of() {
  sed -nE 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "$1" | head -1
}

# scalar KEY in the whole file, quotes/comments stripped (the manifests here have
# one occurrence of each key we read, and comment lines are skipped).
scalar() {
  awk -v key="$2" '
    { line = $0; sub(/^[[:space:]]+/, "", line) }
    line ~ /^#/ || line == "" { next }
    {
      k = line; sub(/=.*$/, "", k); gsub(/[[:space:]]/, "", k)
      if (k != key) next
      v = line; sub(/^[^=]*=/, "", v); sub(/#.*$/, "", v)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", v); gsub(/"/, "", v)
      print v; exit
    }
  ' "$1"
}

# --- the tree scans (also reachable via --scan-only) ---------------------------
# Files to scan: tracked files when we are in a git repo (so build output and
# untracked scratch never trip the gate), a plain find otherwise (the fixture).
scan_files() {
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git ls-files
  else
    find . -type f | sed 's|^\./||'
  fi
}

# scan_stale_versions PIN — every Rust-release-shaped string in the tree must be
# the pin. Historical ticket records are excluded (see the header).
scan_stale_versions() {
  local pin=$1 hits
  hits=$(scan_files \
    | grep -v '^docs/implementation/' \
    | while IFS= read -r f; do [ -f "$f" ] && printf '%s\n' "$f"; done \
    | xargs grep -oEn "$VERSION_RE" 2>/dev/null \
    | grep -v ":${pin}\$" || true)
  if [ -z "$hits" ]; then
    pass "tree: no Rust version other than the pin ($pin) is named outside docs/implementation/"
  else
    bad "tree: a stale Rust version survives outside docs/implementation/ (pin is $pin):"
    printf '%s\n' "$hits" | sed 's/^/        /'
  fi
}

# scan_stale_editions EDITION — every `edition = "…"` declaration in the tree must
# be the workspace edition. Vendored skill guidance is excluded (see the header).
scan_stale_editions() {
  local want=$1 hits
  hits=$(scan_files \
    | grep -v '^docs/implementation/' \
    | grep -v '^\.claude/skills/' \
    | while IFS= read -r f; do [ -f "$f" ] && printf '%s\n' "$f"; done \
    | xargs grep -nE '^[[:space:]]*edition[[:space:]]*=' 2>/dev/null \
    | grep -vE "\"${want}\"" || true)
  if [ -z "$hits" ]; then
    pass "tree: every declared edition is \"$want\""
  else
    bad "tree: an edition declaration disagrees with the workspace edition \"$want\":"
    printf '%s\n' "$hits" | sed 's/^/        /'
  fi
}

if [ -n "$scan_only" ]; then
  pin=$(pin_of rust-toolchain.toml 2>/dev/null || true)
  scan_stale_versions "${pin:-none}"
  scan_stale_editions "$EXPECTED_EDITION"
  [ $fail -eq 0 ] && { echo "OK  scan clean"; exit 0; }
  echo "FAILED  scan found stale pins"; exit 1
fi

# --- 2. rust-toolchain.toml: a CONCRETE channel, never a floating one ----------
for f in Cargo.toml rust-toolchain.toml rustfmt.toml README.md \
         scripts/check-stability-and-criteria.sh crates/core/tests/ui.rs; do
  [ -f "$f" ] || { bad "pin site missing: $f"; }
done
[ $fail -eq 0 ] || { echo; echo "FAILED  a pin site is missing"; exit 1; }

pin=$(pin_of rust-toolchain.toml)
if printf '%s' "$pin" | grep -qE "^${VERSION_RE}\$"; then
  pass "rust-toolchain.toml: pinned to the concrete channel $pin (not a floating stable/nightly)"
else
  bad "rust-toolchain.toml: channel '$pin' is not a concrete x.y.z — snapshots need an exact toolchain"
  echo; echo "FAILED  no usable pin to compare against"; exit 1
fi

# --- 1. Cargo.toml: rust-version, edition, resolver ---------------------------
rv=$(scalar Cargo.toml rust-version)
if [ "$rv" = "$pin" ]; then
  pass "Cargo.toml: [workspace.package] rust-version = \"$rv\" matches the pin"
else
  bad "Cargo.toml: rust-version '$rv' != pinned channel '$pin'"
fi

ed=$(scalar Cargo.toml edition)
if [ "$ed" = "$EXPECTED_EDITION" ]; then
  pass "Cargo.toml: [workspace.package] edition = \"$ed\""
else
  bad "Cargo.toml: edition '$ed' != expected '$EXPECTED_EDITION'"
fi

res=$(scalar Cargo.toml resolver)
if [ "$res" = "3" ]; then
  pass "Cargo.toml: resolver = \"3\" (the MSRV-aware edition-2024 resolver)"
else
  bad "Cargo.toml: resolver '$res' != \"3\" — edition $EXPECTED_EDITION wants the MSRV-aware resolver"
fi

# --- 3. rustfmt.toml ----------------------------------------------------------
fmt_ed=$(scalar rustfmt.toml edition)
if [ "$fmt_ed" = "$ed" ]; then
  pass "rustfmt.toml: edition = \"$fmt_ed\" matches the workspace edition"
else
  bad "rustfmt.toml: edition '$fmt_ed' != workspace edition '$ed' — cargo fmt would reformat against the wrong edition"
fi

# --- 4. README.md: the MSRV line ---------------------------------------------
msrv_line=$(grep -nE 'MSRV: Rust' README.md | head -1)
if printf '%s' "$msrv_line" | grep -q "$pin"; then
  pass "README.md: the MSRV line names the pin ($pin)"
else
  bad "README.md: no 'MSRV: Rust $pin' line found (got: ${msrv_line:-none})"
fi
if grep -qi 'minor version bump' README.md; then
  pass "README.md: records that raising the MSRV is a minor version bump (arch.md \"Stability\")"
else
  bad "README.md: the MSRV section must state the release-notes implication (minor version bump)"
fi

# --- 5. the stability/criteria gate asserts the same concrete MSRV -----------
# It must DERIVE the pin from rust-toolchain.toml, not carry a literal. Its MSRV
# assertion is scoped to the ADR embedded in ticket 005, and ticket 109's DoD
# forbids rewriting `docs/implementation/*.md` to a new version — so a literal
# there would either force that rewrite or silently go stale at the next bump.
# Deriving it makes the gate correct at every future pin, forever.
if grep -q 'rust-toolchain.toml' scripts/check-stability-and-criteria.sh; then
  pass "check-stability-and-criteria.sh: derives the concrete MSRV ($pin) from rust-toolchain.toml"
else
  bad "check-stability-and-criteria.sh: must read the pin from rust-toolchain.toml, not hardcode a version"
fi
# And it must actually assert the pin it read (the tree scan above already proves
# no stale literal survives in it).
if bash scripts/check-stability-and-criteria.sh >/dev/null 2>&1; then
  pass "check-stability-and-criteria.sh: passes against the migrated tree"
else
  bad "check-stability-and-criteria.sh: fails against the migrated tree"
fi

# --- 6. the UI harness: doc pin AND the edition it compiles samples with -----
if grep -q "$pin" crates/core/tests/ui.rs; then
  pass "crates/core/tests/ui.rs: the pinned-toolchain doc names $pin"
else
  bad "crates/core/tests/ui.rs: the pinned-toolchain doc does not name $pin"
fi
# The harness shells out to rustc with an explicit --edition; if that argument
# lags the workspace, every UI sample is checked under the OLD edition and the
# whole compile-fail corpus silently stops testing what ships.
if grep -qE "^[[:space:]]*\.arg\(\"${ed}\"\)" crates/core/tests/ui.rs; then
  pass "crates/core/tests/ui.rs: compiles samples with --edition $ed"
else
  bad "crates/core/tests/ui.rs: the rustc --edition argument is not \"$ed\" — samples would be checked under the wrong edition"
fi

# --- the workflow's prose pin ------------------------------------------------
if [ -f .github/workflows/ci.yml ]; then
  stale=$(grep -oEn "$VERSION_RE" .github/workflows/ci.yml | grep -v ":${pin}\$" || true)
  if [ -z "$stale" ]; then
    pass "ci.yml: every Rust version it names is the pin ($pin)"
  else
    bad "ci.yml: names a stale Rust version:"
    printf '%s\n' "$stale" | sed 's/^/        /'
  fi
fi

# --- tree-wide: nothing stale survives anywhere ------------------------------
scan_stale_versions "$pin"
scan_stale_editions "$ed"

# --- edition-2024 migration surface: unsafe blocks and the `gen` keyword -----
# Every `unsafe { }` block (code, not prose) must have a `// SAFETY:` comment
# within the few lines above it — the run of comments and attributes that
# introduces it — and no file may carry more unsafe blocks than SAFETY comments.
# Together those two say "one justification per operation" mechanically, which is
# exactly what edition 2024's `unsafe_op_in_unsafe_fn` asks a human to do.
undocumented=$(
  git ls-files 'crates/**/*.rs' \
    | while IFS= read -r f; do
        awk -v file="$f" '
          { lines[NR] = $0 }
          {
            t = $0; sub(/^[[:space:]]+/, "", t)
            if (t ~ /^\/\//) next                       # prose, not code
            if ($0 !~ /unsafe[[:space:]]*\{/) next
            blocks++
            found = 0
            for (i = NR - 1; i >= NR - 8 && i >= 1; i--) {
              p = lines[i]; sub(/^[[:space:]]+/, "", p)
              if (p ~ /^\/\/[[:space:]]*SAFETY:/) { found = 1; break }
            }
            if (!found) printf "%s:%d: unsafe block with no `// SAFETY:` above it\n", file, NR
          }
          END {
            if (blocks > 0) {
              safety = 0
              for (i = 1; i <= NR; i++) {
                p = lines[i]; sub(/^[[:space:]]+/, "", p)
                if (p ~ /^\/\/[[:space:]]*SAFETY:/) safety++
              }
              if (safety < blocks)
                printf "%s: %d unsafe blocks but only %d `// SAFETY:` comments\n", file, blocks, safety
            }
          }
        ' "$f"
      done
)
if [ -z "$undocumented" ]; then
  pass "unsafe: every unsafe block is introduced by its own \`// SAFETY:\` comment"
else
  bad "unsafe: an unsafe operation is undocumented (edition $EXPECTED_EDITION requires per-operation blocks):"
  printf '%s\n' "$undocumented" | sed 's/^/        /'
fi

# `unsafe_op_in_unsafe_fn` is only really satisfied when the operations sit in
# blocks INSIDE the unsafe fn, so an allocator impl must carry at least one block
# per `unsafe fn` — a file with four unsafe fns and no blocks compiles under 2021
# and is a hard error under 2024.
for f in crates/core/src/metrics.rs crates/cli/tests/bounded_memory_chain.rs; do
  [ -f "$f" ] || { bad "allocator file missing: $f"; continue; }
  fns=$(grep -cE '^[[:space:]]*unsafe fn ' "$f" || true)
  blks=$(grep -vE '^[[:space:]]*//' "$f" | grep -cE 'unsafe[[:space:]]*\{' || true)
  if [ "$fns" -gt 0 ] && [ "$blks" -ge "$fns" ]; then
    pass "unsafe: $f has $blks per-operation unsafe blocks for $fns \`unsafe fn\`s"
  else
    bad "unsafe: $f has $fns \`unsafe fn\`s but only $blks inner \`unsafe { }\` blocks"
  fi
done

# Code only: a comment explaining *why* the escape was not used is not a use of it.
escaped_gen=$(git grep -n 'r#gen' -- 'crates/**/*.rs' 2>/dev/null \
  | grep -vE ':[0-9]+:[[:space:]]*(//|/\*|\*)' || true)
if [ -n "$escaped_gen" ]; then
  bad "gen: the reserved-keyword escape \`r#gen\` is used — ticket 109 renames the identifier instead:"
  printf '%s\n' "$escaped_gen" | sed 's/^/        /'
else
  pass "gen: the 2024 reserved keyword is renamed, not escaped with \`r#gen\`"
fi

# --- self-check: prove both scans can actually fail --------------------------
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
# The fixture's stale version is DERIVED from the pin (one minor behind) rather
# than written as a literal, so this script never names a release and so cannot
# trip its own tree scan.
stale_minor=$(( $(printf '%s' "$pin" | cut -d. -f2) - 1 ))
printf '[toolchain]\nchannel = "%s"\n' "$pin" >"$fixture/rust-toolchain.toml"
printf '**MSRV: Rust 1.%s.0.**\n' "$stale_minor" >"$fixture/README.md"
printf 'edition = "2015"\n' >"$fixture/rustfmt.toml"
if bash "$self" --scan-only "$fixture" >/dev/null 2>&1; then
  bad "self-check: the scans reported a fixture with a stale version AND a stale edition as clean"
else
  pass "self-check: both tree scans fail on a fixture that names a stale version and a stale edition"
fi

echo
if [ $fail -eq 0 ]; then
  echo "OK  all six pin sites agree on $pin / edition $ed, nothing stale survives the"
  echo "    tree, and the edition-$ed unsafe/keyword migration surface holds"
  exit 0
fi
echo "FAILED  the edition/MSRV pin or the edition-2024 migration surface is inconsistent"
exit 1
