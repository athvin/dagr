#!/usr/bin/env bash
# The lint policy agrees with itself — ticket 111 (T96).
#
# dagr's lint contract is written down twice, deliberately:
#
#   * `lints.toml` at the repository root is the SOURCE OF TRUTH (authored by
#     T0.0a), shaped exactly like a `[workspace.lints]` table so applying it is a
#     copy rather than a translation;
#   * `Cargo.toml`'s `[workspace.lints]` is the APPLIED form Cargo actually reads.
#
# Two files carrying one decision drift. They are a documented pair with no other
# guard, so this script asserts they agree field for field — every table, every
# key, every level — plus the three things that make the pair meaningful:
#
#   * every member opts in with `[lints] workspace = true` (a member that forgets
#     inherits nothing and silently ships unlinted);
#   * `missing_docs` and `clippy::missing_errors_doc` are `deny`, not `warn`
#     (T96's ratchet: both were already effectively denied by `warnings = "deny"`,
#     so this makes the intent readable AT the setting rather than derived);
#   * no crate-level `#![allow]` escape hatch reintroduces what the tables deny.
#
# Non-vacuous by construction: after the real checks pass, the script re-invokes
# ITSELF in `--scan-only` mode against a fixture whose two files disagree, and
# fails if that fixture is reported clean.
#
# Usage:
#   check-lint-parity.sh              # full gate, from the repository root
#   check-lint-parity.sh --scan-only DIR
#
# Exit 0 = the pair agrees, 1 = at least one failure, 2 = bad invocation.
set -u

scan_only=""
case "${1:-}" in
  --scan-only) scan_only="${2:?--scan-only needs a directory}" ;;
  -h|--help)   sed -n '1,30p' "$0"; exit 0 ;;
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

# section_pairs FILE SECTION -> one `key=value` line per setting, sorted.
# Comments are stripped, so the rationale each setting carries cannot be read as
# data; whitespace inside a table value is normalized so the two spellings of the
# same setting compare equal.
section_pairs() {
  awk -v want="$2" '
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
      v = line; sub(/^[^=]*=/, "", v)
      sub(/[[:space:]]#.*$/, "", v)
      gsub(/[[:space:]]/, "", v)
      print k "=" v
    }
  ' "$1" | LC_ALL=C sort
}

for table in rust clippy rustdoc; do
  src=$(section_pairs lints.toml "$table")
  applied=$(section_pairs Cargo.toml "workspace.lints.$table")
  if [ "$src" = "$applied" ]; then
    if [ -z "$src" ]; then
      bad "parity: [$table] is empty in both lints.toml and [workspace.lints.$table] — the policy is not written anywhere"
    else
      pass "parity: lints.toml [$table] and Cargo.toml [workspace.lints.$table] agree field for field"
    fi
  else
    bad "parity: lints.toml [$table] and Cargo.toml [workspace.lints.$table] disagree:"
    diff <(printf '%s\n' "$src") <(printf '%s\n' "$applied") \
      | sed 's/^/        /' || true
  fi
done

# --- The ratchet itself: both lints are denied, in both files ---------------
# `missing_docs` and `clippy::missing_errors_doc` are allow-by-default rustc /
# clippy lints. Setting them to `warn` turns them on and `warnings = "deny"` then
# promotes them, so they were ALREADY effectively denied — the ratchet makes that
# readable at the setting and removes a deferral note that had become false.
expect_level() { # expect_level FILE SECTION KEY EXPECTED LABEL
  got=$(section_pairs "$1" "$2" | sed -n "s/^$3=//p")
  if [ "$got" = "$4" ]; then
    pass "ratchet: $5 sets $3 = $4"
  else
    bad "ratchet: $5 sets $3 = '${got:-<unset>}', expected '$4'"
  fi
}
expect_level lints.toml rust                     missing_docs         '"deny"' "lints.toml [rust]"
expect_level Cargo.toml workspace.lints.rust     missing_docs         '"deny"' "Cargo.toml [workspace.lints.rust]"
expect_level lints.toml clippy                   missing_errors_doc   '"deny"' "lints.toml [clippy]"
expect_level Cargo.toml workspace.lints.clippy   missing_errors_doc   '"deny"' "Cargo.toml [workspace.lints.clippy]"

# --- Every member opts in ----------------------------------------------------
members=$(ls -d crates/*/ 2>/dev/null | sed 's:/$::')
missing_optin=""
for dir in $members; do
  [ -f "$dir/Cargo.toml" ] || continue
  if [ "$(section_pairs "$dir/Cargo.toml" lints)" != "workspace=true" ]; then
    missing_optin="$missing_optin$dir/Cargo.toml
"
  fi
done
if [ -z "$missing_optin" ]; then
  pass "opt-in: every member declares [lints] workspace = true"
else
  bad "opt-in: a member does not inherit the workspace lint policy:"
  printf '%s' "$missing_optin" | sed 's/^/        /'
fi

# --- No crate-level escape hatch --------------------------------------------
crate_allows=$(grep -rn '^#!\[allow(' crates/*/src 2>/dev/null || true)
if [ -z "$crate_allows" ]; then
  pass "no-escape: no crate root carries a blanket #![allow(...)]"
else
  bad "no-escape: a crate root silences the workspace policy wholesale:"
  printf '%s\n' "$crate_allows" | sed 's/^/        /'
fi

# --- Suppressions expire: #[expect], not #[allow] ---------------------------
# `#[expect]` (stable since 1.81) warns when the suppressed lint STOPS firing,
# so a suppression that outlives its cause becomes visible instead of
# accumulating silently. An `#[allow]` never expires. Production `src/` is held
# to `#[expect]`; the one structural exception is a suppression inside a
# macro-generated impl, where the covered lint fires for some expansions and not
# others, so `#[expect]` would report "unfulfilled" for the fulfilled ones — it
# must carry `EXPECT-EXEMPT:` on the line above, stating which.
prod_allows=""
for f in $(find crates/*/src -name '*.rs' 2>/dev/null); do
  hits=$(grep -n '#\[allow(' "$f" || true)
  [ -z "$hits" ] && continue
  for lineno in $(printf '%s\n' "$hits" | cut -d: -f1); do
    prev=$((lineno - 1))
    if [ "$prev" -ge 1 ] && sed -n "${prev}p" "$f" | grep -q 'EXPECT-EXEMPT:'; then
      continue
    fi
    prod_allows="$prod_allows$f:$lineno: $(sed -n "${lineno}p" "$f")
"
  done
done
if [ -z "$prod_allows" ]; then
  pass "expect-over-allow: every production suppression is an #[expect] (it expires loudly when its cause goes away)"
else
  bad "expect-over-allow: a production #[allow] never expires — convert it to #[expect(…, reason = \"…\")], or mark it EXPECT-EXEMPT: with the reason:"
  printf '%s' "$prod_allows" | sed 's/^/        /'
fi

# Every suppression states WHY, whichever form it takes.
reasonless=""
for f in $(find crates/*/src -name '*.rs' 2>/dev/null); do
  # Attributes may span lines; take the 8 lines from the attribute's start and
  # require a `reason =` before the closing `)]`.
  for lineno in $(grep -n '#\[\(expect\|allow\)(' "$f" | cut -d: -f1); do
    block=$(sed -n "${lineno},$((lineno + 8))p" "$f" | awk '{print} /\)\]/{exit}')
    printf '%s' "$block" | grep -q 'reason[[:space:]]*=' || {
      reasonless="$reasonless$f:$lineno: $(sed -n "${lineno}p" "$f")
"
    }
  done
done
if [ -z "$reasonless" ]; then
  pass "expect-reason: every production suppression carries a reason = \"…\""
else
  bad "expect-reason: a production suppression states no reason:"
  printf '%s' "$reasonless" | sed 's/^/        /'
fi

if [ -n "$scan_only" ]; then
  if [ "$fail" -eq 0 ]; then echo "LINT_PARITY=PASS"; else echo "LINT_PARITY=FAIL"; fi
  exit "$fail"
fi

# --- The scans are non-vacuous ----------------------------------------------
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/crates/drifted/src"
cat >"$fixture/lints.toml" <<'FIXTURE'
[rust]
warnings = "deny"
missing_docs = "warn"

[clippy]
missing_errors_doc = "warn"

[rustdoc]
broken_intra_doc_links = "deny"
FIXTURE
cat >"$fixture/Cargo.toml" <<'FIXTURE'
[workspace.lints.rust]
warnings = "deny"
missing_docs = "allow"

[workspace.lints.clippy]
missing_errors_doc = "warn"

[workspace.lints.rustdoc]
broken_intra_doc_links = "deny"
FIXTURE
cat >"$fixture/crates/drifted/Cargo.toml" <<'FIXTURE'
[package]
name = "drifted"
FIXTURE
cat >"$fixture/crates/drifted/src/lib.rs" <<'FIXTURE'
#![allow(missing_docs)]

#[allow(dead_code)]
fn stale() {}

#[expect(dead_code)]
fn unexplained() {}
FIXTURE
if bash "$self" --scan-only "$fixture" >"$fixture/out" 2>&1; then
  bad "self-check: the scans reported a fixture whose two files disagree, whose ratchet is off, whose member does not opt in, and whose crate root carries a blanket allow as clean — they are vacuous"
else
  missed=""
  grep -q 'disagree'                       "$fixture/out" || missed="$missed parity"
  grep -q "ratchet:.*missing_docs"         "$fixture/out" || missed="$missed ratchet-missing-docs"
  grep -q "ratchet:.*missing_errors_doc"   "$fixture/out" || missed="$missed ratchet-missing-errors-doc"
  grep -q 'does not inherit'               "$fixture/out" || missed="$missed opt-in"
  grep -q 'silences the workspace policy'  "$fixture/out" || missed="$missed no-escape"
  grep -q 'production #\[allow\] never expires' "$fixture/out" || missed="$missed expect-over-allow"
  grep -q 'states no reason'               "$fixture/out" || missed="$missed expect-reason"
  if [ -z "$missed" ]; then
    pass "self-check: every scan rejects a fixture that violates it (non-vacuous)"
  else
    bad "self-check: the fixture failed, but these scans did not fire:$missed"
  fi
fi

echo "---"
if [ "$fail" -eq 0 ]; then
  echo "LINT_PARITY=PASS"
else
  echo "LINT_PARITY=FAIL"
fi
exit "$fail"
