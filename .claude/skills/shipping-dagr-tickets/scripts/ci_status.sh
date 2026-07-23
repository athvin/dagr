#!/usr/bin/env bash
# Normalized CI verdict for a PR, safe against the gh footguns:
#   - `gh pr checks` exits 1 for BOTH "checks failed" and "no checks configured"
#     (and its 'cancel' bucket is not a pass) — so this script never uses it.
#   - statusCheckRollup is empty for a few minutes after a push while GitHub
#     registers the workflow run — callers pass --grace to treat that window
#     as PENDING instead of NO_CHECKS.
#
# Usage: ci_status.sh <pr-number> [--wait <seconds>] [--grace <seconds>]
#
# Prints KEY=VALUE lines then a final VERDICT= line:
#   PASS       every check concluded success/neutral/skipped
#   FAIL       any check concluded failure/cancelled/timed_out/action_required/
#              startup_failure/error/stale (FAILING= lines name them, RUN_ID=
#              points the fixer at the head run)
#   PENDING    checks still queued/running (or empty rollup within --grace)
#   NO_CHECKS  no checks on the head commit — NORMAL before ticket 006 lands
#   ANOMALY    PR not open, or an unrecognized status/conclusion value
#
# Exit codes: 0 PASS · 1 FAIL · 2 PENDING · 3 NO_CHECKS · 4 ANOMALY
set -u

pr=${1:?usage: ci_status.sh <pr-number> [--wait <seconds>] [--grace <seconds>]}
shift
wait_cap=0
grace=0
while [ $# -gt 0 ]; do
  case "$1" in
    --wait)  wait_cap=$2; shift 2 ;;
    --grace) grace=$2; shift 2 ;;
    *) echo "VERDICT=ANOMALY"; echo "ERROR=unknown flag $1"; exit 4 ;;
  esac
done

here=$(cd "$(dirname "$0")" && pwd)

# 30s poll interval: check runs update at ~minute granularity, finer polling
# only burns API rate limit (~120 calls/hr at 30s is far inside limits).
POLL_INTERVAL=30

run_once() {
  local json out rc ref
  json=$(gh pr view "$pr" --json statusCheckRollup,headRefOid,headRefName,state,mergeable 2>/dev/null) || {
    echo "VERDICT=ANOMALY"; echo "ERROR=gh pr view failed for PR #$pr"; return 4
  }
  out=$(printf '%s' "$json" | python3 "$here/classify_checks.py"); rc=$?
  echo "$out"
  if [ $rc -eq 1 ]; then
    # Hand the fixer the head run id so it can fetch logs itself.
    ref=$(echo "$out" | sed -n 's/^HEAD_REF=//p')
    gh run list --branch "$ref" --limit 1 --json databaseId \
      --jq '"RUN_ID=" + (.[0].databaseId | tostring)' 2>/dev/null || true
  fi
  return $rc
}

if [ "$wait_cap" -eq 0 ]; then
  run_once; rc=$?
  if [ $rc -eq 3 ] && [ "$grace" -gt 0 ]; then
    echo "NOTE=within-grace treat as PENDING"; exit 2
  fi
  exit $rc
fi

elapsed=0
while :; do
  out=$(run_once); rc=$?
  if [ $rc -eq 2 ] || { [ $rc -eq 3 ] && [ "$elapsed" -lt "$grace" ]; }; then
    if [ "$elapsed" -ge "$wait_cap" ]; then
      echo "$out"
      echo "NOTE=wait cap ${wait_cap}s reached"
      exit 2
    fi
    sleep $POLL_INTERVAL
    elapsed=$((elapsed + POLL_INTERVAL))
    continue
  fi
  echo "$out"
  exit $rc
done
