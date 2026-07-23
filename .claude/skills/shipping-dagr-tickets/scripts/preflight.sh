#!/usr/bin/env bash
# Repo-state and resume probe for the shipping-dagr-tickets loop.
#
# Repo mode:    preflight.sh
#   Asserts the session can start: on main, tree clean, main synced, gh authed.
#   Prints KEY=VALUE lines and a final PREFLIGHT=PASS|FAIL.
#
# Branch mode:  preflight.sh <branch> <nnn>
#   Probes where a (possibly interrupted) ticket stands and prints a single
#   ROUTE= line the orchestrator switches on:
#     FRESH          no branch, no PR, box unchecked — start from scratch
#     CONTINUE_IMPL  branch exists with commits, no PR — resume implementation
#     POLL_CI        open PR — go poll CI (fix rounds decided by the verdict)
#     FLIP_BOX       PR merged but box unchecked — finish the bookkeeping
#     DONE           PR merged and box checked — nothing to do
#     STOP_AMBIGUOUS closed-unmerged PR (human veto), diverged branches, or
#                    any state the loop must not guess around
#   Leaves the checkout on the ticket branch for CONTINUE_IMPL/POLL_CI routes.
set -u

# Retry wrapper for network calls: gh/git hiccups are common on long
# unattended runs; 3 tries with 2s/8s backoff outlasts transient failures
# without hiding a real outage.
net() {
  local i
  for i in 1 2 3; do
    if "$@"; then return 0; fi
    if [ "$i" -eq 1 ]; then sleep 2; elif [ "$i" -eq 2 ]; then sleep 8; fi
  done
  return 1
}

fail=0
say() { echo "$1"; }
bad() { say "$1"; fail=1; }

if [ $# -eq 0 ]; then
  # ---------- repo mode ----------
  net git fetch origin --quiet || { say "FETCH=FAIL"; say "PREFLIGHT=FAIL"; exit 1; }

  branch=$(git rev-parse --abbrev-ref HEAD)
  if [ "$branch" = "main" ]; then say "BRANCH=main"; else bad "BRANCH=$branch (expected main)"; fi

  # --untracked-files=no: untracked noise (.DS_Store, operator notes) must
  # never wedge an unattended loop; only tracked modifications block.
  if [ -z "$(git status --porcelain --untracked-files=no)" ]; then
    say "CLEAN=yes"
  else
    bad "CLEAN=no"
  fi

  if [ "$(git rev-parse main)" = "$(git rev-parse origin/main)" ]; then
    say "SYNCED=yes"
  elif git merge-base --is-ancestor main origin/main && [ "$branch" = "main" ] \
       && git merge --ff-only --quiet origin/main; then
    say "SYNCED=fast-forwarded"   # behind but clean: solve, don't defer
  else
    bad "SYNCED=no (diverged or not fast-forwardable)"
  fi

  if net gh auth status >/dev/null 2>&1; then
    say "GH_AUTH=yes"
  else
    bad "GH_AUTH=no"
  fi

  # 'workflow' OAuth scope is required to push .github/workflows/ files;
  # without it ticket 006's push is rejected with a cryptic error, so
  # surface it at session start. Only fatal at ticket 006 (SKILL.md decides).
  if gh auth status 2>&1 | grep -q "workflow"; then
    say "WORKFLOW_SCOPE=yes"
  else
    say "WORKFLOW_SCOPE=no"
  fi

  # Red main = stop before cutting any branch (meaningful once CI exists).
  if [ -d .github/workflows ]; then
    run=$(gh run list --branch main --limit 1 --json status,conclusion \
      --jq '.[0] | .status + ":" + (.conclusion // "")' 2>/dev/null || true)
    case "$run" in
      completed:success) say "MAIN_CI=green" ;;
      "")                say "MAIN_CI=none" ;;
      completed:*)       bad "MAIN_CI=red ($run)" ;;
      *)                 say "MAIN_CI=running" ;;
    esac
  else
    say "MAIN_CI=not-configured"
  fi

  for tool in python3 gh git; do
    command -v "$tool" >/dev/null || bad "TOOL_$tool=missing"
  done

  if [ $fail -eq 0 ]; then say "PREFLIGHT=PASS"; else say "PREFLIGHT=FAIL"; fi
  exit $fail
fi

# ---------- branch mode ----------
ticket_branch=$1
nnn=$2

net git fetch origin --quiet || { say "FETCH=FAIL"; say "ROUTE=STOP_AMBIGUOUS"; exit 1; }

box=$(python3 "$(dirname "$0")/next_ticket.py" --ticket "$nnn" | sed -n 's/^BOX=//p')
say "BOX=${box:-unknown}"

pr_info=$(net gh pr list --head "$ticket_branch" --state all --json number,state \
  --jq 'map("\(.number):\(.state)") | join(",")' 2>/dev/null || true)
say "PR=${pr_info:-none}"

has_local=no; git show-ref --verify --quiet "refs/heads/$ticket_branch" && has_local=yes
has_remote=no; git show-ref --verify --quiet "refs/remotes/origin/$ticket_branch" && has_remote=yes
say "LOCAL_BRANCH=$has_local"
say "REMOTE_BRANCH=$has_remote"

ahead=0; behind=0
if [ "$has_local" = yes ] && [ "$has_remote" = yes ]; then
  read -r behind ahead <<EOF
$(git rev-list --left-right --count "origin/$ticket_branch...$ticket_branch")
EOF
  say "AHEAD=$ahead"; say "BEHIND=$behind"
fi

route=""
case "$pr_info" in
  *,*)      say "ROUTE=STOP_AMBIGUOUS"; say "REASON=multiple PRs for one branch"; exit 1 ;;
  *:MERGED)
    if [ "$box" = "unchecked" ]; then say "ROUTE=FLIP_BOX"; else say "ROUTE=DONE"; fi
    exit 0 ;;
  *:CLOSED) say "ROUTE=STOP_AMBIGUOUS"; say "REASON=PR closed without merge (human veto)"; exit 1 ;;
  *:OPEN)   route=POLL_CI ;;
  *)
    if [ "$has_local" = no ] && [ "$has_remote" = no ]; then
      say "ROUTE=FRESH"; exit 0
    fi
    route=CONTINUE_IMPL ;;
esac

# Reconcile local/remote before resuming on the branch.
if [ "$ahead" -gt 0 ] && [ "$behind" -gt 0 ]; then
  say "ROUTE=STOP_AMBIGUOUS"; say "REASON=local and remote diverged (ahead=$ahead behind=$behind)"
  exit 1
fi
if [ "$has_local" = yes ]; then
  git switch --quiet "$ticket_branch" \
    || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot switch to $ticket_branch"; exit 1; }
  if [ "$behind" -gt 0 ]; then
    git merge --ff-only --quiet "origin/$ticket_branch" \
      || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=fast-forward from origin failed"; exit 1; }
    say "RECONCILED=pulled"
  fi
  [ "$ahead" -gt 0 ] && say "PUSH_NEEDED=yes"
else
  git switch --quiet -c "$ticket_branch" --track "origin/$ticket_branch" \
    || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot check out origin/$ticket_branch"; exit 1; }
  say "RECONCILED=checked-out-remote"
fi

say "ROUTE=$route"
exit 0
