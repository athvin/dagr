#!/usr/bin/env python3
"""Classify a PR's statusCheckRollup JSON (stdin) into a single CI verdict.

Called by ci_status.sh with the output of
`gh pr view --json statusCheckRollup,headRefOid,headRefName,state,mergeable`.

Every status/conclusion value is classified exhaustively; an unrecognized
value yields ANOMALY so the loop stops instead of silently merging.

Exit codes: 0 PASS · 1 FAIL · 2 PENDING · 3 NO_CHECKS · 4 ANOMALY
"""

import json
import sys

# CheckRun conclusions and StatusContext states.
PASS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
FAIL = {"FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED",
        "STARTUP_FAILURE", "ERROR", "STALE"}
PENDING_STATES = {"QUEUED", "IN_PROGRESS", "WAITING", "PENDING",
                  "REQUESTED", "EXPECTED"}


def main() -> int:
    d = json.load(sys.stdin)
    print(f"PR_STATE={d.get('state')}")
    print(f"HEAD_SHA={d.get('headRefOid')}")
    print(f"HEAD_REF={d.get('headRefName')}")
    print(f"MERGEABLE={d.get('mergeable')}")

    if d.get("state") != "OPEN":
        print("VERDICT=ANOMALY")
        return 4

    rollup = d.get("statusCheckRollup") or []
    print(f"CHECKS={len(rollup)}")
    if not rollup:
        print("VERDICT=NO_CHECKS")
        return 3

    failing, pending, unknown = [], [], []
    for c in rollup:
        name = c.get("name") or c.get("context") or "?"
        if c.get("__typename") == "StatusContext" or "state" in c:
            val = c.get("state", "")
            if val in PASS:
                continue
            if val in FAIL:
                failing.append(name)
            elif val in PENDING_STATES:
                pending.append(name)
            else:
                unknown.append(f"{name}:{val}")
        else:
            status, concl = c.get("status", ""), c.get("conclusion", "")
            if status != "COMPLETED":
                if status in PENDING_STATES:
                    pending.append(name)
                else:
                    unknown.append(f"{name}:{status}")
            elif concl in PASS:
                continue
            elif concl in FAIL:
                failing.append(name)
            else:
                unknown.append(f"{name}:{concl}")

    if unknown:
        for u in unknown:
            print(f"UNKNOWN={u}")
        print("VERDICT=ANOMALY")
        return 4
    if failing:
        for f in failing:
            print(f"FAILING={f}")
        print("VERDICT=FAIL")
        return 1
    if pending:
        for p in pending:
            print(f"PENDING_CHECK={p}")
        print("VERDICT=PENDING")
        return 2
    print("VERDICT=PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
