#!/usr/bin/env python3
"""Select the next eligible dagr ticket from docs/implementation/README.md.

The README is the sole done-tracker: one checkbox line per ticket. This script
is the single source of truth for ticket selection, era assignment, and
ticket-header extraction, so the orchestrator never parses markdown itself.

Usage:
  next_ticket.py            select next eligible ticket, print JSON to stdout
  next_ticket.py --status   print progress counts and the would-be selection
  next_ticket.py --ticket NNN   print JSON for a specific ticket (any state)

Exit codes:
  0  ticket selected / status printed
  2  ALL_DONE      every box is checked
  3  NO_ELIGIBLE   lowest unchecked ticket has an unchecked dependency
                   (impossible unless the README was hand-edited out of
                   topological order — named chain printed for diagnosis)
  4  PARSE_ERROR   a box line or ticket header failed to parse
"""

import json
import re
import subprocess
import sys
from pathlib import Path

# Box line, e.g.:
# - [ ] **024** · [T12 — Compile-failure suite for wiring](024-...md) · S · feature (tests) — after T8, T11, T0.9
# Separators are UTF-8 middle dot (U+00B7) and em dash (U+2014); a shell/sed
# parser mangles these multibyte characters, hence Python.
BOX_RE = re.compile(
    r"^- \[(?P<checked>[ x])\] \*\*(?P<nnn>\d{3})\*\* · "
    r"\[(?P<tid>T[^\s]+?) — (?P<title>.+?)\]\((?P<file>[^)]+)\)"
    r" · (?P<size>[SML]) · (?P<type>[a-z]+(?: \([a-z]+\))?)"
    r"(?: — after (?P<after>.+))?$"
)

# Ticket header line 4, e.g.:
# > **Branch:** `chore/t0.0a-repo-init-and-hygiene` · **Depends on:** — · **Blocks:** T1, T7
BRANCH_RE = re.compile(
    r"^> \*\*Branch:\*\* `(?P<branch>[^`]+)` · "
    r"\*\*Depends on:\*\* (?P<deps>[^·]+?) · \*\*Blocks:\*\* (?P<blocks>.+)$"
)


def repo_root() -> Path:
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    )
    return Path(out.stdout.strip())


def era_for(nnn: int) -> str:
    # Static NNN boundaries — sound because ticket numbering is a topological
    # order and the loop ships tickets serially: 001-002 predate Cargo.toml
    # (ticket 001 forbids creating crates), 003-005 predate CI, 006 (T7)
    # authors the GitHub Actions workflow. run_gate.sh asserts the observed
    # repo state matches the declared era at gate time.
    if nnn <= 2:
        return "pre-workspace"
    if nnn <= 5:
        return "pre-ci"
    return "ci"


def parse_readme(readme: Path):
    tickets = {}
    errors = []
    for lineno, line in enumerate(readme.read_text().splitlines(), 1):
        if not line.startswith("- ["):
            continue
        m = BOX_RE.match(line)
        if not m:
            errors.append(f"line {lineno}: unparseable box line: {line}")
            continue
        nnn = int(m.group("nnn"))
        tickets[nnn] = {
            "nnn": f"{nnn:03d}",
            "tid": m.group("tid"),
            "title": m.group("title"),
            "file": m.group("file"),
            "size": m.group("size"),
            "type": m.group("type"),
            "checked": m.group("checked") == "x",
            "after": [t.strip() for t in m.group("after").split(",")] if m.group("after") else [],
        }
    return tickets, errors


def read_header(root: Path, ticket: dict) -> dict:
    path = root / "docs" / "implementation" / ticket["file"]
    if not path.exists():
        return {"header_error": f"ticket file missing: {path}"}
    lines = path.read_text().splitlines()
    header = {"path": str(path.relative_to(root))}
    for line in lines[:6]:
        m = BRANCH_RE.match(line)
        if m:
            header["branch"] = m.group("branch")
            deps = m.group("deps").strip()
            header["depends_on"] = [] if deps == "—" else [t.strip() for t in deps.split(",")]
            blocks = m.group("blocks").strip()
            header["blocks"] = [] if blocks == "—" else [t.strip() for t in blocks.split(",")]
        cm = re.search(r"\*\*Components:\*\* (.+)$", line)
        if cm:
            header["components"] = cm.group(1).strip()
        mm = re.search(r"\*\*Milestone:\*\* (\S+)", line)
        if mm:
            header["milestone"] = mm.group(1)
    if "branch" not in header:
        header["header_error"] = f"no Branch line found in first 6 lines of {path.name}"
    return header


def ticket_json(root: Path, tickets: dict, nnn: int) -> dict:
    t = dict(tickets[nnn])
    t.update(read_header(root, t))
    t["era"] = era_for(nnn)
    t["pr_title"] = f"{t['nnn']} · {t['tid']} — {t['title']}"
    return t


def main() -> int:
    root = repo_root()
    readme = root / "docs" / "implementation" / "README.md"
    if not readme.exists():
        print(f"VERDICT=PARSE_ERROR\nERROR=README not found at {readme}")
        return 4

    tickets, errors = parse_readme(readme)
    if errors:
        print("VERDICT=PARSE_ERROR")
        for e in errors:
            print(f"ERROR={e}")
        return 4
    if not tickets:
        print("VERDICT=PARSE_ERROR\nERROR=no box lines found")
        return 4

    tid_to_nnn = {t["tid"]: n for n, t in tickets.items()}

    if len(sys.argv) >= 3 and sys.argv[1] == "--ticket":
        nnn = int(sys.argv[2])
        if nnn not in tickets:
            print(f"VERDICT=PARSE_ERROR\nERROR=no ticket {nnn:03d} in README")
            return 4
        t = ticket_json(root, tickets, nnn)
        print(f"VERDICT=TICKET\nBOX={'checked' if t['checked'] else 'unchecked'}")
        print(json.dumps(t, indent=2, ensure_ascii=False))
        return 0

    done = sorted(n for n, t in tickets.items() if t["checked"])
    todo = sorted(n for n, t in tickets.items() if not t["checked"])

    if sys.argv[1:2] == ["--status"]:
        print(f"VERDICT=STATUS\nTOTAL={len(tickets)}\nDONE={len(done)}\nREMAINING={len(todo)}")
        if todo:
            print(f"NEXT={todo[0]:03d}")
        return 0

    if not todo:
        print("VERDICT=ALL_DONE")
        return 2

    nnn = todo[0]
    unmet = []
    for dep_tid in tickets[nnn]["after"]:
        dep_nnn = tid_to_nnn.get(dep_tid)
        if dep_nnn is None:
            unmet.append(f"{dep_tid} (unknown tid)")
        elif not tickets[dep_nnn]["checked"]:
            unmet.append(f"{dep_tid} = ticket {dep_nnn:03d} (unchecked)")
    if unmet:
        print(f"VERDICT=NO_ELIGIBLE\nTICKET={nnn:03d}")
        for u in unmet:
            print(f"UNMET_DEP={u}")
        return 3

    t = ticket_json(root, tickets, nnn)
    if "header_error" in t:
        print(f"VERDICT=PARSE_ERROR\nERROR={t['header_error']}")
        return 4
    print("VERDICT=SELECTED")
    print(json.dumps(t, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
