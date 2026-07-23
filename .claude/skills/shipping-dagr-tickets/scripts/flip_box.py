#!/usr/bin/env python3
"""Flip a ticket's checkbox in docs/implementation/README.md after its PR merges.

The box flip is the only edit the orchestrator makes to the README, and the
exact-text edit is fragile enough to script: this is deterministic, touches
exactly one line, and is idempotent so an interrupted post-merge flip can be
safely re-run on resume.

Usage:
  flip_box.py NNN [--path <readme>]   flip '- [ ] **NNN**' to '- [x] **NNN**'

Exit codes:
  0  FLIPPED or ALREADY_CHECKED (idempotent success either way)
  1  no line matched — the NNN and any near-miss lines are printed
"""

import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        print("usage: flip_box.py NNN [--path <readme>]")
        return 1
    nnn = f"{int(sys.argv[1]):03d}"

    if "--path" in sys.argv:
        readme = Path(sys.argv[sys.argv.index("--path") + 1])
    else:
        root = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        readme = Path(root) / "docs" / "implementation" / "README.md"

    unchecked = f"- [ ] **{nnn}**"
    checked = f"- [x] **{nnn}**"

    lines = readme.read_text().splitlines(keepends=True)
    hits = [i for i, l in enumerate(lines) if l.startswith(unchecked)]
    already = [i for i, l in enumerate(lines) if l.startswith(checked)]

    if not hits and len(already) == 1:
        print(f"VERDICT=ALREADY_CHECKED\nLINE={lines[already[0]].rstrip()}")
        return 0
    if len(hits) != 1:
        print(f"VERDICT=ERROR\nERROR=expected exactly one '{unchecked}' line, found {len(hits)}")
        for i, l in enumerate(lines):
            if f"**{nnn}**" in l:
                print(f"NEAR_MISS=line {i + 1}: {l.rstrip()}")
        return 1

    i = hits[0]
    lines[i] = lines[i].replace(unchecked, checked, 1)
    readme.write_text("".join(lines))
    print(f"VERDICT=FLIPPED\nLINE={lines[i].rstrip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
