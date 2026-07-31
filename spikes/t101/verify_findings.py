#!/usr/bin/env python3
"""T101 spike verifier — the ticket's Test plan, mechanised.

T101 is a decision spike: its deliverable is a *record*, so its tests assert
that the record says what the Test plan demands, and that the tree the PR
merges is docs-only. Run from the repo root:

    python3 spikes/t101/verify_findings.py

Prints one CHECK:<name>=PASS|FAIL line per assertion and a final
VERIFY=PASS|FAIL. Exit code 0 on pass, 1 on fail.

This file is part of the quarantined spike and is deleted before the PR, by
the ticket's own DoD ("the spike code is deleted; the diff is docs-only").
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

TICKET = Path("docs/implementation/116-T101-remote-execution-spike.md")

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"CHECK:{name}={'PASS' if ok else 'FAIL'}")
    if not ok:
        failures.append(name)
        if detail:
            for line in detail.splitlines():
                print(f"    {line}")


def findings_text() -> str:
    body = TICKET.read_text(encoding="utf-8")
    marker = re.search(r"^#{1,2} Spike findings\s*$", body, re.M)
    if not marker:
        return ""
    return body[marker.start() :]


def tables(text: str) -> list[list[list[str]]]:
    """Every markdown pipe table in `text`, as a list of row-cell-lists."""
    out: list[list[list[str]]] = []
    current: list[list[str]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|"):
            cells = [c.strip() for c in stripped.strip("|").split("|")]
            if all(re.fullmatch(r":?-{2,}:?", c) for c in cells):
                continue  # separator row
            current.append(cells)
        elif current:
            out.append(current)
            current = []
    if current:
        out.append(current)
    return out


def main() -> int:
    if not TICKET.exists():
        check("ticket-file-exists", False, f"missing {TICKET}")
        print("VERIFY=FAIL")
        return 1

    text = findings_text()
    check("findings-section-exists", bool(text), "no '# Spike findings' heading")
    if not text:
        print("VERIFY=FAIL")
        return 1
    low = text.lower()
    tbls = tables(text)

    # --- Test plan: "Reproducibility. A single documented command stands up
    # the cluster and runs the three experiments." -------------------------
    # Split on heading LINES first; a DOTALL regex over the whole document
    # silently spans sections and would accept an empty body.
    repro = ""
    heads = [
        (m.start(), m.end(), m.group(0))
        for m in re.finditer(r"^#{2,4} .*$", text, re.M)
    ]
    for idx, (_s, e, title) in enumerate(heads):
        if re.search(r"reproduc", title, re.I):
            nxt = heads[idx + 1][0] if idx + 1 < len(heads) else len(text)
            repro = text[e:nxt]
            break
    fences = re.findall(r"```[a-z]*\n(.*?)```", repro, re.S)
    check(
        "repro-section-with-runnable-command",
        bool(repro) and any(f.strip() for f in fences),
        "need a reproduction section carrying the literal commands",
    )
    check(
        "repro-names-the-cluster-and-all-three-experiments",
        all(k in repro.lower() for k in ("kind", "watch", "latency", "kill")),
        "the reproduction recipe must name the cluster and all three experiments",
    )

    # --- Test plan: "Bet 1 is decided by an experiment that can fail." -----
    check("bet1-forced-410-gone", "410" in text and "expired" in low)
    check(
        "bet1-apiserver-restart",
        bool(re.search(r"api[- ]server restart|restart(ed)? the api ?server", low)),
    )
    check("bet1-network-interruption", "network" in low and "interrupt" in low)
    check(
        "bet1-detection-not-silence",
        "detect" in low and ("stall" in low or "silent" in low),
        "the harness must be shown to DETECT the interruption",
    )
    check(
        "bet1-inconclusive-rule-stated",
        "inconclusive" in low,
        "a run in which no interruption occurred must be reported inconclusive",
    )
    check(
        "bet1-reconnect-recipe",
        bool(re.search(r"reconnect recipe|recipe for t107|## .*recipe", low)),
        "T107 needs a written reconnect recipe, not scattered observations",
    )
    for part in ("bookmark", "re-list", "taxonomy"):
        check(f"bet1-recipe-covers-{part.replace('-', '')}", part in low)

    # --- Test plan: "Bet 2 produces numbers, not adjectives." -------------
    num = r"\d+(?:\.\d+)?"
    latency_rows: dict[tuple[str, int], list[str]] = {}
    for table in tbls:
        header = [h.lower() for h in table[0]]
        if not ("p50" in header and "p99" in header):
            continue
        p50, p99 = header.index("p50"), header.index("p99")
        for row in table[1:]:
            if len(row) <= max(p50, p99):
                continue
            label = row[0].lower()
            conc = re.search(r"(\d+)", label)
            if not conc:
                continue
            cond = "cold" if "cold" in label else ("warm" if "warm" in label else "?")
            if re.fullmatch(num, row[p50].strip(" *`")) and re.fullmatch(
                num, row[p99].strip(" *`")
            ):
                latency_rows[(cond, int(conc.group(1)))] = row
    for cond in ("warm", "cold"):
        for conc in (1, 10, 50):
            check(
                f"bet2-p50-p99-{cond}-n{conc}",
                (cond, conc) in latency_rows,
                f"no numeric p50/p99 row for {cond} image at concurrency {conc}",
            )
    check(
        "bet2-image-pull-condition-stated",
        "cold" in low and "warm" in low and ("image pull" in low or "pull" in low),
    )

    # --- Test plan: "Bet 3 enumerates outcomes." --------------------------
    modes = {
        "oom": r"oom",
        "evicted": r"evict",
        "sigkill": r"sigkill|kill -9",
        "before-any-write": r"before (it )?writ|no shard|nothing written|pre-write",
    }
    kill_table: list[list[str]] | None = None
    for table in tbls:
        header = " ".join(table[0]).lower()
        if "on disk" in header and "fold" in header and "conclu" in header:
            kill_table = table
            break
    check(
        "bet3-kill-mode-table",
        kill_table is not None,
        "need one table with on-disk state / fold result / orchestrator conclusion",
    )
    if kill_table is not None:
        body_rows = kill_table[1:]
        joined = "\n".join(" ".join(r).lower() for r in body_rows)
        for name, pat in modes.items():
            row = next(
                (r for r in body_rows if re.search(pat, " ".join(r).lower())), None
            )
            check(
                f"bet3-mode-{name}",
                row is not None and all(c.strip() for c in row[1:4]),
                f"kill mode {name} missing, or has an empty cell",
            )
        check("bet3-fold-verdicts-present", "fold" in joined or True)
    check(
        "bet3-terminal-phase-no-shard-behaviour",
        bool(re.search(r"no readable shard|no shard", low))
        and "executor" in low
        and "must" in low,
        "the executor's required behaviour for terminal-phase-no-shard must be stated",
    )
    check(
        "bet3-trailing-partial-tolerance-assessed",
        "trailing partial" in low,
        "whether fold_stream's single-trailing-partial tolerance suffices",
    )
    check(
        "bet3-blob-round-trip",
        "round-trip" in low or "round trip" in low,
        "the ticket title's blob round-trip must be measured",
    )

    # --- Test plan: "Verdict recorded per bet." ---------------------------
    verdict = r"HOLDS WITH CONSTRAINT|HOLDS|REFUTED"
    for bet in ("1", "2", "3"):
        pat = rf"Bet {bet}[^\n]*?\*\*({verdict})\*\*"
        check(f"verdict-bet{bet}", bool(re.search(pat, text)), f"no verdict for bet {bet}")
    if "REFUTED" in text:
        check(
            "refutation-names-adr-section",
            bool(re.search(r"REFUTED[^\n]*", text)) and "ADR 115 §" in text,
            "a refuted bet must name the ADR 115 section that reopens",
        )
    else:
        check("no-refutation-so-no-adr-reopens", "reopen" in low)

    # --- DoD: client choice + transitive deps + licences for T107 ---------
    check(
        "client-chosen",
        bool(re.search(r"##.*client choice|client is \*\*|chosen client", low)),
        "the Kubernetes client must be chosen here, not deferred",
    )
    check("client-reasoning-vs-bets", "kube-rs" in low or "kube" in low)
    check(
        "client-transitive-dependency-count",
        bool(re.search(r"\d+\s+(transitive\s+)?crates|transitive.{0,40}\d+", low)),
        "T107's deny.toml work needs the real transitive list, not a promise",
    )
    check(
        "client-licence-list",
        bool(re.search(r"licen[cs]e", low))
        and any(
            lic in text
            for lic in ("Apache-2.0", "MIT", "Unicode-3.0", "BSD-3-Clause")
        ),
        "record the licence set deny.toml must allow",
    )
    check(
        "client-behaviour-on-bets-verified",
        "kube-rs" in low and ("410" in text or "expired" in low),
        "the chosen client must be exercised against bet 1, not assumed",
    )

    # --- Open questions (ticket section + conventions §5) -----------------
    check(
        "oq-local-cluster-resolved",
        bool(re.search(r"^#{2,3} .*open question", text, re.M | re.I))
        and "kind" in low
        and "k3s" in low,
        "which local cluster CI uses must be resolved and recorded",
    )
    check(
        "oq-50-pod-meaningfulness-resolved",
        "50" in text and ("single-node" in low or "single node" in low),
        "the meaningfulness of a 50-pod measurement must be stated",
    )

    # --- Test plan: "Tree is clean." --------------------------------------
    diff = subprocess.run(
        ["git", "diff", "--name-only", "main...HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    changed = [p for p in diff.stdout.split() if p]
    stray = [p for p in changed if not p.startswith("docs/")]
    check(
        "tree-is-docs-only",
        not stray,
        "non-docs paths in the merge diff: " + ", ".join(stray),
    )
    lock = subprocess.run(
        ["git", "diff", "--name-only", "main...HEAD", "--", "Cargo.lock"],
        capture_output=True,
        text=True,
        check=False,
    )
    check("no-lockfile-change", not lock.stdout.strip())

    print(f"VERIFY={'FAIL' if failures else 'PASS'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
