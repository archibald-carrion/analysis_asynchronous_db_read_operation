#!/usr/bin/env python3
"""Repair complete.csv rows split by the old row_count=$'0\\n0' bug.

run_tests.sh used to compute row_count with `grep -c . || echo 0`. When a query
returned zero rows, grep printed "0" AND exited non-zero, so `|| echo 0` appended
a SECOND "0" on its own line. The result was a row_count field of $'0\\n0' that
split one logical CSV record across two physical lines:

    iouring,...,<exec_time>,0          <- real row ends with the true row_count (0)
    0,2026-06-26 18:23:50              <- orphan: spurious "0" + the real timestamp

This rejoins each such pair into a single, correct line:

    iouring,...,<exec_time>,0,2026-06-26 18:23:50

The orphan's leading "0," (the spurious echo) is dropped; the previous line
already carries the true row_count, so we only append the timestamp tail.

Idempotent and safe: only lines matching the orphan signature are touched, and a
.bak copy is written before any change. Run this BEFORE recompute_qphh.py.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# An orphaned tail line: "<digits>,<YYYY-MM-DD HH:MM:SS>" and nothing else.
# A legitimate data row always starts with the io_method name (a letter) and the
# header starts with "io_method", so a line beginning with a bare number can only
# be the spurious "0,<timestamp>" tail produced by the row_count=$'0\n0' bug.
ORPHAN_RE = re.compile(r"^(\d+),(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*$")


def repair_text(lines: list[str]) -> tuple[list[str], int]:
    out: list[str] = []
    fixed = 0
    for line in lines:
        m = ORPHAN_RE.match(line)
        if m and out:
            # The previous line is the truncated record; it already ends with the
            # real row_count. Append only the timestamp tail; the orphan's leading
            # number is the spurious `echo 0`, so it is discarded.
            timestamp = m.group(2)
            out[-1] = out[-1].rstrip("\n") + "," + timestamp + "\n"
            fixed += 1
        else:
            out.append(line)
    return out, fixed


def repair_file(path: Path, dry_run: bool) -> int:
    if not path.exists():
        print(f"  ! not found: {path}", file=sys.stderr)
        return 0
    with path.open(newline="") as handle:
        lines = handle.readlines()

    repaired, fixed = repair_text(lines)
    if fixed == 0:
        print(f"  = {path.name}: no split rows (already clean)")
        return 0

    if dry_run:
        print(f"  [dry-run] {path.name}: would rejoin {fixed} split row(s)")
        return fixed

    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        backup.write_text("".join(lines))
    path.write_text("".join(repaired))
    print(f"  ✓ {path.name}: rejoined {fixed} split row(s) (backup: {backup.name})")
    return fixed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path,
                        help="complete.csv files to repair")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would change without writing")
    args = parser.parse_args()

    total = 0
    for f in args.files:
        total += repair_file(f, args.dry_run)
    print(f"\nTotal rows rejoined: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
