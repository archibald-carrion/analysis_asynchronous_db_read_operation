#!/usr/bin/env python3
"""Repair misaligned randomized schedule rows.

Older runs produced rows without the `cumulative_time_hours` column. When those
rows are loaded alongside the new header, every field from `status` onward ends
up shifted one position to the left. This utility realigns the affected rows,
recomputes the missing hours column, and writes the corrected CSV back to disk.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Tuple

STATUS_VALUES = {"COMPLETED", "FAILED", "PENDING"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fix misaligned experimental schedule rows.")
    parser.add_argument(
        "schedule",
        nargs="?",
        default="experimental_design_schedule.csv",
        type=Path,
        help="Path to the schedule CSV (default: experimental_design_schedule.csv).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze and report fixes without modifying the CSV.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not create a .bak copy before rewriting the schedule.",
    )
    return parser.parse_args()


def parse_minutes(value: str | None) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def realign_row(row: Dict[str, str]) -> bool:
    """Fix rows where `status` was shifted into `cumulative_time_hours`."""

    status_value = (row.get("status") or "").strip().upper()
    if status_value in STATUS_VALUES:
        return False

    candidate_status = (row.get("cumulative_time_hours") or "").strip()
    if candidate_status.upper() not in STATUS_VALUES:
        return False

    actual_status = candidate_status
    actual_runtime = row.get("status", "")
    actual_timestamp = row.get("actual_runtime_sec", "")
    actual_qphh = row.get("execution_timestamp", "")
    actual_power = row.get("qphh_result", "")
    actual_throughput = row.get("power_result", "")
    trailing_throughput = row.get("throughput_result", "")

    row["cumulative_time_hours"] = ""
    row["status"] = actual_status
    row["actual_runtime_sec"] = actual_runtime
    row["execution_timestamp"] = actual_timestamp
    row["qphh_result"] = actual_qphh
    row["power_result"] = actual_power
    row["throughput_result"] = actual_throughput or trailing_throughput

    return True


def fill_hours(row: Dict[str, str]) -> bool:
    if row.get("cumulative_time_hours"):
        return False

    minutes = parse_minutes(row.get("cumulative_time_min"))
    if minutes is None:
        return False

    row["cumulative_time_hours"] = f"{minutes / 60.0:.2f}"
    return True


def repair_rows(rows: List[Dict[str, str]]) -> Tuple[int, int]:
    realigned = 0
    hours_backfilled = 0

    for row in rows:
        if realign_row(row):
            realigned += 1
        if fill_hours(row):
            hours_backfilled += 1

    return realigned, hours_backfilled


def write_rows(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    schedule_path = args.schedule.expanduser().resolve()

    if not schedule_path.exists():
        print(f"Schedule not found: {schedule_path}", file=sys.stderr)
        return 1

    with schedule_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames
        if not fieldnames:
            print("Schedule is missing a header row.", file=sys.stderr)
            return 1
        rows = list(reader)

    realigned, hours_backfilled = repair_rows(rows)

    if args.dry_run:
        print(
            f"[dry-run] Rows to realign: {realigned}, rows needing hours backfill: {hours_backfilled}"
        )
        return 0

    if not args.no_backup:
        backup_path = schedule_path.with_suffix(schedule_path.suffix + ".bak")
        shutil.copy2(schedule_path, backup_path)

    write_rows(schedule_path, fieldnames, rows)
    print(
        f"Repaired schedule in {schedule_path.name}: "
        f"{realigned} rows realigned, {hours_backfilled} hours fields backfilled."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
