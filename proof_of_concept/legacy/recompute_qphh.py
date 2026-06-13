#!/usr/bin/env python3
"""Recompute QphH metrics from existing run artifacts.

This script inspects the randomized experimental schedule, locates the
corresponding result CSVs produced by `run_tests.sh`, recomputes the QphH
metric (and its power/throughput components), and writes the updated values back
to the schedule.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


STATUS_VALUES = {"COMPLETED", "FAILED", "PENDING"}


@dataclass
class MetricComputation:
    qphh: float
    power: float
    throughput: float


@dataclass
class RunArtifacts:
    prefix: str
    complete: Path
    refresh: Path
    interval: Path


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute QphH metrics using existing raw result CSVs."
    )
    parser.add_argument(
        "--schedule",
        default="experimental_design_schedule.csv",
        type=Path,
        help="Path to the randomized schedule CSV (default: experimental_design_schedule.csv).",
    )
    parser.add_argument(
        "--results-dir",
        default=Path("randomized_results") / "raw_data",
        type=Path,
        help="Directory containing per-run result CSVs (default: randomized_results/raw_data).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and report metrics without modifying the schedule file.",
    )
    return parser.parse_args()


def parse_positive_float(value: str) -> Optional[float]:
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return None
    return candidate if candidate > 0 else None


def collect_times(rows: Iterable[Dict[str, str]], expected: int) -> Optional[List[float]]:
    times: List[float] = []
    for row in rows:
        parsed = parse_positive_float(row.get("execution_time_seconds", ""))
        if parsed is not None:
            times.append(parsed)
    if len(times) != expected:
        return None
    return times


def normalize_row(row: Dict[str, str]) -> None:
    """Fix misaligned columns caused by missing cumulative_time_hours values."""

    raw_status = (row.get("status") or "").strip().upper()
    if raw_status in STATUS_VALUES:
        return

    candidate_status = (row.get("cumulative_time_hours") or "").strip()
    if candidate_status.upper() not in STATUS_VALUES:
        return

    actual_runtime = row.get("status", "")
    execution_timestamp = row.get("actual_runtime_sec", "")

    row["cumulative_time_hours"] = ""
    row["status"] = candidate_status
    row["actual_runtime_sec"] = actual_runtime
    row["execution_timestamp"] = execution_timestamp


def compute_metrics(artifacts: RunArtifacts, scale_factor: float) -> Tuple[Optional[MetricComputation], str]:
    if not artifacts.complete.exists():
        return None, f"missing complete CSV: {artifacts.complete.name}"
    if not artifacts.refresh.exists():
        return None, f"missing refresh CSV: {artifacts.refresh.name}"
    if not artifacts.interval.exists():
        return None, f"missing interval CSV: {artifacts.interval.name}"

    with artifacts.complete.open(newline="") as handle:
        complete_rows = list(csv.DictReader(handle))

    power_rows = [
        row
        for row in complete_rows
        if row.get("test_type", "").strip().upper() == "POWER"
        and row.get("stream_id", "").strip() == "0"
    ]

    power_times = collect_times(power_rows, expected=22)
    if power_times is None:
        return None, "expected 22 POWER stream=0 query timings"

    with artifacts.refresh.open(newline="") as handle:
        refresh_rows = list(csv.DictReader(handle))

    refresh_power_rows = [
        row
        for row in refresh_rows
        if row.get("test_type", "").strip().upper() == "POWER"
        and row.get("stream_id", "").strip().upper() == "0"
    ]

    refresh_times = collect_times(refresh_power_rows, expected=2)
    if refresh_times is None:
        return None, "expected 2 POWER refresh timings"

    with artifacts.interval.open(newline="") as handle:
        interval_reader = csv.DictReader(handle)
        interval_row = next(
            (
                row
                for row in interval_reader
                if row.get("test_type", "").strip().upper() == "THROUGHPUT"
            ),
            None,
        )

    if not interval_row:
        return None, "missing THROUGHPUT interval entry"

    measurement = parse_positive_float(interval_row.get("measurement_interval_seconds", ""))
    if measurement is None:
        return None, "invalid measurement interval"

    try:
        stream_count = int(float(interval_row.get("stream_count", "0")))
    except (TypeError, ValueError):
        return None, "invalid stream count"
    if stream_count <= 0:
        return None, "stream count must be positive"

    # Geometric mean of the 24 POWER timings.
    log_sum = sum(math.log(t) for t in power_times + refresh_times)
    geom_mean = math.exp(log_sum / 24.0)

    power_metric = (3600.0 * scale_factor) / geom_mean
    throughput_metric = (stream_count * 22 * 3600.0) / measurement

    if power_metric <= 0 or throughput_metric <= 0:
        return None, "non-positive power or throughput metric"

    qphh = math.sqrt(power_metric * throughput_metric)
    return MetricComputation(qphh=qphh, power=power_metric, throughput=throughput_metric), ""


def build_artifacts(row: Dict[str, str], results_dir: Path) -> RunArtifacts:
    run_order = row.get("run_order", "").strip()
    db_size = row.get("db_size_gb", "").strip()
    io_method = row.get("io_method", "").strip()
    replicate = row.get("replicate", "").strip()

    prefix = f"run{run_order}_{db_size}gb_{io_method}_rep{replicate}"
    return RunArtifacts(
        prefix=prefix,
        complete=results_dir / f"{prefix}_complete.csv",
        refresh=results_dir / f"{prefix}_refresh.csv",
        interval=results_dir / f"{prefix}_interval.csv",
    )


def format_metric(value: float) -> str:
    return f"{value:.2f}"


def recompute(schedule_path: Path, results_dir: Path, dry_run: bool) -> int:
    if not schedule_path.exists():
        print(f"Schedule not found: {schedule_path}", file=sys.stderr)
        return 1
    if not results_dir.exists():
        print(f"Results directory not found: {results_dir}", file=sys.stderr)
        return 1

    with schedule_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if not rows:
        print("No rows found in schedule; nothing to do.")
        return 0

    successes = 0
    failures: List[str] = []

    for row in rows:
        normalize_row(row)
        artifacts = build_artifacts(row, results_dir)
        try:
            scale_factor = float(row.get("db_size_gb", "1"))
        except (TypeError, ValueError):
            failures.append(f"run {row.get('run_order')} → invalid scale factor")
            continue

        metrics, reason = compute_metrics(artifacts, scale_factor)
        if metrics is None:
            failures.append(f"run {row.get('run_order')} ({artifacts.prefix}) → {reason}")
            continue

        successes += 1
        row["qphh_result"] = format_metric(metrics.qphh)
        row["power_result"] = format_metric(metrics.power)
        row["throughput_result"] = format_metric(metrics.throughput)

    if dry_run:
        print(f"[dry-run] Would update {successes} runs with recalculated metrics.")
    else:
        temp_path = schedule_path.with_suffix(schedule_path.suffix + ".tmp")
        with temp_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        temp_path.replace(schedule_path)
        print(f"Updated schedule with recalculated metrics for {successes} runs.")

    if failures:
        print("Some runs could not be processed:")
        for entry in failures:
            print(f"  - {entry}")
        return 1 if successes == 0 else 2

    return 0


def main() -> int:
    args = parse_arguments()
    return recompute(args.schedule.resolve(), args.results_dir.resolve(), args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
