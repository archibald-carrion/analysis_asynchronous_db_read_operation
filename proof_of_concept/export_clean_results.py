#!/usr/bin/env python3
"""
Generate a reader-friendly CSV summary of randomized TPC-H runs.

The script keeps only the essential columns for analysis:
- Run number and replicate metadata
- Database size in GB plus a human label (e.g., 10MB, 1GB)
- I/O configuration used
- Runtime, timestamp, and response variable (QphH)
- Optional power/throughput metrics (recomputed from raw CSVs when available)

Usage:
    python export_clean_results.py \
        --schedule experimental_design_schedule.csv \
        --results-dir randomized_results/raw_data \
        --output randomized_results/clean_tpch_results.csv
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_SCHEDULE = Path(__file__).with_name("experimental_design_schedule.csv")
DEFAULT_RESULTS_DIR = Path(__file__).with_name("randomized_results").joinpath("raw_data")
DEFAULT_OUTPUT = Path(__file__).with_name("randomized_results").joinpath("clean_tpch_results.csv")


@dataclass
class ResultFiles:
    complete: Optional[Path]
    refresh: Optional[Path]
    interval: Optional[Path]

    def all_present(self) -> bool:
        return all(path is not None and path.exists() for path in (self.complete, self.refresh, self.interval))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce a clean CSV summary of completed randomized runs.")
    parser.add_argument("--schedule", type=Path, default=DEFAULT_SCHEDULE, help="Path to experimental_design_schedule.csv")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR, help="Directory with raw per-run CSV files")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination CSV path")
    parser.add_argument("--include-non-completed", action="store_true", help="Include runs that are not COMPLETED")
    return parser.parse_args()


def to_float(value: str) -> Optional[float]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_number(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return ""
    return f"{value:.{decimals}f}"


def format_db_label(db_size_gb: float) -> str:
    if db_size_gb >= 1:
        return f"{db_size_gb:g}GB"
    return f"{db_size_gb * 1000:g}MB"


def locate_result_files(results_dir: Path, run_order: int, io_method: str, replicate: int) -> ResultFiles:
    """
    Attempt to locate the *_complete.csv, *_refresh.csv and *_interval.csv files
    for a given run. Uses glob patterns so it works even if db_size formatting varies.
    """
    if not results_dir.exists():
        return ResultFiles(None, None, None)

    base_pattern = f"run{run_order}_*_{io_method}_rep{replicate}"
    complete_matches = sorted(results_dir.glob(f"{base_pattern}_complete.csv"))
    refresh_matches = sorted(results_dir.glob(f"{base_pattern}_refresh.csv"))
    interval_matches = sorted(results_dir.glob(f"{base_pattern}_interval.csv"))

    return ResultFiles(
        complete=complete_matches[0] if complete_matches else None,
        refresh=refresh_matches[0] if refresh_matches else None,
        interval=interval_matches[0] if interval_matches else None,
    )


def collect_times(path: Path, predicate) -> List[float]:
    values: List[float] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not predicate(row):
                continue
            exec_time = to_float(row.get("execution_time_seconds", ""))
            if exec_time and exec_time > 0:
                values.append(exec_time)
    return values


def calculate_power_metric(complete_csv: Path, refresh_csv: Path, scale_factor: float) -> Optional[float]:
    power_times = collect_times(
        complete_csv,
        lambda r: r.get("test_type", "").upper() == "POWER" and r.get("stream_id") == "0",
    )
    refresh_times = collect_times(
        refresh_csv,
        lambda r: r.get("test_type", "").upper() == "POWER" and r.get("stream_id") == "0",
    )

    if len(power_times) != 22 or len(refresh_times) != 2:
        return None

    log_sum = sum(math.log(t) for t in power_times + refresh_times)
    geom_mean = math.exp(log_sum / 24.0)
    if geom_mean <= 0:
        return None
    return (3600.0 * scale_factor) / geom_mean


def calculate_throughput_metric(interval_csv: Path, scale_factor: float) -> Optional[float]:
    with interval_csv.open(newline="") as handle:
        reader = csv.DictReader(handle)
        row = next((r for r in reader if r.get("test_type", "").upper() == "THROUGHPUT"), None)

    if not row:
        return None

    measurement = to_float(row.get("measurement_interval_seconds", ""))
    stream_count = to_float(row.get("stream_count", ""))
    if not measurement or measurement <= 0 or not stream_count or stream_count <= 0:
        return None

    return ((stream_count * 22.0 * 3600.0) / measurement) * scale_factor


def calculate_metrics(files: ResultFiles, scale_factor: float) -> Tuple[Optional[float], Optional[float]]:
    if not files.all_present():
        return (None, None)

    power = calculate_power_metric(files.complete, files.refresh, scale_factor)
    throughput = calculate_throughput_metric(files.interval, scale_factor)
    return power, throughput


def summarize_runs(
    schedule_path: Path,
    results_dir: Path,
    include_non_completed: bool,
) -> List[Dict[str, str]]:
    cleaned_rows: List[Dict[str, str]] = []

    with schedule_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            status = row.get("status", "").strip().upper()
            if not include_non_completed and status != "COMPLETED":
                continue

            run_number = int(float(row["run_order"]))
            db_size = to_float(row.get("db_size_gb", "0")) or 0.0
            replicate = int(float(row.get("replicate", "0")))
            io_method = row.get("io_method", "")
            qphh = to_float(row.get("qphh_result", ""))

            files = locate_result_files(results_dir, run_number, io_method, replicate)
            power = to_float(row.get("power_result", ""))
            throughput = to_float(row.get("throughput_result", ""))

            # If schedule lacks power/throughput, attempt to compute from raw CSVs.
            if power is None or throughput is None:
                calc_power, calc_throughput = calculate_metrics(files, db_size or 1.0)
                power = power if power is not None else calc_power
                throughput = throughput if throughput is not None else calc_throughput

            cleaned_rows.append(
                {
                    "run_number": run_number,
                    "io_method": io_method,
                    "replicate": replicate,
                    "database_size_gb": format_number(db_size, 3),
                    "database_size_label": format_db_label(db_size),
                    "database_name": row.get("db_name", ""),
                    "status": status,
                    "runtime_seconds": row.get("actual_runtime_sec", ""),
                    "executed_at": row.get("execution_timestamp", ""),
                    "qphh_metric": format_number(qphh),
                    "power_metric": format_number(power),
                    "throughput_metric": format_number(throughput),
                    "notes": row.get("notes", "").strip(),
                }
            )

    return cleaned_rows


def write_clean_csv(output_path: Path, rows: Iterable[Dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    headers = [
        "run_number",
        "io_method",
        "replicate",
        "database_size_gb",
        "database_size_label",
        "database_name",
        "status",
        "runtime_seconds",
        "executed_at",
        "qphh_metric",
        "power_metric",
        "throughput_metric",
        "notes",
    ]

    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    cleaned = summarize_runs(args.schedule, args.results_dir, args.include_non_completed)
    if not cleaned:
        print("No runs matched the selected criteria; nothing to export.")
        return

    write_clean_csv(args.output, cleaned)
    print(f"Wrote {len(cleaned)} rows to {args.output}")


if __name__ == "__main__":
    main()
