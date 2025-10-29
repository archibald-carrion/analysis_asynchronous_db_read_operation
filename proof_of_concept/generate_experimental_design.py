#!/usr/bin/env python3
"""
Generate Randomized Experimental Design for TPC-H I/O Method Comparison

This script creates a randomized run schedule for a factorial experimental design:
- Factor 1: I/O Method (configurable list, default: sync, bgworkers, iouring)
- Factor 2: Database Size (configurable list, supports sub-GB scales such as 0.01)
- Replicates: user-defined per treatment combination

Output: CSV file with randomized run schedule plus a textual summary.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import sys
from decimal import Decimal, InvalidOperation


def format_numeric(value: float) -> str:
    """Format numbers without trailing zeros (e.g., 1.0 -> '1', 0.25 -> '0.25')."""
    return f"{value:g}"


def format_db_label(db_size_gb: float) -> str:
    """
    Convert a scale factor (GB) into a human-friendly size label.
    Examples:
        0.01 -> '10MB'
        0.1  -> '100MB'
        1    -> '1GB'
        10   -> '10GB'
    """
    size_decimal = Decimal(str(db_size_gb))
    if size_decimal >= 1:
        integral = size_decimal == size_decimal.to_integral()
        value = int(size_decimal) if integral else float(size_decimal)
        return f"{format_numeric(value)}GB"

    mb_value = size_decimal * Decimal(1000)
    integral = mb_value == mb_value.to_integral()
    value = int(mb_value) if integral else float(mb_value)
    return f"{format_numeric(value)}MB"


def sanitize_db_name(db_label: str) -> str:
    """Create a database name from the label (e.g., '10MB' -> 'tpch_db_10mb')."""
    return f"tpch_db_{db_label.lower()}"


def db_size_type(value: str) -> float:
    """Argparse helper to parse positive numeric database sizes."""
    try:
        decimal_value = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"Invalid database size '{value}'") from exc

    if decimal_value <= 0:
        raise argparse.ArgumentTypeError(f"Database size must be positive (got {value})")

    return float(decimal_value)


def generate_treatment_combinations(io_methods, db_sizes, replicates):
    """
    Generate all treatment combinations with replicates
    
    Args:
        io_methods: List of I/O methods
        db_sizes: List of database sizes (in GB)
        replicates: Number of replicates per treatment combination
    
    Returns:
        DataFrame with all treatment combinations
    """
    treatments = []
    
    for db_size in db_sizes:
        db_label = format_db_label(db_size)
        db_name = sanitize_db_name(db_label)
        for io_method in io_methods:
            for rep in range(1, replicates + 1):
                treatments.append({
                    'db_size_gb': db_size,
                    'db_name': db_name,
                    'io_method': io_method,
                    'replicate': rep,
                    'treatment_id': f"{db_label}_{io_method}"
                })
    
    return pd.DataFrame(treatments)


def generate_crd_schedule(treatments_df, seed=None):
    """
    Generate Completely Randomized Design (CRD) schedule
    
    Randomizes ALL runs across all treatment combinations
    """
    if seed is not None:
        np.random.seed(seed)
    
    # Shuffle all runs randomly
    schedule = treatments_df.copy()
    schedule = schedule.sample(frac=1.0).reset_index(drop=True)
    
    # Add run order
    schedule.insert(0, 'run_order', range(1, len(schedule) + 1))
    schedule['design_type'] = 'CRD'
    schedule['block_id'] = 'NA'
    
    return schedule


def generate_rcbd_schedule(treatments_df, blocking_factor='db_size_gb', seed=None):
    """
    Generate Randomized Complete Block Design (RCBD) schedule
    
    Blocks by specified factor (typically db_size), randomizes treatments within each block
    """
    if seed is not None:
        np.random.seed(seed)
    
    # Group by blocking factor
    blocks = []
    block_ids = list(treatments_df[blocking_factor].unique())
    
    # Randomize block order
    np.random.shuffle(block_ids)
    
    for block_num, block_value in enumerate(block_ids, 1):
        block_data = treatments_df[treatments_df[blocking_factor] == block_value].copy()
        
        # Randomize within block
        block_data = block_data.sample(frac=1.0).reset_index(drop=True)
        block_label = (
            block_value if blocking_factor != 'db_size_gb'
            else format_db_label(block_value)
        )
        block_data['block_id'] = f"Block{block_num}_{block_label}"
        
        blocks.append(block_data)
    
    # Concatenate all blocks (block order already randomized)
    schedule = pd.concat(blocks, ignore_index=True)
    schedule.insert(0, 'run_order', range(1, len(schedule) + 1))
    schedule['design_type'] = 'RCBD'
    
    return schedule


def generate_latin_square(treatments, seed=None):
    """
    Generate Latin Square Design
    
    Balances two blocking factors (e.g., time period and hardware platform)
    """
    if seed is not None:
        np.random.seed(seed)
    
    # For 3x3 Latin Square (3 I/O methods)
    # This is a simplified version - full implementation would be more complex
    n = len(treatments['io_method'].unique())
    
    # Generate a random Latin square
    square = []
    for i in range(n):
        row = [(j + i) % n for j in range(n)]
        np.random.shuffle(row)
        square.append(row)
    
    return pd.DataFrame(square)


def add_execution_metadata(schedule, runtime_per_run=30, cooldown=5):
    """
    Add execution time estimates and other metadata
    
    Args:
        schedule: DataFrame with run schedule
        runtime_per_run: Expected runtime per run in minutes
        cooldown: Cooldown period between runs in minutes
    """
    total_time = runtime_per_run + cooldown
    schedule['estimated_runtime_min'] = runtime_per_run
    schedule['cooldown_min'] = cooldown
    schedule['cumulative_time_min'] = schedule['run_order'] * total_time
    schedule['cumulative_time_hours'] = schedule['cumulative_time_min'] / 60
    
    # Add execution status tracking
    schedule['status'] = 'PENDING'
    schedule['actual_runtime_sec'] = np.nan
    schedule['execution_timestamp'] = ''
    schedule['qphh_result'] = np.nan
    schedule['power_result'] = np.nan
    schedule['throughput_result'] = np.nan
    schedule['notes'] = ''
    
    return schedule


def generate_summary_stats(schedule):
    """Generate summary statistics for the experimental design"""
    summary = {
        'total_runs': len(schedule),
        'io_methods': schedule['io_method'].unique().tolist(),
        'db_sizes': schedule['db_size_gb'].unique().tolist(),
        'replicates_per_treatment': len(schedule) // (
            len(schedule['io_method'].unique()) * len(schedule['db_size_gb'].unique())
        ),
        'design_type': schedule['design_type'].iloc[0],
        'estimated_total_time_hours': schedule['cumulative_time_hours'].max(),
    }
    
    # Treatment balance check
    treatment_counts = schedule.groupby(['db_size_gb', 'io_method']).size()
    summary['balanced'] = len(treatment_counts.unique()) == 1
    
    return summary


def save_schedule(schedule, output_file, summary):
    """Save schedule to CSV and print summary"""
    
    # Save main schedule
    schedule.to_csv(output_file, index=False)
    print(f"✅ Experimental design schedule saved to: {output_file}")
    
    # Save summary to text file
    if output_file.lower().endswith('.csv'):
        summary_file = f"{output_file[:-4]}_summary.txt"
    else:
        summary_file = f"{output_file}_summary.txt"
    with open(summary_file, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("EXPERIMENTAL DESIGN SUMMARY\n")
        f.write("=" * 70 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"Design Type: {summary['design_type']}\n")
        f.write(f"Total Runs: {summary['total_runs']}\n")
        f.write(f"Balanced Design: {'Yes' if summary['balanced'] else 'No'}\n\n")
        
        db_labels = [format_db_label(size) for size in summary['db_sizes']]
        f.write("Factors:\n")
        f.write(f"  - I/O Methods: {', '.join(summary['io_methods'])}\n")
        f.write(f"  - Database Sizes: {', '.join(db_labels)}\n")
        f.write(f"  - Replicates per treatment: {summary['replicates_per_treatment']}\n\n")
        
        f.write(f"Time Estimate:\n")
        f.write(f"  - Total experiment duration: {summary['estimated_total_time_hours']:.1f} hours\n")
        f.write(f"  - Approximately {summary['estimated_total_time_hours']/24:.1f} days\n\n")
        
        f.write("Treatment Allocation:\n")
        allocation = schedule.groupby(['db_size_gb', 'io_method']).size()
        for (db_size, io_method), count in allocation.items():
            db_label = format_db_label(db_size)
            f.write(f"  - {db_label} + {io_method}: {count} runs\n")
        
        f.write("\n" + "=" * 70 + "\n")
    
    print(f"✅ Summary saved to: {summary_file}")
    
    # Print summary to console
    print("\n" + "=" * 70)
    print("EXPERIMENTAL DESIGN SUMMARY")
    print("=" * 70)
    print(f"Design Type: {summary['design_type']}")
    db_labels = [format_db_label(size) for size in summary['db_sizes']]
    print(f"Total Runs: {summary['total_runs']}")
    print(f"I/O Methods: {', '.join(summary['io_methods'])}")
    print(f"Database Sizes: {', '.join(db_labels)}")
    print(f"Replicates: {summary['replicates_per_treatment']} per treatment")
    print(f"Estimated Duration: {summary['estimated_total_time_hours']:.1f} hours ({summary['estimated_total_time_hours']/24:.1f} days)")
    print("=" * 70)
    
    # Show first 10 runs
    print("\nFirst 10 runs in randomized order:")
    preview_columns = [
        col for col in [
            'run_order',
            'db_size_gb',
            'db_name',
            'io_method',
            'replicate',
            'treatment_id'
        ] if col in schedule.columns
    ]
    print(schedule[preview_columns].head(10).to_string(index=False))
    print("\n... (see full schedule in CSV file)")


def main():
    parser = argparse.ArgumentParser(
        description='Generate RCBD schedule for TPC-H I/O benchmarking'
    )
    
    parser.add_argument(
        '--replicates',
        type=int,
        default=5,
        help='Replicates per I/O method and database size (default: 5)'
    )
    
    parser.add_argument(
        '--db-sizes',
        nargs='+',
        type=db_size_type,
        default=[1.0, 10.0, 100.0],
        help='Database sizes in GB (supports decimals, default: 1 10 100)'
    )
    
    parser.add_argument(
        '--io-methods',
        nargs='+',
        default=['sync', 'bgworkers', 'iouring'],
        help='I/O methods to include in the design (default: sync bgworkers iouring)'
    )
    
    parser.add_argument(
        '--output',
        default='experimental_design_schedule.csv',
        help='Output CSV filename (default: experimental_design_schedule.csv)'
    )
    
    parser.add_argument(
        '--runtime-per-run',
        type=int,
        default=30,
        help='Estimated runtime per run in minutes (default: 30)'
    )
    
    parser.add_argument(
        '--cooldown',
        type=int,
        default=5,
        help='Cooldown period between runs in minutes (default: 5)'
    )
    
    parser.add_argument(
        '--seed',
        type=int,
        default=None,
        help='Random seed for reproducibility (default: random)'
    )
    
    args = parser.parse_args()
    
    if args.replicates < 1:
        print("ERROR: Replicates must be at least 1")
        sys.exit(1)
    
    if args.runtime_per_run < 1:
        print("ERROR: Runtime per run must be at least 1 minute")
        sys.exit(1)
    
    if args.cooldown < 0:
        print("ERROR: Cooldown cannot be negative")
        sys.exit(1)
    
    if args.seed is None:
        args.seed = np.random.randint(0, 10000)
        print(f"Seed not provided. Using randomly generated seed: {args.seed}")
    
    io_methods = args.io_methods
    output_file = args.output
    runtime_per_run = args.runtime_per_run
    cooldown = args.cooldown
    formatted_sizes = [format_db_label(size) for size in args.db_sizes]
    
    print("\nGenerating randomized complete block design (RCBD)...")
    print(f"  Replicates: {args.replicates}")
    print(f"  Database sizes: {', '.join(formatted_sizes)}")
    print(f"  I/O methods: {', '.join(io_methods)}")
    print(f"  Runtime per run (min): {runtime_per_run}")
    print(f"  Cooldown (min): {cooldown}")
    print(f"  Seed: {args.seed}")
    
    treatments = generate_treatment_combinations(io_methods, args.db_sizes, args.replicates)
    schedule = generate_rcbd_schedule(treatments, blocking_factor='db_size_gb', seed=args.seed)
    schedule = add_execution_metadata(schedule, runtime_per_run=runtime_per_run, cooldown=cooldown)
    
    # Reorder columns for readability
    preferred_columns = [
        'run_order',
        'db_size_gb',
        'io_method',
        'replicate',
        'treatment_id',
        'db_name',
        'block_id',
        'design_type',
        'estimated_runtime_min',
        'cooldown_min',
        'cumulative_time_min',
        'cumulative_time_hours',
        'status',
        'actual_runtime_sec',
        'execution_timestamp',
        'qphh_result',
        'power_result',
        'throughput_result',
        'notes'
    ]
    existing_columns = [col for col in preferred_columns if col in schedule.columns]
    remaining_columns = [col for col in schedule.columns if col not in existing_columns]
    schedule = schedule[existing_columns + remaining_columns]
    
    summary = generate_summary_stats(schedule)
    save_schedule(schedule, output_file, summary)
    
    if output_file.lower().endswith('.csv'):
        summary_file = f"{output_file[:-4]}_summary.txt"
    else:
        summary_file = f"{output_file}_summary.txt"
    print(f"\nSchedule written to {output_file}")
    print(f"Summary written to {summary_file}")


if __name__ == '__main__':
    main()
