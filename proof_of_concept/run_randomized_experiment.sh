#!/bin/bash
# run_randomized_experiment.sh - Execute TPC-H benchmarks following randomized experimental design
#
# This script reads from a pre-generated randomized schedule CSV and executes runs in that order.
# Properly implements a randomized experimental design with multiple database sizes.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MASTER_LOG="$SCRIPT_DIR/randomized_experiment.log"
RESULTS_BASE="$SCRIPT_DIR/randomized_results"
SCHEDULE_FILE="${SCHEDULE_FILE:-$SCRIPT_DIR/experimental_design_schedule.csv}"

# Logging functions
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$MASTER_LOG"; }
error() { log "ERROR: $1"; exit 1; }
warning() { log "WARNING: $1"; }
info() { log "INFO: $1"; }
section() { log "----- $1 -----"; }

# Check if schedule file exists
check_schedule() {
    section "Checking Experimental Schedule"
    
    if [[ ! -f "$SCHEDULE_FILE" ]]; then
        error "Schedule file not found: $SCHEDULE_FILE
        
Please generate it first:
    python generate_experimental_design.py --replicates 5 --seed 42
        "
    fi
    
    # Count total runs, pending runs, and failed runs
    local total_runs=$(tail -n +2 "$SCHEDULE_FILE" | wc -l | tr -d ' ')
    local pending_runs=$(tail -n +2 "$SCHEDULE_FILE" | grep -c "PENDING" || true)
    local failed_runs=$(tail -n +2 "$SCHEDULE_FILE" | grep -c "FAILED" || true)
    local completed_runs=$(tail -n +2 "$SCHEDULE_FILE" | grep -c "COMPLETED" || true)
    local runs_to_execute=$((pending_runs + failed_runs))
    
    log "Schedule file: $SCHEDULE_FILE"
    log "Total runs: $total_runs"
    log "Completed: $completed_runs"
    log "Pending: $pending_runs"
    log "Failed (will retry): $failed_runs"
    log "Runs to execute: $runs_to_execute"
    
    if [[ $runs_to_execute -eq 0 ]]; then
        warning "All runs are already completed!"
        read -p "Do you want to reset and run again? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            reset_schedule
        else
            error "No pending or failed runs. Exiting."
        fi
    fi
    
    export TOTAL_RUNS=$total_runs
    export PENDING_RUNS=$runs_to_execute
    export COMPLETED_RUNS=$completed_runs
}

# Reset schedule status
reset_schedule() {
    info "Resetting schedule status to PENDING..."
    
    # Backup current schedule
    cp "$SCHEDULE_FILE" "${SCHEDULE_FILE}.backup_$(date +%Y%m%d_%H%M%S)"
    
    # Reset status column using awk
    awk -F',' 'BEGIN {OFS=","} NR==1 {print; next} {$13="PENDING"; for(i=14; i<=19; i++) $i=""; print}' \
        "$SCHEDULE_FILE" > "${SCHEDULE_FILE}.tmp"
    mv "${SCHEDULE_FILE}.tmp" "$SCHEDULE_FILE"
    
    log "Schedule reset complete"
}

# Check prerequisites
check_prerequisites() {
    section "Verifying Prerequisites"
    
    # Check required scripts
    [[ -f "$SCRIPT_DIR/run_tests.sh" ]] || error "run_tests.sh not found"
    [[ -f "$SCRIPT_DIR/toggle_pg_config.sh" ]] || error "toggle_pg_config.sh not found"
    
    chmod +x "$SCRIPT_DIR/run_tests.sh"
    chmod +x "$SCRIPT_DIR/toggle_pg_config.sh"
    
    # Check PostgreSQL
    if ! sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; then
        error "PostgreSQL is not available"
    fi
    
    # Check databases exist
    info "Checking required databases..."
    
    local required_dbs=($(tail -n +2 "$SCHEDULE_FILE" | cut -d',' -f6 | sort -u))
    
    for db in "${required_dbs[@]}"; do
        if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$db"; then
            log "✓ Database exists: $db"
        else
            error "❌ Database not found: $db
            
Please create it first:
    SCALE_FACTOR=${db##*_} DB_NAME=$db ./run_setup.sh
            "
        fi
    done
    
    # Check disk space (minimum 20GB)
    local available_space=$(df "$SCRIPT_DIR" | tail -1 | awk '{print $4}')
    if [[ $available_space -lt 20971520 ]]; then
        warning "Low disk space: $(($available_space / 1024 / 1024))GB available"
    fi
    
    log "All prerequisites verified ✓"
}

# Initialize experiment
initialize_experiment() {
    section "Initializing Experiment"
    
    mkdir -p "$RESULTS_BASE"
    mkdir -p "$RESULTS_BASE/raw_data"
    mkdir -p "$RESULTS_BASE/logs"
    
    EXPERIMENT_ID="experiment_$(date '+%Y%m%d_%H%M%S')"
    export EXPERIMENT_ID
    
    log "Experiment ID: $EXPERIMENT_ID"
    log "Results directory: $RESULTS_BASE"
}

# Clear system caches
clear_caches() {
    info "Clearing system caches..."
    sync
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null || warning "Could not clear system cache"
    sleep 3
}

# Restart PostgreSQL
restart_postgresql() {
    info "Restarting PostgreSQL..."
    sudo systemctl restart postgresql
    sleep 10
    
    local retries=0
    while ! sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; do
        retries=$((retries + 1))
        if [[ $retries -gt 30 ]]; then
            error "PostgreSQL did not start after restart"
        fi
        sleep 2
    done
    
    info "PostgreSQL restarted successfully"
}

# Switch to specific database
switch_database() {
    local db_name=$1
    
    info "Switching to database: $db_name"
    
    # Verify database exists
    if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$db_name"; then
        error "Database $db_name does not exist"
    fi
    
    # Export for use by run_tests.sh
    export DB_NAME="$db_name"
    
    log "Database set to: $db_name"
}

# Configure I/O method
configure_io_method() {
    local io_method=$1
    
    info "Configuring I/O method: $io_method"
    
    if ! sudo "$SCRIPT_DIR/toggle_pg_config.sh" "$io_method"; then
        error "Failed to configure I/O method: $io_method"
    fi
    
    log "I/O method configured: $io_method"
}

# Execute single run
execute_run() {
    local run_order=$1
    local db_size_gb=$2
    local io_method=$3
    local replicate=$4
    local db_name=$5
    local treatment_id=$6
    local cooldown_minutes=$7
    
    if [[ -z "$cooldown_minutes" ]]; then
        cooldown_minutes=5
    fi
    
    # Ensure cooldown is a non-negative integer
    if ! [[ "$cooldown_minutes" =~ ^[0-9]+$ ]]; then
        warning "Cooldown value '$cooldown_minutes' is invalid. Falling back to 5 minutes."
        cooldown_minutes=5
    fi
    
    local cooldown_seconds=$((cooldown_minutes * 60))
    
    section "Run $run_order of $TOTAL_RUNS"
    info "Treatment: $treatment_id"
    info "Database: ${db_size_gb}GB ($db_name)"
    info "I/O method: $io_method"
    info "Replicate: #$replicate"
    info "Cooldown (min): $cooldown_minutes"
    
    # Switch database
    switch_database "$db_name"
    
    # Configure I/O method
    configure_io_method "$io_method"
    
    # Restart PostgreSQL
    restart_postgresql
    
    # Clear caches
    clear_caches
    
    # Cooldown period (5 minutes)
    if [[ $cooldown_seconds -gt 0 ]]; then
        info "Cooldown period: ${cooldown_minutes} minute(s)..."
        sleep "$cooldown_seconds"
    else
        info "Cooldown period skipped (0 minutes)"
    fi
    
    # Set environment variables for run_tests.sh
    export IO_METHOD="$io_method"
    export DB_NAME="$db_name"
    export SCALE_FACTOR="$db_size_gb"
    export ITERATIONS=1
    export RUNS_PER_ITERATION=1
    export SKIP_POSTGRES_RESTART=1
    
    # Execute benchmark
    local run_log="$RESULTS_BASE/logs/run_${run_order}_${treatment_id}_rep${replicate}.log"
    local start_timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local start_time=$(date +%s)
    
    log "Starting TPC-H benchmark..."
    
    if "$SCRIPT_DIR/run_tests.sh" "$io_method" > "$run_log" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log "✓ Benchmark completed in $((duration / 60)) minutes"
        
        # Move results to organized location
        local result_prefix="$RESULTS_BASE/raw_data/run${run_order}_${db_size_gb}gb_${io_method}_rep${replicate}"
        
        [[ -f "$SCRIPT_DIR/tpch_complete_results.csv" ]] && \
            mv "$SCRIPT_DIR/tpch_complete_results.csv" "${result_prefix}_complete.csv"
        
        [[ -f "$SCRIPT_DIR/tpch_refresh_results.csv" ]] && \
            mv "$SCRIPT_DIR/tpch_refresh_results.csv" "${result_prefix}_refresh.csv"
        
        [[ -f "$SCRIPT_DIR/tpch_interval_results.csv" ]] && \
            mv "$SCRIPT_DIR/tpch_interval_results.csv" "${result_prefix}_interval.csv"
        
        # Calculate QphH metric using TPC-H formulas
        local qphh_result=$(calculate_qphh "${result_prefix}_complete.csv" "${result_prefix}_refresh.csv" "${result_prefix}_interval.csv")
        
        # Update schedule with results
        update_schedule_status "$run_order" "COMPLETED" "$duration" "$qphh_result" "$start_timestamp"
        unset SKIP_POSTGRES_RESTART
        
        return 0
    else
        warning "Benchmark failed for run $run_order (see $run_log)"
        update_schedule_status "$run_order" "FAILED" "0" "0" "$start_timestamp"
        unset SKIP_POSTGRES_RESTART
        return 1
    fi
}

# Calculate QphH metric (placeholder - implement proper calculation)
calculate_qphh() {
    local complete_csv=$1
    local refresh_csv=$2
    local interval_csv=$3
    
    if [[ ! -f "$complete_csv" ]] || [[ ! -f "$refresh_csv" ]] || [[ ! -f "$interval_csv" ]]; then
        echo "0.00"
        return
    fi
    
    local result
    result=$(python3 - "$complete_csv" "$refresh_csv" "$interval_csv" <<'PY'
import csv
import math
import os
import sys

complete_path, refresh_path, interval_path = sys.argv[1:4]

def parse_positive(value):
    try:
        v = float(value)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None

def collect_times(path, expected_count):
    times = []
    with open(path, newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get('test_type', '').upper() != 'POWER':
                continue
            if row.get('stream_id', '') != '0':
                continue
            value = parse_positive(row.get('execution_time_seconds'))
            if value is not None:
                times.append(value)
    return times if len(times) == expected_count else None

power_times = collect_times(complete_path, 22)
refresh_times = collect_times(refresh_path, 2)

if not power_times or not refresh_times:
    print("0.00")
    sys.exit(0)

with open(interval_path, newline='') as handle:
    reader = csv.DictReader(handle)
    interval_row = next((row for row in reader if row.get('test_type', '').upper() == 'THROUGHPUT'), None)

if not interval_row:
    print("0.00")
    sys.exit(0)

measurement = parse_positive(interval_row.get('measurement_interval_seconds'))
stream_count_val = interval_row.get('stream_count')
try:
    stream_count = int(float(stream_count_val))
except (ValueError, TypeError):
    stream_count = 0

if measurement is None or stream_count <= 0:
    print("0.00")
    sys.exit(0)

try:
    scale_factor = float(os.environ.get('SCALE_FACTOR', '1'))
except ValueError:
    scale_factor = 1.0

# Geometric mean via log-sum to avoid overflow
log_sum = sum(math.log(t) for t in power_times + refresh_times)
geom_mean = math.exp(log_sum / 24.0)

power_metric = (3600.0 * scale_factor) / geom_mean
throughput_metric = ((stream_count * 22 * 3600.0) / measurement) * scale_factor

if power_metric <= 0 or throughput_metric <= 0:
    print("0.00")
    sys.exit(0)

qphh = math.sqrt(power_metric * throughput_metric)
print(f"{qphh:.2f}")
PY
)
    
    if [[ -n "$result" ]]; then
        echo "$result"
    else
        echo "0.00"
    fi
}

# Update schedule CSV with execution results
update_schedule_status() {
    local run_order=$1
    local status=$2
    local runtime_sec=$3
    local qphh=$4
    local timestamp=$5
    
    # Create temporary file with updated status
    awk -F',' -v run="$run_order" -v status="$status" -v runtime="$runtime_sec" \
        -v qphh="$qphh" -v ts="$timestamp" \
        'BEGIN {OFS=","}
         NR==1 {print; next}
         $1==run {$(NF-6)=status; $(NF-5)=runtime; $(NF-4)=ts; $(NF-3)=qphh; print; next}
         {print}' \
        "$SCHEDULE_FILE" > "${SCHEDULE_FILE}.tmp"
    
    mv "${SCHEDULE_FILE}.tmp" "$SCHEDULE_FILE"
    
    info "Schedule updated: Run $run_order → $status"
}

# Generate progress report
show_progress() {
    local current_run=$1
    local total_runs=$2
    local percent=$((current_run * 100 / total_runs))
    
    info "Progress: $current_run / $total_runs runs (${percent}%)"
}

# Main execution loop
execute_experiment() {
    section "Starting Randomized Experiment Execution"
    
    local run_count=0
    local start_time=$(date +%s)
    local completed_counter=$COMPLETED_RUNS
    
    if [[ $completed_counter -gt 0 ]]; then
        show_progress "$completed_counter" "$TOTAL_RUNS"
    fi
    
    # Read schedule line by line (skip header)
    while IFS=',' read -r run_order db_size_gb io_method replicate treatment_id db_name block_id design_type est_runtime cooldown cumulative_time cumulative_hours status actual_runtime exec_timestamp qphh_result power_result throughput_result notes; do
        
        # Skip if already completed
        if [[ "$status" == "COMPLETED" ]]; then
            info "Skipping run $run_order (already completed)"
            continue
        fi
        
        # Log if retrying a failed run
        if [[ "$status" == "FAILED" ]]; then
            info "Retrying run $run_order (previously failed)"
        fi
        
        # Execute the run
        if execute_run "$run_order" "$db_size_gb" "$io_method" "$replicate" "$db_name" "$treatment_id" "$cooldown"; then
            run_count=$((run_count + 1))
            completed_counter=$((completed_counter + 1))
            show_progress "$completed_counter" "$TOTAL_RUNS"
        else
            warning "Run $run_order failed, continuing with next run..."
            run_count=$((run_count + 1))
            show_progress "$completed_counter" "$TOTAL_RUNS"
        fi
        
    done < <(tail -n +2 "$SCHEDULE_FILE")
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    section "Experiment Execution Complete"
    log "Total duration: $((total_duration / 60)) minutes ($((total_duration / 3600)) hours)"
    log "Total runs attempted this session: $run_count"
    log "Cumulative completed runs: $completed_counter / $TOTAL_RUNS"
}

# Generate final report
generate_final_report() {
    section "Generating Final Report"
    
    local report_file="$RESULTS_BASE/experiment_report_${EXPERIMENT_ID}.txt"
    
    cat > "$report_file" << EOF
Randomized TPC-H Benchmark Experiment
=====================================
Experiment ID: $EXPERIMENT_ID
Generated: $(date '+%Y-%m-%d %H:%M:%S')

Experimental Design
-------------------
Design type: $(head -2 "$SCHEDULE_FILE" | tail -1 | cut -d',' -f8)
Total runs: $TOTAL_RUNS
Schedule file: $SCHEDULE_FILE

Execution Summary
-----------------
Completed runs: $(grep -c "COMPLETED" "$SCHEDULE_FILE" || true)
Failed runs: $(grep -c "FAILED" "$SCHEDULE_FILE" || true)
Pending runs: $(grep -c "PENDING" "$SCHEDULE_FILE" || true)

Results Location
----------------
Raw data: $RESULTS_BASE/raw_data/
Run logs: $RESULTS_BASE/logs/
Master log: $MASTER_LOG
Updated schedule: $SCHEDULE_FILE
EOF

    log "Final report saved to: $report_file"
    cat "$report_file"
}

# Main function
main() {
    log "Starting Randomized Experimental Design Execution"
    log "Current time: $(date '+%Y-%m-%d %H:%M:%S')"
    
    check_schedule
    check_prerequisites
    initialize_experiment
    
    # Confirmation
    echo ""
    warning "About to execute $PENDING_RUNS randomized benchmark runs"
    echo -e "This will take approximately ${YELLOW}$((PENDING_RUNS * 35 / 60))${NC} hours"
    echo ""
    read -p "Do you want to continue? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "Execution cancelled by user"
        exit 0
    fi
    
    execute_experiment
    generate_final_report
    
    section "All Done!"
    log "Check the results in: $RESULTS_BASE"
    log "Updated schedule: $SCHEDULE_FILE"
}

# Signal handling
trap 'log "Experiment interrupted!"; exit 1' INT TERM

# Execute
main "$@"
