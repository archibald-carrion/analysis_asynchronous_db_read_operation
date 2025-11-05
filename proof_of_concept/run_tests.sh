#!/bin/bash

# TPC-H Complete Benchmark Script - Clean Version
# Uses existing SQL files directly

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${LOG_FILE:-$SCRIPT_DIR/query_execution.log}"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/query_results}"
CSV_OUTPUT="${CSV_OUTPUT:-$SCRIPT_DIR/tpch_complete_results.csv}"
REFRESH_CSV="${REFRESH_CSV:-$SCRIPT_DIR/tpch_refresh_results.csv}"
INTERVAL_CSV="${INTERVAL_CSV:-$SCRIPT_DIR/tpch_interval_results.csv}"
DB_NAME="${DB_NAME:-tpch_db}"
DB_USER="${DB_USER:-tpch_user}"
DB_PASSWORD="${DB_PASSWORD:-tpch_password_123}"
ITERATIONS="${ITERATIONS:-2}"
RUNS_PER_ITERATION="${RUNS_PER_ITERATION:-2}"
QUERY_STREAMS="${QUERY_STREAMS:-2}"
SCALE_FACTOR="${SCALE_FACTOR:-1}"
IO_METHOD="${IO_METHOD:-${1:-sync}}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"; exit 1; }
warning() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"; }
info() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"; }

# Initialize CSV files
initialize_csv() {
    log "Initializing CSV output files"
    
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_id,query_number,execution_order,execution_time_seconds,row_count,timestamp" > "$CSV_OUTPUT"
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_id,refresh_number,execution_order,execution_time_seconds,rows_affected,timestamp" > "$REFRESH_CSV"
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_count,measurement_interval_seconds,start_time,end_time" > "$INTERVAL_CSV"
}

# Test PostgreSQL connection
test_postgres_connection() {
    info "Testing PostgreSQL connection..."
    export PGPASSWORD="$DB_PASSWORD"
    if ! psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" > /dev/null 2>&1; then
        error "Cannot connect to PostgreSQL database $DB_NAME as user $DB_USER"
    fi
    info "PostgreSQL connection successful"
}

# Execute single query with robust error handling
execute_query() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    local test_type=$4
    local stream_id=$5
    local query_num=$6
    local execution_order=$7
    local query_file="$SCRIPT_DIR/tpch_queries/q${query_num}.sql"
    
    if [[ ! -f "$query_file" ]]; then
        warning "Query file $query_file not found, skipping"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
    
    info "Executing Iteration ${iteration} Run ${run_in_iteration} ${test_type} Stream ${stream_id} Q${query_num}..."
    
    export PGPASSWORD="$DB_PASSWORD"
    mkdir -p "$RESULTS_DIR"
    
    local result_file="$RESULTS_DIR/${IO_METHOD}_iter${iteration}_run${run_in_iteration}_${test_type}_s${stream_id}_q${query_num}.txt"
    local start_time=$(date +%s.%N)
    
    # Execute query without timeout (let it run until completion)
    if psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$query_file" > "$result_file" 2>&1; then
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        # Get row count (excluding headers)
        local row_count=$(tail -n +3 "$result_file" | grep -c . 2>/dev/null || echo "0")
        
        # Write to CSV
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},${execution_time},${row_count},$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        
        info "Q${query_num} completed in ${execution_time}s with ${row_count} rows"
        return 0
    else
        local exit_code=$?
        warning "Query Q${query_num} failed with exit code $exit_code"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
}

# Ensure refresh function files exist (copy _fixed.sql to .sql if needed)
ensure_refresh_files() {
    for rf_num in 1 2; do
        local fixed_file="$SCRIPT_DIR/tpch_queries/rf${rf_num}_fixed.sql"
        local target_file="$SCRIPT_DIR/tpch_queries/rf${rf_num}.sql"
        
        if [[ -f "$fixed_file" ]] && [[ ! -f "$target_file" ]]; then
            cp "$fixed_file" "$target_file"
            info "Copied rf${rf_num}_fixed.sql to rf${rf_num}.sql"
        fi
    done
}

# Execute refresh function with robust error handling
execute_refresh_function() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    local test_type=$4
    local stream_id=$5
    local refresh_num=$6
    local execution_order=$7
    
    # Always use _fixed.sql files - copy to .sql if needed
    local fixed_file="$SCRIPT_DIR/tpch_queries/rf${refresh_num}_fixed.sql"
    local refresh_file="$SCRIPT_DIR/tpch_queries/rf${refresh_num}.sql"
    
    # Ensure fixed file exists
    if [[ ! -f "$fixed_file" ]]; then
        warning "Refresh function file $fixed_file not found, skipping"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        return 1
    fi
    
    # Copy fixed file to target file if needed
    if [[ ! -f "$refresh_file" ]] || [[ "$fixed_file" -nt "$refresh_file" ]]; then
        cp "$fixed_file" "$refresh_file"
    fi
    
    info "Executing Iteration ${iteration} Run ${run_in_iteration} ${test_type} Stream ${stream_id} RF${refresh_num}..."
    info "To monitor progress, run: DB_NAME=$DB_NAME ./monitor_rf_progress.sh"
    
    export PGPASSWORD="$DB_PASSWORD"
    local start_time=$(date +%s.%N)
    local output_file="$RESULTS_DIR/${IO_METHOD}_iter${iteration}_run${run_in_iteration}_${test_type}_s${stream_id}_rf${refresh_num}.txt"
    
    # Execute refresh function without timeout (let it run until completion)
    # Add verbose timing for debugging
    if psql -h localhost -U "$DB_USER" -d "$DB_NAME" \
        -c "\set VERBOSITY verbose" \
        -c "\timing on" \
        -f "$refresh_file" > "$output_file" 2>&1; then
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        # Estimate rows affected
        local rows_affected=0
        if [ $refresh_num -eq 1 ]; then
            rows_affected=600  # RF1: ~100 orders + 500 lineitems
        else
            rows_affected=100  # RF2: ~50 orders + 50 lineitems
        fi
        
        # Write to refresh CSV
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},${execution_time},${rows_affected},$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        
        info "RF${refresh_num} completed in ${execution_time}s"
        return 0
    else
        local exit_code=$?
        warning "Refresh function RF${refresh_num} failed with exit code $exit_code"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        return 1
    fi
}

# Power Test (TPC-H Requirement) - SIMPLIFIED without return value issues
execute_power_test() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    
    log "Starting Power Test (Iteration $iteration, Run $run_in_iteration)"
    
    # RF1 before queries
    execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "1" "1"
    
    # Execute all 22 queries sequentially (stream 0)
    for query_num in {1..22}; do
        execute_query "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "$query_num" "$((query_num + 1))"
        sleep 1
    done
    
    # RF2 after queries
    execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "2" "24"
    
    log "Power Test (Iteration $iteration, Run $run_in_iteration) completed"
}

# Generate random query order for throughput test
generate_random_order() {
    local queries=()
    for i in {1..22}; do
        queries+=($i)
    done
    
    for ((i=${#queries[@]}-1; i>0; i--)); do
        j=$((RANDOM % (i+1)))
        temp=${queries[i]}
        queries[i]=${queries[j]}
        queries[j]=$temp
    done
    
    echo "${queries[@]}"
}

# Throughput Test (TPC-H Requirement) - SIMPLIFIED without return value issues
execute_throughput_test() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    
    log "Starting Throughput Test (Iteration $iteration, Run $run_in_iteration) with $QUERY_STREAMS streams"
    
    # Record measurement interval start time
    local start_time=$(date +%s.%N)
    
    local pids=()
    local queries_per_stream=22
    local refresh_start=$((QUERY_STREAMS * queries_per_stream + 1))
    
    # Execute query streams in parallel
    for stream in $(seq 1 $QUERY_STREAMS); do
        (
            # Generate random query order for this stream
            local stream_queries=($(generate_random_order))
            local execution_order=$(( (stream - 1) * queries_per_stream + 1 ))
            for query_num in "${stream_queries[@]}"; do
                execute_query "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "$stream" "$query_num" "$execution_order"
                execution_order=$((execution_order + 1))
            done
        ) &
        pids+=($!)
    done
    
    # Execute refresh stream in background (RF1 and RF2 pairs)
    # According to TPC-H spec: refresh stream runs continuously in parallel with query streams
    # It executes RF1→RF2 pairs repeatedly throughout the measurement interval
    (
        local execution_order=$refresh_start
        local rf_pair=1
        # Execute refresh pairs continuously - enough pairs to match query stream activity
        # Typically one pair per query stream is minimum, but we run a bit more to ensure coverage
        while [ $rf_pair -le $((QUERY_STREAMS * 2)) ]; do
            execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "R" "1" "$execution_order"
            execution_order=$((execution_order + 1))
            execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "R" "2" "$execution_order"
            execution_order=$((execution_order + 1))
            rf_pair=$((rf_pair + 1))
        done
    ) &
    pids+=($!)
    
    # Wait for all processes to complete
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    # Record measurement interval end time
    local end_time=$(date +%s.%N)
    local measurement_interval=$(echo "$end_time - $start_time" | bc)
    
    # Record measurement interval
    echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},THROUGHPUT,${QUERY_STREAMS},${measurement_interval},${start_time},${end_time}" >> "$INTERVAL_CSV"
    
    log "Throughput Test (Iteration $iteration, Run $run_in_iteration) completed in ${measurement_interval} seconds"
}

# Configure PostgreSQL for specific I/O method
configure_postgresql() {
    local io_method=$1
    info "Configuring PostgreSQL for I/O method: $io_method"
    
    case $io_method in
        "sync")
            info "Using synchronous I/O (default)"
            ;;
        "bgworkers")
            info "Using background workers configuration"
            ;;
        "iouring"|"io_uring")
            info "Using io_uring configuration"
            ;;
        *)
            warning "Unknown I/O method: $io_method (continuing with current configuration)"
            ;;
    esac
    
    # Restart PostgreSQL to apply changes unless orchestrator already handled it
    if [[ -z "${SKIP_POSTGRES_RESTART:-}" ]]; then
        sudo systemctl restart postgresql
        sleep 5
    else
        info "PostgreSQL restart skipped (SKIP_POSTGRES_RESTART=${SKIP_POSTGRES_RESTART})"
    fi
}

# Calculate QphH metric for a single run
calculate_qphh() {
    local complete_csv=$1
    local refresh_csv=$2
    local interval_csv=$3
    
    if [[ ! -f "$complete_csv" ]] || [[ ! -f "$refresh_csv" ]] || [[ ! -f "$interval_csv" ]]; then
        echo "0.00"
        return
    fi
    
    local result
    result=$(python3 - "$complete_csv" "$refresh_csv" "$interval_csv" "$SCALE_FACTOR" <<'PY'
import csv
import math
import sys

complete_path, refresh_path, interval_path, scale_factor_str = sys.argv[1:5]

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

try:
    scale_factor = float(scale_factor_str)
except (ValueError, TypeError):
    scale_factor = 1.0

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

# Geometric mean via log-sum to avoid overflow
log_sum = sum(math.log(t) for t in power_times + refresh_times)
geom_mean = math.exp(log_sum / 24.0)

power_metric = (3600.0 * scale_factor) / geom_mean
# Throughput@Size según la imagen: (S × 22 × 3600) / Ts (sin multiplicar por SF)
throughput_metric = (stream_count * 22 * 3600.0) / measurement

if power_metric <= 0 or throughput_metric <= 0:
    print("0.00")
    sys.exit(0)

# QphH@Size según la imagen: 1 / sqrt((1 / Power@Size) × (1 / Throughput@Size))
qphh = 1.0 / math.sqrt((1.0 / power_metric) * (1.0 / throughput_metric))
print(f"{qphh:.2f}")
PY
)
    
    if [[ -n "$result" ]] && [[ "$result" != "0.00" ]]; then
        echo "$result"
    else
        echo "0.00"
    fi
}

# Generate CSV with response variable (QphH) for all runs
generate_response_variable_csv() {
    local response_csv="$RESULTS_DIR/tpch_response_variable.csv"
    
    log "Calculating QphH metrics and generating response variable CSV..."
    
    # Initialize CSV header
    echo "io_method,iteration,run_in_iteration,global_run_id,power_metric,throughput_metric,qphh_metric,scale_factor" > "$response_csv"
    
    local total_runs=$((ITERATIONS * RUNS_PER_ITERATION))
    local calculated_count=0
    local failed_count=0
    
    # Process each run
    for iteration in $(seq 1 $ITERATIONS); do
        for run_in_iteration in $(seq 1 $RUNS_PER_ITERATION); do
            local run_id=$(( (iteration - 1) * RUNS_PER_ITERATION + run_in_iteration ))
            
            # Create temporary CSV files filtered for this specific run
            local temp_complete=$(mktemp)
            local temp_refresh=$(mktemp)
            local temp_interval=$(mktemp)
            
            # Filter CSVs for this run and add headers
            (head -1 "$CSV_OUTPUT" && grep "^${IO_METHOD},${iteration},${run_in_iteration},${run_id}," "$CSV_OUTPUT") > "$temp_complete" 2>/dev/null || true
            (head -1 "$REFRESH_CSV" && grep "^${IO_METHOD},${iteration},${run_in_iteration},${run_id}," "$REFRESH_CSV") > "$temp_refresh" 2>/dev/null || true
            (head -1 "$INTERVAL_CSV" && grep "^${IO_METHOD},${iteration},${run_in_iteration},${run_id}," "$INTERVAL_CSV") > "$temp_interval" 2>/dev/null || true
            
            # Calculate QphH for this run (which internally calculates power and throughput)
            local qphh_result=$(calculate_qphh "$temp_complete" "$temp_refresh" "$temp_interval")
            
            # Extract power and throughput metrics (calculate them separately for completeness)
            local power_metric="0.00"
            local throughput_metric="0.00"
            
            # If we have valid data, calculate power and throughput separately
            if [[ "$qphh_result" != "0.00" ]] && [[ -s "$temp_complete" ]] && [[ -s "$temp_refresh" ]] && [[ -s "$temp_interval" ]]; then
                # Calculate power metric using Python
                power_metric=$(python3 - "$temp_complete" "$temp_refresh" "$SCALE_FACTOR" <<'PY'
import csv
import math
import sys

complete_path, refresh_path, scale_factor_str = sys.argv[1:4]

def parse_positive(value):
    try:
        v = float(value)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None

def collect_times(path):
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
    return times

try:
    scale_factor = float(scale_factor_str)
except (ValueError, TypeError):
    scale_factor = 1.0

power_times = collect_times(complete_path)
refresh_times = collect_times(refresh_path)

if len(power_times) != 22 or len(refresh_times) != 2:
    print("0.00")
    sys.exit(0)

log_sum = sum(math.log(t) for t in power_times + refresh_times)
geom_mean = math.exp(log_sum / 24.0)
power_metric = (3600.0 * scale_factor) / geom_mean
print(f"{power_metric:.2f}")
PY
)
                
                # Calculate throughput metric
                throughput_metric=$(python3 - "$temp_interval" "$QUERY_STREAMS" "$SCALE_FACTOR" <<'PY'
import csv
import sys

interval_path, stream_count_str, scale_factor_str = sys.argv[1:4]

def parse_positive(value):
    try:
        v = float(value)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None

try:
    stream_count = int(float(stream_count_str))
    scale_factor = float(scale_factor_str)
except (ValueError, TypeError):
    stream_count = 0
    scale_factor = 1.0

with open(interval_path, newline='') as handle:
    reader = csv.DictReader(handle)
    interval_row = next((row for row in reader if row.get('test_type', '').upper() == 'THROUGHPUT'), None)

if not interval_row:
    print("0.00")
    sys.exit(0)

measurement = parse_positive(interval_row.get('measurement_interval_seconds'))

if measurement is None or stream_count <= 0:
    print("0.00")
    sys.exit(0)

# Throughput@Size según la imagen: (S × 22 × 3600) / Ts (sin multiplicar por SF)
throughput_metric = (stream_count * 22 * 3600.0) / measurement
print(f"{throughput_metric:.2f}")
PY
)
            fi
            
            if [[ "$qphh_result" != "0.00" ]]; then
                calculated_count=$((calculated_count + 1))
            else
                failed_count=$((failed_count + 1))
                warning "Failed to calculate QphH for Iteration $iteration, Run $run_in_iteration (Run ID: $run_id)"
            fi
            
            # Write to response CSV
            echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${power_metric},${throughput_metric},${qphh_result},${SCALE_FACTOR}" >> "$response_csv"
            
            # Cleanup temp files
            rm -f "$temp_complete" "$temp_refresh" "$temp_interval"
        done
    done
    
    log "Response variable CSV generated: $response_csv"
    log "Successfully calculated QphH for $calculated_count out of $total_runs runs"
    if [[ $failed_count -gt 0 ]]; then
        warning "$failed_count runs failed to calculate QphH (check input CSVs)"
    fi
}

# Generate TPC-H metric calculation summary
generate_tpch_summary() {
    local summary_file="$RESULTS_DIR/tpch_metrics_summary.txt"
    
    cat > "$summary_file" << EOF
TPC-H Complete Benchmark Metrics Summary
Generated: $(date)
I/O Method: $IO_METHOD
Database: $DB_NAME
Scale Factor: $SCALE_FACTOR
Iterations: $ITERATIONS
Runs per Iteration: $RUNS_PER_ITERATION
Total Runs: $((ITERATIONS * RUNS_PER_ITERATION))

Output Files:
- Query Results: $CSV_OUTPUT
- Refresh Results: $REFRESH_CSV  
- Interval Results: $INTERVAL_CSV

TPC-H Metric Formulas:

1. POWER@Size = 3600 × SF × √[1 / (∏ QI(i,0) × ∏ RI(j,0))]^(1/24)

2. THROUGHPUT@Size = (S × 22 × 3600) / T_s

3. QphH@Size = 1 / sqrt((1 / Power@Size) × (1 / Throughput@Size))

Where:
- QI(i,0): Query times from POWER test (stream 0)
- RI(j,0): Refresh times from POWER test (stream 0)  
- S: Query streams ($QUERY_STREAMS)
- T_s: Measurement interval from INTERVAL_CSV
- SF: Scale factor ($SCALE_FACTOR)

Data Structure:
- 15 iterations, each with 2 runs (Run 1 and Run 2)
- For each iteration, calculate TPC-H metrics using the LOWER QphH@Size
- Perform statistical analysis across 15 iterations

Analysis Approach:
1. Calculate Power, Throughput, and QphH for each of the 30 runs
2. Group by iteration (2 runs per iteration)
3. For each iteration, take the lower QphH@Size (TPC-H requirement)
4. Perform statistical analysis on the 15 resulting QphH values
EOF

    log "TPC-H metrics summary saved to: $summary_file"
}

# Main execution function - SIMPLIFIED without execution order tracking
main() {
    log "Starting Complete TPC-H Benchmark..."
    log "I/O Method: $IO_METHOD"
    log "Database: $DB_NAME"
    log "Scale Factor: $SCALE_FACTOR"
    log "Query Streams: $QUERY_STREAMS"
    log "Iterations: $ITERATIONS (with $RUNS_PER_ITERATION runs each)"
    log "Total Runs: $((ITERATIONS * RUNS_PER_ITERATION))"
    
    mkdir -p "$RESULTS_DIR"
    initialize_csv
    test_postgres_connection
    
    # Ensure refresh function files are available
    ensure_refresh_files
    
    # Configure PostgreSQL for this I/O method
    configure_postgresql "$IO_METHOD"
    
    # Execute 15 iterations, each with 2 runs (TPC-H compliant)
    for iteration in $(seq 1 $ITERATIONS); do
        log "Starting Iteration $iteration of $ITERATIONS"
        
        for run_in_iteration in $(seq 1 $RUNS_PER_ITERATION); do
            # Calculate global run_id for CSV tracking
            local run_id=$(( (iteration - 1) * RUNS_PER_ITERATION + run_in_iteration ))
            
            log "Starting Run $run_in_iteration of $RUNS_PER_ITERATION (Global Run ID: $run_id)"
            
            # Power Test followed by Throughput Test (TPC-H requirement)
            execute_power_test "$run_id" "$iteration" "$run_in_iteration"
            execute_throughput_test "$run_id" "$iteration" "$run_in_iteration"
            
            log "Completed Run $run_in_iteration of $RUNS_PER_ITERATION"
        done
        
        log "Completed Iteration $iteration of $ITERATIONS"
    done
    
    generate_tpch_summary
    
    # Calculate QphH metrics and generate response variable CSV
    generate_response_variable_csv
    
    log "Complete TPC-H benchmark execution finished!"
    log "Total runs executed: $((ITERATIONS * RUNS_PER_ITERATION))"
    log "Query results: $CSV_OUTPUT"
    log "Refresh results: $REFRESH_CSV"
    log "Interval results: $INTERVAL_CSV"
    log "Response variable (QphH): ${RESULTS_DIR}/tpch_response_variable.csv"
}

# Cleanup
trap 'unset PGPASSWORD; log "Script execution completed"' EXIT

main "$@"
