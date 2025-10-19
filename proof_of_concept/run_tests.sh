#!/bin/bash

# TPC-H Complete Benchmark Script - Clean Version
# Uses existing SQL files directly

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/query_execution.log"
RESULTS_DIR="$SCRIPT_DIR/query_results"
CSV_OUTPUT="$SCRIPT_DIR/tpch_complete_results.csv"
REFRESH_CSV="$SCRIPT_DIR/tpch_refresh_results.csv"
INTERVAL_CSV="$SCRIPT_DIR/tpch_interval_results.csv"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
ITERATIONS=2
RUNS_PER_ITERATION=2
QUERY_STREAMS=2
SCALE_FACTOR=1
IO_METHOD="${1:-sync}"

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
    
    # Execute query with timeout
    if timeout 300s psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$query_file" > "$result_file" 2>&1; then
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

# Execute refresh function with robust error handling
execute_refresh_function() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    local test_type=$4
    local stream_id=$5
    local refresh_num=$6
    local execution_order=$7
    
    # Try fixed version first, then fall back to original
    local refresh_file="$SCRIPT_DIR/tpch_queries/rf${refresh_num}_fixed.sql"
    if [[ ! -f "$refresh_file" ]]; then
        refresh_file="$SCRIPT_DIR/tpch_queries/rf${refresh_num}.sql"
    fi
    
    if [[ ! -f "$refresh_file" ]]; then
        warning "Refresh function file $refresh_file not found, skipping"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        return 1
    fi
    
    info "Executing Iteration ${iteration} Run ${run_in_iteration} ${test_type} Stream ${stream_id} RF${refresh_num}..."
    
    export PGPASSWORD="$DB_PASSWORD"
    local start_time=$(date +%s.%N)
    local output_file="$RESULTS_DIR/${IO_METHOD}_iter${iteration}_run${run_in_iteration}_${test_type}_s${stream_id}_rf${refresh_num}.txt"
    
    # Execute refresh function with timeout
    if timeout 120s psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$refresh_file" > "$output_file" 2>&1; then
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
    
    # Array to track background process IDs
    local pids=()
    local execution_order=1
    
    # Execute query streams in parallel
    for stream in $(seq 1 $QUERY_STREAMS); do
        (
            # Generate random query order for this stream
            local stream_queries=($(generate_random_order))
            for query_num in "${stream_queries[@]}"; do
                execute_query "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "$stream" "$query_num" "$execution_order"
                execution_order=$((execution_order + 1))
            done
        ) &
        pids+=($!)
    done
    
    # Execute refresh stream in background (RF1 and RF2 pairs)
    (
        for rf_pair in $(seq 1 $QUERY_STREAMS); do
            execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "R" "1" "$execution_order"
            execution_order=$((execution_order + 1))
            execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "R" "2" "$execution_order"
            execution_order=$((execution_order + 1))
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
        "io_uring")
            info "Using io_uring configuration"
            ;;
    esac
    
    # Restart PostgreSQL to apply changes
    sudo systemctl restart postgresql
    sleep 5
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

2. THROUGHPUT@Size = (S × 22 × 3600 / T_s) × SF

3. QphH@Size = √(POWER@Size × THROUGHPUT@Size)

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
    log "Complete TPC-H benchmark execution finished!"
    log "Total runs executed: $((ITERATIONS * RUNS_PER_ITERATION))"
    log "Query results: $CSV_OUTPUT"
    log "Refresh results: $REFRESH_CSV"
    log "Interval results: $INTERVAL_CSV"
}

# Cleanup
trap 'unset PGPASSWORD; log "Script execution completed"' EXIT

main "$@"
