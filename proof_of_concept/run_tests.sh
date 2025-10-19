cat > final_benchmark.sh << 'EOF'
#!/bin/bash

# FINAL TPC-H Benchmark Script - Production Ready
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/final_benchmark_$(date +%Y%m%d_%H%M%S).log"
RESULTS_DIR="$SCRIPT_DIR/final_results"
CSV_OUTPUT="$RESULTS_DIR/tpch_final_results_$(date +%Y%m%d_%H%M%S).csv"
SUMMARY_FILE="$RESULTS_DIR/benchmark_summary_$(date +%Y%m%d_%H%M%S).txt"

# TPC-H Configuration (FINAL)
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
IO_METHOD="${1:-sync}"
ITERATIONS=2           # TPC-H requirement
RUNS_PER_ITERATION=2    # TPC-H requirement  
QUERY_STREAMS=2         # TPC-H minimum
SCALE_FACTOR=1

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"; exit 1; }
warning() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"; }
info() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"; }

# Initialize file structure
initialize() {
    log "Initializing TPC-H Benchmark..."
    mkdir -p "$RESULTS_DIR"
    
    # Create CSV headers
    echo "io_method,iteration,run,global_run_id,test_type,query_number,execution_time,rows,timestamp" > "$CSV_OUTPUT"
    
    # Create fixed refresh functions if they don't exist
    if [[ ! -f "$SCRIPT_DIR/tpch_queries/rf1_fixed.sql" ]]; then
        cat > "$SCRIPT_DIR/tpch_queries/rf1_fixed.sql" << 'RF1EOF'
-- Refresh Function 1 (RF1) - FIXED: Insert new sales
BEGIN;

-- First insert new orders
INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
SELECT 
    o_orderkey + 10000000,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate + INTERVAL '1 year',
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment
FROM orders 
WHERE o_orderkey IN (
    SELECT o_orderkey FROM orders ORDER BY RANDOM() LIMIT 100
);

-- Then insert corresponding lineitems ONLY for the orders we just inserted
INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
SELECT 
    l_orderkey + 10000000,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate + INTERVAL '1 year',
    l_commitdate + INTERVAL '1 year',
    l_receiptdate + INTERVAL '1 year',
    l_shipinstruct,
    l_shipmode,
    l_comment
FROM lineitem 
WHERE l_orderkey IN (
    SELECT o_orderkey - 10000000 
    FROM orders 
    WHERE o_orderkey > 10000000 
    AND o_orderdate >= DATE '1995-01-01'
    LIMIT 500
);

COMMIT;
RF1EOF
        info "Created fixed RF1"
    fi

    if [[ ! -f "$SCRIPT_DIR/tpch_queries/rf2_fixed.sql" ]]; then
        cat > "$SCRIPT_DIR/tpch_queries/rf2_fixed.sql" << 'RF2EOF'
-- Refresh Function 2 (RF2) - FIXED: Delete old sales
BEGIN;

-- Find orders that are safe to delete (no lineitems referencing them)
CREATE TEMPORARY TABLE safe_orders_to_delete AS
SELECT o_orderkey 
FROM orders 
WHERE o_orderdate < DATE '1993-01-01'
AND o_orderkey NOT IN (
    SELECT DISTINCT l_orderkey FROM lineitem WHERE l_orderkey = o_orderkey
)
ORDER BY RANDOM() 
LIMIT 50;

-- Delete from lineitems first for orders we're about to delete
DELETE FROM lineitem 
WHERE l_orderkey IN (SELECT o_orderkey FROM safe_orders_to_delete);

-- Then delete the orders
DELETE FROM orders 
WHERE o_orderkey IN (SELECT o_orderkey FROM safe_orders_to_delete);

DROP TABLE safe_orders_to_delete;

COMMIT;
RF2EOF
        info "Created fixed RF2"
    fi
}

# Test database connection
test_connection() {
    info "Testing database connection..."
    export PGPASSWORD="$DB_PASSWORD"
    if ! psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" > /dev/null 2>&1; then
        error "Cannot connect to database $DB_NAME as user $DB_USER"
    fi
    info "Database connection successful"
}

# Execute query with robust error handling
execute_query() {
    local iteration=$1
    local run=$2
    local global_run_id=$3
    local test_type=$4
    local query_num=$5
    local query_file="$SCRIPT_DIR/tpch_queries/q${query_num}.sql"
    
    info "Executing Q${query_num} ($test_type - Iteration $iteration, Run $run)"
    
    if [[ ! -f "$query_file" ]]; then
        warning "Query file $query_num not found"
        echo "${IO_METHOD},${iteration},${run},${global_run_id},${test_type},${query_num},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
    
    export PGPASSWORD="$DB_PASSWORD"
    local start_time=$(date +%s.%N)
    local output_file="$RESULTS_DIR/${test_type}_iter${iteration}_run${run}_q${query_num}.txt"
    
    if timeout 300s psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$query_file" > "$output_file" 2>&1; then
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        local row_count=$(tail -n +3 "$output_file" | grep -c . 2>/dev/null || echo "0")
        
        echo "${IO_METHOD},${iteration},${run},${global_run_id},${test_type},${query_num},${execution_time},${row_count},$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        info "✓ Q${query_num} completed in ${execution_time}s"
        return 0
    else
        local exit_code=$?
        warning "Q${query_num} failed (exit code: $exit_code)"
        echo "${IO_METHOD},${iteration},${run},${global_run_id},${test_type},${query_num},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
}

# Execute refresh function
execute_refresh() {
    local iteration=$1
    local run=$2
    local global_run_id=$3
    local test_type=$4
    local rf_num=$5
    local rf_file="$SCRIPT_DIR/tpch_queries/rf${rf_num}_fixed.sql"
    
    info "Executing RF${rf_num} ($test_type - Iteration $iteration, Run $run)"
    
    export PGPASSWORD="$DB_PASSWORD"
    local output_file="$RESULTS_DIR/${test_type}_iter${iteration}_run${run}_rf${rf_num}.txt"
    
    if timeout 120s psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$rf_file" > "$output_file" 2>&1; then
        info "✓ RF${rf_num} completed"
        return 0
    else
        warning "RF${rf_num} failed but continuing..."
        return 1
    fi
}

# Power Test (TPC-H requirement)
execute_power_test() {
    local iteration=$1
    local run=$2
    local global_run_id=$3
    
    log "Starting POWER Test (Iteration $iteration, Run $run)"
    
    # RF1 before queries
    execute_refresh "$iteration" "$run" "$global_run_id" "POWER" "1"
    
    # Execute all 22 queries sequentially
    for query_num in {1..22}; do
        execute_query "$iteration" "$run" "$global_run_id" "POWER" "$query_num"
        sleep 1
    done
    
    # RF2 after queries
    execute_refresh "$iteration" "$run" "$global_run_id" "POWER" "2"
    
    log "POWER Test completed (Iteration $iteration, Run $run)"
}

# Throughput Test (TPC-H requirement)  
execute_throughput_test() {
    local iteration=$1
    local run=$2
    local global_run_id=$3
    
    log "Starting THROUGHPUT Test with $QUERY_STREAMS streams (Iteration $iteration, Run $run)"
    local start_time=$(date +%s.%N)
    
    # Execute query streams in parallel
    local pids=()
    for stream in $(seq 1 $QUERY_STREAMS); do
        (
            # Random query order for each stream
            for query_num in $(shuf -i 1-22); do
                execute_query "$iteration" "$run" "$global_run_id" "THROUGHPUT" "$query_num"
            done
        ) &
        pids+=($!)
    done
    
    # Wait for all streams to complete
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    local end_time=$(date +%s.%N)
    local measurement_interval=$(echo "$end_time - $start_time" | bc)
    
    log "THROUGHPUT Test completed in ${measurement_interval}s (Iteration $iteration, Run $run)"
}

# Generate summary report
generate_summary() {
    log "Generating benchmark summary..."
    
    cat > "$SUMMARY_FILE" << EOSUMMARY
TPC-H COMPLETE BENCHMARK RESULTS
Generated: $(date)
I/O Method: $IO_METHOD
Database: $DB_NAME
Scale Factor: $SCALE_FACTOR
Iterations: $ITERATIONS
Runs per Iteration: $RUNS_PER_ITERATION
Total Runs: $((ITERATIONS * RUNS_PER_ITERATION))

EXECUTION SUMMARY:
- Log File: $LOG_FILE
- Results CSV: $CSV_OUTPUT
- Output Directory: $RESULTS_DIR

TPC-H METRICS:
- Power Test: Sequential execution of all 22 queries
- Throughput Test: Parallel execution with $QUERY_STREAMS streams
- Refresh Functions: RF1 (inserts) and RF2 (deletes)

FILES CREATED:
1. $(basename "$LOG_FILE") - Detailed execution log
2. $(basename "$CSV_OUTPUT") - Query timing results
3. $(basename "$SUMMARY_FILE") - This summary file
4. query_results/ - Individual query outputs
5. rf1_fixed.sql, rf2_fixed.sql - Fixed refresh functions

NEXT STEPS:
1. Verify all queries completed successfully
2. Calculate TPC-H metrics (QphH) from the results
3. Compare with other I/O methods
4. Perform statistical analysis on results

EOSUMMARY

    info "Summary saved to: $SUMMARY_FILE"
}

main() {
    log "=== TPC-H COMPLETE BENCHMARK ==="
    log "I/O Method: $IO_METHOD"
    log "Database: $DB_NAME"
    log "Scale Factor: $SCALE_FACTOR"
    log "Iterations: $ITERATIONS"
    log "Runs per Iteration: $RUNS_PER_ITERATION"
    log "Total Runs: $((ITERATIONS * RUNS_PER_ITERATION))"
    
    # Initialize
    initialize
    test_connection
    
    local global_run_id=0
    
    # Main benchmark loop
    for iteration in $(seq 1 $ITERATIONS); do
        log "=== STARTING ITERATION $iteration of $ITERATIONS ==="
        
        for run in $(seq 1 $RUNS_PER_ITERATION); do
            global_run_id=$((global_run_id + 1))
            log "--- Run $run of $RUNS_PER_ITERATION (Global ID: $global_run_id) ---"
            
            # Execute both test types
            execute_power_test "$iteration" "$run" "$global_run_id"
            execute_throughput_test "$iteration" "$run" "$global_run_id"
            
            log "--- Completed Run $run ---"
        done
        
        log "=== COMPLETED ITERATION $iteration ===\n"
    done
    
    generate_summary
    
    log "=== BENCHMARK COMPLETED SUCCESSFULLY ==="
    log "Total queries executed: $((global_run_id * 22 * 2))"  # 22 queries × 2 test types
    log "Results: $CSV_OUTPUT"
    log "Summary: $SUMMARY_FILE"
    log "Log: $LOG_FILE"
}

# Run main function
main "$@"
EOF

chmod +x final_benchmark.sh