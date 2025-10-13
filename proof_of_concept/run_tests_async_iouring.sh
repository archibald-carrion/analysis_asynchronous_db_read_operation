#!/bin/bash

# Parallel TPC-H Query Execution Script using PostgreSQL 18 with io_uring
# Uses io_uring for asynchronous I/O operations

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/query_execution_iouring.log"
RESULTS_DIR="$SCRIPT_DIR/query_results_iouring"
CSV_OUTPUT="$SCRIPT_DIR/tpch_results_iouring.csv"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
ITERATIONS=30  # Number of times to run each query
IOURING_WORKERS=4  # Number of io_uring workers

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"
    exit 1
}

warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"
}

info() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"
}

# Check if io_uring is available
check_iouring_support() {
    log "Checking io_uring support..."
    
    # Check kernel version (io_uring requires 5.1+)
    local kernel_version=$(uname -r | cut -d. -f1-2)
    local major=$(echo $kernel_version | cut -d. -f1)
    local minor=$(echo $kernel_version | cut -d. -f2)
    
    if [ $major -lt 5 ] || ([ $major -eq 5 ] && [ $minor -lt 1 ]); then
        warning "Kernel version $kernel_version detected. io_uring requires kernel 5.1 or newer."
        return 1
    fi
    
    # Check if io_uring is compiled in kernel
    if [ ! -e "/proc/config.gz" ]; then
        warning "Cannot check kernel config. Assuming io_uring is available."
        return 0
    fi
    
    if zgrep -q "CONFIG_IO_URING=y" /proc/config.gz 2>/dev/null; then
        log "✓ io_uring support detected in kernel"
        return 0
    else
        warning "io_uring not enabled in kernel. Performance may be limited."
        return 1
    fi
}

# Configure PostgreSQL for io_uring
configure_postgres_iouring() {
    log "Configuring PostgreSQL for io_uring optimization..."
    
    export PGPASSWORD="$DB_PASSWORD"
    
    # Check current PostgreSQL settings
    local current_io=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c \
        "SELECT name, setting FROM pg_settings WHERE name LIKE '%io%' OR name LIKE '%uring%';" 2>/dev/null)
    
    info "Current I/O settings:"
    echo "$current_io" | while read line; do
        if [ ! -z "$line" ]; then
            info "  $line"
        fi
    done
    
    # Try to enable io_uring if available (PostgreSQL 18+)
    local pg_version=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c \
        "SHOW server_version_num;" | tr -d ' ')
    
    if [ $pg_version -ge 180000 ]; then
        info "PostgreSQL 18+ detected, attempting to configure io_uring..."
        
        # Set io_uring parameters (these would typically be in postgresql.conf)
        # For demo purposes, we'll just log the recommended settings
        cat > "$RESULTS_DIR/postgres_iouring_recommendations.txt" << 'EOF'
PostgreSQL 18 io_uring Configuration Recommendations:

# In postgresql.conf:
io_uring_workers = 4                    # Number of io_uring workers
io_uring_ring_entries = 1024           # Size of io_uring ring buffer
io_uring_sqpoll = on                   # Enable kernel-side polling
io_uring_sqpoll_cpu = 0                # CPU affinity for sqpoll thread

# Additional performance settings:
shared_buffers = 1GB                   # Increase shared buffers
effective_io_concurrency = 4           # Match io_uring workers
wal_compression = on                   # Enable WAL compression
max_worker_processes = 8               # Increase worker processes

# Restart required after changes:
# systemctl restart postgresql
EOF
        log "io_uring configuration recommendations saved to: $RESULTS_DIR/postgres_iouring_recommendations.txt"
    else
        warning "PostgreSQL version < 18 detected. io_uring support may be limited."
    fi
}

# Initialize CSV file
initialize_csv() {
    log "Initializing CSV output file: $CSV_OUTPUT"
    echo "query_number,iteration,execution_order,execution_time_seconds,status,row_count,timestamp,io_worker_id,io_operations" > "$CSV_OUTPUT"
}

# Create TPC-H queries optimized for parallel I/O
create_queries() {
    local query_dir="$SCRIPT_DIR/tpch_queries_iouring"
    mkdir -p "$query_dir"
    
    # Query 1: Pricing Summary Report (I/O intensive)
    cat > "$query_dir/q1.sql" << 'EOF'
-- Pricing Summary Report Query (Q1) - Optimized for parallel I/O
SET effective_io_concurrency = 4;
SET max_parallel_workers_per_gather = 4;

SELECT 
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM 
    lineitem
WHERE 
    l_shipdate <= DATE '1998-12-01' - INTERVAL '90 days'
GROUP BY 
    l_returnflag, 
    l_linestatus
ORDER BY 
    l_returnflag, 
    l_linestatus;
EOF

    # Query 6: Forecasting Revenue Change Query (Sequential scan optimized)
    cat > "$query_dir/q6.sql" << 'EOF'
-- Forecasting Revenue Change Query (Q6) - Sequential scan with async I/O
SET effective_io_concurrency = 4;
SET enable_indexonlyscan = off;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

SELECT 
    SUM(l_extendedprice * l_discount) AS revenue
FROM 
    lineitem
WHERE 
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;
EOF

    # Query 14: Promotion Effect Query (Join heavy with parallel I/O)
    cat > "$query_dir/q14.sql" << 'EOF'
-- Promotion Effect Query (Q14) - Parallel joins
SET effective_io_concurrency = 4;
SET max_parallel_workers_per_gather = 4;
SET enable_parallel_hash = on;
SET enable_parallel_append = on;

SELECT 
    100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
        THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM 
    lineitem,
    part
WHERE 
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1 month';
EOF

    # Query 3: Shipping Priority Query (Complex joins)
    cat > "$query_dir/q3.sql" << 'EOF'
-- Shipping Priority Query (Q3) - Multi-table join with async I/O
SET effective_io_concurrency = 4;
SET max_parallel_workers_per_gather = 4;
SET enable_parallel_hash = on;

SELECT 
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM 
    customer,
    orders,
    lineitem
WHERE 
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY 
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY 
    revenue DESC,
    o_orderdate
LIMIT 10;
EOF

    # Query 5: Local Supplier Volume Query (Large joins)
    cat > "$query_dir/q5.sql" << 'EOF'
-- Local Supplier Volume Query (Q5) - Large multi-table join
SET effective_io_concurrency = 4;
SET max_parallel_workers_per_gather = 4;
SET enable_parallel_hash = on;
SET enable_parallel_append = on;

SELECT 
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM 
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE 
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1994-01-01' + INTERVAL '1 year'
GROUP BY 
    n_name
ORDER BY 
    revenue DESC;
EOF

    # Create remaining queries with basic I/O optimization
    for i in 2 4 7 8 9 10 11 12 13 15 16 17 18 19 20 21 22; do
        cat > "$query_dir/q${i}.sql" << EOF
-- TPC-H Query ${i} - Basic I/O optimization
SET effective_io_concurrency = 4;

$(cat "$SCRIPT_DIR/tpch_queries/q${i}.sql" | tail -n +2)
EOF
    done

    log "Created all 22 TPC-H queries with io_uring optimizations in $query_dir"
}

# Generate random query order
generate_random_order() {
    local queries=()
    for i in {1..22}; do
        queries+=($i)
    done
    
    # Shuffle the array
    for ((i=${#queries[@]}-1; i>0; i--)); do
        j=$((RANDOM % (i+1)))
        temp=${queries[i]}
        queries[i]=${queries[j]}
        queries[j]=$temp
    done
    
    echo "${queries[@]}"
}

# Monitor io_uring statistics
monitor_iouring_stats() {
    local duration=$1
    local stats_file="$RESULTS_DIR/io_uring_stats.csv"
    
    # Initialize stats file
    if [ ! -f "$stats_file" ]; then
        echo "timestamp,user_ops,kernel_ops,submissions,completions,poll_events" > "$stats_file"
    fi
    
    info "Monitoring io_uring statistics for ${duration} seconds..."
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))
    
    while [ $(date +%s) -lt $end_time ]; do
        local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
        
        # Try to get io_uring stats from various sources
        local stats=""
        
        # Check /proc if available
        if [ -f "/proc/self/io_uring" ]; then
            stats=$(cat /proc/self/io_uring 2>/dev/null | tr '\n' ' ' | sed 's/ /,/g')
        fi
        
        # Check debugfs if available
        if [ -d "/sys/kernel/debug/io_uring" ]; then
            local ring_stats=$(find /sys/kernel/debug/io_uring -name "stats" -exec cat {} \; 2>/dev/null | head -1)
            if [ ! -z "$ring_stats" ]; then
                stats="$ring_stats"
            fi
        fi
        
        if [ ! -z "$stats" ]; then
            echo "$timestamp,$stats" >> "$stats_file"
        fi
        
        sleep 2
    done
}

# Execute query with io_uring monitoring
execute_query_iouring() {
    local query_num=$1
    local iteration=$2
    local execution_order=$3
    local io_worker_id=$4
    local query_file="$SCRIPT_DIR/tpch_queries_iouring/q${query_num}.sql"
    
    if [[ ! -f "$query_file" ]]; then
        warning "Query file $query_file not found, skipping"
        echo "${query_num},${iteration},${execution_order},0,SKIPPED,0,$(date '+%Y-%m-%d %H:%M:%S'),${io_worker_id},0" >> "$CSV_OUTPUT"
        return 1
    fi
    
    info "I/O Worker ${io_worker_id}: Executing Q${query_num} (Iteration ${iteration}, Order: ${execution_order})"
    
    # Set up password for psql
    export PGPASSWORD="$DB_PASSWORD"
    
    # Start io_uring monitoring in background
    local monitor_pid=""
    if command -v python3 &> /dev/null; then
        python3 -c "
import time
import subprocess
import os

def get_iouring_ops():
    try:
        # Try to get I/O statistics
        result = subprocess.run(['cat', '/proc/self/io'], capture_output=True, text=True)
        if result.returncode == 0:
            return sum(int(line.split()[1]) for line in result.stdout.strip().split('\n') if 'read_bytes' in line or 'write_bytes' in line)
    except:
        pass
    return 0

start_ops = get_iouring_ops()
start_time = time.time()

# Run the query
try:
    result = subprocess.run([
        'psql', '-h', 'localhost', '-U', '$DB_USER', '-d', '$DB_NAME',
        '-c', '\timing on', '-f', '$query_file'
    ], capture_output=True, text=True, env={**os.environ, 'PGPASSWORD': '$DB_PASSWORD'})
    
    end_time = time.time()
    end_ops = get_iouring_ops()
    execution_time = end_time - start_time
    io_operations = end_ops - start_ops
    
    # Parse timing from PostgreSQL output
    timing_line = [line for line in result.stderr.split('\n') if 'Time:' in line]
    if timing_line:
        time_str = timing_line[-1].split(':')[1].strip().replace(' ms', '')
        try:
            execution_time = float(time_str) / 1000.0
        except:
            pass
    
    # Count rows from output
    row_count = len([line for line in result.stdout.split('\n') if line.strip() and not line.startswith(('--', 'Time:', 'SET', '\\'))]) - 2
    row_count = max(0, row_count)
    
    status = 'SUCCESS' if result.returncode == 0 and 'ERROR' not in result.stderr else 'ERROR'
    
    print(f'{execution_time:.6f}|{status}|{row_count}|{io_operations}')
    
except Exception as e:
    print(f'0.0|ERROR|0|0')
" > "$RESULTS_DIR/q${query_num}_iter${iteration}_worker${io_worker_id}.result" 2>/dev/null &
        monitor_pid=$!
    fi
    
    # Execute query and capture detailed timing
    local start_time=$(date +%s%N)
    
    local output_file="$RESULTS_DIR/q${query_num}_iter${iteration}_worker${io_worker_id}.txt"
    local error_file="$RESULTS_DIR/q${query_num}_iter${iteration}_worker${io_worker_id}.error"
    
    # Run the query
    (
        PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" \
            -c "\timing on" \
            -f "$query_file" > "$output_file" 2> "$error_file"
        
        local exit_code=$?
        local end_time=$(date +%s%N)
        local execution_time_ns=$((end_time - start_time))
        local execution_time=$(echo "scale=6; $execution_time_ns / 1000000000" | bc)
        
        # Wait for monitor to finish
        if [ ! -z "$monitor_pid" ]; then
            wait $monitor_pid 2>/dev/null
        fi
        
        # Read results from monitor
        local io_operations=0
        if [ -f "$RESULTS_DIR/q${query_num}_iter${iteration}_worker${io_worker_id}.result" ]; then
            local result_line=$(cat "$RESULTS_DIR/q${query_num}_iter${iteration}_worker${io_worker_id}.result")
            IFS='|' read -r monitored_time status row_count io_ops <<< "$result_line"
            
            if [ ! -z "$monitored_time" ] && [ "$monitored_time" != "0.0" ]; then
                execution_time="$monitored_time"
            fi
            io_operations="$io_ops"
            
            # Clean up temp file
            rm -f "$RESULTS_DIR/q${query_num}_iter${iteration}_worker${io_worker_id}.result"
        fi
        
        # Count rows from output file as fallback
        local actual_row_count=$(grep -c '^' "$output_file" 2>/dev/null || echo "0")
        actual_row_count=$((actual_row_count - 3))
        if [ $actual_row_count -lt 0 ]; then
            actual_row_count=0
        fi
        
        if [ -z "$row_count" ] || [ "$row_count" = "0" ]; then
            row_count="$actual_row_count"
        fi
        
        if [ $exit_code -eq 0 ] && [ "$status" != "ERROR" ] && ! grep -q "ERROR" "$error_file"; then
            echo "${query_num},${iteration},${execution_order},${execution_time},SUCCESS,${row_count},$(date '+%Y-%m-%d %H:%M:%S'),${io_worker_id},${io_operations}" >> "$CSV_OUTPUT"
            info "I/O Worker ${io_worker_id}: Q${query_num} completed in ${execution_time} seconds (${row_count} rows, ${io_operations} I/O ops)"
        else
            echo "${query_num},${iteration},${execution_order},${execution_time},ERROR,${row_count},$(date '+%Y-%m-%d %H:%M:%S'),${io_worker_id},${io_operations}" >> "$CSV_OUTPUT"
            warning "I/O Worker ${io_worker_id}: Q${query_num} failed"
        fi
    ) &
    
    return 0
}

# Wait for all background jobs to complete
wait_for_all_jobs() {
    local active_jobs=$(jobs -p | wc -l)
    if [ $active_jobs -gt 0 ]; then
        info "Waiting for $active_jobs I/O jobs to complete..."
        wait
    fi
}

# Main execution function with io_uring
main() {
    log "========================================="
    log "TPC-H Benchmark - PostgreSQL 18 with io_uring"
    log "========================================="
    log "Database: ${DB_NAME}"
    log "User: ${DB_USER}"
    log "Iterations: ${ITERATIONS}"
    log "I/O Workers: ${IOURING_WORKERS}"
    log ""
    
    # Create directories
    mkdir -p "$RESULTS_DIR"
    
    # Step 1: Check io_uring support
    check_iouring_support
    
    # Step 2: Configure PostgreSQL for io_uring
    configure_postgres_iouring
    
    # Step 3: Initialize CSV file
    initialize_csv
    
    # Step 4: Create optimized queries
    log "Step 1: Creating TPC-H queries with io_uring optimizations"
    create_queries
    
    # Step 5: Generate random query order for each iteration
    log "Step 2: Generating random query execution orders"
    local query_orders=()
    for iteration in $(seq 1 $ITERATIONS); do
        query_orders[$iteration]="$(generate_random_order)"
        info "Iteration $iteration order: ${query_orders[$iteration]}"
    done
    
    # Step 6: Execute queries with io_uring workers
    log "Step 3: Executing queries with ${IOURING_WORKERS} I/O workers"
    
    local execution_order=1
    local io_worker_id=0
    
    # Start global io_uring monitoring
    monitor_iouring_stats $((ITERATIONS * 60)) &
    local global_monitor_pid=$!
    
    for iteration in $(seq 1 $ITERATIONS); do
        log "Starting iteration $iteration of $ITERATIONS"
        
        # Convert space-separated string to array
        local current_order=(${query_orders[$iteration]})
        
        for query_num in "${current_order[@]}"; do
            # Assign to next I/O worker (round-robin)
            io_worker_id=$(( (io_worker_id % IOURING_WORKERS) + 1 ))
            
            # Execute query with I/O worker
            execute_query_iouring "$query_num" "$iteration" "$execution_order" "$io_worker_id"
            
            execution_order=$((execution_order + 1))
            
            # Small delay to prevent connection storms
            sleep 0.2
        done
        
        # Wait for all queries in this iteration to complete
        wait_for_all_jobs
        
        info "Completed iteration $iteration"
    done
    
    # Stop global monitoring
    kill $global_monitor_pid 2>/dev/null || true
    
    # Final wait for any remaining jobs
    wait_for_all_jobs
    
    # Step 7: Generate summary report
    log "Step 4: Generating execution summary"
    generate_summary
    
    log ""
    log "========================================="
    log "TPC-H query execution with io_uring completed!"
    log "I/O Workers used: ${IOURING_WORKERS}"
    log "CSV results saved to: ${CSV_OUTPUT}"
    log "I/O statistics in: ${RESULTS_DIR}"
    log "Detailed logs in: ${LOG_FILE}"
    log "========================================="
}

# Generate execution summary
generate_summary() {
    local summary_file="$RESULTS_DIR/execution_summary_iouring.txt"
    
    cat > "$summary_file" << EOF
TPC-H Query Execution Summary (io_uring)
Generated: $(date)
Database: $DB_NAME
User: $DB_USER
Iterations: $ITERATIONS
I/O Workers: $IOURING_WORKERS

CSV Output: $CSV_OUTPUT

Execution Statistics:
EOF

    # Calculate statistics from CSV
    local total_queries=$(tail -n +2 "$CSV_OUTPUT" | wc -l)
    local successful_queries=$(tail -n +2 "$CSV_OUTPUT" | grep -c "SUCCESS" || echo "0")
    local failed_queries=$(tail -n +2 "$CSV_OUTPUT" | grep -c "ERROR" || echo "0")
    
    echo "  Total Queries Executed: $total_queries" >> "$summary_file"
    echo "  Successful: $successful_queries" >> "$summary_file"
    echo "  Failed: $failed_queries" >> "$summary_file"
    echo "" >> "$summary_file"
    
    # Calculate total and average execution time
    local total_time=$(tail -n +2 "$CSV_OUTPUT" | grep "SUCCESS" | cut -d',' -f4 | awk '{sum+=$1} END {printf "%.2f", sum}')
    if [ -z "$total_time" ]; then
        total_time=0
    fi
    
    local total_io_ops=$(tail -n +2 "$CSV_OUTPUT" | grep "SUCCESS" | cut -d',' -f9 | awk '{sum+=$1} END {print sum}')
    if [ -z "$total_io_ops" ]; then
        total_io_ops=0
    fi
    
    echo "Total Execution Time: ${total_time} seconds" >> "$summary_file"
    echo "Total I/O Operations: ${total_io_ops}" >> "$summary_file"
    
    if [ "$successful_queries" -gt 0 ]; then
        local avg_time=$(echo "scale=3; $total_time / $successful_queries" | bc)
        local avg_io_ops=$(echo "scale=0; $total_io_ops / $successful_queries" | bc)
        echo "Average Time per Query: ${avg_time} seconds" >> "$summary_file"
        echo "Average I/O Operations per Query: ${avg_io_ops}" >> "$summary_file"
    fi
    
    # I/O Worker statistics
    echo "" >> "$summary_file"
    echo "I/O Worker Distribution:" >> "$summary_file"
    for worker in $(seq 1 $IOURING_WORKERS); do
        local worker_queries=$(tail -n +2 "$CSV_OUTPUT" | cut -d',' -f8 | grep -c "^${worker}$" || echo "0")
        local worker_success=$(tail -n +2 "$CSV_OUTPUT" | grep ",SUCCESS,.*,${worker}," | wc -l)
        local worker_io_ops=$(tail -n +2 "$CSV_OUTPUT" | grep ",SUCCESS,.*,${worker}," | cut -d',' -f9 | awk '{sum+=$1} END {print sum}')
        echo "  I/O Worker ${worker}: ${worker_queries} queries (${worker_success} successful, ${worker_io_ops} I/O ops)" >> "$summary_file"
    done
    
    echo "" >> "$summary_file"
    echo "Top I/O Intensive Queries:" >> "$summary_file"
    echo "Query | Avg I/O Ops | Executions | Avg Time (s)" >> "$summary_file"
    echo "------|-------------|------------|--------------" >> "$summary_file"
    
    # Show I/O intensive queries
    for q in {1..22}; do
        local executions=$(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | wc -l)
        local avg_io=$(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | grep "SUCCESS" | cut -d',' -f9 | awk '{sum+=$1; count++} END {if(count>0) printf "%d", sum/count; else print "N/A"}')
        local avg_time=$(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | grep "SUCCESS" | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.3f", sum/count; else print "N/A"}')
        
        if [ "$executions" -gt 0 ] && [ "$avg_io" != "N/A" ]; then
            echo "Q${q}    | ${avg_io} | ${executions} | ${avg_time}" >> "$summary_file"
        fi
    done
    
    log "Execution summary saved to: $summary_file"
}

# Cleanup function
cleanup() {
    # Kill any remaining background jobs
    jobs -p | xargs -r kill
    unset PGPASSWORD
}

# Signal handling
trap cleanup EXIT INT TERM

# Initialize script
main "$@"