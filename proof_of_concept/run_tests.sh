#!/bin/bash

# TPC-H Query Execution Script
# Runs all 22 TPC-H queries in random order and generates CSV output

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/query_execution.log"
RESULTS_DIR="$SCRIPT_DIR/query_results"
CSV_OUTPUT="$SCRIPT_DIR/tpch_results.csv"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
ITERATIONS=1  # Number of times to run each query

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

# Create TPC-H queries
create_queries() {
    local query_dir="$SCRIPT_DIR/tpch_queries"
    mkdir -p "$query_dir"
    
    # Query 1: Pricing Summary Report
    cat > "$query_dir/q1.sql" << 'EOF'
-- Pricing Summary Report Query (Q1)
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

    # Query 2: Minimum Cost Supplier Query
    cat > "$query_dir/q2.sql" << 'EOF'
-- Minimum Cost Supplier Query (Q2)
SELECT 
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM 
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE 
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT 
            MIN(ps_supplycost)
        FROM 
            partsupp,
            supplier,
            nation,
            region
        WHERE 
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
    )
ORDER BY 
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
EOF

    # Query 3: Shipping Priority Query
    cat > "$query_dir/q3.sql" << 'EOF'
-- Shipping Priority Query (Q3)
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

    # Query 4: Order Priority Checking Query
    cat > "$query_dir/q4.sql" << 'EOF'
-- Order Priority Checking Query (Q4)
SELECT 
    o_orderpriority,
    COUNT(*) AS order_count
FROM 
    orders
WHERE 
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3 months'
    AND EXISTS (
        SELECT *
        FROM lineitem
        WHERE l_orderkey = o_orderkey
        AND l_commitdate < l_receiptdate
    )
GROUP BY 
    o_orderpriority
ORDER BY 
    o_orderpriority;
EOF

    # Query 5: Local Supplier Volume Query
    cat > "$query_dir/q5.sql" << 'EOF'
-- Local Supplier Volume Query (Q5)
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

    # Query 6: Forecasting Revenue Change Query
    cat > "$query_dir/q6.sql" << 'EOF'
-- Forecasting Revenue Change Query (Q6)
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

    log "Created TPC-H queries in $query_dir"
}

# Generate random query order
generate_random_order() {
    local queries=()
    for i in {1..6}; do  # Only 6 queries created for now
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

# Initialize CSV file
initialize_csv() {
    log "Initializing CSV output file: $CSV_OUTPUT"
    echo "query_number,iteration,execution_order,execution_time_seconds,status,row_count,timestamp" > "$CSV_OUTPUT"
}

# Execute single query
execute_query() {
    local query_num=$1
    local iteration=$2
    local execution_order=$3
    local query_file="$SCRIPT_DIR/tpch_queries/q${query_num}.sql"
    local result_file="$RESULTS_DIR/q${query_num}_iter${iteration}.txt"
    
    if [[ ! -f "$query_file" ]]; then
        warning "Query file $query_file not found, skipping"
        echo "${query_num},${iteration},${execution_order},0,SKIPPED,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
    
    log "Executing Q$query_num (Iteration $iteration, Order: $execution_order)..."
    
    # Set up password for psql
    export PGPASSWORD="$DB_PASSWORD"
    
    # Execute query with timing
    local start_time=$(date +%s.%N)
    
    if psql -h localhost -U "$DB_USER" -d "$DB_NAME" \
        -f "$query_file" \
        > "$result_file" 2>>"$LOG_FILE"; then
        
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        # Count rows in result (excluding header)
        local row_count=$(grep -c '^' "$result_file" || echo "0")
        row_count=$((row_count - 2))  # Subtract header lines
        if [ $row_count -lt 0 ]; then
            row_count=0
        fi
        
        # Write to CSV
        echo "${query_num},${iteration},${execution_order},${execution_time},SUCCESS,${row_count},$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        
        info "Q$query_num completed in ${execution_time} seconds ($row_count rows)"
        return 0
    else
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        warning "Query Q$query_num encountered errors"
        echo "${query_num},${iteration},${execution_order},${execution_time},ERROR,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
}

# Main execution function
main() {
    log "Starting TPC-H query execution in random order..."
    log "Database: ${DB_NAME}"
    log "User: ${DB_USER}"
    log "Iterations: ${ITERATIONS}"
    log "CSV Output: ${CSV_OUTPUT}"
    
    # Create directories
    mkdir -p "$RESULTS_DIR"
    
    # Initialize CSV file
    initialize_csv
    
    # Step 1: Create queries if they don't exist
    if [[ ! -d "$SCRIPT_DIR/tpch_queries" ]]; then
        log "Step 1: Creating TPC-H queries"
        create_queries
    else
        log "Step 1: TPC-H queries already exist"
    fi
    
    # Step 2: Generate random query order
    log "Step 2: Generating random query execution order"
    local query_order=($(generate_random_order))
    info "Query execution order: ${query_order[*]}"
    
    # Step 3: Execute queries in random order
    log "Step 3: Executing queries in random order"
    
    local execution_order=1
    for iteration in $(seq 1 $ITERATIONS); do
        log "Starting iteration $iteration of $ITERATIONS"
        
        for query_num in "${query_order[@]}"; do
            execute_query "$query_num" "$iteration" "$execution_order"
            execution_order=$((execution_order + 1))
        done
        
        # Generate new random order for next iteration if multiple iterations
        if [[ $iteration -lt $ITERATIONS ]]; then
            query_order=($(generate_random_order))
            info "New query order for iteration $((iteration + 1)): ${query_order[*]}"
        fi
    done
    
    # Step 4: Generate summary report
    log "Step 4: Generating execution summary"
    generate_summary
    
    log ""
    log "========================================="
    log "TPC-H query execution completed successfully!"
    log "CSV results saved to: ${CSV_OUTPUT}"
    log "Individual results in: ${RESULTS_DIR}"
    log "Detailed logs in: ${LOG_FILE}"
    log "========================================="
}

# Generate execution summary
generate_summary() {
    local summary_file="$RESULTS_DIR/execution_summary.txt"
    
    cat > "$summary_file" << EOF
TPC-H Query Execution Summary
Generated: $(date)
Database: $DB_NAME
User: $DB_USER
Iterations: $ITERATIONS

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
    local total_time=$(tail -n +2 "$CSV_OUTPUT" | grep "SUCCESS" | cut -d',' -f4 | awk '{sum+=$1} END {print sum}')
    if [ -z "$total_time" ]; then
        total_time=0
    fi
    
    echo "Total Execution Time: ${total_time} seconds" >> "$summary_file"
    
    if [ "$successful_queries" -gt 0 ]; then
        local avg_time=$(echo "scale=2; $total_time / $successful_queries" | bc)
        echo "Average Time per Query: ${avg_time} seconds" >> "$summary_file"
    fi
    
    echo "" >> "$summary_file"
    echo "Individual Query Performance (from CSV):" >> "$summary_file"
    echo "Query | Avg Time (s) | Status" >> "$summary_file"
    echo "------|--------------|--------" >> "$summary_file"
    
    # Show per-query statistics
    for q in {1..6}; do
        local avg=$(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | grep "SUCCESS" | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.3f", sum/count; else print "N/A"}')
        local status=$(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | tail -1 | cut -d',' -f5)
        echo "Q${q}    | ${avg} | ${status}" >> "$summary_file"
    done
    
    log "Execution summary saved to: $summary_file"
}

# Cleanup function
cleanup() {
    unset PGPASSWORD
}

# Signal handling
trap cleanup EXIT

# Initialize script
main "$@"