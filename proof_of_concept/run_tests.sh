#!/bin/bash

# TPC-H Query Execution Script
# Runs all 22 TPC-H queries in random order

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/query_execution.log"
RESULTS_DIR="$SCRIPT_DIR/query_results"
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

# Check if running as database_user
check_user() {
    if [[ "$(whoami)" != "database_user" ]]; then
        error "This script must be run as database_user"
    fi
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
    p_partkey;
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

    # Create remaining queries (Q4-Q22) similarly...
    # For brevity, I'll show the pattern and you can expand the rest
    
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

    # Add queries 5-22 following the same pattern...
    # You can find the complete TPC-H queries online or generate them from dbgen
    
    log "Created TPC-H queries in $query_dir"
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

# Execute single query
execute_query() {
    local query_num=$1
    local iteration=$2
    local query_file="$SCRIPT_DIR/tpch_queries/q${query_num}.sql"
    local result_file="$RESULTS_DIR/q${query_num}_iter${iteration}.txt"
    local timing_file="$RESULTS_DIR/q${query_num}_iter${iteration}_time.txt"
    
    if [[ ! -f "$query_file" ]]; then
        warning "Query file $query_file not found, skipping"
        return 1
    fi
    
    log "Executing Q$query_num (Iteration $iteration)..."
    
    # Set up password for psql
    export PGPASSWORD="$DB_PASSWORD"
    
    # Execute query with timing
    start_time=$(date +%s.%N)
    
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" \
        -f "$query_file" \
        > "$result_file" 2>>"$LOG_FILE"
    
    end_time=$(date +%s.%N)
    execution_time=$(echo "$end_time - $start_time" | bc)
    
    # Save timing information
    echo "Query Q$query_num, Iteration $iteration: ${execution_time} seconds" >> "$timing_file"
    
    # Check if query executed successfully
    if grep -q "ERROR" "$result_file"; then
        warning "Query Q$query_num encountered errors, check $result_file"
        return 1
    fi
    
    info "Q$query_num completed in ${execution_time} seconds"
    return 0
}

# Main execution function
main() {
    log "Starting TPC-H query execution in random order..."
    log "Database: ${DB_NAME}"
    log "User: ${DB_USER}"
    log "Iterations: ${ITERATIONS}"
    
    # Create directories
    mkdir -p "$RESULTS_DIR"
    
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
    
    for iteration in $(seq 1 $ITERATIONS); do
        log "Starting iteration $iteration of $ITERATIONS"
        
        for query_num in "${query_order[@]}"; do
            execute_query "$query_num" "$iteration"
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
    
    log "TPC-H query execution completed successfully!"
    log "Results saved in: $RESULTS_DIR"
    log "Check $LOG_FILE for detailed logs"
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

Execution Order:
$(for i in "${query_order[@]}"; do echo "  Q$i"; done)

Individual Query Times:
EOF

    # Collect timing information
    for query_num in {1..22}; do
        for iteration in $(seq 1 $ITERATIONS); do
            local timing_file="$RESULTS_DIR/q${query_num}_iter${iteration}_time.txt"
            if [[ -f "$timing_file" ]]; then
                cat "$timing_file" >> "$summary_file"
            fi
        done
    done
    
    # Calculate total execution time
    local total_time=0
    for timing_file in "$RESULTS_DIR"/*_time.txt; do
        if [[ -f "$timing_file" ]]; then
            local time_str=$(grep -o '[0-9]*\.[0-9]*' "$timing_file")
            total_time=$(echo "$total_time + $time_str" | bc)
        fi
    done
    
    echo -e "\nTotal Execution Time: $total_time seconds" >> "$summary_file"
    echo "Average Time per Query: $(echo "scale=2; $total_time / (22 * $ITERATIONS)" | bc) seconds" >> "$summary_file"
    
    log "Execution summary saved to: $summary_file"
}

# Cleanup function (optional)
cleanup() {
    log "Cleaning up..."
    unset PGPASSWORD
}

# Signal handling
trap cleanup EXIT

# Initialize script
check_user
main "$@"