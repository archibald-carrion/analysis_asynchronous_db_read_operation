#!/bin/bash

# Parallel TPC-H Query Execution Script using PostgreSQL Background Workers
# Uses hardcoded background workers for parallel execution

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/query_execution_bgworker.log"
RESULTS_DIR="$SCRIPT_DIR/query_results_bgworker"
CSV_OUTPUT="$SCRIPT_DIR/tpch_results_bgworker.csv"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
ITERATIONS=30  # Number of times to run each query
HARDCODED_WORKERS=4  # Hardcoded number of background workers

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

# Initialize CSV file
initialize_csv() {
    log "Initializing CSV output file: $CSV_OUTPUT"
    echo "query_number,iteration,execution_order,execution_time_seconds,status,row_count,timestamp,worker_id" > "$CSV_OUTPUT"
}

# Create TPC-H queries
create_queries() {
    local query_dir="$SCRIPT_DIR/tpch_queries_bgworker"
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

    # Query 7: Volume Shipping Query
    cat > "$query_dir/q7.sql" << 'EOF'
-- Volume Shipping Query (Q7)
SELECT 
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM (
    SELECT 
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        EXTRACT(year FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM 
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    WHERE 
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY 
    supp_nation,
    cust_nation,
    l_year
ORDER BY 
    supp_nation,
    cust_nation,
    l_year;
EOF

    # Query 8: National Market Share Query
    cat > "$query_dir/q8.sql" << 'EOF'
-- National Market Share Query (Q8)
SELECT 
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM (
    SELECT 
        EXTRACT(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM 
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    WHERE 
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY 
    o_year
ORDER BY 
    o_year;
EOF

    # Query 9: Product Type Profit Measure Query
    cat > "$query_dir/q9.sql" << 'EOF'
-- Product Type Profit Measure Query (Q9)
SELECT 
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM (
    SELECT 
        n_name AS nation,
        EXTRACT(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM 
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE 
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
) AS profit
GROUP BY 
    nation,
    o_year
ORDER BY 
    nation,
    o_year DESC;
EOF

    # Query 10: Returned Item Reporting Query
    cat > "$query_dir/q10.sql" << 'EOF'
-- Returned Item Reporting Query (Q10)
SELECT 
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM 
    customer,
    orders,
    lineitem,
    nation
WHERE 
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-10-01'
    AND o_orderdate < DATE '1993-10-01' + INTERVAL '3 months'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY 
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY 
    revenue DESC
LIMIT 20;
EOF

    # Query 11: Important Stock Identification Query
    cat > "$query_dir/q11.sql" << 'EOF'
-- Important Stock Identification Query (Q11)
SELECT 
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM 
    partsupp,
    supplier,
    nation
WHERE 
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY 
    ps_partkey
HAVING 
    SUM(ps_supplycost * ps_availqty) > (
        SELECT 
            SUM(ps_supplycost * ps_availqty) * 0.0001
        FROM 
            partsupp,
            supplier,
            nation
        WHERE 
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY'
    )
ORDER BY 
    value DESC;
EOF

    # Query 12: Shipping Modes and Order Priority Query
    cat > "$query_dir/q12.sql" << 'EOF'
-- Shipping Modes and Order Priority Query (Q12)
SELECT 
    l_shipmode,
    SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
        THEN 1 ELSE 0 END) AS high_line_count,
    SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
        THEN 1 ELSE 0 END) AS low_line_count
FROM 
    orders,
    lineitem
WHERE 
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1 year'
GROUP BY 
    l_shipmode
ORDER BY 
    l_shipmode;
EOF

    # Query 13: Customer Distribution Query
    cat > "$query_dir/q13.sql" << 'EOF'
-- Customer Distribution Query (Q13)
SELECT 
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT 
        c_custkey,
        COUNT(o_orderkey) AS c_count
    FROM 
        customer
        LEFT OUTER JOIN orders ON c_custkey = o_custkey
            AND o_comment NOT LIKE '%special%requests%'
    GROUP BY 
        c_custkey
) AS c_orders
GROUP BY 
    c_count
ORDER BY 
    custdist DESC,
    c_count DESC;
EOF

    # Query 14: Promotion Effect Query
    cat > "$query_dir/q14.sql" << 'EOF'
-- Promotion Effect Query (Q14)
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

    # Query 15: Top Supplier Query
    cat > "$query_dir/q15.sql" << 'EOF'
-- Top Supplier Query (Q15)
WITH revenue AS (
    SELECT 
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM 
        lineitem
    WHERE 
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3 months'
    GROUP BY 
        l_suppkey
)
SELECT 
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM 
    supplier,
    revenue
WHERE 
    s_suppkey = supplier_no
    AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
ORDER BY 
    s_suppkey;
EOF

    # Query 16: Parts/Supplier Relationship Query
    cat > "$query_dir/q16.sql" << 'EOF'
-- Parts/Supplier Relationship Query (Q16)
SELECT 
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM 
    partsupp,
    part
WHERE 
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT 
            s_suppkey
        FROM 
            supplier
        WHERE 
            s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY 
    p_brand,
    p_type,
    p_size
ORDER BY 
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;
EOF

    # Query 17: Small-Quantity-Order Revenue Query
    cat > "$query_dir/q17.sql" << 'EOF'
-- Small-Quantity-Order Revenue Query (Q17)
SELECT 
    SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM 
    lineitem,
    part
WHERE 
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 
            0.2 * AVG(l_quantity)
        FROM 
            lineitem
        WHERE 
            l_partkey = p_partkey
    );
EOF

    # Query 18: Large Volume Customer Query
    cat > "$query_dir/q18.sql" << 'EOF'
-- Large Volume Customer Query (Q18)
SELECT 
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity)
FROM 
    customer,
    orders,
    lineitem
WHERE 
    o_orderkey IN (
        SELECT 
            l_orderkey
        FROM 
            lineitem
        GROUP BY 
            l_orderkey
        HAVING 
            SUM(l_quantity) > 300
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY 
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY 
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
EOF

    # Query 19: Discounted Revenue Query
    cat > "$query_dir/q19.sql" << 'EOF'
-- Discounted Revenue Query (Q19)
SELECT 
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM 
    lineitem,
    part
WHERE 
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    );
EOF

    # Query 20: Potential Part Promotion Query
    cat > "$query_dir/q20.sql" << 'EOF'
-- Potential Part Promotion Query (Q20)
SELECT 
    s_name,
    s_address
FROM 
    supplier,
    nation
WHERE 
    s_suppkey IN (
        SELECT 
            ps_suppkey
        FROM 
            partsupp
        WHERE 
            ps_partkey IN (
                SELECT 
                    p_partkey
                FROM 
                    part
                WHERE 
                    p_name LIKE 'forest%'
            )
            AND ps_availqty > (
                SELECT 
                    0.5 * SUM(l_quantity)
                FROM 
                    lineitem
                WHERE 
                    l_partkey = ps_partkey
                    AND l_suppkey = ps_suppkey
                    AND l_shipdate >= DATE '1994-01-01'
                    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year'
            )
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY 
    s_name;
EOF

    # Query 21: Suppliers Who Kept Orders Waiting Query
    cat > "$query_dir/q21.sql" << 'EOF'
-- Suppliers Who Kept Orders Waiting Query (Q21)
SELECT 
    s_name,
    COUNT(*) AS numwait
FROM 
    supplier,
    lineitem l1,
    orders,
    nation
WHERE 
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey
        AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT *
        FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY 
    s_name
ORDER BY 
    numwait DESC,
    s_name
LIMIT 100;
EOF

    # Query 22: Global Sales Opportunity Query
    cat > "$query_dir/q22.sql" << 'EOF'
-- Global Sales Opportunity Query (Q22)
SELECT 
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT 
        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM 
        customer
    WHERE 
        SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT 
                AVG(c_acctbal)
            FROM 
                customer
            WHERE 
                c_acctbal > 0.00
                AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        )
        AND NOT EXISTS (
            SELECT *
            FROM orders
            WHERE o_custkey = c_custkey
        )
) AS custsale
GROUP BY 
    cntrycode
ORDER BY 
    cntrycode;
EOF

    log "Created all 22 TPC-H queries in $query_dir"
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

# Execute query using background worker
execute_query_bgworker() {
    local query_num=$1
    local iteration=$2
    local execution_order=$3
    local worker_id=$4
    local query_file="$SCRIPT_DIR/tpch_queries_bgworker/q${query_num}.sql"
    
    if [[ ! -f "$query_file" ]]; then
        warning "Query file $query_file not found, skipping"
        echo "${query_num},${iteration},${execution_order},0,SKIPPED,0,$(date '+%Y-%m-%d %H:%M:%S'),${worker_id}" >> "$CSV_OUTPUT"
        return 1
    fi
    
    info "Worker ${worker_id}: Executing Q${query_num} (Iteration ${iteration}, Order: ${execution_order})"
    
    # Set up password for psql
    export PGPASSWORD="$DB_PASSWORD"
    
    # Execute query in background and capture timing
    local start_time=$(date +%s%N)
    
    # Execute query and capture output
    local output_file="$RESULTS_DIR/q${query_num}_iter${iteration}_worker${worker_id}.txt"
    local error_file="$RESULTS_DIR/q${query_num}_iter${iteration}_worker${worker_id}.error"
    
    # Run the query in background
    (
        PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" \
            -c "\timing on" \
            -f "$query_file" > "$output_file" 2> "$error_file"
        
        local exit_code=$?
        local end_time=$(date +%s%N)
        local execution_time_ns=$((end_time - start_time))
        local execution_time=$(echo "scale=6; $execution_time_ns / 1000000000" | bc)
        
        # Count rows in result (excluding header and footer)
        local row_count=$(grep -c '^' "$output_file" 2>/dev/null || echo "0")
        row_count=$((row_count - 3))  # Subtract header and footer lines
        if [ $row_count -lt 0 ]; then
            row_count=0
        fi
        
        if [ $exit_code -eq 0 ]; then
            # Check if query actually succeeded by looking for errors
            if grep -q "ERROR" "$error_file"; then
                echo "${query_num},${iteration},${execution_order},${execution_time},ERROR,${row_count},$(date '+%Y-%m-%d %H:%M:%S'),${worker_id}" >> "$CSV_OUTPUT"
                warning "Worker ${worker_id}: Q${query_num} failed with errors"
            else
                echo "${query_num},${iteration},${execution_order},${execution_time},SUCCESS,${row_count},$(date '+%Y-%m-%d %H:%M:%S'),${worker_id}" >> "$CSV_OUTPUT"
                info "Worker ${worker_id}: Q${query_num} completed in ${execution_time} seconds ($row_count rows)"
            fi
        else
            echo "${query_num},${iteration},${execution_order},${execution_time},ERROR,${row_count},$(date '+%Y-%m-%d %H:%M:%S'),${worker_id}" >> "$CSV_OUTPUT"
            warning "Worker ${worker_id}: Q${query_num} failed with exit code $exit_code"
        fi
    ) &
    
    return 0
}

# Wait for all background jobs to complete
wait_for_all_jobs() {
    local active_jobs=$(jobs -p | wc -l)
    if [ $active_jobs -gt 0 ]; then
        info "Waiting for $active_jobs background jobs to complete..."
        wait
    fi
}

# Main execution function with background workers
main() {
    log "Starting TPC-H query execution with PostgreSQL background workers..."
    log "Database: ${DB_NAME}"
    log "User: ${DB_USER}"
    log "Iterations: ${ITERATIONS}"
    log "Hardcoded Workers: ${HARDCODED_WORKERS}"
    log "CSV Output: ${CSV_OUTPUT}"
    
    # Create directories
    mkdir -p "$RESULTS_DIR"
    
    # Initialize CSV file
    initialize_csv
    
    # Step 1: Create queries
    log "Step 1: Creating/updating TPC-H queries"
    create_queries
    
    # Step 2: Generate random query order for each iteration
    log "Step 2: Generating random query execution orders"
    local query_orders=()
    for iteration in $(seq 1 $ITERATIONS); do
        query_orders[$iteration]="$(generate_random_order)"
        info "Iteration $iteration order: ${query_orders[$iteration]}"
    done
    
    # Step 3: Execute queries using background workers
    log "Step 3: Executing queries with ${HARDCODED_WORKERS} background workers"
    
    local execution_order=1
    local worker_id=0
    
    for iteration in $(seq 1 $ITERATIONS); do
        log "Starting iteration $iteration of $ITERATIONS"
        
        # Convert space-separated string to array
        local current_order=(${query_orders[$iteration]})
        
        for query_num in "${current_order[@]}"; do
            # Assign to next worker (round-robin)
            worker_id=$(( (worker_id % HARDCODED_WORKERS) + 1 ))
            
            # Execute query with background worker
            execute_query_bgworker "$query_num" "$iteration" "$execution_order" "$worker_id"
            
            execution_order=$((execution_order + 1))
            
            # Small delay to prevent too many concurrent connections
            sleep 0.1
        done
        
        # Wait for all queries in this iteration to complete before next iteration
        wait_for_all_jobs
        
        info "Completed iteration $iteration"
    done
    
    # Final wait for any remaining jobs
    wait_for_all_jobs
    
    # Step 4: Generate summary report
    log "Step 4: Generating execution summary"
    generate_summary
    
    log ""
    log "========================================="
    log "TPC-H query execution with background workers completed!"
    log "Workers used: ${HARDCODED_WORKERS}"
    log "CSV results saved to: ${CSV_OUTPUT}"
    log "Individual results in: ${RESULTS_DIR}"
    log "Detailed logs in: ${LOG_FILE}"
    log "========================================="
}

# Generate execution summary
generate_summary() {
    local summary_file="$RESULTS_DIR/execution_summary_bgworker.txt"
    
    cat > "$summary_file" << EOF
TPC-H Query Execution Summary (Background Workers)
Generated: $(date)
Database: $DB_NAME
User: $DB_USER
Iterations: $ITERATIONS
Background Workers: $HARDCODED_WORKERS

CSV Output: $CSV_OUTPUT

Execution Statistics:
EOF

    # Calculate statistics from CSV
    local total_queries=$(tail -n +2 "$CSV_OUTPUT" | wc -l)
    local successful_queries=$(tail -n +2 "$CSV_OUTPUT" | grep -c "SUCCESS" || echo "0")
    local failed_queries=$(tail -n +2 "$CSV_OUTPUT" | grep -c "ERROR" || echo "0")
    local skipped_queries=$(tail -n +2 "$CSV_OUTPUT" | grep -c "SKIPPED" || echo "0")
    
    echo "  Total Queries Executed: $total_queries" >> "$summary_file"
    echo "  Successful: $successful_queries" >> "$summary_file"
    echo "  Failed: $failed_queries" >> "$summary_file"
    echo "  Skipped: $skipped_queries" >> "$summary_file"
    echo "" >> "$summary_file"
    
    # Calculate total and average execution time
    local total_time=$(tail -n +2 "$CSV_OUTPUT" | grep "SUCCESS" | cut -d',' -f4 | awk '{sum+=$1} END {printf "%.2f", sum}')
    if [ -z "$total_time" ]; then
        total_time=0
    fi
    
    echo "Total Execution Time: ${total_time} seconds" >> "$summary_file"
    
    if [ "$successful_queries" -gt 0 ]; then
        local avg_time=$(echo "scale=3; $total_time / $successful_queries" | bc)
        echo "Average Time per Query: ${avg_time} seconds" >> "$summary_file"
    fi
    
    # Worker statistics
    echo "" >> "$summary_file"
    echo "Worker Distribution:" >> "$summary_file"
    for worker in $(seq 1 $HARDCODED_WORKERS); do
        local worker_queries=$(tail -n +2 "$CSV_OUTPUT" | cut -d',' -f8 | grep -c "^${worker}$" || echo "0")
        local worker_success=$(tail -n +2 "$CSV_OUTPUT" | grep ",SUCCESS,.*,${worker}$" | wc -l)
        echo "  Worker ${worker}: ${worker_queries} queries (${worker_success} successful)" >> "$summary_file"
    done
    
    echo "" >> "$summary_file"
    echo "Individual Query Performance (from CSV):" >> "$summary_file"
    echo "Query | Avg Time (s) | Executions | Status" >> "$summary_file"
    echo "------|--------------|------------|--------" >> "$summary_file"
    
    # Show per-query statistics
    for q in {1..22}; do
        local executions=$(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | wc -l)
        local avg=$(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | grep "SUCCESS" | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.3f", sum/count; else print "N/A"}')
        local status="MIXED"
        if [ $executions -eq 0 ]; then
            status="NOT EXECUTED"
        elif [ $(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | grep -c "SUCCESS") -eq $executions ]; then
            status="ALL SUCCESS"
        elif [ $(tail -n +2 "$CSV_OUTPUT" | grep "^${q}," | grep -c "ERROR") -eq $executions ]; then
            status="ALL ERROR"
        fi
        
        echo "Q${q}    | ${avg} | ${executions} | ${status}" >> "$summary_file"
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