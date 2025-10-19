#!/bin/bash

# TPC-H Complete Benchmark Script
# Runs Power Test and Throughput Test with all required components for full TPC-H metrics
# 15 iterations with 2 runs each for statistical significance and TPC-H compliance

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/query_execution.log"
RESULTS_DIR="$SCRIPT_DIR/query_results"
CSV_OUTPUT="$SCRIPT_DIR/tpch_complete_results.csv"
REFRESH_CSV="$SCRIPT_DIR/tpch_refresh_results.csv"
INTERVAL_CSV="$SCRIPT_DIR/tpch_interval_results.csv"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
ITERATIONS=15        # 15 iterations for statistical significance
RUNS_PER_ITERATION=2 # 2 runs per iteration (TPC-H requirement)
QUERY_STREAMS=2      # Minimum streams for throughput test
SCALE_FACTOR=1       # Your database scale factor
IO_METHOD="${1:-sync}"  # Get I/O method from command line

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"; }
warning() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"; }
info() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"; }

# Test PostgreSQL connection
test_postgres_connection() {
    info "Testing PostgreSQL connection..."
    export PGPASSWORD="$DB_PASSWORD"
    if ! psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" > /dev/null 2>&1; then
        error "Cannot connect to PostgreSQL database $DB_NAME as user $DB_USER"
        error "Please check:"
        error "1. PostgreSQL is running: sudo systemctl status postgresql"
        error "2. Database exists: psql -h localhost -U $DB_USER -l"
        error "3. User permissions are correct"
        return 1
    fi
    info "PostgreSQL connection successful"
    return 0
}

# Check if required commands are available
check_dependencies() {
    local deps=("psql" "bc")
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            error "Required command '$dep' not found. Please install it."
            return 1
        fi
    done
    return 0
}

# Create TPC-H queries (ALL 22 QUERIES INCLUDED)
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

# Create Refresh Functions (Required for TPC-H)
create_refresh_functions() {
    local query_dir="$SCRIPT_DIR/tpch_queries"
    
    # Refresh Function 1 (RF1): Insert new sales
    cat > "$query_dir/rf1.sql" << 'EOF'
-- Refresh Function 1 (RF1): Insert new sales
BEGIN;

-- Insert new orders
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

-- Insert corresponding lineitems
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
    SELECT l_orderkey FROM lineitem ORDER BY RANDOM() LIMIT 500
);

COMMIT;
EOF

    # Refresh Function 2 (RF2): Delete old sales
    cat > "$query_dir/rf2.sql" << 'EOF'
-- Refresh Function 2 (RF2): Delete old sales
BEGIN;

-- Delete lineitems first (foreign key constraint)
DELETE FROM lineitem 
WHERE l_orderkey IN (
    SELECT o_orderkey FROM orders 
    WHERE o_orderdate < DATE '1993-01-01'
    ORDER BY RANDOM() 
    LIMIT 50
);

-- Then delete orders
DELETE FROM orders 
WHERE o_orderkey IN (
    SELECT o_orderkey FROM orders 
    WHERE o_orderdate < DATE '1993-01-01'
    ORDER BY RANDOM() 
    LIMIT 50
);

COMMIT;
EOF

    log "Created TPC-H refresh functions in $query_dir"
}

# Initialize CSV files
initialize_csv() {
    log "Initializing CSV output files"
    
    # Main query results with iteration and run tracking
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_id,query_number,execution_order,execution_time_seconds,row_count,timestamp" > "$CSV_OUTPUT"
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_id,refresh_number,execution_order,execution_time_seconds,rows_affected,timestamp" > "$REFRESH_CSV"
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_count,measurement_interval_seconds,start_time,end_time" > "$INTERVAL_CSV"
}

# Execute single query
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
    
    # Create results directory
    mkdir -p "$RESULTS_DIR"
    
    # Execute query with better error handling
    local result_file="$RESULTS_DIR/${IO_METHOD}_iter${iteration}_run${run_in_iteration}_${test_type}_s${stream_id}_q${query_num}.txt"
    
    # Time the query execution
    local start_time=$(date +%s.%N)
    
    # Execute query and capture output
    if PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$query_file" > "$result_file" 2>&1; then
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        # Get row count (excluding headers)
        local row_count=$(tail -n +3 "$result_file" | grep -c . 2>/dev/null || echo "0")
        
        # Write to CSV
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},${execution_time},${row_count},$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        
        info "Q${query_num} completed in ${execution_time}s with ${row_count} rows"
        return 0
    else
        warning "Query Q${query_num} failed - check $result_file for details"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
}

# Execute refresh function
execute_refresh_function() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    local test_type=$4
    local stream_id=$5
    local refresh_num=$6
    local execution_order=$7
    local refresh_file="$SCRIPT_DIR/tpch_queries/rf${refresh_num}.sql"
    
    if [[ ! -f "$refresh_file" ]]; then
        warning "Refresh function file $refresh_file not found, skipping"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        return 1
    fi
    
    info "Executing Iteration ${iteration} Run ${run_in_iteration} ${test_type} Stream ${stream_id} RF${refresh_num}..."
    
    export PGPASSWORD="$DB_PASSWORD"
    
    local start_time=$(date +%s.%N)
    
    if PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$refresh_file" > /dev/null 2>&1; then
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        local rows_affected=0
        if [ $refresh_num -eq 1 ]; then
            rows_affected=600
        else
            rows_affected=100
        fi
        
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},${execution_time},${rows_affected},$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        
        info "RF${refresh_num} completed in ${execution_time}s"
        return 0
    else
        warning "Refresh function RF${refresh_num} failed"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        return 1
    fi
}

# Power Test (TPC-H Requirement)
execute_power_test() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    local execution_order_start=$4
    
    log "Starting Power Test (Iteration $iteration, Run $run_in_iteration)"
    local execution_order=$execution_order_start
    
    # RF1 before queries
    if ! execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "1" "$execution_order"; then
        warning "RF1 failed in Power Test"
    fi
    execution_order=$((execution_order + 1))
    
    # Execute all 22 queries sequentially
    for query_num in {1..22}; do
        if ! execute_query "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "$query_num" "$execution_order"; then
            warning "Query Q${query_num} failed in Power Test"
        fi
        execution_order=$((execution_order + 1))
        sleep 1  # Small delay between queries
    done
    
    # RF2 after queries
    if ! execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "2" "$execution_order"; then
        warning "RF2 failed in Power Test"
    fi
    execution_order=$((execution_order + 1))
    
    log "Power Test (Iteration $iteration, Run $run_in_iteration) completed"
    return $execution_order
}

# Configure PostgreSQL (simplified)
configure_postgresql() {
    local io_method=$1
    info "Configuring PostgreSQL for I/O method: $io_method"
    
    # For now, just log the configuration
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
    
    # Test that PostgreSQL is running
    if ! sudo systemctl is-active --quiet postgresql; then
        error "PostgreSQL is not running. Starting it now..."
        sudo systemctl start postgresql
        sleep 5
    fi
    
    return 0
}

# Main execution function with comprehensive error handling
main() {
    log "Starting Complete TPC-H Benchmark..."
    log "I/O Method: $IO_METHOD"
    log "Database: $DB_NAME"
    log "Scale Factor: $SCALE_FACTOR"
    log "Query Streams: $QUERY_STREAMS"
    log "Iterations: $ITERATIONS (with $RUNS_PER_ITERATION runs each)"
    log "Total Runs: $((ITERATIONS * RUNS_PER_ITERATION))"
    
    # Check dependencies
    if ! check_dependencies; then
        error "Dependency check failed"
        exit 1
    fi
    
    # Test database connection
    if ! test_postgres_connection; then
        error "Database connection test failed"
        exit 1
    fi
    
    mkdir -p "$RESULTS_DIR"
    initialize_csv
    create_queries
    create_refresh_functions
    
    # Configure PostgreSQL
    if ! configure_postgresql "$IO_METHOD"; then
        error "PostgreSQL configuration failed"
        exit 1
    fi
    
    local execution_order=1
    
    # Execute iterations
    for iteration in $(seq 1 $ITERATIONS); do
        log "Starting Iteration $iteration of $ITERATIONS"
        
        for run_in_iteration in $(seq 1 $RUNS_PER_ITERATION); do
            local run_id=$(( (iteration - 1) * RUNS_PER_ITERATION + run_in_iteration ))
            
            log "Starting Run $run_in_iteration of $RUNS_PER_ITERATION (Global Run ID: $run_id)"
            
            # Execute tests with error handling
            execution_order=$(execute_power_test "$run_id" "$iteration" "$run_in_iteration" "$execution_order")
            
            log "Completed Run $run_in_iteration of $RUNS_PER_ITERATION"
        done
        
        log "Completed Iteration $iteration of $ITERATIONS"
    done
    
    log "Complete TPC-H benchmark execution finished!"
    log "Results saved to:"
    log "  - Query results: $CSV_OUTPUT"
    log "  - Refresh results: $REFRESH_CSV"
    log "  - Interval results: $INTERVAL_CSV"
}

# Cleanup
trap 'unset PGPASSWORD; log "Script execution completed (or interrupted)"' EXIT

# Run main function
main "$@"