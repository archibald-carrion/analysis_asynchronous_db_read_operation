-- ============================================================================
-- REFRESH FUNCTION 1 (RF1) - Insert New Orders and Line Items
-- ============================================================================
-- Business Purpose: Inserts new orders and line items to simulate ongoing 
-- business transactions. According to TPC-H spec: inserts SF * 1500 new orders
-- with 1-7 line items each (average ~4.5).
-- ============================================================================
-- PERFORMANCE OPTIMIZED: Uses temporary tables and bulk operations for speed
-- ============================================================================

DO $$
DECLARE
    num_orders INTEGER;
    scale_factor NUMERIC;
    refresh_date DATE := '1998-12-01';  -- Refresh date for new orders
    max_orderkey BIGINT;
    partsupp_sample_size INTEGER;
    partsupp_max_rn INTEGER;
    customer_count BIGINT;
    customer_offset BIGINT;
BEGIN
    -- Calculate scale factor from existing data
    RAISE NOTICE 'RF1: Starting execution...';
    SELECT COALESCE(COUNT(*)::NUMERIC / 150000.0, 1) INTO scale_factor
    FROM orders;
    
    -- Calculate number of orders: SF * 1500
    num_orders := GREATEST(1, FLOOR(scale_factor * 1500)::INTEGER);
    RAISE NOTICE 'RF1: Scale factor = %, Number of orders to insert = %', scale_factor, num_orders;
    
    -- Get current max orderkey to avoid collisions
    SELECT COALESCE(MAX(o_orderkey), 0) INTO max_orderkey FROM orders;
    RAISE NOTICE 'RF1: Max orderkey = %', max_orderkey;
    
    -- OPTIMIZED: Get customer count once for efficient sampling
    SELECT COUNT(*) INTO customer_count FROM customer;
    RAISE NOTICE 'RF1: Step 1/3 - Inserting % orders...', num_orders;
    
    -- Step 1: Insert new orders using efficient offset-based random sampling
    -- Much faster than TABLESAMPLE for large tables when we need small samples
    INSERT INTO orders (
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment
    )
    SELECT
        max_orderkey + row_number() OVER () AS o_orderkey,
        c.c_custkey,
        'O' AS o_orderstatus,
        ROUND((RANDOM() * 500000 + 50000)::numeric, 2) AS o_totalprice,
        refresh_date + (RANDOM() * 30)::int AS o_orderdate,
        CASE (RANDOM() * 5)::int
            WHEN 0 THEN '1-URGENT'
            WHEN 1 THEN '2-HIGH'
            WHEN 2 THEN '3-MEDIUM'
            WHEN 3 THEN '4-NOT SPECIFIED'
            ELSE '5-LOW'
        END AS o_orderpriority,
        'Clerk#' || LPAD((RANDOM() * 1000)::int::text, 9, '0') AS o_clerk,
        (RANDOM() * 10)::int AS o_shippriority,
        'New order comment ' || (max_orderkey + row_number() OVER ())::text AS o_comment
    FROM (
        -- OPTIMIZED: Use offset-based sampling instead of TABLESAMPLE
        -- Sample random customers by using random offsets
        SELECT c_custkey
        FROM customer
        TABLESAMPLE SYSTEM (GREATEST(100.0 * num_orders::numeric / GREATEST(customer_count, 1), 0.5))
        LIMIT num_orders
    ) c;
    
    RAISE NOTICE 'RF1: Step 1/3 - Orders inserted successfully';
    
    -- Step 2: Prepare partsupp sample in temporary table for fast access
    partsupp_sample_size := num_orders * 10;
    RAISE NOTICE 'RF1: Step 2/3 - Creating partsupp sample (size = %)...', partsupp_sample_size;
    
    -- Create temporary table for partsupp sample (faster than CTE for repeated access)
    CREATE TEMP TABLE IF NOT EXISTS temp_partsupp_sample AS
    SELECT 
        ps_partkey, 
        ps_suppkey,
        row_number() OVER () as rn
    FROM (
        SELECT ps_partkey, ps_suppkey
        FROM partsupp
        TABLESAMPLE SYSTEM (GREATEST(100.0 * partsupp_sample_size::numeric / 800000.0, 0.1))
        LIMIT partsupp_sample_size
    ) ps_sample;
    
    -- Get max row number once (used for modulo selection)
    SELECT COALESCE(MAX(rn), 1) INTO partsupp_max_rn FROM temp_partsupp_sample;
    
    -- Create index on rn for fast lookups
    CREATE INDEX IF NOT EXISTS idx_temp_partsupp_rn ON temp_partsupp_sample(rn);
    RAISE NOTICE 'RF1: Step 2/3 - Partsupp sample created (max_rn = %)', partsupp_max_rn;
    
    -- Step 3: Insert line items using bulk operation with direct join
    RAISE NOTICE 'RF1: Step 3/3 - Inserting lineitems...';
    -- OPTIMIZED: Eliminate LATERAL join by pre-calculating all partsupp selections
    WITH new_orders AS (
        SELECT o_orderkey, o_orderdate
        FROM orders
        WHERE o_orderdate >= refresh_date
        ORDER BY o_orderkey DESC
        LIMIT num_orders
    ),
    generated_lineitems AS (
        SELECT
            o.o_orderkey,
            o.o_orderdate,
            generate_series(1, 4 + (o.o_orderkey::bigint % 3)) AS line_num
        FROM new_orders o
    ),
    lineitems_with_partsupp AS (
        SELECT
            gl.o_orderkey,
            gl.o_orderdate,
            gl.line_num,
            -- OPTIMIZED: Direct indexed join - eliminates LATERAL subquery overhead!
            -- rn is unique (row_number), so join will be 1:1
            ps.ps_partkey,
            ps.ps_suppkey
        FROM generated_lineitems gl
        JOIN temp_partsupp_sample ps ON (
            ps.rn = 1 + ((gl.o_orderkey::bigint * 1000 + gl.line_num) % GREATEST(partsupp_max_rn, 1))
        )
    )
    INSERT INTO lineitem (
        l_orderkey,
        l_partkey,
        l_suppkey,
        l_linenumber,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_linestatus,
        l_shipdate,
        l_commitdate,
        l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_comment
    )
    SELECT
        lwp.o_orderkey,
        lwp.ps_partkey,
        lwp.ps_suppkey,
        lwp.line_num AS l_linenumber,
        ROUND((RANDOM() * 50 + 1)::numeric, 2) AS l_quantity,
        ROUND((RANDOM() * 100000 + 10000)::numeric, 2) AS l_extendedprice,
        ROUND((RANDOM() * 0.1)::numeric, 2) AS l_discount,
        ROUND((RANDOM() * 0.08 + 0.01)::numeric, 2) AS l_tax,
        CASE WHEN RANDOM() < 0.1 THEN 'R' ELSE 'A' END AS l_returnflag,
        'O' AS l_linestatus,
        lwp.o_orderdate + (RANDOM() * 90)::int AS l_shipdate,
        lwp.o_orderdate + (RANDOM() * 120)::int AS l_commitdate,
        lwp.o_orderdate + (RANDOM() * 150)::int AS l_receiptdate,
        CASE (RANDOM() * 4)::int
            WHEN 0 THEN 'DELIVER IN PERSON'
            WHEN 1 THEN 'COLLECT COD'
            WHEN 2 THEN 'NONE'
            ELSE 'TAKE BACK RETURN'
        END AS l_shipinstruct,
        CASE (RANDOM() * 7)::int
            WHEN 0 THEN 'REG AIR'
            WHEN 1 THEN 'AIR'
            WHEN 2 THEN 'RAIL'
            WHEN 3 THEN 'SHIP'
            WHEN 4 THEN 'TRUCK'
            WHEN 5 THEN 'MAIL'
            ELSE 'FOB'
        END AS l_shipmode,
        'TEXT' AS l_comment
    FROM lineitems_with_partsupp lwp;
    
    -- Clean up temporary table
    DROP TABLE IF EXISTS temp_partsupp_sample;
    RAISE NOTICE 'RF1: Completed successfully!';
END $$;
