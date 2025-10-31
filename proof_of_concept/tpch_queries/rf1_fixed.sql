-- ============================================================================
-- REFRESH FUNCTION 1 (RF1) - Insert New Orders and Line Items
-- ============================================================================
-- Business Purpose: Inserts new orders and line items to simulate ongoing 
-- business transactions. According to TPC-H spec: inserts SF * 1500 new orders
-- with 1-7 line items each (average ~4.5).
-- ============================================================================

-- Calculate number of orders to insert: SF * 1500
-- Get SF by calculating from existing data: num_orders / 1500
DO $$
DECLARE
    num_orders INTEGER;
    scale_factor NUMERIC;
    refresh_date DATE := '1998-12-01';  -- Refresh date for new orders
    max_orderkey BIGINT;
BEGIN
    -- Calculate scale factor from existing data
    SELECT COALESCE(COUNT(*)::NUMERIC / 150000.0, 1) INTO scale_factor
    FROM orders;
    
    -- Calculate number of orders: SF * 1500
    num_orders := GREATEST(1, FLOOR(scale_factor * 1500)::INTEGER);
    
    -- Get current max orderkey to avoid collisions
    SELECT COALESCE(MAX(o_orderkey), 0) INTO max_orderkey FROM orders;
    
    -- Step 1: Insert new orders
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
        max_orderkey + row_number() OVER (ORDER BY c.c_custkey, sub.row_num) AS o_orderkey,
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
        'New order comment ' || row_number() OVER (ORDER BY c.c_custkey, sub.row_num)::text AS o_comment
    FROM (
        SELECT c_custkey, row_number() OVER (ORDER BY RANDOM()) as row_num
        FROM customer
        ORDER BY RANDOM()
        LIMIT num_orders
    ) sub
    JOIN customer c ON c.c_custkey = sub.c_custkey;
    
    -- Step 2: Insert line items for the new orders
    -- Each order gets 1-7 line items (using modulo for variation: 4 + (o_orderkey % 3) = 4-6 items)
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
    partsupp_randomized AS (
        SELECT ps_partkey, ps_suppkey,
               row_number() OVER (ORDER BY RANDOM()) as rn
        FROM partsupp
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
        gl.o_orderkey,
        ps.ps_partkey,
        ps.ps_suppkey,
        gl.line_num AS l_linenumber,
        ROUND((RANDOM() * 50 + 1)::numeric, 2) AS l_quantity,
        ROUND((RANDOM() * 100000 + 10000)::numeric, 2) AS l_extendedprice,
        ROUND((RANDOM() * 0.1)::numeric, 2) AS l_discount,
        ROUND((RANDOM() * 0.08 + 0.01)::numeric, 2) AS l_tax,
        CASE WHEN RANDOM() < 0.1 THEN 'R' ELSE 'A' END AS l_returnflag,
        'O' AS l_linestatus,
        gl.o_orderdate + (RANDOM() * 90)::int AS l_shipdate,
        gl.o_orderdate + (RANDOM() * 120)::int AS l_commitdate,
        gl.o_orderdate + (RANDOM() * 150)::int AS l_receiptdate,
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
    FROM
        generated_lineitems gl
        CROSS JOIN LATERAL (
            SELECT ps_partkey, ps_suppkey
            FROM partsupp_randomized
            ORDER BY RANDOM()
            LIMIT 1
        ) ps;
END $$;
