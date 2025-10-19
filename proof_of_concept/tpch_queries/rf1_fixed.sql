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
