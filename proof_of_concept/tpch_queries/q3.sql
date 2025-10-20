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
c_mktsegment = '[SEGMENT]'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '[DATE]'
    AND l_shipdate > DATE '[DATE]'
GROUP BY 
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY 
    revenue DESC,
    o_orderdate
LIMIT 10;
