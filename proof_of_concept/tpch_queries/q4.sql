-- Order Priority Checking Query (Q4)
SELECT 
    o_orderpriority,
    COUNT(*) AS order_count
FROM 
    orders
WHERE 
o_orderdate >= date '[DATE]'
and o_orderdate < date '[DATE]' + interval '3' month
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
