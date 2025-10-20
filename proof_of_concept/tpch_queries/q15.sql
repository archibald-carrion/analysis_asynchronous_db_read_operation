-- Top Supplier Query (Q15)
WITH revenue AS (
    SELECT 
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM 
        lineitem
    WHERE 
l_shipdate >= date '[DATE]'
and l_shipdate < date '[DATE]' + interval '3' month
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
