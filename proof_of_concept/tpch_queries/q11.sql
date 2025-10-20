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
    AND n_name = '[NATION]'
GROUP BY 
    ps_partkey
HAVING 
    SUM(ps_supplycost * ps_availqty) > (
        SELECT 
sum(ps_supplycost * ps_availqty) *
[fraction]
        FROM 
            partsupp,
            supplier,
            nation
        WHERE 
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = '[NATION]'
    )
ORDER BY 
    value DESC;
