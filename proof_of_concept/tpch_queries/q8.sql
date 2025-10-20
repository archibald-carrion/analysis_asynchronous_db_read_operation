-- National Market Share Query (Q8)
SELECT 
    o_year,
    SUM(CASE WHEN nation = '[NATION]' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
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
        AND r_name = '[REGION]'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN DATE '[DATE]' AND DATE '[DATE]' + INTERVAL '1' YEAR
        AND p_type = '[TYPE]'
) AS all_nations
GROUP BY 
    o_year
ORDER BY 
    o_year;
