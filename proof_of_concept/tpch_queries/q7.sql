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
        AND ((n1.n_name = '[NATION1]' AND n2.n_name = '[NATION2]')
            OR (n1.n_name = '[NATION2]' AND n2.n_name = '[NATION1]'))
        AND l_shipdate BETWEEN DATE '[DATE]' AND DATE '[DATE]' + INTERVAL '1' YEAR
) AS shipping
GROUP BY 
    supp_nation,
    cust_nation,
    l_year
ORDER BY 
    supp_nation,
    cust_nation,
    l_year;
