"""TPC-H Benchmark Queries"""

TPC_H_QUERIES = {
    "Q1": """
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
            l_linestatus
    """,
    
    "Q2": """
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
    """,
    
    "Q3": """
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
    """,
    
    "Q6": """
        SELECT
            SUM(l_extendedprice * l_discount) AS revenue
        FROM
            lineitem
        WHERE
            l_shipdate >= DATE '1994-01-01'
            AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year'
            AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
            AND l_quantity < 24
    """,
    
    "Q9": """
        SELECT
            nation,
            o_year,
            SUM(amount) AS sum_profit
        FROM (
            SELECT
                n_name AS nation,
                EXTRACT(YEAR FROM o_orderdate) AS o_year,
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
            o_year DESC
    """,
    
    "Q13": """
        SELECT
            c_count,
            COUNT(*) AS custdist
        FROM (
            SELECT
                c_custkey,
                COUNT(o_orderkey) AS c_count
            FROM
                customer LEFT OUTER JOIN orders ON
                c_custkey = o_custkey
                AND o_comment NOT LIKE '%special%requests%'
            GROUP BY
                c_custkey
        ) AS c_orders
        GROUP BY
            c_count
        ORDER BY
            custdist DESC,
            c_count DESC
    """,
    
    "Q18": """
        SELECT
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            SUM(l_quantity) AS total_quantity
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
    """,
    
    "Q22": """
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
                    SELECT
                        *
                    FROM
                        orders
                    WHERE
                        o_custkey = c_custkey
                )
        ) AS custsale
        GROUP BY
            cntrycode
        ORDER BY
            cntrycode
    """
}