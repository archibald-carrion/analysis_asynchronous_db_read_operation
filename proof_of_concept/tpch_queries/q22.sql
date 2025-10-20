-- Global Sales Opportunity Query (Q22)
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
        c_acctbal
    FROM customer
    WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('[I1]', '[I2]', '[I3]', '[I4]', '[I5]', '[I6]', '[I7]')
      AND c_acctbal > (
          SELECT AVG(c_acctbal)
          FROM customer
          WHERE c_acctbal > 0.00
            AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('[I1]', '[I2]', '[I3]', '[I4]', '[I5]', '[I6]', '[I7]')
      )
      AND NOT EXISTS (
          SELECT *
          FROM orders
          WHERE o_custkey = c_custkey
      )
) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode;
