-- Forecasting Revenue Change Query (Q6)
SELECT 
    SUM(l_extendedprice * l_discount) AS revenue
FROM 
    lineitem
WHERE 
    l_shipdate >= DATE '[DATE]'
    AND l_shipdate < DATE '[DATE]' + INTERVAL '1' YEAR
    AND l_discount BETWEEN [DISCOUNT] - 0.01 AND [DISCOUNT] + 0.01
    AND l_quantity < [QUANTITY];
