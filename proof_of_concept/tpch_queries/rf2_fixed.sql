-- Refresh Function 2 (RF2) - FIXED: Delete old sales
BEGIN;

-- Find orders that are safe to delete (no lineitems referencing them)
CREATE TEMPORARY TABLE safe_orders_to_delete AS
SELECT o_orderkey 
FROM orders 
WHERE o_orderdate < DATE '1993-01-01'
AND o_orderkey NOT IN (
    SELECT DISTINCT l_orderkey FROM lineitem WHERE l_orderkey = o_orderkey
)
ORDER BY RANDOM() 
LIMIT 50;

-- Delete from lineitems first for orders we're about to delete
DELETE FROM lineitem 
WHERE l_orderkey IN (SELECT o_orderkey FROM safe_orders_to_delete);

-- Then delete the orders
DELETE FROM orders 
WHERE o_orderkey IN (SELECT o_orderkey FROM safe_orders_to_delete);

DROP TABLE safe_orders_to_delete;

COMMIT;
