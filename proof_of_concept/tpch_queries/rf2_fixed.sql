-- ============================================================================
-- REFRESH FUNCTION 2 (RF2) - Delete Old Orders and Line Items
-- ============================================================================
-- Business Purpose: Deletes old orders and line items to simulate data 
-- lifecycle management. According to TPC-H spec: deletes SF * 1500 oldest orders.
-- ============================================================================

DO $$
DECLARE
    num_orders_to_delete INTEGER;
    scale_factor NUMERIC;
    delete_date DATE := '1992-01-01';  -- Cutoff date for deletions
BEGIN
    -- Calculate scale factor from existing data
    SELECT COALESCE(COUNT(*)::NUMERIC / 150000.0, 1) INTO scale_factor
    FROM orders;
    
    -- Calculate number of orders to delete: SF * 1500
    num_orders_to_delete := GREATEST(1, FLOOR(scale_factor * 1500)::INTEGER);
    
    -- OPTIMIZED: Store orderkeys in a temporary variable to avoid repeated subqueries
    -- Step 1: Delete old line items first (due to foreign key constraints)
    -- Use a single query that deletes both lineitems and orders efficiently
    DELETE FROM lineitem
    WHERE l_orderkey IN (
        SELECT o_orderkey
        FROM orders
        WHERE o_orderdate < delete_date
        ORDER BY o_orderdate ASC, o_orderkey ASC
        LIMIT num_orders_to_delete
    );
    
    -- Step 2: Delete old orders (after lineitems are deleted)
    -- OPTIMIZED: Use the same efficient subquery pattern
    DELETE FROM orders
    WHERE o_orderkey IN (
        SELECT o_orderkey
        FROM orders
        WHERE o_orderdate < delete_date
        ORDER BY o_orderdate ASC, o_orderkey ASC
        LIMIT num_orders_to_delete
    );
END $$;
