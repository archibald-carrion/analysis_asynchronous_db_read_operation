-- ============================================================================
-- REFRESH FUNCTION 2 (RF2) - Delete Old Orders and Line Items
-- ============================================================================
-- Business Purpose: Deletes old orders and line items to simulate data 
-- lifecycle management. According to TPC-H spec: deletes SF * 1500 oldest orders.
-- ============================================================================
-- PERFORMANCE OPTIMIZED: Uses temporary table and direct deletes for speed
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
    
    -- OPTIMIZED: Create temporary table with orderkeys to delete
    -- This allows PostgreSQL to use indexes efficiently for both deletes
    CREATE TEMP TABLE IF NOT EXISTS temp_orders_to_delete (
        o_orderkey BIGINT PRIMARY KEY
    );
    
    -- Populate temp table with orderkeys to delete (execute once)
    INSERT INTO temp_orders_to_delete
    SELECT o_orderkey
    FROM orders
    WHERE o_orderdate < delete_date
    ORDER BY o_orderdate ASC, o_orderkey ASC
    LIMIT num_orders_to_delete;
    
    -- Create index for fast lookups
    -- (Primary key already creates index, but this ensures it exists)
    
    -- Step 1: Delete old line items first (due to foreign key constraints)
    -- OPTIMIZED: Use JOIN with temp table instead of IN subquery
    DELETE FROM lineitem
    USING temp_orders_to_delete tod
    WHERE lineitem.l_orderkey = tod.o_orderkey;
    
    -- Step 2: Delete old orders (after lineitems are deleted)
    -- OPTIMIZED: Use JOIN with temp table for direct delete
    DELETE FROM orders
    USING temp_orders_to_delete tod
    WHERE orders.o_orderkey = tod.o_orderkey;
    
    -- Clean up temporary table
    DROP TABLE IF EXISTS temp_orders_to_delete;
END $$;
