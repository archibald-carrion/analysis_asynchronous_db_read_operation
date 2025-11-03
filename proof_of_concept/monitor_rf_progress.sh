#!/bin/bash
# monitor_rf_progress.sh - Monitor RF1/RF2 progress specifically
# Shows progress indicators when refresh functions are running

DB_NAME="${DB_NAME:-tpch_db_10gb}"
DB_USER="${DB_USER:-tpch_user}"
DB_PASSWORD="${DB_PASSWORD:-tpch_password_123}"

export PGPASSWORD="$DB_PASSWORD"

echo "=== Refresh Function Progress Monitor ==="
echo "Database: $DB_NAME"
echo "Press Ctrl+C to stop"
echo ""

while true; do
    clear
    echo "=== Refresh Function Progress Monitor - $(date '+%Y-%m-%d %H:%M:%S') ==="
    echo ""
    
    # Check if RF1 or RF2 are running
    RF_ACTIVE=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT COUNT(*) 
        FROM pg_stat_activity 
        WHERE datname = '$DB_NAME'
            AND (query ILIKE '%rf1%' OR query ILIKE '%RF1%' 
                 OR query ILIKE '%rf2%' OR query ILIKE '%RF2%'
                 OR query ILIKE '%REFRESH FUNCTION%'
                 OR query ILIKE '%temp_partsupp_sample%'
                 OR query ILIKE '%temp_orders_to_delete%')
            AND state != 'idle';
    " | tr -d ' ')
    
    if [[ "$RF_ACTIVE" -gt 0 ]]; then
        echo "✓ Refresh Function ACTIVE"
        echo ""
        
        # Show current query
        echo "CURRENT QUERY:"
        psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
            SELECT 
                pid,
                now() - query_start as running_for,
                state,
                wait_event_type,
                wait_event,
                CASE 
                    WHEN query ILIKE '%INSERT INTO orders%' THEN 'RF1: Inserting orders...'
                    WHEN query ILIKE '%INSERT INTO lineitem%' THEN 'RF1: Inserting lineitems...'
                    WHEN query ILIKE '%DELETE FROM lineitem%' THEN 'RF2: Deleting lineitems...'
                    WHEN query ILIKE '%DELETE FROM orders%' THEN 'RF2: Deleting orders...'
                    WHEN query ILIKE '%TABLESAMPLE%' THEN 'RF1: Sampling data...'
                    WHEN query ILIKE '%temp_partsupp_sample%' THEN 'RF1: Working with partsupp sample...'
                    WHEN query ILIKE '%temp_orders_to_delete%' THEN 'RF2: Working with orders to delete...'
                    ELSE LEFT(query, 100)
                END as status,
                query
            FROM pg_stat_activity
            WHERE datname = '$DB_NAME'
                AND (query ILIKE '%rf1%' OR query ILIKE '%RF1%' 
                     OR query ILIKE '%rf2%' OR query ILIKE '%RF2%'
                     OR query ILIKE '%REFRESH FUNCTION%'
                     OR query ILIKE '%temp_partsupp_sample%'
                     OR query ILIKE '%temp_orders_to_delete%')
                AND state != 'idle'
            ORDER BY query_start
            LIMIT 5;
        "
        
        echo ""
        echo "PROGRESS INDICATORS:"
        
        # Check table sizes to estimate progress
        ORDERS_COUNT=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "
            SELECT COUNT(*) FROM orders WHERE o_orderdate >= '1998-12-01';
        " | tr -d ' ')
        
        LINEITEM_COUNT=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "
            SELECT COUNT(*) FROM lineitem l 
            JOIN orders o ON l.l_orderkey = o.o_orderkey 
            WHERE o.o_orderdate >= '1998-12-01';
        " | tr -d ' ')
        
        echo "  Orders with date >= 1998-12-01: $ORDERS_COUNT"
        echo "  Lineitems for recent orders: $LINEITEM_COUNT"
        
        # Check temporary tables
        TEMP_TABLES=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "
            SELECT COUNT(*) 
            FROM pg_tables 
            WHERE schemaname LIKE 'pg_temp_%';
        " | tr -d ' ')
        
        if [[ "$TEMP_TABLES" -gt 0 ]]; then
            echo "  Temporary tables exist: $TEMP_TABLES"
            psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                FROM pg_tables 
                WHERE schemaname LIKE 'pg_temp_%'
                ORDER BY tablename;
            "
        fi
        
        # Show locks that might indicate blocking
        BLOCKING=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "
            SELECT COUNT(*) 
            FROM pg_locks l
            JOIN pg_stat_activity a ON l.pid = a.pid
            WHERE NOT l.granted AND a.datname = '$DB_NAME';
        " | tr -d ' ')
        
        if [[ "$BLOCKING" -gt 0 ]]; then
            echo ""
            echo "⚠ WARNING: $BLOCKING queries are blocked/waiting!"
        else
            echo ""
            echo "✓ No blocking detected"
        fi
        
    else
        echo "○ No Refresh Function currently running"
        echo ""
        echo "Last refresh activity:"
        psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
            SELECT 
                pid,
                now() - query_start as time_ago,
                LEFT(query, 80) as last_query
            FROM pg_stat_activity
            WHERE datname = '$DB_NAME'
                AND state = 'idle'
                AND (query ILIKE '%rf1%' OR query ILIKE '%rf2%')
            ORDER BY query_start DESC
            LIMIT 3;
        " 2>/dev/null || echo "  No recent activity found"
    fi
    
    echo ""
    echo "Database size:"
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            pg_size_pretty(pg_database_size('$DB_NAME')) as db_size;
    "
    
    sleep 2
done
