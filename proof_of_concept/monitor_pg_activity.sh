#!/bin/bash
# monitor_pg_activity.sh - Monitor PostgreSQL activity in real-time
# Shows what queries are running, blocking, and waiting

DB_NAME="${DB_NAME:-tpch_db_10gb}"
DB_USER="${DB_USER:-tpch_user}"

echo "=== PostgreSQL Activity Monitor ==="
echo "Database: $DB_NAME"
echo "Press Ctrl+C to stop"
echo ""

# Function to show active queries
show_activity() {
    echo "--- $(date '+%Y-%m-%d %H:%M:%S') ---"
    
    # Active queries with details
    echo "ACTIVE QUERIES:"
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            pid,
            now() - query_start as duration,
            state,
            wait_event_type,
            wait_event,
            LEFT(query, 80) as query_preview
        FROM pg_stat_activity
        WHERE datname = '$DB_NAME'
            AND state != 'idle'
            AND pid != pg_backend_pid()
        ORDER BY query_start;
    "
    
    echo ""
    echo "BLOCKING QUERIES:"
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            blocked_locks.pid AS blocked_pid,
            blocking_locks.pid AS blocking_pid,
            blocked_activity.usename AS blocked_user,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query AS blocked_statement,
            blocking_activity.query AS blocking_statement
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks 
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted;
    "
    
    echo ""
    echo "TABLE LOCKS:"
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            l.locktype,
            l.database,
            l.relation::regclass as table_name,
            l.pid,
            l.mode,
            l.granted,
            a.query,
            a.query_start
        FROM pg_locks l
        JOIN pg_stat_activity a ON l.pid = a.pid
        WHERE l.relation IS NOT NULL
            AND a.datname = '$DB_NAME'
        ORDER BY a.query_start;
    "
    
    echo ""
    echo "TEMPORARY TABLES (might indicate RF1/RF2 activity):"
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            schemaname,
            tablename,
            n_live_tup as rows
        FROM pg_stat_user_tables
        WHERE schemaname = 'pg_temp_1'
            OR schemaname LIKE 'pg_temp_%'
        ORDER BY schemaname, tablename;
    "
    
    echo ""
    echo "LONG RUNNING QUERIES (>5 seconds):"
    psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            pid,
            now() - query_start as duration,
            state,
            wait_event_type,
            wait_event,
            query
        FROM pg_stat_activity
        WHERE datname = '$DB_NAME'
            AND state != 'idle'
            AND now() - query_start > interval '5 seconds'
            AND pid != pg_backend_pid()
        ORDER BY query_start;
    "
}

# Continuous monitoring
if [[ "${1:-}" == "--once" ]]; then
    show_activity
else
    while true; do
        clear
        show_activity
        sleep 2
    fi
fi

