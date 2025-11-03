#!/bin/bash
# check_pg_mode.sh - Check current PostgreSQL mode

CONF_FILE="/etc/postgresql/18/main/postgresql.conf"

get_current_mode() {
    if grep -q "^io_uring_workers = 4" "$CONF_FILE" 2>/dev/null; then
        echo "iouring"
    elif grep -q "^max_worker_processes = 8" "$CONF_FILE" 2>/dev/null; then
        echo "bgworkers"
    else
        echo "sync"
    fi
}

show_mode_info() {
    local mode=$1
    
    echo "=== PostgreSQL Current Mode ==="
    
    case $mode in
        "sync")
            echo "✅ MODE: SYNC (Baseline)"
            echo "   - Single-threaded execution"
            echo "   - No parallel workers"
            echo "   - Conservative resource usage"
            ;;
        "bgworkers")
            echo "✅ MODE: BACKGROUND WORKERS"
            echo "   - Parallel query execution"
            echo "   - Multiple worker processes"
            echo "   - Optimized for CPU parallelism"
            ;;
        "iouring")
            echo "✅ MODE: IO_URING"
            echo "   - Async I/O operations"
            echo "   - Optimized for storage throughput"
            echo "   - Reduced I/O latency"
            ;;
    esac
    
    echo ""
    echo "Key Settings:"
    grep -E "^(max_worker_processes|max_parallel_workers|io_uring_workers)" "$CONF_FILE" 2>/dev/null || \
    echo "  (Default synchronous settings)"
}

main() {
    local mode=$(get_current_mode)
    show_mode_info "$mode"
}

main "$@"