#!/bin/bash
# toggle_pg_mode.sh - Switch between sync, bgworkers, and iouring modes

CONF_FILE="/etc/postgresql/18/main/postgresql.conf"
BACKUP_FILE="${CONF_FILE}.original_backup"
MODES=("sync" "bgworkers" "iouring")

show_usage() {
    echo "Usage: $0 [mode]"
    echo "Modes:"
    echo "  sync       - Synchronous baseline"
    echo "  bgworkers  - Parallel background workers"
    echo "  iouring    - Async I/O with io_uring"
    echo ""
    echo "If no mode specified, cycles to next mode."
    exit 1
}

get_current_mode() {
    if grep -q "^max_worker_processes = 8" "$CONF_FILE" && ! grep -q "^io_uring_workers" "$CONF_FILE"; then
        echo "bgworkers"
    elif grep -q "^io_uring_workers = 4" "$CONF_FILE"; then
        echo "iouring"
    else
        echo "sync"
    fi
}

get_next_mode() {
    local current=$1
    case $current in
        "sync") echo "bgworkers" ;;
        "bgworkers") echo "iouring" ;;
        "iouring") echo "sync" ;;
        *) echo "sync" ;;
    esac
}

apply_mode() {
    local mode=$1
    
    # Restore from backup
    if [[ ! -f "$BACKUP_FILE" ]]; then
        echo "❌ Backup file not found. Run configure_pg_modes.sh first."
        exit 1
    fi
    
    echo "Applying $mode mode..."
    cp "$BACKUP_FILE" "$CONF_FILE"
    
    # Append mode configuration
    case $mode in
        "sync")
            echo "# Synchronous mode - baseline" >> "$CONF_FILE"
            ;;
        "bgworkers")
            cat "/etc/postgresql/18/main/modes/bgworkers.conf" >> "$CONF_FILE"
            ;;
        "iouring")
            cat "/etc/postgresql/18/main/modes/iouring.conf" >> "$CONF_FILE"
            ;;
        *)
            echo "❌ Unknown mode: $mode"
            exit 1
            ;;
    esac
}

wait_for_postgres() {
    local timeout=${1:-180}
    local waited=0
    local step=3

    while ! sudo -u postgres psql -Atqc "SELECT 1;" >/dev/null 2>&1; do
        sleep "$step"
        waited=$((waited + step))

        if [[ $waited -ge $timeout ]]; then
            echo "❌ PostgreSQL did not become ready within ${timeout}s"
            echo "Last service status:"
            systemctl status postgresql --no-pager | tail -n 20
            return 1
        fi
    done

    return 0
}

# Main execution
main() {
    local target_mode=$1
    
    # Check root
    if [[ $EUID -ne 0 ]]; then
        echo "❌ This script must be run as root"
        exit 1
    fi
    
    # Determine target mode
    local current_mode=$(get_current_mode)
    
    if [[ -n "$target_mode" ]]; then
        # Specific mode requested
        if [[ " ${MODES[@]} " =~ " ${target_mode} " ]]; then
            if [[ "$current_mode" == "$target_mode" ]]; then
                echo "✅ Already in $target_mode mode"
                return 0
            fi
            apply_mode "$target_mode"
        else
            echo "❌ Invalid mode: $target_mode"
            show_usage
        fi
    else
        # Cycle to next mode
        local next_mode=$(get_next_mode "$current_mode")
        echo "Cycling from $current_mode to $next_mode..."
        apply_mode "$next_mode"
        target_mode=$next_mode
    fi
    
    # Restart PostgreSQL
    echo "Restarting PostgreSQL..."
    systemctl restart postgresql
    echo "Waiting for PostgreSQL to accept connections..."

    if ! wait_for_postgres 180; then
        exit 1
    fi

    echo "✅ PostgreSQL restarted in $target_mode mode"
    
    # Show configuration summary
    echo ""
    echo "=== Configuration Summary ==="
    case $target_mode in
        "sync")
            echo "MODE: SYNC (Baseline)"
            echo "  - Single-threaded execution"
            echo "  - No parallel workers"
            echo "  - Conservative resource usage"
            ;;
        "bgworkers")
            echo "MODE: BACKGROUND WORKERS"
            echo "  - Parallel query execution"
            echo "  - 8 max worker processes"
            echo "  - 4 parallel workers per gather"
            ;;
        "iouring")
            echo "MODE: IO_URING"
            echo "  - Async I/O operations"
            echo "  - 4 io_uring workers"
            echo "  - Optimized for high I/O throughput"
            ;;
    esac
    
    echo ""
    echo "Key settings:"
    grep -E "^(max_worker_processes|max_parallel_workers|io_uring_workers)" "$CONF_FILE" 2>/dev/null || \
    echo "  (Using default synchronous settings)"
}

# Handle help
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_usage
fi

main "$@"
