#!/bin/bash
# configure_pg_modes.sh - Setup all three PostgreSQL modes

CONF_FILE="/etc/postgresql/18/main/postgresql.conf"
BACKUP_FILE="${CONF_FILE}.original_backup"

echo "=== PostgreSQL Multi-Mode Configuration Setup ==="

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    echo "❌ This script must be run as root"
    exit 1
fi

# Backup original config
if [[ ! -f "$BACKUP_FILE" ]]; then
    echo "Creating backup of original configuration..."
    cp "$CONF_FILE" "$BACKUP_FILE"
    echo "✓ Backup created: $BACKUP_FILE"
fi

# Create mode configurations
#
# SINGLE-FACTOR DESIGN: the ONLY setting that varies across the three modes is
# `io_method`. Everything else (shared_buffers, work_mem, parallel workers, WAL,
# planner costs, effective_io_concurrency, ...) is left at PostgreSQL's stock
# defaults from the backed-up postgresql.conf, identical for all three modes.
# This keeps the experiment a clean comparison of io_method = sync vs worker vs
# io_uring; any other tuning here would confound the result.
#
#   sync.conf      -> io_method = sync     (synchronous, no async I/O)
#   bgworkers.conf -> io_method = worker   (async I/O via background I/O workers)
#   iouring.conf   -> io_method = io_uring (async I/O via Linux io_uring)
create_mode_configs() {
    local conf_dir="/etc/postgresql/18/main/modes"
    mkdir -p "$conf_dir"

    # Synchronous mode (baseline): no async I/O.
    cat > "$conf_dir/sync.conf" << 'EOF'
# Sync Mode - synchronous I/O (baseline)
# Only io_method differs from the other modes; all else stays at conf defaults.
io_method = 'sync'
EOF

    # Background Workers mode: async I/O serviced by background I/O worker procs.
    cat > "$conf_dir/bgworkers.conf" << 'EOF'
# Background Workers Mode - async I/O via background I/O workers
# Only io_method differs from the other modes; all else stays at conf defaults.
io_method = 'worker'
EOF

    # io_uring mode: async I/O via Linux io_uring (PostgreSQL must be built with
    # --with-liburing; PGDG packages are).
    cat > "$conf_dir/iouring.conf" << 'EOF'
# io_uring Mode - async I/O via Linux io_uring
# Only io_method differs from the other modes; all else stays at conf defaults.
io_method = 'io_uring'
EOF

    echo "✓ Mode configurations created in $conf_dir"
}

# Apply a specific mode
apply_mode() {
    local mode=$1
    
    echo "Applying $mode mode..."
    
    # Restore from backup first
    cp "$BACKUP_FILE" "$CONF_FILE"
    
    # Append mode-specific configuration
    case $mode in
        "sync")
            cat "/etc/postgresql/18/main/modes/sync.conf" >> "$CONF_FILE"
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
    
    echo "✓ $mode configuration applied"
}

# Check io_uring support
check_iouring_support() {
    echo "Checking io_uring support..."
    
    # Check kernel version
    local kernel_version=$(uname -r)
    local major=$(echo $kernel_version | cut -d. -f1)
    local minor=$(echo $kernel_version | cut -d. -f2)
    
    if [[ $major -lt 5 ]] || ([[ $major -eq 5 ]] && [[ $minor -lt 1 ]]); then
        echo "⚠️  Kernel $kernel_version - io_uring requires 5.1+ (performance may be limited)"
        return 1
    fi
    
    # Check PostgreSQL version for io_uring support
    local pg_version=$(psql -t -c "SHOW server_version_num;" 2>/dev/null | tr -d ' ')
    if [[ $pg_version -lt 180000 ]]; then
        echo "⚠️  PostgreSQL < 18 - io_uring support may be limited"
        return 1
    fi
    
    echo "✓ io_uring supported"
    return 0
}

# Main setup
main() {
    echo "Setting up all three PostgreSQL modes..."
    
    # Create mode configurations
    create_mode_configs
    
    # Check io_uring support
    check_iouring_support
    
    # Apply sync mode by default
    apply_mode "sync"
    
    # Restart to apply
    echo ""
    echo "Restarting PostgreSQL..."
    systemctl restart postgresql
    sleep 5
    
    if pg_isready >/dev/null 2>&1; then
        echo "✓ PostgreSQL is running in SYNC mode"
    else
        echo "❌ PostgreSQL failed to start"
        exit 1
    fi
    
    echo ""
    echo "=== Setup Complete ==="
    echo "Three modes are now available:"
    echo "1. sync       - Synchronous baseline"
    echo "2. bgworkers  - Parallel background workers" 
    echo "3. iouring    - Async I/O with io_uring"
    echo ""
    echo "Use: ./toggle_pg_config.sh [sync|bgworkers|iouring]"
}

main "$@"
