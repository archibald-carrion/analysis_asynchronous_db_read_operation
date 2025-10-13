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
create_mode_configs() {
    local conf_dir="/etc/postgresql/18/main/modes"
    mkdir -p "$conf_dir"
    
    # Synchronous mode (baseline)
    cat > "$conf_dir/sync.conf" << 'EOF'
# Synchronous Mode - Baseline
# max_worker_processes = 0                   # Disabled
# max_parallel_workers_per_gather = 0        # Disabled  
# max_parallel_workers = 0                   # Disabled
# io_uring_workers = 0                       # Disabled

# Conservative settings
shared_buffers = 128MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB
EOF

    # Background Workers mode
    cat > "$conf_dir/bgworkers.conf" << 'EOF'
# Background Workers Mode - Parallel Queries
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
max_parallel_maintenance_workers = 2

# Optimized for parallelism
shared_buffers = 1GB
effective_cache_size = 2GB
work_mem = 32MB
maintenance_work_mem = 256MB
effective_io_concurrency = 2
parallel_setup_cost = 10.0
parallel_tuple_cost = 0.001

# Disable io_uring for clean comparison
# io_uring_workers = 0
EOF

    # io_uring mode
    cat > "$conf_dir/iouring.conf" << 'EOF'
# io_uring Mode - Async I/O
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8

# io_uring specific settings (PostgreSQL 18+)
io_uring_workers = 4
io_uring_ring_entries = 1024
# io_uring_sqpoll = on
# io_uring_sqpoll_cpu = 0

# Optimized for async I/O
shared_buffers = 1GB
effective_cache_size = 2GB
work_mem = 32MB
maintenance_work_mem = 256MB
effective_io_concurrency = 4
random_page_cost = 1.1

# I/O optimizations
wal_compression = on
max_wal_size = 2GB
min_wal_size = 1GB
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
            # Keep default settings (commented out parallel workers)
            echo "# Synchronous mode - no parallel workers" >> "$CONF_FILE"
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
    echo "Use: ./toggle_pg_mode.sh [sync|bgworkers|iouring]"
    echo "Or:  ./toggle_pg_mode.sh (to cycle through modes)"
}

main "$@"