#!/bin/bash

# TPC-H Database Cleanup Script
# Removes all TPC-H databases, users, data files, and tools
#
# Usage:
#   ./cleanup_tpch.sh              # Normal cleanup (keeps tools)
#   ./cleanup_tpch.sh --full       # Full cleanup (removes everything including tools)
#   ./cleanup_tpch.sh --force      # Force cleanup without prompts

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/cleanup.log"
DB_NAME="tpch_db"
DB_USER="tpch_user"

# Parse command line arguments
FULL_CLEAN=false
FORCE=false
for arg in "$@"; do
    case $arg in
        --full|--complete)
            FULL_CLEAN=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        *)
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"
    exit 1
}

warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"
}

info() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"
}

# Wait for PostgreSQL to be ready
wait_for_postgres() {
    log "Checking PostgreSQL availability..."
    for i in {1..30}; do
        if sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; then
            log "PostgreSQL is available"
            return 0
        fi
        if [ $i -eq 30 ]; then
            error "PostgreSQL is not responding after 30 seconds"
        fi
        sleep 1
    done
}

# Confirm cleanup with user
confirm_cleanup() {
    if [ "$FORCE" = true ]; then
        return 0
    fi
    
    echo
    echo -e "${YELLOW}=== TPC-H CLEANUP WARNING ==="
    echo "This will remove:"
    echo "  - Database: $DB_NAME"
    echo "  - User: $DB_USER"
    echo "  - All TPC-H data files"
    if [ "$FULL_CLEAN" = true ]; then
        echo "  - TPC-H tools directory (tpch-tools/)"
    fi
    echo
    echo -e "This action cannot be undone!${NC}"
    echo
    
    read -p "Are you sure you want to continue? (yes/no): " confirmation
    case $confirmation in
        [Yy]*|[Yy][Ee][Ss]*)
            log "User confirmed cleanup"
            ;;
        *)
            log "Cleanup cancelled by user"
            exit 0
            ;;
    esac
}

# Drop PostgreSQL database and user
cleanup_database() {
    log "Cleaning up PostgreSQL database and user..."
    
    # Ensure PostgreSQL is ready
    wait_for_postgres
    
    # Drop database
    log "Dropping database: $DB_NAME"
    sudo -u postgres psql <<EOF 2>&1 | tee -a "$LOG_FILE" || warning "Database might not exist"
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = '$DB_NAME'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS $DB_NAME;
EOF

    # Drop user
    log "Dropping user: $DB_USER"
    sudo -u postgres psql <<EOF 2>&1 | tee -a "$LOG_FILE" || warning "User might not exist"
DROP USER IF EXISTS $DB_USER;
EOF

    log "PostgreSQL cleanup completed"
}

# Clean up data files and directories
cleanup_files() {
    log "Cleaning up TPC-H files and directories..."
    
    # Remove generated data files
    local tpch_dir="$SCRIPT_DIR/tpch-tools"
    local data_dir="$SCRIPT_DIR/tpch-data"
    local results_dir="$SCRIPT_DIR/query_results"
    local query_dir="$SCRIPT_DIR/tpch_queries"
    
    # Remove .tbl data files (these are large!)
    if [[ -d "$tpch_dir" ]]; then
        log "Removing generated .tbl data files..."
        rm -f "$tpch_dir"/*.tbl 2>/dev/null || true
        rm -f "$tpch_dir"/*.tbl.* 2>/dev/null || true
        log "Data files removed from $tpch_dir"
    fi
    
    # Remove data directory
    if [[ -d "$data_dir" ]]; then
        log "Removing directory: $data_dir"
        rm -rf "$data_dir"
        log "Data directory removed"
    fi
    
    # Remove query results directory
    if [[ -d "$results_dir" ]]; then
        log "Removing directory: $results_dir"
        rm -rf "$results_dir"
        log "Results directory removed"
    fi
    
    # Remove generated queries directory
    if [[ -d "$query_dir" ]]; then
        log "Removing directory: $query_dir"
        rm -rf "$query_dir"
        log "Queries directory removed"
    fi
    
    # Remove CSV output files
    log "Removing CSV output files..."
    rm -f "$SCRIPT_DIR"/tpch_*.csv 2>/dev/null || true
    rm -f "$SCRIPT_DIR"/query_execution.log 2>/dev/null || true
    
    # Full cleanup: remove tools directory
    if [ "$FULL_CLEAN" = true ]; then
        if [[ -d "$tpch_dir" ]]; then
            log "Full cleanup: removing TPC-H tools directory..."
            rm -rf "$tpch_dir"
            log "TPC-H tools directory removed"
        fi
    fi
    
    log "File cleanup completed"
}

# Clean up any running PostgreSQL connections
cleanup_connections() {
    log "Cleaning up any stale database connections..."
    
    sudo -u postgres psql <<EOF 2>&1 | tee -a "$LOG_FILE" || true
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = '$DB_NAME'
  AND pid <> pg_backend_pid();
EOF

    log "Connection cleanup completed"
}

# Remove log files
cleanup_logs() {
    log "Cleaning up log files..."
    
    local logs=(
        "$SCRIPT_DIR/installation.log"
        "$SCRIPT_DIR/cleanup.log"
        "$SCRIPT_DIR/query_execution.log"
    )
    
    for log_file in "${logs[@]}"; do
        if [[ -f "$log_file" ]]; then
            rm -f "$log_file"
            log "Removed: $log_file"
        fi
    done
    
    log "Log cleanup completed"
}

# Show disk space savings
show_disk_space() {
    log "Calculating disk space freed..."
    
    # Calculate space used by TPC-H directories
    local total_freed=0
    
    # Check each directory and calculate size
    local dirs=(
        "$SCRIPT_DIR/tpch-tools"
        "$SCRIPT_DIR/tpch-data" 
        "$SCRIPT_DIR/query_results"
        "$SCRIPT_DIR/tpch_queries"
    )
    
    for dir in "${dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            local size=$(du -sb "$dir" 2>/dev/null | cut -f1 || echo 0)
            total_freed=$((total_freed + size))
            log "Directory $dir: $(numfmt --to=iec $size)"
        fi
    done
    
    # Check .tbl files separately
    if [[ -d "$SCRIPT_DIR/tpch-tools" ]]; then
        local tbl_size=$(find "$SCRIPT_DIR/tpch-tools" -name "*.tbl" -type f -exec du -sb {} + 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
        total_freed=$((total_freed + tbl_size))
        if [ $tbl_size -gt 0 ]; then
            log "Data files: $(numfmt --to=iec $tbl_size)"
        fi
    fi
    
    if [ $total_freed -gt 0 ]; then
        log "Total disk space freed: $(numfmt --to=iec $total_freed)"
    else
        log "No significant disk space was occupied by TPC-H files"
    fi
}

# Verify cleanup was successful
verify_cleanup() {
    log "Verifying cleanup..."
    
    local errors=0
    
    # Check if database still exists
    if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
        warning "Database $DB_NAME still exists"
        errors=$((errors + 1))
    else
        log "✓ Database $DB_NAME successfully removed"
    fi
    
    # Check if user still exists
    if sudo -u postgres psql -t -c "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER';" | grep -q 1; then
        warning "User $DB_USER still exists"
        errors=$((errors + 1))
    else
        log "✓ User $DB_USER successfully removed"
    fi
    
    # Check if data directories still exist
    local dirs=(
        "$SCRIPT_DIR/tpch-data"
        "$SCRIPT_DIR/query_results"
        "$SCRIPT_DIR/tpch_queries"
    )
    
    for dir in "${dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            warning "Directory still exists: $dir"
            errors=$((errors + 1))
        else
            log "✓ Directory removed: $dir"
        fi
    done
    
    # Check if .tbl files still exist
    if find "$SCRIPT_DIR" -name "*.tbl" -type f | grep -q .; then
        warning "Some .tbl data files still exist"
        errors=$((errors + 1))
    else
        log "✓ All .tbl data files removed"
    fi
    
    if [ $errors -eq 0 ]; then
        log "✓ Cleanup verification passed - all components removed successfully"
    else
        warning "Cleanup verification found $errors issues that may need manual attention"
    fi
}

# Main execution function
main() {
    log "Starting TPC-H cleanup process..."
    log "Mode: $([ "$FULL_CLEAN" = true ] && echo "FULL" || echo "NORMAL")"
    log "Force: $([ "$FORCE" = true ] && echo "YES" || echo "NO")"
    
    # Show what will be cleaned up
    echo
    info "This cleanup will remove:"
    info "  - PostgreSQL database: $DB_NAME"
    info "  - PostgreSQL user: $DB_USER"
    info "  - All TPC-H data files (.tbl)"
    info "  - Query results and logs"
    if [ "$FULL_CLEAN" = true ]; then
        info "  - TPC-H tools directory (complete removal)"
    fi
    echo
    
    # Get confirmation
    confirm_cleanup
    
    # Show disk space before cleanup
    log "Calculating disk space usage before cleanup..."
    show_disk_space
    
    # Perform cleanup steps
    cleanup_connections
    cleanup_database
    cleanup_files
    cleanup_logs
    
    # Verify cleanup
    verify_cleanup
    
    # Show disk space savings
    log "Cleanup completed successfully!"
    log "You can now re-run the setup script to create a fresh TPC-H database."
    
    if [ "$FULL_CLEAN" = true ]; then
        log "Note: TPC-H tools were completely removed and will be re-downloaded on next setup."
    else
        log "Note: TPC-H tools were preserved for future use."
    fi
}

# Handle script interruption
cleanup_on_exit() {
    echo
    warning "Script interrupted - cleanup may be incomplete"
    exit 1
}

# Set up signal handlers
trap cleanup_on_exit INT TERM

# Initialize script
main "$@"