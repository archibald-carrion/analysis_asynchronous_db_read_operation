#!/bin/bash

# TPC-H Database Creation Script
# Creates a TPC-H compliant PostgreSQL database
#
# Usage:
#   ./run_test.sh              # Normal mode (auto-detects memory)
#   ./run_test.sh --low-memory # Force low-memory mode for Raspberry Pi
#   ./run_test.sh --clean      # Clean up all data and tools before running
#   ./run_test.sh --low-memory --clean  # Combine flags

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/installation.log"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
SCALE_FACTOR=1
PG_VERSION=18

# Parse command line arguments
LOW_MEMORY=false
CLEAN=false
for arg in "$@"; do
    case $arg in
        --low-memory)
            LOW_MEMORY=true
            shift
            ;;
        --clean)
            CLEAN=true
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

# Clean up function
cleanup_files() {
    log "Cleaning up old TPC-H files and data..."
    
    # Remove generated data files (.tbl files are large!)
    if [[ -d "$SCRIPT_DIR/tpch-tools" ]]; then
        log "Removing generated .tbl data files..."
        rm -f "$SCRIPT_DIR/tpch-tools"/*.tbl
        rm -f "$SCRIPT_DIR/tpch-tools"/*.tbl.*
        
        # Show space saved
        log "Data files cleaned"
    fi
    
    # Remove data directory if it exists
    if [[ -d "$SCRIPT_DIR/tpch-data" ]]; then
        log "Removing tpch-data directory..."
        rm -rf "$SCRIPT_DIR/tpch-data"
    fi
    
    # Optionally remove entire tpch-tools directory (including source and binaries)
    if [[ "$CLEAN" == "true" ]]; then
        log "Full clean requested - removing tpch-tools directory..."
        rm -rf "$SCRIPT_DIR/tpch-tools"
        log "TPC-H tools removed (will be re-downloaded)"
    fi
    
    log "Cleanup completed"
}

# Main execution function
main() {
    log "Starting TPC-H database creation process..."
    log "Scale Factor: ${SCALE_FACTOR}GB"
    log "PostgreSQL Version: ${PG_VERSION}"
    
    if [ "$LOW_MEMORY" = true ]; then
        log "========================================="
        log "RUNNING IN LOW MEMORY MODE"
        log "Suitable for Raspberry Pi and systems <2GB RAM"
        log "========================================="
    fi
    
    # Clean up old files first if requested
    if [ "$CLEAN" = true ]; then
        log "========================================="
        log "CLEAN MODE: Removing old files"
        log "========================================="
        cleanup_files
    else
        # Always clean .tbl files to save space (they'll be regenerated)
        log "Cleaning up old .tbl data files to save space..."
        rm -f "$SCRIPT_DIR/tpch-tools"/*.tbl 2>/dev/null || true
        rm -f "$SCRIPT_DIR/tpch-tools"/*.tbl.* 2>/dev/null || true
    fi
    
    # Step 1: Install dependencies and PostgreSQL
    log "Step 1: Installing dependencies and PostgreSQL ${PG_VERSION}"
    if [[ -f "$SCRIPT_DIR/install_dependencies.sh" ]]; then
        if [ "$LOW_MEMORY" = true ]; then
            source "$SCRIPT_DIR/install_dependencies.sh" --low-memory
        else
            source "$SCRIPT_DIR/install_dependencies.sh"
        fi
    else
        warning "install_dependencies.sh not found, assuming dependencies are installed"
    fi
    
    # Ensure PostgreSQL is ready
    wait_for_postgres
    
    # Step 2: Create database and user
    log "Step 2: Setting up database and user"
    setup_database
    
    # Step 3: Download and build TPC-H tools
    log "Step 3: Setting up TPC-H tools"
    setup_tpch_tools
    
    # Step 4: Generate and load data
    log "Step 4: Generating and loading TPC-H data (this will take a while...)"
    generate_and_load_data
    
    # Step 5: Create indexes and constraints
    log "Step 5: Creating indexes and constraints"
    create_indexes
    
    log "TPC-H database creation completed successfully!"
    log "Database: ${DB_NAME}"
    log "User: ${DB_USER}"
    log "Scale Factor: ${SCALE_FACTOR}GB"
    log "Check $LOG_FILE for detailed logs"
}

# Database setup function
setup_database() {
    log "Creating database user: ${DB_USER}"
    
    # Create user
    sudo -u postgres psql <<EOF 2>&1 | tee -a "$LOG_FILE"
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = '${DB_USER}') THEN
        CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}' CREATEDB SUPERUSER;
    ELSE
        ALTER USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}' CREATEDB SUPERUSER;
    END IF;
END
\$\$;
EOF
    
    log "Creating database: ${DB_NAME}"
    
    # Get available locales
    available_locale=$(locale -a | grep -E "^(en_US\.utf8|en_US\.UTF-8|C\.UTF-8|C\.utf8)" | head -1)
    
    if [ -z "$available_locale" ]; then
        log "No UTF-8 locale found, using C locale"
        available_locale="C"
    else
        log "Using locale: $available_locale"
    fi
    
    # Drop and recreate database with available locale
    sudo -u postgres psql <<EOF 2>&1 | tee -a "$LOG_FILE"
DROP DATABASE IF EXISTS ${DB_NAME};
CREATE DATABASE ${DB_NAME} WITH OWNER = ${DB_USER} ENCODING = 'UTF8' LC_COLLATE = '${available_locale}' LC_CTYPE = '${available_locale}' TEMPLATE = template0;
EOF
    
    log "Granting permissions"
    sudo -u postgres psql -d ${DB_NAME} <<EOF 2>&1 | tee -a "$LOG_FILE"
GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};
EOF
}

# TPC-H tools setup
setup_tpch_tools() {
    local tpch_dir="$SCRIPT_DIR/tpch-tools"
    
    if [[ ! -d "$tpch_dir" ]]; then
        log "Downloading TPC-H tools..."
        mkdir -p "$tpch_dir"
        cd "$tpch_dir"
        
        # Clone from GitHub
        git clone https://github.com/electrum/tpch-dbgen.git .
        
        # Fix the build issue - add missing include
        log "Patching TPC-H dbgen for modern compilers..."
        sed -i '1i#include <string.h>' bm_utils.c
        
        # Build dbgen
        log "Building TPC-H dbgen tool..."
        make -j$(nproc) 2>&1 | tee -a "$LOG_FILE"
        
        if [[ ! -f "dbgen" ]]; then
            error "Failed to build dbgen tool"
        fi
        
        log "TPC-H tools built successfully"
    else
        log "TPC-H tools already exist, skipping download"
        cd "$tpch_dir"
        if [[ ! -f "dbgen" ]]; then
            log "dbgen not found, rebuilding..."
            sed -i '1i#include <string.h>' bm_utils.c 2>/dev/null || true
            make clean
            make -j$(nproc) 2>&1 | tee -a "$LOG_FILE"
        fi
    fi
}

# Data generation and loading
generate_and_load_data() {
    local tpch_dir="$SCRIPT_DIR/tpch-tools"
    local data_dir="$SCRIPT_DIR/tpch-data"
    
    mkdir -p "$data_dir"
    cd "$tpch_dir"
    
    log "Generating TPC-H data with scale factor ${SCALE_FACTOR}..."
    ./dbgen -s "$SCALE_FACTOR" -f 2>&1 | tee -a "$LOG_FILE"
    
    # Verify data files were generated
    if [[ ! -f "nation.tbl" ]]; then
        error "Data generation failed - nation.tbl not found"
    fi
    
    # Create table schema in PostgreSQL
    log "Creating TPC-H schema..."
    PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" <<EOF 2>&1 | tee -a "$LOG_FILE"
-- TPC-H Schema
CREATE TABLE nation (
    n_nationkey integer NOT NULL,
    n_name character(25) NOT NULL,
    n_regionkey integer NOT NULL,
    n_comment character varying(152)
);

CREATE TABLE region (
    r_regionkey integer NOT NULL,
    r_name character(25) NOT NULL,
    r_comment character varying(152)
);

CREATE TABLE part (
    p_partkey integer NOT NULL,
    p_name character varying(55) NOT NULL,
    p_mfgr character(25) NOT NULL,
    p_brand character(10) NOT NULL,
    p_type character varying(25) NOT NULL,
    p_size integer NOT NULL,
    p_container character(10) NOT NULL,
    p_retailprice numeric(15,2) NOT NULL,
    p_comment character varying(23) NOT NULL
);

CREATE TABLE supplier (
    s_suppkey integer NOT NULL,
    s_name character(25) NOT NULL,
    s_address character varying(40) NOT NULL,
    s_nationkey integer NOT NULL,
    s_phone character(15) NOT NULL,
    s_acctbal numeric(15,2) NOT NULL,
    s_comment character varying(101) NOT NULL
);

CREATE TABLE partsupp (
    ps_partkey integer NOT NULL,
    ps_suppkey integer NOT NULL,
    ps_availqty integer NOT NULL,
    ps_supplycost numeric(15,2) NOT NULL,
    ps_comment character varying(199) NOT NULL
);

CREATE TABLE customer (
    c_custkey integer NOT NULL,
    c_name character varying(25) NOT NULL,
    c_address character varying(40) NOT NULL,
    c_nationkey integer NOT NULL,
    c_phone character(15) NOT NULL,
    c_acctbal numeric(15,2) NOT NULL,
    c_mktsegment character(10) NOT NULL,
    c_comment character varying(117) NOT NULL
);

CREATE TABLE orders (
    o_orderkey integer NOT NULL,
    o_custkey integer NOT NULL,
    o_orderstatus character(1) NOT NULL,
    o_totalprice numeric(15,2) NOT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority character(15) NOT NULL,
    o_clerk character(15) NOT NULL,
    o_shippriority integer NOT NULL,
    o_comment character varying(79) NOT NULL
);

CREATE TABLE lineitem (
    l_orderkey integer NOT NULL,
    l_partkey integer NOT NULL,
    l_suppkey integer NOT NULL,
    l_linenumber integer NOT NULL,
    l_quantity numeric(15,2) NOT NULL,
    l_extendedprice numeric(15,2) NOT NULL,
    l_discount numeric(15,2) NOT NULL,
    l_tax numeric(15,2) NOT NULL,
    l_returnflag character(1) NOT NULL,
    l_linestatus character(1) NOT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date NOT NULL,
    l_receiptdate date NOT NULL,
    l_shipinstruct character(25) NOT NULL,
    l_shipmode character(10) NOT NULL,
    l_comment character varying(44) NOT NULL
);
EOF

    # Load data using PostgreSQL \copy (client-side COPY)
    log "Loading data into PostgreSQL..."
    
    # Set up password for psql
    export PGPASSWORD="$DB_PASSWORD"
    
    # Fix file permissions first
    chmod 644 *.tbl 2>/dev/null || true
    
    # Pre-process .tbl files to remove trailing delimiters
    log "Pre-processing .tbl files to remove trailing delimiters..."
    for tbl_file in *.tbl; do
        if [[ -f "$tbl_file" ]]; then
            # Remove trailing pipe character from each line
            sed -i 's/|$//' "$tbl_file"
            log "Processed: $tbl_file"
        fi
    done
    
    # Load each table using \copy which runs client-side
    for table in nation region part supplier partsupp customer orders lineitem; do
        log "Loading table: $table"
        data_file="${tpch_dir}/${table}.tbl"
        
        if [[ ! -f "$data_file" ]]; then
            error "Data file not found: $data_file"
        fi
        
        # Use \copy with proper TPC-H format (pipe delimiter, no header)
        PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" <<EOF 2>&1 | tee -a "$LOG_FILE"
\\copy $table FROM '$data_file' WITH (DELIMITER '|', FORMAT csv);
EOF
        
        # Check if the copy succeeded by counting rows
        row_count=$(PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM $table;" | tr -d ' ')
        
        if [ "$row_count" -gt 0 ]; then
            log "Successfully loaded table: $table ($row_count rows)"
        else
            error "Failed to load data into table: $table (0 rows loaded)"
        fi
    done
    
    log "Data loading completed"
}

# Create indexes and constraints
create_indexes() {
    log "Creating primary keys and indexes..."
    
    PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" <<EOF 2>&1 | tee -a "$LOG_FILE"
-- Primary Keys
ALTER TABLE nation ADD PRIMARY KEY (n_nationkey);
ALTER TABLE region ADD PRIMARY KEY (r_regionkey);
ALTER TABLE part ADD PRIMARY KEY (p_partkey);
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber);

-- Foreign Keys
ALTER TABLE nation ADD FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey);
ALTER TABLE supplier ADD FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE partsupp ADD FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey);
ALTER TABLE partsupp ADD FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey);
ALTER TABLE customer ADD FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey);

-- Performance Indexes
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);

-- Analyze tables for query planner
ANALYZE;
EOF
    
    log "Indexes and constraints created"
}

# Initialize script
main "$@"