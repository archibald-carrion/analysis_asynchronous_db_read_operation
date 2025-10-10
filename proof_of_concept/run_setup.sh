#!/bin/bash

# TPC-H Database Creation Script
# Creates a 40GB TPC-H compliant PostgreSQL database

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/installation.log"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
SCALE_FACTOR=1
PG_VERSION=18

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

# Check if running as debian user
check_user() {
    if [[ "$(whoami)" != "debian" ]]; then
        error "This script must be run as debian user"
    fi
}

# Main execution function
main() {
    log "Starting TPC-H database creation process..."
    log "Scale Factor: ${SCALE_FACTOR}GB"
    log "PostgreSQL Version: ${PG_VERSION}"
    
    # Step 1: Install dependencies and PostgreSQL
    log "Step 1: Installing dependencies and PostgreSQL ${PG_VERSION}"
    source "$SCRIPT_DIR/install_dependencies.sh"
    
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
    sudo -u postgres psql -c "CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}' CREATEDB;" || warning "User may already exist"
    
    log "Creating database: ${DB_NAME}"
    sudo -u postgres psql -c "CREATE DATABASE ${DB_NAME} WITH OWNER = ${DB_USER} ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8' TEMPLATE = template0;" || warning "Database may already exist"
    
    log "Granting permissions"
    sudo -u postgres psql -d ${DB_NAME} -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};"
    sudo -u postgres psql -d ${DB_NAME} -c "ALTER USER ${DB_USER} WITH SUPERUSER;"
}

# TPC-H tools setup
setup_tpch_tools() {
    local tpch_dir="$SCRIPT_DIR/tpch-tools"
    
    if [[ ! -d "$tpch_dir" ]]; then
        log "Downloading TPC-H tools..."
        mkdir -p "$tpch_dir"
        cd "$tpch_dir"
        
        # Download TPC-H toolkit (using a mirror - replace with official if available)
        wget -q http://tpc.org/tpc_documents_current_versions/current_specifications5.asp || \
        wget -q https://github.com/electrum/tpch-dbgen/archive/master.zip -O tpch-dbgen.zip
        
        if [[ -f "tpch-dbgen.zip" ]]; then
            unzip -q tpch-dbgen.zip
            mv tpch-dbgen-master/* .
            rm -rf tpch-dbgen-master tpch-dbgen.zip
        else
            # Alternative: clone from GitHub
            git clone -q https://github.com/electrum/tpch-dbgen.git .
        fi
        
        # Build dbgen
        log "Building TPC-H dbgen tool..."
        make -j$(nproc) 1>/dev/null 2>>"$LOG_FILE"
        
        if [[ ! -f "dbgen" ]]; then
            error "Failed to build dbgen tool"
        fi
    else
        log "TPC-H tools already exist, skipping download"
    fi
}

# Data generation and loading
generate_and_load_data() {
    local tpch_dir="$SCRIPT_DIR/tpch-tools"
    local data_dir="$SCRIPT_DIR/tpch-data"
    
    mkdir -p "$data_dir"
    cd "$tpch_dir"
    
    log "Generating TPC-H data with scale factor ${SCALE_FACTOR}..."
    ./dbgen -s "$SCALE_FACTOR" -f 1>>"$LOG_FILE" 2>&1
    
    # Create table schema in PostgreSQL
    log "Creating TPC-H schema..."
    sudo -u postgres psql -d "$DB_NAME" -U "$DB_USER" <<EOF
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
        c_name character(25) NOT NULL,
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

    # Load data using PostgreSQL COPY command
    log "Loading data into PostgreSQL (this will take a long time for 40GB)..."
    
    # Set up password for psql
    export PGPASSWORD="$DB_PASSWORD"
    
    # Load each table
    for table in nation region part supplier partsupp customer orders lineitem; do
        log "Loading table: $table"
        data_file="$table.tbl"
        
        if [[ "$table" == "region" || "$table" == "nation" ]]; then
            # Small tables, load directly
            psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "\COPY $table FROM '$data_file' WITH DELIMITER '|' CSV;" 1>>"$LOG_FILE" 2>&1
        else
            # Large tables, show progress
            log "Loading large table $table..."
            psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "\COPY $table FROM '$data_file' WITH DELIMITER '|' CSV;" 1>>"$LOG_FILE" 2>&1 &
            PID=$!
            
            # Show progress for large tables
            while kill -0 $PID 2>/dev/null; do
                echo -n "."
                sleep 30
            done
            echo ""
        fi
    done
    
    log "Data loading completed"
}

# Create indexes and constraints
create_indexes() {
    log "Creating primary keys and indexes..."
    
    sudo -u postgres psql -d "$DB_NAME" -U "$DB_USER" <<EOF 1>>"$LOG_FILE" 2>&1
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
check_user
main "$@"