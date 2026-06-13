#!/bin/bash
# Export TPC-H database to a file that can be transferred

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_NAME="tpch_db"
DB_USER="tpch_user"
DB_PASSWORD="tpch_password_123"
EXPORT_DIR="$SCRIPT_DIR/exports"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
EXPORT_FILE="$EXPORT_DIR/${DB_NAME}_${TIMESTAMP}.sql"
COMPRESSED_FILE="$EXPORT_DIR/${DB_NAME}_${TIMESTAMP}.sql.gz"

echo "TPC-H Database Export Utility"
echo "=============================="
echo ""

# Create export directory
mkdir -p "$EXPORT_DIR"

# Check available space
available_space=$(df -BM "$EXPORT_DIR" | tail -1 | awk '{print $4}' | sed 's/M//')
echo "Available disk space: ${available_space}MB"
echo ""

# Get database size
db_size=$(PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));")
echo "Database size: $db_size"
echo ""

# Export options
echo "Export options:"
echo "  1. Full dump (schema + data) - Recommended"
echo "  2. Schema only (no data) - Fast, small file"
echo "  3. Data only (no schema) - Large file"
echo ""
read -p "Select option [1-3]: " choice

# Set password for pg_dump
export PGPASSWORD="$DB_PASSWORD"

case $choice in
    1)
        echo "Exporting full database (schema + data)..."
        pg_dump -h localhost -U "$DB_USER" -d "$DB_NAME" > "$EXPORT_FILE"
        ;;
    2)
        echo "Exporting schema only..."
        pg_dump -h localhost -U "$DB_USER" -d "$DB_NAME" --schema-only > "$EXPORT_FILE"
        ;;
    3)
        echo "Exporting data only..."
        pg_dump -h localhost -U "$DB_USER" -d "$DB_NAME" --data-only > "$EXPORT_FILE"
        ;;
    *)
        echo "Invalid option. Exiting."
        exit 1
        ;;
esac

# Unset password
unset PGPASSWORD

echo "Export completed: $EXPORT_FILE"

# Get file size
export_size=$(du -h "$EXPORT_FILE" | cut -f1)
echo "Export file size: $export_size"
echo ""

# Ask if user wants to compress
read -p "Compress the file? (recommended) [y/n]: " compress
if [[ "$compress" == "y" || "$compress" == "Y" ]]; then
    echo "Compressing..."
    gzip "$EXPORT_FILE"
    compressed_size=$(du -h "$COMPRESSED_FILE" | cut -f1)
    echo "Compressed file size: $compressed_size"
    echo "Compressed file: $COMPRESSED_FILE"
    FINAL_FILE="$COMPRESSED_FILE"
else
    FINAL_FILE="$EXPORT_FILE"
fi

echo ""
echo "========================================"
echo "Export completed successfully!"
echo "========================================"
echo ""
echo "File location: $FINAL_FILE"
echo ""
echo "To download to your local machine, run this on your LOCAL machine:"
echo "  scp debian@archraspberrypi:$FINAL_FILE ."
echo ""
echo "Or use this command format:"
echo "  scp YOUR_USER@YOUR_SERVER_IP:$FINAL_FILE /path/to/local/directory/"
echo ""
echo "To import on another PostgreSQL server:"
if [[ "$FINAL_FILE" == *.gz ]]; then
    echo "  gunzip -c $(basename $FINAL_FILE) | psql -U postgres -d tpch_db"
else
    echo "  psql -U postgres -d tpch_db < $(basename $FINAL_FILE)"
fi
echo ""
