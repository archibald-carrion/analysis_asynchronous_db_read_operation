#!/usr/bin/env bash
# Fix LIMIT -1 in TPC-H queries
# TPC-H :n -1 directive means "no limit", but qgen sometimes generates "LIMIT -1"
# PostgreSQL doesn't accept LIMIT -1, so we remove it

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERIES_DIR="$SCRIPT_DIR/tpch_queries"

if [[ ! -d "$QUERIES_DIR" ]]; then
    echo "Error: $QUERIES_DIR not found"
    exit 1
fi

echo "Fixing LIMIT -1 in TPC-H queries..."

fixed_count=0
for query_file in "$QUERIES_DIR"/q*.sql; do
    if [[ ! -f "$query_file" ]]; then
        continue
    fi
    
    # Check if file has LIMIT -1 (case-insensitive)
    if grep -qi "limit.*-1" "$query_file"; then
        echo "  Fixing: $(basename "$query_file")"
        
        # Use extended regex (-E) and case-insensitive pattern to match both LIMIT and limit
        # First: Remove standalone LIMIT -1 lines (including semicolon if present)
        sed -i -E '/^[[:space:]]*[Ll][Ii][Mm][Ii][Tt][[:space:]]+-1[[:space:]]*;?[[:space:]]*$/d' "$query_file"
        # Second: Replace LIMIT -1 at end of line (with optional semicolon) with just semicolon
        sed -i -E 's/[[:space:]]*[Ll][Ii][Mm][Ii][Tt][[:space:]]+-1[[:space:]]*;?[[:space:]]*$/;/' "$query_file"
        # Third: Remove any remaining LIMIT -1 in middle of line
        sed -i -E 's/[[:space:]]*[Ll][Ii][Mm][Ii][Tt][[:space:]]+-1[[:space:]]*//' "$query_file"
        
        ((fixed_count++))
    fi
done

if [[ $fixed_count -eq 0 ]]; then
    echo "No queries with LIMIT -1 found. All queries are OK."
else
    echo "Fixed $fixed_count query file(s)."
fi

