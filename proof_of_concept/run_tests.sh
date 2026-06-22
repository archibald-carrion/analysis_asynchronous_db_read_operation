#!/bin/bash

# TPC-H Complete Benchmark Script - Clean Version
# Uses existing SQL files directly

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${LOG_FILE:-$SCRIPT_DIR/query_execution.log}"
# Errors-only log: every error/warning plus the actual psql failure output is
# appended here, so a clean run leaves this file empty. The orchestrator
# (run_randomized_experiment.sh) overrides ERROR_LOG to a per-run path.
ERROR_LOG="${ERROR_LOG:-$SCRIPT_DIR/query_errors.log}"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/query_results}"
CSV_OUTPUT="${CSV_OUTPUT:-$SCRIPT_DIR/tpch_complete_results.csv}"
REFRESH_CSV="${REFRESH_CSV:-$SCRIPT_DIR/tpch_refresh_results.csv}"
INTERVAL_CSV="${INTERVAL_CSV:-$SCRIPT_DIR/tpch_interval_results.csv}"
DB_NAME="${DB_NAME:-tpch_db}"
DB_USER="${DB_USER:-tpch_user}"
DB_PASSWORD="${DB_PASSWORD:-tpch_password_123}"
ITERATIONS="${ITERATIONS:-2}"
RUNS_PER_ITERATION="${RUNS_PER_ITERATION:-2}"
QUERY_STREAMS="${QUERY_STREAMS:-2}"
SCALE_FACTOR="${SCALE_FACTOR:-1}"
AUTO_SET_QUERY_STREAMS="${AUTO_SET_QUERY_STREAMS:-1}"
IO_METHOD="${IO_METHOD:-${1:-sync}}"
QUERIES_DIR="${QUERIES_DIR:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"; }
# errorlog appends a plain (no-color) line to the errors-only log. Used by
# error()/warning() and by query/RF failure paths to record the real cause.
errorlog() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$ERROR_LOG"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"; errorlog "ERROR: $1"; exit 1; }
warning() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"; errorlog "WARNING: $1"; }
info() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$LOG_FILE"; }

# Append the tail of a psql output file (the actual Postgres error text) to the
# errors-only log, so failures are diagnosable from a single file. The whole
# block is assembled then appended in one write, so concurrent throughput
# streams don't interleave their error blocks.
errorlog_file() {
    local label="$1"
    local file="$2"
    local body
    if [[ -f "$file" ]]; then
        body="$(tail -n 20 "$file" 2>/dev/null)"
    else
        body="(output file not found: $file)"
    fi
    {
        printf '[%s] ----- %s -----\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$label"
        printf '%s\n' "$body"
        printf -- '-------------------------\n'
    } >> "$ERROR_LOG"
}

# Initialize CSV files
initialize_csv() {
    log "Initializing CSV output files"

    # Start the errors-only log fresh for this run (stays empty if nothing fails)
    : > "$ERROR_LOG"

    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_id,query_number,execution_order,execution_time_seconds,row_count,timestamp" > "$CSV_OUTPUT"
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_id,refresh_number,execution_order,execution_time_seconds,rows_affected,timestamp" > "$REFRESH_CSV"
    echo "io_method,iteration,run_in_iteration,global_run_id,test_type,stream_count,measurement_interval_seconds,start_time,end_time" > "$INTERVAL_CSV"
}

# Test PostgreSQL connection
test_postgres_connection() {
    info "Testing PostgreSQL connection..."
    export PGPASSWORD="$DB_PASSWORD"
    if ! psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" > /dev/null 2>&1; then
        error "Cannot connect to PostgreSQL database $DB_NAME as user $DB_USER"
    fi
    info "PostgreSQL connection successful"
}

# Auto-set query streams based on SF using the TPC-H minimum-streams table
# (Clause 5.4.1.2): SF<=1 -> 2, 10 -> 3, 30 -> 4, 100 -> 5, 300 -> 6, 1000 -> 7, ...
# We pick the S for the largest SF band that does not exceed the actual SF.
auto_set_query_streams() {
    if [[ "${AUTO_SET_QUERY_STREAMS}" != "1" ]]; then
        return 0
    fi

    local streams
    streams=$(python3 - <<'PY'
import os
sf = float(os.environ.get("SCALE_FACTOR", "1") or "1")
# (min SF for band, stream count) ordered ascending
table = [(0, 2), (10, 3), (30, 4), (100, 5), (300, 6), (1000, 7),
         (3000, 8), (10000, 9), (30000, 10), (100000, 11)]
s = table[0][1]
for threshold, count in table:
    if sf >= threshold:
        s = count
    else:
        break
print(s)
PY
)
    if [[ -n "$streams" ]]; then
        QUERY_STREAMS="$streams"
        export QUERY_STREAMS
        info "Auto-set query streams to ${QUERY_STREAMS} based on scale factor ${SCALE_FACTOR}"
    fi
}

# Choose queries directory based on scale factor (if QUERIES_DIR is not provided)
# Canonical scale-factor tag for the query dir name (e.g. SF 1, 1.0, 1.00 -> "1p0";
# 0.1 -> "0p1"; 10 -> "10p0"). The number is normalized to ONE decimal place first so
# integer and dotted forms of the same scale (the driver passes "1.0"/"10.0", a human
# may type "1"/"10") never produce divergent dir names. MUST match scale_query_dir() in
# run_setup.sh. NOTE: one-decimal normalization means sub-0.1 scales (e.g. 0.01) are not
# distinguishable here; the active design uses 0.1/1/10 only.
scale_factor_tag() {
    local norm
    # LC_ALL=C forces a '.' decimal point: locales like es_* use ',' for %.1f,
    # which would produce "10,0" and break the tag (tr '.' 'p' finds no dot).
    norm=$(LC_ALL=C printf "%.1f" "$1")
    printf "%s" "$norm" | tr '.' 'p' | tr '-' 'm'
}

select_queries_dir() {
    if [[ -n "$QUERIES_DIR" && -d "$QUERIES_DIR" ]]; then
        info "Using queries directory from QUERIES_DIR: $QUERIES_DIR"
        return 0
    fi
    
    local scale_tag
    scale_tag=$(scale_factor_tag "$SCALE_FACTOR")
    local candidate="$SCRIPT_DIR/tpch_queries_sf${scale_tag}"

    # The scale-specific dir is mandatory. Each scale factor has its own parameter
    # sets; there is intentionally no flat fallback, because running another scale's
    # (or a stale) query set against this database would silently corrupt the result.
    if [[ -d "$candidate" ]]; then
        QUERIES_DIR="$candidate"
        info "Using scale-specific queries directory: $QUERIES_DIR"
    else
        error "Queries directory not found for scale factor ${SCALE_FACTOR}: $candidate. Run run_setup.sh for SF=${SCALE_FACTOR} (generates per-stream query sets). No flat fallback is used."
    fi
}

# Execute single query with robust error handling
execute_query() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    local test_type=$4
    local stream_id=$5
    local query_num=$6
    local execution_order=$7

    # Per-stream parameter set: prefer $QUERIES_DIR/stream<id>/q<n>.sql so each
    # stream runs distinct substitution parameters (TPC-H). Fall back to the flat
    # $QUERIES_DIR/q<n>.sql for back-compat or when per-stream sets are absent.
    local query_file="$QUERIES_DIR/stream${stream_id}/q${query_num}.sql"
    if [[ ! -f "$query_file" ]]; then
        query_file="$QUERIES_DIR/q${query_num}.sql"
    fi

    if [[ ! -f "$query_file" ]]; then
        warning "Query file $query_file not found in $QUERIES_DIR, skipping"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
    
    info "Executing Iteration ${iteration} Run ${run_in_iteration} ${test_type} Stream ${stream_id} Q${query_num}..."
    
    export PGPASSWORD="$DB_PASSWORD"
    mkdir -p "$RESULTS_DIR"
    
    local result_file="$RESULTS_DIR/${IO_METHOD}_iter${iteration}_run${run_in_iteration}_${test_type}_s${stream_id}_q${query_num}.txt"
    local start_time=$(date +%s.%N)
    
    # Execute query without timeout (let it run until completion)
    if psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$query_file" > "$result_file" 2>&1; then
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        # Get row count (excluding headers)
        local row_count=$(tail -n +3 "$result_file" | grep -c . 2>/dev/null || echo "0")
        
        # Write to CSV
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},${execution_time},${row_count},$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        
        info "Q${query_num} completed in ${execution_time}s with ${row_count} rows"
        return 0
    else
        local exit_code=$?
        warning "Query Q${query_num} failed (exit $exit_code) ${test_type} stream ${stream_id} run ${run_id} [file: $query_file]"
        errorlog_file "Q${query_num} ${test_type} stream ${stream_id} run ${run_id} (psql exit $exit_code)" "$result_file"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${query_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$CSV_OUTPUT"
        return 1
    fi
}

# Ensure refresh function files and refresh data exist locally.
# Mandatory for a valid TPC-H run. RF1/RF2 use dbgen's numbered update sets
# (dss.ri.orders.k / dss.ri.lineitem.k / dss.rd.k, k=1..N). If anything is
# missing we abort rather than silently skip RF (which yields an invalid 0.00 QphH).
#
# TPC-H Clause 2.8.1 "endless reuse": the database is NOT reloaded between runs; it
# evolves. A persistent per-DB counter (dss.next_set, see next_refresh_set) hands each
# RF1/RF2 pair the NEXT unused set so no RF1 ever re-inserts an existing key. run_setup.sh
# provisions a pool of (1+S)*runs sets; here we just verify enough remain for THIS run.
ensure_refresh_files() {
    # rf*.sql: hand-authored source, recoverable from templates/.
    for fname in rf1.sql rf2.sql; do
        local target="$QUERIES_DIR/$fname"
        [[ -f "$target" ]] && continue
        local fallback="$SCRIPT_DIR/templates/$fname"
        if [[ -f "$fallback" ]]; then
            mkdir -p "$QUERIES_DIR"
            cp "$fallback" "$target"
            info "Copied $fname from templates/ into $QUERIES_DIR"
        else
            error "Missing refresh function: $target (template $fallback not found). RF1/RF2 are mandatory for a valid run."
        fi
    done

    # Initialize the per-DB RF-set counter on first use. It records the next UNUSED
    # set index; it persists in QUERIES_DIR (per-scale, survives across run_tests.sh
    # invocations) and advances by 1+S each run for the life of the experiment.
    [[ -f "$QUERIES_DIR/dss.next_set" ]] || echo 1 > "$QUERIES_DIR/dss.next_set"

    # This run will consume 1 (power) + QUERY_STREAMS (throughput) fresh sets starting
    # at dss.next_set. dss.nsets records how many run_setup.sh generated; verify enough
    # remain in the pool (not just that 1+S exist at all).
    local need=$((1 + QUERY_STREAMS))
    local have=0
    [[ -f "$QUERIES_DIR/dss.nsets" ]] && have=$(cat "$QUERIES_DIR/dss.nsets")
    local next; next=$(cat "$QUERIES_DIR/dss.next_set")
    local remaining=$((have - next + 1))
    if [[ "$remaining" -lt "$need" ]]; then
        error "Refresh-set pool exhausted in $QUERIES_DIR: ${remaining} set(s) remain (next=${next}, total=${have}), need ${need} (1 power + ${QUERY_STREAMS} throughput) for this run. The experiment consumed more runs (incl. FAILED-run retries) than provisioned. Re-run run_setup.sh for this scale to regenerate a larger pool (sized (1+S)*runs), or raise SET_SAFETY_RUNS."
    fi
    # Verify the specific sets this run will use are present and well-formed.
    local k
    for ((k=next; k<next+need; k++)); do
        [[ -f "$QUERIES_DIR/dss.ri.orders.${k}" && -f "$QUERIES_DIR/dss.ri.lineitem.${k}" && -f "$QUERIES_DIR/dss.rd.${k}" ]] || \
            error "Missing refresh data for set ${k} in $QUERIES_DIR (dss.ri.orders.${k}/dss.ri.lineitem.${k}/dss.rd.${k}). Re-run run_setup.sh for this scale."
    done
}

# Allocate the next unused refresh set index for this DB and advance the persistent
# counter. MUST be called serially from the orchestrating shell (NOT from a parallel
# throughput subshell) so two streams never race on dss.next_set: callers pre-allocate
# every set a run needs up front, before forking. Echoes the allocated index.
next_refresh_set() {
    local counter="$QUERIES_DIR/dss.next_set"
    local n
    n=$(cat "$counter")
    echo $((n + 1)) > "$counter"
    echo "$n"
}

# Execute refresh function with robust error handling
execute_refresh_function() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    local test_type=$4
    local stream_id=$5
    local refresh_num=$6
    local execution_order=$7
    local set_num=$8   # which dbgen refresh set (1..1+S) this RF1/RF2 pair uses

    local refresh_file="$QUERIES_DIR/rf${refresh_num}.sql"
    # TPC-H requires a distinct insert/delete set per RF pair (spec 2.6.3/2.8.1).
    # run_setup.sh produced numbered sets dss.ri.orders.k / dss.ri.lineitem.k /
    # dss.rd.k in the query dir. psql \copy cannot interpolate -v variables, and the
    # refresh stream may run concurrently with query streams, so we STAGE set
    # ${set_num} into a private temp dir under fixed names (dss.ri.orders /
    # dss.ri.lineitem / dss.rd) and run psql with CWD there. The rf SQL then uses
    # plain relative \copy of those fixed names.
    local src_orders="$QUERIES_DIR/dss.ri.orders.${set_num}"
    local src_lineitem="$QUERIES_DIR/dss.ri.lineitem.${set_num}"
    local src_rd="$QUERIES_DIR/dss.rd.${set_num}"

    # Ensure refresh SQL exists. Source of truth is templates/ (hand-authored,
    # version-controlled); run_setup.sh normally copies it into the scale dir.
    if [[ ! -f "$refresh_file" ]]; then
        local fallback="$SCRIPT_DIR/templates/rf${refresh_num}.sql"
        if [[ -f "$fallback" ]]; then
            mkdir -p "$QUERIES_DIR"
            cp "$fallback" "$refresh_file"
            info "Copied refresh SQL rf${refresh_num}.sql from templates/ into $QUERIES_DIR"
        else
            error "Refresh function file $refresh_file not found (template $fallback missing). RF is mandatory for a valid run."
        fi
    fi

    # Ensure refresh data for this set exists (mandatory: missing data invalidates run)
    if [[ "$refresh_num" == "1" ]]; then
        [[ -f "$src_orders" && -f "$src_lineitem" ]] || \
            error "Refresh data missing for set ${set_num}: $src_orders / $src_lineitem (required for RF1). Re-run run_setup.sh for this scale."
    elif [[ "$refresh_num" == "2" ]]; then
        [[ -f "$src_rd" ]] || \
            error "Refresh data file missing for set ${set_num}: $src_rd (required for RF2). Re-run run_setup.sh for this scale."
    fi

    # Stage this set into a private temp dir with the fixed names rf SQL expects.
    local refresh_dir
    refresh_dir="$(mktemp -d "${TMPDIR:-/tmp}/tpch_rf_${test_type}_s${stream_id}_set${set_num}.XXXXXX")"
    cp "$refresh_file" "$refresh_dir/"
    if [[ "$refresh_num" == "1" ]]; then
        cp "$src_orders"   "$refresh_dir/dss.ri.orders"
        cp "$src_lineitem" "$refresh_dir/dss.ri.lineitem"
    else
        cp "$src_rd" "$refresh_dir/dss.rd"
    fi

    info "Executing Iteration ${iteration} Run ${run_in_iteration} ${test_type} Stream ${stream_id} RF${refresh_num} (set ${set_num})..."

    export PGPASSWORD="$DB_PASSWORD"
    local start_time=$(date +%s.%N)
    local output_file="$RESULTS_DIR/${IO_METHOD}_iter${iteration}_run${run_in_iteration}_${test_type}_s${stream_id}_rf${refresh_num}.txt"
    
    # Capture counts before refresh to compute actual rows affected
    local orders_before lineitem_before orders_after lineitem_after rows_affected
    orders_before=$(psql -X -h localhost -U "$DB_USER" -d "$DB_NAME" -At -c "SELECT COUNT(*) FROM orders;" 2>/dev/null | tr -d '[:space:]') || orders_before=0
    lineitem_before=$(psql -X -h localhost -U "$DB_USER" -d "$DB_NAME" -At -c "SELECT COUNT(*) FROM lineitem;" 2>/dev/null | tr -d '[:space:]') || lineitem_before=0
    
    # Execute refresh function without timeout (let it run until completion).
    # Run psql with CWD = the scale's query dir so the relative \copy paths in
    # rf1.sql/rf2.sql (dss.ri.orders / dss.ri.lineitem / dss.rd) resolve. psql's
    # \copy is client-side and cannot interpolate -v variables, so the data files
    # must be referenced by relative path from this working directory.
    # $output_file is absolute (under RESULTS_DIR), so it is unaffected by the cd.
    if ( cd "$refresh_dir" && psql -h localhost -U "$DB_USER" -d "$DB_NAME" \
        -c "\set VERBOSITY verbose" \
        -c "\timing on" \
        -f "$(basename "$refresh_file")" ) > "$output_file" 2>&1; then
        local end_time=$(date +%s.%N)
        local execution_time=$(echo "$end_time - $start_time" | bc)
        
        # Compute rows affected from before/after counts
        orders_after=$(psql -X -h localhost -U "$DB_USER" -d "$DB_NAME" -At -c "SELECT COUNT(*) FROM orders;" 2>/dev/null | tr -d '[:space:]') || orders_after="$orders_before"
        lineitem_after=$(psql -X -h localhost -U "$DB_USER" -d "$DB_NAME" -At -c "SELECT COUNT(*) FROM lineitem;" 2>/dev/null | tr -d '[:space:]') || lineitem_after="$lineitem_before"
        rows_affected=$(( (orders_after - orders_before) < 0 ? orders_before - orders_after : orders_after - orders_before ))
        local li_delta=$(( (lineitem_after - lineitem_before) < 0 ? lineitem_before - lineitem_after : lineitem_after - lineitem_before ))
        rows_affected=$(( rows_affected + li_delta ))
        
        # Write to refresh CSV
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},${execution_time},${rows_affected},$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        
        info "RF${refresh_num} completed in ${execution_time}s"
        rm -rf "$refresh_dir"
        return 0
    else
        local exit_code=$?
        warning "Refresh function RF${refresh_num} failed (exit $exit_code) ${test_type} stream ${stream_id} run ${run_id} [file: $refresh_file]"
        errorlog_file "RF${refresh_num} ${test_type} stream ${stream_id} run ${run_id} (psql exit $exit_code)" "$output_file"
        echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${test_type},${stream_id},${refresh_num},${execution_order},0,0,$(date '+%Y-%m-%d %H:%M:%S')" >> "$REFRESH_CSV"
        rm -rf "$refresh_dir"
        return 1
    fi
}

# Power Test (TPC-H Requirement) - SIMPLIFIED without return value issues
execute_power_test() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    
    log "Starting Power Test (Iteration $iteration, Run $run_in_iteration)"

    # TPC-H spec 2.8.1: the power test executes exactly ONE RF1/RF2 pair, using the
    # NEXT unused refresh set from the persistent per-DB counter (endless reuse: the DB
    # is never reloaded, so each run must advance to a fresh set to avoid re-inserting
    # existing keys). RF1 and RF2 of the pair share the same set index.
    local power_set
    power_set=$(next_refresh_set)
    info "Power test using refresh set ${power_set}"
    # RF1 before queries (set $power_set)
    execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "1" "1" "$power_set"

    # Execute all 22 queries sequentially (stream 0). Clause 5.3.5.4: the power
    # test uses ordering number O(00) -> Appendix A ordered set 0.
    local power_order=($(ordered_set_for_stream 0))
    local execution_order=2
    for query_num in "${power_order[@]}"; do
        execute_query "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "$query_num" "$execution_order"
        execution_order=$((execution_order + 1))
    done

    # RF2 after queries (same set as RF1)
    execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "POWER" "0" "2" "24" "$power_set"
    
    log "Power Test (Iteration $iteration, Run $run_in_iteration) completed"
}

# TPC-H Appendix A "Ordered Sets" (Revision 3.0.1, adapted from Moses & Oakford,
# Tables of Random Permutations). Clause 5.3.5.4 REQUIRES that the query submission
# order for ordering number O(s) be the ordered set with reference s from this table
# -- it is a FIXED permutation, not a random shuffle. Index 0 is the Power test
# (O(00)); indices 1..40 are the Throughput streams (O(01)..O(40)). Per the 5.3.5.4
# comment, references wrap around (s mod 41) once the list is exhausted.
TPCH_ORDERED_SETS=(
    "14 2 9 20 6 17 18 8 21 13 3 22 16 4 11 15 1 10 19 5 7 12"   # 0  Power
    "21 3 18 5 11 7 6 20 17 12 16 15 13 10 2 8 14 19 9 22 1 4"   # 1
    "6 17 14 16 19 10 9 2 15 8 5 22 12 7 13 18 1 4 20 3 11 21"   # 2
    "8 5 4 6 17 7 1 18 22 14 9 10 15 11 20 2 21 19 13 16 12 3"   # 3
    "5 21 14 19 15 17 12 6 4 9 8 16 11 2 10 18 1 13 7 22 3 20"   # 4
    "21 15 4 6 7 16 19 18 14 22 11 13 3 1 2 5 8 20 12 17 10 9"   # 5
    "10 3 15 13 6 8 9 7 4 11 22 18 12 1 5 16 2 14 19 20 17 21"   # 6
    "18 8 20 21 2 4 22 17 1 11 9 19 3 13 5 7 10 16 6 14 15 12"   # 7
    "19 1 15 17 5 8 9 12 14 7 4 3 20 16 6 22 10 13 2 21 18 11"   # 8
    "8 13 2 20 17 3 6 21 18 11 19 10 15 4 22 1 7 12 9 14 5 16"   # 9
    "6 15 18 17 12 1 7 2 22 13 21 10 14 9 3 16 20 19 11 4 8 5"   # 10
    "15 14 18 17 10 20 16 11 1 8 4 22 5 12 3 9 21 2 13 6 19 7"   # 11
    "1 7 16 17 18 22 12 6 8 9 11 4 2 5 20 21 13 10 19 3 14 15"   # 12
    "21 17 7 3 1 10 12 22 9 16 6 11 2 4 5 14 8 20 13 18 15 19"   # 13
    "2 9 5 4 18 1 20 15 16 17 7 21 13 14 19 8 22 11 10 3 12 6"   # 14
    "16 9 17 8 14 11 10 12 6 21 7 3 15 5 22 20 1 13 19 2 4 18"   # 15
    "1 3 6 5 2 16 14 22 17 20 4 9 10 11 15 8 12 19 18 13 7 21"   # 16
    "3 16 5 11 21 9 2 15 10 18 17 7 8 19 14 13 1 4 22 20 6 12"   # 17
    "14 4 13 5 21 11 8 6 3 17 2 20 1 19 10 9 12 18 15 7 22 16"   # 18
    "4 12 22 14 5 15 16 2 8 10 17 9 21 7 3 6 13 18 11 20 19 1"   # 19
    "16 15 14 13 4 22 18 19 7 1 12 17 5 10 20 3 9 21 11 2 6 8"   # 20
    "20 14 21 12 15 17 4 19 13 10 11 1 16 5 18 7 8 22 9 6 3 2"   # 21
    "16 14 13 2 21 10 11 4 1 22 18 12 19 5 7 8 6 3 15 20 9 17"   # 22
    "18 15 9 14 12 2 8 11 22 21 16 1 6 17 5 10 19 4 20 13 3 7"   # 23
    "7 3 10 14 13 21 18 6 20 4 9 8 22 15 2 1 5 12 19 17 11 16"   # 24
    "18 1 13 7 16 10 14 2 19 5 21 11 22 15 8 17 20 3 4 12 6 9"   # 25
    "13 2 22 5 11 21 20 14 7 10 4 9 19 18 6 3 1 8 15 12 17 16"   # 26
    "14 17 21 8 2 9 6 4 5 13 22 7 15 3 1 18 16 11 10 12 20 19"   # 27
    "10 22 1 12 13 18 21 20 2 14 16 7 15 3 4 17 5 19 6 8 9 11"   # 28
    "10 8 9 18 12 6 1 5 20 11 17 22 16 3 13 2 15 21 14 19 7 4"   # 29
    "7 17 22 5 3 10 13 18 9 1 14 15 21 19 16 12 8 6 11 20 4 2"   # 30
    "2 9 21 3 4 7 1 11 16 5 20 19 18 8 17 13 10 12 15 6 14 22"   # 31
    "15 12 8 4 22 13 16 17 18 3 7 5 6 1 9 11 21 10 14 20 19 2"   # 32
    "15 16 2 11 17 7 5 14 20 4 21 3 10 9 12 8 13 6 18 19 22 1"   # 33
    "1 13 11 3 4 21 6 14 15 22 18 9 7 5 10 20 12 16 17 8 19 2"   # 34
    "14 17 22 20 8 16 5 10 1 13 2 21 12 9 4 18 3 7 6 19 15 11"   # 35
    "9 17 7 4 5 13 21 18 11 3 22 1 6 16 20 14 15 10 8 2 12 19"   # 36
    "13 14 5 22 19 11 9 6 18 15 8 10 7 4 17 16 3 1 12 2 21 20"   # 37
    "20 5 4 14 11 1 6 16 8 22 7 3 2 12 21 19 17 13 10 15 18 9"   # 38
    "3 7 14 15 6 5 21 20 18 10 4 16 19 1 13 9 8 17 11 12 22 2"   # 39
    "13 15 17 1 22 11 3 4 7 20 14 21 9 8 2 18 16 6 10 12 5 19"   # 40
)

# Return the TPC-H-mandated query submission order for ordering number O(s).
# Argument is the stream's ordering number s (0 = power, 1..N = throughput).
# Wraps around the 41-entry table per Clause 5.3.5.4's comment.
ordered_set_for_stream() {
    local s=$1
    local n=${#TPCH_ORDERED_SETS[@]}
    echo "${TPCH_ORDERED_SETS[$(( s % n ))]}"
}

# Throughput Test (TPC-H Requirement) - SIMPLIFIED without return value issues
execute_throughput_test() {
    local run_id=$1
    local iteration=$2
    local run_in_iteration=$3
    
    log "Starting Throughput Test (Iteration $iteration, Run $run_in_iteration) with $QUERY_STREAMS streams"
    
    # Record measurement interval start time
    local start_time=$(date +%s.%N)
    
    local pids=()
    local queries_per_stream=22
    local refresh_start=$((QUERY_STREAMS * queries_per_stream + 1))

    # Pre-allocate the S refresh sets this test will use, BEFORE forking. The counter
    # (dss.next_set) must only ever be advanced by this orchestrating shell; allocating
    # inside the parallel refresh subshell would race nothing here (single refresh
    # stream), but pre-allocating keeps all counter writes serial and lets the subshell
    # capture the indices by value. One distinct set per RF1/RF2 pair (TPC-H 2.8.1).
    local tput_sets=()
    local pair
    for pair in $(seq 1 "$QUERY_STREAMS"); do
        tput_sets+=("$(next_refresh_set)")
    done
    info "Throughput test using refresh sets: ${tput_sets[*]}"

    # Execute query streams in parallel
    for stream in $(seq 1 $QUERY_STREAMS); do
        (
            # Clause 5.3.5.4: throughput query stream s uses ordering number O(s)
            # -> Appendix A ordered set s (s = 1..N).
            local stream_queries=($(ordered_set_for_stream "$stream"))
            local execution_order=$(( (stream - 1) * queries_per_stream + 1 ))
            for query_num in "${stream_queries[@]}"; do
                execute_query "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "$stream" "$query_num" "$execution_order"
                execution_order=$((execution_order + 1))
            done
        ) &
        pids+=($!)
    done
    
    # Execute the refresh stream in parallel with the query streams.
    # TPC-H spec 2.8.1: the throughput test executes exactly S RF1/RF2 pairs (S =
    # number of query streams), each using a DISTINCT refresh set. The sets were
    # pre-allocated above into tput_sets[] (the next S unused indices after the power
    # test's set), so each pair uses a fresh set -> no pk collision. This is a bounded
    # sequence, NOT a continuous loop.
    (
        local execution_order=$refresh_start
        local set_num
        for set_num in "${tput_sets[@]}"; do
            execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "R" "1" "$execution_order" "$set_num"
            execution_order=$((execution_order + 1))
            execute_refresh_function "$run_id" "$iteration" "$run_in_iteration" "THROUGHPUT" "R" "2" "$execution_order" "$set_num"
            execution_order=$((execution_order + 1))
        done
    ) &
    local refresh_pid=$!
    
    # Wait for all processes to complete
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    # Ensure refresh stream stops after queries finish
    if kill -0 "$refresh_pid" 2>/dev/null; then
        wait "$refresh_pid"
    fi
    
    # Record measurement interval end time
    local end_time=$(date +%s.%N)
    local measurement_interval=$(echo "$end_time - $start_time" | bc)
    
    # Record measurement interval
    echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},THROUGHPUT,${QUERY_STREAMS},${measurement_interval},${start_time},${end_time}" >> "$INTERVAL_CSV"
    
    log "Throughput Test (Iteration $iteration, Run $run_in_iteration) completed in ${measurement_interval} seconds"
}

# Configure PostgreSQL for specific I/O method.
# NOTE: this function only logs the intended method. The actual postgresql.conf
# changes (io_method / worker settings) are applied upstream by toggle_pg_config.sh
# before this script runs; this is just a record of what the run expects.
configure_postgresql() {
    local io_method=$1
    info "I/O method for this run: $io_method (postgresql.conf already set by toggle_pg_config.sh)"

    case $io_method in
        "sync")
            info "Using synchronous I/O (default)"
            ;;
        "bgworkers")
            info "Using background workers configuration"
            ;;
        "iouring"|"io_uring")
            info "Using io_uring configuration"
            ;;
        *)
            warning "Unknown I/O method: $io_method (continuing with current configuration)"
            ;;
    esac
    
    # Restart PostgreSQL to apply changes unless orchestrator already handled it
    if [[ -z "${SKIP_POSTGRES_RESTART:-}" ]]; then
        sudo systemctl restart postgresql
        sleep 5
    else
        info "PostgreSQL restart skipped (SKIP_POSTGRES_RESTART=${SKIP_POSTGRES_RESTART})"
    fi
}

# Calculate QphH metric for a single run
calculate_qphh() {
    local complete_csv=$1
    local refresh_csv=$2
    local interval_csv=$3
    
    if [[ ! -f "$complete_csv" ]] || [[ ! -f "$refresh_csv" ]] || [[ ! -f "$interval_csv" ]]; then
        echo "0.00"
        return
    fi
    
    local result
    result=$(python3 - "$complete_csv" "$refresh_csv" "$interval_csv" "$SCALE_FACTOR" <<'PY'
import csv
import math
import sys

complete_path, refresh_path, interval_path, scale_factor_str = sys.argv[1:5]

def parse_positive(value):
    try:
        v = float(value)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None

def collect_times(path, expected_count):
    times = []
    with open(path, newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get('test_type', '').upper() != 'POWER':
                continue
            if row.get('stream_id', '') != '0':
                continue
            value = parse_positive(row.get('execution_time_seconds'))
            if value is not None:
                times.append(value)
    return times if len(times) == expected_count else None

try:
    scale_factor = float(scale_factor_str)
except (ValueError, TypeError):
    scale_factor = 1.0

power_times = collect_times(complete_path, 22)
refresh_times = collect_times(refresh_path, 2)

if not power_times or not refresh_times:
    print("0.00")
    sys.exit(0)

with open(interval_path, newline='') as handle:
    reader = csv.DictReader(handle)
    interval_row = next((row for row in reader if row.get('test_type', '').upper() == 'THROUGHPUT'), None)

if not interval_row:
    print("0.00")
    sys.exit(0)

measurement = parse_positive(interval_row.get('measurement_interval_seconds'))
stream_count_val = interval_row.get('stream_count')
try:
    stream_count = int(float(stream_count_val))
except (ValueError, TypeError):
    stream_count = 0

if measurement is None or stream_count <= 0:
    print("0.00")
    sys.exit(0)

# Geometric mean via log-sum to avoid overflow
log_sum = sum(math.log(t) for t in power_times + refresh_times)
geom_mean = math.exp(log_sum / 24.0)

power_metric = (3600.0 * scale_factor) / geom_mean
# Throughput@Size (TPC-H Clause 5.4.2.1): (S × 22 × 3600) / Ts × SF
throughput_metric = (stream_count * 22 * 3600.0) / measurement * scale_factor

if power_metric <= 0 or throughput_metric <= 0:
    print("0.00")
    sys.exit(0)

# QphH@Size según la imagen: 1 / sqrt((1 / Power@Size) × (1 / Throughput@Size))
qphh = 1.0 / math.sqrt((1.0 / power_metric) * (1.0 / throughput_metric))
print(f"{qphh:.2f}")
PY
)
    
    if [[ -n "$result" ]] && [[ "$result" != "0.00" ]]; then
        echo "$result"
    else
        echo "0.00"
    fi
}

# Generate CSV with response variable (QphH) for all runs
generate_response_variable_csv() {
    local response_csv="$RESULTS_DIR/tpch_response_variable.csv"
    
    log "Calculating QphH metrics and generating response variable CSV..."
    
    # Initialize CSV header
    echo "io_method,iteration,run_in_iteration,global_run_id,power_metric,throughput_metric,qphh_metric,scale_factor" > "$response_csv"
    
    local total_runs=$((ITERATIONS * RUNS_PER_ITERATION))
    local calculated_count=0
    local failed_count=0
    
    # Process each run
    for iteration in $(seq 1 $ITERATIONS); do
        for run_in_iteration in $(seq 1 $RUNS_PER_ITERATION); do
            local run_id=$(( (iteration - 1) * RUNS_PER_ITERATION + run_in_iteration ))
            
            # Create temporary CSV files filtered for this specific run
            local temp_complete=$(mktemp)
            local temp_refresh=$(mktemp)
            local temp_interval=$(mktemp)
            
            # Filter CSVs for this run and add headers
            (head -1 "$CSV_OUTPUT" && grep "^${IO_METHOD},${iteration},${run_in_iteration},${run_id}," "$CSV_OUTPUT") > "$temp_complete" 2>/dev/null || true
            (head -1 "$REFRESH_CSV" && grep "^${IO_METHOD},${iteration},${run_in_iteration},${run_id}," "$REFRESH_CSV") > "$temp_refresh" 2>/dev/null || true
            (head -1 "$INTERVAL_CSV" && grep "^${IO_METHOD},${iteration},${run_in_iteration},${run_id}," "$INTERVAL_CSV") > "$temp_interval" 2>/dev/null || true
            
            # Calculate QphH for this run (which internally calculates power and throughput)
            local qphh_result=$(calculate_qphh "$temp_complete" "$temp_refresh" "$temp_interval")
            
            # Extract power and throughput metrics (calculate them separately for completeness)
            local power_metric="0.00"
            local throughput_metric="0.00"
            
            # If we have valid data, calculate power and throughput separately
            if [[ "$qphh_result" != "0.00" ]] && [[ -s "$temp_complete" ]] && [[ -s "$temp_refresh" ]] && [[ -s "$temp_interval" ]]; then
                # Calculate power metric using Python
                power_metric=$(python3 - "$temp_complete" "$temp_refresh" "$SCALE_FACTOR" <<'PY'
import csv
import math
import sys

complete_path, refresh_path, scale_factor_str = sys.argv[1:4]

def parse_positive(value):
    try:
        v = float(value)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None

def collect_times(path):
    times = []
    with open(path, newline='') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get('test_type', '').upper() != 'POWER':
                continue
            if row.get('stream_id', '') != '0':
                continue
            value = parse_positive(row.get('execution_time_seconds'))
            if value is not None:
                times.append(value)
    return times

try:
    scale_factor = float(scale_factor_str)
except (ValueError, TypeError):
    scale_factor = 1.0

power_times = collect_times(complete_path)
refresh_times = collect_times(refresh_path)

if len(power_times) != 22 or len(refresh_times) != 2:
    print("0.00")
    sys.exit(0)

log_sum = sum(math.log(t) for t in power_times + refresh_times)
geom_mean = math.exp(log_sum / 24.0)
power_metric = (3600.0 * scale_factor) / geom_mean
print(f"{power_metric:.2f}")
PY
)
                
                # Calculate throughput metric
                throughput_metric=$(python3 - "$temp_interval" "$QUERY_STREAMS" "$SCALE_FACTOR" <<'PY'
import csv
import sys

interval_path, stream_count_str, scale_factor_str = sys.argv[1:4]

def parse_positive(value):
    try:
        v = float(value)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None

try:
    stream_count = int(float(stream_count_str))
    scale_factor = float(scale_factor_str)
except (ValueError, TypeError):
    stream_count = 0
    scale_factor = 1.0

with open(interval_path, newline='') as handle:
    reader = csv.DictReader(handle)
    interval_row = next((row for row in reader if row.get('test_type', '').upper() == 'THROUGHPUT'), None)

if not interval_row:
    print("0.00")
    sys.exit(0)

measurement = parse_positive(interval_row.get('measurement_interval_seconds'))

if measurement is None or stream_count <= 0:
    print("0.00")
    sys.exit(0)

# Throughput@Size (TPC-H Clause 5.4.2.1): (S × 22 × 3600) / Ts × SF
throughput_metric = (stream_count * 22 * 3600.0) / measurement * scale_factor
print(f"{throughput_metric:.2f}")
PY
)
            fi
            
            if [[ "$qphh_result" != "0.00" ]]; then
                calculated_count=$((calculated_count + 1))
            else
                failed_count=$((failed_count + 1))
                warning "Failed to calculate QphH for Iteration $iteration, Run $run_in_iteration (Run ID: $run_id)"
            fi
            
            # Write to response CSV
            echo "${IO_METHOD},${iteration},${run_in_iteration},${run_id},${power_metric},${throughput_metric},${qphh_result},${SCALE_FACTOR}" >> "$response_csv"
            
            # Cleanup temp files
            rm -f "$temp_complete" "$temp_refresh" "$temp_interval"
        done
    done
    
    log "Response variable CSV generated: $response_csv"
    log "Successfully calculated QphH for $calculated_count out of $total_runs runs"
    if [[ $failed_count -gt 0 ]]; then
        warning "$failed_count runs failed to calculate QphH (check input CSVs)"
    fi
}

# Generate TPC-H metric calculation summary
generate_tpch_summary() {
    local summary_file="$RESULTS_DIR/tpch_metrics_summary.txt"
    
    cat > "$summary_file" << EOF
TPC-H Complete Benchmark Metrics Summary
Generated: $(date)
I/O Method: $IO_METHOD
Database: $DB_NAME
Scale Factor: $SCALE_FACTOR
Iterations: $ITERATIONS
Runs per Iteration: $RUNS_PER_ITERATION
Total Runs: $((ITERATIONS * RUNS_PER_ITERATION))

Output Files:
- Query Results: $CSV_OUTPUT
- Refresh Results: $REFRESH_CSV  
- Interval Results: $INTERVAL_CSV

TPC-H Metric Formulas:

1. POWER@Size = 3600 × SF × √[1 / (∏ QI(i,0) × ∏ RI(j,0))]^(1/24)

2. THROUGHPUT@Size = (S × 22 × 3600) / T_s × SF

3. QphH@Size = 1 / sqrt((1 / Power@Size) × (1 / Throughput@Size))

Where:
- QI(i,0): Query times from POWER test (stream 0)
- RI(j,0): Refresh times from POWER test (stream 0)
- S: Query streams ($QUERY_STREAMS)
- T_s: Measurement interval from INTERVAL_CSV
- SF: Scale factor ($SCALE_FACTOR)

Data Structure:
- This invocation runs $((ITERATIONS * RUNS_PER_ITERATION)) run(s)
  (ITERATIONS=$ITERATIONS x RUNS_PER_ITERATION=$RUNS_PER_ITERATION).
- In the live experiment, run_randomized_experiment.sh calls this script once per
  scheduled treatment with ITERATIONS=1, RUNS_PER_ITERATION=1, producing one QphH
  per (I/O method, DB size, replicate) row of the CRD schedule.

Analysis Approach:
1. Calculate Power, Throughput, and QphH for each run.
2. Each scheduled run yields one QphH@Size, recorded back into the schedule CSV.
3. Aggregate across the full randomized design (3 I/O methods x 4 DB sizes x 12
   replicates = 144 runs) for the comparative analysis.
EOF

    log "TPC-H metrics summary saved to: $summary_file"
}

# Main execution function - SIMPLIFIED without execution order tracking
main() {
    log "Starting Complete TPC-H Benchmark..."
    log "I/O Method: $IO_METHOD"
    log "Database: $DB_NAME"
    
    mkdir -p "$RESULTS_DIR"
    initialize_csv
    test_postgres_connection
    auto_set_query_streams
    select_queries_dir
    
    log "Scale Factor: $SCALE_FACTOR"
    log "Query Streams: $QUERY_STREAMS"
    log "Queries Dir: $QUERIES_DIR"
    log "Iterations: $ITERATIONS (with $RUNS_PER_ITERATION runs each)"
    log "Total Runs: $((ITERATIONS * RUNS_PER_ITERATION))"
    
    # Ensure refresh function files are available
    ensure_refresh_files
    
    # Configure PostgreSQL for this I/O method
    configure_postgresql "$IO_METHOD"
    
    # Execute ITERATIONS x RUNS_PER_ITERATION runs (live flow uses 1 x 1 per treatment)
    for iteration in $(seq 1 $ITERATIONS); do
        log "Starting Iteration $iteration of $ITERATIONS"
        
        for run_in_iteration in $(seq 1 $RUNS_PER_ITERATION); do
            # Calculate global run_id for CSV tracking
            local run_id=$(( (iteration - 1) * RUNS_PER_ITERATION + run_in_iteration ))
            
            log "Starting Run $run_in_iteration of $RUNS_PER_ITERATION (Global Run ID: $run_id)"
            
            # Power Test followed by Throughput Test (TPC-H requirement)
            execute_power_test "$run_id" "$iteration" "$run_in_iteration"
            execute_throughput_test "$run_id" "$iteration" "$run_in_iteration"
            
            log "Completed Run $run_in_iteration of $RUNS_PER_ITERATION"
        done
        
        log "Completed Iteration $iteration of $ITERATIONS"
    done
    
    generate_tpch_summary
    
    # Calculate QphH metrics and generate response variable CSV
    generate_response_variable_csv
    
    log "Complete TPC-H benchmark execution finished!"
    log "Total runs executed: $((ITERATIONS * RUNS_PER_ITERATION))"
    log "Query results: $CSV_OUTPUT"
    log "Refresh results: $REFRESH_CSV"
    log "Interval results: $INTERVAL_CSV"
    log "Response variable (QphH): ${RESULTS_DIR}/tpch_response_variable.csv"
}

# Cleanup
trap 'unset PGPASSWORD; log "Script execution completed"' EXIT

main "$@"
