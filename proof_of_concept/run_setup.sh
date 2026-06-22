#!/usr/bin/env bash
# TPC-H setup for PostgreSQL on Debian 13 (Trixie)
# - Arregla el repo PGDG sin conflictos de Signed-By (Deb822)
# - Usa tpch-kit (gregrahn) y carga datos vía \copy (DELIMITER '|')
# - No reinstala PostgreSQL si ya lo tienes
#
# Uso:
#   ./run_setup.sh                # modo normal (crea DB, datos y queries)
#   ./run_setup.sh --clean        # limpia y recompila tpch-kit
#   ./run_setup.sh --queries-only # solo genera queries Q1-Q22 (NO toca la DB)
#   DB_NAME=tpch DB_USER=tpch_user DB_PASSWORD=xxx SCALE_FACTOR=10 ./run_setup.sh
#   SCALE_FACTOR=0.01 DB_NAME=tpch_db_10mb ./run_setup.sh --queries-only

set -euo pipefail

# ---- Config editables por variables de entorno ----
DB_NAME="${DB_NAME:-tpch_db}"
DB_USER="${DB_USER:-tpch_user}"
DB_PASSWORD="${DB_PASSWORD:-tpch_password_123}"
SCALE_FACTOR="${SCALE_FACTOR:-1}"       # ~GB (1, 10, 40, 100)
PGVER="${PGVER:-18}"                    # para mensajes solamente

# ---- Flags ----
CLEAN=false
LOW_MEMORY=false
QUERIES_ONLY=false
for arg in "${@:-}"; do
  case "$arg" in
    --clean) CLEAN=true ;;
    --low-memory) LOW_MEMORY=true ;;
    --queries-only) QUERIES_ONLY=true ;;
  esac
done

# ---- Utilidades de logging ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Source-of-truth, hand-authored files that nothing regenerates (RF1/RF2 SQL).
# These are version-controlled and copied into each scale-specific query dir.
TEMPLATES_DIR="$SCRIPT_DIR/templates"
LOG_FILE="$SCRIPT_DIR/installation.log"
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log(){ echo -e "${GREEN}[$(date '+%F %T')]${NC} $*" | tee -a "$LOG_FILE"; }
warn(){ echo -e "${YELLOW}[$(date '+%F %T')] WARNING:${NC} $*" | tee -a "$LOG_FILE"; }
err(){ echo -e "${RED}[$(date '+%F %T')] ERROR:${NC} $*" | tee -a "$LOG_FILE"; exit 1; }

# ---- Repo PGDG: asegurar UNA sola fuente Deb822 y UN solo Signed-By ----
ensure_pgdg_repo() {
  # Si ya existe la fuente Deb822, sólo garantizamos que NO haya .list en paralelo
  if [[ -f /etc/apt/sources.list.d/pgdg.sources ]]; then
    sudo rm -f /etc/apt/sources.list.d/pgdg.list || true
    return 0
  fi

  # Instalar script oficial y crear Deb822 (suite trixie-pgdg) con keyring único
  sudo apt-get update -y >>"$LOG_FILE" 2>&1 || true
  sudo apt-get install -y postgresql-common ca-certificates curl >>"$LOG_FILE" 2>&1
  # El script genera /etc/apt/sources.list.d/pgdg.sources con Signed-By coherente
  sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y >>"$LOG_FILE" 2>&1
  # Por si quedaba un .list viejo, elimínalo para evitar "Conflicting values set for Signed-By"
  sudo rm -f /etc/apt/sources.list.d/pgdg.list || true
}

# ---- Dependencias mínimas de build ----
install_build_tools() {
  log "Installing build tools (git, gcc, make)"
  sudo apt-get update -y >>"$LOG_FILE" 2>&1 || true
  sudo apt-get install -y git gcc make >>"$LOG_FILE" 2>&1
}

# ---- Comprobar que PostgreSQL responde ----
wait_for_postgres() {
  log "Checking PostgreSQL availability..."
  for _ in {1..30}; do
    if sudo -u postgres psql -qAt -c "SELECT 1;" >/dev/null 2>&1; then
      log "PostgreSQL is available"
      return 0
    fi
    sleep 1
  done
  err "PostgreSQL is not responding after 30 seconds"
}

# ---- DB y rol ----
setup_database() {
  log "Creating/altering role: ${DB_USER}"
  sudo -u postgres psql <<EOF >>"$LOG_FILE" 2>&1
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}') THEN
    CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}' CREATEDB;
  ELSE
    ALTER ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}';
  END IF;
END
\$\$;
EOF

  local lc
  lc="$(locale -a | grep -Ei '^(en_US\.utf8|en_US\.UTF-8|C\.utf8|C\.UTF-8)$' | head -1 || true)"
  [[ -z "$lc" ]] && lc="C"
  log "Recreating database: ${DB_NAME} (locale: ${lc})"
  sudo -u postgres psql <<EOF >>"$LOG_FILE" 2>&1
DROP DATABASE IF EXISTS ${DB_NAME};
CREATE DATABASE ${DB_NAME}
  WITH OWNER=${DB_USER} ENCODING 'UTF8'
       LC_COLLATE='${lc}' LC_CTYPE='${lc}' TEMPLATE=template0;
GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};
EOF
}

# ---- tpch-kit (gregrahn) ----
# Pinned to a fixed commit for reproducibility and TPC-H Clause 4.2.1.3 provenance
# (the sponsor must be able to state exactly which DBGEN produced the data). gregrahn
# is a third-party fork whose dbgen self-reports version 2.17.3; this commit's data-gen
# semantics correspond to the TPC-H 3.0.1 spec we target. Do NOT float HEAD: bump this
# SHA deliberately and re-validate if you ever update the generator.
TPCH_KIT_COMMIT="852ad0a5ee31ebefeed884cea4188781dd9613a3"

setup_tpch_tools() {
  local kit_dir="$SCRIPT_DIR/tpch-kit"
  local dbgen_dir="$kit_dir/dbgen"

  if $CLEAN; then
    log "Cleaning tpch-kit (full clean)"
    rm -rf "$kit_dir"
  fi

  if [[ ! -d "$dbgen_dir" ]]; then
    log "Cloning tpch-kit (gregrahn) pinned at ${TPCH_KIT_COMMIT}..."
    git clone https://github.com/gregrahn/tpch-kit.git "$kit_dir" >>"$LOG_FILE" 2>&1
    (cd "$kit_dir" && git checkout --quiet "$TPCH_KIT_COMMIT") >>"$LOG_FILE" 2>&1 \
      || err "Could not check out pinned tpch-kit commit ${TPCH_KIT_COMMIT}"
  else
    log "Re-pinning existing tpch-kit to ${TPCH_KIT_COMMIT}..."
    # Fetch in case the pinned commit isn't present locally yet, then check it out.
    # We intentionally do NOT 'git pull' (that would float to a moving HEAD).
    (cd "$kit_dir" \
      && git fetch --quiet origin "$TPCH_KIT_COMMIT" 2>/dev/null \
      ; git checkout --quiet "$TPCH_KIT_COMMIT") >>"$LOG_FILE" 2>&1 \
      || err "Could not check out pinned tpch-kit commit ${TPCH_KIT_COMMIT}"
  fi

  # Stamp the generator version into the log for per-run provenance (Clause 4.2.1.3).
  log "tpch-kit pinned commit: $(cd "$kit_dir" && git rev-parse --short HEAD 2>/dev/null)"

  log "Building dbgen/qgen for Linux + PostgreSQL"
  if $LOW_MEMORY; then
    (cd "$dbgen_dir" && make -j1 MACHINE=LINUX DATABASE=POSTGRESQL >>"$LOG_FILE" 2>&1)
  else
    (cd "$dbgen_dir" && make MACHINE=LINUX DATABASE=POSTGRESQL >>"$LOG_FILE" 2>&1)
  fi

  # Entorno para dbgen/qgen
  export DSS_CONFIG="$dbgen_dir"
  export DSS_QUERY="$dbgen_dir/queries"
  export DSS_PATH="$SCRIPT_DIR/tpch-data"
  mkdir -p "$DSS_PATH"

  [[ -x "$dbgen_dir/dbgen" ]] || err "dbgen not built"
  [[ -x "$dbgen_dir/qgen"  ]] || err "qgen not built"
}

# ---- Generación y carga de datos ----
generate_and_load_data() {
  # DSS_PATH is shared across all scales, and `dbgen -U 1` (below) does NOT rewrite
  # the base .tbl files -- it only emits the refresh stream. So a stale .tbl from a
  # previous scale (e.g. an SF=1 orders.tbl) can survive and get loaded over fresh
  # data, producing an internally inconsistent DB (mismatched orders/lineitem row
  # counts). Wipe the shared dir before generating to guarantee a clean single-scale set.
  log "Cleaning stale generated data in $DSS_PATH"
  rm -f "$DSS_PATH"/*.tbl "$DSS_PATH"/*.tbl.u* "$DSS_PATH"/delete.* "$DSS_PATH"/dss.* 2>/dev/null || true

  log "Generating TPC-H base tables (SF=${SCALE_FACTOR})"
  # tpch-kit v2.17.3 dbgen: base tables only (no -U). Emits *.tbl into DSS_PATH.
  (cd "$DSS_CONFIG" && ./dbgen -v -f -s "$SCALE_FACTOR" >>"$LOG_FILE" 2>&1)
  [[ -f "$DSS_PATH/nation.tbl" ]] || err "Data generation failed (nation.tbl missing)"

  # Number of refresh sets to generate. TPC-H (spec 3.0.1 sec 2.6.3/2.8.1) requires a
  # DISTINCT insert/delete set per RF1/RF2 pair, else RF1 re-inserts existing keys and
  # hits a pk_orders duplicate-key error. Per run we need 1 pair (power) + S pairs
  # (throughput) = 1+S. S is the stream count from spec Table 11.
  #
  # "Endless reuse" (Clause 2.8.1): the DB is NEVER reloaded between runs; it evolves,
  # and a persistent per-DB counter (dss.next_set in run_tests.sh) hands each run the
  # NEXT 1+S unused sets. So the pool must cover EVERY run of this DB across the whole
  # experiment: NSETS = (1+S) * (runs_for_this_db + SET_SAFETY_RUNS). The safety margin
  # absorbs FAILED-run retries (which burn sets that are not reclaimed). Each set is
  # ~0.1% of the tables (tiny), so a large pool costs little disk.
  local S
  S=$(refresh_stream_count "$SCALE_FACTOR")
  local RUNS; RUNS=$(runs_for_this_db)
  local SAFETY="${SET_SAFETY_RUNS:-2}"
  local PER_RUN=$((1 + S))
  local NSETS=$(( PER_RUN * (RUNS + SAFETY) ))
  log "Generating ${NSETS} TPC-H refresh sets (SF=${SCALE_FACTOR}, S=${S}, ${PER_RUN} per run x (${RUNS} runs + ${SAFETY} safety) for ${DB_NAME})"
  # -U N emits N update streams: orders.tbl.u1..uN, lineitem.tbl.u1..uN, delete.1..N.
  (cd "$DSS_CONFIG" && ./dbgen -v -f -s "$SCALE_FACTOR" -U "$NSETS" >>"$LOG_FILE" 2>&1)

  # Map each dbgen set k into the names the RF SQL expects, split + trailing-pipe
  # stripped. dbgen emits every row with a TRAILING '|', so a 9-col orders row has
  # 10 awk fields and a 16-col lineitem row has 17; COPY rejects the extra empty
  # field unless we strip it (sed 's/\|$//'). Same for delete keys (bigint column).
  #   dss.ri.orders.k   = orders insert rows for set k
  #   dss.ri.lineitem.k = lineitem insert rows for set k
  #   dss.rd.k          = delete order keys for set k
  # run_tests.sh selects set k per RF call (psql \copy can't take -v vars).
  log "Mapping ${NSETS} refresh sets -> dss.ri.orders.k / dss.ri.lineitem.k / dss.rd.k"
  local k
  for ((k=1; k<=NSETS; k++)); do
    [[ -f "$DSS_PATH/orders.tbl.u${k}" && -f "$DSS_PATH/lineitem.tbl.u${k}" && -f "$DSS_PATH/delete.${k}" ]] \
      || err "Refresh set ${k} incomplete (orders.tbl.u${k}/lineitem.tbl.u${k}/delete.${k} missing)"
    sed -E 's/\|$//' "$DSS_PATH/orders.tbl.u${k}"   > "$DSS_PATH/dss.ri.orders.${k}"
    sed -E 's/\|$//' "$DSS_PATH/lineitem.tbl.u${k}" > "$DSS_PATH/dss.ri.lineitem.${k}"
    sed -E 's/\|$//' "$DSS_PATH/delete.${k}"        > "$DSS_PATH/dss.rd.${k}"
    [[ -s "$DSS_PATH/dss.ri.orders.${k}" && -s "$DSS_PATH/dss.ri.lineitem.${k}" && -s "$DSS_PATH/dss.rd.${k}" ]] \
      || err "Refresh mapping produced empty files for set ${k}"
  done
  # Record how many sets exist so run_tests.sh can validate.
  echo "$NSETS" > "$DSS_PATH/dss.nsets"

  log "Creating schema from dss.ddl"
  PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -f "$DSS_CONFIG/dss.ddl" >>"$LOG_FILE" 2>&1

  log "Loading data with \\copy (DELIMITER '|')"
  local tables=(region nation part supplier partsupp customer orders lineitem)
  for t in "${tables[@]}"; do
    local file="$DSS_PATH/$t.tbl"
    [[ -f "$file" ]] || err "Missing data file: $file"
    log "  -> $t"
    PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
      -c "\copy $t FROM '${file}' WITH (FORMAT csv, DELIMITER '|')" >>"$LOG_FILE" 2>&1
  done

  log "Adding primary and foreign key constraints"
  if ! PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 >>"$LOG_FILE" 2>&1 <<'SQL'; then
ALTER TABLE ONLY region    ADD CONSTRAINT pk_region     PRIMARY KEY (r_regionkey);
ALTER TABLE ONLY nation    ADD CONSTRAINT pk_nation     PRIMARY KEY (n_nationkey);
ALTER TABLE ONLY supplier  ADD CONSTRAINT pk_supplier   PRIMARY KEY (s_suppkey);
ALTER TABLE ONLY customer  ADD CONSTRAINT pk_customer   PRIMARY KEY (c_custkey);
ALTER TABLE ONLY part      ADD CONSTRAINT pk_part       PRIMARY KEY (p_partkey);
ALTER TABLE ONLY partsupp  ADD CONSTRAINT pk_partsupp   PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE ONLY orders    ADD CONSTRAINT pk_orders     PRIMARY KEY (o_orderkey);
ALTER TABLE ONLY lineitem  ADD CONSTRAINT pk_lineitem   PRIMARY KEY (l_orderkey, l_linenumber);

ALTER TABLE ONLY nation    ADD CONSTRAINT fk_nation_region         FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey);
ALTER TABLE ONLY supplier  ADD CONSTRAINT fk_supplier_nation       FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE ONLY customer  ADD CONSTRAINT fk_customer_nation       FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE ONLY partsupp  ADD CONSTRAINT fk_partsupp_part         FOREIGN KEY (ps_partkey)  REFERENCES part(p_partkey);
ALTER TABLE ONLY partsupp  ADD CONSTRAINT fk_partsupp_supplier     FOREIGN KEY (ps_suppkey)  REFERENCES supplier(s_suppkey);
ALTER TABLE ONLY orders    ADD CONSTRAINT fk_orders_customer       FOREIGN KEY (o_custkey)   REFERENCES customer(c_custkey);
ALTER TABLE ONLY lineitem  ADD CONSTRAINT fk_lineitem_order        FOREIGN KEY (l_orderkey)  REFERENCES orders(o_orderkey);
ALTER TABLE ONLY lineitem  ADD CONSTRAINT fk_lineitem_part         FOREIGN KEY (l_partkey)   REFERENCES part(p_partkey);
ALTER TABLE ONLY lineitem  ADD CONSTRAINT fk_lineitem_supplier     FOREIGN KEY (l_suppkey)   REFERENCES supplier(s_suppkey);
ALTER TABLE ONLY lineitem  ADD CONSTRAINT fk_lineitem_partsupp     FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey);
SQL
    err "Failed adding constraints. Review $LOG_FILE."
  fi

  # ---------------------------------------------------------------------------
  # Compliant supplemental indexes (TPC-H V3.0.1 spec)
  # ---------------------------------------------------------------------------
  # Indexes ARE permitted as "auxiliary data structures" under Clause 1.5.7,
  # PROVIDED each index references only one base table and only ONE of:
  #   (a) a Primary-Key column   (listed in Clause 1.4.2.2),
  #   (b) a Foreign-Key column   (listed in Clause 1.4.2.3), or
  #   (c) a column of date type   (Clause 1.3),
  # "whether or not it is defined as a primary/foreign key constraint."
  # Indexes on non-key, non-date columns (e.g. l_quantity, p_brand) are NOT
  # permitted. Verbatim clause text is quoted at the bottom of this block.
  #
  # Every index below is on an eligible column AND is used by the query set
  # generated from TPC-H V3.0.1/dbgen/queries. We deliberately omit indexes
  # that an eligible column already covers or that no query would use:
  #   * lineitem(l_orderkey) -> already the leading column of pk_lineitem.
  #   * partsupp(ps_suppkey) -> partsupp is small; scan cost is negligible.
  #
  # Index             Eligibility           Queries accelerated
  # ----------------- --------------------- ----------------------------------
  # lineitem(l_partkey)  FK col (1.4.2.3)   Q9,Q14,Q17,Q19,Q20  (fixes Q17:
  #                                         the correlated subquery otherwise
  #                                         full-scans lineitem per part)
  # lineitem(l_suppkey)  FK col (1.4.2.3)   Q5,Q7,Q9,Q15,Q21
  # lineitem(l_shipdate) date col (1.3)     Q1,Q6,Q12,Q14,Q15,Q20 (date ranges)
  # orders(o_custkey)    FK col (1.4.2.3)   Q3,Q5,Q7,Q10,Q18,Q22
  # orders(o_orderdate)  date col (1.3)     Q3,Q4,Q5,Q8,Q10        (date ranges)
  log "Creating compliant supplemental indexes (Clause 1.5.7)"
  if ! PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 >>"$LOG_FILE" 2>&1 <<'SQL'; then
CREATE INDEX idx_lineitem_partkey  ON lineitem (l_partkey);   -- FK col, Clause 1.4.2.3
CREATE INDEX idx_lineitem_suppkey  ON lineitem (l_suppkey);   -- FK col, Clause 1.4.2.3
CREATE INDEX idx_lineitem_shipdate ON lineitem (l_shipdate);  -- date col, Clause 1.3
CREATE INDEX idx_orders_custkey    ON orders   (o_custkey);   -- FK col, Clause 1.4.2.3
CREATE INDEX idx_orders_orderdate  ON orders   (o_orderdate); -- date col, Clause 1.3
SQL
    err "Failed creating indexes. Review $LOG_FILE."
  fi

  # --- Verbatim spec text for future reference -------------------------------
  # TPC-H V3.0.1, Clause 1.5.7 (Auxiliary Data Structures):
  #   "Auxiliary data structures that constitute logical replications of data
  #    from one or more columns of a base table (e.g., indexes, materialized
  #    views, summary tables, structures used to enforce relational integrity
  #    constraints) must conform to the provisions of Clause 1.5.6. The
  #    directives defining and creating these structures are subject to the
  #    following limitations:
  #      - Each directive may reference no more than one base table, and may
  #        not reference other auxiliary structures.
  #      - Each directive may reference one and only one of the following:
  #          o A column or set of columns listed in Clause 1.4.2.2, whether or
  #            not it is defined as a primary key constraint;
  #          o A column or set of columns listed in Clause 1.4.2.3, whether or
  #            not it is defined as a foreign key constraint;
  #          o A column having a date datatype as defined in Clause 1.3.
  #      - Each directive may contain functions or expressions on explicitly
  #        permitted columns"
  #
  # Clause 1.4.2.3 (Foreign Key columns) lists, among others:
  #   L_PARTKEY (-> P_PARTKEY), L_SUPPKEY (-> S_SUPPKEY), O_CUSTKEY (-> C_CUSTKEY).
  # ---------------------------------------------------------------------------

  log "ANALYZE..."
  PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "ANALYZE;" >>"$LOG_FILE" 2>&1
}

# Copy the refresh artifacts into a scale-specific query dir:
#   - rf1.sql / rf2.sql : hand-authored SQL from templates/ (source of truth)
#   - dss.ri / dss.rd   : dbgen-generated refresh data sets from DSS_PATH
# All four are required for a valid run; missing RF SQL is fatal, missing data warns.
copy_refresh_files() {
  local dest="$1"
  mkdir -p "$dest"

  if [[ -f "$TEMPLATES_DIR/rf1.sql" && -f "$TEMPLATES_DIR/rf2.sql" ]]; then
    log "Copying refresh functions (rf1.sql/rf2.sql) from templates/ into $dest"
    cp "$TEMPLATES_DIR/rf1.sql" "$dest/"
    cp "$TEMPLATES_DIR/rf2.sql" "$dest/"
  else
    err "Refresh functions not found in $TEMPLATES_DIR (expected rf1.sql and rf2.sql). These are hand-authored source files."
  fi

  if [[ -f "$DSS_PATH/dss.nsets" ]]; then
    local nsets; nsets=$(cat "$DSS_PATH/dss.nsets")
    log "Copying ${nsets} numbered refresh data sets (dss.ri.orders.k/dss.ri.lineitem.k/dss.rd.k) into $dest"
    local k
    for ((k=1; k<=nsets; k++)); do
      cp "$DSS_PATH/dss.ri.orders.${k}"   "$dest/"
      cp "$DSS_PATH/dss.ri.lineitem.${k}" "$dest/"
      cp "$DSS_PATH/dss.rd.${k}"          "$dest/"
    done
    cp "$DSS_PATH/dss.nsets" "$dest/"
  else
    warn "Refresh data sets not found in $DSS_PATH (expected dss.nsets + dss.ri.orders.k etc). Run generate_and_load_data first."
  fi
}

# Minimum query-stream count S for a scale factor, per TPC-H spec Table 11
# (5.4.1.2). MUST match auto_set_query_streams() in run_tests.sh. Used to size the
# number of refresh sets (1 power + S throughput pairs).
refresh_stream_count() {
  local sf="$1"
  python3 - "$sf" <<'PY'
import sys
sf = float(sys.argv[1] or "1")
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
}

# How many scheduled runs target this database, for sizing the refresh-set pool.
# Under TPC-H "endless reuse" (Clause 2.8.1) the DB is never reloaded; each run
# consumes 1+S fresh RF sets from a persistent counter, so the pool must cover every
# run of this DB across the whole experiment. We count rows in the randomized schedule
# whose db_name column matches $DB_NAME. Overridable via RUNS_FOR_THIS_DB for setups
# done before the schedule exists (or for ad-hoc runs).
runs_for_this_db() {
  if [[ -n "${RUNS_FOR_THIS_DB:-}" ]]; then
    echo "$RUNS_FOR_THIS_DB"
    return 0
  fi
  local schedule="${SCHEDULE_FILE:-$SCRIPT_DIR/experimental_design_schedule.csv}"
  if [[ -f "$schedule" ]]; then
    # db_name is column 6 (see run_randomized_experiment.sh). Count matching data rows.
    local n
    n=$(tail -n +2 "$schedule" | awk -F',' -v db="$DB_NAME" '$6 == db {c++} END {print c+0}')
    if [[ "$n" -gt 0 ]]; then
      echo "$n"
      return 0
    fi
  fi
  # No schedule (or no matching rows): fall back to a single run's worth of sets.
  # The setup still works; the pool just won't cover a long experiment until re-run
  # with the schedule present or RUNS_FOR_THIS_DB set.
  warn "Could not determine run count for $DB_NAME from $schedule; defaulting RF-set pool to 1 run. Set RUNS_FOR_THIS_DB or run with the schedule present for a full-experiment pool."
  echo 1
}

# Number of query streams to pre-generate per scale. Must cover the max S that
# run_tests.sh (auto_set_query_streams) may pick. SF=40 -> S=4; we generate a
# safe margin. stream0 is the POWER stream; stream1..N are THROUGHPUT streams.
QGEN_STREAMS="${QGEN_STREAMS:-8}"

# Apply psql-compatibility fixups to a generated query file (LIMIT -1 handling +
# stray semicolon before LIMIT emitted by some qgen versions).
fix_query_file() {
  local f="$1"
  # Remove standalone "LIMIT -1" lines (TPC-H :n -1 means "no limit")
  sed -i -E '/^[[:space:]]*[Ll][Ii][Mm][Ii][Tt][[:space:]]+-1[[:space:]]*;?[[:space:]]*$/d' "$f"
  # Replace trailing "LIMIT -1" (optional ;) with just ;
  sed -i -E 's/[[:space:]]*[Ll][Ii][Mm][Ii][Tt][[:space:]]+-1[[:space:]]*;?[[:space:]]*$/;/' "$f"
  # Remove any remaining inline "LIMIT -1"
  sed -i -E 's/[[:space:]]*[Ll][Ii][Mm][Ii][Tt][[:space:]]+-1[[:space:]]*//' "$f"
  # qgen emits "ORDER BY ... ;\nLIMIT n;" which breaks in psql; drop the stray ;
  perl -0777 -i -pe 's/;\s*\n\s*(LIMIT\s+-?[0-9]+)/\n\1/ig' "$f"
}

# Generate the 22 queries for one stream into a target dir, with a per-stream
# RNG seed so each stream gets distinct substitution parameters (TPC-H 5.3.5.4).
generate_query_set() {
  local target_dir="$1"
  local seed="$2"
  mkdir -p "$target_dir"
  local i
  for i in $(seq 1 22); do
    "$DSS_CONFIG/qgen" -v -c -s "$SCALE_FACTOR" -r "$seed" "$i" > "$target_dir/q${i}.sql"
    fix_query_file "$target_dir/q${i}.sql"
  done
}

# Canonical scale-factor tag (e.g. SF 1, 1.0, 1.00 -> "1p0"; 0.1 -> "0p1";
# 10 -> "10p0"). The number is normalized to ONE decimal place first so integer and
# dotted forms of the same scale never produce divergent dir names. MUST match
# scale_factor_tag() in run_tests.sh. NOTE: one-decimal normalization means sub-0.1
# scales (e.g. 0.01) are not distinguishable; the active design uses 0.1/1/10 only.
scale_factor_tag() {
  local norm
  # LC_ALL=C forces a '.' decimal point: locales like es_* use ',' for %.1f,
  # which would produce "10,0" and break the tag (tr '.' 'p' finds no dot).
  norm=$(LC_ALL=C printf "%.1f" "$1")
  printf "%s" "$norm" | tr '.' 'p' | tr '-' 'm'
}

# Derive the scale-specific query dir name (e.g. SF=10 -> tpch_queries_sf10p0,
# SF=0.1 -> tpch_queries_sf0p1). MUST match select_queries_dir() in run_tests.sh.
scale_query_dir() {
  printf "%s" "$SCRIPT_DIR/tpch_queries_sf$(scale_factor_tag "$SCALE_FACTOR")"
}

# ---- Generación de consultas Q1..Q22 ----
# Queries are generated ONCE per scale factor into a scale-specific directory.
# There is no shared "flat" tpch_queries/ dir: each scale is fully isolated so a
# later scale can never overwrite an earlier one's parameter sets.
generate_queries() {
  log "Generating queries Q1..Q22 (SF=${SCALE_FACTOR})"

  local sf_dir
  sf_dir="$(scale_query_dir)"
  mkdir -p "$sf_dir"

  # Per-stream parameter sets: stream0 (POWER) .. streamN (THROUGHPUT).
  # Each stream uses a distinct, deterministic seed (1000 + stream index) so
  # streams differ per TPC-H Clause 5.3.5.4.
  local s
  for s in $(seq 0 "$QGEN_STREAMS"); do
    log "Generating per-stream query set: stream${s} (seed $((1000 + s)))"
    generate_query_set "$sf_dir/stream${s}" "$((1000 + s))"
  done

  log "Queries saved to scale-specific dir: $sf_dir (per-stream sets stream0..stream${QGEN_STREAMS})"
}

main() {
  log "Starting TPC-H setup"
  log "Scale Factor: ${SCALE_FACTOR}GB | PostgreSQL: ${PGVER}"

  if $QUERIES_ONLY; then
    log "QUERIES-ONLY mode: Only generating queries (database and data are preserved)"
    
    # Only setup tpch-kit and generate queries
    setup_tpch_tools
    generate_queries
    copy_refresh_files "$(scale_query_dir)"

    log "Queries generation complete!"
    log "Queries saved in: $(scale_query_dir)"
    return 0
  fi

  # 1) Asegura repo PGDG consistente (evita conflicto de Signed-By)
  ensure_pgdg_repo
  sudo apt-get update -y >>"$LOG_FILE" 2>&1 || true

  # 2) Herramientas de build
  install_build_tools

  # 3) Comprobar PostgreSQL y preparar DB
  wait_for_postgres
  setup_database

  # 4) tpch-kit + datos
  setup_tpch_tools
  generate_and_load_data
  generate_queries
  copy_refresh_files "$(scale_query_dir)"

  log "All done!"
  log "DB: ${DB_NAME}  User: ${DB_USER}"
  log "Data dir: ${DSS_PATH}"
  log "Queries : $(scale_query_dir)"
}

main "$@"
