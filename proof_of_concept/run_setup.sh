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
#   DB_NAME=tpch DB_USER=tpch_user DB_PASSWORD=xxx SCALE_FACTOR=40 ./run_setup.sh
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
setup_tpch_tools() {
  local kit_dir="$SCRIPT_DIR/tpch-kit"
  local dbgen_dir="$kit_dir/dbgen"

  if $CLEAN; then
    log "Cleaning tpch-kit (full clean)"
    rm -rf "$kit_dir"
  fi

  if [[ ! -d "$dbgen_dir" ]]; then
    log "Cloning tpch-kit (gregrahn)..."
    git clone https://github.com/gregrahn/tpch-kit.git "$kit_dir" >>"$LOG_FILE" 2>&1
  else
    log "Updating tpch-kit..."
    (cd "$kit_dir" && git pull --ff-only >>"$LOG_FILE" 2>&1) || true
  fi

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

  log "Generating TPC-H refresh sets (SF=${SCALE_FACTOR})"
  # -U 1 emits the update/refresh stream as orders.tbl.u1 + lineitem.tbl.u1
  # (RF1 inserts) and delete.1 (RF2 deletes). This dbgen version does NOT
  # produce the older dss.ri/dss.rd names, so we map them below.
  (cd "$DSS_CONFIG" && ./dbgen -v -f -s "$SCALE_FACTOR" -U 1 >>"$LOG_FILE" 2>&1)
  [[ -f "$DSS_PATH/orders.tbl.u1" && -f "$DSS_PATH/lineitem.tbl.u1" && -f "$DSS_PATH/delete.1" ]] \
    || err "Refresh set generation failed (orders.tbl.u1/lineitem.tbl.u1/delete.1 missing)"

  # Map the v2.17.3 refresh files into the dss.ri/dss.rd names the rest of the
  # pipeline (rf1.sql/rf2.sql, run_tests.sh) expects:
  #   dss.ri = orders rows (9 cols) + lineitem rows (16 cols) concatenated;
  #            rf1.sql splits them at load time with awk by field count.
  #   dss.rd = bare order keys, one per line; rf2.sql loads them as one column.
  log "Mapping refresh sets -> dss.ri (RF1 inserts) and dss.rd (RF2 deletes)"
  cat "$DSS_PATH/orders.tbl.u1" "$DSS_PATH/lineitem.tbl.u1" > "$DSS_PATH/dss.ri"
  cp "$DSS_PATH/delete.1" "$DSS_PATH/dss.rd"
  [[ -s "$DSS_PATH/dss.ri" && -s "$DSS_PATH/dss.rd" ]] \
    || err "Refresh mapping produced empty dss.ri/dss.rd"

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

  # No supplemental indexes are created: only the primary/foreign key constraints
  # above are defined, which is the most that TPC-H Clause 1.5.7 permits without
  # query-specific tuning. (Extra analytic indexes on non-key columns would make
  # the database non-compliant.)

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

  if [[ -f "$DSS_PATH/dss.ri" && -f "$DSS_PATH/dss.rd" ]]; then
    log "Copying refresh data files (dss.ri/dss.rd) into $dest"
    cp "$DSS_PATH/dss.ri" "$dest/"
    cp "$DSS_PATH/dss.rd" "$dest/"
  else
    warn "Refresh data files not found in $DSS_PATH (expected dss.ri and dss.rd). Run dbgen with -U 1 (see generate_and_load_data)."
  fi
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

# Derive the scale-specific query dir name (e.g. SF=40 -> tpch_queries_sf40,
# SF=0.1 -> tpch_queries_sf0p1). MUST match select_queries_dir() in run_tests.sh.
scale_query_dir() {
  local scale_tag
  scale_tag=$(printf "%s" "$SCALE_FACTOR" | tr '.' 'p' | tr '-' 'm')
  printf "%s" "$SCRIPT_DIR/tpch_queries_sf${scale_tag}"
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
