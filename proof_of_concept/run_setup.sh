#!/usr/bin/env bash
# TPC-H setup for PostgreSQL on Debian 13 (Trixie)
# - Arregla el repo PGDG sin conflictos de Signed-By (Deb822)
# - Usa tpch-kit (gregrahn) y carga datos vía \copy (DELIMITER '|')
# - No reinstala PostgreSQL si ya lo tienes
#
# Uso:
#   ./run_setup.sh                # modo normal
#   ./run_setup.sh --clean        # limpia y recompila tpch-kit
#   DB_NAME=tpch DB_USER=tpch_user DB_PASSWORD=xxx SCALE_FACTOR=40 ./run_setup.sh

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
for arg in "${@:-}"; do
  case "$arg" in
    --clean) CLEAN=true ;;
    --low-memory) LOW_MEMORY=true ;;
  esac
done

# ---- Utilidades de logging ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
  mkdir -p "$DSS_PATH" "$SCRIPT_DIR/tpch_queries"

  [[ -x "$dbgen_dir/dbgen" ]] || err "dbgen not built"
  [[ -x "$dbgen_dir/qgen"  ]] || err "qgen not built"
}

# ---- Generación y carga de datos ----
generate_and_load_data() {
  log "Generating TPC-H data (SF=${SCALE_FACTOR})"
  (cd "$DSS_CONFIG" && ./dbgen -v -f -s "$SCALE_FACTOR" >>"$LOG_FILE" 2>&1)
  [[ -f "$DSS_PATH/nation.tbl" ]] || err "Data generation failed (nation.tbl missing)"

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

  log "ANALYZE..."
  PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "ANALYZE;" >>"$LOG_FILE" 2>&1

  # (Opcional) FK después de cargar para acelerar el bulk load
  if [[ -f "$DSS_CONFIG/dss.ri" ]]; then
    log "Applying referential integrity (dss.ri)"
    PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
      -f "$DSS_CONFIG/dss.ri" >>"$LOG_FILE" 2>&1 || warn "dss.ri failed (you can skip it)"
  fi
}

# ---- Generación de consultas Q1..Q22 ----
generate_queries() {
  log "Generating queries Q1..Q22 (SF=${SCALE_FACTOR})"
  for i in $(seq 1 22); do
    "$DSS_CONFIG/qgen" -v -c -s "$SCALE_FACTOR" "$i" > "$SCRIPT_DIR/tpch_queries/q${i}.sql"
  done
  log "Queries saved in $SCRIPT_DIR/tpch_queries/"
}

main() {
  log "Starting TPC-H setup"
  log "Scale Factor: ${SCALE_FACTOR}GB | PostgreSQL: ${PGVER}"

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

  log "All done!"
  log "DB: ${DB_NAME}  User: ${DB_USER}"
  log "Data dir: ${DSS_PATH}"
  log "Queries : $SCRIPT_DIR/tpch_queries/"
}

main "$@"
