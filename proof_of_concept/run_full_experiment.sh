#!/usr/bin/env bash
# run_full_experiment.sh
# Orquesta el ciclo completo:
#  1) Limpia BD previas (40/10/1/0.1 GB)
#  2) Regenera datos y queries por escala (guardando sets específicos de SF)
#  3) Limpia resultados anteriores (CSV/LOG en randomized_results)
#  4) Genera el diseño experimental
#  5) Ejecuta el experimento aleatorizado

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuración de escalas y nombres de BD
SCALES=(
  "40:tpch_db_40gb"
  "10:tpch_db_10gb"
  "1:tpch_db_1gb"
  "0.1:tpch_db_100mb"
)

MASTER_LOG="$SCRIPT_DIR/randomized_experiment.log"
RESULTS_DIR="$SCRIPT_DIR/randomized_results"
RAW_DATA_DIR="$RESULTS_DIR/raw_data"
RUN_LOGS_DIR="$RESULTS_DIR/logs"
SCHEDULE_FILE="$SCRIPT_DIR/experimental_design_schedule.csv"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
section() { log "----- $* -----"; }

cleanup_results() {
  section "Limpiando resultados previos"
  rm -f "$MASTER_LOG" 2>/dev/null || true
  rm -f "$RESULTS_DIR"/*.csv "$RESULTS_DIR"/*.log 2>/dev/null || true
  rm -f "$RAW_DATA_DIR"/*.csv 2>/dev/null || true
  rm -f "$RUN_LOGS_DIR"/*.log 2>/dev/null || true
}

cleanup_databases() {
  section "Eliminando bases de datos previas"
  for pair in "${SCALES[@]}"; do
    IFS=':' read -r sf db <<<"$pair"
    log "Borrando DB $db"
    sudo DB_NAME="$db" "$SCRIPT_DIR/cleanup_tpch.sh" --force --full
  done
}

setup_databases() {
  section "Generando datos y queries por escala"
  for pair in "${SCALES[@]}"; do
    IFS=':' read -r sf db <<<"$pair"
    log "SF=${sf} → DB=${db}"
    SCALE_FACTOR="$sf" DB_NAME="$db" "$SCRIPT_DIR/run_setup.sh"
  done
}

generate_schedule() {
  section "Generando diseño experimental"
  uv run "$SCRIPT_DIR/generate_experimental_design.py" \
    --db-sizes 0.1 1 10 40 \
    --replicates 12 \
    --output "$SCHEDULE_FILE" \
    --cooldown 1 \
    --randomize-databases
}

run_experiment() {
  section "Ejecutando experimento aleatorizado"
  "$SCRIPT_DIR/run_randomized_experiment.sh"
}

main() {
  cleanup_results
  cleanup_databases
  setup_databases
  generate_schedule
  run_experiment
  section "Fin. Revisa resultados en $RESULTS_DIR"
}

main "$@"
