#!/usr/bin/env bash
# run_full_experiment.sh
# Orquesta el ciclo completo:
#  1) Limpia BD previas (según las escalas indicadas)
#  2) Regenera datos y queries por escala (guardando sets específicos de SF)
#  3) Limpia resultados anteriores (CSV/LOG en randomized_results)
#  4) Genera el diseño experimental
#  5) Ejecuta el experimento aleatorizado
#
# Escalas configurables vía parámetros o env var SCALE_FACTORS:
#   ./run_full_experiment.sh 1 5 10 15 20 40
#   SCALE_FACTORS="1 5 10 15 20 40" ./run_full_experiment.sh
#   REPLICATES=12 ./run_full_experiment.sh 1 5 10 40
# Sin argumentos usa la lista por defecto (0.1 1 10).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Escalas: argumentos posicionales > env SCALE_FACTORS > default ---
DEFAULT_SCALES=(0.1 1 10)
if [[ $# -gt 0 ]]; then
  SCALE_FACTORS_LIST=("$@")
elif [[ -n "${SCALE_FACTORS:-}" ]]; then
  # shellcheck disable=SC2206
  SCALE_FACTORS_LIST=(${SCALE_FACTORS})
else
  SCALE_FACTORS_LIST=("${DEFAULT_SCALES[@]}")
fi

REPLICATES="${REPLICATES:-12}"
COOLDOWN="${COOLDOWN:-1}"

# Deriva el nombre de BD desde la escala (GB), igual que generate_experimental_design.py:
#   0.1 -> tpch_db_100mb,  1 -> tpch_db_1gb,  5 -> tpch_db_5gb,  10 -> tpch_db_10gb
db_name_for_scale() {
  local sf="$1"
  python3 - "$sf" <<'PY'
import sys
from decimal import Decimal

def fmt(v):
    return str(int(v)) if float(v) == int(v) else str(v)

sf = Decimal(sys.argv[1])
if sf >= 1:
    label = f"{fmt(sf)}GB"
else:
    mb = sf * Decimal(1000)
    label = f"{fmt(mb)}MB"
print(f"tpch_db_{label.lower()}")
PY
}

# Construye los pares "sf:db" a partir de la lista de escalas
SCALES=()
for sf in "${SCALE_FACTORS_LIST[@]}"; do
  db="$(db_name_for_scale "$sf")"
  SCALES+=("${sf}:${db}")
done

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
  section "Generando diseño experimental (escalas: ${SCALE_FACTORS_LIST[*]}; réplicas: ${REPLICATES})"
  uv run "$SCRIPT_DIR/generate_experimental_design.py" \
    --db-sizes "${SCALE_FACTORS_LIST[@]}" \
    --replicates "$REPLICATES" \
    --output "$SCHEDULE_FILE" \
    --cooldown "$COOLDOWN" \
    --randomize-databases
}

run_experiment() {
  section "Ejecutando experimento aleatorizado"
  "$SCRIPT_DIR/run_randomized_experiment.sh"
}

main() {
  section "Escalas a usar: ${SCALE_FACTORS_LIST[*]}  (réplicas=${REPLICATES}, cooldown=${COOLDOWN})"
  for pair in "${SCALES[@]}"; do log "  ${pair%%:*} GB → ${pair#*:}"; done
  cleanup_results
  cleanup_databases
  generate_schedule
  setup_databases
  run_experiment
  section "Fin. Revisa resultados en $RESULTS_DIR"
}

main "$@"
