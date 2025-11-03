#!/bin/bash
# run_all_benchmarks.sh - Script maestro para ejecutar benchmarks TPC-H en los 3 modos

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MASTER_LOG="$SCRIPT_DIR/master_benchmark.log"
RESULTS_BASE="$SCRIPT_DIR/results"
LOGS_BASE="$SCRIPT_DIR/logs"
ANALYSIS_DIR="$SCRIPT_DIR/analysis"

# Configuración de pruebas
MODES=("sync" "bgworkers" "iouring")
ITERATIONS="${ITERATIONS:-15}"
RUNS_PER_ITERATION="${RUNS_PER_ITERATION:-2}"
QUERY_STREAMS="${QUERY_STREAMS:-2}"
SCALE_FACTOR="${SCALE_FACTOR:-1}"

# Parámetros de ejecución
COOLDOWN_TIME="${COOLDOWN_TIME:-300}"  # 5 minutos entre modos
WARMUP_QUERIES="${WARMUP_QUERIES:-3}"  # Consultas de calentamiento

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Funciones de logging
log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$MASTER_LOG"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$MASTER_LOG"; exit 1; }
warning() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$MASTER_LOG"; }
info() { echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO:${NC} $1" | tee -a "$MASTER_LOG"; }
section() { echo -e "${CYAN}[$(date '+%Y-%m-%d %H:%M:%S')] ========== $1 ==========${NC}" | tee -a "$MASTER_LOG"; }

# Crear estructura de directorios
initialize_directories() {
    log "Inicializando estructura de directorios..."
    mkdir -p "$RESULTS_BASE"/{sync,bgworkers,iouring}
    mkdir -p "$LOGS_BASE"
    mkdir -p "$ANALYSIS_DIR"
    
    # Timestamp para esta ejecución
    EXECUTION_ID="$(date '+%Y%m%d_%H%M%S')"
    export EXECUTION_ID
    
    log "ID de ejecución: $EXECUTION_ID"
}

# Verificar prerequisitos
check_prerequisites() {
    section "Verificando prerequisitos"
    
    # Verificar que existen los scripts necesarios
    [[ -f "$SCRIPT_DIR/run_tests.sh" ]] || error "run_tests.sh no encontrado"
    [[ -f "$SCRIPT_DIR/toggle_pg_config.sh" ]] || error "toggle_pg_config.sh no encontrado"
    
    # Verificar permisos de ejecución
    chmod +x "$SCRIPT_DIR/run_tests.sh"
    chmod +x "$SCRIPT_DIR/toggle_pg_config.sh"
    
    # Verificar PostgreSQL
    if ! sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; then
        error "PostgreSQL no está disponible"
    fi
    
    # Verificar espacio en disco (mínimo 10GB)
    local available_space=$(df "$SCRIPT_DIR" | tail -1 | awk '{print $4}')
    if [[ $available_space -lt 10485760 ]]; then
        warning "Espacio en disco bajo: $(($available_space / 1024 / 1024))GB disponibles"
    fi
    
    log "Todos los prerequisitos verificados"
}

# Ejecutar queries de calentamiento
warmup_database() {
    local mode=$1
    info "Ejecutando $WARMUP_QUERIES consultas de calentamiento para modo $mode..."
    
    export PGPASSWORD="tpch_password_123"
    for i in $(seq 1 $WARMUP_QUERIES); do
        local query_num=$((RANDOM % 22 + 1))
        psql -h localhost -U tpch_user -d tpch_db \
             -f "$SCRIPT_DIR/tpch_queries/q${query_num}.sql" \
             >/dev/null 2>&1 || true
        sleep 2
    done
    
    info "Calentamiento completado"
}

# Limpiar caché del sistema
clear_system_cache() {
    info "Limpiando caché del sistema..."
    sync
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    sleep 5
    info "Caché limpiado"
}

# Reiniciar PostgreSQL y verificar
restart_postgresql() {
    info "Reiniciando PostgreSQL..."
    sudo systemctl restart postgresql
    sleep 10
    
    # Verificar que está funcionando
    local retries=0
    while ! sudo -u postgres psql -c "SELECT 1;" >/dev/null 2>&1; do
        retries=$((retries + 1))
        if [[ $retries -gt 30 ]]; then
            error "PostgreSQL no respondió después del reinicio"
        fi
        sleep 2
    done
    
    info "PostgreSQL reiniciado correctamente"
}

# Ejecutar benchmark para un modo específico
run_mode_benchmark() {
    local mode=$1
    local mode_num=$2
    local total_modes=$3
    
    section "Ejecutando Benchmark: $mode ($mode_num/$total_modes)"
    
    # Configurar PostgreSQL para este modo
    info "Configurando PostgreSQL en modo: $mode"
    if ! sudo "$SCRIPT_DIR/toggle_pg_config.sh" "$mode"; then
        error "Fallo al configurar modo $mode"
    fi
    
    restart_postgresql
    clear_system_cache
    warmup_database "$mode"
    
    # Configurar variables de entorno para run_tests.sh
    export IO_METHOD="$mode"
    export RESULTS_DIR="$RESULTS_BASE/$mode"
    export LOG_FILE="$LOGS_BASE/${mode}_benchmark.log"
    
    # Ejecutar el benchmark
    info "Iniciando benchmark TPC-H para modo $mode..."
    local start_time=$(date +%s)
    
    if "$SCRIPT_DIR/run_tests.sh" "$mode"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log "Benchmark $mode completado en $((duration / 60)) minutos"
        
        # Mover resultados a directorio específico del modo
        mv "$SCRIPT_DIR/tpch_complete_results.csv" "$RESULTS_BASE/$mode/results_${EXECUTION_ID}.csv" 2>/dev/null || true
        mv "$SCRIPT_DIR/tpch_refresh_results.csv" "$RESULTS_BASE/$mode/refresh_${EXECUTION_ID}.csv" 2>/dev/null || true
        mv "$SCRIPT_DIR/tpch_interval_results.csv" "$RESULTS_BASE/$mode/interval_${EXECUTION_ID}.csv" 2>/dev/null || true
        
        return 0
    else
        error "Fallo en benchmark para modo $mode"
    fi
}

# Período de enfriamiento entre modos
cooldown_period() {
    local remaining=$COOLDOWN_TIME
    info "Período de enfriamiento: $COOLDOWN_TIME segundos..."
    
    while [[ $remaining -gt 0 ]]; do
        echo -ne "\rTiempo restante: ${remaining}s  "
        sleep 10
        remaining=$((remaining - 10))
    done
    echo ""
    
    clear_system_cache
}

# Consolidar resultados de todos los modos
consolidate_results() {
    section "Consolidando resultados"
    
    local combined_csv="$ANALYSIS_DIR/combined_results_${EXECUTION_ID}.csv"
    
    info "Combinando archivos CSV..."
    
    # Combinar resultados de queries
    echo "mode,iteration,run_in_iteration,global_run_id,test_type,stream_id,query_number,execution_order,execution_time_seconds,row_count,timestamp" > "$combined_csv"
    
    for mode in "${MODES[@]}"; do
        if [[ -f "$RESULTS_BASE/$mode/results_${EXECUTION_ID}.csv" ]]; then
            tail -n +2 "$RESULTS_BASE/$mode/results_${EXECUTION_ID}.csv" >> "$combined_csv"
        fi
    done
    
    log "Resultados consolidados en: $combined_csv"
}

# Generar reporte de comparación
generate_comparison_report() {
    section "Generando reporte de comparación"
    
    local report_file="$ANALYSIS_DIR/comparison_report_${EXECUTION_ID}.txt"
    
    cat > "$report_file" << EOF
===============================================
TPC-H Benchmark Comparison Report
===============================================
Execution ID: $EXECUTION_ID
Generated: $(date '+%Y-%m-%d %H:%M:%S')

Configuration:
- Scale Factor: ${SCALE_FACTOR}GB
- Iterations: $ITERATIONS
- Runs per Iteration: $RUNS_PER_ITERATION
- Query Streams: $QUERY_STREAMS
- Cooldown Time: ${COOLDOWN_TIME}s

Modes Tested:
$(for mode in "${MODES[@]}"; do echo "  - $mode"; done)

===============================================
Results Summary
===============================================

EOF

    for mode in "${MODES[@]}"; do
        cat >> "$report_file" << EOF
--- Mode: $mode ---
Results Directory: $RESULTS_BASE/$mode/
Log File: $LOGS_BASE/${mode}_benchmark.log

EOF
    done
    
    cat >> "$report_file" << EOF

===============================================
Next Steps
===============================================

1. Analyze consolidated results:
   $ANALYSIS_DIR/combined_results_${EXECUTION_ID}.csv

2. Calculate TPC-H metrics:
   - POWER@Size
   - THROUGHPUT@Size
   - QphH@Size

3. Perform statistical analysis:
   - Compare means across modes
   - Calculate confidence intervals
   - Run significance tests (t-test, ANOVA)

4. Generate visualizations:
   - Performance comparison charts
   - Query execution time distributions
   - Resource utilization graphs

===============================================
EOF

    log "Reporte generado en: $report_file"
    cat "$report_file"
}

# Función principal
main() {
    log "=========================================="
    log "TPC-H Multi-Mode Benchmark Suite"
    log "=========================================="
    log "Inicio: $(date '+%Y-%m-%d %H:%M:%S')"
    
    initialize_directories
    check_prerequisites
    
    local total_modes=${#MODES[@]}
    local mode_num=1
    
    for mode in "${MODES[@]}"; do
        run_mode_benchmark "$mode" "$mode_num" "$total_modes"
        
        # Enfriamiento entre modos (excepto después del último)
        if [[ $mode_num -lt $total_modes ]]; then
            cooldown_period
        fi
        
        mode_num=$((mode_num + 1))
    done
    
    consolidate_results
    generate_comparison_report
    
    section "Benchmark Completo"
    log "Duración total: $((($(date +%s) - $(date -d "$(head -1 $MASTER_LOG | cut -d']' -f1 | tr -d '[')" +%s)) / 60)) minutos"
    log "Resultados disponibles en: $RESULTS_BASE/"
    log "Análisis disponible en: $ANALYSIS_DIR/"
    
    log "=========================================="
    log "Ejecución completada exitosamente"
    log "=========================================="
}

# Manejo de señales
trap 'log "Benchmark interrumpido"; exit 1' INT TERM

# Ejecutar
main "$@"
