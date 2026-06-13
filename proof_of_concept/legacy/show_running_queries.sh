#!/bin/bash
# show_running_queries.sh - Monitor simple view of qué consulta está corriendo

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_NAME="${DB_NAME:-tpch_db_10gb}"
DB_USER="${DB_USER:-tpch_user}"
DB_PASSWORD="${DB_PASSWORD:-tpch_password_123}"
REFRESH_INTERVAL="${REFRESH_INTERVAL:-2}"

export PGPASSWORD="$DB_PASSWORD"

while true; do
    IFS=$'\n' read -d '' -r -a rows < <(
        psql -h localhost -U "$DB_USER" -d "$DB_NAME" -At -F '|' <<'SQL' && printf '\0'
WITH running AS (
    SELECT
        pid,
        now() - query_start AS duration,
        CASE
            WHEN wait_event_type IS NULL THEN 'ejecutando'
            ELSE wait_event_type || '/' || wait_event
        END AS wait_info,
        ltrim(query) AS query_text
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND state <> 'idle'
      AND pid <> pg_backend_pid()
    ORDER BY query_start
)
SELECT
    pid,
    to_char(duration, 'FMHH24:MI:SS') AS running_for,
    CASE
        WHEN lower(query_text) LIKE 'insert into orders%' THEN 'RF1 (insertando orders)'
        WHEN lower(query_text) LIKE 'insert into lineitem%' THEN 'RF1 (insertando lineitem)'
        WHEN lower(query_text) LIKE '%temp_partsupp_sample%' THEN 'RF1 (preparando muestras)'
        WHEN lower(query_text) LIKE 'delete from lineitem%' THEN 'RF2 (borrando lineitem)'
        WHEN lower(query_text) LIKE 'delete from orders%' THEN 'RF2 (borrando orders)'
        WHEN lower(query_text) LIKE 'select l_returnflag%' THEN 'Q1'
        WHEN lower(query_text) LIKE 'select s_acctbal%' THEN 'Q2'
        WHEN lower(query_text) LIKE 'select l_orderkey%' THEN 'Q3'
        WHEN lower(query_text) LIKE 'select o_orderpriority%' THEN 'Q4'
        WHEN lower(query_text) LIKE 'select n_name%' THEN 'Q5'
        WHEN lower(query_text) LIKE 'select sum(l_extendedprice * l_discount)%' THEN 'Q6'
        WHEN lower(query_text) LIKE 'select supp_nation%' THEN 'Q7'
        WHEN lower(query_text) LIKE 'select o_year%' THEN 'Q8'
        WHEN lower(query_text) LIKE 'select nation%' THEN 'Q9'
        WHEN lower(query_text) LIKE 'select c_custkey%' THEN 'Q10'
        WHEN lower(query_text) LIKE 'select ps_partkey%' THEN 'Q11'
        WHEN lower(query_text) LIKE 'select l_shipmode%' THEN 'Q12'
        WHEN lower(query_text) LIKE 'select c_count%' THEN 'Q13'
        WHEN lower(query_text) LIKE 'select 100.00 * sum(case%' THEN 'Q14'
        WHEN lower(query_text) LIKE 'create view revenue%' THEN 'Q15 (crear vista)'
        WHEN lower(query_text) LIKE 'select s_suppkey%' AND query_text ILIKE '%revenue%' THEN 'Q15 (consulta)'
        WHEN lower(query_text) LIKE 'drop view revenue%' THEN 'Q15 (drop vista)'
        WHEN lower(query_text) LIKE 'select p_brand%' THEN 'Q16'
        WHEN lower(query_text) LIKE 'select sum(l_extendedprice)%avg%' THEN 'Q17'
        WHEN lower(query_text) LIKE 'select c_name%' THEN 'Q18'
        WHEN lower(query_text) LIKE 'select sum(l_extendedprice)%between%' THEN 'Q19'
        WHEN lower(query_text) LIKE 'select s_name%' AND query_text ILIKE '%exists%' THEN 'Q20'
        WHEN lower(query_text) LIKE 'select s_name%' THEN 'Q21'
        WHEN lower(query_text) LIKE 'select cntrycode%' THEN 'Q22'
        ELSE 'Consulta desconocida'
    END AS etiqueta,
    substring(regexp_replace(query_text, E'\\s+', ' ', 'g') FROM 1 FOR 80) AS resumen,
    wait_info
FROM running;
SQL
    )

    clear
    echo "Base de datos: $DB_NAME  |  Usuario: $DB_USER"
    echo "Hora: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""

    if ((${#rows[@]} == 0)); then
        echo "No hay consultas ejecutándose ahora mismo."
    else
        printf "%-7s %-10s %-22s %-20s %s\n" "PID" "Duración" "Estado/Espera" "Identificador" "Resumen"
        printf "%-7s %-10s %-22s %-20s %s\n" "-------" "----------" "----------------------" "--------------------" "-------------------------------"
        for row in "${rows[@]}"; do
            IFS='|' read -r pid duration etiqueta resumen wait_info <<<"$row"
            printf "%-7s %-10s %-22s %-20s %s\n" "$pid" "$duration" "$wait_info" "$etiqueta" "$resumen"
        done
    fi

    sleep "$REFRESH_INTERVAL"
done
